#![allow(dead_code)]
//! Integration tests for the `object_store` endpoint against LocalStack S3.
//!
//! `object_store` reads its backend config (creds/endpoint/region) from the process
//! environment, so each test sets the LocalStack S3 vars before building endpoints.

use mq_bridge::endpoints::object_store::{ObjectStoreConsumer, ObjectStorePublisher};
use mq_bridge::models::{FileFormat, ObjectStoreConfig};
use mq_bridge::test_utils::{run_pipeline_test, run_test_with_docker, setup_logging};
use mq_bridge::traits::{MessageConsumer, MessagePublisher};
use mq_bridge::CanonicalMessage;

const BUCKET: &str = "mqb-object-store-test";
const ENDPOINT: &str = "http://localhost:4566";

/// Points the `object_store` crate at LocalStack S3 over plain HTTP.
fn set_s3_env() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_ENDPOINT", ENDPOINT);
    std::env::set_var("AWS_ALLOW_HTTP", "true");
}

/// Creates the test bucket (path-style PUT). LocalStack accepts the unsigned request and
/// the call is idempotent, so re-running against a warm container is fine.
async fn ensure_bucket() {
    let client = reqwest::Client::new();
    client
        .put(format!("{ENDPOINT}/{BUCKET}"))
        .send()
        .await
        .expect("create bucket request")
        .error_for_status()
        .expect("create bucket response");
}

fn config(prefix: &str, checkpoint_store: Option<String>) -> ObjectStoreConfig {
    ObjectStoreConfig {
        url: format!("s3://{BUCKET}/{prefix}"),
        format: FileFormat::Normal,
        cursor_id: checkpoint_store.as_ref().map(|_| "resume-test".to_string()),
        checkpoint_store,
        polling_interval_ms: Some(100),
        ..Default::default()
    }
}

fn json_msg(n: i64) -> CanonicalMessage {
    CanonicalMessage::new(
        serde_json::to_vec(&serde_json::json!({ "n": n })).unwrap(),
        None,
    )
}

const CONFIG_YAML: &str = r#"
routes:
  memory_to_object_store:
    concurrency: 4
    batch_size: 64
    input:
      memory: { topic: "obj-test-in" }
    output:
      object_store:
        url: "s3://mqb-object-store-test/{prefix}"
        format: normal
  object_store_to_memory:
    concurrency: 1
    batch_size: 64
    input:
      object_store:
        url: "s3://mqb-object-store-test/{prefix}"
        format: normal
        polling_interval_ms: 100
    output:
      memory: { topic: "obj-test-out", capacity: {out_capacity} }
"#;

/// End-to-end pipeline: memory -> object_store (write objects) -> object_store (read) -> memory.
pub async fn test_object_store_pipeline() {
    setup_logging();
    run_test_with_docker(
        "tests/integration/docker-compose/object_store.yml",
        || async {
            set_s3_env();
            ensure_bucket().await;
            // Unique prefix per run so a reused container can't leak objects between tests.
            let prefix = format!("pipeline/{}", fast_uuid_v7::gen_id_str());
            let config_yaml = CONFIG_YAML
                .replace("{prefix}", &prefix)
                .replace("{out_capacity}", "10000");
            run_pipeline_test("object_store", &config_yaml).await;
        },
    )
    .await;
}

/// Durable resume: after acking the first object, a fresh consumer built with the same
/// checkpoint store must not re-read it.
pub async fn test_object_store_resume() {
    setup_logging();
    run_test_with_docker(
        "tests/integration/docker-compose/object_store.yml",
        || async {
            set_s3_env();
            ensure_bucket().await;

            let prefix = format!("resume/{}", fast_uuid_v7::gen_id_str());
            let ckpt_dir =
                std::env::temp_dir().join(format!("mqb_obj_ckpt_{}", fast_uuid_v7::gen_id_str()));
            let ckpt = format!("file://{}/cursor.json", ckpt_dir.display());

            // Write three single-record objects.
            let publisher = ObjectStorePublisher::new(&config(&prefix, None))
                .await
                .unwrap();
            for n in 0..3 {
                publisher.send_batch(vec![json_msg(n)]).await.unwrap();
            }

            // First consumer: read + ack exactly the first object, then drop.
            {
                let mut consumer = ObjectStoreConsumer::new(&config(&prefix, Some(ckpt.clone())))
                    .await
                    .unwrap();
                let batch = wait_for_batch(&mut consumer).await;
                assert_eq!(batch.messages.len(), 1, "one record per object");
                let first_payload = batch.messages[0].payload.clone();
                (batch.commit)(vec![mq_bridge::traits::MessageDisposition::Ack; 1])
                    .await
                    .unwrap();

                // A restarted consumer with the same checkpoint resumes past the acked object.
                let mut resumed = ObjectStoreConsumer::new(&config(&prefix, Some(ckpt.clone())))
                    .await
                    .unwrap();
                let next = wait_for_batch(&mut resumed).await;
                assert_eq!(next.messages.len(), 1);
                assert_ne!(
                    next.messages[0].payload, first_payload,
                    "resumed consumer must not re-read the acked object"
                );
            }

            tokio::fs::remove_dir_all(&ckpt_dir).await.ok();
        },
    )
    .await;
}

/// Polls `receive_batch` until a non-empty batch arrives (the source returns empty batches
/// while idle / between objects).
async fn wait_for_batch(consumer: &mut ObjectStoreConsumer) -> mq_bridge::traits::ReceivedBatch {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let batch = consumer.receive_batch(64).await.unwrap();
        if !batch.messages.is_empty() {
            return batch;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for a non-empty batch"
        );
    }
}
