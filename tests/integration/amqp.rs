#![allow(dead_code)]

use mq_bridge::endpoints::amqp::{AmqpConsumer, AmqpPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_chaos_pipeline_test, run_direct_perf_test,
    run_performance_pipeline_test, run_pipeline_test, run_test_with_docker,
    run_test_with_docker_controller, setup_logging, PERF_TEST_MESSAGE_COUNT,
};
use std::sync::Arc;

const CONFIG_YAML: &str = r#"
routes:
  memory_to_amqp:
    concurrency: 4
    batch_size: 128
    input:
      memory: { topic: "amqp-test-in" }
    output:
      middlewares:
        - retry:
            max_attempts: 10
            initial_interval_ms: 500
            max_interval_ms: 2000
      amqp: { url: "amqp://guest:guest@localhost:5672/%2f", queue: "test_queue_amqp" }

  amqp_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      amqp: { url: "amqp://guest:guest@localhost:5672/%2f", queue: "test_queue_amqp", prefetch_count: 1000 }
    output:
      memory: { topic: "amqp-test-out", capacity: {out_capacity} }
"#;

pub async fn test_amqp_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/amqp.yml", || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_pipeline_test("AMQP", &config_yaml).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn test_amqp_publisher_handles_nack() {
    use mq_bridge::traits::MessagePublisher;
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/amqp.yml", || async {
        let nack_queue = "test_nack_queue";
        let config = mq_bridge::models::AmqpConfig {
            url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            queue: Some(nack_queue.to_string()),
            no_declare_queue: true, // The test manually declares the queue with special args
            ..Default::default()
        };

        let conn = lapin::Connection::connect(&config.url, lapin::ConnectionProperties::default())
            .await
            .unwrap();
        let channel = conn.create_channel().await.unwrap();
        // Manually create a queue that will cause a NACK.
        // A queue with max-length 0 and overflow "reject-publish" will reject messages.
        let mut args = lapin::types::FieldTable::default();
        args.insert("x-max-length".into(), lapin::types::AMQPValue::LongInt(0));
        args.insert(
            "x-overflow".into(),
            lapin::types::AMQPValue::LongString("reject-publish".into()),
        );
        channel
            .queue_declare(
                nack_queue,
                lapin::options::QueueDeclareOptions::default(),
                args,
            )
            .await
            .unwrap();

        // Create our publisher
        let publisher = AmqpPublisher::new(&config).await.unwrap();

        // Send a message that should be NACKed
        let msg = mq_bridge::CanonicalMessage::from("this will be nacked");
        let result = publisher.send(msg).await;

        // Assert that we received a Retryable error because of the NACK
        assert!(result.is_err(), "Expected send to fail with a NACK");
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            mq_bridge::traits::PublisherError::Retryable(_)
        ));
        assert!(
            err.to_string().contains("Broker Nacked the message"),
            "Error message should indicate a NACK"
        );

        println!("AMQP NACK handling test passed!");
    })
    .await;
}

pub async fn test_amqp_chaos() {
    setup_logging();
    run_test_with_docker_controller(
        "tests/integration/docker-compose/amqp.yml",
        |controller| async move {
            let config_yaml = CONFIG_YAML.replace(
                "{out_capacity}",
                &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
            );
            run_chaos_pipeline_test("AMQP", &config_yaml, controller, "rabbitmq").await;
        },
    )
    .await;
}

pub async fn test_amqp_performance_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/amqp.yml", || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_performance_pipeline_test("AMQP", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
    })
    .await;
}

pub async fn test_amqp_performance_direct() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/amqp.yml", || async {
        let queue = "perf_test_amqp_direct";
        let config = mq_bridge::models::AmqpConfig {
            url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            delayed_ack: false,
            prefetch_count: Some(1000),
            ..Default::default()
        };

        let result = run_direct_perf_test(
            "AMQP",
            || async {
                let mut pub_config = config.clone();
                pub_config.queue = Some(queue.to_string());
                Arc::new(AmqpPublisher::new(&pub_config).await.unwrap())
            },
            || async {
                let mut endpoint = config.clone();
                endpoint.queue = Some(queue.to_string());
                endpoint.subscribe_mode = false;

                Arc::new(tokio::sync::Mutex::new(
                    AmqpConsumer::new(&endpoint).await.unwrap(),
                ))
            },
        )
        .await;
        add_performance_result(result);
    })
    .await;
}
