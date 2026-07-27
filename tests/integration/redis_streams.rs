#![allow(dead_code)]
use std::sync::Arc;

use mq_bridge::endpoints::redis_streams::{RedisStreamsConsumer, RedisStreamsPublisher};
use mq_bridge::test_utils::{
    run_performance_pipeline_test, run_pipeline_test, run_test_with_docker,
    run_test_with_docker_controller, setup_logging, verify_subscriber_logic,
    PERF_TEST_MESSAGE_COUNT,
};

const COMPOSE: &str = "tests/integration/docker-compose/redis.yml";
const URL: &str = "redis://localhost:6379";

const CONFIG_YAML: &str = r#"
routes:
  memory_to_redis_streams:
    concurrency: 4
    batch_size: 128
    input:
      memory: { topic: "test-in-redis" }
    output:
      middlewares:
        - retry:
            max_attempts: 20
            initial_interval_ms: 500
            max_interval_ms: 2000
      redis_streams: { url: "redis://localhost:6379", stream: "test-redis-pipeline" }

  redis_streams_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      redis_streams: { url: "redis://localhost:6379", stream: "test-redis-pipeline", group: "itest-pipeline", reader_connections: 4 }
    output:
      memory: { topic: "test-out-redis", capacity: {out_capacity} }
"#;

pub async fn test_redis_pipeline() {
    setup_logging();
    run_test_with_docker(COMPOSE, || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_pipeline_test("redis_streams", &config_yaml).await;
    })
    .await;
}

pub async fn test_redis_subscriber_logic() {
    setup_logging();
    run_test_with_docker(COMPOSE, || async {
        let stream = format!("sub_logic_redis_{}", fast_uuid_v7::gen_id());
        let config = mq_bridge::models::RedisStreamsConfig {
            url: URL.to_string(),
            stream: Some(stream),
            subscriber_mode: true,
            ..Default::default()
        };

        let publisher = Arc::new(RedisStreamsPublisher::new(&config).await.unwrap());
        let sub1 = Arc::new(tokio::sync::Mutex::new(
            RedisStreamsConsumer::new(&config).await.unwrap(),
        ));
        let sub2 = Arc::new(tokio::sync::Mutex::new(
            RedisStreamsConsumer::new(&config).await.unwrap(),
        ));
        // Give the subscriber read tasks time to issue their first blocking XREAD
        // from "$" so they don't miss the messages published below.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        verify_subscriber_logic(publisher, sub1, sub2).await;
    })
    .await;
}

pub async fn test_redis_performance_pipeline() {
    setup_logging();
    run_test_with_docker(COMPOSE, || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_performance_pipeline_test("redis_streams", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
    })
    .await;
}

pub async fn test_redis_status() {
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use tokio::time::{sleep, Duration};

    setup_logging();
    run_test_with_docker_controller(COMPOSE, |controller| async move {
        let config = mq_bridge::models::RedisStreamsConfig {
            url: URL.to_string(),
            stream: Some("status_redis".to_string()),
            group: Some("itest-status".to_string()),
            ..Default::default()
        };

        let publisher = RedisStreamsPublisher::new(&config).await.unwrap();
        let consumer = RedisStreamsConsumer::new(&config).await.unwrap();

        println!("[Redis] Checking initial status...");
        sleep(Duration::from_secs(2)).await;
        assert!(
            publisher.status().await.healthy,
            "Publisher should be healthy initially"
        );
        assert!(
            consumer.status().await.healthy,
            "Consumer should be healthy initially"
        );
        println!("[Redis] Initial status check OK.");

        controller.stop_service("redis");
        println!("[Redis] Service 'redis' stopped. Waiting for disconnect detection...");
        let start = std::time::Instant::now();
        loop {
            if !publisher.status().await.healthy && !consumer.status().await.healthy {
                println!("[Redis] Disconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[Redis] Timeout waiting for disconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }

        controller.start_service("redis");
        println!("[Redis] Service 'redis' started. Waiting for reconnect...");
        let start = std::time::Instant::now();
        loop {
            if publisher.status().await.healthy && consumer.status().await.healthy {
                println!("[Redis] Reconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[Redis] Timeout waiting for reconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }
        println!("[Redis] Status test successful.");
    })
    .await;
}

// Issue 1 (regression): reading FROM a Redis stream with `exit_on_empty` must drain
// every existing entry and then terminate. Before the fix the consumer blocked on the
// first `recv` forever, so `--drain` never fired and a broker batch job hung.
pub async fn test_redis_drain_exits_on_empty() {
    use mq_bridge::models::{Endpoint, EndpointType, RedisStreamsConfig};
    use mq_bridge::route::RouteOutcome;
    use mq_bridge::traits::{MessageDisposition, MessagePublisher};
    use mq_bridge::{CanonicalMessage, Route};

    setup_logging();
    run_test_with_docker(COMPOSE, || async {
        const N: usize = 300;
        let stream = format!("drain_redis_{}", fast_uuid_v7::gen_id());

        // Publish N entries.
        let pub_config = RedisStreamsConfig {
            url: URL.to_string(),
            stream: Some(stream.clone()),
            ..Default::default()
        };
        let publisher = RedisStreamsPublisher::new(&pub_config).await.unwrap();
        let batch: Vec<_> = (0..N)
            .map(|i| CanonicalMessage::new(format!("msg-{i}").into_bytes(), None))
            .collect();
        publisher.send_batch(batch).await.unwrap();

        // Read them with a group from the start of the stream, draining to memory.
        let in_config = RedisStreamsConfig {
            url: URL.to_string(),
            stream: Some(stream.clone()),
            group: Some(format!("g_{}", fast_uuid_v7::gen_id())),
            read_from_start: true,
            ..Default::default()
        };
        let out_topic = format!("drain_redis_out_{}", fast_uuid_v7::gen_id());
        let input = Endpoint::new(EndpointType::RedisStreams(in_config));
        let output = Endpoint::new_memory(&out_topic, N + 100);
        let route = Route::new(input, output)
            .with_batch_size(128)
            .with_exit_on_empty(true);

        let mut verifier = route
            .connect_to_output("redis_drain_verifier")
            .await
            .unwrap();
        let collector = tokio::spawn(async move {
            let mut received = 0usize;
            while received < N {
                let item =
                    tokio::time::timeout(std::time::Duration::from_secs(15), verifier.receive())
                        .await
                        .expect("timed out draining output")
                        .expect("output stream closed early");
                received += 1;
                (item.commit)(MessageDisposition::Ack).await.unwrap();
            }
            received
        });

        let handle = route.run("redis_drain_test").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while handle.outcome().is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("redis route did not exit on empty after draining");
        assert_eq!(handle.outcome(), Some(RouteOutcome::Completed));
        handle.join().await.expect("route task panicked");

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), collector)
            .await
            .expect("timed out collecting output")
            .expect("collector task panicked");
        assert_eq!(received, N);
        println!("[Redis] Drain exit_on_empty test successful.");
    })
    .await;
}
