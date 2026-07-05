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
