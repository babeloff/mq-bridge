#![allow(dead_code)]

use mq_bridge::endpoints::sqlx::{SqlxConsumer, SqlxPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_chaos_pipeline_test, run_direct_perf_test, run_pipeline_test,
    run_test_with_docker, run_test_with_docker_controller, setup_logging, PERF_TEST_MESSAGE_COUNT,
};
use std::sync::Arc;

const DOCKER_COMPOSE_FILE: &str = "tests/integration/docker-compose/postgres.yml";
const DATABASE_URL: &str = "postgres://testuser:testpass@localhost:5432/testdb";
const TABLE_NAME: &str = "messages";

async fn setup_db() {
    let config = mq_bridge::models::SqlxConfig {
        url: DATABASE_URL.to_string(),
        table: TABLE_NAME.to_string(),
        auto_create_table: true,
        ..Default::default()
    };
    // This will trigger table creation
    let _publisher = SqlxPublisher::new(&config).await.unwrap();
}

const CONFIG_YAML: &str = r#"
routes:
  memory_to_sqlx:
    concurrency: 4
    batch_size: 128
    input:
      memory: { topic: "sqlx-test-in" }
    output:
      middlewares:
        - retry:
            max_attempts: 20
            initial_interval_ms: 500
            max_interval_ms: 2000
      sqlx:
        url: "postgres://testuser:testpass@localhost:5432/testdb"
        table: "messages"
        min_connections: 2

  sqlx_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      sqlx:
        url: "postgres://testuser:testpass@localhost:5432/testdb"
        table: "messages"
        delete_after_read: true
        polling_interval_ms: 20
        min_connections: 2
    output:
      memory: { topic: "sqlx-test-out", capacity: {out_capacity} }
"#;

pub async fn test_sqlx_pipeline() {
    setup_logging();
    run_test_with_docker(DOCKER_COMPOSE_FILE, || async {
        setup_db().await;
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_pipeline_test("sqlx", &config_yaml).await;
    })
    .await;
}

pub async fn test_sqlx_chaos() {
    setup_logging();
    run_test_with_docker_controller(DOCKER_COMPOSE_FILE, |controller| async move {
        setup_db().await;
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(10000 + 1000).to_string(), // Using a smaller number for chaos tests
        );
        run_chaos_pipeline_test("sqlx", &config_yaml, controller, "postgres").await;
    })
    .await;
}

pub async fn test_sqlx_performance_direct() {
    setup_logging();
    run_test_with_docker(DOCKER_COMPOSE_FILE, || async {
        setup_db().await;
        let config = mq_bridge::models::SqlxConfig {
            url: DATABASE_URL.to_string(),
            table: TABLE_NAME.to_string(),
            delete_after_read: true,
            polling_interval_ms: Some(1),
            auto_create_table: true,
            ..Default::default()
        };

        let result = run_direct_perf_test(
            "SQLx (Postgres)",
            || async {
                let pub_config = config.clone();
                Arc::new(SqlxPublisher::new(&pub_config).await.unwrap())
            },
            || async {
                let consumer_config = config.clone();
                Arc::new(tokio::sync::Mutex::new(
                    SqlxConsumer::new(&consumer_config).await.unwrap(),
                ))
            },
        )
        .await;

        add_performance_result(result);
    })
    .await;
}

pub async fn test_sqlx_status() {
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use tokio::time::{sleep, Duration};

    setup_logging();
    run_test_with_docker_controller(DOCKER_COMPOSE_FILE, |controller| async move {
        setup_db().await;
        let config = mq_bridge::models::SqlxConfig {
            url: DATABASE_URL.to_string(),
            table: TABLE_NAME.to_string(),
            ..Default::default()
        };

        let publisher = SqlxPublisher::new(&config).await.unwrap();
        let consumer = SqlxConsumer::new(&config).await.unwrap();

        println!("[SQLx] Checking initial status...");
        sleep(Duration::from_secs(2)).await;
        let pub_status = publisher.status().await;
        let con_status = consumer.status().await;
        assert!(
            pub_status.healthy,
            "Publisher should be healthy initially. Status: {:?}",
            pub_status
        );
        assert!(
            con_status.healthy,
            "Consumer should be healthy initially. Status: {:?}",
            con_status
        );
        println!("[SQLx] Initial status check OK.");

        controller.stop_service("postgres");
        println!("[SQLx] Service 'postgres' stopped. Waiting for disconnect detection...");

        let start = std::time::Instant::now();
        loop {
            if !publisher.status().await.healthy && !consumer.status().await.healthy {
                println!("[SQLx] Disconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[SQLx] Timeout waiting for disconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }

        controller.start_service("postgres");
        println!("[SQLx] Service 'postgres' started. Waiting for reconnect...");

        let start = std::time::Instant::now();
        loop {
            if publisher.status().await.healthy && consumer.status().await.healthy {
                println!("[SQLx] Reconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[SQLx] Timeout waiting for reconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }
        println!("[SQLx] Status test successful.");
    })
    .await;
}
