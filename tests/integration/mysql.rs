#![allow(dead_code)]
#![cfg(feature = "sqlx")]

use mq_bridge::endpoints::sqlx::{SqlxConsumer, SqlxPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_chaos_pipeline_test, run_direct_perf_test,
    run_performance_pipeline_test, run_pipeline_test, run_test_with_docker,
    run_test_with_docker_controller, setup_logging, PERF_TEST_MESSAGE_COUNT,
};
use std::future::Future;
use std::sync::Arc;

const DOCKER_COMPOSE_FILE: &str = "tests/integration/docker-compose/mysql.yml";
const DATABASE_URL: &str = "mysql://testuser:testpass@localhost:3306/testdb";
const TABLE_NAME: &str = "messages";

async fn setup_db() {
    let config = mq_bridge::models::SqlxConfig {
        url: DATABASE_URL.to_string(),
        table: TABLE_NAME.to_string(),
        auto_create_table: true,
        ..Default::default()
    };
    let _publisher = SqlxPublisher::new(&config).await.unwrap();
}

const CONFIG_YAML: &str = r#"
routes:
  memory_to_sqlx:
    concurrency: 4
    batch_size: 128
    input:
      memory: { topic: "sqlx-mysql-in" }
    output:
      sqlx:
        url: "mysql://testuser:testpass@localhost:3306/testdb"
        table: "messages"

  sqlx_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      sqlx:
        url: "mysql://testuser:testpass@localhost:3306/testdb"
        table: "messages"
        delete_after_read: true
        polling_interval_ms: 20
    output:
      memory: { topic: "sqlx-mysql-out", capacity: {out_capacity} }
"#;

pub async fn test_mysql_pipeline() {
    run_mysql_test(|config_yaml| async move {
        run_pipeline_test("mysql", &config_yaml).await;
    })
    .await;
}

pub async fn test_mysql_performance_pipeline() {
    run_mysql_test(|config_yaml| async move {
        run_performance_pipeline_test("mysql", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
    })
    .await;
}

async fn run_mysql_test<F, Fut>(runner: F)
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    setup_logging();
    run_test_with_docker(DOCKER_COMPOSE_FILE, || async {
        setup_db().await;
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        runner(config_yaml).await;
    })
    .await;
}

pub async fn test_mysql_chaos() {
    setup_logging();
    run_test_with_docker_controller(DOCKER_COMPOSE_FILE, |controller| async move {
        setup_db().await;
        let config_yaml = CONFIG_YAML.replace("{out_capacity}", &(10000 + 1000).to_string());
        run_chaos_pipeline_test("mysql", &config_yaml, controller, "mysql").await;
    })
    .await;
}

pub async fn test_mysql_performance_direct() {
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
            "SQLx (MySQL)",
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

pub async fn test_mysql_status() {
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

        println!("[MySQL] Checking initial status...");
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
        println!("[MySQL] Initial status check OK.");

        controller.stop_service("mysql");
        println!("[MySQL] Service 'mysql' stopped. Waiting for disconnect detection...");

        let start = std::time::Instant::now();
        loop {
            if !publisher.status().await.healthy && !consumer.status().await.healthy {
                println!("[MySQL] Disconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[MySQL] Timeout waiting for disconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }

        controller.start_service("mysql");
        println!("[MySQL] Service 'mysql' started. Waiting for reconnect...");

        let start = std::time::Instant::now();
        loop {
            if publisher.status().await.healthy && consumer.status().await.healthy {
                println!("[MySQL] Reconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[MySQL] Timeout waiting for reconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }
        println!("[MySQL] Status test successful.");
    })
    .await;
}
