#![allow(dead_code)]

use mq_bridge::endpoints::sqlx::{SqlxConsumer, SqlxPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_chaos_pipeline_test, run_direct_perf_test, run_pipeline_test,
    run_test_with_docker, run_test_with_docker_controller, setup_logging, PERF_TEST_MESSAGE_COUNT,
};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

const DOCKER_COMPOSE_FILE: &str = "tests/integration/docker-compose/postgres.yml";
const DATABASE_URL: &str = "postgres://testuser:testpass@localhost:5432/testdb";
const TABLE_NAME: &str = "messages";

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

  sqlx_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      sqlx:
        url: "postgres://testuser:testpass@localhost:5432/testdb"
        table: "messages"
        delete_after_read: true
        polling_interval_ms: 10
    output:
      memory: { topic: "sqlx-test-out", capacity: {out_capacity} }
"#;

async fn setup_db() {
    // Wait for DB to be ready
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(DATABASE_URL)
        .await
        .expect("Failed to connect to Postgres");

    // Drop table if exists and create new one
    sqlx::query(&format!("DROP TABLE IF EXISTS {}", TABLE_NAME))
        .execute(&pool)
        .await
        .expect("Failed to drop table");

    // The consumer expects an 'id' and 'payload' column.
    // 'id' should be auto-incrementing.
    sqlx::query(&format!(
        "CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, payload BYTEA NOT NULL)",
        TABLE_NAME
    ))
    .execute(&pool)
    .await
    .expect("Failed to create table");
}

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
