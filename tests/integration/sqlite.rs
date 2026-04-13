#![allow(dead_code)]
#![cfg(feature = "sqlx")]

use mq_bridge::endpoints::sqlx::{SqlxConsumer, SqlxPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_direct_perf_test, run_pipeline_test, setup_logging,
    PERF_TEST_MESSAGE_COUNT,
};
use std::sync::Arc;

const TABLE_NAME: &str = "messages";

fn db_file_path() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push("mq_bridge_test_sqlite.db");
    p
}

fn db_url() -> String {
    format!("sqlite://{}", db_file_path().display())
}

async fn setup_db() {
    let db_path = db_file_path();
    let _ = std::fs::remove_file(&db_path);
    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::File::create(&db_path);
    let config = mq_bridge::models::SqlxConfig {
        url: db_url(),
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
            memory: { topic: "sqlx-sqlite-in" }
        output:
            sqlx:
                url: "{db_url}"
                table: "messages"

    sqlx_to_memory:
        concurrency: 4
        batch_size: 128
        input:
            sqlx:
                url: "{db_url}"
                table: "messages"
                delete_after_read: true
                polling_interval_ms: 20
        output:
            memory: { topic: "sqlx-sqlite-out", capacity: {out_capacity} }
"#;

pub async fn test_sqlite_pipeline() {
    setup_logging();
    setup_db().await;
    let config_yaml = CONFIG_YAML
        .replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        )
        .replace("{db_url}", &db_url());
    run_pipeline_test("sqlx", &config_yaml).await;
}

pub async fn test_sqlite_performance_direct() {
    setup_logging();
    setup_db().await;
    let config = mq_bridge::models::SqlxConfig {
        url: db_url(),
        table: TABLE_NAME.to_string(),
        delete_after_read: true,
        polling_interval_ms: Some(1),
        auto_create_table: true,
        ..Default::default()
    };

    let result = run_direct_perf_test(
        "SQLx (SQLite)",
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
}
