#![allow(dead_code)]
#![cfg(feature = "sqlx")]

use mq_bridge::endpoints::sqlx::{SqlxConsumer, SqlxPublisher};
use mq_bridge::test_utils::{
    add_performance_result, run_chaos_pipeline_test, run_direct_perf_test,
    run_performance_pipeline_test_named, run_pipeline_test, run_test_with_docker,
    run_test_with_docker_controller, setup_logging, PERF_TEST_MESSAGE_COUNT,
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
    batch_size: 1024
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
    batch_size: 1024
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

pub async fn test_postgres_pipeline() {
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

pub async fn test_postgres_performance_pipeline() {
    setup_logging();
    run_test_with_docker(DOCKER_COMPOSE_FILE, || async {
        setup_db().await;
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_performance_pipeline_test_named(
            "sqlx",
            "postgres",
            &config_yaml,
            PERF_TEST_MESSAGE_COUNT,
        )
        .await;
    })
    .await;
}

pub async fn test_postgres_chaos() {
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

pub async fn test_postgres_performance_direct() {
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

pub async fn test_postgres_multicolumn() {
    use mq_bridge::traits::MessagePublisher;
    use mq_bridge::CanonicalMessage;
    use sqlx::{AnyPool, Row};

    setup_logging();
    run_test_with_docker(DOCKER_COMPOSE_FILE, || async {
        sqlx::any::install_default_drivers();
        let pool = AnyPool::connect(DATABASE_URL).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS orders")
            .execute(&pool)
            .await
            .unwrap();
        // Typed columns: string-binding everything would fail on `qty INTEGER` / `price DOUBLE PRECISION`.
        sqlx::query(
            "CREATE TABLE orders (sku TEXT, qty INTEGER, price DOUBLE PRECISION, cust TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let config = mq_bridge::models::SqlxConfig {
            url: DATABASE_URL.to_string(),
            table: "orders".to_string(),
            insert_query: Some(
                "INSERT INTO orders (sku, qty, price, cust) VALUES (${payload:sku}, ${payload:qty}, ${payload:price}, ${metadata:cust})"
                    .to_string(),
            ),
            ..Default::default()
        };
        let publisher = SqlxPublisher::new(&config).await.unwrap();

        let mut msg =
            CanonicalMessage::new(br#"{"sku":"abc","qty":7,"price":1.5}"#.to_vec(), None);
        msg.metadata.insert("cust".to_string(), "c1".to_string());
        publisher.send(msg).await.unwrap();

        // Batch of 2 with typed columns and per-row metadata.
        let mut batch = Vec::new();
        for i in 0..2 {
            let mut m = CanonicalMessage::new(
                format!(r#"{{"sku":"s{i}","qty":{},"price":{}}}"#, i * 10, i as f64 + 0.5)
                    .into_bytes(),
                None,
            );
            m.metadata.insert("cust".to_string(), format!("b{i}"));
            batch.push(m);
        }
        publisher.send_batch(batch).await.unwrap();

        let row = sqlx::query("SELECT sku, qty, price, cust FROM orders WHERE sku = 'abc'")
            .fetch_one(&pool)
            .await
            .unwrap();
        let sku: String = row.get("sku");
        let qty: i32 = row.get("qty");
        let price: f64 = row.get("price");
        let cust: String = row.get("cust");
        assert_eq!(sku, "abc");
        assert_eq!(qty, 7);
        assert_eq!(price, 1.5);
        assert_eq!(cust, "c1");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 3, "1 single + 2 batch rows expected");

        println!("[Postgres] Multi-column typed insert test successful.");
    })
    .await;
}

pub async fn test_postgres_status() {
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use tokio::time::{sleep, Duration};

    setup_logging();
    run_test_with_docker_controller(DOCKER_COMPOSE_FILE, |controller| async move {
        setup_db().await;
        let config = mq_bridge::models::SqlxConfig {
            url: DATABASE_URL.to_string(),
            table: TABLE_NAME.to_string(),
            acquire_timeout_ms: Some(1000),
            ..Default::default()
        };

        let publisher = SqlxPublisher::new(&config).await.unwrap();
        let consumer = SqlxConsumer::new(&config).await.unwrap();

        println!("[Postgres] Checking initial status...");
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
        println!("[Postgres] Initial status check OK.");

        controller.stop_service("postgres");
        println!("[Postgres] Service 'postgres' stopped. Waiting for disconnect detection...");

        let start = std::time::Instant::now();
        loop {
            if !publisher.status().await.healthy && !consumer.status().await.healthy {
                println!("[Postgres] Disconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[Postgres] Timeout waiting for disconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }

        controller.start_service("postgres");
        println!("[Postgres] Service 'postgres' started. Waiting for reconnect...");

        let start = std::time::Instant::now();
        loop {
            if publisher.status().await.healthy && consumer.status().await.healthy {
                println!("[Postgres] Reconnect detected.");
                break;
            }
            if start.elapsed() > Duration::from_secs(20) {
                panic!("[Postgres] Timeout waiting for reconnect.");
            }
            sleep(Duration::from_secs(1)).await;
        }
        println!("[Postgres] Status test successful.");
    })
    .await;
}
