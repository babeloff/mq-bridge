// Aggregate Docker-backed integration suites. Keep these ignored so default
// `cargo test` stays local and fast.
//
// Run from the project root:
// cargo test --test integration_test --features full,test-utils --release -- --ignored --nocapture --test-threads=1
//
// Limit to a backend with MQB_TEST_BACKEND, for example:
// MQB_TEST_BACKEND=kafka cargo test --test integration_test --features full,test-utils --release -- --ignored --nocapture --test-threads=1

#![allow(unused_imports, dead_code)]

#[path = "integration/mod.rs"]
mod integration;

#[allow(dead_code)]
pub fn should_run(test_name: &str) -> bool {
    mq_bridge::test_utils::should_run(test_name)
}

#[cfg(all(feature = "kafka", feature = "perf-diagnostics"))]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "diagnostic: consume-only isolation"]
async fn test_kafka_consume_only() {
    integration::kafka::test_kafka_consume_only_bench().await;
}

#[cfg(all(feature = "kafka", feature = "perf-diagnostics"))]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "diagnostic: produce-only isolation"]
async fn test_kafka_produce_only() {
    integration::kafka::test_kafka_produce_only_bench().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose"]
async fn test_all_request_reply() {
    println!("--- Running All Request-Reply Tests ---");
    #[cfg(feature = "kafka")]
    if should_run("kafka") {
        integration::route::test_kafka_request_reply().await;
        integration::route::test_kafka_request_reply_multiple_sequential().await;
        integration::route::test_kafka_request_reply_lost_response().await;
    }
    #[cfg(feature = "nats")]
    if should_run("nats") {
        integration::route::test_nats_request_reply().await;
        integration::route::test_nats_core_request_reply().await;
    }
    #[cfg(feature = "mongodb")]
    if should_run("mongodb") {
        integration::route::test_mongodb_request_reply_pattern().await;
        integration::route::test_mongodb_request_reply_multiple_sequential().await;
        integration::route::test_mongodb_request_reply_lost_response().await;
    }
    #[cfg(feature = "amqp")]
    if should_run("amqp") {
        integration::route::test_amqp_request_reply().await;
    }
    #[cfg(feature = "mqtt")]
    if should_run("mqtt") {
        integration::route::test_mqtt_request_reply().await;
    }
    integration::route::test_memory_request_reply().await;
    // Additional memory request-reply tests for multi-message scenarios
    integration::route::test_memory_request_reply_multiple_sequential().await;
    integration::route::test_memory_request_reply_multiple_concurrent().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose"]
async fn test_all_subscriber_logic() {
    println!("--- Running All Subscriber and Request-Reply Logic Tests ---");

    // --- Subscriber Logic ---
    #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
    {
        if should_run("ibm-mq") && integration::ibm_mq::client_available() {
            println!("\n\n>>> Starting IBM MQ Subscriber Logic Test...");
            integration::ibm_mq::test_ibm_mq_subscriber_logic().await;
        }
    }
    #[cfg(feature = "kafka")]
    {
        if should_run("kafka") {
            println!("\n\n>>> Starting Kafka Subscriber Logic Test...");
            integration::kafka::test_kafka_subscriber_logic().await;
        }
    }
    #[cfg(feature = "mongodb")]
    {
        if should_run("mongodb") {
            println!("\n\n>>> Starting MongoDB Subscriber Logic Test...");
            integration::mongodb::test_mongodb_subscriber_logic().await;
        }
    }
    #[cfg(feature = "mqtt")]
    {
        if should_run("mqtt") {
            println!("\n\n>>> Starting MQTT Subscriber Logic Test...");
            integration::mqtt::test_mqtt_subscriber_logic().await;
        }
    }
    #[cfg(feature = "nats")]
    {
        if should_run("nats") {
            println!("\n\n>>> Starting NATS Subscriber Logic Test...");
            integration::nats::test_nats_subscriber_logic().await;
        }
    }
    #[cfg(feature = "redis-streams")]
    {
        if should_run("redis_streams") {
            println!("\n\n>>> Starting Redis Streams Subscriber Logic Test...");
            integration::redis_streams::test_redis_subscriber_logic().await;
        }
    }
    #[cfg(feature = "amqp")]
    {
        if should_run("amqp") {
            println!("\n\n>>> Starting AMQP Subscriber Logic Test...");
            integration::amqp::test_amqp_subscriber_logic().await;
        }
    }
    if should_run("file") {
        println!("\n\n>>> Starting File Subscriber Logic Test...");
        integration::file::test_file_subscriber_logic().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose, takes long time to run"]
async fn test_all_chaos() {
    println!("--- Running All Chaos Tests ---");
    println!("Tests are run sequentially.");

    #[cfg(feature = "kafka")]
    {
        if should_run("kafka") {
            println!("\n\n>>> Starting Kafka Chaos Test...");
            integration::kafka::test_kafka_chaos().await;
        }
    }

    #[cfg(feature = "nats")]
    {
        if should_run("nats") {
            println!("\n\n>>> Starting NATS Chaos Test...");
            integration::nats::test_nats_chaos().await;
        }
    }

    #[cfg(feature = "amqp")]
    {
        if should_run("amqp") {
            println!("\n\n>>> Starting AMQP Chaos Test...");
            integration::amqp::test_amqp_chaos().await;
        }
    }

    #[cfg(feature = "mqtt")]
    {
        if should_run("mqtt") {
            println!("\n\n>>> Starting MQTT Chaos Test...");
            // MQTT chaos tests are currently flaky due to issues with session persistence/QoS handling
            // in the test environment (Mosquitto + rumqttc).
            integration::mqtt::test_mqtt_chaos().await;
        }
    }

    #[cfg(feature = "mongodb")]
    {
        if should_run("mongodb") {
            println!("\n\n>>> Starting MongoDB Chaos Test...");
            integration::mongodb::test_mongodb_chaos().await;
        }
    }

    #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
    {
        if should_run("ibm-mq") && integration::ibm_mq::client_available() {
            println!("\n\n>>> Starting IBM MQ Chaos Test...");
            integration::ibm_mq::test_ibm_mq_chaos().await;
        }
    }

    // AWS chaos test is excluded by default as it requires LocalStack which can be heavy/flaky in some envs
    #[cfg(feature = "sqlx")]
    {
        if should_run("sqlx") || should_run("postgres") {
            integration::postgres::test_postgres_chaos().await;
        }
        if should_run("sqlx") || should_run("mariadb") {
            integration::mariadb::test_mariadb_chaos().await;
        }
        if should_run("sqlx") || should_run("mysql") {
            integration::mysql::test_mysql_chaos().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose, takes long time to run"]
async fn test_all_status() {
    println!("--- Running All Status Tests ---");
    println!("Tests are run sequentially.");

    #[cfg(feature = "kafka")]
    {
        if should_run("kafka") {
            println!("\n\n>>> Starting Kafka Status Test...");
            integration::kafka::test_kafka_status().await;
        }
    }

    #[cfg(feature = "nats")]
    {
        if should_run("nats") {
            println!("\n\n>>> Starting NATS Status Test...");
            integration::nats::test_nats_status().await;
        }
    }

    #[cfg(feature = "redis-streams")]
    {
        if should_run("redis_streams") {
            println!("\n\n>>> Starting Redis Streams Status Test...");
            integration::redis_streams::test_redis_status().await;
        }
    }

    #[cfg(feature = "amqp")]
    {
        if should_run("amqp") {
            println!("\n\n>>> Starting AMQP Status Test...");
            integration::amqp::test_amqp_status().await;
        }
    }

    #[cfg(feature = "mqtt")]
    {
        if should_run("mqtt") {
            println!("\n\n>>> Starting MQTT Status Test...");
            integration::mqtt::test_mqtt_status().await;
        }
    }

    #[cfg(feature = "mongodb")]
    {
        if should_run("mongodb") {
            println!("\n\n>>> Starting MongoDB Status Test...");
            integration::mongodb::test_mongodb_status().await;
        }
    }

    #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
    {
        if should_run("ibm-mq") && integration::ibm_mq::client_available() {
            println!("\n\n>>> Starting IBM MQ Status Test...");
            integration::ibm_mq::test_ibm_mq_status().await;
        }
    }

    #[cfg(feature = "sqlx")]
    {
        if should_run("sqlx") || should_run("postgres") {
            integration::postgres::test_postgres_status().await;
        }
        if should_run("sqlx") || should_run("mariadb") {
            integration::mariadb::test_mariadb_status().await;
        }
        if should_run("sqlx") || should_run("sqlite") {
            integration::sqlite::test_sqlite_status().await;
        }
        if should_run("sqlx") || should_run("mysql") {
            integration::mysql::test_mysql_status().await;
        }
    }

    #[cfg(feature = "aws")]
    {
        if should_run("aws") {
            println!("\n\n>>> Starting AWS Status Test...");
            integration::aws::test_aws_status().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose, takes long time to run"]
async fn test_all_performance_pipeline() {
    let _summary_printer = mq_bridge::test_utils::PerformanceSummaryPrinter;
    println!("--- Running All Performance Pipeline Tests ---");
    #[cfg(feature = "kafka")]
    {
        if should_run("kafka") {
            println!("\n\n>>> Starting Kafka Performance Pipeline Test...");
            integration::kafka::test_kafka_performance_pipeline().await;
        }
    }
    #[cfg(feature = "aws")]
    {
        if should_run("aws") {
            println!("\n\n>>> Starting AWS Performance Pipeline Test...");
            integration::aws::test_aws_performance_pipeline().await;
        }
    }
    #[cfg(feature = "amqp")]
    {
        if should_run("amqp") {
            println!("\n\n>>> Starting AMQP Performance Pipeline Test...");
            integration::amqp::test_amqp_performance_pipeline().await;
        }
    }
    #[cfg(feature = "mqtt")]
    {
        if should_run("mqtt") {
            println!("\n\n>>> Starting MQTT Performance Pipeline Test...");
            integration::mqtt::test_mqtt_performance_pipeline().await;
        }
    }
    #[cfg(feature = "nats")]
    {
        if should_run("nats") {
            println!("\n\n>>> Starting NATS Performance Pipeline Test...");
            integration::nats::test_nats_performance_pipeline().await;
        }
    }
    #[cfg(feature = "mongodb")]
    {
        if should_run("mongodb") {
            println!("\n\n>>> Starting MongoDB Performance Pipeline Test...");
            integration::mongodb::test_mongodb_performance_pipeline().await;
        }
        if should_run("mongodb_replica_set") {
            println!("\n\n>>> Starting MongoDB Replica Set Performance Pipeline Test...");
            integration::mongodb::test_mongodb_replica_set_pipeline().await;
        }
    }
    #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
    {
        if should_run("ibm-mq") && integration::ibm_mq::client_available() {
            println!("\n\n>>> Starting IBM MQ Performance Pipeline Test...");
            integration::ibm_mq::test_ibm_mq_performance_pipeline().await;
        }
    }
    #[cfg(feature = "zeromq")]
    {
        if should_run("zeromq") {
            println!("\n\n>>> Starting ZeroMQ Performance Pipeline Test...");
            integration::zeromq::test_zeromq_performance_pipeline().await;
        }
    }
    #[cfg(feature = "redis-streams")]
    {
        if should_run("redis_streams") {
            println!("\n\n>>> Starting Redis Streams Performance Pipeline Test...");
            integration::redis_streams::test_redis_performance_pipeline().await;
        }
    }
    #[cfg(feature = "grpc")]
    {
        if should_run("grpc") {
            println!("\n\n>>> Starting gRPC Performance Pipeline Test...");
            integration::grpc::test_grpc_performance_pipeline().await;
        }
    }
    #[cfg(feature = "http")]
    {
        if should_run("http") {
            println!("\n\n>>> Starting HTTP Performance Pipeline Test...");
            integration::http::test_http_performance_pipeline().await;
        }
    }
    #[cfg(feature = "websocket")]
    {
        if should_run("websocket") {
            println!("\n\n>>> Starting WebSocket Performance Pipeline Test...");
            integration::websocket::test_websocket_performance_pipeline().await;
        }
    }
    #[cfg(feature = "sqlx")]
    {
        if should_run("sqlx") || should_run("postgres") {
            integration::postgres::test_postgres_performance_pipeline().await;
        }
        if should_run("sqlx") || should_run("mysql") {
            integration::mysql::test_mysql_performance_pipeline().await;
        }
        if should_run("sqlx") || should_run("mariadb") {
            integration::mariadb::test_mariadb_performance_pipeline().await;
        }
        if should_run("sqlx") || should_run("sqlite") {
            integration::sqlite::test_sqlite_performance_pipeline().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose, takes long time to run"]
async fn test_all_performance_direct() {
    // This instance will print the summary table when it's dropped at the end of the test.
    let _summary_printer = mq_bridge::test_utils::PerformanceSummaryPrinter;

    println!("--- Running All Direct Performance Tests ---");
    println!("Tests are run sequentially to ensure accurate measurements.");

    #[cfg(feature = "mongodb")]
    {
        if should_run("mongodb_rs") {
            println!("\n\n>>> Starting MongoDB Replica Set Direct Performance Test...");
            integration::mongodb::test_mongodb_replica_set_performance_direct().await;
        }
        if should_run("mongodb_direct") {
            println!("\n\n>>> Starting MongoDB Direct Performance Test...");
            integration::mongodb::test_mongodb_performance_direct().await;
        }
    }
    #[cfg(feature = "aws")]
    {
        if should_run("aws") {
            println!("\n\n>>> Starting AWS Direct Performance Test...");
            integration::aws::test_aws_performance_direct().await;
        }
    }
    #[cfg(feature = "nats")]
    {
        if should_run("nats") {
            println!("\n\n>>> Starting NATS Direct Performance Test...");
            integration::nats::test_nats_performance_direct().await;
        }
    }
    #[cfg(feature = "mqtt")]
    {
        if should_run("mqtt") {
            println!("\n\n>>> Starting MQTT Direct Performance Test...");
            integration::mqtt::test_mqtt_performance_direct().await;
        }
    }
    #[cfg(feature = "kafka")]
    {
        if should_run("kafka") {
            println!("\n\n>>> Starting Kafka Direct Performance Test...");
            integration::kafka::test_kafka_performance_direct().await;
        }
    }
    #[cfg(feature = "amqp")]
    {
        if should_run("amqp") {
            println!("\n\n>>> Starting AMQP Direct Performance Test...");
            integration::amqp::test_amqp_performance_direct().await;
        }
    }
    #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
    {
        if should_run("ibm-mq") && integration::ibm_mq::client_available() {
            println!("\n\n>>> Starting IBM MQ Direct Performance Test...");
            integration::ibm_mq::test_ibm_mq_performance_direct().await;
        }
    }
    #[cfg(feature = "sqlx")]
    {
        if should_run("sqlx") || should_run("postgres") {
            integration::postgres::test_postgres_performance_direct().await;
        }
        if should_run("sqlx") || should_run("mariadb") {
            integration::mariadb::test_mariadb_performance_direct().await;
        }
        if should_run("sqlx") || should_run("sqlite") {
            integration::sqlite::test_sqlite_performance_direct().await;
        }
        if should_run("sqlx") || should_run("mysql") {
            integration::mysql::test_mysql_performance_direct().await;
        }
    }
    // The summary table will be printed here when `_summary_printer` is dropped.
}
