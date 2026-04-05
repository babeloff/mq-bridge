// To run these tests, use the command from the project root:
// cargo test --test performance_test -- --ignored --nocapture --test-threads=1

#![allow(unused_imports, dead_code, unused_variables)]

#[path = "integration/mod.rs"]
mod integration;

pub fn should_run(test_name: &str) -> bool {
    mq_bridge::test_utils::should_run(test_name)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker compose, takes long time to run"] // This is a performance test, run it explicitly
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
    #[cfg(feature = "ibm-mq")]
    {
        if should_run("ibm-mq") {
            println!("\n\n>>> Starting IBM MQ Direct Performance Test...");
            integration::ibm_mq::test_ibm_mq_performance_direct().await;
        }
    }
    #[cfg(feature = "sqlx")]
    {
        if should_run("sqlx") {
            println!("\n\n>>> Starting SQLx Direct Performance Test...");
            integration::sqlx::test_sqlx_performance_direct().await;
        }
    }

    // The summary table will be printed here when `_summary_printer` is dropped.
}
