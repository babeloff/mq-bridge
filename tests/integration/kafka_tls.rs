#![allow(unused_imports)]
#[cfg(feature = "kafka")]
mod kafka_tls {
    use std::process::Command;
    use std::path::PathBuf;

    use mq_bridge::test_utils::run_test_with_docker;
    use mq_bridge::endpoints::kafka::{KafkaConsumer, KafkaPublisher};
    use mq_bridge::models::KafkaConfig;

    use mq_bridge::test_utils::setup_logging;

    #[tokio::test]
    #[ignore = "requires docker compose and keytool/openssl"]
    async fn test_kafka_tls_publish_consume() {
        setup_logging();

        // Generate certs and build TLS-enabled Kafka config
        let cert_dir = crate::tls_helpers::generate_service_certs("kafka").expect("generate certs");

        run_test_with_docker("tests/integration/docker-compose/kafka-tls.yml", || async {
            let topic = format!("tls_topic_{}", fast_uuid_v7::gen_id());
            let mut cfg = crate::tls_helpers::kafka_config_with_tls(&cert_dir, topic.clone());
            cfg = cfg.with_consumer_option("auto.offset.reset", "earliest");

            let publisher = KafkaPublisher::new(&cfg).await.expect("create publisher");
            let mut consumer = KafkaConsumer::new(&cfg).await.expect("create consumer");

            let payload = b"kafka-tls-test".to_vec();
            let msg = mq_bridge::CanonicalMessage::new(payload.clone(), None);
            publisher.send(msg).await.expect("publish failed");

            // Poll for the message
            let received = consumer.receive().await.expect("receive failed");
            assert_eq!(received.message.payload, payload);
        })
        .await;
    }
}
