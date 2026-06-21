#![allow(unused_imports)]
#[cfg(feature = "mongodb")]
mod mongodb_tls {
    use std::process::Command;
    use std::path::PathBuf;

    use crate::super::setup_logging;
    use mq_bridge::test_utils::run_test_with_docker;

    use mongodb::bson::doc;

    #[tokio::test]
    #[ignore = "requires docker compose and openssl"]
    async fn test_mongodb_tls_connect() {
        // Generates local cert material and starts the TLS MongoDB compose stack.
        setup_logging();

        // Generate certs and build TLS-enabled MongoDB config
        let cert_dir = crate::tls_helpers::generate_service_certs("mongodb").expect("generate certs");

        run_test_with_docker("tests/integration/docker-compose/mongodb-tls.yml", || async {
            let collection = format!("tls_test_{}", fast_uuid_v7::gen_id());
            let config = crate::tls_helpers::mongo_config_with_tls(&cert_dir, "mq_bridge_test_tls", collection.clone());

            let publisher = mq_bridge::endpoints::mongodb::MongoDbPublisher::new(&config)
                .await
                .expect("Failed to create MongoDbPublisher");
            let mut consumer = mq_bridge::endpoints::mongodb::MongoDbConsumer::new(&config)
                .await
                .expect("Failed to create MongoDbConsumer");

            // Publish and receive
            let msg_payload = b"tls-test-payload".to_vec();
            let msg = mq_bridge::CanonicalMessage::new(msg_payload.clone(), None);
            publisher.send(msg).await.expect("publish failed");
            let received = consumer.receive().await.expect("receive failed");
            assert_eq!(received.message.payload, msg_payload);
        })
        .await;
    }
}
