#![allow(unused_imports)]
#![cfg(any(feature = "ibm-mq", feature = "ibm-mq-static"))]

use std::sync::Arc;

use crate::integration::tls_helpers;
use mq_bridge::endpoints::ibm_mq::{IbmMqConsumer, IbmMqPublisher};
use mq_bridge::test_utils::run_test_with_docker;
use mq_bridge::test_utils::setup_logging;
use mq_bridge::traits::{MessageConsumer, MessagePublisher};

#[tokio::test]
#[ignore = "requires docker compose and openssl/runmqakm"]
async fn test_ibm_mq_tls_roundtrip() {
    // Generates local cert material and starts the TLS IBM MQ compose stack.
    // Requires IBM MQ key database tooling in addition to Docker Compose.
    setup_logging();

    // The dlopen build only has a usable client when the IBM MQ redistributable
    // is installed (e.g. the dedicated ibm-mq workflow). Skip rather than panic
    // where it isn't, matching the test_all_* runners.
    if !crate::integration::ibm_mq::client_available() {
        eprintln!("IBM MQ client library not available; skipping test_ibm_mq_tls_roundtrip");
        return;
    }

    // Generate certs and build TLS-enabled IBM MQ config
    let cert_dir = tls_helpers::generate_service_certs("ibm-mq").expect("generate certs");

    run_test_with_docker(
        "tests/integration/docker-compose/ibm_mq_tls.yml",
        || async {
            let mut cfg = tls_helpers::ibm_mq_config_with_tls(&cert_dir, "QM1", "DEV.APP.SVRCONN");
            cfg.username = Some("app".to_string());
            cfg.password = Some("adminpass".to_string());
            cfg.queue = Some("DEV.QUEUE.1".to_string());

            let publisher = Arc::new(
                IbmMqPublisher::new(&cfg)
                    .await
                    .expect("Failed to create publisher"),
            );
            let mut consumer = IbmMqConsumer::new(&cfg)
                .await
                .expect("Failed to create consumer");

            // Publish and receive
            let msg = mq_bridge::CanonicalMessage::from_vec("hello");
            publisher.send(msg).await.expect("publish failed");
            let received = consumer.receive().await.expect("receive failed");
            assert_eq!(received.message.payload, b"hello".to_vec());
        },
    )
    .await;
}
