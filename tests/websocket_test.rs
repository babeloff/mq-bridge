#![cfg(feature = "websocket")]

use mq_bridge::endpoints::websocket::WebSocketConsumer;
use mq_bridge::endpoints::websocket::WebSocketPublisher;
use mq_bridge::models::WebSocketConfig;
use mq_bridge::traits::{MessageConsumer, MessageDisposition, MessagePublisher};
use mq_bridge::CanonicalMessage;

#[tokio::test(flavor = "multi_thread")]
async fn websocket_endpoint_roundtrip() {
    let mut consumer =
        WebSocketConsumer::new(&WebSocketConfig::new("127.0.0.1:0").with_path("/events"))
            .await
            .expect("consumer should be created");

    let publisher = WebSocketPublisher::new(&WebSocketConfig::new(consumer.url().to_string()));

    publisher
        .send(
            CanonicalMessage::from_vec("hello integration")
                .with_metadata_kv("ws_message_type", "text"),
        )
        .await
        .expect("publisher should send");

    let mut batch = consumer
        .receive_batch(1)
        .await
        .expect("consumer should receive");
    assert_eq!(batch.messages.len(), 1);
    let message = batch.messages.pop().expect("one message");
    assert_eq!(message.get_payload_str(), "hello integration");
    assert_eq!(
        message.metadata.get("ws_message_type").map(String::as_str),
        Some("text")
    );
    assert_eq!(
        message.metadata.get("ws_path").map(String::as_str),
        Some("/events")
    );
    (batch.commit)(vec![MessageDisposition::Ack])
        .await
        .expect("commit should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_endpoint_handles_binary_payloads() {
    let mut consumer =
        WebSocketConsumer::new(&WebSocketConfig::new("127.0.0.1:0").with_path("/binary"))
            .await
            .expect("consumer should be created");

    let publisher = WebSocketPublisher::new(&WebSocketConfig::new(consumer.url().to_string()));

    publisher
        .send(
            CanonicalMessage::new(vec![0, 1, 2, 3, 255], None)
                .with_metadata_kv("ws_message_type", "binary"),
        )
        .await
        .expect("publisher should send");

    let mut batch = consumer
        .receive_batch(1)
        .await
        .expect("consumer should receive");
    assert_eq!(batch.messages.len(), 1);
    let message = batch.messages.pop().expect("one message");
    assert_eq!(message.payload.as_ref(), &[0, 1, 2, 3, 255]);
    assert_eq!(
        message.metadata.get("ws_message_type").map(String::as_str),
        Some("binary")
    );
    (batch.commit)(vec![MessageDisposition::Ack])
        .await
        .expect("commit should succeed");
}
