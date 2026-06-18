#![cfg(feature = "websocket")]

use futures::{SinkExt, StreamExt};
use mq_bridge::endpoints::websocket::WebSocketConsumer;
use mq_bridge::endpoints::websocket::WebSocketPublisher;
use mq_bridge::models::{Endpoint, EndpointType, WebSocketConfig};
use mq_bridge::traits::{MessageConsumer, MessageDisposition, MessagePublisher};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

async fn echo(msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    Ok(Handled::Publish(msg))
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

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

#[tokio::test(flavor = "multi_thread")]
async fn websocket_consumer_sends_commit_reply_to_client() {
    let mut consumer =
        WebSocketConsumer::new(&WebSocketConfig::new("127.0.0.1:0").with_path("/reply"))
            .await
            .expect("consumer should be created");

    let (mut stream, _) = connect_async(consumer.url())
        .await
        .expect("client should connect");

    stream
        .send(Message::Text("request".into()))
        .await
        .expect("client should send request");

    let mut batch = consumer
        .receive_batch(1)
        .await
        .expect("consumer should receive request");
    assert_eq!(batch.messages.len(), 1);
    let request = batch.messages.pop().expect("one request");
    assert_eq!(request.get_payload_str(), "request");

    (batch.commit)(vec![MessageDisposition::Reply(
        CanonicalMessage::from_vec("response").with_metadata_kv("ws_message_type", "text"),
    )])
    .await
    .expect("reply commit should succeed");

    let reply = stream
        .next()
        .await
        .expect("client should receive response")
        .expect("response frame should be valid");
    assert_eq!(reply, Message::Text("response".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_default_inline_response_fast_path_replies_with_handler() {
    let port = get_free_port();
    let input = Endpoint::new(EndpointType::WebSocket(
        WebSocketConfig::new(format!("127.0.0.1:{port}")).with_path("/inline"),
    ));
    let output = Endpoint::new_response();
    let route = Route::new(input, output).with_handler(echo);
    let handle = route
        .run(&format!("websocket-inline-response-fast-path-{port}"))
        .await
        .expect("route should start");

    let (mut stream, _) = connect_async(format!("ws://127.0.0.1:{port}/inline"))
        .await
        .expect("client should connect");
    stream
        .send(Message::Text("request".into()))
        .await
        .expect("client should send request");

    let reply = stream
        .next()
        .await
        .expect("client should receive response")
        .expect("response frame should be valid");
    assert_eq!(reply, Message::Text("request".into()));

    handle.stop().await;
    handle.join().await.expect("route task should finish");
}
