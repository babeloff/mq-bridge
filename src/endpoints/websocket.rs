//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::models::WebSocketConfig;
use crate::traits::{
    CommitFunc, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, trace, warn};
use uuid::Uuid;

type WebSocketSourceMessage = (CanonicalMessage, CommitFunc);
type WebSocketResponseTx = tokio::sync::mpsc::Sender<Message>;

pub struct WebSocketConsumer {
    request_rx: tokio::sync::mpsc::Receiver<WebSocketSourceMessage>,
    shutdown_tx: watch::Sender<bool>,
    buffer_size: usize,
    url: String,
    bound_addr: SocketAddr,
}

impl WebSocketConsumer {
    pub async fn new(config: &WebSocketConfig) -> anyhow::Result<Self> {
        let buffer_size = config.internal_buffer_size.unwrap_or(100).max(1);
        let listen_addr: SocketAddr = config
            .url
            .parse()
            .with_context(|| format!("Invalid listen address: {}", config.url))?;
        let listener = TcpListener::bind(listen_addr).await?;
        let bound_addr = listener.local_addr()?;
        let path = config.path.as_deref().map(normalize_websocket_path);
        let message_id_header = config
            .message_id_header
            .clone()
            .unwrap_or_else(|| "message-id".to_string());
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(buffer_size);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        spawn_accept_loop(
            listener,
            request_tx,
            shutdown_rx,
            path.clone(),
            message_id_header,
        );

        let url = if let Some(path) = path {
            format!("ws://{}{}", bound_addr, path)
        } else {
            format!("ws://{}", bound_addr)
        };

        Ok(Self {
            request_rx,
            shutdown_tx,
            buffer_size,
            url,
            bound_addr,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }
}

impl Drop for WebSocketConsumer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

pub struct WebSocketPublisher {
    url: String,
}

impl WebSocketPublisher {
    pub fn new(config: &WebSocketConfig) -> Self {
        Self {
            url: config.url.clone(),
        }
    }
}

#[derive(Clone, Default)]
struct HandshakeMetadata {
    path: String,
    message_id: Option<u128>,
    headers: HashMap<String, String>,
}

fn spawn_accept_loop(
    listener: TcpListener,
    request_tx: tokio::sync::mpsc::Sender<WebSocketSourceMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    expected_path: Option<String>,
    message_id_header: String,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = match accept_result {
                        Ok(parts) => parts,
                        Err(error) => {
                            warn!(error = %error, "WebSocket accept failed");
                            continue;
                        }
                    };

                    let request_tx = request_tx.clone();
                    let expected_path = expected_path.clone();
                    let message_id_header = message_id_header.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(
                            stream,
                            peer_addr,
                            request_tx,
                            expected_path,
                            message_id_header,
                        )
                        .await
                        {
                            debug!(error = %error, %peer_addr, "WebSocket connection closed with error");
                        }
                    });
                }
            }
        }
    });
}

#[allow(clippy::result_large_err)] // TODO: find a good fix for the box issue
async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    request_tx: tokio::sync::mpsc::Sender<WebSocketSourceMessage>,
    expected_path: Option<String>,
    message_id_header: String,
) -> anyhow::Result<()> {
    let handshake = Arc::new(Mutex::new(HandshakeMetadata::default()));
    let handshake_capture = Arc::clone(&handshake);
    let ws_stream = accept_hdr_async(stream, move |request: &Request, response: Response| {
        if let Some(expected_path) = expected_path.as_deref() {
            let actual_path = normalize_websocket_path(request.uri().path());
            if actual_path != expected_path {
                return Err(reject_handshake(
                    StatusCode::NOT_FOUND,
                    format!("Unexpected websocket path '{}'", actual_path),
                ));
            }
        }

        let mut metadata = HandshakeMetadata {
            path: request.uri().path().to_string(),
            message_id: request
                .headers()
                .get(message_id_header.as_str())
                .and_then(|value| value.to_str().ok())
                .and_then(parse_message_id),
            headers: HashMap::new(),
        };

        for (name, value) in request.headers() {
            let name_str = name.as_str();
            if matches!(
                name_str,
                "authorization"
                    | "cookie"
                    | "set-cookie"
                    | "proxy-authorization"
                    | "x-api-key"
                    | "session"
            ) {
                continue;
            }

            if let Ok(value) = value.to_str() {
                metadata
                    .headers
                    .insert(format!("ws_header.{}", name_str), value.to_string());
            }
        }

        if let Ok(mut captured) = handshake_capture.lock() {
            *captured = metadata;
        }

        Ok(response)
    })
    .await?;

    let metadata = handshake
        .lock()
        .map(|captured| captured.clone())
        .unwrap_or_default();
    let (mut write_stream, mut read_stream) = ws_stream.split();
    let (response_tx, mut response_rx) = tokio::sync::mpsc::channel::<Message>(16);
    let writer_peer_addr = peer_addr;
    let writer_task = tokio::spawn(async move {
        while let Some(message) = response_rx.recv().await {
            if let Err(error) = write_stream.send(message).await {
                debug!(error = %error, %writer_peer_addr, "Failed to send WebSocket response");
                break;
            }
        }
    });

    while let Some(frame) = read_stream.next().await {
        let frame = frame?;
        let (payload, message_type) = match frame {
            Message::Text(text) => (text.to_string().into_bytes(), "text"),
            Message::Binary(binary) => (binary.to_vec(), "binary"),
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => continue,
            _ => continue,
        };

        let mut message = CanonicalMessage::new(payload, metadata.message_id);
        message
            .metadata
            .insert("ws_message_type".to_string(), message_type.to_string());
        message
            .metadata
            .insert("ws_path".to_string(), metadata.path.clone());
        message
            .metadata
            .insert("ws_peer_addr".to_string(), peer_addr.to_string());
        message.metadata.extend(metadata.headers.clone());

        let response_tx = response_tx.clone();
        let commit: CommitFunc =
            Box::new(move |disposition| websocket_commit(disposition, response_tx));
        if request_tx.send((message, commit)).await.is_err() {
            break;
        }
    }

    drop(response_tx);
    let _ = writer_task.await;
    Ok(())
}

fn websocket_commit(
    disposition: MessageDisposition,
    response_tx: WebSocketResponseTx,
) -> futures::future::BoxFuture<'static, anyhow::Result<()>> {
    Box::pin(async move {
        match disposition {
            MessageDisposition::Reply(message) => {
                response_tx
                    .send(canonical_to_websocket_message(&message))
                    .await
                    .map_err(|_| anyhow!("WebSocket connection closed before response was sent"))?;
            }
            MessageDisposition::Ack | MessageDisposition::Nack => {}
        }
        Ok(())
    })
}

fn reject_handshake(status: StatusCode, body: String) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(body));
    *response.status_mut() = status;
    response
}

fn normalize_websocket_path(path: &str) -> String {
    if path.is_empty() || path == "/" {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn parse_message_id(raw: &str) -> Option<u128> {
    if let Ok(uuid) = Uuid::parse_str(raw) {
        Some(uuid.as_u128())
    } else if raw.starts_with("0x") || raw.starts_with("0X") {
        u128::from_str_radix(raw.trim_start_matches("0x").trim_start_matches("0X"), 16).ok()
    } else {
        raw.parse::<u128>().ok()
    }
}

fn canonical_to_websocket_message(message: &CanonicalMessage) -> Message {
    let message_type = message.metadata.get("ws_message_type").map(String::as_str);
    match message_type {
        Some("binary") => Message::Binary(message.payload.clone().to_vec().into()),
        Some("text") => Message::Text(message.get_payload_str().into_owned().into()),
        _ => match std::str::from_utf8(&message.payload) {
            Ok(text) => Message::Text(text.to_string().into()),
            Err(_) => Message::Binary(message.payload.clone().to_vec().into()),
        },
    }
}

#[async_trait]
impl MessageConsumer for WebSocketConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let max_messages = max_messages.max(1);
        let (first_message, first_commit) = self
            .request_rx
            .recv()
            .await
            .ok_or_else(|| ConsumerError::EndOfStream)?;

        let mut messages = vec![first_message];
        let mut commits = vec![first_commit];

        while messages.len() < max_messages {
            match self.request_rx.try_recv() {
                Ok((message, commit)) => {
                    messages.push(message);
                    commits.push(commit);
                }
                Err(_) => break,
            }
        }

        let batch_commit: crate::traits::BatchCommitFunc =
            Box::new(move |dispositions: Vec<MessageDisposition>| {
                Box::pin(async move {
                    for (commit, disposition) in commits.into_iter().zip(dispositions) {
                        commit(disposition).await?;
                    }
                    Ok(())
                })
            });

        Ok(ReceivedBatch {
            messages,
            commit: batch_commit,
        })
    }

    async fn status(&self) -> crate::traits::EndpointStatus {
        crate::traits::EndpointStatus {
            healthy: true,
            target: self.url.clone(),
            pending: Some(self.request_rx.len()),
            capacity: Some(self.buffer_size),
            details: serde_json::json!({
                "bound_addr": self.bound_addr.to_string(),
                "buffer_size": self.buffer_size,
            }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl MessagePublisher for WebSocketPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        trace!(url = %self.url, count = messages.len(), "Sending WebSocket batch");
        let (mut stream, _) = connect_async(&self.url)
            .await
            .with_context(|| format!("Failed to connect to WebSocket endpoint '{}'", self.url))
            .map_err(PublisherError::Connection)?;

        for message in messages {
            stream
                .send(canonical_to_websocket_message(&message))
                .await
                .map_err(|error| PublisherError::Retryable(anyhow!(error)))?;
        }

        let _ = stream.close(None).await;
        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_consumer_publisher_integration() {
        let consumer_config = WebSocketConfig::new("127.0.0.1:0").with_path("/events");
        let mut consumer = WebSocketConsumer::new(&consumer_config)
            .await
            .expect("consumer should start");

        let publisher = WebSocketPublisher::new(&WebSocketConfig::new(consumer.url().to_string()));
        publisher
            .send(
                CanonicalMessage::from_vec("hello websocket")
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
        assert_eq!(message.get_payload_str(), "hello websocket");
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
}
