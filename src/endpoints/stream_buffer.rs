//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Correlated in-process stream response buffer endpoint.
//!
//! `stream_buffer` is a small endpoint for workflows where one publisher send
//! produces many response messages. The main use case is
//! `HttpConfig::stream_response_to`: an HTTP publisher sends a request as usual,
//! reads a streaming HTTP response, and publishes each response item into this
//! buffer with a `correlation_id`.
//!
//! The buffer is partitioned by `(topic, correlation_id)`. Publishers write to a
//! topic and require each message to carry `metadata["correlation_id"]`.
//! Consumers must be configured with both the same topic and the exact
//! correlation id they want to read. This makes parallel streams safe by
//! default: a consumer for request A cannot accidentally drain response items
//! for request B.
//!
//! Commit semantics are intentionally mq-bridge-like:
//!
//! - received messages stay uncommitted until the returned batch commit
//!   function is called;
//! - `Ack` finalizes the read;
//! - `Nack` or dropping the batch without committing requeues messages to the
//!   same correlation partition;
//! - acking an end marker with `metadata["http_stream_end"] == "true"` removes
//!   that correlation partition.
//!
//! # HTTP streaming response example
//!
//! ```rust,ignore
//! use mq_bridge::models::{Endpoint, EndpointType, HttpConfig, StreamBufferConfig};
//! use mq_bridge::{CanonicalMessage, Payload};
//!
//! let buffer_topic = "llm-response-streams";
//! let correlation_id = "request-42";
//!
//! // Configure the HTTP publisher to capture streamed response items into the
//! // shared stream_buffer topic. This endpoint is publisher-only, so it does
//! // not set a correlation_id.
//! let http = Endpoint::new(EndpointType::Http(HttpConfig {
//!     url: "http://127.0.0.1:8000/v1/generate".to_string(),
//!     stream_response_to: Some(Box::new(Endpoint::new(EndpointType::StreamBuffer(
//!         StreamBufferConfig::new(buffer_topic).with_capacity(100),
//!     )))),
//!     ..Default::default()
//! }));
//!
//! // Send the request with an explicit correlation id. If you omit this
//! // metadata, the HTTP publisher uses format!("{:032x}", message.message_id).
//! let mut request = CanonicalMessage::new(Payload::Text("hello".into()));
//! request.metadata.insert("correlation_id".into(), correlation_id.into());
//! // create_publisher(&http).await?.send(request).await?;
//!
//! // Read only this request's streamed responses using the same topic and
//! // correlation id. Other parallel HTTP responses remain isolated.
//! let responses = Endpoint::new(EndpointType::StreamBuffer(
//!     StreamBufferConfig::new(buffer_topic)
//!         .with_correlation_id(correlation_id)
//!         .with_capacity(100),
//! ));
//! // let batch = create_consumer(&responses).await?.receive_batch(10).await?;
//! // (batch.commit)(MessageDisposition::Ack).await?;
//!
//!
//!
//! ```

/// The publisher side writes messages to a shared `topic`. Each message must
/// include `metadata["correlation_id"]`; HTTP response streaming adds this
/// automatically from the request `correlation_id`, or falls back to the
/// request `message_id` formatted as 32 lowercase hexadecimal characters.
///
/// The consumer side must be configured with both `topic` and `correlation_id`.
/// It only receives messages for that correlation partition, so parallel HTTP
/// streaming responses do not share a FIFO queue or consume each other's data.
///
/// Messages are removed from the buffer only when the received batch is acked.
/// Nacked or dropped uncommitted batches are requeued to the same correlation
/// partition. Acking a message with `metadata["http_stream_end"] == "true"`
/// cleans up that partition.
///
/// # Example
///
/// ```rust,ignore
/// use mq_bridge::models::{Endpoint, EndpointType, HttpConfig, StreamBufferConfig};
///
/// let responses = Endpoint::new(EndpointType::StreamBuffer(StreamBufferConfig {
///     topic: "llm-responses".to_string(),
///     correlation_id: None,
///     capacity: Some(100),
/// }));
///
/// let http_publisher = Endpoint::new(EndpointType::Http(HttpConfig {
///     url: "http://127.0.0.1:8000/generate".to_string(),
///     stream_response_to: Some(Box::new(responses)),
///     ..Default::default()
/// }));
///
/// // A UI or route can later read only this request's streamed responses:
/// let response_consumer = Endpoint::new(EndpointType::StreamBuffer(
///     StreamBufferConfig::new("llm-responses")
///         .with_correlation_id("request-123")
///         .with_capacity(100),
/// ));
/// ```
use crate::models::StreamBufferConfig;
use crate::traits::{
    BatchCommitFunc, BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::anyhow;
use async_channel::{bounded, Receiver, Sender};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{trace, warn};

const DEFAULT_CAPACITY: usize = 100;

#[derive(Clone)]
struct StreamPartition {
    sender: Sender<Vec<CanonicalMessage>>,
    receiver: Receiver<Vec<CanonicalMessage>>,
}

impl StreamPartition {
    fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self { sender, receiver }
    }
}

type StreamTopic = HashMap<String, StreamPartition>;

static STREAM_BUFFERS: Lazy<Mutex<HashMap<String, Arc<Mutex<StreamTopic>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn get_or_create_topic(topic: &str) -> Arc<Mutex<StreamTopic>> {
    let mut buffers = STREAM_BUFFERS
        .lock()
        .expect("stream buffer registry poisoned");
    buffers
        .entry(topic.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

fn get_or_create_partition(topic: &str, correlation_id: &str, capacity: usize) -> StreamPartition {
    let topic = get_or_create_topic(topic);
    let mut partitions = topic.lock().expect("stream buffer topic poisoned");
    partitions
        .entry(correlation_id.to_string())
        .or_insert_with(|| StreamPartition::new(capacity))
        .clone()
}

fn remove_partition_if_current(
    topic: &str,
    correlation_id: &str,
    sender: &Sender<Vec<CanonicalMessage>>,
) {
    let topic = get_or_create_topic(topic);
    let mut partitions = topic.lock().expect("stream buffer topic poisoned");
    let should_remove = partitions
        .get(correlation_id)
        .is_some_and(|partition| partition.sender.same_channel(sender));
    if should_remove {
        partitions.remove(correlation_id);
        sender.close();
    }
}

/// Publisher side of the `stream_buffer` endpoint.
///
/// `send` appends a message to the partition selected by
/// `message.metadata["correlation_id"]`. It does not accept messages without a
/// correlation id because otherwise consumers could not safely read one stream
/// without also consuming another stream's responses.
#[derive(Debug, Clone)]
pub struct StreamBufferPublisher {
    topic: String,
    capacity: usize,
}

impl StreamBufferPublisher {
    pub fn new(config: &StreamBufferConfig) -> anyhow::Result<Self> {
        Ok(Self {
            topic: config.topic.clone(),
            capacity: config.capacity.unwrap_or(DEFAULT_CAPACITY),
        })
    }
}

#[async_trait]
impl MessagePublisher for StreamBufferPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let correlation_id = message
            .metadata
            .get("correlation_id")
            .cloned()
            .ok_or_else(|| {
                PublisherError::NonRetryable(anyhow!(
                    "stream_buffer publisher requires message metadata 'correlation_id'"
                ))
            })?;
        let partition = get_or_create_partition(&self.topic, &correlation_id, self.capacity);
        partition
            .sender
            .send(vec![message])
            .await
            .map_err(|e| anyhow!("Failed to send to stream_buffer: {}", e))?;
        Ok(Sent::Ack)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut messages = messages;
        let Some(first) = messages.first() else {
            return Ok(SentBatch::Ack);
        };
        let correlation_id = first
            .metadata
            .get("correlation_id")
            .cloned()
            .ok_or_else(|| {
                PublisherError::NonRetryable(anyhow!(
                    "stream_buffer publisher requires message metadata 'correlation_id'"
                ))
            })?;
        if messages
            .iter()
            .any(|message| message.metadata.get("correlation_id") != Some(&correlation_id))
        {
            return Err(PublisherError::NonRetryable(anyhow!(
                "stream_buffer publisher batch requires a single shared correlation_id"
            )));
        }
        let partition = get_or_create_partition(&self.topic, &correlation_id, self.capacity);
        if partition.sender.is_closed() {
            return Err(PublisherError::Retryable(anyhow!(
                "stream_buffer partition is closed"
            )));
        }
        partition
            .sender
            .send(std::mem::take(&mut messages))
            .await
            .map_err(|e| anyhow!("Failed to send to stream_buffer: {}", e))?;
        Ok(SentBatch::Ack)
    }

    async fn status(&self) -> EndpointStatus {
        EndpointStatus {
            healthy: true,
            target: self.topic.clone(),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Consumer side of the `stream_buffer` endpoint.
///
/// A consumer is bound to exactly one `(topic, correlation_id)` partition. Use
/// this for reading the responses for one HTTP request, LLM generation, MCP
/// call, or similar one-request/many-response workflow.
#[derive(Debug)]
pub struct StreamBufferConsumer {
    topic: String,
    correlation_id: String,
    sender: Sender<Vec<CanonicalMessage>>,
    receiver: Receiver<Vec<CanonicalMessage>>,
    buffer: Vec<CanonicalMessage>,
}

impl StreamBufferConsumer {
    pub fn new(config: &StreamBufferConfig) -> anyhow::Result<Self> {
        let correlation_id = config.correlation_id.clone().ok_or_else(|| {
            anyhow!("stream_buffer consumer requires 'correlation_id' in its config")
        })?;
        let partition = get_or_create_partition(
            &config.topic,
            &correlation_id,
            config.capacity.unwrap_or(DEFAULT_CAPACITY),
        );
        Ok(Self {
            topic: config.topic.clone(),
            correlation_id,
            sender: partition.sender,
            receiver: partition.receiver,
            buffer: Vec::new(),
        })
    }

    async fn get_buffered_messages(
        &mut self,
        max_messages: usize,
    ) -> Result<Vec<CanonicalMessage>, ConsumerError> {
        let max_messages = max_messages.max(1);
        if self.buffer.is_empty() {
            self.buffer = match self.receiver.recv().await {
                Ok(batch) => batch,
                Err(_) => return Err(ConsumerError::EndOfStream),
            };
            self.buffer.reverse();
        }

        let num_to_take = self.buffer.len().min(max_messages);
        let split_at = self.buffer.len() - num_to_take;
        let mut messages = self.buffer.split_off(split_at);
        messages.reverse();
        Ok(messages)
    }
}

impl Drop for StreamBufferConsumer {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            requeue_messages(self.sender.clone(), std::mem::take(&mut self.buffer));
        }
    }
}

struct RequeueGuard {
    sender: Sender<Vec<CanonicalMessage>>,
    messages: Vec<CanonicalMessage>,
}

impl Drop for RequeueGuard {
    fn drop(&mut self) {
        if !self.messages.is_empty() {
            requeue_messages(self.sender.clone(), std::mem::take(&mut self.messages));
        }
    }
}

fn requeue_messages(sender: Sender<Vec<CanonicalMessage>>, messages: Vec<CanonicalMessage>) {
    if messages.is_empty() {
        return;
    }
    match sender.try_send(messages) {
        Ok(_) => {}
        Err(error) => {
            let messages = match error {
                async_channel::TrySendError::Full(messages) => messages,
                async_channel::TrySendError::Closed(messages) => messages,
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    if let Err(error) = sender.send(messages).await {
                        tracing::error!("Failed to requeue stream_buffer messages: {}", error);
                    }
                });
            } else {
                tracing::error!(
                    "No active runtime found, could not requeue stream_buffer messages"
                );
            }
        }
    }
}

#[async_trait]
impl MessageConsumer for StreamBufferConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let mut messages = self.get_buffered_messages(max_messages).await?;
        while messages.len() < max_messages.max(1) / 2 {
            if let Ok(mut next_batch) = self.receiver.try_recv() {
                let max_messages = max_messages.max(1);
                if next_batch.len() + messages.len() > max_messages {
                    let needed = max_messages - messages.len();
                    let mut to_buffer = next_batch.split_off(needed);
                    messages.append(&mut next_batch);
                    self.buffer.append(&mut to_buffer);
                    self.buffer.reverse();
                    break;
                }
                messages.append(&mut next_batch);
            } else {
                break;
            }
        }

        trace!(
            topic = %self.topic,
            correlation_id = %self.correlation_id,
            count = messages.len(),
            "Received stream_buffer messages"
        );

        let topic = self.topic.clone();
        let correlation_id = self.correlation_id.clone();
        let sender = self.sender.clone();
        let expected_count = messages.len();
        let mut guard = RequeueGuard {
            sender: sender.clone(),
            messages: messages.clone(),
        };

        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                if dispositions.len() != expected_count {
                    return Err(anyhow!(
                        "stream_buffer commit received mismatched disposition count: expected {}, got {}",
                        expected_count,
                        dispositions.len()
                    ));
                }

                let mut to_requeue = Vec::new();
                let mut saw_acked_end_marker = false;
                for (index, disposition) in dispositions.into_iter().enumerate() {
                    match disposition {
                        MessageDisposition::Ack | MessageDisposition::Reply(_) => {
                            if guard
                                .messages
                                .get(index)
                                .and_then(|message| message.metadata.get("http_stream_end"))
                                .is_some_and(|value| value == "true")
                            {
                                saw_acked_end_marker = true;
                            }
                        }
                        MessageDisposition::Nack => {
                            if let Some(message) = guard.messages.get(index) {
                                warn!(
                                    topic = %topic,
                                    correlation_id = %correlation_id,
                                    index,
                                    "Requeueing nacked stream_buffer message"
                                );
                                to_requeue.push(message.clone());
                            }
                        }
                    }
                }

                std::mem::take(&mut guard.messages);
                if !to_requeue.is_empty() {
                    requeue_messages(sender.clone(), to_requeue);
                } else if saw_acked_end_marker {
                    remove_partition_if_current(&topic, &correlation_id, &sender);
                }

                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        }) as BatchCommitFunc;

        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        EndpointStatus {
            healthy: !self.receiver.is_closed(),
            target: format!("{}/{}", self.topic, self.correlation_id),
            pending: Some(self.receiver.len()),
            capacity: Some(self.receiver.capacity().unwrap_or(0)),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::MessageConsumer;

    fn config(topic: &str, correlation_id: Option<&str>) -> StreamBufferConfig {
        StreamBufferConfig {
            topic: topic.to_string(),
            correlation_id: correlation_id.map(str::to_string),
            capacity: Some(10),
        }
    }

    #[tokio::test]
    async fn test_stream_buffer_isolates_by_correlation_id() {
        let topic = format!("stream_buffer_iso_{}", fast_uuid_v7::gen_id_str());
        let publisher = StreamBufferPublisher::new(&config(&topic, None)).unwrap();

        publisher
            .send(CanonicalMessage::from_vec("a1").with_metadata_kv("correlation_id", "a"))
            .await
            .unwrap();
        publisher
            .send(CanonicalMessage::from_vec("b1").with_metadata_kv("correlation_id", "b"))
            .await
            .unwrap();
        publisher
            .send(CanonicalMessage::from_vec("a2").with_metadata_kv("correlation_id", "a"))
            .await
            .unwrap();

        let mut consumer_a = StreamBufferConsumer::new(&config(&topic, Some("a"))).unwrap();
        let mut consumer_b = StreamBufferConsumer::new(&config(&topic, Some("b"))).unwrap();

        let first_a = consumer_a.receive().await.unwrap();
        assert_eq!(first_a.message.get_payload_str(), "a1");
        (first_a.commit)(MessageDisposition::Ack).await.unwrap();

        let first_b = consumer_b.receive().await.unwrap();
        assert_eq!(first_b.message.get_payload_str(), "b1");
        (first_b.commit)(MessageDisposition::Ack).await.unwrap();

        let second_a = consumer_a.receive().await.unwrap();
        assert_eq!(second_a.message.get_payload_str(), "a2");
        (second_a.commit)(MessageDisposition::Ack).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_buffer_requeues_nacked_message() {
        let topic = format!("stream_buffer_nack_{}", fast_uuid_v7::gen_id_str());
        let publisher = StreamBufferPublisher::new(&config(&topic, None)).unwrap();
        publisher
            .send(CanonicalMessage::from_vec("retry").with_metadata_kv("correlation_id", "c"))
            .await
            .unwrap();

        let mut consumer = StreamBufferConsumer::new(&config(&topic, Some("c"))).unwrap();
        let received = consumer.receive().await.unwrap();
        assert_eq!(received.message.get_payload_str(), "retry");
        (received.commit)(MessageDisposition::Nack).await.unwrap();

        let retried = consumer.receive().await.unwrap();
        assert_eq!(retried.message.get_payload_str(), "retry");
        (retried.commit)(MessageDisposition::Ack).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_buffer_requeues_dropped_message() {
        let topic = format!("stream_buffer_drop_{}", fast_uuid_v7::gen_id_str());
        let publisher = StreamBufferPublisher::new(&config(&topic, None)).unwrap();
        publisher
            .send(CanonicalMessage::from_vec("held").with_metadata_kv("correlation_id", "c"))
            .await
            .unwrap();

        let mut consumer = StreamBufferConsumer::new(&config(&topic, Some("c"))).unwrap();
        {
            let received = consumer.receive().await.unwrap();
            assert_eq!(received.message.get_payload_str(), "held");
        }

        let redelivered = consumer.receive().await.unwrap();
        assert_eq!(redelivered.message.get_payload_str(), "held");
        (redelivered.commit)(MessageDisposition::Ack).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_buffer_end_marker_ack_cleans_partition() {
        let topic = format!("stream_buffer_end_{}", fast_uuid_v7::gen_id_str());
        let publisher = StreamBufferPublisher::new(&config(&topic, None)).unwrap();
        publisher
            .send(
                CanonicalMessage::from_vec("")
                    .with_metadata_kv("correlation_id", "done")
                    .with_metadata_kv("http_stream_end", "true"),
            )
            .await
            .unwrap();

        let mut consumer = StreamBufferConsumer::new(&config(&topic, Some("done"))).unwrap();
        let end = consumer.receive().await.unwrap();
        assert!(end.message.payload.is_empty());
        (end.commit)(MessageDisposition::Ack).await.unwrap();

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), consumer.receive()).await;
        assert!(matches!(result, Ok(Err(ConsumerError::EndOfStream))));
    }
}
