//! Proof-of-concept alternative ZeroMQ backend built on omq.rs (`omq-tokio`),
//! behind the opt-in `zeromq-omq` feature. It reuses the shared framing/format
//! codec ([`super::codec`]) and the same [`ZeroMqConfig`] surface as the default
//! zmq.rs backend, but only covers **PUSH/PULL + PUB/SUB** — REQ/REP and
//! request-reply are out of scope. Selected from config via `backend: omq` on a
//! `zeromq` endpoint (see [`ZeroMqBackend`](crate::models::ZeroMqBackend)).
//!
//! omq's `Socket` is a cheaply-cloneable actor handle whose `send`/`recv` take
//! `&self` and apply HWM backpressure internally, so unlike the zmq.rs backend
//! this needs no spawn-a-task + channel plumbing.

use super::codec;
use crate::models::{ZeroMqConfig, ZeroMqFormat, ZeroMqSocketType};
use crate::traits::{
    ConsumerError, EndpointStatus, MessageConsumer, MessagePublisher, PublisherError,
    ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::anyhow;
use async_trait::async_trait;
use omq_tokio::{Endpoint, Message, Options, Socket, SocketType};
use std::any::Any;
use std::collections::VecDeque;
use tracing::trace;

fn parse_endpoint(url: &str) -> anyhow::Result<Endpoint> {
    url.parse::<Endpoint>()
        .map_err(|e| anyhow!("invalid ZeroMQ endpoint {url:?}: {e}"))
}

/// Collect every frame of a received omq `Message` into a plain `Vec<Bytes>` for
/// the shared codec.
fn message_frames(msg: &Message) -> Vec<bytes::Bytes> {
    (0..msg.len()).filter_map(|i| msg.part_bytes(i)).collect()
}

/// Build an omq `Message` from codec frames (always at least one).
fn frames_to_message(frames: Vec<bytes::Bytes>) -> Message {
    Message::multipart(frames)
}

// ----------------------------------------------------------------------------
// Publisher (PUSH / PUB)
// ----------------------------------------------------------------------------

pub struct ZeroMqOmqPublisher {
    socket: Socket,
    format: ZeroMqFormat,
}

impl ZeroMqOmqPublisher {
    pub async fn new(config: &ZeroMqConfig) -> anyhow::Result<Self> {
        let socket_type = config.socket_type.clone().unwrap_or(ZeroMqSocketType::Push);
        let omq_type = match socket_type {
            ZeroMqSocketType::Push => SocketType::Push,
            ZeroMqSocketType::Pub => SocketType::Pub,
            other => {
                return Err(anyhow!(
                    "socket type {other:?} is not supported by the omq PoC backend \
                     (publisher supports Push/Pub only)"
                ))
            }
        };

        let socket = Socket::new(omq_type, Options::default());
        let endpoint = parse_endpoint(&config.url)?;
        if config.bind {
            socket.bind(endpoint).await?;
        } else {
            socket.connect(endpoint).await?;
        }

        Ok(Self {
            socket,
            format: config.format.clone(),
        })
    }

    /// Send one already-framed message, mapping omq errors to a retryable failure.
    async fn send_message(&self, msg: Message) -> Result<(), PublisherError> {
        self.socket
            .send(msg)
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))
    }
}

#[async_trait]
impl MessagePublisher for ZeroMqOmqPublisher {
    async fn send_batch(
        &self,
        mut messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        trace!(
            count = messages.len(),
            "Publishing batch via omq ZeroMQ backend"
        );

        if matches!(self.format, ZeroMqFormat::Json) {
            // Whole batch is serialized into one JSON frame, mirroring zmq.rs.
            for message in &mut messages {
                message.strip_source_metadata();
            }
            let payload = serde_json::to_vec(&messages)
                .map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;
            self.send_message(Message::single(bytes::Bytes::from(payload)))
                .await?;
            return Ok(SentBatch::Ack);
        }

        // Raw / RawFramed: one ZMQ message per canonical message so a single
        // failure only re-sends the offending message.
        let mut failed = Vec::new();
        for mut message in messages {
            let frames = match codec::encode_frames(&mut message, &self.format) {
                Ok(f) => f,
                Err(e) => {
                    failed.push((message, e));
                    continue;
                }
            };
            if let Err(e) = self.send_message(frames_to_message(frames)).await {
                failed.push((message, e));
            }
        }

        if failed.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed,
            })
        }
    }

    async fn status(&self) -> EndpointStatus {
        EndpointStatus {
            healthy: true,
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ----------------------------------------------------------------------------
// Consumer (PULL / SUB)
// ----------------------------------------------------------------------------

pub struct ZeroMqOmqConsumer {
    socket: Socket,
    buffer: VecDeque<CanonicalMessage>,
    // Only SUB sockets prepend a subscription-topic frame; for PULL a leading
    // frame is payload, not a topic.
    is_sub: bool,
    format: ZeroMqFormat,
    /// Drain mode: only then does an idle recv return empty (see `drain_gated`).
    exit_on_empty: bool,
}

impl ZeroMqOmqConsumer {
    pub async fn new(config: &ZeroMqConfig) -> anyhow::Result<Self> {
        let socket_type = config.socket_type.clone().unwrap_or(ZeroMqSocketType::Pull);
        let is_sub = matches!(socket_type, ZeroMqSocketType::Sub);
        let omq_type = match socket_type {
            ZeroMqSocketType::Pull => SocketType::Pull,
            ZeroMqSocketType::Sub => SocketType::Sub,
            other => {
                return Err(anyhow!(
                    "socket type {other:?} is not supported by the omq PoC backend \
                     (consumer supports Pull/Sub only)"
                ))
            }
        };

        let socket = Socket::new(omq_type, Options::default());
        // Subscribe before connecting so early PUB traffic isn't missed.
        if is_sub {
            let topic = config.topic.as_deref().unwrap_or("");
            socket
                .subscribe(bytes::Bytes::from(topic.to_owned()))
                .await?;
        }
        let endpoint = parse_endpoint(&config.url)?;
        if config.bind {
            socket.bind(endpoint).await?;
        } else {
            socket.connect(endpoint).await?;
        }

        Ok(Self {
            socket,
            buffer: VecDeque::new(),
            is_sub,
            format: config.format.clone(),
            exit_on_empty: false,
        })
    }

    async fn fill_buffer(&mut self) -> Result<(), ConsumerError> {
        // Drain mode: a brief idle timeout returns empty-handed so --drain can fire.
        let Some(res) = crate::traits::drain_gated(self.exit_on_empty, self.socket.recv()).await
        else {
            return Ok(());
        };
        let msg = res.map_err(|e| ConsumerError::Connection(anyhow!(e)))?;
        let msgs = codec::decode_frames(message_frames(&msg), self.is_sub, &self.format)
            .map_err(|e| ConsumerError::Connection(anyhow!(e)))?;
        self.buffer.extend(msgs);
        Ok(())
    }
}

#[async_trait]
impl MessageConsumer for ZeroMqOmqConsumer {
    // ZeroMQ has no broker-side ack, so commits are order-independent.
    fn commit_requires_order(&self) -> bool {
        false
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }

        if self.buffer.is_empty() {
            self.fill_buffer().await?;
        }

        let mut messages = Vec::with_capacity(max_messages.min(self.buffer.len()));
        while messages.len() < max_messages {
            match self.buffer.pop_front() {
                Some(msg) => messages.push(msg),
                None => break,
            }
        }

        trace!(
            count = messages.len(),
            "Received batch via omq ZeroMQ backend"
        );
        Ok(ReceivedBatch {
            messages,
            commit: Box::new(|_| Box::pin(async { Ok(()) })),
        })
    }

    async fn status(&self) -> EndpointStatus {
        EndpointStatus {
            healthy: true,
            pending: Some(self.buffer.len()),
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
    use crate::models::ZeroMqBackend;
    use crate::traits::MessageConsumer;
    use std::sync::atomic::{AtomicU16, Ordering};
    use tokio::time::Duration;

    #[test]
    fn config_selects_omq_backend_and_defaults_to_zmq() {
        // The YAML/JSON surface: `backend: omq` opts into this backend; absent
        // means the default zmq.rs backend.
        let omq: ZeroMqConfig =
            serde_json::from_str(r#"{"url":"tcp://127.0.0.1:5555","backend":"omq"}"#).unwrap();
        assert_eq!(omq.backend, ZeroMqBackend::Omq);

        let default: ZeroMqConfig =
            serde_json::from_str(r#"{"url":"tcp://127.0.0.1:5555"}"#).unwrap();
        assert_eq!(default.backend, ZeroMqBackend::Zmq);
    }

    // Give each test its own port so parallel runs don't collide.
    static NEXT_PORT: AtomicU16 = AtomicU16::new(5620);
    fn next_url() -> String {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        format!("tcp://127.0.0.1:{port}")
    }

    #[tokio::test]
    async fn omq_push_pull_round_trip() {
        let url = next_url();
        let consumer_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Pull),
            bind: true,
            ..Default::default()
        };
        let publisher_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Push),
            bind: false,
            ..Default::default()
        };

        let mut consumer = ZeroMqOmqConsumer::new(&consumer_config).await.unwrap();
        let publisher = ZeroMqOmqPublisher::new(&publisher_config).await.unwrap();

        let msg = CanonicalMessage::from("hello omq");
        publisher.send(msg).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for message")
            .unwrap();
        assert_eq!(received.message.get_payload_str(), "hello omq");
    }

    #[tokio::test]
    async fn omq_pub_sub_round_trip() {
        let url = next_url();
        let consumer_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Sub),
            bind: false,
            ..Default::default()
        };
        let publisher_config = ZeroMqConfig {
            url: url.clone(),
            socket_type: Some(ZeroMqSocketType::Pub),
            bind: true,
            ..Default::default()
        };

        // PUB binds first so the SUB has something to connect to.
        let publisher = ZeroMqOmqPublisher::new(&publisher_config).await.unwrap();
        let mut consumer = ZeroMqOmqConsumer::new(&consumer_config).await.unwrap();

        // Allow the subscription to propagate before publishing (PUB/SUB drops
        // messages sent before the subscription is registered).
        tokio::time::sleep(Duration::from_millis(200)).await;
        publisher
            .send(CanonicalMessage::from("hello sub"))
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for pub/sub message")
            .unwrap();
        assert_eq!(received.message.get_payload_str(), "hello sub");
    }
}
