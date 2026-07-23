//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
//
//! Two-way payload encryption middleware: the publisher side seals each
//! message payload into an AEAD envelope, the consumer side opens it.
//! Metadata and routing keys stay in the clear. Request-reply response
//! payloads pass through untouched.

use crate::models::EncryptionConfig;
use crate::support::crypto::Crypto;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessagePublisher, PublisherError, Received,
    ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

pub struct EncryptionPublisher {
    inner: Box<dyn MessagePublisher>,
    crypto: Arc<Crypto>,
}

impl EncryptionPublisher {
    pub fn new(
        inner: Box<dyn MessagePublisher>,
        config: &EncryptionConfig,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            crypto: Arc::new(Crypto::new(config)?),
        })
    }

    fn seal_message(&self, message: &mut CanonicalMessage) -> Result<(), PublisherError> {
        let sealed = self
            .crypto
            .seal(&message.payload, b"")
            .map_err(PublisherError::NonRetryable)?;
        message.payload = sealed.into();
        Ok(())
    }
}

#[async_trait]
impl MessagePublisher for EncryptionPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send(&self, mut message: CanonicalMessage) -> Result<Sent, PublisherError> {
        self.seal_message(&mut message)?;
        self.inner.send(message).await
    }

    async fn send_batch(
        &self,
        mut messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        // Keep the plaintext payloads: messages surfaced as failed must go back
        // upstream (retry/dlq) in their original form, or an outer retry would
        // double-seal them.
        let originals: std::collections::HashMap<u128, bytes::Bytes> = messages
            .iter()
            .map(|m| (m.message_id, m.payload.clone()))
            .collect();
        for message in &mut messages {
            self.seal_message(message)?;
        }
        match self.inner.send_batch(messages).await? {
            SentBatch::Ack => Ok(SentBatch::Ack),
            SentBatch::Partial {
                responses,
                mut failed,
            } => {
                for (msg, _) in &mut failed {
                    if let Some(original) = originals.get(&msg.message_id) {
                        msg.payload = original.clone();
                    }
                }
                Ok(SentBatch::Partial { responses, failed })
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct EncryptionConsumer {
    inner: Box<dyn MessageConsumer>,
    crypto: Arc<Crypto>,
}

impl EncryptionConsumer {
    pub fn new(inner: Box<dyn MessageConsumer>, config: &EncryptionConfig) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            crypto: Arc::new(Crypto::new(config)?),
        })
    }

    fn open_message(&self, message: &mut CanonicalMessage) -> Result<(), ConsumerError> {
        // A decrypt/authentication failure is permanent: the ciphertext will never
        // open, so it must be surfaced as non-retryable rather than triggering an
        // endless reconnect-and-re-read of the same poison message.
        let opened = self
            .crypto
            .open(&message.payload, b"")
            .map_err(ConsumerError::Permanent)?;
        message.payload = opened.into();
        Ok(())
    }
}

#[async_trait]
impl MessageConsumer for EncryptionConsumer {
    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }

    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        let mut received = self.inner.receive().await?;
        self.open_message(&mut received.message)?;
        Ok(received)
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let mut batch = self.inner.receive_batch(max_messages).await?;
        for message in &mut batch.messages {
            self.open_message(message)?;
        }
        Ok(batch)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use std::sync::Mutex;

    fn config() -> EncryptionConfig {
        EncryptionConfig {
            key: base64::engine::general_purpose::STANDARD.encode([5u8; 32]),
            ..Default::default()
        }
    }

    #[derive(Clone)]
    struct RecordingPublisher {
        sent: Arc<Mutex<Vec<CanonicalMessage>>>,
    }

    #[async_trait]
    impl MessagePublisher for RecordingPublisher {
        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            self.sent.lock().unwrap().extend(messages);
            Ok(SentBatch::Ack)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct MockConsumer {
        messages: Option<Vec<CanonicalMessage>>,
    }

    #[async_trait]
    impl MessageConsumer for MockConsumer {
        async fn receive_batch(
            &mut self,
            _max_messages: usize,
        ) -> Result<ReceivedBatch, ConsumerError> {
            Ok(ReceivedBatch {
                messages: self.messages.take().expect("batch already consumed"),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            })
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn publisher_encrypts_and_consumer_decrypts() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let publisher = EncryptionPublisher::new(
            Box::new(RecordingPublisher { sent: sent.clone() }),
            &config(),
        )
        .unwrap();

        let mut msg = CanonicalMessage::from("top secret");
        msg.metadata.insert("kind".to_string(), "note".to_string());
        publisher.send_batch(vec![msg]).await.unwrap();

        // The wire payload is ciphertext; metadata stays clear.
        let wire = sent.lock().unwrap().clone();
        assert_ne!(wire[0].payload.as_ref(), b"top secret");
        assert_eq!(
            wire[0].metadata.get("kind").map(|s| s.as_str()),
            Some("note")
        );

        let mut consumer = EncryptionConsumer::new(
            Box::new(MockConsumer {
                messages: Some(wire),
            }),
            &config(),
        )
        .unwrap();
        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages[0].payload.as_ref(), b"top secret");
        assert_eq!(
            batch.messages[0].metadata.get("kind").map(|s| s.as_str()),
            Some("note")
        );
    }

    #[tokio::test]
    async fn tampered_payload_fails_consume() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let publisher = EncryptionPublisher::new(
            Box::new(RecordingPublisher { sent: sent.clone() }),
            &config(),
        )
        .unwrap();
        publisher
            .send_batch(vec![CanonicalMessage::from("payload")])
            .await
            .unwrap();

        let mut wire = sent.lock().unwrap().clone();
        let mut tampered = wire[0].payload.to_vec();
        *tampered.last_mut().unwrap() ^= 1;
        wire[0].payload = tampered.into();

        let mut consumer = EncryptionConsumer::new(
            Box::new(MockConsumer {
                messages: Some(wire),
            }),
            &config(),
        )
        .unwrap();
        // A tampered payload is a permanent failure, not a reconnectable one, so
        // the poison message is not re-read indefinitely.
        assert!(matches!(
            consumer.receive_batch(10).await,
            Err(ConsumerError::Permanent(_))
        ));
    }
}
