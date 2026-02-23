//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::SledConfig;
use crate::traits::{
    ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher, PublisherError, Received,
    ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use sled::{Db, IVec, Tree};
use std::any::Any;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Mutex;
use tracing::trace;

pub struct SledPublisher {
    db: Db,
    tree: Tree,
}

static SLED_DBS: Lazy<Mutex<HashMap<String, Db>>> = Lazy::new(|| Mutex::new(HashMap::new()));

fn get_or_open_db(path: &str) -> anyhow::Result<Db> {
    let mut dbs = SLED_DBS
        .lock()
        .map_err(|_| anyhow!("Sled DB registry lock poisoned"))?;
    if let Some(db) = dbs.get(path) {
        return Ok(db.clone());
    }
    let db = sled::open(path)?;
    dbs.insert(path.to_string(), db.clone());
    Ok(db)
}

impl SledPublisher {
    pub fn new(config: &SledConfig) -> anyhow::Result<Self> {
        let db = get_or_open_db(&config.path).context("Failed to open Sled DB")?;
        let tree_name = config.tree.as_deref().unwrap_or("default");
        let tree = db
            .open_tree(tree_name)
            .context("Failed to open Sled tree")?;
        Ok(Self { db, tree })
    }
}

#[async_trait]
impl MessagePublisher for SledPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let id = self
            .db
            .generate_id()
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        let key = id.to_be_bytes();
        let value =
            serde_json::to_vec(&message).map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;

        self.tree
            .insert(key, value)
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;

        Ok(Sent::Ack)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        trace!(count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Publishing batch to Sled");
        let mut batch = sled::Batch::default();
        for message in messages {
            let id = self
                .db
                .generate_id()
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
            let key = id.to_be_bytes();
            let value = serde_json::to_vec(&message)
                .map_err(|e| PublisherError::NonRetryable(anyhow!(e)))?;
            batch.insert(&key, value);
        }
        self.tree
            .apply_batch(batch)
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        self.tree
            .flush_async()
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;

        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CanonicalMessage;
    use tempfile::tempdir;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sled_queue_mode() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let config = SledConfig {
            path: path.clone(),
            tree: None,
            read_from_start: true,
            delete_after_read: true,
        };

        let publisher = SledPublisher::new(&config).unwrap();
        let mut consumer = SledConsumer::new(&config).unwrap();

        let msg = CanonicalMessage::new(b"queue_item".to_vec(), None);
        publisher.send(msg.clone()).await.unwrap();

        let received = timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for message")
            .unwrap();

        assert_eq!(received.message.payload, msg.payload);

        // Commit (Ack)
        (received.commit)(MessageDisposition::Ack).await.unwrap();

        // Verify DB is empty
        let db = get_or_open_db(&path).unwrap();
        let tree = db.open_tree("default").unwrap();
        assert!(tree.is_empty());
    }

    #[tokio::test]
    async fn test_sled_topic_mode() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let config = SledConfig {
            path: path.clone(),
            tree: Some("topic".to_string()),
            read_from_start: true,
            delete_after_read: false,
        };

        let publisher = SledPublisher::new(&config).unwrap();
        let mut consumer = SledConsumer::new(&config).unwrap();

        let msg1 = CanonicalMessage::new(b"msg1".to_vec(), None);
        publisher.send(msg1.clone()).await.unwrap();

        let received1 = timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for msg1")
            .unwrap();
        assert_eq!(received1.message.payload, msg1.payload);

        let msg2 = CanonicalMessage::new(b"msg2".to_vec(), None);
        publisher.send(msg2.clone()).await.unwrap();

        let received2 = timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for msg2")
            .unwrap();
        assert_eq!(received2.message.payload, msg2.payload);
    }

    #[tokio::test]
    async fn test_sled_nack_requeue() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let config = SledConfig {
            path: path.clone(),
            tree: None,
            read_from_start: true,
            delete_after_read: true,
        };

        let publisher = SledPublisher::new(&config).unwrap();
        let mut consumer = SledConsumer::new(&config).unwrap();

        let msg = CanonicalMessage::new(b"retry_me".to_vec(), None);
        publisher.send(msg.clone()).await.unwrap();

        // Receive and Nack
        let received = consumer.receive().await.unwrap();
        (received.commit)(MessageDisposition::Nack).await.unwrap();

        // Should be available again
        let received_retry = timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("Timed out waiting for retry")
            .unwrap();

        assert_eq!(received_retry.message.payload, msg.payload);
    }
}

pub struct SledConsumer {
    tree: Tree,
    notify_rx: async_channel::Receiver<()>,
    delete_after_read: bool,
    last_key: Option<IVec>,
}

impl SledConsumer {
    pub fn new(config: &SledConfig) -> anyhow::Result<Self> {
        let db = get_or_open_db(&config.path).context("Failed to open Sled DB")?;
        let tree_name = config.tree.as_deref().unwrap_or("default");
        let tree = db
            .open_tree(tree_name)
            .context("Failed to open Sled tree")?;

        let subscriber = tree.watch_prefix(vec![]);
        let (tx, rx) = async_channel::bounded(1);

        std::thread::spawn(move || {
            for _event in subscriber {
                if tx.send_blocking(()).is_err() {
                    break;
                }
            }
        });

        let last_key = if !config.read_from_start {
            tree.last().map_err(|e| anyhow!(e))?.map(|(k, _)| k)
        } else {
            None
        };

        Ok(Self {
            tree,
            notify_rx: rx,
            delete_after_read: config.delete_after_read,
            last_key,
        })
    }
}

#[async_trait]
impl MessageConsumer for SledConsumer {
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        loop {
            let next_item = if self.delete_after_read {
                // Queue mode: Atomic pop_min
                self.tree
                    .pop_min()
                    .map_err(|e| ConsumerError::Connection(anyhow!(e)))?
            } else {
                // Topic mode: Scan forward
                let start = if let Some(k) = &self.last_key {
                    Bound::Excluded(k)
                } else {
                    Bound::Unbounded
                };
                self.tree
                    .range::<&IVec, _>((start, Bound::Unbounded))
                    .next()
                    .transpose()
                    .map_err(|e| ConsumerError::Connection(anyhow!(e)))?
            };

            if let Some((key, value)) = next_item {
                self.last_key = Some(key.clone());
                let message = serde_json::from_slice(&value)
                    .map_err(|e| ConsumerError::Connection(anyhow!(e)))?;

                let tree = self.tree.clone();
                let delete = self.delete_after_read;
                let key_clone = key.clone();
                let value_clone = value.to_vec();

                let commit = Box::new(move |disposition: MessageDisposition| {
                    Box::pin(async move {
                        if delete && matches!(disposition, MessageDisposition::Nack) {
                            // Re-insert on Nack in Queue mode
                            tree.insert(key_clone, value_clone)
                                .map_err(|e| anyhow!(e))?;
                        }
                        Ok(())
                    }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
                });

                return Ok(Received { message, commit });
            }

            // If no message, wait for notification
            if self.notify_rx.recv().await.is_err() {
                return Err(ConsumerError::EndOfStream);
            }
        }
    }

    async fn receive_batch(
        &mut self,
        _max_messages: usize,
    ) -> Result<ReceivedBatch, ConsumerError> {
        let received = self.receive().await?;
        Ok(ReceivedBatch {
            messages: vec![received.message],
            commit: crate::traits::into_batch_commit_func(received.commit),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
