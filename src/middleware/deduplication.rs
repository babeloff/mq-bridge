//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
use crate::models::DeduplicationMiddleware;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, Received, ReceivedBatch,
};
use anyhow::Context;
use async_trait::async_trait;
use sled::Db;
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, instrument, trace, warn};

pub struct DeduplicationConsumer {
    inner: Box<dyn MessageConsumer>,
    db: Arc<Db>,
    ttl_seconds: u64,
    last_cleanup: Arc<AtomicU64>,
}

impl DeduplicationConsumer {
    pub fn new(
        inner: Box<dyn MessageConsumer>,
        config: &DeduplicationMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        info!(
            "Deduplication Middleware enabled for route '{}' with TTL {}s",
            route_name, config.ttl_seconds
        );
        let db = sled::open(&config.sled_path)?;
        Ok(Self {
            inner,
            db: Arc::new(db),
            ttl_seconds: config.ttl_seconds,
            last_cleanup: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn check_and_reserve_id(
        &self,
        message_id: u128,
        now: u64,
    ) -> Result<bool, ConsumerError> {
        let key = message_id.to_be_bytes();
        let now_bytes = now.to_be_bytes();

        const STATE_PENDING: u8 = 0;
        const PENDING_TTL: u64 = 5;

        let mut pending_val = [0u8; 9];
        pending_val[0] = STATE_PENDING;
        pending_val[1..9].copy_from_slice(&now_bytes);

        let mut yield_counter = 0;
        let mut total_attempts = 0;
        const MAX_TOTAL_ATTEMPTS: usize = 1000;

        loop {
            if total_attempts >= MAX_TOTAL_ATTEMPTS {
                return Err(ConsumerError::Connection(anyhow::anyhow!(
                    "Deduplication CAS exceeded max attempts for message ID {:032x}",
                    message_id
                )));
            }
            if yield_counter > 10 {
                tokio::task::yield_now().await;
                yield_counter = 0;
            }
            yield_counter += 1;
            total_attempts += 1;

            match self
                .db
                .compare_and_swap(key, None::<&[u8]>, Some(&pending_val[..]))
            {
                Ok(Ok(())) => return Ok(false),
                Ok(Err(cas_error)) => {
                    if let Some(current_bytes) = cas_error.current.as_deref() {
                        let (ts, ttl) = if current_bytes.len() == 9 {
                            let state = current_bytes[0];
                            let ts_bytes: [u8; 8] = current_bytes[1..9].try_into().unwrap();
                            (
                                u64::from_be_bytes(ts_bytes),
                                if state == STATE_PENDING {
                                    PENDING_TTL
                                } else {
                                    self.ttl_seconds
                                },
                            )
                        } else if current_bytes.len() == 8 {
                            let ts_bytes: [u8; 8] = current_bytes.try_into().unwrap();
                            (u64::from_be_bytes(ts_bytes), self.ttl_seconds)
                        } else {
                            (0, 0)
                        };

                        if now.saturating_sub(ts) < ttl {
                            return Ok(true);
                        }
                        match self.db.compare_and_swap(
                            key,
                            Some(current_bytes),
                            Some(&pending_val[..]),
                        ) {
                            Ok(Ok(())) => return Ok(false),
                            Ok(Err(_)) => continue,
                            Err(e) => {
                                return Err(ConsumerError::Connection(anyhow::anyhow!(
                                    "Deduplication DB error: {}",
                                    e
                                )))
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Err(e) => {
                    return Err(ConsumerError::Connection(anyhow::anyhow!(
                        "Deduplication DB error: {}",
                        e
                    )))
                }
            }
        }
    }

    fn trigger_cleanup_if_needed(&self, now: u64) {
        let last = self.last_cleanup.load(Ordering::Acquire);
        if now.saturating_sub(last) > 30
            && self
                .last_cleanup
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::Acquire)
                .is_ok()
        {
            let db = self.db.clone();
            let ttl = self.ttl_seconds;
            tokio::spawn(async move {
                let cutoff = now.saturating_sub(ttl);
                for (key, val) in db.iter().flatten() {
                    let len = val.len();
                    let ts_offset = if len == 9 {
                        1
                    } else if len == 8 {
                        0
                    } else {
                        continue;
                    };
                    if let Ok(ts_bytes) = val[ts_offset..ts_offset + 8].try_into() {
                        if u64::from_be_bytes(ts_bytes) < cutoff {
                            let _ = db.compare_and_swap(&key, Some(val), None::<&[u8]>);
                        }
                    }
                }
            });
        }
    }
}

#[async_trait]
impl MessageConsumer for DeduplicationConsumer {
    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        let inner_hook = self.inner.on_disconnect_hook();
        let db = self.db.clone();

        Some(Box::pin(async move {
            let mut first_error = None;
            if let Some(hook) = inner_hook {
                if let Err(err) = hook.await {
                    first_error = Some(err);
                }
            }
            if let Err(err) = db.flush_async().await {
                first_error.get_or_insert_with(|| anyhow::anyhow!(err));
            }
            match first_error {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }))
    }

    #[instrument(skip_all)]
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        loop {
            let received = self.inner.receive().await?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("System time is before UNIX EPOCH")?
                .as_secs();

            self.trigger_cleanup_if_needed(now);

            if self
                .check_and_reserve_id(received.message.message_id, now)
                .await?
            {
                info!(message_id = %format!("{:032x}", received.message.message_id), "Duplicate message detected and skipped");
                if let Err(e) = (received.commit)(MessageDisposition::Ack).await {
                    warn!("Failed to commit skipped duplicate message: {}", e);
                }
                continue;
            }

            let db = self.db.clone();
            let message_id = received.message.message_id;
            let original_commit = received.commit;

            // Wrap commit to update DB to "processed" state
            let commit = Box::new(move |disposition: MessageDisposition| {
                Box::pin(async move {
                    let is_ack = matches!(
                        disposition,
                        MessageDisposition::Ack | MessageDisposition::Reply(_)
                    );
                    original_commit(disposition).await?;

                    if is_ack {
                        let now_bytes = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                            .to_be_bytes();
                        let mut processed_val = [0u8; 9];
                        processed_val[0] = 1; // STATE_PROCESSED
                        processed_val[1..9].copy_from_slice(&now_bytes);

                        // Update the pending marker to the final processed value
                        if let Err(e) = db.insert(message_id.to_be_bytes(), &processed_val[..]) {
                            error!(
                                "Failed to update message {:032x} as processed in deduplication DB: {}",
                                message_id, e
                            );
                        } else {
                            trace!("Updated message as processed in deduplication DB");
                        }
                    } else {
                        trace!("Updated message as processed in deduplication DB");
                    }
                    Ok(())
                }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
            });

            return Ok(Received {
                message: received.message,
                commit,
            });
        }
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        loop {
            let ReceivedBatch {
                messages,
                commit: inner_commit,
            } = self.inner.receive_batch(max_messages).await?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| ConsumerError::Connection(anyhow::anyhow!(e)))?
                .as_secs();

            self.trigger_cleanup_if_needed(now);

            let mut filtered_messages = Vec::with_capacity(messages.len());
            let mut kept_indices = Vec::with_capacity(messages.len());

            for (idx, msg) in messages.iter().enumerate() {
                if self.check_and_reserve_id(msg.message_id, now).await? {
                    info!(message_id = %format!("{:032x}", msg.message_id), "Duplicate message detected and skipped");
                } else {
                    filtered_messages.push(msg.clone());
                    kept_indices.push(idx);
                }
            }

            if filtered_messages.is_empty() {
                let _ = inner_commit(vec![MessageDisposition::Ack; messages.len()]).await;
                continue;
            }

            let db = self.db.clone();
            let kept_ids: Vec<u128> = filtered_messages.iter().map(|m| m.message_id).collect();
            let total_len = messages.len();

            let commit: crate::traits::BatchCommitFunc = Box::new(move |dispositions| {
                let db = db.clone();
                let inner_commit = inner_commit;
                let kept_indices = kept_indices;
                let kept_ids = kept_ids;

                Box::pin(async move {
                    let mut full_dispositions = vec![MessageDisposition::Ack; total_len];
                    let mut acks = Vec::new();
                    for (i, disp) in dispositions.into_iter().enumerate() {
                        if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                            acks.push(kept_ids[i]);
                        }
                        full_dispositions[kept_indices[i]] = disp;
                    }

                    inner_commit(full_dispositions).await?;

                    let now_bytes = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        .to_be_bytes();
                    let mut processed_val = [0u8; 9];
                    processed_val[0] = 1; // STATE_PROCESSED
                    processed_val[1..9].copy_from_slice(&now_bytes);

                    for id in acks {
                        let _ = db.insert(id.to_be_bytes(), &processed_val[..]);
                    }
                    Ok(())
                }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
            });

            return Ok(ReceivedBatch {
                messages: filtered_messages,
                commit,
            });
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::memory::MemoryConsumer;
    use crate::models::DeduplicationMiddleware;
    use crate::CanonicalMessage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_deduplication_logic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("dedup_test").to_str().unwrap().to_string();

        let config = DeduplicationMiddleware {
            sled_path: db_path,
            ttl_seconds: 60,
        };

        let mem_consumer = MemoryConsumer::new_local("dedup_topic", 10);
        let channel = mem_consumer.channel();

        // 1. Send a message
        let msg1 = CanonicalMessage::new(b"data1".to_vec(), Some(100));
        channel.send_message(msg1).await.unwrap();

        // 2. Send a duplicate message
        let msg2 = CanonicalMessage::new(b"data1_dup".to_vec(), Some(100));
        channel.send_message(msg2).await.unwrap();

        // 3. Send a new message
        let msg3 = CanonicalMessage::new(b"data2".to_vec(), Some(101));
        channel.send_message(msg3).await.unwrap();

        let mut dedup_consumer =
            DeduplicationConsumer::new(Box::new(mem_consumer), &config, "test_route").unwrap();

        // First receive: Should be msg1 (ID 100)
        let rec1 = dedup_consumer.receive().await.unwrap();
        assert_eq!(rec1.message.message_id, 100);
        let _ = (rec1.commit)(crate::traits::MessageDisposition::Ack).await;

        // Second receive: Should be msg3 (ID 101). msg2 (ID 100) is skipped internally.
        let rec2 = dedup_consumer.receive().await.unwrap();
        assert_eq!(rec2.message.message_id, 101);
        let _ = (rec2.commit)(crate::traits::MessageDisposition::Ack).await;
    }
}
