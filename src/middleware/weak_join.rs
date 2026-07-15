use crate::models::{WeakJoinMiddleware, WeakJoinTimeout};
use crate::traits::{BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, ReceivedBatch};
use crate::CanonicalMessage;
use async_trait::async_trait;
use serde_json::Value;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

struct JoinState {
    // Key -> (CreationTime, Messages)
    pending: HashMap<String, (Instant, Vec<CanonicalMessage>)>,
    ready_buffer: Vec<CanonicalMessage>,
}

pub struct WeakJoinConsumer {
    inner: Box<dyn MessageConsumer>,
    config: WeakJoinMiddleware,
    state: Arc<Mutex<JoinState>>,
}

impl WeakJoinConsumer {
    pub fn new(inner: Box<dyn MessageConsumer>, config: &WeakJoinMiddleware) -> Self {
        Self {
            inner,
            config: config.clone(),
            state: Arc::new(Mutex::new(JoinState {
                pending: HashMap::new(),
                ready_buffer: Vec::new(),
            })),
        }
    }

    /// Dispatches to branch-keyed or count-array joining depending on config.
    fn emit_join(&self, key: &str, messages: &[CanonicalMessage]) -> CanonicalMessage {
        if self.config.branch_by.is_some() {
            self.try_join_branches(key, messages)
        } else {
            self.try_join(key, messages)
        }
    }

    /// Count mode: merge payloads into a JSON array (arrival order).
    fn try_join(&self, key: &str, messages: &[CanonicalMessage]) -> CanonicalMessage {
        let payloads: Vec<Value> = messages.iter().map(payload_to_value).collect();
        let merged_payload = serde_json::to_vec(&payloads).unwrap_or_default();
        self.finalize(key, merged_payload, messages.first())
    }

    /// Branch mode: merge payloads into an object keyed by branch name.
    /// The first message seen for a branch wins; later duplicates are ignored (idempotent).
    fn try_join_branches(&self, key: &str, messages: &[CanonicalMessage]) -> CanonicalMessage {
        let branch_by = self.config.branch_by.as_deref().unwrap_or_default();
        let mut obj = serde_json::Map::new();
        let mut present: Vec<String> = Vec::new();
        for m in messages {
            // Skip messages without a branch label rather than collapsing them
            // under a shared "unknown" key.
            let branch = match m.metadata.get(branch_by) {
                Some(b) => b.clone(),
                None => continue,
            };
            if obj.contains_key(&branch) {
                continue;
            }
            present.push(branch.clone());
            obj.insert(branch, payload_to_value(m));
        }
        let complete = self.is_complete(messages);
        let merged_payload = serde_json::to_vec(&Value::Object(obj)).unwrap_or_default();
        let mut new_msg = self.finalize(key, merged_payload, messages.first());
        new_msg
            .metadata
            .insert("join_branches".to_string(), present.join(","));
        new_msg
            .metadata
            .insert("join_complete".to_string(), complete.to_string());
        new_msg
    }

    /// Builds the joined message: fresh id, metadata inherited from the first source, group key stamped.
    fn finalize(
        &self,
        key: &str,
        payload: Vec<u8>,
        first: Option<&CanonicalMessage>,
    ) -> CanonicalMessage {
        let mut new_msg =
            CanonicalMessage::new(payload, Some(fast_uuid_v7::gen_id_with_sub_ms_4()));
        if let Some(first) = first {
            new_msg.metadata = first.metadata.clone();
        }
        new_msg
            .metadata
            .insert(self.config.group_by.clone(), key.to_string());
        new_msg
    }

    /// Whether a pending group satisfies its fire condition.
    /// Count mode: `expected_count` messages. Branch mode: all `required` branches present,
    /// or (if `required` is empty) `expected_count` distinct branches.
    fn is_complete(&self, messages: &[CanonicalMessage]) -> bool {
        match self.config.branch_by.as_deref() {
            None => messages.len() >= self.config.expected_count,
            Some(branch_by) => {
                let distinct: HashSet<&str> = messages
                    .iter()
                    .filter_map(|m| m.metadata.get(branch_by).map(String::as_str))
                    .collect();
                if self.config.required.is_empty() {
                    distinct.len() >= self.config.expected_count
                } else {
                    self.config
                        .required
                        .iter()
                        .all(|b| distinct.contains(b.as_str()))
                }
            }
        }
    }

    fn check_timeouts(&self, state: &mut JoinState, ready_messages: &mut Vec<CanonicalMessage>) {
        let now = Instant::now();
        let timeout = Duration::from_millis(self.config.timeout_ms);
        let mut timed_out_keys = Vec::new();

        for (key, (start_time, _)) in state.pending.iter() {
            if now.duration_since(*start_time) >= timeout {
                timed_out_keys.push(key.clone());
            }
        }

        for key in timed_out_keys {
            if let Some((_, msgs)) = state.pending.remove(&key) {
                if self.config.on_timeout == WeakJoinTimeout::Discard {
                    continue; // drop the incomplete group without emitting
                }
                ready_messages.push(self.emit_join(&key, &msgs));
            }
        }
    }
}

/// Deserializes a message payload as JSON, falling back to a lossy UTF-8 string.
fn payload_to_value(m: &CanonicalMessage) -> Value {
    match serde_json::from_slice(&m.payload) {
        Ok(v) => v,
        Err(_) => Value::String(String::from_utf8_lossy(&m.payload).to_string()),
    }
}

#[async_trait]
impl MessageConsumer for WeakJoinConsumer {
    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let mut state = self.state.lock().await;

        if !state.ready_buffer.is_empty() {
            let count = std::cmp::min(state.ready_buffer.len(), max_messages);
            let messages: Vec<_> = state.ready_buffer.drain(0..count).collect();
            return Ok(ReceivedBatch {
                messages,
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        let now = Instant::now();
        let timeout_duration = Duration::from_millis(self.config.timeout_ms);
        let next_timeout = state
            .pending
            .values()
            .map(|(start, _)| *start + timeout_duration)
            .min()
            .unwrap_or(now + Duration::from_secs(3600));

        let sleep_duration = next_timeout.saturating_duration_since(now);
        drop(state);

        let batch_future = self.inner.receive_batch(max_messages);
        let timeout_future = tokio::time::sleep(sleep_duration);

        tokio::select! {
            res = batch_future => {
                match res {
                    Ok(batch) => {
                        // Weak join: Ack immediately to avoid complex disposition mapping
                        let count = batch.messages.len();
                        if count > 0 {
                            if let Err(e) = (batch.commit)(vec![MessageDisposition::Ack; count]).await {
                                return Err(ConsumerError::Connection(e));
                            }
                        }

                        let mut state = self.state.lock().await;
                        let mut ready_messages = Vec::new();
                        // Flush expired groups before admitting new messages, so a
                        // fresh message for an expired key starts a new group rather
                        // than joining a stale one.
                        self.check_timeouts(&mut state, &mut ready_messages);

                        let now = Instant::now();
                        for msg in batch.messages {
                            let key = msg
                                .metadata
                                .get(&self.config.group_by)
                                .cloned()
                                .unwrap_or_else(|| "default".to_string());
                            let entry = state
                                .pending
                                .entry(key.clone())
                                .or_insert_with(|| (now, Vec::new()));
                            entry.1.push(msg);

                            if self.is_complete(&entry.1) {
                                let (_, msgs) = state.pending.remove(&key).unwrap();
                                ready_messages.push(self.emit_join(&key, &msgs));
                            }
                        }

                        if ready_messages.len() > max_messages {
                            let overflow = ready_messages.split_off(max_messages);
                            state.ready_buffer.extend(overflow);
                        }

                        Ok(ReceivedBatch {
                            messages: ready_messages,
                            commit: Box::new(|_| Box::pin(async { Ok(()) })),
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            _ = timeout_future => {
                let mut state = self.state.lock().await;
                let mut ready_messages = Vec::new();
                self.check_timeouts(&mut state, &mut ready_messages);

                if ready_messages.len() > max_messages {
                    let overflow = ready_messages.split_off(max_messages);
                    state.ready_buffer.extend(overflow);
                }

                Ok(ReceivedBatch {
                    messages: ready_messages,
                    commit: Box::new(|_| Box::pin(async { Ok(()) })),
                })
            }
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
    use crate::CanonicalMessage;
    use serde_json::json;

    #[tokio::test]
    async fn test_weak_join_grouping() {
        let config = WeakJoinMiddleware {
            group_by: "group_id".to_string(),
            expected_count: 2,
            timeout_ms: 1000,
            branch_by: None,
            required: Vec::new(),
            on_timeout: WeakJoinTimeout::Fire,
        };

        let mem_consumer = MemoryConsumer::new_local("join_test", 10);
        let channel = mem_consumer.channel();

        // Send 2 messages with group_id "A"
        let msg1 = CanonicalMessage::from_json(json!({"val": 1}))
            .unwrap()
            .with_metadata_kv("group_id", "A");
        let msg2 = CanonicalMessage::from_json(json!({"val": 2}))
            .unwrap()
            .with_metadata_kv("group_id", "A");

        channel.send_message(msg1).await.unwrap();
        channel.send_message(msg2).await.unwrap();

        let mut join_consumer = WeakJoinConsumer::new(Box::new(mem_consumer), &config);

        let batch = join_consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 1);

        let joined = &batch.messages[0];
        let payload: Vec<Value> = serde_json::from_slice(&joined.payload).unwrap();
        assert_eq!(payload.len(), 2);
        assert_eq!(payload[0]["val"], 1);
        assert_eq!(payload[1]["val"], 2);
        assert_eq!(
            joined.metadata.get("group_id").map(|s| s.as_str()),
            Some("A")
        );
    }

    #[tokio::test]
    async fn test_weak_join_timeout() {
        let config = WeakJoinMiddleware {
            group_by: "group_id".to_string(),
            expected_count: 3,
            timeout_ms: 100,
            branch_by: None,
            required: Vec::new(),
            on_timeout: WeakJoinTimeout::Fire,
        };

        let mem_consumer = MemoryConsumer::new_local("join_timeout_test", 10);
        let channel = mem_consumer.channel();

        // Send 1 message with group_id "B" (less than expected 3)
        let msg1 = CanonicalMessage::from_json(json!({"val": 1}))
            .unwrap()
            .with_metadata_kv("group_id", "B");

        channel.send_message(msg1).await.unwrap();

        let mut join_consumer = WeakJoinConsumer::new(Box::new(mem_consumer), &config);

        // First call will pick up the message but return empty batch because count < expected
        let batch1 = join_consumer.receive_batch(10).await.unwrap();
        assert!(batch1.messages.is_empty());

        // Wait for timeout to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Second call should trigger timeout logic and return the partial batch
        let batch2 = join_consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch2.messages.len(), 1);

        let joined = &batch2.messages[0];
        let payload: Vec<Value> = serde_json::from_slice(&joined.payload).unwrap();
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0]["val"], 1);
    }

    #[tokio::test]
    async fn test_weak_join_branches_all_required() {
        let config = WeakJoinMiddleware {
            group_by: "correlation_id".to_string(),
            expected_count: 0,
            timeout_ms: 1000,
            branch_by: Some("branch".to_string()),
            required: vec!["postgres".to_string(), "features".to_string()],
            on_timeout: WeakJoinTimeout::Fire,
        };

        let mem_consumer = MemoryConsumer::new_local("join_branch_test", 10);
        let channel = mem_consumer.channel();

        let pg = CanonicalMessage::from_json(json!({"id": 42}))
            .unwrap()
            .with_metadata_kv("correlation_id", "order-1")
            .with_metadata_kv("branch", "postgres");
        let feat = CanonicalMessage::from_json(json!({"score": 0.9}))
            .unwrap()
            .with_metadata_kv("correlation_id", "order-1")
            .with_metadata_kv("branch", "features");

        channel.send_message(pg).await.unwrap();
        channel.send_message(feat).await.unwrap();

        let mut join_consumer = WeakJoinConsumer::new(Box::new(mem_consumer), &config);

        let batch = join_consumer.receive_batch(10).await.unwrap();
        assert_eq!(batch.messages.len(), 1);

        let joined = &batch.messages[0];
        let payload: Value = serde_json::from_slice(&joined.payload).unwrap();
        // Branch-keyed object, not a positional array.
        assert_eq!(payload["postgres"]["id"], 42);
        assert_eq!(payload["features"]["score"], 0.9);
        assert_eq!(
            joined.metadata.get("join_complete").map(|s| s.as_str()),
            Some("true")
        );
    }

    #[tokio::test]
    async fn test_weak_join_branch_incomplete_does_not_fire() {
        let config = WeakJoinMiddleware {
            group_by: "correlation_id".to_string(),
            expected_count: 0,
            timeout_ms: 1000,
            branch_by: Some("branch".to_string()),
            required: vec!["postgres".to_string(), "features".to_string()],
            on_timeout: WeakJoinTimeout::Fire,
        };

        let mem_consumer = MemoryConsumer::new_local("join_branch_incomplete", 10);
        let channel = mem_consumer.channel();

        // Two messages from the SAME branch must NOT satisfy a two-branch join.
        let pg1 = CanonicalMessage::from_json(json!({"id": 1}))
            .unwrap()
            .with_metadata_kv("correlation_id", "order-2")
            .with_metadata_kv("branch", "postgres");
        let pg2 = CanonicalMessage::from_json(json!({"id": 2}))
            .unwrap()
            .with_metadata_kv("correlation_id", "order-2")
            .with_metadata_kv("branch", "postgres");

        channel.send_message(pg1).await.unwrap();
        channel.send_message(pg2).await.unwrap();

        let mut join_consumer = WeakJoinConsumer::new(Box::new(mem_consumer), &config);

        let batch = join_consumer.receive_batch(10).await.unwrap();
        assert!(batch.messages.is_empty());
    }

    #[tokio::test]
    async fn test_weak_join_branch_timeout_discard() {
        let config = WeakJoinMiddleware {
            group_by: "correlation_id".to_string(),
            expected_count: 0,
            timeout_ms: 100,
            branch_by: Some("branch".to_string()),
            required: vec!["postgres".to_string(), "features".to_string()],
            on_timeout: WeakJoinTimeout::Discard,
        };

        let mem_consumer = MemoryConsumer::new_local("join_branch_discard", 10);
        let channel = mem_consumer.channel();

        let pg = CanonicalMessage::from_json(json!({"id": 7}))
            .unwrap()
            .with_metadata_kv("correlation_id", "order-3")
            .with_metadata_kv("branch", "postgres");
        channel.send_message(pg).await.unwrap();

        let mut join_consumer = WeakJoinConsumer::new(Box::new(mem_consumer), &config);

        let batch1 = join_consumer.receive_batch(10).await.unwrap();
        assert!(batch1.messages.is_empty());

        tokio::time::sleep(Duration::from_millis(150)).await;

        // Incomplete group is dropped on timeout, not emitted as a partial.
        let batch2 = join_consumer.receive_batch(10).await.unwrap();
        assert!(batch2.messages.is_empty());
    }
}
