use crate::traits::{MessagePublisher, PublisherError, Sent, SentBatch};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

pub struct SwitchPublisher {
    metadata_key: String,
    cases: HashMap<String, Arc<dyn MessagePublisher>>,
    default: Option<Arc<dyn MessagePublisher>>,
}

impl SwitchPublisher {
    pub fn new(
        metadata_key: String,
        cases: HashMap<String, Arc<dyn MessagePublisher>>,
        default: Option<Arc<dyn MessagePublisher>>,
    ) -> Self {
        Self {
            metadata_key,
            cases,
            default,
        }
    }

    fn get_publisher(&self, message: &CanonicalMessage) -> Option<&Arc<dyn MessagePublisher>> {
        if let Some(val) = message.metadata.get(&self.metadata_key) {
            if let Some(publisher) = self.cases.get(val) {
                return Some(publisher);
            }
        }
        self.default.as_ref()
    }
}

#[async_trait]
impl MessagePublisher for SwitchPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        if let Some(publisher) = self.get_publisher(&message) {
            publisher.send(message).await
        } else {
            warn!(
                "Switch publisher dropped message with id {:032x}: metadata key '{}' not found or no matching case/default.",
                message.message_id, self.metadata_key
            );
            Ok(Sent::Ack)
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        use futures::future::join_all;
        use std::collections::HashMap;

        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        // Group by the resolved publisher's identity, so every message headed for the same
        // destination lands in one `send_batch` call — including distinct metadata values
        // that both fall through to `default`.
        let mut grouped_messages: HashMap<
            usize,
            (Arc<dyn MessagePublisher>, Vec<CanonicalMessage>),
        > = HashMap::new();

        for message in messages {
            if let Some(publisher) = self.get_publisher(&message) {
                let key = Arc::as_ptr(publisher) as *const () as usize;
                grouped_messages
                    .entry(key)
                    .or_insert_with(|| (publisher.clone(), Vec::new()))
                    .1
                    .push(message);
            } else {
                warn!(
                    "Switch publisher dropped message with id {:032x}: metadata key '{}' not found or no matching case/default.",
                    message.message_id, self.metadata_key
                );
            }
        }

        // Create futures for sending each group as a batch.
        let batch_sends = grouped_messages
            .into_values()
            .map(|(publisher, batch)| async move { publisher.send_batch(batch).await });

        let results = join_all(batch_sends).await;

        // Aggregate results from all the batch sends.
        let mut all_responses = Vec::new();
        let mut all_failed = Vec::new();

        for result in results {
            match result {
                Ok(SentBatch::Ack) => {}
                Ok(SentBatch::Partial { responses, failed }) => {
                    if let Some(resps) = responses {
                        all_responses.extend(resps);
                    }
                    all_failed.extend(failed);
                }
                Err(e) => {
                    // If a whole sub-batch fails, we can't easily recover the messages that were part of it.
                    // Propagating the error is the safest and simplest option. The caller (e.g., retry middleware)
                    // will have to re-process the original, larger batch.
                    return Err(e);
                }
            }
        }

        if all_failed.is_empty() && all_responses.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: if all_responses.is_empty() {
                    None
                } else {
                    Some(all_responses)
                },
                failed: all_failed,
            })
        }
    }

    /// Ordered if any branch is: batches for a given branch must stay in source order.
    fn requires_ordered_publish(&self) -> bool {
        self.cases
            .values()
            .chain(self.default.iter())
            .any(|p| p.requires_ordered_publish())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::memory::MemoryPublisher;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_switch_publisher_routing() {
        // Unique topic names: memory topics resolve through a process-global registry,
        // so shared names would collide with parallel tests.
        let pub_a = MemoryPublisher::new_local("switch_routing_topic_a", 10);
        let pub_b = MemoryPublisher::new_local("switch_routing_topic_b", 10);
        let pub_default = MemoryPublisher::new_local("switch_routing_topic_default", 10);

        let chan_a = pub_a.channel();
        let chan_b = pub_b.channel();
        let chan_default = pub_default.channel();

        let mut cases = HashMap::new();
        cases.insert(
            "A".to_string(),
            Arc::new(pub_a) as Arc<dyn MessagePublisher>,
        );
        cases.insert(
            "B".to_string(),
            Arc::new(pub_b) as Arc<dyn MessagePublisher>,
        );

        let switch = SwitchPublisher::new(
            "route_key".to_string(),
            cases,
            Some(Arc::new(pub_default) as Arc<dyn MessagePublisher>),
        );

        // Test Case A
        let msg_a = CanonicalMessage::from("payload_a").with_metadata_kv("route_key", "A");
        switch.send(msg_a).await.unwrap();
        assert_eq!(chan_a.len(), 1);
        assert_eq!(chan_b.len(), 0);
        assert_eq!(chan_default.len(), 0);
        chan_a.drain_messages();

        // Test Case B
        let msg_b = CanonicalMessage::from("payload_b").with_metadata_kv("route_key", "B");
        switch.send(msg_b).await.unwrap();
        assert_eq!(chan_a.len(), 0);
        assert_eq!(chan_b.len(), 1);
        assert_eq!(chan_default.len(), 0);
        chan_b.drain_messages();

        // Test Default (Unknown Key)
        let msg_c =
            CanonicalMessage::new(b"payload_c".to_vec(), None).with_metadata_kv("route_key", "C");
        switch.send(msg_c).await.unwrap();
        assert_eq!(chan_a.len(), 0);
        assert_eq!(chan_b.len(), 0);
        assert_eq!(chan_default.len(), 1);
        chan_default.drain_messages();

        // Test Default (Missing Key)
        let msg_d = CanonicalMessage::new(b"payload_d".to_vec(), None);
        switch.send(msg_d).await.unwrap();
        assert_eq!(chan_a.len(), 0);
        assert_eq!(chan_b.len(), 0);
        assert_eq!(chan_default.len(), 1);
    }

    /// Records the size of every `send_batch` call it receives.
    #[derive(Default)]
    struct RecordingPublisher {
        batches: std::sync::Mutex<Vec<usize>>,
    }

    #[async_trait]
    impl MessagePublisher for RecordingPublisher {
        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            self.batches.lock().unwrap().push(messages.len());
            Ok(SentBatch::Ack)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_switch_batch_merges_default_routed_groups() {
        let pub_default = Arc::new(RecordingPublisher::default());
        // Memory topics resolve through a process-global channel registry, so this needs a
        // name no other test uses or the parallel suite sees the other test's messages.
        let pub_a = MemoryPublisher::new_local("switch_merge_topic_a", 10);
        let chan_a = pub_a.channel();

        let mut cases = HashMap::new();
        cases.insert(
            "A".to_string(),
            Arc::new(pub_a) as Arc<dyn MessagePublisher>,
        );

        let switch = SwitchPublisher::new(
            "route_key".to_string(),
            cases,
            Some(pub_default.clone() as Arc<dyn MessagePublisher>),
        );

        // An unmatched value, an explicitly empty value and an absent key all route to
        // `default`, and must arrive there as a single batch.
        let messages = vec![
            CanonicalMessage::from("a").with_metadata_kv("route_key", "A"),
            CanonicalMessage::from("unmatched").with_metadata_kv("route_key", "C"),
            CanonicalMessage::from("empty").with_metadata_kv("route_key", ""),
            CanonicalMessage::from("absent"),
        ];
        switch.send_batch(messages).await.unwrap();

        assert_eq!(chan_a.len(), 1);
        assert_eq!(*pub_default.batches.lock().unwrap(), vec![3]);
    }
}
