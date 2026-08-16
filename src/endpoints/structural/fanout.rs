use crate::traits::{EndpointStatus, MessagePublisher, PublisherError, Sent, SentBatch};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

pub struct FanoutPublisher {
    publishers: Vec<Arc<dyn MessagePublisher>>,
}

impl FanoutPublisher {
    pub fn new(publishers: Vec<Arc<dyn MessagePublisher>>) -> Self {
        Self { publishers }
    }
}

#[async_trait]
impl MessagePublisher for FanoutPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        for publisher in &self.publishers {
            // We must clone the message for each publisher.
            publisher.send(message.clone()).await?;
        }
        Ok(Sent::Ack)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        use futures::future::join_all;

        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        // Send the batch to all publishers concurrently.
        let batch_sends = self.publishers.iter().map(|p| {
            // Each publisher gets a clone of the entire batch. This can be memory-intensive.
            p.send_batch(messages.clone())
        });

        let results = join_all(batch_sends).await;

        // A hard error from any publisher propagates. Otherwise per-destination failures merge:
        // a message that failed at any destination is nacked for the whole fan-out (duplicate
        // failures across destinations are harmless — the route keys by message_id). Responses
        // aren't forwarded: with several destinations there's no single response to attribute.
        let mut failed = Vec::new();
        for result in results {
            match result? {
                SentBatch::Ack => {}
                SentBatch::Partial {
                    responses: _,
                    failed: child_failed,
                } => failed.extend(child_failed),
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
        use futures::future::join_all;

        let status_futs = self.publishers.iter().map(|p| p.status());
        let results = join_all(status_futs).await;

        let mut healthy = true;
        let mut pending = 0;
        let mut capacity = 0;
        let mut error: Option<String> = None;
        let mut details = Vec::new();

        for status in results {
            if !status.healthy {
                healthy = false;
                if error.is_none() {
                    error = status.error.clone();
                }
            }
            pending += status.pending.unwrap_or(0);
            capacity += status.capacity.unwrap_or(0);
            details.push(status);
        }

        EndpointStatus {
            healthy,
            pending: Some(pending),
            capacity: Some(capacity),
            error,
            details: serde_json::json!({ "destinations": details }),
            ..Default::default()
        }
    }

    /// Ordered if any leg is: a fanout is only as order-tolerant as its strictest sink.
    fn requires_ordered_publish(&self) -> bool {
        self.publishers.iter().any(|p| p.requires_ordered_publish())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::ProcessingError;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingPublisher {
        single_payloads: Mutex<Vec<String>>,
        batch_payloads: Mutex<Vec<Vec<String>>>,
        status: EndpointStatus,
        batch_error: Option<String>,
        /// Payloads this publisher reports as failed instead of erroring the whole batch.
        batch_partial_failures: Vec<String>,
    }

    #[async_trait]
    impl MessagePublisher for RecordingPublisher {
        async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
            self.single_payloads
                .lock()
                .unwrap()
                .push(message.get_payload_str().to_string());
            Ok(Sent::Ack)
        }

        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            self.batch_payloads.lock().unwrap().push(
                messages
                    .iter()
                    .map(|message| message.get_payload_str().to_string())
                    .collect(),
            );

            if let Some(message) = &self.batch_error {
                return Err(ProcessingError::NonRetryable(anyhow::anyhow!(
                    message.clone()
                )));
            }

            if !self.batch_partial_failures.is_empty() {
                let failed: Vec<_> = messages
                    .into_iter()
                    .filter(|m| {
                        self.batch_partial_failures
                            .contains(&m.get_payload_str().to_string())
                    })
                    .map(|m| {
                        (
                            m,
                            ProcessingError::Retryable(anyhow::anyhow!("destination rejected")),
                        )
                    })
                    .collect();
                if !failed.is_empty() {
                    return Ok(SentBatch::Partial {
                        responses: None,
                        failed,
                    });
                }
            }

            Ok(SentBatch::Ack)
        }

        async fn status(&self) -> EndpointStatus {
            self.status.clone()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_fanout_send_delivers_message_to_all_publishers() {
        let left = Arc::new(RecordingPublisher::default());
        let right = Arc::new(RecordingPublisher::default());
        let fanout = FanoutPublisher::new(vec![left.clone(), right.clone()]);

        let result = fanout.send(CanonicalMessage::from("hello")).await.unwrap();
        assert!(matches!(result, Sent::Ack));
        assert_eq!(left.single_payloads.lock().unwrap().as_slice(), ["hello"]);
        assert_eq!(right.single_payloads.lock().unwrap().as_slice(), ["hello"]);
    }

    #[tokio::test]
    async fn test_fanout_send_batch_propagates_errors() {
        let ok = Arc::new(RecordingPublisher::default());
        let failing = Arc::new(RecordingPublisher {
            batch_error: Some("fanout failure".to_string()),
            ..Default::default()
        });
        let fanout = FanoutPublisher::new(vec![ok.clone(), failing.clone()]);

        let err = fanout
            .send_batch(vec![
                CanonicalMessage::from("one"),
                CanonicalMessage::from("two"),
            ])
            .await
            .unwrap_err();
        assert!(matches!(err, ProcessingError::NonRetryable(_)));
        assert_eq!(ok.batch_payloads.lock().unwrap().len(), 1);
        assert_eq!(failing.batch_payloads.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_fanout_send_batch_preserves_child_partial_failures() {
        let ok = Arc::new(RecordingPublisher::default());
        let partial = Arc::new(RecordingPublisher {
            batch_partial_failures: vec!["two".to_string()],
            ..Default::default()
        });
        let fanout = FanoutPublisher::new(vec![ok.clone(), partial.clone()]);

        let sent = fanout
            .send_batch(vec![
                CanonicalMessage::from("one"),
                CanonicalMessage::from("two"),
            ])
            .await
            .unwrap();

        match sent {
            SentBatch::Partial { responses, failed } => {
                assert!(responses.is_none());
                assert_eq!(failed.len(), 1);
                assert_eq!(failed[0].0.get_payload_str(), "two");
            }
            SentBatch::Ack => panic!("expected partial failure to be preserved"),
        }
    }

    #[tokio::test]
    async fn test_fanout_status_aggregates_destination_status() {
        let healthy = Arc::new(RecordingPublisher {
            status: EndpointStatus {
                healthy: true,
                target: "a".to_string(),
                pending: Some(2),
                capacity: Some(5),
                error: None,
                details: serde_json::json!({"id": "a"}),
            },
            ..Default::default()
        });
        let unhealthy = Arc::new(RecordingPublisher {
            status: EndpointStatus {
                healthy: false,
                target: "b".to_string(),
                pending: Some(3),
                capacity: Some(7),
                error: Some("down".to_string()),
                details: serde_json::json!({"id": "b"}),
            },
            ..Default::default()
        });
        let fanout = FanoutPublisher::new(vec![healthy, unhealthy]);

        let status = fanout.status().await;
        assert!(!status.healthy);
        assert_eq!(status.pending, Some(5));
        assert_eq!(status.capacity, Some(12));
        assert_eq!(status.error.as_deref(), Some("down"));
        assert_eq!(status.details["destinations"].as_array().unwrap().len(), 2);
    }
}
