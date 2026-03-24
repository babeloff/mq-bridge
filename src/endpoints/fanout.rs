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

        // For fan-out, we consider the batch successful if it was successfully sent to *all* publishers.
        // If any publisher returns a hard error, we propagate it.
        // We don't currently aggregate partial failures from different fan-out destinations.
        for result in results {
            result?;
        }

        Ok(SentBatch::Ack)
    }

    async fn status(&self) -> EndpointStatus {
        use futures::future::join_all;

        let status_futs = self.publishers.iter().map(|p| p.status());
        let results = join_all(status_futs).await;

        let mut healthy = true;
        let mut pending = 0;
        let mut capacity = 0;
        let mut last_error: Option<String> = None;
        let mut details = Vec::new();

        for status in results {
            if !status.healthy {
                healthy = false;
                if last_error.is_none() {
                    last_error = status.last_error.clone();
                }
            }
            pending += status.pending.unwrap_or(0);
            capacity += status.capacity.unwrap_or(0);
            details.push(status);
        }

        EndpointStatus {
            healthy,
            pending: if pending > 0 { Some(pending) } else { None },
            capacity: if capacity > 0 { Some(capacity) } else { None },
            last_error,
            details: serde_json::json!({ "destinations": details }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
