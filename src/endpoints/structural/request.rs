use crate::traits::{BoxFuture, MessagePublisher, PublisherError, Sent, SentBatch};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;
use tracing::warn;

/// Structural publisher that sends each message to a request-capable endpoint and forwards
/// the response to another endpoint.
///
/// On success the response is forwarded verbatim (it already carries the original
/// `message_id`, and for HTTP the `http_status_code`). On request error/timeout the original
/// message is forwarded unchanged instead, so a downstream `switch` can route success vs.
/// failure on the transport-native status. This endpoint adds no metadata of its own.
///
/// Retry, if configured, wraps the whole `send`: a `forward_to` failure after a successful
/// request re-issues the request on the next attempt. The `to` endpoint must therefore be
/// idempotent — delivery is at-least-once.
pub struct RequestForwardPublisher {
    request: Arc<dyn MessagePublisher>,
    forward: Arc<dyn MessagePublisher>,
}

impl RequestForwardPublisher {
    pub fn new(request: Arc<dyn MessagePublisher>, forward: Arc<dyn MessagePublisher>) -> Self {
        Self { request, forward }
    }
}

#[async_trait]
impl MessagePublisher for RequestForwardPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            if let Some(hook) = self.request.on_connect_hook() {
                hook.await?;
            }
            if let Some(hook) = self.forward.on_connect_hook() {
                hook.await?;
            }
            Ok(())
        }))
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            if let Some(hook) = self.request.on_disconnect_hook() {
                hook.await?;
            }
            if let Some(hook) = self.forward.on_disconnect_hook() {
                hook.await?;
            }
            Ok(())
        }))
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        // Keep the original for the error fallback (preserves its message_id).
        let original = message.clone();
        match self.request.send(message).await {
            Ok(Sent::Response(response)) => {
                self.forward.send(response).await?;
                Ok(Sent::Ack)
            }
            Ok(Sent::Ack) => {
                // The inner endpoint produced no response (e.g. request_reply disabled), so
                // there is nothing to forward.
                warn!(
                    message_id = %format!("{:032x}", original.message_id),
                    "request endpoint: inner returned Ack with no response, nothing forwarded"
                );
                Ok(Sent::Ack)
            }
            Err(e) => {
                // Forward the original unchanged; downstream (a switch on the transport
                // status) decides how to handle it. Ack so the input commits.
                warn!(
                    message_id = %format!("{:032x}", original.message_id),
                    error = %e,
                    "request endpoint: inner send failed, forwarding original message"
                );
                self.forward.send(original).await?;
                Ok(Sent::Ack)
            }
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        crate::traits::send_batch_helper(self, messages, |p, m| Box::pin(p.send(m))).await
    }

    async fn status(&self) -> crate::traits::EndpointStatus {
        // Health follows the request-capable `to` endpoint (where the actual I/O happens).
        self.request.status().await
    }

    fn requires_ordered_publish(&self) -> bool {
        self.request.requires_ordered_publish() || self.forward.requires_ordered_publish()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::EndpointStatus;
    use std::sync::Mutex as StdMutex;

    /// A request-capable publisher whose `send` returns a preconfigured result once.
    struct MockRequest {
        result: StdMutex<Option<Result<Sent, PublisherError>>>,
    }

    impl MockRequest {
        fn new(result: Result<Sent, PublisherError>) -> Arc<Self> {
            Arc::new(Self {
                result: StdMutex::new(Some(result)),
            })
        }
    }

    #[async_trait]
    impl MessagePublisher for MockRequest {
        async fn send(&self, _message: CanonicalMessage) -> Result<Sent, PublisherError> {
            self.result
                .lock()
                .unwrap()
                .take()
                .expect("MockRequest send called more than configured")
        }

        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            crate::traits::send_batch_helper(self, messages, |p, m| Box::pin(p.send(m))).await
        }

        async fn status(&self) -> EndpointStatus {
            EndpointStatus::default()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// A sink that records every message it receives, and can be told to fail.
    struct RecordingSink {
        received: StdArc,
        fail: bool,
    }

    type StdArc = std::sync::Arc<StdMutex<Vec<CanonicalMessage>>>;

    impl RecordingSink {
        fn new() -> (Arc<Self>, StdArc) {
            let received: StdArc = std::sync::Arc::new(StdMutex::new(Vec::new()));
            (
                Arc::new(Self {
                    received: received.clone(),
                    fail: false,
                }),
                received,
            )
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self {
                received: std::sync::Arc::new(StdMutex::new(Vec::new())),
                fail: true,
            })
        }
    }

    #[async_trait]
    impl MessagePublisher for RecordingSink {
        async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
            if self.fail {
                return Err(PublisherError::Retryable(anyhow::anyhow!("sink failed")));
            }
            self.received.lock().unwrap().push(message);
            Ok(Sent::Ack)
        }

        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            crate::traits::send_batch_helper(self, messages, |p, m| Box::pin(p.send(m))).await
        }

        async fn status(&self) -> EndpointStatus {
            EndpointStatus::default()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn forwards_response_verbatim() {
        let mut response = CanonicalMessage::from("http-body");
        response
            .metadata
            .insert("http_status_code".into(), "200".into());
        let response_id = response.message_id;

        let request = MockRequest::new(Ok(Sent::Response(response)));
        let (sink, received) = RecordingSink::new();
        let publisher = RequestForwardPublisher::new(request, sink);

        let sent = publisher.send(CanonicalMessage::from("in")).await.unwrap();
        assert!(matches!(sent, Sent::Ack));

        let received = received.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].get_payload_str(), "http-body");
        assert_eq!(
            received[0]
                .metadata
                .get("http_status_code")
                .map(String::as_str),
            Some("200")
        );
        assert_eq!(received[0].message_id, response_id);
    }

    #[tokio::test]
    async fn forwards_original_unchanged_on_error() {
        let request = MockRequest::new(Err(PublisherError::Retryable(anyhow::anyhow!("boom"))));
        let (sink, received) = RecordingSink::new();
        let publisher = RequestForwardPublisher::new(request, sink);

        let original = CanonicalMessage::from("original-payload");
        let original_id = original.message_id;

        let sent = publisher.send(original).await.unwrap();
        assert!(matches!(sent, Sent::Ack));

        let received = received.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].get_payload_str(), "original-payload");
        assert_eq!(received[0].message_id, original_id);
        // No status tag was invented on the fallback.
        assert!(!received[0].metadata.contains_key("http_status_code"));
    }

    #[tokio::test]
    async fn inner_ack_forwards_nothing() {
        let request = MockRequest::new(Ok(Sent::Ack));
        let (sink, received) = RecordingSink::new();
        let publisher = RequestForwardPublisher::new(request, sink);

        let sent = publisher.send(CanonicalMessage::from("in")).await.unwrap();
        assert!(matches!(sent, Sent::Ack));
        assert_eq!(received.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn forward_failure_propagates() {
        let mut response = CanonicalMessage::from("body");
        response
            .metadata
            .insert("http_status_code".into(), "200".into());
        let request = MockRequest::new(Ok(Sent::Response(response)));
        let publisher = RequestForwardPublisher::new(request, RecordingSink::failing());

        let err = publisher
            .send(CanonicalMessage::from("in"))
            .await
            .unwrap_err();
        assert!(matches!(err, PublisherError::Retryable(_)));
    }
}
