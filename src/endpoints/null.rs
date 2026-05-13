use crate::traits::{MessagePublisher, PublisherError, SentBatch};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;

#[derive(Clone)]
pub struct NullPublisher;

#[async_trait]
impl MessagePublisher for NullPublisher {
    async fn send_batch(
        &self,
        _messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Sent;

    #[tokio::test]
    async fn test_null_publisher_acks_single_and_batch_messages() {
        let publisher = NullPublisher;

        assert!(matches!(
            publisher
                .send(CanonicalMessage::from("ignored"))
                .await
                .unwrap(),
            Sent::Ack
        ));
        assert!(matches!(
            publisher
                .send_batch(vec![
                    CanonicalMessage::from("one"),
                    CanonicalMessage::from("two")
                ])
                .await
                .unwrap(),
            SentBatch::Ack
        ));
        assert!(publisher.as_any().is::<NullPublisher>());
    }
}
