use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, Sent, SentBatch,
};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ReaderPublisher {
    consumer: Arc<Mutex<Box<dyn MessageConsumer>>>,
}

impl ReaderPublisher {
    pub fn new(consumer: Box<dyn MessageConsumer>) -> Self {
        Self {
            consumer: Arc::new(Mutex::new(consumer)),
        }
    }
}

#[async_trait]
impl MessagePublisher for ReaderPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            let consumer = self.consumer.lock().await;
            if let Some(hook) = consumer.on_connect_hook() {
                hook.await?;
            }
            Ok(())
        }))
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            let consumer = self.consumer.lock().await;
            if let Some(hook) = consumer.on_disconnect_hook() {
                hook.await?;
            }
            Ok(())
        }))
    }

    async fn send(&self, _message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let mut consumer = self.consumer.lock().await;
        // We ignore the incoming message payload and just read from the consumer.
        // The incoming message acts purely as a trigger.
        match consumer.receive().await {
            Ok(received) => {
                // We must commit the message immediately because the Publisher interface
                // doesn't support passing the commit responsibility back to the caller
                // in a way that aligns with the input's commit lifecycle.
                if let Err(e) = (received.commit)(MessageDisposition::Ack).await {
                    return Err(PublisherError::Retryable(anyhow::anyhow!(
                        "Failed to commit message in ReaderPublisher: {}",
                        e
                    )));
                }
                Ok(Sent::Response(received.message))
            }
            Err(e) => match e {
                ConsumerError::EndOfStream => Err(PublisherError::NonRetryable(anyhow::anyhow!(e))),
                _ => Err(PublisherError::Retryable(anyhow::anyhow!(e))),
            },
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let count = messages.len();
        if count == 0 {
            return Ok(SentBatch::Ack);
        }

        let mut consumer = self.consumer.lock().await;
        match consumer.receive_batch(count).await {
            Ok(batch) => {
                let received_count = batch.messages.len();
                if received_count > 0 {
                    if let Err(e) =
                        (batch.commit)(vec![MessageDisposition::Ack; received_count]).await
                    {
                        return Err(PublisherError::Retryable(anyhow::anyhow!(
                            "Failed to commit batch in ReaderPublisher: {}",
                            e
                        )));
                    }
                }

                Ok(SentBatch::Ack)
            }
            Err(e) => match e {
                ConsumerError::EndOfStream => Err(PublisherError::NonRetryable(anyhow::anyhow!(e))),
                _ => Err(PublisherError::Retryable(anyhow::anyhow!(e))),
            },
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
