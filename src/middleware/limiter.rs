use crate::models::LimiterMiddleware;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessagePublisher, PublisherError, Received,
    ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Mutex;
use tokio::time::{Duration, Instant};

#[derive(Debug)]
struct RateState {
    next_allowed_at: Instant,
}

impl RateState {
    fn new() -> Self {
        Self {
            next_allowed_at: Instant::now(),
        }
    }

    fn reserve(&mut self, count: usize, per_message: Duration) -> Duration {
        const MAX_DELAY: Duration = Duration::from_secs(3600);

        if count == 0 {
            return Duration::ZERO;
        }

        let now = Instant::now();
        let start_at = self.next_allowed_at.max(now);
        let additional = per_message.mul_f64(count as f64).min(MAX_DELAY);
        self.next_allowed_at = start_at
            .checked_add(additional)
            .unwrap_or_else(|| start_at + MAX_DELAY);
        start_at.saturating_duration_since(now)
    }
}

pub struct LimiterConsumer {
    inner: Box<dyn MessageConsumer>,
    per_message: Duration,
    state: RateState,
}

impl LimiterConsumer {
    pub fn new(
        inner: Box<dyn MessageConsumer>,
        config: &LimiterMiddleware,
    ) -> anyhow::Result<Self> {
        if !(config.messages_per_second.is_finite() && config.messages_per_second > 0.0) {
            return Err(anyhow::anyhow!(
                "Limiter messages_per_second must be a finite value greater than zero"
            ));
        }
        Ok(Self {
            inner,
            per_message: Duration::from_secs_f64(1.0 / config.messages_per_second),
            state: RateState::new(),
        })
    }

    async fn wait_for(&mut self, count: usize) {
        let delay = self.state.reserve(count, self.per_message);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

#[async_trait]
impl MessageConsumer for LimiterConsumer {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        let received = self.inner.receive().await?;
        self.wait_for(1).await;
        Ok(received)
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let batch = self.inner.receive_batch(max_messages).await?;
        self.wait_for(batch.messages.len()).await;
        Ok(batch)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct LimiterPublisher {
    inner: Box<dyn MessagePublisher>,
    per_message: Duration,
    state: Mutex<RateState>,
}

impl LimiterPublisher {
    pub fn new(
        inner: Box<dyn MessagePublisher>,
        config: &LimiterMiddleware,
    ) -> anyhow::Result<Self> {
        if !(config.messages_per_second.is_finite() && config.messages_per_second > 0.0) {
            return Err(anyhow::anyhow!(
                "Limiter messages_per_second must be a finite value greater than zero"
            ));
        }
        Ok(Self {
            inner,
            per_message: Duration::from_secs_f64(1.0 / config.messages_per_second),
            state: Mutex::new(RateState::new()),
        })
    }

    async fn wait_for(&self, count: usize) {
        let delay = self
            .state
            .lock()
            .expect("Limiter mutex poisoned")
            .reserve(count, self.per_message);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

#[async_trait]
impl MessagePublisher for LimiterPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        self.wait_for(1).await;
        self.inner.send(message).await
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        self.wait_for(messages.len()).await;
        self.inner.send_batch(messages).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::MessagePublisher;
    use crate::CanonicalMessage;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex as StdMutex};

    struct MockConsumer {
        batches: VecDeque<Vec<CanonicalMessage>>,
    }

    #[async_trait]
    impl MessageConsumer for MockConsumer {
        async fn receive_batch(
            &mut self,
            _max_messages: usize,
        ) -> Result<ReceivedBatch, ConsumerError> {
            Ok(ReceivedBatch {
                messages: self.batches.pop_front().expect("batch already consumed"),
                commit: ack_commit(),
            })
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Clone)]
    struct MockPublisher {
        sent: Arc<StdMutex<Vec<CanonicalMessage>>>,
    }

    #[async_trait]
    impl MessagePublisher for MockPublisher {
        async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
            self.sent.lock().unwrap().push(message);
            Ok(Sent::Ack)
        }

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

    fn ack_commit() -> crate::traits::BatchCommitFunc {
        Box::new(|_| Box::pin(async { Ok(()) }))
    }

    #[tokio::test]
    async fn test_limiter_consumer_delays_batch_by_message_count() {
        let config = LimiterMiddleware {
            messages_per_second: 20.0,
        };
        let mut consumer = LimiterConsumer::new(
            Box::new(MockConsumer {
                batches: VecDeque::from([
                    vec![CanonicalMessage::from("one"), CanonicalMessage::from("two")],
                    vec![
                        CanonicalMessage::from("three"),
                        CanonicalMessage::from("four"),
                    ],
                ]),
            }),
            &config,
        )
        .unwrap();

        let first = consumer.receive_batch(10).await.unwrap();
        let start = Instant::now();
        let second = consumer.receive_batch(10).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(first.messages.len(), 2);
        assert_eq!(second.messages.len(), 2);
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_limiter_publisher_delays_consecutive_sends() {
        let config = LimiterMiddleware {
            messages_per_second: 20.0,
        };
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let publisher =
            LimiterPublisher::new(Box::new(MockPublisher { sent: sent.clone() }), &config).unwrap();

        let start = Instant::now();
        publisher.send(CanonicalMessage::from("one")).await.unwrap();
        publisher.send(CanonicalMessage::from("two")).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(sent.lock().unwrap().len(), 2);
        assert!(elapsed >= Duration::from_millis(45));
    }
}
