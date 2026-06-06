use crate::models::BufferMiddleware;
use crate::traits::{BoxFuture, MessagePublisher, PublisherError, Sent, SentBatch};
use crate::CanonicalMessage;
use anyhow::anyhow;
use async_trait::async_trait;
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Duration;

struct PendingEntry {
    message: CanonicalMessage,
    result_tx: oneshot::Sender<Result<Sent, PublisherError>>,
}

struct InFlightEntry {
    message_id: u128,
    result_tx: oneshot::Sender<Result<Sent, PublisherError>>,
}

struct PendingBatch {
    entries: Vec<InFlightEntry>,
    messages: Vec<CanonicalMessage>,
}

#[derive(Default)]
struct BufferState {
    pending: VecDeque<PendingEntry>,
    flush_in_progress: bool,
    timer_generation: u64,
    flush_waiters: Vec<oneshot::Sender<()>>,
}

struct BufferCore {
    inner: Arc<dyn MessagePublisher>,
    max_messages: usize,
    max_delay: Duration,
    state: Mutex<BufferState>,
}

impl BufferCore {
    fn spawn_timer(self: &Arc<Self>, generation: u64) {
        let delay = self.max_delay;
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if let Some(core) = weak.upgrade() {
                core.on_timer(generation).await;
            }
        });
    }

    fn try_start_flush(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            if let Some(core) = weak.upgrade() {
                core.maybe_flush().await;
            }
        });
    }

    async fn on_timer(self: Arc<Self>, generation: u64) {
        let should_flush = {
            let state = self.state.lock().await;
            state.timer_generation == generation
                && !state.flush_in_progress
                && !state.pending.is_empty()
        };
        if should_flush {
            self.maybe_flush().await;
        }
    }

    async fn maybe_flush(self: Arc<Self>) {
        let batch = {
            let mut state = self.state.lock().await;
            if state.flush_in_progress || state.pending.is_empty() {
                None
            } else {
                state.flush_in_progress = true;
                state.timer_generation = state.timer_generation.wrapping_add(1);

                let count = state.pending.len().min(self.max_messages);
                let mut entries = Vec::with_capacity(count);
                let mut messages = Vec::with_capacity(count);

                for _ in 0..count {
                    if let Some(entry) = state.pending.pop_front() {
                        let message_id = entry.message.message_id;
                        messages.push(entry.message);
                        entries.push(InFlightEntry {
                            message_id,
                            result_tx: entry.result_tx,
                        });
                    }
                }

                Some(PendingBatch { entries, messages })
            }
        };

        if let Some(batch) = batch {
            self.process_batch(batch).await;
        }
    }

    async fn process_batch(self: Arc<Self>, batch: PendingBatch) {
        let send_result = self.inner.send_batch(batch.messages).await;
        distribute_batch_results(batch.entries, send_result);

        let (waiters, next_timer, should_flush_again) = {
            let mut state = self.state.lock().await;
            state.flush_in_progress = false;

            let waiters = if state.pending.is_empty() {
                std::mem::take(&mut state.flush_waiters)
            } else {
                Vec::new()
            };

            if state.pending.is_empty() {
                (waiters, None, false)
            } else if state.pending.len() >= self.max_messages {
                (waiters, None, true)
            } else {
                state.timer_generation = state.timer_generation.wrapping_add(1);
                (waiters, Some(state.timer_generation), false)
            }
        };

        for waiter in waiters {
            let _ = waiter.send(());
        }

        if let Some(generation) = next_timer {
            self.spawn_timer(generation);
        }
        if should_flush_again {
            self.try_start_flush();
        }
    }
}

fn distribute_batch_results(
    entries: Vec<InFlightEntry>,
    send_result: Result<SentBatch, PublisherError>,
) {
    match send_result {
        Ok(SentBatch::Ack) => {
            for entry in entries {
                let _ = entry.result_tx.send(Ok(Sent::Ack));
            }
        }
        Ok(SentBatch::Partial { responses, failed }) => {
            let mut response_map: HashMap<u128, CanonicalMessage> = responses
                .unwrap_or_default()
                .into_iter()
                .map(|response| (response.message_id, response))
                .collect();
            let mut failed_map: HashMap<u128, PublisherError> = failed
                .into_iter()
                .map(|(message, error)| (message.message_id, error))
                .collect();

            for entry in entries {
                let result = if let Some(error) = failed_map.remove(&entry.message_id) {
                    Err(error)
                } else if let Some(response) = response_map.remove(&entry.message_id) {
                    Ok(Sent::Response(response))
                } else {
                    Ok(Sent::Ack)
                };
                let _ = entry.result_tx.send(result);
            }
        }
        Err(error) => {
            let message = error.to_string();
            for entry in entries {
                let _ = entry.result_tx.send(Err(rebuild_error(&error, &message)));
            }
        }
    }
}

fn rebuild_error(error: &PublisherError, message: &str) -> PublisherError {
    match error {
        PublisherError::Retryable(_) => PublisherError::Retryable(anyhow!(message.to_string())),
        PublisherError::NonRetryable(_) => {
            PublisherError::NonRetryable(anyhow!(message.to_string()))
        }
        PublisherError::Connection(_) => PublisherError::Connection(anyhow!(message.to_string())),
    }
}

async fn await_send_result(
    receiver: oneshot::Receiver<Result<Sent, PublisherError>>,
) -> Result<Sent, PublisherError> {
    match receiver.await {
        Ok(result) => result,
        Err(_) => Err(PublisherError::Connection(anyhow!(
            "Buffer middleware dropped a pending send result unexpectedly"
        ))),
    }
}

pub struct BufferPublisher {
    core: Arc<BufferCore>,
}

impl BufferPublisher {
    pub fn new(
        inner: Box<dyn MessagePublisher>,
        config: &BufferMiddleware,
    ) -> anyhow::Result<Self> {
        if config.max_messages == 0 {
            return Err(anyhow!("Buffer max_messages must be greater than zero"));
        }

        Ok(Self {
            core: Arc::new(BufferCore {
                inner: inner.into(),
                max_messages: config.max_messages,
                max_delay: Duration::from_millis(config.max_delay_ms),
                state: Mutex::new(BufferState::default()),
            }),
        })
    }

    async fn enqueue_messages(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Vec<oneshot::Receiver<Result<Sent, PublisherError>>> {
        if messages.is_empty() {
            return Vec::new();
        }

        let mut state = self.core.state.lock().await;
        let was_empty = state.pending.is_empty();
        let mut receivers = Vec::with_capacity(messages.len());

        for message in messages {
            let (result_tx, result_rx) = oneshot::channel();
            state.pending.push_back(PendingEntry { message, result_tx });
            receivers.push(result_rx);
        }

        let should_flush_now = state.pending.len() >= self.core.max_messages;
        let next_timer = if !should_flush_now && was_empty && !state.flush_in_progress {
            state.timer_generation = state.timer_generation.wrapping_add(1);
            Some(state.timer_generation)
        } else {
            None
        };

        drop(state);

        if let Some(generation) = next_timer {
            self.core.spawn_timer(generation);
        }
        if should_flush_now {
            self.core.try_start_flush();
        }

        receivers
    }
}

#[async_trait]
impl MessagePublisher for BufferPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.core.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            self.flush().await?;
            if let Some(hook) = self.core.inner.on_disconnect_hook() {
                hook.await?;
            }
            Ok(())
        }))
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let mut receivers = self.enqueue_messages(vec![message]).await;
        await_send_result(receivers.pop().expect("single message receiver missing")).await
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        let receivers = self.enqueue_messages(messages.clone()).await;
        let mut responses = Vec::new();
        let mut failed = Vec::new();

        for (message, receiver) in messages.into_iter().zip(receivers) {
            match await_send_result(receiver).await {
                Ok(Sent::Ack) => {}
                Ok(Sent::Response(response)) => responses.push(response),
                Err(error) => failed.push((message, error)),
            }
        }

        if failed.is_empty() && responses.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: if responses.is_empty() {
                    None
                } else {
                    Some(responses)
                },
                failed,
            })
        }
    }

    async fn flush(&self) -> anyhow::Result<()> {
        loop {
            let flush_waiter = {
                let mut state = self.core.state.lock().await;
                if state.pending.is_empty() && !state.flush_in_progress {
                    None
                } else {
                    let (tx, rx) = oneshot::channel();
                    state.flush_waiters.push(tx);
                    Some(rx)
                }
            };

            let Some(flush_waiter) = flush_waiter else {
                break;
            };

            self.core.try_start_flush();
            flush_waiter
                .await
                .map_err(|_| anyhow!("Buffer flush waiter dropped unexpectedly"))?;
        }

        self.core.inner.flush().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::Notify;
    use tokio::time::{timeout, Instant};

    #[derive(Clone)]
    struct BlockingPublisher {
        batches: Arc<StdMutex<Vec<Vec<CanonicalMessage>>>>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl MessagePublisher for BlockingPublisher {
        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            self.batches.lock().unwrap().push(messages);
            self.release.notified().await;
            Ok(SentBatch::Ack)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Clone)]
    struct PartialPublisher;

    #[async_trait]
    impl MessagePublisher for PartialPublisher {
        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            let mut response = CanonicalMessage::from("ok");
            response.message_id = messages[0].message_id;
            Ok(SentBatch::Partial {
                responses: Some(vec![response]),
                failed: vec![(
                    messages[1].clone(),
                    PublisherError::NonRetryable(anyhow!("boom")),
                )],
            })
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Clone)]
    struct RecordingPublisher {
        batches: Arc<StdMutex<Vec<Vec<CanonicalMessage>>>>,
    }

    #[async_trait]
    impl MessagePublisher for RecordingPublisher {
        async fn send_batch(
            &self,
            messages: Vec<CanonicalMessage>,
        ) -> Result<SentBatch, PublisherError> {
            self.batches.lock().unwrap().push(messages);
            Ok(SentBatch::Ack)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_buffer_merges_single_sends_without_acknowledging_early() {
        let batches = Arc::new(StdMutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        let publisher = Arc::new(
            BufferPublisher::new(
                Box::new(BlockingPublisher {
                    batches: batches.clone(),
                    release: release.clone(),
                }),
                &BufferMiddleware {
                    max_messages: 8,
                    max_delay_ms: 25,
                },
            )
            .unwrap(),
        );

        let first = {
            let publisher = Arc::clone(&publisher);
            tokio::spawn(async move { publisher.send(CanonicalMessage::from("one")).await })
        };
        let second = {
            let publisher = Arc::clone(&publisher);
            tokio::spawn(async move { publisher.send(CanonicalMessage::from("two")).await })
        };

        let started = Instant::now();
        loop {
            if batches.lock().unwrap().len() == 1 {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(1));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(batches.lock().unwrap()[0].len(), 2);
        assert!(!first.is_finished());
        assert!(!second.is_finished());

        release.notify_waiters();

        assert!(matches!(first.await.unwrap().unwrap(), Sent::Ack));
        assert!(matches!(second.await.unwrap().unwrap(), Sent::Ack));
    }

    #[tokio::test]
    async fn test_buffer_maps_partial_batch_results_back_to_callers() {
        let publisher = BufferPublisher::new(
            Box::new(PartialPublisher),
            &BufferMiddleware {
                max_messages: 8,
                max_delay_ms: 0,
            },
        )
        .unwrap();

        let first = CanonicalMessage::from("one");
        let second = CanonicalMessage::from("two");
        let result = publisher
            .send_batch(vec![first.clone(), second.clone()])
            .await
            .unwrap();

        match result {
            SentBatch::Partial { responses, failed } => {
                assert_eq!(responses.unwrap().len(), 1);
                assert_eq!(failed.len(), 1);
                assert_eq!(failed[0].0.message_id, second.message_id);
            }
            SentBatch::Ack => panic!("expected partial batch result"),
        }
    }

    #[tokio::test]
    async fn test_buffer_flush_forces_pending_messages_out_before_timeout() {
        let batches = Arc::new(StdMutex::new(Vec::new()));
        let publisher = Arc::new(
            BufferPublisher::new(
                Box::new(RecordingPublisher {
                    batches: batches.clone(),
                }),
                &BufferMiddleware {
                    max_messages: 16,
                    max_delay_ms: 1_000,
                },
            )
            .unwrap(),
        );

        let send_task = {
            let publisher = Arc::clone(&publisher);
            tokio::spawn(async move { publisher.send(CanonicalMessage::from("one")).await })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(batches.lock().unwrap().is_empty());

        publisher.flush().await.unwrap();
        timeout(Duration::from_secs(1), send_task)
            .await
            .expect("send task did not complete after flush")
            .unwrap()
            .unwrap();

        assert_eq!(batches.lock().unwrap().len(), 1);
        assert_eq!(batches.lock().unwrap()[0].len(), 1);
    }
}
