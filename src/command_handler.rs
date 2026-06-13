//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::traits::{BoxFuture, Handler, MessagePublisher};
use crate::traits::{Handled, HandlerError};
use crate::CanonicalMessage;
use anyhow::anyhow;
use async_trait::async_trait;
use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use crate::traits::{PublisherError, Sent, SentBatch};
#[async_trait]
impl<F, Fut> Handler for F
where
    F: Fn(CanonicalMessage) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Handled, HandlerError>> + Send,
{
    async fn handle(&self, msg: CanonicalMessage) -> Result<Handled, HandlerError> {
        self(msg).await
    }
}

/// A publisher middleware that intercepts messages and passes them to a `Handler`.
/// If the handler returns a new message, it is passed to the inner publisher.
pub struct CommandPublisher {
    inner: Box<dyn MessagePublisher>,
    handler: Arc<dyn Handler>,
}

impl CommandPublisher {
    pub fn new(inner: impl MessagePublisher, handler: impl Handler + 'static) -> Self {
        Self {
            inner: Box::new(inner),
            handler: Arc::new(handler),
        }
    }
}

#[async_trait]
impl MessagePublisher for CommandPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let inbound_correlation_id = message.metadata.get("correlation_id").cloned();
        let original_id = message.message_id;
        match self.handler.handle(message).await {
            Ok(Handled::Publish(mut response_msg)) => {
                // For internal correlation, set the response message's ID to the original.
                response_msg.message_id = original_id;
                // For end-to-end tracing, propagate or create a correlation_id.
                let fallback_correlation_id =
                    inbound_correlation_id.unwrap_or_else(|| format!("{:032x}", original_id));
                response_msg
                    .metadata
                    .entry("correlation_id".to_string())
                    .or_insert(fallback_correlation_id);
                self.inner.send(response_msg).await
            }
            Ok(Handled::Ack) => Ok(Sent::Ack),
            Err(e) => Err(e), // Converts HandlerError to PublisherError
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let handler_results = self.handler.handle_many(messages.clone()).await;

        if handler_results.len() != messages.len() {
            return Err(PublisherError::NonRetryable(anyhow::anyhow!(
                "handler returned {} results for {} messages",
                handler_results.len(),
                messages.len()
            )));
        }

        let mut responses = Vec::new();
        let mut failed = Vec::new();

        let mut iter = messages.into_iter().zip(handler_results);
        while let Some((message, result)) = iter.next() {
            let original_id = message.message_id;
            let inbound_correlation_id = message.metadata.get("correlation_id").cloned();
            match result {
                Ok(Handled::Ack) => {}
                Ok(Handled::Publish(mut response_msg)) => {
                    response_msg.message_id = original_id;
                    let fallback_correlation_id =
                        inbound_correlation_id.unwrap_or_else(|| format!("{:032x}", original_id));
                    response_msg
                        .metadata
                        .entry("correlation_id".to_string())
                        .or_insert(fallback_correlation_id);

                    match self.inner.send(response_msg).await {
                        Ok(Sent::Response(response)) => responses.push(response),
                        Ok(Sent::Ack) => {}
                        Err(PublisherError::NonRetryable(err)) => {
                            failed.push((message, PublisherError::NonRetryable(err)));
                        }
                        Err(PublisherError::Retryable(err)) => {
                            failed.push((message, PublisherError::Retryable(err)));
                            for (remaining, _) in iter {
                                failed.push((
                                    remaining,
                                    PublisherError::Retryable(anyhow!(
                                        "Batch aborted due to previous error"
                                    )),
                                ));
                            }
                            break;
                        }
                        Err(PublisherError::Connection(err)) => {
                            failed.push((message, PublisherError::Connection(err)));
                            for (remaining, _) in iter {
                                failed.push((
                                    remaining,
                                    PublisherError::Connection(anyhow!(
                                        "Batch aborted due to previous connection error"
                                    )),
                                ));
                            }
                            break;
                        }
                    }
                }
                Err(HandlerError::NonRetryable(err)) => {
                    failed.push((message, PublisherError::NonRetryable(err)));
                }
                Err(HandlerError::Retryable(err)) => {
                    failed.push((message, PublisherError::Retryable(err)));
                    for (remaining, _) in iter {
                        failed.push((
                            remaining,
                            PublisherError::Retryable(anyhow!(
                                "Batch aborted due to previous error"
                            )),
                        ));
                    }
                    break;
                }
                Err(HandlerError::Connection(err)) => {
                    failed.push((message, PublisherError::Connection(err)));
                    for (remaining, _) in iter {
                        failed.push((
                            remaining,
                            PublisherError::Connection(anyhow!(
                                "Batch aborted due to previous connection error"
                            )),
                        ));
                    }
                    break;
                }
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
        self.inner.flush().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::endpoints::memory::MemoryPublisher;

    #[tokio::test]
    async fn test_command_handler_produces_response() {
        let memory_publisher = MemoryPublisher::new_local("test_command_out_resp", 10);
        let channel = memory_publisher.channel();

        let handler = |msg: CanonicalMessage| async move {
            let response_payload = format!("response_to_{}", String::from_utf8_lossy(&msg.payload));
            Ok(Handled::Publish(response_payload.into()))
        };

        let publisher = CommandPublisher::new(memory_publisher, handler);

        publisher.send("command1".into()).await.unwrap();

        let received = channel.drain_messages();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].payload, "response_to_command1".as_bytes());
    }

    #[tokio::test]
    async fn test_command_handler_acks() {
        let memory_publisher = MemoryPublisher::new_local("test_command_out_ack", 10);
        let channel = memory_publisher.channel();

        let handler = |_msg: CanonicalMessage| async move { Ok(Handled::Ack) };

        let publisher = CommandPublisher::new(memory_publisher, handler);

        let result = publisher.send("command1".into()).await.unwrap();

        assert!(matches!(result, Sent::Ack));
        let received = channel.drain_messages();
        assert_eq!(received.len(), 0);
    }

    #[tokio::test]
    async fn test_command_handler_retryable_error() {
        let memory_publisher = MemoryPublisher::new_local("test_command_out_err", 10);

        let handler = |_msg: CanonicalMessage| async move {
            Err(HandlerError::Retryable(anyhow::anyhow!("db is down")))
        };

        let publisher = CommandPublisher::new(memory_publisher, handler);
        let result = publisher.send("command1".into()).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        // The HandlerError is converted into a PublisherError
        assert!(matches!(err, PublisherError::Retryable(_)));
    }

    #[tokio::test]
    async fn test_command_handler_integration_with_memory_consumer() {
        use crate::endpoints::memory::MemoryConsumer;
        use crate::traits::MessageConsumer;

        // 1. Setup Input (MemoryConsumer)
        let mut consumer = MemoryConsumer::new_local("cmd_input", 10);
        let input_channel = consumer.channel();

        // 2. Setup Output (MemoryPublisher wrapped by CommandPublisher)
        let memory_publisher = MemoryPublisher::new_local("cmd_output", 10);
        let output_channel = memory_publisher.channel();

        // 3. Create Publisher Middleware with inline handler
        let publisher =
            CommandPublisher::new(memory_publisher, |msg: CanonicalMessage| async move {
                let payload = String::from_utf8_lossy(&msg.payload);
                let response = format!("processed_{}", payload);
                Ok(Handled::Publish(response.into()))
            });

        // 4. Inject message into input
        input_channel
            .send_message("test_data".into())
            .await
            .unwrap();

        // 5. Simulate Bridge Loop (Consume -> Publish)
        let received = consumer.receive().await.unwrap();
        let result = publisher.send(received.message).await.unwrap();

        // 6. Verify
        assert!(matches!(result, Sent::Ack));

        let output_msgs = output_channel.drain_messages();
        assert_eq!(output_msgs.len(), 1);
        assert_eq!(output_msgs[0].payload.to_vec(), b"processed_test_data");

        let _ = (received.commit)(crate::traits::MessageDisposition::Ack).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_command_handler_with_route_config() {
        use crate::models::{Endpoint, Route};

        let success = Arc::new(AtomicBool::new(false));
        let success_clone = success.clone();

        // 1. Define Handler
        let handler = move |mut msg: CanonicalMessage| {
            success_clone.store(true, Ordering::SeqCst);
            msg.set_payload_str(format!("modified {}", msg.get_payload_str()));
            async move { Ok(Handled::Publish(msg)) }
        };
        // 2. Define Route
        let route = Route::new(
            Endpoint::new_memory("route_in", 100),
            Endpoint::new_memory("route_out", 100),
        )
        .with_handler(handler);

        // 3. Deploy Route
        route.deploy("command_handler_test_route").await.unwrap();

        // 4. Inject Data
        let input_channel = route.input.channel().unwrap();
        input_channel.send_message("hello".into()).await.unwrap();

        // 5. Verify
        let mut verifier = route.connect_to_output("verifier").await.unwrap();
        let received = verifier.receive().await.unwrap();
        assert_eq!(received.message.get_payload_str(), "modified hello");
        assert!(success.load(Ordering::SeqCst));
        Route::stop("command_handler_test_route").await;
    }

    #[tokio::test]
    async fn test_command_handler_inner_publisher_failure() {
        use crate::traits::MessagePublisher;

        struct FailPublisher;
        #[async_trait]
        impl MessagePublisher for FailPublisher {
            async fn send(&self, _msg: CanonicalMessage) -> Result<Sent, PublisherError> {
                Err(PublisherError::NonRetryable(anyhow::anyhow!("inner fail")))
            }
            async fn send_batch(
                &self,
                _msgs: Vec<CanonicalMessage>,
            ) -> Result<SentBatch, PublisherError> {
                Ok(SentBatch::Ack)
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        let handler = |msg: CanonicalMessage| async move { Ok(Handled::Publish(msg)) };
        let publisher = CommandPublisher::new(FailPublisher, handler);
        let result = publisher.send("test".into()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("inner fail"));
    }

    #[tokio::test]
    async fn test_command_handler_preserves_message_id() {
        let memory_publisher = MemoryPublisher::new_local("test_cmd_id_preservation", 10);
        let channel = memory_publisher.channel();

        let handler = |_msg: CanonicalMessage| async move {
            let new_msg = CanonicalMessage::new(b"response".to_vec(), None);
            Ok(Handled::Publish(new_msg))
        };

        let publisher = CommandPublisher::new(memory_publisher, handler);
        let original_id = 987654321u128;
        publisher
            .send(CanonicalMessage::new(b"req".to_vec(), Some(original_id)))
            .await
            .unwrap();

        let received = channel.drain_messages();
        assert_eq!(received[0].message_id, original_id);
    }

    #[tokio::test]
    async fn test_command_handler_send_batch_uses_handler_handle_many() {
        struct BatchAwareHandler {
            single_calls: AtomicUsize,
            batch_calls: AtomicUsize,
        }

        #[async_trait]
        impl Handler for BatchAwareHandler {
            async fn handle(&self, _msg: CanonicalMessage) -> Result<Handled, HandlerError> {
                self.single_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Handled::Ack)
            }

            async fn handle_many(
                &self,
                msgs: Vec<CanonicalMessage>,
            ) -> Vec<Result<Handled, HandlerError>> {
                self.batch_calls.fetch_add(1, Ordering::SeqCst);
                msgs.into_iter()
                    .map(|mut msg| {
                        msg.set_payload_str(format!("batched {}", msg.get_payload_str()));
                        Ok(Handled::Publish(msg))
                    })
                    .collect()
            }
        }

        let memory_publisher = MemoryPublisher::new_local("test_cmd_batch_many", 10);
        let channel = memory_publisher.channel();
        let handler = Arc::new(BatchAwareHandler {
            single_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
        });
        let publisher = CommandPublisher::new(memory_publisher, handler.clone());

        let result = publisher
            .send_batch(vec!["one".into(), "two".into(), "three".into()])
            .await
            .unwrap();

        assert!(matches!(result, SentBatch::Ack));
        assert_eq!(handler.single_calls.load(Ordering::SeqCst), 0);
        assert_eq!(handler.batch_calls.load(Ordering::SeqCst), 1);

        let received = channel.drain_messages();
        assert_eq!(received.len(), 3);
        assert_eq!(received[0].get_payload_str(), "batched one");
        assert_eq!(received[1].get_payload_str(), "batched two");
        assert_eq!(received[2].get_payload_str(), "batched three");
    }

    #[tokio::test]
    async fn test_command_handler_send_batch_non_retryable_handler_error_continues() {
        struct PartiallyFailingHandler;

        #[async_trait]
        impl Handler for PartiallyFailingHandler {
            async fn handle(&self, _msg: CanonicalMessage) -> Result<Handled, HandlerError> {
                unreachable!("send_batch should use handle_many")
            }

            async fn handle_many(
                &self,
                msgs: Vec<CanonicalMessage>,
            ) -> Vec<Result<Handled, HandlerError>> {
                msgs.into_iter()
                    .map(|msg| {
                        if msg.get_payload_str() == "two" {
                            Err(HandlerError::NonRetryable(anyhow::anyhow!("bad message")))
                        } else {
                            Ok(Handled::Publish(msg))
                        }
                    })
                    .collect()
            }
        }

        let memory_publisher = MemoryPublisher::new_local("test_cmd_batch_non_retryable", 10);
        let channel = memory_publisher.channel();
        let publisher = CommandPublisher::new(memory_publisher, PartiallyFailingHandler);

        let result = publisher
            .send_batch(vec!["one".into(), "two".into(), "three".into()])
            .await
            .unwrap();

        match result {
            SentBatch::Partial { responses, failed } => {
                assert!(responses.is_none());
                assert_eq!(failed.len(), 1);
                assert_eq!(failed[0].0.get_payload_str(), "two");
                assert!(matches!(failed[0].1, PublisherError::NonRetryable(_)));
            }
            other => panic!("expected partial failure, got {other:?}"),
        }

        let received = channel.drain_messages();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].get_payload_str(), "one");
        assert_eq!(received[1].get_payload_str(), "three");
    }

    #[tokio::test]
    async fn test_command_handler_send_batch_retryable_publish_error_aborts_remainder() {
        struct PublishAllHandler;

        #[async_trait]
        impl Handler for PublishAllHandler {
            async fn handle(&self, _msg: CanonicalMessage) -> Result<Handled, HandlerError> {
                unreachable!("send_batch should use handle_many")
            }

            async fn handle_many(
                &self,
                msgs: Vec<CanonicalMessage>,
            ) -> Vec<Result<Handled, HandlerError>> {
                msgs.into_iter()
                    .map(|msg| Ok(Handled::Publish(msg)))
                    .collect()
            }
        }

        struct RetryableSecondSendPublisher {
            sends: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl MessagePublisher for RetryableSecondSendPublisher {
            async fn send(&self, _msg: CanonicalMessage) -> Result<Sent, PublisherError> {
                let send_number = self.sends.fetch_add(1, Ordering::SeqCst) + 1;
                if send_number == 2 {
                    Err(PublisherError::Retryable(anyhow::anyhow!(
                        "temporary failure"
                    )))
                } else {
                    Ok(Sent::Ack)
                }
            }

            async fn send_batch(
                &self,
                _msgs: Vec<CanonicalMessage>,
            ) -> Result<SentBatch, PublisherError> {
                unreachable!(
                    "command batch publishing should preserve old sequential send behavior"
                )
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        let sends = Arc::new(AtomicUsize::new(0));
        let publisher = CommandPublisher::new(
            RetryableSecondSendPublisher {
                sends: sends.clone(),
            },
            PublishAllHandler,
        );

        let result = publisher
            .send_batch(vec!["one".into(), "two".into(), "three".into()])
            .await
            .unwrap();

        assert_eq!(sends.load(Ordering::SeqCst), 2);
        match result {
            SentBatch::Partial { responses, failed } => {
                assert!(responses.is_none());
                assert_eq!(failed.len(), 2);
                assert_eq!(failed[0].0.get_payload_str(), "two");
                assert_eq!(failed[1].0.get_payload_str(), "three");
                assert!(matches!(failed[0].1, PublisherError::Retryable(_)));
                assert!(matches!(failed[1].1, PublisherError::Retryable(_)));
            }
            other => panic!("expected partial failure, got {other:?}"),
        }
    }
}
