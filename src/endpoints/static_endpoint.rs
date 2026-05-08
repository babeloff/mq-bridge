//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, Received, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::any::Any;
use tracing::trace;

/// A sink that responds with a static, pre-configured message.
#[derive(Clone)]
pub struct StaticEndpointPublisher {
    payload: Vec<u8>,
    content_raw: String,
}

impl StaticEndpointPublisher {
    pub fn new(config: &str) -> anyhow::Result<Self> {
        let payload = serde_json::to_vec(&Value::String(config.to_string()))
            .context("Failed to serialize static response to JSON")?;
        Ok(Self {
            payload,
            content_raw: config.to_string(),
        })
    }
}

#[async_trait]
impl MessagePublisher for StaticEndpointPublisher {
    async fn send(&self, _message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let response_msg = CanonicalMessage::new(self.payload.clone(), None);
        trace!(
            message_id = %format!("{:032x}", response_msg.message_id),
            response = %self.content_raw, "Sending static response"
        );
        Ok(Sent::Response(response_msg))
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        crate::traits::send_batch_helper(self, messages, |publisher, message| {
            Box::pin(publisher.send(message))
        })
        .await
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(()) // Nothing to flush for a static response
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A source that always produces the same static message.
#[derive(Clone)]
pub struct StaticRequestConsumer {
    payload: Bytes,
    #[allow(dead_code)]
    content: String, // Kept for metadata/tracing if needed
}

impl StaticRequestConsumer {
    pub fn new(config: &str) -> anyhow::Result<Self> {
        Ok(Self {
            payload: Bytes::copy_from_slice(config.as_bytes()),
            content: config.to_string(),
        })
    }
}

#[async_trait]
impl MessageConsumer for StaticRequestConsumer {
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        let message = CanonicalMessage::new_bytes(self.payload.clone(), None);
        trace!(message_id = %format!("{:032x}", message.message_id), "Producing static message");
        let commit = Box::new(|_disposition: MessageDisposition| {
            Box::pin(async { Ok(()) }) as BoxFuture<'static, anyhow::Result<()>>
        });
        Ok(Received { message, commit })
    }

    async fn receive_batch(
        &mut self,
        _max_messages: usize,
    ) -> Result<ReceivedBatch, ConsumerError> {
        // To properly utilize batching, we generate `_max_messages` here.
        // Each message still involves cloning the payload and generating a new UUID.
        let mut messages = Vec::with_capacity(_max_messages);
        for _ in 0.._max_messages {
            messages.push(CanonicalMessage::new_bytes(self.payload.clone(), None));
        }
        // For a static consumer, committing is a no-op, so we provide a simple async no-op closure.
        let commit = Box::new(|_disposition: Vec<MessageDisposition>| {
            Box::pin(async { Ok(()) }) as BoxFuture<'static, anyhow::Result<()>>
        });
        Ok(ReceivedBatch { messages, commit })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CanonicalMessage;
    use serde_json::Value;

    #[tokio::test]
    async fn test_static_publisher() {
        let content = "static_response";
        let publisher = StaticEndpointPublisher::new(content).unwrap();
        let msg = CanonicalMessage::new(vec![], None);

        let response = publisher.send(msg).await.unwrap();
        let response_msg = match response {
            Sent::Response(msg) => msg,
            _ => panic!("Expected response"),
        };
        let expected_payload = serde_json::to_vec(&Value::String(content.to_string())).unwrap();
        assert_eq!(response_msg.payload, expected_payload);
    }

    #[tokio::test]
    async fn test_static_consumer() {
        let content = "static_message";
        let mut consumer = StaticRequestConsumer::new(content).unwrap();

        let received = consumer.receive().await.unwrap();
        assert_eq!(received.message.payload, content.as_bytes());
    }

    #[test]
    fn test_static_config_yaml() {
        use crate::models::{Config, EndpointType};

        let yaml = r#"
test_route:
  input:
    static: "static_input_value"
  output:
    static: "static_output_value"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse YAML");
        let route = config.get("test_route").expect("Route should exist");

        if let EndpointType::Static(val) = &route.input.endpoint_type {
            assert_eq!(val, "static_input_value");
        } else {
            panic!("Input was not static");
        }

        if let EndpointType::Static(val) = &route.output.endpoint_type {
            assert_eq!(val, "static_output_value");
        } else {
            panic!("Output was not static");
        }
    }
}
