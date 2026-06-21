//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
use crate::models::StaticConfig;
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
    metadata: std::collections::HashMap<String, String>,
}

impl StaticEndpointPublisher {
    pub fn new(config: &StaticConfig) -> anyhow::Result<Self> {
        // In `raw` mode the body is sent verbatim; otherwise it is JSON-encoded
        // as a string (the historical behaviour, kept for backward compatibility).
        let payload = if config.raw {
            config.body.clone().into_bytes()
        } else {
            serde_json::to_vec(&Value::String(config.body.clone()))
                .context("Failed to serialize static response to JSON")?
        };
        Ok(Self {
            payload,
            content_raw: config.body.clone(),
            metadata: config.metadata.clone(),
        })
    }
}

#[async_trait]
impl MessagePublisher for StaticEndpointPublisher {
    async fn send(&self, _message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let mut response_msg = CanonicalMessage::new(self.payload.clone(), None);
        // Attach configured metadata to the response. When this feeds an HTTP
        // response these become headers (e.g. `content-type`), so the server
        // emits them instead of defaulting to `application/octet-stream`.
        for (key, value) in &self.metadata {
            response_msg.metadata.insert(key.clone(), value.clone());
        }
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
    metadata: std::collections::HashMap<String, String>,
}

impl StaticRequestConsumer {
    pub fn new(config: &StaticConfig) -> anyhow::Result<Self> {
        Ok(Self {
            payload: Bytes::copy_from_slice(config.body.as_bytes()),
            content: config.body.clone(),
            metadata: config.metadata.clone(),
        })
    }
}

#[async_trait]
impl MessageConsumer for StaticRequestConsumer {
    // Committing is a no-op for a static consumer, so ordering is irrelevant.
    fn commit_requires_order(&self) -> bool {
        false
    }
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        let mut message = CanonicalMessage::new_bytes(self.payload.clone(), None);
        message.metadata = self.metadata.clone();
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
            let mut message = CanonicalMessage::new_bytes(self.payload.clone(), None);
            message.metadata = self.metadata.clone();
            messages.push(message);
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

    fn config(body: &str) -> StaticConfig {
        StaticConfig {
            body: body.to_string(),
            raw: false,
            metadata: std::collections::HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_static_publisher() {
        let content = "static_response";
        let publisher = StaticEndpointPublisher::new(&config(content)).unwrap();
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
    async fn test_static_publisher_raw_with_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("content-type".to_string(), "text/plain".to_string());
        metadata.insert("server".to_string(), "mq-bridge".to_string());
        let publisher = StaticEndpointPublisher::new(&StaticConfig {
            body: "Hello, World!".to_string(),
            raw: true,
            metadata,
        })
        .unwrap();

        let response = publisher
            .send(CanonicalMessage::new(vec![], None))
            .await
            .unwrap();
        let response_msg = match response {
            Sent::Response(msg) => msg,
            _ => panic!("Expected response"),
        };
        // Raw payload: no JSON quoting.
        assert_eq!(response_msg.payload.as_ref(), b"Hello, World!");
        assert_eq!(
            response_msg
                .metadata
                .get("content-type")
                .map(String::as_str),
            Some("text/plain")
        );
        assert_eq!(
            response_msg.metadata.get("server").map(String::as_str),
            Some("mq-bridge")
        );
    }

    #[tokio::test]
    async fn test_static_consumer() {
        let content = "static_message";
        let mut consumer = StaticRequestConsumer::new(&config(content)).unwrap();

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
            assert_eq!(val.body, "static_input_value");
        } else {
            panic!("Input was not static");
        }

        if let EndpointType::Static(val) = &route.output.endpoint_type {
            assert_eq!(val.body, "static_output_value");
        } else {
            panic!("Output was not static");
        }
    }

    #[test]
    fn test_static_config_simple_serializes_as_bare_string() {
        // Backward compat: a plain static config must serialize back to a bare
        // string (not a map), so configs written by this version stay readable
        // by older versions that only understand `static: "value"`.
        let cfg = StaticConfig::from("hello");
        let yaml = serde_yaml_ng::to_string(&cfg).unwrap();
        assert_eq!(yaml.trim(), "hello");

        let cfg = StaticConfig {
            body: "hi".to_string(),
            raw: true,
            metadata: std::collections::HashMap::new(),
        };
        let yaml = serde_yaml_ng::to_string(&cfg).unwrap();
        assert!(yaml.contains("body: hi"), "expected map form, got: {yaml}");
        assert!(yaml.contains("raw: true"));
    }

    #[test]
    fn test_static_config_map_form() {
        use crate::models::{Config, EndpointType};

        let yaml = r#"
test_route:
  input:
    static: "in"
  output:
    static:
      body: "Hello, World!"
      raw: true
      metadata:
        content-type: "text/plain"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse YAML");
        let route = config.get("test_route").expect("Route should exist");

        if let EndpointType::Static(val) = &route.output.endpoint_type {
            assert_eq!(val.body, "Hello, World!");
            assert_eq!(
                val.metadata.get("content-type").map(String::as_str),
                Some("text/plain")
            );
            assert!(val.raw);
        } else {
            panic!("Output was not static");
        }
    }
}
