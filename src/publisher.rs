use crate::endpoints;
use crate::models;
use crate::traits;
use crate::CanonicalMessage;
use crate::Sent;
use crate::SentBatch;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// A simple wrapper around a publisher to send messages to a specific endpoint.
#[derive(Clone)]
pub struct Publisher {
    publisher: std::sync::Arc<dyn traits::MessagePublisher>,
}

static PUBLISHER_REGISTRY: OnceLock<RwLock<HashMap<String, Publisher>>> = OnceLock::new();

impl Publisher {
    /// Creates a new publisher for the given endpoint configuration.
    pub async fn new(endpoint: models::Endpoint) -> anyhow::Result<Self> {
        let publisher = endpoints::create_publisher_from_route("publisher", &endpoint).await?;
        Ok(Self { publisher })
    }

    /// Sends a message and expects a response message from the endpoint.
    /// Returns an error if the endpoint does not support responses (e.g. returns a simple Ack).
    pub async fn request(&self, message: CanonicalMessage) -> anyhow::Result<CanonicalMessage> {
        match self.publisher.send(message).await? {
            Sent::Response(resp) => Ok(resp),
            Sent::Ack => Err(anyhow::anyhow!("Expected a response from the endpoint, but received only an acknowledgment (Ack). Ensure the endpoint and route are correctly configured for request-reply.")),
        }
    }

    /// Sends a batch of messages and expects a response message for each from the endpoint.
    /// Returns an error if any message fails or if the endpoint does not support responses (e.g. returns a simple Ack).
    pub async fn request_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<Vec<CanonicalMessage>> {
        let count = messages.len();
        if count == 0 {
            return Ok(Vec::new());
        }
        match self.publisher.send_batch(messages).await? {
            SentBatch::Partial { responses: Some(resps), failed } if failed.is_empty() && resps.len() == count => Ok(resps),
            SentBatch::Ack => Err(anyhow::anyhow!("Expected responses from the endpoint, but received only acknowledgments (Ack). Ensure the endpoint and route are correctly configured for request-reply.")),
            _ => Err(anyhow::anyhow!("Request batch failed to return the expected responses. Ensure the endpoint and route are correctly configured for request-reply.")),
        }
    }

    /// Sends a message to the configured endpoint.
    pub async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Sent> {
        self.publisher
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Sends a batch of messages to the configured endpoint.
    pub async fn send_batch(&self, messages: Vec<CanonicalMessage>) -> anyhow::Result<SentBatch> {
        self.publisher
            .send_batch(messages)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn inner(&self) -> std::sync::Arc<dyn traits::MessagePublisher> {
        self.publisher.clone()
    }

    /// Attempt to borrow the underlying concrete publisher as `T`.
    /// Returns `Some(&T)` if the underlying publisher is of type `T`.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.publisher.as_ref().as_any().downcast_ref::<T>()
    }

    /// Registers this publisher globally with a given name.
    pub fn register(&self, name: &str) -> Option<Self> {
        let registry = PUBLISHER_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
        let mut map = registry.write().expect("Publisher registry lock poisoned");
        map.insert(name.to_string(), self.clone())
    }

    /// Retrieves a registered publisher by name.
    pub fn get(name: &str) -> Option<Self> {
        let registry = PUBLISHER_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
        let map = registry.read().expect("Publisher registry lock poisoned");
        map.get(name).cloned()
    }

    /// Removes a registered publisher by name.
    pub fn unregister(name: &str) -> Option<Self> {
        let registry = PUBLISHER_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
        let mut map = registry.write().expect("Publisher registry lock poisoned");
        map.remove(name)
    }
}

// Generic conversion from any concrete publisher into the generic `Publisher` wrapper.
impl<T> From<T> for Publisher
where
    T: traits::MessagePublisher + 'static,
{
    fn from(p: T) -> Self {
        Self {
            publisher: std::sync::Arc::new(p),
        }
    }
}

pub fn get_publisher(name: &str) -> Option<Publisher> {
    Publisher::get(name)
}

pub fn list_publishers() -> Vec<String> {
    let registry = PUBLISHER_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    registry
        .read()
        .expect("Publisher registry lock poisoned")
        .keys()
        .cloned()
        .collect()
}

pub fn register_publisher(name: &str, publisher: Publisher) -> Option<Publisher> {
    publisher.register(name)
}

pub fn unregister_publisher(name: &str) -> Option<Publisher> {
    Publisher::unregister(name)
}

pub use crate::middleware::apply_middlewares_to_publisher as apply_middlewares;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Endpoint, PublisherConfig};
    use crate::CanonicalMessage;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_publisher_config_usage() {
        // 1. Create a PublisherConfig (simulating loading from config)
        let mut publisher_config: PublisherConfig = HashMap::new();
        let endpoint = Endpoint::new_memory("pub_test_topic", 10);
        let channel = endpoint.channel().unwrap();
        publisher_config.insert("my_publisher".to_string(), endpoint);

        // 2. Initialize publishers from config
        let mut publishers = HashMap::new();
        for (name, endpoint) in publisher_config {
            let publisher = Publisher::new(endpoint)
                .await
                .expect("Failed to create publisher");
            publishers.insert(name, publisher);
        }

        // 3. Retrieve and use the publisher
        let publisher = publishers.get("my_publisher").expect("Publisher not found");
        let msg = CanonicalMessage::from("hello world");

        publisher.send(msg).await.expect("Failed to send message");

        // 4. Verify with underlying channel
        let received = channel.drain_messages();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].get_payload_str(), "hello world");
    }

    #[tokio::test]
    async fn test_publisher_registry() {
        let endpoint = Endpoint::new_memory("registry_test", 10);
        let publisher = Publisher::new(endpoint)
            .await
            .expect("Failed to create publisher");

        publisher.register("static_pub");

        let retrieved = Publisher::get("static_pub").expect("Failed to get publisher");
        assert!(Arc::ptr_eq(&publisher.publisher, &retrieved.publisher));
    }

    #[tokio::test]
    async fn test_publisher_request_batch() {
        use crate::traits::{MessagePublisher, PublisherError, SentBatch};
        use async_trait::async_trait;
        use std::any::Any;

        struct MockRR;
        #[async_trait]
        impl MessagePublisher for MockRR {
            async fn send_batch(
                &self,
                messages: Vec<CanonicalMessage>,
            ) -> Result<SentBatch, PublisherError> {
                Ok(SentBatch::Partial {
                    responses: Some(messages),
                    failed: vec![],
                })
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        let publisher: Publisher = MockRR.into();
        let msgs = vec![CanonicalMessage::from("1"), CanonicalMessage::from("2")];
        let res = publisher.request_batch(msgs).await.unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].get_payload_str(), "1");
    }
}
