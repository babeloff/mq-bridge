use std::any::Any;

use async_trait::async_trait;
use mq_bridge::{
    outcomes::ReceivedBatch,
    traits::{ConsumerError, CustomEndpointFactory, MessageConsumer},
};

#[derive(Debug, Default)]
struct FixtureFactory;

#[derive(Debug)]
struct FixtureConsumer;

#[async_trait]
impl MessageConsumer for FixtureConsumer {
    async fn receive_batch(
        &mut self,
        _max_messages: usize,
    ) -> Result<ReceivedBatch, ConsumerError> {
        Ok(ReceivedBatch::empty())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl CustomEndpointFactory for FixtureFactory {
    async fn create_consumer(
        &self,
        _route_name: &str,
        _config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        Ok(Box::new(FixtureConsumer))
    }
}

mq_bridge::export_endpoint_plugin! {
    name: "startup-fixture",
    factory: FixtureFactory,
}
