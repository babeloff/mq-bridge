//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::models::GrpcConfig;
use crate::traits::{
    ConsumerError, MessageConsumer, MessagePublisher, PublisherError, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Result;
use async_trait::async_trait;
use std::any::Any;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use uuid::Uuid;

pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("mqbridge");
}

use proto::bridge_client::BridgeClient;
use proto::{BridgeMessage, SubscribeRequest};
use tonic::Request;

pub struct GrpcConsumer {
    client: Mutex<BridgeClient<Channel>>,
    stream: Mutex<Option<tonic::Streaming<BridgeMessage>>>,
    topic: String,
}

impl GrpcConsumer {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let client = BridgeClient::connect(config.url.clone()).await?;
        let topic = config.topic.clone().unwrap_or_else(|| "default".to_string());
        Ok(Self {
            client: Mutex::new(client),
            stream: Mutex::new(None),
            topic,
        })
    }
}

#[async_trait]
impl MessageConsumer for GrpcConsumer {
    async fn receive_batch(
        &mut self,
        _max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        let mut stream_guard = self.stream.lock().await;
        if stream_guard.is_none() {
            let request = Request::new(SubscribeRequest {
                topic: self.topic.clone(),
            });
            let stream = self
                .client
                .lock()
                .await
                .subscribe(request)
                .await
                .map_err(|e| ConsumerError::Connection(e.into()))?
                .into_inner();
            *stream_guard = Some(stream);
        }

        if let Some(stream) = stream_guard.as_mut() {
            match stream.message().await {
                Ok(Some(msg)) => {
                    let message_id = if msg.id.is_empty() {
                        None
                    } else if let Ok(uuid) = Uuid::parse_str(&msg.id) {
                        Some(uuid.as_u128())
                    } else if let Ok(n) = u128::from_str_radix(msg.id.trim_start_matches("0x"), 16) {
                        Some(n)
                    } else {
                        msg.id.parse::<u128>().ok()
                    };

                    let canonical = CanonicalMessage::new(msg.payload, message_id)
                        .with_metadata(msg.metadata);
                    Ok(crate::outcomes::ReceivedBatch {
                        messages: vec![canonical],
                        commit: Box::new(|_| Box::pin(async { Ok(()) })), // Auto-ack for now
                    })
                }
                Ok(None) => Err(ConsumerError::EndOfStream),
                Err(e) => Err(ConsumerError::Connection(e.into())),
            }
        } else {
            Err(ConsumerError::Connection(anyhow::anyhow!(
                "Stream not initialized"
            )))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct GrpcPublisher {
    client: Mutex<BridgeClient<Channel>>,
}

impl GrpcPublisher {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let client = BridgeClient::connect(config.url.clone()).await?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

#[async_trait]
impl MessagePublisher for GrpcPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut client = self.client.lock().await;
        for msg in messages {
            let req = BridgeMessage {
                payload: msg.payload.to_vec(),
                id: msg.message_id.to_string(),
                metadata: msg.metadata.into_iter().collect(),
            };
            let _ = client.publish(req).await.map_err(anyhow::Error::from)?;
        }
        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Endpoint, EndpointType, GrpcConfig, Route};
    use proto::bridge_server::{Bridge, BridgeServer};
    use proto::{BridgeMessage, PublishResponse, SubscribeRequest};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{transport::Server, Request, Response, Status};

    struct MockBridge {
        tx: mpsc::Sender<Result<BridgeMessage, Status>>,
        #[allow(dead_code)]
        rx: tokio::sync::Mutex<mpsc::Receiver<Result<BridgeMessage, Status>>>,
    }

    #[tonic::async_trait]
    impl Bridge for MockBridge {
        async fn publish(
            &self,
            request: Request<BridgeMessage>,
        ) -> Result<Response<PublishResponse>, Status> {
            let msg = request.into_inner();
            self.tx.send(Ok(msg)).await.unwrap();
            Ok(Response::new(PublishResponse {
                success: true,
                error: "".to_string(),
            }))
        }

        type SubscribeStream = ReceiverStream<Result<BridgeMessage, Status>>;

        async fn subscribe(
            &self,
            _request: Request<SubscribeRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            // For testing, we just return a stream that yields what we received via publish
            // In a real scenario, this would subscribe to a specific topic.
            // Here we hack it: we create a new channel for this subscription and forward from the main rx?
            // Or simpler: The test will just use Publish to verify the Publisher, and we can test Consumer separately.

            // Let's implement a simple echo for the consumer test:
            // The consumer connects, and we immediately send a message.
            let (tx, rx) = mpsc::channel(4);
            tx.send(Ok(BridgeMessage {
                payload: b"grpc_payload".to_vec(),
                id: "1".to_string(),
                metadata: Default::default(),
            }))
            .await
            .unwrap();

            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test]
    async fn test_grpc_publisher_and_consumer() {
        let addr = "[::1]:50051".parse().unwrap();
        let (tx, rx) = mpsc::channel(10);
        let bridge = MockBridge {
            tx,
            rx: tokio::sync::Mutex::new(rx),
        };

        // Start Server
        let server_handle = tokio::spawn(async move {
            Server::builder()
                .serve(addr, BridgeServer::new(bridge))
                .await
                .unwrap();
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let config = GrpcConfig {
            url: "http://[::1]:50051".to_string(),
            timeout_ms: None,
            topic: Some("test_topic".to_string()),
        };

        // 1. Test Publisher
        let publisher_ep = Endpoint {
            endpoint_type: EndpointType::Grpc(config.clone()),
            middlewares: vec![],
            handler: None,
        };
        let publisher = Route::new(Endpoint::new_memory("in", 10), publisher_ep)
            .create_publisher()
            .await
            .expect("Failed to create publisher");

        publisher
            .send("hello_grpc".into())
            .await
            .expect("Failed to send");

        // 2. Test Consumer
        let consumer_ep = Endpoint {
            endpoint_type: EndpointType::Grpc(config),
            middlewares: vec![],
            handler: None,
        };
        let mut consumer = consumer_ep.create_consumer("test_route").await.unwrap();
        let batch = consumer.receive_batch(1).await.unwrap();
        assert_eq!(batch.messages[0].get_payload_str(), "grpc_payload");

        server_handle.abort();
    }
}
