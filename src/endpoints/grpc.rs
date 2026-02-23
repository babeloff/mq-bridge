//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::models::GrpcConfig;
use crate::traits::{ConsumerError, MessageConsumer, MessagePublisher, PublisherError, SentBatch};
use crate::CanonicalMessage;
use anyhow::Result;
use async_trait::async_trait;
use std::any::Any;
use std::time::Duration;
use tonic::transport::Channel;
use uuid::Uuid;

pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("mqbridge");
}

use proto::bridge_client::BridgeClient;
use proto::{BridgeMessage, SubscribeRequest};
use tonic::Request;

const GRPC_BATCH_POLL_MS: u64 = 10;

pub struct GrpcConsumer {
    _client: BridgeClient<Channel>,
    stream: Option<tonic::Streaming<BridgeMessage>>,
}

impl GrpcConsumer {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let mut endpoint = tonic::transport::Endpoint::from_shared(config.url.clone())?;
        if config.tls.required {
            return Err(anyhow::anyhow!(
                "gRPC TLS support is not compiled in. Please enable tonic TLS features."
            ));
        }
        if let Some(timeout) = config.timeout_ms {
            endpoint = endpoint.connect_timeout(Duration::from_millis(timeout));
        }
        let channel = endpoint.connect().await?;
        let mut client = BridgeClient::new(channel);

        let topic = config
            .topic
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Eagerly subscribe to ensure the consumer is ready upon creation.
        let request = Request::new(SubscribeRequest { topic });
        let stream = if let Some(timeout) = config.timeout_ms {
            tokio::time::timeout(Duration::from_millis(timeout), client.subscribe(request))
                .await
                .map_err(|_| anyhow::anyhow!("gRPC subscribe timed out"))??
        } else {
            client.subscribe(request).await?
        }
        .into_inner();

        Ok(Self {
            _client: client,
            stream: Some(stream),
        })
    }
}

#[async_trait]
impl MessageConsumer for GrpcConsumer {
    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        let mut messages = Vec::with_capacity(max_messages);
        if let Some(stream) = self.stream.as_mut() {
            // The first message will block until it arrives. Subsequent messages will be
            // fetched with a short timeout to fill the batch without blocking indefinitely.
            loop {
                let msg_future = stream.message();
                let msg_result = if messages.is_empty() {
                    // Block for the first message
                    Ok(msg_future.await)
                } else {
                    // Poll for subsequent messages
                    tokio::time::timeout(Duration::from_millis(GRPC_BATCH_POLL_MS), msg_future)
                        .await
                };

                match msg_result {
                    Ok(Ok(Some(msg))) => {
                        let message_id = if msg.id.is_empty() {
                            None
                        } else if let Ok(uuid) = Uuid::parse_str(&msg.id) {
                            Some(uuid.as_u128())
                        } else if let Ok(n) =
                            u128::from_str_radix(msg.id.trim_start_matches("0x"), 16)
                        {
                            Some(n)
                        } else {
                            msg.id.parse::<u128>().ok()
                        };

                        let canonical = CanonicalMessage::new(msg.payload, message_id)
                            .with_metadata(msg.metadata);
                        messages.push(canonical);
                        if messages.len() >= max_messages {
                            break;
                        }
                    }
                    Ok(Ok(None)) => break, // Stream ended
                    Ok(Err(e)) => return Err(ConsumerError::Connection(e.into())),
                    Err(_) => break, // Timeout on subsequent message
                }
            }
        } else {
            // This case should now be impossible if `new` always initializes the stream.
            return Err(ConsumerError::Connection(anyhow::anyhow!(
                "gRPC stream not initialized. This is a bug."
            )));
        }

        if messages.is_empty() {
            Err(ConsumerError::EndOfStream)
        } else {
            Ok(crate::outcomes::ReceivedBatch {
                messages,
                commit: Box::new(|_| Box::pin(async { Ok(()) })), // Auto-ack for now
            })
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct GrpcPublisher {
    client: BridgeClient<Channel>,
    timeout: Option<Duration>,
}

impl GrpcPublisher {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let mut endpoint = tonic::transport::Endpoint::from_shared(config.url.clone())?;
        if config.tls.required {
            return Err(anyhow::anyhow!(
                "gRPC TLS support is not compiled in. Please enable tonic TLS features."
            ));
        }
        if let Some(timeout) = config.timeout_ms {
            endpoint = endpoint.connect_timeout(Duration::from_millis(timeout));
        }
        let client = BridgeClient::new(endpoint.connect().await?);
        Ok(Self {
            client,
            timeout: config.timeout_ms.map(Duration::from_millis),
        })
    }
}

#[async_trait]
impl MessagePublisher for GrpcPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut client = self.client.clone();
        let bridge_messages = messages.into_iter().map(|msg| BridgeMessage {
            payload: msg.payload.to_vec(),
            id: fast_uuid_v7::format_uuid(msg.message_id).to_string(),
            metadata: msg.metadata.into_iter().collect(),
        });

        let request_stream = tokio_stream::iter(bridge_messages);

        let response_fut = client.publish_batch(request_stream);
        let response = if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, response_fut)
                .await
                .map_err(|_| {
                    PublisherError::Retryable(anyhow::anyhow!("gRPC publish batch timed out"))
                })?
                .map_err(anyhow::Error::from)?
        } else {
            response_fut.await.map_err(anyhow::Error::from)?
        };

        let inner_response = response.into_inner();
        if inner_response.success {
            Ok(SentBatch::Ack)
        } else {
            Err(PublisherError::Retryable(anyhow::anyhow!(
                inner_response.error
            )))
        }
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
    use tokio::sync::{broadcast, mpsc};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{transport::Server, Request, Response, Status};

    struct MockBridge {
        tx: broadcast::Sender<BridgeMessage>,
    }

    #[tonic::async_trait]
    impl Bridge for MockBridge {
        async fn publish(
            &self,
            request: Request<BridgeMessage>,
        ) -> Result<Response<PublishResponse>, Status> {
            // The receiver can be dropped if no subscriber is active. We can ignore the error.
            let _ = self.tx.send(request.into_inner());
            Ok(Response::new(PublishResponse {
                success: true,
                error: "".to_string(),
            }))
        }

        async fn publish_batch(
            &self,
            request: Request<tonic::Streaming<BridgeMessage>>,
        ) -> Result<Response<PublishResponse>, Status> {
            let mut stream = request.into_inner();
            while let Some(msg_result) = stream.message().await? {
                // The receiver can be dropped if no subscriber is active. We can ignore the error.
                let _ = self.tx.send(msg_result);
            }
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
            let mut rx = self.tx.subscribe();
            let (tx_stream, rx_stream) = mpsc::channel(10);

            // Spawn a task to bridge broadcast::Receiver to mpsc::Sender for the tonic stream
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            if tx_stream.send(Ok(msg)).await.is_err() {
                                // Downstream consumer has disconnected, so we stop.
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // This means the consumer is slow, and we skipped some messages.
                            // In a real-world scenario, you might want to log this.
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            // The sender is gone, no more messages will come.
                            break;
                        }
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx_stream)))
        }
    }

    #[tokio::test]
    async fn test_grpc_publisher_and_consumer() {
        let addr = "[::1]:50051".parse().unwrap();
        let (tx, _) = broadcast::channel(16);
        let mut rx_for_pub_test = tx.subscribe();
        let bridge = MockBridge { tx: tx.clone() };

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
            url: format!("http://{}", addr),
            timeout_ms: None,
            topic: Some("test_topic".to_string()),
            tls: Default::default(),
            ..Default::default()
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

        let sent_payload = "hello_grpc";
        publisher
            .send(sent_payload.into())
            .await
            .expect("Failed to send");

        // Verify the mock server received the message from the publisher
        let received_msg = rx_for_pub_test.recv().await.unwrap();
        assert_eq!(received_msg.payload, sent_payload.as_bytes());

        // 2. Test Consumer
        let consumer_ep = Endpoint {
            endpoint_type: EndpointType::Grpc(config),
            middlewares: vec![],
            handler: None,
        };
        // Create the consumer first. This will establish the subscription inside `new()`.
        let mut consumer = consumer_ep.create_consumer("test_route").await.unwrap();

        // Now send messages for the consumer to receive.
        tx.send(BridgeMessage {
            payload: b"grpc_payload_1".to_vec(),
            id: "0190163d-8694-739b-aea5-966c26f8ad90".to_string(),
            metadata: Default::default(),
        })
        .unwrap();
        tx.send(BridgeMessage {
            payload: b"grpc_payload_2".to_vec(),
            id: "0190163d-8694-739b-aea5-966c26f8ad91".to_string(),
            metadata: Default::default(),
        })
        .unwrap();

        let batch = consumer.receive_batch(5).await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].get_payload_str(), "grpc_payload_1");
        assert_eq!(batch.messages[1].get_payload_str(), "grpc_payload_2");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_grpc_route_end_to_end() {
        // 1. Setup Mock Server on a unique port
        let addr = "[::1]:50052".parse().unwrap();
        let (tx, _) = broadcast::channel(32);
        let bridge = MockBridge { tx };

        let server_handle = tokio::spawn(async move {
            Server::builder()
                .serve(addr, BridgeServer::new(bridge))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 2. Setup Config and Endpoints
        let config = GrpcConfig {
            url: format!("http://{}", addr),
            timeout_ms: None,
            topic: Some("e2e_test_topic".to_string()),
            tls: Default::default(),
            ..Default::default()
        };

        // Source for sending messages into the system
        let mem_source_topic = format!("e2e_in_{}", fast_uuid_v7::gen_id_str());
        let mem_dest_topic = format!("e2e_out_{}", fast_uuid_v7::gen_id_str());
        let mem_source_ep = Endpoint::new_memory(&mem_source_topic, 10);
        let mem_source_publisher = mem_source_ep.create_publisher("mem_source").await.unwrap();

        // The gRPC endpoint that will publish messages to our mock server
        let grpc_publisher_ep = Endpoint {
            endpoint_type: EndpointType::Grpc(config.clone()),
            middlewares: vec![],
            handler: None,
        };

        // The gRPC endpoint that will consume messages from our mock server
        let grpc_consumer_ep = Endpoint {
            endpoint_type: EndpointType::Grpc(config),
            middlewares: vec![],
            handler: None,
        };

        // The final destination for messages
        let mem_dest_ep = Endpoint::new_memory(&mem_dest_topic, 10);
        let mut mem_dest_consumer = mem_dest_ep.create_consumer("test_route").await.unwrap();

        // 3. Setup and run routes using deploy()
        // Route 1: Memory -> gRPC (tests GrpcPublisher::send_batch)
        let route_to_grpc = Route::new(mem_source_ep, grpc_publisher_ep);
        let _controller_to_grpc = route_to_grpc.deploy("route_to_grpc").await.unwrap();

        // Route 2: gRPC -> Memory (tests GrpcConsumer::receive_batch)
        let route_from_grpc = Route::new(grpc_consumer_ep, mem_dest_ep);
        let _controller_from_grpc = route_from_grpc.deploy("route_from_grpc").await.unwrap();

        // 4. Execute test: Send a batch of messages into the first route
        let messages_to_send = vec![
            CanonicalMessage::new("e2e_payload_1".into(), None),
            CanonicalMessage::new("e2e_payload_2".into(), None),
        ];
        mem_source_publisher
            .send_batch(messages_to_send.clone())
            .await
            .unwrap();

        // 5. Verify: Receive the batch from the second route's destination
        let mut received_messages = Vec::new();
        while received_messages.len() < messages_to_send.len() {
            let batch = mem_dest_consumer.receive_batch(5).await.unwrap();
            received_messages.extend(batch.messages);
        }

        assert_eq!(received_messages.len(), messages_to_send.len());
        assert_eq!(
            received_messages[0].get_payload_str(),
            messages_to_send[0].get_payload_str()
        );
        assert_eq!(
            received_messages[1].get_payload_str(),
            messages_to_send[1].get_payload_str()
        );

        // 6. Teardown
        server_handle.abort();
    }
}
