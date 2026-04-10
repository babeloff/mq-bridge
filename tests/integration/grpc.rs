#![allow(dead_code)]

use mq_bridge::test_utils::{
    run_performance_pipeline_test, setup_logging, PERF_TEST_MESSAGE_COUNT,
};

const CONFIG_YAML: &str = r#"
routes:
  memory_to_grpc:
    concurrency: 4
    batch_size: 128
    input:
      memory: { topic: "test-in-grpc" }
    output:
      grpc:
        url: "{grpc_url}"

  grpc_to_memory:
    concurrency: 4
    batch_size: 128
    input:
      grpc:
        url: "{grpc_url}"
    output:
      memory: { topic: "test-out-grpc", capacity: {out_capacity} }
"#;

pub async fn test_grpc_performance_pipeline() {
    setup_logging();

    // Run client-mode (external mock server) first, then server-mode (embedded server).
    test_grpc_client_mode().await;
    test_grpc_server_mode().await;
}

async fn test_grpc_client_mode() {
    // Bind to an ephemeral port with tokio so we can pass the listener to the server
    // and avoid the bind-drop TOCTOU race.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let grpc_url = format!("http://{}", addr);

    // Use the same proto that endpoints/grpc.rs exposes
    use mq_bridge::endpoints::grpc::proto;
    use proto::bridge_server::{Bridge, BridgeServer};
    use proto::{BridgeMessage, PublishResponse};
    use tokio::sync::{broadcast, mpsc};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    struct MockBridge {
        tx: broadcast::Sender<BridgeMessage>,
    }

    #[tonic::async_trait]
    impl Bridge for MockBridge {
        async fn publish(
            &self,
            request: Request<BridgeMessage>,
        ) -> Result<Response<PublishResponse>, Status> {
            let msg = request.into_inner();
            let msg_id = msg.id.clone();
            let _ = self.tx.send(msg);
            Ok(Response::new(PublishResponse {
                result: Some(proto::publish_response::Result::Ack(proto::Ack {
                    id: msg_id,
                    status: 0,
                    reason: String::new(),
                    metadata: Default::default(),
                })),
            }))
        }

        async fn acknowledge(
            &self,
            request: Request<proto::Ack>,
        ) -> Result<Response<proto::AckResponse>, Status> {
            let _ = request.into_inner();
            Ok(Response::new(proto::AckResponse {
                success: true,
                error: String::new(),
            }))
        }

        type PublishBatchStream = ReceiverStream<Result<PublishResponse, Status>>;

        async fn publish_batch(
            &self,
            request: Request<tonic::Streaming<BridgeMessage>>,
        ) -> Result<Response<Self::PublishBatchStream>, Status> {
            let mut stream = request.into_inner();
            let (tx, rx) = mpsc::channel(32);
            let sender = self.tx.clone();

            tokio::spawn(async move {
                while let Ok(Some(msg)) = stream.message().await {
                    let msg_id = msg.id.clone();
                    let _ = sender.send(msg);
                    let resp = PublishResponse {
                        result: Some(proto::publish_response::Result::Ack(proto::Ack {
                            id: msg_id,
                            status: 0,
                            reason: String::new(),
                            metadata: Default::default(),
                        })),
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        }

        type SubscribeStream = ReceiverStream<Result<BridgeMessage, Status>>;

        async fn subscribe(
            &self,
            request: Request<proto::SubscribeRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let req = request.into_inner();
            let topic_filter = req.topic;
            let mut rx = self.tx.subscribe();
            let (tx_stream, rx_stream) = mpsc::channel(10);

            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            // Only forward messages that match the requested topic.
                            let msg_topic =
                                msg.metadata.get("topic").map(|s| s.as_str()).unwrap_or("");
                            if msg_topic == topic_filter {
                                if tx_stream.send(Ok(msg)).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx_stream)))
        }
    }

    // Start the mock server using a TcpListenerStream so we own the bound socket.
    use tokio_stream::wrappers::TcpListenerStream;
    let (tx, _rx) = broadcast::channel(PERF_TEST_MESSAGE_COUNT + 1000);
    let bridge = MockBridge { tx };
    let incoming = TcpListenerStream::new(listener);
    let _server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .serve_with_incoming(BridgeServer::new(bridge), incoming)
            .await
            .unwrap();
    });

    let config_yaml = CONFIG_YAML.replace("{grpc_url}", &grpc_url).replace(
        "{out_capacity}",
        &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
    );

    run_performance_pipeline_test("grpc", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
}

async fn test_grpc_server_mode() {
    // Server-mode style test: bind an ephemeral port and start a mock server
    // on that listener, then run the pipeline with a client-mode consumer
    // connecting to this server. This reserves the port and avoids TOCTOU.
    use mq_bridge::endpoints::grpc::proto;
    use proto::bridge_server::{Bridge, BridgeServer};
    use proto::{BridgeMessage, PublishResponse};
    use tokio::sync::{broadcast, mpsc};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    struct MockBridge {
        tx: broadcast::Sender<BridgeMessage>,
    }

    #[tonic::async_trait]
    impl Bridge for MockBridge {
        async fn publish(
            &self,
            request: Request<BridgeMessage>,
        ) -> Result<Response<PublishResponse>, Status> {
            let msg = request.into_inner();
            let msg_id = msg.id.clone();
            let _ = self.tx.send(msg);
            Ok(Response::new(PublishResponse {
                result: Some(proto::publish_response::Result::Ack(proto::Ack {
                    id: msg_id,
                    status: 0,
                    reason: String::new(),
                    metadata: Default::default(),
                })),
            }))
        }

        async fn acknowledge(
            &self,
            request: Request<proto::Ack>,
        ) -> Result<Response<proto::AckResponse>, Status> {
            let _ = request.into_inner();
            Ok(Response::new(proto::AckResponse {
                success: true,
                error: String::new(),
            }))
        }

        type PublishBatchStream = ReceiverStream<Result<PublishResponse, Status>>;

        async fn publish_batch(
            &self,
            request: Request<tonic::Streaming<BridgeMessage>>,
        ) -> Result<Response<Self::PublishBatchStream>, Status> {
            let mut stream = request.into_inner();
            let (tx, rx) = mpsc::channel(32);
            let sender = self.tx.clone();

            tokio::spawn(async move {
                while let Ok(Some(msg)) = stream.message().await {
                    let msg_id = msg.id.clone();
                    let _ = sender.send(msg);
                    let resp = PublishResponse {
                        result: Some(proto::publish_response::Result::Ack(proto::Ack {
                            id: msg_id,
                            status: 0,
                            reason: String::new(),
                            metadata: Default::default(),
                        })),
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        }

        type SubscribeStream = ReceiverStream<Result<BridgeMessage, Status>>;

        async fn subscribe(
            &self,
            request: Request<proto::SubscribeRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let req = request.into_inner();
            let topic_filter = req.topic;
            let mut rx = self.tx.subscribe();
            let (tx_stream, rx_stream) = mpsc::channel(10);

            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            let msg_topic =
                                msg.metadata.get("topic").map(|s| s.as_str()).unwrap_or("");
                            if msg_topic == topic_filter {
                                if tx_stream.send(Ok(msg)).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx_stream)))
        }
    }

    // Bind an ephemeral port and start the mock server using the owned listener.
    use tokio_stream::wrappers::TcpListenerStream;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let grpc_url = format!("http://{}", addr);

    let (tx, _rx) = broadcast::channel(PERF_TEST_MESSAGE_COUNT + 1000);
    let bridge = MockBridge { tx };
    let incoming = TcpListenerStream::new(listener);
    let _server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .serve_with_incoming(BridgeServer::new(bridge), incoming)
            .await
            .unwrap();
    });

    let config_yaml = CONFIG_YAML.replace("{grpc_url}", &grpc_url).replace(
        "{out_capacity}",
        &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
    );

    run_performance_pipeline_test("grpc", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
}
