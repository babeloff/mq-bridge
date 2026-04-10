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
            _request: Request<proto::SubscribeRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            let mut rx = self.tx.subscribe();
            let (tx_stream, rx_stream) = mpsc::channel(10);

            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            if tx_stream.send(Ok(msg)).await.is_err() {
                                break;
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
    // Server-mode: start the route consumer which will spawn an embedded server,
    // and have the memory->grpc publisher send messages to it.
    // Try several ports with readiness probing to avoid TOCTOU races when selecting an ephemeral port.
    let mut last_err = None;
    for _ in 0..6 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0");
        let port = match listener {
            Ok(l) => l.local_addr().ok().map(|a| a.port()),
            Err(_) => None,
        };
        let port = match port {
            Some(p) => p,
            None => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                continue;
            }
        };

        let grpc_url = format!("http://127.0.0.1:{}", port);

        // Inject `server_mode: true` before substituting the URL placeholder.
        let config_yaml = CONFIG_YAML
            .replace(
                "input:\n      grpc:\n        url: \"{grpc_url}\"",
                "input:\n      grpc:\n        url: \"{grpc_url}\"\n        server_mode: true",
            )
            .replace("{grpc_url}", &grpc_url)
            .replace(
                "{out_capacity}",
                &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
            );

        // Deploy the consumer (embedded server) and wait for the server to accept connections.
        let deploy_res = tokio::time::timeout(std::time::Duration::from_secs(8), async {
            run_performance_pipeline_test("grpc", &config_yaml, PERF_TEST_MESSAGE_COUNT).await
        })
        .await;

        match deploy_res {
            Ok(()) => return,
            Err(e) => {
                // Timeout or other error: try another port
                last_err = Some(format!("attempt failed for port {}: {}", port, e));
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        }
    }

    panic!(
        "Failed to run gRPC server-mode performance test: {:?}",
        last_err
    );
}
