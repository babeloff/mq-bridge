//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

#[cfg(feature = "reqwest")]
use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::HttpConfig;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessagePublisher, ReceivedBatch, Sent,
};
#[cfg(feature = "actix-web")]
use crate::traits::{CommitFunc, MessageDisposition};
#[cfg(feature = "reqwest")]
use crate::traits::{PublisherError, SentBatch};
use crate::CanonicalMessage;
#[cfg(feature = "actix-web")]
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
#[cfg(feature = "actix-web")]
use std::net::SocketAddr;
use tracing::{info, trace};
use uuid::Uuid;

#[cfg(feature = "actix-web")]
type HttpSourceMessage = (CanonicalMessage, CommitFunc);

/// A source that listens for incoming HTTP requests.
#[cfg(feature = "actix-web")]
pub struct HttpConsumer {
    request_rx: tokio::sync::mpsc::Receiver<HttpSourceMessage>,
    _shutdown_tx: tokio::sync::watch::Sender<()>,
    _server_handle: actix_web::dev::ServerHandle,
}

#[cfg(feature = "actix-web")]
#[derive(Clone)]
struct HttpConsumerState {
    tx: tokio::sync::mpsc::Sender<HttpSourceMessage>,
    message_id_header: String,
    request_timeout: std::time::Duration,
    fire_and_forget: bool,
}

#[cfg(feature = "actix-web")]
impl HttpConsumer {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        let buffer_size = config.internal_buffer_size.unwrap_or(100).max(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel::<HttpSourceMessage>(buffer_size);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(());

        let message_id_header = config
            .message_id_header
            .clone()
            .unwrap_or_else(|| "message-id".to_string());
        let request_timeout =
            std::time::Duration::from_millis(config.request_timeout_ms.unwrap_or(30000));
        let state = HttpConsumerState {
            tx: request_tx,
            message_id_header,
            request_timeout,
            fire_and_forget: config.fire_and_forget,
        };

        let listen_address = &config.url;

        let addr: SocketAddr = listen_address
            .parse()
            .with_context(|| format!("Invalid listen address: {}", listen_address))?;

        let tls_config = config.tls.clone();
        // Channel to signal when the server is ready
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

        let workers = config.workers.unwrap_or(0);
        let workers = if workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            workers
        };
        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(state.clone()))
                // actual request handle here:
                .default_service(web::to(handle_request))
        })
        .workers(workers)
        .disable_signals(); // We handle shutdown manually

        let server = if tls_config.is_tls_server_configured() {
            info!("Starting HTTPS source on {} with {} workers", addr, workers);
            let config = load_rustls_config(&tls_config)?;
            server.bind_rustls_0_23(addr, config)?
        } else {
            info!("Starting HTTP source on {} with {} workers", addr, workers);
            server.bind(addr)?
        };

        let server = server.run();
        let handle = server.handle();

        tokio::spawn(async move {
            // Signal that we are about to start serving
            let _ = ready_tx.send(());
            if let Err(e) = server.await {
                tracing::error!("HTTP server error: {}", e);
            }
        });

        // Spawn shutdown handler
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            let _ = shutdown_rx.changed().await;
            shutdown_handle.stop(true).await;
        });

        ready_rx.await?;
        Ok(Self {
            request_rx,
            _shutdown_tx: shutdown_tx,
            _server_handle: handle,
        })
    }
}

#[cfg(feature = "actix-web")]
fn load_rustls_config(
    tls_config: &crate::models::TlsConfig,
) -> anyhow::Result<rustls::ServerConfig> {
    let cert_file = tls_config
        .cert_file
        .as_ref()
        .ok_or_else(|| anyhow!("Missing cert_file"))?;
    let key_file = tls_config
        .key_file
        .as_ref()
        .ok_or_else(|| anyhow!("Missing key_file"))?;

    let cert_file = std::fs::File::open(cert_file)?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

    let key_file = std::fs::File::open(key_file)?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow!("No private key found"))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(config)
}

#[cfg(feature = "actix-web")]
#[async_trait]
impl MessageConsumer for HttpConsumer {
    async fn receive_batch(
        &mut self,
        _max_messages: usize,
    ) -> Result<ReceivedBatch, ConsumerError> {
        let (message, commit) = self
            .request_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("HTTP source channel closed"))?;

        Ok(ReceivedBatch {
            messages: vec![message],
            commit: crate::traits::into_batch_commit_func(commit),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(feature = "actix-web")]
#[tracing::instrument(level = "trace", skip_all, fields(http.method = %req.method(), http.uri = %req.uri()))]
async fn handle_request(
    state: web::Data<HttpConsumerState>,
    req: HttpRequest,
    body: web::Bytes,
) -> impl Responder {
    let mut message_id = None;
    if let Some(header_value) = req.headers().get(state.message_id_header.as_str()) {
        if let Ok(s) = header_value.to_str() {
            if let Ok(uuid) = Uuid::parse_str(s) {
                message_id = Some(uuid.as_u128());
            } else if let Ok(n) = u128::from_str_radix(s.trim_start_matches("0x"), 16) {
                message_id = Some(n);
            } else if let Ok(n) = s.parse::<u128>() {
                message_id = Some(n);
            }
        }
    }

    let payload = body.to_vec();

    let mut message = CanonicalMessage::new(payload, message_id);
    trace!(message_id = %format!("{:032x}", message.message_id), "Received HTTP request");
    let headers = req.headers();
    let mut metadata = HashMap::with_capacity(headers.len() + 5);
    
    metadata.extend([
        ("http_method".to_string(), req.method().to_string()),
        ("http_path".to_string(), req.path().to_string()),
        ("http_query".to_string(), req.query_string().to_string()),
        ("http_uri".to_string(), req.uri().to_string()),
    ]);

    metadata.extend(headers.iter().filter_map(|(key, value)| {
        value
            .to_str()
            .ok()
            .map(|v_str| (key.as_str().to_string(), v_str.to_string()))
    }));

    if let Some(peer) = req.peer_addr() {
        metadata.insert("http_peer_addr".to_string(), peer.to_string());
    }
    message.metadata = metadata;

    // Channel to receive the commit confirmation from the pipeline.
    // The HTTP response will be determined by the disposition received here.
    // Reply -> 200 OK with payload. Ack -> 202 Accepted. Nack -> 500 Internal Server Error.
    let fire_and_forget = state.fire_and_forget;
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<MessageDisposition>();
    let commit = Box::new(move |disposition: MessageDisposition| {
        Box::pin(async move {
            if ack_tx.send(disposition).is_err() && !fire_and_forget {
                trace!("HTTP handler was no longer waiting for commit disposition (client disconnected).");
            }
            Ok(())
        }) as BoxFuture<'static, anyhow::Result<()>>
    });

    if let Err(e) = state.tx.send((message, commit)).await {
        tracing::error!("Failed to send request to bridge: {}", e);
        return HttpResponse::InternalServerError().body("Failed to send request to bridge");
    }

    if state.fire_and_forget {
        return HttpResponse::Accepted().body("Message accepted for processing");
    }

    // Wait for pipeline to process the message
    let timeout_duration = state.request_timeout;
    match tokio::time::timeout(timeout_duration, async {
        match ack_rx.await {
            Ok(disposition) => {
                // Pipeline processed the message.
                make_response(disposition)
            }
            Err(_) => HttpResponse::InternalServerError().body("Pipeline closed"),
        }
    })
    .await
    {
        Ok(response) => response,
        Err(_) => HttpResponse::GatewayTimeout().body("Request timed out"),
    }
}

#[cfg(feature = "actix-web")]
fn make_response(disposition: MessageDisposition) -> HttpResponse {
    match disposition {
        MessageDisposition::Reply(mut msg) => {
            let status = msg
                .metadata
                .remove("http_status_code")
                .and_then(|s| s.parse::<u16>().ok())
                .and_then(|code| actix_web::http::StatusCode::from_u16(code).ok())
                .unwrap_or(actix_web::http::StatusCode::OK);

            let mut builder = HttpResponse::build(status);
            for (key, value) in &msg.metadata {
                builder.insert_header((key.as_str(), value.as_str()));
            }
            let has_content_type = msg
                .metadata
                .keys()
                .any(|k| k.eq_ignore_ascii_case("content-type"));
            if !has_content_type {
                if status == actix_web::http::StatusCode::OK {
                    builder.content_type("application/octet-stream");
                } else {
                    builder.content_type("text/plain; charset=UTF-8");
                }
            }
            builder.body(msg.payload)
        }
        MessageDisposition::Ack => HttpResponse::Accepted().body("Message processed"),
        MessageDisposition::Nack => {
            HttpResponse::InternalServerError().body("Message processing failed")
        }
    }
}

/// A sink that sends messages to an HTTP endpoint.
#[cfg(feature = "reqwest")]
#[derive(Clone)]
pub struct HttpPublisher {
    client: reqwest::Client,
    url: String,
    request_timeout: Option<std::time::Duration>,
    batch_concurrency: usize,
}

#[cfg(feature = "reqwest")]
impl HttpPublisher {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        let batch_concurrency = config.batch_concurrency.unwrap_or(20).max(1);
        let mut client_builder = reqwest::Client::builder()
            .http2_adaptive_window(true)
            .pool_max_idle_per_host(batch_concurrency);

        let keepalive = std::time::Duration::from_millis(config.tcp_keepalive_ms.unwrap_or(60000));
        let idle_timeout =
            std::time::Duration::from_millis(config.pool_idle_timeout_ms.unwrap_or(90000));
        client_builder = client_builder
            .tcp_keepalive(keepalive)
            .pool_idle_timeout(idle_timeout);

        if config.tls.required {
            client_builder = client_builder.https_only(true);
        }

        if config.tls.is_mtls_client_configured() {
            let cert_path = config.tls.cert_file.as_ref().unwrap();
            let key_path = config.tls.key_file.as_ref().unwrap();
            let cert = tokio::fs::read(cert_path).await?;
            let key = tokio::fs::read(key_path).await?;
            let identity = reqwest::Identity::from_pem(&[cert, key].concat())?;
            client_builder = client_builder.identity(identity);
        }

        let url = if config.url.to_lowercase().starts_with("http://")
            || config.url.to_lowercase().starts_with("https://")
        {
            config.url.clone()
        } else {
            let scheme = if config.tls.required { "https" } else { "http" };
            format!("{}://{}", scheme, config.url)
        };

        let request_timeout = config
            .request_timeout_ms
            .map(std::time::Duration::from_millis);

        Ok(Self {
            client: client_builder.build()?,
            url,
            request_timeout,
            batch_concurrency,
        })
    }
}

#[cfg(feature = "reqwest")]
#[async_trait]
impl MessagePublisher for HttpPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        trace!(
            message_id = %format!("{:032x}", message.message_id),
            url = %self.url,
            "Sending HTTP request"
        );
        let mut request_builder = self.client.post(&self.url);
        for (key, value) in &message.metadata {
            request_builder = request_builder.header(key, value);
        }
        if let Some(timeout) = self.request_timeout {
            request_builder = request_builder.timeout(timeout);
        }

        let response = request_builder
            .body(message.payload)
            .send()
            .await
            .with_context(|| format!("Failed to send HTTP request to {}", self.url))?;

        let response_status = response.status();
        let mut response_metadata = HashMap::with_capacity(response.headers().len());
        for (key, value) in response.headers() {
            if let Ok(value_str) = value.to_str() {
                response_metadata.insert(key.as_str().to_string(), value_str.to_string());
            }
        }

        let response_bytes = response
            .bytes()
            .await
            .context("Failed to read HTTP response body")?
            .to_vec();

        if !response_status.is_success() {
            let error = anyhow::anyhow!(
                "HTTP send request failed with status {}: {:?}",
                response_status,
                String::from_utf8_lossy(&response_bytes)
            );

            // 4xx errors are client-side and should not be retried.
            // 5xx server errors are potentially transient.
            if response_status.is_client_error() {
                return Err(PublisherError::NonRetryable(error));
            } else if response_status.is_server_error() {
                // 500, 502, 503, 504 are often transient and worth retrying (with backoff).
                match response_status.as_u16() {
                    500 | 502 | 503 | 504 => return Err(PublisherError::Retryable(error)),
                    _ => return Err(PublisherError::NonRetryable(error)),
                }
            }
            // For other non-success statuses (e.g., redirects), treat as non-retryable.
            return Err(PublisherError::NonRetryable(error));
        }

        // If a response sink is configured, wrap the response in a CanonicalMessage
        let mut response_message = CanonicalMessage::new(response_bytes, Some(message.message_id));
        response_message.metadata = response_metadata;
        Ok(Sent::Response(response_message))
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        use futures::StreamExt;

        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        trace!(
            count = messages.len(),
            url = %self.url,
            message_ids = ?LazyMessageIds(&messages),
            "Publishing batch of HTTP requests"
        );

        let send_futures = messages.into_iter().map(|message| {
            // Clone the message for the error case.
            let msg_for_err = message.clone();
            async move { self.send(message).await.map_err(|e| (msg_for_err, e)) }
        });

        // Limit concurrency to 20 to allow connection reuse within the batch
        let mut stream = futures::stream::iter(send_futures).buffered(self.batch_concurrency);

        let mut responses = Vec::new();
        let mut failed = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(Sent::Response(resp)) => responses.push(resp),
                Ok(Sent::Ack) => {}
                Err((msg, e)) => failed.push((msg, e)),
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
#[cfg(all(feature = "actix-web", feature = "reqwest"))]
mod tests {
    use super::*;
    use crate::endpoints::create_publisher_from_route;
    use crate::models::{Config, EndpointType};
    use crate::CanonicalMessage;
    use std::time::Duration;

    // Helper to find a free port
    fn get_free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    #[test]
    fn test_http_config_yaml() {
        let yaml = r#"
http_route:
  input:
    http:
      url: "127.0.0.1:8080"
  output:
    http:
      url: "http://localhost:9090"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse YAML");
        let route = config.get("http_route").expect("Route not found");

        match &route.input.endpoint_type {
            EndpointType::Http(cfg) => {
                assert_eq!(cfg.url, "127.0.0.1:8080".to_string());
            }
            _ => panic!("Expected HTTP input"),
        }

        match &route.output.endpoint_type {
            EndpointType::Http(cfg) => {
                assert_eq!(cfg.url, "http://localhost:9090".to_string());
            }
            _ => panic!("Expected HTTP output"),
        }
    }

    #[tokio::test]
    async fn test_http_consumer_publisher_integration() {
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        let config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };

        // Start Consumer (Server)
        let mut consumer = HttpConsumer::new(&config)
            .await
            .expect("Failed to create consumer");

        // Start Publisher (Client)
        let pub_config = HttpConfig {
            url: url.clone(),
            ..Default::default()
        };
        let publisher = HttpPublisher::new(&pub_config)
            .await
            .expect("Failed to create publisher");

        // Send message
        let msg_payload = b"test_payload".to_vec();
        let msg = CanonicalMessage::new(msg_payload.clone(), None);

        // Spawn a task to handle the receiving side
        let receive_task = tokio::spawn(async move {
            let received = consumer.receive().await.expect("Failed to receive");
            // Send a response back via commit
            let response_msg = CanonicalMessage::new(b"response_payload".to_vec(), None);
            let _ = (received.commit)(crate::traits::MessageDisposition::Reply(response_msg)).await;
            received.message
        });

        // Publisher sends
        let response = publisher.send(msg).await.expect("Failed to send");

        let received_msg = receive_task.await.expect("Receive task failed");
        assert_eq!(received_msg.payload, msg_payload);
        let response = match response {
            Sent::Response(msg) => msg,
            _ => panic!("Expected response"),
        };
        assert_eq!(response.payload, b"response_payload".to_vec());
    }

    #[tokio::test]
    async fn test_http_server_shutdown_on_drop() {
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };

        {
            let _consumer = HttpConsumer::new(&config)
                .await
                .expect("Failed to create consumer");
            // Verify we can connect while consumer is alive
            assert!(tokio::net::TcpStream::connect(&addr).await.is_ok());
        } // consumer is dropped here, triggering shutdown via _shutdown_tx drop

        // Wait for shutdown to propagate
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify connection is refused (server is down)
        assert!(tokio::net::TcpStream::connect(&addr).await.is_err());
    }

    #[tokio::test]
    async fn test_http_to_static_response() {
        // This test simulates a route: HTTP In -> Static Out.
        // It verifies that an HTTP request receives the static response.

        // 1. Setup an HttpConsumer (server)
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let http_config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };
        let mut consumer = HttpConsumer::new(&http_config).await.unwrap();

        // 2. Setup a StaticEndpointPublisher
        let static_content = "This is a static response";
        let static_publisher =
            crate::endpoints::static_endpoint::StaticEndpointPublisher::new(static_content)
                .unwrap();

        // 3. Emulate the route logic in a separate task
        tokio::spawn(async move {
            if let Ok(received) = consumer.receive().await {
                let static_response_outcome =
                    static_publisher.send(received.message).await.unwrap();
                let disposition = match static_response_outcome {
                    Sent::Response(msg) => crate::traits::MessageDisposition::Reply(msg),
                    Sent::Ack => crate::traits::MessageDisposition::Ack,
                };
                let _ = (received.commit)(disposition).await;
            }
        });

        // 4. Make a request to the server
        let client = reqwest::Client::new();
        let response = client
            .post(format!("http://{}", addr))
            .send()
            .await
            .unwrap();

        // 5. Assert the response from the server
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body = response.text().await.unwrap();
        let expected_body = serde_json::to_string(static_content).unwrap();
        assert_eq!(body, expected_body);
    }

    #[tokio::test]
    async fn test_http_to_response_endpoint() {
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let http_config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };
        let mut consumer = HttpConsumer::new(&http_config).await.unwrap();

        // Create ResponsePublisher via factory to simulate route config
        let response_endpoint =
            crate::models::Endpoint::new(EndpointType::Response(crate::models::ResponseConfig {}));
        let publisher = create_publisher_from_route("test_response", &response_endpoint)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Ok(received) = consumer.receive().await {
                let outcome = publisher.send(received.message).await.unwrap();
                let disposition = match outcome {
                    Sent::Response(msg) => crate::traits::MessageDisposition::Reply(msg),
                    Sent::Ack => crate::traits::MessageDisposition::Ack,
                };
                let _ = (received.commit)(disposition).await;
            }
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}", addr))
            .body("echo_test")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "echo_test");
    }

    #[tokio::test]
    async fn test_http_request_reply_with_handler() {
        use crate::traits::Handled;

        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let http_config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };
        let mut consumer = HttpConsumer::new(&http_config).await.unwrap();

        let mut response_endpoint =
            crate::models::Endpoint::new(EndpointType::Response(crate::models::ResponseConfig {}));

        let handler = |mut msg: CanonicalMessage| async move {
            let original = String::from_utf8_lossy(&msg.payload).to_string();
            msg.payload = format!("handled: {}", original).into();
            Ok(Handled::Publish(msg))
        };
        response_endpoint.handler = Some(std::sync::Arc::new(handler));

        let publisher = create_publisher_from_route("test_response_handler", &response_endpoint)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Ok(received) = consumer.receive().await {
                let outcome = publisher.send(received.message).await.unwrap();
                let disposition = match outcome {
                    Sent::Response(msg) => crate::traits::MessageDisposition::Reply(msg),
                    Sent::Ack => crate::traits::MessageDisposition::Ack,
                };
                let _ = (received.commit)(disposition).await;
            }
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}", addr))
            .body("input_data")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "handled: input_data");
    }

    #[tokio::test]
    async fn test_http_reply_with_custom_status_code() {
        use crate::traits::Handled;

        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let http_config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };
        let mut consumer = HttpConsumer::new(&http_config).await.unwrap();

        let mut response_endpoint =
            crate::models::Endpoint::new(EndpointType::Response(crate::models::ResponseConfig {}));

        let handler = |mut msg: CanonicalMessage| async move {
            msg.metadata
                .insert("http_status_code".to_string(), "201".to_string());
            Ok(Handled::Publish(msg))
        };
        response_endpoint.handler = Some(std::sync::Arc::new(handler));

        let publisher =
            create_publisher_from_route("test_response_handler_status", &response_endpoint)
                .await
                .unwrap();

        tokio::spawn(async move {
            if let Ok(received) = consumer.receive().await {
                let outcome = publisher.send(received.message).await.unwrap();
                let disposition = match outcome {
                    Sent::Response(msg) => crate::traits::MessageDisposition::Reply(msg),
                    Sent::Ack => crate::traits::MessageDisposition::Ack,
                };
                let _ = (received.commit)(disposition).await;
            }
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{}", addr))
            .body("input_data")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
        assert_eq!(resp.text().await.unwrap(), "input_data");
    }

    #[tokio::test]
    async fn test_http_publisher_server_error() {
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        // Start a simple server that always returns 500
        let server = actix_web::HttpServer::new(|| {
            actix_web::App::new().route(
                "/",
                actix_web::web::post()
                    .to(|| async { actix_web::HttpResponse::InternalServerError().body("error") }),
            )
        })
        .bind(&addr)
        .unwrap()
        .run();

        let server_handle = server.handle();
        tokio::spawn(server);

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let pub_config = HttpConfig {
            url: url.clone(),
            ..Default::default()
        };
        let publisher = HttpPublisher::new(&pub_config)
            .await
            .expect("Failed to create publisher");

        let msg = CanonicalMessage::new(b"test".to_vec(), None);
        let result = publisher.send(msg).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("500 Internal Server Error"));

        server_handle.stop(true).await;
    }

    #[tokio::test]
    async fn test_http_publisher_metadata_propagation() {
        use std::collections::HashMap;
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<HashMap<String, String>>(1);

        let server = actix_web::HttpServer::new(move || {
            let tx = tx.clone();
            actix_web::App::new().route(
                "/",
                actix_web::web::post().to(move |req: actix_web::HttpRequest| {
                    let tx = tx.clone();
                    async move {
                        let mut headers = HashMap::new();
                        for (k, v) in req.headers() {
                            if let Ok(s) = v.to_str() {
                                headers.insert(k.to_string(), s.to_string());
                            }
                        }
                        tx.send(headers).await.unwrap();
                        actix_web::HttpResponse::Ok().finish()
                    }
                }),
            )
        })
        .bind(&addr)
        .unwrap()
        .run();

        let server_handle = server.handle();
        tokio::spawn(server);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let pub_config = HttpConfig {
            url: url.clone(),
            ..Default::default()
        };
        let publisher = HttpPublisher::new(&pub_config)
            .await
            .expect("Failed to create publisher");

        let mut msg = CanonicalMessage::new(b"test".to_vec(), None);
        msg.metadata
            .insert("x-custom-header".to_string(), "custom-value".to_string());

        publisher.send(msg).await.expect("Failed to send");

        let received_headers = rx.recv().await.expect("Server didn't receive request");
        let found = received_headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("x-custom-header") && v == "custom-value");
        assert!(
            found,
            "Header x-custom-header not found or value mismatch. Headers: {:?}",
            received_headers
        );

        server_handle.stop(true).await;
    }

    #[tokio::test]
    async fn test_route_http_publisher_multiple_messages() {
        use crate::models::{Endpoint, Route};
        use std::sync::{Arc, Mutex};

        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        let received_count = Arc::new(Mutex::new(0));
        let received_payloads = Arc::new(Mutex::new(Vec::new()));
        let received_count_clone = received_count.clone();
        let received_payloads_clone = received_payloads.clone();

        let server = actix_web::HttpServer::new(move || {
            let count = received_count_clone.clone();
            let payloads = received_payloads_clone.clone();
            actix_web::App::new().route(
                "/",
                actix_web::web::post().to(move |body: actix_web::web::Bytes| {
                    let mut c = count.lock().unwrap();
                    *c += 1;
                    let mut p = payloads.lock().unwrap();
                    p.push(body.to_vec());
                    async { actix_web::HttpResponse::Ok().finish() }
                }),
            )
        })
        .bind(&addr)
        .unwrap()
        .run();

        let server_handle = server.handle();
        tokio::spawn(server);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let input = Endpoint::new_memory("http_multi_in", 100);
        let output = Endpoint::new(EndpointType::Http(HttpConfig {
            url: url.clone(),
            ..Default::default()
        }));

        let route = Route::new(input.clone(), output).with_batch_size(10);
        let handle = route.run("test_http_multi").await.unwrap();

        let input_channel = input.channel().unwrap();
        let mut messages = Vec::new();
        for i in 0..10 {
            messages.push(CanonicalMessage::new(
                format!("msg{}", i).into_bytes(),
                None,
            ));
        }

        // Send messages to the route input
        input_channel.fill_messages(messages).await.unwrap();

        // Wait for processing
        for _ in 0..50 {
            let count = *received_count.lock().unwrap();
            if count >= 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let count = *received_count.lock().unwrap();
        assert_eq!(count, 10);

        {
            let payloads = received_payloads.lock().unwrap();
            assert_eq!(payloads.len(), 10);
            for i in 0..10 {
                let expected = format!("msg{}", i).into_bytes();
                assert!(payloads.contains(&expected), "Missing payload: msg{}", i);
            }
        }

        handle.stop().await;
        server_handle.stop(true).await;
    }
}
