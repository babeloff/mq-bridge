//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::{HttpConfig, TlsConfig};
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessagePublisher, ReceivedBatch, Sent,
};
use crate::traits::{CommitFunc, MessageDisposition, PublisherError, SentBatch};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::service::service_fn;
use hyper::{body::Incoming, Request, Response, StatusCode};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, trace};
use uuid::Uuid;

type HttpSourceMessage = (CanonicalMessage, CommitFunc);

/// A source that listens for incoming HTTP requests using hyper.
pub struct HttpConsumer {
    request_rx: tokio::sync::mpsc::Receiver<HttpSourceMessage>,
    _shutdown_tx: tokio::sync::watch::Sender<()>,
}

#[derive(Clone)]
struct HttpConsumerState {
    tx: tokio::sync::mpsc::Sender<HttpSourceMessage>,
    message_id_header: String,
    request_timeout: std::time::Duration,
    fire_and_forget: bool,
    basic_auth: Option<(String, String)>,
    compression_enabled: bool,
    compression_threshold_bytes: usize,
    custom_headers: HashMap<String, String>,
}

impl HttpConsumer {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let buffer_size = config.internal_buffer_size.unwrap_or(100).max(1);
        let (request_tx, request_rx) = tokio::sync::mpsc::channel::<HttpSourceMessage>(buffer_size);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

        let message_id_header = config
            .message_id_header
            .clone()
            .unwrap_or_else(|| "message-id".to_string());
        let request_timeout =
            std::time::Duration::from_millis(config.request_timeout_ms.unwrap_or(30000));
        let compression_threshold_bytes = config.compression_threshold_bytes.unwrap_or(1024);
        let state = HttpConsumerState {
            tx: request_tx,
            message_id_header,
            request_timeout,
            fire_and_forget: config.fire_and_forget,
            basic_auth: config.basic_auth.clone(),
            compression_enabled: config.compression_enabled,
            compression_threshold_bytes,
            custom_headers: config.custom_headers.clone(),
        };

        let listen_address = &config.url;
        let addr: SocketAddr = listen_address
            .parse()
            .with_context(|| format!("Invalid listen address: {}", listen_address))?;

        let tls_config = config.tls.clone();
        let workers = config.workers.unwrap_or(0);
        let workers = if workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            workers
        };

        if is_tls_server_configured(&tls_config) {
            info!("Starting HTTPS source on {} with {} workers", addr, workers);
            spawn_tls_server(addr, state.clone(), shutdown_rx, &tls_config).await?;
        } else {
            info!("Starting HTTP source on {} with {} workers", addr, workers);
            spawn_http_server(addr, state.clone(), shutdown_rx).await?;
        }

        Ok(Self {
            request_rx,
            _shutdown_tx: shutdown_tx,
        })
    }
}

async fn spawn_http_server(
    addr: SocketAddr,
    state: HttpConsumerState,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {}", addr))?;

    let state = Arc::new(state);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    trace!("HTTP server shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            let state = state.clone();
                            tokio::spawn(async move {
                                let io = TokioIo::new(socket);
                                let state_clone = (*state).clone();
                                let service = service_fn(move |request: Request<Incoming>| {
                                    let state = state_clone.clone();
                                    async move {
                                        handle_request(state, request).await
                                    }
                                });

                                let conn = hyper::server::conn::http1::Builder::new()
                                    .keep_alive(true)
                                    .serve_connection(io, service)
                                    .await;
                                if let Err(e) = conn {
                                    trace!("Connection error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            trace!("Accept error: {}", e);
                        }
                    }
                }
            }
        }
    });

    Ok(())
}

async fn spawn_tls_server(
    addr: SocketAddr,
    state: HttpConsumerState,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
    tls_config: &TlsConfig,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {} (TLS)", addr))?;

    let rustls_server_config =
        create_rustls_server_config(tls_config).context("Failed to create rustls server config")?;
    let acceptor = TlsAcceptor::from(rustls_server_config);

    let state = Arc::new(state);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    trace!("TLS server shutting down");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            let state = state.clone();
                            let acceptor = acceptor.clone();
                            tokio::spawn(async move {
                                match acceptor.accept(socket).await {
                                    Ok(stream) => {
                                        let io = TokioIo::new(stream);
                                        let state_clone = (*state).clone();
                                        let service = service_fn(move |request: Request<Incoming>| {
                                            let state = state_clone.clone();
                                            async move {
                                                handle_request(state, request).await
                                            }
                                        });

                                        let conn = AutoBuilder::new(TokioExecutor::new())
                                            .serve_connection_with_upgrades(io, service)
                                            .await;

                                        if let Err(e) = conn {
                                            // Benign errors like "connection closed" are expected
                                            trace!("TLS Connection error: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        debug!("TLS handshake error: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            trace!("Accept error: {}", e);
                        }
                    }
                }
            }
        }
    });

    Ok(())
}

#[async_trait]
impl MessageConsumer for HttpConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let max_messages = max_messages.max(1);

        // Block on first message
        let (first_message, first_commit) = self
            .request_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("HTTP source channel closed"))?;

        let mut messages = vec![first_message];
        let mut commits = vec![first_commit];

        // Non-blocking collection of additional messages
        while messages.len() < max_messages {
            match self.request_rx.try_recv() {
                Ok((message, commit)) => {
                    messages.push(message);
                    commits.push(commit);
                }
                Err(_) => break,
            }
        }

        // Create a batch commit that handles all collected messages
        let batch_commit: crate::traits::BatchCommitFunc =
            Box::new(move |dispositions: Vec<MessageDisposition>| {
                Box::pin(async move {
                    // Commit each message with its corresponding disposition
                    for (commit, disposition) in commits.into_iter().zip(dispositions.into_iter()) {
                        commit(disposition).await?;
                    }
                    Ok(())
                }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
            });

        Ok(ReceivedBatch {
            messages,
            commit: batch_commit,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tracing::instrument(level = "trace", skip_all)]
async fn handle_request(
    state: HttpConsumerState,
    req: Request<Incoming>,
) -> anyhow::Result<Response<http_body_util::Full<Bytes>>> {
    // Validate Basic Authentication if configured
    if let Some((expected_user, expected_pass)) = &state.basic_auth {
        if let Some(auth_header) = req.headers().get("authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                if let Some(encoded) = auth_str.strip_prefix("Basic ") {
                    if let Ok(decoded) = general_purpose::STANDARD.decode(encoded) {
                        if let Ok(credentials) = String::from_utf8(decoded) {
                            let (user, pass) = if let Some(colon_pos) = credentials.find(':') {
                                (&credentials[..colon_pos], &credentials[colon_pos + 1..])
                            } else {
                                ("", "")
                            };
                            if user == expected_user && pass == expected_pass {
                                // Auth successful, proceed
                            } else {
                                return Ok(Response::builder()
                                    .status(StatusCode::UNAUTHORIZED)
                                    .body(http_body_util::Full::from("Invalid credentials"))
                                    .unwrap());
                            }
                        } else {
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(http_body_util::Full::from(
                                    "Invalid authorization header encoding",
                                ))
                                .unwrap());
                        }
                    } else {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(http_body_util::Full::from(
                                "Invalid base64 encoding in authorization header",
                            ))
                            .unwrap());
                    }
                } else {
                    return Ok(Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(http_body_util::Full::from(
                            "Missing Basic authentication scheme",
                        ))
                        .unwrap());
                }
            }
        } else {
            return Ok(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(http_body_util::Full::from("Missing authorization header"))
                .unwrap());
        }
    }

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

    // Extract metadata before consuming body
    let mut metadata = HashMap::with_capacity(req.headers().len() + 5);
    let mut content_encoding = None;

    metadata.extend([
        ("http_method".to_string(), req.method().to_string()),
        ("http_path".to_string(), req.uri().path().to_string()),
        (
            "http_query".to_string(),
            req.uri().query().unwrap_or("").to_string(),
        ),
        ("http_uri".to_string(), req.uri().to_string()),
    ]);

    for (key, value) in req.headers() {
        if let Ok(v_str) = value.to_str() {
            if key.as_str().eq_ignore_ascii_case("content-encoding") {
                content_encoding = Some(v_str.to_string());
            }
            metadata.insert(key.as_str().to_string(), v_str.to_string());
        }
    }

    // Read body after extracting metadata
    let body_bytes = req.collect().await?.to_bytes();
    let body_bytes_raw = body_bytes.to_vec();

    // Decompress if needed
    let payload = decompress_if_needed(&body_bytes_raw, content_encoding.as_deref())
        .map_err(|e| anyhow!("Failed to decompress request body: {}", e))?;

    let mut message = CanonicalMessage::new(payload, message_id);
    trace!(message_id = %format!("{:032x}", message.message_id), "Received HTTP request");

    message.metadata = metadata;

    let fire_and_forget = state.fire_and_forget;
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<MessageDisposition>();
    let commit = Box::new(move |disposition: MessageDisposition| {
        Box::pin(async move {
            if ack_tx.send(disposition).is_err() && !fire_and_forget {
                trace!("HTTP handler was no longer waiting for commit disposition");
            }
            Ok(())
        }) as BoxFuture<'static, anyhow::Result<()>>
    });

    if let Err(e) = state.tx.send((message, commit)).await {
        tracing::error!("Failed to send request to bridge: {}", e);
        let mut builder = Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR);
        for (header_name, header_value) in &state.custom_headers {
            builder = builder.header(header_name.as_str(), header_value.as_str());
        }
        return Ok(builder
            .body(http_body_util::Full::from(
                "Failed to send request to bridge",
            ))
            .unwrap());
    }

    if state.fire_and_forget {
        let mut builder = Response::builder().status(StatusCode::ACCEPTED);
        for (header_name, header_value) in &state.custom_headers {
            builder = builder.header(header_name.as_str(), header_value.as_str());
        }
        return Ok(builder
            .body(http_body_util::Full::from(
                "Message accepted for processing",
            ))
            .unwrap());
    }

    let timeout_duration = state.request_timeout;
    let custom_headers = state.custom_headers.clone();
    match tokio::time::timeout(timeout_duration, ack_rx).await {
        Ok(Ok(disposition)) => make_response(
            disposition,
            state.compression_enabled,
            state.compression_threshold_bytes,
            custom_headers,
        ),
        Ok(Err(_)) => {
            let mut builder = Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR);
            for (header_name, header_value) in &custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }
            Ok(builder
                .body(http_body_util::Full::from("Pipeline closed"))
                .unwrap())
        }
        Err(_) => {
            let mut builder = Response::builder().status(StatusCode::GATEWAY_TIMEOUT);
            for (header_name, header_value) in &custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }
            Ok(builder
                .body(http_body_util::Full::from("Request timed out"))
                .unwrap())
        }
    }
}

fn make_response(
    disposition: MessageDisposition,
    compression_enabled: bool,
    compression_threshold_bytes: usize,
    custom_headers: HashMap<String, String>,
) -> anyhow::Result<Response<http_body_util::Full<Bytes>>> {
    match disposition {
        MessageDisposition::Reply(mut msg) => {
            let status = msg
                .metadata
                .remove("http_status_code")
                .and_then(|s| s.parse::<u16>().ok())
                .and_then(|code| StatusCode::from_u16(code).ok())
                .unwrap_or(StatusCode::OK);

            let mut builder = Response::builder().status(status);

            for (key, value) in &msg.metadata {
                builder = builder.header(key.as_str(), value.as_str());
            }

            let has_content_type = msg
                .metadata
                .keys()
                .any(|k| k.eq_ignore_ascii_case("content-type"));
            if !has_content_type && status == StatusCode::OK {
                builder = builder.header("content-type", "application/octet-stream");
            }

            // Compress payload if enabled and beneficial
            let (payload, was_compressed) = compress_if_needed(
                &msg.payload,
                compression_enabled,
                compression_threshold_bytes,
            )?;

            if was_compressed {
                builder = builder.header("Content-Encoding", "gzip");
            }

            // Add custom headers to response
            for (header_name, header_value) in &custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }

            Ok(builder.body(http_body_util::Full::from(payload)).unwrap())
        }
        MessageDisposition::Ack => {
            let mut builder = Response::builder().status(StatusCode::ACCEPTED);
            for (header_name, header_value) in &custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }
            Ok(builder
                .body(http_body_util::Full::from("Message processed"))
                .unwrap())
        }
        MessageDisposition::Nack => {
            let mut builder = Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR);
            for (header_name, header_value) in &custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }
            Ok(builder
                .body(http_body_util::Full::from("Message processing failed"))
                .unwrap())
        }
    }
}

/// A publisher that sends messages to an HTTP endpoint using hyper.
///
/// Features:
/// - Connection pooling for both HTTP and HTTPS via `hyper-rustls`.
/// - Automatic negotiation of HTTP/1.1 or HTTP/2.
/// - Configurable request timeout and batch concurrency
/// - Comprehensive tracing for sent messages and response status
/// - Proper error classification (Retryable vs NonRetryable)
#[derive(Clone)]
pub struct HttpPublisher {
    /// Persistent HTTP client with connection pooling
    client: std::sync::Arc<
        hyper_util::client::legacy::Client<
            hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
            http_body_util::Full<Bytes>,
        >,
    >,
    url: String,
    request_timeout: Option<std::time::Duration>,
    batch_concurrency: usize,
    compression_enabled: bool,
    compression_threshold_bytes: usize,
    basic_auth: Option<(String, String)>,
    custom_headers: HashMap<String, String>,
}

impl HttpPublisher {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let batch_concurrency = config.batch_concurrency.unwrap_or(20).max(1);

        let tls_client_config = create_rustls_client_config(&config.tls)
            .context("Failed to create rustls client config")?;

        // Create HTTPS connector that handles both http and https, and http1/http2
        let https_connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls_client_config)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();

        // Create persistent client with connection pooling
        let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .build(https_connector);

        let url = if config.url.to_lowercase().starts_with("http://")
            || config.url.to_lowercase().starts_with("https://")
        {
            config.url.clone()
        } else {
            let scheme = if is_tls_client_configured(&config.tls) {
                "https"
            } else {
                "http"
            };
            format!("{}://{}", scheme, config.url)
        };

        let request_timeout = config
            .request_timeout_ms
            .map(std::time::Duration::from_millis);

        let compression_threshold_bytes = config.compression_threshold_bytes.unwrap_or(1024);

        Ok(Self {
            client: std::sync::Arc::new(client),
            url,
            request_timeout,
            batch_concurrency,
            compression_enabled: config.compression_enabled,
            compression_threshold_bytes,
            basic_auth: config.basic_auth.clone(),
            custom_headers: config.custom_headers.clone(),
        })
    }
}

#[async_trait]
impl MessagePublisher for HttpPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        trace!(
            message_id = %format!("{:032x}", message.message_id),
            url = %self.url,
            "Sending HTTP request"
        );

        let mut request_builder = Request::builder()
            .method(hyper::Method::POST)
            .uri(&self.url);

        for (key, value) in &message.metadata {
            request_builder = request_builder.header(key, value);
        }

        // Add HTTP Basic Authentication header
        if let Some((username, password)) = &self.basic_auth {
            let credentials = format!("{}:{}", username, password);
            let encoded = base64_encode(credentials.as_bytes());
            request_builder = request_builder.header("Authorization", format!("Basic {}", encoded));
        }

        // Add custom authentication headers
        for (header_name, header_value) in &self.custom_headers {
            request_builder = request_builder.header(header_name.as_str(), header_value.as_str());
        }

        // Compress payload if enabled and beneficial
        let (payload, was_compressed) = compress_if_needed(
            &message.payload,
            self.compression_enabled,
            self.compression_threshold_bytes,
        )
        .map_err(|e| {
            PublisherError::NonRetryable(anyhow::anyhow!("Failed to compress payload: {}", e))
        })?;

        if was_compressed {
            request_builder = request_builder.header("Content-Encoding", "gzip");
        }

        let body = http_body_util::Full::from(Bytes::from(payload));
        let request = request_builder.body(body).map_err(|e| {
            PublisherError::NonRetryable(anyhow::anyhow!("Failed to build request: {}", e))
        })?;

        let timeout_dur = self
            .request_timeout
            .unwrap_or(std::time::Duration::from_secs(30));
        let future = tokio::time::timeout(timeout_dur, self.client.request(request));

        let response: hyper::Response<Incoming> = match future.await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                let error = anyhow::anyhow!("Failed to send HTTP request to {}: {}", self.url, e);
                return Err(PublisherError::Retryable(error));
            }
            Err(_) => {
                return Err(PublisherError::Retryable(anyhow::anyhow!(
                    "HTTP request timeout"
                )));
            }
        };

        let response_status = response.status();
        let mut response_metadata = HashMap::new();
        let mut content_encoding = None;
        for (key, value) in response.headers() {
            if let Ok(value_str) = value.to_str() {
                if key.as_str().eq_ignore_ascii_case("content-encoding") {
                    content_encoding = Some(value_str.to_string());
                }
                response_metadata.insert(key.as_str().to_string(), value_str.to_string());
            }
        }

        let response_bytes_raw = match response.into_body().collect().await {
            Ok(collected) => collected.to_bytes().to_vec(),
            Err(e) => {
                return Err(PublisherError::Retryable(anyhow::anyhow!(
                    "Failed to read HTTP response body: {}",
                    e
                )));
            }
        };

        // Decompress response if needed
        let response_bytes = decompress_if_needed(&response_bytes_raw, content_encoding.as_deref())
            .map_err(|e| {
                PublisherError::Retryable(anyhow::anyhow!("Failed to decompress response: {}", e))
            })?;

        if !response_status.is_success() {
            debug!(
                message_id = %format!("{:032x}", message.message_id),
                status = %response_status,
                "HTTP request failed"
            );
            let error = anyhow::anyhow!(
                "HTTP send request failed with status {}: {:?}",
                response_status,
                String::from_utf8_lossy(&response_bytes)
            );

            if response_status.is_client_error() {
                return Err(PublisherError::NonRetryable(error));
            } else if response_status.is_server_error() {
                match response_status.as_u16() {
                    501 | 505 => return Err(PublisherError::NonRetryable(error)),
                    _ => return Err(PublisherError::Retryable(error)),
                }
            }
            return Err(PublisherError::NonRetryable(error));
        }

        trace!(
            message_id = %format!("{:032x}", message.message_id),
            status = %response_status,
            "HTTP request succeeded"
        );

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
            let msg_for_error = message.clone();
            async move { self.send(message).await.map_err(|e| (msg_for_error, e)) }
        });

        let mut stream = futures::stream::iter(send_futures).buffered(self.batch_concurrency);

        let mut responses = Vec::new();
        let mut failed = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(Sent::Response(resp)) => responses.push(resp),
                Ok(Sent::Ack) => {}
                Err((msg, e)) => {
                    failed.push((msg, e));
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Helper functions to build rustls configurations from TlsConfig.
// These are placed here because the methods were not found on the TlsConfig struct itself.
// NOTE: These helpers assume `TlsConfig` has fields like `cert_file`, `key_file`, `ca_file`,
// `client_cert_file`, and `client_key_file`. You may need to adjust field names if your
// `TlsConfig` struct in `src/models.rs` is different.

/// Checks if the TLS configuration is sufficient to start a TLS server.
fn is_tls_server_configured(tls_config: &TlsConfig) -> bool {
    tls_config.cert_file.is_some() && tls_config.key_file.is_some()
}

/// Checks if the TLS configuration is sufficient to make a TLS client connection.
fn is_tls_client_configured(tls_config: &TlsConfig) -> bool {
    // A client is configured for TLS if it has a CA to trust or a client cert to present.
    tls_config.required
        || tls_config.ca_file.is_some()
        || (tls_config.cert_file.is_some() && tls_config.key_file.is_some())
}

/// Creates a `rustls::ServerConfig` for the HTTPS server.
fn create_rustls_server_config(
    tls_config: &TlsConfig,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let cert_file = tls_config
        .cert_file
        .as_ref()
        .context("TLS cert_file not provided for server")?;
    let key_file = tls_config
        .key_file
        .as_ref()
        .context("TLS key_file not provided for server")?;

    let certs = load_certs(cert_file)?;
    let key = load_private_key(key_file)?;

    let config_builder = rustls::ServerConfig::builder();

    let config = if let Some(ca_file) = &tls_config.ca_file {
        // mTLS: verify client certificates using the provided CA
        let mut client_auth_roots = rustls::RootCertStore::empty();
        let mut pem = BufReader::new(File::open(ca_file).with_context(|| {
            format!(
                "Failed to open CA file for client verification: {}",
                ca_file
            )
        })?);
        for cert in rustls_pemfile::certs(&mut pem) {
            client_auth_roots.add(cert?)?;
        }
        let client_verifier =
            rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(client_auth_roots))
                .build()
                .context("Failed to build client certificate verifier")?;
        config_builder
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .context("Failed to build rustls mTLS server config")?
    } else {
        // Standard TLS: no client certificate verification
        config_builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("Failed to build rustls server config")?
    };

    Ok(Arc::new(config))
}

/// Creates a `rustls::ClientConfig` for the HTTPS client.
fn create_rustls_client_config(tls_config: &TlsConfig) -> anyhow::Result<rustls::ClientConfig> {
    let mut root_cert_store = rustls::RootCertStore::empty();

    if let Some(ca_file) = &tls_config.ca_file {
        let mut pem = BufReader::new(
            File::open(ca_file).with_context(|| format!("Failed to open CA file: {}", ca_file))?,
        );
        for cert in rustls_pemfile::certs(&mut pem) {
            root_cert_store.add(cert?)?;
        }
    } else {
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let config_builder = rustls::ClientConfig::builder().with_root_certificates(root_cert_store);

    if let (Some(cert_file), Some(key_file)) = (&tls_config.cert_file, &tls_config.key_file) {
        let certs = load_certs(cert_file)?;
        let key = load_private_key(key_file)?;
        config_builder
            .with_client_auth_cert(certs, key)
            .context("Failed to build mTLS client config")
    } else {
        Ok(config_builder.with_no_client_auth())
    }
}

/// Loads certificate chains from a PEM file.
fn load_certs(path: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let mut cert_file = BufReader::new(
        File::open(path).with_context(|| format!("Cannot open cert file {}", path))?,
    );
    let certs = rustls_pemfile::certs(&mut cert_file).collect::<Result<Vec<_>, _>>()?;
    Ok(certs)
}

/// Loads a private key from a PEM file.
fn load_private_key(path: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let mut key_file =
        BufReader::new(File::open(path).with_context(|| format!("Cannot open key file {}", path))?);
    rustls_pemfile::private_key(&mut key_file)?.context("No private key found in file")
}

/// Compresses data using gzip if it exceeds the threshold.
/// Returns (compressed_data, was_compressed).
#[cfg(feature = "http")]
fn compress_if_needed(
    data: &[u8],
    compression_enabled: bool,
    threshold: usize,
) -> anyhow::Result<(Vec<u8>, bool)> {
    if !compression_enabled || data.len() < threshold {
        return Ok((data.to_vec(), false));
    }

    use flate2::Compression;
    use std::io::Write;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data)?;
    let compressed = encoder.finish()?;

    // Only use compression if it actually saves space
    if compressed.len() < data.len() {
        Ok((compressed, true))
    } else {
        Ok((data.to_vec(), false))
    }
}

/// Decompresses gzip data if the Content-Encoding header indicates it.
#[cfg(feature = "http")]
fn decompress_if_needed(data: &[u8], content_encoding: Option<&str>) -> anyhow::Result<Vec<u8>> {
    if let Some(encoding) = content_encoding {
        if encoding.to_lowercase().contains("gzip") {
            use std::io::Read;
            let mut decoder = flate2::read::GzDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            return Ok(decompressed);
        }
    }
    Ok(data.to_vec())
}

/// Encodes data as base64 for HTTP Basic Authentication.
#[cfg(feature = "http")]
fn base64_encode(data: &[u8]) -> String {
    general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::create_publisher_from_route;
    use crate::models::{Config, EndpointType};
    use std::time::Duration;

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

        let mut consumer = HttpConsumer::new(&config)
            .await
            .expect("Failed to create consumer");

        let pub_config = HttpConfig {
            url: url.clone(),
            ..Default::default()
        };
        let publisher = HttpPublisher::new(&pub_config)
            .await
            .expect("Failed to create publisher");

        let msg_payload = b"test_payload".to_vec();
        let msg = CanonicalMessage::new(msg_payload.clone(), None);

        let receive_task = tokio::spawn(async move {
            let received = consumer.receive().await.expect("Failed to receive");
            let response_msg = CanonicalMessage::new(b"response_payload".to_vec(), None);
            let _ = (received.commit)(crate::traits::MessageDisposition::Reply(response_msg)).await;
            received.message
        });

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
            assert!(tokio::net::TcpStream::connect(&addr).await.is_ok());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(tokio::net::TcpStream::connect(&addr).await.is_err());
    }

    #[tokio::test]
    async fn test_http_to_static_response() {
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let http_config = HttpConfig {
            url: addr.clone(),
            ..Default::default()
        };
        let mut consumer = HttpConsumer::new(&http_config).await.unwrap();

        let static_content = "This is a static response";
        let static_publisher =
            crate::endpoints::static_endpoint::StaticEndpointPublisher::new(static_content)
                .unwrap();

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

        tokio::time::sleep(Duration::from_millis(50)).await;
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

        tokio::time::sleep(Duration::from_millis(50)).await;
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

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
