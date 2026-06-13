//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use super::http_stream::{
    handle_streamable_request, stream_request_format, stream_response_format,
    HttpReceiveStreamConfig, PublishResponseStreamError,
};
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
use http_body_util::StreamBody;
use hyper::{
    body::{Frame, Incoming},
    Request, Response, StatusCode,
};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

type HttpSourceMessage = (CanonicalMessage, CommitFunc);

#[derive(Clone, Default)]
struct HttpConnInfo {
    cipher_suite: Option<String>,
    protocol_version: Option<String>,
}

struct RequestMetadataView<'a> {
    method: &'a hyper::Method,
    path: &'a str,
    query: Option<&'a str>,
    version: hyper::Version,
    headers: &'a hyper::HeaderMap,
    conn_info: &'a HttpConnInfo,
}

type BoxBody = http_body_util::combinators::BoxBody<Bytes, anyhow::Error>;
use hyper::service::Service;
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    http_body_util::Full::new(chunk.into())
        .map_err(|_| anyhow::anyhow!("Infallible"))
        .boxed()
}

fn streamed<S>(stream: S) -> BoxBody
where
    S: futures::Stream<Item = Result<Frame<Bytes>, anyhow::Error>> + Send + Sync + 'static,
{
    StreamBody::new(stream).boxed()
}

/// A source that listens for incoming HTTP requests using hyper.
pub struct HttpConsumer {
    request_rx: tokio::sync::mpsc::Receiver<HttpSourceMessage>,
    route_id: u64,
    shared_server: Arc<SharedHttpServer>,
    buffer_size: usize,
    url: String,
    bound_addr: Option<SocketAddr>,
}

impl HttpConsumer {
    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr
    }
}

impl Drop for HttpConsumer {
    fn drop(&mut self) {
        let Ok(mut registry) = http_server_registry().lock() else {
            return;
        };
        let should_shutdown = self.shared_server.router.unregister_route(self.route_id);
        if !should_shutdown {
            return;
        }

        registry.retain(|_, server| !Arc::ptr_eq(server, &self.shared_server));
        let _ = self.shared_server.shutdown_tx.send(());
    }
}

#[derive(Clone)]
struct HttpConsumerState {
    path: Option<String>,
    tx: tokio::sync::mpsc::Sender<HttpSourceMessage>,
    inline_publisher: Option<Arc<dyn MessagePublisher>>,
    message_id_header: String,
    request_timeout: std::time::Duration,
    fire_and_forget: bool,
    receive_streamable: bool,
    basic_auth: Option<(String, String)>,
    compression_enabled: bool,
    compression_threshold_bytes: usize,
    custom_headers: HashMap<String, String>,
    concurrency_limit: Arc<tokio::sync::Semaphore>,
    method: Option<hyper::Method>,
}

#[derive(Default)]
struct SharedHttpRouter {
    routes: Mutex<HashMap<u64, Arc<HttpConsumerState>>>,
}

impl SharedHttpRouter {
    fn register_route(&self, route_id: u64, state: Arc<HttpConsumerState>) -> anyhow::Result<()> {
        let mut routes = self
            .routes
            .lock()
            .map_err(|_| anyhow!("HTTP route registry lock poisoned"))?;

        for existing in routes.values() {
            if routes_conflict(existing, &state) {
                return Err(anyhow!(
                    "Conflicting HTTP consumer registration for path {:?} and method {:?}",
                    state.path,
                    state.method
                ));
            }
        }

        routes.insert(route_id, state);
        Ok(())
    }

    fn unregister_route(&self, route_id: u64) -> bool {
        let Ok(mut routes) = self.routes.lock() else {
            return false;
        };
        routes.remove(&route_id);
        routes.is_empty()
    }

    fn match_route(&self, path: &str, method: &hyper::Method) -> anyhow::Result<RouteMatchResult> {
        let routes = self
            .routes
            .lock()
            .map_err(|_| anyhow!("HTTP route registry lock poisoned"))?;

        let mut matched_path = false;
        let mut best: Option<Arc<HttpConsumerState>> = None;
        let mut best_specificity = (0, 0);

        for state in routes.values() {
            if !route_matches_path(state, path) {
                continue;
            }

            matched_path = true;
            if route_matches_method(state, method) {
                let specificity = route_specificity(state);
                if best.is_none() || specificity > best_specificity {
                    best_specificity = specificity;
                    best = Some(Arc::clone(state));
                }
            }
        }

        Ok(match best {
            Some(state) => RouteMatchResult::Matched(state),
            None if matched_path => {
                let mut methods = routes
                    .values()
                    .filter(|state| route_matches_path(state, path))
                    .filter_map(|state| state.method.clone())
                    .collect::<Vec<_>>();
                methods.sort_by(|left, right| left.as_str().cmp(right.as_str()));
                methods.dedup();
                RouteMatchResult::MethodNotAllowed(methods)
            }
            None => RouteMatchResult::NotFound,
        })
    }
}

enum RouteMatchResult {
    Matched(Arc<HttpConsumerState>),
    MethodNotAllowed(Vec<hyper::Method>),
    NotFound,
}

struct SharedHttpServer {
    router: Arc<SharedHttpRouter>,
    shutdown_tx: tokio::sync::watch::Sender<()>,
    bound_addr: Option<SocketAddr>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct HttpServerKey {
    listen_addr: String,
    tls: TlsConfig,
    workers: usize,
}

static HTTP_SERVER_REGISTRY: OnceLock<Mutex<HashMap<HttpServerKey, Arc<SharedHttpServer>>>> =
    OnceLock::new();
static HTTP_ROUTE_ID: AtomicU64 = AtomicU64::new(1);

fn http_server_registry() -> &'static Mutex<HashMap<HttpServerKey, Arc<SharedHttpServer>>> {
    HTTP_SERVER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_http_path(path: Option<&str>) -> Option<String> {
    path.map(str::trim)
        .filter(|path| !path.is_empty())
        .map(|path| {
            if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{}", path)
            }
        })
}

/// Guesses an HTTP `Content-Type` from a file path or bare file extension.
///
/// This accepts values like `"index.html"`, `"/assets/app.js"`, `".svg"`, or `"png"`.
/// Unknown extensions default to `application/octet-stream`.
pub fn guess_content_type(path_or_extension: &str) -> &'static str {
    let input = path_or_extension.trim();
    let extension = Path::new(input)
        .extension()
        .and_then(|ext| ext.to_str())
        .filter(|ext| !ext.is_empty())
        .or_else(|| input.strip_prefix('.'))
        .unwrap_or(input)
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase();

    match extension.as_str() {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" | "cjs" => "text/javascript; charset=utf-8",
        "json" | "map" | "jsonld" => "application/json; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "yaml" | "yml" => "application/yaml; charset=utf-8",
        "pdf" => "application/pdf",
        "wasm" => "application/wasm",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "tar" => "application/x-tar",
        "7z" => "application/x-7z-compressed",
        "rar" => "application/vnd.rar",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "png" => "image/png",
        "apng" => "image/apng",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "bmp" => "image/bmp",
        "tif" | "tiff" => "image/tiff",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "eot" => "application/vnd.ms-fontobject",
        "txt" | "text" => "text/plain; charset=utf-8",
        "md" => "text/markdown; charset=utf-8",
        "csv" => "text/csv; charset=utf-8",
        "tsv" => "text/tab-separated-values; charset=utf-8",
        "ics" => "text/calendar; charset=utf-8",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" | "oga" => "audio/ogg",
        "m4a" => "audio/mp4",
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "avi" => "video/x-msvideo",
        "mpeg" | "mpg" => "video/mpeg",
        "ogv" => "video/ogg",
        _ => "application/octet-stream",
    }
}

fn routes_conflict(left: &HttpConsumerState, right: &HttpConsumerState) -> bool {
    left.path == right.path
        && (left.method == right.method || left.method.is_none() || right.method.is_none())
}

fn route_matches_path(state: &HttpConsumerState, path: &str) -> bool {
    match &state.path {
        Some(route_path) => route_path == path,
        None => true,
    }
}

fn route_matches_method(state: &HttpConsumerState, method: &hyper::Method) -> bool {
    match &state.method {
        Some(route_method) => route_method == method,
        None => true,
    }
}

fn route_specificity(state: &HttpConsumerState) -> (u8, u8) {
    (
        u8::from(state.path.is_some()),
        u8::from(state.method.is_some()),
    )
}

fn request_accepts_text(headers: &hyper::HeaderMap) -> bool {
    let accept_values = headers.get_all("accept");
    if accept_values.iter().next().is_none() {
        return true;
    }

    accept_values.iter().any(|value| {
        value.to_str().ok().is_some_and(|raw| {
            raw.split(',').any(|item| {
                let media_type = item
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                matches!(media_type.as_str(), "*/*" | "text/*" | "text/plain")
            })
        })
    })
}

fn http_version_str(version: hyper::Version) -> &'static str {
    match version {
        hyper::Version::HTTP_09 => "HTTP/0.9",
        hyper::Version::HTTP_10 => "HTTP/1.0",
        hyper::Version::HTTP_11 => "HTTP/1.1",
        hyper::Version::HTTP_2 => "HTTP/2.0",
        hyper::Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/?",
    }
}

fn request_metadata_matches(request: &RequestMetadataView<'_>, key: &str, value: &str) -> bool {
    match key {
        "http_method" => request.method.as_str() == value,
        "http_path" => request.path == value,
        "http_query" => request.query.unwrap_or("") == value,
        "http_version" => http_version_str(request.version) == value,
        "tls_cipher_suite" => request.conn_info.cipher_suite.as_deref() == Some(value),
        "tls_protocol_version" => request.conn_info.protocol_version.as_deref() == Some(value),
        _ => request
            .headers
            .get(key)
            .and_then(|header| header.to_str().ok())
            .is_some_and(|original| original == value),
    }
}

fn has_content_type_header(headers: &HashMap<String, String>) -> bool {
    headers
        .keys()
        .any(|key| key.eq_ignore_ascii_case("content-type"))
}

fn text_error_response(
    status: StatusCode,
    body: impl Into<Bytes>,
    accepts_text: bool,
    custom_headers: Option<&HashMap<String, String>>,
) -> Response<BoxBody> {
    let mut builder = Response::builder().status(status);

    if let Some(custom_headers) = custom_headers {
        for (header_name, header_value) in custom_headers {
            builder = builder.header(header_name.as_str(), header_value.as_str());
        }

        if accepts_text && !has_content_type_header(custom_headers) {
            builder = builder.header("content-type", "text/plain; charset=utf-8");
        }
    } else if accepts_text {
        builder = builder.header("content-type", "text/plain; charset=utf-8");
    }

    builder.body(full(body)).unwrap()
}

/// A `hyper::Service` that can be nested into other services (like Axum).
/// It forwards requests into the `mq-bridge` ecosystem.
#[derive(Clone)]
pub struct HttpBridgeService {
    router: Arc<SharedHttpRouter>,
    conn_info: HttpConnInfo,
}

impl Service<Request<Incoming>> for HttpBridgeService {
    type Response = Response<BoxBody>;
    type Error = anyhow::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let router = self.router.clone();
        let conn_info = self.conn_info.clone();
        Box::pin(handle_request(router, conn_info, req))
    }
}

impl HttpConsumer {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        Self::new_with_inline_publisher(config, None).await
    }

    pub async fn new_with_inline_publisher(
        config: &HttpConfig,
        inline_publisher: Option<Arc<dyn MessagePublisher>>,
    ) -> anyhow::Result<Self> {
        let (request_rx, state, buffer_size) =
            setup_http_state_and_channel(config, inline_publisher)?;
        let listen_address = &config.url;
        let addr: SocketAddr = listen_address
            .parse()
            .with_context(|| format!("Invalid listen address: {}", listen_address))?;

        let tls_config = config.tls.clone();
        let workers = if config.workers.unwrap_or(0) == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            config.workers.unwrap()
        };
        let route_id = HTTP_ROUTE_ID.fetch_add(1, Ordering::Relaxed);
        let server_key = HttpServerKey {
            listen_addr: addr.to_string(),
            tls: tls_config.clone(),
            workers,
        };
        let shared_server =
            get_or_create_shared_http_server(&server_key, &tls_config, route_id, Arc::new(state))
                .await?;

        Ok(Self {
            request_rx,
            route_id,
            shared_server: shared_server.clone(),
            buffer_size,
            url: build_consumer_target_url(config, shared_server.bound_addr),
            bound_addr: shared_server.bound_addr,
        })
    }
}

/// Helper to set up the shared state and communication channel for the HTTP consumer.
fn setup_http_state_and_channel(
    config: &HttpConfig,
    inline_publisher: Option<Arc<dyn MessagePublisher>>,
) -> anyhow::Result<(
    tokio::sync::mpsc::Receiver<HttpSourceMessage>,
    HttpConsumerState,
    usize,
)> {
    let buffer_size = config.internal_buffer_size.unwrap_or(100).max(1);
    let (request_tx, request_rx) = tokio::sync::mpsc::channel::<HttpSourceMessage>(buffer_size);

    let message_id_header = config
        .message_id_header
        .clone()
        .unwrap_or_else(|| "message-id".to_string());
    let request_timeout =
        std::time::Duration::from_millis(config.request_timeout_ms.unwrap_or(30000));
    let compression_threshold_bytes = config.compression_threshold_bytes.unwrap_or(1024);

    let method = config
        .method
        .as_deref()
        .map(|m| {
            hyper::Method::from_bytes(m.as_bytes())
                .map_err(|_| anyhow::anyhow!("Invalid config.method: '{}'", m))
        })
        .transpose()?;

    let state = HttpConsumerState {
        path: normalize_http_path(config.path.as_deref()),
        tx: request_tx,
        inline_publisher,
        message_id_header,
        request_timeout,
        fire_and_forget: config.fire_and_forget,
        receive_streamable: config.receive_streamable,
        basic_auth: config.basic_auth.clone(),
        compression_enabled: config.compression_enabled,
        compression_threshold_bytes,
        custom_headers: config.custom_headers.clone(),
        concurrency_limit: Arc::new(tokio::sync::Semaphore::new(
            config.concurrency_limit.unwrap_or(100).max(1),
        )),
        method,
    };
    Ok((request_rx, state, buffer_size))
}

fn build_consumer_target_url(config: &HttpConfig, bound_addr: Option<SocketAddr>) -> String {
    let base = config
        .url
        .parse::<SocketAddr>()
        .ok()
        .and_then(|configured_addr| {
            if configured_addr.port() == 0 {
                bound_addr.map(|bound_addr| {
                    SocketAddr::new(configured_addr.ip(), bound_addr.port()).to_string()
                })
            } else {
                None
            }
        })
        .unwrap_or_else(|| config.url.clone());
    let mut url = config.tls.normalize_url(&base);
    if let Some(path) = normalize_http_path(config.path.as_deref()) {
        url.push_str(&path);
    }
    url
}

async fn get_or_create_shared_http_server(
    key: &HttpServerKey,
    tls_config: &TlsConfig,
    route_id: u64,
    state: Arc<HttpConsumerState>,
) -> anyhow::Result<Arc<SharedHttpServer>> {
    let addr: SocketAddr = key
        .listen_addr
        .parse()
        .with_context(|| format!("Invalid listen address: {}", key.listen_addr))?;
    let uses_ephemeral_port = addr.port() == 0;

    if !uses_ephemeral_port {
        if let Ok(registry) = http_server_registry().lock() {
            for (existing_key, server) in registry.iter() {
                if existing_key.listen_addr != key.listen_addr {
                    continue;
                }
                if existing_key == key {
                    server.router.register_route(route_id, state.clone())?;
                    return Ok(server.clone());
                }
                return Err(anyhow!(
                    "HTTP consumer {} is already registered with different TLS or worker settings",
                    key.listen_addr
                ));
            }
        }
    }

    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {}", addr))?;
    let bound_addr = listener.local_addr().ok();
    let registry_key = if uses_ephemeral_port {
        let Some(bound_addr) = bound_addr else {
            return Err(anyhow!("Failed to determine bound HTTP listener address"));
        };
        HttpServerKey {
            listen_addr: bound_addr.to_string(),
            tls: key.tls.clone(),
            workers: key.workers,
        }
    } else {
        key.clone()
    };
    let listener = Arc::new(listener);
    let router = Arc::new(SharedHttpRouter::default());
    let service = HttpBridgeService {
        router: router.clone(),
        conn_info: HttpConnInfo::default(),
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    if tls_config.required {
        if !tls_config.is_tls_server_configured() {
            return Err(anyhow!(
                "HTTP server TLS enabled but no cert/key provided in HttpConfig"
            ));
        }
        info!(
            "Starting shared HTTPS source on {} with {} workers",
            addr, key.workers
        );
        spawn_tls_server(listener, service, shutdown_rx, tls_config, key.workers).await?;
    } else {
        info!(
            "Starting shared HTTP source on {} with {} workers",
            addr, key.workers
        );
        spawn_http_server(listener, service, shutdown_rx, key.workers).await?;
    }

    let server = Arc::new(SharedHttpServer {
        router,
        shutdown_tx,
        bound_addr,
    });

    let mut registry = http_server_registry()
        .lock()
        .map_err(|_| anyhow!("HTTP server registry lock poisoned"))?;
    for (existing_key, existing) in registry.iter() {
        if existing_key.listen_addr != registry_key.listen_addr {
            continue;
        }
        if existing_key == &registry_key {
            let _ = server.shutdown_tx.send(());
            existing.router.register_route(route_id, state.clone())?;
            return Ok(existing.clone());
        }
        let _ = server.shutdown_tx.send(());
        return Err(anyhow!(
            "HTTP consumer {} is already registered with different TLS or worker settings",
            key.listen_addr
        ));
    }
    server.router.register_route(route_id, state)?;
    registry.insert(registry_key, server.clone());
    Ok(server)
}

async fn spawn_http_server(
    listener: Arc<TcpListener>,
    service: HttpBridgeService,
    shutdown_rx: tokio::sync::watch::Receiver<()>,
    workers: usize,
) -> anyhow::Result<()> {
    for i in 0..workers {
        let listener = listener.clone();
        let service = service.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        tokio::spawn(async move {
            trace!("HTTP worker {} started", i);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        trace!("HTTP worker {} shutting down", i);
                        break;
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((socket, _)) => {
                                let _ = socket.set_nodelay(true);
                                let mut conn_service = service.clone();
                                conn_service.conn_info = HttpConnInfo::default();
                                tokio::spawn(async move {
                                    let io = TokioIo::new(socket);
                                    let mut builder = AutoBuilder::new(TokioExecutor::new());
                                    builder.http1().keep_alive(true);
                                    builder.http2().max_concurrent_streams(200);
                                    let conn = builder.serve_connection_with_upgrades(io, conn_service).await;
                                    if let Err(e) = conn {
                                        trace!("Connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                match e.kind() {
                                    std::io::ErrorKind::WouldBlock
                                    | std::io::ErrorKind::Interrupted
                                    | std::io::ErrorKind::TimedOut => {
                                        trace!("Transient accept error in worker {}: {}", i, e);
                                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                                    }
                                    _ if e.raw_os_error() == Some(24) => { // EMFILE (Too many open files)
                                        warn!("HTTP worker {}: FD limit reached, cooling down...", i);
                                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                    }
                                    _ => {
                                        warn!("Accept error in worker {}: {}. Retrying...", i, e);
                                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    Ok(())
}

async fn spawn_tls_server(
    listener: Arc<TcpListener>,
    service: HttpBridgeService,
    shutdown_rx: tokio::sync::watch::Receiver<()>,
    tls_config: &TlsConfig,
    workers: usize,
) -> anyhow::Result<()> {
    let rustls_server_config =
        create_rustls_server_config(tls_config).context("Failed to create rustls server config")?;
    let acceptor = TlsAcceptor::from(rustls_server_config);

    for i in 0..workers {
        let listener = listener.clone();
        let service = service.clone();
        let acceptor = acceptor.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        tokio::spawn(async move {
            trace!("TLS worker {} started", i);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        trace!("TLS worker {} shutting down", i);
                        break;
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((socket, _)) => {
                                let acceptor = acceptor.clone();
                                let mut conn_service = service.clone();
                                tokio::spawn(async move {
                                    match acceptor.accept(socket).await {
                                        Ok(stream) => {
                                            let mut conn_info = HttpConnInfo::default();
                                            let (_, session) = stream.get_ref();
                                            conn_info.cipher_suite = session.negotiated_cipher_suite().map(|c| format!("{:?}", c.suite()));
                                            conn_info.protocol_version = session.protocol_version().map(|v| format!("{:?}", v));
                                            conn_service.conn_info = conn_info;

                                            let io = TokioIo::new(stream);
                                            let mut builder = AutoBuilder::new(TokioExecutor::new());
                                            builder.http1().keep_alive(true);
                                            builder.http2().max_concurrent_streams(200);
                                            let conn = builder.serve_connection_with_upgrades(io, conn_service).await;

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
                                match e.kind() {
                                    std::io::ErrorKind::WouldBlock
                                    | std::io::ErrorKind::Interrupted
                                    | std::io::ErrorKind::TimedOut => {
                                        trace!("Transient accept error in TLS worker {}: {}", i, e);
                                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                                    }
                                    _ if e.raw_os_error() == Some(24) => { // EMFILE
                                        warn!("TLS worker {}: FD limit reached, cooling down...", i);
                                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                    }
                                    _ => {
                                        warn!("Accept error in TLS worker {}: {}. Retrying...", i, e);
                                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

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

        let mut messages = Vec::with_capacity(max_messages);
        messages.push(first_message);
        let mut commits = Vec::with_capacity(max_messages);
        commits.push(first_commit);

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
                    tracing::trace!(
                        count = dispositions.len(),
                        "Committing batch of HTTP messages"
                    );
                    // Commit each message with its corresponding disposition
                    let mut results = Vec::with_capacity(commits.len());
                    for (commit, disposition) in commits.into_iter().zip(dispositions) {
                        results.push(commit(disposition).await);
                    }
                    results.into_iter().collect::<anyhow::Result<()>>()
                }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
            });

        Ok(ReceivedBatch {
            messages,
            commit: batch_commit,
        })
    }

    async fn status(&self) -> crate::traits::EndpointStatus {
        crate::traits::EndpointStatus {
            healthy: true,
            target: self.url.clone(),
            pending: Some(self.request_rx.len()),
            capacity: Some(self.buffer_size),
            details: serde_json::json!({ "bound_addr": self.bound_addr }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tracing::instrument(level = "trace", skip_all)]
async fn handle_request(
    router: Arc<SharedHttpRouter>,
    conn_info: HttpConnInfo,
    req: Request<Incoming>,
) -> anyhow::Result<Response<BoxBody>> {
    let accepts_text = request_accepts_text(req.headers());
    match handle_request_internal(router, conn_info, req, accepts_text).await {
        Ok(res) => Ok(res),
        Err(e) => {
            tracing::error!("Internal error handling HTTP request: {}", e);
            Ok(text_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {}", e),
                accepts_text,
                None,
            ))
        }
    }
}

async fn handle_request_internal(
    router: Arc<SharedHttpRouter>,
    conn_info: HttpConnInfo,
    req: Request<Incoming>,
    accepts_text: bool,
) -> anyhow::Result<Response<BoxBody>> {
    let state = match router.match_route(req.uri().path(), req.method())? {
        RouteMatchResult::Matched(state) => state,
        RouteMatchResult::MethodNotAllowed(allowed_methods) => {
            let mut headers = HashMap::new();
            if !allowed_methods.is_empty() {
                headers.insert(
                    "Allow".to_string(),
                    allowed_methods
                        .iter()
                        .map(hyper::Method::as_str)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            return Ok(text_error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                format!("Method {} not allowed", req.method()),
                accepts_text,
                Some(&headers),
            ));
        }
        RouteMatchResult::NotFound => {
            return Ok(text_error_response(
                StatusCode::NOT_FOUND,
                "No HTTP consumer registered for this path",
                accepts_text,
                None,
            ));
        }
    };

    // Acquire a permit to limit concurrent requests and prevent resource exhaustion.
    // This applies backpressure to Hyper and prevents task explosion during retry storms.
    let permit = state
        .concurrency_limit
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| anyhow!(e))?;

    if let Some((expected_user, expected_pass)) = configured_basic_auth(state.basic_auth.as_ref()) {
        if let Some(auth_header) = req.headers().get("authorization") {
            match auth_header.to_str() {
                Ok(auth_str) => {
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
                                    return Ok(text_error_response(
                                        StatusCode::UNAUTHORIZED,
                                        "Invalid credentials",
                                        accepts_text,
                                        None,
                                    ));
                                }
                            } else {
                                return Ok(text_error_response(
                                    StatusCode::BAD_REQUEST,
                                    "Invalid authorization header encoding",
                                    accepts_text,
                                    None,
                                ));
                            }
                        } else {
                            return Ok(text_error_response(
                                StatusCode::BAD_REQUEST,
                                "Invalid base64 encoding in authorization header",
                                accepts_text,
                                None,
                            ));
                        }
                    } else {
                        return Ok(text_error_response(
                            StatusCode::UNAUTHORIZED,
                            "Missing Basic authentication scheme",
                            accepts_text,
                            None,
                        ));
                    }
                }
                Err(_) => {
                    return Ok(text_error_response(
                        StatusCode::BAD_REQUEST,
                        "Invalid authorization header encoding",
                        accepts_text,
                        None,
                    ));
                }
            }
        } else {
            return Ok(text_error_response(
                StatusCode::UNAUTHORIZED,
                "Missing authorization header",
                accepts_text,
                None,
            ));
        }
    }

    let (parts, body) = req.into_parts();
    let request_metadata_view = RequestMetadataView {
        method: &parts.method,
        path: parts.uri.path(),
        query: parts.uri.query(),
        version: parts.version,
        headers: &parts.headers,
        conn_info: &conn_info,
    };

    let mut message_id = None;
    if let Some(header_value) = parts.headers.get(state.message_id_header.as_str()) {
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
    let mut metadata = HashMap::with_capacity(parts.headers.len() + 6);
    let mut content_encoding = None;

    metadata.extend([
        ("http_method".to_string(), parts.method.to_string()),
        ("http_path".to_string(), parts.uri.path().to_string()),
        (
            "http_query".to_string(),
            parts.uri.query().unwrap_or("").to_string(),
        ),
        (
            "http_version".to_string(),
            http_version_str(parts.version).to_string(),
        ),
    ]);

    if let Some(cs) = conn_info.cipher_suite.as_ref() {
        metadata.insert("tls_cipher_suite".to_string(), cs.clone());
    }
    if let Some(pv) = conn_info.protocol_version.as_ref() {
        metadata.insert("tls_protocol_version".to_string(), pv.clone());
    }

    for (key, value) in &parts.headers {
        if let Ok(v_str) = value.to_str() {
            if key.as_str().eq_ignore_ascii_case("content-encoding") {
                content_encoding = Some(v_str.to_string());
            }
            let k_str = key.as_str();
            if k_str == "http_method"
                || k_str == "http_path"
                || k_str == "http_query"
                || k_str == "http_version"
                || k_str.eq_ignore_ascii_case("tls_cipher_suite")
                || k_str.eq_ignore_ascii_case("tls_protocol_version")
            {
                continue;
            }
            metadata.insert(k_str.to_string(), v_str.to_string());
        }
    }

    if state.receive_streamable {
        if content_encoding.is_some() {
            return Ok(text_error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Compressed streamable HTTP requests are not supported",
                accepts_text,
                Some(&state.custom_headers),
            ));
        }

        let request_format = stream_request_format(&parts.headers);
        let response_format = stream_response_format(&parts.headers);
        return handle_streamable_request(
            body,
            metadata,
            HttpReceiveStreamConfig {
                tx: state.tx.clone(),
                inline_publisher: state.inline_publisher.clone(),
                fire_and_forget: state.fire_and_forget,
                request_timeout: state.request_timeout,
                custom_headers: state.custom_headers.clone(),
            },
            request_format,
            response_format,
            accepts_text,
            permit,
        )
        .await;
    }

    // Read body with a timeout to prevent hanging on abandoned client connections.
    // This prevents "zombie" tasks from saturating the runtime during retry storms.
    let body_collect_timeout = state.request_timeout;
    let body_bytes = match tokio::time::timeout(body_collect_timeout, body.collect()).await {
        Ok(Ok(b)) => b.to_bytes(),
        Ok(Err(e)) => {
            return Ok(text_error_response(
                StatusCode::BAD_REQUEST,
                format!("Failed to read body: {}", e),
                accepts_text,
                None,
            ));
        }
        Err(_) => {
            return Ok(text_error_response(
                StatusCode::REQUEST_TIMEOUT,
                "Timed out reading request body",
                accepts_text,
                None,
            ));
        }
    };

    // Decompress if needed
    let payload = decompress_if_needed(body_bytes, content_encoding.as_deref())
        .map_err(|e| anyhow!("Failed to decompress request body: {}", e))?;

    let mut message = CanonicalMessage::new_bytes(payload, message_id);
    trace!(
        message_id = format!("{:032x}", message.message_id),
        "Received HTTP request"
    );

    message.metadata = metadata;

    if let Some(inline_publisher) = state.inline_publisher.as_ref() {
        let timeout_duration = state.request_timeout;
        drop(permit);
        tracing::trace!(
            timeout_ms = timeout_duration.as_millis(),
            "HTTP handler waiting for inline publisher response"
        );
        let disposition =
            match tokio::time::timeout(timeout_duration, inline_publisher.send(message)).await {
                Ok(Ok(Sent::Response(response))) => MessageDisposition::Reply(response),
                Ok(Ok(Sent::Ack)) => MessageDisposition::Ack,
                Ok(Err(err)) => {
                    tracing::warn!("HTTP inline publisher failed: {}", err);
                    MessageDisposition::Nack
                }
                Err(_) => {
                    tracing::warn!(
                        "HTTP handler: inline request timed out after {} ms",
                        timeout_duration.as_millis()
                    );
                    return Ok(text_error_response(
                        StatusCode::GATEWAY_TIMEOUT,
                        "Request timed out",
                        accepts_text,
                        Some(&state.custom_headers),
                    ));
                }
            };

        return make_response(
            disposition,
            state.compression_enabled,
            state.compression_threshold_bytes,
            &state.custom_headers,
            accepts_text,
            Some(&request_metadata_view),
        );
    }

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

    // Use a timeout for the internal channel send to prevent connection task hang
    // if the bridge pipeline is overloaded. 503 Service Unavailable is returned
    // to allow the client to retry later without holding onto server resources.
    let send_timeout = std::time::Duration::from_millis(2000).min(state.request_timeout / 2);
    match tokio::time::timeout(send_timeout, state.tx.send((message, commit))).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            tracing::error!("Failed to send request to bridge (channel closed): {}", e);
            return Ok(text_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal pipeline closed",
                accepts_text,
                None,
            ));
        }
        Err(_) => {
            tracing::warn!("HTTP handler: internal channel full, request rejected");
            return Ok(text_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Server overloaded",
                accepts_text,
                Some(&state.custom_headers),
            ));
        }
    }

    // Release the concurrency permit here, after the message has been handed off to the
    // pipeline. The permit protects against connection/body-read explosion, not against
    // slow downstream processing. Holding it while waiting on ack_rx would cause a
    // deadlock: all permits occupied by requests waiting on downstream, no capacity
    // left for new requests, retry storm fills the channel → everything stalls.
    drop(permit);

    if state.fire_and_forget {
        let mut builder = Response::builder().status(StatusCode::ACCEPTED);
        for (header_name, header_value) in &state.custom_headers {
            builder = builder.header(header_name.as_str(), header_value.as_str());
        }
        return Ok(builder
            .body(full("Message accepted for processing"))
            .unwrap());
    }

    let timeout_duration = state.request_timeout;
    tracing::trace!(
        timeout_ms = timeout_duration.as_millis(),
        "HTTP handler waiting for disposition"
    );
    match tokio::time::timeout(timeout_duration, ack_rx).await {
        Ok(Ok(disposition)) => make_response(
            disposition,
            state.compression_enabled,
            state.compression_threshold_bytes,
            &state.custom_headers,
            accepts_text,
            None,
        ),
        Ok(Err(_)) => {
            tracing::error!("HTTP handler: pipeline closed before disposition arrived");
            Ok(text_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Pipeline closed",
                accepts_text,
                Some(&state.custom_headers),
            ))
        }
        Err(_) => {
            tracing::warn!(
                "HTTP handler: request timed out after {} ms",
                timeout_duration.as_millis()
            );
            Ok(text_error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "Request timed out",
                accepts_text,
                Some(&state.custom_headers),
            ))
        }
    }
}

fn make_response(
    disposition: MessageDisposition,
    compression_enabled: bool,
    compression_threshold_bytes: usize,
    custom_headers: &HashMap<String, String>,
    accepts_text: bool,
    request_metadata: Option<&RequestMetadataView<'_>>,
) -> anyhow::Result<Response<BoxBody>> {
    match disposition {
        MessageDisposition::Reply(mut msg) => {
            let status = msg
                .metadata
                .remove("http_status_code")
                .and_then(|s| s.parse::<u16>().ok())
                .and_then(|code| StatusCode::from_u16(code).ok())
                .unwrap_or(StatusCode::OK);

            let mut builder = Response::builder().status(status);

            let mut has_content_type = false;
            let mut is_streaming = false;
            for (key, value) in &msg.metadata {
                if request_metadata
                    .is_some_and(|metadata| request_metadata_matches(metadata, key, value))
                {
                    continue;
                }
                if key.eq_ignore_ascii_case("content-type") {
                    has_content_type = true;
                    if value.contains("text/event-stream") {
                        is_streaming = true;
                    }
                } else if key.eq_ignore_ascii_case("transfer-encoding") && value.contains("chunked")
                {
                    is_streaming = true;
                }

                if !key.eq_ignore_ascii_case("content-encoding")
                    && !key.eq_ignore_ascii_case("transfer-encoding")
                    && !key.eq_ignore_ascii_case("content-length")
                {
                    builder = builder.header(key.as_str(), value.as_str());
                }
            }

            if !has_content_type && status == StatusCode::OK {
                builder = builder.header("content-type", "application/octet-stream");
            }

            // Compress payload if enabled and beneficial
            let (payload_out, was_compressed) = compress_if_needed(
                msg.payload,
                compression_enabled,
                compression_threshold_bytes,
            )?;

            if was_compressed {
                builder = builder.header("Content-Encoding", "gzip");
            }

            // Add custom headers to response
            for (header_name, header_value) in custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }

            if is_streaming {
                let stream = futures::stream::once(async move {
                    Ok::<_, anyhow::Error>(Frame::data(payload_out))
                });
                Ok(builder.body(streamed(stream)).unwrap())
            } else {
                Ok(builder.body(full(payload_out)).unwrap())
            }
        }
        MessageDisposition::Ack => {
            let mut builder = Response::builder().status(StatusCode::ACCEPTED);
            for (header_name, header_value) in custom_headers {
                builder = builder.header(header_name.as_str(), header_value.as_str());
            }
            Ok(builder.body(full("Message processed")).unwrap())
        }
        MessageDisposition::Nack => Ok(text_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Message processing failed",
            accepts_text,
            Some(custom_headers),
        )),
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
    base_uri: hyper::Uri,
    /// Default HTTP method to use if not overridden by message metadata
    method: hyper::Method,
    request_timeout: std::time::Duration,
    batch_concurrency: usize,
    compression_enabled: bool,
    compression_threshold_bytes: usize,
    basic_auth_header: Option<String>,
    custom_headers: HashMap<String, String>,
    stream_response_sink: Option<std::sync::Arc<dyn MessagePublisher>>,
}

impl HttpPublisher {
    pub async fn new(config: &HttpConfig) -> anyhow::Result<Self> {
        Self::new_with_stream_response_sink(config, None).await
    }

    pub async fn new_with_stream_response_sink(
        config: &HttpConfig,
        stream_response_sink: Option<std::sync::Arc<dyn MessagePublisher>>,
    ) -> anyhow::Result<Self> {
        // Initialize TLS provider if TLS is configured for this endpoint.
        let batch_concurrency = config.batch_concurrency.unwrap_or(20).max(1);

        let tls_client_config = create_rustls_client_config(&config.tls)
            .context("Failed to create rustls client config")?;

        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        http_connector.set_nodelay(true);
        if let Some(keepalive) = config.tcp_keepalive_ms {
            http_connector.set_keepalive(Some(std::time::Duration::from_millis(keepalive)));
        }

        // Create HTTPS connector that handles both http and https, and http1/http2
        let https_connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls_client_config)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http_connector);

        // Create persistent client with connection pooling
        let mut client_builder = hyper_util::client::legacy::Client::builder(TokioExecutor::new());
        if let Some(timeout) = config.pool_idle_timeout_ms {
            client_builder.pool_idle_timeout(std::time::Duration::from_millis(timeout));
        }
        let client = client_builder.build(https_connector);

        let url = config.tls.normalize_url(&config.url);

        let base_uri = url
            .parse::<hyper::Uri>()
            .map_err(|e| anyhow::anyhow!("Invalid configured URL '{}': {}", url, e))?;

        let method = config
            .method
            .as_deref()
            .map(|m| {
                hyper::Method::from_bytes(m.as_bytes())
                    .map_err(|_| anyhow::anyhow!("Invalid config.method: '{}'", m))
            })
            .transpose()?
            .unwrap_or(hyper::Method::POST);

        let request_timeout =
            std::time::Duration::from_millis(config.request_timeout_ms.unwrap_or(30000));

        let compression_threshold_bytes = config.compression_threshold_bytes.unwrap_or(1024);

        Ok(Self {
            client: std::sync::Arc::new(client),
            url,
            base_uri,
            method,
            request_timeout,
            batch_concurrency,
            compression_enabled: config.compression_enabled,
            compression_threshold_bytes,
            basic_auth_header: basic_auth_header_value(config.basic_auth.as_ref()),
            custom_headers: config.custom_headers.clone(),
            stream_response_sink,
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

        let method = message
            .metadata
            .get("http_method")
            .and_then(|m| hyper::Method::from_bytes(m.as_bytes()).ok())
            .unwrap_or_else(|| self.method.clone());

        let uri = if let Some(path) = message.metadata.get("http_path") {
            let mut path_and_query = path.clone();
            if let Some(query) = message.metadata.get("http_query") {
                if !query.is_empty() {
                    path_and_query.push('?');
                    path_and_query.push_str(query);
                }
            }
            let mut builder = hyper::Uri::builder();
            if let Some(scheme) = self.base_uri.scheme() {
                builder = builder.scheme(scheme.clone());
            }
            if let Some(authority) = self.base_uri.authority() {
                builder = builder.authority(authority.clone());
            }
            builder
                .path_and_query(path_and_query)
                .build()
                .map_err(|e| {
                    PublisherError::NonRetryable(anyhow::anyhow!("Failed to build URI: {}", e))
                })?
        } else {
            self.base_uri.clone()
        };

        let mut request_builder = Request::builder().method(method).uri(uri);

        for (key, value) in &message.metadata {
            if key == "http_method"
                || key == "http_path"
                || key == "http_query"
                || key == "http_version"
                || key == "tls_cipher_suite"
                || key == "tls_protocol_version"
            {
                continue;
            }
            request_builder = request_builder.header(key, value);
        }

        // Only attach Basic auth when credentials are actually configured.
        if let Some(header_value) = self.basic_auth_header.as_deref() {
            request_builder = request_builder.header("Authorization", header_value);
        }

        // Add custom authentication headers
        for (header_name, header_value) in &self.custom_headers {
            request_builder = request_builder.header(header_name.as_str(), header_value.as_str());
        }

        // Compress payload if enabled and beneficial
        let (payload_out, was_compressed) = compress_if_needed(
            message.payload.clone(),
            self.compression_enabled,
            self.compression_threshold_bytes,
        )
        .map_err(|e| {
            PublisherError::NonRetryable(anyhow::anyhow!("Failed to compress payload: {}", e))
        })?;

        if was_compressed {
            request_builder = request_builder.header("Content-Encoding", "gzip");
        }

        let body = http_body_util::Full::from(payload_out);
        let request = request_builder.body(body).map_err(|e| {
            PublisherError::NonRetryable(anyhow::anyhow!("Failed to build request: {}", e))
        })?;

        let future = tokio::time::timeout(self.request_timeout, self.client.request(request));

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
        let stream_response_format = self.stream_response_sink.as_ref().and_then(|_| {
            super::http_stream::streaming_response_format_from_headers(response.headers())
        });
        let mut response_metadata = HashMap::with_capacity(response.headers().len() + 1);
        response_metadata.insert(
            "http_version".to_string(),
            format!("{:?}", response.version()),
        );
        let mut content_encoding = None;
        for (key, value) in response.headers() {
            if let Ok(value_str) = value.to_str() {
                if key.as_str().eq_ignore_ascii_case("content-encoding") {
                    content_encoding = Some(value_str.to_string());
                }
                response_metadata.insert(key.as_str().to_string(), value_str.to_string());
            }
        }

        if response_status.is_success() {
            if let (Some(stream_response_sink), Some(stream_response_format)) =
                (&self.stream_response_sink, stream_response_format)
            {
                if content_encoding.is_some() {
                    return Err(PublisherError::Retryable(anyhow::anyhow!(
                        "Compressed HTTP response streams cannot be published to stream_response_to"
                    )));
                }

                let correlation_id = message
                    .metadata
                    .get("correlation_id")
                    .cloned()
                    .unwrap_or_else(|| format!("{:032x}", message.message_id));
                match super::http_stream::publish_response_stream(
                    response.into_body(),
                    stream_response_sink.clone(),
                    response_metadata,
                    correlation_id,
                    stream_response_format,
                    self.request_timeout,
                )
                .await
                {
                    Ok(()) => {}
                    Err(PublishResponseStreamError::Partial(error)) => {
                        tracing::warn!(
                            "HTTP response stream terminated after partial publish: {}",
                            error
                        );
                    }
                    Err(PublishResponseStreamError::BeforePublish(error)) => return Err(error),
                }
                return Ok(Sent::Ack);
            }
        }

        // Use a shorter cap for body collection — the full request timeout was already
        // spent on getting the response headers. Reusing it here could double the total
        // wall time a single send() call blocks the caller.
        let body_collect_timeout = self.request_timeout;
        let response_bytes_raw = match tokio::time::timeout(
            body_collect_timeout,
            response.into_body().collect(),
        )
        .await
        {
            Ok(Ok(collected)) => collected.to_bytes(),
            Ok(Err(e)) => {
                return Err(PublisherError::Retryable(anyhow::anyhow!(
                    "Failed to read HTTP response body: {}",
                    e
                )))
            }
            Err(_) => {
                return Err(PublisherError::Retryable(anyhow::anyhow!(
                    "HTTP response body collection timeout"
                )))
            }
        };

        // Decompress response if needed
        let response_bytes = decompress_if_needed(response_bytes_raw, content_encoding.as_deref())
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

        let mut response_message =
            CanonicalMessage::new_bytes(response_bytes, Some(message.message_id));
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

        if messages.len() == 1 {
            let message = messages.into_iter().next().expect("checked len");
            return match self.send(message.clone()).await {
                Ok(Sent::Ack) => Ok(SentBatch::Ack),
                Ok(Sent::Response(resp)) => Ok(SentBatch::Partial {
                    responses: Some(vec![resp]),
                    failed: Vec::new(),
                }),
                Err(e) => Ok(SentBatch::Partial {
                    responses: None,
                    failed: vec![(message, e)],
                }),
            };
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

    async fn status(&self) -> crate::traits::EndpointStatus {
        crate::traits::EndpointStatus {
            healthy: true,
            target: self.url.clone(),
            ..Default::default()
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

/// Creates a `rustls::ServerConfig` for the HTTPS server.
fn create_rustls_server_config(
    tls_config: &TlsConfig,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    // Ensure a process-level rustls CryptoProvider is installed when building server config.
    // This avoids a runtime panic if the provider wasn't set elsewhere.

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

    let config_builder =
        rustls::ServerConfig::builder_with_provider(crate::endpoints::get_crypto_provider()?)
            .with_safe_default_protocol_versions()?;

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

    let config_builder =
        rustls::ClientConfig::builder_with_provider(crate::endpoints::get_crypto_provider()?)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(root_cert_store);

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
    data: Bytes,
    compression_enabled: bool,
    threshold: usize,
) -> anyhow::Result<(Bytes, bool)> {
    if !compression_enabled || data.len() < threshold {
        return Ok((data, false));
    }

    use flate2::Compression;
    use std::io::Write;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&data)?;
    let compressed = encoder.finish()?;

    // Only use compression if it actually saves space
    if compressed.len() < data.len() {
        Ok((Bytes::from(compressed), true))
    } else {
        Ok((data, false))
    }
}

/// Decompresses gzip data if the Content-Encoding header indicates it.
#[cfg(feature = "http")]
fn decompress_if_needed(data: Bytes, content_encoding: Option<&str>) -> anyhow::Result<Bytes> {
    if let Some(encoding) = content_encoding {
        if encoding.to_lowercase().contains("gzip") {
            use std::io::Read;
            let mut decoder = flate2::read::GzDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            return Ok(Bytes::from(decompressed));
        }
    }
    Ok(data)
}

/// Encodes data as base64 for HTTP Basic Authentication.
#[cfg(feature = "http")]
fn base64_encode(data: &[u8]) -> String {
    general_purpose::STANDARD.encode(data)
}

fn configured_basic_auth(basic_auth: Option<&(String, String)>) -> Option<(&str, &str)> {
    basic_auth.and_then(|(username, password)| {
        if username.is_empty() && password.is_empty() {
            None
        } else {
            Some((username.as_str(), password.as_str()))
        }
    })
}

fn basic_auth_header_value(basic_auth: Option<&(String, String)>) -> Option<String> {
    configured_basic_auth(basic_auth).map(|(username, password)| {
        let credentials = format!("{}:{}", username, password);
        format!("Basic {}", base64_encode(credentials.as_bytes()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::{create_consumer_from_route, create_publisher_from_route};
    use crate::models::{Config, Endpoint, EndpointType, StreamBufferConfig};
    use hyper::header::{ACCEPT, CONTENT_TYPE};
    use std::time::Duration;

    fn get_free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }
    fn init_crypto() {
        #[cfg(feature = "rustls-aws-lc")]
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        #[cfg(all(feature = "rustls-ring", not(feature = "rustls-aws-lc")))]
        let _ = rustls::crypto::ring::default_provider().install_default();
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

    #[test]
    fn test_guess_content_type_from_path() {
        assert_eq!(
            guess_content_type("/assets/app.bundle.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(guess_content_type("images/logo.SVG"), "image/svg+xml");
    }

    #[test]
    fn test_guess_content_type_from_extension() {
        assert_eq!(guess_content_type("html"), "text/html; charset=utf-8");
        assert_eq!(guess_content_type(".woff2"), "font/woff2");
        assert_eq!(
            guess_content_type("JSON"),
            "application/json; charset=utf-8"
        );
    }

    #[test]
    fn test_guess_content_type_unknown_defaults_to_octet_stream() {
        assert_eq!(guess_content_type(""), "application/octet-stream");
        assert_eq!(
            guess_content_type("unknown-ext"),
            "application/octet-stream"
        );
        assert_eq!(
            guess_content_type("archive.custombin"),
            "application/octet-stream"
        );
    }

    #[test]
    fn test_request_accepts_text_defaults_true_without_accept_header() {
        let headers = hyper::HeaderMap::new();
        assert!(request_accepts_text(&headers));
    }

    #[test]
    fn test_request_accepts_text_matches_text_and_wildcards() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(ACCEPT, "application/json, text/plain".parse().unwrap());
        assert!(request_accepts_text(&headers));

        headers.insert(ACCEPT, "*/*".parse().unwrap());
        assert!(request_accepts_text(&headers));
    }

    #[test]
    fn test_request_accepts_text_rejects_binary_only_accept_header() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(ACCEPT, "application/octet-stream".parse().unwrap());
        assert!(!request_accepts_text(&headers));
    }

    #[test]
    fn test_text_error_response_sets_text_content_type_when_accepted() {
        let response = text_error_response(StatusCode::BAD_REQUEST, "bad request", true, None);

        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn test_text_error_response_skips_text_content_type_when_not_accepted() {
        let response = text_error_response(StatusCode::BAD_REQUEST, "bad request", false, None);

        assert!(response.headers().get(CONTENT_TYPE).is_none());
    }

    #[test]
    fn test_text_error_response_preserves_custom_content_type() {
        let mut headers = HashMap::new();
        headers.insert(
            "content-type".to_string(),
            "application/problem+json".to_string(),
        );

        let response =
            text_error_response(StatusCode::BAD_REQUEST, "bad request", true, Some(&headers));

        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );
    }

    #[test]
    fn test_basic_auth_header_value_omits_empty_credentials() {
        let empty = (String::new(), String::new());
        assert_eq!(basic_auth_header_value(Some(&empty)), None);
        assert_eq!(basic_auth_header_value(None), None);
    }

    #[test]
    fn test_configured_basic_auth_omits_empty_credentials() {
        let empty = (String::new(), String::new());
        assert_eq!(configured_basic_auth(Some(&empty)), None);
        assert_eq!(configured_basic_auth(None), None);
    }

    #[test]
    fn test_configured_basic_auth_keeps_non_empty_credentials() {
        let creds = ("user".to_string(), "pass".to_string());
        assert_eq!(configured_basic_auth(Some(&creds)), Some(("user", "pass")));
    }

    #[test]
    fn test_basic_auth_header_value_encodes_configured_credentials() {
        let creds = ("user".to_string(), "pass".to_string());
        assert_eq!(
            basic_auth_header_value(Some(&creds)).as_deref(),
            Some("Basic dXNlcjpwYXNz")
        );
    }

    #[tokio::test]
    async fn test_http_consumer_publisher_integration() {
        init_crypto();

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
    async fn test_http_receive_streamable_sse_items_share_correlation_id() {
        init_crypto();

        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        let config = HttpConfig {
            url: addr.clone(),
            receive_streamable: true,
            ..Default::default()
        };

        let mut consumer = HttpConsumer::new(&config)
            .await
            .expect("Failed to create consumer");

        let publisher = HttpPublisher::new(&HttpConfig {
            url,
            ..Default::default()
        })
        .await
        .expect("Failed to create publisher");

        let receive_task = tokio::spawn(async move {
            let first = consumer.receive().await.expect("first stream item");
            let second = consumer.receive().await.expect("second stream item");

            assert_eq!(first.message.get_payload_str(), "first");
            assert_eq!(second.message.get_payload_str(), "second");
            assert_ne!(first.message.message_id, second.message.message_id);

            let first_correlation = first
                .message
                .metadata
                .get("correlation_id")
                .cloned()
                .expect("first correlation_id");
            let second_correlation = second
                .message
                .metadata
                .get("correlation_id")
                .cloned()
                .expect("second correlation_id");
            assert_eq!(first_correlation, second_correlation);
            assert_eq!(
                first
                    .message
                    .metadata
                    .get("http_stream_index")
                    .map(String::as_str),
                Some("0")
            );
            assert_eq!(
                second
                    .message
                    .metadata
                    .get("http_stream_index")
                    .map(String::as_str),
                Some("1")
            );
            assert_eq!(
                second.message.metadata.get("sse_id").map(String::as_str),
                Some("evt-2")
            );
            assert_eq!(
                second.message.metadata.get("sse_event").map(String::as_str),
                Some("update")
            );

            let first_reply = CanonicalMessage::from_vec("reply-first");
            let second_reply = CanonicalMessage::from_vec("reply-second");
            (first.commit)(MessageDisposition::Reply(first_reply))
                .await
                .expect("commit first reply");
            (second.commit)(MessageDisposition::Reply(second_reply))
                .await
                .expect("commit second reply");
        });

        let request =
            CanonicalMessage::from_vec("data: first\n\nid: evt-2\nevent: update\ndata: second\n\n")
                .with_metadata_kv("content-type", "text/event-stream")
                .with_metadata_kv("accept", "text/event-stream")
                .with_metadata_kv("correlation_id", "shared-stream-correlation");

        let response = publisher
            .send(request)
            .await
            .expect("stream request succeeds");
        receive_task.await.expect("receive task finished");

        let response = match response {
            Sent::Response(message) => message,
            Sent::Ack => panic!("expected streamed HTTP response body"),
        };
        let body = response.get_payload_str();
        assert!(body.contains("data: reply-first"));
        assert!(body.contains("data: reply-second"));
        assert_eq!(
            response.metadata.get("content-type").map(String::as_str),
            Some("text/event-stream")
        );
    }

    #[tokio::test]
    async fn test_http_publisher_stream_response_to_sink() {
        init_crypto();

        let port = get_free_port();
        let bind_addr = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&bind_addr)
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept test request");
            let io = TokioIo::new(stream);
            let service = hyper::service::service_fn(|_req: Request<Incoming>| async move {
                let stream = futures::stream::iter(vec![
                    Ok::<_, anyhow::Error>(Frame::data(Bytes::from_static(
                        b"id: one\ndata: alpha\n\n",
                    ))),
                    Ok::<_, anyhow::Error>(Frame::data(Bytes::from_static(
                        b"id: two\nevent: delta\ndata: beta\n\n",
                    ))),
                ]);
                Ok::<_, anyhow::Error>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(streamed(stream))
                        .unwrap(),
                )
            });
            let builder = AutoBuilder::new(TokioExecutor::new());
            builder
                .serve_connection(io, service)
                .await
                .expect("serve test response");
        });

        let sink_endpoint = Endpoint::new_memory(
            &format!("http_stream_sink_{}", fast_uuid_v7::gen_id_str()),
            10,
        );
        let mut sink_consumer = create_consumer_from_route("http_stream_sink", &sink_endpoint)
            .await
            .expect("create stream sink consumer");

        let publisher_endpoint = Endpoint::new(EndpointType::Http(HttpConfig {
            url: format!("http://{}", addr),
            stream_response_to: Some(Box::new(sink_endpoint)),
            ..Default::default()
        }));
        let publisher = create_publisher_from_route("http_stream_publisher", &publisher_endpoint)
            .await
            .expect("create http publisher");

        let sent = publisher
            .send(
                CanonicalMessage::from_vec("prompt")
                    .with_metadata_kv("correlation_id", "llm-stream-1"),
            )
            .await
            .expect("publish request");
        assert!(matches!(sent, Sent::Ack));
        server_task.abort();
        let _ = server_task.await;

        let first = sink_consumer
            .receive()
            .await
            .expect("first streamed response");
        assert_eq!(first.message.get_payload_str(), "alpha");
        assert_eq!(
            first
                .message
                .metadata
                .get("correlation_id")
                .map(String::as_str),
            Some("llm-stream-1")
        );
        assert_eq!(
            first
                .message
                .metadata
                .get("http_stream_index")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            first
                .message
                .metadata
                .get("http_stream_end")
                .map(String::as_str),
            Some("false")
        );
        (first.commit)(MessageDisposition::Ack).await.unwrap();

        let second = sink_consumer
            .receive()
            .await
            .expect("second streamed response");
        assert_eq!(second.message.get_payload_str(), "beta");
        assert_eq!(
            second.message.metadata.get("sse_event").map(String::as_str),
            Some("delta")
        );
        assert_eq!(
            second
                .message
                .metadata
                .get("http_stream_index")
                .map(String::as_str),
            Some("1")
        );
        (second.commit)(MessageDisposition::Ack).await.unwrap();

        let end = sink_consumer.receive().await.expect("stream end marker");
        assert!(end.message.payload.is_empty());
        assert_eq!(
            end.message
                .metadata
                .get("http_stream_end")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            end.message
                .metadata
                .get("http_stream_index")
                .map(String::as_str),
            Some("2")
        );
        (end.commit)(MessageDisposition::Ack).await.unwrap();
    }

    #[tokio::test]
    async fn test_http_publisher_stream_response_to_stream_buffer_isolates_parallel_responses() {
        init_crypto();

        let port = get_free_port();
        let bind_addr = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&bind_addr)
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let server_task = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.expect("accept test request");
                tasks.push(tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = hyper::service::service_fn(|req: Request<Incoming>| async move {
                        let path = req.uri().path().trim_start_matches('/').to_string();
                        let first = format!("data: {}-1\n\n", path);
                        let second = format!("data: {}-2\n\n", path);
                        let stream = futures::stream::iter(vec![
                            Ok::<_, anyhow::Error>(Frame::data(Bytes::from(first))),
                            Ok::<_, anyhow::Error>(Frame::data(Bytes::from(second))),
                        ]);
                        Ok::<_, anyhow::Error>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "text/event-stream")
                                .body(streamed(stream))
                                .unwrap(),
                        )
                    });
                    let builder = AutoBuilder::new(TokioExecutor::new());
                    builder
                        .serve_connection(io, service)
                        .await
                        .expect("serve test response");
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let topic = format!("http_stream_buffer_parallel_{}", fast_uuid_v7::gen_id_str());
        let sink_endpoint = Endpoint::new(EndpointType::StreamBuffer(StreamBufferConfig {
            topic: topic.clone(),
            correlation_id: None,
            capacity: Some(20),
        }));
        let mut consumer_a = create_consumer_from_route(
            "http_stream_buffer_a",
            &Endpoint::new(EndpointType::StreamBuffer(StreamBufferConfig {
                topic: topic.clone(),
                correlation_id: Some("stream-a".to_string()),
                capacity: Some(20),
            })),
        )
        .await
        .expect("create stream-a consumer");
        let mut consumer_b = create_consumer_from_route(
            "http_stream_buffer_b",
            &Endpoint::new(EndpointType::StreamBuffer(StreamBufferConfig {
                topic: topic.clone(),
                correlation_id: Some("stream-b".to_string()),
                capacity: Some(20),
            })),
        )
        .await
        .expect("create stream-b consumer");

        let publisher_endpoint = Endpoint::new(EndpointType::Http(HttpConfig {
            url: format!("http://{}", addr),
            stream_response_to: Some(Box::new(sink_endpoint)),
            ..Default::default()
        }));
        let publisher: std::sync::Arc<dyn MessagePublisher> =
            create_publisher_from_route("http_stream_buffer_publisher", &publisher_endpoint)
                .await
                .expect("create http publisher");

        let send_a = {
            let publisher = publisher.clone();
            tokio::spawn(async move {
                publisher
                    .send(
                        CanonicalMessage::from_vec("prompt-a")
                            .with_metadata_kv("http_path", "/a")
                            .with_metadata_kv("correlation_id", "stream-a"),
                    )
                    .await
                    .expect("send stream-a")
            })
        };
        let send_b = {
            let publisher = publisher.clone();
            tokio::spawn(async move {
                publisher
                    .send(
                        CanonicalMessage::from_vec("prompt-b")
                            .with_metadata_kv("http_path", "/b")
                            .with_metadata_kv("correlation_id", "stream-b"),
                    )
                    .await
                    .expect("send stream-b")
            })
        };

        assert!(matches!(send_a.await.expect("join stream-a"), Sent::Ack));
        assert!(matches!(send_b.await.expect("join stream-b"), Sent::Ack));
        server_task.abort();
        let _ = server_task.await;

        let mut stream_a_payloads = Vec::new();
        loop {
            let received = consumer_a.receive().await.expect("stream-a item");
            let is_end = received
                .message
                .metadata
                .get("http_stream_end")
                .is_some_and(|value| value == "true");
            assert_eq!(
                received
                    .message
                    .metadata
                    .get("correlation_id")
                    .map(String::as_str),
                Some("stream-a")
            );
            if !is_end {
                stream_a_payloads.push(received.message.get_payload_str().to_string());
            }
            (received.commit)(MessageDisposition::Ack).await.unwrap();
            if is_end {
                break;
            }
        }

        let mut stream_b_payloads = Vec::new();
        loop {
            let received = consumer_b.receive().await.expect("stream-b item");
            let is_end = received
                .message
                .metadata
                .get("http_stream_end")
                .is_some_and(|value| value == "true");
            assert_eq!(
                received
                    .message
                    .metadata
                    .get("correlation_id")
                    .map(String::as_str),
                Some("stream-b")
            );
            if !is_end {
                stream_b_payloads.push(received.message.get_payload_str().to_string());
            }
            (received.commit)(MessageDisposition::Ack).await.unwrap();
            if is_end {
                break;
            }
        }

        assert_eq!(stream_a_payloads, vec!["a-1", "a-2"]);
        assert_eq!(stream_b_payloads, vec!["b-1", "b-2"]);
    }

    #[tokio::test]
    async fn test_http_publisher_stream_response_to_stream_buffer_uses_message_id_fallback() {
        init_crypto();

        let port = get_free_port();
        let bind_addr = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&bind_addr)
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept test request");
            let io = TokioIo::new(stream);
            let service = hyper::service::service_fn(|_req: Request<Incoming>| async move {
                let stream = futures::stream::iter(vec![Ok::<_, anyhow::Error>(Frame::data(
                    Bytes::from_static(b"data: fallback\n\n"),
                ))]);
                Ok::<_, anyhow::Error>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(streamed(stream))
                        .unwrap(),
                )
            });
            let builder = AutoBuilder::new(TokioExecutor::new());
            builder
                .serve_connection(io, service)
                .await
                .expect("serve test response");
        });

        let topic = format!("http_stream_buffer_fallback_{}", fast_uuid_v7::gen_id_str());
        let sink_endpoint = Endpoint::new(EndpointType::StreamBuffer(StreamBufferConfig {
            topic: topic.clone(),
            correlation_id: None,
            capacity: Some(10),
        }));
        let publisher_endpoint = Endpoint::new(EndpointType::Http(HttpConfig {
            url: format!("http://{}", addr),
            stream_response_to: Some(Box::new(sink_endpoint)),
            ..Default::default()
        }));
        let publisher =
            create_publisher_from_route("http_stream_fallback_publisher", &publisher_endpoint)
                .await
                .expect("create http publisher");

        let request = CanonicalMessage::from_vec("prompt");
        let expected_correlation_id = format!("{:032x}", request.message_id);
        let mut consumer = create_consumer_from_route(
            "http_stream_fallback_consumer",
            &Endpoint::new(EndpointType::StreamBuffer(StreamBufferConfig {
                topic: topic.clone(),
                correlation_id: Some(expected_correlation_id.clone()),
                capacity: Some(10),
            })),
        )
        .await
        .expect("create fallback consumer");

        let sent = publisher.send(request).await.expect("send request");
        assert!(matches!(sent, Sent::Ack));
        server_task.abort();
        let _ = server_task.await;

        let item = consumer.receive().await.expect("fallback stream item");
        assert_eq!(item.message.get_payload_str(), "fallback");
        assert_eq!(
            item.message
                .metadata
                .get("correlation_id")
                .map(String::as_str),
            Some(expected_correlation_id.as_str())
        );
        (item.commit)(MessageDisposition::Ack).await.unwrap();

        let end = consumer.receive().await.expect("fallback end marker");
        assert_eq!(
            end.message
                .metadata
                .get("http_stream_end")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            end.message
                .metadata
                .get("correlation_id")
                .map(String::as_str),
            Some(expected_correlation_id.as_str())
        );
        (end.commit)(MessageDisposition::Ack).await.unwrap();
    }

    #[tokio::test]
    async fn test_http_server_shutdown_on_drop() {
        init_crypto();
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
        init_crypto();
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
        init_crypto();
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
    async fn test_http_route_inline_response_does_not_echo_unchanged_request_headers() {
        init_crypto();
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);

        let input = Endpoint::new(EndpointType::Http(HttpConfig {
            url: addr.clone(),
            path: Some("/inline".to_string()),
            ..Default::default()
        }));
        let output = Endpoint::new(EndpointType::Response(
            crate::models::ResponseConfig::default(),
        ));

        let route = crate::Route::new(input, output);
        let handle = route.run("test_http_inline_fast_path").await.unwrap();

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector);
        let request = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{addr}/inline"))
            .header("content-type", "application/json")
            .header("accept", "application/octet-stream")
            .header("x-request-id", "req-123")
            .body(http_body_util::Full::<Bytes>::new(Bytes::from_static(
                br#"{"value":1}"#,
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
        assert!(response.headers().get("x-request-id").is_none());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"value":1}"#));

        handle.stop().await;
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn test_http_route_inline_response_can_be_disabled() {
        init_crypto();
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);

        let input = Endpoint::new(EndpointType::Http(HttpConfig {
            url: addr.clone(),
            path: Some("/inline-disabled".to_string()),
            inline_response_fast_path: Some(false),
            ..Default::default()
        }));
        let output = Endpoint::new(EndpointType::Response(
            crate::models::ResponseConfig::default(),
        ));

        let route = crate::Route::new(input, output);
        let handle = route
            .run("test_http_inline_fast_path_disabled")
            .await
            .unwrap();

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector);
        let request = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{addr}/inline-disabled"))
            .header("content-type", "application/json")
            .header("accept", "application/octet-stream")
            .header("x-request-id", "req-123")
            .body(http_body_util::Full::<Bytes>::new(Bytes::from_static(
                br#"{"value":1}"#,
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(response.headers().get("x-request-id").unwrap(), "req-123");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"value":1}"#));

        handle.stop().await;
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn test_http_route_handler_response_uses_inline_path() {
        use crate::traits::Handled;

        init_crypto();
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);

        let input = Endpoint::new(EndpointType::Http(HttpConfig {
            url: addr.clone(),
            path: Some("/handler".to_string()),
            ..Default::default()
        }));
        let mut output = Endpoint::new(EndpointType::Response(
            crate::models::ResponseConfig::default(),
        ));
        let handler = |mut msg: CanonicalMessage| async move {
            msg.payload = Bytes::from_static(b"handled-response");
            msg.metadata
                .insert("content-type".to_string(), "text/plain".to_string());
            msg.metadata
                .insert("x-response-id".to_string(), "resp-1".to_string());
            msg.metadata
                .insert("http_status_code".to_string(), "201".to_string());
            Ok(Handled::Publish(msg))
        };
        output.handler = Some(std::sync::Arc::new(handler));

        let route = crate::Route::new(input, output);
        let handle = route.run("test_http_inline_handler_path").await.unwrap();

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector);
        let request = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{addr}/handler"))
            .header("content-type", "application/json")
            .header("accept", "application/octet-stream")
            .header("x-request-id", "req-123")
            .body(http_body_util::Full::<Bytes>::new(Bytes::from_static(
                br#"{"value":1}"#,
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
        assert_eq!(response.headers().get("x-response-id").unwrap(), "resp-1");
        assert!(response.headers().get("x-request-id").is_none());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"handled-response"));

        handle.stop().await;
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn test_http_route_handler_with_buffer_uses_inline_path() {
        use crate::models::{BufferMiddleware, Middleware};
        use crate::traits::Handled;

        init_crypto();
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);

        let input = Endpoint::new(EndpointType::Http(HttpConfig {
            url: addr.clone(),
            path: Some("/handler-buffered".to_string()),
            ..Default::default()
        }));
        let mut output = Endpoint::new(EndpointType::Response(
            crate::models::ResponseConfig::default(),
        ));
        output
            .middlewares
            .push(Middleware::Buffer(BufferMiddleware {
                max_messages: 16,
                max_delay_ms: 0,
            }));
        let handler = |mut msg: CanonicalMessage| async move {
            msg.payload = Bytes::from_static(b"handled-buffered");
            msg.metadata
                .insert("content-type".to_string(), "text/plain".to_string());
            Ok(Handled::Publish(msg))
        };
        output.handler = Some(std::sync::Arc::new(handler));

        let route = crate::Route::new(input, output);
        let handle = route
            .run("test_http_inline_handler_buffer_path")
            .await
            .unwrap();

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector);
        let request = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{addr}/handler-buffered"))
            .header("content-type", "application/json")
            .header("accept", "application/octet-stream")
            .header("x-request-id", "req-123")
            .body(http_body_util::Full::<Bytes>::new(Bytes::from_static(
                br#"{"value":1}"#,
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
        assert!(response.headers().get("x-request-id").is_none());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"handled-buffered"));

        handle.stop().await;
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn test_http_streamable_route_handler_uses_inline_path() {
        use crate::traits::Handled;

        init_crypto();
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);

        let input = Endpoint::new(EndpointType::Http(HttpConfig {
            url: addr.clone(),
            path: Some("/handler-stream".to_string()),
            receive_streamable: true,
            ..Default::default()
        }));
        let mut output = Endpoint::new(EndpointType::Response(
            crate::models::ResponseConfig::default(),
        ));
        let handler = |mut msg: CanonicalMessage| async move {
            let payload = msg.get_payload_str();
            msg.set_payload_str(format!("reply-{payload}"));
            Ok(Handled::Publish(msg))
        };
        output.handler = Some(std::sync::Arc::new(handler));

        let route = crate::Route::new(input, output);
        let handle = route
            .run("test_http_inline_streamable_handler_path")
            .await
            .unwrap();

        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector);
        let request = Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{addr}/handler-stream"))
            .header("content-type", "application/x-ndjson")
            .header("accept", "application/x-ndjson")
            .body(http_body_util::Full::<Bytes>::new(Bytes::from_static(
                b"first\nsecond\n",
            )))
            .unwrap();
        let response = client.request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/x-ndjson"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"reply-first\nreply-second\n"));

        handle.stop().await;
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn test_http_reply_with_custom_status_code() {
        use crate::traits::Handled;
        init_crypto();

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

    #[tokio::test]
    async fn test_http_consumers_share_listener_by_path() {
        init_crypto();

        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let url = format!("http://{}", addr);

        let mut alpha_consumer = HttpConsumer::new(&HttpConfig {
            url: addr.clone(),
            path: Some("/alpha".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

        let mut beta_consumer = HttpConsumer::new(&HttpConfig {
            url: addr.clone(),
            path: Some("/beta".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

        let publisher = HttpPublisher::new(&HttpConfig {
            url,
            ..Default::default()
        })
        .await
        .unwrap();

        let alpha_task = tokio::spawn(async move {
            let received = consumer_receive_ack(&mut alpha_consumer).await;
            received.payload
        });
        let beta_task = tokio::spawn(async move {
            let received = consumer_receive_ack(&mut beta_consumer).await;
            received.payload
        });

        let mut alpha_message = CanonicalMessage::new(b"alpha".to_vec(), None);
        alpha_message
            .metadata
            .insert("http_path".to_string(), "/alpha".to_string());
        let mut beta_message = CanonicalMessage::new(b"beta".to_vec(), None);
        beta_message
            .metadata
            .insert("http_path".to_string(), "/beta".to_string());

        publisher.send(alpha_message).await.unwrap();
        publisher.send(beta_message).await.unwrap();

        assert_eq!(alpha_task.await.unwrap(), b"alpha".to_vec());
        assert_eq!(beta_task.await.unwrap(), b"beta".to_vec());
    }

    #[tokio::test]
    async fn test_http_consumer_rejects_duplicate_path_registration() {
        init_crypto();

        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);

        let _consumer = HttpConsumer::new(&HttpConfig {
            url: addr.clone(),
            path: Some("/shared".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

        let error = HttpConsumer::new(&HttpConfig {
            url: addr,
            path: Some("/shared".to_string()),
            ..Default::default()
        })
        .await
        .err()
        .expect("duplicate registration should fail");

        assert!(
            error
                .to_string()
                .contains("Conflicting HTTP consumer registration"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn test_http_consumers_on_ephemeral_ports_do_not_share_listener() {
        init_crypto();

        let first_consumer = HttpConsumer::new(&HttpConfig {
            url: "127.0.0.1:0".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        let second_consumer = HttpConsumer::new(&HttpConfig {
            url: "127.0.0.1:0".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        let first_addr = first_consumer.bound_addr().unwrap();
        let second_addr = second_consumer.bound_addr().unwrap();

        assert_ne!(first_addr, second_addr);
        assert_ne!(first_addr.port(), 0);
        assert_ne!(second_addr.port(), 0);
    }

    async fn consumer_receive_ack(consumer: &mut HttpConsumer) -> CanonicalMessage {
        let received = consumer.receive().await.unwrap();
        let message = received.message.clone();
        (received.commit)(crate::traits::MessageDisposition::Ack)
            .await
            .unwrap();
        message
    }
}
