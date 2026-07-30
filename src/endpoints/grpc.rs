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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("mqbridge");
}

use proto::bridge_client::BridgeClient;
use proto::{BridgeMessage, SubscribeRequest};
use tonic::Request;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server as TonicServer;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::{Response, Status};

const GRPC_BATCH_POLL_MS: u64 = 15; // Increased for better batching in performance tests, and to reduce "poll timed out" warnings

// ── Consumer ──────────────────────────────────────────────────────────────────

pub struct GrpcConsumer {
    inner: GrpcConsumerInner,
    url: String,
    bound_addr: Option<std::net::SocketAddr>,
}

enum GrpcConsumerInner {
    Client(Box<ClientModeConsumer>),
    Server(ServerModeConsumer),
}

impl GrpcConsumer {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let url = config.tls.normalize_url(&config.url);
        let (inner, bound_addr) = if config.server_mode {
            let s = ServerModeConsumer::new(config, &url).await?;
            let addr = s.bound_addr();
            (GrpcConsumerInner::Server(s), Some(addr))
        } else {
            (
                GrpcConsumerInner::Client(Box::new(ClientModeConsumer::new(config, &url).await?)),
                None,
            )
        };
        Ok(Self {
            inner,
            url,
            bound_addr,
        })
    }

    /// True when `receive_batch` is cancel-safe. Server mode is mpsc-backed (a
    /// dropped read consumes nothing); client mode reads a tonic stream directly,
    /// where a cancelled `message()` may drop an in-flight frame.
    pub(crate) fn is_cancel_safe(&self) -> bool {
        matches!(self.inner, GrpcConsumerInner::Server(_))
    }
}

#[async_trait]
impl MessageConsumer for GrpcConsumer {
    // Both modes use no-op commits (client mode subscribes to a server stream with
    // no ack; server mode replies per call), so commits are order-independent.
    fn commit_requires_order(&self) -> bool {
        false
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        match &mut self.inner {
            GrpcConsumerInner::Client(c) => c.set_exit_on_empty(exit_on_empty),
            GrpcConsumerInner::Server(s) => s.set_exit_on_empty(exit_on_empty),
        }
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        match &mut self.inner {
            GrpcConsumerInner::Client(c) => c.receive_batch(max_messages).await,
            GrpcConsumerInner::Server(s) => s.receive_batch(max_messages).await,
        }
    }

    async fn status(&self) -> crate::traits::EndpointStatus {
        // Server mode: healthy as long as the embedded server task is still
        // running. Client mode: a tonic client stream has no cheap liveness
        // probe, so report healthy and leave verification to the next receive.
        let (healthy, details) = match &self.inner {
            GrpcConsumerInner::Server(s) => (
                !s.shared_server.handle.is_finished(),
                serde_json::json!({ "mode": "server", "bound_addr": self.bound_addr }),
            ),
            GrpcConsumerInner::Client(_) => (true, serde_json::json!({ "mode": "client" })),
        };
        crate::traits::EndpointStatus {
            healthy,
            target: self.url.clone(),
            error: if healthy {
                None
            } else {
                Some("gRPC server task stopped".to_string())
            },
            details,
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ClientModeConsumer {
    _client: BridgeClient<Channel>,
    stream: tonic::Streaming<BridgeMessage>,
    /// Drain mode: only then does an idle first-message read time out into an empty batch.
    exit_on_empty: bool,
}

impl ClientModeConsumer {
    async fn new(config: &GrpcConfig, url: &str) -> Result<Self> {
        debug!(grpc_url = %url, "Creating gRPC client consumer (client mode)");
        let endpoint = make_endpoint(config, url).await?;
        let mut client = BridgeClient::new(endpoint.connect().await?);
        let topic = config
            .topic
            .clone()
            .unwrap_or_else(|| "default".to_string());
        debug!(grpc_url = %config.url, subscribe_topic = %topic, "gRPC client consumer subscribing to topic");
        let request = Request::new(SubscribeRequest { topic });
        let stream = if let Some(ms) = config.timeout_ms {
            tokio::time::timeout(Duration::from_millis(ms), client.subscribe(request))
                .await
                .map_err(|_| anyhow::anyhow!("gRPC subscribe timed out"))??
        } else {
            client.subscribe(request).await?
        }
        .into_inner();
        info!(grpc_url = %url, "gRPC client consumer connected and subscription started");
        Ok(Self {
            _client: client,
            stream,
            exit_on_empty: false,
        })
    }
}

#[async_trait]
impl MessageConsumer for ClientModeConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }
    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        receive_from_stream(&mut self.stream, max_messages, self.exit_on_empty).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Reads a batch from a tonic server-streaming response.
/// Blocks on the first message; polls briefly for subsequent ones to fill the batch.
async fn receive_from_stream(
    stream: &mut tonic::Streaming<BridgeMessage>,
    max_messages: usize,
    exit_on_empty: bool,
) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
    let mut messages = Vec::with_capacity(max_messages);
    loop {
        let result = if messages.is_empty() {
            // Drain mode: a brief idle timeout on the first message yields an empty batch.
            match crate::traits::drain_gated(exit_on_empty, stream.message()).await {
                Some(r) => Ok(r),
                None => return Ok(crate::outcomes::ReceivedBatch::empty()),
            }
        } else {
            tokio::time::timeout(Duration::from_millis(GRPC_BATCH_POLL_MS), stream.message()).await
        };
        match result {
            Ok(Ok(Some(msg))) => {
                messages.push(bridge_to_canonical(msg));
                if messages.len() >= max_messages {
                    break;
                }
            }
            Ok(Ok(None)) => {
                trace!("gRPC stream closed by server (None)");
                break;
            }
            Err(_) => {
                trace!("gRPC stream poll timed out while filling batch (normal exit)");
                break;
            }
            Ok(Err(e)) => {
                error!("gRPC stream returned error while receiving: {:?}", e);
                return Err(ConsumerError::Connection(e.into()));
            }
        }
    }
    if messages.is_empty() {
        Err(ConsumerError::EndOfStream)
    } else {
        Ok(crate::outcomes::ReceivedBatch {
            messages,
            commit: Box::new(|_| Box::pin(async { Ok(()) })),
        })
    }
}

// ── Embedded gRPC server (server_mode) ────────────────────────────────────────

struct ServerModeConsumer {
    route_id: u64,
    shared_server: Arc<SharedGrpcServer>,
    bound_addr: std::net::SocketAddr,
    // One receive channel per shard; publishes are spread round-robin across the
    // shards so many concurrent producers don't all contend on one channel.
    rxs: Vec<mpsc::Receiver<BridgeMessage>>,
    // Round-robin cursor for the next shard to drain first, so none starves.
    drain_start: usize,
    /// Drain mode: only then does an idle first-message poll time out into an empty batch.
    exit_on_empty: bool,
}

/// Tonic service implementation that fans incoming messages into a subscriber
/// broadcast stream and a reliable internal queue for the server-mode consumer.
struct BridgeService {
    router: Arc<SharedGrpcRouter>,
}

struct SharedGrpcRouter {
    // RwLock (not Mutex): `dispatch` only reads the table, so concurrent publishes
    // no longer serialize against each other on the lock.
    routes: RwLock<HashMap<u64, SharedGrpcRoute>>,
}

#[derive(Clone)]
struct SharedGrpcRoute {
    topic: String,
    // Sharded senders; `cursor` round-robins publishes across them. `cursor` is
    // shared (Arc) so all clones of this route advance the same counter.
    txs: Vec<mpsc::Sender<BridgeMessage>>,
    cursor: Arc<AtomicUsize>,
    broadcast_tx: broadcast::Sender<BridgeMessage>,
}

struct SharedGrpcServer {
    router: Arc<SharedGrpcRouter>,
    handle: tokio::task::JoinHandle<()>,
    bound_addr: std::net::SocketAddr,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct GrpcServerKey {
    listen_addr: String,
    tls: crate::models::TlsConfig,
    timeout_ms: Option<u64>,
    initial_stream_window_size: Option<u32>,
    initial_connection_window_size: Option<u32>,
    concurrency_limit_per_connection: Option<usize>,
    http2_keepalive_interval_ms: Option<u64>,
    http2_keepalive_timeout_ms: Option<u64>,
    max_decoding_message_size: Option<usize>,
}

static GRPC_SERVER_REGISTRY: OnceLock<Mutex<HashMap<GrpcServerKey, Arc<SharedGrpcServer>>>> =
    OnceLock::new();
static GRPC_ROUTE_ID: AtomicU64 = AtomicU64::new(1);

fn grpc_server_registry() -> &'static Mutex<HashMap<GrpcServerKey, Arc<SharedGrpcServer>>> {
    GRPC_SERVER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_grpc_topic(topic: Option<&str>) -> String {
    topic
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .unwrap_or("default")
        .to_string()
}

impl SharedGrpcRouter {
    fn new() -> Self {
        Self {
            routes: RwLock::new(HashMap::new()),
        }
    }
}

fn bridge_message_topic(msg: &BridgeMessage) -> String {
    normalize_grpc_topic(msg.metadata.get("mq_bridge.topic").map(String::as_str))
}

impl SharedGrpcRouter {
    fn register_route(
        &self,
        route_id: u64,
        topic: String,
        txs: Vec<mpsc::Sender<BridgeMessage>>,
    ) -> Result<()> {
        let mut routes = self
            .routes
            .write()
            .map_err(|_| anyhow::anyhow!("gRPC route registry lock poisoned"))?;
        if routes.values().any(|route| route.topic == topic) {
            return Err(anyhow::anyhow!(
                "Conflicting gRPC consumer registration for topic '{}'",
                topic
            ));
        }
        let (broadcast_tx, _) = broadcast::channel(1024);
        routes.insert(
            route_id,
            SharedGrpcRoute {
                topic,
                txs,
                cursor: Arc::new(AtomicUsize::new(0)),
                broadcast_tx,
            },
        );
        Ok(())
    }

    fn unregister_route(&self, route_id: u64) -> bool {
        let Ok(mut routes) = self.routes.write() else {
            return false;
        };
        routes.remove(&route_id);
        routes.is_empty()
    }

    fn route_for_topic(&self, topic: &str) -> Option<SharedGrpcRoute> {
        let Ok(routes) = self.routes.read() else {
            return None;
        };
        routes.values().find(|route| route.topic == topic).cloned()
    }

    fn subscribe_to_topic(&self, topic: &str) -> Option<broadcast::Receiver<BridgeMessage>> {
        self.route_for_topic(topic)
            .map(|route| route.broadcast_tx.subscribe())
    }

    async fn dispatch(&self, msg: BridgeMessage) -> Result<()> {
        let topic = bridge_message_topic(&msg);
        let route = self
            .route_for_topic(&topic)
            .ok_or_else(|| anyhow::anyhow!("No route for topic '{}'", topic))?;
        // Only clone for the broadcast stream when someone is actually subscribed.
        if route.broadcast_tx.receiver_count() > 0 {
            let _ = route.broadcast_tx.send(msg.clone());
        }
        let shard = route.cursor.fetch_add(1, Ordering::Relaxed) % route.txs.len();
        route.txs[shard]
            .send(msg)
            .await
            .map_err(|_| anyhow::anyhow!("No active gRPC consumer for topic '{}'", topic))?;
        Ok(())
    }
}

#[tonic::async_trait]
impl proto::bridge_server::Bridge for BridgeService {
    async fn publish(
        &self,
        request: Request<BridgeMessage>,
    ) -> Result<Response<proto::PublishResponse>, Status> {
        let msg = request.into_inner();
        let msg_id = msg.id.clone();
        let topic = bridge_message_topic(&msg);
        trace!(msg_id = %msg_id, topic = %topic, "BridgeService::publish received message");
        if self.router.dispatch(msg).await.is_err() {
            warn!(msg_id = %msg_id, topic = %topic, "BridgeService::publish failed: internal server queue is closed");
            return Ok(Response::new(proto::PublishResponse {
                result: Some(proto::publish_response::Result::Ack(proto::Ack {
                    id: msg_id,
                    status: 1, // NACK
                    reason: "Internal queue closed".to_string(),
                    metadata: Default::default(),
                })),
            }));
        }
        Ok(Response::new(proto::PublishResponse {
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
        // Minimal implementation: accept the ack and return success.
        let ack = request.into_inner();
        trace!(ack_id = %ack.id, "BridgeService::acknowledge received ack");
        Ok(Response::new(proto::AckResponse {
            success: true,
            error: String::new(),
        }))
    }

    type PublishBatchStream = ReceiverStream<Result<proto::PublishResponse, Status>>;

    async fn publish_batch(
        &self,
        request: Request<tonic::Streaming<BridgeMessage>>,
    ) -> Result<Response<Self::PublishBatchStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let router = self.router.clone();

        tokio::spawn(async move {
            while let Ok(Some(msg)) = stream.message().await {
                let msg_id = msg.id.clone();
                let topic = bridge_message_topic(&msg);
                trace!(msg_id = %msg_id, topic = %topic, "BridgeService::publish_batch received message");
                if router.dispatch(msg).await.is_err() {
                    warn!("publish_batch: internal server queue closed, stopping responder task");
                    // Send an explicit NACK for this message so clients receive a terminal response
                    let nack = proto::PublishResponse {
                        result: Some(proto::publish_response::Result::Ack(proto::Ack {
                            id: msg_id.clone(),
                            status: 1,
                            reason: "Internal queue closed".to_string(),
                            metadata: Default::default(),
                        })),
                    };
                    let _ = tx.send(Ok(nack)).await;
                    break;
                }
                let resp = proto::PublishResponse {
                    result: Some(proto::publish_response::Result::Ack(proto::Ack {
                        id: msg_id,
                        status: 0,
                        reason: String::new(),
                        metadata: Default::default(),
                    })),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    warn!("publish_batch: client stream closed, stopping responder task");
                    break;
                }
            }
            trace!("publish_batch responder task exiting");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeStream = ReceiverStream<Result<BridgeMessage, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let topic = normalize_grpc_topic(Some(request.into_inner().topic.as_str()));
        let mut rx = self
            .router
            .subscribe_to_topic(&topic)
            .ok_or_else(|| Status::not_found(format!("No active gRPC topic '{}'", topic)))?;
        let (tx_stream, rx_stream) = mpsc::channel(32);
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        if tx_stream.send(Ok(msg)).await.is_err() {
                            warn!("subscribe: downstream consumer disconnected");
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

impl ServerModeConsumer {
    async fn new(config: &GrpcConfig, url: &str) -> Result<Self> {
        let key = GrpcServerKey {
            listen_addr: parse_addr(url)?.to_string(),
            tls: config.tls.clone(),
            timeout_ms: config.timeout_ms,
            initial_stream_window_size: config.initial_stream_window_size,
            initial_connection_window_size: config.initial_connection_window_size,
            concurrency_limit_per_connection: config.concurrency_limit_per_connection,
            http2_keepalive_interval_ms: config.http2_keepalive_interval_ms,
            http2_keepalive_timeout_ms: config.http2_keepalive_timeout_ms,
            max_decoding_message_size: config.max_decoding_message_size,
        };
        let topic = normalize_grpc_topic(config.topic.as_deref());
        // Total queue depth stays ~16k, split across shards to cut producer contention.
        let shard_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .clamp(1, 16);
        let per_shard = ((16 * 1024) / shard_count).max(1);
        let mut txs = Vec::with_capacity(shard_count);
        let mut rxs = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = mpsc::channel(per_shard);
            txs.push(tx);
            rxs.push(rx);
        }
        let route_id = GRPC_ROUTE_ID.fetch_add(1, Ordering::Relaxed);
        let shared_server =
            get_or_create_shared_grpc_server(config, &key, route_id, topic, txs).await?;

        Ok(Self {
            route_id,
            bound_addr: shared_server.bound_addr,
            shared_server,
            rxs,
            drain_start: 0,
            exit_on_empty: false,
        })
    }

    fn bound_addr(&self) -> std::net::SocketAddr {
        self.bound_addr
    }
}

async fn get_or_create_shared_grpc_server(
    config: &GrpcConfig,
    key: &GrpcServerKey,
    route_id: u64,
    topic: String,
    txs: Vec<mpsc::Sender<BridgeMessage>>,
) -> Result<Arc<SharedGrpcServer>> {
    if let Ok(registry) = grpc_server_registry().lock() {
        for (existing_key, server) in registry.iter() {
            if existing_key.listen_addr != key.listen_addr {
                continue;
            }
            if existing_key == key {
                server
                    .router
                    .register_route(route_id, topic.clone(), txs.clone())?;
                return Ok(server.clone());
            }
            return Err(anyhow::anyhow!(
                "gRPC consumer {} is already registered with different server settings",
                key.listen_addr
            ));
        }
    }

    let addr = parse_addr(&key.listen_addr)?;
    let router = Arc::new(SharedGrpcRouter::new());
    let mut builder = TonicServer::builder();
    if let Some(v) = config.initial_stream_window_size {
        builder = builder.initial_stream_window_size(v);
    }
    if let Some(v) = config.initial_connection_window_size {
        builder = builder.initial_connection_window_size(v);
    }
    if let Some(v) = config.concurrency_limit_per_connection {
        builder = builder.concurrency_limit_per_connection(v);
    }
    if let Some(ms) = config.http2_keepalive_interval_ms {
        builder = builder.http2_keepalive_interval(Some(Duration::from_millis(ms)));
    }
    if let Some(ms) = config.http2_keepalive_timeout_ms {
        builder = builder.http2_keepalive_timeout(Some(Duration::from_millis(ms)));
    }
    if let Some(ms) = config.timeout_ms {
        builder = builder.timeout(Duration::from_millis(ms));
    }

    if config.tls.required {
        if !config.tls.is_tls_server_configured() {
            return Err(anyhow::anyhow!(
                "gRPC server TLS enabled but no cert/key provided in GrpcConfig"
            ));
        }
        let cert_path = config.tls.cert_file.as_ref().unwrap();
        let key_path = config.tls.key_file.as_ref().unwrap();
        let cert = tokio::fs::read(cert_path).await?;
        let key = tokio::fs::read(key_path).await?;
        let identity = Identity::from_pem(cert, key);

        let mut tls_config = ServerTlsConfig::new().identity(identity);
        if let Some(ca_path) = &config.tls.ca_file {
            let ca_pem = tokio::fs::read(ca_path).await?;
            let ca_cert = Certificate::from_pem(ca_pem);
            tls_config = tls_config.client_ca_root(ca_cert);
        }

        builder = builder.tls_config(tls_config)?;
    }

    let mut service = proto::bridge_server::BridgeServer::new(BridgeService {
        router: router.clone(),
    });
    if let Some(max) = config.max_decoding_message_size {
        service = service.max_decoding_message_size(max);
    }

    // Bind the TCP listener first so we know the server port is bound and
    // listening before returning. This avoids races where the consumer
    // tries to connect before the server is ready.
    info!(addr = %addr, "Binding gRPC embedded server listener");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    info!(server_addr = %local, "gRPC embedded server listener bound");
    let incoming = TcpListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        info!(server_addr = %local, "gRPC embedded server starting to serve");
        if let Err(e) = builder.serve_with_incoming(service, incoming).await {
            error!(server_addr = %local, "gRPC server error: {:?}", e);
        }
        info!(server_addr = %local, "gRPC embedded server stopped");
    });

    let server = Arc::new(SharedGrpcServer {
        router,
        handle,
        bound_addr: local,
    });

    let mut registry = grpc_server_registry()
        .lock()
        .map_err(|_| anyhow::anyhow!("gRPC server registry lock poisoned"))?;
    for (existing_key, existing) in registry.iter() {
        if existing_key.listen_addr != key.listen_addr {
            continue;
        }
        if existing_key == key {
            server.handle.abort();
            existing
                .router
                .register_route(route_id, topic.clone(), txs.clone())?;
            return Ok(existing.clone());
        }
        server.handle.abort();
        return Err(anyhow::anyhow!(
            "gRPC consumer {} is already registered with different server settings",
            key.listen_addr
        ));
    }
    server.router.register_route(route_id, topic, txs)?;
    registry.insert(key.clone(), server.clone());
    Ok(server)
}

impl Drop for ServerModeConsumer {
    fn drop(&mut self) {
        let Ok(mut registry) = grpc_server_registry().lock() else {
            return;
        };
        let should_shutdown = self.shared_server.router.unregister_route(self.route_id);
        if !should_shutdown {
            return;
        }

        registry.retain(|_, server| !Arc::ptr_eq(server, &self.shared_server));
        self.shared_server.handle.abort();
    }
}

#[async_trait]
impl MessageConsumer for ServerModeConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }
    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        let max_messages = max_messages.max(1);
        let shard_count = self.rxs.len();
        let mut messages = Vec::with_capacity(max_messages);
        'fill: loop {
            // Greedily sweep all shards for whatever is immediately available.
            let mut got_any = false;
            for offset in 0..shard_count {
                let idx = (self.drain_start + offset) % shard_count;
                if let Ok(msg) = self.rxs[idx].try_recv() {
                    messages.push(bridge_to_canonical(msg));
                    got_any = true;
                    if messages.len() >= max_messages {
                        break 'fill;
                    }
                }
            }
            if got_any {
                self.drain_start = (self.drain_start + 1) % shard_count;
                continue;
            }

            // Nothing buffered: block for the first message, then linger briefly for
            // more. Polling every shard registers our waker on each.
            let start = self.drain_start;
            let poll = std::future::poll_fn(|cx| {
                let mut all_closed = true;
                for offset in 0..shard_count {
                    let idx = (start + offset) % shard_count;
                    match self.rxs[idx].poll_recv(cx) {
                        std::task::Poll::Ready(Some(msg)) => {
                            self.drain_start = (idx + 1) % shard_count;
                            return std::task::Poll::Ready(Some(msg));
                        }
                        std::task::Poll::Ready(None) => {}
                        std::task::Poll::Pending => all_closed = false,
                    }
                }
                if all_closed {
                    std::task::Poll::Ready(None)
                } else {
                    std::task::Poll::Pending
                }
            });
            let next = if messages.is_empty() {
                // Drain mode: a brief idle timeout on the first message yields an empty batch.
                match crate::traits::drain_gated(self.exit_on_empty, poll).await {
                    Some(value) => value,
                    None => return Ok(crate::outcomes::ReceivedBatch::empty()),
                }
            } else {
                match tokio::time::timeout(Duration::from_millis(GRPC_BATCH_POLL_MS), poll).await {
                    Ok(value) => value,
                    Err(_) => break, // linger window closed
                }
            };
            match next {
                Some(msg) => messages.push(bridge_to_canonical(msg)),
                None => break, // every shard closed
            }
        }
        if messages.is_empty() {
            Err(ConsumerError::EndOfStream)
        } else {
            Ok(crate::outcomes::ReceivedBatch {
                messages,
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            })
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ── Publisher ─────────────────────────────────────────────────────────────────

pub struct GrpcPublisher {
    client: BridgeClient<Channel>,
    // Retains the shared registry entry so concurrent publishers reuse this channel.
    _shared_channel: std::sync::Arc<Channel>,
    url: String,
    timeout: Option<Duration>,
    topic: Option<String>,
}

impl GrpcPublisher {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        // Use a lazy channel so the publisher route can start before a server-mode
        // gRPC consumer has finished binding its embedded listener.
        let url = config.tls.normalize_url(&config.url);
        // Share one channel across publishers with the same connection settings; the
        // channel multiplexes and the topic is per-message.
        let identity = crate::support::connection_registry::connection_identity((
            &url,
            config.tls.required,
            &config.tls.ca_file,
            &config.tls.cert_file,
            &config.tls.key_file,
            config.tls.accept_invalid_certs,
            config.timeout_ms,
        ));
        let config_clone = config.clone();
        let url_for_build = url.clone();
        let shared_channel = crate::support::connection_registry::get_or_create(
            "grpc-channel",
            identity,
            config.shared.unwrap_or(true),
            move || async move {
                let endpoint = make_endpoint(&config_clone, &url_for_build).await?;
                Ok(endpoint.connect_lazy())
            },
        )
        .await?;
        let client = BridgeClient::new((*shared_channel).clone());
        Ok(Self {
            client,
            _shared_channel: shared_channel,
            url,
            timeout: config.timeout_ms.map(Duration::from_millis),
            topic: Some(
                config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
            ),
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

        // Preserve the original messages so we can map response ids back to originals.
        let original_messages = messages;
        let bridge_messages_vec: Vec<BridgeMessage> = original_messages
            .iter()
            .cloned()
            .map(|msg| {
                let mut md: std::collections::HashMap<String, String> = msg
                    .metadata
                    .into_iter()
                    .filter(|(key, _)| !crate::canonical_message::is_source_metadata_key(key))
                    .collect();
                if let Some(topic) = &self.topic {
                    md.entry("mq_bridge.topic".to_string())
                        .or_insert_with(|| topic.clone());
                }
                BridgeMessage {
                    payload: msg.payload.to_vec(),
                    id: fast_uuid_v7::format_uuid(msg.message_id).to_string(),
                    metadata: md.into_iter().collect(),
                }
            })
            .collect();

        // Process responses and enforce an overall timeout if configured.
        let mut id_map: std::collections::HashMap<String, Vec<CanonicalMessage>> =
            std::collections::HashMap::new();
        for msg in &original_messages {
            let id_str = fast_uuid_v7::format_uuid(msg.message_id).to_string();
            id_map.entry(id_str).or_default().push(msg.clone());
        }
        let total_messages = original_messages.len();

        // Start the publish_batch call but don't await it yet; the future is
        // driven inside the processing future so we can apply a timeout that
        // bounds the entire response-handling phase.
        let response_fut = client.publish_batch(tokio_stream::iter(bridge_messages_vec));

        let process_fut = async {
            let response = response_fut.await.map_err(|e| {
                PublisherError::Retryable(anyhow::anyhow!(format!(
                    "gRPC publish_batch error: {:?}",
                    e
                )))
            })?;
            let mut stream = response.into_inner();
            let mut responses = Vec::new();
            let mut failed: Vec<(CanonicalMessage, PublisherError)> = Vec::new();
            let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

            loop {
                match stream.message().await {
                    Ok(Some(r)) => match r.result {
                        Some(proto::publish_response::Result::Ack(ack)) => {
                            seen_ids.insert(ack.id.clone());
                            if ack.status != 0 {
                                if let Some(origs) = id_map.get(&ack.id) {
                                    for orig in origs {
                                        failed.push((
                                            orig.clone(),
                                            PublisherError::Retryable(anyhow::anyhow!(ack
                                                .reason
                                                .clone())),
                                        ));
                                    }
                                } else {
                                    return Err(PublisherError::Retryable(anyhow::anyhow!(ack
                                        .reason
                                        .clone())));
                                }
                            }
                        }
                        Some(proto::publish_response::Result::Reply(reply)) => {
                            seen_ids.insert(reply.id.clone());
                            responses.push(bridge_to_canonical(reply));
                        }
                        Some(proto::publish_response::Result::Error(err)) => {
                            // Treat explicit error responses as a retryable batch-level failure.
                            return Err(PublisherError::Retryable(anyhow::anyhow!(err)));
                        }
                        None => {}
                    },
                    Ok(None) => break,
                    Err(e) => {
                        error!("Error reading publish batch response stream: {:?}", e);
                        return Err(PublisherError::Retryable(anyhow::anyhow!(format!(
                            "gRPC stream error: {:?}",
                            e
                        ))));
                    }
                }
            }

            // Any ids that were not seen are treated as missing responses -> retryable.
            for (id, origs) in &id_map {
                if !seen_ids.contains(id) {
                    for orig in origs {
                        failed.push((
                            orig.clone(),
                            PublisherError::Retryable(anyhow::anyhow!("missing response for id")),
                        ));
                    }
                }
            }

            Ok((responses, failed)) as Result<_, PublisherError>
        };

        let (responses, failed): (
            Vec<crate::CanonicalMessage>,
            Vec<(crate::CanonicalMessage, PublisherError)>,
        ) = if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, process_fut)
                .await
                .map_err(|_| {
                    PublisherError::Retryable(anyhow::anyhow!("gRPC publish batch timed out"))
                })??
        } else {
            process_fut.await?
        };

        let total = total_messages;
        if failed.is_empty() && responses.is_empty() {
            Ok(SentBatch::Ack)
        } else if failed.len() == total {
            Err(PublisherError::Retryable(anyhow::anyhow!(
                "All messages in batch failed"
            )))
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn bridge_to_canonical(msg: BridgeMessage) -> CanonicalMessage {
    let message_id = if msg.id.is_empty() {
        None
    } else if let Ok(uuid) = Uuid::parse_str(&msg.id) {
        Some(uuid.as_u128())
    } else if msg.id.starts_with("0x") || msg.id.starts_with("0X") {
        u128::from_str_radix(msg.id.trim_start_matches("0x").trim_start_matches("0X"), 16).ok()
    } else {
        msg.id.parse::<u128>().ok()
    };
    CanonicalMessage::new(msg.payload, message_id).with_metadata(msg.metadata)
}

async fn make_endpoint(config: &GrpcConfig, url: &str) -> Result<tonic::transport::Endpoint> {
    let mut endpoint = tonic::transport::Endpoint::from_shared(url.to_string())?;

    if config.tls.required {
        let mut tls_config = ClientTlsConfig::new();
        if let Some(ca_path) = &config.tls.ca_file {
            let ca_pem = tokio::fs::read(ca_path).await?;
            let ca_cert = Certificate::from_pem(ca_pem);
            tls_config = tls_config.ca_certificate(ca_cert);
        }
        if let (Some(cert_path), Some(key_path)) = (&config.tls.cert_file, &config.tls.key_file) {
            let cert_pem = tokio::fs::read(cert_path).await?;
            let key_pem = tokio::fs::read(key_path).await?;
            let identity = Identity::from_pem(cert_pem, key_pem);
            tls_config = tls_config.identity(identity);
        }
        endpoint = endpoint.tls_config(tls_config)?;
    }

    if let Some(ms) = config.timeout_ms {
        endpoint = endpoint.connect_timeout(Duration::from_millis(ms));
    }

    Ok(endpoint)
}

fn parse_addr(url: &str) -> Result<std::net::SocketAddr> {
    let stripped = url.find("://").map(|p| &url[p + 3..]).unwrap_or(url);
    let host = stripped
        .find('/')
        .map(|p| &stripped[..p])
        .unwrap_or(stripped);
    host.parse()
        .map_err(|e| anyhow::anyhow!("Invalid gRPC server address '{}': {}", host, e))
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
                while let Ok(Some(msg_result)) = stream.message().await {
                    let msg_id = msg_result.id.clone();
                    let _ = sender.send(msg_result);
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
        // Bind an ephemeral port and start the server using that listener so tests
        // don't rely on a hardcoded port.
        let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        let (tx, _) = broadcast::channel(16);
        let mut rx_for_pub_test = tx.subscribe();
        let bridge = MockBridge { tx: tx.clone() };

        let incoming: TcpListenerStream = TcpListenerStream::new(listener);
        let server_handle = tokio::spawn(async move {
            TonicServer::builder()
                .serve_with_incoming(BridgeServer::new(bridge), incoming)
                .await
                .unwrap();
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let config = GrpcConfig {
            url: format!("http://{}", local),
            timeout_ms: None,
            topic: Some("test_topic".to_string()),
            ..Default::default()
        };

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

        let consumer_ep = Endpoint {
            endpoint_type: EndpointType::Grpc(config),
            middlewares: vec![],
            handler: None,
        };
        // Create the consumer first. This will establish the subscription inside `new()`.
        let mut consumer = consumer_ep.create_consumer("test_route").await.unwrap();

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
        // Setup Mock Server on a unique port
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

        let config = GrpcConfig {
            url: format!("http://{}", addr),
            timeout_ms: None,
            topic: Some("e2e_test_topic".to_string()),
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

        // Setup and run routes using deploy()
        // Route 1: Memory -> gRPC (tests GrpcPublisher::send_batch)
        let route_to_grpc = Route::new(mem_source_ep, grpc_publisher_ep);
        route_to_grpc.deploy("route_to_grpc").await.unwrap();

        // Route 2: gRPC -> Memory (tests GrpcConsumer::receive_batch)
        let route_from_grpc = Route::new(grpc_consumer_ep, mem_dest_ep);
        route_from_grpc.deploy("route_from_grpc").await.unwrap();

        // Execute test: Send a batch of messages into the first route
        let messages_to_send = vec![
            CanonicalMessage::new("e2e_payload_1".into(), None),
            CanonicalMessage::new("e2e_payload_2".into(), None),
        ];
        mem_source_publisher
            .send_batch(messages_to_send.clone())
            .await
            .unwrap();

        // Verify: Receive the batch from the second route's destination
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

        server_handle.abort();
    }
    #[tokio::test]
    async fn test_grpc_acknowledge_and_batch_streaming() {
        let addr = "[::1]:50055".parse().unwrap();
        let (tx, _) = broadcast::channel(16);
        let bridge = MockBridge { tx: tx.clone() };

        let server_handle = tokio::spawn(async move {
            Server::builder()
                .serve(addr, BridgeServer::new(bridge))
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let config = GrpcConfig {
            url: format!("http://{}", addr),
            timeout_ms: None,
            topic: Some("batch_test_topic".to_string()),
            ..Default::default()
        };

        // Test sending a batch using GrpcPublisher
        let publisher = GrpcPublisher::new(&config)
            .await
            .expect("Failed to create GrpcPublisher");

        let msgs = vec![
            CanonicalMessage::new("batch_1".into(), None),
            CanonicalMessage::new("batch_2".into(), None),
        ];

        let sent_result = publisher.send_batch(msgs).await;
        // The mock server returns Ack variant with status 0, so it should map to SentBatch::Ack
        assert!(matches!(sent_result, Ok(SentBatch::Ack)));

        // Test explicit acknowledge
        let mut client = BridgeClient::new(
            tonic::transport::Endpoint::from_shared(config.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        );
        let ack_req = tonic::Request::new(proto::Ack {
            id: fast_uuid_v7::gen_id_str().to_string(),
            status: 0,
            reason: String::new(),
            metadata: Default::default(),
        });

        let ack_resp = client.acknowledge(ack_req).await;
        assert!(ack_resp.is_ok());
        assert!(ack_resp.unwrap().into_inner().success);

        server_handle.abort();
    }
}
