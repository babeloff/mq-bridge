//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::models::GrpcConfig;
use crate::traits::{
    ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher, PublisherError, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Result;
use async_trait::async_trait;
use bytes::{Buf, BufMut};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
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

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server as TonicServer;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::{Response, Status};

const GRPC_BATCH_POLL_MS: u64 = 15; // Increased for better batching in performance tests, and to reduce "poll timed out" warnings
/// Acks for one committed batch that may be in flight at once.
const GRPC_ACK_CONCURRENCY: usize = 64;
/// `publish_batch` messages that may be dispatched but not yet answered. Bounds how far
/// the dispatch task can run ahead of the commits it is waiting on.
const PUBLISH_BATCH_INFLIGHT: usize = 1024;
/// Unacknowledged messages retained per subscriber, and subscribers retained per route.
const MAX_PENDING_PER_CONSUMER: usize = 1024;
const MAX_PENDING_CONSUMERS: usize = 64;

// ── Consumer ──────────────────────────────────────────────────────────────────

pub struct GrpcConsumer {
    inner: GrpcConsumerInner,
    url: String,
    bound_addr: Option<std::net::SocketAddr>,
}

enum GrpcConsumerInner {
    Client(Box<ClientModeConsumer>),
    Dynamic(Box<DynamicConsumer>),
    Server(ServerModeConsumer),
}

impl GrpcConsumer {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let url = config.tls.normalize_url(&config.url);
        let (inner, bound_addr) = if config.server_mode {
            let s = ServerModeConsumer::new(config, &url).await?;
            let addr = s.bound_addr();
            (GrpcConsumerInner::Server(s), Some(addr))
        } else if config.descriptor_set_path.is_some() {
            (
                GrpcConsumerInner::Dynamic(Box::new(DynamicConsumer::new(config, &url).await?)),
                None,
            )
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
    // Client mode sends acknowledgement RPCs and server mode resolves per-message
    // completion channels. Both operations are independent across messages.
    fn commit_requires_order(&self) -> bool {
        false
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        match &mut self.inner {
            GrpcConsumerInner::Client(c) => c.set_exit_on_empty(exit_on_empty),
            GrpcConsumerInner::Dynamic(c) => c.set_exit_on_empty(exit_on_empty),
            GrpcConsumerInner::Server(s) => s.set_exit_on_empty(exit_on_empty),
        }
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        match &mut self.inner {
            GrpcConsumerInner::Client(c) => c.receive_batch(max_messages).await,
            GrpcConsumerInner::Dynamic(c) => c.receive_batch(max_messages).await,
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
            GrpcConsumerInner::Dynamic(_) => (true, serde_json::json!({ "mode": "dynamic" })),
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

#[derive(Clone, Default)]
struct RawProtobufCodec;

#[derive(Clone, Default)]
struct RawProtobufEncoder;

#[derive(Clone, Default)]
struct RawProtobufDecoder;

impl tonic::codec::Codec for RawProtobufCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawProtobufEncoder;
    type Decoder = RawProtobufDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawProtobufEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawProtobufDecoder
    }
}

impl tonic::codec::Encoder for RawProtobufEncoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> std::result::Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl tonic::codec::Decoder for RawProtobufDecoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        Ok(Some(src.copy_to_bytes(src.remaining()).to_vec()))
    }
}

/// Await `call`, failing with a clear error if `deadline` passes first. No deadline
/// configured means no bound, matching the rest of the endpoint.
async fn with_deadline<T>(
    call: impl std::future::Future<Output = Result<T, Status>>,
    deadline: Option<Duration>,
) -> Result<T> {
    match deadline {
        Some(deadline) => tokio::time::timeout(deadline, call)
            .await
            .map_err(|_| anyhow::anyhow!("dynamic gRPC call timed out after {deadline:?}"))?
            .map_err(Into::into),
        None => call.await.map_err(Into::into),
    }
}

enum DynamicResponse {
    Unary(Option<Vec<u8>>),
    // Boxed: an inline `Streaming` is an order of magnitude larger than the unary arm.
    Streaming(Box<tonic::Streaming<Vec<u8>>>),
}

struct DynamicConsumer {
    response: DynamicResponse,
    output: MessageDescriptor,
    exit_on_empty: bool,
}

impl DynamicConsumer {
    async fn new(config: &GrpcConfig, url: &str) -> Result<Self> {
        let descriptor_path = config
            .descriptor_set_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("dynamic gRPC requires descriptor_set_path"))?;
        let service_name = config
            .service_name
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("dynamic gRPC requires service_name"))?;
        let method_name = config
            .method_name
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("dynamic gRPC requires method_name"))?;
        let descriptor_bytes = tokio::fs::read(descriptor_path).await?;
        let pool = DescriptorPool::decode(descriptor_bytes.as_slice())?;
        let service = pool.get_service_by_name(service_name).ok_or_else(|| {
            anyhow::anyhow!(
                "gRPC service '{}' not found in descriptor set",
                service_name
            )
        })?;
        let method = service
            .methods()
            .find(|method| method.name() == method_name)
            .ok_or_else(|| {
                anyhow::anyhow!("gRPC method '{}.{}' not found", service_name, method_name)
            })?;
        if method.is_client_streaming() {
            anyhow::bail!("dynamic gRPC client-streaming methods are not supported");
        }
        if config.server_streaming != method.is_server_streaming() {
            anyhow::bail!(
                "dynamic gRPC server_streaming={} does not match descriptor for '{}.{}'",
                config.server_streaming,
                service_name,
                method_name
            );
        }

        let request_json = config
            .request
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        let request_text = serde_json::to_string(&request_json)?;
        let mut deserializer = serde_json::Deserializer::from_str(&request_text);
        let request = DynamicMessage::deserialize(method.input(), &mut deserializer)?;
        let mut request_bytes = Vec::with_capacity(request.encoded_len());
        request.encode(&mut request_bytes)?;

        let channel = make_endpoint(config, url).await?.connect().await?;
        let mut client = tonic::client::Grpc::new(channel);
        let path = tonic::codegen::http::uri::PathAndQuery::from_maybe_shared(format!(
            "/{service_name}/{method_name}"
        ))?;
        // `make_endpoint` only bounds connection setup, so the call itself gets the same
        // deadline — otherwise a server that accepts the connection and then never answers
        // hangs route startup forever.
        let deadline = config.timeout_ms.map(Duration::from_millis);
        let response = if method.is_server_streaming() {
            let call = client.server_streaming(Request::new(request_bytes), path, RawProtobufCodec);
            DynamicResponse::Streaming(Box::new(with_deadline(call, deadline).await?.into_inner()))
        } else {
            let call = client.unary(Request::new(request_bytes), path, RawProtobufCodec);
            DynamicResponse::Unary(Some(with_deadline(call, deadline).await?.into_inner()))
        };
        Ok(Self {
            response,
            output: method.output(),
            exit_on_empty: false,
        })
    }

    /// A body that does not match the descriptor is a permanent error, not a connection
    /// one: reconnecting re-reads the same bytes and fails identically.
    fn decode_message(&self, bytes: &[u8]) -> Result<CanonicalMessage, ConsumerError> {
        let message = DynamicMessage::decode(self.output.clone(), bytes)
            .map_err(|error| ConsumerError::Permanent(error.into()))?;
        let payload =
            serde_json::to_vec(&message).map_err(|error| ConsumerError::Permanent(error.into()))?;
        Ok(CanonicalMessage::new(payload, None))
    }
}

#[async_trait]
impl MessageConsumer for DynamicConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
        let max_messages = max_messages.max(1);
        let mut raw = Vec::with_capacity(max_messages);
        match &mut self.response {
            DynamicResponse::Unary(message) => {
                if let Some(message) = message.take() {
                    raw.push(message);
                }
            }
            DynamicResponse::Streaming(stream) => {
                while raw.len() < max_messages {
                    let next = if raw.is_empty() {
                        match crate::traits::drain_gated(self.exit_on_empty, stream.message()).await
                        {
                            Some(result) => result,
                            None => return Ok(crate::outcomes::ReceivedBatch::empty()),
                        }
                    } else {
                        match tokio::time::timeout(
                            Duration::from_millis(GRPC_BATCH_POLL_MS),
                            stream.message(),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => break,
                        }
                    };
                    match next {
                        Ok(Some(message)) => raw.push(message),
                        Ok(None) => break,
                        Err(error) => return Err(ConsumerError::Connection(error.into())),
                    }
                }
            }
        }
        if raw.is_empty() {
            return Err(ConsumerError::EndOfStream);
        }
        // Skip what will not decode rather than failing the batch: these bytes are already
        // off the stream and cannot be re-read, so discarding the whole batch for one bad
        // message would silently drop every healthy message alongside it.
        let mut messages = Vec::with_capacity(raw.len());
        for bytes in &raw {
            match self.decode_message(bytes) {
                Ok(message) => messages.push(message),
                Err(error) => {
                    warn!(%error, "Dropping a dynamic gRPC response that does not match the descriptor")
                }
            }
        }
        if messages.is_empty() {
            return Err(ConsumerError::Permanent(anyhow::anyhow!(
                "every message in the dynamic gRPC batch failed to decode"
            )));
        }
        Ok(crate::outcomes::ReceivedBatch {
            messages,
            commit: Box::new(|_| Box::pin(async { Ok(()) })),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ClientModeConsumer {
    client: BridgeClient<Channel>,
    stream: tonic::Streaming<BridgeMessage>,
    consumer_id: String,
    /// Drain mode: only then does an idle first-message read time out into an empty batch.
    exit_on_empty: bool,
}

impl ClientModeConsumer {
    async fn new(config: &GrpcConfig, url: &str) -> Result<Self> {
        debug!(grpc_url = %url, "Creating gRPC client consumer (client mode)");
        let endpoint = make_endpoint(config, url).await?;
        let channel = endpoint.connect().await?;
        let mut client = configured_client(config, channel);
        let topic = config
            .topic
            .clone()
            .unwrap_or_else(|| "default".to_string());
        debug!(grpc_url = %config.url, subscribe_topic = %topic, "gRPC client consumer subscribing to topic");
        // A fresh id per consumer, not the topic: competing consumers on one topic would
        // otherwise share a pending set, so the first ack would remove the entry and every
        // other consumer's ack for the same message would be rejected as unknown. Set
        // `consumer_id` explicitly to keep redelivery across reconnects.
        let consumer_id = config
            .consumer_id
            .clone()
            .unwrap_or_else(|| fast_uuid_v7::gen_id().to_string());
        let request = Request::new(SubscribeRequest {
            topic: topic.clone(),
            consumer_id: consumer_id.clone(),
        });
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
            client,
            stream,
            consumer_id,
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
        receive_from_stream(
            &mut self.stream,
            self.client.clone(),
            self.consumer_id.clone(),
            max_messages,
            self.exit_on_empty,
        )
        .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Reads a batch from a tonic server-streaming response.
/// Blocks on the first message; polls briefly for subsequent ones to fill the batch.
async fn receive_from_stream(
    stream: &mut tonic::Streaming<BridgeMessage>,
    client: BridgeClient<Channel>,
    consumer_id: String,
    max_messages: usize,
    exit_on_empty: bool,
) -> Result<crate::outcomes::ReceivedBatch, ConsumerError> {
    let max_messages = max_messages.max(1);
    let mut messages = Vec::with_capacity(max_messages);
    let mut message_ids = Vec::with_capacity(max_messages);
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
                message_ids.push(msg.id.clone());
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
        let commit = grpc_client_commit(client, consumer_id, message_ids);
        Ok(crate::outcomes::ReceivedBatch { messages, commit })
    }
}

fn grpc_client_commit(
    client: BridgeClient<Channel>,
    consumer_id: String,
    message_ids: Vec<String>,
) -> crate::traits::BatchCommitFunc {
    Box::new(move |dispositions| {
        Box::pin(async move {
            if dispositions.len() != message_ids.len() {
                anyhow::bail!(
                    "gRPC batch commit length mismatch: dispositions={}, messages={}",
                    dispositions.len(),
                    message_ids.len()
                );
            }
            // Acks are independent, so they go out concurrently. Awaiting them one at a
            // time would cost `batch_size` round trips per commit.
            let client = &client;
            let consumer_id = &consumer_id;
            futures::stream::iter(message_ids.into_iter().zip(dispositions))
                .map(|(id, disposition)| async move {
                    let status = match disposition {
                        MessageDisposition::Ack | MessageDisposition::Reply(_) => {
                            proto::ack::Status::Ack
                        }
                        MessageDisposition::Nack => proto::ack::Status::Nack,
                    };
                    let mut metadata = HashMap::new();
                    metadata.insert("mq_bridge.consumer_id".to_string(), consumer_id.clone());
                    let response = client
                        .clone()
                        .acknowledge(Request::new(proto::Ack {
                            id: id.clone(),
                            status: status as i32,
                            reason: String::new(),
                            metadata,
                        }))
                        .await?
                        .into_inner();
                    if !response.success {
                        warn!(ack_id = %id, error = %response.error, "gRPC acknowledge rejected");
                    }
                    Ok::<(), tonic::Status>(())
                })
                .buffer_unordered(GRPC_ACK_CONCURRENCY)
                .try_collect::<Vec<()>>()
                .await?;
            Ok(())
        })
    })
}

fn publish_response_for_disposition(
    id: String,
    disposition: MessageDisposition,
) -> proto::PublishResponse {
    match disposition {
        MessageDisposition::Reply(message) => {
            let mut reply = canonical_to_bridge(message, None);
            reply.id = id;
            proto::PublishResponse {
                result: Some(proto::publish_response::Result::Reply(reply)),
            }
        }
        MessageDisposition::Ack => proto::PublishResponse {
            result: Some(proto::publish_response::Result::Ack(proto::Ack {
                id,
                status: proto::ack::Status::Ack as i32,
                reason: String::new(),
                metadata: Default::default(),
            })),
        },
        MessageDisposition::Nack => proto::PublishResponse {
            result: Some(proto::publish_response::Result::Ack(proto::Ack {
                id,
                status: proto::ack::Status::Nack as i32,
                reason: "Downstream processing failed".to_string(),
                metadata: Default::default(),
            })),
        },
    }
}

fn canonical_to_bridge(message: CanonicalMessage, topic: Option<&str>) -> BridgeMessage {
    let mut metadata: HashMap<String, String> = message
        .metadata
        .into_iter()
        .filter(|(key, _)| !crate::canonical_message::is_source_metadata_key(key))
        .collect();
    if let Some(topic) = topic {
        metadata
            .entry("mq_bridge.topic".to_string())
            .or_insert_with(|| topic.to_string());
    }
    BridgeMessage {
        payload: message.payload.to_vec(),
        id: fast_uuid_v7::format_uuid(message.message_id).to_string(),
        metadata,
    }
}

// ── Embedded gRPC server (server_mode) ────────────────────────────────────────

struct ServerModeConsumer {
    route_id: u64,
    shared_server: Arc<SharedGrpcServer>,
    bound_addr: std::net::SocketAddr,
    // One receive channel per shard; publishes are spread round-robin across the
    // shards so many concurrent producers don't all contend on one channel.
    rxs: Vec<mpsc::Receiver<InboundDelivery>>,
    // Round-robin cursor for the next shard to drain first, so none starves.
    drain_start: usize,
    /// Drain mode: only then does an idle first-message poll time out into an empty batch.
    exit_on_empty: bool,
}

/// Tonic service implementation that fans incoming messages into a subscriber
/// broadcast stream and a reliable internal queue for the server-mode consumer.
struct BridgeService {
    router: Arc<SharedGrpcRouter>,
    /// How long to wait for the consuming route to commit a published message before
    /// answering NACK. `None` (no `timeout_ms`) waits indefinitely, so a route that never
    /// commits blocks the publisher — set `timeout_ms` to bound it.
    commit_timeout: Option<Duration>,
}

/// Wait for the route's disposition, treating an expired `commit_timeout` or a dropped
/// sender as a NACK: either way the message was not confirmed committed.
async fn await_disposition(
    receipt: oneshot::Receiver<MessageDisposition>,
    commit_timeout: Option<Duration>,
) -> MessageDisposition {
    match commit_timeout {
        Some(limit) => match tokio::time::timeout(limit, receipt).await {
            Ok(disposition) => disposition.unwrap_or(MessageDisposition::Nack),
            Err(_) => {
                warn!(
                    ?limit,
                    "gRPC publish timed out waiting for the route to commit"
                );
                MessageDisposition::Nack
            }
        },
        None => receipt.await.unwrap_or(MessageDisposition::Nack),
    }
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
    txs: Vec<mpsc::Sender<InboundDelivery>>,
    cursor: Arc<AtomicUsize>,
    broadcast_tx: broadcast::Sender<BridgeMessage>,
    subscriber_pending: Arc<Mutex<SubscriberPending>>,
    /// `consumer_id`s with a live subscribe stream, so a duplicate is rejected rather than
    /// silently sharing the first one's retention set.
    active_subscribers: Arc<Mutex<HashSet<String>>>,
}

struct InboundDelivery {
    message: BridgeMessage,
    completion: oneshot::Sender<MessageDisposition>,
}

/// A `publish_batch` message that has been dispatched and is waiting for its response.
enum Pending {
    /// Resolves to the disposition the consuming route committed.
    Receipt(oneshot::Receiver<MessageDisposition>),
    /// Never reached a consumer; answered with this reason.
    Nack(&'static str),
}

/// Unacknowledged messages retained for one subscriber, so a consumer reconnecting with
/// the same `consumer_id` is redelivered them.
///
/// Both caps are hard: retention is a redelivery aid for a running server, not durable
/// storage, and a subscriber that never acks would otherwise grow the server without
/// bound. `unacked` is authoritative — `queue` keeps arrival order and may hold already
/// acked entries until it is compacted, which keeps every operation O(1) amortized.
#[derive(Default)]
struct PendingMessages {
    queue: VecDeque<BridgeMessage>,
    unacked: HashSet<String>,
}

impl PendingMessages {
    fn retain(&mut self, msg: &BridgeMessage) {
        if !self.unacked.insert(msg.id.clone()) {
            return;
        }
        if self.queue.len() >= MAX_PENDING_PER_CONSUMER {
            self.queue.retain(|held| self.unacked.contains(&held.id));
        }
        if self.queue.len() >= MAX_PENDING_PER_CONSUMER {
            if let Some(dropped) = self.queue.pop_front() {
                warn!(
                    msg_id = %dropped.id,
                    "gRPC subscriber holds too many unacknowledged messages, dropping the oldest"
                );
                self.unacked.remove(&dropped.id);
            }
        }
        self.queue.push_back(msg.clone());
    }

    fn is_unacked(&self, msg_id: &str) -> bool {
        self.unacked.contains(msg_id)
    }

    /// `true` if the id was still awaiting acknowledgement.
    fn acknowledge(&mut self, msg_id: &str) -> bool {
        self.unacked.remove(msg_id)
    }

    fn replay(&self) -> Vec<BridgeMessage> {
        self.queue
            .iter()
            .filter(|msg| self.unacked.contains(&msg.id))
            .cloned()
            .collect()
    }
}

/// Per-subscriber retention for one route, capped in both dimensions.
#[derive(Default)]
struct SubscriberPending {
    by_consumer: HashMap<String, PendingMessages>,
    /// Insertion order of `by_consumer`, so the oldest subscriber can be evicted. A
    /// consumer that never reconnects (the default id is per-instance) would otherwise
    /// leave its entry behind forever.
    order: VecDeque<String>,
}

impl SubscriberPending {
    fn entry(&mut self, consumer_id: &str) -> &mut PendingMessages {
        if !self.by_consumer.contains_key(consumer_id) {
            if self.order.len() >= MAX_PENDING_CONSUMERS {
                if let Some(evicted) = self.order.pop_front() {
                    warn!(
                        consumer_id = %evicted,
                        "gRPC subscriber retention is full, dropping the oldest subscriber"
                    );
                    self.by_consumer.remove(&evicted);
                }
            }
            self.order.push_back(consumer_id.to_string());
            self.by_consumer
                .insert(consumer_id.to_string(), PendingMessages::default());
        }
        self.by_consumer
            .get_mut(consumer_id)
            .expect("entry was just inserted")
    }

    fn get(&self, consumer_id: &str) -> Option<&PendingMessages> {
        self.by_consumer.get(consumer_id)
    }

    fn get_mut(&mut self, consumer_id: &str) -> Option<&mut PendingMessages> {
        self.by_consumer.get_mut(consumer_id)
    }

    fn remove(&mut self, consumer_id: &str) {
        self.by_consumer.remove(consumer_id);
        self.order.retain(|held| held != consumer_id);
    }
}

/// Holds a `consumer_id` for the lifetime of one subscribe stream, so a second stream
/// cannot claim the same id while the first is live. Released on drop, however the
/// stream's task ends.
struct SubscriptionClaim {
    consumer_id: String,
    active: Arc<Mutex<HashSet<String>>>,
    /// Set for a server-generated id, whose retention is worthless once the stream ends
    /// because no client can ever reconnect under it.
    drop_pending: Option<Arc<Mutex<SubscriberPending>>>,
}

impl SubscriptionClaim {
    fn acquire(route: &SharedGrpcRoute, consumer_id: String, ephemeral: bool) -> Option<Self> {
        let mut active = route.active_subscribers.lock().ok()?;
        if !active.insert(consumer_id.clone()) {
            return None;
        }
        drop(active);
        Some(Self {
            consumer_id,
            active: route.active_subscribers.clone(),
            drop_pending: ephemeral.then(|| route.subscriber_pending.clone()),
        })
    }
}

impl Drop for SubscriptionClaim {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.consumer_id);
        }
        if let Some(pending) = &self.drop_pending {
            if let Ok(mut pending) = pending.lock() {
                pending.remove(&self.consumer_id);
            }
        }
    }
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
        txs: Vec<mpsc::Sender<InboundDelivery>>,
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
                subscriber_pending: Arc::new(Mutex::new(SubscriberPending::default())),
                active_subscribers: Arc::new(Mutex::new(HashSet::new())),
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

    async fn dispatch(&self, msg: BridgeMessage) -> Result<oneshot::Receiver<MessageDisposition>> {
        let topic = bridge_message_topic(&msg);
        let route = self
            .route_for_topic(&topic)
            .ok_or_else(|| anyhow::anyhow!("No route for topic '{}'", topic))?;
        {
            let active = route
                .active_subscribers
                .lock()
                .map_err(|_| anyhow::anyhow!("gRPC active subscriber lock poisoned"))?;
            if !active.is_empty() {
                let mut pending = route
                    .subscriber_pending
                    .lock()
                    .map_err(|_| anyhow::anyhow!("gRPC subscriber retention lock poisoned"))?;
                for consumer_id in active.iter() {
                    pending.entry(consumer_id).retain(&msg);
                }
            }
        }
        // Only clone for the broadcast stream when someone is actually subscribed.
        if route.broadcast_tx.receiver_count() > 0 {
            let _ = route.broadcast_tx.send(msg.clone());
        }
        let shard = route.cursor.fetch_add(1, Ordering::Relaxed) % route.txs.len();
        let (completion, receipt) = oneshot::channel();
        route.txs[shard]
            .send(InboundDelivery {
                message: msg,
                completion,
            })
            .await
            .map_err(|_| anyhow::anyhow!("No active gRPC consumer for topic '{}'", topic))?;
        Ok(receipt)
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
        let receipt = match self.router.dispatch(msg).await {
            Ok(receipt) => receipt,
            Err(_) => {
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
        };
        let disposition = await_disposition(receipt, self.commit_timeout).await;
        Ok(Response::new(publish_response_for_disposition(
            msg_id,
            disposition,
        )))
    }

    async fn acknowledge(
        &self,
        request: Request<proto::Ack>,
    ) -> Result<Response<proto::AckResponse>, Status> {
        let ack = request.into_inner();
        trace!(ack_id = %ack.id, "BridgeService::acknowledge received ack");
        // Without an id there is no retention set to resolve the ack against, so reporting
        // success would tell the caller its message was committed when nothing was tracked.
        let Some(consumer_id) = ack.metadata.get("mq_bridge.consumer_id") else {
            return Ok(Response::new(proto::AckResponse {
                success: false,
                error: "Ack is missing the mq_bridge.consumer_id metadata entry".to_string(),
            }));
        };
        let acked = ack.status == proto::ack::Status::Ack as i32;
        let mut found = false;
        if let Ok(routes) = self.router.routes.read() {
            for route in routes.values() {
                let Ok(mut pending) = route.subscriber_pending.lock() else {
                    continue;
                };
                let Some(messages) = pending.get_mut(consumer_id) else {
                    continue;
                };
                if !messages.is_unacked(&ack.id) {
                    continue;
                }
                // A NACK leaves the message pending so it is redelivered on reconnect.
                found = if acked {
                    messages.acknowledge(&ack.id)
                } else {
                    true
                };
                break;
            }
        }
        Ok(Response::new(proto::AckResponse {
            success: found,
            error: if found {
                String::new()
            } else {
                "Unknown consumer or message".to_string()
            },
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

        // Dispatch and commit-wait run in separate tasks. Awaiting a receipt inline would
        // keep the next message from reaching the consumer until this one had committed,
        // serializing the stream and forcing every consumer batch to hold one message.
        let (pending_tx, mut pending_rx) =
            mpsc::channel::<(String, Pending)>(PUBLISH_BATCH_INFLIGHT);

        let commit_timeout = self.commit_timeout;
        tokio::spawn(async move {
            while let Some((msg_id, pending)) = pending_rx.recv().await {
                let resp = match pending {
                    Pending::Receipt(receipt) => publish_response_for_disposition(
                        msg_id,
                        await_disposition(receipt, commit_timeout).await,
                    ),
                    Pending::Nack(reason) => proto::PublishResponse {
                        result: Some(proto::publish_response::Result::Ack(proto::Ack {
                            id: msg_id,
                            status: proto::ack::Status::Nack as i32,
                            reason: reason.to_string(),
                            metadata: Default::default(),
                        })),
                    },
                };
                if tx.send(Ok(resp)).await.is_err() {
                    warn!("publish_batch: client stream closed, stopping responder task");
                    break;
                }
            }
            trace!("publish_batch responder task exiting");
        });

        tokio::spawn(async move {
            while let Ok(Some(msg)) = stream.message().await {
                let msg_id = msg.id.clone();
                let topic = bridge_message_topic(&msg);
                trace!(msg_id = %msg_id, topic = %topic, "BridgeService::publish_batch received message");
                let pending = match router.dispatch(msg).await {
                    Ok(receipt) => Pending::Receipt(receipt),
                    Err(_) => {
                        warn!(
                            "publish_batch: internal server queue closed, stopping dispatch task"
                        );
                        // Queued rather than sent directly, so this terminal NACK still
                        // arrives after the responses for the messages before it.
                        let _ = pending_tx
                            .send((msg_id, Pending::Nack("Internal queue closed")))
                            .await;
                        break;
                    }
                };
                if pending_tx.send((msg_id, pending)).await.is_err() {
                    break;
                }
            }
            trace!("publish_batch dispatch task exiting");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeStream = ReceiverStream<Result<BridgeMessage, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        let topic = normalize_grpc_topic(Some(request.topic.as_str()));
        // An id the server made up cannot be reconnected to, so its retention is dropped
        // when the stream ends instead of waiting to be evicted by the cap.
        let ephemeral = request.consumer_id.is_empty();
        let consumer_id = if ephemeral {
            fast_uuid_v7::gen_id().to_string()
        } else {
            request.consumer_id
        };
        let route = self
            .router
            .route_for_topic(&topic)
            .ok_or_else(|| Status::not_found(format!("No active gRPC topic '{}'", topic)))?;

        // One stream per id. Two live subscriptions sharing an id would both be fanned the
        // same broadcast messages but share one retention set, so whichever acked first
        // would remove the entry and the other's ack would come back rejected.
        let claim = SubscriptionClaim::acquire(&route, consumer_id.clone(), ephemeral).ok_or_else(|| {
            Status::already_exists(format!(
                "gRPC consumer_id '{consumer_id}' already has an active subscription on topic '{topic}'"
            ))
        })?;

        let mut rx = route.broadcast_tx.subscribe();
        let replay = route
            .subscriber_pending
            .lock()
            .ok()
            .and_then(|pending| pending.get(&consumer_id).map(PendingMessages::replay))
            .unwrap_or_default();
        let replayed_ids: HashSet<_> = replay.iter().map(|msg| msg.id.clone()).collect();
        let (tx_stream, rx_stream) = mpsc::channel(32);
        tokio::spawn(async move {
            // Releases the id, and an ephemeral consumer's retention with it, however this
            // task ends.
            let _claim = claim;
            for msg in replay {
                if tx_stream.send(Ok(msg)).await.is_err() {
                    return;
                }
            }
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        // A dispatch racing the replay snapshot is both retained and broadcast.
                        // The retained copy was already sent above, so do not send it twice.
                        if replayed_ids.contains(&msg.id) {
                            continue;
                        }
                        if tx_stream.send(Ok(msg)).await.is_err() {
                            warn!("subscribe: downstream consumer disconnected");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            "subscribe: subscriber lagged; closing stream for retained replay"
                        );
                        break;
                    }
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
    txs: Vec<mpsc::Sender<InboundDelivery>>,
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
        commit_timeout: config.timeout_ms.map(Duration::from_millis),
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
        let mut completions = Vec::with_capacity(max_messages);
        'fill: loop {
            // Greedily sweep all shards for whatever is immediately available.
            let mut got_any = false;
            for offset in 0..shard_count {
                let idx = (self.drain_start + offset) % shard_count;
                if let Ok(delivery) = self.rxs[idx].try_recv() {
                    messages.push(bridge_to_canonical(delivery.message));
                    completions.push(delivery.completion);
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

            // Everything currently buffered has been drained. Return a partial batch
            // immediately so publishers waiting for its commit are not held behind the
            // stream-oriented batching linger.
            if !messages.is_empty() {
                break;
            }

            // Nothing buffered yet: block for the first message. Polling every shard
            // registers our waker on each.
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
            // Drain mode: a brief idle timeout on the first message yields an empty batch.
            let next = match crate::traits::drain_gated(self.exit_on_empty, poll).await {
                Some(value) => value,
                None => return Ok(crate::outcomes::ReceivedBatch::empty()),
            };
            match next {
                Some(delivery) => {
                    messages.push(bridge_to_canonical(delivery.message));
                    completions.push(delivery.completion);
                }
                None => break, // every shard closed
            }
        }
        if messages.is_empty() {
            Err(ConsumerError::EndOfStream)
        } else {
            let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
                Box::pin(async move {
                    if dispositions.len() != completions.len() {
                        anyhow::bail!(
                            "gRPC server batch commit length mismatch: dispositions={}, messages={}",
                            dispositions.len(),
                            completions.len()
                        );
                    }
                    for (completion, disposition) in completions.into_iter().zip(dispositions) {
                        let _ = completion.send(disposition);
                    }
                    Ok(())
                }) as futures::future::BoxFuture<'static, anyhow::Result<()>>
            });
            Ok(crate::outcomes::ReceivedBatch { messages, commit })
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
            config.initial_stream_window_size,
            config.initial_connection_window_size,
            config.http2_keepalive_interval_ms,
            config.http2_keepalive_timeout_ms,
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
        let client = configured_client(config, (*shared_channel).clone());
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
            .map(|msg| canonical_to_bridge(msg, self.topic.as_deref()))
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
    if let Some(v) = config.initial_stream_window_size {
        endpoint = endpoint.initial_stream_window_size(v);
    }
    if let Some(v) = config.initial_connection_window_size {
        endpoint = endpoint.initial_connection_window_size(v);
    }
    if let Some(ms) = config.http2_keepalive_interval_ms {
        endpoint = endpoint.http2_keep_alive_interval(Duration::from_millis(ms));
    }
    if let Some(ms) = config.http2_keepalive_timeout_ms {
        endpoint = endpoint.keep_alive_timeout(Duration::from_millis(ms));
    }

    Ok(endpoint)
}

fn configured_client(config: &GrpcConfig, channel: Channel) -> BridgeClient<Channel> {
    let mut client = BridgeClient::new(channel);
    if let Some(max) = config.max_decoding_message_size {
        client = client.max_decoding_message_size(max);
    }
    if let Some(max) = config.max_encoding_message_size {
        client = client.max_encoding_message_size(max);
    }
    client
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

    /// `publish_batch` must dispatch without waiting for each message to commit. Awaiting
    /// receipts inline prevents later messages from reaching the consumer until the first
    /// has committed, so every batch holds exactly one message and the stream is serialized.
    ///
    /// Uses the real `BridgeService`, not `MockBridge` — the mock answers immediately and
    /// cannot catch this.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn publish_batch_does_not_serialize_on_commits() {
        const COUNT: usize = 200;

        let mut consumer = GrpcConsumer::new(&GrpcConfig {
            url: "http://127.0.0.1:0".into(),
            topic: Some("batching".into()),
            server_mode: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let url = format!("http://{}", consumer.bound_addr.unwrap());

        let drain = tokio::spawn(async move {
            let first = consumer.receive_batch(512).await.expect("first receive");
            let first_count = first.messages.len();
            let second = tokio::time::timeout(Duration::from_secs(1), consumer.receive_batch(512))
                .await
                .expect("dispatch waited for the first batch to commit")
                .expect("second receive");
            let second_count = second.messages.len();

            (first.commit)(vec![MessageDisposition::Ack; first_count])
                .await
                .expect("first commit");
            (second.commit)(vec![MessageDisposition::Ack; second_count])
                .await
                .expect("second commit");

            let mut total = first_count + second_count;
            while total < COUNT {
                let batch = consumer.receive_batch(512).await.expect("receive");
                let n = batch.messages.len();
                if n == 0 {
                    continue;
                }
                total += n;
                (batch.commit)(vec![MessageDisposition::Ack; n])
                    .await
                    .expect("commit");
            }
            total
        });

        let publisher = GrpcPublisher::new(&GrpcConfig {
            url,
            topic: Some("batching".into()),
            ..Default::default()
        })
        .await
        .unwrap();

        let messages = (0..COUNT)
            .map(|i| CanonicalMessage::from(format!("m{i}")))
            .collect();
        publisher.send_batch(messages).await.unwrap();

        let total = tokio::time::timeout(Duration::from_secs(30), drain)
            .await
            .expect("route did not drain")
            .unwrap();

        assert_eq!(total, COUNT, "every message must arrive");
    }

    #[tokio::test(start_paused = true)]
    async fn server_mode_partial_batch_does_not_linger_before_commit() {
        let (tx, rx) = mpsc::channel(1);
        let shared_server = Arc::new(SharedGrpcServer {
            router: Arc::new(SharedGrpcRouter::new()),
            handle: tokio::spawn(std::future::pending()),
            bound_addr: "127.0.0.1:0".parse().unwrap(),
        });
        let mut consumer = ServerModeConsumer {
            route_id: GRPC_ROUTE_ID.fetch_add(1, Ordering::Relaxed),
            shared_server,
            bound_addr: "127.0.0.1:0".parse().unwrap(),
            rxs: vec![rx],
            drain_start: 0,
            exit_on_empty: false,
        };
        let (completion, receipt) = oneshot::channel();
        tx.send(InboundDelivery {
            message: bridge_msg("prompt-commit"),
            completion,
        })
        .await
        .unwrap();

        let batch = tokio::time::timeout(Duration::from_millis(1), consumer.receive_batch(128))
            .await
            .expect("server-mode receive must not linger for a partial batch")
            .expect("receive");
        assert_eq!(batch.messages.len(), 1);
        (batch.commit)(vec![MessageDisposition::Ack])
            .await
            .expect("commit");
        assert!(matches!(receipt.await, Ok(MessageDisposition::Ack)));
    }

    fn bridge_msg(id: &str) -> BridgeMessage {
        BridgeMessage {
            payload: id.as_bytes().to_vec(),
            id: id.to_string(),
            metadata: Default::default(),
        }
    }

    #[test]
    fn reply_preserves_the_request_id() {
        let response = publish_response_for_disposition(
            "request-id".to_string(),
            MessageDisposition::Reply(CanonicalMessage::from("reply")),
        );
        let Some(proto::publish_response::Result::Reply(reply)) = response.result else {
            panic!("expected a reply response");
        };
        assert_eq!(reply.id, "request-id");
    }

    #[test]
    fn pending_messages_replays_only_unacknowledged() {
        let mut pending = PendingMessages::default();
        for id in ["a", "b", "c"] {
            pending.retain(&bridge_msg(id));
        }
        // Retaining the same id twice must not duplicate it.
        pending.retain(&bridge_msg("b"));

        assert!(pending.acknowledge("b"));
        assert!(!pending.acknowledge("b"), "a second ack finds nothing");
        assert!(!pending.is_unacked("b"));

        let replayed: Vec<String> = pending.replay().into_iter().map(|msg| msg.id).collect();
        assert_eq!(replayed, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn pending_messages_caps_retention_and_drops_the_oldest() {
        let mut pending = PendingMessages::default();
        for i in 0..MAX_PENDING_PER_CONSUMER + 10 {
            pending.retain(&bridge_msg(&format!("m{i}")));
        }
        assert_eq!(pending.replay().len(), MAX_PENDING_PER_CONSUMER);
        assert!(!pending.is_unacked("m0"), "the oldest is evicted");
        assert!(pending.is_unacked(&format!("m{}", MAX_PENDING_PER_CONSUMER + 9)));
    }

    fn service_with_topic(topic: &str) -> BridgeService {
        let router = Arc::new(SharedGrpcRouter::new());
        let (tx, _rx) = mpsc::channel::<InboundDelivery>(8);
        router
            .register_route(1, topic.to_string(), vec![tx])
            .unwrap();
        BridgeService {
            router,
            commit_timeout: None,
        }
    }

    /// Two live subscriptions under one id would be fanned the same broadcast messages
    /// while sharing a single retention set, so the first ack would remove the entry and
    /// the second consumer's ack would come back rejected.
    #[tokio::test]
    async fn subscribe_rejects_a_duplicate_active_consumer_id() {
        let service = service_with_topic("dup");
        let subscribe = |consumer_id: &str| {
            service.subscribe(Request::new(SubscribeRequest {
                topic: "dup".to_string(),
                consumer_id: consumer_id.to_string(),
            }))
        };

        let _first = subscribe("shared").await.expect("first subscription");
        let err = subscribe("shared")
            .await
            .expect_err("duplicate is rejected");
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert!(
            err.message().contains("shared"),
            "the error should name the id: {}",
            err.message()
        );

        subscribe("other").await.expect("a distinct id still works");
    }

    #[tokio::test]
    async fn subscriber_lag_closes_the_stream_and_reconnect_replays_retained_messages() {
        let router = Arc::new(SharedGrpcRouter::new());
        let (tx, _rx) = mpsc::channel::<InboundDelivery>(8);
        let (broadcast_tx, _) = broadcast::channel(2);
        router.routes.write().unwrap().insert(
            1,
            SharedGrpcRoute {
                topic: "default".to_string(),
                txs: vec![tx],
                cursor: Arc::new(AtomicUsize::new(0)),
                broadcast_tx,
                subscriber_pending: Arc::new(Mutex::new(SubscriberPending::default())),
                active_subscribers: Arc::new(Mutex::new(HashSet::new())),
            },
        );
        let service = BridgeService {
            router: router.clone(),
            commit_timeout: None,
        };
        let request = || {
            Request::new(SubscribeRequest {
                topic: "default".to_string(),
                consumer_id: "durable".to_string(),
            })
        };

        let mut first = service.subscribe(request()).await.unwrap().into_inner();
        for id in ["a", "b", "c"] {
            router.dispatch(bridge_msg(id)).await.unwrap();
        }
        assert!(tokio::time::timeout(Duration::from_secs(1), first.next())
            .await
            .expect("lagged stream should terminate")
            .is_none());

        let mut replay = service.subscribe(request()).await.unwrap().into_inner();
        for id in ["a", "b", "c"] {
            assert_eq!(replay.next().await.unwrap().unwrap().id, id);
        }
    }

    /// Without the id there is no retention set to resolve the ack against, so reporting
    /// success would claim a commit that never tracked anything.
    #[tokio::test]
    async fn acknowledge_without_a_consumer_id_reports_failure() {
        let service = service_with_topic("acks");
        let response = service
            .acknowledge(Request::new(proto::Ack {
                id: "m1".to_string(),
                status: proto::ack::Status::Ack as i32,
                reason: String::new(),
                metadata: Default::default(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
        assert!(response.error.contains("mq_bridge.consumer_id"));
    }

    #[test]
    fn subscriber_pending_caps_the_number_of_subscribers() {
        let mut subscribers = SubscriberPending::default();
        for i in 0..MAX_PENDING_CONSUMERS + 5 {
            subscribers.entry(&format!("c{i}")).retain(&bridge_msg("x"));
        }
        assert!(subscribers.get("c0").is_none(), "the oldest is evicted");
        assert!(subscribers
            .get(&format!("c{}", MAX_PENDING_CONSUMERS + 4))
            .is_some());
    }
}
