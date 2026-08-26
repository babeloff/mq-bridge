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
use sha2::{Digest, Sha256};
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

    /// Encoded descriptors for the stable `mqbridge` public API.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("mqbridge_descriptor");
}

use proto::bridge_client::BridgeClient;
use proto::{BridgeMessage, SubscribeRequest};
use tonic::metadata::{Ascii, Binary, MetadataKey, MetadataMap, MetadataValue};
use tonic::Request;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server as TonicServer;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::{Response, Status};

/// Structured failure returned by descriptor-driven gRPC calls.
///
/// `Display` and `Debug` intentionally omit trailing metadata values so credentials
/// returned by a peer cannot leak through ordinary error logging. Callers that need
/// protocol details can inspect [`Self::trailing_metadata`] explicitly.
pub struct GrpcStatusError {
    code: tonic::Code,
    message: String,
    trailing_metadata: MetadataMap,
}

impl GrpcStatusError {
    pub fn code(&self) -> tonic::Code {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn trailing_metadata(&self) -> &MetadataMap {
        &self.trailing_metadata
    }
}

impl From<Status> for GrpcStatusError {
    fn from(status: Status) -> Self {
        Self {
            code: status.code(),
            message: status.message().to_owned(),
            trailing_metadata: status.metadata().clone(),
        }
    }
}

impl std::fmt::Display for GrpcStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gRPC status {:?}: {}", self.code, self.message)
    }
}

impl std::fmt::Debug for GrpcStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrpcStatusError")
            .field("code", &self.code)
            .field("message", &self.message)
            .field("trailing_metadata_entries", &self.trailing_metadata.len())
            .finish()
    }
}

impl std::error::Error for GrpcStatusError {}

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
        } else if config.descriptor_set_bytes.is_some()
            || config.descriptor_set_path.is_some()
            || config.reflection
        {
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
            GrpcConsumerInner::Dynamic(_) => (
                true,
                serde_json::json!({
                    "mode": "dynamic-client",
                    "acknowledgement_guarantee": "none"
                }),
            ),
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
            .map_err(|status| anyhow::Error::new(GrpcStatusError::from(status))),
        None => call
            .await
            .map_err(|status| anyhow::Error::new(GrpcStatusError::from(status))),
    }
}

/// Attaches the configured static metadata and credentials. Error text never
/// includes a configured value, so an unusable credential cannot leak through logs.
fn apply_call_metadata(config: &GrpcConfig, metadata: &mut MetadataMap) -> Result<()> {
    for (name, value) in &config.metadata {
        let key = MetadataKey::<Ascii>::from_bytes(name.as_bytes())
            .map_err(|error| anyhow::anyhow!("invalid gRPC metadata key '{name}': {error}"))?;
        let value = MetadataValue::<Ascii>::try_from(value.as_str()).map_err(|error| {
            anyhow::anyhow!("invalid gRPC metadata value for '{name}': {error}")
        })?;
        metadata.insert(key, value);
    }
    for (name, value) in &config.binary_metadata {
        let key = MetadataKey::<Binary>::from_bytes(name.as_bytes()).map_err(|error| {
            anyhow::anyhow!("invalid binary gRPC metadata key '{name}': {error}")
        })?;
        metadata.insert_bin(key, MetadataValue::<Binary>::from_bytes(value));
    }
    if let Some(token) = &config.bearer_token {
        let value = MetadataValue::<Ascii>::try_from(format!("Bearer {token}"))
            .map_err(|_| anyhow::anyhow!("bearer_token is not a valid gRPC metadata value"))?;
        metadata.insert("authorization", value);
    }
    if let Some(api_key) = &config.api_key {
        let name = config.api_key_name.as_deref().unwrap_or("x-api-key");
        let key = MetadataKey::<Ascii>::from_bytes(name.as_bytes())
            .map_err(|error| anyhow::anyhow!("invalid api_key_name '{name}': {error}"))?;
        let value = MetadataValue::<Ascii>::try_from(api_key.as_str())
            .map_err(|_| anyhow::anyhow!("api_key is not a valid gRPC metadata value"))?;
        metadata.insert(key, value);
    }

    Ok(())
}

/// Metadata and credentials are only attached to descriptor-driven calls. Accepting them
/// silently elsewhere would connect unauthenticated, so those modes reject them instead.
fn reject_unsupported_call_metadata(config: &GrpcConfig, mode: &str) -> Result<()> {
    let mut set = Vec::new();
    if !config.metadata.is_empty() {
        set.push("metadata");
    }
    if !config.binary_metadata.is_empty() {
        set.push("binary_metadata");
    }
    if config.bearer_token.is_some() {
        set.push("bearer_token");
    }
    if config.api_key.is_some() {
        set.push("api_key");
    }
    if set.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "gRPC {mode} mode does not send {}; these apply only to dynamic \
         descriptor-driven calls. Use TLS client certificates for the Bridge protocol.",
        set.join(", ")
    )
}

fn dynamic_request(config: &GrpcConfig, payload: Vec<u8>) -> Result<Request<Vec<u8>>> {
    let mut request = Request::new(payload);
    apply_call_metadata(config, request.metadata_mut())?;
    Ok(request)
}

/// Connects and resolves the configured service/method from a descriptor set, a
/// descriptor file, or server reflection. Shared by the dynamic source and sink so both
/// accept the same configuration and produce the same errors.
async fn resolve_dynamic_method(
    config: &GrpcConfig,
    url: &str,
) -> Result<(Channel, prost_reflect::MethodDescriptor)> {
    let service_name = config
        .service_name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("dynamic gRPC requires service_name"))?;
    let method_name = config
        .method_name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("dynamic gRPC requires method_name"))?;

    let channel = make_endpoint(config, url).await?.connect().await?;
    let request_deadline = config
        .request_timeout_ms
        .or(config.timeout_ms)
        .map(Duration::from_millis);
    let pool = if let Some(bytes) = &config.descriptor_set_bytes {
        DescriptorPool::decode(bytes.as_slice())?
    } else if let Some(path) = &config.descriptor_set_path {
        let bytes = tokio::fs::read(path).await?;
        DescriptorPool::decode(bytes.as_slice())?
    } else if config.reflection {
        reflected_descriptor_pool(config, channel.clone(), service_name, request_deadline).await?
    } else {
        anyhow::bail!(
            "dynamic gRPC requires descriptor_set_bytes, descriptor_set_path, or reflection: true"
        );
    };

    let service = pool.get_service_by_name(service_name).ok_or_else(|| {
        anyhow::anyhow!(
            "gRPC service '{}' not found in the discovered descriptors",
            service_name
        )
    })?;
    let method = service
        .methods()
        .find(|method| method.name() == method_name)
        .ok_or_else(|| {
            anyhow::anyhow!("gRPC method '{}.{}' not found", service_name, method_name)
        })?;
    Ok((channel, method))
}

/// Names an RPC's streaming shape for capability errors.
fn method_shape(method: &prost_reflect::MethodDescriptor) -> &'static str {
    match (method.is_client_streaming(), method.is_server_streaming()) {
        (true, true) => "bidirectional-streaming",
        (true, false) => "client-streaming",
        (false, true) => "server-streaming",
        (false, false) => "unary",
    }
}

/// Encodes one canonical payload as the method's protobuf input message. A payload that
/// does not match the descriptor is permanent: retrying re-encodes the same bytes.
fn encode_dynamic_input(
    method: &prost_reflect::MethodDescriptor,
    payload: &[u8],
) -> Result<Vec<u8>, PublisherError> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let message =
        DynamicMessage::deserialize(method.input(), &mut deserializer).map_err(|error| {
            PublisherError::NonRetryable(anyhow::anyhow!(
                "gRPC payload does not match '{}': {error}",
                method.input().full_name()
            ))
        })?;
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes).map_err(|error| {
        PublisherError::NonRetryable(anyhow::anyhow!("gRPC payload encode failed: {error}"))
    })?;
    Ok(bytes)
}

/// Decodes a protobuf response into a canonical message carrying the originating id, so
/// the route can correlate it as a reply.
fn decode_dynamic_output(
    method: &prost_reflect::MethodDescriptor,
    bytes: &[u8],
    correlation_id: Option<u128>,
) -> Result<CanonicalMessage, PublisherError> {
    let message = DynamicMessage::decode(method.output(), bytes).map_err(|error| {
        PublisherError::NonRetryable(anyhow::anyhow!("gRPC response decode failed: {error}"))
    })?;
    let payload = serde_json::to_vec(&message).map_err(|error| {
        PublisherError::NonRetryable(anyhow::anyhow!("gRPC response encode failed: {error}"))
    })?;
    Ok(CanonicalMessage::new(payload, correlation_id))
}

async fn reflected_descriptor_pool(
    config: &GrpcConfig,
    channel: Channel,
    service_name: &str,
    deadline: Option<Duration>,
) -> Result<DescriptorPool> {
    use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
    use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
    use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
    use tonic_reflection::pb::v1::ServerReflectionRequest;

    let reflection_request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileContainingSymbol(
            service_name.to_owned(),
        )),
    };
    // Reflection is an ordinary RPC, so a server that guards it needs the same
    // credentials as the call the descriptors are being fetched for.
    let mut request = Request::new(tokio_stream::iter([reflection_request]));
    apply_call_metadata(config, request.metadata_mut())?;
    let mut client = ServerReflectionClient::new(channel);
    let call = client.server_reflection_info(request);
    let mut responses = with_deadline(call, deadline).await?.into_inner();
    let response = with_deadline(responses.message(), deadline)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("gRPC reflection returned no descriptor for '{service_name}'")
        })?;
    let descriptors = match response.message_response {
        Some(MessageResponse::FileDescriptorResponse(response)) => response.file_descriptor_proto,
        Some(MessageResponse::ErrorResponse(error)) => anyhow::bail!(
            "gRPC reflection failed for '{service_name}' with code {}: {}",
            error.error_code,
            error.error_message
        ),
        _ => anyhow::bail!("gRPC reflection returned an unexpected response for '{service_name}'"),
    };
    let files = descriptors
        .into_iter()
        .map(|bytes| prost_types::FileDescriptorProto::decode(bytes.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_protos(files)?;
    Ok(pool)
}

/// `overall_timeout_ms` caps the lifetime of the RPC, so it is permanent: a reconnect
/// would restart the call and recompute the deadline, turning the cap into an endless
/// restart loop. The idle timeout stays retryable, where reconnecting is the point.
fn overall_deadline_exceeded() -> ConsumerError {
    ConsumerError::Permanent(anyhow::anyhow!("dynamic gRPC overall deadline exceeded"))
}

enum DynamicResponse {
    Unary(Option<Vec<u8>>),
    // Boxed: an inline `Streaming` is an order of magnitude larger than the unary arm.
    Streaming(Box<tonic::Streaming<Vec<u8>>>),
}

struct DynamicConsumer {
    response: DynamicResponse,
    output: MessageDescriptor,
    service_name: String,
    method_name: String,
    response_index: u64,
    idle_stream_timeout: Option<Duration>,
    overall_deadline: Option<tokio::time::Instant>,
    exit_on_empty: bool,
}

impl DynamicConsumer {
    async fn new(config: &GrpcConfig, url: &str) -> Result<Self> {
        let (channel, method) = resolve_dynamic_method(config, url).await?;
        let service_name = method.parent_service().full_name().to_owned();
        let method_name = method.name().to_owned();
        let request_deadline = config
            .request_timeout_ms
            .or(config.timeout_ms)
            .map(Duration::from_millis);
        if method.is_client_streaming() {
            anyhow::bail!(
                "dynamic gRPC method '{}.{}' is {}; a gRPC *input* consumes responses, so it \
                 supports unary and server-streaming methods only. A method that streams \
                 requests is a sink: use it as the route's output instead",
                service_name,
                method_name,
                method_shape(&method)
            );
        }
        if config.server_streaming && !method.is_server_streaming() {
            warn!(
                service = service_name,
                method = method_name,
                "Ignoring deprecated server_streaming hint; the RPC shape is descriptor-derived"
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

        let mut client = tonic::client::Grpc::new(channel);
        if let Some(max) = config.max_decoding_message_size {
            client = client.max_decoding_message_size(max);
        }
        if let Some(max) = config.max_encoding_message_size {
            client = client.max_encoding_message_size(max);
        }
        client
            .ready()
            .await
            .map_err(|error| anyhow::anyhow!("dynamic gRPC service was not ready: {error}"))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_maybe_shared(format!(
            "/{service_name}/{method_name}"
        ))?;
        let overall_timeout = config.overall_timeout_ms.map(Duration::from_millis);
        let overall_deadline = overall_timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        let response = if method.is_server_streaming() {
            let call = client.server_streaming(
                dynamic_request(config, request_bytes)?,
                path,
                RawProtobufCodec,
            );
            DynamicResponse::Streaming(Box::new(
                with_deadline(call, request_deadline).await?.into_inner(),
            ))
        } else {
            let call = client.unary(
                dynamic_request(config, request_bytes)?,
                path,
                RawProtobufCodec,
            );
            DynamicResponse::Unary(Some(
                with_deadline(call, request_deadline).await?.into_inner(),
            ))
        };
        Ok(Self {
            response,
            output: method.output(),
            service_name: service_name.to_owned(),
            method_name: method_name.to_owned(),
            response_index: 0,
            idle_stream_timeout: config.idle_stream_timeout_ms.map(Duration::from_millis),
            overall_deadline,
            exit_on_empty: false,
        })
    }

    /// A body that does not match the descriptor is a permanent error, not a connection
    /// one: reconnecting re-reads the same bytes and fails identically.
    fn decode_message(&mut self, bytes: &[u8]) -> Result<CanonicalMessage, ConsumerError> {
        // Claim the position before decoding. Ids are advertised as deterministic, so the
        // index has to follow stream position; a skipped response would otherwise shift
        // every id after it.
        let index = self.response_index;
        self.response_index = self.response_index.saturating_add(1);

        let message = DynamicMessage::decode(self.output.clone(), bytes)
            .map_err(|error| ConsumerError::Permanent(error.into()))?;
        let payload =
            serde_json::to_vec(&message).map_err(|error| ConsumerError::Permanent(error.into()))?;

        let mut hasher = Sha256::new();
        hasher.update(b"mqbridge.dynamic-response.v1\0");
        hasher.update(self.service_name.as_bytes());
        hasher.update(b"\0");
        hasher.update(self.method_name.as_bytes());
        hasher.update(b"\0");
        hasher.update(index.to_be_bytes());
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);

        Ok(
            CanonicalMessage::new(payload, Some(u128::from_be_bytes(id)))
                .with_metadata_kv("grpc.service", self.service_name.clone())
                .with_metadata_kv("grpc.method", self.method_name.clone())
                .with_metadata_kv("grpc.response_index", index.to_string())
                .with_metadata_kv("grpc.ack_guarantee", "none"),
        )
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
        let idle_timeout = self.idle_stream_timeout;
        let overall_deadline = self.overall_deadline;
        let exit_on_empty = self.exit_on_empty;
        match &mut self.response {
            DynamicResponse::Unary(message) => {
                if let Some(message) = message.take() {
                    raw.push(message);
                }
            }
            DynamicResponse::Streaming(stream) => {
                while raw.len() < max_messages {
                    let overall_remaining = overall_deadline.map(|deadline| {
                        deadline.saturating_duration_since(tokio::time::Instant::now())
                    });
                    if overall_remaining == Some(Duration::ZERO) {
                        return Err(overall_deadline_exceeded());
                    }
                    let next_result = if raw.is_empty() {
                        let wait = crate::traits::drain_gated(exit_on_empty, stream.message());
                        let timeout = match (idle_timeout, overall_remaining) {
                            (Some(idle), Some(overall)) => Some(idle.min(overall)),
                            (Some(idle), None) => Some(idle),
                            (None, Some(overall)) => Some(overall),
                            (None, None) => None,
                        };
                        let next = match timeout {
                            Some(timeout) => {
                                tokio::time::timeout(timeout, wait).await.map_err(|_| {
                                    if overall_deadline.is_some_and(|deadline| {
                                        tokio::time::Instant::now() >= deadline
                                    }) {
                                        overall_deadline_exceeded()
                                    } else {
                                        ConsumerError::Connection(anyhow::anyhow!(
                                            "dynamic gRPC response stream idle timeout exceeded"
                                        ))
                                    }
                                })?
                            }
                            None => wait.await,
                        };
                        match next {
                            Some(result) => result,
                            None => return Ok(crate::outcomes::ReceivedBatch::empty()),
                        }
                    } else {
                        let poll = Duration::from_millis(GRPC_BATCH_POLL_MS);
                        let timeout =
                            overall_remaining.map_or(poll, |remaining| poll.min(remaining));
                        match tokio::time::timeout(timeout, stream.message()).await {
                            Ok(result) => result,
                            Err(_) if timeout == poll => break,
                            Err(_) => return Err(overall_deadline_exceeded()),
                        }
                    };
                    match next_result {
                        Ok(Some(message)) => raw.push(message),
                        Ok(None) => break,
                        Err(status) => {
                            return Err(ConsumerError::Connection(anyhow::Error::new(
                                GrpcStatusError::from(status),
                            )))
                        }
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
            // Dynamic services define no acknowledgement operation. This only tells the
            // route that the already-received response may be released locally.
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
        reject_unsupported_call_metadata(config, "Bridge client")?;
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
        let request_timeout = config
            .request_timeout_ms
            .or(config.timeout_ms)
            .map(Duration::from_millis);
        let stream = if let Some(timeout) = request_timeout {
            tokio::time::timeout(timeout, client.subscribe(request))
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

const REFLECTION_V1_PREFIX: &str = "/grpc.reflection.v1.ServerReflection/";
const REFLECTION_V1ALPHA_PREFIX: &str = "/grpc.reflection.v1alpha.ServerReflection/";

/// Sends one gRPC path prefix to `matched` and everything else to `fallback`, so hosting
/// reflection alongside the Bridge service does not pull in tonic's axum-backed router.
///
/// Every generated tonic service shares one response type and already returns a boxed
/// future, so this dispatches by delegation alone — no wrapping, no extra allocation.
/// Nest it to add further services.
#[derive(Clone)]
struct PrefixRouter<F, M> {
    fallback: F,
    prefix: &'static str,
    matched: M,
}

impl<F, M, B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for PrefixRouter<F, M>
where
    F: tonic::codegen::Service<tonic::codegen::http::Request<B>>,
    M: tonic::codegen::Service<
        tonic::codegen::http::Request<B>,
        Response = F::Response,
        Error = F::Error,
        Future = F::Future,
    >,
{
    type Response = F::Response;
    type Error = F::Error;
    type Future = F::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self.fallback.poll_ready(cx) {
            std::task::Poll::Ready(Ok(())) => self.matched.poll_ready(cx),
            other => other,
        }
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        // Unmatched paths go to the fallback, whose generated service answers anything it
        // does not recognise with UNIMPLEMENTED.
        if request.uri().path().starts_with(self.prefix) {
            self.matched.call(request)
        } else {
            self.fallback.call(request)
        }
    }
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
    request_timeout_ms: Option<u64>,
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
        reject_unsupported_call_metadata(config, "server")?;
        let key = GrpcServerKey {
            listen_addr: parse_addr(url)?.to_string(),
            tls: config.tls.clone(),
            request_timeout_ms: config.request_timeout_ms.or(config.timeout_ms),
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
    if let Some(ms) = config.request_timeout_ms.or(config.timeout_ms) {
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
        commit_timeout: config
            .request_timeout_ms
            .or(config.timeout_ms)
            .map(Duration::from_millis),
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

    // Both reflection versions: v1 for current tooling, v1alpha for older grpcurl/evans.
    let configure_reflection = || {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
    };
    let services = PrefixRouter {
        fallback: PrefixRouter {
            fallback: service,
            prefix: REFLECTION_V1_PREFIX,
            matched: configure_reflection().build_v1()?,
        },
        prefix: REFLECTION_V1ALPHA_PREFIX,
        matched: configure_reflection().build_v1alpha()?,
    };
    let handle = tokio::spawn(async move {
        info!(server_addr = %local, "gRPC embedded server starting to serve");
        if let Err(e) = builder.serve_with_incoming(services, incoming).await {
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

/// Builds the gRPC output for a route: a descriptor-driven call when a descriptor source
/// is configured, otherwise the built-in Bridge publisher.
pub async fn create_grpc_publisher(config: &GrpcConfig) -> Result<Box<dyn MessagePublisher>> {
    if config.descriptor_set_bytes.is_some()
        || config.descriptor_set_path.is_some()
        || config.reflection
    {
        let url = config.tls.normalize_url(&config.url);
        Ok(Box::new(DynamicPublisher::new(config, &url).await?))
    } else {
        Ok(Box::new(GrpcPublisher::new(config).await?))
    }
}

// ── Dynamic publisher ─────────────────────────────────────────────────────────

/// In-flight unary sends per batch. Matches `GRPC_ACK_CONCURRENCY`: both bound work that
/// one HTTP/2 connection multiplexes.
const GRPC_DYNAMIC_SEND_CONCURRENCY: usize = 64;

/// Calls an arbitrary descriptor-defined method as a route's output.
///
/// Unary methods make one call per message. Client-streaming methods make one call per
/// batch, which is also the acknowledgement granularity: the single reply covers every
/// message in the batch, and a failure part-way through cannot say which ones the server
/// already consumed, so a retry redelivers all of them.
struct DynamicPublisher {
    client: tonic::client::Grpc<Channel>,
    method: prost_reflect::MethodDescriptor,
    path: tonic::codegen::http::uri::PathAndQuery,
    config: GrpcConfig,
    service_name: String,
    method_name: String,
    request_timeout: Option<Duration>,
    overall_timeout: Option<Duration>,
}

impl DynamicPublisher {
    async fn new(config: &GrpcConfig, url: &str) -> Result<Self> {
        let (channel, method) = resolve_dynamic_method(config, url).await?;
        let service_name = method.parent_service().full_name().to_owned();
        let method_name = method.name().to_owned();
        if method.is_server_streaming() {
            anyhow::bail!(
                "dynamic gRPC method '{}.{}' is {}; a gRPC *output* publishes messages and \
                 consumes one reply, so it supports unary and client-streaming methods only. \
                 A method that streams responses is a source: use it as the route's input instead",
                service_name,
                method_name,
                method_shape(&method)
            );
        }
        // A bare `request:` in YAML deserializes to null; only a real value is a mistake here.
        if config
            .request
            .as_ref()
            .is_some_and(|value| !value.is_null())
        {
            anyhow::bail!(
                "dynamic gRPC output does not use `request`: the published messages are the \
                 requests. Remove `request`, or move this endpoint to the route's input"
            );
        }

        let mut client = tonic::client::Grpc::new(channel);
        if let Some(max) = config.max_decoding_message_size {
            client = client.max_decoding_message_size(max);
        }
        if let Some(max) = config.max_encoding_message_size {
            client = client.max_encoding_message_size(max);
        }
        let path = tonic::codegen::http::uri::PathAndQuery::from_maybe_shared(format!(
            "/{service_name}/{method_name}"
        ))?;

        Ok(Self {
            client,
            method,
            path,
            config: config.clone(),
            service_name,
            method_name,
            request_timeout: config
                .request_timeout_ms
                .or(config.timeout_ms)
                .map(Duration::from_millis),
            overall_timeout: config.overall_timeout_ms.map(Duration::from_millis),
        })
    }

    async fn send_unary(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let results = futures::stream::iter(messages.into_iter().map(|message| async move {
            let bytes = match encode_dynamic_input(&self.method, &message.payload) {
                Ok(bytes) => bytes,
                Err(error) => return (message, Err(error)),
            };
            let request = match dynamic_request(&self.config, bytes) {
                Ok(request) => request,
                Err(error) => return (message, Err(PublisherError::NonRetryable(error))),
            };
            let mut client = self.client.clone();
            if let Err(error) = client.ready().await {
                return (
                    message,
                    Err(PublisherError::Retryable(anyhow::anyhow!(
                        "dynamic gRPC service was not ready: {error}"
                    ))),
                );
            }
            let call = client.unary(request, self.path.clone(), RawProtobufCodec);
            let response = match self.request_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, call).await {
                    Ok(response) => response,
                    Err(_) => {
                        return (
                            message,
                            Err(PublisherError::Retryable(anyhow::anyhow!(
                                "dynamic gRPC call timed out"
                            ))),
                        )
                    }
                },
                None => call.await,
            };
            match response {
                Ok(response) => {
                    let reply = decode_dynamic_output(
                        &self.method,
                        response.get_ref(),
                        Some(message.message_id),
                    );
                    match reply {
                        Ok(reply) => (message, Ok(reply)),
                        Err(error) => (message, Err(error)),
                    }
                }
                Err(status) => (message, Err(status_to_publisher_error(status))),
            }
        }))
        .buffered(GRPC_DYNAMIC_SEND_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        let mut responses = Vec::with_capacity(results.len());
        let mut failed = Vec::new();
        for (message, result) in results {
            match result {
                Ok(reply) => responses.push(reply),
                Err(error) => failed.push((message, error)),
            }
        }
        Ok(SentBatch::Partial {
            responses: Some(responses),
            failed,
        })
    }

    async fn send_client_streaming(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        // Encode everything first: a payload that does not match the descriptor fails
        // permanently, and finding that out mid-stream would leave a half-sent RPC.
        let mut encoded = Vec::with_capacity(messages.len());
        let mut failed = Vec::new();
        let mut streamed = Vec::with_capacity(messages.len());
        for message in messages {
            match encode_dynamic_input(&self.method, &message.payload) {
                Ok(bytes) => {
                    encoded.push(bytes);
                    streamed.push(message);
                }
                Err(error) => failed.push((message, error)),
            }
        }
        if encoded.is_empty() {
            return Ok(SentBatch::Partial {
                responses: Some(Vec::new()),
                failed,
            });
        }

        let correlation_id = streamed.first().map(|message| message.message_id);
        let request = dynamic_request(&self.config, Vec::new())
            .map_err(PublisherError::NonRetryable)?
            .map(|_| tokio_stream::iter(encoded));
        let mut client = self.client.clone();
        client.ready().await.map_err(|error| {
            PublisherError::Retryable(anyhow::anyhow!(
                "dynamic gRPC service was not ready: {error}"
            ))
        })?;
        let call = client.client_streaming(request, self.path.clone(), RawProtobufCodec);
        let response = match self.request_timeout {
            Some(timeout) => tokio::time::timeout(timeout, call).await.map_err(|_| {
                PublisherError::Retryable(anyhow::anyhow!("dynamic gRPC call timed out"))
            })?,
            None => call.await,
        };

        match response {
            Ok(response) => {
                let reply =
                    decode_dynamic_output(&self.method, response.get_ref(), correlation_id)?;
                Ok(SentBatch::Partial {
                    responses: Some(vec![reply]),
                    failed,
                })
            }
            // One reply covers the whole stream, so a failure fails every message in it.
            Err(status) => {
                let error = status_to_publisher_error(status);
                let message = error.to_string();
                failed.extend(streamed.into_iter().map(|sent| {
                    (
                        sent,
                        match error {
                            PublisherError::NonRetryable(_) => {
                                PublisherError::NonRetryable(anyhow::anyhow!(message.clone()))
                            }
                            _ => PublisherError::Retryable(anyhow::anyhow!(message.clone())),
                        },
                    )
                }));
                Ok(SentBatch::Partial {
                    responses: Some(Vec::new()),
                    failed,
                })
            }
        }
    }
}

/// gRPC codes that mean "this request will never succeed" become non-retryable so the
/// route dead-letters instead of replaying a request the server already rejected.
fn status_to_publisher_error(status: Status) -> PublisherError {
    let permanent = matches!(
        status.code(),
        tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::AlreadyExists
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::FailedPrecondition
            | tonic::Code::OutOfRange
            | tonic::Code::Unimplemented
    );
    let error = anyhow::Error::new(GrpcStatusError::from(status));
    if permanent {
        PublisherError::NonRetryable(error)
    } else {
        PublisherError::Retryable(error)
    }
}

#[async_trait]
impl MessagePublisher for DynamicPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        let send = async {
            if self.method.is_client_streaming() {
                self.send_client_streaming(messages).await
            } else {
                self.send_unary(messages).await
            }
        };
        match self.overall_timeout {
            Some(timeout) => tokio::time::timeout(timeout, send).await.map_err(|_| {
                PublisherError::Retryable(anyhow::anyhow!("dynamic gRPC batch timed out"))
            })?,
            None => send.await,
        }
    }

    async fn status(&self) -> crate::traits::EndpointStatus {
        crate::traits::EndpointStatus {
            healthy: true,
            details: serde_json::json!({
                "mode": "dynamic-client",
                "service": self.service_name,
                "method": self.method_name,
                "shape": method_shape(&self.method),
                "acknowledgement_guarantee": if self.method.is_client_streaming() {
                    "batch"
                } else {
                    "per-message"
                },
            }),
            ..Default::default()
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
    request_timeout: Option<Duration>,
    overall_timeout: Option<Duration>,
    topic: Option<String>,
}

impl GrpcPublisher {
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        reject_unsupported_call_metadata(config, "Bridge publisher")?;
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
            config.connect_timeout_ms.or(config.timeout_ms),
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
            request_timeout: config
                .request_timeout_ms
                .or(config.timeout_ms)
                .map(Duration::from_millis),
            overall_timeout: config
                .overall_timeout_ms
                .or(config.timeout_ms)
                .map(Duration::from_millis),
            topic: Some(
                config
                    .topic
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
            ),
        })
    }

    /// Opens the response stream, bounding only call setup. `overall_timeout` bounds the
    /// response-handling phase separately in `send_batch`.
    async fn publish_batch_stream(
        &self,
        messages: Vec<BridgeMessage>,
    ) -> Result<tonic::Streaming<proto::PublishResponse>, PublisherError> {
        let mut client = self.client.clone();
        let call = client.publish_batch(tokio_stream::iter(messages));
        let response = match self.request_timeout {
            Some(timeout) => tokio::time::timeout(timeout, call).await.map_err(|_| {
                PublisherError::Retryable(anyhow::anyhow!("gRPC publish request timed out"))
            })?,
            None => call.await,
        }
        .map_err(|status| {
            PublisherError::Retryable(anyhow::anyhow!("gRPC publish_batch error: {status:?}"))
        })?;
        Ok(response.into_inner())
    }
}

#[async_trait]
impl MessagePublisher for GrpcPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
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

        let process_fut = async {
            let mut stream = self.publish_batch_stream(bridge_messages_vec).await?;
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
        ) = if let Some(timeout) = self.overall_timeout {
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

    if let Some(ms) = config.connect_timeout_ms.or(config.timeout_ms) {
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
    async fn acknowledge_and_batch_streaming_round_trip() {
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

        let mut consumer = ClientModeConsumer::new(&config, &config.url)
            .await
            .expect("Failed to create ClientModeConsumer");
        let publisher = GrpcPublisher::new(&config)
            .await
            .expect("Failed to create GrpcPublisher");

        let msgs = vec![
            CanonicalMessage::new("batch_1".into(), None),
            CanonicalMessage::new("batch_2".into(), None),
        ];

        // The mock answers with an Ack variant, which maps to SentBatch::Ack.
        let sent_result = publisher.send_batch(msgs).await;
        assert!(matches!(sent_result, Ok(SentBatch::Ack)));

        let received = tokio::time::timeout(Duration::from_secs(1), consumer.receive_batch(2))
            .await
            .expect("subscription timed out")
            .expect("subscription failed");
        assert_eq!(received.messages.len(), 2);
        (received.commit)(vec![MessageDisposition::Ack; 2])
            .await
            .expect("acknowledge failed");

        // Explicit Acknowledge, outside the route commit path.
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

    mod dynamic_fixture {
        tonic::include_proto!("mqbridge.test.v1");
        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("grpc_dynamic_test_descriptor");
    }

    #[derive(Default)]
    struct DynamicFixtureService;

    #[tonic::async_trait]
    impl dynamic_fixture::dynamic_fixture_server::DynamicFixture for DynamicFixtureService {
        async fn unary(
            &self,
            request: Request<dynamic_fixture::DynamicRequest>,
        ) -> Result<Response<dynamic_fixture::DynamicResponse>, Status> {
            if request.get_ref().sequence == 98 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if request.get_ref().sequence == -1 {
                let mut metadata = tonic::metadata::MetadataMap::new();
                metadata.insert("error-detail", "secret-trailer".parse().unwrap());
                return Err(Status::with_metadata(
                    tonic::Code::InvalidArgument,
                    "invalid fixture request",
                    metadata,
                ));
            }
            let auth = request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            Ok(Response::new(dynamic_fixture::DynamicResponse {
                data: request.get_ref().data.clone(),
                sequence: request.get_ref().sequence,
                auth,
            }))
        }

        type StreamStream = std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<dynamic_fixture::DynamicResponse, Status>> + Send,
            >,
        >;

        async fn stream(
            &self,
            request: Request<dynamic_fixture::DynamicRequest>,
        ) -> Result<Response<Self::StreamStream>, Status> {
            let input = request.into_inner();
            if input.sequence == 99 {
                let response = dynamic_fixture::DynamicResponse {
                    data: input.data,
                    sequence: input.sequence,
                    auth: String::new(),
                };
                return Ok(Response::new(Box::pin(futures::stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(response)
                }))));
            }
            let responses = (0..2).map(move |offset| {
                Ok(dynamic_fixture::DynamicResponse {
                    data: input.data.clone(),
                    sequence: input.sequence + offset,
                    auth: String::new(),
                })
            });
            Ok(Response::new(Box::pin(tokio_stream::iter(responses))))
        }

        /// Sums the sequences it received and echoes the count via `data`, so a test can
        /// prove every streamed request arrived in one RPC.
        async fn client_stream(
            &self,
            request: Request<tonic::Streaming<dynamic_fixture::DynamicRequest>>,
        ) -> Result<Response<dynamic_fixture::DynamicResponse>, Status> {
            let auth = request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            let mut stream = request.into_inner();
            let mut total = 0_i64;
            let mut count = 0_u8;
            while let Some(message) = stream.message().await? {
                if message.sequence == -1 {
                    return Err(Status::invalid_argument("fixture rejects sequence -1"));
                }
                total += message.sequence;
                count += 1;
            }
            Ok(Response::new(dynamic_fixture::DynamicResponse {
                data: vec![count],
                sequence: total,
                auth,
            }))
        }

        type BidiStream = std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<dynamic_fixture::DynamicResponse, Status>> + Send,
            >,
        >;

        async fn bidi(
            &self,
            _request: Request<tonic::Streaming<dynamic_fixture::DynamicRequest>>,
        ) -> Result<Response<Self::BidiStream>, Status> {
            Err(Status::unimplemented("fixture"))
        }
    }

    async fn dynamic_fixture_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dynamic_fixture::FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();
        let handle = tokio::spawn(async move {
            TonicServer::builder()
                .serve_with_incoming(
                    PrefixRouter {
                        fallback:
                            dynamic_fixture::dynamic_fixture_server::DynamicFixtureServer::new(
                                DynamicFixtureService,
                            ),
                        prefix: REFLECTION_V1_PREFIX,
                        matched: reflection,
                    },
                    incoming,
                )
                .await
                .unwrap();
        });
        (address, handle)
    }

    fn dynamic_config(address: std::net::SocketAddr, method: &str) -> GrpcConfig {
        GrpcConfig::new(format!("http://{address}"))
            .with_descriptor_set_bytes(dynamic_fixture::FILE_DESCRIPTOR_SET.to_vec())
            .with_service_name("mqbridge.test.v1.DynamicFixture")
            .with_method_name(method)
            .with_request(serde_json::json!({
                "data": "aGVsbG8=",
                "sequence": "7"
            }))
    }

    #[tokio::test]
    async fn dynamic_unary_uses_canonical_json_metadata_and_stable_ids() {
        let (address, handle) = dynamic_fixture_server().await;
        let mut config = dynamic_config(address, "Unary")
            .with_bearer_token("test-token")
            .with_metadata(HashMap::from([("x-static".into(), "value".into())]));
        config.server_streaming = true; // Deprecated hint must not override the descriptor.

        let mut first = DynamicConsumer::new(&config, &config.url).await.unwrap();
        let first_batch = first.receive_batch(1).await.unwrap();
        let first_message = &first_batch.messages[0];
        let json: serde_json::Value = serde_json::from_slice(&first_message.payload).unwrap();
        assert_eq!(json["data"], "aGVsbG8=");
        assert_eq!(json["sequence"], "7");
        assert_eq!(json["auth"], "Bearer test-token");
        assert_eq!(first_message.metadata["grpc.ack_guarantee"], "none");
        assert_eq!(first_message.metadata["grpc.response_index"], "0");

        let mut second = DynamicConsumer::new(&config, &config.url).await.unwrap();
        let second_batch = second.receive_batch(1).await.unwrap();
        assert_eq!(
            first_message.message_id, second_batch.messages[0].message_id,
            "the same RPC response must have a deterministic id"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_server_streaming_is_descriptor_derived() {
        let (address, handle) = dynamic_fixture_server().await;
        let config = dynamic_config(address, "Stream");
        let mut consumer = DynamicConsumer::new(&config, &config.url).await.unwrap();
        let batch = consumer.receive_batch(8).await.unwrap();
        assert_eq!(batch.messages.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&batch.messages[0].payload).unwrap();
        let second: serde_json::Value = serde_json::from_slice(&batch.messages[1].payload).unwrap();
        assert_eq!(first["sequence"], "7");
        assert_eq!(second["sequence"], "8");
        handle.abort();
    }

    /// A concrete free port, so each embedded server gets its own registry entry:
    /// `GrpcServerKey` keys on the literal address, so two `127.0.0.1:0` consumers share
    /// one server and the first of them to drop tears it down under the other.
    async fn free_server_url() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        format!("http://127.0.0.1:{port}")
    }

    /// `PrefixRouter` replaces tonic's axum router, so every branch it dispatches has to
    /// be exercised: the Bridge service, reflection v1, and reflection v1alpha.
    #[tokio::test]
    async fn prefix_router_dispatches_bridge_and_both_reflection_versions() {
        use tonic_reflection::pb::v1alpha::server_reflection_client::ServerReflectionClient;
        use tonic_reflection::pb::v1alpha::server_reflection_request::MessageRequest;
        use tonic_reflection::pb::v1alpha::server_reflection_response::MessageResponse;
        use tonic_reflection::pb::v1alpha::ServerReflectionRequest;

        let mut consumer = GrpcConsumer::new(&GrpcConfig {
            url: free_server_url().await,
            topic: Some("router".into()),
            server_mode: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let address = consumer.bound_addr.unwrap();
        let url = format!("http://{address}");

        // v1: the descriptor-discovery path a dynamic source uses.
        let channel = tonic::transport::Endpoint::from_shared(url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let pool = reflected_descriptor_pool(
            &GrpcConfig::new(url.clone()),
            channel.clone(),
            "mqbridge.Bridge",
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("v1 reflection must route to the reflection service");
        assert!(pool.get_service_by_name("mqbridge.Bridge").is_some());

        // v1alpha: same descriptors over the older path older tooling still uses.
        let mut v1alpha = ServerReflectionClient::new(channel);
        let response = v1alpha
            .server_reflection_info(tokio_stream::iter([ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::FileContainingSymbol(
                    "mqbridge.Bridge".to_owned(),
                )),
            }]))
            .await
            .expect("v1alpha reflection must route to the reflection service")
            .into_inner()
            .message()
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            response.message_response,
            Some(MessageResponse::FileDescriptorResponse(_))
        ));

        // Fallback branch: a Bridge RPC must still reach the Bridge service.
        let publisher = GrpcPublisher::new(&GrpcConfig {
            url: url.clone(),
            topic: Some("router".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        let sent = tokio::spawn(async move {
            publisher
                .send_batch(vec![CanonicalMessage::new("routed".into(), None)])
                .await
        });
        let batch = tokio::time::timeout(Duration::from_secs(5), consumer.receive_batch(1))
            .await
            .expect("Bridge publish did not reach the fallback branch")
            .unwrap();
        assert_eq!(batch.messages[0].payload.as_ref(), b"routed");
        (batch.commit)(vec![MessageDisposition::Ack]).await.unwrap();
        sent.await.unwrap().expect("Bridge publish failed");
    }

    #[tokio::test]
    async fn dynamic_unary_output_calls_once_per_message_and_returns_replies() {
        let (address, handle) = dynamic_fixture_server().await;
        let config = dynamic_config(address, "Unary")
            .with_bearer_token("sink-token")
            .with_request(serde_json::Value::Null);
        let publisher = DynamicPublisher::new(&config, &config.url).await.unwrap();

        let sent = publisher
            .send_batch(vec![
                CanonicalMessage::new(br#"{"data":"aGk=","sequence":"1"}"#.to_vec(), Some(11)),
                CanonicalMessage::new(br#"{"data":"aGk=","sequence":"2"}"#.to_vec(), Some(22)),
            ])
            .await
            .unwrap();

        let SentBatch::Partial { responses, failed } = sent else {
            panic!("a unary sink replies per message");
        };
        assert!(failed.is_empty(), "{failed:?}");
        let responses = responses.unwrap();
        assert_eq!(responses.len(), 2);
        // Replies carry the originating id so the route can correlate them.
        let mut ids: Vec<_> = responses.iter().map(|reply| reply.message_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 22]);
        for reply in &responses {
            let json: serde_json::Value = serde_json::from_slice(&reply.payload).unwrap();
            assert_eq!(json["auth"], "Bearer sink-token");
        }
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_client_streaming_output_sends_one_batch_per_rpc() {
        let (address, handle) = dynamic_fixture_server().await;
        let config = dynamic_config(address, "ClientStream").with_request(serde_json::Value::Null);
        let publisher = DynamicPublisher::new(&config, &config.url).await.unwrap();

        let sent = publisher
            .send_batch(vec![
                CanonicalMessage::new(br#"{"data":"","sequence":"3"}"#.to_vec(), Some(7)),
                CanonicalMessage::new(br#"{"data":"","sequence":"4"}"#.to_vec(), Some(8)),
                CanonicalMessage::new(br#"{"data":"","sequence":"5"}"#.to_vec(), Some(9)),
            ])
            .await
            .unwrap();

        let SentBatch::Partial { responses, failed } = sent else {
            panic!("a client-streaming sink replies once per batch");
        };
        assert!(failed.is_empty(), "{failed:?}");
        let responses = responses.unwrap();
        assert_eq!(responses.len(), 1, "one reply covers the whole batch");
        let json: serde_json::Value = serde_json::from_slice(&responses[0].payload).unwrap();
        // The fixture sums sequences and reports how many requests one RPC carried.
        assert_eq!(json["sequence"], "12");
        assert_eq!(json["data"], "Aw==", "one RPC carried all three requests");
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_output_failures_are_classified_and_scoped() {
        let (address, handle) = dynamic_fixture_server().await;

        // A payload that does not match the descriptor fails permanently, on its own,
        // without stopping the messages around it.
        let unary = dynamic_config(address, "Unary").with_request(serde_json::Value::Null);
        let publisher = DynamicPublisher::new(&unary, &unary.url).await.unwrap();
        let sent = publisher
            .send_batch(vec![
                CanonicalMessage::new(br#"{"nope":1}"#.to_vec(), Some(1)),
                CanonicalMessage::new(br#"{"data":"","sequence":"5"}"#.to_vec(), Some(2)),
            ])
            .await
            .unwrap();
        let SentBatch::Partial { responses, failed } = sent else {
            panic!("expected per-message outcomes");
        };
        assert_eq!(responses.unwrap().len(), 1, "the good message still went");
        assert_eq!(failed.len(), 1);
        assert!(matches!(failed[0].1, PublisherError::NonRetryable(_)));

        // INVALID_ARGUMENT is permanent, and on a client-streaming RPC the one reply
        // covers the batch, so every streamed message fails with it.
        let streaming =
            dynamic_config(address, "ClientStream").with_request(serde_json::Value::Null);
        let publisher = DynamicPublisher::new(&streaming, &streaming.url)
            .await
            .unwrap();
        let sent = publisher
            .send_batch(vec![
                CanonicalMessage::new(br#"{"data":"","sequence":"1"}"#.to_vec(), Some(3)),
                CanonicalMessage::new(br#"{"data":"","sequence":"-1"}"#.to_vec(), Some(4)),
            ])
            .await
            .unwrap();
        let SentBatch::Partial { failed, .. } = sent else {
            panic!("expected a failed batch");
        };
        assert_eq!(failed.len(), 2, "batch granularity fails the whole stream");
        assert!(failed
            .iter()
            .all(|(_, error)| matches!(error, PublisherError::NonRetryable(_))));
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_output_rejects_source_shaped_methods_and_request() {
        let (address, handle) = dynamic_fixture_server().await;

        for method in ["Stream", "Bidi"] {
            let config = dynamic_config(address, method).with_request(serde_json::Value::Null);
            let error = DynamicPublisher::new(&config, &config.url)
                .await
                .err()
                .unwrap();
            assert!(
                error.to_string().contains("use it as the route's input"),
                "{error:#}"
            );
        }

        // `request` belongs to a source; on a sink the messages are the requests.
        let config = dynamic_config(address, "Unary");
        let error = DynamicPublisher::new(&config, &config.url)
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("does not use `request`"),
            "{error:#}"
        );

        // The mirror image: a client-streaming method used as an input.
        let config = dynamic_config(address, "ClientStream");
        let error = DynamicConsumer::new(&config, &config.url)
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("use it as the route's output"),
            "{error:#}"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_reflection_and_capability_errors_are_explicit() {
        let (address, handle) = dynamic_fixture_server().await;
        let mut reflected = dynamic_config(address, "Unary");
        reflected.descriptor_set_bytes = None;
        reflected.reflection = true;
        DynamicConsumer::new(&reflected, &reflected.url)
            .await
            .expect("reflection should discover the fixture");

        for (method, shape) in [
            ("ClientStream", "client-streaming"),
            ("Bidi", "bidirectional-streaming"),
        ] {
            let config = dynamic_config(address, method);
            let error = DynamicConsumer::new(&config, &config.url)
                .await
                .err()
                .unwrap();
            assert!(error.to_string().contains(shape), "{error:#}");
            assert!(error
                .to_string()
                .contains("supports unary and server-streaming"));
        }
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_status_preserves_code_message_and_trailing_metadata() {
        let (address, handle) = dynamic_fixture_server().await;
        let mut config = dynamic_config(address, "Unary");
        config.request = Some(serde_json::json!({"data": "", "sequence": "-1"}));
        let error = DynamicConsumer::new(&config, &config.url)
            .await
            .err()
            .unwrap();
        let status = error
            .downcast_ref::<GrpcStatusError>()
            .expect("structured gRPC status");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "invalid fixture request");
        assert_eq!(
            status
                .trailing_metadata()
                .get("error-detail")
                .unwrap()
                .to_str()
                .unwrap(),
            "secret-trailer"
        );
        assert!(!format!("{status:?}").contains("secret-trailer"));
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_request_and_idle_stream_deadlines_are_separate() {
        let (address, handle) = dynamic_fixture_server().await;

        let mut request_timeout = dynamic_config(address, "Unary");
        request_timeout.request = Some(serde_json::json!({"data": "", "sequence": "98"}));
        request_timeout.request_timeout_ms = Some(10);
        let error = DynamicConsumer::new(&request_timeout, &request_timeout.url)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("call timed out"), "{error:#}");

        let mut idle_timeout = dynamic_config(address, "Stream");
        idle_timeout.request = Some(serde_json::json!({"data": "", "sequence": "99"}));
        idle_timeout.request_timeout_ms = Some(1_000);
        idle_timeout.idle_stream_timeout_ms = Some(10);
        let mut consumer = DynamicConsumer::new(&idle_timeout, &idle_timeout.url)
            .await
            .unwrap();
        let error = consumer.receive_batch(1).await.err().unwrap();
        assert!(error.to_string().contains("idle timeout"), "{error}");

        let mut legacy_timeout = dynamic_config(address, "Stream");
        legacy_timeout.request = Some(serde_json::json!({"data": "", "sequence": "99"}));
        legacy_timeout.timeout_ms = Some(10);
        legacy_timeout.request_timeout_ms = Some(1_000);
        let mut consumer = DynamicConsumer::new(&legacy_timeout, &legacy_timeout.url)
            .await
            .unwrap();
        let batch = consumer.receive_batch(1).await.unwrap();
        assert_eq!(batch.messages.len(), 1);
        handle.abort();
    }

    #[tokio::test]
    async fn dynamic_overall_deadline_stops_the_route_instead_of_reconnecting() {
        let (address, handle) = dynamic_fixture_server().await;
        let mut config = dynamic_config(address, "Stream");
        config.request = Some(serde_json::json!({"data": "", "sequence": "99"}));
        config.overall_timeout_ms = Some(10);

        let mut consumer = DynamicConsumer::new(&config, &config.url).await.unwrap();
        let error = consumer.receive_batch(1).await.err().unwrap();
        // Connection would make the route reconnect, restarting the RPC and resetting the
        // very cap that just fired.
        assert!(
            matches!(error, ConsumerError::Permanent(_)),
            "overall deadline must be terminal, got {error:?}"
        );
        assert!(error.to_string().contains("overall deadline exceeded"));
        handle.abort();
    }

    #[tokio::test]
    async fn bridge_modes_reject_dynamic_only_credentials() {
        let base = GrpcConfig::new("http://127.0.0.1:1".to_string()).with_topic("orders");

        for (label, config) in [
            ("bearer_token", base.clone().with_bearer_token("token")),
            ("api_key", base.clone().with_api_key("key")),
            (
                "metadata",
                base.clone()
                    .with_metadata(HashMap::from([("x-tenant".into(), "acme".into())])),
            ),
            (
                "binary_metadata",
                base.clone()
                    .with_binary_metadata(HashMap::from([("x-trace-bin".into(), vec![1_u8])])),
            ),
        ] {
            let publisher = GrpcPublisher::new(&config).await.err().unwrap();
            assert!(publisher.to_string().contains(label), "{publisher:#}");

            let consumer = ClientModeConsumer::new(&config, &config.url).await.err();
            assert!(
                consumer.is_some_and(|error| error.to_string().contains(label)),
                "Bridge client must reject {label} rather than connect unauthenticated"
            );

            let mut server = config.clone();
            server.server_mode = true;
            server.url = "http://127.0.0.1:0".to_string();
            let error = ServerModeConsumer::new(&server, &server.url).await.err();
            assert!(error.is_some_and(|error| error.to_string().contains(label)));
        }
    }

    #[test]
    fn legacy_grpc_config_keys_still_deserialize() {
        let config: GrpcConfig = serde_json::from_value(serde_json::json!({
            "url": "http://127.0.0.1:50051",
            "timeout_ms": 250,
            "server_streaming": true
        }))
        .unwrap();

        assert_eq!(config.timeout_ms, Some(250));
        assert!(config.server_streaming);
    }

    #[tokio::test]
    async fn dynamic_construction_rejects_invalid_descriptors_names_and_json() {
        let (address, handle) = dynamic_fixture_server().await;

        let mut invalid_descriptor = dynamic_config(address, "Unary");
        invalid_descriptor.descriptor_set_bytes = Some(vec![0xff]);
        assert!(
            DynamicConsumer::new(&invalid_descriptor, &invalid_descriptor.url)
                .await
                .is_err()
        );

        let mut invalid_service = dynamic_config(address, "Unary");
        invalid_service.service_name = Some("missing.Service".into());
        let error = DynamicConsumer::new(&invalid_service, &invalid_service.url)
            .await
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("service 'missing.Service' not found"));

        let invalid_method = dynamic_config(address, "Missing");
        let error = DynamicConsumer::new(&invalid_method, &invalid_method.url)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("method"));
        assert!(error.to_string().contains("Missing"));

        let mut invalid_json = dynamic_config(address, "Unary");
        invalid_json.request = Some(serde_json::json!({"sequence": {"not": "an integer"}}));
        assert!(DynamicConsumer::new(&invalid_json, &invalid_json.url)
            .await
            .is_err());

        handle.abort();
    }

    #[tokio::test]
    async fn generated_python_client_interoperates_with_bridge_server_mode() {
        let python = std::process::Command::new("python3")
            .args(["-c", "import grpc, grpc_tools.protoc"])
            .status();
        if !python.is_ok_and(|status| status.success()) {
            eprintln!("skipping Python gRPC compatibility test: grpcio-tools is unavailable");
            return;
        }

        let generated = tempfile::tempdir().unwrap();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let generation = std::process::Command::new("python3")
            .current_dir(root)
            .args([
                "-m",
                "grpc_tools.protoc",
                "-I",
                "src/endpoints/proto",
                "--python_out",
                generated.path().to_str().unwrap(),
                "--grpc_python_out",
                generated.path().to_str().unwrap(),
                "src/endpoints/proto/mqbridge/bridge.proto",
            ])
            .status()
            .unwrap();
        assert!(generation.success(), "Python client generation failed");

        let mut consumer = GrpcConsumer::new(&GrpcConfig {
            url: free_server_url().await,
            topic: Some("compat".into()),
            server_mode: true,
            request_timeout_ms: Some(5_000),
            ..Default::default()
        })
        .await
        .unwrap();
        let address = consumer.bound_addr.unwrap();

        let mut child = std::process::Command::new("python3")
            .current_dir(root)
            .env("PYTHONPATH", generated.path())
            .arg("tests/compat/python/bridge_client.py")
            .arg(address.to_string())
            .spawn()
            .unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(5), consumer.receive_batch(1))
            .await
            .expect("Python client did not publish")
            .expect("Bridge server did not receive Python publish");
        assert_eq!(
            batch.messages[0].payload.as_ref(),
            b"python-generated-client"
        );
        (batch.commit)(vec![MessageDisposition::Ack])
            .await
            .expect("commit Python publish");

        let wait = tokio::task::spawn_blocking(move || child.wait());
        let status = tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("Python compatibility client timed out")
            .expect("Python compatibility wait task failed")
            .unwrap();
        assert!(status.success(), "Python compatibility client failed");
    }
}
