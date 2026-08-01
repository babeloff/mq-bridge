//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

#[cfg(feature = "amqp")]
pub mod amqp;
#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
pub mod file;
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "http")]
pub mod http;
#[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
pub mod ibm_mq;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "object-store")]
pub mod object_store;
#[cfg(any(feature = "sqlx", feature = "clickhouse"))]
mod poll;
#[cfg(feature = "postgres-cdc")]
pub mod postgres;
#[cfg(feature = "redis-streams")]
pub mod redis_streams;
#[cfg(feature = "sled")]
pub mod sled;
#[cfg(feature = "sqlx")]
pub mod sqlx;
/// Structural endpoints (`fanout`, `switch`, `request`, `response`, `reader`,
/// `static`, `stream_buffer`, `null`) that route or terminate a flow instead of
/// talking to an external system.
pub mod structural;
#[cfg(feature = "websocket")]
pub mod websocket;
#[cfg(any(feature = "zeromq", feature = "zeromq-omq"))]
pub mod zeromq;
use crate::endpoints::memory::{get_or_create_channel, MemoryChannel};
/// Backwards-compatible aliases for the structural endpoints, which used to live
/// directly under `endpoints`. Prefer `endpoints::structural::*`.
#[doc(hidden)]
pub use crate::endpoints::structural::{
    fanout, null, reader, request, response, static_endpoint, stream_buffer, switch,
};
use crate::middleware::apply_middlewares_to_consumer;
use crate::models::{
    Endpoint, EndpointType, MemoryConfig, Middleware, ResponseConfig, StreamBufferConfig,
};
use crate::route::get_endpoint_factory;
use crate::traits::{BoxFuture, MessageConsumer, MessagePublisher};
use anyhow::{anyhow, Result};
use std::sync::Arc;

impl Endpoint {
    pub fn new(endpoint_type: EndpointType) -> Self {
        Self {
            middlewares: Vec::new(),
            endpoint_type,
            handler: None,
        }
    }
    /// Creates a new in-memory endpoint with the specified topic and capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use mq_bridge::models::Endpoint;
    /// let endpoint = Endpoint::new_memory("my_topic", 100);
    /// ```
    pub fn new_memory(topic: &str, capacity: usize) -> Self {
        Self::new(EndpointType::Memory(MemoryConfig::new(
            topic,
            Some(capacity),
        )))
    }
    pub fn new_response() -> Self {
        Self::new(EndpointType::Response(ResponseConfig::default()))
    }
    pub fn new_stream_buffer(topic: &str, correlation_id: Option<&str>, capacity: usize) -> Self {
        Self::new(EndpointType::StreamBuffer(StreamBufferConfig {
            topic: topic.to_string(),
            correlation_id: correlation_id.map(str::to_string),
            capacity: Some(capacity),
        }))
    }
    pub fn has_retry_middleware(&self) -> bool {
        self.middlewares
            .iter()
            .any(|m| matches!(m, Middleware::Retry(_)))
    }
    pub fn add_middleware(mut self, middleware: Middleware) -> Self {
        self.middlewares.push(middleware);
        self
    }
    pub fn add_middlewares(mut self, mut middlewares: Vec<Middleware>) -> Self {
        self.middlewares.append(&mut middlewares);
        self
    }
    ///
    /// Returns a reference to the in-memory channel associated with this Endpoint.
    /// This function will only succeed if the Endpoint is of type EndpointType::Memory.
    /// If the Endpoint is not a memory endpoint, this function will return an error.
    /// This function is primarily used for testing purposes where a Queue is needed.
    pub fn channel(&self) -> anyhow::Result<MemoryChannel> {
        match &self.endpoint_type {
            EndpointType::Memory(cfg) => Ok(get_or_create_channel(cfg)),
            _ => Err(anyhow::anyhow!("channel() called on non-memory Endpoint")),
        }
    }
    pub fn null() -> Self {
        Self::new(EndpointType::Null)
    }

    pub fn with_retry(mut self, retry: crate::models::RetryMiddleware) -> Self {
        // Retry should be inner to DLQ and Metrics.
        // We insert it before any existing DLQ or Metrics middleware.
        let mut insert_idx = self.middlewares.len();
        for (i, m) in self.middlewares.iter().enumerate() {
            if matches!(m, Middleware::Dlq(_) | Middleware::Metrics(_)) {
                insert_idx = i;
                break;
            }
        }
        self.middlewares
            .insert(insert_idx, Middleware::Retry(retry));
        self
    }

    pub fn with_dlq(mut self, dlq: crate::models::DeadLetterQueueMiddleware) -> Self {
        // DLQ should be outer to Retry, but inner to Metrics.
        let mut insert_idx = self.middlewares.len();
        for (i, m) in self.middlewares.iter().enumerate() {
            if matches!(m, Middleware::Metrics(_)) {
                insert_idx = i;
                break;
            }
        }
        self.middlewares
            .insert(insert_idx, Middleware::Dlq(Box::new(dlq)));
        self
    }

    pub fn with_deduplication(mut self, dedup: crate::models::DeduplicationMiddleware) -> Self {
        // Deduplication is consumer-only.
        // We insert it at the beginning so it is applied last (innermost) for consumers,
        // or second to last if metrics are at 0.
        // List: [Dedup, ...] -> Consumer: ... ( Dedup ( base ) )
        self.middlewares.insert(0, Middleware::Deduplication(dedup));
        self
    }

    pub fn with_consumer_metrics(mut self) -> Self {
        // For consumers, the first middleware in the list is the outermost (applied last).
        // Inserting at 0 ensures it wraps everything else (Ingestion Metrics).
        // List: [Metrics, Dedup] -> Consumer: Metrics ( Dedup ( base ) )
        if !self
            .middlewares
            .iter()
            .any(|m| matches!(m, Middleware::Metrics(_)))
        {
            self.middlewares
                .insert(0, Middleware::Metrics(crate::models::MetricsMiddleware {}));
        }
        self
    }

    pub fn with_metrics(mut self) -> Self {
        // Metrics should be outer to everything (last in the list for publishers).
        if !self
            .middlewares
            .iter()
            .any(|m| matches!(m, Middleware::Metrics(_)))
        {
            self.middlewares
                .push(Middleware::Metrics(crate::models::MetricsMiddleware {}));
        }
        self
    }

    pub async fn create_consumer(
        &self,
        route_name: &str,
    ) -> anyhow::Result<Box<dyn crate::traits::MessageConsumer>> {
        crate::endpoints::create_consumer_from_route(route_name, self).await
    }

    pub async fn create_publisher(&self, _route_name: &str) -> anyhow::Result<crate::Publisher> {
        crate::Publisher::new(self.clone()).await
    }

    pub fn check_consumer(
        &self,
        route_name: &str,
        allowed_endpoints: Option<&[&str]>,
    ) -> anyhow::Result<Vec<String>> {
        crate::endpoints::check_consumer(route_name, self, allowed_endpoints)
    }

    pub fn check_publisher(
        &self,
        route_name: &str,
        allowed_endpoints: Option<&[&str]>,
    ) -> anyhow::Result<Vec<String>> {
        crate::endpoints::check_publisher(route_name, self, allowed_endpoints)
    }
}

/// Validates the consumer configuration for a route.
pub fn check_consumer(
    route_name: &str,
    endpoint: &Endpoint,
    allowed_types: Option<&[&str]>,
) -> Result<Vec<String>> {
    check_consumer_recursive(route_name, endpoint, 0, allowed_types)
}

fn check_consumer_recursive(
    route_name: &str,
    endpoint: &Endpoint,
    depth: usize,
    allowed_types: Option<&[&str]>,
) -> Result<Vec<String>> {
    const MAX_DEPTH: usize = 16;
    if depth > MAX_DEPTH {
        return Err(anyhow!(
            "Ref recursion depth exceeded limit of {}",
            MAX_DEPTH
        ));
    }
    let mut warnings = Vec::new();
    if endpoint.handler.is_some() {
        warnings.push(
            "Endpoint 'handler' is set on an input endpoint. Handlers are currently only supported on output endpoints (publishers) and will be ignored here."
            .to_string()
        );
    }

    if let Some(allowed) = allowed_types {
        if !endpoint.endpoint_type.is_core() {
            let name = endpoint.endpoint_type.name();
            if !allowed.contains(&name) {
                return Err(anyhow!(
                    "[route:{}] Endpoint type '{}' is not allowed by policy",
                    route_name,
                    name
                ));
            }
        }
    }
    match &endpoint.endpoint_type {
        EndpointType::Ref(name) => {
            let referenced = crate::route::get_endpoint(name).ok_or_else(|| {
                anyhow!(
                    "[route:{}] Referenced endpoint '{}' not found",
                    route_name,
                    name
                )
            })?;
            // We need to check the referenced endpoint, but we don't need to merge middlewares
            // for the check itself, as we just want to validate the core type.
            // However, to be thorough, we recurse on the referenced endpoint.
            // Note: This check ignores the middlewares on the 'ref' itself, which is acceptable for type checking.
            warnings.extend(check_consumer_recursive(
                route_name,
                &referenced,
                depth + 1,
                allowed_types,
            )?);
            Ok(warnings)
        }
        #[cfg(feature = "aws")]
        EndpointType::Aws(cfg) => {
            if cfg.topic_arn.is_some() {
                warnings.push(
                    "Endpoint 'aws' is used as a consumer, but 'topic_arn' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "kafka")]
        EndpointType::Kafka(cfg) => {
            if cfg.delayed_ack {
                warnings.push(
                    "Endpoint 'kafka' is used as a consumer, but 'delayed_ack' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.producer_options.is_some() {
                warnings.push(
                    "Endpoint 'kafka' is used as a consumer, but 'producer_options' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "nats")]
        EndpointType::Nats(cfg) => {
            if cfg.stream.is_none() {
                return Err(anyhow!(
                    "[route:{}] NATS consumer must specify a 'stream'",
                    route_name
                ));
            }
            if cfg.request_reply {
                warnings.push(
                    "Endpoint 'nats' is used as a consumer, but 'request_reply' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.request_timeout_ms.is_some() {
                warnings.push(
                    "Endpoint 'nats' is used as a consumer, but 'request_timeout_ms' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.delayed_ack {
                warnings.push(
                    "Endpoint 'nats' is used as a consumer, but 'delayed_ack' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.stream_max_messages.is_some() {
                warnings.push(
                    "Endpoint 'nats' is used as a consumer, but 'stream_max_messages' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.stream_max_bytes.is_some() {
                warnings.push(
                    "Endpoint 'nats' is used as a consumer, but 'stream_max_bytes' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "amqp")]
        EndpointType::Amqp(cfg) => {
            if cfg.delayed_ack {
                warnings.push(
                    "Endpoint 'amqp' is used as a consumer, but 'delayed_ack' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "mqtt")]
        EndpointType::Mqtt(cfg) => {
            if cfg.delayed_ack {
                warnings.push(
                    "Endpoint 'mqtt' is used as a consumer, but 'delayed_ack' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(any(feature = "zeromq", feature = "zeromq-omq"))]
        EndpointType::ZeroMq(_) => Ok(warnings),
        #[cfg(feature = "redis-streams")]
        EndpointType::RedisStreams(cfg) => {
            if cfg.maxlen.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a consumer, but 'maxlen' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.approx_trim.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a consumer, but 'approx_trim' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
        EndpointType::IbmMq(_) => Ok(warnings),
        #[cfg(feature = "mongodb")]
        EndpointType::MongoDb(cfg) => {
            use crate::models::MongoConsume;
            let mode = cfg.resolved_consume();
            if mode == MongoConsume::Subscriber
                && matches!(cfg.format, crate::models::MongoDbFormat::Raw)
            {
                return Err(anyhow!(
                    "[route:{}] MongoDB raw format cannot be used with subscriber mode because raw documents do not include the seq ordering field",
                    route_name
                ));
            }
            if cfg.consume.is_some() && cfg.change_stream {
                warnings.push(
                    "Endpoint 'mongodb' sets 'consume'; the deprecated 'change_stream' boolean is ignored and should be removed."
                    .to_string(),
                );
            } else if cfg.change_stream {
                warnings.push(
                    "Endpoint 'mongodb' option 'change_stream' is deprecated; use 'consume: subscriber'."
                    .to_string(),
                );
            }
            if cfg.reply_polling_ms.is_some() {
                warnings.push(
                    "Endpoint 'mongodb' is used as a consumer, but 'reply_polling_ms' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.request_reply {
                warnings.push(
                    "Endpoint 'mongodb' is used as a consumer, but 'request_reply' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.request_timeout_ms.is_some() {
                warnings.push(
                    "Endpoint 'mongodb' is used as a consumer, but 'request_timeout_ms' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.ttl_seconds.is_some() {
                warnings.push(
                    "Endpoint 'mongodb' is used as a consumer, but 'ttl_seconds' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.capped_size_bytes.is_some() {
                warnings.push(
                    "Endpoint 'mongodb' is used as a consumer, but 'capped_size_bytes' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "grpc")]
        EndpointType::Grpc(_) => Ok(warnings),
        #[cfg(feature = "http")]
        EndpointType::Http(cfg) => {
            if cfg.batch_concurrency.is_some() {
                warnings.push("Endpoint 'http' is used as a consumer, but 'batch_concurrency' is a publisher-only option and will be ignored.".to_string());
            }
            if cfg.tcp_keepalive_ms.is_some() {
                warnings.push("Endpoint 'http' is used as a consumer, but 'tcp_keepalive_ms' is a publisher-only option and will be ignored.".to_string());
            }
            if cfg.pool_idle_timeout_ms.is_some() {
                warnings.push(
                        "Endpoint 'http' is used as a consumer, but 'pool_idle_timeout_ms' is a publisher-only option and will be ignored."
                        .to_string(),
                    );
            }
            if cfg.stream_response_to.is_some() {
                warnings.push("Endpoint 'http' is used as a consumer, but 'stream_response_to' is a publisher-only option and will be ignored.".to_string());
            }
            Ok(warnings)
        }
        #[cfg(feature = "clickhouse")]
        EndpointType::ClickHouse(cfg) => {
            if cfg.cursor_column.is_none() {
                return Err(anyhow!(
                    "ClickHouse endpoint used as a consumer requires 'cursor_column' (ClickHouse has no native queue; only non-destructive cursor reads are supported)."
                ));
            }
            if cfg.columns.is_some() {
                warnings.push("Endpoint 'clickhouse' is used as a consumer, but 'columns' is a publisher-only option and will be ignored.".to_string());
            }
            if cfg.async_insert {
                warnings.push("Endpoint 'clickhouse' is used as a consumer, but 'async_insert' is a publisher-only option and will be ignored.".to_string());
            }
            Ok(warnings)
        }
        #[cfg(feature = "sqlx")]
        EndpointType::Sqlx(cfg) => {
            if cfg.insert_query.is_some() {
                warnings.push(
                    "Endpoint 'sqlx' is used as a consumer, but 'insert_query' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "postgres-cdc")]
        EndpointType::PostgresCdc(cfg) => {
            if cfg.publication.trim().is_empty() {
                return Err(anyhow!(
                    "postgres_cdc consumer requires a 'publication' (defines which tables are captured)."
                ));
            }
            Ok(warnings)
        }
        #[cfg(feature = "sled")]
        EndpointType::Sled(_) => Ok(warnings),
        EndpointType::Static(_) => Ok(warnings),
        EndpointType::Memory(cfg) => {
            if cfg.request_reply {
                warnings.push(
                    "Endpoint 'memory' is used as a consumer, but 'request_reply' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.request_timeout_ms.is_some() {
                warnings.push(
                    "Endpoint 'memory' is used as a consumer, but 'request_timeout_ms' is a publisher-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        EndpointType::StreamBuffer(cfg) => {
            if cfg.correlation_id.is_none() {
                return Err(anyhow!(
                    "[route:{}] stream_buffer consumer must specify 'correlation_id'",
                    route_name
                ));
            }
            Ok(warnings)
        }
        EndpointType::File(_) => Ok(warnings),
        #[cfg(feature = "object-store")]
        EndpointType::ObjectStore(cfg) => {
            if cfg.extension.is_some() {
                warnings.push("Endpoint 'object_store' is used as a consumer, but 'extension' is a publisher-only option and will be ignored.".to_string());
            }
            if cfg.date_partition {
                warnings.push("Endpoint 'object_store' is used as a consumer, but 'date_partition' is a publisher-only option and will be ignored.".to_string());
            }
            Ok(warnings)
        }
        #[cfg(feature = "websocket")]
        EndpointType::WebSocket(_) => Ok(warnings),
        EndpointType::Custom { .. } => Ok(warnings),
        EndpointType::Switch(_) => Err(anyhow!(
            "[route:{}] Switch endpoint is only supported as an output",
            route_name
        )),
        EndpointType::Reader(_) => Err(anyhow!(
            "[route:{}] Reader endpoint is only supported as an output",
            route_name
        )),
        #[allow(unreachable_patterns)]
        _ => {
            if let Some(allowed) = allowed_types {
                let name = endpoint.endpoint_type.name();
                if allowed.contains(&name) {
                    return Ok(warnings);
                }
            }
            Err(anyhow!(
                "[route:{}] Unsupported consumer endpoint type '{:?}'",
                route_name,
                endpoint.endpoint_type
            ))
        }
    }
}

fn resolve_endpoint(endpoint: &Endpoint, route_name: &str) -> Result<Endpoint> {
    let mut visited = std::collections::HashSet::new();
    resolve_endpoint_recursive(endpoint, route_name, &mut visited)
}

fn resolve_endpoint_recursive(
    endpoint: &Endpoint,
    route_name: &str,
    visited: &mut std::collections::HashSet<String>,
) -> Result<Endpoint> {
    const MAX_DEPTH: usize = 16;
    if visited.len() > MAX_DEPTH {
        return Err(anyhow!(
            "Reference recursion depth exceeded limit of {}",
            MAX_DEPTH
        ));
    }

    if let EndpointType::Ref(name) = &endpoint.endpoint_type {
        if !visited.insert(name.clone()) {
            return Err(anyhow!(
                "[route:{}] Circular reference detected for endpoint '{}'",
                route_name,
                name
            ));
        }

        let referenced_endpoint = crate::route::get_endpoint(name).ok_or_else(|| {
            anyhow!(
                "[route:{}] Referenced endpoint '{}' not found",
                route_name,
                name
            )
        })?;

        let mut resolved = resolve_endpoint_recursive(&referenced_endpoint, route_name, visited)?;
        // Merge middlewares: The ref's middlewares should be outer (applied last in the rev() loop).
        // Since apply_middlewares_to_consumer iterates in reverse, we prepend the ref's middlewares.
        let mut new_middlewares = endpoint.middlewares.clone();
        new_middlewares.extend(resolved.middlewares);
        resolved.middlewares = new_middlewares;
        Ok(resolved)
    } else {
        Ok(endpoint.clone())
    }
}

/// Map a `sqlx` consumer config that requested CDC (via `publication`) onto a
/// `PostgresCdcConfig`, so a Postgres `sqlx` endpoint can transparently fall back
/// to logical-replication CDC. Postgres-only; other drivers error.
#[cfg(all(feature = "sqlx", feature = "postgres-cdc"))]
fn sqlx_cfg_to_cdc(
    cfg: &crate::models::SqlxConfig,
) -> anyhow::Result<crate::models::PostgresCdcConfig> {
    let url = cfg.url.trim();
    if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
        return Err(anyhow!(
            "sqlx `publication` (CDC) is only supported for PostgreSQL URLs; got '{}'. \
             Use a postgres:// URL or the dedicated `postgres_cdc` endpoint.",
            cfg.url
        ));
    }
    if cfg.cursor_column.is_some() {
        return Err(anyhow!(
            "sqlx endpoint sets both `publication` (CDC) and `cursor_column` (polling); pick one."
        ));
    }
    Ok(crate::models::PostgresCdcConfig {
        url: cfg.url.clone(),
        publication: cfg.publication.clone().unwrap_or_default(),
        slot_name: cfg
            .slot_name
            .clone()
            .unwrap_or_else(|| "mq_bridge_slot".to_string()),
        create_slot: true,
        create_publication: cfg.create_publication,
        publication_tables: if cfg.create_publication {
            vec![cfg.table.clone()]
        } else {
            Vec::new()
        },
        temporary_slot: false,
        cursor_id: cfg.cursor_id.clone(),
        checkpoint_store: cfg.checkpoint_store.clone(),
        status_interval_ms: 10_000,
        tls: cfg.tls.clone(),
    })
}

/// Creates a `MessageConsumer` based on the route's "in" configuration.
pub async fn create_consumer_from_route(
    route_name: &str,
    endpoint: &Endpoint,
) -> Result<Box<dyn MessageConsumer>> {
    let resolved_endpoint = resolve_endpoint(endpoint, route_name)?;
    check_consumer(route_name, &resolved_endpoint, None)?;
    let consumer = create_base_consumer(route_name, &resolved_endpoint).await?;
    apply_middlewares_to_consumer(consumer, &resolved_endpoint, route_name).await
}

pub(crate) async fn try_run_fast_path_route(
    route: &crate::models::Route,
    name: &str,
    shutdown_rx: async_channel::Receiver<()>,
    ready_tx: Option<async_channel::Sender<()>>,
) -> Option<anyhow::Result<bool>> {
    #[cfg(feature = "http")]
    {
        // The inline fast path applies to outputs that reply synchronously
        // without the route worker/disposition pipeline: `response` (handler- or
        // request-derived reply) and `static` (a fixed, pre-rendered reply).
        let output_is_inline = matches!(
            route.output.endpoint_type,
            EndpointType::Response(_) | EndpointType::Static(_)
        );
        if let EndpointType::Http(cfg) = &route.input.endpoint_type {
            if output_is_inline
                && cfg.inline_response_fast_path_enabled()
                && route.input.middlewares.is_empty()
                && output_middlewares_allow_http_inline_fast_path(&route.output.middlewares)
                && !cfg.fire_and_forget
            {
                return Some(
                    run_http_inline_response_fast_path(
                        route,
                        name,
                        shutdown_rx,
                        ready_tx,
                        cfg.clone(),
                    )
                    .await,
                );
            }
        }
    }

    #[cfg(feature = "websocket")]
    {
        if let EndpointType::WebSocket(cfg) = &route.input.endpoint_type {
            match websocket_direct_route_support(route) {
                WebSocketDirectRouteSupport::Supported => {
                    return Some(
                        websocket::run_direct_response_route(
                            name,
                            cfg.clone(),
                            route.output.handler.clone(),
                            shutdown_rx,
                            ready_tx,
                        )
                        .await,
                    );
                }
                WebSocketDirectRouteSupport::Unsupported(reason) => match cfg.execution_mode {
                    crate::models::WebSocketExecutionMode::Auto => {
                        tracing::warn!(
                            route = name,
                            reason = reason,
                            "WebSocket route cannot run in direct mode; falling back to routed mode"
                        );
                    }
                    crate::models::WebSocketExecutionMode::DirectOnly => {
                        return Some(Err(anyhow!(
                            "WebSocket route '{}' is configured for direct_only, but direct mode is unsupported: {}",
                            name,
                            reason
                        )));
                    }
                    crate::models::WebSocketExecutionMode::Routed => {}
                },
            }
        }
    }

    let _ = route;
    let _ = name;
    let _ = shutdown_rx;
    let _ = ready_tx;
    None
}

#[cfg(feature = "websocket")]
enum WebSocketDirectRouteSupport {
    Supported,
    Unsupported(&'static str),
}

#[cfg(feature = "websocket")]
fn websocket_direct_route_support(route: &crate::models::Route) -> WebSocketDirectRouteSupport {
    let EndpointType::WebSocket(cfg) = &route.input.endpoint_type else {
        return WebSocketDirectRouteSupport::Unsupported("input is not websocket");
    };

    if cfg.execution_mode == crate::models::WebSocketExecutionMode::Routed {
        return WebSocketDirectRouteSupport::Unsupported("execution_mode is routed");
    }
    if !matches!(route.output.endpoint_type, EndpointType::Response(_)) {
        return WebSocketDirectRouteSupport::Unsupported("output is not response");
    }
    if !websocket_direct_route_options_allowed(&route.options) {
        return WebSocketDirectRouteSupport::Unsupported(
            "custom route options require routed mode",
        );
    }
    if !route.input.middlewares.is_empty() || !route.output.middlewares.is_empty() {
        return WebSocketDirectRouteSupport::Unsupported("middleware requires routed mode");
    }

    WebSocketDirectRouteSupport::Supported
}

#[cfg(feature = "websocket")]
fn websocket_direct_route_options_allowed(options: &crate::models::RouteOptions) -> bool {
    let mut defaults = crate::models::RouteOptions::default();
    defaults.description.clone_from(&options.description);
    options == &defaults
}

#[cfg(feature = "http")]
fn output_middlewares_allow_http_inline_fast_path(middlewares: &[Middleware]) -> bool {
    middlewares.iter().all(|middleware| {
        matches!(
            middleware,
            Middleware::Buffer(_)
                | Middleware::Delay(_)
                | Middleware::Limiter(_)
                | Middleware::Metrics(_)
        )
    })
}

#[cfg(feature = "http")]
async fn run_http_inline_response_fast_path(
    route: &crate::models::Route,
    name: &str,
    shutdown_rx: async_channel::Receiver<()>,
    ready_tx: Option<async_channel::Sender<()>>,
    http_config: crate::models::HttpConfig,
) -> anyhow::Result<bool> {
    let publisher = create_publisher_from_route(name, &route.output).await?;
    let consumer =
        http::HttpConsumer::new_with_inline_publisher(&http_config, Some(publisher.clone()))
            .await?;

    if let Err(err) = crate::route::run_publisher_connect_hook(name, &publisher).await {
        crate::route::run_publisher_disconnect_hook(name, &publisher).await;
        return Err(err);
    }
    if let Err(err) = crate::route::run_consumer_connect_hook(name, &consumer).await {
        crate::route::run_consumer_disconnect_hook(name, &consumer).await;
        crate::route::run_publisher_disconnect_hook(name, &publisher).await;
        return Err(err);
    }

    tracing::info!(
        route = name,
        has_output_handler = route.output.handler.is_some(),
        output_middlewares = route.output.middlewares.len(),
        "Running HTTP inline response fast path; bypassing the normal route consumer/worker/disposition pipeline while keeping the output publisher chain active"
    );
    tracing::debug!(
        route = name,
        "HTTP inline response fast path differences: no input middlewares, no fire-and-forget, only buffer/metrics output middlewares allowed, and unchanged request metadata is not echoed back as response headers"
    );
    if let Some(tx) = ready_tx {
        let _ = tx.send(()).await;
    }

    let stopped = shutdown_rx.recv().await.is_ok();
    if stopped {
        tracing::info!(
            "Shutdown signal received in HTTP inline response runner for route '{}'.",
            name
        );
    }
    crate::route::run_consumer_disconnect_hook(name, &consumer).await;
    crate::route::run_publisher_disconnect_hook(name, &publisher).await;
    Ok(true)
}

async fn create_base_consumer(
    route_name: &str,
    endpoint: &Endpoint,
) -> Result<Box<dyn MessageConsumer>> {
    // Helper to coerce concrete consumers to the trait object, fixing type inference issues in the match block.
    fn boxed<T: MessageConsumer + 'static>(c: T) -> Box<dyn MessageConsumer> {
        Box::new(c)
    }

    match &endpoint.endpoint_type {
        #[cfg(feature = "aws")]
        EndpointType::Aws(cfg) => Ok(boxed(aws::AwsConsumer::new(cfg).await?)),
        #[cfg(feature = "kafka")]
        EndpointType::Kafka(cfg) => {
            let mut config = cfg.clone();
            if config.topic.is_none() {
                config.topic = Some(route_name.to_string());
            }
            Ok(boxed(kafka::KafkaConsumer::new(&config).await?))
        }
        #[cfg(feature = "nats")]
        EndpointType::Nats(cfg) => {
            let mut config = cfg.clone();
            if config.subject.is_none() {
                config.subject = Some(route_name.to_string());
            }
            Ok(boxed(nats::NatsConsumer::new(&config).await?))
        }
        #[cfg(feature = "amqp")]
        EndpointType::Amqp(cfg) => {
            let mut config = cfg.clone();
            if config.queue.is_none() {
                config.queue = Some(route_name.to_string());
            }
            Ok(boxed(amqp::AmqpConsumer::new(&config).await?))
        }
        #[cfg(feature = "mqtt")]
        EndpointType::Mqtt(cfg) => {
            let mut config = cfg.clone();
            if config.topic.is_none() {
                config.topic = Some(route_name.to_string());
            }
            if config.client_id.is_none() && !config.clean_session {
                // For persistent sessions, default client_id to route_name if not provided
                config.client_id = Some(format!("{}-{}", crate::APP_NAME, route_name));
            }
            Ok(boxed(mqtt::MqttConsumer::new(&config).await?))
        }
        #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
        EndpointType::IbmMq(cfg) => {
            let mut config = cfg.clone();
            if config.queue.is_none() && config.topic.is_none() {
                config.queue = Some(route_name.to_string());
            }
            Ok(boxed(ibm_mq::IbmMqConsumer::new(&config).await?))
        }
        #[cfg(any(feature = "zeromq", feature = "zeromq-omq"))]
        EndpointType::ZeroMq(cfg) => zeromq::create_consumer(cfg).await,
        #[cfg(feature = "redis-streams")]
        EndpointType::RedisStreams(cfg) => {
            let mut config = cfg.clone();
            if config.stream.is_none() {
                config.stream = Some(route_name.to_string());
            }
            Ok(boxed(
                redis_streams::RedisStreamsConsumer::new(&config).await?,
            ))
        }
        EndpointType::File(cfg) => Ok(boxed(file::FileConsumer::new(cfg).await?)),
        #[cfg(feature = "object-store")]
        EndpointType::ObjectStore(cfg) => {
            Ok(boxed(object_store::ObjectStoreConsumer::new(cfg).await?))
        }
        #[cfg(feature = "grpc")]
        EndpointType::Grpc(cfg) => {
            let mut config = cfg.clone();
            if config.topic.is_none() {
                config.topic = Some(route_name.to_string());
            }
            Ok(boxed(grpc::GrpcConsumer::new(&config).await?))
        }
        #[cfg(feature = "sqlx")]
        EndpointType::Sqlx(cfg) => {
            if cfg.publication.is_some() {
                // A Postgres publication means CDC (logical replication), not polling —
                // delegate to the postgres_cdc consumer. See `sqlx_cfg_to_cdc`.
                #[cfg(feature = "postgres-cdc")]
                {
                    Ok(boxed(
                        postgres::PostgresCdcConsumer::new(&sqlx_cfg_to_cdc(cfg)?).await?,
                    ))
                }
                #[cfg(not(feature = "postgres-cdc"))]
                {
                    Err(anyhow!(
                        "sqlx endpoint with `publication` set uses Postgres CDC, which requires \
                         the `postgres-cdc` feature to be enabled."
                    ))
                }
            } else if cfg.cursor_column.is_some() {
                // Non-destructive, resumable cursor read of an arbitrary table.
                Ok(boxed(sqlx::SqlxCursorReader::new(cfg).await?))
            } else {
                Ok(boxed(sqlx::SqlxConsumer::new(cfg).await?))
            }
        }
        #[cfg(feature = "clickhouse")]
        EndpointType::ClickHouse(cfg) => {
            if cfg.cursor_column.is_some() {
                // ClickHouse has no native queue; only non-destructive cursor reads are supported.
                Ok(boxed(clickhouse::ClickHouseCursorReader::new(cfg).await?))
            } else {
                Err(anyhow::anyhow!(
                    "ClickHouse endpoint used as a consumer requires 'cursor_column' (ClickHouse has no native queue; only non-destructive cursor reads are supported)."
                ))
            }
        }
        #[cfg(feature = "postgres-cdc")]
        EndpointType::PostgresCdc(cfg) => Ok(boxed(postgres::PostgresCdcConsumer::new(cfg).await?)),
        #[cfg(feature = "http")]
        EndpointType::Http(cfg) => Ok(boxed(http::HttpConsumer::new(cfg).await?)),
        #[cfg(feature = "websocket")]
        EndpointType::WebSocket(cfg) => Ok(boxed(websocket::WebSocketConsumer::new(cfg).await?)),
        EndpointType::Static(cfg) => Ok(boxed(static_endpoint::StaticRequestConsumer::new(cfg)?)),
        EndpointType::Memory(cfg) => Ok(boxed(memory::MemoryConsumer::new_async(cfg).await?)),
        EndpointType::StreamBuffer(cfg) => {
            Ok(boxed(stream_buffer::StreamBufferConsumer::new(cfg)?))
        }
        #[cfg(feature = "sled")]
        EndpointType::Sled(cfg) => Ok(boxed(sled::SledConsumer::new(cfg)?)),
        #[cfg(feature = "mongodb")]
        EndpointType::MongoDb(cfg) => {
            use crate::models::MongoConsume;
            let mut config = cfg.clone();
            if config.collection.is_none() {
                config.collection = Some(route_name.to_string());
            }
            match config.resolved_consume() {
                MongoConsume::Consumer => {
                    // Durable queue drain (auto-uses a change stream when available, else polls).
                    Ok(boxed(mongodb::MongoDbConsumer::new(&config).await?))
                }
                MongoConsume::Subscriber => {
                    if config.ttl_seconds.is_none() {
                        config.ttl_seconds = Some(86400); // Remove events by default after 24 hours
                    }
                    Ok(boxed(mongodb::MongoDbSubscriber::new(&config).await?))
                }
                MongoConsume::CaptureNew => {
                    // Watch an existing collection for changes from now on (needs a replica set;
                    // otherwise the change-stream open returns a clear error).
                    Ok(boxed(
                        mongodb::MongoDbChangeStreamReader::new(&config, false).await?,
                    ))
                }
                MongoConsume::CaptureAll => {
                    // Read existing documents first, then capture changes. Prefer a change stream
                    // (captures updates/deletes); on a standalone server change streams are
                    // unavailable, so fall back to a non-destructive `_id` read (inserts only).
                    match mongodb::MongoDbChangeStreamReader::new(&config, true).await {
                        Ok(reader) => Ok(boxed(reader)),
                        // Only fall back for the "change streams need a replica set" error (40573);
                        // auth/network/config and other startup errors propagate untouched.
                        Err(e) if mongodb::is_change_stream_unsupported(&e) => {
                            tracing::warn!(error = %e, "MongoDB change streams unavailable (needs a replica set); 'capture_all' falling back to an insert-only read");
                            Ok(boxed(mongodb::MongoDbIdReader::new(&config).await?))
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        }
        EndpointType::Custom { name, config } => {
            let factory = get_endpoint_factory(name)
                .ok_or_else(|| anyhow!("Custom endpoint factory '{}' not found", name))?;
            factory.create_consumer(route_name, config).await
        }
        EndpointType::Switch(_) => Err(anyhow!(
            "[route:{}] Switch endpoint is only supported as an output",
            route_name
        )),
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!(
            "[route:{}] Unsupported consumer endpoint type '{:?}'",
            route_name,
            endpoint.endpoint_type
        )),
    }
}

/// Validates the publisher configuration for a route.
pub fn check_publisher(
    route_name: &str,
    endpoint: &Endpoint,
    allowed_types: Option<&[&str]>,
) -> Result<Vec<String>> {
    check_publisher_recursive(route_name, endpoint, 0, allowed_types)
}

fn check_publisher_recursive(
    route_name: &str,
    endpoint: &Endpoint,
    depth: usize,
    allowed_types: Option<&[&str]>,
) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    if let Some(allowed) = allowed_types {
        if !endpoint.endpoint_type.is_core() {
            let name = endpoint.endpoint_type.name();
            if !allowed.contains(&name) {
                return Err(anyhow!(
                    "[route:{}] Endpoint type '{}' is not allowed by policy",
                    route_name,
                    name
                ));
            }
        }
    }
    const MAX_DEPTH: usize = 16;
    if depth > MAX_DEPTH {
        return Err(anyhow!(
            "Fanout recursion depth exceeded limit of {}",
            MAX_DEPTH
        ));
    }
    match &endpoint.endpoint_type {
        EndpointType::Ref(name) => {
            let referenced = crate::route::get_endpoint(name).ok_or_else(|| {
                anyhow!(
                    "[route:{}] Referenced endpoint '{}' not found in endpoint registry",
                    route_name,
                    name
                )
            });
            if let Ok(referenced) = referenced {
                warnings.extend(check_publisher_recursive(
                    route_name,
                    &referenced,
                    depth + 1,
                    allowed_types,
                )?);
                return Ok(warnings);
            }
            if crate::publisher::get_publisher(name).is_some() {
                return Ok(warnings);
            }
            Err(anyhow!(
                "[route:{}] Referenced endpoint '{}' not found in any registry",
                route_name,
                name
            ))
        }
        #[cfg(feature = "aws")]
        EndpointType::Aws(cfg) => {
            if cfg.max_messages.is_some() {
                warnings.push(
                    "Endpoint 'aws' is used as a publisher, but 'max_messages' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.wait_time_seconds.is_some() {
                warnings.push(
                    "Endpoint 'aws' is used as a publisher, but 'wait_time_seconds' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "kafka")]
        EndpointType::Kafka(cfg) => {
            if cfg.group_id.is_some() {
                warnings.push(
                    "Endpoint 'kafka' is used as a publisher, but 'group_id' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.consumer_options.is_some() {
                warnings.push(
                    "Endpoint 'kafka' is used as a publisher, but 'consumer_options' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "nats")]
        EndpointType::Nats(cfg) => {
            if cfg.stream.is_some() {
                warnings.push(
                    "Endpoint 'nats' is used as a publisher, but 'stream' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.subscriber_mode {
                warnings.push(
                    "Endpoint 'nats' is used as a publisher, but 'subscriber_mode' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.prefetch_count.is_some() {
                warnings.push(
                    "Endpoint 'nats' is used as a publisher, but 'prefetch_count' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "amqp")]
        EndpointType::Amqp(cfg) => {
            if cfg.subscribe_mode {
                warnings.push(
                    "Endpoint 'amqp' is used as a publisher, but 'subscribe_mode' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.prefetch_count.is_some() {
                warnings.push(
                    "Endpoint 'amqp' is used as a publisher, but 'prefetch_count' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "mqtt")]
        EndpointType::Mqtt(cfg) => {
            if cfg.clean_session {
                warnings.push(
                    "Endpoint 'mqtt' is used as a publisher, but 'clean_session' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(any(feature = "zeromq", feature = "zeromq-omq"))]
        EndpointType::ZeroMq(cfg) => {
            if cfg.topic.is_some() {
                warnings.push(
                    "Endpoint 'zeromq' is used as a publisher, but 'topic' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }

        #[cfg(feature = "http")]
        EndpointType::Http(_cfg) => {
            if _cfg.path.is_some() {
                warnings.push(
                    "Endpoint 'http' is used as a publisher, but 'path' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if _cfg.workers.is_some() {
                warnings.push(
                    "Endpoint 'http' is used as a publisher, but 'workers' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if _cfg.message_id_header.is_some() {
                warnings.push(
                    "Endpoint 'http' is used as a publisher, but 'message_id_header' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if _cfg.internal_buffer_size.is_some() {
                warnings.push(
                    "Endpoint 'http' is used as a publisher, but 'internal_buffer_size' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if _cfg.fire_and_forget {
                warnings.push(
                    "Endpoint 'http' is used as a publisher, but 'fire_and_forget' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if _cfg.receive_streamable {
                warnings.push(
                    "Endpoint 'http' is used as a publisher, but 'receive_streamable' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "redis-streams")]
        EndpointType::RedisStreams(cfg) => {
            if cfg.group.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'group' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.consumer_name.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'consumer_name' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.subscriber_mode {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'subscriber_mode' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.block_ms.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'block_ms' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.read_from_start {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'read_from_start' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.redelivery_timeout_ms.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'redelivery_timeout_ms' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.internal_buffer_size.is_some() {
                warnings.push(
                    "Endpoint 'redis_streams' is used as a publisher, but 'internal_buffer_size' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "grpc")]
        EndpointType::Grpc(_) => Ok(warnings),
        #[cfg(feature = "sqlx")]
        EndpointType::Sqlx(cfg) => {
            if cfg.select_query.is_some() {
                warnings.push(
                    "Endpoint 'sqlx' is used as a publisher, but 'select_query' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.delete_after_read {
                warnings.push(
                    "Endpoint 'sqlx' is used as a publisher, but 'delete_after_read' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.polling_interval_ms.is_some() {
                warnings.push(
                    "Endpoint 'sqlx' is used as a publisher, but 'polling_interval_ms' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "clickhouse")]
        EndpointType::ClickHouse(cfg) => {
            if cfg.cursor_column.is_some() {
                warnings.push("Endpoint 'clickhouse' is used as a publisher, but 'cursor_column' is a consumer-only option and will be ignored.".to_string());
            }
            if cfg.checkpoint_store.is_some() {
                warnings.push("Endpoint 'clickhouse' is used as a publisher, but 'checkpoint_store' is a consumer-only option and will be ignored.".to_string());
            }
            if cfg.polling_interval_ms.is_some() {
                warnings.push("Endpoint 'clickhouse' is used as a publisher, but 'polling_interval_ms' is a consumer-only option and will be ignored.".to_string());
            }
            Ok(warnings)
        }
        #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
        EndpointType::IbmMq(cfg) => {
            if cfg.wait_timeout_ms != 1000 {
                warnings.push(
                    "Endpoint 'ibmmq' is used as a publisher, but 'wait_timeout_ms' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "mongodb")]
        EndpointType::MongoDb(cfg) => {
            if cfg.polling_interval_ms.is_some() {
                warnings.push(
                    "Endpoint 'mongodb' is used as a publisher, but 'polling_interval_ms' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.change_stream {
                warnings.push(
                    "Endpoint 'mongodb' is used as a publisher, but 'change_stream' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.cursor_id.is_some() {
                warnings.push(
                    "Endpoint 'mongodb' is used as a publisher, but 'cursor_id' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        EndpointType::File(_) => Ok(warnings),
        #[cfg(feature = "object-store")]
        EndpointType::ObjectStore(cfg) => {
            if cfg.checkpoint_store.is_some() {
                warnings.push("Endpoint 'object_store' is used as a publisher, but 'checkpoint_store' is a consumer-only option and will be ignored.".to_string());
            }
            if cfg.cursor_id.is_some() {
                warnings.push("Endpoint 'object_store' is used as a publisher, but 'cursor_id' is a consumer-only option and will be ignored.".to_string());
            }
            if cfg.polling_interval_ms.is_some() {
                warnings.push("Endpoint 'object_store' is used as a publisher, but 'polling_interval_ms' is a consumer-only option and will be ignored.".to_string());
            }
            if cfg.max_object_bytes.is_some() {
                warnings.push("Endpoint 'object_store' is used as a publisher, but 'max_object_bytes' is a consumer-only option and will be ignored.".to_string());
            }
            Ok(warnings)
        }
        #[cfg(feature = "websocket")]
        EndpointType::WebSocket(_) => Ok(warnings),
        EndpointType::Static(_) => Ok(warnings),
        EndpointType::Memory(cfg) => {
            if cfg.subscribe_mode {
                warnings.push(
                    "Endpoint 'memory' is used as a publisher, but 'subscribe_mode' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.enable_nack {
                warnings.push(
                    "Endpoint 'memory' is used as a publisher, but 'enable_nack' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        EndpointType::StreamBuffer(cfg) => {
            if cfg.correlation_id.is_some() {
                warnings.push(
                    "Endpoint 'stream_buffer' is used as a publisher, but 'correlation_id' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        #[cfg(feature = "sled")]
        EndpointType::Sled(cfg) => {
            if cfg.read_from_start {
                warnings.push(
                    "Endpoint 'sled' is used as a publisher, but 'read_from_start' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            if cfg.delete_after_read {
                warnings.push(
                    "Endpoint 'sled' is used as a publisher, but 'delete_after_read' is a consumer-only option and will be ignored."
                    .to_string()
                );
            }
            Ok(warnings)
        }
        EndpointType::Null => Ok(warnings),
        EndpointType::Fanout(endpoints) => {
            for endpoint in endpoints {
                warnings.extend(check_publisher_recursive(
                    route_name,
                    endpoint,
                    depth + 1,
                    allowed_types,
                )?);
            }
            Ok(warnings)
        }
        EndpointType::Switch(cfg) => {
            for endpoint in cfg.cases.values() {
                warnings.extend(check_publisher_recursive(
                    route_name,
                    endpoint,
                    depth + 1,
                    allowed_types,
                )?);
            }
            if let Some(endpoint) = &cfg.default {
                warnings.extend(check_publisher_recursive(
                    route_name,
                    endpoint,
                    depth + 1,
                    allowed_types,
                )?);
            }
            Ok(warnings)
        }
        EndpointType::Response(_) => Ok(warnings),
        EndpointType::Custom { .. } => Ok(warnings),
        EndpointType::Reader(inner) => check_consumer(route_name, inner, allowed_types),
        EndpointType::Request(cfg) => {
            warnings.extend(check_publisher_recursive(
                route_name,
                &cfg.to,
                depth + 1,
                allowed_types,
            )?);
            warnings.extend(check_publisher_recursive(
                route_name,
                &cfg.forward_to,
                depth + 1,
                allowed_types,
            )?);
            Ok(warnings)
        }
        #[allow(unreachable_patterns)]
        _ => {
            if let Some(allowed) = allowed_types {
                let name = endpoint.endpoint_type.name();
                if allowed.contains(&name) {
                    return Ok(warnings);
                }
            }
            Err(anyhow!(
                "[route:{}] Unsupported publisher endpoint type '{:?}'",
                route_name,
                endpoint.endpoint_type
            ))
        }
    }
}

/// Creates a `MessagePublisher` based on the route's "out" configuration.
pub async fn create_publisher_from_route(
    route_name: &str,
    endpoint: &Endpoint,
) -> Result<Arc<dyn MessagePublisher>> {
    check_publisher(route_name, endpoint, None)?;
    create_publisher_with_depth(route_name.to_string(), endpoint.clone(), 0).await
}

fn create_publisher_with_depth(
    route_name: String,
    endpoint: Endpoint,
    depth: usize,
) -> BoxFuture<'static, Result<Arc<dyn MessagePublisher>>> {
    Box::pin(async move {
        const MAX_DEPTH: usize = 16;
        if depth > MAX_DEPTH {
            return Err(anyhow!(
                "Fanout/Ref recursion depth exceeded limit of {}",
                MAX_DEPTH
            ));
        }

        if let EndpointType::Ref(name) = &endpoint.endpoint_type {
            let referenced_opt = crate::route::get_endpoint(name);

            if referenced_opt.is_none() {
                if let Some(pub_instance) = crate::publisher::get_publisher(name) {
                    let inner = pub_instance.inner();
                    let mut publisher: Box<dyn MessagePublisher> = Box::new(inner);

                    if let Some(handler) = &endpoint.handler {
                        publisher = Box::new(crate::command_handler::CommandPublisher::new(
                            publisher,
                            handler.clone(),
                        ));
                    }
                    return crate::middleware::apply_middlewares_to_publisher(
                        publisher,
                        &endpoint,
                        &route_name,
                    )
                    .await;
                }
            }

            let referenced = referenced_opt.ok_or_else(|| {
                anyhow!(
                    "[route:{}] Referenced endpoint '{}' not found",
                    route_name,
                    name
                )
            })?;

            let mut merged = referenced;
            // Merge middlewares: The ref's middlewares should be outer (applied last).
            // Since apply_middlewares_to_publisher iterates forward, we append the ref's middlewares to the referenced ones.
            merged.middlewares.extend(endpoint.middlewares);

            if endpoint.handler.is_some() {
                if merged.handler.is_some() {
                    return Err(anyhow!("[route:{}] Both ref endpoint and referenced endpoint '{}' have handlers defined. This is ambiguous.", route_name, name));
                }
                merged.handler = endpoint.handler;
            }

            return create_publisher_with_depth(route_name, merged, depth + 1).await;
        }

        let mut publisher =
            create_base_publisher(&route_name, &endpoint.endpoint_type, depth).await?;
        if let Some(handler) = &endpoint.handler {
            publisher = Box::new(crate::command_handler::CommandPublisher::new(
                publisher,
                handler.clone(),
            ));
        }
        crate::middleware::apply_middlewares_to_publisher(publisher, &endpoint, &route_name).await
    })
}

async fn create_base_publisher(
    route_name: &str,
    endpoint_type: &EndpointType,
    depth: usize,
) -> Result<Box<dyn MessagePublisher>> {
    let publisher = match endpoint_type {
        #[cfg(feature = "aws")]
        EndpointType::Aws(cfg) => {
            Ok(Box::new(aws::AwsPublisher::new(cfg).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "kafka")]
        EndpointType::Kafka(cfg) => {
            let mut config = cfg.clone();
            if config.topic.is_none() {
                config.topic = Some(route_name.to_string());
            }
            Ok(Box::new(kafka::KafkaPublisher::new(&config).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "nats")]
        EndpointType::Nats(cfg) => {
            let mut config = cfg.clone();
            if config.subject.is_none() {
                config.subject = Some(route_name.to_string());
            }
            Ok(Box::new(nats::NatsPublisher::new(&config).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "amqp")]
        EndpointType::Amqp(cfg) => {
            let mut config = cfg.clone();
            if config.queue.is_none() {
                config.queue = Some(route_name.to_string());
            }
            Ok(Box::new(amqp::AmqpPublisher::new(&config).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "mqtt")]
        EndpointType::Mqtt(cfg) => {
            let mut config = cfg.clone();
            if config.topic.is_none() {
                config.topic = Some(route_name.to_string());
            }
            if config.client_id.is_none() {
                config.client_id = Some(format!("{}-{}", crate::APP_NAME, route_name));
            }
            Ok(Box::new(mqtt::MqttPublisher::new(&config).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(any(feature = "zeromq", feature = "zeromq-omq"))]
        EndpointType::ZeroMq(cfg) => zeromq::create_publisher(cfg).await,
        #[cfg(feature = "redis-streams")]
        EndpointType::RedisStreams(cfg) => {
            let mut config = cfg.clone();
            if config.stream.is_none() {
                config.stream = Some(route_name.to_string());
            }
            Ok(
                Box::new(redis_streams::RedisStreamsPublisher::new(&config).await?)
                    as Box<dyn MessagePublisher>,
            )
        }
        #[cfg(feature = "grpc")]
        EndpointType::Grpc(cfg) => {
            Ok(Box::new(grpc::GrpcPublisher::new(cfg).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "sqlx")]
        EndpointType::Sqlx(cfg) => {
            Ok(Box::new(sqlx::SqlxPublisher::new(cfg).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "clickhouse")]
        EndpointType::ClickHouse(cfg) => {
            Ok(Box::new(clickhouse::ClickHousePublisher::new(cfg).await?)
                as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "http")]
        EndpointType::Http(cfg) => {
            let stream_response_sink =
                if let Some(stream_response_to) = cfg.stream_response_to.as_deref() {
                    Some(
                        create_publisher_with_depth(
                            route_name.to_string(),
                            stream_response_to.clone(),
                            depth + 1,
                        )
                        .await?,
                    )
                } else {
                    None
                };
            let sink =
                http::HttpPublisher::new_with_stream_response_sink(cfg, stream_response_sink)
                    .await?;
            Ok(Box::new(sink) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "websocket")]
        EndpointType::WebSocket(cfg) => {
            let sink = websocket::WebSocketPublisher::new(cfg);
            Ok(Box::new(sink) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "mongodb")]
        EndpointType::MongoDb(cfg) => {
            let mut config = cfg.clone();
            if config.collection.is_none() {
                config.collection = Some(route_name.to_string());
            }
            Ok(Box::new(mongodb::MongoDbPublisher::new(&config).await?)
                as Box<dyn MessagePublisher>)
        }
        EndpointType::File(cfg) => {
            Ok(Box::new(file::FilePublisher::new(cfg).await?) as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "object-store")]
        EndpointType::ObjectStore(cfg) => Ok(Box::new(
            object_store::ObjectStorePublisher::new(cfg).await?,
        ) as Box<dyn MessagePublisher>),
        EndpointType::Static(cfg) => Ok(Box::new(static_endpoint::StaticEndpointPublisher::new(
            cfg,
        )?) as Box<dyn MessagePublisher>),
        EndpointType::Memory(cfg) => {
            Ok(Box::new(memory::MemoryPublisher::new_async(cfg).await?)
                as Box<dyn MessagePublisher>)
        }
        EndpointType::StreamBuffer(cfg) => {
            Ok(Box::new(stream_buffer::StreamBufferPublisher::new(cfg)?)
                as Box<dyn MessagePublisher>)
        }
        #[cfg(feature = "sled")]
        EndpointType::Sled(cfg) => {
            Ok(Box::new(sled::SledPublisher::new(cfg)?) as Box<dyn MessagePublisher>)
        }
        #[cfg(any(feature = "ibm-mq-static", feature = "ibm-mq"))]
        EndpointType::IbmMq(cfg) => {
            Ok(Box::new(ibm_mq::IbmMqPublisher::new(cfg).await?) as Box<dyn MessagePublisher>)
        }
        EndpointType::Null => Ok(Box::new(null::NullPublisher) as Box<dyn MessagePublisher>),
        EndpointType::Fanout(endpoints) => {
            let mut publishers = Vec::with_capacity(endpoints.len());
            for endpoint in endpoints {
                let p = create_publisher_with_depth(
                    route_name.to_string(),
                    endpoint.clone(),
                    depth + 1,
                )
                .await?;
                publishers.push(p);
            }
            Ok(Box::new(fanout::FanoutPublisher::new(publishers)) as Box<dyn MessagePublisher>)
        }
        EndpointType::Switch(cfg) => {
            let mut cases = std::collections::HashMap::new();
            for (key, endpoint) in &cfg.cases {
                let p = create_publisher_with_depth(
                    route_name.to_string(),
                    endpoint.clone(),
                    depth + 1,
                )
                .await?;
                cases.insert(key.clone(), p);
            }
            let default = if let Some(endpoint) = &cfg.default {
                Some(
                    create_publisher_with_depth(
                        route_name.to_string(),
                        (**endpoint).clone(),
                        depth + 1,
                    )
                    .await?,
                )
            } else {
                None
            };
            Ok(Box::new(switch::SwitchPublisher::new(
                cfg.metadata_key.clone(),
                cases,
                default,
            )) as Box<dyn MessagePublisher>)
        }
        EndpointType::Response(_) => {
            Ok(Box::new(response::ResponsePublisher) as Box<dyn MessagePublisher>)
        }
        EndpointType::Reader(inner) => {
            let consumer = create_consumer_from_route(route_name, inner).await?;
            Ok(Box::new(reader::ReaderPublisher::new(consumer)) as Box<dyn MessagePublisher>)
        }
        EndpointType::Request(cfg) => {
            let request =
                create_publisher_with_depth(route_name.to_string(), (*cfg.to).clone(), depth + 1)
                    .await?;
            let forward = create_publisher_with_depth(
                route_name.to_string(),
                (*cfg.forward_to).clone(),
                depth + 1,
            )
            .await?;
            Ok(
                Box::new(request::RequestForwardPublisher::new(request, forward))
                    as Box<dyn MessagePublisher>,
            )
        }
        EndpointType::Custom { name, config } => {
            let factory = get_endpoint_factory(name)
                .ok_or_else(|| anyhow!("Custom endpoint factory '{}' not found", name))?;
            factory.create_publisher(route_name, config).await
        }
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!(
            "[route:{}] Unsupported publisher endpoint type '{:?}'",
            route_name,
            endpoint_type
        )),
    }?;
    Ok(publisher)
}

/// Returns the active process-level rustls `CryptoProvider`, or a descriptive error if none
/// has been installed yet.
///
/// This is called by every endpoint that creates a rustls `ClientConfig` / `ServerConfig`.
/// As a library, mq-bridge never installs a provider itself; the choice belongs to the
/// application binary.  To resolve the error, either:
///
/// * Enable the **`rustls-ring`** or **`rustls-aws-lc`** feature of `mq-bridge`, or
/// * Call `rustls::crypto::CryptoProvider::install_default()` early in your `main()`.
#[cfg(feature = "rustls")]
#[allow(unused)]
pub(crate) fn get_crypto_provider() -> anyhow::Result<std::sync::Arc<rustls::crypto::CryptoProvider>>
{
    rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .ok_or_else(|| {
            anyhow!("No rustls CryptoProvider is installed.\n\
Fix: enable the `rustls-ring` or `rustls-aws-lc` feature of mq-bridge, or call `rustls::crypto::CryptoProvider::install_default()` in your application binary before creating any TLS endpoint.")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Endpoint, EndpointType};
    use crate::CanonicalMessage;

    #[tokio::test]
    async fn test_fanout_publisher_integration() {
        let ep1 = Endpoint::new_memory("fanout_1", 10);
        let ep2 = Endpoint::new_memory("fanout_2", 10);

        let chan1 = ep1.channel().unwrap();
        let chan2 = ep2.channel().unwrap();
        let fanout_ep = Endpoint::new(EndpointType::Fanout(vec![ep1, ep2]));

        let publisher = create_publisher_from_route("test_fanout", &fanout_ep)
            .await
            .expect("Failed to create fanout publisher");

        let msg = CanonicalMessage::new(b"fanout_payload".to_vec(), None);
        publisher.send(msg).await.expect("Failed to send message");

        assert_eq!(chan1.len(), 1);
        assert_eq!(chan2.len(), 1);

        let msg1 = chan1.drain_messages().pop().unwrap();
        let msg2 = chan2.drain_messages().pop().unwrap();

        assert_eq!(msg1.payload, "fanout_payload".as_bytes());
        assert_eq!(msg2.payload, "fanout_payload".as_bytes());
    }

    use crate::models::MemoryConfig;
    #[tokio::test]
    async fn test_factory_creates_memory_subscriber() {
        let endpoint = Endpoint {
            endpoint_type: EndpointType::Memory(
                MemoryConfig::new("mem".to_string(), None).with_subscribe(true),
            ),
            middlewares: vec![],
            handler: None,
        };

        let consumer = create_consumer_from_route("test", &endpoint).await.unwrap();
        // Check if it is a MemoryConsumer (MemorySubscriber was merged)
        let is_subscriber = consumer
            .as_any()
            .is::<crate::endpoints::memory::MemoryConsumer>();
        assert!(is_subscriber, "Factory should create MemoryConsumer");
    }

    #[cfg(feature = "websocket")]
    #[test]
    fn websocket_direct_route_support_requires_default_route_options() {
        let mut options = crate::models::RouteOptions::default();
        assert!(websocket_direct_route_options_allowed(&options));

        options.batch_size = 128;
        assert!(!websocket_direct_route_options_allowed(&options));
    }

    #[cfg(feature = "websocket")]
    #[test]
    fn websocket_direct_route_support_respects_execution_mode_and_output() {
        let input = Endpoint::new(EndpointType::WebSocket(
            crate::models::WebSocketConfig::new("127.0.0.1:0"),
        ));
        let response_route = crate::models::Route::new(input.clone(), Endpoint::new_response());
        assert!(matches!(
            websocket_direct_route_support(&response_route),
            WebSocketDirectRouteSupport::Supported
        ));

        let memory_route = crate::models::Route::new(input.clone(), Endpoint::new_memory("ws", 1));
        assert!(matches!(
            websocket_direct_route_support(&memory_route),
            WebSocketDirectRouteSupport::Unsupported("output is not response")
        ));

        let routed_input = Endpoint::new(EndpointType::WebSocket(
            crate::models::WebSocketConfig::new("127.0.0.1:0")
                .with_execution_mode(crate::models::WebSocketExecutionMode::Routed),
        ));
        let routed_route = crate::models::Route::new(routed_input, Endpoint::new_response());
        assert!(matches!(
            websocket_direct_route_support(&routed_route),
            WebSocketDirectRouteSupport::Unsupported("execution_mode is routed")
        ));
    }

    #[test]
    fn test_endpoint_middleware_ordering_helpers() {
        let endpoint = Endpoint::new_memory("test", 10)
            .with_metrics()
            .with_dlq(crate::models::DeadLetterQueueMiddleware::default())
            .with_retry(crate::models::RetryMiddleware::default());

        // Expected order: Retry, Dlq, Metrics
        assert_eq!(endpoint.middlewares.len(), 3);
        assert!(matches!(endpoint.middlewares[0], Middleware::Retry(_)));
        assert!(matches!(endpoint.middlewares[1], Middleware::Dlq(_)));
        assert!(matches!(endpoint.middlewares[2], Middleware::Metrics(_)));
    }

    #[cfg(feature = "http")]
    #[test]
    fn test_http_inline_fast_path_allows_simple_output_publisher_middlewares() {
        assert!(output_middlewares_allow_http_inline_fast_path(&[
            Middleware::Buffer(crate::models::BufferMiddleware {
                max_messages: 16,
                max_delay_ms: 0,
            }),
            Middleware::Delay(crate::models::DelayMiddleware { delay_ms: 0 }),
            Middleware::Limiter(crate::models::LimiterMiddleware {
                messages_per_second: 1_000_000.0,
            }),
        ]));

        assert!(!output_middlewares_allow_http_inline_fast_path(&[
            Middleware::Retry(crate::models::RetryMiddleware::default()),
        ]));
        assert!(!output_middlewares_allow_http_inline_fast_path(&[
            Middleware::Dlq(Box::default()),
        ]));
    }

    #[test]
    fn test_consumer_middleware_ordering() {
        let endpoint = Endpoint::new_memory("test", 10)
            .with_deduplication(crate::models::DeduplicationMiddleware {
                store: None,
                sled_path: Some("".into()),
                ttl_seconds: 10,
            })
            .with_consumer_metrics();

        // Expected order in list: [Metrics, Dedup]
        // Consumer application (rev): Dedup -> Metrics.
        // Execution: Metrics( Dedup ( base ) ). Metrics is Outer.
        assert_eq!(endpoint.middlewares.len(), 2);
        assert!(matches!(endpoint.middlewares[0], Middleware::Metrics(_)));
        assert!(matches!(
            endpoint.middlewares[1],
            Middleware::Deduplication(_)
        ));
    }

    #[test]
    fn test_check_consumer_invalid_config() {
        let config = crate::models::MemoryConfig {
            topic: "test".to_string(),
            request_reply: true, // Invalid for consumer
            ..Default::default()
        };
        let endpoint = Endpoint::new(EndpointType::Memory(config));

        let warnings = check_consumer("test_route", &endpoint, None).unwrap();
        assert!(warnings
            .iter()
            .any(|w| w.contains("request_reply") && w.contains("publisher-only")));
    }
}
