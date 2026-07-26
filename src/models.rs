//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::traits::Handler;
use tracing::trace;

/// The top-level configuration is a map of named routes.
/// The key is the route name (e.g., "kafka_to_nats").
///
/// # Examples
///
/// Deserializing a complex configuration from YAML:
///
/// ```
/// use mq_bridge::models::{Config, EndpointType, Middleware};
///
/// let yaml = r#"
/// kafka_to_nats:
///   concurrency: 10
///   input:
///     middlewares:
///       - deduplication:
///           sled_path: "/tmp/mq-bridge/dedup_db"
///           ttl_seconds: 3600
///       - metrics: {}
///       - retry:
///           max_attempts: 5
///           initial_interval_ms: 200
///       - random_panic:
///           mode: nack
///       - dlq:
///           endpoint:
///             nats:
///               subject: "dlq-subject"
///               url: "nats://localhost:4222"
///     kafka:
///       topic: "input-topic"
///       url: "localhost:9092"
///       group_id: "my-consumer-group"
///       tls:
///         required: true
///         ca_file: "/path_to_ca"
///         cert_file: "/path_to_cert"
///         key_file: "/path_to_key"
///         cert_password: "password"
///         accept_invalid_certs: true
///   output:
///     middlewares:
///       - metrics: {}
///       - dlq:
///           endpoint:
///             file:
///               path: "error.out"
///     nats:
///       subject: "output-subject"
///       url: "nats://localhost:4222"
/// "#;
///
/// let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
/// let route = config.get("kafka_to_nats").unwrap();
///
/// assert_eq!(route.options.concurrency, 10);
/// // Check input middleware
/// assert!(route.input.middlewares.iter().any(|m| matches!(m, Middleware::Deduplication(_))));
/// // Check output endpoint
/// assert!(matches!(route.output.endpoint_type, EndpointType::Nats(_)));
/// ```
pub type Config = HashMap<String, Route>;

/// A configuration map for named publishers (endpoints).
/// The key is the publisher name.
pub type PublisherConfig = HashMap<String, Endpoint>;

/// Defines a single message processing route from an input to an output.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(transform = route_schema_transform))]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// The input/source endpoint for the route.
    pub input: Endpoint,
    /// The output/sink endpoint for the route.
    #[serde(default = "default_output_endpoint")]
    pub output: Endpoint,
    /// (Optional) Fine-tuning options for the route's execution.
    #[serde(flatten, default)]
    pub options: RouteOptions,
}

impl Default for Route {
    fn default() -> Self {
        Self {
            input: Endpoint::null(),
            output: Endpoint::null(),
            options: RouteOptions::default(),
        }
    }
}

/// Fine-tuning options for a route's execution.
///
/// These options control concurrency, batching, and commit behavior for message processing.
///
/// # Examples
///
/// ```
/// use mq_bridge::models::RouteOptions;
///
/// let options = RouteOptions {
///     description: "My Route".to_string(),
///     concurrency: 10,
///     batch_size: 5,
///     commit_concurrency_limit: 1024,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RouteOptions {
    /// A human-readable description of the route's purpose. Defaults to an empty string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// (Optional) Number of concurrent processing tasks for this route. While it improves throughput for high-latency
    /// handlers, it adds synchronization overhead for ordered commits and may lead to out-of-order processing
    /// in the handler. Defaults to 1.
    #[serde(default = "default_concurrency")]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub concurrency: usize,
    /// (Optional) Maximum number of messages to process in a single batch. The consumer waits for at least one message
    /// and then attempts to fetch more if available. Increasing this improves throughput but also increases
    /// the potential impact of a single batch processing failure. Defaults to 1.
    #[serde(default = "default_batch_size")]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub batch_size: usize,
    /// (Optional) The maximum number of in-flight commit requests queued for ordered sequencing.
    /// Lower values apply backpressure earlier; higher values allow larger commit backlogs.
    /// Defaults to 4096.
    #[serde(default = "default_commit_concurrency_limit")]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub commit_concurrency_limit: usize,
    /// Time to wait for a route to establish connections before startup fails. Defaults to 5000ms.
    #[serde(default = "default_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
    /// Time to wait before reconnecting after a transient route failure. Defaults to 5000ms.
    #[serde(default = "default_reconnect_interval_ms")]
    pub reconnect_interval_ms: u64,
    /// Delay after an empty receive batch to avoid hot polling. Set to 0 to only yield. Defaults to 10ms.
    #[serde(default = "default_empty_batch_delay_ms")]
    pub empty_batch_delay_ms: u64,
    /// Allows fault-injection middleware such as random_panic. Disabled by default.
    #[serde(default = "default_false", skip_serializing_if = "is_false")]
    #[cfg_attr(feature = "schema", schemars(default = "default_false"))]
    pub allow_fault_injection: bool,
    /// If true, the route exits gracefully once the source yields an empty batch
    /// (drain-then-exit). Off by default — routes normally poll indefinitely.
    #[serde(default = "default_false", skip_serializing_if = "is_false")]
    #[cfg_attr(feature = "schema", schemars(default = "default_false"))]
    pub exit_on_empty: bool,
}

impl Default for RouteOptions {
    fn default() -> Self {
        Self {
            description: String::new(),
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            commit_concurrency_limit: default_commit_concurrency_limit(),
            startup_timeout_ms: default_startup_timeout_ms(),
            reconnect_interval_ms: default_reconnect_interval_ms(),
            empty_batch_delay_ms: default_empty_batch_delay_ms(),
            allow_fault_injection: false,
            exit_on_empty: false,
        }
    }
}

impl RouteOptions {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.concurrency == 0 {
            return Err(anyhow::anyhow!("route concurrency must be at least 1"));
        }
        if self.batch_size == 0 {
            return Err(anyhow::anyhow!("route batch_size must be at least 1"));
        }
        if self.commit_concurrency_limit == 0 {
            return Err(anyhow::anyhow!(
                "route commit_concurrency_limit must be at least 1"
            ));
        }
        Ok(())
    }
}

pub(crate) fn default_concurrency() -> usize {
    1
}

pub(crate) fn default_batch_size() -> usize {
    1
}

pub(crate) fn default_commit_concurrency_limit() -> usize {
    4096
}

pub(crate) fn default_startup_timeout_ms() -> u64 {
    5000
}

pub(crate) fn default_reconnect_interval_ms() -> u64 {
    5000
}

pub(crate) fn default_empty_batch_delay_ms() -> u64 {
    10
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_false() -> bool {
    false
}

#[cfg(feature = "schema")]
fn default_inline_response_fast_path_schema() -> Option<bool> {
    Some(true)
}

/// Schema default for `shared` fields, whose runtime default is `true`.
#[cfg(feature = "schema")]
fn default_shared_schema() -> Option<bool> {
    Some(true)
}

/// Schema default for Kafka `partitions`, whose runtime default is 6.
#[cfg(feature = "schema")]
fn default_kafka_partitions_schema() -> Option<i32> {
    Some(DEFAULT_KAFKA_PARTITIONS)
}

/// Partition count used when auto-creating a Kafka topic if none is configured.
/// ordering is per-key (we key by message_id), not global across the topic if > 1
pub const DEFAULT_KAFKA_PARTITIONS: i32 = 6;

fn default_output_endpoint() -> Endpoint {
    Endpoint::new(EndpointType::Null)
}

fn default_retry_attempts() -> usize {
    3
}
fn default_initial_interval_ms() -> u64 {
    100
}
fn default_max_interval_ms() -> u64 {
    5000
}
fn default_multiplier() -> f64 {
    2.0
}
fn default_clean_session() -> bool {
    false
}
fn default_cookie_metadata_key() -> String {
    "cookie".to_string()
}
fn default_set_cookie_metadata_key() -> String {
    "set-cookie".to_string()
}

fn is_known_endpoint_name(name: &str) -> bool {
    matches!(
        name,
        "aws"
            | "kafka"
            | "nats"
            | "file"
            | "static"
            | "memory"
            | "sled"
            | "amqp"
            | "mongodb"
            | "mqtt"
            | "http"
            | "websocket"
            | "ibmmq"
            | "zeromq"
            | "grpc"
            | "fanout"
            | "stream_buffer"
            | "ref"
            | "switch"
            | "response"
            | "reader"
            | "null"
            | "sqlx"
    )
}

/// Represents a connection point for messages, which can be a source (input) or a sink (output).
#[derive(Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    /// (Optional) A list of middlewares to apply to the endpoint.
    #[serde(default)]
    pub middlewares: Vec<Middleware>,

    /// The specific endpoint implementation, determined by the configuration key (e.g., "kafka", "nats").
    #[serde(flatten)]
    pub endpoint_type: EndpointType,

    #[serde(skip_serializing)]
    #[cfg_attr(feature = "schema", schemars(skip))]
    /// Internal handler for processing messages (not serialized).
    pub handler: Option<Arc<dyn Handler>>,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("middlewares", &self.middlewares)
            .field("endpoint_type", &self.endpoint_type)
            .field(
                "handler",
                &if self.handler.is_some() {
                    "Some(<Handler>)"
                } else {
                    "None"
                },
            )
            .finish()
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EndpointVisitor;

        impl<'de> Visitor<'de> for EndpointVisitor {
            type Value = Endpoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map representing an endpoint or null")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Endpoint::new(EndpointType::Null))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                // Buffer the map into a temporary serde_json::Map.
                // This allows us to separate the `middlewares` field from the rest.
                let mut temp_map = serde_json::Map::new();
                let mut middlewares_val = None;

                while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                    if key == "middlewares" {
                        middlewares_val = Some(value);
                    } else {
                        temp_map.insert(key, value);
                    }
                }

                // Deserialize the rest of the map into the flattened EndpointType.
                let temp_val = serde_json::Value::Object(temp_map);
                let endpoint_type: EndpointType = match serde_json::from_value(temp_val.clone()) {
                    Ok(et) => et,
                    Err(original_err) => {
                        if let serde_json::Value::Object(map) = &temp_val {
                            if map.len() == 1 {
                                let (name, config) = map.iter().next().unwrap();
                                if is_known_endpoint_name(name) {
                                    return Err(serde::de::Error::custom(original_err));
                                }
                                trace!("Falling back to Custom endpoint for key: {}", name);
                                EndpointType::Custom {
                                    name: name.clone(),
                                    config: config.clone(),
                                }
                            } else if map.is_empty() {
                                EndpointType::Null
                            } else {
                                return Err(serde::de::Error::custom(
                                    "Invalid endpoint configuration: multiple keys found or unknown endpoint type",
                                ));
                            }
                        } else {
                            return Err(serde::de::Error::custom("Invalid endpoint configuration"));
                        }
                    }
                };

                // Deserialize the extracted middlewares value using the existing helper logic.
                let middlewares = match middlewares_val {
                    Some(val) => {
                        deserialize_middlewares_from_value(val).map_err(serde::de::Error::custom)?
                    }
                    None => Vec::new(),
                };

                Ok(Endpoint {
                    middlewares,
                    endpoint_type,
                    handler: None,
                })
            }
        }

        deserializer.deserialize_any(EndpointVisitor)
    }
}

fn is_known_middleware_name(name: &str) -> bool {
    matches!(
        name,
        "deduplication"
            | "metrics"
            | "dlq"
            | "retry"
            | "random_panic"
            | "delay"
            | "weak_join"
            | "limiter"
            | "buffer"
            | "cookie_jar"
            | "custom"
    )
}

/// Deserialize middlewares from a generic serde_json::Value.
///
/// This logic was extracted from `deserialize_middlewares_from_map_or_seq` to be reused by the custom `Endpoint` deserializer.
fn deserialize_middlewares_from_value(value: serde_json::Value) -> anyhow::Result<Vec<Middleware>> {
    let arr = match value {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Object(map) => {
            let mut middlewares: Vec<_> = map
                .into_iter()
                // The config crate can produce maps with numeric string keys ("0", "1", ...)
                // from environment variables. We need to sort by these keys to maintain order.
                .filter_map(|(key, value)| key.parse::<usize>().ok().map(|index| (index, value)))
                .collect();
            middlewares.sort_by_key(|(index, _)| *index);

            middlewares.into_iter().map(|(_, value)| value).collect()
        }
        _ => return Err(anyhow::anyhow!("Expected an array or object")),
    };

    let mut middlewares = Vec::new();
    for item in arr {
        // Check if it is a map with a single key that matches a known middleware
        let known_name = if let serde_json::Value::Object(map) = &item {
            if map.len() == 1 {
                let (name, _) = map.iter().next().unwrap();
                if is_known_middleware_name(name) {
                    Some(name.clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(name) = known_name {
            match serde_json::from_value::<Middleware>(item.clone()) {
                Ok(m) => middlewares.push(m),
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to deserialize known middleware '{}': {}",
                        name,
                        e
                    ))
                }
            }
        } else if let Ok(m) = serde_json::from_value::<Middleware>(item.clone()) {
            middlewares.push(m);
        } else if let serde_json::Value::Object(map) = &item {
            if map.len() == 1 {
                let (name, config) = map.iter().next().unwrap();
                middlewares.push(Middleware::Custom {
                    name: name.clone(),
                    config: config.clone(),
                });
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid middleware configuration: {:?}",
                    item
                ));
            }
        } else {
            return Err(anyhow::anyhow!(
                "Invalid middleware configuration: {:?}",
                item
            ));
        }
    }
    Ok(middlewares)
}

/// Configuration for the `static` endpoint.
///
/// Accepts either a bare string (the response body, JSON-encoded for backward
/// compatibility) or a map for full control:
///
/// ```yaml
/// # bare string  -> body is JSON-encoded ("Hello" comes back quoted)
/// static: "Hello, World!"
///
/// # map form -> raw body + custom metadata (HTTP maps metadata to headers)
/// static:
///   body: "Hello, World!"
///   raw: true
///   metadata:
///     content-type: "text/plain"
///     server: "mq-bridge"
/// ```
///
/// When `raw` is true the body is sent verbatim; otherwise it is JSON-encoded as
/// a string. Every entry in `metadata` is attached to the produced message; when
/// this endpoint feeds an HTTP response, those entries become response headers
/// (e.g. `content-type`), otherwise they are ordinary message metadata.
///
/// The `body` supports `${…}` placeholders (compiled once at startup): request
/// fields `${payload:a.b}` / `${metadata:key}` / `${message:id}`, generators
/// `${gen:uuid|now|timestamp|counter|random(1,100)}`, and `${env:VAR}`. When the
/// `content-type` metadata is a JSON type, interpolated request values are
/// JSON-escaped by default; append `| raw` to splice verbatim, and write `$${…}`
/// to emit a literal `${…}`. See [`crate::support::interpolation`] for the full reference.
#[derive(Debug, Clone, Default)]
pub struct StaticConfig {
    /// The static response body.
    pub body: String,
    /// Send the body verbatim instead of JSON-encoding it as a string.
    pub raw: bool,
    /// Extra metadata entries attached to the produced message.
    pub metadata: std::collections::HashMap<String, String>,
}

// Hand-written schema: the `Deserialize` impl below accepts either a bare string
// or a map where only `body` is required, so the derived all-fields-required
// object schema would reject valid configs.
#[cfg(feature = "schema")]
impl schemars::JsonSchema for StaticConfig {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "StaticConfig".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "Configuration for the `static` endpoint. Accepts either a bare string (the response body, JSON-encoded for backward compatibility) or a map where only `body` is required and `raw` / `metadata` are optional.",
            "oneOf": [
                {
                    "type": "string",
                    "description": "The response body, JSON-encoded as a string."
                },
                {
                    "type": "object",
                    "properties": {
                        "body": {
                            "type": "string",
                            "description": "The static response body."
                        },
                        "raw": {
                            "type": "boolean",
                            "description": "Send the body verbatim instead of JSON-encoding it as a string.",
                            "default": false
                        },
                        "metadata": {
                            "type": "object",
                            "description": "Extra metadata entries attached to the produced message.",
                            "additionalProperties": { "type": "string" }
                        }
                    },
                    "required": ["body"],
                    "additionalProperties": false
                }
            ]
        })
    }
}

impl Serialize for StaticConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Backward-compatible: when no extra options are set, serialize as a bare
        // string exactly like the historical `Static(String)` so configs written
        // by this version remain readable by older versions.
        if !self.raw && self.metadata.is_empty() {
            return serializer.serialize_str(&self.body);
        }
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("StaticConfig", 3)?;
        state.serialize_field("body", &self.body)?;
        state.serialize_field("raw", &self.raw)?;
        state.serialize_field("metadata", &self.metadata)?;
        state.end()
    }
}

impl From<String> for StaticConfig {
    fn from(body: String) -> Self {
        StaticConfig {
            body,
            raw: false,
            metadata: std::collections::HashMap::new(),
        }
    }
}

impl From<&str> for StaticConfig {
    fn from(body: &str) -> Self {
        StaticConfig::from(body.to_string())
    }
}

impl<'de> Deserialize<'de> for StaticConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Str(String),
            Map {
                body: String,
                #[serde(default)]
                raw: bool,
                #[serde(default)]
                metadata: std::collections::HashMap<String, String>,
            },
        }
        Ok(match Repr::deserialize(deserializer)? {
            Repr::Str(body) => StaticConfig {
                body,
                raw: false,
                metadata: std::collections::HashMap::new(),
            },
            Repr::Map {
                body,
                raw,
                metadata,
            } => StaticConfig {
                body,
                raw,
                metadata,
            },
        })
    }
}

/// An enumeration of all supported endpoint types.
/// `#[serde(rename_all = "lowercase")]` ensures that the keys in the config (e.g., "kafka")
/// match the enum variants.
///
/// # Examples
///
/// Configuring a Fanout endpoint in YAML:
/// ```
/// use mq_bridge::models::{Endpoint, EndpointType};
///
/// let yaml = r#"
/// fanout:
///   - memory: { topic: "out1" }
///   - memory: { topic: "out2" }
/// "#;
///
/// let endpoint: Endpoint = serde_yaml_ng::from_str(yaml).unwrap();
/// if let EndpointType::Fanout(targets) = endpoint.endpoint_type {
///     assert_eq!(targets.len(), 2);
/// }
/// ```
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum EndpointType {
    Aws(AwsConfig),
    Kafka(KafkaConfig),
    Nats(NatsConfig),
    File(FileConfig),
    #[serde(rename = "object_store", alias = "objectstore", alias = "s3")]
    ObjectStore(ObjectStoreConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Static(StaticConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Ref(String),
    Memory(MemoryConfig),
    Sled(SledConfig),
    Amqp(AmqpConfig),
    MongoDb(MongoDbConfig),
    Mqtt(MqttConfig),
    Http(HttpConfig),
    WebSocket(WebSocketConfig),
    IbmMq(IbmMqConfig),
    ZeroMq(ZeroMqConfig),
    #[serde(rename = "redis_streams", alias = "redis")]
    RedisStreams(RedisStreamsConfig),
    Grpc(GrpcConfig),
    Sqlx(SqlxConfig),
    #[serde(rename = "clickhouse", alias = "click_house")]
    ClickHouse(ClickHouseConfig),
    #[serde(rename = "postgres_cdc", alias = "postgres-cdc")]
    PostgresCdc(PostgresCdcConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Fanout(Vec<Endpoint>),
    #[serde(rename = "stream_buffer")]
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    StreamBuffer(StreamBufferConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Switch(SwitchConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Response(ResponseConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Reader(Box<Endpoint>),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Request(RequestForwardConfig),
    #[cfg_attr(feature = "schema", schemars(extend("format" = "structural_endpoint")))]
    Custom {
        name: String,
        config: serde_json::Value,
    },
    #[default]
    Null,
}

impl EndpointType {
    pub fn name(&self) -> &'static str {
        match self {
            EndpointType::Aws(_) => "aws",
            EndpointType::Kafka(_) => "kafka",
            EndpointType::Nats(_) => "nats",
            EndpointType::File(_) => "file",
            EndpointType::ObjectStore(_) => "object_store",
            EndpointType::Static(_) => "static",
            EndpointType::Ref(_) => "ref",
            EndpointType::Memory(_) => "memory",
            EndpointType::Sled(_) => "sled",
            EndpointType::Amqp(_) => "amqp",
            EndpointType::MongoDb(_) => "mongodb",
            EndpointType::Mqtt(_) => "mqtt",
            EndpointType::Http(_) => "http",
            EndpointType::WebSocket(_) => "websocket",
            EndpointType::IbmMq(_) => "ibmmq",
            EndpointType::ZeroMq(_) => "zeromq",
            EndpointType::RedisStreams(_) => "redis_streams",
            EndpointType::Grpc(_) => "grpc",
            EndpointType::Sqlx(_) => "sqlx",
            EndpointType::ClickHouse(_) => "clickhouse",
            EndpointType::PostgresCdc(_) => "postgres_cdc",
            EndpointType::Fanout(_) => "fanout",
            EndpointType::StreamBuffer(_) => "stream_buffer",
            EndpointType::Switch(_) => "switch",
            EndpointType::Response(_) => "response",
            EndpointType::Reader(_) => "reader",
            EndpointType::Request(_) => "request",
            EndpointType::Custom { .. } => "custom",
            EndpointType::Null => "null",
        }
    }

    pub fn is_core(&self) -> bool {
        matches!(
            self,
            EndpointType::File(_)
                | EndpointType::Static(_)
                | EndpointType::Ref(_)
                | EndpointType::Memory(_)
                | EndpointType::Fanout(_)
                | EndpointType::StreamBuffer(_)
                | EndpointType::Switch(_)
                | EndpointType::Response(_)
                | EndpointType::Reader(_)
                | EndpointType::Request(_)
                | EndpointType::Custom { .. }
                | EndpointType::Null
        )
    }
}

/// AEAD cipher selection for [`EncryptionConfig`].
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CipherKind {
    /// XChaCha20-Poly1305 (default): 192-bit random nonce, safe at high message rates.
    #[default]
    Xchacha20poly1305,
    /// AES-256-GCM: 96-bit random nonce; prefer the default for very high volumes.
    Aes256gcm,
}

/// AEAD encryption settings, shared by the `encryption` middleware (per-message
/// payload encryption) and the at-rest `encryption` field of the file and
/// object_store endpoints. Requires the `encryption` feature.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    /// AEAD cipher. Defaults to `xchacha20poly1305`.
    #[serde(default)]
    pub cipher: CipherKind,
    /// Key identifier written into each envelope; selects the key when decrypting. Defaults to `default`.
    #[serde(default = "default_encryption_key_id")]
    pub key_id: String,
    /// Base64-encoded 32-byte key. Supports `${env:VAR}` to read it from the environment.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub key: String,
    /// Extra `key_id -> base64 key` entries accepted when decrypting (key rotation).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub decrypt_keys: HashMap<String, String>,
}

fn default_encryption_key_id() -> String {
    "default".to_string()
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            cipher: CipherKind::default(),
            key_id: default_encryption_key_id(),
            key: String::new(),
            decrypt_keys: HashMap::new(),
        }
    }
}

/// An enumeration of all supported middleware types.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Middleware {
    Deduplication(DeduplicationMiddleware),
    Metrics(MetricsMiddleware),
    Dlq(Box<DeadLetterQueueMiddleware>),
    Retry(RetryMiddleware),
    RandomPanic(RandomPanicMiddleware),
    Delay(DelayMiddleware),
    WeakJoin(WeakJoinMiddleware),
    Limiter(LimiterMiddleware),
    Buffer(BufferMiddleware),
    CookieJar(CookieJarMiddleware),
    Transform(TransformMiddleware),
    Encryption(EncryptionConfig),
    Custom {
        name: String,
        config: serde_json::Value,
    },
}

/// Deduplication middleware configuration.
///
/// Prevents duplicate messages from being processed using a Sled-backed database.
/// Messages are identified by their deduplication key and removed after the TTL expires.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DeduplicationMiddleware {
    /// Path to the Sled database directory.
    pub sled_path: String,
    /// Time-to-live for deduplication entries in seconds.
    pub ttl_seconds: u64,
}

/// Metrics middleware configuration.
///
/// Enables collection and reporting of message processing metrics such as throughput,
/// latency, and error rates. The presence of this middleware in the configuration
/// enables metrics collection for the endpoint.
///
/// Metrics are typically exported via Prometheus or similar monitoring systems.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MetricsMiddleware {}

/// Dead-Letter Queue (DLQ) middleware configuration.
///
/// Routes failed messages to a designated endpoint for later analysis and recovery.
/// It is recommended to pair this with the Retry middleware to avoid message loss.
///
/// Failed messages are sent to the configured endpoint when they are exhausted after retry attempts.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DeadLetterQueueMiddleware {
    /// The endpoint to send failed messages to.
    pub endpoint: Endpoint,
}

/// Retry middleware configuration.
///
/// Implements exponential backoff retry logic for failed message processing.
/// Failed messages are automatically retried with increasing delays between attempts.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RetryMiddleware {
    /// Maximum number of retry attempts. Defaults to 3.
    #[serde(default = "default_retry_attempts")]
    pub max_attempts: usize,
    /// Initial retry interval in milliseconds. Defaults to 100ms.
    #[serde(default = "default_initial_interval_ms")]
    pub initial_interval_ms: u64,
    /// Maximum retry interval in milliseconds. Defaults to 5000ms.
    #[serde(default = "default_max_interval_ms")]
    pub max_interval_ms: u64,
    /// Multiplier for exponential backoff. Defaults to 2.0.
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
}

/// Delay middleware configuration.
///
/// Introduces a fixed delay before processing each message.
/// Useful for rate limiting, testing, or allowing time for dependent systems to become ready.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DelayMiddleware {
    /// Delay duration in milliseconds.
    pub delay_ms: u64,
}

/// Throughput limiter middleware configuration.
///
/// Applies a best-effort pacing delay so an endpoint does not exceed the configured
/// message rate. For batch operations the limiter accounts for the number of messages
/// in the batch, not just the batch count.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LimiterMiddleware {
    /// Target throughput in messages per second. Must be greater than zero.
    pub messages_per_second: f64,
}

/// Publisher-side buffer middleware configuration.
///
/// Buffers outbound messages briefly so multiple single-message sends can be
/// forwarded as one `send_batch` call to the wrapped publisher.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BufferMiddleware {
    /// Maximum number of messages to accumulate before flushing immediately.
    pub max_messages: usize,
    /// Maximum time to wait before flushing a non-full buffer.
    pub max_delay_ms: u64,
}

/// Cookie/session jar middleware configuration.
///
/// Optimized for HTTP by default: it can read `cookie` and `set-cookie` metadata,
/// persist session cookies, and inject them into later outgoing requests.
///
/// The middleware can also capture arbitrary metadata values into the same session store
/// and optionally expose stored values back into message metadata.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CookieJarMiddleware {
    /// Optional shared scope name. When set, middleware instances using the same scope
    /// share one session store across endpoints/routes in the process.
    #[serde(default)]
    pub shared_scope: Option<String>,
    /// Metadata key used to read/write HTTP Cookie headers. Defaults to `cookie`.
    #[serde(default = "default_cookie_metadata_key")]
    pub cookie_metadata_key: String,
    /// Metadata key used to read HTTP Set-Cookie responses. Defaults to `set-cookie`.
    #[serde(default = "default_set_cookie_metadata_key")]
    pub set_cookie_metadata_key: String,
    /// Additional metadata keys to persist into the session value store.
    #[serde(default)]
    pub capture_metadata_keys: Vec<String>,
    /// Optional metadata prefix used to export stored values back onto each message.
    ///
    /// Exported keys use `PREFIXcookie.<name>` for cookies and `PREFIXvalue.<name>` for
    /// captured generic values.
    #[serde(default)]
    pub export_metadata_prefix: Option<String>,
    /// Optional mapping of outgoing metadata keys to stored session value names.
    ///
    /// Example: `{ "authorization": "access_token" }` copies the stored value
    /// `access_token` into outgoing metadata key `authorization` when not already present.
    #[serde(default)]
    pub inject_metadata: HashMap<String, String>,
}

impl Default for CookieJarMiddleware {
    fn default() -> Self {
        Self {
            shared_scope: None,
            cookie_metadata_key: default_cookie_metadata_key(),
            set_cookie_metadata_key: default_set_cookie_metadata_key(),
            capture_metadata_keys: Vec::new(),
            export_metadata_prefix: None,
            inject_metadata: HashMap::new(),
        }
    }
}

/// Weak Join middleware configuration.
///
/// Correlates messages by a metadata key and joins them within a timeout window.
/// Count mode (default) waits for `expected_count` messages and emits a JSON array.
/// Branch mode (set `branch_by`) waits for named branches and emits a branch-keyed object.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WeakJoinMiddleware {
    /// The metadata key to group messages by (e.g., "correlation_id").
    pub group_by: String,
    /// The number of messages (count mode) or distinct branches (branch mode) to wait for.
    pub expected_count: usize,
    /// Timeout in milliseconds.
    pub timeout_ms: u64,
    /// Metadata key naming each message's branch; enables branch mode when set.
    #[serde(default)]
    pub branch_by: Option<String>,
    /// Branch names that must all arrive before firing (branch mode; overrides expected_count).
    #[serde(default)]
    pub required: Vec<String>,
    /// What to do with an incomplete group when the timeout expires.
    #[serde(default)]
    pub on_timeout: WeakJoinTimeout,
}

/// Action taken on an incomplete weak-join group when its timeout expires.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WeakJoinTimeout {
    /// Emit the partial join (current behavior).
    #[default]
    Fire,
    /// Drop the incomplete group without emitting.
    Discard,
}

/// JSON transform middleware configuration.
///
/// Reshapes JSON payloads declaratively, in two stages over a single parse: field
/// `mapping` (rename/move/nest), then `schema` (type coercion, defaults, validation).
/// Either stage may be omitted; with neither configured the message passes through
/// untouched and is never parsed.
///
/// On an output endpoint a rejected message becomes a non-retryable failure, so a `dlq`
/// middleware listed *after* this one captures it (publisher middlewares are wrapped in
/// list order, so the last entry is the outermost layer). On an input endpoint a rejected
/// message is dropped from the batch and acknowledged, which is how invalid input is kept
/// out of the route.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TransformMiddleware {
    /// Output field name -> source path (e.g. `firstName: "$.first_name"`). Dots nest the output.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub mapping: HashMap<String, MappingRule>,
    /// Inline JSON Schema subset (type, properties, required, default, items, nullable, enum).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Path to a JSON Schema file. Read once at startup; never re-read per message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_file: Option<String>,
    /// Coerce safely convertible types (e.g. `"42"` -> `42`) instead of rejecting. Defaults to true.
    #[serde(default = "default_true")]
    pub coerce: bool,
    /// Insert `default` values from the schema for missing fields. Defaults to true.
    #[serde(default = "default_true")]
    pub apply_defaults: bool,
    /// What to do with a message that fails to transform. Defaults to `reject`.
    #[serde(default)]
    pub on_error: TransformErrorPolicy,
}

// Hand-written rather than derived: `coerce` and `apply_defaults` default to *true*, which
// a derived `Default` would silently turn into `false`. That would make
// `TransformMiddleware { ..Default::default() }` in Rust behave differently from the same
// config parsed from YAML.
impl Default for TransformMiddleware {
    fn default() -> Self {
        Self {
            mapping: HashMap::new(),
            schema: None,
            schema_file: None,
            coerce: default_true(),
            apply_defaults: default_true(),
            on_error: TransformErrorPolicy::default(),
        }
    }
}

/// How one output field is produced from the input document.
///
/// Either a bare path string (`"$.first_name"`) or an object with a `path` plus an
/// optional `default` and `required` flag.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum MappingRule {
    /// Shorthand: just the source path.
    Path(String),
    /// Full form with a fallback value and/or a presence requirement.
    Detailed(DetailedMappingRule),
}

/// Full mapping form with a fallback value and/or a presence requirement.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DetailedMappingRule {
    /// Source path in the input document (e.g. `$.user.id`, `user.id`, `$.items[0]`).
    pub path: String,
    /// Value used when the source path is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Reject the message when the source path is absent and no `default` is set.
    #[serde(default)]
    pub required: bool,
}

impl MappingRule {
    /// The source path this rule reads from.
    pub fn path(&self) -> &str {
        match self {
            MappingRule::Path(p) => p,
            MappingRule::Detailed(d) => &d.path,
        }
    }
}

/// Action taken on a message that fails to transform.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TransformErrorPolicy {
    /// Reject the message: non-retryable failure on output, dropped from the batch on input.
    #[default]
    Reject,
    /// Forward the original payload unchanged with the error recorded in metadata.
    PassThrough,
}

/// Fault injection modes for testing error handling and recovery mechanisms.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FaultMode {
    /// Trigger a thread panic.
    #[default]
    Panic,
    /// Simulate a connection/network error (retryable).
    Disconnect,
    /// Simulate a timeout error (retryable).
    Timeout,
    /// Simulate a JSON format error (non-retryable).
    JsonFormatError,
    /// Return a negative acknowledgement (for handlers).
    Nack,
}

impl std::fmt::Display for FaultMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultMode::Panic => write!(f, "panic"),
            FaultMode::Disconnect => write!(f, "disconnect"),
            FaultMode::Timeout => write!(f, "timeout"),
            FaultMode::JsonFormatError => write!(f, "json_format_error"),
            FaultMode::Nack => write!(f, "nack"),
        }
    }
}

/// Middleware for fault injection testing.
///
/// Allows testing error handling and recovery mechanisms by injecting faults
/// at specific points in the message processing pipeline.
///
/// # Examples
///
/// ```yaml
/// random_panic:
///   mode: panic
///   trigger_on_message: 3  # Trigger on the 3rd message
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RandomPanicMiddleware {
    /// The type of fault to inject.
    #[serde(default)]
    pub mode: FaultMode,
    /// Trigger the fault on the Nth message (1-indexed). None = trigger on every message.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    #[serde(default)]
    pub trigger_on_message: Option<usize>,
    /// Enable/disable the fault injection without removing the configuration.
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip, default = "default_atomic_usize_arc")]
    #[cfg_attr(feature = "schema", schemars(skip))]
    pub message_count: Arc<AtomicUsize>,
}

fn default_true() -> bool {
    true
}

fn default_atomic_usize_arc() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn deserialize_null_as_false<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<bool>::deserialize(deserializer)?;
    Ok(opt.unwrap_or(false))
}

// --- AWS Specific Configuration ---
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AwsConfig {
    /// The SQS queue URL. Required for Consumer. Optional for Publisher if `topic_arn` is set. If it contains userinfo, it will be treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub queue_url: Option<String>,
    /// (Publisher only) The SNS topic ARN.
    pub topic_arn: Option<String>,
    /// AWS Region (e.g., "us-east-1").
    pub region: Option<String>,
    /// Custom endpoint URL (e.g., for LocalStack).
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub endpoint_url: Option<String>,
    /// AWS Access Key ID.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub access_key: Option<String>,
    /// AWS Secret Access Key.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub secret_key: Option<String>,
    /// AWS Session Token.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub session_token: Option<String>,
    /// (Consumer only) Maximum number of messages to receive in a batch (1-10).
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = 10)))]
    pub max_messages: Option<i32>,
    /// (Consumer only) Wait time for long polling in seconds (0-20).
    #[cfg_attr(feature = "schema", schemars(range(min = 0, max = 20)))]
    pub wait_time_seconds: Option<i32>,
    /// Use binary payloads in SQS/SNS messages.
    #[serde(default)]
    pub binary_payload_mode: bool,
}

impl AwsConfig {
    /// Creates a new AWS configuration with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_queue_url(mut self, queue_url: impl Into<String>) -> Self {
        self.queue_url = Some(queue_url.into());
        self
    }

    pub fn with_topic_arn(mut self, topic_arn: impl Into<String>) -> Self {
        self.topic_arn = Some(topic_arn.into());
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_endpoint_url(mut self, endpoint_url: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint_url.into());
        self
    }

    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = Some(access_key.into());
        self.secret_key = Some(secret_key.into());
        self
    }
}

// --- Kafka Specific Configuration ---

/// General Kafka connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    /// Comma-separated list of Kafka broker URLs. If it contains userinfo, it will be treated as a secret.
    #[serde(alias = "brokers")]
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// The Kafka topic to produce to or consume from.
    pub topic: Option<String>,
    /// Optional username for SASL authentication.
    pub username: Option<String>,
    /// Optional password for SASL authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub password: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// (Consumer only) Consumer group ID.
    /// If not provided, the consumer acts in **Subscriber mode**: it generates a unique, ephemeral group ID and starts consuming from the latest offset.
    pub group_id: Option<String>,
    /// (Publisher only) If true, do not wait for an acknowledgement when sending to broker. Defaults to false.
    #[serde(default)]
    pub delayed_ack: bool,
    /// (Publisher only) Additional librdkafka producer configuration options (key-value pairs).
    #[serde(default)]
    pub producer_options: Option<Vec<(String, String)>>,
    /// (Consumer only) Additional librdkafka consumer configuration options (key-value pairs).
    #[serde(default)]
    pub consumer_options: Option<Vec<(String, String)>>,
    /// (Publisher only) Share one producer per connection (default: true); false gives a dedicated producer.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(default = "default_shared_schema"))]
    pub shared: Option<bool>,
    /// (Publisher only) Partition count used when auto-creating the topic (default: 6).
    /// Higher values raise write/consume parallelism; ordering is only guaranteed per
    /// partition key (message_id), not across the whole topic. Ignored if the topic exists.
    #[serde(default)]
    #[cfg_attr(
        feature = "schema",
        schemars(default = "default_kafka_partitions_schema", range(min = 1))
    )]
    pub partitions: Option<i32>,
    /// (Publisher only) Name of a metadata field whose value is used as the Kafka record
    /// key (drives partitioning/ordering). Unset, or absent on a given message, falls back
    /// to the message id. Default unset.
    #[serde(default)]
    pub partition_key: Option<String>,
}

impl KafkaConfig {
    /// Creates a new Kafka configuration with the specified broker URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_producer_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let options = self.producer_options.get_or_insert_with(Vec::new);
        options.push((key.into(), value.into()));
        self
    }

    pub fn with_consumer_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let options = self.consumer_options.get_or_insert_with(Vec::new);
        options.push((key.into(), value.into()));
        self
    }
}

// --- Sled Specific Configuration ---

/// General Sled database configuration
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SledConfig {
    /// Path to the Sled database directory.
    pub path: String,
    /// The tree name to use as a queue. Defaults to "default".
    pub tree: Option<String>,
    /// (Consumer only) If true, start reading from the beginning of the tree.
    #[serde(default)]
    pub read_from_start: bool,
    /// (Consumer only) If true, delete messages after processing (Queue mode).
    #[serde(default)]
    pub delete_after_read: bool,
}

impl SledConfig {
    /// Creates a new Sled configuration with the specified database path.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    pub fn with_tree(mut self, tree: impl Into<String>) -> Self {
        self.tree = Some(tree.into());
        self
    }

    pub fn with_read_from_start(mut self, read_from_start: bool) -> Self {
        self.read_from_start = read_from_start;
        self
    }
}

/// Format for messages written to or read from a file.
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// The full `CanonicalMessage` is serialized to JSON. Payload is a byte array.
    #[default]
    Normal,
    /// The full `CanonicalMessage` is serialized to JSON. Payload is rendered as a JSON value if possible.
    Json,
    /// The full `CanonicalMessage` is serialized to JSON. Payload is rendered as a string if possible.
    Text,
    /// The raw payload of the message is written. For consumers, the line is read as raw bytes.
    Raw,
    /// CSV rows mapped to/from JSON objects (string values only). The first row is the header/schema.
    Csv,
}

/// Compression algorithm. Used for at-rest batches (file, object_store) and for HTTP
/// body compression (http, clickhouse). Orthogonal to `format`.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// No compression (default).
    #[default]
    None,
    /// gzip; each batch is a self-contained member, so files stay readable with `zcat`.
    Gzip,
    /// lz4 frame format; each batch is a self-contained frame (`lz4 -d` compatible).
    Lz4,
    /// zstd; each batch is a self-contained frame, concatenated frames decode as one
    /// stream (`zstd -d` compatible). Better ratio than lz4, still fast.
    Zstd,
}

// --- File Specific Configuration ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FileConfig {
    /// Path to the file.
    pub path: String,
    /// Optional delimiter for messages. Defaults to newline ("\n").
    /// Can be a string or a hex sequence (e.g. "0x00").
    /// Currently only single-byte delimiters are supported.
    pub delimiter: Option<String>,
    /// The consumption mode. If not specified, defaults to `consume`.
    /// For publishers, this setting is ignored.
    #[serde(flatten, default)]
    pub mode: Option<FileConsumerMode>,
    /// The format for writing messages to the file (Publisher) or interpreting them (Consumer). Defaults to `normal`.
    #[serde(default)]
    pub format: FileFormat,
    /// Per-batch compression (`none`, `gzip`, `lz4`, `zstd`). Requires the `compression` feature; consume mode only.
    #[serde(default)]
    pub compression: Compression,
    /// At-rest AEAD encryption applied after compression. Requires the `encryption` feature; consume mode only.
    #[serde(default)]
    pub encryption: Option<EncryptionConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum FileConsumerMode {
    /// **Queue Mode**: Standard point-to-point consumption. Reads from the start
    /// of the file. If `delete` is true, processed lines are physically removed
    /// from the file once they are successfully acknowledged.
    Consume {
        /// If true, processed lines are physically removed from the file once
        /// they are successfully acknowledged.
        #[serde(default)]
        delete: bool,
    },
    /// **Broadcast Mode**: Pub-sub style consumption. Tails the file by starting
    /// at the current end. If `delete` is true, lines are removed only after
    /// all local application subscribers for this specific file have acknowledged them.
    Subscribe {
        /// If true, lines are removed only after all local application
        /// subscribers for this file have acknowledged them.
        #[serde(default)]
        delete: bool,
    },
    /// **Persistent Mode**: Consumption with external offset tracking.
    /// Saves the last read byte position to a `.offset` file identified by the `group_id`.
    /// This allows the consumer to resume exactly where it left off after a restart
    /// without deleting data or requiring the bridge to stay running.
    GroupSubscribe {
        /// The consumer group ID that is used for offset tracking. Should be unique.
        group_id: String,
        /// If true, starts reading from the end of the file if no offset is stored.
        /// If false, starts reading from the beginning.
        #[serde(default)]
        read_from_tail: bool,
    },
}

impl Default for FileConsumerMode {
    fn default() -> Self {
        Self::Consume { delete: false }
    }
}

impl FileConfig {
    /// Creates a new File configuration with the specified path.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            mode: Some(FileConsumerMode::default()),
            delimiter: None,
            format: FileFormat::default(),
            compression: Compression::default(),
            encryption: None,
        }
    }

    pub fn with_mode(mut self, mode: FileConsumerMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Returns the effective consumer mode, defaulting to `Consume` if not set.
    pub fn effective_mode(&self) -> FileConsumerMode {
        self.mode.clone().unwrap_or_default()
    }
}

// --- Object Store (S3/GCS/Azure) Specific Configuration ---

/// Configuration for a cloud object-store endpoint (S3, GCS, Azure Blob, R2, ...).
///
/// As a **sink**, each flushed batch is written as one immutable object under `url`,
/// named `<prefix>/[YYYY/MM/DD/]<uuidv7>.<ext>`. As a **source**, objects under `url`
/// are listed in key order, fetched, split by `delimiter`, and emitted as messages;
/// progress is persisted to `checkpoint_store` (the last processed object key) so a
/// restart resumes without re-emitting. Objects are never mutated or deleted in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ObjectStoreConfig {
    /// Object-store URL, e.g. `s3://bucket/prefix`, `gs://bucket/prefix`,
    /// `az://account/container/prefix`. Credentials are resolved from the environment by
    /// the `object_store` crate (same mechanism as the checkpoint backend); R2 uses
    /// `s3://` plus a custom `AWS_ENDPOINT_URL`.
    pub url: String,
    /// Record encoding within an object, shared with the file endpoint. Defaults to
    /// `normal` (one JSON `CanonicalMessage` per line). CSV is supported for sources only.
    #[serde(default)]
    pub format: FileFormat,
    /// Record delimiter within an object. Defaults to newline ("\n"). Can be a string or a
    /// hex sequence (e.g. "0x00").
    pub delimiter: Option<String>,
    /// (Source only) Durable resume store URL recording the last processed object key, e.g.
    /// `file:///var/lib/mqb/obj.json`, `s3://bucket/cursors`, or `postgres://…`. Without it
    /// every restart re-lists and re-emits all objects.
    pub checkpoint_store: Option<String>,
    /// (Source only) Cursor id namespacing the checkpoint key; enables durable resume.
    pub cursor_id: Option<String>,
    /// (Source only) Idle poll interval in milliseconds when no new objects are found.
    /// Defaults to 1000.
    pub polling_interval_ms: Option<u64>,
    /// (Source only) Maximum size in bytes of a single object to fetch into memory. An object
    /// larger than this fails the read (surfaced as a consumer error) instead of being
    /// buffered whole. Unset means no limit (the whole object is materialized).
    pub max_object_bytes: Option<u64>,
    /// (Sink only) Prepend a `YYYY/MM/DD/` path (write time, UTC) to each object key. Purely
    /// for readability / lifecycle rules — the uuidv7 name already sorts by time. Default true.
    #[serde(default = "default_true")]
    pub date_partition: bool,
    /// (Sink only) Extension for written objects, without the dot. Defaults to a value derived
    /// from `format`, `compression` and `encryption` (e.g. `jsonl`, `csv`, `bin`, `jsonl.gz`,
    /// `jsonl.lz4`, `jsonl.gz.enc`); encrypted objects get a trailing `.enc` since they are
    /// ciphertext, not a directly decompressible `.gz`.
    pub extension: Option<String>,
    /// Whole-object compression (`none`, `gzip`, `lz4`, `zstd`). Requires the `compression` feature.
    #[serde(default)]
    pub compression: Compression,
    /// At-rest AEAD encryption applied after compression. Requires the `encryption` feature.
    #[serde(default)]
    pub encryption: Option<EncryptionConfig>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            format: FileFormat::default(),
            delimiter: None,
            checkpoint_store: None,
            cursor_id: None,
            polling_interval_ms: None,
            max_object_bytes: None,
            date_partition: true,
            extension: None,
            compression: Compression::default(),
            encryption: None,
        }
    }
}

// --- NATS Specific Configuration ---

/// General NATS connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// Comma-separated list of NATS server URLs (e.g., "nats://localhost:4222,nats://localhost:4223"). If it contains userinfo, it will be treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// The NATS subject to publish to or subscribe to. If a stream is
    /// auto-created, it's scoped to `{stream}.>`, so prefix accordingly.
    pub subject: Option<String>,
    /// The JetStream stream name. Required for Consumers, even with
    /// `no_jetstream: true` (unused there, but still validated).
    pub stream: Option<String>,
    /// Optional username for authentication.
    pub username: Option<String>,
    /// Optional password for authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub password: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Optional token for authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub token: Option<String>,
    /// (Publisher only) If true, the publisher uses the request-reply pattern.
    /// It sends a request and waits for a response (using `core_client.request_with_headers()`).
    /// Defaults to false.
    #[serde(default)]
    pub request_reply: bool,
    /// (Publisher only) Timeout for request-reply operations in milliseconds. Defaults to 30000ms.
    pub request_timeout_ms: Option<u64>,
    /// (Publisher only) If true, do not wait for an acknowledgement when sending to broker. Defaults to false.
    #[serde(default)]
    pub delayed_ack: bool,
    /// (Publisher only, JetStream) If true, publish a `Nats-Msg-Id` header (from the message id) so
    /// JetStream deduplicates redeliveries within the stream's duplicate window. Defaults to false.
    #[serde(default)]
    pub deduplicate: bool,
    /// If no_jetstream: true, use Core NATS (fire-and-forget) instead of JetStream. Defaults to false.
    #[serde(default)]
    pub no_jetstream: bool,
    /// (Consumer only) If true, use ephemeral **Subscriber mode**. Defaults to false (durable consumer).
    #[serde(default)]
    pub subscriber_mode: bool,
    /// (Publisher only) Maximum number of messages in the stream (if created by the bridge). Defaults to 1,000,000.
    pub stream_max_messages: Option<i64>,
    /// (Consumer only) The delivery policy for the consumer. Defaults to "all".
    pub deliver_policy: Option<NatsDeliverPolicy>,
    /// (Publisher only) Maximum total bytes in the stream (if created by the bridge). Defaults to 1GB.
    pub stream_max_bytes: Option<i64>,
    /// (Consumer only) Number of messages to prefetch from the consumer. Defaults to 10000.
    pub prefetch_count: Option<usize>,
    /// Share one NATS client per connection (default: true); false forces a dedicated connection.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(default = "default_shared_schema"))]
    pub shared: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum NatsDeliverPolicy {
    #[default]
    All,
    Last,
    New,
    LastPerSubject,
}

impl NatsConfig {
    /// Creates a new NATS configuration with the specified server URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    pub fn with_deliver_policy(mut self, policy: NatsDeliverPolicy) -> Self {
        self.deliver_policy = Some(policy);
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

#[derive(Debug, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(transform = memory_config_schema_transform))]
#[serde(deny_unknown_fields)]
pub struct MemoryConfig {
    /// The topic name or transport URL. Can be:
    /// - Simple name: "my-topic" (defaults to memory://my-topic)
    /// - Memory URL: "memory://my-topic"
    /// - IPC URL: "ipc://my-queue" or "ipc:///path/to/socket"
    /// - Unix socket: "unix:///path/to/socket" (Unix only)
    /// - Named pipe: "pipe://my-pipe" (Windows only)
    ///
    /// Either `topic` or `url` can be specified (they are serde aliases).
    #[serde(default, skip_serializing_if = "String::is_empty", alias = "url")]
    pub topic: String,
    /// Transport URL (serde alias for `topic`). Use either `topic` or `url`.
    #[serde(skip)]
    pub url: Option<String>,
    /// The capacity of the channel. Defaults to 100.
    pub capacity: Option<usize>,
    /// (Publisher only) If true, send() waits for a response.
    #[serde(default)]
    pub request_reply: bool,
    /// (Publisher only) Timeout for request-reply operations in milliseconds. Defaults to 30000ms.
    pub request_timeout_ms: Option<u64>,
    /// (Consumer only) If true, act as a **Subscriber** (fan-out). Defaults to false (queue).
    #[serde(default)]
    pub subscribe_mode: bool,
    /// (Consumer only) If true, enables NACK support (re-queuing), which requires cloning messages.
    /// Defaults to false for memory:// transports, automatically true for IPC transports (ipc://, unix://, pipe://).
    #[serde(default)]
    pub enable_nack: bool,
    #[serde(skip)]
    pub enable_nack_overridden: bool,
}

impl MemoryConfig {
    pub fn new(topic: impl Into<String>, capacity: Option<usize>) -> Self {
        Self {
            topic: topic.into(),
            url: None,
            capacity,
            ..Default::default()
        }
    }

    pub fn new_with_url(url: impl Into<String>, capacity: Option<usize>) -> Self {
        let url = url.into();
        Self {
            topic: url.clone(),
            url: Some(url),
            capacity,
            ..Default::default()
        }
    }

    pub fn with_subscribe(self, subscribe_mode: bool) -> Self {
        Self {
            subscribe_mode,
            ..self
        }
    }

    pub fn with_request_reply(mut self, request_reply: bool) -> Self {
        self.request_reply = request_reply;
        self
    }

    /// Gets the effective transport identifier.
    /// If topic contains ://, it's treated as a URL, otherwise as memory://topic.
    pub fn get_transport_identifier(&self) -> anyhow::Result<String> {
        let identifier = if !self.topic.is_empty() {
            &self.topic
        } else if let Some(url) = self.url.as_ref().filter(|url| !url.is_empty()) {
            url
        } else {
            return Err(anyhow::anyhow!(
                "MemoryConfig: 'topic' (or 'url' alias) is required."
            ));
        };

        // If topic doesn't contain ://, treat it as memory://topic for backward compatibility
        if identifier.contains("://") {
            Ok(identifier.clone())
        } else {
            Ok(format!("memory://{}", identifier))
        }
    }

    /// Check if the transport URL scheme suggests IPC (inter-process communication).
    /// IPC transports should enable nack by default for reliability.
    pub fn is_ipc_transport(&self) -> bool {
        if let Ok(identifier) = self.get_transport_identifier() {
            identifier.starts_with("ipc://")
                || identifier.starts_with("unix://")
                || identifier.starts_with("pipe://")
        } else {
            false
        }
    }

    /// Apply smart defaults based on the transport type.
    /// For IPC transports, enable_nack defaults to true for reliability.
    pub fn with_smart_defaults(mut self) -> Self {
        if !self.enable_nack_overridden && self.is_ipc_transport() {
            self.enable_nack = true;
        }
        self
    }
}

impl<'de> Deserialize<'de> for MemoryConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct MemoryConfigSerde {
            #[serde(default)]
            topic: String,
            #[serde(default)]
            url: Option<String>,
            capacity: Option<usize>,
            #[serde(default)]
            request_reply: bool,
            request_timeout_ms: Option<u64>,
            #[serde(default)]
            subscribe_mode: bool,
            #[serde(default)]
            enable_nack: Option<bool>,
        }

        let raw = MemoryConfigSerde::deserialize(deserializer)?;
        if raw.topic.is_empty() && raw.url.as_deref().map_or(true, str::is_empty) {
            return Err(serde::de::Error::custom(
                "MemoryConfig: 'topic' (or 'url' alias) is required.",
            ));
        }
        let topic = if raw.topic.is_empty() {
            raw.url.clone().unwrap_or_default()
        } else {
            raw.topic
        };
        Ok(Self {
            topic,
            url: raw.url,
            capacity: raw.capacity,
            request_reply: raw.request_reply,
            request_timeout_ms: raw.request_timeout_ms,
            subscribe_mode: raw.subscribe_mode,
            enable_nack: raw.enable_nack.unwrap_or(false),
            enable_nack_overridden: raw.enable_nack.is_some(),
        })
    }
}

#[cfg(feature = "schema")]
fn memory_config_schema_transform(schema: &mut schemars::Schema) {
    let Some(schema_obj) = schema.as_object_mut() else {
        return;
    };

    let Some(properties) = schema_obj
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    properties.insert(
        "url".to_string(),
        serde_json::json!({
            "description": "Alias for `topic`. Use either `topic` or `url`.",
            "type": "string",
            "minLength": 1
        }),
    );

    // Mirror the runtime check (see `MemoryConfig::deserialize`): an empty
    // `topic`/`url` is rejected, so the schema must require a non-empty value.
    if let Some(topic) = properties
        .get_mut("topic")
        .and_then(serde_json::Value::as_object_mut)
    {
        topic.insert("minLength".to_string(), serde_json::json!(1));
    }

    schema_obj.insert(
        "anyOf".to_string(),
        serde_json::json!([
            { "required": ["topic"] },
            { "required": ["url"] }
        ]),
    );
}

#[cfg(feature = "schema")]
fn route_schema_transform(schema: &mut schemars::Schema) {
    let Some(properties) = schema
        .as_object_mut()
        .and_then(|schema_obj| schema_obj.get_mut("properties"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    let Some(allow_fault_injection) = properties
        .get_mut("allow_fault_injection")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    allow_fault_injection.insert("default".to_string(), serde_json::Value::Bool(false));
}

/// Configuration for the correlated in-process stream response buffer.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct StreamBufferConfig {
    /// Shared buffer topic used by both the publisher and correlated consumers.
    pub topic: String,
    /// Consumer-only correlation id partition to read from.
    ///
    /// Leave this unset for the publisher endpoint configured in
    /// `HttpConfig::stream_response_to`. Set it on consumers so a reader only
    /// receives messages belonging to one request or response stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    /// Capacity of each correlation partition. Defaults to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<usize>,
}

impl StreamBufferConfig {
    /// Creates a `stream_buffer` config for the given topic.
    ///
    /// Add `with_correlation_id` when constructing a consumer for one stream.
    /// Leave the correlation id unset when constructing the publisher buffer
    /// used by `HttpConfig::stream_response_to`.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            ..Default::default()
        }
    }

    /// Selects the response stream partition that a consumer should read.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Sets the per-correlation partition capacity.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }
}

// --- AMQP Specific Configuration ---

/// General AMQP connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AmqpConfig {
    /// AMQP connection URI. The `lapin` client connects to a single host specified in the URI. If it contains userinfo, it will be treated as a secret.
    /// For high availability, provide the address of a load balancer or use DNS resolution
    /// that points to multiple brokers. Example: "amqp://localhost:5672/vhost".
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// The AMQP queue name.
    pub queue: Option<String>,
    /// (Consumer only) If true, act as a **Subscriber** (fan-out). Defaults to false.
    #[serde(default)]
    pub subscribe_mode: bool,
    /// Optional username for authentication.
    pub username: Option<String>,
    /// Optional password for authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub password: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// The exchange to publish to or bind the queue to.
    pub exchange: Option<String>,
    /// (Consumer only) Number of messages to prefetch. Defaults to 100.
    pub prefetch_count: Option<u16>,
    /// If true, declare queues as non-durable (transient). Defaults to false. Affects both Consumer (queue durability) and Publisher (message persistence).
    #[serde(default)]
    pub no_persistence: bool,
    /// (Publisher only) If true, do not attempt to declare the queue. Assumes the queue already exists. Defaults to false.
    #[serde(default)]
    pub no_declare_queue: bool,
    /// (Publisher only) If true, do not wait for an acknowledgement when sending to broker. Defaults to false.
    #[serde(default)]
    pub delayed_ack: bool,
}

impl AmqpConfig {
    /// Creates a new AMQP configuration with the specified connection URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn with_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = Some(exchange.into());
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

/// MongoDB message storage format.
///
/// Determines how messages are stored and retrieved from MongoDB collections.
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum MongoDbFormat {
    #[default]
    Normal,
    Json,
    Text,
    Raw,
}

/// How a MongoDB endpoint consumes a collection. One intent-named selector — the bridge picks the
/// underlying mechanism (change stream vs. polling) automatically. Defaults to `consumer`.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MongoConsume {
    /// **Queue** — competing consumers: claim, process, delete, so each document goes to exactly one
    /// reader. Default. Destructive and ~5x slower than `capture_all`; for jobs, not bulk reads.
    #[default]
    Consumer,
    /// **Queue, ephemeral** — receive only new messages, no durable position (fan-out subscriber).
    Subscriber,
    /// **Watch existing collection** — capture changes from now on (insert/update/delete), resuming
    /// under `cursor_id`. Reads an existing collection non-destructively; never ends on drain.
    CaptureNew,
    /// **Watch existing collection** — read the existing documents first, then capture changes.
    /// Non-destructive and the fastest read mode; use this for bulk reads and ETL.
    CaptureAll,
}

// --- MongoDB Specific Configuration ---

/// General MongoDB connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MongoDbConfig {
    /// MongoDB connection string URI. Can contain a comma-separated list of hosts for a replica set. If it contains userinfo, it will be treated as a secret.
    /// Credentials provided via the separate `username` and `password` fields take precedence over any credentials embedded in the URL.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// The MongoDB collection name.
    pub collection: Option<String>,
    /// Optional username. Takes precedence over any credentials embedded in the `url`.
    /// Use embedded URL credentials for simple one-off connections but prefer explicit username/password fields (or environment-sourced secrets) for clarity and secret management in production.
    pub username: Option<String>,
    /// Optional password. Takes precedence over any credentials embedded in the `url`.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    /// Use embedded URL credentials for simple one-off connections but prefer explicit username/password fields (or environment-sourced secrets) for clarity and secret management in production.
    pub password: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// The database name.
    pub database: String,
    /// (Consumer only) Polling interval in milliseconds for the consumer (when not using Change Streams). Defaults to 100ms.
    pub polling_interval_ms: Option<u64>,
    /// (Publisher only) Polling interval in milliseconds for the publisher when waiting for a reply. Defaults to 50ms.
    pub reply_polling_ms: Option<u64>,
    /// (Publisher only) If true, the publisher will wait for a response in a dedicated collection. Defaults to false.
    #[serde(default)]
    pub request_reply: bool,
    /// (Consumer only) How to consume the collection: `consumer` (default, competing-consumers work
    /// queue — destructive and ~5x slower), `subscriber` (ephemeral queue), `capture_new` (watch an
    /// existing collection for changes), or `capture_all` (read existing documents first, then watch
    /// for changes — use this for single-reader bulk reads and ETL). The bridge selects the
    /// underlying mechanism automatically. If unset, the deprecated `change_stream` boolean is
    /// honored for backward compatibility.
    pub consume: Option<MongoConsume>,
    /// (Consumer only) **Deprecated** — use `consume: subscriber`. Kept for compatibility.
    #[serde(default)]
    pub change_stream: bool,
    /// (Consumer only) Where to persist the resume cursor in `capture_new`/`capture_all` mode. A URL
    /// selects the backend; a bare name (or `/name`) reuses the **source** database with that name:
    /// - absent → source database, collection `mqb_cursors_<source_collection>` (auto-unique)
    /// - `/my_cursors` → source database, collection `my_cursors`
    /// - `file:///var/lib/mqb/cursors.json` → local JSON file (read-only / write-restricted sources)
    /// - `mongodb://host/db/collection` → external MongoDB collection (collection optional)
    /// - `postgres://user@host/db/table` or `mysql://host/db/table` → external SQL table (table optional)
    /// - `s3://bucket/prefix` (also `gs://`, `az://`, `abfs://`) → cloud object store; creds via env
    ///
    /// When no collection/table is named, it defaults to `mqb_cursors_<source_collection>`.
    /// May embed connection credentials, so it is treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub checkpoint_store: Option<String>,
    /// (Publisher only) Timeout for request-reply operations in milliseconds. Defaults to 30000ms.
    pub request_timeout_ms: Option<u64>,
    /// (Publisher only) TTL in seconds for documents created by the publisher. If set, a TTL index is created.
    pub ttl_seconds: Option<u64>,
    /// (Publisher only) If set, creates a capped collection with this size in bytes.
    pub capped_size_bytes: Option<i64>,
    /// Format for storing messages. Defaults to Normal.
    #[serde(default)]
    pub format: MongoDbFormat,
    /// The ID used for the cursor in sequenced mode. If not provided, consumption starts from the current sequence (ephemeral).
    pub cursor_id: Option<String>,
    /// (Consumer only) Optional custom MongoDB query to filter messages. Provided as a JSON string (e.g., '{"type": "notification"}').
    pub receive_query: Option<String>,
    /// (Optional) Collection to store sequence counters and cursor positions. Defaults to the message collection if not set.
    pub meta_collection: Option<String>,
    /// Share one MongoDB client per connection (default: true); false forces a dedicated client.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(default = "default_shared_schema"))]
    pub shared: Option<bool>,
}

impl MongoDbConfig {
    /// Creates a new MongoDB configuration with the specified URL and database name.
    pub fn new(url: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            database: database.into(),
            ..Default::default()
        }
    }

    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_change_stream(mut self, change_stream: bool) -> Self {
        self.change_stream = change_stream;
        self
    }

    /// The effective consume mode: the explicit `consume` field if set, otherwise derived from the
    /// deprecated `change_stream` boolean.
    pub fn resolved_consume(&self) -> MongoConsume {
        if let Some(mode) = self.consume {
            return mode;
        }
        if self.change_stream {
            MongoConsume::Subscriber
        } else {
            MongoConsume::Consumer
        }
    }
}

// --- MQTT Specific Configuration ---

/// General MQTT connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    /// MQTT broker URL (e.g., "tcp://localhost:1883"). Does not support multiple hosts. If it contains userinfo, it will be treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// The MQTT topic.
    pub topic: Option<String>,
    /// Optional username for authentication.
    pub username: Option<String>,
    /// Optional password for authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub password: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Optional client ID. If not provided, one is generated or derived from route name.
    pub client_id: Option<String>,
    /// Capacity of the internal channel for incoming messages. Defaults to 100.
    pub queue_capacity: Option<usize>,
    /// Maximum number of inflight messages.
    pub max_inflight: Option<u16>,
    /// Quality of Service level (0, 1, or 2). Defaults to 1.
    pub qos: Option<u8>,
    /// (Consumer only) If true, start with a clean session. Defaults to false (persistent session). Setting this to true effectively enables **Subscriber mode** (ephemeral).
    #[serde(default = "default_clean_session")]
    pub clean_session: bool,
    /// Keep-alive interval in seconds. Defaults to 20.
    pub keep_alive_seconds: Option<u64>,
    /// MQTT protocol version (V3 or V5). Defaults to V5.
    #[serde(default)]
    pub protocol: MqttProtocol,
    /// Session expiry interval in seconds (MQTT v5 only).
    pub session_expiry_interval: Option<u32>,
    /// (Consumer only) If true, messages are acknowledged immediately upon receipt (auto-ack).
    /// If false (default), messages are acknowledged after processing (manual-ack).
    /// Note: For QoS 1/2 the publisher always waits for end-to-end broker
    /// confirmation (PUBACK/PUBCOMP) before reporting success, independent of
    /// this setting; QoS 0 remains fire-and-forget.
    #[serde(default)]
    pub delayed_ack: bool,
}

impl MqttConfig {
    /// Creates a new MQTT configuration with the specified broker URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

/// MQTT protocol version.
///
/// Specifies which version of the MQTT protocol to use for connections.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum MqttProtocol {
    #[default]
    V5,
    V3,
}

// --- ZeroMQ Specific Configuration ---

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ZeroMqConfig {
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    /// The ZeroMQ URL (e.g., "tcp://127.0.0.1:5555").
    pub url: String,
    /// The socket type (PUSH, PULL, PUB, SUB, REQ, REP).
    #[serde(default)]
    pub socket_type: Option<ZeroMqSocketType>,
    /// (Consumer only) The ZeroMQ topic (for SUB sockets).
    pub topic: Option<String>,
    /// If true, bind to the address. If false, connect.
    #[serde(default)]
    pub bind: bool,
    /// Internal buffer size for the channel. Defaults to 128.
    #[serde(default)]
    pub internal_buffer_size: Option<usize>,
    /// Wire format: `json` wraps the CanonicalMessage; `raw` sends payload bytes per frame; `raw_framed` adds a JSON metadata frame. Default `json`.
    #[serde(default)]
    pub format: ZeroMqFormat,
}

impl ZeroMqConfig {
    /// Creates a new ZeroMQ configuration with the specified URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_socket_type(mut self, socket_type: ZeroMqSocketType) -> Self {
        self.socket_type = Some(socket_type);
        self
    }

    pub fn with_bind(mut self, bind: bool) -> Self {
        self.bind = bind;
        self
    }
}

/// ZeroMQ wire format.
///
/// `json` wraps each message as a JSON CanonicalMessage (batched into one frame);
/// `raw` sends/receives the payload bytes directly, one frame per message (metadata
/// is not transmitted); `raw_framed` sends a two-frame message — a JSON metadata frame
/// followed by the raw payload frame — keeping the payload binary-safe while still
/// carrying headers. Use `raw`/`raw_framed` for binary feeds such as JPEG, Avro or Protobuf.
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ZeroMqFormat {
    #[default]
    Json,
    Raw,
    RawFramed,
}

/// ZeroMQ socket type.
///
/// Defines the messaging pattern for ZeroMQ connections.
/// Different patterns support different communication paradigms (request-reply, publish-subscribe, etc.).
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum ZeroMqSocketType {
    Push,
    Pull,
    Pub,
    Sub,
    Req,
    Rep,
}

// --- Redis Streams Specific Configuration ---

/// Configuration for a Redis Streams endpoint.
///
/// Publishers `XADD` to the stream; consumers read via a consumer group
/// (`XREADGROUP` + `XACK`) by default, or ephemerally via `XREAD` from new
/// messages when `subscriber_mode` is set.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RedisStreamsConfig {
    /// Redis URL, `redis://` or `rediss://` for TLS. Userinfo is treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// The stream key to publish to or read from. Defaults to the route name.
    pub stream: Option<String>,
    /// (Consumer) Group name. Defaults to `{APP_NAME}-{stream}`; ignored in `subscriber_mode`.
    pub group: Option<String>,
    /// (Consumer) Consumer name within the group. Defaults to a unique per-instance id.
    pub consumer_name: Option<String>,
    /// (Consumer) Read ephemerally via `XREAD` from new messages (no group/acks). Default false.
    #[serde(default)]
    pub subscriber_mode: bool,
    /// (Consumer) Block timeout in milliseconds for each read. Defaults to 5000ms.
    pub block_ms: Option<u64>,
    /// (Consumer) On group creation, start from the stream beginning ("0") not "$". Default false.
    #[serde(default)]
    pub read_from_start: bool,
    /// (Consumer) Redeliver entries pending ≥ this long via `XAUTOCLAIM`; 0 disables. Default 60000ms.
    pub redelivery_timeout_ms: Option<u64>,
    /// (Publisher) If set, cap the stream length with `XADD MAXLEN`.
    pub maxlen: Option<usize>,
    /// (Publisher) Use approximate (`~`) trimming when `maxlen` is set. Defaults to true.
    pub approx_trim: Option<bool>,
    /// Optional username for authentication (Redis ACL).
    pub username: Option<String>,
    /// Optional password for authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub password: Option<String>,
    /// Internal buffer size for the consumer channel. Defaults to 128.
    pub internal_buffer_size: Option<usize>,
    /// (Consumer) Parallel `XREADGROUP` reader connections fanned out across the group. Default 1.
    /// Ignored in `subscriber_mode`.
    pub reader_connections: Option<usize>,
}

impl RedisStreamsConfig {
    /// Creates a new Redis Streams configuration with the specified URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    pub fn with_subscriber(mut self, subscriber: bool) -> Self {
        self.subscriber_mode = subscriber;
        self
    }

    pub fn with_reader_connections(mut self, connections: usize) -> Self {
        self.reader_connections = Some(connections);
        self
    }
}

// --- gRPC Specific Configuration ---

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    /// The gRPC server URL (e.g., "http://localhost:50051" for client or "0.0.0.0:50051" for server mode).
    pub url: String,
    /// Topic / subject used for both subscribe and publish paths.
    pub topic: Option<String>,
    /// Timeout in milliseconds.
    /// - Client mode: used as the connection timeout and per-request deadline.
    /// - Server mode: applied as the per-request deadline on the embedded server.
    pub timeout_ms: Option<u64>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// If `true`, start an embedded tonic gRPC server that accepts incoming `Publish` /
    /// `PublishBatch` RPCs. If `false` (the default), connect to a remote server as a client.
    #[serde(default)]
    pub server_mode: bool,
    /// HTTP/2 stream-level initial window size in bytes. **Server-mode only.**
    #[serde(default)]
    pub initial_stream_window_size: Option<u32>,
    /// HTTP/2 connection-level initial window size in bytes. **Server-mode only.**
    #[serde(default)]
    pub initial_connection_window_size: Option<u32>,
    /// Maximum number of concurrent requests handled per connection. **Server-mode only.**
    #[serde(default)]
    pub concurrency_limit_per_connection: Option<usize>,
    /// HTTP/2 keepalive ping interval in milliseconds. **Server-mode only.** Default disabled
    #[serde(default)]
    pub http2_keepalive_interval_ms: Option<u64>,
    /// Timeout for a keepalive ping acknowledgement in milliseconds. **Server-mode only.**
    #[serde(default)]
    pub http2_keepalive_timeout_ms: Option<u64>,
    /// Maximum size of a decoded incoming message in bytes. **Server-mode only.** Default 4 MiB.
    #[serde(default)]
    pub max_decoding_message_size: Option<usize>,
    /// (Publisher only) Share one gRPC channel per connection (default: true); false forces a dedicated channel.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(default = "default_shared_schema"))]
    pub shared: Option<bool>,
}

impl GrpcConfig {
    /// Creates a new gRPC configuration with the specified server URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Enable or disable server mode for this gRPC endpoint.
    pub fn with_server_mode(mut self, server_mode: bool) -> Self {
        self.server_mode = server_mode;
        self
    }
}

// --- HTTP Specific Configuration ---

/// Supported inbound HTTP protocols for server listeners.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum HttpServerProtocol {
    /// Accept both HTTP/1.1 and HTTP/2, matching the current default behavior.
    #[default]
    Auto,
    /// Accept only HTTP/1.x connections.
    Http1Only,
    /// Accept only HTTP/2 connections.
    Http2Only,
}

/// WebSocket route execution strategy.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WebSocketExecutionMode {
    /// Use direct per-connection handling for simple `websocket -> response` routes and fall back
    /// to the routed adapter with a warning when route semantics need the normal pipeline.
    #[default]
    Auto,
    /// Require direct per-connection handling. Startup fails if the route cannot run directly.
    DirectOnly,
    /// Always use the normal routed consumer/worker/disposition pipeline.
    Routed,
}

/// General HTTP connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// For consumers, the listen address (e.g., "0.0.0.0:8080"). For publishers, the target URL.
    pub url: String,
    /// (Consumer only) Optional request path filter. If set, only requests whose URI path matches exactly are delivered to this consumer.
    pub path: Option<String>,
    /// (Optional) HTTP method. For publishers: the method to use (defaults to POST). For consumers: restrict to this method (others return 405).
    pub method: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// (Consumer only) Number of worker threads to use. Defaults to 0 for unlimited.
    pub workers: Option<usize>,
    /// (Consumer only) Header key to extract the message ID from. Defaults to "message-id".
    pub message_id_header: Option<String>,
    /// Timeout for HTTP requests in milliseconds. For consumers, it's the request-reply timeout. For publishers, it's the timeout for each individual request. Defaults to 30000ms.
    pub request_timeout_ms: Option<u64>,
    /// (Consumer only) Internal buffer size for the channel. Defaults to 100.
    pub internal_buffer_size: Option<usize>,
    /// (Consumer only) If true, respond immediately with 202 Accepted without waiting for downstream processing. Defaults to false.
    #[serde(default)]
    pub fire_and_forget: bool,
    /// (Consumer only) If true, read request bodies as a stream and emit each received stream item as a separate message.
    #[serde(default)]
    pub receive_streamable: bool,
    /// (Consumer only) If true, compatible `http -> response` routes may bypass the normal route consumer/worker/disposition pipeline
    /// and reply inline for lower latency. Defaults to true. Set to false to force the normal route path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "schema",
        schemars(default = "default_inline_response_fast_path_schema")
    )]
    pub inline_response_fast_path: Option<bool>,
    /// (Consumer only) Restrict which HTTP protocol versions a server listener accepts.
    /// Defaults to `auto` (HTTP/1.1 + HTTP/2). On cleartext listeners, `http2_only`
    /// means HTTP/2 prior-knowledge (h2c) only.
    #[serde(default)]
    pub server_protocol: HttpServerProtocol,
    /// (Publisher only) Optional endpoint that receives streamed HTTP response items as correlated messages.
    ///
    /// Use a `stream_buffer` endpoint here when callers need to read streamed
    /// response items later through a normal mq-bridge consumer. Each streamed
    /// item is published with `correlation_id`, `http_stream_id`,
    /// `http_stream_index`, `http_stream_format`, and `http_stream_end`
    /// metadata. If the request message has no `correlation_id`, the HTTP
    /// publisher uses `format!("{:032x}", request.message_id)` so callers can
    /// derive the consumer correlation id before calling `send`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_response_to: Option<Box<Endpoint>>,
    /// (Publisher only) The number of concurrent HTTP requests to send in a batch. Defaults to 20.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_concurrency: Option<usize>,
    /// (Publisher only) TCP keepalive timeout for the underlying connection pool in milliseconds. Defaults to 60000ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_keepalive_ms: Option<u64>,
    /// (Publisher only) Timeout for idle connections in the connection pool in milliseconds. Defaults to 90000ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_idle_timeout_ms: Option<u64>,
    /// (Publisher only) Codec for the request body (`none`, `gzip`, `lz4`, `zstd`); overrides
    /// `compression_enabled`. `lz4` is non-standard (mq-bridge peers only). Ignored on a consumer —
    /// enable response compression with `compression_enabled`. Defaults to `none`.
    #[serde(default)]
    pub compression: Compression,
    /// Turns compression on. Publisher: compress the request body with gzip (unless `compression`
    /// sets another codec). Consumer: compress responses, negotiating the best codec the client's
    /// `Accept-Encoding` accepts. Defaults to off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_enabled: Option<bool>,
    /// Minimum message size in bytes to compress. Messages smaller than this are sent uncompressed. Defaults to 1024 bytes.
    #[serde(default)]
    pub compression_threshold_bytes: Option<usize>,
    /// (Consumer only) Maximum number of concurrent requests to handle. Defaults to 100.
    pub concurrency_limit: Option<usize>,
    /// HTTP Basic Authentication credentials (username, password). For consumers: validates incoming requests. For publishers: adds Authorization header.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_basic_auth"
    )]
    pub basic_auth: Option<(String, String)>,
    /// Custom headers as key-value pairs (e.g., {"X-API-Key": "token123"}). Added to outgoing HTTP headers for both consumers and publishers.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub custom_headers: HashMap<String, String>,
    /// (Publisher only) Share one HTTP client per connection (default: true); false forces a dedicated client.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(default = "default_shared_schema"))]
    pub shared: Option<bool>,
}

/// WebSocket connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    /// For consumers, the listen address (e.g. "0.0.0.0:9000"). For publishers, the target URL.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// (Consumer only) Optional request path filter. If set, only upgrade requests whose URI path matches exactly are delivered to this consumer.
    pub path: Option<String>,
    /// (Consumer only) Header key to extract the message ID from the WebSocket handshake. Defaults to "message-id".
    pub message_id_header: Option<String>,
    /// (Consumer only) Queue capacity for the routed adapter. Direct response routes do not use this queue. Defaults to 100.
    pub routed_queue_capacity: Option<usize>,
    /// (Consumer only) TCP listen backlog (pending-connection queue depth) for the accept socket.
    /// Raise this if high-concurrency handshake bursts are being dropped/reset before `accept()`
    /// can keep up. Defaults to 4096, which is higher than the OS/tokio default of 1024.
    pub backlog: Option<u32>,
    /// (Consumer only) Selects whether WebSocket routes run directly or through the routed pipeline.
    #[serde(default)]
    pub execution_mode: WebSocketExecutionMode,
}

fn deserialize_basic_auth<'de, D>(deserializer: D) -> Result<Option<(String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = serde_json::Value::deserialize(deserializer)?;
    match val {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Array(arr) => {
            if arr.len() != 2 {
                return Err(serde::de::Error::custom("basic_auth must have 2 elements"));
            }
            let u = arr[0]
                .as_str()
                .ok_or_else(|| serde::de::Error::custom("basic_auth[0] must be string"))?
                .to_string();
            let p = arr[1]
                .as_str()
                .ok_or_else(|| serde::de::Error::custom("basic_auth[1] must be string"))?
                .to_string();
            Ok(Some((u, p)))
        }
        serde_json::Value::Object(map) => {
            let u = map
                .get("0")
                .and_then(|v| v.as_str())
                .ok_or_else(|| serde::de::Error::custom("basic_auth map missing '0'"))?
                .to_string();
            let p = map
                .get("1")
                .and_then(|v| v.as_str())
                .ok_or_else(|| serde::de::Error::custom("basic_auth map missing '1'"))?
                .to_string();
            Ok(Some((u, p)))
        }
        _ => Err(serde::de::Error::custom("invalid type for basic_auth")),
    }
}

impl HttpConfig {
    /// Creates a new HTTP configuration with the specified URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn with_receive_streamable(mut self, receive_streamable: bool) -> Self {
        self.receive_streamable = receive_streamable;
        self
    }

    pub fn with_inline_response_fast_path(mut self, inline_response_fast_path: bool) -> Self {
        self.inline_response_fast_path = Some(inline_response_fast_path);
        self
    }

    pub fn with_server_protocol(mut self, server_protocol: HttpServerProtocol) -> Self {
        self.server_protocol = server_protocol;
        self
    }

    pub fn inline_response_fast_path_enabled(&self) -> bool {
        self.inline_response_fast_path.unwrap_or(true)
    }

    /// Request-body codec for a publisher: explicit `compression`, else gzip when
    /// `compression_enabled`, else none.
    pub fn publisher_compression(&self) -> Compression {
        match self.compression {
            Compression::None if self.compression_enabled == Some(true) => Compression::Gzip,
            other => other,
        }
    }

    /// Whether a consumer compresses responses (then it negotiates the best codec the client
    /// accepts). Driven by `compression_enabled`; the publisher-only `compression` codec is ignored.
    pub fn consumer_compression_enabled(&self) -> bool {
        self.compression_enabled == Some(true)
    }

    pub fn with_stream_response_to(mut self, endpoint: Endpoint) -> Self {
        self.stream_response_to = Some(Box::new(endpoint));
        self
    }
}

impl WebSocketConfig {
    /// Creates a new WebSocket configuration with the specified URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn with_backlog(mut self, backlog: u32) -> Self {
        self.backlog = Some(backlog);
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: WebSocketExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }
}

// --- IBM MQ Specific Configuration ---

/// TLS configuration for the IBM MQ native client.
///
/// The IBM MQ client doesn't consume PEM files, so this uses MQ-native field
/// names rather than the generic [`TlsConfig`] used by the other endpoints.
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(transform = ibm_tls_config_schema_transform))]
#[serde(deny_unknown_fields)]
pub struct IbmTlsConfig {
    /// If true, enable TLS/SSL.
    #[serde(default, deserialize_with = "deserialize_null_as_false")]
    pub required: bool,
    /// TLS CipherSpec (e.g., `ANY_TLS12`). Required for encrypted connections. IBM MQ-specific.
    pub cipher_spec: Option<String>,
    /// For IBM MQ this is the CMS key repository stem (e.g. `/path/to/tls` for `tls.kdb`/`tls.sth`),
    /// not a PEM file. Exposed as `cert_file` for config parity with the generic `TlsConfig`;
    /// the MQ-native name `key_repository` is still accepted.
    #[serde(rename = "cert_file", alias = "key_repository")]
    pub key_repository: Option<String>,
    /// Password unlocking the key repository. Requires an IBM MQ client/server at 9.3.0.0+.
    /// Exposed as `cert_password` for parity with `TlsConfig`; alias `key_repository_password`.
    #[serde(rename = "cert_password", alias = "key_repository_password")]
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub key_repository_password: Option<String>,
    /// If true, disable server certificate verification (insecure).
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

// schemars ignores serde `alias`, so the MQ-native names accepted at runtime
// (`key_repository`, `key_repository_password`) must be added to the schema by
// hand, otherwise `additionalProperties: false` rejects otherwise-valid configs.
#[cfg(feature = "schema")]
fn ibm_tls_config_schema_transform(schema: &mut schemars::Schema) {
    let Some(properties) = schema
        .as_object_mut()
        .and_then(|schema_obj| schema_obj.get_mut("properties"))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    properties.insert(
        "key_repository".to_string(),
        serde_json::json!({
            "description": "MQ-native alias for `cert_file`: the CMS key repository stem \
                (e.g. `/path/to/tls` for `tls.kdb`/`tls.sth`).",
            "type": ["string", "null"]
        }),
    );

    properties.insert(
        "key_repository_password".to_string(),
        serde_json::json!({
            "description": "MQ-native alias for `cert_password`: password unlocking the key \
                repository. Requires an IBM MQ client/server at 9.3.0.0+.",
            "type": ["string", "null"],
            "format": "password"
        }),
    );
}

/// Connection settings for the IBM MQ Queue Manager.
// Default is implemented manually (not derived): the numeric fields must match
// the serde defaults, otherwise `IbmMqConfig::new()` / `..Default::default()`
// would yield max_message_size=0 (zero-length receive buffer) and wait_timeout=0.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct IbmMqConfig {
    /// Required. Connection URL in `host(port)` format. Supports comma-separated list for failover (e.g., `host1(1414),host2(1414)`). If it contains userinfo, it will be treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// Target Queue name for point-to-point messaging. Optional if `topic` is set; defaults to route name if omitted.
    pub queue: Option<String>,
    /// Target Topic string for Publish/Subscribe. If set, enables **Subscriber mode** (Consumer) or publishes to a topic (Publisher). Optional if `queue` is set.
    pub topic: Option<String>,
    /// Required. Name of the Queue Manager to connect to (e.g., `QM1`).
    pub queue_manager: String,
    /// Required. Server Connection (SVRCONN) Channel name defined on the QM.
    pub channel: String,
    /// Username for authentication. Optional; required if the channel enforces authentication
    pub username: Option<String>,
    /// Password for authentication. Optional; required if the channel enforces authentication.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub password: Option<String>,
    /// TLS configuration settings (e.g., keystore paths). Optional.
    #[serde(default)]
    pub tls: IbmTlsConfig,
    /// Maximum message size in bytes (default: 4MB). Optional.
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    /// (Consumer only) Polling timeout in milliseconds (default: 1000ms). Optional.
    #[serde(default = "default_wait_timeout_ms")]
    pub wait_timeout_ms: i32,
    /// Internal buffer size for the channel. Defaults to 100.
    #[serde(default)]
    pub internal_buffer_size: Option<usize>,
    /// If false, attempt to open the queue with INQUIRE permissions to fetch queue depth for status checks. Defaults to false.
    #[serde(default)]
    pub disable_status_inq: bool,
}

impl IbmMqConfig {
    /// Creates a new IBM MQ configuration with the specified connection URL, queue manager, and channel.
    pub fn new(
        url: impl Into<String>,
        queue_manager: impl Into<String>,
        channel: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            queue_manager: queue_manager.into(),
            channel: channel.into(),
            disable_status_inq: false,
            ..Default::default()
        }
    }

    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

impl Default for IbmMqConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            queue: None,
            topic: None,
            queue_manager: String::new(),
            channel: String::new(),
            username: None,
            password: None,
            tls: IbmTlsConfig::default(),
            max_message_size: default_max_message_size(),
            wait_timeout_ms: default_wait_timeout_ms(),
            internal_buffer_size: None,
            disable_status_inq: false,
        }
    }
}

fn default_max_message_size() -> usize {
    4 * 1024 * 1024 // 4MB default
}

fn default_wait_timeout_ms() -> i32 {
    1000 // 1 second default
}

// --- Switch/Router Configuration ---

#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SwitchConfig {
    /// The metadata key to inspect for routing decisions.
    pub metadata_key: String,
    /// A map of values to endpoints.
    pub cases: HashMap<String, Endpoint>,
    /// The default endpoint if no case matches.
    pub default: Option<Box<Endpoint>>,
}

// --- Response Endpoint Configuration ---
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResponseConfig {
    // This struct is a marker and currently has no fields.
}

// --- Request/Forward Endpoint Configuration ---

/// Sends each message to a request-capable endpoint and forwards its response elsewhere.
///
/// Turns a request/reply exchange (HTTP, or a request_reply NATS/Mongo/Memory endpoint) into
/// a one-way flow whose response lands on `forward_to` — e.g. IBM MQ → HTTP → IBM MQ. On
/// request error/timeout the original message is forwarded instead (unchanged). Successful
/// responses carry the transport-native status (e.g. `http_status_code`), so a `switch` on
/// `forward_to` can route them by status; a failed request forwards the original message with
/// no status key, so catch failures on the switch's default branch.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RequestForwardConfig {
    /// The request-capable endpoint to send each message to (e.g. an `http` client).
    pub to: Box<Endpoint>,
    /// Where the response (or, on error, the original message) is forwarded.
    pub forward_to: Box<Endpoint>,
}

// --- Postgres CDC (logical replication) Configuration ---

/// Postgres logical-replication CDC source (pgoutput). Source-only.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcConfig {
    /// Connection URL, e.g. `postgres://user:pass@host:5432/dbname`.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// Publication name (must already exist; defines which tables are captured).
    pub publication: String,
    /// Replication slot name; created if missing when `create_slot` is true.
    #[serde(default = "default_pg_cdc_slot")]
    pub slot_name: String,
    /// Create the replication slot if it does not exist.
    #[serde(default = "default_true")]
    pub create_slot: bool,
    /// Create the `publication` if missing (default false; leave off if it pre-exists).
    /// Needs table ownership for `publication_tables`, or superuser when none are set (`FOR ALL TABLES`).
    #[serde(default)]
    pub create_publication: bool,
    /// Tables to include when managing the publication (`create_publication`); may be `schema.table`.
    /// Missing ones are added to an existing publication (never removed). Empty = `FOR ALL TABLES` (needs superuser).
    #[serde(default)]
    pub publication_tables: Vec<String>,
    /// Use a temporary slot (dropped on disconnect). Not restart-safe; default is a permanent slot.
    #[serde(default)]
    pub temporary_slot: bool,
    /// Checkpoint key for persisting the confirmed LSN across restarts (optional; the slot is authoritative).
    pub cursor_id: Option<String>,
    /// Checkpoint store spec (e.g. `file:///path`, `s3://bucket/prefix`); defaults to the source database.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub checkpoint_store: Option<String>,
    /// Standby-status-update interval in ms; must be shorter than the server's `wal_sender_timeout`.
    #[serde(default = "default_pg_cdc_status_interval_ms")]
    pub status_interval_ms: u64,
    /// TLS configuration for the replication connection.
    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_pg_cdc_slot() -> String {
    "mq_bridge_slot".to_string()
}

fn default_pg_cdc_status_interval_ms() -> u64 {
    10_000
}

// --- SQLx Specific Configuration ---

/// General SQLx connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SqlxConfig {
    /// Database connection URL. If it contains userinfo, it will be treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// Optional username. Takes precedence over any credentials embedded in the `url`.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password. Takes precedence over any credentials embedded in the `url`.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    #[serde(default)]
    pub password: Option<String>,
    /// The table to interact with.
    pub table: String,
    /// (Publisher only) Optional. A custom SQL INSERT query. Use `?` as a placeholder for the payload.
    /// If not provided, a default `INSERT INTO {table} (payload) VALUES (?)` is used.
    ///
    /// For multi-column inserts, embed explicit source tokens directly in the query:
    /// `${metadata:<key>}` binds `message.metadata["<key>"]`, and `${payload:<field>}`
    /// binds the top-level JSON field `<field>` of the payload (types preserved:
    /// numbers/bools stay numeric/bool). There is no fallback between the two: an
    /// absent metadata key, non-JSON payload, or missing/non-scalar field binds SQL NULL.
    /// Example: `INSERT INTO orders (customer_id, sku, qty) VALUES (${metadata:customer_id}, ${payload:sku}, ${payload:qty})`.
    /// A query with no `${...}` tokens behaves exactly as before (whole payload bound once).
    /// `auto_create_table` is not supported together with a token-based query.
    ///
    /// Tokens bind as text/number/bool; Postgres won't implicitly cast text into a
    /// `numeric`/`timestamptz` column (these arrive as JSON strings from a sql source).
    /// Add an explicit cast next to the token — it is preserved verbatim in the SQL:
    /// `VALUES (${payload:amount}::numeric, ${payload:created_at}::timestamptz)`.
    pub insert_query: Option<String>,
    /// (Consumer only) Optional. A custom SQL SELECT query to fetch messages. This is only supported for PostgreSQL and Microsoft SQL Server.
    /// The query must include a placeholder for the batch size (`$1` for PostgreSQL, `@p1` for SQL Server).
    /// The bridge will bind the route's `batch_size` to this placeholder.
    pub select_query: Option<String>,
    /// (Consumer only) If true, delete messages after processing.
    #[serde(default)]
    pub delete_after_read: bool,
    /// (Consumer only) Read an existing table **non-destructively** and resumably, paging by this
    /// monotonic column (`SELECT * FROM {table} WHERE {cursor_column} > $last ORDER BY {cursor_column} ASC LIMIT n`)
    /// and persisting the last read value under `cursor_id`. Does not delete/lock source rows.
    /// Mutually exclusive with `delete_after_read`.
    pub cursor_column: Option<String>,
    /// (Consumer only) Cursor id used to key the persisted resume position. Recommended when
    /// `cursor_column` is set: without it, progress is not persisted and every restart re-copies
    /// from the beginning.
    pub cursor_id: Option<String>,
    /// (Consumer only) Where to persist the resume cursor in `cursor_column` mode. A URL selects the
    /// backend; a bare name (or `/name`) reuses the **source** datastore with that table name:
    /// - absent → source datastore, table `mqb_cursors_<source_table>` (auto-unique)
    /// - `/my_cursors` → source datastore, table `my_cursors`
    /// - `file:///var/lib/mqb/cursors.json` → local JSON file (read-only / write-restricted sources)
    /// - `postgres://user@host/db/table` or `mysql://host/db/table` → external SQL table (table optional)
    /// - `mongodb://host/db/collection` → external MongoDB collection (collection optional)
    /// - `s3://bucket/prefix` (also `gs://`, `az://`, `abfs://`) → cloud object store; creds via env
    ///
    /// When no table/collection is named, it defaults to `mqb_cursors_<source_table>`.
    /// May embed connection credentials, so it is treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub checkpoint_store: Option<String>,
    /// (Publisher only) If true, automatically create the table and indexes if they don't exist. Defaults to false.
    #[serde(default)]
    pub auto_create_table: bool,
    /// (Publisher only) PostgreSQL only. Bulk-load batches via `COPY FROM STDIN` (much faster than multi-row INSERT). Requires a token-based `insert_query`; no `ON CONFLICT`/`RETURNING`.
    #[serde(default)]
    pub bulk_copy: bool,
    /// (Consumer only) Polling interval in milliseconds. Defaults to 100ms.
    pub polling_interval_ms: Option<u64>,
    /// (Consumer only) If set, the poll interval backs off exponentially from `polling_interval_ms`
    /// up to this value while drained, resetting on new rows. Unset = constant interval.
    pub max_polling_interval_ms: Option<u64>,
    /// (Consumer only, PostgreSQL) If set, consume via logical-replication CDC instead of cursor
    /// polling: streams inserts/updates/deletes from this publication. Requires the `postgres-cdc`
    /// feature and a Postgres URL. For full control use the dedicated `postgres_cdc` endpoint.
    pub publication: Option<String>,
    /// (Consumer only, CDC) Replication slot name; created if missing. Defaults to `mq_bridge_slot`.
    pub slot_name: Option<String>,
    /// (Consumer only, CDC) When `publication` is set, create it if missing (default false).
    /// Needs table-owner privilege: it is auto-published `FOR TABLE {table}`.
    #[serde(default)]
    pub create_publication: bool,
    /// TLS configuration for the database connection.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Maximum number of connections in the pool. Defaults to 10.
    pub max_connections: Option<u32>,
    /// Minimum number of connections to keep in the pool. Defaults to 0.
    pub min_connections: Option<u32>,
    /// Timeout for acquiring a connection from the pool in milliseconds. Defaults to 30000ms.
    pub acquire_timeout_ms: Option<u64>,
    /// Maximum idle time for a connection in milliseconds. Defaults to 600000ms (10 minutes).
    pub idle_timeout_ms: Option<u64>,
    /// Maximum lifetime of a connection in milliseconds. Defaults to 1800000ms (30 minutes).
    pub max_lifetime_ms: Option<u64>,
    /// Share one connection pool per connection (default: true); false forces a dedicated pool.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(default = "default_shared_schema"))]
    pub shared: Option<bool>,
}

// --- ClickHouse Specific Configuration ---

/// ClickHouse endpoint configuration (talks the ClickHouse HTTP interface).
///
/// As a **publisher** it batch-inserts messages using `FORMAT JSONEachRow` — by default the whole
/// message payload (which must be a JSON object) becomes one row; set `columns` to build each row
/// from explicit `${payload:<field>}` / `${metadata:<key>}` tokens instead. As a **consumer** it
/// reads an existing table **non-destructively** by paging over a monotonic `cursor_column`
/// (ClickHouse has no native queue/pub-sub), serializing each row to a JSON payload.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP endpoint URL, e.g. `http://localhost:8123` (or `https://…`). If it contains
    /// userinfo, it will be treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub url: String,
    /// Optional username. Takes precedence over any credentials embedded in the `url`. Defaults to `default`.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password. Takes precedence over any credentials embedded in the `url`.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    #[serde(default)]
    pub password: Option<String>,
    /// Database name. Defaults to `default`.
    pub database: Option<String>,
    /// The table to read from / write to. May be schema-qualified (`db.table`).
    pub table: String,
    /// (Publisher only) Optional per-column mapping. Each entry maps a target column name to a value
    /// token: `${payload:<field>}` takes the top-level JSON field `<field>` of the payload (JSON type
    /// preserved), `${metadata:<key>}` takes `message.metadata["<key>"]` (as a string), and any other
    /// value is inserted literally. When omitted, the whole payload JSON object is inserted as one row.
    pub columns: Option<std::collections::BTreeMap<String, String>>,
    /// (Publisher only) If true, set the ClickHouse `async_insert=1` server setting so inserts are
    /// buffered server-side. Defaults to false.
    #[serde(default)]
    pub async_insert: bool,
    /// (Publisher only) With `async_insert`, wait for the server to flush before acking. Defaults to
    /// true (durable). False = fire-and-forget: faster, but a crash before flush can drop the batch.
    #[serde(default)]
    pub wait_for_async_insert: Option<bool>,
    /// (Consumer only) Read an existing table **non-destructively** and resumably, paging by this
    /// monotonic column (`SELECT … WHERE {cursor_column} > {last} ORDER BY {cursor_column} ASC LIMIT n`)
    /// and persisting the last read value under `cursor_id`.
    pub cursor_column: Option<String>,
    /// (Consumer only) Cursor id used to key the persisted resume position. Without it, progress is not
    /// persisted and every restart re-copies from the beginning.
    pub cursor_id: Option<String>,
    /// (Consumer only) Where to persist the resume cursor. Because ClickHouse is unsuited to per-row
    /// cursor upserts, a durable checkpoint requires an **external** store URL:
    /// - `file:///var/lib/mqb/cursors.json` → local JSON file
    /// - `postgres://user@host/db/table` / `mysql://host/db/table` → external SQL table (table optional)
    /// - `mongodb://host/db/collection` → external MongoDB collection (collection optional)
    /// - `s3://bucket/prefix` (also `gs://`, `az://`, `abfs://`) → cloud object store; creds via env
    ///
    /// May embed connection credentials, so it is treated as a secret.
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub checkpoint_store: Option<String>,
    /// (Consumer only) Columns to select in `cursor_column` mode. Defaults to `*`.
    pub select_columns: Option<String>,
    /// (Consumer only) Polling interval in milliseconds when the table is drained. Defaults to 100ms.
    pub polling_interval_ms: Option<u64>,
    /// (Consumer only) If set, the poll interval backs off exponentially from `polling_interval_ms`
    /// up to this value while drained, resetting on new rows. Unset = constant interval.
    pub max_polling_interval_ms: Option<u64>,
    /// Request timeout in milliseconds for ClickHouse HTTP calls (inserts, cursor reads, status).
    /// Unset = no timeout (wait indefinitely), which suits very large batch inserts.
    pub request_timeout_ms: Option<u64>,
    /// Connection (TCP + TLS handshake) timeout in milliseconds. Defaults to 10000ms.
    pub connect_timeout_ms: Option<u64>,
    /// TLS configuration for `https://` connections.
    #[serde(default)]
    pub tls: TlsConfig,
    /// HTTP body compression for inserts and cursor reads (`none`, `gzip`, `lz4`, `zstd`). Applied
    /// as `Content-Encoding` on the request body and negotiated on the response via `Accept-Encoding`.
    /// `lz4`/`zstd` are faster than `gzip`; all are understood natively by ClickHouse. Defaults to `gzip`.
    #[serde(default = "default_gzip_compression")]
    pub compression: Compression,
}

fn default_gzip_compression() -> Compression {
    Compression::Gzip
}

// --- Common Configuration ---

/// TLS configuration for secure connections.
///
/// Configures Transport Layer Security (TLS/SSL) for encrypted communication.
/// Supports both client certificate (mutual TLS) and server certificate validation.
///
/// # Examples
///
/// ```
/// use mq_bridge::models::TlsConfig;
///
/// let tls = TlsConfig {
///     required: true,
///     ca_file: Some("/path/to/ca.pem".to_string()),
///     cert_file: Some("/path/to/cert.pem".to_string()),
///     key_file: Some("/path/to/key.pem".to_string()),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// If true, enable TLS/SSL.
    #[serde(default, deserialize_with = "deserialize_null_as_false")]
    pub required: bool,
    /// Path to the CA certificate file.
    pub ca_file: Option<String>,
    /// Path to the client certificate file (PEM).
    pub cert_file: Option<String>,
    /// Path to the client private key file (PEM).
    pub key_file: Option<String>,
    /// Password for the private key (if encrypted).
    #[cfg_attr(feature = "schema", schemars(extend("format"="password")))]
    pub cert_password: Option<String>,
    /// If true, disable server certificate verification (insecure).
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl TlsConfig {
    /// Creates a new TLS configuration with default settings (TLS not required).
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ca_file(mut self, ca_file: impl Into<String>) -> Self {
        self.ca_file = Some(ca_file.into());
        self.required = true;
        self
    }

    pub fn with_client_cert(
        mut self,
        cert_file: impl Into<String>,
        key_file: impl Into<String>,
    ) -> Self {
        self.cert_file = Some(cert_file.into());
        self.key_file = Some(key_file.into());
        self.required = true;
        self
    }

    pub fn with_insecure(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    /// Checks if mutual TLS (mTLS) client authentication is configured.
    pub fn is_mtls_client_configured(&self) -> bool {
        self.required && self.cert_file.is_some() && self.key_file.is_some()
    }

    /// Checks if TLS server certificate authentication is configured.
    pub fn is_tls_server_configured(&self) -> bool {
        self.required && self.cert_file.is_some() && self.key_file.is_some()
    }

    /// Checks if the TLS configuration is sufficient to make a TLS client connection.
    pub fn is_tls_client_configured(&self) -> bool {
        self.required
            || self.ca_file.is_some()
            || (self.cert_file.is_some() && self.key_file.is_some())
    }

    /// Helper to normalize a URL by adding the appropriate scheme prefix (http:// or https://) if missing.
    pub fn normalize_url(&self, url: &str) -> String {
        if url
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
            || url
                .get(..8)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
        {
            url.to_string()
        } else {
            let is_tls = self.required;
            let scheme = if is_tls { "https" } else { "http" };
            format!("{}://{}", scheme, url)
        }
    }
}

/// Trait for extracting secrets from configuration structures.
pub trait SecretExtractor {
    /// Extracts secrets into the provided map using the given prefix, and clears them from self.
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>);
}

fn extract_sensitive_string_map_entries(
    values: &mut HashMap<String, String>,
    prefix: &str,
    field_name: &str,
    secrets: &mut HashMap<String, String>,
) {
    let secret_keys = values
        .keys()
        .filter(|key| {
            let key = key.to_ascii_lowercase();
            key.contains("key") || key.contains("token") || key.contains("auth")
        })
        .cloned()
        .collect::<Vec<_>>();

    for key in secret_keys {
        if let Some(value) = values.remove(&key) {
            secrets.insert(
                sanitize_secret_key(&format!("{}__{}__{}", prefix, field_name, key)),
                value,
            );
        }
    }
}

fn url_has_userinfo(url: &str) -> bool {
    let Some(authority_start) = url.find("://").map(|idx| idx + 3) else {
        return false;
    };
    let authority_end = url[authority_start..]
        .find(['/', '?', '#'])
        .map(|idx| authority_start + idx)
        .unwrap_or(url.len());
    url[authority_start..authority_end].contains('@')
}

fn sanitize_secret_key(key: &str) -> String {
    key.chars()
        .map(|ch| {
            let ch = ch.to_ascii_uppercase();
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn extract_sensitive_url(
    url: &mut String,
    prefix: &str,
    field_name: &str,
    secrets: &mut HashMap<String, String>,
) {
    if !url.is_empty() && url_has_userinfo(url) {
        secrets.insert(
            sanitize_secret_key(&format!("{}__{}", prefix, field_name)),
            std::mem::take(url),
        );
    }
}

fn extract_sensitive_optional_url(
    url: &mut Option<String>,
    prefix: &str,
    field_name: &str,
    secrets: &mut HashMap<String, String>,
) {
    if url.as_ref().is_some_and(|url| url_has_userinfo(url)) {
        if let Some(url) = url.take() {
            secrets.insert(
                sanitize_secret_key(&format!("{}__{}", prefix, field_name)),
                url,
            );
        }
    }
}

impl SecretExtractor for Route {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        self.input
            .extract_secrets(&format!("{}__{}", prefix, "INPUT"), secrets);
        self.output
            .extract_secrets(&format!("{}__{}", prefix, "OUTPUT"), secrets);
    }
}

impl SecretExtractor for Endpoint {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        for (i, middleware) in self.middlewares.iter_mut().enumerate() {
            middleware.extract_secrets(&format!("{}__{}__{}", prefix, "MIDDLEWARES", i), secrets);
        }
        self.endpoint_type.extract_secrets(prefix, secrets);
    }
}

impl SecretExtractor for EndpointType {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        match self {
            EndpointType::Aws(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "AWS"), secrets)
            }
            EndpointType::Kafka(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "KAFKA"), secrets)
            }
            EndpointType::Nats(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "NATS"), secrets)
            }
            EndpointType::Amqp(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "AMQP"), secrets)
            }
            EndpointType::MongoDb(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "MONGODB"), secrets)
            }
            EndpointType::Mqtt(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "MQTT"), secrets)
            }
            EndpointType::Http(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "HTTP"), secrets)
            }
            EndpointType::WebSocket(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "WEBSOCKET"), secrets)
            }
            EndpointType::IbmMq(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "IBMMQ"), secrets)
            }
            EndpointType::ZeroMq(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "ZEROMQ"), secrets)
            }
            EndpointType::RedisStreams(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "REDIS_STREAMS"), secrets)
            }
            EndpointType::Sqlx(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "SQLX"), secrets)
            }
            EndpointType::ClickHouse(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "CLICKHOUSE"), secrets)
            }
            EndpointType::PostgresCdc(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "POSTGRES_CDC"), secrets)
            }
            EndpointType::Grpc(cfg) => {
                cfg.extract_secrets(&format!("{}__{}", prefix, "GRPC"), secrets)
            }
            EndpointType::Fanout(endpoints) => {
                for (i, ep) in endpoints.iter_mut().enumerate() {
                    ep.extract_secrets(&format!("{}__{}__{}", prefix, "FANOUT", i), secrets);
                }
            }
            EndpointType::Switch(cfg) => {
                for (key, ep) in cfg.cases.iter_mut() {
                    ep.extract_secrets(
                        &format!(
                            "{}__{}__{}",
                            prefix,
                            "SWITCH__CASES",
                            sanitize_secret_key(key)
                        ),
                        secrets,
                    );
                }
                if let Some(default) = &mut cfg.default {
                    default.extract_secrets(&format!("{}__{}", prefix, "SWITCH__DEFAULT"), secrets);
                }
            }
            EndpointType::Reader(ep) => {
                ep.extract_secrets(&format!("{}__{}", prefix, "READER"), secrets)
            }
            EndpointType::Request(cfg) => {
                cfg.to
                    .extract_secrets(&format!("{}__{}", prefix, "REQUEST__TO"), secrets);
                cfg.forward_to
                    .extract_secrets(&format!("{}__{}", prefix, "REQUEST__FORWARD_TO"), secrets);
            }
            _ => {}
        }
    }
}

impl SecretExtractor for Middleware {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        if let Middleware::Dlq(cfg) = self {
            cfg.endpoint
                .extract_secrets(&format!("{}__{}__{}", prefix, "DLQ", "ENDPOINT"), secrets);
        }
    }
}

impl SecretExtractor for AwsConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        if let Some(val) = self.access_key.take() {
            secrets.insert(format!("{}__{}", prefix, "ACCESS_KEY"), val);
        }
        if let Some(val) = self.secret_key.take() {
            secrets.insert(format!("{}__{}", prefix, "SECRET_KEY"), val);
        }
        if let Some(val) = self.session_token.take() {
            secrets.insert(format!("{}__{}", prefix, "SESSION_TOKEN"), val);
        }
        extract_sensitive_optional_url(&mut self.queue_url, prefix, "QUEUE_URL", secrets);
        extract_sensitive_optional_url(&mut self.endpoint_url, prefix, "ENDPOINT_URL", secrets);
    }
}

impl SecretExtractor for KafkaConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for NatsConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        if let Some(val) = self.token.take() {
            secrets.insert(format!("{}__{}", prefix, "TOKEN"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for AmqpConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for MongoDbConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        // The checkpoint store URL may embed connection credentials.
        extract_sensitive_optional_url(
            &mut self.checkpoint_store,
            prefix,
            "CHECKPOINT_STORE",
            secrets,
        );
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for MqttConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for HttpConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some((u, p)) = self.basic_auth.take() {
            secrets.insert(format!("{}__{}__{}", prefix, "BASIC_AUTH", 0), u);
            secrets.insert(format!("{}__{}__{}", prefix, "BASIC_AUTH", 1), p);
        }
        extract_sensitive_string_map_entries(
            &mut self.custom_headers,
            prefix,
            "CUSTOM_HEADERS",
            secrets,
        );
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
        if let Some(endpoint) = &mut self.stream_response_to {
            endpoint.extract_secrets(&format!("{}__{}", prefix, "STREAM_RESPONSE_TO"), secrets);
        }
    }
}

impl SecretExtractor for WebSocketConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
    }
}

impl SecretExtractor for IbmMqConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for ZeroMqConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
    }
}

impl SecretExtractor for RedisStreamsConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
    }
}

impl SecretExtractor for SqlxConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for ClickHouseConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.username.take() {
            secrets.insert(format!("{}__{}", prefix, "USERNAME"), val);
        }
        if let Some(val) = self.password.take() {
            secrets.insert(format!("{}__{}", prefix, "PASSWORD"), val);
        }
        if let Some(val) = self.checkpoint_store.take() {
            secrets.insert(format!("{}__{}", prefix, "CHECKPOINT_STORE"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for PostgresCdcConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        if let Some(val) = self.checkpoint_store.take() {
            secrets.insert(format!("{}__{}", prefix, "CHECKPOINT_STORE"), val);
        }
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for GrpcConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        extract_sensitive_url(&mut self.url, prefix, "URL", secrets);
        self.tls
            .extract_secrets(&format!("{}__{}", prefix, "TLS"), secrets);
    }
}

impl SecretExtractor for TlsConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        if let Some(val) = self.cert_password.take() {
            secrets.insert(format!("{}__{}", prefix, "CERT_PASSWORD"), val);
        }
    }
}

impl SecretExtractor for IbmTlsConfig {
    fn extract_secrets(&mut self, prefix: &str, secrets: &mut HashMap<String, String>) {
        if let Some(val) = self.key_repository_password.take() {
            // Wire/env name matches the serde rename (`cert_password`), so the config
            // crate's env override resolves back to this field.
            secrets.insert(format!("{}__{}", prefix, "CERT_PASSWORD"), val);
        }
    }
}

/// Extracts sensitive values (passwords, keys, tokens) from the configuration
/// and returns them as a map of environment variables (key-value pairs).
/// The extracted fields in the configuration are set to `None`.
///
/// The keys in the returned map follow the `MQB__{ROUTE}__{ENDPOINT}__{FIELD}` pattern
/// compatible with the `config` crate's environment variable override mechanism.
pub fn extract_config_secrets(config: &mut Config) -> HashMap<String, String> {
    let mut secrets = HashMap::new();
    for (route_name, route) in config.iter_mut() {
        let prefix = sanitize_secret_key(&format!("MQB__{}", route_name));
        route.extract_secrets(&prefix, &mut secrets);
    }
    secrets
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{Config as ConfigBuilder, Environment};

    const TEST_YAML: &str = r#"
kafka_to_nats:
  concurrency: 10
  input:
    middlewares:
      - deduplication:
          sled_path: "/tmp/mq-bridge/dedup_db"
          ttl_seconds: 3600
      - metrics: {}
      - retry:
          max_attempts: 5
          initial_interval_ms: 200
      - random_panic:
          mode: nack
      - dlq:
          endpoint:
            nats:
              subject: "dlq-subject"
              url: "nats://localhost:4222"
    kafka:
      topic: "input-topic"
      url: "localhost:9092"
      group_id: "my-consumer-group"
      tls:
        required: true
        ca_file: "/path_to_ca"
        cert_file: "/path_to_cert"
        key_file: "/path_to_key"
        cert_password: "password"
        accept_invalid_certs: true
  output:
    middlewares:
      - metrics: {}
      - dlq:
          endpoint:
            file:
              path: "error.out"
    nats:
      subject: "output-subject"
      url: "nats://localhost:4222"
"#;

    fn assert_config_values(config: &Config) {
        assert_eq!(config.len(), 1);
        let route = config.get("kafka_to_nats").expect("Route should exist");

        assert_eq!(route.options.concurrency, 10);

        // --- Assert Input ---
        let input = &route.input;
        assert_eq!(input.middlewares.len(), 5);

        let mut has_dedup = false;
        let mut has_metrics = false;
        let mut has_dlq = false;
        let mut has_retry = false;
        let mut has_random_panic = false;
        for middleware in &input.middlewares {
            match middleware {
                Middleware::Deduplication(dedup) => {
                    assert_eq!(dedup.sled_path, "/tmp/mq-bridge/dedup_db");
                    assert_eq!(dedup.ttl_seconds, 3600);
                    has_dedup = true;
                }
                Middleware::Metrics(_) => {
                    has_metrics = true;
                }
                Middleware::Custom { .. } => {}
                Middleware::Dlq(dlq) => {
                    assert!(dlq.endpoint.middlewares.is_empty());
                    if let EndpointType::Nats(nats_cfg) = &dlq.endpoint.endpoint_type {
                        assert_eq!(nats_cfg.subject, Some("dlq-subject".to_string()));
                        assert_eq!(nats_cfg.url, "nats://localhost:4222");
                    }
                    has_dlq = true;
                }
                Middleware::Retry(retry) => {
                    assert_eq!(retry.max_attempts, 5);
                    assert_eq!(retry.initial_interval_ms, 200);
                    has_retry = true;
                }
                Middleware::RandomPanic(rp) => {
                    assert!(rp.mode == FaultMode::Nack);
                    has_random_panic = true;
                }
                Middleware::Delay(_) => {}
                Middleware::WeakJoin(_) => {}
                Middleware::Limiter(_) => {}
                Middleware::Buffer(_) => {}
                Middleware::CookieJar(_) => {}
                Middleware::Transform(_) => {}
                Middleware::Encryption(_) => {}
            }
        }

        if let EndpointType::Kafka(kafka) = &input.endpoint_type {
            assert_eq!(kafka.topic, Some("input-topic".to_string()));
            assert_eq!(kafka.url, "localhost:9092");
            assert_eq!(kafka.group_id, Some("my-consumer-group".to_string()));
            let tls = &kafka.tls;
            assert!(tls.required);
            assert_eq!(tls.ca_file.as_deref(), Some("/path_to_ca"));
            assert!(tls.accept_invalid_certs);
        } else {
            panic!("Input endpoint should be Kafka");
        }
        assert!(has_dedup);
        assert!(has_metrics);
        assert!(has_dlq);
        assert!(has_retry);
        assert!(has_random_panic);

        // --- Assert Output ---
        let output = &route.output;
        assert_eq!(output.middlewares.len(), 2);
        assert!(matches!(output.middlewares[0], Middleware::Metrics(_)));

        if let EndpointType::Nats(nats) = &output.endpoint_type {
            assert_eq!(nats.subject, Some("output-subject".to_string()));
            assert_eq!(nats.url, "nats://localhost:4222");
        } else {
            panic!("Output endpoint should be NATS");
        }
    }

    #[test]
    fn test_deserialize_from_yaml() {
        // We use serde_yaml directly here because the `config` crate's processing
        // can interfere with complex deserialization logic.
        let result: Result<Config, _> = serde_yaml_ng::from_str(TEST_YAML);
        println!("Deserialized from YAML: {:#?}", result);
        let config = result.expect("Failed to deserialize TEST_YAML");
        assert_config_values(&config);
    }

    #[test]
    fn test_deserialize_from_env() {
        // Set environment variables based on README
        unsafe {
            std::env::set_var("MQB__KAFKA_TO_NATS__CONCURRENCY", "10");
            std::env::set_var("MQB__KAFKA_TO_NATS__INPUT__KAFKA__TOPIC", "input-topic");
            std::env::set_var("MQB__KAFKA_TO_NATS__INPUT__KAFKA__URL", "localhost:9092");
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__INPUT__KAFKA__GROUP_ID",
                "my-consumer-group",
            );
            std::env::set_var("MQB__KAFKA_TO_NATS__INPUT__KAFKA__TLS__REQUIRED", "true");
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__INPUT__KAFKA__TLS__CA_FILE",
                "/path_to_ca",
            );
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__INPUT__KAFKA__TLS__ACCEPT_INVALID_CERTS",
                "true",
            );
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__OUTPUT__NATS__SUBJECT",
                "output-subject",
            );
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__OUTPUT__NATS__URL",
                "nats://localhost:4222",
            );
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__INPUT__MIDDLEWARES__0__DLQ__ENDPOINT__NATS__SUBJECT",
                "dlq-subject",
            );
            std::env::set_var(
                "MQB__KAFKA_TO_NATS__INPUT__MIDDLEWARES__0__DLQ__ENDPOINT__NATS__URL",
                "nats://localhost:4222",
            );
        }

        let builder = ConfigBuilder::builder()
            // Enable automatic type parsing for values from environment variables.
            .add_source(
                Environment::with_prefix("MQB")
                    .separator("__")
                    .try_parsing(true),
            );

        let config: Config = builder
            .build()
            .expect("Failed to build config")
            .try_deserialize()
            .expect("Failed to deserialize config");

        // We can't test all values from env, but we can check the ones we set.
        assert_eq!(config.get("kafka_to_nats").unwrap().options.concurrency, 10);
        if let EndpointType::Kafka(k) = &config.get("kafka_to_nats").unwrap().input.endpoint_type {
            assert_eq!(k.topic, Some("input-topic".to_string()));
            assert!(k.tls.required);
        } else {
            panic!("Expected Kafka endpoint");
        }

        let input = &config.get("kafka_to_nats").unwrap().input;
        assert_eq!(input.middlewares.len(), 1);
        if let Middleware::Dlq(_) = &input.middlewares[0] {
            // Correctly parsed
        } else {
            panic!("Expected DLQ middleware");
        }
    }

    #[test]
    fn test_extract_secrets() {
        let mut config = Config::new();
        let mut route = Route::default();

        // Setup Kafka with secrets
        let mut kafka_config = KafkaConfig::new("kafka://user:pass@localhost:9092");
        kafka_config.username = Some("user".to_string());
        kafka_config.password = Some("pass".to_string());
        kafka_config.tls.cert_password = Some("certpass".to_string());

        route.input = Endpoint {
            endpoint_type: EndpointType::Kafka(kafka_config),
            middlewares: vec![],
            handler: None,
        };

        // Setup HTTP with basic auth
        let mut http_config = HttpConfig::new("http://httpuser:httppass@localhost");
        http_config.basic_auth = Some(("httpuser".to_string(), "httppass".to_string()));
        http_config
            .custom_headers
            .insert("X-API-Key".to_string(), "http-api-key".to_string());
        http_config.custom_headers.insert(
            "X-Access-Token".to_string(),
            "http-access-token".to_string(),
        );
        http_config.custom_headers.insert(
            "X-Authentication".to_string(),
            "http-authentication".to_string(),
        );
        http_config.custom_headers.insert(
            "Authorization".to_string(),
            "Bearer secret-token".to_string(),
        );
        http_config
            .custom_headers
            .insert("X-Trace-Id".to_string(), "trace-value".to_string());

        route.output = Endpoint {
            endpoint_type: EndpointType::Http(http_config),
            middlewares: vec![],
            handler: None,
        };

        config.insert("test_route".to_string(), route);

        let secrets = extract_config_secrets(&mut config);

        // Verify secrets extracted
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__INPUT__KAFKA__URL")
                .map(|s| s.as_str()),
            Some("kafka://user:pass@localhost:9092")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__INPUT__KAFKA__USERNAME")
                .map(|s| s.as_str()),
            Some("user")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__INPUT__KAFKA__PASSWORD")
                .map(|s| s.as_str()),
            Some("pass")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__INPUT__KAFKA__TLS__CERT_PASSWORD")
                .map(|s| s.as_str()),
            Some("certpass")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__URL")
                .map(|s| s.as_str()),
            Some("http://httpuser:httppass@localhost")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__BASIC_AUTH__0")
                .map(|s| s.as_str()),
            Some("httpuser")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__BASIC_AUTH__1")
                .map(|s| s.as_str()),
            Some("httppass")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__CUSTOM_HEADERS__X_API_KEY")
                .map(|s| s.as_str()),
            Some("http-api-key")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__CUSTOM_HEADERS__X_ACCESS_TOKEN")
                .map(|s| s.as_str()),
            Some("http-access-token")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__CUSTOM_HEADERS__X_AUTHENTICATION")
                .map(|s| s.as_str()),
            Some("http-authentication")
        );
        assert_eq!(
            secrets
                .get("MQB__TEST_ROUTE__OUTPUT__HTTP__CUSTOM_HEADERS__AUTHORIZATION")
                .map(|s| s.as_str()),
            Some("Bearer secret-token")
        );

        // Verify config cleared
        let route = config.get("test_route").unwrap();
        if let EndpointType::Kafka(k) = &route.input.endpoint_type {
            assert!(k.url.is_empty());
            assert!(k.username.is_none());
            assert!(k.password.is_none());
            assert!(k.tls.cert_password.is_none());
        }
        if let EndpointType::Http(h) = &route.output.endpoint_type {
            assert!(h.url.is_empty());
            assert!(h.basic_auth.is_none());
            assert!(!h.custom_headers.contains_key("X-API-Key"));
            assert!(!h.custom_headers.contains_key("X-Access-Token"));
            assert!(!h.custom_headers.contains_key("X-Authentication"));
            assert!(!h.custom_headers.contains_key("Authorization"));
            assert_eq!(
                h.custom_headers.get("X-Trace-Id").map(|s| s.as_str()),
                Some("trace-value")
            );
        }
    }

    #[test]
    fn test_extract_sensitive_url_only_strips_authority_credentials() {
        let mut config = Config::new();
        let path_at_route = Route {
            output: Endpoint {
                endpoint_type: EndpointType::Http(HttpConfig::new(
                    "https://example.com/path/user@example.com?email=a@b.test",
                )),
                middlewares: vec![],
                handler: None,
            },
            ..Default::default()
        };
        config.insert("path_at_route".to_string(), path_at_route);

        let credential_route = Route {
            output: Endpoint {
                endpoint_type: EndpointType::Http(HttpConfig::new(
                    "https://user:pass@example.com/path",
                )),
                middlewares: vec![],
                handler: None,
            },
            ..Default::default()
        };
        config.insert("credential_route".to_string(), credential_route);

        let query_at_route = Route {
            output: Endpoint {
                endpoint_type: EndpointType::Http(HttpConfig::new(
                    "https://example.com?next=a@b.test",
                )),
                middlewares: vec![],
                handler: None,
            },
            ..Default::default()
        };
        config.insert("query_at_route".to_string(), query_at_route);

        let fragment_at_route = Route {
            output: Endpoint {
                endpoint_type: EndpointType::Http(HttpConfig::new(
                    "https://example.com#user@example.com",
                )),
                middlewares: vec![],
                handler: None,
            },
            ..Default::default()
        };
        config.insert("fragment_at_route".to_string(), fragment_at_route);

        let secrets = extract_config_secrets(&mut config);

        if let EndpointType::Http(http) = &config.get("path_at_route").unwrap().output.endpoint_type
        {
            assert_eq!(
                http.url,
                "https://example.com/path/user@example.com?email=a@b.test"
            );
        }
        if let EndpointType::Http(http) =
            &config.get("query_at_route").unwrap().output.endpoint_type
        {
            assert_eq!(http.url, "https://example.com?next=a@b.test");
        }
        if let EndpointType::Http(http) = &config
            .get("fragment_at_route")
            .unwrap()
            .output
            .endpoint_type
        {
            assert_eq!(http.url, "https://example.com#user@example.com");
        }
        if let EndpointType::Http(http) =
            &config.get("credential_route").unwrap().output.endpoint_type
        {
            assert!(http.url.is_empty());
        }
        assert_eq!(
            secrets
                .get("MQB__CREDENTIAL_ROUTE__OUTPUT__HTTP__URL")
                .map(String::as_str),
            Some("https://user:pass@example.com/path")
        );
        assert!(!secrets.contains_key("MQB__PATH_AT_ROUTE__OUTPUT__HTTP__URL"));
        assert!(!secrets.contains_key("MQB__QUERY_AT_ROUTE__OUTPUT__HTTP__URL"));
        assert!(!secrets.contains_key("MQB__FRAGMENT_AT_ROUTE__OUTPUT__HTTP__URL"));
    }

    #[test]
    fn test_memory_config_requires_topic_or_url() {
        let err = serde_yaml_ng::from_str::<MemoryConfig>("{}").unwrap_err();
        assert!(err
            .to_string()
            .contains("MemoryConfig: 'topic' (or 'url' alias) is required."));
    }

    #[test]
    fn test_file_config_inference() {
        let yaml = r#"
mode: group_subscribe
path: "/tmp/test"
group_id: "my_group"
"#;
        let config: FileConfig = serde_yaml_ng::from_str(yaml).unwrap();
        match config.mode {
            Some(FileConsumerMode::GroupSubscribe { group_id, .. }) => {
                assert_eq!(group_id, "my_group")
            }
            _ => panic!("Expected GroupSubscribe"),
        }

        let yaml_queue = r#"
mode: consume
path: "/tmp/test"
"#;
        let config_queue: FileConfig = serde_yaml_ng::from_str(yaml_queue).unwrap();
        match config_queue.mode {
            Some(FileConsumerMode::Consume { delete }) => assert!(!delete),
            _ => panic!("Expected Consume"),
        }
    }
}

#[cfg(all(test, feature = "schema"))]
mod schema_tests {
    use super::*;

    #[test]
    fn generate_json_schema() {
        let schema = schemars::schema_for!(Config);
        let schema_json = serde_json::to_string_pretty(&schema).unwrap();

        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("mq-bridge.schema.json");
        std::fs::write(path, schema_json).expect("Failed to write schema file");
    }
}
