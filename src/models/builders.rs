//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Builder and convenience methods on the configuration structs — `new`, `with_*`,
//! and the small accessors that resolve a field to its effective value.

use super::*;

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

impl MappingRule {
    /// The source path this rule reads from.
    pub fn path(&self) -> &str {
        match self {
            MappingRule::Path(p) => p,
            MappingRule::Detailed(d) => &d.path,
        }
    }
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

impl FileConfig {
    /// Creates a new File configuration with the specified path.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            idempotency: false,
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
            MongoConsume::default()
        }
    }
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
