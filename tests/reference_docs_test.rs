//! Parses every config shape documented in REFERENCE.md.
//!
//! The reference file is the place people (and LLMs) look up what exists and how to spell
//! it, so a snippet that does not deserialize is a real bug. Add a case here whenever a
//! middleware or structural endpoint is added or its config changes.

use mq_bridge::models::{Endpoint, Middleware};

/// Indents every non-empty line, so a doc snippet can be embedded under a YAML key.
fn indent(yaml: &str, spaces: usize) -> String {
    let pad = " ".repeat(spaces);
    yaml.trim()
        .lines()
        .map(|line| {
            if line.trim().is_empty() {
                String::new()
            } else {
                format!("{pad}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parses a `middlewares:` list entry exactly as it appears in the reference — middleware
/// is only ever deserialized as part of an endpoint, never standalone.
fn middleware(entry_yaml: &str) -> Middleware {
    let doc = format!(
        "middlewares:\n{}\nmemory: {{ topic: \"reference_probe\" }}\n",
        indent(entry_yaml, 2)
    );
    let endpoint: Endpoint = serde_yaml_ng::from_str(&doc)
        .unwrap_or_else(|e| panic!("REFERENCE.md middleware snippet does not parse: {e}\n{doc}"));
    endpoint
        .middlewares
        .into_iter()
        .next()
        .expect("snippet should yield one middleware")
}

fn endpoint(yaml: &str) -> Endpoint {
    serde_yaml_ng::from_str(yaml)
        .unwrap_or_else(|e| panic!("REFERENCE.md endpoint snippet does not parse: {e}\n{yaml}"))
}

#[test]
fn documented_middleware_snippets_parse() {
    let snippets = [
        r#"- retry: { max_attempts: 5, initial_interval_ms: 200, max_interval_ms: 10000, multiplier: 2.0 }"#,
        r#"- dlq: { endpoint: { file: { path: "dead-letters.jsonl" } } }"#,
        r#"
- transform:
    mapping:
      firstName: "$.first_name"
      id: "$.user_id"
      "address.city": { path: "$.city", default: "unknown" }
    schema_file: "schemas/user.json"
"#,
        r#"
- transform:
    schema:
      type: object
      properties:
        payload:
          type: string
          contentMediaType: application/json
          contentSchema:
            type: object
            properties:
              qty: { type: integer }
"#,
        r#"- deduplication: { sled_path: "/var/lib/mq-bridge/dedup", ttl_seconds: 3600 }"#,
        r#"- deduplication: { store: "sled:///var/lib/mq-bridge/dedup", ttl_seconds: 3600 }"#,
        r#"- deduplication: { store: "mongodb://localhost:27017/etl", ttl_seconds: 3600 }"#,
        r#"- deduplication: { store: "postgres://user:pass@localhost/etl", ttl_seconds: 3600 }"#,
        r#"- weak_join: { group_by: "correlation_id", expected_count: 3, timeout_ms: 5000 }"#,
        r#"
- weak_join:
    group_by: "correlation_id"
    expected_count: 2
    timeout_ms: 5000
    branch_by: "source"
    required: ["inventory", "pricing"]
    on_timeout: discard
"#,
        r#"- buffer: { max_messages: 500, max_delay_ms: 20 }"#,
        r#"- limiter: { messages_per_second: 250 }"#,
        r#"- delay: { delay_ms: 100 }"#,
        r#"
- cookie_jar:
    shared_scope: "login-session"
    capture_metadata_keys: ["x-csrf-token"]
    export_metadata_prefix: "session."
"#,
        r#"- encryption: { key: "${env:MQB_ENC_KEY}" }"#,
        r#"
- encryption:
    cipher: aes256gcm
    key_id: "k2"
    key: "${env:MQB_ENC_KEY}"
    decrypt_keys: { k1: "${env:MQB_OLD_ENC_KEY}" }
"#,
        r#"- compression: { algorithm: zstd }"#,
        r#"- compression: { algorithm: gzip, max_decompressed_bytes: 1048576 }"#,
        r#"- metrics: {}"#,
        r#"- random_panic: { mode: disconnect, trigger_on_message: 500 }"#,
        r#"
- custom:
    name: "my_enricher"
    config: { lookup_url: "http://enrich.internal" }
"#,
    ];

    for snippet in snippets {
        middleware(snippet);
    }
}

#[test]
fn documented_structural_endpoint_snippets_parse() {
    let snippets = [
        r#"ref: "common_queue""#,
        r#"
fanout:
  - kafka: { topic: "audit", url: "localhost:9092" }
  - file: { path: "audit.jsonl" }
  - nats: { subject: "audit", url: "nats://localhost:4222" }
"#,
        r#"
switch:
  metadata_key: "http_status_code"
  cases:
    "200": { nats: { subject: "ok", url: "nats://localhost:4222" } }
    "404": { file: { path: "not-found.jsonl" } }
  default: { file: { path: "other.jsonl" } }
"#,
        r#"
request:
  to: { http: { url: "https://api.internal/score" } }
  forward_to: { ibmmq: { queue: "RESULTS", url: "mq(1414)", queue_manager: "QM1", channel: "APP.SVRCONN" } }
"#,
        r#"response: {}"#,
        r#"
reader:
  kafka: { topic: "queue", url: "localhost:9092" }
"#,
        r#"static: "OK""#,
        r#"
static:
  body: '{"status":"ok"}'
  raw: true
  metadata: { content-type: "application/json" }
"#,
        r#"
static:
  body: '{"error":"not found","id":"${message:id}","at":"${gen:now}"}'
  raw: true
  metadata: { content-type: "application/json" }
"#,
        r#"stream_buffer: { topic: "responses" }"#,
        r#"stream_buffer: { topic: "responses", correlation_id: "req-123" }"#,
        r#"
custom:
  name: "my_sink"
  config: { target: "internal://thing" }
"#,
    ];

    for snippet in snippets {
        endpoint(snippet);
    }
}

/// The at-rest compress-then-encrypt example from the `encryption` section.
#[test]
fn documented_at_rest_encryption_snippet_parses() {
    let ep = endpoint(
        r#"
file:
  path: "data.enc"
  format: raw
  compression: lz4
  encryption: { key: "${env:MQB_ENC_KEY}" }
"#,
    );
    assert_eq!(ep.endpoint_type.name(), "file");
}

/// `null` is the one endpoint whose YAML spelling is a trap. A bare YAML null is the form
/// that works; `null: {}` does *not* parse. This pins what the reference documents.
#[test]
fn documented_null_endpoint_spelling_parses() {
    assert_eq!(endpoint("null").endpoint_type.name(), "null");
    assert_eq!(endpoint("null: null").endpoint_type.name(), "null");

    let rejected: Result<Endpoint, _> = serde_yaml_ng::from_str("null: {}");
    assert!(
        rejected.is_err(),
        "`null: {{}}` must stay documented as invalid"
    );
}

/// The reference states that omitting `output` defaults it to `null`.
#[test]
fn omitted_output_defaults_to_null_as_documented() {
    let route: mq_bridge::models::Route = serde_yaml_ng::from_str(
        r#"
input:
  memory: { topic: "in" }
"#,
    )
    .unwrap();
    assert_eq!(route.output.endpoint_type.name(), "null");
}

/// Full route shapes used as examples in the reference.
#[test]
fn documented_route_snippets_parse() {
    let routes = [
        r#"
input:
  middlewares:
    - deduplication: { sled_path: "/var/lib/mqb/dedup", ttl_seconds: 3600 }
  kafka: { topic: "orders", url: "localhost:9092" }
output:
  middlewares:
    - retry: { max_attempts: 5 }
    - dlq: { endpoint: { file: { path: "failed.jsonl" } } }
  nats: { subject: "orders.processed", url: "nats://localhost:4222" }
"#,
        r#"
input: { http: { url: "0.0.0.0:8080" } }
output: { response: {} }
"#,
        r#"
input: { ref: "common_queue" }
output: { nats: { subject: "enriched", url: "nats://localhost:4222" } }
"#,
        r#"
input: { kafka: { topic: "noisy", url: "localhost:9092" } }
output: null
"#,
    ];

    for yaml in routes {
        let _: mq_bridge::models::Route = serde_yaml_ng::from_str(yaml)
            .unwrap_or_else(|e| panic!("REFERENCE.md route snippet does not parse: {e}\n{yaml}"));
    }
}

/// The reference distinguishes "wrong side, warns and is skipped" from "wrong side, hard
/// error". Both behaviours are pinned here so the table stays accurate.
#[tokio::test]
async fn wrong_side_middleware_matches_documented_behaviour() {
    use mq_bridge::models::{EndpointType, WeakJoinMiddleware};

    async fn publisher_for(middlewares: Vec<Middleware>) -> anyhow::Result<()> {
        let mut output = Endpoint::new_memory("reference_docs_wrong_side", 10);
        output.middlewares = middlewares;
        let inner = match &output.endpoint_type {
            EndpointType::Memory(cfg) => {
                mq_bridge::endpoints::memory::MemoryPublisher::new_async(cfg)
                    .await
                    .unwrap()
            }
            _ => unreachable!(),
        };
        mq_bridge::middleware::apply_middlewares_to_publisher(
            Box::new(inner),
            &output,
            "reference_docs_route",
        )
        .await
        .map(|_| ())
    }

    // Documented as a hard error on the publisher side.
    let weak_join = publisher_for(vec![Middleware::WeakJoin(WeakJoinMiddleware {
        group_by: "correlation_id".to_string(),
        expected_count: 2,
        timeout_ms: 1000,
        branch_by: None,
        required: Vec::new(),
        on_timeout: Default::default(),
    })])
    .await;
    assert!(
        weak_join.is_err(),
        "weak_join on an output must fail at startup, as documented"
    );

    // Documented as warn-and-skip on the consumer side: the route still starts.
    let mut input = Endpoint::new_memory("reference_docs_wrong_side_in", 10);
    input.middlewares = vec![Middleware::Retry(Default::default())];
    let consumer = match &input.endpoint_type {
        EndpointType::Memory(cfg) => mq_bridge::endpoints::memory::MemoryConsumer::new_async(cfg)
            .await
            .unwrap(),
        _ => unreachable!(),
    };
    assert!(
        mq_bridge::middleware::apply_middlewares_to_consumer(
            Box::new(consumer),
            &input,
            "reference_docs_route",
        )
        .await
        .is_ok(),
        "retry on an input must warn and be skipped, not fail"
    );
}

/// The ordering rule the reference leads with: for publishers the last middleware in the
/// list is the outermost layer. Pinned here because every config that combines `dlq` with
/// anything else depends on it, and it is the reverse of the consumer side.
#[tokio::test]
async fn publisher_middleware_wraps_last_entry_outermost() {
    use mq_bridge::models::{
        DeadLetterQueueMiddleware, EndpointType, FaultMode, RandomPanicMiddleware,
    };
    use mq_bridge::CanonicalMessage;

    // `enabled` must be set explicitly: the derived Default is false, while the serde
    // default used when parsing YAML is true.
    let always_fail = RandomPanicMiddleware {
        mode: FaultMode::JsonFormatError,
        enabled: true,
        ..Default::default()
    };

    let dlq_endpoint = Endpoint::new_memory("reference_docs_dlq", 10);
    let mut output = Endpoint::new_memory("reference_docs_main", 10);
    output.middlewares = vec![
        Middleware::RandomPanic(always_fail),
        Middleware::Dlq(Box::new(DeadLetterQueueMiddleware {
            endpoint: dlq_endpoint.clone(),
        })),
    ];

    let inner = match &output.endpoint_type {
        EndpointType::Memory(cfg) => mq_bridge::endpoints::memory::MemoryPublisher::new_async(cfg)
            .await
            .unwrap(),
        _ => unreachable!(),
    };

    let publisher = mq_bridge::middleware::apply_middlewares_to_publisher(
        Box::new(inner),
        &output,
        "reference_docs_route",
    )
    .await
    .unwrap();

    // With `dlq` last (outermost) it catches the failure instead of the error escaping.
    publisher
        .send(CanonicalMessage::from("payload"))
        .await
        .expect("dlq listed last should capture the failure");

    assert_eq!(
        dlq_endpoint.channel().unwrap().drain_messages().len(),
        1,
        "the failed message should have been dead-lettered"
    );
}
