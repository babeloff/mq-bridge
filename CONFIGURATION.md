# Configuration Guide

`mq-bridge` uses a flexible configuration system supporting YAML, JSON, and environment variables.

## Configuration Reference

The best way to understand the configuration structure is through a comprehensive example. `mq-bridge` uses a YAML map where keys are route names.

```yaml
# mq-bridge.yaml

# Route 1: Kafka to NATS
kafka_to_nats:
  concurrency: 4
  input:
    kafka:
      url: "localhost:9092"
      topic: "orders"
      group_id: "bridge_group"
      # TLS Configuration (Optional)
      tls:
        required: true
        ca_file: "./certs/ca.pem"
  output:
    nats:
      url: "nats://localhost:4222"
      subject: "orders_stream.processed"
      stream: "orders_stream"

# Route 2: HTTP Webhook to MongoDB with Middleware
webhook_to_mongo:
  input:
    http:
      url: "0.0.0.0:8080"
      # Force the normal route pipeline instead of the inline HTTP response fast path.
      inline_response_fast_path: false
    middlewares:
      - retry:
          max_attempts: 3
          initial_interval_ms: 500
  output:
    mongodb:
      url: "mongodb://localhost:27017"
      database: "app_db"
      collection: "webhooks"
      format: "json" # a bit slower, but better readability

# Route 3: File to AMQP (RabbitMQ)
file_ingest:
  input:
    file:
      path: "./data/input.jsonl"
  output:
    amqp:
      url: "amqp://localhost:5672"
      exchange: "logs"
      queue: "file_logs"

# Route 4: AWS SQS to SNS
aws_sqs_to_sns:
  input:
    aws:
      # To consume from SNS, subscribe this SQS queue to the SNS topic in AWS Console/Terraform.
      queue_url: "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue"
      region: "us-east-1"
      # Credentials (optional if using env vars or IAM roles)
      access_key: "test"
      secret_key: "test"
  output:
    aws:
      topic_arn: "arn:aws:sns:us-east-1:000000000000:my-topic"
      region: "us-east-1"

# Route 5: IBM MQ Example
ibm_mq_route:
  input:
    ibmmq:
      queue_manager: "QM1"
      url: "localhost(1414)"
      channel: "DEV.APP.SVRCONN"
      queue: "DEV.QUEUE.1"
      username: "app"
      password: "admin"
  output:
    memory:
      topic: "received_from_mq"

# Route 6: MQTT to Switch (Content-based Routing)
iot_router:
  input:
    mqtt:
      url: "mqtt://localhost:1883"
      topic: "sensors/+"
      qos: 1
  output:
    switch:
      metadata_key: "sensor_type"
      cases:
        temp:
          kafka:
            url: "localhost:9092"
            topic: "temperature"
      default:
        memory:
          topic: "dropped_sensors"

# Route 7: ZeroMQ PUSH/PULL
zeromq_pipeline:
  input:
    zeromq:
      url: "tcp://0.0.0.0:5555"
      socket_type: "pull"
      bind: true
  output:
    zeromq:
      url: "tcp://localhost:5556"
      socket_type: "push"
      bind: false

# Route 8: PostgreSQL via SQLx
sqlx_postgres_route:
  input:
    sqlx:
      url: "postgres://user:pass@localhost:5432/mydb"
      table: "job_queue"
      delete_after_read: true
  output:
    memory:
      topic: "processed_jobs"
```

## Configuration Details

### Environment Variables
All YAML configuration can be overridden with environment variables. The mapping follows this pattern:
`MQB__{ROUTE_NAME}__{PATH_TO_SETTING}`

For example, to set the Kafka topic for the `kafka_to_nats` route:
```sh
export MQB__KAFKA_TO_NATS__INPUT__KAFKA__TOPIC="my-other-topic"
```

### NATS JetStream Notes

Two gotchas worth knowing before wiring up a `nats` endpoint:

- **Subject must be prefixed with the stream name.** When mq-bridge auto-creates
  a JetStream stream (no existing stream already covers the subject), it scopes
  the stream to `{stream}.>`. So `stream: "orders_stream"` requires a subject
  like `orders_stream.foo` — a subject such as `orders.foo` will fail to publish
  with "no stream found for given subject". This only applies to
  auto-creation; publishing to a stream that already exists with a wider
  subject filter works regardless of naming.
- **`stream` is required even in Core NATS mode.** Consumer validation requires
  a `stream` value even when `no_jetstream: true`. It's unused for the actual
  Core NATS subscribe, but validation still rejects a missing value — pass any
  placeholder string.

### Middleware Configuration
Middleware is defined as a list under an endpoint.

```yaml
input:
  middlewares:
    - retry:
        max_attempts: 5
        initial_interval_ms: 200
    - dlq:
        endpoint:
          nats:
            subject: "my-dlq-subject"
            url: "nats://localhost:4222"
    - deduplication:
        sled_path: "/var/data/mq-bridge/dedup_db"
        ttl_seconds: 3600 # 1 hour
  kafka:
    # ... kafka config
```

### TLS & Security Hardening

Most endpoints accept a `tls` block. The available fields are:

```yaml
tls:
  required: true                   # enable TLS
  ca_file: "./certs/ca.pem"        # CA to verify the server
  cert_file: "./certs/client.pem"  # client cert (mTLS)
  key_file: "./certs/client.key"   # client private key (mTLS)
  cert_password: "secret"          # password for an encrypted key (where supported)
  accept_invalid_certs: false      # NEVER set true in production
```

**Hardening checklist (e.g. for PCI-DSS Req 4.2.1):**

1. **Enable TLS on every endpoint carrying sensitive data** (`required: true`) and supply
   a `ca_file`. Use mTLS (`cert_file` + `key_file`) for mutual authentication where the
   broker supports it.
2. **Never disable certificate validation.** `accept_invalid_certs` defaults to `false`;
   leaving it that way is required — setting it `true` on a sensitive path defeats TLS.
3. **Choose the crypto provider feature.** Build with `rustls-aws-lc` (FIPS-capable, also
   enables post-quantum key exchange) or `rustls-ring`. The rustls-based endpoints — NATS,
   MQTT, HTTP, gRPC, WebSocket, AMQP — only ever negotiate rustls's safe TLS 1.2/1.3 AEAD
   cipher suites; weak/legacy suites (RC4, 3DES, CBC-SHA1, export) cannot be offered, so
   "strong ciphers only" holds without any explicit cipher list.
4. **Kafka** (librdkafka/OpenSSL): certificate verification is on by default
   (`enable.ssl.certificate.verification` follows `accept_invalid_certs`). If an auditor
   requires an explicit allowlist, pin it via `producer_options` / `consumer_options`:
   ```yaml
   kafka:
     producer_options: [["ssl.cipher.suites", "ECDHE-RSA-AES256-GCM-SHA384"]]
     consumer_options: [["ssl.cipher.suites", "ECDHE-RSA-AES256-GCM-SHA384"]]
   ```
5. **IBM MQ** (native stack): set a strong `cipher_spec` (a TLS 1.2/1.3 CipherSpec) — it is
   required for encrypted connections.
6. **Keep sensitive payloads out of logs.** Message payloads are emitted at `trace` level;
   run production above `trace` and confirm no cardholder data (PAN) reaches logs or traces.
7. **Do not commit secrets.** Source passwords and tokens from a secrets manager or env vars
   (`MQB__...`) rather than checked-in config.

**Notes and boundaries:**

- TLS 1.3 alone is sufficient in 2026 (Mozilla "Modern" profile). The rustls endpoints
  currently negotiate **TLS 1.2 and 1.3** (both are PCI-acceptable). A central
  "TLS 1.3-only" toggle is not yet configurable in the library; enforce a minimum protocol
  version on the broker/server side, which is the side that accepts the connection.
- Kafka and IBM MQ use native TLS stacks, so a library-wide version policy cannot be
  applied to them — configure their minimum TLS version on the broker.

### HTTP Consumer Fast Path

Compatible `http -> response` routes may use an inline response fast path for lower latency. This bypasses the normal route consumer/worker/disposition pipeline, but it still keeps the output publisher chain active, including output handlers and allowed output middlewares.

The fast path is only considered when:

- the input has no middlewares
- `receive_streamable` is `false`
- `fire_and_forget` is `false`
- output middlewares are limited to `buffer`, `delay`, `limiter`, and/or `metrics`

To force the normal route pipeline, set this on the HTTP consumer:

```yaml
input:
  http:
    url: "0.0.0.0:8080"
    inline_response_fast_path: false
```

This is useful when you want stable, explicit semantics regardless of future optimizations, or when you want to avoid the inline path's response behavior differences. In particular, the inline path does not automatically echo unchanged request metadata back as HTTP response headers.

### Connection Sharing

Publishers that target the same server reuse one underlying transport client by
default, instead of each opening its own. This consolidates TCP connections, background
threads, and batching, and follows each driver's own guidance (one shared producer /
client / pool per application). Sharing applies to **Kafka, NATS, MongoDB, SQLx, HTTP,
and gRPC**; the client is keyed by its connection-level settings (URL, auth, TLS, and
client-level options), never by topic/subject/collection. A shared client is released
once the last publisher using it is dropped.

Set `shared: false` on a publisher to give it a dedicated connection:

```yaml
orders_out:
  output:
    kafka:
      topic: "orders"
      url: "localhost:9092"
      shared: false   # dedicated producer — keeps this latency-sensitive topic off a busy producer's queue
```

*   **Kafka**: a single producer serves every topic and is the recommended setup. Use
    `shared: false` to isolate a latency-sensitive topic from a high-throughput one so
    they don't share one internal send queue (head-of-line blocking).
*   **SQLx**: a shared pool means its `max_connections` is a budget shared across every
    route using that database. Use `shared: false` if a route needs its own pool.
*   **gRPC**: a shared channel multiplexes over one HTTP/2 connection; at very high
    concurrency its max-concurrent-streams cap can bottleneck — `shared: false` gives a
    dedicated channel.

### Specialized Endpoints

#### Switch

The `switch` endpoint is a conditional publisher that routes messages to different outputs based on a metadata key.

It checks the specified `metadata_key` in each message. If the key's value matches one of the `cases`, the message is forwarded to that endpoint. If no case matches, it's sent to the `default` endpoint. If there is no default, the message is dropped.

This is useful for content-based routing.

**Example**: Route orders to different systems based on `country_code` metadata.

```yaml
output:
  switch:
    metadata_key: "country_code"
    cases:
      US:
        kafka:
          topic: "us_orders"
          url: "kafka-us:9092"
      EU:
        nats:
          subject: "eu_orders"
          url: "nats-eu:4222"
    default:
      file:
        path: "/var/data/unroutable_orders.log"
```

### IDE Support (Schema Validation) 
mq-bridge includes a JSON schema for configuration validation and auto-completion. 
1. Ensure you have a YAML plugin installed (e.g., YAML for VS Code). 
2. Configure your editor to reference the schema. For VS Code, add this to .vscode/settings.json: 
```json 
{ 
  "yaml.schemas": { 
    "https://raw.githubusercontent.com/marcomq/mq-bridge/main/mq-bridge.schema.json": ["mq-bridge.yaml", "config.yaml"]
  } 
} 
```
To regenerate the schema from this repo, run: `cargo test --features schema`
