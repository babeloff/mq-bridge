# Middleware & Structural Endpoint Reference

Complete listing of every **middleware** and every **structural endpoint** mq-bridge ships.

Structural endpoints are the ones that do not talk to a broker or store: they compose other
endpoints, shape routing, or terminate a request. Data endpoints (`kafka`, `nats`, `mqtt`,
`sqlx`, …) are covered in [README.md](README.md#backend-features--configuration) and
[CONFIGURATION.md](CONFIGURATION.md).

- [Middleware](#middleware)
- [Structural endpoints](#structural-endpoints)

---

## Middleware

Middleware attaches to an endpoint via a `middlewares:` list, on the **input**, the
**output**, or both:

```yaml
my_route:
  input:
    middlewares:
      - deduplication: { sled_path: "/var/lib/mqb/dedup", ttl_seconds: 3600 }
    kafka: { topic: "orders", url: "localhost:9092" }
  output:
    middlewares:
      - retry: { max_attempts: 5 }
      - dlq: { endpoint: { file: { path: "failed.jsonl" } } }
    nats: { subject: "orders.processed", url: "nats://localhost:4222" }
```

### Ordering — read this before combining middleware

**Output (publisher) middlewares wrap in list order, so the *last* entry is the outermost
layer and sees the failures of the ones before it.** Put `dlq` last.

**Input (consumer) middlewares are applied in reverse, so the *first* entry is outermost**
and runs first on an incoming message.

> This is asserted by `route::tests::test_dlq_and_retry_batch_integration`,
> `middleware::transform::tests::test_rejected_message_reaches_the_dlq_through_the_config_wiring`,
> and `reference_docs_test::publisher_middleware_wraps_last_entry_outermost`, and is
> documented on `apply_middlewares_to_publisher` in `src/middleware/mod.rs`.

```yaml
# Correct: transform rejects -> retry gives up -> dlq captures.
middlewares:
  - transform: { schema_file: "user.json" }
  - retry: { max_attempts: 3 }
  - dlq: { endpoint: { file: { path: "rejected.jsonl" } } }
```

### What exists

| Name | Input | Output | Feature | Purpose |
|---|:---:|:---:|---|---|
| [`retry`](#retry) | – | ✅ | – | Exponential-backoff retry of failed sends |
| [`dlq`](#dlq) | – | ✅ | – | Route permanently-failed messages to another endpoint |
| [`transform`](#transform) | ✅ | ✅ | – | Declarative JSON mapping, coercion, validation |
| [`deduplication`](#deduplication) | ✅ | – | `dedup` | Drop repeated message IDs within a TTL |
| [`weak_join`](#weak_join) | ✅ | – | – | Correlate and join related messages |
| [`buffer`](#buffer) | ✅ | ✅ | – | Coalesce single sends into batches |
| [`limiter`](#limiter) | ✅ | ✅ | – | Cap throughput to a message rate |
| [`delay`](#delay) | ✅ | ✅ | – | Fixed delay per receive/send |
| [`cookie_jar`](#cookie_jar) | ✅ | ✅ | – | Persist HTTP cookies / session values across messages |
| [`metrics`](#metrics) | ✅ | ✅ | `metrics` | Emit throughput/latency/error metrics |
| [`random_panic`](#random_panic) | ✅ | ✅ | – | Fault injection for testing |
| [`custom`](#custom-middleware) | ✅ | ✅ | – | Your own middleware via a registered factory |

**Putting a middleware on the wrong side behaves in two different ways**, so check the table
above rather than assuming:

- `deduplication` on an output, and `dlq` / `retry` on an input, log a warning and are
  skipped. The route still starts.
- `weak_join` on an output is a **hard startup error** (`Unsupported publisher middleware`).

A middleware whose feature is not compiled in (`deduplication` without `dedup`, `metrics`
without `metrics`) is likewise a startup error, not a silent no-op.

---

### `retry`

Retries failed sends with exponential backoff. Output only.

| Field | Type | Default |
|---|---|---|
| `max_attempts` | integer | `3` |
| `initial_interval_ms` | integer | `100` |
| `max_interval_ms` | integer | `5000` |
| `multiplier` | float | `2.0` |

```yaml
- retry: { max_attempts: 5, initial_interval_ms: 200, max_interval_ms: 10000, multiplier: 2.0 }
```

Only `Retryable` and connection errors are retried; `NonRetryable` failures pass straight
through. Once attempts are exhausted the error is marked so a following `dlq` treats it as
permanent. Pair the two.

### `dlq`

Sends permanently-failed messages to a separate endpoint instead of failing the batch. Output only.

| Field | Type | Required |
|---|---|---|
| `endpoint` | Endpoint | yes |

```yaml
- dlq:
    endpoint:
      file: { path: "dead-letters.jsonl" }
```

Captures `NonRetryable` failures and `Retryable` ones whose retries are exhausted. Connection
errors are **not** dead-lettered — they propagate so the route can reconnect. The DLQ endpoint
is a full endpoint, so it can itself have middleware. If the DLQ send fails with a connection
error that error propagates rather than silently dropping the message.

### `transform`

Declarative JSON reshaping: field mapping, then schema-directed coercion, defaults and
validation — over a single parse. Input and output.

| Field | Type | Default |
|---|---|---|
| `mapping` | map of output field → rule | `{}` |
| `schema` | inline JSON Schema subset | – |
| `schema_file` | path to a schema file | – |
| `coerce` | bool | `true` |
| `apply_defaults` | bool | `true` |
| `on_error` | `reject` \| `pass_through` | `reject` |

`schema` and `schema_file` are mutually exclusive. A mapping rule is either a bare path
string or `{ path, default, required }`.

```yaml
- transform:
    mapping:
      firstName: "$.first_name"
      id: "$.user_id"
      "address.city": { path: "$.city", default: "unknown" }
    schema_file: "schemas/user.json"
```

Paths accept `$.field`, `$.a.b`, and `$.items[0]`; the `$.` prefix is optional. Dots in the
*output* key nest the result. An absent optional source field is omitted rather than emitted
as null.

Schema keywords honoured: `type`, `properties`, `required`, `default`, `items`, `nullable`
(also `"type": ["string","null"]`), `enum`. Everything else is ignored, so an existing fuller
schema can be used as-is. Coercions are limited to the lossless ones: `string → integer`,
`string → number`, `string → boolean` (`true`/`false`/`1`/`0`), `number → string`.

Failures are always non-retryable and name the field, e.g.
`transform failed at $.items[1].qty [coercion]: cannot coerce string "oops" to integer`.
On an **output** endpoint the message is failed so a following `dlq` captures it; on an
**input** endpoint it is dropped from the batch and acknowledged, keeping invalid data out of
the route. `on_error: pass_through` instead forwards the original payload with the reason in
the `mqb.transform_error` metadata key, which a [`switch`](#switch) can route on.

Schemas and paths compile once at startup; `schema_file` is read a single time. A `transform`
with neither stage configured leaves the payload untouched without parsing it.

### `deduplication`

Drops messages whose ID was already seen within the TTL. Input only. Requires the `dedup`
feature (pulls `sled`).

| Field | Type | Required |
|---|---|---|
| `sled_path` | string | yes |
| `ttl_seconds` | integer | yes |

```yaml
- deduplication: { sled_path: "/var/lib/mq-bridge/dedup", ttl_seconds: 3600 }
```

State is a local sled database, so deduplication is per-process, not cluster-wide.

### `weak_join`

Correlates messages by a metadata key and emits them as one joined message. Input only.

| Field | Type | Default |
|---|---|---|
| `group_by` | string (metadata key) | required |
| `expected_count` | integer | required |
| `timeout_ms` | integer | required |
| `branch_by` | string (metadata key) | – |
| `required` | list of branch names | `[]` |
| `on_timeout` | `fire` \| `discard` | `fire` |

```yaml
# Count mode: wait for any 3 messages sharing a correlation_id, emit a JSON array.
- weak_join: { group_by: "correlation_id", expected_count: 3, timeout_ms: 5000 }

# Branch mode: wait for named branches, emit a branch-keyed JSON object.
- weak_join:
    group_by: "correlation_id"
    expected_count: 2
    timeout_ms: 5000
    branch_by: "source"
    required: ["inventory", "pricing"]
    on_timeout: discard
```

Setting `branch_by` switches to branch mode, where `required` overrides `expected_count`.
On timeout an incomplete group is either emitted partially (`fire`) or dropped (`discard`).
Messages are acknowledged on receipt, so a crash before the group completes loses the
buffered members.

### `buffer`

Accumulates single sends and forwards them as one batch. Input and output.

| Field | Type | Required |
|---|---|---|
| `max_messages` | integer | yes |
| `max_delay_ms` | integer | yes |

```yaml
- buffer: { max_messages: 500, max_delay_ms: 20 }
```

Flushes when either bound is hit. Useful in front of an endpoint whose per-call overhead
dominates. Adds up to `max_delay_ms` of latency.

### `limiter`

Paces throughput to a target rate. Input and output.

| Field | Type | Required |
|---|---|---|
| `messages_per_second` | float (> 0) | yes |

```yaml
- limiter: { messages_per_second: 250 }
```

Best-effort pacing that accounts for batch size, not just call count.

### `delay`

Sleeps a fixed duration before each receive or send. Input and output.

| Field | Type | Required |
|---|---|---|
| `delay_ms` | integer | yes |

```yaml
- delay: { delay_ms: 100 }
```

Mainly for testing and for crude pacing of a downstream system; prefer
[`limiter`](#limiter) for real rate control.

### `cookie_jar`

Persists HTTP cookies and arbitrary session values across messages. Input and output.

| Field | Type | Default |
|---|---|---|
| `shared_scope` | string | – (per-instance store) |
| `cookie_metadata_key` | string | `cookie` |
| `set_cookie_metadata_key` | string | `set-cookie` |
| `capture_metadata_keys` | list of strings | `[]` |
| `export_metadata_prefix` | string | – |
| `inject_metadata` | map string→string | `{}` |

```yaml
- cookie_jar:
    shared_scope: "login-session"
    capture_metadata_keys: ["x-csrf-token"]
    export_metadata_prefix: "session."
```

Reads `set-cookie` from responses and injects `cookie` into later requests. With
`shared_scope`, instances using the same name share one store across endpoints and routes in
the process — that is how a login route and a data route reuse one session.

### `metrics`

Emits throughput, latency and error metrics for the endpoint. Input and output. Requires the
`metrics` feature. Takes no options; its presence enables collection.

```yaml
- metrics: {}
```

Input and output are labelled separately, so attaching it to both sides is meaningful.

### `random_panic`

Deliberate fault injection for testing recovery paths. Input and output.

| Field | Type | Default |
|---|---|---|
| `mode` | `panic` \| `disconnect` \| `timeout` \| `json_format_error` \| `nack` | `panic` |
| `trigger_on_message` | integer (1-indexed) | – (every message) |
| `enabled` | bool | `true` |

```yaml
- random_panic: { mode: disconnect, trigger_on_message: 500 }
```

`disconnect` and `timeout` produce retryable errors; `json_format_error` produces a
non-retryable one — useful for exercising a `dlq`. Keep `enabled: false` in committed configs
rather than deleting the block.

### `custom` (middleware)

Delegates to a factory you registered programmatically.

| Field | Type | Required |
|---|---|---|
| `name` | string | yes |
| `config` | any JSON | yes |

```yaml
- custom:
    name: "my_enricher"
    config: { lookup_url: "http://enrich.internal" }
```

Implement `CustomMiddlewareFactory` (`apply_consumer` and/or `apply_publisher`, each
defaulting to pass-through) and register it before starting routes. See
[ARCHITECTURE.md](ARCHITECTURE.md#extending-mq-bridge).

---

## Structural endpoints

These appear wherever an endpoint is expected — as a route `input`/`output`, or nested inside
another structural endpoint.

| Name | Input | Output | Purpose |
|---|:---:|:---:|---|
| [`ref`](#ref) | ✅ | ✅ | Reuse an endpoint defined elsewhere by name |
| [`fanout`](#fanout) | – | ✅ | Send every message to all listed endpoints |
| [`switch`](#switch) | – | ✅ | Content-based routing on a metadata key |
| [`request`](#request) | – | ✅ | Call a request/reply endpoint, forward the response onward |
| [`response`](#response) | – | ✅ | Reply to the origin of the current request |
| [`reader`](#reader) | – | ✅ | Use an incoming message as a trigger to pull from a consumer |
| [`static`](#static) | ✅ | ✅ | Fixed, pre-rendered message |
| [`stream_buffer`](#stream_buffer) | ✅ | ✅ | Correlation-partitioned in-memory stream |
| [`null`](#null) | – | ✅ | Discard everything |
| [`custom`](#custom-endpoint) | ✅ | ✅ | Your own endpoint via a registered factory |

### `ref`

Reuses an endpoint registered under a name, instead of repeating its configuration.

The name is a **registry key, not a topic name**. Register it from Rust before starting the
routes:

```rust
use mq_bridge::models::Endpoint;
use mq_bridge::route::register_endpoint;

register_endpoint("common_queue", Endpoint::new_memory("shared_memory_topic", 100));
```

```yaml
enrich:
  input: { ref: "common_queue" }
  output: { nats: { subject: "enriched", url: "nats://localhost:4222" } }
```

A route can also publish its own output under a name with
`Route::register_output_endpoint(Some("name"))`, which is how one route's output becomes
another's input.

The value is a bare string. Resolution looks in the endpoint registry first, then in
registered publishers. Middleware on the `ref` itself is applied **outside** the referenced
endpoint's own middleware. Circular references are detected and rejected at startup, and
nesting depth is bounded.

### `fanout`

Publishes each message to every listed endpoint. Output only.

```yaml
output:
  fanout:
    - kafka: { topic: "audit", url: "localhost:9092" }
    - file: { path: "audit.jsonl" }
    - nats: { subject: "audit", url: "nats://localhost:4222" }
```

The value is a plain list of endpoints, each of which may have its own middleware and may
itself be structural. All branches receive the same message.

### `switch`

Content-based routing: picks a destination by the value of a **metadata key**.

| Field | Type | Required |
|---|---|---|
| `metadata_key` | string | yes |
| `cases` | map value → Endpoint | yes |
| `default` | Endpoint | no |

```yaml
output:
  switch:
    metadata_key: "http_status_code"
    cases:
      "200": { nats: { subject: "ok", url: "nats://localhost:4222" } }
      "404": { file: { path: "not-found.jsonl" } }
    default: { file: { path: "other.jsonl" } }
```

Matching is on the **metadata** value, not the payload — it does not read JSON fields. To
route on payload content, first promote the value into metadata (for example with
[`transform`](#transform)'s `on_error: pass_through`, which sets `mqb.transform_error`, or an
endpoint that emits a status key such as `http_status_code`). A message whose key is missing
or unmatched goes to `default`; without a `default` it is dropped.

### `request`

Sends each message to a request-capable endpoint and forwards the **response** somewhere else,
turning a request/reply exchange into a one-way flow.

| Field | Type | Required |
|---|---|---|
| `to` | Endpoint (request-capable) | yes |
| `forward_to` | Endpoint | yes |

```yaml
output:
  request:
    to: { http: { url: "https://api.internal/score" } }
    forward_to: { ibmmq: { queue: "RESULTS", url: "mq(1414)", queue_manager: "QM1", channel: "APP.SVRCONN" } }
```

`to` must support request/reply: `http`, or a `nats`/`mongodb`/`memory` endpoint with
`request_reply: true`. On error or timeout the **original** message is forwarded instead of a
response, so nothing is lost — distinguish the two downstream with a [`switch`](#switch) on a
status key such as `http_status_code`.

### `response`

Replies to the origin of the current request. Output only, and the recommended way to build
request/reply routes.

```yaml
http_echo:
  input: { http: { url: "0.0.0.0:8080" } }
  output: { response: {} }
```

Takes no options. Requires an input that carries a reply channel (`http`, `websocket`, `grpc`,
or a request/reply `nats`/`mongodb`/`memory`). With an `http` or `websocket` input and no
middleware, `response` (and `static`) enables an inline fast path that skips the normal route
pipeline. See [README.md](README.md#patterns-request-response).

### `reader`

An output endpoint that **ignores the incoming payload** and instead reads one message from
the wrapped consumer, returning it as the response. The inbound message is purely a trigger.

```yaml
# HTTP GET pulls the next message off a Kafka topic.
poll_api:
  input: { http: { url: "0.0.0.0:8080", method: "GET" } }
  output:
    reader:
      kafka: { topic: "queue", url: "localhost:9092" }
```

The value is a single nested endpoint, which must be valid as a **consumer**. The message read
is acknowledged immediately, before the caller has necessarily received it — so a crash in
between loses it. Use it for polling APIs, not for guaranteed delivery.

### `static`

A fixed, pre-rendered message. Usable as an output (a constant reply) or an input (a constant
source).

| Field | Type | Default |
|---|---|---|
| `body` | string | required |
| `raw` | bool | `false` |
| `metadata` | map string→string | `{}` |

Accepts either a bare string or the full map form:

```yaml
output: { static: "OK" }                       # shorthand, body JSON-encoded

output:
  static:
    body: '{"status":"ok"}'
    raw: true                                  # send verbatim, do not JSON-encode
    metadata: { content-type: "application/json" }
```

`raw: true` sends `body` byte-for-byte; the default JSON-encodes it as a string. Like
`response`, a `static` output enables the HTTP inline fast path.

### `stream_buffer`

An in-memory stream partitioned by correlation ID, used to carry streaming request/response
bodies between routes.

| Field | Type | Notes |
|---|---|---|
| `topic` | string | required; shared by publisher and consumers |
| `correlation_id` | string | **required on consumers, must be unset on publishers** |
| `capacity` | integer | default `100`, per partition |

```yaml
output:
  stream_buffer: { topic: "responses" }        # publisher: no correlation_id

input:
  stream_buffer: { topic: "responses", correlation_id: "req-123" }   # consumer
```

A consumer without `correlation_id` is a startup error; a publisher *with* one logs a warning
and ignores it. Primarily wired up via `HttpConfig::stream_response_to`.

### `null`

Discards every message. Output only. This is the **default output** when a route omits one.

```yaml
drain:
  input: { kafka: { topic: "noisy", url: "localhost:9092" } }
  output: null          # a bare YAML null
```

> Spelling trap: it is a bare YAML `null` (or `~`, or the explicit `null: null`).
> **`null: {}` does not parse.** Omitting `output:` entirely gives the same result.

Useful for consume-and-handle routes where a handler does the work and there is nothing to
forward, and for benchmarking an input in isolation.

### `custom` (endpoint)

Delegates to a factory you registered programmatically.

| Field | Type | Required |
|---|---|---|
| `name` | string | yes |
| `config` | any JSON | yes |

```yaml
output:
  custom:
    name: "my_sink"
    config: { target: "internal://thing" }
```

Implement `CustomEndpointFactory` and register it before starting routes. See
[ARCHITECTURE.md](ARCHITECTURE.md#extending-mq-bridge).

---

## See also

- [README.md](README.md) — overview, data endpoints, request/response and CQRS patterns
- [CONFIGURATION.md](CONFIGURATION.md) — full YAML examples, env vars, TLS, IDE schema validation
- [ARCHITECTURE.md](ARCHITECTURE.md) — internals, batching/concurrency, extension traits
