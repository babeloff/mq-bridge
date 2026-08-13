# Middleware & Structural Endpoint Reference

Complete listing of every **middleware** and every **structural endpoint** mq-bridge ships.

Structural endpoints are the ones that do not talk to a broker or store: they compose other
endpoints, shape routing, or terminate a request. Data endpoints (`kafka`, `nats`, `mqtt`,
`sqlx`, …) are covered in [README.md](../README.md#backend-features--configuration) and
[CONFIGURATION.md](CONFIGURATION.md).

- [Middleware](#middleware)
- [Structural endpoints](#structural-endpoints)

---

## Middleware

Middleware attaches to an endpoint via a `middlewares:` list, on the **input**, the
**output**, or both:

```yaml route
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

**Consequence: a route that reads back what another route wrote needs the *reversed* list.**
Writing with `[compression, encryption]` produces `compress(encrypt(payload))`; a reader
given that same list would try to decrypt first and fail. The reading route must say
`[encryption, compression]`. The lists mirror — they are not copied.

**A route handler sits outside every output middleware**, so it runs **once** per message
and the middlewares act on what it returned. In particular `retry` re-attempts only the
publish, never the handler — a handler with a side effect fires once however many times the
sink is retried. The trade: a `dlq` cannot capture a handler failure, only a send failure;
a handler error propagates to the route and is reported there.

> This is asserted by `route::tests::test_retryable_handler_error_is_not_retried_by_output_middleware`
> (the handler runs once and `retry` does not re-run it),
> `route::tests::test_dlq_and_retry_batch_integration`,
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
| [`id`](#id) | ✅ | – | – | Derive a replay-stable business identity into `mqb.id` |
| [`deduplication`](#deduplication) | ✅ | – | `dedup` | Drop repeated keys within a TTL |
| [`weak_join`](#weak_join) | ✅ | – | – | Correlate and join related messages |
| [`buffer`](#buffer) | ✅ | ✅ | – | Coalesce single sends into batches |
| [`limiter`](#limiter) | ✅ | ✅ | – | Cap throughput to a message rate |
| [`delay`](#delay) | ✅ | ✅ | – | Fixed delay per receive/send |
| [`cookie_jar`](#cookie_jar) | ✅ | ✅ | – | Persist HTTP cookies / session values across messages |
| [`encryption`](#encryption) | ✅ | ✅ | `encryption` | AEAD-encrypt payloads on send, decrypt on receive |
| [`compression`](#compression) | ✅ | ✅ | `compression` | Compress payloads on send, decompress on receive |
| [`metrics`](#metrics) | ✅ | ✅ | `metrics` | Emit throughput/latency/error metrics |
| [`random_panic`](#random_panic) | ✅ | ✅ | – | Fault injection for testing |
| [`custom`](#custom-middleware) | ✅ | ✅ | – | Your own middleware via a registered factory |

> **Two kinds of compression.** The [`compression`](#compression) *middleware* compresses
> each message **payload** on any transport and decompresses it on the far side. Separately,
> the batch `compression` *field* on the `file` and `object_store` endpoints (`none` / `gzip`
> / `lz4` / `zstd`, same `compression` feature) compresses whole write batches so the file
> stays decodable with `zcat` / `lz4 -d`. Use the field for CLI-readable data at rest, the
> middleware for over-the-wire payloads. Don't stack either compression with the
> [`encryption`](#encryption) middleware on the same route — ciphertext does not compress; for
> compressed-and-encrypted data at rest use the endpoints' own `compression`/`encryption`
> fields (compress-then-encrypt per batch).

**Putting a middleware on the wrong side behaves in two different ways**, so check the table
above rather than assuming:

- `dlq` / `retry` on an input log a warning and are skipped. The route still starts.
- `deduplication`, `weak_join` and `id` on an output are **hard startup errors**. Deduplication
  cannot work on the publish side, and silently starting an un-deduplicated route is worse
  than refusing to start.

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

```yaml middleware
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

```yaml middleware
- dlq:
    endpoint:
      file: { path: "dead-letters.jsonl" }
```

Captures `NonRetryable` failures and `Retryable` ones whose retries are exhausted. Connection
errors are **not** dead-lettered — they propagate so the route can reconnect. Nor are handler
failures: the handler runs outside the middlewares (see [Ordering](#ordering--read-this-before-combining-middleware)),
so a `dlq` only ever sees what failed on the way to the sink. The DLQ endpoint
is a full endpoint, so it can itself have middleware. If the DLQ send fails with a connection
error that error propagates rather than silently dropping the message.

**Without a `dlq` middleware**, a message that fails permanently — a data/type
error the sink rejects — is logged at `error` level and
**dropped**, and the route keeps processing the rest of the batch. `dlq` is the only retention
mechanism: `retry` alone does not retain a permanently-failed message nor prevent it from being
dropped — it only re-attempts `Retryable` errors (a connection error is passed straight through
for the route to reconnect on, not retried), then hands a still-failing message on to be dropped
(or to a following `dlq`). This is why a sink that fails with a *connection* error never reaches
its `dlq`, whether it is a route's sole output or one leg of a `fanout`. This tolerate-and-continue
policy keeps one bad message from halting the whole stream, but it means a *systematic* failure
(e.g. every row hitting a column-type mismatch) drains the input while committing nothing and
still ends `completed`. Add a `dlq` to capture the failures for inspection/replay, or watch the
route's logs — a burst of `Dropping message … due to non-retryable error` is the signal. Note
that transient errors are handled separately: several endpoints retry connection/timeout errors
internally, and the `retry` middleware adds backoff on top, so only genuinely permanent errors
reach this drop path.

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
| `coerce_empty_as_null` | bool | `false` |
| `on_error` | `reject` \| `pass_through` | `reject` |

`schema` and `schema_file` are mutually exclusive. A mapping rule is either a bare path
string or `{ path, default, required }`.

`schema` must be a JSON *object*, not a string containing one. A flat `key=value` middleware
syntax (such as the `|transform?schema=…` form in a connection URI) can only pass strings, so
it cannot express `schema` or any mapping rule beyond a bare path — use `schema_file`, or move
the route into a config file.

```yaml middleware
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
(also `"type": ["string","null"]`), `enum`, `contentMediaType`, `contentSchema`. Everything
else is ignored, so an existing fuller schema can be used as-is. Coercions are limited to the
lossless ones: `string → integer`, `string → number`, `string → boolean` (`true`/`false`/`1`/`0`),
`number → string`.

#### Empty strings

CSV and many SQL exports spell "no value" as an empty string. `coerce_empty_as_null: true`
reads every `""` the schema visits as `null`, which is then handled like any other null —
a `nullable` field keeps it, a `default` replaces it:

```yaml middleware
- transform:
    coerce_empty_as_null: true
    schema:
      type: object
      properties:
        note: { type: string, nullable: true }
        tier: { type: string, default: standard }
```

`note: ""` arrives as `null` and `tier: ""` as `"standard"`. A field that is neither
nullable nor defaulted is rejected, naming the coercion. Only fields the schema declares are
affected; `" "` is not empty.

#### Embedded JSON

A field carrying a JSON document as a string is decoded by `contentMediaType`, following
JSON Schema 2020-12:

```yaml middleware
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
```

The string is replaced by the parsed document, and `contentSchema` — if given — is applied to
it with the same coercion, defaults and validation as anywhere else, so the inner `qty: "7"`
arrives as `7`. Without `contentSchema` the value is parsed but not validated. A root-level
schema of this shape decodes a double-encoded message body.

This is **not** a coercion, and `coerce: true` never performs it: widening `"42"` to `42` is
lossless, whereas evaluating a string as a document is a parse that can succeed on input never
meant as JSON. It is opt-in per field, as the JSON Schema spec requires. Note that the spec
treats `contentSchema` as annotation-only; applying it is the opt-in behaviour it carves out.

Media types ending in `+json` (and `text/json`) are decoded too; parameters like
`; charset=utf-8` are ignored. A media type we cannot decode, or one paired with a
`contentEncoding`, leaves the string untouched rather than failing. A string that does not
parse fails with kind `content`:
`transform failed at $.payload [content]: contentMediaType is JSON but the string does not parse: ...`.

Failures are always non-retryable and name the field, e.g.
`transform failed at $.items[1].qty [coercion]: cannot coerce string "oops" to integer`.
On an **output** endpoint the message is failed so a following `dlq` captures it; on an
**input** endpoint it is dropped from the batch and acknowledged, keeping invalid data out of
the route. `on_error: pass_through` instead forwards the original payload with the reason in
the `mqb.transform_error` metadata key, which a [`switch`](#switch) can route on.

Schemas and paths compile once at startup; `schema_file` is read a single time. A `transform`
with neither stage configured leaves the payload untouched without parsing it.

### `id`

Renders a template into the `mqb.id` metadata key, giving a message a **business identity**
that survives a re-read. Input only.

The value is a bare interpolation template string (see `${namespace:selector}`).

```yaml middleware
- id: "${payload:order_id}"
```

Most sources mint a fresh `message_id` on every read, so it identifies the *delivery*, not the
record — see [DELIVERY.md](DELIVERY.md#sources-what-identity-you-get) for which ones do carry a
stable id. `mqb.id` fills that gap: derived from the message itself, it is the same on every
re-read. Unlike `message_id` (a `u128`) it keeps the key as a string, so a sink can use it
verbatim, and unlike `mqb.src.*` it is **not** stripped on publish — an identity describes the
record, not the hop, so it propagates downstream.

**Order matters, and not the way it reads.** Consumer middlewares wrap in reverse, so the entry
*closest to the end of the list* touches an incoming message *first*. Anything that consumes
`mqb.id` must therefore be listed **before** the `id` that produces it:

```yaml middleware
- deduplication: { store: "sled:///var/lib/mq-bridge/dedup", ttl_seconds: 3600, key: "${metadata:mqb.id}" }
- id: "${payload:order_id}"
```

Reversing those two leaves `mqb.id` unset when `deduplication` reads it, which falls back to
`message_id` with only a warning. Pinned by
`middleware::id::tests::the_last_listed_consumer_middleware_runs_first`.

**A partial identity is no identity.** The key is set only when *every* selector in the template
resolves; if any one is missing the message passes through with `mqb.id` unset (warned once per
route, then at `debug`). This matters for multi-part templates: `"${payload:tenant}-${payload:order_id}"`
would otherwise render `"acme-"` for every message missing `order_id` and hand them all the same
identity — which, used as a `deduplication` key, drops all but the first.

Malformed templates fail at startup, and so does a template with no `${...}` token at all, since a
constant would give every message one identity.

### `deduplication`

Drops messages whose key was already seen within the TTL. Input only. Requires the `dedup`
feature (pulls `sled`).

| Field | Type | Required |
|---|---|---|
| `store` | string | one of `store`/`sled_path` |
| `sled_path` | string | one of `store`/`sled_path` |
| `ttl_seconds` | integer | yes |
| `key` | string | no (defaults to `message_id`) |

`key` is an interpolation template (see `${namespace:selector}`), typically
`"${payload:order_id}"`. Without it the key is the `message_id`, which most sources
regenerate on every read — so re-reading the same source deduplicates nothing and only
in-flight redeliveries are suppressed. Set `key` to a business key whenever you need
dedup to survive a re-read.

`store` selects the backend by URL scheme:

- `sled:///path` (or a bare path) — a local sled database; per-process, not cluster-wide.
- `mongodb://host/db[/collection]` — a shared collection, so multiple instances of a route
  deduplicate against one another. Requires the `mongodb` feature. Expiry is judged on read,
  so a `ttl_seconds` boundary is honoured exactly; the TTL index only reclaims space
  afterwards (MongoDB's sweep can lag by up to a minute). The collection defaults to
  `mqb_dedup_<route>`. Point it at the same
  deployment your sink already uses to avoid running extra infrastructure.
- `postgres|mysql|mariadb|sqlite://…[/table]` — a shared SQL table (`dedup_key` PK,
  `expire_at`), so multiple instances deduplicate against one another. Requires the `sqlx`
  feature. SQL has no native TTL, so expired rows are swept periodically; the table defaults
  to `mqb_dedup_<route>`.

`sled_path` is the legacy spelling of a local sled store and is equivalent to `store: "sled://<path>"`.

```yaml middleware
- deduplication: { store: "sled:///var/lib/mq-bridge/dedup", ttl_seconds: 3600 }
```

```yaml middleware
- deduplication: { store: "mongodb://localhost:27017/etl", ttl_seconds: 3600 }
```

```yaml middleware
- deduplication: { store: "postgres://user:pass@localhost/etl", ttl_seconds: 3600 }
```

```yaml middleware
- deduplication: { store: "sled:///var/lib/mq-bridge/dedup", ttl_seconds: 3600, key: "${payload:order_id}" }
```

When MongoDB is your sink and messages carry a business key, prefer the sink's own unique
index (`id_field` on the mongodb output) over this middleware — the target collection then
*is* the deduplication authority, with no second write. See the idempotency notes in README.

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

```yaml middleware
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

`group_by` reads message **metadata** only — never the payload. A message that lacks the key
falls into a shared `"default"` group, so a mistyped key or a source that never sets it joins
unrelated messages instead of failing. If the value lives in the payload, lift it into metadata
first (a `transform` mapping, or the source's own metadata options).

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

```yaml middleware
- buffer: { max_messages: 500, max_delay_ms: 20 }
```

Flushes when either bound is hit. Useful in front of an endpoint whose per-call overhead
dominates. Adds up to `max_delay_ms` of latency.

### `limiter`

Paces throughput to a target rate. Input and output.

| Field | Type | Required |
|---|---|---|
| `messages_per_second` | float (> 0) | yes |

```yaml middleware
- limiter: { messages_per_second: 250 }
```

Best-effort pacing that accounts for batch size, not just call count.

### `delay`

Sleeps a fixed duration before each receive or send. Input and output.

| Field | Type | Required |
|---|---|---|
| `delay_ms` | integer | yes |

```yaml middleware
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

```yaml middleware
- cookie_jar:
    shared_scope: "login-session"
    capture_metadata_keys: ["x-csrf-token"]
    export_metadata_prefix: "session."
```

Reads `set-cookie` from responses and injects `cookie` into later requests. With
`shared_scope`, instances using the same name share one store across endpoints and routes in
the process — that is how a login route and a data route reuse one session.

### `encryption`

Encrypts each message **payload** into a self-describing AEAD envelope on the output side
and decrypts it on the input side. Metadata and routing keys stay in the clear. Input and
output. Requires the `encryption` feature.

| Field | Type | Default |
|---|---|---|
| `cipher` | `xchacha20poly1305` \| `aes256gcm` | `xchacha20poly1305` |
| `key_id` | string | `default` |
| `key` | string — base64-encoded 32-byte key; `${env:VAR}` reads it from the environment | required |
| `decrypt_keys` | map key_id → key | `{}` |

```yaml middleware
- encryption: { key: "${env:MQB_ENC_KEY}" }
```

The envelope records the cipher and `key_id`, so key rotation works by sealing with a new
`key_id`/`key` while listing the old key under `decrypt_keys` on the consuming side. Each
payload is authenticated independently: any bit-level tampering, a torn frame, or a
missing/wrong key is a hard consumer error, not a silent drop. The AEAD binds only the
payload (empty associated data): metadata and routing keys are *not* authenticated against
the ciphertext, since they are not guaranteed to survive transport round-trips (many
endpoints regenerate the `message_id` or drop `kind`). A sealed payload can therefore be
replayed under different metadata; use the `deduplication` middleware or a sink uniqueness
constraint if that matters. Note that this authenticates
each payload, not the file as a whole — like any append-structured file, an at-rest file
that loses whole trailing frames (truncation at a frame boundary) reads back as a shorter
stream with no error, so rely on the consumer's checkpoint/cursor for completeness rather
than on the encryption layer.

Do **not** combine this middleware with a sink's batch `compression` on the same route:
ciphertext does not compress. For compressed *and* encrypted data at rest, use the `file` /
`object_store` endpoints' own fields instead, which apply compress-then-encrypt per batch:

```yaml endpoint
output:
  file:
    path: "data.enc"
    format: raw
    compression: lz4          # none | gzip | lz4 | zstd  (`compression` feature)
    encryption: { key: "${env:MQB_ENC_KEY}" }
```

Both endpoints accept the same `compression` and `encryption` fields (`object_store`
derives its default object extension from them, e.g. `.jsonl.gz` / `.jsonl.lz4`, and adds a
trailing `.enc` when encryption is on since the object is ciphertext, not a directly
decompressible `.gz`). An
encrypted **file** is written as length-prefixed sealed frames (one per batch) and is only
readable through a matching consumer; a compressed-only file stays a standard `.gz`/`.lz4`
stream. File compression/encryption supports only the default `consume` mode. `csv` works
too: the header row is written into the first member, so the decoded stream is a normal CSV
file.

A file **source** must declare the same `compression`/`encryption` the data was written with.
A mismatch (wrong key, wrong codec, or a missing field) is a permanent decode failure: the
route ends `failed` with the error in its status, rather than completing as if the file were
empty. Reading a compressed file with no `compression` set is likewise rejected up front by
sniffing the leading magic bytes, so raw compressed bytes are never emitted as messages.

> **f64 precision.** Numbers move through payloads as JSON. serde_json's default parser shifts
> ~1 ULP on ~19% of 17-significant-digit doubles, so a `postgres → file → postgres` hop of a
> `double precision` column can change the last bit. Build with the `float-roundtrip` feature
> for bit-exact float parsing across every endpoint (it trades a little parse speed for it).

### `compression`

Compresses each message **payload** on the output side and decompresses it on the input
side. Metadata and routing keys are untouched. Input and output. Requires the `compression`
feature.

| Field | Type | Default |
|---|---|---|
| `algorithm` | `none` \| `gzip` \| `lz4` \| `zstd` | `zstd` |
| `max_decompressed_bytes` | integer — reject a payload that decompresses larger than this (bomb guard); consumer side only | unset (no limit) |

```yaml middleware
- compression: { algorithm: zstd }
```

Each payload is compressed independently into a single self-contained member, so this works
over any transport, not just files. `algorithm: none` is a passthrough. A truncated or
corrupt frame is a **permanent** consumer error (the poison message is not re-read
indefinitely), as is a payload that exceeds `max_decompressed_bytes`. Put the same
`algorithm` on both the input and output side of a route.

Unlike the `file` / `object_store` batch `compression` field — which keeps whole write
batches decodable with `zcat` / `lz4 -d` — this middleware frames per message and is only
readable through a matching consumer. Do not combine it with the [`encryption`](#encryption)
middleware (ciphertext does not compress); for compressed-and-encrypted data at rest, use the
endpoints' own `compression`/`encryption` fields instead.

### `metrics`

Emits throughput, latency and error metrics for the endpoint. Input and output. Requires the
`metrics` feature. Takes no options; its presence enables collection.

```yaml middleware
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

```yaml middleware
- random_panic: { mode: disconnect, trigger_on_message: 500 }
```

`disconnect` and `timeout` produce retryable errors; `json_format_error` produces a
non-retryable one — useful for exercising a `dlq`. Keep `enabled: false` in committed configs
rather than deleting the block.

On the **input** side, `json_format_error`/`nack` never call the real consumer at all — they
substitute a synthetic message (or error) on every triggered `receive`. Leaving
`trigger_on_message` unset means *every* poll is faulted, so the real source is never read and
`exit_on_empty`/`--drain` never sees the empty batch it waits for — the route runs forever,
manufacturing synthetic messages. Always set `trigger_on_message` to a specific count when
testing a drain-mode route with input-side fault injection. And since `dlq`/`retry` on an input
are no-ops (see above), pair an input-side fault with a real assertion on the *consumer's*
recovery, not a `dlq`.

The middleware block alone is **not** enough: fault injection is gated per route by
`allow_fault_injection`, which defaults to `false`. Copying only the snippet above leaves the
middleware inert (the route logs that it is disabled). A complete, working configuration:

```yaml route
flaky_test_route:
  allow_fault_injection: true
  input:
    memory: { topic: "in" }
    middlewares:
      - random_panic: { mode: disconnect, trigger_on_message: 500 }
  output:
    memory: { topic: "out" }
```

`allow_fault_injection: true` is intended for test configurations only. Do not enable it — or
the `random_panic` middleware — in production configs.

### `custom` (middleware)

Delegates to a factory you registered programmatically.

| Field | Type | Required |
|---|---|---|
| `name` | string | yes |
| `config` | any JSON | yes |

```yaml middleware
- custom:
    name: "my_enricher"
    config: { lookup_url: "http://enrich.internal" }
```

Implement `CustomMiddlewareFactory` (`apply_consumer` and/or `apply_publisher`, each
defaulting to pass-through) and register it before starting routes. It can also be written
in Python (`register_middleware`, hooks `on_receive` / `on_send`) or JavaScript
(`registerMiddleware`, hooks `onReceive` / `onSend`). See **[EXTENDING.md](EXTENDING.md)**
for the full guide.

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

They live under `src/endpoints/structural/`, and each of the variants above carries
`"format": "structural_endpoint"` in the generated JSON schema (`mq-bridge.schema.json`),
so external tooling can tell them apart from the transport endpoints.

### `ref`

Reuses an endpoint registered under a name, instead of repeating its configuration.

The name is a **registry key, not a topic name**. Register it from Rust before starting the
routes:

```rust
use mq_bridge::models::Endpoint;
use mq_bridge::route::register_endpoint;

register_endpoint("common_queue", Endpoint::new_memory("shared_memory_topic", 100));
```

```yaml route
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

```yaml endpoint
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

```yaml endpoint
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

```yaml endpoint
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

```yaml route
http_echo:
  input: { http: { url: "0.0.0.0:8080" } }
  output: { response: {} }
```

Takes no options. Requires an input that carries a reply channel (`http`, `websocket`, `grpc`,
or a request/reply `nats`/`mongodb`/`memory`). With an `http` or `websocket` input and no
middleware, `response` (and `static`) enables an inline fast path that skips the normal route
pipeline. See [README.md](../README.md#patterns-request-response).

### `reader`

An output endpoint that **ignores the incoming payload** and instead reads one message from
the wrapped consumer, returning it as the response. The inbound message is purely a trigger.

```yaml route
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

```yaml endpoint
output: { static: "OK" }                       # shorthand, body JSON-encoded

output:
  static:
    body: '{"status":"ok"}'
    raw: true                                  # send verbatim, do not JSON-encode
    metadata: { content-type: "application/json" }
```

`raw: true` sends `body` byte-for-byte; the default JSON-encodes it as a string. Like
`response`, a `static` output enables the HTTP inline fast path.

#### Placeholders

`body` is a template compiled **once at startup**; rendering a message never re-parses it.
Tokens use the `${namespace:selector}` form:

| Token | Resolves to |
|---|---|
| `${payload:a.b.c}` | a field of the incoming JSON payload (dotted path; array indices allowed) |
| `${metadata:key}` | a metadata value |
| `${message:id}` | the message id (UUID string) |
| `${gen:uuid}` | a fresh UUID v7 |
| `${gen:now}` / `${gen:timestamp}` | current time (RFC3339 UTC / Unix epoch ms) |
| `${gen:counter}` | a per-endpoint counter, starting at 0 |
| `${gen:random(1,100)}` | a random integer in `[min, max]` |
| `${env:VAR}` | an environment variable, resolved once at startup |

`payload`/`metadata`/`message` read the request, so they are the useful ones on an **output**
(e.g. an error reply that echoes the request); on an **input** (load-test source) only
`gen`/`env` produce values. When the body's `content-type` metadata is a JSON type,
interpolated request values are **JSON-escaped by default** so external data cannot break the
structure — append `| raw` to a token to splice it verbatim. To emit a literal, un-interpolated
`${…}`, write `$${…}` (a bare `$$` is left as-is); any `${…}` with an unknown namespace is also
left untouched.

```yaml endpoint
output:
  static:
    body: '{"error":"not found","id":"${message:id}","at":"${gen:now}"}'
    raw: true
    metadata: { content-type: "application/json" }
```

### `stream_buffer`

An in-memory stream partitioned by correlation ID, used to carry streaming request/response
bodies between routes.

| Field | Type | Notes |
|---|---|---|
| `topic` | string | required; shared by publisher and consumers |
| `correlation_id` | string | **required on consumers, must be unset on publishers** |
| `capacity` | integer | default `100`, per partition |

```yaml endpoint
output:
  stream_buffer: { topic: "responses" }        # publisher: no correlation_id

input:
  stream_buffer: { topic: "responses", correlation_id: "req-123" }   # consumer
```

A consumer without `correlation_id` is a startup error; a publisher *with* one logs a warning
and ignores it. Primarily wired up via `HttpConfig::stream_response_to`.

### `null`

Discards every message. Output only. This is the **default output** when a route omits one.

```yaml route
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

```yaml endpoint
output:
  custom:
    name: "my_sink"
    config: { target: "internal://thing" }
```

Implement `CustomEndpointFactory` and register it before starting routes. Once registered,
the name also works as a bare endpoint key — `input: { my_sink: {...} }` — since any
unrecognised key is looked up in the custom-endpoint registry. Use the explicit `custom:`
form above if you validate configs against `mq-bridge.schema.json`, which cannot know your
key. Endpoints can also be written in Python (`register_endpoint`) or JavaScript
(`registerEndpoint`). See **[EXTENDING.md](EXTENDING.md)** for the full guide.

---

## See also

- [README.md](../README.md) — overview, data endpoints, request/response and CQRS patterns
- [CONFIGURATION.md](CONFIGURATION.md) — full YAML examples, env vars, TLS, IDE schema validation
- [DELIVERY.md](DELIVERY.md) — delivery guarantees, per-source identity, per-sink idempotency
- [ARCHITECTURE.md](ARCHITECTURE.md) — internals, batching/concurrency, extension traits
- [EXTENDING.md](EXTENDING.md) — writing your own endpoint or middleware, in Rust, Python or Node
