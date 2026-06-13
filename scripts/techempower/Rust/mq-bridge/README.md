# mq-bridge

[mq-bridge](https://github.com/marcomq/mq-bridge) is an asynchronous
message-bridging library for Rust. It connects messaging systems, data stores,
and protocols (Kafka, NATS, AMQP, MQTT, MongoDB, ZeroMQ, HTTP, files, in-memory
channels) into named routes, with optional transformation, filtering, and
middleware.

This entry exercises mq-bridge's HTTP endpoint and its **inline-response fast
path**: HTTP framing and JSON (de)serialization run in the Rust core, and the
route handler returns the reply inline without entering the route
worker/disposition pipeline. The HTTP server is [hyper](https://hyper.rs/).

## Test types implemented

| Test             | URL                      | Response |
|------------------|--------------------------|----------|
| JSON             | `GET /json`              | `{"message":"Hello, World!"}`, serialized **per request** |
| Plaintext        | `GET /plaintext`         | `Hello, World!` (pipelined throughput test) |
| Single Query     | `GET /db`                | one random `World` row as JSON |
| Multiple Queries | `GET /queries?queries=N` | N random `World` rows as a JSON array (N clamped to 1..500) |

Fortunes (HTML templating) and Data Updates are not implemented — mq-bridge is a
message-bridging library, not a full-stack web framework. The DB tests are
included here as a raw [SQLx](https://github.com/launchbadge/sqlx)/Postgres
example.

## How it works

Two routes share one listener on `0.0.0.0:8080` (the HTTP server keys on the
listen address):

- **`GET /plaintext -> static`** — a path-filtered route answers the Plaintext
  test **handler-free**. The body is sent raw (no JSON quoting) with explicit
  `Content-Type`/`Server` headers, taking the inline fast path without ever
  entering a handler.
- **`http -> response`** (catch-all) — dispatches `/json`, `/db`, and `/queries`
  inside the handler on the request's `http_path` metadata. The router prefers
  the more specific (path-filtered) route, so `/plaintext` hits the static reply
  and everything else falls through here.

Notes:

- **Inline fast path.** Both the `static` and `response` outputs reply inline, so
  route `concurrency`/`batch_size` do not gate the hot path; we use
  `concurrency: 1`. Per-connection parallelism comes from the Tokio runtime
  spawning a task per accepted connection.
- **`concurrency_limit`** is a per-request semaphore (default 100). It is raised
  to 65536 so it never throttles TechEmpower's many connections.
- **Pipelined writes.** The core HTTP server enables hyper's
  `pipeline_flush(true)`, coalescing pipelined HTTP/1.1 responses into a single
  buffered write — the win for the Plaintext test, a no-op otherwise.
- **Headers.** Returned-message metadata becomes response headers
  (`Content-Type`, `Server`); hyper adds `Date` and `Content-Length`. The status
  comes from `http_status_code` metadata (default 200).

### Database access

`/db` and `/queries` run `SELECT id, randomnumber FROM world WHERE id = $1` for a
random id (1..10000) via a handler-owned `sqlx::PgPool`.

mq-bridge's built-in `sqlx` *endpoint* models a table as a message queue (INSERT
publisher / polling SELECT consumer) with no per-request parameter and no
request↔reply correlation, so it cannot serve TechEmpower's random-id,
reply-synchronously pattern. The handler owns a pool and runs the query directly.

The pool is **optional**: without a reachable `DATABASE_URL` the DB routes return
`503` and `/json` + `/plaintext` still run. The Docker image points
`DATABASE_URL` at TechEmpower's `tfb-database` host.

## Important source files

- [src/main.rs](src/main.rs) — route setup, the request handler, and the SQLx pool
- [Cargo.toml](Cargo.toml) — dependencies (`mq-bridge` with the `http` feature, `sqlx`, `serde`)
- [mq-bridge.dockerfile](mq-bridge.dockerfile) — build/run image

## Test URLs

- JSON: <http://localhost:8080/json>
- Plaintext: <http://localhost:8080/plaintext>
- Single Query: <http://localhost:8080/db>
- Multiple Queries: <http://localhost:8080/queries?queries=20>

## Versus

`hyper` — this entry is a thin routing layer over hyper; comparing the two shows
mq-bridge's per-request overhead.
