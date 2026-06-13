# mq-bridge-py

`mq-bridge-py` is the Python binding for
[mq-bridge](https://github.com/marcomq/mq-bridge), an asynchronous
message-bridging library written in Rust. You configure routes (HTTP, Kafka,
NATS, AMQP, MQTT, MongoDB, files, in-memory channels) and attach a Python
handler; the Rust core does the I/O.

The point of this entry is **off-GIL work**: all HTTP framing and JSON
(de)serialization run in the Rust core off the GIL, while the single Python
worker thread runs only the trivial per-request dispatch. The HTTP server is
[hyper](https://hyper.rs/). This is what should place `mq-bridge-py` near the top
of the Python-language entries on the JSON test.

## Test types implemented

| Test      | URL              | Response |
|-----------|------------------|----------|
| JSON      | `GET /json`      | `{"message":"Hello, World!"}`, serialized **per request** |
| Plaintext | `GET /plaintext` | `Hello, World!` (pipelined throughput test) |

The database tests are **intentionally not implemented**. The Python handler runs
synchronously on the single Python worker thread, so a Python DB driver there
would serialize every query through one thread and misrepresent the framework.
This entry's value is off-GIL JSON serialization, not DB I/O. (The Rust
[`mq-bridge`](../../Rust/mq-bridge/) entry does include the DB tests.)

## How it works

A single `http -> response` route is bound to `0.0.0.0:8080` with no path filter;
the Python handler dispatches both `/json` and `/plaintext` on the request's
`http_path` metadata.

Notes:

- **Inline fast path.** The route replies inline, bypassing the route
  worker/disposition pipeline, so route `concurrency`/`batch_size` do not gate the
  hot path; we use `concurrency: 1` (no worker pool is spawned). Per-request
  parallelism comes from the multi-threaded Tokio runtime in the Rust core.
- **Off-GIL serialization.** `Message.from_json(...)` serializes the object in
  Rust/serde, off the GIL, satisfying the per-request-serialization rule without
  paying for it in Python.
- **`concurrency_limit`** is a per-request semaphore (default 100). It is raised
  to 65536 so it never throttles TechEmpower's many connections.
- **Headers.** The returned `Message`'s metadata becomes the HTTP response
  headers (`Content-Type`, `Server`); hyper adds `Date` and `Content-Length`. A
  fresh `Message` is returned so no request headers are echoed back.

## Important source files

- [server.py](server.py) — route config, the dispatch handler, and startup
- [mq-bridge-py.dockerfile](mq-bridge-py.dockerfile) — builds the wheel with
  [maturin](https://github.com/PyO3/maturin) (`http` feature only) and runs `server.py`

## Test URLs

- JSON: <http://localhost:8080/json>
- Plaintext: <http://localhost:8080/plaintext>
