# mq-bridge Python bindings

Thin Python bindings for the Rust `mq-bridge` core.

## Install

Pick exactly one distribution. Both install the same import path: `mq_bridge`.

| Package | Install | Includes |
| :--- | :--- | :--- |
| Full | `pip install mq-bridge-py` | Basic set plus Kafka, AWS, gRPC, MongoDB, SQLx |
| Basic | `pip install mq-bridge-py-basic` | HTTP, NATS, MQTT, AMQP, WebSocket, ZeroMQ, middleware |

Memory and file endpoints are always present in both packages. Use `mq-bridge-py-basic` when you want the lean all-platform wheel set. Use `mq-bridge-py` when you need Kafka or the heavier non-messaging backends.

The public API stays close to mq-bridge itself:

- `Route.from_yaml(path, name)` loads one named route
- `Route.with_handler(...)` attaches a raw `Message` handler, with lazy `json()`/`text()` readers and `with_json()`/`with_payload()` response helpers
- `Route.add_handler(kind, ...)` uses mq-bridge's `kind` dispatch and delivers decoded JSON
- `RetryableError` and `NonRetryableError` let Python handlers signal retry intent
- `Publisher.from_yaml(path, name)` loads one named publisher
- `Publisher.send_json(...)` and `Publisher.request_json(...)` serialize Python JSON values in Rust

The Python surface is synchronous and blocking. Tokio, broker I/O, routing, and batching all stay in Rust.

## Local development

`uv` is a good fit here for the Python-side developer workflow, while `maturin` stays the build backend:

```bash
cd python/mq-bridge-py
uv sync --group dev --no-install-project
uv run maturin develop
uv run pytest -q
```

Performance smoke tests are skipped by default because they start routes and measure
local throughput:

```bash
cd python/mq-bridge-py
MQ_BRIDGE_RUN_PERF_TESTS=1 uv run pytest -q -m performance
```

## Examples

Raw message handler:

```bash
cd python/mq-bridge-py
uv run python examples/raw_route.py
```

Kind-based JSON handler:

```bash
cd python/mq-bridge-py
uv run python examples/json_route.py
```

Memory benchmark:

```bash
cd python/mq-bridge-py
uv run maturin develop --release
uv run python examples/bench_memory.py --messages 100000
```

## Analysis

HTTP comparison benchmark, driven by a native load generator (`wrk`) so the
client is never the bottleneck. It boots each server itself (mq-bridge in worker
and direct executor modes, plus FastAPI, Starlette, Sanic, aiohttp, and
FastStream when installed) and drives each with `wrk`:

```bash
cd python/mq-bridge-py
uv run maturin develop --release
uv sync --group bench   # optional Python HTTP peers
uv run python analysis/bench_http_native.py --connections 1,8,32 --duration 8
```

Requires `wrk` on `PATH` (`brew install wrk`). The FastStream target compares its
ASGI custom-route path over Uvicorn; it is not a broker-backed subscriber/publisher
benchmark. The examples use included sample configs or create temporary configs.
