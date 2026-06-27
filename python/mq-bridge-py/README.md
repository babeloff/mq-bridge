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

- `Route.from_file(path, name=None)` loads a route from a YAML/JSON file. The three constructors differ only by source: `from_file` (path), `from_str` (in-memory YAML/JSON string), `from_config` (Python `dict`)
- The `name` is optional: pass it to pick one entry out of a `routes:`/`publishers:` document, or omit it to treat the whole config as a single bare route/endpoint body
- `Route.with_handler(...)` attaches a raw `Message` handler, with lazy `json()`/`text()` readers and `with_json()`/`with_payload()` response helpers
- `Route.add_handler(kind, ...)` uses mq-bridge's `kind` dispatch and delivers decoded JSON
- `RetryableError` and `NonRetryableError` let Python handlers signal retry intent
- `Publisher.from_file(path, name=None)` (plus `from_str` / `from_config`) builds a publisher endpoint

`from_yaml` / `from_yaml_str` remain as deprecated aliases for `from_file` / `from_str`.
- `Publisher.send_json(...)` and `Publisher.request_json(...)` serialize Python JSON values in Rust

The Python surface is synchronous and blocking. Tokio, broker I/O, routing, and batching all stay in Rust.

## Config types and schema

`mq-bridge-app` can create and test route and endpoint JSON/YAML through its UI.
It does not replace your Python code or handlers, but it is useful when you want
a known-good connection and route shape before pasting the configuration into
Python. Load the generated config with `Route.from_config`, `Route.from_file`,
`Publisher.from_config`, or `Publisher.from_file`.

For the `from_config` / `from_str` mappings, `mq_bridge.config` ships
`TypedDict` definitions so editors autocomplete the config keys (`input`,
`output`, `batch_size`, every transport config, middleware, …):

```python
from mq_bridge import Route
from mq_bridge.config import ConfigDocument

config: ConfigDocument = {
    "routes": {
        "orders": {
            "input": {"memory": {"topic": "orders.in", "capacity": 1600}},
            "output": {"response": {}},
            "batch_size": 128,
        }
    }
}
route = Route.from_config(config, "orders")
```

These types are generated from the JSON Schema, which the extension produces on
demand from the Rust models — there is no checked-in schema copy to drift:

```python
from mq_bridge import config_schema

schema = config_schema()        # the JSON Schema as a dict
```

`config_schema()` is handy for editor validation of YAML configs too — dump it
to a file and point your `# yaml-language-server: $schema=` line at it. The types
are regenerated with `uv run python scripts/gen_config_types.py` (a test fails if
they drift from the schema).

## Running a route

`Route.run()` **blocks the calling thread** until another thread calls `stop()` —
it deploys the route and then parks. This is convenient for a process whose only
job is the route, but it is a common trap: nothing after `route.run()` executes
until the route stops.

To keep running Python code after the route is up, use `start()` (non-blocking)
or the context-manager form:

```python
route = Route.from_config(config, "orders_route").with_handler(handle)

# Non-blocking: deploys, returns, and runs on a background thread.
route.start()
publisher.send_json({"order_id": 42}, {"kind": "order.created"})
route.stop()
route.join()   # optional: wait for a clean shutdown

# Or scope it to a block — starts on enter, stops + joins on exit:
with Route.from_config(config, "orders_route").with_handler(handle):
    publisher.send_json({"order_id": 42}, {"kind": "order.created"})
```

Configuration/connection errors surface from `start()` itself, not from a
background thread. `run()` remains available for the blocking single-route case.

## Pull-based consumer

`Route` is push-based: you attach a handler and the route drives it. When you
instead want to **pull** messages on your own schedule — e.g. to feed a
generator-style sink such as a [`dlt`](https://dlthub.com) resource — use
`Consumer`. It wraps any input endpoint and hands batches back to Python:

```python
from mq_bridge import Consumer

consumer = Consumer.from_config({"nats": {"subject": "orders", "url": "nats://localhost:4222"}})

while not consumer.exhausted:
    batch = consumer.poll(max=500, timeout_ms=1000)   # [] on timeout
    if not batch:
        continue
    for message in batch:
        handle(message.json())
    consumer.commit()                                 # ack only after handling
```

`poll()` receives up to `max` messages **without** acknowledging them;
`commit()` acks every batch returned since the last commit, advancing the
consumer offset (or removing them from the queue). Committing only after the
downstream write succeeds gives at-least-once delivery: a crash before `commit()`
re-delivers the batch. `poll()` returns `[]` once `timeout_ms` elapses with
nothing received (omit it to block until a message arrives), and sets
`exhausted` once a bounded source (e.g. a file) is fully drained — streaming
brokers never set it.

> **You must call `commit()` — it is not optional.** It is the only thing that
> tells the broker a batch is done. If you keep polling without committing:
> - the consumer offset never advances, so every message is **re-delivered** on
>   the next run (and you reprocess from the start);
> - most brokers stop sending once their unacknowledged/prefetch window fills, so
>   `poll()` eventually **stalls** and returns nothing;
> - the uncommitted batches are held in memory pending their ack, so the process
>   **grows unbounded**.
>
> Commit after each batch you have durably handled (as in the loops above). If a
> batch fails downstream, simply *don't* commit it — it will be redelivered.

`consumer.status()` returns a snapshot dict (`healthy`, `target`, `pending`,
`capacity`, `error`, `details`). `pending` is the broker backlog/lag where the
transport reports it — Kafka offset lag, AMQP queue depth, NATS JetStream
`num_pending` — so `pending == 0` is a precise "caught up" check for a bounded
drain; it is `None` where the broker exposes no backlog (core NATS, MQTT), where
you fall back to a `timeout_ms` that returns `[]`. It's a point-in-time snapshot,
not a guarantee.

`consumer.close()` releases the broker connection; it's idempotent, and `poll()`
/`status()` raise afterwards. Python is garbage-collected, so close explicitly
(or use the context-manager form, which closes on exit) rather than relying on
the object being collected:

```python
with Consumer.from_config(cfg) as consumer:
    batch = consumer.poll(max=500, timeout_ms=1000)
    ...
    consumer.commit()
# connection released here
```

The endpoint config decides durability exactly as a route input does: a
consumer-group config resumes from the last commit, a subscriber config receives
only new messages. `Consumer.from_file` / `from_str` accept the same shapes,
plus a named entry under a `consumers:` document section.

As a `dlt` resource this is a few lines:

```python
import dlt
from mq_bridge import Consumer

@dlt.resource(name="orders")
def orders():
    consumer = Consumer.from_config({"nats": {"subject": "orders", "url": "nats://localhost:4222"}})
    while not consumer.exhausted:
        batch = consumer.poll(max=500, timeout_ms=1000)
        if not batch:
            break                 # nothing more pending this run
        yield [m.json() for m in batch]
        consumer.commit()
```

## Tuning (environment variables)

These knobs are read from the environment at startup:

| Variable | Default | Effect |
| :--- | :--- | :--- |
| `MQ_BRIDGE_PY_HANDLER_EXECUTOR` | `worker` | `worker` runs handlers on a dedicated interpreter thread that coalesces queued batches under one GIL acquisition (best under load); `direct` calls the handler inline. |
| `MQ_BRIDGE_PY_HANDLER_CONCURRENCY` | CPU count | Max in-flight handler batches. `0` disables the limit. |
| `MQ_BRIDGE_PY_GC_MODE` | `default` | `default` leaves CPython's cyclic GC alone; `count` disables it and runs `gc.collect()` every N messages; `off` disables it entirely (pure refcounting). |
| `MQ_BRIDGE_PY_GC_THRESHOLD` | `100000` | Messages between collections when `MQ_BRIDGE_PY_GC_MODE=count`. |

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
