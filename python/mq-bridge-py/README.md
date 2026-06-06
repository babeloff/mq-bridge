# mq-bridge Python bindings

Thin Python bindings for the Rust `mq-bridge` core.

The public API stays close to mq-bridge itself:

- `Route.from_yaml(path, name)` loads one named route
- `Route.with_handler(...)` attaches a raw `Message` handler
- `Route.add_handler(kind, ...)` uses mq-bridge's `kind` dispatch and delivers decoded JSON
- `Route.add_message_handler(kind, ...)` uses `kind` dispatch and delivers `Message` objects with lazy `json()`/`text()` readers and `with_json()`/`with_payload()` response helpers
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

HTTP comparison benchmark:

```bash
cd python/mq-bridge-py
uv run maturin develop --release
uv run python analysis/bench_http_compare.py --messages 20000 --clients 8
```

Use `--mq-output-buffer` to test mq-bridge response output buffering.

Install `starlette uvicorn fastapi sanic` to include the three optional Python HTTP peers.

The examples use included sample configs or create temporary benchmark configs.
