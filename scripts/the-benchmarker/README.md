# the-benchmarker/web-frameworks entries for mq-bridge

Entries for [the-benchmarker/web-frameworks](https://github.com/the-benchmarker/web-frameworks),
shaped to drop straight into a fork of that repo. Both the Rust core
(`mq-bridge`) and the Python bindings (`mq-bridge-py`) are covered.

That suite is deliberately tiny — three routes, served on **port 3000**:

| Method | Path        | Response          |
|--------|-------------|-------------------|
| GET    | `/`         | `200`, empty body |
| GET    | `/user/:id` | `200`, the `:id`  |
| POST   | `/user`     | `200`, empty body |

## How it works

Each entry is a single catch-all `http -> response` route bound to
`0.0.0.0:3000` (no path/method filter). The handler dispatches on the request's
`http_method` / `http_path` metadata and extracts `:id` as the suffix after
`/user/`. mq-bridge keeps all HTTP framing in Rust; the inline-response fast path
keeps the reply on the Rust side, so the handler runs only the trivial dispatch.
For the Python entry that means the framing and response never touch the GIL.

## Layout

```text
scripts/the-benchmarker/
  rust/mq-bridge/
    Cargo.toml        # binary MUST be named `server` (the engine runs target/release/server)
    config.yaml       # framework metadata (website / github / version)
    src/main.rs
  python/mq-bridge-py/
    config.yaml       # custom `mq-bridge` engine: bootstrap builds the wheel via maturin
    pyproject.toml
    server.py
```

The suite supplies the language-level `Dockerfile` and shared `config.yaml`; per
framework you provide the directory above. The Rust binary must be named
`server` because the engine invokes `target/release/server`. The Python entry
declares a custom engine (`mq-bridge`, modeled on `robyn`) whose bootstrap
installs Rust + maturin and builds `mq_bridge_py` from this repo at the pinned
`MQB_REF` tag.

## Submitting upstream

1. Pin the version: the Rust `Cargo.toml` and Python `config.yaml` reference this
   repo at tag `v0.2.18` — bump to the release you want to benchmark.
2. Fork `the-benchmarker/web-frameworks` and copy:
   - `scripts/the-benchmarker/rust/mq-bridge/`      → `rust/mq-bridge/`
   - `scripts/the-benchmarker/python/mq-bridge-py/` → `python/mq-bridge-py/`
3. Run their tooling (`make rust` / `bin/run`) to validate, then open the PR.

## Notes

- This suite is latency/throughput on trivial routes; it does not exercise
  JSON-body, DB, or streaming paths. The interesting mq-bridge results
  (off-GIL JSON for Python, the DB/static/h2c/websocket profiles) are in the
  [HttpArena](../httparena/) entries.
