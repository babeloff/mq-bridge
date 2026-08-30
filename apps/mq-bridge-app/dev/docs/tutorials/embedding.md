# Embed the library (Rust / Python / Node)

The core engine is a Rust library, and the **same engine** ships as native bindings
for **Python** and **Node.js**. The Tokio runtime, broker I/O, routing, and batching
all stay in Rust; the binding is a thin layer for handlers and configuration. Behaviour
and reliability match the Rust engine regardless of which language calls in.

| Language | Package | Install |
| :--- | :--- | :--- |
| Rust | [`mq-bridge`](https://crates.io/crates/mq-bridge) | `cargo add mq-bridge` |
| Python | [`mq-bridge-py`](https://pypi.org/project/mq-bridge-py/) | `pip install mq-bridge-py` |
| Node.js | [`mq-bridge`](https://www.npmjs.com/package/mq-bridge) | `npm install mq-bridge` |

## The config-first workflow

The natural way to embed the library is to **design the route as config**, then load
that exact config from your code. Design and test a route in the desktop UI (or hand-write
the YAML), export the JSON/YAML, and load it — the running route behaves identically to
the `--config` CLI form.

Constructor names are kept aligned across languages (Python `snake_case`, Node
`camelCase`):

| Purpose | Python | Node.js |
|---|---|---|
| Load a route from a YAML/JSON **file** | `Route.from_file` | `Route.fromFile` |
| Load from an in-memory YAML/JSON **string** | `Route.from_str` | `Route.fromStr` |
| Load from a parsed **dict / JS object** | `Route.from_config` | `Route.fromConfig` |
| Build a publisher endpoint | the matching `Publisher.*` | the matching `Publisher.*` |

The `name` argument is optional: pass it to select one entry from a
`routes:`/`publishers:` document, or omit it to treat the config as a single bare
route/endpoint body.

## Rust

The Rust crate exposes the full engine. The main types:

- `Route::new(input, output)` — a pipeline from one endpoint to one endpoint, with
  `.with_batch_size(n)`, `.with_handler(h)`, `.add_handler(kind, f)`, `.deploy(name)` /
  `.run()`.
- `Endpoint` — protocol adapters (`Endpoint::new_memory`, `Endpoint::null`, …) plus the
  serde-configured variants.
- `Publisher::new(endpoint)` — publish into a route's input or any endpoint.
- Handlers: `CommandHandler` (1-to-1 / 1-to-0), `EventHandler` (terminal 1-to-N), and
  `TypeHandler` (dispatches on the `kind` metadata field, deserializing payloads).
- `CanonicalMessage` — the unified message type all handlers work with; `msg!(&value,
  "kind")` builds one with a `kind`.

The `MessageConsumer` and `MessagePublisher` traits (in `mq_bridge::traits`) are the
core abstractions. See the [Rust docs on docs.rs](https://docs.rs/mq-bridge) for the
full API.

## See also

- [Language bindings API](../reference/bindings.md) — the reference for this material.
- [Writing endpoints & middleware](../engine/extending.md) — plugging your own endpoint or
  middleware into the engine from Rust, Python, or Node.
- [Core concepts](../getting-started/concepts.md) and [Learn the architecture](../engine/architecture.md) — the handler and message model.
- [The three ways to run it](../getting-started/run-forms.md) — library vs CLI-server vs desktop.
