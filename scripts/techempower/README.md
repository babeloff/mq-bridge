# TechEmpower FrameworkBenchmarks entries for mq-bridge

Submission-shaped entries for the [TechEmpower FrameworkBenchmarks](https://github.com/TechEmpower/FrameworkBenchmarks)
project, for both the Rust core (`mq-bridge`) and the Python bindings
(`mq-bridge-py`).

## Scope

| Test            | Route                    | Entry        | Response |
|-----------------|--------------------------|--------------|----------|
| JSON            | `GET /json`              | Rust + Python | `{"message":"Hello, World!"}` serialized **per request** |
| Plaintext       | `GET /plaintext`         | Rust + Python | `Hello, World!` (the pipelined throughput test) |
| Single Query    | `GET /db`                | Rust only     | one random `World` row as JSON |
| Multiple Queries| `GET /queries?queries=N` | Rust only     | N random `World` rows (N clamped 1..500) |

Fortunes (HTML templating) and Data Updates are intentionally not implemented —
mq-bridge is a message-bridging library, not a full-stack web framework. The DB
tests are included for the Rust entry as a SQLx/Postgres example (see below);
they are omitted from the Python entry on purpose.

## How it works

Both entries expose a single `http -> response` route on `0.0.0.0:8080` with **no
path filter**, and dispatch on the request's `http_path` metadata inside the
handler. The shared HTTP server keys on the listen address, so one process/port
serves both endpoints.

Key properties (see the inline comments in each entry):

- **Inline fast path.** An `http -> response` route replies inline, bypassing the
  route worker/disposition pipeline. So route `concurrency`/`batch_size` do *not*
  gate the hot path; we use `concurrency: 1`. Per-connection parallelism comes
  from the Tokio runtime spawning a task per accepted connection.
- **Off-GIL work (Python).** All HTTP framing and JSON (de)serialization run in
  Rust off the GIL; the single Python worker thread only runs the trivial
  dispatch. This is the differentiator that should place `mq-bridge-py` near the
  top of the Python-language entries on the JSON test.
- **`concurrency_limit`.** A per-request semaphore (`acquire_owned` per request,
  default 100). We raise it to 65536 so it never throttles TechEmpower's many
  connections; the acquire cost itself is inherent to the current server.
- **Buffered pipelined writes.** The core HTTP server enables hyper's
  `pipeline_flush(true)`, which coalesces the responses of pipelined HTTP/1.1
  requests into a single buffered write — the throughput win for the Plaintext
  test, and a no-op for non-pipelined traffic.
- **Headers.** The handler's returned message metadata becomes response headers
  (`Content-Type`, `Server`); hyper adds `Date` and `Content-Length`. The status
  comes from `http_status_code` metadata (default 200).

## Database tests (Rust entry)

`/db` and `/queries` run `SELECT id, randomnumber FROM world WHERE id = $1` for a
random id (1..10000) and return the row(s) as JSON (`{"id":..,"randomNumber":..}`).

Why a handler-owned `sqlx::PgPool` rather than mq-bridge's `sqlx` endpoint: that
endpoint models a table as a **message queue** — the publisher does `INSERT`, the
consumer runs a *polling* `SELECT` whose only bind is the route `batch_size` and
emits rows as a stream of messages. It has no per-request parameter and no
request↔reply correlation, so it cannot serve TechEmpower's random-id,
reply-synchronously pattern. The handler owns a pool and runs the query directly.

The pool is optional: without a reachable `DATABASE_URL` the DB routes return 503
and `/json` + `/plaintext` still run. The Docker image points `DATABASE_URL` at
TechEmpower's `tfb-database` host.

**Not added to the Python entry on purpose.** The Python handler runs
synchronously on the single Python worker thread, so a Python DB driver there
would serialize every query through one thread and badly misrepresent the
framework. The Python entry's value is off-GIL JSON serialization, not DB I/O.

## On bypassing the handler (switch + static)

mq-bridge has a `static` endpoint (emits a fixed response body) and a `switch`
endpoint (routes on a metadata key), so in principle `http → switch on http_path
→ static` could answer with **no handler at all**. Two gaps stop it from being
TechEmpower-conformant today:

1. the `static` endpoint JSON-encodes its string (so `Hello, World!` comes back
   as `"Hello, World!"` with quotes), and
2. neither `static` nor `switch` sets the response `Content-Type`/`Server`
   headers, so the reply defaults to `application/octet-stream` and fails the
   header validation.

For **plaintext** this is the only test where a static (handler-free) reply is
allowed by the rules, and it would shave the per-request handler hop — but it
needs those two small core additions (raw/unquoted static payload + a way to set
the response content-type), and it would make the plaintext number measure the
Rust core rather than the Python path. For **JSON** it is not an option at all:
the rules require per-request serialization, not a pre-rendered string. The
handler is therefore the conformant choice here; the static path is noted as a
possible future throughput optimization for plaintext only.

## Layout

```text
scripts/techempower/
  verify.sh                       # local conformance + optional wrk read
  postgres.yml                    # local Postgres for the /db tests (not submitted)
  seed.sql                        # World table seed (10,000 rows)
  Rust/mq-bridge/                 # Rust entry (cargo binary)
    src/main.rs
    Cargo.toml                    # path-dep on this repo; swap to git tag for the PR
    benchmark_config.json
    mq-bridge.dockerfile
  Python/mq-bridge-py/            # Python entry
    server.py
    benchmark_config.json
    mq-bridge-py.dockerfile
```

## Verify locally

```bash
# Rust: builds and checks /json + /plaintext conform, then a quick wrk read if present
scripts/techempower/verify.sh rust

# Rust including the DB tests: bring up local Postgres, then point DATABASE_URL at it
docker compose -f scripts/techempower/postgres.yml up -d
DATABASE_URL="postgres://benchmarkdbuser:benchmarkdbpass@127.0.0.1:5433/hello_world" \
  scripts/techempower/verify.sh rust
docker compose -f scripts/techempower/postgres.yml down -v

# Python: requires the wheel installed in the active venv
#   (cd python/mq-bridge-py && uv run maturin develop --release \
#       --no-default-features -F http -F pyo3/extension-module)
scripts/techempower/verify.sh python
```

For a head-to-head throughput comparison against FastAPI/Sanic/etc., the
existing wrk harness still applies:

```bash
cd python/mq-bridge-py && uv run python analysis/bench_http_native.py
```

## Submitting upstream

1. **Release first.** The Dockerfiles `git clone` this repo at a ref (`MQB_REF`,
   default `python`). Push these files and pin `MQB_REF` to a released tag so the
   image builds reproducibly. Likewise swap the Rust `Cargo.toml` path dependency
   for the matching `git`/crates.io release.
2. Fork `TechEmpower/FrameworkBenchmarks` and copy:
   - `scripts/techempower/Rust/mq-bridge/`   → `frameworks/Rust/mq-bridge/`
   - `scripts/techempower/Python/mq-bridge-py/` → `frameworks/Python/mq-bridge-py/`
3. Build/validate with their toolset (`./tfb --mode verify --test mq-bridge mq-bridge-py`)
   and open the PR. Note this is a maintenance commitment: entries that break a
   later round's CI get disabled until fixed.

> Honest framing for the PR: `mq-bridge-py` is the interesting entry (top-tier
> Python JSON throughput via off-GIL serialization). The Rust entry is a
> mid-pack hyper-based result — it optimizes for bridging/routing, not raw req/s.
