# Benchmark prompt — ETL / data-movement comparison via mq-bridge-app

Hand this to a session working in the **`mq-bridge-app`** repo. It defines *what* to
benchmark and *what to report*; the concrete app config / UI wiring is decided there.

## Goal

Produce credible, **like-for-like ETL / data-movement numbers for mq-bridge, run
through `mq-bridge-app`** — the zero-code, config-driven path a real user would take —
not a bespoke Rust harness. The numbers must sit next to industry references
(**Debezium** for CDC, **OpenMessaging Benchmark** / **Airbyte** for throughput)
without an apples-to-oranges disclaimer.

## Why through mq-bridge-app

`mq-bridge-app` runs the same engine configured purely by YAML/env. Benchmarking through
it means the published figure reflects what a no-code user actually gets, and matches how
competing ETL/CDC tools are benchmarked (config in → data moved out). This is the credible
framing, not a Criterion micro-harness.

## Scenarios (mirror `benches/ETL_BENCHMARKS.md` in the library repo)

1. **Bulk-insert throughput** — `memory → sqlx(postgres)` sink, rows/sec at batch 1 and 128.
   Answers "how fast can it load a table". Compare to Airbyte records/sec.
2. **CDC event-to-sink latency** — insert a row → `postgres_cdc` → `memory`/`null` sink,
   report p50/p95/p99. Answers "how fast does a change propagate". Compare to Debezium.
3. **Batched vs unbatched throughput** — the same route at `batch_size: 1` vs `128`, to
   quantify the batching lever.

## Fixed parameters (hold constant, print next to every number)

| Parameter    | Value                                             |
| ------------ | ------------------------------------------------- |
| Payload      | 256 B and 4 KiB JSON rows (report both)           |
| Message count| 100_000 per run                                   |
| Batch sizes  | 1 (unbatched) and 128 (batched)                   |
| Concurrency  | 1 and 4 route workers                             |
| Postgres     | `postgres:16-alpine`, `wal_level=logical`         |
| Warm-up      | 5_000-message pre-roll                            |
| Environment  | record CPU model, cores, RAM, mq-bridge version   |

No cherry-picking — publish the methodology row with every number.

## Reference baselines to line up against

- **Debezium** (Postgres CDC latency/throughput) → scenario 2.
- **OpenMessaging Benchmark** payload sizes + latency-percentile reporting → scenarios 1/3.
- **Airbyte** records/sec → scenario 1.

## Deliverable

A results block (scenario × parameters → throughput/latency) plus an environment header,
formatted to **paste straight into `benches/ETL_BENCHMARKS.md` → "Results" section** in the
mq-bridge library repo (currently a placeholder).

## To work out in mq-bridge-app (the details)

- Exact YAML route configs per scenario (source generator, sink, batch/concurrency knobs).
- How to drive N messages: UI-triggered vs config-only vs env; the load/source generator.
- Reuse the Postgres compose from the library repo
  (`tests/integration/docker-compose/postgres_cdc.yml`, `wal_level=logical`).
- Where run artifacts/results live in the app repo, and how they're published
  (the library already links a criterion dashboard at
  https://marcomq.github.io/mq-bridge/dev/bench/).
