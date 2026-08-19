# Changelog

All notable changes to `mq-bridge`. Newest first.

## 0.4.4

### Breaking

- **`idempotency` on the `file` and `object_store` sinks is replaced by `name_by`.** The old
  field described a consequence rather than the thing it set. What it actually chooses is how
  written objects and parts are *named*, and everything else — sortability, replay dedup,
  whether `date_partition` applies, how errors are reported — follows from that name:

  | `name_by` | Name | Consequence |
  | --- | --- | --- |
  | `write_time` | `<uuidv7>.<ext>`, optionally under `YYYY/MM/DD/` | unique per write, sorts by write order |
  | `source_position` | `part-<topic>-<partition>-<start>-<end>.<ext>`, zero-padded, flat | repeats exactly on a replay, sorts by source order |
  | `auto` (new default) | `source_position` where the input carries a replay position, else `write_time` | see below |

  `idempotency: true|false` is still read and still means
  `name_by: source_position|write_time`, so v0.4.x configs keep loading. An explicit `name_by`
  wins over it.

- **An `object_store` sink now defaults to `source_position` naming over a replayable input**
  (Kafka, Postgres CDC, a SQL cursor, a MongoDB change stream, or a file). Key order is the
  only order a bucket has, so this is what makes `mqb copy orders.csv s3://…` come back in
  source order at any `concurrency` without configuring anything — and it is what removed the
  `concurrency > 1` warning from that route. Three consequences to know about:
  - **A re-run into the same prefix writes nothing — where the source repeats its
    positions.** The names repeat, and a repeat is a no-op. That covers a re-read of the same
    file, Kafka offsets, a SQL cursor and a CDC stream. A file source in `subscribe` or
    `group_subscribe` mode is the exception: it stamps a per-run epoch, so its names stay
    distinct across runs and a re-run writes a second copy. Where the no-op does apply it is a
    change of behaviour — the same command used to append a second copy under fresh uuid
    names. Use a fresh prefix, or `name_by: write_time`, if you want the old behaviour. The
    no-op also needs the prefix listing to succeed: a replay whose batches fall on different
    boundaries names different objects, so where the listing is denied the sink can only catch
    a repeat that lands on the exact same name and the rest is written twice.
  - **`date_partition` does not apply** under `source_position`; parts are written flat.
  - **Do not mix the two families in one prefix.** uuid names and `part-…` names do not sort
    correctly against each other. A prefix written by an older version keeps working on its
    own; point the upgraded route at a new prefix if a reader depends on listing order.

- **The `file` sink stays on `write_time` under `auto`** and must be told
  `name_by: source_position` explicitly. There, positional naming turns `path` from a file
  into a *directory* of part files — a change of structure, not of name, and not something to
  derive on the operator's behalf.

- `object_store` `date_partition` is now `Option<bool>` (unset = on, and only for
  `write_time`). Side effect: the "`date_partition` is a publisher-only option" warning used
  to fire for *every* `object_store` consumer, because the field defaulted to `true` and the
  check could not tell that from an explicit setting. It now fires only when the field was
  actually set.

### Changed

- **The `object_store` sink lists its prefix only on the `source_position` write path**, and
  only once per publisher, immediately before its first write rather than when it opens. A
  `write_time` sink never lists at all; a `source_position` restart into a populated prefix
  pays for exactly one `LIST` and then skips the rest of the replay without re-encoding or
  re-uploading. The listing cannot be deferred any further than this: `PutMode::Create` only
  catches a repeat that lands on the same object name, so a replay whose batches fall on
  different boundaries collides with nothing and would otherwise write every record a second
  time under overlapping names. A listing that fails is retried on the next batch, unless it
  failed for a reason retrying cannot clear (a denied `ListObjects`), in which case the sink
  warns once and falls back to same-name-only deduplication.
- **The `concurrency > 1` warning now fires only for a `write_time` name**, which is the only
  case where write order is not source order — under `auto` over a replayable input that is
  now nobody. It names `name_by: source_position` where the input carries a replay position
  and `concurrency: 1` otherwise, rather than pointing at a setting that would fail on an
  input with no position.
- The `source_position` write path reports `SentBatch::Partial` like the ordinary path does.
  A record that cannot be encoded splits the run instead of failing the whole batch: the
  offsets around it are written under names covering exactly what went in, and the bad record
  goes to the DLQ. (Encode failures are unreachable in practice today; this removes an
  asymmetry rather than fixing an observed bug.)

### Fixed

- A failed prefix listing no longer fails an `object_store` batch. The error was propagated as
  non-retryable, which dropped a replay's batch on a bucket that denies `ListObjects`, or on a
  transient listing hiccup. It is now logged and the sink degrades to same-name-only
  deduplication: a transient failure is retried on the next batch, while a denied `ListObjects`
  is latched after one warning and not retried.

- A route rejected for `name_by: source_position` over an input that carries no replay position
  no longer leaves a directory behind at a `file` sink's `path`. The publisher opens before the
  consumer, and the part-file sink creates its directory as it opens, so the rejected route used
  to turn the operator's output *file* into an empty directory — after which a corrected run
  failed on the leftover. The requirement is now checked before any sink opens.

## 0.4.0

### Breaking

- **Changed defaults.** Four defaults were chosen for safety or speed rather than history.
  Set the field explicitly to keep the old behaviour:
  - `batch_size` is **512** instead of 1. Batches fill opportunistically — the consumer waits
    for one message and takes whatever else is already there — so this raises throughput
    without adding idle latency. It does widen the blast radius of a failed batch, and
    nothing caps a batch by bytes, so lower it on routes carrying large payloads.
  - MongoDB `consume` defaults to **`capture_all`** instead of `consumer`. The old default
    claimed, re-fetched and **deleted** each document, so pointing a route at a collection to
    read it destroyed it — and it only ever worked on collections written by the mq-bridge
    MongoDB publisher. `capture_all` is non-destructive, reads arbitrary collections, and is
    ~5x faster. On a replica set it snapshots, then follows the change stream; a one-shot
    `exit_on_empty` / `--drain` job finishes once that stream goes quiet.
    **`capture_all` and `capture_new` now require a replica set** (a single-node one is enough)
    and refuse to start without one. `capture_all` used to fall back to paging `_id` forward on
    a standalone `mongod`; that reader only ever matches ids above its high-water mark, so any
    document a concurrent writer commits below it was dropped — silently, with no error and no
    gap in the delivery count. On a standalone server use the new `consume: snapshot`, or
    `consume: consumer` for a work queue.
  - MongoDB `consume: subscriber` and the `MongoDbSubscriber` type are **removed**. It polled
    `seq > last_seq` and advanced the watermark to the highest seq it saw, so a batch whose seq
    block was reserved first but committed second was skipped for good — the same silent loss as
    above, and present on a replica set too. Once a replica set is required it is also redundant:
    `consume: capture_new` without a `cursor_id` is ephemeral fan-out from now on, and reads
    arbitrary collections rather than only the bridge's own wrapped documents. The deprecated
    `change_stream: true` boolean now resolves to `capture_new` instead of `subscriber`.
  - ZeroMQ `format` defaults to **`raw_framed`** instead of `json`: binary-safe payloads with
    a JSON metadata frame in front, so headers still travel. This is a wire-format change —
    a 0.4 peer and a 0.3 peer no longer interoperate on the same socket unless one sets
    `format: json`.
  - ZeroMQ `backend` defaults to **`try_omq`**, which uses the faster omq backend when the
    `zeromq-omq` feature is compiled in and falls back to `zmq` otherwise. Naming `omq` or
    `zmq` explicitly still makes that backend a hard requirement.
- `extensions::register_endpoint_factory` and `register_middleware_factory` return
  `anyhow::Result<()>` instead of `()`, and registering a name that is already taken is now an
  error instead of silently replacing the previous factory. A duplicate name meant one of the
  two registrations was quietly ignored, which is impossible to diagnose from a route that then
  behaves like the wrong endpoint. Existing callers add `?` or `.unwrap()`; anything that relied
  on re-registering the same name must pick distinct names.

### Added

- MongoDB `consume: snapshot` — a one-shot, non-destructive read that pages the collection by
  `_id` and ends the route on drain. It needs no replica set, reads arbitrary collections, and is
  the supported way to read a standalone `mongod` without deleting anything. Its contract is
  deliberately narrow: it delivers what exists when the run starts, and it is **not** a tail.
  `cursor_id` is rejected at startup — resuming above a stored `_id` would skip whatever a
  concurrent writer commits below that mark, and `_id` is assigned client-side before the insert,
  so it does not follow commit order. Incremental reads need commit order, i.e. the oplog, i.e. a
  replica set.
- gRPC consumers can call arbitrary unary and server-streaming services without generated
  Rust clients. Point an endpoint at a compiled protobuf `FileDescriptorSet`, name the
  service and method, and provide the request as JSON; responses are decoded dynamically
  with `prost-reflect` and emitted using protobuf's canonical JSON representation. The
  existing generated `mqbridge.Bridge` protocol remains the default.
- The omq ZeroMQ backend covers **REQ/REP** as well as PUSH/PULL and PUB/SUB, so the whole
  `zeromq` endpoint surface — including request-reply — works on either backend. REQ exchanges
  are serialised and bounded by `request_timeout_ms`, and a timed-out socket is rebuilt rather
  than reused, because ZMTP requires strict send/recv alternation.
- Custom endpoints and middleware can be written in Python and Node.js, not just Rust —
  `register_endpoint` / `register_middleware` in both bindings, with the same batch, ack and
  request-reply semantics as a Rust `CustomEndpointFactory`. See [EXTENDING.md](docs/EXTENDING.md).
- Native endpoint plugins. An endpoint can live in its own crate and package and be loaded
  into any mq-bridge process at runtime — `mq_bridge::plugin::load_endpoint_plugin(path)` in
  Rust, `mq_bridge.load_endpoint_plugin(path)` in Python, `loadEndpointPlugin(path)` in
  Node.js — so a broker's dependencies stay out of curated mq-bridge builds while every
  language runs the same implementation and delivery semantics. The `plugin` feature (part of
  `full` and `portable`) provides the loader; `plugin-sdk` provides the authoring side:
  `export_endpoint_plugin!`, which exports an ordinary `CustomEndpointFactory` through the
  stable C ABI with no handwritten `unsafe`, and a conformance suite to run against the
  endpoint both linked directly and loaded as a plugin. A plugin can provide a
  middleware too (`export_middleware_plugin!`, or `middleware:` alongside an
  endpoint): it returns one entry per message — `None` drops it — while the
  wrapper around the endpoint stays on the host side, so nothing calls back
  across the boundary. See [PLUGINS.md](docs/PLUGINS.md).

### Fixed

- The built-in `mqbridge.Bridge` gRPC transport now commits messages with real ACK/NACK
  RPCs instead of a no-op. Embedded publishers wait for downstream processing to commit
  before receiving an ACK, and unacknowledged subscription messages are retained in memory
  and redelivered to the same `consumer_id`. This provides at-least-once delivery while the
  server process is running; durable restart recovery and exactly-once processing still
  require persistent state or downstream deduplication by message ID. Arbitrary dynamic
  services retain the delivery semantics of their own API because protobuf descriptors do
  not define a generic acknowledgement operation.
  Retention is capped (1024 messages per subscriber, 64 subscribers per route, oldest
  evicted first) so a consumer that never acknowledges cannot grow the server without bound.
  `consumer_id` defaults to a fresh id per consumer rather than to the topic, so competing
  consumers on one topic no longer share a retention set; set it explicitly to be
  redelivered unacknowledged messages after a reconnect.
- A **non-retryable handler failure no longer discards the rest of its batch**. The default
  `Handler::handle_many` aborted the remaining messages after any failure; for retryable and
  connection errors that is right (the batch is nacked and redelivered together), but a
  non-retryable message is dropped and never redelivered, so every healthy message behind it
  was silently lost. How many depended purely on `batch_size` — at the old default of 1 the
  collateral was zero, which is why this went unnoticed. The behaviour now matches what
  `CommandPublisher::send_batch` already documented and tested.
- ZeroMQ REQ/REP with `format: raw` or `raw_framed` decoded the reply using that format, but the
  REP side always answers with a JSON array of canonical messages, so the caller got one message
  whose payload was the JSON text instead of the decoded replies. Both backends now decode replies
  as JSON. This was invisible while `json` was the default format.

## 0.3.10


### Changed

- DLQ and output middleware now run around the publish step, not inside the handler, so a
  handler failure is no longer retried by the output chain. The tradeoff: a handler failure
  now skips the output middleware entirely, so `dlq` on the output endpoint cannot capture
  it — only publish failures reach the DLQ. Put a `dlq` on the input endpoint to catch
  handler failures.
- Errors carry their full cause chain instead of only the outermost context.
- `deduplication` on an **output** endpoint is now a startup error instead of a warning and a
  silent no-op. Move it to the route's input endpoint.
- `limiter` paces a single `send_batch` by its own message count, so one large batch is no
  longer a free burst. Sustained throughput is unchanged.
- A `message_id` that is neither a UUID nor a `u128` is hashed to a stable id rather than
  making the whole JSON envelope unparseable.

### Performance

- **Kafka consumer**: a long-lived prefetch task reads librdkafka continuously into a bounded
  channel, instead of rebuilding the stream inside every `receive_batch`. librdkafka only keeps
  requesting records while its queue is drained, so every pause the pipeline took — a transform,
  a slow sink — was also a pause in fetching, and the fetch rate collapsed to well under what the
  broker could serve. Batch offsets are also recorded once per partition rather than once per
  message, which was O(n²) in the batch and allocated two `CString`s per record.
  Kafka → transform → file: **192,854 → 824,983 rows/s** on the 1M-row, four-partition
  benchmark, from 0.35x to 1.51x Arroyo on identical output. A 16,384-message passthrough
  batch went from 11.5s to 2.2s, and from 10.4s to 0.9s of CPU.
- **Postgres / sqlx**: `test_before_acquire(false)` on the pool, zero-copy row encoding via
  a prebuilt `JsonRowSchema`, and prebuilt first/next page queries on the cursor path.
- **Command handler**: `send_batch` no longer loops per-message `send()` (≈8x on batched routes).
- **File**: single-pass byte-array decode, faster CSV writes, compression sniffing.
- **Deduplication**: the two-phase reserve/commit no longer writes to the store twice per
  message. Reservations are held in memory — sled takes an exclusive file lock on its directory,
  so a claim only ever has to be visible to this process — leaving one write per message, on
  commit.

### Fixed

- `insert_query` batch inserts dropped anything but a bare token from the `VALUES` tuple, so
  `decode(${payload:x}, 'base64')` and casts were silently discarded — a binary (`bytea`)
  column could not be written. The batch path now keeps the user's SQL and falls back to
  iterative inserts when the tuple contains an expression.
- A payload string containing an embedded NUL byte was dropped by the driver and stored as
  SQL `NULL` while the route reported success. It is now rejected as non-retryable.
- Database errors were rendered twice, because `sqlx` errors already display their own
  source. Cause chains now skip a link an earlier one already contains.
- `cookie_jar`'s `inject_metadata` resolved only a bare stored name, so the namespaced
  `cookie.<name>` / `value.<name>` spelling that `export_metadata_prefix` reports back
  injected nothing. Both spellings now work.
- MQTT publishes are confirmed by PUBACK/PUBCOMP and guarded against session resets, which
  took chaos-test message loss to zero.
- Postgres CDC advances its replication slot durably on shutdown instead of leaving the
  feedback unflushed.
- `deduplication` could silently drop a message after a crash. The reservation was written to
  the store *before* the message was processed, so a redelivery arriving within the 5s pending
  TTL was classified as a duplicate and acked — without ever having been written to the sink.
  Reservations are now in-memory and die with the process, so the redelivery is reprocessed.
- A Kafka record with no value — a tombstone, which is ordinary traffic on a compacted topic —
  failed the whole batch. Its offset was never committed, so the route reconnected onto the same
  record forever: one tombstone wedged the consumer permanently, and a compacted topic could not
  be consumed at all. Tombstones now arrive as an empty payload flagged `mqb.kafka.tombstone`.
- `--drain` on a Kafka source could report success having copied only part of a topic, or none
  of it. An idle wait was taken to mean "source exhausted", but an idle channel means nothing on
  its own: before the first fetch lands, or in an ordinary gap between fetches, it looks exactly
  the same as the end of the data. The shorter the idle timeout the more went missing — at 1ms a
  1,000,000-row topic landed 0 rows, and the copy still exited successfully. A drain now
  completes only once every assigned partition has reached the offset it held when the drain
  began, and lands all 1,000,000 rows at every idle timeout including 0.
- Drain no longer hangs on an empty source, and reconnect attempts are bounded.
- CSV reader handles quoted newlines; file endpoints fail fast on an unopenable path.
