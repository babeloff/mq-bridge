# Changelog

All notable changes to `mq-bridge`. Newest first.

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
