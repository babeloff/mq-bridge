# Changelog

All notable changes to `mq-bridge`. Newest first.

## 0.3.10


### Changed

- DLQ and output middleware now run around the publish step, not inside the handler, so a
  handler failure is no longer retried by the output chain.
- Errors carry their full cause chain instead of only the outermost context.
- `deduplication` on an **output** endpoint is now a startup error instead of a warning and a
  silent no-op. Move it to the route's input endpoint.
- `limiter` paces a single `send_batch` by its own message count, so one large batch is no
  longer a free burst. Sustained throughput is unchanged.
- A `message_id` that is neither a UUID nor a `u128` is hashed to a stable id rather than
  making the whole JSON envelope unparseable.

### Performance

- **Postgres / sqlx**: `test_before_acquire(false)` on the pool, zero-copy row encoding via
  a prebuilt `JsonRowSchema`, and prebuilt first/next page queries on the cursor path.
- **Command handler**: `send_batch` no longer loops per-message `send()` (≈8x on batched routes).
- **File**: single-pass byte-array decode, faster CSV writes, compression sniffing.

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
- Drain no longer hangs on an empty source, and reconnect attempts are bounded.
- CSV reader handles quoted newlines; file endpoints fail fast on an unopenable path.
