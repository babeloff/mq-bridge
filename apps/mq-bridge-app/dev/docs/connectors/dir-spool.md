# Directory spool

Stores each message as a payload file in a directory, with an optional JSON
metadata sidecar. Use it as a durable FIFO hand-off between processes when you
want a queue that can be inspected with ordinary filesystem tools and do not
want to operate a broker.

Unlike the [file connector](./file.md), which frames many records in one file,
the directory spool writes one opaque payload per file. A producer may exit
while a consumer continues draining the backlog.

## URL format

```text
dir-spool:///absolute/path/to/spool?<option>=<value>
```

The aliases `spool://` and `dirspool://` are also accepted. The directory path
comes from the URI path, not a `path` query parameter.

## Examples

**Write a finite input to a spool and mark it complete:**

```bash
mqb copy --drain \
  --from file:///data/orders.jsonl?format=json \
  --to 'dir-spool:///var/spool/orders?emit_done=success'
```

**Drain that spool into PostgreSQL, then exit:**

```bash
mqb copy --drain \
  --from 'dir-spool:///var/spool/orders?stop_on_done=true' \
  --to 'postgres://user:pass@localhost/app?table=orders'
```

This one-shot example starts after the producer command has completed, so
`--drain` exits when the backlog is empty. In a continuously running route,
`stop_on_done=true` ends the source only when both the queue is empty and the
producer's `DONE` sentinel exists. Set `emit_done` only on the last producer;
a producer opening the spool removes a stale sentinel before writing again.

**Shard a high-volume spool:**

```bash
mqb copy \
  --from mqtt://broker.local:1883?topic=telemetry \
  --to 'dir-spool:///var/spool/telemetry?naming_pattern={seq:012}&shard_depth=2&shard_width=3'
```

Configure the consumer with the same `shard_depth`, `shard_width`, payload
extension, and metadata extension. Sharding uses leading sequence digits as
subdirectories and avoids placing an unbounded number of files in one
directory.

## Delivery and concurrency

Chunks are delivered in lexical filename order. Keep a zero-padded sequence at
the start of `naming_pattern`; the default `{seq:09}` is safe. The producer
writes temporary files and renames them into place, so the consumer does not
observe a partial chunk. With the default `fsync=chunk`, acknowledged writes
are also flushed to durable storage.

A draining consumer deletes a chunk only after its message is acknowledged. A
nack leaves the chunk on disk for redelivery. Set `drain_on_read=false` for a
non-destructive pass that leaves files in place.

By default, one producer and one draining consumer may use a spool at the same
time. Separate `PRODUCER` and `CONSUMER` lock files enforce those roles without
preventing the normal producer-consumer pair. Avoid disabling these claims:
multiple producers can collide, and multiple draining consumers can deliver
duplicates because claiming is in memory rather than an on-disk rename.

## Key options

| Option | Purpose |
|---|---|
| `naming_pattern` | Sink only: chunk name template. It must begin with a sequence; default `{seq:09}`. |
| `payload_extension` / `metadata_extension` | File suffixes. Set `metadata_extension` to an empty string to omit sidecars. |
| `atomic` | Sink only: write through a temporary file and rename; default `true`. |
| `fsync` | `chunk` (default) for durable writes, or `off` for higher throughput with weaker crash guarantees. |
| `emit_done` | Sink only: write the completion sentinel on `success`, on any `end`, or `never` (default). |
| `stop_on_done` | Source only: exit after the sentinel is present and the backlog is empty. |
| `drain_on_read` | Source only: delete acknowledged chunks; default `true`. |
| `shard_depth` / `shard_width` | Spread chunks across sequence-derived subdirectories. Both ends must agree. |
| `claim` | `exclusive` (default), `warn`, or `off` for same-role locking. |

Full field list: [reference/dir-spool.md](../reference/dir-spool.md).

For detailed durability, sharding, lock, and completion-sentinel behavior, see
the [configuration guide](../engine/configuration.md#directory-spool-dir_spool).
