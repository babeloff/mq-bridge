# File (CSV / JSON / JSONL)

Reads from or writes to a local file. Useful as a one-shot source/sink for
migrating data in or out of the other connectors.

## URL format

```text
file:///absolute/path/to/file?format=<normal|json|text|raw|csv>
```

The path comes from the URI path itself (`file:///...`), not a query param.
`format` defaults to `normal` (the full message serialized as JSON).

## Examples

**Load a CSV file into MongoDB, one-shot (first row = header):**

```bash
mqb copy --drain \
  --from file:///data/customers.csv?format=csv \
  --to 'mongodb://localhost?database=app&collection=customers'
```

**Export a table to JSONL, one-shot:**

```bash
mqb copy --drain \
  --from postgres://user:pass@localhost/app?table=orders \
  --to file:///data/orders.jsonl?format=json
```

**Tail a file as it grows (broadcast/subscribe mode), continuous:**

```bash
mqb copy \
  --from file:///var/log/app/events.log?mode=subscribe \
  --to kafka://kafka.local:9092?topic=app-events
```

## Key options

| Option | Purpose |
|---|---|
| `format` | `normal`, `json`, `text`, `raw`, or `csv`. |
| `delimiter` | Message delimiter. Defaults to newline. |
| `mode` | Consumer only: `consume` (from start), `subscribe` (tail from end), or persistent offset-tracked modes. |
| `compression` | Compress/decompress each batch: `none` (default), `gzip`, `lz4`, `zstd` (needs the `compression` build feature). A source must declare the same codec the file was written with. See [Compression](../cookbook/compression.md). |

Full field list: [reference/file.md](../reference/file.md).
