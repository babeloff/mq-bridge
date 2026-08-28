# Weak join / correlation

The [`weak_join`](../engine/reference.md#weak_join) middleware correlates messages by a metadata
key and emits them as one joined message. **Input only.** It has two modes.

## Count mode

Wait for any N messages sharing a correlation key, then emit them as a JSON array:

```yaml
input:
  middlewares:
    - weak_join: { group_by: "correlation_id", expected_count: 3, timeout_ms: 5000 }
  kafka: { topic: "fragments", url: "localhost:9092" }
```

## Branch mode

Set `branch_by` to wait for **named** branches (e.g. one message from `inventory` and one from
`pricing`), then emit a branch-keyed JSON object. `required` overrides `expected_count`:

```yaml
- weak_join:
    group_by: "correlation_id"
    expected_count: 2
    timeout_ms: 5000
    branch_by: "source"
    required: ["inventory", "pricing"]
    on_timeout: discard
```

From the `copy` CLI, object/array fields take a JSON literal:

```bash
--from '...|weak-join?group_by=cid&expected_count=2&timeout_ms=1000&required=["inventory","pricing"]'
```

## On timeout

An incomplete group is either emitted partially (`on_timeout: fire`, the default) or dropped
(`on_timeout: discard`).

> **Durability caveat.** Messages are acknowledged on receipt, so a crash before the group
> completes **loses the buffered members** — `weak_join` correlates in memory and is not a
> durable join. For correlation that must survive a restart, land the fragments in a store and
> join there.

Full field list in the [middleware reference → `weak_join`](../engine/reference.md#weak_join).
