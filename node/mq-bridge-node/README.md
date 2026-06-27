# mq-bridge Node bindings

Native Node.js bindings for `mq-bridge`, built with `napi-rs`.

This package is server-side only. It loads a native `.node` addon and does not run in browsers.

## Build

```sh
npm install
npm run build:basic
```

For the fastest local smoke test, build only the HTTP + middleware surface used
by the example:

```sh
npm run build:ci
npm run example
```

## Handler shape

```ts
import { Message, Route } from "mq-bridge";

const route = Route.fromStr(config, "orders");

route.withHandler(async (message) => {
  const data = message.json();
  return Message.fromJson({ ok: true, data });
});

route.addHandler("order.created", async (data) => {
  return Message.fromJson({ seen: data });
});

route.start();
```

Middleware is configured through the normal mq-bridge route config.

## Loading config

The constructors mirror the Python bindings — the same capabilities are exposed
in both, with Node using camelCase names where Python uses snake_case:

- `Route.fromFile(path, name?)` loads from a YAML/JSON file
- `Route.fromStr(text, name?)` loads from an in-memory YAML/JSON string
- `Route.fromConfig(obj, name?)` loads from a JS object
- `Publisher.fromFile` / `Publisher.fromStr` / `Publisher.fromConfig` build a publisher endpoint

The `name` is optional: pass it to pick one entry out of a `routes:`/`publishers:`
document, or omit it to treat the whole config as a single bare route/endpoint body.
`fromYaml` / `fromYamlStr` remain as deprecated aliases for `fromFile` / `fromStr`.

## Pull-based consumer

`Route` is push-based. To pull messages on your own schedule, use `Consumer`,
which wraps any input endpoint:

```js
import { Consumer } from "mq-bridge";

const consumer = Consumer.fromConfig({ nats: { subject: "orders", url: "nats://localhost:4222" } });

while (!consumer.exhausted) {
  const batch = await consumer.poll(500, 1000); // [] on timeout
  if (batch.length === 0) continue;
  for (const message of batch) handle(message.json());
  await consumer.commit(); // ack only after handling
}
```

`poll(max?, timeoutMs?)` returns up to `max` messages (default 256) without
acking, waiting up to `timeoutMs` milliseconds (omit it to block until a message
arrives); `commit()` acks every batch returned since the last commit. Committing
only after the downstream write succeeds gives at-least-once delivery. `exhausted`
turns `true` once a bounded source (e.g. a file) is drained; streaming brokers
never set it. `Consumer.fromFile` / `fromStr` accept a named entry under a
`consumers:` section or a single bare endpoint body.

`consumer.status()` resolves to a snapshot (`healthy`, `target`, `pending`,
`capacity`, `error`, `details`). `pending` is the broker backlog/lag where the
transport reports it (Kafka offset lag, AMQP queue depth, NATS JetStream
`numPending`), so `pending === 0` is a precise "caught up" check; it's absent
where the broker exposes no backlog (core NATS, MQTT). `consumer.close()`
releases the connection — idempotent, and `poll()`/`status()` reject afterwards.
Node is garbage-collected, so close explicitly rather than waiting for GC.

> **You must call `commit()` — it is not optional.** It is the only thing that
> tells the broker a batch is done. If you keep polling without committing, the
> offset never advances (every message is **re-delivered** on the next run), most
> brokers **stall** once their unacknowledged/prefetch window fills, and the
> uncommitted batches are held in memory so the process **grows unbounded**.
> Commit after each batch you have durably handled; to retry a failed batch,
> simply don't commit it.

## Analysis

HTTP comparison benchmark, driven by a native load generator (`wrk`) so the
client is never the bottleneck. It boots each server in its own process — the
mq-bridge Node route plus raw Node `http`, uWebSockets.js, Fastify, and Express
(all peers run only when installed) — and drives each with `wrk`:

```bash
cd node/mq-bridge-node
npm run build                 # release addon (the bench is meaningless on a debug build)
npm i -D fastify express      # optional Node HTTP peers
npm i -D uNetworking/uWebSockets.js#v20.51.0   # the http-arena Node leader (native, from GitHub)
npm run bench:http -- --connections 1,8,32 --duration 8
```

Requires `wrk` on `PATH` (`brew install wrk`). The `mqb` target needs the addon
built with the `http` feature (`npm run build` or `npm run build:ci`). Each
server echoes a tiny JSON body with `value` incremented; mq-bridge routes on the
`kind` header while the framework peers post plain JSON.

`uws` (uWebSockets.js) is the framework that tops public Node benchmarks like
http-arena and TechEmpower — it bypasses Node's `http` stack with a C++ socket
layer, so it is the most demanding yardstick here.
