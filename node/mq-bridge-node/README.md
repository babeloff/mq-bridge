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
