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

The constructors mirror the Python bindings so the same names work across both:

- `Route.fromFile(path, name?)` loads from a YAML/JSON file
- `Route.fromStr(text, name?)` loads from an in-memory string
- `Route.fromConfig(obj, name?)` loads from a JS object
- `Publisher.fromFile` / `Publisher.fromStr` / `Publisher.fromConfig` build a publisher endpoint

The `name` is optional: pass it to pick one entry out of a `routes:`/`publishers:`
document, or omit it to treat the whole config as a single bare route/endpoint body.
`fromYaml` / `fromYamlStr` remain as deprecated aliases for `fromFile` / `fromStr`.
