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
import { Message, Route } from "@mq-bridge/node";

const route = Route.fromYamlStr(config, "orders");

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
