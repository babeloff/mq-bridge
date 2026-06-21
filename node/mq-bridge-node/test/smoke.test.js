"use strict";

const assert = require("node:assert/strict");
const net = require("node:net");
const test = require("node:test");
const { Message, Publisher, Route } = require("..");

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

async function postWithRetry(url, body, headers = {}) {
  let lastError;
  for (let attempt = 0; attempt < 30; attempt += 1) {
    try {
      return await fetch(url, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify(body),
      });
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  throw lastError;
}

function httpRouteConfig(routeName, port, path, withMetrics = false) {
  const metrics = withMetrics
    ? `      middlewares:
        - metrics: {}
`
    : "";

  return `
routes:
  ${routeName}:
    input:
${metrics}      http:
        url: "127.0.0.1:${port}"
        path: "${path}"
        method: "POST"
    output:
${metrics}      response: {}
`;
}

test("Message round-trips JSON, text, metadata, and ids", () => {
  const json = Message.fromJson({ value: 42 }, { kind: "demo" });
  assert.deepEqual(json.json(), { value: 42 });
  assert.equal(json.metadata.kind, "demo");
  assert.equal(json.id, null);

  const raw = new Message(Buffer.from("hello"), { source: "test" });
  assert.equal(raw.text(), "hello");
  assert.equal(raw.metadata.source, "test");
});

test("Publisher.requestJson echoes through response endpoint", async () => {
  const publisher = Publisher.fromYamlStr(
    `
publishers:
  echo:
    response: {}
`,
    "echo",
  );

  const response = await publisher.requestJson({ orderId: 7 }, { kind: "order.test" });
  assert.deepEqual(response.json(), { orderId: 7 });
  assert.equal(response.metadata.kind, "order.test");
});

test("Route.withHandler serves an HTTP response", async () => {
  const port = await freePort();
  const routeName = `node_with_handler_${port}`;
  const route = Route.fromYamlStr(
    httpRouteConfig(routeName, port, "/with-handler"),
    routeName,
  );

  route.withHandler(async (message) => {
    const data = message.json();
    return Message.fromJson(
      { handledBy: "withHandler", value: data.value },
      { "content-type": "application/json" },
    );
  });

  try {
    route.start();
    const response = await postWithRetry(
      `http://127.0.0.1:${port}/with-handler`,
      { value: 11 },
    );
    const text = await response.text();
    assert.equal(response.status, 200, text);
    assert.deepEqual(JSON.parse(text), {
      handledBy: "withHandler",
      value: 11,
    });
  } finally {
    route.stop();
    route.join();
  }
});

test("Route.addHandler dispatches by kind with middleware config", async () => {
  const port = await freePort();
  const routeName = `node_add_handler_${port}`;
  const route = Route.fromYamlStr(
    httpRouteConfig(routeName, port, "/typed-handler", true),
    routeName,
  );

  route.addHandler("order.created", async (data) => {
    return Message.fromJson(
      { handledBy: "addHandler", orderId: data.orderId },
      { "content-type": "application/json" },
    );
  });

  try {
    route.start();
    const response = await postWithRetry(
      `http://127.0.0.1:${port}/typed-handler`,
      { orderId: 42 },
      { kind: "order.created" },
    );
    const text = await response.text();
    assert.equal(response.status, 200, text);
    assert.deepEqual(JSON.parse(text), {
      handledBy: "addHandler",
      orderId: 42,
    });
  } finally {
    route.stop();
    route.join();
  }
});
