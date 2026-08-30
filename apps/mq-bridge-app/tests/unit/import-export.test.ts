// @vitest-environment jsdom

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  buildConsumerConfigExport,
  buildPublisherConfigExport,
  exportFullBundle,
  importAppConfigFromJsonText,
  importFromJsonText,
  resetAppConfigToDefaults,
} from "../../ui/src/lib/import-export";
import { appShell } from "../../ui/src/lib/app-shell";

describe("import-export", () => {
  const readFixture = (name: string) =>
    readFileSync(join(process.cwd(), "tests/fixtures/import-export", name), "utf8");

  beforeEach(() => {
    if (typeof window.localStorage?.setItem === "function") {
      window.localStorage.setItem("mqb_publisher_history", JSON.stringify({ version: 1, updated_at: 0, publishers: {} }));
    }
    window.appConfig = { publishers: [{ name: "orders_http" }], routes: {}, consumers: [], presets: {}, env_vars: {}, history: {} };
    appShell.setConfig(window.appConfig);
    let serverConfig: any = {
      publishers: [{ name: "saved_pub" }],
      routes: { r1: {} },
      consumers: [],
      presets: {},
      env_vars: {},
      history: {},
    };
    globalThis.fetch = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (String(input) === "/config" && init?.method === "POST") {
        try {
          const parsedBody = JSON.parse(String(init.body || "{}"));
          if (parsedBody && typeof parsedBody === "object") {
            serverConfig = parsedBody;
          }
        } catch {
          // ignore test mock parse failures
        }
        return Promise.resolve({ ok: true, text: async () => "" });
      }
      if (String(input) === "/config") {
        return Promise.resolve({
          json: async () => serverConfig,
        });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    }) as any;
  });

  test("imports native preset bundle into target local storage keys", async () => {
    const file = readFixture("mqb-presets.sample.json");

    const result = await importFromJsonText(file, {
      includeConfig: false,
      includePresets: true,
      targetPublisherName: "orders_http",
    });

    expect(result.importedKind).toBe("mqb-presets");
    expect(result.importedPresetCount).toBe(1);
    expect(window.appConfig.presets).toEqual({});
    expect(window.appConfig.publishers.map((row: any) => row.name)).toContain("orders_http - preset_1");
    expect(window.appConfig.env_vars).toEqual({
      baseUrl: "https://api.local",
    });
  });

  test("imports postman collection items into presets", async () => {
    const postmanCollection = readFixture("postman.collection.sample.json");

    const result = await importFromJsonText(postmanCollection, {
      includeConfig: false,
      includePresets: true,
      targetPublisherName: "orders_http",
    });

    expect(result.importedKind).toBe("postman");
    expect(result.importedPresetCount).toBe(1);
    const importedPublisher = window.appConfig.publishers.find((row: any) => row.name === "orders_http - Create order");
    expect(importedPublisher).toMatchObject({
      name: "orders_http - Create order",
      payload: "{\"order\":1}",
    });
    expect(importedPublisher.endpoint.http).toMatchObject({
      method: "POST",
      url: "https://api.test",
      path: "/orders",
    });
  });

  test("imports openapi paths as presets and captures baseUrl env var", async () => {
    const openApi = readFixture("openapi.sample.json");

    const result = await importFromJsonText(openApi, {
      includeConfig: false,
      includePresets: true,
      targetPublisherName: "orders_http",
    });

    expect(result.importedKind).toBe("openapi");
    const importedPublisher = window.appConfig.publishers.find((row: any) => row.name === "orders_http - Create order");
    expect(importedPublisher).toMatchObject({
      name: "orders_http - Create order",
    });
    expect(importedPublisher.endpoint.http).toMatchObject({
      method: "POST",
      url: "${baseUrl}",
      path: "/orders",
    });
    expect(window.appConfig.env_vars.baseUrl).toBe("https://openapi.example");
  });

  test("imports asyncapi channels as presets", async () => {
    const asyncApi = readFixture("asyncapi.sample.json");

    const result = await importFromJsonText(asyncApi, {
      includeConfig: false,
      includePresets: true,
      targetPublisherName: "orders_http",
    });

    expect(result.importedKind).toBe("asyncapi");
    const importedPublisher = window.appConfig.publishers.find((row: any) => row.name === "orders_http - Publish order created");
    expect(importedPublisher).toMatchObject({
      name: "orders_http - Publish order created",
    });
    expect(importedPublisher.endpoint.http).toMatchObject({
      method: "POST",
      url: "${baseUrl}",
      path: "/orders/created",
    });
    expect(window.appConfig.env_vars.baseUrl).toBe("mqtt://broker.local:1883");
  });

  test("imports config payload and replaces appConfig with refreshed server config", async () => {
    const bundle = readFixture("mqb-export.sample.json");

    await importFromJsonText(bundle, {
      includeConfig: true,
      includePresets: false,
      targetPublisherName: "orders_http",
    });

    expect(window.appConfig.publishers[0].name).toBe("incoming");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/config",
      expect.objectContaining({ method: "POST" }),
    );
  });

  test("exports full bundle as downloadable JSON", () => {
    window.appConfig.presets = { orders_http: [] };
    window.appConfig.env_vars = { baseUrl: "https://x" };

    const createObjectURL = vi.fn().mockReturnValue("blob:test");
    const revokeObjectURL = vi.fn();
    const click = vi.fn();

    globalThis.URL.createObjectURL = createObjectURL as any;
    globalThis.URL.revokeObjectURL = revokeObjectURL as any;
    const createElementSpy = vi.spyOn(document, "createElement").mockImplementation((tagName: string) => {
      const element = document.createElementNS("http://www.w3.org/1999/xhtml", tagName) as any;
      if (tagName.toLowerCase() === "a") {
        element.click = click;
      }
      return element;
    });

    exportFullBundle();

    expect(createObjectURL).toHaveBeenCalledTimes(1);
    expect(click).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalledTimes(1);
    createElementSpy.mockRestore();
  });

  test("builds one consumer as an importable config payload", () => {
    window.appConfig.env_vars = { baseUrl: "https://api.local" };
    appShell.setConfig(window.appConfig);

    const payload = buildConsumerConfigExport({
      name: "orders_in",
      endpoint: { http: { url: "${baseUrl}", path: "/orders" } },
      output: { mode: "none" },
    });

    expect(payload.type).toBe("mqb-config");
    expect(payload.config.consumers).toEqual([
      {
        name: "orders_in",
        endpoint: { http: { url: "${baseUrl}", path: "/orders" } },
        output: { mode: "none" },
      },
    ]);
    expect(payload.config.env_vars).toEqual({ baseUrl: "https://api.local" });
  });

  test("preserves MongoDB consume compatibility fields in consumer exports", () => {
    const payload = buildConsumerConfigExport({
      name: "mongo_in",
      endpoint: {
        mongodb: {
          url: "mongodb://localhost:27017",
          database: "app",
          collection: "orders",
          consume: "snapshot",
          change_stream: true,
        },
      },
      output: { mode: "none" },
    });

    expect(payload.config.consumers[0].endpoint).toEqual({
      mongodb: {
        url: "mongodb://localhost:27017",
        database: "app",
        collection: "orders",
        consume: "snapshot",
        change_stream: true,
      },
    });
  });

  test("builds one publisher as an importable config payload", () => {
    window.appConfig.env_vars = { baseUrl: "https://api.local" };
    appShell.setConfig(window.appConfig);

    const payload = buildPublisherConfigExport({
      name: "orders_http",
      endpoint: { http: { url: "${baseUrl}", path: "/orders" } },
      payload: "{}",
    });

    expect(payload.type).toBe("mqb-config");
    expect(payload.config.publishers).toEqual([
      {
        name: "orders_http",
        endpoint: { http: { url: "${baseUrl}", path: "/orders" } },
        payload: "{}",
      },
    ]);
    expect(payload.config.env_vars).toEqual({ baseUrl: "https://api.local" });
  });

  test("app config import merges entries and keeps existing data", async () => {
    window.appConfig = {
      publishers: [{ name: "existing_pub" }],
      consumers: [{ name: "existing_cons", endpoint: { http: {} } }],
      routes: { existing_route: {} },
      presets: {},
      env_vars: {},
    };
    appShell.setConfig(window.appConfig);
    const bundle = JSON.stringify({
      type: "mqb-export",
      config: {
        publishers: [{ name: "existing_pub" }, { name: "new_pub" }],
        consumers: [{ name: "existing_cons", endpoint: { memory: {} } }, { name: "new_cons", endpoint: { http: {} } }],
        routes: { existing_route: {}, imported_route: {} },
      },
      presets: { existing_pub: [{ name: "p1", method: "GET", url: "http://x", payload: "", headers: [] }] },
      envVars: { baseUrl: "http://x" },
    });

    const result = await importAppConfigFromJsonText(bundle);

    expect(result.importedPublishers).toBe(2);
    expect(result.importedConsumers).toBe(2);
    expect(result.importedRoutes).toBe(2);
    expect(window.appConfig.publishers.length).toBeGreaterThanOrEqual(3);
    expect(window.appConfig.consumers.length).toBeGreaterThanOrEqual(3);
    expect(window.appConfig.routes).toBeUndefined();
    expect(window.appConfig.env_vars.baseUrl).toBe("http://x");
    expect(window.appConfig.history).toMatchObject({ version: 1, publishers: {} });
  });

  test("reset app config clears publishers and consumers", async () => {
    window.appConfig = {
      publishers: [{ name: "p1" }],
      consumers: [{ name: "c1", endpoint: { http: {} } }],
      routes: { r1: {} },
      default_tab: "consumers",
      presets: { keep_me: [{ name: "p1", method: "GET", url: "http://x", payload: "", headers: [] }] },
      env_vars: { baseUrl: "http://x" },
      history: { version: 1, updated_at: 1, publishers: { p1: [{ name: "p1", payload: "", headers: [], metadata: [], request_fields: {}, requestMetadata: {}, status: 200, duration: 1, time: 1 }] } },
    };
    appShell.setConfig(window.appConfig);

    await resetAppConfigToDefaults();

    expect(window.appConfig.publishers).toEqual([]);
    expect(window.appConfig.consumers).toEqual([]);
    expect(window.appConfig.routes).toBeUndefined();
    expect(window.appConfig.default_tab).toBe("publishers");
    expect(window.appConfig.presets).toEqual({});
    expect(window.appConfig.env_vars.baseUrl).toBe("http://x");
    expect(window.appConfig.history).toMatchObject({ version: 1, publishers: {} });
  });
});
