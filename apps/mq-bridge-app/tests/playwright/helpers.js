const { expect } = require("@playwright/test");

/**
 * Shared fixture for specs that need a populated workspace. Mirrors the inline
 * fixture in ui.spec.js; that file predates this module and still owns its copy.
 */
const BASE_CONFIG = {
  log_level: "info",
  ui_addr: "127.0.0.1:39091",
  metrics_addr: "",
  default_tab: "publishers",
  routes: {
    ingest_http: {
      enabled: false,
      input: {
        middlewares: [{ metrics: {} }],
        http: { url: "127.0.0.1:39081" },
      },
      output: { memory: { topic: "route-output" } },
    },
  },
  consumers: [
    {
      name: "memory_consumer",
      comment: "Demo consumer comment",
      endpoint: {
        middlewares: [{ metrics: {} }],
        memory: { topic: "consumer-events" },
      },
      response: {
        headers: { "x-initial": "test" },
        payload: "ok",
      },
    },
  ],
  publishers: [
    {
      name: "http_publisher",
      comment: "Demo publisher comment",
      endpoint: {
        middlewares: [{ metrics: {} }],
        http: { url: "http://localhost:8080/api/orders" },
      },
    },
    {
      name: "memory_publisher",
      comment: "Queue publisher comment",
      endpoint: {
        middlewares: [{ metrics: {} }],
        memory: { topic: "publisher-events" },
      },
    },
  ],
};

async function resetConfig(page, config = BASE_CONFIG) {
  const response = await page.request.post("/config", { data: config });
  expect(response.ok()).toBeTruthy();
}

async function readConfig(page) {
  const response = await page.request.get("/config");
  expect(response.ok()).toBeTruthy();
  return response.json();
}

/**
 * Navigate to a hash view and wait for the shell to finish booting. Cannot use
 * networkidle: the runtime poller keeps a request in flight every second.
 */
async function gotoView(page, hash) {
  await page.goto(`/${hash}`);
  await expect(page.locator("#mainTabs")).toBeVisible();
  await expect(page.locator(".tab-content-panel.active")).toBeVisible();
}

module.exports = { BASE_CONFIG, resetConfig, readConfig, gotoView };
