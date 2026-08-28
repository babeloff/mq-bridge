const { test, expect } = require("@playwright/test");

const VISUAL_CONFIG = {
  log_level: "info",
  ui_addr: "127.0.0.1:39091",
  metrics_addr: "",
  default_tab: "publishers",
  routes: {},
  consumers: [
    {
      name: "visual_http_consumer",
      comment: "Screenshot consumer",
      endpoint: {
        middlewares: [{ metrics: {} }],
        http: { url: "127.0.0.1:39081", path: "/visual-regression", method: "POST" },
      },
      response: {
        headers: { "content-type": "application/json" },
        payload: "{\"ok\":true}",
      },
      message_capture: {
        enabled: true,
        keep_last: 10,
      },
    },
  ],
  publishers: [
    {
      name: "orders list",
      comment: "HTTP list endpoint",
      endpoint: {
        middlewares: [{ metrics: {} }],
        http: { url: "http://127.0.0.1:39081", path: "/api/orders", method: "GET" },
      },
      payload: "{\"limit\":25}",
    },
    {
      name: "orders create",
      comment: "HTTP create endpoint",
      endpoint: {
        middlewares: [{ metrics: {} }],
        http: { url: "http://127.0.0.1:39081", path: "/api/orders", method: "POST" },
      },
      payload: "{\"sku\":\"VISUAL-1\",\"quantity\":2}",
    },
    {
      name: "orders detail",
      comment: "HTTP detail endpoint",
      endpoint: {
        middlewares: [{ metrics: {} }],
        http: { url: "http://127.0.0.1:39081", path: "/api/orders/{id}", method: "GET" },
      },
      payload: "{}",
    },
    {
      name: "visual delivery",
      comment: "HTTP publisher used by the consumer screenshot",
      endpoint: {
        middlewares: [{ metrics: {} }],
        http: { url: "http://127.0.0.1:39081", path: "/visual-regression", method: "POST" },
      },
      payload: "{\"event\":\"visual-regression\",\"status\":\"ready\"}",
    },
    {
      name: "events kafka",
      comment: "Non-HTTP publisher in the menu",
      endpoint: {
        middlewares: [{ metrics: {} }],
        kafka: { url: "localhost:9092", topic: "events.visual" },
      },
      payload: "{\"event\":\"queued\"}",
    },
  ],
  config_security: {
    mode: "unencrypted",
  },
  env_vars: {
    API_HOST: "127.0.0.1",
  },
};

const screenshotOptions = {
  fullPage: true,
  animations: "disabled",
  maxDiffPixelRatio: 0.001,
};

test.skip(!!process.env.CI, "Screenshot baselines are local-only; CI runs the behavioral UI suite.");

async function stabilizeForScreenshot(page) {
  await page.addStyleTag({
    content: `
      *, *::before, *::after {
        animation-duration: 0s !important;
        animation-delay: 0s !important;
        transition-duration: 0s !important;
        transition-delay: 0s !important;
        caret-color: transparent !important;
      }
      .cm-cursor, .cm-cursorLayer, .cm-selectionLayer {
        visibility: hidden !important;
      }
    `,
  });
}

test.use({
  colorScheme: "light",
  viewport: { width: 1440, height: 1000 },
});

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/config", { data: VISUAL_CONFIG });
  expect(response.ok()).toBeTruthy();
});

test("publisher HTTP definition layout", async ({ page }) => {
  await page.goto("/#publishers:0");
  await expect(page.locator("#pub-list")).toBeVisible();
  await expect(page.locator("#pub-list .sidebar-item--group").first()).toContainText("HTTP 127.0.0.1:39081");
  await page.locator("#ctab-config").click();
  await expect(page.locator("#pub-config-form")).toBeVisible();
  await expect(page.locator("#pub-url")).toBeVisible();
  await expect(page.locator("#pub-method")).toBeVisible();
  await stabilizeForScreenshot(page);

  await expect(page).toHaveScreenshot("publisher-view-http-endpoint.png", screenshotOptions);
});

test("consumer messages layout with selected message", async ({ page }) => {
  await page.goto("/#consumers:0");
  await page.locator("#ctab-msg").click();
  await page.locator("#cons-toggle").click();
  await expect(page.locator(".consumer-live-badge")).toContainText("Connected");

  await page.goto("/#publishers:3");
  await page.locator("#ctab-payload").click();
  await page.locator("#pub-payload .cm-content").fill("{\"event\":\"visual-regression\",\"status\":\"ready\"}");
  const publishResponse = page.waitForResponse(
    (response) => response.url().includes("/publish") && response.request().method() === "POST",
  );
  await page.locator("#pub-send").click();
  await expect((await publishResponse).ok()).toBeTruthy();
  const publisherPretty = page.locator('#pub-actual-payload button[title="Pretty-print JSON content"]');
  await expect(publisherPretty).toBeVisible();
  await publisherPretty.click();
  await expect(page.locator("#pub-actual-payload .cm-content")).toContainText('"ok": true');

  await page.goto("/#consumers:0");
  await page.locator("#ctab-msg").click();
  await expect(page.locator("#consumer-log-body")).toContainText("visual-regression", { timeout: 5000 });
  await page.locator("#consumer-log-body tr").first().click();
  await expect(page.locator("#consumer-log-body tr.selected")).toHaveCount(1);
  await expect(page.locator("#cons-msg-payload")).toContainText("visual-regression");
  await stabilizeForScreenshot(page);

  await expect(page).toHaveScreenshot("consumer-view-selected-message.png", {
    ...screenshotOptions,
    mask: [
      page.locator("#consumer-log-body td:first-child"),
      page.locator("#cons-msg-detail-info"),
    ],
  });

  const requestPretty = page.locator('#cons-msg-payload button[title="Pretty-print JSON content"]');
  await expect(requestPretty).toBeVisible();
  await requestPretty.click();
  await expect(page.locator("#cons-msg-payload .cm-content")).toContainText('"event": "visual-regression"');

  const responsePretty = page.locator('#cons-msg-response button[title="Pretty-print JSON content"]');
  await expect(responsePretty).toBeVisible();
  await responsePretty.click();
  await expect(page.locator("#cons-msg-response .cm-content")).toContainText('"ok": true');
});

test("settings main layout", async ({ page }) => {
  await page.goto("/#config");
  await expect(page.locator("#form-container")).toBeVisible();
  await expect(page.locator("#form-container")).toContainText("Config Security");
  await stabilizeForScreenshot(page);

  await expect(page).toHaveScreenshot("settings-view-main.png", screenshotOptions);
});
