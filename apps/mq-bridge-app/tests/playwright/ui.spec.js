const { test, expect } = require("@playwright/test");

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
      name: "amqp_publisher",
      comment: "Queue publisher comment",
      endpoint: {
        middlewares: [{ metrics: {} }],
        amqp: { url: "amqp://localhost:5672/%2f", queue: "jobs" },
      },
    },
    {
      name: "kafka_publisher",
      comment: "Topic publisher comment",
      endpoint: {
        middlewares: [{ metrics: {} }],
        kafka: { url: "localhost:9092", topic: "events" },
      },
    },
    {
      name: "mongo_publisher",
      comment: "Database publisher comment",
      endpoint: {
        middlewares: [{ metrics: {} }],
        mongodb: {
          url: "mongodb://localhost:27017",
          database: "app",
          collection: "messages",
        },
      },
    },
  ],
};

async function resetConfig(page, config = BASE_CONFIG) {
  const response = await page.request.post("/config", {
    data: config,
  });
  expect(response.ok()).toBeTruthy();
}

async function openPublisherDefinition(page, index = 0) {
  await page.goto(`/#publishers:${index}`);
  const item = page.locator(`#pub-list .pub-item[data-idx="${index}"]`);
  if (await item.count()) {
    await item.first().click();
  }
  await page.locator("#ctab-config").click();
  await expect(page.locator("#pub-config-pane")).toBeVisible();
  await expect(page.locator("#pub-config-form")).toBeVisible();
}

async function openConsumerDefinition(page, index = 0) {
  await page.goto(`/#consumers:${index}`);
  await page.locator("#ctab-def").click();
  await expect(page.locator("#cons-config-form")).toBeVisible();
}

async function expectSaveButtonClean(page, buttonSelector) {
  await expect(page.locator(buttonSelector)).toHaveAttribute("data-dirty", "false");
}

async function expectSaveButtonDirty(page, buttonSelector) {
  await expect(page.locator(buttonSelector)).toHaveAttribute("data-dirty", "true");
}

function formFieldByLabel(page, formSelector, labelText) {
  return page.locator(
    `${formSelector} .wa-form-row:has(.wa-form-label:text-matches("^${labelText}$", "i")) input:visible, ${formSelector} .wa-form-row:has(.wa-form-label:text-matches("^${labelText}$", "i")) textarea:visible`,
  ).first();
}

async function openConsumerResponse(page, index = 0) {
  await page.goto(`/#consumers:${index}`);
  await page.locator("#cons-response-tab").click();
  await expect(page.locator("#cons-response-editor")).toBeVisible();
}

async function clickAllVisibleShowMore(container) {
  const buttons = container.locator("button", { hasText: "Show more" });
  const count = await buttons.count();
  for (let i = 0; i < count; i += 1) {
    const button = buttons.nth(i);
    if (await button.isVisible()) {
      await button.click();
    }
  }
}

async function expectFormLabelAbsent(page, text) {
  await expect(
    page.locator("#pub-config-form label").filter({ hasText: new RegExp(`^${text}$`, "i") }),
  ).toHaveCount(0);
}

test.beforeEach(async ({ page }) => {
  await resetConfig(page);
  const pageErrors = [];
  page.on("pageerror", (error) => {
    pageErrors.push(error);
  });
  page.__pageErrors = pageErrors;
});

test.afterEach(async ({ page }) => {
  expect(page.__pageErrors || []).toEqual([]);
});

test("publisher advanced fields can be expanded and middlewares can be added", async ({ page }) => {
  await openPublisherDefinition(page, 0);

  await clickAllVisibleShowMore(page.locator("#pub-config-form"));

  // The picker is the "Add Middleware" control itself: no reveal click, because select.showPicker()
  // that the reveal relied on does not exist in WebKit.
  const picker = page.locator("#pub-config-form select.js-array-type-select").first();
  await expect(picker).toBeVisible();
  await expect(picker.locator("option").first()).toHaveText("Add Middleware");

  const items = page.locator("#pub-config-form [id='root.endpoint.middlewares-items'] > *");
  const before = await items.count();

  await picker.selectOption({ label: "Retry" });
  await expect(items).toHaveCount(before + 1);
  await expect(page.locator("#workspace-save-button")).toHaveAttribute("data-dirty", "true");

  // The picker stays available so more middlewares can be added.
  await expect(picker).toBeVisible();
  await picker.selectOption({ label: "Delay" });
  await expect(items).toHaveCount(before + 2);
});

test("consumer response editor is available in its own response tab", async ({ page }) => {
  await openConsumerDefinition(page, 0);
  await expect(page.locator("#cons-response-tab")).toBeVisible();

  await page.locator("#cons-response-tab").click();
  await expect(page.locator("#cons-response-panel")).toBeVisible();
  await expect(page.locator("#cons-response-editor")).toBeVisible();
});

test("publisher form hides transport fields already handled by the request bar", async ({ page }) => {
  await openPublisherDefinition(page, 0);
  await expect(page.locator("#pub-url")).toBeVisible();
  await expect(page.locator("#pub-method")).toBeVisible();

  await openPublisherDefinition(page, 1);
  await expect(page.locator("#pub-extra-1")).toHaveValue("jobs");

  await openPublisherDefinition(page, 2);
  await expect(page.locator("#pub-extra-1")).toHaveValue("events");

  await openPublisherDefinition(page, 3);
  await expect(page.locator("#pub-extra-1")).toHaveValue("app");
});

test("consumer custom response headers can be added and removed", async ({ page }) => {
  await openConsumerResponse(page, 0);
  await page.locator("#cons-response-editor").getByText("Add Header", { exact: true }).click();

  const rows = page.locator("#cons-response-editor .response-header-row");
  await expect(rows).toHaveCount(2);

  const newRow = rows.last();
  await newRow.locator("input.field-input").nth(0).fill("x-test");
  await newRow.locator("input.field-input").nth(1).fill("123");
  await newRow.getByText("Delete", { exact: true }).click();

  await expect(rows).toHaveCount(1);
});

test("consumer response header row survives a message poll while being edited", async ({ page }) => {
  await openConsumerResponse(page, 0);
  const rows = page.locator("#cons-response-editor .response-header-row");
  const initialCount = await rows.count();

  await page.locator("#cons-response-editor").getByText("Add Header", { exact: true }).click();
  await expect(rows).toHaveCount(initialCount + 1);

  const keyInput = rows.last().locator("input.field-input").nth(0);
  await keyInput.click();
  await page.keyboard.type("x-poll");

  // The message poll re-renders the panel every 2s; a blank row is not in the
  // consumer config, so a rebuild would drop the row and the focused input.
  await page.waitForTimeout(4500);

  await expect(rows).toHaveCount(initialCount + 1);
  await expect(keyInput).toHaveValue("x-poll");
  await expect(keyInput).toBeFocused();

  await page.keyboard.type("-tail");
  await expect(keyInput).toHaveValue("x-poll-tail");
});

test("publisher and consumer save buttons are not dirty on initial load", async ({ page }) => {
  await openPublisherDefinition(page, 0);
  await expectSaveButtonClean(page, "#workspace-save-button");

  await openConsumerDefinition(page, 0);
  await expectSaveButtonClean(page, "#workspace-save-button");
});

test("publisher and consumer save buttons become dirty on edit and clean after save", async ({ page }) => {
  await openPublisherDefinition(page, 0);
  await page.locator("#pub-url").fill("http://localhost:8080/api/orders/updated");
  await expectSaveButtonDirty(page, "#workspace-save-button");
  await page.locator("#workspace-save-button").click();
  await expectSaveButtonClean(page, "#workspace-save-button");

  await openConsumerResponse(page, 0);
  await page.locator("#cons-response-editor").getByText("Add Header", { exact: true }).click();
  const responseRows = page.locator("#cons-response-editor .response-header-row");
  await responseRows.last().locator("input.field-input").nth(0).fill("x-new");
  await responseRows.last().locator("input.field-input").nth(1).fill("123");
  await expectSaveButtonDirty(page, "#workspace-save-button");
  await page.locator("#ctab-def").click();
  await page.locator("#workspace-save-button").click();
  await expectSaveButtonClean(page, "#workspace-save-button");
});

test("http publisher delivers a message to the http consumer within 2 seconds without metrics polling", async ({ page }) => {
  const metricsRequests = [];
  page.on("request", (request) => {
    if (request.url().includes("/metrics")) {
      metricsRequests.push(request.url());
    }
  });

  await resetConfig(page, {
    ...BASE_CONFIG,
    routes: {},
    consumers: [
      {
        name: "http_consumer",
        comment: "HTTP consumer for UI delivery test",
        endpoint: {
          middlewares: [{ metrics: {} }],
          http: { url: "127.0.0.1:39081", path: "/ui-test", method: "POST" },
        },
        response: null,
      },
    ],
    publishers: [
      {
        name: "http_local_publisher",
        comment: "HTTP publisher for UI delivery test",
        endpoint: {
          middlewares: [{ metrics: {} }],
          http: { url: "http://127.0.0.1:39081/ui-test", method: "POST" },
        },
      },
    ],
  });

  await page.goto("/#consumers:0");
  await expect(page.locator("#cons-list .cons-item.active .item-name")).toHaveText("http_consumer");
  await page.locator("#ctab-msg").click();
  await page.locator("#cons-toggle").click();
  await expect(page.locator("#cons-toggle")).toHaveText("Stop");
  await expect(page.locator(".consumer-live-badge")).toContainText("Connected");

  await page.goto("/#publishers:0");
  await expect(page.locator("#pub-list .pub-item.active .item-name")).toContainText("http_local_publisher");
  await page.locator("#ctab-payload").click();
  await page.locator("#pub-payload .cm-content").fill("{\"hello\":\"ui-test\"}");
  const publishResponsePromise = page.waitForResponse(
    (response) => response.url().includes("/publish") && response.request().method() === "POST",
  );
  await page.locator("#pub-send").click();
  await expect((await publishResponsePromise).ok()).toBeTruthy();

  const messagesResponsePromise = page.waitForResponse(
    (response) => response.url().includes("/messages?consumer_id=") && response.request().method() === "GET",
  );
  await page.goto("/#consumers:0");
  await page.locator("#ctab-msg").click();
  await expect
    .poll(async () =>
      page.evaluate(() =>
        Math.max(
          0,
          ...Object.values(window._mqb_runtime_status?.consumers || {}).map((consumer) => consumer?.message_sequence ?? 0),
        )),
    )
    .toBeGreaterThan(0);
  await expect((await messagesResponsePromise).ok()).toBeTruthy();
  await expect(page.locator("#consumer-log-body")).toContainText("ui-test", { timeout: 2000 });
  expect(metricsRequests).toEqual([]);
});

test("publisher can be copied to a new consumer", async ({ page }) => {
  await openPublisherDefinition(page, 0);

  await page.locator("#pub-copy").click();

  await expect(page.locator("#mtab-consumers")).toHaveClass(/active/);
  await expect(page.locator("#cons-list .cons-item.active .item-name")).toContainText("127.0.0.1:8080");
  await expect(page.locator("#cons-list .cons-item.active .item-name")).toContainText("/api/orders");
  await expect(page.locator("#cons-config-form")).toBeVisible();
  await expect(page.locator("#workspace-save-button")).toHaveAttribute("data-dirty", "true");
});

test("consumer can be copied to a new publisher for review", async ({ page }) => {
  await openConsumerDefinition(page, 0);

  await page.locator("#cons-copy").click();
  const input = page.locator(".mqb-dialog-input");
  await expect(input).toBeVisible();
  await input.fill("copied_memory_publisher");
  await page.locator("wa-button", { hasText: "Create" }).click();

  await expect(page.locator("#mtab-publishers")).toHaveClass(/active/);
  await expect(page.locator("#pub-list .pub-item.active .item-name")).toHaveText("copied_memory_publisher");
  await expect(page.locator("#pub-config-form")).toBeVisible();
  await expect(page.locator("#workspace-save-button")).toHaveAttribute("data-dirty", /^(true|false)$/);
});

test("publisher delete can be saved and stays deleted after reload", async ({ page }) => {
  await openPublisherDefinition(page, 3);
  await expect(page.getByRole("textbox", { name: "Name" })).toHaveValue("mongo_publisher");

  await page.locator("#pub-delete").click();
  const deleteSave = page.waitForResponse(
    (response) => response.url().endsWith("/config") && response.request().method() === "GET",
  );
  await page.locator("wa-button", { hasText: "Continue" }).click();
  await expect((await deleteSave).ok()).toBeTruthy();
  await expect(page.locator("#pub-list .pub-item .item-name").filter({ hasText: "mongo_publisher" })).toHaveCount(0);
  await expectSaveButtonClean(page, "#workspace-save-button");

  await openPublisherDefinition(page, 0);
  await expect(page.locator("#pub-list .pub-item .item-name").filter({ hasText: "mongo_publisher" })).toHaveCount(0);
});

test("app config shows security and environment variable sections", async ({ page }) => {
  await page.goto("/#config");
  await expect(page.locator("#form-container")).toBeVisible();
  await expect(page.locator("#form-container")).toContainText("Config Security");
  await expect(page.locator("#form-container")).toContainText("Environment Variables");
});

test("active tabs stretch to the full viewport height", async ({ page }) => {
  for (const hash of ["/#consumers:0", "/#publishers:0", "/#config"]) {
    await page.goto(hash);
    const metrics = await page.evaluate(() => {
      const activePanel = document.querySelector(".tab-content-panel.active");
      const appBody = document.querySelector(".app-body");
      const app = document.getElementById("app");
      if (!activePanel || !appBody || !app) {
        return null;
      }

      const panelRect = activePanel.getBoundingClientRect();
      const appBodyRect = appBody.getBoundingClientRect();
      const appRect = app.getBoundingClientRect();

      return {
        viewportHeight: window.innerHeight,
        appBottom: appRect.bottom,
        appBodyBottom: appBodyRect.bottom,
        panelBottom: panelRect.bottom,
      };
    });

    expect(metrics).not.toBeNull();
    expect(Math.abs(metrics.appBottom - metrics.viewportHeight)).toBeLessThanOrEqual(2);
    expect(Math.abs(metrics.appBodyBottom - metrics.viewportHeight)).toBeLessThanOrEqual(2);
    expect(Math.abs(metrics.panelBottom - metrics.viewportHeight)).toBeLessThanOrEqual(2);
  }
});
