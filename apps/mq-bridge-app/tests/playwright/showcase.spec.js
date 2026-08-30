const { test, expect } = require("@playwright/test");

/**
 * This test is designed to be recorded as a video showcase.
 * It demonstrates the bridge workflow: Publisher -> Consumer -> History -> Presets.
 */

const BASE_CONFIG = {
  log_level: "info",
  ui_addr: "127.0.0.1:39091",
  metrics_addr: "",
  default_tab: "publishers",
  publishers: [
    {
      name: "demo_http_publisher",
      endpoint: {
        http: {
          url: "http://127.0.0.1:39081/showcase",
          method: "POST",
        },
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
  consumers: [],
  routes: {},
};

const isShowcase = process.env.SHOWCASE === "true";

test.use({
  video: isShowcase ? "on" : "off",
});

test.use({
  colorScheme: "dark",
});

test("Generate showcase video", async ({ page }) => {
  test.skip(!isShowcase, "Showcase recording is opt-in and skipped during normal CI runs.");

  // Utility to slow down actions for recording clarity
  const pause = (ms = 1500) => isShowcase ? page.waitForTimeout(ms) : Promise.resolve();

  if (isShowcase) {
    test.info().annotations.push({ type: "mode", description: "Showcase recording enabled" });
  }

  // 1. Reset state to show the initial environment
  const response = await page.request.post("/config", { data: BASE_CONFIG });
  expect(response.ok()).toBeTruthy();

  await page.goto("/");
  await page.waitForLoadState("networkidle");
  await expect(page.locator("#pub-list")).toBeVisible({ timeout: 60000 });
  await pause();

  // 2. Create a consumer by copying the publisher configuration
  await page.locator("#pub-list .pub-item").first().click();
  await pause(800);
  await page.locator("#ctab-config").click();
  await pause(800);
  await page.locator("#pub-copy").click();
  await pause(800);
  await page.locator("#cons-save").click();
  await pause();

  // 3. Start the new consumer and show the empty message list
  await expect(page.locator("#cons-list")).toBeVisible();
  await expect(page.locator("#mtab-consumers")).toHaveClass(/active/);
  await page.locator("#ctab-msg").click();
  await pause(800);
  await page.locator("#cons-toggle").click();
  
  // Wait for connection status
  await expect(page.locator("#cons-msg-panel")).toContainText("Connected");
  await expect(page.locator("#consumer-log-body")).toContainText("Waiting for messages");
  await pause();

  // 4. Navigate back to publisher and send varied messages
  await page.locator("#mtab-publishers").click();
  await page.locator("#ctab-payload").click();
  await pause(500);

  // Message 1: Simple JSON
  await page.locator("#pub-payload .cm-content").fill(JSON.stringify({ type: "notification", text: "First message" }, null, 2));
  await pause(2000);
  await page.locator("#pub-send").click();
  await pause(1000);

  // Message 2: Different body and custom header
  await page.locator("#pub-payload .cm-content").fill(JSON.stringify({ type: "alert", priority: "high" }, null, 2));
  await pause(1000);
  await page.locator("#pub-sub-tabs button", { hasText: "Headers" }).click();
  await pause(800);
  await page.locator('#pub-top-content-wrapper wa-button:has-text("+ Add Header")').click();
  
  const headerRow = page.locator(".response-header-row").last();
  await headerRow.locator('input[placeholder="Header name"]').fill("X-Demo-Event");
  await headerRow.locator('input[placeholder="Header value"]').fill("Showcase-01");
  await pause(1000);
  await page.locator("#pub-send").click();
  await pause(1500);

  // 5. Switch to Consumer to see received messages and details
  await page.locator("#mtab-consumers").click();
  await expect(page.locator("#consumer-log-body tr")).toHaveCount(2);
  await pause(800);
  
  // Click the most recent message
  await page.locator("#consumer-log-body tr").first().click();
  await pause(800);
  await expect(page.locator("#cons-msg-payload")).toContainText("alert");
  await pause();

  // 6. Switch to Publisher and create a preset from history
  await page.locator("#mtab-publishers").click();
  await page.locator("#pub-sub-tabs button", { hasText: "History" }).click();
  await pause(1000);
  
  // Save the latest history entry as a preset
  await page.locator(".history-row").first().locator("wa-button", { hasText: "Save Preset" }).click();
  await pause(800);
  await page.locator(".mqb-dialog-input").fill("Showcase Preset");
  await pause(800);
  await page.locator(".mqb-dialog").first().locator("wa-button", { hasText: "Save" }).click();
  await pause(1000);

  // 7. Show the presets list
  await page.locator("#pub-sub-tabs button", { hasText: "Presets" }).click();
  await expect(page.locator("#pub-presets-pane")).toContainText("Showcase Preset");
  await pause(1000); // Hold the final screen for a second
  return;
});
