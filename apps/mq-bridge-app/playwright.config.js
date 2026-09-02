const { defineConfig, devices } = require("@playwright/test");

const isCI = !!process.env.CI;

/**
 * Specs cheap and stable enough to be worth running on every engine. The rest
 * are Chromium-only: they are slow, and the bugs they find are not engine
 * specific. What crosses engines is layout and form-control behaviour — the
 * publisher form's middleware picker already had to be rewritten once because
 * `select.showPicker()` does not exist in WebKit.
 */
const CROSS_BROWSER_SPECS = /(ui|button-outcomes|settings)\.spec\.js/;

/**
 * firefox and webkit are half the wall time of a full run for three specs' worth
 * of extra coverage, which is a bad trade on every edit-run cycle and a good one
 * before a merge. CI always runs them; locally they are opt-in via
 * PW_ALL_BROWSERS. PW_CHROMIUM_ONLY forces chromium even on CI.
 */
const allEngines = !process.env.PW_CHROMIUM_ONLY && (isCI || !!process.env.PW_ALL_BROWSERS);

module.exports = defineConfig({
  testDir: "./tests/playwright",
  timeout: 30_000,
  // No maxFailures: when the UI is broken the point is to see every failure in
  // one run, not to stop at the first one.
  workers: 1, // Required because tests modify shared global /config state
  expect: {
    timeout: 5_000,
  },
  reporter: [["line"]],
  // Projects would otherwise push the project name into the baseline filename
  // and orphan every reviewed screenshot; keep the established naming.
  snapshotPathTemplate: "{testFilePath}-snapshots/{arg}-{platform}{ext}",
  use: {
    baseURL: "http://127.0.0.1:39091",
    headless: process.env.SHOWCASE === "true" ? false : true,
    // Keep the evidence for a failure without paying for it on green runs.
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
    {
      name: "firefox",
      use: { ...devices["Desktop Firefox"] },
      testMatch: CROSS_BROWSER_SPECS,
    },
    {
      name: "webkit",
      use: { ...devices["Desktop Safari"] },
      testMatch: CROSS_BROWSER_SPECS,
    },
  ].filter((project) => project.name === "chromium" || allEngines),
  webServer: {
    command:
      'echo \'ui_addr: "127.0.0.1:39091"\nlog_level: "info"\npublishers: []\nconsumers: []\nroutes: {}\' > /tmp/mqb-playwright-minimal.yml && cargo run -p mq-bridge-app -- --config /tmp/mqb-playwright-minimal.yml',
    url: "http://127.0.0.1:39091/health",
    reuseExistingServer: !isCI,
    // CI pre-builds the binary so this is a no-op link check; the generous
    // budget is for cold local runs, where a full build takes ~10 min.
    timeout: 900_000,
  },
});
