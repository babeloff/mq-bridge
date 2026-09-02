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
  //
  // Each worker runs its own app process on its own port (tests/playwright/
  // app-server.js), so the shared global /config that used to force workers: 1
  // is no longer shared. The cap is deliberate: a worker costs a browser *and*
  // an app process, and the box still has to run them.
  workers: isCI ? 2 : 4,
  // Spread a file's tests across workers too, not just whole files. Without it
  // the four sweep tests queue up behind each other in one worker and splitting
  // them buys nothing.
  fullyParallel: true,
  // Builds the binary once so the workers only exec it.
  globalSetup: require.resolve("./tests/playwright/global-setup.js"),
  expect: {
    timeout: 5_000,
  },
  reporter: [["line"]],
  // Projects would otherwise push the project name into the baseline filename
  // and orphan every reviewed screenshot; keep the established naming.
  snapshotPathTemplate: "{testFilePath}-snapshots/{arg}-{platform}{ext}",
  use: {
    // baseURL is not set here: the appServer fixture assigns each worker the
    // port its own app process actually bound.
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
});
