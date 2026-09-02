const { test, expect } = require("./fixtures");

// Peer rows come from other mq-bridge processes on the same machine. Driving
// them through a real second process would make these tests depend on process
// startup races and on the tester's own runtime directory, so the registry
// endpoint is stubbed instead — what is under test is the UI contract:
// grouping, read-only rendering, escaping, and disappearance.

const BASE_CONFIG = {
  log_level: "info",
  ui_addr: "127.0.0.1:39091",
  metrics_addr: "",
  default_tab: "publishers",
  routes: {},
  consumers: [
    {
      name: "local_consumer",
      endpoint: { memory: { topic: "local-events" } },
    },
  ],
  publishers: [
    {
      name: "local_publisher",
      endpoint: { memory: { topic: "local-out" } },
    },
  ],
};

const summary = (overrides = {}) => ({
  running: true,
  healthy: true,
  pending: null,
  capacity: null,
  error: null,
  throughput: 0,
  message_sequence: 0,
  ...overrides,
});

const instance = (id, kind, workspace, entities = {}) => ({
  schema_version: 1,
  instance_id: id,
  pid: 4242,
  kind,
  application_version: "0.0.0-test",
  started_at_ms: 1,
  last_seen_at_ms: 1,
  workspace_id: `${id}-workspace`,
  workspace_label: workspace,
  consumers: entities.consumers || [],
  publishers: entities.publishers || [],
  routes: entities.routes || [],
});

const MCP_PEER = instance("peer-mcp", "mcp", "scratch", {
  consumers: [
    {
      id: "mcp-in",
      label: "mcp_inbox",
      endpoint: "kafka",
      summary: summary({ throughput: 12.5 }),
    },
  ],
  publishers: [
    { id: "mcp-out", label: "mcp_sink", endpoint: "nats", summary: summary() },
  ],
  routes: [
    {
      id: "mcp_route",
      label: "mcp_route",
      input: {
        id: "mcp_route:input",
        label: "mcp_route",
        endpoint: "http",
        summary: summary({ throughput: 3 }),
      },
      output: {
        id: "mcp_route:output",
        label: "mcp_route",
        endpoint: "file",
        summary: summary({ throughput: 3 }),
      },
      summary: summary({ throughput: 3 }),
    },
  ],
});

const CLI_PEER = instance("peer-cli", "cli", "other-workspace", {
  consumers: [
    {
      id: "cli-in",
      label: "cli_stopped",
      endpoint: "sqlx",
      // A configured-but-not-running consumer must not read as healthy.
      summary: summary({ running: false, healthy: false }),
    },
  ],
});

// The registry always reports the reading process too, advertising the same
// entities this UI already renders as its own rows. It must never be duplicated
// into the peer section, so it carries entities here — with none, the exclusion
// would be untestable.
const SELF = instance("self-web-ui", "web-ui", "playwright", {
  consumers: [
    { id: "local_consumer", label: "local_consumer", endpoint: "memory", summary: summary() },
  ],
  publishers: [
    { id: "local_publisher", label: "local_publisher", endpoint: "memory", summary: summary() },
  ],
});

/** Serves a fixed peer payload, and lets a test swap it mid-session. */
async function stubPeerStatus(page, initialInstances) {
  const state = { instances: initialInstances, status: 200 };
  await page.route("**/peer-status", async (route) => {
    if (state.status !== 200) {
      await route.fulfill({ status: state.status, body: "" });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        current_instance_id: SELF.instance_id,
        instances: state.instances,
      }),
    });
  });
  return state;
}

async function resetConfig(page) {
  const response = await page.request.post("/config", { data: BASE_CONFIG });
  expect(response.ok()).toBeTruthy();
}

test.beforeEach(async ({ page }) => {
  await resetConfig(page);
});

test("peer consumers and route inputs are grouped under their instance", async ({ page }) => {
  await stubPeerStatus(page, [SELF, MCP_PEER, CLI_PEER]);
  await page.goto("/#consumers");

  const list = page.locator("#cons-list");
  await expect(list.locator(".peer-group-label")).toHaveText("Other instances");

  // One header per advertising instance, and the reading process is not one.
  await expect(list.locator(".sidebar-peer-instance")).toHaveText([
    "MCP · scratch",
    "CLI · other-workspace",
  ]);

  // Each peer contributes its consumers plus every route's input.
  const peerRows = list.locator(".sidebar-item--peer");
  await expect(peerRows).toHaveCount(3);
  await expect(peerRows.locator(".item-name")).toHaveText([
    "mcp_inbox",
    "mcp_route",
    "cli_stopped",
  ]);
  await expect(peerRows.nth(0).locator(".proto-badge")).toHaveText("kafka");
  await expect(peerRows.nth(1).locator(".proto-badge")).toHaveText("http");
});

test("peer publishers and route outputs appear in the publishers panel", async ({ page }) => {
  await stubPeerStatus(page, [SELF, MCP_PEER, CLI_PEER]);
  await page.goto("/#publishers");

  const peerRows = page.locator("#pub-list .sidebar-item--peer");
  await expect(peerRows.locator(".item-name")).toHaveText(["mcp_sink", "mcp_route"]);
  // The CLI peer advertises no publishers, so it contributes no header here.
  await expect(page.locator("#pub-list .sidebar-peer-instance")).toHaveText(["MCP · scratch"]);
});

test("peer activity indicators are distinct from local ones and follow real state", async ({
  page,
}) => {
  await stubPeerStatus(page, [SELF, MCP_PEER, CLI_PEER]);
  await page.goto("/#consumers");

  const list = page.locator("#cons-list");
  // Local rows carry no peer marker; peer rows always do.
  await expect(list.locator(".sidebar-item:not(.sidebar-item--peer) .item-status.peer-status")).toHaveCount(0);
  await expect(list.locator(".sidebar-item--peer .item-status.peer-status")).toHaveCount(3);

  // A running peer consumer reads as active, a stopped one does not.
  await expect(list.locator(".sidebar-item--peer").nth(0).locator(".item-status")).toHaveClass(
    /status-ok/,
  );
  await expect(list.locator(".sidebar-item--peer").nth(2).locator(".item-status")).toHaveClass(
    /status-off/,
  );

  // Throughput is shown only where there is some.
  await expect(list.locator(".sidebar-item--peer").nth(0).locator(".msg-count")).toHaveText(
    "12.5 msg/s",
  );
  await expect(list.locator(".sidebar-item--peer").nth(2).locator(".msg-count")).toHaveCount(0);
});

test("peer rows are inspection-only and cannot be selected or edited", async ({ page }) => {
  await stubPeerStatus(page, [SELF, MCP_PEER]);
  await page.goto("/#consumers");

  const peerRow = page.locator("#cons-list .sidebar-item--peer").first();
  await expect(peerRow).toBeVisible();
  // Local rows are buttons; peer rows must not be actionable.
  expect(await peerRow.evaluate((node) => node.tagName)).toBe("DIV");
  await expect(peerRow).not.toHaveAttribute("tabindex");
  await expect(peerRow).toHaveCSS("cursor", "default");

  const hashBefore = await page.evaluate(() => window.location.hash);
  await peerRow.click();
  await expect(peerRow).not.toHaveClass(/\bactive\b/);
  expect(await page.evaluate(() => window.location.hash)).toBe(hashBefore);
});

test("advertised names and metadata are rendered as text, never as markup", async ({ page }) => {
  const hostile = instance("peer-xss", "cli", '<img src=x onerror="window.__xss=1">', {
    consumers: [
      {
        id: "x",
        label: '<script>window.__xss=1</script>',
        endpoint: "memory",
        summary: summary(),
      },
    ],
  });
  await stubPeerStatus(page, [SELF, hostile]);
  await page.goto("/#consumers");

  const peerRow = page.locator("#cons-list .sidebar-item--peer").first();
  await expect(peerRow.locator(".item-name")).toHaveText('<script>window.__xss=1</script>');
  await expect(page.locator("#cons-list img")).toHaveCount(0);
  await expect(page.locator("#cons-list script")).toHaveCount(0);
  expect(await page.evaluate(() => window.__xss)).toBeUndefined();
});

test("a peer whose lease expires disappears from the sidebar", async ({ page }) => {
  const state = await stubPeerStatus(page, [SELF, MCP_PEER, CLI_PEER]);
  await page.goto("/#consumers");
  await expect(page.locator("#cons-list .sidebar-item--peer")).toHaveCount(3);

  // The server drops stale leases, so the next poll simply stops listing it.
  state.instances = [SELF, MCP_PEER];
  await expect(page.locator("#cons-list .sidebar-item--peer")).toHaveCount(2);
  await expect(page.locator("#cons-list .sidebar-peer-instance")).toHaveText(["MCP · scratch"]);

  state.instances = [SELF];
  await expect(page.locator("#cons-list .sidebar-item--peer")).toHaveCount(0);
  await expect(page.locator("#cons-list .peer-group-label")).toHaveCount(0);
});

test.describe("non-local client", () => {
  // This case drives the peer endpoint into the 403 it is meant to return, so
  // the resulting console error is the behaviour under test, not a defect.
  test.use({ allowedPageProblems: /403 \(Forbidden\)/ });

  test("a non-local client is shown no peer section at all", async ({ page }) => {
    const state = await stubPeerStatus(page, [SELF, MCP_PEER]);
    state.status = 403;
    await page.goto("/#consumers");

    await expect(page.locator("#cons-list .sidebar-item")).not.toHaveCount(0);
    await expect(page.locator("#cons-list .sidebar-item--peer")).toHaveCount(0);
    await expect(page.locator("#cons-list .peer-group-label")).toHaveCount(0);
  });
});

test("the sidebar filter applies to peer rows", async ({ page }) => {
  await stubPeerStatus(page, [SELF, MCP_PEER, CLI_PEER]);
  await page.goto("/#consumers");
  await expect(page.locator("#cons-list .sidebar-item--peer")).toHaveCount(3);

  await page.locator("#cons-filter").fill("cli_stopped");
  await expect(page.locator("#cons-list .sidebar-item--peer .item-name")).toHaveText([
    "cli_stopped",
  ]);

  await page.locator("#cons-filter").fill("kafka");
  await expect(page.locator("#cons-list .sidebar-item--peer .item-name")).toHaveText([
    "mcp_inbox",
  ]);
});
