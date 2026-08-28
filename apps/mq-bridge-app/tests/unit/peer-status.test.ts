import { describe, expect, test, vi } from "vitest";
import {
  filterPeerRows,
  peerActivity,
  peerActivityLabel,
  peerConsumerRows,
  peerPublisherRows,
} from "../../ui/src/lib/peer-status";
import { createRuntimeStatusPoller } from "../../ui/src/lib/runtime-status";

const status = {
  current_instance_id: "local",
  instances: [
    {
      schema_version: 1,
      instance_id: "local",
      pid: 1,
      kind: "web-ui" as const,
      application_version: "1",
      started_at_ms: 1,
      last_seen_at_ms: 1,
      workspace_id: "local-workspace",
      workspace_label: "local",
      consumers: [{ id: "local-c", label: "local", endpoint: "memory", summary: { running: true, healthy: true, throughput: 0, message_sequence: 0 } }],
      publishers: [],
      routes: [],
    },
    {
      schema_version: 1,
      instance_id: "peer",
      pid: 2,
      kind: "mcp" as const,
      application_version: "1",
      started_at_ms: 1,
      last_seen_at_ms: 1,
      workspace_id: "peer-workspace",
      workspace_label: "peer",
      consumers: [],
      publishers: [{ id: "peer-p", label: "publisher", endpoint: "kafka", summary: { running: true, healthy: true, throughput: 2, message_sequence: 3 } }],
      routes: [{
        id: "route",
        label: "route",
        input: { id: "in", label: "input", endpoint: "memory", summary: { running: true, healthy: true, throughput: 1, message_sequence: 1 } },
        output: { id: "out", label: "output", endpoint: "null", summary: { running: true, healthy: true, throughput: 1, message_sequence: 1 } },
        summary: { running: true, healthy: true, throughput: 1, message_sequence: 1 },
      }],
    },
  ],
};

describe("peer status rows", () => {
  test("excludes the current instance and decomposes routes", () => {
    expect(peerConsumerRows(status).map((row) => row.label)).toEqual(["input"]);
    expect(peerPublisherRows(status).map((row) => row.label)).toEqual(["publisher", "output"]);
  });

  test("filters on label, endpoint and instance", () => {
    const rows = peerPublisherRows(status);
    expect(filterPeerRows(rows, "  ").length).toBe(rows.length);
    expect(filterPeerRows(rows, "kafka").map((row) => row.label)).toEqual(["publisher"]);
    expect(filterPeerRows(rows, "MCP").length).toBe(rows.length);
    expect(filterPeerRows(rows, "nothing-matches")).toEqual([]);
  });

  // A route is one unit of traffic here, not the two sidebar rows it renders as.
  test("summarises peer traffic without double-counting routes", () => {
    expect(peerActivity(status)).toEqual({ instances: 1, running: 1, throughput: 1 });
    expect(peerActivityLabel(peerActivity(status))).toBe(
      "1 other instance • 1 running • 1.0 msg/s",
    );
    expect(peerActivity({ current_instance_id: "local", instances: [status.instances[0]] })).toEqual(
      { instances: 0, running: 0, throughput: 0 },
    );
  });
});

const peerBody = JSON.stringify({ current_instance_id: "local", instances: status.instances });
const runtimeBody = JSON.stringify({
  active_consumers: [],
  active_routes: [],
  route_throughput: {},
  consumers: {},
});
const ok = (body: string) => new Response(body, { status: 200 });

describe("peer status polling", () => {
  test("fetches runtime and peer status concurrently", async () => {
    const started: string[] = [];
    // No response is released until both requests have been issued, which never
    // happens if the second waits on the first. Order between them is not part
    // of the contract — only that neither blocks the other.
    let releaseAll = () => {};
    const bothIssued = new Promise<void>((resolve) => {
      releaseAll = resolve;
    });
    const fetchImpl = vi.fn(async (url: string) => {
      started.push(url);
      if (started.length === 2) releaseAll();
      await bothIssued;
      return ok(url === "/peer-status" ? peerBody : runtimeBody);
    });

    const onPeerStatus = vi.fn();
    await createRuntimeStatusPoller({ fetchImpl: fetchImpl as never, onPeerStatus }).poll();

    expect(started).toHaveLength(2);
    expect(started).toContain("/runtime-status");
    expect(started).toContain("/peer-status");
    expect(onPeerStatus).toHaveBeenCalledTimes(1);
  });

  test("stops asking after a 403 and reports no peers", async () => {
    const fetchImpl = vi.fn(async (url: string) =>
      url === "/peer-status" ? new Response("", { status: 403 }) : ok(runtimeBody),
    );
    const onPeerStatus = vi.fn();
    const poller = createRuntimeStatusPoller({ fetchImpl: fetchImpl as never, onPeerStatus });

    await poller.poll();
    await poller.poll();

    const peerCalls = fetchImpl.mock.calls.filter(([url]) => url === "/peer-status");
    expect(peerCalls).toHaveLength(1);
    expect(onPeerStatus).toHaveBeenCalledWith({ current_instance_id: "", instances: [] });
  });

  // Leases expire server-side on their own TTL, so one failed poll must not
  // blank every peer row and restore it a second later.
  test("keeps the last peer list when a single poll fails", async () => {
    let failNext = false;
    const fetchImpl = vi.fn(async (url: string) => {
      if (url === "/peer-status") {
        if (failNext) throw new Error("network");
        return ok(peerBody);
      }
      return ok(runtimeBody);
    });
    const onPeerStatus = vi.fn();
    const poller = createRuntimeStatusPoller({ fetchImpl: fetchImpl as never, onPeerStatus });

    await poller.poll();
    failNext = true;
    await poller.poll();

    expect(onPeerStatus).toHaveBeenCalledTimes(1);
    expect(onPeerStatus.mock.calls[0][0].instances).toHaveLength(2);
  });
});

