// @vitest-environment jsdom

/**
 * Model-based sweep of the publishers panel's state machine.
 *
 * The hand-written tests each drive one path. The bugs that survive them are the
 * combinations nobody thought to write down — delete the selected publisher
 * while its payload is edited, switch subtab mid-edit, add on top of an unsaved
 * add — so this drives random sequences of the same actions and checks the
 * invariants that must hold after every single one.
 *
 * The sequence is derived from a fixed seed, so a failure names the exact steps
 * to replay rather than being unreproducible.
 */

import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { get } from "svelte/store";

const dialogMocks = vi.hoisted(() => ({ open: vi.fn(), save: vi.fn() }));
vi.mock("@tauri-apps/plugin-dialog", () => dialogMocks);

import {
  addPublisherAction,
  addPublisherMetadataRow,
  cloneCurrentPublisherAction,
  deleteCurrentPublisherAction,
  initPublishers,
  removePublisherMetadataRow,
  restorePublisherStateFromView,
  saveCurrentPublisherAction,
  selectPublisherSubtab,
  updatePublisherMetadataRow,
  updatePublisherPayload,
  updatePublisherRequestField,
} from "../../ui/src/lib/publishers-view";
import { publishersPanelState, type PublishersPanelState } from "../../ui/src/lib/stores";
import { installBaseWindowStubs, mountPublishersDom, restoreBaseWindowStubs } from "./test-helpers";

/** Deterministic 32-bit PRNG: a failing seed replays exactly. */
function random(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

const INITIAL_CONFIG = {
  publishers: [
    { name: "orders_http", endpoint: { http: { url: "https://example.test/orders", custom_headers: {} } } },
    { name: "events_memory", endpoint: { memory: { topic: "events" } } },
    { name: "jobs_memory", endpoint: { memory: { topic: "jobs" } } },
  ],
  routes: {},
  consumers: [],
};

const SCHEMA = {
  properties: { publishers: { items: {} } },
  $defs: { HttpConfig: { properties: { custom_headers: {} } } },
};

const SUBTABS = ["payload", "headers", "history", "definition"];

/**
 * Every action the panel exposes that can change selection, contents or
 * dirtiness. Each returns a label so a failure reads as a reproducible script.
 */
const ACTIONS = [
  {
    name: "select",
    async run(next: () => number) {
      const { items } = get(publishersPanelState);
      if (items.length === 0) return "select(none)";
      const index = Math.floor(next() * items.length);
      await restorePublisherStateFromView(index);
      return `select(${index})`;
    },
  },
  {
    name: "subtab",
    async run(next: () => number) {
      const tab = SUBTABS[Math.floor(next() * SUBTABS.length)];
      selectPublisherSubtab(tab);
      return `subtab(${tab})`;
    },
  },
  {
    name: "editPayload",
    async run(next: () => number) {
      const value = `{"n":${Math.floor(next() * 1000)}}`;
      updatePublisherPayload(value);
      return `editPayload(${value})`;
    },
  },
  {
    name: "editUrl",
    async run(next: () => number) {
      const value = `https://example.test/${Math.floor(next() * 1000)}`;
      updatePublisherRequestField("pub-url", value);
      return `editUrl(${value})`;
    },
  },
  {
    name: "addHeader",
    async run() {
      addPublisherMetadataRow();
      return "addHeader()";
    },
  },
  {
    name: "editHeader",
    async run(next: () => number) {
      const { metadataRows } = get(publishersPanelState);
      if (metadataRows.length === 0) return "editHeader(none)";
      const index = Math.floor(next() * metadataRows.length);
      updatePublisherMetadataRow(index, next() < 0.5 ? "key" : "value", `v${Math.floor(next() * 100)}`);
      return `editHeader(${index})`;
    },
  },
  {
    name: "removeHeader",
    async run(next: () => number) {
      const { metadataRows } = get(publishersPanelState);
      if (metadataRows.length === 0) return "removeHeader(none)";
      const index = Math.floor(next() * metadataRows.length);
      removePublisherMetadataRow(index);
      return `removeHeader(${index})`;
    },
  },
  {
    name: "add",
    async run(next: () => number) {
      const type = next() < 0.5 ? "memory" : "http";
      await addPublisherAction(type);
      return `add(${type})`;
    },
  },
  {
    name: "clone",
    async run() {
      cloneCurrentPublisherAction();
      return "clone()";
    },
  },
  {
    name: "save",
    async run() {
      await saveCurrentPublisherAction();
      return "save()";
    },
  },
  {
    name: "delete",
    async run() {
      await deleteCurrentPublisherAction();
      return "delete()";
    },
  },
];

/**
 * What must be true after any action, in any order. These are the properties a
 * user would notice breaking: a selection that points nowhere, a list and a
 * detail pane that disagree, a panel that claims publishers it does not have.
 */
function checkInvariants(state: PublishersPanelState) {
  const { items, selectedIndex, hasPublishers, metadataRows, activeSubtab } = state;

  expect(hasPublishers).toBe(items.length > 0);

  if (items.length > 0) {
    expect(selectedIndex).toBeGreaterThanOrEqual(0);
    expect(selectedIndex).toBeLessThan(items.length);
  }

  // Every sidebar row must be renderable: the panel reads these fields directly.
  for (const item of items) {
    expect(typeof item.name).toBe("string");
    expect(typeof item.endpointType).toBe("string");
    expect(Number.isInteger(item.originalIndex)).toBe(true);
  }

  // Header rows are keyed by id in the DOM; a duplicate silently merges two rows.
  const ids = metadataRows.map((row) => row.id);
  expect(new Set(ids).size).toBe(ids.length);

  expect(SUBTABS).toContain(activeSubtab);
}

function installStubs() {
  const storage = new Map<string, string>([
    ["mqb_publisher_state", "{}"],
    ["mqb_publisher_history", JSON.stringify({ version: 1, updated_at: 0, publishers: {} })],
  ]);
  installBaseWindowStubs();

  let serverConfig: any = { publishers: [], routes: {}, consumers: [], presets: {}, env_vars: {}, history: {} };
  window.appConfig = serverConfig;
  window.fetchConfigFromServer = vi.fn().mockResolvedValue({ publishers: [] });
  window.initConsumers = vi.fn();
  window.Split = vi.fn().mockReturnValue({});
  // The panel asks before destructive actions; the model always says yes so the
  // delete path is actually exercised.
  window.mqbConfirm = vi.fn().mockResolvedValue(true);
  window.mqbPrompt = vi.fn().mockResolvedValue("modelled_copy");

  Object.defineProperty(window, "localStorage", {
    value: {
      getItem: vi.fn((key: string) => storage.get(key) ?? null),
      setItem: vi.fn((key: string, value: string) => void storage.set(key, value)),
      removeItem: vi.fn((key: string) => void storage.delete(key)),
    },
    configurable: true,
  });

  globalThis.fetch = vi.fn(async (input: string, init?: RequestInit) => {
    if (String(input) === "/config" && init?.method === "POST") {
      serverConfig = JSON.parse(String(init.body || "{}"));
      window.appConfig = serverConfig;
      return { ok: true, status: 200, statusText: "OK", text: async () => "", json: async () => serverConfig };
    }
    if (String(input) === "/config") {
      return { ok: true, status: 200, statusText: "OK", json: async () => serverConfig };
    }
    return {
      ok: true,
      status: 200,
      statusText: "OK",
      text: async () => '{"status":"Ack"}',
      json: async () => ({ status: "Ack" }),
    };
  }) as any;
}

describe("publishers panel state machine", () => {
  beforeEach(() => {
    delete (window as any).__mqb_state;
    (window as any)._mqb_last_publisher_idx = undefined;
    (window as any)._mqb_last_publisher_tab = undefined;
    delete (window as any).__MQB_DESKTOP__;
    dialogMocks.open.mockReset();
    dialogMocks.save.mockReset();
    mountPublishersDom();
    installStubs();
  });

  afterEach(() => {
    vi.useRealTimers();
    restoreBaseWindowStubs();
  });

  // Several seeds, each a different action order; all must hold the invariants.
  for (const seed of [1, 7, 42, 1337, 90210]) {
    test(`holds its invariants across a random action sequence (seed ${seed})`, async () => {
      const next = random(seed);
      initPublishers(structuredClone(INITIAL_CONFIG), SCHEMA);
      checkInvariants(get(publishersPanelState));

      const script: string[] = [];
      const seenCounts = new Set<number>([get(publishersPanelState).items.length]);
      for (let step = 0; step < 40; step += 1) {
        const action = ACTIONS[Math.floor(next() * ACTIONS.length)];
        let label = action.name;
        try {
          label = await action.run(next);
          script.push(label);
        } catch (error) {
          throw new Error(`step ${step} (${label}) threw: ${error}\nscript:\n  ${script.join("\n  ")}`);
        }

        try {
          checkInvariants(get(publishersPanelState));
        } catch (error) {
          throw new Error(
            `invariant broken after step ${step} (${label}): ${error}\nscript:\n  ${script.join("\n  ")}`,
          );
        }
        seenCounts.add(get(publishersPanelState).items.length);
      }

      // Guards against a vacuous run: if every action were a silent no-op the
      // invariants would hold trivially and this test would prove nothing.
      expect(seenCounts.size, `the sequence never changed the publisher list:\n  ${script.join("\n  ")}`)
        .toBeGreaterThan(1);
    });
  }

  /**
   * The panel must survive deleting its way to empty: the sidebar, the detail
   * pane and the "no publishers" alert all read the same state.
   */
  test("deleting every publisher leaves a consistent empty panel", async () => {
    initPublishers(structuredClone(INITIAL_CONFIG), SCHEMA);

    for (let guard = 0; guard < 10 && get(publishersPanelState).items.length > 0; guard += 1) {
      await deleteCurrentPublisherAction();
      checkInvariants(get(publishersPanelState));
    }

    const state = get(publishersPanelState);
    expect(state.items).toHaveLength(0);
    expect(state.hasPublishers).toBe(false);
  });
});
