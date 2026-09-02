// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const runtimeStatusStoreSet = vi.fn();
const activeMainTabSet = vi.fn();
const storageSecurityStoreSet = vi.fn();
const workspaceDirtyStoreSet = vi.fn();
const workspaceSavingStoreSet = vi.fn();

let capturedOnStatus: ((status: any) => void) | null = null;

const runtimeState = {
  runtime_status: {
    active_consumers: [],
    active_routes: [],
    route_throughput: {},
    consumers: {},
  },
  dirty_sections: {},
  saved_sections: {},
};

vi.mock("../../ui/src/lib/stores", () => ({
  activeMainTab: { set: activeMainTabSet },
  runtimeStatusStore: { set: runtimeStatusStoreSet },
  storageSecurityStore: { set: storageSecurityStoreSet },
  workspaceDirtyStore: { set: workspaceDirtyStoreSet },
  workspaceSavingStore: { set: workspaceSavingStoreSet },
}));

vi.mock("../../ui/src/lib/runtime-status", () => ({
  EMPTY_PEER_STATUS: {
    current_instance_id: "",
    instances: [],
  },
  EMPTY_RUNTIME_STATUS: {
    active_consumers: [],
    active_routes: [],
    route_throughput: {},
    consumers: {},
  },
  createRuntimeStatusPoller: (options: { onStatus?: (status: any) => void }) => {
    capturedOnStatus = options.onStatus || null;
    return {
      poll: vi.fn(),
      start: vi.fn(),
      stop: vi.fn(),
    };
  },
}));

vi.mock("../../ui/src/lib/runtime-window", () => ({
  appWindow: () => window as any,
  clearLegacyPendingRestoreGlobals: vi.fn(),
  currentHash: () => "#publishers",
  getMqbState: () => {
    const w = window as any;
    if (w._mqb_runtime_status !== undefined) {
      runtimeState.runtime_status = w._mqb_runtime_status;
    }
    w._mqb_runtime_status = runtimeState.runtime_status;
    return runtimeState;
  },
  mqbApp: {
    config: vi.fn(() => ({})),
    schema: vi.fn(() => ({})),
    setConfig: vi.fn(),
    setSchema: vi.fn(),
    isDesktop: vi.fn(() => false),
    init: {
      consumers: vi.fn(),
      publishers: vi.fn(),
    },
  },
  mqbDialogs: {
    alert: vi.fn(),
    confirm: vi.fn(),
    prompt: vi.fn(),
    choose: vi.fn(),
  },
  onHashChange: vi.fn(),
  replaceHash: vi.fn(),
}));

vi.mock("../../ui/src/lib/dirty-state", () => ({
  createDirtyTracker: vi.fn(() => ["publishers", { buttonId: "pub-save", getValue: () => ({}), baseline: "{}" }]),
  cloneSectionState: vi.fn((v) => v),
  isDirty: vi.fn(() => false),
}));
vi.mock("../../ui/src/lib/config-api", () => ({
  saveWholeConfig: vi.fn(),
  saveConfigSection: vi.fn(),
  fetchConfigFromServer: vi.fn(),
  fetchConfigRecoveryFromServer: vi.fn(),
  fetchStorageSecurityFromServer: vi.fn(),
  postResetConfigRecovery: vi.fn(),
}));
vi.mock("../../ui/src/lib/consumers-view", () => ({ initConsumers: vi.fn(), restoreConsumerStateFromView: vi.fn() }));
vi.mock("../../ui/src/lib/publishers-view", () => ({ initPublishers: vi.fn(), restorePublisherStateFromView: vi.fn() }));
vi.mock("../../ui/src/lib/settings", () => ({
  extractSettingsConfig: vi.fn((config) => config),
  initSettings: vi.fn(),
}));
vi.mock("../../ui/src/lib/dialogs", () => ({ installDialogs: vi.fn() }));
vi.mock("../../ui/src/lib/routing", () => ({
  nextHashForTab: vi.fn(() => "#publishers"),
  pickDefaultTab: vi.fn(() => "publishers"),
  resolveTabFromHash: vi.fn(() => "publishers"),
}));

describe("bootstrap runtime status sync", () => {
  // In afterEach, not at the end of a test: a failing assertion skipped it.
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  beforeEach(async () => {
    vi.resetModules();
    runtimeStatusStoreSet.mockReset();
    activeMainTabSet.mockReset();
    storageSecurityStoreSet.mockReset();
    workspaceDirtyStoreSet.mockReset();
    workspaceSavingStoreSet.mockReset();
    capturedOnStatus = null;
    runtimeState.runtime_status = {
      active_consumers: [],
      active_routes: [],
      route_throughput: {},
      consumers: {},
    };
    (window as any)._mqb_runtime_status = {
      active_consumers: [],
      active_routes: [],
      route_throughput: {},
      consumers: {},
    };

    await import("../../ui/src/bootstrap");
  });

  test("refreshes storage security after saving config", async () => {
    const {
      fetchConfigFromServer,
      fetchConfigRecoveryFromServer,
      saveWholeConfig,
      fetchStorageSecurityFromServer,
    } = await import("../../ui/src/lib/config-api");
    const { bootstrapApp } = await import("../../ui/src/bootstrap");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        json: async () => ({ properties: {} }),
      }),
    );
    vi.stubGlobal("localStorage", {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });
    vi.mocked(fetchConfigFromServer).mockResolvedValue({
      consumers: [],
      publishers: [],
      default_tab: "publishers",
      config_security: { mode: "balanced" },
    });
    vi.mocked(fetchConfigRecoveryFromServer).mockResolvedValue(null);
    vi.mocked(fetchStorageSecurityFromServer).mockResolvedValueOnce({
      encrypted: false,
      persistent: true,
      keySource: "env",
      configEncrypted: false,
      messagesEncrypted: false,
      messagesPersistent: true,
    });
    vi.mocked(fetchStorageSecurityFromServer).mockResolvedValue({
      encrypted: true,
      persistent: false,
      keySource: "ephemeral-process",
      configEncrypted: true,
      messagesEncrypted: true,
      messagesPersistent: false,
    });

    (window as any).appConfig = { consumers: [], publishers: [], config_security: { mode: "balanced" } };
    vi.mocked(saveWholeConfig).mockResolvedValueOnce({
      consumers: [],
      publishers: [],
      config_security: { mode: "sensitive" },
    });

    await bootstrapApp();
    await (window as any).saveConfig();

    expect(fetchStorageSecurityFromServer).toHaveBeenCalled();
    expect(storageSecurityStoreSet).toHaveBeenLastCalledWith({
      target: "cli",
      encrypted: true,
      persistent: false,
      keySource: "ephemeral-process",
      keyStoreAvailable: false,
      encryptedConfigAvailable: false,
      persistentMessagesAvailable: false,
      configEncrypted: true,
      messagesEncrypted: true,
      messagesPersistent: false,
    });
    expect((window as any)._mqb_storage_security).toEqual({
      target: "cli",
      encrypted: true,
      persistent: false,
      keySource: "ephemeral-process",
      keyStoreAvailable: false,
      encryptedConfigAvailable: false,
      persistentMessagesAvailable: false,
      configEncrypted: true,
      messagesEncrypted: true,
      messagesPersistent: false,
    });
  });

  test("publishes the fresh polled status even when legacy global is stale", () => {
    const nextStatus = {
      active_consumers: ["memory_consumer", "http222"],
      active_routes: [],
      route_throughput: {},
      consumers: {},
    };
    expect(capturedOnStatus).toBeTypeOf("function");

    capturedOnStatus?.(nextStatus);

    expect((window as any)._mqb_runtime_status).toEqual(nextStatus);
    expect(runtimeStatusStoreSet).toHaveBeenLastCalledWith(nextStatus);
  });

  test("offers encrypted config recovery reset before continuing bootstrap", async () => {
    const {
      fetchConfigFromServer,
      fetchConfigRecoveryFromServer,
      fetchStorageSecurityFromServer,
      postResetConfigRecovery,
    } = await import("../../ui/src/lib/config-api");
    const { bootstrapApp } = await import("../../ui/src/bootstrap");
    const { mqbDialogs } = await import("../../ui/src/lib/runtime-window");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        json: async () => ({ properties: {} }),
      }),
    );
    vi.stubGlobal("localStorage", {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });

    vi.mocked(fetchConfigRecoveryFromServer).mockResolvedValue({
      message: "The encrypted config could not be decrypted with the available key.",
      detail: "Failed to decrypt sensitive config",
    });
    vi.mocked((mqbDialogs as any).choose).mockResolvedValue("reset");
    vi.mocked((mqbDialogs as any).alert).mockResolvedValue(undefined);
    vi.mocked(postResetConfigRecovery).mockResolvedValue({
      backup_path: "/tmp/config.yml.recovery-20260507-174500.bak",
    });
    vi.mocked(fetchConfigFromServer).mockResolvedValue({
      consumers: [],
      publishers: [],
      default_tab: "publishers",
      config_security: { mode: "balanced" },
    });
    vi.mocked(fetchStorageSecurityFromServer).mockResolvedValue({
      encrypted: false,
      persistent: true,
      keySource: "env",
      configEncrypted: false,
      messagesEncrypted: false,
      messagesPersistent: true,
    });

    await bootstrapApp();

    expect(mqbDialogs.choose).toHaveBeenCalled();
    expect(postResetConfigRecovery).toHaveBeenCalled();
    expect(mqbDialogs.alert).toHaveBeenCalledWith(
      "The unreadable config was backed up to:\n/tmp/config.yml.recovery-20260507-174500.bak",
      "Encrypted Config Reset",
    );
  });
});
