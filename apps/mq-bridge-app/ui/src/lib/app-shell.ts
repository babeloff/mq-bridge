import Split from "split.js";
import * as VanillaSchemaForms from "vanilla-schema-forms";
import type { MainTab, RuntimeStatus } from "./runtime-status";
import { EMPTY_RUNTIME_STATUS } from "./runtime-status";
import { browserWindow } from "./browser";
import { EMPTY_STORAGE_SECURITY, type StorageSecurityInfo } from "./storage-security";
import type { FeatureAvailabilityResponse } from "./generated/ui-types";

export type AppState = {
  active_tab?: MainTab;
  consumers_initialized?: boolean;
  publishers_initialized?: boolean;
  config_initialized?: boolean;
  pending_consumer_restore?: { idx: number; tab?: string } | null;
  pending_publisher_restore?: { idx: number; tab?: string } | null;
  last_consumer_idx?: number;
  last_publisher_idx?: number;
  last_consumer_tab?: "definition" | "response" | "messages";
  last_publisher_tab?: "payload" | "headers" | "history" | "definition";
  runtime_status: RuntimeStatus;
  storage_security?: StorageSecurityInfo;
  features?: FeatureAvailabilityResponse;
  storage_cache?: {
    publisher_state?: Record<string, unknown>;
    publisher_history?: unknown;
    consumer_messages?: Record<string, unknown>;
  };
  before_workspace_save_hooks: Record<string, () => void | Promise<void>>;
  after_workspace_save_hooks: Record<string, (savedConfig: Record<string, unknown>) => void | Promise<void>>;
  dirty_sections: Record<string, { buttonId: string; getValue: () => unknown; baseline: string }>;
  saved_sections: Record<string, unknown>;
  runtime_poll_timer?: number;
  consumer_poll_timer?: number | null;
  consumer_poll_nonce?: number;
  form_mode?: string | null;
  cons_split?: unknown;
};

type AppShellBridge = {
  config: Record<string, unknown>;
  schema: Record<string, unknown>;
  desktop: boolean;
  initConsumers?: (...args: any[]) => void | Promise<void>;
  initPublishers?: (...args: any[]) => void | Promise<void>;
  restoreConsumerState?: (idx: number, options?: { tab?: string }) => void | Promise<void>;
  restorePublisherState?: (idx: number, options?: { tab?: string }) => void | Promise<void>;
  switchMain?: (name: MainTab) => void | Promise<void>;
  syncSaveButtonLabel?: (button: HTMLElement | null) => void;
  registerDirtySection?: (
    sectionName: string,
    options: { buttonId: string; getValue: () => unknown },
  ) => void;
  registerBeforeWorkspaceSave?: (key: string, callback: () => void | Promise<void>) => void;
  registerAfterWorkspaceSave?: (
    key: string,
    callback: (savedConfig: Record<string, unknown>) => void | Promise<void>,
  ) => void;
  refreshDirtySection?: (sectionName: string) => boolean;
  markSectionSaved?: (sectionName: string, savedValue?: unknown) => void;
  pollRuntimeStatus?: () => Promise<unknown>;
  fetchConfigFromServer?: <T>() => Promise<T>;
  saveWorkspace?: (
    silent?: boolean,
    button?: (HTMLElement & { loading?: boolean }) | null,
  ) => Promise<Record<string, unknown> | boolean | null>;
  saveConfig?: (
    silent?: boolean,
    button?: (HTMLElement & { loading?: boolean }) | null,
  ) => Promise<Record<string, unknown> | boolean | null>;
  saveConfigSection?: (
    sectionName: string,
    sectionValue: unknown,
    silent?: boolean,
    button?: (HTMLElement & { loading?: boolean }) | null,
  ) => Promise<any>;
};

function createDefaultAppState(): AppState {
  return {
    active_tab: "publishers",
    consumers_initialized: false,
    publishers_initialized: false,
    config_initialized: false,
    pending_consumer_restore: null,
    pending_publisher_restore: null,
    last_consumer_idx: 0,
    last_publisher_idx: 0,
    last_consumer_tab: "messages",
    last_publisher_tab: "payload",
    runtime_status: { ...EMPTY_RUNTIME_STATUS },
    storage_security: { ...EMPTY_STORAGE_SECURITY },
    storage_cache: undefined,
    before_workspace_save_hooks: {},
    after_workspace_save_hooks: {},
    dirty_sections: {},
    saved_sections: {},
    runtime_poll_timer: undefined,
    consumer_poll_timer: null,
    consumer_poll_nonce: 0,
    form_mode: null,
    cons_split: undefined,
  };
}

const appState = createDefaultAppState();

const appShellBridge: AppShellBridge = {
  config: {},
  schema: {},
  desktop: false,
};
let hasBridgeConfig = false;
let hasBridgeSchema = false;

function getFormsLibrary(): any {
  return browserWindow().VanillaSchemaForms || VanillaSchemaForms;
}

export function getAppState() {
  return appState;
}

export function resetAppState() {
  Object.assign(appState, createDefaultAppState());
}

export function configureAppShell(next: Partial<AppShellBridge>) {
  if ("config" in next) hasBridgeConfig = true;
  if ("schema" in next) hasBridgeSchema = true;
  Object.assign(appShellBridge, next);
  Object.assign(browserWindow(), next);
}

export function switchMainTab(name: MainTab) {
  return appShellBridge.switchMain?.(name) ?? browserWindow().switchMain?.(name);
}

export function hasMainTabSwitch() {
  return typeof appShellBridge.switchMain === "function" || typeof browserWindow().switchMain === "function";
}

export const workspaceRuntime = {
  state() {
    return appState;
  },
  refreshDirtySection(sectionName: string) {
    return appShellBridge.refreshDirtySection?.(sectionName) ?? browserWindow().refreshDirtySection?.(sectionName) ?? false;
  },
  registerDirtySection(sectionName: string, options: { buttonId: string; getValue: () => unknown }) {
    appShellBridge.registerDirtySection?.(sectionName, options);
    if (!appShellBridge.registerDirtySection) browserWindow().registerDirtySection?.(sectionName, options);
  },
  registerBeforeWorkspaceSave(key: string, callback: () => void | Promise<void>) {
    appShellBridge.registerBeforeWorkspaceSave?.(key, callback);
    if (!appShellBridge.registerBeforeWorkspaceSave) browserWindow().registerBeforeWorkspaceSave?.(key, callback);
  },
  registerAfterWorkspaceSave(key: string, callback: (savedConfig: Record<string, unknown>) => void | Promise<void>) {
    appShellBridge.registerAfterWorkspaceSave?.(key, callback);
    if (!appShellBridge.registerAfterWorkspaceSave) browserWindow().registerAfterWorkspaceSave?.(key, callback);
  },
  markSectionSaved(sectionName: string, savedValue?: unknown) {
    appShellBridge.markSectionSaved?.(sectionName, savedValue);
    if (!appShellBridge.markSectionSaved) browserWindow().markSectionSaved?.(sectionName, savedValue);
  },
  saveWorkspace(silent = false, button?: HTMLElement | null) {
    return appShellBridge.saveWorkspace?.(silent, button) ?? browserWindow().saveWorkspace?.(silent, button);
  },
  saveConfig(silent = false, button?: HTMLElement | null) {
    return appShellBridge.saveConfig?.(silent, button) ?? browserWindow().saveConfig?.(silent, button);
  },
  saveConfigSection(sectionName: string, sectionValue: unknown, silent = false, button?: HTMLElement | null) {
    return appShellBridge.saveConfigSection?.(sectionName, sectionValue, silent, button)
      ?? browserWindow().saveConfigSection?.(sectionName, sectionValue, silent, button);
  },
  fetchConfigFromServer<T>() {
    const fetchConfigFromServer = browserWindow().fetchConfigFromServer as (<U>() => Promise<U>) | undefined;
    return appShellBridge.fetchConfigFromServer?.<T>() ?? fetchConfigFromServer?.<T>() ?? Promise.resolve({} as T);
  },
  pollRuntimeStatus() {
    return appShellBridge.pollRuntimeStatus?.() ?? browserWindow().pollRuntimeStatus?.();
  },
  syncSaveButtonLabel(button: HTMLElement | null) {
    appShellBridge.syncSaveButtonLabel?.(button);
    if (!appShellBridge.syncSaveButtonLabel) browserWindow().syncSaveButtonLabel?.(button);
  },
};

export const appShell = {
  config<T extends Record<string, unknown>>() {
    const windowConfig = browserWindow().appConfig;
    return ((hasBridgeConfig ? appShellBridge.config : windowConfig) || {}) as T;
  },
  setConfig(value: Record<string, unknown>) {
    hasBridgeConfig = true;
    appShellBridge.config = value;
    browserWindow().appConfig = value;
  },
  schema<T extends Record<string, unknown>>() {
    const windowSchema = browserWindow().appSchema;
    return ((hasBridgeSchema ? appShellBridge.schema : windowSchema) || {}) as T;
  },
  setSchema(value: Record<string, unknown>) {
    hasBridgeSchema = true;
    appShellBridge.schema = value;
    browserWindow().appSchema = value;
  },
  isDesktop() {
    return Boolean(appShellBridge.desktop || browserWindow().__MQB_DESKTOP__);
  },
  setDesktop(value: boolean) {
    appShellBridge.desktop = value;
  },
  split() {
    return Split;
  },
  init: {
    consumers<T extends Record<string, unknown>, S extends Record<string, unknown>>(config: T, schema: S) {
      return appShellBridge.initConsumers?.(config, schema) ?? browserWindow().initConsumers?.(config, schema);
    },
    publishers<T extends Record<string, unknown>, S extends Record<string, unknown>>(config: T, schema: S) {
      return appShellBridge.initPublishers?.(config, schema) ?? browserWindow().initPublishers?.(config, schema);
    },
  },
  restore: {
    consumers(idx: number, options?: { tab?: string }) {
      return appShellBridge.restoreConsumerState?.(idx, options) ?? browserWindow().restoreConsumerState?.(idx, options);
    },
    publishers(idx: number, options?: { tab?: string }) {
      return appShellBridge.restorePublisherState?.(idx, options) ?? browserWindow().restorePublisherState?.(idx, options);
    },
  },
  libs: {
    split() {
      return Split;
    },
    forms() {
      return getFormsLibrary();
    },
  },
  forms() {
    return getFormsLibrary();
  },
};
