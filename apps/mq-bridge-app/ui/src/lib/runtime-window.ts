import { appShell, configureAppShell, getAppState, workspaceRuntime, type AppState } from "./app-shell";
import { browserWindow, currentHash, onHashChange, replaceHash, type AppWindow } from "./browser";
import { alertDialog, chooseDialog, confirmDialog, promptDialog } from "./dialogs";

export type { AppWindow } from "./browser";
export type MqbState = AppState;

export function appWindow() {
  return browserWindow();
}

function hydrateLegacyField<T extends keyof AppState>(
  state: AppState,
  key: T,
  value: AppState[T] | undefined,
) {
  if (state[key] === undefined && value !== undefined) {
    state[key] = value;
  }
}

function syncLegacyWindowState(state: AppState) {
  const win = browserWindow() as any;
  win.__mqb_state = state;
  win._mqb_active_tab = state.active_tab;
  win._mqb_consumers_initialized = state.consumers_initialized;
  win._mqb_publishers_initialized = state.publishers_initialized;
  win._mqb_config_initialized = state.config_initialized;
  win._mqb_pending_consumer_restore = state.pending_consumer_restore;
  win._mqb_pending_publisher_restore = state.pending_publisher_restore;
  win._mqb_last_consumer_idx = state.last_consumer_idx;
  win._mqb_last_publisher_idx = state.last_publisher_idx;
  win._mqb_last_consumer_tab = state.last_consumer_tab;
  win._mqb_last_publisher_tab = state.last_publisher_tab;
  win._mqb_runtime_status = state.runtime_status;
  win._mqb_storage_security = state.storage_security;
  win._mqb_features = state.features;
  win._mqb_storage_cache = state.storage_cache;
  win._mqb_before_workspace_save_hooks = state.before_workspace_save_hooks;
  win._mqb_after_workspace_save_hooks = state.after_workspace_save_hooks;
  win._mqb_dirty_sections = state.dirty_sections;
  win._mqb_saved_sections = state.saved_sections;
  win._mqb_runtime_poll_timer = state.runtime_poll_timer;
  win._mqb_consumer_poll_timer = state.consumer_poll_timer;
  win._mqb_consumer_poll_nonce = state.consumer_poll_nonce;
  win._mqb_form_mode = state.form_mode;
  win._consSplit = state.cons_split;
}

export function getMqbState() {
  const win = browserWindow() as any;
  const state = getAppState();
  if (!win.__mqb_state_hydrated) {
    const legacyState = win.__mqb_state && win.__mqb_state !== state && typeof win.__mqb_state === "object"
      ? win.__mqb_state as Partial<AppState>
      : null;
    if (legacyState) {
      for (const key of Object.keys(legacyState) as Array<keyof AppState>) {
        hydrateLegacyField(state, key, legacyState[key]);
      }
    }
    hydrateLegacyField(state, "active_tab", win._mqb_active_tab);
    hydrateLegacyField(state, "consumers_initialized", win._mqb_consumers_initialized === undefined ? undefined : Boolean(win._mqb_consumers_initialized));
    hydrateLegacyField(state, "publishers_initialized", win._mqb_publishers_initialized === undefined ? undefined : Boolean(win._mqb_publishers_initialized));
    hydrateLegacyField(state, "config_initialized", win._mqb_config_initialized === undefined ? undefined : Boolean(win._mqb_config_initialized));
    hydrateLegacyField(state, "pending_consumer_restore", win._mqb_pending_consumer_restore);
    hydrateLegacyField(state, "pending_publisher_restore", win._mqb_pending_publisher_restore);
    hydrateLegacyField(state, "last_consumer_idx", win._mqb_last_consumer_idx);
    hydrateLegacyField(state, "last_publisher_idx", win._mqb_last_publisher_idx);
    hydrateLegacyField(state, "last_consumer_tab", win._mqb_last_consumer_tab);
    hydrateLegacyField(state, "last_publisher_tab", win._mqb_last_publisher_tab);
    hydrateLegacyField(state, "runtime_status", win._mqb_runtime_status);
    hydrateLegacyField(state, "storage_security", win._mqb_storage_security);
    hydrateLegacyField(state, "features", win._mqb_features);
    hydrateLegacyField(state, "storage_cache", win._mqb_storage_cache);
    hydrateLegacyField(state, "before_workspace_save_hooks", win._mqb_before_workspace_save_hooks);
    hydrateLegacyField(state, "after_workspace_save_hooks", win._mqb_after_workspace_save_hooks);
    hydrateLegacyField(state, "dirty_sections", win._mqb_dirty_sections);
    hydrateLegacyField(state, "saved_sections", win._mqb_saved_sections);
    hydrateLegacyField(state, "runtime_poll_timer", win._mqb_runtime_poll_timer);
    hydrateLegacyField(state, "consumer_poll_timer", win._mqb_consumer_poll_timer);
    hydrateLegacyField(state, "consumer_poll_nonce", win._mqb_consumer_poll_nonce);
    hydrateLegacyField(state, "form_mode", win._mqb_form_mode);
    hydrateLegacyField(state, "cons_split", win._consSplit);
    win.__mqb_state_hydrated = true;
  }
  syncLegacyWindowState(state);
  return state;
}

export function clearLegacyPendingRestoreGlobals() {
  const state = getAppState();
  state.pending_consumer_restore = null;
  state.pending_publisher_restore = null;
  const win = browserWindow() as any;
  if (win.__mqb_state && typeof win.__mqb_state === "object") {
    win.__mqb_state.pending_consumer_restore = null;
    win.__mqb_state.pending_publisher_restore = null;
  }
  win._mqb_pending_consumer_restore = null;
  win._mqb_pending_publisher_restore = null;
}

export function configureRuntimeBridge(next: Parameters<typeof configureAppShell>[0]) {
  configureAppShell(next);
}

export { currentHash, onHashChange, replaceHash };

export const mqbDialogs = {
  alert(message: string, title?: string) {
    const dialog = browserWindow().mqbAlert;
    if (dialog && dialog !== mqbDialogs.alert) return dialog(message, title);
    return alertDialog(message, title);
  },
  confirm(message: string, title?: string) {
    const dialog = browserWindow().mqbConfirm;
    if (dialog && dialog !== mqbDialogs.confirm) return dialog(message, title);
    return confirmDialog(message, title);
  },
  prompt(message: string, title?: string, options?: {
    confirmLabel?: string;
    cancelLabel?: string;
    value?: string;
    placeholder?: string;
  }) {
    const dialog = browserWindow().mqbPrompt;
    if (dialog && dialog !== mqbDialogs.prompt) return dialog(message, title, options);
    return promptDialog(message, title, options);
  },
  choose(message: string, title?: string, options?: {
    confirmLabel?: string;
    cancelLabel?: string;
    choices?: Array<{ value: string; label: string; description?: string }>;
  }) {
    const dialog = browserWindow().mqbChoose;
    if (dialog && dialog !== mqbDialogs.choose) return dialog(message, title, options);
    return chooseDialog(message, title, options);
  },
};

export const mqbRuntime = workspaceRuntime;
export const mqbApp = appShell;
