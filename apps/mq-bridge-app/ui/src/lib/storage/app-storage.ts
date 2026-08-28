import { getStoredJson, setStoredJson } from "../encrypted-json-storage";
import type { StorageSecurityInfo } from "../storage-security";

export type ConfigBucket<T> = {
  last_changed: number;
  config: T;
};

export type RuntimeCacheBucket = {
  publisher_history?: unknown;
  consumer_messages?: Record<string, unknown>;
};

export type UiPrefsBucket = {
  active_tab?: string;
  selected_publisher_idx?: number;
  selected_consumer_idx?: number;
};

const CONFIG_KEY = "mqb_config_bucket";
const RUNTIME_CACHE_KEY = "mqb_runtime_cache_bucket";
const UI_PREFS_KEY = "mqb_ui_prefs_bucket";

export async function loadConfigBucket<T>(
  fallback: ConfigBucket<T>,
  security: StorageSecurityInfo,
) {
  return getStoredJson(CONFIG_KEY, fallback, security, { clearOnFailure: false });
}

export async function saveConfigBucket<T>(value: ConfigBucket<T>, security: StorageSecurityInfo) {
  await setStoredJson(CONFIG_KEY, value, security);
}

export async function loadRuntimeCacheBucket(
  fallback: RuntimeCacheBucket,
  security: StorageSecurityInfo,
) {
  return getStoredJson(RUNTIME_CACHE_KEY, fallback, security);
}

export async function saveRuntimeCacheBucket(value: RuntimeCacheBucket, security: StorageSecurityInfo) {
  await setStoredJson(RUNTIME_CACHE_KEY, value, security);
}

export async function loadUiPrefsBucket(
  fallback: UiPrefsBucket,
  security: StorageSecurityInfo,
) {
  return getStoredJson(UI_PREFS_KEY, fallback, security, { clearOnFailure: false });
}

export async function saveUiPrefsBucket(value: UiPrefsBucket, security: StorageSecurityInfo) {
  await setStoredJson(UI_PREFS_KEY, value, security);
}
