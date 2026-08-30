export interface StorageBackend {
  getItem(key: string): string | null;
  setItem(key: string, value: string): boolean;
  removeItem(key: string): boolean;
}

function getBrowserStorage(): Storage | null {
  try {
    return typeof globalThis !== "undefined" && "localStorage" in globalThis
      ? globalThis.localStorage
      : null;
  } catch {
    return null;
  }
}

export const localStorageBackend: StorageBackend = {
  getItem: (key) => {
    try {
      return getBrowserStorage()?.getItem(key) ?? null;
    } catch {
      return null;
    }
  },
  setItem: (key, value) => {
    try {
      const storage = getBrowserStorage();
      if (!storage) return false;
      storage.setItem(key, value);
      return true;
    } catch {
      return false;
    }
  },
  removeItem: (key) => {
    try {
      const storage = getBrowserStorage();
      if (!storage) return false;
      storage.removeItem(key);
      return true;
    } catch {
      return false;
    }
  },
};

let activeBackend: StorageBackend = localStorageBackend;

export function setStorageBackend(backend: StorageBackend): void {
  activeBackend = backend;
}

export function getStorageBackend(): StorageBackend {
  return activeBackend;
}

/**
 * Reads and parses JSON from the active storage backend.
 *
 * This helper does not perform runtime type validation. It returns either the parsed JSON value
 * or the provided fallback, so callers should still validate the returned shape before using it.
 */
export function readJson<T>(key: string, fallback: T): T {
  try {
    const raw = activeBackend.getItem(key);
    if (raw === null) {
      return fallback;
    }
    const parsed = JSON.parse(raw);
    return parsed as T;
  } catch {
    return fallback;
  }
}

export function writeJson<T>(key: string, value: T): void {
  try {
    activeBackend.setItem(key, JSON.stringify(value));
  } catch {}
}

export function removeKey(key: string): void {
  try {
    activeBackend.removeItem(key);
  } catch {}
}
