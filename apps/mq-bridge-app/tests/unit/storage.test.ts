// @vitest-environment jsdom

import { afterEach, describe, expect, test, vi } from "vitest";
import {
  localStorageBackend,
  readJson,
  removeKey,
  setStorageBackend,
  writeJson,
  type StorageBackend,
} from "../../ui/src/lib/storage";

describe("storage helpers", () => {
  afterEach(() => {
    setStorageBackend(localStorageBackend);
  });

  test("reads writes and removes json through the active backend", () => {
    const data = new Map<string, string>();
    const backend: StorageBackend = {
      getItem: vi.fn((key: string) => data.get(key) ?? null),
      setItem: vi.fn((key: string, value: string) => {
        data.set(key, value);
        return true;
      }),
      removeItem: vi.fn((key: string) => {
        data.delete(key);
        return true;
      }),
    };

    setStorageBackend(backend);
    writeJson("alpha", { ok: true, count: 2 });
    expect(readJson("alpha", null)).toEqual({ ok: true, count: 2 });

    removeKey("alpha");
    expect(readJson("alpha", { fallback: true })).toEqual({ fallback: true });
    expect(backend.setItem).toHaveBeenCalledOnce();
    expect(backend.removeItem).toHaveBeenCalledWith("alpha");
  });

  test("falls back on invalid json", () => {
    const backend: StorageBackend = {
      getItem: vi.fn(() => "{not-json"),
      setItem: vi.fn(() => true),
      removeItem: vi.fn(() => true),
    };

    setStorageBackend(backend);
    expect(readJson("broken", { safe: true })).toEqual({ safe: true });
  });

  test("swallows serialization failures and backend errors", () => {
    const backend: StorageBackend = {
      getItem: vi.fn(() => {
        throw new Error("read failed");
      }),
      setItem: vi.fn(() => {
        throw new Error("write failed");
      }),
      removeItem: vi.fn(() => {
        throw new Error("remove failed");
      }),
    };
    setStorageBackend(backend);

    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(() => writeJson("circular", circular)).not.toThrow();
    expect(backend.setItem).not.toHaveBeenCalled();
    expect(readJson("missing", { fallback: true })).toEqual({ fallback: true });
    expect(() => removeKey("missing")).not.toThrow();
  });

  test("handles nullish and non-serializable values without crashing", () => {
    const data = new Map<string, string>();
    const backend: StorageBackend = {
      getItem: vi.fn((key: string) => data.get(key) ?? null),
      setItem: vi.fn((key: string, value: string) => {
        data.set(key, value);
        return true;
      }),
      removeItem: vi.fn((key: string) => {
        data.delete(key);
        return true;
      }),
    };
    setStorageBackend(backend);

    expect(() => writeJson("nullish", null)).not.toThrow();
    expect(readJson("nullish", { fallback: true })).toBeNull();
    expect(readJson("undefined", { fallback: true })).toEqual({ fallback: true });
    expect(() => writeJson("function", { fn: () => true, count: 1 })).not.toThrow();
    expect(readJson("function", null)).toEqual({ count: 1 });
    expect(() => writeJson("bigint", { value: BigInt(1) })).not.toThrow();
  });

  test("localStorageBackend reports write and remove failures", () => {
    const originalLocalStorage = globalThis.localStorage;
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: {
        getItem: vi.fn(() => {
          throw new Error("get failed");
        }),
        setItem: vi.fn(() => {
          throw new Error("set failed");
        }),
        removeItem: vi.fn(() => {
          throw new Error("remove failed");
        }),
      },
    });

    expect(localStorageBackend.getItem("alpha")).toBeNull();
    expect(localStorageBackend.setItem("alpha", "{}")).toBe(false);
    expect(localStorageBackend.removeItem("alpha")).toBe(false);

    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: originalLocalStorage,
    });
  });
});
