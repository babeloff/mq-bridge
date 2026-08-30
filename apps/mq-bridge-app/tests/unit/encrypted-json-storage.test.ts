// @vitest-environment jsdom

import { beforeEach, describe, expect, test, vi } from "vitest";
import { getStoredJson, removeStoredJson, setStoredJson } from "../../ui/src/lib/encrypted-json-storage";
import { localStorageBackend, setStorageBackend } from "../../ui/src/lib/storage";
import type { StorageSecurityInfo } from "../../ui/src/lib/storage-security";

const temporarySecurity: StorageSecurityInfo = {
  encrypted: true,
  persistent: false,
  keySource: "ephemeral-process",
  configEncrypted: false,
  messagesEncrypted: true,
  messagesPersistent: false,
  messageKeyHex: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
  kid: "session-1",
};

const persistentSecurity: StorageSecurityInfo = {
  encrypted: true,
  persistent: true,
  keySource: "os-key-store",
  configEncrypted: true,
  messagesEncrypted: true,
  messagesPersistent: true,
  messageKeyHex: "8899aabbccddeeff00112233445566778899aabbccddeeff0011223344556677",
  kid: "message-history-key:/tmp/config.yml",
};

describe("encrypted-json-storage", () => {
  beforeEach(() => {
    const store = new Map<string, string>();
    setStorageBackend(localStorageBackend);
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: {
        getItem: vi.fn((key: string) => store.get(key) ?? null),
        setItem: vi.fn((key: string, value: string) => {
          store.set(key, value);
        }),
        removeItem: vi.fn((key: string) => {
          store.delete(key);
        }),
      },
    });
  });

  test("encrypts temporary message storage at rest", async () => {
    await setStoredJson("mqb_consumer_messages", { orders: [{ payload: "secret" }] }, temporarySecurity);

    const raw = window.localStorage.getItem("mqb_consumer_messages") || "";
    expect(raw).toContain("\"alg\":\"AES-256-GCM\"");
    expect(raw).not.toContain("secret");

    await expect(
      getStoredJson("mqb_consumer_messages", {}, temporarySecurity),
    ).resolves.toEqual({ orders: [{ payload: "secret" }] });
  });

  test("clears unreadable encrypted payloads for ephemeral keys", async () => {
    await setStoredJson("mqb_consumer_messages", { orders: [{ payload: "secret" }] }, temporarySecurity);

    const wrongKey = {
      ...temporarySecurity,
      messageKeyHex: "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100",
    };

    await expect(getStoredJson("mqb_consumer_messages", {}, wrongKey)).resolves.toEqual({});
    expect(window.localStorage.getItem("mqb_consumer_messages")).toBeNull();
  });

  test("keeps unreadable encrypted payloads for persistent keys", async () => {
    await setStoredJson("mqb_publisher_history", { rows: [{ payload: "secret" }] }, persistentSecurity);
    const raw = window.localStorage.getItem("mqb_publisher_history");

    const wrongKey = {
      ...persistentSecurity,
      messageKeyHex: "7766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa9988",
    };

    await expect(getStoredJson("mqb_publisher_history", {}, wrongKey)).resolves.toEqual({});
    expect(window.localStorage.getItem("mqb_publisher_history")).toBe(raw);
  });

  test("binds ciphertext to its storage slot via authenticated data", async () => {
    await setStoredJson("mqb_consumer_messages", { orders: [{ payload: "secret" }] }, temporarySecurity);

    const raw = window.localStorage.getItem("mqb_consumer_messages");
    expect(raw).toBeTruthy();
    window.localStorage.setItem("mqb_other_messages", raw as string);

    await expect(
      getStoredJson("mqb_other_messages", { fallback: true }, temporarySecurity, { clearOnFailure: false }),
    ).resolves.toEqual({ fallback: true });
  });

  test("uses the configured storage backend for writes and removals", async () => {
    const store = new Map<string, string>();
    setStorageBackend({
      getItem: (key) => store.get(key) ?? null,
      setItem: (key, value) => {
        store.set(key, value);
      },
      removeItem: (key) => {
        store.delete(key);
      },
    });

    await setStoredJson("mqb_publisher_history", { rows: [1] }, persistentSecurity);
    expect(store.get("mqb_publisher_history")).toBeTruthy();

    removeStoredJson("mqb_publisher_history");
    expect(store.has("mqb_publisher_history")).toBe(false);
  });
});
