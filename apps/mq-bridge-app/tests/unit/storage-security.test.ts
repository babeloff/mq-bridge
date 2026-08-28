import { describe, expect, test } from "vitest";
import {
  availableStorageModeValues,
  fallbackStorageSecurity,
  normalizeStorageSecurityInfo,
} from "../../ui/src/lib/storage-security";

describe("storage-security", () => {
  test("shows only cli-relevant storage modes", () => {
    const options = availableStorageModeValues(
      normalizeStorageSecurityInfo({
        target: "cli",
        key_source: "env",
        encrypted: false,
        persistent: true,
        config_encrypted: false,
        messages_encrypted: false,
        messages_persistent: true,
      }),
    );

    expect(options).toEqual([
      "unencrypted",
      "balanced",
      "env_temporary_messages",
    ]);
  });

  test("shows desktop fallback modes when no key store is available", () => {
    const info = normalizeStorageSecurityInfo({
      target: "desktop",
      key_source: "ephemeral-process",
      key_store_available: false,
      encrypted_config_available: false,
      persistent_messages_available: false,
      encrypted: true,
      persistent: false,
      config_encrypted: false,
      messages_encrypted: true,
      messages_persistent: false,
      reason: "key-store-unavailable",
    });

    expect(availableStorageModeValues(info)).toEqual([
      "unencrypted",
      "temporary_messages",
    ]);
  });

  test("omits durable mode when desktop persistent history is not available", () => {
    const options = availableStorageModeValues(
      normalizeStorageSecurityInfo({
        target: "desktop",
        key_source: "os-key-store",
        key_store_available: true,
        encrypted_config_available: true,
        persistent_messages_available: false,
        encrypted: false,
        persistent: true,
        config_encrypted: false,
        messages_encrypted: false,
        messages_persistent: true,
      }),
    );

    expect(options).not.toContain("durable");
  });

  test("falls back to temporary encrypted message handling for temporary modes", () => {
    expect(
      fallbackStorageSecurity({
        config_security: { mode: "env_temporary_messages" },
      }),
    ).toEqual(
      expect.objectContaining({
        encrypted: true,
        persistent: false,
        configEncrypted: false,
        messagesEncrypted: true,
        messagesPersistent: false,
        keySource: "ephemeral-process",
      }),
    );

    expect(
      fallbackStorageSecurity({
        config_security: { mode: "temporary_messages" },
      }),
    ).toEqual(
      expect.objectContaining({
        encrypted: true,
        persistent: false,
        configEncrypted: false,
        messagesEncrypted: true,
        messagesPersistent: false,
        keySource: "ephemeral-process",
      }),
    );
  });

  test("falls back to encrypted config and temporary encrypted messages for sensitive mode", () => {
    expect(
      fallbackStorageSecurity({
        config_security: { mode: "sensitive" },
      }),
    ).toEqual(
      expect.objectContaining({
        encrypted: true,
        persistent: false,
        configEncrypted: true,
        messagesEncrypted: true,
        messagesPersistent: false,
        keySource: "ephemeral-process",
      }),
    );
  });
});
