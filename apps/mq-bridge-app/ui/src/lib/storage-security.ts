import type { WorkspaceConfig } from "./workspace-config";

export type StorageModeValue =
  | "unencrypted"
  | "balanced"
  | "env_temporary_messages"
  | "temporary_messages"
  | "sensitive"
  | "durable";

export type StorageSecurityInfo = {
  target?: "cli" | "desktop";
  encrypted: boolean;
  persistent: boolean;
  keySource: "none" | "os-key-store" | "ephemeral-process" | "env";
  keyStoreAvailable?: boolean;
  encryptedConfigAvailable?: boolean;
  persistentMessagesAvailable?: boolean;
  configEncrypted: boolean;
  messagesEncrypted: boolean;
  messagesPersistent: boolean;
  reason?: "key-store-unavailable" | "key-store-write-failed" | "cli-mode";
  messageKeyHex?: string;
  kid?: string;
};

export const EMPTY_STORAGE_SECURITY: StorageSecurityInfo = {
  target: "cli",
  encrypted: false,
  persistent: true,
  keySource: "none",
  keyStoreAvailable: false,
  encryptedConfigAvailable: false,
  persistentMessagesAvailable: false,
  configEncrypted: false,
  messagesEncrypted: false,
  messagesPersistent: true,
};

export function normalizeStorageSecurityInfo(raw: unknown): StorageSecurityInfo {
  if (!raw || typeof raw !== "object") {
    return { ...EMPTY_STORAGE_SECURITY };
  }

  const value = raw as Record<string, unknown>;
  const target = value.target;
  const keySource = value.keySource ?? value.key_source;
  const reason = value.reason;

  return {
    target: target === "desktop" ? "desktop" : "cli",
    encrypted: value.encrypted === true,
    persistent: value.persistent !== false,
    keySource:
      keySource === "os-key-store" || keySource === "ephemeral-process" || keySource === "env"
        ? keySource
        : "none",
    keyStoreAvailable: value.keyStoreAvailable === true || value.key_store_available === true,
    encryptedConfigAvailable:
      value.encryptedConfigAvailable === true || value.encrypted_config_available === true,
    persistentMessagesAvailable:
      value.persistentMessagesAvailable === true || value.persistent_messages_available === true,
    configEncrypted: value.configEncrypted === true || value.config_encrypted === true,
    messagesEncrypted: value.messagesEncrypted === true || value.messages_encrypted === true,
    messagesPersistent: value.messagesPersistent === true || value.messages_persistent === true,
    ...(reason === "key-store-unavailable" || reason === "key-store-write-failed" || reason === "cli-mode"
      ? { reason }
      : {}),
    ...(typeof value.messageKeyHex === "string"
      ? { messageKeyHex: value.messageKeyHex }
      : typeof value.message_key_hex === "string"
        ? { messageKeyHex: value.message_key_hex }
        : {}),
    ...(typeof value.kid === "string" ? { kid: value.kid } : {}),
  };
}

export function availableStorageModeValues(info: StorageSecurityInfo): StorageModeValue[] {
  const target = info.target === "desktop" ? "desktop" : "cli";
  if (target === "desktop") {
    if (info.keyStoreAvailable !== true) {
      return ["unencrypted", "temporary_messages"];
    }
    return [
      "unencrypted",
      "balanced",
      "sensitive",
      ...(info.persistentMessagesAvailable === true ? ["durable" as const] : []),
    ];
  }

  return ["unencrypted", "balanced", "env_temporary_messages"];
}

export function fallbackStorageSecurity(config: Pick<WorkspaceConfig, "config_security"> | Record<string, unknown>) {
  const mode = String((config as WorkspaceConfig).config_security?.mode || "balanced");
  // These fallback objects are metadata-only. Callers such as hasEncryptedMessages() and
  // saveImportedConfig()/setStoredJson() must still check messageKeyHex/kid before assuming
  // encrypted message storage is actually writable or decryptable in the current process.
  if (mode === "durable") {
    return {
      ...EMPTY_STORAGE_SECURITY,
      encrypted: true,
      persistent: true,
      keySource: "os-key-store" as const,
      configEncrypted: true,
      messagesEncrypted: true,
      messagesPersistent: true,
    };
  }
  if (mode === "sensitive") {
    return {
      ...EMPTY_STORAGE_SECURITY,
      encrypted: true,
      persistent: false,
      keySource: "ephemeral-process" as const,
      configEncrypted: true,
      messagesEncrypted: true,
      messagesPersistent: false,
    };
  }
  if (mode === "env_temporary_messages" || mode === "temporary_messages") {
    return {
      ...EMPTY_STORAGE_SECURITY,
      encrypted: true,
      persistent: false,
      keySource: "ephemeral-process" as const,
      configEncrypted: false,
      messagesEncrypted: true,
      messagesPersistent: false,
    };
  }
  return { ...EMPTY_STORAGE_SECURITY };
}

export function resolveStorageSecurity(
  runtimeInfo: StorageSecurityInfo | null | undefined,
  config: Pick<WorkspaceConfig, "config_security"> | Record<string, unknown>,
) {
  return runtimeInfo ? normalizeStorageSecurityInfo(runtimeInfo) : fallbackStorageSecurity(config);
}

export function hasTemporaryEncryptedMessages(info: StorageSecurityInfo) {
  return info.messagesEncrypted && !info.messagesPersistent && info.keySource === "ephemeral-process";
}

export function hasEncryptedMessages(info: StorageSecurityInfo) {
  return info.messagesEncrypted && typeof info.messageKeyHex === "string" && info.messageKeyHex.length > 0;
}
