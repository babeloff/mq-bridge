import { hasEncryptedMessages, hasTemporaryEncryptedMessages, type StorageSecurityInfo } from "./storage-security";
import { getStorageBackend } from "./storage";

type EncryptedEnvelope = {
  v: number;
  alg: "AES-256-GCM";
  kid: string;
  nonce: string;
  ciphertext: string;
};

function utf8Encode(value: string) {
  return new TextEncoder().encode(value);
}

function bytesToBase64(bytes: Uint8Array) {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function base64ToBytes(value: string) {
  const binary = atob(value);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

function hexToBytes(value: string) {
  const normalized = value.trim();
  if (normalized.length === 0 || normalized.length % 2 !== 0) {
    throw new Error("Invalid hex key length");
  }
  if (!/^[0-9a-fA-F]+$/.test(normalized)) {
    throw new Error("Invalid messageKeyHex value");
  }
  const bytes = new Uint8Array(normalized.length / 2);
  for (let index = 0; index < normalized.length; index += 2) {
    bytes[index / 2] = parseInt(normalized.slice(index, index + 2), 16);
  }
  return bytes;
}

async function importMessageKey(security: StorageSecurityInfo) {
  if (!security.messageKeyHex) {
    throw new Error("Missing message encryption key");
  }
  return crypto.subtle.importKey("raw", hexToBytes(security.messageKeyHex), "AES-GCM", false, ["encrypt", "decrypt"]);
}

function isEnvelope(value: unknown): value is EncryptedEnvelope {
  if (!value || typeof value !== "object") return false;
  const envelope = value as Record<string, unknown>;
  return envelope.v === 1
    && envelope.alg === "AES-256-GCM"
    && typeof envelope.kid === "string"
    && typeof envelope.nonce === "string"
    && typeof envelope.ciphertext === "string";
}

async function encryptJson(slot: string, value: unknown, security: StorageSecurityInfo) {
  const key = await importMessageKey(security);
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt(
    {
      name: "AES-GCM",
      iv: nonce,
      additionalData: utf8Encode(slot),
    },
    key,
    utf8Encode(JSON.stringify(value)),
  );
  const envelope: EncryptedEnvelope = {
    v: 1,
    alg: "AES-256-GCM",
    kid: security.kid || "ephemeral-process",
    nonce: bytesToBase64(nonce),
    ciphertext: bytesToBase64(new Uint8Array(ciphertext)),
  };
  return JSON.stringify(envelope);
}

async function decryptJson<T>(slot: string, raw: string, security: StorageSecurityInfo): Promise<T> {
  const parsed = JSON.parse(raw) as unknown;
  if (!isEnvelope(parsed)) {
    return parsed as T;
  }
  const key = await importMessageKey(security);
  const plaintext = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: base64ToBytes(parsed.nonce),
      additionalData: utf8Encode(slot),
    },
    key,
    base64ToBytes(parsed.ciphertext),
  );
  return JSON.parse(new TextDecoder().decode(plaintext)) as T;
}

export async function getStoredJson<T>(
  storageKey: string,
  fallback: T,
  security: StorageSecurityInfo,
  options: { clearOnFailure?: boolean } = {},
) {
  const storage = getStorageBackend();
  const raw = storage.getItem(storageKey);
  if (!raw) return fallback;

  if (!hasEncryptedMessages(security)) {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (isEnvelope(parsed)) {
        const shouldClear = options.clearOnFailure ?? hasTemporaryEncryptedMessages(security);
        if (shouldClear) {
          storage.removeItem(storageKey);
        }
        return fallback;
      }
      return parsed as T;
    } catch {
      return fallback;
    }
  }

  try {
    return await decryptJson<T>(`mq-bridge-app:localStorage:${storageKey}`, raw, security);
  } catch {
    const shouldClear = options.clearOnFailure ?? hasTemporaryEncryptedMessages(security);
    if (shouldClear) {
      storage.removeItem(storageKey);
    }
    return fallback;
  }
}

export async function setStoredJson(storageKey: string, value: unknown, security: StorageSecurityInfo) {
  const storage = getStorageBackend();
  if (!hasEncryptedMessages(security)) {
    storage.setItem(storageKey, JSON.stringify(value));
    return;
  }
  const encrypted = await encryptJson(`mq-bridge-app:localStorage:${storageKey}`, value, security);
  storage.setItem(storageKey, encrypted);
}

export function removeStoredJson(storageKey: string) {
  getStorageBackend().removeItem(storageKey);
}
