import { nextUniqueName } from "./routes";
import {
  BASIC_ENDPOINT_FIELDS,
  KNOWN_ENDPOINT_ROOT_KEYS,
  formatEndpointTypeLabel,
} from "./endpoint-metadata";

export type ThemePreference = "auto" | "light" | "dark";

export type NamedEntity = {
  id?: string | null;
  name?: string | null;
};

function getEndpointType(endpoint: Record<string, unknown>): string {
  return Object.keys(endpoint).find((key) => key !== "middlewares" && (KNOWN_ENDPOINT_ROOT_KEYS as readonly string[]).includes(key)) || "http";
}

export function cloneJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export function normalizeNamedEntityFormShape<T extends { id?: string; name?: string }>(
  entity: T,
  idPrefix: string,
): T {
  let data = { ...entity } as any;
  if (data.root) {
    const { root, ...rest } = data;
    data = { ...rest, ...root };
  }
  data.id = String(data.id || "").trim() || createLocalEntityId(idPrefix);
  data.name = String(data.name ?? "");
  return data as T;
}

export function getThemePreference(): ThemePreference | undefined {
  const value = (window as Window & { getThemePreference?: () => unknown }).getThemePreference?.();
  if (value === "auto" || value === "light" || value === "dark") {
    return value;
  }
  return undefined;
}

export function setThemePreference(value: ThemePreference) {
  (window as Window & { setThemePreference?: (value: ThemePreference) => void }).setThemePreference?.(value);
}

export function getLabel(node: any) {
  if (!node) return "";
  if (node.item && typeof node.label === "string" && node.label.trim()) return node.label;
  if (typeof node.label === "string" && node.label.trim()) return node.label;

  const entity = node.publisher || node.item || node;
  const name = entity.name || entity.displayName || "";
  const endpoint = entity.endpoint || {};
  const type = entity.endpointType || entity.inputProto || entity.proto || "";

  if (!name && Object.keys(endpoint).length === 0 && !type) return "Unnamed Entity";

  return getEntityDisplayLabel(name, endpoint, type);
}

export function sanitizeConsumerName(name: string): string {
  const asciiName = String(name || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
  const normalized = asciiName
    .replace(/\s+/g, "_")
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^[_\-.]+|[_\-.]+$/g, "");
  return normalized || "consumer";
}

export function normalizeConsumerNames<T extends { name: string }>(consumers: T[], selectedIndex: number | null = null) {
  const usedNames = new Set<string>();
  let changed = false;

  for (const consumer of consumers) {
    const nextName = nextUniqueName(sanitizeConsumerName(consumer.name), usedNames);
    if (consumer.name !== nextName) {
      consumer.name = nextName;
      changed = true;
    }
    usedNames.add(nextName);
  }

  return {
    changed,
    selectedName: selectedIndex === null ? null : consumers[selectedIndex]?.name || null,
  };
}

export function normalizeConsumerResponse(response: unknown): { headers: Record<string, string>; payload: string } | null {
  if (!response || typeof response !== "object") return null;

  const rawHeaders = (response as { headers?: unknown }).headers;
  const isPlainHeadersObject =
    rawHeaders !== null && typeof rawHeaders === "object" && !Array.isArray(rawHeaders);
  const headers = Object.fromEntries(
    Object.entries(isPlainHeadersObject ? (rawHeaders as Record<string, unknown>) : {})
      .map(([key, value]) => [String(key).trim(), String(value).trim()])
      .filter(([key, value]) => key && value),
  );
  const payload = typeof (response as { payload?: unknown }).payload === "string"
    ? (response as { payload: string }).payload
    : "";

  return Object.keys(headers).length > 0 || payload.trim() ? { headers, payload } : null;
}

function sanitizeUrlHost(rawUrl: string) {
  const value = String(rawUrl || "").trim();
  if (!value) return "";
  try {
    const parsed = new URL(value);
    parsed.username = "";
    parsed.password = "";
    return parsed.host;
  } catch {
    return value.replace(/^[a-zA-Z][a-zA-Z\d+\-.]*:\/\//, "");
  }
}

function sanitizeUrlTarget(rawUrl: string, includePath: boolean) {
  const value = String(rawUrl || "").trim();
  if (!value) return "";
  const host = sanitizeUrlHost(value);
  if (!includePath) return host;
  try {
    const parsed = new URL(value);
    const path = `${parsed.pathname || ""}${parsed.search || ""}`;
    return `${host}${path && path !== "/" ? path : ""}`.trim();
  } catch {
    return host;
  }
}

function normalizePathValue(rawPath: string) {
  const value = String(rawPath || "").trim();
  if (!value) return "";
  return value.startsWith("/") ? value : `/${value}`;
}

// Credentials may appear in an endpoint's basic fields (so they render directly in
// the form) but must never leak into the read-only summary/upper bar.
const SUMMARY_SECRET_FIELDS = new Set([
  "username",
  "password",
  "access_key",
  "secret_key",
  "secret_access_key",
  "key_repository_password",
  "cert_password",
]);

export function getTechnicalDisplayLabel(
  endpoint: Record<string, unknown>,
  endpointType?: string,
) {
  if (!endpoint) return endpointType ? formatEndpointTypeLabel(endpointType) : "";
  const type = String(endpointType || getEndpointType(endpoint)).trim().toLowerCase();
  const endpointData = (endpoint as Record<string, unknown>)[type];
  const data = endpointData && typeof endpointData === "object" && !Array.isArray(endpointData)
    ? endpointData as Record<string, unknown>
    : endpoint;
  const fields = (BASIC_ENDPOINT_FIELDS[type] || []).filter((field) => !SUMMARY_SECRET_FIELDS.has(field));

  const values = fields.map((field) => {
    const rawValue = data[field];
    if (field === "url") {
      return sanitizeUrlTarget(String(rawValue || ""), !String(data.path || "").trim());
    }
    if (field === "path") {
      return normalizePathValue(String(rawValue || ""));
    }
    return String(rawValue || "").trim();
  }).filter(Boolean);

  return values.join(" ").trim() || formatEndpointTypeLabel(type);
}

export function getEntityDisplayLabel(
  name: string | undefined,
  endpoint: Record<string, unknown>,
  endpointType?: string,
) {
  const title = String(name || "").trim();
  if (title) return title;

  return getTechnicalDisplayLabel(endpoint, endpointType);
}

export function handleActionKey(event: KeyboardEvent, action: () => void | Promise<void>) {
  // This utility function handles keyboard activation for interactive elements.
  // It prevents the default browser action for 'Enter' or 'Space' keys and then executes the provided action.
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    void action(); // Use void to explicitly ignore the Promise return value if the action is async
  }
}
/**
 * Checks if a string contains only printable ASCII characters (32-126).
 * This is a heuristic to guess if a string might represent binary data.
 * @param str The string to check.
 * @returns True if the string contains only printable ASCII characters, false otherwise.
 */
export function isPrintableAscii(str: string): boolean {
  for (let i = 0; i < str.length; i++) {
    const charCode = str.charCodeAt(i);
    if (charCode < 32 || charCode > 126) {
      return false;
    }
  }
  return true;
}

export function toHexString(byteArray: Uint8Array): string {
  return Array.from(byteArray, (byte) => ('0' + (byte & 0xFF).toString(16)).slice(-2)).join(' ');
}

export function fromHexString(hexString: string): Uint8Array {
  const cleanedHexString = hexString.replace(/\s/g, ''); // Remove spaces for parsing
  const bytes = new Uint8Array(cleanedHexString.length / 2);
  for (let i = 0; i < cleanedHexString.length; i += 2) {
    bytes[i / 2] = parseInt(cleanedHexString.substring(i, i + 2), 16);
  }
  return bytes;
}

export function uint8ArrayToString(arr: Uint8Array): string {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(arr);
  } catch (e) {
    // Not valid UTF-8, return empty string or handle as needed
    return "[BINARY DATA]"; // Indicate it's binary data
  }
}

export function stringToUint8ArrayLatin1(str: string): Uint8Array {
  const bytes = new Uint8Array(str.length);
  for (let i = 0; i < str.length; i++) {
    bytes[i] = str.charCodeAt(i) & 0xFF; // Take the lower 8 bits
  }
  return bytes;
}

export async function withSelectedFileText(
  event: Event,
  onText: (text: string, target: HTMLInputElement) => Promise<void> | void,
) {
  const target = event.currentTarget as HTMLInputElement | null;
  const file = target?.files?.[0];
  if (!target || !file) return;

  try {
    await onText(await file.text(), target);
  } finally {
    target.value = "";
  }
}
export function getEntityStorageKey(entity: NamedEntity | null | undefined): string {
  const trimmedId = String(entity?.id || "").trim();
  const trimmedName = String(entity?.name || "").trim();
  return trimmedId !== "" ? trimmedId : trimmedName;
}

export function createLocalEntityId(prefix = "ui"): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}_${crypto.randomUUID()}`;
  }
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}
