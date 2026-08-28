import type { ConfigSecurity as GeneratedConfigSecurity } from "./generated/ui-types";
import { createLocalEntityId } from "./utils";

export type HeaderRow = { key: string; value: string; enabled: boolean };
export type HistoryMetadataRow = { k: string; v: string };
export type ConfigSecurity = GeneratedConfigSecurity;

export type PublisherPreset = {
  name: string;
  payload: string;
  headers: HeaderRow[];
  group?: string;
  endpoint_type?: string;
  method?: string;
  url?: string;
  request_fields?: Record<string, string>;
};

export type PresetsByPublisher = Record<string, PublisherPreset[]>;
export type EnvVars = Record<string, string>;
export type PublisherHistoryEntry = {
  publisher_id?: string;
  name: string;
  payload: string;
  headers: HeaderRow[];
  metadata: HistoryMetadataRow[];
  endpoint_type?: string;
  method?: string;
  url?: string;
  request_fields: Record<string, string>;
  requestMetadata: Record<string, string>;
  targetLabel?: string;
  responseData?: unknown;
  ok?: boolean;
  status: number;
  statusText?: string;
  displayStatus?: string;
  displayStatusText?: string;
  status_info?: Record<string, unknown>;
  duration: number;
  time: number;
  pinned?: boolean;
};

export type PublisherHistoryByPublisher = Record<string, PublisherHistoryEntry[]>;
export type PublisherHistoryStore = {
  version: number;
  updated_at: number;
  publishers: PublisherHistoryByPublisher;
};

export type WorkspaceConfig = Record<string, unknown> & {
  presets?: PresetsByPublisher;
  env_vars?: EnvVars;
  history?: PublisherHistoryStore;
  config_security?: ConfigSecurity | null;
  extract_secrets?: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function getRawPublisherStorageKey(value: unknown) {
  const entry = isRecord(value) ? value : {};
  return String(entry.id ?? entry.name ?? "").trim();
}

function getRawEndpointType(endpoint: unknown) {
  if (!isRecord(endpoint)) return "http";
  const data = isRecord(endpoint.root) ? endpoint.root : endpoint;
  const endpointType = Object.keys(data).find((key) => key !== "middlewares");
  return endpointType || "http";
}

function createDefaultRawPublisherEndpoint(endpointType: string) {
  const base: Record<string, unknown> = {
    middlewares: [],
  };

  const defaults: Record<string, unknown> = {
    http: {
      url: "http://localhost:8080",
      path: "/",
      method: "POST",
      tls: {
        required: false,
        accept_invalid_certs: false,
      },
      fire_and_forget: false,
      compression_enabled: false,
      basic_auth: ["", ""],
      custom_headers: {},
    },
    grpc: { url: "http://localhost:50051" },
    nats: { url: "nats://localhost:4222", subject: "events.created" },
    memory: { topic: "events" },
    webservice: { url: "ws://localhost:8070" },
    amqp: { url: "amqp://guest:guest@localhost:5672/%2f", queue: "jobs" },
    kafka: { url: "localhost:9092", topic: "events" },
    pulsar: { url: "pulsar://localhost:6650", topic: "persistent://public/default/events" },
    sqlx: { url: "postgres://postgres:password@localhost/postgres", table: "events" },
    mqtt: { url: "tcp://localhost:1883", topic: "events/updates" },
    mongodb: { url: "mongodb://localhost:27017", database: "app", collection: "messages" },
    zeromq: { url: "tcp://127.0.0.1:5555", topic: "events" },
    file: { path: "/tmp/messages.jsonl" },
    sled: { path: "./data/sled", tree: "default" },
    ibmmq: { url: "localhost(1414)", queue: "DEV.QUEUE.1", topic: "topic://events" },
    redis_streams: { url: "redis://localhost:6379", stream: "events" },
  };

  return {
    ...base,
    [endpointType]: structuredClone(defaults[endpointType] || {}),
  };
}

function sanitizeHeaderRows(value: unknown): HeaderRow[] {
  const headersValue = Array.isArray(value) ? value : [];
  return headersValue.map((headerRow) => {
    const header = isRecord(headerRow) ? headerRow : {};
    return {
      key: String(header.key ?? ""),
      value: String(header.value ?? ""),
      enabled: header.enabled !== false,
    };
  });
}

function sanitizeHistoryMetadataRows(value: unknown): HistoryMetadataRow[] {
  const rows = Array.isArray(value) ? value : [];
  return rows.map((row) => {
    const entry = isRecord(row) ? row : {};
    return {
      k: String(entry.k ?? entry.key ?? ""),
      v: String(entry.v ?? entry.value ?? ""),
    };
  });
}

function sanitizeStringMap(value: unknown): Record<string, string> {
  const result: Record<string, string> = {};
  if (!isRecord(value)) return result;
  for (const [key, raw] of Object.entries(value)) {
    result[String(key)] = String(raw ?? "");
  }
  return result;
}

function sanitizeRequestDetails(entry: Record<string, unknown>) {
  const request_fields = sanitizeStringMap(entry.request_fields);
  const endpoint_type = entry.endpoint_type ? String(entry.endpoint_type) : undefined;
  const method = entry.method ? String(entry.method).toUpperCase() : undefined;
  const url = entry.url ? String(entry.url) : undefined;
  if (url && !request_fields.url) {
    request_fields.url = url;
  }
  return { request_fields, endpoint_type, method, url };
}

function sanitizeRowsByPublisher<T>(
  value: unknown,
  mapRow: (row: unknown, index: number, publisherName: string) => T,
): Record<string, T[]> {
  const result: Record<string, T[]> = {};
  if (!isRecord(value)) return result;
  for (const [publisherName, rows] of Object.entries(value)) {
    if (!Array.isArray(rows)) continue;
    result[publisherName] = rows.map((row, index) => mapRow(row, index, publisherName));
  }
  return result;
}

export function sanitizePresets(value: unknown): PresetsByPublisher {
  return sanitizeRowsByPublisher(value, (row, index) => {
    const entry = isRecord(row) ? row : {};
    const { request_fields, endpoint_type, method, url } = sanitizeRequestDetails(entry);
    return {
      name: String(entry.name || `Imported preset ${index + 1}`),
      payload: String(entry.payload || ""),
      headers: sanitizeHeaderRows(entry.headers),
      group: entry.group ? String(entry.group) : undefined,
      endpoint_type,
      method,
      url,
      request_fields,
    };
  });
}

function sanitizeHistoryEntry(value: unknown, fallbackName = "", index = 0): PublisherHistoryEntry {
  const entry = isRecord(value) ? value : {};
  const headers = sanitizeHeaderRows(entry.headers);
  const metadata = sanitizeHistoryMetadataRows(entry.metadata);
  const mergedMetadata = metadata.length > 0
    ? metadata
    : headers.map((header) => ({ k: header.key, v: header.value }));
  const mergedHeaders = headers.length > 0
    ? headers
    : mergedMetadata.map((row) => ({ key: row.k, value: row.v, enabled: true }));
  const { request_fields, endpoint_type, method, url } = sanitizeRequestDetails(entry);
  const requestMetadata = sanitizeStringMap(entry.requestMetadata);
  const status = Number.isFinite(entry.status) ? Number(entry.status) : 0;
  const duration = Number.isFinite(entry.duration) ? Number(entry.duration) : 0;
  const time = Number.isFinite(entry.time) ? Number(entry.time) : index;
  const statusInfo = isRecord(entry.status_info)
    ? { ...entry.status_info }
    : {
      ok: entry.ok,
      code: status,
      label: entry.displayStatus || status,
      text: entry.displayStatusText || entry.statusText || "",
    };

  return {
    publisher_id: entry.publisher_id ? String(entry.publisher_id) : undefined,
    name: String(entry.name || fallbackName || `History entry ${index + 1}`),
    payload: String(entry.payload || ""),
    headers: mergedHeaders,
    metadata: mergedMetadata,
    endpoint_type,
    method,
    url,
    request_fields,
    requestMetadata,
    targetLabel: entry.targetLabel ? String(entry.targetLabel) : undefined,
    responseData: entry.responseData,
    ok: typeof entry.ok === "boolean" ? entry.ok : undefined,
    status,
    statusText: entry.statusText ? String(entry.statusText) : undefined,
    displayStatus: entry.displayStatus ? String(entry.displayStatus) : undefined,
    displayStatusText: entry.displayStatusText ? String(entry.displayStatusText) : undefined,
    status_info: statusInfo,
    duration,
    time,
    pinned: entry.pinned === true,
  };
}

function sanitizeHistoryPublishers(value: unknown): PublisherHistoryByPublisher {
  return sanitizeRowsByPublisher(value, (row, index, publisherName) =>
    sanitizeHistoryEntry(row, publisherName, index));
}

export function sanitizePublisherHistory(value: unknown): PublisherHistoryStore {
  if (Array.isArray(value)) {
    const publishers: PublisherHistoryByPublisher = {};
    value.forEach((row, index) => {
      const entry = sanitizeHistoryEntry(row, "", index);
      if (!publishers[entry.name]) {
        publishers[entry.name] = [];
      }
      publishers[entry.name].push(entry);
    });
    return { version: 1, updated_at: 0, publishers };
  }

  const entry = isRecord(value) ? value : {};
  return {
    version: Number.isFinite(entry.version) ? Number(entry.version) : 1,
    updated_at: Number.isFinite(entry.updated_at) ? Number(entry.updated_at) : 0,
    publishers: entry.publishers ? sanitizeHistoryPublishers(entry.publishers) : {},
  };
}

export function sanitizeEnvVars(value: unknown): EnvVars {
  const result: EnvVars = {};
  if (!isRecord(value)) return result;
  for (const [key, raw] of Object.entries(value)) {
    result[String(key)] = typeof raw === "string" ? raw : JSON.stringify(raw ?? "");
  }
  return result;
}

export function sanitizeConfigSecurity(value: unknown): ConfigSecurity {
  const entry = isRecord(value) ? value : {};
  const mode = entry.mode;
  return {
    mode:
      mode === "unencrypted"
      || mode === "balanced"
      || mode === "env_temporary_messages"
      || mode === "temporary_messages"
      || mode === "sensitive"
      || mode === "durable"
        ? mode
        : "balanced",
  };
}

function applyLegacyRequestToRawPublisher(
  publisher: Record<string, unknown>,
  preset: PublisherPreset,
) {
  const endpointType = preset.endpoint_type || getRawEndpointType(publisher.endpoint);
  const endpoint = isRecord(publisher.endpoint)
    ? structuredClone(publisher.endpoint)
    : createDefaultRawPublisherEndpoint(endpointType);
  const endpointConfig = isRecord(endpoint[endpointType]) ? structuredClone(endpoint[endpointType]) : {};
  const requestFields = { ...(preset.request_fields || {}) };
  if (preset.url && !requestFields.url) {
    requestFields.url = preset.url;
  }

  if (endpointType === "http") {
    const rawUrl = String(requestFields.url || "").trim();
    if (rawUrl) {
      try {
        const parsed = new URL(rawUrl);
        endpointConfig.url = parsed.origin;
        endpointConfig.path = `${parsed.pathname || "/"}${parsed.search || ""}`;
      } catch {
        const match = rawUrl.match(/^([^/]+)(\/.*)?$/);
        if (match) {
          endpointConfig.url = match[1];
          endpointConfig.path = match[2] || "/";
        } else {
          endpointConfig.url = rawUrl;
        }
      }
    }
    endpointConfig.method = String(preset.method || endpointConfig.method || "POST").toUpperCase();
    endpointConfig.custom_headers = Object.fromEntries(
      (preset.headers || [])
        .filter((row) => row.enabled !== false && String(row.key || "").trim().length > 0)
        .map((row) => [String(row.key).trim(), String(row.value || "")]),
    );
  } else {
    Object.entries(requestFields).forEach(([key, value]) => {
      endpointConfig[key] = String(value || "");
    });
  }

  endpoint[endpointType] = endpointConfig;
  publisher.endpoint = endpoint;
  publisher.payload = String(preset.payload || "");
  publisher.headers = (preset.headers || []).map((header) => ({
    key: String(header.key || ""),
    value: String(header.value || ""),
    enabled: header.enabled !== false,
  }));
}

function nextUniquePublisherName(base: string, existing: Set<string>) {
  const normalizedBase = base.trim() || "publisher";
  if (!existing.has(normalizedBase)) {
    existing.add(normalizedBase);
    return normalizedBase;
  }

  let index = 1;
  while (existing.has(`${normalizedBase} ${index}`)) {
    index += 1;
  }
  const nextName = `${normalizedBase} ${index}`;
  existing.add(nextName);
  return nextName;
}

function migrateLegacyPresetsToPublishers(config: WorkspaceConfig) {
  const legacyPresets = sanitizePresets(config.presets);
  if (Object.keys(legacyPresets).length === 0) {
    return;
  }

  const publishers = Array.isArray(config.publishers)
    ? (config.publishers.filter((row) => row && typeof row === "object") as Array<Record<string, unknown>>)
    : [];
  const migratedPublishers = [...publishers];
  const existingNames = new Set(
    migratedPublishers
      .map((publisher) => String(publisher.name || "").trim())
      .filter((name) => name.length > 0),
  );

  for (const [publisherKey, rows] of Object.entries(legacyPresets)) {
    const basePublisher = migratedPublishers.find((publisher) =>
      getRawPublisherStorageKey(publisher) === publisherKey
      || String(publisher.name || "").trim() === String(publisherKey || "").trim(),
    );
    for (const preset of rows) {
      const fallbackType = preset.endpoint_type || "http";
      const nextPublisher = basePublisher
        ? structuredClone(basePublisher)
        : {
            id: createLocalEntityId("publisher"),
            name: publisherKey || preset.name || fallbackType,
            endpoint: createDefaultRawPublisherEndpoint(fallbackType),
            comment: "",
          };

      nextPublisher.id = createLocalEntityId("publisher");
      nextPublisher.name = nextUniquePublisherName(
        basePublisher
          ? `${String(basePublisher.name || publisherKey || fallbackType).trim()} - ${String(preset.name || fallbackType).trim()}`
          : String(preset.name || publisherKey || fallbackType).trim(),
        existingNames,
      );
      applyLegacyRequestToRawPublisher(nextPublisher, preset);
      migratedPublishers.push(nextPublisher);
    }
  }

  config.publishers = migratedPublishers as unknown as WorkspaceConfig["publishers"];
  config.presets = {};
}

function migrateConfigSecurity(config: WorkspaceConfig): ConfigSecurity {
  if (isRecord(config.config_security)) {
    return sanitizeConfigSecurity(config.config_security);
  }
  if (typeof config.extract_secrets === "boolean") {
    return { mode: config.extract_secrets ? "balanced" : "unencrypted" };
  }
  return sanitizeConfigSecurity(config.config_security);
}

export function isSensitiveConfig(config: Pick<WorkspaceConfig, "config_security"> | Record<string, unknown>) {
  const mode = sanitizeConfigSecurity((config as WorkspaceConfig).config_security).mode;
  return mode === "sensitive" || mode === "durable";
}

export function ensureWorkspaceCollections<T extends WorkspaceConfig>(config: T): T & {
  presets: PresetsByPublisher;
  env_vars: EnvVars;
  history: PublisherHistoryStore;
  config_security: ConfigSecurity;
} {
  migrateLegacyPresetsToPublishers(config);
  config.presets = sanitizePresets(config.presets);
  config.env_vars = sanitizeEnvVars(config.env_vars);
  config.history = sanitizePublisherHistory(config.history);
  config.config_security = migrateConfigSecurity(config);
  return config as T & {
    presets: PresetsByPublisher;
    env_vars: EnvVars;
    history: PublisherHistoryStore;
    config_security: ConfigSecurity;
  };
}
