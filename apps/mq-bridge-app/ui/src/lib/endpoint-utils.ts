import { cloneJson } from "./utils";
import { defaultMetricsMiddleware } from "./routes";
import { KNOWN_ENDPOINT_ROOT_KEYS } from "./endpoint-metadata";

export type EndpointRecord = Record<string, unknown>;

export type DlqMiddleware = {
  dlq: { endpoint: EndpointRecord };
};

export type Middleware = DlqMiddleware | Record<string, unknown>;

const CLIENT_URL_PROTOCOLS: Record<string, string> = {
  http: "http:",
  websocket: "ws:",
  grpc: "grpc:",
};
const PATH_CAPABLE_CLIENT_ENDPOINTS = new Set(["http", "websocket"]);

function unwrapRoot(endpoint: unknown): EndpointRecord | null {
  if (!endpoint || typeof endpoint !== "object" || Array.isArray(endpoint)) {
    return null;
  }

  const endpointRecord = endpoint as EndpointRecord & { root?: unknown };
  if (endpointRecord.root && typeof endpointRecord.root === "object" && !Array.isArray(endpointRecord.root)) {
    return endpointRecord.root as EndpointRecord;
  }

  return endpointRecord;
}

export function createDefaultEndpoint(type: string): EndpointRecord {
  if (type === "static" || type === "ref") {
    return { [type]: "" };
  }
  if (type === "switch") {
    return { switch: { metadata_key: "type", cases: {}, default: { ref: "" } }, middlewares: [] };
  }
  if (type === "fanout") {
    return { fanout: [{ ref: "" }], middlewares: [] };
  }
  return { [type]: {}, middlewares: defaultMetricsMiddleware() };
}

export function getEndpointType(endpoint: EndpointRecord): string {
  return ["ref", "static"].find((key) => key in endpoint)
    || Object.keys(endpoint).find((key) => key !== "middlewares")
    || "http";
}

export function normalizeMiddlewares(
  middlewares: unknown,
  recurse: (endpoint: unknown) => EndpointRecord,
): Middleware[] {
  if (!Array.isArray(middlewares)) {
    return [];
  }

  return middlewares.flatMap((middleware) => {
    if (!middleware || typeof middleware !== "object" || Array.isArray(middleware)) {
      return [];
    }

    const nextMiddleware = cloneJson(middleware as Record<string, unknown>);
    const dlq = nextMiddleware.dlq;
    if (dlq && typeof dlq === "object" && !Array.isArray(dlq)) {
      const dlqRecord = dlq as Record<string, unknown>;
      dlqRecord.endpoint = recurse(dlqRecord.endpoint ?? { ref: "" });
    }

    return [nextMiddleware as Middleware];
  });
}

// The schema form seeds every defaulted field as soon as it renders, so a file or
// object_store endpoint the user never encrypted still carries `cipher` and `key_id`.
// `encryption` is optional, but once present the backend demands its `key` and rejects
// the save, so drop the block again while it holds nothing the user chose.
const SEEDED_ENCRYPTION_DEFAULTS: Record<string, unknown> = {
  cipher: "xchacha20poly1305",
  key_id: "default",
};

function isUnsetEncryption(encryption: unknown): boolean {
  if (!encryption || typeof encryption !== "object" || Array.isArray(encryption)) {
    return false;
  }

  return Object.entries(encryption as EndpointRecord).every(([field, value]) => {
    if (value === null || value === "") return true;
    if (typeof value === "object") return Object.keys(value as object).length === 0;
    return SEEDED_ENCRYPTION_DEFAULTS[field] === value;
  });
}

function dropUnsetEncryption(endpointConfig: unknown): void {
  if (!endpointConfig || typeof endpointConfig !== "object" || Array.isArray(endpointConfig)) {
    return;
  }

  const config = endpointConfig as EndpointRecord;
  if ("encryption" in config && isUnsetEncryption(config.encryption)) {
    delete config.encryption;
  }
}

export function ensureEndpointDefaults(
  endpoint: unknown,
  recurse: (endpoint: unknown) => EndpointRecord,
): EndpointRecord {
  const data = unwrapRoot(endpoint);
  if (!data) {
    return createDefaultEndpoint("http");
  }

  const endpointType = getEndpointType(data);
  const normalized: EndpointRecord = {
    ...createDefaultEndpoint(endpointType),
    ...cloneJson(data),
  };

  if (endpointType === "switch") {
    const nextSwitch = normalized.switch;
    if (nextSwitch && typeof nextSwitch === "object" && !Array.isArray(nextSwitch)) {
      const switchRecord = nextSwitch as Record<string, unknown>;
      const rawWhen = switchRecord.when;
      const predicateMode = Array.isArray(rawWhen) && rawWhen.length > 0;
      // The engine takes `metadata_key` + `cases` or `when`, never both, so
      // filling the value-lookup defaults in would make a predicate switch
      // invalid the moment it is opened.
      if (predicateMode) {
        switchRecord.when = (rawWhen as unknown[]).map((entry) => {
          const record = (entry && typeof entry === "object" ? entry : {}) as Record<string, unknown>;
          return { ...record, to: recurse(record.to) };
        });
        delete switchRecord.metadata_key;
        delete switchRecord.cases;
      } else {
        delete switchRecord.when;
        if (!switchRecord.metadata_key) {
          switchRecord.metadata_key = "type";
        }
        const rawCases = switchRecord.cases;
        const normalizedCases: Record<string, EndpointRecord> = {};
        if (rawCases && typeof rawCases === "object" && !Array.isArray(rawCases)) {
          for (const [key, value] of Object.entries(rawCases as Record<string, unknown>)) {
            normalizedCases[key] = recurse(value);
          }
        }
        switchRecord.cases = normalizedCases;
      }
      switchRecord.default = recurse(switchRecord.default ?? { ref: "" });
    }
  } else if (endpointType === "fanout") {
    normalized.fanout = Array.isArray(normalized.fanout)
      ? (normalized.fanout as unknown[]).map((item) => recurse(item))
      : [{ ref: "" }];
  } else if (endpointType === "static" || endpointType === "ref") {
    const value = normalized[endpointType];
    normalized[endpointType] =
      typeof value === "string"
        ? value
        : value && typeof value === "object" && !Array.isArray(value) && typeof (value as Record<string, unknown>)[endpointType] === "string"
          ? String((value as Record<string, unknown>)[endpointType])
          : "";
  }

  dropUnsetEncryption(normalized[endpointType]);
  normalized.middlewares = normalizeMiddlewares(normalized.middlewares, recurse);
  return normalized;
}

export function ensureRefOnlyEndpointDefaults(endpoint: unknown): EndpointRecord {
  const data = unwrapRoot(endpoint);
  if (typeof endpoint === "string") {
    return { ref: endpoint, middlewares: [] };
  }
  if (!data) {
    return { ref: "", middlewares: [] };
  }

  return {
    ref: typeof data.ref === "string" ? data.ref : "",
    middlewares: normalizeMiddlewares(data.middlewares, ensureRefOnlyEndpointDefaults),
  };
}

export function normalizeScalarEndpointValue(endpointType: string, value: unknown): unknown {
  if (endpointType !== "static" && endpointType !== "ref") {
    return value;
  }
  if (value && typeof value === "object" && !Array.isArray(value) && typeof (value as Record<string, unknown>)[endpointType] === "string") {
    return (value as Record<string, unknown>)[endpointType];
  }
  return typeof value === "string" ? value : "";
}

export function prunePolymorphicEndpointKeys(normalized: EndpointRecord, endpointType: string): void {
  for (const key of KNOWN_ENDPOINT_ROOT_KEYS) {
    if (key !== endpointType && key in normalized) {
      delete normalized[key];
    }
  }
}

function splitEndpointUrl(rawUrl: unknown, defaultProtocol: string) {
  const value = String(rawUrl || "").trim();
  if (!value) return null;

  try {
    const parsed = new URL(value.includes("://") ? value : `${defaultProtocol}//${value}`);
    const path = `${parsed.pathname || "/"}${parsed.search || ""}`;
    return {
      host: parsed.host,
      fullUrl: `${parsed.protocol}//${parsed.host}${path === "/" ? "" : path}`,
      path: path && path !== "/" ? path : "",
    };
  } catch {
    const [host, ...pathParts] = value.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "").split("/");
    const path = pathParts.length > 0 ? `/${pathParts.join("/")}` : "";
    return {
      host,
      fullUrl: `${defaultProtocol}//${host}${path}`,
      path,
    };
  }
}

function normalizeListenHost(host: string) {
  if (host === "localhost") return "127.0.0.1";
  if (host.startsWith("localhost:")) return `127.0.0.1:${host.slice("localhost:".length)}`;
  return host;
}

function getMutableClientEndpointParts(endpoint: EndpointRecord) {
  const nextEndpoint = cloneJson(endpoint || {});
  const endpointType = getEndpointType(nextEndpoint);
  const protocol = CLIENT_URL_PROTOCOLS[endpointType];
  const endpointConfig = nextEndpoint[endpointType];
  if (!protocol || !endpointConfig || typeof endpointConfig !== "object" || Array.isArray(endpointConfig)) {
    return null;
  }

  const config = endpointConfig as Record<string, unknown>;
  const parsed = splitEndpointUrl(config.url, protocol);
  if (!parsed?.host) return null;

  return { nextEndpoint, endpointType, protocol, config, parsed };
}

export function createConsumerEndpointFromPublisherEndpoint(endpoint: EndpointRecord): EndpointRecord {
  const parts = getMutableClientEndpointParts(endpoint);
  if (!parts) return cloneJson(endpoint || {});

  const { nextEndpoint, endpointType, config, parsed } = parts;
  config.url = normalizeListenHost(parsed.host);
  if (PATH_CAPABLE_CLIENT_ENDPOINTS.has(endpointType)) {
    const path = parsed.path || (typeof config.path === "string" ? config.path : "");
    if (path && path !== "/") {
      config.path = path;
    } else {
      delete config.path;
    }
  }

  return nextEndpoint;
}

export function createPublisherEndpointFromConsumerEndpoint(endpoint: EndpointRecord): EndpointRecord {
  const parts = getMutableClientEndpointParts(endpoint);
  if (!parts) return cloneJson(endpoint || {});

  const { nextEndpoint, endpointType, protocol, config, parsed } = parts;
  const path = PATH_CAPABLE_CLIENT_ENDPOINTS.has(endpointType) && typeof config.path === "string"
    ? config.path.trim()
    : parsed.path;
  config.url = `${protocol}//${parsed.host}${path && path !== "/" ? path.startsWith("/") ? path : `/${path}` : ""}`;
  if (PATH_CAPABLE_CLIENT_ENDPOINTS.has(endpointType)) {
    delete config.path;
  }

  return nextEndpoint;
}
