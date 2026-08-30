export interface RouteMiddleware {
  metrics?: Record<string, never>;
  [key: string]: unknown;
}

export interface RouteEndpoint {
  middlewares?: RouteMiddleware[];
  ref?: string;
  [key: string]: unknown;
}

export interface RouteDefinition {
  id?: string;
  enabled: boolean;
  input: RouteEndpoint;
  output: RouteEndpoint;
  description?: string;
  concurrency?: number;
  batch_size?: number;
  commit_concurrency_limit?: number;
  [key: string]: unknown;
}

export interface RouteSchema {
  $defs?: Record<string, { properties?: Record<string, Record<string, unknown>> }>;
  properties?: Record<string, unknown>;
  required?: string[];
  [key: string]: unknown;
}

export interface SplitRouteFormDataResult {
  nextName: string;
  routeData: Record<string, unknown>;
}

export function defaultMetricsMiddleware(): Array<{ metrics: Record<string, never> }> {
  return [];
}

export function isRouteEnabled(route: Partial<RouteDefinition> | undefined): boolean {
  return route?.enabled !== false;
}

export function hasMetricsMiddleware(route: { input?: RouteEndpoint; output?: RouteEndpoint } | undefined): boolean {
  const hasMetrics = (endpoint: RouteEndpoint | undefined) =>
    Array.isArray(endpoint?.middlewares)
    && endpoint.middlewares.some((middleware) => Boolean(middleware?.metrics));
  return hasMetrics(route?.input) || hasMetrics(route?.output);
}

export function formatThroughput(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0 msg/s";
  if (value < 10) return `${value.toFixed(2)} msg/s`;
  if (value < 100) return `${value.toFixed(1)} msg/s`;
  return `${Math.round(value)} msg/s`;
}

function isNullableEndpointProperty(property: unknown): boolean {
  if (!property || typeof property !== "object") return false;
  const branches = (property as Record<string, unknown>).anyOf || (property as Record<string, unknown>).oneOf;
  if (!Array.isArray(branches)) return false;
  const refsEndpoint = branches.some((branch) =>
    Boolean(branch) && typeof branch === "object" && (branch as { $ref?: string }).$ref === "#/$defs/Endpoint");
  const allowsNull = branches.some((branch) =>
    Boolean(branch) && typeof branch === "object" && (branch as { type?: string }).type === "null");
  return refsEndpoint && allowsNull;
}

// Optional nested endpoints (e.g. HttpConfig.stream_response_to) have no editor in this UI, and
// the schema form library turns the `null` branch into the literal string "null" while building
// its data - which made every form load report unsaved changes. Drop them from the form schema.
function dropNullableEndpointProperties(routeSchema: RouteSchema): void {
  for (const definition of Object.values(routeSchema.$defs || {})) {
    const properties = definition?.properties;
    if (!properties) continue;
    for (const [name, property] of Object.entries(properties)) {
      if (isNullableEndpointProperty(property)) delete properties[name];
    }
  }
}

export function applyEndpointSchemaDefaults(routeSchema: RouteSchema): void {
  dropNullableEndpointProperties(routeSchema);

  const fileConfigSchema = routeSchema.$defs?.FileConfig;
  if (fileConfigSchema?.properties?.format) {
    fileConfigSchema.properties.format.default = "raw";
  }

  const mongoDbConfigSchema = routeSchema.$defs?.MongoDbConfig;
  if (mongoDbConfigSchema?.properties?.format) {
    mongoDbConfigSchema.properties.format.default = "raw";
  }
  // Non-destructive default: `capture_all` (read existing docs, then watch) so a
  // MongoDB source never mutates the collection unless the user opts into a queue
  // mode. This matches the library default since 0.4.0 and is pinned here so a
  // future library change cannot make a UI-built source destructive.
  if (mongoDbConfigSchema?.properties?.consume) {
    mongoDbConfigSchema.properties.consume.default = "capture_all";
  }

  const properties = routeSchema.properties;
  if (properties && typeof properties === "object") {
    delete properties.id;
    delete properties.enabled;
    delete properties.description;
  }

  if (Array.isArray(routeSchema.required)) {
    routeSchema.required = routeSchema.required.filter(
      (key) => key !== "id" && key !== "enabled" && key !== "description",
    );
  }
}

export function nextUniqueName(baseName: string, existingNames: Iterable<string>): string {
  const names = new Set(existingNames);
  let candidate = baseName;
  let index = 1;
  while (names.has(candidate)) {
    candidate = `${baseName}_${index}`;
    index += 1;
  }
  return candidate;
}

export function createRefInputEndpoint(refName: string): RouteEndpoint {
  return {
    middlewares: defaultMetricsMiddleware(),
    ref: refName,
  };
}

export function splitRouteFormData(
  currentName: string,
  formData: Record<string, unknown>,
): SplitRouteFormDataResult {
  const { name, ...routeData } = formData;
  const nextName = String(name || "").trim() || currentName;
  return { nextName, routeData };
}
