/**
 * The backend rejects a bad config with a serde message that points at a byte
 * offset into the payload we posted ("missing field `key` at line 1 column 2603"),
 * which tells the user nothing. These helpers map that offset back onto the JSON
 * body so the dialog names the consumer, publisher or route that needs fixing.
 */

const LOCATION_PATTERN = /\s*at line (\d+) column (\d+)/;
const MISSING_FIELD_PATTERN = /missing field `([^`]+)`/;
const DESERIALIZE_PREFIX_PATTERN = /^Json deserialize error:\s*/i;
// The backend appends a raw payload excerpt for its log; it is noise in a dialog.
const PAYLOAD_EXCERPT_PATTERN = /\s*(?:\r?\n)?At: \.\.\.[\s\S]*$/;

type Frame = { array: boolean; key: string | null; index: number };

function currentSegments(stack: readonly Frame[]): string[] {
  const segments: string[] = [];
  for (const frame of stack) {
    if (frame.array) segments.push(`[${frame.index}]`);
    else if (frame.key !== null) segments.push(frame.key);
  }
  return segments;
}

/**
 * Path of the value the parser is sitting on at `offset`. A closing brace counts
 * as part of the container it closes, which is where serde reports struct errors.
 */
export function jsonPathAtOffset(json: string, offset: number): string[] {
  const stack: Frame[] = [];
  let index = 0;

  while (index < json.length && index <= offset) {
    const character = json[index];

    if (character === '"') {
      const start = index;
      index += 1;
      while (index < json.length && json[index] !== '"') {
        index += json[index] === "\\" ? 2 : 1;
      }
      const literal = json.slice(start, index + 1);
      index += 1;

      let lookahead = index;
      while (lookahead < json.length && /\s/.test(json[lookahead])) lookahead += 1;

      const frame = stack[stack.length - 1];
      if (json[lookahead] === ":" && frame && !frame.array) {
        try {
          frame.key = JSON.parse(literal) as string;
        } catch {
          frame.key = null;
        }
      }
      continue;
    }

    if (character === "{" || character === "[") {
      stack.push({ array: character === "[", key: null, index: 0 });
    } else if (character === "}" || character === "]") {
      // The top frame still points at the member that was just read, so the
      // container being closed is one label short of the running path.
      if (index >= offset) return currentSegments(stack.slice(0, -1));
      stack.pop();
    } else if (character === ",") {
      const frame = stack[stack.length - 1];
      if (frame?.array) frame.index += 1;
      else if (frame) frame.key = null;
    }

    index += 1;
  }

  return currentSegments(stack);
}

function valueAtPath(root: unknown, segments: readonly string[]): unknown {
  return segments.reduce<unknown>((current, segment) => {
    if (current === null || typeof current !== "object") return undefined;
    if (segment.startsWith("[")) return (current as unknown[])[Number(segment.slice(1, -1))];
    return (current as Record<string, unknown>)[segment];
  }, root);
}

// Property-name sets of every schema definition that requires a given field, so a block
// that simply has no such field of its own (a `retry` middleware, say) is not blamed for
// a missing `key`. Empty until the schema is registered, which only turns the filter off.
let requiredFieldOwners = new Map<string, Array<Set<string>>>();

export function registerConfigSchema(schema: unknown): void {
  const owners = new Map<string, Array<Set<string>>>();
  const definitions = (schema as { $defs?: Record<string, unknown> } | null)?.$defs || {};

  for (const definition of Object.values(definitions)) {
    const { required, properties } = (definition || {}) as {
      required?: unknown;
      properties?: Record<string, unknown>;
    };
    if (!Array.isArray(required) || !properties) continue;

    const propertyNames = new Set(Object.keys(properties));
    for (const field of required) {
      const entries = owners.get(String(field)) || [];
      entries.push(propertyNames);
      owners.set(String(field), entries);
    }
  }

  requiredFieldOwners = owners;
}

/** Could this block be one of the schema definitions that requires `field`? */
function couldRequireField(record: Record<string, unknown>, field: string): boolean {
  const owners = requiredFieldOwners.get(field);
  if (!owners) return requiredFieldOwners.size === 0;

  const keys = Object.keys(record);
  // An empty block fits every definition, so it is evidence for none of them.
  if (keys.length === 0) return false;
  return owners.some((propertyNames) => keys.every((key) => propertyNames.has(key)));
}

/**
 * serde reports a missing field wherever it finished reading the buffered value —
 * for an endpoint that is the whole endpoint object. Narrow it down to the deepest
 * objects inside that actually lack the field.
 */
function deepestPathsMissingField(root: unknown, field: string, limit = 2): string[][] {
  const found: string[][] = [];
  let deepest = -1;

  const visit = (value: unknown, path: string[]) => {
    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, [...path, `[${index}]`]));
      return;
    }
    if (value === null || typeof value !== "object") return;

    const record = value as Record<string, unknown>;
    if (!(field in record) && couldRequireField(record, field)) {
      if (path.length > deepest) {
        deepest = path.length;
        found.length = 0;
      }
      if (path.length === deepest) found.push(path);
    }
    for (const [key, child] of Object.entries(record)) visit(child, [...path, key]);
  };

  visit(root, []);
  return found.slice(0, limit);
}

function formatSegments(segments: readonly string[]): string {
  return segments.reduce((joined, segment) => {
    if (segment.startsWith("[")) return `${joined}${segment}`;
    return joined ? `${joined} › ${segment}` : segment;
  }, "");
}

/** Swap the leading `consumers[2]` / `publishers[0]` / `routes.x` for the entry's own name. */
function describeLocation(segments: readonly string[], config: unknown): string {
  if (segments.length === 0) return "the configuration";

  const [section, selector, ...rest] = segments;
  const named = (singular: string, name: string, fallback: string) =>
    formatSegments([name ? `${singular} “${name}”` : fallback, ...rest]);

  if ((section === "consumers" || section === "publishers") && /^\[\d+\]$/.test(selector || "")) {
    const position = Number(selector.slice(1, -1));
    const entry = valueAtPath(config, [section, selector]) as { name?: string; id?: string } | undefined;
    const singular = section === "consumers" ? "consumer" : "publisher";
    return named(singular, String(entry?.name || entry?.id || "").trim(), `${singular} ${position + 1}`);
  }

  if (section === "routes" && selector) {
    return named("route", selector, "route");
  }

  return formatSegments(segments);
}

/**
 * serde counts the column in UTF-8 bytes; JS indexes strings in UTF-16 units. One
 * emoji or accented character anywhere earlier in the payload would otherwise skew
 * the offset and name the wrong field.
 */
function utf16IndexForByteOffset(json: string, byteOffset: number): number {
  if (byteOffset <= 0) return 0;
  let bytes = 0;
  let index = 0;
  while (index < json.length && bytes < byteOffset) {
    const codePoint = json.codePointAt(index) as number;
    bytes += codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
    index += codePoint > 0xffff ? 2 : 1;
  }
  return index;
}

/**
 * Rewrite a failed `POST /config` response into a message that names the offending
 * field. Anything that is not a located serde error is passed through untouched.
 */
export function explainConfigSaveError(rawMessage: string, body: string): string {
  const message = String(rawMessage || "").replace(PAYLOAD_EXCERPT_PATTERN, "").trim();
  const location = message.match(LOCATION_PATTERN);
  if (!location || Number(location[1]) !== 1) return message;

  let config: unknown;
  try {
    config = JSON.parse(body);
  } catch {
    return message;
  }

  const detail = message.replace(LOCATION_PATTERN, "").replace(DESERIALIZE_PREFIX_PATTERN, "").trim();
  const segments = jsonPathAtOffset(body, utf16IndexForByteOffset(body, Number(location[2]) - 1));
  const missingField = detail.match(MISSING_FIELD_PATTERN);

  const paths = missingField
    ? deepestPathsMissingField(valueAtPath(config, segments), missingField[1])
        .map((suffix) => [...segments, ...suffix])
    : [segments];

  const described = (paths.length > 0 ? paths : [segments])
    .map((path) => describeLocation(path, config))
    .join(" or ");

  return `${described}: ${detail}`;
}
