import { fromHexString, stringToUint8ArrayLatin1, toHexString, uint8ArrayToString } from "./utils";

export type PayloadViewMode = "auto" | "text" | "json" | "xml" | "hex";
export type PayloadEditorLanguage = "text" | "json" | "json-auto" | "xml";

export type PayloadFormatResult = {
  formatted: string;
  language: PayloadEditorLanguage;
  effectiveMode: Exclude<PayloadViewMode, "auto">;
};

function isValidJson(value: string): boolean {
  const trimmed = value.trim();
  if (!((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]")))) {
    return false;
  }
  try {
    JSON.parse(trimmed);
    return true;
  } catch {
    return false;
  }
}

function isLikelyXml(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed.startsWith("<") || !trimmed.endsWith(">")) return false;
  if (/^<\?xml[\s?>]/i.test(trimmed)) return true;
  return /^<[A-Za-z_][\w:.-]*(\s[^>]*)?>[\s\S]*<\/[A-Za-z_][\w:.-]*>$/.test(trimmed) || /^<[A-Za-z_][\w:.-]*(\s[^>]*)?\/>$/.test(trimmed);
}

function isBinaryString(value: string): boolean {
  if (!value) return false;
  for (let index = 0; index < value.length; index += 1) {
    const charCode = value.charCodeAt(index);
    const allowedControl = charCode === 9 || charCode === 10 || charCode === 13;
    if ((charCode < 32 && !allowedControl) || charCode === 127) return true;
  }
  return false;
}

function isCompleteHex(value: string): boolean {
  const cleaned = value.replace(/\s/g, "");
  return cleaned.length > 0 && cleaned.length % 2 === 0 && /^[0-9a-fA-F]+$/.test(cleaned);
}

function toPayloadString(rawPayload: string | Uint8Array | null | undefined) {
  if (rawPayload === null || rawPayload === undefined) return { text: "", binary: false, bytes: new Uint8Array(0) };
  if (!(rawPayload instanceof Uint8Array)) {
    return { text: rawPayload, binary: isBinaryString(rawPayload), bytes: stringToUint8ArrayLatin1(rawPayload) };
  }

  const text = uint8ArrayToString(rawPayload);
  const binary = rawPayload.length > 0 && text === "[BINARY DATA]";
  return { text: binary ? "" : text, binary, bytes: rawPayload };
}

export function toHexPreview(bytes: Uint8Array, bytesPerLine = 16): string {
  const rowWidth = Math.max(1, bytesPerLine);
  const rows: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += rowWidth) {
    const row = bytes.slice(offset, offset + rowWidth);
    const hex = toHexString(row).padEnd(rowWidth * 3 - 1, " ");
    const ascii = Array.from(row)
      .map((byte) => (byte >= 32 && byte <= 126 ? String.fromCharCode(byte) : "."))
      .join("");
    rows.push(`${hex}  |  ${ascii}`);
  }
  return rows.join("\n");
}

export function formatPayload(
  rawPayload: string | Uint8Array | null | undefined,
  mode: PayloadViewMode,
  contentType: string,
): PayloadFormatResult {
  const { text, binary, bytes } = toPayloadString(rawPayload);
  const normalizedContentType = contentType.toLowerCase();

  let effectiveMode: Exclude<PayloadViewMode, "auto">;
  if (mode === "auto") {
    if (normalizedContentType.includes("json") && isValidJson(text)) {
      effectiveMode = "json";
    } else if (normalizedContentType.includes("xml") && isLikelyXml(text)) {
      effectiveMode = "xml";
    } else if (isValidJson(text)) {
      effectiveMode = "json";
    } else if (isLikelyXml(text)) {
      effectiveMode = "xml";
    } else if (binary) {
      effectiveMode = "hex";
    } else {
      effectiveMode = "text";
    }
  } else {
    effectiveMode = mode;
  }

  if (effectiveMode === "json") {
    try {
      return {
        formatted: JSON.stringify(JSON.parse(text), null, 2),
        language: "json",
        effectiveMode,
      };
    } catch {
      return { formatted: text, language: "text", effectiveMode };
    }
  }

  if (effectiveMode === "hex") {
    const hexBytes = typeof rawPayload === "string" && isCompleteHex(rawPayload) ? fromHexString(rawPayload) : bytes;
    return { formatted: toHexPreview(hexBytes), language: "text", effectiveMode };
  }

  if (effectiveMode === "xml") {
    return { formatted: text, language: "xml", effectiveMode };
  }

  return { formatted: text, language: "text", effectiveMode };
}
