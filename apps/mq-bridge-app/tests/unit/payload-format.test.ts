import { describe, expect, test } from "vitest";
import { formatPayload } from "../../ui/src/lib/payload-format";

describe("payload formatting", () => {
  test("keeps hex-looking text as text in auto mode", () => {
    expect(formatPayload("deadbeef", "auto", "text/plain")).toEqual({
      formatted: "deadbeef",
      language: "text",
      effectiveMode: "text",
    });
  });

  test("detects xml payloads without changing their text", () => {
    expect(formatPayload("<message><text>Hello</text></message>", "auto", "application/xml")).toEqual({
      formatted: "<message><text>Hello</text></message>",
      language: "xml",
      effectiveMode: "xml",
    });
  });

  test("formats json payloads in json mode", () => {
    expect(formatPayload("{\"ok\":true}", "json", "application/json")).toEqual({
      formatted: "{\n  \"ok\": true\n}",
      language: "json",
      effectiveMode: "json",
    });
  });

  test("uses hex preview for explicit hex mode", () => {
    expect(formatPayload("6869", "hex", "text/plain").formatted).toBe(
      "68 69                                            |  hi",
    );
  });

  test("wraps hex preview at 16 bytes per row", () => {
    const bytes = new Uint8Array(Array.from({ length: 20 }, (_, index) => index + 65));

    expect(formatPayload(bytes, "hex", "application/octet-stream").formatted).toBe(
      "41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f 50  |  ABCDEFGHIJKLMNOP\n" +
        "51 52 53 54                                      |  QRST",
    );
  });

  test("uses hex preview for binary bytes in auto mode", () => {
    const result = formatPayload(new Uint8Array([0, 1, 255]), "auto", "application/octet-stream");
    expect(result.effectiveMode).toBe("hex");
    expect(result.formatted).toBe("00 01 ff                                         |  ...");
  });
});
