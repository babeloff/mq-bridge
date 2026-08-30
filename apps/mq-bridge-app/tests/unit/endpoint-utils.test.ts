import { describe, expect, test } from "vitest";
import {
  createDefaultEndpoint,
  ensureEndpointDefaults,
  ensureRefOnlyEndpointDefaults,
  getEndpointType,
  normalizeMiddlewares,
} from "../../ui/src/lib/endpoint-utils";

describe("endpoint-utils", () => {
  test("creates basic defaults", () => {
    expect(createDefaultEndpoint("ref")).toEqual({ ref: "" });
    expect(createDefaultEndpoint("switch")).toEqual({
      switch: { metadata_key: "type", cases: {}, default: { ref: "" } },
      middlewares: [],
    });
    expect(createDefaultEndpoint("fanout")).toEqual({
      fanout: [{ ref: "" }],
      middlewares: [],
    });
  });

  test("detects endpoint types with ref priority", () => {
    expect(getEndpointType({ ref: "", http: {} })).toBe("ref");
    expect(getEndpointType({ middlewares: [], mqtt: {} })).toBe("mqtt");
    expect(getEndpointType({ middlewares: [] })).toBe("http");
  });

  test("normalizes dlq middlewares and skips invalid rows", () => {
    const middlewares = normalizeMiddlewares(
      [{ dlq: {} }, null, { metrics: {} }],
      ensureRefOnlyEndpointDefaults,
    );

    expect(middlewares).toEqual([
      { dlq: { endpoint: { ref: "", middlewares: [] } } },
      { metrics: {} },
    ]);
  });

  test("keeps saved fanout entries and normalizes nested refs", () => {
    const normalized = ensureEndpointDefaults(
      {
        fanout: ["one", { ref: "two" }],
        middlewares: [{ dlq: {} }],
      },
      ensureRefOnlyEndpointDefaults,
    );

    expect(normalized).toEqual({
      fanout: [
        { ref: "one", middlewares: [] },
        { ref: "two", middlewares: [] },
      ],
      middlewares: [{ dlq: { endpoint: { ref: "", middlewares: [] } } }],
    });
  });

  test("normalizes switch defaults and scalar refs", () => {
    const normalizedSwitch = ensureEndpointDefaults(
      {
        switch: {
          cases: { ok: "publisher.orders" },
        },
      },
      ensureRefOnlyEndpointDefaults,
    );
    const normalizedRef = ensureEndpointDefaults(
      { ref: { ref: "publisher.orders" } },
      ensureRefOnlyEndpointDefaults,
    );

    expect(normalizedSwitch).toEqual({
      switch: {
        metadata_key: "type",
        cases: { ok: { ref: "publisher.orders", middlewares: [] } },
        default: { ref: "", middlewares: [] },
      },
      middlewares: [],
    });
    expect(normalizedRef).toEqual({ ref: "publisher.orders", middlewares: [] });
  });

  test("leaves a predicate switch in when mode instead of seeding value lookup", () => {
    const normalized = ensureEndpointDefaults(
      {
        switch: {
          when: [{ if: "amount > 100", to: "publisher.large" }],
        },
      },
      ensureRefOnlyEndpointDefaults,
    );

    // Seeding `metadata_key` here would make the engine reject the endpoint for
    // naming both modes at once.
    expect(normalized).toEqual({
      switch: {
        when: [{ if: "amount > 100", to: { ref: "publisher.large", middlewares: [] } }],
        default: { ref: "", middlewares: [] },
      },
      middlewares: [],
    });
  });

  test("clears a leftover empty when list when the switch is in value-lookup mode", () => {
    const normalized = ensureEndpointDefaults(
      {
        switch: {
          metadata_key: "kind",
          cases: { a: "publisher.a" },
          when: [],
        },
      },
      ensureRefOnlyEndpointDefaults,
    );

    // An empty `when` left behind by a mode switch in the form would travel to
    // the engine as a second mode named alongside `metadata_key`.
    expect(normalized).toEqual({
      switch: {
        metadata_key: "kind",
        cases: { a: { ref: "publisher.a", middlewares: [] } },
        default: { ref: "", middlewares: [] },
      },
      middlewares: [],
    });
  });

  test("drops an encryption block that only holds the form's seeded defaults", () => {
    const normalized = ensureEndpointDefaults(
      {
        file: {
          path: "/tmp/out.jsonl",
          encryption: { cipher: "xchacha20poly1305", key_id: "default" },
        },
      },
      ensureRefOnlyEndpointDefaults,
    );

    expect(normalized.file).toEqual({ path: "/tmp/out.jsonl" });
  });

  test("keeps an encryption block the user configured", () => {
    const withKey = ensureEndpointDefaults(
      { file: { path: "/tmp/out.jsonl", encryption: { cipher: "aes256gcm", key: "c2VjcmV0" } } },
      ensureRefOnlyEndpointDefaults,
    );
    const withCustomKeyId = ensureEndpointDefaults(
      { file: { path: "/tmp/out.jsonl", encryption: { key_id: "rotated" } } },
      ensureRefOnlyEndpointDefaults,
    );

    expect(withKey.file).toEqual({
      path: "/tmp/out.jsonl",
      encryption: { cipher: "aes256gcm", key: "c2VjcmV0" },
    });
    expect(withCustomKeyId.file).toEqual({
      path: "/tmp/out.jsonl",
      encryption: { key_id: "rotated" },
    });
  });
});
