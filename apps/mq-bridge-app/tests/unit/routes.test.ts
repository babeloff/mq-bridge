import { describe, expect, test } from "vitest";
import {
  applyEndpointSchemaDefaults,
  createRefInputEndpoint,
  defaultMetricsMiddleware,
  formatThroughput,
  hasMetricsMiddleware,
  isRouteEnabled,
  nextUniqueName,
  splitRouteFormData,
} from "../../ui/src/lib/routes";

describe("routes helpers", () => {
  test("defaults route enabled state to true unless explicitly disabled", () => {
    expect(isRouteEnabled(undefined)).toBe(true);
    expect(isRouteEnabled({})).toBe(true);
    expect(isRouteEnabled({ enabled: true })).toBe(true);
    expect(isRouteEnabled({ enabled: false })).toBe(false);
  });

  test("detects metrics middleware on either route endpoint", () => {
    expect(
      hasMetricsMiddleware({
        input: { middlewares: [{ metrics: {} }] },
        output: {},
      }),
    ).toBe(true);
    expect(
      hasMetricsMiddleware({
        input: {},
        output: { middlewares: [{ transform: {} }, { metrics: {} }] },
      }),
    ).toBe(true);
    expect(
      hasMetricsMiddleware({
        input: { middlewares: [{ transform: {} }] },
        output: { middlewares: [] },
      }),
    ).toBe(false);
  });

  test("formats throughput values for runtime badges", () => {
    expect(formatThroughput(NaN)).toBe("0 msg/s");
    expect(formatThroughput(0)).toBe("0 msg/s");
    expect(formatThroughput(9.876)).toBe("9.88 msg/s");
    expect(formatThroughput(42.34)).toBe("42.3 msg/s");
    expect(formatThroughput(120.49)).toBe("120 msg/s");
  });

  test("applies endpoint schema defaults without touching unrelated fields", () => {
    const routeSchema = {
      $defs: {
        FileConfig: {
          properties: {
            format: {},
          },
        },
        MongoDbConfig: {
          properties: {
            format: {},
            consume: {},
          },
        },
      },
      properties: {
        id: { type: "string" },
        enabled: { type: "boolean" },
        description: { type: "string" },
        input: { type: "object" },
      },
      required: ["name", "id", "enabled", "description", "input"],
    };

    applyEndpointSchemaDefaults(routeSchema);

    expect(routeSchema.$defs.FileConfig.properties.format.default).toBe("raw");
    expect(routeSchema.$defs.MongoDbConfig.properties.format.default).toBe("raw");
    expect(routeSchema.$defs.MongoDbConfig.properties.consume.default).toBe("capture_all");
    expect(routeSchema.properties).toEqual({
      input: { type: "object" },
    });
    expect(routeSchema.required).toEqual(["name", "input"]);
  });

  test("drops nullable endpoint properties declared with anyOf or oneOf", () => {
    const nullableEndpoint = (keyword: "anyOf" | "oneOf") => ({
      [keyword]: [{ $ref: "#/$defs/Endpoint" }, { type: "null" }],
    });
    const routeSchema = {
      $defs: {
        HttpConfig: {
          properties: {
            stream_response_to: nullableEndpoint("anyOf"),
            url: { type: "string" },
          },
        },
        SwitchConfig: {
          properties: {
            fallback: nullableEndpoint("oneOf"),
            required_endpoint: { $ref: "#/$defs/Endpoint" },
            nullable_string: { anyOf: [{ type: "string" }, { type: "null" }] },
          },
        },
        FileConfig: {
          properties: {
            format: {},
          },
        },
      },
      properties: {
        enabled: { type: "boolean" },
        input: { type: "object" },
      },
      required: ["name", "enabled", "input"],
    };

    applyEndpointSchemaDefaults(routeSchema);

    expect(routeSchema.$defs.HttpConfig.properties).toEqual({ url: { type: "string" } });
    expect(routeSchema.$defs.SwitchConfig.properties).toEqual({
      required_endpoint: { $ref: "#/$defs/Endpoint" },
      nullable_string: { anyOf: [{ type: "string" }, { type: "null" }] },
    });
    expect(routeSchema.$defs.FileConfig.properties.format.default).toBe("raw");
    expect(routeSchema.properties).toEqual({ input: { type: "object" } });
    expect(routeSchema.required).toEqual(["name", "input"]);
  });

  test("generates stable unique names for copied route resources", () => {
    expect(nextUniqueName("route", [])).toBe("route");
    expect(nextUniqueName("route", ["route"])).toBe("route_1");
    expect(nextUniqueName("route", ["route", "route_1", "route_2"])).toBe("route_3");
  });

  test("creates ref inputs with default metrics middleware", () => {
    expect(createRefInputEndpoint("orders")).toEqual({
      middlewares: defaultMetricsMiddleware(),
      ref: "orders",
    });
  });

  test("splits route form payloads and preserves the original name when blank", () => {
    expect(
      splitRouteFormData("existing_route", {
        name: " renamed_route ",
        enabled: true,
        input: { ref: "in" },
      }),
    ).toEqual({
      nextName: "renamed_route",
      routeData: {
        enabled: true,
        input: { ref: "in" },
      },
    });

    expect(
      splitRouteFormData("existing_route", {
        name: "   ",
        input: {
          mongodb: {
            url: "mongodb://localhost:27017",
            consume: "snapshot",
            change_stream: true,
          },
        },
        output: { ref: "out" },
      }),
    ).toEqual({
      nextName: "existing_route",
      routeData: {
        input: {
          mongodb: {
            url: "mongodb://localhost:27017",
            consume: "snapshot",
            change_stream: true,
          },
        },
        output: { ref: "out" },
      },
    });
  });
});
