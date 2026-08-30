import { describe, expect, test } from "vitest";
import {
  EMPTY_RUNTIME_STATUS,
  isRuntimeConnected,
  runtimeStatusLabel,
} from "../../ui/src/lib/runtime-status";

describe("runtime-status", () => {
  test("treats empty status as idle", () => {
    expect(isRuntimeConnected(EMPTY_RUNTIME_STATUS)).toBe(false);
    expect(runtimeStatusLabel(EMPTY_RUNTIME_STATUS)).toBe("mq-bridge idle");
  });

  test("formats mixed route and consumer activity", () => {
    expect(
      runtimeStatusLabel({
        active_consumers: ["a", "b"],
        active_routes: ["r1"],
        route_throughput: {},
        consumers: {},
      }),
    ).toBe("1 active route • 2 active consumers");
  });
});
