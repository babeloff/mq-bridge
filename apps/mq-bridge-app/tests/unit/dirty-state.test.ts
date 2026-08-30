import { describe, expect, test } from "vitest";
import {
  cloneSectionState,
  createDirtyTracker,
  isDirty,
  normalizeSectionState,
  serializeSectionState,
} from "../../ui/src/lib/dirty-state";

describe("dirty-state", () => {
  test("normalizes object keys recursively", () => {
    expect(normalizeSectionState({ b: 1, a: { d: 2, c: 3 } })).toEqual({
      a: { c: 3, d: 2 },
      b: 1,
    });
  });

  test("serializes equivalent objects consistently", () => {
    const left = serializeSectionState({ b: 1, a: { d: 2, c: 3 } });
    const right = serializeSectionState({ a: { c: 3, d: 2 }, b: 1 });
    expect(left).toBe(right);
  });

  test("clones via normalized JSON state", () => {
    const original = { b: 1, a: { d: 2, c: 3 } };
    expect(cloneSectionState(original)).toEqual({ a: { c: 3, d: 2 }, b: 1 });
  });

  test("reuses prior baseline when rebuilding a tracker", () => {
    const firstValue = { foo: "bar" };
    const [, previous] = createDirtyTracker("routes", "route-save", () => firstValue, undefined, {});

    const nextValue = { foo: "baz" };
    const [, tracker] = createDirtyTracker("routes", "route-save", () => nextValue, previous, {});
    expect(tracker.baseline).toBe(previous.baseline);
    expect(isDirty(tracker)).toBe(true);
  });
});
