import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, test } from "vitest";

describe("style regressions", () => {
  test("semantic content-tab buttons reset browser button chrome", () => {
    const css = readFileSync(join(process.cwd(), "crates/core/static/style.css"), "utf8");

    expect(css).toContain("Keep semantic tab buttons visually identical to the old plain tab labels.");
    expect(css).toMatch(/button\.content-tab\s*\{/);
    expect(css).toMatch(/button\.content-tab[\s\S]*background:\s*transparent;/);
    expect(css).toMatch(/button\.content-tab[\s\S]*border:\s*0;/);
    expect(css).toMatch(/button\.content-tab[\s\S]*border-bottom:\s*2px solid transparent;/);
    expect(css).toMatch(/button\.content-tab[\s\S]*border-radius:\s*0;/);
    expect(css).toMatch(/button\.content-tab[\s\S]*appearance:\s*none;/);
  });
});
