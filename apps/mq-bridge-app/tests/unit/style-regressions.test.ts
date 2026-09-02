// @vitest-environment jsdom

/**
 * Style regressions checked through the cascade rather than by grepping
 * `style.css`: what matters is the value that lands on the element, including
 * whether the selector still wins, which a source match cannot tell you.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beforeAll, describe, expect, test } from "vitest";

// The stylesheet is served from the core crate so it ships inside the binary.
const STYLESHEET = join(process.cwd(), "crates/core/static/style.css");

beforeAll(() => {
  const style = document.createElement("style");
  style.textContent = readFileSync(STYLESHEET, "utf8");
  document.head.appendChild(style);
});

function element(tag: string, className = ""): HTMLElement {
  const created = document.createElement(tag);
  if (className) created.className = className;
  document.body.appendChild(created);
  return created;
}

/** The properties that make a `<button>` look like a button. */
const CHROME = ["appearance", "borderTopWidth", "borderRadius", "boxShadow", "textAlign"] as const;

/** The properties that give a tab its size and its underline. */
const TAB_SHAPE = ["height", "padding", "borderBottomWidth", "borderBottomStyle"] as const;

const styleOf = (target: HTMLElement, properties: readonly string[]) => {
  const computed = getComputedStyle(target);
  return Object.fromEntries(properties.map((property) => [property, computed[property as never] as string]));
};

describe("content tabs", () => {
  /**
   * The tabs became real `<button>`s so keyboard users get a focusable control.
   * That is only safe while the button chrome stays fully reset — otherwise the
   * tab strip renders as a row of raised grey buttons.
   */
  test("a semantic tab button carries none of the default button chrome", () => {
    const plain = element("button");
    const tab = element("button", "content-tab");

    expect(styleOf(tab, CHROME)).not.toEqual(styleOf(plain, CHROME));
    expect(getComputedStyle(tab).appearance).toBe("none");
    expect(getComputedStyle(tab).borderTopWidth).toBe("0px");
    expect(getComputedStyle(tab).borderRadius).toBe("0px");
    expect(getComputedStyle(tab).boxShadow).toBe("none");
    expect(getComputedStyle(plain).appearance).toBe("auto");
  });

  /** A button tab and a plain tab label must be the same shape, side by side. */
  test("a button tab keeps the size and underline of a plain tab label", () => {
    const label = element("div", "content-tab");
    const button = element("button", "content-tab");

    expect(styleOf(button, TAB_SHAPE)).toEqual(styleOf(label, TAB_SHAPE));
    expect(getComputedStyle(button).height).toBe("22px");
  });

  test("the active tab is the only one with a coloured underline", () => {
    const inactive = element("button", "content-tab");
    const active = element("button", "content-tab active");

    const inactiveColor = getComputedStyle(inactive).borderBottomColor;
    const activeColor = getComputedStyle(active).borderBottomColor;
    expect(inactiveColor).toBe("rgba(0, 0, 0, 0)");
    expect(activeColor).not.toBe(inactiveColor);
  });
});
