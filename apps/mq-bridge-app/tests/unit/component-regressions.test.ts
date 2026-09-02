// @vitest-environment jsdom

/**
 * Behavioural regressions for the shared panel components.
 *
 * These assert on what the components *render and do*, not on the source text
 * of the `.svelte` files: a grep passes while the rendered output is broken and
 * fails on a harmless rename, which is the opposite of what a regression test
 * should do.
 */

import { afterEach, describe, expect, test, vi } from "vitest";
import { flushSync, mount, unmount } from "svelte";
import { get } from "svelte/store";
import HeaderRowsEditor from "../../ui/src/components/HeaderRowsEditor.svelte";
import PayloadDisplay from "../../ui/src/components/PayloadDisplay.svelte";
import PublishersPanel from "../../ui/src/components/PublishersPanel.svelte";
import ConsumersPanel from "../../ui/src/components/ConsumersPanel.svelte";
import { consumersPanelState, publishersPanelState } from "../../ui/src/lib/stores";
import { reactiveBox } from "./reactive-box.svelte";

const mounted: unknown[] = [];

function render<Props extends Record<string, unknown>>(component: unknown, props?: Props) {
  const target = document.createElement("div");
  document.body.appendChild(target);
  const instance = mount(component as never, { target, props: (props ?? {}) as never });
  mounted.push(instance);
  flushSync();
  return target;
}

const defaultPublisherState = { ...get(publishersPanelState) };
const defaultConsumerState = { ...get(consumersPanelState) };

afterEach(() => {
  while (mounted.length > 0) unmount(mounted.pop() as never);
  document.body.innerHTML = "";
  publishersPanelState.set({ ...defaultPublisherState });
  consumersPanelState.set({ ...defaultConsumerState });
});

const headerRow = (id: number, key: string, value: string, enabled = true) => ({ id, key, value, enabled });

/**
 * `wa-button.click()` forwards to a button in its shadow root that jsdom never
 * renders, so a real event is the only way to reach the Svelte handler.
 */
const clickElement = (element: Element) =>
  element.dispatchEvent(new MouseEvent("click", { bubbles: true }));

const rowInputs = (target: HTMLElement) =>
  [...target.querySelectorAll<HTMLInputElement>(".response-header-row input.field-input")].map(
    (input) => input.value,
  );

describe("HeaderRowsEditor", () => {
  test("renders one key/value pair per row, with a toggle only when asked", () => {
    const withoutToggle = render(HeaderRowsEditor, {
      rows: [headerRow(1, "accept", "application/json")],
      onAdd: () => {},
      onUpdate: () => {},
      onRemove: () => {},
    });
    expect(rowInputs(withoutToggle)).toEqual(["accept", "application/json"]);
    expect(withoutToggle.querySelectorAll(".response-header-toggle")).toHaveLength(0);

    const withToggle = render(HeaderRowsEditor, {
      rows: [headerRow(1, "accept", "application/json"), headerRow(2, "x-off", "1", false)],
      showEnabled: true,
      onAdd: () => {},
      onUpdate: () => {},
      onRemove: () => {},
    });
    const toggles = withToggle.querySelectorAll<HTMLInputElement>(".response-header-toggle input");
    expect(toggles).toHaveLength(2);
    expect([...toggles].map((toggle) => toggle.checked)).toEqual([true, false]);
  });

  /**
   * The keyed `{#each}` is what keeps a row's DOM node bound to its own data.
   * Keying by index instead reuses the removed row's node for its successor,
   * which silently discards in-progress edits and focus.
   */
  test("a row keeps its own DOM node when an earlier row is removed", () => {
    const rows = reactiveBox([headerRow(1, "a", "1"), headerRow(2, "b", "2"), headerRow(3, "c", "3")]);
    const target = document.createElement("div");
    document.body.appendChild(target);
    const instance = mount(HeaderRowsEditor as never, {
      target,
      props: {
        get rows() {
          return rows.value;
        },
        onAdd: () => {},
        onUpdate: () => {},
        onRemove: () => {},
      } as never,
    });
    mounted.push(instance);
    flushSync();

    const nodeForC = target.querySelectorAll(".response-header-row")[2];
    (nodeForC as HTMLElement).dataset.marker = "c";

    rows.value = rows.value.filter((row) => row.id !== 1);
    flushSync();

    const remaining = target.querySelectorAll<HTMLElement>(".response-header-row");
    expect(remaining).toHaveLength(2);
    expect(rowInputs(target)).toEqual(["b", "2", "c", "3"]);
    expect(remaining[1].dataset.marker).toBe("c");
  });

  test("editing, adding and deleting report the row they happened on", () => {
    const onUpdate = vi.fn();
    const onRemove = vi.fn();
    const onAdd = vi.fn();
    const onToggle = vi.fn();
    const target = render(HeaderRowsEditor, {
      rows: [headerRow(1, "a", "1"), headerRow(2, "b", "2")],
      showEnabled: true,
      onAdd,
      onUpdate,
      onRemove,
      onToggle,
    });

    const secondRow = target.querySelectorAll(".response-header-row")[1];
    const valueInput = secondRow.querySelectorAll<HTMLInputElement>("input.field-input")[1];
    valueInput.value = "changed";
    valueInput.dispatchEvent(new Event("input", { bubbles: true }));
    expect(onUpdate).toHaveBeenCalledWith(1, "value", "changed");

    const toggle = secondRow.querySelector<HTMLInputElement>(".response-header-toggle input")!;
    toggle.checked = false;
    toggle.dispatchEvent(new Event("change", { bubbles: true }));
    expect(onToggle).toHaveBeenCalledWith(1, false);

    clickElement(secondRow.querySelector(".cons-response-header-delete")!);
    expect(onRemove).toHaveBeenCalledWith(1);

    clickElement(target.querySelector(".response-editor-actions wa-button")!);
    expect(onAdd).toHaveBeenCalledTimes(1);
  });
});

describe("PayloadDisplay", () => {
  const json = '{"sku":"A-1","quantity":2}';

  test("offers pretty printing for JSON only when the caller asks for it", () => {
    const withPretty = render(PayloadDisplay, {
      id: "pretty",
      payload: json,
      contentType: "application/json",
      readOnly: true,
      showPretty: true,
    });
    expect(prettyButton(withPretty)).not.toBeNull();

    const withoutPretty = render(PayloadDisplay, {
      id: "plain",
      payload: json,
      contentType: "application/json",
      readOnly: true,
    });
    expect(prettyButton(withoutPretty)).toBeNull();

    const notJson = render(PayloadDisplay, {
      id: "text",
      payload: "plain text body",
      contentType: "text/plain",
      readOnly: true,
      showPretty: true,
    });
    expect(prettyButton(notJson)).toBeNull();
  });

  test("Pretty re-renders the body indented instead of on one line", async () => {
    const target = render(PayloadDisplay, {
      id: "pretty-click",
      payload: json,
      contentType: "application/json",
      readOnly: true,
      showPretty: true,
    });
    expect(editorText(target)).not.toContain("\n");

    prettyButton(target)!.click();
    flushSync();
    await Promise.resolve();

    const formatted = editorText(target);
    expect(formatted).toContain("\n");
    expect(formatted.replace(/\s+/g, "")).toBe(json.replace(/\s+/g, ""));
  });
});

function prettyButton(target: HTMLElement): HTMLButtonElement | null {
  return (
    [...target.querySelectorAll<HTMLButtonElement>("button.toolbar-btn")].find(
      (button) => button.textContent?.trim() === "Pretty",
    ) ?? null
  );
}

/** CodeMirror renders each line as its own element, so join them back up. */
function editorText(target: HTMLElement): string {
  const lines = target.querySelectorAll(".cm-content .cm-line");
  return [...lines].map((line) => line.textContent ?? "").join("\n");
}

describe("PublishersPanel", () => {
  test("subtabs render in their documented order", () => {
    const target = render(PublishersPanel);
    const labels = [...target.querySelectorAll("#pub-sub-tabs button.content-tab")].map((tab) =>
      tab.textContent?.trim(),
    );
    expect(labels).toEqual(["Definition", "Body", "Headers", "History"]);
  });

  /**
   * The response summary belongs to a request, not to the endpoint definition;
   * leaving it up while the definition form is open shows a status that has
   * nothing to do with what is on screen.
   */
  test("the response tab is hidden while the definition subtab is open", () => {
    publishersPanelState.update((state) => ({ ...state, responseVisible: true, activeSubtab: "payload" }));
    const target = render(PublishersPanel);
    const responseTab = target.querySelector<HTMLElement>("#pub-response-tab")!;
    expect(responseTab.style.display).toBe("flex");

    publishersPanelState.update((state) => ({ ...state, activeSubtab: "definition" }));
    flushSync();
    expect(responseTab.style.display).toBe("none");

    publishersPanelState.update((state) => ({ ...state, activeSubtab: "headers" }));
    flushSync();
    expect(responseTab.style.display).toBe("flex");
  });

  test("request headers use the shared row editor, not the old key/value table", () => {
    publishersPanelState.update((state) => ({
      ...state,
      activeSubtab: "headers",
      metadataRows: [headerRow(1, "authorization", "Bearer x"), headerRow(2, "x-trace", "abc", false)],
    }));
    const target = render(PublishersPanel);

    const pane = target.querySelector<HTMLElement>("#pub-meta-pane")!;
    expect(rowInputs(pane)).toEqual(["authorization", "Bearer x", "x-trace", "abc"]);
    // Header enable toggles are the reason the shared editor exists here.
    expect(pane.querySelectorAll(".response-header-toggle input")).toHaveLength(2);
    expect(pane.querySelector("table.kv-table")).toBeNull();
  });

  test("the captured response body offers pretty printing", () => {
    publishersPanelState.update((state) => ({
      ...state,
      responseVisible: true,
      activeSubtab: "payload",
      responsePayload: '{"ok":true}',
    }));
    const target = render(PublishersPanel);
    const response = target.querySelector<HTMLElement>("#pub-actual-payload")!;
    expect(prettyButton(response)).not.toBeNull();
  });
});

describe("ConsumersPanel", () => {
  test("response headers use the shared row editor with enable toggles", () => {
    consumersPanelState.update((state) => ({
      ...state,
      hasConsumers: true,
      responseSupported: true,
      activeSubtab: "response",
      outputMode: "response",
      responseHeaders: [headerRow(1, "content-type", "application/json")],
    }));
    const target = render(ConsumersPanel);

    const editor = target.querySelector<HTMLElement>("#cons-response-editor")!;
    expect(rowInputs(editor)).toEqual(["content-type", "application/json"]);
    expect(editor.querySelectorAll(".response-header-toggle input")).toHaveLength(1);
  });

  test("captured request and response bodies both offer pretty printing", () => {
    consumersPanelState.update((state) => ({
      ...state,
      hasConsumers: true,
      activeSubtab: "messages",
      selectedMessageIndex: 0,
      detailRequestPayload: '{"in":1}',
      detailRequestContentType: "application/json",
      hasResponse: true,
      detailResponsePayload: '{"out":2}',
      detailResponseContentType: "application/json",
    }));
    const target = render(ConsumersPanel);

    expect(prettyButton(target.querySelector<HTMLElement>("#cons-msg-payload")!)).not.toBeNull();
    expect(prettyButton(target.querySelector<HTMLElement>("#cons-msg-response")!)).not.toBeNull();
  });
});
