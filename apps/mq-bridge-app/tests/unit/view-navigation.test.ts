// @vitest-environment jsdom
import { describe, expect, it, vi } from "vitest";
import { openConsumerByIndex, openPublisherByIndex } from "../../ui/src/lib/view-navigation";
import { getAppState } from "../../ui/src/lib/app-shell";

describe("view-navigation", () => {
  it("runs fallback when switchMain is unavailable", () => {
    const replaceState = vi.spyOn(window.history, "replaceState").mockImplementation(() => {});
    const restorePublisherState = vi.fn();
    const fallback = vi.fn();

    window.switchMain = undefined as any;
    openPublisherByIndex(2, "definition", restorePublisherState, fallback);

    expect(getAppState().pending_publisher_restore).toEqual({ idx: 2, tab: "definition" });
    expect(getAppState().last_publisher_idx).toBe(2);
    expect(getAppState().last_publisher_tab).toBe("definition");
    expect(replaceState).toHaveBeenCalledWith(null, "", "#publishers:2");
    expect(fallback).toHaveBeenCalledTimes(1);
    expect(restorePublisherState).toHaveBeenCalledWith(2, { tab: "definition" });

    replaceState.mockRestore();
  });
  it("opens consumer and writes pending state", () => {
    const replaceState = vi.spyOn(window.history, "replaceState").mockImplementation(() => {});
    const restoreConsumerState = vi.fn();
    const switchMain = vi.fn();

    window.switchMain = switchMain;
    openConsumerByIndex(4, "messages", restoreConsumerState);

    expect(getAppState().pending_consumer_restore).toEqual({ idx: 4, tab: "messages" });
    expect(getAppState().last_consumer_idx).toBe(4);
    expect(getAppState().last_consumer_tab).toBe("messages");
    expect(replaceState).toHaveBeenCalledWith(null, "", "#consumers:4");
    expect(switchMain).toHaveBeenCalledWith("consumers");
    expect(restoreConsumerState).not.toHaveBeenCalled();

    replaceState.mockRestore();
  });
});
