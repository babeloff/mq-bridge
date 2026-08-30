import { getAppState, hasMainTabSwitch, switchMainTab } from "./app-shell";
import { replaceHash } from "./browser";

type ConsumerTab = "definition" | "response" | "messages";
type PublisherTab = "payload" | "headers" | "history" | "definition";

function switchOrRun(mainTab: "consumers" | "publishers", callback: () => void, fallback?: () => void) {
  if (hasMainTabSwitch()) {
    fallback?.();
    void switchMainTab(mainTab);
    return;
  }

  fallback?.();
  callback();
}

export function openConsumerByIndex(
  idx: number,
  tab: ConsumerTab,
  restoreConsumerState: (idx: number, options?: { tab?: ConsumerTab }) => void,
  fallback?: () => void,
) {
  const state = getAppState();
  state.last_consumer_idx = idx;
  state.last_consumer_tab = tab;
  state.pending_consumer_restore = { idx, tab };
  replaceHash(`#consumers:${idx}`);
  switchOrRun("consumers", () => restoreConsumerState(idx, { tab }), fallback);
}

export function openPublisherByIndex(
  idx: number,
  tab: PublisherTab,
  restorePublisherState: (idx: number, options?: { tab?: PublisherTab }) => void,
  fallback?: () => void,
) {
  const state = getAppState();
  state.last_publisher_idx = idx;
  state.last_publisher_tab = tab;
  state.pending_publisher_restore = { idx, tab };
  replaceHash(`#publishers:${idx}`);
  switchOrRun("publishers", () => restorePublisherState(idx, { tab }), fallback);
}
