import { vi } from "vitest";

const WINDOW_STUB_KEYS = [
  "VanillaSchemaForms",
  "registerDirtySection",
  "refreshDirtySection",
  "markSectionSaved",
  "saveConfigSection",
  "fetchConfigFromServer",
  "mqbAlert",
  "mqbConfirm",
  "mqbPrompt",
  "mqbChoose",
  "switchMain",
  "_mqb_saved_sections",
  "appSchema",
  "initRoutes",
] as const;

let previousWindowStubValues: Partial<Record<(typeof WINDOW_STUB_KEYS)[number], unknown>> | null = null;

export function createHyperscriptNode(tag: string, props?: Record<string, unknown>, ...children: unknown[]) {
  const element = document.createElement(tag);
  Object.entries(props || {}).forEach(([key, value]) => {
    if (key === "className") {
      element.className = String(value);
      return;
    }
    element.setAttribute(key, String(value));
  });
  children.flat().forEach((child) => {
    if (child instanceof Node) {
      element.appendChild(child);
    } else if (child !== null && child !== undefined) {
      element.appendChild(document.createTextNode(String(child)));
    }
  });
  return element;
}

/** Shared window stubs that both consumer and publisher tests need. */
export function installBaseWindowStubs() {
  previousWindowStubValues = Object.fromEntries(
    WINDOW_STUB_KEYS.map((key) => [key, (window as Record<string, unknown>)[key]]),
  ) as Partial<Record<(typeof WINDOW_STUB_KEYS)[number], unknown>>;
  window.VanillaSchemaForms = {
    h: createHyperscriptNode,
    init: vi.fn().mockResolvedValue(undefined),
  };
  window.registerDirtySection = vi.fn();
  window.refreshDirtySection = vi.fn().mockReturnValue(false);
  // Mirror the production markSectionSaved: record the snapshot in
  // _mqb_saved_sections so saved/dirty checks (e.g. isSavedConsumer,
  // hasUnsavedConsumers) behave like the real app instead of treating every
  // loaded entity as unsaved.
  window.markSectionSaved = vi.fn((sectionName?: string, savedValue?: unknown) => {
    if (typeof sectionName !== "string") return;
    const current = (window._mqb_saved_sections || {}) as Record<string, unknown>;
    current[sectionName] = savedValue === undefined ? undefined : JSON.parse(JSON.stringify(savedValue));
    window._mqb_saved_sections = current;
  });
  window.saveConfigSection = vi.fn().mockResolvedValue({});
  window.fetchConfigFromServer = vi.fn().mockResolvedValue({});
  window.mqbAlert = vi.fn().mockResolvedValue(undefined);
  window.mqbConfirm = vi.fn().mockResolvedValue(true);
  window.mqbPrompt = vi.fn().mockResolvedValue(null);
  window.mqbChoose = vi.fn().mockResolvedValue(null);
  window.switchMain = vi.fn();
  window._mqb_saved_sections = {};
  window.appSchema = {};
  window.initRoutes = vi.fn();
}

export function restoreBaseWindowStubs() {
  if (!previousWindowStubValues) return;

  for (const key of WINDOW_STUB_KEYS) {
    const previousValue = previousWindowStubValues[key];
    if (previousValue === undefined) {
      delete (window as Record<string, unknown>)[key];
    } else {
      (window as Record<string, unknown>)[key] = previousValue;
    }
  }

  previousWindowStubValues = null;
}

/**
 * The publishers panel's DOM contract, for tests that drive `publishers-view`
 * without mounting the Svelte component.
 */
export function mountPublishersDom() {
  document.body.innerHTML = `
    <div id="tab-publishers" class="active">
      <div id="publishers-container">
        <input id="pub-filter" />
        <button id="pub-add"></button>
        <button id="pub-copy"></button>
        <button id="pub-clone"></button>
        <button id="pub-save"></button>
        <button id="pub-delete"></button>
        <button id="pub-send"></button>
        <button id="pub-beautify"></button>
        <button id="add-meta"></button>
        <div id="pub-list"></div>
        <div id="pub-empty-alert"></div>
        <div id="pub-main-ui"></div>
        <input id="pub-proto" />
        <div id="pub-method-wrap"></div>
        <select id="pub-method"></select>
        <div id="pub-extra-1-wrap"></div>
        <div id="pub-extra-2-wrap"></div>
        <div id="pub-url-wrap"></div>
        <span id="pub-extra-1-label"></span>
        <span id="pub-extra-2-label"></span>
        <span id="pub-url-label"></span>
        <input id="pub-extra-1" />
        <input id="pub-extra-2" />
        <input id="pub-url" />
        <div id="pub-sub-tabs">
          <button class="content-tab" id="ctab-payload" data-target="pub-payload-pane"></button>
          <button class="content-tab" id="ctab-config" data-target="pub-config-pane"></button>
          <button class="content-tab" data-target="pub-meta-pane"></button>
          <button class="content-tab" data-target="pub-history-pane"></button>
        </div>
        <div id="pub-top-content-wrapper">
          <div class="pane-top" id="pub-payload-pane"></div>
          <div class="pane-top" id="pub-meta-pane"></div>
          <div class="pane-top" id="pub-history-pane"></div>
          <div class="pane-top" id="pub-config-pane"></div>
        </div>
        <textarea id="pub-payload"></textarea>
        <table id="metadata-container"><tbody></tbody></table>
        <div id="pub-response-container"></div>
        <div id="pub-response-status"></div>
        <div id="pub-response"></div>
        <button id="pub-resp-copy"></button>
        <div id="pub-response-tab"></div>
        <div id="pub-config-form"></div>
      </div>
    </div>
  `;
}
