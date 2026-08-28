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
