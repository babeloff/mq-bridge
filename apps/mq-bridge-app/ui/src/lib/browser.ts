export type AppWindow = Window & typeof globalThis & {
  __MQB_DESKTOP__?: boolean;
  appConfig?: Record<string, unknown>;
  appSchema?: Record<string, unknown>;
  VanillaSchemaForms?: unknown;
  mqbAlert?: (message: string, title?: string) => Promise<void>;
  mqbConfirm?: (message: string, title?: string) => Promise<boolean>;
  mqbPrompt?: (
    message: string,
    title?: string,
    options?: {
      confirmLabel?: string;
      cancelLabel?: string;
      value?: string;
      placeholder?: string;
    },
  ) => Promise<string | null>;
  mqbChoose?: (
    message: string,
    title?: string,
    options?: {
      confirmLabel?: string;
      cancelLabel?: string;
      choices?: Array<{ value: string; label: string; description?: string }>;
    },
  ) => Promise<string | null>;
  [key: string]: any;
};

export function browserWindow() {
  return window as AppWindow;
}

export function currentHash() {
  return browserWindow().location.hash;
}

export function replaceHash(nextHash: string) {
  browserWindow().history.replaceState(null, "", nextHash);
}

export function onHashChange(listener: () => void) {
  browserWindow().addEventListener("hashchange", listener);
}
