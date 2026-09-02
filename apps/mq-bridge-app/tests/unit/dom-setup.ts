/**
 * jsdom gaps that the Web Awesome custom elements hit on mount.
 *
 * Without these, mounting any component containing a `wa-*` element floods the
 * run with unhandled rejections and vitest exits non-zero even though every
 * assertion passed. Node-environment test files load this too, so it must
 * no-op when there is no DOM.
 */

if (typeof HTMLElement !== "undefined") {
  const cache = Symbol("element-internals");

  const stub = () => ({
    setValidity: () => {},
    setFormValue: () => {},
    reportValidity: () => true,
    checkValidity: () => true,
    states: new Set<string>(),
    form: null,
    validity: { valid: true },
    validationMessage: "",
    willValidate: true,
    labels: [],
  });

  const original = HTMLElement.prototype.attachInternals;
  HTMLElement.prototype.attachInternals = function attachInternals(this: HTMLElement) {
    const self = this as HTMLElement & { [cache]?: ReturnType<typeof stub> };
    if (!self[cache]) {
      let base: Record<string, unknown> = {};
      try {
        base = (original?.call(this) as Record<string, unknown>) ?? {};
      } catch {
        base = {};
      }
      self[cache] = { ...stub(), ...base, setValidity: () => {}, setFormValue: () => {} };
    }
    return self[cache] as unknown as ElementInternals;
  };

  if (!("ResizeObserver" in globalThis)) {
    (globalThis as Record<string, unknown>).ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    };
  }

  if (!("IntersectionObserver" in globalThis)) {
    (globalThis as Record<string, unknown>).IntersectionObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
      takeRecords() {
        return [];
      }
    };
  }

  if (!window.matchMedia) {
    window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    })) as typeof window.matchMedia;
  }

  if (!Element.prototype.animate) {
    Element.prototype.animate = (() => ({
      finished: Promise.resolve(),
      cancel() {},
      finish() {},
      addEventListener() {},
      removeEventListener() {},
    })) as unknown as Element["animate"];
  }

  if (!Element.prototype.getAnimations) {
    Element.prototype.getAnimations = () => [];
  }

  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = () => {};
  }
}

export {};
