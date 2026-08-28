declare module "vanilla-schema-forms" {
  export type JsonObject = Record<string, unknown>;
  export type Renderer = (...args: unknown[]) => HTMLElement | Text | DocumentFragment | null;
  export type FormChangeHandler = (updated: unknown) => void | Promise<void>;
  export type FormApi = { destroy?: () => void; update?: (data: unknown) => void };

  export const renderObject: Renderer;
  export const renderProperties: Renderer;
  export const renderNode: Renderer;
  export const domRenderer: Record<string, unknown> & { renderBoolean?: Renderer };
  export const setI18n: (config: Record<string, string>) => void;
  export const setConfig: (config: JsonObject) => void;
  export const setCustomRenderers: (renderers: Record<string, Renderer>) => void;
  export const createTypeSelectArrayRenderer: (...args: unknown[]) => Renderer;
  export const formatWebAwesomeLabel: (...args: unknown[]) => string;
  export const hydrateNodeWithData: (...args: unknown[]) => void;
  export const toStorePath: (dataPath: Array<string | number>) => Array<string | number>;
  export const rendererConfig: JsonObject;
  export const createOptionalRenderer: (...args: unknown[]) => Renderer;
  export const createForm: (schema: unknown, data?: unknown, options?: JsonObject) => FormApi;
  export const init: (
    container: HTMLElement,
    schema: unknown,
    data?: unknown,
    onChange?: FormChangeHandler,
  ) => void | Promise<void> | FormApi;
  export const generateDefaultData: (schema: unknown) => unknown;
  export const validateAndShowErrors: (...args: unknown[]) => boolean;

  const vanillaSchemaForms: {
    init: typeof init;
    createForm: typeof createForm;
    setCustomRenderers: typeof setCustomRenderers;
    setConfig: typeof setConfig;
    setI18n: typeof setI18n;
  } & Record<string, unknown>;
  export default vanillaSchemaForms;
}
