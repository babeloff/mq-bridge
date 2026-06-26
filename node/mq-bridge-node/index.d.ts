export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type Metadata = Record<string, string>;
export type HandlerResult = Message | null | undefined;
export type MessageHandler = (message: Message) => HandlerResult | Promise<HandlerResult>;
export type JsonHandler = (data: JsonValue) => HandlerResult | Promise<HandlerResult>;

export class Message {
  constructor(payload: Buffer | Uint8Array, metadata?: Metadata | null, id?: string | null);
  static fromJson(data: JsonValue, metadata?: Metadata | null, id?: string | null): Message;
  get payload(): Buffer;
  get metadata(): Metadata;
  get id(): string | null;
  json(): JsonValue;
  text(): string;
}

export class Publisher {
  static fromFile(path: string, name?: string | null): Publisher;
  static fromStr(text: string, name?: string | null): Publisher;
  static fromConfig(config: JsonValue, name?: string | null): Publisher;
  /** @deprecated Use {@link Publisher.fromFile} instead. */
  static fromYaml(path: string, name?: string | null): Publisher;
  /** @deprecated Use {@link Publisher.fromStr} instead. */
  static fromYamlStr(text: string, name?: string | null): Publisher;
  send(message: Message): Promise<void>;
  request(message: Message): Promise<Message>;
  sendJson(data: JsonValue, metadata?: Metadata | null, id?: string | null): Promise<void>;
  requestJson(data: JsonValue, metadata?: Metadata | null, id?: string | null): Promise<Message>;
}

export class Route {
  static fromFile(path: string, name?: string | null): Route;
  static fromStr(text: string, name?: string | null): Route;
  static fromConfig(config: JsonValue, name?: string | null): Route;
  /** @deprecated Use {@link Route.fromFile} instead. */
  static fromYaml(path: string, name?: string | null): Route;
  /** @deprecated Use {@link Route.fromStr} instead. */
  static fromYamlStr(text: string, name?: string | null): Route;
  withHandler(handler: MessageHandler): void;
  addHandler(kind: string, handler: JsonHandler): void;
  start(): void;
  stop(): void;
  join(): void;
}

/**
 * JSON Schema for the route/config mapping, generated from the compiled Rust
 * models. Throws if the addon was built without the `schema` feature.
 */
export function configSchema(): JsonValue;

export const version: string;
