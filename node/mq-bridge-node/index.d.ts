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
  static fromYaml(path: string, name: string): Publisher;
  static fromYamlStr(text: string, name: string): Publisher;
  static fromConfig(config: JsonValue, name: string): Publisher;
  send(message: Message): void;
  request(message: Message): Message;
  sendJson(data: JsonValue, metadata?: Metadata | null, id?: string | null): void;
  requestJson(data: JsonValue, metadata?: Metadata | null, id?: string | null): Message;
}

export class Route {
  static fromYaml(path: string, name: string): Route;
  static fromYamlStr(text: string, name: string): Route;
  static fromConfig(config: JsonValue, name: string): Route;
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
