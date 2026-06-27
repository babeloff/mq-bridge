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

export interface ConsumerStatus {
  healthy: boolean;
  target: string;
  /** Broker backlog/lag where the transport reports it; absent otherwise. */
  pending?: number;
  capacity?: number;
  error?: string;
  details: JsonValue;
}

export class Consumer {
  static fromFile(path: string, name?: string | null): Consumer;
  static fromStr(text: string, name?: string | null): Consumer;
  static fromConfig(config: JsonValue, name?: string | null): Consumer;
  /**
   * Receive up to `max` messages (default 256) without acking. Resolves to an
   * empty array if `timeoutMs` milliseconds elapse with nothing received, or the
   * source is exhausted. Omit `timeoutMs` to block until a message arrives. The
   * returned messages are acked by the next `commit()` call — which you must
   * call (see {@link Consumer.commit}).
   */
  poll(max?: number | null, timeoutMs?: number | null): Promise<Message[]>;
  /**
   * Acknowledge every batch returned by `poll()` since the last `commit()`,
   * advancing the consumer offset.
   *
   * Calling this is required, not optional. Without it the offset never advances
   * (messages are re-delivered on the next run), most brokers stall once their
   * unacknowledged/prefetch window fills, and uncommitted batches are held in
   * memory so the process grows unbounded. To retry a failed batch, simply
   * don't commit it — it will be redelivered.
   */
  commit(): Promise<void>;
  /**
   * Status snapshot for the underlying endpoint. `pending === 0` is a precise
   * "caught up" signal on transports that report backlog (Kafka, AMQP, NATS
   * JetStream); `pending` is absent where the broker exposes none.
   */
  status(): Promise<ConsumerStatus>;
  /** Release the broker connection. Idempotent; `poll()`/`status()` reject after. */
  close(): Promise<void>;
  /** `true` once the source has signalled end-of-stream (e.g. a drained file). */
  get exhausted(): boolean;
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
