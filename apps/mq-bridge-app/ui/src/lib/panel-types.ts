import type { PresetsByPublisher, PublisherHistoryEntry, PublisherHistoryStore, PublisherPreset } from "./workspace-config";
import type {
  AppConfig,
  ConsumerConfig as GeneratedConsumerConfig,
  ConsumerMessageCaptureConfig as GeneratedConsumerMessageCaptureConfig,
  ConsumerOutputConfig as GeneratedConsumerOutputConfig,
  ConsumerResponseConfig as GeneratedConsumerResponseConfig,
  ConsumerStatusResponse,
  HeaderRow as GeneratedHeaderRow,
  PublisherClient,
} from "./generated/ui-types";

export type ConsumerMessage = {
  id?: string;
  payload: unknown;
  metadata?: Record<string, string>;
  time?: string;
  response?: unknown;
  response_metadata?: Record<string, string>;
};

export type ConsumerStatus = ConsumerStatusResponse & {
  unsaved?: boolean;
};

export type ConsumerMessageCaptureConfig = Required<GeneratedConsumerMessageCaptureConfig>;
export type ConsumerResponseConfig = Required<GeneratedConsumerResponseConfig>;
export type ConsumerOutputConfig = GeneratedConsumerOutputConfig;

export type ConsumerConfig = Omit<GeneratedConsumerConfig, "endpoint" | "response" | "output" | "message_capture"> & {
  id?: string;
  endpoint: Record<string, unknown>;
  comment?: string;
  response?: unknown;
  output?: ConsumerOutputConfig | null;
  message_capture?: ConsumerMessageCaptureConfig;
  batch_size?: number;
};

export type PublisherConfig = Omit<PublisherClient, "endpoint" | "headers"> & {
  id?: string;
  endpoint: Record<string, unknown>;
  comment?: string;
  headers?: Required<GeneratedHeaderRow>[];
};

export type ConsumerResponseHeaderRow = {
  id: number;
  key: string;
  value: string;
  enabled: boolean;
};

export type PublisherState = {
  payload: string;
  headers?: Array<{
    id: number;
    key: string;
    value: string;
    enabled: boolean;
  }>;
};

export type PublisherResponseState = {
  responseVisible?: boolean;
  responseTabLabel?: string;
  responseStatusLabel?: string;
  responseStatusText?: string;
  responseStatusColor?: string;
  responseDurationLabel?: string;
  responseSizeLabel?: string;
  requestRows?: Array<[string, string]>;
  requestHeaders?: Array<[string, string]>;
  responseHeaders?: Array<[string, string]>;
  responsePayload?: string;
};

export type PublisherHistoryItem = PublisherHistoryEntry;

export type ConsumersAppConfig = Pick<AppConfig, "routes" | "config_security"> & {
  consumers: ConsumerConfig[];
  publishers?: PublisherConfig[];
};

export type PublishersAppConfig = Pick<AppConfig, "routes" | "env_vars" | "config_security"> & {
  publishers: PublisherConfig[];
  consumers?: ConsumerConfig[];
  history?: PublisherHistoryStore;
  presets?: PresetsByPublisher;
};

export type ConsumersSchemaRoot = {
  properties?: {
    consumers?: {
      items?: Record<string, any>;
    };
  };
  $defs?: Record<string, any>;
};

export type PublishersSchemaRoot = {
  properties?: {
    publishers?: {
      items?: Record<string, any>;
    };
  };
  $defs?: Record<string, any>;
};

export type PublisherPresetLike = PublisherPreset;
