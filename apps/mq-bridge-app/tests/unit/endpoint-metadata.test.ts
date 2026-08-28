import { describe, it, expect, afterEach } from 'vitest';
import {
  BASIC_ENDPOINT_FIELDS,
  CONSUMER_TYPE_OPTIONS,
  KNOWN_ENDPOINT_ROOT_KEYS,
  PUBLISHER_TYPE_OPTIONS,
  REQUEST_BAR_LAYOUTS,
  STRUCTURAL_ENDPOINT_KINDS,
  getEndpointTypeGroups,
  registerEndpointKindsFromSchema,
  resetEndpointKinds,
} from '../../ui/src/lib/endpoint-metadata';

const schemaWith = (variants: unknown[]) => ({
  $defs: {
    Endpoint: { oneOf: variants },
    NewThingConfig: {
      type: 'object',
      properties: { url: { type: 'string' }, stream: { type: 'string' }, tuning: { type: 'object' } },
      required: ['stream', 'url'],
    },
  },
});

const kafkaVariant = { type: 'object', properties: { kafka: { $ref: '#/$defs/KafkaConfig' } }, required: ['kafka'] };
const switchVariant = {
  type: 'object',
  format: 'structural_endpoint',
  properties: { switch: { $ref: '#/$defs/SwitchConfig' } },
  required: ['switch'],
};

describe('endpoint kind registry', () => {
  afterEach(() => {
    resetEndpointKinds();
  });

  it('starts from the curated kinds before a schema is registered', () => {
    expect(KNOWN_ENDPOINT_ROOT_KEYS).toContain('http');
    expect(PUBLISHER_TYPE_OPTIONS).toContain('kafka');
    expect(PUBLISHER_TYPE_OPTIONS).toContain('pulsar');
    expect(CONSUMER_TYPE_OPTIONS).toContain('pulsar');
    expect(BASIC_ENDPOINT_FIELDS.pulsar).toEqual(['url', 'topic', 'subscription']);
    expect(CONSUMER_TYPE_OPTIONS).toContain('postgres_cdc');
    expect(PUBLISHER_TYPE_OPTIONS).not.toContain('postgres_cdc');
    expect(STRUCTURAL_ENDPOINT_KINDS.has('stream_buffer')).toBe(true);
  });

  it('registers unknown kinds from the schema without a code change', () => {
    const newThing = {
      type: 'object',
      properties: { new_thing: { $ref: '#/$defs/NewThingConfig' } },
      required: ['new_thing'],
    };

    const registered = registerEndpointKindsFromSchema(schemaWith([kafkaVariant, newThing, switchVariant]));

    expect(registered).toEqual(['kafka', 'switch', 'new_thing']);
    expect(PUBLISHER_TYPE_OPTIONS).toContain('new_thing');
    expect(CONSUMER_TYPE_OPTIONS).toContain('new_thing');
    // Basic fields fall back to the config's required properties, and the
    // url-ish one lands in the wide request bar input.
    expect(BASIC_ENDPOINT_FIELDS.new_thing).toEqual(['stream', 'url']);
    expect(REQUEST_BAR_LAYOUTS.new_thing.fields).toEqual([
      { inputId: 'pub-extra-1', field: 'stream', label: 'STREAM' },
      { inputId: 'pub-url', field: 'url', label: 'URL' },
    ]);
  });

  it('treats schema-tagged variants as structural and keeps curated overrides', () => {
    const structuralNewThing = {
      type: 'object',
      format: 'structural_endpoint',
      properties: { new_thing: { $ref: '#/$defs/NewThingConfig' } },
      required: ['new_thing'],
    };

    registerEndpointKindsFromSchema(schemaWith([kafkaVariant, structuralNewThing]));

    expect(STRUCTURAL_ENDPOINT_KINDS.has('new_thing')).toBe(true);
    // Structural kinds are outputs by default, so they stay out of consumers.
    expect(CONSUMER_TYPE_OPTIONS).not.toContain('new_thing');
    expect(BASIC_ENDPOINT_FIELDS.kafka).toEqual(['url', 'topic', 'group_id']);
  });

  it('keeps the curated list when the schema has no endpoint definition', () => {
    expect(registerEndpointKindsFromSchema({ $defs: {} })).toEqual([]);
    expect(KNOWN_ENDPOINT_ROOT_KEYS).toContain('http');
  });

  it('groups structural kinds after the transports', () => {
    const groups = getEndpointTypeGroups('publisher');

    expect(groups[0].label).toBe('');
    expect(groups[0].kinds[0]).toBe('http');
    expect(groups[0].kinds).not.toContain('switch');
    expect(groups[1].label).toBe('Routing');
    expect(groups[1].kinds).toContain('switch');
    expect(groups[1].kinds).toContain('request');
  });

  it('hides feature-gated kinds when the backend lacks the feature', () => {
    const groups = getEndpointTypeGroups('publisher', { ibm_mq: false } as any);
    expect(groups[0].kinds).not.toContain('ibmmq');
    expect(groups[0].kinds).toContain('http');
  });
});
