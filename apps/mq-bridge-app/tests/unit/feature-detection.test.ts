import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { getAvailableFeatures, isFeatureAvailable, clearFeatureCache } from '../../ui/src/lib/feature-detection';
import type { FeatureAvailabilityResponse } from '../../ui/src/lib/generated/ui-types';

describe('Feature Detection', () => {
  const originalFetch = global.fetch;

  beforeEach(() => {
    clearFeatureCache();
    vi.restoreAllMocks();
  });

  afterEach(() => {
    // Tests reassign global.fetch directly (not via vi.spyOn), so restoreAllMocks
    // won't undo them; restore the original reference to avoid cross-test pollution.
    global.fetch = originalFetch;
  });

  it('should fetch features from the backend', async () => {
    const mockFeatures: FeatureAvailabilityResponse = {
      ibm_mq: true,
      kafka: true,
      nats: true,
      amqp: true,
      mqtt: true,
      http: true,
      grpc: true,
      zeromq: true,
      mongodb: true,
      aws: true,
      sled: true,
    };

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => mockFeatures,
    });

    const features = await getAvailableFeatures();
    expect(features).toEqual(mockFeatures);
    expect(global.fetch).toHaveBeenCalledWith('/features');
  });

  it('should cache features after first fetch', async () => {
    const mockFeatures: FeatureAvailabilityResponse = {
      ibm_mq: true,
      kafka: true,
      nats: true,
      amqp: true,
      mqtt: true,
      http: true,
      grpc: true,
      zeromq: true,
      mongodb: true,
      aws: true,
      sled: true,
    };

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => mockFeatures,
    });

    await getAvailableFeatures();
    await getAvailableFeatures();
    await getAvailableFeatures();

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('should return default features on fetch error', async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));

    const features = await getAvailableFeatures();
    expect(features.ibm_mq).toBe(false);
    expect(features.kafka).toBe(true);
    expect(features.http).toBe(true);
  });

  it('should check if specific feature is available', async () => {
    const mockFeatures: FeatureAvailabilityResponse = {
      ibm_mq: true,
      kafka: true,
      nats: false,
      amqp: true,
      mqtt: true,
      http: true,
      grpc: false,
      zeromq: true,
      mongodb: true,
      aws: true,
      sled: true,
    };

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => mockFeatures,
    });

    expect(await isFeatureAvailable('ibm_mq')).toBe(true);
    expect(await isFeatureAvailable('kafka')).toBe(true);
    expect(await isFeatureAvailable('nats')).toBe(false);
    expect(await isFeatureAvailable('grpc')).toBe(false);
  });

  it('should clear cache when requested', async () => {
    const mockFeatures: FeatureAvailabilityResponse = {
      ibm_mq: true,
      kafka: true,
      nats: true,
      amqp: true,
      mqtt: true,
      http: true,
      grpc: true,
      zeromq: true,
      mongodb: true,
      aws: true,
      sled: true,
    };

    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => mockFeatures,
    });

    await getAvailableFeatures();
    clearFeatureCache();
    await getAvailableFeatures();

    expect(global.fetch).toHaveBeenCalledTimes(2);
  });
});

// Made with Bob
