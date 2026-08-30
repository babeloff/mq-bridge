import type { FeatureAvailabilityResponse } from './generated/ui-types';

let cachedFeatures: FeatureAvailabilityResponse | null = null;

/**
 * Fetch available features from the backend.
 * Results are cached after the first successful fetch.
 */
export async function getAvailableFeatures(): Promise<FeatureAvailabilityResponse> {
  if (cachedFeatures) {
    return cachedFeatures;
  }

  try {
    const response = await fetch('/features');
    if (!response.ok) {
      throw new Error(`Failed to fetch features: ${response.statusText}`);
    }
    const features = (await response.json()) as FeatureAvailabilityResponse;
    cachedFeatures = features;
    return features;
  } catch (error) {
    console.error('Failed to fetch feature availability:', error);
    // Return a default set of features (assume all are available as fallback)
    return {
      ibm_mq: false,
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
      redis_streams: true,
      object_store: true,
    };
  }
}

/**
 * Check if a specific feature is available.
 */
export async function isFeatureAvailable(feature: keyof FeatureAvailabilityResponse): Promise<boolean> {
  const features = await getAvailableFeatures();
  return features[feature];
}

/**
 * Clear the cached features (useful for testing or after reconnection).
 */
export function clearFeatureCache(): void {
  cachedFeatures = null;
}

// Made with Bob
