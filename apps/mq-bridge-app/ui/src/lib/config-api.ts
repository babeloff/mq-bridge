import { explainConfigSaveError } from "./save-error";

async function readResponseError(response: Response, fallbackLabel: string): Promise<Error> {
  const body = await response.text().catch(() => "");
  const detail = body || response.statusText || `${fallbackLabel} failed with status ${response.status}`;
  return new Error(detail);
}

export async function fetchConfigFromServer<T>(fetchImpl: typeof fetch): Promise<T> {
  const response = await fetchImpl("/config", { cache: "no-store" });
  return response.json() as Promise<T>;
}

export async function fetchStorageSecurityFromServer<T>(fetchImpl: typeof fetch): Promise<T> {
  const response = await fetchImpl("/storage-security", { cache: "no-store" });
  if (!response.ok) {
    throw await readResponseError(response, "Fetching storage security");
  }
  return response.json() as Promise<T>;
}

export async function fetchConfigRecoveryFromServer<T>(fetchImpl: typeof fetch): Promise<T> {
  const response = await fetchImpl("/config-recovery", { cache: "no-store" });
  if (!response.ok) {
    throw await readResponseError(response, "Fetching config recovery");
  }
  return response.json() as Promise<T>;
}

export async function postConfig(fetchImpl: typeof fetch, config: unknown): Promise<void> {
  const body = JSON.stringify(config);
  const response = await fetchImpl("/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  if (!response.ok) {
    const error = await readResponseError(response, "Posting config");
    throw new Error(explainConfigSaveError(error.message, body));
  }
}

export async function postResetConfigRecovery<T>(fetchImpl: typeof fetch): Promise<T> {
  const response = await fetchImpl("/config-recovery/reset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
  if (!response.ok) {
    throw await readResponseError(response, "Resetting config recovery");
  }
  return response.json() as Promise<T>;
}

export async function saveWholeConfig<T extends object>(
  fetchImpl: typeof fetch,
  currentConfig: T,
): Promise<T> {
  await postConfig(fetchImpl, currentConfig);
  return fetchConfigFromServer<T>(fetchImpl);
}

export async function saveConfigSection<T extends object, K extends keyof T>(
  fetchImpl: typeof fetch,
  currentConfig: T,
  sectionName: K,
  sectionValue: T[K],
): Promise<T> {
  const nextConfig = { ...currentConfig, [sectionName]: sectionValue };
  await postConfig(fetchImpl, nextConfig);
  return fetchConfigFromServer<T>(fetchImpl);
}
