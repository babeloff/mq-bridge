import { describe, expect, test, vi } from "vitest";
import {
  fetchConfigFromServer,
  fetchConfigRecoveryFromServer,
  fetchStorageSecurityFromServer,
  postConfig,
  postResetConfigRecovery,
  saveConfigSection,
  saveWholeConfig,
} from "../../ui/src/lib/config-api";

describe("config-api", () => {
  test("fetches config without cache", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      json: async () => ({ ok: true }),
    });

    await expect(fetchConfigFromServer(fetchImpl as unknown as typeof fetch)).resolves.toEqual({ ok: true });
    expect(fetchImpl).toHaveBeenCalledWith("/config", { cache: "no-store" });
  });

  test("throws response text when post fails", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: false,
      text: async () => "bad config",
    });

    await expect(postConfig(fetchImpl as unknown as typeof fetch, { foo: "bar" })).rejects.toThrow(
      "bad config",
    );
  });

  test("fetches storage security without cache", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ messagesEncrypted: true }),
    });

    await expect(fetchStorageSecurityFromServer(fetchImpl as unknown as typeof fetch)).resolves.toEqual({
      messagesEncrypted: true,
    });
    expect(fetchImpl).toHaveBeenCalledWith("/storage-security", { cache: "no-store" });
  });

  test("fetches config recovery status without cache", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ message: "needs recovery" }),
    });

    await expect(fetchConfigRecoveryFromServer(fetchImpl as unknown as typeof fetch)).resolves.toEqual({
      message: "needs recovery",
    });
    expect(fetchImpl).toHaveBeenCalledWith("/config-recovery", { cache: "no-store" });
  });

  test("posts config recovery reset and returns backup info", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ backup_path: "/tmp/config.yml.recovery.bak" }),
    });

    await expect(postResetConfigRecovery(fetchImpl as unknown as typeof fetch)).resolves.toEqual({
      backup_path: "/tmp/config.yml.recovery.bak",
    });
    expect(fetchImpl).toHaveBeenCalledWith("/config-recovery/reset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
  });

  test("throws response text when fetching storage security fails", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      statusText: "Service Unavailable",
      text: async () => "storage down",
    });

    await expect(fetchStorageSecurityFromServer(fetchImpl as unknown as typeof fetch)).rejects.toThrow(
      "storage down",
    );
  });

  test("throws response text when fetching config recovery fails", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      text: async () => "recovery unavailable",
    });

    await expect(fetchConfigRecoveryFromServer(fetchImpl as unknown as typeof fetch)).rejects.toThrow(
      "recovery unavailable",
    );
  });

  test("throws response text when config recovery reset fails", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: false,
      text: async () => "reset failed",
    });

    await expect(postResetConfigRecovery(fetchImpl as unknown as typeof fetch)).rejects.toThrow(
      "reset failed",
    );
    expect(fetchImpl).toHaveBeenCalledWith("/config-recovery/reset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
  });

  test("saves whole config and returns refreshed data", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, text: async () => "" })
      .mockResolvedValueOnce({ json: async () => ({ saved: true }) });

    await expect(
      saveWholeConfig(fetchImpl as unknown as typeof fetch, { foo: "bar" }),
    ).resolves.toEqual({ saved: true });
  });

  test("saves one section against the current in-memory config", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, text: async () => "" })
      .mockResolvedValueOnce({ json: async () => ({ routes: {}, publishers: [{ name: "pub" }], consumers: [{ name: "c1" }] }) });

    await expect(
      saveConfigSection(
        fetchImpl as unknown as typeof fetch,
        { routes: {}, publishers: [], consumers: [{ name: "c1" }] },
        "publishers",
        [{ name: "pub" }],
      ),
    ).resolves.toEqual({
      routes: {},
      publishers: [{ name: "pub" }],
      consumers: [{ name: "c1" }],
    });
    expect(fetchImpl).toHaveBeenNthCalledWith(1, "/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        routes: {},
        publishers: [{ name: "pub" }],
        consumers: [{ name: "c1" }],
      }),
    });
  });
});
