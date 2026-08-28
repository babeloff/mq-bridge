// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
// @ts-expect-error Svelte's client test runtime is only available from the client entrypoint path here.
import { flushSync, mount, unmount } from "../../node_modules/svelte/src/index-client.js";
import SettingsPanel from "../../ui/src/components/SettingsPanel.svelte";
import { activeMainTab, storageSecurityStore } from "../../ui/src/lib/stores";
import { EMPTY_STORAGE_SECURITY } from "../../ui/src/lib/storage-security";

function mountSettingsPanel() {
  const target = document.createElement("div");
  document.body.appendChild(target);
  const instance = mount(SettingsPanel, { target });
  flushSync();
  return { target, instance };
}

describe("SettingsPanel", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    activeMainTab.set("config");
    storageSecurityStore.set({
      ...EMPTY_STORAGE_SECURITY,
      encrypted: true,
      persistent: false,
      keySource: "ephemeral-process",
      messagesEncrypted: true,
      messagesPersistent: false,
    });
    (window as any).__MQB_DESKTOP__ = false;
    (window as any).mqbAlert = vi.fn().mockResolvedValue(undefined);
    (window as any).mqbConfirm = vi.fn().mockResolvedValue(true);
  });

  afterEach(() => {
    document.body.innerHTML = "";
    (window as any).__MQB_DESKTOP__ = false;
  });

  test("renders the storage notice above the form", () => {
    const { target, instance } = mountSettingsPanel();

    expect(target.querySelector("#settings-security-banner")?.textContent).toContain(
      "Cached message history is encrypted for this session and cleared after restart.",
    );
    expect(target.querySelector("#storage-security-note")).toBeNull();
    expect(target.querySelector("#storage-mode-note")).toBeNull();

    unmount(instance);
  });

  test("renders desktop secret actions directly in the template", () => {
    (window as any).__MQB_DESKTOP__ = true;

    const { target, instance } = mountSettingsPanel();

    expect(target.querySelector("#js-check-desktop-secrets")?.textContent).toContain("Check Stored Secrets");
    expect(target.querySelector("#js-delete-desktop-secrets")?.textContent).toContain("Delete Stored Secrets");

    unmount(instance);
  });

  test("handles desktop secret checks from the Svelte action bar", async () => {
    (window as any).__MQB_DESKTOP__ = true;
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          routes: {
            route_a: [{ key: "token", extracted: true, stored: true }],
          },
        }),
      }),
    );

    const { target, instance } = mountSettingsPanel();
    const checkButton = target.querySelector("#js-check-desktop-secrets") as HTMLButtonElement | null;
    expect(checkButton).not.toBeNull();

    checkButton?.click();
    await new Promise((resolve) => window.setTimeout(resolve, 0));

    expect(fetch).toHaveBeenCalledWith("/desktop-secrets", { cache: "no-store" });
    expect((window as any).mqbAlert).toHaveBeenCalledWith(
      "Routes:\n- route_a: 1/1 extracted, 1/1 stored",
      "Stored Secrets",
    );

    vi.unstubAllGlobals();
    unmount(instance);
  });
});
