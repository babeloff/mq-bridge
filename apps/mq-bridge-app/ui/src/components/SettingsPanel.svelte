<script lang="ts">
  import '@awesome.me/webawesome/dist/components/callout/callout.js';
  import { tick } from "svelte";
  import { activeMainTab, storageSecurityStore } from "../lib/stores";
  import { exportFullBundle, importAppConfigFromJsonText, resetAppConfigToDefaults } from "../lib/import-export";
  import { withSelectedFileText } from "../lib/utils";
  import { appShell } from "../lib/app-shell";
  import { browserWindow } from "../lib/browser";
  import { alertDialog, confirmDialog } from "../lib/dialogs";
  import type { StorageSecurityInfo } from "../lib/storage-security";
  import { formatDesktopSecretsSummary } from "../lib/settings";
  import { EditorView, basicSetup } from "codemirror";
  import { json } from "@codemirror/lang-json";

  let isJsonModalOpen = $state(false);
  let editorContainer = $state<HTMLElement | null>(null);
  let editorView: EditorView | null = null;
  let importInputEl = $state<HTMLInputElement | null>(null);
  const isDesktop = appShell.isDesktop();
  const storageSecurity = $derived($storageSecurityStore);
  const storageNotice = $derived(getStorageNotice(storageSecurity));

  function openImportPicker() {
    importInputEl?.click();
  }

  async function handleImportSelected(event: Event) {
    try {
      await withSelectedFileText(event, async (text) => {
        const result = await importAppConfigFromJsonText(text);
        await alertDialog(
          `Imported ${result.importedPublishers} publishers and ${result.importedConsumers} consumers.`,
          "Import complete",
        );
        window.location.reload();
      });
    } catch (error) {
      await alertDialog(`Import failed: ${(error as Error).message}`, "Import");
    }
  }

  async function resetConfig() {
    const confirmed = await confirmDialog(
      "Reset publishers and consumers? Existing entries will be removed.",
      "Reset App Config",
    );
    if (!confirmed) return;
    await resetAppConfigToDefaults();
    window.location.reload();
  }

  async function openJsonModal() {
    isJsonModalOpen = true;
    const configData = JSON.stringify(appShell.config(), null, 2);

    await tick();
    if (!editorContainer) return;
    if (!editorView) {
      editorView = new EditorView({
        doc: configData,
        extensions: [
          basicSetup,
          json(),
          EditorView.editable.of(false),
          EditorView.theme({
            "&": { height: "60vh", fontSize: "13px" },
            ".cm-scroller": { overflow: "auto" },
          }),
        ],
        parent: editorContainer,
      });
    } else {
      editorView.dispatch({
        changes: { from: 0, to: editorView.state.doc.length, insert: configData },
      });
    }
  }

  async function checkStoredSecrets() {
    const alert = browserWindow().mqbAlert || alertDialog;
    try {
      const response = await fetch("/desktop-secrets", { cache: "no-store" });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || "Failed to inspect stored secrets");
      }

      const summary = await response.json();
      await alert(formatDesktopSecretsSummary(summary), "Stored Secrets");
    } catch (error) {
      await alert(`Failed to inspect stored secrets: ${(error as Error).message}`, "Stored Secrets");
    }
  }

  async function deleteStoredSecrets() {
    try {
      const confirmed = await confirmDialog(
        "Delete all securely stored secrets referenced by the current desktop config?",
        "Delete Stored Secrets",
      );
      if (!confirmed) {
        return;
      }

      const response = await fetch("/desktop-secrets", { method: "DELETE" });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || "Failed to delete stored secrets");
      }

      const result = await response.json().catch(() => ({ deleted: 0 }));
      const deleted = Number(result?.deleted || 0);
      await alertDialog(
        deleted > 0
          ? `Deleted ${deleted} stored secret${deleted === 1 ? "" : "s"}.`
          : "No stored secrets were found for the current desktop config.",
        "Stored Secrets",
      );
    } catch (error) {
      await alertDialog(`Failed to delete stored secrets: ${(error as Error).message}`, "Stored Secrets");
    }
  }

  function getStorageNotice(info: StorageSecurityInfo) {
    if (info.messagesEncrypted && info.messagesPersistent) {
      return "Cached message history is encrypted and restored after restart on this machine.";
    }
    if (info.messagesEncrypted) {
      return "Cached message history is encrypted for this session and cleared after restart.";
    }
    if (info.reason === "key-store-unavailable") {
      return "No OS key store is available, so persistent encrypted storage is not available on this machine.";
    }
    return "Cached message history is stored on disk without encryption in this mode.";
  }
</script>

<div class:active={$activeMainTab === "config"} class="tab-content-panel" id="tab-config">
  <div id="form-tab-wrapper">
    <div
      class="settings-security-banner"
      id="settings-security-banner"
      data-test-id="js-storage-security-note"
    >
      {storageNotice}
    </div>
    <div
      id="form-actions"
      class="section-toolbar editor-action-bar editor-action-bar--config editor-action-bar--compact settings-action-bar-initial"
    >
      <div class="form-actions-row section-actions">
        <div class="section-actions-right">
          <div class="editor-action-cluster">
            <button
              class="wa-native-button wa-native-button--neutral"
              type="button"
              title="Export app config + env vars"
              onclick={exportFullBundle}>Export</button
            >
            <button
              class="wa-native-button wa-native-button--neutral"
              type="button"
              title="Import app config and merge data"
              onclick={openImportPicker}>Import</button
            >
            <button
              class="wa-native-button wa-native-button--danger"
              type="button"
              title="Reset publishers and consumers"
              onclick={resetConfig}>Reset</button
            >
            <input bind:this={importInputEl} type="file" accept=".json,application/json" class="hidden-file-input" onchange={handleImportSelected} />
            <button
              class="wa-native-button wa-native-button--neutral"
              id="js-show-json"
              type="button"
              title="Show current configuration as JSON"
              onclick={openJsonModal}>{`{?} JSON`}</button
            >
            {#if isDesktop}
              <button
                class="wa-native-button wa-native-button--neutral"
                id="js-check-desktop-secrets"
                type="button"
                title="Inspect securely stored secrets referenced by this config"
                onclick={checkStoredSecrets}>Check Stored Secrets</button
              >
              <button
                class="wa-native-button wa-native-button--danger"
                id="js-delete-desktop-secrets"
                type="button"
                title="Delete securely stored secrets referenced by this config"
                onclick={deleteStoredSecrets}>Delete Stored Secrets</button
              >
            {/if}
          </div>
        </div>
      </div>
    </div> 
  </div>
  <div class="form-scroll-wrapper">
    <div id="form-container" class="field-grid"></div>
  </div>
</div>

<wa-dialog label="Current Configuration (JSON)" open={isJsonModalOpen} onwa-hide={() => (isJsonModalOpen = false)}>
  <div bind:this={editorContainer} class="json-preview-container"></div>
  <wa-button slot="footer" variant="brand" 
    role="button"
    tabindex="0"
    onclick={() => (isJsonModalOpen = false)} 
    onkeydown={(e) => e.key === 'Enter' && (isJsonModalOpen = false)}>Close</wa-button>
</wa-dialog>

<style>
  .json-preview-container {
    border: 1px solid var(--wa-color-neutral-border);
    border-radius: var(--wa-border-radius-medium);
    background: var(--wa-color-neutral-surface);
    overflow: hidden;
  }

  :global(.cm-editor) {
    outline: none !important;
  }

  .settings-security-banner {
    color: var(--text-secondary);
    font-size: 12px;
    line-height: 1.45;
    padding: 0.5rem 0.75rem 0.75rem;
  }
  #form-tab-wrapper {
    position: relative;
  }

  .settings-action-bar-initial,
  .hidden-file-input {
    display: none;
  }
</style>
