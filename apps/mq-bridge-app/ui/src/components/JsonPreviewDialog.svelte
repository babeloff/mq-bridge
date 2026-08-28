<script lang="ts">
  import "@awesome.me/webawesome/dist/components/button/button.js";
  import "@awesome.me/webawesome/dist/components/dialog/dialog.js";
  import { tick, onDestroy } from "svelte";
  import { EditorView, basicSetup } from "codemirror";
  import { json } from "@codemirror/lang-json";
  import type { ConfigJsonVariant } from "../lib/import-export";

  let {
    open = false,
    title = "JSON",
    value = "",
    variants = [],
    onClose = () => {},
  }: {
    open?: boolean;
    title?: string;
    value?: string;
    variants?: ConfigJsonVariant[];
    onClose?: () => void;
  } = $props();

  let editorContainer = $state<HTMLElement | null>(null);
  let copyLabel = $state("Copy");
  let selectedVariantId = $state<string | null>(null);
  let editorView: EditorView | null = null;
  let copyTimer: ReturnType<typeof setTimeout> | null = null;

  const activeVariant = $derived(
    variants.length > 0 ? (variants.find((variant) => variant.id === selectedVariantId) ?? variants[0]) : null,
  );
  const displayValue = $derived(activeVariant ? activeVariant.value : value);

  $effect(() => {
    if (open) {
      void renderJson(displayValue);
    }
  });

  async function renderJson(doc: string) {
    await tick();
    if (!editorContainer) return;
    if (!editorView) {
      editorView = new EditorView({
        doc,
        extensions: [
          basicSetup,
          json(),
          EditorView.editable.of(false),
          EditorView.theme({
            "&": { height: "min(64vh, 680px)", fontSize: "13px" },
            ".cm-scroller": { overflow: "auto" },
          }),
        ],
        parent: editorContainer,
      });
      return;
    }
    editorView.dispatch({
      changes: { from: 0, to: editorView.state.doc.length, insert: doc },
    });
  }

  async function copyJson() {
    try {
      await navigator.clipboard.writeText(displayValue);
      copyLabel = "Copied";
    } catch (error) {
      console.error("Failed to copy JSON to clipboard:", error);
      copyLabel = "Copy failed";
    }
    if (copyTimer) clearTimeout(copyTimer);
    copyTimer = setTimeout(() => {
      copyLabel = "Copy";
      copyTimer = null;
    }, 1200);
  }

  onDestroy(() => {
    if (copyTimer) {
      clearTimeout(copyTimer);
      copyTimer = null;
    }
    editorView?.destroy();
    editorView = null;
  });
</script>

<wa-dialog label={title} open={open} class="json-preview-dialog" onwa-hide={onClose}>
  <div bind:this={editorContainer} class="json-preview-container"></div>
  <div slot="footer" class="json-preview-actions">
    {#if variants.length > 1}
      <div class="json-preview-variants" role="group" aria-label="Format">
        {#each variants as variant (variant.id)}
          <wa-button
            size="small"
            variant={activeVariant?.id === variant.id ? "brand" : "neutral"}
            appearance={activeVariant?.id === variant.id ? "filled" : "outlined"}
            role="button"
            tabindex="0"
            onclick={() => (selectedVariantId = variant.id)}
            onkeydown={(event: KeyboardEvent) => event.key === "Enter" && (selectedVariantId = variant.id)}
            >{variant.label}</wa-button
          >
        {/each}
      </div>
    {/if}
    <div class="json-preview-actions-right">
      <wa-button
        variant="neutral"
        appearance="outlined"
        role="button"
        tabindex="0"
        onclick={() => void copyJson()}
        onkeydown={(event: KeyboardEvent) => event.key === "Enter" && void copyJson()}>{copyLabel}</wa-button
      >
      <wa-button
        variant="brand"
        role="button"
        tabindex="0"
        onclick={onClose}
        onkeydown={(event: KeyboardEvent) => event.key === "Enter" && onClose()}>Close</wa-button
      >
    </div>
  </div>
</wa-dialog>

<style>
  :global(wa-dialog.json-preview-dialog::part(panel)) {
    width: min(920px, calc(100vw - 32px));
  }

  .json-preview-container {
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg-editor);
    overflow: hidden;
  }

  .json-preview-actions {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
  }

  .json-preview-variants {
    display: flex;
    gap: 6px;
  }

  .json-preview-actions-right {
    display: flex;
    gap: 8px;
    margin-left: auto;
  }

  :global(.cm-editor) {
    outline: none !important;
  }
</style>
