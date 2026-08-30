<script lang="ts">
  import type { Snippet } from "svelte";
  import CodeEditor from "./CodeEditor.svelte";
  import { formatPayload, type PayloadViewMode } from "../lib/payload-format";

  export type { PayloadViewMode };

  let {
    id = "",
    payload = "",
    contentType = "",
    readOnly = true,
    showPretty = false,
    label = "",
    onChange = (_value: string) => {},
    extra,
  }: {
    id?: string;
    payload?: string | Uint8Array | null;
    contentType?: string;
    readOnly?: boolean;
    showPretty?: boolean;
    label?: string;
    onChange?: (value: string) => void;
    extra?: Snippet;
  } = $props();

  let currentViewMode = $state<PayloadViewMode>("auto");
  let showWhitespace = $state(false);
  let showLineEndings = $state(false);
  let wrapLines = $state(true);
  let isFocused = $state(false);
  let prettyJson = $state(false);

  // Simple XML beautifier as a fallback since the core formatter doesn't seem to handle it yet
  function beautifyXml(xml: string) {
    const PADDING = '  ';
    // Remove existing whitespace between tags to normalize before formatting
    const cleanXml = xml.replace(/>\s*</g, '><').trim();
    const reg = /(>)(<)(\/*)/g;
    let pad = 0;
    const formatted = cleanXml.replace(reg, '$1\n$2$3');
    const lines = formatted.split('\n');
    let result = '';
    for (let node of lines) {
      let indent = 0;
      if (node.match(/.+<\/\w[^>]*>$/)) {
        indent = 0;
      } else if (node.match(/^<\/\w/)) {
        if (pad !== 0) pad -= 1;
      } else if (node.match(/^<\w[^>]*[^\/]>.*$/)) {
        indent = 1;
      } else {
        indent = 0;
      }

      result += PADDING.repeat(pad) + node + '\n';
      pad += indent;
    }
    return result.trim();
  }

  // Helper to convert the human-readable hex editor text back to its decoded string representation
  function parseHexViewToText(input: string): string | null {
    const lines = input.split(/\r?\n/);
    let cleanedHex = "";
    for (const line of lines) {
      let hexPart = line.split("|")[0] || "";
      // Strip address prefix (e.g., "00000000: ")
      hexPart = hexPart.replace(/^[0-9a-fA-F]+:\s*/, "");
      cleanedHex += hexPart.replace(/\s/g, "");
    }

    if (cleanedHex === "") return "";

    // Only process even length (complete byte pairs)
    const validHex = cleanedHex.length % 2 === 0 ? cleanedHex : cleanedHex.slice(0, -1);
    if (!validHex) return null;
    const hexBytes = validHex.match(/.{1,2}/g) || [];
    return new TextDecoder().decode(new Uint8Array(hexBytes.map(byte => parseInt(byte, 16))));
  }

  let formattedPayload = $derived.by(() => {
    const base = formatPayload(payload, currentViewMode, contentType);
    return base;
  });

  // New state to hold user's raw hex input when actively editing
  let userHexInput: string | null = $state(null);
  let isEditingHex = $state(false);
  let displayedPayload = $derived.by(() => {
    if (isEditingHex && userHexInput !== null) return userHexInput;
    if ((prettyJson && formattedPayload.language === "json") || currentViewMode === "hex") {
      return formattedPayload.formatted;
    }
    return typeof payload === "string" ? payload : payload ? new TextDecoder().decode(payload) : "";
  });

  function handleEditorChange(newValue: string) {
    if (readOnly) return;

    if (currentViewMode === "hex") {
      isEditingHex = true;
      userHexInput = newValue;
      const text = parseHexViewToText(newValue);
      if (text !== null) onChange(text);
    } else {
      isEditingHex = false;
      userHexInput = null;
      onChange(newValue);
    }
  }

  function handleEditorBlur() {
    if (currentViewMode === "hex" && isEditingHex && userHexInput !== null && !readOnly) {
      const text = parseHexViewToText(userHexInput);
      if (text !== null) onChange(text);
    }
    isEditingHex = false;
    userHexInput = null;
  }

  function handleBeautify() {
    if (readOnly) return;
    let text = "";
    if (isEditingHex && userHexInput !== null) {
      text = parseHexViewToText(userHexInput) || "";
    } else {
      // Decode binary payload if necessary
      const data = payload;
      text = typeof data === 'string' ? data : (data ? new TextDecoder().decode(data) : '');
    }
    if (!text) return;

    // Use current view mode as primary language indicator, fall back to detection
    const lang = (currentViewMode === 'json' || currentViewMode === 'xml') 
      ? currentViewMode 
      : formattedPayload.language;
    const lowerLang = (lang || '').toLowerCase();

    if (lowerLang === 'xml') {
      onChange(beautifyXml(text));
      currentViewMode = 'xml';
    } else if (lowerLang === 'json' || lowerLang === 'json-auto') {
      try { 
        onChange(JSON.stringify(JSON.parse(text), null, 2)); 
        currentViewMode = 'json';
      } catch (e) {}
    }
    isEditingHex = false;
    userHexInput = null;
  }

  function handlePrettyJson() {
    currentViewMode = "json";
    prettyJson = true;
    isEditingHex = false;
    userHexInput = null;
  }

  function setViewMode(mode: PayloadViewMode) {
    currentViewMode = mode;
    prettyJson = false;
    isEditingHex = false;
    userHexInput = null;
  }

  function handleImportFile() {
    const input = document.createElement('input');
    input.type = 'file';
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;
      const buffer = await file.arrayBuffer();
      const bytes = new Uint8Array(buffer);
      const decodedString = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
      onChange(decodedString);
      currentViewMode = "text";
    };
    input.click();
  }

  function handleExportFile() {
    const data = typeof payload === 'string' ? new TextEncoder().encode(payload) : (payload || new Uint8Array());
    const blob = new Blob([data], { type: contentType || 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `payload-${id || 'data'}.bin`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handleCopyBinary() {
    const bytes = typeof payload === 'string' ? new TextEncoder().encode(payload) : (payload || new Uint8Array());
    const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
    try {
      await navigator.clipboard.writeText(hex);
    } catch (err) {
      console.error("Failed to copy binary hex", err);
    }
  }
</script>

<div {id} class="payload-display-container">
  <div class="payload-toolbar">
    <div class="view-mode-selector">
      {#if label}<span class="toolbar-label">{label}</span>{/if}
      <button class:active={currentViewMode === 'auto'} type="button" onclick={() => setViewMode('auto')}>Auto</button>
      <button class:active={currentViewMode === 'text'} type="button" onclick={() => setViewMode('text')}>Text</button>
      <button class:active={currentViewMode === 'json'} type="button" onclick={() => setViewMode('json')}>JSON</button>
      <button class:active={currentViewMode === 'xml'} type="button" onclick={() => setViewMode('xml')}>XML</button>
      <button class:active={currentViewMode === 'hex'} type="button" onclick={() => setViewMode('hex')}>Hex</button>
    </div>

    <div class="display-options">
      <label title="Show whitespace">
        <input type="checkbox" bind:checked={showWhitespace} /> <span>Whitespace</span>
      </label>
      <label title="Show line endings">
        <input type="checkbox" bind:checked={showLineEndings} /> <span>Endings</span>
      </label>
      <label title="Toggle line wrapping">
        <input type="checkbox" bind:checked={wrapLines} /> <span>Wrap</span>
      </label>

      <div class="toolbar-divider"></div>
      {#if showPretty && formattedPayload.language === 'json'}
        <button class="toolbar-btn" type="button" onclick={handlePrettyJson} title="Pretty-print JSON content">
          Pretty
        </button>
      {/if}
      {#if !readOnly && (formattedPayload.language?.includes('json') || formattedPayload.language?.includes('xml'))}
        <button class="toolbar-btn" type="button" onclick={handleBeautify} title="Format JSON/XML content">
          Beautify
        </button>
      {/if}
      {@render extra?.()}
      <button class="toolbar-btn" type="button" onclick={handleCopyBinary} title="Copy as hex digits">
        Copy Bin
      </button>
      <button class="toolbar-btn" type="button" onclick={handleExportFile} title="Save to file">
        Export
      </button>
      {#if !readOnly}
        <button class="toolbar-btn" type="button" onclick={handleImportFile} title="Load from file">
          Import
        </button>
      {/if}
    </div>
  </div>

  <div class="payload-content">
    <CodeEditor
      id={id ? `${id}-editor` : ""}
      value={displayedPayload}
      language={currentViewMode === 'auto' ? 'json-auto' : 
                (currentViewMode === 'hex' ? 'text' : currentViewMode)}
      useLinter={currentViewMode === 'json' || currentViewMode === 'xml'}
      onChange={handleEditorChange}
      onBlur={() => { isFocused = false; handleEditorBlur(); }}
      onFocus={() => isFocused = true}
      readOnly={readOnly}
      wrapLines={wrapLines}
      showWhitespace={showWhitespace}
      showLineEndings={showLineEndings}
    />
  </div>
</div>

<style>
  .payload-display-container {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
  }

  .payload-toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px 4px;
    background-color: var(--bg-secondary);
    border-bottom: 1px solid var(--border-color);
    gap: 8px;
    font-family: var(--font-ui);
  }

  .view-mode-selector, .display-options {
    display: flex;
    align-items: center;
    gap: 4px;
  }

  .toolbar-label {
    font-size: 10px;
    font-weight: bold;
    color: var(--text-dim);
    text-transform: uppercase;
    margin-right: 4px;
  }

  .view-mode-selector button, .toolbar-btn {
    background-color: var(--bg-primary);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
    padding: 2px 6px;
    border-radius: 3px;
    cursor: pointer;
    font-size: 9px;
    text-transform: uppercase;
    font-weight: 500;
    transition: all 0.1s ease;
  }

  .view-mode-selector button.active {
    background-color: var(--accent-color) !important;
    color: var(--text-on-accent);
    border-color: var(--accent-color-dark, var(--accent-color));
    box-shadow: inset 0 1px 2px var(--shadow-color);;
    font-weight: 700;
  }

  .display-options label {
    font-size: 9px;
    display: inline-flex;
    align-items: center;
    gap: 3px;
    cursor: pointer;
    color: var(--text-dim);
    text-transform: uppercase;
    user-select: none;
    white-space: nowrap; /* Prevent labels from wrapping */
    min-width: 60px; /* Ensure consistent width to prevent movement */
    padding: 0 2px; /* Small horizontal padding for labels */
  }

  .display-options input[type="checkbox"] {
    margin: 0;
    width: 11px;
    height: 11px;
  }

  .toolbar-divider {
    width: 1px;
    height: 12px;
    background-color: var(--border-color);
    margin: 0 4px;
  }

  .payload-content {
    flex: 1;
    overflow: auto;
    background-color: var(--bg-editor);
    color: var(--text-payload);
    min-height: 0;
    border: 1px solid var(--border-color);
    border-top: none;
   }
</style>
