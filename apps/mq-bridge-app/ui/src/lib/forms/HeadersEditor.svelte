<script lang="ts">
  // `updateRow` spreads the row, so callers may hang extra fields (e.g. the key a
  // row started with) off it and get them back unchanged through `onChange`.
  type HeaderItem = { key: string; value: string; [extra: string]: unknown };

  interface Props {
    title?: string;
    sectionLabel?: string;
    description?: string;
    keyPlaceholder?: string;
    valuePlaceholder?: string;
    addLabel?: string;
    emptyLabel?: string;
    deleteLabel?: string;
    rows: HeaderItem[];
    onChange: (next: HeaderItem[]) => void;
  }

  let {
    title = "Custom Headers",
    sectionLabel = "Headers",
    description = "",
    keyPlaceholder = "Header name",
    valuePlaceholder = "Header value",
    addLabel = "Add Header",
    emptyLabel = "No headers defined.",
    deleteLabel = "Delete",
    rows,
    onChange,
  }: Props = $props();

  function sync() {
    onChange(rows.filter((row) => row.key.trim().length > 0 || row.value.trim().length > 0));
  }

  function addRow() {
    rows = [...rows, { key: "", value: "" }];
    sync();
  }

  function removeRow(index: number) {
    rows = rows.filter((_, currentIndex) => currentIndex !== index);
    sync();
  }

  function updateRow(index: number, field: "key" | "value", value: string) {
    rows = rows.map((row, currentIndex) =>
      currentIndex === index ? { ...row, [field]: value } : row,
    );
    sync();
  }
</script>

<fieldset class="ui_custom_headers mqb-inline-editor">
  <legend>{title}</legend>

  {#if description}
    <div class="form-description form-description-block mqb-form-description-block">{description}</div>
  {/if}

  {#if sectionLabel}
    <div class="section-label">{sectionLabel}</div>
  {/if}
  <div class="response-editor-grid mqb-headers-grid">
    {#if rows.length === 0}
      <div class="mqb-empty-inline-note">{emptyLabel}</div>
    {/if}

    {#each rows as row, index}
      <div class="response-header-row mqb-header-row">
        <input
          class="field-input cons-response-header-key"
          type="text"
          placeholder={keyPlaceholder}
          value={row.key}
          oninput={(event) => updateRow(index, "key", (event.currentTarget as HTMLInputElement).value)}
        />
        <input
          class="field-input cons-response-header-value"
          type="text"
          placeholder={valuePlaceholder}
          value={row.value}
          oninput={(event) => updateRow(index, "value", (event.currentTarget as HTMLInputElement).value)}
        />
        <button
          type="button"
          class="wa-native-button wa-native-button--neutral cons-response-header-delete"
          onclick={() => removeRow(index)}
        >
          {deleteLabel}
        </button>
      </div>
    {/each}
  </div>

  <div class="response-editor-actions">
    <button type="button" class="wa-native-button wa-native-button--neutral" onclick={addRow}>{addLabel}</button>
  </div>
</fieldset>
