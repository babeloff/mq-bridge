<script lang="ts">
  import { handleActionKey } from "../lib/utils";
  type HeaderRow = {
    id: number;
    key: string;
    value: string;
    enabled?: boolean;
  };

  let {
    rows,
    addLabel = "Add Header",
    keyPlaceholder = "Header name",
    valuePlaceholder = "Header value",
    showEnabled = false,
    onAdd,
    onUpdate,
    onToggle = () => {},
    onRemove,
  }: {
    rows: HeaderRow[];
    addLabel?: string;
    keyPlaceholder?: string;
    valuePlaceholder?: string;
    showEnabled?: boolean;
    onAdd: () => void;
    onUpdate: (index: number, field: "key" | "value", value: string) => void;
    onToggle?: (index: number, enabled: boolean) => void;
    onRemove: (index: number) => void;
  } = $props();

  function handleFieldInput(
    event: Event,
    index: number,
    field: "key" | "value",
  ) {
    onUpdate(index, field, (event.currentTarget as HTMLInputElement).value);
  }
</script>

<div class="response-editor-grid">
  <div>
    {#each rows as row, index (row.id)}
      <div class:response-header-row--toggle={showEnabled} class="response-header-row">
        {#if showEnabled}
          <label class="response-header-toggle" title="Enable header">
            <input
              type="checkbox"
              checked={row.enabled ?? true}
              onchange={(event) => onToggle(index, (event.currentTarget as HTMLInputElement).checked)}
            />
          </label>
        {/if}
        <input
          class="field-input"
          type="text"
          placeholder={keyPlaceholder}
          autocomplete="off"
          autocapitalize="off"
          spellcheck="false"
          value={row.key}
          onkeydown={(event) => event.stopPropagation()}
          oninput={(event) => handleFieldInput(event, index, "key")}
          onchange={(event) => handleFieldInput(event, index, "key")}
        />
        <input
          class="field-input"
          type="text"
          placeholder={valuePlaceholder}
          autocomplete="off"
          autocapitalize="off"
          spellcheck="false"
          value={row.value}
          onkeydown={(event) => event.stopPropagation()}
          oninput={(event) => handleFieldInput(event, index, "value")}
          onchange={(event) => handleFieldInput(event, index, "value")}
        />
        <wa-button
          class="cons-response-header-delete"
          variant="neutral"
          appearance="outlined"
          role="button"
          tabindex="0"
          onclick={() => onRemove(index)}
          onkeydown={(event: KeyboardEvent) => handleActionKey(event, () => onRemove(index))}
        >
          Delete
        </wa-button>
      </div>
    {/each}
  </div>
  <div class="response-editor-actions">
    <wa-button
      variant="neutral"
      appearance="outlined"
      role="button"
      tabindex="0"
      onclick={onAdd}
      onkeydown={(event: KeyboardEvent) => handleActionKey(event, onAdd)}
    >
      {addLabel}
    </wa-button>
  </div>
</div>
