<script lang="ts">
  import { withSelectedFileText } from "../lib/utils";

  type ImportAction = {
    key: string;
    label: string;
  };

  interface Props {
    actions: ImportAction[];
    onImport: (key: string, text: string) => Promise<void> | void;
  }

  let { actions, onImport }: Props = $props();
  let importInputEl = $state<HTMLInputElement | null>(null);
  let selectedActionKey = $state("");

  function openImportPicker(actionKey: string) {
    selectedActionKey = actionKey;
    importInputEl?.click();
  }

  async function handleImportSelected(event: Event) {
    await withSelectedFileText(event, async (text) => {
      await onImport(selectedActionKey, text);
    });
  }
</script>

<div class="sidebar-import-actions">
  {#each actions as action (action.key)}
    <button
      class="wa-native-button wa-native-button--neutral sidebar-import-button"
      type="button"
      onclick={() => openImportPicker(action.key)}
    >
      {action.label}
    </button>
  {/each}
  <input
    bind:this={importInputEl}
    type="file"
    accept=".json,application/json"
    hidden
    class="hidden-file-input"
    onchange={handleImportSelected}
  />
</div>

<style>
  .hidden-file-input {
    display: none;
  }
</style>
