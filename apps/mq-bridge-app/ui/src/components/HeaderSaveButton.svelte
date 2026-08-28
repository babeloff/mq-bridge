<script lang="ts">
  import { saveWorkspace } from "../bootstrap";
  import { workspaceDirtyStore, workspaceSavingStore } from "../lib/stores";

  const idleLabel = "Save workspace";
  const dirtyLabel = "Save changes";

  function handleClick(event: MouseEvent) {
    void saveWorkspace(event.currentTarget as HTMLElement);
  }
</script>

<button
  id="workspace-save-button"
  type="button"
  class:dirty={$workspaceDirtyStore}
  class="header-save-button"
  data-icon-only="true"
  disabled={$workspaceSavingStore}
  title={$workspaceDirtyStore ? dirtyLabel : idleLabel}
  aria-label={$workspaceDirtyStore ? dirtyLabel : idleLabel}
  onclick={handleClick}
>
  <svg class="save-icon" viewBox="0 0 16 16" width="24" height="18" aria-hidden="true">
  
  <path
    fill="currentColor"
    d="
      M13.9 4.6 11.4 2.1
      C11.3 2.1 11.1 2 11 2H3
      C2.4 2 2 2.4 2 3v10
      c0 .6.4 1 1 1h10
      c.6 0 1-.4 1-1V5
      c0-.1-.1-.3-.1-.4z
      M6 3h4v2H6V3z
      M10 13H6V9h4v4z
      M11 13V9
      c0-.6-.4-1-1-1H6
      c-.6 0-1 .4-1 1v4H3V3h2v2
      c0 .6.4 1 1 1h4
      c.6 0 1-.4 1-1V3.2l2 2V13h-2z
    "
  />
  </svg>
  {#if $workspaceDirtyStore}
    <span class="dirty-dot" aria-hidden="true"></span>
  {/if}
  <span>Save</span>
</button>
