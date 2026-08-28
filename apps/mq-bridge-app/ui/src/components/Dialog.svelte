<script lang="ts">
  import { onMount } from "svelte";
  import { handleActionKey } from "../lib/utils";
  import { activeDialog, closeDialog, type DialogChoice } from "../lib/dialogs";

  let {
    title = "",
    message = "",
    confirmLabel = "OK",
    cancelLabel = "",
    value = "",
    placeholder = "",
    mode = "message",
    choices = [],
  } = $derived(($activeDialog?.request ?? {}) as {
    title?: string;
    message?: string;
    confirmLabel?: string;
    cancelLabel?: string;
    value?: string;
    placeholder?: string;
    mode?: "message" | "confirm" | "prompt" | "choose";
    choices?: DialogChoice[];
  });

  let dialogEl: any = $state();
  let inputValue = $state("");
  $effect(() => {
    // Update inputValue if the prop 'value' changes, e.g., if the dialog is reused
    inputValue = value;
  });
  let isOpen = $state(false);

  onMount(() => {
    // Trigger open after mount for animation
    requestAnimationFrame(() => {
      isOpen = true;
    });
  });

  function handleClose(result: any) {
    closeDialog(result, $activeDialog?.id);
  }

  function createAfterHideHandler(dialogId: number) {
    return () => closeDialog(null, dialogId);
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Enter" && mode === "prompt") {
      handleClose(inputValue.trim());
    }
  }

  function useFocus(node: any) {
    node.focus(); 
    node.select(); 
  }
</script>

{#if $activeDialog}
  {#key $activeDialog.id}
    <wa-dialog
      bind:this={dialogEl}
      label={title}
      open={isOpen}
      class="mqb-dialog"
      onwa-after-hide={createAfterHideHandler($activeDialog.id)}
    >
      <div class="mqb-dialog-body">
        <div class="mqb-message">{message}</div>

      {#if mode === "prompt"}
        <input
          type="text"
          class="mqb-dialog-input"
          bind:value={inputValue}
          placeholder={placeholder}
          onkeydown={handleKeydown}
          use:useFocus
        />
      {/if}

      {#if mode === "choose"}
        <div class="mqb-choice-list">
          {#each choices as choice (choice.value)}
            <div class="mqb-choice-row">
              <wa-button
                variant="neutral"
                appearance="outlined"
                role="button"
                tabindex="0"
                onclick={() => handleClose(choice.value)}
                onkeydown={(e: KeyboardEvent) => handleActionKey(e, () => handleClose(choice.value))}
              >
                {choice.label}
              </wa-button>
              {#if choice.description}
                <span class="mqb-choice-description">{choice.description}</span>
              {/if}
            </div>
          {/each}
        </div>
      {/if}
      <div class="mqb-dialog-actions">
        {#if mode !== "choose"}
          {#if cancelLabel}
            <wa-button
              variant="neutral"
              appearance="outlined"
              role="button"
              tabindex="0"
              onclick={() => handleClose(mode === "confirm" ? false : null)}
              onkeydown={(e: KeyboardEvent) => handleActionKey(e, () => handleClose(mode === "confirm" ? false : null))}
            >
              {cancelLabel}
            </wa-button>
          {/if}
          <wa-button
            variant="brand"
            role="button"
            tabindex="0"
            onclick={() => handleClose(mode === "prompt" ? inputValue.trim() : true)}
            onkeydown={(e: KeyboardEvent) => handleActionKey(e, () => handleClose(mode === "prompt" ? inputValue.trim() : true))}
          >
            {confirmLabel}
          </wa-button>
        {:else if cancelLabel}
          <wa-button
            variant="neutral"
            appearance="outlined"
            role="button"
            tabindex="0"
            onclick={() => handleClose(null)}
            onkeydown={(e: KeyboardEvent) => handleActionKey(e, () => handleClose(null))}
          >
            {cancelLabel}
          </wa-button>
        {/if}
      </div>
      </div>
    </wa-dialog>
  {/key}
{/if}

<style>
  :global(wa-dialog.mqb-dialog::part(panel)) {
    min-width: min(520px, calc(100vw - 32px));
  }
  .mqb-dialog-body {
    display: grid;
    gap: 12px;
    color: var(--text-muted);
    line-height: 1.4;
  }
  .mqb-message {
    color: var(--text-primary);
  }
  .mqb-dialog-input {
    width: 100%;
    box-sizing: border-box;
    padding: 8px 12px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg-input);
    color: var(--text-primary);
    font: inherit;
  }
  .mqb-choice-list {
    display: grid;
    gap: 8px;
    margin-top: 4px;
  }
  .mqb-choice-row {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .mqb-choice-description {
    font-size: 13px;
    color: var(--text-dim);
    flex: 1;
  }
  .mqb-dialog-actions {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    margin-top: 8px;
  }
</style>
