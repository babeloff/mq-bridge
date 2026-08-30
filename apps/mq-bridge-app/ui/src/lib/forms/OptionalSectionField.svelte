<script lang="ts">
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    label: string;
    description?: string;
    checked?: boolean;
    inputId?: string;
    onToggle?: (checked: boolean) => void;
    content: Node | null;
  }

  let {
    label,
    description = "",
    checked = false,
    inputId,
    onToggle,
    content,
  }: Props = $props();

  let expanded = $state(false);
  let initialized = false;

  $effect(() => {
    if (initialized) return;
    expanded = checked;
    initialized = true;
  });
</script>

<fieldset class="mqb-inline-editor mqb-optional-section">
  <legend>{label}</legend>

  <div class="mqb-checkbox-row mqb-checkbox-row--required">
    <div class="mqb-checkbox-control">
      <input
        id={inputId}
        class="wa-checkbox"
        type="checkbox"
        bind:checked={expanded}
        onchange={() => onToggle?.(expanded)}
      />
    </div>
    <div class="mqb-checkbox-copy">
      <label class="mqb-checkbox-label" for={inputId}>{label}</label>
      {#if description}
        <div class="mqb-checkbox-description">{description}</div>
      {/if}
    </div>
  </div>

  {#if expanded}
    <div class="mqb-optional-section-content">
      <MountedNode node={content} />
    </div>
  {/if}
</fieldset>
