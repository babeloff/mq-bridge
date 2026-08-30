<script lang="ts">
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    description?: string;
    visibleContent: Node | null;
    hiddenContent?: Node | null;
    toggleLabel?: string;
  }

  let {
    description = "",
    visibleContent,
    hiddenContent = null,
    toggleLabel = "Show more...",
  }: Props = $props();

  let expanded = $state(false);
</script>

<div class="mqb-form-block">
  {#if description}
    <div class="form-description form-description-block mqb-form-description-block">{description}</div>
  {/if}

  <MountedNode node={visibleContent} />

  {#if hiddenContent}
    <button
      type="button"
      class="wa-native-button wa-native-button--neutral mqb-form-toggle"
      aria-expanded={expanded}
      onclick={() => {
        expanded = !expanded;
      }}
    >
      {expanded ? "Hide advanced" : toggleLabel}
    </button>

    {#if expanded}
      <div class="form-advanced-block mqb-form-advanced-block">
        <MountedNode node={hiddenContent} />
      </div>
    {/if}
  {/if}
</div>
