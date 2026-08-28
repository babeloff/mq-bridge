<script lang="ts">
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    content: Node | null;
    removable?: boolean;
    removeClassName: string;
  }

  let { content, removable = true, removeClassName }: Props = $props();
</script>

<div class="mqb-array-item-row">
  <div class="mqb-array-item-content">
    <MountedNode node={content} />
  </div>

  {#if removable}
    <button type="button" class={removeClassName}>Remove</button>
  {/if}
</div>
