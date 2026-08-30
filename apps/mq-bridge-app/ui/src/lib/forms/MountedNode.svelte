<script lang="ts">
  interface Props {
    node: Node | null;
    className?: string;
  }

  let { node, className = "" }: Props = $props();
  let host = $state<HTMLDivElement | null>(null);

  $effect(() => {
    if (!host) return;
    host.replaceChildren();
    if (node) {
      host.appendChild(node);
    }
  });
</script>

<div bind:this={host} class={className}></div>
