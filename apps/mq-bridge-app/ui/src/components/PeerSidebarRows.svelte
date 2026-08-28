<script lang="ts">
  import type { PeerSidebarRow } from "../lib/peer-status";

  interface Props {
    rows: PeerSidebarRow[];
    /** Extra class for the row, so each panel keeps its own item styling. */
    itemClass?: string;
  }

  const { rows, itemClass = "" }: Props = $props();
</script>

{#if rows.length > 0}
  <div class="sidebar-group-label peer-group-label">Other instances</div>
  {#each rows as row, i (row.id)}
    {#if i === 0 || rows[i - 1].instanceId !== row.instanceId}
      <div
        class="sidebar-peer-instance"
        title={`${row.instanceLabel} · workspace ${row.workspaceLabel}`}
      >
        {row.instanceLabel}
      </div>
    {/if}
    <div
      class={`sidebar-item sidebar-item--peer ${itemClass}`}
      title={`${row.label} · ${row.endpoint} · ${row.instanceLabel} · ${row.workspaceLabel}`}
      aria-label={`${row.label} from ${row.instanceLabel}`}
    >
      <span class="proto-badge proto-peer">{row.endpoint}</span>
      <span class="item-name">{row.label}</span>
      {#if row.throughput > 0}<span class="msg-count">{row.throughput.toFixed(1)} msg/s</span>{/if}
      <span
        class={`item-status peer-status ${row.running && row.healthy ? "status-ok" : "status-off"}`}
      ></span>
    </div>
  {/each}
{/if}
