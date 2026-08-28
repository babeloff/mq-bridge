import type { InstanceStatus, StatusEntity, StatusSummary } from "./generated/ui-types";
import type { PeerStatus } from "./runtime-status";

export interface PeerSidebarRow {
  id: string;
  instanceId: string;
  instanceLabel: string;
  workspaceLabel: string;
  kind: string;
  label: string;
  endpoint: string;
  running: boolean;
  healthy: boolean;
  throughput: number;
}

function instanceLabel(instance: InstanceStatus): string {
  const kind = instance.kind === "web-ui" ? "Web UI" : instance.kind.toUpperCase();
  return `${kind} · ${instance.workspace_label || "workspace"}`;
}

function row(instance: InstanceStatus, entity: StatusEntity, prefix: string): PeerSidebarRow {
  return {
    id: `${instance.instance_id}:${prefix}:${entity.id}`,
    instanceId: instance.instance_id,
    instanceLabel: instanceLabel(instance),
    workspaceLabel: instance.workspace_label,
    kind: instance.kind,
    label: entity.label || entity.id,
    endpoint: entity.endpoint || "unknown",
    running: Boolean(entity.summary?.running),
    healthy: entity.summary?.healthy !== false,
    throughput: Number(entity.summary?.throughput || 0),
  };
}

function instancesForDisplay(status: PeerStatus): InstanceStatus[] {
  // Locally configured rows already represent the current instance. The
  // registry still returns it (for Tauri and diagnostics), but only other
  // instances are rendered in the peer section to avoid duplicate controls.
  return status.instances.filter((instance) => instance.instance_id !== status.current_instance_id);
}

export function peerConsumerRows(status: PeerStatus): PeerSidebarRow[] {
  return instancesForDisplay(status).flatMap((instance) => [
    ...(instance.consumers || []).map((entity) => row(instance, entity, "consumer")),
    ...(instance.routes || []).map((route) => row(instance, route.input, `route-input:${route.id}`)),
  ]);
}

export function peerPublisherRows(status: PeerStatus): PeerSidebarRow[] {
  return instancesForDisplay(status).flatMap((instance) => [
    ...(instance.publishers || []).map((entity) => row(instance, entity, "publisher")),
    ...(instance.routes || []).map((route) => row(instance, route.output, `route-output:${route.id}`)),
  ]);
}

export interface PeerActivity {
  /** Instances other than this one that currently hold a lease. */
  instances: number;
  running: number;
  throughput: number;
}

/** What the other instances are doing, for the top bar's peer indicator. */
export function peerActivity(status: PeerStatus): PeerActivity {
  const instances = instancesForDisplay(status);
  let running = 0;
  let throughput = 0;
  for (const instance of instances) {
    // A route counts once here, unlike the sidebar where it appears as both an
    // input row and an output row.
    const summaries: (StatusSummary | undefined)[] = [
      ...(instance.consumers || []).map((entity) => entity.summary),
      ...(instance.routes || []).map((route) => route.summary),
    ];
    for (const summary of summaries) {
      if (summary?.running) running += 1;
      throughput += Number(summary?.throughput || 0);
    }
  }
  return { instances: instances.length, running, throughput };
}

export function peerActivityLabel(activity: PeerActivity): string {
  const parts = [
    `${activity.instances} other instance${activity.instances === 1 ? "" : "s"}`,
  ];
  if (activity.running > 0) parts.push(`${activity.running} running`);
  if (activity.throughput > 0) parts.push(`${activity.throughput.toFixed(1)} msg/s`);
  return parts.join(" • ");
}

/** Applies a sidebar's free-text filter to peer rows. */
export function filterPeerRows(rows: PeerSidebarRow[], filterText: string): PeerSidebarRow[] {
  const needle = filterText.trim().toLowerCase();
  if (!needle) return rows;
  return rows.filter((row) =>
    `${row.label} ${row.endpoint} ${row.instanceLabel}`.toLowerCase().includes(needle),
  );
}
