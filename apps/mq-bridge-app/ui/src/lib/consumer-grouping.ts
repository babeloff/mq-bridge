import { getEndpointType } from "./endpoint-utils";
import {
  addHttpLeafToBucket,
  buildNonHttpGroupInfo,
  createHttpBucket,
  displayUrlWithoutScheme,
  formatHttpSegments,
  isGroupNode,
  materializeBucketChildren,
  parseHttpPath,
  type GroupBucket,
  type GroupTreeGroup,
  type GroupTreeLeafBase,
  type GroupTreeNode,
  type MutableGroupLeaf,
} from "./grouping-utils";
import type { ConsumerConfig } from "./panel-types";
import { getEntityDisplayLabel } from "./utils";

export type ConsumerTreeItem = {
  name: string;
  displayName: string;
  inputProto: string;
  statusClass: string;
  messageCount: number;
  throughputLabel: string;
  originalIndex: number;
  id?: string;
};

export type ConsumerLeafNode = GroupTreeLeafBase<"consumer"> & {
  consumerIndex: number;
  statusClass: string;
  messageCount: number;
  throughputLabel: string;
};

export type ConsumerGroupNode = GroupTreeGroup<ConsumerLeafNode>;
export type ConsumerTreeNode = GroupTreeNode<ConsumerLeafNode>;

type MutableConsumerLeaf = MutableGroupLeaf<ConsumerLeafNode>;

function fallbackLeafId(consumer: ConsumerConfig, item: ConsumerTreeItem, consumerIndex: number) {
  return String(item.id || consumer.id || consumer.name || consumerIndex);
}

function getConsumerMenuLabel(consumer: ConsumerConfig, item: ConsumerTreeItem, fallback = "") {
  const label = String(item.displayName || item.name || "").trim();
  if (label) return label;
  return String(fallback || getEntityDisplayLabel("", consumer.endpoint, getEndpointType(consumer.endpoint))).trim();
}

function createLeaf(
  consumer: ConsumerConfig,
  item: ConsumerTreeItem,
  endpointType: string,
  targetSummary: string,
  tooltip?: string,
): MutableConsumerLeaf {
  const label = getConsumerMenuLabel(consumer, item, targetSummary);
  return {
    kind: "consumer",
    id: fallbackLeafId(consumer, item, item.originalIndex),
    label,
    consumerIndex: item.originalIndex,
    endpointType,
    statusClass: item.statusClass,
    messageCount: item.messageCount,
    throughputLabel: item.throughputLabel,
    tooltip,
    sortKey: {
      label: label.toLowerCase(),
      targetSummary: targetSummary.toLowerCase(),
    },
  };
}

function createHttpLeaf(
  consumer: ConsumerConfig,
  item: ConsumerTreeItem,
  rawUrl: string,
  path: string,
): MutableConsumerLeaf {
  const baseTarget = displayUrlWithoutScheme(rawUrl);
  const fullTarget = path && baseTarget.endsWith(path) ? baseTarget : `${baseTarget}${path || ""}`;
  return createLeaf(consumer, item, "HTTP", fullTarget || "/", fullTarget);
}

function toRelativeHttpLeaf(leaf: MutableConsumerLeaf, segments: string[]): ConsumerLeafNode {
  return {
    kind: "consumer",
    id: leaf.id,
    label: leaf.label || formatHttpSegments(segments),
    consumerIndex: leaf.consumerIndex,
    endpointType: leaf.endpointType,
    statusClass: leaf.statusClass,
    messageCount: leaf.messageCount,
    throughputLabel: leaf.throughputLabel,
    tooltip: leaf.tooltip,
  };
}

function toLeafNode(leaf: MutableConsumerLeaf): ConsumerLeafNode {
  return {
    kind: "consumer",
    id: leaf.id,
    label: leaf.label,
    consumerIndex: leaf.consumerIndex,
    endpointType: leaf.endpointType,
    statusClass: leaf.statusClass,
    messageCount: leaf.messageCount,
    throughputLabel: leaf.throughputLabel,
    tooltip: leaf.tooltip,
  };
}

function getOrCreateBucket(groups: Map<string, GroupBucket<ConsumerLeafNode>>, bucket: GroupBucket<ConsumerLeafNode>) {
  const existing = groups.get(bucket.id);
  if (existing) return existing;
  groups.set(bucket.id, bucket);
  return bucket;
}

function addHttpConsumerToBucket(bucket: GroupBucket<ConsumerLeafNode>, consumer: ConsumerConfig, item: ConsumerTreeItem) {
  const http = consumer.endpoint?.http as Record<string, unknown> | undefined;
  const rawUrl = String(http?.url || "");
  const path = parseHttpPath(rawUrl, String(http?.path || ""));
  addHttpLeafToBucket(bucket, rawUrl, String(http?.path || ""), createHttpLeaf(consumer, item, rawUrl, path));
}

export function buildConsumerTree(consumers: ConsumerConfig[], items: ConsumerTreeItem[]): ConsumerTreeNode[] {
  const itemByIndex = new Map(items.map((item) => [item.originalIndex, item]));
  const groups = new Map<string, GroupBucket<ConsumerLeafNode>>();

  consumers.forEach((consumer, consumerIndex) => {
    const item = itemByIndex.get(consumerIndex);
    if (!item) return;
    const endpointType = getEndpointType(consumer.endpoint || {});

    if (endpointType === "http") {
      const http = consumer.endpoint?.http as Record<string, unknown> | undefined;
      const bucket = getOrCreateBucket(groups, createHttpBucket(String(http?.url || "")));
      addHttpConsumerToBucket(bucket, consumer, item);
      return;
    }

    const info = buildNonHttpGroupInfo(consumer.endpoint);
    const bucket = getOrCreateBucket(groups, {
      id: info.groupKey,
      label: info.groupLabel,
      endpointType: info.endpointType,
      tooltip: info.tooltip,
      leaves: [],
    });
    bucket.leaves.push(createLeaf(consumer, item, info.endpointType, info.sortTarget, info.leafTooltip));
  });

  return [...groups.values()]
    .sort((left, right) => left.label.localeCompare(right.label))
    .map((bucket) => {
      const { children, label } = materializeBucketChildren(bucket, toLeafNode, toRelativeHttpLeaf);

      if (children.length === 1 && !isGroupNode(children[0])) {
        return {
          ...children[0],
          tooltip: bucket.tooltip || children[0].tooltip,
        } satisfies ConsumerLeafNode;
      }

      return {
        kind: "group",
        id: bucket.id,
        label,
        endpointType: bucket.endpointType,
        tooltip: bucket.tooltip,
        children,
      } satisfies ConsumerGroupNode;
    });
}
