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
import type { PublisherConfig } from "./panel-types";
import { getEntityDisplayLabel } from "./utils";

export type PublisherLeafNode = GroupTreeLeafBase<"publisher"> & {
  publisher: PublisherConfig;
  publisherIndex: number;
};

export type PublisherGroupNode = GroupTreeGroup<PublisherLeafNode>;
export type PublisherTreeNode = GroupTreeNode<PublisherLeafNode>;

const HTTP_METHOD_SORT_ORDER = ["GET", "POST", "PUT", "PATCH", "DELETE"];

type MutablePublisherLeaf = MutableGroupLeaf<PublisherLeafNode>;

function fallbackLeafId(publisher: PublisherConfig, publisherIndex: number) {
  return String(publisher.id || publisher.name || publisherIndex);
}

function getPublisherMenuLabel(publisher: PublisherConfig, fallback = "") {
  const name = String(publisher.name || "").trim();
  if (name) return name;
  return String(fallback || getEntityDisplayLabel("", publisher.endpoint, getEndpointType(publisher.endpoint))).trim();
}

function getHttpMethod(publisher: PublisherConfig) {
  const http = publisher.endpoint?.http;
  const value = http && typeof http === "object" ? (http as Record<string, unknown>).method : undefined;
  return String(value || "POST").toUpperCase();
}

function buildLeafSortKey(publisher: PublisherConfig, endpointType: string, targetSummary: string) {
  const manualOrder = Number.isFinite(publisher.sort_order) ? Number(publisher.sort_order) : Number.MAX_SAFE_INTEGER;
  const method = getHttpMethod(publisher);
  const methodIndex = HTTP_METHOD_SORT_ORDER.indexOf(method);
  const methodOrder = endpointType === "http"
    ? (methodIndex === -1 ? HTTP_METHOD_SORT_ORDER.length : methodIndex)
    : Number.MAX_SAFE_INTEGER - 1;

  return {
    manualOrder,
    methodOrder,
    targetSummary: targetSummary.toLowerCase(),
    label: String(publisher.name || "").toLowerCase(),
  };
}

function createHttpLeaf(
  publisher: PublisherConfig,
  publisherIndex: number,
  rawUrl: string,
  path: string,
): MutablePublisherLeaf {
  const publisherName = String(publisher.name || "").trim();
  const baseTarget = displayUrlWithoutScheme(rawUrl);
  const fullTarget = path && baseTarget.endsWith(path) ? baseTarget : `${baseTarget}${path || ""}`;
  return {
    kind: "publisher",
    id: fallbackLeafId(publisher, publisherIndex),
    label: getPublisherMenuLabel(publisher, fullTarget || "/"),
    publisher,
    publisherIndex,
    endpointType: "HTTP",
    tooltip: fullTarget,
    sortKey: buildLeafSortKey(publisher, "http", `${path}|${publisherName}`),
  };
}

function toRelativeHttpLeaf(leaf: MutablePublisherLeaf, segments: string[]): PublisherLeafNode {
  const pathLabel = formatHttpSegments(segments);
  return {
    kind: "publisher",
    id: leaf.id,
    label: getPublisherMenuLabel(leaf.publisher, `${getHttpMethod(leaf.publisher)} ${pathLabel}`),
    publisher: leaf.publisher,
    publisherIndex: leaf.publisherIndex,
    endpointType: leaf.endpointType,
    tooltip: leaf.tooltip,
  };
}

function toLeafNode(leaf: MutablePublisherLeaf): PublisherLeafNode {
  return {
    kind: "publisher",
    id: leaf.id,
    label: leaf.label,
    publisher: leaf.publisher,
    publisherIndex: leaf.publisherIndex,
    endpointType: leaf.endpointType,
    tooltip: leaf.tooltip,
  };
}

function collapseSingleLeaf(leaf: PublisherLeafNode, bucket: GroupBucket<PublisherLeafNode>): PublisherLeafNode {
  const name = String(leaf.publisher.name || "").trim();
  return {
    ...leaf,
    label: name || leaf.tooltip || bucket.label,
    tooltip: bucket.tooltip || leaf.tooltip,
  };
}

function getOrCreateBucket(groups: Map<string, GroupBucket<PublisherLeafNode>>, bucket: GroupBucket<PublisherLeafNode>) {
  const existing = groups.get(bucket.id);
  if (existing) return existing;
  groups.set(bucket.id, bucket);
  return bucket;
}

function addHttpPublisherToBucket(bucket: GroupBucket<PublisherLeafNode>, publisher: PublisherConfig, publisherIndex: number) {
  const http = publisher.endpoint?.http as Record<string, unknown> | undefined;
  const rawUrl = String(http?.url || "");
  const path = parseHttpPath(rawUrl, String(http?.path || ""));
  addHttpLeafToBucket(bucket, rawUrl, String(http?.path || ""), createHttpLeaf(publisher, publisherIndex, rawUrl, path));
}

export function buildPublisherTree(publishers: PublisherConfig[]): PublisherTreeNode[] {
  const groups = new Map<string, GroupBucket<PublisherLeafNode>>();

  publishers.forEach((publisher, publisherIndex) => {
    const endpointType = getEndpointType(publisher.endpoint || {});

    if (endpointType === "http") {
      const http = publisher.endpoint?.http as Record<string, unknown> | undefined;
      const bucket = getOrCreateBucket(groups, createHttpBucket(String(http?.url || "")));
      addHttpPublisherToBucket(bucket, publisher, publisherIndex);
      return;
    }

    const info = buildNonHttpGroupInfo(publisher.endpoint);
    const bucket = getOrCreateBucket(groups, {
      id: info.groupKey,
      label: info.groupLabel,
      endpointType: info.endpointType,
      tooltip: info.tooltip,
      leaves: [],
    });
    bucket.leaves.push({
      kind: "publisher",
      id: fallbackLeafId(publisher, publisherIndex),
      label: getPublisherMenuLabel(publisher, info.leafTooltip || info.sortTarget),
      publisher,
      publisherIndex,
      endpointType: info.endpointType,
      tooltip: info.leafTooltip,
      sortKey: buildLeafSortKey(publisher, endpointType, `${info.sortTarget}|${publisher.name}`),
    });
  });

  return [...groups.values()]
    .sort((left, right) => left.label.localeCompare(right.label))
    .map((bucket) => {
      const { children, label } = materializeBucketChildren(bucket, toLeafNode, toRelativeHttpLeaf);

      if (children.length === 1 && !isGroupNode(children[0])) {
        return collapseSingleLeaf(children[0], bucket);
      }

      return {
        kind: "group",
        id: bucket.id,
        label,
        endpointType: bucket.endpointType,
        tooltip: bucket.tooltip,
        children,
      } satisfies PublisherGroupNode;
    });
}
