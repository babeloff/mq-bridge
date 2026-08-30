import { getEndpointType } from "./endpoint-utils";
import { formatEndpointTypeLabel } from "./endpoint-metadata";

export type GroupTreeLeafBase<Kind extends string> = {
  kind: Kind;
  id: string;
  label: string;
  endpointType: string;
  tooltip?: string;
};

export type GroupTreeGroup<Leaf> = {
  kind: "group";
  id: string;
  label: string;
  children: Array<GroupTreeGroup<Leaf> | Leaf>;
  endpointType?: string;
  tooltip?: string;
};

export type GroupTreeNode<Leaf> = GroupTreeGroup<Leaf> | Leaf;

export type GroupSortKey = {
  manualOrder?: number;
  methodOrder?: number;
  targetSummary: string;
  label: string;
};

export type MutableGroupLeaf<Leaf> = Leaf & {
  sortKey: GroupSortKey;
};

export type PathTrieNode<Leaf> = {
  segment: string;
  pathKey: string;
  exactLeaves: Array<MutableGroupLeaf<Leaf>>;
  children: Map<string, PathTrieNode<Leaf>>;
};

export type GroupBucket<Leaf> = {
  id: string;
  label: string;
  endpointType: string;
  tooltip: string;
  leaves: Array<MutableGroupLeaf<Leaf>>;
  pathRoot?: PathTrieNode<Leaf>;
  httpPathSegments?: string[][];
};

export type NonHttpGroupInfo = {
  endpointType: string;
  groupKey: string;
  groupLabel: string;
  tooltip: string;
  leafTooltip: string;
  sortTarget: string;
};

const GROUP_DELIMITERS = /[./]/;

function normalizeSegment(value: string) {
  return value.trim().replace(/\s+/g, " ");
}

export function sanitizeTooltipValue(value: string) {
  const trimmed = String(value || "").trim();
  if (!trimmed) return "";

  try {
    const parsed = new URL(trimmed);
    parsed.username = "";
    parsed.password = "";
    return parsed.toString();
  } catch {
    return trimmed.replace(/\/\/([^/@:\s]+):([^@/\s]+)@/g, "//$1@");
  }
}

export function displayUrlWithoutScheme(rawUrl: string) {
  const safe = sanitizeTooltipValue(rawUrl);
  if (!safe) return "";
  return safe.replace(/^[a-z]+:\/\//i, "").replace(/\/$/, "");
}

export function sanitizeHttpRootUrl(rawUrl: string) {
  const value = String(rawUrl || "").trim();
  if (!value) return "";
  try {
    const parsed = new URL(value);
    parsed.username = "";
    parsed.password = "";
    parsed.pathname = "";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return value.replace(/^[a-z]+:\/\//i, "").split("/")[0] || value;
  }
}

export function parseHttpPath(rawUrl: string, rawPath: string) {
  const trimmedPath = String(rawPath || "").trim();
  if (trimmedPath) return trimmedPath;

  const trimmedUrl = String(rawUrl || "").trim();
  if (!trimmedUrl) return "";

  try {
    const parsed = new URL(trimmedUrl);
    return `${parsed.pathname || "/"}${parsed.search || ""}`;
  } catch {
    const slashIndex = trimmedUrl.indexOf("/");
    return slashIndex >= 0 ? trimmedUrl.slice(slashIndex) : "";
  }
}

export function splitPathSegments(path: string) {
  const [pathname] = String(path || "").split("?");
  return pathname
    .split("/")
    .map(normalizeSegment)
    .filter(Boolean);
}

export function commonPathPrefix(paths: string[][]) {
  if (paths.length === 0) return [];
  const prefix: string[] = [];
  const shortest = Math.min(...paths.map((path) => path.length));
  for (let index = 0; index < shortest; index += 1) {
    const segment = paths[0][index];
    if (paths.every((path) => path[index] === segment)) {
      prefix.push(segment);
    } else {
      break;
    }
  }
  return prefix;
}

export function splitMessagingSegments(value: string) {
  return String(value || "")
    .split(GROUP_DELIMITERS)
    .map(normalizeSegment)
    .filter(Boolean);
}

export function formatHttpSegments(segments: string[]) {
  return segments.length === 0 ? "/" : `/${segments.join("/")}/`;
}

export function createPathTrieNode<Leaf>(segment: string, pathKey: string): PathTrieNode<Leaf> {
  return {
    segment,
    pathKey,
    exactLeaves: [],
    children: new Map(),
  };
}

export function nodeForPathPrefix<Leaf>(root: PathTrieNode<Leaf>, prefix: string[]) {
  let cursor = root;
  for (const segment of prefix) {
    const next = cursor.children.get(segment);
    if (!next) return root;
    cursor = next;
  }
  return cursor;
}

export function countTrieLeaves<Leaf>(node: PathTrieNode<Leaf>): number {
  return node.exactLeaves.length + [...node.children.values()].reduce((sum, child) => sum + countTrieLeaves(child), 0);
}

export function sortGroupLeaves<Leaf>(leaves: Array<MutableGroupLeaf<Leaf>>) {
  return [...leaves].sort((left, right) =>
    (left.sortKey.manualOrder ?? Number.MAX_SAFE_INTEGER) - (right.sortKey.manualOrder ?? Number.MAX_SAFE_INTEGER)
    || (left.sortKey.methodOrder ?? Number.MAX_SAFE_INTEGER) - (right.sortKey.methodOrder ?? Number.MAX_SAFE_INTEGER)
    || left.sortKey.targetSummary.localeCompare(right.sortKey.targetSummary)
    || left.sortKey.label.localeCompare(right.sortKey.label));
}

function flattenSingleDescendant<Leaf>(
  node: PathTrieNode<Leaf>,
  segments: string[],
  toRelativeHttpLeaf: (leaf: MutableGroupLeaf<Leaf>, segments: string[]) => Leaf,
): Leaf | null {
  if (countTrieLeaves(node) !== 1) return null;
  if (node.exactLeaves.length === 1) {
    return toRelativeHttpLeaf(node.exactLeaves[0], segments);
  }
  const onlyChild = [...node.children.values()][0];
  return onlyChild ? flattenSingleDescendant(onlyChild, [...segments, onlyChild.segment], toRelativeHttpLeaf) : null;
}

export function materializeHttpNodes<Leaf>(
  node: PathTrieNode<Leaf>,
  rootId: string,
  toRelativeHttpLeaf: (leaf: MutableGroupLeaf<Leaf>, segments: string[]) => Leaf,
  inheritedSegments: string[] = [],
): Array<GroupTreeNode<Leaf>> {
  let currentNode = node;
  let currentSegments = [...inheritedSegments];

  while (currentSegments.length > 0 && currentNode.exactLeaves.length === 0 && currentNode.children.size === 1) {
    const onlyChild = [...currentNode.children.values()][0];
    currentNode = onlyChild;
    currentSegments = [...currentSegments, onlyChild.segment];
  }

  const exactLeaves = sortGroupLeaves(currentNode.exactLeaves).map((leaf) => toRelativeHttpLeaf(leaf, currentSegments));
  const childEntries = [...currentNode.children.values()].sort((left, right) => left.segment.localeCompare(right.segment));
  const childNodes: Array<GroupTreeNode<Leaf>> = [];
  const canFlattenChildPath = childEntries.length === 1 && exactLeaves.length === 0;
  for (const child of childEntries) {
    const childSegments = [...currentSegments, child.segment];
    const flattened = canFlattenChildPath ? flattenSingleDescendant(child, childSegments, toRelativeHttpLeaf) : null;
    if (flattened) {
      childNodes.push(flattened);
    } else {
      childNodes.push(...materializeHttpNodes(child, rootId, toRelativeHttpLeaf, childSegments));
    }
  }

  const mergedChildren = [...exactLeaves, ...childNodes];
  if (currentSegments.length === 0) {
    if (mergedChildren.length === 1 && isGroupNode(mergedChildren[0])) {
      const onlyGroup = mergedChildren[0];
      if (onlyGroup.children.every((child) => !isGroupNode(child))) {
        return onlyGroup.children;
      }
    }
    return mergedChildren;
  }

  return [{
    kind: "group",
    id: `${rootId}:path:${currentNode.pathKey || currentSegments.join("/")}`,
    label: formatHttpSegments(currentSegments),
    children: mergedChildren,
  }];
}

export function addHttpLeafToBucket<Leaf>(
  bucket: GroupBucket<Leaf>,
  rawUrl: string,
  rawPath: string,
  leaf: MutableGroupLeaf<Leaf>,
) {
  const path = parseHttpPath(rawUrl, rawPath);
  const segments = splitPathSegments(path);
  bucket.httpPathSegments?.push(segments);

  if (!bucket.pathRoot) {
    bucket.pathRoot = createPathTrieNode("", "");
  }

  if (segments.length === 0) {
    bucket.pathRoot.exactLeaves.push(leaf);
    return;
  }

  let cursor = bucket.pathRoot;
  const builtSegments: string[] = [];
  for (const segment of segments) {
    builtSegments.push(segment);
    const nextKey = builtSegments.join("/");
    let next = cursor.children.get(segment);
    if (!next) {
      next = createPathTrieNode(segment, nextKey);
      cursor.children.set(segment, next);
    }
    cursor = next;
  }

  cursor.exactLeaves.push(leaf);
}

export function createHttpBucket<Leaf>(rawUrl: string): GroupBucket<Leaf> {
  const rootUrl = sanitizeHttpRootUrl(rawUrl);
  const groupLabel = `HTTP ${displayUrlWithoutScheme(rootUrl) || "Ungrouped"}`.trim();
  return {
    id: `http:${rootUrl || groupLabel}`,
    label: groupLabel,
    endpointType: "HTTP",
    tooltip: rootUrl,
    leaves: [],
    pathRoot: createPathTrieNode("", ""),
    httpPathSegments: [],
  };
}

export function isGroupNode<Leaf>(node: GroupTreeNode<Leaf>): node is GroupTreeGroup<Leaf> {
  return Boolean(node && typeof node === "object" && (node as { kind?: string }).kind === "group");
}

export function buildNonHttpGroupInfo(endpoint: Record<string, unknown> | undefined): NonHttpGroupInfo {
  const endpointType = getEndpointType(endpoint || {});
  const endpointConfig = endpoint?.[endpointType];
  const objectConfig = endpointConfig && typeof endpointConfig === "object" ? (endpointConfig as Record<string, unknown>) : {};
  const safeUrl = sanitizeTooltipValue(String(objectConfig.url || ""));

  if (endpointType === "mongodb") {
    const database = String(objectConfig.database || "").trim();
    const collection = String(objectConfig.collection || "").trim();
    const target = `${database}/${collection}`.replace(/^\/+|\/+$/g, "");
    const label = `MONGODB ${displayUrlWithoutScheme(safeUrl)}${database ? `/${database}` : ""}${collection ? `/${collection}` : ""}`;
    return {
      endpointType: "MONGODB",
      groupKey: `mongodb:${safeUrl}:${database}:${collection}`,
      groupLabel: label.trim(),
      tooltip: safeUrl,
      leafTooltip: target || safeUrl,
      sortTarget: `${database}/${collection}`,
    };
  }

  const target = String(
    objectConfig.subject
      || objectConfig.topic
      || objectConfig.queue
      || objectConfig.routing_key
      || objectConfig.routingKey
      || objectConfig.table
      || objectConfig.path
      || "",
  ).trim();
  const firstSegment = splitMessagingSegments(target)[0] || target || "Ungrouped";
  return {
    endpointType: formatEndpointTypeLabel(endpointType),
    groupKey: `${endpointType}:${safeUrl}:${firstSegment}`,
    groupLabel: `${formatEndpointTypeLabel(endpointType)} ${displayUrlWithoutScheme(safeUrl) || firstSegment}`.trim(),
    tooltip: safeUrl || target,
    leafTooltip: target || safeUrl,
    sortTarget: target || safeUrl || firstSegment,
  };
}

export function materializeBucketChildren<Leaf>(
  bucket: GroupBucket<Leaf>,
  toLeafNode: (leaf: MutableGroupLeaf<Leaf>) => Leaf,
  toRelativeHttpLeaf: (leaf: MutableGroupLeaf<Leaf>, segments: string[]) => Leaf,
) {
  const commonHttpPath = bucket.pathRoot ? commonPathPrefix(bucket.httpPathSegments || []) : [];
  const httpRootNode = bucket.pathRoot ? nodeForPathPrefix(bucket.pathRoot, commonHttpPath) : null;
  const children = httpRootNode
    ? materializeHttpNodes(httpRootNode, bucket.id, toRelativeHttpLeaf)
    : sortGroupLeaves(bucket.leaves).map(toLeafNode);
  const label = commonHttpPath.length > 0 ? `${bucket.label}/${commonHttpPath.join("/")}` : bucket.label;
  return { children, label };
}
