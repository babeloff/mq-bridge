import { describe, expect, test } from "vitest";
import { buildConsumerTree, type ConsumerTreeItem } from "../../ui/src/lib/consumer-grouping";
import type { ConsumerConfig } from "../../ui/src/lib/panel-types";

function sidebarItems(consumers: ConsumerConfig[]): ConsumerTreeItem[] {
  return consumers.map((consumer, index) => ({
    name: String(consumer.name || ""),
    displayName: String(consumer.name || ""),
    inputProto: Object.keys(consumer.endpoint || {}).find((key) => key !== "middlewares")?.toUpperCase() || "",
    statusClass: "status-off",
    messageCount: 0,
    throughputLabel: "",
    originalIndex: index,
    id: consumer.id,
  }));
}

function flattenLabels(nodes: ReturnType<typeof buildConsumerTree>): string[] {
  const result: string[] = [];
  for (const node of nodes) {
    result.push(node.label);
    if (node.kind === "group") {
      result.push(...flattenLabels(node.children));
    }
  }
  return result;
}

describe("consumer grouping", () => {
  test("groups http consumers by shared root path", () => {
    const consumers = [
      { name: "order a", endpoint: { http: { url: "https://api.test/order", method: "POST" } } },
      { name: "order b", endpoint: { http: { url: "https://api.test/order", method: "POST" } } },
    ] as ConsumerConfig[];

    expect(flattenLabels(buildConsumerTree(consumers, sidebarItems(consumers)))).toEqual([
      "HTTP api.test/order",
      "order a",
      "order b",
    ]);
  });

  test("keeps differing http child paths below the shared root", () => {
    const consumers = [
      { name: "order a", endpoint: { http: { url: "https://api.test/order", path: "/order/a", method: "POST" } } },
      { name: "order b", endpoint: { http: { url: "https://api.test/order", path: "/order/b", method: "POST" } } },
    ] as ConsumerConfig[];

    expect(flattenLabels(buildConsumerTree(consumers, sidebarItems(consumers)))).toEqual([
      "HTTP api.test/order",
      "/a/",
      "order a",
      "/b/",
      "order b",
    ]);
  });

  test("preserves consumer indices and status fields for selection", () => {
    const consumers = [
      { name: "alpha", endpoint: { nats: { subject: "orders.created" } } },
      { name: "beta", endpoint: { nats: { subject: "orders.updated" } } },
    ] as ConsumerConfig[];
    const items = sidebarItems(consumers);
    items[1].statusClass = "status-ok";
    items[1].messageCount = 3;
    items[1].throughputLabel = "2.0 msg/s";

    const tree = buildConsumerTree(consumers, items);
    const group = tree[0]?.kind === "group" ? tree[0] : null;
    const beta = group?.children.find((node) => node.kind === "consumer" && node.label === "beta");

    expect(beta).toMatchObject({
      kind: "consumer",
      consumerIndex: 1,
      statusClass: "status-ok",
      messageCount: 3,
      throughputLabel: "2.0 msg/s",
    });
  });
});
