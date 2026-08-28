import { describe, expect, test } from "vitest";
import { buildPublisherTree } from "../../ui/src/lib/publisher-grouping";
import type { PublisherConfig } from "../../ui/src/lib/panel-types";

function flattenLabels(nodes: ReturnType<typeof buildPublisherTree>): string[] {
  const result: string[] = [];
  for (const node of nodes) {
    result.push(node.label);
    if (node.kind === "group") {
      result.push(...flattenLabels(node.children));
    }
  }
  return result;
}

describe("publisher grouping", () => {
  test("groups http publishers by path segments", () => {
    const tree = buildPublisherTree([
      {
        name: "list users",
        endpoint: { http: { url: "https://api.test", path: "/admin/users", method: "GET" } },
      },
      {
        name: "get user",
        endpoint: { http: { url: "https://api.test", path: "/admin/users/{id}", method: "GET" } },
      },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual([
      "HTTP api.test/admin/users",
      "list users",
      "/{id}/",
      "get user",
    ]);
  });

  test("uses http method fallback ordering", () => {
    const tree = buildPublisherTree([
      { name: "delete user", endpoint: { http: { url: "https://api.test", path: "/users/{id}", method: "DELETE" } } },
      { name: "get user", endpoint: { http: { url: "https://api.test", path: "/users/{id}", method: "GET" } } },
      { name: "patch user", endpoint: { http: { url: "https://api.test", path: "/users/{id}", method: "PATCH" } } },
    ] as PublisherConfig[]);

    const usersGroup = tree[0];
    const users = usersGroup.kind === "group" ? usersGroup.children : [];
    expect(users.map((node) => node.label)).toEqual([
      "get user",
      "patch user",
      "delete user",
    ]);
  });

  test("groups non-http publishers by subject and topic", () => {
    const tree = buildPublisherTree([
      { name: "jobs created", endpoint: { nats: { subject: "jobs.created" } } },
      { name: "orders created", endpoint: { kafka: { topic: "orders.created" } } },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual([
      "orders created",
      "jobs created",
    ]);
  });

  test("shows single ungrouped publishers directly", () => {
    const tree = buildPublisherTree([
      { name: "write file", endpoint: { file: { path: "" } } },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual(["write file"]);
  });

  test("respects manual ordering inside groups", () => {
    const tree = buildPublisherTree([
      {
        name: "delete user",
        sort_order: 20,
        endpoint: { http: { url: "https://api.test", path: "/users/{id}", method: "DELETE" } },
      },
      {
        name: "get user",
        sort_order: 1,
        endpoint: { http: { url: "https://api.test", path: "/users/{id}", method: "GET" } },
      },
    ] as PublisherConfig[]);

    const usersGroup = tree[0];
    const users = usersGroup.kind === "group" ? usersGroup.children : [];
    expect(users.map((node) => node.label)).toEqual([
      "get user",
      "delete user",
    ]);
  });

  test("keeps stable sorting for publishers with same endpoint", () => {
    const tree = buildPublisherTree([
      { name: "alpha", endpoint: { http: { url: "https://api.test", path: "/users", method: "POST" } }, payload: "a" },
      { name: "beta", endpoint: { http: { url: "https://api.test", path: "/users", method: "POST" } }, payload: "b" },
    ] as PublisherConfig[]);

    const usersGroup = tree[0];
    const users = usersGroup.kind === "group" ? usersGroup.children : [];
    expect(users.map((node) => node.label)).toEqual([
      "alpha",
      "beta",
    ]);
  });

  test("preserves publisher indices for selection", () => {
    const tree = buildPublisherTree([
      { name: "alpha", endpoint: { http: { url: "https://api.test", path: "/users", method: "POST" } } },
      { name: "beta", endpoint: { http: { url: "https://api.test", path: "/users/{id}", method: "GET" } } },
    ] as PublisherConfig[]);

    const usersGroup = tree[0];
    const nestedPath = usersGroup.kind === "group" && usersGroup.children[1]?.kind === "group"
      ? usersGroup.children[1]
      : null;
    const nestedLeaf = nestedPath?.children[0]?.kind === "publisher"
      ? nestedPath.children[0]
      : null;

    expect(nestedLeaf?.publisherIndex).toBe(1);
    expect(nestedLeaf?.publisher.name).toBe("beta");
  });

  test("keeps multi-leaf ungrouped publishers under one collapsible root", () => {
    const tree = buildPublisherTree([
      { name: "write file", endpoint: { file: { path: "" } } },
      { name: "append file", endpoint: { file: { path: "" } } },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual(["FILE Ungrouped", "append file", "write file"]);
  });

  test("keeps exact path leaves alongside deeper child paths", () => {
    const tree = buildPublisherTree([
      { name: "order a", endpoint: { http: { url: "https://api.test", path: "/order", method: "GET" } } },
      { name: "order b", endpoint: { http: { url: "https://api.test", path: "/order", method: "POST" } } },
      { name: "order item", endpoint: { http: { url: "https://api.test", path: "/order/1", method: "GET" } } },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual([
      "HTTP api.test/order",
      "order a",
      "order b",
      "/1/",
      "order item",
    ]);
  });

  test("does not repeat the root URL path as an extra HTTP group", () => {
    const tree = buildPublisherTree([
      { name: "order a", endpoint: { http: { url: "https://api.test/order", method: "GET" } } },
      { name: "order b", endpoint: { http: { url: "https://api.test/order", method: "POST" } } },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual([
      "HTTP api.test/order",
      "order a",
      "order b",
    ]);
  });

  test("keeps HTTP child path groups when they differ below the root URL path", () => {
    const tree = buildPublisherTree([
      { name: "order a", endpoint: { http: { url: "https://api.test/order", path: "/order/a", method: "GET" } } },
      { name: "order b", endpoint: { http: { url: "https://api.test/order", path: "/order/b", method: "GET" } } },
    ] as PublisherConfig[]);

    expect(flattenLabels(tree)).toEqual([
      "HTTP api.test/order",
      "/a/",
      "order a",
      "/b/",
      "order b",
    ]);
  });
});
