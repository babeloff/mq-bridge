import { flushSync, mount, type Component } from "svelte";

export function renderSvelteNode<Props extends Record<string, unknown>>(
  component: Component<Props>,
  props: Props,
) {
  const target = document.createElement("div");
  mount(component, { target, props });
  flushSync();
  return target.childElementCount === 1
    ? (target.firstElementChild as HTMLElement)
    : target;
}
