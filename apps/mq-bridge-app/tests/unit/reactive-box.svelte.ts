/**
 * A reactive holder for a prop a test needs to change after mount.
 *
 * Runes only compile inside `.svelte`/`.svelte.ts` modules, so a plain `.test.ts`
 * cannot declare `$state` of its own.
 */
export function reactiveBox<T>(initial: T) {
  let value = $state(initial);
  return {
    get value() {
      return value;
    },
    set value(next: T) {
      value = next;
    },
  };
}
