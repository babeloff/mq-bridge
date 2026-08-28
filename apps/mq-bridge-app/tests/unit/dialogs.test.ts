import { describe, expect, test } from "vitest";
import { get } from "svelte/store";
import { activeDialog, closeDialog, showDialog } from "../../ui/src/lib/dialogs";

describe("dialogs", () => {
  test("ignores stale close events after a queued dialog becomes active", async () => {
    const first = showDialog({ title: "First", message: "First", mode: "message" });
    const firstId = get(activeDialog)?.id;
    const second = showDialog({ title: "Second", message: "Second", mode: "message" });

    closeDialog(true, firstId);
    await expect(first).resolves.toBe(true);

    const secondState = get(activeDialog);
    expect(secondState?.request.title).toBe("Second");

    closeDialog(null, firstId);
    expect(get(activeDialog)?.id).toBe(secondState?.id);

    let secondResolved = false;
    second.then(() => {
      secondResolved = true;
    });
    await Promise.resolve();
    expect(secondResolved).toBe(false);

    closeDialog(false, secondState?.id);
    await expect(second).resolves.toBe(false);
  });
});
