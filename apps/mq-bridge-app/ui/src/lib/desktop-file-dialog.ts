/** Native file dialogs, desktop only. Returns null when the user cancels. */
export async function pickFilePath(mode: "open" | "save", currentPath: string): Promise<string | null> {
  const { open, save } = await import("@tauri-apps/plugin-dialog");
  const defaultPath = currentPath.trim() || (mode === "save" ? "messages.jsonl" : undefined);
  const selected = mode === "open"
    ? await open({ multiple: false, directory: false, defaultPath })
    : await save({ defaultPath });
  return typeof selected === "string" ? selected : null;
}
