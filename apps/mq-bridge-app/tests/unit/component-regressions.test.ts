import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, test } from "vitest";

describe("component regressions", () => {
  test("header editors use the shared row editor component", () => {
    const publishersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/PublishersPanel.svelte"),
      "utf8",
    );
    const consumersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/ConsumersPanel.svelte"),
      "utf8",
    );
    const sharedEditor = readFileSync(
      join(process.cwd(), "ui/src/components/HeaderRowsEditor.svelte"),
      "utf8",
    );

    expect(publishersPanel).toContain("<HeaderRowsEditor");
    expect(consumersPanel).toContain("<HeaderRowsEditor");
    expect(sharedEditor).toContain("{#each rows as row, index (row.id)}");
    expect(sharedEditor).not.toContain("{#each rows as row, index (`");
  });

  test("publisher and consumer headers both use the shared toggle-capable editor", () => {
    const publishersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/PublishersPanel.svelte"),
      "utf8",
    );
    const consumersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/ConsumersPanel.svelte"),
      "utf8",
    );

    expect(publishersPanel).toContain("showEnabled={true}");
    expect(consumersPanel).toContain("showEnabled={true}");
    expect(publishersPanel).not.toContain('<table class="kv-table" id="metadata-container">');
  });

  test("publisher definition tab stays first and hides the response pane while editing", () => {
    const publishersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/PublishersPanel.svelte"),
      "utf8",
    );

    expect(publishersPanel).toMatch(
      /<div class="content-tabs" id="pub-sub-tabs">[\s\S]*Definition[\s\S]*Body[\s\S]*Headers[\s\S]*History/,
    );
    expect(publishersPanel).toContain(
      'style:display={$publishersPanelState.responseVisible && $publishersPanelState.activeSubtab !== "definition" ? "flex" : "none"}',
    );
  });

  test("captured consumer and publisher JSON bodies expose read-only pretty printing", () => {
    const payloadDisplay = readFileSync(
      join(process.cwd(), "ui/src/components/PayloadDisplay.svelte"),
      "utf8",
    );
    const consumersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/ConsumersPanel.svelte"),
      "utf8",
    );
    const publishersPanel = readFileSync(
      join(process.cwd(), "ui/src/components/PublishersPanel.svelte"),
      "utf8",
    );

    expect(payloadDisplay).toContain('prettyJson && formattedPayload.language === "json"');
    expect(consumersPanel.match(/showPretty=\{true\}/g)).toHaveLength(2);
    expect(publishersPanel.match(/showPretty=\{true\}/g)).toHaveLength(1);
  });
});
