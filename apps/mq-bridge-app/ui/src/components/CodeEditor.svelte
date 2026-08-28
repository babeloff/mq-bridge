<script lang="ts">
  import { basicSetup } from "codemirror";
  import { EditorState, Compartment, RangeSetBuilder } from "@codemirror/state";
  import {
    Decoration,
    EditorView,
    ViewPlugin,
    WidgetType,
    highlightWhitespace,
    keymap,
    placeholder as cmPlaceholder,
  } from "@codemirror/view";
  import { historyKeymap } from "@codemirror/commands";
  import { syntaxHighlighting, HighlightStyle } from "@codemirror/language";
  import { json, jsonParseLinter } from "@codemirror/lang-json";
  import { xml } from "@codemirror/lang-xml";
  import { linter, lintGutter, setDiagnosticsEffect } from "@codemirror/lint";
  import { tags } from "@lezer/highlight";
  import { onDestroy, untrack } from "svelte";

  type EditorLanguage = "text" | "json" | "json-auto" | "xml";

  let {
    id = "",
    value = "",
    placeholder = "",
    language = "json-auto",
    readOnly = false,
    wrapLines = true,
    useLinter = true,
    showWhitespace = false,
    showLineEndings = false,
    onChange = (_value: string) => {},
    onBlur = () => {},
    onFocus = () => {},
  }: {
    id?: string;
    value?: string;
    placeholder?: string;
    language?: EditorLanguage;
    useLinter?: boolean;
    onChange?: (value: string) => void;
    readOnly?: boolean;
    wrapLines?: boolean;
    showWhitespace?: boolean;
    showLineEndings?: boolean;
    onBlur?: () => void;
    onFocus?: () => void;
  } = $props();
  // Initialize with the current prop value. The warning is silenced by acknowledging this is local state.
  let lastPropValue = $state(untrack(() => value ?? ""));

  let container: HTMLDivElement | null = null;
  let editorView: EditorView | null = null;
  let applyingExternalUpdate = false;

  const languageCompartment = new Compartment();
  const readOnlyCompartment = new Compartment();
  const wrapLinesCompartment = new Compartment();
  const whitespaceCompartment = new Compartment();
  const lineEndingsCompartment = new Compartment();
  const lintGutterCompartment = new Compartment();
  const highlightStyle = HighlightStyle.define([
    { tag: [tags.propertyName, tags.attributeName], color: "var(--json-key)" },
    { tag: tags.tagName, color: "var(--accent-color, var(--text-payload, #007acc))" },
    { tag: tags.string, color: "var(--json-string)" },
    { tag: tags.number, color: "var(--json-number)" },
    { tag: [tags.bool, tags.keyword], color: "var(--json-bool)" },
    { tag: tags.null, color: "var(--json-null)" },
    { tag: tags.angleBracket, color: "var(--text-dim)" },
    { tag: tags.comment, color: "var(--text-dim)", fontStyle: "italic" },
    { tag: tags.processingInstruction, color: "var(--text-dim)" },
  ]);

  function isLikelyJson(text: string) {
    const trimmed = text.trim();
    return (trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"));
  }

  // Static instances for linters to prevent resets during reconfigurations
  const jsonLinterInstance = linter(jsonParseLinter());

  // Basic XML linter using the browser's DOMParser
  const xmlLinterInstance = linter((view) => {
    const text = view.state.doc.toString();
    if (!text.trim() || view.state.readOnly) return [];
    try {
      const parser = new DOMParser();
      const xmlDoc = parser.parseFromString(text, "application/xml");
      const parseError = xmlDoc.querySelector("parsererror");
      if (parseError) {
        let message = parseError.textContent || "XML Syntax Error";
        let from = 0, to = text.length;
        
        // Try to find line/column info (common in Chromium browsers)
        const match = message.match(/line (\d+) at column (\d+)/i);
        if (match) {
          const lineNum = parseInt(match[1], 10);
          const colNum = parseInt(match[2], 10);
          if (lineNum > 0 && lineNum <= view.state.doc.lines) {
            const line = view.state.doc.line(lineNum);
            from = Math.min(line.from + Math.max(0, colNum - 1), line.to);
            to = line.to;
          }
        }
        return [{ from, to, severity: "error", message: message.split('\n')[0] }];
      }
    } catch (e) { /* ignore */ }
    return [];
  });

  function languageExtensionFor(text: string, mode: EditorLanguage, linterEnabled: boolean) {
    const extensions = [];
    if (mode === "json" || (mode === "json-auto" && isLikelyJson(text))) {
      extensions.push(json({ indentUnit: 2 }));
      if (linterEnabled) {
        extensions.push(jsonLinterInstance);
      }
    } else if (mode === "xml") {
      extensions.push(xml());
      if (linterEnabled) {
        extensions.push(xmlLinterInstance);
      }
    }
    return extensions;
  }

  class LineEndingWidget extends WidgetType {
    private marker: string;

    constructor(marker: string) {
      super();
      this.marker = marker;
    }

    toDOM() {
      const span = document.createElement("span");
      span.className = "cm-line-ending-marker";
      span.textContent = this.marker;
      span.setAttribute("aria-hidden", "true");
      return span;
    }
  }

  function lineEndingMarkers() {
    return ViewPlugin.fromClass(
      class {
        decorations;

        constructor(view: EditorView) {
          this.decorations = this.buildDecorations(view);
        }

        update(update: { docChanged: boolean; viewportChanged: boolean; view: EditorView }) {
          if (update.docChanged || update.viewportChanged) {
            this.decorations = this.buildDecorations(update.view);
          }
        }

        buildDecorations(view: EditorView) {
          const builder = new RangeSetBuilder<Decoration>();
          for (const range of view.visibleRanges) {
            let line = view.state.doc.lineAt(range.from);
            while (line.from <= range.to) {
              if (line.to < view.state.doc.length) {
                const next = view.state.doc.sliceString(line.to, Math.min(line.to + 2, view.state.doc.length));
                builder.add(line.to, line.to, Decoration.widget({ widget: new LineEndingWidget(next.startsWith("\r\n") ? "CRLF" : next.startsWith("\r") ? "CR" : "LF"), side: 1 }));
              }
              if (line.to >= range.to || line.number >= view.state.doc.lines) break;
              line = view.state.doc.line(line.number + 1);
            }
          }
          return builder.finish();
        }
      },
      {
        decorations: (plugin) => plugin.decorations,
      },
    );
  }

  $effect(() => {
    if (!container || editorView) return;
    const initialValue = String(value ?? "");

    editorView = new EditorView({
      state: EditorState.create({
        doc: initialValue,
        extensions: [
          basicSetup,
          lintGutterCompartment.of(useLinter ? lintGutter() : []),
          keymap.of([...historyKeymap]),
          cmPlaceholder(placeholder),
          syntaxHighlighting(highlightStyle),
          languageCompartment.of(languageExtensionFor(initialValue, language, useLinter)),
          readOnlyCompartment.of(EditorState.readOnly.of(readOnly)),
          wrapLinesCompartment.of(wrapLines ? EditorView.lineWrapping : []),
          whitespaceCompartment.of(showWhitespace ? highlightWhitespace() : []),
          lineEndingsCompartment.of(showLineEndings ? lineEndingMarkers() : []),
          EditorView.updateListener.of((update) => {
            if (!update.docChanged || applyingExternalUpdate) return;
            const docString = update.state.doc.toString();
            onChange(docString);
          }),
          EditorView.domEventHandlers({
            blur: onBlur,
            focus: onFocus,
          }),
        ],
      }),
      parent: container,
    });
  });

  $effect(() => {
    if (!editorView) return;
    const nextValue = String(value ?? "");
    const currentValue = editorView.state.doc.toString();

    // Only update if the prop value actually changed externally
    // and it differs from what we currently have in the editor
    if (nextValue !== untrack(() => lastPropValue) && nextValue !== currentValue) {
      applyingExternalUpdate = true;
      editorView.dispatch({
        changes: { from: 0, to: currentValue.length, insert: nextValue },
      });
      applyingExternalUpdate = false;
    }
    lastPropValue = nextValue;

    editorView.dispatch({
      effects: [
        languageCompartment.reconfigure(languageExtensionFor(nextValue, language, useLinter)),
        readOnlyCompartment.reconfigure(EditorState.readOnly.of(readOnly)),
        wrapLinesCompartment.reconfigure(wrapLines ? EditorView.lineWrapping : []),
        whitespaceCompartment.reconfigure(showWhitespace ? highlightWhitespace() : []),
        lineEndingsCompartment.reconfigure(showLineEndings ? lineEndingMarkers() : []),
        lintGutterCompartment.reconfigure(useLinter ? lintGutter() : []),
        // Clear diagnostics when linting is disabled to prevent markers from sticking around
        ...(!useLinter ? [setDiagnosticsEffect.of([])] : []),
      ]
    });
  });

  onDestroy(() => {
    editorView?.destroy();
    editorView = null;
  });
</script>

<div id={id} class="code-editor body-editor" bind:this={container}></div>

<style>
  .code-editor {
    flex: 1;
    min-height: 0;
  }
  :global(.cm-editor) {
    height: 100% !important;
  }
  /* Fix cursor visibility in dark mode */
  :global(.cm-content) {
    caret-color: var(--text-primary, #fff) !important;
  }
  :global(.cm-cursor, .cm-dropCursor) {
    border-left-color: var(--text-primary, #fff) !important;
  }

  :global(.cm-line-ending-marker) {
    color: var(--text-dim);
    font-size: 0.72em;
    margin-left: 4px;
    opacity: 0.75;
    user-select: none;
  }
</style>
