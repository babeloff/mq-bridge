import "../../crates/core/static/tauri-bridge.js";
import "./lib/forms/custom-form";
import "@awesome.me/webawesome/dist/webawesome.js";
import "@awesome.me/webawesome/dist/styles/webawesome.css";
import Split from "split.js";
import * as VanillaSchemaForms from "vanilla-schema-forms";
import { Buffer } from "buffer";
import type { AppWindow } from "./lib/browser";

globalThis.Buffer = Buffer;
const appWindow = window as AppWindow;

// Simple dialog implementations for browser mode
if (typeof appWindow.mqbAlert === "undefined") {
  appWindow.mqbAlert = async (message: string, title?: string) => {
    if (title) {
      alert(`${title}\n\n${message}`);
    } else {
      alert(message);
    }
  };
}

if (typeof appWindow.mqbConfirm === "undefined") {
  appWindow.mqbConfirm = async (message: string, title?: string) => {
    return confirm(title ? `${title}\n\n${message}` : message);
  };
}

if (typeof appWindow.mqbPrompt === "undefined") {
  appWindow.mqbPrompt = async (message: string, title?: string, options?: { placeholder?: string; value?: string }) => {
    const placeholder = options?.placeholder || "";
    const value = options?.value || "";
    return prompt(title ? `${title}\n\n${message}` : message, value || placeholder);
  };
}

if (typeof appWindow.mqbChoose === "undefined") {
  appWindow.mqbChoose = async (
    message: string,
    title?: string,
    options?: { choices?: Array<{ value: string; label: string; description?: string }> },
  ) => {
    const choices = options?.choices || [];
    if (choices.length === 0) return null;

    const choiceText = choices.map((c, i) => `${i + 1}. ${c.label}${c.description ? ` - ${c.description}` : ""}`).join("\n");
    const promptText = title ? `${title}\n\n${message}\n\n${choiceText}\n\nEnter number (1-${choices.length}):` : `${message}\n\n${choiceText}\n\nEnter number (1-${choices.length}):`;

    const result = prompt(promptText);
    if (!result) return null;

    const index = parseInt(result, 10) - 1;
    if (index >= 0 && index < choices.length) {
      return choices[index].value;
    }
    return null;
  };
}
