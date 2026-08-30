import { get, writable } from "svelte/store";

export interface DialogChoice {
  value: string;
  label: string;
  description?: string;
}

export type DialogMode = "message" | "confirm" | "prompt" | "choose";

export type DialogRequest = {
  title: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  value?: string;
  placeholder?: string;
  mode?: DialogMode;
  choices?: DialogChoice[];
};

type DialogState = {
  id: number;
  request: DialogRequest;
  resolve: (result: boolean | string | null) => void;
} | null;

export const activeDialog = writable<DialogState>(null);
let nextDialogId = 1;
const pendingDialogs: Array<{
  id: number;
  request: DialogRequest;
  resolve: (result: boolean | string | null) => void;
}> = [];

export function showDialog(request: DialogRequest) {
  return new Promise<boolean | string | null>((resolve) => {
    const nextDialog = { id: nextDialogId++, request, resolve };
    if (get(activeDialog)) {
      pendingDialogs.push(nextDialog);
      return;
    }
    activeDialog.set(nextDialog);
  });
}

export function closeDialog(result: boolean | string | null, expectedDialogId?: number) {
  const state = get(activeDialog);
  if (expectedDialogId !== undefined && state?.id !== expectedDialogId) return;
  state?.resolve(result);
  const next = pendingDialogs.shift() || null;
  activeDialog.set(next);
}

export function alertDialog(message: string, title = "Notice") {
  return showDialog({
    title,
    message,
    confirmLabel: "OK",
    mode: "message",
  }).then(() => undefined);
}

export function confirmDialog(message: string, title = "Confirm") {
  return showDialog({
    title,
    message,
    confirmLabel: "Continue",
    cancelLabel: "Cancel",
    mode: "confirm",
  }) as Promise<boolean>;
}

type PromptOptions = {
  confirmLabel?: string;
  cancelLabel?: string;
  value?: string;
  placeholder?: string;
};

type ChooseOptions = {
  confirmLabel?: string;
  cancelLabel?: string;
  choices?: DialogChoice[];
};

export function promptDialog(message: string, title = "Enter Name", options: PromptOptions = {}) {
  return showDialog({
    title,
    message,
    confirmLabel: options.confirmLabel || "Create",
    cancelLabel: options.cancelLabel || "Cancel",
    value: options.value || "",
    placeholder: options.placeholder || "",
    mode: "prompt",
  }) as Promise<string | null>;
}

export function chooseDialog(message: string, title = "Choose", options: ChooseOptions = {}) {
  return showDialog({
    title,
    message,
    confirmLabel: options.confirmLabel,
    cancelLabel: options.cancelLabel || "Cancel",
    mode: "choose",
    choices: options.choices || [],
  }) as Promise<string | null>;
}

export function installDialogs(target: Window & Record<string, any> = window as Window & Record<string, any>) {
  target.mqbAlert = alertDialog;
  target.mqbConfirm = confirmDialog;
  target.mqbPrompt = promptDialog;
  target.mqbChoose = chooseDialog;
}
