import { mount } from "svelte";
import App from "./App.svelte";
import { bootstrapApp } from "./bootstrap";
import { installDialogs } from "./lib/dialogs";

// Register global styles and common components
import "@awesome.me/webawesome/dist/components/dialog/dialog.js";
import "@awesome.me/webawesome/dist/components/icon/icon.js";
import "@awesome.me/webawesome/dist/components/badge/badge.js";

const target = document.getElementById("app");

if (!target) {
  throw new Error("Missing #app mount target");
}

installDialogs();
mount(App, { target });
void bootstrapApp().catch((error) => {
  console.error("Failed to bootstrap app", error);
  target.innerHTML = '<div class="app-init-error">Failed to initialize the app.</div>';
});
