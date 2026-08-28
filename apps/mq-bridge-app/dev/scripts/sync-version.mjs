#!/usr/bin/env node

// Propagates `[workspace.package] version` from Cargo.toml into the files that
// cannot inherit it: server.json (scanned by MCP registry bots, so it must be
// correct in the repo, not just at release time), Tauri's app configuration,
// and the internal path-dependency requirement in Cargo.toml. Run with --check
// to fail instead of writing.

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const checkOnly = process.argv.includes("--check");
const appRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const workspaceRoot = resolve(appRoot, "../..");
const cargoPath = resolve(workspaceRoot, "Cargo.toml");
const serverPath = resolve(appRoot, "server.json");
const tauriPath = resolve(appRoot, "crates/desktop/tauri.conf.json");

const cargo = readFileSync(cargoPath, "utf8");
const version = cargo
  .split(/^\[/m)
  .find((section) => section.startsWith("workspace.package]"))
  ?.match(/^version = "([^"]+)"/m)?.[1];

if (!version) {
  console.error("Could not read [workspace.package] version from Cargo.toml");
  process.exit(1);
}

const drift = [];

function update(path, current, next) {
  if (current === next) return;
  drift.push(path);
  if (!checkOnly) writeFileSync(path, next);
}

const patchedCargo = cargo
  .replace(
    /^(mq-bridge = \{ version = ")[^"]+(".*)$/m,
    `$1${version}$2`,
  )
  .replace(
    /^(mq_bridge_app = \{ version = ")[^"]+(".*)$/m,
    `$1${version}$2`,
  );

// Patched as text rather than re-serialised, so the hand-written formatting
// survives. Every "version" in server.json is the app version, and the only
// tagged identifier is the oci image.
const serverRaw = readFileSync(serverPath, "utf8");
const patchedServer = serverRaw
  .replace(/"version": "[^"]+"/g, `"version": "${version}"`)
  .replace(/("identifier": "[^"]*):[^":]*"/g, `$1:${version}"`);

const tauriRaw = readFileSync(tauriPath, "utf8");
const tauri = JSON.parse(tauriRaw);
if (typeof tauri.version !== "string") {
  console.error("Could not read version from crates/desktop/tauri.conf.json");
  process.exit(1);
}
const patchedTauri = tauriRaw.replace(
  /^(\s*"version"\s*:\s*)"[^"]+"/m,
  `$1"${version}"`,
);
// The regex matches the first `"version"` line at any nesting depth, which is
// only the root property while the file keeps its current shape. Re-parse to
// prove the patch landed there before writing it.
const repatchedTauri = JSON.parse(patchedTauri);
if (repatchedTauri.version !== version) {
  console.error(
    `Patching crates/desktop/tauri.conf.json did not set the root version to ${version}`,
  );
  process.exit(1);
}

// Nothing is written until every file has parsed and validated, so a bad
// manifest cannot leave the repo half-synced.
update(cargoPath, cargo, patchedCargo);
update(serverPath, serverRaw, patchedServer);
update(tauriPath, tauriRaw, patchedTauri);

if (drift.length === 0) {
  console.log(`Version ${version} is in sync.`);
} else if (checkOnly) {
  const files = drift.map((path) => `  ${path}`).join("\n");
  console.error(`Out of sync with version ${version}:\n${files}\nRun: npm run sync:version`);
  process.exit(1);
} else {
  console.log(`Synced to version ${version}:\n${drift.map((p) => `  ${p}`).join("\n")}`);
}
