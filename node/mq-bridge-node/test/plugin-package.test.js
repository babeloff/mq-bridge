"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const { definePluginPackage, pluginLibraryPath } = require("..");

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mq-bridge-plugin-"));
  const tag = `${process.platform}-${process.arch}${
    process.platform === "linux" ? "-gnu" : process.platform === "win32" ? "-msvc" : ""
  }`;
  const file = process.platform === "win32"
    ? "reference.dll"
    : process.platform === "darwin"
      ? "libreference.dylib"
      : "libreference.so";
  fs.writeFileSync(
    path.join(root, "mq-bridge-plugin.json"),
    JSON.stringify({ name: "reference", library: "reference" }),
  );
  fs.mkdirSync(path.join(root, "prebuilds", tag), { recursive: true });
  const library = path.join(root, "prebuilds", tag, file);
  fs.writeFileSync(library, "fixture");
  return { root, library };
}

test("pluginLibraryPath selects the current platform prebuild", (t) => {
  const { root, library } = fixture();
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.equal(pluginLibraryPath(root), library);
});

test("definePluginPackage creates the standard thin-package exports", (t) => {
  const { root, library } = fixture();
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const plugin = definePluginPackage(root);
  assert.equal(plugin.ENDPOINT_NAME, "reference");
  assert.equal(plugin.libraryPath(), library);
  assert.equal(typeof plugin.register, "function");
});
