#!/usr/bin/env node
// Sync this package's version to the Cargo workspace version, the single source
// of truth (matches how maturin derives the Python package version). Updates
// package.json `version` and every `optionalDependencies` entry in lockstep.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const pkgPath = resolve(here, "..", "package.json");
const cargoPath = resolve(here, "..", "..", "..", "Cargo.toml");

const cargo = readFileSync(cargoPath, "utf8");
const match = cargo.match(/\[workspace\.package\][\s\S]*?version\s*=\s*"([^"]+)"/);
if (!match) {
  console.error("Could not find [workspace.package] version in Cargo.toml");
  process.exit(1);
}
const version = match[1];

const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
pkg.version = version;
if (pkg.optionalDependencies) {
  for (const name of Object.keys(pkg.optionalDependencies)) {
    pkg.optionalDependencies[name] = version;
  }
}
writeFileSync(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`);
console.log(`Synced package version to ${version}`);
