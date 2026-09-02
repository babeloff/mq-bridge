const { resolve } = require("path");
const viteConfig = require("./vite.config.js");
const { defineConfig, mergeConfig } = require("vitest/config");

module.exports = mergeConfig(
  viteConfig,
  defineConfig({
    resolve: {
      alias: [
        // Vitest transforms through the SSR pipeline, so a component's bare
        // `import { onDestroy } from "svelte"` resolves to Svelte's server
        // entrypoint and throws the moment the component mounts. Pinning the
        // bare specifier to the client entry is what lets a real component be
        // mounted in a test; `svelte/store` and friends are unaffected.
        {
          find: /^svelte$/,
          replacement: resolve(__dirname, "node_modules/svelte/src/index-client.js"),
        },
      ],
    },
    test: {
      environment: "node",
      setupFiles: [resolve(__dirname, "tests/unit/dom-setup.ts")],
      include: ["../tests/unit/**/*.test.ts"],
      // Setup hooks that pull in a cold module graph exceed the 10s default on a
      // CI runner with an empty vite cache, which fails the file before a single
      // assertion runs.
      hookTimeout: 30_000,
      coverage: {
        enabled: false,
        reporter: ["text-summary", "html", "json-summary"],
        reportsDirectory: resolve(__dirname, "coverage"),
        include: ["ui/src/**/*.{ts,svelte}"],
        exclude: ["ui/src/lib/generated/**", "ui/src/**/*.d.ts"],
      },
    },
  }),
);
