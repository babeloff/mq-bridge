/**
 * One app process per Playwright worker.
 *
 * Every spec drives the same global `/config`, which is the reason the suite ran
 * on a single worker. Giving each worker its own process removes the shared
 * state rather than serialising around it: own config file, own port, own peer
 * registry. The binary is built once in global setup, so a worker only execs it —
 * `cargo run` per worker would serialise them all on the cargo build lock.
 */
const { execFileSync, spawn } = require("child_process");
const { mkdtempSync, rmSync, writeFileSync } = require("fs");
const { tmpdir } = require("os");
const { join } = require("path");

const APP_ROOT = join(__dirname, "..", "..");
const HEALTH_TIMEOUT_MS = 20_000;
const START_ATTEMPTS = 8;

/**
 * Guess a port and let the bind decide, rather than probing with a throwaway
 * listener first: a port that a probe socket has just released still fails to
 * bind for a moment, which loses three starts out of four when the workers come
 * up together. Losing a race here costs one retry.
 */
function candidatePort() {
  return 40000 + Math.floor(Math.random() * 20000);
}

function resolveBinary() {
  const metadata = JSON.parse(
    execFileSync("cargo", ["metadata", "--format-version", "1", "--no-deps"], {
      cwd: APP_ROOT,
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
    }),
  );
  return join(metadata.target_directory, "debug", "mq-bridge-app");
}

/** Global setup builds once and publishes the path; workers only read it. */
function buildBinary() {
  execFileSync("cargo", ["build", "-p", "mq-bridge-app", "--bin", "mq-bridge-app"], {
    cwd: APP_ROOT,
    stdio: "inherit",
  });
  return resolveBinary();
}

/** Resolves when the app answers, rejects as soon as it exits without doing so. */
async function waitForHealth(url, child, stderr) {
  const deadline = Date.now() + HEALTH_TIMEOUT_MS;
  for (;;) {
    try {
      // Bounded per-probe: a stalled response must not eat the whole budget.
      if ((await fetch(url, { signal: AbortSignal.timeout(2000) })).ok) return;
    } catch {
      // Not listening yet.
    }
    // A port collision kills the process outright. Noticing that beats waiting
    // out the whole health budget for an answer that is never coming.
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`app exited before answering ${url}\n${stderr().slice(-2000)}`);
    }
    if (Date.now() > deadline) {
      throw new Error(`app did not answer ${url} within ${HEALTH_TIMEOUT_MS}ms\n${stderr().slice(-2000)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

async function startApp() {
  let lastError;
  for (let attempt = 0; attempt < START_ATTEMPTS; attempt += 1) {
    try {
      return await startAppOnce();
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

async function startAppOnce() {
  const binary = process.env.MQB_APP_BINARY || resolveBinary();
  const port = candidatePort();
  const directory = mkdtempSync(join(tmpdir(), "mqb-pw-"));
  const configPath = join(directory, "config.yml");
  // metrics_addr must be pinned to the UI port, not left out: a config with no
  // consumers counts as "unconfigured", which makes the CLI fall back to the
  // fixed DEFAULT_METRICS_ADDR, and every worker after the first then dies
  // binding it. Matching ui_addr is the documented way to say "no second
  // listener" — the UI serves /metrics itself.
  writeFileSync(
    configPath,
    [
      `ui_addr: "127.0.0.1:${port}"`,
      `metrics_addr: "127.0.0.1:${port}"`,
      `log_level: "info"`,
      `publishers: []`,
      `consumers: []`,
      `routes: {}`,
      ``,
    ].join("\n"),
  );

  const child = spawn(binary, ["--config", configPath], {
    cwd: APP_ROOT,
    stdio: ["ignore", "ignore", "pipe"],
    env: {
      ...process.env,
      // The peer registry lives under the runtime directory. Without a private
      // one the workers discover each other — and any mq-bridge the developer
      // happens to be running — and render each other's routes as peer rows.
      TMPDIR: directory,
      XDG_RUNTIME_DIR: directory,
    },
  });

  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  const baseURL = `http://127.0.0.1:${port}`;
  try {
    await waitForHealth(`${baseURL}/health`, child, () => stderr);
  } catch (error) {
    child.kill("SIGKILL");
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }

  return {
    baseURL,
    async stop() {
      if (child.exitCode === null) {
        child.kill("SIGTERM");
        await new Promise((resolve) => {
          const forced = setTimeout(() => {
            child.kill("SIGKILL");
            resolve();
          }, 5_000);
          child.once("exit", () => {
            clearTimeout(forced);
            resolve();
          });
        });
      }
      rmSync(directory, { recursive: true, force: true });
    },
  };
}

module.exports = { buildBinary, startApp };
