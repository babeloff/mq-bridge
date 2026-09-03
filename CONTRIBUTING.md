# Contributing to mq-bridge

Thank you for your interest in contributing to **mq-bridge**! We welcome bug reports, feature requests, documentation improvements, and code contributions.

## Getting Started

The repository is a [pixi](https://pixi.sh) workspace, which is the supported way
to get a build environment. It pins the Rust toolchain *and* every native library
the optional features link against, so there is nothing to install by hand and
nothing for a `-sys` crate to compile from vendored source.

1. **Fork the repository** and clone your fork locally.
2. **Install pixi** — <https://pixi.sh/latest/#installation>.
3. **Enter the environment and run the tests:**
   ```sh
   pixi run test              # cargo test --lib --features full
   ```
   The first `pixi run` solves `pixi.toml` against the checked-in `pixi.lock` and
   materialises `.pixi/envs/` (git-ignored). Use `pixi shell` for an interactive
   session in which plain `cargo` commands work.

Some integration tests need message brokers; `tests/integration/docker-compose/`
has a Compose file per broker, so you don't have to install them natively.

### Native dependencies

Only two endpoints link a C library whose provenance is a choice — `kafka`
(librdkafka) and `sqlx` (SQLite) — plus IBM MQ, whose client is always a shared
object and differs only in when it is resolved. That gives three build variants:

| Task | Feature set | librdkafka / SQLite | IBM MQ client |
| --- | --- | --- | --- |
| `pixi run build-static` | `full` | compiled in | runtime `dlopen`, optional |
| `pixi run build-static-ibm-mq` | `full-static-ibm-mq` | compiled in | bound at link time, required at build |
| `pixi run build-dynamic` | `full-dynamic` | linked from the environment | runtime `dlopen`, optional |

The linkage is chosen by the `link-static` / `link-dynamic` cargo features,
which are deliberately **orthogonal** to the endpoint features: they gate no
code, so the `#[cfg(feature = "kafka")]` / `#[cfg(feature = "sqlx")]` sites are
unaffected by the choice. Enable exactly one — `src/lib.rs` rejects both, and
rejects neither-when-`sqlx`-is-on, with an explanatory `compile_error!`.

That is also why CI lints with `--features lint-all` rather than
`--all-features`: the latter would switch both linkage features on at once.

`full` stays self-contained so `cargo add mq-bridge --features full` needs no
system librdkafka or libsqlite. `full-dynamic` exists for conda-forge and distro
packaging, where the shared libraries have to stay patchable.

What conda-forge supplies:

| conda-forge package | Needed by |
| --- | --- |
| `libprotobuf` | every variant — `grpc` runs `protoc` (replaces `protoc-bin-vendored`) |
| `librdkafka` | `link-dynamic` only, via `rdkafka/dynamic-linking` |
| `libsqlite` + `libclang` | `link-dynamic` only, via `sqlx/sqlite-unbundled` (bindgen) |
| `cmake`, `c-compiler` | `link-static` (librdkafka, SQLite) and always for aws-lc/ring/zstd |
| `zeromq` | libzmq interop peers in tests; the `zeromq` endpoint is pure-Rust zmq.rs and links nothing |

`pixi run verify-native-deps` prints the versions actually resolved.

Building without pixi still works: `full` needs only `protoc` plus a C
compiler and cmake, while `full-dynamic` additionally needs librdkafka ≥ 2.12.1
and SQLite ≥ 3.34.1 discoverable through `pkg-config`, and `libclang`.

For IBM MQ specifically — installing the client, the loader's search order, TLS
key repositories — see [docs/IBM_MQ.md](docs/IBM_MQ.md).

### Task reference

`pixi task list` shows all of them. The most used:

| Task | Command |
| --- | --- |
| `pixi run build-full` | `cargo build --features full` |
| `pixi run test` | unit tests, full features |
| `pixi run test-no-docker` | integration tests needing no services |
| `pixi run fmt` / `fmt-check` | `cargo fmt --all` |
| `pixi run clippy` | `cargo clippy --all-targets --all-features -- -D warnings` |
| `pixi run check-features` | the per-feature-subset `cargo check` sweep CI runs |
| `pixi run -e dev test-integration` | the Docker-backed nextest suite |
| `pixi run -e release bump-version 0.4.11` | set the version everywhere |
| `pixi run -e release check-version` | fail if a copy of the version has drifted |

### Bumping the version

The root `Cargo.toml` `[workspace.package] version` is the source of truth;
`scripts/sync-version.mjs` fans it out to every committed copy — `pixi.toml`,
both `Cargo.toml`/`Cargo.lock` pairs, `server.json`, `tauri.conf.json` and the
Node `package.json`/`package-lock.json`. `pixi.lock` is not touched: it records
no workspace version, and its top-level `version: 7` is the lockfile format
number.

The `release` environment exists only to carry `nodejs` for that script, so the
environments CI builds and tests in stay free of it. The same script is also
reachable as `npm run sync-version` from `node/mq-bridge-node` and as
`npm run sync:version` / `check:version` from `apps/mq-bridge-app`.

`apps/mq-bridge-app` is a separate cargo workspace with its own toolchain
expectations; it is not covered by this pixi workspace.

## Code Style

- Run `cargo fmt --all` before submitting a PR.
- Ensure code passes `cargo clippy --all-features -- -D warnings`.
- Follow idiomatic Rust and existing code conventions.

## Making Changes

- **New endpoints or middleware:**
  - Add new files in `src/endpoints/` or `src/middleware/`.
  - Update factory functions in `mod.rs` as needed.
  - Add configuration models to `src/models.rs`.
- **Tests:**
  - Add or update unit tests in the relevant module.
  - Add integration tests in `tests/integration/` if applicable.
- **Documentation:**
  - Update `README.md` and add doc comments for public APIs.

## Running Tests

Inside `pixi shell` (or prefixed with `pixi run`):

- **Unit tests:**
  ```sh
  cargo test
  ```
- **Integration tests:**
  ```sh
  cargo test --test integration_test --features full
  ```
- **Performance tests:**
  ```sh
  cargo bench
  ```
- **Memory tests:**
  ```sh
  cargo test --test memory_leak_test --features full -- --ignored
  ```

Some integration tests require Docker services. See `tests/integration/docker-compose/` for setup.

## Submitting a Pull Request

1. **Create a branch** for your change.
2. **Write clear commit messages**.
3. **Open a pull request** against the `main` branch.
4. **Describe your changes** and reference any related issues.
5. Ensure all tests pass and CI checks succeed.

## Reporting Issues

- Use [GitHub Issues](https://github.com/marcomq/mq-bridge/issues) for bugs, enhancements, or questions.
- Provide as much detail as possible (logs, configs, steps to reproduce).

## Code of Conduct

Be respectful and inclusive. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) if present.

---

Thank you for helping make **mq-bridge** better!
