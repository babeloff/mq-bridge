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

These come from conda-forge rather than being vendored, so the corresponding
cargo features build in seconds instead of minutes:

| conda-forge package | Replaces | Consumed by |
| --- | --- | --- |
| `librdkafka` | librdkafka C source bundled in `rdkafka-sys` (cmake build) | `kafka`, via `rdkafka/dynamic-linking` |
| `libsqlite` | the SQLite amalgamation bundled in `libsqlite3-sys` | `sqlx`, via `sqlx/sqlite-unbundled` |
| `libprotobuf` | `protoc-bin-vendored` | `grpc`, via `$PROTOC` in `build.rs` |
| `zeromq` | — (libzmq is not linked; the `zeromq` endpoint uses the pure-Rust zmq.rs) | libzmq interop peers in tests and the `compare_libzmq` bench |

`pixi run verify-native-deps` prints the versions actually resolved.

Building without pixi still works, but then Kafka needs librdkafka ≥ 2.12.1 and
SQLite ≥ 3.34.1 discoverable through `pkg-config`, gRPC needs `protoc` on `PATH`
or in `$PROTOC`, and SQLite additionally needs `libclang` for bindgen.

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
