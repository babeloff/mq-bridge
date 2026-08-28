# Desktop App

The Tauri desktop application: the same UI the CLI serves in a browser, shipped
as a native window with OS key-store access for encrypted config and secrets.

You can build it by running `cargo run -p mq-bridge-app-desktop`

Crate layout:

- `crates/core`: shared backend logic and transport-agnostic application services
- `crates/cli`: installable HTTP/CLI application published as `mq-bridge-app`
- `crates/desktop`: the Tauri application, depending on `crates/core`

Keeping the desktop app in its own crate avoids pulling Tauri dependencies into the CLI build or the `cargo install mq-bridge-app` package.
