# Prescribed actions for mq-bridge. Each recipe is the command
# .github/workflows/ runs, so a green `just ci` means the PR gate has already
# been answered locally. When a workflow changes, change it here too.
#
# Two cargo workspaces live in this repository: the engine at the root, and the
# application under apps/mq-bridge-app with its own Cargo.lock. Recipes for the
# latter are prefixed `app-` and run from inside it.
#
# `just` on its own lists everything, grouped.

# The feature set for every lint and doc build. NOT `--all-features`: that
# enables `link-static` and `link-dynamic` together, which src/lib.rs rejects
# with a compile_error!. See the `lint-all` comment in Cargo.toml.
lint_features := "lint-all"

_default:
    @just --list

# Fail early, with instructions, for the recipes that need a protoc the build
# does not supply itself.
#
# The engine's own `grpc` needs none: `full` and `lint-all` include
# `vendored-protoc`, which hands its build script a prebuilt binary. Two things
# are not covered by that. The app workspace depends on `pulsar`, whose build
# script calls protoc and vendors nothing — `std::env::set_var("PROTOC", …)` in
# the engine's build script sets it only in that process, not in a sibling's.
# And `full-dynamic` deliberately drops `vendored-protoc`, because a distro or
# conda-forge build has to compile against the protobuf it packages.
_require-protoc:
    #!/usr/bin/env bash
    if [ -n "${PROTOC:-}" ] && [ -x "${PROTOC:-}" ]; then exit 0; fi
    if command -v protoc >/dev/null 2>&1; then exit 0; fi
    cat >&2 <<'EOF'
    error: protoc not found, and this target does not vendor one.

      Fedora        sudo dnf install protobuf-compiler
      Debian        sudo apt-get install protobuf-compiler
      macOS         brew install protobuf
      conda-forge   <env>/bin/protoc, from the libprotobuf package
      Windows       choco install protoc

    Or point $PROTOC at a binary you already have.
    EOF
    exit 1

# Fail early for `build-dynamic`, which needs bindgen.
#
# `sqlite-unbundled` resolves to libsqlite3-sys/buildtime_bindgen, and bindgen
# dlopens libclang at build time. `link-static` does not: it compiles the
# SQLite amalgamation, whose bindings are pre-generated. So this is a cost of
# the dynamic variant alone, and one the librdkafka/libsqlite provider does not
# necessarily also supply.
#
# clang-sys, which is how bindgen loads it, checks $LIBCLANG_PATH first and
# then a set of standard directories.
_require-libclang:
    #!/usr/bin/env bash
    shopt -s nullglob
    # Each candidate is tested with -e, NOT by counting glob matches: nullglob
    # only drops patterns that contain metacharacters, so a literal
    # `libclang.dylib` survives even when the file does not exist and would
    # make every directory look like a hit.
    holds_libclang() {
        local file
        for file in "$1"/libclang.so* "$1"/libclang.dylib "$1"/libclang*.dll; do
            [ -e "$file" ] && return 0
        done
        return 1
    }
    if [ -n "${LIBCLANG_PATH:-}" ]; then
        holds_libclang "$LIBCLANG_PATH" && exit 0
        echo "error: \$LIBCLANG_PATH=$LIBCLANG_PATH contains no libclang." >&2
        exit 1
    fi
    for dir in /usr/lib64 /usr/lib /usr/lib/x86_64-linux-gnu /usr/local/lib \
               /usr/lib/llvm*/lib /opt/homebrew/opt/llvm/lib /usr/local/opt/llvm/lib; do
        holds_libclang "$dir" && exit 0
    done
    if command -v llvm-config >/dev/null 2>&1; then
        holds_libclang "$(llvm-config --libdir)" && exit 0
    fi
    cat >&2 <<'EOF'
    error: libclang not found, and `full-dynamic` needs it — sqlite-unbundled
           generates its bindings with bindgen, which dlopens libclang.

      Fedora        sudo dnf install clang-devel
      Debian        sudo apt-get install libclang-dev
      macOS         brew install llvm
      pixi          pixi global install libclang
      conda-forge   the libclang package

    Or set $LIBCLANG_PATH to the directory holding libclang.so.
    EOF
    exit 1

# Check that the recipes compiling pyo3 have an interpreter to compile against.
#
# Only the chain those recipes actually leave pyo3: $PYO3_PYTHON, then
# $VIRTUAL_ENV, then PATH. $CONDA_PREFIX is deliberately NOT considered — the
# python recipes unset it, for the reasons documented above that group — so
# treating it as a candidate here would reject the very recipe that fixes it.
_require-python:
    #!/usr/bin/env bash
    if [ -n "${PYO3_PYTHON:-}" ]; then
        [ -x "${PYO3_PYTHON}" ] && exit 0
        echo "error: \$PYO3_PYTHON=${PYO3_PYTHON} is not executable." >&2
        exit 1
    fi
    if [ -n "${VIRTUAL_ENV:-}" ]; then
        if [ -x "${VIRTUAL_ENV}/bin/python" ] || [ -x "${VIRTUAL_ENV}/bin/python3" ]; then exit 0; fi
        cat >&2 <<EOF
    error: \$VIRTUAL_ENV=${VIRTUAL_ENV} has no bin/python, and pyo3 prefers it
           over PATH rather than falling through.

      Clear it, or point \$PYO3_PYTHON at a real interpreter:
        unset VIRTUAL_ENV
        PYO3_PYTHON=\$(command -v python3) just <recipe>
    EOF
        exit 1
    fi
    command -v python3 >/dev/null 2>&1 && exit 0
    echo "error: no python3 on PATH, \$PYO3_PYTHON unset and \$VIRTUAL_ENV unset." >&2
    exit 1

# --- Gates --------------------------------------------------------------------

[doc('Everything ci.yml gates a PR on, bar the Docker suites')]
[group('gate')]
ci: fmt-check lint config-compat check-features test test-no-docker doc

[doc('Format the workspace')]
[group('gate')]
fmt:
    cargo fmt --all

[doc('Fail on unformatted code')]
[group('gate')]
fmt-check:
    cargo fmt --all -- --check

[doc('Clippy as CI does, warnings denied')]
[group('gate')]
lint:
    cargo clippy --all-targets --features {{ lint_features }} -- -D warnings

[doc('Build rustdoc, warnings denied')]
[group('gate')]
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --features {{ lint_features }} --no-deps

# Not a workspace default member, so nothing else builds it. It pins the
# exhaustive `GrpcConfig` literal a downstream crate would write.
[doc('Check the downstream config-compat fixture')]
[group('gate')]
config-compat:
    cargo check -p grpc-config-compat

# `grpc` carries `vendored-protoc` because protoc no longer arrives with the
# feature itself and a bare checkout may have none installed; every `full*` set
# already includes it.
[doc('cargo check each feature subset CI covers')]
[group('gate')]
check-features:
    #!/usr/bin/env bash
    set -uo pipefail
    failed=""
    for f in "" full kafka nats grpc,vendored-protoc mqtt mongodb http; do
        echo "::: cargo check --features ${f:-<default>}"
        if [ -z "$f" ]; then
            cargo check --all-targets || failed="$failed <default>"
        else
            cargo check --all-targets --features "$f" || failed="$failed $f"
        fi
    done
    if [ -n "$failed" ]; then echo "FAILED:$failed" >&2; exit 1; fi

# A hard gate in CI: an unapproved license, a banned crate or a non-crates.io
# source is always actionable here.
[doc('Licenses, bans and sources')]
[group('gate')]
deny:
    cargo deny check bans licenses sources

# Informational in CI, because every current advisory is in a transitive
# dependency we cannot upgrade ourselves. deny.toml carries the reasoning.
[doc('RustSec advisories (never fails)')]
[group('gate')]
deny-advisories:
    -cargo deny check advisories

# --- Tests --------------------------------------------------------------------

[doc('Unit tests, full features')]
[group('test')]
test:
    cargo test --lib --features full

[doc('Integration tests that need no Docker services')]
[group('test')]
test-no-docker:
    cargo test --test ref_test --test sqlite_test --test tls_example --test websocket_test --features full

# #[ignore]d, so it has to be named explicitly.
[doc('Route commit-task JoinSet leak soak')]
[group('test')]
test-memory-leak:
    cargo test --test memory_leak_test --features full -- --ignored --nocapture

# Compiled once into an archive, as ci.yml does, so the filtered runs need no
# recompilation.
[doc('Build the nextest archive for the Docker suites')]
[group('test')]
integration-archive:
    cargo nextest archive --release --features full,test-utils \
        --test integration_test --test armature_integration \
        --archive-file nextest-archive.tar.zst

# Needs a running Docker daemon, the archive above, and the TLS fixtures from
# `just integration-certs`. Narrow it with a nextest filter, e.g.
# `just integration 'binary(integration_test) & test(=test_all_status)'`.
[doc('Run the Docker suites from the archive; takes a nextest filter')]
[group('test')]
integration filter='all()':
    cargo nextest run --archive-file nextest-archive.tar.zst \
        --run-ignored all --no-capture -E '{{ filter }}'

[doc('Generate the TLS fixtures the integration services need')]
[group('test')]
integration-certs:
    #!/usr/bin/env bash
    set -euo pipefail
    chmod +x tests/integration/scripts/gen_certs.sh
    for svc in mongodb kafka ibm-mq; do ./tests/integration/scripts/gen_certs.sh "$svc"; done

# --- Build variants -----------------------------------------------------------
#
# Two ways to obtain the C libraries; see "The two build variants" in
# Cargo.toml. They differ only in librdkafka and libsqlite — IBM MQ reaches
# both the same way, via runtime dlopen.

# librdkafka and SQLite compiled from bundled sources, protoc from the vendored
# binary, IBM MQ resolved at runtime via dlopen. Needs nothing installed.
[doc('Self-contained release build (the default variant)')]
[group('build')]
build-static:
    cargo build --release --features full

# Links librdkafka >= 2.12.1 and libsqlite3 >= 3.34.1 from the environment via
# pkg-config. What a conda-forge recipe or a distro package wants, so the
# shared libraries stay patchable.
#
# Three prerequisites, from three different places, which is why this recipe is
# more than a cargo line:
#
#   librdkafka + libsqlite3   pkg-config. A distro's librdkafka-dev +
#                             libsqlite3-dev, or `pixi global install
#                             librdkafka libsqlite`.
#   protoc                    $PROTOC or PATH — `full-dynamic` drops
#                             `vendored-protoc` on purpose.
#   libclang                  bindgen, for the SQLite bindings that
#                             `sqlite-unbundled` generates.
#
# The rpath is derived from pkg-config rather than hard-coded, because the
# shared libraries generally live somewhere the loader does not search by
# default — a conda prefix, a Conan package cache — and a binary linked here
# would not start without it. rustc does not read LDFLAGS, hence RUSTFLAGS.
# Appended, so an ambient RUSTFLAGS survives.
#
# Verified on linux-64: readelf on the resulting test binary shows
# librdkafka.so.1 and libsqlite3.so in DT_NEEDED, a RUNPATH covering both, and
# 180 rd_kafka* / 90 sqlite3_* symbols imported rather than compiled in.
[doc('Link librdkafka and libsqlite from the environment')]
[group('build')]
build-dynamic: _require-protoc _require-libclang
    #!/usr/bin/env bash
    set -euo pipefail
    if ! pkg-config --exists rdkafka sqlite3; then
        echo "error: rdkafka and/or sqlite3 not on PKG_CONFIG_PATH." >&2
        echo "       'just check-native-deps' reports what is missing." >&2
        exit 1
    fi
    for dir in $(pkg-config --libs-only-L rdkafka sqlite3 | tr ' ' '\n' | sed -n 's/^-L//p' | sort -u); do
        RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-Wl,-rpath,$dir"
    done
    export RUSTFLAGS
    cargo build --release --features full-dynamic

# Reports rather than fails on the first problem, so it is usable as a
# diagnosis of a broken build-dynamic. Exits non-zero if anything is missing.
[doc('Check the environment can satisfy build-dynamic')]
[group('build')]
check-native-deps:
    #!/usr/bin/env bash
    missing=0
    for probe in "rdkafka >= 2.12.1" "sqlite3 >= 3.34.1"; do
        name="${probe%% *}"
        if version=$(pkg-config --modversion "$name" 2>/dev/null); then
            printf '  %-12s %s\n' "$name" "$version"
            pkg-config "$probe" || { echo "    ^ too old, need '$probe'"; missing=1; }
        else
            printf '  %-12s MISSING\n' "$name"; missing=1
        fi
    done
    if command -v protoc >/dev/null 2>&1; then
        printf '  %-12s %s\n' protoc "$(protoc --version)"
    elif [ -n "${PROTOC:-}" ] && [ -x "${PROTOC}" ]; then
        printf '  %-12s %s\n' protoc "$("$PROTOC" --version) (\$PROTOC)"
    else
        printf '  %-12s MISSING\n' protoc; missing=1
    fi
    if just _require-libclang >/dev/null 2>&1; then
        printf '  %-12s found\n' libclang
    else
        printf '  %-12s MISSING\n' libclang; missing=1
    fi
    exit $missing

# --- Python bindings ----------------------------------------------------------
#
# These recipes drop $CONDA_PREFIX, which CI never has but a local shell often
# does — `pixi global install` and `conda activate` both export it. It breaks
# the Python build two different ways:
#
#   * pyo3-ffi picks its interpreter from $PYO3_PYTHON, then $VIRTUAL_ENV, then
#     $CONDA_PREFIX, and does not fall through when the one it picked is wrong.
#     A prefix with no bin/python fails the build naming a path you never chose.
#   * maturin refuses outright when $VIRTUAL_ENV and $CONDA_PREFIX are *both*
#     set ("Please unset one of them") — and `uv run` always sets the first.
#     This one bites even when $CONDA_PREFIX is a perfectly good env, so
#     pointing $PYO3_PYTHON somewhere valid does not rescue it.
#
# uv owns the interpreter for this project (python/mq-bridge-py/.venv), so
# $CONDA_PREFIX has no say here and dropping it is the fix for both.

[doc('Sync the dev environment and build the extension into it')]
[group('python')]
py-dev: _require-python
    #!/usr/bin/env bash
    set -euo pipefail
    cd python/mq-bridge-py
    unset CONDA_PREFIX
    uv sync --group dev --no-install-project
    uv run maturin develop

[doc('Run the Python test suite')]
[group('python')]
py-test:
    #!/usr/bin/env bash
    set -euo pipefail
    cd python/mq-bridge-py
    unset CONDA_PREFIX
    uv run pytest -q

# python.yml's regression check that the lean, no-default build still exposes
# the always-on public API. The two cargo steps run outside uv, as they do in
# CI, so they need an interpreter of their own — hence _require-python.
[doc('Lean no-default-features regression tests')]
[group('python')]
py-test-lean: _require-python
    #!/usr/bin/env bash
    set -euo pipefail
    unset CONDA_PREFIX
    cargo test -p mq-bridge-py --no-default-features --features http,rustls-ring test_config_schema_is_always_available
    cargo test -p mq-bridge-py --no-default-features --features http,rustls-ring test_module_init_installs_rustls_provider
    cd python/mq-bridge-py
    uv run maturin develop --no-default-features -F http -F rustls-ring -F pyo3/extension-module
    uv run pytest -q tests/test_public_api.py tests/test_config_types.py

# --- Application (separate workspace) -----------------------------------------

[doc('Everything app.yml gates the app crates on')]
[group('app')]
app-ci: app-check app-lint app-test

[doc('cargo check the app crates')]
[group('app')]
app-check: _require-protoc
    cd apps/mq-bridge-app && cargo check -p mq-bridge-app-core -p mq-bridge-app --all-targets

[doc('Clippy the app crates, warnings denied')]
[group('app')]
app-lint: _require-protoc
    cd apps/mq-bridge-app && cargo clippy -p mq-bridge-app-core -p mq-bridge-app --all-targets -- -D warnings

[doc('Unit and bin tests for the app crates')]
[group('app')]
app-test: _require-protoc
    cd apps/mq-bridge-app && cargo test -p mq-bridge-app-core -p mq-bridge-app --lib --bins

# Reaches the engine's `link-dynamic` through the passthroughs in crates/core
# and crates/cli. `--no-default-features` is required: `default = ["full"]`
# would otherwise be on too and collide with it.
[doc('The app, linked against the environment libraries')]
[group('app')]
app-build-dynamic: _require-protoc
    cd apps/mq-bridge-app && cargo build --release -p mq-bridge-app \
        --no-default-features --features full-dynamic

# --- Node bindings ------------------------------------------------------------

[doc('cargo check the node bindings as node.yml does')]
[group('node')]
node-check:
    cargo check -p mq-bridge-node --no-default-features --features "http middleware schema"

# --- Release chores -----------------------------------------------------------

# Everything downstream of the root [workspace.package] version is rewritten
# from it. Pass the new version, or none to re-sync.
[doc('Set the version everywhere')]
[group('release')]
version new='':
    node scripts/sync-version.mjs {{ new }}

[doc('Fail if any committed copy of the version has drifted')]
[group('release')]
version-check:
    node scripts/sync-version.mjs --check
