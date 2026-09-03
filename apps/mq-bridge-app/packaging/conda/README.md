# Conda distribution

The recipe builds the full `mq-bridge-app` CLI from the checked-out source and
installs both `mq-bridge-app` and `mqb`. It targets native `linux-64`,
`osx-arm64`, and `win-64` builds in CI; conda is primarily the convenient
package-manager route for Windows users who cannot use Homebrew.

## Build locally

Install [rattler-build](https://rattler.build/) and run from the repository root:

```bash
export MQ_BRIDGE_VERSION="$(cargo metadata --manifest-path apps/mq-bridge-app/Cargo.toml --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.name == "mq-bridge-app") | .version')"
rattler-build build \
  --recipe apps/mq-bridge-app/packaging/conda/recipe.yaml \
  -c conda-forge
```

On PowerShell, set `$env:MQ_BRIDGE_VERSION` to the version from
`apps/mq-bridge-app/Cargo.toml`, then run the same `rattler-build` command.

The build runs the recipe tests, including `mqb --version`. Generated packages
are written below `output/<platform>/`.

## Publish releases

`.github/workflows/conda.yml` builds packages on pull requests that change the
recipe, on manual dispatch, and when a GitHub release is published. Every build
is retained as a workflow artifact.

To additionally publish release builds to the `marcomq` channel on
Anaconda.org, create an Anaconda API token with upload permission and save it as
the repository secret `ANACONDA_API_TOKEN`. Users can then install with:

```bash
conda install -c marcomq -c conda-forge mq-bridge-app
```

The upload step is skipped, with a workflow warning, until that secret exists.

## Submit to conda-forge

Conda-forge feedstocks build from immutable release sources. Before copying the
recipe into `conda-forge/staged-recipes`, replace the local `source.path` with a
tag archive URL and SHA-256, and replace the environment-derived version with
the released version. The build, requirements, tests, and metadata can remain
the same.
