#!/usr/bin/env bash
# CSV -> JSONL, mq-bridge-app only. Runnable on its own:
#
#   ./run_csv_mqb.sh              # typed: with the transform middleware (default)
#   ./run_csv_mqb.sh --untyped    # untyped: raw string passthrough, no transform
#   ROWS=50000 ./run_csv_mqb.sh   # quick smoke run
#
# The two modes measure deliberately different work, and the results CSV keeps
# them as separate rows so nothing can be quoted out of context:
#
#   mq-bridge-app          - `|transform?schema_file=schemas/bench.json` appended
#                            to the output endpoint. `coerce` widens `id` to an
#                            integer and `contentMediaType` decodes the embedded
#                            JSON in `attributes` into a real object, which is
#                            exactly what Sling's type inference produces. This is
#                            the number to compare against a transforming tool,
#                            and the only one the parity check covers.
#   mq-bridge-app-untyped  - no middleware at all: every field stays the string
#                            the CSV reader produced, `attributes` stays a JSON
#                            *string*. This is the honest number to compare
#                            against a tool that also does no transformation, and
#                            it is the floor the transform is measured against.
#
# Quoting the untyped figure against a type-inferring tool would overstate the
# margin — that is the mistake the typed run exists to correct. Report both.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/csv_common.sh"

TRANSFORM=1
case "${1:-}" in
  --untyped|--no-transform) TRANSFORM=0 ;;
  --typed|--transform|"")   TRANSFORM=1 ;;
  *) echo "usage: $0 [--typed|--untyped]" >&2; exit 2 ;;
esac

require_bin
ensure_csv

from="file://${CSV}?format=csv"
if ((TRANSFORM)); then
  label="mq-bridge-app"
  out="$OUT_MQB"
  # The `|transform` suffix appends the middleware to the output endpoint.
  to="file://${out}?format=raw|transform?schema_file=${SCHEMA}"
else
  label="mq-bridge-app-untyped"
  out="$OUT_MQB_RAW"
  to="file://${out}?format=raw"
fi

run_mqb_once() {
  rm -f "$out"
  "$BIN" copy --from "$from" --to "$to" --drain --batch-size 1024 --concurrency 1
}

bench_tool "$label" "$out" "$COPY_TIMEOUT" run_mqb_once

# Output is left in place on purpose: run_csv_parity.sh diffs it against Sling's
# after both tools have exited. run_csv_to_jsonl.sh cleans up at the end.
echo "output kept at $out"
