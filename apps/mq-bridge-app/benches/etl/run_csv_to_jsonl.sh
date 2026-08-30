#!/usr/bin/env bash
# CSV -> JSONL throughput, all tools. This is the orchestrator; each tool has its
# own script and can be run on its own when only one number needs re-measuring:
#
#   ./run_csv_mqb.sh              mq-bridge-app, with transform  (typed)
#   ./run_csv_mqb.sh --untyped    mq-bridge-app, no middleware   (untyped)
#   ./run_csv_sling.sh            Sling 1.5.21, defaults
#   ./run_csv_meltano.sh          Meltano tap-csv -> target-jsonl
#   ./run_csv_parity.sh           asserts the typed output matches Sling's
#
# Shared dataset/paths/timeouts live in csv_common.sh, timing in lib.sh.
#
# Dataset: data/bench.csv, 1,000,000 rows, seed 42 (id, first_name, country,
# amount, created_at, active, attributes — a mixed-type row, ~116 MiB), generated
# by gen_bench_data.py. Every tool does a one-shot full-file CSV -> local JSONL,
# one JSON object per input row. 1 warmup discarded + $REPEATS timed runs
# (default 2), median/stddev, row-count check on every sample.
#
# TWO mq-bridge numbers are produced, on purpose:
#
#   - `mq-bridge-app` runs a `transform` middleware that reproduces Sling's
#     typing (`id` coerced to integer, `attributes` decoded from an embedded JSON
#     document into a real object). Same per-row work as Sling, priced into the
#     timing, and compare_jsonl.py proves the outputs are identical.
#   - `mq-bridge-app-untyped` runs no middleware: strings in, strings out. That
#     is the right comparison against tools that likewise don't transform — such
#     as Meltano's tap-csv, which also emits every field as a string.
#
# Publishing only the untyped figure against a type-inferring tool would
# overstate the margin; publishing only the typed one hides what the engine costs
# without a transform. The results CSV carries both as separate rows.
#
# Prereqs: a lean build, plus (optionally) the Meltano venv and the Sling binary,
# each of which is skipped if absent:
#   cargo build -p mq-bridge-app --no-default-features --features bench --release
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/csv_common.sh"

require_bin
ensure_csv

# Fresh results file for a full matrix run (the per-tool scripts append, and
# replace their own row, so running one alone doesn't clobber the others).
rm -f "$RESULTS_CSV"
results_init "$RESULTS_CSV"

"$HERE/run_csv_mqb.sh" --typed
"$HERE/run_csv_mqb.sh" --untyped
"$HERE/run_csv_meltano.sh"
"$HERE/run_csv_sling.sh"

# Only meaningful if Sling actually ran; skipped along with it otherwise.
if [[ -e "$OUT_SLING" ]]; then
  "$HERE/run_csv_parity.sh"
fi

rm -f "$OUT_MQB" "$OUT_MQB_RAW"
rm -rf "$OUT_SLING"
echo "done -> $RESULTS_CSV"
