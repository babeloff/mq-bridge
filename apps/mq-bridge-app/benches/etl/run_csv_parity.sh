#!/usr/bin/env bash
# Record-level parity check for the CSV -> JSONL scenario: mq-bridge-app's typed
# output vs Sling's. Runnable on its own once both runners have produced output:
#
#   ./run_csv_mqb.sh && ./run_csv_sling.sh && ./run_csv_parity.sh
#
# This is what makes the timings mean something. Comparing a string passthrough
# against a type-inferring tool would flatter mq-bridge, so the two outputs have
# to be proven identical before the numbers are quoted side by side. A mismatch
# fails, loudly — it is not a warning.
#
# Records are compared as parsed JSON: mq-bridge keeps the source column order
# inside `attributes` and Sling alphabetizes it, which is a serialization
# difference, not a data one.
#
# The untyped run (mq-bridge-app-untyped) is deliberately NOT checked here — it
# emits strings where Sling emits typed values, which is the whole point of it.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/csv_common.sh"

for f in "$OUT_MQB" "$OUT_SLING"; do
  [[ -e "$f" ]] || { echo "missing $f — run run_csv_mqb.sh and run_csv_sling.sh first" >&2; exit 1; }
done

echo "-- parity: mq-bridge-app vs sling"
if ! python3 "$HERE/compare_jsonl.py" "$OUT_MQB" "$OUT_SLING"; then
  echo "  FAILED: outputs diverge — the timings are not comparable" >&2
  exit 1
fi
