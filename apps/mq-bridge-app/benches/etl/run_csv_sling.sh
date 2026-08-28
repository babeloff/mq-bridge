#!/usr/bin/env bash
# CSV -> JSONL, Sling only. Runnable on its own: ./run_csv_sling.sh
#
# Sling 1.5.21, run at its defaults — defaults are what a user actually types,
# and type inference is part of what Sling is. That inference is why the
# mq-bridge run carries a transform: see run_csv_mqb.sh.
#
# `file_max_rows: 0` is load-bearing. Sling's default is 500000, so at 1M rows it
# silently emits a *directory of part files* instead of one file, which breaks
# both the row count and the parity check.
#
# Skipped (not fatal) if the binary is absent — see lib.sh for the install command.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/csv_common.sh"

if [[ ! -x "$SLING_BIN" ]]; then
  echo "-- sling: not found at $SLING_BIN, skipping (see lib.sh for the install command)" >&2
  exit 0
fi

ensure_csv

run_sling_once() {
  rm -rf "$OUT_SLING"
  "$SLING_BIN" run \
    --src-stream "file://${CSV}" \
    --src-options '{"format":"csv","header":true}' \
    --tgt-object "file://${OUT_SLING}" \
    --tgt-options '{"format":"jsonlines","file_max_rows":0}'
}

bench_tool "sling" "$OUT_SLING" "$SLING_TIMEOUT" run_sling_once

echo "output kept at $OUT_SLING"
