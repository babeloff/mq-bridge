#!/usr/bin/env bash
# CSV -> JSONL, Meltano only. Runnable on its own: ./run_csv_meltano.sh
#
# tap-csv 1.3.2 -> target-jsonl 0.1.4. Like mq-bridge's CSV reader and unlike
# Sling, tap-csv emits every field as a string, so this is an untyped baseline —
# compare it against `mq-bridge-app-untyped`, not the transform run.
#
# Skipped (not fatal) if the venv is absent: it is gitignored, so a fresh
# checkout can still run the other tools. Install with:
#   (cd meltano_project/bench && ../.venv/bin/meltano install extractor tap-csv)
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/csv_common.sh"

if [[ ! -x "$MELTANO_BIN" ]]; then
  echo "-- meltano: venv not found at $MELTANO_BIN, skipping" >&2
  exit 0
fi

ensure_csv

# meltano.yml pins tap-csv at ../../data/bench.csv, so without this override a
# reduced-scale run (ROWS/CSV) still pushed the full 1M rows through Meltano —
# minutes of silence that read as a hang. Meltano maps plugin config to
# <PLUGIN>_<SETTING>, so TAP_CSV_FILES redirects it at whatever $CSV is.
export TAP_CSV_FILES="[{\"entity\":\"bench\",\"path\":\"${CSV}\",\"keys\":[\"id\"]}]"

run_meltano_once() {
  rm -rf "$MELTANO_PROJECT/output"
  (cd "$MELTANO_PROJECT" && "$MELTANO_BIN" run tap-csv target-jsonl)
}

bench_tool "meltano" "$OUT_MELTANO" "$MELTANO_TIMEOUT" run_meltano_once
