#!/usr/bin/env bash
# Postgres -> JSONL throughput. Uses the app's zero-code `copy` command with a
# `file://…?format=raw` sink (raw payload per line, newline-delimited — no
# CanonicalMessage envelope), so the output is plain JSONL: one JSON row per
# line, directly comparable with any other EL tool's JSONL output.
#
# Prereqs:  ./seed.sh up   and a lean build:
#           cargo build -p mq-bridge-app --no-default-features --features bench --release
#
# Matrix (override via env): PAYLOADS, BATCHES, CONCURRENCIES, REPEATS, MSG_COUNT.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/seed.sh"   # also sources lib.sh

PAYLOADS="${PAYLOADS:-256 4096}"
BATCHES="${BATCHES:-1 128}"
CONCURRENCIES="${CONCURRENCIES:-1 4}"
REPEATS="${REPEATS:-2}"          # timed runs per cell (1 warmup + REPEATS, median/stddev reported)

RESULTS_DIR="${RESULTS_DIR:-$HERE/results}"
mkdir -p "$RESULTS_DIR"
CSV="$RESULTS_DIR/pg_to_jsonl.csv"
echo "payload_bytes,batch,concurrency,rows,repeats,median_elapsed_s,stddev_elapsed_s,median_rows_per_s" > "$CSV"

now() { python3 -c 'import time; print(time.time())'; }
COPY_TIMEOUT="${COPY_TIMEOUT:-900}"
OUT_FILE="${OUT_FILE:-/tmp/mqb_bench_out.jsonl}"

# Delegates to lib.sh's run_guarded, which escalates SIGTERM -> SIGKILL and
# signals the whole process group. The TERM-only version this replaced could not
# kill a wedged route — mq-bridge-app traps SIGTERM for graceful shutdown — so the
# watchdog fired, the process survived, and the script waited forever.
copy_guarded() {
  run_guarded "$COPY_TIMEOUT" "$BIN" copy "$@"
}

[[ -x "$BIN" ]] || { echo "binary not found at $BIN — build with --features bench --release" >&2; exit 1; }
wait_for_pg

run_one() {
  local bytes="$1" batch="$2" conc="$3"
  local src="src_${bytes}"
  local from="${PG_URL}?table=${src}&cursor_column=id&sslmode=disable"
  local to="file://${OUT_FILE}?format=raw"

  # Warmup pre-roll (discarded, not timed).
  seed_source "$src" "$bytes" "$WARMUP_COUNT" >/dev/null
  rm -f "$OUT_FILE"
  copy_guarded --from "$from" --to "$to" --drain --batch-size "$batch" --concurrency "$conc" || true

  local -a elapsed_samples=()
  local i t0 t1 elapsed landed
  for ((i = 1; i <= REPEATS; i++)); do
    seed_source "$src" "$bytes" "$MSG_COUNT" >/dev/null
    rm -f "$OUT_FILE"
    t0="$(now)"
    copy_guarded --from "$from" --to "$to" --drain --batch-size "$batch" --concurrency "$conc" || true
    t1="$(now)"
    landed="$( [[ -f "$OUT_FILE" ]] && wc -l < "$OUT_FILE" | tr -d ' ' || echo 0 )"
    if [[ "$landed" != "$MSG_COUNT" ]]; then
      echo "  WARNING: run $i landed ${landed} != expected ${MSG_COUNT}" >&2
    fi
    elapsed="$(python3 -c "print(f'{$t1-$t0:.6f}')")"
    elapsed_samples+=("$elapsed")
  done

  local median stddev rate
  read -r median stddev <<<"$(python3 -c "
import statistics as s
xs=[${elapsed_samples[*]/%/,}]
print(f'{s.median(xs):.3f} {s.pstdev(xs):.3f}' if len(xs) > 1 else f'{xs[0]:.3f} 0.000')
")"
  rate="$(python3 -c "print(int($MSG_COUNT/$median))")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$bytes" "$batch" "$conc" "$MSG_COUNT" "$REPEATS" "$median" "$stddev" "$rate" | tee -a "$CSV"
}

echo "# pg->jsonl matrix: payloads=[$PAYLOADS] batches=[$BATCHES] concurrency=[$CONCURRENCIES] rows=$MSG_COUNT"
for bytes in $PAYLOADS; do
  for batch in $BATCHES; do
    for conc in $CONCURRENCIES; do
      echo "-- ${bytes}B batch=${batch} conc=${conc}"
      run_one "$bytes" "$batch" "$conc"
    done
  done
done
rm -f "$OUT_FILE"
echo "done -> $CSV"
