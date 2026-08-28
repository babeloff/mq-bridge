#!/usr/bin/env bash
# Postgres -> JSONL comparison against Meltano (tap-postgres -> target-jsonl):
# `bench` table/schema/dataset (id, first_name, country, amount, created_at,
# active, attributes; 1,000,000 rows, seed 42, see gen_bench_data.py), 1 warmup
# run discarded + 5 timed runs, median/stddev, row-count parity check.
#
# Prereqs:  ./seed.sh up && ./seed.sh bench   and a lean build:
#           cargo build -p mq-bridge-app --no-default-features --features bench --release
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/seed.sh"   # also sources lib.sh

ROWS="${ROWS:-1000000}"
REPEATS="${REPEATS:-2}"
MELTANO_PROJECT="$HERE/meltano_project/bench"
MELTANO_BIN="$HERE/meltano_project/.venv/bin/meltano"

RESULTS_DIR="${RESULTS_DIR:-$HERE/results}"
mkdir -p "$RESULTS_DIR"
CSV="$RESULTS_DIR/meltano_pg_to_jsonl.csv"
echo "tool,rows,repeats,median_elapsed_s,stddev_elapsed_s,median_rows_per_s" > "$CSV"

now() { python3 -c 'import time; print(time.time())'; }
COPY_TIMEOUT="${COPY_TIMEOUT:-900}"
OUT_FILE="${OUT_FILE:-/tmp/mqb_bench_out.jsonl}"

median_stddev() {
  python3 -c "
import statistics as s
xs=[${1}]
print(f'{s.median(xs):.3f} {s.pstdev(xs):.3f}' if len(xs) > 1 else f'{xs[0]:.3f} 0.000')
"
}

[[ -x "$BIN" ]] || { echo "binary not found at $BIN — build with --features bench --release" >&2; exit 1; }
wait_for_pg

n_bench="$(psql_q -c "SELECT count(*) FROM bench;" 2>/dev/null || echo 0)"
if [[ "$n_bench" != "$ROWS" ]]; then
  echo "bench table has ${n_bench} rows, expected ${ROWS} — reseeding"
  seed_bench "$ROWS" 42
fi

# --- mqb copy ---
# Delegates to lib.sh's run_guarded, which escalates SIGTERM -> SIGKILL and
# signals the whole process group. The TERM-only version this replaced could not
# kill a wedged route — mq-bridge-app traps SIGTERM for graceful shutdown — so the
# watchdog fired, the process survived, and the script waited forever.
copy_guarded() {
  run_guarded "$COPY_TIMEOUT" "$BIN" copy "$@"
}

from="${PG_URL}?table=bench&cursor_column=id&sslmode=disable"
to="file://${OUT_FILE}?format=raw"

echo "-- mqb copy: warmup"
rm -f "$OUT_FILE"
copy_guarded --from "$from" --to "$to" --drain --batch-size 1024 --concurrency 1 || true

mqb_samples=()
for ((i = 1; i <= REPEATS; i++)); do
  rm -f "$OUT_FILE"
  t0="$(now)"
  copy_guarded --from "$from" --to "$to" --drain --batch-size 1024 --concurrency 1 || true
  t1="$(now)"
  landed="$( [[ -f "$OUT_FILE" ]] && wc -l < "$OUT_FILE" | tr -d ' ' || echo 0 )"
  [[ "$landed" == "$ROWS" ]] || echo "  WARNING: mq-bridge-app run $i landed ${landed} != expected ${ROWS}" >&2
  elapsed="$(python3 -c "print(f'{$t1-$t0:.6f}')")"
  echo "  mq-bridge-app run $i: ${elapsed}s"
  mqb_samples+=("$elapsed")
done
read -r mqb_median mqb_stddev <<<"$(median_stddev "${mqb_samples[*]/%/,}")"
mqb_rate="$(python3 -c "print(int($ROWS/$mqb_median))")"
printf 'mq-bridge-app,%s,%s,%s,%s,%s\n' "$ROWS" "$REPEATS" "$mqb_median" "$mqb_stddev" "$mqb_rate" | tee -a "$CSV"
rm -f "$OUT_FILE"

# --- Meltano (tap-postgres -> target-jsonl) ---
# Skipped rather than fatal (same as the Sling block below): the venv is
# gitignored, so a fresh checkout can still run the other tools.
if [[ ! -x "$MELTANO_BIN" ]]; then
  echo "-- meltano: venv not found at $MELTANO_BIN, skipping" >&2
else
  run_meltano_once() {
    rm -rf "$MELTANO_PROJECT/output"
    # Under run_guarded, same as the Sling block: meltano's tap|target pipeline
    # gets the whole-process-group SIGTERM->SIGKILL escalation, so a stalled run
    # is killed instead of hanging the matrix. The subshell keeps its cwd.
    ( cd "$MELTANO_PROJECT" && run_guarded "${MELTANO_TIMEOUT:-$COPY_TIMEOUT}" \
      "$MELTANO_BIN" run tap-postgres target-jsonl )
  }

  echo "-- meltano: warmup"
  run_meltano_once || true

  meltano_samples=()
  for ((i = 1; i <= REPEATS; i++)); do
    t0="$(now)"
    run_meltano_once
    t1="$(now)"
    landed="$(landed_rows "$MELTANO_PROJECT/output/public-bench.jsonl")"
    [[ "$landed" == "$ROWS" ]] || { echo "  FAILED: meltano run $i landed ${landed} != expected ${ROWS} — not a publishable measurement" >&2; exit 1; }
    elapsed="$(python3 -c "print(f'{$t1-$t0:.6f}')")"
    echo "  meltano run $i: ${elapsed}s"
    meltano_samples+=("$elapsed")
  done
  read -r meltano_median meltano_stddev <<<"$(median_stddev "${meltano_samples[*]/%/,}")"
  meltano_rate="$(python3 -c "print(int($ROWS/$meltano_median))")"
  printf 'meltano,%s,%s,%s,%s,%s\n' "$ROWS" "$REPEATS" "$meltano_median" "$meltano_stddev" "$meltano_rate" | tee -a "$CSV"
fi

# --- Sling (postgres -> file JSONL) ---
# Sling auto-detects env vars whose value is a connection URL, so BENCH_PG below
# becomes the named connection --src-conn refers to. That keeps the run
# self-contained — no `sling conns set`, which would write to ~/.sling/env.yaml.
SLING_OUT="${SLING_OUT:-/tmp/sling_pg_out.jsonl}"
if [[ ! -x "$SLING_BIN" ]]; then
  echo "-- sling: not found at $SLING_BIN, skipping (see lib.sh for the install command)" >&2
else
  export BENCH_PG="${PG_URL}?sslmode=disable"
  run_sling_once() {
    rm -rf "$SLING_OUT"
    # Watchdog, same as the mq-bridge block: output is discarded, so an
    # unguarded stall would hang the whole matrix with nothing on screen.
    run_guarded "${SLING_TIMEOUT:-$COPY_TIMEOUT}" "$SLING_BIN" run \
      --src-conn BENCH_PG \
      --src-stream public.bench \
      --tgt-object "file://${SLING_OUT}" \
      --tgt-options '{"format":"jsonlines","file_max_rows":0}'
  }

  echo "-- sling: warmup"
  run_sling_once || true

  sling_samples=()
  for ((i = 1; i <= REPEATS; i++)); do
    t0="$(now)"
    run_sling_once
    t1="$(now)"
    landed="$(landed_rows "$SLING_OUT")"
    [[ "$landed" == "$ROWS" ]] || { echo "  FAILED: sling run $i landed ${landed} != expected ${ROWS} — not a publishable measurement" >&2; exit 1; }
    elapsed="$(python3 -c "print(f'{$t1-$t0:.6f}')")"
    echo "  sling run $i: ${elapsed}s"
    sling_samples+=("$elapsed")
  done
  read -r sling_median sling_stddev <<<"$(median_stddev "${sling_samples[*]/%/,}")"
  sling_rate="$(python3 -c "print(int($ROWS/$sling_median))")"
  printf 'sling,%s,%s,%s,%s,%s\n' "$ROWS" "$REPEATS" "$sling_median" "$sling_stddev" "$sling_rate" | tee -a "$CSV"
  rm -rf "$SLING_OUT"
fi

echo "done -> $CSV"
