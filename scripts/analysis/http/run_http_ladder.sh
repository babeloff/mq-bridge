#!/usr/bin/env bash
#
# HTTP throughput ladder — runs the four benchmark targets from the throughput
# analysis back-to-back with the same in-tree Rust load client, so each rung adds
# exactly one layer and the RPS deltas localize the bottleneck.
#
#   A  raw-hyper          (immediate)          : Hyper + body read + fixed "ok". Ceiling.
#   B  worker-local-chan  (worker-local-ack)   : + per-connection mpsc + oneshot ack (no shared receiver)
#   C  global-channel     (channel-ack)        : + ONE process-wide mpsc + single drain task
#   Dr router+consumer    (direct-consumer-ack): real HttpConsumer — router Mutex + per-route mpsc
#   D  full-route         (route-handler)      : full mq-bridge route executor
#
# Read the deltas:
#   A -> B : cost of the per-request channel+ack machinery itself (should be small)
#   B -> C : cost of funneling every connection through ONE shared receiver  (bottleneck #2)
#   C -> Dr: cost of the router Mutex + real consumer path                   (bottleneck #1)
#   Dr -> D: cost of the route executor / worker pool / commit sequencer
#
# Caveat: A/B/C use a single-acceptor standalone server; Dr/D use the production
# SO_REUSEPORT multi-worker accept. Compare within {A,B,C} and within {Dr,D}; the
# router lock itself is isolated cleanly (no accept-model confound) by
# `cargo bench --bench router_bench`.
#
# Usage:  scripts/analysis/http/run_http_ladder.sh
# Env:    WORKERS, CLIENTS, DURATION (s), PORT, FEATURES
set -euo pipefail

cd "$(dirname "$0")/../../.."

FEATURES="${FEATURES:-http,rustls-ring}"
if command -v nproc >/dev/null 2>&1; then
  CORES="$(nproc)"
else
  CORES="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
fi
WORKERS="${WORKERS:-$CORES}"
CLIENTS="${CLIENTS:-$CORES}"
DURATION="${DURATION:-15}"
PORT="${PORT:-18080}"
REQ_PATH="/bench"
BIN=target/release/mq_bridge_http_profile

echo ">> building release binary (features: $FEATURES)"
cargo build --release --bin mq_bridge_http_profile --features "$FEATURES" >/dev/null

run_rung() {
  local label="$1" mode="$2" expect="$3"
  local log; log="$(mktemp)"

  "$BIN" --mode "$mode" --port "$PORT" --path "$REQ_PATH" \
    --workers "$WORKERS" --duration-s "$((DURATION + 8))" >"$log" 2>&1 &
  local server_pid=$!

  # Wait for the server to announce READY (up to ~10s).
  local ready=0
  for _ in $(seq 1 100); do
    if grep -q READY "$log"; then ready=1; break; fi
    if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
    sleep 0.1
  done
  if [ "$ready" -ne 1 ]; then
    echo "  [$label] server failed to start:"; sed 's/^/    /' "$log"
    kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true
    rm -f "$log"; return 1
  fi

  local out
  out="$("$BIN" --client-url "http://127.0.0.1:${PORT}${REQ_PATH}" \
    --clients "$CLIENTS" --duration-s "$DURATION" --expected-body "$expect" 2>&1 || true)"
  local rps; rps="$(printf '%s' "$out" | sed -n 's/.*(\([0-9]*\) req\/s).*/\1/p')"
  printf '  %-22s %12s req/s\n' "$label" "${rps:-FAILED}"
  if [ -z "$rps" ]; then printf '%s\n' "$out" | sed 's/^/      /'; fi

  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  rm -f "$log"

  if [ -z "$rps" ]; then return 1; fi
}

echo ">> HTTP throughput ladder (workers=$WORKERS clients=$CLIENTS duration=${DURATION}s)"
run_rung "A raw-hyper"          immediate           ok
run_rung "B worker-local-chan"  worker-local-ack    ok
run_rung "C global-channel"     channel-ack         ok
run_rung "Dr router+consumer"   direct-consumer-ack message-processed
run_rung "D full-route"         route-handler       payload
echo ">> done"
