#!/usr/bin/env bash
# Local conformance check for the TechEmpower entries.
#
# Starts a server, then asserts that GET /json and GET /plaintext return the
# exact bodies and headers TechEmpower validates. Optionally runs `wrk` for a
# quick throughput read if it is on PATH.
#
# Usage:
#   scripts/techempower/verify.sh rust      # build + run the Rust entry
#   scripts/techempower/verify.sh python    # run the Python entry (needs the wheel installed)
set -euo pipefail

TARGET="${1:-rust}"
HOST="127.0.0.1"
PORT="${PORT:-8080}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export MQB_LISTEN="${HOST}:${PORT}"

start_server() {
  case "$TARGET" in
    rust)
      ( cd "$HERE/Rust/mq-bridge" && cargo build --release )
      "$HERE/Rust/mq-bridge/target/release/mq-bridge-techempower" &
      ;;
    python)
      python "$HERE/Python/mq-bridge-py/server.py" &
      ;;
    *)
      echo "unknown target '$TARGET' (use: rust | python)" >&2
      exit 2
      ;;
  esac
  SERVER_PID=$!
}

cleanup() { kill "${SERVER_PID:-}" 2>/dev/null || true; }
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 100); do
    if curl -fsS "http://${HOST}:${PORT}/plaintext" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "server did not become ready" >&2
  exit 1
}

assert_contains() { # haystack needle label
  if ! grep -qi "$2" <<<"$1"; then
    echo "FAIL: $3 (missing: $2)" >&2
    echo "----- response -----" >&2; echo "$1" >&2
    exit 1
  fi
}

start_server
wait_ready

echo "== GET /json =="
JSON="$(curl -fsS -i "http://${HOST}:${PORT}/json")"
assert_contains "$JSON" '200 OK'                     "/json status"
assert_contains "$JSON" 'content-type: application/json' "/json content-type"
assert_contains "$JSON" 'server: mq-bridge'          "/json Server header"
assert_contains "$JSON" 'date:'                      "/json Date header"
assert_contains "$JSON" '{"message":"Hello, World!"}' "/json body"

echo "== GET /plaintext =="
TEXT="$(curl -fsS -i "http://${HOST}:${PORT}/plaintext")"
assert_contains "$TEXT" '200 OK'              "/plaintext status"
assert_contains "$TEXT" 'content-type: text/plain' "/plaintext content-type"
assert_contains "$TEXT" 'server: mq-bridge'   "/plaintext Server header"
assert_contains "$TEXT" 'Hello, World!'       "/plaintext body"

if [ -n "${DATABASE_URL:-}" ]; then
  echo "== GET /db =="
  DB="$(curl -fsS -i "http://${HOST}:${PORT}/db")"
  assert_contains "$DB" '200 OK'                          "/db status"
  assert_contains "$DB" 'content-type: application/json'  "/db content-type"
  assert_contains "$DB" '"id":'                           "/db id field"
  assert_contains "$DB" '"randomNumber":'                 "/db randomNumber field"

  echo "== GET /queries?queries=3 =="
  Q="$(curl -fsS "http://${HOST}:${PORT}/queries?queries=3")"
  ROWS="$(printf '%s' "$Q" | tr ',' '\n' | grep -c '"id"' || true)"
  [ "$ROWS" = "3" ] || { echo "FAIL: /queries?queries=3 returned $ROWS rows" >&2; echo "$Q" >&2; exit 1; }

  echo "PASS: DB endpoints conform"
else
  echo "(set DATABASE_URL to also check /db and /queries — see scripts/techempower/postgres.yml)"
fi

echo "PASS: both endpoints conform"

if command -v wrk >/dev/null 2>&1; then
  echo "== wrk /plaintext (pipelined) =="
  LUA="$(mktemp)"; printf 'wrk.method="GET"\n' >"$LUA"
  wrk -t"$(nproc 2>/dev/null || sysctl -n hw.ncpu)" -c256 -d5s -s "$LUA" "http://${HOST}:${PORT}/plaintext" || true
  echo "== wrk /json =="
  wrk -t"$(nproc 2>/dev/null || sysctl -n hw.ncpu)" -c256 -d5s "http://${HOST}:${PORT}/json" || true
else
  echo "(install wrk for a throughput read: brew install wrk)"
fi
