#!/bin/bash
# nginx vs mq-bridge-app as a mirroring reverse proxy.
#
#   ./run.sh                 # full A/B, results into results/<date>-*.txt
#   MQB=/path/to/binary ./run.sh
#
# See README.md for setup, the macOS sysctl requirement and how to read the output.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
WORK="$HERE/work"
RESULTS_DIR="$HERE/results"
STAMP="${STAMP:-$(date +%Y-%m-%d)}"
RESULTS="$RESULTS_DIR/${STAMP}-wrk.txt"
SUMMARY="$RESULTS_DIR/${STAMP}-summary.log"

DUR=${DUR:-15}
WARM=${WARM:-5}
THREADS=${THREADS:-2}
CONNS=${CONNS:-50}

mkdir -p "$WORK/tmp" "$WORK/logs" "$RESULTS_DIR"
: > "$RESULTS"
STAGING_LOG="$WORK/logs/staging-access.log"
: > "$STAGING_LOG"

# ---- preflight -------------------------------------------------------------
for tool in wrk nginx curl; do
  command -v $tool > /dev/null || { echo "missing '$tool' (brew install $tool)"; exit 1; }
done

MQB="${MQB:-$ROOT/target/release/mq-bridge-app}"
if [ ! -x "$MQB" ]; then
  echo "mq-bridge-app not found at $MQB"
  echo "build it with:"
  echo "  cargo build --release -p mq-bridge-app --no-default-features --features 'http,mimalloc,rustls-aws-lc'"
  echo "or point MQB=... at an existing binary."
  exit 1
fi

# macOS: a non-keepalive loopback leg (nginx's mirror) exhausts the default
# 16k ephemeral ports in ~15s and caps at ~1090 conn/s. Without this the nginx
# mirror rows measure the OS, not nginx. See README.md.
if [ "$(uname)" = "Darwin" ]; then
  first=$(sysctl -n net.inet.ip.portrange.first)
  msl=$(sysctl -n net.inet.tcp.msl)
  if [ "$first" -ge 49152 ] || [ "$msl" -ge 15000 ]; then
    echo "!! macOS ephemeral ports are at defaults (first=$first msl=$msl)."
    echo "!! nginx's mirror rows will be capped at ~1000 rps by port exhaustion."
    echo "!! Run this first (restore with 49152 / 15000, does not survive reboot):"
    echo "     sudo sysctl -w net.inet.ip.portrange.first=16384 net.inet.ip.portrange.hifirst=16384 net.inet.tcp.msl=1000"
    echo ""
    if [ -t 0 ]; then
      read -r -p "continue anyway? [y/N] " a; [ "$a" = "y" ] || exit 1
    else
      echo "!! non-interactive: continuing, nginx mirror rows will be OS-capped"
    fi
  fi
fi

# ---- materialise configs + certs into work/ --------------------------------
for f in upstream.conf upstream-verify.conf proxy.conf; do
  sed "s|__BENCH_DIR__|$WORK|g" "$HERE/$f" > "$WORK/$f"
done
if [ ! -f "$WORK/cert.pem" ]; then
  openssl req -x509 -newkey rsa:2048 -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
    -days 30 -nodes -subj "/CN=localhost" 2>/dev/null
fi
cp "$HERE/post.lua" "$WORK/post.lua"

MQB_PIDS=""
MQB_PID=""
cleanup() {
  pkill -f "nginx: master.*$WORK" 2>/dev/null || true
  for pid in $MQB_PIDS; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

# ---- helpers ---------------------------------------------------------------
# Wait for TIME_WAIT to drain: one run's backlog otherwise starves the next
# process of ephemeral ports, which reports as a huge rps of pure 5xx.
cooldown() {
  local tw
  for _ in $(seq 1 120); do
    tw=$(netstat -an 2>/dev/null | grep -c TIME_WAIT)
    [ "$tw" -lt 3000 ] && { echo "  cooldown ok (TIME_WAIT=$tw)"; return 0; }
    sleep 1
  done
  echo "  WARNING: TIME_WAIT still $tw after 120s"
}

bench() { # name url
  local name="$1" url="$2"
  cooldown
  wrk -t$THREADS -c$CONNS -d${WARM}s -s "$WORK/post.lua" "$url" > /dev/null 2>&1
  local mbefore out mafter mirrored rps p50 p99 nonok total
  mbefore=$(wc -l < "$STAGING_LOG")
  out=$(wrk -t$THREADS -c$CONNS -d${DUR}s --latency -s "$WORK/post.lua" "$url" 2>&1)
  sleep 1
  mafter=$(wc -l < "$STAGING_LOG")
  mirrored=$(( mafter - mbefore ))
  { echo "===== $name ($url)"; echo "$out"; echo ""; } >> "$RESULTS"
  rps=$(echo "$out"   | awk '/Requests\/sec/{print $2}')
  p50=$(echo "$out"   | awk '/^ +50%/{print $2}')
  p99=$(echo "$out"   | awk '/^ +99%/{print $2}')
  nonok=$(echo "$out" | awk '/Non-2xx or 3xx/{print $NF}')
  total=$(echo "$out" | awk '/requests in/{print $1}')
  echo "$out" | grep -q "unable to connect" && echo "  !! $name: proxy unreachable - run INVALID"
  printf '%-34s rps=%-12s p50=%-9s p99=%-9s non2xx=%-8s reqs=%-9s mirrored=%s\n' \
    "$name" "$rps" "$p50" "$p99" "${nonok:-0}" "${total:-?}" "$mirrored"
}

start_mqb() { # port to_uri concurrency logname
  "$MQB" copy \
    --from "http://0.0.0.0:$1?method=POST&workers=2&concurrency_limit=256" \
    --to "$2" --concurrency "$3" > "$WORK/logs/$4" 2>&1 &
  MQB_PID=$!
  MQB_PIDS="$MQB_PIDS $MQB_PID"
}
start_mqb_tls() { # port to_uri concurrency logname
  "$MQB" copy \
    --from "https://0.0.0.0:$1?method=POST&workers=2&concurrency_limit=256&tls={\"required\":true,\"cert_file\":\"$WORK/cert.pem\",\"key_file\":\"$WORK/key.pem\"}" \
    --to "$2" --concurrency "$3" > "$WORK/logs/$4" 2>&1 &
  MQB_PID=$!
  MQB_PIDS="$MQB_PIDS $MQB_PID"
}
stop_mqb() {
  kill "$MQB_PID" 2>/dev/null || true
  wait "$MQB_PID" 2>/dev/null || true
  active_pids=""
  for pid in $MQB_PIDS; do
    [ "$pid" = "$MQB_PID" ] || active_pids="$active_pids $pid"
  done
  MQB_PIDS=$active_pids
}
wait_up() { for _ in $(seq 1 60); do curl -sk -m1 -X POST --data x "$1" >/dev/null 2>&1 && return 0; sleep 0.5; done; return 1; }

PROD='http://127.0.0.1:9001/'
STAGE='http://127.0.0.1:9002/'
FANOUT="fanout:?mirror=${STAGE}&to=${PROD}"

# ---- upstream stubs (shared by every row) ----------------------------------
nginx -c "$WORK/upstream-verify.conf" -p "$WORK" > "$WORK/logs/upstream.log" 2>&1 &
wait_up "http://127.0.0.1:9001/t" || { echo "upstream stubs failed"; cat "$WORK/logs/upstream.log"; exit 1; }

echo "--- 0. upstream ceiling (no proxy) ---"
bench "upstream-direct" "http://127.0.0.1:9001/test"

echo "--- 1. nginx ---"
nginx -c "$WORK/proxy.conf" -p "$WORK" > "$WORK/logs/proxy-nginx.log" 2>&1 &
wait_up "http://127.0.0.1:8080/plain" || { echo "nginx proxy failed"; cat "$WORK/logs/proxy-nginx.log"; exit 1; }
echo "  sanity: $(curl -sk -m2 -X POST --data x http://127.0.0.1:8080/plain) / $(curl -sk -m2 -X POST --data x http://127.0.0.1:8080/test) / $(curl -sk -m2 -X POST --data x https://127.0.0.1:8443/test)"
bench "nginx-proxy"            "http://127.0.0.1:8080/plain"
bench "nginx-proxy+mirror"     "http://127.0.0.1:8080/test"
bench "nginx-proxy+mirror-tls" "https://127.0.0.1:8443/test"
pkill -f "nginx: master.*$WORK/proxy.conf"; sleep 1

echo "--- 2. mq-bridge-app ---"
start_mqb 8081 "$PROD" 64 mqb-plain.log
wait_up "http://127.0.0.1:8081/plain" || { echo "mqb plain failed"; tail -20 "$WORK/logs/mqb-plain.log"; exit 1; }
echo "  sanity: $(curl -s -m2 -X POST --data x http://127.0.0.1:8081/plain)"
bench "mqb-proxy (conc=64)" "http://127.0.0.1:8081/plain"
stop_mqb; sleep 1

start_mqb 8081 "$FANOUT" 64 mqb-mirror.log
wait_up "http://127.0.0.1:8081/test" || { echo "mqb mirror failed"; tail -20 "$WORK/logs/mqb-mirror.log"; exit 1; }
echo "  sanity: $(curl -s -m2 -X POST --data x http://127.0.0.1:8081/test)"
bench "mqb-proxy+mirror (conc=64)" "http://127.0.0.1:8081/test"
stop_mqb; sleep 1

start_mqb 8081 "$FANOUT" 4 mqb-mirror-c4.log
wait_up "http://127.0.0.1:8081/test" || { echo "mqb mirror c4 failed"; tail -20 "$WORK/logs/mqb-mirror-c4.log"; exit 1; }
bench "mqb-proxy+mirror (conc=4 default)" "http://127.0.0.1:8081/test"
stop_mqb; sleep 1

start_mqb_tls 8444 "$FANOUT" 64 mqb-mirror-tls.log
wait_up "https://127.0.0.1:8444/test" || { echo "mqb tls failed"; tail -20 "$WORK/logs/mqb-mirror-tls.log"; exit 1; }
echo "  sanity: $(curl -sk -m2 -X POST --data x https://127.0.0.1:8444/test)"
bench "mqb-proxy+mirror-tls (conc=64)" "https://127.0.0.1:8444/test"

echo "--- done: full wrk output in $RESULTS ---"
