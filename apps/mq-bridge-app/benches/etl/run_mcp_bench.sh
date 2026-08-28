#!/usr/bin/env bash
# Benchmark the MCP server itself — tool-call latency, route throughput driven
# through a tool call, and the agent-side token cost of moving the dataset.
#
#   ./run_mcp_bench.sh                      # 1M rows, 2 timed runs
#   ROWS=50000 ./run_mcp_bench.sh           # quick smoke run
#   LATENCY_CALLS=1000 ./run_mcp_bench.sh   # tighter latency tail
#
# The measuring is all in mcp_bench.py, which is a real MCP client over stdio: it
# spawns `mqb mcp` and speaks JSON-RPC to it the way Claude Code does.
# Nothing here reaches into the process, so the numbers include the full
# interface cost an agent actually pays.
#
# This wrapper exists only so the scenario is entered the same way as the other
# eight (`benches/etl/run_*.sh`) and inherits the same dataset, defaults, watchdog
# budget and environment header from csv_common.sh/lib.sh. Re-implementing those
# in Python would fork the harness's conventions for one scenario.
#
# The same CSV -> JSONL job as scenario 6 is used on purpose: the point of the
# throughput number is that it matches the `copy` CLI figure, i.e. that the MCP
# interface costs one round-trip and nothing per row. Comparing it against a
# different workload would prove nothing.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/csv_common.sh"

LATENCY_CALLS="${LATENCY_CALLS:-200}"
OUT_MCP="${OUT_MCP:-/tmp/mqb_mcp_out.jsonl}"
# The throughput row is appended to scenario 6's shared results CSV (inherited
# from csv_common.sh), because the whole point is to read it against the `copy`
# CLI rows measured on the same dataset. Latency and token cost, which have no
# `copy` counterpart, go to the JSON file.
RESULTS_JSON="${RESULTS_JSON:-$RESULTS_DIR/mcp_bench.json}"

require_bin
ensure_csv

print_env_header
echo

# The route is driven by the client, so the watchdog lives there (--timeout);
# COPY_TIMEOUT is the same row-scaled budget the other scenarios use.
python3 "$HERE/mcp_bench.py" \
  --bin "$BIN" \
  --csv "$CSV" \
  --out "$OUT_MCP" \
  --rows "$ROWS" \
  --repeats "$REPEATS" \
  --latency-calls "$LATENCY_CALLS" \
  --timeout "$COPY_TIMEOUT" \
  --json-out "$RESULTS_JSON" \
  --results-csv "$RESULTS_CSV"

rm -f "$OUT_MCP"
