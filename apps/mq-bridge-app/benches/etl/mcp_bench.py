#!/usr/bin/env python3
"""Benchmark the mq-bridge-app MCP server over its real stdio transport.

This is deliberately a *client*, not a harness that pokes at internals: it spawns
`mqb mcp --transport stdio` and speaks JSON-RPC to it exactly the way
Claude Code or any other MCP client does, so every number below includes the
framing, serialization and process-boundary cost a real agent pays.

Three things are measured, because "how fast is the MCP server" is really three
different questions:

  1. Tool-call round-trip latency  — the cost of the MCP interface itself
     (`route_status` with nothing running: one request, one response, no work).
  2. Route throughput             — rows/s of a 1M-row job *started through a
     tool call*, to show the interface adds no per-row cost versus the `copy`
     CLI measured in the same harness.
  3. Agent token cost             — the bytes of JSON-RPC an agent exchanges to
     move the whole dataset. This is the point of the MCP server: the data never
     passes through the model's context, so the token cost is flat in the number
     of rows moved.

The rate is read from the server's own `average_messages_per_second` (total
messages over the span in which they moved), not from the instantaneous
`messages_per_second`, which decays to ~0 within a second of a drain finishing
and cannot describe a job that has already completed.

Usage:
  mcp_bench.py --bin BIN --csv IN.csv --out OUT.jsonl --rows N [--repeats 2]
               [--latency-calls 200] [--json-out results.json] [--allow-debug]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

PROTOCOL_VERSION = "2024-11-05"
# A drained route is observed by polling, so this interval is the upper bound on
# how much completion lag lands in the client-side wall-clock. It is well under
# the server's 200ms metric sampler on purpose: `finished` comes straight off the
# route handle's outcome, not off the sampler, so it is fresh on every call —
# only the rate fields are sampler-gated. At 200ms the poll lag was most of the
# gap between this number and the `copy` CLI's; below ~50ms the polls start
# perturbing the run they are measuring instead, so this is the middle ground
# (≤50ms of lag on a ~1.3s job, ~26 calls of ~0.06ms each).
POLL_INTERVAL_S = 0.05


class McpStdioClient:
    """A minimal MCP client over stdio, with byte accounting.

    Every frame is counted in both directions so the token cost of driving a job
    through the tools can be reported. `sent_bytes`/`recv_bytes` are cumulative;
    `reset_accounting()` starts a fresh window.
    """

    def __init__(self, argv: list[str]) -> None:
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # The server logs to stderr; keep it off our stdout parsing path but
            # let it through to the terminal so a startup failure is visible.
            stderr=None,
            text=True,
            bufsize=1,
        )
        self._next_id = 0
        self.sent_bytes = 0
        self.recv_bytes = 0

    def reset_accounting(self) -> None:
        self.sent_bytes = 0
        self.recv_bytes = 0

    def _write(self, message: dict) -> None:
        line = json.dumps(message, separators=(",", ":"))
        self.sent_bytes += len(line) + 1
        assert self.proc.stdin is not None
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()

    def _read(self) -> dict:
        assert self.proc.stdout is not None
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(
                    f"MCP server closed stdout (exit code {self.proc.poll()})"
                )
            line = line.strip()
            if not line:
                continue
            self.recv_bytes += len(line) + 1
            return json.loads(line)

    def request(self, method: str, params: dict | None = None) -> dict:
        self._next_id += 1
        request_id = self._next_id
        self._write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params or {},
            }
        )
        while True:
            message = self._read()
            # Skip anything that is not the response we are waiting for
            # (notifications, or a response that arrives out of order).
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"{method} failed: {message['error']}")
                return message.get("result", {})

    def notify(self, method: str, params: dict | None = None) -> None:
        self._write({"jsonrpc": "2.0", "method": method, "params": params or {}})

    def initialize(self) -> dict:
        result = self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "mqb-bench", "version": "1"},
            },
        )
        self.notify("notifications/initialized")
        return result

    def call_tool(self, name: str, arguments: dict | None = None) -> dict:
        """Calls a tool and returns its parsed JSON payload.

        Every tool in this server answers with a single JSON text block, so the
        content is unwrapped here rather than at each call site. A tool result
        flagged `isError` is raised: a benchmark must never average in a failed
        call as if it had succeeded.
        """
        result = self.request("tools/call", {"name": name, "arguments": arguments or {}})
        text = "".join(
            block.get("text", "")
            for block in result.get("content", [])
            if block.get("type") == "text"
        )
        if result.get("isError"):
            raise RuntimeError(f"tool {name} reported an error: {text}")
        return json.loads(text) if text else {}

    def close(self) -> None:
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile. `statistics.quantiles` interpolates, which is
    misleading for a latency tail read off a few hundred samples."""
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def measure_latency(client: McpStdioClient, calls: int) -> dict:
    """Round-trip latency of a no-op tool call, in milliseconds.

    `route_status` with no routes running is the cheapest real tool there is: the
    server does a map lookup over an empty map, so what is left is the MCP
    interface — JSON-RPC encode, pipe write, dispatch, encode, pipe read.
    """
    samples = []
    for _ in range(calls):
        started = time.perf_counter()
        client.call_tool("route_status")
        samples.append((time.perf_counter() - started) * 1000.0)
    return {
        "calls": calls,
        "mean_ms": statistics.fmean(samples),
        "p50_ms": percentile(samples, 0.50),
        "p95_ms": percentile(samples, 0.95),
        "p99_ms": percentile(samples, 0.99),
        "max_ms": max(samples),
    }


def run_route(
    client: McpStdioClient,
    name: str,
    csv_path: str,
    out_path: str,
    timeout_s: float,
) -> dict:
    """Drives one full CSV -> JSONL job through the tools and returns its result.

    Mirrors what an agent does: one `start_route`, poll `route_status` until it
    reports finished, one `stop_route` to clear the entry (finished routes are not
    reaped upstream). Wall-clock is measured client-side around the whole
    sequence; the server's own `elapsed_s` / `average_messages_per_second` are
    reported alongside it so the two can be sanity-checked against each other.
    """
    if os.path.exists(out_path):
        os.remove(out_path)

    def traffic() -> int:
        return client.sent_bytes + client.recv_bytes

    client.reset_accounting()
    wall_start = time.perf_counter()
    client.call_tool(
        "start_route",
        {
            "name": name,
            "route": {
                "input": {"file": {"path": csv_path, "format": "csv"}},
                "output": {"file": {"path": out_path, "format": "raw"}},
                "exit_on_empty": True,
            },
            "batch_size": 1024,
            "concurrency": 1,
        },
    )

    start_bytes = traffic()

    polls = 0
    poll_bytes = 0
    status: dict = {}
    while True:
        before_poll = traffic()
        status = client.call_tool("route_status", {"name": name})
        poll_bytes = traffic() - before_poll
        polls += 1
        if status.get("finished"):
            break
        if time.perf_counter() - wall_start > timeout_s:
            client.call_tool("stop_route", {"name": name})
            raise RuntimeError(
                f"route {name} did not finish within {timeout_s}s "
                f"(messages={status.get('messages')})"
            )
        time.sleep(POLL_INTERVAL_S)
    wall_s = time.perf_counter() - wall_start

    outcome = status.get("outcome")
    if outcome != "completed":
        raise RuntimeError(f"route {name} ended as {outcome!r}, not 'completed'")

    before_stop = traffic()
    stopped = client.call_tool("stop_route", {"name": name})
    stop_bytes = traffic() - before_stop

    return {
        "wall_s": wall_s,
        "polls": polls,
        "messages": stopped.get("messages", status.get("messages")),
        "server_elapsed_s": status.get("elapsed_s"),
        "server_avg_rows_per_s": status.get("average_messages_per_second"),
        "sent_bytes": client.sent_bytes,
        "recv_bytes": client.recv_bytes,
        "total_bytes": traffic(),
        # What an agent actually spends: start the route, check it once, stop it.
        # The harness's poll loop exists to time completion precisely and polls
        # far harder than any agent would, so its traffic must not be published as
        # the agent's cost.
        "agent_bytes": start_bytes + poll_bytes + stop_bytes,
    }


RESULTS_TOOL = "mq-bridge-app-mcp"
RESULTS_HEADER = (
    "tool,rows,repeats,median_elapsed_s,stddev_elapsed_s,median_rows_per_s\n"
)


def write_results_row(path: str, summary: dict) -> None:
    """Appends this run's throughput row to the harness's shared results CSV.

    Same shape and same file as the `copy` CLI rows (lib.sh's `bench_tool` writes
    it), so the MCP row can be read directly against them. A re-run replaces its
    own row rather than duplicating it, matching `results_drop_tool`.
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    kept = [RESULTS_HEADER]
    if os.path.exists(path):
        with open(path) as handle:
            kept = [
                line
                for line in handle
                if line.split(",")[0] not in (RESULTS_TOOL,)
            ] or [RESULTS_HEADER]
    kept.append(
        "{tool},{rows},{repeats},{wall:.3f},{stdev:.3f},{rate}\n".format(
            tool=RESULTS_TOOL,
            rows=summary["rows"],
            repeats=summary["repeats"],
            wall=summary["median_wall_s"],
            stdev=summary["stdev_wall_s"],
            rate=int(summary["median_rows_per_s"]),
        )
    )
    with open(path, "w") as handle:
        handle.writelines(kept)
    print(f"wrote {path}")


def landed_rows(path: str) -> int:
    with open(path, "rb") as handle:
        return sum(1 for _ in handle)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, help="path to the mq-bridge-app binary")
    parser.add_argument("--csv", required=True, help="input CSV")
    parser.add_argument("--out", required=True, help="output JSONL path")
    parser.add_argument("--rows", type=int, required=True, help="expected row count")
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--latency-calls", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--json-out", help="write the full result set here as JSON")
    parser.add_argument(
        "--results-csv",
        help="append the throughput row to this shared results CSV, replacing any "
        "previous mq-bridge-app-mcp row",
    )
    parser.add_argument(
        "--allow-debug",
        action="store_true",
        help="do not abort when the server reports a debug build",
    )
    args = parser.parse_args()

    client = McpStdioClient([args.bin, "mcp", "--transport", "stdio"])
    try:
        server = client.initialize()
        info = client.call_tool("server_info")
        print(
            f"server: mq-bridge-app {info.get('version')} "
            f"{info.get('git_hash')} profile={info.get('profile')} "
            f"(protocol {server.get('protocolVersion')})"
        )
        # A debug binary silently produced meaningless numbers once already; the
        # server reports its own profile precisely so that cannot recur.
        if info.get("profile") != "release" and not args.allow_debug:
            print(
                f"ABORT: server is a {info.get('profile')!r} build — "
                "rebuild with --release (or pass --allow-debug)",
                file=sys.stderr,
            )
            return 2

        latency = measure_latency(client, args.latency_calls)
        print(
            f"tool-call latency ({latency['calls']} calls): "
            f"p50 {latency['p50_ms']:.3f} ms · p95 {latency['p95_ms']:.3f} ms · "
            f"p99 {latency['p99_ms']:.3f} ms · max {latency['max_ms']:.3f} ms"
        )

        runs = []
        for index in range(args.repeats):
            run = run_route(
                client,
                f"mcp_bench_{index}",
                args.csv,
                args.out,
                args.timeout,
            )
            landed = landed_rows(args.out)
            if landed != args.rows:
                print(
                    f"ABORT: run {index} landed {landed} rows, expected {args.rows}",
                    file=sys.stderr,
                )
                return 1
            if run["messages"] != args.rows:
                print(
                    f"ABORT: run {index} counted {run['messages']} messages, "
                    f"expected {args.rows}",
                    file=sys.stderr,
                )
                return 1
            run["rows_per_s"] = args.rows / run["wall_s"]
            runs.append(run)
            # A job that finishes inside one 200ms sampler tick has no server-side
            # average at all; say so rather than printing "None rows/s".
            if run["server_avg_rows_per_s"] is None:
                server_note = "server not sampled (finished within one 200ms tick)"
            else:
                server_note = (
                    f"server {run['server_elapsed_s']}s / "
                    f"{run['server_avg_rows_per_s']:.0f} rows/s"
                )
            print(
                f"run {index + 1}/{args.repeats}: {run['wall_s']:.3f}s wall "
                f"({run['rows_per_s']:.0f} rows/s) · {server_note} · "
                f"{run['polls']} status polls · "
                f"{run['total_bytes']} JSON-RPC bytes "
                f"({run['agent_bytes']} for the 3 calls an agent needs)"
            )
    finally:
        client.close()

    wall_samples = [run["wall_s"] for run in runs]
    median_wall = statistics.median(wall_samples)
    # The tool traffic is what an agent's context actually pays for. ~4 bytes per
    # token is the usual rough English/JSON ratio; it is an estimate and labelled
    # as one wherever it is published. The published figure is the agent-shaped
    # one (start / check / stop), not the harness's poll loop.
    median_bytes = statistics.median([run["agent_bytes"] for run in runs])
    median_total_bytes = statistics.median([run["total_bytes"] for run in runs])
    dataset_bytes = os.path.getsize(args.csv)
    summary = {
        "rows": args.rows,
        "repeats": args.repeats,
        "median_wall_s": median_wall,
        "stdev_wall_s": statistics.pstdev(wall_samples) if len(wall_samples) > 1 else 0.0,
        "median_rows_per_s": args.rows / median_wall,
        # None whenever no run was sampled, rather than a 0.0 that reads as a
        # measurement.
        "server_avg_rows_per_s": (
            statistics.median(sampled) if (sampled := [
                run["server_avg_rows_per_s"]
                for run in runs
                if run["server_avg_rows_per_s"] is not None
            ]) else None
        ),
        "median_agent_bytes": median_bytes,
        "est_agent_tokens": median_bytes / 4,
        "median_harness_bytes": median_total_bytes,
        "dataset_bytes": dataset_bytes,
        "est_dataset_tokens": dataset_bytes / 4,
        "latency": latency,
        "server": info,
        "runs": runs,
    }

    print(
        f"\nmedian: {median_wall:.3f}s → {summary['median_rows_per_s']:.0f} rows/s "
        f"for {args.rows} rows"
    )
    print(
        f"agent cost: {median_bytes:.0f} bytes of tool traffic "
        f"(~{summary['est_agent_tokens']:.0f} tokens, 3 calls) to move "
        f"{dataset_bytes / 1024 / 1024:.1f} MiB "
        f"(~{summary['est_dataset_tokens'] / 1_000_000:.1f}M tokens if it had gone "
        f"through the context)"
    )

    if args.results_csv:
        write_results_row(args.results_csv, summary)
    if args.json_out:
        os.makedirs(os.path.dirname(os.path.abspath(args.json_out)), exist_ok=True)
        with open(args.json_out, "w") as handle:
            json.dump(summary, handle, indent=2)
        print(f"wrote {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
