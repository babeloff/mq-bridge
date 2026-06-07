import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


RESULT_RE = re.compile(
    r"^(?P<label>.+): (?P<requests>\d+) requests in (?P<elapsed>[0-9.]+)s "
    r"\((?P<throughput>[0-9,]+) req/sec\)$"
)


def parse_csv_ints(value: str) -> list[int]:
    values = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not values:
        raise argparse.ArgumentTypeError("expected at least one integer")
    if any(item < 1 for item in values):
        raise argparse.ArgumentTypeError("values must be positive")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare mq-bridge Python HTTP handler executors across concurrency levels."
    )
    parser.add_argument("--messages", type=int, default=20_000)
    parser.add_argument("--warmup", type=int, default=2_000)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--clients", type=parse_csv_ints, default=[1, 2, 4, 8, 16])
    parser.add_argument(
        "--route-concurrency",
        type=parse_csv_ints,
        default=None,
        help="Comma-separated route concurrency values. Defaults to the clients list.",
    )
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument(
        "--target-filter",
        action="append",
        default=None,
        help="Target filter passed to bench_http_compare.py. Can be repeated.",
    )
    parser.add_argument(
        "--executors",
        choices=["worker", "direct"],
        nargs="+",
        default=["worker", "direct"],
    )
    return parser.parse_args()


def run_one(
    script: Path,
    args: argparse.Namespace,
    executor: str,
    clients: int,
    route_concurrency: int,
) -> list[tuple[str, int, float]]:
    env = os.environ.copy()
    if executor == "direct":
        env["MQ_BRIDGE_PY_HANDLER_EXECUTOR"] = "direct"
    else:
        env.pop("MQ_BRIDGE_PY_HANDLER_EXECUTOR", None)

    command = [
        sys.executable,
        str(script),
        "--messages",
        str(args.messages),
        "--warmup",
        str(args.warmup),
        "--timeout",
        str(args.timeout),
        "--clients",
        str(clients),
        "--route-concurrency",
        str(route_concurrency),
        "--batch-size",
        str(args.batch_size),
    ]
    for target_filter in args.target_filter:
        command.extend(["--target-filter", target_filter])

    completed = subprocess.run(
        command,
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    results = []
    for line in completed.stdout.splitlines():
        match = RESULT_RE.match(line)
        if match:
            results.append(
                (
                    match.group("label"),
                    int(match.group("requests")),
                    float(match.group("throughput").replace(",", "")),
                )
            )
    if not results:
        print(completed.stdout, end="")
        raise RuntimeError("benchmark produced no target results")
    return results


def main() -> None:
    args = parse_args()
    route_concurrency_values = args.route_concurrency or args.clients
    target_filters = args.target_filter or ["mq-bridge eager json"]
    script = Path(__file__).with_name("bench_http_compare.py")

    print(
        "executor,clients,route_concurrency,target,requests,req_per_sec",
        flush=True,
    )
    args.target_filter = target_filters
    for executor in args.executors:
        for clients in args.clients:
            for route_concurrency in route_concurrency_values:
                for label, requests, throughput in run_one(
                    script, args, executor, clients, route_concurrency
                ):
                    print(
                        f"{executor},{clients},{route_concurrency},"
                        f"{label},{requests},{throughput:.0f}",
                        flush=True,
                    )


if __name__ == "__main__":
    main()
