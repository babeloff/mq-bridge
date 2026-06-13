#!/usr/bin/env python3
import argparse
import http.client
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path


HOST = "127.0.0.1"
PAYLOAD = b'{"value":0}'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture a flamegraph for the Rust-only mq-bridge HTTP echo route."
    )
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--path", default="/bench")
    parser.add_argument(
        "--mode",
        default="route",
        choices=[
            "route",
            "route-fire-forget",
            "route-handler",
            "immediate",
            "metadata",
            "channel-ack",
            "direct-consumer",
            "direct-consumer-ack",
            "direct-consumer-fire-forget",
            "inline-response",
            "inline-body-only",
        ],
        help="mq_bridge_http_profile mode to run.",
    )
    parser.add_argument("--duration-s", type=int, default=45)
    parser.add_argument("--warmup-s", type=int, default=5)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--header-count", type=int, default=0)
    parser.add_argument(
        "--load-client",
        default="python",
        choices=["python", "rust"],
        help="Client used to generate load.",
    )
    parser.add_argument("--route-concurrency", type=int, default=8)
    parser.add_argument("--commit-concurrency-limit", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--internal-buffer-size", type=int, default=65536)
    parser.add_argument("--concurrency-limit", type=int, default=512)
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    parser.add_argument(
        "--startup-timeout-s",
        type=int,
        default=180,
        help="Seconds to wait for cargo build plus server startup.",
    )
    parser.add_argument(
        "--output",
        default="target/flamegraphs/http-rust-only.svg",
        help="Output SVG path.",
    )
    parser.add_argument(
        "--release-profile",
        default="release",
        help="Cargo profile passed to cargo flamegraph.",
    )
    parser.add_argument(
        "--flamegraph-wait-s",
        type=int,
        default=180,
        help="Seconds to wait for cargo-flamegraph to finish trace conversion.",
    )
    parser.add_argument(
        "--raw-xctrace",
        action="store_true",
        help="Record a raw Instruments .trace bundle instead of generating an SVG.",
    )
    parser.add_argument(
        "--no-record",
        action="store_true",
        help="Run the profile target and load generator without cargo-flamegraph/xctrace.",
    )
    args = parser.parse_args()
    if not args.path.startswith("/"):
        args.path = f"/{args.path}"
    return args


def wait_for_port(port: int, timeout: float) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server on {HOST}:{port} did not become ready")


def load_phase(
    port: int,
    path: str,
    duration_s: int,
    clients: int,
    expected_body: bytes,
    header_count: int,
) -> tuple[int, float]:
    stop_at = time.perf_counter() + duration_s
    failures: list[BaseException] = []
    counts = [0 for _ in range(clients)]
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/octet-stream",
    }
    for index in range(header_count):
        headers[f"X-Bench-{index}"] = "bench-value"

    def worker(index: int) -> None:
        conn = http.client.HTTPConnection(HOST, port, timeout=10)
        try:
            while time.perf_counter() < stop_at:
                conn.request("POST", path, body=PAYLOAD, headers=headers)
                response = conn.getresponse()
                body = response.read()
                if response.status not in (200, 202):
                    raise RuntimeError(f"HTTP {response.status}: {body!r}")
                if body != expected_body:
                    raise RuntimeError(f"unexpected body: {body!r}")
                counts[index] += 1
        except BaseException as exc:
            failures.append(exc)
        finally:
            conn.close()

    started_at = time.perf_counter()
    threads = [
        threading.Thread(target=worker, args=(idx,), daemon=True)
        for idx in range(clients)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(duration_s + 15)
    stuck = [thread.name for thread in threads if thread.is_alive()]
    if stuck:
        raise RuntimeError(f"load workers did not finish: {', '.join(stuck)}")
    if failures:
        raise RuntimeError(f"load failed: {failures[0]}")
    elapsed = time.perf_counter() - started_at
    return sum(counts), elapsed


def rust_load_phase(
    binary: Path,
    port: int,
    path: str,
    duration_s: int,
    clients: int,
    expected_body: bytes,
    header_count: int,
) -> tuple[int, float]:
    if expected_body == PAYLOAD:
        expected_arg = "payload"
    elif expected_body == b"Message processed":
        expected_arg = "message-processed"
    elif expected_body == b"Message accepted for processing":
        expected_arg = "message-accepted"
    else:
        expected_arg = "ok"
    started_at = time.perf_counter()
    load_timeout_s = duration_s + 30
    try:
        proc = subprocess.run(
            [
                str(binary),
                "--client-url",
                f"http://{HOST}:{port}{path}",
                "--duration-s",
                str(duration_s),
                "--clients",
                str(clients),
                "--expected-body",
                expected_arg,
                "--header-count",
                str(header_count),
            ],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            timeout=load_timeout_s,
        )
    except subprocess.TimeoutExpired:
        print(
            f"Rust load client timed out after {load_timeout_s}s: binary={binary} host={HOST} port={port} path={path}",
            file=sys.stderr,
        )
        raise
    elapsed = time.perf_counter() - started_at
    print(proc.stdout.strip())
    for line in proc.stdout.splitlines():
        if line.startswith("Rust load:"):
            return int(line.split()[2]), elapsed
    raise RuntimeError(f"could not parse Rust load output: {proc.stdout!r}")


def main() -> int:
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    server_duration_s = args.duration_s + args.warmup_s + 5
    target_args = [
        "--mode",
        args.mode,
        "--port",
        str(args.port),
        "--path",
        args.path,
        "--duration-s",
        str(server_duration_s),
        "--route-concurrency",
        str(args.route_concurrency),
        "--commit-concurrency-limit",
        str(args.commit_concurrency_limit),
        "--batch-size",
        str(args.batch_size),
        "--internal-buffer-size",
        str(args.internal_buffer_size),
        "--concurrency-limit",
        str(args.concurrency_limit),
        "--workers",
        str(args.workers),
    ]

    binary = Path("target") / args.release_profile / "mq_bridge_http_profile"

    if args.no_record:
        subprocess.run(
            [
                "cargo",
                "build",
                "--profile",
                args.release_profile,
                "--features",
                "http",
                "--bin",
                "mq_bridge_http_profile",
            ],
            check=True,
        )
        cmd = [str(binary), *target_args]
    elif args.raw_xctrace:
        subprocess.run(
            [
                "cargo",
                "build",
                "--profile",
                args.release_profile,
                "--features",
                "http",
                "--bin",
                "mq_bridge_http_profile",
            ],
            check=True,
        )
        cmd = [
            "xctrace",
            "record",
            "--template",
            "Time Profiler",
            "--output",
            str(output),
            "--time-limit",
            f"{server_duration_s}s",
            "--target-stdout",
            "-",
            "--launch",
            "--",
            str(binary),
            *target_args,
        ]
    else:
        cmd = [
            "cargo",
            "flamegraph",
            "--profile",
            args.release_profile,
            "--features",
            "http",
            "--bin",
            "mq_bridge_http_profile",
            "-o",
            str(output),
            "--",
            *target_args,
        ]

    if args.no_record:
        print("Starting HTTP profile target:")
    else:
        print("Starting flamegraph capture:")
    print(" ".join(cmd))
    proc = subprocess.Popen(cmd)
    try:
        wait_for_port(args.port, args.startup_timeout_s)
        if args.mode in ("route", "route-handler", "direct-consumer", "inline-response", "inline-body-only"):
            expected_body = PAYLOAD
        elif args.mode == "direct-consumer-ack":
            expected_body = b"Message processed"
        elif args.mode in ("route-fire-forget", "direct-consumer-fire-forget"):
            expected_body = b"Message accepted for processing"
        else:
            expected_body = b"ok"
        load = rust_load_phase if args.load_client == "rust" else load_phase
        if args.warmup_s > 0:
            if args.load_client == "rust":
                requests, elapsed = load(
                    binary,
                    args.port,
                    args.path,
                    args.warmup_s,
                    args.clients,
                    expected_body,
                    args.header_count,
                )
            else:
                requests, elapsed = load(
                    args.port,
                    args.path,
                    args.warmup_s,
                    args.clients,
                    expected_body,
                    args.header_count,
                )
            print(f"Warmup: {requests} requests in {elapsed:.2f}s")
        if args.load_client == "rust":
            requests, elapsed = load(
                binary,
                args.port,
                args.path,
                args.duration_s,
                args.clients,
                expected_body,
                args.header_count,
            )
        else:
            requests, elapsed = load(
                args.port,
                args.path,
                args.duration_s,
                args.clients,
                expected_body,
                args.header_count,
            )
        rate = requests / elapsed if elapsed > 0 else 0.0
        print(f"Measured load: {requests} requests in {elapsed:.2f}s ({rate:.0f} req/s)")
        return_code = proc.wait(timeout=args.flamegraph_wait_s)
        if return_code != 0:
            print(f"cargo flamegraph exited with {return_code}", file=sys.stderr)
            return return_code
    except BaseException:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.terminate()
        raise

    if args.no_record:
        print("No recording requested.")
    elif args.raw_xctrace:
        print(f"Trace: {output.resolve()}")
    else:
        print(f"Flamegraph: {output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
