import argparse
import http.client
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable

from mq_bridge import Route


HOST = "127.0.0.1"
PATH = "/bench"
KIND = "bench.tick"
PAYLOAD = b'{"value":0}'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare mq-bridge Python HTTP handler styles."
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=20_000,
        help="Number of measured HTTP requests per benchmark target.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2_000,
        help="Number of warmup HTTP requests per benchmark target.",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=8,
        help="Number of concurrent HTTP client threads.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for each benchmark phase.",
    )
    parser.add_argument(
        "--route-concurrency",
        type=int,
        default=8,
        help="mq-bridge route worker concurrency.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
        help="mq-bridge route batch size.",
    )
    parser.add_argument(
        "--mq-output-buffer",
        action="store_true",
        help="Enable mq-bridge buffer middleware on the response output.",
    )
    return parser.parse_args()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, timeout: float) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.01)
    raise TimeoutError(f"server on {HOST}:{port} did not become ready")


def write_mq_bridge_config(
    directory: Path,
    port: int,
    route_concurrency: int,
    batch_size: int,
    clients: int,
    output_buffer: bool,
) -> Path:
    config_path = directory / f"bench_http_{port}.yaml"
    internal_buffer_size = max(1024, clients * 512)
    concurrency_limit = max(100, clients * 16)
    output_config = (
        """
    output:
      response: {}
      middlewares:
        - buffer:
            max_messages: 128
            max_delay_ms: 0
"""
        if output_buffer
        else """
    output:
      response: {}
"""
    )
    config_path.write_text(
        f"""
routes:
  http_bench:
    concurrency: {route_concurrency}
    batch_size: {batch_size}
    input:
      http:
        url: "{HOST}:{port}"
        path: "{PATH}"
        method: "POST"
        internal_buffer_size: {internal_buffer_size}
        request_timeout_ms: 30000
        concurrency_limit: {concurrency_limit}
{output_config.rstrip()}
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def run_http_phase(
    port: int,
    requests: int,
    clients: int,
    timeout: float,
    headers: dict[str, str] | None = None,
) -> tuple[float, float]:
    failures: list[BaseException] = []
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/octet-stream",
        **(headers or {}),
    }

    def send_range(start: int, stop: int) -> None:
        conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
        try:
            for _ in range(start, stop):
                conn.request("POST", PATH, body=PAYLOAD, headers=headers)
                response = conn.getresponse()
                body = response.read()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {body!r}")
                if not body:
                    raise RuntimeError("empty response body")
        except BaseException as exc:
            failures.append(exc)
        finally:
            conn.close()

    threads = []
    started_at = time.perf_counter()
    for idx in range(clients):
        start = idx * requests // clients
        stop = (idx + 1) * requests // clients
        if start >= stop:
            continue
        thread = threading.Thread(target=send_range, args=(start, stop), daemon=True)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join(timeout)
        if thread.is_alive():
            raise TimeoutError(f"HTTP phase timed out after {timeout:.1f}s")
    if failures:
        raise RuntimeError(f"HTTP client failed: {failures[0]}")

    elapsed = time.perf_counter() - started_at
    throughput = requests / elapsed if elapsed > 0 else float("inf")
    return elapsed, throughput


def module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def run_mq_bridge_target(
    label: str,
    args: argparse.Namespace,
    attach_handler: Callable[[Route], None],
) -> tuple[float, float]:
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="mqb-http-bench-") as tmp:
        config_path = write_mq_bridge_config(
            Path(tmp),
            port,
            args.route_concurrency,
            args.batch_size,
            args.clients,
            args.mq_output_buffer,
        )
        route = Route.from_yaml(str(config_path), "http_bench")
        attach_handler(route)
        route_thread = threading.Thread(target=route.run, daemon=True)
        route_thread.start()
        try:
            wait_for_port(port, args.timeout)
            if args.warmup > 0:
                run_http_phase(
                    port,
                    args.warmup,
                    args.clients,
                    args.timeout,
                    {"kind": KIND},
                )
            return run_http_phase(
                port,
                args.messages,
                args.clients,
                args.timeout,
                {"kind": KIND},
            )
        finally:
            route.stop()
            route_thread.join(timeout=2.0)


def run_process_target(
    args: argparse.Namespace,
    module_name: str,
    source: str,
    command: list[str],
) -> tuple[float, float]:
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="mqb-http-peer-") as tmp:
        module_path = Path(tmp) / f"{module_name}.py"
        module_path.write_text(source.format(host=HOST, port=port), encoding="utf-8")
        env = os.environ.copy()
        env["PYTHONPATH"] = (
            tmp
            if not env.get("PYTHONPATH")
            else f"{tmp}{os.pathsep}{env['PYTHONPATH']}"
        )
        proc = subprocess.Popen(
            [part.format(host=HOST, port=port, module=module_name) for part in command],
            cwd=tmp,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            wait_for_port(port, args.timeout)
            if args.warmup > 0:
                run_http_phase(port, args.warmup, args.clients, args.timeout)
            return run_http_phase(port, args.messages, args.clients, args.timeout)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2.0)


def run_starlette_uvicorn_target(args: argparse.Namespace) -> tuple[float, float]:
    return run_process_target(
        args,
        "bench_starlette",
        """
import json
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Route


async def bench(request):
    data = json.loads(await request.body())
    data["value"] += 1
    body = json.dumps(data, separators=(",", ":")).encode()
    return Response(body, media_type="application/json")


app = Starlette(routes=[Route("/bench", bench, methods=["POST"])])
""".lstrip(),
        [
            sys.executable,
            "-m",
            "uvicorn",
            "{module}:app",
            "--host",
            "{host}",
            "--port",
            "{port}",
            "--log-level",
            "warning",
        ],
    )


def run_fastapi_uvicorn_target(args: argparse.Namespace) -> tuple[float, float]:
    return run_process_target(
        args,
        "bench_fastapi",
        """
import json
from fastapi import FastAPI, Request, Response


app = FastAPI()


@app.post("/bench")
async def bench(request: Request):
    data = json.loads(await request.body())
    data["value"] += 1
    body = json.dumps(data, separators=(",", ":")).encode()
    return Response(body, media_type="application/json")
""".lstrip(),
        [
            sys.executable,
            "-m",
            "uvicorn",
            "{module}:app",
            "--host",
            "{host}",
            "--port",
            "{port}",
            "--log-level",
            "warning",
        ],
    )


def run_sanic_target(args: argparse.Namespace) -> tuple[float, float]:
    return run_process_target(
        args,
        "bench_sanic",
        """
import json
from sanic import Sanic, response


app = Sanic("bench_sanic")


@app.post("/bench")
async def bench(request):
    data = json.loads(request.body)
    data["value"] += 1
    body = json.dumps(data, separators=(",", ":")).encode()
    return response.raw(body, content_type="application/json")


if __name__ == "__main__":
    app.run(
        host="{host}",
        port={port},
        single_process=True,
        access_log=False,
        dev=False,
    )
""".lstrip(),
        [sys.executable, "{module}.py"],
    )


def main() -> None:
    args = parse_args()
    if args.messages < 1:
        raise ValueError("--messages must be at least 1")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or greater")
    if args.clients < 1:
        raise ValueError("--clients must be at least 1")
    if args.route_concurrency < 1:
        raise ValueError("--route-concurrency must be at least 1")
    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")

    targets: list[tuple[str, Callable[[], tuple[float, float]], bool]] = [
        (
            "mq-bridge message forward",
            lambda: run_mq_bridge_target(
                "mq-bridge message forward",
                args,
                lambda route: route.add_message_handler(
                    KIND,
                    lambda msg: msg.with_payload(msg.payload),
                ),
            ),
            True,
        ),
        (
            "mq-bridge message json",
            lambda: run_mq_bridge_target(
                "mq-bridge message json",
                args,
                lambda route: route.add_message_handler(
                    KIND,
                    lambda msg: msg.with_json(
                        {"value": msg.json()["value"] + 1}
                    ),
                ),
            ),
            True,
        ),
        (
            "mq-bridge eager json",
            lambda: run_mq_bridge_target(
                "mq-bridge eager json",
                args,
                lambda route: route.add_handler(
                    KIND,
                    lambda data: {"value": data["value"] + 1},
                ),
            ),
            True,
        ),
        (
            "Starlette + Uvicorn json",
            lambda: run_starlette_uvicorn_target(args),
            module_available("starlette") and module_available("uvicorn"),
        ),
        (
            "FastAPI + Uvicorn json",
            lambda: run_fastapi_uvicorn_target(args),
            module_available("fastapi") and module_available("uvicorn"),
        ),
        (
            "Sanic json",
            lambda: run_sanic_target(args),
            module_available("sanic"),
        ),
    ]

    print(
        f"messages={args.messages} warmup={args.warmup} clients={args.clients} "
        f"route_concurrency={args.route_concurrency} batch_size={args.batch_size} "
        f"mq_output_buffer={args.mq_output_buffer}"
    )
    for label, run_target, enabled in targets:
        if not enabled:
            print(f"{label}: skipped (dependency not installed)")
            continue
        elapsed, throughput = run_target()
        print(f"{label}: {args.messages} requests in {elapsed:.3f}s ({throughput:,.0f} req/sec)")


if __name__ == "__main__":
    main()
