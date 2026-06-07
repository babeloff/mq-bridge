#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parents[1]
PGO_ROOT = REPO_ROOT / "target" / "pgo-http-py"
PROFRAW_DIR = PGO_ROOT / "profraw"
MERGED_PROFILE = PGO_ROOT / "merged.profdata"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build mq-bridge-py with HTTP-only Rust PGO using the HTTP comparison benchmark."
    )
    parser.add_argument("--messages", type=int, default=50_000)
    parser.add_argument("--warmup", type=int, default=5_000)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--features", default="http")
    parser.add_argument(
        "--handler-executor",
        choices=["worker", "direct"],
        default="worker",
        help="Python handler executor used during PGO training.",
    )
    parser.add_argument(
        "--skip-train",
        action="store_true",
        help="Reuse existing .profraw files and only merge/rebuild.",
    )
    return parser.parse_args()


def run(cmd: list[str], env: dict[str, str] | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def rustc_host() -> str:
    output = subprocess.check_output(["rustc", "-vV"], text=True)
    for line in output.splitlines():
        if line.startswith("host: "):
            return line.split(": ", 1)[1]
    raise RuntimeError("could not determine rustc host triple")


def llvm_profdata() -> str:
    found = shutil.which("llvm-profdata")
    if found:
        return found

    sysroot = subprocess.check_output(["rustc", "--print", "sysroot"], text=True).strip()
    candidate = (
        Path(sysroot)
        / "lib"
        / "rustlib"
        / rustc_host()
        / "bin"
        / "llvm-profdata"
    )
    if candidate.exists():
        return str(candidate)
    raise RuntimeError("llvm-profdata not found on PATH or in the rust toolchain")


def maturin_cmd(features: str) -> list[str]:
    return [
        "uv",
        "run",
        "maturin",
        "develop",
        "--release",
        "--no-default-features",
        "--features",
        features,
    ]


def bench_cmd(args: argparse.Namespace) -> list[str]:
    return [
        "uv",
        "run",
        "python",
        "analysis/bench_http_compare.py",
        "--messages",
        str(args.messages),
        "--warmup",
        str(args.warmup),
        "--clients",
        str(args.clients),
    ]


def pgo_env(rustflags: str, args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    env["CARGO_INCREMENTAL"] = "0"
    env["CARGO_TARGET_DIR"] = str(PGO_ROOT / "target")
    env["RUSTFLAGS"] = rustflags
    if args.handler_executor == "direct":
        env["MQ_BRIDGE_PY_HANDLER_EXECUTOR"] = "direct"
    else:
        env.pop("MQ_BRIDGE_PY_HANDLER_EXECUTOR", None)
    return env


def main() -> None:
    args = parse_args()
    PGO_ROOT.mkdir(parents=True, exist_ok=True)

    if not args.skip_train:
        if PROFRAW_DIR.exists():
            shutil.rmtree(PROFRAW_DIR)
        PROFRAW_DIR.mkdir(parents=True)
        train_env = pgo_env(f"-Cprofile-generate={PROFRAW_DIR}", args)
        run(maturin_cmd(args.features), env=train_env)
        run(bench_cmd(args), env=train_env)

    profraw_files = sorted(str(path) for path in PROFRAW_DIR.glob("*.profraw"))
    if not profraw_files:
        raise RuntimeError(f"no .profraw files found in {PROFRAW_DIR}")

    run([llvm_profdata(), "merge", "-o", str(MERGED_PROFILE), *profraw_files])
    use_env = pgo_env(f"-Cprofile-use={MERGED_PROFILE}", args)
    run(maturin_cmd(args.features), env=use_env)
    print(f"PGO optimized HTTP-only mq-bridge-py installed using {MERGED_PROFILE}")


if __name__ == "__main__":
    main()
