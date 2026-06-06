#!/usr/bin/env python3
import argparse
import collections
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize exported xctrace time-profile XML.")
    parser.add_argument("xml", nargs="+", type=Path)
    parser.add_argument("--skip-s", type=float, default=5.0)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--write-folded", action="store_true")
    return parser.parse_args()


def category_for(stack: list[str]) -> str:
    joined = "\n".join(stack)
    if "handle_request_internal" in joined:
        return "http handle_request_internal"
    if "make_response" in joined:
        return "http make_response"
    if "HttpConsumer" in joined and "receive_batch" in joined:
        return "http receive_batch"
    if "SharedHttpRouter::match_route" in joined:
        return "http route matching"
    if "hyper::" in joined or "http::" in joined:
        return "hyper/http"
    if "tokio::sync::mpsc" in joined or "tokio..sync..mpsc" in joined:
        return "tokio mpsc"
    if "tokio::sync::oneshot" in joined or "tokio..sync..oneshot" in joined:
        return "tokio oneshot"
    if "tokio::" in joined or "mio::" in joined:
        return "tokio/mio runtime"
    if "malloc" in joined or "_xzm_" in joined or "alloc::" in joined:
        return "allocation"
    if "HashMap" in joined or "hashbrown" in joined or "SipHash" in joined:
        return "hashing/hashmap"
    if any(name in joined for name in ("__recvfrom", "writev", "kevent", "pthread_", "mach_", "clock_gettime")):
        return "system/syscall"
    if "mq_bridge" in joined:
        return "mq_bridge other"
    return "other"


def sanitize(name: str) -> str:
    return name.replace(";", "_").replace("\n", " ")


def frame_names(backtrace: ET.Element, frames_by_id: dict[str, str]) -> list[str]:
    names: list[str] = []
    for frame in backtrace.iter("frame"):
        ref = frame.attrib.get("ref")
        if ref:
            name = frames_by_id.get(ref)
        else:
            name = frame.attrib.get("name")
            frame_id = frame.attrib.get("id")
            if frame_id and name:
                frames_by_id[frame_id] = name
        if name:
            names.append(name)
    return names


def analyze(path: Path, skip_s: float, top: int, write_folded: bool) -> None:
    frames_by_id: dict[str, str] = {}
    backtraces_by_id: dict[str, list[str]] = {}
    self_counts: collections.Counter[str] = collections.Counter()
    category_counts: collections.Counter[str] = collections.Counter()
    folded_counts: collections.Counter[tuple[str, ...]] = collections.Counter()
    total = 0
    skipped = 0
    skip_ns = int(skip_s * 1_000_000_000)

    for _event, row in ET.iterparse(path, events=("end",)):
        if row.tag != "row":
            continue

        sample = row.find("sample-time")
        if sample is not None and sample.text and sample.text.isdigit():
            if int(sample.text) < skip_ns:
                skipped += 1
                row.clear()
                continue

        tagged = row.find("tagged-backtrace")
        stack: list[str] | None = None
        if tagged is not None:
            ref = tagged.attrib.get("ref")
            if ref:
                stack = backtraces_by_id.get(ref)
            else:
                tagged_id = tagged.attrib.get("id")
                backtrace = tagged.find("backtrace")
                if backtrace is not None:
                    stack = frame_names(backtrace, frames_by_id)
                    if tagged_id:
                        backtraces_by_id[tagged_id] = stack

        if stack:
            total += 1
            self_counts[stack[0]] += 1
            category_counts[category_for(stack)] += 1
            folded_counts[tuple(reversed([sanitize(name) for name in stack]))] += 1

        row.clear()

    print(f"\n== {path} ==")
    print(f"samples: {total} (skipped startup: {skipped})")
    print("\nTop self symbols:")
    for name, count in self_counts.most_common(top):
        print(f"{count:6d} {count / total * 100:6.2f}%  {name}")
    print("\nCategories:")
    for name, count in category_counts.most_common():
        print(f"{count:6d} {count / total * 100:6.2f}%  {name}")

    if write_folded:
        folded_path = path.with_suffix(".folded")
        with folded_path.open("w", encoding="utf-8") as out:
            for stack, count in folded_counts.items():
                out.write(f"{';'.join(stack)} {count}\n")
        print(f"\nFolded stacks: {folded_path}")


def main() -> int:
    args = parse_args()
    for path in args.xml:
        analyze(path, args.skip_s, args.top, args.write_folded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
