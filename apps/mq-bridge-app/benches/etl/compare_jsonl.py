#!/usr/bin/env python3
"""Assert two JSONL outputs carry the same records.

Used by run_csv_to_jsonl.sh to prove the tools did equivalent work rather than
one of them taking a cheaper path -- without this, a raw string passthrough would
score against a tool that parses and re-types every row, and the comparison would
be meaningless.

Records are compared as parsed JSON, so key order and whitespace don't count
(mq-bridge preserves the source column order inside `attributes`, Sling
alphabetizes it; JSON objects are unordered, so that is not a difference). Types
very much do count: "1" and 1 are a mismatch, which is the whole point -- the
mq-bridge run reaches Sling's typing through the transform middleware rather than
by emitting everything as a string.
"""

import argparse
import json
import sys
from itertools import zip_longest
from pathlib import Path


def rows(path: Path):
    """Yield parsed rows from a JSONL file, or from a directory of part files."""
    files = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
    for f in files:
        with f.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("left")
    ap.add_argument("right")
    ap.add_argument("--max-report", type=int, default=3)
    args = ap.parse_args()

    left = rows(Path(args.left))
    right = rows(Path(args.right))
    mismatches = 0
    count = 0

    # zip_longest, not zip: plain zip discards the one row it pulls from the
    # longer side when the shorter side ends, so a difference of exactly one row
    # would pass silently. The sentinel marks which side ran out and preserves
    # that first unmatched row in the extra count.
    missing = object()
    for count, (a, b) in enumerate(zip_longest(left, right, fillvalue=missing), start=1):
        if a is missing or b is missing:
            extra_left = (0 if a is missing else 1) + sum(1 for _ in left)
            extra_right = (0 if b is missing else 1) + sum(1 for _ in right)
            print(
                f"  row-count mismatch: {args.left} has {extra_left} extra, "
                f"{args.right} has {extra_right} extra (after {count - 1} compared)",
                file=sys.stderr,
            )
            return 1
        if a != b:
            mismatches += 1
            if mismatches <= args.max_report:
                print(f"  row {count} differs:", file=sys.stderr)
                for key in sorted(set(a) | set(b)):
                    if a.get(key) != b.get(key):
                        print(f"    {key}: {a.get(key)!r} != {b.get(key)!r}", file=sys.stderr)

    if mismatches:
        print(f"  {mismatches}/{count} rows differ", file=sys.stderr)
        return 1

    print(f"  parity OK: {count} rows identical")
    return 0


if __name__ == "__main__":
    sys.exit(main())
