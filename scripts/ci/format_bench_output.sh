#!/usr/bin/env bash
# Reformat `cargo bench --output-format bencher` raw output into the
# "test <name> ... bench: ..." lines github-action-benchmark expects.
#
# Usage: format_bench_output.sh <input-file> <output-file>
set -euo pipefail

input="$1"
output="$2"

awk '/^test / { if ($0 ~ /bench:/) { print $0; name="" } else { name=$2 } } /^bench:/ { if (name != "") { print "test " name " ... " $0; name="" } }' "$input" > "$output"
