#!/usr/bin/env nu
# Mirror of the feature-subset sweep in .github/workflows/ci.yml.
#
# Reports every failing subset instead of stopping at the first, so one run
# tells the whole story.
#
# `reduce` rather than a mutable accumulator: nushell closures — and `catch`
# takes one — may not capture a mutable variable. `try`/`catch` returning a
# value keeps cargo's output streaming, which `complete` would buffer until
# each subset finished.
#
# Two subsets differ from the non-pixi task runner. `grpc` needs no
# `vendored-protoc` companion, because this environment supplies protoc from
# libprotobuf. And `full-dynamic` is swept at all only because librdkafka is
# pinned in pixi.lock -- there is no way to run it in an environment that does
# not declare one.

let subsets = ["" "full" "full-dynamic" "kafka" "nats" "grpc" "mqtt" "mongodb" "http"]

let failed = ($subsets | reduce --fold [] { |s, acc|
    let label = (if ($s | is-empty) { "<default>" } else { $s })
    print $"::: cargo check --all-targets --features ($label)"
    let ok = (try {
        if ($s | is-empty) {
            ^cargo check --all-targets
        } else {
            ^cargo check --all-targets --features $s
        }
        true
    } catch {
        false
    })
    if $ok { $acc } else { $acc | append $label }
})

if ($failed | is-not-empty) {
    print --stderr $"feature subsets failed: ($failed | str join ', ')"
    exit 1
}
print $"all ($subsets | length) feature subsets check"
