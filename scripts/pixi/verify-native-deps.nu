#!/usr/bin/env nu
# Prove the conda-forge libraries are the ones actually resolved, and show
# where from. Unlike the non-pixi task runner's equivalent there is nothing to
# discover or advise here: every one of these is pinned in pixi.lock, so a miss
# means the environment is broken rather than merely incomplete.
#
# `complete` rather than a stderr redirect: pkg-config is chatty about a miss
# and the table says MISSING more legibly, but `e> /dev/null` would not work on
# the win-64 platform this workspace also declares.

def run-quiet [...cmd: string]: nothing -> record {
    let r = (do { ^($cmd | first) ...($cmd | skip 1) } | complete)
    {ok: ($r.exit_code == 0), out: ($r.stdout | str trim)}
}

def probe [name: string]: nothing -> record {
    let v = (run-quiet "pkg-config" "--modversion" $name)
    let p = (run-quiet "pkg-config" "--variable=prefix" $name)
    {
        library: $name,
        version: (if $v.ok { $v.out } else { "MISSING" }),
        prefix:  (if $p.ok { $p.out } else { "" }),
    }
}

let pc = ([rdkafka libzmq] | each { |n| probe $n })
let pv = (run-quiet "protoc" "--version")
let rows = ($pc | append {
    library: "protoc",
    version: (if $pv.ok { $pv.out | split row " " | last } else { "MISSING" }),
    prefix:  (if $pv.ok { (which protoc | get 0.path | path dirname) } else { "" }),
})

print ($rows | table)
let missing = ($rows | where version == "MISSING" | get library)
if ($missing | is-not-empty) {
    print --stderr $"not resolved: ($missing | str join ', ')"
    exit 1
}
print "all native dependencies resolved from the environment"
