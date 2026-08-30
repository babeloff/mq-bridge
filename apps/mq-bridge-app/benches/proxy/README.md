# HTTP mirroring-proxy benchmark — mq-bridge-app vs nginx

Compares `mqb copy` acting as a **mirroring reverse proxy** against the nginx config it is meant
to replace: answer the client from prod, send a copy of every request to staging.

```bash
# what is being measured
mqb copy \
  --from 'https://0.0.0.0:8443?tls={"required":true,"cert_file":"cert.pem","key_file":"key.pem"}' \
  --to   'fanout:?mirror=http://staging.internal/&to=http://prod.internal/'
```

```nginx
location /test {
    mirror /mirror;
    mirror_request_body on;
    proxy_pass http://prod;
}
location = /mirror { internal; proxy_pass http://staging; }
```

Known gaps between the two, with code references and fix suggestions:
[`dev/http-proxy-gaps-prompt.md`](../../dev/http-proxy-gaps-prompt.md) (P1–P7).

## Run it

```bash
brew install nginx wrk
cargo build --release -p mq-bridge-app --no-default-features --features 'http,mimalloc,rustls-aws-lc'
./run.sh                       # ~6 min including cooldowns
MQB=/path/to/binary ./run.sh   # or point at an existing binary
DUR=30 CONNS=100 ./run.sh      # knobs: DUR WARM THREADS CONNS
```

Results land in `results/<date>-wrk.txt` (full wrk output) and the summary table is printed to
stdout. `work/` is scratch (configs, certs, logs) and is gitignored. The script generates its own
self-signed cert and cleans up its processes on exit.

**macOS: apply the sysctl first** (the script refuses to run quietly without it):

```bash
sudo sysctl -w net.inet.ip.portrange.first=16384 net.inet.ip.portrange.hifirst=16384 net.inet.tcp.msl=1000
# restore (does not survive a reboot anyway)
sudo sysctl -w net.inet.ip.portrange.first=49152 net.inet.ip.portrange.hifirst=49152 net.inet.tcp.msl=15000
```

## Layout

| file | what it is |
| --- | --- |
| `run.sh` | the whole A/B: 8 rows, cooldown gates, sanity checks |
| `upstream.conf` | prod (9001) + staging (9002) stubs, `return 200`, the shared upstream for every row |
| `upstream-verify.conf` | same, plus an access log on staging so mirror deliveries can be counted |
| `proxy.conf` | nginx under test: 8080 plain + 8443 TLS, `/plain` (no mirror) and `/test` (mirror) |
| `post.lua` | wrk script: POST, 200-byte body |
| `results/` | committed result sets, one per run date |

`__BENCH_DIR__` in the `.conf` files is substituted with `work/` at run time.

## Traps — every one of these produced a wrong number first

1. **nginx's `mirror` cannot keepalive.** The subrequest discards the response body, so the
   upstream connection never returns to the keepalive pool and every mirrored request pays a
   fresh TCP connect. On stock macOS that caps the mirror at ~1,090 conn/s (16,384 ephemeral
   ports ÷ 15s TIME_WAIT) and reports as `connect() ... (49: Can't assign requested address)`.
   Measured 1,045 rps and looked like an nginx result — it was the OS. Hence the sysctl above.
2. **`proxy_pass http://staging$request_uri;` bypasses the upstream keepalive pool.** With an
   `upstream` block, write `proxy_pass http://staging;` — the mirror subrequest already carries
   the original URI. (The variable form still resolves the group; it just won't pool.) Note the
   non-variable form rewrites the mirrored path to `/mirror`, where mq-bridge preserves `/test`.
3. **Cross-run contamination.** One run's TIME_WAIT backlog starves the *next* process of ports.
   That produced a row of 145k rps that was 100% 5xx, and a `Connection refused` on the row after
   it. `run.sh` now gates every row on TIME_WAIT dropping below 3,000.
4. **macOS power mode.** The same nginx row measured 22k / 40k / 98k rps under low-power mode, a
   contaminated run, and clean conditions. Confirm "reduce power" is **off**.
5. **Verify the mirror actually fires.** Count staging hits, don't assume: a broken mirror config
   looks like a *fast* proxy. `run.sh` reports `mirrored=` per row; it should equal `reqs=`.
6. **Watch `non2xx`.** mq-bridge answers 500 when the upstream returns any non-2xx (P1), and an
   error path is much faster than a working one.

## Results — 2026-08-21

M-series MacBook, 8 logical cores (4P+4E), low-power mode off, loopback, both proxies 2 workers,
wrk `-t2 -c50`, 5s warmup + 15s measured, POST 200 B.
Full output: [`results/2026-08-21-wrk.txt`](results/2026-08-21-wrk.txt).

| run | rps | p50 | p99 | non-2xx | mirrored |
| --- | --- | --- | --- | --- | --- |
| upstream direct (ceiling) | 180,592 | 265 µs | 459 µs | 0 | — |
| nginx proxy | **98,171** | 494 µs | 0.98 ms | 0 | — |
| nginx proxy + mirror | 14,912 | 1.54 ms | 112.30 ms | 0 | 98.2% |
| nginx proxy + mirror, TLS | 13,990 | 1.61 ms | 142.60 ms | 26 | 98.3% |
| mqb proxy (conc 64) | 61,543 | 789 µs | 1.22 ms | 0 | — |
| mqb proxy + mirror (conc 64) | 30,596 | 1.58 ms | 3.61 ms | 0 | 100% |
| mqb proxy + mirror (conc 4, default) | **32,709** | 1.49 ms | 2.53 ms | 0 | 100% |
| mqb proxy + mirror, TLS (conc 64) | 26,316 | 1.78 ms | 4.42 ms | 0 | 100% |

- **Plain proxying: nginx is 1.6x faster** (98,171 vs 61,543), p99 competitive (0.98 vs 1.22 ms).
- **Mirroring: mq-bridge is 2.2x faster** (32,709 vs 14,912), **44x better p99** (2.53 vs
  112.30 ms), 100% mirror delivery vs 98.2%. Cause is trap 1, not cleverness here.
- Adding the mirror costs nginx **6.6x** throughput and mq-bridge **1.9x**. That 1.9x is the
  sequential `Fanout::send` (P2) — detaching the mirror branch should recover most of it.
- **TLS is cheap on both**: -14% for mq-bridge, -6% for nginx. rustls is not the bottleneck.
- Two predictions from the code read that this **disproved** — do not re-file them: route
  concurrency 4 (the app default) does not cap proxy throughput (it beat 64), and the
  per-request `CanonicalMessage` + two header `HashMap`s cost far less than expected.

Caveats: single box, loopback, tiny bodies, no upstream latency, upstream and both proxies
sharing 8 cores. **Trust the ratios, not the absolutes.** Nothing here is published.

## Re-running after the gaps are fixed

The point of the next run is whether P1/P2/P7 changed the picture. Expect:

- **P2 detached mirror** → the mirror row should approach the plain-proxy row (the 1.9x shrinks).
- **P1 status pass-through** → `non2xx` becomes meaningful; add a row with an upstream that
  returns 404 and assert the client sees 404, not 500.
- **P7 listener survives sink failure** → add a row that kills the upstream mid-run and asserts
  502s rather than `Connection refused`.

Add the new table under a new `## Results — <date>` heading, keep the old one, and record the
mq-bridge revision both runs were built against.
