# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
[GitHub Security Advisories](https://github.com/marcomq/mq-bridge/security/advisories/new)
rather than opening a public issue.

## Supply-chain checks

`cargo deny check` runs in CI ([deny.toml](deny.toml)). Licenses, bans, and
sources are hard-gated; advisories run as an informational step, for the reasons
explained below.

## Known advisories and why they do not (mostly) affect mq-bridge

Running `cargo audit` against this repository reports findings. **Most of them
are not reachable in mq-bridge**, and the rest are narrower than the raw output
suggests. This section records the analysis so you do not have to redo it.

Two structural points come first, because they explain most of the gap between
the tool output and the real exposure:

1. **`cargo audit` and `cargo deny` read `Cargo.lock`, which is feature-blind.**
   Almost every transport in this crate sits behind an optional Cargo feature.
   A finding attributed to, say, the AWS SDK is not compiled at all in a
   Kafka-only or Postgres-only build. The table below therefore states status
   *per feature*.
2. **A crate appearing in the tree is not the same as its vulnerable code being
   used.** In one case below, a dependency pulls in an affected crate purely to
   name a type in an error enum, while the actual cryptographic work happens in
   a different, patched version.

### Current status

| Advisory | Crate / path | Feature | Status |
| --- | --- | --- | --- |
| [RUSTSEC-2026-0104](https://rustsec.org/advisories/RUSTSEC-2026-0104) | `rustls-webpki` via `rumqttc`, `aws-sdk-*` | `mqtt`, `aws` | **Not affected** — CRL code path never invoked |
| [RUSTSEC-2026-0049](https://rustsec.org/advisories/RUSTSEC-2026-0049) | `rustls-webpki 0.102.8` via `rumqttc` | `mqtt` | **Not affected** — CRL code path never invoked |
| [RUSTSEC-2026-0098](https://rustsec.org/advisories/RUSTSEC-2026-0098) | `rustls-webpki 0.102.8` via `rumqttc` | `mqtt` | **Not affected** — validation runs on patched `0.103.13` |
| [RUSTSEC-2026-0099](https://rustsec.org/advisories/RUSTSEC-2026-0099) | `rustls-webpki 0.102.8` via `rumqttc` | `mqtt` | **Not affected** — validation runs on patched `0.103.13` |
| [RUSTSEC-2026-0098](https://rustsec.org/advisories/RUSTSEC-2026-0098) | `rustls-webpki 0.101.7` via `aws-sdk-*` | `aws` | **Reachable**, narrow preconditions — see below |
| [RUSTSEC-2026-0099](https://rustsec.org/advisories/RUSTSEC-2026-0099) | `rustls-webpki 0.101.7` via `aws-sdk-*` | `aws` | **Reachable**, narrow preconditions — see below |
| [RUSTSEC-2025-0057](https://rustsec.org/advisories/RUSTSEC-2025-0057) | `fxhash` via `sled 0.34` | `sled`, `dedup` | Unmaintained only, no vulnerability |
| [RUSTSEC-2024-0384](https://rustsec.org/advisories/RUSTSEC-2024-0384) | `instant` via `sled 0.34` → `parking_lot 0.11` | `sled`, `dedup` | Unmaintained only, no vulnerability |
| [RUSTSEC-2025-0134](https://rustsec.org/advisories/RUSTSEC-2025-0134) | `rustls-pemfile` (direct + `rumqttc`) | `amqp`, `nats`, `mqtt`, `http` | Unmaintained only, no vulnerability |

Each row applies only to the features listed for it: a build that enables none of
a row's features does not compile that finding's code. `rustls-pemfile` is the
broadest — it is pulled in by `amqp`, `nats`, `mqtt`, and `http` alike.

### Detail: the `rustls-webpki` advisories

These reach the dependency tree by two independent paths.

**Path A — `rumqttc 0.25` → `rustls-webpki 0.102.8` (feature `mqtt`).**
`rumqttc` declares this dependency but uses exactly one item from it: the
`webpki::Error` variant in its own error enum (`src/tls.rs:42` is the only
reference to `webpki` in the crate). All certificate validation on the MQTT path
runs through `tokio-rustls 0.26` → `rustls 0.23` → `rustls-webpki 0.103.13`,
which is patched for all four advisories. **Not affected.**

**Path B — `aws-config` / `aws-sdk-sqs` / `aws-sdk-sns` →
`aws-smithy-http-client` → legacy `rustls 0.21` → `rustls-webpki 0.101.7`
(feature `aws`).** `aws-smithy-http-client` still selects its
`legacy-rustls-ring` connector, so this is live code on the SQS/SNS path. It
splits two ways:

- **RUSTSEC-2026-0104 and RUSTSEC-2026-0049 (CRL handling): not affected.** This
  code executes only when the application supplies a certificate revocation list
  to a client-certificate verifier. The AWS SDK is an outbound HTTPS client and
  never configures one. mq-bridge's only `WebPkiClientVerifier` — used for the
  optional mTLS HTTP server in `src/endpoints/http/mod.rs` — runs on
  `rustls 0.23` / `rustls-webpki 0.103.13` and is never given a CRL.
- **RUSTSEC-2026-0098 and RUSTSEC-2026-0099 (name constraints): reachable.**
  These fire during ordinary server-certificate chain validation, which the SQS
  and SNS clients perform on every call. Exploitation requires a
  name-constrained CA present in the trust store that issues certificates
  outside its constraints — narrow, and not something mq-bridge can trigger or
  mitigate itself. The fix depends on the AWS SDK migrating off its legacy
  `rustls 0.21` connector. We do not claim to be unaffected here.

### Detail: the unmaintained crates

None of these carry a known vulnerability; RustSec flags them as unmaintained.

- **`fxhash`, `instant`** arrive through `sled 0.34`, used by the deduplication
  store. They resolve when `sled` updates its own dependencies.
- **`rustls-pemfile`** is a direct dependency of mq-bridge as well as of
  `rumqttc`. It is superseded by pemfile support in `rustls-pki-types`.
  Migrating the direct dependency is tracked as a follow-up.

### Keeping this current

[deny.toml](deny.toml) carries these advisories as `[advisories].ignore`
entries, each with its reasoning inline, so suppressions and justifications
cannot drift apart. When an advisory is added, removed, or changes status,
update both files together.

The two tools do not report identically. `cargo deny` is configured with
`unmaintained = "workspace"`, which surfaces unmaintained crates only when we
depend on them directly — so the transitive `fxhash` and `instant` advisories
need no entry there. `cargo audit` applies no such filter and does report them.
Both are covered in the table above so either tool leads to the same answer.
