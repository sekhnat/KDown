# KDown Engine — release performance report (v1, 2026-09-27)

Scope: the multi-axis release performance gate from the
`establish-production-stability` change (tasks 6.1–6.5). Companion documents:
`docs/acceptance-v1.md` (correctness/durability/security acceptance and the
release-evidence gate), `docs/benchmark-profiling.md` (measurement procedures),
`crates/engine/benches/results/baselines/*.json` (versioned baselines),
`release/evidence-manifest.json` (machine-checkable gates).

**Bottom line: production stability is NOT declared.** The measurement
machinery, pinned profiles, versioned baselines, per-axis thresholds and the
release-evidence gate are implemented and verified, and the shaped profiles pass
on this host. The loopback profile is refused as release evidence on this host
because the host exceeds the noise limit (see
[Noise disposition](#noise-disposition-not-a-pass)), and the Windows/macOS,
fuzzing and dynamic-checker lanes can only run in CI. The exact blockers are
listed below and in the machine-readable verdict.

## 1. Axes and profiles

`cargo bench --bench throughput -- --suite <profile>` runs one profile with
process-isolated, shaped fixture servers and writes a machine-readable report
(`metrics.json`, schema `kdown.bench.suite/1`) plus `report.md` per profile.
Each scenario row carries every axis independently:

| Axis | Field | Notes |
|---|---|---|
| Throughput | `throughput_mib_s` | median/min/max/best-of-N + `spread_pct` over the repetitions |
| Network amplification | `amplification` | fixture-server emitted bytes ÷ unique completed bytes |
| CPU per byte | `cpu_ns_per_byte` + `cpu_ns_per_byte_aggregate` | `/proc` ticks are 10 ms, so the gated value is the cumulative CPU over all repetitions of a scenario |
| Transfer-pipeline memory high-water | `managed_memory_high_water_bytes` + `managed_memory_peak_bytes` | engine-accounted ledger high-water plus its configured cap; RSS is recorded only as an auxiliary axis |
| Job/worker scaling | `scaling.<protocol>[]` | goodput and efficiency at every worker/job scale point |

Unmeasurable axes are emitted as `null` with the axis listed in
`unavailable_axes`; they are never written as zero and never count as passing.

Pinned profiles (constants in `crates/engine/benches/throughput.rs`, recorded in
every report):

| Profile | Dataset | RTT | Jitter | Loss | Bandwidth | Scale points | Repetitions |
|---|---|---|---|---|---|---|---|
| `loopback` | 64 MiB | – | – | – | unshaped | workers 1/4/8 × jobs 1/4 | 7 |
| `low-latency` | 8 MiB | 10 ms | 1 ms | 0.1 % | 100 Mbps (12.5 MiB/s) | workers 1/4 × jobs 1 | 5 |
| `wan` | 8 MiB | 80 ms | 20 ms | 1 % | 20 Mbps (2.5 MiB/s) | workers 1/4 × jobs 1 | 5 |

Both `h1` and `h2` (ALPN over TLS) run for every scale point. The disk
condition is observed from the destination filesystem (`/proc/mounts`) or set
with `--dest-dir`/`--disk-label`, so a tmpfs report is never compared with a
rotational-disk report.

Replay is exact when the fingerprint matches:

```sh
cargo bench --locked -p kdown-engine --bench throughput -- --suite wan --suite-out /tmp/wan
# host/config fingerprint + pinned inputs are inside metrics.json; the same command on a
# matched host reproduces fingerprint 5c2630f2e89cf749 (see the recorded baselines).
```

## 2. Gate policy

`scripts/bench_gate.py check` compares a report against
`crates/engine/benches/results/baselines/<profile>.json`. Thresholds are
versioned inside each baseline (`thresholds`), initial policy from design D7:

| Axis | Rule |
|---|---|
| Throughput | best-of-N goodput ≤ 10 % below the approved baseline |
| Scaling | scale-point efficiency ≤ 10 % below the baseline efficiency (a uniform drop is reported once, on the throughput axis) |
| Amplification | ≤ +0.05 above the baseline |
| CPU per byte | ≤ 15 % above the baseline |
| Managed memory | never above the configured cap; ≤ 10 % above the baseline **and** > 1 MiB absolute increase |
| Availability | a `null`/`unavailable` axis is an error, never a pass |
| Environment | a different host/config fingerprint is never compared |
| Freshness | a baseline older than its review window (180 days) is stale |
| Noise | run-to-run spread above 15 %, or a reference-capacity shift above 15 %, makes the run non-evidence |

Two statistics are deliberately noise-robust and documented in the baseline
files: the gated throughput is the **best of the repetitions** (the capacity
estimator; a sporadic fixed host hiccup costs a few percent of a 150 ms run but
moves a median by tens of percent), and the gated memory figure is the **peak**
high-water (a high-water mark is a peak quantity) with a 1 MiB absolute floor so
sub-megabyte frame-scheduling jitter cannot fail a release.

Per-axis independence is verified two ways:

* `scripts/bench_gate.py self-test` injects a regression on each axis and
  asserts each fails alone (`throughput`, `amplification`, `cpu`, `memory`,
  memory-cap breach, missing scenario, unavailable axis, fingerprint mismatch,
  scaling-only, stale baseline, noise).
* The same injection through real report files (`--metrics`) reproduces the
  independent failures; the throughput case additionally reports host-capacity
  noise, which blocks the release either way.

## 3. Recorded baselines

Recorded 2026-09-27 on the development host (Ryzen 7 9700X, 16 logical CPUs,
kernel 7.2.7, rustc 1.98.1, btrfs root, `/tmp` tmpfs). Each baseline carries
the host/config fingerprint, the pinned inputs, the reviewed thresholds and
`host_noise_ok`. Values below are the reviewed best-of-N throughput, the median
and spread of the recorded run, the amplification, the CPU per byte and the
managed peak high-water. `Cap` is the configured transfer-memory cap (64 MiB).

### `loopback` — fingerprint `33919309d07fed4f`, `host_noise_ok: false`

Recorded for reference and regression diffing, **not usable as release
evidence on this host**: 6 of 12 scenarios exceeded the 15 % noise limit
(documented desktop load/frequency effects), the same reason the release gate
refuses a rerun.

| Scenario | Best-of-N MiB/s | Median MiB/s | Spread % | Amplification | CPU ns/byte | Managed peak B | Cap B |
|---|---|---|---|---|---|---|---|
| `loopback/h1/workers_1/jobs_1` | 555.8 | 411.5 | 36.1 | 1.000 | 0.75 | 131072 | 67108864 |
| `loopback/h1/workers_1/jobs_4` | 906.3 | 839.7 | 11.3 | 1.000 | 0.73 | 196502 | 67108864 |
| `loopback/h1/workers_4/jobs_1` | 541.6 | 534.7 | 23.7 | 1.004 | 1.15 | 262144 | 67108864 |
| `loopback/h1/workers_4/jobs_4` | 859.4 | 816.6 | 7.3 | 1.004 | 1.14 | 262038 | 67108864 |
| `loopback/h1/workers_8/jobs_1` | 544.7 | 541.0 | 1.3 | 1.003 | 1.32 | 163840 | 67108864 |
| `loopback/h1/workers_8/jobs_4` | 815.7 | 777.5 | 7.1 | 1.002 | 1.32 | 163840 | 67108864 |
| `loopback/h2/workers_1/jobs_1` | 415.9 | 412.6 | 21.5 | 1.000 | 1.06 | 4096 | 67108864 |
| `loopback/h2/workers_1/jobs_4` | 808.9 | 707.3 | 17.5 | 1.000 | 1.10 | 4096 | 67108864 |
| `loopback/h2/workers_4/jobs_1` | 472.3 | 468.5 | 1.4 | 1.001 | 2.38 | 16384 | 67108864 |
| `loopback/h2/workers_4/jobs_4` | 823.3 | 791.8 | 18.7 | 1.003 | 2.27 | 16384 | 67108864 |
| `loopback/h2/workers_8/jobs_1` | 468.0 | 464.6 | 2.8 | 1.001 | 2.72 | 32768 | 67108864 |
| `loopback/h2/workers_8/jobs_4` | 808.8 | 673.8 | 20.9 | 1.002 | 2.57 | 32768 | 67108864 |

Reading the shape: H1 scales from ~550 MiB/s (1 worker) to ~860–910 MiB/s at 4
concurrent jobs; H2 single-connection ~416–468 MiB/s per job and ~810 MiB/s with
4 jobs. Amplification is 1.000–1.004 (segmented range requests add ≤0.4 %), CPU
is 0.7–2.7 ns per byte, and the engine-accounted pipeline high-water stays
between 4 KiB and 256 KiB against a 64 MiB cap on every scale point.

### `low-latency` — fingerprint `8fdeeeddb03afdf5`, `host_noise_ok: true`

| Scenario | Best-of-N MiB/s | Median MiB/s | Spread % | Amplification | CPU ns/byte | Managed peak B | Cap B |
|---|---|---|---|---|---|---|---|
| `low-latency/h1/workers_1/jobs_1` | 11.4 | 11.4 | 0.3 | 1.000 | 1.43 | 65536 | 67108864 |
| `low-latency/h1/workers_4/jobs_1` | 11.7 | 11.4 | 3.1 | 1.015 | 2.38 | 65536 | 67108864 |
| `low-latency/h2/workers_1/jobs_1` | 11.0 | 10.9 | 0.7 | 1.000 | 1.67 | 4096 | 67108864 |
| `low-latency/h2/workers_4/jobs_1` | 11.3 | 11.2 | 5.7 | 1.015 | 3.10 | 16384 | 67108864 |

The shared 12.5 MiB/s link is saturated at every scale point (11.0–11.7 MiB/s
measured, the remainder is connection/request overhead), so concurrency no
longer produces throughput beyond the link. Amplification rises to 1.015 at 4
workers: live-tail range splits duplicate 1.5 % of the payload under 10 ms RTT.

### `wan` — fingerprint `5c2630f2e89cf749`, `host_noise_ok: true`

| Scenario | Best-of-N MiB/s | Median MiB/s | Spread % | Amplification | CPU ns/byte | Managed peak B | Cap B |
|---|---|---|---|---|---|---|---|
| `wan/h1/workers_1/jobs_1` | 2.4 | 2.4 | 0.1 | 1.000 | 1.67 | 65536 | 67108864 |
| `wan/h1/workers_4/jobs_1` | 2.3 | 2.3 | 2.0 | 1.009 | 3.10 | 57344 | 67108864 |
| `wan/h2/workers_1/jobs_1` | 2.4 | 2.4 | 1.2 | 1.000 | 2.15 | 4096 | 67108864 |
| `wan/h2/workers_4/jobs_1` | 2.3 | 2.3 | 1.1 | 1.009 | 5.72 | 16384 | 67108864 |

The 2.5 MiB/s link is saturated (2.3–2.4 MiB/s) under 80 ms RTT, 20 ms jitter
and 1 % loss; the loss/retry path recovers byte-exact output and adds ≤0.9 %
wire overhead. H2 pays roughly twice the CPU per byte of H1 at this shape.

## 4. Local verification results

| Lane | Command | Result |
|---|---|---|
| Workspace correctness (Linux) | `scripts/ci_lane.sh correctness` (build + full matrix + clippy) | **passed** — 23 test binaries, 648 tests, 0 failures (evidence `correctness_workspace_tests_linux`) |
| Platform durability (Linux) | `scripts/ci_lane.sh durability` | **passed** — `platform_fs_tests` + `crash_restart_tests` (evidence `durability_platform_matrix_linux`) |
| Resource-bound adversarial | `scripts/ci_lane.sh resource-bound` | **passed** — transfer-memory, ingress-bound and metrics-transfer suites (evidence `resource_bound_adversarial`) |
| Interoperability | `scripts/ci_lane.sh interoperability` | **passed** — HTTP edge cases, network conditions, proxy, H2, wire amplification (evidence `interoperability_http_matrix`) |
| Stress (reduced) | two seeded rounds, 512 property cases, 58 tests per round | **passed, advisory only** — CI runs 4096 cases × 3 rounds and records the release-approving fragment (evidence `stress_state_machine`, `approves_release: false`) |
| Dependency audit | `cargo audit --file Cargo.lock`, `--file fuzz/Cargo.lock` | **passed** — no vulnerabilities; the reviewed dev-only `RUSTSEC-2026-0009` exception and the visible `RUSTSEC-2025-0134` unmaintained-parser warning remain |
| Performance: `low-latency` | `scripts/bench_check.sh --release low-latency` | **passed** (evidence `performance_low_latency`) |
| Performance: `wan` | `scripts/bench_check.sh --release wan` | **passed** (evidence `performance_wan`) |
| Performance: `loopback` | `scripts/bench_check.sh --release loopback` | **failed (noise disposition)** — reruns exceed the 15 % noise limit on this host (evidence `performance_loopback`) |
| PR smoke | `scripts/bench_check.sh --smoke` | **passed, non-approving** — criterion scenarios, one 5-repetition loopback scenario with all axes, gate self-test (evidence `performance_pr_smoke`) |
| API surface | `scripts/api_surface_check.sh` | **passed** |
| Formatting / docs | `cargo fmt --all -- --check`, `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` | **passed** |
| Gate self-tests | `dynamic_checks.sh self-test`, `release_gate.py self-test`, `bench_gate.py self-test` | **passed** |
| Miri (targeted) | `scripts/dynamic_checks.sh miri` | **unavailable** — no nightly toolchain/Miri on this host; CI runs the lane (or its documented equivalent) |
| AddressSanitizer (targeted) | `scripts/dynamic_checks.sh address` | **unavailable** — nightly `rust-src` for `-Zbuild-std` is missing locally |
| ThreadSanitizer (targeted) | `scripts/dynamic_checks.sh thread` | **unavailable** — same limitation; CI records the runner's result |

Machine-readable verdict (13 merged local fragments recorded against commit
`a8a51b3`, no reviewed exceptions):

```
PRODUCTION STABILITY NOT DECLARED: 16 blocking item(s)
  blocked: correctness_workspace_tests_macos: no evidence recorded
  blocked: correctness_workspace_tests_windows: no evidence recorded
  blocked: durability_platform_matrix_macos: no evidence recorded
  blocked: durability_platform_matrix_windows: no evidence recorded
  blocked: stress_fuzz_url: no evidence recorded
  blocked: stress_fuzz_content_range: no evidence recorded
  blocked: stress_fuzz_etag: no evidence recorded
  blocked: stress_fuzz_content_disposition: no evidence recorded
  blocked: stress_fuzz_checkpoint: no evidence recorded
  blocked: stress_state_machine: evidence does not approve release (smoke/advisory only)
  blocked: security_dynamic_miri: checker unavailable (Miri is not installed for the nightly toolchain on this runner.) and no reviewed exception
  blocked: security_dynamic_asan: checker unavailable (Nightly rust-src is unavailable, so -Zbuild-std sanitizer runs cannot execute here.) and no reviewed exception
  blocked: security_dynamic_tsan: checker unavailable (Nightly rust-src is unavailable, so ThreadSanitizer (-Zbuild-std) cannot execute here.) and no reviewed exception
  blocked: performance_loopback: evidence status is failed
  blocked: category:stress: no passing evidence for this required category
  blocked: category:security_dynamic: no passing evidence for this required category
```

All 13 fragments were recorded against commit `a8a51b3` (the revision this
report describes). Reproduce the verdict with:

```sh
cp release/evidence-manifest.json /tmp/manifest.json
python3 scripts/release_gate.py merge  --manifest /tmp/manifest.json artifacts/evidence artifacts/dynamic
python3 scripts/release_gate.py status --manifest /tmp/manifest.json --commit "$(git rev-parse HEAD)"
```

Recorded fragments: `correctness_workspace_tests_linux`,
`durability_platform_matrix_linux`, `resource_bound_adversarial`,
`interoperability_http_matrix`, `stress_state_machine` (advisory),
`security_dependency_audit`, `security_dynamic_miri|asan|tsan` (unavailable),
`performance_loopback` (failed, noise), `performance_low_latency`,
`performance_wan`, `performance_pr_smoke` (non-approving).

## 5. Noise disposition (not a pass)

The loopback profile is the only profile whose measurements are not
link-limited, so it is the one that exposes the host's own variability: repeated
recorded runs showed 1.3 %–36 % per-scenario spread, with sporadic fixed
~40 ms stalls in individual repetitions (desktop load from a browser/compositor
plus CPU frequency changes). The gate therefore reports loopback as `noise`:

```
[noise] loopback/h1/workers_1/jobs_1: run-to-run spread 35.1% exceeds the 15.0% noise limit;
        this run is not evidence about the engine - rerun on controlled hardware
```

This is deliberate: a noisy run must not pass, and it must not be misread as an
engine regression. Disposition (not a silent threshold weakening):

1. Re-record the loopback baseline and rerun `--release loopback` on a
   controlled host (pinned frequency governor, no desktop load, or a dedicated
   runner) so the spread falls under the limit.
2. Only then can `performance_loopback` record a release-approving fragment.
3. The shaped profiles (`low-latency`, `wan`) are link-limited, stay inside the
   noise limit and pass on this host.

## 6. Harness changes behind these numbers

* **Shared link rate.** `--throttle-mib-s` now emulates one shared link (a single
  slot schedule across connections) instead of capping each response
  independently. Previously N concurrent range responses each received the full
  rate, so a "100 Mbps" profile measured 33 MiB/s at 4 workers; the same shape
  now measures 11.3 MiB/s.
* **Deadline pacing with 64 KiB quanta.** Pacing is scheduled against a virtual
  clock instead of sleeping per 4 KiB chunk, which fixes the previously
  documented 1 Gbps limitation (measured 12.49 MiB/s against a 12.5 MiB/s
  target, and a 4 × 4 MiB concurrent batch completes in 1282 ms against the
  1280 ms the shared link allows).
* **Seeded jitter.** `--jitter-ms` adds a deterministic per-response delay drawn
  from the server seed, so a jittered profile replays identically.
* **Observed disk condition.** The report's `disk_label` comes from the
  destination filesystem rather than a hardcoded guess.
* **Wall-clock-free reports.** Timestamps and the fingerprint are computed
  in-process (`iso8601_now`, a stable SHA-256 fingerprint over host/config
  fields), so the same command on a matched host produces the same fingerprint.

## 7. Blockers carried forward

| Id | Blocker | Impact | Resolution |
|---|---|---|---|
| PR-1 | Loopback profile exceeds the noise limit on the shared development host | `performance_loopback` cannot approve release here | rerun/re-record on a controlled matched host (CI supports `bench_runner` dispatch input for a matched runner) |
| PR-2 | No nightly toolchain, Miri, `rust-src` or cargo-fuzz on this host | Miri/sanitizer/fuzz lanes record `unavailable` locally | scheduled CI lanes (`miri`, `sanitizer-address`, `sanitizer-thread`, `fuzz`) record the release evidence |
| PR-3 | macOS and Windows lanes are CI-only | Both OS correctness and durability gates block the local verdict | CI matrix artifacts merged into the manifest via `release_gate.py merge` (or the `ci_run_id` dispatch input) |
| PR-4 | Baseline fingerprints are host-specific by design | A run on a different host records `unavailable` (fingerprint mismatch) rather than a bogus comparison | record/review a baseline per matched environment |
| PR-5 | `RUSTSEC-2025-0134` (unmaintained `rustls-pemfile`) is a visible warning; `RUSTSEC-2026-0009` is a reviewed dev-only exception | Neither is a vulnerability today, but both are pre-release review items | replace the PEM parser (non-ring/rustls-pemfile path) and re-check the `time`/`rcgen` MSRV interaction before tagging |
