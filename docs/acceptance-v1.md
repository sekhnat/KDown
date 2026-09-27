# KDown v1 acceptance review

Phase-5 hardening review (§42). Evidence is the
repository's unit/integration/property suites and the loopback benchmark
baseline in `crates/engine/benches/results/baseline.md`.

## Correctness

| Criterion | Evidence |
|---|---|
| Exact HTTP/1.1 output | `single_stream_tests`, `segmented_tests`, randomized disconnect suites |
| Exact segmented ranges | `range_validation_tests`, `phase3_exit_tests`, scheduler property suite |
| HTTP/2 byte-exact output | `h2_tests::h2_segmented_download_is_byte_exact` |
| Byte-exact accepted coverage (preallocation, stale partials, interrupts) | `publication_tests` short/stale-tail/preallocated-hole cases; `allocation_tests`, `single_stream_tests`, `segmented_tests` |
| Redirect credential isolation (H1/H2, absolute/relative, multi-hop, opt-in) | `http_edge_cases_tests`, `h2_tests` credential regressions; redirect resolution unit suite |
| Resume never mixes generations | `resume_tests`, `publication_tests` validator/binding admission cases; checkpoints without comparable validators or with pre-v2 formats restart conservatively |
| Hash mismatch prevents commit | integrity cases in `single_stream_tests`, `metrics_tests` |
| >4 GiB offsets | `phase3_exit_tests::oversized_4gib_sparse_download_exact` |
| Race-safe overwrite publication | Real-file `io::publish` tests cover no-replace conflicts, atomic replacement observation, and non-destructive failure; the suite runs in the Ubuntu/macOS/Windows CI matrix |

## Reliability

| Criterion | Evidence |
|---|---|
| Transient retry without completed-range replay | randomized disconnect/retry suites; tail-only scheduler tests |
| Pause/resume across restart | `resume_tests`, `crash_restart_tests` |
| Corrupt checkpoints fail safely | checkpoint unit/store tests |
| Destination ownership and crash recovery | `destination_lease` contention/release tests, controller conflict cases, and `crash_restart_tests`; persistent unlocked lockfiles remain reusable |
| Disk/sink errors stop workers | sink tests and controller terminal-error paths |
| Cancellation settles workers | handle-control and runtime-control suites |
| Scheduled parser fuzz and state-machine stress | [run 36308902628](https://github.com/sekhnat/KDown/actions/runs/36308902628): all five bounded parser targets and three seeded stress rounds passed. [Run 36308390829](https://github.com/sekhnat/KDown/actions/runs/36308390829) found the `..*` filename-invariant false positive; its seed is retained and covered by `regression_2026_09_27_fuzzer_dotdot_component_not_traversal`. |

## Performance

| Criterion | Evidence |
|---|---|
| Bounded transfer memory | End-to-end transfer-memory admission and telemetry (`transfer_memory_tests`, `ingress_bound_tests`, `metrics_transfer_tests`; adversarial multi-job/RSS profiles) plus the `transfer-resource-bounds` capability; the standalone `BufferPool` budget is no longer the claim. Per-axis release numbers and the configured-cap checks live in `docs/performance-report-v1.md`. |
| No thread per segment | Tokio worker tasks; no OS-thread worker creation |
| Connection pooling | `connection_pool_tests`, `ConnectionLimits`, idle timeout/retry config |
| H2 multiplexing | `h2_single_connection_default_multiplexes` (exactly one TLS connection) |
| Loopback throughput | `benches/results/baseline.md`: ~1.67–1.96 GiB/s, 4 workers (historical criterion axis); release gating uses the multi-axis profile suite and versioned baselines in `crates/engine/benches/results/baselines/` |
| No hot global scheduler lock | atomic `LeaseProgress` cells; boundary reconciliation tests |

## API quality

| Criterion | Evidence |
|---|---|
| Start/pause/resume/cancel/limit controls | `handle_control_tests`, runtime-control suites |
| Observe progress/events | atomic counters/events module; a retained-handle `EventStream::next()` ends after the terminal outcome (`metrics_tests` terminal-stream cases, events unit tests) |
| Enforced deadline, prompt cancellation, bounded admission | `handle_control_tests` deadline latency cases; `sink_fault_tests` commit-boundary races; `active_job_cap_rejects_immediately_and_releases_on_completion` |
| Structured errors | `DownloadError` taxonomy; error-category assertions throughout |
| Replaceable transport/sink/checkpoint layers | `HttpTransport`, `Sink`, `CheckpointStore` interfaces |
| Workspace lint wiring | `crates/engine` opts into the workspace lint set: `clippy::all` plus `unsafe_code`/`missing_debug_implementations` are enforced in the `-D warnings` clippy lane; `pedantic`/`unwrap_used`/`expect_used` stay declared-but-off with the cleanup follow-up recorded in the root `Cargo.toml` |
| Public API drift | `scripts/api_surface_check.sh` inventory plus exact-signature proofs in `tests/external_consumer_fixture.rs` |

## Security

| Criterion | Evidence |
|---|---|
| TLS validation by default | `security_tests::invalid_certificate_fails_by_default` |
| Explicit custom CA | `security_tests::custom_ca_bundle_honored` |
| No HTTPS downgrade | redirect-policy security test and redirect integration suite |
| No cross-origin credentials | `http_edge_cases_tests` and `h2_tests` credential regressions (absolute/relative, multi-hop, default vs opt-in, segmented workers); `transport_integration::credentials_stripped_on_cross_origin_redirect` |
| Secret-safe logs/errors | `redaction_audit_tests`, `request_redaction_tests`, `Redactor` unit tests: userinfo and every query value masked by default, header values never formatted |
| Partial-file ownership and sidecar privacy | symlink/hardlink/entry-swap fixtures in `publication_tests`, identity-bound publication in `io::publish`; sidecar bytes carry no URLs and owner-only permissions on Unix; trusted-destination-directory precondition documented |
| Server filenames cannot traverse | `security_tests` sanitization cases; fuzz target |
| Resource bounds | endless-header, redirect-loop, checkpoint/range parser tests |
| SSRF restrictions | allow/block `AddressFilter` tests |
| Proxy/auth safety | `proxy_tests`: CONNECT, absolute-form, bounded credential-provider stages |
| Locked dependency audit | PR audits both tracked lockfiles; `RUSTSEC-2026-0009` is narrowly excepted in `.cargo/audit.toml` because `time` is dev-only via `rcgen`, and its fix requires Rust 1.88 (above MSRV 1.85); re-review/remove before production release. `RUSTSEC-2025-0134` (unmaintained `rustls-pemfile`) remains a visible warning with no ignore; review a maintained PEM parser. |

## Release readiness (machine-checkable evidence gate)

`release/evidence-manifest.json` declares every gate a production-stable verdict
needs (three-OS correctness and durability, resource bound, interoperability,
scheduled fuzz/stress lanes, dependency audit, targeted dynamic checks, and the
loopback/low-latency/WAN performance profiles). Each verification lane records a
JSON fragment (`scripts/evidence_io.py`); the gate aggregates them and refuses
the verdict for missing, failed, stale, future-dated, wrong-commit,
unavailable, fingerprint-mismatched or non-approving evidence, for a check
requested without an explicit candidate revision, and for untriaged
high-severity defects. Freshness and revision binding are checked for every
required gate before it is counted satisfied — including non-approving
PR/smoke prerequisites, which still never approve a release:

```sh
cp release/evidence-manifest.json /tmp/manifest.json
python3 scripts/release_gate.py merge  --manifest /tmp/manifest.json artifacts/evidence artifacts/dynamic
python3 scripts/release_gate.py status --manifest /tmp/manifest.json --commit "$(git rev-parse HEAD)"
python3 scripts/release_gate.py check  --manifest /tmp/manifest.json --commit "$(git rev-parse HEAD)"
```

`python3 scripts/release_gate.py self-test` proves the blocking semantics
(missing / failed / stale / future / other-commit / missing-revision /
non-approving / unavailable / untriaged-defect), including that a stale or
wrong-commit smoke fragment satisfies nothing and that the PR benchmark
smoke can never approve a release.

**Status: production stability is NOT declared for this revision.** The local
verification run passed the Linux correctness, durability, resource-bound,
interoperability, dependency-audit and shaped-profile (low-latency, WAN)
lanes, and recorded `unavailable` for the dynamic checkers (no nightly
toolchain on this host), `failed` for the loopback performance profile (host
noise limit), and no evidence for the macOS/Windows and fuzz lanes (CI-only).
The full verdict, blockers and evidence are in
[`docs/performance-report-v1.md`](performance-report-v1.md) §4–§7. The passing
targeted regressions above are not by themselves a full-workspace or
all-platform audit: this revision carries no production-stability claim until
the commit-bound, cross-platform evidence set in the manifest is complete.

## Verification commands

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps
cargo test -p kdown-engine fuzz_targets::smoke::corpus_smoke_no_panics
cargo audit --file Cargo.lock
cargo audit --file fuzz/Cargo.lock
PROPTEST_RNG_SEED=2026092701 PROPTEST_CASES=64 cargo test --locked -p kdown-engine --lib internal_tests::scheduler_property_tests -- --test-threads=1
cargo test --locked -p kdown-engine --lib fuzz_targets::smoke::corpus_smoke_no_panics -- --exact
cargo check --locked --manifest-path fuzz/Cargo.toml --all-targets
cargo bench -p kdown-engine --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5
```

Verification lanes and gates added by the production-stability change:

```sh
scripts/ci_lane.sh correctness|durability|resource-bound|interoperability
scripts/dynamic_checks.sh probe|miri|address|thread|self-test
scripts/bench_check.sh --self-test
scripts/bench_check.sh --smoke
scripts/bench_check.sh --release loopback|low-latency|wan|all
scripts/bench_check.sh --baseline <profile>          # reviewed baseline (re-)record
python3 scripts/bench_gate.py self-test
python3 scripts/release_gate.py self-test
python3 scripts/release_gate.py merge|status|check --manifest <file>
```

`cargo audit` requires the `cargo-audit` subcommand (`cargo install cargo-audit --locked`); it reads the committed lockfiles and honors the reviewed exception in `.cargo/audit.toml`.

`cargo-fuzz` target manifests are checked with `cargo check
--manifest-path fuzz/Cargo.toml`. The current workstation does not have a
nightly toolchain/cargo-fuzz binary; the stable corpus smoke test is the
available fuzz verification. CI runners with nightly can execute each
`cargo fuzz run` target from `fuzz/README.md`.
