# KDown v1 acceptance review

Phase-5 hardening review against `KDownSpec.md` §42. Evidence is the
repository's unit/integration/property suites and the loopback benchmark
baseline in `crates/engine/benches/results/baseline.md`.

## Correctness

| Criterion | Evidence |
|---|---|
| Exact HTTP/1.1 output | `single_stream_tests`, `segmented_tests`, randomized disconnect suites |
| Exact segmented ranges | `range_validation_tests`, `phase3_exit_tests`, scheduler property suite |
| HTTP/2 byte-exact output | `h2_tests::h2_segmented_download_is_byte_exact` |
| Resume never mixes generations | `resume_tests`, generation-change tests in controller suite |
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

## Performance

| Criterion | Evidence |
|---|---|
| Bounded transfer memory | `BufferPool` budget tests; criterion RSS record fields |
| No thread per segment | Tokio worker tasks; no OS-thread worker creation |
| Connection pooling | `connection_pool_tests`, `ConnectionLimits`, idle timeout/retry config |
| H2 multiplexing | `h2_single_connection_default_multiplexes` (exactly one TLS connection) |
| Loopback throughput | `benches/results/baseline.md`: ~1.67–1.96 GiB/s, 4 workers |
| No hot global scheduler lock | atomic `LeaseProgress` cells; boundary reconciliation tests |

## API quality

| Criterion | Evidence |
|---|---|
| Start/pause/resume/cancel/limit controls | `handle_control_tests`, runtime-control suites |
| Observe progress/events | atomic counters/events module; event API documentation |
| Structured errors | `DownloadError` taxonomy; error-category assertions throughout |
| Replaceable transport/sink/checkpoint layers | `HttpTransport`, `Sink`, `CheckpointStore` interfaces |

## Security

| Criterion | Evidence |
|---|---|
| TLS validation by default | `security_tests::invalid_certificate_fails_by_default` |
| Explicit custom CA | `security_tests::custom_ca_bundle_honored` |
| No HTTPS downgrade | redirect-policy security test and redirect integration suite |
| No cross-origin credentials | `transport_integration::credentials_stripped_on_cross_origin_redirect` |
| Secret-safe logs/errors | `redaction_audit_tests`, `Redactor` unit tests |
| Server filenames cannot traverse | `security_tests` sanitization cases; fuzz target |
| Resource bounds | endless-header, redirect-loop, checkpoint/range parser tests |
| SSRF restrictions | allow/block `AddressFilter` tests |
| Proxy/auth safety | `proxy_tests`: CONNECT, absolute-form, bounded credential-provider stages |

## Verification commands

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps
cargo test -p kdown-engine fuzz_targets::smoke::corpus_smoke_no_panics
cargo bench -p kdown-engine --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5
```

`cargo-fuzz` target manifests are checked with `cargo check
--manifest-path fuzz/Cargo.toml`. The current workstation does not have a
nightly toolchain/cargo-fuzz binary; the stable corpus smoke test is the
available fuzz verification. CI runners with nightly can execute each
`cargo fuzz run` target from `fuzz/README.md`.
