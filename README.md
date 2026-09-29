# KDown Engine

`kdown-engine` is a Rust library for correct, resumable HTTP downloads.
It supports single-stream and range-segmented transfers over HTTP/1.1 and
HTTP/2, atomic temp-file commits, checkpoints with explicit durability
boundaries, SHA-256/SHA-512 verification, pause/cancel controls, live
progress and rate/concurrency updates, bounded retries, shared-origin
throttle coordination, proxy hooks, TLS validation, and structured redacted
errors.

By default, segmented transfers write positionally through per-worker
blocking lanes; an opt-in bounded shared write executor provides pipelined
writes. A job-level coordinator persists checkpoints, and accounting
separates unique completed bytes, wire throughput, and retransferred waste.

## Quickstart

Add the crate from this workspace or a published release:

```toml
[dependencies]
kdown-engine = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Start a job from an async application:

```rust,no_run
use std::path::PathBuf;
use kdown_engine::{DownloadRequest, EngineConfig, HttpTransport, DownloadController};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::default();
    let transport = HttpTransport::from_config(&config)?;
    let controller = DownloadController::new(transport, config);
    let request = DownloadRequest::new(
        "https://example.test/archive.tar.zst",
        PathBuf::from("archive.tar.zst"),
    );

    let (handle, task) = controller.start(request);
    while !task.is_finished() {
        let progress = handle.snapshot();
        eprintln!("{} bytes from network", progress.network_bytes);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let result = task.await??;
    println!("downloaded to {:?}", result.final_path);
    Ok(())
}
```

The example program in `crates/engine/examples/download.rs` accepts a URL
and destination:

```sh
cargo run --release --example download -- \
  https://example.test/archive.tar.zst archive.tar.zst
```

For a quick URL download with automatic filename resolution, use the
`download_link` example through the provided launcher:

```sh
scripts/download.sh https://example.test/archive.tar.zst
scripts/download.sh https://example.test/archive.tar.zst ~/downloads
```

Tuning options are forwarded to the engine: `--segments N` sets the
parallel range-worker count (and lowers the segmentation threshold so
N-way splitting applies even to smaller files; the server must still
advertise range support), `--segment-size N` sets the initial range size
(`4M`-style K/M/G decimal suffixes accepted), `--rate N` applies a
per-job bytes/s limit, and `--retries`, `--resume`, and `--overwrite`
adjust retry attempts, resume policy, and collision handling.
`--connections N` sets the parallel HTTP/2 connection slots (default 8):
each slot adds another bounded 2 MiB flow-control window, so transfers
scale past the single-connection window/RTT ceiling on high-bandwidth or
high-RTT paths — a 10 Gbit line at 50 ms RTT wants roughly `32`.
`download.sh --help` prints the full list. The final summary line reports
wall time, average speed, checkpoint reuse, wasted retransmit bytes,
retries, and the number of HTTP range requests issued.

The script wraps `cargo run --release --example download_link URL
[DIRECTORY]` (`DIRECTORY` defaults to the current directory and must
already exist). The output filename is resolved from the server's
`Content-Disposition`, the final redirected URL, or the original URL, and
an existing file is never replaced: `OverwritePolicy::Rename` picks a free
`name (1).ext` sibling instead. While the job runs, a progress line shows
transferred bytes, a decimal MB/s rate over a rolling 5 s window, and the
estimated time remaining; the ETA shows `--` while the total size is
unknown or the rate is below the engine's meaningfulness threshold
(1 KiB/s). Exit codes: 0 success, 1 download failure, 2 usage or
configuration error.

## Desktop app: the local web UI

Alongside the library, the workspace ships `kdown-app`: a local download
manager with a browser UI. It serves a bundled single-page app and a
loopback-only API from one process, with first-run root setup, live
dashboard telemetry, pause/resume/cancel with explicit artifact choices,
history, and automatic recovery after a restart.

```sh
./scripts/build_app.sh
./target/release/kdown-app serve --open
```

See [docs/web-ui.md](docs/web-ui.md) for the walkthrough and
[docs/security-local-ui.md](docs/security-local-ui.md) for the security
model. Linux is the supported platform for the desktop app.

## Supported surface and versioning

`kdown-engine` 0.1 was followed by a **breaking** API cleanup: success and
failure are unambiguous, the supported surface is a documented whitelist, and
the previously advertised injection seams are retired.

- Terminal outcomes are `Result<CompletedDownload, DownloadRunError>`:
  `Ok` means a verified, published download. Every other outcome is a typed
  `Transfer`, `Infrastructure` or `Cancelled` error, so a failed download is
  never a successful `Result` (`DownloadResult`/`ResultStatus` are removed).
- Import paths: `DownloadController`, `DownloadRequest`, `DownloadHandle`,
  `CancelMode`, `JobState`, the outcome/error types, `EngineConfig`,
  `HttpTransport` and the metrics/event types are re-exported at the crate
  root. The scheduler, job, I/O, resume (beyond the checkpoint-store injection
  surface), observability and fuzz modules are implementation details, not
  consumer API.
- Retired seams: `http::HttpExecution`, `http::HttpExecutor`,
  `http::scripted::*`, `http::probe::ProbeMetadata` and
  `DownloadController::with_execution*` have no supported replacement;
  construct a real `HttpTransport` with `DownloadController::new` (or
  `with_metrics`) instead.
- Policy: the supported surface follows SemVer with `#[non_exhaustive]` enums
  on expansion-prone types, and the MSRV (Rust 1.85) may only move in a major
  release. Signature drift is caught by the external consumer fixture,
  the retired-seam negative test and `scripts/api_surface_check.sh`.

See [`docs/api-surface.md`](docs/api-surface.md) for the exact whitelist,
[`docs/migration-0.1.md`](docs/migration-0.1.md) for the old-to-new migration
tables, and [`docs/api-compatibility.md`](docs/api-compatibility.md) for the
semver/MSRV policy and drift checks.

## Live metrics and runtime controls

While a job runs, `DownloadHandle::snapshot()` returns a coherent
`ProgressSnapshot`; after it finishes, a verified, published download
reports its final accounting through `CompletedDownload.accounting`.
Retransferred bytes (`wasted_bytes`) are measured at retry boundaries —
never derived from warning counts.

```rust,no_run
let snapshot = handle.snapshot();
println!(
    "completed {} B, useful {:.0} MiB/s, wire {:.0} MiB/s, retries {}",
    snapshot.completed_bytes,
    snapshot.useful_goodput_per_sec() / 1024.0 / 1024.0,
    snapshot.wire_throughput_per_sec() / 1024.0 / 1024.0,
    snapshot.retries,
);

// All of these take effect on a running job:
handle.set_rate_limit(4 * 1024 * 1024); // stable per-job token bucket
controller.set_global_rate_limit(8 * 1024 * 1024); // engine-wide ceiling
handle.set_concurrency(6);              // clamped to [min_workers, max_workers]
handle.pause();                         // checkpoints absorbed progress first
handle.resume_now();
handle.cancel_with(kdown_engine::CancelMode::KeepPartial);
```

Rate limits apply at two levels (§18): `EngineConfig::network.rate_limit`
configures a per-job limit and `EngineConfig::global_rate_limit` an
engine-wide ceiling shared by every job of the controller — the slowest
level governs, burst is bounded (~250 ms of the rate), configured limits
seed the live buckets, and rate waits stop promptly on cancellation.
Unlimited jobs keep a lock-free fast path.

`CompletedDownload.accounting` carries `completed_bytes` (unique,
scheduler-accepted coverage), `bytes_reused_from_checkpoint`,
`wasted_bytes`, and `retries`, so callers can distinguish useful progress
from retransferred overhead. Every non-success terminal outcome is a typed
`DownloadRunError`: `Transfer` (remote transfer failure), `Infrastructure`
(engine or local-environment failure), or `Cancelled` — each carrying the
partial accounting and retained-artifact disposition, so a failed transfer
is never a successful `Result`.

`accounting.wire_amplification()` reports payload received from the network
over unique output coverage (`completed_bytes` plus
`bytes_reused_from_checkpoint`), counting every wire byte once: retries
inflate the numerator, a mostly-reused resume can report below `1.0`, and a
job with no unique coverage reports `None`. The benchmark harness's
server-emitted-byte amplification is a separate, explicitly labeled axis.

Beyond per-job counters, the transport exposes protocol-level
instrumentation shared by every job it serves:
`HttpTransport::connection_limits()` reports live physical connections
(per origin), and `HttpTransport::protocol_stats()` reports logical HTTP
requests separately from physical TCP/TLS establishments, each labeled
with its actually negotiated protocol — HTTP/1.x requests vs multiplexed
HTTP/2 streams (`HttpProtocolStats::requests_h1()` / `h2_streams()` /
`establishments_*`). HTTP/2 flow-control stall data and peer stream limits
are not exposed by the underlying client, so the corresponding accessor
reports `None`: that axis is labeled unavailable rather than fabricated.

The same `EngineMetrics::snapshot()` carries the transfer-memory budget view
(`MetricsSnapshot::transfer_memory`), sampled by the engine at every job
terminal: `scope` states what is accounted and what deliberately is not, and
`aggregate`/`components` report `cap`, `current` and `high_water` for the
engine-wide pool and for each component (`network_ingress`, `frames`,
`writer`, `checkpoint`). `job_memory_high_water_max` reports the largest
single-job high-water. Before the first sample the field is `None` —
unmeasured telemetry is never reported as zero. `MetricsSnapshot` is
`Serialize`, so a JSON export is just `serde_json::to_value(metrics.snapshot())`.

A `DownloadController` also shares an origin registry across its jobs.
Requests to the same final HTTP(S) origin use cancellable, fair admission;
429/503 responses and capped `Retry-After` delay subsequent requests from
peer jobs. Unrelated origins remain independent. This request-level gate
does not replace the transport's physical connection limits. To coordinate
jobs across separate controllers, inject the same registry with
`with_origin_registry`; a new controller otherwise owns its own registry.

## Configuration reference

Key `EngineConfig::transfer` fields (defaults are production-safe):

| Field | Default | Meaning |
|---|---|---|
| `initial_segment_size` | 8 MiB | First lease size in segmented mode |
| `segment_sizing` | `Explicit` | Honors `initial_segment_size`; `Automatic` derives the target from remaining coverage ÷ (workers × `auto_oversubscription`) |
| `min_segment_size` / `max_segment_size` | 1 MiB / 64 MiB | Lease clamp bounds |
| `min_workers` / `max_workers` | 1 / 8 | Worker pool bounds (runtime updates clamp here) |
| `concurrency_mode` | `Fixed` | `Adaptive` starts at `min_workers` and probes on useful goodput |
| `durability` | `Performance` | `Durable` syncs output data before ranges are persisted |
| `checkpoint_flush_interval` | 2 s | Job-level checkpoint cadence |
| `preallocate_output` | `true` | Logical `set_len` sizing of the temp file |
| `preallocate_physical` | `false` | Opt-in physical reservation where supported; keep off without filesystem-specific evidence |

`EngineConfig::read_buffer_size` defaults to 128 KiB and sets the
pipelined writer's reservation quantum, not Hyper's socket read size.
`write_executor.pipeline_writes` is `false` by default: per-worker lanes
remain the production path; enable it to try the shared bounded executor.
The public `buffer_pool_max_bytes` budget (128 MiB default) bounds the
standalone `BufferPool` only; the transfer path does not use that pool.

`EngineConfig::transfer_memory` bounds the whole pipeline (defaults are
production-safe; caps must satisfy `job ≤ aggregate` and
`component ≤ job`):

| Field | Default | Meaning |
|---|---|---|
| `aggregate_max_bytes` | 64 MiB | Engine-wide ceiling every accounted component of every concurrent job sums into |
| `job_max_bytes` | 8 MiB | Per-job cap across all components of one download |
| `network_ingress_max_bytes` | 1 MiB | Client ingress: HTTP/1 read buffers, header metadata and HTTP/2 flow-control windows admitted before frame ownership |
| `frames_max_bytes` | 2 MiB | Held/queued payload frames moving from ingress toward the writer |
| `writer_max_bytes` | 2 MiB | Queued and in-flight writes (subsumes the legacy `write_budget` caps) |
| `checkpoint_max_bytes` | 1 MiB | In-memory range metadata plus the serialized checkpoint a save may hold |

Every transfer-path allocation is admitted through one fair,
cancellation-aware ledger (design D3): the caps above are reserved before the
bytes are held, ownership moves with zero-copy `Bytes` without charging twice,
reservations are released on drop/ack/failure/cancel, and an atomic allocation
larger than a binding cap is refused with a typed error instead of waiting
while holding capacity. HTTP/1 and HTTP/2 ingress shapes are derived from the
ingress cap (read buffer, flow-control windows, header-list ceiling) and each
connection's worst-case footprint is carved out of the aggregate before
dialing, so client buffering is bounded before frames are admitted rather than
paused after the fact. The live view of all of this is the
`MetricsSnapshot::transfer_memory` block described above; kernel socket
buffers, allocator arenas and runtime stacks stay outside the accounted
guarantee (named in that snapshot's `scope`).

## Safety and durability

- HTTPS certificate and hostname validation are enabled by default. Custom
  CA bundles require explicit `EngineConfig::tls` configuration.
- HTTP→HTTPS redirects are allowed; HTTPS→HTTP downgrade redirects are
  denied by default.
- Credentials do not cross origins by default, including over multi-hop
  redirects and in segmented workers; explicit opt-in is required.
  URL userinfo and every query value are masked in `Debug`, error and log
  diagnostics (header values are never formatted), while the URL sent on
  the wire is unchanged.
- A fresh job never writes through or publishes a pre-existing `.part`
  entry, symlink or hard link; engine-created partials and checkpoint
  sidecars are owner-only (`0600`) on Unix, and a resumed partial is
  tightened on open. Publication trusts the destination directory: keep
  downloads in a directory whose writers you control — a non-cooperating
  writer with directory access can race path operations on some
  platforms, which pathname checks cannot eliminate.
- Downloads use `<destination>.part` plus an atomic `<destination>.kdown`
  checkpoint sidecar. `DurabilityMode::Performance` records page-cache
  acknowledgements; `DurabilityMode::Durable` flushes data before checkpoint
  ranges are recorded.
- Resume requires a comparable strong ETag or eligible matching
  Last-Modified plus the checkpoint's local binding to the partial file;
  checkpoints without comparable evidence are discarded and restarted, and
  pre-v2 checkpoint files restart conservatively. Raw request/final URLs are
  never persisted in a default sidecar.
- `transfer.job_deadline` bounds the whole job from admission (probe, body
  waits, retries, verification and the decision to publish), `max_active_jobs`
  rejects an over-cap `start` with a typed `AdmissionRejected` before any
  artifact is written, and `EventStream::next()` ends after the terminal
  outcome even while the caller still holds the handle.
- The destination is committed only after exact accepted-byte coverage and
  requested-hash verification succeed.

## Verification

Minimum supported Rust: **1.85** (`rust-version` in workspace metadata,
verified by a dedicated CI job on the tracked `Cargo.lock` dependency set).

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

Category verification lanes record machine-checkable release evidence, and a
production-stable verdict requires every mandatory gate to have fresh, passing,
release-approving evidence:

```sh
scripts/ci_lane.sh correctness|durability|resource-bound|interoperability
scripts/dynamic_checks.sh probe|miri|address|thread|self-test
scripts/bench_check.sh --release loopback|low-latency|wan|all
scripts/bench_check.sh --self-test
python3 scripts/release_gate.py merge --manifest /tmp/manifest.json artifacts/evidence artifacts/dynamic
python3 scripts/release_gate.py status --manifest /tmp/manifest.json --commit "$(git rev-parse HEAD)"
```

See [`docs/acceptance-v1.md`](docs/acceptance-v1.md) for the v1 acceptance
mapping, [`docs/performance-report-v1.md`](docs/performance-report-v1.md) for
the multi-axis performance results, matched-host baselines, thresholds and the
current production-stability blockers,
[`docs/regression-triage.md`](docs/regression-triage.md) for the seed/fixture
retention and severity-triage process, and
[`release/evidence-manifest.json`](release/evidence-manifest.json) for the gate
definitions. The historical loopback criterion baseline remains at
[`crates/engine/benches/results/baseline.md`](crates/engine/benches/results/baseline.md).

**Reading benchmark numbers:** recorded loopback/synthetic throughput is
regression evidence for the specific machine and fixture it was measured on.
It is NOT a promise of Internet download speed, and it does not imply that
segmented downloading will beat sequential downloading for a given remote
server. Real-world results depend on the server's range (`Range`) support
and correctness, server/CDN throttling, round-trip latency, available
bandwidth, HTTP version and connection behavior, the engine's connection
limits, local disk and CPU capacity, and overall environment load. CI runs
benchmarks as a smoke check only and never compares hosted-runner numbers
against a workstation baseline; compare like-for-like hosts only. See
[`docs/benchmark-profiling.md`](docs/benchmark-profiling.md) for the
measurement procedures and their scope.
## Segmented transfer tuning and durability

Defaults are production-safe and unchanged from earlier releases:

- **Checkpoint cadence** — one job-level coordinator saves acknowledged
  progress every `checkpoint_flush_interval` (2 s default) and at pause
  boundaries; saves are coalesced (unchanged snapshots are skipped) and
  never run on the network chunk path.
- **Durability boundaries** — `transfer.durability = Performance` (default)
  records page-cache-acknowledged writes; `Durable` synchronizes the output
  file BEFORE the corresponding ranges are persisted (a failed sync never
  advances the checkpoint). Checkpoint coverage never leads the promised
  durability.
- **Segment sizing** — `transfer.segment_sizing = Explicit` (default) honors
  `initial_segment_size` (8 MiB default) for initial leases, clamped to
  `[min_segment_size, max_segment_size]`; opt-in
  `segment_sizing = Automatic` derives the target from remaining coverage
  divided by (initial workers × `auto_oversubscription`, default 3).
- **Concurrency** — `transfer.concurrency_mode = Fixed` (default) keeps the
  configured fixed worker count; opt-in `Adaptive` starts at `min_workers`
  and probes upward on useful (unique-byte) goodput with hysteresis and
  cooldown. Manual `set_concurrency` (clamped to
  `[min_workers, max_workers]`) overrides the controller for the job's
  remainder. Growth is protocol-aware: on HTTP/1 an additional active worker
  means an additional physical connection, so adaptive probes never target
  more concurrency than the configured connection limits allow; on HTTP/2
  additional workers are multiplexed streams that keep the single healthy
  connection (automatic extra H2 sockets stay off, and the explicit
  `h2_policy = Additional` override retains its meaning). Storage pressure,
  retry/throttle signals and process-resource ceilings veto growth regardless
  of raw network throughput.
- **Memory** — the legacy writer holds one received frame per active
  worker; the opt-in pipelined executor uses pre-read and queued-write
  byte budgets to bound outstanding frames. Both paths avoid extra
  `BufferPool` copies. Transfer-path allocations on both paths are admitted
  through the single `transfer_memory` ledger described above: the HTTP client
  is built with an exact HTTP/1 read buffer, bounded HTTP/2 connection/stream
  windows and a response header-list ceiling, and each connection's worst-case
  ingress footprint is reserved before dialing, so ingress, held/queued frames,
  writer-held bytes and checkpoint state are bounded per job and engine-wide.
  OS socket buffers, allocator arenas, runtime stacks and client internals
  beyond the configured windows remain outside the accounted guarantee (named
  in the snapshot `scope`, never reported as zero). `buffer_pool_max_bytes`
  bounds only the standalone `BufferPool`, which the transfer path does not
  use.
- **Preallocation** — `preallocate_output` sizes the temp file logically
  (`set_len`); opt-in `preallocate_physical` attempts a fallocate-style
  reservation where supported and falls back silently elsewhere. Real
  out-of-space/permission failures surface as errors; allocation is
  never required for correctness.

See [`docs/benchmark-profiling.md`](docs/benchmark-profiling.md) for
benchmark/profiling procedures and recorded results, and the phase-gate
reports
[`crates/engine/benches/results/report-phase-5.md`](crates/engine/benches/results/report-phase-5.md)
through
[`report-phase-9.md`](crates/engine/benches/results/report-phase-9.md):
protocol-aware concurrency (H1 connection gating, H2 single-socket stream
growth, request/socket accounting), shared-origin throttle coordination,
evidence-gated hot-path tuning (rate limiting repaired end-to-end; token
leasing, counter padding and buffer resizing rejected on measurements),
physical-preallocation comparison and the final baseline-vs-optimized
matrix.
