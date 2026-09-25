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

## Live metrics and runtime controls

While a job runs, `DownloadHandle::snapshot()` returns a coherent
`ProgressSnapshot`; after it finishes, `DownloadResult` reports the final
accounting. Retransferred bytes (`wasted_bytes`) are measured at retry
boundaries — never derived from warning counts.

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

`DownloadResult` carries `completed_bytes` (unique, scheduler-accepted
coverage), `bytes_reused_from_checkpoint`, `wasted_bytes`, and `retries`,
so callers can distinguish useful progress from retransferred overhead.

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

## Safety and durability

- HTTPS certificate and hostname validation are enabled by default. Custom
  CA bundles require explicit `EngineConfig::tls` configuration.
- HTTP→HTTPS redirects are allowed; HTTPS→HTTP downgrade redirects are
  denied by default.
- Credentials do not cross origins by default and are redacted from logs.
- Downloads use `<destination>.part` plus an atomic `<destination>.kdown`
  checkpoint sidecar. `DurabilityMode::Performance` records page-cache
  acknowledgements; `DurabilityMode::Durable` flushes data before checkpoint
  ranges are recorded.
- The destination is committed only after exact-size and requested hash
  verification succeed.

## Verification

Minimum supported Rust: **1.85** (`rust-version` in workspace metadata,
verified by a dedicated CI job on the tracked `Cargo.lock` dependency set).

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

See [`docs/acceptance-v1.md`](docs/acceptance-v1.md) for the v1 acceptance
See [`docs/acceptance-v1.md`](docs/acceptance-v1.md) for the v1 acceptance
mapping and [`crates/engine/benches/results/baseline.md`](crates/engine/benches/results/baseline.md)
for the loopback benchmark baseline.

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
  `BufferPool` copies. `buffer_pool_max_bytes` bounds only the standalone
  pool, NOT Hyper's internal ingress buffers.
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
