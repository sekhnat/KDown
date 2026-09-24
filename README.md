# KDown Engine

`kdown-engine` is a Rust library for correct, resumable HTTP downloads.
It supports single-stream and range-segmented transfers, HTTP/1.1 and HTTP/2,
atomic temp-file commits, checkpoints with explicit durability boundaries,
SHA-256/SHA-512 verification, pause/cancel controls, live progress and
rate/concurrency updates, bounded retries, proxy hooks, TLS validation, and
structured redacted errors.

Segmented transfers write received bytes positionally (one blocking lane per
worker, no global output lock), persist checkpoints through a single
job-level coordinator, and account every byte exactly: useful goodput,
wire throughput, and retransferred waste are reported separately.

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
use kdown_engine::{DownloadRequest, EngineConfig, HttpTransport, SingleStreamController};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::default();
    let transport = HttpTransport::from_config(&config)?;
    let controller = SingleStreamController::new(transport, config);
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
handle.set_concurrency(6);              // clamped to [min_workers, max_workers]
handle.pause();                         // checkpoints absorbed progress first
handle.resume_now();
handle.cancel_with(kdown_engine::CancelMode::KeepPartial);
```

`DownloadResult` carries `completed_bytes` (unique, scheduler-accepted
coverage), `bytes_reused_from_checkpoint`, `wasted_bytes`, and `retries`,
so callers can distinguish useful progress from retransferred overhead.

## Configuration reference

Key `EngineConfig::transfer` fields (defaults are production-safe):

| Field | Default | Meaning |
|---|---|---|
| `initial_segment_size` | 8 MiB | First lease size in segmented mode |
| `segment_sizing` | `Explicit` | Honors `initial_segment_size`; `Automatic` derives the target from remaining coverage ÷ (workers × `auto_oversubscription`) |
| `min_segment_size` / `max_segment_size` | 256 KiB / 64 MiB | Lease clamp bounds |
| `min_workers` / `max_workers` | 1 / 8 | Worker pool bounds (runtime updates clamp here) |
| `concurrency_mode` | `Fixed` | `Adaptive` starts at `min_workers` and probes on useful goodput |
| `durability` | `Performance` | `Durable` syncs output data before ranges are persisted |
| `checkpoint_flush_interval` | 2 s | Job-level checkpoint cadence |
| `preallocate_output` | `true` | Logical `set_len` sizing of the temp file |
| `preallocate_physical` | `false` | Opt-in fallocate-style reservation (silent fallback where unsupported) |

The public `buffer_pool_max_bytes` budget (128 MiB default) bounds the
standalone `BufferPool` only; the transfer path keeps one received frame
per worker and does not consult it.

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

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

See [`docs/acceptance-v1.md`](docs/acceptance-v1.md) for the v1 acceptance
mapping and [`crates/engine/benches/results/baseline.md`](crates/engine/benches/results/baseline.md)
for the loopback benchmark baseline.

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
  remainder. HTTP/2 connection policy is independent of stream-worker
  concurrency.
- **Memory** — the transfer path writes received Hyper `Bytes` straight to
  positional writes (one outstanding frame per worker, no pooled copies).
  `buffer_pool_max_bytes` bounds the public `BufferPool` only, NOT Hyper's
  internal ingress buffers.
- **Preallocation** — `preallocate_output` sizes the temp file logically
  (`set_len`); opt-in `preallocate_physical` attempts an fallocate-style
  reservation where supported and falls back silently elsewhere. Real
  out-of-space/permission failures always surface as errors; allocation is
  never required for correctness.

See [`docs/benchmark-profiling.md`](docs/benchmark-profiling.md) for
benchmark/profiling procedures and recorded results.
