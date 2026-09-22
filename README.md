# KDown Engine

`kdown-engine` is a Rust library for correct, resumable HTTP downloads.
It supports single-stream and range-segmented transfers, HTTP/1.1 and HTTP/2,
atomic temp-file commits, checkpoints, SHA-256/SHA-512 verification,
pause/cancel controls, progress snapshots, bounded retries, proxy hooks,
TLS validation, and structured redacted errors.

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
