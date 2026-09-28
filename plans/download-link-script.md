# Download-link script plan (draft)

## Context
Provide a simple command taking a URL and downloading through `kdown-engine`, reporting MB/s and estimated time remaining. The existing `crates/engine/examples/download.rs` already polls `DownloadHandle::snapshot()` but requires an explicit destination file and displays only byte counts/retries; preserve that documented behavior. The engine's directory-target API provides automatic filenames.

## Approach
Provide `scripts/download.sh URL [DIRECTORY]` (default `.`) as a thin Cargo launcher of a new Rust example, preserving the existing documented `download` example's explicit-file CLI unchanged. Use `DirectoryDownloadRequest` with `OverwritePolicy::Rename` so a URL-only invocation resolves a safe filename and never replaces an existing output. Poll snapshots periodically; compute decimal MB/s from newly completed bytes over a rolling interval and ETA from known `ProbeCompleted.total_size` minus (completed + reused) bytes; show `--` when size/rate is unknown. Production does not emit `Event::Progress`; do not depend on it.
## Files to modify
- `crates/engine/examples/download_link.rs` — new Rust example and progress formatter; leave `download.rs` unchanged.
- `scripts/download.sh` — shell launcher with argument validation and forwarding to Cargo.
- `README.md` — document URL-only and optional directory usage, terminal output, and prerequisites.

## Reuse
- `DownloadController`, `HttpTransport`, `EngineConfig` and the existing result/error handling in `crates/engine/examples/download.rs`.
- `DirectoryDownloadRequest`, `OverwritePolicy::Rename`, and `DownloadController::start_to_directory` in `crates/engine/src/job/controller.rs`.
- `DownloadHandle::snapshot()` in `crates/engine/src/job/controller.rs` provides live completed/reused/network byte counts; `handle.events()` yields `Event::ProbeCompleted { total_size, .. }` for ETA's denominator. `ProgressEvent` has smoothed rate/ETA helpers but grep shows production does not emit `Event::Progress` (only tests do), so do **not** depend on those events for live speed. Compute a simple rolling speed from snapshots in the example instead.
## Steps
- [x] Add `crates/engine/examples/download_link.rs`: parse URL and optional existing directory (default `.`), reject extra arguments with usage/exit 2, configure the engine and `DirectoryDownloadRequest` with `Rename`, then call `start_to_directory`.
- [x] Subscribe to `handle.events()` immediately; drain `ProbeCompleted` (known total) and optionally `DestinationResolved` each tick while polling `handle.snapshot()` about 250–500 ms. Compute a rolling positive completed-byte rate in decimal MB/s, remaining bytes with saturating subtraction from total minus completed and reused, and ETA only when total/rate are meaningful. Render single-line progress to stderr; print the verified final path on success, errors on stderr and nonzero exit on failure. Handle non-TTY output without carriage-return noise.
- [x] Add `scripts/download.sh` as a Bash launcher deriving its repo root from the script location, validating the argument count and forwarding arguments safely to `cargo run --release --manifest-path "$ROOT/Cargo.toml" -p kdown-engine --example download_link -- ...`; preserve caller working directory for relative output paths.
- [x] Document usage and decimal MB/s (unknown ETA shown `--`) in `README.md` and add focused formatter/math/arg tests in the example.

## Verification
- `cargo fmt --all -- --check`; `cargo clippy --locked -p kdown-engine --all-targets -- -D warnings`; `cargo test --locked -p kdown-engine --example download_link` (rate, ETA, zero/unknown total, reused bytes, argument handling); `bash -n scripts/download.sh`.
- Run `scripts/download.sh URL` from outside the repository against a local HTTP server: verify saved basename, reported speed/ETA, final path and bytes; rerun to confirm ` (1)` Rename rather than overwrite; try optional directory with spaces, missing directory, bad args and unknown-length response (ETA `--`). Check non-interactive output has no carriage-return artifacts.
