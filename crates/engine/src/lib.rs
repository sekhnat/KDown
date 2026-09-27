//! `kdown-engine`: a standalone, library-first download engine.
//!
//! Transfers one remote object to a caller-selected local destination while
//! preserving correctness under cancellation, interruption, network failure,
//! partial completion, range retries, and resource-generation changes.
//! There is no GUI/runtime-global state: an embedding application owns the
//! engine configuration, controller, event stream, and shutdown lifecycle.
//!
//! ## Start, observe, pause, resume, and cancel
//!
//! The controller returns a concurrent [`DownloadHandle`]. Calls are safe
//! from multiple tasks. Events are delivered through a broadcast stream;
//! callbacks never execute while scheduler locks are held. Progress events
//! are cadence-batched, while [`DownloadHandle::snapshot`] is authoritative
//! when a slow event consumer has lagged.
//!
//! ```no_run
//! use std::path::PathBuf;
//! use kdown_engine::{DownloadRequest, EngineConfig, HttpTransport, DownloadController};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EngineConfig::default();
//! let transport = HttpTransport::from_config(&config)?;
//! let controller = DownloadController::new(transport, config);
//! let request = DownloadRequest::new(
//!     "https://example.test/archive.bin",
//!     PathBuf::from("archive.bin"),
//! );
//! let (handle, task) = controller.start(request);
//!
//! // Observation is lock-free and safe from another task.
//! let before = handle.snapshot();
//! println!("{} network bytes", before.network_bytes);
//!
//! // Pause converges at a safe chunk boundary and persists resume state.
//! handle.pause();
//! handle.resume_now();
//!
//! // Or cancel and delete partial artifacts after workers settle.
//! // handle.cancel();
//!
//! let mut events = handle.events();
//! while let Some(event) = events.next().await {
//!     println!("{event:?}");
//! }
//! // Success means the verified output was published; every other
//! // outcome is a typed `DownloadRunError`.
//! let completed = task.await??;
//! println!("published at {}", completed.final_path.display());
//! # Ok(())
//! # }
//! ```
//!
//! ## HTTP layer boundary
//!
//! Job orchestration never touches concrete HTTP client, response-body, or
//! framing types. Statuses, `Retry-After` timing, authentication
//! challenges, range validation, generation conflicts, body overruns, and
//! read-idle timeouts are all classified inside the HTTP layer before any
//! body chunk is delivered. The transport is built by the embedding
//! application with [`HttpTransport::from_config`]; alternate HTTP
//! execution adapters and the deterministic scripted test adapters are
//! crate-internal — the former 0.1 scripted-injection seam is retired (see
//! `docs/migration-0.1.md`).
//!
//! ## Configuration reference
//!
//! [`EngineConfig`] validates concurrency bounds, timeout values, segment
//! sizes, retry settings, buffer budgets, connection pools, proxy URLs, TLS
//! trust configuration, and H2 policy before network activity begins.
//! `max_connections_total` is engine-wide; `max_connections_per_origin`
//! applies to each scheme/authority/proxy pool key. HTTP/2 uses one
//! multiplexed connection first (`H2ConnectionPolicy::Single`) and exposes
//! `Additional { max_connections }` as the §24 policy hook.
//!
//! ## Durability and resume
//!
//! [`DurabilityMode::Performance`] records page-cache-acknowledged writes;
//! [`DurabilityMode::Durable`] flushes file data before checkpoint ranges are
//! recorded. Checkpoints are versioned JSON sidecars, atomically replaced,
//! validated on load, and never claim stronger durability than configured.
//! A final destination is produced only after exact-size/hash verification
//! and an atomic commit.
//!
//! ## Events and concurrency guarantees
//!
//! [`EventHub`] broadcasts lifecycle, segment, progress, retry, resource,
//! rate-limit and runtime-concurrency control, integrity, commit, warning,
//! and failure events. `set_concurrency` publishes
//! [`Event::ConcurrencyChanged`] with the applied worker count, and
//! `set_rate_limit` publishes [`Event::RateLimitChanged`] with the
//! effective limit — each control emits only its own event. Progress is
//! cadence batched (default 500 ms) and may be skipped by a lagging
//! subscriber; callers can always read a current [`ProgressSnapshot`] from
//! the handle.
//! The engine never invokes user callbacks while holding scheduler or sink
//! locks; host code receives events on the consumer task's executor.
//!
//! ## Redaction
//!
//! [`Redactor`] strips URL userinfo and masks every query value by
//! default, because an unfamiliar query key cannot be assumed non-secret;
//! caller-marked keys and the
//! [`Redactor::with_marked_query_params_only`] opt-down remain available.
//! Authorization, Cookie, Set-Cookie, and proxy-authorization values are
//! redacted at the logging/error boundary. Proxy and credential providers
//! do not transfer long-term secret ownership to the engine.
//!
//! See `docs/acceptance-v1.md` for the acceptance review,
//! `docs/api-surface.md` for the supported surface and
//! `docs/api-compatibility.md` for the compatibility policy.

// Test builds alias the crate as its own external name so relocated
// integration tests keep their `kdown_engine::` import paths while
// compiling as crate-internal modules.
#[cfg(test)]
extern crate self as kdown_engine;

// Consumer-facing modules.
pub mod config;
pub mod control;
pub mod error;
pub mod http;
pub mod metrics;
pub mod redact;

// Implementation modules: not part of the supported surface
// (docs/api-surface.md).
// Fuzz entry points compile publicly only for the fuzzing harness
// (non-default `fuzz-entry` feature); never for consumers.
// The fuzz entry points exist for the fuzzing harness and the corpus
// smoke tests; the plain library build has no caller.
#[cfg_attr(all(not(test), not(feature = "fuzz-entry")), allow(dead_code))]
#[cfg(not(feature = "fuzz-entry"))]
mod fuzz_targets;
#[cfg(feature = "fuzz-entry")]
#[doc(hidden)]
pub mod fuzz_targets;
mod io;
mod job;
mod observability;
mod resume;
mod scheduler;

pub use config::{
    DurabilityMode, EngineConfig, ExpectedHash, H2ConnectionPolicy, HashAlgorithm, IntegrityPolicy,
    NetworkPolicy, OverwritePolicy, PoolConfig, ProxyConfig, ResumePolicy, TlsConfig,
    TransferMemoryConfig, TransferPolicy,
};
pub use error::{
    ArtifactDisposition, CancellationSummary, CompletedDownload, DownloadError, DownloadRunError,
    EngineFailure, ErrorCategory, FailureDomain, Retryability, TransferAccounting, TransferFailure,
};
pub use http::HttpTransport;
#[allow(deprecated)]
pub use job::controller::SingleStreamController;
pub use job::controller::{CancelMode, DownloadController, DownloadHandle, DownloadRequest};
pub use job::state::JobState;
pub use metrics::{EngineMetrics, Event, EventHub, EventStream, MetricsSnapshot, ProgressSnapshot};
pub use redact::Redactor;
/// Checkpoint-store injection surface for resolver implementors (§34):
/// the only `resume` items on the supported surface.
pub use resume::{
    checkpoint::{ByteRange, Checkpoint, CheckpointError},
    checkpoint_store::DurabilityMode as StoreDurabilityMode,
    checkpoint_store::{
        CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver, FileCheckpointStore,
        SidecarCheckpointResolver,
    },
};

#[cfg(test)]
mod internal_tests;

/// Test-only: a standalone transfer ledger for the scripted-execution
/// constructor path (`with_execution_and_metrics` is crate-internal and
/// relocated internal tests reach it through this helper).
#[cfg(test)]
pub(crate) fn __internal_ledger_for_test(
    config: &config::EngineConfig,
) -> std::sync::Arc<io::transfer_ledger::TransferLedger> {
    std::sync::Arc::new(io::transfer_ledger::TransferLedger::new(
        &config.transfer_memory,
        config
            .transfer_memory
            .connection_ingress_reserve(config.read_buffer_size, config.max_connections_total),
    ))
}
