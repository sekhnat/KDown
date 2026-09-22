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
//! use kdown_engine::{DownloadRequest, EngineConfig, HttpTransport, SingleStreamController};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = EngineConfig::default();
//! let transport = HttpTransport::from_config(&config)?;
//! let controller = SingleStreamController::new(transport, config);
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
//! let result = task.await??;
//! assert!(result.error.is_none());
//! # Ok(())
//! # }
//! ```
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
//! integrity, commit, warning, and failure events. Progress is cadence
//! batched (default 500 ms) and may be skipped by a lagging subscriber;
//! callers can always read a current [`ProgressSnapshot`] from the handle.
//! The engine never invokes user callbacks while holding scheduler or sink
//! locks; host code receives events on the consumer task's executor.
//!
//! ## Redaction
//!
//! [`Redactor`] strips URL userinfo and caller-marked sensitive query
//! parameters. Authorization, Cookie, Set-Cookie, and proxy-authorization
//! values are redacted at the logging/error boundary. Proxy and credential
//! providers do not transfer long-term secret ownership to the engine.
//!
//! See `KDownSpec.md` for the complete language-neutral architecture and
//! `docs/acceptance-v1.md` for the §42 acceptance review.

pub mod config;
pub mod control;
pub mod error;
pub mod fuzz_targets;
pub mod http;
pub mod io;
pub mod job;
pub mod metrics;
pub mod observability;
pub mod redact;
pub mod resume;
pub mod scheduler;

pub use config::{
    DurabilityMode, EngineConfig, ExpectedHash, HashAlgorithm, H2ConnectionPolicy,
    IntegrityPolicy, NetworkPolicy, OverwritePolicy, PoolConfig, ProxyConfig, ResumePolicy,
    TlsConfig, TransferPolicy,
};
pub use error::{DownloadError, ErrorCategory, Retryability};
pub use http::HttpTransport;
pub use job::controller::{
    CancelMode, DownloadHandle, DownloadRequest, DownloadResult, ResultStatus,
    SingleStreamController,
};
pub use metrics::{EngineMetrics, Event, EventHub, EventStream, MetricsSnapshot, ProgressSnapshot};
pub use redact::Redactor;
