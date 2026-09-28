//! Validated engine and job configuration (§8, D15 policy types).
//!
//! All values are validated at construction/request time; invalid
//! combinations return [`ConfigurationError`] before any network activity.

use crate::error::DownloadError;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Structured configuration rejection (§20 ConfigurationError).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("configuration invalid: {field}: {reason}")]
pub struct ConfigurationError {
    pub field: &'static str,
    pub reason: String,
}

fn invalid(field: &'static str, reason: impl Into<String>) -> ConfigurationError {
    ConfigurationError {
        field,
        reason: reason.into(),
    }
}

/// Bounded HTTP/2 flow-control ceiling (design D3, task 3.3): the largest
/// connection/stream receive window the transport advertises. WAN-realistic
/// throughput needs bytes-in-flight ≈ bandwidth × RTT — a 128 KiB window
/// caps one connection at window/RTT (a few MB/s on typical CDN RTTs), a
/// cap no number of multiplexed streams can exceed. The per-connection
/// ingress footprint reserves exactly this worst case from the transfer
/// ledger, so the bound stays memory-accounted end to end.
pub(crate) const INGRESS_WINDOW_CAP: u64 = 2 * 1024 * 1024;

/// Policy for handling an existing destination at final publication (§14.6).
///
/// `FailIfExists` is rejected early when the destination exists and is enforced
/// again with an atomic no-replace operation at commit; if that operation is
/// unsupported, publication fails without changing the destination. `Replace`
/// uses atomic replacement and fails non-destructively if the filesystem cannot
/// provide it safely.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverwritePolicy {
    /// Reject an existing destination before network activity and never
    /// overwrite an entry created before publication.
    #[default]
    FailIfExists,
    /// Replace the destination atomically; a failed or unsupported replacement
    /// leaves the existing destination unchanged.
    Replace,
    /// Resume existing partial output only when validator identity matches;
    /// final publication uses replacement semantics.
    ResumeIfMatching,
    /// Opt-in automatic collision handling for explicit-file and directory
    /// targets: never replace an existing entry. A free base name is taken
    /// first; occupied names fall through to `stem (1).ext` … `stem (999).ext`
    /// under a held destination lease, preferring resumable checkpointed
    /// siblings. Publication is always atomic no-replace; an occupied name or
    /// a late publication race fails with `DestinationConflict` instead of
    /// overwriting.
    Rename,
}

/// Whether a job may resume from persisted state (§7.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResumePolicy {
    /// Resume when a valid checkpoint exists; otherwise start fresh.
    #[default]
    Allowed,
    /// Refuse to resume; always start fresh (existing checkpoint untouched).
    Never,
    /// Require a checkpoint; fail when none exists.
    Required,
}

/// What durability guarantee checkpoints/data make (§15.4, D4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DurabilityMode {
    /// Checkpoint records writes acknowledged by the OS page cache.
    /// After power loss some recorded bytes may need redownload.
    #[default]
    Performance,
    /// Data is flushed before corresponding completed intervals are
    /// committed to the checkpoint.
    Durable,
}

/// Range-worker concurrency mode: `Fixed` (default) keeps the
/// configured fixed concurrency; `Adaptive` opts into the conservative
/// goodput-driven controller starting at `min_workers`. Manual runtime
/// control remains supported in both modes and suspends the controller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConcurrencyMode {
    /// Fixed concurrency (default; unchanged behavior).
    #[default]
    Fixed,
    /// Opt-in conservative adaptive range concurrency.
    Adaptive,
}

/// How initial segmented lease sizes are chosen.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SegmentSizing {
    /// Honor `initial_segment_size` (default; the existing documented
    /// meaning — previously ignored in favor of `max_segment_size`).
    #[default]
    Explicit,
    /// Opt-in automatic target: `ceil(remaining bytes / (initial active
    /// workers × `auto_oversubscription`))`, clamped to the segment bounds.
    /// Remaining coverage comes from validated intervals, not total length.
    Automatic,
    /// Opt-in duration-informed sizing: each new
    /// lease aims to hold its request for about `duration_ms`, sized from
    /// the scheduler's smoothed per-lease unique-goodput samples (request
    /// setup included in the measured service time). Allocations are
    /// clamped to the segment bounds, bounded to a 2× step change, and
    /// seeded from `initial_segment_size` until samples stabilize.
    ///
    /// Recorded tuning guidance (`benches/results/report-duration-sweep.md`):
    /// `duration_ms = 1000`
    /// with `auto_oversubscription = 3` (the ready-work factor) was at or
    /// above the explicit/automatic baselines on every non-network-bound
    /// axis with amplification ≤ 1.021 and zero retries. Opt-in only; the
    /// `Explicit` default is unchanged.
    Duration { duration_ms: u64 },
}

/// Integrity verification requirements for a job (§16).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntegrityPolicy {
    /// Expected digests as `(hex, algorithm)` pairs; only SHA-256/SHA-512
    /// are accepted in v1.
    pub expected_hashes: Vec<ExpectedHash>,
    /// Apply modification time from Last-Modified at commit, if desired.
    pub apply_mtime: bool,
}

/// A caller-provided expected digest (§7.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedHash {
    pub algorithm: HashAlgorithm,
    pub hex: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HashAlgorithm {
    Sha256,
    Sha512,
}

impl HashAlgorithm {
    /// Digest length in bytes; used to validate the hex expectation.
    #[must_use]
    pub fn digest_len(self) -> usize {
        match self {
            HashAlgorithm::Sha256 => 32,
            HashAlgorithm::Sha512 => 64,
        }
    }
}

/// Network-level policy: timeouts, TLS, redirect behavior, limits.
#[derive(Debug, Clone, PartialEq)]
pub struct NetworkPolicy {
    pub connect_timeout: Duration,
    pub tls_handshake_timeout: Duration,
    pub response_header_timeout: Duration,
    pub read_idle_timeout: Duration,
    /// Maximum redirect hops (§11.1, default 10).
    pub max_redirects: u32,
    /// Deny HTTPS→HTTP redirects (§11.1; default true).
    pub deny_https_downgrade: bool,
    /// Forward Authorization/Cookie across origins on redirect
    /// (§21.2; default false).
    pub forward_credentials_cross_origin: bool,
    /// Per-job rate limit; `None` = unlimited.
    pub rate_limit: Option<u64>,
}

impl Default for NetworkPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            tls_handshake_timeout: Duration::from_secs(10),
            response_header_timeout: Duration::from_secs(20),
            read_idle_timeout: Duration::from_secs(30),
            max_redirects: 10,
            deny_https_downgrade: true,
            forward_credentials_cross_origin: false,
            rate_limit: None,
        }
    }
}

/// Transfer shaping: segmentation, workers, durability (§8.2).
#[derive(Debug, Clone, PartialEq)]
pub struct TransferPolicy {
    pub max_workers: u32,
    pub min_workers: u32,
    pub initial_segment_size: u64,
    pub min_segment_size: u64,
    pub max_segment_size: u64,
    /// Initial-lease sizing selector: `Explicit` (default) honors
    /// `initial_segment_size`; `Automatic` opts into the derived target.
    pub segment_sizing: SegmentSizing,
    /// Oversubscription factor for `SegmentSizing::Automatic`;
    /// initial candidate 3, tuned from benchmarks.
    pub auto_oversubscription: u64,
    /// Range-worker concurrency mode: `Fixed` (default) or
    /// opt-in `Adaptive` (starts at `min_workers`; manual control wins).
    pub concurrency_mode: ConcurrencyMode,
    pub segmentation_threshold: u64,
    pub preallocate_output: bool,
    /// Opt-in physical space reservation at output preparation:
    /// attempts an fallocate-style reservation where supported; unsupported
    /// platforms/filesystems fall back to logical sizing.
    pub preallocate_physical: bool,
    pub durability: DurabilityMode,
    pub verify_range_support: bool,
    /// Total job deadline; `None` = no deadline (§4.1).
    pub job_deadline: Option<Duration>,
}

impl Default for TransferPolicy {
    fn default() -> Self {
        Self {
            max_workers: 8,
            min_workers: 1,
            initial_segment_size: 8 * 1024 * 1024,
            min_segment_size: 1024 * 1024,
            max_segment_size: 64 * 1024 * 1024,
            segment_sizing: SegmentSizing::default(),
            auto_oversubscription: 3,
            concurrency_mode: ConcurrencyMode::default(),
            segmentation_threshold: 16 * 1024 * 1024,
            preallocate_output: true,
            preallocate_physical: false,
            durability: DurabilityMode::default(),
            verify_range_support: true,
            job_deadline: None,
        }
    }
}

/// Retry shaping (§8.3).
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts_per_segment: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
    /// Retry HTTP 408 / 429 / selected 5xx / connection resets / temp DNS.
    pub retry_408: bool,
    pub retry_429: bool,
    pub retry_5xx: bool,
    pub honor_retry_after: bool,
    /// Cap applied to a server-provided `Retry-After` value.
    pub retry_after_max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts_per_segment: 8,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            retry_408: true,
            retry_429: true,
            retry_5xx: true,
            honor_retry_after: true,
            retry_after_max: Duration::from_secs(120),
        }
    }
}

/// Minimum feasible checkpoint allowance: the serialized state of a
/// minimal checkpoint (identity, URL, validators, empty ranges) must fit.
pub(crate) const MIN_CHECKPOINT_BYTES: u64 = 256;

/// End-to-end transfer-pipeline memory budget (design D3, task 3.1).
///
/// Per-job and engine-wide aggregate caps over every memory stage the
/// engine accounts: network/client ingress, held/queued frames, writer-held
/// bytes, and checkpoint state — across concurrent jobs, with each `Bytes`
/// payload charged exactly once as ownership moves through the pipeline.
///
/// This bounds the ACCOUNTED pipeline memory. It is distinct from total
/// process RSS and from operating-system socket/kernel memory, which stay
/// outside the guarantee (see `docs/benchmark-profiling.md`).
///
/// Invalid budgets fail [`EngineConfig::validate`] before any network
/// activity. The existing [`WriteBudgetConfig`] and
/// [`WriteExecutorConfig`] caps become subordinate to these limits.
#[derive(Debug, Clone, PartialEq)]
pub struct TransferMemoryConfig {
    /// Engine-wide aggregate cap: every accounted component of every
    /// concurrent job sums into this ceiling.
    pub aggregate_max_bytes: u64,
    /// Per-job cap across all components of one download.
    pub job_max_bytes: u64,
    /// Network/client ingress: HTTP client read buffers, header metadata,
    /// and flow-control windows the engine admits before frame ownership.
    pub network_ingress_max_bytes: u64,
    /// Held/queued frames: owned payload chunks moving from ingress
    /// toward the writer.
    pub frames_max_bytes: u64,
    /// Writer-held bytes: queued and in-flight writes (subsumes the
    /// legacy `write_budget` caps).
    pub writer_max_bytes: u64,
    /// Checkpoint state: in-memory range metadata plus the serialized
    /// checkpoint a save may hold. Must admit at least one minimal
    /// checkpoint (`MIN_CHECKPOINT_BYTES`).
    pub checkpoint_max_bytes: u64,
}

impl Default for TransferMemoryConfig {
    fn default() -> Self {
        Self {
            // WAN-realistic defaults: the h2 flow-control windows scale
            // from INGRESS_WINDOW_CAP (2 MiB), so the worst-case
            // connection reserve is 256 × (2 MiB + 64 KiB) ≈ 528 MiB.
            // This bounds the GUARANTEE, not actual usage — real
            // buffered ingress stays at in-flight data.
            aggregate_max_bytes: 1024 * 1024 * 1024,
            job_max_bytes: 8 * 1024 * 1024,
            network_ingress_max_bytes: 8 * 1024 * 1024,
            frames_max_bytes: 2 * 1024 * 1024,
            writer_max_bytes: 2 * 1024 * 1024,
            checkpoint_max_bytes: 1024 * 1024,
        }
    }
}

impl TransferMemoryConfig {
    /// Worst-case buffered-ingress footprint of ONE transport connection
    /// (design D3, task 3.3): the flow-control window (or HTTP/1 read
    /// buffer, whichever is larger) plus the response-header metadata
    /// allowance. `read_buffer_size` in bytes.
    ///
    /// The engine reserves `max_connections_total` of these out of the
    /// aggregate cap at ledger construction — the maximum connection
    /// footprint is known and accounted BEFORE any connection exists.
    #[must_use]
    pub fn connection_ingress_footprint(&self, read_buffer_size: u32) -> u64 {
        // Module-level ceiling shared with the transport's ingress profile.
        const MAX_HEADER_LIST_BYTES: u64 = 64 * 1024;
        let frame = u64::from(read_buffer_size);
        let window = INGRESS_WINDOW_CAP.min(self.network_ingress_max_bytes);
        let buffer = frame.min(self.network_ingress_max_bytes);
        let header = MAX_HEADER_LIST_BYTES.min(self.network_ingress_max_bytes);
        (window.max(buffer)) + header
    }

    /// The engine-wide connection-ingress carve-out (task 3.8):
    /// `max_connections_total` worst-case connection footprints, taken
    /// from the aggregate cap at ledger construction so the pipeline can
    /// never be starved by long-lived connections.
    #[must_use]
    pub fn connection_ingress_reserve(
        &self,
        read_buffer_size: u32,
        max_connections_total: u32,
    ) -> u64 {
        self.connection_ingress_footprint(read_buffer_size)
            .saturating_mul(u64::from(max_connections_total))
    }

    /// Validate the budget set (design D3): no zero caps, no contradictory
    /// nesting, minimum feasible frame/checkpoint sizes, subordination of
    /// the legacy write budgets, and headroom against accounting overflow.
    ///
    /// # Errors
    /// Returns a [`ConfigurationError`] naming the first violated field.
    pub(crate) fn validate(&self, engine: &EngineConfig) -> Result<(), ConfigurationError> {
        for (field, value) in [
            (
                "transfer_memory.aggregate_max_bytes",
                self.aggregate_max_bytes,
            ),
            ("transfer_memory.job_max_bytes", self.job_max_bytes),
            (
                "transfer_memory.network_ingress_max_bytes",
                self.network_ingress_max_bytes,
            ),
            ("transfer_memory.frames_max_bytes", self.frames_max_bytes),
            ("transfer_memory.writer_max_bytes", self.writer_max_bytes),
            (
                "transfer_memory.checkpoint_max_bytes",
                self.checkpoint_max_bytes,
            ),
        ] {
            if value == 0 {
                return Err(invalid(field, "must be greater than zero"));
            }
        }
        // Contradictory nesting: every component fits its job, the job
        // fits the aggregate.
        for (field, value) in [
            (
                "transfer_memory.network_ingress_max_bytes",
                self.network_ingress_max_bytes,
            ),
            ("transfer_memory.frames_max_bytes", self.frames_max_bytes),
            ("transfer_memory.writer_max_bytes", self.writer_max_bytes),
            (
                "transfer_memory.checkpoint_max_bytes",
                self.checkpoint_max_bytes,
            ),
        ] {
            if value > self.job_max_bytes {
                return Err(invalid(field, "must be <= transfer_memory.job_max_bytes"));
            }
        }
        if self.job_max_bytes > self.aggregate_max_bytes {
            return Err(invalid(
                "transfer_memory.job_max_bytes",
                "must be <= transfer_memory.aggregate_max_bytes",
            ));
        }
        // Minimum feasible sizes: at least one frame quantum of ingress
        // and one owned frame; at least one minimal checkpoint.
        let frame = u64::from(engine.read_buffer_size);
        if frame > self.network_ingress_max_bytes {
            return Err(invalid(
                "transfer_memory.network_ingress_max_bytes",
                "must be >= read_buffer_size (one ingress frame)",
            ));
        }
        if frame > self.frames_max_bytes {
            return Err(invalid(
                "transfer_memory.frames_max_bytes",
                "must be >= read_buffer_size (one owned frame)",
            ));
        }
        if self.checkpoint_max_bytes < MIN_CHECKPOINT_BYTES {
            return Err(invalid(
                "transfer_memory.checkpoint_max_bytes",
                "must admit one minimal serialized checkpoint",
            ));
        }
        // Overflow headroom: the ledger sums outstanding bytes across
        // components and jobs with saturating arithmetic; a cap of
        // `u64::MAX` would make the total indistinguishable from overflow.
        if self.aggregate_max_bytes == u64::MAX {
            return Err(invalid(
                "transfer_memory.aggregate_max_bytes",
                "must leave headroom below u64::MAX for accounting",
            ));
        }
        // Connection-ingress carve-out (task 3.8): the maximum total
        // connection footprint plus at least one full job cap must fit
        // the aggregate — otherwise long-lived connections could starve
        // the pipeline (a contradictory limit).
        let footprint = self.connection_ingress_footprint(engine.read_buffer_size);
        let reserved = u64::from(engine.max_connections_total)
            .checked_mul(footprint)
            .and_then(|reserved| reserved.checked_add(self.job_max_bytes));
        let Some(reserved) = reserved else {
            return Err(invalid(
                "transfer_memory.aggregate_max_bytes",
                "connection footprint reservation overflows",
            ));
        };
        if reserved > self.aggregate_max_bytes {
            return Err(invalid(
                "transfer_memory.aggregate_max_bytes",
                "must fit max_connections_total connection footprints plus one full job cap",
            ));
        }
        // Legacy write budgets become subordinate to the single ledger.
        if engine.write_budget.job_max_bytes > self.job_max_bytes {
            return Err(invalid(
                "write_budget.job_max_bytes",
                "must be <= transfer_memory.job_max_bytes",
            ));
        }
        if engine.write_budget.global_max_bytes > self.aggregate_max_bytes {
            return Err(invalid(
                "write_budget.global_max_bytes",
                "must be <= transfer_memory.aggregate_max_bytes",
            ));
        }
        if engine.write_budget.worker_read_ahead_bytes > self.writer_max_bytes {
            return Err(invalid(
                "write_budget.worker_read_ahead_bytes",
                "must be <= transfer_memory.writer_max_bytes",
            ));
        }
        if engine.write_executor.max_queued_bytes > self.aggregate_max_bytes {
            return Err(invalid(
                "write_executor.max_queued_bytes",
                "must be <= transfer_memory.aggregate_max_bytes",
            ));
        }
        Ok(())
    }
}
/// Connection pooling shape (§27).
#[derive(Debug, Clone, PartialEq)]
pub struct PoolConfig {
    /// Engine-global concurrent connection cap across all jobs.
    pub max_total: u32,
    /// Concurrent connections per origin (scheme + host + port + proxy +
    /// TLS identity, §27.1); H2 multiplexes streams over fewer.
    pub max_per_origin: u32,
    /// Idle pooled connections expire after this (§27.3).
    pub idle_timeout: Duration,
    /// TCP keepalive probe interval; `None` disables.
    pub tcp_keepalive: Option<Duration>,
    /// Enable TCP_NODELAY (default on: latency over tiny writes).
    pub tcp_nodelay: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_total: 256,
            max_per_origin: 16,
            idle_timeout: Duration::from_secs(90),
            tcp_keepalive: Some(Duration::from_secs(60)),
            tcp_nodelay: true,
        }
    }
}
/// Outstanding-write byte budgets : caps on the
/// payload bytes the engine retains between network receipt and write
/// acknowledgement (queued+executing), enforced before each body chunk is
/// read. Invalid budgets fail validation before any network activity.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteBudgetConfig {
    /// Engine-global outstanding payload bytes across all jobs.
    pub global_max_bytes: u64,
    /// Per-job outstanding payload bytes.
    pub job_max_bytes: u64,
    /// Per-worker read-ahead: submitted-but-unacknowledged bytes one
    /// worker may hold; at least one frame quantum (`read_buffer_size`),
    /// enforced by validation.
    pub worker_read_ahead_bytes: u64,
}

impl Default for WriteBudgetConfig {
    fn default() -> Self {
        // Provisional conservative limits: two
        // `read_buffer_size` frames of read-ahead per active worker, a
        // job pool admitting the default `max_workers` of them, and an
        // engine-wide aggregate. Tuned by the phase 2 benchmark gate;
        // not a committed final default.
        const FRAME: u64 = 128 * 1024; // default read_buffer_size
        Self {
            global_max_bytes: 64 * 1024 * 1024,
            job_max_bytes: 8 * 2 * FRAME,
            worker_read_ahead_bytes: 2 * FRAME,
        }
    }
}

/// Shared blocking write-executor policy : a
/// small fixed-size blocking pool serves positional writes for every
/// job's network workers, so blocking filesystem threads never scale with
/// `jobs × workers`. Invalid bounds fail validation before network
/// activity.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteExecutorConfig {
    /// Shared blocking writer threads across all jobs (provisional 2-4
    /// per design D2; tuned by the phase 2 benchmark gate).
    pub writer_threads: u32,
    /// Executor-wide bound on queued+executing payload bytes — defense in
    /// depth beyond the write budgets; must admit at least one frame
    /// quantum (validated against `read_buffer_size`).
    pub max_queued_bytes: u64,
    /// Internal rollout switch : route segmented
    /// worker writes through the shared pipelined executor instead of the
    /// legacy per-worker blocking lanes. Defaults to `false` — the legacy
    /// path remains the production behavior until the phase 2 gate proves
    /// parity; removed after acceptance.
    pub pipeline_writes: bool,
}

impl Default for WriteExecutorConfig {
    fn default() -> Self {
        Self {
            writer_threads: 4,
            max_queued_bytes: 32 * 1024 * 1024,
            pipeline_writes: false,
        }
    }
}

/// How many additional HTTP/2 connections a segmented job may open to one
/// origin beyond the first multiplexed connection (§24, D5).
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub enum H2ConnectionPolicy {
    /// One connection; all range streams multiplex over it (default).
    #[default]
    Single,
    /// Open up to `max_connections` per origin when the policy hook says
    /// the single connection underutilizes the path (server stream limits,
    /// flow-control bottleneck, measured gain — §24).
    Additional { max_connections: u32 },
}

/// Proxy selection (§28): the caller picks; the engine never reads the
/// environment implicitly.
#[derive(Clone, PartialEq, Default)]
#[non_exhaustive]
pub enum ProxyConfig {
    /// Direct connection (default).
    #[default]
    None,
    /// HTTP proxy for http:// URLs; CONNECT tunnel for https:// URLs.
    Http { url: String },
    /// SOCKS5 proxy (extension hook, §28).
    Socks5 { url: String },
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Proxy URLs may embed userinfo credentials; they are redacted
        // exactly like request URLs, while the proxy kind stays visible.
        let redacted_url = |url: &String| crate::redact::Redactor::new().redact_url(url);
        match self {
            Self::None => f.write_str("None"),
            Self::Http { url } => f
                .debug_struct("Http")
                .field("url", &redacted_url(url))
                .finish(),
            Self::Socks5 { url } => f
                .debug_struct("Socks5")
                .field("url", &redacted_url(url))
                .finish(),
        }
    }
}

/// TLS trust configuration (§21.1).
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct TlsConfig {
    /// Optional path to a PEM CA bundle; platform roots used otherwise.
    pub custom_ca_bundle: Option<PathBuf>,
}

/// Engine-wide shared configuration (§8.1 defaults).
#[derive(Clone)]
pub struct EngineConfig {
    pub max_active_jobs: u32,
    pub max_connections_total: u32,
    pub max_connections_per_origin: u32,
    pub read_buffer_size: u32,
    /// Bounds the public `BufferPool`'s own allocation ONLY:
    /// the engine's transfer path no longer constructs pools (Hyper `Bytes`
    /// flow straight to positional writes). This is NOT a bound on Hyper's
    /// internal ingress buffers or socket windows — peak RSS may
    /// transiently exceed this budget because of them.
    pub buffer_pool_max_bytes: u64,
    pub checkpoint_flush_interval: Duration,
    pub metrics_interval: Duration,
    pub prefer_http2: bool,
    /// Pooling/limits (§27); `max_connections_*` above stay authoritative
    /// for validation and are mirrored into `pool` defaults.
    pub pool: PoolConfig,
    /// HTTP/2 additional-connection policy (§24, D5).
    pub h2_policy: H2ConnectionPolicy,
    /// Proxy selection (§28); caller-defined.
    pub proxy: ProxyConfig,
    /// TLS trust overrides (§21.1).
    pub tls: TlsConfig,
    /// Optional SSRF restriction hook (§21.5): consulted before connecting
    /// and on every redirect; rejects resolved addresses/targets.
    pub address_filter: Option<Arc<dyn AddressFilter>>,
    pub transfer: TransferPolicy,
    pub retry: RetryPolicy,
    pub network: NetworkPolicy,
    /// Outstanding-write byte budgets : engine-global,
    /// per-job and per-worker read-ahead caps on unacknowledged payload.
    pub write_budget: WriteBudgetConfig,
    /// End-to-end transfer-pipeline memory budget (design D3): per-job and
    /// engine-wide caps with per-component maxima, validated before any
    /// network activity.
    pub transfer_memory: TransferMemoryConfig,
    /// Shared blocking write-executor policy : the
    /// small bounded blocking pool serving positional writes for all jobs.
    pub write_executor: WriteExecutorConfig,
    /// Engine-wide (global) payload rate limit in bytes/second shared by
    /// every job of the controller (§18: global above per-job). `None` =
    /// unlimited; per-job [`NetworkPolicy::rate_limit`] still applies.
    pub global_rate_limit: Option<u64>,
}

/// Restrict resolved addresses / redirect targets (§21.5 SSRF hook).
/// Implemented by embedding layers; the engine core makes no assumptions
/// about which ranges are allowed.
pub trait AddressFilter: Send + Sync {
    /// Decide whether the engine may connect to this resolved address
    /// (or, pre-resolution, the host:port target). `None` = allow.
    fn check(&self, target: &ConnectionTarget) -> Result<(), DownloadError>;
}

/// What the engine is about to connect to (§21.5 hook payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionTarget {
    /// Host resolved to a concrete address before connecting.
    Resolved {
        host: String,
        ip: std::net::IpAddr,
        port: u16,
    },
    /// Host not yet resolved (redirect-target pre-check).
    Unresolved { host: String, port: u16 },
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_active_jobs: 64,
            max_connections_total: 256,
            max_connections_per_origin: 16,
            read_buffer_size: 128 * 1024,
            buffer_pool_max_bytes: 128 * 1024 * 1024,
            checkpoint_flush_interval: Duration::from_secs(2),
            metrics_interval: Duration::from_millis(500),
            prefer_http2: true,
            pool: PoolConfig {
                max_total: 256,
                max_per_origin: 16,
                ..PoolConfig::default()
            },
            h2_policy: H2ConnectionPolicy::default(),
            proxy: ProxyConfig::None,
            tls: TlsConfig::default(),
            address_filter: None,
            transfer: TransferPolicy::default(),
            retry: RetryPolicy::default(),
            network: NetworkPolicy::default(),
            write_budget: WriteBudgetConfig::default(),
            write_executor: WriteExecutorConfig::default(),
            transfer_memory: TransferMemoryConfig::default(),
            global_rate_limit: None,
        }
    }
}

impl std::fmt::Debug for EngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineConfig")
            .field("max_active_jobs", &self.max_active_jobs)
            .field("max_connections_total", &self.max_connections_total)
            .field(
                "max_connections_per_origin",
                &self.max_connections_per_origin,
            )
            .field("read_buffer_size", &self.read_buffer_size)
            .field("buffer_pool_max_bytes", &self.buffer_pool_max_bytes)
            .field("transfer_memory", &self.transfer_memory)
            .field("checkpoint_flush_interval", &self.checkpoint_flush_interval)
            .field("metrics_interval", &self.metrics_interval)
            .field("prefer_http2", &self.prefer_http2)
            .field("pool", &self.pool)
            .field("h2_policy", &self.h2_policy)
            .field("proxy", &self.proxy)
            .field("tls", &self.tls)
            .field(
                "address_filter",
                &self.address_filter.as_ref().map(|_| "<filter>"),
            )
            .field("transfer", &self.transfer)
            .field("retry", &self.retry)
            .field("network", &self.network)
            .field("write_budget", &self.write_budget)
            .field("write_executor", &self.write_executor)
            .field("global_rate_limit", &self.global_rate_limit)
            .finish()
    }
}

impl PartialEq for EngineConfig {
    fn eq(&self, other: &Self) -> bool {
        // The address filter is an opaque hook; equality ignores it by
        // design (comparing behavior of a closure is meaningless).
        self.max_active_jobs == other.max_active_jobs
            && self.max_connections_total == other.max_connections_total
            && self.max_connections_per_origin == other.max_connections_per_origin
            && self.read_buffer_size == other.read_buffer_size
            && self.buffer_pool_max_bytes == other.buffer_pool_max_bytes
            && self.checkpoint_flush_interval == other.checkpoint_flush_interval
            && self.metrics_interval == other.metrics_interval
            && self.prefer_http2 == other.prefer_http2
            && self.pool == other.pool
            && self.h2_policy == other.h2_policy
            && self.proxy == other.proxy
            && self.tls == other.tls
            && self.transfer == other.transfer
            && self.retry == other.retry
            && self.network == other.network
            && self.write_budget == other.write_budget
            && self.write_executor == other.write_executor
            && self.global_rate_limit == other.global_rate_limit
    }
}

fn validate_transfer(t: &TransferPolicy) -> Result<(), ConfigurationError> {
    if t.max_workers == 0 {
        return Err(invalid("transfer.max_workers", "must be at least 1"));
    }
    if t.min_workers == 0 || t.min_workers > t.max_workers {
        return Err(invalid(
            "transfer.min_workers",
            "must be in 1..=max_workers",
        ));
    }
    if t.min_segment_size == 0 {
        return Err(invalid("transfer.min_segment_size", "must be at least 1"));
    }
    if t.max_segment_size < t.min_segment_size {
        return Err(invalid(
            "transfer.max_segment_size",
            "must be >= min_segment_size",
        ));
    }
    if t.initial_segment_size < t.min_segment_size || t.initial_segment_size > t.max_segment_size {
        return Err(invalid(
            "transfer.initial_segment_size",
            "must be within [min_segment_size, max_segment_size]",
        ));
    }
    if t.auto_oversubscription == 0 {
        return Err(invalid(
            "transfer.auto_oversubscription",
            "must be at least 1",
        ));
    }
    Ok(())
}

fn validate_retry(r: &RetryPolicy) -> Result<(), ConfigurationError> {
    if r.max_attempts_per_segment == 0 {
        return Err(invalid(
            "retry.max_attempts_per_segment",
            "must be at least 1",
        ));
    }
    if r.base_delay.is_zero() {
        return Err(invalid("retry.base_delay", "must be greater than zero"));
    }
    if r.max_delay < r.base_delay {
        return Err(invalid("retry.max_delay", "must be >= base_delay"));
    }
    if r.multiplier < 1.0 {
        return Err(invalid("retry.multiplier", "must be >= 1.0"));
    }
    Ok(())
}

fn validate_network(n: &NetworkPolicy) -> Result<(), ConfigurationError> {
    for (field, d) in [
        ("network.connect_timeout", n.connect_timeout),
        ("network.tls_handshake_timeout", n.tls_handshake_timeout),
        ("network.response_header_timeout", n.response_header_timeout),
        ("network.read_idle_timeout", n.read_idle_timeout),
    ] {
        if d.is_zero() {
            return Err(invalid(field, "must be greater than zero"));
        }
    }
    Ok(())
}

fn validate_pool(
    p: &PoolConfig,
    max_total: u32,
    max_per_origin: u32,
) -> Result<(), ConfigurationError> {
    if p.max_total == 0 || p.max_per_origin == 0 {
        return Err(invalid(
            "pool.max_total/max_per_origin",
            "must be at least 1",
        ));
    }
    if p.max_per_origin > p.max_total {
        return Err(invalid("pool.max_per_origin", "must be <= pool.max_total"));
    }
    // The legacy flat fields must agree with the structured pool config.
    if max_total != p.max_total || max_per_origin != p.max_per_origin {
        return Err(invalid(
            "pool.max_total",
            "must match max_connections_total/max_connections_per_origin",
        ));
    }
    if p.idle_timeout.is_zero() {
        return Err(invalid("pool.idle_timeout", "must be greater than zero"));
    }
    Ok(())
}

impl EngineConfig {
    /// Validate the entire configuration (§8); returns the first violation.
    ///
    /// # Errors
    /// Returns [`ConfigurationError`] describing the invalid field.
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_active_jobs == 0 {
            return Err(invalid("max_active_jobs", "must be at least 1"));
        }
        if self.max_connections_total == 0 || self.max_connections_per_origin == 0 {
            return Err(invalid("max_connections_*", "must be at least 1"));
        }
        if self.max_connections_per_origin > self.max_connections_total {
            return Err(invalid(
                "max_connections_per_origin",
                "must be <= max_connections_total",
            ));
        }
        if (self.read_buffer_size as u64) < 4096 {
            return Err(invalid("read_buffer_size", "must be at least 4096"));
        }
        if (self.read_buffer_size as u64) > self.buffer_pool_max_bytes {
            return Err(invalid(
                "buffer_pool_max_bytes",
                "must be >= read_buffer_size",
            ));
        }
        if self.write_budget.global_max_bytes == 0
            || self.write_budget.job_max_bytes == 0
            || self.write_budget.worker_read_ahead_bytes == 0
        {
            return Err(invalid("write_budget.*", "must be greater than zero"));
        }
        if self.write_budget.job_max_bytes > self.write_budget.global_max_bytes {
            return Err(invalid(
                "write_budget.job_max_bytes",
                "must be <= write_budget.global_max_bytes",
            ));
        }
        if self.global_rate_limit == Some(0) {
            return Err(invalid(
                "global_rate_limit",
                "use None for unlimited, not Some(0)",
            ));
        }
        if self.write_budget.worker_read_ahead_bytes > self.write_budget.job_max_bytes {
            return Err(invalid(
                "write_budget.worker_read_ahead_bytes",
                "must be <= write_budget.job_max_bytes",
            ));
        }
        if u64::from(self.read_buffer_size) > self.write_budget.worker_read_ahead_bytes {
            return Err(invalid(
                "write_budget.worker_read_ahead_bytes",
                "must be >= read_buffer_size (one frame quantum)",
            ));
        }
        if self.write_executor.writer_threads == 0 {
            return Err(invalid(
                "write_executor.writer_threads",
                "must be at least 1",
            ));
        }
        if u64::from(self.read_buffer_size) > self.write_executor.max_queued_bytes {
            return Err(invalid(
                "write_executor.max_queued_bytes",
                "must be >= read_buffer_size (one frame quantum)",
            ));
        }
        self.transfer_memory.validate(self)?;
        if self.checkpoint_flush_interval.is_zero() || self.metrics_interval.is_zero() {
            return Err(invalid(
                "checkpoint_flush_interval/metrics_interval",
                "must be greater than zero",
            ));
        }
        validate_transfer(&self.transfer)?;
        validate_retry(&self.retry)?;
        validate_network(&self.network)?;
        validate_pool(
            &self.pool,
            self.max_connections_total,
            self.max_connections_per_origin,
        )?;
        if let ProxyConfig::Http { url } | ProxyConfig::Socks5 { url } = &self.proxy {
            let ok = url.parse::<hyper::Uri>().is_ok()
                && hyper::Uri::try_from(url.as_str())
                    .ok()
                    .and_then(|u| u.authority().map(|a| a.port_u16().unwrap_or(0)))
                    .is_some_and(|p| p > 0);
            if !ok {
                return Err(invalid(
                    "proxy.url",
                    "must parse as a URI with host and port",
                ));
            }
        }
        match &self.h2_policy {
            H2ConnectionPolicy::Single => {}
            H2ConnectionPolicy::Additional { max_connections } => {
                if *max_connections == 0 {
                    return Err(invalid("h2_policy.max_connections", "must be at least 1"));
                }
            }
        }
        Ok(())
    }
}

impl IntegrityPolicy {
    /// Validate hash expectations: hex length must match the algorithm and
    /// contain only hex digits.
    ///
    /// # Errors
    /// Returns a [`ConfigurationError`] on the first malformed expectation.
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        for h in &self.expected_hashes {
            let want = h.algorithm.digest_len() * 2;
            if h.hex.len() != want || !h.hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(invalid(
                    "integrity.expected_hashes",
                    format!(
                        "{} digest must be {want} hex chars",
                        match h.algorithm {
                            HashAlgorithm::Sha256 => "sha256",
                            HashAlgorithm::Sha512 => "sha512",
                        }
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Convert a configuration failure into the engine error type.
impl From<ConfigurationError> for DownloadError {
    fn from(e: ConfigurationError) -> Self {
        DownloadError::Configuration(format!("{e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        EngineConfig::default().validate().expect("defaults valid");
        IntegrityPolicy::default()
            .validate()
            .expect("empty integrity valid");
    }

    #[test]
    fn min_workers_above_max_rejected() {
        let mut c = EngineConfig::default();
        c.transfer.min_workers = c.transfer.max_workers + 1;
        let err = c.validate().expect_err("must reject");
        assert_eq!(err.field, "transfer.min_workers");
    }

    #[test]
    fn zero_timeout_rejected() {
        let mut c = EngineConfig::default();
        c.network.connect_timeout = Duration::ZERO;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "network.connect_timeout"
        );
    }

    #[test]
    fn zero_max_workers_rejected() {
        let mut c = EngineConfig::default();
        c.transfer.max_workers = 0;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer.max_workers"
        );
    }

    #[test]
    fn zero_write_budget_rejected() {
        let mut c = EngineConfig::default();
        c.write_budget.global_max_bytes = 0;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.*"
        );
        let mut c = EngineConfig::default();
        c.write_budget.job_max_bytes = 0;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.*"
        );
        let mut c = EngineConfig::default();
        c.write_budget.worker_read_ahead_bytes = 0;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.*"
        );
    }

    #[test]
    fn write_budget_ordering_rejected() {
        // Job budget above the engine-global budget.
        let mut c = EngineConfig::default();
        c.write_budget.job_max_bytes = c.write_budget.global_max_bytes + 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.job_max_bytes"
        );

        // Worker read-ahead above the job budget.
        let mut c = EngineConfig::default();
        c.write_budget.worker_read_ahead_bytes = c.write_budget.job_max_bytes + 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.worker_read_ahead_bytes"
        );

        // Read-ahead below one frame quantum cannot ever admit a frame.
        let mut c = EngineConfig::default();
        c.write_budget.worker_read_ahead_bytes = u64::from(c.read_buffer_size) - 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.worker_read_ahead_bytes"
        );
    }

    #[test]
    fn segment_bounds_rejected() {
        let mut c = EngineConfig::default();
        c.transfer.initial_segment_size = c.transfer.min_segment_size - 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer.initial_segment_size"
        );
    }

    #[test]
    fn per_origin_above_total_rejected() {
        let mut c = EngineConfig::default();
        c.max_connections_per_origin = c.max_connections_total + 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "max_connections_per_origin"
        );
    }

    #[test]
    fn bad_hash_expectation_rejected() {
        let p = IntegrityPolicy {
            expected_hashes: vec![ExpectedHash {
                algorithm: HashAlgorithm::Sha256,
                hex: "nothex".into(),
            }],
            apply_mtime: false,
        };
        assert_eq!(
            p.validate().expect_err("must reject").field,
            "integrity.expected_hashes"
        );
    }

    // ---- Transfer-memory budget validation (task 3.1) ----

    #[test]
    fn default_transfer_memory_is_valid() {
        EngineConfig::default()
            .transfer_memory
            .validate(&EngineConfig::default())
            .expect("default budget valid");
    }

    #[test]
    fn zero_transfer_memory_components_rejected() {
        let zero_fields = [
            ("aggregate_max_bytes", "transfer_memory.aggregate_max_bytes"),
            ("job_max_bytes", "transfer_memory.job_max_bytes"),
            (
                "network_ingress_max_bytes",
                "transfer_memory.network_ingress_max_bytes",
            ),
            ("frames_max_bytes", "transfer_memory.frames_max_bytes"),
            ("writer_max_bytes", "transfer_memory.writer_max_bytes"),
            (
                "checkpoint_max_bytes",
                "transfer_memory.checkpoint_max_bytes",
            ),
        ];
        for (name, field) in zero_fields {
            let mut c = EngineConfig::default();
            match name {
                "aggregate_max_bytes" => c.transfer_memory.aggregate_max_bytes = 0,
                "job_max_bytes" => c.transfer_memory.job_max_bytes = 0,
                "network_ingress_max_bytes" => c.transfer_memory.network_ingress_max_bytes = 0,
                "frames_max_bytes" => c.transfer_memory.frames_max_bytes = 0,
                "writer_max_bytes" => c.transfer_memory.writer_max_bytes = 0,
                "checkpoint_max_bytes" => c.transfer_memory.checkpoint_max_bytes = 0,
                _ => unreachable!(),
            }
            assert_eq!(
                c.validate().expect_err("zero budget must reject").field,
                field,
                "{name} = 0 must be rejected"
            );
        }
    }

    #[test]
    fn contradictory_budget_nesting_rejected() {
        // A component cap above its job cap.
        let mut c = EngineConfig::default();
        c.transfer_memory.frames_max_bytes = c.transfer_memory.job_max_bytes + 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer_memory.frames_max_bytes"
        );
        // A job cap above the aggregate cap.
        let mut c = EngineConfig::default();
        c.transfer_memory.job_max_bytes = c.transfer_memory.aggregate_max_bytes + 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer_memory.job_max_bytes"
        );
    }

    #[test]
    fn minimum_feasible_sizes_enforced() {
        // Ingress must admit at least one frame quantum.
        let mut c = EngineConfig::default();
        c.transfer_memory.network_ingress_max_bytes = u64::from(c.read_buffer_size) - 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer_memory.network_ingress_max_bytes"
        );
        // Frames must admit at least one owned frame.
        let mut c = EngineConfig::default();
        c.transfer_memory.frames_max_bytes = u64::from(c.read_buffer_size) - 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer_memory.frames_max_bytes"
        );
        // Checkpoint allowance must admit one minimal checkpoint.
        let mut c = EngineConfig::default();
        c.transfer_memory.checkpoint_max_bytes = MIN_CHECKPOINT_BYTES - 1;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer_memory.checkpoint_max_bytes"
        );
    }

    #[test]
    fn overflow_headroom_rejected() {
        let mut c = EngineConfig::default();
        c.transfer_memory.aggregate_max_bytes = u64::MAX;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "transfer_memory.aggregate_max_bytes"
        );
    }

    #[test]
    fn legacy_budgets_must_stay_subordinate() {
        const FRAME: u64 = 128 * 1024;

        // write_budget.job_max_bytes above the transfer-memory job cap.
        let mut c = EngineConfig::default();
        // Components must stay within the job cap while it shrinks.
        c.transfer_memory.network_ingress_max_bytes = FRAME;
        c.transfer_memory.frames_max_bytes = FRAME;
        c.transfer_memory.writer_max_bytes = FRAME;
        c.transfer_memory.checkpoint_max_bytes = FRAME;
        c.transfer_memory.job_max_bytes = 512 * 1024;
        c.write_budget.job_max_bytes = 1024 * 1024;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.job_max_bytes"
        );
        // write_budget.global_max_bytes above the aggregate cap.
        let mut c = EngineConfig::default();
        // Everything nests: components >= frame, job >= components,
        // aggregate >= job; only the legacy global cap contradicts.
        c.transfer_memory.network_ingress_max_bytes = FRAME;
        c.transfer_memory.frames_max_bytes = FRAME;
        c.transfer_memory.writer_max_bytes = FRAME;
        c.transfer_memory.checkpoint_max_bytes = FRAME;
        c.transfer_memory.job_max_bytes = 2 * FRAME;
        c.transfer_memory.aggregate_max_bytes = 2 * 1024 * 1024;
        // The connection carve-out must fit the aggregate first.
        c.max_connections_total = 2;
        c.max_connections_per_origin = 2;
        c.pool.max_total = 2;
        c.pool.max_per_origin = 2;
        c.write_budget.job_max_bytes = 2 * FRAME;
        c.write_budget.worker_read_ahead_bytes = FRAME;
        // The legacy global cap exceeds the (valid) aggregate: only this
        // subordination rule contradicts.
        c.write_budget.global_max_bytes = 4 * 1024 * 1024;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.global_max_bytes"
        );
        // worker read-ahead above the writer component cap.
        let mut c = EngineConfig::default();
        c.transfer_memory.writer_max_bytes = 256 * 1024;
        c.write_budget.worker_read_ahead_bytes = 512 * 1024;
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_budget.worker_read_ahead_bytes"
        );
        // write executor queue above the aggregate cap.
        let c = EngineConfig {
            // The whole budget set shrinks coherently first; only the
            // executor queue contradicts.
            transfer_memory: TransferMemoryConfig {
                network_ingress_max_bytes: FRAME,
                frames_max_bytes: FRAME,
                writer_max_bytes: FRAME,
                checkpoint_max_bytes: FRAME,
                job_max_bytes: FRAME,
                aggregate_max_bytes: 2 * 1024 * 1024,
            },
            max_connections_total: 2,
            max_connections_per_origin: 2,
            pool: PoolConfig {
                max_total: 2,
                max_per_origin: 2,
                ..PoolConfig::default()
            },
            write_budget: WriteBudgetConfig {
                job_max_bytes: FRAME,
                worker_read_ahead_bytes: FRAME,
                global_max_bytes: 2 * 1024 * 1024,
            },
            write_executor: WriteExecutorConfig {
                max_queued_bytes: 4 * 1024 * 1024,
                ..WriteExecutorConfig::default()
            },
            ..EngineConfig::default()
        };
        assert_eq!(
            c.validate().expect_err("must reject").field,
            "write_executor.max_queued_bytes"
        );
    }

    #[test]
    fn invalid_budget_fails_before_any_network() {
        // Transport construction is the earliest network-adjacent step:
        // an invalid budget must fail it before any connection could exist.
        // (validate() is a pure function: no sockets, no threads.)
        let mut c = EngineConfig::default();
        c.transfer_memory.frames_max_bytes = 0;
        assert!(c.validate().is_err(), "invalid budget must fail validation");
    }
}
