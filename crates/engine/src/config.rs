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

/// Range-worker concurrency mode (task 9.1): `Fixed` (default) keeps the
/// configured fixed concurrency; `Adaptive` opts into the conservative
/// goodput-driven controller starting at `min_workers`. Manual runtime
/// control remains supported in both modes and suspends the controller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConcurrencyMode {
    /// Fixed concurrency (default; unchanged behavior).
    #[default]
    Fixed,
    /// Opt-in conservative adaptive range concurrency (design D6).
    Adaptive,
}

/// How initial segmented lease sizes are chosen (task 6.1).
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
    /// Initial-lease sizing selector (task 6.1): `Explicit` (default) honors
    /// `initial_segment_size`; `Automatic` opts into the derived target.
    pub segment_sizing: SegmentSizing,
    /// Oversubscription factor for `SegmentSizing::Automatic` (task 6.1);
    /// initial candidate 3, tuned from benchmarks.
    pub auto_oversubscription: u64,
    /// Range-worker concurrency mode (task 9.1): `Fixed` (default) or
    /// opt-in `Adaptive` (starts at `min_workers`; manual control wins).
    pub concurrency_mode: ConcurrencyMode,
    pub segmentation_threshold: u64,
    pub preallocate_output: bool,
    /// Opt-in physical space reservation at output preparation (task 11.1):
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

/// Connection pooling shape (§27, task 6.1).
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

/// How many additional HTTP/2 connections a segmented job may open to one
/// origin beyond the first multiplexed connection (§24, D5, task 6.2).
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
#[derive(Debug, Clone, PartialEq, Default)]
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
    /// Bounds the public `BufferPool`'s own allocation ONLY (task 10.2):
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
}
