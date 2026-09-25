//! Normalized final-origin identity and the controller-shared origin
//! registry (tasks 6.1-6.4, design D6).
//!
//! The registry coordinates *request admission* and *throttle feedback* for
//! jobs that share one final origin within one engine. It sits ABOVE the
//! transport's physical connection permits (`http::connect::ConnectionLimits`
//! stays authoritative for sockets, keyed by scheme/host/port/proxy/TLS
//! identity): an origin request slot is acquired before network dispatch and
//! released by RAII on success, failure or cancellation, while the connector's
//! physical permit is only taken when a socket is actually created.
//!
//! The canonical key is the NORMALIZED FINAL ORIGIN: lowercased scheme +
//! case-folded host (trailing dot stripped) + effective port (explicit or
//! scheme default), with any userinfo stripped. It is deliberately distinct
//! from the connector's physical pool key, which includes the proxy address —
//! congestion feedback follows the resource's final origin even when
//! connections are pooled or proxied differently.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::control::CancellationToken;
use crate::error::DownloadError;

/// Normalize a URL to its final-origin identity (task 6.1):
/// `scheme://host:effective-port`, lowercased, trailing dot stripped, any
/// `user:pass@` userinfo removed.
///
/// - Scheme and host are case-folded (`HTTP://ExAmple.COM` ==
///   `https://example.com` modulo scheme).
/// - The EFFECTIVE port is always included: an explicit port wins, otherwise
///   the scheme default (http 80, https 443) — so `http://h:80` and `http://h`
///   share one origin while `http://h:8080` stays distinct.
/// - Userinfo never reaches the key (no credentials can leak into registry
///   state or logs through it).
/// - A trailing dot on the host (`example.com.`) is stripped; the empty
///   root zone and the plain name share one origin.
/// - IPv6 hosts keep their brackets (`[::1]`).
///
/// Non-ASCII (IDN) hosts are case-folded with Unicode simple lowercasing but
/// NOT converted to punycode: this engine passes the raw host to TLS and DNS
/// today, so byte-level equality after case folding is the identity that
/// matches actual connection behavior. `None` when the URL has no parseable
/// `scheme://authority` prefix.
#[must_use]
pub fn normalized_origin(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    if scheme.is_empty() || rest.is_empty() {
        return None;
    }
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        // Only HTTP-family schemes carry origin semantics here.
        return None;
    }
    // Authority ends at the first path/query/fragment separator.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    // Strip userinfo: everything up to the LAST '@' inside the authority.
    let host_port = match authority.rfind('@') {
        Some(at) => &authority[at + 1..],
        None => authority,
    };
    if host_port.is_empty() {
        return None;
    }
    let default_port: u16 = if scheme == "https" { 443 } else { 80 };
    let (host, port) = split_host_port(host_port, default_port)?;
    // Case-fold the host (ASCII fast path, Unicode lowercase otherwise) and
    // strip one trailing dot (the DNS root zone).
    let mut host = host.to_lowercase();
    if host.ends_with('.') {
        host.pop();
    }
    if host.is_empty() {
        return None;
    }
    Some(format!("{scheme}://{host}:{port}"))
}

/// Split `host[:port]` (IPv6 literals keep their brackets) into
/// `(host, effective_port)`; unparseable ports invalidate the authority.
fn split_host_port(host_port: &str, default_port: u16) -> Option<(String, u16)> {
    if host_port.starts_with('[') {
        // IPv6 literal: host is `[...]`, optionally followed by `:port`.
        let close = host_port.find(']')?;
        let host = &host_port[..=close];
        let after = &host_port[close + 1..];
        let port = if let Some(p) = after.strip_prefix(':') {
            p.parse::<u16>().ok()?
        } else if after.is_empty() {
            default_port
        } else {
            return None;
        };
        return Some((host.to_string(), port));
    }
    match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port.parse().ok()?;
            Some((host.to_string(), port))
        }
        None => Some((host_port.to_string(), default_port)),
    }
}

/// Per-origin registry entry: fair request slots, the shared throttle
/// deadline, and throttle/success event counts (the raw material of the
/// throttle ratio reported by the gate).
#[derive(Debug)]
struct OriginEntry {
    slots: Arc<tokio::sync::Semaphore>,
    capacity: u32,
    /// Shared throttle deadline in epoch milliseconds (0 = none). All peers
    /// of this origin wait until it passes before their next request.
    backoff_until_ms: AtomicU64,
    throttle_events: AtomicU64,
    success_events: AtomicU64,
    /// Last admission/feedback touch, for idle eviction (task 6.4).
    last_active_ms: AtomicU64,
}

impl OriginEntry {
    fn new(capacity: u32) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(capacity.max(1) as usize)),
            capacity: capacity.max(1),
            backoff_until_ms: AtomicU64::new(0),
            throttle_events: AtomicU64::new(0),
            success_events: AtomicU64::new(0),
            last_active_ms: AtomicU64::new(now_ms()),
        }
    }

    fn touch(&self) {
        self.last_active_ms.store(now_ms(), Ordering::Relaxed);
    }

    fn in_flight(&self) -> u32 {
        self.capacity
            .saturating_sub(self.slots.available_permits() as u32)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// RAII origin request permit (task 6.2): held from before network dispatch
/// until the response is fully consumed (or failed/cancelled); dropping it
/// releases the origin's request slot. Cancelled or failed jobs therefore
/// never retain origin capacity.
#[derive(Debug)]
pub struct OriginPermit {
    /// Keeps the entry (and its semaphore) alive while the permit is held.
    #[allow(dead_code)] // retained for the drop-time release ordering
    entry: Arc<OriginEntry>,
    /// `None` only in the disabled (no-coordination) registry, which never
    /// consumes slots.
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Controller-shared origin registry (design D6): one instance per engine
/// (`SingleStreamController`), shared by every job it starts. Entries are
/// created lazily per normalized final origin and bounded by an inactive-TTL
/// plus a size cap that never evicts entries with live permit holders
/// (task 6.4).
#[derive(Debug)]
pub struct OriginRegistry {
    entries: Mutex<HashMap<String, Arc<OriginEntry>>>,
    /// Concurrent request ceiling per origin (fair FIFO across jobs).
    capacity_per_origin: u32,
    /// Maximum retained idle entries.
    max_entries: usize,
    /// Idle entries older than this become evictable.
    idle_ttl: Duration,
    /// Test/diagnostic switch (task 6.5 "no-origin-feedback" variant): when
    /// set, admission never waits and feedback is ignored — the per-job
    /// `origin_backoff_until` gate alone remains active.
    disabled: bool,
}

impl Default for OriginRegistry {
    fn default() -> Self {
        // Provisional defaults (recorded in the phase-6 gate): a generous
        // per-origin request ceiling that never binds below the legal
        // `max_active_jobs × max_workers` request population, bounded
        // retained state, and a five-minute idle lifetime.
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity_per_origin: 256,
            max_entries: 1024,
            idle_ttl: Duration::from_secs(300),
            disabled: false,
        }
    }
}

impl OriginRegistry {
    /// The engine-shared registry with default limits.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Constructor with explicit limits (tests and diagnostics).
    #[must_use]
    pub fn with_limits(
        capacity_per_origin: u32,
        max_entries: usize,
        idle_ttl: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            capacity_per_origin: capacity_per_origin.max(1),
            max_entries: max_entries.max(1),
            idle_ttl,
            disabled: false,
        })
    }

    /// A registry that performs no coordination: admission always succeeds
    /// immediately and throttle feedback is ignored (task 6.5 fallback
    /// variant; the per-job backoff gate remains active).
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            capacity_per_origin: 1,
            max_entries: 1,
            idle_ttl: Duration::ZERO,
            disabled: true,
        })
    }

    /// Get or create the entry for `key`, touching its idle clock and
    /// opportunistically evicting expired idle entries.
    fn entry(&self, key: &str) -> Arc<OriginEntry> {
        let mut map = self.entries.lock().expect("registry entries");
        if let Some(e) = map.get(key) {
            e.touch();
            return e.clone();
        }
        // Opportunistic eviction keeps retained state bounded (task 6.4):
        // expired idle entries first, then the size cap.
        if map.len() >= self.max_entries {
            evict_idle_entries(&mut map, self.idle_ttl);
        }
        while map.len() >= self.max_entries {
            // The cap is hard but never evicts live entries: with every
            // entry holding permits, retention temporarily exceeds the cap
            // rather than stranding in-flight requests. Among inactive
            // candidates the LEAST RECENTLY touched entry goes first.
            let Some(victim) = map
                .iter()
                .filter(|(_, e)| e.in_flight() == 0)
                .min_by_key(|(_, e)| e.last_active_ms.load(Ordering::Relaxed))
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            map.remove(&victim);
        }
        let entry = Arc::new(OriginEntry::new(self.capacity_per_origin));
        map.insert(key.to_string(), entry.clone());
        entry
    }

    /// Admit one request to `key` (task 6.2): wait out the origin's shared
    /// throttle deadline (if any), then acquire one request slot. Fair FIFO
    /// across waiting jobs; both waits are cancellation-aware, and the
    /// returned permit releases its slot on drop — success, failure or
    /// cancellation all release capacity.
    ///
    /// # Errors
    /// [`DownloadError::Cancelled`] when the token fires while waiting.
    pub async fn admit(
        &self,
        key: &str,
        cancel: &CancellationToken,
    ) -> Result<OriginPermit, DownloadError> {
        let entry = self.entry(key);
        loop {
            // Shared throttle deadline (task 6.3): every peer of this origin
            // waits until the coordinated earliest-retry instant passes.
            if !self.disabled {
                let wait = {
                    let until_ms = entry.backoff_until_ms.load(Ordering::Acquire);
                    deadline_wait(Duration::from_millis(until_ms.saturating_sub(now_ms())))
                };
                if wait > Duration::ZERO {
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = cancel.cancelled() => {
                            return Err(DownloadError::Cancelled);
                        }
                    }
                    continue;
                }
            }
            let permit = if self.disabled {
                // No coordination: admit without consuming a slot.
                Some(None)
            } else {
                tokio::select! {
                    permit = entry.slots.clone().acquire_owned() => {
                        Some(Some(permit.expect("origin semaphore open")))
                    }
                    _ = cancel.cancelled() => None,
                }
            };
            match permit {
                Some(p) => {
                    entry.touch();
                    return Ok(OriginPermit {
                        entry: entry.clone(),
                        _permit: p,
                    });
                }
                None => {
                    if cancel.is_cancelled() {
                        return Err(DownloadError::Cancelled);
                    }
                    // Slot contended: the select! branch lost acquisition;
                    // retry (the semaphore queue is fair FIFO).
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                        _ = cancel.cancelled() => {
                            return Err(DownloadError::Cancelled);
                        }
                    }
                }
            }
        }
    }

    /// Record a throttle response (429/503) observed by ANY job against this
    /// origin (task 6.3): the shared deadline extends to
    /// `now + min(retry_after, retry_after_max)` — the caller passes the
    /// `RetryClassifier`-capped value — or `now + fallback_delay` when the
    /// server sent no usable Retry-After. Peers' next requests wait out the
    /// deadline; recovery after cooldown is a plain probe (admission
    /// resumes), and per-job retry limits remain the bound on retries.
    pub fn report_throttle(
        &self,
        key: &str,
        capped_retry_after: Option<Duration>,
        fallback_delay: Duration,
    ) {
        if self.disabled {
            return;
        }
        let entry = self.entry(key);
        entry.throttle_events.fetch_add(1, Ordering::Relaxed);
        let delay = capped_retry_after.unwrap_or(fallback_delay);
        let until = now_ms().saturating_add(delay.as_millis() as u64);
        // Extend-only: an earlier deadline is never shortened by a new
        // throttle (the longest server-provided window wins).
        let mut current = entry.backoff_until_ms.load(Ordering::Acquire);
        loop {
            if current >= until {
                break;
            }
            match entry.backoff_until_ms.compare_exchange(
                current,
                until,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record one successfully completed request against `key` (task 6.3
    /// recovery accounting): successes are the probe signal that the origin
    /// recovered after its cooldown.
    pub fn report_success(&self, key: &str) {
        if self.disabled {
            return;
        }
        let entry = self.entry(key);
        entry.success_events.fetch_add(1, Ordering::Relaxed);
        entry.touch();
    }

    /// Remaining shared-backoff wait for `key` (`None` when clear).
    #[must_use]
    pub fn backoff_remaining(&self, key: &str) -> Option<Duration> {
        let map = self.entries.lock().expect("registry entries");
        let entry = map.get(key)?;
        let wait = deadline_wait(Duration::from_millis(
            entry
                .backoff_until_ms
                .load(Ordering::Acquire)
                .saturating_sub(now_ms()),
        ));
        if wait.is_zero() {
            None
        } else {
            Some(wait)
        }
    }

    /// In-flight (slot-held) request count for `key` (diagnostics/tests).
    #[must_use]
    pub fn in_flight(&self, key: &str) -> u32 {
        let map = self.entries.lock().expect("registry entries");
        map.get(key).map_or(0, |e| e.in_flight())
    }

    /// Retained entry count (diagnostics/tests).
    #[must_use]
    pub fn live_entries(&self) -> usize {
        self.entries.lock().expect("registry entries").len()
    }

    /// Throttle/success event counts for `key` (task 6.5 traces).
    #[must_use]
    pub fn throttle_trace(&self, key: &str) -> (u64, u64) {
        let map = self.entries.lock().expect("registry entries");
        match map.get(key) {
            Some(e) => (
                e.throttle_events.load(Ordering::Relaxed),
                e.success_events.load(Ordering::Relaxed),
            ),
            None => (0, 0),
        }
    }

    /// Evict entries idle beyond the TTL that hold no permits (task 6.4);
    /// returns the number evicted. Live permit holders are never evicted.
    pub fn evict_idle(&self) -> usize {
        let mut map = self.entries.lock().expect("registry entries");
        evict_idle_entries(&mut map, self.idle_ttl)
    }
}

fn deadline_wait(remaining: Duration) -> Duration {
    if remaining.is_zero() {
        Duration::ZERO
    } else {
        remaining
    }
}

fn evict_idle_entries(map: &mut HashMap<String, Arc<OriginEntry>>, idle_ttl: Duration) -> usize {
    let cutoff = now_ms().saturating_sub(idle_ttl.as_millis() as u64);
    let before = map.len();
    map.retain(|_, e| e.in_flight() > 0 || e.last_active_ms.load(Ordering::Relaxed) > cutoff);
    before - map.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_case_and_default_ports() {
        assert_eq!(
            normalized_origin("HTTP://ExAmple.COM:80/path?q=1"),
            normalized_origin("http://example.com/")
        );
        assert_eq!(
            normalized_origin("HTTPS://EXAMPLE.COM"),
            Some("https://example.com:443".to_string())
        );
        // Explicit non-default ports stay distinct from the default.
        assert_ne!(
            normalized_origin("http://example.com:8080/x"),
            normalized_origin("http://example.com/x")
        );
        // Scheme + effective port both participate: same host on http vs
        // https is a different origin.
        assert_ne!(
            normalized_origin("http://example.com:443"),
            normalized_origin("https://example.com:443")
        );
    }

    #[test]
    fn strips_credentials_and_trailing_dot() {
        let credentialed = normalized_origin("https://alice:s3cret@Example.COM./x");
        let plain = normalized_origin("https://example.com/x");
        assert_eq!(credentialed, plain);
        let key = credentialed.expect("normalized");
        assert!(
            !key.contains("alice"),
            "userinfo must not reach the key: {key}"
        );
        assert!(
            !key.contains("s3cret"),
            "userinfo must not reach the key: {key}"
        );
    }

    #[test]
    fn ipv6_and_invalid_inputs() {
        assert_eq!(
            normalized_origin("http://[::1]:8080/x"),
            Some("http://[::1]:8080".to_string())
        );
        assert_eq!(
            normalized_origin("http://[::1]"),
            Some("http://[::1]:80".to_string())
        );
        assert_eq!(normalized_origin("ftp://example.com"), None);
        assert_eq!(normalized_origin("example.com/x"), None);
        assert_eq!(normalized_origin(""), None);
        assert_eq!(normalized_origin("http:///path"), None);
        assert_eq!(normalized_origin("http://host:notaport/"), None);
    }

    #[test]
    fn redirect_changes_the_origin_identity() {
        // A cross-origin redirect changes the origin: feedback keyed by the
        // pre-redirect URL would coordinate the wrong hosts. The engine keys
        // admission and throttle feedback on the FINAL origin
        // (ProbeMetadata::final_url), so the identity function must reflect
        // that distinction.
        let before = normalized_origin("https://origin.test/file").expect("before");
        let after = normalized_origin("https://cdn.example.net/file").expect("after");
        assert_ne!(before, after);
        // Same-origin redirects keep one identity.
        assert_eq!(
            normalized_origin("https://origin.test/a").expect("a"),
            normalized_origin("https://ORIGIN.test:443/b").expect("b")
        );
    }

    #[test]
    fn origin_key_is_distinct_from_connector_pool_key() {
        // The registry key is the plain final origin; the connector's
        // physical pool key embeds the proxy (http::connect::origin_key).
        let registry_key = normalized_origin("https://example.com/x").expect("origin");
        let proxied_pool_key =
            crate::http::connect::origin_key("https", "example.com", 443, Some("proxy:3128"));
        let direct_pool_key = crate::http::connect::origin_key("https", "example.com", 443, None);
        assert_ne!(registry_key, proxied_pool_key);
        assert_ne!(proxied_pool_key, direct_pool_key);
        // Same origin, different physical pool identities.
        assert_eq!(
            registry_key,
            normalized_origin("https://EXAMPLE.com:443/y").expect("origin")
        );
    }

    #[tokio::test]
    async fn permit_releases_on_drop_and_cancel_releases_capacity() {
        let registry = OriginRegistry::with_limits(2, 64, Duration::from_secs(60));
        let key = normalized_origin("http://origin.test/f").expect("origin");
        let cancel = CancellationToken::new();
        let p1 = registry.admit(&key, &cancel).await.expect("admit 1");
        let p2 = registry.admit(&key, &cancel).await.expect("admit 2");
        assert_eq!(registry.in_flight(&key), 2);
        // Third admission waits (capacity 2); cancellation releases nothing
        // because no permit was taken yet.
        let waiter = {
            let registry = registry.clone();
            let cancel = cancel.clone();
            let key = key.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                cancel.cancel();
                registry.admit(&key, &cancel).await
            })
        };
        let waited = waiter.await.expect("task").expect_err("cancelled");
        assert!(matches!(waited, DownloadError::Cancelled), "{waited:?}");
        assert_eq!(registry.in_flight(&key), 2);
        drop(p2);
        assert_eq!(registry.in_flight(&key), 1);
        drop(p1);
        assert_eq!(registry.in_flight(&key), 0);
        // After release, admission succeeds again (fresh token: the test
        // cancelled the earlier one while exercising the waiting path).
        let fresh = CancellationToken::new();
        let _p3 = registry.admit(&key, &fresh).await.expect("admit 3");
        assert_eq!(registry.in_flight(&key), 1);
    }

    #[tokio::test]
    async fn stress_many_ephemeral_origins_keeps_state_bounded() {
        // Small cap + short TTL: hundreds of one-shot origins churn through
        // the registry; retained entries stay within the size cap and every
        // retained entry keeps exactly its configured slot capacity (no
        // leaked or negative capacity).
        let registry = OriginRegistry::with_limits(4, 32, Duration::from_millis(50));
        let cancel = CancellationToken::new();
        for i in 0..400u32 {
            let key = normalized_origin(&format!("http://ephemeral-{i}.test/f")).expect("origin");
            let permit = registry.admit(&key, &cancel).await.expect("admit");
            assert!(registry.in_flight(&key) <= 4);
            drop(permit);
            assert_eq!(registry.in_flight(&key), 0, "drop must release the slot");
        }
        assert!(
            registry.live_entries() <= 32,
            "retained entries must respect the size cap: {}",
            registry.live_entries()
        );
        assert!(registry.live_entries() >= 1);
        // The registry still admits after the churn.
        let key = normalized_origin("http://after-churn.test/f").expect("origin");
        let permit = registry.admit(&key, &cancel).await.expect("admit");
        assert_eq!(registry.in_flight(&key), 1);
        drop(permit);
        assert_eq!(registry.in_flight(&key), 0);
    }

    #[tokio::test]
    async fn ttl_eviction_never_evicts_live_permit_holders() {
        let registry = OriginRegistry::with_limits(4, 8, Duration::from_millis(60));
        let cancel = CancellationToken::new();
        let live_a = normalized_origin("http://live-a.test/f").expect("origin");
        let live_b = normalized_origin("http://live-b.test/f").expect("origin");
        let held_a = registry.admit(&live_a, &cancel).await.expect("admit a");
        let held_b = registry.admit(&live_b, &cancel).await.expect("admit b");

        // Age every entry past the TTL while the two permits stay held.
        tokio::time::sleep(Duration::from_millis(90)).await;

        // Churn enough new origins to force repeated eviction sweeps.
        for i in 0..40u32 {
            let key = normalized_origin(&format!("http://churn-{i}.test/f")).expect("origin");
            let permit = registry.admit(&key, &cancel).await.expect("admit");
            drop(permit);
        }

        // The live entries survived every sweep (their permits hold slots).
        assert_eq!(
            registry.in_flight(&live_a),
            1,
            "live entry must not be evicted"
        );
        assert_eq!(
            registry.in_flight(&live_b),
            1,
            "live entry must not be evicted"
        );
        assert!(
            registry.live_entries() <= 8 + 2,
            "cap plus the protected live entries: {}",
            registry.live_entries()
        );

        // Once released and aged, the same entries become evictable.
        drop(held_a);
        drop(held_b);
        tokio::time::sleep(Duration::from_millis(90)).await;
        for i in 0..40u32 {
            let key = normalized_origin(&format!("http://churn2-{i}.test/f")).expect("origin");
            let permit = registry.admit(&key, &cancel).await.expect("admit");
            drop(permit);
        }
        assert_eq!(
            registry.in_flight(&live_a),
            0,
            "a released, aged entry must be evictable (0 = gone or idle)"
        );
        // No negative or leaked capacity anywhere: every retained entry
        // reports at most its capacity in flight and admits cleanly.
        let key = normalized_origin("http://final.test/f").expect("origin");
        let permit = registry.admit(&key, &cancel).await.expect("admit");
        assert!(registry.in_flight(&key) <= 4);
        drop(permit);
    }

    #[tokio::test]
    async fn throttle_deadline_is_shared_across_keys_and_capped() {
        let registry = OriginRegistry::new();
        let key = normalized_origin("http://shared.test/f").expect("origin");
        let other = normalized_origin("http://other.test/f").expect("origin");
        // A 503 with Retry-After: 2 (capped by the caller via the
        // classifier) extends the shared deadline of THAT origin only.
        registry.report_throttle(&key, Some(Duration::from_secs(2)), Duration::from_secs(1));
        let remaining = registry.backoff_remaining(&key).expect("deadline");
        assert!(
            remaining > Duration::from_millis(1_500) && remaining <= Duration::from_secs(2),
            "shared deadline must reflect the capped Retry-After: {remaining:?}"
        );
        assert_eq!(
            registry.backoff_remaining(&other),
            None,
            "unrelated origin unaffected"
        );
        // A later throttle never shortens the window.
        registry.report_throttle(&key, Some(Duration::ZERO), Duration::ZERO);
        assert!(registry.backoff_remaining(&key).is_some());
        // Recovery probe: after the deadline the entry accepts admission.
        let cancel = CancellationToken::new();
        registry.report_success(&key);
        registry.report_success(&key);
        let (throttles, successes) = registry.throttle_trace(&key);
        assert_eq!((throttles, successes), (2, 2));
        let _permit = registry
            .admit(&key, &cancel)
            .await
            .expect("admit after deadline");
    }
}
