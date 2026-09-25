//! Hierarchical token-bucket rate limiting (§18, D11).
//!
//! Limits apply to payload bytes read from the network. A job limiter is
//! shared by its workers; a global limiter sits above all jobs. Unlimited
//! mode bypasses accounting entirely (near-zero overhead, §18.2).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Token bucket for one hierarchy level.
#[derive(Debug)]
pub struct TokenBucket {
    state: Mutex<BucketState>,
    /// Configured limit in bytes/second; 0 = unlimited.
    limit: AtomicU64,
    /// Burst capacity in bytes (≈250 ms of rate, §18.3).
    burst: AtomicU64,
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

/// Result of acquiring tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Acquisition {
    /// Wait time before consuming `bytes` is allowed.
    pub wait: Option<std::time::Duration>,
}

impl TokenBucket {
    /// Create a bucket limited to `bytes_per_second` (0 = unlimited).
    #[must_use]
    pub fn new(bytes_per_second: u64) -> Self {
        Self::with_burst(bytes_per_second, default_burst(bytes_per_second))
    }

    /// Create with an explicit burst size in bytes.
    #[must_use]
    pub fn with_burst(bytes_per_second: u64, burst_bytes: u64) -> Self {
        Self {
            state: Mutex::new(BucketState {
                tokens: burst_bytes as f64,
                last_refill: Instant::now(),
            }),
            limit: AtomicU64::new(bytes_per_second),
            burst: AtomicU64::new(burst_bytes),
        }
    }

    /// Unlimited when limit is 0 (§18.2).
    #[must_use]
    pub fn is_unlimited(&self) -> bool {
        self.limit.load(Ordering::Relaxed) == 0
    }

    /// Change the limit at runtime (§18.2); takes effect on the next
    /// acquire. Burst follows the new rate.
    pub fn set_rate(&self, bytes_per_second: u64) {
        self.limit.store(bytes_per_second, Ordering::Relaxed);
        self.burst
            .store(default_burst(bytes_per_second), Ordering::Relaxed);
    }

    #[must_use]
    pub fn rate(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Account for `bytes` payload; returns the wait required before the
    /// transfer may proceed (None when immediately allowed).
    pub fn acquire(&self, bytes: u64) -> Acquisition {
        if self.is_unlimited() {
            return Acquisition { wait: None };
        }
        let rate = self.rate() as f64;
        if rate <= 0.0 {
            return Acquisition { wait: None };
        }
        let burst = self.burst.load(Ordering::Relaxed) as f64;
        let mut st = self.state.lock().expect("bucket lock");
        refill(&mut st, rate, burst);
        // Negative-balance accounting: the balance may go below
        // zero by the requested bytes; the caller sleeps the deficit off
        // while refill climbs back. Keeping the debit inside the balance
        // (instead of zeroing it and leaving the wait's accrual for the
        // next caller) prevents double-spending — the previous behavior
        // delivered ~2× the configured rate in the deficit regime.
        st.tokens -= bytes as f64;
        if st.tokens >= 0.0 {
            return Acquisition { wait: None };
        }
        let wait_secs = -st.tokens / rate;
        Acquisition {
            wait: Some(std::time::Duration::from_secs_f64(wait_secs)),
        }
    }

    /// Reserve and asynchronously wait for the tokens.
    pub async fn acquire_async(&self, bytes: u64) {
        match self.acquire(bytes) {
            Acquisition { wait: None } => {}
            Acquisition { wait: Some(d) } => tokio::time::sleep(d).await,
        }
    }
}

/// Burst ≈ 250 ms of the configured rate, bounded to avoid unbounded
/// accumulation on rate changes (§18.3). Never exceeds a quarter-second's
/// worth of the rate itself, so tiny rates stay tiny.
fn default_burst(bytes_per_second: u64) -> u64 {
    let quarter = bytes_per_second / 4;
    if quarter == 0 {
        bytes_per_second.max(1) // very slow rates: burst is at least one byte's worth
    } else {
        quarter
    }
}

fn refill(st: &mut BucketState, rate: f64, burst: f64) {
    let now = Instant::now();
    let elapsed = now.duration_since(st.last_refill).as_secs_f64();
    if elapsed > 0.0 {
        st.tokens = (st.tokens + elapsed * rate).min(burst);
        st.last_refill = now;
    }
}

/// Hierarchical limiter: global -> per-job -> worker consumption (§18.1).
#[derive(Debug)]
pub struct RateLimiter {
    global: Option<Arc<TokenBucket>>,
    job: Option<Arc<TokenBucket>>,
}

impl RateLimiter {
    #[must_use]
    pub fn unlimited() -> Self {
        Self {
            global: None,
            job: None,
        }
    }

    #[must_use]
    pub fn with_global(bucket: Arc<TokenBucket>) -> Self {
        Self {
            global: Some(bucket),
            job: None,
        }
    }

    /// Attach a job-level bucket.
    #[must_use]
    pub fn with_job(mut self, bucket: Arc<TokenBucket>) -> Self {
        self.job = Some(bucket);
        self
    }

    /// Replace the job-level bucket at runtime (§18.2).
    pub fn set_job_bucket(&mut self, bucket: Option<Arc<TokenBucket>>) {
        self.job = bucket;
    }

    /// Change the job rate at runtime.
    pub fn set_job_rate(&self, bytes_per_second: u64) {
        if let Some(job) = &self.job {
            job.set_rate(bytes_per_second);
        }
    }

    /// Account payload bytes through every level; the slowest level's wait
    /// governs (see `dominant_wait` in this module). Each applicable bucket
    /// accounts for the bytes exactly once per call, so later callers wait
    /// for the balance the earlier callers left behind.
    pub fn acquire(&self, bytes: u64) -> Acquisition {
        let global_wait = self
            .global
            .as_ref()
            .map(|bucket| bucket.acquire(bytes).wait);
        let job_wait = self.job.as_ref().map(|bucket| bucket.acquire(bytes).wait);
        Acquisition {
            wait: dominant_wait([global_wait.unwrap_or(None), job_wait.unwrap_or(None)]),
        }
    }

    pub async fn acquire_async(&self, bytes: u64) {
        if let Some(a) = self.acquire(bytes).wait {
            tokio::time::sleep(a).await;
        }
    }
}

/// The effective wait when several buckets gate the same payload: the
/// slowest (largest) required delay governs, and unlimited/absent buckets
/// contribute nothing. Evaluation order must never change the result. Both
/// transfer paths and the hierarchical limiter share this function so they
/// cannot silently disagree on which wait wins.
pub(crate) fn dominant_wait(
    waits: [Option<std::time::Duration>; 2],
) -> Option<std::time::Duration> {
    match waits {
        [Some(a), Some(b)] => Some(a.max(b)),
        [Some(a), None] => Some(a),
        [None, b] => b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Order-independence and max semantics of the shared wait combiner,
    /// independent of any wall clock.
    #[test]
    fn dominant_wait_takes_the_maximum() {
        let d = |ms: u64| Some(std::time::Duration::from_millis(ms));
        assert_eq!(dominant_wait([d(2_000), d(1_000)]), d(2_000));
        assert_eq!(dominant_wait([d(1_000), d(2_000)]), d(2_000));
        assert_eq!(dominant_wait([d(1_000), d(1_000)]), d(1_000));
        assert_eq!(dominant_wait([d(1_000), None]), d(1_000));
        assert_eq!(dominant_wait([None, d(1_000)]), d(1_000));
        assert_eq!(dominant_wait([None, None]), None);
    }

    #[test]
    fn hierarchy_global_slower_than_job() {
        // Zero burst: every acquire waits for the full debit at the rate.
        let global = Arc::new(TokenBucket::with_burst(1_000, 0));
        let job = Arc::new(TokenBucket::with_burst(1_000_000, 0));
        let limiter = RateLimiter::with_global(global).with_job(job);
        let a = limiter.acquire(1_000);
        let wait = a.wait.expect("global limit must gate");
        assert!(
            wait >= Duration::from_millis(900) && wait <= Duration::from_secs(1),
            "slowest (global) wait must govern: {wait:?}"
        );
    }

    #[test]
    fn hierarchy_job_slower_than_global() {
        let global = Arc::new(TokenBucket::with_burst(1_000_000, 0));
        let job = Arc::new(TokenBucket::with_burst(1_000, 0));
        let limiter = RateLimiter::with_global(global).with_job(job);
        let a = limiter.acquire(1_000);
        let wait = a.wait.expect("job limit must gate");
        assert!(
            wait >= Duration::from_millis(900) && wait <= Duration::from_secs(1),
            "slowest (job) wait must govern: {wait:?}"
        );
    }

    #[test]
    fn hierarchy_equal_waits_and_single_debit_per_bucket() {
        let global = Arc::new(TokenBucket::with_burst(1_000, 0));
        let job = Arc::new(TokenBucket::with_burst(1_000, 0));
        let limiter = RateLimiter::with_global(global).with_job(job);
        let first = limiter.acquire(1_000).wait.expect("must wait");
        assert!(
            first >= Duration::from_millis(900) && first <= Duration::from_secs(1),
            "equal waits must combine to that wait: {first:?}"
        );
        // Each bucket debited the bytes exactly once: the next identical
        // acquire owes ~2 s, not ~3 s (double debit) or ~1 s (missed debit).
        let second = limiter.acquire(1_000).wait.expect("must wait");
        assert!(
            second >= Duration::from_millis(1_900) && second <= Duration::from_millis(2_100),
            "second acquire must reflect one debit per bucket: {second:?}"
        );
    }

    #[test]
    fn hierarchy_only_one_level_active() {
        let only_global = RateLimiter::with_global(Arc::new(TokenBucket::with_burst(1_000, 0)));
        let wait = only_global.acquire(1_000).wait.expect("global must gate");
        assert!(wait >= Duration::from_millis(900), "{wait:?}");

        let only_job =
            RateLimiter::unlimited().with_job(Arc::new(TokenBucket::with_burst(1_000, 0)));
        let wait = only_job.acquire(1_000).wait.expect("job must gate");
        assert!(wait >= Duration::from_millis(900), "{wait:?}");
    }

    #[test]
    fn hierarchy_both_unlimited_never_waits() {
        let limiter = RateLimiter::with_global(Arc::new(TokenBucket::new(0)))
            .with_job(Arc::new(TokenBucket::new(0)));
        assert_eq!(limiter.acquire(u64::MAX / 2).wait, None);
    }

    #[test]
    fn unlimited_has_zero_overhead_path() {
        let b = TokenBucket::new(0);
        assert!(b.is_unlimited());
        assert_eq!(b.acquire(10 * 1024 * 1024).wait, None);
    }

    #[test]
    fn burst_allows_initial_chunk() {
        // 1 MiB/s -> 256 KiB burst; acquiring 128 KiB is immediate.
        let b = TokenBucket::new(1024 * 1024);
        assert_eq!(b.acquire(128 * 1024).wait, None);
    }

    #[test]
    fn exceeding_burst_requires_wait() {
        let b = TokenBucket::new(1024 * 1024); // 1 MiB/s, 256 KiB burst
        let a = b.acquire(1024 * 1024); // more than burst
        let wait = a.wait.expect("must wait");
        assert!(wait.as_secs_f64() >= 0.5, "deficit wait scales with rate");
        assert!(wait.as_secs_f64() <= 1.5, "wait bounded by rate");
    }

    #[test]
    fn token_accounting_refills_over_time() {
        let b = TokenBucket::with_burst(1000, 1000);
        let _ = b.acquire(1000); // drain
        let a = b.acquire(500);
        // At 1000 B/s, refilling 500 tokens takes ~0.5 s (already elapsed
        // time since creation also helps, so wait <= 0.5s here).
        if let Some(w) = a.wait {
            assert!(w <= std::time::Duration::from_millis(600));
        }
    }

    #[test]
    fn runtime_rate_change_takes_effect() {
        let b = TokenBucket::new(1000);
        b.set_rate(0);
        assert!(b.is_unlimited());
        assert_eq!(b.acquire(u64::MAX / 2).wait, None, "unlimited after set 0");
        b.set_rate(1000);
        assert!(!b.is_unlimited());
    }

    #[test]
    fn hierarchy_takes_max_wait() {
        // Global is fast, job is slow: the job wait must dominate.
        let global = Arc::new(TokenBucket::new(10 * 1024 * 1024));
        let job = Arc::new(TokenBucket::new(1000));
        let limiter = RateLimiter::with_global(global).with_job(job);
        let a = limiter.acquire(10_000);
        assert!(a.wait.is_some(), "job-level limit must gate");
    }

    #[test]
    fn unlimited_limiter_never_waits() {
        let limiter = RateLimiter::unlimited();
        assert_eq!(limiter.acquire(u64::MAX / 2).wait, None);
    }

    /// Contended limited-mode probe (run explicitly
    /// with `cargo test -p kdown-engine --lib -- --ignored --nocapture`).
    /// Measures ns/acquire for N workers hammering a shared limited bucket
    /// with 64 KiB acquires — the segmented worker pattern (one acquire per
    /// received chunk). Evidence only; asserts nothing timing-tight.
    #[test]
    #[ignore]
    fn probe_limited_mode_lock_contention() {
        const WORKERS: usize = 16;
        const ACQUIRES_PER_WORKER: u64 = 40_000;
        const CHUNK: u64 = 64 * 1024;
        // Limited (the contended path) and unlimited (the fast path).
        for rate in [0u64, 64 * 1024 * 1024 * 1024] {
            let bucket = Arc::new(TokenBucket::new(rate));
            let start = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..WORKERS {
                    scope.spawn(|| {
                        for _ in 0..ACQUIRES_PER_WORKER {
                            let _ = bucket.acquire(CHUNK);
                        }
                    });
                }
            });
            let total = WORKERS as u64 * ACQUIRES_PER_WORKER;
            let elapsed = start.elapsed();
            println!(
                "rate={rate:>12} ns/acquire={} ({total} acquires, {elapsed:?})",
                elapsed.as_nanos() as u64 / total
            );
        }
    }

    #[tokio::test]
    async fn acquire_async_sleeps_expected_time() {
        let b = TokenBucket::with_burst(0, 0); // zero burst: everything waits
        b.set_rate(10_000); // 10 KiB/s
        let start = Instant::now();
        b.acquire_async(10_000).await; // full second's worth
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(900),
            "{elapsed:?}"
        );
    }
}
