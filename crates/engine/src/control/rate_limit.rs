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
        if st.tokens >= bytes as f64 {
            st.tokens -= bytes as f64;
            return Acquisition { wait: None };
        }
        // Deficit: time until the bucket refills enough.
        let deficit = bytes as f64 - st.tokens;
        st.tokens = 0.0;
        let wait_secs = deficit / rate;
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
    /// governs.
    pub fn acquire(&self, bytes: u64) -> Acquisition {
        let mut worst = Acquisition { wait: None };
        if let Some(g) = &self.global {
            let a = g.acquire(bytes);
            if a.wait.is_some() {
                worst = a;
            }
        }
        if let Some(j) = &self.job {
            let a = j.acquire(bytes);
            if a.wait.is_some() {
                worst = a;
            }
        }
        worst
    }

    pub async fn acquire_async(&self, bytes: u64) {
        if let Some(a) = self.acquire(bytes).wait {
            tokio::time::sleep(a).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
