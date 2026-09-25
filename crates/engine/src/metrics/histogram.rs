//! Lock-free bucketed histograms for adaptive-window instrumentation
//!.
//!
//! The controller needs stable window inputs instead of instantaneous
//! samples: writer acknowledgement-latency and outstanding-queue-depth
//! percentiles, recorded on the write path without a lock. Recording is one
//! relaxed atomic increment; percentiles are computed at window boundaries
//! from a bucket snapshot, and each window uses the bucket-wise delta
//! against the previous window's snapshot, so a window's p50/p95 describes
//! that window only.
//!
//! Percentile reporting is deliberately conservative where buckets are
//! coarse: a latency percentile reports the bucket's inclusive upper bound
//! (never below the true value — under-reporting latency could hide storage
//! pressure), while an overflow depth sample reports the last bucket index
//! and is documented as a lower bound.

use std::sync::atomic::{AtomicU64, Ordering};

/// Bucket count for both histogram kinds.
const BUCKETS: usize = 40;

/// Bucket-wise counter snapshot: the unit both histograms
/// expose for window deltas.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BucketSnapshot {
    buckets: Vec<u64>,
    total: u64,
}

impl BucketSnapshot {
    /// Bucket-wise delta against a later snapshot of the same histogram
    /// (saturating; counters are monotonic).
    #[must_use]
    pub(crate) fn delta(&self, later: &Self) -> Self {
        let mut buckets = Vec::with_capacity(BUCKETS);
        for index in 0..BUCKETS {
            let earlier = self.buckets.get(index).copied().unwrap_or(0);
            let now = later.buckets.get(index).copied().unwrap_or(0);
            buckets.push(now.saturating_sub(earlier));
        }
        Self {
            buckets,
            total: later.total.saturating_sub(self.total),
        }
    }

    /// Whether the snapshot observed no samples at all (test/diagnostic
    /// helper; the percentile accessors already report `None` when empty).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Sample count (test/diagnostic helper).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Index of the bucket holding the `p` quantile (`p` in `0..=1`).
    fn quantile_index(&self, p: f64) -> Option<usize> {
        if self.total == 0 {
            return None;
        }
        let clamped = p.clamp(0.0, 1.0);
        // Ceil so the reported quantile covers at least `p` of the samples.
        let threshold = (clamped * self.total as f64).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            if cumulative >= threshold {
                return Some(index);
            }
        }
        Some(BUCKETS - 1)
    }

    /// Latency percentile in microseconds: the bucket's inclusive
    /// upper bound, so the value never under-reports the true percentile.
    /// `None` when the window observed no acknowledgement.
    #[must_use]
    pub(crate) fn latency_percentile_us(&self, p: f64) -> Option<f64> {
        let index = self.quantile_index(p)?;
        if index == 0 {
            return Some(0.0);
        }
        let bound = (1u64 << index.min(63)) - 1;
        Some(bound as f64)
    }

    /// Queue-depth percentile: exact for every bucket except the
    /// overflow bucket, which reports `BUCKETS - 1` (a documented lower
    /// bound). `None` when the window observed no submission.
    #[must_use]
    pub(crate) fn depth_percentile(&self, p: f64) -> Option<f64> {
        let index = self.quantile_index(p)?;
        Some(index as f64)
    }
}

/// Log2-bucketed latency histogram in microseconds.
#[derive(Debug)]
pub(crate) struct LatencyHistogram {
    buckets: [AtomicU64; BUCKETS],
    total: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total: AtomicU64::new(0),
        }
    }

    /// Record one latency sample. Bucket `0` holds exact zero; bucket `i`
    /// (>= 1) holds `[2^(i-1), 2^i)` microseconds; the last bucket also
    /// collects overflow.
    pub(crate) fn record_us(&self, us: u64) {
        let index = if us == 0 {
            0
        } else {
            (us.ilog2() as usize + 1).min(BUCKETS - 1)
        };
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> BucketSnapshot {
        BucketSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            total: self.total.load(Ordering::Relaxed),
        }
    }

    /// Total samples recorded since construction.
    #[must_use]
    pub(crate) fn samples(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// Linear depth histogram: bucket `i` is exactly depth `i`; the
/// last bucket also collects overflow.
#[derive(Debug)]
pub(crate) struct DepthHistogram {
    buckets: [AtomicU64; BUCKETS],
    total: AtomicU64,
}

impl Default for DepthHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl DepthHistogram {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total: AtomicU64::new(0),
        }
    }

    /// Record one depth sample.
    pub(crate) fn record(&self, depth: u64) {
        let index = (depth as usize).min(BUCKETS - 1);
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> BucketSnapshot {
        BucketSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            total: self.total.load(Ordering::Relaxed),
        }
    }

    /// Total samples recorded since construction.
    #[must_use]
    pub(crate) fn samples(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_percentiles_are_monotonic_and_bounded() {
        let histogram = LatencyHistogram::new();
        for us in 1..=1000u64 {
            histogram.record_us(us);
        }
        let snapshot = histogram.snapshot();
        let p50 = snapshot.latency_percentile_us(0.50).expect("p50");
        let p95 = snapshot.latency_percentile_us(0.95).expect("p95");
        let p99 = snapshot.latency_percentile_us(0.99).expect("p99");
        assert!(p50 <= p95 && p95 <= p99, "{p50} {p95} {p99}");
        // Conservative: a reported latency never under-reports the sample.
        assert!(p50 >= 500.0, "p50 {p50} must cover the median sample");
        assert!(p99 >= 990.0, "p99 {p99} must cover the 99th sample");
        assert_eq!(snapshot.total(), 1000);
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn empty_snapshots_report_none() {
        let histogram = LatencyHistogram::new();
        let snapshot = histogram.snapshot();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.latency_percentile_us(0.5), None);
        let depth = DepthHistogram::new();
        assert_eq!(depth.snapshot().depth_percentile(0.5), None);
    }

    #[test]
    fn delta_snapshots_isolate_one_window() {
        let histogram = LatencyHistogram::new();
        for _ in 0..10 {
            histogram.record_us(1_000);
        }
        let before = histogram.snapshot();
        for _ in 0..10 {
            histogram.record_us(1_000_000);
        }
        let after = histogram.snapshot();
        let window = before.delta(&after);
        assert_eq!(window.total(), 10);
        let p50 = window.latency_percentile_us(0.5).expect("p50");
        // Only the slow window is described: ~1s, not the earlier 1ms.
        assert!(p50 > 100_000.0, "window p50 {p50}");
        // The previous window is unaffected.
        assert_eq!(before.total(), 10);
    }

    #[test]
    fn depth_buckets_are_exact_and_overflow_is_a_lower_bound() {
        let depth = DepthHistogram::new();
        depth.record(0);
        depth.record(2);
        depth.record(2);
        depth.record(7);
        let snapshot = depth.snapshot();
        assert_eq!(snapshot.depth_percentile(0.25), Some(0.0));
        assert_eq!(snapshot.depth_percentile(0.5), Some(2.0));
        assert_eq!(snapshot.depth_percentile(0.9), Some(7.0));
        assert_eq!(snapshot.total(), 4);
        depth.record(u64::MAX);
        let overflow = depth.snapshot();
        assert_eq!(overflow.depth_percentile(1.0), Some((BUCKETS - 1) as f64));
    }

    #[test]
    fn zero_latency_lands_in_the_zero_bucket() {
        let histogram = LatencyHistogram::new();
        histogram.record_us(0);
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.latency_percentile_us(0.5), Some(0.0));
    }
}
