//! Atomic per-worker counters folded into job-level snapshots (§19, D12).
//!
//! Workers update their own [`WorkerCounters`] with atomics (no scheduler
//! lock on the chunk path, §13.3); a fold task combines them periodically.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Hot-path counters owned by one worker. All atomics; never locked.
#[derive(Debug, Default)]
pub struct WorkerCounters {
    /// Payload bytes read from the network (retries inflate this).
    pub network_bytes: AtomicU64,
    /// Unique file bytes durably completed (retries never inflate this,
    /// §19.1 — workers only add bytes newly acknowledged by the sink).
    pub completed_bytes: AtomicU64,
    /// Bytes skipped because a checkpoint already had them.
    pub reused_bytes: AtomicU64,
    pub retries: AtomicU64,
    /// Bytes re-transferred after failure (wasted network traffic).
    pub wasted_bytes: AtomicU64,
}

impl WorkerCounters {
    pub fn add_network(&self, n: u64) {
        self.network_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_completed(&self, n: u64) {
        self.completed_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_reused(&self, n: u64) {
        self.reused_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_retries(&self, n: u64) {
        self.retries.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_wasted(&self, n: u64) {
        self.wasted_bytes.fetch_add(n, Ordering::Relaxed);
    }
    fn snapshot(&self) -> RawCounters {
        RawCounters {
            network: self.network_bytes.load(Ordering::Relaxed),
            completed: self.completed_bytes.load(Ordering::Relaxed),
            reused: self.reused_bytes.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            wasted: self.wasted_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct RawCounters {
    network: u64,
    completed: u64,
    reused: u64,
    retries: u64,
    wasted: u64,
}

/// Aggregate job counters: owns one slot per worker.
#[derive(Debug)]
pub struct JobCounters {
    workers: Vec<WorkerCounters>,
    started: Instant,
}

use std::time::Instant;

impl JobCounters {
    #[must_use]
    pub fn new(worker_count: u32) -> Self {
        Self {
            workers: (0..worker_count)
                .map(|_| WorkerCounters::default())
                .collect(),
            started: Instant::now(),
        }
    }

    #[must_use]
    pub fn worker(&self, idx: usize) -> Option<&WorkerCounters> {
        self.workers.get(idx)
    }

    /// Fold all worker counters into a consistent snapshot (D12).
    #[must_use]
    pub fn fold(&self) -> ProgressSnapshot {
        let raw: RawCounters = self.workers.iter().map(WorkerCounters::snapshot).fold(
            RawCounters::default(),
            |acc, r| RawCounters {
                network: acc.network + r.network,
                completed: acc.completed + r.completed,
                reused: acc.reused + r.reused,
                retries: acc.retries + r.retries,
                wasted: acc.wasted + r.wasted,
            },
        );
        ProgressSnapshot {
            network_bytes: raw.network,
            completed_bytes: raw.completed,
            reused_bytes: raw.reused,
            retries: raw.retries,
            wasted_bytes: raw.wasted,
            elapsed: self.started.elapsed(),
        }
    }
}

/// Immutable folded view (§19.1): `completed_bytes` counts unique file
/// bytes only.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProgressSnapshot {
    pub network_bytes: u64,
    pub completed_bytes: u64,
    pub reused_bytes: u64,
    pub retries: u64,
    pub wasted_bytes: u64,
    pub elapsed: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_bytes_uniqueness_under_re_reads() {
        let counters = JobCounters::new(2);
        // Worker 0 reads 10 MiB but only the first 4 MiB is acknowledged.
        counters
            .worker(0)
            .expect("slot")
            .add_network(10 * 1024 * 1024);
        counters
            .worker(0)
            .expect("slot")
            .add_completed(4 * 1024 * 1024);
        counters
            .worker(0)
            .expect("slot")
            .add_wasted(6 * 1024 * 1024);
        // Retry re-reads 6 MiB, 6 MiB acknowledged: network grows, completed
        // only grows by the newly acknowledged 6 MiB (§19.1, invariant 5).
        counters
            .worker(0)
            .expect("slot")
            .add_network(6 * 1024 * 1024);
        counters
            .worker(0)
            .expect("slot")
            .add_completed(6 * 1024 * 1024);
        counters.worker(0).expect("slot").add_retries(1);
        let snap = counters.fold();
        assert_eq!(snap.network_bytes, 16 * 1024 * 1024);
        assert_eq!(snap.completed_bytes, 10 * 1024 * 1024);
        assert_eq!(snap.retries, 1);
        assert_eq!(snap.wasted_bytes, 6 * 1024 * 1024);
    }

    #[test]
    fn fold_across_workers_sums() {
        let counters = JobCounters::new(3);
        counters.worker(0).expect("slot").add_completed(100);
        counters.worker(1).expect("slot").add_completed(250);
        counters.worker(2).expect("slot").add_network(50);
        let snap = counters.fold();
        assert_eq!(snap.completed_bytes, 350);
        assert_eq!(snap.network_bytes, 50);
    }
}
