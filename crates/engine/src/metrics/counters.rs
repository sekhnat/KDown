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

impl ProgressSnapshot {
    /// Useful completed-byte goodput (§19.1): unique newly completed file
    /// bytes per second of elapsed time. Reused checkpoint bytes never
    /// inflate this number.
    #[must_use]
    pub fn useful_goodput_per_sec(&self) -> f64 {
        self.completed_bytes as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }

    /// Wire throughput: all network payload bytes (including retransmit
    /// overhead) per second of elapsed time.
    #[must_use]
    pub fn wire_throughput_per_sec(&self) -> f64 {
        self.network_bytes as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }

    /// Synthetic accounting check (observability spec): wire bytes bound
    /// both wasted retransmit overhead and unique completed bytes; reused
    /// checkpoint bytes are never also counted as network bytes.
    #[must_use]
    pub fn accounting_consistent(&self) -> bool {
        self.wasted_bytes <= self.network_bytes && self.completed_bytes <= self.network_bytes
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

    /// False-sharing probe (task 7.3, #[ignore]): N threads each hammering
    /// their OWN WorkerCounters (the real pattern — one worker, one cell)
    /// laid out adjacently (current) vs cache-line-padded. Evidence only.
    #[test]
    #[ignore]
    fn probe_worker_counters_false_sharing() {
        use std::sync::atomic::Ordering;
        use std::time::Instant;

        #[repr(align(64))]
        struct Padded(WorkerCounters);

        const WORKERS: usize = 16;
        const ITERS: u64 = 200_000;
        for padded in [false, true] {
            // Leak the storage: opaque addresses keep LLVM from eliding
            // the atomics after inlining the scoped threads. The padded arm
            // gives each worker its own line; the adjacent arm uses the
            // current production layout (Vec<WorkerCounters>, ~40 B stride).
            let padded_cells: &'static [Padded] = Box::leak(Box::new(
                (0..WORKERS)
                    .map(|_| Padded(WorkerCounters::default()))
                    .collect::<Vec<Padded>>(),
            ));
            let plain_cells: &'static [WorkerCounters] = Box::leak(Box::new(
                (0..WORKERS)
                    .map(|_| WorkerCounters::default())
                    .collect::<Vec<WorkerCounters>>(),
            ));
            let start = Instant::now();
            std::thread::scope(|scope| {
                for w in 0..WORKERS {
                    let cell: &WorkerCounters = if padded {
                        &padded_cells[w].0
                    } else {
                        &plain_cells[w]
                    };
                    scope.spawn(move || {
                        let mut sink: u64 = 0;
                        for i in 0..ITERS {
                            cell.add_network(64 * 1024);
                            if i % 8 == 0 {
                                cell.add_completed(64 * 1024);
                            }
                            sink ^= cell.network_bytes.load(Ordering::Relaxed);
                        }
                        std::hint::black_box(sink);
                    });
                }
            });
            let elapsed = start.elapsed();
            let total = (WORKERS as u64 * (ITERS + ITERS / 8)) as f64;
            println!(
                "padded={padded} ns/op={} ({elapsed:?} for {total:.0} ops)",
                elapsed.as_nanos() as u64 / total as u64
            );
        }
    }

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

/// Synthetic accounting (observability spec, resumed coverage):
/// reused checkpoint bytes count toward neither goodput nor wire
/// throughput.
#[test]
fn reused_checkpoint_bytes_do_not_inflate_throughput() {
    let snap = ProgressSnapshot {
        network_bytes: 4 * 1024 * 1024,
        completed_bytes: 4 * 1024 * 1024,
        reused_bytes: 12 * 1024 * 1024,
        retries: 0,
        wasted_bytes: 0,
        elapsed: Duration::from_secs(2),
    };
    assert!(snap.accounting_consistent());
    // Goodput counts only the 4 MiB newly transferred, never the
    // 12 MiB reused.
    assert_eq!(snap.useful_goodput_per_sec(), 2.0 * 1024.0 * 1024.0);
    assert_eq!(snap.wire_throughput_per_sec(), 2.0 * 1024.0 * 1024.0);
}

/// Synthetic accounting (retried transfer): retransmitted bytes
/// separate wire throughput from useful goodput.
#[test]
fn retransferred_bytes_separate_wire_from_useful() {
    let snap = ProgressSnapshot {
        network_bytes: 150,
        completed_bytes: 100,
        reused_bytes: 0,
        retries: 1,
        wasted_bytes: 50,
        elapsed: Duration::from_secs(2),
    };
    assert!(snap.accounting_consistent());
    assert_eq!(snap.useful_goodput_per_sec(), 50.0);
    assert_eq!(snap.wire_throughput_per_sec(), 75.0);
}

/// Synthetic accounting (failure fixture): a warning without any
/// retransferred payload must never be reported as bytes.
#[test]
fn warnings_are_not_bytes() {
    let counters = JobCounters::new(1);
    counters.worker(0).expect("slot").add_completed(100);
    counters.worker(0).expect("slot").add_network(100);
    let mut snap = counters.fold();
    snap.elapsed = Duration::from_secs(1);
    // Ten warnings, zero retransmitted bytes.
    let warnings = 10usize;
    assert_eq!(snap.wasted_bytes, 0);
    assert_ne!(warnings as u64, snap.wasted_bytes, "warnings are not bytes");
    assert_eq!(snap.useful_goodput_per_sec(), 100.0);
    assert!(snap.accounting_consistent());
}

/// Inconsistent synthetic snapshots (over-counting) are detectable.
#[test]
fn inconsistent_accounting_is_flagged() {
    let snap = ProgressSnapshot {
        network_bytes: 10,
        completed_bytes: 40,
        reused_bytes: 0,
        retries: 0,
        wasted_bytes: 0,
        elapsed: Duration::from_secs(1),
    };
    assert!(!snap.accounting_consistent());
    let wasted = ProgressSnapshot {
        network_bytes: 10,
        completed_bytes: 5,
        reused_bytes: 0,
        retries: 0,
        wasted_bytes: 20,
        elapsed: Duration::from_secs(1),
    };
    assert!(!wasted.accounting_consistent());
}
