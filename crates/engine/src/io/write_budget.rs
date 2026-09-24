// The budgets are defined and verified here; the segmented transfer paths
// reserve against them when the executor is integrated (tasks 2.5-2.7).
#![allow(dead_code)]
//! Outstanding-write byte budgets (design D2, task 2.2).
//!
//! Bounds the payload bytes the engine retains between network receipt and
//! write acknowledgement: one engine-global pool shared by every job, one
//! per-job pool, and a per-worker read-ahead cap enforced by the worker
//! loop (task 2.5). Capacity is counted in **bytes**, and permits cover
//! queued+executing payload: a reservation is taken BEFORE the next body
//! chunk is polled (pre-read reservation), reconciled to the actual frame
//! size after receipt, and released by RAII when the write completes,
//! fails or is discarded.
//!
//! Acquisition order is fixed (global pool first, then the job pool) with
//! rollback of partially acquired reservations, so concurrent jobs cannot
//! form an acquisition cycle (design D2: no head-of-line deadlock).
//! Waits are cancellation-aware by construction: dropping a pending
//! reservation future mutates nothing (a partially acquired state rolls
//! back through a drop guard).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;

use super::write_executor::JobId;

/// One byte-counted permit pool: capacity `max`, outstanding bytes tracked
/// atomically, waiters woken on every release. Pure counting — no payload
/// data is ever published through this counter, so relaxed ordering is
/// sufficient.
#[derive(Debug)]
pub(crate) struct OutstandingByteBudget {
    max: u64,
    outstanding: AtomicU64,
    released: Notify,
}

impl OutstandingByteBudget {
    #[must_use]
    pub(crate) fn new(max: u64) -> Self {
        Self {
            max,
            outstanding: AtomicU64::new(0),
            released: Notify::new(),
        }
    }

    /// Bytes currently held (queued+executing payload).
    #[must_use]
    pub(crate) fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// Acquire `bytes` without waiting: `Some(outstanding_after)` on
    /// success, `None` when the pool cannot admit the request right now.
    fn try_acquire(&self, bytes: u64) -> Option<u64> {
        let mut current = self.outstanding.load(Ordering::Relaxed);
        loop {
            let new = current.checked_add(bytes)?;
            if new > self.max {
                return None;
            }
            match self.outstanding.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(new),
                Err(observed) => current = observed,
            }
        }
    }

    /// Release `bytes` back to the pool and wake every waiter (a large
    /// release may satisfy several waiters; spurious wakeups re-check).
    pub(crate) fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let previous = self.outstanding.fetch_sub(bytes, Ordering::Relaxed);
        debug_assert!(previous >= bytes, "byte budget released more than it held");
        self.released.notify_waiters();
    }

    /// Await `bytes`, registering interest before checking so a concurrent
    /// release can never be slept through. Dropping the future before
    /// completion acquires nothing.
    pub(crate) async fn acquire(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        loop {
            let notified = self.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_acquire(bytes).is_some() {
                return;
            }
            notified.await;
        }
    }
}

/// Engine-wide budget set (design D2, task 2.2): one global pool plus the
/// configured per-job cap and per-worker read-ahead. Shared across every
/// job of one engine context; each job derives its own [`JobWriteBudget`].
#[derive(Debug, Clone)]
pub(crate) struct WriteBudgets {
    global: Arc<OutstandingByteBudget>,
    job_max_bytes: u64,
    worker_read_ahead_bytes: u64,
}

impl WriteBudgets {
    /// Build from validated configuration (construction validation lives in
    /// `EngineConfig::validate`, task 2.2: budgets admit at least one frame).
    #[must_use]
    pub(crate) fn new(
        global_max_bytes: u64,
        job_max_bytes: u64,
        worker_read_ahead_bytes: u64,
    ) -> Self {
        Self {
            global: Arc::new(OutstandingByteBudget::new(global_max_bytes)),
            job_max_bytes,
            worker_read_ahead_bytes,
        }
    }

    /// The engine-global pool (observability and tests).
    #[must_use]
    pub(crate) fn global_outstanding(&self) -> u64 {
        self.global.outstanding()
    }

    /// One job's budget handle: acquires the engine-global pool first, then
    /// the job pool — the consistent order every caller uses.
    #[must_use]
    pub(crate) fn job(&self, _job: JobId) -> JobWriteBudget {
        JobWriteBudget {
            global: Arc::clone(&self.global),
            job: Arc::new(OutstandingByteBudget::new(self.job_max_bytes)),
            worker_read_ahead_bytes: self.worker_read_ahead_bytes,
        }
    }
}

/// Per-job write budget (design D2): the job's workers reserve payload
/// bytes here before reading the next body chunk.
#[derive(Debug, Clone)]
pub(crate) struct JobWriteBudget {
    global: Arc<OutstandingByteBudget>,
    job: Arc<OutstandingByteBudget>,
    worker_read_ahead_bytes: u64,
}

impl JobWriteBudget {
    /// Per-worker read-ahead cap (worker-loop enforcement, task 2.5): the
    /// bytes one worker may hold submitted-but-unacknowledged.
    #[must_use]
    pub(crate) fn worker_read_ahead_bytes(&self) -> u64 {
        self.worker_read_ahead_bytes
    }

    /// The job pool's outstanding bytes (observability and tests).
    #[must_use]
    pub(crate) fn job_outstanding(&self) -> u64 {
        self.job.outstanding()
    }

    /// Reserve payload bytes for one write: acquires the engine-global
    /// pool, then the job pool (consistent order, no hold-and-wait cycle),
    /// returning a RAII reservation the caller reconciles to the actual
    /// frame size and drops on completion/failure/discard.
    ///
    /// Cancellation-aware: dropping the future mid-wait releases any
    /// partially acquired bytes.
    pub(crate) async fn reserve(&self, bytes: u64) -> ByteReservation {
        let mut reservation = ByteReservation {
            global: Arc::clone(&self.global),
            job: Arc::clone(&self.job),
            held: 0,
        };
        if bytes == 0 {
            return reservation;
        }
        // Global first (consistent order). If the job acquisition below is
        // cancelled, the guard releases the global bytes (rollback).
        self.global.acquire(bytes).await;
        let guard = RollbackGuard {
            budget: Arc::clone(&self.global),
            armed: bytes,
        };
        self.job.acquire(bytes).await;
        std::mem::forget(guard);
        reservation.held = bytes;
        reservation
    }
}

/// Releases the global acquisition if the job acquisition never completes
/// (cancellation or panic while waiting).
struct RollbackGuard {
    budget: Arc<OutstandingByteBudget>,
    /// Bytes to release on drop; disarmed (zeroed) after full acquisition.
    armed: u64,
}

impl Drop for RollbackGuard {
    fn drop(&mut self) {
        if self.armed > 0 {
            self.budget.release(self.armed);
        }
    }
}

/// RAII byte reservation for one write (design D2): covers the write's
/// queued+executing payload in BOTH pools. Reconciled to the actual frame
/// size after receipt; released on completion, failure or discard.
#[derive(Debug)]
pub(crate) struct ByteReservation {
    global: Arc<OutstandingByteBudget>,
    job: Arc<OutstandingByteBudget>,
    held: u64,
}

impl ByteReservation {
    /// Bytes currently held by this reservation.
    #[must_use]
    pub(crate) fn held(&self) -> u64 {
        self.held
    }

    /// Reconcile the reservation down to the actual received frame size,
    /// immediately releasing the excess (design D2: reconcile actual frame
    /// size after the pre-read reservation). Must not exceed the held
    /// bytes — an oversize frame grows the reservation first.
    ///
    /// # Panics
    /// When `actual` exceeds the held bytes (internal accounting bug).
    pub(crate) fn reconcile(&mut self, actual: u64) {
        assert!(
            actual <= self.held,
            "reconcile to {actual} exceeds held {}",
            self.held
        );
        let excess = self.held - actual;
        self.global.release(excess);
        self.job.release(excess);
        self.held = actual;
    }

    /// Grow the reservation for an oversize frame: acquires `extra` more
    /// bytes in the same global→job order. The payload is already in
    /// memory; held bytes are attached to writes that complete
    /// independently, so waiting for extra admission delays but cannot
    /// deadlock (design D2).
    ///
    /// Cancellation-aware: dropping the future mid-wait releases any
    /// partially acquired extra bytes.
    pub(crate) async fn grow(&mut self, extra: u64) {
        if extra == 0 {
            return;
        }
        self.global.acquire(extra).await;
        let guard = RollbackGuard {
            budget: Arc::clone(&self.global),
            armed: extra,
        };
        self.job.acquire(extra).await;
        std::mem::forget(guard);
        self.held += extra;
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        if self.held > 0 {
            self.global.release(self.held);
            self.job.release(self.held);
            self.held = 0;
        }
    }
}

/// Split an oversize received frame into bounded slices of at most
/// `quantum` bytes each (design D2: split a larger frame into bounded
/// slices). Zero-copy — slices borrow the input buffer.
///
/// An empty frame splits to no slices; a frame within the quantum splits
/// to itself.
#[must_use]
pub(crate) fn split_oversize_frame(data: &Bytes, quantum: u64) -> Vec<Bytes> {
    assert!(quantum >= 1, "frame quantum must be positive");
    let quantum = usize::try_from(quantum).unwrap_or(usize::MAX);
    if data.is_empty() {
        return vec![];
    }
    let len = data.len();
    if len <= quantum {
        return vec![data.clone()];
    }
    let mut slices = Vec::with_capacity(len.div_ceil(quantum));
    let mut start = 0;
    while start < len {
        let end = (start + quantum).min(len);
        slices.push(data.slice(start..end));
        start = end;
    }
    slices
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn wait_short() -> Duration {
        Duration::from_millis(50)
    }

    #[tokio::test]
    async fn reserve_respects_the_exact_budget() {
        let budgets = WriteBudgets::new(100, 100, 100);
        let job = budgets.job(1);

        let first = job.reserve(60).await;
        assert_eq!(first.held(), 60);
        assert_eq!(job.job_outstanding(), 60);
        assert_eq!(budgets.global_outstanding(), 60);

        let _second = job.reserve(40).await;
        assert_eq!(job.job_outstanding(), 100, "exactly at capacity");

        // One more byte cannot fit; the acquire must stay pending.
        let third = tokio::time::timeout(wait_short(), job.reserve(1)).await;
        assert!(third.is_err(), "reserve beyond capacity must pend");
        assert_eq!(job.job_outstanding(), 100);

        // Releasing the first reservation admits exactly its bytes.
        drop(first);
        assert_eq!(job.job_outstanding(), 40);
        let third = tokio::time::timeout(wait_short(), job.reserve(1))
            .await
            .expect("reserve completes after release");
        assert_eq!(third.held(), 1);
        assert_eq!(job.job_outstanding(), 41);
    }

    #[tokio::test]
    async fn reconcile_releases_the_excess_immediately() {
        let budgets = WriteBudgets::new(100, 100, 100);
        let job = budgets.job(1);

        let mut reservation = job.reserve(100).await;
        reservation.reconcile(30);
        assert_eq!(reservation.held(), 30);
        assert_eq!(job.job_outstanding(), 30);
        assert_eq!(budgets.global_outstanding(), 30);

        // The freed 70 bytes are usable at once.
        let other = tokio::time::timeout(wait_short(), job.reserve(70))
            .await
            .expect("reconciled bytes are immediately available");
        assert_eq!(other.held(), 70);
        assert_eq!(job.job_outstanding(), 100);
    }

    #[tokio::test]
    async fn grow_acquires_additional_budget_for_oversize_frames() {
        let budgets = WriteBudgets::new(100, 100, 100);
        let job = budgets.job(1);

        let mut reservation = job.reserve(40).await;
        reservation.grow(60).await;
        assert_eq!(reservation.held(), 100);
        assert_eq!(job.job_outstanding(), 100);

        // No further growth fits while capacity is exhausted.
        let more = tokio::time::timeout(wait_short(), reservation.grow(1)).await;
        assert!(more.is_err(), "grow beyond capacity must pend");
        assert_eq!(reservation.held(), 100);

        drop(reservation);
        assert_eq!(job.job_outstanding(), 0, "full grown reservation releases");
    }

    #[tokio::test]
    async fn dropped_reservation_releases_every_held_byte() {
        let budgets = WriteBudgets::new(100, 100, 100);
        let job = budgets.job(1);

        // Failure/discard release path: drop without reconciling.
        let held = job.reserve(70).await;
        assert_eq!(budgets.global_outstanding(), 70);
        drop(held);
        assert_eq!(budgets.global_outstanding(), 0);
        assert_eq!(job.job_outstanding(), 0);

        // Partial: reconcile then drop releases only the remainder.
        let mut partial = job.reserve(80).await;
        partial.reconcile(20);
        drop(partial);
        assert_eq!(job.job_outstanding(), 0);
    }

    #[tokio::test]
    async fn cancelled_reserve_rolls_back_the_global_acquisition() {
        // Global has room; the job pool is exhausted by another
        // reservation, so `reserve` must block on the JOB pool after
        // acquiring global — dropping the future must roll the global
        // bytes back (consistent-order rollback guard).
        let budgets = WriteBudgets::new(100, 50, 50);
        let job = budgets.job(1);
        let blocker = job.reserve(50).await;
        assert_eq!(job.job_outstanding(), 50);

        let job_for_task = job.clone();
        let pending = tokio::spawn(async move {
            job_for_task.reserve(10).await;
        });
        // Give the task time to acquire global (and block on job).
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            budgets.global_outstanding(),
            60,
            "blocked reserve holds its global bytes mid-acquisition"
        );
        pending.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            budgets.global_outstanding(),
            50,
            "aborted reserve must roll back its global acquisition"
        );
        assert_eq!(job.job_outstanding(), 50, "job pool untouched");

        // The rolled-back bytes are usable again.
        drop(blocker);
        let fresh = tokio::time::timeout(wait_short(), job.reserve(50))
            .await
            .expect("rolled-back capacity is available");
        assert_eq!(fresh.held(), 50);
    }

    #[tokio::test]
    async fn cancelled_grow_rolls_back_the_global_acquisition() {
        // Global has headroom; the job pool is exhausted, so `grow` blocks
        // on the JOB pool after acquiring global — aborting must roll the
        // global bytes back (consistent-order rollback guard).
        let budgets = WriteBudgets::new(200, 100, 100);
        let job = budgets.job(1);
        let mut reservation = job.reserve(50).await;
        // Exhaust the job pool with a second reservation so `grow` blocks
        // on the job pool after acquiring global.
        let blocker = job.reserve(50).await;

        let pending = tokio::spawn(async move {
            reservation.grow(10).await;
            reservation
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(budgets.global_outstanding(), 110);
        pending.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            budgets.global_outstanding(),
            50,
            "abort releases the moved reservation (50) and rolls back grow's partial global acquisition (10); the blocker's 50 remains"
        );
        drop(blocker);
        assert_eq!(budgets.global_outstanding(), 0);
    }

    #[tokio::test]
    async fn job_pools_are_independent_while_sharing_the_global_pool() {
        let budgets = WriteBudgets::new(200, 100, 100);
        let job_a = budgets.job(1);
        let job_b = budgets.job(2);

        let a = job_a.reserve(100).await;
        assert_eq!(job_a.job_outstanding(), 100);
        assert_eq!(job_b.job_outstanding(), 0, "job pools are independent");
        assert_eq!(budgets.global_outstanding(), 100, "global pool is shared");

        // Job B is limited by the GLOBAL pool now (100 free), not by its
        // own empty pool.
        let b = tokio::time::timeout(wait_short(), job_b.reserve(100))
            .await
            .expect("job b uses the shared global headroom");
        assert_eq!(b.held(), 100);
        assert_eq!(budgets.global_outstanding(), 200);

        // Beyond the global capacity neither job may grow.
        let mut a = a;
        let over = tokio::time::timeout(wait_short(), a.grow(1)).await;
        assert!(over.is_err(), "global capacity bounds every job");
    }

    #[test]
    fn split_oversize_frame_bounds_slices() {
        let data = Bytes::from_static(b"0123456789");
        let slices = split_oversize_frame(&data, 4);
        assert_eq!(
            slices.iter().map(|s| s.len()).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        // Reassembly is byte-exact and zero-copy (slices share the buffer).
        let rebuilt: Vec<u8> = slices.iter().flat_map(|s| s.iter().copied()).collect();
        assert_eq!(rebuilt, data);

        // Exact multiple: no trailing empty slice.
        let exact = split_oversize_frame(&data, 5);
        assert_eq!(
            exact.iter().map(|s| s.len()).collect::<Vec<_>>(),
            vec![5, 5]
        );

        // Within the quantum: the frame itself.
        let one = split_oversize_frame(&data, 10);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0], data);

        // Empty frame: nothing to submit.
        assert!(split_oversize_frame(&Bytes::new(), 4).is_empty());
    }

    #[tokio::test]
    async fn zero_byte_reservation_holds_nothing() {
        let budgets = WriteBudgets::new(10, 10, 10);
        let job = budgets.job(1);
        let reservation = job.reserve(0).await;
        assert_eq!(reservation.held(), 0);
        assert_eq!(job.job_outstanding(), 0);
        drop(reservation);
        assert_eq!(job.job_outstanding(), 0);
    }

    #[tokio::test]
    async fn waiter_is_woken_by_a_concurrent_release() {
        let budgets = WriteBudgets::new(10, 10, 10);
        let job = budgets.job(1);
        let holder = job.reserve(10).await;

        let job_for_waiter = job.clone();
        let waiter = tokio::spawn(async move {
            let reservation = job_for_waiter.reserve(10).await;
            reservation.held()
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter must be parked");

        drop(holder);
        let held = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter wakes on release")
            .expect("waiter task");
        assert_eq!(held, 10);
    }
}
