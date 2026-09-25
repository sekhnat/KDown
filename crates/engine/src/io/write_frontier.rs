// The frontier is defined and verified here; the segmented transfer paths
// drive it when the executor is integrated.
#![allow(dead_code)]
//! Generation-tagged per-lease write frontiers .
//!
//! One frontier tracks one lease attempt's write pipeline through four
//! distinct stages: network **receipt** (a high-watermark, since the server
//! may over-send), write **submission** (queued+executing payload),
//! **acknowledgement** (the checked positional write completed) and the
//! **published** contiguous frontier derived from acknowledgement. The
//! published frontier may never advance across a gap: a missing, failed or
//! discarded earlier write blocks every later publication until it settles,
//! so checkpoint coverage stays a contiguous acknowledged prefix.
//!
//! Stale completions (wrong lease id or generation) are rejected outright:
//! they must never credit the live lease, and the scheduler's accepted-delta
//! accounting ([`crate::scheduler::core::SegmentScheduler::
//! absorb_worker_progress`]) folds only frontier-published records, so
//! retries and splits can never double-credit unique coverage.

use crate::scheduler::interval_set::IntervalSet;

/// Outcome of recording one write completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionStatus {
    /// Accepted as successful; `through` is the (possibly advanced)
    /// contiguous acknowledged frontier, exclusive.
    Acknowledged {
        /// Exclusive contiguous acknowledged offset from the lease start.
        through: u64,
    },
    /// Accepted as failed/discarded; the frontier did not advance (the
    /// failed bytes block it until retried).
    Failed,
    /// Rejected: the completion belongs to another lease id or an
    /// invalidated generation. It must not credit this frontier.
    StaleGeneration,
}

/// Per-lease, per-generation write frontier.
#[derive(Debug)]
pub(crate) struct LeaseFrontier {
    lease_id: u64,
    generation: u64,
    /// Inclusive lease domain `[start, end]`.
    start: u64,
    end: u64,
    /// Exclusive offset past the last contiguously received byte from
    /// `start` (the receipt watermark).
    received_through: u64,
    /// Exclusive high-watermark of any received byte (may exceed
    /// `received_through` when the server over-sends past a gap).
    received_high_water: u64,
    /// Every byte ever submitted for a write (merged, absolute offsets).
    submitted: IntervalSet,
    /// Bytes with an outstanding (queued or executing) write.
    outstanding: IntervalSet,
    /// Bytes whose write completed successfully.
    acknowledged: IntervalSet,
    /// Bytes whose write failed or was discarded (blocked, retried later).
    failed: IntervalSet,
}

impl LeaseFrontier {
    /// Create a frontier for one lease attempt over `[start, end]`
    /// (inclusive) under `lease_id`/`generation`.
    #[must_use]
    pub(crate) fn new(lease_id: u64, generation: u64, start: u64, end: u64) -> Self {
        assert!(start <= end, "lease domain must be non-empty ordered");
        Self {
            lease_id,
            generation,
            start,
            end,
            received_through: start,
            received_high_water: start,
            submitted: IntervalSet::new(),
            outstanding: IntervalSet::new(),
            acknowledged: IntervalSet::new(),
            failed: IntervalSet::new(),
        }
    }

    /// The lease id this frontier tracks.
    #[must_use]
    pub(crate) fn lease_id(&self) -> u64 {
        self.lease_id
    }

    /// The generation this frontier accepts completions for.
    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Inclusive lease start.
    #[must_use]
    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    /// Inclusive lease end.
    #[must_use]
    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    /// Record network receipt of `[offset, offset+len)` (    /// wire bytes are counted at receipt). The contiguous watermark
    /// advances only across touching bytes; the high-watermark records any
    /// position regardless of gaps.
    ///
    /// # Panics
    /// When the receipt lies outside the lease domain (internal bug: the
    /// worker never consumes body bytes outside its validated range).
    pub(crate) fn record_received(&mut self, offset: u64, len: u64) {
        assert!(
            offset >= self.start && offset.saturating_add(len) <= self.end.saturating_add(1),
            "receipt [{offset}, {}) outside lease [{}, {}]",
            offset.saturating_add(len),
            self.start,
            self.end
        );
        self.received_high_water = self.received_high_water.max(offset.saturating_add(len));
        if offset == self.received_through {
            self.received_through = offset.saturating_add(len);
        }
    }

    /// Exclusive contiguous receipt watermark from the lease start.
    #[must_use]
    pub(crate) fn received_through(&self) -> u64 {
        self.received_through
    }

    /// Exclusive high-watermark of received bytes (may pass gaps).
    #[must_use]
    pub(crate) fn received_high_water(&self) -> u64 {
        self.received_high_water
    }

    /// Record that `[offset, offset+len)` was submitted for a write
    /// (queued or executing). Re-submitting failed bytes marks them
    /// outstanding again.
    ///
    /// # Panics
    /// When the submission lies outside the lease domain (internal bug).
    pub(crate) fn record_submission(&mut self, offset: u64, len: u64) {
        assert!(
            len > 0
                && offset >= self.start
                && offset.saturating_add(len) <= self.end.saturating_add(1),
            "submission [{offset}, {}) outside lease [{}, {}]",
            offset.saturating_add(len),
            self.start,
            self.end
        );
        self.submitted.insert(offset, offset + len - 1);
        self.outstanding.insert(offset, offset + len - 1);
    }

    /// Record one write completion. Successful completions insert into the
    /// acknowledged set; failures/discards block the frontier from those
    /// bytes (design D3: never publish past a gap). Completions for
    /// another lease id or an invalidated generation are rejected.
    pub(crate) fn record_completion(
        &mut self,
        lease_id: u64,
        generation: u64,
        offset: u64,
        len: u64,
        success: bool,
    ) -> CompletionStatus {
        if lease_id != self.lease_id || generation != self.generation {
            return CompletionStatus::StaleGeneration;
        }
        debug_assert!(
            len > 0
                && offset >= self.start
                && offset.saturating_add(len) <= self.end.saturating_add(1),
            "completion [{offset}, {}) outside lease [{}, {}]",
            offset.saturating_add(len),
            self.start,
            self.end
        );
        self.outstanding.subtract(offset, offset + len - 1);
        if success {
            self.acknowledged.insert(offset, offset + len - 1);
            CompletionStatus::Acknowledged {
                through: self.acknowledged_through(),
            }
        } else {
            self.failed.insert(offset, offset + len - 1);
            CompletionStatus::Failed
        }
    }

    /// The contiguous acknowledged frontier (exclusive) from the lease
    /// start: the first gap in the acknowledged set, or the lease end + 1
    /// when everything acknowledged. Later completions NEVER advance the
    /// published frontier past a missing/failed earlier write.
    #[must_use]
    pub(crate) fn acknowledged_through(&self) -> u64 {
        self.acknowledged
            .first_gap_from(self.start, self.end.saturating_add(1))
            .unwrap_or_else(|| self.end.saturating_add(1))
    }

    /// Whether any write is still queued or executing.
    #[must_use]
    pub(crate) fn has_outstanding(&self) -> bool {
        !self.outstanding.is_empty()
    }

    /// Bytes with an outstanding (queued or executing) write.
    #[must_use]
    pub(crate) fn outstanding_bytes(&self) -> u64 {
        self.outstanding.len()
    }

    /// Total bytes ever submitted for a write (merged; retries of the same
    /// bytes do not accumulate).
    #[must_use]
    pub(crate) fn submitted_bytes(&self) -> u64 {
        self.submitted.len()
    }

    /// Bytes acknowledged as written.
    #[must_use]
    pub(crate) fn acknowledged_bytes(&self) -> u64 {
        self.acknowledged.len()
    }

    /// Bytes that failed or were discarded and await retry.
    #[must_use]
    pub(crate) fn failed_bytes(&self) -> u64 {
        self.failed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::segmented::LeaseProgress;
    use crate::scheduler::core::{SchedulerPolicy, SegmentScheduler, TargetSelector};
    use std::sync::Arc;

    /// Lease `[100, 299]` (200 bytes) with generation 7.
    fn frontier() -> LeaseFrontier {
        LeaseFrontier::new(42, 7, 100, 299)
    }

    #[test]
    fn reverse_completions_block_then_advance_exactly_once() {
        let mut f = frontier();
        // Two consecutive writes submitted in order.
        f.record_submission(100, 100);
        f.record_submission(200, 100);
        assert!(f.has_outstanding());

        // The LATER write completes first: publication must not move.
        let status = f.record_completion(42, 7, 200, 100, true);
        assert_eq!(
            status,
            CompletionStatus::Acknowledged { through: 100 },
            "frontier stays at the lease start behind the gap"
        );
        assert_eq!(f.acknowledged_bytes(), 100);

        // The earlier write completes: the frontier crosses both ranges
        // exactly once.
        let status = f.record_completion(42, 7, 100, 100, true);
        assert_eq!(
            status,
            CompletionStatus::Acknowledged { through: 300 },
            "contiguous frontier advances across both completions"
        );
        assert!(!f.has_outstanding());
        assert_eq!(f.acknowledged_bytes(), 200);
        // Idempotent re-report of the same completion must not move it
        // again (a duplicate delivery cannot double-advance).
        let status = f.record_completion(42, 7, 200, 100, true);
        assert_eq!(status, CompletionStatus::Acknowledged { through: 300 });
    }

    #[test]
    fn missing_middle_write_blocks_later_publication() {
        let mut f = frontier();
        f.record_submission(100, 50);
        f.record_submission(150, 50);
        f.record_submission(200, 100);

        assert_eq!(
            f.record_completion(42, 7, 100, 50, true),
            CompletionStatus::Acknowledged { through: 150 }
        );
        assert_eq!(
            f.record_completion(42, 7, 200, 100, true),
            CompletionStatus::Acknowledged { through: 150 },
            "the later completion must not publish across the missing middle"
        );
        assert!(f.has_outstanding());

        assert_eq!(
            f.record_completion(42, 7, 150, 50, true),
            CompletionStatus::Acknowledged { through: 300 }
        );
        assert!(!f.has_outstanding());
    }

    #[test]
    fn failed_earlier_write_blocks_frontier_until_retried() {
        let mut f = frontier();
        f.record_submission(100, 100);
        f.record_submission(200, 100);

        // The earlier write fails: it leaves the outstanding set but BLOCKS
        // the frontier; the later success must not publish past it.
        assert_eq!(
            f.record_completion(42, 7, 100, 100, false),
            CompletionStatus::Failed
        );
        assert_eq!(
            f.record_completion(42, 7, 200, 100, true),
            CompletionStatus::Acknowledged { through: 100 }
        );
        assert_eq!(f.failed_bytes(), 100);
        assert_eq!(f.acknowledged_bytes(), 100);
        // Failed bytes are not "outstanding" (nothing is executing), but
        // they are not acknowledged either.
        assert!(!f.has_outstanding());
        assert_eq!(f.outstanding_bytes(), 0);

        // Retry resubmits exactly the failed coverage.
        f.record_submission(100, 100);
        assert!(f.has_outstanding());
        assert_eq!(f.outstanding_bytes(), 100);
        assert_eq!(
            f.record_completion(42, 7, 100, 100, true),
            CompletionStatus::Acknowledged { through: 300 },
            "the retry completes the contiguous frontier across both ranges"
        );
        assert_eq!(f.failed_bytes(), 100, "failure history is retained");
        assert_eq!(f.acknowledged_bytes(), 200);
    }

    #[test]
    fn stale_generation_and_lease_completions_are_rejected() {
        let mut f = frontier();
        f.record_submission(100, 100);

        // Wrong generation (validator change / bump_generation).
        assert_eq!(
            f.record_completion(42, 8, 100, 100, true),
            CompletionStatus::StaleGeneration
        );
        // Wrong lease id (split-reassigned tail).
        assert_eq!(
            f.record_completion(43, 7, 100, 100, true),
            CompletionStatus::StaleGeneration
        );
        // Nothing credited, everything still outstanding.
        assert_eq!(f.acknowledged_bytes(), 0);
        assert_eq!(f.outstanding_bytes(), 100);
        assert_eq!(f.acknowledged_through(), 100);

        // The correct generation still completes (the one submitted write
        // covers [100, 199], so the frontier reaches 200).
        assert_eq!(
            f.record_completion(42, 7, 100, 100, true),
            CompletionStatus::Acknowledged { through: 200 }
        );
    }

    #[test]
    fn received_watermarks_are_distinct_from_acknowledgement() {
        let mut f = frontier();
        f.record_received(100, 64);
        assert_eq!(f.received_through(), 164);
        f.record_received(164, 64);
        assert_eq!(f.received_through(), 228);
        // A gap in receipt does not advance the contiguous watermark but
        // does move the high-watermark (measured separately).
        f.record_received(250, 20);
        assert_eq!(f.received_through(), 228);
        assert_eq!(f.received_high_water(), 270);

        // Acknowledgement is independent of receipt.
        f.record_submission(100, 64);
        assert_eq!(
            f.record_completion(42, 7, 100, 64, true),
            CompletionStatus::Acknowledged { through: 164 }
        );
        assert_eq!(f.acknowledged_through(), 164);
        assert_eq!(f.received_through(), 228);
    }

    #[test]
    fn discarded_write_blocks_the_frontier_like_a_failure() {
        let mut f = frontier();
        f.record_submission(100, 100);
        f.record_submission(200, 100);
        // A discarded write (detach disposition) is a non-success.
        assert_eq!(
            f.record_completion(42, 7, 100, 100, false),
            CompletionStatus::Failed
        );
        assert_eq!(
            f.record_completion(42, 7, 200, 100, true),
            CompletionStatus::Acknowledged { through: 100 }
        );
        assert_eq!(f.acknowledged_through(), 100, "gap blocks publication");
    }

    /// Integration (design D3): frontier-published records feed
    /// the scheduler's accepted-delta accounting; retries and splits must
    /// never double-credit unique coverage.
    #[test]
    fn scheduler_unique_credit_never_doubles_after_retries_and_splits() {
        let mut scheduler = SegmentScheduler::initialize(
            1000,
            &[],
            SchedulerPolicy::with_target(1, 500, TargetSelector::None, 1),
        );

        // Worker A holds the first lease [0, 499].
        let lease = scheduler.acquire().expect("initial lease");
        assert_eq!((lease.start, lease.end), (0, 499));
        let mut frontier = LeaseFrontier::new(lease.id, lease.generation, lease.start, lease.end);
        let cell = Arc::new(LeaseProgress::default());

        // Two submissions; the second completes FIRST (reverse order).
        frontier.record_submission(0, 200);
        frontier.record_submission(200, 300);
        frontier.record_received(0, 200);
        frontier.record_received(200, 300);

        // Publish the frontier state after each completion (worker loop
        // behavior: publish one coherent record per advancement).
        let publish = |frontier: &LeaseFrontier, cell: &LeaseProgress| {
            cell.test_publish(
                frontier.lease_id(),
                frontier.generation(),
                frontier.start(),
                frontier.acknowledged_through(),
            );
        };

        assert_eq!(
            frontier.record_completion(lease.id, lease.generation, 200, 300, true),
            CompletionStatus::Acknowledged { through: 0 },
            "reverse completion must not advance the frontier"
        );
        publish(&frontier, &cell);
        let deltas = scheduler.absorb_worker_progress(&[Arc::clone(&cell)]);
        assert_eq!(deltas[0], 0, "no coverage may be credited across the gap");

        // The earlier write completes: the frontier crosses everything.
        assert_eq!(
            frontier.record_completion(lease.id, lease.generation, 0, 200, true),
            CompletionStatus::Acknowledged { through: 500 }
        );
        publish(&frontier, &cell);
        let deltas = scheduler.absorb_worker_progress(&[Arc::clone(&cell)]);
        assert_eq!(deltas[0], 500, "the full accepted delta credits once");

        // Re-publishing the SAME record (idempotent reconcile) must credit
        // nothing further.
        publish(&frontier, &cell);
        let deltas = scheduler.absorb_worker_progress(&[Arc::clone(&cell)]);
        assert_eq!(deltas[0], 0, "no double-credit for a repeated record");

        // Complete the lease; a retry cycle must not re-credit it.
        assert!(scheduler.complete(lease.id, lease.generation));
        let before = scheduler.completed_ranges().len();
        publish(&frontier, &cell);
        let deltas = scheduler.absorb_worker_progress(&[Arc::clone(&cell)]);
        assert_eq!(deltas[0], 0, "unknown/expired leases credit nothing");
        assert_eq!(scheduler.completed_ranges().len(), before);

        // Split path: the original worker's published frontier crossing the
        // split boundary is clamped to the shrunk lease — bytes beyond the
        // split never credit the original lease, and the split lease
        // credits its own coverage exactly once.
        let lease = scheduler.acquire().expect("second lease [500, 999]");
        assert_eq!((lease.start, lease.end), (500, 999));
        let mut original = LeaseFrontier::new(lease.id, lease.generation, lease.start, lease.end);
        let split = scheduler
            .split_tail(lease.id, lease.generation, 1, 0)
            .expect("split");
        assert_eq!(
            (split.start, split.end),
            (750, 999),
            "half the 500-byte lease"
        );
        // The original worker wrote 400 bytes, crossing the split boundary
        // at 750 (pipelined writes already in flight when the split landed).
        original.record_submission(500, 400);
        original.record_completion(lease.id, lease.generation, 500, 400, true);
        let cell2 = Arc::new(LeaseProgress::default());
        cell2.test_publish(
            lease.id,
            lease.generation,
            lease.start,
            original.acknowledged_through(),
        );
        let deltas = scheduler.absorb_worker_progress(&[Arc::clone(&cell2)]);
        assert_eq!(
            deltas[0], 250,
            "only bytes inside the shrunk lease [500, 749] credit; the rest is clamped"
        );
        assert!(scheduler.complete(lease.id, lease.generation));

        // The split lease's own worker completes its range exactly once.
        let mut split_frontier =
            LeaseFrontier::new(split.id, split.generation, split.start, split.end);
        split_frontier.record_submission(split.start, 250);
        assert_eq!(
            split_frontier.record_completion(split.id, split.generation, split.start, 250, true),
            CompletionStatus::Acknowledged { through: 1000 }
        );
        let cell3 = Arc::new(LeaseProgress::default());
        cell3.test_publish(
            split.id,
            split.generation,
            split.start,
            split_frontier.acknowledged_through(),
        );
        let _ = scheduler.absorb_worker_progress(&[Arc::clone(&cell3)]);
        assert!(scheduler.complete(split.id, split.generation));
        assert!(
            scheduler.is_complete(),
            "the union of credited coverage is exact with no overlap"
        );
    }
}
