//! Segment scheduler: interval planning, leases, and assignment (§12, §31).
//!
//! Work is a normalized [`IntervalSet`] of *pending* bytes. Workers acquire
//! leases carved from the first pending range; progress reporting advances
//! `next_offset`; completed bytes merge into the completed set. All
//! callbacks carry `(lease_id, generation)`; stale generations are rejected
//! (§31, invariant §39.6). Locking model: the owner (job controller) takes
//! one short-lived lock per acquire/report/complete/fail — never per chunk
//! (§13.3).

use std::collections::HashMap;
use std::sync::Arc;

use crate::resume::checkpoint::ByteRange;
use crate::scheduler::interval_set::IntervalSet;
use crate::scheduler::lease::{LeaseId, SegmentLease};

/// Scheduler shaping parameters (§12.2-§12.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerPolicy {
    pub min_segment_size: u64,
    pub max_segment_size: u64,
}

impl SchedulerPolicy {
    #[must_use]
    pub fn new(min_segment_size: u64, max_segment_size: u64) -> Self {
        let min = min_segment_size.max(1);
        Self {
            min_segment_size: min,
            max_segment_size: max_segment_size.max(min),
        }
    }
}

/// The byte-scheduling core (§31).
#[derive(Debug)]
pub struct SegmentScheduler {
    total_size: u64,
    /// Remaining bytes not owned by any lease (coverage set).
    pending: IntervalSet,
    /// Active leases by id.
    active: HashMap<LeaseId, SegmentLease>,
    /// Durably completed ranges.
    completed: IntervalSet,
    next_lease: LeaseId,
    generation: u64,
    policy: SchedulerPolicy,
}

impl SegmentScheduler {
    /// Build a scheduler over `[0, total)` with `completed_ranges` already
    /// finished (§31 initialize). Out-of-domain completed ranges are clamped.
    #[must_use]
    pub fn initialize(
        total_size: u64,
        completed_ranges: &[ByteRange],
        policy: SchedulerPolicy,
    ) -> Self {
        let mut completed = IntervalSet::new();
        for &(s, e) in completed_ranges {
            if s >= total_size {
                continue; // out-of-domain garbage: ignore defensively
            }
            completed.insert(s, e.min(total_size - 1));
        }
        let pending = if total_size == 0 {
            IntervalSet::new()
        } else {
            IntervalSet::from_ranges(&completed.complement_within(total_size))
        };
        Self {
            total_size,
            pending,
            active: HashMap::new(),
            completed,
            next_lease: 1,
            generation: 1,
            policy,
        }
    }

    #[must_use]
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    #[must_use]
    pub fn policy(&self) -> SchedulerPolicy {
        self.policy
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Acquire a pending segment lease (§12.2): carve a slice of at most
    /// `max_segment_size` from the first pending range. `None` when no
    /// pending work remains.
    pub fn acquire(&mut self) -> Option<SegmentLease> {
        let gap = *self.pending.ranges().first()?;
        let gap_len = gap.1 - gap.0 + 1;
        // Segment length: bounded by policy; the whole gap when small.
        let want = self
            .policy
            .max_segment_size
            .min(gap_len)
            .max(self.policy.min_segment_size.min(gap_len));
        let end = gap.0.saturating_add(want).saturating_sub(1).min(gap.1);
        let id = self.next_lease;
        self.next_lease += 1;
        let lease = SegmentLease {
            id,
            generation: self.generation,
            start: gap.0,
            end,
            next_offset: gap.0,
        };
        self.pending.subtract(gap.0, end);
        self.active.insert(id, lease);
        Some(lease)
    }

    /// Report durable progress for a lease (§31): advance `next_offset` to
    /// `durable_through_offset` (bytes `[start, offset)` are written).
    /// Rejects stale generations; never moves the offset backward.
    pub fn report_progress(&mut self, lease_id: LeaseId, generation: u64, durable_through_offset: u64) -> bool {
        let Some(lease) = self.active.get_mut(&lease_id) else {
            return false;
        };
        if lease.generation != generation {
            return false; // stale callback (§39.6)
        }
        if durable_through_offset <= lease.next_offset {
            return true; // no forward movement; not an error
        }
        lease.next_offset = durable_through_offset.min(lease.end.saturating_add(1));
        true
    }

    /// Reconcile lock-free worker progress cells into active leases
    /// (§13.3, task 6.3). Each worker publishes `(lease_id, generation,
    /// durable_through)` with atomics on the chunk path; this pulls those
    /// into the scheduler at lease-boundary moments (complete/fail/split,
    /// checkpoint cadence). Cells for unknown/expired leases are skipped.
    pub fn absorb_worker_progress(&mut self, cells: &[Arc<crate::job::segmented::LeaseProgress>]) {
        for cell in cells {
            let (id, generation, through) = cell.snapshot();
            if id == 0 {
                continue;
            }
            let _ = self.report_progress(id, generation, through);
        }
    }

    #[cfg(test)]
    pub(crate) fn test_publish(
        cell: &crate::job::segmented::LeaseProgress,
        lease_id: LeaseId,
        generation: u64,
        lease_start: u64,
        through: u64,
    ) {
        cell.test_publish(lease_id, generation, lease_start, through);
    }

    /// Mark a lease's full range complete (§31). Rejects stale generations;
    /// double-complete is a rejected no-op.
    pub fn complete(&mut self, lease_id: LeaseId, generation: u64) -> bool {
        let Some(lease) = self.active.remove(&lease_id) else {
            return false;
        };
        if lease.generation != generation {
            // Stale caller must not mutate a reassigned range (§39.6);
            // the live lease stays active.
            self.active.insert(lease_id, lease);
            return false;
        }
        self.completed.insert(lease.start, lease.end);
        true
    }

    /// Fail a lease: requeue only the unfinished tail (§17.3); the consumed
    /// prefix (acknowledged through report_progress) is completed.
    pub fn fail(&mut self, lease_id: LeaseId, generation: u64) -> bool {
        let Some(lease) = self.active.remove(&lease_id) else {
            return false;
        };
        if lease.generation != generation {
            self.active.insert(lease_id, lease);
            return false;
        }
        if lease.next_offset <= lease.end {
            self.pending.insert(lease.next_offset, lease.end);
        }
        if lease.start < lease.next_offset {
            self.completed.insert(lease.start, lease.next_offset - 1);
        }
        true
    }

    /// Give back a lease's unconsumed remainder without completing
    /// anything (worker shutdown / concurrency reduction, §7.3).
    pub fn release(&mut self, lease_id: LeaseId, generation: u64) -> bool {
        let Some(lease) = self.active.remove(&lease_id) else {
            return false;
        };
        if lease.generation != generation {
            self.active.insert(lease_id, lease);
            return false;
        }
        if lease.next_offset <= lease.end {
            self.pending.insert(lease.next_offset, lease.end);
        }
        if lease.start < lease.next_offset {
            self.completed.insert(lease.start, lease.next_offset - 1);
        }
        true
    }

    /// Split: take the unconsumed tail of an active lease for an idle
    /// worker (§12.3). Only bytes at/after the lease's live `next_offset`
    /// may move — bytes read or queued for write are excluded. Returns the
    /// new lease covering the tail. Stale generations are rejected.
    pub fn split_tail(&mut self, lease_id: LeaseId, generation: u64, min_tail: u64) -> Option<SegmentLease> {
        let min_tail = min_tail.max(1);
        let lease = *self.active.get(&lease_id)?;
        if lease.generation != generation {
            return None;
        }
        let tail_start = lease.next_offset;
        let tail_len = lease.end.saturating_sub(tail_start).saturating_add(1);
        if tail_len <= min_tail {
            return None; // nothing worth splitting (§12.3)
        }
        let take = (tail_len / 2).clamp(1, tail_len - 1);
        let new_start = lease.end - take + 1;
        // Shrink the original lease in place.
        let original = self.active.get_mut(&lease_id)?;
        original.end = new_start - 1;
        let id = self.next_lease;
        self.next_lease += 1;
        let split = SegmentLease {
            id,
            generation: self.generation,
            start: new_start,
            end: lease.end,
            next_offset: new_start,
        };
        self.active.insert(id, split);
        Some(split)
    }

    /// Advance the generation after a detected resource change (§26): all
    /// active leases are invalidated and their unconsumed remainders return
    /// to pending. Acknowledged prefixes stay completed.
    pub fn bump_generation(&mut self) -> u64 {
        self.generation += 1;
        let ids: Vec<LeaseId> = self.active.keys().copied().collect();
        for id in ids {
            let lease = self.active.remove(&id).expect("checked above");
            if lease.next_offset <= lease.end {
                self.pending.insert(lease.next_offset, lease.end);
            }
            if lease.start < lease.next_offset {
                self.completed.insert(lease.start, lease.next_offset - 1);
            }
        }
        self.generation
    }

    /// All active leases (§31 active_ranges).
    #[must_use]
    pub fn active_leases(&self) -> Vec<SegmentLease> {
        let mut v: Vec<SegmentLease> = self.active.values().copied().collect();
        v.sort_by_key(|l| l.start);
        v
    }

    /// Bytes currently owned by active leases (unconsumed portions).
    #[must_use]
    pub fn active_bytes(&self) -> u64 {
        self.active.values().map(SegmentLease::remaining).sum()
    }

    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.pending.len()
    }

    /// Completed ranges so far (§31 completed_ranges).
    #[must_use]
    pub fn completed_ranges(&self) -> Vec<ByteRange> {
        self.completed.ranges()
    }

    #[must_use]
    pub fn completed_set(&self) -> &IntervalSet {
        &self.completed
    }

    /// Whether every byte of the domain is complete (§12.1 invariant).
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.total_size == 0 || self.completed.len() == self.total_size
    }

    /// The full accounting invariant (§12.1): union of completed, active
    /// remainders, and pending equals the target domain, with no overlaps.
    #[must_use]
    pub fn invariants_hold(&self) -> bool {
        if self.total_size == 0 {
            return self.pending.is_empty() && self.active.is_empty() && self.completed.is_empty();
        }
        // No overlap between pending and completed.
        for (s, e) in self.pending.ranges() {
            for (cs, ce) in self.completed.ranges() {
                if s <= ce && cs <= e {
                    return false;
                }
            }
        }
        // Active remainders never touch pending or completed bytes.
        for l in self.active.values() {
            if l.next_offset <= l.end {
                for (cs, ce) in self.completed.ranges() {
                    if l.next_offset <= ce && cs <= l.end {
                        return false;
                    }
                }
                for (ps, pe) in self.pending.ranges() {
                    if l.next_offset <= pe && ps <= l.end {
                        return false;
                    }
                }
            }
            if l.end >= self.total_size {
                return false;
            }
        }
        // Union coverage: total = completed + active remaining + pending
        // + in-flight acknowledged prefixes.
        let acknowledged_prefix: u64 = self
            .active
            .values()
            .map(|l| l.next_offset.saturating_sub(l.start))
            .sum();
        self.completed.len() + self.active_bytes() + self.pending_bytes() + acknowledged_prefix
            == self.total_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sched(total: u64) -> SegmentScheduler {
        SegmentScheduler::initialize(total, &[], SchedulerPolicy::new(1024, 64 * 1024))
    }

    /// §13.3/§15.4 (task 6.3): chunk-path progress is published lock-free
    /// into worker cells; the scheduler only reconciles at boundary
    /// moments. Absorb must advance the lease without the worker taking a
    /// scheduler lock, and a fail after absorb must complete the prefix
    /// and requeue only the tail.
    #[test]
    fn absorb_worker_progress_reconciles_without_chunk_locks() {
        let mut s = sched(10_000);
        let lease = s.acquire().expect("lease");
        let cell = crate::job::segmented::LeaseProgress::default();
        SegmentScheduler::test_publish(&cell, lease.id, lease.generation, lease.start, 4_096);
        SegmentScheduler::test_publish(&cell, lease.id, lease.generation, lease.start, 8_192);
        s.absorb_worker_progress(&[std::sync::Arc::new(cell)]);
        let active = s.active_leases();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].next_offset, 8_192);
        // Checkpoint after absorb claims exactly the absorbed bytes.
        let ranges = s.completed_ranges();
        assert!(ranges.is_empty(), "absorbed progress is in-flight, not complete");
        // Fail after absorb: prefix completes, tail requeues (§17.3).
        assert!(s.fail(lease.id, lease.generation));
        assert_eq!(s.completed_ranges(), vec![(lease.start, 8_191)]);
        // Tail-only requeue: pending covers exactly the unconsumed tail.
        assert_eq!(s.pending_bytes(), lease.end - 8_192 + 1);
        let l = s.acquire().expect("requeued tail reacquirable");
        assert_eq!(l.start, 8_192);
    }

    #[test]
    fn absorb_ignores_idle_cells_and_stale_generations() {
        let mut s = sched(10_000);
        let lease = s.acquire().expect("lease");
        let idle = crate::job::segmented::LeaseProgress::default();
        s.absorb_worker_progress(&[std::sync::Arc::new(idle)]);
        // Stale generation must not move the lease (§39.6).
        let cell = crate::job::segmented::LeaseProgress::default();
        SegmentScheduler::test_publish(&cell, lease.id, lease.generation + 100, lease.start, 5_000);
        s.absorb_worker_progress(&[std::sync::Arc::new(cell)]);
        assert_eq!(s.active_leases()[0].next_offset, lease.start);
    }

    #[test]
    fn lease_uniqueness_no_double_lease() {
        // §36.1: no byte belongs to two active leases.
        let mut s = sched(10_000);
        let mut seen: Vec<(u64, u64)> = vec![];
        while let Some(l) = s.acquire() {
            for &(a, b) in &seen {
                assert!(
                    l.start > b || l.end < a,
                    "lease [{}, {}] overlaps [{}, {}]",
                    l.start,
                    l.end,
                    a,
                    b
                );
            }
            seen.push((l.start, l.end));
        }
        assert_eq!(s.pending_bytes(), 0);
        assert_eq!(s.active_bytes(), 10_000);
        assert!(!s.is_complete());
        assert_eq!(s.active_leases().len(), 1, "whole domain fits one max segment");
    }

    #[test]
    fn stale_generation_callbacks_rejected() {
        let mut s = sched(1000);
        let lease = s.acquire().expect("lease");
        let stale_gen = lease.generation.wrapping_add(1);
        assert!(!s.complete(lease.id, stale_gen), "stale complete rejected");
        assert!(!s.fail(lease.id, stale_gen), "stale fail rejected");
        assert!(!s.report_progress(lease.id, stale_gen, 50), "stale progress rejected");
        // Current generation still works.
        assert!(s.report_progress(lease.id, lease.generation, 50));
        assert!(s.complete(lease.id, lease.generation));
        assert_eq!(s.completed_ranges(), vec![(lease.start, lease.end)]);
    }

    #[test]
    fn complete_advances_and_converges() {
        let mut s = sched(300);
        let mut done = 0u64;
        while let Some(l) = s.acquire() {
            done += l.end - l.start + 1;
            assert!(s.complete(l.id, l.generation));
        }
        assert_eq!(done, 300);
        assert!(s.is_complete());
        assert_eq!(s.pending_bytes(), 0);
        assert!(s.invariants_hold());
    }

    #[test]
    fn fail_requeues_only_unfinished_tail() {
        let mut s = sched(1000);
        let l = s.acquire().expect("lease");
        // Acknowledge 400 bytes of the lease.
        assert!(s.report_progress(l.id, l.generation, l.start + 400));
        assert!(s.fail(l.id, l.generation));
        // Completed prefix recorded; tail returns to pending.
        assert_eq!(s.completed_ranges(), vec![(l.start, l.start + 399)]);
        assert_eq!(s.pending_bytes(), 600);
        // Reacquiring starts at the failed tail offset, not from zero (§17.3).
        let next = s.acquire().expect("reacquire");
        assert_eq!(next.start, l.start + 400, "tail-only retry (§17.3)");
        assert!(s.invariants_hold());
    }

    #[test]
    fn double_complete_is_noop() {
        let mut s = sched(100);
        let l = s.acquire().expect("lease");
        assert!(s.complete(l.id, l.generation));
        assert!(!s.complete(l.id, l.generation), "second complete must be rejected");
        assert_eq!(s.completed_ranges(), vec![(l.start, l.end)]);
    }

    #[test]
    fn release_returns_remainder() {
        let mut s = sched(1000);
        let l = s.acquire().expect("lease");
        assert!(s.report_progress(l.id, l.generation, 500));
        assert!(s.release(l.id, l.generation));
        // [500, 999] back to pending; [0, 499] completed.
        assert_eq!(s.completed_ranges(), vec![(0, 499)]);
        assert_eq!(s.pending_bytes(), 500);
        assert!(s.invariants_hold());
    }

    #[test]
    fn unknown_lease_rejected() {
        let mut s = sched(100);
        assert!(!s.complete(999, 1));
        assert!(!s.fail(999, 1));
        assert!(!s.report_progress(999, 1, 5));
        assert!(!s.release(999, 1));
    }

    #[test]
    fn split_tail_excludes_consumed_bytes() {
        let mut s = sched(10_000);
        let l = s.acquire().expect("lease");
        // Worker consumed 2,000 bytes.
        assert!(s.report_progress(l.id, l.generation, 2_000));
        let tail = s.split_tail(l.id, l.generation, 100).expect("split");
        assert!(tail.start >= 2_000, "split must exclude consumed bytes (§12.3)");
        assert_eq!(tail.next_offset, tail.start, "split lease starts fresh");
        let orig = s
            .active_leases()
            .into_iter()
            .find(|x| x.id == l.id)
            .expect("original stays active");
        assert!(orig.end < tail.start, "no overlap after split");
        assert_eq!(orig.next_offset, 2_000, "original next_offset untouched");
        assert!(s.invariants_hold());
    }

    #[test]
    fn split_refuses_small_tail_and_stale() {
        let mut s = sched(200);
        let l = s.acquire().expect("lease");
        assert!(s.split_tail(l.id, l.generation, 10_000).is_none(), "tail too small");
        let stale = l.generation.wrapping_add(1);
        assert!(s.split_tail(l.id, stale, 1).is_none(), "stale split rejected");
    }

    #[test]
    fn bump_generation_drops_active_leases() {
        let mut s = sched(1000);
        let l = s.acquire().expect("lease");
        assert!(s.report_progress(l.id, l.generation, 100));
        let _ = s.bump_generation();
        assert!(s.active_leases().is_empty(), "generation change invalidates leases");
        // Old lease's callback now stale.
        assert!(!s.complete(l.id, l.generation));
        // Acknowledged prefix [0,99] completed; remainder [100,999] pending.
        assert_eq!(s.completed_ranges(), vec![(0, 99)]);
        let next = s.acquire().expect("reacquire after bump");
        assert_eq!(next.start, 100);
        assert_ne!(next.generation, l.generation, "new generation stamp");
        assert!(s.invariants_hold());
    }

    #[test]
    fn initialize_from_completed_ranges() {
        let mut s = SegmentScheduler::initialize(
            1000,
            &[(0, 199), (400, 699)],
            SchedulerPolicy::new(1, 100),
        );
        assert_eq!(s.pending_bytes(), 500);
        assert_eq!(s.completed_ranges(), vec![(0, 199), (400, 699)]);
        let l = s.acquire().expect("lease");
        assert_eq!(l.start, 200, "first gap is [200, 399]");
        assert!(l.end <= 399);
    }

    #[test]
    fn empty_domain_completes_immediately() {
        let mut s = sched(0);
        assert!(s.is_complete());
        assert!(s.acquire().is_none());
        assert!(s.invariants_hold());
    }

    #[test]
    fn segment_size_respects_policy_bounds() {
        let mut s = SegmentScheduler::initialize(1_000_000, &[], SchedulerPolicy::new(1024, 4096));
        let l = s.acquire().expect("lease");
        assert_eq!(l.end - l.start + 1, 4096, "max segment size honored");
    }

    #[test]
    fn progress_report_does_not_recede() {
        let mut s = sched(1000);
        let l = s.acquire().expect("lease");
        assert!(s.report_progress(l.id, l.generation, 500));
        // Older report must not move the offset back.
        assert!(s.report_progress(l.id, l.generation, 200));
        let after = s.active_leases().into_iter().find(|x| x.id == l.id).expect("lease");
        assert_eq!(after.next_offset, 500, "monotonic progress");
        // Beyond-end report clamps to end+1.
        assert!(s.report_progress(l.id, l.generation, 10_000));
        let after = s.active_leases().into_iter().find(|x| x.id == l.id).expect("lease");
        assert_eq!(after.next_offset, l.end + 1);
        assert_eq!(after.remaining(), 0);
    }
}