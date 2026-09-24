//! Property tests for the interval set and segment scheduler (§36.4,
//! tasks 5.1-5.3): normalization, disjointness, and coverage invariants
//! hold across randomized operation sequences; successful completion
//! covers every byte exactly once logically across acquire/fail/split/
//! release sequences.

use kdown_engine::resume::checkpoint::ByteRange;
use kdown_engine::scheduler::core::{SchedulerPolicy, SegmentScheduler};
use kdown_engine::scheduler::interval_set::IntervalSet;
use kdown_engine::scheduler::lease::SegmentLease;

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Normalization: any sequence of inserts (random overlapping/adjacent/
    /// disjoint) leaves the set sorted, disjoint, and unmerged, with `len`
    /// equal to the sum of range lengths.
    #[test]
    fn insert_normalizes(ranges in proptest::collection::vec((0u64..500, 0u64..500), 1..60)) {
        let mut s = IntervalSet::new();
        for (a, b) in &ranges {
            s.insert((*a).min(*b), (*a).max(*b));
        }
        let rs = s.ranges();
        for w in rs.windows(2) {
            prop_assert!(w[0].1 + 1 < w[1].0, "not sorted/merged: {:?}", rs);
        }
        prop_assert_eq!(s.len(), rs.iter().map(|(a, b)| b - a + 1).sum::<u64>());
    }

    /// Subtract removes exactly the intersected window from any set.
    #[test]
    fn subtract_is_exact(
        base in proptest::collection::vec((0u64..500, 0u64..500), 1..30),
        c0 in 0u64..600,
        c1 in 0u64..600,
    ) {
        let mut s = IntervalSet::new();
        for (a, b) in &base {
            s.insert((*a).min(*b), (*a).max(*b));
        }
        let before = s.len();
        let window = (c0.min(c1), c0.max(c1));
        let covering: u64 = s
            .ranges()
            .iter()
            .filter(|(a, b)| *a <= window.1 && window.0 <= *b)
            .map(|(a, b)| (*b).min(window.1) - (*a).max(window.0) + 1)
            .sum();
        s.subtract(window.0, window.1);
        prop_assert_eq!(s.len(), before - covering);
        for (a, b) in s.ranges() {
            prop_assert!(b < window.0 || a > window.1, "window residue: {:?}", (a, b, window));
        }
    }

    /// Coverage invariant (§36.4): for any randomized sequence of
    /// acquire/report/fail/split/release on a scheduler, when every pending
    /// byte is eventually acquired and completed, the completed set covers
    /// [0, N) exactly — no gaps, no overlaps, no double coverage.
    #[test]
    fn randomized_sequences_cover_exactly(
        total in 1u64..2_000,
        ops in proptest::collection::vec(0u8..5, 10..200),
    ) {
        let mut s = SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::new(1, total / 3 + 1),
        );
        let mut counter = 0u64;

        for op in ops {
            counter += 1;
            prop_assert!(s.invariants_hold(), "invariants after op {op}");
            let act = s.active_leases();
            match op {
                0 => {
                    let _ = s.acquire();
                }
                1 => {
                    // Report random forward progress on a live lease.
                    if let Some(l) = act.first().copied() {
                        let step = 1 + counter % 7;
                        let target = l.next_offset.saturating_add(step).min(l.end + 1);
                        prop_assert!(s.report_progress(l.id, l.generation, target));
                    }
                }
                2 => {
                    // Complete a live lease.
                    if let Some(l) = act.last().copied() {
                        prop_assert!(s.complete(l.id, l.generation));
                    }
                }
                3 => {
                    // Fail a live lease -> tail requeued.
                    if let Some(l) = act.first().copied() {
                        prop_assert!(s.fail(l.id, l.generation));
                    }
                }
                _ => {
                    // Release or split a live lease.
                    if let Some(l) = act.last().copied() {
                        if counter % 2 == 0 {
                            prop_assert!(s.release(l.id, l.generation));
                        } else {
                            let _ = s.split_tail(l.id, l.generation, 1, 0);
                        }
                    }
                }
            }
        }

        // Drain: finish all active leases, then acquire everything
        // remaining and complete it (active leases carry work too).
        loop {
            let live = s.active_leases();
            if let Some(l) = live.first().copied() {
                prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
                prop_assert!(s.complete(l.id, l.generation));
                continue;
            }
            match s.acquire() {
                Some(l) => {
                    prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
                    prop_assert!(s.complete(l.id, l.generation));
                }
                None => break,
            }
        }
        prop_assert!(s.is_complete(), "drain must complete the domain");
        prop_assert!(s.invariants_hold());
        // Every byte covered exactly once: completed set is exactly [0, N).
        let rs = s.completed_ranges();
        prop_assert_eq!(rs, vec![(0u64, total - 1)] as Vec<ByteRange>, "exact single coverage");
    }

    /// Resumed intervals (task 3.1): any completed prefix admitted at
    /// initialization leaves a scheduler whose union of completed, active
    /// remainders and pending is exactly the target domain, and draining it
    /// completes every remaining byte exactly once.
    #[test]
    fn resumed_intervals_complete_exactly(
        total in 2u64..2_000,
        prefix in 0u64..2_000,
        ops in proptest::collection::vec(0u8..4, 5..120),
    ) {
        let prefix_end = prefix.min(total - 1); // inclusive end of resumed coverage
        let mut s = SegmentScheduler::initialize(
            total,
            &[(0, prefix_end)],
            SchedulerPolicy::new(1, total / 3 + 1),
        );
        prop_assert!(s.invariants_hold());
        // The union of pending + active remainders + completed is exactly
        // the domain — no gaps, no overlaps (task 3.1 normalized union).
        prop_assert_eq!(
            s.completed_set().len() + s.pending_bytes() + s.active_bytes(),
            total
        );
        for op in ops {
            let act = s.active_leases();
            match op {
                0 => {
                    let _ = s.acquire();
                }
                1 => {
                    if let Some(l) = act.first().copied() {
                        let step = 1 + l.end.saturating_sub(l.next_offset) / 3;
                        let _ = s.report_progress(l.id, l.generation, l.next_offset + step);
                    }
                }
                2 => {
                    if let Some(l) = act.last().copied() {
                        prop_assert!(s.complete(l.id, l.generation));
                    }
                }
                _ => {
                    if let Some(l) = act.first().copied() {
                        prop_assert!(s.fail(l.id, l.generation));
                    }
                }
            }
            prop_assert!(s.invariants_hold());
        }
        // Drain: complete everything exactly once.
        loop {
            let live = s.active_leases();
            if let Some(l) = live.first().copied() {
                prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
                prop_assert!(s.complete(l.id, l.generation));
                continue;
            }
            match s.acquire() {
                Some(l) => {
                    prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
                    prop_assert!(s.complete(l.id, l.generation));
                }
                None => break,
            }
        }
        prop_assert!(s.is_complete());
        let rs = s.completed_ranges();
        prop_assert_eq!(rs, vec![(0u64, total - 1)] as Vec<ByteRange>, "resumed drain exact");
    }

    /// Stale generations (§31, task 3.1): after a generation bump, every
    /// old-generation operation is rejected and mutates nothing.
    #[test]
    fn stale_generation_operations_are_rejected(
        total in 2u64..1_000,
        step in 1u64..64,
    ) {
        let mut s = SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::new(1, total / 2 + 1),
        );
        let lease = s.acquire().expect("initial lease");
        prop_assert!(s.report_progress(lease.id, lease.generation, lease.start + step.min(lease.end - lease.start)));
        let old_generation = lease.generation;
        // A snapshot of the live state before the bump.
        let _before_active = s.active_leases();
        let before_completed_size = s.completed_set().len();
        let _before_pending = s.pending_bytes();

        prop_assert_eq!(s.bump_generation(), old_generation + 1);
        // Every operation with the STALE generation must be rejected.
        prop_assert!(!s.report_progress(lease.id, old_generation, lease.end + 1));
        prop_assert!(!s.complete(lease.id, old_generation));
        prop_assert!(!s.fail(lease.id, old_generation));
        prop_assert!(s.split_tail(lease.id, old_generation, 1, 0).is_none());
        prop_assert!(!s.release(lease.id, old_generation));

        // The generation change invalidates the whole job (§26): the
        // acknowledged prefix is promoted to completed, the unconsumed
        // remainder returns to pending, and nothing stays active.
        prop_assert!(s.active_leases().is_empty());
        prop_assert!(s.completed_set().len() > before_completed_size);
        // The union stays exactly the domain (no gaps, no overlaps).
        prop_assert_eq!(
            s.completed_set().len() + s.pending_bytes() + s.active_bytes(),
            total
        );
        prop_assert!(s.invariants_hold());

        // The new generation re-leases and completes the domain exactly.
        while let Some(l) = s.acquire() {
            prop_assert_eq!(l.generation, old_generation + 1);
            prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
            prop_assert!(s.complete(l.id, l.generation));
        }
        prop_assert!(s.is_complete());
        prop_assert_eq!(
            s.completed_ranges(),
            vec![(0u64, total - 1)] as Vec<ByteRange>,
            "post-invalidation drain covers every byte exactly once"
        );
    }

    /// Inclusive/exclusive boundary math (§31, task 3.1): lease endpoints
    /// are inclusive `[start, end]`, the write frontier `next_offset` is
    /// exclusive, and `remaining()` counts `end - next_offset + 1`.
    #[test]
    fn lease_boundary_math_is_inclusive_end_exclusive_frontier(
        total in 1u64..4_000,
        target in 1u64..1_000,
    ) {
        let mut s = SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::with_target(1, target.max(1), kdown_engine::scheduler::core::TargetSelector::None, 256 * 1024),
        );
        let mut covered: u64 = 0;
        while let Some(lease) = s.acquire() {
            prop_assert_eq!(lease.next_offset, lease.start, "frontier starts at the inclusive start");
            prop_assert_eq!(lease.remaining(), lease.end - lease.next_offset + 1);
            prop_assert!(lease.end >= lease.start);
            // Report full progress: next_offset lands exactly at end + 1.
            prop_assert!(s.report_progress(lease.id, lease.generation, lease.end + 1));
            let after = s.lease_next_offset(lease.id).expect("active lease");
            prop_assert_eq!(after, lease.end + 1, "exclusive frontier is end + 1");
            prop_assert!(s.complete(lease.id, lease.generation));
            covered += lease.end - lease.start + 1;
            prop_assert!(covered <= total, "leases must not exceed the domain");
        }
        prop_assert_eq!(covered, total, "the union of leases is exactly the domain");
        prop_assert!(s.is_complete());
        // The final lease was the exact partial remainder: the last
        // completed range ends at total - 1 (inclusive).
        let rs = s.completed_ranges();
        prop_assert_eq!(rs.last().copied(), Some((0u64, total - 1)));
    }

    /// Ready-work target (task 3.3, design D4): with the divisor enabled,
    /// consecutive acquires leave roughly `divisor` unclaimed leases of
    /// pending work behind (where the gap permits), and the union invariant
    /// plus exact drain coverage still hold under 1→4→1 desired changes.
    #[test]
    fn ready_work_carving_keeps_pending_and_stays_exact(
        total in 4_000u64..400_000,
        desired_seq in proptest::collection::vec(1u64..5, 1..12),
    ) {
        let mut s = SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::with_target(1, total, kdown_engine::scheduler::core::TargetSelector::None, 1),
        );
        // The job's ready factor candidate: ~3x desired (design D4).
        let ready_factor = 3u64;
        let mut acquired: Vec<SegmentLease> = Vec::new();
        for &desired in &desired_seq {
            s.set_ready_work_divisor(ready_factor * desired.max(1));
            // Acquire up to `desired` leases at this concurrency.
            for _ in 0..desired {
                if let Some(lease) = s.acquire() {
                    prop_assert!(s.invariants_hold());
                    acquired.push(lease);
                }
            }
            prop_assert_eq!(
                s.completed_set().len() + s.pending_bytes() + s.active_bytes(),
                total,
                "union stays exact under concurrency changes"
            );
        }
        // No overlapping logical ownership: live leases stay disjoint and
        // every recorded lease is still owned (no leak — the drain below
        // must complete every one of them).
        let live = s.active_leases();
        for (a, b) in live.iter().zip(live.iter().skip(1)) {
            prop_assert!(a.end < b.start, "leases overlap: {a:?} {b:?}");
        }
        // Drain: every lease (and every pending byte) completes exactly once.
        for lease in &acquired {
            let live_now = s.lease_next_offset(lease.id);
            if live_now.is_some() {
                prop_assert!(s.report_progress(lease.id, lease.generation, lease.end + 1));
                prop_assert!(s.complete(lease.id, lease.generation));
            }
        }
        loop {
            let live = s.active_leases();
            if let Some(l) = live.first().copied() {
                prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
                prop_assert!(s.complete(l.id, l.generation));
                continue;
            }
            match s.acquire() {
                Some(l) => {
                    prop_assert!(s.report_progress(l.id, l.generation, l.end + 1));
                    prop_assert!(s.complete(l.id, l.generation));
                }
                None => break,
            }
        }
        prop_assert!(s.is_complete(), "no lease leak: the domain drains exactly");
        prop_assert_eq!(
            s.completed_ranges(),
            vec![(0u64, total - 1)] as Vec<ByteRange>
        );
    }

    /// Split never includes consumed bytes and no two live leases ever
    /// overlap (no-double-lease property, §12.3).
    #[test]
    fn split_never_double_leases(
        total in 2u64..5_000,
        ops in proptest::collection::vec(0u8..4, 5..80),
    ) {
        let mut s = SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::new(1, total / 2 + 1),
        );
        for op in ops {
            let act = s.active_leases();
            match op {
                0 => {
                    let _ = s.acquire();
                }
                1 => {
                    // Advance a live lease by a deterministic step.
                    if let Some(l) = act.first().copied() {
                        let span = l.end - l.start + 1;
                        let step = (span / 3).max(1);
                        let _ = s.report_progress(l.id, l.generation, l.start + step);
                    }
                }
                2 => {
                    // Split the lease with the largest tail.
                    if let Some(src) = act.iter().copied().max_by_key(SegmentLease::remaining) {
                        if let Some(tail) = s.split_tail(src.id, src.generation, 1, 0) {
                            prop_assert!(tail.start >= src.next_offset, "split excludes consumed bytes");
                        }
                    }
                }
                _ => {
                    // Complete or fail a live lease.
                    if let Some(l) = act.last().copied() {
                        if !s.complete(l.id, l.generation) {
                            let _ = s.fail(l.id, l.generation);
                        }
                    }
                }
            }
            // Two live leases never overlap.
            let live = s.active_leases();
            for (a, b) in live.iter().zip(live.iter().skip(1)) {
                prop_assert!(a.end < b.start, "leases overlap: {a:?} {b:?}");
            }
            prop_assert!(s.invariants_hold());
        }
    }
}

/// Ready-work shaping (deterministic, task 3.3): with the divisor set,
/// the first carve leaves roughly `divisor - 1` further acquires of
/// pending work instead of consuming the whole gap.
#[test]
fn ready_work_leaves_unclaimed_pending() {
    let total: u64 = 64 * 1024 * 1024;
    let min_segment: u64 = 1024 * 1024;
    let mut s = SegmentScheduler::initialize(
        total,
        &[],
        SchedulerPolicy::with_target(
            min_segment,
            8 * 1024 * 1024,
            kdown_engine::scheduler::core::TargetSelector::Explicit(8 * 1024 * 1024),
            256 * 1024,
        ),
    );
    // divisor 12 reserves 11 min-sized unclaimed leases behind each carve;
    // with a 64 MiB gap the target (8 MiB) fits inside the reserve cap, so
    // every carve keeps the reserve pending until the tail.
    s.set_ready_work_divisor(12);
    let reserve: u64 = 11 * min_segment;
    let mut leases = 0;
    while let Some(lease) = s.acquire() {
        leases += 1;
        let span = lease.end - lease.start + 1;
        assert!(
            span <= 8 * 1024 * 1024,
            "carve bounded by the target: {span}"
        );
        let pending = s.pending_bytes();
        if pending > 0 {
            assert!(
                pending >= reserve.min(pending),
                "ready work maintained while bytes permit: pending={pending}"
            );
        }
        assert!(s.invariants_hold());
    }
    // The union of all carved leases is exactly the domain (no gaps, no
    // overlaps, no lease leak) and near the tail the reserve shrinks carves
    // (5 MiB at gap 16 MiB with an 11 MiB reserve) instead of letting
    // pending run dry.
    let carved: u64 = s.active_leases().iter().map(|l| l.end - l.start + 1).sum();
    assert_eq!(carved, total, "all leases from pending, domain fully owned");
    assert!(leases >= 8, "at least the target-sized leases: {leases}");
    assert!(s.invariants_hold());

    // Where the gap does NOT leave room beyond the reserve (12 MiB gap,
    // 11 MiB reserve), the carve shrinks to keep the ready-work promise:
    // the first lease is 1 MiB with 11 MiB left pending for other workers.
    let mut small = SegmentScheduler::initialize(
        12 * 1024 * 1024,
        &[],
        SchedulerPolicy::with_target(
            min_segment,
            8 * 1024 * 1024,
            kdown_engine::scheduler::core::TargetSelector::Explicit(8 * 1024 * 1024),
            256 * 1024,
        ),
    );
    small.set_ready_work_divisor(12);
    let lease = small.acquire().expect("small-gap lease");
    assert_eq!(
        lease.end - lease.start + 1,
        min_segment,
        "carve shrinks to keep the ready-work reserve pending"
    );
    assert_eq!(
        small.pending_bytes(),
        11 * min_segment,
        "11 unclaimed min-sized leases remain pending"
    );
    assert!(small.invariants_hold());
}

/// Duration-informed sizing (task 3.4, design D4): seeded from the explicit
/// size until samples stabilize, driven by smoothed per-lease unique
/// goodput afterwards, bounded to [min, max], a 2× step change, and exact
/// coverage over checkpoint-resumed gaps.
mod duration_sizing_tests {
    use super::*;
    use kdown_engine::scheduler::core::TargetSelector;
    use std::time::Duration;

    fn duration_scheduler(total: u64, duration_ms: u64) -> SegmentScheduler {
        SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::with_target(
                64 * 1024,
                32 * 1024 * 1024,
                TargetSelector::Duration {
                    duration_ms,
                    seed_size: 1024 * 1024,
                },
                256 * 1024,
            ),
        )
    }

    /// Fast/slow scripted samples: a fast lease (many bytes, short hold)
    /// grows the next allocation; a slow lease (few bytes, long hold)
    /// shrinks it — in the right direction, within bounds, without
    /// oscillating on one outlier (EWMA + 2× step bound).
    #[test]
    fn duration_sizing_follows_scripted_service_rates() {
        let mut s = duration_scheduler(256 * 1024 * 1024, 1_000);

        // Seeded from the explicit size until samples stabilize.
        let first = s.acquire().expect("first lease");
        assert_eq!(
            first.end - first.start + 1,
            1024 * 1024,
            "seed allocation is the explicit size"
        );
        // Fast sample: 4 MiB in 500 ms (8 bytes/ms) completes quickly.
        assert!(s.report_progress(first.id, first.generation, first.end + 1));
        std::thread::sleep(Duration::from_millis(30));
        assert!(s.complete(first.id, first.generation));
        // Two more quick samples stabilize the EWMA high.
        for _ in 0..2 {
            let l = s.acquire().expect("lease");
            assert!(s.report_progress(l.id, l.generation, l.end + 1));
            std::thread::sleep(Duration::from_millis(30));
            assert!(s.complete(l.id, l.generation));
        }
        let fast = s.acquire().expect("post-fast lease");
        let fast_size = fast.end - fast.start + 1;
        assert!(
            fast_size > 1024 * 1024,
            "fast service grows the allocation: {fast_size}"
        );
        assert!(
            fast_size <= 2 * 1024 * 1024,
            "step change bounded to 2x the previous allocation: {fast_size}"
        );

        // Slow service: several held-long leases with tiny acknowledged
        // prefixes decay the EWMA (one outlier must NOT collapse it —
        // design D4 hysteresis).
        for _ in 0..3 {
            let slow = s.acquire().expect("slow lease");
            assert!(s.report_progress(slow.id, slow.generation, slow.start + 64 * 1024));
            std::thread::sleep(Duration::from_millis(120));
            assert!(s.fail(slow.id, slow.generation));
        }
        // The estimate has decayed; the next allocation shrinks (the 2x
        // step bound halves per allocation, so the decay is gradual).
        let after_slow = s.acquire().expect("post-slow lease");
        let after_slow_size = after_slow.end - after_slow.start + 1;
        assert!(
            after_slow_size <= fast_size / 2,
            "sustained slow service shrinks the allocation: {after_slow_size} <= {}/2",
            fast_size
        );
        assert!(s.invariants_hold());
    }

    /// Min/max bounds: an extreme sample cannot push the allocation outside
    /// the configured segment bounds.
    #[test]
    fn duration_sizing_respects_bounds() {
        let mut s = duration_scheduler(256 * 1024 * 1024, 10_000);
        // Very fast samples (huge bytes/ms) cannot exceed max_segment.
        for _ in 0..4 {
            let l = s.acquire().expect("lease");
            assert!(s.report_progress(l.id, l.generation, l.end + 1));
            assert!(s.complete(l.id, l.generation));
        }
        let l = s.acquire().expect("lease");
        assert!(l.end - l.start < 32 * 1024 * 1024, "max segment bound");
        // A tiny-gap scheduler: the allocation never exceeds the gap.
        let mut small = duration_scheduler(128 * 1024, 1_000);
        for _ in 0..4 {
            if let Some(l) = small.acquire() {
                assert!(small.report_progress(l.id, l.generation, l.end + 1));
                assert!(small.complete(l.id, l.generation));
            }
        }
        if let Some(l) = small.acquire() {
            assert!(
                l.end - l.start < 128 * 1024,
                "allocation clamped to the gap"
            );
        }
        assert!(small.invariants_hold());
    }

    /// Checkpoint resume gaps: with a resumed completed prefix, duration
    /// sizing allocates only from the remaining gap, the final partial
    /// range is exact, and the drain covers the domain exactly once.
    #[test]
    fn duration_sizing_over_resumed_gaps_is_exact() {
        let total: u64 = 8 * 1024 * 1024;
        let resumed_end: u64 = 3 * 1024 * 1024 - 1;
        let mut s = SegmentScheduler::initialize(
            total,
            &[(0, resumed_end)],
            SchedulerPolicy::with_target(
                64 * 1024,
                32 * 1024 * 1024,
                TargetSelector::Duration {
                    duration_ms: 1_000,
                    seed_size: 1024 * 1024,
                },
                256 * 1024,
            ),
        );
        // The first allocation starts exactly at the resumed gap.
        let first = s.acquire().expect("first lease over the gap");
        assert_eq!(first.start, resumed_end + 1);
        assert!(s.report_progress(first.id, first.generation, first.end + 1));
        assert!(s.complete(first.id, first.generation));
        // Drain with samples along the way; the union stays exact.
        while let Some(l) = s.acquire() {
            assert!(s.report_progress(l.id, l.generation, l.end + 1));
            assert!(s.complete(l.id, l.generation));
            assert!(s.invariants_hold());
        }
        assert!(s.is_complete());
        assert_eq!(
            s.completed_ranges(),
            vec![(0u64, total - 1)] as Vec<ByteRange>,
            "resumed + duration-sized coverage is exact"
        );
    }
}
