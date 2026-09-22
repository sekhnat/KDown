//! Property tests for the interval set and segment scheduler (§36.4,
//! tasks 5.1-5.3): normalization, disjointness, and coverage invariants
//! hold across randomized operation sequences; successful completion
//! covers every byte exactly once logically across acquire/fail/split/
//! release sequences.

use kdown_engine::resume::checkpoint::ByteRange;
use kdown_engine::scheduler::interval_set::IntervalSet;
use kdown_engine::scheduler::lease::SegmentLease;
use kdown_engine::scheduler::core::{SchedulerPolicy, SegmentScheduler};

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
                            let _ = s.split_tail(l.id, l.generation, 1);
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
                        if let Some(tail) = s.split_tail(src.id, src.generation, 1) {
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