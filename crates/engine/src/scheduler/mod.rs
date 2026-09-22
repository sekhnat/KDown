//! Segment scheduling: interval planning, leases, and assignment (§12).

use crate::resume::checkpoint::ByteRange;

pub mod interval_set;
pub mod lease;
pub mod core;

pub use interval_set::IntervalSet;
pub use lease::{LeaseId, SegmentLease};
pub use core::{SchedulerPolicy, SegmentScheduler};

/// Default oversubscription factor (§12.2): 2-4 so faster workers can
/// consume additional work instead of waiting on one slow long-lived
/// segment.
pub const OVERSUBSCRIPTION_FACTOR: u64 = 3;

/// Initial segmentation (§12.2): the segment size and per-worker slice
/// plan for a file of `N` bytes and target worker count `W`.
///
/// ```text
/// segment_size = clamp(
///     ceil(N / (W * oversubscription_factor)),
///     min_segment_size,
///     max_segment_size
/// )
/// ```
///
/// Returns `(segment_size, ranges)` where `ranges` tiles `[0, N)` into
/// inclusive segments of `segment_size` (the last one short). Empty files
/// yield no ranges.
#[must_use]
pub fn plan_segments(
    total_size: u64,
    target_workers: u32,
    min_segment_size: u64,
    max_segment_size: u64,
) -> (u64, Vec<ByteRange>) {
    if total_size == 0 {
        return (0, vec![]);
    }
    let target_workers = target_workers.max(1);
    let min_segment_size = min_segment_size.max(1);
    let max_segment_size = max_segment_size.max(min_segment_size);
    let oversubscribed = (target_workers as u64)
        .saturating_mul(OVERSUBSCRIPTION_FACTOR)
        .max(1);
    let raw = total_size.div_ceil(oversubscribed);
    let segment_size = raw.clamp(min_segment_size, max_segment_size);
    let ranges = tile_ranges(total_size, segment_size);
    (segment_size, ranges)
}

/// Tile `[0, total)` into inclusive ranges of `chunk` bytes.
#[must_use]
pub fn tile_ranges(total_size: u64, chunk: u64) -> Vec<ByteRange> {
    let chunk = chunk.max(1);
    let mut out = Vec::new();
    let mut start = 0u64;
    while start < total_size {
        let end = start.saturating_add(chunk).saturating_sub(1).min(total_size - 1);
        out.push((start, end));
        start = end + 1;
    }
    out
}

#[cfg(test)]
mod planning_tests {
    use super::*;

    #[test]
    fn formula_clamps_to_policy() {
        // N=64 MiB, W=4, oversub 3 -> raw = ceil(64/12) ≈ 5.46 MiB, inside
        // [1, 64] MiB: unclamped.
        let (seg, _) = plan_segments(64 * 1024 * 1024, 4, 1024 * 1024, 64 * 1024 * 1024);
        assert_eq!(seg, 5_592_406); // ceil(67108864 / 12)
        // Tiny file: min segment clamps up.
        let (seg, ranges) = plan_segments(100, 8, 1024, 4096);
        assert_eq!(seg, 1024);
        assert_eq!(ranges, vec![(0, 99)]);
        // Huge worker count: raw below min -> min.
        let (seg, _) = plan_segments(1024, 128, 256, 1024);
        assert_eq!(seg, 256);
    }

    #[test]
    fn tiling_covers_exactly_once() {
        for total in [0u64, 1, 2, 1023, 1024, 1025, 4096, 1_000_003] {
            let (_, ranges) = plan_segments(total, 4, 1, 1024);
            let covered: u64 = ranges.iter().map(|(s, e)| e - s + 1).sum();
            assert_eq!(covered, total, "total {total}");
            for w in ranges.windows(2) {
                assert_eq!(w[1].0, w[0].1 + 1, "contiguous tiling for {total}");
            }
            if let Some((s, _)) = ranges.first() {
                assert_eq!(*s, 0);
            }
            if let Some((_, e)) = ranges.last() {
                assert_eq!(*e, total.saturating_sub(1));
            }
        }
    }

    #[test]
    fn empty_file_has_no_ranges() {
        let (seg, ranges) = plan_segments(0, 4, 1024, 4096);
        assert_eq!(seg, 0);
        assert!(ranges.is_empty());
    }

    #[test]
    fn boundary_sizes_tile_cleanly() {
        // Segment boundary sizes from §36.6.
        for total in [
            8 * 1024 * 1024 - 1,
            8 * 1024 * 1024,
            8 * 1024 * 1024 + 1,
        ] {
            let (seg, ranges) = plan_segments(total, 4, 1024 * 1024, 64 * 1024 * 1024);
            assert!((1024 * 1024..=64 * 1024 * 1024).contains(&seg));
            let covered: u64 = ranges.iter().map(|(s, e)| e - s + 1).sum();
            assert_eq!(covered, total);
        }
    }

    #[test]
    fn no_double_lease_property_on_plan() {
        // Plan ranges never overlap (they tile the domain).
        let (seg, ranges) = plan_segments(10 * 1024 * 1024, 6, 1024 * 1024, 32 * 1024 * 1024);
        assert!(seg >= 1024 * 1024);
        for w in ranges.windows(2) {
            assert_eq!(w[1].0, w[0].1 + 1);
        }
        assert_eq!(ranges.len() as u64, (10u64 * 1024 * 1024).div_ceil(seg));
    }
}