//! Normalized interval set over inclusive byte ranges (§12.1, D7).
//!
//! Backed by a `BTreeMap<u64, u64>` (start -> inclusive end). All
//! operations preserve the invariants: non-overlapping, sorted, no
//! adjacent-unmerged ranges.

use crate::resume::checkpoint::ByteRange;
use std::collections::BTreeMap;

/// Normalized set of inclusive byte ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntervalSet {
    map: BTreeMap<u64, u64>,
}

impl IntervalSet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from (possibly unsorted/overlapping) ranges.
    #[must_use]
    pub fn from_ranges(ranges: &[ByteRange]) -> Self {
        let mut s = Self::new();
        for &(a, b) in ranges {
            s.insert(a, b);
        }
        s
    }

    /// Insert `[start, end]` and renormalize (merge overlapping/adjacent).
    pub fn insert(&mut self, start: u64, end: u64) {
        if end < start {
            return; // empty/invalid range contributes nothing
        }
        let lower = self.map.range(..=start).next_back().map(|(k, v)| (*k, *v));
        let mut lo = start;
        let mut hi = end;
        // Absorb the predecessor when it overlaps or touches.
        if let Some((k, v)) = lower {
            if v >= start.saturating_sub(1) {
                self.map.remove(&k);
                lo = lo.min(k);
                hi = hi.max(v);
            }
        }
        // Absorb successors while they overlap or touch.
        loop {
            let next = self.map.range(lo..).next().map(|(k, v)| (*k, *v));
            match next {
                Some((k, v)) if k <= hi.saturating_add(1) => {
                    self.map.remove(&k);
                    hi = hi.max(v);
                    lo = lo.min(k);
                }
                _ => break,
            }
        }
        self.map.insert(lo, hi);
    }

    /// Remove `[start, end]` (subtract); used to consume pending work.
    pub fn subtract(&mut self, start: u64, end: u64) {
        if end < start {
            return;
        }
        // Find entries overlapping the removal window.
        let mut to_split: Vec<(u64, u64)> = vec![];
        for (k, v) in self.map.range(start..=end) {
            to_split.push((*k, *v));
        }
        // Also the entry that spans across `start` from below.
        if let Some((k, v)) = self.map.range(..start).next_back() {
            if *v >= start {
                to_split.push((*k, *v));
            }
        }
        for (k, v) in to_split {
            self.map.remove(&k);
            if k < start {
                self.map.insert(k, start - 1);
            }
            if v > end {
                self.map.insert(end + 1, v);
            }
        }
    }

    /// Whether the byte `offset` is covered.
    #[must_use]
    pub fn contains(&self, offset: u64) -> bool {
        self.map
            .range(..=offset)
            .next_back()
            .map(|(k, v)| *k <= offset && offset <= *v)
            .unwrap_or(false)
    }

    /// First uncovered byte at or after `start` within `limit` (inclusive).
    #[must_use]
    pub fn first_gap_from(&self, start: u64, limit: u64) -> Option<u64> {
        if start > limit {
            return None;
        }
        // If covered at start, jump past that range's end.
        if self.contains(start) {
            let (_, &v) = self.map.range(..=start).next_back()?;
            if v >= limit {
                return None;
            }
            return Some(v + 1);
        }
        Some(start)
    }

    /// Inclusive total byte count (§19.1 unique completed bytes).
    #[must_use]
    pub fn len(&self) -> u64 {
        self.map.iter().map(|(k, v)| v - k + 1).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Sorted inclusive ranges (§15.2 checkpoint serialization shape).
    #[must_use]
    pub fn ranges(&self) -> Vec<ByteRange> {
        self.map.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// The greatest covered end, if any.
    #[must_use]
    pub fn max_end(&self) -> Option<u64> {
        self.map.values().max().copied()
    }

    /// Whether the full domain `[0, total-1]` is covered.
    #[must_use]
    pub fn covers(&self, total: u64) -> bool {
        total == 0 || self.len() == total
    }

    /// Union coverage of two sets (used by scheduler merging).
    pub fn union(&mut self, other: &IntervalSet) {
        for (k, v) in other.map.iter() {
            self.insert(*k, *v);
        }
    }

    /// Complement within `[0, total-1]`.
    #[must_use]
    pub fn complement_within(&self, total: u64) -> Vec<ByteRange> {
        let mut out = vec![];
        let mut next = 0u64;
        for (k, v) in &self.map {
            if *k > next {
                out.push((next, k - 1));
            }
            next = v.saturating_add(1);
        }
        if next < total {
            out.push((next, total - 1));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_normalize() {
        let mut s = IntervalSet::new();
        s.insert(400, 499);
        s.insert(0, 99);
        s.insert(100, 199); // adjacent -> merge
        s.insert(490, 599); // overlap -> merge
        assert_eq!(s.ranges(), vec![(0, 199), (400, 599)]);
        assert_eq!(s.len(), 400);
    }

    #[test]
    fn subtract_splits() {
        let mut s = IntervalSet::from_ranges(&[(0, 999)]);
        s.subtract(100, 199);
        assert_eq!(s.ranges(), vec![(0, 99), (200, 999)]);
        s.subtract(0, 49);
        assert_eq!(s.ranges(), vec![(50, 99), (200, 999)]);
        s.subtract(900, 999);
        assert_eq!(s.ranges(), vec![(50, 99), (200, 899)]);
        // Remove a full middle range.
        s.subtract(50, 99);
        assert_eq!(s.ranges(), vec![(200, 899)]);
    }

    #[test]
    fn contains_queries() {
        let s = IntervalSet::from_ranges(&[(10, 19), (30, 39)]);
        assert!(s.contains(10));
        assert!(s.contains(19));
        assert!(!s.contains(20));
        assert!(s.contains(35));
        assert!(!s.contains(29));
        assert!(!s.contains(0));
    }

    #[test]
    fn first_gap_from_skips_ranges() {
        let s = IntervalSet::from_ranges(&[(10, 19)]);
        assert_eq!(s.first_gap_from(0, 100), Some(0));
        assert_eq!(s.first_gap_from(10, 100), Some(20));
        assert_eq!(s.first_gap_from(95, 100), Some(95));
        // Fully covered.
        let full = IntervalSet::from_ranges(&[(0, 99)]);
        assert_eq!(full.first_gap_from(0, 99), None);
    }

    #[test]
    fn complement_matches_checkpoint_shape() {
        let s = IntervalSet::from_ranges(&[(0, 99), (400, 499)]);
        assert_eq!(s.complement_within(1000), vec![(100, 399), (500, 999)]);
        assert_eq!(IntervalSet::new().complement_within(10), vec![(0, 9)]);
        let full = IntervalSet::from_ranges(&[(0, 9)]);
        assert_eq!(full.complement_within(10), vec![]);
    }

    #[test]
    fn covers_full_domain() {
        let mut s = IntervalSet::new();
        assert!(!s.covers(10));
        s.insert(0, 9);
        assert!(s.covers(10));
        assert!(s.covers(0), "empty domain trivially covered");
    }

    // Property tests (§36.4) live in tests/scheduler_property_tests.rs.
}
