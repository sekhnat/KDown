//! Segment leases with generation guards (§31, D7).

/// Lease identifier; unique per scheduler.
pub type LeaseId = u64;

/// A lease over one segment; `generation` invalidates stale callbacks
/// after reassignment/cancellation (§31: lease IDs/generation numbers
/// prevent stale worker callbacks from mutating newly reassigned ranges).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentLease {
    pub id: LeaseId,
    pub generation: u64,
    /// Inclusive start.
    pub start: u64,
    /// Inclusive end.
    pub end: u64,
    /// Worker-local next offset to fetch (§31 SegmentLease.next_offset).
    pub next_offset: u64,
}

impl SegmentLease {
    /// The next absolute offset to read from the network.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        if self.next_offset > self.end {
            0
        } else {
            self.end - self.next_offset + 1
        }
    }

    /// Whether this lease matches the given generation stamp.
    #[must_use]
    pub fn is_current(&self, generation: u64) -> bool {
        self.generation == generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_accounting() {
        let l = SegmentLease {
            id: 1,
            generation: 0,
            start: 100,
            end: 199,
            next_offset: 100,
        };
        assert_eq!(l.remaining(), 100);
        let advanced = SegmentLease {
            next_offset: 150,
            ..l
        };
        assert_eq!(advanced.remaining(), 50);
        let done = SegmentLease {
            next_offset: 200,
            ..l
        };
        assert_eq!(done.remaining(), 0);
        let past = SegmentLease {
            next_offset: 250,
            ..l
        };
        assert_eq!(past.remaining(), 0, "no negative remaining");
    }
}
