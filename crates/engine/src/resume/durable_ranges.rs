//! Durable-interval tracking (§15.4, task 4.3): a checkpoint may never
//! claim durability beyond what the selected policy has acknowledged.
//!
//! Two valid modes:
//! - Performance: ranges recorded once the OS acknowledges writes.
//! - Durable: ranges recorded only after an explicit fsync of the data.
//!
//! This module is the *ordering authority*: [`DurableRangeTracker::admissible_through`]
//! refuses to admit ranges beyond the acknowledged frontier, and the
//! controller consults `admissible()` before calling the store.

use crate::config::DurabilityMode as ConfigDurability;
use crate::resume::checkpoint::ByteRange;
use crate::resume::checkpoint_store::DurabilityMode;

/// Tracks which byte offsets are safe to record, per durability mode.
#[derive(Debug)]
pub struct DurableRangeTracker {
    mode: DurabilityMode,
    /// Frontier through which data is durably acknowledged:
    /// - Performance mode: advances on page-cache write acknowledgment.
    /// - Durable: advances only on fsync.
    acknowledged_through: u64,
}

impl DurableRangeTracker {
    #[must_use]
    pub fn new(mode: DurabilityMode) -> Self {
        Self {
            mode,
            acknowledged_through: 0,
        }
    }

    /// Report that writes through `offset` (exclusive) have been
    /// acknowledged at the page-cache level.
    pub fn page_cache_ack(&mut self, through: u64) {
        if through > self.acknowledged_through {
            self.acknowledged_through = through;
        }
    }

    /// Report an fsync of the whole prefix (§15.4 durable mode).
    pub fn fsync_through(&mut self, through: u64) {
        // fsync acknowledges everything written before it.
        self.page_cache_ack(through);
    }

    /// The highest offset this tracker may record as durable right now.
    #[must_use]
    pub fn admissible_through(&self) -> u64 {
        match self.mode {
            DurabilityMode::Performance => self.acknowledged_through,
            DurabilityMode::Durable => self.acknowledged_through,
        }
    }

    /// Clip a desired completed range to the admissible frontier
    /// (§15.4: never claim durability beyond acknowledged writes).
    ///
    /// Returns `None` when nothing from the range is admissible.
    #[must_use]
    pub fn clip(&self, range: ByteRange) -> Option<ByteRange> {
        let (start, end) = range;
        if self.acknowledged_through == 0 || start >= self.acknowledged_through {
            return None;
        }
        // Ranges are inclusive; admissible end is frontier - 1.
        let end = end.min(self.acknowledged_through - 1);
        if end < start {
            return None;
        }
        Some((start, end))
    }

    #[must_use]
    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    #[must_use]
    pub fn from_config(config: ConfigDurability) -> Self {
        Self::new(match config {
            ConfigDurability::Performance => DurabilityMode::Performance,
            ConfigDurability::Durable => DurabilityMode::Durable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn performance_mode_records_on_ack() {
        let mut t = DurableRangeTracker::new(DurabilityMode::Performance);
        t.page_cache_ack(1000);
        assert_eq!(
            t.clip((0, 999)),
            Some((0, 999)),
            "page-cache ack admits the range (performance mode)"
        );
        // Beyond the frontier must be clipped away.
        assert_eq!(t.clip((1000, 1999)), None);
        assert_eq!(t.clip((500, 1499)), Some((500, 999)));
    }

    #[test]
    fn frontier_never_recedes() {
        let mut t = DurableRangeTracker::new(DurabilityMode::Performance);
        t.page_cache_ack(500);
        t.page_cache_ack(100);
        assert_eq!(t.admissible_through(), 500);
    }

    #[test]
    fn empty_ranges_clipped_to_none() {
        let t = DurableRangeTracker::new(DurabilityMode::Performance);
        assert_eq!(t.clip((0, 10)), None, "nothing acknowledged yet");
    }

    #[test]
    fn config_mapping() {
        let t = DurableRangeTracker::from_config(ConfigDurability::Durable);
        assert_eq!(t.mode(), DurabilityMode::Durable);
    }
}
