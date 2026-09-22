//! Cooperative cancellation tokens (§13 step 9, §23, §9.2 invariant 8:
//! cancellation eventually prevents further network reads and sink writes).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A cancellation token shared between a handle and workers.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    cancelled: AtomicBool,
    /// Separate pause flag: pause must stop network reads (§9.3) but is
    /// distinct from terminal cancellation.
    paused: AtomicBool,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Trigger cancellation; idempotent.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
    }

    /// True once cancelled; latches forever.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Request cooperative pause.
    pub fn pause(&self) {
        self.inner.paused.store(true, Ordering::SeqCst);
    }

    /// Lift a pause.
    pub fn unpause(&self) {
        self.inner.paused.store(false, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.inner.paused.load(Ordering::SeqCst)
    }

    /// Resolve when cancelled (or paused, optionally) — async wait point
    /// for workers between chunk reads/writes.
    pub async fn cancelled_or_paused(&self) -> CancellationReason {
        loop {
            if self.is_cancelled() {
                return CancellationReason::Cancelled;
            }
            if self.is_paused() {
                return CancellationReason::Paused;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// True when the worker should stop issuing new reads now.
    #[must_use]
    pub fn should_stop(&self) -> bool {
        self.is_cancelled() || self.is_paused()
    }
}

/// Why a worker stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    Cancelled,
    Paused,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_latches() {
        let t = CancellationToken::new();
        assert!(!t.is_cancelled());
        t.cancel();
        assert!(t.is_cancelled());
        t.cancel(); // idempotent
        assert!(t.is_cancelled());
    }

    #[test]
    fn pause_unpause_cycle() {
        let t = CancellationToken::new();
        t.pause();
        assert!(t.is_paused());
        assert!(t.should_stop());
        t.unpause();
        assert!(!t.is_paused());
        assert!(!t.should_stop());
    }

    #[test]
    fn cancellation_is_cloned_state() {
        let t = CancellationToken::new();
        let t2 = t.clone();
        t2.cancel();
        assert!(t.is_cancelled(), "clones share state");
    }

    #[tokio::test]
    async fn cancelled_or_paused_resolves() {
        let t = CancellationToken::new();
        let t2 = t.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            t2.pause();
        });
        let reason = tokio::time::timeout(std::time::Duration::from_secs(2), t.cancelled_or_paused())
            .await
            .expect("resolves")
            ;
        assert_eq!(reason, CancellationReason::Paused);
    }
}