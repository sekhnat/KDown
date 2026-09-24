//! Cooperative cancellation tokens (§13 step 9, §23, §9.2 invariant 8:
//! cancellation eventually prevents further network reads and sink writes).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A cancellation token shared between a handle and workers.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    cancelled: AtomicBool,
    /// Separate pause flag: pause must stop network reads (§9.3) but is
    /// distinct from terminal cancellation.
    paused: AtomicBool,
    /// Versioned state signal (task 7.2): 0 = running, 1 = paused,
    /// 2 = cancelled. Published AFTER every flag change so parked waiters
    /// wake on transitions without fixed-duration polling. The atomics stay
    /// the synchronous fast path; the watch is the wake mechanism.
    state_tx: tokio::sync::watch::Sender<u8>,
}

impl Default for Inner {
    fn default() -> Self {
        let (state_tx, _) = tokio::sync::watch::channel(0u8);
        Self {
            cancelled: AtomicBool::default(),
            paused: AtomicBool::default(),
            state_tx,
        }
    }
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Trigger cancellation; idempotent.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        self.publish_state(2);
    }

    /// Publish the versioned state after a flag change (task 7.2): waiters
    /// registered before the change are woken.
    fn publish_state(&self, state: u8) {
        let _ = self.inner.state_tx.send(state);
    }

    /// True once cancelled; latches forever.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Request cooperative pause.
    pub fn pause(&self) {
        self.inner.paused.store(true, Ordering::SeqCst);
        self.publish_state(1);
    }

    /// Lift a pause.
    pub fn unpause(&self) {
        self.inner.paused.store(false, Ordering::SeqCst);
        self.publish_state(0);
    }

    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.inner.paused.load(Ordering::SeqCst)
    }

    /// Resolve when cancelled (or paused, optionally) — async wait point
    /// for workers between chunk reads/writes. Watches the versioned state
    /// signal: register-before-check, then wait for the next transition —
    /// no fixed-duration polling (task 7.2).
    pub async fn cancelled_or_paused(&self) -> CancellationReason {
        let mut rx = self.inner.state_tx.subscribe();
        loop {
            // Mark the current version seen FIRST, then check the
            // authoritative atomics: a change between the two is published
            // after this mark, so the wait below wakes immediately.
            {
                let _seen = rx.borrow_and_update();
            } // guard dropped: the version is marked seen
            if self.is_cancelled() {
                return CancellationReason::Cancelled;
            }
            if self.is_paused() {
                return CancellationReason::Paused;
            }
            if rx.changed().await.is_err() {
                // All senders gone (token dropped): recheck the atomics.
                return if self.is_cancelled() {
                    CancellationReason::Cancelled
                } else {
                    CancellationReason::Paused
                };
            }
        }
    }

    /// Resolve only on terminal cancellation (not pause) — the parked-worker
    /// wait (task 7.2): wakes via the versioned state signal, no polling.
    pub async fn cancelled(&self) {
        let mut rx = self.inner.state_tx.subscribe();
        loop {
            {
                let _seen = rx.borrow_and_update();
            }
            if self.is_cancelled() {
                return;
            }
            if rx.changed().await.is_err() {
                // Senders gone: settle on the atomic truth.
                while !self.is_cancelled() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                return;
            }
        }
    }

    /// Wait until the pause lifts (`true`) or the token is cancelled
    /// (`false`) — the parked-worker resume wait (task 7.2): wakes on
    /// unpause/cancel via the versioned signal, never by fixed polling.
    pub async fn wait_for_resume(&self) -> bool {
        let mut rx = self.inner.state_tx.subscribe();
        loop {
            {
                let _seen = rx.borrow_and_update();
            } // guard dropped: the version is marked seen
            if self.is_cancelled() {
                return false;
            }
            if !self.is_paused() {
                return true;
            }
            if rx.changed().await.is_err() {
                return !self.is_paused() || self.is_cancelled();
            }
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
        let reason =
            tokio::time::timeout(std::time::Duration::from_secs(2), t.cancelled_or_paused())
                .await
                .expect("resolves");
        assert_eq!(reason, CancellationReason::Paused);
    }
}
