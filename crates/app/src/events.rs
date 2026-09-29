//! Bounded supervisor event broadcast.
//!
//! Events are revisioned, display-safe job views. The channel is bounded:
//! a lagging subscriber is told to reconcile instead of growing an
//! unbounded buffer, and no durable replay log exists.

use crate::domain::JobView;
use tokio::sync::broadcast;

/// Events the supervisor publishes to API subscribers.
#[derive(Clone, Debug)]
pub enum SupervisorEvent {
    /// A complete display-safe view for one job at a revision.
    JobSnapshot(JobView),
}

/// Bounded broadcast broker for supervisor events.
#[derive(Clone, Debug)]
pub struct EventBroker {
    tx: broadcast::Sender<SupervisorEvent>,
}

impl EventBroker {
    /// Creates a broker with a bounded channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribes to the stream. Events published before this point are not
    /// replayed; subscribers reconcile from the collections endpoints.
    pub fn subscribe(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.tx.subscribe()
    }

    /// Publishes an event. A full channel drops the event for lagging
    /// receivers (they observe `Lagged` and reconcile); this never blocks
    /// the supervisor.
    pub fn publish(&self, event: SupervisorEvent) {
        let _ = self.tx.send(event);
    }
}
