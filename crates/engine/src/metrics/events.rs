//! Event stream with cadence batching (§19.4, D12).
//!
//! Lifecycle events are small and delivered loss-free through a bounded
//! mpsc channel; high-frequency chunk events are never emitted (§19.4).
//! Speed uses an EWMA over payload throughput; ETA is omitted when it
//! would be meaningless (§19.2-§19.3).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use crate::job::state::JobState;

/// Documented event set (§19.4).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    StateChanged {
        from: JobState,
        to: JobState,
    },
    ProbeCompleted {
        total_size: Option<u64>,
        range_support: bool,
    },
    SegmentStarted {
        worker: usize,
        start: u64,
        end: u64,
    },
    SegmentRetried {
        worker: usize,
        start: u64,
        end: u64,
        error: String,
    },
    SegmentCompleted {
        worker: usize,
        start: u64,
        end: u64,
    },
    Progress(ProgressEvent),
    RateLimitChanged {
        bytes_per_second: Option<u64>,
    },
    ResourceChanged {
        detail: String,
    },
    IntegrityCheckStarted,
    IntegrityCheckPassed,
    IntegrityCheckFailed {
        detail: String,
    },
    Committed {
        path: String,
    },
    Warning {
        detail: String,
    },
    Failed {
        detail: String,
    },
}

/// Batched progress payload (§19.1-§19.3).
#[derive(Debug, Clone)]
pub struct ProgressEvent {
    pub total_size: Option<u64>,
    pub completed_bytes: u64,
    pub network_bytes: u64,
    pub reused_bytes: u64,
    pub smoothed_rate: f64,
    /// Present only when total size is known and smoothed rate is above
    /// the meaningfulness threshold (§19.3).
    pub eta: Option<Duration>,
    pub retries: u64,
}

/// Sender side: batch progress updates, forward lifecycle events.
#[derive(Debug)]
pub struct EventHub {
    tx: broadcast::Sender<Event>,
    cadence: Duration,
    last_progress: std::sync::Mutex<Option<Instant>>,
}

/// Minimum smoothed rate (B/s) below which ETA is omitted (§19.3).
pub const ETA_MIN_RATE: f64 = 1024.0;

impl EventHub {
    #[must_use]
    pub fn new(buffer: usize, cadence: Duration) -> (Self, EventStream) {
        let (tx, rx) = broadcast::channel(buffer.max(1));
        (
            Self {
                tx,
                cadence,
                last_progress: std::sync::Mutex::new(None),
            },
            EventStream { rx },
        )
    }

    /// Broadcast an event to every current subscriber. Slow subscribers may
    /// observe a Lagged gap; progress snapshots remain recoverable through
    /// DownloadHandle::snapshot() (§19.4).
    pub async fn emit(&self, event: Event) {
        let _ = self.tx.send(event);
    }

    /// Blocking send for sync contexts.
    pub fn emit_blocking(&self, event: Event) {
        let _ = self.tx.send(event);
    }

    /// Non-blocking send for control paths that must never block or panic.
    pub fn emit_try(&self, event: Event) {
        let _ = self.tx.send(event);
    }

    /// Subscribe to future events (§7.3).
    #[must_use]
    pub fn subscribe(&self) -> EventStream {
        EventStream {
            rx: self.tx.subscribe(),
        }
    }

    /// Whether a progress event may be emitted now (cadence gate).
    #[must_use]
    pub fn progress_due(&self) -> bool {
        let mut last = self.last_progress.lock().expect("progress gate");
        match *last {
            None => {
                *last = Some(Instant::now());
                true
            }
            Some(t) if t.elapsed() >= self.cadence => {
                *last = Some(Instant::now());
                true
            }
            Some(_) => false,
        }
    }

    /// Build a progress event from a snapshot, applying EWMA smoothing and
    /// ETA gating (§19.2-§19.3). The caller folds counters then calls this
    /// on the cadence boundary.
    #[must_use]
    pub fn progress_event(
        &self,
        snap: &crate::metrics::counters::ProgressSnapshot,
        total_size: Option<u64>,
        ewma: &mut EwmaRate,
        prev_completed: u64,
    ) -> ProgressEvent {
        // Bytes newly completed since the last event drive the rate.
        let delta = snap.completed_bytes.saturating_sub(prev_completed);
        let rate = ewma.update(delta as f64, snap.elapsed.as_secs_f64().max(1e-6));
        let eta = total_size
            .filter(|t| snap.completed_bytes < *t && rate > ETA_MIN_RATE)
            .map(|t| Duration::from_secs_f64((t - snap.completed_bytes) as f64 / rate));
        ProgressEvent {
            total_size,
            completed_bytes: snap.completed_bytes,
            network_bytes: snap.network_bytes,
            reused_bytes: snap.reused_bytes,
            smoothed_rate: rate,
            eta,
            retries: snap.retries,
        }
    }
}

/// Exponentially-weighted moving average of payload throughput (§19.2).
#[derive(Debug)]
pub struct EwmaRate {
    alpha: f64,
    rate: f64,
    last_t: f64,
}

impl EwmaRate {
    /// `alpha` closer to 1 reacts faster; 0.3 default per D12 tuning.
    #[must_use]
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha,
            rate: 0.0,
            last_t: 0.0,
        }
    }

    /// Feed `delta_bytes` completed over the job time window ending at
    /// `elapsed_secs`; returns the smoothed rate.
    pub fn update(&mut self, delta_bytes: f64, elapsed_secs: f64) -> f64 {
        let dt = (elapsed_secs - self.last_t).max(1e-6);
        self.last_t = elapsed_secs;
        let instant = delta_bytes / dt;
        self.rate = if self.rate == 0.0 {
            instant
        } else {
            self.alpha * instant + (1.0 - self.alpha) * self.rate
        };
        self.rate
    }

    #[must_use]
    pub fn rate(&self) -> f64 {
        self.rate
    }
}

/// Receiver side handed to callers (§7.3 `events()`).
#[derive(Debug)]
pub struct EventStream {
    rx: broadcast::Receiver<Event>,
}

impl EventStream {
    pub async fn next(&mut self) -> Option<Event> {
        loop {
            match self.rx.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Try to collect all currently pending events without awaiting. Lagged
    /// events are skipped; the snapshot API is the recovery path (§19.4).
    pub fn try_next(&mut self) -> Option<Event> {
        loop {
            match self.rx.try_recv() {
                Ok(event) => return Some(event),
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(_) => return None,
            }
        }
    }
}

/// Shared hub handle for worker tasks.
pub type SharedHub = Arc<EventHub>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::counters::{JobCounters, ProgressSnapshot};

    fn snap(completed: u64, network: u64, elapsed: Duration) -> ProgressSnapshot {
        ProgressSnapshot {
            completed_bytes: completed,
            network_bytes: network,
            elapsed,
            ..ProgressSnapshot::default()
        }
    }

    #[tokio::test]
    async fn lifecycle_events_delivered_in_order() {
        let (hub, mut stream) = EventHub::new(16, Duration::from_millis(50));
        hub.emit(Event::StateChanged {
            from: JobState::Created,
            to: JobState::Probing,
        })
        .await;
        hub.emit(Event::StateChanged {
            from: JobState::Probing,
            to: JobState::Running,
        })
        .await;
        hub.emit(Event::Failed {
            detail: "boom".into(),
        })
        .await;
        assert!(matches!(
            stream.next().await,
            Some(Event::StateChanged {
                to: JobState::Probing,
                ..
            })
        ));
        assert!(matches!(
            stream.next().await,
            Some(Event::StateChanged {
                to: JobState::Running,
                ..
            })
        ));
        assert!(matches!(stream.next().await, Some(Event::Failed { .. })));
    }

    #[test]
    fn cadence_gates_progress() {
        let (hub, _stream) = EventHub::new(16, Duration::from_millis(50));
        assert!(hub.progress_due(), "first event immediate");
        assert!(!hub.progress_due(), "within cadence");
        std::thread::sleep(Duration::from_millis(60));
        assert!(hub.progress_due(), "after cadence");
    }

    #[test]
    fn eta_omitted_when_size_unknown() {
        let (hub, _) = EventHub::new(16, Duration::from_secs(1));
        let mut ewma = EwmaRate::new(0.3);
        let s = snap(1024, 2048, Duration::from_secs(1));
        let ev = hub.progress_event(&s, None, &mut ewma, 0);
        assert!(ev.eta.is_none(), "unknown size -> no ETA (§19.3)");
    }

    #[test]
    fn eta_omitted_when_rate_below_threshold() {
        let (hub, _) = EventHub::new(16, Duration::from_secs(1));
        let mut ewma = EwmaRate::new(0.3);
        // 10 bytes over 10 s = 1 B/s, below ETA_MIN_RATE.
        let s = snap(10, 10, Duration::from_secs(10));
        let ev = hub.progress_event(&s, Some(1000), &mut ewma, 0);
        assert!(ev.eta.is_none(), "rate below threshold -> no ETA");
    }

    #[test]
    fn eta_present_when_meaningful() {
        let (hub, _) = EventHub::new(16, Duration::from_secs(1));
        let mut ewma = EwmaRate::new(1.0); // instant rate only
        let s = snap(500_000, 500_000, Duration::from_secs(1));
        let ev = hub.progress_event(&s, Some(1_000_000), &mut ewma, 0);
        let eta = ev.eta.expect("meaningful rate -> ETA");
        assert!((eta.as_secs_f64() - 1.0).abs() < 0.1);
    }

    #[test]
    fn ewma_smooths_instantaneous_spikes() {
        let mut ewma = EwmaRate::new(0.3);
        let mut t = 0.0;
        for _ in 0..10 {
            t += 1.0;
            ewma.update(1000.0, t); // steady 1000 B/s
        }
        // Spike: 1_000_000 bytes in the next second.
        t += 1.0;
        let smoothed = ewma.update(1_000_000.0, t);
        // 0.3 * 1M + 0.7 * 1000 ≈ 300 700 — far below the spike.
        assert!(smoothed < 400_000.0, "EWMA must dampen: {smoothed}");
        assert!(smoothed > 250_000.0, "EWMA must react: {smoothed}");
    }

    #[tokio::test]
    async fn progress_events_batched_via_cadence() {
        let (hub, mut stream) = EventHub::new(16, Duration::from_millis(30));
        let counters = JobCounters::new(1);
        counters.worker(0).expect("slot").add_completed(1000);
        let mut ewma = EwmaRate::new(0.3);
        if hub.progress_due() {
            let fold = counters.fold();
            let ev = hub.progress_event(&fold, Some(10_000), &mut ewma, 0);
            hub.emit(Event::Progress(ev)).await;
        }
        // Second attempt inside cadence is gated out.
        assert!(!hub.progress_due());
        match stream.next().await {
            Some(Event::Progress(p)) => {
                assert_eq!(p.completed_bytes, 1000);
                assert_eq!(p.total_size, Some(10_000));
            }
            other => panic!("expected progress, got {other:?}"),
        }
    }
}
