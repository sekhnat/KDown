//! The `/api/v1/events` SSE stream.
//!
//! The handler subscribes to the bounded broker BEFORE producing the hello
//! event, so any event published after subscription but before the client's
//! authoritative collection fetch is delivered after the hello instead of
//! being lost. Telemetry snapshots are coalesced per job to at most one per
//! 250 ms window; lifecycle milestones (status changes) pass immediately.
//! Broker lag becomes an explicit `service.degraded` event. There is no
//! durable replay log.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::StreamExt;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

use crate::api::dto::EventEnvelopeDto;
use crate::api::AppState;

/// Per-connection coalescing state: the last instant and status each job
/// was emitted at on this stream.
#[derive(Default)]
struct SnapshotCoalescer {
    last_emitted: HashMap<
        crate::domain::JobId,
        (
            tokio::time::Instant,
            Option<crate::domain::DurableJobStatus>,
        ),
    >,
}

impl SnapshotCoalescer {
    /// Whether a snapshot may be emitted now: first sight always passes,
    /// status changes pass immediately, otherwise at most one per 250 ms.
    fn allow(&mut self, view: &crate::domain::JobView) -> bool {
        let now = tokio::time::Instant::now();
        match self.last_emitted.get_mut(&view.id) {
            Some((last, last_status)) => {
                let status_changed = *last_status != Some(view.status);
                let window_elapsed = now.duration_since(*last) >= Duration::from_millis(250);
                if status_changed || window_elapsed {
                    *last = now;
                    *last_status = Some(view.status);
                    true
                } else {
                    false
                }
            }
            None => {
                self.last_emitted.insert(view.id, (now, Some(view.status)));
                true
            }
        }
    }
}

fn to_sse(envelope: EventEnvelopeDto) -> Event {
    let kind = envelope.kind.clone();
    match serde_json::to_string(&envelope) {
        Ok(data) => Event::default().event(kind).data(data),
        Err(_) => Event::default()
            .event("service.degraded")
            .data(r#"{"kind":"service.degraded"}"#),
    }
}

/// The SSE stream: hello first, then revisioned events.
pub async fn stream(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    // Subscribe before creating the hello so nothing published between
    // subscription and the client's collection fetch is lost.
    let receiver = state.events.subscribe();
    let hello = EventEnvelopeDto::hello(state.stream_epoch.clone(), state.build.clone());
    let stream_epoch = state.stream_epoch.clone();
    let coalescer: Arc<Mutex<SnapshotCoalescer>> =
        Arc::new(Mutex::new(SnapshotCoalescer::default()));
    let events = BroadcastStream::new(receiver).filter_map(move |item| {
        let coalescer = Arc::clone(&coalescer);
        let stream_epoch = stream_epoch.clone();
        async move {
            match item {
                Ok(crate::events::SupervisorEvent::JobSnapshot(view)) => {
                    let allowed = coalescer.lock().unwrap().allow(&view);
                    if allowed {
                        let mut envelope = EventEnvelopeDto::job_snapshot(view);
                        envelope.stream_epoch = stream_epoch;
                        Some(Ok(to_sse(envelope)))
                    } else {
                        None
                    }
                }
                Err(BroadcastStreamRecvError::Lagged(_skipped)) => {
                    Some(Ok(to_sse(EventEnvelopeDto::service_degraded())))
                }
            }
        }
    });
    let stream = futures_util::stream::once(async move { Ok(to_sse(hello)) }).chain(events);
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
