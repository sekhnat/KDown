//! Bounded long-lived blocking writer lanes (task 2.3, design D1).
//!
//! Each active segmented worker owns one long-lived `spawn_blocking` lane
//! (not one per chunk): the worker submits an owned [`Bytes`] chunk and its
//! absolute offset, then awaits the acknowledgment before publishing progress
//! or reading another chunk — one outstanding payload per worker, so retained
//! memory stays bounded. Lanes write concurrently to the same file because
//! every operation is positional. Lanes are shut down and joined before the
//! output owner reclaims exclusive access.

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::output_session::OutputWriteHandle;
use super::sink::SinkError;
use crate::error::DownloadError;

enum LaneRequest {
    Write {
        offset: u64,
        data: Bytes,
        ack: oneshot::Sender<Result<(), SinkError>>,
    },
}

/// One worker's blocking write lane. Owned by the job coordinator
/// (`run_segmented`), which shuts it down and joins it before reclaiming the
/// output; workers submit writes through a cloneable [`LaneHandle`].
pub(crate) struct WriterLane {
    tx: mpsc::Sender<LaneRequest>,
    join: tokio::task::JoinHandle<()>,
}

/// Cloneable submit end of one writer lane; workers hold this.
#[derive(Clone)]
pub(crate) struct LaneHandle {
    tx: mpsc::Sender<LaneRequest>,
}

impl LaneHandle {
    /// Submit one payload chunk at its absolute offset and await the
    /// acknowledgment. The worker must not publish progress or read the next
    /// chunk before this resolves — one outstanding payload per worker
    /// (task 2.3), so retained memory is one chunk.
    ///
    /// # Errors
    /// Structured sink error from the write, or a lane-closed error when the
    /// lane shut down.
    pub(crate) async fn write(&self, offset: u64, data: Bytes) -> Result<(), SinkError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(LaneRequest::Write {
                offset,
                data,
                ack: ack_tx,
            })
            .await
            .map_err(|_| lane_closed_error())?;
        ack_rx.await.map_err(|_| lane_closed_error())?
    }
}

impl WriterLane {
    /// Spawn the long-lived blocking lane over one write-only capability.
    /// The capability moves into the lane and stays there until shutdown,
    /// which is what keeps `reclaim_exclusive` fail-closed while writing.
    pub(crate) fn spawn(handle: OutputWriteHandle) -> Self {
        let (tx, mut rx) = mpsc::channel::<LaneRequest>(1);
        let join = tokio::task::spawn_blocking(move || {
            while let Some(request) = rx.blocking_recv() {
                match request {
                    LaneRequest::Write {
                        offset,
                        data,
                        ack,
                    } => {
                        let result = handle.write_blocking(offset, &data);
                        // The awaiting worker owns the other side; a dropped
                        // ack (worker gone) just discards the result.
                        let _ = ack.send(result);
                    }
                }
            }
            // tx dropped: lane exits, dropping the capability afterwards.
        });
        Self { tx, join }
    }

    /// The cloneable submit end for one worker.
    #[must_use]
    pub(crate) fn handle(&self) -> LaneHandle {
        LaneHandle {
            tx: self.tx.clone(),
        }
    }

    /// Stop accepting writes and join the lane. Resolves only after the last
    /// in-flight write completed, so callers may reclaim the output owner
    /// afterwards.
    ///
    /// # Errors
    /// Sink error when the lane task panicked.
    pub(crate) async fn shutdown(self) -> Result<(), SinkError> {
        let WriterLane { tx, join } = self;
        // Dropping the sender closes the channel: queued requests drain (an
        // in-flight write always completes and acks) before `blocking_recv`
        // yields `None` and the lane exits.
        drop(tx);
        join.await.map_err(|join_err| {
            SinkError(DownloadError::SinkWrite(format!(
                "writer lane failed: {join_err}"
            )))
        })?;
        Ok(())
    }
}

fn lane_closed_error() -> SinkError {
    SinkError(DownloadError::SinkWrite(
        "writer lane shut down before acknowledging the write".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::output_session::OutputSession;
    use crate::io::sink::TempFileSpec;

    fn session(directory: &tempfile::TempDir) -> OutputSession {
        OutputSession::create(
            &directory.path().join("out.bin"),
            &TempFileSpec::default(),
            false,
            false,
            None,
        )
        .expect("session")
    }

    #[tokio::test]
    async fn lanes_write_disjoint_ranges_out_of_order() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut session = session(&directory);
        let mut handles = session.share_write_handles(3).expect("share");
        let lane_c = WriterLane::spawn(handles.pop().expect("c"));
        let lane_b = WriterLane::spawn(handles.pop().expect("b"));
        let lane_a = WriterLane::spawn(handles.pop().expect("a"));
        let (hc, hb, ha) = (lane_c.handle(), lane_b.handle(), lane_a.handle());

        // Submit out of order and unaligned; each lane writes concurrently.
        let (rc, rb, ra) = tokio::join!(
            hc.write(11, Bytes::from_static(b"segment-c")),
            hb.write(5, Bytes::from_static(b"-seg-b")),
            ha.write(0, Bytes::from_static(b"seg-a")),
        );
        rc.expect("c");
        rb.expect("b");
        ra.expect("a");
        // Drop the submit handles so the channels close and lanes can exit.
        drop(hc);
        drop(hb);
        drop(ha);

        // Join lanes before reclaim (design D1).
        lane_a.shutdown().await.expect("join a");
        lane_b.shutdown().await.expect("join b");
        lane_c.shutdown().await.expect("join c");
        session.reclaim_exclusive().expect("reclaim after lanes join");
        assert_eq!(
            std::fs::read(session.temp_path()).expect("content"),
            b"seg-a-seg-bsegment-c"
        );
    }

    #[tokio::test]
    async fn lane_acknowledges_before_worker_progresses() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut session = session(&directory);
        let mut handles = session.share_write_handles(1).expect("share");
        let lane = WriterLane::spawn(handles.pop().expect("h"));
        let handle = lane.handle();
        // Sequence: submit, await, submit again — the one-item queue keeps
        // at most one outstanding payload per lane.
        handle.write(0, Bytes::from_static(b"one")).await.expect("w1");
        handle.write(3, Bytes::from_static(b"two")).await.expect("w2");
        drop(handle);
        lane.shutdown().await.expect("join");
        session.reclaim_exclusive().expect("reclaim");
        assert_eq!(
            std::fs::read(session.temp_path()).expect("content"),
            b"onetwo"
        );
    }

    /// Sequential writes through one lane land in submission order; the
    /// lane is shut down (joined) before the owner reclaims.
    #[tokio::test]
    async fn lane_processes_writes_in_submission_order_then_joins() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut session = session(&directory);
        let mut handles = session.share_write_handles(1).expect("share");
        let lane = WriterLane::spawn(handles.pop().expect("h"));
        let handle = lane.handle();
        let lane = tokio::spawn(async move {
            handle.write(0, Bytes::from_static(b"first"))
                .await
                .expect("w1");
            handle.write(5, Bytes::from_static(b"second"))
                .await
                .expect("w2");
            drop(handle);
            lane // return the lane for shutdown/join
        })
        .await
        .expect("worker task");
        lane.shutdown().await.expect("join");
        session.reclaim_exclusive().expect("reclaim after join");
        assert_eq!(
            std::fs::read(session.temp_path()).expect("content"),
            b"firstsecond"
        );
    }

}
