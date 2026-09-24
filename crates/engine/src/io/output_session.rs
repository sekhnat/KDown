//! Crate-private owner for one temporary output's open handle and partial-file policy.
//!
//! The session is the sole lifecycle owner of the temp file (design D1,
//! task 2.2): it prepares, synchronizes, aborts and publishes. During
//! segmented transfer it lends bounded, cloneable **write-only** capabilities
//! over the shared immutable file handle — each capability writes complete
//! buffers at absolute offsets through the checked positional adapter, with
//! no shared cursor, no output-wide lock and no flush authority. The owner
//! cannot reclaim, close or publish until every writer capability has been
//! dropped (`reclaim_exclusive` fails closed otherwise).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::positional::write_all_at;
use super::sink::{AbortDisposition, FileSink, FlushLevel, Sink, SinkError, TempFileSpec};
use crate::error::DownloadError;
use crate::io::publish::PublishMode;

#[cfg(test)]
use super::fault_script::{self, OutputFaultScript, OutputOperation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartialArtifactDisposition {
    DeleteOnDrop,
    PreserveOnDrop,
}

/// Owns the temporary file from preparation through publication or disposition.
///
/// A fresh session removes its partial file on drop. A reopened session keeps
/// existing bytes unless the caller explicitly aborts it. During segmented
/// transfer it lends write-only capabilities to the configured worker set,
/// then regains exclusive access after all capabilities have been dropped.
pub(crate) struct OutputSession {
    sink: Option<FileSink>,
    /// Present while write-only capabilities are outstanding. Blocks every
    /// exclusive operation until `reclaim_exclusive` observes zero writers.
    shared: Option<SharedOutput>,
    temp_path: PathBuf,
    disposition: PartialArtifactDisposition,
    #[cfg(test)]
    script: Option<Arc<OutputFaultScript>>,
}

/// Lend-state: the live-writer counter gates exclusive operations until
/// every capability is dropped.
struct SharedOutput {
    writers_alive: Arc<AtomicUsize>,
}

impl std::fmt::Debug for SharedOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedOutput")
            .field("writers_alive", &self.writers_alive.load(Ordering::SeqCst))
            .finish()
    }
}

/// A worker-scoped, clonable write-only capability over the shared handle
/// (task 2.2). It can positionally write complete buffers at absolute
/// offsets; it cannot flush, synchronize, resize, abort or publish.
/// Dropping the capability releases its lease on the handle.
pub(crate) struct OutputWriteHandle {
    file: Arc<std::fs::File>,
    writers_alive: Arc<AtomicUsize>,
    #[cfg(test)]
    script: Option<Arc<OutputFaultScript>>,
}

impl Drop for OutputWriteHandle {
    fn drop(&mut self) {
        self.writers_alive.fetch_sub(1, Ordering::SeqCst);
    }
}

impl OutputWriteHandle {
    /// Synchronous positional write used by blocking writer lanes (task 2.3).
    /// Scripted output faults apply at this capability boundary.
    ///
    /// # Errors
    /// Structured sink error on write failure.
    pub(crate) fn write_blocking(&self, offset: u64, bytes: &[u8]) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Write).map_err(SinkError)?;
        }
        write_all_at(self.file.as_ref(), offset, bytes).map_err(SinkError::from)
    }
}

/// Synchronization capability over the shared output file (task 3.3).
/// Does not move any cursor and does not require exclusivity: concurrent
/// positional writers keep writing while the sync flushes everything
/// acknowledged so far.
#[derive(Clone)]
pub(crate) struct OutputSyncCapability {
    file: Arc<std::fs::File>,
    #[cfg(test)]
    script: Option<Arc<OutputFaultScript>>,
}

impl std::fmt::Debug for OutputSyncCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputSyncCapability").finish_non_exhaustive()
    }
}

impl OutputSyncCapability {
    /// Synchronize the output file data (`sync_all`, matching the durable
    /// `FlushLevel::FsyncFile` semantics of the sequential path).
    ///
    /// # Errors
    /// Structured sink error when the synchronization fails; the caller must
    /// not persist checkpoint coverage of unsynchronized data.
    pub(crate) fn sync_data(&self) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Flush).map_err(SinkError)?;
        }
        self.file
            .sync_all()
            .map_err(|e| SinkError(DownloadError::from_io(&e)))
    }
}

pub(crate) trait PartialArtifactOwner {
    fn preserve_partial(&mut self);
}

impl PartialArtifactOwner for FileSink {
    fn preserve_partial(&mut self) {
        self.set_keep_on_drop(true);
    }
}

impl PartialArtifactOwner for OutputSession {
    fn preserve_partial(&mut self) {
        OutputSession::preserve_partial(self);
    }
}

impl OutputSession {
    /// Create and prepare a fresh temporary output.
    pub(crate) fn create(
        destination: &Path,
        spec: &TempFileSpec,
        preallocate: bool,
        physical: bool,
        total_size: Option<u64>,
    ) -> Result<Self, SinkError> {
        let temp_path = spec.temp_path_for(destination);
        #[cfg(test)]
        let script = fault_script::for_destination(destination);
        #[cfg(test)]
        if let Some(script) = &script {
            script.check(OutputOperation::Open).map_err(SinkError)?;
        }
        let mut sink = FileSink::open(destination, spec, preallocate, physical)?;
        sink.prepare(total_size)?;
        Ok(Self {
            sink: Some(sink),
            shared: None,
            temp_path,
            disposition: PartialArtifactDisposition::DeleteOnDrop,
            #[cfg(test)]
            script,
        })
    }

    /// Reopen an existing temporary output without truncating it.
    pub(crate) fn reopen(destination: &Path, spec: &TempFileSpec) -> Result<Self, SinkError> {
        let temp_path = spec.temp_path_for(destination);
        #[cfg(test)]
        let script = fault_script::for_destination(destination);
        #[cfg(test)]
        if let Some(script) = &script {
            script.check(OutputOperation::Open).map_err(SinkError)?;
        }
        let mut sink = FileSink::open(destination, spec, false, false)?;
        sink.set_keep_on_drop(true);
        Ok(Self {
            sink: Some(sink),
            shared: None,
            temp_path,
            disposition: PartialArtifactDisposition::PreserveOnDrop,
            #[cfg(test)]
            script,
        })
    }

    /// A durable-mode data-sync capability over the output file (design D1:
    /// the owner keeps synchronization authority; during segmented transfer
    /// the job coordinator holds this capability for pre-checkpoint syncs).
    /// Syncs are safe while writers are live — `sync_all` flushes exactly the
    /// writes acknowledged before the call.
    pub(crate) fn sync_capability(&self) -> Result<OutputSyncCapability, SinkError> {
        let sink = self.sink.as_ref().ok_or_else(shared_session_error)?;
        Ok(OutputSyncCapability {
            file: sink.shared_handle().ok_or_else(shared_session_error)?,
            #[cfg(test)]
            script: self.script.clone(),
        })
    }

    /// Preserve the current temporary output after this session is dropped.
    pub(crate) fn preserve_partial(&mut self) {
        self.disposition = PartialArtifactDisposition::PreserveOnDrop;
        if let Some(sink) = self.sink.as_mut() {
            sink.set_keep_on_drop(true);
        }
    }

    /// Lend write-only capabilities to segmented workers (task 2.2). The
    /// session retains lifecycle/sync ownership and cannot finalize, abort,
    /// resize or publish until `reclaim_exclusive` succeeds.
    pub(crate) fn share_write_handles(
        &mut self,
        worker_count: usize,
    ) -> Result<Vec<OutputWriteHandle>, SinkError> {
        if worker_count == 0 || self.shared.is_some() {
            return Err(shared_session_error());
        }
        let Some(sink) = self.sink.as_ref() else {
            return Err(shared_session_error());
        };
        let file = sink.shared_handle().ok_or_else(shared_session_error)?;
        let writers_alive = Arc::new(AtomicUsize::new(0));
        self.shared = Some(SharedOutput {
            writers_alive: writers_alive.clone(),
        });
        Ok((0..worker_count)
            .map(|_| {
                writers_alive.fetch_add(1, Ordering::SeqCst);
                OutputWriteHandle {
                    file: file.clone(),
                    writers_alive: writers_alive.clone(),
                    #[cfg(test)]
                    script: self.script.clone(),
                }
            })
            .collect())
    }

    /// Regain exclusive access. Fails closed while any writer capability is
    /// still alive, so callers cannot finalize an output during active writes.
    pub(crate) fn reclaim_exclusive(&mut self) -> Result<(), SinkError> {
        let Some(shared) = self.shared.take() else {
            return Ok(());
        };
        if shared.writers_alive.load(Ordering::SeqCst) > 0 {
            self.shared = Some(shared);
            return Err(shared_session_error());
        }
        Ok(())
    }

    fn sink_mut(&mut self) -> Result<&mut FileSink, SinkError> {
        if self.shared.is_some() {
            return Err(shared_session_error());
        }
        self.sink.as_mut().ok_or_else(shared_session_error)
    }

    pub(crate) fn verification_read(&self) -> Result<(), DownloadError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::VerificationRead)?;
        }
        Ok(())
    }

    /// Path backing the temporary output.
    #[must_use]
    pub(crate) fn temp_path(&self) -> &Path {
        &self.temp_path
    }

    /// Publish the completed temp file according to the selected policy.
    pub(crate) fn commit_with_policy(
        mut self,
        mode: PublishMode,
    ) -> Result<(PathBuf, Option<String>), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Publish).map_err(SinkError)?;
        }
        self.reclaim_exclusive()?;
        self.sink
            .take()
            .ok_or_else(shared_session_error)?
            .commit_with_policy(mode)
    }

    #[cfg(test)]
    fn fail_next_write(&mut self) {
        self.sink_mut()
            .expect("session is exclusive")
            .fail_next_write();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_flush(&mut self) {
        self.sink_mut()
            .expect("session is exclusive")
            .fail_next_flush();
    }
}

impl Sink for OutputSession {
    fn prepare(&mut self, total_size: Option<u64>) -> Result<(), SinkError> {
        self.sink_mut()?.prepare(total_size)
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Write).map_err(SinkError)?;
        }
        self.sink_mut()?.write_at(offset, bytes)
    }

    fn flush(&mut self, level: FlushLevel) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Flush).map_err(SinkError)?;
        }
        self.sink_mut()?.flush(level)
    }

    fn size(&mut self) -> Result<u64, SinkError> {
        self.sink_mut()?.size()
    }

    fn finalize(&mut self) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Flush).map_err(SinkError)?;
            script.check(OutputOperation::Finalize).map_err(SinkError)?;
        }
        self.sink_mut()?.finalize()
    }

    fn abort(&mut self) -> Result<AbortDisposition, SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Cleanup).map_err(SinkError)?;
        }
        self.sink_mut()?.abort()
    }

    fn temp_path(&self) -> &Path {
        OutputSession::temp_path(self)
    }
}

impl Drop for OutputSession {
    fn drop(&mut self) {
        if let Some(sink) = self.sink.as_mut() {
            sink.set_keep_on_drop(self.disposition == PartialArtifactDisposition::PreserveOnDrop);
        }
    }
}

fn shared_session_error() -> SinkError {
    SinkError(DownloadError::Protocol(
        "output session is shared with worker handles".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn destination(directory: &tempfile::TempDir) -> PathBuf {
        directory.path().join("output.bin")
    }

    fn temp_path(destination: &Path) -> PathBuf {
        TempFileSpec::default().temp_path_for(destination)
    }

    #[test]
    fn fresh_session_prepares_and_deletes_partial_on_drop() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), true, false, Some(32))
                .expect("fresh session");

        assert_eq!(session.size().expect("preallocated size"), 32);
        session.write_at(0, b"fresh").expect("write");
        session.flush(FlushLevel::PageCache).expect("flush");
        assert_eq!(&std::fs::read(&temp).expect("read temp")[..5], b"fresh");

        drop(session);
        assert!(!temp.exists(), "fresh partial is deleted on drop");
        assert!(!destination.exists());
    }

    #[test]
    fn reopened_session_preserves_existing_bytes_and_writes_at_offset() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        std::fs::write(&temp, b"prior").expect("seed partial");

        let mut session =
            shol_open(&destination).expect("reopen session");
        assert_eq!(session.size().expect("reopened size"), 5);
        session.write_at(5, b" resume").expect("append by offset");
        session.flush(FlushLevel::PageCache).expect("flush");
        drop(session);

        assert_eq!(
            std::fs::read(&temp).expect("preserved partial"),
            b"prior resume"
        );
    }

    fn shol_open(destination: &Path) -> Result<OutputSession, SinkError> {
        OutputSession::reopen(destination, &TempFileSpec::default())
    }

    #[test]
    fn write_failure_uses_structured_error_and_fresh_drop_disposition() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, false, None)
                .expect("fresh session");
        session.fail_next_write();

        let error = session
            .write_at(0, b"data")
            .expect_err("injected write failure");
        assert!(matches!(error.0, DownloadError::SinkWrite(_)));
        drop(session);
        assert!(!temp.exists(), "fresh partial is deleted after failure");
    }

    #[test]
    fn flush_failure_uses_structured_error_and_fresh_drop_disposition() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, false, None)
                .expect("fresh session");
        session.write_at(0, b"data").expect("write");
        session.fail_next_flush();

        let error = session
            .flush(FlushLevel::PageCache)
            .expect_err("injected flush failure");
        assert!(matches!(error.0, DownloadError::SinkWrite(_)));
        drop(session);
        assert!(!temp.exists(), "fresh partial is deleted after failure");
    }

    #[test]
    fn reopened_session_preserves_partial_on_write_failure() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        std::fs::write(&temp, b"prior").expect("seed partial");

        let mut session = shol_open(&destination).expect("reopen session");
        session.fail_next_write();
        let error = session
            .write_at(5, b"data")
            .expect_err("injected write failure");
        assert!(matches!(error.0, DownloadError::SinkWrite(_)));
        drop(session);

        assert_eq!(std::fs::read(&temp).expect("preserved partial"), b"prior");
    }

    /// Exclusive operations are blocked while any writer capability is alive;
    /// dropping all capabilities restores reclaim/finalize/publish (task 2.2).
    #[tokio::test]
    async fn cannot_publish_while_writers_live() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, false, None)
                .expect("fresh session");
        session.preserve_partial();

        let mut writers = session.share_write_handles(2).expect("share session");
        let second = writers.pop().expect("first worker handle");
        let first = writers.pop().expect("second worker handle");

        // Exclusive operations fail while writers are alive.
        assert!(session
            .finalize()
            .expect_err("cannot finalize while shared")
            .0
            .to_string()
            .contains("shared with worker handles"));
        assert!(session.reclaim_exclusive().is_err());
        assert!(session.size().is_err());
        assert!(session.flush(FlushLevel::PageCache).is_err());

        // Both capabilities can write concurrently at disjoint offsets
        // through their blocking writer lanes (task 2.3).
        let lane1 = super::super::writer_lane::WriterLane::spawn(first);
        let lane2 = super::super::writer_lane::WriterLane::spawn(second);
        let (h1, h2) = (lane1.handle(), lane2.handle());
        let (r1, r2) = tokio::join!(
            h1.write(0, bytes::Bytes::from_static(b"first")),
            h2.write(5, bytes::Bytes::from_static(b"-last")),
        );
        r1.expect("worker write 1");
        r2.expect("worker write 2");
        drop(h1);
        drop(h2);

        // Reclaim still fails while only ONE lane (capability) is alive.
        lane1.shutdown().await.expect("join lane 1");
        assert!(session.reclaim_exclusive().is_err(), "one writer remains");

        lane2.shutdown().await.expect("join lane 2");
        session.reclaim_exclusive().expect("all writers joined");
        session.finalize().expect("finalize after reclaim");
        assert_eq!(
            std::fs::read(&temp).expect("assembled temp"),
            b"first-last"
        );
    }

    /// Write-only capabilities expose no flush/sync/size/abort authority:
    /// the type simply has no such methods (compile-time separation), and
    /// out-of-order disjoint positional writes land exactly (task 2.2/2.3).
    #[tokio::test]
    async fn capabilities_write_out_of_order_at_disjoint_offsets() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, false, None)
                .expect("fresh session");
        let mut writers = session.share_write_handles(3).expect("share session");
        let lane_c = super::super::writer_lane::WriterLane::spawn(writers.pop().expect("cap c"));
        let lane_b = super::super::writer_lane::WriterLane::spawn(writers.pop().expect("cap b"));
        let lane_a = super::super::writer_lane::WriterLane::spawn(writers.pop().expect("cap a"));
        // Out of order, unaligned, disjoint.
        let (hc, hb, ha) = (lane_c.handle(), lane_b.handle(), lane_a.handle());
        let (rc, rb, ra) = tokio::join!(
            hc.write(11, bytes::Bytes::from_static(b"segment-c")),
            hb.write(5, bytes::Bytes::from_static(b"-seg-b")),
            ha.write(0, bytes::Bytes::from_static(b"seg-a")),
        );
        rc.expect("c");
        rb.expect("b");
        ra.expect("a");
        drop(hc);
        drop(hb);
        drop(ha);
        lane_a.shutdown().await.expect("join a");
        lane_b.shutdown().await.expect("join b");
        lane_c.shutdown().await.expect("join c");
        session.reclaim_exclusive().expect("reclaim");
        assert_eq!(
            std::fs::read(&temp).expect("assembled"),
            b"seg-a-seg-bsegment-c"
        );
    }
}
