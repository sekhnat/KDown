//! Crate-private owner for one temporary output's open handle and partial-file policy.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

#[cfg(test)]
use super::fault_script::{self, OutputFaultScript, OutputOperation};
use super::sink::{AbortDisposition, FileSink, FlushLevel, Sink, SinkError, TempFileSpec};
use crate::error::DownloadError;
use crate::io::publish::PublishMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartialArtifactDisposition {
    DeleteOnDrop,
    PreserveOnDrop,
}

/// Owns the temporary file from preparation through publication or disposition.
///
/// A fresh session removes its partial file on drop. A reopened session keeps
/// existing bytes unless the caller explicitly aborts it. During segmented
/// transfer it lends bounded, cloneable write handles to the configured worker
/// set, then regains exclusive access after all handles have been joined.
pub(crate) struct OutputSession {
    sink: Option<FileSink>,
    shared_sink: Option<Arc<AsyncMutex<FileSink>>>,
    temp_path: PathBuf,
    disposition: PartialArtifactDisposition,
    #[cfg(test)]
    script: Option<Arc<OutputFaultScript>>,
}

/// A worker-scoped, serialized write capability. Worker count and the transfer
/// buffer budget bound outstanding writers and their in-flight payloads.
pub(crate) struct OutputWriteHandle {
    sink: Arc<AsyncMutex<FileSink>>,
    #[cfg(test)]
    script: Option<Arc<OutputFaultScript>>,
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
        total_size: Option<u64>,
    ) -> Result<Self, SinkError> {
        let temp_path = spec.temp_path_for(destination);
        #[cfg(test)]
        let script = fault_script::for_destination(destination);
        #[cfg(test)]
        if let Some(script) = &script {
            script.check(OutputOperation::Open).map_err(SinkError)?;
        }
        let mut sink = FileSink::open(destination, spec, preallocate)?;
        sink.prepare(total_size)?;
        Ok(Self {
            sink: Some(sink),
            shared_sink: None,
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
        let mut sink = FileSink::open(destination, spec, false)?;
        sink.set_keep_on_drop(true);
        Ok(Self {
            sink: Some(sink),
            shared_sink: None,
            temp_path,
            disposition: PartialArtifactDisposition::PreserveOnDrop,
            #[cfg(test)]
            script,
        })
    }

    /// Preserve the current temporary output after this session is dropped.
    pub(crate) fn preserve_partial(&mut self) {
        self.disposition = PartialArtifactDisposition::PreserveOnDrop;
        if let Some(sink) = self.sink.as_mut() {
            sink.set_keep_on_drop(true);
        }
    }

    /// Lend the output to segmented workers. The session retains the owner token
    /// and cannot perform synchronous I/O or finalize until it reclaims it.
    pub(crate) fn share_write_handles(
        &mut self,
        worker_count: usize,
    ) -> Result<Vec<OutputWriteHandle>, SinkError> {
        if worker_count == 0 || self.shared_sink.is_some() {
            return Err(shared_session_error());
        }
        let Some(mut sink) = self.sink.take() else {
            return Err(shared_session_error());
        };
        sink.set_keep_on_drop(self.disposition == PartialArtifactDisposition::PreserveOnDrop);
        let shared = Arc::new(AsyncMutex::new(sink));
        self.shared_sink = Some(shared.clone());
        Ok((0..worker_count)
            .map(|_| OutputWriteHandle {
                sink: shared.clone(),
                #[cfg(test)]
                script: self.script.clone(),
            })
            .collect())
    }

    /// Regain exclusive access. Fails closed while any worker still owns a
    /// write handle, so callers cannot finalize an output during active writes.
    pub(crate) fn reclaim_exclusive(&mut self) -> Result<(), SinkError> {
        let Some(shared) = self.shared_sink.take() else {
            return Ok(());
        };
        match Arc::try_unwrap(shared) {
            Ok(sink) => {
                self.sink = Some(sink.into_inner());
                Ok(())
            }
            Err(shared) => {
                self.shared_sink = Some(shared);
                Err(shared_session_error())
            }
        }
    }

    fn sink_mut(&mut self) -> Result<&mut FileSink, SinkError> {
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

impl OutputWriteHandle {
    pub(crate) async fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Write).map_err(SinkError)?;
        }
        self.sink.lock().await.write_at(offset, bytes)
    }

    pub(crate) async fn flush(&self, level: FlushLevel) -> Result<(), SinkError> {
        #[cfg(test)]
        if let Some(script) = &self.script {
            script.check(OutputOperation::Flush).map_err(SinkError)?;
        }
        self.sink.lock().await.flush(level)
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
            OutputSession::create(&destination, &TempFileSpec::default(), true, Some(32))
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
            OutputSession::reopen(&destination, &TempFileSpec::default()).expect("reopen session");
        assert_eq!(session.size().expect("reopened size"), 5);
        session.write_at(5, b" resume").expect("append by offset");
        session.flush(FlushLevel::PageCache).expect("flush");
        drop(session);

        assert_eq!(
            std::fs::read(&temp).expect("preserved partial"),
            b"prior resume"
        );
    }

    #[test]
    fn write_failure_uses_structured_error_and_fresh_drop_disposition() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, None)
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
            OutputSession::create(&destination, &TempFileSpec::default(), false, None)
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

        let mut session =
            OutputSession::reopen(&destination, &TempFileSpec::default()).expect("reopen session");
        session.fail_next_write();
        let error = session
            .write_at(5, b"data")
            .expect_err("injected write failure");
        assert!(matches!(error.0, DownloadError::SinkWrite(_)));
        drop(session);

        assert_eq!(std::fs::read(&temp).expect("preserved partial"), b"prior");
    }

    #[tokio::test]
    async fn worker_handles_must_be_released_before_session_can_finalize() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = destination(&directory);
        let temp = temp_path(&destination);
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, None)
                .expect("fresh session");
        session.preserve_partial();

        let mut writers = session.share_write_handles(2).expect("share session");
        let writer = writers.pop().expect("first worker handle");
        let held_by_worker = writers.pop().expect("second worker handle");
        writer.write_at(0, b"first").await.expect("worker write");
        assert!(session
            .finalize()
            .expect_err("cannot finalize while shared")
            .0
            .to_string()
            .contains("shared with worker handles"));
        assert!(session.reclaim_exclusive().is_err());

        held_by_worker
            .write_at(5, b"-last")
            .await
            .expect("worker remains able to write");
        drop(held_by_worker);
        drop(writer);
        session.reclaim_exclusive().expect("all workers joined");
        session.finalize().expect("finalize after reclaim");
        assert_eq!(std::fs::read(&temp).expect("assembled temp"), b"first-last");
    }
}
