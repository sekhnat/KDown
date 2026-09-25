//! Random-access output storage (§14, §33).
//!
//! All downloaded bytes are written to a temp file distinct from the final
//! destination; commit is an atomic rename. Errors map to the structured
//! taxonomy ([`DownloadError::from_io`]).

use std::fs::File;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::DownloadError;
use crate::io::publish::{self, PublishMode};

/// Where the sink's temporary output lives before commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TempFileSpec {
    /// `<destination>.part` (default, §14.1).
    SiblingSuffix(String),
    /// An explicit alternate temp path.
    Explicit(PathBuf),
}

impl TempFileSpec {
    /// Resolve the concrete temp path for a destination.
    #[must_use]
    pub fn temp_path_for(&self, destination: &Path) -> PathBuf {
        match self {
            TempFileSpec::SiblingSuffix(suffix) => {
                let mut name = destination
                    .file_name()
                    .map(std::ffi::OsStr::to_os_string)
                    .unwrap_or_else(|| destination.as_os_str().to_os_string());
                name.push(suffix.as_str());
                destination.with_file_name(name)
            }
            TempFileSpec::Explicit(p) => p.clone(),
        }
    }
}

impl Default for TempFileSpec {
    fn default() -> Self {
        Self::SiblingSuffix(".part".to_string())
    }
}

/// Random-access output storage (§33 Sink).
pub trait Sink {
    /// Create/prepare the sink, optionally preallocating `total_size`.
    ///
    /// # Errors
    /// SinkOpen/DiskFull/PermissionDenied on failure (§14.5: lack of disk
    /// space is fatal; unsupported preallocation is not — the caller may
    /// treat `PreallocUnsupported` as non-fatal).
    fn prepare(&mut self, total_size: Option<u64>) -> Result<(), SinkError>;

    /// Write `bytes` at absolute `offset` (positional, §14.2).
    ///
    /// # Errors
    /// SinkWrite/DiskFull/PermissionDenied on failure.
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), SinkError>;

    /// Flush data to the level required by `mode` (§14.6 step 1).
    ///
    /// # Errors
    /// SinkWrite on flush failure.
    fn flush(&mut self, level: FlushLevel) -> Result<(), SinkError>;

    /// Current file size.
    ///
    /// # Errors
    /// SinkOpen when the file cannot be inspected.
    fn size(&mut self) -> Result<u64, SinkError>;

    /// Flush and settle the temporary output according to the active durability policy.
    ///
    /// This does not publish the file. The engine publishes it only after
    /// verification, using the selected overwrite policy; the local `FileSink`
    /// path uses an atomic no-replace operation for `FailIfExists` and safe
    /// atomic replacement for `Replace`.
    ///
    /// # Errors
    /// SinkWrite if flushing or finalization fails.
    fn finalize(&mut self) -> Result<(), SinkError>;

    /// Discard partial state per cleanup policy (§9.4).
    ///
    /// # Errors
    /// I/O failure during cleanup.
    fn abort(&mut self) -> Result<AbortDisposition, SinkError>;

    /// The temp path backing this sink (for checkpoint identity, §15.2).
    fn temp_path(&self) -> &Path;
}

/// Durability level for a flush (§14.6, §15.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushLevel {
    /// Hand bytes to the OS page cache; no fsync.
    PageCache,
    /// fsync the file data.
    FsyncFile,
    /// fsync the file and the parent directory (rename durability).
    FsyncDir,
}

/// What `abort` did with the partial artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortDisposition {
    /// Temp file removed.
    TempDeleted,
    /// Temp file preserved for later inspection/resume.
    TempKept,
}

/// Local sink failure reason; wraps the structured taxonomy.
#[derive(Debug)]
pub struct SinkError(pub DownloadError);

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sink error: {}", self.0)
    }
}
impl std::error::Error for SinkError {}

impl From<std::io::Error> for SinkError {
    fn from(e: std::io::Error) -> Self {
        SinkError(DownloadError::from_io(&e))
    }
}

impl From<SinkError> for DownloadError {
    fn from(e: SinkError) -> Self {
        e.0
    }
}

/// The default local-filesystem sink (§14, §33).
#[derive(Debug)]
pub struct FileSink {
    destination: PathBuf,
    temp_path: PathBuf,
    preallocate: bool,
    /// Opt-in physical reservation: attempt fallocate-style
    /// space reservation after the logical `set_len`; unsupported
    /// platforms/filesystems fall back to logical sizing.
    physical: bool,
    /// When set, dropping without commit/finalize keeps the temp file
    /// (KeepPartial semantics and crash-resume preservation, §9.4/§15.1).
    keep_on_drop: bool,
    #[cfg(test)]
    fail_next_write: bool,
    #[cfg(test)]
    fail_next_flush: bool,
    /// Shared immutable handle: positional writes need only `&File`, so the
    /// handle is reference-counted and lent to write-only worker capabilities
    /// while the owner keeps lifecycle authority .
    file: Option<Arc<File>>,
    /// Bytes written so far (high-water mark) — informational.
    bytes_written: u64,
    finalized: bool,
    aborted: bool,
}

impl FileSink {
    /// Open a sink targeting `destination`, buffering into a distinct temp
    /// file in the same directory so final rename is atomic (§14.1).
    ///
    /// # Errors
    /// `SinkOpen`/`PermissionDenied` when the temp file cannot be created.
    pub fn open(
        destination: &Path,
        spec: &TempFileSpec,
        preallocate: bool,
        physical: bool,
    ) -> Result<Self, SinkError> {
        let temp_path = spec.temp_path_for(destination);
        let parent = temp_path
            .parent()
            .ok_or_else(|| SinkError(DownloadError::SinkOpen("no parent directory".into())))?;
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(SinkError(DownloadError::SinkOpen(format!(
                "destination directory does not exist: {}",
                parent.display()
            ))));
        }
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&temp_path)
            .map_err(|e| DownloadError::from_io(&e))
            .map_err(SinkError)?;
        Ok(Self {
            destination: destination.to_path_buf(),
            temp_path,
            preallocate,
            physical,
            keep_on_drop: false,
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            fail_next_flush: false,
            file: Some(Arc::new(file)),
            bytes_written: 0,
            finalized: false,
            aborted: false,
        })
    }

    /// The shared immutable handle for lending write-only worker
    /// capabilities: positional writes need only `&File`.
    pub(crate) fn shared_handle(&self) -> Option<Arc<File>> {
        self.file.clone()
    }

    /// The final destination this sink commits to.
    #[must_use]
    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// Preallocate `size` bytes: portable logical sizing (`set_len`) plus,
    /// when opted in, a physical reservation attempt.
    /// Unsupported filesystem ops are non-fatal (§14.3) — the reservation
    /// falls back to logical sizing without correctness changes. Real
    /// errors (permission, out-of-space) surface as sink errors.
    ///
    /// Returns whether the logical sizing happened.
    ///
    /// # Errors
    /// Fatal only for real errors (permission, disk full).
    pub fn preallocate(&mut self, size: u64) -> Result<bool, SinkError> {
        if !self.preallocate {
            return Ok(false);
        }
        let Some(file) = self.file.as_ref() else {
            return Err(SinkError(DownloadError::SinkOpen("sink closed".into())));
        };
        // set_len is the portable preallocation path; on Linux ext4/xfs it
        // also serves sparse purposes. FIEMAP/fallocate is a perf nicety,
        // not a correctness requirement (§14.3-14.4).
        let logical = match file.set_len(size) {
            Ok(()) => true,
            Err(e) if e.raw_os_error() == Some(95) => false, // EOPNOTSUPP
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => false,
            Err(e) => return Err(SinkError(DownloadError::from_io(&e))),
        };
        if self.physical {
            // Real errors (ENOSPC, EPERM, ...) surface;
            // unsupported operations/filesystems fall back silently.
            self.reserve_physical(file, size)?;
        }
        Ok(logical)
    }

    /// Optional physical reservation: attempted only
    /// where a portable binding exists (fs2 wraps fallocate on Unix and the
    /// SetFileInformationByHandle path on Windows). Unsupported
    /// operations/filesystems fall back silently — allocation is never a
    /// correctness dependency — while ENOSPC and permission errors surface
    /// as sink errors.
    fn reserve_physical(&self, file: &std::fs::File, size: u64) -> Result<(), SinkError> {
        use fs2::FileExt as _;
        match file.allocate(size) {
            Ok(()) => Ok(()),
            // Unsupported operation/filesystem: fall back to logical sizing.
            Err(e)
                if e.raw_os_error() == Some(95)              // EOPNOTSUPP
                    || e.raw_os_error() == Some(38)          // ENOSYS
                    || e.raw_os_error() == Some(22)          // EINVAL (flags/fs)
                    || e.kind() == std::io::ErrorKind::Unsupported =>
            {
                tracing::debug!("physical preallocation unsupported; logical fallback");
                Ok(())
            }
            // Real errors (ENOSPC, EPERM, ...) surface to the job.
            Err(e) => Err(SinkError(DownloadError::from_io(&e))),
        }
    }

    /// Atomically publish this completed temp file to the destination with replace semantics.
    ///
    /// The old destination is never removed as a fallback: when the platform or
    /// filesystem cannot safely replace it, this operation fails and preserves
    /// the old bytes. Engine jobs using `FailIfExists` select atomic no-replace
    /// semantics instead.
    ///
    /// # Errors
    /// `Commit` on publication failure; the temp file and prior destination are preserved.
    pub fn commit(self) -> Result<PathBuf, SinkError> {
        self.commit_with_policy(PublishMode::Replace)
            .map(|(destination, _warning)| destination)
    }

    /// Publish according to an explicit filesystem policy.
    ///
    /// # Errors
    /// `Commit` if the requested atomic publication operation fails.
    pub(crate) fn commit_with_policy(
        mut self,
        mode: PublishMode,
    ) -> Result<(PathBuf, Option<String>), SinkError> {
        // Close the handle before publication, which is required by Windows.
        self.file = None;
        self.finalized = true; // Drop must preserve the temp file on failure.
        let outcome =
            publish::publish(&self.temp_path, &self.destination, mode).map_err(|error| {
                let download_error = if mode == PublishMode::NoReplace
                    && error.kind() == std::io::ErrorKind::AlreadyExists
                {
                    DownloadError::DestinationConflict(format!(
                        "destination appeared before no-replace publication ({}): {error}",
                        self.destination.display()
                    ))
                } else {
                    DownloadError::Commit(error.to_string())
                };
                SinkError(download_error)
            })?;
        Ok((self.destination.clone(), outcome.temp_cleanup_warning))
    }

    /// True once `abort` ran.
    #[must_use]
    pub fn aborted(&self) -> bool {
        self.aborted
    }

    /// Preserve the temp file if this sink is dropped without commit
    /// (KeepPartial / crash-resume preservation).
    pub fn set_keep_on_drop(&mut self, keep: bool) {
        self.keep_on_drop = keep;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_write(&mut self) {
        self.fail_next_write = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_flush(&mut self) {
        self.fail_next_flush = true;
    }
}

impl Sink for FileSink {
    fn prepare(&mut self, total_size: Option<u64>) -> Result<(), SinkError> {
        if let Some(size) = total_size {
            self.preallocate(size)?;
        }
        Ok(())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), SinkError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_write) {
            return Err(SinkError(DownloadError::SinkWrite(
                "injected output-session write failure".into(),
            )));
        }
        let Some(file) = self.file.as_mut() else {
            return Err(SinkError(DownloadError::SinkWrite("sink closed".into())));
        };
        // Sequential fallback path (nonsegmented use, design D1): requires
        // exclusive handle ownership; worker capabilities hold clones and
        // use the positional adapter instead.
        let Some(file) = Arc::get_mut(file) else {
            return Err(SinkError(DownloadError::SinkWrite(
                "sink handle is shared with write-only worker capabilities".into(),
            )));
        };
        // Positional write without a shared seek pointer (§14.2): seek to
        // the absolute offset then write; each call states its position
        // explicitly rather than relying on prior state.
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
        self.bytes_written = self.bytes_written.max(offset + bytes.len() as u64);
        Ok(())
    }

    fn flush(&mut self, level: FlushLevel) -> Result<(), SinkError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_flush) {
            return Err(SinkError(DownloadError::SinkWrite(
                "injected output-session flush failure".into(),
            )));
        }
        let Some(file) = self.file.as_ref() else {
            return Err(SinkError(DownloadError::SinkWrite("sink closed".into())));
        };
        // std::fs::File has no userspace buffer: PageCache is a no-op and
        // FsyncFile/FsyncDir synchronize through the shared handle.
        if matches!(level, FlushLevel::FsyncFile | FlushLevel::FsyncDir) {
            file.sync_all()
                .map_err(|e| SinkError(DownloadError::from_io(&e)))?;
        }
        Ok(())
    }

    fn size(&mut self) -> Result<u64, SinkError> {
        match self.file.as_ref() {
            Some(file) => Ok(file.metadata()?.len()),
            None => Err(SinkError(DownloadError::SinkOpen("sink closed".into()))),
        }
    }

    fn finalize(&mut self) -> Result<(), SinkError> {
        self.flush(FlushLevel::FsyncFile)?;
        if let Some(file) = self.file.take() {
            drop(file);
        }
        self.finalized = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<AbortDisposition, SinkError> {
        if let Some(file) = self.file.take() {
            drop(file);
        }
        match std::fs::remove_file(&self.temp_path) {
            Ok(()) => {
                self.aborted = true;
                Ok(AbortDisposition::TempDeleted)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.aborted = true;
                Ok(AbortDisposition::TempDeleted)
            }
            Err(e) => Err(SinkError(DownloadError::from_io(&e))),
        }
    }

    fn temp_path(&self) -> &Path {
        &self.temp_path
    }
}

impl Drop for FileSink {
    fn drop(&mut self) {
        // If never finalized/committed, clean the temp file so failed jobs
        // leave no residue (DeletePartial default, §9.4) — unless the owner
        // opted to preserve it (KeepPartial / crash-resume, §15.1).
        if !self.finalized && !self.aborted && !self.keep_on_drop {
            self.file = None;
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        (dir, path)
    }

    #[test]
    fn temp_naming_is_sibling_suffix() {
        let (_d, dest) = tmpdir();
        let spec = TempFileSpec::default();
        assert_eq!(
            spec.temp_path_for(&dest),
            dest.with_file_name("out.bin.part")
        );
    }

    #[test]
    fn write_read_commit_roundtrip() {
        let (dir, dest) = tmpdir();
        {
            let mut sink =
                FileSink::open(&dest, &TempFileSpec::default(), false, false).expect("open");
            sink.write_at(0, b"hello").expect("write");
            sink.write_at(5, b" world").expect("write at 5");
            assert_eq!(sink.size().expect("size"), 11);
            sink.finalize().expect("finalize");
            let out = sink.commit().expect("commit");
            assert_eq!(out, dest);
        }
        let got = std::fs::read(&dest).expect("read final");
        assert_eq!(got, b"hello world");
        assert!(!dest.with_file_name("out.bin.part").exists());
        dir.close().expect("cleanup");
    }

    #[test]
    fn abort_removes_temp_only() {
        let (dir, dest) = tmpdir();
        let mut sink = FileSink::open(&dest, &TempFileSpec::default(), false, false).expect("open");
        sink.write_at(0, b"x").expect("write");
        assert_eq!(sink.abort().expect("abort"), AbortDisposition::TempDeleted);
        assert!(!sink.temp_path().exists());
        assert!(!dest.exists(), "abort never touches destination");
        dir.close().expect("cleanup");
    }

    #[test]
    fn drop_without_commit_cleans_temp() {
        let (dir, dest) = tmpdir();
        {
            let mut sink =
                FileSink::open(&dest, &TempFileSpec::default(), false, false).expect("open");
            sink.write_at(0, b"partial").expect("write");
            // Dropped without finalize/commit/abort.
        }
        assert!(!dest.exists());
        assert!(!dest.with_file_name("out.bin.part").exists());
        dir.close().expect("cleanup");
    }

    #[test]
    fn preallocate_sets_size() {
        let (dir, dest) = tmpdir();
        let mut sink = FileSink::open(&dest, &TempFileSpec::default(), true, false).expect("open");
        sink.prepare(Some(1024)).expect("prepare");
        assert_eq!(sink.size().expect("size"), 1024);
        sink.abort().expect("abort");
        dir.close().expect("cleanup");
    }

    #[test]
    fn commit_failure_preserves_temp() {
        let (dir, _dest) = tmpdir();
        // Temp lives in the surviving dir; destination lives in a dir that
        // vanishes before commit.
        let orphan_dir = dir.path().join("gone");
        std::fs::create_dir(&orphan_dir).expect("mkdir");
        let orphan_dest = orphan_dir.join("x.bin");
        let temp = dir.path().join("explicit.part");
        let mut sink = FileSink::open(
            &orphan_dest,
            &TempFileSpec::Explicit(temp.clone()),
            false,
            false,
        )
        .expect("open");
        sink.write_at(0, b"data").expect("write");
        sink.finalize().expect("finalize");
        std::fs::remove_dir_all(&orphan_dir).expect("remove target dir");
        let err = sink.commit().expect_err("rename to missing dir must fail");
        assert!(matches!(err.0, DownloadError::Commit(_)));
        assert!(temp.exists(), "failed commit keeps temp file");
        assert!(!orphan_dest.exists());
        let _ = std::fs::remove_file(&temp);
        dir.close().expect("cleanup");
    }
}
