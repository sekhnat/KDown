//! Checked positional writes over one shared immutable file handle (§14.2,
//! design D1, task 2.1).
//!
//! Segmented workers write complete buffers at their assigned absolute
//! offsets without sharing or moving a common file cursor. The adapter is
//! cfg-gated per platform: `FileExt::write_at` on Unix, `seek_write` on
//! Windows (both safe standard-library APIs; neither moves the process-wide
//! cursor the way `Seek` would). The `write_all_at` loop completes short
//! writes, retries interruption, rejects `Ok(0)`, and treats offset
//! arithmetic as checked — unwritten bytes are never reported as written.

use std::io::{Error, ErrorKind};

/// A positional write primitive: write `buf` at absolute `offset` without
/// moving any shared cursor, returning the number of bytes written.
pub(crate) trait PositionalWriter {
    fn pos_write(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize>;
}

#[cfg(unix)]
impl PositionalWriter for std::fs::File {
    fn pos_write(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        // write_at: positional write, cursor untouched.
        std::os::unix::fs::FileExt::write_at(self, buf, offset)
    }
}

#[cfg(windows)]
impl PositionalWriter for std::fs::File {
    fn pos_write(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        // seek_write: positional write; does not move the file pointer used
        // by other handles, but this handle's own cursor is per-handle state
        // that no segmented worker shares.
        std::os::windows::fs::FileExt::seek_write(self, buf, offset)
    }
}

/// Write all of `bytes` at absolute `offset`, looping over short writes.
///
/// Semantics (resume-and-storage spec, "Partial and failed writes"):
/// - zero-length writes succeed without touching the writer;
/// - interruption (`ErrorKind::Interrupted`) is retried;
/// - `Ok(0)` without progress is a structured error, never silent success;
/// - the offset is advanced with checked arithmetic (`offset + len` must
///   fit in `u64`; overflow fails before any write);
/// - the remainder is retried at the advanced offset until complete.
///
/// # Errors
/// The underlying positional error, a zero-progress error, or an offset
/// overflow error.
pub(crate) fn write_all_at<W: PositionalWriter>(
    writer: &W,
    offset: u64,
    bytes: &[u8],
) -> std::io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let _end = offset
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "positional write offset overflow"))?;
    let mut written: usize = 0;
    loop {
        match writer.pos_write(offset + written as u64, &bytes[written..]) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::WriteZero,
                    "positional write made no progress",
                ));
            }
            Ok(n) => {
                written += n;
                if written == bytes.len() {
                    return Ok(());
                }
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic scripted writer: each call writes at most `limit`
    /// bytes, optionally failing first with `error_first` or reporting
    /// `zero_after` zero-length responses.
    /// All fields are interior-mutable so the writer can be shared behind
    /// `&self` like the real `File` handle.
    struct ScriptedWriter {
        writes: std::cell::RefCell<Vec<(u64, Vec<u8>)>>,
        /// Maximum bytes per call (short-write simulation).
        limit: Option<usize>,
        /// Fail with this error from call N (1-based) onward.
        error_from: Option<(usize, ErrorKind)>,
        /// Report Ok(0) on the first N calls.
        zero_first: usize,
        /// Report Interrupted on the first N calls.
        interrupted_first: usize,
        calls: std::cell::Cell<usize>,
    }

    impl ScriptedWriter {
        fn new() -> Self {
            Self {
                writes: std::cell::RefCell::new(vec![]),
                limit: None,
                error_from: None,
                zero_first: 0,
                interrupted_first: 0,
                calls: std::cell::Cell::new(0),
            }
        }
    }

    impl PositionalWriter for ScriptedWriter {
        fn pos_write(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
            self.calls.set(self.calls.get() + 1);
            if let Some((n, kind)) = &self.error_from {
                if self.calls.get() >= *n {
                    return Err(Error::new(*kind, "scripted"));
                }
            }
            if self.calls.get() <= self.interrupted_first {
                return Err(Error::new(ErrorKind::Interrupted, "scripted interrupt"));
            }
            if self.calls.get() <= self.zero_first {
                return Ok(0);
            }
            let n = self.limit.map_or(buf.len(), |l| l.min(buf.len()));
            self.writes.borrow_mut().push((offset, buf[..n].to_vec()));
            Ok(n)
        }
    }

    fn reconstructed(w: &ScriptedWriter) -> Vec<u8> {
        let mut out = vec![];
        for (offset, bytes) in w.writes.borrow().iter() {
            let end = *offset as usize + bytes.len();
            if out.len() < end {
                out.resize(end, 0);
            }
            out[*offset as usize..end].copy_from_slice(bytes);
        }
        out
    }

    #[test]
    fn full_write_single_call() {
        let w = ScriptedWriter::new();
        write_all_at(&w, 0, b"hello").expect("write");
        assert_eq!(reconstructed(&w), b"hello");
        assert_eq!(w.calls.get(), 1);
    }

    #[test]
    fn short_writes_are_completed_at_advanced_offset() {
        let mut w = ScriptedWriter::new();
        w.limit = Some(3);
        write_all_at(&w, 10, b"abcdefghij").expect("write");
        let mut expected = vec![0u8; 20];
        expected[10..20].copy_from_slice(b"abcdefghij");
        assert_eq!(reconstructed(&w), expected, "remainder at advanced offset");
        assert_eq!(w.calls.get(), 4, "3+3+3+1");
    }

    #[test]
    fn interrupted_is_retried_without_losing_bytes() {
        let mut w = ScriptedWriter::new();
        w.interrupted_first = 2;
        write_all_at(&w, 0, b"data").expect("write");
        assert_eq!(reconstructed(&w), b"data");
        assert_eq!(w.calls.get(), 3, "2 interrupts then one full write");
    }

    #[test]
    fn zero_progress_is_an_error_never_success() {
        let mut w = ScriptedWriter::new();
        w.zero_first = 5;
        let err = write_all_at(&w, 0, b"never").expect_err("zero writes");
        assert_eq!(err.kind(), ErrorKind::WriteZero);
        assert!(w.writes.borrow().is_empty(), "no bytes reported written");
    }

    #[test]
    fn underlying_error_propagates_after_partial_write() {
        let mut w = ScriptedWriter::new();
        w.limit = Some(2);
        w.error_from = Some((2, ErrorKind::StorageFull));
        let err = write_all_at(&w, 0, b"abcdef").expect_err("fails on call 2");
        assert_eq!(err.kind(), ErrorKind::StorageFull);
        // Exactly the two written bytes were written; nothing more.
        assert_eq!(reconstructed(&w), b"ab");
    }

    #[test]
    fn zero_length_write_is_a_no_op() {
        let w = ScriptedWriter::new();
        write_all_at(&w, 7, b"").expect("empty write");
        assert_eq!(w.calls.get(), 0, "writer untouched");
    }

    #[test]
    fn offset_overflow_fails_before_any_write() {
        let w = ScriptedWriter::new();
        let err = write_all_at(&w, u64::MAX - 1, b"0123").expect_err("offset + len overflows u64");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(w.calls.get(), 0);
    }

    #[test]
    fn disjoint_out_of_order_writes_reconstruct_exactly() {
        let mut w = ScriptedWriter::new();
        w.limit = Some(2);
        write_all_at(&w, 5, b"world").expect("tail");
        write_all_at(&w, 0, b"hello").expect("head (later, out of order)");
        assert_eq!(reconstructed(&w), b"helloworld");
    }

    #[test]
    fn real_file_positional_writes_at_large_offsets() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("pos.bin");
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("open");
        let head = b"head";
        let tail = b"tail-bytes";
        write_all_at(&file, 0, head).expect("write head");
        // Large offset: sparse file, content only at the written positions.
        const FAR: u64 = 4 * 1024 * 1024;
        write_all_at(&file, FAR, tail).expect("write tail at large offset");
        let mut out = vec![0u8; FAR as usize + tail.len()];
        file.read_exact_at(&mut out, 0).expect("read back");
        assert_eq!(&out[..head.len()], head);
        assert_eq!(&out[FAR as usize..FAR as usize + tail.len()], tail);
    }

    #[cfg(unix)]
    trait ReadExactAt {
        fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<()>;
    }
    #[cfg(unix)]
    impl ReadExactAt for std::fs::File {
        fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
            std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
        }
    }
}
