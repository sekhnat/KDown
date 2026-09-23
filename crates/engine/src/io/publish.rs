//! Filesystem publication primitives for completed temporary output.
//!
//! No-replace publication creates the destination as a hard link, which is an
//! atomic no-clobber directory-entry operation on the supported local filesystems.
//! If the filesystem cannot provide that operation, publication fails closed.
//! Replace publication delegates to the platform's atomic same-directory rename;
//! it never deletes the previous destination as a fallback.

use std::io;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishMode {
    NoReplace,
    Replace,
}

#[derive(Debug, Default)]
pub(crate) struct PublishOutcome {
    /// Set only if no-replace publication succeeded but unlinking the old temp
    /// name failed. The final destination is already published and must not be
    /// reported as failed or rolled back.
    pub(crate) temp_cleanup_warning: Option<String>,
}

trait PublicationOps {
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
}

struct LocalPublicationOps;

impl PublicationOps for LocalPublicationOps {
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::hard_link(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

#[cfg(test)]
struct PublishTestGate {
    destination: std::path::PathBuf,
    mode: PublishMode,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static PUBLISH_TEST_GATE: std::sync::OnceLock<std::sync::Mutex<Vec<PublishTestGate>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn install_test_gate(
    destination: std::path::PathBuf,
    mode: PublishMode,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
) {
    let gate = PUBLISH_TEST_GATE.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    gate.lock()
        .expect("publish test gate lock")
        .push(PublishTestGate {
            destination,
            mode,
            entered,
            release,
        });
}

#[cfg(test)]
fn wait_at_test_gate(destination: &Path, mode: PublishMode) {
    let gate = PUBLISH_TEST_GATE.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    let matching = {
        let mut current = gate.lock().expect("publish test gate lock");
        current
            .iter()
            .position(|gate| gate.destination == destination && gate.mode == mode)
            .map(|index| current.remove(index))
    };
    if let Some(gate) = matching {
        let _ = gate.entered.send(());
        let _ = gate
            .release
            .recv_timeout(std::time::Duration::from_secs(30));
    }
}

pub(crate) fn publish(
    temporary: &Path,
    destination: &Path,
    mode: PublishMode,
) -> io::Result<PublishOutcome> {
    #[cfg(test)]
    wait_at_test_gate(destination, mode);
    publish_with_ops(&LocalPublicationOps, temporary, destination, mode)
}

fn publish_with_ops(
    ops: &impl PublicationOps,
    temporary: &Path,
    destination: &Path,
    mode: PublishMode,
) -> io::Result<PublishOutcome> {
    match mode {
        PublishMode::Replace => {
            ops.rename(temporary, destination)?;
            Ok(PublishOutcome::default())
        }
        PublishMode::NoReplace => {
            // hard_link is atomic with respect to creation of the destination
            // entry and fails if any entry already occupies it, including a
            // dangling symlink. Temp and destination are siblings, so they
            // necessarily reside on the same filesystem.
            ops.hard_link(temporary, destination)?;
            let temp_cleanup_warning = ops.remove_file(temporary).err().map(|error| {
                format!(
                    "published {} but could not remove temporary file {}: {error}",
                    destination.display(),
                    temporary.display()
                )
            });
            Ok(PublishOutcome {
                temp_cleanup_warning,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::config::{EngineConfig, OverwritePolicy};
    use crate::error::ErrorCategory;
    use crate::http::probe::ProbeMetadata;
    use crate::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
    use crate::http::HttpExecution;
    use crate::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};

    fn write_temp(directory: &Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = directory.join(name);
        std::fs::write(&path, bytes).expect("write temp");
        path
    }

    #[test]
    fn no_replace_publishes_complete_file_and_removes_temp_name() {
        let directory = tempfile::tempdir().expect("tempdir");
        let temporary = write_temp(directory.path(), "output.part", b"complete");
        let destination = directory.path().join("output.bin");

        let result =
            publish(&temporary, &destination, PublishMode::NoReplace).expect("no-replace publish");
        assert!(result.temp_cleanup_warning.is_none());
        assert_eq!(
            std::fs::read(&destination).expect("read final"),
            b"complete"
        );
        assert!(!temporary.exists());
    }

    #[test]
    fn no_replace_preserves_existing_destination_and_temp() {
        let directory = tempfile::tempdir().expect("tempdir");
        let temporary = write_temp(directory.path(), "output.part", b"new bytes");
        let destination = write_temp(directory.path(), "output.bin", b"old bytes");

        let error = publish(&temporary, &destination, PublishMode::NoReplace)
            .expect_err("existing destination must conflict");
        assert!(matches!(
            error.kind(),
            ErrorKind::AlreadyExists | ErrorKind::Other
        ));
        assert_eq!(
            std::fs::read(&destination).expect("old destination"),
            b"old bytes"
        );
        assert_eq!(
            std::fs::read(&temporary).expect("temp preserved"),
            b"new bytes"
        );
    }

    #[cfg(unix)]
    #[test]
    fn no_replace_rejects_dangling_symlink_entry() {
        let directory = tempfile::tempdir().expect("tempdir");
        let temporary = write_temp(directory.path(), "output.part", b"new bytes");
        let destination = directory.path().join("output.bin");
        let missing_target = directory.path().join("missing-target");
        std::os::unix::fs::symlink(&missing_target, &destination).expect("dangling symlink");

        assert!(publish(&temporary, &destination, PublishMode::NoReplace).is_err());
        assert_eq!(
            std::fs::read(&temporary).expect("temp preserved"),
            b"new bytes"
        );
        assert_eq!(
            std::fs::read_link(&destination).expect("symlink preserved"),
            missing_target
        );
    }

    #[test]
    fn replace_atomically_replaces_old_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let temporary = write_temp(directory.path(), "output.part", b"new bytes");
        let destination = write_temp(directory.path(), "output.bin", b"old bytes");

        publish(&temporary, &destination, PublishMode::Replace).expect("replace publish");
        assert_eq!(
            std::fs::read(&destination).expect("new destination"),
            b"new bytes"
        );
        assert!(!temporary.exists());
    }

    #[test]
    fn replace_is_never_observed_as_missing_or_partial() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;

        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("observed.bin");
        let old = vec![0x31; 256 * 1024];
        let new = vec![0xA7; 256 * 1024];
        std::fs::write(&destination, &old).expect("write old destination");
        let old_for_observer = old.clone();
        let new_for_observer = new.clone();
        let done = Arc::new(AtomicBool::new(false));
        let observations = Arc::new(AtomicUsize::new(0));
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let observer_done = done.clone();
        let observer_count = observations.clone();
        let observer = std::thread::spawn(move || {
            let initial = std::fs::read(&destination).expect("initial destination");
            assert_eq!(initial, old_for_observer);
            ready_tx.send(()).expect("signal observer ready");
            while !observer_done.load(Ordering::SeqCst) {
                let observed = std::fs::read(&destination).expect("destination remains visible");
                assert!(
                    observed == old_for_observer || observed == new_for_observer,
                    "reader observed partial or unexpected bytes"
                );
                observer_count.fetch_add(1, Ordering::SeqCst);
                std::thread::yield_now();
            }
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("observer starts before replacement");

        for index in 0..20 {
            let temporary = directory.path().join(format!("replace-{index}.part"));
            let bytes = if index % 2 == 0 { &new } else { &old };
            std::fs::write(&temporary, bytes).expect("write replacement temp");
            publish(
                &temporary,
                &directory.path().join("observed.bin"),
                PublishMode::Replace,
            )
            .expect("atomic replacement");
        }
        done.store(true, Ordering::SeqCst);
        observer.join().expect("reader thread");
        assert!(observations.load(Ordering::SeqCst) > 0);
        assert_eq!(
            std::fs::read(directory.path().join("observed.bin")).expect("final"),
            old
        );
    }

    #[test]
    fn failed_replace_preserves_old_destination_bytes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let missing_temp = directory.path().join("missing.part");
        let destination = write_temp(directory.path(), "output.bin", b"old bytes");

        assert!(publish(&missing_temp, &destination, PublishMode::Replace).is_err());
        assert_eq!(
            std::fs::read(&destination).expect("old destination"),
            b"old bytes"
        );
    }

    struct UnsupportedOps;

    impl PublicationOps for UnsupportedOps {
        fn rename(&self, _: &Path, _: &Path) -> io::Result<()> {
            Err(io::Error::new(
                ErrorKind::Unsupported,
                "atomic replace unavailable",
            ))
        }

        fn hard_link(&self, _: &Path, _: &Path) -> io::Result<()> {
            Err(io::Error::new(
                ErrorKind::Unsupported,
                "no-replace unavailable",
            ))
        }

        fn remove_file(&self, _: &Path) -> io::Result<()> {
            panic!("unsupported publication must fail before cleanup")
        }
    }

    #[test]
    fn unsupported_publication_fails_closed_without_touching_old_bytes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let temporary = write_temp(directory.path(), "output.part", b"new bytes");
        let destination = write_temp(directory.path(), "output.bin", b"old bytes");

        for mode in [PublishMode::NoReplace, PublishMode::Replace] {
            let error = publish_with_ops(&UnsupportedOps, &temporary, &destination, mode)
                .expect_err("unsupported primitive must fail closed");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(
                std::fs::read(&destination).expect("old destination"),
                b"old bytes"
            );
            assert_eq!(
                std::fs::read(&temporary).expect("temp preserved"),
                b"new bytes"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fail_if_exists_is_enforced_at_atomic_publication() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let scripted = ScriptedHttp::new()
            .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
                status: 200,
                total_size: Some(5),
                ..ProbeMetadata::default()
            }))
            .expect_transfer(
                TransferStep::new().ok(TransferOk::new().total(5).chunk(b"hello".as_slice())),
            );
        let controller = SingleStreamController::with_execution(
            HttpExecution::from_adapter(scripted.clone()),
            EngineConfig::default(),
        );
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        install_test_gate(
            destination.clone(),
            PublishMode::NoReplace,
            entered_tx,
            release_rx,
        );
        let mut request =
            DownloadRequest::new("https://publish.example.test/file", destination.clone());
        request.overwrite = OverwritePolicy::FailIfExists;
        let (handle, task) = controller.start(request);
        let mut events = handle.events();

        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("gate waiter task")
            .expect("job reached publication gate");
        std::fs::write(&destination, b"concurrent writer").expect("create destination at gate");
        release_tx.send(()).expect("release publication gate");

        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("job reaches a terminal outcome")
            .expect("job task")
            .expect("structured terminal result");
        assert_eq!(result.status, ResultStatus::Failed);
        assert_eq!(
            result.error.as_ref().map(|error| error.category()),
            Some(ErrorCategory::DestinationConflict)
        );
        assert_eq!(
            std::fs::read(&destination).expect("read competing destination"),
            b"concurrent writer"
        );
        let committed = std::iter::from_fn(|| events.try_next())
            .any(|event| matches!(event, crate::metrics::events::Event::Committed { .. }));
        assert!(
            !committed,
            "failed no-replace publication emits no committed event"
        );
        scripted.assert_all_consumed();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_replace_never_reports_completed_or_damages_old_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = write_temp(directory.path(), "output.bin", b"old destination");
        let temporary = directory.path().join("output.bin.part");
        let scripted = ScriptedHttp::new()
            .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
                status: 200,
                total_size: Some(5),
                ..ProbeMetadata::default()
            }))
            .expect_transfer(
                TransferStep::new().ok(TransferOk::new().total(5).chunk(b"hello".as_slice())),
            );
        let controller = SingleStreamController::with_execution(
            HttpExecution::from_adapter(scripted.clone()),
            EngineConfig::default(),
        );
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        install_test_gate(
            destination.clone(),
            PublishMode::Replace,
            entered_tx,
            release_rx,
        );
        let mut request =
            DownloadRequest::new("https://publish.example.test/replace", destination.clone());
        request.overwrite = OverwritePolicy::Replace;
        let (handle, task) = controller.start(request);
        let mut events = handle.events();

        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("gate waiter task")
            .expect("job reached publication gate");
        std::fs::remove_file(&temporary).expect("inject missing temp at publication");
        release_tx.send(()).expect("release publication gate");

        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("job reaches a terminal outcome")
            .expect("job task")
            .expect("structured terminal result");
        assert_eq!(result.status, ResultStatus::Failed);
        assert_eq!(
            result.error.as_ref().map(|error| error.category()),
            Some(ErrorCategory::Commit)
        );
        assert_eq!(
            std::fs::read(&destination).expect("read previous destination"),
            b"old destination"
        );
        let committed = std::iter::from_fn(|| events.try_next())
            .any(|event| matches!(event, crate::metrics::events::Event::Committed { .. }));
        assert!(!committed, "failed replacement emits no committed event");
        scripted.assert_all_consumed();
    }
}
