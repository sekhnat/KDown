//! Exclusive ownership of destination-associated mutable artifacts.
//!
//! The in-process registry is deliberately nonblocking: a second controller
//! targeting an already-owned destination fails before it can inspect or
//! mutate the destination's `.part` file or checkpoint.

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use fs2::FileExt;
use sha2::{Digest, Sha256};

use crate::error::DownloadError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DestinationKey {
    parent: PathBuf,
    file_name: OsString,
}

impl DestinationKey {
    fn resolve(destination: &Path) -> Result<Self, DownloadError> {
        let file_name = destination.file_name().ok_or_else(|| {
            DownloadError::Commit(format!(
                "destination has no file name: {}",
                destination.display()
            ))
        })?;
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let parent = std::fs::canonicalize(parent).map_err(|error| {
            DownloadError::Commit(format!(
                "cannot resolve destination parent {}: {error}",
                parent.display()
            ))
        })?;

        Ok(Self {
            parent,
            file_name: file_name.to_os_string(),
        })
    }

    fn describe(&self) -> String {
        format!("{}", self.parent.join(&self.file_name).display())
    }

    fn lock_path(&self) -> PathBuf {
        let mut hasher = Sha256::new();
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            hasher.update(self.file_name.as_bytes());
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            for unit in self.file_name.encode_wide() {
                hasher.update(unit.to_le_bytes());
            }
        }
        #[cfg(not(any(unix, windows)))]
        hasher.update(self.file_name.to_string_lossy().as_bytes());
        let suffix: String = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        self.parent
            .join(format!(".kdown-destination-{suffix}.lock"))
    }
}

static OWNED_DESTINATIONS: OnceLock<Mutex<HashSet<DestinationKey>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashSet<DestinationKey>> {
    OWNED_DESTINATIONS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn release_registration(key: &DestinationKey) {
    if let Ok(mut owned) = registry().lock() {
        owned.remove(key);
    }
}

/// Process-local, nonblocking destination ownership guard.
///
/// An in-process registration plus an OS-managed cross-process lock.
///
/// Dropping the guard unlocks the persistent lockfile and releases the
/// process-local registration; the lockfile itself is never removed.
#[derive(Debug)]
pub(crate) struct DestinationLease {
    key: DestinationKey,
    lock_file: File,
}

impl DestinationLease {
    /// Try to acquire exclusive ownership of `destination` without waiting.
    pub(crate) fn acquire(destination: &Path) -> Result<Self, DownloadError> {
        let key = DestinationKey::resolve(destination)?;
        let mut owned = registry().lock().map_err(|_| {
            DownloadError::Commit("destination ownership registry is poisoned".into())
        })?;
        if !owned.insert(key.clone()) {
            return Err(DownloadError::DestinationConflict(format!(
                "destination is already owned by an active job: {}",
                key.describe()
            )));
        }
        drop(owned);

        let lock_path = key.lock_path();
        let lock_file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(error) => {
                release_registration(&key);
                return Err(DownloadError::Commit(format!(
                    "cannot open destination lockfile {}: {error}",
                    lock_path.display()
                )));
            }
        };
        if let Err(error) = lock_file.try_lock_exclusive() {
            drop(lock_file);
            release_registration(&key);
            if error.kind() == fs2::lock_contended_error().kind() {
                return Err(DownloadError::DestinationConflict(format!(
                    "destination is already owned by another process: {}",
                    key.describe()
                )));
            }
            return Err(DownloadError::Commit(format!(
                "cannot lock destination lockfile {}: {error}",
                lock_path.display()
            )));
        }

        Ok(Self { key, lock_file })
    }
}

impl Drop for DestinationLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
        release_registration(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_destination_is_nonblocking_and_released_on_drop() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");

        let first = DestinationLease::acquire(&destination).expect("first lease");
        let second = DestinationLease::acquire(&destination);
        assert!(matches!(second, Err(DownloadError::DestinationConflict(_))));
        drop(first);
        assert!(DestinationLease::acquire(&destination).is_ok());
    }

    #[test]
    fn normalized_and_parent_alias_paths_share_one_key() {
        let directory = tempfile::tempdir().expect("tempdir");
        let parent = directory.path().join("parent");
        std::fs::create_dir(&parent).expect("parent");
        let destination = parent.join("output.bin");
        let normalized_alias = parent.join("nested").join("..").join("output.bin");
        std::fs::create_dir(parent.join("nested")).expect("nested");

        let first = DestinationLease::acquire(&destination).expect("first lease");
        assert!(DestinationLease::acquire(&normalized_alias).is_err());
        drop(first);

        #[cfg(unix)]
        {
            let parent_alias = directory.path().join("parent-alias");
            std::os::unix::fs::symlink(&parent, &parent_alias).expect("parent symlink");
            let _first = DestinationLease::acquire(&destination).expect("first lease");
            assert!(DestinationLease::acquire(&parent_alias.join("output.bin")).is_err());
        }
    }

    #[test]
    fn different_destinations_in_one_directory_are_independent() {
        let directory = tempfile::tempdir().expect("tempdir");
        let first = DestinationLease::acquire(&directory.path().join("first.bin"))
            .expect("first destination");
        let second = DestinationLease::acquire(&directory.path().join("second.bin"))
            .expect("distinct destination");
        drop((first, second));
    }

    #[test]
    fn unlocked_lockfile_is_persistent_and_reused() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let lock_path = DestinationKey::resolve(&destination)
            .expect("key")
            .lock_path();
        {
            let _first = DestinationLease::acquire(&destination).expect("first lease");
            assert!(lock_path.exists(), "lockfile is created before transfer");
        }
        assert!(lock_path.exists(), "unlocked lockfile is never unlinked");
        let _second = DestinationLease::acquire(&destination).expect("reuse unlocked lockfile");
    }

    #[test]
    fn lease_child_process() {
        let Some(destination) = std::env::var_os("KDOWN_LEASE_TEST_DESTINATION") else {
            return;
        };
        let ready = std::env::var_os("KDOWN_LEASE_TEST_READY").expect("ready path");
        let release = std::env::var_os("KDOWN_LEASE_TEST_RELEASE").expect("release path");
        let _lease = DestinationLease::acquire(Path::new(&destination))
            .expect("child obtains destination lease");
        std::fs::write(&ready, b"ready").expect("signal ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !Path::new(&release).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "parent did not release child"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn spawn_lease_child(destination: &Path, ready: &Path, release: &Path) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "io::destination_lease::tests::lease_child_process",
                "--nocapture",
            ])
            .env("KDOWN_LEASE_TEST_DESTINATION", destination)
            .env("KDOWN_LEASE_TEST_READY", ready)
            .env("KDOWN_LEASE_TEST_RELEASE", release)
            .spawn()
            .expect("spawn child test process")
    }

    fn wait_for_child_ready(child: &mut std::process::Child, ready: &Path) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !ready.exists() {
            if let Some(status) = child.try_wait().expect("poll child") {
                panic!("child exited before acquiring lease: {status}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "child did not acquire lease"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn cross_process_contention_releases_on_exit_and_crash() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let ready = directory.path().join("ready");
        let release = directory.path().join("release");
        let mut child = spawn_lease_child(&destination, &ready, &release);
        wait_for_child_ready(&mut child, &ready);
        let contention = DestinationLease::acquire(&destination);
        assert!(
            matches!(&contention, Err(DownloadError::DestinationConflict(_))),
            "expected destination conflict while child owns lock, got {contention:?}"
        );
        std::fs::write(&release, b"release").expect("release child");
        assert!(child.wait().expect("wait for clean child exit").success());
        let lock_path = DestinationKey::resolve(&destination)
            .expect("key")
            .lock_path();
        assert!(lock_path.exists(), "clean release leaves lockfile in place");
        let lease =
            DestinationLease::acquire(&destination).expect("OS releases lock when owner exits");
        drop(lease);

        let ready = directory.path().join("ready-after-kill");
        let release = directory.path().join("unused-release");
        let mut child = spawn_lease_child(&destination, &ready, &release);
        wait_for_child_ready(&mut child, &ready);
        let contention = DestinationLease::acquire(&destination);
        assert!(
            matches!(&contention, Err(DownloadError::DestinationConflict(_))),
            "expected destination conflict while child owns lock, got {contention:?}"
        );
        child.kill().expect("kill lock owner");
        let _ = child.wait().expect("reap killed child");
        let _lease =
            DestinationLease::acquire(&destination).expect("OS releases lock when process crashes");
    }
}
