//! Destination-lease platform contract (task 4.4): a second exclusive
//! acquisition of the SAME destination must fail typed on every platform
//! (flock on POSIX, LockFileEx on Windows); the failure must not disturb
//! the holder, and the lease is re-acquirable after release.
//!
//! Crate-internal: the lease type is not consumer surface.

use kdown_engine::DownloadError;

#[test]
fn destination_lease_conflict_is_typed_and_releasable() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let first =
        crate::io::destination_lease::DestinationLease::acquire(&dest).expect("first lease");
    let second = crate::io::destination_lease::DestinationLease::acquire(&dest);
    assert!(
        matches!(second, Err(DownloadError::DestinationConflict(_))),
        "the second lease must conflict: {second:?}"
    );
    drop(first);
    assert!(
        crate::io::destination_lease::DestinationLease::acquire(&dest).is_ok(),
        "the lease is re-acquirable after release"
    );
}
