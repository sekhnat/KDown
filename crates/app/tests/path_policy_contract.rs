use std::sync::Mutex;

use kdown_app::domain::RootId;
use kdown_app::path_policy::PathPolicy;
use kdown_app::registry::Registry;

static FIXTURE_ROOT: Mutex<Option<RootId>> = Mutex::new(None);

/// The brief's fixture exposes the root id through `root_id()`; a process
/// global therefore serializes the tests that set it.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn root_id() -> RootId {
    FIXTURE_ROOT
        .lock()
        .unwrap()
        .expect("fixture root not initialized")
}

struct TestDb {
    dir: tempfile::TempDir,
}

impl TestDb {
    async fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    async fn registry(&self) -> Registry {
        let registry = Registry::connect(self.dir.path().join("kdown.db"))
            .await
            .unwrap();
        registry.migrate().await.unwrap();
        registry
    }
}

struct PolicyFixture {
    _db: TestDb,
    policy: PathPolicy,
}

impl std::ops::Deref for PolicyFixture {
    type Target = PathPolicy;

    fn deref(&self) -> &PathPolicy {
        &self.policy
    }
}

async fn fixture_policy_with_root(root: &std::path::Path) -> PolicyFixture {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let policy = PathPolicy::new(registry.clone());
    let record = policy
        .add_root("Downloads", root, true)
        .await
        .expect("fixture root must be addable");
    *FIXTURE_ROOT.lock().unwrap() = Some(record.id);
    PolicyFixture { _db: db, policy }
}

#[tokio::test]
#[cfg(unix)]
async fn launch_recheck_rejects_parent_symlink_swapped_outside_root() {
    let _serial = SERIAL.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("downloads");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(root.join("safe")).unwrap();
    std::fs::create_dir_all(&outside).unwrap();

    let policy = fixture_policy_with_root(&root).await;
    assert!(policy
        .resolve_for_launch(root_id(), "safe/file.iso")
        .await
        .is_ok());

    std::fs::remove_dir(root.join("safe")).unwrap();
    std::os::unix::fs::symlink(&outside, root.join("safe")).unwrap();

    let error = policy
        .resolve_for_launch(root_id(), "safe/file.iso")
        .await
        .unwrap_err();
    assert_eq!(error.code(), "destination_outside_root");
}

#[tokio::test]
async fn valid_nested_destination_prepares_parent_inside_root() {
    let _serial = SERIAL.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("downloads");
    std::fs::create_dir_all(&root).unwrap();

    let policy = fixture_policy_with_root(&root).await;
    let resolved = policy
        .resolve_for_launch(root_id(), "iso/2026")
        .await
        .unwrap();

    let canonical_root = root.canonicalize().unwrap();
    assert_eq!(resolved.canonical_root, canonical_root);
    // Parent directories are prepared; the final component is left for the
    // engine's publication or the supervisor's directory creation.
    assert!(resolved
        .destination
        .parent()
        .is_some_and(|parent| parent.is_dir()));
    assert!(!resolved.destination.exists());
    assert!(resolved.destination.starts_with(canonical_root));
}

#[tokio::test]
async fn parent_traversal_is_rejected() {
    let _serial = SERIAL.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("downloads");
    std::fs::create_dir_all(&root).unwrap();

    let policy = fixture_policy_with_root(&root).await;
    let error = policy
        .resolve_for_launch(root_id(), "../outside/file.iso")
        .await
        .unwrap_err();
    assert_eq!(error.code(), "destination_outside_root");
}

#[tokio::test]
async fn nul_bytes_are_rejected() {
    let _serial = SERIAL.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("downloads");
    std::fs::create_dir_all(&root).unwrap();

    let policy = fixture_policy_with_root(&root).await;
    let error = policy
        .resolve_for_launch(root_id(), "safe\0file")
        .await
        .unwrap_err();
    assert_eq!(error.code(), "destination_outside_root");
}

#[tokio::test]
async fn disabled_root_is_unavailable() {
    let _serial = SERIAL.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("downloads");
    std::fs::create_dir_all(&root).unwrap();

    let policy = fixture_policy_with_root(&root).await;
    policy.registry().disable_root(root_id()).await.unwrap();

    let error = policy
        .resolve_for_launch(root_id(), "file.iso")
        .await
        .unwrap_err();
    assert_eq!(error.code(), "root_unavailable");
}
