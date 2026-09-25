//! Optional physical allocation tests (tasks 11.1, 11.2).
//!
//! Physical reservation is opt-in and never a correctness dependency:
//! unsupported platforms/filesystems fall back to logical sizing silently,
//! while genuine out-of-space and permission failures surface as sink
//! errors and never publish incomplete output.

#[path = "support/mod.rs"]
mod support;

use std::io::BufRead as _;
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, TransferPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadController, DownloadRequest, ResultStatus};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};

fn cfg(physical: bool) -> EngineConfig {
    let mut c = EngineConfig {
        transfer: TransferPolicy {
            ..TransferPolicy::default()
        },
        ..EngineConfig::default()
    };
    c.transfer.preallocate_output = true;
    c.transfer.preallocate_physical = physical;
    c.network.response_header_timeout = Duration::from_secs(30);
    c
}

fn controller(cfg: EngineConfig) -> DownloadController {
    DownloadController::new(
        HttpTransport::new(cfg.network.clone()).expect("transport"),
        cfg,
    )
}

/// The isolated fixture server serves the deterministic file; the client
/// exercises preallocation paths against it.
fn spawn_fixture_server(size: &str, seed: u64) -> (std::process::Child, String, String) {
    let bin = env!("CARGO_BIN_EXE_fixture_server");
    let mut child = std::process::Command::new(bin)
        .args([
            "--size",
            size,
            "--seed",
            &seed.to_string(),
            "--addr",
            "127.0.0.1:0",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn fixture_server");
    let stdout = child.stdout.take().expect("stdout");
    let mut addr = None;
    let mut sha = None;
    let started = std::time::Instant::now();
    for line in std::io::BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if let Some(a) = line.strip_prefix("LISTENING ") {
            addr = Some(a.to_string());
        } else if let Some(h) = line.strip_prefix("SHA256 ") {
            sha = Some(h.to_string());
        } else if line == "READY" {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "fixture_server did not report READY"
        );
    }
    (child, addr.expect("LISTENING"), sha.expect("SHA256"))
}

/// Unsupported physical allocation (task 11.1): on a filesystem where the
/// fallocate-style reservation is unsupported (tmpfs), the download falls
/// back to logical sizing and completes with correct content — the
/// fallback changes no correctness behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_physical_allocation_falls_back_and_completes() {
    let (_child, addr, sha) = spawn_fixture_server("1MiB", 4401);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let c = controller(cfg(true));
    let result = c
        .run(DownloadRequest::new(
            format!("http://{addr}/f.bin"),
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    let path = result.final_path.expect("published");
    assert_eq!(support::fixtures::file_sha256(&path), sha, "content exact");
    // The temp is gone; the destination holds the full file.
    assert!(!dir.path().join("out.bin.part").exists());
    assert_eq!(std::fs::metadata(&dest).expect("size").len(), 1024 * 1024);
}

/// Physical allocation disabled (default): identical completion semantics —
/// allocation is never necessary for correctness (task 11.1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_only_allocation_completes_identically() {
    let (_child, addr, sha) = spawn_fixture_server("1MiB", 4402);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let c = controller(cfg(false));
    let result = c
        .run(DownloadRequest::new(
            format!("http://{addr}/f.bin"),
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    let path = result.final_path.expect("published");
    assert_eq!(support::fixtures::file_sha256(&path), sha, "content exact");
}

/// A real storage failure (unreadable destination directory) surfaces as a
/// sink error and NEVER publishes incomplete output (task 11.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_permission_failure_surfaces_and_never_publishes() {
    let content = deterministic_bytes(64 * 1024, 4403);
    let server = support::test_server::TestServer::new()
        .serve_static("/perm.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    // A directory without write permission: opening the temp file fails.
    let blocked = dir.path().join("blocked");
    std::fs::create_dir(&blocked).expect("mkdir");
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o500))
            .expect("chmod read-only");
    }
    let dest = blocked.join("out.bin");
    let c = controller(cfg(true));
    let result = c
        .run(DownloadRequest::new(server.url("/perm.bin"), dest.clone()))
        .await
        .expect("terminal");
    // Restore permissions so the tempdir can be cleaned up.
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700))
            .expect("restore permissions");
    }
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(result.final_path.is_none(), "nothing published");
    assert!(!dest.exists(), "no partial output at the destination");
    assert!(!blocked.join("out.bin.part").exists(), "no temp residue");
    // The permission failure surfaces as a structured error (the
    // destination lockfile/PermissionDenied family) — never silently.
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            kdown_engine::DownloadError::SinkOpen(_)
                | kdown_engine::DownloadError::PermissionDenied(_)
                | kdown_engine::DownloadError::Commit(_)
        ),
        "structured storage error: {result:?}"
    );
}

/// Retry/resume works identically for both allocation paths: a reset-heavy
/// transfer with physical reservation completes byte-exact (the fallback
/// and the reservation change no retry semantics).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allocation_paths_preserve_retry_and_resume_semantics() {
    let content = Arc::new(deterministic_bytes(1024 * 1024, 4404));
    let server = {
        let content = content.clone();
        support::test_server::TestServer::new().serve_handler("/alloc.bin", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return support::test_server::ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            if let Some((s, e)) = req.range {
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                support::test_server::ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes")
                    .reset_after(128 * 1024)
            } else {
                support::test_server::ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes")
                    .reset_after(128 * 1024)
            }
        })
    }
    .start()
    .await
    .expect("start");
    for physical in [true, false] {
        let dir = tempfile::tempdir().expect("tmpdir");
        let dest = dir.path().join("out.bin");
        let mut c_cfg = cfg(physical);
        c_cfg.transfer.segmentation_threshold = 1;
        c_cfg.transfer.max_segment_size = 256 * 1024;
        c_cfg.retry.base_delay = Duration::from_millis(10);
        let c = controller(c_cfg);
        let result = c
            .run(DownloadRequest::new(server.url("/alloc.bin"), dest.clone()))
            .await
            .expect("terminal");
        assert_eq!(
            result.status,
            ResultStatus::Completed,
            "physical={physical}: {result:?}"
        );
        assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
        assert!(!dir.path().join("out.bin.part").exists());
    }
}
