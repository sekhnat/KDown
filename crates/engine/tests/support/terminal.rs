//! Typed terminal-outcome helpers for the post-0.1 result API.
//!
//! A successful terminal result is always a verified, published
//! [`CompletedDownload`]; every other outcome is a typed
//! [`DownloadRunError`]. These helpers keep integration tests honest about
//! which outcome they require.

use kdown_engine::error::{CompletedDownload, DownloadRunError};
use kdown_engine::{DownloadController, DownloadRequest};

/// Run one download and require a verified, published completion.
///
/// # Panics
/// When the run ends in any non-success terminal outcome.
pub async fn run_completed(
    controller: &DownloadController,
    request: DownloadRequest,
) -> CompletedDownload {
    match controller.run(request).await {
        Ok(completed) => completed,
        Err(error) => panic!("expected a completed download, got {error:?}"),
    }
}

/// Run one download and require a non-success terminal outcome.
///
/// # Panics
/// When the run reports a verified, published completion.
pub async fn run_error(
    controller: &DownloadController,
    request: DownloadRequest,
) -> DownloadRunError {
    match controller.run(request).await {
        Ok(completed) => panic!("expected a terminal error, got completed: {completed:?}"),
        Err(error) => error,
    }
}
