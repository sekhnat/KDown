//! Platform integration surface: desktop actions the host performs on the
//! user's behalf. The Linux implementation runs `xdg-open` with one
//! argument and never through a shell.

use std::path::Path;
use std::sync::Arc;

use crate::error::AppError;

/// Injectable desktop action; the Linux implementation shells out to
/// nothing: it spawns `xdg-open` directly.
type RevealAction = Arc<dyn Fn(&Path) -> Result<(), AppError> + Send + Sync>;

/// Desktop integration injected at startup; tests substitute a no-op.
#[derive(Clone)]
pub struct DesktopIntegration {
    reveal_parent: RevealAction,
}

impl std::fmt::Debug for DesktopIntegration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DesktopIntegration").finish_non_exhaustive()
    }
}

impl DesktopIntegration {
    /// The Linux implementation: spawn `xdg-open` on the parent directory.
    pub fn native() -> Self {
        Self {
            reveal_parent: Arc::new(|parent| {
                tokio::process::Command::new("xdg-open")
                    .arg(parent)
                    .spawn()
                    .map(|_| ())
                    .map_err(|error| AppError::DesktopReveal(error.to_string()))
            }),
        }
    }

    /// Test double that records nothing and fails nothing.
    pub fn no_op() -> Self {
        Self {
            reveal_parent: Arc::new(|_| Ok(())),
        }
    }

    /// Reveals a directory in the platform file manager.
    pub async fn reveal_parent(&self, parent: &Path) -> Result<(), AppError> {
        (self.reveal_parent)(parent)
    }
}
