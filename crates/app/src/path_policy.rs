//! Configured-root canonicalization and launch-time destination validation.
//!
//! Ordinary job requests submit an opaque root ID plus a relative
//! destination; adding a root is the explicit authorization step and may
//! submit an absolute existing directory. Every launch re-resolves the
//! destination immediately before the engine starts, so a symlink swapped
//! between enqueue and launch fails closed instead of escaping the root.

use std::path::{Component, Path, PathBuf};

use crate::domain::{RootId, RootRecord};
use crate::error::AppError;
use crate::registry::Registry;

/// A validated launch destination. `destination` is the directory the
/// engine will download into when no filename override exists, or the full
/// file path when an override was joined by the supervisor.
#[derive(Clone, Debug)]
pub struct ResolvedDestination {
    pub root_id: RootId,
    pub canonical_root: PathBuf,
    pub destination: PathBuf,
    pub filename_override: Option<String>,
}

/// Path policy over the registry's configured roots.
#[derive(Clone, Debug)]
pub struct PathPolicy {
    registry: Registry,
}

impl PathPolicy {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Registry access for callers composing policy with persistence.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Adds an existing directory as a configured download root. The stored
    /// path is canonical; `make_default` records it as the default root.
    pub async fn add_root(
        &self,
        label: &str,
        path: &Path,
        make_default: bool,
    ) -> Result<RootRecord, AppError> {
        self.registry.add_root(label, path, make_default).await
    }

    /// Resolves and re-validates a launch destination immediately before
    /// engine startup. Missing directories are created, the resulting path
    /// is re-canonicalized, and containment is verified against the root —
    /// so a parent swapped to a symlink outside the root after enqueue is
    /// rejected here, not by the engine.
    pub async fn resolve_for_launch(
        &self,
        root_id: RootId,
        relative: &str,
    ) -> Result<ResolvedDestination, AppError> {
        let root = self.registry.load_enabled_root(root_id).await?;
        let canonical_root = tokio::fs::canonicalize(&root.canonical_path)
            .await
            .map_err(|_| AppError::RootUnavailable)?;
        let components = validate_relative_components(relative)?;

        // Walk the existing prefix, canonicalizing at every step so symlink
        // components resolve to their targets; collect the missing tail.
        let mut current = canonical_root.clone();
        let mut missing: Vec<String> = Vec::new();
        for (index, component) in components.iter().enumerate() {
            let next = current.join(component);
            if tokio::fs::symlink_metadata(&next).await.is_ok() {
                current = tokio::fs::canonicalize(&next)
                    .await
                    .map_err(|_| AppError::DestinationUnavailable)?;
            } else {
                missing.extend(components[index..].iter().cloned());
                break;
            }
        }

        // The final component is never created here: it is the file the
        // engine produces, or a directory the supervisor creates and
        // re-verifies before a directory-target launch.
        let destination = if missing.is_empty() {
            current
        } else {
            let (last, parents) = missing.split_last().expect("non-empty missing tail");
            let parent = parents
                .iter()
                .fold(current.clone(), |acc, part| acc.join(part));
            if !parents.is_empty() {
                tokio::fs::create_dir_all(&parent)
                    .await
                    .map_err(|_| AppError::DestinationUnavailable)?;
            }
            let parent = tokio::fs::canonicalize(&parent)
                .await
                .map_err(|_| AppError::DestinationUnavailable)?;
            parent.join(last)
        };
        if !destination.starts_with(&canonical_root) {
            return Err(AppError::DestinationOutsideRoot);
        }
        Ok(ResolvedDestination {
            root_id,
            canonical_root,
            destination,
            filename_override: None,
        })
    }
}

/// Validates a relative destination as plain components: no root or prefix
/// components, no parent traversal, no current-directory components, and
/// no NUL bytes.
fn validate_relative_components(relative: &str) -> Result<Vec<String>, AppError> {
    if relative.contains('\0') {
        return Err(AppError::DestinationOutsideRoot);
    }
    let path = Path::new(relative);
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => components.push(part.to_string_lossy().into_owned()),
            Component::RootDir
            | Component::Prefix(_)
            | Component::ParentDir
            | Component::CurDir => {
                return Err(AppError::DestinationOutsideRoot);
            }
        }
    }
    Ok(components)
}

/// Validates an explicit filename override as exactly one normal component.
pub fn validate_filename(name: &str) -> Result<(), AppError> {
    if name.is_empty()
        || name.contains('\0')
        || name.contains('/')
        || name.contains('\\')
        || name == "."
        || name == ".."
    {
        return Err(AppError::DestinationOutsideRoot);
    }
    Ok(())
}
