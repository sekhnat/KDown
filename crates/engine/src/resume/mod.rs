//! Resumable state: versioned checkpoint model, atomic sidecar store,
//! durable-interval tracking, and resume orchestration (§15, D3, D4).

pub(crate) mod checkpoint;
pub(crate) mod checkpoint_store;
pub(crate) mod coordinated_store;
pub(crate) mod durable_ranges;
pub(crate) mod flow;

// Mod-level re-exports consumed by relocated internal tests; the
// supported surface re-exports the same items at the crate root.
#[allow(unused_imports)]
pub(crate) use checkpoint::{ByteRange, Checkpoint, CheckpointError, CHECKPOINT_FORMAT_VERSION};
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use checkpoint_store::{
    CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver, DurabilityMode,
    FileCheckpointStore, SidecarCheckpointResolver,
};
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use flow::job_identity;
