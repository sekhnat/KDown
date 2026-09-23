//! Resumable state: versioned checkpoint model, atomic sidecar store,
//! durable-interval tracking, and resume orchestration (§15, D3, D4).

pub mod checkpoint;
pub mod checkpoint_store;
pub(crate) mod coordinated_store;
pub mod durable_ranges;
pub mod flow;

pub use checkpoint::{ByteRange, Checkpoint, CheckpointError, CHECKPOINT_FORMAT_VERSION};
pub use checkpoint_store::{
    CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver, DurabilityMode,
    FileCheckpointStore, SidecarCheckpointResolver,
};
pub use flow::{job_identity, GenerationChangePolicy};
