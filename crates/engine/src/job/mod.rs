//! Job lifecycle management (§9).

pub mod controller;
pub mod segmented;
pub mod state;

pub use controller::{DownloadHandle, DownloadRequest, DownloadResult, ResultStatus};
pub use state::{InvalidTransition, JobState, StateMachine};
