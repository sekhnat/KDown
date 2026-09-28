//! Job lifecycle management (§9).

pub(crate) mod controller;
pub(crate) mod naming;
pub(crate) mod segmented;
pub(crate) mod state;

// Relocated internal tests address JobState through this module path.
#[allow(unused_imports)]
pub(crate) use state::JobState;
