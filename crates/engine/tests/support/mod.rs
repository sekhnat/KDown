//! Shared test support modules. Included via `#[path]` by integration tests.

// Support APIs are consumed incrementally by integration test binaries;
// not every binary uses every helper yet.
#![allow(dead_code)]

pub mod fixtures;
pub mod test_server;
