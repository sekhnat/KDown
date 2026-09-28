//! Internal test relocation (consumer-api task 2.2).
//!
//! These integration tests exercise crate internals (the scripted HTTP
//! seam, scheduler, resume, and I/O state machines). They compile as
//! crate-internal modules so they keep full internal access while the
//! external `kdown_engine::` import paths stay valid through the test-only
//! crate alias (`extern crate self as kdown_engine`). They are not consumer
//! facade tests: see `tests/external_consumer_fixture.rs` for the supported
//! surface proof.

// Shared support compiled once for all relocated internal test modules
// (the per-file `mod support;` declarations were removed in favor of
// `super::support::…` paths).
#[path = "../../tests/support/mod.rs"]
mod support;

#[path = "../../tests/checkpoint_seam_tests.rs"]
mod checkpoint_seam_tests;

#[path = "../../tests/controller_compat_tests.rs"]
mod controller_compat_tests;

#[path = "../../tests/crash_restart_tests.rs"]
mod crash_restart_tests;

#[path = "../../tests/execution_conformance_tests.rs"]
mod execution_conformance_tests;

#[path = "../../tests/execution_seam_tests.rs"]
mod execution_seam_tests;

#[path = "../../tests/mode_parity_tests.rs"]
mod mode_parity_tests;

#[path = "../../tests/phase3_exit_tests.rs"]
mod phase3_exit_tests;

#[path = "../../tests/range_validation_tests.rs"]
mod range_validation_tests;

#[path = "../../tests/request_redaction_tests.rs"]
mod request_redaction_tests;

#[path = "../../tests/resume_tests.rs"]
mod resume_tests;

#[path = "../../tests/scheduler_property_tests.rs"]
mod scheduler_property_tests;

#[path = "../../tests/segmented_tests.rs"]
mod segmented_tests;

#[path = "../../tests/single_stream_tests.rs"]
mod single_stream_tests;

#[path = "../../tests/security_tests.rs"]
mod security_tests;

#[path = "../../tests/transport_integration.rs"]
mod transport_integration;

#[path = "../../tests/ingress_bound_tests.rs"]
mod ingress_bound_tests;

#[path = "../../tests/transfer_memory_tests.rs"]
mod transfer_memory_tests;

#[path = "../../tests/metrics_transfer_tests.rs"]
mod metrics_transfer_tests;

#[path = "../../tests/sink_fault_tests.rs"]
mod sink_fault_tests;

#[path = "../../tests/platform_lease_tests.rs"]
mod platform_lease_tests;

#[path = "../../tests/directory_resolution_tests.rs"]
mod directory_resolution_tests;
#[path = "../../tests/regression_tests.rs"]
mod regression_tests;
