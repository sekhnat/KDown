//! HTTP transport layer (§11, §32): request execution, redirect policy,
//! probe, validators.

// Transport construction, policy, and response-metadata types are the
// supported HTTP surface (docs/api-surface.md).
pub mod connect;
pub mod redirect;
pub mod transport;
pub mod validators;

// Execution/injection internals: the scripted seam is crate-internal
// (consumer-api task 2.3); it never appears in the supported surface.
// Relocated internal tests exercise the whole surface, so the non-test
// build tolerates its unused remainder as dead code.
#[allow(dead_code)]
pub(crate) mod execution;
#[allow(dead_code)]
pub(crate) mod probe;
#[allow(dead_code)]
pub(crate) mod range;
#[allow(dead_code)]
pub(crate) mod scripted;

pub use connect::{
    ConnectError, ConnectionLimits, HttpProtocol, HttpProtocolStats, H2_FLOW_CONTROL_INSTRUMENTED,
};
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use execution::{
    BodyEvent, FullResponsePolicy, HttpBody, HttpExecution, HttpFailure, ProbeRequest, RangeIntent,
    TransferIntent, TransferRequest, TransferResponse,
};
pub(crate) use probe::filename_from_disposition;
// Test-only convenience re-export (relocated internal tests).
#[allow(unused_imports)]
pub(crate) use probe::ProbeMetadata;
pub use redirect::{RedirectAction, RedirectDecision, RedirectPolicy, RedirectTracker};
pub use transport::{HttpTransport, RequestSpec};
pub use validators::{ContentRange, ResourceValidators};
