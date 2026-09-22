//! HTTP transport layer (§11, §32): request execution, redirect policy,
//! probe, validators.

pub mod connect;
pub mod probe;
pub mod range;
pub mod redirect;
pub mod transport;
pub mod validators;

pub use connect::{ConnectError, ConnectionLimits};
pub use probe::{filename_from_disposition, ProbeMetadata};
pub use range::{RangeRejection, RejectionKind, ValidatedRange};
pub use redirect::{RedirectAction, RedirectDecision, RedirectPolicy, RedirectTracker};
pub use transport::{HttpTransport, RangeResponse, RequestSpec};
pub use validators::{ContentRange, ResourceValidators};