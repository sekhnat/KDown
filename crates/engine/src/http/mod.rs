//! HTTP transport layer (§11, §32): request execution, redirect policy,
//! probe, validators.

pub mod connect;
pub mod execution;
pub mod probe;
pub mod range;
pub mod redirect;
pub mod scripted;
pub mod transport;
pub mod validators;

pub use connect::{ConnectError, ConnectionLimits};
pub use execution::{
    BodyEvent, FullResponsePolicy, HttpBody, HttpBodySource, HttpExecution, HttpExecutor,
    HttpFailure, ProbeOutcome, ProbeRequest, RangeIntent, TransferIntent, TransferRequest,
    TransferResponse,
};
pub use probe::{filename_from_disposition, ProbeMetadata};
pub use range::{RangeRejection, RejectionKind, ValidatedRange};
pub use redirect::{RedirectAction, RedirectDecision, RedirectPolicy, RedirectTracker};
pub use scripted::{
    CallKind, CallRecord, ProbeStep, ScriptedBodyEvent, ScriptedHttp, TransferOk, TransferStep,
};
pub use transport::{HttpTransport, RequestSpec};
pub use validators::{ContentRange, ResourceValidators};
