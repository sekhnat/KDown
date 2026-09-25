//! Deterministic scripted HTTP adapter (§32, design decision 6).
//!
//! A small, dependency-free [`ScriptedHttp`] implementation of the
//! [`HttpExecutor`] port. It is intended for engine and downstream
//! orchestration tests and performs no work and opens no sockets:
//! each scripted call matches (and consumes) the next script step and
//! returns its scripted outcome.
//!
//! A script is a sequence of phases:
//!
//! - an ordered step ([`ScriptedHttp::expect_probe`] /
//!   [`ScriptedHttp::expect_transfer`]) consumes exactly one call in FIFO
//!   order;
//! - a bounded unordered phase ([`ScriptedHttp::expect_unordered_ranges`])
//!   consumes one call per ranged step, matched by requested range, so
//!   concurrent workers do not make arrival order an assertion;
//! - a gate ([`ScriptedHttp::gate`]) parks arriving calls until the test
//!   opens it with [`ScriptedHttp::open_gate`].
//!
//! Mismatches return an explicit protocol-style
//! [`DownloadError::Protocol`] failure whose message contains both the
//! expected step summary and the observed call summary. The adapter keeps
//! a log of consumed calls ([`ScriptedHttp::request_log`]) and asserts
//! full consumption ([`ScriptedHttp::assert_all_consumed`]).
//!
//! # Example
//!
//! ```
//! use kdown_engine::control::CancellationToken;
//! use kdown_engine::http::scripted::{ScriptedHttp, ProbeStep, TransferStep, TransferOk};
//! use kdown_engine::http::{
//!     HttpExecution, ProbeMetadata, ProbeRequest, RequestSpec, TransferIntent, TransferRequest,
//! };
//!
//! tokio::runtime::Runtime::new().unwrap().block_on(async {
//!     let scripted = ScriptedHttp::new()
//!         .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
//!             total_size: Some(5),
//!             ..ProbeMetadata::default()
//!         }))
//!         .expect_transfer(
//!             TransferStep::new()
//!                 .ok(TransferOk::new().total(5).chunk(b"hello".as_slice())),
//!         );
//!     let execution = HttpExecution::from_adapter(scripted.clone());
//!     let cancel = CancellationToken::new();
//!
//!     let outcome = execution
//!         .probe(
//!             ProbeRequest {
//!                 spec: RequestSpec {
//!                     url: "https://x/f.bin".into(),
//!                     ..RequestSpec::default()
//!                 },
//!                 segmentation_threshold: 1024,
//!                 verify_range_support: true,
//!             },
//!             &cancel,
//!         )
//!         .await
//!         .expect("probe");
//!     assert_eq!(outcome.metadata.total_size, Some(5));
//!
//!     let response = execution
//!         .transfer(
//!             TransferRequest {
//!                 spec: RequestSpec {
//!                     url: "https://x/f.bin".into(),
//!                     ..RequestSpec::default()
//!                 },
//!                 intent: TransferIntent::Full,
//!             },
//!             &cancel,
//!         )
//!         .await
//!         .expect("transfer");
//!     assert_eq!(response.total_size, Some(5));
//!
//!     scripted.assert_all_consumed();
//!     assert_eq!(scripted.request_log().len(), 2);
//! });
//! ```

use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::control::CancellationToken;
use crate::error::DownloadError;
use crate::http::execution::{
    FullResponsePolicy, HttpBody, HttpBodySource, HttpExecutor, HttpFailure, ProbeOutcome,
    ProbeRequest, TransferIntent, TransferRequest, TransferResponse,
};
use crate::http::probe::ProbeMetadata;
use crate::http::validators::ResourceValidators;

/// How long a scripted body read may idle before the wrapper's shared
/// read-idle classification applies. Scripted events that must surface
/// the timeout (`ScriptedBodyEvent::IdleTimeout`) do so immediately, so
/// this only bounds pathological waits in a buggy test.
const DEFAULT_SCRIPTED_IDLE: Duration = Duration::from_secs(30);

/// Which semantic call a step expects or a record observed (§32).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CallKind {
    #[default]
    Probe,
    Transfer,
}

impl std::fmt::Display for CallKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallKind::Probe => f.write_str("probe"),
            CallKind::Transfer => f.write_str("transfer"),
        }
    }
}

/// Observed summary of one semantic call, used for matching and for
/// actionable mismatch messages.
#[derive(Debug, Clone, Default)]
struct ObservedCall {
    kind: CallKind,
    url: String,
    headers: Vec<(String, String)>,
    /// Requested range for ranged transfer intents.
    range: Option<(u64, u64)>,
    /// Probe policy: whether range support must be verified.
    verify_range_support: Option<bool>,
    /// Probe policy: segmentation threshold.
    segmentation_threshold: Option<u64>,
    /// Transfer intent kind: `Some(true)` full, `Some(false)` ranged.
    intent_full: Option<bool>,
    /// Ranged intent: established total.
    established_total: Option<Option<u64>>,
    /// Ranged intent: expected validators sent with the request.
    expected_validators: Option<ResourceValidators>,
    /// Ranged intent: full-response classification policy.
    full_response: Option<FullResponsePolicy>,
}

impl ObservedCall {
    fn from_probe(request: &ProbeRequest) -> Self {
        Self {
            kind: CallKind::Probe,
            url: request.spec.url.clone(),
            headers: request.spec.headers.clone(),
            verify_range_support: Some(request.verify_range_support),
            segmentation_threshold: Some(request.segmentation_threshold),
            ..Self::default()
        }
    }

    fn from_transfer(request: &TransferRequest) -> Self {
        let (intent_full, established_total, expected_validators, full_response) =
            match &request.intent {
                TransferIntent::Full => (true, None, None, None),
                TransferIntent::Range(ri) => (
                    false,
                    Some(ri.established_total),
                    ri.expected_validators.clone(),
                    Some(ri.full_response),
                ),
            };
        Self {
            kind: CallKind::Transfer,
            url: request.spec.url.clone(),
            headers: request.spec.headers.clone(),
            range: request.intent.range(),
            intent_full: Some(intent_full),
            established_total,
            expected_validators,
            full_response,
            ..Self::default()
        }
    }

    /// One-line human summary for mismatch messages.
    fn summary(&self) -> String {
        let mut s = format!("{} url={:?}", self.kind, self.url);
        if let Some(range) = self.range {
            s.push_str(&format!(" range={range:?}"));
        }
        if let Some(v) = self.verify_range_support {
            s.push_str(&format!(" verify_range_support={v}"));
        }
        if let Some(t) = self.segmentation_threshold {
            s.push_str(&format!(" segmentation_threshold={t}"));
        }
        if let Some(full) = self.intent_full {
            s.push_str(if full {
                " intent=full"
            } else {
                " intent=range"
            });
        }
        if let Some(t) = self.established_total {
            s.push_str(&format!(" established_total={t:?}"));
        }
        if let Some(v) = &self.expected_validators {
            s.push_str(&format!(" validators={v:?}"));
        }
        if let Some(p) = self.full_response {
            s.push_str(&format!(" full_response={p:?}"));
        }
        if !self.headers.is_empty() {
            s.push_str(&format!(
                " headers={:?}",
                crate::redact::RedactedHeaders(&self.headers)
            ));
        }
        s
    }
}

/// One consumed call in the request log (§32).
/// One consumed call in the request log (§32).
#[derive(Clone)]
pub struct CallRecord {
    pub kind: CallKind,
    pub url: String,
    pub range: Option<(u64, u64)>,
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for CallRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Call records mirror the request as sent, including credential
        // headers attached by the caller or a credential provider; debug
        // output keeps header names but never formats their values.
        f.debug_struct("CallRecord")
            .field("kind", &self.kind)
            .field("url", &self.url)
            .field("range", &self.range)
            .field("headers", &crate::redact::RedactedHeaders(&self.headers))
            .finish()
    }
}

/// Scripted body event (§32): stored chunks are yielded as `Bytes` in
/// order; a fault surfaces the stored structured error; `IdleTimeout`
/// returns the same semantic error as the production body wrapper;
/// `WaitForCancellation` resolves only once the consumer's token cancels.
#[derive(Debug)]
pub enum ScriptedBodyEvent {
    /// One payload chunk, stored and yielded as `Bytes` (zero-copy).
    Chunk(bytes::Bytes),
    /// Classified body fault delivered at this position.
    Fault(DownloadError),
    /// Immediate read-idle timeout (same semantic error as production).
    IdleTimeout,
    /// Park the body read until the consumer's token is cancelled.
    WaitForCancellation,
    /// Explicit clean end of body. (An implicit end follows the last
    /// event when absent.)
    End,
}

/// Scripted successful transfer reply: accepted metadata plus the body
/// event script (§32).
#[derive(Debug)]
pub struct TransferOk {
    start: u64,
    end: u64,
    total_size: Option<u64>,
    validators: ResourceValidators,
    body: Vec<ScriptedBodyEvent>,
}

impl Default for TransferOk {
    fn default() -> Self {
        Self {
            start: 0,
            // Production full-transfer semantics: unknown end means
            // "until EOF" (§11.2).
            end: u64::MAX,
            total_size: None,
            validators: ResourceValidators::default(),
            body: Vec::new(),
        }
    }
}

impl TransferOk {
    /// A successful reply with production-like defaults (start 0, open
    /// end, unknown total, empty validators).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Accepted inclusive range override.
    #[must_use]
    pub fn range(mut self, start: u64, end: u64) -> Self {
        self.start = start;
        self.end = end;
        self
    }

    /// Authoritative total; also narrows an open end to `total - 1`
    /// (matching the production full-transfer rule, §11.2).
    #[must_use]
    pub fn total(mut self, total: u64) -> Self {
        self.total_size = Some(total);
        if self.end == u64::MAX && total > 0 {
            self.end = total - 1;
        }
        self
    }

    /// Response validators for generation tracking (§5.2).
    #[must_use]
    pub fn validators(mut self, validators: ResourceValidators) -> Self {
        self.validators = validators;
        self
    }

    /// Append a payload chunk (stored as `Bytes`).
    #[must_use]
    pub fn chunk(mut self, data: impl Into<bytes::Bytes>) -> Self {
        self.body.push(ScriptedBodyEvent::Chunk(data.into()));
        self
    }

    /// Append a classified body fault at this position.
    #[must_use]
    pub fn fault(mut self, error: DownloadError) -> Self {
        self.body.push(ScriptedBodyEvent::Fault(error));
        self
    }

    /// Append an immediate read-idle timeout.
    #[must_use]
    pub fn idle_timeout(mut self) -> Self {
        self.body.push(ScriptedBodyEvent::IdleTimeout);
        self
    }

    /// Append a wait-for-cancellation park.
    #[must_use]
    pub fn wait_for_cancellation(mut self) -> Self {
        self.body.push(ScriptedBodyEvent::WaitForCancellation);
        self
    }

    /// Append an explicit end of body.
    #[must_use]
    pub fn end(mut self) -> Self {
        self.body.push(ScriptedBodyEvent::End);
        self
    }
}

/// Outcome a step returns (moved out at consumption; `DownloadError` is
/// not `Clone`, so replies are one-shot by construction).
enum Reply {
    Probe(Box<ProbeOutcome>),
    Transfer(Box<TransferOk>),
    Fail(Box<HttpFailure>),
}

impl std::fmt::Debug for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reply::Probe(o) => f.debug_tuple("ok(probe)").field(o).finish(),
            Reply::Transfer(t) => f.debug_tuple("ok(transfer)").field(t).finish(),
            Reply::Fail(e) => f.debug_tuple("fail").field(&e.error).finish(),
        }
    }
}

/// One expected probe call plus its scripted outcome (§32).
#[derive(Debug)]
pub struct ProbeStep {
    // Matching clauses (None = wildcard).
    url: Option<String>,
    headers: Vec<(String, String)>,
    verify_range_support: Option<bool>,
    segmentation_threshold: Option<u64>,
    // Outcome.
    reply: Option<Reply>,
}

impl Default for ProbeStep {
    fn default() -> Self {
        Self::new()
    }
}

impl ProbeStep {
    /// A probe step matching any probe request.
    #[must_use]
    pub fn new() -> Self {
        Self {
            url: None,
            headers: Vec::new(),
            verify_range_support: None,
            segmentation_threshold: None,
            reply: None,
        }
    }

    /// Require this exact request URL.
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Require this header to be present with this value (relevant-header
    /// matching: other headers are ignored; names compare
    /// case-insensitively).
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Require the probe policy's `verify_range_support` flag.
    #[must_use]
    pub fn verify_range_support(mut self, verify: bool) -> Self {
        self.verify_range_support = Some(verify);
        self
    }

    /// Require the probe policy's segmentation threshold.
    #[must_use]
    pub fn segmentation_threshold(mut self, threshold: u64) -> Self {
        self.segmentation_threshold = Some(threshold);
        self
    }

    /// Return this successful probe outcome.
    #[must_use]
    pub fn ok(mut self, outcome: ProbeOutcome) -> Self {
        self.reply = Some(Reply::Probe(Box::new(outcome)));
        self
    }

    /// Return a successful probe outcome with no notices.
    #[must_use]
    pub fn ok_meta(self, metadata: ProbeMetadata) -> Self {
        self.ok(ProbeOutcome {
            metadata,
            notices: Vec::new(),
        })
    }

    /// Return this structured failure.
    #[must_use]
    pub fn fail(mut self, failure: HttpFailure) -> Self {
        self.reply = Some(Reply::Fail(Box::new(failure)));
        self
    }

    /// Return a structured failure wrapping this error (no retry timing,
    /// no challenge).
    #[must_use]
    pub fn fail_error(self, error: DownloadError) -> Self {
        self.fail(HttpFailure::from_error(error))
    }

    fn mismatches(&self, call: &ObservedCall) -> Vec<String> {
        let mut why = Vec::new();
        if call.kind != CallKind::Probe {
            why.push(format!("expected a probe call, observed {}", call.kind));
        }
        if let Some(url) = &self.url {
            if *url != call.url {
                why.push(format!("expected url {url:?}, observed {:?}", call.url));
            }
        }
        why.extend(header_mismatches(&self.headers, &call.headers));
        if let Some(v) = self.verify_range_support {
            if call.verify_range_support != Some(v) {
                why.push(format!(
                    "expected verify_range_support={v}, observed {:?}",
                    call.verify_range_support
                ));
            }
        }
        if let Some(t) = self.segmentation_threshold {
            if call.segmentation_threshold != Some(t) {
                why.push(format!(
                    "expected segmentation_threshold={t}, observed {:?}",
                    call.segmentation_threshold
                ));
            }
        }
        why
    }

    fn summary(&self) -> String {
        let mut s = String::from("probe");
        if let Some(url) = &self.url {
            s.push_str(&format!(" url={url:?}"));
        }
        if let Some(v) = self.verify_range_support {
            s.push_str(&format!(" verify_range_support={v}"));
        }
        if let Some(t) = self.segmentation_threshold {
            s.push_str(&format!(" segmentation_threshold={t}"));
        }
        if !self.headers.is_empty() {
            s.push_str(&format!(
                " headers={:?}",
                crate::redact::RedactedHeaders(&self.headers)
            ));
        }
        s
    }
}

/// One expected transfer call plus its scripted outcome (§32).
#[derive(Debug)]
pub struct TransferStep {
    url: Option<String>,
    headers: Vec<(String, String)>,
    intent_full: Option<bool>,
    range: Option<(u64, u64)>,
    /// Tri-state: `None` = don't care; `Some(None)` = must be absent;
    /// `Some(Some(t))` = must equal `t`.
    established_total: Option<Option<u64>>,
    expected_validators: Option<ResourceValidators>,
    full_response: Option<FullResponsePolicy>,
    reply: Option<Reply>,
}

impl Default for TransferStep {
    fn default() -> Self {
        Self::new()
    }
}

impl TransferStep {
    /// A transfer step matching any transfer request.
    #[must_use]
    pub fn new() -> Self {
        Self {
            url: None,
            headers: Vec::new(),
            intent_full: None,
            range: None,
            established_total: None,
            expected_validators: None,
            full_response: None,
            reply: None,
        }
    }

    /// Require this exact request URL.
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Require this relevant header (see [`ProbeStep::header`]).
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Require a full (fresh sequential) intent.
    #[must_use]
    pub fn intent_full(mut self) -> Self {
        self.intent_full = Some(true);
        self
    }

    /// Require a ranged intent (optionally with the exact range).
    #[must_use]
    pub fn intent_range(mut self, range: impl Into<Option<(u64, u64)>>) -> Self {
        self.intent_full = Some(false);
        self.range = range.into();
        self
    }

    /// Require this exact requested range (implies a ranged intent).
    #[must_use]
    pub fn range(mut self, range: (u64, u64)) -> Self {
        self.intent_full = Some(false);
        self.range = Some(range);
        self
    }

    /// Require this established total on a ranged intent.
    #[must_use]
    pub fn established_total(mut self, total: u64) -> Self {
        self.established_total = Some(Some(total));
        self
    }

    /// Require the ranged intent to carry no established total.
    #[must_use]
    pub fn no_established_total(mut self) -> Self {
        self.established_total = Some(None);
        self
    }

    /// Require these expected validators on a ranged intent.
    #[must_use]
    pub fn validators(mut self, validators: &ResourceValidators) -> Self {
        self.expected_validators = Some(validators.clone());
        self
    }

    /// Require this full-response classification policy.
    #[must_use]
    pub fn full_response_policy(mut self, policy: FullResponsePolicy) -> Self {
        self.full_response = Some(policy);
        self
    }

    /// Return this successful transfer reply (metadata + body script).
    #[must_use]
    pub fn ok(mut self, reply: TransferOk) -> Self {
        self.reply = Some(Reply::Transfer(Box::new(reply)));
        self
    }

    /// Return this structured failure.
    #[must_use]
    pub fn fail(mut self, failure: HttpFailure) -> Self {
        self.reply = Some(Reply::Fail(Box::new(failure)));
        self
    }

    /// Return a structured failure wrapping this error.
    #[must_use]
    pub fn fail_error(self, error: DownloadError) -> Self {
        self.fail(HttpFailure::from_error(error))
    }

    /// Return a retryable structured failure with `Retry-After` timing.
    #[must_use]
    pub fn fail_retry_after(self, error: DownloadError, retry_after: Duration) -> Self {
        self.fail(HttpFailure {
            error,
            retry_after: Some(retry_after),
            challenge: None,
        })
    }

    fn mismatches(&self, call: &ObservedCall) -> Vec<String> {
        let mut why = Vec::new();
        if call.kind != CallKind::Transfer {
            why.push(format!("expected a transfer call, observed {}", call.kind));
        }
        if let Some(url) = &self.url {
            if *url != call.url {
                why.push(format!("expected url {url:?}, observed {:?}", call.url));
            }
        }
        why.extend(header_mismatches(&self.headers, &call.headers));
        if let Some(full) = self.intent_full {
            if call.intent_full != Some(full) {
                why.push(format!(
                    "expected {} intent, observed {:?}",
                    if full { "full" } else { "ranged" },
                    call.intent_full
                ));
            }
        }
        if let Some(range) = self.range {
            if call.range != Some(range) {
                why.push(format!(
                    "expected requested range {range:?}, observed {:?}",
                    call.range
                ));
            }
        }
        if let Some(total) = &self.established_total {
            if call.established_total != Some(*total) {
                why.push(format!(
                    "expected established_total={total:?}, observed {:?}",
                    call.established_total
                ));
            }
        }
        if let Some(v) = &self.expected_validators {
            if call.expected_validators.as_ref() != Some(v) {
                why.push(format!(
                    "expected validators {v:?}, observed {:?}",
                    call.expected_validators
                ));
            }
        }
        if let Some(p) = self.full_response {
            if call.full_response != Some(p) {
                why.push(format!(
                    "expected full_response policy {p:?}, observed {:?}",
                    call.full_response
                ));
            }
        }
        why
    }

    fn summary(&self) -> String {
        let mut s = String::from("transfer");
        if let Some(url) = &self.url {
            s.push_str(&format!(" url={url:?}"));
        }
        if let Some(full) = self.intent_full {
            s.push_str(if full {
                " intent=full"
            } else {
                " intent=range"
            });
        }
        if let Some(range) = self.range {
            s.push_str(&format!(" range={range:?}"));
        }
        if let Some(total) = &self.established_total {
            s.push_str(&format!(" established_total={total:?}"));
        }
        if let Some(v) = &self.expected_validators {
            s.push_str(&format!(" validators={v:?}"));
        }
        if let Some(p) = self.full_response {
            s.push_str(&format!(" full_response={p:?}"));
        }
        if !self.headers.is_empty() {
            s.push_str(&format!(
                " headers={:?}",
                crate::redact::RedactedHeaders(&self.headers)
            ));
        }
        s
    }
}

/// Case-insensitive relevant-header mismatch reasons.
fn header_mismatches(expected: &[(String, String)], actual: &[(String, String)]) -> Vec<String> {
    let mut why = Vec::new();
    for (name, value) in expected {
        let found = actual
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case(name) && v == value);
        if !found {
            // Expectation values may be credentials (test fixtures attach
            // bearer tokens here); a mismatch names the header but never
            // prints the value that was expected.
            why.push(format!(
                "expected header {name}: {} to be present",
                crate::redact::REDACTED_VALUE
            ));
        }
    }
    why
}

enum Phase {
    /// FIFO step with an optional label for [`ScriptedHttp::wait_for_phase`].
    Ordered {
        label: Option<String>,
        step: Box<Step>,
    },
    /// Bounded unordered phase: one call per step, matched by requested
    /// range; advances the script when every step is consumed.
    UnorderedRanges {
        label: String,
        steps: Vec<Option<Step>>,
    },
    /// Barrier: arriving calls park until [`ScriptedHttp::open_gate`].
    Gate { label: String },
    /// Consumed placeholder (the step was moved out).
    Taken,
}

enum Step {
    Probe(ProbeStep),
    Transfer(TransferStep),
}

impl Step {
    fn summary(&self) -> String {
        match self {
            Step::Probe(p) => p.summary(),
            Step::Transfer(t) => t.summary(),
        }
    }

    fn mismatches(&self, call: &ObservedCall) -> Vec<String> {
        match self {
            Step::Probe(p) => p.mismatches(call),
            Step::Transfer(t) => t.mismatches(call),
        }
    }

    fn take_reply(&mut self) -> Option<Reply> {
        match self {
            Step::Probe(p) => p.reply.take(),
            Step::Transfer(t) => t.reply.take(),
        }
    }
}

struct ScriptState {
    phases: Vec<Phase>,
    next: usize,
    consumed_labels: BTreeSet<String>,
    log: Vec<CallRecord>,
}

impl ScriptState {
    /// Unconsumed step summaries for [`ScriptedHttp::assert_all_consumed`].
    fn unconsumed(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (i, phase) in self.phases.iter().enumerate() {
            if i < self.next {
                continue;
            }
            match phase {
                Phase::Ordered { label, step } => out.push(format!(
                    "step {}: {}{}",
                    i + 1,
                    step.summary(),
                    label
                        .as_ref()
                        .map(|l| format!(" (label {l:?})"))
                        .unwrap_or_default()
                )),
                Phase::UnorderedRanges { label, steps } => {
                    let left: Vec<String> = steps.iter().flatten().map(|s| s.summary()).collect();
                    out.push(format!(
                        "unordered phase {label:?} with {} unconsumed steps: {}",
                        left.len(),
                        left.join("; ")
                    ));
                }
                Phase::Gate { label } => {
                    out.push(format!("gate {label:?} never opened"));
                }
                Phase::Taken => {}
            }
        }
        out
    }
}

enum Decision {
    /// Consume matched and return this reply.
    Matched(Reply),
    /// Park until the named gate opens.
    Parked(String),
    /// Explicit protocol-style failure with expected vs observed.
    Mismatched(String),
}

/// Deterministic scripted [`HttpExecutor`] (§32).
///
/// Cloneable handle sharing one script; see the [module docs](self) for
/// usage. Build with `ScriptedHttp::new()` plus the `expect_*` methods.
pub struct ScriptedHttp {
    inner: Arc<Inner>,
    read_idle_timeout: Duration,
}

struct Inner {
    state: Mutex<ScriptState>,
    /// Bumped on every consumption and gate opening; waiters re-check
    /// their condition under the state lock on every wake (the value
    /// latches, so no wakeup is lost).
    changed_tx: tokio::sync::watch::Sender<u64>,
}

impl std::fmt::Debug for ScriptedHttp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedHttp").finish_non_exhaustive()
    }
}

impl Clone for ScriptedHttp {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            read_idle_timeout: self.read_idle_timeout,
        }
    }
}

impl Default for ScriptedHttp {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptedHttp {
    /// An empty script: the first call mismatches with "script exhausted".
    #[must_use]
    pub fn new() -> Self {
        let (changed_tx, _) = tokio::sync::watch::channel(0u64);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(ScriptState {
                    phases: Vec::new(),
                    next: 0,
                    consumed_labels: BTreeSet::new(),
                    log: Vec::new(),
                }),
                changed_tx,
            }),
            read_idle_timeout: DEFAULT_SCRIPTED_IDLE,
        }
    }

    /// Override the wrapper read-idle timeout used for scripted bodies
    /// (scripted timeouts are immediate; this only bounds pathological
    /// waits in a buggy test).
    #[must_use]
    pub fn with_read_idle_timeout(mut self, timeout: Duration) -> Self {
        self.read_idle_timeout = timeout;
        self
    }

    fn push(self, phase: Phase) -> Self {
        self.inner
            .state
            .lock()
            .expect("script mutex")
            .phases
            .push(phase);
        self
    }

    /// Append an ordered probe step (FIFO).
    #[must_use]
    pub fn expect_probe(self, step: ProbeStep) -> Self {
        self.push(Phase::Ordered {
            label: None,
            step: Box::new(Step::Probe(step)),
        })
    }

    /// Append an ordered, labeled probe step for
    /// [`ScriptedHttp::wait_for_phase`].
    #[must_use]
    pub fn expect_labeled_probe(self, label: &str, step: ProbeStep) -> Self {
        self.push(Phase::Ordered {
            label: Some(label.to_string()),
            step: Box::new(Step::Probe(step)),
        })
    }

    /// Append an ordered transfer step (FIFO).
    #[must_use]
    pub fn expect_transfer(self, step: TransferStep) -> Self {
        self.push(Phase::Ordered {
            label: None,
            step: Box::new(Step::Transfer(step)),
        })
    }

    /// Append an ordered, labeled transfer step for
    /// [`ScriptedHttp::wait_for_phase`].
    #[must_use]
    pub fn expect_labeled_transfer(self, label: &str, step: TransferStep) -> Self {
        self.push(Phase::Ordered {
            label: Some(label.to_string()),
            step: Box::new(Step::Transfer(step)),
        })
    }

    /// Append a bounded unordered phase keyed by requested range: each
    /// step must be ranged; a call consumes the step with its range.
    /// Arrival order does not matter; full consumption advances the
    /// script. Duplicate ranges are allowed (retry of the same range).
    ///
    /// # Panics
    /// When any step lacks a range — an unordered phase is range-keyed
    /// by construction.
    #[must_use]
    pub fn expect_unordered_ranges(self, label: &str, steps: Vec<TransferStep>) -> Self {
        for (i, step) in steps.iter().enumerate() {
            assert!(
                step.range.is_some(),
                "unordered phase {label:?} step {i} needs .range(..): \
                 the phase matches calls by requested range"
            );
        }
        self.push(Phase::UnorderedRanges {
            label: label.to_string(),
            steps: steps.into_iter().map(|s| Some(Step::Transfer(s))).collect(),
        })
    }

    /// Append a named barrier: arriving calls park until
    /// [`ScriptedHttp::open_gate`] releases them into the following
    /// phases. Prefer an unordered range-keyed phase after a gate when
    /// releasing concurrent workers.
    #[must_use]
    pub fn gate(self, label: &str) -> Self {
        self.push(Phase::Gate {
            label: label.to_string(),
        })
    }

    /// Open a gate by label (idempotent); parked calls resume matching
    /// the phases after the gate. Opening an unknown or already-passed
    /// label is a no-op for matching.
    pub fn open_gate(&self, label: &str) {
        {
            let mut st = self.inner.state.lock().expect("script mutex");
            st.consumed_labels.insert(label.to_string());
        }
        self.bump();
    }

    fn bump(&self) {
        self.inner.changed_tx.send_modify(|v| *v += 1);
    }

    /// Wait until the labeled step/phase was consumed (ordered steps get
    /// labels via `expect_labeled_*`; gates are "consumed" when opened).
    /// Wrap in `tokio::time::timeout` in tests that must not hang on a
    /// scripting bug.
    pub async fn wait_for_phase(&self, label: &str) {
        let mut rx = self.inner.changed_tx.subscribe();
        loop {
            rx.borrow_and_update();
            {
                let st = self.inner.state.lock().expect("script mutex");
                if st.consumed_labels.contains(label) {
                    return;
                }
            }
            if rx.changed().await.is_err() {
                return; // sender dropped with the adapter: stop waiting
            }
        }
    }

    /// Log of consumed calls, in consumption order.
    #[must_use]
    pub fn request_log(&self) -> Vec<CallRecord> {
        self.inner.state.lock().expect("script mutex").log.clone()
    }

    /// Assert every scripted step was consumed; otherwise panic with the
    /// remaining step summaries (§32 all-steps-consumed assertion).
    ///
    /// # Panics
    /// When any step was never matched by a call.
    pub fn assert_all_consumed(&self) {
        let st = self.inner.state.lock().expect("script mutex");
        let remaining = st.unconsumed();
        assert!(
            remaining.is_empty(),
            "scripted HTTP script not fully consumed ({} of {} phases consumed); \
             remaining: {}",
            st.next,
            st.phases.len(),
            remaining.join(" | ")
        );
    }

    /// Match one observed call against the current phase, consuming it on
    /// success. The lock is never held across await points: gate parking
    /// happens on [`Decision::Parked`].
    fn decide(&self, call: &ObservedCall) -> Decision {
        let mut st = self.inner.state.lock().expect("script mutex");
        loop {
            if st.next >= st.phases.len() {
                return Decision::Mismatched(format!(
                    "scripted HTTP script exhausted ({} phases consumed); \
                     unexpected {} call: {}",
                    st.phases.len(),
                    call.kind,
                    call.summary()
                ));
            }
            // Opened gates are transparent: pass through to the next phase.
            let gate_label = match &st.phases[st.next] {
                Phase::Gate { label } => Some(label.clone()),
                _ => None,
            };
            let Some(gate_label) = gate_label else {
                break; // ordered or unordered phase: fall through
            };
            if st.consumed_labels.contains(&gate_label) {
                let idx = st.next;
                st.phases[idx] = Phase::Taken;
                st.next += 1;
                continue;
            }
            return Decision::Parked(gate_label);
        }

        let is_unordered = matches!(st.phases[st.next], Phase::UnorderedRanges { .. });
        if is_unordered {
            // Work on an owned phase so no borrow of `st` is held across
            // the bookkeeping below.
            let idx = st.next;
            let mut phase = std::mem::replace(&mut st.phases[idx], Phase::Taken);
            let Phase::UnorderedRanges { label, steps } = &mut phase else {
                unreachable!("checked above")
            };
            let match_idx = steps
                .iter()
                .position(|s| s.as_ref().is_some_and(|s| s.mismatches(call).is_empty()));
            let Some(i) = match_idx else {
                let label = label.clone();
                let remaining: Vec<String> = steps.iter().flatten().map(|s| s.summary()).collect();
                st.phases[idx] = phase; // put the phase back, unchanged
                return Decision::Mismatched(format!(
                    "scripted HTTP unordered phase {label:?}: no step matches {}; \
                     remaining steps: {}",
                    call.summary(),
                    remaining.join("; ")
                ));
            };
            let mut step = steps[i].take().expect("checked just above");
            let reply = step.take_reply().expect("scripted step has a reply");
            let phase_done = steps.iter().all(Option::is_none);
            let label = label.clone();
            st.phases[idx] = if phase_done { Phase::Taken } else { phase };
            if phase_done {
                st.next += 1;
            }
            st.consumed_labels.insert(label);
            st.log.push(CallRecord {
                kind: call.kind,
                url: call.url.clone(),
                range: call.range,
                headers: call.headers.clone(),
            });
            return Decision::Matched(reply);
        }

        // Ordered FIFO step.
        let (why, step_summary) = {
            let Phase::Ordered { step, .. } = &st.phases[st.next] else {
                unreachable!("checked above")
            };
            (step.mismatches(call), step.summary())
        };
        if !why.is_empty() {
            return Decision::Mismatched(format!(
                "scripted HTTP mismatch at step {} of {}: expected {}; \
                 observed: {}; differences: {}",
                st.next + 1,
                st.phases.len(),
                step_summary,
                call.summary(),
                why.join("; ")
            ));
        }
        let idx = st.next;
        let taken = std::mem::replace(&mut st.phases[idx], Phase::Taken);
        let Phase::Ordered { label, mut step } = taken else {
            unreachable!("checked above")
        };
        let reply = step.take_reply().expect("scripted step has a reply");
        st.next += 1;
        if let Some(l) = label {
            st.consumed_labels.insert(l);
        }
        st.log.push(CallRecord {
            kind: call.kind,
            url: call.url.clone(),
            range: call.range,
            headers: call.headers.clone(),
        });
        Decision::Matched(reply)
    }

    /// Drive the phase loop: match, park at gates, or fail with an
    /// actionable protocol mismatch.
    async fn drive(
        &self,
        call: &ObservedCall,
        cancel: &CancellationToken,
    ) -> Result<Reply, HttpFailure> {
        loop {
            let decision = self.decide(call);
            match decision {
                Decision::Matched(reply) => {
                    self.bump();
                    return Ok(reply);
                }
                Decision::Mismatched(msg) => {
                    return Err(HttpFailure::from_error(DownloadError::Protocol(msg)));
                }
                Decision::Parked(gate) => {
                    let mut rx = self.inner.changed_tx.subscribe();
                    loop {
                        // Baseline before checking: any open after this
                        // point latches in the watch and wakes us.
                        rx.borrow_and_update();
                        {
                            let st = self.inner.state.lock().expect("script mutex");
                            if st.consumed_labels.contains(&gate) {
                                break; // gate open: re-match from the top
                            }
                        }
                        tokio::select! {
                            changed = rx.changed() => {
                                if changed.is_err() {
                                    return Err(HttpFailure::from_error(
                                        DownloadError::Cancelled,
                                    ));
                                }
                            }
                            _ = cancel.cancelled_or_paused() => {
                                if cancel.is_cancelled() {
                                    return Err(HttpFailure::from_error(
                                        DownloadError::Cancelled,
                                    ));
                                }
                                // Paused: keep waiting for the gate; pause
                                // governs body reads, not script matching.
                            }
                        }
                    }
                }
            }
        }
    }
}

fn protocol_bug(call: &ObservedCall, reply: &Reply) -> HttpFailure {
    HttpFailure::from_error(DownloadError::Protocol(format!(
        "scripted HTTP returned {reply:?} to a {} call: {}",
        call.kind,
        call.summary()
    )))
}

impl HttpExecutor for ScriptedHttp {
    fn probe<'a>(
        &'a self,
        request: ProbeRequest,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ProbeOutcome, HttpFailure>> + Send + 'a>> {
        Box::pin(async move {
            let call = ObservedCall::from_probe(&request);
            match self.drive(&call, cancel).await {
                Ok(Reply::Probe(outcome)) => Ok(*outcome),
                Ok(Reply::Fail(failure)) => Err(*failure),
                Ok(other) => Err(protocol_bug(&call, &other)),
                Err(failure) => Err(failure),
            }
        })
    }

    fn transfer<'a>(
        &'a self,
        request: TransferRequest,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<TransferResponse, HttpFailure>> + Send + 'a>> {
        Box::pin(async move {
            let call = ObservedCall::from_transfer(&request);
            match self.drive(&call, cancel).await {
                Ok(Reply::Transfer(ok)) => {
                    let body = HttpBody::new(
                        ScriptedBodySource {
                            events: ok.body.into_iter().collect(),
                            cancel: cancel.clone(),
                            waker_sleep: None,
                        },
                        self.read_idle_timeout,
                    );
                    Ok(TransferResponse {
                        start: ok.start,
                        end: ok.end,
                        total_size: ok.total_size,
                        validators: ok.validators,
                        body,
                    })
                }
                Ok(Reply::Fail(failure)) => Err(*failure),
                Ok(other) => Err(protocol_bug(&call, &other)),
                Err(failure) => Err(failure),
            }
        })
    }
}

/// Scripted body source (§32): yields stored events in order without
/// sockets or delays. `IdleTimeout` immediately classifies as the shared
/// read-idle connection error; `WaitForCancellation` pends (with a 1 ms
/// internal wake so the consumer's cancellation is observed promptly)
/// until the caller's token cancels, then resolves.
struct ScriptedBodySource {
    events: VecDeque<ScriptedBodyEvent>,
    cancel: CancellationToken,
    /// Reused short timer for the `WaitForCancellation` wake loop.
    waker_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl HttpBodySource for ScriptedBodySource {
    fn poll_chunk(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>> {
        let this = self.get_mut();
        match this.events.front_mut() {
            None => std::task::Poll::Ready(Ok(None)), // implicit clean end
            Some(ScriptedBodyEvent::Chunk(data)) => {
                let data = std::mem::take(data);
                this.events.pop_front();
                std::task::Poll::Ready(Ok(Some(data)))
            }
            Some(ScriptedBodyEvent::Fault(_)) => {
                let Some(ScriptedBodyEvent::Fault(err)) = this.events.pop_front() else {
                    unreachable!("front was Fault");
                };
                std::task::Poll::Ready(Err(err))
            }
            Some(ScriptedBodyEvent::IdleTimeout) => {
                this.events.pop_front();
                // Same semantic error as the production body wrapper
                // (§32: immediate, no real timeout wait).
                std::task::Poll::Ready(Err(DownloadError::Connection("read idle timeout".into())))
            }
            Some(ScriptedBodyEvent::End) => {
                this.events.pop_front();
                std::task::Poll::Ready(Ok(None))
            }
            Some(ScriptedBodyEvent::WaitForCancellation) => {
                if this.cancel.is_cancelled() {
                    this.events.pop_front();
                    return std::task::Poll::Ready(Err(DownloadError::Cancelled));
                }
                // Short self-wake so cancellation is observed promptly
                // without wall-clock dependencies on real timeouts.
                let deadline = tokio::time::Instant::now() + Duration::from_millis(1);
                let sleep = this
                    .waker_sleep
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
                sleep.as_mut().reset(deadline);
                let _ = sleep.as_mut().poll(cx);
                std::task::Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::CancellationToken;
    use crate::error::ErrorCategory;
    use crate::http::execution::{BodyEvent, RangeIntent};
    use crate::http::transport::RequestSpec;
    use crate::http::HttpExecution;

    fn spec(url: &str) -> RequestSpec {
        RequestSpec {
            url: url.to_string(),
            ..RequestSpec::default()
        }
    }

    fn range_req(url: &str, start: u64, end: u64, total: u64) -> TransferRequest {
        TransferRequest {
            spec: spec(url),
            intent: TransferIntent::Range(RangeIntent {
                range: (start, end),
                established_total: Some(total),
                expected_validators: None,
                full_response: FullResponsePolicy::InvalidRange,
            }),
        }
    }

    fn full_req(url: &str) -> TransferRequest {
        TransferRequest {
            spec: spec(url),
            intent: TransferIntent::Full,
        }
    }

    // ---- Call ordering, matching, log, mismatch, assertion ----

    #[tokio::test]
    async fn probe_and_transfer_outcomes_follow_script_order() {
        let scripted = ScriptedHttp::new()
            .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
                total_size: Some(3),
                accept_ranges: true,
                ..ProbeMetadata::default()
            }))
            .expect_transfer(
                TransferStep::new().ok(TransferOk::new().total(3).chunk(b"abc".as_slice())),
            );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();

        let outcome = execution
            .probe(
                ProbeRequest {
                    spec: spec("https://x/f"),
                    segmentation_threshold: 2,
                    verify_range_support: true,
                },
                &cancel,
            )
            .await
            .expect("probe outcome");
        assert_eq!(outcome.metadata.total_size, Some(3));

        let response = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect("transfer response");
        assert_eq!(response.total_size, Some(3));
        let mut body = response.body;
        assert_eq!(
            body.next_chunk(&cancel).await.expect("chunk").data(),
            Some(&bytes::Bytes::from_static(b"abc"))
        );
        assert_eq!(body.next_chunk(&cancel).await.expect("eof"), BodyEvent::End);

        scripted.assert_all_consumed();
        let log = scripted.request_log();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].kind, CallKind::Probe);
        assert_eq!(log[0].url, "https://x/f");
        assert_eq!(log[1].kind, CallKind::Transfer);
    }

    #[tokio::test]
    async fn wrong_call_kind_is_an_actionable_mismatch() {
        let scripted =
            ScriptedHttp::new().expect_probe(ProbeStep::new().ok_meta(ProbeMetadata::default()));
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect_err("kind mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m)
                if m.contains("expected a probe call") && m.contains("transfer")),
            "mismatch: {:?}",
            err.error
        );
        // The step stays available for a corrected call.
        let outcome = execution
            .probe(
                ProbeRequest {
                    spec: spec("https://x/f"),
                    segmentation_threshold: 1,
                    verify_range_support: false,
                },
                &cancel,
            )
            .await
            .expect("probe after mismatch");
        assert_eq!(outcome.metadata.total_size, None);
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn mismatch_reports_url_difference() {
        let scripted =
            ScriptedHttp::new().expect_transfer(TransferStep::new().url("https://x/other"));
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect_err("url mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m)
                if m.contains("https://x/other") && m.contains("https://x/f")),
            "mismatch: {:?}",
            err.error
        );
    }

    #[tokio::test]
    async fn mismatch_reports_missing_relevant_header() {
        let scripted = ScriptedHttp::new()
            .expect_transfer(TransferStep::new().header("authorization", "Bearer tok"));
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect_err("header mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m)
                if m.contains("authorization")),
            "mismatch: {:?}",
            err.error
        );
        // Case-insensitive header name matching; present header matches.
        let scripted = ScriptedHttp::new().expect_transfer(
            TransferStep::new()
                .header("Authorization", "Bearer tok")
                .ok(TransferOk::new()),
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let mut req = full_req("https://x/f");
        req.spec
            .headers
            .push(("AUTHORIZATION".into(), "Bearer tok".into()));
        assert!(execution.transfer(req, &cancel).await.is_ok());
    }

    #[tokio::test]
    async fn mismatch_reports_probe_policy_difference() {
        let scripted = ScriptedHttp::new().expect_probe(
            ProbeStep::new()
                .verify_range_support(false)
                .segmentation_threshold(64),
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .probe(
                ProbeRequest {
                    spec: spec("https://x/f"),
                    segmentation_threshold: 64,
                    verify_range_support: true,
                },
                &cancel,
            )
            .await
            .expect_err("policy mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m)
                if m.contains("verify_range_support=false")),
            "mismatch: {:?}",
            err.error
        );
    }

    #[tokio::test]
    async fn mismatch_reports_range_total_and_validator_differences() {
        let validators = ResourceValidators {
            etag: Some("\"v1\"".into()),
            ..ResourceValidators::default()
        };
        let other = ResourceValidators {
            etag: Some("\"v2\"".into()),
            ..ResourceValidators::default()
        };
        let scripted = ScriptedHttp::new().expect_transfer(
            TransferStep::new()
                .range((100, 199))
                .established_total(1000)
                .validators(&validators)
                .ok(TransferOk::new().range(100, 199).total(1000)),
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();

        // Wrong range.
        let err = execution
            .transfer(range_req("https://x/f", 0, 99, 1000), &cancel)
            .await
            .expect_err("range mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m) if m.contains("(100, 199)")),
            "mismatch: {:?}",
            err.error
        );

        // Right range, wrong total.
        let mut req = range_req("https://x/f", 100, 199, 1000);
        req.intent = TransferIntent::Range(RangeIntent {
            range: (100, 199),
            established_total: None,
            expected_validators: None,
            full_response: FullResponsePolicy::InvalidRange,
        });
        let err = execution
            .transfer(req, &cancel)
            .await
            .expect_err("total mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m) if m.contains("established_total")),
            "mismatch: {:?}",
            err.error
        );

        // Right range and total, wrong validators.
        let mut req = range_req("https://x/f", 100, 199, 1000);
        req.intent = TransferIntent::Range(RangeIntent {
            range: (100, 199),
            established_total: Some(1000),
            expected_validators: Some(other),
            full_response: FullResponsePolicy::InvalidRange,
        });
        let err = execution
            .transfer(req, &cancel)
            .await
            .expect_err("validator mismatch");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m) if m.contains("validators")),
            "mismatch: {:?}",
            err.error
        );

        // Exact match finally consumes the step.
        let mut req = range_req("https://x/f", 100, 199, 1000);
        req.intent = TransferIntent::Range(RangeIntent {
            range: (100, 199),
            established_total: Some(1000),
            expected_validators: Some(validators),
            full_response: FullResponsePolicy::InvalidRange,
        });
        let response = execution.transfer(req, &cancel).await.expect("matched");
        assert_eq!(response.start, 100);
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn exhausted_script_mismatches_clearly() {
        let scripted = ScriptedHttp::new();
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .probe(
                ProbeRequest {
                    spec: spec("https://x/f"),
                    segmentation_threshold: 1,
                    verify_range_support: false,
                },
                &cancel,
            )
            .await
            .expect_err("exhausted");
        assert!(
            matches!(err.error, DownloadError::Protocol(ref m) if m.contains("script exhausted")),
            "mismatch: {:?}",
            err.error
        );
    }

    #[test]
    #[should_panic(expected = "not fully consumed")]
    fn all_steps_consumed_assertion_panics_with_remaining_summary() {
        let scripted = ScriptedHttp::new()
            .expect_probe(ProbeStep::new())
            .expect_transfer(TransferStep::new());
        let _ = scripted;
        // No calls made: both steps remain.
        ScriptedHttp::assert_all_consumed(&scripted);
    }

    // ---- Outcomes, body events, gates, unordered phases ----

    #[tokio::test]
    async fn body_events_deliver_in_order_without_sockets_or_delay() {
        let scripted = ScriptedHttp::new().expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(6)
                .chunk(b"ab".as_slice())
                .chunk(b"cde".as_slice())
                .chunk(b"f".as_slice())),
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let body = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect("transfer")
            .body;
        let started = std::time::Instant::now();
        let mut body = body;
        let mut got = Vec::new();
        loop {
            match body.next_chunk(&cancel).await.expect("event") {
                BodyEvent::Data(d) => got.push(d),
                BodyEvent::End => break,
                BodyEvent::Paused => panic!("unexpected pause"),
            }
        }
        assert_eq!(
            got.iter().map(|b| b.as_ref()).collect::<Vec<_>>(),
            vec![&b"ab"[..], &b"cde"[..], &b"f"[..]]
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "no wall-clock dependency: {:?}",
            started.elapsed()
        );
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn fault_after_prefix_yields_chunks_then_classified_fault() {
        let scripted = ScriptedHttp::new().expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(4)
                .chunk(b"ab".as_slice())
                .fault(DownloadError::Connection("connection reset".into()))),
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let mut body = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect("transfer")
            .body;
        assert_eq!(
            body.next_chunk(&cancel).await.expect("chunk").data(),
            Some(&bytes::Bytes::from_static(b"ab"))
        );
        let err = body.next_chunk(&cancel).await.expect_err("fault");
        assert!(matches!(err, DownloadError::Connection(ref m) if m == "connection reset"));
        assert_eq!(err.category(), ErrorCategory::Connection);
    }

    #[tokio::test]
    async fn idle_timeout_event_is_immediate_and_shared() {
        let scripted = ScriptedHttp::new()
            .expect_transfer(TransferStep::new().ok(TransferOk::new().total(1).idle_timeout()));
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let mut body = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect("transfer")
            .body;
        let started = std::time::Instant::now();
        let err = body.next_chunk(&cancel).await.expect_err("idle timeout");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "IdleTimeout is immediate, took {:?}",
            started.elapsed()
        );
        assert!(
            matches!(err, DownloadError::Connection(ref m) if m.contains("read idle timeout")),
            "same semantic error as the production wrapper: {err:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_cancellation_unblocks_deterministically() {
        let scripted = ScriptedHttp::new().expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(2)
                .chunk(b"ab".as_slice())
                .wait_for_cancellation()),
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let mut body = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect("transfer")
            .body;
        assert_eq!(
            body.next_chunk(&cancel).await.expect("chunk").data(),
            Some(&bytes::Bytes::from_static(b"ab"))
        );
        let cancel2 = cancel.clone();
        let waiter = tokio::spawn(async move {
            let started = std::time::Instant::now();
            let err = body.next_chunk(&cancel2).await.expect_err("cancelled");
            (err, started.elapsed())
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel();
        let (err, elapsed) = waiter.await.expect("join");
        assert!(matches!(err, DownloadError::Cancelled));
        assert!(
            elapsed < Duration::from_secs(2),
            "cancellation unblocks without a real-time timeout wait: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn fail_reply_carries_retry_after_and_challenge() {
        let scripted = ScriptedHttp::new()
            .expect_transfer(TransferStep::new().fail_retry_after(
                DownloadError::RateLimited { status: 429 },
                Duration::from_secs(7),
            ))
            .expect_probe(ProbeStep::new().fail(HttpFailure {
                error: DownloadError::AuthenticationRequired,
                retry_after: None,
                challenge: Some(crate::control::auth::Challenge {
                    status: 401,
                    authenticate: vec!["Basic".into()],
                    origin: "x".into(),
                }),
            }));
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect_err("rate limited");
        assert_eq!(err.retry_after, Some(Duration::from_secs(7)));
        let err = execution
            .probe(
                ProbeRequest {
                    spec: spec("https://x/f"),
                    segmentation_threshold: 1,
                    verify_range_support: false,
                },
                &cancel,
            )
            .await
            .expect_err("challenge");
        assert!(err.challenge.is_some());
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn unordered_range_phase_admits_any_arrival_order() {
        let scripted = ScriptedHttp::new()
            .expect_unordered_ranges(
                "workers",
                vec![
                    TransferStep::new().range((0, 99)).ok(TransferOk::new()
                        .range(0, 99)
                        .total(200)
                        .chunk(b"a".as_slice())),
                    TransferStep::new().range((100, 199)).ok(TransferOk::new()
                        .range(100, 199)
                        .total(200)
                        .chunk(b"b".as_slice())),
                ],
            )
            .expect_transfer(
                TransferStep::new()
                    .range((200, 299))
                    .ok(TransferOk::new().range(200, 299).total(300)),
            );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();

        // Deliberately reversed arrival order: no flakiness allowed.
        let b = execution
            .transfer(range_req("https://x/f", 100, 199, 200), &cancel)
            .await
            .expect("second range first");
        assert_eq!(b.start, 100);
        let a = execution
            .transfer(range_req("https://x/f", 0, 99, 200), &cancel)
            .await
            .expect("first range second");
        assert_eq!(a.start, 0);
        // The phase only advances once fully consumed: the next step
        // follows it.
        let c = execution
            .transfer(range_req("https://x/f", 200, 299, 300), &cancel)
            .await
            .expect("after phase");
        assert_eq!(c.start, 200);
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn unordered_range_phase_allows_duplicate_ranges_for_retries() {
        let scripted = ScriptedHttp::new().expect_unordered_ranges(
            "retryable-range",
            vec![
                TransferStep::new()
                    .range((0, 99))
                    .fail_error(DownloadError::Server { status: 503 }),
                TransferStep::new().range((0, 99)).ok(TransferOk::new()
                    .range(0, 99)
                    .total(100)
                    .chunk(b"ok".as_slice())),
            ],
        );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();
        let err = execution
            .transfer(range_req("https://x/f", 0, 99, 100), &cancel)
            .await
            .expect_err("first attempt fails");
        assert!(matches!(err.error, DownloadError::Server { status: 503 }));
        let ok = execution
            .transfer(range_req("https://x/f", 0, 99, 100), &cancel)
            .await
            .expect("retry succeeds");
        assert_eq!(ok.start, 0);
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn gate_parks_calls_until_opened() {
        let scripted = ScriptedHttp::new()
            .expect_transfer(
                TransferStep::new()
                    .range((0, 99))
                    .ok(TransferOk::new().range(0, 99).total(200)),
            )
            .gate("round-two")
            .expect_transfer(
                TransferStep::new()
                    .range((100, 199))
                    .ok(TransferOk::new().range(100, 199).total(200)),
            );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();

        let first = execution
            .transfer(range_req("https://x/f", 0, 99, 200), &cancel)
            .await
            .expect("before gate");
        assert_eq!(first.start, 0);

        // The next call parks at the closed gate.
        let parked_execution = execution.clone();
        let parked_cancel = cancel.clone();
        let parked = tokio::spawn(async move {
            parked_execution
                .transfer(range_req("https://x/f", 100, 199, 200), &parked_cancel)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !parked.is_finished(),
            "call must stay parked while the gate is closed"
        );

        scripted.open_gate("round-two");
        let second = parked.await.expect("join").expect("after gate");
        assert_eq!(second.start, 100);
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn gate_parks_multiple_calls_released_together() {
        let scripted = ScriptedHttp::new()
            .gate("all-workers")
            .expect_unordered_ranges(
                "released",
                vec![
                    TransferStep::new()
                        .range((0, 9))
                        .ok(TransferOk::new().range(0, 9).total(20)),
                    TransferStep::new()
                        .range((10, 19))
                        .ok(TransferOk::new().range(10, 19).total(20)),
                ],
            );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();

        let e1 = execution.clone();
        let c1 = cancel.clone();
        let w1 =
            tokio::spawn(async move { e1.transfer(range_req("https://x/f", 0, 9, 20), &c1).await });
        let e2 = execution.clone();
        let c2 = cancel.clone();
        let w2 =
            tokio::spawn(
                async move { e2.transfer(range_req("https://x/f", 10, 19, 20), &c2).await },
            );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!w1.is_finished() && !w2.is_finished(), "both parked");

        scripted.open_gate("all-workers");
        let r1 = w1.await.expect("join").expect("worker 1");
        let r2 = w2.await.expect("join").expect("worker 2");
        assert_eq!((r1.start, r2.start), (0, 10));
        scripted.assert_all_consumed();
    }

    #[tokio::test]
    async fn wait_for_phase_resolves_only_after_consumption() {
        let scripted = ScriptedHttp::new()
            .expect_labeled_transfer(
                "first",
                TransferStep::new().ok(TransferOk::new().total(1).chunk(b"x".as_slice())),
            )
            .expect_labeled_transfer(
                "second",
                TransferStep::new().ok(TransferOk::new().total(1).chunk(b"y".as_slice())),
            );
        let execution = HttpExecution::from_adapter(scripted.clone());
        let cancel = CancellationToken::new();

        assert!(
            tokio::time::timeout(Duration::from_millis(20), scripted.wait_for_phase("first"))
                .await
                .is_err(),
            "label not consumed yet"
        );
        execution
            .transfer(full_req("https://x/f"), &cancel)
            .await
            .expect("first");
        tokio::time::timeout(Duration::from_secs(1), scripted.wait_for_phase("first"))
            .await
            .expect("first consumed");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), scripted.wait_for_phase("second"))
                .await
                .is_err(),
            "second not consumed yet"
        );
    }
}
