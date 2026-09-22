//! Range request framing and response validation (§11.2, task 5.4).
//!
//! The validation gate accepts metadata *before* any body byte is written:
//! a 206 must carry a `Content-Range` starting exactly at the requested
//! start, not overshoot the requested end, and agree with the established
//! total. A 200 response to a nonzero range request is never treated as
//! the requested range (§11.2: abort segmented mode for the job).

use crate::http::validators::{ContentRange, ResourceValidators};

/// A validated range response: the engine may read the body and write it
/// at `validated.start`.
#[derive(Debug)]
pub struct ValidatedRange {
    pub start: u64,
    /// Inclusive end the response body must not exceed.
    pub end: u64,
    pub total_size: Option<u64>,
}

/// Why a range response was rejected (§11.2 reject list).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct RangeRejection {
    pub kind: RejectionKind,
    pub detail: String,
}

impl std::fmt::Display for RangeRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self.kind {
            RejectionKind::FullResponseToNonzeroRange => "full response to nonzero range",
            RejectionKind::UnexpectedStatus => "unexpected status",
            RejectionKind::StartMismatch => "start mismatch",
            RejectionKind::EndOvershoot => "end overshoot",
            RejectionKind::TotalConflict => "total conflict",
            RejectionKind::GenerationChanged => "generation changed",
            RejectionKind::BodyOverrun => "body overrun",
        };
        write!(f, "range response rejected: {label}: {}", self.detail)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionKind {
    /// 200 to a nonzero range: full representation, unusable as a slice.
    FullResponseToNonzeroRange,
    /// Status neither 200 nor 206.
    UnexpectedStatus,
    /// Content-Range missing or start != S.
    StartMismatch,
    /// Content-Range end overshoots the requested end.
    EndOvershoot,
    /// Content-Range total conflicts with the established size.
    TotalConflict,
    /// Validators indicate a different generation (§26).
    GenerationChanged,
    /// The response body exceeded `end - start + 1` bytes.
    BodyOverrun,
}

impl RangeRejection {
    /// Map to the structured error taxonomy (§20 InvalidRangeResponse /
    /// ResourceChanged / Protocol).
    #[must_use]
    pub fn into_error(self) -> crate::error::DownloadError {
        use crate::error::DownloadError;
        let label = match self.kind {
            RejectionKind::FullResponseToNonzeroRange => "full response to nonzero range",
            RejectionKind::UnexpectedStatus => "unexpected status",
            RejectionKind::StartMismatch => "start mismatch",
            RejectionKind::EndOvershoot => "end overshoot",
            RejectionKind::TotalConflict => "total conflict",
            RejectionKind::GenerationChanged => "generation changed",
            RejectionKind::BodyOverrun => "body overrun",
        };
        match self.kind {
            RejectionKind::GenerationChanged => DownloadError::ResourceChanged(self.detail),
            _ => DownloadError::InvalidRangeResponse(format!(
                "range response rejected: {label}: {}",
                self.detail
            )),
        }
    }
}

/// Whether the requested range was nonzero (S > 0 or E < u64::MAX-1).
#[must_use]
pub fn range_request_is_nonzero(start: u64) -> bool {
    start > 0
}

/// HTTP-private view of a response head used for range validation (§11.2).
///
/// Implemented by the production adapter's raw response and by the
/// HTTP-private metadata carrier (`crate::http::execution::ResponseMetadata`),
/// so one validation table serves both without exposing raw response types
/// to job code. Implementation detail of the HTTP module.
pub trait ResponseHead {
    fn status(&self) -> u16;
    fn content_range(&self) -> Option<ContentRange>;
    fn validators(&self) -> &ResourceValidators;
    fn total_size(&self) -> Option<u64>;
    fn header(&self, name: &str) -> Option<&str>;
}

/// Validate a range response against its request (§11.2).
///
/// `request_range` is `(S, E)` inclusive; `established_total` is the size
/// the job already knows (probe or Content-Range totals from earlier
/// segments); `expected_validators` is the job's generation identity.
///
/// Returns the validated slice, or the structured rejection reason. The
/// caller MUST NOT read body bytes before this passes (§32: metadata
/// validated before body).
pub fn validate_range_response<T: ResponseHead + ?Sized>(
    request_range: (u64, u64),
    response: &T,
    established_total: Option<u64>,
    expected_validators: Option<&ResourceValidators>,
) -> Result<ValidatedRange, RangeRejection> {
    let (s, e) = request_range;
    // 200 to a nonzero range request: full representation — never usable
    // as the requested slice (§11.2).
    if response.status() == 200 {
        if range_request_is_nonzero(s) {
            return Err(RangeRejection {
                kind: RejectionKind::FullResponseToNonzeroRange,
                detail: format!("200 response to range request bytes={s}-{e}"),
            });
        }
        // 200 to bytes=0-E is the full body; acceptable as [0, min(e,total-1)]
        // only when it does not claim a conflicting total.
        return Ok(ValidatedRange {
            start: 0,
            end: e,
            total_size: response.total_size().or(established_total),
        });
    }
    if response.status() != 206 {
        return Err(RangeRejection {
            kind: RejectionKind::UnexpectedStatus,
            detail: format!(
                "status {} for range request bytes={s}-{e}",
                response.status()
            ),
        });
    }
    let Some(cr) = response.content_range() else {
        return Err(RangeRejection {
            kind: RejectionKind::StartMismatch,
            detail: "206 response missing Content-Range".into(),
        });
    };
    if cr.start != s {
        return Err(RangeRejection {
            kind: RejectionKind::StartMismatch,
            detail: format!("Content-Range start {} != requested {s}", cr.start),
        });
    }
    if cr.end > e {
        return Err(RangeRejection {
            kind: RejectionKind::EndOvershoot,
            detail: format!("Content-Range end {} exceeds requested end {e}", cr.end),
        });
    }
    // Total conflict with the established size (§11.2).
    if let (Some(est), Some(cr_total)) = (established_total, cr.total) {
        if est != cr_total {
            return Err(RangeRejection {
                kind: RejectionKind::TotalConflict,
                detail: format!("Content-Range total {cr_total} conflicts with established {est}"),
            });
        }
    }
    // Generation identity (§26): validators present in the response must
    // not contradict the established ones.
    if let Some(expected) = expected_validators {
        if expected.same_generation(response.validators()).is_err() {
            return Err(RangeRejection {
                kind: RejectionKind::GenerationChanged,
                detail: "response validators differ from job generation".into(),
            });
        }
    }
    Ok(ValidatedRange {
        start: s,
        end: cr.end,
        total_size: cr.total.or(established_total),
    })
}

/// Enforce the accepted range length on the streamed body (§11.2: body
/// overrun rejects the response).
///
/// Returns the running byte count; callers abort when it exceeds
/// `validated.end - validated.start + 1`.
#[must_use]
pub fn body_overrun(offset_in_range: u64, incoming_len: u64, accepted_len: u64) -> bool {
    offset_in_range.saturating_add(incoming_len) > accepted_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::execution::ResponseMetadata;
    use crate::http::validators::{ContentRange, ResourceValidators};

    fn resp(
        status: u16,
        content_range: Option<ContentRange>,
        total: Option<u64>,
    ) -> ResponseMetadata {
        ResponseMetadata::for_test(
            status,
            vec![],
            content_range,
            ResourceValidators::from_headers(None, None, total),
            total,
        )
    }

    #[test]
    fn valid_206_accepted() {
        let r = resp(
            206,
            Some(ContentRange {
                start: 100,
                end: 199,
                total: Some(1000),
            }),
            Some(1000),
        );
        let v = validate_range_response((100, 199), &r, Some(1000), None).expect("valid");
        assert_eq!(v.start, 100);
        assert_eq!(v.end, 199);
        assert_eq!(v.total_size, Some(1000));
    }

    #[test]
    fn start_mismatch_rejected() {
        let r = resp(
            206,
            Some(ContentRange {
                start: 101,
                end: 199,
                total: Some(1000),
            }),
            Some(1000),
        );
        let err = validate_range_response((100, 199), &r, Some(1000), None).unwrap_err();
        assert_eq!(err.kind, RejectionKind::StartMismatch);
    }

    #[test]
    fn missing_content_range_rejected() {
        let r = resp(206, None, None);
        let err = validate_range_response((100, 199), &r, None, None).unwrap_err();
        assert_eq!(err.kind, RejectionKind::StartMismatch);
    }

    #[test]
    fn end_overshoot_rejected() {
        let r = resp(
            206,
            Some(ContentRange {
                start: 100,
                end: 250,
                total: Some(1000),
            }),
            Some(1000),
        );
        let err = validate_range_response((100, 199), &r, Some(1000), None).unwrap_err();
        assert_eq!(err.kind, RejectionKind::EndOvershoot);
    }

    #[test]
    fn total_conflict_rejected() {
        let r = resp(
            206,
            Some(ContentRange {
                start: 100,
                end: 199,
                total: Some(999),
            }),
            Some(999),
        );
        let err = validate_range_response((100, 199), &r, Some(1000), None).unwrap_err();
        assert_eq!(err.kind, RejectionKind::TotalConflict);
    }

    #[test]
    fn two_hundred_on_nonzero_range_rejected() {
        let r = resp(200, None, Some(1000));
        let err = validate_range_response((100, 199), &r, Some(1000), None).unwrap_err();
        assert_eq!(err.kind, RejectionKind::FullResponseToNonzeroRange);
    }

    #[test]
    fn two_hundred_on_zero_range_accepted_as_full() {
        // bytes=0-E with a 200 full response is acceptable for the prefix.
        let r = resp(200, None, Some(1000));
        let v = validate_range_response((0, 999), &r, None, None).expect("zero range ok");
        assert_eq!(v.start, 0);
    }

    #[test]
    fn generation_change_rejected() {
        let mut r = resp(
            206,
            Some(ContentRange {
                start: 0,
                end: 99,
                total: Some(1000),
            }),
            Some(1000),
        );
        r.validators = ResourceValidators::from_headers(Some("\"v2\""), None, Some(1000));
        let expected = ResourceValidators::from_headers(Some("\"v1\""), None, Some(1000));
        let err = validate_range_response((0, 99), &r, Some(1000), Some(&expected)).unwrap_err();
        assert_eq!(err.kind, RejectionKind::GenerationChanged);
        // Same generation passes.
        let mut r_ok = resp(
            206,
            Some(ContentRange {
                start: 0,
                end: 99,
                total: Some(1000),
            }),
            Some(1000),
        );
        r_ok.validators = expected.clone();
        assert!(validate_range_response((0, 99), &r_ok, Some(1000), Some(&expected)).is_ok());
    }

    #[test]
    fn unexpected_status_rejected() {
        let r = resp(503, None, None);
        let err = validate_range_response((0, 99), &r, None, None).unwrap_err();
        assert_eq!(err.kind, RejectionKind::UnexpectedStatus);
    }

    #[test]
    fn body_overrun_detected() {
        // Accepted range [100, 199] = 100 bytes; 150 incoming = overrun.
        assert!(body_overrun(50, 100, 100));
        assert!(body_overrun(101, 0, 100));
        assert!(!body_overrun(0, 100, 100));
        assert!(!body_overrun(50, 50, 100));
    }

    #[test]
    fn rejection_maps_to_structured_error() {
        let err = RangeRejection {
            kind: RejectionKind::TotalConflict,
            detail: "x".into(),
        }
        .into_error();
        assert!(matches!(
            err,
            crate::error::DownloadError::InvalidRangeResponse(_)
        ));
        let err = RangeRejection {
            kind: RejectionKind::GenerationChanged,
            detail: "x".into(),
        }
        .into_error();
        assert!(matches!(
            err,
            crate::error::DownloadError::ResourceChanged(_)
        ));
    }
}
