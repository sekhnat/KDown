//! Control plane: cancellation, retry classification, and rate limiting
//! (§17-§18).

pub mod auth;
pub mod cancellation;
pub mod rate_limit;
pub mod retry;

pub use cancellation::{CancellationReason, CancellationToken};
pub use rate_limit::{Acquisition, RateLimiter, TokenBucket};
pub use retry::{parse_retry_after, RetryClassifier, RetryDecision};