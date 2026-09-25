//! Control plane: cancellation, retry classification, and rate limiting
//! (§17-§18).

pub mod adaptive;
pub mod auth;
pub mod cancellation;
pub mod origin;
pub mod rate_limit;
pub mod retry;

pub use adaptive::{AdaptiveConfig, AdaptiveController, Decision, WindowSample};
pub use cancellation::{CancellationReason, CancellationToken};
pub use origin::{normalized_origin, OriginPermit, OriginRegistry};
pub use rate_limit::{Acquisition, RateLimiter, TokenBucket};
pub use retry::{parse_retry_after, RetryClassifier, RetryDecision};
