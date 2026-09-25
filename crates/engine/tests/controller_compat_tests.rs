//! Compile-level compatibility coverage for the controller rename: the
//! deprecated `SingleStreamController` name must keep resolving to the
//! canonical `DownloadController` implementation (same type, same
//! constructors and methods) with a deprecation diagnostic pointing at the
//! canonical name.

// The deprecated name is intentionally used here; the rest of the codebase
// and CI lint stay clean because only this compatibility test opts out.
#![allow(deprecated)]

use kdown_engine::config::{EngineConfig, NetworkPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadController, SingleStreamController};

#[test]
fn deprecated_alias_resolves_to_download_controller() {
    let cfg = EngineConfig::default();
    let transport = HttpTransport::new(NetworkPolicy::default()).expect("transport");
    let canonical = DownloadController::new(transport.clone(), cfg.clone());
    let legacy = SingleStreamController::new(transport, cfg);

    // A value built through the deprecated alias IS the canonical type:
    // the alias adds no second implementation.
    let _same_impl: &DownloadController = &legacy;

    // Inherent methods resolve identically through either name.
    let bucket = std::sync::Arc::new(kdown_engine::control::rate_limit::TokenBucket::new(0));
    let canonical = canonical.with_global_rate_bucket(bucket.clone());
    let legacy = legacy.with_global_rate_bucket(bucket);
    let _ = (canonical, legacy);

    // The other constructors are reachable through the alias as well.
    let _through_alias: SingleStreamController =
        SingleStreamController::with_execution(noop_execution(), EngineConfig::default());
}

fn noop_execution() -> kdown_engine::http::HttpExecution {
    // `HttpExecution::from_adapter` requires an executor; the cheapest
    // compile-level proof that constructors resolve is calling them, so use
    // the production adapter built from a default config.
    let cfg = EngineConfig::default();
    let transport = kdown_engine::http::HttpTransport::from_config(&cfg).expect("transport");
    kdown_engine::http::HttpExecution::from_adapter(transport)
}
