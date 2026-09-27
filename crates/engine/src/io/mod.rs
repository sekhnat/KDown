//! Output storage and buffer management (§13-§14, §33).
//! Implementation module — not part of the supported surface
//! (docs/api-surface.md).

// The standalone pool is exercised by relocated internal allocation
// tests; the transfer path does not use it.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod buffer_pool;
pub(crate) mod destination_lease;
#[cfg(test)]
pub(crate) mod fault_script;
pub(crate) mod output_session;
pub(crate) mod positional;
pub(crate) mod publish;
pub(crate) mod sanitize;
pub(crate) mod sink;
pub(crate) mod transfer_ledger;
pub(crate) mod write_budget;
pub(crate) mod write_executor;
pub(crate) mod write_frontier;
pub(crate) mod writer_lane;

// In-crate code and relocated internal tests address these through
// `io::sink::…` submodule paths; the re-export below keeps the
// `io::sanitize_filename` convenience path used by the fuzz targets.
pub(crate) use sanitize::sanitize_filename;
#[allow(unused_imports)]
pub(crate) use sink::{AbortDisposition, FileSink, FlushLevel, Sink, SinkError, TempFileSpec};
