//! Output storage and buffer management (§13-§14, §33).

pub mod buffer_pool;
pub(crate) mod destination_lease;
#[cfg(test)]
pub(crate) mod fault_script;
pub(crate) mod output_session;
pub(crate) mod positional;
pub(crate) mod publish;
pub mod sanitize;
pub mod sink;
pub(crate) mod write_budget;
pub(crate) mod write_executor;
pub(crate) mod write_frontier;
pub(crate) mod writer_lane;

pub use buffer_pool::{BufferPool, PooledBuffer};
pub use sanitize::sanitize_filename;
pub use sink::{AbortDisposition, FileSink, FlushLevel, Sink, SinkError, TempFileSpec};
