//! Output storage and buffer management (§13-§14, §33).

pub mod buffer_pool;
pub mod sanitize;
pub mod sink;

pub use buffer_pool::{BufferPool, PooledBuffer};
pub use sanitize::sanitize_filename;
pub use sink::{AbortDisposition, FileSink, FlushLevel, Sink, SinkError, TempFileSpec};
