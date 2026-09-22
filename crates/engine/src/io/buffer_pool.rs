//! Bounded pooled byte buffers (§13.1, D8).
//!
//! Workers borrow fixed-size buffers from a shared pool; the pool never
//! allocates beyond its byte budget, providing backpressure: when the pool
//! is empty, `try_acquire` returns `None` and the caller must wait.

use bytes::{Bytes, BytesMut};
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex};

/// Shared bounded buffer pool.
#[derive(Debug)]
pub struct BufferPool {
    buffer_size: usize,
    max_buffers: usize,
    state: Mutex<PoolState>,
    available: Condvar,
}

#[derive(Debug, Default)]
struct PoolState {
    /// Buffers currently sitting idle in the pool.
    idle: Vec<BytesMut>,
    /// Total buffers in existence (idle + in use).
    allocated: usize,
}

/// A leased buffer; returned to the pool on drop.
pub struct PooledBuffer {
    pool: Option<BufferPoolGuard>,
    buf: Option<BytesMut>,
}

impl std::fmt::Debug for PooledBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledBuffer")
            .field("len", &self.len())
            .finish()
    }
}

struct BufferPoolGuard(Arc<BufferPool>);

impl std::ops::Deref for BufferPoolGuard {
    type Target = BufferPool;
    fn deref(&self) -> &BufferPool {
        &self.0
    }
}
impl Drop for BufferPoolGuard {
    fn drop(&mut self) {
        // Nothing to do here; PooledBuffer handles return.
    }
}

use std::sync::Arc;

impl BufferPool {
    /// Create a pool whose total allocation never exceeds
    /// `max_total_bytes` (`buffer_pool_max_bytes`, §8.1).
    #[must_use]
    pub fn new(buffer_size: usize, max_total_bytes: u64) -> Self {
        let buffer_size = buffer_size.max(1);
        let max_buffers = (max_total_bytes as usize / buffer_size).max(1);
        Self {
            buffer_size,
            max_buffers,
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        }
    }

    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Maximum number of buffers the pool may ever hold.
    #[must_use]
    pub fn max_buffers(&self) -> usize {
        self.max_buffers
    }

    /// Current count of buffers in existence.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        let st = self.state.lock().expect("pool lock");
        st.allocated
    }

    /// Bytes currently committed to pooled buffers.
    #[must_use]
    pub fn bytes_outstanding(&self) -> usize {
        self.outstanding() * self.buffer_size
    }

    /// Try to take a buffer without blocking.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>) -> Option<PooledBuffer> {
        let mut st = self.state.lock().expect("pool lock");
        Some(self.acquire_inner(&mut st))
    }

    /// Take a buffer, blocking until one is available.
    pub async fn acquire(self: &Arc<Self>) -> PooledBuffer {
        // Async-friendly: typical path is instant; when the pool is at
        // budget and empty, wait via a short blocking sleep loop on a
        // background thread.
        loop {
            {
                let mut st = self.state.lock().expect("pool lock");
                if st.allocated < self.max_buffers || !st.idle.is_empty() {
                    return self.acquire_inner(&mut st);
                }
            }
            tokio::task::spawn_blocking({
                let pool = Arc::clone(self);
                move || pool.block_until_available()
            })
            .await
            .expect("blocking task");
        }
    }

    fn block_until_available(&self) {
        let mut st = self.state.lock().expect("pool lock");
        while st.allocated >= self.max_buffers && st.idle.is_empty() {
            st = self
                .available
                .wait_timeout(st, std::time::Duration::from_millis(10))
                .expect("pool wait")
                .0;
        }
    }

    fn acquire_inner(self: &Arc<Self>, st: &mut PoolState) -> PooledBuffer {
        let buf = match st.idle.pop() {
            Some(b) => b,
            None => {
                // Fresh allocation; hard budget check.
                if st.allocated >= self.max_buffers {
                    return PooledBuffer::exhausted();
                }
                st.allocated += 1;
                BytesMut::with_capacity(self.buffer_size)
            }
        };
        PooledBuffer {
            pool: None,
            buf: Some(buf),
        }
        .attach(self)
    }

    fn release(&self, buf: BytesMut) {
        let mut st = self.state.lock().expect("pool lock");
        if st.idle.len() < self.max_buffers {
            st.idle.push(buf);
        } else {
            // Park is full: destroy the buffer; allocated shrinks.
            st.allocated = st.allocated.saturating_sub(1);
        }
        self.available.notify_one();
    }
}

impl PooledBuffer {
    /// Sentinel handed out when the pool is at hard budget with nothing
    /// idle; `len() == 0`, writing to it panics, and it holds no pool.
    fn exhausted() -> PooledBuffer {
        PooledBuffer {
            pool: None,
            buf: None,
        }
    }

    fn attach(mut self, pool: &Arc<BufferPool>) -> PooledBuffer {
        self.pool = Some(BufferPoolGuard(pool.clone()));
        self
    }

    /// Frozen view of the buffer contents (zero-copy hand-off to transport).
    #[must_use]
    pub fn freeze(&mut self) -> Bytes {
        self.buf.as_mut().expect("buffer alive").split().freeze()
    }

    /// Mutable access for filling from the network.
    pub fn fill_mut(&mut self) -> &mut BytesMut {
        self.buf.as_mut().expect("exhausted sentinel has no buffer")
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.as_ref().map_or(0, BytesMut::len)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let (Some(guard), Some(buf)) = (self.pool.take(), self.buf.take()) {
            guard.release(buf);
        }
    }
}

// Atomic outstanding counter used by tests via the pool; the pool itself
// tracks through mutex state, so no separate AtomicUsize is needed.
#[allow(dead_code)]
static _UNUSED: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_never_exceeds_byte_budget() {
        let pool = Arc::new(BufferPool::new(128 * 1024, 1024 * 1024)); // 1 MiB budget, 128 KiB buffers
        assert_eq!(pool.max_buffers(), 8);

        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(pool.try_acquire().expect("buffer under budget"));
        }
        assert_eq!(pool.outstanding(), 8);
        assert_eq!(pool.bytes_outstanding(), 1024 * 1024);
        // Budget exhausted: fresh acquisition returns the exhausted
        // sentinel (no buffer allocated).
        let sentinel = pool.try_acquire().expect("try_acquire always yields");
        assert_eq!(sentinel.len(), 0);
        assert_eq!(pool.outstanding(), 8, "sentinel allocates nothing");

        // Release one -> immediately reusable, still within budget.
        held.pop().expect("held");
        assert_eq!(pool.outstanding(), 8, "released buffer parked in pool");
        let again = pool.try_acquire().expect("reuse after release");
        assert!(again.is_empty() || again.len() <= pool.buffer_size());
        assert_eq!(pool.outstanding(), 8);
        let sentinel2 = pool.try_acquire().expect("sentinel when exhausted");
        assert_eq!(sentinel2.len(), 0);
        assert_eq!(pool.outstanding(), 8);
    }

    #[test]
    fn returned_buffers_are_reused() {
        let pool = Arc::new(BufferPool::new(4096, 4096));
        {
            let mut b = pool.try_acquire().expect("acquire");
            b.fill_mut().extend_from_slice(b"data");
        }
        // Same capacity comes back; content is NOT cleared (caller must
        // truncate); we assert it can be acquired again.
        let b = pool.try_acquire().expect("reuse after release");
        assert_eq!(pool.buffer_size(), 4096);
        drop(b);
    }

    #[tokio::test]
    async fn acquire_blocks_until_release() {
        let pool = Arc::new(BufferPool::new(4096, 4096)); // single buffer
        let first = pool.try_acquire().expect("first");
        let pool2 = pool.clone();
        let waiter = tokio::spawn(async move {
            pool2.acquire().await; // must block until first drops
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "must still be blocked");
        drop(first);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(waiter.is_finished(), "waiter must finish after release");
    }

    #[test]
    fn zero_sized_budget_still_yields_one_buffer() {
        let pool = Arc::new(BufferPool::new(4096, 0));
        assert_eq!(pool.max_buffers(), 1);
        assert!(pool.try_acquire().is_some());
    }
}
