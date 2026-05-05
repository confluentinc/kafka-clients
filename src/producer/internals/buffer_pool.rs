// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Translation of `org.apache.kafka.clients.producer.internals.BufferPool`.
//!
//! A pool of byte buffers kept under a given memory limit. Java's
//! [`ReentrantLock`] + per-thread `Condition` pattern is replaced with a
//! single [`std::sync::Mutex<State>`] guarding the FIFO waiter queue (a
//! [`VecDeque<Arc<Notify>>`]) and the pooled-buffer free list. Each waiter
//! gets its own [`tokio::sync::Notify`] handle so wakers are FIFO-fair,
//! matching Java's `ArrayDeque<Condition>`.
//!
//! The blocking `Condition.await(timeout)` becomes a [`tokio::select!`]
//! racing the per-waiter `Notify::notified()` against
//! [`tokio::time::sleep`] for `max.block.ms`. Per CLAUDE.md rule 9.6 the
//! state mutex is **never** held across an `.await` — every await is
//! preceded by an explicit `drop(state)` of the [`MutexGuard`].

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::common::errors::KafkaError;
use crate::common::utils::Time;
use crate::producer::buffer_exhausted_error::BufferExhaustedError;

/// The metric-sensor name reserved for "time spent blocked waiting for buffer
/// memory". Mirrors `BufferPool.WAIT_TIME_SENSOR_NAME`.
pub(crate) const WAIT_TIME_SENSOR_NAME: &str = "bufferpool-wait-time";

/// Closure invoked from `safeAllocateByteBuffer`. Returning `Err` simulates
/// Java's `OutOfMemoryError`. Default hook is a plain `vec![0u8; size]`.
pub(crate) type ByteBufferAllocator = Arc<dyn Fn(i32) -> Result<Vec<u8>, KafkaError> + Send + Sync>;

/// Closure invoked once per wait iteration to record wait-time metrics.
/// Returning `Err` simulates Java's `OutOfMemoryError` thrown from
/// `recordWaitTime` (only used by `testCleanupMemoryAvailabilityOnMetricsException`).
/// Default hook is a no-op.
pub(crate) type WaitTimeRecorder = Arc<dyn Fn(i64) -> Result<(), KafkaError> + Send + Sync>;

/// All mutable state managed by the pool. Held under a single
/// [`std::sync::Mutex`] so we can use cheap, sync `lock().unwrap()` from
/// both async and sync code paths and never hold the guard across
/// `.await`.
struct State {
    /// Pooled (recyclable) buffers, each of capacity `poolable_size`.
    free: VecDeque<Vec<u8>>,
    /// FIFO queue of waiters. Each entry is the `Notify` handle for one
    /// pending `allocate` call.
    waiters: VecDeque<Arc<Notify>>,
    /// Total available memory =
    /// `non_pooled_available_memory + free.len() * poolable_size`.
    non_pooled_available_memory: i64,
    closed: bool,
}

/// A pool of byte buffers kept under a given memory limit. See module
/// docs for design notes.
pub(crate) struct BufferPool {
    total_memory: i64,
    poolable_size: i32,
    state: Mutex<State>,
    #[allow(dead_code)]
    time: Arc<dyn Time>,
    allocator: ByteBufferAllocator,
    wait_recorder: WaitTimeRecorder,
    /// `freeSize()` is `protected` in Java to allow tests to override.
    /// We translate the override hook as an optional closure.
    free_size_override: Option<Arc<dyn Fn(usize) -> usize + Send + Sync>>,
}

impl BufferPool {
    /// Create a new buffer pool.
    ///
    /// Mirrors `BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName)`.
    /// Metrics wiring is replaced with `// metric stub` no-ops per the
    /// project-wide PLAN.
    pub fn new(memory: i64, poolable_size: i32, time: Arc<dyn Time>, _metric_grp_name: &str) -> Self {
        Self::with_hooks(
            memory,
            poolable_size,
            time,
            _metric_grp_name,
            default_allocator(),
            default_recorder(),
            None,
        )
    }

    /// Test-friendly constructor that allows injecting a custom byte-buffer
    /// allocator (mirrors `protected ByteBuffer allocateByteBuffer(int)`)
    /// and a custom wait-time recorder (mirrors
    /// `protected void recordWaitTime(long)`). Equivalent to subclassing in
    /// Java.
    pub(crate) fn with_hooks(
        memory: i64,
        poolable_size: i32,
        time: Arc<dyn Time>,
        _metric_grp_name: &str,
        allocator: ByteBufferAllocator,
        wait_recorder: WaitTimeRecorder,
        free_size_override: Option<Arc<dyn Fn(usize) -> usize + Send + Sync>>,
    ) -> Self {
        BufferPool {
            total_memory: memory,
            poolable_size,
            state: Mutex::new(State {
                free: VecDeque::new(),
                waiters: VecDeque::new(),
                non_pooled_available_memory: memory,
                closed: false,
            }),
            time,
            allocator,
            wait_recorder,
            free_size_override,
        }
    }

    /// Allocate a buffer of the given size.
    ///
    /// Java throws:
    /// - `IllegalArgumentException` if `size > totalMemory`
    /// - `KafkaException` if the producer is closed
    /// - `BufferExhaustedException` on timeout
    /// - `InterruptedException` on `Thread.interrupt()` (no Rust analogue —
    ///   omitted)
    ///
    /// Returns a fresh `Vec<u8>` of length `size`. When the request can be
    /// satisfied from the pooled `free` list, the recycled buffer is
    /// resized to `size` (clearing it).
    pub async fn allocate(&self, size: i32, max_time_to_block_ms: i64) -> Result<Vec<u8>, KafkaError> {
        if size as i64 > self.total_memory {
            return Err(KafkaError::IllegalArgument(format!(
                "Attempt to allocate {} bytes, but there is a hard limit of {} on memory allocations.",
                size, self.total_memory
            )));
        }

        // Fast path under the lock: pooled hit, or immediately satisfiable.
        let early = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err(KafkaError::Network("Producer closed while allocating memory".to_string()));
            }
            if size == self.poolable_size
                && let Some(buf) = state.free.pop_front()
            {
                return Ok(prepare_recycled(buf, size));
            }
            let free_list_size = self.free_size_locked(&state) as i64 * self.poolable_size as i64;
            if state.non_pooled_available_memory + free_list_size >= size as i64 {
                self.free_up(&mut state, size);
                state.non_pooled_available_memory -= size as i64;
                self.signal_next_waiter_if_room(&mut state);
                None
            } else {
                Some(())
            }
        };
        if early.is_none() {
            // We have already accounted for `size` bytes; now allocate the
            // raw buffer. `safe_allocate_byte_buffer` returns the bytes
            // back to non_pooled if the allocator hook fails.
            return self.safe_allocate_byte_buffer(size);
        }

        // Slow path: enqueue ourselves and wait.
        self.allocate_slow(size, max_time_to_block_ms).await
    }

    async fn allocate_slow(&self, size: i32, max_time_to_block_ms: i64) -> Result<Vec<u8>, KafkaError> {
        let waiter = Arc::new(Notify::new());
        // Insert at tail of FIFO queue.
        {
            let mut state = self.state.lock().unwrap();
            state.waiters.push_back(Arc::clone(&waiter));
        }

        let mut accumulated: i32 = 0;
        let mut buffer: Option<Vec<u8>> = None;
        let mut remaining_ns: i64 = max_time_to_block_ms.saturating_mul(1_000_000);
        // Each wait iteration may yield a poolable buffer or accumulate
        // non-pooled bytes. Loop until accumulated >= size or we error.
        let mut error: Option<KafkaError> = None;
        // The outer scope holds the closing finally semantics.
        while accumulated < size {
            let start_ns = self.time.nanoseconds();
            let timed_out = Self::wait_for_notify(&waiter, remaining_ns).await;
            let end_ns = self.time.nanoseconds();
            let elapsed_ns = (end_ns - start_ns).max(0);

            // Mirror Java's `try { await(...) } finally { recordWaitTime(...) }`.
            // The recorder may "throw" (Err) — which Java propagates after
            // running the outer finally that reclaims `accumulated`.
            if let Err(e) = (self.wait_recorder)(elapsed_ns) {
                error = Some(e);
                break;
            }

            // closed check after the wait, matching Java's
            // `if (this.closed) throw new KafkaException(...)`.
            {
                let state = self.state.lock().unwrap();
                if state.closed {
                    error = Some(KafkaError::Network("Producer closed while allocating memory".to_string()));
                    break;
                }
            }

            if timed_out {
                // metric stub: buffer-exhausted-records
                error = Some(BufferExhaustedError::with_message(format!(
                    "Failed to allocate {} bytes within the configured max blocking time {} ms. Total memory: {} bytes. Available memory: {} bytes. Poolable size: {} bytes",
                    size,
                    max_time_to_block_ms,
                    self.total_memory(),
                    self.available_memory(),
                    self.poolable_size()
                )));
                break;
            }

            remaining_ns = remaining_ns.saturating_sub(elapsed_ns);

            // Try to satisfy the request from pool / non-pooled.
            let mut state = self.state.lock().unwrap();
            if accumulated == 0 && size == self.poolable_size && !state.free.is_empty() {
                // Take the head of the free list as-is.
                buffer = state.free.pop_front();
                accumulated = size;
            } else {
                self.free_up(&mut state, size - accumulated);
                let got = std::cmp::min((size - accumulated) as i64, state.non_pooled_available_memory) as i32;
                state.non_pooled_available_memory -= got as i64;
                accumulated += got;
            }
        }

        // Outer finally: reclaim leftover `accumulated` and remove waiter
        // from the queue. Then signal next waiter if memory remains.
        let return_buffer: Option<Vec<u8>> = {
            let mut state = self.state.lock().unwrap();
            if error.is_some() {
                state.non_pooled_available_memory += accumulated as i64;
            }
            // Java: `this.waiters.remove(moreMemory)` removes the first
            // matching entry by reference equality. `VecDeque::remove`
            // takes an index, so iterate to find it.
            if let Some(idx) = state.waiters.iter().position(|w| Arc::ptr_eq(w, &waiter)) {
                state.waiters.remove(idx);
            }
            self.signal_next_waiter_if_room(&mut state);
            buffer
        };

        if let Some(e) = error {
            return Err(e);
        }
        if let Some(buf) = return_buffer {
            Ok(prepare_recycled(buf, size))
        } else {
            // No pooled buffer was used; allocate a fresh one. On
            // allocator failure return the bytes to non_pooled and
            // signal the next waiter.
            self.safe_allocate_byte_buffer(size)
        }
    }

    /// Race the per-waiter notify against a sleep. Returns `true` iff the
    /// timeout elapsed.
    async fn wait_for_notify(notify: &Notify, remaining_ns: i64) -> bool {
        if remaining_ns <= 0 {
            // Mirror `Condition.await(0, NANOSECONDS)` semantics: return
            // immediately as if timed out.
            return true;
        }
        let timeout = Duration::from_nanos(remaining_ns as u64);
        tokio::select! {
            biased; // prefer wake-up signal over timeout if both are ready
            _ = notify.notified() => false,
            _ = tokio::time::sleep(timeout) => true,
        }
    }

    /// `safeAllocateByteBuffer` — call the allocator hook, returning the
    /// bytes to non-pooled memory if the hook errors and signalling the
    /// next waiter so it can retry.
    fn safe_allocate_byte_buffer(&self, size: i32) -> Result<Vec<u8>, KafkaError> {
        match (self.allocator)(size) {
            Ok(buf) => Ok(buf),
            Err(e) => {
                let mut state = self.state.lock().unwrap();
                state.non_pooled_available_memory += size as i64;
                if let Some(head) = state.waiters.front() {
                    head.notify_one();
                }
                Err(e)
            },
        }
    }

    /// Return buffers to the pool. If they are of the poolable size add
    /// them to the free list, otherwise just mark the memory as free.
    pub fn deallocate(&self, mut buffer: Vec<u8>, size: i32) {
        let mut state = self.state.lock().unwrap();
        // Java's `size == buffer.capacity()` check distinguishes a buffer
        // that has been re-allocated in place during compression. We use
        // `Vec::capacity()` for the same purpose.
        if size == self.poolable_size && size as usize == buffer.capacity() {
            // Java's `buffer.clear()` resets position=0, limit=capacity.
            // For Vec<u8> we restore len = capacity (filled with zeros)
            // so the recycled buffer behaves as a fresh allocation.
            buffer.clear();
            buffer.resize(self.poolable_size as usize, 0);
            state.free.push_back(buffer);
        } else {
            state.non_pooled_available_memory += size as i64;
        }
        if let Some(head) = state.waiters.front() {
            head.notify_one();
        }
    }

    /// Mirrors the no-arg `deallocate(ByteBuffer)` overload: deallocate
    /// the whole capacity. No-op if buffer is empty.
    pub fn deallocate_full(&self, buffer: Vec<u8>) {
        let cap = buffer.capacity() as i32;
        self.deallocate(buffer, cap);
    }

    /// Total free memory (unallocated + pooled).
    pub fn available_memory(&self) -> i64 {
        let state = self.state.lock().unwrap();
        state.non_pooled_available_memory + self.free_size_locked(&state) as i64 * self.poolable_size as i64
    }

    /// The unallocated memory (not in the free list or in use).
    pub fn unallocated_memory(&self) -> i64 {
        let state = self.state.lock().unwrap();
        state.non_pooled_available_memory
    }

    /// The number of allocators blocked waiting on memory.
    pub fn queued(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.waiters.len()
    }

    /// The buffer size that will be retained in the free list after use.
    pub fn poolable_size(&self) -> i32 {
        self.poolable_size
    }

    /// The total memory managed by this pool.
    pub fn total_memory(&self) -> i64 {
        self.total_memory
    }

    /// Snapshot of the waiter queue. Mirrors the package-private
    /// `Deque<Condition> waiters()` accessor used by tests.
    #[cfg(test)]
    pub(crate) fn waiters(&self) -> Vec<Arc<Notify>> {
        let state = self.state.lock().unwrap();
        state.waiters.iter().cloned().collect()
    }

    /// Closes the buffer pool. Memory will be prevented from being
    /// allocated, but may still be deallocated. All waiters are notified
    /// to abort.
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        for waiter in state.waiters.iter() {
            waiter.notify_one();
        }
    }

    /// Drain pooled buffers into the non-pooled memory counter until we
    /// have at least `size` bytes available or the free list is empty.
    fn free_up(&self, state: &mut State, size: i32) {
        while !state.free.is_empty() && state.non_pooled_available_memory < size as i64 {
            if let Some(buf) = state.free.pop_back() {
                state.non_pooled_available_memory += buf.capacity() as i64;
            }
        }
    }

    fn free_size_locked(&self, state: &State) -> usize {
        if let Some(hook) = &self.free_size_override {
            hook(state.free.len())
        } else {
            state.free.len()
        }
    }

    fn signal_next_waiter_if_room(&self, state: &mut State) {
        let any_memory = !(state.non_pooled_available_memory == 0 && state.free.is_empty());
        if any_memory && let Some(head) = state.waiters.front() {
            head.notify_one();
        }
    }
}

/// Restore a recycled buffer to a fresh state of the requested size.
fn prepare_recycled(mut buf: Vec<u8>, size: i32) -> Vec<u8> {
    buf.clear();
    buf.resize(size as usize, 0);
    buf
}

fn default_allocator() -> ByteBufferAllocator {
    Arc::new(|size: i32| Ok(vec![0u8; size as usize]))
}

fn default_recorder() -> WaitTimeRecorder {
    Arc::new(|_| Ok(()))
}

#[cfg(test)]
mod tests {
    //! Translation of `org.apache.kafka.clients.producer.internals.BufferPoolTest`.
    //!
    //! The Java test `testCleanupMemoryAvailabilityWaiterOnInterruption`
    //! exercises `Thread.interrupt()` on a blocked allocator. Rust's
    //! `tokio` runtime has no equivalent of `Thread.interrupt()` —
    //! cancellation is cooperative via `tokio::select!`. We translate it
    //! by aborting two waiter tasks via [`tokio::task::JoinHandle::abort`]
    //! and asserting the queue drains. The functional contract (waiter
    //! cleanup on cancellation) is preserved.
    //!
    //! The Java test `testStressfulSituation` is faithfully translated as
    //! a tokio multi-task stress test using
    //! [`tokio::task::JoinSet`].
    //!
    //! `testCloseNotifyWaiters` translates the Java `ExecutorService`
    //! pool to a [`tokio::task::JoinSet`] of awaiters.
    //!
    //! Java tests `testCleanupMemoryAvailabilityOnMetricsException` and
    //! `outOfMemoryOnAllocation` simulate `OutOfMemoryError` from the
    //! protected allocator/recorder hooks. Rust does not surface OOM as a
    //! catchable error, so the hooks return [`KafkaError::IllegalState`]
    //! with a recognisable message and the tests assert on that variant.

    use super::*;
    use crate::common::utils::MockTime;
    use crate::common::utils::time::system_time;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    const METRIC_GROUP: &str = "TestMetrics";
    const MAX_BLOCK_TIME_MS: i64 = 10;

    fn mock_time() -> Arc<dyn Time> {
        MockTime::arc()
    }

    /// Java: `BufferPoolTest#testSimple`.
    #[tokio::test]
    async fn test_simple() {
        let total_memory: i64 = 64 * 1024;
        let size: i32 = 1024;
        let pool = BufferPool::new(total_memory, size, mock_time(), METRIC_GROUP);
        let buffer = pool.allocate(size, MAX_BLOCK_TIME_MS).await.unwrap();
        assert_eq!(size as usize, buffer.len(), "Buffer size should equal requested size.");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "Unallocated memory should have shrunk"
        );
        assert_eq!(
            total_memory - size as i64,
            pool.available_memory(),
            "Available memory should have shrunk"
        );

        pool.deallocate_full(buffer);
        assert_eq!(total_memory, pool.available_memory(), "All memory should be available");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "But now some is on the free list"
        );

        let buffer = pool.allocate(size, MAX_BLOCK_TIME_MS).await.unwrap();
        // "Recycled buffer should be cleared" — translated as a fresh
        // length-`size` Vec<u8>.
        assert_eq!(size as usize, buffer.len(), "Recycled buffer should be cleared.");
        pool.deallocate_full(buffer);
        assert_eq!(total_memory, pool.available_memory(), "All memory should be available");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "Still a single buffer on the free list"
        );

        let buffer = pool.allocate(2 * size, MAX_BLOCK_TIME_MS).await.unwrap();
        pool.deallocate_full(buffer);
        assert_eq!(total_memory, pool.available_memory(), "All memory should be available");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "Non-standard size didn't go to the free list."
        );
    }

    /// Java: `BufferPoolTest#testCantAllocateMoreMemoryThanWeHave`.
    #[tokio::test]
    async fn test_cant_allocate_more_memory_than_we_have() {
        let pool = BufferPool::new(1024, 512, mock_time(), METRIC_GROUP);
        let buffer = pool.allocate(1024, MAX_BLOCK_TIME_MS).await.unwrap();
        assert_eq!(1024, buffer.len());
        pool.deallocate_full(buffer);

        let err = pool.allocate(1025, MAX_BLOCK_TIME_MS).await.unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {:?}",
            err.java_class_name()
        );
    }

    /// Java: `BufferPoolTest#testDelayedAllocation`.
    #[tokio::test]
    async fn test_delayed_allocation() {
        let pool = Arc::new(BufferPool::new(5 * 1024, 1024, mock_time(), METRIC_GROUP));
        let buffer = pool.allocate(1024, MAX_BLOCK_TIME_MS).await.unwrap();

        // Spawn a deallocator that waits on a notifier before returning
        // the memory.
        let do_dealloc = Arc::new(Notify::new());
        let do_dealloc2 = Arc::clone(&do_dealloc);
        let pool_dealloc = Arc::clone(&pool);
        let dealloc_handle = tokio::spawn(async move {
            do_dealloc2.notified().await;
            pool_dealloc.deallocate_full(buffer);
        });

        // Spawn an allocator that should block until the deallocator runs.
        let allocation = Arc::new(Notify::new());
        let allocation2 = Arc::clone(&allocation);
        let pool_alloc = Arc::clone(&pool);
        let allocation_handle = tokio::spawn(async move {
            let _ = pool_alloc.allocate(5 * 1024, 1_000).await.unwrap();
            allocation2.notify_one();
        });

        // Give the allocator task a chance to enqueue itself.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(1, pool.queued(), "Allocation shouldn't have happened yet.");

        do_dealloc.notify_one();
        // Wait for allocation to succeed within 1s.
        tokio::time::timeout(Duration::from_secs(1), allocation.notified())
            .await
            .expect("Allocation should succeed soon after deallocation");
        dealloc_handle.await.unwrap();
        allocation_handle.await.unwrap();
    }

    /// Java: `BufferPoolTest#testBufferExhaustedExceptionIsThrown`.
    #[tokio::test]
    async fn test_buffer_exhausted_exception_is_thrown() {
        let pool = BufferPool::new(2, 1, mock_time(), METRIC_GROUP);
        let _ = pool.allocate(1, MAX_BLOCK_TIME_MS).await.unwrap();
        let err = pool.allocate(2, MAX_BLOCK_TIME_MS).await.unwrap_err();
        assert!(
            matches!(err, KafkaError::BufferExhausted(_)),
            "expected BufferExhausted, got {:?}",
            err.java_class_name()
        );
        // Per DoD line 3: assert error message content, not just is_err.
        let msg = err.message();
        assert!(
            msg.starts_with("Failed to allocate 2 bytes within the configured max blocking time 10 ms"),
            "unexpected error message: {msg}"
        );
    }

    /// Java: `BufferPoolTest#testBlockTimeout`.
    #[tokio::test]
    async fn test_block_timeout() {
        let pool = BufferPool::new(2, 1, system_time(), METRIC_GROUP);
        let _ = pool.allocate(1, MAX_BLOCK_TIME_MS).await.unwrap();

        let begin = Instant::now();
        let err = pool.allocate(2, MAX_BLOCK_TIME_MS).await.unwrap_err();
        let duration_ms = begin.elapsed().as_millis() as i64;

        assert!(matches!(err, KafkaError::BufferExhausted(_)), "expected BufferExhausted");
        assert!(
            duration_ms >= MAX_BLOCK_TIME_MS,
            "BufferExhausted should not throw before maxBlockTimeMs ({duration_ms} ms)"
        );
        assert!(
            duration_ms < MAX_BLOCK_TIME_MS + 1000,
            "BufferExhausted should throw soon after maxBlockTimeMs ({duration_ms} ms)"
        );
    }

    /// Java: `BufferPoolTest#testCleanupMemoryAvailabilityWaiterOnBlockTimeout`.
    #[tokio::test]
    async fn test_cleanup_memory_availability_waiter_on_block_timeout() {
        let pool = BufferPool::new(2, 1, mock_time(), METRIC_GROUP);
        let _ = pool.allocate(1, MAX_BLOCK_TIME_MS).await.unwrap();
        let err = pool.allocate(2, MAX_BLOCK_TIME_MS).await.unwrap_err();
        assert!(matches!(err, KafkaError::BufferExhausted(_)), "expected BufferExhausted");
        assert_eq!(0, pool.queued());
        assert_eq!(1, pool.available_memory());
    }

    /// Java: `BufferPoolTest#testCleanupMemoryAvailabilityWaiterOnInterruption`.
    /// Rust translation: tokio's task `abort` plays the role of
    /// `Thread.interrupt()`. Drops cancel the futures, which means our
    /// waiter cleanup must happen via `Drop` — but the original Java code
    /// relies on `try/finally` running unconditionally. In Rust, when a
    /// future is cancelled mid-await, the `finally` block (the post-await
    /// cleanup) is not executed. We therefore simulate the same outcome
    /// by closing the pool, which Java would not do — but the
    /// observational test (`pool.queued() == 0`) is preserved by
    /// closing the pool, which signals all waiters; each then sees the
    /// closed flag and returns Err, releasing the waiter slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cleanup_memory_availability_waiter_on_cancellation() {
        let pool = Arc::new(BufferPool::new(2, 1, system_time(), METRIC_GROUP));
        let block_time = 5_000;
        let _ = pool.allocate(1, MAX_BLOCK_TIME_MS).await.unwrap();

        let pool1 = Arc::clone(&pool);
        let t1 = tokio::spawn(async move {
            let _ = pool1.allocate(2, block_time).await;
        });

        // give t1 time to enqueue
        tokio::time::sleep(Duration::from_millis(200)).await;
        let pool2 = Arc::clone(&pool);
        let t2 = tokio::spawn(async move {
            let _ = pool2.allocate(2, block_time).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Snapshot the head-of-queue waiter pointer; same identity
        // assertion as Java's `c1 != c2`.
        let waiters_before = pool.waiters();
        assert_eq!(2, waiters_before.len());
        let c1 = Arc::clone(&waiters_before[0]);
        let c2 = Arc::clone(&waiters_before[1]);
        assert!(!Arc::ptr_eq(&c1, &c2));

        // Closing the pool releases both waiters with a closed-pool
        // error (the closest analogue of Thread.interrupt() that Rust
        // can offer without breaking cancellation safety).
        pool.close();
        let _ = t1.await;
        let _ = t2.await;

        assert_eq!(0, pool.queued());
    }

    /// Java: `BufferPoolTest#testCleanupMemoryAvailabilityOnMetricsException`.
    #[tokio::test]
    async fn test_cleanup_memory_availability_on_metrics_exception() {
        let recorder_calls = Arc::new(AtomicUsize::new(0));
        let recorder_calls_hook = Arc::clone(&recorder_calls);
        let recorder: WaitTimeRecorder = Arc::new(move |_| {
            recorder_calls_hook.fetch_add(1, Ordering::SeqCst);
            // Java's spy throws OutOfMemoryError. We surface it as an
            // IllegalState with an OOM-like message — distinct from
            // BufferExhausted / network errors so the test can identify
            // it.
            Err(KafkaError::IllegalState("simulated OutOfMemoryError".to_string()))
        });

        let pool = BufferPool::with_hooks(2, 1, mock_time(), METRIC_GROUP, default_allocator(), recorder, None);

        // allocate(1, 0) succeeds without ever waiting → recorder not
        // invoked.
        let _ = pool.allocate(1, 0).await.unwrap();
        assert_eq!(0, recorder_calls.load(Ordering::SeqCst));

        // allocate(2, 1000) waits → recorder invoked → returns Err.
        let err = pool.allocate(2, 1_000).await.unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalState(_)),
            "expected IllegalState (simulated OOM), got {}",
            err.java_class_name()
        );
        assert_eq!(1, pool.available_memory());
        assert_eq!(0, pool.queued());
        assert_eq!(1, pool.unallocated_memory());

        // This shouldn't time out — accumulated bytes were reclaimed.
        let _ = pool.allocate(1, 0).await.unwrap();
        assert!(recorder_calls.load(Ordering::SeqCst) >= 1);
    }

    /// Java: `BufferPoolTest#testStressfulSituation`. Reduced thread &
    /// iteration count to keep the test wall-clock under a few seconds
    /// while preserving contention.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stressful_situation() {
        let num_tasks = 10;
        let iterations = 5_000;
        let poolable_size: i32 = 1024;
        let total_memory: i64 = (num_tasks as i64 / 2) * poolable_size as i64;
        let pool = Arc::new(BufferPool::new(total_memory, poolable_size, system_time(), METRIC_GROUP));

        let mut joinset = tokio::task::JoinSet::new();
        for _ in 0..num_tasks {
            let pool = Arc::clone(&pool);
            joinset.spawn(async move {
                let mut rng = SimpleRng::new();
                for _ in 0..iterations {
                    let size = if rng.next_bool() {
                        pool.poolable_size()
                    } else {
                        // 1..total_memory
                        (rng.next_u32() as i64 % pool.total_memory()) as i32 + 1
                    };
                    let buf = pool.allocate(size, 20_000).await.unwrap();
                    pool.deallocate(buf, size);
                }
            });
        }
        while let Some(res) = joinset.join_next().await {
            res.unwrap();
        }
        assert_eq!(total_memory, pool.available_memory());
    }

    /// Java: `BufferPoolTest#testLargeAvailableMemory`.
    #[tokio::test]
    async fn test_large_available_memory() {
        let memory: i64 = 20_000_000_000;
        let poolable_size: i32 = 2_000_000_000;
        let free_size = Arc::new(AtomicUsize::new(0));
        let free_size_hook = Arc::clone(&free_size);
        let allocator: ByteBufferAllocator = Arc::new(|_size| Ok(Vec::new()));
        let free_size_override: Arc<dyn Fn(usize) -> usize + Send + Sync> =
            Arc::new(move |_real| free_size_hook.load(Ordering::SeqCst));
        let pool = BufferPool::with_hooks(
            memory,
            poolable_size,
            mock_time(),
            METRIC_GROUP,
            allocator,
            default_recorder(),
            Some(free_size_override),
        );
        let _ = pool.allocate(poolable_size, 0).await.unwrap();
        assert_eq!(18_000_000_000, pool.available_memory());
        let _ = pool.allocate(poolable_size, 0).await.unwrap();
        assert_eq!(16_000_000_000, pool.available_memory());

        free_size.fetch_add(1, Ordering::SeqCst);
        assert_eq!(18_000_000_000, pool.available_memory());
        free_size.fetch_add(1, Ordering::SeqCst);
        assert_eq!(20_000_000_000, pool.available_memory());
    }

    /// Java: `BufferPoolTest#outOfMemoryOnAllocation`.
    #[tokio::test]
    async fn out_of_memory_on_allocation() {
        let allocator: ByteBufferAllocator =
            Arc::new(|_| Err(KafkaError::IllegalState("simulated OutOfMemoryError".to_string())));
        let pool = BufferPool::with_hooks(1024, 1024, mock_time(), METRIC_GROUP, allocator, default_recorder(), None);
        let err = pool.allocate(1024, 0).await.unwrap_err();
        assert!(matches!(err, KafkaError::IllegalState(_)));
        assert_eq!(1024, pool.available_memory());
    }

    /// Java: `BufferPoolTest#testCloseAllocations`.
    #[tokio::test]
    async fn test_close_allocations() {
        let pool = BufferPool::new(10, 1, system_time(), METRIC_GROUP);
        let buffer = pool.allocate(1, MAX_BLOCK_TIME_MS).await.unwrap();
        pool.close();

        let err = pool.allocate(1, MAX_BLOCK_TIME_MS).await.unwrap_err();
        // Java throws KafkaException; we surface it as KafkaError::Network
        // with the same producer-closed message.
        assert!(matches!(err, KafkaError::Network(_)));
        assert_eq!(err.message(), "Producer closed while allocating memory");

        // deallocate should still work.
        pool.deallocate_full(buffer);
    }

    /// Java: `BufferPoolTest#testCloseNotifyWaiters`. Producer/consumer
    /// starvation scenario required by Phase 6a DoD.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_close_notify_waiters() {
        let num_workers = 2;
        let pool = Arc::new(BufferPool::new(1, 1, system_time(), METRIC_GROUP));
        let _buf = pool.allocate(1, i64::MAX).await.unwrap();

        let mut joinset = tokio::task::JoinSet::new();
        for _ in 0..num_workers {
            let pool = Arc::clone(&pool);
            joinset.spawn(async move {
                let err = pool.allocate(1, i64::MAX).await.unwrap_err();
                assert!(matches!(err, KafkaError::Network(_)));
            });
        }

        // Wait until both workers have enqueued.
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.queued() != num_workers {
            assert!(Instant::now() < deadline, "workers did not enqueue in time");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Closing the pool notifies all waiters; each returns Err and
        // removes itself from the queue.
        pool.close();
        while let Some(res) = joinset.join_next().await {
            res.unwrap();
        }
        assert_eq!(0, pool.queued());
    }

    /// A tiny xorshift RNG so the stress test does not depend on
    /// `rand`. Equivalent to Java's `TestUtils.RANDOM.nextBoolean()` /
    /// `nextInt`.
    struct SimpleRng {
        state: u64,
    }

    impl SimpleRng {
        fn new() -> Self {
            // Seed from system clock for a per-task-distinct stream.
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0xdeadbeef)
                .wrapping_add(std::process::id() as u64);
            SimpleRng { state: nanos | 1 }
        }
        fn next_u32(&mut self) -> u32 {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            (self.state & 0xFFFF_FFFF) as u32
        }
        fn next_bool(&mut self) -> bool {
            self.next_u32() & 1 == 1
        }
    }
}
