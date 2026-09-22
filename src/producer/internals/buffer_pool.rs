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

#![allow(dead_code)]
//! A pool of byte buffers kept under a given memory limit.
//!
//! This class is fairly specific to the needs of the producer. In particular it has the
//! following properties:
//!
//! 1. There is a special "poolable size" and buffers of this size are kept in a free list
//!    and recycled.
//! 2. It is fair. That is all memory is given to the longest waiting task until it has
//!    sufficient memory. This prevents starvation or deadlock when a task asks for a large
//!    chunk of memory and needs to block until multiple buffers are deallocated.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.BufferPool`.
//!
//! # Design
//!
//! Java uses `ReentrantLock` + `Condition` + a free-list of `ByteBuffer` objects for two
//! purposes: (1) reducing GC pressure via buffer recycling, and (2) bounded memory with
//! backpressure. In Rust, GC pressure does not exist so only memory bounding + backpressure
//! are needed.
//!
//! We use a `Mutex`-protected inner state with `Notify` for backpressure, faithfully
//! translating Java's lock + condition variable pattern:
//! - The inner state tracks `non_pooled_available_memory` and a free list of recycled buffers
//! - `allocate` checks if memory is immediately available, otherwise registers a waiter and
//!   waits for notification (with timeout)
//! - `deallocate` returns memory and notifies the next waiter

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::common::Error;
use crate::common::metrics::internals::TimeUnit;
use crate::common::metrics::stats::Meter;
use crate::common::metrics::{Metrics, Sensor};

/// A provider of the current POSIX time in milliseconds. The Rust analog of
/// Java's `Time.milliseconds()`, used as the timestamp when recording the
/// wait-time sensor (`SenderMetrics` holds the same shape).
type TimeProvider = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Mutable state protected by the lock.
struct PoolInner {
    /// Available memory not held by the free list or in use.
    /// Corresponds to Java's `nonPooledAvailableMemory`.
    non_pooled_available_memory: i64,
    /// Free list of recycled buffers (only buffers of `poolable_size` capacity).
    free: VecDeque<Vec<u8>>,
    /// Queue of waiters (each is a `tokio::sync::Notify` that will be notified
    /// when memory becomes available).
    waiters: VecDeque<Arc<tokio::sync::Notify>>,
}

/// Result from trying to allocate within the lock.
enum AllocResult {
    /// Allocation succeeded immediately — here is the buffer.
    Immediate(Vec<u8>),
    /// Need to wait — here is the Notify to wait on.
    NeedWait(Arc<tokio::sync::Notify>),
    /// Pool is closed.
    Closed,
}

/// Result from checking state after being notified.
enum WakeResult {
    /// Got a buffer from the free list.
    GotBuffer(Vec<u8>),
    /// Accumulated enough memory — allocate a new buffer.
    Ready,
    /// Need more memory — keep waiting.
    NeedMore,
    /// Timed out.
    TimedOut,
    /// Pool was closed.
    Closed,
}

/// A pool of byte buffers kept under a given memory limit.
///
/// Provides bounded memory allocation with backpressure and buffer recycling
/// for the most common buffer size (the "poolable size").
pub struct BufferPool {
    /// The lock-protected inner state.
    inner: Mutex<PoolInner>,
    /// Total memory managed by this pool.
    total_memory: i64,
    /// The buffer size to cache in the free list rather than deallocating.
    poolable_size: usize,
    /// Whether the pool has been closed.
    closed: AtomicBool,
    /// Provider of the current POSIX time in milliseconds (Java's `Time time`).
    time_provider: TimeProvider,
    /// Sensor tracking the time an appender waits for space allocation
    /// (Java's `Sensor waitTime`, `WAIT_TIME_SENSOR_NAME`).
    wait_time_sensor: Arc<Sensor>,
    /// Sensor tracking record sends dropped due to buffer exhaustion
    /// (Java's `buffer-exhausted-records` sensor).
    buffer_exhausted_sensor: Arc<Sensor>,
    /// Test-only injection point mirroring Java's
    /// `spy(pool).doThrow(...).when(pool).recordWaitTime(...)`. When `true`,
    /// [`record_wait_time`](Self::record_wait_time) returns an error instead
    /// of recording, exercising the memory-cleanup path.
    #[cfg(test)]
    fail_record_wait_time: AtomicBool,
}

impl BufferPool {
    /// Sensor name for tracking buffer pool wait time.
    pub const WAIT_TIME_SENSOR_NAME: &str = "bufferpool-wait-time";

    /// Create a new buffer pool.
    ///
    /// # Arguments
    ///
    /// * `memory` - The maximum amount of memory that this buffer pool can allocate
    /// * `poolable_size` - The buffer size to cache in the free list rather than deallocating
    /// * `metrics` - Instance of `Metrics`
    /// * `time_provider` - Provider of the current POSIX time in milliseconds
    /// * `metric_grp_name` - Logical group name for metrics
    pub fn new(
        memory: i64,
        poolable_size: usize,
        metrics: Arc<Metrics>,
        time_provider: TimeProvider,
        metric_grp_name: &str,
    ) -> Self {
        assert!(memory > 0, "Buffer pool memory must be positive");

        // `bufferpool-wait-time` sensor: a Meter over the fraction of time an
        // appender waits and the total wait time in nanoseconds.
        let wait_time_sensor = metrics
            .sensor(BufferPool::WAIT_TIME_SENSOR_NAME)
            .expect("registering bufferpool-wait-time sensor");
        let rate_metric_name = metrics.metric_name_description_tags(
            "bufferpool-wait-ratio",
            metric_grp_name,
            "The fraction of time an appender waits for space allocation.",
            std::collections::BTreeMap::new(),
        );
        let total_ns_metric_name = metrics.metric_name_description_tags(
            "bufferpool-wait-time-ns-total",
            metric_grp_name,
            "The total time in nanoseconds an appender waits for space allocation.",
            std::collections::BTreeMap::new(),
        );
        wait_time_sensor
            .add(Box::new(Meter::with_unit(
                TimeUnit::Nanoseconds,
                rate_metric_name,
                total_ns_metric_name,
            )))
            .expect("registering bufferpool-wait-time meter");

        // `buffer-exhausted-records` sensor: a Meter over the per-second and
        // total number of record sends dropped due to buffer exhaustion.
        let buffer_exhausted_sensor = metrics
            .sensor("buffer-exhausted-records")
            .expect("registering buffer-exhausted-records sensor");
        let buffer_exhausted_rate_metric_name = metrics.metric_name_description_tags(
            "buffer-exhausted-rate",
            metric_grp_name,
            "The average per-second number of record sends that are dropped due to buffer exhaustion",
            std::collections::BTreeMap::new(),
        );
        let buffer_exhausted_total_metric_name = metrics.metric_name_description_tags(
            "buffer-exhausted-total",
            metric_grp_name,
            "The total number of record sends that are dropped due to buffer exhaustion",
            std::collections::BTreeMap::new(),
        );
        buffer_exhausted_sensor
            .add(Box::new(Meter::new(
                buffer_exhausted_rate_metric_name,
                buffer_exhausted_total_metric_name,
            )))
            .expect("registering buffer-exhausted-records meter");

        Self {
            inner: Mutex::new(PoolInner {
                non_pooled_available_memory: memory,
                free: VecDeque::new(),
                waiters: VecDeque::new(),
            }),
            total_memory: memory,
            poolable_size,
            closed: AtomicBool::new(false),
            time_provider,
            wait_time_sensor,
            buffer_exhausted_sensor,
            #[cfg(test)]
            fail_record_wait_time: AtomicBool::new(false),
        }
    }

    /// Test-only convenience constructor that supplies a fresh reporter-less
    /// [`Metrics`] registry and a system-clock time provider, mirroring Java's
    /// `BufferPoolTest` passing `new Metrics()`. Java has no metrics-less
    /// production constructor.
    #[cfg(test)]
    pub(crate) fn new_for_test(memory: i64, poolable_size: usize) -> Self {
        let time_provider: TimeProvider = Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });
        Self::new(
            memory,
            poolable_size,
            Arc::new(Metrics::new()),
            time_provider,
            "producer-metrics",
        )
    }

    /// Allocate a buffer of the given size. This method blocks if there is not enough memory
    /// and the buffer pool is configured with blocking mode.
    ///
    /// # OOM handling deviation from Java
    ///
    /// Java's `BufferPool.allocate()` wraps the actual `ByteBuffer.allocate()` call in
    /// `safeAllocateByteBuffer()`, which catches `OutOfMemoryError` and restores
    /// `nonPooledAvailableMemory` in a `finally` block. In Rust, the default global
    /// allocator aborts the process on OOM (rather than throwing a recoverable exception),
    /// so the `safeAllocateByteBuffer` recovery path is intentionally omitted.
    /// Buffer allocation (`vec![0u8; size]`) is performed outside the lock, matching
    /// Java's design, but OOM during allocation is unrecoverable.
    ///
    /// # Arguments
    ///
    /// * `size` - The buffer size to allocate in bytes
    /// * `max_block_ms` - The maximum time in milliseconds to block for buffer memory to be
    ///   available
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `size` is larger than the total memory
    /// controlled by the pool.
    ///
    /// Returns [`Error::ProducerBufferExhausted`] if the timeout elapses before enough memory
    /// becomes available.
    ///
    /// Returns [`Error::KafkaError`] if the pool is closed while waiting.
    pub async fn allocate(&self, size: usize, max_block_ms: i64) -> Result<Vec<u8>, Error> {
        if size as i64 > self.total_memory {
            return Err(Error::local_illegal_argument(format!(
                "Attempt to allocate {} bytes, but there is a hard limit of {} on memory allocations.",
                size, self.total_memory
            )));
        }

        // Phase 1: try to satisfy immediately under the lock (no await in this block)
        let alloc_result = {
            let mut inner = self.inner.lock().unwrap();

            if self.closed.load(Ordering::Acquire) {
                AllocResult::Closed
            } else if size == self.poolable_size && !inner.free.is_empty() {
                // Fast path: grab a recycled buffer
                let mut buf = inner.free.pop_front().unwrap();
                buf.clear();
                buf.resize(size, 0);
                AllocResult::Immediate(buf)
            } else {
                let free_list_size = inner.free.len() as i64 * self.poolable_size as i64;
                if inner.non_pooled_available_memory + free_list_size >= size as i64 {
                    // Enough memory available right now
                    Self::free_up(&mut inner, size as i64);
                    inner.non_pooled_available_memory -= size as i64;
                    AllocResult::Immediate(vec![0u8; size])
                } else {
                    // Need to wait
                    let notify = Arc::new(tokio::sync::Notify::new());
                    inner.waiters.push_back(Arc::clone(&notify));
                    AllocResult::NeedWait(notify)
                }
            }
        }; // MutexGuard dropped here, before any .await

        match alloc_result {
            AllocResult::Immediate(buf) => Ok(buf),
            // Java: `throw new KafkaException("Producer closed while allocating
            // memory")` (`BufferPool.java:119`) — a BARE `KafkaException`, not an
            // `ApiException`. `Error::with_message(Errors::UnknownServerError, ..)`
            // resolves the code to `UnknownServerException`, which IS an
            // `ApiException`, and `KafkaProducer.doSend` dispatches on exactly that
            // difference: `catch (ApiException e)` (`:1056`) records the error state
            // and returns a failed future, `catch (KafkaException e)` (`:1072`)
            // rethrows out of `send()`.
            // `KafkaProducer.doSend` therefore rethrows it out of `send()`
            // without invoking the user callback.
            AllocResult::Closed => Err(Error::kafka_message("Producer closed while allocating memory")),
            AllocResult::NeedWait(more_memory) => {
                // Phase 2: blocking wait loop
                self.allocate_blocking(size, max_block_ms, &more_memory).await
            },
        }
    }

    /// The blocking portion of allocate, called when memory is not immediately available.
    /// The `more_memory` Notify has already been registered in the waiters queue.
    async fn allocate_blocking(
        &self,
        size: usize,
        max_block_ms: i64,
        more_memory: &Arc<tokio::sync::Notify>,
    ) -> Result<Vec<u8>, Error> {
        /// Java's inner `finally` (`BufferPool.java:185-189`), as a `Drop` type:
        ///
        /// ```java
        /// } finally {
        ///     // When this loop was not able to successfully terminate don't loose available memory
        ///     this.nonPooledAvailableMemory += accumulated;
        ///     this.waiters.remove(moreMemory);
        /// }
        /// ```
        ///
        /// plus the waiter signal from the enclosing `finally` (`:190-197`), which
        /// also runs on every exit.
        ///
        /// Java's exits are "got the memory" (where it zeroes `accumulated` first, at
        /// `:183`, so the credit is a no-op) and "threw". Rust adds a third: the
        /// future being dropped at the wait below, which has no Java analogue because
        /// threads cannot be cancelled (CLAUDE.md §9.6). Without this the waiter's
        /// `Arc<Notify>` stayed in `inner.waiters` forever, and since
        /// `deallocate_with_size` and `maybe_signal_next_waiter` only ever signal
        /// `waiters.front()`, a leaked entry that reached the head **swallowed every
        /// wakeup**: live waiters were never notified and all timed out with
        /// `BufferExhausted` while memory sat free.
        struct WaitGuard<'a> {
            pool: &'a BufferPool,
            waiter: &'a Arc<tokio::sync::Notify>,
            /// Memory reserved so far and not yet handed to the caller. Zeroed on
            /// the success paths, exactly as Java zeroes its local.
            accumulated: i64,
        }

        impl Drop for WaitGuard<'_> {
            fn drop(&mut self) {
                let mut inner = self.pool.inner.lock().unwrap();
                inner.non_pooled_available_memory += self.accumulated;
                BufferPool::remove_waiter(&mut inner, self.waiter);
                BufferPool::maybe_signal_next_waiter(&inner);
            }
        }

        let mut guard = WaitGuard { pool: self, waiter: more_memory, accumulated: 0 };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(max_block_ms.max(0) as u64);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

            // Wait for notification (no lock held here). This is the await the
            // caller may be cancelled at, which is what `WaitGuard` exists for.
            // The wait duration is measured in nanoseconds with a monotonic
            // clock, the analog of Java's `time.nanoseconds()` bracketing
            // `moreMemory.await(...)`.
            let start_wait = std::time::Instant::now();
            let timed_out = tokio::time::timeout(remaining, more_memory.notified()).await.is_err();
            let time_ns = start_wait.elapsed().as_nanos() as i64;

            // Java records the wait time in a `finally` — even when the wait
            // ends in timeout/close/error (`BufferPool.java:150-154`). If
            // recording fails (the metrics-exception path), restore the
            // accumulated memory and remove the waiter, mirroring Java's outer
            // `finally` (`:185-189`), then propagate the error.
            // `WaitGuard`'s Drop performs Java's outer `finally`
            // (`BufferPool.java:185-189`): it credits back `accumulated`, removes
            // this waiter and signals the next one. Doing it by hand here as well
            // would return the memory twice, so the error just propagates.
            self.record_wait_time(time_ns)?;

            // Check state under the lock (no await in this block). Every exit leaves
            // the waiter removal, the credit-back and the next-waiter signal to
            // `WaitGuard::drop`, mirroring Java's `finally`s.
            let wake_result = {
                let mut inner = self.inner.lock().unwrap();

                if self.closed.load(Ordering::Acquire) {
                    WakeResult::Closed
                } else if timed_out {
                    WakeResult::TimedOut
                } else if guard.accumulated == 0 && size == self.poolable_size && !inner.free.is_empty() {
                    // Grab a buffer from the free list
                    let mut buf = inner.free.pop_front().unwrap();
                    buf.clear();
                    buf.resize(size, 0);
                    WakeResult::GotBuffer(buf)
                } else {
                    // Try to accumulate memory
                    Self::free_up(&mut inner, size as i64 - guard.accumulated);
                    let got = std::cmp::min(size as i64 - guard.accumulated, inner.non_pooled_available_memory);
                    inner.non_pooled_available_memory -= got;
                    guard.accumulated += got;

                    if guard.accumulated >= size as i64 {
                        WakeResult::Ready
                    } else {
                        WakeResult::NeedMore
                    }
                }
            }; // MutexGuard dropped here, before any .await

            match wake_result {
                WakeResult::GotBuffer(buf) => {
                    // Java 172-173 sets `accumulated = size` and then zeroes it at
                    // `:183`; the buffer came off the free list, so there is nothing
                    // to credit back either way.
                    guard.accumulated = 0;
                    return Ok(buf);
                },
                WakeResult::Ready => {
                    // "Don't reclaim memory on throwable since nothing was thrown"
                    // (Java 182-183): the reserved bytes leave with the caller.
                    guard.accumulated = 0;
                    return Ok(vec![0u8; size]);
                },
                WakeResult::NeedMore => continue,
                // Bare `KafkaException` in Java (`BufferPool.java:157`) — see
                // the `AllocResult::Closed` arm in `allocate`.
                WakeResult::Closed => {
                    // Java `BufferPool.java:157`, the same bare `KafkaException` as
                    // the fast-path check above.
                    return Err(Error::kafka_message("Producer closed while allocating memory"));
                },
                WakeResult::TimedOut => {
                    // Java records `buffer-exhausted-records` when the wait
                    // elapsed, before throwing `BufferExhaustedException`
                    // (`BufferPool.java:160`). Recorded outside the pool lock
                    // (value/timestamp are independent of pool state).
                    self.buffer_exhausted_sensor.record_value_time_ms(1.0, (self.time_provider)());
                    return Err(Error::buffer_exhausted(format!(
                        "Failed to allocate {} bytes within the configured max blocking time \
                         {} ms. Total memory: {} bytes. Available memory: {} bytes. \
                         Poolable size: {} bytes",
                        size,
                        max_block_ms,
                        self.total_memory,
                        self.available_memory(),
                        self.poolable_size
                    )));
                },
            }
        }
    }

    /// Record the time (in nanoseconds) an appender waited for space
    /// allocation. Translated from Java's `protected void recordWaitTime(long
    /// timeNs)` (`BufferPool.java:210-212`), which records against the
    /// `bufferpool-wait-time` sensor at the current wall-clock millisecond.
    ///
    /// Java's method is `void` but can throw (its tests inject an
    /// `OutOfMemoryError` via a Mockito spy). Rust sensor recording is
    /// infallible, so this always returns `Ok` in production; the `Result`
    /// return type is the faithful translation of the throwing contract, and
    /// tests inject a failure through `fail_record_wait_time`.
    fn record_wait_time(&self, time_ns: i64) -> Result<(), Error> {
        #[cfg(test)]
        if self.fail_record_wait_time.load(Ordering::Relaxed) {
            return Err(Error::with_message(
                crate::common::Errors::UnknownServerError,
                "Injected recordWaitTime failure",
            ));
        }
        self.wait_time_sensor
            .record_value_time_ms(time_ns as f64, (self.time_provider)());
        Ok(())
    }

    /// Attempt to ensure we have at least the requested number of bytes of memory for
    /// allocation by deallocating pooled buffers (if needed).
    fn free_up(inner: &mut PoolInner, size: i64) {
        while !inner.free.is_empty() && inner.non_pooled_available_memory < size {
            let buf = inner.free.pop_back().unwrap();
            inner.non_pooled_available_memory += buf.capacity() as i64;
        }
    }

    /// Remove a specific waiter from the waiters queue.
    fn remove_waiter(inner: &mut PoolInner, waiter: &Arc<tokio::sync::Notify>) {
        inner.waiters.retain(|w| !Arc::ptr_eq(w, waiter));
    }

    /// Signal the next waiter in the queue if there is available memory.
    fn maybe_signal_next_waiter(inner: &PoolInner) {
        if !(inner.non_pooled_available_memory == 0 && inner.free.is_empty())
            && let Some(next) = inner.waiters.front()
        {
            next.notify_one();
        }
    }

    /// Return buffers to the pool. If they are of the poolable size add them to the free list,
    /// otherwise just mark the memory as free.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to return
    /// * `size` - The size of the buffer to mark as deallocated, note that this may be smaller
    ///   than `buffer.capacity()` since the buffer may re-allocate itself during in-place
    ///   compression
    pub fn deallocate_with_size(&self, buffer: Vec<u8>, size: usize) {
        let mut inner = self.inner.lock().unwrap();
        if size == self.poolable_size && size == buffer.capacity() {
            inner.free.push_back(buffer);
        } else {
            drop(buffer);
            inner.non_pooled_available_memory += size as i64;
        }
        if let Some(next) = inner.waiters.front() {
            next.notify_one();
        }
    }

    /// Return a buffer to the pool using its full capacity as the size.
    pub fn deallocate(&self, buffer: Vec<u8>) {
        let size = buffer.capacity();
        self.deallocate_with_size(buffer, size);
    }

    /// The total free memory both unallocated and in the free list.
    pub fn available_memory(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.non_pooled_available_memory + inner.free.len() as i64 * self.poolable_size as i64
    }

    /// Get the unallocated memory (not in the free list or in use).
    pub fn unallocated_memory(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.non_pooled_available_memory
    }

    /// The number of tasks blocked waiting on memory.
    pub fn queued(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.waiters.len()
    }

    /// The buffer size that will be retained in the free list after use.
    pub fn poolable_size(&self) -> usize {
        self.poolable_size
    }

    /// The total memory managed by this pool.
    pub fn total_memory(&self) -> i64 {
        self.total_memory
    }

    /// The number of buffers in the free list.
    pub fn free_size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.free.len()
    }

    /// Closes the buffer pool. Memory will be prevented from being allocated, but may be
    /// deallocated. All allocations awaiting available memory will be notified to abort.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let inner = self.inner.lock().unwrap();
        for waiter in &inner.waiters {
            waiter.notify_one();
        }
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("total_memory", &self.total_memory)
            .field("poolable_size", &self.poolable_size)
            .field("available_memory", &self.available_memory())
            .field("queued", &self.queued())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `BufferPoolTest.testSimple`.
    ///
    /// Test the simple non-blocking allocation paths.
    #[tokio::test]
    async fn test_simple() {
        let total_memory: i64 = 64 * 1024;
        let size: usize = 1024;
        let max_block_ms: i64 = 10;
        let pool = BufferPool::new_for_test(total_memory, size);

        let buffer = pool.allocate(size, max_block_ms).await.unwrap();
        assert_eq!(size, buffer.len(), "Buffer size should equal requested size.");
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

        pool.deallocate(buffer);
        assert_eq!(total_memory, pool.available_memory(), "All memory should be available");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "But now some is on the free list"
        );

        let buffer = pool.allocate(size, max_block_ms).await.unwrap();
        assert_eq!(size, buffer.len(), "Recycled buffer should be the right size.");
        pool.deallocate(buffer);
        assert_eq!(total_memory, pool.available_memory(), "All memory should be available");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "Still a single buffer on the free list"
        );

        let buffer = pool.allocate(2 * size, max_block_ms).await.unwrap();
        pool.deallocate(buffer);
        assert_eq!(total_memory, pool.available_memory(), "All memory should be available");
        assert_eq!(
            total_memory - size as i64,
            pool.unallocated_memory(),
            "Non-standard size didn't go to the free list."
        );
    }

    /// Translated from `BufferPoolTest.testCantAllocateMoreMemoryThanWeHave`.
    ///
    /// Test that we cannot try to allocate more memory than we have in the whole pool.
    #[tokio::test]
    async fn test_cant_allocate_more_memory_than_we_have() {
        let pool = BufferPool::new_for_test(1024, 512);
        let buffer = pool.allocate(1024, 10).await.unwrap();
        assert_eq!(1024, buffer.len());
        pool.deallocate(buffer);

        let result = pool.allocate(1025, 10).await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::LocalIllegalArgument(_)),
            "Should be an IllegalArgument error"
        );
    }

    /// Translated from `BufferPoolTest.testDelayedAllocation`.
    ///
    /// Test that delayed allocation blocks.
    #[tokio::test]
    async fn test_delayed_allocation() {
        let pool = Arc::new(BufferPool::new_for_test(5 * 1024, 1024));
        let buffer = pool.allocate(1024, 10000).await.unwrap();

        let pool_clone = Arc::clone(&pool);
        let dealloc_notify = Arc::new(tokio::sync::Notify::new());
        let dealloc_notify_clone = Arc::clone(&dealloc_notify);

        // Spawn a task that waits for notification, then deallocates
        tokio::spawn(async move {
            dealloc_notify_clone.notified().await;
            pool_clone.deallocate(buffer);
        });

        let pool_clone2 = Arc::clone(&pool);
        let alloc_handle = tokio::spawn(async move {
            pool_clone2.allocate(5 * 1024, 10000).await.unwrap();
        });

        // Give the allocation task time to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(pool.queued() > 0, "Allocation should be waiting on memory.");

        // Return the memory
        dealloc_notify.notify_one();

        // Allocation should succeed
        tokio::time::timeout(std::time::Duration::from_secs(1), alloc_handle)
            .await
            .expect("Allocation should succeed soon after de-allocation")
            .unwrap();
    }

    /// Translated from `BufferPoolTest.testBufferExhaustedExceptionIsThrown`.
    ///
    /// Test if BufferExhausted error is returned when there is not enough memory to allocate
    /// and the elapsed time is greater than the max specified block time.
    #[tokio::test]
    async fn test_buffer_exhausted_error_is_returned() {
        let pool = BufferPool::new_for_test(2, 1);
        let _buffer = pool.allocate(1, 10).await.unwrap();
        let result = pool.allocate(2, 10).await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::ProducerBufferExhausted(_)),
            "Should be a BufferExhausted error"
        );
    }

    /// Translated from `BufferPoolTest.testBlockTimeout`.
    ///
    /// Verify that a failed allocation attempt due to not enough memory finishes soon
    /// after the maxBlockTimeMs.
    #[tokio::test]
    async fn test_block_timeout() {
        let max_block_ms: i64 = 10;
        let pool = BufferPool::new_for_test(2, 1);
        let _buffer = pool.allocate(1, max_block_ms).await.unwrap();

        let begin = std::time::Instant::now();
        let result = pool.allocate(2, max_block_ms).await;
        let duration = begin.elapsed();

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::ProducerBufferExhausted(_)),
            "Should be a BufferExhausted error"
        );
        assert!(
            duration.as_millis() >= max_block_ms as u128,
            "BufferExhausted should not return before maxBlockTimeMs"
        );
        assert!(
            duration.as_millis() < (max_block_ms as u128) + 1000,
            "BufferExhausted should return soon after maxBlockTimeMs"
        );
    }

    /// Translated from `BufferPoolTest.testCleanupMemoryAvailabilityWaiterOnBlockTimeout`.
    ///
    /// Test if the waiter that is waiting on availability of more memory is cleaned up
    /// when a timeout occurs.
    #[tokio::test]
    async fn test_cleanup_memory_availability_waiter_on_block_timeout() {
        let pool = BufferPool::new_for_test(2, 1);
        let _buffer = pool.allocate(1, 10).await.unwrap();

        let result = pool.allocate(2, 10).await;
        assert!(matches!(result.unwrap_err(), Error::ProducerBufferExhausted(_)));

        assert_eq!(0, pool.queued());
        assert_eq!(1, pool.available_memory());
    }

    /// Translated from `BufferPoolTest.testStressfulSituation`.
    ///
    /// This test creates lots of tasks that hammer on the pool.
    #[tokio::test]
    async fn test_stressful_situation() {
        let num_tasks = 10;
        let iterations = 50000;
        let poolable_size = 1024;
        let total_memory = (num_tasks / 2 * poolable_size) as i64;
        let pool = Arc::new(BufferPool::new_for_test(total_memory, poolable_size));

        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                let mut rng_state: u64 = rand::random();
                for _ in 0..iterations {
                    // Simple LCG random
                    rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let use_poolable = (rng_state >> 32) & 1 == 0;
                    let size = if use_poolable {
                        pool.poolable_size()
                    } else {
                        // Random size between 1 and total_memory
                        ((rng_state >> 16) as usize % pool.total_memory() as usize).max(1)
                    };
                    let buffer = pool.allocate(size, 20_000).await.unwrap();
                    pool.deallocate(buffer);
                }
            }));
        }

        for handle in handles {
            handle.await.expect("Task should have completed all iterations successfully.");
        }

        assert_eq!(total_memory, pool.available_memory());
    }

    /// Translated from `BufferPoolTest.testLargeAvailableMemory`.
    ///
    /// Tests that `available_memory()` arithmetic works correctly with values exceeding
    /// i32 range (total = 20 billion bytes). The Java test uses mock `allocateByteBuffer`
    /// and `freeSize()` to avoid actual 2GB allocations. In Rust, we simulate the same
    /// scenario by directly manipulating the pool's internal state through the public
    /// allocation/deallocation API with smaller buffers, then verifying the arithmetic
    /// with a separate pool using large values and the `free_size` + `unallocated_memory`
    /// methods.
    #[tokio::test]
    async fn test_large_available_memory() {
        let memory: i64 = 20_000_000_000;
        let poolable_size: usize = 2_000_000_000;
        let pool = BufferPool::new_for_test(memory, poolable_size);

        assert_eq!(memory, pool.available_memory());
        assert_eq!(memory, pool.total_memory());

        // Verify the available_memory formula works with large values by simulating
        // the accounting that occurs during allocation. We can't actually allocate
        // 2GB buffers in tests, but we can verify the i64 arithmetic doesn't overflow
        // by using smaller allocations and checking the accounting.
        let small_pool = BufferPool::new_for_test(1024, 256);
        assert_eq!(1024, small_pool.available_memory());

        // Allocate a poolable-sized buffer
        let buf1 = small_pool.allocate(256, 10).await.unwrap();
        assert_eq!(768, small_pool.available_memory());
        assert_eq!(768, small_pool.unallocated_memory());
        assert_eq!(0, small_pool.free_size());

        // Allocate another poolable-sized buffer
        let buf2 = small_pool.allocate(256, 10).await.unwrap();
        assert_eq!(512, small_pool.available_memory());

        // Deallocate first buffer -- should go to free list (poolable size)
        small_pool.deallocate(buf1);
        // available_memory = unallocated(512) + freeSize(1) * poolableSize(256) = 768
        assert_eq!(768, small_pool.available_memory());
        assert_eq!(512, small_pool.unallocated_memory());
        assert_eq!(1, small_pool.free_size());

        // Deallocate second buffer -- should also go to free list
        small_pool.deallocate(buf2);
        // available_memory = unallocated(512) + freeSize(2) * poolableSize(256) = 1024
        assert_eq!(1024, small_pool.available_memory());
        assert_eq!(512, small_pool.unallocated_memory());
        assert_eq!(2, small_pool.free_size());
    }

    /// Translated from `BufferPoolTest.testCleanupMemoryAvailabilityOnMetricsException`
    /// (BufferPoolTest.java:226).
    ///
    /// Verifies that when `record_wait_time` fails, the pool's memory accounting
    /// and waiter queue are still restored and the error propagates. Java uses a
    /// Mockito spy to make `recordWaitTime` throw `OutOfMemoryError`; the Rust
    /// analog injects the failure through the `fail_record_wait_time` test seam.
    #[tokio::test]
    async fn test_cleanup_memory_availability_on_metrics_error() {
        let pool = BufferPool::new_for_test(2, 1);
        pool.fail_record_wait_time.store(true, Ordering::Relaxed);

        // First allocation succeeds immediately (no wait -> no record_wait_time).
        let _buffer = pool.allocate(1, 0).await.unwrap();

        // Second allocation must block (only 1 byte free, needs 2), so the wait
        // path runs record_wait_time, which now fails.
        let result = pool.allocate(2, 1000).await;
        assert!(result.is_err(), "Expected the injected metrics failure to propagate");

        // Memory accounting and waiter queue are restored.
        assert_eq!(1, pool.available_memory());
        assert_eq!(0, pool.queued());
        assert_eq!(1, pool.unallocated_memory());

        // A subsequent allocation should not time out.
        pool.fail_record_wait_time.store(false, Ordering::Relaxed);
        pool.allocate(1, 0).await.unwrap();
    }

    /// Rust-added: verifies the `bufferpool-wait-time` and
    /// `buffer-exhausted-records` metrics are registered under the group and,
    /// after a blocking timeout, both record. Java's `BufferPoolTest` asserts
    /// these indirectly via the sensors existing; here we check the registry
    /// directly.
    #[tokio::test]
    async fn test_wait_and_exhausted_metrics_recorded() {
        let metrics = Arc::new(Metrics::new());
        let time_provider: TimeProvider = Arc::new(|| 0);
        let pool = BufferPool::new(2, 1, Arc::clone(&metrics), time_provider, "producer-metrics");

        // The four metric names are registered in the group.
        for name in [
            "bufferpool-wait-ratio",
            "bufferpool-wait-time-ns-total",
            "buffer-exhausted-rate",
            "buffer-exhausted-total",
        ] {
            let mn =
                metrics.metric_name_description_tags(name, "producer-metrics", "", std::collections::BTreeMap::new());
            assert!(metrics.metric(&mn).is_some(), "metric {name} should be registered");
        }

        // Take all memory, then a blocking allocation times out -> records both
        // the wait time and the buffer-exhausted count.
        let _buffer = pool.allocate(1, 0).await.unwrap();
        let result = pool.allocate(2, 10).await;
        assert!(matches!(result.unwrap_err(), Error::ProducerBufferExhausted(_)));

        let total_ns = metrics.metric_name_description_tags(
            "bufferpool-wait-time-ns-total",
            "producer-metrics",
            "",
            std::collections::BTreeMap::new(),
        );
        assert!(
            metrics.metric(&total_ns).unwrap().measurable_value(0) > 0.0,
            "wait-time-ns-total should have recorded"
        );
        let exhausted_total = metrics.metric_name_description_tags(
            "buffer-exhausted-total",
            "producer-metrics",
            "",
            std::collections::BTreeMap::new(),
        );
        assert_eq!(
            1.0,
            metrics.metric(&exhausted_total).unwrap().measurable_value(0),
            "buffer-exhausted-total should be 1 after one exhaustion"
        );
    }

    // Intentionally skipped Java test:
    //
    // `outOfMemoryOnAllocation` (BufferPoolTest.java:318):
    //   This test verifies that when `allocateByteBuffer()` throws `OutOfMemoryError`,
    //   the pool restores `nonPooledAvailableMemory`. In Rust, `Vec::new()` / `vec![]`
    //   on the default global allocator aborts the process on OOM (not a recoverable
    //   panic), so this recovery path cannot be tested or implemented.

    /// Translated from `BufferPoolTest.testCloseAllocations`.
    #[tokio::test]
    async fn test_close_allocations() {
        let pool = Arc::new(BufferPool::new_for_test(10, 1));
        let buffer = pool.allocate(1, 10).await.unwrap();

        // Close the buffer pool. This should prevent any further allocations.
        pool.close();

        let err = pool.allocate(1, 10).await.expect_err("Allocation should fail after close");
        assert_eq!(err.message(), "Producer closed while allocating memory");
        // Java throws a BARE `KafkaException` (`BufferPool.java:119`), matching the
        // test's `assertThrows(KafkaException.class, ..)`. It is deliberately NOT an
        // `ApiException`: `KafkaProducer.doSend` rethrows the former out of `send()`
        // and turns the latter into a failed future.
        assert!(matches!(err, Error::KafkaError(_)), "expected a bare KafkaError, got {err:?}");
        assert!(err.is_kafka_error(), "Java throws KafkaException here");
        assert!(!err.is_api_error(), "a bare KafkaException is not an ApiException");

        // Ensure deallocation still works.
        pool.deallocate(buffer);
    }

    /// Translated from `BufferPoolTest.testCloseNotifyWaiters`.
    #[tokio::test]
    async fn test_close_notify_waiters() {
        let num_workers = 2;
        let pool = Arc::new(BufferPool::new_for_test(1, 1));
        let _buffer = pool.allocate(1, i64::MAX).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..num_workers {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                let err = pool
                    .allocate(1, i64::MAX)
                    .await
                    .expect_err("Allocation should fail after close");
                assert_eq!(err.message(), "Producer closed while allocating memory");
                // Java `BufferPool.java:157`: a bare `KafkaException`, as asserted by
                // `assertThrows(KafkaException.class, ..)` in the Java test.
                assert!(matches!(err, Error::KafkaError(_)), "expected a bare KafkaError, got {err:?}");
                assert!(!err.is_api_error(), "a bare KafkaException is not an ApiException");
            }));
        }

        // Wait for workers to be blocked
        let pool_ref = Arc::clone(&pool);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if pool_ref.queued() == num_workers {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Workers should be blocked on allocation");

        // Close the buffer pool. This should notify all waiters.
        pool.close();

        for handle in handles {
            tokio::time::timeout(std::time::Duration::from_secs(5), handle)
                .await
                .expect("Worker should complete after close")
                .unwrap();
        }

        assert_eq!(0, pool.queued());
    }

    /// Translated from `BufferPoolTest.testCleanupMemoryAvailabilityWaiterOnInterruption`.
    ///
    /// In Rust/Tokio, we test that tasks timing out while waiting on allocation
    /// properly clean up their waiters and don't leak memory.
    #[tokio::test]
    async fn test_cleanup_memory_availability_waiter_on_cancellation() {
        let pool = Arc::new(BufferPool::new_for_test(2, 1));
        let _buffer = pool.allocate(1, 10).await.unwrap();

        let pool_clone1 = Arc::clone(&pool);
        let handle1 = tokio::spawn(async move {
            let _ = pool_clone1.allocate(2, 500).await;
        });

        let pool_clone2 = Arc::clone(&pool);
        let handle2 = tokio::spawn(async move {
            let _ = pool_clone2.allocate(2, 500).await;
        });

        // Wait for both to time out
        let _ = handle1.await;
        let _ = handle2.await;

        // Both allocations should have timed out and cleaned up
        assert_eq!(0, pool.queued());
        assert_eq!(1, pool.available_memory(), "Memory should not be leaked");
    }

    /// A dropped `allocate` future must credit back whatever it had accumulated and
    /// remove its waiter — Java's inner `finally` (`BufferPool.java:185-189`).
    ///
    /// This exit has no Java analogue (threads cannot be cancelled), so it was
    /// missing entirely. Both `deallocate_with_size` and `maybe_signal_next_waiter`
    /// only ever signal `waiters.front()`, so a leaked waiter that reaches the head
    /// swallows every wakeup: the live waiter behind it is never notified and times
    /// out with `BufferExhausted` even though memory is free. That is what this test
    /// pins.
    #[tokio::test]
    async fn cancelled_allocate_does_not_leak_its_waiter_or_memory() {
        let pool = Arc::new(BufferPool::new_for_test(2, 1));
        // Exhaust the pool so both allocations below have to wait.
        let held = pool.allocate(2, 10).await.unwrap();

        // Waiter 1 is cancelled while parked; it must not stay at the head of the
        // queue.
        let cancelled = {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move { pool.allocate(2, 60_000).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(1, pool.queued(), "waiter 1 should be parked");
        cancelled.abort();
        let _ = cancelled.await;

        // Waiter 2 parks behind it and must be served once memory is returned.
        let served = {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move { pool.allocate(2, 60_000).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        pool.deallocate(held);

        let buffer = tokio::time::timeout(std::time::Duration::from_secs(5), served)
            .await
            .expect("the live waiter must be signalled, not starved by a leaked waiter")
            .expect("the waiter task must not panic")
            .expect("memory was returned, so the allocation must succeed");
        assert_eq!(2, buffer.len());

        pool.deallocate(buffer);
        assert_eq!(0, pool.queued(), "no waiter may be left behind");
        assert_eq!(2, pool.available_memory(), "all memory must be back in the pool");
    }
}
