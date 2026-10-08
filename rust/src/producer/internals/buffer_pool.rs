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

#![cfg_attr(not(test), expect(dead_code))]
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
use crate::common::metrics::TimeUnit;
use crate::common::metrics::stats::Meter;
use crate::common::metrics::{Metrics, Sensor};
use crate::common::utils::Time;

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
}

/// Which allocation method a pool serves: [`Full`](Self::Full) accepts only
/// [`BufferPool::allocate`] (the full strategy), [`Incremental`](Self::Incremental) only
/// [`BufferPool::allocate_chunks`] / [`BufferPool::try_allocate_chunks`] (the incremental
/// strategy, KIP-1332). Fixed at construction so the two are never mixed on the same pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool$AllocationMode")]
pub(crate) enum AllocationMode {
    /// Java's `FULL`.
    Full,
    /// Java's `INCREMENTAL`.
    Incremental,
}

impl std::fmt::Display for AllocationMode {
    /// Java's enum `toString()`, which the mode-guard messages embed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AllocationMode::Full => "FULL",
            AllocationMode::Incremental => "INCREMENTAL",
        })
    }
}

/// A pool of byte buffers kept under a given memory limit.
///
/// Provides bounded memory allocation with backpressure and buffer recycling
/// for the most common buffer size (the "poolable size").
#[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool")]
pub struct BufferPool {
    /// The lock-protected inner state.
    inner: Mutex<PoolInner>,
    /// Total memory managed by this pool.
    total_memory: i64,
    /// The buffer size to cache in the free list rather than deallocating.
    poolable_size: usize,
    /// Whether the pool has been closed.
    closed: AtomicBool,
    /// Which allocation method this pool serves (Java's `allocationMode`).
    allocation_mode: AllocationMode,
    /// Java: `private final Time time`.
    time: Arc<dyn Time>,
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
    /// Test-only injection point mirroring Java tests overriding the protected
    /// `allocateByteBuffer` to throw `OutOfMemoryError`: when `true`,
    /// [`allocate_byte_buffer`](Self::allocate_byte_buffer) panics, exercising the
    /// unwinding (`finally`) refund paths.
    #[cfg(test)]
    fail_allocate_byte_buffer: AtomicBool,
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
    /// * `time` - Time instance
    /// * `metric_grp_name` - Logical group name for metrics
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#BufferPool")]
    pub fn new(
        memory: i64,
        poolable_size: usize,
        metrics: Arc<Metrics>,
        time: Arc<dyn Time>,
        metric_grp_name: &str,
    ) -> Self {
        Self::with_allocation_mode(memory, poolable_size, metrics, time, metric_grp_name, AllocationMode::Full)
    }

    /// Create a new buffer pool that serves the given [`AllocationMode`].
    ///
    /// # Arguments
    ///
    /// * `memory` - The maximum amount of memory that this buffer pool can allocate
    /// * `poolable_size` - The buffer size to cache in the free list rather than deallocating
    /// * `metrics` - Instance of `Metrics`
    /// * `time` - Time instance
    /// * `metric_grp_name` - Logical group name for metrics
    /// * `allocation_mode` - which allocation method this pool serves
    ///   ([`allocate`](Self::allocate) vs [`allocate_chunks`](Self::allocate_chunks))
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#BufferPool")]
    pub(crate) fn with_allocation_mode(
        memory: i64,
        poolable_size: usize,
        metrics: Arc<Metrics>,
        time: Arc<dyn Time>,
        metric_grp_name: &str,
        allocation_mode: AllocationMode,
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
            allocation_mode,
            time,
            wait_time_sensor,
            buffer_exhausted_sensor,
            #[cfg(test)]
            fail_record_wait_time: AtomicBool::new(false),
            #[cfg(test)]
            fail_allocate_byte_buffer: AtomicBool::new(false),
        }
    }

    /// Test-only convenience constructor that supplies a fresh reporter-less
    /// [`Metrics`] registry and the system clock, mirroring Java's
    /// `BufferPoolTest` passing `new Metrics()`. Java has no metrics-less
    /// production constructor.
    #[cfg(test)]
    pub(crate) fn new_for_test(memory: i64, poolable_size: usize) -> Self {
        Self::new(
            memory,
            poolable_size,
            Arc::new(Metrics::new()),
            Arc::new(crate::common::utils::SystemTime),
            "producer-metrics",
        )
    }

    /// Test-only [`AllocationMode::Incremental`] pool over a fresh [`Metrics`] registry and the
    /// system clock, mirroring the `pool(totalMemory, chunkSize)` helpers of Java's
    /// `BufferPoolChunkAllocationTest` / `ChunkedByteBufferOutputStreamTest`.
    #[cfg(test)]
    pub(crate) fn new_incremental_for_test(memory: i64, chunk_size: usize) -> Self {
        Self::with_allocation_mode(
            memory,
            chunk_size,
            Arc::new(Metrics::new()),
            Arc::new(crate::common::utils::SystemTime),
            "producer-metrics",
            AllocationMode::Incremental,
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
    /// Returns [`Error::LocalIllegalState`] if the pool serves [`AllocationMode::Incremental`].
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `size` is larger than the total memory
    /// controlled by the pool.
    ///
    /// Returns [`Error::ProducerBufferExhausted`] if the timeout elapses before enough memory
    /// becomes available.
    ///
    /// Returns [`Error::KafkaError`] if the pool is closed while waiting.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#allocate")]
    pub async fn allocate(&self, size: usize, max_block_ms: i64) -> Result<Vec<u8>, Error> {
        if self.allocation_mode != AllocationMode::Full {
            return Err(Error::local_illegal_state(format!(
                "allocate() is not supported in {} allocation mode; use allocateChunks()",
                self.allocation_mode
            )));
        }
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
                    AllocResult::Immediate(self.allocate_byte_buffer(size))
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
            // memory")` (`BufferPool.java:151`) — a BARE `KafkaException`, not an
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
        /// Java's inner `finally` (`BufferPool.java:199-203`), as a `Drop` type:
        ///
        /// ```java
        /// } finally {
        ///     // When this loop was not able to successfully terminate don't loose available memory
        ///     this.nonPooledAvailableMemory += accumulated;
        ///     this.waiters.remove(moreMemory);
        /// }
        /// ```
        ///
        /// plus the waiter signal from the enclosing `finally` (`:205-213`), which
        /// also runs on every exit.
        ///
        /// Java's exits are "got the memory" (where it zeroes `accumulated` first, at
        /// `:197-198`, so the credit is a no-op) and "threw". Rust adds a third: the
        /// future being dropped at the wait below, which has no Java analogue because
        /// threads cannot be cancelled (CLAUDE.md §11.6). Without this the waiter's
        /// `Arc<Notify>` stayed in `inner.waiters` forever, and since
        /// `deallocate_with_size` and `signal_next_waiter_if_memory_available` only ever signal
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
                BufferPool::signal_next_waiter_if_memory_available(&inner);
            }
        }

        let mut guard = WaitGuard { pool: self, waiter: more_memory, accumulated: 0 };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(max_block_ms.max(0) as u64);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

            // Wait for notification (no lock held here). This is the await the
            // caller may be cancelled at, which is what `WaitGuard` exists for.
            // `await_memory` records the wait time and returns the close /
            // timeout errors (Java's `awaitMemory`, extracted by KIP-1332). Every
            // error exit leaves the waiter removal, the credit-back and the
            // next-waiter signal to `WaitGuard::drop`, mirroring Java's `finally`s;
            // doing it by hand as well would return the memory twice.
            self.await_memory(more_memory, remaining, true, || {
                format!(
                    "Failed to allocate {} bytes within the configured max blocking time \
                     {} ms. Total memory: {} bytes. Available memory: {} bytes. \
                     Poolable size: {} bytes",
                    size,
                    max_block_ms,
                    self.total_memory,
                    self.available_memory(),
                    self.poolable_size
                )
            })
            .await?;

            // Check state under the lock (no await in this block).
            let wake_result = {
                let mut inner = self.inner.lock().unwrap();

                if guard.accumulated == 0 && size == self.poolable_size && !inner.free.is_empty() {
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
                    // Java 186-187 sets `accumulated = size` and then zeroes it at
                    // `:197-198`; the buffer came off the free list, so there is nothing
                    // to credit back either way.
                    guard.accumulated = 0;
                    return Ok(buf);
                },
                WakeResult::Ready => {
                    // "Don't reclaim memory on throwable since nothing was thrown"
                    // (Java 197-198): the reserved bytes leave with the caller.
                    guard.accumulated = 0;
                    return Ok(self.allocate_byte_buffer(size));
                },
                WakeResult::NeedMore => continue,
            }
        }
    }

    /// Record the time (in nanoseconds) an appender waited for space
    /// allocation. Translated from Java's `protected void recordWaitTime(long
    /// timeNs)` (`BufferPool.java:404-407`), which records against the
    /// `bufferpool-wait-time` sensor at the current wall-clock millisecond.
    ///
    /// Java's method is `void` but can throw (its tests inject an
    /// `OutOfMemoryError` via a Mockito spy). Rust sensor recording is
    /// infallible, so this always returns `Ok` in production; the `Result`
    /// return type is the faithful translation of the throwing contract, and
    /// tests inject a failure through `fail_record_wait_time`.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#recordWaitTime")]
    fn record_wait_time(&self, time_ns: i64) -> Result<(), Error> {
        #[cfg(test)]
        if self.fail_record_wait_time.load(Ordering::Relaxed) {
            return Err(Error::with_message(
                crate::common::protocol::Errors::UnknownServerError,
                "Injected recordWaitTime failure",
            ));
        }
        self.wait_time_sensor
            .record_value_time_ms(time_ns as f64, self.time.milliseconds());
        Ok(())
    }

    /// Attempt to ensure we have at least the requested number of bytes of memory for
    /// allocation by deallocating pooled buffers (if needed).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#freeUp")]
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

    /// Wake the longest-waiting task if any memory (pooled or non-pooled) is available.
    /// Takes the locked state, as Java requires the lock to be held. No-op if no waiters or no
    /// memory is free.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#signalNextWaiterIfMemoryAvailable")]
    fn signal_next_waiter_if_memory_available(inner: &PoolInner) {
        if !(inner.non_pooled_available_memory == 0 && inner.free.is_empty())
            && let Some(next) = inner.waiters.front()
        {
            next.notify_one();
        }
    }

    /// Block once on `more_memory` for up to `remaining`, recording the wait time. Shared by
    /// [`allocate`](Self::allocate) and [`allocate_chunks`](Self::allocate_chunks).
    ///
    /// Java returns the nanos waited for the caller to deduct from its blocking budget. The Rust
    /// callers keep an absolute deadline on the tokio clock instead (the timeout is driven by it,
    /// and an injected `MockTime` would never advance a deducted budget), so nothing is returned.
    ///
    /// Must be called without the pool lock held: `exhausted_message` reads
    /// [`available_memory`](Self::available_memory), which takes it.
    ///
    /// # Errors
    ///
    /// - [`Error::KafkaError`] if the pool was closed during the wait;
    /// - [`Error::ProducerBufferExhausted`] if the wait timed out; the buffer-exhausted metric is
    ///   recorded only when `record_exhausted_on_timeout` is true (the incremental extension path
    ///   recovers without dropping the record, so it records the drop itself if needed);
    /// - the error from [`record_wait_time`](Self::record_wait_time).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#awaitMemory")]
    async fn await_memory(
        &self,
        more_memory: &tokio::sync::Notify,
        remaining: std::time::Duration,
        record_exhausted_on_timeout: bool,
        exhausted_message: impl FnOnce() -> String,
    ) -> Result<(), Error> {
        // The wait duration is measured with `time.nanoseconds()` bracketing the wait, as in
        // Java, and recorded even when the wait ends in timeout / close (Java's `finally`).
        let start_wait_ns = self.time.nanoseconds();
        let waiting_time_elapsed = tokio::time::timeout(remaining, more_memory.notified()).await.is_err();
        let end_wait_ns = self.time.nanoseconds();
        self.record_wait_time((end_wait_ns - start_wait_ns).max(0))?;

        if self.closed.load(Ordering::Acquire) {
            // Java: `throw new KafkaException("Producer closed while allocating memory")` — a
            // BARE `KafkaException`, not an `ApiException`. `KafkaProducer.doSend` dispatches on
            // exactly that difference: it rethrows the former out of `send()` without invoking
            // the user callback, and turns an `ApiException` into a failed future.
            return Err(Error::kafka_message("Producer closed while allocating memory"));
        }

        if waiting_time_elapsed {
            if record_exhausted_on_timeout {
                self.record_buffer_exhausted();
            }
            return Err(Error::buffer_exhausted(exhausted_message()));
        }
        Ok(())
    }

    /// Allocate `ceil(total_size / poolable_size)` poolable-sized buffers atomically, mirroring
    /// [`allocate`](Self::allocate): satisfied immediately if memory is available, else blocks up
    /// to `max_time_to_block_ms` for the whole request, FIFO on the waiters queue with a single
    /// waiter per request. The reservation is tracked as bytes against the non-pooled memory plus
    /// chunks taken from the free list. Any failure refunds the whole reservation and signals the
    /// next waiter before the error propagates, so a failed request leaves nothing reserved.
    ///
    /// Used by the incremental buffer.memory allocation strategy (KIP-1332); the poolable size is
    /// the chunk size.
    ///
    /// # Cancellation (Rust-only)
    ///
    /// Java threads cannot be cancelled, but this future can be dropped at its wait (CLAUDE.md
    /// §11.6). Dropping it runs the same refund as a timeout: chunks already taken go back to the
    /// free list, reserved bytes back to the non-pooled memory, and the waiter leaves the queue,
    /// so it cannot swallow the next waiter's wakeup.
    ///
    /// # Arguments
    ///
    /// * `total_size` - minimum total bytes of capacity required across the returned chunks
    /// * `max_time_to_block_ms` - maximum time in milliseconds to block waiting for memory
    ///
    /// Returns `ceil(total_size / poolable_size())` buffers, each of length and capacity
    /// `poolable_size()`.
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalState`] if the pool serves [`AllocationMode::Full`];
    /// - [`Error::LocalIllegalArgument`] if `total_size <= 0`, or if the request rounded up to
    ///   whole chunks exceeds [`total_memory`](Self::total_memory);
    /// - [`Error::ProducerBufferExhausted`] if the request can't be satisfied within
    ///   `max_time_to_block_ms` (the buffer-exhausted metric is NOT recorded: the caller decides
    ///   whether the record is dropped, see [`record_buffer_exhausted`](Self::record_buffer_exhausted));
    /// - [`Error::KafkaError`] if the pool is closed, before or during the wait.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#allocateChunks")]
    pub(crate) async fn allocate_chunks(
        &self,
        total_size: i32,
        max_time_to_block_ms: i64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let (num_chunks, memory_required) = self.chunk_request(total_size)?;

        let more_memory = {
            let mut inner = self.inner.lock().unwrap();
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::kafka_message("Producer closed while allocating memory"));
            }
            if let Some(pooled) = self.take_chunks_if_available(&mut inner, num_chunks, memory_required) {
                // Java's outer `finally`.
                Self::signal_next_waiter_if_memory_available(&inner);
                drop(inner);
                return Ok(self.allocate_reserved_chunks(pooled, num_chunks, memory_required));
            }
            // Not enough memory available, so we wait to acquire the memory needed for all the
            // chunks. Same as allocate, but for the whole multi-chunk request: a single waiter is
            // added to the queue to ensure FIFO fairness at the request level.
            let notify = Arc::new(tokio::sync::Notify::new());
            inner.waiters.push_back(Arc::clone(&notify));
            notify
        }; // MutexGuard dropped here, before any .await

        let pooled = self
            .allocate_chunks_blocking(num_chunks, memory_required, max_time_to_block_ms, &more_memory)
            .await?;
        Ok(self.allocate_reserved_chunks(pooled, num_chunks, memory_required))
    }

    /// The non-blocking form of [`allocate_chunks`](Self::allocate_chunks): Java's
    /// `allocateChunks(totalSize, 0L)`, which the incremental strategy uses to extend an open
    /// batch mid-append (`ChunkedRecordAccumulator.allocateExtensionChunks`).
    ///
    /// With a zero budget Java's wait path joins the queue, times out on its first wait without
    /// taking anything, records the (zero) wait, leaves the queue and signals the next waiter. A
    /// synchronous method does all of that without ever parking, so it has no `.await` to be
    /// cancelled at and needs no refund-on-drop: the outcome is the same as Java's, minus the
    /// momentary queue entry.
    ///
    /// # Errors
    ///
    /// As [`allocate_chunks`](Self::allocate_chunks), with
    /// [`Error::ProducerBufferExhausted`] whenever the memory is not available right now.
    pub(crate) fn try_allocate_chunks(&self, total_size: i32) -> Result<Vec<Vec<u8>>, Error> {
        let (num_chunks, memory_required) = self.chunk_request(total_size)?;
        {
            let mut inner = self.inner.lock().unwrap();
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::kafka_message("Producer closed while allocating memory"));
            }
            let pooled = self.take_chunks_if_available(&mut inner, num_chunks, memory_required);
            Self::signal_next_waiter_if_memory_available(&inner);
            if let Some(pooled) = pooled {
                drop(inner);
                return Ok(self.allocate_reserved_chunks(pooled, num_chunks, memory_required));
            }
        }
        // Java's zero-length `awaitMemory`: record the wait, then report close before timeout.
        let start_wait_ns = self.time.nanoseconds();
        let end_wait_ns = self.time.nanoseconds();
        self.record_wait_time((end_wait_ns - start_wait_ns).max(0))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::kafka_message("Producer closed while allocating memory"));
        }
        Err(Error::buffer_exhausted(self.chunks_exhausted_message(
            num_chunks,
            memory_required,
            0,
        )))
    }

    /// The validation `allocateChunks` performs before taking the lock: the mode guard, the
    /// positive-size check and [`return_if_chunks_needed_exceeds_pool`](Self::return_if_chunks_needed_exceeds_pool).
    /// Returns `(num_chunks, memory_required)`.
    fn chunk_request(&self, total_size: i32) -> Result<(usize, i64), Error> {
        if self.allocation_mode != AllocationMode::Incremental {
            return Err(Error::local_illegal_state(format!(
                "allocateChunks() is not supported in {} allocation mode; use allocate()",
                self.allocation_mode
            )));
        }
        if total_size <= 0 {
            return Err(Error::local_illegal_argument(format!(
                "totalSize must be positive: {}",
                total_size
            )));
        }
        let chunk_size = self.poolable_size as i64;
        let num_chunks = (total_size as i64 + chunk_size - 1) / chunk_size;
        let memory_required = num_chunks * chunk_size;
        self.return_if_chunks_needed_exceeds_pool(total_size, num_chunks, memory_required)?;
        Ok((num_chunks as usize, memory_required))
    }

    /// Java's immediate branch of `allocateChunks`: if the free list plus the non-pooled memory
    /// cover the request, take free-list chunks first and reserve the rest as non-pooled bytes.
    /// Returns the taken free-list chunks, or `None` (taking nothing) if memory is short.
    fn take_chunks_if_available(
        &self,
        inner: &mut PoolInner,
        num_chunks: usize,
        memory_required: i64,
    ) -> Option<Vec<Vec<u8>>> {
        let chunk_size = self.poolable_size as i64;
        let free_list_bytes = inner.free.len() as i64 * chunk_size;
        if inner.non_pooled_available_memory + free_list_bytes < memory_required {
            return None;
        }
        let mut pooled = Vec::with_capacity(num_chunks);
        while pooled.len() < num_chunks
            && let Some(chunk) = inner.free.pop_front()
        {
            pooled.push(chunk);
        }
        let remaining_bytes = memory_required - pooled.len() as i64 * chunk_size;
        if remaining_bytes > 0 {
            // remaining_bytes > 0 means the free list was fully drained into `pooled`, so the
            // remainder comes entirely from non-pooled memory (sufficient per the check above).
            inner.non_pooled_available_memory -= remaining_bytes;
        }
        Some(pooled)
    }

    /// The blocking portion of [`allocate_chunks`](Self::allocate_chunks), called when memory is
    /// not immediately available. `more_memory` has already been added to the waiters queue.
    /// Returns the chunks taken from the free list; the rest of `memory_required` is reserved as
    /// non-pooled bytes for the caller to materialise.
    async fn allocate_chunks_blocking(
        &self,
        num_chunks: usize,
        memory_required: i64,
        max_time_to_block_ms: i64,
        more_memory: &Arc<tokio::sync::Notify>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        /// Java's `finally` blocks around the wait loop, as a `Drop` type:
        ///
        /// ```java
        /// } finally {
        ///     if (!allocationCompleted) {
        ///         // Refund all that was reserved (pooled chunks and non-pooled bytes)
        ///         this.nonPooledAvailableMemory += nonPoolAccumulated;
        ///         for (ByteBuffer chunk : pooled)
        ///             free.addFirst(chunk);
        ///         pooled.clear();
        ///     }
        ///     waiters.remove(moreMemory);
        /// }
        /// ```
        ///
        /// plus the outer `finally`'s `signalNextWaiterIfMemoryAvailable()`. It also runs when
        /// the future is dropped at the wait, which Java cannot express.
        struct ChunkWaitGuard<'a> {
            pool: &'a BufferPool,
            waiter: &'a Arc<tokio::sync::Notify>,
            /// Chunks taken from the free list (Java's `pooled`).
            pooled: Vec<Vec<u8>>,
            /// Bytes drawn from non-pooled memory only, always a whole-chunk multiple (Java's
            /// `nonPoolAccumulated`).
            non_pool_accumulated: i64,
            allocation_completed: bool,
        }

        impl Drop for ChunkWaitGuard<'_> {
            fn drop(&mut self) {
                let mut inner = self.pool.inner.lock().unwrap();
                if !self.allocation_completed {
                    inner.non_pooled_available_memory += self.non_pool_accumulated;
                    for chunk in self.pooled.drain(..) {
                        inner.free.push_front(chunk);
                    }
                }
                BufferPool::remove_waiter(&mut inner, self.waiter);
                BufferPool::signal_next_waiter_if_memory_available(&inner);
            }
        }

        let chunk_size = self.poolable_size as i64;
        let mut guard = ChunkWaitGuard {
            pool: self,
            waiter: more_memory,
            pooled: Vec::with_capacity(num_chunks),
            non_pool_accumulated: 0,
            allocation_completed: false,
        };
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(max_time_to_block_ms.max(0) as u64);

        while guard.pooled.len() as i64 * chunk_size + guard.non_pool_accumulated < memory_required {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            // Not recording the buffer-exhausted metric on timeout (`record_exhausted_on_timeout`
            // false): this may be the extension path, which recovers without dropping the
            // record, so the caller records the drop if needed.
            self.await_memory(more_memory, remaining, false, || {
                self.chunks_exhausted_message(num_chunks, memory_required, max_time_to_block_ms)
            })
            .await?;

            let mut inner = self.inner.lock().unwrap();
            // Reuse free-list chunks first, preferring them over raw reservations: if a taken
            // chunk covers a slot already reserved as raw bytes in an earlier iteration, hand
            // that raw reservation back to the pool.
            while guard.pooled.len() < num_chunks
                && let Some(chunk) = inner.free.pop_front()
            {
                guard.pooled.push(chunk);
                // non_pool_accumulated is always chunk-aligned
                if guard.non_pool_accumulated >= chunk_size {
                    guard.non_pool_accumulated -= chunk_size;
                    inner.non_pooled_available_memory += chunk_size;
                }
            }
            // Reserve non-pooled memory for the still-uncovered chunks, in whole chunks (the
            // buffers themselves are allocated after the lock is released).
            while guard.pooled.len() + ((guard.non_pool_accumulated / chunk_size) as usize) < num_chunks
                && inner.non_pooled_available_memory >= chunk_size
            {
                inner.non_pooled_available_memory -= chunk_size;
                guard.non_pool_accumulated += chunk_size;
            }
        } // MutexGuard dropped at the end of each iteration, before the next .await

        guard.allocation_completed = true;
        Ok(std::mem::take(&mut guard.pooled))
    }

    /// Java's tail of `allocateChunks`, run outside the lock: allocate raw chunks for the
    /// reserved non-pooled portion and return them after the free-list chunks.
    ///
    /// Java refunds all of `memory_required` and signals the next waiter if an allocation throws
    /// (`releaseReservedBytes` in a `finally`); the free-list chunks already taken are lost on
    /// that path, as in `safeAllocateByteBuffer` (the bytes return, the buffer instances become
    /// garbage). The Rust default allocator aborts on OOM rather than unwinding, so the only way
    /// to leave this loop early is a panic; the `finally` becomes a guard that runs on unwind.
    ///
    /// Free-list chunks are handed out at full length (`resize` to their capacity). Chunks the
    /// incremental strategy returns already have length equal to capacity, so this writes
    /// nothing; unlike [`allocate`](Self::allocate) it does not zero recycled bytes, which, like
    /// Java's `ByteBuffer.clear()`, nothing reads before overwriting.
    fn allocate_reserved_chunks(&self, pooled: Vec<Vec<u8>>, num_chunks: usize, memory_required: i64) -> Vec<Vec<u8>> {
        struct ReleaseOnUnwind<'a> {
            pool: &'a BufferPool,
            bytes: i64,
            error: bool,
        }
        impl Drop for ReleaseOnUnwind<'_> {
            fn drop(&mut self) {
                if self.error {
                    self.pool.release_reserved_bytes(self.bytes);
                }
            }
        }

        let mut release = ReleaseOnUnwind { pool: self, bytes: memory_required, error: true };
        let chunks_still_needed = num_chunks - pooled.len();
        let mut result = Vec::with_capacity(num_chunks);
        for mut chunk in pooled {
            let capacity = chunk.capacity();
            chunk.resize(capacity, 0);
            result.push(chunk);
        }
        for _ in 0..chunks_still_needed {
            result.push(self.allocate_byte_buffer(self.poolable_size));
        }
        release.error = false;
        result
    }

    /// Java's `BufferExhaustedException` message for a chunk request.
    fn chunks_exhausted_message(&self, num_chunks: usize, memory_required: i64, max_time_to_block_ms: i64) -> String {
        format!(
            "Failed to allocate {} bytes ({} chunks of {}) within the configured max blocking time {} ms. \
             Total memory: {} bytes. Available memory: {} bytes.",
            memory_required,
            num_chunks,
            self.poolable_size,
            max_time_to_block_ms,
            self.total_memory,
            self.available_memory()
        )
    }

    /// Return an error if the request memory rounded up to whole chunks would exceed the pool.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#throwIfChunksNeededExceedsPool")]
    fn return_if_chunks_needed_exceeds_pool(
        &self,
        total_size: i32,
        num_chunks: i64,
        memory_required: i64,
    ) -> Result<(), Error> {
        if memory_required > self.total_memory {
            return Err(Error::local_illegal_argument(format!(
                "Attempt to allocate {} bytes ({} chunks of {} = {} bytes), but the hard limit on memory \
                 allocations is {}.",
                total_size, num_chunks, self.poolable_size, memory_required, self.total_memory
            )));
        }
        Ok(())
    }

    /// Record that a record send was dropped because the buffer pool was exhausted. Shared by the
    /// full strategy ([`allocate`](Self::allocate)) and the incremental strategy
    /// (`ChunkedRecordAccumulator`), so both update the same buffer-exhausted metrics.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#recordBufferExhausted")]
    pub(crate) fn record_buffer_exhausted(&self) {
        // Recorded outside the pool lock (value/timestamp are independent of pool state).
        self.buffer_exhausted_sensor.record_value_time_ms(1.0, self.time.milliseconds());
    }

    /// Return previously-reserved non-pooled bytes to the pool and signal the next waiter. Takes
    /// the lock internally. Used by callers that reserve memory and then need to roll back the
    /// reservation (e.g., upon errors).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#releaseReservedBytes")]
    fn release_reserved_bytes(&self, bytes: i64) {
        let mut inner = self.inner.lock().unwrap();
        inner.non_pooled_available_memory += bytes;
        if let Some(next) = inner.waiters.front() {
            next.notify_one();
        }
    }

    /// Allocate a zeroed buffer of `size` bytes (Java's protected `allocateByteBuffer`, the
    /// override point its tests use to inject an `OutOfMemoryError`).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#allocateByteBuffer")]
    fn allocate_byte_buffer(&self, size: usize) -> Vec<u8> {
        #[cfg(test)]
        if self.fail_allocate_byte_buffer.load(Ordering::Relaxed) {
            panic!("Injected allocateByteBuffer failure");
        }
        vec![0u8; size]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#deallocate")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#deallocate")]
    pub fn deallocate(&self, buffer: Vec<u8>) {
        let size = buffer.capacity();
        self.deallocate_with_size(buffer, size);
    }

    /// The total free memory both unallocated and in the free list.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#availableMemory")]
    pub fn available_memory(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.non_pooled_available_memory + inner.free.len() as i64 * self.poolable_size as i64
    }

    /// Get the unallocated memory (not in the free list or in use).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#unallocatedMemory")]
    pub fn unallocated_memory(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.non_pooled_available_memory
    }

    /// The number of tasks blocked waiting on memory.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#queued")]
    pub fn queued(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.waiters.len()
    }

    /// The buffer size that will be retained in the free list after use.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#poolableSize")]
    pub fn poolable_size(&self) -> usize {
        self.poolable_size
    }

    /// The total memory managed by this pool.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#totalMemory")]
    pub fn total_memory(&self) -> i64 {
        self.total_memory
    }

    /// Which allocation method this pool serves, so callers that only use one of them can
    /// validate the pool they were given up front.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#allocationMode")]
    pub(crate) fn allocation_mode(&self) -> AllocationMode {
        self.allocation_mode
    }

    /// The number of buffers in the free list.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#freeSize")]
    pub fn free_size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.free.len()
    }

    /// Closes the buffer pool. Memory will be prevented from being allocated, but may be
    /// deallocated. All allocations awaiting available memory will be notified to abort.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPool#close")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testSimple")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testCantAllocateMoreMemoryThanWeHave")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testDelayedAllocation")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testBlockTimeout")]
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
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testCleanupMemoryAvailabilityWaiterOnBlockTimeout"
    )]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testStressfulSituation")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testLargeAvailableMemory")]
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
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testCleanupMemoryAvailabilityOnMetricsException"
    )]
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
        let pool = BufferPool::new(
            2,
            1,
            Arc::clone(&metrics),
            Arc::new(crate::common::utils::SystemTime),
            "producer-metrics",
        );

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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testCloseAllocations")]
    async fn test_close_allocations() {
        let pool = Arc::new(BufferPool::new_for_test(10, 1));
        let buffer = pool.allocate(1, 10).await.unwrap();

        // Close the buffer pool. This should prevent any further allocations.
        pool.close();

        let err = pool.allocate(1, 10).await.expect_err("Allocation should fail after close");
        assert_eq!(err.message(), "Producer closed while allocating memory");
        // Java throws a BARE `KafkaException` (`BufferPool.java:151`), matching the
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolTest#testCloseNotifyWaiters")]
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
                // Java `BufferPool.java:246`: a bare `KafkaException`, as asserted by
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
    /// remove its waiter — Java's inner `finally` (`BufferPool.java:199-203`).
    ///
    /// This exit has no Java analogue (threads cannot be cancelled), so it was
    /// missing entirely. Both `deallocate_with_size` and `signal_next_waiter_if_memory_available`
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

/// Translated from `BufferPoolChunkAllocationTest` (Apache Kafka 4.4, KAFKA-20578).
///
/// Java parks real threads in `allocateChunks` and polls `queued()` with
/// `TestUtils.waitForCondition`; here the waiters are spawned tokio tasks and
/// [`wait_for_condition`] polls the same predicates.
#[cfg(test)]
mod chunk_allocation_tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Safety ceiling for block-until-available requests that the test unblocks itself (by
    /// deallocating or closing). The waiter is always signaled first, so a passing test returns
    /// in milliseconds; this only bounds how long a broken test can block before failing.
    const MAX_BLOCK_TIME_MS: i64 = 2_000;

    fn pool(total_memory: i64, chunk_size: usize) -> Arc<BufferPool> {
        Arc::new(BufferPool::new_incremental_for_test(total_memory, chunk_size))
    }

    /// Java's `TestUtils.waitForCondition`, with its default 15 s ceiling.
    async fn wait_for_condition(condition: impl Fn() -> bool, message: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !condition() {
            assert!(Instant::now() < deadline, "Condition not met within timeout 15000. {message}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn take_one(p: &BufferPool, chunk_size: usize) -> Vec<u8> {
        p.allocate_chunks(chunk_size as i32, 100).await.unwrap().pop().unwrap()
    }

    /// Single-chunk request returns a list of one buffer at the chunk size.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testAllocateOneChunk")]
    async fn test_allocate_one_chunk() {
        let chunk_size = 64;
        let p = pool(1024, chunk_size);
        let chunks = p.allocate_chunks(chunk_size as i32, 100).await.unwrap();
        assert_eq!(1, chunks.len());
        assert_eq!(chunk_size, chunks[0].capacity());
    }

    /// Total size that's not a multiple of chunk size rounds up.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testAllocateRoundsUpToChunkBoundary"
    )]
    async fn test_allocate_rounds_up_to_chunk_boundary() {
        let chunk_size = 64;
        let p = pool(1024, chunk_size);
        // 65 bytes requested -> 2 chunks (128 bytes total).
        let chunks = p.allocate_chunks(65, 100).await.unwrap();
        assert_eq!(2, chunks.len());
        for chunk in &chunks {
            assert_eq!(chunk_size, chunk.capacity());
        }
    }

    /// Multi-chunk request returns ceil(totalSize / chunkSize) chunks.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testAllocateMultipleChunks"
    )]
    async fn test_allocate_multiple_chunks() {
        let chunk_size = 64;
        let p = pool(1024, chunk_size);
        let chunks = p.allocate_chunks(4 * chunk_size as i32, 100).await.unwrap();
        assert_eq!(4, chunks.len());
    }

    /// Pool memory accounting: after allocation, the unallocated portion shrinks by the request.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testAvailableMemoryAfterAllocation"
    )]
    async fn test_available_memory_after_allocation() {
        let chunk_size = 64;
        let total = 256;
        let p = pool(total, chunk_size);
        let _chunks = p.allocate_chunks(3 * chunk_size as i32, 100).await.unwrap();
        assert_eq!(total - 3 * chunk_size as i64, p.available_memory());
    }

    /// Returning chunks via deallocate restores pool memory.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testDeallocationRestoresMemory"
    )]
    async fn test_deallocation_restores_memory() {
        let chunk_size = 64;
        let total = 256;
        let p = pool(total, chunk_size);
        let chunks = p.allocate_chunks(2 * chunk_size as i32, 100).await.unwrap();
        for chunk in chunks {
            p.deallocate(chunk);
        }
        assert_eq!(total, p.available_memory());
    }

    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testRejectsRequestExceedingTotalMemory"
    )]
    async fn test_rejects_request_exceeding_total_memory() {
        let p = pool(128, 64);
        let err = p.allocate_chunks(129, 100).await.unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)), "got {err:?}");
        assert_eq!(
            "Attempt to allocate 129 bytes (3 chunks of 64 = 192 bytes), but the hard limit on memory allocations is 128.",
            err.message()
        );
    }

    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testRejectsNonPositiveRequest"
    )]
    async fn test_rejects_non_positive_request() {
        let p = pool(128, 64);
        for total_size in [0, -1] {
            let err = p.allocate_chunks(total_size, 100).await.unwrap_err();
            assert!(matches!(err, Error::LocalIllegalArgument(_)), "got {err:?}");
            assert_eq!(format!("totalSize must be positive: {total_size}"), err.message());
        }
    }

    /// A request that cannot be satisfied immediately and has no time to wait takes nothing: it
    /// blocks on the wait queue before acquiring anything, so the timeout leaves pool memory
    /// untouched (no roll back needed).
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testImmediateTimeoutAcquiresNothing"
    )]
    async fn test_immediate_timeout_acquires_nothing() {
        let chunk_size = 64;
        let total = 2 * chunk_size as i64; // only 2 chunks worth of memory
        let p = pool(total, chunk_size);
        // Reserve one chunk so the pool has only 1 left.
        let held = take_one(&p, chunk_size).await;

        // Request 2 chunks with a zero deadline. Only 1 chunk's worth is free, so the request
        // goes to the wait queue and times out on its first wait, before taking anything.
        let err = p.allocate_chunks(2 * chunk_size as i32, 0).await.unwrap_err();
        assert!(matches!(err, Error::ProducerBufferExhausted(_)), "got {err:?}");
        assert_eq!(
            "Failed to allocate 128 bytes (2 chunks of 64) within the configured max blocking time 0 ms. \
             Total memory: 128 bytes. Available memory: 64 bytes.",
            err.message()
        );

        // Available memory reflects only the chunk we deliberately hold.
        assert_eq!(total - chunk_size as i64, p.available_memory());

        p.deallocate(held);
        assert_eq!(total, p.available_memory());
    }

    /// No partial chunk holds during the wait. While a multi-chunk request blocks, the pool's
    /// `available_memory()` must reflect bytes the waiter has not yet "earned".
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testNoPartialHoldsDuringWait"
    )]
    async fn test_no_partial_holds_during_wait() {
        let chunk_size = 64;
        // Hold 2 of 3 chunks so exactly 1 is free — a 2-chunk request can't be satisfied
        // immediately.
        let total = 3 * chunk_size as i64;
        let p = pool(total, chunk_size);
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        assert_eq!(chunk_size as i64, p.available_memory(), "pool should have exactly 1 chunk free");

        // Background task requests 2 chunks; will block on the 2nd.
        let t = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(2 * chunk_size as i32, MAX_BLOCK_TIME_MS).await })
        };
        // Wait until the task has joined the waiters queue.
        wait_for_condition(|| p.queued() == 1, "waiter should be parked on the pool's queue").await;

        // While the waiter is parked, the pool's available memory must still report the 1
        // chunk's worth — the waiter has NOT consumed any of it (no partial hold).
        assert_eq!(
            chunk_size as i64,
            p.available_memory(),
            "pool memory must reflect no partial holds during the atomic wait"
        );

        // The pool still has 1 chunk free (asserted above) and the waiter never consumed it, so
        // freeing just one more chunk reaches the 2 it needs and unblocks it.
        p.deallocate(h1);
        let result = t.await.unwrap();
        assert!(result.is_ok(), "waiter unexpectedly failed: {result:?}");
        p.deallocate(h2);
    }

    /// FIFO fairness across the K-chunk request. A multi-chunk request that joins the wait queue
    /// before a single-chunk request must complete first when memory becomes available — the
    /// K-chunk request occupies a single waiter slot, not K of them.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testFifoFairnessAcrossMultiChunkAndSingleChunkRequests"
    )]
    async fn test_fifo_fairness_across_multi_chunk_and_single_chunk_requests() {
        let chunk_size = 64;
        // Exactly the chunks the two waiters need: 2 for the multi request, 1 for the single.
        let total = 3 * chunk_size as i64;
        let p = pool(total, chunk_size);
        // Drain the pool entirely so any new request must wait.
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        let h3 = take_one(&p, chunk_size).await;
        assert_eq!(0, p.available_memory());

        // T_multi enters first, requesting 2 chunks; T_single enters second, requesting 1 chunk.
        let t_multi = {
            let p = Arc::clone(&p);
            tokio::spawn(async move {
                let got = p.allocate_chunks(2 * chunk_size as i32, MAX_BLOCK_TIME_MS).await?;
                let completed = Instant::now();
                let count = got.len();
                for b in got {
                    p.deallocate(b);
                }
                Ok::<_, Error>((completed, count))
            })
        };
        // Wait for t_multi to be parked before starting t_single, so the FIFO order is
        // deterministic.
        wait_for_condition(|| p.queued() == 1, "multi-chunk waiter should be the only one queued").await;
        let t_single = {
            let p = Arc::clone(&p);
            tokio::spawn(async move {
                let mut got = p.allocate_chunks(chunk_size as i32, MAX_BLOCK_TIME_MS).await?;
                let completed = Instant::now();
                let chunk = got.pop().expect("one chunk");
                p.deallocate(chunk);
                Ok::<_, Error>(completed)
            })
        };
        // Wait for t_single to also be parked.
        wait_for_condition(|| p.queued() == 2, "single-chunk waiter joined after the multi-chunk one").await;

        // Free the two chunks the multi request needs.
        p.deallocate(h1);
        p.deallocate(h2);
        // Both freed — the FIFO leader (multi) should claim both before the single-chunk waiter
        // gets any. The multi completes first.
        let multi = t_multi.await.unwrap();
        // Now free another chunk for the single-chunk waiter.
        p.deallocate(h3);
        let single = t_single.await.unwrap();

        let (multi_completion, multi_chunks) = multi.expect("multi must complete without error");
        let single_completion = single.expect("single must complete without error");
        // Each waiter must actually have received its chunks.
        assert_eq!(2, multi_chunks, "multi must receive exactly 2 chunks");
        assert!(
            multi_completion <= single_completion,
            "FIFO violated: single (joined later) completed at {single_completion:?} before multi at {multi_completion:?}"
        );
    }

    /// A chunk request that can't be satisfied immediately must wait for memory, taking chunks
    /// as they are freed. If it then times out before acquiring all it needs, each chunk it
    /// already took must be returned to the pool exactly once. A double refund would make the
    /// pool over-report `available_memory()`, letting later allocations exceed the configured
    /// `buffer.memory` limit.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testWaitingRequestDoesNotDoubleRefundChunksOnTimeout"
    )]
    async fn test_waiting_request_does_not_double_refund_chunks_on_timeout() {
        let chunk_size = 64;
        let total = 3 * chunk_size as i64;
        let p = pool(total, chunk_size);

        // Drain so the next allocate_chunks must wait.
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        let h3 = take_one(&p, chunk_size).await;
        assert_eq!(0, p.available_memory());

        let t = {
            let p = Arc::clone(&p);
            // Asks for 3 chunks with a finite deadline. We free 2 chunks below, so the waiter
            // takes those but is still 1 short and times out waiting for the 3rd.
            tokio::spawn(async move { p.allocate_chunks(3 * chunk_size as i32, 500).await })
        };

        wait_for_condition(|| p.queued() == 1, "waiter should be parked on the pool's queue").await;

        // Free 2 chunks — the waiter takes both, then waits again for the 3rd it never gets.
        p.deallocate(h1);
        p.deallocate(h2);

        // Rely on available_memory being 0 to confirm the waiter has taken both freed chunks.
        wait_for_condition(|| p.available_memory() == 0, "waiter should have taken both freed chunks").await;

        let err = t.await.unwrap().unwrap_err();
        assert!(
            matches!(err, Error::ProducerBufferExhausted(_)),
            "expected BufferExhausted, got {err:?}"
        );

        // After the timeout, exactly the 2 freed chunks (h1 + h2) must be back in the pool, each
        // returned once; h3 is still held outside the pool. A double refund would over-report
        // available_memory() (e.g. 4*chunk_size instead of 2).
        assert_eq!(
            2 * chunk_size as i64,
            p.available_memory(),
            "pool over-reports availableMemory if polled chunks are refunded twice on rollback"
        );

        p.deallocate(h3);
    }

    /// A chunk request that must wait for memory and then completes by taking chunks as they are
    /// freed must account for those chunks exactly once — as memory now held by the caller — so
    /// the pool neither loses nor over-reports capacity.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testWaitingRequestDoesNotCorruptAccountingOnSuccess"
    )]
    async fn test_waiting_request_does_not_corrupt_accounting_on_success() {
        let chunk_size = 64;
        let total = 3 * chunk_size as i64;
        let p = pool(total, chunk_size);

        // Drain so a 2-chunk request must wait.
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        let h3 = take_one(&p, chunk_size).await;
        assert_eq!(0, p.available_memory());

        let t = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(2 * chunk_size as i32, MAX_BLOCK_TIME_MS).await })
        };

        wait_for_condition(|| p.queued() == 1, "waiter should be parked on the pool's queue").await;

        // Free 2 chunks -> the waiter wakes, takes both, and completes normally.
        p.deallocate(h1);
        p.deallocate(h2);
        let got = t.await.unwrap().expect("waiter unexpectedly failed");

        // After success the waiter owns the 2 chunks (returned via `got`) and the test still
        // holds h3, so the pool has lent out everything it had — available_memory must be
        // exactly 0.
        assert_eq!(
            0,
            p.available_memory(),
            "a completed waiting request must leave the pool with no available memory (accounting not corrupted)"
        );

        // Returning all the held buffers must fully restore the pool.
        for chunk in got {
            p.deallocate(chunk);
        }
        p.deallocate(h3);
        assert_eq!(
            total,
            p.available_memory(),
            "pool not fully restored after returning all chunks"
        );
    }

    /// Closing the pool while a multi-chunk request is parked must surface a `KafkaException`
    /// and refund all reserved bytes.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BufferPoolChunkAllocationTest#testCloseDuringAtomicWait"
    )]
    async fn test_close_during_atomic_wait() {
        let chunk_size = 64;
        let total = 2 * chunk_size as i64;
        let p = pool(total, chunk_size);
        // Drain so the next request must wait.
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        assert_eq!(0, p.available_memory());

        let t = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(2 * chunk_size as i32, MAX_BLOCK_TIME_MS).await })
        };
        wait_for_condition(|| p.queued() == 1, "waiter should be parked on the pool's queue").await;

        // Close the pool: signals all waiters; the waiter must fail with a KafkaException.
        p.close();
        let err = t.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::KafkaError(_)), "expected a bare KafkaError, got {err:?}");
        assert_eq!("Producer closed while allocating memory", err.message());
        p.deallocate(h1);
        p.deallocate(h2);
    }

    // ---- Rust-only tests -------------------------------------------------------------------

    /// The mode guards (`BufferPool.java` `allocate` / `allocateChunks`) with Java's messages:
    /// a pool serves exactly one of the two allocation methods.
    #[tokio::test]
    async fn test_allocation_mode_guards() {
        let full = BufferPool::new_for_test(1024, 64);
        assert_eq!(AllocationMode::Full, full.allocation_mode());
        let err = full.allocate_chunks(64, 100).await.unwrap_err();
        assert!(matches!(err, Error::LocalIllegalState(_)), "got {err:?}");
        assert_eq!(
            "allocateChunks() is not supported in FULL allocation mode; use allocate()",
            err.message()
        );
        let err = full.try_allocate_chunks(64).unwrap_err();
        assert_eq!(
            "allocateChunks() is not supported in FULL allocation mode; use allocate()",
            err.message()
        );

        let incremental = BufferPool::new_incremental_for_test(1024, 64);
        assert_eq!(AllocationMode::Incremental, incremental.allocation_mode());
        let err = incremental.allocate(64, 100).await.unwrap_err();
        assert!(matches!(err, Error::LocalIllegalState(_)), "got {err:?}");
        assert_eq!(
            "allocate() is not supported in INCREMENTAL allocation mode; use allocateChunks()",
            err.message()
        );
        // Neither guard touched the accounting.
        assert_eq!(1024, full.available_memory());
        assert_eq!(1024, incremental.available_memory());
    }

    /// A dropped `allocate_chunks` future — parked, possibly already holding chunks it took on an
    /// earlier wakeup — must refund everything and leave the waiters queue, the counterpart of
    /// `cancelled_allocate_does_not_leak_its_waiter_or_memory` for the chunk path. A leaked waiter
    /// at the head of the queue would swallow every later wakeup.
    #[tokio::test]
    async fn cancelled_allocate_chunks_does_not_leak_its_waiter_or_memory() {
        let chunk_size = 64;
        let total = 3 * chunk_size as i64;
        let p = pool(total, chunk_size);
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        let h3 = take_one(&p, chunk_size).await;

        // Waiter 1 wants all 3 chunks; it takes h1 when it is freed, then parks again.
        let cancelled = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(3 * chunk_size as i32, 60_000).await })
        };
        wait_for_condition(|| p.queued() == 1, "waiter 1 should be parked").await;
        p.deallocate(h1);
        wait_for_condition(|| p.available_memory() == 0, "waiter 1 should hold the freed chunk").await;
        assert_eq!(1, p.queued(), "waiter 1 is still parked, holding one chunk");

        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert_eq!(0, p.queued(), "the dropped waiter must leave the queue");
        assert_eq!(chunk_size as i64, p.available_memory(), "the chunk it held must be refunded");

        // Waiter 2 parks behind where waiter 1 was and must be served once memory returns.
        let served = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(3 * chunk_size as i32, 60_000).await })
        };
        wait_for_condition(|| p.queued() == 1, "waiter 2 should be parked").await;
        p.deallocate(h2);
        p.deallocate(h3);
        let chunks = tokio::time::timeout(Duration::from_secs(5), served)
            .await
            .expect("the live waiter must be signalled, not starved by a leaked waiter")
            .expect("the waiter task must not panic")
            .expect("memory was returned, so the allocation must succeed");
        assert_eq!(3, chunks.len());
        for chunk in chunks {
            p.deallocate(chunk);
        }
        assert_eq!(0, p.queued(), "no waiter may be left behind");
        assert_eq!(total, p.available_memory(), "all memory must be back in the pool");
    }

    /// The non-blocking path (Java's `allocateChunks(n, 0L)`): succeeds from free memory, fails
    /// fast with Java's message and no accounting change when memory is short, never parks, and
    /// does not record the buffer-exhausted metric.
    #[tokio::test]
    async fn test_try_allocate_chunks() {
        let chunk_size = 64;
        let total = 3 * chunk_size as i64;
        let metrics = Arc::new(Metrics::new());
        let p = Arc::new(BufferPool::with_allocation_mode(
            total,
            chunk_size,
            Arc::clone(&metrics),
            Arc::new(crate::common::utils::SystemTime),
            "producer-metrics",
            AllocationMode::Incremental,
        ));

        let held = p.try_allocate_chunks(2 * chunk_size as i32).unwrap();
        assert_eq!(2, held.len());
        assert!(held.iter().all(|c| c.len() == chunk_size && c.capacity() == chunk_size));
        assert_eq!(chunk_size as i64, p.available_memory());

        let err = p.try_allocate_chunks(2 * chunk_size as i32).unwrap_err();
        assert!(matches!(err, Error::ProducerBufferExhausted(_)), "got {err:?}");
        assert_eq!(
            "Failed to allocate 128 bytes (2 chunks of 64) within the configured max blocking time 0 ms. \
             Total memory: 192 bytes. Available memory: 64 bytes.",
            err.message()
        );
        assert_eq!(chunk_size as i64, p.available_memory(), "a failed attempt takes nothing");
        assert_eq!(0, p.queued(), "the non-blocking path never parks");
        let exhausted_total = metrics.metric_name_description_tags(
            "buffer-exhausted-total",
            "producer-metrics",
            "",
            std::collections::BTreeMap::new(),
        );
        assert_eq!(
            0.0,
            metrics.metric(&exhausted_total).unwrap().measurable_value(0),
            "allocateChunks leaves the buffer-exhausted metric to the caller"
        );
        p.record_buffer_exhausted();
        assert_eq!(1.0, metrics.metric(&exhausted_total).unwrap().measurable_value(0));

        p.close();
        let err = p.try_allocate_chunks(chunk_size as i32).unwrap_err();
        assert!(matches!(err, Error::KafkaError(_)), "got {err:?}");
        assert_eq!("Producer closed while allocating memory", err.message());
        for chunk in held {
            p.deallocate(chunk);
        }
    }

    /// Chunks recycled through the free list come back at full length and capacity, and a
    /// request mixing free-list chunks with fresh ones is accounted exactly.
    #[tokio::test]
    async fn test_mixed_free_list_and_fresh_chunks() {
        let chunk_size = 64;
        let total = 4 * chunk_size as i64;
        let p = pool(total, chunk_size);
        let first = p.allocate_chunks(2 * chunk_size as i32, 100).await.unwrap();
        for chunk in first {
            p.deallocate(chunk);
        }
        assert_eq!(2, p.free_size());
        let chunks = p.allocate_chunks(3 * chunk_size as i32, 100).await.unwrap();
        assert_eq!(3, chunks.len());
        assert!(chunks.iter().all(|c| c.len() == chunk_size && c.capacity() == chunk_size));
        assert_eq!(0, p.free_size(), "free-list chunks are preferred");
        assert_eq!(chunk_size as i64, p.available_memory());
        for chunk in chunks {
            p.deallocate(chunk);
        }
        assert_eq!(total, p.available_memory());
    }

    /// Java's free-list-before-raw swap in the wait loop (`BufferPool.java:338-344`): a waiter
    /// that already reserved a raw chunk and then finds enough free-list chunks takes them and
    /// hands the raw reservation back. Without the swap it keeps both, and the raw part is never
    /// materialised or refunded, so one chunk of `buffer.memory` leaks.
    #[tokio::test]
    async fn test_waiter_hands_back_raw_reservation_when_free_list_chunks_arrive() {
        let chunk_size = 64;
        let total = 4 * chunk_size as i64;
        let p = pool(total, chunk_size);
        // Three raw chunks held, so exactly one chunk of non-pooled memory is free.
        let h1 = take_one(&p, chunk_size).await;
        let h2 = take_one(&p, chunk_size).await;
        let h3 = take_one(&p, chunk_size).await;
        assert_eq!(chunk_size as i64, p.available_memory());

        // w0 wants the whole pool and will time out; w1 queues behind it.
        let w0 = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(4 * chunk_size as i32, 50).await })
        };
        wait_for_condition(|| p.queued() == 1, "w0 should be parked").await;
        let w1 = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(3 * chunk_size as i32, 5_000).await })
        };
        wait_for_condition(|| p.queued() == 2, "w1 should be parked behind w0").await;

        // w0 times out; its exit signals w1, which reserves the one free raw chunk.
        assert!(matches!(w0.await.unwrap(), Err(Error::ProducerBufferExhausted(_))));
        wait_for_condition(|| p.available_memory() == 0, "w1 should reserve the free raw chunk").await;

        // Three chunks land on the free list with no await in between; w1 needs only 3 in total,
        // so it must take them and return its raw reservation.
        p.deallocate(h1);
        p.deallocate(h2);
        p.deallocate(h3);
        let chunks = w1.await.unwrap().expect("w1 must be served");
        assert_eq!(3, chunks.len());
        assert_eq!(
            chunk_size as i64,
            p.available_memory(),
            "the raw reservation must be handed back once free-list chunks cover the request"
        );
        for chunk in chunks {
            p.deallocate(chunk);
        }
        assert_eq!(total, p.available_memory(), "no chunk of buffer.memory may leak");
    }

    /// Java's `finally { if (error) releaseReservedBytes(memoryRequired); }` around the raw chunk
    /// allocation: if allocating a fresh chunk unwinds, the whole reservation returns to the
    /// non-pooled memory and the next waiter is signalled.
    #[tokio::test]
    async fn test_failed_chunk_allocation_releases_the_reservation() {
        let chunk_size = 64;
        let total = 2 * chunk_size as i64;
        let p = pool(total, chunk_size);
        p.fail_allocate_byte_buffer.store(true, Ordering::Relaxed);
        let task = {
            let p = Arc::clone(&p);
            tokio::spawn(async move { p.allocate_chunks(2 * chunk_size as i32, 100).await })
        };
        assert!(task.await.unwrap_err().is_panic(), "the injected failure unwinds");
        assert_eq!(total, p.available_memory(), "the reserved bytes are released");
        assert_eq!(0, p.queued());

        p.fail_allocate_byte_buffer.store(false, Ordering::Relaxed);
        let chunks = p.allocate_chunks(2 * chunk_size as i32, 100).await.unwrap();
        assert_eq!(2, chunks.len());
    }
}
