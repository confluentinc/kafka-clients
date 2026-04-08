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

//! A common memory pool interface for non-blocking pools.
//!
//! Translated from `org.apache.kafka.common.memory.MemoryPool`.
//!
//! Every buffer returned from [`try_allocate`](MemoryPool::try_allocate) must always be
//! [`release`](MemoryPool::release)d.

use std::fmt;

/// A common memory pool interface for non-blocking pools.
///
/// Translated from the Java `MemoryPool` interface.
pub trait MemoryPool: Send {
    /// Tries to acquire a buffer of the specified size.
    ///
    /// Returns `Some(Vec<u8>)` with the buffer, or `None` if no memory is available.
    /// The buffer will be of the exact size requested, even if backed by a larger
    /// chunk of memory.
    fn try_allocate(&self, size_bytes: usize) -> Option<Vec<u8>>;

    /// Returns a previously allocated buffer to the pool.
    fn release(&self, previously_allocated: Vec<u8>);

    /// Returns the total size of this pool in bytes.
    fn size(&self) -> i64;

    /// Returns the amount of memory available for allocation by this pool.
    ///
    /// Note: result may be negative (pools may over-allocate to avoid starvation issues).
    fn available_memory(&self) -> i64;

    /// Returns `true` if the pool cannot currently allocate any more buffers,
    /// meaning total outstanding buffers meets or exceeds pool size and some
    /// would need to be released before further allocations are possible.
    ///
    /// This is equivalent to `available_memory() <= 0`.
    fn is_out_of_memory(&self) -> bool;
}

/// A no-op memory pool that always allocates from the heap.
///
/// This corresponds to Java's `MemoryPool.NONE` — the default pool used by clients
/// where memory pooling is not needed.
#[derive(Debug)]
pub struct NoopMemoryPool;

impl MemoryPool for NoopMemoryPool {
    fn try_allocate(&self, size_bytes: usize) -> Option<Vec<u8>> {
        Some(vec![0u8; size_bytes])
    }

    fn release(&self, _previously_allocated: Vec<u8>) {
        // no-op: memory is freed when the Vec is dropped
    }

    fn size(&self) -> i64 {
        i64::MAX
    }

    fn available_memory(&self) -> i64 {
        i64::MAX
    }

    fn is_out_of_memory(&self) -> bool {
        false
    }
}

impl fmt::Display for NoopMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NONE")
    }
}
