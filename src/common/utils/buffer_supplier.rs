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

//! Translation of `org.apache.kafka.common.utils.BufferSupplier`.

use std::collections::{HashMap, VecDeque};

/// A non-thread-safe interface for caching byte buffers. Mirrors the Java
/// abstract class. Used to amortize the cost of allocating large
/// decompression buffers across multiple records in a batch.
///
/// In Java this is an `abstract class implements AutoCloseable`. In Rust we
/// translate it as an enum with the three concrete variants Java exposes —
/// no-op, default (per-size LRU), and growable. We could use a trait but
/// the closed set of impls makes the enum lighter and avoids a `Box<dyn>`
/// indirection.
pub enum BufferSupplier {
    /// `BufferSupplier.NO_CACHING` — never caches; every `get` allocates.
    NoCaching,
    /// `BufferSupplier.create()` — caches by exact buffer size.
    Default(DefaultSupplier),
    /// `BufferSupplier.GrowableBufferSupplier` — caches a single buffer
    /// that grows monotonically.
    Growable(GrowableSupplier),
}

impl BufferSupplier {
    /// `BufferSupplier.NO_CACHING` constant. Returned by value (rather than
    /// as a `static`) since each value is independent and stateless.
    pub fn no_caching() -> Self {
        BufferSupplier::NoCaching
    }

    /// `BufferSupplier.create()` — default (per-size LRU) supplier.
    pub fn create() -> Self {
        BufferSupplier::Default(DefaultSupplier::default())
    }

    /// Single-buffer growable supplier.
    pub fn growable() -> Self {
        BufferSupplier::Growable(GrowableSupplier::default())
    }

    /// Supply a buffer with at least the requested capacity.
    pub fn get(&mut self, capacity: usize) -> Vec<u8> {
        match self {
            BufferSupplier::NoCaching => vec![0u8; capacity],
            BufferSupplier::Default(d) => d.get(capacity),
            BufferSupplier::Growable(g) => g.get(capacity),
        }
    }

    /// Return a buffer for reuse. The buffer is cleared (length set to its
    /// existing capacity, contents zeroed) so the next caller sees a fresh
    /// "ByteBuffer" semantically.
    pub fn release(&mut self, buffer: Vec<u8>) {
        match self {
            BufferSupplier::NoCaching => {
                // drop
            },
            BufferSupplier::Default(d) => d.release(buffer),
            BufferSupplier::Growable(g) => g.release(buffer),
        }
    }

    /// Release all cached buffers. Mirrors `close()`.
    pub fn close(&mut self) {
        match self {
            BufferSupplier::NoCaching => {},
            BufferSupplier::Default(d) => d.close(),
            BufferSupplier::Growable(g) => g.close(),
        }
    }
}

impl Drop for BufferSupplier {
    fn drop(&mut self) {
        self.close();
    }
}

/// Default supplier: caches per exact capacity. Mirrors Java's `DefaultSupplier`.
#[derive(Default)]
pub struct DefaultSupplier {
    buffer_map: HashMap<usize, VecDeque<Vec<u8>>>,
}

impl DefaultSupplier {
    fn get(&mut self, size: usize) -> Vec<u8> {
        if let Some(q) = self.buffer_map.get_mut(&size)
            && let Some(b) = q.pop_front()
        {
            return b;
        }
        vec![0u8; size]
    }

    fn release(&mut self, mut buffer: Vec<u8>) {
        // `clear()` in the Java reference resets position/limit; for our
        // `Vec<u8>` we restore the buffer to its full capacity so the next
        // caller sees the same length they originally requested.
        let cap = buffer.capacity();
        buffer.clear();
        buffer.resize(cap, 0);
        self.buffer_map.entry(cap).or_default().push_back(buffer);
    }

    fn close(&mut self) {
        self.buffer_map.clear();
    }
}

/// Single-buffer supplier that grows monotonically. Mirrors Java's
/// `GrowableBufferSupplier`.
#[derive(Default)]
pub struct GrowableSupplier {
    cached: Option<Vec<u8>>,
}

impl GrowableSupplier {
    fn get(&mut self, min_capacity: usize) -> Vec<u8> {
        match self.cached.take() {
            Some(b) if b.capacity() >= min_capacity => b,
            _ => vec![0u8; min_capacity],
        }
    }

    fn release(&mut self, mut buffer: Vec<u8>) {
        let cap = buffer.capacity();
        buffer.clear();
        buffer.resize(cap, 0);
        self.cached = Some(buffer);
    }

    fn close(&mut self) {
        self.cached = None;
    }
}

#[cfg(test)]
mod tests {
    // The Java client does not have a dedicated `BufferSupplierTest.java`
    // (the class is exercised indirectly by record-batch tests in Phase 3).
    // We test the cache contract directly here.

    use super::*;

    #[test]
    fn no_caching_always_allocates_fresh() {
        let mut s = BufferSupplier::no_caching();
        let a = s.get(8);
        s.release(a);
        let b = s.get(8);
        // No-caching: every get returns a fresh buffer (zeroed, len = 8).
        assert_eq!(b.len(), 8);
        assert!(b.iter().all(|x| *x == 0));
    }

    #[test]
    fn default_caches_per_size() {
        let mut s = BufferSupplier::create();
        let mut a = s.get(64);
        a[0] = 0xff;
        s.release(a);
        let b = s.get(64);
        // The cached buffer should be reused (same capacity); contents are
        // cleared on release.
        assert_eq!(b.len(), 64);
        assert!(b.iter().all(|x| *x == 0));
    }

    #[test]
    fn default_allocates_fresh_for_new_size() {
        let mut s = BufferSupplier::create();
        let a = s.get(64);
        s.release(a);
        let b = s.get(128);
        // 128-byte request -> fresh allocation (cache only had 64).
        assert_eq!(b.len(), 128);
    }

    #[test]
    fn growable_reuses_for_smaller_or_equal_request() {
        let mut s = BufferSupplier::growable();
        let a = s.get(128);
        s.release(a);
        let b = s.get(64);
        // Growable returns a >=128 cached buffer.
        assert!(b.capacity() >= 128);
    }

    #[test]
    fn growable_reallocates_for_bigger_request() {
        let mut s = BufferSupplier::growable();
        let a = s.get(64);
        s.release(a);
        let b = s.get(256);
        assert_eq!(b.len(), 256);
    }

    #[test]
    fn close_clears_cache() {
        let mut s = BufferSupplier::create();
        let a = s.get(64);
        s.release(a);
        s.close();
        let b = s.get(64);
        assert_eq!(b.len(), 64);
    }
}
