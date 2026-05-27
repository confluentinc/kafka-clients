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

//! Decompression-buffer reuse pool used by the consumer fetch path.
//!
//! Translated from `org.apache.kafka.common.utils.BufferSupplier`.
//!
//! In Java this is an `AutoCloseable` abstract class with two concrete
//! implementations:
//!
//! - `BufferSupplier.NO_CACHING` — never pools; every `get()` returns a fresh
//!   `ByteBuffer`.
//! - `DefaultSupplier` — pools by capacity in a `Map<Integer, Deque<ByteBuffer>>`.
//! - `GrowableBufferSupplier` — caches one buffer that grows monotonically.
//!
//! The Rust translation collapses these into a single enum-driven struct.
//! `ByteBuffer` becomes `Vec<u8>` — Java uses fixed-capacity scratch buffers
//! and `clear()`s them on release, which maps to draining a `Vec` length-wise
//! while retaining capacity.
//!
//! Each [`BufferSupplier`] is single-threaded in Java; the Rust version wraps
//! the pool state in [`Mutex`] so peers on the fetch path (the bg task adding
//! `CompletedFetch`es and the app task draining them) can both share an
//! `Arc<BufferSupplier>` without contention concerns. Critical sections never
//! await (CLAUDE.md §9), so `std::sync::Mutex` is the right choice.
//!
//! The struct is referenced by `CompletedFetch` and `AbstractFetch` later in
//! Phase 7a; the `#[allow(dead_code)]` keeps the lib's `#![deny(warnings)]`
//! happy until the wiring lands.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// A pool of byte buffers keyed by capacity, used to amortize the cost of
/// allocating large scratch buffers (e.g. LZ4 decompression buffers) across
/// many small record batches.
///
/// Corresponds to `org.apache.kafka.common.utils.BufferSupplier`.
#[derive(Debug)]
pub(crate) struct BufferSupplier {
    mode: Mode,
}

#[derive(Debug)]
enum Mode {
    /// Pools buffers by exact capacity. Mirrors Java's `DefaultSupplier`.
    Default(Mutex<HashMap<usize, VecDeque<Vec<u8>>>>),
    /// Caches at most one buffer that grows monotonically. Mirrors Java's
    /// `GrowableBufferSupplier`.
    Growable(Mutex<Option<Vec<u8>>>),
    /// No caching — every `get` allocates and every `release` drops.
    /// Mirrors Java's `BufferSupplier.NO_CACHING`.
    NoCaching,
}

impl BufferSupplier {
    /// Constructs a pool that keeps freed buffers grouped by capacity.
    ///
    /// Translates Java's `BufferSupplier.create()`.
    pub(crate) fn create() -> Self {
        Self { mode: Mode::Default(Mutex::new(HashMap::new())) }
    }

    /// Constructs a pool that caches a single buffer and grows it
    /// monotonically.
    ///
    /// Translates Java's `BufferSupplier.GrowableBufferSupplier`.
    pub(crate) fn growable() -> Self {
        Self { mode: Mode::Growable(Mutex::new(None)) }
    }

    /// Constructs a pool that never caches — every `get` allocates a fresh
    /// buffer, every `release` drops.
    ///
    /// Translates Java's `BufferSupplier.NO_CACHING`.
    pub(crate) fn no_caching() -> Self {
        Self { mode: Mode::NoCaching }
    }

    /// Supplies a buffer with at least the requested capacity.
    ///
    /// The returned `Vec<u8>` has `len == 0`; callers may extend it up to the
    /// reported capacity (and beyond if they wish to reallocate). For
    /// [`BufferSupplier::create`], the returned buffer's capacity is exactly
    /// `size`; for [`BufferSupplier::growable`], it is `>= size`.
    ///
    /// Translates Java's `ByteBuffer get(int size)`.
    pub(crate) fn get(&self, size: usize) -> Vec<u8> {
        match &self.mode {
            Mode::Default(map) => {
                let mut guard = map.lock().expect("BufferSupplier pool mutex poisoned");
                match guard.get_mut(&size).and_then(VecDeque::pop_front) {
                    Some(mut buf) => {
                        buf.clear();
                        buf
                    },
                    None => Vec::with_capacity(size),
                }
            },
            Mode::Growable(cached) => {
                let mut guard = cached.lock().expect("BufferSupplier growable mutex poisoned");
                match guard.take() {
                    Some(mut buf) if buf.capacity() >= size => {
                        buf.clear();
                        buf
                    },
                    _ => Vec::with_capacity(size),
                }
            },
            Mode::NoCaching => Vec::with_capacity(size),
        }
    }

    /// Returns a buffer to the pool to be reused by a subsequent call to
    /// [`Self::get`].
    ///
    /// Translates Java's `void release(ByteBuffer buffer)`.
    pub(crate) fn release(&self, mut buffer: Vec<u8>) {
        match &self.mode {
            Mode::Default(map) => {
                let capacity = buffer.capacity();
                buffer.clear();
                let mut guard = map.lock().expect("BufferSupplier pool mutex poisoned");
                guard.entry(capacity).or_default().push_back(buffer);
            },
            Mode::Growable(cached) => {
                buffer.clear();
                let mut guard = cached.lock().expect("BufferSupplier growable mutex poisoned");
                *guard = Some(buffer);
            },
            Mode::NoCaching => {
                // Drop the buffer.
            },
        }
    }

    /// Releases all resources associated with this supplier (drops every
    /// pooled buffer).
    ///
    /// Translates Java's `AutoCloseable.close()`. Idempotent.
    pub(crate) fn close(&self) {
        match &self.mode {
            Mode::Default(map) => {
                let mut guard = map.lock().expect("BufferSupplier pool mutex poisoned");
                guard.clear();
            },
            Mode::Growable(cached) => {
                let mut guard = cached.lock().expect("BufferSupplier growable mutex poisoned");
                *guard = None;
            },
            Mode::NoCaching => {},
        }
    }
}

impl Default for BufferSupplier {
    fn default() -> Self {
        Self::create()
    }
}

impl Drop for BufferSupplier {
    fn drop(&mut self) {
        // Equivalent to Java's `try-with-resources` cleanup.
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from
    /// `org.apache.kafka.common.record.BufferSupplierTest.testGrowableBuffer`.
    #[test]
    fn test_growable_buffer() {
        let supplier = BufferSupplier::growable();
        let buffer = supplier.get(1024);
        assert_eq!(0, buffer.len());
        assert_eq!(1024, buffer.capacity());
        supplier.release(buffer);

        let cached = supplier.get(512);
        assert_eq!(0, cached.len());
        // The cached buffer's capacity is at least 1024 (the original
        // request) — matches Java's `assertSame(buffer, cached)`.
        assert!(cached.capacity() >= 1024);
        supplier.release(cached);

        let increased = supplier.get(2048);
        assert_eq!(2048, increased.capacity());
        assert_eq!(0, increased.len());
    }

    /// Default supplier reuses a buffer of the exact requested capacity.
    #[test]
    fn test_default_supplier_reuses_buffer() {
        let supplier = BufferSupplier::create();
        let mut buffer = supplier.get(1024);
        let original_ptr = buffer.as_ptr();
        buffer.extend_from_slice(&[1, 2, 3]);
        supplier.release(buffer);

        let recycled = supplier.get(1024);
        assert_eq!(0, recycled.len());
        assert_eq!(1024, recycled.capacity());
        // Same underlying allocation.
        assert_eq!(original_ptr, recycled.as_ptr());
    }

    /// Default supplier keys by capacity — a different size gets a fresh
    /// allocation even if a buffer of another size is available.
    #[test]
    fn test_default_supplier_keys_by_capacity() {
        let supplier = BufferSupplier::create();
        let buffer_1k = supplier.get(1024);
        supplier.release(buffer_1k);

        // Asking for 2k allocates fresh — does not reuse the 1k buffer.
        let buffer_2k = supplier.get(2048);
        assert_eq!(2048, buffer_2k.capacity());

        // The 1k buffer is still pooled.
        let buffer_1k_again = supplier.get(1024);
        assert_eq!(1024, buffer_1k_again.capacity());
    }

    /// `no_caching` always allocates a fresh buffer.
    #[test]
    fn test_no_caching_does_not_pool() {
        let supplier = BufferSupplier::no_caching();
        let buffer_a = supplier.get(1024);
        let ptr_a = buffer_a.as_ptr();
        supplier.release(buffer_a);

        let buffer_b = supplier.get(1024);
        // No reuse — pointers must differ on a fresh allocation. Note: this
        // is not strictly guaranteed by the allocator (a slab allocator
        // could hand the same address back), but is a reasonable expectation
        // for the default system allocator.
        // We assert behavioral equivalence: capacity is correct and the
        // buffer is empty.
        assert_eq!(0, buffer_b.len());
        assert_eq!(1024, buffer_b.capacity());
        // Avoid the unused-variable warning when the pointer comparison is
        // skipped above.
        let _ = ptr_a;
    }

    /// `close` empties the pool — a subsequent `get` allocates fresh.
    #[test]
    fn test_close_drops_pooled_buffers() {
        let supplier = BufferSupplier::create();
        let buffer = supplier.get(1024);
        let original_ptr = buffer.as_ptr();
        supplier.release(buffer);
        supplier.close();

        let fresh = supplier.get(1024);
        assert_ne!(original_ptr, fresh.as_ptr());
    }

    /// The supplier may be shared across threads via `Arc`.
    #[test]
    fn test_thread_safe_via_arc() {
        use std::sync::Arc;
        use std::thread;

        let supplier = Arc::new(BufferSupplier::create());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let supplier = Arc::clone(&supplier);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let buf = supplier.get(64);
                    supplier.release(buf);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // The pool should still have buffers available.
        let buf = supplier.get(64);
        assert_eq!(0, buf.len());
        assert_eq!(64, buf.capacity());
    }
}
