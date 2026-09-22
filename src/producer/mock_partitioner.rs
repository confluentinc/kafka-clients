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

//! A fake partitioner for testing purposes.
//!
//! Translated from `org.apache.kafka.test.MockPartitioner`.

use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::common::Cluster;
use crate::producer::Partitioner;

/// Backs [`MockPartitioner::init_count`]. Private to this module: CLAUDE.md §2
/// exports a Java static only through the struct that declares it, and Rust has
/// no associated `static`, so the item is reached through the accessor — the
/// `Node::no_node` precedent.
static INIT_COUNT: AtomicI32 = AtomicI32::new(0);

/// Backs [`MockPartitioner::close_count`]; private for the reason above.
static CLOSE_COUNT: AtomicI32 = AtomicI32::new(0);

/// Serializes every test that reads or writes the process-global
/// [`MockPartitioner::init_count`] / [`MockPartitioner::close_count`].
///
/// Java's counters are `static`, and Java runs the `MockPartitioner` tests
/// sequentially; `cargo test`, by contrast, runs test functions on parallel
/// threads, so two tests each resetting and asserting on these counters would
/// race. Every test that touches the counters MUST hold this guard for its
/// whole body — acquire it via [`MockPartitioner::lock_counters`], then
/// [`MockPartitioner::reset_counters`], then exercise the code under test.
static COUNTER_GUARD: Mutex<()> = Mutex::new(());

/// A [`Partitioner`] that always returns partition `0` and counts its
/// construction and close calls, for use in producer tests.
pub(crate) struct MockPartitioner;

impl MockPartitioner {
    /// Create a new `MockPartitioner`, incrementing [`Self::init_count`].
    ///
    /// Java's constructor calls `INIT_COUNT.incrementAndGet()`.
    pub(crate) fn new() -> Self {
        INIT_COUNT.fetch_add(1, Ordering::SeqCst);
        MockPartitioner
    }

    /// Number of times a `MockPartitioner` has been constructed.
    ///
    /// Java: `public static final AtomicInteger INIT_COUNT`.
    pub(crate) fn init_count() -> &'static AtomicI32 {
        &INIT_COUNT
    }

    /// Number of times a `MockPartitioner` has been closed.
    ///
    /// Java: `public static final AtomicInteger CLOSE_COUNT`.
    pub(crate) fn close_count() -> &'static AtomicI32 {
        &CLOSE_COUNT
    }

    /// Acquire the [`COUNTER_GUARD`] for the duration of a counter-touching test.
    ///
    /// Poison is tolerated (`into_inner`): if one counter test fails while
    /// holding the guard, the others still acquire it and report their own
    /// results rather than all failing with a confusing `PoisonError` that hides
    /// which test actually failed.
    pub(crate) fn lock_counters() -> MutexGuard<'static, ()> {
        COUNTER_GUARD.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reset both counters to zero.
    ///
    /// Java: `public static void resetCounters()`.
    pub(crate) fn reset_counters() {
        INIT_COUNT.store(0, Ordering::SeqCst);
        CLOSE_COUNT.store(0, Ordering::SeqCst);
    }
}

impl<K, V> Partitioner<K, V> for MockPartitioner {
    // Java's `configure` is an empty override; the trait's default no-op covers
    // it, so it is intentionally not overridden here.

    fn partition(
        &self,
        _topic: &str,
        _key: Option<&K>,
        _key_bytes: Option<&[u8]>,
        _value: Option<&V>,
        _value_bytes: Option<&[u8]>,
        _cluster: &Cluster,
    ) -> i32 {
        0
    }

    fn close(&self) {
        CLOSE_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity check of the counter/close semantics used by the producer tests.
    #[test]
    fn test_counts_init_and_close() {
        let _guard = MockPartitioner::lock_counters();
        MockPartitioner::reset_counters();

        let p = MockPartitioner::new();
        let p2 = MockPartitioner::new();
        assert_eq!(2, MockPartitioner::init_count().load(Ordering::SeqCst));
        assert_eq!(0, MockPartitioner::close_count().load(Ordering::SeqCst));

        // `partition` always returns 0 regardless of arguments.
        let cluster = Cluster::empty();
        assert_eq!(
            0,
            Partitioner::<String, String>::partition(&p, "any-topic", None, None, None, None, &cluster)
        );

        Partitioner::<String, String>::close(&p);
        Partitioner::<String, String>::close(&p2);
        assert_eq!(2, MockPartitioner::close_count().load(Ordering::SeqCst));

        MockPartitioner::reset_counters();
        assert_eq!(0, MockPartitioner::init_count().load(Ordering::SeqCst));
        assert_eq!(0, MockPartitioner::close_count().load(Ordering::SeqCst));
    }
}
