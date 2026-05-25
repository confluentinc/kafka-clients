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

//! Consumer interceptor plugin trait.
//!
//! Translated from `org.apache.kafka.clients.consumer.ConsumerInterceptor`.

use std::collections::HashMap;

use crate::common::TopicPartition;
use crate::consumer::{ConsumerRecords, OffsetAndMetadata};

/// A plugin interface that allows you to intercept (and possibly mutate)
/// records received by the consumer. A primary use-case is for third-party
/// components to hook into the consumer applications for custom monitoring,
/// logging, etc.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.ConsumerInterceptor<K, V>`.
///
/// # Sync, not async
///
/// Java's `onConsume` and `onCommit` are synchronous; this trait mirrors
/// that exactly. Per CLAUDE.md §11, the interceptor chain runs per-batch
/// inside `poll()` — not on a hot per-record path — so per-call boxed-dyn
/// dispatch is acceptable, but `#[async_trait]` is forbidden because
/// Java's API does not allow asynchrony here and adding it would change
/// the contract.
///
/// # Bounds: `Send + 'static`, NOT `Send + Sync`
///
/// The interceptor is stored inside `ConsumerInterceptors` as
/// `Vec<Box<dyn ConsumerInterceptor<K, V>>>` with a single owner (the
/// `ConsumerInterceptors` container on the app side). No `Arc<dyn>` or
/// shared `&dyn` storage exists across tasks, so `Sync` is not required.
/// Dropping `Sync` lets users use interior mutability (`RefCell`, `Cell`)
/// inside their interceptors without wrapping in a `Mutex`.
///
/// # Exception model
///
/// Java catches `Exception` thrown by interceptors, logs at WARN, and
/// continues calling the next interceptor with the last successful
/// `records` value. The Rust analog: `ConsumerInterceptors` wraps each
/// call in `std::panic::catch_unwind` and passes the previous-good batch
/// to the next interceptor on panic.
pub trait ConsumerInterceptor<K, V>: Send + 'static {
    /// Called just before records are returned by
    /// [`Consumer::poll`](crate::consumer::Consumer::poll).
    ///
    /// This method is allowed to modify the consumer records in place.
    /// There is no limitation on the number of records that could be left
    /// in `records` when the method returns; the interceptor can filter
    /// records, generate new ones, or replace the batch entirely.
    ///
    /// Corresponds to Java's
    /// `ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records)`.
    ///
    /// # Replacing the batch
    ///
    /// Java's `onConsume` returns a (possibly different) `ConsumerRecords`
    /// reference, which lets interceptors swap the batch wholesale. The
    /// Rust translation expresses the same operation via in-place
    /// reassignment: `*records = new_batch;`.
    ///
    /// The `&mut` form (rather than ownership transfer) avoids a
    /// `K: Clone, V: Clone` bound that would otherwise propagate through
    /// `ConsumerInterceptors::on_consume` and into the public
    /// `Consumer<K, V>` trait — Java imposes no `Cloneable` constraint on
    /// keys/values.
    ///
    /// # Panic safety
    ///
    /// Java's `ConsumerRecords` is **structurally immutable** — its
    /// fields are `private final` and the maps/lists exposed by its
    /// getters are unmodifiable views (see
    /// `ConsumerRecords.java:37-50`). An interceptor that throws
    /// **cannot** corrupt the input batch in Java; the "next interceptor
    /// receives the previous-good batch" guarantee
    /// (`ConsumerInterceptor.java:63-65`) is a structural property of
    /// the input type, not a contract on interceptor implementations.
    ///
    /// The Rust `&mut ConsumerRecords` form cannot reproduce that
    /// structural immutability — `&mut` permits mutation, and any
    /// partial write committed before a panic is observable to the next
    /// interceptor. The chain still catches the panic and continues to
    /// the next interceptor (mirroring Java's
    /// `ConsumerInterceptors.java:60-61`), but matching Java's
    /// "previous-good batch" guarantee becomes an **interceptor-side
    /// discipline** rather than a structural property.
    ///
    /// ## Recommended pattern: build off to the side, commit at the end
    ///
    /// ```text
    /// fn on_consume(&self, records: &mut ConsumerRecords<K, V>) {
    ///     // Phase 1: read-only inspection / construction.
    ///     let new_batch = build_replacement(records);  // may panic
    ///
    ///     // Phase 2: single non-fallible commit.
    ///     *records = new_batch;
    /// }
    /// ```
    ///
    /// A panic anywhere in Phase 1 leaves `*records` unchanged because
    /// the assignment is the only place we write to the borrow, and that
    /// assignment cannot panic. This pattern reproduces Java's
    /// "previous-good batch" guarantee for the common case.
    ///
    /// ## Anti-pattern: `std::mem::take` followed by fallible logic
    ///
    /// ```text
    /// // DO NOT do this:
    /// let owned = std::mem::take(records);           // (a) *records is now empty
    /// let new_batch = build_from_owned(owned);       // (b) MAY PANIC
    /// *records = new_batch;                          // (c) only reached on success
    /// ```
    ///
    /// `std::mem::take` writes `ConsumerRecords::default()` (an empty
    /// batch) to `*records` at step (a). If step (b) panics, the next
    /// interceptor sees an **empty** batch — not the previous-good
    /// batch. This is the failure mode Java does not have. If ownership
    /// transfer is unavoidable, sequence it so that nothing fallible
    /// runs between the take and the commit assignment.
    ///
    /// ## Implementation-defined: interior mutability + partial writes
    ///
    /// Even with the recommended pattern, interceptors using interior
    /// mutability (`RefCell`, `Cell`, atomics, `Mutex`) are responsible
    /// for their own state consistency on panic. A panic mid-mutation
    /// of an interior-mutable field leaves that field in whatever state
    /// the interceptor produced — undefined-by-the-framework. Java has
    /// the same caveat (its `synchronized` and atomic state are likewise
    /// the interceptor's responsibility).
    fn on_consume(&self, records: &mut ConsumerRecords<K, V>);

    /// Called when offsets get committed.
    ///
    /// Corresponds to Java's
    /// `void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    ///
    /// Takes `&HashMap` because Java's signature is also read-only for the
    /// interceptor (interceptors should not mutate the commit map; if they
    /// do, behavior is undefined in Java too).
    fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>);

    /// Configure this interceptor. The default implementation is a no-op.
    ///
    /// Corresponds to Java's `default void configure(Map<String, ?> configs)`.
    fn configure(&mut self, _configs: &HashMap<String, String>) {}

    /// Close this interceptor. The default implementation is a no-op.
    ///
    /// Corresponds to Java's `void close()`.
    fn close(&mut self) {}
}
