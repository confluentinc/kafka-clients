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

//! Container for chained `ConsumerInterceptor` instances.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerInterceptors`.
//!
//! The container is not yet referenced by `MockConsumer` (Phase 3) or
//! `AsyncKafkaConsumer` (Phase 11); `dead_code` is allowed at the module
//! level to keep the public-API surface frozen ahead of consumer wiring.

#![allow(dead_code)]

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::common::TopicPartition;
use crate::consumer::interceptor::ConsumerInterceptor;
use crate::consumer::{ConsumerRecords, OffsetAndMetadata};

/// A container that holds the list of [`ConsumerInterceptor`] instances and
/// wraps calls to the chain.
///
/// Translates `org.apache.kafka.clients.consumer.internals.ConsumerInterceptors`.
///
/// # Panic safety
///
/// Java catches `Exception` (not `Throwable`) thrown by an interceptor,
/// logs at WARN via `LoggerFactory`, and continues calling the remaining
/// interceptors. The Rust analog uses [`std::panic::catch_unwind`] around
/// each `on_consume` / `on_commit` / `close` call so that a panicking
/// interceptor does not poison the poll loop.
///
/// Per Java's contract, when `on_consume` panics, the **previous successful
/// `records` value** is forwarded to the next interceptor in the chain
/// (not the input to the panicking interceptor). [`Self::on_consume`]
/// preserves this "pass last good value along" semantics.
///
/// ## Caveats
///
/// 1. **`panic = "abort"`:** under this profile setting, panics call
///    `abort()` directly; `catch_unwind` cannot recover. A panicking
///    interceptor crashes the process. Rust-wide limitation, not specific
///    to this code.
/// 2. **Interior mutability + panic.** Interceptors using `RefCell`,
///    `Cell`, atomics, or `Mutex` are responsible for their own state
///    consistency on panic. A panic mid-mutation may leave a `RefCell`
///    borrowed or a `Mutex` poisoned; subsequent calls on the same
///    interceptor are undefined-by-the-framework. Java's analog is
///    "behavior is undefined if onConsume throws mid-modification" — same
///    guarantee, different mechanism.
pub(crate) struct ConsumerInterceptors<K: 'static, V: 'static> {
    interceptors: Vec<Box<dyn ConsumerInterceptor<K, V>>>,
}

impl<K, V> ConsumerInterceptors<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Creates a new container from an explicit list of interceptors. The
    /// chain is invoked in the order given.
    ///
    /// Translates Java's `ConsumerInterceptors(List<ConsumerInterceptor<K,V>>,
    /// Metrics)` constructor. The `Metrics` parameter is dropped — see Phase
    /// 2 PLAN.md for the rationale (no metrics framework in this milestone).
    pub(crate) fn new(interceptors: Vec<Box<dyn ConsumerInterceptor<K, V>>>) -> Self {
        Self { interceptors }
    }

    /// Returns `true` if no interceptors are defined. All other methods
    /// will be no-ops in this case.
    ///
    /// Translates Java's `boolean isEmpty()`.
    pub(crate) fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }

    /// This is called when the records are about to be returned to the user.
    ///
    /// Calls [`ConsumerInterceptor::on_consume`] for each interceptor.
    /// Records returned from each interceptor are passed to `on_consume` of
    /// the next interceptor in the chain.
    ///
    /// This method does not propagate panics. If any of the interceptors
    /// panics, the panic is caught and logged at WARN, and the next
    /// interceptor is called with the `records` value returned by the
    /// previous successful `on_consume` call (matching Java's contract).
    ///
    /// To preserve this "pass last good value along" semantics across a
    /// panic, the loop clones the current records *before* each call
    /// (`K: Clone, V: Clone`) — the moved-in value is consumed by the
    /// closure, but the previous-good clone is kept in `intercept_records`
    /// and reused if `catch_unwind` returns `Err`. Java achieves the same
    /// effect via JVM-managed shared references.
    ///
    /// Translates Java's
    /// `ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records)`.
    pub(crate) fn on_consume(&self, records: ConsumerRecords<K, V>) -> ConsumerRecords<K, V>
    where
        K: Clone,
        V: Clone,
    {
        let mut intercept_records = records;
        for interceptor in &self.interceptors {
            // Pass a clone to the interceptor. If it panics, the moved-in
            // clone is lost, but `intercept_records` (the previous-good
            // value) is still owned by this stack frame and reused on the
            // next iteration. `AssertUnwindSafe` is required because trait
            // objects are not auto-`RefUnwindSafe`.
            let input = intercept_records.clone();
            let result = catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(input)));
            match result {
                Ok(new_records) => {
                    intercept_records = new_records;
                }
                Err(_panic_payload) => {
                    // Matches Java's
                    //   log.warn("Error executing interceptor onConsume callback", e);
                    // Next interceptor is called with the previous good
                    // value (already in `intercept_records`).
                    log::warn!("Error executing interceptor onConsume callback");
                }
            }
        }
        intercept_records
    }

    /// This is called when commit request returns successfully from the
    /// broker.
    ///
    /// Calls [`ConsumerInterceptor::on_commit`] on every interceptor.
    /// Panics are caught per-interceptor and logged at WARN; the next
    /// interceptor still gets called.
    ///
    /// Translates Java's
    /// `void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    pub(crate) fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
        for interceptor in &self.interceptors {
            // `&self` call: trait objects are not auto-`RefUnwindSafe`, so
            // `AssertUnwindSafe` is required despite the call signature
            // being immutable. No cross-call invariant is touched here.
            let result = catch_unwind(AssertUnwindSafe(|| interceptor.on_commit(offsets)));
            if result.is_err() {
                log::warn!("Error executing interceptor onCommit callback");
            }
        }
    }
}

impl<K: 'static, V: 'static> Drop for ConsumerInterceptors<K, V> {
    /// Closes every interceptor in the container.
    ///
    /// Translates Java's `void close()`. Errors thrown during close are
    /// logged but not propagated; matching Java behavior, panics are
    /// caught and ignored to allow remaining interceptors to be closed.
    fn drop(&mut self) {
        for interceptor in self.interceptors.iter_mut() {
            // `&mut self` call: AssertUnwindSafe required because the
            // interceptor's interior state may not be `UnwindSafe` and we
            // are about to drop the value anyway.
            let result = catch_unwind(AssertUnwindSafe(|| interceptor.close()));
            if result.is_err() {
                log::error!("Failed to close consumer interceptor");
            }
        }
    }
}
