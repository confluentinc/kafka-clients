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

/// Render a [`catch_unwind`] payload as text for a log line.
///
/// Java logs the caught `Exception` itself (`log.warn("...", e)`), so the
/// operator sees a message and a stack trace. The nearest Rust equivalent is
/// the panic payload, which for `panic!("...")` is a `String` or `&str`; any
/// other payload type is opaque and reported as such.
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic payload>")
}

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
/// ## Java's structural guarantee vs Rust's behavioral guarantee
///
/// Java's `ConsumerRecords` is **structurally immutable** (see
/// `ConsumerRecords.java:37-50`: `private final` fields, `Map.copyOf`,
/// `Collections.unmodifiableList(...)` views). When an interceptor
/// throws in Java, the input batch **cannot** have been mutated —
/// `interceptRecords` retains its previous-good value because the
/// assignment in `ConsumerInterceptors.java:70` evaluates the RHS
/// before assigning, and a thrown exception aborts the assignment.
/// The "previous-good batch" guarantee is a property of the *input
/// type*, not of interceptor implementations.
///
/// The Rust `&mut ConsumerRecords` form cannot reproduce that
/// structural immutability. The chain still catches the panic and
/// passes `*records` (whatever state the panicking interceptor left
/// it in) to the next interceptor. Matching Java's previous-good
/// guarantee becomes an **interceptor-side discipline**: build the
/// replacement off to the side, then commit with a single non-fallible
/// `*records = new_batch;` assignment. See
/// [`ConsumerInterceptor::on_consume`] for the detailed pattern.
///
/// **The in-repo test fixture `FilterConsumerInterceptor` uses this
/// pattern**: it reads `records` immutably to build a new
/// `ConsumerRecords` off to the side, then assigns at the end. This
/// is the structural Rust analog of Java's immutable input and the
/// reference pattern future in-tree interceptors should follow.
///
/// ## Caveats
///
/// 1. **`panic = "abort"`:** under this profile setting, panics call
///    `abort()` directly; `catch_unwind` cannot recover. A panicking
///    interceptor crashes the process. Rust-wide limitation, not
///    specific to this code.
/// 2. **Interior mutability + panic.** Interceptors using `RefCell`,
///    `Cell`, atomics, or `Mutex` are responsible for their own state
///    consistency on panic. Same caveat as Java's `synchronized` blocks.
/// 3. **`std::mem::take` is an anti-pattern.**
///    `mem::take(records)` writes `ConsumerRecords::default()` (empty)
///    to `*records` immediately, then yields the owned previous value.
///    If anything between the `take` and the final `*records = ...`
///    assignment panics, the next interceptor sees an **empty batch**,
///    not the previous-good batch. See
///    [`ConsumerInterceptor::on_consume`] §"Anti-pattern" for the
///    explicit example.
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
    /// Calls [`ConsumerInterceptor::on_consume`] for each interceptor in
    /// turn, passing the same `&mut ConsumerRecords` through the chain.
    /// Each interceptor may mutate `records` in place (Java's
    /// `FilterConsumerInterceptor` pattern: build a fresh batch, then
    /// `*records = new_batch;`).
    ///
    /// This method does not propagate panics. If an interceptor panics,
    /// the panic is caught and logged at WARN, and the next interceptor
    /// is invoked with the **current value of `*records`**. For a
    /// well-behaved interceptor that follows the "build off to the side,
    /// commit at the end" pattern (see [`ConsumerInterceptor::on_consume`]
    /// §"Recommended pattern"), `*records` retains its previous-good value
    /// because no fallible work writes to it. For an interceptor that
    /// uses `std::mem::take` followed by fallible work, the next
    /// interceptor would see an empty batch — see
    /// [`ConsumerInterceptor::on_consume`] §"Anti-pattern".
    ///
    /// [`AssertUnwindSafe`] is required because the `&mut` borrow of
    /// `records` is not auto-`UnwindSafe` (Rust assumes that a panic
    /// could leave the borrowed value in an inconsistent state). For
    /// this chain that risk is real but explicit and documented above;
    /// we accept it and wrap with `AssertUnwindSafe`.
    ///
    /// Note: no `K: Clone, V: Clone` bound. The chain never clones the
    /// batch; the `&mut` form lets each interceptor observe / replace the
    /// batch without any defensive copies, matching Java's reference
    /// semantics.
    ///
    /// Translates Java's
    /// `ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records)`.
    pub(crate) fn on_consume(&self, records: &mut ConsumerRecords<K, V>) {
        for interceptor in &self.interceptors {
            // Wrap the `&mut` call in `AssertUnwindSafe`: `&mut T` is not
            // auto-`UnwindSafe`, but the partial-mutation risk is matched
            // by Java's "undefined behavior on mid-modification panic"
            // caveat, so we assert it explicitly.
            let result = catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(records)));
            if let Err(payload) = result {
                // Matches Java's
                //   log.warn("Error executing interceptor onConsume callback", e);
                // including the caught exception — the operator needs the
                // cause to act on it.
                // The next interceptor is called with the current value
                // of `*records` (unchanged if the panic happened before
                // the interceptor wrote, partially mutated otherwise).
                log::warn!(
                    "Error executing interceptor onConsume callback: {}",
                    panic_payload_message(&*payload)
                );
            }
        }
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
            if let Err(payload) = result {
                // Java: `log.warn("Error executing interceptor onCommit callback", e)`.
                log::warn!(
                    "Error executing interceptor onCommit callback: {}",
                    panic_payload_message(&*payload)
                );
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
            if let Err(payload) = result {
                // Java: `log.error("Failed to close consumer interceptor ", e)`.
                log::error!("Failed to close consumer interceptor: {}", panic_payload_message(&*payload));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.clients.consumer.internals.ConsumerInterceptorsTest`.
    //!
    //! Inline tests because [`ConsumerInterceptors`] is `pub(crate)` per
    //! CLAUDE.md §2 (lives in `internals/`). The `tests/consumer/internals/`
    //! path called out in the Phase 2 PLAN.md cannot reach `pub(crate)`
    //! items; the producer module uses the same inline pattern for its
    //! internals (see `producer::internals::buffer_pool`).
    //!
    //! Additional Rust-only regression test:
    //! `test_on_consume_chain_panic_with_partition_filter` — exercises the
    //! interceptor-panic recovery path through the actual filter logic
    //! (the Java test sets `throwExceptionOnConsume = true` and asserts
    //! the next interceptor still runs on the previous-good batch).
    //! `test_on_consume_panic_does_not_poison_chain` is the focused
    //! panic-safety unit test required by Phase 2 PLAN.md verification §7.
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use indexmap::IndexMap;

    use super::*;
    use crate::common::TopicPartition;
    use crate::common::header::internals::RecordHeaders;
    use crate::common::record::TimestampType;
    use crate::consumer::interceptor::ConsumerInterceptor;
    use crate::consumer::{ConsumerRecord, ConsumerRecords, OffsetAndMetadata};

    /// Shared interior state for [`FilterConsumerInterceptor`]. Held in an
    /// [`Arc`] so the test can poke the toggles and read the counts from
    /// outside the container after the interceptor has been moved into the
    /// `ConsumerInterceptors`.
    struct FilterState {
        filter_partition: i32,
        throw_on_consume: std::sync::atomic::AtomicBool,
        throw_on_commit: std::sync::atomic::AtomicBool,
        on_consume_count: AtomicUsize,
        on_commit_count: AtomicUsize,
    }

    impl FilterState {
        fn new(filter_partition: i32) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                filter_partition,
                throw_on_consume: std::sync::atomic::AtomicBool::new(false),
                throw_on_commit: std::sync::atomic::AtomicBool::new(false),
                on_consume_count: AtomicUsize::new(0),
                on_commit_count: AtomicUsize::new(0),
            })
        }

        fn inject_on_consume_error(&self, on: bool) {
            self.throw_on_consume.store(on, Ordering::SeqCst);
        }

        fn inject_on_commit_error(&self, on: bool) {
            self.throw_on_commit.store(on, Ordering::SeqCst);
        }

        fn on_consume_count(&self) -> usize {
            self.on_consume_count.load(Ordering::SeqCst)
        }

        fn on_commit_count(&self) -> usize {
            self.on_commit_count.load(Ordering::SeqCst)
        }
    }

    /// Test consumer interceptor that filters records in `on_consume`,
    /// mirroring `FilterConsumerInterceptor` in the Java test. The
    /// observable state lives behind an [`Arc<FilterState>`] so the test
    /// can inspect counts after the interceptor has been moved into the
    /// container — avoiding `unsafe` raw-pointer aliasing.
    struct FilterConsumerInterceptor {
        state: std::sync::Arc<FilterState>,
    }

    impl FilterConsumerInterceptor {
        fn new(state: std::sync::Arc<FilterState>) -> Self {
            Self { state }
        }
    }

    impl ConsumerInterceptor<i32, i32> for FilterConsumerInterceptor {
        fn on_consume(&self, records: &mut ConsumerRecords<i32, i32>) {
            self.state.on_consume_count.fetch_add(1, Ordering::SeqCst);
            if self.state.throw_on_consume.load(Ordering::SeqCst) {
                // Java: `"Injected exception in FilterConsumerInterceptor.onConsume."`.
                panic!("Injected failure in FilterConsumerInterceptor.on_consume.");
            }

            // Mirror Java's `FilterConsumerInterceptor.onConsume`
            // (ConsumerInterceptorsTest.java:71-86): build a fresh
            // `recordMap` from the surviving partitions, then commit it
            // wholesale at the end.
            //
            // Build the replacement off to the side using a borrowed
            // view of `*records` — no `std::mem::take` up front. This
            // matches the "build off to the side, write at the end"
            // pattern documented on [`ConsumerInterceptor::on_consume`]
            // and is the structural Rust analog of Java's immutable
            // input: any panic in the loop below leaves `*records`
            // unchanged because the final assignment happens only after
            // all fallible work is complete.
            //
            // For the test fixture's `<i32, i32>` records, cloning is
            // cheap (`i32: Copy`, `RecordHeaders: Clone`); production
            // interceptors over expensive `K, V` would need a different
            // strategy (e.g. drain via `Vec::drain_filter` once stable),
            // but the panic-safety pattern is the same.
            let mut new_records: IndexMap<TopicPartition, Vec<ConsumerRecord<i32, i32>>> = IndexMap::new();
            let mut new_next_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
            for tp in records.partitions().cloned().collect::<Vec<_>>() {
                if tp.partition() != self.state.filter_partition {
                    let recs: Vec<ConsumerRecord<i32, i32>> = records
                        .records_for_partition(&tp)
                        .iter()
                        .map(|r| {
                            ConsumerRecord::with_all(
                                r.topic().to_string(),
                                r.partition(),
                                r.offset(),
                                r.timestamp(),
                                r.timestamp_type(),
                                r.serialized_key_size(),
                                r.serialized_value_size(),
                                r.key().copied(),
                                r.value().copied(),
                                r.headers().clone(),
                                r.leader_epoch(),
                                r.delivery_count(),
                            )
                        })
                        .collect();
                    if let Some(oam) = records.next_offsets().get(&tp) {
                        new_next_offsets.insert(tp.clone(), oam.clone());
                    }
                    new_records.insert(tp, recs);
                }
            }
            // Single non-fallible commit: by the time we reach this line,
            // every fallible step (record allocation, hashmap insertions)
            // has already succeeded. If a panic occurred above, this
            // assignment is never reached and `*records` retains its
            // original value — Java's "previous-good batch" guarantee.
            *records = ConsumerRecords::new(new_records, new_next_offsets);
        }

        fn on_commit(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
            self.state.on_commit_count.fetch_add(1, Ordering::SeqCst);
            if self.state.throw_on_commit.load(Ordering::SeqCst) {
                // Java: `"Injected exception in FilterConsumerInterceptor.onCommit."`.
                panic!("Injected failure in FilterConsumerInterceptor.on_commit.");
            }
        }
    }

    fn make_consumer_record(topic: &str, partition: i32) -> ConsumerRecord<i32, i32> {
        // Mirrors Java's
        //   new ConsumerRecord<>(topic, partition, 0, 0L,
        //       TimestampType.CREATE_TIME, 0, 0, 1, 1, new RecordHeaders(),
        //       Optional.empty())
        ConsumerRecord::with_all(
            topic.to_string(),
            partition,
            0, // offset
            0, // timestamp
            TimestampType::CreateTime,
            0, // serialized_key_size
            0, // serialized_value_size
            Some(1),
            Some(1),
            RecordHeaders::new(),
            None, // leader_epoch
            None, // delivery_count
        )
    }

    fn make_offset_and_metadata(offset: i64) -> OffsetAndMetadata {
        OffsetAndMetadata::with_leader_epoch(offset, None, "").unwrap()
    }

    fn validate_next_offsets(
        next_offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        size: usize,
        tp: &TopicPartition,
        filter_topic_part1: &TopicPartition,
        filter_topic_part2: &TopicPartition,
    ) {
        assert_eq!(next_offsets.len(), size);
        let expected = make_offset_and_metadata(1);
        if size == 1 {
            assert_eq!(next_offsets.get(tp), Some(&expected));
        } else if size == 2 {
            assert_eq!(next_offsets.get(tp), Some(&expected));
            assert_eq!(next_offsets.get(filter_topic_part1), Some(&expected));
        } else if size == 3 {
            assert_eq!(next_offsets.get(tp), Some(&expected));
            assert_eq!(next_offsets.get(filter_topic_part1), Some(&expected));
            assert_eq!(next_offsets.get(filter_topic_part2), Some(&expected));
        }
    }

    /// Build the 3-partition input `ConsumerRecords` used by
    /// `test_on_consume_chain`. Helper rather than a single shared value
    /// because [`ConsumerRecords`] is no longer `Clone` (Phase 2 fixup) —
    /// each test invocation needs a fresh batch since the chain mutates
    /// the value in place.
    fn build_three_partition_input(
        tp: &TopicPartition,
        filter_topic_part1: &TopicPartition,
        filter_topic_part2: &TopicPartition,
    ) -> ConsumerRecords<i32, i32> {
        let mut records: IndexMap<TopicPartition, Vec<ConsumerRecord<i32, i32>>> = IndexMap::new();
        let mut next_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();

        records.insert(tp.clone(), vec![make_consumer_record(tp.topic(), tp.partition())]);
        next_offsets.insert(tp.clone(), make_offset_and_metadata(1));

        records.insert(
            filter_topic_part1.clone(),
            vec![make_consumer_record(
                filter_topic_part1.topic(),
                filter_topic_part1.partition(),
            )],
        );
        next_offsets.insert(filter_topic_part1.clone(), make_offset_and_metadata(1));

        records.insert(
            filter_topic_part2.clone(),
            vec![make_consumer_record(
                filter_topic_part2.topic(),
                filter_topic_part2.partition(),
            )],
        );
        next_offsets.insert(filter_topic_part2.clone(), make_offset_and_metadata(1));

        ConsumerRecords::new(records, next_offsets)
    }

    /// Translates `ConsumerInterceptorsTest.testOnConsumeChain`.
    #[test]
    fn test_on_consume_chain() {
        let filter_partition1 = 5;
        let filter_partition2 = 6;
        let topic = "test";
        let partition = 1;
        let tp = TopicPartition::new(topic.to_string(), partition);
        let filter_topic_part1 = TopicPartition::new("test5".to_string(), filter_partition1);
        let filter_topic_part2 = TopicPartition::new("test6".to_string(), filter_partition2);

        let state1 = FilterState::new(filter_partition1);
        let state2 = FilterState::new(filter_partition2);
        let interceptor1 = Box::new(FilterConsumerInterceptor::new(state1.clone()));
        let interceptor2 = Box::new(FilterConsumerInterceptor::new(state2.clone()));
        let interceptors: ConsumerInterceptors<i32, i32> = ConsumerInterceptors::new(vec![interceptor1, interceptor2]);
        let i1 = &state1;
        let i2 = &state2;

        // verify that on_consume modifies ConsumerRecords in place.
        let mut intercepted = build_three_partition_input(&tp, &filter_topic_part1, &filter_topic_part2);
        interceptors.on_consume(&mut intercepted);
        assert_eq!(intercepted.count(), 1);
        let parts: Vec<TopicPartition> = intercepted.partitions().cloned().collect();
        assert!(parts.contains(&tp));
        assert!(!parts.contains(&filter_topic_part1));
        assert!(!parts.contains(&filter_topic_part2));
        assert_eq!(i1.on_consume_count() + i2.on_consume_count(), 2);
        validate_next_offsets(intercepted.next_offsets(), 1, &tp, &filter_topic_part1, &filter_topic_part2);

        // verify that even if one of the intermediate interceptors panics,
        // all interceptors' on_consume are called and the next interceptor
        // sees the previous-good batch (the input in this case, since
        // interceptor1 panicked before writing `*records`).
        i1.inject_on_consume_error(true);
        let mut part_intercepted = build_three_partition_input(&tp, &filter_topic_part1, &filter_topic_part2);
        interceptors.on_consume(&mut part_intercepted);
        assert_eq!(part_intercepted.count(), 2);
        let parts: Vec<TopicPartition> = part_intercepted.partitions().cloned().collect();
        assert!(parts.contains(&filter_topic_part1)); // interceptor1 panicked
        assert!(!parts.contains(&filter_topic_part2)); // interceptor2 still ran
        assert_eq!(i1.on_consume_count() + i2.on_consume_count(), 4);
        validate_next_offsets(
            part_intercepted.next_offsets(),
            2,
            &tp,
            &filter_topic_part1,
            &filter_topic_part2,
        );

        // if all interceptors panic, records should be unmodified.
        i2.inject_on_consume_error(true);
        let mut none_intercepted = build_three_partition_input(&tp, &filter_topic_part1, &filter_topic_part2);
        let baseline = build_three_partition_input(&tp, &filter_topic_part1, &filter_topic_part2);
        interceptors.on_consume(&mut none_intercepted);
        // Matches Java's `assertEquals(noneInterceptedRecs, consumerRecords)`
        // (ConsumerInterceptorsTest.java:162-166): full structural equality
        // over the entire batch — per-partition record lists AND the
        // `next_offsets` map — guards against any future regression that
        // would mutate downstream state on the all-panic path.
        assert_eq!(none_intercepted, baseline);
        assert_eq!(i1.on_consume_count() + i2.on_consume_count(), 6);
        validate_next_offsets(
            none_intercepted.next_offsets(),
            3,
            &tp,
            &filter_topic_part1,
            &filter_topic_part2,
        );

        drop(interceptors); // explicit close (mirrors Java's interceptors.close())
    }

    /// Translates `ConsumerInterceptorsTest.testOnCommitChain`.
    #[test]
    fn test_on_commit_chain() {
        let filter_partition1 = 5;
        let filter_partition2 = 6;
        let topic = "test";
        let partition = 1;
        let tp = TopicPartition::new(topic.to_string(), partition);

        let state1 = FilterState::new(filter_partition1);
        let state2 = FilterState::new(filter_partition2);
        let interceptor1 = Box::new(FilterConsumerInterceptor::new(state1.clone()));
        let interceptor2 = Box::new(FilterConsumerInterceptor::new(state2.clone()));
        let interceptors: ConsumerInterceptors<i32, i32> = ConsumerInterceptors::new(vec![interceptor1, interceptor2]);
        let i1 = &state1;
        let i2 = &state2;

        let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        offsets.insert(tp, OffsetAndMetadata::new(0).unwrap());

        // verify that on_commit is called for all interceptors in the chain.
        interceptors.on_commit(&offsets);
        assert_eq!(i1.on_commit_count() + i2.on_commit_count(), 2);

        // verify that even if one of the interceptors panics, all
        // interceptors' on_commit are called.
        i1.inject_on_commit_error(true);
        interceptors.on_commit(&offsets);
        assert_eq!(i1.on_commit_count() + i2.on_commit_count(), 4);

        drop(interceptors);
    }

    /// Focused panic-safety regression test required by Phase 2 PLAN.md §7
    /// — separate from the Java-translated tests so a regression in the
    /// `catch_unwind` wiring fails this test directly. A panicking
    /// interceptor must not poison the chain or leak the panic.
    #[test]
    fn test_on_consume_panic_does_not_poison_chain() {
        struct PanickyInterceptor;
        impl ConsumerInterceptor<i32, i32> for PanickyInterceptor {
            fn on_consume(&self, _records: &mut ConsumerRecords<i32, i32>) {
                panic!("boom");
            }
            fn on_commit(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
                panic!("boom-commit");
            }
        }

        struct CountingState {
            consume_count: AtomicUsize,
            commit_count: AtomicUsize,
        }
        struct Counting {
            state: std::sync::Arc<CountingState>,
        }
        impl ConsumerInterceptor<i32, i32> for Counting {
            fn on_consume(&self, _records: &mut ConsumerRecords<i32, i32>) {
                self.state.consume_count.fetch_add(1, Ordering::SeqCst);
            }
            fn on_commit(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
                self.state.commit_count.fetch_add(1, Ordering::SeqCst);
            }
        }

        let counting_state = std::sync::Arc::new(CountingState {
            consume_count: AtomicUsize::new(0),
            commit_count: AtomicUsize::new(0),
        });
        let counting = Box::new(Counting { state: counting_state.clone() });
        let interceptors: ConsumerInterceptors<i32, i32> =
            ConsumerInterceptors::new(vec![Box::new(PanickyInterceptor), counting, Box::new(PanickyInterceptor)]);
        let counting = &counting_state;

        // Build a minimal non-empty ConsumerRecords so we can inspect that
        // the chain passes the value through.
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut records: IndexMap<TopicPartition, Vec<ConsumerRecord<i32, i32>>> = IndexMap::new();
        records.insert(tp.clone(), vec![make_consumer_record("t", 0)]);
        let mut next_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        next_offsets.insert(tp, make_offset_and_metadata(1));
        let mut input = ConsumerRecords::new(records, next_offsets);

        // No panic should escape `on_consume`. All three interceptors are
        // called; the middle (Counting) interceptor records once.
        interceptors.on_consume(&mut input);
        assert_eq!(counting.consume_count.load(Ordering::SeqCst), 1);

        let empty_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        interceptors.on_commit(&empty_offsets);
        assert_eq!(counting.commit_count.load(Ordering::SeqCst), 1);
    }

    /// Translates Java's `testOnCommitChain` failure-recovery assertion in
    /// a more focused form: verify that Drop-time close() panics do not
    /// prevent subsequent interceptors from being closed.
    #[test]
    fn test_drop_close_panic_does_not_block_remaining_closes() {
        struct PanicOnClose {
            closed: std::sync::Arc<Mutex<bool>>,
        }
        impl ConsumerInterceptor<i32, i32> for PanicOnClose {
            fn on_consume(&self, _records: &mut ConsumerRecords<i32, i32>) {}
            fn on_commit(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {}
            fn close(&mut self) {
                panic!("boom on close");
            }
        }
        impl Drop for PanicOnClose {
            fn drop(&mut self) {
                // Track Drop separately so we can confirm both
                // interceptors were dropped despite the first panicking on
                // close.
                *self.closed.lock().unwrap() = true;
            }
        }

        let flag1 = std::sync::Arc::new(Mutex::new(false));
        let flag2 = std::sync::Arc::new(Mutex::new(false));
        let interceptors: ConsumerInterceptors<i32, i32> = ConsumerInterceptors::new(vec![
            Box::new(PanicOnClose { closed: flag1.clone() }),
            Box::new(PanicOnClose { closed: flag2.clone() }),
        ]);
        drop(interceptors);

        // Both should have been close()-ed (and panicked on close()),
        // then dropped (setting their flags).
        assert!(*flag1.lock().unwrap());
        assert!(*flag2.lock().unwrap());
    }
}
