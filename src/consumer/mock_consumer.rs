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

//! In-memory `Consumer<K, V>` implementation for use in tests.
//!
//! Translated from `org.apache.kafka.clients.consumer.MockConsumer`.
//! Mirrors the Java class field-for-field. Mock-specific driver methods
//! (`add_record`, `update_beginning_offsets`, `rebalance`, `schedule_poll_task`,
//! `set_poll_exception`, etc.) live on the concrete `MockConsumer<K, V>`
//! type — they are NOT on the [`Consumer<K, V>`](crate::consumer::Consumer)
//! trait. Tests hold a `MockConsumer` directly and pass `&mut consumer` where
//! `&mut dyn Consumer<K, V>` is expected (see `consumer-threading.md` §2).
//!
//! Threading: `MockConsumer` is `!Sync` and intended for single-task use
//! through the `&mut self` dispatch surface. `wakeup()` is callable from
//! any task because it takes `&self` and uses an atomic flag.

// Some fields and private helpers are exercised by the `Consumer<K, V>` impl
// landing in the next commit; the trait impl is not yet wired up.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::common::{KafkaError, PartitionInfo, TopicPartition};
use crate::consumer::internals::auto_offset_reset_strategy::StrategyType;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::{AutoOffsetResetStrategy, ConsumerRecord, OffsetAndMetadata};

/// Fixed `client.id` returned by [`MockConsumer::client_id`]. Java's
/// `MockConsumer` has no equivalent accessor; the `Consumer` trait requires
/// one, so we pick a stable constant.
const MOCK_CLIENT_ID: &str = "mock-consumer";

/// One scheduled poll task — see [`MockConsumer::schedule_poll_task`].
///
/// The deviation from Java's parameterless `Runnable` is documented on the
/// `schedule_poll_task` method; the task receives an explicit
/// `&mut MockConsumer<K, V>` because Rust closures cannot safely capture
/// the outer struct.
pub type PollTask<K, V> = Box<dyn FnOnce(&mut MockConsumer<K, V>) + Send>;

/// A mock of the [`Consumer`](crate::consumer::Consumer) interface, intended
/// for testing code that uses Kafka.
///
/// Translated from
/// `org.apache.kafka.clients.consumer.MockConsumer`.
///
/// This struct is NOT thread-safe. However, you can use
/// [`MockConsumer::schedule_poll_task`] to write multi-task tests where one
/// task waits for [`Consumer::poll`](crate::consumer::Consumer::poll) to be
/// called by another task and can safely perform operations during a
/// callback.
///
/// # Java mapping
///
/// Java's `MockConsumer(String offsetResetStrategy)` and the deprecated
/// `MockConsumer(OffsetResetStrategy)` constructors both collapse into
/// [`MockConsumer::new`]. Callers needing string-based parsing use
/// [`AutoOffsetResetStrategy::from_string`].
pub struct MockConsumer<K, V> {
    partitions: HashMap<String, Vec<PartitionInfo>>,
    /// Held directly (NOT `Arc<Mutex<...>>`). Per `consumer-threading.md` §16
    /// the mutex wrap is only required when state crosses task boundaries;
    /// `MockConsumer`'s [`Consumer`](crate::consumer::Consumer) trait API is
    /// `&mut self`, so the Rust borrow checker enforces single-writer access
    /// without runtime locking.
    subscriptions: SubscriptionState,
    beginning_offsets: HashMap<TopicPartition, i64>,
    end_offsets: HashMap<TopicPartition, i64>,
    duration_reset_offsets: HashMap<TopicPartition, i64>,
    committed: HashMap<TopicPartition, OffsetAndMetadata>,
    /// Java stores `Runnable` tasks that capture `MockConsumer` implicitly.
    /// Rust closures cannot capture the outer struct safely without
    /// `Arc<Mutex<...>>` wrapping, so we deviate: tasks receive `&mut
    /// MockConsumer<K, V>` explicitly. Tests write
    /// `consumer.schedule_poll_task(Box::new(|c| c.add_record(...).unwrap()));`.
    poll_tasks: VecDeque<PollTask<K, V>>,
    paused: HashSet<TopicPartition>,
    /// Atomic so [`Consumer::wakeup`](crate::consumer::Consumer::wakeup) can
    /// take `&self` (callable from any task / signal handler). `SeqCst`
    /// because wakeup is rare and reorder reasoning is not worth the win.
    wakeup: AtomicBool,
    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    poll_exception: Option<KafkaError>,
    offsets_exception: Option<KafkaError>,
    last_poll_timeout: Option<Duration>,
    closed: bool,
    should_rebalance: bool,
    /// `i64::MAX` is the "no cap" sentinel mirroring Java's `Long.MAX_VALUE`.
    max_poll_records: i64,
}

impl<K, V> MockConsumer<K, V> {
    /// Create a new mock consumer with the given offset-reset strategy.
    ///
    /// Translates Java's `MockConsumer(String)` and the deprecated
    /// `MockConsumer(OffsetResetStrategy)` into a single constructor that
    /// takes the typed [`AutoOffsetResetStrategy`] value.
    pub fn new(offset_reset_strategy: AutoOffsetResetStrategy) -> Self {
        Self {
            partitions: HashMap::new(),
            subscriptions: SubscriptionState::new(offset_reset_strategy),
            beginning_offsets: HashMap::new(),
            end_offsets: HashMap::new(),
            duration_reset_offsets: HashMap::new(),
            committed: HashMap::new(),
            poll_tasks: VecDeque::new(),
            paused: HashSet::new(),
            wakeup: AtomicBool::new(false),
            records: HashMap::new(),
            poll_exception: None,
            offsets_exception: None,
            last_poll_timeout: None,
            closed: false,
            should_rebalance: false,
            max_poll_records: i64::MAX,
        }
    }

    // ── Driver methods (mock-specific) ──────────────────────────────────

    /// Add a record to the buffer that the next [`Consumer::poll`](crate::consumer::Consumer::poll)
    /// call will return. The record's `(topic, partition)` must already be
    /// assigned to the consumer.
    ///
    /// Translates Java's `addRecord(ConsumerRecord<K, V>)`
    /// (`MockConsumer.java:322`). Returns
    /// [`KafkaError::IllegalState`] if the partition is not assigned —
    /// Java throws `IllegalStateException` in that case.
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) -> Result<(), KafkaError> {
        self.ensure_not_closed()?;
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        if !self.subscriptions.assigned_partitions().contains(&tp) {
            return Err(KafkaError::illegal_state(
                "Cannot add records for a partition that is not assigned to the consumer",
            ));
        }
        self.records.entry(tp).or_default().push(record);
        Ok(())
    }

    /// Update the beginning offsets used for `seekToBeginning` resets.
    ///
    /// Translates Java's `updateBeginningOffsets(Map<TopicPartition, Long>)`.
    pub fn update_beginning_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        self.beginning_offsets.extend(offsets);
    }

    /// Update the end offsets used for `seekToEnd` resets and for
    /// [`Consumer::end_offsets`](crate::consumer::Consumer::end_offsets) /
    /// [`Consumer::current_lag`](crate::consumer::Consumer::current_lag).
    ///
    /// Translates Java's `updateEndOffsets(Map<TopicPartition, Long>)`.
    pub fn update_end_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        self.end_offsets.extend(offsets);
    }

    /// Update the duration-based reset offsets used when the reset strategy
    /// is `by_duration:<ISO-8601>`.
    ///
    /// Translates Java's `updateDurationOffsets(Map<TopicPartition, Long>)`.
    pub fn update_duration_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        self.duration_reset_offsets.extend(offsets);
    }

    /// Configure the [`PartitionInfo`] list for a topic, used by
    /// [`Consumer::partitions_for`](crate::consumer::Consumer::partitions_for)
    /// and [`Consumer::list_topics`](crate::consumer::Consumer::list_topics).
    ///
    /// Translates Java's `updatePartitions(String, List<PartitionInfo>)`.
    pub fn update_partitions(&mut self, topic: &str, partitions: Vec<PartitionInfo>) -> Result<(), KafkaError> {
        self.ensure_not_closed()?;
        self.partitions.insert(topic.to_string(), partitions);
        Ok(())
    }

    /// Inject an exception to be returned by the next
    /// [`Consumer::poll`](crate::consumer::Consumer::poll) call. The
    /// exception is taken (cleared) on use.
    ///
    /// Translates Java's `setPollException(KafkaException)`.
    pub fn set_poll_exception(&mut self, exception: KafkaError) {
        self.poll_exception = Some(exception);
    }

    /// Inject an exception to be returned by the next
    /// [`Consumer::beginning_offsets`](crate::consumer::Consumer::beginning_offsets) /
    /// [`Consumer::end_offsets`](crate::consumer::Consumer::end_offsets)
    /// call. The exception is taken (cleared) on use.
    ///
    /// Translates Java's `setOffsetsException(KafkaException)`.
    pub fn set_offsets_exception(&mut self, exception: KafkaError) {
        self.offsets_exception = Some(exception);
    }

    /// Set the maximum number of records returned in a single
    /// [`Consumer::poll`](crate::consumer::Consumer::poll) call.
    ///
    /// Translates Java's `setMaxPollRecords(long)`. Returns
    /// [`KafkaError::IllegalArgument`] when `max_poll_records < 1`, matching
    /// Java's `IllegalArgumentException`.
    pub fn set_max_poll_records(&mut self, max_poll_records: i64) -> Result<(), KafkaError> {
        if max_poll_records < 1 {
            return Err(KafkaError::illegal_argument("MaxPollRecords must be strictly superior to 0"));
        }
        self.max_poll_records = max_poll_records;
        Ok(())
    }

    /// Simulate a rebalance event: compute revoked / added partitions
    /// against the current assignment, invoke any registered
    /// [`ConsumerRebalanceListener`](crate::consumer::ConsumerRebalanceListener)
    /// callbacks, and replace the assignment.
    ///
    /// Translates Java's `rebalance(Collection<TopicPartition>)`. Async
    /// because the Rust listener methods are `async fn` (per
    /// `consumer-threading.md` §31 / Phase 2); Java's listener methods
    /// are sync.
    pub async fn rebalance(&mut self, new_assignment: &[TopicPartition]) -> Result<(), KafkaError> {
        let old_assignment_set = self.subscriptions.assigned_partitions();
        let new_assignment_set: HashSet<TopicPartition> = new_assignment.iter().cloned().collect();

        let added: Vec<TopicPartition> = new_assignment
            .iter()
            .filter(|tp| !old_assignment_set.contains(tp))
            .cloned()
            .collect();
        let removed: Vec<TopicPartition> = old_assignment_set
            .iter()
            .filter(|tp| !new_assignment_set.contains(tp))
            .cloned()
            .collect();

        // Clear buffered records (Java: `records.clear()` at line 135).
        self.records.clear();

        // Invoke onPartitionsRevoked first (Java line 138-140). Per
        // consumer-threading.md §16, clone the Arc out before awaiting.
        if !removed.is_empty()
            && let Some(listener) = self.subscriptions.rebalance_listener()
        {
            listener.on_partitions_revoked(&removed).await?;
        }

        // Replace assignment (Java line 141:
        // `subscriptions.assignFromSubscribed(newAssignment)`).
        self.subscriptions.assign_from_subscribed(new_assignment)?;

        // Invoke onPartitionsAssigned (Java line 142). Java passes only the
        // *added* partitions, not the full new assignment.
        if let Some(listener) = self.subscriptions.rebalance_listener() {
            listener.on_partitions_assigned(&added).await?;
        }

        Ok(())
    }

    /// Schedule a task to run on the next
    /// [`Consumer::poll`](crate::consumer::Consumer::poll) call. One task
    /// is consumed per `poll` invocation, in FIFO order.
    ///
    /// Translates Java's `schedulePollTask(Runnable)`. Java's `Runnable`
    /// captures `MockConsumer` implicitly via closure; Rust closures cannot
    /// capture the outer struct safely, so the task is invoked with
    /// `&mut MockConsumer<K, V>` explicitly. Callers write
    /// `consumer.schedule_poll_task(Box::new(|c| { c.add_record(...).unwrap(); }));`.
    pub fn schedule_poll_task(&mut self, task: PollTask<K, V>) {
        self.poll_tasks.push_back(task);
    }

    /// Return whether the consumer has been closed.
    ///
    /// Translates Java's `closed()`.
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Return whether an `enforceRebalance` request is pending.
    ///
    /// Translates Java's `shouldRebalance()`.
    pub fn should_rebalance(&self) -> bool {
        self.should_rebalance
    }

    /// Reset the rebalance-pending flag after handling it.
    ///
    /// Translates Java's `resetShouldRebalance()`.
    pub fn reset_should_rebalance(&mut self) {
        self.should_rebalance = false;
    }

    /// The timeout passed to the most recent
    /// [`Consumer::poll`](crate::consumer::Consumer::poll) call, or `None`
    /// if `poll` has not been called yet.
    ///
    /// Translates Java's `lastPollTimeout()`.
    pub fn last_poll_timeout(&self) -> Option<Duration> {
        self.last_poll_timeout
    }

    // ── Internal helpers (used by both inherent + Consumer impls) ───────

    /// Mirrors Java's `ensureNotClosed`. Java throws
    /// `IllegalStateException("This consumer has already been closed.")`.
    fn ensure_not_closed(&self) -> Result<(), KafkaError> {
        if self.closed {
            Err(KafkaError::illegal_state("This consumer has already been closed."))
        } else {
            Ok(())
        }
    }

    /// Mirrors Java's `updateFetchPosition(TopicPartition)`
    /// (`MockConsumer.java:622-631`).
    fn update_fetch_position(&mut self, tp: &TopicPartition) -> Result<(), KafkaError> {
        if self.subscriptions.is_offset_reset_needed(tp)? {
            self.reset_offset_position(tp)
        } else if !self.committed.contains_key(tp) {
            self.subscriptions.request_offset_reset_default(tp)?;
            self.reset_offset_position(tp)
        } else {
            // Safe: presence checked just above.
            let offset = self.committed.get(tp).expect("checked containsKey").offset();
            self.subscriptions.seek(tp, offset)
        }
    }

    /// Mirrors Java's `resetOffsetPosition(TopicPartition)`
    /// (`MockConsumer.java:633-652`). Picks the source map by the
    /// partition's reset strategy and `seek`s to the configured offset.
    /// Returns [`KafkaError::IllegalState`] when the map does not have an
    /// entry for the partition (Java throws `IllegalStateException`), or
    /// the `ConsumerError::NoOffsetForPartition` variant
    /// (Java's `NoOffsetForPartitionException`) when the strategy is `None`.
    fn reset_offset_position(&mut self, tp: &TopicPartition) -> Result<(), KafkaError> {
        let strategy = self.subscriptions.reset_strategy(tp)?.unwrap_or(AutoOffsetResetStrategy::NONE);

        if strategy == AutoOffsetResetStrategy::EARLIEST {
            let offset = self.beginning_offsets.get(tp).copied().ok_or_else(|| {
                KafkaError::illegal_state(
                    "MockConsumer didn't have beginning offset specified, but tried to seek to beginning",
                )
            })?;
            self.subscriptions.seek(tp, offset)
        } else if strategy == AutoOffsetResetStrategy::LATEST {
            let offset = self.end_offsets.get(tp).copied().ok_or_else(|| {
                KafkaError::illegal_state("MockConsumer didn't have end offset specified, but tried to seek to end")
            })?;
            self.subscriptions.seek(tp, offset)
        } else if strategy.type_() == StrategyType::ByDuration {
            let offset = self.duration_reset_offsets.get(tp).copied().ok_or_else(|| {
                KafkaError::illegal_state(
                    "MockConsumer didn't have duration offset specified, but tried to seek to timestamp",
                )
            })?;
            self.subscriptions.seek(tp, offset)
        } else {
            // strategy == NONE
            Err(crate::consumer::errors::ConsumerError::no_offset_for_partition(tp.clone()).into())
        }
    }
}

#[cfg(test)]
mod tests {
    //! Inherent-impl unit tests. The full [`MockConsumerTest`] translation
    //! lives in `tests/consumer/mock_consumer_test.rs` and exercises the
    //! public surface (incl. the `Consumer<K, V>` trait impl).

    use super::*;

    #[test]
    fn test_new_initial_state() {
        let c: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
        assert!(!c.closed());
        assert!(!c.should_rebalance());
        assert!(c.last_poll_timeout().is_none());
        assert_eq!(c.max_poll_records, i64::MAX);
    }

    #[test]
    fn test_set_max_poll_records_rejects_zero_and_negative() {
        let mut c: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
        assert!(c.set_max_poll_records(0).is_err());
        assert!(c.set_max_poll_records(-1).is_err());
        assert!(c.set_max_poll_records(1).is_ok());
        assert_eq!(c.max_poll_records, 1);
    }

    #[test]
    fn test_reset_should_rebalance() {
        let mut c: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
        c.should_rebalance = true;
        assert!(c.should_rebalance());
        c.reset_should_rebalance();
        assert!(!c.should_rebalance());
    }

    #[test]
    fn test_update_partitions_after_close_errors() {
        let mut c: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
        c.closed = true;
        let err = c.update_partitions("t", Vec::new()).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalState(_)));
    }
}
