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
//! `set_poll_error`, etc.) live on the concrete `MockConsumer<K, V>`
//! type — they are NOT on the [`Consumer<K, V>`](crate::consumer::Consumer)
//! trait. Tests hold a `MockConsumer` directly and pass `&mut consumer` where
//! `&mut dyn Consumer<K, V>` is expected (see `consumer-threading.md` §2).
//!
//! Threading: `MockConsumer` is `!Sync` and intended for single-task use
//! through the `&mut self` dispatch surface. `wakeup()` is callable from
//! any task because it takes `&self` and uses an atomic flag.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::common::metrics::KafkaMetric;
use crate::common::{Error, MetricName, PartitionInfo, TopicPartition};
use crate::consumer::StrategyType;
use crate::consumer::internals::{FetchPosition, SubscriptionState};
use crate::consumer::{
    AutoOffsetResetStrategy, CloseOptions, Consumer, ConsumerGroupMetadata, ConsumerHandle, ConsumerRebalanceListener,
    ConsumerRecord, ConsumerRecords, OffsetAndMetadata, OffsetAndTimestamp, OffsetCommitCallback, SubscriptionPattern,
};
use crate::consumer::{ConsumerNoOffsetForPartitionError, ConsumerOffsetOutOfRangeError};
use crate::metadata::LeaderAndEpoch;

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

/// A mock of the [`Consumer`] interface, intended
/// for testing code that uses Kafka.
///
/// Translated from
/// `org.apache.kafka.clients.consumer.MockConsumer`.
///
/// This struct is NOT thread-safe. However, you can use
/// [`MockConsumer::schedule_poll_task`] to write multi-task tests where one
/// task waits for [`Consumer::poll`] to be
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
    /// `MockConsumer`'s [`Consumer`] trait API is
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
    /// Atomic so [`Consumer::wakeup`] can
    /// take `&self` (callable from any task / signal handler). `SeqCst`
    /// because wakeup is rare and reorder reasoning is not worth the win.
    /// `Arc` so [`Consumer::handle`] can hand a shareable clone to
    /// another task.
    wakeup: Arc<AtomicBool>,
    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    poll_error: Option<Error>,
    offsets_error: Option<Error>,
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
            wakeup: Arc::new(AtomicBool::new(false)),
            records: HashMap::new(),
            poll_error: None,
            offsets_error: None,
            last_poll_timeout: None,
            closed: false,
            should_rebalance: false,
            max_poll_records: i64::MAX,
        }
    }

    // ── Driver methods (mock-specific) ──────────────────────────────────

    /// Add a record to the buffer that the next [`Consumer::poll`]
    /// call will return. The record's `(topic, partition)` must already be
    /// assigned to the consumer.
    ///
    /// Translates Java's `addRecord(ConsumerRecord<K, V>)`
    /// (`MockConsumer.java:322`). Returns
    /// [`Error::LocalIllegalState`] if the partition is not assigned —
    /// Java throws `IllegalStateException` in that case.
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) -> Result<(), Error> {
        self.ensure_not_closed()?;
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        if !self.subscriptions.assigned_partitions().contains(&tp) {
            return Err(Error::local_illegal_state(
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
    /// [`Consumer::end_offsets`] /
    /// [`Consumer::current_lag`].
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
    /// [`Consumer::partitions_for`]
    /// and [`Consumer::list_topics`].
    ///
    /// Translates Java's `updatePartitions(String, List<PartitionInfo>)`.
    pub fn update_partitions(&mut self, topic: &str, partitions: Vec<PartitionInfo>) -> Result<(), Error> {
        self.ensure_not_closed()?;
        self.partitions.insert(topic.to_string(), partitions);
        Ok(())
    }

    /// Inject an exception to be returned by the next
    /// [`Consumer::poll`] call. The
    /// exception is taken (cleared) on use.
    ///
    /// Translates Java's `setPollException(KafkaException)`.
    pub fn set_poll_error(&mut self, error: Error) {
        self.poll_error = Some(error);
    }

    /// Inject an exception to be returned by the next
    /// [`Consumer::beginning_offsets`] /
    /// [`Consumer::end_offsets`]
    /// call. The exception is taken (cleared) on use.
    ///
    /// Translates Java's `setOffsetsException(KafkaException)`.
    pub fn set_offsets_error(&mut self, error: Error) {
        self.offsets_error = Some(error);
    }

    /// Set the maximum number of records returned in a single
    /// [`Consumer::poll`] call.
    ///
    /// Translates Java's `setMaxPollRecords(long)`. Returns
    /// [`Error::LocalIllegalArgument`] when `max_poll_records < 1`, matching
    /// Java's `IllegalArgumentException`.
    pub fn set_max_poll_records(&mut self, max_poll_records: i64) -> Result<(), Error> {
        if max_poll_records < 1 {
            return Err(Error::local_illegal_argument("MaxPollRecords must be strictly superior to 0"));
        }
        self.max_poll_records = max_poll_records;
        Ok(())
    }

    /// Simulate a rebalance event: compute revoked / added partitions
    /// against the current assignment, invoke any registered
    /// [`ConsumerRebalanceListener`]
    /// callbacks, and replace the assignment.
    ///
    /// Translates Java's `rebalance(Collection<TopicPartition>)`. Async
    /// because the Rust listener methods are `async fn` (per
    /// `consumer-threading.md` §31 / Phase 2); Java's listener methods
    /// are sync.
    pub async fn rebalance(&mut self, new_assignment: &[TopicPartition]) -> Result<(), Error> {
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
    /// [`Consumer::poll`] call. One task
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
    /// [`Consumer::poll`] call, or `None`
    /// if `poll` has not been called yet.
    ///
    /// Translates Java's `lastPollTimeout()`.
    pub fn last_poll_timeout(&self) -> Option<Duration> {
        self.last_poll_timeout
    }

    // ── Internal helpers (used by both inherent + Consumer impls) ───────

    /// Mirrors Java's `ensureNotClosed`. Java throws
    /// `IllegalStateException("This consumer has already been closed.")`.
    fn ensure_not_closed(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::local_illegal_state("This consumer has already been closed."))
        } else {
            Ok(())
        }
    }

    /// Mirrors Java's private
    /// `subscribe(Collection<String>, Optional<ConsumerRebalanceListener>)`
    /// (`MockConsumer.java:196-200`).
    fn subscribe_internal_topics(
        &mut self,
        topics: Vec<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        self.ensure_not_closed()?;
        self.committed.clear();
        self.subscriptions
            .subscribe_with_topics(topics.into_iter().collect(), listener)?;
        Ok(())
    }

    // Java's private `subscribe(Pattern, Optional<ConsumerRebalanceListener>)`
    // (`MockConsumer.java:202-222`) has no Rust counterpart: the two public
    // `subscribe(Pattern ...)` overloads it serves are deliberately NOT
    // implemented (see `Consumer`), so the client-side matching loop it
    // contained has no caller. Only the `SubscriptionPattern` form below is
    // translated.

    /// Mirrors Java's private
    /// `subscribe(SubscriptionPattern, Optional<ConsumerRebalanceListener>)`
    /// (`MockConsumer.java:180-186`).
    fn subscribe_internal_subscription_pattern(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        // Java line 181-182: an empty pattern is rejected. Java also rejects
        // `null`, which Rust's non-`Option` parameter makes unrepresentable,
        // so only the "empty" half of the message can be produced.
        if pattern.pattern().is_empty() {
            return Err(Error::local_illegal_argument("Topic pattern cannot be empty"));
        }
        self.ensure_not_closed()?;
        self.committed.clear();
        self.subscriptions.subscribe_with_pattern(pattern, listener)?;
        Ok(())
    }

    /// Mirrors Java's `updateFetchPosition(TopicPartition)`
    /// (`MockConsumer.java:622-631`).
    fn update_fetch_position(&mut self, tp: &TopicPartition) -> Result<(), Error> {
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
    /// Returns [`Error::LocalIllegalState`] when the map does not have an
    /// entry for the partition (Java throws `IllegalStateException`), or
    /// [`Error::ConsumerNoOffsetForPartition`](crate::common::Error::ConsumerNoOffsetForPartition)
    /// (Java's `NoOffsetForPartitionException`) when the strategy is `None`.
    fn reset_offset_position(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        let strategy = self.subscriptions.reset_strategy(tp)?.unwrap_or(AutoOffsetResetStrategy::NONE);

        if strategy == AutoOffsetResetStrategy::EARLIEST {
            let offset = self.beginning_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state(
                    "MockConsumer didn't have beginning offset specified, but tried to seek to beginning",
                )
            })?;
            self.subscriptions.seek(tp, offset)
        } else if strategy == AutoOffsetResetStrategy::LATEST {
            let offset = self.end_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state("MockConsumer didn't have end offset specified, but tried to seek to end")
            })?;
            self.subscriptions.seek(tp, offset)
        } else if strategy.type_() == StrategyType::ByDuration {
            let offset = self.duration_reset_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state(
                    "MockConsumer didn't have duration offset specified, but tried to seek to timestamp",
                )
            })?;
            self.subscriptions.seek(tp, offset)
        } else {
            // strategy == NONE
            Err(crate::consumer::Error::ConsumerNoOffsetForPartition(
                ConsumerNoOffsetForPartitionError::new(tp.clone()),
            ))
        }
    }
}

// ─── Consumer<K, V> trait impl ──────────────────────────────────────────

#[async_trait]
impl<K, V> Consumer<K, V> for MockConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    // ── Sync accessors ─────────────────────────────────────────────────

    fn assignment(&self) -> HashSet<TopicPartition> {
        self.subscriptions.assigned_partitions()
    }

    fn subscription(&self) -> HashSet<String> {
        self.subscriptions.subscription()
    }

    fn paused(&self) -> HashSet<TopicPartition> {
        // Java line 613-615: returns a copy of `paused`.
        self.paused.clone()
    }

    fn group_metadata(&self) -> ConsumerGroupMetadata {
        // Java line 692-693: hard-coded sentinel values.
        #[allow(deprecated)]
        // ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id is the only way to set the fields.
        ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id("dummy.group.id", 1, "1", None)
    }

    fn client_id(&self) -> &str {
        MOCK_CLIENT_ID
    }

    /// Translates Java's
    /// `synchronized Map<MetricName, ? extends Metric> metrics()`
    /// (`MockConsumer.java:496-499`).
    ///
    /// Java: `ensureNotClosed(); return Collections.emptyMap();`. The mock
    /// has no metrics registry, so the Rust port returns an empty map,
    /// matching Java. (The Rust mock's sync accessors do not panic on a
    /// closed consumer — see the other accessors — so the `ensureNotClosed`
    /// guard is not replicated here; the result is the same empty map either
    /// way.)
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        HashMap::new()
    }

    /// Translates Java's
    /// `OptionalLong currentLag(TopicPartition)`
    /// (`MockConsumer.java:681-688`).
    ///
    /// Behavior summary:
    /// - Returns `None` if the partition is **not assigned** to this
    ///   consumer. (Java throws `IllegalArgumentException` from the inner
    ///   `position(tp)` call in that case; Rust's `&self` API can't throw,
    ///   so `None` is the closest analog and lets the caller distinguish
    ///   "unassigned" from a real lag of `0`.)
    /// - Returns `Some(0)` if assigned but no end offset is known (Java's
    ///   "caught up" model — `endOffsets` has no entry for the partition).
    /// - Returns `Some(0)` if assigned with end offset known but no
    ///   position has been set yet. Java's `position(tp)` would call
    ///   `updateFetchPosition` to seed a position; the Rust `&self` API
    ///   cannot mutate, so `Some(0)` is the least-surprising fallback for
    ///   the caught-up model.
    /// - Returns `Some(end - position)` otherwise.
    ///
    /// Divergence from Java: Java throws on unassigned partitions; Rust
    /// returns `None`. Callers needing the strict Java behavior must
    /// pre-check `assignment().contains(tp)` themselves.
    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64> {
        // Unassigned → None (diverges from Java, which throws). See rustdoc.
        if !self.subscriptions.is_assigned(topic_partition) {
            return None;
        }
        match self.end_offsets.get(topic_partition).copied() {
            // No end offset known: Java's "caught up" model.
            None => Some(0),
            Some(end) => {
                // Read position WITHOUT triggering update_fetch_position
                // (which requires &mut self and is not available here).
                // If the partition has no valid position yet, fall back to
                // the caught-up model.
                match self.subscriptions.position_or_null(topic_partition) {
                    Some(p) => Some(end - p.offset),
                    None => Some(0),
                }
            },
        }
    }

    // ── Subscription / assignment ──────────────────────────────────────

    async fn subscribe_with_topics(&mut self, topics: Vec<String>) -> Result<(), Error> {
        self.subscribe_internal_topics(topics, None)
    }

    async fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error> {
        self.subscribe_internal_topics(topics, Some(listener))
    }

    async fn subscribe_with_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), Error> {
        self.subscribe_internal_subscription_pattern(pattern, None)
    }

    async fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error> {
        self.subscribe_internal_subscription_pattern(pattern, Some(listener))
    }

    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Error> {
        // Java line 235-239.
        self.ensure_not_closed()?;
        self.committed.clear();
        self.subscriptions.assign_from_user(partitions.into_iter().collect())?;
        Ok(())
    }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        // Java line 242-246.
        self.ensure_not_closed()?;
        self.committed.clear();
        self.subscriptions.unsubscribe();
        Ok(())
    }

    // ── Poll ────────────────────────────────────────────────────────────

    /// Translates Java's `poll(Duration)` (`MockConsumer.java:249-320`).
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error> {
        // Step 1: ensureNotClosed (Java line 250).
        self.ensure_not_closed()?;

        // Step 2: record the timeout (Java line 252).
        self.last_poll_timeout = Some(timeout);

        // Step 3: drain one queued poll task (Java line 256-260). Pop with
        // pop_front then invoke with &mut self — the task may schedule more
        // tasks, but only one is consumed per poll call.
        if let Some(task) = self.poll_tasks.pop_front() {
            task(self);
        }

        // Step 4: check wakeup flag and clear it (Java line 262-265).
        if self
            .wakeup
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(Error::wakeup("Mock consumer was woken up"));
        }

        // Step 5: take poll exception (Java line 267-271).
        if let Some(error) = self.poll_error.take() {
            return Err(error);
        }

        // Step 6: update fetch positions for newly-assigned partitions that
        // do not yet have a valid position (Java line 274-276).
        let assigned: Vec<TopicPartition> = self.subscriptions.assigned_partitions().into_iter().collect();
        for tp in &assigned {
            if !self.subscriptions.has_valid_position(tp) {
                self.update_fetch_position(tp)?;
            }
        }

        // Step 7: drain records up to max_poll_records (Java line 279-317).
        // Java iterates `records.entrySet()` with mutation; we collect the
        // partition keys first to avoid simultaneous mutable borrows.
        let mut results: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>> = IndexMap::new();
        let mut next_offset_and_metadata: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        let mut num_poll_records: i64 = 0;

        // Snapshot the partition keys in insertion order — match Java which
        // iterates the HashMap.entrySet() in whatever order Java's HashMap
        // produces. Test ordering does not depend on this.
        let partition_keys: Vec<TopicPartition> = self.records.keys().cloned().collect();

        for tp in partition_keys {
            if num_poll_records >= self.max_poll_records {
                break;
            }
            // AK 4.3.1: skip the whole partition if it is paused OR not
            // assigned. Java moved the `isAssigned` check up to the partition
            // level (`!subscriptions.isPaused(tp) && subscriptions.isAssigned(tp)`)
            // and dropped the per-record `assignment().contains(tp)` check.
            if self.subscriptions.is_paused(&tp) || !self.subscriptions.is_assigned(&tp) {
                continue;
            }

            // Java's recIterator removes records in place; we take ownership
            // of the partition's record list, drain it, and put the remainder
            // back if any records are left.
            let mut recs = self.records.remove(&tp).expect("key from records.keys()");
            let mut kept: Vec<ConsumerRecord<K, V>> = Vec::new();
            let mut iter = recs.drain(..);
            while let Some(rec) = iter.next() {
                if num_poll_records >= self.max_poll_records {
                    // Push back the un-iterated remainder.
                    kept.push(rec);
                    kept.extend(iter);
                    break;
                }

                // Read the current position. position_or_null returns the
                // FetchPosition; we need its offset.
                let position_opt = self.subscriptions.position_or_null(&tp).map(|p| p.offset);
                let Some(position) = position_opt else {
                    // No valid position — preserve Java's behavior:
                    // `subscriptions.position(...).offset` would NPE in Java
                    // if position were null, but Java's loop only reaches
                    // here AFTER updateFetchPosition has been called on the
                    // partition (Step 6). If the partition has no position
                    // here, it means the position was unset; skip the record.
                    kept.push(rec);
                    kept.extend(iter);
                    break;
                };

                // OffsetOutOfRange check (Java line 297-299): if a beginning
                // offset is configured and position is before it, throw.
                if let Some(&begin) = self.beginning_offsets.get(&tp)
                    && begin > position
                {
                    let mut m = HashMap::new();
                    m.insert(tp.clone(), position);
                    // Put the un-iterated remainder back BEFORE returning so
                    // a subsequent poll sees the same record at the same
                    // position.
                    kept.push(rec);
                    kept.extend(iter);
                    self.records.insert(tp, kept);
                    return Err(crate::consumer::Error::ConsumerOffsetOutOfRange(
                        ConsumerOffsetOutOfRangeError::new(m),
                    ));
                }

                // AK 4.3.1 (MockConsumer): the per-partition `isAssigned`
                // check now sits at the entry level above, so the per-record
                // guard is just `rec.offset() >= position`.
                if rec.offset() >= position {
                    let leader_epoch = rec.leader_epoch();
                    let next_offset = rec.offset() + 1;

                    // Push the record onto the result list.
                    results.entry(tp.clone()).or_default().push(rec);

                    // Update the partition's fetch position (Java line
                    // 303-306). Java uses `subscriptions.position(entry, fp)`
                    // (not `seek`), which requires a valid current position.
                    let leader_and_epoch = LeaderAndEpoch::new(None, leader_epoch);
                    let new_position = FetchPosition::with_leader(next_offset, leader_epoch, leader_and_epoch);
                    self.subscriptions.set_position(&tp, new_position)?;

                    // Build the next-offsets entry (Java line 307).
                    let oam = OffsetAndMetadata::with_leader_epoch_metadata(next_offset, leader_epoch, String::new())?;
                    next_offset_and_metadata.insert(tp.clone(), oam);

                    num_poll_records += 1;
                } else {
                    // Java's loop simply does NOT remove the record (no
                    // `recIterator.remove()` call). Keep it for the next poll.
                    kept.push(rec);
                }
            }

            // Java line 313-315: drop empty entries from the records map.
            if !kept.is_empty() {
                self.records.insert(tp, kept);
            }
            // If kept is empty, the entry was implicitly removed by the
            // `remove` above — no further action needed.
        }

        Ok(ConsumerRecords::with_next_offsets(results, next_offset_and_metadata))
    }

    // ── Commit ─────────────────────────────────────────────────────────

    async fn commit_sync(&mut self) -> Result<(), Error> {
        // Java's `commitSync()` (MockConsumer.java:378-380) reads
        // `allConsumed()` BEFORE the closed check —
        // `commitSync(allConsumed())` → line 362-364 →
        // `commitAsync(offsets, null)` at line 353-358 where
        // `ensureNotClosed()` is finally called. We tighten by checking
        // closed first: end behavior is identical (both error when the
        // consumer is closed) but Rust skips the wasted subscription-map
        // traversal.
        self.ensure_not_closed()?;
        let offsets = self.subscriptions.all_consumed();
        self.commit_async_impl(offsets, None).await
    }

    async fn commit_sync_with_timeout(&mut self, _timeout: Duration) -> Result<(), Error> {
        // Java line 383-385: ignores the timeout.
        self.commit_sync().await
    }

    async fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error> {
        // Java line 362-364: delegates to commitAsync.
        self.commit_async_impl(offsets, None).await
    }

    async fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _timeout: Duration,
    ) -> Result<(), Error> {
        // Java line 388-390: ignores the timeout.
        self.commit_sync_with_offsets(offsets).await
    }

    async fn commit_async(&mut self) -> Result<(), Error> {
        // Java line 367-369 → line 372-375 which `ensureNotClosed()`s BEFORE
        // reading `allConsumed()`. Mirror that ordering so a closed consumer
        // errors out without traversing the subscription map.
        self.ensure_not_closed()?;
        let offsets = self.subscriptions.all_consumed();
        self.commit_async_impl(offsets, None).await
    }

    async fn commit_async_with_callback(&mut self, callback: Arc<dyn OffsetCommitCallback>) -> Result<(), Error> {
        // Java line 372-375 calls `ensureNotClosed()` BEFORE `allConsumed()`.
        // Rust relies on `commit_async_impl`'s check at the cost of a wasted
        // `all_consumed()` traversal when the consumer is already closed.
        // Keeping the check only in `commit_async_impl` removes the redundant
        // double-check the previous code had. (Unlike `commit_sync` /
        // `commit_async`, which deliberately pre-check to skip that traversal,
        // this path is left as-is because all `*_offsets` / callback variants
        // funnel into `commit_async_impl` for the single closed check.)
        let offsets = self.subscriptions.all_consumed();
        self.commit_async_impl(offsets, Some(callback)).await
    }

    async fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), Error> {
        // Java line 353-359.
        self.commit_async_impl(offsets, Some(callback)).await
    }

    // ── Seek ───────────────────────────────────────────────────────────

    async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error> {
        // Java line 393-396.
        self.ensure_not_closed()?;
        self.subscriptions.seek(&partition, offset)?;
        Ok(())
    }

    async fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error> {
        // Java line 398-402.
        self.ensure_not_closed()?;
        self.subscriptions.seek(&partition, offset_and_metadata.offset())?;
        Ok(())
    }

    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java line 438-441.
        self.ensure_not_closed()?;
        self.subscriptions
            .request_offset_reset_all(partitions, AutoOffsetResetStrategy::EARLIEST)?;
        Ok(())
    }

    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java line 448-451.
        self.ensure_not_closed()?;
        self.subscriptions
            .request_offset_reset_all(partitions, AutoOffsetResetStrategy::LATEST)?;
        Ok(())
    }

    // ── Position / committed ───────────────────────────────────────────

    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error> {
        // Java line 420-430.
        self.ensure_not_closed()?;
        if !self.subscriptions.is_assigned(partition) {
            return Err(Error::local_illegal_argument(
                "You can only check the position for partitions assigned to this consumer.",
            ));
        }
        // First read; if absent, refresh via update_fetch_position and re-read.
        let pos = self.subscriptions.position_or_null(partition).map(|p| p.offset);
        if let Some(off) = pos {
            return Ok(off);
        }
        self.update_fetch_position(partition)?;
        let pos = self
            .subscriptions
            .position_or_null(partition)
            .map(|p| p.offset)
            .ok_or_else(|| {
                Error::local_illegal_state(format!(
                    "Position for partition {partition} is still unset after update_fetch_position",
                ))
            })?;
        Ok(pos)
    }

    async fn position_with_timeout(&mut self, partition: &TopicPartition, _timeout: Duration) -> Result<i64, Error> {
        // Java line 433-435: ignores the timeout.
        self.position(partition).await
    }

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        // Java line 405-412.
        self.ensure_not_closed()?;
        let mut result = HashMap::new();
        for tp in partitions {
            if let Some(om) = self.committed.get(tp) {
                // Java: `subscriptions.isAssigned(tp) ? committed.get(tp) : new OffsetAndMetadata(0)`.
                let value = if self.subscriptions.is_assigned(tp) {
                    om.clone()
                } else {
                    // `OffsetAndMetadata::new(0)` is infallible for offset=0.
                    OffsetAndMetadata::new(0).expect("0 is non-negative")
                };
                result.insert(tp.clone(), value);
            }
        }
        Ok(result)
    }

    async fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        // Java line 415-417: ignores the timeout.
        self.committed(partitions).await
    }

    // ── Metadata ───────────────────────────────────────────────────────

    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
        // Java line 502-505.
        self.ensure_not_closed()?;
        Ok(self.partitions.get(topic).cloned().unwrap_or_default())
    }

    async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        _timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, Error> {
        // Java line 655-657.
        self.partitions_for(topic).await
    }

    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<PartitionInfo>>, Error> {
        // Java line 508-511. Java returns the internal map reference; Rust
        // returns a clone so the caller is not exposed to subsequent
        // mutations of the mock's state.
        self.ensure_not_closed()?;
        Ok(self.partitions.clone())
    }

    async fn list_topics_with_timeout(
        &mut self,
        _timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, Error> {
        // Java line 660-662.
        self.list_topics().await
    }

    async fn offsets_for_times(
        &mut self,
        _timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        // Java line 534-537 throws UnsupportedOperationException("Not
        // implemented yet."). Rust analog is Error::unsupported_version
        // (`admin-client.md` §9). That rule requires a *faithful* translation
        // and DoD #3 makes the message text part of the contract, so the text
        // is Java's verbatim rather than a Rust-side restatement.
        Err(Error::unsupported_version("Not implemented yet."))
    }

    async fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        // Java line 665-668: delegates to the no-timeout variant.
        self.offsets_for_times(timestamps_to_search).await
    }

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java line 540-554.
        if let Some(error) = self.offsets_error.take() {
            return Err(error);
        }
        let mut result = HashMap::new();
        for tp in partitions {
            let off = self.beginning_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state(format!("The partition {tp} does not have a beginning offset."))
            })?;
            result.insert(tp.clone(), off);
        }
        Ok(result)
    }

    async fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java line 671-673.
        self.beginning_offsets(partitions).await
    }

    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java line 557-571.
        if let Some(error) = self.offsets_error.take() {
            return Err(error);
        }
        let mut result = HashMap::new();
        for tp in partitions {
            let off = self.end_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state(format!("The partition {tp} does not have an end offset."))
            })?;
            result.insert(tp.clone(), off);
        }
        Ok(result)
    }

    async fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java line 676-678.
        self.end_offsets(partitions).await
    }

    // ── Pause / resume ─────────────────────────────────────────────────

    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java line 519-524.
        for tp in partitions {
            self.subscriptions.pause(tp)?;
            self.paused.insert(tp.clone());
        }
        Ok(())
    }

    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java line 527-532.
        for tp in partitions {
            self.subscriptions.resume(tp)?;
            self.paused.remove(tp);
        }
        Ok(())
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    async fn enforce_rebalance(&mut self) -> Result<(), Error> {
        // Java line 697-699: `enforceRebalance()` forwards to
        // `enforceRebalance(null)`; Rust's suffixed form takes a non-null
        // `&str`, so the flag is set directly here instead.
        self.should_rebalance = true;
        Ok(())
    }

    async fn enforce_rebalance_with_reason(&mut self, _reason: &str) -> Result<(), Error> {
        // Java line 702-704: sets the flag; the reason is ignored.
        self.should_rebalance = true;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Java line 574-576: `close()` delegates to
        // `close(CloseOptions.timeout(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS)))`.
        self.close_with_options(CloseOptions::new_timeout(Duration::from_millis(
            CloseOptions::DEFAULT_CLOSE_TIMEOUT_MS,
        )))
        .await
    }

    #[allow(deprecated)]
    async fn close_with_timeout(&mut self, _timeout: Duration) -> Result<(), Error> {
        // Java line 578-582: `@Deprecated close(Duration)` sets the flag
        // directly; unlike `AsyncKafkaConsumer` it does NOT forward to
        // `close(CloseOptions.timeout(..))`.
        self.closed = true;
        Ok(())
    }

    async fn close_with_options(&mut self, _options: CloseOptions) -> Result<(), Error> {
        // Java line 594-596: ignores the options.
        self.closed = true;
        Ok(())
    }

    fn wakeup(&self) {
        // Java line 589-591: sets the flag.
        self.wakeup.store(true, Ordering::SeqCst);
    }

    fn handle(&self) -> ConsumerHandle {
        ConsumerHandle::for_mock(Arc::clone(&self.wakeup))
    }
}

impl<K, V> MockConsumer<K, V> {
    /// Shared implementation for all `commit_*` variants. Java's
    /// `commitAsync(Map, OffsetCommitCallback)` (`MockConsumer.java:353-358`)
    /// invokes the callback inline (synchronously) — the Rust translation
    /// awaits the callback before returning, matching that contract.
    async fn commit_async_impl(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Option<Arc<dyn OffsetCommitCallback>>,
    ) -> Result<(), Error> {
        self.ensure_not_closed()?;
        // Invoke the callback FIRST with a borrowed reference, then move the
        // map into `self.committed`. Avoids the clone that would otherwise be
        // required to satisfy both `extend(offsets)` (consumes the map) and
        // `on_complete(&offsets, ...)` (borrows the map).
        //
        // Java's ordering (`MockConsumer.java:355-358`) is
        // `committed.putAll(offsets); callback.onComplete(offsets, null);` —
        // the callback observes the post-merge state. Rust observes the
        // pre-merge state. The only practical divergence is a callback that
        // calls back into `committed(...)`; no Java test exercises this and
        // the MockConsumer contract does not document the order.
        if let Some(cb) = callback {
            cb.on_complete(&offsets, None).await;
        }
        self.committed.extend(offsets);
        Ok(())
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

    /// Java `MockConsumer.metrics()` returns `Collections.emptyMap()`
    /// (`MockConsumer.java:496-499`). The Rust port returns an empty map.
    #[test]
    fn test_metrics_returns_empty_map() {
        let c: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
        assert!(Consumer::metrics(&c).is_empty());
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
        assert!(matches!(err, Error::LocalIllegalState(_)));
    }

    /// A [`ConsumerHandle`] obtained from the mock fires the SAME wakeup
    /// flag as `wakeup()`: the next `poll` observes it and returns `Wakeup`.
    #[tokio::test]
    async fn handle_wakeup_wakes_next_poll() {
        let mut c: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);

        // Handle is moved into another task — no reference to the consumer
        // crosses the boundary.
        let handle = c.handle();
        tokio::spawn(async move {
            handle.wakeup();
        })
        .await
        .expect("waker task");

        let err = c.poll(Duration::from_millis(0)).await.unwrap_err();
        assert!(matches!(err, Error::Wakeup(_)), "expected Wakeup, got {err:?}");
    }

    /// Java's `MockConsumer.offsetsForTimes` throws
    /// `UnsupportedOperationException("Not implemented yet.")`
    /// (`MockConsumer.java:534-537`). The message text is part of the contract
    /// (DoD #3), and `admin-client.md` §9 sanctions the
    /// `UnsupportedOperationException` -> `unsupported_version` mapping only as
    /// a *faithful* translation — so the text must be Java's, not a Rust-side
    /// restatement.
    #[tokio::test]
    async fn offsets_for_times_reports_javas_not_implemented_message() {
        let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
        let err = consumer
            .offsets_for_times(HashMap::new())
            .await
            .expect_err("Java's mock does not implement this");
        assert_eq!("Not implemented yet.", err.message());
    }
}
