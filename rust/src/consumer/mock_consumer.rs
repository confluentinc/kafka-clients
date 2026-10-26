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
//! a `C: Consumer<K, V>` or a `&mut dyn DynConsumer<K, V>` is expected (see
//! `consumer-threading.md` §2).
//!
//! Threading: `MockConsumer` is `!Sync` and intended for single-task use
//! through the `&mut self` dispatch surface. `wakeup()` is callable from
//! any task because it takes `&self` and uses an atomic flag.

use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use indexmap::IndexMap;

use crate::common::metrics::KafkaMetric;
use crate::common::{Error, MetricName, PartitionInfo, TopicPartition};
use crate::consumer::internals::AutoOffsetResetStrategy;
use crate::consumer::internals::StrategyType;
use crate::consumer::internals::{FetchPosition, SubscriptionState};
use crate::consumer::{
    CloseOptions, Consumer, ConsumerGroupMetadata, ConsumerGroupMetadataImpl, ConsumerHandle,
    ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, OffsetAndMetadata, OffsetAndTimestamp,
    OffsetCommitCallback, SubscriptionPattern,
};
use crate::consumer::{ConsumerNoOffsetForPartitionError, ConsumerOffsetOutOfRangeError};
use crate::metadata::LeaderAndEpoch;

/// One scheduled poll task — see [`MockConsumer::schedule_poll_task`].
///
/// The deviation from Java's parameterless `Runnable` is documented on the
/// `schedule_poll_task` method; the task receives an explicit
/// `&mut MockConsumer<K, V>` because Rust closures cannot safely capture
/// the outer struct.
pub type PollTask<K, V> = Box<dyn FnOnce(&mut MockConsumer<K, V>) + Send>;

/// A mock of the [`Consumer`] interface you can use for testing code that
/// uses Kafka. This class is *not threadsafe*. However, you can use the
/// [`Self::schedule_poll_task`] method to write multithreaded tests where a
/// driver thread waits for [`Consumer::poll`] to be called by a background
/// thread and then can safely perform operations during a callback.
///
/// # Translation notes
///
/// The mock is `Send` but not `Sync`: another task reaches it through
/// [`Consumer::handle`].
///
/// ```compile_fail,E0277
/// use confluent_kafka::consumer::MockConsumer;
///
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<MockConsumer<String, String>>();
/// ```
// Java's `MockConsumer(String offsetResetStrategy)` is `MockConsumer::new`. The
// deprecated `MockConsumer(OffsetResetStrategy)` and the deprecated
// `OffsetResetStrategy` enum are not translated (CLAUDE.md §3); that constructor
// only forwards `offsetResetStrategy.toString()`, so Rust callers pass the
// `auto.offset.reset` string to `MockConsumer::new` instead. Like `KafkaConsumer`,
// the mock is `Send` but not `Sync` (`consumer-threading.md` §2).
#[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer")]
pub struct MockConsumer<K, V> {
    partitions: HashMap<String, Vec<PartitionInfo>>,
    /// Shared (`consumer-threading.md` §16: a `std::sync::Mutex`, never held
    /// across an `.await` or a listener call) so a [`Consumer::handle`]
    /// obtained from this mock reads the assignment and commits the consumed
    /// positions as a Java callback calling back into the mock does
    /// (`commitSync()` is `commitSync(subscriptions.allConsumed())`).
    subscriptions: Arc<Mutex<SubscriptionState>>,
    beginning_offsets: HashMap<TopicPartition, i64>,
    /// Shared so a [`Consumer::handle`] obtained from this mock computes
    /// `current_lag` over the same end offsets, as a Java callback calling
    /// `consumer.currentLag(tp)` on the mock does.
    end_offsets: Arc<Mutex<HashMap<TopicPartition, i64>>>,
    duration_reset_offsets: HashMap<TopicPartition, i64>,
    /// Shared so a [`Consumer::handle`] obtained from this mock commits into
    /// the same map, as a Java callback calling back into the mock does.
    committed: Arc<Mutex<HashMap<TopicPartition, OffsetAndMetadata>>>,
    /// Java stores `Runnable` tasks that capture `MockConsumer` implicitly.
    /// Rust closures cannot capture the outer struct safely without
    /// `Arc<Mutex<...>>` wrapping, so we deviate: tasks receive `&mut
    /// MockConsumer<K, V>` explicitly. Tests write
    /// `consumer.schedule_poll_task(Box::new(|c| c.add_record(...).unwrap()));`.
    poll_tasks: VecDeque<PollTask<K, V>>,
    /// Shared for the same reason as `subscriptions`: the handle's `paused()`
    /// is the mock's `paused()`.
    paused: Arc<Mutex<HashSet<TopicPartition>>>,
    /// Atomic so [`Consumer::wakeup`] can
    /// take `&self` (callable from any task / signal handler). `SeqCst`
    /// because wakeup is rare and reorder reasoning is not worth the win.
    /// `Arc` so [`Consumer::handle`] can hand a shareable clone to
    /// another task.
    wakeup: Arc<AtomicBool>,
    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    poll_error: Option<Error>,
    offsets_error: Option<Error>,
    /// The three fields Java reads outside the mock's monitor
    /// (`lastPollTimeout()`, `shouldRebalance()`) or under it while waiting
    /// (`closed()`), shared so [`Consumer::handle`] can answer them while an
    /// operation borrows the mock: the C binding reads them through the
    /// handle rather than its single-owner flag.
    last_poll_timeout: Arc<Mutex<Option<Duration>>>,
    closed: Arc<AtomicBool>,
    should_rebalance: Arc<AtomicBool>,
    /// `i64::MAX` is the "no cap" sentinel mirroring Java's `Long.MAX_VALUE`.
    max_poll_records: i64,
    /// Zero-sized: keeps the mock `Send` and makes it `!Sync` whatever its
    /// other fields are, as `KafkaConsumer` (see `Threads` above).
    not_sync: PhantomData<Cell<()>>,
}

impl<K, V> std::fmt::Debug for MockConsumer<K, V> {
    // Whether the mock is closed; the records and offsets it holds are not
    // printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockConsumer")
            .field("closed", &self.closed())
            .finish_non_exhaustive()
    }
}

impl<K, V> MockConsumer<K, V> {
    /// A mock consumer is instantiated by providing
    /// `ConsumerConfig::AUTO_OFFSET_RESET_CONFIG` value as the input.
    ///
    /// # Arguments
    ///
    /// - `offset_reset_strategy` - the offset reset strategy to use
    ///
    /// # Returns
    ///
    /// The new mock consumer.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `offset_reset_strategy` is not a valid
    ///   `auto.offset.reset` value
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#MockConsumer(String)")]
    pub fn new(offset_reset_strategy: &str) -> Result<Self, Error> {
        Ok(Self::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::from_string(
            offset_reset_strategy,
        )?))
    }

    /// Java's private `MockConsumer(AutoOffsetResetStrategy)`, which the public
    /// constructors delegate to.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#MockConsumer")]
    pub(crate) fn with_auto_offset_reset_strategy(offset_reset_strategy: AutoOffsetResetStrategy) -> Self {
        Self {
            partitions: HashMap::new(),
            subscriptions: Arc::new(Mutex::new(SubscriptionState::new(offset_reset_strategy))),
            beginning_offsets: HashMap::new(),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            duration_reset_offsets: HashMap::new(),
            committed: Arc::new(Mutex::new(HashMap::new())),
            poll_tasks: VecDeque::new(),
            paused: Arc::new(Mutex::new(HashSet::new())),
            wakeup: Arc::new(AtomicBool::new(false)),
            records: HashMap::new(),
            poll_error: None,
            offsets_error: None,
            last_poll_timeout: Arc::new(Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
            should_rebalance: Arc::new(AtomicBool::new(false)),
            max_poll_records: i64::MAX,
            not_sync: PhantomData,
        }
    }

    // ── Driver methods (mock-specific) ──────────────────────────────────

    /// Adds a record to be returned when [`Consumer::poll`] is called.
    ///
    /// # Arguments
    ///
    /// - `record` - the record to add
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the partition is not assigned to the consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#addRecord")]
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) -> Result<(), Error> {
        self.ensure_not_closed()?;
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        if !self.subs().assigned_partitions().contains(&tp) {
            return Err(Error::local_illegal_state(
                "Cannot add records for a partition that is not assigned to the consumer",
            ));
        }
        self.records.entry(tp).or_default().push(record);
        Ok(())
    }

    /// Updates the beginning offsets for the specified partitions.
    ///
    /// # Arguments
    ///
    /// - `offsets` - the beginning offsets to set
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#updateBeginningOffsets")]
    pub fn update_beginning_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        self.beginning_offsets.extend(offsets);
    }

    /// Updates the end offsets for the specified partitions.
    ///
    /// # Arguments
    ///
    /// - `offsets` - the end offsets to set
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#updateEndOffsets")]
    pub fn update_end_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        self.end_offsets_lock().extend(offsets);
    }

    /// Locks the shared end offsets; never held across an `.await`.
    fn end_offsets_lock(&self) -> std::sync::MutexGuard<'_, HashMap<TopicPartition, i64>> {
        self.end_offsets.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Updates the duration-based reset offsets for the specified partitions.
    ///
    /// # Arguments
    ///
    /// - `offsets` - the duration offsets to set
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#updateDurationOffsets")]
    pub fn update_duration_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        self.duration_reset_offsets.extend(offsets);
    }

    /// Updates the partition information for the specified topic.
    ///
    /// # Arguments
    ///
    /// - `topic` - the topic to update
    /// - `partitions` - the partition information
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the consumer is closed
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#updatePartitions")]
    pub fn update_partitions(&mut self, topic: &str, partitions: Vec<PartitionInfo>) -> Result<(), Error> {
        self.ensure_not_closed()?;
        self.partitions.insert(topic.to_string(), partitions);
        Ok(())
    }

    /// Sets an exception to throw when [`Consumer::poll`] is called.
    ///
    /// # Arguments
    ///
    /// - `error` - the exception to throw; `None` clears a pending one
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#setPollException")]
    pub fn set_poll_error(&mut self, error: Option<Error>) {
        self.poll_error = error;
    }

    /// Sets an exception to throw when offset-related methods are called.
    ///
    /// # Arguments
    ///
    /// - `error` - the exception to throw; `None` clears a pending one
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#setOffsetsException")]
    pub fn set_offsets_error(&mut self, error: Option<Error>) {
        self.offsets_error = error;
    }

    /// Sets the maximum number of records returned in a single call to
    /// [`Consumer::poll`].
    ///
    /// # Arguments
    ///
    /// - `max_poll_records` - the max.poll.records.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `max_poll_records` is less than 1
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#setMaxPollRecords")]
    pub fn set_max_poll_records(&mut self, max_poll_records: i64) -> Result<(), Error> {
        if max_poll_records < 1 {
            return Err(Error::local_illegal_argument("MaxPollRecords must be strictly superior to 0"));
        }
        self.max_poll_records = max_poll_records;
        Ok(())
    }

    /// Simulate a rebalance event.
    ///
    /// # Arguments
    ///
    /// - `new_assignment` - the new assignment
    ///
    /// # Errors
    ///
    /// - The error the listener returns.
    /// - `Error::is_local_illegal_argument_error` if a partition is not of a subscribed topic
    // Async because the Rust listener methods are `async fn`
    // (`consumer-threading.md` §31); Java's listener methods are sync.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#rebalance")]
    pub fn rebalance(&mut self, new_assignment: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send
    where
        K: Send,
        V: Send,
    {
        async move {
            let old_assignment_set = self.subs().assigned_partitions();
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

            // Clear buffered records (Java: `records.clear()`).
            self.records.clear();

            // Invoke onPartitionsRevoked first. Per
            // consumer-threading.md §16, clone the Arc out before awaiting.
            let listener = self.subs().rebalance_listener();
            if !removed.is_empty()
                && let Some(listener) = listener
            {
                listener.on_partitions_revoked(&removed).await?;
            }

            // Replace assignment (Java:
            // `subscriptions.assignFromSubscribed(newAssignment)`).
            self.subs().assign_from_subscribed(new_assignment)?;

            // Invoke onPartitionsAssigned. Java passes only the
            // *added* partitions, not the full new assignment.
            let listener = self.subs().rebalance_listener();
            if let Some(listener) = listener {
                listener.on_partitions_assigned(&added).await?;
            }

            Ok(())
        }
    }

    /// Simulates a partition loss event. Calls
    /// [`ConsumerRebalanceListener::on_partitions_lost`] for the specified
    /// partitions and removes them from the current assignment. Unlike
    /// [`Self::rebalance`], which calls
    /// [`ConsumerRebalanceListener::on_partitions_revoked`], this method models
    /// the case where the consumer loses partitions without a graceful revoke.
    ///
    /// Only records belonging to the lost partitions are cleared; records for
    /// retained partitions are unaffected.
    ///
    /// # Arguments
    ///
    /// - `partitions_lost` - the partitions to lose; all must be currently assigned
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if any partition is not currently assigned
    /// - The error the listener returns; the assignment is then left unchanged.
    ///
    /// [`ConsumerRebalanceListener::on_partitions_lost`]: crate::consumer::ConsumerRebalanceListener::on_partitions_lost
    /// [`ConsumerRebalanceListener::on_partitions_revoked`]: crate::consumer::ConsumerRebalanceListener::on_partitions_revoked
    // Async for the same reason as `rebalance`. Java hands the listener a
    // `Set`; the lost partitions are passed deduplicated, in the order given.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#losePartitions")]
    pub fn lose_partitions(
        &mut self,
        partitions_lost: &[TopicPartition],
    ) -> impl Future<Output = Result<(), Error>> + Send
    where
        K: Send,
        V: Send,
    {
        async move {
            let current_assignment = self.subs().assigned_partitions();
            let mut lost: Vec<TopicPartition> = Vec::with_capacity(partitions_lost.len());
            for tp in partitions_lost {
                if !lost.contains(tp) {
                    lost.push(tp.clone());
                }
            }
            let not_assigned: Vec<String> = lost
                .iter()
                .filter(|tp| !current_assignment.contains(*tp))
                .map(ToString::to_string)
                .collect();
            if !not_assigned.is_empty() {
                // Java formats the `List` with `toString()`: `[test-1, test-2]`.
                return Err(Error::local_illegal_state(format!(
                    "Cannot lose partitions that are not currently assigned: [{}]",
                    not_assigned.join(", ")
                )));
            }
            for tp in &lost {
                self.records.remove(tp);
            }
            // Clone the listener `Arc` out, dropping the guard, before awaiting
            // (consumer-threading.md §16).
            let listener = self.subs().rebalance_listener();
            if let Some(listener) = listener {
                listener.on_partitions_lost(&lost).await?;
            }
            let remaining: Vec<TopicPartition> =
                current_assignment.into_iter().filter(|tp| !lost.contains(tp)).collect();
            self.subs().assign_from_subscribed(&remaining)
        }
    }

    /// Schedule a task to be executed during a poll(). One enqueued task will
    /// be executed per [`Consumer::poll`] invocation. You can use this
    /// repeatedly to mock out multiple responses to poll invocations.
    ///
    /// # Arguments
    ///
    /// - `task` - the task to be executed
    ///
    /// # Translation notes
    ///
    /// The task is called with the mock, `&mut MockConsumer<K, V>`, which a
    /// Rust closure cannot capture.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#schedulePollTask")]
    pub fn schedule_poll_task(&mut self, task: PollTask<K, V>) {
        self.poll_tasks.push_back(task);
    }

    /// Schedules a no-op task to be executed during a poll invocation.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#scheduleNopPollTask")]
    pub fn schedule_nop_poll_task(&mut self) {
        self.schedule_poll_task(Box::new(|_| {}));
    }

    /// Return whether the consumer has been closed.
    ///
    /// # Returns
    ///
    /// true if closed, false otherwise
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#closed")]
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Return whether an `enforceRebalance` request is pending.
    ///
    /// # Returns
    ///
    /// true if rebalance is pending, false otherwise
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#shouldRebalance")]
    pub fn should_rebalance(&self) -> bool {
        self.should_rebalance.load(Ordering::SeqCst)
    }

    /// Resets the rebalance flag set by [`Consumer::enforce_rebalance`].
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#resetShouldRebalance")]
    pub fn reset_should_rebalance(&mut self) {
        self.should_rebalance.store(false, Ordering::SeqCst);
    }

    /// Returns the timeout used in the last [`Consumer::poll`] call.
    ///
    /// # Returns
    ///
    /// the last poll timeout, or None if poll has not been called
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#lastPollTimeout")]
    pub fn last_poll_timeout(&self) -> Option<Duration> {
        *self.last_poll_timeout.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // ── Internal helpers (used by both inherent + Consumer impls) ───────

    /// Mirrors Java's `ensureNotClosed`. Java throws
    /// `IllegalStateException("This consumer has already been closed.")`.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#ensureNotClosed")]
    fn ensure_not_closed(&self) -> Result<(), Error> {
        ensure_mock_not_closed(&self.closed)
    }

    /// The shared committed-offsets map, locked for a read or a merge.
    fn lock_committed(&self) -> std::sync::MutexGuard<'_, HashMap<TopicPartition, OffsetAndMetadata>> {
        self.committed.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Locks the shared `SubscriptionState`. Each caller drops the guard
    /// before awaiting or invoking a listener (`consumer-threading.md` §16),
    /// and never locks it twice in one expression: the mutex is not
    /// reentrant.
    fn subs(&self) -> std::sync::MutexGuard<'_, SubscriptionState> {
        self.subscriptions.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Mirrors Java's private
    /// `subscribe(Collection<String>, Optional<ConsumerRebalanceListener>)`
    /// (`org.apache.kafka.clients.consumer.MockConsumer#subscribe`).
    fn subscribe_internal_topics(
        &mut self,
        topics: Vec<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        self.ensure_not_closed()?;
        self.lock_committed().clear();
        self.subs().subscribe_with_topics(topics.into_iter().collect(), listener)?;
        Ok(())
    }

    // Java's private `subscribe(Pattern, Optional<ConsumerRebalanceListener>)`
    // (`org.apache.kafka.clients.consumer.MockConsumer#subscribe`) has no Rust counterpart: the two public
    // `subscribe(Pattern ...)` overloads it serves are deliberately NOT
    // implemented (see `Consumer`), so the client-side matching loop it
    // contained has no caller. Only the `SubscriptionPattern` form below is
    // translated.

    /// Mirrors Java's private
    /// `subscribe(SubscriptionPattern, Optional<ConsumerRebalanceListener>)`
    /// (`org.apache.kafka.clients.consumer.MockConsumer#subscribe`).
    fn subscribe_internal_subscription_pattern(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        // Java rejects an empty pattern. Java also rejects
        // `null`, which Rust's non-`Option` parameter makes unrepresentable,
        // so only the "empty" half of the message can be produced.
        if pattern.pattern().is_empty() {
            return Err(Error::local_illegal_argument("Topic pattern cannot be empty"));
        }
        self.ensure_not_closed()?;
        self.lock_committed().clear();
        self.subs().subscribe_with_pattern(pattern, listener)?;
        Ok(())
    }

    /// Mirrors Java's `updateFetchPosition(TopicPartition)`
    /// (`org.apache.kafka.clients.consumer.MockConsumer#updateFetchPosition`).
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#updateFetchPosition")]
    fn update_fetch_position(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        if self.subs().is_offset_reset_needed(tp)? {
            self.reset_offset_position(tp)
        } else {
            let committed_offset = self.lock_committed().get(tp).map(OffsetAndMetadata::offset);
            match committed_offset {
                None => {
                    self.subs().request_offset_reset_default(tp)?;
                    self.reset_offset_position(tp)
                },
                Some(offset) => self.subs().seek(tp, offset),
            }
        }
    }

    /// Mirrors Java's `resetOffsetPosition(TopicPartition)`
    /// (`org.apache.kafka.clients.consumer.MockConsumer#resetOffsetPosition`). Picks the source map by the
    /// partition's reset strategy and `seek`s to the configured offset.
    /// Returns [`Error::LocalIllegalState`] when the map does not have an
    /// entry for the partition (Java throws `IllegalStateException`), or
    /// [`Error::ConsumerNoOffsetForPartition`](crate::common::Error::ConsumerNoOffsetForPartition)
    /// (Java's `NoOffsetForPartitionException`) when the strategy is `None`.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockConsumer#resetOffsetPosition")]
    fn reset_offset_position(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        let strategy = self.subs().reset_strategy(tp)?.unwrap_or(AutoOffsetResetStrategy::NONE);

        if strategy == AutoOffsetResetStrategy::EARLIEST {
            let offset = self.beginning_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state(
                    "MockConsumer didn't have beginning offset specified, but tried to seek to beginning",
                )
            })?;
            self.subs().seek(tp, offset)
        } else if strategy == AutoOffsetResetStrategy::LATEST {
            let offset = self.end_offsets_lock().get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state("MockConsumer didn't have end offset specified, but tried to seek to end")
            })?;
            self.subs().seek(tp, offset)
        } else if strategy.type_() == StrategyType::ByDuration {
            let offset = self.duration_reset_offsets.get(tp).copied().ok_or_else(|| {
                Error::local_illegal_state(
                    "MockConsumer didn't have duration offset specified, but tried to seek to timestamp",
                )
            })?;
            self.subs().seek(tp, offset)
        } else {
            // strategy == NONE
            Err(crate::consumer::Error::ConsumerNoOffsetForPartition(
                ConsumerNoOffsetForPartitionError::new(tp.clone()),
            ))
        }
    }
}

/// Rust-only: releases the rebalance listener. A listener that captured
/// [`Consumer::handle`] holds the shared `SubscriptionState` through the
/// handle, and that state stores the listener, so without this the cycle would
/// outlive the mock; Java's garbage collector frees the same graph. The
/// release waits for the drop rather than `close()`, because Java's
/// `rebalance` and `losePartitions` still run the listener on a closed mock
/// (`org.apache.kafka.clients.consumer.MockConsumer#rebalance` and `org.apache.kafka.clients.consumer.MockConsumer#losePartitions` make no `ensureNotClosed()` check).
impl<K, V> Drop for MockConsumer<K, V> {
    fn drop(&mut self) {
        self.subs().register_rebalance_listener(None);
    }
}

// ─── Consumer<K, V> trait impl ──────────────────────────────────────────

impl<K, V> Consumer<K, V> for MockConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    // ── Sync accessors ─────────────────────────────────────────────────

    // Java's `assignment()`, `subscription()`, `paused()` and
    // `groupMetadata()` do not call `ensureNotClosed()`, so these answer on a
    // closed mock too.

    fn assignment(&mut self) -> HashSet<TopicPartition> {
        self.subs().assigned_partitions()
    }

    fn subscription(&mut self) -> HashSet<String> {
        self.subs().subscription()
    }

    fn paused(&mut self) -> HashSet<TopicPartition> {
        // Java's `MockConsumer#paused` returns a copy of `paused`.
        self.paused.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn group_metadata(&mut self) -> Arc<dyn ConsumerGroupMetadata> {
        mock_group_metadata()
    }

    /// Returns an empty map: the mock keeps no metrics.
    ///
    /// # Panics
    ///
    /// Panics with `"This consumer has already been closed."` on a closed
    /// mock, where Java's `ensureNotClosed()` throws `IllegalStateException`:
    /// calling it after `close()` is a programming error.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        if let Err(error) = self.ensure_not_closed() {
            panic!("{}", error.message());
        }
        HashMap::new()
    }

    /// Returns the end offset set by [`MockConsumer::update_end_offsets`] minus
    /// the position, or `0` when no end offset was set.
    ///
    /// # Arguments
    ///
    /// - `topic_partition` - The partition to get the lag for.
    ///
    /// # Errors
    ///
    /// - The error of [`Consumer::position`] (a closed mock, an unassigned partition).
    // `org.apache.kafka.clients.consumer.MockConsumer#currentLag`:
    // `endOffsets.get(tp) - position(tp)`, `position(tp)` seeding a missing
    // position as Java's `updateFetchPosition` does.
    async fn current_lag(&mut self, topic_partition: &TopicPartition) -> Result<Option<i64>, Error> {
        let end = self.end_offsets_lock().get(topic_partition).copied();
        match end {
            Some(end) => Ok(Some(end - Consumer::position(self, topic_partition).await?)),
            // If the test doesn't bother to set an end offset, we assume it
            // wants to model being caught up.
            None => Ok(Some(0)),
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
        // Java `MockConsumer#assign`.
        self.ensure_not_closed()?;
        self.lock_committed().clear();
        self.subs().assign_from_user(partitions.into_iter().collect())?;
        Ok(())
    }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        // Java `MockConsumer#unsubscribe`.
        self.ensure_not_closed()?;
        self.lock_committed().clear();
        self.subs().unsubscribe();
        Ok(())
    }

    // ── Poll ────────────────────────────────────────────────────────────

    // `org.apache.kafka.clients.consumer.MockConsumer#poll(Duration)`.
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error> {
        // Step 1: ensureNotClosed.
        self.ensure_not_closed()?;

        // Step 2: record the timeout.
        *self.last_poll_timeout.lock().unwrap_or_else(PoisonError::into_inner) = Some(timeout);

        // Step 3: drain one queued poll task. Pop with
        // pop_front then invoke with &mut self — the task may schedule more
        // tasks, but only one is consumed per poll call.
        if let Some(task) = self.poll_tasks.pop_front() {
            task(self);
        }

        // Step 4: check wakeup flag and clear it.
        if self
            .wakeup
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(Error::wakeup("Mock consumer was woken up"));
        }

        // Step 5: take poll exception.
        if let Some(error) = self.poll_error.take() {
            return Err(error);
        }

        // Step 6: update fetch positions for newly-assigned partitions that
        // do not yet have a valid position.
        let assigned: Vec<TopicPartition> = self.subs().assigned_partitions().into_iter().collect();
        for tp in &assigned {
            if !self.subs().has_valid_position(tp) {
                self.update_fetch_position(tp)?;
            }
        }

        // Step 7: drain records up to max_poll_records.
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
            let skip = {
                let subs = self.subs();
                subs.is_paused(&tp) || !subs.is_assigned(&tp)
            };
            if skip {
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
                let position_opt = self.subs().position_or_null(&tp).map(|p| p.offset);
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

                // OffsetOutOfRange check: if a beginning
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

                    // Update the partition's fetch position, as Java's
                    // `MockConsumer#poll` does. Java uses `subscriptions.position(entry, fp)`
                    // (not `seek`), which requires a valid current position.
                    let leader_and_epoch = LeaderAndEpoch::new(None, leader_epoch);
                    let new_position = FetchPosition::with_leader(next_offset, leader_epoch, leader_and_epoch);
                    self.subs().set_position(&tp, new_position)?;

                    // Build the next-offsets entry.
                    let oam = OffsetAndMetadata::with_leader_epoch_metadata(next_offset, leader_epoch, String::new())?;
                    next_offset_and_metadata.insert(tp.clone(), oam);

                    num_poll_records += 1;
                } else {
                    // Java's loop simply does NOT remove the record (no
                    // `recIterator.remove()` call). Keep it for the next poll.
                    kept.push(rec);
                }
            }

            // Drop empty entries from the records map, as Java does.
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
        // Java's `commitSync()` (`org.apache.kafka.clients.consumer.MockConsumer#commitSync`) reads
        // `allConsumed()` BEFORE the closed check —
        // `commitSync(allConsumed())` → `commitSync(Map)` →
        // `commitAsync(offsets, null)`, where
        // `ensureNotClosed()` is finally called. We tighten by checking
        // closed first: end behavior is identical (both error when the
        // consumer is closed) but Rust skips the wasted subscription-map
        // traversal.
        self.ensure_not_closed()?;
        let offsets = self.subs().all_consumed();
        self.commit_async_impl(offsets, None).await
    }

    async fn commit_sync_with_timeout(&mut self, _timeout: Duration) -> Result<(), Error> {
        // Java's `commitSync(Duration)` ignores the timeout.
        self.commit_sync().await
    }

    async fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error> {
        // Java's `commitSync(Map)` delegates to `commitAsync`.
        self.commit_async_impl(offsets, None).await
    }

    async fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _timeout: Duration,
    ) -> Result<(), Error> {
        // Java's `commitSync(Map, Duration)` ignores the timeout.
        self.commit_sync_with_offsets(offsets).await
    }

    async fn commit_async(&mut self) -> Result<(), Error> {
        // Java's `commitAsync()` → `commitAsync(OffsetCommitCallback)`, which `ensureNotClosed()`s BEFORE
        // reading `allConsumed()`. Mirror that ordering so a closed consumer
        // errors out without traversing the subscription map.
        self.ensure_not_closed()?;
        let offsets = self.subs().all_consumed();
        self.commit_async_impl(offsets, None).await
    }

    async fn commit_async_with_callback(&mut self, callback: Arc<dyn OffsetCommitCallback>) -> Result<(), Error> {
        // Java's `commitAsync(OffsetCommitCallback)` calls `ensureNotClosed()` BEFORE `allConsumed()`.
        // Rust relies on `commit_async_impl`'s check at the cost of a wasted
        // `all_consumed()` traversal when the consumer is already closed.
        // Keeping the check only in `commit_async_impl` removes the redundant
        // double-check the previous code had. (Unlike `commit_sync` /
        // `commit_async`, which deliberately pre-check to skip that traversal,
        // this path is left as-is because all `*_offsets` / callback variants
        // funnel into `commit_async_impl` for the single closed check.)
        let offsets = self.subs().all_consumed();
        self.commit_async_impl(offsets, Some(callback)).await
    }

    async fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), Error> {
        // Java `MockConsumer#commitAsync(Map, OffsetCommitCallback)`.
        self.commit_async_impl(offsets, Some(callback)).await
    }

    // ── Seek ───────────────────────────────────────────────────────────

    async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error> {
        // Java `MockConsumer#seek(TopicPartition, long)`.
        self.ensure_not_closed()?;
        self.subs().seek(&partition, offset)?;
        Ok(())
    }

    async fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error> {
        // Java `MockConsumer#seek(TopicPartition, OffsetAndMetadata)`.
        self.ensure_not_closed()?;
        self.subs().seek(&partition, offset_and_metadata.offset())?;
        Ok(())
    }

    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java `MockConsumer#seekToBeginning`.
        self.ensure_not_closed()?;
        self.subs()
            .request_offset_reset_all(partitions, AutoOffsetResetStrategy::EARLIEST)?;
        Ok(())
    }

    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java `MockConsumer#seekToEnd`.
        self.ensure_not_closed()?;
        self.subs()
            .request_offset_reset_all(partitions, AutoOffsetResetStrategy::LATEST)?;
        Ok(())
    }

    // ── Position / committed ───────────────────────────────────────────

    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error> {
        // Java `MockConsumer#position(TopicPartition)`.
        self.ensure_not_closed()?;
        if !self.subs().is_assigned(partition) {
            return Err(Error::local_illegal_argument(POSITION_NOT_ASSIGNED_MSG));
        }
        // First read; if absent, refresh via update_fetch_position and re-read.
        let pos = self.subs().position_or_null(partition).map(|p| p.offset);
        if let Some(off) = pos {
            return Ok(off);
        }
        self.update_fetch_position(partition)?;
        let pos = self.subs().position_or_null(partition).map(|p| p.offset).ok_or_else(|| {
            Error::local_illegal_state(format!(
                "Position for partition {partition} is still unset after update_fetch_position",
            ))
        })?;
        Ok(pos)
    }

    async fn position_with_timeout(&mut self, partition: &TopicPartition, _timeout: Duration) -> Result<i64, Error> {
        // Java's `position(TopicPartition, Duration)` ignores the timeout.
        self.position(partition).await
    }

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        // Java `MockConsumer#committed(Set)`.
        self.ensure_not_closed()?;
        let mut result = HashMap::new();
        let committed = self.lock_committed();
        let subs = self.subs();
        for tp in partitions {
            if let Some(om) = committed.get(tp) {
                // Java: `subscriptions.isAssigned(tp) ? committed.get(tp) : new OffsetAndMetadata(0)`.
                let value = if subs.is_assigned(tp) {
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
        // Java's `committed(Set, Duration)` ignores the timeout.
        self.committed(partitions).await
    }

    // ── Metadata ───────────────────────────────────────────────────────

    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
        // Java `MockConsumer#partitionsFor(String)`.
        self.ensure_not_closed()?;
        Ok(self.partitions.get(topic).cloned().unwrap_or_default())
    }

    async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        _timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, Error> {
        // Java `MockConsumer#partitionsFor(String, Duration)`.
        self.partitions_for(topic).await
    }

    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<PartitionInfo>>, Error> {
        // Java `MockConsumer#listTopics()` returns the internal map reference; Rust
        // returns a clone so the caller is not exposed to subsequent
        // mutations of the mock's state.
        self.ensure_not_closed()?;
        Ok(self.partitions.clone())
    }

    async fn list_topics_with_timeout(
        &mut self,
        _timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, Error> {
        // Java `MockConsumer#listTopics(Duration)`.
        self.list_topics().await
    }

    async fn offsets_for_times(
        &mut self,
        _timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        // Java throws UnsupportedOperationException("Not implemented yet.")
        // (`org.apache.kafka.clients.consumer.MockConsumer#offsetsForTimes`), translated as Error::LocalUnsupportedOperation
        // (`admin-client.md` §9). DoD #3 makes the message text part of the
        // contract, so the text is Java's verbatim rather than a Rust-side
        // restatement.
        Err(Error::local_unsupported_operation("Not implemented yet."))
    }

    async fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error> {
        // Java's `offsetsForTimes(Map, Duration)` delegates to the no-timeout variant.
        self.offsets_for_times(timestamps_to_search).await
    }

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java `MockConsumer#beginningOffsets(Collection)`.
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
        // Java `MockConsumer#beginningOffsets(Collection, Duration)`.
        self.beginning_offsets(partitions).await
    }

    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error> {
        // Java `MockConsumer#endOffsets(Collection)`.
        if let Some(error) = self.offsets_error.take() {
            return Err(error);
        }
        let mut result = HashMap::new();
        let end_offsets = self.end_offsets_lock();
        for tp in partitions {
            let off = end_offsets.get(tp).copied().ok_or_else(|| {
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
        // Java `MockConsumer#endOffsets(Collection, Duration)`.
        self.end_offsets(partitions).await
    }

    // ── Pause / resume ─────────────────────────────────────────────────

    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java `MockConsumer#pause`.
        for tp in partitions {
            self.subs().pause(tp)?;
            self.paused.lock().unwrap_or_else(PoisonError::into_inner).insert(tp.clone());
        }
        Ok(())
    }

    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java `MockConsumer#resume`.
        for tp in partitions {
            self.subs().resume(tp)?;
            self.paused.lock().unwrap_or_else(PoisonError::into_inner).remove(tp);
        }
        Ok(())
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    async fn enforce_rebalance(&mut self) -> Result<(), Error> {
        // Java's `enforceRebalance()` forwards to
        // `enforceRebalance(null)`; Rust's suffixed form takes a non-null
        // `&str`, so the flag is set directly here instead.
        self.should_rebalance.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn enforce_rebalance_with_reason(&mut self, _reason: &str) -> Result<(), Error> {
        // Java's `enforceRebalance(String)` sets the flag; the reason is ignored.
        self.should_rebalance.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Java's `close()` delegates to
        // `close(CloseOptions.timeout(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS)))`.
        self.close_with_options(CloseOptions::new_with_timeout(Duration::from_millis(
            crate::consumer::internals::ConsumerUtils::DEFAULT_CLOSE_TIMEOUT_MS as u64,
        )))
        .await
    }

    async fn close_with_options(&mut self, _options: CloseOptions) -> Result<(), Error> {
        // Java's `close(CloseOptions)` ignores the options.
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn wakeup(&self) {
        // Java's `wakeup()` sets the flag.
        self.wakeup.store(true, Ordering::SeqCst);
    }

    fn handle(&self) -> ConsumerHandle {
        ConsumerHandle::for_mock(
            Arc::clone(&self.wakeup),
            Arc::clone(&self.closed),
            Arc::clone(&self.should_rebalance),
            Arc::clone(&self.last_poll_timeout),
            Arc::clone(&self.committed),
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.paused),
            Arc::clone(&self.end_offsets),
        )
    }
}

impl<K, V> MockConsumer<K, V> {
    /// Shared implementation for all `commit_*` variants; see
    /// [`mock_commit_async`].
    async fn commit_async_impl(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Option<Arc<dyn OffsetCommitCallback>>,
    ) -> Result<(), Error> {
        mock_commit_async(&self.closed, &self.committed, offsets, callback).await
    }
}

/// The message of the `IllegalArgumentException` Java's
/// `MockConsumer.position(TopicPartition)` throws for an unassigned partition.
const POSITION_NOT_ASSIGNED_MSG: &str = "You can only check the position for partitions assigned to this consumer.";

/// Java's `ensureNotClosed()` (`org.apache.kafka.clients.consumer.MockConsumer#ensureNotClosed`) over the mock's
/// shared `closed` flag, so a [`ConsumerHandle`] obtained from the mock
/// checks it too.
fn ensure_mock_not_closed(closed: &AtomicBool) -> Result<(), Error> {
    if closed.load(Ordering::SeqCst) {
        Err(Error::local_illegal_state("This consumer has already been closed."))
    } else {
        Ok(())
    }
}

/// Java's `groupMetadata()` (`org.apache.kafka.clients.consumer.MockConsumer#groupMetadata`): hard-coded
/// sentinel values, returned by the mock and by every [`ConsumerHandle`]
/// obtained from it.
pub(crate) fn mock_group_metadata() -> Arc<dyn ConsumerGroupMetadata> {
    Arc::new(ConsumerGroupMetadataImpl::with_generation_id_member_id_group_instance_id(
        "dummy.group.id",
        1,
        "1",
        None,
    ))
}

/// Java's `currentLag(TopicPartition)` (`org.apache.kafka.clients.consumer.MockConsumer#currentLag`) over
/// the mock's shared state, for a [`ConsumerHandle`] obtained from the mock:
/// `endOffsets.get(tp) - position(tp)` when an end offset is set, else `0`
/// ("caught up"). Java's `position(tp)` checks that the mock is open and that
/// the partition is assigned, with its messages; a partition without a
/// position answers `None`, since the handle cannot seed one as
/// `updateFetchPosition` does.
pub(crate) fn mock_current_lag(
    closed: &AtomicBool,
    subscriptions: &Mutex<SubscriptionState>,
    end_offsets: &Mutex<HashMap<TopicPartition, i64>>,
    topic_partition: &TopicPartition,
) -> Result<Option<i64>, Error> {
    let end = end_offsets
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(topic_partition)
        .copied();
    let Some(end) = end else {
        // If the test doesn't bother to set an end offset, we assume it wants
        // to model being caught up.
        return Ok(Some(0));
    };
    ensure_mock_not_closed(closed)?;
    let subs = subscriptions.lock().unwrap_or_else(PoisonError::into_inner);
    if !subs.is_assigned(topic_partition) {
        return Err(Error::local_illegal_argument(POSITION_NOT_ASSIGNED_MSG));
    }
    Ok(subs.position_or_null(topic_partition).map(|position| end - position.offset))
}

/// Java's `commitAsync(Map, OffsetCommitCallback)` (`org.apache.kafka.clients.consumer.MockConsumer#commitAsync`)
/// over the mock's shared state: the commit path of the mock and of every
/// [`ConsumerHandle`] obtained from it.
///
/// Java's order: `committed.putAll(offsets)`, then
/// `callback.onComplete(offsets, null)` inline, so the callback (which may
/// commit again through a handle) observes the merged offsets and its own
/// commits land after them. The lock is released before the callback runs.
pub(crate) async fn mock_commit_async(
    closed: &AtomicBool,
    committed: &Mutex<HashMap<TopicPartition, OffsetAndMetadata>>,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    callback: Option<Arc<dyn OffsetCommitCallback>>,
) -> Result<(), Error> {
    ensure_mock_not_closed(closed)?;
    let lock = || committed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    match callback {
        None => lock().extend(offsets),
        Some(cb) => {
            lock().extend(offsets.iter().map(|(tp, om)| (tp.clone(), om.clone())));
            // Java calls `onComplete` with no try/catch: an exception it
            // throws propagates out of `commitAsync`.
            cb.on_complete(&offsets, None).await?;
        },
    }
    Ok(())
}

/// The commits of a [`MockConsumer`] handle that name no offsets: Java's
/// `MockConsumer.commitAsync(callback)` (`ensureNotClosed()`, then
/// `commitAsync(subscriptions.allConsumed(), callback)`), which its
/// `commitSync()` and `commitAsync()` reach too. The `SubscriptionState`
/// guard is dropped before the commit awaits the callback
/// (`consumer-threading.md` §16).
pub(crate) async fn mock_commit_all_consumed(
    closed: &AtomicBool,
    committed: &Mutex<HashMap<TopicPartition, OffsetAndMetadata>>,
    subscriptions: &Mutex<SubscriptionState>,
    callback: Option<Arc<dyn OffsetCommitCallback>>,
) -> Result<(), Error> {
    ensure_mock_not_closed(closed)?;
    let offsets = subscriptions.lock().unwrap_or_else(PoisonError::into_inner).all_consumed();
    mock_commit_async(closed, committed, offsets, callback).await
}

#[cfg(test)]
mod tests {
    //! Inherent-impl unit tests. The full [`MockConsumerTest`] translation
    //! lives in `tests/consumer/mock_consumer_test.rs` and exercises the
    //! public surface (incl. the `Consumer<K, V>` trait impl).

    use async_trait::async_trait;

    use super::*;

    /// `MockConsumer` has the same thread shape as `KafkaConsumer`: it can
    /// move to another task but not be shared (`consumer-threading.md` §2),
    /// through its `not_sync` marker.
    #[test]
    fn mock_consumer_is_send_and_not_sync() {
        use crate::test_thread_safety::{is_send, is_sync};
        assert!(is_send!(MockConsumer<String, String>));
        assert!(!is_sync!(MockConsumer<String, String>));
    }

    /// Java's `MockConsumer.metrics()` calls `ensureNotClosed()`
    /// (`org.apache.kafka.clients.consumer.MockConsumer#metrics`), which throws `IllegalStateException`.
    #[tokio::test]
    #[should_panic(expected = "This consumer has already been closed.")]
    async fn test_metrics_panics_after_close() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        Consumer::close(&mut c).await.expect("close");
        let _ = Consumer::metrics(&c);
    }

    #[test]
    fn test_new_initial_state() {
        let c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        assert!(!c.closed());
        assert!(!c.should_rebalance());
        assert!(c.last_poll_timeout().is_none());
        assert_eq!(c.max_poll_records, i64::MAX);
    }

    #[test]
    fn test_set_max_poll_records_rejects_zero_and_negative() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        assert!(c.set_max_poll_records(0).is_err());
        assert!(c.set_max_poll_records(-1).is_err());
        assert!(c.set_max_poll_records(1).is_ok());
        assert_eq!(c.max_poll_records, 1);
    }

    /// Java `MockConsumer.metrics()` returns `Collections.emptyMap()`
    /// (`org.apache.kafka.clients.consumer.MockConsumer#metrics`). The Rust port returns an empty map.
    #[test]
    fn test_metrics_returns_empty_map() {
        let c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        assert!(Consumer::metrics(&c).is_empty());
    }

    #[test]
    fn test_reset_should_rebalance() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        c.should_rebalance.store(true, Ordering::SeqCst);
        assert!(c.should_rebalance());
        c.reset_should_rebalance();
        assert!(!c.should_rebalance());
    }

    #[test]
    fn test_update_partitions_after_close_errors() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        c.closed.store(true, Ordering::SeqCst);
        let err = c.update_partitions("t", Vec::new()).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalState(_)));
    }

    /// Java's `scheduleNopPollTask()` queues `() -> { }` like any other poll
    /// task (`org.apache.kafka.clients.consumer.MockConsumer#scheduleNopPollTask`): it consumes one `poll`, so the
    /// task scheduled after it runs on the next one.
    #[tokio::test]
    async fn schedule_nop_poll_task_takes_one_poll() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        c.schedule_nop_poll_task();
        c.schedule_poll_task(Box::new(|c| c.reset_should_rebalance()));
        c.should_rebalance.store(true, Ordering::SeqCst);

        c.poll(Duration::from_millis(0)).await.expect("poll");
        assert!(c.should_rebalance(), "the first poll ran the no-op task");
        c.poll(Duration::from_millis(0)).await.expect("poll");
        assert!(!c.should_rebalance(), "the second poll ran the task scheduled after it");
        assert!(c.poll_tasks.is_empty());
    }

    /// Java's `setPollException(null)` clears a pending poll exception
    /// (`org.apache.kafka.clients.consumer.MockConsumer#setPollException`): the next `poll` then succeeds.
    #[tokio::test]
    async fn set_poll_error_none_clears_the_pending_error() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = TopicPartition::new("t", 0);
        c.assign(vec![tp.clone()]).await.expect("assign");
        c.update_beginning_offsets(HashMap::from([(tp, 0)]));

        c.set_poll_error(Some(Error::local_illegal_state("boom")));
        c.set_poll_error(None);
        assert!(
            c.poll(Duration::from_millis(0)).await.is_ok(),
            "the cleared error must not fire"
        );

        c.set_poll_error(Some(Error::local_illegal_state("boom")));
        let err = c.poll(Duration::from_millis(0)).await.unwrap_err();
        assert_eq!(err.message(), "boom");
        assert!(c.poll(Duration::from_millis(0)).await.is_ok(), "the error fires once");
    }

    /// Java's `setOffsetsException(null)` clears a pending offsets exception
    /// (`org.apache.kafka.clients.consumer.MockConsumer#setOffsetsException`).
    #[tokio::test]
    async fn set_offsets_error_none_clears_the_pending_error() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = TopicPartition::new("t", 0);
        c.update_end_offsets(HashMap::from([(tp.clone(), 5)]));

        c.set_offsets_error(Some(Error::local_illegal_state("boom")));
        c.set_offsets_error(None);
        let offsets = c
            .end_offsets(std::slice::from_ref(&tp))
            .await
            .expect("the cleared error must not fire");
        assert_eq!(offsets.get(&tp), Some(&5));

        c.set_offsets_error(Some(Error::local_illegal_state("boom")));
        let err = c.end_offsets(std::slice::from_ref(&tp)).await.unwrap_err();
        assert_eq!(err.message(), "boom");
    }

    /// A [`ConsumerHandle`] obtained from the mock fires the SAME wakeup
    /// flag as `wakeup()`: the next `poll` observes it and returns `Wakeup`.
    #[tokio::test]
    async fn handle_wakeup_wakes_next_poll() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);

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
    /// (`org.apache.kafka.clients.consumer.MockConsumer#offsetsForTimes`), translated as
    /// [`Error::LocalUnsupportedOperation`]. The message text is part of the
    /// contract (DoD #3), so it must be Java's, not a Rust-side restatement.
    #[tokio::test]
    async fn offsets_for_times_reports_javas_not_implemented_message() {
        let mut consumer: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let err = consumer
            .offsets_for_times(HashMap::new())
            .await
            .expect_err("Java's mock does not implement this");
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");
        assert_eq!("Not implemented yet.", err.message());
    }

    // ─── ConsumerHandle commits (Java callbacks calling back into the mock) ───

    fn offsets(tp: &TopicPartition, offset: i64) -> HashMap<TopicPartition, OffsetAndMetadata> {
        HashMap::from([(tp.clone(), OffsetAndMetadata::new(offset).expect("non-negative"))])
    }

    /// Java's `MockConsumer.commitSync(Map)` records the offsets in the
    /// mock's `committed` map; through a handle it records them in the same
    /// map, so the consumer's `committed()` returns them.
    #[tokio::test]
    async fn handle_commit_sync_with_offsets_records_committed() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = tp_of("test");
        c.assign(vec![tp.clone()]).await.expect("assign");

        let handle = c.handle();
        handle.commit_sync_with_offsets(offsets(&tp, 5)).await.expect("commit sync");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 5)
        );

        let seen = Arc::new(Mutex::new(Vec::new()));
        let cb = Arc::new(RecordingCallback { label: "async", seen: Arc::clone(&seen), nested: None });
        handle
            .commit_async_with_offsets_callback(offsets(&tp, 6), cb)
            .await
            .expect("commit async");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 6)
        );
        assert_eq!(*seen.lock().unwrap(), vec![("async", offsets(&tp, 6))]);
    }

    fn tp_of(topic: &str) -> TopicPartition {
        TopicPartition::new(topic.to_string(), 0)
    }

    /// A callback's label and the offsets it was handed.
    type Seen = (&'static str, HashMap<TopicPartition, OffsetAndMetadata>);

    /// Records `(label, offsets)` and, with `nested`, commits again through
    /// the handle from inside the callback.
    struct RecordingCallback {
        label: &'static str,
        seen: Arc<Mutex<Vec<Seen>>>,
        nested: Option<(ConsumerHandle, HashMap<TopicPartition, OffsetAndMetadata>)>,
    }

    #[async_trait]
    impl OffsetCommitCallback for RecordingCallback {
        async fn on_complete(
            &self,
            offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
            error: Option<&Error>,
        ) -> Result<(), Error> {
            assert!(error.is_none(), "the mock completes commits without an error");
            self.seen.lock().unwrap().push((self.label, offsets.clone()));
            if let Some((handle, nested)) = &self.nested {
                let inner = Arc::new(RecordingCallback { label: "inner", seen: Arc::clone(&self.seen), nested: None });
                handle
                    .commit_async_with_offsets_callback(nested.clone(), inner)
                    .await
                    .expect("nested commit");
                self.seen.lock().unwrap().push(("outer-end", HashMap::new()));
            }
            Ok(())
        }
    }

    /// Java's `MockConsumer.commitAsync(Map, OffsetCommitCallback)` puts the
    /// offsets, then calls the callback inline. A callback committing through
    /// the handle runs its own callback inline too, before the outer one
    /// returns, and its offsets land after the outer ones.
    #[tokio::test]
    async fn handle_commit_async_with_offsets_callback_nests_inline() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = tp_of("test");
        c.assign(vec![tp.clone()]).await.expect("assign");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let outer = Arc::new(RecordingCallback {
            label: "outer",
            seen: Arc::clone(&seen),
            nested: Some((c.handle(), offsets(&tp, 9))),
        });

        c.commit_async_with_offsets_callback(offsets(&tp, 3), outer)
            .await
            .expect("commit");

        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("outer", offsets(&tp, 3)),
                ("inner", offsets(&tp, 9)),
                ("outer-end", HashMap::new())
            ]
        );
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 9)
        );
    }

    /// Assigns `tp`, seeds its beginning offset and polls `count` records
    /// from offset `from`, so the mock's consumed position is `from + count`.
    async fn consume(c: &mut MockConsumer<String, String>, tp: &TopicPartition, from: i64, count: i64) {
        for offset in from..from + count {
            c.add_record(ConsumerRecord::new(tp.topic().to_string(), tp.partition(), offset, None, None))
                .expect("add record");
        }
        assert_eq!(c.poll(Duration::ZERO).await.expect("poll").count() as i64, count);
    }

    /// Java's `MockConsumer.commitSync()`, `commitAsync()` and
    /// `commitAsync(callback)` commit `subscriptions.allConsumed()`; through a
    /// handle they commit the same mock's consumed positions, the callback
    /// running inline with them.
    #[tokio::test]
    async fn handle_commit_without_offsets_commits_consumed_positions() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = tp_of("test");
        c.assign(vec![tp.clone()]).await.expect("assign");
        c.update_beginning_offsets(HashMap::from([(tp.clone(), 0)]));
        let handle = c.handle();

        consume(&mut c, &tp, 0, 1).await;
        handle.commit_sync().await.expect("commit sync");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 1)
        );

        consume(&mut c, &tp, 1, 2).await;
        handle.commit_async().await.expect("commit async");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 3)
        );

        consume(&mut c, &tp, 3, 1).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        handle
            .commit_async_with_callback(Arc::new(RecordingCallback {
                label: "cb",
                seen: Arc::clone(&seen),
                nested: None,
            }))
            .await
            .expect("commit async with callback");
        assert_eq!(*seen.lock().unwrap(), vec![("cb", offsets(&tp, 4))]);
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 4)
        );
    }

    /// The handle's getters read the mock's own state, as a Java callback
    /// calling `consumer.assignment()` / `paused()` / `currentLag()` /
    /// `groupMetadata()` does.
    #[tokio::test]
    async fn handle_getters_read_the_mock_state() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let handle = c.handle();
        c.subscribe_with_topics(vec!["test".to_string()]).await.expect("subscribe");
        let tp = tp_of("test");
        // No end offset: caught up, assigned or not, as Java's mock.
        assert_eq!(handle.current_lag(&tp).await.expect("lag"), Some(0));
        c.update_end_offsets(HashMap::from([(tp.clone(), 10)]));
        // Unassigned with an end offset: Java's `position(tp)` throws.
        let error = handle.current_lag(&tp).await.expect_err("unassigned");
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(
            error.message(),
            "You can only check the position for partitions assigned to this consumer."
        );
        c.rebalance(std::slice::from_ref(&tp)).await.expect("rebalance");
        assert_eq!(handle.assignment(), HashSet::from([tp.clone()]));
        c.pause(std::slice::from_ref(&tp)).await.expect("pause");
        assert_eq!(handle.paused(), HashSet::from([tp.clone()]));
        c.resume(std::slice::from_ref(&tp)).await.expect("resume");
        assert!(handle.paused().is_empty());

        // Assigned with an end offset but no position yet: Java's mock would
        // reset the position through `position(tp)`; the handle cannot seed
        // one, so the lag is unknown.
        assert_eq!(handle.current_lag(&tp).await.expect("lag"), None);
        c.seek_with_offset(tp.clone(), 4).await.expect("seek");
        assert_eq!(
            handle.current_lag(&tp).await.expect("lag"),
            Some(6),
            "handle lag reads the mock's end offsets and position"
        );
        assert_eq!(
            handle.current_lag(&tp).await.expect("lag"),
            c.current_lag(&tp).await.expect("lag")
        );

        // Java's `MockConsumer.groupMetadata()` sentinel values.
        let meta = handle.group_metadata();
        assert_eq!(meta.group_id(), "dummy.group.id");
        assert_eq!(meta.generation_id(), 1);
        assert_eq!(meta.member_id(), "1");
        assert_eq!(meta.group_instance_id(), None);
    }

    /// Commits from inside `onPartitionsRevoked` through the handle, the
    /// canonical listener pattern: the mock holds no `SubscriptionState` lock
    /// while the listener runs, so the commit reads the consumed positions
    /// and records them before the rebalance replaces the assignment.
    #[tokio::test]
    async fn handle_commit_sync_inside_rebalance_listener() {
        struct CommitOnRevoke(ConsumerHandle, Arc<Mutex<Vec<HashSet<TopicPartition>>>>);
        #[async_trait]
        impl ConsumerRebalanceListener for CommitOnRevoke {
            async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
                self.1.lock().unwrap().push(self.0.assignment());
                self.0.commit_sync().await
            }
            async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = tp_of("test");
        let seen = Arc::new(Mutex::new(Vec::new()));
        c.subscribe_with_topics_listener(
            vec!["test".to_string()],
            Arc::new(CommitOnRevoke(c.handle(), Arc::clone(&seen))),
        )
        .await
        .expect("subscribe");
        c.rebalance(std::slice::from_ref(&tp)).await.expect("assign");
        c.update_beginning_offsets(HashMap::from([(tp.clone(), 0)]));
        consume(&mut c, &tp, 0, 2).await;

        c.rebalance(&[]).await.expect("revoke");
        assert_eq!(*seen.lock().unwrap(), vec![HashSet::from([tp.clone()])]);
        // The commit recorded the consumed position (read directly: the
        // mock's `committed()` answers 0 for a partition no longer assigned).
        assert_eq!(*c.lock_committed(), offsets(&tp, 2));
    }

    /// A listener that captured the mock's handle is freed with the mock, and
    /// until the drop it still runs on a closed mock, as Java's `rebalance`
    /// makes no `ensureNotClosed()` check (`org.apache.kafka.clients.consumer.MockConsumer#rebalance`).
    #[tokio::test]
    async fn listener_capturing_the_handle_is_freed_with_the_mock() {
        struct Sentinel(Arc<AtomicBool>);
        impl Drop for Sentinel {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        struct HandleListener {
            _handle: ConsumerHandle,
            _sentinel: Sentinel,
            assigned: Arc<Mutex<Vec<TopicPartition>>>,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for HandleListener {
            async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
                self.assigned.lock().unwrap().extend_from_slice(partitions);
                Ok(())
            }
        }

        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let dropped = Arc::new(AtomicBool::new(false));
        let assigned = Arc::new(Mutex::new(Vec::new()));
        let subscriptions = Arc::downgrade(&c.subscriptions);
        c.subscribe_with_topics_listener(
            vec!["test".to_string()],
            Arc::new(HandleListener {
                _handle: c.handle(),
                _sentinel: Sentinel(Arc::clone(&dropped)),
                assigned: Arc::clone(&assigned),
            }),
        )
        .await
        .expect("subscribe");
        c.close().await.expect("close");

        let tp = tp_of("test");
        c.rebalance(std::slice::from_ref(&tp)).await.expect("rebalance");
        assert_eq!(*assigned.lock().unwrap(), vec![tp]);
        assert!(!dropped.load(Ordering::SeqCst), "the listener is released only by the drop");

        drop(c);
        assert!(dropped.load(Ordering::SeqCst), "listener still referenced");
        assert_eq!(0, subscriptions.strong_count(), "SubscriptionState still referenced");
    }

    /// An error the callback returns (Java's `onComplete` throwing) propagates
    /// out of the mock's commit, after the offsets were committed: Java's
    /// `MockConsumer.commitAsync` calls the callback with no try/catch. The
    /// handle's commits behave the same.
    #[tokio::test]
    async fn commit_callback_error_propagates() {
        struct Raising;
        #[async_trait]
        impl OffsetCommitCallback for Raising {
            async fn on_complete(
                &self,
                _offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
                _error: Option<&Error>,
            ) -> Result<(), Error> {
                Err(Error::local_illegal_state("callback failed"))
            }
        }

        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let tp = tp_of("test");
        c.assign(vec![tp.clone()]).await.expect("assign");
        c.update_beginning_offsets(HashMap::from([(tp.clone(), 0)]));
        consume(&mut c, &tp, 0, 3).await;

        let err = c
            .commit_async_with_offsets_callback(offsets(&tp, 2), Arc::new(Raising))
            .await
            .expect_err("propagates");
        assert!(err.is_local_illegal_state_error(), "unexpected error: {err:?}");
        assert_eq!(err.message(), "callback failed");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 2)
        );

        let err = c.commit_async_with_callback(Arc::new(Raising)).await.expect_err("propagates");
        assert_eq!(err.message(), "callback failed");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 3)
        );

        let handle = c.handle();
        let err = handle
            .commit_async_with_offsets_callback(offsets(&tp, 5), Arc::new(Raising))
            .await
            .expect_err("propagates");
        assert_eq!(err.message(), "callback failed");
        let err = handle
            .commit_async_with_callback(Arc::new(Raising))
            .await
            .expect_err("propagates");
        assert_eq!(err.message(), "callback failed");
        assert_eq!(
            c.committed(std::slice::from_ref(&tp)).await.expect("committed"),
            offsets(&tp, 3)
        );
    }

    /// Java's `commitAsync` starts with `ensureNotClosed()`, so a handle
    /// commit after `close()` fails with the mock's closed error.
    #[tokio::test]
    async fn handle_commit_after_close_fails() {
        let mut c: MockConsumer<String, String> =
            MockConsumer::with_auto_offset_reset_strategy(AutoOffsetResetStrategy::EARLIEST);
        let handle = c.handle();
        c.close().await.expect("close");
        let tp = tp_of("test");
        let err = handle.commit_sync_with_offsets(offsets(&tp, 1)).await.expect_err("closed");
        assert_eq!(err.message(), "This consumer has already been closed.");
        let err = handle
            .commit_async_with_offsets_callback(
                offsets(&tp, 1),
                Arc::new(RecordingCallback { label: "x", seen: Arc::new(Mutex::new(Vec::new())), nested: None }),
            )
            .await
            .expect_err("closed");
        assert_eq!(err.message(), "This consumer has already been closed.");
        for err in [
            handle.commit_sync().await.expect_err("closed"),
            handle.commit_async().await.expect_err("closed"),
            handle
                .commit_async_with_callback(Arc::new(RecordingCallback {
                    label: "x",
                    seen: Arc::new(Mutex::new(Vec::new())),
                    nested: None,
                }))
                .await
                .expect_err("closed"),
        ] {
            assert!(err.is_local_illegal_state_error(), "unexpected error: {err:?}");
            assert_eq!(err.message(), "This consumer has already been closed.");
        }
    }
}
