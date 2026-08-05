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

//! Mock producer for testing code that uses Kafka.
//!
//! Corresponds to Java's `org.apache.kafka.clients.producer.MockProducer`.
//!
//! By default this mock will synchronously complete each send call successfully.
//! However it can be configured to allow the user to control the completion of
//! the call and supply an optional error for the producer to throw.
//!
//! # Transactional API
//!
//! The [`Producer`] trait's transactional methods are pure in-memory state:
//! there is no coordinator, no `TransactionManager` and no network, matching
//! Java's `MockProducer`. While a transaction is in flight, sends are staged in
//! `uncommitted_sends` (Java `MockProducer.java:60`) and offsets in
//! `uncommitted_consumer_group_offsets` (`:68`) rather than published;
//! [`commit_transaction`](Producer::commit_transaction) moves both into the
//! visible [`history()`](MockProducer::history) and
//! [`consumer_group_offsets_history()`](MockProducer::consumer_group_offsets_history),
//! while [`abort_transaction`](Producer::abort_transaction) discards them.
//!
//! Misuse returns `Err` where Java throws: `IllegalStateException` becomes
//! [`KafkaError::illegal_state`] and `ProducerFencedException` becomes a
//! [`KafkaError`] carrying [`Errors::ProducerFenced`], with Java's message text
//! preserved verbatim (CLAUDE.md §10.2).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::Producer;
use super::ProducerRecord;
use super::RecordMetadata;
use super::internals::FutureRecordMetadata;
use super::internals::ProduceRequestResult;
use crate::common::Cluster;
use crate::common::KafkaError;
use crate::common::KafkaFuture;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::protocol::Errors;
use crate::common::record::RecordBatch;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;

use super::Callback;

/// A mock of the producer interface for testing code that uses Kafka.
///
/// By default this mock will synchronously complete each send call successfully.
/// However it can be configured to allow the user to control the completion of
/// the call and supply an optional error for the producer to throw.
///
/// Corresponds to Java's `org.apache.kafka.clients.producer.MockProducer`.
///
/// # Thread Safety
///
/// All methods use interior mutability via `Mutex`, matching Java's
/// `synchronized` methods. The struct is `Send + Sync` so it can be shared
/// via `Arc`.
pub struct MockProducer<K, V> {
    inner: Mutex<MockProducerInner<K, V>>,
}

/// The offsets one transaction contributed, grouped by consumer group id.
///
/// Spells Java's `Map<String, Map<TopicPartition, OffsetAndMetadata>>`
/// (`MockProducer.java:63`, `:68`); an alias rather than a new type, so it adds
/// no struct absent from the Java source (`definition-of-done.md` §7).
type ConsumerGroupOffsets = HashMap<String, HashMap<TopicPartition, OffsetAndMetadata>>;

struct MockProducerInner<K, V> {
    cluster: Cluster,
    auto_complete: bool,
    /// Java `sent` (`MockProducer.java:59`).
    sent: Vec<ProducerRecord<K, V>>,
    /// Java `uncommittedSends` (`:60`) — records sent inside the in-flight
    /// transaction, published to `sent` only on commit.
    uncommitted_sends: Vec<ProducerRecord<K, V>>,
    completions: VecDeque<Completion>,
    offsets: HashMap<TopicPartition, i64>,
    /// Java `consumerGroupOffsets` (`:63`) — one entry per committed
    /// transaction that carried offsets.
    consumer_group_offsets: Vec<ConsumerGroupOffsets>,
    /// Java `uncommittedConsumerGroupOffsets` (`:68`).
    uncommitted_consumer_group_offsets: ConsumerGroupOffsets,
    closed: bool,
    /// Java `transactionInitialized` (`:70`).
    transaction_initialized: bool,
    /// Java `transactionInFlight` (`:71`).
    transaction_in_flight: bool,
    /// Java `transactionCommitted` (`:72`).
    transaction_committed: bool,
    /// Java `transactionAborted` (`:73`).
    transaction_aborted: bool,
    /// Java `producerFenced` (`:74`).
    producer_fenced: bool,
    /// Java `sentOffsets` (`:75`).
    sent_offsets: bool,
    /// Java `commitCount` (`:76`).
    commit_count: i64,
    /// Java `initTransactionException` (`:79`).
    init_transaction_error: Option<KafkaError>,
    /// Java `beginTransactionException` (`:80`).
    begin_transaction_error: Option<KafkaError>,
    /// Java `sendOffsetsToTransactionException` (`:81`).
    send_offsets_to_transaction_error: Option<KafkaError>,
    /// Java `commitTransactionException` (`:82`).
    commit_transaction_error: Option<KafkaError>,
    /// Java `abortTransactionException` (`:83`).
    abort_transaction_error: Option<KafkaError>,
    /// Java `sendException` (`:84`).
    send_error: Option<KafkaError>,
    /// Java `flushException` (`:85`).
    flush_error: Option<KafkaError>,
    /// Java `partitionsForException` (`:86`).
    partitions_for_error: Option<KafkaError>,
    /// Java `closeException` (`:87`).
    close_error: Option<KafkaError>,
}

impl<K, V> MockProducerInner<K, V> {
    /// Corresponds to Java's `verifyNotClosed()` (`MockProducer.java:248`).
    fn verify_not_closed(&self) -> Result<(), KafkaError> {
        if self.closed {
            return Err(KafkaError::illegal_state("MockProducer is already closed."));
        }
        Ok(())
    }

    /// Corresponds to Java's `verifyNotFenced()` (`MockProducer.java:254`),
    /// which throws `ProducerFencedException`.
    fn verify_not_fenced(&self) -> Result<(), KafkaError> {
        if self.producer_fenced {
            return Err(KafkaError::with_message(Errors::ProducerFenced, "MockProducer is fenced."));
        }
        Ok(())
    }

    /// Corresponds to Java's `verifyTransactionsInitialized()`
    /// (`MockProducer.java:260`).
    fn verify_transactions_initialized(&self) -> Result<(), KafkaError> {
        if !self.transaction_initialized {
            return Err(KafkaError::illegal_state(
                "MockProducer hasn't been initialized for transactions.",
            ));
        }
        Ok(())
    }

    /// Corresponds to Java's `verifyTransactionInFlight()`
    /// (`MockProducer.java:266`).
    fn verify_transaction_in_flight(&self) -> Result<(), KafkaError> {
        if !self.transaction_in_flight {
            return Err(KafkaError::illegal_state("There is no open transaction."));
        }
        Ok(())
    }

    /// Corresponds to Java's `flush()` (`MockProducer.java:347`).
    ///
    /// Java's `flush()` is `synchronized` and is called from within the equally
    /// `synchronized` `commitTransaction` / `abortTransaction`; a Java monitor is
    /// reentrant, `std::sync::Mutex` is not. So the body lives here, with the lock
    /// already held by the caller, and [`Producer::flush`] is the entry point that
    /// acquires it. Note Java's `flush()` deliberately does *not*
    /// `verifyNotFenced()` — see `shouldNotThrowOnFlushProducerIfProducerIsFenced`.
    fn flush(&mut self) -> Result<(), KafkaError> {
        self.verify_not_closed()?;

        if let Some(err) = self.flush_error.as_ref() {
            return Err(err.clone());
        }

        while self.complete_next() {}

        Ok(())
    }

    /// Corresponds to Java's `completeNext()` (`MockProducer.java:504`).
    fn complete_next(&mut self) -> bool {
        self.error_next(None)
    }

    /// Corresponds to Java's `errorNext(RuntimeException)`
    /// (`MockProducer.java:513`).
    fn error_next(&mut self, error: Option<KafkaError>) -> bool {
        match self.completions.pop_front() {
            Some(completion) => {
                completion.complete(error);
                true
            },
            None => false,
        }
    }
}

/// Internal completion record that holds the state needed to fulfill a
/// [`FutureRecordMetadata`].
///
/// Corresponds to Java's `MockProducer.Completion` inner class.
struct Completion {
    offset: i64,
    metadata: RecordMetadata,
    result: Arc<ProduceRequestResult>,
    callback: Option<Callback>,
    topic_partition: TopicPartition,
}

impl Completion {
    /// Complete this send with either a success or an error.
    ///
    /// Corresponds to Java's `Completion.complete(RuntimeException)`
    /// (`MockProducer.java:567-581`), whose three steps run in this order: set the
    /// result, fire the callback, then `done()`. The `done()` last is load-bearing
    /// in Java — it is the latch a `Future.get()` waits on, so a waiter cannot
    /// observe the send as complete before the callback has returned. It is not
    /// observable from the single task that calls `complete`, since both happen
    /// before the call returns, but it is from a concurrent one.
    fn complete(self, error: Option<KafkaError>) {
        let Completion { offset, metadata, result, callback, topic_partition } = self;
        if let Some(e) = error {
            let error_fn: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> = {
                let e = e.clone();
                Arc::new(move |_| Some(e.clone()))
            };
            result.set(-1, RecordBatch::NO_TIMESTAMP, Some(error_fn));
            // Java 578: the callback still receives metadata on the error path,
            // carrying -1 for every unknown field, exactly as `KafkaProducer`'s own
            // error path does (`kafka_producer.rs:1155`).
            if let Some(cb) = callback {
                let null_metadata = RecordMetadata::new(topic_partition, -1, -1, RecordBatch::NO_TIMESTAMP, -1, -1);
                cb(Some(&null_metadata), Some(&e));
            }
        } else {
            result.set(offset, RecordBatch::NO_TIMESTAMP, None);
            // Java 576: fire the callback with the record's metadata.
            if let Some(cb) = callback {
                cb(Some(&metadata), None);
            }
        }
        result.done();
    }
}

impl<K, V> MockProducer<K, V> {
    /// Create a mock producer.
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster holding metadata for this producer.
    /// * `auto_complete` - If `true`, automatically complete all requests
    ///   successfully. Otherwise the user must call [`complete_next()`](Self::complete_next)
    ///   or [`error_next()`](Self::error_next) after [`send()`](Producer::send)
    ///   to complete the call and resolve the [`FutureRecordMetadata`].
    ///
    /// Corresponds to Java's `MockProducer(Cluster, boolean, Partitioner,
    /// Serializer, Serializer)` constructor (without serializers or partitioner,
    /// since the Rust producer works with pre-serialized bytes).
    pub fn new(cluster: Cluster, auto_complete: bool) -> Self {
        Self {
            inner: Mutex::new(MockProducerInner {
                cluster,
                auto_complete,
                sent: Vec::new(),
                uncommitted_sends: Vec::new(),
                completions: VecDeque::new(),
                offsets: HashMap::new(),
                consumer_group_offsets: Vec::new(),
                uncommitted_consumer_group_offsets: HashMap::new(),
                closed: false,
                transaction_initialized: false,
                transaction_in_flight: false,
                transaction_committed: false,
                transaction_aborted: false,
                producer_fenced: false,
                sent_offsets: false,
                commit_count: 0,
                init_transaction_error: None,
                begin_transaction_error: None,
                send_offsets_to_transaction_error: None,
                commit_transaction_error: None,
                abort_transaction_error: None,
                send_error: None,
                flush_error: None,
                partitions_for_error: None,
                close_error: None,
            }),
        }
    }

    /// Create a new mock producer with an empty cluster and the given
    /// `auto_complete` setting.
    ///
    /// Equivalent to `MockProducer::new(Cluster::empty(), auto_complete)`.
    ///
    /// Corresponds to Java's `MockProducer(boolean, Partitioner, Serializer,
    /// Serializer)`.
    pub fn with_auto_complete(auto_complete: bool) -> Self {
        Self::new(Cluster::empty(), auto_complete)
    }

    /// Get the list of sent records since the last call to [`clear()`](Self::clear).
    ///
    /// Returns a clone of the internal sent list.
    ///
    /// Corresponds to Java's `MockProducer.history()`.
    pub fn history(&self) -> Vec<ProducerRecord<K, V>>
    where
        K: Clone,
        V: Clone,
    {
        let inner = self.inner.lock().unwrap();
        inner.sent.clone()
    }

    /// Get the list of records sent inside the in-flight transaction and not yet
    /// committed.
    ///
    /// Returns a clone of the internal uncommitted-sends list.
    ///
    /// Corresponds to Java's `MockProducer.uncommittedRecords()`
    /// (`MockProducer.java:471`).
    pub fn uncommitted_records(&self) -> Vec<ProducerRecord<K, V>>
    where
        K: Clone,
        V: Clone,
    {
        let inner = self.inner.lock().unwrap();
        inner.uncommitted_sends.clone()
    }

    /// Get the list of committed consumer group offsets since the last call to
    /// [`clear()`](Self::clear) — one entry per committed transaction that
    /// carried offsets.
    ///
    /// Corresponds to Java's `MockProducer.consumerGroupOffsetsHistory()`
    /// (`MockProducer.java:479`).
    pub fn consumer_group_offsets_history(&self) -> Vec<ConsumerGroupOffsets> {
        let inner = self.inner.lock().unwrap();
        inner.consumer_group_offsets.clone()
    }

    /// Get the offsets staged by the in-flight transaction and not yet committed.
    ///
    /// Corresponds to Java's `MockProducer.uncommittedOffsets()`
    /// (`MockProducer.java:483`). Java hands back the live map; behind the mutex
    /// that is not expressible, so this returns a snapshot clone. No Java caller
    /// mutates the returned map.
    pub fn uncommitted_offsets(&self) -> ConsumerGroupOffsets {
        let inner = self.inner.lock().unwrap();
        inner.uncommitted_consumer_group_offsets.clone()
    }

    /// Clear the stored history of sent records and consumer group offsets.
    ///
    /// Note: per-topic-partition offset counters are intentionally preserved
    /// across `clear()` calls, matching Java's `MockProducer.clear()` which
    /// does **not** reset the `offsets` map. Nor does it reset the transaction
    /// flags — only `sentOffsets`.
    ///
    /// Corresponds to Java's `MockProducer.clear()` (`MockProducer.java:490`).
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.sent.clear();
        inner.uncommitted_sends.clear();
        inner.sent_offsets = false;
        inner.completions.clear();
        inner.consumer_group_offsets.clear();
        inner.uncommitted_consumer_group_offsets.clear();
    }

    /// Complete the earliest uncompleted call successfully.
    ///
    /// Returns `true` if there was an uncompleted call to complete.
    ///
    /// Corresponds to Java's `MockProducer.completeNext()`.
    pub fn complete_next(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.complete_next()
    }

    /// Complete the earliest uncompleted call with the given error.
    ///
    /// Returns `true` if there was an uncompleted call to complete.
    ///
    /// Corresponds to Java's `MockProducer.errorNext(RuntimeException)`.
    pub fn error_next(&self, error: KafkaError) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.error_next(Some(error))
    }

    /// Mark this producer as fenced by another producer with the same
    /// `transactional.id`. Every subsequent transactional call and every
    /// [`send()`](Producer::send) then fails with
    /// [`Errors::ProducerFenced`].
    ///
    /// Corresponds to Java's `MockProducer.fenceProducer()`
    /// (`MockProducer.java:429`).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is closed, is already fenced, or was never
    /// initialized for transactions ([`KafkaError::illegal_state`] for the first
    /// and last, [`Errors::ProducerFenced`] for the second).
    pub fn fence_producer(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        inner.verify_transactions_initialized()?;
        inner.producer_fenced = true;
        Ok(())
    }

    /// Returns `true` if [`init_transactions()`](Producer::init_transactions) has
    /// completed successfully.
    ///
    /// Corresponds to Java's `MockProducer.transactionInitialized()`
    /// (`MockProducer.java:436`).
    pub fn transaction_initialized(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.transaction_initialized
    }

    /// Returns `true` if a transaction has been begun and neither committed nor
    /// aborted.
    ///
    /// Corresponds to Java's `MockProducer.transactionInFlight()`
    /// (`MockProducer.java:440`).
    pub fn transaction_in_flight(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.transaction_in_flight
    }

    /// Returns `true` if the most recent transaction was committed.
    ///
    /// Corresponds to Java's `MockProducer.transactionCommitted()`
    /// (`MockProducer.java:444`).
    pub fn transaction_committed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.transaction_committed
    }

    /// Returns `true` if the most recent transaction was aborted.
    ///
    /// Corresponds to Java's `MockProducer.transactionAborted()`
    /// (`MockProducer.java:448`).
    pub fn transaction_aborted(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.transaction_aborted
    }

    /// Returns `true` if offsets were sent to the current or most recent
    /// transaction. Reset only by
    /// [`begin_transaction()`](Producer::begin_transaction) and
    /// [`clear()`](Self::clear) — not by a commit.
    ///
    /// Corresponds to Java's `MockProducer.sentOffsets()`
    /// (`MockProducer.java:456`).
    pub fn sent_offsets(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.sent_offsets
    }

    /// The number of transactions committed so far. Aborted transactions are not
    /// counted.
    ///
    /// Corresponds to Java's `MockProducer.commitCount()`
    /// (`MockProducer.java:460`).
    pub fn commit_count(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.commit_count
    }

    /// Returns `true` if the producer is closed.
    ///
    /// Corresponds to Java's `MockProducer.closed()`.
    pub fn closed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.closed
    }

    /// Returns `true` if there are no pending completions.
    ///
    /// Corresponds to Java's `MockProducer.flushed()`.
    pub fn flushed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.completions.is_empty()
    }

    /// Set an error to be returned on every [`send()`](Producer::send) call
    /// until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.sendException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_send_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.send_error = error;
    }

    /// Set an error to be returned on every [`flush()`](Producer::flush) call
    /// until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.flushException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_flush_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.flush_error = error;
    }

    /// Set an error to be returned on every
    /// [`partitions_for()`](Producer::partitions_for) call until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.partitionsForException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_partitions_for_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.partitions_for_error = error;
    }

    /// Set an error to be returned on every [`close()`](Producer::close) call
    /// until cleared.
    ///
    /// The error persists across calls until explicitly cleared with `None`,
    /// matching Java's `MockProducer.closeException` field semantics.
    ///
    /// Pass `None` to clear a previously set error.
    pub fn set_close_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.close_error = error;
    }

    /// Set an error to be returned on every
    /// [`init_transactions()`](Producer::init_transactions) call until cleared.
    ///
    /// Matches Java's public `MockProducer.initTransactionException` field
    /// (`MockProducer.java:79`), which likewise persists until set back to `null`.
    pub fn set_init_transaction_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.init_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`begin_transaction()`](Producer::begin_transaction) call until cleared.
    ///
    /// Matches Java's public `MockProducer.beginTransactionException` field
    /// (`MockProducer.java:80`).
    pub fn set_begin_transaction_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.begin_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`send_offsets_to_transaction()`](Producer::send_offsets_to_transaction)
    /// call until cleared.
    ///
    /// Matches Java's public `MockProducer.sendOffsetsToTransactionException`
    /// field (`MockProducer.java:81`).
    pub fn set_send_offsets_to_transaction_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.send_offsets_to_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`commit_transaction()`](Producer::commit_transaction) call until cleared.
    ///
    /// Matches Java's public `MockProducer.commitTransactionException` field
    /// (`MockProducer.java:82`).
    pub fn set_commit_transaction_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.commit_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`abort_transaction()`](Producer::abort_transaction) call until cleared.
    ///
    /// Matches Java's public `MockProducer.abortTransactionException` field
    /// (`MockProducer.java:83`).
    pub fn set_abort_transaction_error(&self, error: Option<KafkaError>) {
        let mut inner = self.inner.lock().unwrap();
        inner.abort_transaction_error = error;
    }
}

impl<K, V> Default for MockProducer<K, V> {
    /// Create a new mock producer with an empty cluster and `auto_complete=false`.
    ///
    /// Corresponds to Java's no-arg `MockProducer()` constructor.
    fn default() -> Self {
        Self::new(Cluster::empty(), false)
    }
}

impl<K: Send + Sync, V: Send + Sync> Producer<K, V> for MockProducer<K, V> {
    /// Initialize this mock for transactions.
    ///
    /// Corresponds to Java's `MockProducer.initTransactions()`
    /// (`MockProducer.java:145`). Stays `async` because the trait declares it so
    /// (Java's `KafkaProducer.initTransactions` blocks); the mock never awaits.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is closed, is fenced, has already been
    /// initialized, or an error was installed with
    /// [`set_init_transaction_error`](MockProducer::set_init_transaction_error).
    async fn init_transactions(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        if inner.transaction_initialized {
            return Err(KafkaError::illegal_state(
                "MockProducer has already been initialized for transactions.",
            ));
        }
        if let Some(err) = inner.init_transaction_error.as_ref() {
            return Err(err.clone());
        }
        inner.transaction_initialized = true;
        inner.transaction_in_flight = false;
        inner.transaction_committed = false;
        inner.transaction_aborted = false;
        inner.sent_offsets = false;
        Ok(())
    }

    /// Begin a transaction.
    ///
    /// Corresponds to Java's `MockProducer.beginTransaction()`
    /// (`MockProducer.java:162`).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is closed, is fenced, was not initialized for
    /// transactions, a transaction is already in flight, or an error was installed
    /// with [`set_begin_transaction_error`](MockProducer::set_begin_transaction_error).
    fn begin_transaction(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        inner.verify_transactions_initialized()?;

        if let Some(err) = inner.begin_transaction_error.as_ref() {
            return Err(err.clone());
        }

        if inner.transaction_in_flight {
            return Err(KafkaError::illegal_state("Transaction already started"));
        }

        inner.transaction_in_flight = true;
        inner.transaction_committed = false;
        inner.transaction_aborted = false;
        inner.sent_offsets = false;
        Ok(())
    }

    /// Stage consumer group offsets as part of the in-flight transaction.
    ///
    /// Corresponds to Java's `MockProducer.sendOffsetsToTransaction(Map,
    /// ConsumerGroupMetadata)` (`MockProducer.java:182`). Java's
    /// `Objects.requireNonNull(groupMetadata)` (`:184`) has no counterpart: the
    /// parameter is taken by value and is not an `Option`, so a missing metadata is
    /// not expressible.
    ///
    /// An empty `offsets` map is ignored and leaves
    /// [`sent_offsets()`](MockProducer::sent_offsets) `false` (Java `:194-196`).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is closed, is fenced, was not initialized for
    /// transactions, has no open transaction, or an error was installed with
    /// [`set_send_offsets_to_transaction_error`](MockProducer::set_send_offsets_to_transaction_error).
    async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        inner.verify_transactions_initialized()?;
        inner.verify_transaction_in_flight()?;

        if let Some(err) = inner.send_offsets_to_transaction_error.as_ref() {
            return Err(err.clone());
        }

        if offsets.is_empty() {
            return Ok(());
        }

        // Java 197-199: `computeIfAbsent` then `putAll`, so a second call for the
        // same group merges into the first, later offsets winning per partition.
        let uncommitted_offsets = inner
            .uncommitted_consumer_group_offsets
            .entry(group_metadata.group_id().to_string())
            .or_default();
        uncommitted_offsets.extend(offsets);
        inner.sent_offsets = true;
        Ok(())
    }

    /// Commit the in-flight transaction, publishing its records and offsets.
    ///
    /// Corresponds to Java's `MockProducer.commitTransaction()`
    /// (`MockProducer.java:204`).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is closed, is fenced, was not initialized for
    /// transactions, has no open transaction, an error was installed with
    /// [`set_commit_transaction_error`](MockProducer::set_commit_transaction_error),
    /// or the `flush()` this performs fails.
    async fn commit_transaction(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        inner.verify_transactions_initialized()?;
        inner.verify_transaction_in_flight()?;

        if let Some(err) = inner.commit_transaction_error.as_ref() {
            return Err(err.clone());
        }

        inner.flush()?;

        // Java 216/220: `sent.addAll(uncommittedSends)` then
        // `uncommittedSends.clear()`.
        let uncommitted_sends = std::mem::take(&mut inner.uncommitted_sends);
        inner.sent.extend(uncommitted_sends);

        // Java 217-218/221: the map *object* is appended to the history and the
        // field is then reassigned to a fresh map — deliberately not `clear()`,
        // which would empty the very map just published. `mem::take` is that pair.
        let uncommitted_offsets = std::mem::take(&mut inner.uncommitted_consumer_group_offsets);
        if !uncommitted_offsets.is_empty() {
            inner.consumer_group_offsets.push(uncommitted_offsets);
        }

        inner.transaction_committed = true;
        inner.transaction_aborted = false;
        inner.transaction_in_flight = false;

        inner.commit_count += 1;
        Ok(())
    }

    /// Abort the in-flight transaction, discarding its records and offsets.
    ///
    /// Corresponds to Java's `MockProducer.abortTransaction()`
    /// (`MockProducer.java:230`).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is closed, is fenced, was not initialized for
    /// transactions, has no open transaction, an error was installed with
    /// [`set_abort_transaction_error`](MockProducer::set_abort_transaction_error),
    /// or the `flush()` this performs fails.
    async fn abort_transaction(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        inner.verify_transactions_initialized()?;
        inner.verify_transaction_in_flight()?;

        if let Some(err) = inner.abort_transaction_error.as_ref() {
            return Err(err.clone());
        }

        inner.flush()?;
        // Java 241-242: `clear()` on both — unlike the commit path, neither
        // collection has been handed to the history, so there is nothing to detach.
        inner.uncommitted_sends.clear();
        inner.uncommitted_consumer_group_offsets.clear();
        inner.transaction_committed = false;
        inner.transaction_aborted = true;
        inner.transaction_in_flight = false;
        Ok(())
    }

    async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.send_with_callback(record, None).await
    }

    async fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if inner.closed {
            return Err(KafkaError::illegal_state("MockProducer is already closed."));
        }

        // Java 293-295 throws `KafkaException("MockProducer is fenced.", new
        // ProducerFencedException("Fenced"))` — a wrapper whose *cause* is what
        // `shouldThrowOnSendIfProducerGotFenced` asserts on. `KafkaError` has no
        // cause chain (PLAN §10.5 deviation 5), so the two collapse into one value
        // that keeps both observable halves: the fenced error code and Java's
        // wrapper message. It is the same value `verify_not_fenced` produces.
        if inner.producer_fenced {
            return Err(KafkaError::with_message(Errors::ProducerFenced, "MockProducer is fenced."));
        }

        if let Some(err) = inner.send_error.as_ref() {
            return Err(err.clone());
        }

        let partition = record.partition().unwrap_or(0);
        let tp = TopicPartition::new(record.topic().to_string(), partition);

        let result = Arc::new(ProduceRequestResult::new(tp.clone()));
        let future = Arc::new(FutureRecordMetadata::new(
            Arc::clone(&result),
            0,
            RecordBatch::NO_TIMESTAMP,
            0,
            0,
        ));

        let offset = next_offset(&mut inner.offsets, &tp);
        let base_offset = 0i64.max(offset - i64::from(i32::MAX));
        let batch_index = (offset.min(i64::from(i32::MAX))) as i32;

        let metadata = RecordMetadata::new(tp.clone(), base_offset, batch_index, RecordBatch::NO_TIMESTAMP, 0, 0);

        // Java 319-322: inside a transaction the record is staged rather than
        // published, and only `commitTransaction` moves it across.
        if inner.transaction_in_flight {
            inner.uncommitted_sends.push(record);
        } else {
            inner.sent.push(record);
        }

        let completion = Completion { offset, metadata, result: Arc::clone(&result), callback, topic_partition: tp };

        if inner.auto_complete {
            completion.complete(None);
        } else {
            inner.completions.push_back(completion);
        }

        Ok(KafkaFuture::new(future))
    }

    async fn flush(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush()
    }

    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let inner = self.inner.lock().unwrap();

        if let Some(err) = inner.partitions_for_error.as_ref() {
            return Err(err.clone());
        }

        Ok(inner.cluster.partitions_for_topic(topic).to_vec())
    }

    async fn close(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(err) = inner.close_error.as_ref() {
            return Err(err.clone());
        }

        inner.closed = true;
        Ok(())
    }

    async fn close_timeout(&self, _timeout: Duration) -> Result<(), KafkaError> {
        self.close().await
    }
}

/// Get the next offset for this topic/partition.
///
/// First call for a topic-partition returns 0 and stores 1. Subsequent calls
/// increment and return the previous value.
///
/// Corresponds to Java's `MockProducer.nextOffset(TopicPartition)`.
fn next_offset(offsets: &mut HashMap<TopicPartition, i64>, tp: &TopicPartition) -> i64 {
    match offsets.get_mut(tp) {
        Some(offset) => {
            let current = *offset;
            *offset = current + 1;
            current
        },
        None => {
            offsets.insert(tp.clone(), 1);
            0
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Errors;

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn make_record(topic: &str, key: &str, value: &str) -> ProducerRecord<String, String> {
        ProducerRecord::with_key(topic.to_string(), Some(key.to_string()), Some(value.to_string()))
    }

    // -----------------------------------------------------------------------
    // Tests translated from MockProducerTest.java
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.testAutoCompleteMock`.
    #[tokio::test]
    async fn test_auto_complete_mock() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let record1 = make_record("topic", "key1", "value1");

        let future = producer.send(record1.clone()).await.unwrap();
        assert!(future.is_done(), "Send should be immediately complete");

        let metadata = future.get().await;
        assert!(metadata.is_ok(), "Send should be successful");
        let md = metadata.unwrap();
        assert_eq!(0, md.offset(), "Offset should be 0");
        assert_eq!("topic", md.topic());

        assert_eq!(vec![record1], producer.history(), "We should have the record in our history");

        producer.clear();
        assert_eq!(0, producer.history().len(), "Clear should erase our history");
    }

    /// Translated from `MockProducerTest.testPartitioner`.
    ///
    /// Java's test uses a `RoundRobinPartitioner` with cluster metadata.
    /// Since our Rust `MockProducer` doesn't use a partitioner (it uses
    /// `record.partition().unwrap_or(0)` directly), we test that a record
    /// with an explicit partition is assigned correctly.
    #[tokio::test]
    async fn test_partitioner() {
        let node = crate::common::Node::new(0, "localhost".to_string(), 9092);
        let pi0 = PartitionInfo::new("topic".to_string(), 0, Some(node.clone()), vec![], vec![]);
        let pi1 = PartitionInfo::new("topic".to_string(), 1, Some(node), vec![], vec![]);

        let cluster = Cluster::new(
            None,
            vec![],
            vec![pi0, pi1],
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            None,
            HashMap::new(),
        );
        let producer: MockProducer<String, String> = MockProducer::new(cluster, true);

        // Send with explicit partition=1
        let record = ProducerRecord::with_partition(
            "topic".to_string(),
            Some(1),
            Some("key".to_string()),
            Some("value".to_string()),
        )
        .unwrap();
        let future = producer.send(record).await.unwrap();
        let md = future.get().await.unwrap();
        assert_eq!(1, md.partition(), "Partition should be correct");

        producer.clear();
        assert_eq!(0, producer.history().len(), "Clear should erase our history");
        producer.close().await.unwrap();
    }

    /// Translated from `MockProducerTest.testManualCompletion`.
    #[tokio::test]
    async fn test_manual_completion() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        let record1 = make_record("topic", "key1", "value1");
        let record2 = make_record("topic", "key2", "value2");

        let md1 = producer.send(record1.clone()).await.unwrap();
        assert!(!md1.is_done(), "Send shouldn't have completed");

        let md2 = producer.send(record2.clone()).await.unwrap();
        assert!(!md2.is_done(), "Send shouldn't have completed");

        assert!(producer.complete_next(), "Complete the first request");
        let result1 = md1.get().await;
        assert!(result1.is_ok(), "Request should be successful");
        assert!(!md2.is_done(), "Second request still incomplete");

        assert!(
            producer.error_next(KafkaError::illegal_argument("blah")),
            "Complete the second request with an error"
        );
        let result2 = md2.get().await;
        assert!(result2.is_err(), "Expected error to be thrown");

        assert!(!producer.complete_next(), "No more requests to complete");

        // Test flush completes remaining sends
        let md3 = producer.send(record1).await.unwrap();
        let md4 = producer.send(record2).await.unwrap();
        assert!(!md3.is_done() && !md4.is_done(), "Requests should not be completed.");
        producer.flush().await.unwrap();
        assert!(md3.is_done() && md4.is_done(), "Requests should be completed.");
    }

    // -----------------------------------------------------------------------
    // Transactional tests (landing)
    //
    // The transactional surface these exercise now exists (Milestone 11 Phase 7);
    // the tests land in the commits that follow, and each name is struck from this
    // list as it does. The list is replaced by the standard test-accounting block
    // once it is empty.
    //
    //   - shouldInitTransactions
    //   - shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions
    //   - shouldThrowOnBeginTransactionIfTransactionsNotInitialized
    //   - shouldBeginTransactions
    //   - shouldThrowOnBeginTransactionsIfTransactionInflight
    //   - shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized
    //   - shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted
    //   - shouldThrowOnCommitIfTransactionsNotInitialized
    //   - shouldThrowOnCommitTransactionIfNoTransactionGotStarted
    //   - shouldCommitEmptyTransaction
    //   - shouldCountCommittedTransaction
    //   - shouldNotCountAbortedTransaction
    //   - shouldThrowOnAbortIfTransactionsNotInitialized
    //   - shouldThrowOnAbortTransactionIfNoTransactionGotStarted
    //   - shouldAbortEmptyTransaction
    //   - shouldThrowFenceProducerIfTransactionsNotInitialized
    //   - shouldThrowOnBeginTransactionsIfProducerGotFenced
    //   - shouldThrowOnSendIfProducerGotFenced
    //   - shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced
    //   - shouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced
    //   - shouldThrowOnCommitTransactionIfProducerGotFenced
    //   - shouldThrowOnAbortTransactionIfProducerGotFenced
    //   - shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled
    //   - shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled
    //   - shouldDropMessagesOnAbortIfTransactionsAreEnabled
    //   - shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled
    //   - shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled
    //   - shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled
    //   - shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction
    //   - shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata
    //   - shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata
    //   - shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction
    //   - shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled
    //   - shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled
    //   - shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled
    //   - shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled
    //   - shouldThrowOnInitTransactionIfProducerIsClosed
    //   - shouldThrowOnBeginTransactionIfProducerIsClosed
    //   - shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed
    //   - shouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed
    //   - shouldThrowOnCommitTransactionIfProducerIsClosed
    //   - shouldThrowOnAbortTransactionIfProducerIsClosed
    //   - shouldThrowOnFenceProducerIfProducerIsClosed
    //   - shouldNotThrowOnFlushProducerIfProducerIsFenced
    //
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Serializer-related test (skipped)
    //
    //   - shouldThrowClassCastException: This test is Java-specific. It tests
    //     that Java's type erasure + serializer causes a ClassCastException
    //     when the wrong type is used. Rust has no type erasure and the
    //     MockProducer works with pre-serialized bytes, so this test is
    //     not applicable.
    //
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.shouldThrowOnSendIfProducerIsClosed`.
    #[tokio::test]
    async fn should_throw_on_send_if_producer_is_closed() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.close().await.unwrap();
        let result = producer.send(make_record("topic", "key1", "value1")).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.message().contains("MockProducer is already closed"));
    }

    /// Translated from `MockProducerTest.shouldThrowOnFlushProducerIfProducerIsClosed`.
    #[tokio::test]
    async fn should_throw_on_flush_if_producer_is_closed() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.close().await.unwrap();
        let result = producer.flush().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("MockProducer is already closed"));
    }

    /// Translated from `MockProducerTest.shouldBeFlushedIfNoBufferedRecords`.
    #[test]
    fn should_be_flushed_if_no_buffered_records() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldBeFlushedWithAutoCompleteIfBufferedRecords`.
    #[tokio::test]
    async fn should_be_flushed_with_auto_complete_if_buffered_records() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords`.
    #[tokio::test]
    async fn should_not_be_flushed_with_no_auto_complete_if_buffered_records() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        assert!(!producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedAfterFlush`.
    ///
    /// Note: The Java test name is misleading — it tests that after flush,
    /// `flushed()` returns `true` (not `false`). The Rust version matches the
    /// actual Java assertion: `assertTrue(producer.flushed())`.
    #[tokio::test]
    async fn should_be_flushed_after_flush() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        producer.flush().await.unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.testMetadataOnException`.
    ///
    /// Java's version uses a callback to inspect metadata on error. Since our
    /// Producer trait does not have callbacks, we test that `error_next()`
    /// causes the future to resolve with the injected error.
    #[tokio::test]
    async fn test_metadata_on_exception() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        let record2 = make_record("topic", "key2", "value2");

        let future = producer.send(record2).await.unwrap();
        let e = KafkaError::illegal_argument("dummy exception");
        assert!(producer.error_next(e), "Complete the request with an error");

        let result = future.get().await;
        assert!(result.is_err(), "Expected error");
    }

    // -----------------------------------------------------------------------
    // Additional unit tests
    // -----------------------------------------------------------------------

    /// Tests that `Default` creates a producer with `auto_complete=false`.
    #[tokio::test]
    async fn test_default() {
        let producer: MockProducer<String, String> = MockProducer::default();
        assert!(!producer.closed());
        assert!(producer.flushed());

        // auto_complete=false means sends don't complete immediately
        let future = producer.send(make_record("topic", "k", "v")).await.unwrap();
        assert!(!future.is_done());
    }

    /// Tests that multiple sends to the same topic-partition get incrementing
    /// offsets.
    #[tokio::test]
    async fn test_incrementing_offsets() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let f0 = producer.send(make_record("t", "k", "v0")).await.unwrap();
        let f1 = producer.send(make_record("t", "k", "v1")).await.unwrap();
        let f2 = producer.send(make_record("t", "k", "v2")).await.unwrap();

        assert_eq!(0, f0.get().await.unwrap().offset());
        assert_eq!(1, f1.get().await.unwrap().offset());
        assert_eq!(2, f2.get().await.unwrap().offset());
    }

    /// Tests that sends to different topic-partitions have independent offsets.
    #[tokio::test]
    async fn test_independent_topic_partition_offsets() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let r1 = ProducerRecord::with_value("t1".to_string(), Some("k".to_string()));
        let r2 = ProducerRecord::with_value("t2".to_string(), Some("k".to_string()));

        let f1 = producer.send(r1.clone()).await.unwrap();
        let f2 = producer.send(r2.clone()).await.unwrap();
        let f3 = producer.send(r1).await.unwrap();

        assert_eq!(0, f1.get().await.unwrap().offset());
        assert_eq!(0, f2.get().await.unwrap().offset());
        assert_eq!(1, f3.get().await.unwrap().offset());
    }

    /// Tests `set_send_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `sendException` is a field that persists until
    /// manually set to `null`.
    #[tokio::test]
    async fn test_set_send_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_send_error(Some(KafkaError::new(Errors::CorruptMessage)));

        let result = producer.send(make_record("t", "k", "v")).await;
        assert!(result.is_err());

        // Error persists — second send also fails
        let result = producer.send(make_record("t", "k", "v")).await;
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next send succeeds
        producer.set_send_error(None);
        let result = producer.send(make_record("t", "k", "v")).await;
        assert!(result.is_ok(), "Send should succeed after clearing error");
    }

    /// Tests `set_flush_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `flushException` is a field that persists until
    /// manually set to `null`.
    #[tokio::test]
    async fn test_set_flush_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_flush_error(Some(KafkaError::new(Errors::CorruptMessage)));

        let result = producer.flush().await;
        assert!(result.is_err());

        // Error persists — second flush also fails
        let result = producer.flush().await;
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next flush succeeds
        producer.set_flush_error(None);
        let result = producer.flush().await;
        assert!(result.is_ok(), "Flush should succeed after clearing error");
    }

    /// Tests `set_partitions_for_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `partitionsForException` is a field that persists
    /// until manually set to `null`.
    #[tokio::test]
    async fn test_set_partitions_for_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_partitions_for_error(Some(KafkaError::new(Errors::UnknownTopicOrPartition)));

        let result = producer.partitions_for("t").await;
        assert!(result.is_err());

        // Error persists — second call also fails
        let result = producer.partitions_for("t").await;
        assert!(result.is_err(), "Error should persist until cleared");

        // Clear the error — next call succeeds
        producer.set_partitions_for_error(None);
        let result = producer.partitions_for("t").await;
        assert!(result.is_ok());
    }

    /// Tests `set_close_error` injection persists until cleared.
    ///
    /// Matches Java behavior: `closeException` is a field that persists until
    /// manually set to `null`.
    #[tokio::test]
    async fn test_set_close_error() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.set_close_error(Some(KafkaError::new(Errors::UnknownServerError)));

        let result = producer.close().await;
        assert!(result.is_err());

        // Error persists — second close also fails
        let result = producer.close().await;
        assert!(result.is_err(), "Error should persist until cleared");
        assert!(!producer.closed(), "Producer should not be closed when error persists");

        // Clear the error — close now succeeds
        producer.set_close_error(None);
        let result = producer.close().await;
        assert!(result.is_ok(), "Close should succeed after clearing error");
        assert!(producer.closed());
    }

    /// Tests `partitions_for` with cluster metadata.
    #[tokio::test]
    async fn test_partitions_for() {
        let node = crate::common::Node::new(0, "localhost".to_string(), 9092);
        let pi0 = PartitionInfo::new("topic".to_string(), 0, Some(node.clone()), vec![], vec![]);
        let pi1 = PartitionInfo::new("topic".to_string(), 1, Some(node), vec![], vec![]);

        let cluster = Cluster::new(
            None,
            vec![],
            vec![pi0, pi1],
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            None,
            HashMap::new(),
        );
        let producer: MockProducer<String, String> = MockProducer::new(cluster, true);

        let partitions = producer.partitions_for("topic").await.unwrap();
        assert_eq!(2, partitions.len());
        assert_eq!(0, partitions[0].partition());
        assert_eq!(1, partitions[1].partition());

        // Unknown topic returns empty
        let partitions = producer.partitions_for("unknown").await.unwrap();
        assert!(partitions.is_empty());
    }

    /// Tests `close_timeout` behaves like `close`.
    #[tokio::test]
    async fn test_close_timeout() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        assert!(!producer.closed());
        producer.close_timeout(Duration::from_secs(5)).await.unwrap();
        assert!(producer.closed());
    }

    /// Tests that `clear` preserves offset counters (matching Java behavior).
    ///
    /// Java's `MockProducer.clear()` does NOT reset the `offsets` map, so
    /// offset numbering continues after `clear()`.
    #[tokio::test]
    async fn test_clear_preserves_offsets() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        producer.send(make_record("t", "k", "v")).await.unwrap();
        producer.send(make_record("t", "k", "v")).await.unwrap();

        producer.clear();

        let future = producer.send(make_record("t", "k", "v")).await.unwrap();
        assert_eq!(
            2,
            future.get().await.unwrap().offset(),
            "Offset should continue from 2 after clear (not restart from 0)"
        );
    }

    /// Tests `next_offset` helper function.
    #[test]
    fn test_next_offset() {
        let mut offsets = HashMap::new();
        let tp = TopicPartition::new("t".to_string(), 0);

        assert_eq!(0, next_offset(&mut offsets, &tp));
        assert_eq!(1, next_offset(&mut offsets, &tp));
        assert_eq!(2, next_offset(&mut offsets, &tp));

        let tp2 = TopicPartition::new("t".to_string(), 1);
        assert_eq!(0, next_offset(&mut offsets, &tp2));
    }

    /// Tests that `history` returns a clone (modifications don't affect internal state).
    #[tokio::test]
    async fn test_history_returns_clone() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(true);
        let record = make_record("t", "k", "v");
        producer.send(record).await.unwrap();

        let mut history = producer.history();
        assert_eq!(1, history.len());

        // Modifying returned history does not affect internal state
        history.clear();
        assert_eq!(1, producer.history().len());
    }

    /// Tests that `error_next` returns false when no completions are pending.
    #[test]
    fn test_error_next_no_pending() {
        let producer: MockProducer<String, String> = MockProducer::with_auto_complete(false);
        assert!(!producer.error_next(KafkaError::new(Errors::UnknownServerError)));
    }
}
