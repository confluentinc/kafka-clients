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
//! [`Error::local_illegal_state`] and `ProducerFencedException` becomes a
//! [`Error`] carrying [`Errors::ProducerFenced`], with Java's message text
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
use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::MetricName;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::metrics::KafkaMetric;
use crate::common::protocol::Errors;
use crate::common::record::internal::RecordBatch;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;

use super::Callback;

// =========================================================================
// PHASE-7 METHOD ACCOUNTING (`definition-of-done.md` §2)
//
// `MockProducer.java` declares **40** distinct method names at class level.
// **30** have a Rust `fn` in this file; the **10** absent are named below with
// what each is blocked on.
//
// Constructors are excluded by construction rather than by assertion: the
// negative lookahead after the modifier run forces a return type *and* a name,
// and a constructor has only a name. (Critic 45 issue 4 was the failure of
// asserting that instead — with the modifier group free to match zero times the
// engine reads `public` as the return type and counts the constructor anyway.)
// The Rust file is cut at its `#[cfg(test)]` module before the `fn` scan, so no
// test-only item can satisfy a production-method claim, and the `assert` on the
// cut's length is not decoration: a rename of that attribute would otherwise
// leave an empty corpus reporting all 40 as absent.
//
// Derivation (real output below it):
//
//   python3 - <<'PY'
//   import re
//   J = ("kafka/clients/src/main/java/org/apache/kafka/clients/producer/"
//        "MockProducer.java")
//   R = "src/producer/mock_producer.rs"
//   MODS = r'(?:public|private|protected|synchronized|static|final|abstract)'
//   decl = re.compile(rf'^    (?:{MODS}\s+)*(?!{MODS}[\s(])'
//                     rf'[A-Za-z_][A-Za-z0-9_\.\[\]]*(?:<.*>)?\s+'
//                     rf'([a-zA-Z_][A-Za-z0-9_]*)\s*\(')
//   names = {}
//   for i, line in enumerate(open(J), 1):
//       if any(k in line for k in ('class ', 'enum ', 'interface ')): continue
//       m = decl.match(line)
//       if m: names.setdefault(m.group(1), i)
//   snake = lambda n: re.sub(r'(?<!^)(?=[A-Z])', '_', n).lower()
//   production = open(R).read().split("\n#[cfg(test)]\n")[0]
//   assert len(production) > 20000, len(production)
//   defs = set(re.findall(r'\bfn ([a-z_0-9]+)\s*[(<]', production))
//   missing = sorted((ln, n) for n, ln in names.items() if snake(n) not in defs)
//   print(f"{len(names)} declared, {len(names)-len(missing)} present, "
//         f"{len(missing)} absent")
//   for ln, n in missing: print(f"  {ln:4d} {n}")
//   PY
//
//   40 declared, 30 present, 10 absent
//     366 disableTelemetry
//     373 injectTimeoutException
//     377 setClientInstanceId
//     382 clientInstanceId
//     400 metrics
//     407 setMockMetrics
//     526 partition
//     584 addedMetrics
//     589 registerMetricForSubscription
//     594 unregisterMetricFromSubscription
//
// All ten predate Phase 7 and none is in its scope — PLAN §Phase-7 enumerates
// the transactional surface, and these are three other features:
//
//   `clientInstanceId` (382), `metrics` (400), `registerMetricForSubscription`
//     (589), `unregisterMetricFromSubscription` (594) — `Producer`-interface
//     methods the *Rust trait does not declare*. The gap is in
//     `producer_trait.rs`, not here: a `MockProducer` impl would have nothing to
//     override. Tracked as PLAN §9.23.
//   `disableTelemetry` (366), `injectTimeoutException` (373),
//     `setClientInstanceId` (377), `setMockMetrics` (407), `addedMetrics` (584)
//     — the mock-only knobs that exist to drive those same two features. They
//     follow whenever the four above land. Same §9.23.
//   `partition` (526) — needs `Partitioner` plus the two `Serializer`s. The Rust
//     mock takes pre-serialized bytes by design, stated at [`MockProducer::new`].
//
// What that costs the test parity, precisely: nine of the ten appear **zero**
// times in `MockProducerTest.java`, so they block nothing —
//
//   J=kafka/clients/src/test/java/org/apache/kafka/clients/producer/MockProducerTest.java
//   for m in disableTelemetry injectTimeoutException setClientInstanceId \
//            clientInstanceId 'metrics(' setMockMetrics addedMetrics \
//            registerMetricForSubscription unregisterMetricFromSubscription; do
//     printf '%s %s\n' "$m" "$(grep -c "$m" $J)"; done
//
// prints `0` nine times. The tenth, `partition`, is driven by the
// `RoundRobinPartitioner` that `testPartitioner` passes (Java 94-96) and by the
// serializers `shouldThrowClassCastException` passes (690) — which is why the
// first is translated in adapted form and the second is not applicable. Both are
// stated at their entries in the test accounting block at the end of this file.
//
// Two Java names cover four Rust `fn`s, so "30 present" is not "30 signatures":
// `send` (278, 288) → `send` / `send_with_callback`, and `close` (412, 417) →
// `close` / `close_timeout`. The nested `Completion` class sits at 8-space
// indent and so is outside the scan; its one method, `complete` (567), is
// translated as `Completion::complete`.
//
// Java's nine public `RuntimeException` *fields* (79-87) are not methods and are
// invisible to the derivation. All nine are present as `set_*_error` setters;
// the five transactional ones landed in Phase 7, covered by
// `test_set_transactional_errors`.
// =========================================================================

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
    init_transaction_error: Option<Error>,
    /// Java `beginTransactionException` (`:80`).
    begin_transaction_error: Option<Error>,
    /// Java `sendOffsetsToTransactionException` (`:81`).
    send_offsets_to_transaction_error: Option<Error>,
    /// Java `commitTransactionException` (`:82`).
    commit_transaction_error: Option<Error>,
    /// Java `abortTransactionException` (`:83`).
    abort_transaction_error: Option<Error>,
    /// Java `sendException` (`:84`).
    send_error: Option<Error>,
    /// Java `flushException` (`:85`).
    flush_error: Option<Error>,
    /// Java `partitionsForException` (`:86`).
    partitions_for_error: Option<Error>,
    /// Java `closeException` (`:87`).
    close_error: Option<Error>,
    /// User-supplied metrics returned by [`metrics()`](Producer::metrics).
    ///
    /// Mirrors Java's `MockProducer.mockMetrics` map, seeded via
    /// [`set_mock_metrics`](MockProducer::set_mock_metrics).
    mock_metrics: HashMap<MetricName, Arc<KafkaMetric>>,
}

impl<K, V> MockProducerInner<K, V> {
    /// Corresponds to Java's `verifyNotClosed()` (`MockProducer.java:248`).
    fn verify_not_closed(&self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::local_illegal_state("MockProducer is already closed."));
        }
        Ok(())
    }

    /// Corresponds to Java's `verifyNotFenced()` (`MockProducer.java:254`),
    /// which throws `ProducerFencedException`.
    fn verify_not_fenced(&self) -> Result<(), Error> {
        if self.producer_fenced {
            return Err(Error::with_message(Errors::ProducerFenced, "MockProducer is fenced."));
        }
        Ok(())
    }

    /// Corresponds to Java's `verifyTransactionsInitialized()`
    /// (`MockProducer.java:260`).
    fn verify_transactions_initialized(&self) -> Result<(), Error> {
        if !self.transaction_initialized {
            return Err(Error::local_illegal_state(
                "MockProducer hasn't been initialized for transactions.",
            ));
        }
        Ok(())
    }

    /// Corresponds to Java's `verifyTransactionInFlight()`
    /// (`MockProducer.java:266`).
    fn verify_transaction_in_flight(&self) -> Result<(), Error> {
        if !self.transaction_in_flight {
            return Err(Error::local_illegal_state("There is no open transaction."));
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
    fn flush(&mut self) -> Result<(), Error> {
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
    fn error_next(&mut self, error: Option<Error>) -> bool {
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
    fn complete(self, error: Option<Error>) {
        let Completion { offset, metadata, result, callback, topic_partition } = self;
        if let Some(e) = error {
            let error_fn: Arc<dyn Fn(i32) -> Option<Error> + Send + Sync> = {
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
                mock_metrics: HashMap::new(),
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

    /// Look up the offset a committed transaction staged for `group` /
    /// `topic_partition`, newest transaction first.
    ///
    /// A targeted lookup over the same data as
    /// [`consumer_group_offsets_history()`](Self::consumer_group_offsets_history).
    /// It exists because that accessor deep-clones the whole history — a
    /// `Vec<HashMap<String, HashMap<TopicPartition, OffsetAndMetadata>>>` — which
    /// is wasteful for a caller that wants one entry, and unreasonably so for the
    /// C FFI probe that does it on every call. Here the scan happens under the
    /// lock and only the matching entry is cloned. No Java counterpart; Java
    /// callers index the returned map directly.
    pub fn committed_offset(&self, group: &str, topic_partition: &TopicPartition) -> Option<OffsetAndMetadata> {
        let inner = self.inner.lock().unwrap();
        inner
            .consumer_group_offsets
            .iter()
            .rev()
            .find_map(|txn| txn.get(group).and_then(|offsets| offsets.get(topic_partition)))
            .cloned()
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
    pub fn error_next(&self, error: Error) -> bool {
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
    /// initialized for transactions ([`Error::local_illegal_state`] for the first
    /// and last, [`Errors::ProducerFenced`] for the second).
    pub fn fence_producer(&self) -> Result<(), Error> {
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
    pub fn set_send_error(&self, error: Option<Error>) {
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
    pub fn set_flush_error(&self, error: Option<Error>) {
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
    pub fn set_partitions_for_error(&self, error: Option<Error>) {
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
    pub fn set_close_error(&self, error: Option<Error>) {
        let mut inner = self.inner.lock().unwrap();
        inner.close_error = error;
    }

    /// Seed a metric returned by [`metrics()`](Producer::metrics).
    ///
    /// Corresponds to Java's `MockProducer.setMockMetrics(MetricName name,
    /// Metric metric)`.
    pub fn set_mock_metrics(&self, name: MetricName, metric: Arc<KafkaMetric>) {
        let mut inner = self.inner.lock().unwrap();
        inner.mock_metrics.insert(name, metric);
    }

    /// Set an error to be returned on every
    /// [`init_transactions()`](Producer::init_transactions) call until cleared.
    ///
    /// Matches Java's public `MockProducer.initTransactionException` field
    /// (`MockProducer.java:79`), which likewise persists until set back to `null`.
    pub fn set_init_transaction_error(&self, error: Option<Error>) {
        let mut inner = self.inner.lock().unwrap();
        inner.init_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`begin_transaction()`](Producer::begin_transaction) call until cleared.
    ///
    /// Matches Java's public `MockProducer.beginTransactionException` field
    /// (`MockProducer.java:80`).
    pub fn set_begin_transaction_error(&self, error: Option<Error>) {
        let mut inner = self.inner.lock().unwrap();
        inner.begin_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`send_offsets_to_transaction()`](Producer::send_offsets_to_transaction)
    /// call until cleared.
    ///
    /// Matches Java's public `MockProducer.sendOffsetsToTransactionException`
    /// field (`MockProducer.java:81`).
    pub fn set_send_offsets_to_transaction_error(&self, error: Option<Error>) {
        let mut inner = self.inner.lock().unwrap();
        inner.send_offsets_to_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`commit_transaction()`](Producer::commit_transaction) call until cleared.
    ///
    /// Matches Java's public `MockProducer.commitTransactionException` field
    /// (`MockProducer.java:82`).
    pub fn set_commit_transaction_error(&self, error: Option<Error>) {
        let mut inner = self.inner.lock().unwrap();
        inner.commit_transaction_error = error;
    }

    /// Set an error to be returned on every
    /// [`abort_transaction()`](Producer::abort_transaction) call until cleared.
    ///
    /// Matches Java's public `MockProducer.abortTransactionException` field
    /// (`MockProducer.java:83`).
    pub fn set_abort_transaction_error(&self, error: Option<Error>) {
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
    async fn init_transactions(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        if inner.transaction_initialized {
            return Err(Error::local_illegal_state(
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
    fn begin_transaction(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.verify_not_closed()?;
        inner.verify_not_fenced()?;
        inner.verify_transactions_initialized()?;

        if let Some(err) = inner.begin_transaction_error.as_ref() {
            return Err(err.clone());
        }

        if inner.transaction_in_flight {
            return Err(Error::local_illegal_state("Transaction already started"));
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
    ) -> Result<(), Error> {
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
    async fn commit_transaction(&self) -> Result<(), Error> {
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
    async fn abort_transaction(&self) -> Result<(), Error> {
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

    async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.send_with_callback(record, None).await
    }

    async fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        let mut inner = self.inner.lock().unwrap();

        if inner.closed {
            return Err(Error::local_illegal_state("MockProducer is already closed."));
        }

        // Java `:293` throws `KafkaException("MockProducer is fenced.", new
        // ProducerFencedException("Fenced"))` — deliberately a DIFFERENT value from
        // `verifyNotFenced`'s bare `ProducerFencedException("MockProducer is fenced.")`
        // (`:256`): here the fenced error is the *cause* of a bare `KafkaException`,
        // which is what `shouldThrowOnSendIfProducerGotFenced` asserts
        // (`assertThrows(KafkaException.class, ..)` plus
        // `assertInstanceOf(ProducerFencedException.class, e.getCause())`). The outer
        // error must therefore be a bare `KafkaError` — `is_api_error()` is `false`
        // for it and `true` for `ProducerFencedError`.
        if inner.producer_fenced {
            return Err(Error::kafka_message_source(
                "MockProducer is fenced.",
                Error::with_message(Errors::ProducerFenced, "Fenced"),
            ));
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

    async fn flush(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush()
    }

    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
        let inner = self.inner.lock().unwrap();

        if let Some(err) = inner.partitions_for_error.as_ref() {
            return Err(err.clone());
        }

        Ok(inner.cluster.partitions_for_topic(topic).to_vec())
    }

    /// Return the mock metrics. Corresponds to Java's `MockProducer.metrics()`
    /// returning the `mockMetrics` map.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        let inner = self.inner.lock().unwrap();
        inner.mock_metrics.clone()
    }

    async fn close(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(err) = inner.close_error.as_ref() {
            return Err(err.clone());
        }

        inner.closed = true;
        Ok(())
    }

    async fn close_timeout(&self, _timeout: Duration) -> Result<(), Error> {
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

    // -----------------------------------------------------------------------
    // Fixtures, mirroring `MockProducerTest.java`'s fields (56-64)
    // -----------------------------------------------------------------------

    /// Java's `MockProducerTest.topic` (`MockProducerTest.java:56`).
    const TOPIC: &str = "topic";

    /// The four `RecordMetadata` fields `testMetadataOnException` inspects
    /// (`MockProducerTest.java:728-731`): offset, timestamp, serialized key size,
    /// serialized value size.
    type ObservedMetadata = (i64, i64, i32, i32);

    /// Java's `MockProducerTest.groupId` (`MockProducerTest.java:60`).
    const GROUP_ID: &str = "group";

    fn make_record(topic: &str, key: &str, value: &str) -> ProducerRecord<String, String> {
        ProducerRecord::with_key(topic.to_string(), Some(key.to_string()), Some(value.to_string()))
    }

    /// Java's `MockProducerTest.record1` (`MockProducerTest.java:58`). A function
    /// rather than a field because the tests hand records to `send` by value.
    fn record1() -> ProducerRecord<String, String> {
        make_record(TOPIC, "key1", "value1")
    }

    /// Java's `MockProducerTest.record2` (`MockProducerTest.java:59`).
    fn record2() -> ProducerRecord<String, String> {
        make_record(TOPIC, "key2", "value2")
    }

    /// Java's `MockProducerTest.buildMockProducer(boolean)`
    /// (`MockProducerTest.java:62`), which passes `Cluster.empty()` and the two
    /// `MockSerializer`s the Rust mock has no counterpart for (it takes
    /// pre-serialized bytes — see [`MockProducer::new`]).
    fn build_mock_producer(auto_complete: bool) -> MockProducer<String, String> {
        MockProducer::with_auto_complete(auto_complete)
    }

    /// Java's `new ConsumerGroupMetadata(groupId)`. The Rust constructor carries
    /// `#[deprecated]`, mirroring Java's `@Deprecated(since = "4.2")`; the tests
    /// must still exercise it, so the allowance sits at this one call site.
    #[allow(deprecated)]
    fn group_metadata(group_id: &str) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata::new(group_id)
    }

    /// Assert `result` failed the way Java's `IllegalStateException` does, with
    /// Java's message text (`definition-of-done.md` §3 — the message is part of
    /// the contract, so `is_err()` alone is not enough).
    fn assert_illegal_state<T>(result: Result<T, Error>, message: &str) {
        let error = result.err().expect("expected an IllegalState error, got Ok");
        assert!(
            matches!(error, Error::LocalIllegalState(_)),
            "expected IllegalState, got {error}"
        );
        assert_eq!(message, error.message());
    }

    /// Build the `(partition, offset)` map Java spells as an anonymous `HashMap`
    /// subclass, e.g. `MockProducerTest.java:403-408`.
    ///
    /// Java writes `new OffsetAndMetadata(42L, null)`; Rust's `metadata` is a
    /// non-nullable `String`, so `OffsetAndMetadata::new(offset)` — metadata `""`,
    /// Java's own one-arg default — is the closest analogue. Every expected and
    /// actual value below is built through this one helper, so the equality
    /// comparisons are the comparisons Java makes.
    fn offsets(entries: &[(i32, i64)]) -> HashMap<TopicPartition, OffsetAndMetadata> {
        entries
            .iter()
            .map(|&(partition, offset)| {
                (
                    TopicPartition::new(TOPIC.to_string(), partition),
                    OffsetAndMetadata::new(offset).expect("offset must be non-negative"),
                )
            })
            .collect()
    }

    /// Assert `result` failed the way Java's `ProducerFencedException` does.
    ///
    /// `MockProducer` raises it with exactly one message, from `verifyNotFenced`
    /// (`MockProducer.java:256`) and from the fenced `send` (`:294`).
    fn assert_producer_fenced<T>(result: Result<T, Error>) {
        let error = result.err().expect("expected a ProducerFenced error, got Ok");
        assert_eq!(Errors::ProducerFenced, error.error(), "expected ProducerFenced, got {error}");
        assert_eq!("MockProducer is fenced.", error.message());
    }

    // -----------------------------------------------------------------------
    // Tests translated from MockProducerTest.java
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.testAutoCompleteMock` (Java 73).
    #[tokio::test]
    async fn test_auto_complete_mock() {
        let producer = build_mock_producer(true);
        let record1 = record1();

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

    /// Translated from `MockProducerTest.testPartitioner` (Java 86).
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

    /// Translated from `MockProducerTest.testManualCompletion` (Java 107).
    #[tokio::test]
    async fn test_manual_completion() {
        let producer = build_mock_producer(false);
        let record1 = record1();
        let record2 = record2();

        let md1 = producer.send(record1.clone()).await.unwrap();
        assert!(!md1.is_done(), "Send shouldn't have completed");

        let md2 = producer.send(record2.clone()).await.unwrap();
        assert!(!md2.is_done(), "Send shouldn't have completed");

        assert!(producer.complete_next(), "Complete the first request");
        let result1 = md1.get().await;
        assert!(result1.is_ok(), "Request should be successful");
        assert!(!md2.is_done(), "Second request still incomplete");

        assert!(
            producer.error_next(Error::local_illegal_argument("blah")),
            "Complete the second request with an error"
        );
        // Java asserts `assertEquals(e, err.getCause())`; `Error` has no cause
        // chain, so the message identifies the injected error.
        let error = md2.get().await.expect_err("Expected error to be thrown");
        assert_eq!("blah", error.message());

        assert!(!producer.complete_next(), "No more requests to complete");

        // Test flush completes remaining sends
        let md3 = producer.send(record1).await.unwrap();
        let md4 = producer.send(record2).await.unwrap();
        assert!(!md3.is_done() && !md4.is_done(), "Requests should not be completed.");
        producer.flush().await.unwrap();
        assert!(md3.is_done() && md4.is_done(), "Requests should be completed.");
    }

    /// Translated from `MockProducerTest.shouldThrowOnSendIfProducerIsClosed` (Java 624).
    #[tokio::test]
    async fn should_throw_on_send_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.send(record1()).await, "MockProducer is already closed.");
    }

    /// Translated from `MockProducerTest.shouldThrowOnFlushProducerIfProducerIsClosed` (Java 673).
    #[tokio::test]
    async fn should_throw_on_flush_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.flush().await, "MockProducer is already closed.");
    }

    /// Translated from `MockProducerTest.shouldBeFlushedIfNoBufferedRecords` (Java 696).
    #[test]
    fn should_be_flushed_if_no_buffered_records() {
        let producer = build_mock_producer(true);
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldBeFlushedWithAutoCompleteIfBufferedRecords` (Java 702).
    #[tokio::test]
    async fn should_be_flushed_with_auto_complete_if_buffered_records() {
        let producer = build_mock_producer(true);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords` (Java 709).
    #[tokio::test]
    async fn should_not_be_flushed_with_no_auto_complete_if_buffered_records() {
        let producer = build_mock_producer(false);
        producer.send(make_record("topic", "key1", "value1")).await.unwrap();
        assert!(!producer.flushed());
    }

    /// Translated from `MockProducerTest.shouldNotBeFlushedAfterFlush` (Java 716).
    ///
    /// Note: the Java name is misleading — the body asserts that after flush
    /// `flushed()` returns `true`, not `false`. The name is kept as Java spells it
    /// (the same call this phase makes for
    /// `shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled`, whose name
    /// is misleading in the same way) and the assertion follows the body.
    #[tokio::test]
    async fn should_not_be_flushed_after_flush() {
        let producer = build_mock_producer(false);
        producer.send(record1()).await.unwrap();
        producer.flush().await.unwrap();
        assert!(producer.flushed());
    }

    /// Translated from `MockProducerTest.testMetadataOnException` (Java 724).
    ///
    /// Java asserts on the metadata handed to the send callback, inline in the
    /// callback body (Java 727-731). The four values are captured here and asserted
    /// *after* `error_next` returns instead, because an assertion that only ever
    /// runs inside the callback cannot fail when the callback **never runs** — Java
    /// has that hole too, and `assertNotNull(md)` at 727 does not close it. Capturing
    /// outside makes "never fired" a failure, so this form is strictly stronger.
    ///
    /// A panic *is* propagated on this path — `error_next` → `Completion::complete`
    /// → `cb(..)` is synchronous with no `catch_unwind`, and the closure's own
    /// `expect` below relies on that. (Java's producer does swallow callback
    /// exceptions, but in `ProducerBatch.completeFutureAndFireCallbacks`
    /// (`ProducerBatch.java:318-320`), not in `MockProducer.Completion.complete`,
    /// which has no try/catch.)
    #[tokio::test]
    async fn test_metadata_on_error() {
        let producer = build_mock_producer(false);

        let observed: Arc<Mutex<Option<ObservedMetadata>>> = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&observed);
        let callback: Callback = Box::new(move |metadata, _error| {
            let metadata = metadata.expect("the callback must receive metadata on the error path");
            *sink.lock().unwrap() = Some((
                metadata.offset(),
                metadata.timestamp(),
                metadata.serialized_key_size(),
                metadata.serialized_value_size(),
            ));
        });

        let future = producer.send_with_callback(record2(), Some(callback)).await.unwrap();
        let e = Error::local_illegal_argument("dummy error");
        assert!(producer.error_next(e), "Complete the second request with an error");

        let (offset, timestamp, key_size, value_size) = observed.lock().unwrap().expect("the callback did not fire");
        assert_eq!(-1, offset, "Invalid offset");
        assert_eq!(RecordBatch::NO_TIMESTAMP, timestamp, "Invalid timestamp");
        assert_eq!(-1, key_size, "Invalid Serialized Key size");
        assert_eq!(-1, value_size, "Invalid Serialized value size");

        // Java asserts the injected exception is the future's cause; `Error`
        // has no cause chain, so the message identifies it.
        let result = future.get().await;
        let error = result.expect_err("Something went wrong, expected an error");
        assert_eq!("dummy error", error.message());
    }

    // -----------------------------------------------------------------------
    // Transaction lifecycle: init / begin / commit / abort state and counts
    // -----------------------------------------------------------------------

    /// Translated from `MockProducerTest.shouldInitTransactions` (Java 134).
    #[tokio::test]
    async fn should_init_transactions() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        assert!(producer.transaction_initialized());
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions`
    /// (Java 141).
    #[tokio::test]
    async fn should_throw_on_init_transaction_if_producer_already_initialized_for_transactions() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        assert_illegal_state(
            producer.init_transactions().await,
            "MockProducer has already been initialized for transactions.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnBeginTransactionIfTransactionsNotInitialized`
    /// (Java 148).
    #[test]
    fn should_throw_on_begin_transaction_if_transactions_not_initialized() {
        let producer = build_mock_producer(true);
        assert_illegal_state(
            producer.begin_transaction(),
            "MockProducer hasn't been initialized for transactions.",
        );
    }

    /// Translated from `MockProducerTest.shouldBeginTransactions` (Java 154).
    #[tokio::test]
    async fn should_begin_transactions() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
        assert!(producer.transaction_in_flight());
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnBeginTransactionsIfTransactionInflight`
    /// (Java 162).
    #[tokio::test]
    async fn should_throw_on_begin_transactions_if_transaction_inflight() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
        assert_illegal_state(producer.begin_transaction(), "Transaction already started");
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized`
    /// (Java 170).
    ///
    /// Java passes `null` for the offsets map, relying on the guard firing before
    /// `offsets.isEmpty()` is reached (`MockProducer.java:194`). A `HashMap`
    /// parameter is not nullable, so the empty map stands in — and it tests the
    /// same ordering: were the guard to move below the emptiness check, Java would
    /// throw `NullPointerException` and this assertion would see `Ok`.
    #[tokio::test]
    async fn should_throw_on_send_offsets_to_transaction_if_transactions_not_initialized() {
        let producer = build_mock_producer(true);
        assert_illegal_state(
            producer
                .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
                .await,
            "MockProducer hasn't been initialized for transactions.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted`
    /// (Java 176). Java's `null` offsets map becomes the empty map, as in
    /// `should_throw_on_send_offsets_to_transaction_if_transactions_not_initialized`.
    #[tokio::test]
    async fn should_throw_on_send_offsets_to_transaction_transaction_if_no_transaction_got_started() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        assert_illegal_state(
            producer
                .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
                .await,
            "There is no open transaction.",
        );
    }

    /// Translated from `MockProducerTest.shouldThrowOnCommitIfTransactionsNotInitialized`
    /// (Java 183).
    #[tokio::test]
    async fn should_throw_on_commit_if_transactions_not_initialized() {
        let producer = build_mock_producer(true);
        assert_illegal_state(
            producer.commit_transaction().await,
            "MockProducer hasn't been initialized for transactions.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnCommitTransactionIfNoTransactionGotStarted`
    /// (Java 189).
    #[tokio::test]
    async fn should_throw_on_commit_transaction_if_no_transaction_got_started() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        assert_illegal_state(producer.commit_transaction().await, "There is no open transaction.");
    }

    /// Translated from `MockProducerTest.shouldCommitEmptyTransaction` (Java 196).
    #[tokio::test]
    async fn should_commit_empty_transaction() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
        producer.commit_transaction().await.unwrap();
        assert!(!producer.transaction_in_flight());
        assert!(producer.transaction_committed());
        assert!(!producer.transaction_aborted());
    }

    /// Translated from `MockProducerTest.shouldCountCommittedTransaction` (Java 207).
    #[tokio::test]
    async fn should_count_committed_transaction() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        assert_eq!(0, producer.commit_count());
        producer.commit_transaction().await.unwrap();
        assert_eq!(1, producer.commit_count());
    }

    /// Translated from `MockProducerTest.shouldNotCountAbortedTransaction` (Java 218).
    #[tokio::test]
    async fn should_not_count_aborted_transaction() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.abort_transaction().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.commit_transaction().await.unwrap();
        assert_eq!(1, producer.commit_count());
    }

    /// Translated from `MockProducerTest.shouldThrowOnAbortIfTransactionsNotInitialized`
    /// (Java 231).
    #[tokio::test]
    async fn should_throw_on_abort_if_transactions_not_initialized() {
        let producer = build_mock_producer(true);
        assert_illegal_state(
            producer.abort_transaction().await,
            "MockProducer hasn't been initialized for transactions.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnAbortTransactionIfNoTransactionGotStarted`
    /// (Java 237).
    #[tokio::test]
    async fn should_throw_on_abort_transaction_if_no_transaction_got_started() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        assert_illegal_state(producer.abort_transaction().await, "There is no open transaction.");
    }

    /// Translated from `MockProducerTest.shouldAbortEmptyTransaction` (Java 244).
    #[tokio::test]
    async fn should_abort_empty_transaction() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
        producer.abort_transaction().await.unwrap();
        assert!(!producer.transaction_in_flight());
        assert!(producer.transaction_aborted());
        assert!(!producer.transaction_committed());
    }

    // -----------------------------------------------------------------------
    // Fencing: every entry point after `fenceProducer()`
    // -----------------------------------------------------------------------

    /// Translated from
    /// `MockProducerTest.shouldThrowFenceProducerIfTransactionsNotInitialized`
    /// (Java 255).
    #[test]
    fn should_throw_fence_producer_if_transactions_not_initialized() {
        let producer = build_mock_producer(true);
        assert_illegal_state(
            producer.fence_producer(),
            "MockProducer hasn't been initialized for transactions.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnBeginTransactionsIfProducerGotFenced` (Java 261).
    #[tokio::test]
    async fn should_throw_on_begin_transactions_if_producer_got_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        assert_producer_fenced(producer.begin_transaction());
    }

    /// Translated from `MockProducerTest.shouldThrowOnSendIfProducerGotFenced`
    /// (Java 269).
    ///
    /// Java calls `producer.send(null)`; the fenced check (`MockProducer.java:293`)
    /// precedes any use of the record, and `ProducerRecord` is taken by value here,
    /// so a real record makes the same point.
    ///
    /// Java throws a bare `KafkaException` *wrapping* a `ProducerFencedException`
    /// and asserts on the cause, so both halves are checked here: the outer error is
    /// a bare `KafkaError` (`is_api_error() == false`) carrying the wrapper message,
    /// and its `source()` is the `ProducerFenced` error.
    #[tokio::test]
    async fn should_throw_on_send_if_producer_got_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        let error = producer.send(record1()).await.expect_err("expected a fenced error, got Ok");

        // `assertThrows(KafkaException.class, ..)` — a BARE `KafkaException`, not the
        // `ProducerFencedException` that `verify_not_fenced` raises.
        assert!(
            matches!(error, Error::KafkaError(_)),
            "expected a bare KafkaError, got {error:?}"
        );
        assert!(error.is_kafka_error(), "Java throws KafkaException here");
        assert!(!error.is_api_error(), "a bare KafkaException is not an ApiException");
        assert_eq!("MockProducer is fenced.", error.message());

        // `assertInstanceOf(ProducerFencedException.class, e.getCause())`.
        let cause = crate::common::kafka_error::ErrorSource::source(&error)
            .expect("Java chains a ProducerFencedException as the cause");
        assert_eq!(Errors::ProducerFenced, cause.error(), "expected ProducerFenced, got {cause}");
        assert_eq!("Fenced", cause.message());
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnSendOffsetsToTransactionByGroupIdIfProducerGotFenced`
    /// (Java 278).
    ///
    /// Java 278 and 286 have identical bodies: the group-id overload of
    /// `sendOffsetsToTransaction` was removed, so both now call the
    /// `ConsumerGroupMetadata` one. Both are translated anyway —
    /// `definition-of-done.md` §3 does not allow skipping a Java test because a
    /// sibling duplicates it.
    #[tokio::test]
    async fn should_throw_on_send_offsets_to_transaction_by_group_id_if_producer_got_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        assert_producer_fenced(
            producer
                .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
                .await,
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnSendOffsetsToTransactionByGroupMetadataIfProducerGotFenced`
    /// (Java 286) — the identical twin of Java 278, see the note there.
    #[tokio::test]
    async fn should_throw_on_send_offsets_to_transaction_by_group_metadata_if_producer_got_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        assert_producer_fenced(
            producer
                .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
                .await,
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnCommitTransactionIfProducerGotFenced` (Java 294).
    #[tokio::test]
    async fn should_throw_on_commit_transaction_if_producer_got_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        assert_producer_fenced(producer.commit_transaction().await);
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnAbortTransactionIfProducerGotFenced` (Java 302).
    #[tokio::test]
    async fn should_throw_on_abort_transaction_if_producer_got_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        assert_producer_fenced(producer.abort_transaction().await);
    }

    /// Translated from
    /// `MockProducerTest.shouldNotThrowOnFlushProducerIfProducerIsFenced` (Java 680).
    ///
    /// The one entry point fencing does *not* close: Java's `flush()`
    /// (`MockProducer.java:347`) omits `verifyNotFenced()`.
    #[tokio::test]
    async fn should_not_throw_on_flush_producer_if_producer_is_fenced() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.fence_producer().unwrap();
        producer.flush().await.expect("flush must not fail on a fenced producer");
    }

    // -----------------------------------------------------------------------
    // Closed producer: every transactional entry point after `close()`
    // -----------------------------------------------------------------------

    /// Translated from
    /// `MockProducerTest.shouldThrowOnInitTransactionIfProducerIsClosed` (Java 617).
    #[tokio::test]
    async fn should_throw_on_init_transaction_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.init_transactions().await, "MockProducer is already closed.");
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnBeginTransactionIfProducerIsClosed` (Java 631).
    #[tokio::test]
    async fn should_throw_on_begin_transaction_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.begin_transaction(), "MockProducer is already closed.");
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowSendOffsetsToTransactionByGroupIdIfProducerIsClosed`
    /// (Java 638). Java 638 and 645 have identical bodies — see the note on
    /// `should_throw_on_send_offsets_to_transaction_by_group_id_if_producer_got_fenced`.
    #[tokio::test]
    async fn should_throw_send_offsets_to_transaction_by_group_id_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(
            producer
                .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
                .await,
            "MockProducer is already closed.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowSendOffsetsToTransactionByGroupMetadataIfProducerIsClosed`
    /// (Java 645) — the identical twin of Java 638.
    #[tokio::test]
    async fn should_throw_send_offsets_to_transaction_by_group_metadata_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(
            producer
                .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
                .await,
            "MockProducer is already closed.",
        );
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnCommitTransactionIfProducerIsClosed` (Java 652).
    #[tokio::test]
    async fn should_throw_on_commit_transaction_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.commit_transaction().await, "MockProducer is already closed.");
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnAbortTransactionIfProducerIsClosed` (Java 659).
    #[tokio::test]
    async fn should_throw_on_abort_transaction_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.abort_transaction().await, "MockProducer is already closed.");
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnFenceProducerIfProducerIsClosed` (Java 666).
    #[tokio::test]
    async fn should_throw_on_fence_producer_if_producer_is_closed() {
        let producer = build_mock_producer(true);
        producer.close().await.unwrap();
        assert_illegal_state(producer.fence_producer(), "MockProducer is already closed.");
    }

    // -----------------------------------------------------------------------
    // Record staging: published on commit, dropped on abort
    // -----------------------------------------------------------------------

    /// Translated from
    /// `MockProducerTest.shouldPublishMessagesOnlyAfterCommitIfTransactionsAreEnabled`
    /// (Java 310).
    #[tokio::test]
    async fn should_publish_messages_only_after_commit_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        producer.send(record1()).await.unwrap();
        producer.send(record2()).await.unwrap();

        assert!(producer.history().is_empty());

        producer.commit_transaction().await.unwrap();

        assert_eq!(vec![record1(), record2()], producer.history());
    }

    /// Translated from
    /// `MockProducerTest.shouldFlushOnCommitForNonAutoCompleteIfTransactionsAreEnabled`
    /// (Java 330).
    #[tokio::test]
    async fn should_flush_on_commit_for_non_auto_complete_if_transactions_are_enabled() {
        let producer = build_mock_producer(false);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let md1 = producer.send(record1()).await.unwrap();
        let md2 = producer.send(record2()).await.unwrap();

        assert!(!md1.is_done());
        assert!(!md2.is_done());

        producer.commit_transaction().await.unwrap();

        assert!(md1.is_done());
        assert!(md2.is_done());
    }

    /// Translated from
    /// `MockProducerTest.shouldDropMessagesOnAbortIfTransactionsAreEnabled` (Java 348).
    #[tokio::test]
    async fn should_drop_messages_on_abort_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.send(record1()).await.unwrap();
        producer.send(record2()).await.unwrap();
        producer.abort_transaction().await.unwrap();
        assert!(producer.history().is_empty());

        producer.begin_transaction().unwrap();
        producer.commit_transaction().await.unwrap();
        assert!(producer.history().is_empty());
    }

    /// Translated from
    /// `MockProducerTest.shouldThrowOnAbortForNonAutoCompleteIfTransactionsAreEnabled`
    /// (Java 364).
    ///
    /// The Java name says "shouldThrow"; the body asserts the opposite — the abort
    /// succeeds and flushes the pending send. The name is kept as Java spells it
    /// (as with `shouldNotBeFlushedAfterFlush`) and the assertions follow the body.
    #[tokio::test]
    async fn should_throw_on_abort_for_non_auto_complete_if_transactions_are_enabled() {
        let producer = build_mock_producer(false);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let md1 = producer.send(record1()).await.unwrap();
        assert!(!md1.is_done());

        producer.abort_transaction().await.unwrap();
        assert!(md1.is_done());
    }

    /// Translated from
    /// `MockProducerTest.shouldPreserveCommittedMessagesOnAbortIfTransactionsAreEnabled`
    /// (Java 377).
    #[tokio::test]
    async fn should_preserve_committed_messages_on_abort_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.send(record1()).await.unwrap();
        producer.send(record2()).await.unwrap();
        producer.commit_transaction().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.abort_transaction().await.unwrap();

        assert_eq!(vec![record1(), record2()], producer.history());
    }

    // -----------------------------------------------------------------------
    // Consumer group offset staging
    // -----------------------------------------------------------------------

    /// Translated from
    /// `MockProducerTest.shouldPublishConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled`
    /// (Java 397).
    #[tokio::test]
    async fn should_publish_consumer_group_offsets_only_after_commit_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let group1 = "g1";
        let group1_commit = offsets(&[(0, 42), (1, 73)]);
        let group2 = "g2";
        let group2_commit = offsets(&[(0, 101), (1, 21)]);
        producer
            .send_offsets_to_transaction(group1_commit.clone(), group_metadata(group1))
            .await
            .unwrap();
        producer
            .send_offsets_to_transaction(group2_commit.clone(), group_metadata(group2))
            .await
            .unwrap();

        assert!(producer.consumer_group_offsets_history().is_empty());

        let expected: ConsumerGroupOffsets =
            HashMap::from([(group1.to_string(), group1_commit), (group2.to_string(), group2_commit)]);

        producer.commit_transaction().await.unwrap();
        assert_eq!(vec![expected], producer.consumer_group_offsets_history());
    }

    /// Translated from
    /// `MockProducerTest.shouldIgnoreEmptyOffsetsWhenSendOffsetsToTransactionByGroupMetadata`
    /// (Java 438).
    #[tokio::test]
    async fn should_ignore_empty_offsets_when_send_offsets_to_transaction_by_group_metadata() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
        producer
            .send_offsets_to_transaction(HashMap::new(), group_metadata("groupId"))
            .await
            .unwrap();
        assert!(!producer.sent_offsets());
    }

    /// Translated from
    /// `MockProducerTest.shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata`
    /// (Java 447).
    #[tokio::test]
    async fn should_add_offsets_when_send_offsets_to_transaction_by_group_metadata() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        assert!(!producer.sent_offsets());

        let group_commit = offsets(&[(0, 42)]);
        producer
            .send_offsets_to_transaction(group_commit, group_metadata("groupId"))
            .await
            .unwrap();
        assert!(producer.sent_offsets());
    }

    /// Translated from
    /// `MockProducerTest.shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction`
    /// (Java 464).
    #[tokio::test]
    async fn should_reset_sent_offsets_flag_only_when_beginning_new_transaction() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        assert!(!producer.sent_offsets());

        let group_commit = offsets(&[(0, 42)]);
        producer
            .send_offsets_to_transaction(group_commit.clone(), group_metadata("groupId"))
            .await
            .unwrap();
        producer.commit_transaction().await.unwrap(); // commit should not reset "sentOffsets"
        assert!(producer.sent_offsets());

        producer.begin_transaction().unwrap();
        assert!(!producer.sent_offsets());

        producer
            .send_offsets_to_transaction(group_commit, group_metadata("groupId"))
            .await
            .unwrap();
        producer.commit_transaction().await.unwrap(); // commit should not reset "sentOffsets"
        assert!(producer.sent_offsets());

        producer.begin_transaction().unwrap();
        assert!(!producer.sent_offsets());
    }

    /// Translated from
    /// `MockProducerTest.shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled`
    /// (Java 492).
    ///
    /// The two calls for the same group merge, and partition 1's offset moves from
    /// 73 to 101 — Java's `putAll` semantics (`MockProducer.java:199`), spelled as
    /// `HashMap::extend`.
    #[tokio::test]
    async fn should_publish_latest_and_cumulative_consumer_group_offsets_only_after_commit_if_transactions_are_enabled()
    {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let group = "g";
        let group_commit1 = offsets(&[(0, 42), (1, 73)]);
        let group_commit2 = offsets(&[(1, 101), (2, 21)]);
        producer
            .send_offsets_to_transaction(group_commit1, group_metadata(group))
            .await
            .unwrap();
        producer
            .send_offsets_to_transaction(group_commit2, group_metadata(group))
            .await
            .unwrap();

        assert!(producer.consumer_group_offsets_history().is_empty());

        let expected: ConsumerGroupOffsets =
            HashMap::from([(group.to_string(), offsets(&[(0, 42), (1, 101), (2, 21)]))]);

        producer.commit_transaction().await.unwrap();
        assert_eq!(vec![expected], producer.consumer_group_offsets_history());
    }

    /// Translated from
    /// `MockProducerTest.shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled`
    /// (Java 529).
    #[tokio::test]
    async fn should_drop_consumer_group_offsets_on_abort_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let group = "g";
        let group_commit = offsets(&[(0, 42), (1, 73)]);
        producer
            .send_offsets_to_transaction(group_commit.clone(), group_metadata(group))
            .await
            .unwrap();
        producer.abort_transaction().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.commit_transaction().await.unwrap();
        assert!(producer.consumer_group_offsets_history().is_empty());

        producer.begin_transaction().unwrap();
        producer
            .send_offsets_to_transaction(group_commit, group_metadata(group))
            .await
            .unwrap();
        producer.abort_transaction().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.commit_transaction().await.unwrap();
        assert!(producer.consumer_group_offsets_history().is_empty());
    }

    /// Translated from
    /// `MockProducerTest.shouldPreserveOffsetsFromCommitByGroupIdOnAbortIfTransactionsAreEnabled`
    /// (Java 558).
    #[tokio::test]
    async fn should_preserve_offsets_from_commit_by_group_id_on_abort_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let group = "g";
        let group_commit = offsets(&[(0, 42), (1, 73)]);
        producer
            .send_offsets_to_transaction(group_commit.clone(), group_metadata(group))
            .await
            .unwrap();
        producer.commit_transaction().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.abort_transaction().await.unwrap();

        let expected: ConsumerGroupOffsets = HashMap::from([(group.to_string(), group_commit)]);

        assert_eq!(vec![expected], producer.consumer_group_offsets_history());
    }

    /// Translated from
    /// `MockProducerTest.shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled`
    /// (Java 583).
    ///
    /// The load-bearing case for the `mem::take` in `commit_transaction`: the map
    /// published by the first commit must survive the second transaction's abort,
    /// which `clear()`s the (now separate) staging map.
    #[tokio::test]
    async fn should_preserve_offsets_from_commit_by_group_metadata_on_abort_if_transactions_are_enabled() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        let group = "g";
        let group_commit = offsets(&[(0, 42), (1, 73)]);
        producer
            .send_offsets_to_transaction(group_commit.clone(), group_metadata(group))
            .await
            .unwrap();
        producer.commit_transaction().await.unwrap();

        producer.begin_transaction().unwrap();

        let group2 = "g2";
        let group_commit2 = offsets(&[(2, 53), (3, 84)]);
        producer
            .send_offsets_to_transaction(group_commit2, group_metadata(group2))
            .await
            .unwrap();
        producer.abort_transaction().await.unwrap();

        let expected: ConsumerGroupOffsets = HashMap::from([(group.to_string(), group_commit)]);

        assert_eq!(vec![expected], producer.consumer_group_offsets_history());
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
        let producer = build_mock_producer(true);
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
        let producer = build_mock_producer(true);
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
        let producer = build_mock_producer(true);
        producer.set_send_error(Some(Error::new(Errors::CorruptMessage)));

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
        let producer = build_mock_producer(true);
        producer.set_flush_error(Some(Error::new(Errors::CorruptMessage)));

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
        let producer = build_mock_producer(true);
        producer.set_partitions_for_error(Some(Error::new(Errors::UnknownTopicOrPartition)));

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
        let producer = build_mock_producer(true);
        producer.set_close_error(Some(Error::new(Errors::UnknownServerError)));

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
        let producer = build_mock_producer(true);
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
        let producer = build_mock_producer(true);
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
        let producer = build_mock_producer(true);
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
        let producer = build_mock_producer(false);
        assert!(!producer.error_next(Error::new(Errors::UnknownServerError)));
    }

    /// `metrics()` returns the mock metrics seeded via `set_mock_metrics`,
    /// mirroring Java `MockProducer.setMockMetrics` + `metrics()`. Java
    /// `MockProducerTest` has no metrics test; this covers the Rust surface.
    #[test]
    fn test_set_and_get_mock_metrics() {
        use crate::common::metrics::Metrics;
        use crate::common::metrics::stats::CumulativeSum;

        let producer: MockProducer<String, String> = MockProducer::default();
        assert!(producer.metrics().is_empty());

        // Build a real KafkaMetric via a Metrics registry.
        let registry = Metrics::new();
        let sensor = registry.sensor("mock-sensor").unwrap();
        let name = registry.metric_name("mock-metric", "mock-group");
        sensor.add_metric_name(name.clone(), Box::new(CumulativeSum::new())).unwrap();
        let metric = registry.metric(&name).unwrap();

        producer.set_mock_metrics(name.clone(), Arc::clone(&metric));

        let snapshot = producer.metrics();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains_key(&name));
    }

    /// Tests `uncommitted_records` and `uncommitted_offsets`, the two staging
    /// accessors `MockProducerTest` never calls — `uncommittedRecords`
    /// (`MockProducer.java:471`) and `uncommittedOffsets` (`:483`) appear zero
    /// times in `MockProducerTest.java`; their Java callers are Kafka Streams
    /// tests, out of scope. Covered here so the accessors are not untested public
    /// API.
    #[tokio::test]
    async fn test_uncommitted_accessors() {
        let producer = build_mock_producer(true);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();

        assert!(producer.uncommitted_records().is_empty());
        assert!(producer.uncommitted_offsets().is_empty());

        producer.send(record1()).await.unwrap();
        let group_commit = offsets(&[(0, 42)]);
        producer
            .send_offsets_to_transaction(group_commit.clone(), group_metadata(GROUP_ID))
            .await
            .unwrap();

        assert_eq!(vec![record1()], producer.uncommitted_records());
        assert_eq!(
            HashMap::from([(GROUP_ID.to_string(), group_commit)]) as ConsumerGroupOffsets,
            producer.uncommitted_offsets()
        );

        // Both are drained by the commit that publishes them.
        producer.commit_transaction().await.unwrap();
        assert!(producer.uncommitted_records().is_empty());
        assert!(producer.uncommitted_offsets().is_empty());
    }

    /// Tests that `clear` empties the four collections and the flag Java's
    /// `clear()` resets beyond `sent` (`MockProducer.java:490-497`), and that it
    /// leaves the transaction flags alone — Java resets `sentOffsets` there but
    /// not `transactionInitialized` / `transactionInFlight`.
    #[tokio::test]
    async fn test_clear_resets_staging_but_not_transaction_flags() {
        let producer = build_mock_producer(false);
        producer.init_transactions().await.unwrap();
        producer.begin_transaction().unwrap();
        producer.send(record1()).await.unwrap();
        producer
            .send_offsets_to_transaction(offsets(&[(0, 42)]), group_metadata(GROUP_ID))
            .await
            .unwrap();
        producer.commit_transaction().await.unwrap();

        producer.begin_transaction().unwrap();
        producer.send(record2()).await.unwrap();
        producer
            .send_offsets_to_transaction(offsets(&[(1, 73)]), group_metadata(GROUP_ID))
            .await
            .unwrap();

        assert!(!producer.history().is_empty());
        assert!(!producer.uncommitted_records().is_empty());
        assert!(!producer.consumer_group_offsets_history().is_empty());
        assert!(!producer.uncommitted_offsets().is_empty());
        assert!(producer.sent_offsets());
        assert!(!producer.flushed());

        producer.clear();

        assert!(producer.history().is_empty());
        assert!(producer.uncommitted_records().is_empty());
        assert!(producer.consumer_group_offsets_history().is_empty());
        assert!(producer.uncommitted_offsets().is_empty());
        assert!(!producer.sent_offsets());
        assert!(producer.flushed());

        // Untouched by `clear()`, so the open transaction can still be committed.
        assert!(producer.transaction_initialized());
        assert!(producer.transaction_in_flight());
        assert_eq!(1, producer.commit_count());
        producer.commit_transaction().await.unwrap();
    }

    /// Tests the five transactional error knobs, Java's public
    /// `initTransactionException` … `abortTransactionException`
    /// (`MockProducer.java:79-83`). Like `uncommittedRecords`, no
    /// `MockProducerTest` method touches them; the four non-transactional knobs
    /// above are covered the same way.
    ///
    /// Each is checked at Java's position in its method: after the `verify*`
    /// guards, so it does not mask them, and — for `beginTransaction` — before the
    /// in-flight check (`MockProducer.java:167-173`).
    #[tokio::test]
    async fn test_set_transactional_errors() {
        let injected = || Error::new(Errors::CoordinatorNotAvailable);

        // `auto_complete = false` so a pending send stays pending: that is what
        // shows the commit / abort errors firing *ahead* of Java's `flush()`
        // (`MockProducer.java:210-214`, `:236-240`).
        let producer = build_mock_producer(false);

        // init: installed before the first `init_transactions`, so it fires there.
        producer.set_init_transaction_error(Some(injected()));
        assert_eq!(
            Errors::CoordinatorNotAvailable,
            producer.init_transactions().await.unwrap_err().error()
        );
        // The failed init left the producer uninitialized.
        assert!(!producer.transaction_initialized());
        producer.set_init_transaction_error(None);
        producer.init_transactions().await.unwrap();

        // begin: fires ahead of the "Transaction already started" check.
        producer.set_begin_transaction_error(Some(injected()));
        assert_eq!(
            Errors::CoordinatorNotAvailable,
            producer.begin_transaction().unwrap_err().error()
        );
        assert!(!producer.transaction_in_flight());
        producer.set_begin_transaction_error(None);
        producer.begin_transaction().unwrap();

        // send_offsets: fires ahead of the empty-map short circuit, so even an
        // empty map surfaces it.
        producer.set_send_offsets_to_transaction_error(Some(injected()));
        let error = producer
            .send_offsets_to_transaction(HashMap::new(), group_metadata(GROUP_ID))
            .await
            .unwrap_err();
        assert_eq!(Errors::CoordinatorNotAvailable, error.error());
        producer.set_send_offsets_to_transaction_error(None);

        // commit: fires ahead of the flush, so the pending send stays pending.
        producer.send(record1()).await.unwrap();
        producer.set_commit_transaction_error(Some(injected()));
        assert_eq!(
            Errors::CoordinatorNotAvailable,
            producer.commit_transaction().await.unwrap_err().error()
        );
        assert!(!producer.flushed(), "a failed commit must not have flushed");
        assert_eq!(0, producer.commit_count());
        producer.set_commit_transaction_error(None);

        // abort: same, and the transaction is still in flight afterwards.
        producer.set_abort_transaction_error(Some(injected()));
        assert_eq!(
            Errors::CoordinatorNotAvailable,
            producer.abort_transaction().await.unwrap_err().error()
        );
        assert!(!producer.flushed(), "a failed abort must not have flushed");
        assert!(producer.transaction_in_flight());
        producer.set_abort_transaction_error(None);

        producer.commit_transaction().await.unwrap();
        assert_eq!(1, producer.commit_count());
        assert_eq!(vec![record1()], producer.history());
    }

    // =====================================================================
    // PHASE-7 TEST ACCOUNTING (`definition-of-done.md` §3)
    //
    // `MockProducerTest.java` declares **55** `@Test` methods. Every one is
    // placed in exactly one group: **53** TRANSLATED, **2** NOT APPLICABLE. The
    // split is derived, not asserted, and the derivation is below with its real
    // output. Java citations are the **declaration** line, never the `@Test`
    // line (Critic 46 filed the inverse).
    //
    // Two extraction guards, each for a failure that has already happened
    // somewhere in this milestone:
    //
    //   1. The `@Test`-to-declaration walk skips *intervening annotations*.
    //      `shouldThrowClassCastException` carries a second one
    //      (`@SuppressWarnings("unchecked")`, Java 688), and without the skip the
    //      walk yields `SuppressWarnings` for it — the count stays 55 while a
    //      name is silently wrong, which a count-only guard cannot see. Check it
    //      by deleting the `w && /^    @/{next}` clause: the only row that
    //      changes is 688 `SuppressWarnings`.
    //   2. TRANSLATED is detected on **three**-slash lines only, so the two NOT
    //      APPLICABLE entries below — `//` lines that necessarily spell a real
    //      prefixed name — cannot also score as translated. Relax the anchor to
    //      `^ *//` and those two rows score in both columns, which the
    //      "neither, or both" check then reports:
    //        while IFS=$'\t' read -r ln nm; do
    //          t3=$(grep -c "^ *///.*\`MockProducerTest\.$nm\`" $R || true)
    //          t2=$(grep -c "^ *//.*\`MockProducerTest\.$nm\`"  $R || true)
    //          [ "$t3" != "$t2" ] && echo "CHANGES $nm $t3 $t2"
    //        done < $W.java.tsv
    //      prints exactly those two names, and nothing else. This is the Phase-6
    //      lesson (a paragraph documenting an escape shape is an adversarial input
    //      for a checker in the same file) handled before the fact: the block's
    //      prose and its own derivation text are invisible to it, because the only
    //      prefixed names they contain are the unexpanded `$nm` literal and these
    //      two markers.
    //
    // The closing backtick in the pattern is **not** load-bearing today, and the
    // honest form of that claim is worth writing down rather than the tempting
    // one: it would matter only if some `@Test` name were a strict prefix of
    // another, and none is —
    //   python3 -c "
    //   names=[l.split(chr(9))[1].strip() for l in open('$W.java.tsv')]
    //   print([(a,b) for a in names for b in names if a!=b and b.startswith(a)])"
    // prints `[]`, and dropping the backtick changes no row's count. It is kept as
    // defence against a future name that does nest, since the `MockProducerTest.`
    // prefix alone would not stop it.
    //
    //   J=kafka/clients/src/test/java/org/apache/kafka/clients/producer/MockProducerTest.java
    //   R=src/producer/mock_producer.rs
    //   W=/tmp/mp
    //
    //   # (1) the 55, as (declaration line, name)
    //   awk '/^    @Test$/{w=1;next} w && /^    @/{next}
    //        w{match($0,/[a-zA-Z_][A-Za-z0-9_]*\(/)
    //          printf "%s\t%s\n", NR, substr($0,RSTART,RLENGTH-1); w=0}' $J > $W.java.tsv
    //   echo "rows=$(wc -l < $W.java.tsv | tr -d ' ') atTest=$(grep -c '^    @Test$' $J)"
    //
    //   # (2) status of each. `|| true` because `grep -c` exits 1 on a zero
    //   # count, which would abandon the loop at the first unplaced name — i.e.
    //   # exactly when the check matters most.
    //   while IFS=$'\t' read -r ln nm; do
    //     t=$(grep -c "^ *///.*\`MockProducerTest\.$nm\`" $R || true)
    //     n=$(grep -c "^    //   NOT APPLICABLE — \`MockProducerTest\.$nm\`" $R || true)
    //     printf "%s\t%s\t%s\t%s\n" "$ln" "$nm" "$t" "$n"
    //   done < $W.java.tsv > $W.status.tsv
    //   echo "TRANSLATED=$(awk -F'\t' '$3==1 && $4==0' $W.status.tsv | wc -l | tr -d ' ')"
    //   echo "NOT_APPLICABLE=$(awk -F'\t' '$3==0 && $4==1' $W.status.tsv | wc -l | tr -d ' ')"
    //   echo '--- neither, or both ---'
    //   awk -F'\t' '!(($3==1&&$4==0)||($3==0&&$4==1))' $W.status.tsv
    //
    //   # (3) the reverse direction: no rustdoc claims a name that is not a
    //   # `@Test` method. This is what catches a typo in a header, which would
    //   # otherwise surface as an unplaced Java method and get mis-explained.
    //   # The five excluded are Java's own fixture *fields* plus the file itself,
    //   # which the fixture rustdoc cites in the same prefixed form.
    //   grep -o '^ *///.*`MockProducerTest\.[A-Za-z0-9_]*`' $R \
    //   | grep -o 'MockProducerTest\.[A-Za-z0-9_]*' | sed 's/.*\.//' | sort -u \
    //   | grep -vxE 'topic|groupId|record1|record2|java' > $W.claimed.txt
    //   comm -23 $W.claimed.txt <(cut -f2 $W.java.tsv | sort -u)
    //
    // Real output, run from the repo root: `rows=55 atTest=55`, then
    // `TRANSLATED=53`, `NOT_APPLICABLE=2`, then (2)'s last command prints
    // nothing, and (3) prints nothing. 53 + 2 = 55, so every method is placed
    // exactly once and nothing is claimed that Java does not declare.
    //
    // NOT APPLICABLE (2). Neither is blocked on unwritten Rust: each tests a
    // Java-language property that has no Rust counterpart, so there is nothing to
    // implement and nothing to defer.
    //
    //   NOT APPLICABLE — `MockProducerTest.shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction`
    //     (Java 430). Despite the name, the `NullPointerException` it asserts does
    //     not come from `sendOffsetsToTransaction`: it comes from evaluating
    //     `new ConsumerGroupMetadata(null)` in the lambda — the one-arg constructor
    //     at `ConsumerGroupMetadata.java:52`, delegating to the four-arg one
    //     declared at `:38`, whose first statement is
    //     `Objects.requireNonNull(groupId, "group.id can't be null")` at `:42` —
    //     before the mock is entered at all. With `Collections.emptyMap()` for the offsets
    //     there is no other reachable throw — `sendOffsetsToTransaction` would
    //     return at `MockProducer.java:195`. Rust's
    //     `ConsumerGroupMetadata::new(impl Into<String>)` cannot receive null, and
    //     `send_offsets_to_transaction` takes the metadata by value rather than as
    //     an `Option`, so Java's own `Objects.requireNonNull(groupMetadata)`
    //     (`MockProducer.java:184`) is equally unrepresentable. The empty-offsets
    //     path it incidentally exercises is covered by
    //     `should_ignore_empty_offsets_when_send_offsets_to_transaction_by_group_metadata`
    //     (Java 438).
    //   NOT APPLICABLE — `MockProducerTest.shouldThrowClassCastException`
    //     (Java 689). Asserts that Java's type erasure lets a raw
    //     `ProducerRecord` carry a `String` key into an `IntegerSerializer`,
    //     producing `ClassCastException` inside `send`. Rust has no type erasure —
    //     `MockProducer<i32, String>` will not accept a `ProducerRecord<String,
    //     String>` at compile time — and the mock holds no serializers to
    //     mis-apply (see the method accounting block on `partition`, Java 526).
    //     Justification carried over from before Phase 7, re-checked and still
    //     accurate.
    //
    // What the derivation cannot see, and where each is pinned instead:
    //
    //   - A test present but weakened. `test_partitioner` (Java 86) is the one
    //     such case remaining: Java drives a `RoundRobinPartitioner` and asserts it
    //     picks partition 0, while the Rust mock has no partitioner, so the
    //     translation asserts an explicit `record.partition()` is honoured instead.
    //     Its own rustdoc says so.
    //
    //     **Four** others were weakened and are no longer. Naming all four matters
    //     more than the number: the two artifacts that first recorded this each
    //     said "three" and each named a *different* three, because each was written
    //     beside the commit that made its own subset visible. A count-level check
    //     finds nothing wrong with two lists that agree on `3`; only membership
    //     does. So the set is re-derived from the diff rather than from either
    //     list — normalise away this phase's two mechanical swaps
    //     (`MockProducer::with_auto_complete(x)` → `build_mock_producer(x)`, and
    //     `make_record("topic", "keyN", "valueN")` → `recordN()`), then compare
    //     every pre-Phase-7 test body at `82aa2da` against its current form. Six
    //     bodies differ before that normalisation and exactly these four after:
    //
    //       `testManualCompletion` (107) — Java compares the cause at 122; the
    //         translation asserted a bare `is_err()`. Now `assert_eq!("blah", ..)`.
    //       `testMetadataOnException` (724) — asserted only that the future failed,
    //         dropping all four values Java checks at 727-731. Now asserts them.
    //       `shouldThrowOnSendIfProducerIsClosed` (624) and
    //         `shouldThrowOnFlushProducerIfProducerIsClosed` (673) — matched their
    //         message with `contains`. Now `assert_illegal_state`, i.e. variant plus
    //         exact message.
    //   - A test that passes with the bug reintroduced. Twelve mutations of the
    //     Phase-7 surface were each applied and each failed the suite; the commit
    //     that landed them lists them.
    //   - Whether the *production* surface is complete. That is the method
    //     accounting block's job (`definition-of-done.md` §2), above the struct.
    //
    // The derivation classifies *Java* methods, so no Rust-only test appears in it
    // at all. This module has 69 tests against the 53 translated, i.e. 16 with no
    // Java counterpart, and 13 of those predate Phase 7. The three it adds are
    // called out here because they are the only ones discharging a
    // `definition-of-done.md` §2 obligation rather than adding local coverage:
    // `test_uncommitted_accessors`,
    // `test_clear_resets_staging_but_not_transaction_flags` and
    // `test_set_transactional_errors` cover `uncommittedRecords` (Java 471),
    // `uncommittedOffsets` (483) and the five transactional `*Exception` fields
    // (79-83) — each of which appears **zero** times in `MockProducerTest.java`,
    // its Java callers being Kafka Streams tests, out of scope. They sit with the
    // other Rust-only additions under "Additional unit tests".
    //
    //   cargo test --lib producer::mock_producer   # 69 passed
    // =====================================================================
}
