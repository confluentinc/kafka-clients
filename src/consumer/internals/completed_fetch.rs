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

//! `CompletedFetch` — per-partition batch state and record iteration.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.CompletedFetch`.
//!
//! **THIS IS THE ZERO-COPY HOT PATH** per `consumer-threading.md` §27.
//!
//! Per-record allocation budget (expected, asserted in Phase 7b's
//! `FetchCollector` allocation test): only the user-supplied
//! `Deserializer<T>` allocations for the key and value `T`, plus the
//! `RecordHeaders` clone (which §27 explicitly allows in Milestone-8).
//! Specifically preserved here:
//!
//! - `partition_data.records: Option<Vec<u8>>` arrives owning the fetch
//!   payload. We never call `.clone()` or `Bytes::copy_from_slice` on it; on
//!   first batch access the buffer is *moved* (not copied) into the cursor's
//!   `MemoryRecords`, which then becomes the single canonical owner of the
//!   record bytes (Java's `recordsOrFail` is the analog).
//! - `topic_arc: Arc<str>` is allocated ONCE per `CompletedFetch` from
//!   `partition.topic()` and cloned cheaply per `ConsumerRecord` — no
//!   `String::from_utf8` or `Arc::from(&str)` per record.
//! - Per-record reads go through `peek_current_record`, which parses a
//!   borrowing `DefaultRecordRef` directly out of the batch's record bytes
//!   on demand — the key and value bytes are `&[u8]` slices into the fetch
//!   buffer (or, for compressed batches, into the cursor's once-per-batch
//!   decompression buffer). We never copy key/value bytes out of the buffer;
//!   the only deep copy of record payload is whatever the user's
//!   `Deserializer<T>` does internally.
//! - Headers are owned per the milestone-8 §27 ruling (`RecordHeaders` built
//!   from `DefaultRecordRef::headers()`, the single owned-copy point); a
//!   future revisit may borrow.
//! - No per-record `tokio::spawn`. The whole struct is sync.
//! - Iteration is lazy via a `BatchCursor`: records are decoded one at a
//!   time, on demand, by walking the current batch's record bytes with a
//!   byte cursor. We never materialize a `Vec<DefaultRecord>` of owned
//!   copies — neither across batches nor within a batch. Compressed batches
//!   are decompressed once into an owned buffer held by the cursor; records
//!   then borrow from it (no re-decompression and no per-record copy).
//!
//! # READ_COMMITTED
//!
//! Fully translated as of Milestone 11 Phase 8. `containsAbortMarker`
//! (`CompletedFetch.java:352-359`) is [`CompletedFetch::contains_abort_marker`],
//! which parses the first record of a control batch through
//! [`ControlRecordType`], and an ABORT marker drops the producer id from
//! `aborted_producer_ids` before `isBatchAborted` is consulted — Java's order
//! at `:210-218`.
//!
//! Phase 7a had deferred this, returning `Error::unsupported_version` on
//! any control batch from an already-aborted producer id, and recorded the gap
//! as low-impact: "production readers will hit it only if their producers reuse
//! producer IDs after an abort, which is rare". **That assessment was wrong,
//! and the Phase-8 broker integration test
//! `test_aborted_transaction_records_are_discarded` is what falsified it.**
//!
//! The true trigger is narrower to state and far wider in effect: **any
//! `read_committed` fetch that reached an ABORT marker at all.** The removed
//! guard sat *after* `consume_aborted_transactions_up_to`, and the ABORT marker
//! batch is itself a control batch carrying the aborted transaction's own
//! producer id — which that call has just inserted, since the response's
//! `AbortedTransaction.first_offset` is ≤ the marker's `last_offset` by
//! construction. So the bail fired on the marker of the very transaction just
//! skipped, in the same fetch. No producer-id reuse and no later commit were
//! needed: a single aborted transaction with nothing after it was enough, as was
//! an empty aborted transaction whose marker is its only batch. `read_committed`
//! was unusable on any partition that had ever had an abort.
//!
//! Producer-id stability is the rebuttal of Phase 7a's *stated premise* — a
//! producer id is allocated once per incarnation and is stable across that
//! producer's transactions, so "reuse" is what every transactional producer does
//! — but it is not the description of the trigger, and an earlier revision of
//! this comment let it stand as one. The lesson is about the shape of the claim
//! rather than the branch: "rare" was asserted about a *client* behaviour
//! without checking what the client actually does.

#![allow(dead_code)]

use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use log::{debug, error};
use rustc_hash::FxHashSet;

use crate::common::Error;
use crate::common::InvalidRecordError;
use crate::common::IsolationLevel;
use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::errors::DeserializationErrorOrigin;
use crate::common::errors::RecordDeserializationError;
use crate::common::header::internals::RecordHeaders;
use crate::common::memory::buffer_supplier::BufferSupplier;
use crate::common::protocol::Errors;
use crate::common::record::TimestampType;
use crate::common::record::internal::abstract_records::LOG_OVERHEAD;
use crate::common::record::internal::{
    ControlRecordType, DefaultRecord, DefaultRecordBatchRef, DefaultRecordRef, MemoryRecords, RecordBatch,
    RecordVersion,
};
use crate::common::serialization::Deserializer;
use crate::consumer::internals::fetch_config::FetchConfig;
use crate::consumer::internals::fetch_metrics_aggregator::FetchMetricsAggregator;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::{ConsumerRecord, ConsumerRecordOptionsBuilder};
use crate::fetch_response_data::{AbortedTransaction, PartitionData};

/// Sentinel value: a partition leader epoch that is unknown / unset.
const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// A min-heap wrapper that orders `AbortedTransaction`s by `first_offset`
/// ascending — matches Java's `PriorityQueue` with `Comparator.comparingLong`.
#[derive(Clone, Debug)]
struct AbortedTxnByFirstOffset(AbortedTransaction);

impl PartialEq for AbortedTxnByFirstOffset {
    fn eq(&self, other: &Self) -> bool {
        self.0.first_offset == other.0.first_offset
    }
}
impl Eq for AbortedTxnByFirstOffset {}
impl Ord for AbortedTxnByFirstOffset {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse so BinaryHeap (max-heap) yields smallest first_offset first.
        other.0.first_offset.cmp(&self.0.first_offset)
    }
}
impl PartialOrd for AbortedTxnByFirstOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Snapshot of the metadata of the current batch the cursor is on. Captures
/// the fields the per-record path needs after the iter_records borrow is
/// dropped.
#[derive(Clone, Debug)]
struct BatchMetadata {
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    /// The batch's log-append timestamp (max timestamp), used when
    /// `timestamp_type == LogAppendTime` to override per-record timestamps.
    last_offset_timestamp: i64,
    last_offset: i64,
    next_offset: i64,
    timestamp_type: TimestampType,
    partition_leader_epoch: i32,
    is_control_batch: bool,
    is_transactional: bool,
    has_producer_id: bool,
    producer_id: i64,
    magic: i8,
}

/// A batch of records returned for a single partition by a fetch request.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.CompletedFetch`.
pub(crate) struct CompletedFetch {
    /// The partition this batch belongs to.
    pub(crate) partition: TopicPartition,
    /// Topic name as `Arc<str>` — allocated once at construction time
    /// from `partition.topic()` and cloned cheaply (atomic pointer bump)
    /// per emitted `ConsumerRecord`. Mirrors `consumer-threading.md` §27.
    topic_arc: Arc<str>,
    /// Raw response data — owns the byte buffer. Borrowed by the cursor.
    pub(crate) partition_data: PartitionData,

    /// Subscription state used for `move_partition_to_end` on drain.
    subscriptions: Option<Arc<Mutex<SubscriptionState>>>,
    /// Decompression buffer pool (used by compressed-batch iteration in a
    /// future revision — current iteration uses the existing
    /// `DefaultRecordBatch::iter_records` path which handles decompression
    /// internally without a `BufferSupplier`; the field is retained for
    /// Java parity and forward compatibility).
    decompression_buffer_supplier: Option<Arc<BufferSupplier>>,

    /// In-progress batch iteration state. Lazily initialized on first
    /// `fetch_records` call so that empty fetches incur no setup cost.
    cursor: Option<BatchCursor>,

    /// Per-batch READ_COMMITTED state.
    ///
    /// FxHash (non-cryptographic) keyed by the internal producer id (`i64`);
    /// checked per record on the abort path. The keys are not
    /// attacker-controlled, so SipHash buys nothing here (Phase 25).
    aborted_producer_ids: FxHashSet<i64>,
    aborted_transactions: BinaryHeap<AbortedTxnByFirstOffset>,

    /// Cached deserialization exception for retry semantics. Java
    /// re-raises on every call until the user seeks past the offset.
    cached_record_error: Option<Error>,
    corrupt_last_record: bool,

    /// Stats. `drain()` reports these to the per-response
    /// [`FetchMetricsAggregator`] (Java `recordAggregatedMetrics`) — once per
    /// partition, NEVER per record. The per-record loop only increments these
    /// `i32`s (no `Sensor.record`, no alloc). `drain()` also consults
    /// `bytes_read` to decide whether to nudge `move_partition_to_end`.
    records_read: i32,
    bytes_read: i32,

    /// Per-response metric aggregator shared across this fetch's partitions.
    /// `None` for the lightweight test / [`FetchBuffer`] constructor that has no
    /// metrics wiring; `drain()` records the partition's totals exactly once.
    metric_aggregator: Option<Arc<FetchMetricsAggregator>>,

    /// Offset the next fetch should start at.
    next_fetch_offset: i64,
    /// Last partition-leader epoch we observed (used by Phase 7b's
    /// FetchCollector to update SubscriptionState).
    last_epoch: Option<i32>,
    /// Whether `drain` has been called.
    is_consumed: bool,
    /// Whether the cursor has been positioned at the first batch.
    initialized: bool,

    /// DIAGNOSTIC (not in Java): instant this `CompletedFetch` was constructed
    /// on the background task (when the fetch response was received). Logged
    /// under the `fetch_diag` target when the app first touches this fetch in
    /// `FetchCollector::initialize`, to measure the bg-receipt -> app-delivery
    /// handoff (FetchBuffer drain depth). `None` when `fetch_diag` is disabled.
    pub(crate) created_at: Option<std::time::Instant>,
}

/// Cursor through the batches and records inside a [`CompletedFetch`].
///
/// Owns the `MemoryRecords` *moved* out of the partition's `records` buffer —
/// there is no copy of the fetch payload (§27 "one buffer"). That buffer is
/// the single canonical owner of the record bytes; individual records are
/// parsed as borrowing [`DefaultRecordRef`]s pointing into it, and batch
/// headers are parsed in place via [`DefaultRecordBatchRef`] — no per-record
/// and no per-batch copy.
#[derive(Debug)]
struct BatchCursor {
    /// Records buffer moved out of `partition_data.records` exactly once
    /// per `CompletedFetch` (no clone).
    memory_records: MemoryRecords,
    /// Absolute byte offset of the next batch header in
    /// `memory_records.buffer()`. Advanced incrementally as each batch is
    /// consumed (NOT recomputed from 0), so locating the next batch is O(1).
    /// `None` means iteration has terminated.
    next_batch_start: Option<usize>,
    /// Metadata of the batch we're currently iterating; `None` before the
    /// first batch is loaded.
    current_batch: Option<BatchMetadata>,
    /// Where the current batch's record bytes live, and how to decode the
    /// next record. The key/value/header bytes of each record are borrowed
    /// from this source — never copied.
    record_source: RecordSource,
    /// Byte offset of the next record to decode, relative to the start of
    /// the current batch's (decompressed) record section.
    record_byte_offset: usize,
    /// Number of records left to decode in the current batch.
    records_remaining: i32,
}

/// Where the current batch's record bytes come from. Uncompressed batches
/// borrow their bytes directly from the cursor's `MemoryRecords` buffer (we
/// remember the byte range so we don't re-walk the batch iterator per
/// record). Compressed batches are decompressed once into an owned buffer.
#[derive(Debug)]
enum RecordSource {
    /// No batch loaded yet (or iteration finished).
    None,
    /// Uncompressed: record bytes are `memory_records.buffer()[range]`.
    Borrowed(std::ops::Range<usize>),
    /// Compressed: record bytes are the owned decompressed buffer, held as a
    /// refcounted [`bytes::Bytes`] so per-record key/value slices can be handed
    /// out zero-copy via [`bytes::Bytes::slice_ref`] (§27).
    Owned(bytes::Bytes),
}

impl CompletedFetch {
    /// Constructs a `CompletedFetch` for the given partition.
    ///
    /// Translates Java's
    /// `CompletedFetch(Logger, SubscriptionState, BufferSupplier,
    ///   TopicPartition, PartitionData, FetchMetricsAggregator, Long)` —
    /// minus the logger (we use the `log` crate). Phase M3 plumbs the
    /// `FetchMetricsAggregator` (dropped by Phase 7a).
    pub(crate) fn new_full(
        subscriptions: Arc<Mutex<SubscriptionState>>,
        decompression_buffer_supplier: Arc<BufferSupplier>,
        partition: TopicPartition,
        partition_data: PartitionData,
        metric_aggregator: Arc<FetchMetricsAggregator>,
        fetch_offset: i64,
    ) -> Self {
        let aborted_transactions = build_aborted_transactions(&partition_data);
        let topic_arc: Arc<str> = Arc::from(partition.topic());
        Self {
            partition,
            topic_arc,
            partition_data,
            subscriptions: Some(subscriptions),
            decompression_buffer_supplier: Some(decompression_buffer_supplier),
            cursor: None,
            aborted_producer_ids: FxHashSet::default(),
            aborted_transactions,
            cached_record_error: None,
            corrupt_last_record: false,
            records_read: 0,
            bytes_read: 0,
            metric_aggregator: Some(metric_aggregator),
            next_fetch_offset: fetch_offset,
            last_epoch: None,
            is_consumed: false,
            initialized: false,
            // DIAGNOSTIC: stamp construction time only when fetch_diag is on.
            created_at: log::log_enabled!(target: "fetch_diag", log::Level::Info).then(std::time::Instant::now),
        }
    }

    /// Lightweight constructor used by tests / [`FetchBuffer`] when the
    /// subscription state and buffer supplier are not yet wired.
    pub(crate) fn new(partition: TopicPartition, partition_data: PartitionData) -> Self {
        let aborted_transactions = build_aborted_transactions(&partition_data);
        let topic_arc: Arc<str> = Arc::from(partition.topic());
        Self {
            partition,
            topic_arc,
            partition_data,
            subscriptions: None,
            decompression_buffer_supplier: None,
            cursor: None,
            aborted_producer_ids: FxHashSet::default(),
            aborted_transactions,
            cached_record_error: None,
            corrupt_last_record: false,
            records_read: 0,
            bytes_read: 0,
            metric_aggregator: None,
            next_fetch_offset: 0,
            last_epoch: None,
            is_consumed: false,
            initialized: false,
            created_at: None,
        }
    }

    /// Returns the offset the next fetch round should start at.
    pub(crate) fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
    }

    /// The per-response metric aggregator, if this fetch has one.
    ///
    /// Lets `FetchCollector::initialize`'s finally record a zero contribution for a
    /// fetch it is about to discard — Java's
    /// `completedFetch.recordAggregatedMetrics(0, 0)`
    /// (`FetchCollector.java:239-241`). A discarded fetch never reaches `drain`, so
    /// without it the aggregator never hears about that partition at all.
    pub(crate) fn metric_aggregator(&self) -> Option<Arc<FetchMetricsAggregator>> {
        self.metric_aggregator.clone()
    }

    /// Returns the most recent partition-leader epoch observed in a batch.
    pub(crate) fn last_epoch(&self) -> Option<i32> {
        self.last_epoch
    }

    /// Returns whether this fetch has been initialized (i.e. the cursor
    /// has been positioned at the first batch).
    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Marks this fetch as initialized. Called by Phase 7b's
    /// `FetchCollector` after position validation.
    pub(crate) fn set_initialized(&mut self) {
        self.initialized = true;
    }

    /// Returns whether the fetch has been fully consumed (or drained).
    pub(crate) fn is_consumed(&self) -> bool {
        self.is_consumed
    }

    /// Drops iteration state and marks the fetch consumed. Idempotent.
    pub(crate) fn drain(&mut self) {
        if self.is_consumed {
            return;
        }
        self.cursor = None;
        self.cached_record_error = None;
        self.is_consumed = true;
        // Report this partition's totals to the per-response aggregator
        // exactly once (Java `recordAggregatedMetrics`). The aggregator writes
        // the fetch-level / per-topic sensors once every partition has drained.
        if let Some(aggregator) = &self.metric_aggregator {
            aggregator.record(&self.partition, self.bytes_read, self.records_read);
        }
        if self.bytes_read > 0
            && let Some(subscriptions) = &self.subscriptions
        {
            // Nudge SubscriptionState to keep partitions of the same
            // topic adjacent (improves wire-protocol serialization
            // locality).
            let mut guard = subscriptions.lock().expect("SubscriptionState mutex poisoned");
            guard.move_partition_to_end(&self.partition);
        }
    }

    /// Lazily initializes the batch cursor on first call.
    ///
    /// §27 zero-copy: we *move* the partition's `records` buffer out of
    /// `partition_data` into the cursor's `MemoryRecords` — there is NO copy.
    /// The fetch payload arrives owned by exactly one buffer; that single
    /// buffer becomes the cursor's `MemoryRecords` and is the canonical owner
    /// of the record bytes that every per-record [`DefaultRecordRef`] borrows
    /// from. Initialization is deferred until the first `fetch_records` call so
    /// empty fetches incur no setup cost.
    ///
    /// All readers of `partition_data.records` (notably
    /// `FetchCollector::initialize`, which snapshots the records size before
    /// any record is decoded) run strictly before this point, so taking
    /// ownership here is safe; a subsequent read would observe `None`
    /// (treated as an empty records buffer).
    fn ensure_cursor(&mut self) {
        if self.cursor.is_some() {
            return;
        }
        // §27: single move (no clone, no per-record copy). Bounded by
        // partition size and runs at most once.
        // `partition_data.records` is already a refcounted `bytes::Bytes`
        // sliced from the FetchResponse payload (BytesReader, §27); moving it
        // into `MemoryRecords` is an O(1) refcount move, no copy.
        let records_buffer = self.partition_data.records.take().unwrap_or_default();
        let memory_records = MemoryRecords::new(records_buffer);
        self.cursor = Some(BatchCursor {
            memory_records,
            next_batch_start: Some(0),
            current_batch: None,
            record_source: RecordSource::None,
            record_byte_offset: 0,
            records_remaining: 0,
        });
    }

    /// Pulls up to `max_records` records out of the fetch, decoding each
    /// via the supplied deserializers.
    ///
    /// Mirrors Java's
    /// `<K, V> List<ConsumerRecord<K, V>> fetchRecords(FetchConfig, Deserializers<K, V>, int)`.
    ///
    /// # Errors
    ///
    /// Returns the cached deserialization error from the previous call
    /// (Java's `corruptLastRecord` re-raise path), or a fresh
    /// deserialization error if a record fails to decode and no records
    /// were successfully decoded in this call.
    pub(crate) fn fetch_records<K, V>(
        &mut self,
        config: &FetchConfig,
        key_deserializer: &dyn Deserializer<K>,
        value_deserializer: &dyn Deserializer<V>,
        max_records: i32,
    ) -> Result<Vec<ConsumerRecord<K, V>>, Error>
    where
        K: 'static,
        V: 'static,
    {
        if self.corrupt_last_record {
            // Java: `throw new KafkaException("Received exception when fetching the
            // next record from " + partition + ". If needed, please seek past the
            // record to continue consumption.", cachedRecordException)` — a *bare*
            // `KafkaException` (so `is_kafka_error()` is true and
            // `FetchCollector`'s swallow guard applies) carrying the cached
            // record exception as its cause, not the cached exception itself.
            let message = format!(
                "Received an error when fetching the next record from {}. If needed, please seek past the record to continue consumption.",
                self.partition
            );
            return Err(match self.cached_record_error.clone() {
                Some(cause) => Error::kafka_message_source(message, cause),
                None => Error::kafka_message(message),
            });
        }
        if self.is_consumed || max_records <= 0 {
            return Ok(Vec::new());
        }

        let mut out: Vec<ConsumerRecord<K, V>> = Vec::new();
        // Java wraps the whole record loop in
        //   try { ... }
        //   catch (SerializationException se) { cachedRecordException = se;
        //       if (records.isEmpty()) throw se; }
        //   catch (KafkaException e) { cachedRecordException = e;
        //       if (records.isEmpty()) throw new KafkaException(
        //           "Received exception when fetching the next record from ...", e); }
        //   return records;
        // (`CompletedFetch.java:266-301`). Three effects per arm: cache the
        // error, propagate ONLY when nothing was decoded, and — for the broad
        // arm — wrap in the "seek past the record" message with the original as
        // the cause. When records ARE in hand the error is swallowed and the
        // prefix returned, so a corrupt batch does not discard the records
        // already decoded before it.
        let loop_result = self.fetch_records_loop(config, key_deserializer, value_deserializer, max_records, &mut out);
        match loop_result {
            Ok(()) => Ok(out),
            // `catch (SerializationException se)` comes FIRST in Java, so it
            // wins over the broad arm. `RecordDeserializationException extends
            // SerializationException`, hence both variants.
            Err(err @ (Error::Serialization(_) | Error::RecordDeserialization(_))) => {
                self.cached_record_error = Some(err.clone());
                if out.is_empty() {
                    // Java rethrows `se` itself — no message wrap.
                    Err(err)
                } else {
                    Ok(out)
                }
            },
            // `catch (KafkaException e)`. A non-`KafkaException` (Java's
            // `java.lang` runtime exceptions, which are siblings of
            // `KafkaException`) matches NEITHER clause: it escapes uncached and
            // unwrapped.
            Err(err) if err.is_kafka_error() => {
                self.cached_record_error = Some(err.clone());
                if out.is_empty() {
                    Err(Error::KafkaError(KafkaError::new_message_source(
                        Errors::UnknownServerError,
                        format!(
                            "Received an error when fetching the next record from {}. If needed, please seek past the record to continue consumption.",
                            self.partition
                        ),
                        err,
                    )))
                } else {
                    Ok(out)
                }
            },
            Err(err) => Err(err),
        }
    }

    /// The body of Java's `try` block inside `fetchRecords`
    /// (`CompletedFetch.java:266-289`).
    ///
    /// Split out so the caller can apply Java's two `catch` arms once, with
    /// access to the records decoded so far — which is what decides between
    /// propagating and swallowing. Every error exit below simply returns `Err`,
    /// exactly as Java's loop body simply throws.
    fn fetch_records_loop<K, V>(
        &mut self,
        config: &FetchConfig,
        key_deserializer: &dyn Deserializer<K>,
        value_deserializer: &dyn Deserializer<V>,
        max_records: i32,
        out: &mut Vec<ConsumerRecord<K, V>>,
    ) -> Result<(), Error>
    where
        K: 'static,
        V: 'static,
    {
        self.ensure_cursor();
        // Preallocate the output Vec to avoid the realloc churn of growing from
        // zero on every batch. At this point no batch has been loaded yet
        // (`ensure_cursor` defers parsing the first batch to the loop below, so
        // `records_remaining == 0` here and the batch's record count is not yet
        // known). Blindly reserving `max_records` (= `max.poll.records`, default
        // 500) would over-allocate ~hundreds of `ConsumerRecord` slots for small
        // fetches. We therefore cap the preallocation to a small constant: the
        // common steady-state fetch returns far fewer records than the cap, and
        // a larger fetch simply grows the Vec a couple more times — far cheaper
        // than the per-poll over-allocation. 512 covers the default
        // `max.poll.records` (500) without exceeding it for typical configs.
        let initial_capacity = (max_records as usize).min(512);
        out.reserve(initial_capacity);

        // §27 / CLAUDE.md §11 metrics-cost invariant (Milestone-9 Phase M8):
        // this per-record loop performs NO `Sensor.record(...)`. The only
        // metric work per record is the pure `records_read += 1; bytes_read
        // += size;` i32 accumulation below. The windowed `Sensor` recording
        // (the `FetchMetricsAggregator.record` → `FetchMetricsManager`
        // bytes/records/throttle/latency sensors) fires exactly once per
        // partition in `drain()`, never here. Adding a `Sensor.record` to this
        // loop would (a) take the sensor mutex per record and (b) potentially
        // allocate in the windowed-stat ring buffer per record — both forbidden
        // on the receive hot path. The guard test
        // `test_per_record_loop_is_pure_counter_no_sensor_record` asserts the
        // loop body allocates zero per record with an aggregator attached.
        for _ in 0..max_records {
            // Only advance to the next record if there was no cached
            // exception. Otherwise re-deserialize the last one so the
            // user can retry after fixing whatever state they like.
            if self.cached_record_error.is_none() {
                self.corrupt_last_record = true;
                let has_next = self.advance_to_next_fetched_record(config)?;
                self.corrupt_last_record = false;
                if !has_next {
                    break;
                }
            } else if self.peek_current_record()?.is_none() {
                break;
            }

            // §27: read the record as a borrowing `DefaultRecordRef` — the
            // key/value bytes are slices into the fetch (or decompression)
            // buffer, never copied. We deserialize while holding the borrow,
            // then advance the cursor's byte offset AFTER the borrow drops.
            let key_result;
            let value_result;
            let leader_epoch;
            let timestamp_type;
            let key_size;
            let value_size;
            let offset;
            let timestamp;
            let record_size_in_bytes;
            let record_bytes_consumed;
            let headers_owned;
            // Java's `newRecordDeserializationException` carries the offending
            // record's RAW key and value buffers (`record.key()` /
            // `record.value()`) so a consumer error handler can inspect them.
            // They are slices into the fetch buffer, alive only inside the
            // borrow below — so they are copied out ONLY when a deserializer
            // actually failed. On the happy path this stays `None` and §27's
            // "no per-record payload copy" guarantee holds.
            let mut error_key_bytes: Option<Vec<u8>> = None;
            let mut error_value_bytes: Option<Vec<u8>> = None;
            {
                // Verified non-empty: `advance_to_next_fetched_record` returned
                // `true` (or a cached exception positioned us here and the
                // `peek` above returned `Some`). The `?` propagates a
                // malformed-record-body error; a `None` here would mean the
                // record went missing between positioning and reading, which
                // is the premature-EOF (declared count > actual) state — surface
                // it as a recoverable error rather than panicking.
                let Some((record, batch_meta)) = self.peek_current_record()? else {
                    return Err(Error::InvalidRecord(InvalidRecordError::new(format!(
                        "Incorrect declared batch size for partition {}, premature EOF reached \
                         (declared record count exceeds the records present in the batch)",
                        self.partition
                    ))));
                };
                let topic_str: &str = &self.topic_arc;
                // §27: the refcounted buffer that owns this record's key/value
                // bytes. `BytesDeserializer::deserialize_from_shared` slices it
                // (slice_ref) with no copy; other deserializers ignore it. The
                // clone is an O(1) refcount bump.
                let source_bytes = self
                    .current_record_source_bytes()
                    .expect("record source must exist while peeking");
                // The only owned copy of record payload on the happy path:
                // the §27-sanctioned `RecordHeaders` (Milestone-8 holds
                // owned headers on the emitted `ConsumerRecord`).
                let headers_vec = record.headers().map_err(|e| {
                    Error::InvalidRecord(InvalidRecordError::new(format!(
                        "Record for partition {} at offset {} has invalid headers, cause: {}",
                        self.partition,
                        record.offset(),
                        e
                    )))
                })?;
                headers_owned = RecordHeaders::new_header_iter(headers_vec);
                key_result = match record.key() {
                    None => Ok(None),
                    Some(key_bytes) => key_deserializer
                        .deserialize_from_shared_with_headers(topic_str, &headers_owned, &source_bytes, key_bytes)
                        .map(Some),
                };
                // Java's `parseRecord` is two sequential `try` blocks and the first
                // one's catch *throws* (`CompletedFetch.java:313-328`), so the value
                // deserializer is never invoked for a record whose key failed. Running
                // it anyway is observable: a user deserializer may count, cache or log,
                // and it would do so for a record Java never hands it. The key error is
                // returned below before this value is read, so `Ok(None)` here is inert.
                value_result = if key_result.is_err() {
                    Ok(None)
                } else {
                    match record.value() {
                        None => Ok(None),
                        Some(value_bytes) => value_deserializer
                            .deserialize_from_shared_with_headers(topic_str, &headers_owned, &source_bytes, value_bytes)
                            .map(Some),
                    }
                };
                // Java passes BOTH buffers regardless of which side failed.
                if key_result.is_err() || value_result.is_err() {
                    error_key_bytes = record.key().map(<[u8]>::to_vec);
                    error_value_bytes = record.value().map(<[u8]>::to_vec);
                }
                leader_epoch = maybe_leader_epoch(batch_meta.partition_leader_epoch);
                timestamp_type = batch_meta.timestamp_type;
                key_size = record.key_size();
                value_size = record.value_size();
                offset = record.offset();
                timestamp = record.timestamp();
                record_size_in_bytes = record.size_in_bytes();
                record_bytes_consumed = record.size_in_bytes() as usize;
            }

            let key = match key_result {
                Ok(k) => k,
                Err(e) => {
                    let err = wrap_deserialization_error(
                        DeserializationOrigin::Key,
                        &self.partition,
                        offset,
                        timestamp,
                        timestamp_type,
                        error_key_bytes.take(),
                        error_value_bytes.take(),
                        Some(headers_owned.clone()),
                        e,
                    );
                    // Java's `catch (SerializationException se)` in the caller
                    // caches this and decides propagate-vs-swallow from
                    // `records.isEmpty()`. Java logs the failing deserializer
                    // inside `parseRecord` before throwing.
                    error!("Key deserialization failed for {} at offset {}", self.partition, offset);
                    return Err(err);
                },
            };
            let value = match value_result {
                Ok(v) => v,
                Err(e) => {
                    let err = wrap_deserialization_error(
                        DeserializationOrigin::Value,
                        &self.partition,
                        offset,
                        timestamp,
                        timestamp_type,
                        error_key_bytes.take(),
                        error_value_bytes.take(),
                        Some(headers_owned.clone()),
                        e,
                    );
                    error!("Value deserialization failed for {} at offset {}", self.partition, offset);
                    return Err(err);
                },
            };

            // §27: cheap Arc clone — atomic pointer bump, no UTF-8 copy.
            let topic_arc = Arc::clone(&self.topic_arc);
            let consumer_record = ConsumerRecord::new_options(
                ConsumerRecordOptionsBuilder::new_topic_partition_offset_key_value(
                    topic_arc,
                    self.partition.partition(),
                    offset,
                    key,
                    value,
                )
                .set_timestamp(timestamp)
                .set_timestamp_type(timestamp_type)
                .set_serialized_key_size(key_size)
                .set_serialized_value_size(value_size)
                .set_headers(headers_owned)
                .set_leader_epoch(leader_epoch)
                .build(),
            );
            self.records_read += 1;
            self.bytes_read += record_size_in_bytes;
            self.next_fetch_offset = offset + 1;
            self.cached_record_error = None;
            out.push(consumer_record);
            // Advance the record cursor — we successfully consumed this
            // record. Move the byte offset past it and decrement the
            // remaining-records count.
            if let Some(cursor) = &mut self.cursor {
                cursor.record_byte_offset += record_bytes_consumed;
                cursor.records_remaining -= 1;
            }
        }

        Ok(())
    }

    /// Advances the cursor to the next record that should be returned
    /// to the user, skipping out-of-range, aborted-transaction, and
    /// control batches per READ_COMMITTED semantics.
    ///
    /// Returns `Ok(true)` if a record is positioned at the cursor and
    /// ready to be read via [`Self::peek_current_record`]. Returns
    /// `Ok(false)` when iteration is exhausted; in that case the cursor
    /// is drained and `next_fetch_offset` is advanced to the end of the
    /// last batch.
    ///
    /// §27 zero-copy note: the previous version returned an owned
    /// `(DefaultRecord, BatchMetadata)` tuple, which forced a deep clone
    /// of the record's key + value bytes on every iteration. The current
    /// version returns a boolean and leaves the cursor positioned at the
    /// next record's byte offset; callers use [`Self::peek_current_record`]
    /// to read it by reference.
    fn advance_to_next_fetched_record(&mut self, config: &FetchConfig) -> Result<bool, Error> {
        loop {
            // Reload current batch if exhausted.
            let needs_new_batch = match &self.cursor {
                Some(cursor) => cursor.records_remaining <= 0,
                None => true,
            };
            if needs_new_batch {
                // Declared count < actual ("too little"): we consumed all
                // `records_count` declared records but the batch's record
                // section still has unconsumed bytes. Java's
                // `DefaultRecordBatch.RecordIterator` validates this via
                // `ensureNoneRemaining()` and throws `InvalidRecordException`
                // ("...records still remaining"), independent of CRC. Mirror
                // that here rather than silently dropping the trailing
                // records. The check is O(1) — it compares the already-walked
                // byte offset against the (already-known) record section
                // length, with no re-walk or copy.
                self.ensure_current_batch_fully_consumed()?;
                if !self.load_next_batch(config)? {
                    // No more batches. Advance to the next-after-last-batch
                    // offset (mirrors Java's `nextFetchOffset = currentBatch.nextOffset()`).
                    if let Some(cursor) = &self.cursor
                        && let Some(batch_meta) = &cursor.current_batch
                    {
                        self.next_fetch_offset = batch_meta.next_offset;
                    }
                    self.drain();
                    return Ok(false);
                }
                // load_next_batch may have skipped the batch entirely for
                // aborted transactions; try again from the top.
                continue;
            }

            // Pull next record from current batch — peek-style; we don't
            // advance until the caller decodes it successfully.
            //
            // Scope the borrow so the early-skip mutations below can
            // touch the cursor freely.
            let (record_offset, record_bytes_consumed, is_control_batch) = {
                // Declared count > actual ("too many"): the batch header
                // declares more records than the record section actually
                // contains. `records_remaining > 0` (we did NOT take the
                // `needs_new_batch` branch), yet `peek_current_record` yields
                // `None` because the record bytes are exhausted. Java's
                // `DefaultRecordBatch.RecordIterator` reads past EOF and throws
                // `InvalidRecordException` ("...premature EOF reached"),
                // independent of CRC. Surface a recoverable error rather than
                // panicking via `.expect`.
                let Some((record, batch_meta)) = self.peek_current_record()? else {
                    return Err(Error::InvalidRecord(InvalidRecordError::new(format!(
                        "Incorrect declared batch size for partition {}, premature EOF reached \
                         (declared record count exceeds the records present in the batch)",
                        self.partition
                    ))));
                };
                // Per-record CRC validation: v2 records carry no per-record
                // CRC (the CRC covers the whole batch and is checked in
                // `load_next_batch`), so `ensure_valid` is infallible here —
                // matching Java's `DefaultRecord`. The `config.check_crcs`
                // gate is therefore a no-op at the record level.
                (record.offset(), record.size_in_bytes() as usize, batch_meta.is_control_batch)
            };

            // Skip out-of-range records.
            if record_offset < self.next_fetch_offset {
                self.advance_record_cursor(record_bytes_consumed);
                continue;
            }

            if is_control_batch {
                // Control records are not returned to the user — advance
                // nextFetchOffset and skip.
                self.next_fetch_offset = record_offset + 1;
                self.advance_record_cursor(record_bytes_consumed);
                continue;
            }
            return Ok(true);
        }
    }

    /// Advances the within-batch record cursor by `record_bytes_consumed`
    /// bytes and decrements the remaining-records count.
    fn advance_record_cursor(&mut self, record_bytes_consumed: usize) {
        if let Some(cursor) = &mut self.cursor {
            cursor.record_byte_offset += record_bytes_consumed;
            cursor.records_remaining -= 1;
        }
    }

    /// Validates that the currently-loaded batch, whose declared record count
    /// has just been exhausted (`records_remaining <= 0`), has no record bytes
    /// left over.
    ///
    /// This is the analog of Java `DefaultRecordBatch.RecordIterator`'s
    /// `ensureNoneRemaining()`, which throws `InvalidRecordException`
    /// ("Incorrect declared batch size, records still remaining") when the
    /// declared record count is *fewer* than the records actually present
    /// (the "too little" direction). Without this check the cursor would
    /// silently drop the trailing valid records and advance to the next
    /// batch. Java performs this validation independently of CRC, so it must
    /// fire even under `check.crcs=false`.
    ///
    /// O(1): compares the already-walked `record_byte_offset` against the
    /// (already-known) length of the batch's record section — no re-walk and
    /// no copy. Returns `Ok(())` when there is no batch loaded yet (nothing to
    /// validate).
    fn ensure_current_batch_fully_consumed(&self) -> Result<(), Error> {
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(());
        };
        // Only meaningful once a batch has been loaded and its declared count
        // consumed. `current_batch == None` means we have not loaded a batch
        // yet (first iteration).
        if cursor.current_batch.is_none() || cursor.records_remaining > 0 {
            return Ok(());
        }
        let records_len = match &cursor.record_source {
            RecordSource::None => return Ok(()),
            RecordSource::Borrowed(range) => range.len(),
            RecordSource::Owned(buf) => buf.len(),
        };
        if cursor.record_byte_offset < records_len {
            return Err(Error::InvalidRecord(InvalidRecordError::new(format!(
                "Incorrect declared batch size for partition {}, records still remaining in batch \
                 (declared record count is fewer than the records present)",
                self.partition
            ))));
        }
        Ok(())
    }

    /// Returns the refcounted buffer that owns the current batch's record
    /// bytes, so per-record key/value slices can be handed to the deserializer
    /// as zero-copy `Bytes` via [`bytes::Bytes::slice_ref`] (§27).
    ///
    /// For an uncompressed batch this is the whole `MemoryRecords` buffer (the
    /// borrowed record slice is a subslice of it); for a compressed batch it is
    /// the decompressed buffer. Clones are O(1) refcount bumps. Returns `None`
    /// when no batch is loaded.
    fn current_record_source_bytes(&self) -> Option<bytes::Bytes> {
        let cursor = self.cursor.as_ref()?;
        match &cursor.record_source {
            RecordSource::None => None,
            RecordSource::Borrowed(_) => Some(cursor.memory_records.buffer_bytes().clone()),
            RecordSource::Owned(buf) => Some(buf.clone()),
        }
    }

    /// Parses the current record (the one at `cursor.record_byte_offset`)
    /// into a borrowing [`DefaultRecordRef`] and returns it together with its
    /// enclosing batch metadata, both borrowing from `self`.
    ///
    /// Returns:
    ///   - `Ok(None)` when no record is positioned at the cursor — either the
    ///     declared record count is exhausted (`records_remaining <= 0`) or the
    ///     batch's record bytes are exhausted (`record_byte_offset` past end).
    ///   - `Ok(Some(..))` when a record is parsed.
    ///   - `Err(..)` when the record body is individually malformed (e.g. a bad
    ///     varint). This is a recoverable [`Error`] — the receive path no
    ///     longer walks/validates the batch's records on load (the O(N²) walk was
    ///     removed in the §27/O(1) batch-loading change), so a malformed record
    ///     body is genuine bad input that must surface to the caller, not be
    ///     swallowed. Mirrors Java `DefaultRecordBatch.RecordIterator` reading
    ///     past EOF / failing to decode a record.
    ///
    /// §27 zero-copy note: the returned `DefaultRecordRef` borrows the
    /// record's key/value/header bytes directly from the cursor's record
    /// source — no copy. The per-record parse is varint decoding only; the
    /// payload bytes are never touched.
    fn peek_current_record(&self) -> Result<Option<(DefaultRecordRef<'_>, &BatchMetadata)>, Error> {
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(None);
        };
        if cursor.records_remaining <= 0 {
            return Ok(None);
        }
        let Some(batch_meta) = cursor.current_batch.as_ref() else {
            return Ok(None);
        };
        let records_bytes = match &cursor.record_source {
            RecordSource::None => return Ok(None),
            RecordSource::Borrowed(range) => &cursor.memory_records.buffer()[range.clone()],
            RecordSource::Owned(buf) => &buf[..],
        };
        if cursor.record_byte_offset >= records_bytes.len() {
            return Ok(None);
        }
        let log_append_time = if batch_meta.timestamp_type == TimestampType::LogAppendTime {
            Some(batch_meta.last_offset_timestamp)
        } else {
            None
        };
        // A parse failure here is bad input (the record body is malformed),
        // not a logic bug: the receive path no longer re-walks the batch's
        // records on load, so this is the first time the record body is
        // decoded. Surface it as a recoverable error rather than panicking
        // or silently dropping the rest of the batch — matching Java
        // `DefaultRecordBatch.RecordIterator`, which throws
        // `InvalidRecordException` (CRC-independent).
        let (record, _consumed) = DefaultRecord::read_ref_from_buffer(
            &records_bytes[cursor.record_byte_offset..],
            batch_meta.base_offset,
            batch_meta.base_timestamp,
            batch_meta.base_sequence,
            log_append_time,
        )
        .map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!(
                "Record batch for partition {} at offset {} is invalid, cause: {}",
                self.partition, batch_meta.base_offset, e
            )))
        })?;
        Ok(Some((record, batch_meta)))
    }

    /// Whether `batch` is a control batch whose first record is an ABORT marker.
    ///
    /// Translated from `CompletedFetch.containsAbortMarker(RecordBatch)`
    /// (`CompletedFetch.java:352-359`): non-control batches are `false`, an empty
    /// batch is `false`, and otherwise the first record's key is parsed as a
    /// [`ControlRecordType`].
    ///
    /// `source` is passed in rather than read from the cursor because the check runs
    /// *before* the batch is installed, which is also where Java runs it — the batch
    /// may be skipped, and a skipped batch is never installed.
    ///
    /// # Errors
    ///
    /// A malformed control record is bad input, not a logic bug, so it becomes a
    /// recoverable error — the same treatment [`Self::peek_current_record`] gives a
    /// malformed data record. Java throws `InvalidRecordException` from
    /// `ControlRecordType.parse`.
    fn contains_abort_marker(&self, batch: &BatchMetadata, source: &RecordSource) -> Result<bool, Error> {
        if !batch.is_control_batch {
            return Ok(false);
        }
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(false);
        };
        let records_bytes = match source {
            RecordSource::None => return Ok(false),
            RecordSource::Borrowed(range) => &cursor.memory_records.buffer()[range.clone()],
            RecordSource::Owned(buf) => &buf[..],
        };
        // Java's `if (!batchIterator.hasNext()) return false` — an empty control
        // batch carries no marker.
        if records_bytes.is_empty() {
            return Ok(false);
        }
        let log_append_time = if batch.timestamp_type == TimestampType::LogAppendTime {
            Some(batch.last_offset_timestamp)
        } else {
            None
        };
        let (record, _consumed) = DefaultRecord::read_ref_from_buffer(
            records_bytes,
            batch.base_offset,
            batch.base_timestamp,
            batch.base_sequence,
            log_append_time,
        )
        .map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!(
                "Control batch for partition {} at offset {} is invalid, cause: {}",
                self.partition, batch.base_offset, e
            )))
        })?;
        // A control record always has a key; a control batch whose first record has
        // none cannot be a marker, so it is `UNKNOWN` in Java terms — `parse` would
        // throw on a null key, and Java never reaches that because the broker always
        // writes one. Treated as "not an abort marker" rather than as an error, so a
        // future control type this client does not know about cannot stall a fetch.
        let Some(key) = record.key() else {
            return Ok(false);
        };
        let control_type = ControlRecordType::parse(key).map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!(
                "Control batch for partition {} at offset {} has an invalid control record key, cause: {}",
                self.partition,
                batch.base_offset,
                e.message()
            )))
        })?;
        Ok(control_type == ControlRecordType::Abort)
    }

    /// Loads the next batch into the cursor. Skips aborted-transaction
    /// batches and applies READ_COMMITTED filtering. Returns
    /// `Ok(true)` if a batch is now loaded, `Ok(false)` if no more
    /// batches remain.
    fn load_next_batch(&mut self, config: &FetchConfig) -> Result<bool, Error> {
        loop {
            // Phase 1: pull batch metadata + record-source descriptor out of
            // the cursor in a tight scope that drops the &mut self.cursor
            // borrow before touching the other self fields.
            //
            // §27 + O(1) batch loading: the next batch's absolute byte offset
            // is tracked incrementally in `cursor.next_batch_start` and parsed
            // in place via a borrowing `DefaultRecordBatchRef` — we do NOT
            // re-walk `memory_records.batches()` from the start (which was
            // O(N²) over a fetch and copied every batch into an owned `Vec`).
            // For uncompressed batches we record the byte *range* of the
            // batch's records section and borrow it lazily per record (no
            // copy). For compressed batches we decompress once into an owned
            // buffer held by the cursor.
            let (batch_meta, source, records_count) = {
                let cursor = match &mut self.cursor {
                    Some(c) => c,
                    None => return Ok(false),
                };
                let Some(batch_start) = cursor.next_batch_start else {
                    return Ok(false);
                };

                let buffer = cursor.memory_records.buffer();
                // Need at least LOG_OVERHEAD bytes to read base_offset + length;
                // mirrors `BatchIterator::next`'s bounds checks. A partial or
                // trailing batch terminates iteration.
                if batch_start + LOG_OVERHEAD > buffer.len() {
                    cursor.next_batch_start = None;
                    return Ok(false);
                }
                let batch = DefaultRecordBatchRef::new(&buffer[batch_start..]);
                let batch_size = batch.size_in_bytes();
                if batch_start + batch_size > buffer.len() {
                    cursor.next_batch_start = None;
                    return Ok(false);
                }

                // CRC validation per Java's maybeEnsureValid(batch).
                if config.check_crcs
                    && batch.magic() >= RecordVersion::V2.value()
                    && let Err(e) = batch.ensure_valid()
                {
                    return Err(Error::kafka_message(format!(
                        "Record batch for partition {} at offset {} is invalid, cause: {}",
                        self.partition,
                        batch.base_offset(),
                        e
                    )));
                }

                let meta = BatchMetadata {
                    base_offset: batch.base_offset(),
                    base_timestamp: batch.base_timestamp(),
                    base_sequence: batch.base_sequence(),
                    last_offset_timestamp: batch.max_timestamp(),
                    last_offset: batch.last_offset(),
                    next_offset: batch.last_offset() + 1,
                    timestamp_type: batch.timestamp_type(),
                    partition_leader_epoch: batch.partition_leader_epoch(),
                    is_control_batch: batch.is_control_batch(),
                    is_transactional: batch.is_transactional(),
                    has_producer_id: batch.producer_id() >= 0,
                    producer_id: batch.producer_id(),
                    magic: batch.magic(),
                };

                // Build the record-source descriptor for this batch.
                // `try_is_compressed`, not `is_compressed`: this batch came off
                // the wire, and an unknown codec id must fail as Java's
                // `CompressionType.forId` does rather than be read as
                // uncompressed (which would parse compressed bytes as records).
                let source = if batch.try_is_compressed()? {
                    // Decompress once per batch into an owned buffer; records
                    // then borrow from it.
                    let decompressed = batch.decompress_records().map_err(|e| {
                        Error::kafka_message(format!(
                            "Record batch for partition {} at offset {} is invalid, cause: {}",
                            self.partition, meta.base_offset, e
                        ))
                    })?;
                    // `Bytes::from(Vec<u8>)` adopts the decompressed allocation
                    // without copying; records then slice_ref from it (§27).
                    RecordSource::Owned(bytes::Bytes::from(decompressed))
                } else {
                    // Borrow the records section directly from the canonical
                    // buffer. `batch_size` includes LOG_OVERHEAD, so the
                    // records section is
                    // [batch_start + RECORD_BATCH_OVERHEAD, batch_start + size).
                    let records_start = batch_start + RecordBatch::RECORD_BATCH_OVERHEAD;
                    let records_end = batch_start + batch_size;
                    RecordSource::Borrowed(records_start..records_end)
                };

                let records_count = batch.records_count();
                // Advance the cursor's next-batch pointer by this batch's size
                // (O(1)) so both the skip path and the load path move forward.
                cursor.next_batch_start = Some(batch_start + batch_size);
                (meta, source, records_count)
            };

            // Phase 2: now that the cursor borrow is dropped, we can
            // touch the other self fields freely.
            self.last_epoch = maybe_leader_epoch(batch_meta.partition_leader_epoch);

            if config.isolation_level == IsolationLevel::ReadCommitted && batch_meta.has_producer_id {
                self.consume_aborted_transactions_up_to(batch_meta.last_offset);
                // `CompletedFetch.java:210-218`, in Java's order: an ABORT marker
                // *clears* the producer id, and only then is a batch considered
                // aborted. The order is load-bearing — a producer that aborts and
                // then commits reuses the same producer id, so without the clear
                // every later transaction from it would be skipped too.
                if self.contains_abort_marker(&batch_meta, &source)? {
                    self.aborted_producer_ids.remove(&batch_meta.producer_id);
                } else if batch_meta.is_transactional && self.aborted_producer_ids.contains(&batch_meta.producer_id) {
                    // Java's `isBatchAborted`, which gates on `isTransactional()` —
                    // a non-transactional batch with a producer id is never aborted.
                    debug!(
                        "Skipping aborted record batch from partition {} with producerId {} and offsets {} to {}",
                        self.partition, batch_meta.producer_id, batch_meta.base_offset, batch_meta.last_offset
                    );
                    self.next_fetch_offset = batch_meta.next_offset;
                    // `source` (incl. any decompression buffer) is dropped
                    // here — we never decode this aborted batch's records.
                    continue;
                }
            }

            // Install the batch as the current one. We use the batch
            // header's declared record count (not the offset span, which can
            // exceed the record count after log compaction).
            if let Some(cursor) = &mut self.cursor {
                cursor.record_source = source;
                cursor.record_byte_offset = 0;
                cursor.records_remaining = records_count;
                cursor.current_batch = Some(batch_meta);
            }
            return Ok(true);
        }
    }

    /// Drains aborted-transaction entries up to and including `offset`,
    /// recording their producer IDs in `aborted_producer_ids`.
    fn consume_aborted_transactions_up_to(&mut self, offset: i64) {
        while let Some(top) = self.aborted_transactions.peek() {
            if top.0.first_offset <= offset {
                let txn = self.aborted_transactions.pop().expect("just peeked");
                self.aborted_producer_ids.insert(txn.0.producer_id);
            } else {
                break;
            }
        }
    }
}

/// Categorizes which side of the deserialization (key vs value) failed.
/// Mirrors Java's
/// `RecordDeserializationException.DeserializationExceptionOrigin`.
#[derive(Clone, Copy, Debug)]
enum DeserializationOrigin {
    Key,
    Value,
}

impl DeserializationOrigin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Key => "KEY",
            Self::Value => "VALUE",
        }
    }
}

impl From<DeserializationOrigin> for DeserializationErrorOrigin {
    fn from(origin: DeserializationOrigin) -> Self {
        match origin {
            DeserializationOrigin::Key => Self::Key,
            DeserializationOrigin::Value => Self::Value,
        }
    }
}

/// Build Java's `RecordDeserializationException` for a failed key/value decode.
///
/// Mirrors `newRecordDeserializationException` (`CompletedFetch.java:336-345`),
/// which passes the full record context — origin, partition, offset, timestamp,
/// timestamp type, raw key and value buffers, and headers — alongside the
/// message, with the deserializer's exception as the **cause**. Those eight
/// fields are the ones consumer error handlers read to decide whether to skip
/// the record, so a plain `SerializationException` carrying only the message
/// would drop the information the type exists to convey.
///
/// The message itself carries no "Cause: ..." suffix; the cause is a separate
/// field reachable through [`Error::source`].
#[allow(clippy::too_many_arguments)]
fn wrap_deserialization_error(
    origin: DeserializationOrigin,
    partition: &TopicPartition,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    key_buffer: Option<Vec<u8>>,
    value_buffer: Option<Vec<u8>>,
    headers: Option<RecordHeaders>,
    source: Error,
) -> Error {
    let message = format!(
        "Error deserializing {} for partition {} at offset {}. \
         If needed, please seek past the record to continue consumption.",
        origin.as_str(),
        partition,
        offset,
    );
    Error::RecordDeserialization(Box::new(
        RecordDeserializationError::new(
            origin.into(),
            partition.clone(),
            offset,
            timestamp,
            timestamp_type,
            key_buffer,
            value_buffer,
            headers,
            message,
        )
        .with_source(source),
    ))
}

fn maybe_leader_epoch(epoch: i32) -> Option<i32> {
    if epoch == NO_PARTITION_LEADER_EPOCH {
        None
    } else {
        Some(epoch)
    }
}

impl std::fmt::Debug for CompletedFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedFetch")
            .field("partition", &self.partition)
            .field("next_fetch_offset", &self.next_fetch_offset)
            .field("is_consumed", &self.is_consumed)
            .field("initialized", &self.initialized)
            .field("records_read", &self.records_read)
            .field("bytes_read", &self.bytes_read)
            .finish_non_exhaustive()
    }
}

fn build_aborted_transactions(partition_data: &PartitionData) -> BinaryHeap<AbortedTxnByFirstOffset> {
    let mut heap: BinaryHeap<AbortedTxnByFirstOffset> = BinaryHeap::new();
    if let Some(txns) = partition_data.aborted_transactions.as_ref() {
        for txn in txns {
            heap.push(AbortedTxnByFirstOffset(txn.clone()));
        }
    }
    heap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::record::internal::{MemoryRecords, SimpleRecord};
    use crate::common::serialization::Deserializer;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::fetch_metrics_manager::FetchMetricsManager;
    use crate::fetch_response_data::PartitionData;
    use std::sync::{Arc, Mutex};

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Builds a throwaway per-response aggregator tracking only `tp("test", 0)`,
    /// for tests that exercise `drain()` but don't assert metric values.
    fn test_aggregator() -> Arc<FetchMetricsAggregator> {
        let mut partitions = std::collections::HashSet::new();
        partitions.insert(tp("test", 0));
        Arc::new(FetchMetricsAggregator::new(FetchMetricsManager::for_test(), partitions))
    }

    /// String deserializer that decodes UTF-8 bytes.
    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(e.to_string()))
        }
    }

    /// Deserializer that always fails — used to drive the deserialization
    /// error path.
    struct FailingDeserializer;
    impl Deserializer<String> for FailingDeserializer {
        fn deserialize(&self, _topic: &str, _data: &[u8]) -> Result<String, Error> {
            Err(Error::serialization("simulated failure"))
        }
    }

    /// Marks which side (KEY or VALUE) of the deserialization is
    /// expected to fail for [`MaybeFailingDeserializer`].
    #[derive(Clone, Copy)]
    enum DeserializationOriginFlag {
        Key,
        Value,
    }

    /// Deserializer that succeeds on most records but fails on a single
    /// configured offset, identified by parsing the
    /// [`new_records_with_keyed_offsets`] fixture's `"key-N"` / `"value-N"`
    /// encoding. Used to exercise the cached-exception re-raise path
    /// (`CompletedFetchTest.testCorruptedMessage`).
    struct MaybeFailingDeserializer {
        side: DeserializationOriginFlag,
        fail_on_offset: i64,
    }
    impl MaybeFailingDeserializer {
        fn new(side: DeserializationOriginFlag, fail_on_offset: i64) -> Self {
            Self { side, fail_on_offset }
        }
        /// Returns the offset embedded in a `"key-N"` / `"value-N"`
        /// fixture string. Returns `None` if the prefix doesn't match.
        fn parse_offset(bytes: &[u8], expected_prefix: &str) -> Option<i64> {
            let s = std::str::from_utf8(bytes).ok()?;
            s.strip_prefix(expected_prefix).and_then(|n| n.parse::<i64>().ok())
        }
    }
    impl Deserializer<String> for MaybeFailingDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            let (prefix, origin_label) = match self.side {
                DeserializationOriginFlag::Key => ("key-", "key"),
                DeserializationOriginFlag::Value => ("value-", "value"),
            };
            if let Some(n) = Self::parse_offset(data, prefix)
                && n == self.fail_on_offset
            {
                return Err(Error::serialization(format!("simulated {origin_label} failure at offset {n}")));
            }
            String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(e.to_string()))
        }
    }

    fn make_fetch_config(isolation_level: IsolationLevel, check_crcs: bool) -> FetchConfig {
        FetchConfig::new(1, 50 * 1024 * 1024, 500, 1024 * 1024, 500, check_crcs, "", isolation_level)
    }

    fn make_subscriptions() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)))
    }

    fn new_records(base_offset: i64, count: i32, first_message_id: i64) -> Vec<u8> {
        let simple_records: Vec<SimpleRecord> = (0..count)
            .map(|i| {
                let value = format!("value-{}", first_message_id + i as i64);
                SimpleRecord::new(0, Some("key".as_bytes().to_vec()), Some(value.into_bytes()), vec![])
            })
            .collect();
        let records = MemoryRecords::with_records_at_offset(
            2,
            base_offset,
            Compression::none(),
            TimestampType::CreateTime,
            &simple_records,
        );
        records.buffer().to_vec()
    }

    /// Fixture that uses `"key-{offset}"`/`"value-{offset}"` so each
    /// record's bytes embed its (synthetic) offset. Used by the
    /// corrupted-message tests, which need a way to fail a SPECIFIC
    /// record (rather than the Nth call across many records) without
    /// pulling in additional dependencies.
    fn new_records_with_keyed_offsets(base_offset: i64, count: i32, first_message_id: i64) -> Vec<u8> {
        let simple_records: Vec<SimpleRecord> = (0..count)
            .map(|i| {
                let n = first_message_id + i as i64;
                let key = format!("key-{n}");
                let value = format!("value-{n}");
                SimpleRecord::new(0, Some(key.into_bytes()), Some(value.into_bytes()), vec![])
            })
            .collect();
        let records = MemoryRecords::with_records_at_offset(
            2,
            base_offset,
            Compression::none(),
            TimestampType::CreateTime,
            &simple_records,
        );
        records.buffer().to_vec()
    }

    /// Like [`new_records`] but with the batch records compressed, to
    /// exercise the `RecordSource::Owned` (decompress-once) path.
    fn new_compressed_records(base_offset: i64, count: i32, first_message_id: i64) -> Vec<u8> {
        let simple_records: Vec<SimpleRecord> = (0..count)
            .map(|i| {
                let value = format!("value-{}", first_message_id + i as i64);
                SimpleRecord::new(0, Some("key".as_bytes().to_vec()), Some(value.into_bytes()), vec![])
            })
            .collect();
        let records = MemoryRecords::with_records_at_offset(
            2,
            base_offset,
            Compression::gzip(),
            TimestampType::CreateTime,
            &simple_records,
        );
        records.buffer().to_vec()
    }

    /// Builds a buffer containing `batch_count` separate, consecutive record
    /// batches (one batch per `MemoryRecords`, concatenated). Each batch holds
    /// `records_per_batch` records; offsets are contiguous across batches
    /// starting at `base_offset`. Used to exercise the multi-batch path that
    /// the previous O(N²) batch walk affected.
    fn new_multi_batch_records(
        base_offset: i64,
        batch_count: i32,
        records_per_batch: i32,
        compression: Compression,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut offset = base_offset;
        for _ in 0..batch_count {
            let simple_records: Vec<SimpleRecord> = (0..records_per_batch)
                .map(|i| {
                    let n = offset + i as i64;
                    SimpleRecord::new(
                        0,
                        Some(format!("key-{n}").into_bytes()),
                        Some(format!("value-{n}").into_bytes()),
                        vec![],
                    )
                })
                .collect();
            let records = MemoryRecords::with_records_at_offset(
                2,
                offset,
                compression.clone(),
                TimestampType::CreateTime,
                &simple_records,
            );
            buf.extend_from_slice(records.buffer());
            offset += records_per_batch as i64;
        }
        buf
    }

    fn new_completed_fetch(fetch_offset: i64, records_bytes: Vec<u8>) -> CompletedFetch {
        let mut partition_data = PartitionData::new();
        partition_data.set_records(Some(bytes::Bytes::from(records_bytes)));
        CompletedFetch::new_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data,
            test_aggregator(),
            fetch_offset,
        )
    }

    /// Zero-allocation deserializer: decodes to the byte length (`usize`),
    /// touching the borrowed slice but allocating nothing. Used by the M8
    /// metrics-cost guard so the only per-record allocations in
    /// `fetch_records` come from `ConsumerRecord` construction +
    /// `out.push(...)` — NOT from the user's `T` decode — making any metrics
    /// regression on the per-record path unmistakable.
    struct LenDeserializer;
    impl Deserializer<usize> for LenDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<usize, Error> {
            Ok(data.len())
        }
    }

    /// Milestone-9 Phase M8 — explicit guard that the metrics wiring added
    /// ZERO per-record cost on the receive hot path (CLAUDE.md §11 / §27).
    ///
    /// The per-record loop in `fetch_records` performs only the pure i32
    /// accumulation `records_read += 1; bytes_read += size;` — there is NO
    /// `Sensor.record(...)` per record. The windowed `Sensor` recording
    /// (`FetchMetricsAggregator::record` → `FetchMetricsManager` sensors)
    /// fires exactly once per partition in `drain()`.
    ///
    /// This is an ALLOCATION-COUNT guard. It catches an *allocating*
    /// per-record metric regression, which is the realistic one:
    ///   1. With a metrics aggregator attached and a zero-alloc deserializer,
    ///      `fetch_records` allocates a small, FIXED count per record
    ///      (`ConsumerRecord` + `Vec` growth only). If an *allocating* metric
    ///      operation leaked into the per-record loop — e.g. moving
    ///      `FetchMetricsAggregator::record` (which allocates a `String` +
    ///      `Vec`) into it, or a windowed-stat sample ROTATION (a new `Sample`
    ///      pushed when a window rolls over) — the per-record allocation count
    ///      would rise above the tight budget and this test trips.
    ///   2. `drain()` — where the per-partition sensor record actually fires —
    ///      is called exactly once, OUTSIDE the per-record window.
    ///
    /// What this test does NOT prove: it would NOT catch a bare steady-state
    /// `Sensor::record(value)` dropped into the per-record loop. A windowed
    /// `SampledStat` preallocates its sample `Vec`
    /// (`Vec::with_capacity(DEFAULT_NUM_SAMPLES + 1)`), so a steady-state
    /// `record_internal` is pure mutex + arithmetic — zero allocation — and an
    /// alloc-count budget cannot see it. The stronger invariant ("NO
    /// `Sensor::record` per record at all") is established by code inspection
    /// of the verified-pure `fetch_records` loop body plus the loop-head
    /// comment, NOT by this allocation test.
    #[test]
    fn test_per_record_loop_is_pure_counter_no_sensor_record() {
        const RECORD_COUNT: i32 = 200;
        // ConsumerRecord construction + Vec growth only. An *allocating*
        // per-record metric operation — moving `FetchMetricsAggregator::record`
        // into the loop, or a windowed-stat sample ROTATION (new `Sample`
        // pushed on window rollover) — would add at least one alloc/record,
        // pushing this well past 3/record. (A non-allocating steady-state
        // `Sensor::record` would NOT be caught here — see the doc comment.)
        // The zero-alloc `LenDeserializer` removes the user-decode allocations
        // so the budget isolates structural per-record cost.
        const ALLOC_BUDGET_PER_RECORD: usize = 3;
        const OVERHEAD_BUDGET: usize = 64;

        let bytes = new_records(0, RECORD_COUNT, 0);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de = LenDeserializer;
        let value_de = LenDeserializer;

        // Warm the cursor / batch setup OUTSIDE the tracking window so the
        // one-time MemoryRecords/cursor allocations don't count.
        cf.ensure_cursor();

        let alloc_count;
        let n_records;
        {
            let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            // The measured call drives ONLY the per-record loop — no drain().
            let recs = cf
                .fetch_records::<usize, usize>(&fetch_config, &key_de, &value_de, RECORD_COUNT)
                .unwrap();
            alloc_count = crate::test_alloc_tracker::AllocTrackingGuard::count();
            n_records = recs.len();
        }

        assert_eq!(RECORD_COUNT as usize, n_records, "fetch_records did not return all records");

        let max_allowed = OVERHEAD_BUDGET + ALLOC_BUDGET_PER_RECORD * (RECORD_COUNT as usize);
        assert!(
            alloc_count <= max_allowed,
            "Metrics regression on the per-record path: {alloc_count} allocs for {RECORD_COUNT} \
             records (budget {max_allowed}). The per-record loop must stay pure i32 counter \
             accumulation — an *allocating* metric operation (e.g. `aggregator.record(...)` or a \
             windowed-stat sample rotation) entered the loop (CLAUDE.md §11 / §27)."
        );

        // The per-partition sensor recording happens HERE — once — not in the
        // loop above. `records_read` reflects the pure counter accumulated by
        // the per-record loop.
        assert_eq!(RECORD_COUNT, cf.records_read);
        cf.drain(); // fires `aggregator.record(...)` exactly once for this partition.
        assert!(cf.is_consumed);

        eprintln!(
            "M8 per-record metrics guard: {alloc_count} allocs for {RECORD_COUNT} records \
             (avg {avg:.2}/record, max allowed {max_allowed}); sensor record fires once in drain()",
            avg = alloc_count as f64 / RECORD_COUNT as f64,
        );
    }

    /// Translated from `CompletedFetchTest.testSimple`.
    #[test]
    fn test_simple() {
        let fetch_offset = 5;
        let starting_offset = 10;
        let num_records = 11; // offsets 10..20 inclusive
        let bytes = new_records(starting_offset, num_records, fetch_offset);
        let mut cf = new_completed_fetch(fetch_offset, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de: StringDeserializer = StringDeserializer;
        let value_de: StringDeserializer = StringDeserializer;

        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(10, records.len());
        assert_eq!(10, records[0].offset());

        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(1, records.len());
        assert_eq!(20, records[0].offset());

        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(0, records.len());
    }

    /// Exercises the compressed-batch decode path (`RecordSource::Owned`):
    /// records are decompressed once into an owned buffer, then each record's
    /// key/value is borrowed from it — same observable result as the
    /// uncompressed path.
    #[test]
    fn test_simple_compressed() {
        let fetch_offset = 5;
        let starting_offset = 10;
        let num_records = 11; // offsets 10..20 inclusive
        let bytes = new_compressed_records(starting_offset, num_records, fetch_offset);
        let mut cf = new_completed_fetch(fetch_offset, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;

        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(10, records.len());
        assert_eq!(10, records[0].offset());
        assert_eq!(Some(&"key".to_string()), records[0].key());
        assert_eq!(Some(&"value-5".to_string()), records[0].value());

        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(1, records.len());
        assert_eq!(20, records[0].offset());

        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(0, records.len());
    }

    /// Exercises a multi-batch `CompletedFetch` (≥3 separate batches in one
    /// fetch payload) end-to-end through `fetch_records`. This is the path the
    /// previous O(N²) batch walk affected: with the incremental
    /// `next_batch_start` tracking and the borrowing `DefaultRecordBatchRef`
    /// header parse, batch loading is O(1)-amortized and copy-free, but the
    /// observable result — every record returned exactly once, in offset
    /// order, with correct key/value — must be unchanged.
    #[test]
    fn test_multi_batch_ordering_and_offsets() {
        for compression in [Compression::none(), Compression::gzip()] {
            let base_offset = 50;
            let batch_count = 4;
            let records_per_batch = 3;
            let total = batch_count * records_per_batch; // 12 records, offsets 50..=61
            let bytes = new_multi_batch_records(base_offset, batch_count, records_per_batch, compression.clone());
            let mut cf = new_completed_fetch(base_offset, bytes);
            let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
            let key_de = StringDeserializer;
            let value_de = StringDeserializer;

            // Pull a few at a time to cross batch boundaries mid-call.
            let mut collected: Vec<ConsumerRecord<String, String>> = Vec::new();
            loop {
                let batch = cf
                    .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 5)
                    .unwrap();
                if batch.is_empty() {
                    break;
                }
                collected.extend(batch);
            }

            assert_eq!(total as usize, collected.len(), "compression {compression:?}");
            for (i, record) in collected.iter().enumerate() {
                let expected_offset = base_offset + i as i64;
                assert_eq!(expected_offset, record.offset(), "offset mismatch at index {i}");
                assert_eq!(Some(&format!("key-{expected_offset}")), record.key());
                assert_eq!(Some(&format!("value-{expected_offset}")), record.value());
            }
            // next_fetch_offset advanced past the last record of the last batch.
            assert_eq!(base_offset + total as i64, cf.next_fetch_offset());
            assert!(cf.is_consumed());
        }
    }

    /// Builds a single uncompressed v2 batch at `base_offset` holding `count`
    /// `value-{offset}` records, then overwrites its declared record count with
    /// `declared_count`. The CRC is NOT recomputed, so this fixture must be
    /// driven with `check.crcs=false` — exactly the configuration under which
    /// Java's `DefaultRecordBatch.RecordIterator` count validation (which is
    /// CRC-independent) still has to fire.
    fn batch_with_overridden_record_count(base_offset: i64, count: i32, declared_count: i32) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_with_magic(
            512,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            base_offset,
        );
        for i in 0..count {
            let offset = base_offset + i as i64;
            let value = format!("value-{offset}");
            builder.append_with_offset_bytes(offset, 0, None, Some(value.as_bytes()));
        }
        let records = builder.build();
        let mut buf = records.buffer().to_vec();
        buf[RecordBatch::RECORDS_COUNT_OFFSET..RecordBatch::RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&declared_count.to_be_bytes());
        buf
    }

    /// Builds a single v2 batch with the given control / transactional flags
    /// and producer id, holding `count` `value-{offset}` records. The CRC is
    /// recomputed after the producer state is written by the builder, so the
    /// batch is valid under `check.crcs=true`.
    #[allow(clippy::too_many_arguments)]
    fn batch_full(
        base_offset: i64,
        count: i32,
        producer_id: i64,
        is_transactional: bool,
        is_control_batch: bool,
    ) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_full(
            512,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            base_offset,
            -1, // log_append_time
            producer_id,
            0, // producer_epoch
            0, // base_sequence
            is_transactional,
            is_control_batch,
            -1, // partition_leader_epoch
            512,
        );
        for i in 0..count {
            let offset = base_offset + i as i64;
            let value = format!("value-{offset}");
            builder.append_with_offset_bytes(offset, 0, None, Some(value.as_bytes()));
        }
        builder.build().buffer().to_vec()
    }

    /// Builds a [`PartitionData`] declaring an aborted transaction for
    /// `producer_id` starting at `first_offset`, carrying `records_bytes`.
    fn partition_data_with_aborted_txn(records_bytes: Vec<u8>, producer_id: i64, first_offset: i64) -> PartitionData {
        let mut txn = AbortedTransaction::new();
        txn.set_producer_id(producer_id);
        txn.set_first_offset(first_offset);
        let mut partition_data = PartitionData::new();
        partition_data.set_records(Some(bytes::Bytes::from(records_bytes)));
        partition_data.set_aborted_transactions(Some(vec![txn]));
        partition_data
    }

    /// Issue-1 regression: a batch whose header declares MORE records than are
    /// actually present ("too many") must surface a recoverable [`Error`]
    /// through the receive-path cursor — NOT panic via `.expect`. The old
    /// `iter_records()` path validated this; the new incremental cursor must
    /// too. Mirrors Java `DefaultRecordBatch.RecordIterator` reading past EOF
    /// (`InvalidRecordException`, "...premature EOF reached"), which is
    /// CRC-independent, so we drive it under `check.crcs=false`.
    #[test]
    fn test_invalid_record_count_too_many_through_fetch_records() {
        // 3 real records, header declares 5.
        let bytes = batch_with_overridden_record_count(0, 3, 5);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, false);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;

        // The 3 real records decode first, THEN the premature-EOF fault is
        // raised while advancing. Java's `catch (KafkaException e)` swallows it
        // because `records` is non-empty (`CompletedFetch.java:294-300`) and
        // returns the prefix; the error is cached and `corruptLastRecord` stays
        // set, so the NEXT call raises. Propagating on this call instead would
        // discard 3 already-decoded records whose positions have advanced.
        let first = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .expect("records already decoded must be returned, not discarded");
        assert_eq!(3, first.len(), "the 3 real records are returned before the fault surfaces");

        // Second call: the cached fault surfaces, wrapped in Java's
        // "seek past the record" message with the original as the cause.
        let err = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .expect_err("the cached premature-EOF fault must surface on the next call");
        // Java: `throw new KafkaException("Received exception when fetching the
        // next record from " + partition + ". If needed, please seek past the
        // record to continue consumption.", e)` (`CompletedFetch.java:257`).
        // §2 bars that word from Rust message text, so ours says "an error";
        // the strings are otherwise identical.
        assert_eq!(
            "Received an error when fetching the next record from test-0. \
             If needed, please seek past the record to continue consumption.",
            err.message(),
            "Java wraps the cached fault in this message, reworded per §2 (see the comment above)"
        );
        let cause = std::error::Error::source(&err).expect("the original fault must be the cause");
        assert!(
            cause.to_string().contains("premature EOF"),
            "cause must be the premature-EOF fault, got: {cause}"
        );
        // `is_kafka_error()` must hold so `FetchCollector`'s swallow guard applies.
        assert!(err.is_kafka_error(), "must be a Kafka error: {err:?}");
        // Recoverable, not fatal: propagates out of poll() rather than aborting.
        assert!(
            !crate::common::requests::request_utils::is_fatal_error(&err),
            "invalid-record-count error must be recoverable"
        );
    }

    /// Issue-1 regression: a batch whose header declares FEWER records than are
    /// actually present ("too little") must surface a recoverable
    /// [`Error`] — NOT silently drop the trailing valid records. Mirrors
    /// Java `ensureNoneRemaining()` ("...records still remaining"), which is
    /// CRC-independent, so we drive it under `check.crcs=false`.
    #[test]
    fn test_invalid_record_count_too_little_through_fetch_records() {
        // 3 real records, header declares 2 — the 3rd would be silently lost
        // under the buggy cursor.
        let bytes = batch_with_overridden_record_count(0, 3, 2);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, false);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;

        // Pull the first (valid) records, then the next call must error rather
        // than reporting exhaustion (which would be a silent drop).
        let mut saw_error = false;
        let mut total = 0;
        for _ in 0..5 {
            match cf.fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 1) {
                Ok(batch) => {
                    if batch.is_empty() {
                        break;
                    }
                    total += batch.len();
                },
                Err(e) => {
                    // Nothing decoded on THIS call, so Java's
                    // `catch (KafkaException e)` propagates — wrapped in the
                    // "seek past the record" message with the real fault as the
                    // cause (`CompletedFetch.java:294-300`). Java's literal text is
                    // "Received exception when fetching the next record from ..."
                    // (`:297`); §2 bars that word, so ours says "an error".
                    assert_eq!(
                        "Received an error when fetching the next record from test-0. \
                         If needed, please seek past the record to continue consumption.",
                        e.message(),
                        "Java wraps a records-empty failure in this message, reworded per §2 (see the comment above)"
                    );
                    let cause = std::error::Error::source(&e).expect("the real fault must be the cause");
                    assert!(
                        cause.to_string().contains("records still remaining") && cause.to_string().contains("test-0"),
                        "cause must be the ensureNoneRemaining fault, got: {cause}"
                    );
                    assert!(
                        !crate::common::requests::request_utils::is_fatal_error(&e),
                        "invalid-record-count error must be recoverable"
                    );
                    saw_error = true;
                    break;
                },
            }
        }
        assert!(saw_error, "declared count < actual must error, not silently drop records");
        assert_eq!(2, total, "only the declared-count records are returned before the error");
    }

    /// Builds a v2 batch whose attributes carry the control-batch flag, holding
    /// `count` records (the producer builder forbids appending genuine control
    /// records, so we build an ordinary data batch and flip the control-flag
    /// bit in the attributes, recomputing the CRC so the batch stays valid under
    /// `check.crcs=true`). The cursor's `is_control_batch` branch keys off this
    /// flag, which is what the test exercises.
    fn control_batch(base_offset: i64, count: i32, producer_id: i64) -> Vec<u8> {
        const CONTROL_FLAG_MASK: u8 = 0x20;
        let mut buf = batch_full(base_offset, count, producer_id, true, false);
        // Attributes is an i16 at ATTRIBUTES_OFFSET; the control flag lives in
        // the low byte (big-endian, so the last of the two bytes).
        let attr_lo = RecordBatch::ATTRIBUTES_OFFSET + 1;
        buf[attr_lo] |= CONTROL_FLAG_MASK;
        // Recompute the CRC over [ATTRIBUTES_OFFSET..] (single-batch buffer).
        let crc = crc32c::crc32c(&buf[RecordBatch::ATTRIBUTES_OFFSET..]);
        buf[RecordBatch::CRC_OFFSET..RecordBatch::CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        buf
    }

    /// Issue-2: a control batch interleaved between two data batches in a
    /// single multi-batch fetch payload must be skipped (its records are not
    /// returned to the user) while the surrounding data records are returned in
    /// offset order. Exercises the `is_control_batch` branch of
    /// `advance_to_next_fetched_record` through the incremental cursor.
    #[test]
    fn test_control_batch_skipped_mid_payload() {
        let mut buf = Vec::new();
        // Data batch: offsets 0..=1.
        buf.extend_from_slice(&batch_full(0, 2, RecordBatch::NO_PRODUCER_ID, false, false));
        // Control batch in the middle: offset 2 (1 control record). Needs a
        // producer id (control batches are transactional control markers).
        buf.extend_from_slice(&control_batch(2, 1, 1000));
        // Data batch: offsets 3..=4.
        buf.extend_from_slice(&batch_full(3, 2, RecordBatch::NO_PRODUCER_ID, false, false));

        let mut cf = new_completed_fetch(0, buf);
        // READ_UNCOMMITTED: control batches are still skipped at the record
        // level (Java does not return control records to the user).
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;

        let mut collected: Vec<ConsumerRecord<String, String>> = Vec::new();
        loop {
            let batch = cf
                .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
                .unwrap();
            if batch.is_empty() {
                break;
            }
            collected.extend(batch);
        }

        // Offsets 0,1,3,4 returned; control offset 2 skipped.
        let offsets: Vec<i64> = collected.iter().map(|r| r.offset()).collect();
        assert_eq!(
            vec![0, 1, 3, 4],
            offsets,
            "control batch must be skipped, surrounding data in order"
        );
        assert_eq!(Some(&"value-0".to_string()), collected[0].value());
        assert_eq!(Some(&"value-4".to_string()), collected[3].value());
    }

    /// Issue-2: an aborted-transaction batch interleaved between two committed
    /// data batches in a single fetch payload must be skipped under
    /// READ_COMMITTED (matching Java's `isBatchAborted`), while surrounding
    /// committed records are returned in order. Exercises the
    /// aborted-transaction `next_batch_start`-advance path of
    /// `load_next_batch` through the incremental cursor.
    #[test]
    fn test_aborted_transaction_batch_skipped_mid_payload() {
        let aborted_pid = 42;
        let mut buf = Vec::new();
        // Committed (non-transactional) data batch: offsets 0..=1.
        buf.extend_from_slice(&batch_full(0, 2, RecordBatch::NO_PRODUCER_ID, false, false));
        // Aborted transactional data batch from `aborted_pid`: offsets 2..=3.
        buf.extend_from_slice(&batch_full(2, 2, aborted_pid, true, false));
        // Committed (non-transactional) data batch: offsets 4..=5.
        buf.extend_from_slice(&batch_full(4, 2, RecordBatch::NO_PRODUCER_ID, false, false));

        let partition_data = partition_data_with_aborted_txn(buf, aborted_pid, 2);
        let mut cf = CompletedFetch::new_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data,
            test_aggregator(),
            0,
        );
        let fetch_config = make_fetch_config(IsolationLevel::ReadCommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;

        let mut collected: Vec<ConsumerRecord<String, String>> = Vec::new();
        loop {
            let batch = cf
                .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
                .unwrap();
            if batch.is_empty() {
                break;
            }
            collected.extend(batch);
        }

        // Offsets 0,1,4,5 returned; aborted offsets 2,3 skipped.
        let offsets: Vec<i64> = collected.iter().map(|r| r.offset()).collect();
        assert_eq!(
            vec![0, 1, 4, 5],
            offsets,
            "aborted batch must be skipped, committed data in order"
        );
        assert_eq!(Some(&"value-0".to_string()), collected[0].value());
        assert_eq!(Some(&"value-5".to_string()), collected[3].value());
    }

    /// Translated from `CompletedFetchTest.testNegativeFetchCount`.
    #[test]
    fn test_negative_fetch_count() {
        let bytes = new_records(0, 10, 0);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;
        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, -10)
            .unwrap();
        assert_eq!(0, records.len());
    }

    /// Translated from `CompletedFetchTest.testNoRecordsInFetch`.
    #[test]
    fn test_no_records_in_fetch() {
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(10);
        partition_data.set_last_stable_offset(20);
        partition_data.set_log_start_offset(0);
        // records is None (Java sets it to null) — but the auto-generated
        // setter also accepts None.
        partition_data.set_records(None);
        let mut cf = CompletedFetch::new_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data,
            test_aggregator(),
            1,
        );
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;
        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(0, records.len());
    }

    /// Failing deserializer on the key returns an error on the first call
    /// (no records decoded yet), then caches the error so subsequent calls
    /// raise without advancing.
    #[test]
    fn test_key_deserialization_failure_caches_error() {
        let bytes = new_records(0, 3, 0);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, false);
        let key_de = FailingDeserializer;
        let value_de = StringDeserializer;
        let err = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap_err();
        assert!(err.message().contains("KEY"), "{}", err.message());
        // Subsequent call should re-raise.
        let err2 = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap_err();
        // The cached path returns the cached error, which embeds the
        // same "Error deserializing KEY" prefix.
        assert!(err2.message().contains("KEY"), "{}", err2.message());
    }

    /// Translated from `CompletedFetchTest.testCorruptedMessage`.
    ///
    /// # What this asserts vs Java
    ///
    /// Java asserts the structured fields on `RecordDeserializationException`:
    /// `origin` (KEY/VALUE), `offset`, `topicPartition`, `timestamp`,
    /// `timestampType`, the raw `keyBuffer`/`valueBuffer` bytes, and `headers`.
    /// The Rust port builds the same
    /// [`Error::RecordDeserialization`](crate::common::Error::RecordDeserialization)
    /// (see `wrap_deserialization_error`), so all of them are asserted here:
    ///
    ///   - **origin** — `Key`/`Value`, both as the typed field and in the message
    ///   - **offset**, **partition**, **timestamp**, **timestamp type**
    ///   - **raw key/value buffers** — the offending record's original bytes
    ///   - **headers**
    ///   - **cause** — the deserializer's own error, via `source()`
    ///   - **cached re-raise** — subsequent calls re-raise
    ///
    /// The Java test's `KEY` case fails on the SECOND record after the
    /// first one decodes successfully. The Rust port models this with a
    /// `MaybeFailingDeserializer` that fails on a specific offset.
    #[test]
    fn test_corrupted_message_key_fails_after_valid_record() {
        // Three records: offsets 0, 1, 2. Key deserializer fails on
        // offset 1; the first record at offset 0 should decode
        // successfully and be returned, then the second call should
        // re-raise the cached exception with KEY origin (the cached
        // record is re-deserialized; same offset bytes ⇒ same failure).
        let bytes = new_records_with_keyed_offsets(0, 3, 0);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadCommitted, false);
        let key_de = MaybeFailingDeserializer::new(DeserializationOriginFlag::Key, 1);
        let value_de = StringDeserializer;

        // First call: offset 0 decodes; offset 1 fails and is cached.
        // Since `out` is non-empty, the call returns the prefix [0] and
        // does NOT propagate the error.
        let first = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(1, first.len(), "expected one valid record before the failure");
        assert_eq!(0, first[0].offset());

        // Second call: cached exception re-raises with KEY origin and
        // the failed offset (1) in the message.
        let err = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap_err();
        let msg = err.message();
        assert!(msg.contains("KEY"), "expected KEY origin: {msg}");
        assert!(msg.contains(" 1"), "expected failed offset 1 in message: {msg}");
        assert!(msg.contains("test-0"), "expected partition string: {msg}");

        // Java's structured `RecordDeserializationException` fields — the whole
        // reason the class exists, since consumer error handlers read them to
        // decide whether to skip the record.
        let Error::RecordDeserialization(rde) = &err else {
            panic!("expected Error::RecordDeserialization, got: {err:?}");
        };
        assert_eq!(Some(DeserializationErrorOrigin::Key), rde.origin());
        assert_eq!(&TopicPartition::new("test", 0), rde.topic_partition());
        assert_eq!(1, rde.offset());
        // Timestamp type is a batch-level property, so it matches the record
        // that decoded successfully from the same batch.
        assert_eq!(first[0].timestamp_type(), rde.timestamp_type());
        assert!(rde.timestamp() >= 0, "the record timestamp must be carried");
        // Raw buffers of the offending record are carried, not just the message.
        assert!(rde.key_buffer().is_some(), "the raw key buffer must be carried");
        assert!(rde.value_buffer().is_some(), "the raw value buffer must be carried");
        assert!(rde.headers().is_some(), "the record headers must be carried");
        // `RecordDeserializationException extends SerializationException extends
        // KafkaException`, so the hierarchy predicate still agrees.
        assert!(err.is_kafka_error(), "must remain a Kafka error: {err:?}");
        // The deserializer's own error is the cause.
        assert!(
            std::error::Error::source(&err).is_some(),
            "the deserializer's error must be the cause: {err:?}"
        );
    }

    /// Mirrors `CompletedFetchTest.testCorruptedMessage`'s VALUE case
    /// (different `CompletedFetch` instance, value deserializer fails on
    /// offset 3).
    #[test]
    fn test_corrupted_message_value_fails_after_valid_record() {
        // Same fixture, different fetch-offset (2) so the cursor sees
        // offsets 2..=4 and the value deserializer fails on offset 3.
        let bytes = new_records_with_keyed_offsets(0, 5, 0);
        let mut cf = new_completed_fetch(2, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadCommitted, false);
        let key_de = StringDeserializer;
        let value_de = MaybeFailingDeserializer::new(DeserializationOriginFlag::Value, 3);

        let first = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(1, first.len(), "expected one valid record before the failure");
        assert_eq!(2, first[0].offset());

        let err = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap_err();
        let msg = err.message();
        assert!(msg.contains("VALUE"), "expected VALUE origin: {msg}");
        assert!(msg.contains(" 3"), "expected failed offset 3 in message: {msg}");
        assert!(msg.contains("test-0"), "expected partition string: {msg}");

        // Cached-exception re-raise: a third call re-raises the same error
        // (Java re-raises until the user seeks past the offset).
        let err3 = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap_err();
        assert!(
            err3.message().contains("VALUE"),
            "cached re-raise must keep VALUE origin: {}",
            err3.message()
        );
    }

    /// Translated from `CompletedFetchTest.testAbortedTransactionRecordsRemoved`.
    ///
    /// Direct port of the simple aborted-transaction contract (the existing
    /// `test_aborted_transaction_batch_skipped_mid_payload` only covers it
    /// indirectly via a multi-batch fixture):
    ///   - READ_COMMITTED: an aborted transactional batch yields 0 records.
    ///   - READ_UNCOMMITTED: the SAME batch yields all `num_records`.
    ///
    /// # Deviation from Java's control-marker layout (documented)
    ///
    /// Java's `newTranscactionalRecords` appends an `EndTransactionMarker`
    /// control batch after the data batch. The Rust `CompletedFetch` does not
    /// translate `ControlRecordType` / `containsAbortMarker` (see module
    /// docstring) and rejects a control batch from a previously-aborted
    /// producer with `UnsupportedVersion`. We therefore use a plain
    /// transactional data batch (no control marker); the abort is driven by the
    /// response's `aborted_transactions` list, which is the same mechanism
    /// `isBatchAborted` consults. The record-count contract is identical.
    #[test]
    fn test_aborted_transaction_records_removed_direct() {
        const PRODUCER_ID: i64 = 1000;
        let num_records = 10;

        // READ_COMMITTED: aborted batch ⇒ 0 records.
        {
            let buf = batch_full(0, num_records, PRODUCER_ID, true, false);
            let partition_data = partition_data_with_aborted_txn(buf, PRODUCER_ID, 0);
            let mut cf = CompletedFetch::new_full(
                make_subscriptions(),
                Arc::new(BufferSupplier::create()),
                tp("test", 0),
                partition_data,
                test_aggregator(),
                0,
            );
            let fetch_config = make_fetch_config(IsolationLevel::ReadCommitted, true);
            let key_de = StringDeserializer;
            let value_de = StringDeserializer;
            let records = cf
                .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
                .unwrap();
            assert_eq!(0, records.len(), "READ_COMMITTED must remove aborted records");
        }

        // READ_UNCOMMITTED: same aborted batch ⇒ all num_records returned.
        {
            let buf = batch_full(0, num_records, PRODUCER_ID, true, false);
            let partition_data = partition_data_with_aborted_txn(buf, PRODUCER_ID, 0);
            let mut cf = CompletedFetch::new_full(
                make_subscriptions(),
                Arc::new(BufferSupplier::create()),
                tp("test", 0),
                partition_data,
                test_aggregator(),
                0,
            );
            let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
            let key_de = StringDeserializer;
            let value_de = StringDeserializer;
            let records = cf
                .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
                .unwrap();
            assert_eq!(
                num_records as usize,
                records.len(),
                "READ_UNCOMMITTED must return all aborted records"
            );
        }
    }

    /// Translated from `CompletedFetchTest.testCommittedTransactionRecordsIncluded`.
    ///
    /// A COMMITTED transactional batch (no entry in the response's
    /// `aborted_transactions` list) returns all its records under
    /// READ_COMMITTED. Direct port of the simple contract (previously only
    /// covered indirectly by the control-batch mid-payload fixture).
    ///
    /// As with the aborted case, Java appends an `EndTransactionMarker` COMMIT
    /// control batch; the Rust port omits the control marker (no
    /// `ControlRecordType` translation) and relies on the absence of an
    /// aborted-txn entry to mark the batch committed — the same observable
    /// record-count contract.
    #[test]
    fn test_committed_transaction_records_included_direct() {
        const PRODUCER_ID: i64 = 1000;
        let num_records = 10;
        // Transactional batch, NOT in any aborted-txn list ⇒ committed.
        let buf = batch_full(0, num_records, PRODUCER_ID, true, false);
        let mut partition_data = PartitionData::new();
        partition_data.set_records(Some(bytes::Bytes::from(buf)));
        let mut cf = CompletedFetch::new_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data,
            test_aggregator(),
            0,
        );
        let fetch_config = make_fetch_config(IsolationLevel::ReadCommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;
        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(
            num_records as usize,
            records.len(),
            "READ_COMMITTED must include committed transaction records"
        );
    }

    /// `drain` is idempotent and clears the consumed state.
    #[test]
    fn test_drain_idempotent() {
        let bytes = new_records(0, 3, 0);
        let mut cf = new_completed_fetch(0, bytes);
        assert!(!cf.is_consumed());
        cf.drain();
        assert!(cf.is_consumed());
        cf.drain(); // no panic
    }

    /// next_fetch_offset advances after each record is decoded, and
    /// jumps to the end-of-batch offset when exhausted (Java's
    /// `nextFetchOffset = currentBatch.nextOffset()` for offset gap
    /// recovery).
    #[test]
    fn test_next_fetch_offset_advances() {
        let bytes = new_records(100, 5, 0);
        let mut cf = new_completed_fetch(100, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let key_de = StringDeserializer;
        let value_de = StringDeserializer;
        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 3)
            .unwrap();
        assert_eq!(3, records.len());
        assert_eq!(103, cf.next_fetch_offset());
        // Exhaust the rest.
        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 100)
            .unwrap();
        assert_eq!(2, records.len());
        // After full drain, next offset should equal last record offset + 1 (104+1=105).
        assert_eq!(105, cf.next_fetch_offset());
        assert!(cf.is_consumed());
    }

    /// Counting deserializer: records how many times it was invoked so a test
    /// can assert an invocation that Java never makes.
    struct CountingDeserializer {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        fail: bool,
    }
    impl Deserializer<String> for CountingDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail {
                return Err(Error::serialization("simulated failure"));
            }
            String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(e.to_string()))
        }
    }

    /// The VALUE deserializer must not run for a record whose KEY failed.
    ///
    /// Java's `parseRecord` is two sequential `try` blocks and the first one's
    /// `catch` *throws* (`CompletedFetch.java:313-328`), so control never
    /// reaches the value block. Running it anyway is observable: a user
    /// deserializer may count, cache, log, or charge for work on a record Java
    /// never hands it — so the skip is asserted, not just documented.
    #[test]
    fn test_value_deserializer_not_invoked_when_key_fails() {
        let key_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let value_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let key_de = CountingDeserializer { calls: Arc::clone(&key_calls), fail: true };
        let value_de = CountingDeserializer { calls: Arc::clone(&value_calls), fail: false };

        let bytes = new_records(0, 3, 0);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, false);
        let err = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap_err();
        assert!(err.message().contains("KEY"), "{}", err.message());

        assert_eq!(
            1,
            key_calls.load(std::sync::atomic::Ordering::SeqCst),
            "the key deserializer runs once, for the first record"
        );
        assert_eq!(
            0,
            value_calls.load(std::sync::atomic::Ordering::SeqCst),
            "the value deserializer must not see a record whose key failed"
        );
    }

    /// The complement: when the KEY succeeds, the VALUE deserializer does run,
    /// so the skip above cannot be an unconditional short-circuit.
    #[test]
    fn test_value_deserializer_invoked_when_key_succeeds() {
        let value_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let key_de = StringDeserializer;
        let value_de = CountingDeserializer { calls: Arc::clone(&value_calls), fail: false };

        let bytes = new_records(0, 3, 0);
        let mut cf = new_completed_fetch(0, bytes);
        let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, false);
        let records = cf
            .fetch_records::<String, String>(&fetch_config, &key_de, &value_de, 10)
            .unwrap();
        assert_eq!(3, records.len());
        assert_eq!(3, value_calls.load(std::sync::atomic::Ordering::SeqCst));
    }
}
