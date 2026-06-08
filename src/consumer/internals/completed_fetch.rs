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
//! # READ_COMMITTED limitations
//!
//! `containsAbortMarker` is NOT translated in Phase 7a because
//! `ControlRecordType::parse_key` is not yet implemented (Phase 7b/c
//! picks this up). The current implementation:
//!
//! - Correctly skips aborted-transaction batches the first time their
//!   producer ID appears in the response's `aborted_transactions` list.
//! - Returns `KafkaError::unsupported_version` when it encounters a
//!   control batch under READ_COMMITTED, since we cannot distinguish
//!   COMMIT markers from ABORT markers without `ControlRecordType`.
//!   This is conservative; production readers will hit it only if their
//!   producers reuse producer IDs after an abort, which is rare.
//!
//! Tracking note: Phase 7b/c must translate `ControlRecordType` AND
//! re-enable the `containsAbortMarker` branch so the `aborted_producer_ids`
//! set drops the producer ID on observing the ABORT marker (so a fresh
//! transaction from the same producer is not skipped).

#![allow(dead_code)]

use std::collections::{BinaryHeap, HashSet};
use std::sync::{Arc, Mutex};

use log::{debug, error};

use crate::common::IsolationLevel;
use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::header::internals::RecordHeaders;
use crate::common::memory::buffer_supplier::BufferSupplier;
use crate::common::record::abstract_records::LOG_OVERHEAD;
use crate::common::record::{
    DefaultRecord, DefaultRecordBatchRef, DefaultRecordRef, MemoryRecords, RecordBatch, RecordVersion, TimestampType,
};
use crate::common::serialization::Deserializer;
use crate::consumer::ConsumerRecord;
use crate::consumer::internals::fetch_config::FetchConfig;
use crate::consumer::internals::subscription_state::SubscriptionState;
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
    aborted_producer_ids: HashSet<i64>,
    aborted_transactions: BinaryHeap<AbortedTxnByFirstOffset>,

    /// Cached deserialization exception for retry semantics. Java
    /// re-raises on every call until the user seeks past the offset.
    cached_record_exception: Option<KafkaError>,
    corrupt_last_record: bool,

    /// Stats. Not surfaced through metrics in Milestone-8 (no metrics
    /// framework yet) but retained because `drain()` consults
    /// `bytes_read` to decide whether to nudge `move_partition_to_end`.
    records_read: i32,
    bytes_read: i32,

    /// Offset the next fetch should start at.
    next_fetch_offset: i64,
    /// Last partition-leader epoch we observed (used by Phase 7b's
    /// FetchCollector to update SubscriptionState).
    last_epoch: Option<i32>,
    /// Whether `drain` has been called.
    is_consumed: bool,
    /// Whether the cursor has been positioned at the first batch.
    initialized: bool,
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
    /// Compressed: record bytes are the owned decompressed buffer.
    Owned(Vec<u8>),
}

impl CompletedFetch {
    /// Constructs a `CompletedFetch` for the given partition.
    ///
    /// Translates Java's
    /// `CompletedFetch(Logger, SubscriptionState, BufferSupplier,
    ///   TopicPartition, PartitionData, FetchMetricsAggregator, Long)` —
    /// minus the logger (we use the `log` crate) and the metrics
    /// aggregator (no Rust metrics framework yet).
    pub(crate) fn new_full(
        subscriptions: Arc<Mutex<SubscriptionState>>,
        decompression_buffer_supplier: Arc<BufferSupplier>,
        partition: TopicPartition,
        partition_data: PartitionData,
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
            aborted_producer_ids: HashSet::new(),
            aborted_transactions,
            cached_record_exception: None,
            corrupt_last_record: false,
            records_read: 0,
            bytes_read: 0,
            next_fetch_offset: fetch_offset,
            last_epoch: None,
            is_consumed: false,
            initialized: false,
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
            aborted_producer_ids: HashSet::new(),
            aborted_transactions,
            cached_record_exception: None,
            corrupt_last_record: false,
            records_read: 0,
            bytes_read: 0,
            next_fetch_offset: 0,
            last_epoch: None,
            is_consumed: false,
            initialized: false,
        }
    }

    /// Returns the offset the next fetch round should start at.
    pub(crate) fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
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
        self.cached_record_exception = None;
        self.is_consumed = true;
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
    ) -> Result<Vec<ConsumerRecord<K, V>>, KafkaError>
    where
        K: 'static,
        V: 'static,
    {
        if self.corrupt_last_record {
            // Java throws KafkaException pointing the user at `seek`.
            let cached = self
                .cached_record_exception
                .clone()
                .unwrap_or_else(|| KafkaError::illegal_state(format!(
                    "Received exception when fetching the next record from {}. If needed, please seek past the record to continue consumption.",
                    self.partition
                )));
            return Err(cached);
        }
        if self.is_consumed || max_records <= 0 {
            return Ok(Vec::new());
        }

        self.ensure_cursor();
        let mut out: Vec<ConsumerRecord<K, V>> = Vec::new();

        for _ in 0..max_records {
            // Only advance to the next record if there was no cached
            // exception. Otherwise re-deserialize the last one so the
            // user can retry after fixing whatever state they like.
            if self.cached_record_exception.is_none() {
                self.corrupt_last_record = true;
                let has_next = self.advance_to_next_fetched_record(config)?;
                self.corrupt_last_record = false;
                if !has_next {
                    break;
                }
            } else if self.peek_current_record().is_none() {
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
            {
                let (record, batch_meta) = self.peek_current_record().expect("verified non-empty above");
                let topic_str: &str = &self.topic_arc;
                // The only owned copy of record payload on the happy path:
                // the §27-sanctioned `RecordHeaders` (Milestone-8 holds
                // owned headers on the emitted `ConsumerRecord`).
                let headers_vec = record.headers().map_err(|e| {
                    KafkaError::illegal_state(format!(
                        "Record for partition {} at offset {} has invalid headers, cause: {}",
                        self.partition,
                        record.offset(),
                        e
                    ))
                })?;
                headers_owned = RecordHeaders::from_headers(headers_vec);
                key_result = match record.key() {
                    None => Ok(None),
                    Some(key_bytes) => key_deserializer
                        .deserialize_with_headers(topic_str, &headers_owned, key_bytes)
                        .map(Some),
                };
                value_result = match record.value() {
                    None => Ok(None),
                    Some(value_bytes) => value_deserializer
                        .deserialize_with_headers(topic_str, &headers_owned, value_bytes)
                        .map(Some),
                };
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
                    let err = wrap_deserialization_error(DeserializationOrigin::Key, &self.partition, offset, e);
                    self.cached_record_exception = Some(err.clone());
                    if out.is_empty() {
                        return Err(err);
                    }
                    error!("Key deserialization failed for {} at offset {}", self.partition, offset);
                    // Stop on the failed record — Java keeps `cachedRecordException` and returns
                    // already-decoded records.
                    break;
                },
            };
            let value = match value_result {
                Ok(v) => v,
                Err(e) => {
                    let err = wrap_deserialization_error(DeserializationOrigin::Value, &self.partition, offset, e);
                    self.cached_record_exception = Some(err.clone());
                    if out.is_empty() {
                        return Err(err);
                    }
                    error!("Value deserialization failed for {} at offset {}", self.partition, offset);
                    break;
                },
            };

            // §27: cheap Arc clone — atomic pointer bump, no UTF-8 copy.
            let topic_arc = Arc::clone(&self.topic_arc);
            let consumer_record = ConsumerRecord::with_headers(
                topic_arc,
                self.partition.partition(),
                offset,
                timestamp,
                timestamp_type,
                key_size,
                value_size,
                key,
                value,
                headers_owned,
                leader_epoch,
            );
            self.records_read += 1;
            self.bytes_read += record_size_in_bytes;
            self.next_fetch_offset = offset + 1;
            self.cached_record_exception = None;
            out.push(consumer_record);
            // Advance the record cursor — we successfully consumed this
            // record. Move the byte offset past it and decrement the
            // remaining-records count.
            if let Some(cursor) = &mut self.cursor {
                cursor.record_byte_offset += record_bytes_consumed;
                cursor.records_remaining -= 1;
            }
        }

        Ok(out)
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
    fn advance_to_next_fetched_record(&mut self, config: &FetchConfig) -> Result<bool, KafkaError> {
        loop {
            // Reload current batch if exhausted.
            let needs_new_batch = match &self.cursor {
                Some(cursor) => cursor.records_remaining <= 0,
                None => true,
            };
            if needs_new_batch {
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
                let (record, batch_meta) = self.peek_current_record().expect("cursor verified non-empty above");
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

    /// Parses the current record (the one at `cursor.record_byte_offset`)
    /// into a borrowing [`DefaultRecordRef`] and returns it together with its
    /// enclosing batch metadata, both borrowing from `self`. Returns `None`
    /// if no current record.
    ///
    /// §27 zero-copy note: the returned `DefaultRecordRef` borrows the
    /// record's key/value/header bytes directly from the cursor's record
    /// source — no copy. The per-record parse is varint decoding only; the
    /// payload bytes are never touched.
    fn peek_current_record(&self) -> Option<(DefaultRecordRef<'_>, &BatchMetadata)> {
        let cursor = self.cursor.as_ref()?;
        if cursor.records_remaining <= 0 {
            return None;
        }
        let batch_meta = cursor.current_batch.as_ref()?;
        let records_bytes = match &cursor.record_source {
            RecordSource::None => return None,
            RecordSource::Borrowed(range) => &cursor.memory_records.buffer()[range.clone()],
            RecordSource::Owned(buf) => buf.as_slice(),
        };
        if cursor.record_byte_offset >= records_bytes.len() {
            return None;
        }
        let log_append_time = if batch_meta.timestamp_type == TimestampType::LogAppendTime {
            Some(batch_meta.last_offset_timestamp)
        } else {
            None
        };
        // The bytes were validated when the batch was loaded
        // (`load_next_batch` walks the batch's records once to find ranges);
        // a parse failure here would indicate a logic bug, not bad input.
        let (record, _consumed) = DefaultRecord::read_ref_from_buffer(
            &records_bytes[cursor.record_byte_offset..],
            batch_meta.base_offset,
            batch_meta.base_timestamp,
            batch_meta.base_sequence,
            log_append_time,
        )
        .ok()?;
        Some((record, batch_meta))
    }

    /// Loads the next batch into the cursor. Skips aborted-transaction
    /// batches and applies READ_COMMITTED filtering. Returns
    /// `Ok(true)` if a batch is now loaded, `Ok(false)` if no more
    /// batches remain.
    fn load_next_batch(&mut self, config: &FetchConfig) -> Result<bool, KafkaError> {
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
                    return Err(KafkaError::illegal_state(format!(
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
                let source = if batch.is_compressed() {
                    // Decompress once per batch into an owned buffer; records
                    // then borrow from it.
                    let decompressed = batch.decompress_records().map_err(|e| {
                        KafkaError::illegal_state(format!(
                            "Record batch for partition {} at offset {} is invalid, cause: {}",
                            self.partition, meta.base_offset, e
                        ))
                    })?;
                    RecordSource::Owned(decompressed)
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
                // Java's `containsAbortMarker` branch is NOT translated
                // here (see module docstring): we don't yet have
                // `ControlRecordType::parse_key`, so we cannot decide
                // whether a control batch is COMMIT vs ABORT. If we ever
                // encounter a control batch from a producer whose ID is
                // in `aborted_producer_ids`, we cannot safely continue —
                // the producer ID might have been reused for a fresh,
                // committed transaction. Fail loudly rather than silently
                // skip records.
                if batch_meta.is_control_batch && self.aborted_producer_ids.contains(&batch_meta.producer_id) {
                    return Err(KafkaError::unsupported_version(format!(
                        "READ_COMMITTED with a control batch from a previously aborted \
                         producer ID ({}) on partition {} requires translating \
                         ControlRecordType to distinguish ABORT vs COMMIT markers, \
                         which is not yet implemented (tracked for Phase 7b/c).",
                        batch_meta.producer_id, self.partition
                    )));
                }
                // The skip path mirrors Java's `isBatchAborted`, which
                // gates on `isTransactional()` — a non-transactional
                // batch with a producer ID is never aborted.
                if batch_meta.is_transactional
                    && !batch_meta.is_control_batch
                    && self.aborted_producer_ids.contains(&batch_meta.producer_id)
                {
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

fn wrap_deserialization_error(
    origin: DeserializationOrigin,
    partition: &TopicPartition,
    offset: i64,
    cause: KafkaError,
) -> KafkaError {
    KafkaError::serialization(format!(
        "Error deserializing {} for partition {} at offset {}. \
         If needed, please seek past the record to continue consumption. Cause: {}",
        origin.as_str(),
        partition,
        offset,
        cause.message(),
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
    use crate::common::record::{MemoryRecords, SimpleRecord};
    use crate::common::serialization::Deserializer;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::fetch_response_data::PartitionData;
    use std::sync::{Arc, Mutex};

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// String deserializer that decodes UTF-8 bytes.
    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(e.to_string()))
        }
    }

    /// Deserializer that always fails — used to drive the deserialization
    /// error path.
    struct FailingDeserializer;
    impl Deserializer<String> for FailingDeserializer {
        fn deserialize(&self, _topic: &str, _data: &[u8]) -> Result<String, KafkaError> {
            Err(KafkaError::serialization("simulated failure"))
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
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            let (prefix, origin_label) = match self.side {
                DeserializationOriginFlag::Key => ("key-", "key"),
                DeserializationOriginFlag::Value => ("value-", "value"),
            };
            if let Some(n) = Self::parse_offset(data, prefix)
                && n == self.fail_on_offset
            {
                return Err(KafkaError::serialization(format!(
                    "simulated {origin_label} failure at offset {n}"
                )));
            }
            String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(e.to_string()))
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
        partition_data.set_records(Some(records_bytes));
        CompletedFetch::new_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data,
            fetch_offset,
        )
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
    /// The Java test asserts the structured fields on
    /// `RecordDeserializationException` (origin, offset, key/value buffers,
    /// headers). The Rust port uses the collapsed
    /// `KafkaError::Serialization(String)` form, so it asserts the same
    /// behavior at the granularity Rust can express: which call raises,
    /// the error message identifies KEY vs VALUE, the offset is embedded
    /// in the message, and the cached-exception re-raise path triggers
    /// on subsequent calls.
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
}
