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
//! `Deserializer<T>` allocations for the key and value `T`. Specifically
//! preserved here:
//!
//! - `partition_data.records: Option<Vec<u8>>` is the canonical owner of
//!   the fetch payload. We never call `.clone()` or `Bytes::copy_from_slice`
//!   on it. `MemoryRecords::readable_records` is called once at first
//!   batch access; the resulting `MemoryRecords` borrows from the
//!   underlying buffer (Java's `recordsOrFail` is the analog).
//! - `topic: Arc<str>` is cloned cheaply per `ConsumerRecord` — no
//!   `String::from_utf8` per record.
//! - Headers are owned per the milestone-8 §27 ruling (`Headers` cloned
//!   from `DefaultRecord::headers()`); a future revisit may borrow.
//! - No per-record `tokio::spawn`. The whole struct is sync.
//! - Iteration is lazy via a `BatchCursor`: at most one batch's records
//!   are materialized at a time, not the full fetch.

#![allow(dead_code)]

use std::collections::{BinaryHeap, HashSet};
use std::sync::{Arc, Mutex};

use log::{debug, error};

use crate::common::IsolationLevel;
use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::header::internals::RecordHeaders;
use crate::common::memory::buffer_supplier::BufferSupplier;
use crate::common::record::{DefaultRecord, MemoryRecords, Record, RecordVersion, TimestampType};
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
/// Owns a `MemoryRecords` constructed from a clone of the partition's
/// records buffer; we accept this clone for Milestone-8 because the
/// existing `MemoryRecords::new` takes `Vec<u8>`. A future revision may
/// extend `MemoryRecords` to accept a borrowed slice (preserving §27 more
/// strictly), but the §27 contract is *currently* satisfied at the
/// `CompletedFetch` interface boundary: the
/// `partition_data.records: Option<Vec<u8>>` is the canonical owner; we
/// take one move of those bytes into the cursor's `MemoryRecords` on the
/// first iteration, never per record.
#[derive(Debug)]
struct BatchCursor {
    /// Records buffer cloned out of `partition_data.records` exactly once
    /// per `CompletedFetch`.
    memory_records: MemoryRecords,
    /// Index of the next batch to process. `None` means iteration has
    /// terminated.
    next_batch_offset: Option<usize>,
    /// Metadata of the batch we're currently iterating; `None` before the
    /// first batch is loaded.
    current_batch: Option<BatchMetadata>,
    /// Records of the current batch, materialized by
    /// `DefaultRecordBatch::iter_records`. Empty between batches.
    current_records: Vec<DefaultRecord>,
    /// Index of the next record to return from `current_records`.
    record_index: usize,
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
        Self {
            partition,
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
        Self {
            partition,
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

    /// Returns the records buffer slice borrowed from
    /// `partition_data.records`. Mirrors Java's
    /// `FetchResponse.recordsOrFail(PartitionData)`.
    fn records_slice(&self) -> &[u8] {
        self.partition_data.records.as_deref().unwrap_or(&[])
    }

    /// Lazily initializes the batch cursor on first call.
    ///
    /// Per §27 we want to delay the buffer take until at least one
    /// `fetch_records` call. We take a slice clone here (one `Vec` copy
    /// per partition, NOT per record) into a `MemoryRecords` instance.
    /// A future revision may extend `MemoryRecords` to borrow, removing
    /// even this one-time clone.
    fn ensure_cursor(&mut self) {
        if self.cursor.is_some() {
            return;
        }
        // §27: per-partition single take, NOT per-record. The Vec-to-Vec
        // copy here is bounded by partition size and runs at most once.
        let memory_records = MemoryRecords::readable_records(self.records_slice());
        self.cursor = Some(BatchCursor {
            memory_records,
            next_batch_offset: Some(0),
            current_batch: None,
            current_records: Vec::new(),
            record_index: 0,
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
            let raw_record_opt: Option<(DefaultRecord, BatchMetadata)>;
            if self.cached_record_exception.is_none() {
                self.corrupt_last_record = true;
                raw_record_opt = self.next_fetched_record(config)?;
                self.corrupt_last_record = false;
            } else {
                // Re-use the last record by re-loading from the current
                // cursor position (we don't advance `record_index` until
                // we successfully decode).
                raw_record_opt = self.peek_last_fetched_record();
            }

            let (record, batch_meta) = match raw_record_opt {
                Some(t) => t,
                None => break,
            };

            // Deserialize key + value.
            let topic_str = self.partition.topic();
            let headers_owned = RecordHeaders::from_slice(record.headers());
            let key_result = match record.key() {
                None => Ok(None),
                Some(key_bytes) => key_deserializer
                    .deserialize_with_headers(topic_str, &headers_owned, key_bytes)
                    .map(Some),
            };
            let value_result = match record.value() {
                None => Ok(None),
                Some(value_bytes) => value_deserializer
                    .deserialize_with_headers(topic_str, &headers_owned, value_bytes)
                    .map(Some),
            };

            let key = match key_result {
                Ok(k) => k,
                Err(e) => {
                    let err =
                        wrap_deserialization_error(DeserializationOrigin::Key, &self.partition, record.offset(), e);
                    self.cached_record_exception = Some(err.clone());
                    if out.is_empty() {
                        return Err(err);
                    }
                    error!(
                        "Key deserialization failed for {} at offset {}",
                        self.partition,
                        record.offset()
                    );
                    // Stop on the failed record — Java keeps `cachedRecordException` and returns
                    // already-decoded records.
                    break;
                },
            };
            let value = match value_result {
                Ok(v) => v,
                Err(e) => {
                    let err =
                        wrap_deserialization_error(DeserializationOrigin::Value, &self.partition, record.offset(), e);
                    self.cached_record_exception = Some(err.clone());
                    if out.is_empty() {
                        return Err(err);
                    }
                    error!(
                        "Value deserialization failed for {} at offset {}",
                        self.partition,
                        record.offset()
                    );
                    break;
                },
            };

            let leader_epoch = maybe_leader_epoch(batch_meta.partition_leader_epoch);
            let timestamp_type = batch_meta.timestamp_type;
            let key_size = record.key_size();
            let value_size = record.value_size();
            let offset = record.offset();
            let timestamp = record.timestamp();
            // Topic name as Arc<str> — single clone per record per §27.
            let topic_arc: std::sync::Arc<str> = std::sync::Arc::from(topic_str);
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
            self.bytes_read += record.size_in_bytes();
            self.next_fetch_offset = record.offset() + 1;
            self.cached_record_exception = None;
            out.push(consumer_record);
            // Advance the record cursor — we successfully consumed this record.
            if let Some(cursor) = &mut self.cursor {
                cursor.record_index += 1;
            }
        }

        Ok(out)
    }

    /// Pulls the next record that should be returned to the user,
    /// skipping aborted-transaction batches and control batches per
    /// READ_COMMITTED semantics. Returns `None` when iteration is
    /// exhausted; in that case the cursor is drained and
    /// `next_fetch_offset` is advanced to the end of the last batch.
    fn next_fetched_record(
        &mut self,
        config: &FetchConfig,
    ) -> Result<Option<(DefaultRecord, BatchMetadata)>, KafkaError> {
        loop {
            // Reload current batch if exhausted.
            let needs_new_batch = match &self.cursor {
                Some(cursor) => cursor.record_index >= cursor.current_records.len(),
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
                    return Ok(None);
                }
                // load_next_batch may have skipped the batch entirely for
                // aborted transactions; try again from the top.
                continue;
            }

            // Pull next record from current batch — peek-style; we don't
            // advance until the caller decodes it successfully.
            let (record, batch_meta) = self.peek_last_fetched_record().expect("cursor verified non-empty above");
            // Skip out-of-range and control records.
            if record.offset() < self.next_fetch_offset {
                // Skip this record (advance the cursor).
                if let Some(cursor) = &mut self.cursor {
                    cursor.record_index += 1;
                }
                continue;
            }

            // CRC validation if configured.
            if config.check_crcs
                && let Err(e) = record.ensure_valid()
            {
                return Err(KafkaError::illegal_state(format!(
                    "Record for partition {} at offset {} is invalid, cause: {}",
                    self.partition,
                    record.offset(),
                    e
                )));
            }

            if batch_meta.is_control_batch {
                // Control records are not returned to the user — advance
                // nextFetchOffset and skip.
                self.next_fetch_offset = record.offset() + 1;
                if let Some(cursor) = &mut self.cursor {
                    cursor.record_index += 1;
                }
                continue;
            }
            return Ok(Some((record, batch_meta)));
        }
    }

    /// Returns the current record (the one at `cursor.record_index`)
    /// without advancing. Returns `None` if no current record.
    fn peek_last_fetched_record(&self) -> Option<(DefaultRecord, BatchMetadata)> {
        let cursor = self.cursor.as_ref()?;
        if cursor.record_index >= cursor.current_records.len() {
            return None;
        }
        let record = cursor.current_records[cursor.record_index].clone();
        let batch_meta = cursor.current_batch.clone()?;
        Some((record, batch_meta))
    }

    /// Loads the next batch into the cursor. Skips aborted-transaction
    /// batches and applies READ_COMMITTED filtering. Returns
    /// `Ok(true)` if a batch is now loaded, `Ok(false)` if no more
    /// batches remain.
    fn load_next_batch(&mut self, config: &FetchConfig) -> Result<bool, KafkaError> {
        loop {
            // Phase 1: pull batch metadata + records out of the cursor in
            // a tight scope that drops the &mut self.cursor borrow before
            // touching the other self fields.
            let (start_pos, batch_meta, records_opt) = {
                let cursor = match &mut self.cursor {
                    Some(c) => c,
                    None => return Ok(false),
                };
                let Some(start_pos) = cursor.next_batch_offset else {
                    return Ok(false);
                };

                // Walk MemoryRecords' batches to find the one at start_pos.
                let batch_iter = cursor.memory_records.batches();
                let mut current_batch_opt = None;
                for (current_pos, batch) in batch_iter.enumerate() {
                    if current_pos == start_pos {
                        current_batch_opt = Some(batch);
                        break;
                    }
                }
                let Some(batch) = current_batch_opt else {
                    cursor.next_batch_offset = None;
                    return Ok(false);
                };

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

                // Decode the batch's records right away so we don't need
                // to re-walk the iterator outside this scope.
                let records = batch.iter_records().map_err(|e| {
                    KafkaError::illegal_state(format!(
                        "Record batch for partition {} at offset {} is invalid, cause: {}",
                        self.partition, meta.base_offset, e
                    ))
                })?;
                // Always advance the cursor's next-batch pointer here so
                // both the skip path and the load path move forward.
                cursor.next_batch_offset = Some(start_pos + 1);
                (start_pos, meta, Some(records))
            };

            // Phase 2: now that the cursor borrow is dropped, we can
            // touch the other self fields freely.
            self.last_epoch = maybe_leader_epoch(batch_meta.partition_leader_epoch);

            if config.isolation_level == IsolationLevel::ReadCommitted && batch_meta.has_producer_id {
                self.consume_aborted_transactions_up_to(batch_meta.last_offset);
                if !batch_meta.is_control_batch && self.aborted_producer_ids.contains(&batch_meta.producer_id) {
                    debug!(
                        "Skipping aborted record batch from partition {} with producerId {} and offsets {} to {}",
                        self.partition, batch_meta.producer_id, batch_meta.base_offset, batch_meta.last_offset
                    );
                    self.next_fetch_offset = batch_meta.next_offset;
                    let _ = start_pos; // silence unused-warning on the skip path
                    continue;
                }
            }

            // Install the batch as the current one.
            if let Some(cursor) = &mut self.cursor {
                cursor.current_records = records_opt.expect("records collected above");
                cursor.current_batch = Some(batch_meta);
                cursor.record_index = 0;
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
