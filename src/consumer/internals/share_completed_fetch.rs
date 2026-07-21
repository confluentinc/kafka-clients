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

//! `ShareCompletedFetch` — per-partition batch state and acquired-record
//! iteration for a share consumer (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareCompletedFetch`.
//!
//! `ShareCompletedFetch` represents a batch of records returned from the broker
//! via a `ShareFetchRequest`. It maintains state between calls to
//! [`fetch_records`](ShareCompletedFetch::fetch_records). Although it has
//! similarities with [`CompletedFetch`](super::completed_fetch::CompletedFetch),
//! the details are quite different: it does not track aborted transactions or a
//! fetch position, but instead interleaves the broker's *acquired records*
//! (offset + delivery-count ranges) with the actual records in the batch,
//! emitting gaps for acquired offsets that have no corresponding record.
//!
//! **THIS IS THE ZERO-COPY RECEIVE PATH** per `consumer-threading.md` §27:
//!
//! - `partition_data.records` arrives owning the fetch payload. It is *moved*
//!   (not copied) into the cursor's [`MemoryRecords`] on first
//!   [`fetch_records`], which becomes the single canonical owner of the record
//!   bytes.
//! - `topic_arc: Arc<str>` is allocated ONCE and cloned cheaply per
//!   `ConsumerRecord` — no `String::from_utf8` per record.
//! - Records are decoded lazily via a byte cursor; the key/value bytes are
//!   `&[u8]` slices into the fetch buffer (or, for compressed batches, into the
//!   once-per-batch decompression buffer). We never materialize a
//!   `Vec<ConsumerRecord>` of the whole batch.
//! - The only per-record allocation is the user `Deserializer<T>`'s decoded
//!   output plus the §27-sanctioned owned `RecordHeaders`.
//!
//! Metrics (`ShareFetchMetricsAggregator`) are omitted: deferred to KIP-714.

#![allow(dead_code)]

use std::collections::HashSet;

use log::error;

use crate::common::header::internals::RecordHeaders;
use crate::common::protocol::Errors;
use crate::common::record::abstract_records::LOG_OVERHEAD;
use crate::common::record::{
    DefaultRecord, DefaultRecordBatchRef, MemoryRecords, RecordBatch, RecordVersion, TimestampType,
};
use crate::common::{KafkaError, TopicIdPartition};
use crate::consumer::internals::deserializers::Deserializers;
use crate::consumer::internals::share_in_flight_batch::ShareInFlightBatch;
use crate::consumer::internals::share_in_flight_batch_exception::ShareInFlightBatchException;
use crate::consumer::{AcknowledgeType, ConsumerRecord};
use crate::share_fetch_response_data::{AcquiredRecords, PartitionData};

/// Sentinel value: a partition leader epoch that is unknown / unset.
const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// One acquired offset with its delivery count, expanded from a
/// [`AcquiredRecords`] range. Mirrors Java's private
/// `ShareCompletedFetch.OffsetAndDeliveryCount`.
#[derive(Clone, Copy, Debug)]
struct OffsetAndDeliveryCount {
    offset: i64,
    delivery_count: i16,
}

/// Snapshot of the metadata of the batch the cursor is currently on. Captures
/// the fields the per-record parse needs after the batch-header borrow drops.
#[derive(Clone, Copy, Debug)]
struct ShareBatchMeta {
    base_offset: i64,
    base_timestamp: i64,
    base_sequence: i32,
    last_offset: i64,
    max_timestamp: i64,
    timestamp_type: TimestampType,
    partition_leader_epoch: i32,
}

/// Where the current batch's record bytes come from. Uncompressed batches
/// borrow their bytes directly from the cursor's `MemoryRecords` buffer;
/// compressed batches are decompressed once into an owned buffer.
#[derive(Debug)]
enum RecordSource {
    None,
    Borrowed(std::ops::Range<usize>),
    Owned(Vec<u8>),
}

/// Lazily-initialized iteration state over the batches / records of a
/// [`ShareCompletedFetch`].
#[derive(Debug)]
struct BatchCursor {
    /// Records buffer moved out of `partition_data.records` exactly once (no clone).
    memory_records: MemoryRecords,
    /// Absolute byte offset of the next batch header; advanced incrementally.
    /// `None` means batch iteration terminated.
    next_batch_start: Option<usize>,
    /// Metadata of the batch we're currently iterating.
    current_batch: Option<ShareBatchMeta>,
    /// Where the current batch's record bytes live.
    record_source: RecordSource,
    /// Byte offset of the next record to decode, relative to the record source.
    record_byte_offset: usize,
    /// Number of records left to decode in the current batch.
    records_remaining: i32,
}

/// Batch of records returned from the broker for one topic-partition via a
/// share fetch request.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareCompletedFetch`.
pub(crate) struct ShareCompletedFetch {
    node_id: i32,
    partition: TopicIdPartition,
    /// Raw response data — owns the byte buffer until `ensure_cursor` moves it.
    pub(crate) partition_data: PartitionData,
    acquisition_lock_timeout_ms: Option<i32>,
    request_version: i16,

    /// Topic name as `Arc<str>`, allocated once and cloned cheaply per record.
    topic_arc: std::sync::Arc<str>,

    /// Expanded acquired-record list (offset + delivery count), with duplicates
    /// removed (overlapping ranges keep the first occurrence).
    acquired_record_list: Vec<OffsetAndDeliveryCount>,
    /// Position in `acquired_record_list` (Java's `acquiredRecordIterator`).
    acquired_record_index: usize,
    /// The current acquired record being matched (Java's `nextAcquired`).
    next_acquired: Option<OffsetAndDeliveryCount>,

    /// Lazily-initialized batch/record cursor.
    cursor: Option<BatchCursor>,

    /// Base / last offset of the batch currently loaded, retained for
    /// `reject_record_batch` (used from the cached-batch-exception path, when
    /// the corrupt batch's cursor metadata may already be gone).
    current_batch_base_offset: i64,
    current_batch_last_offset: i64,
    /// Offset within the current record source of the last record read by
    /// `next_fetched_record`, so it can be re-parsed on an offset match without
    /// copying bytes or holding a borrow across the acquired-record loop.
    pending_record_offset: Option<usize>,

    /// Cached exceptions, re-surfaced on the next `fetch_records` call (mirrors
    /// Java's `cachedBatchException` / `cachedRecordException`).
    cached_batch_exception: Option<KafkaError>,
    cached_record_exception: Option<KafkaError>,
    /// Offset of the record that failed deserialization, retained for the
    /// cached-record-exception path (Java re-reads `lastRecord.offset()`).
    last_record_offset: i64,

    records_read: i32,
    bytes_read: i32,
    is_consumed: bool,
    initialized: bool,
}

/// Error category used inside the record-collection loop to route to the
/// matching Java `catch` block (`SerializationException` vs
/// `CorruptRecordException`).
enum CollectLoopError {
    /// A record failed to deserialize. Carries the offset of the failing record.
    Serialization { offset: i64, err: KafkaError },
    /// A batch failed CRC / record validation.
    Corrupt(KafkaError),
}

impl ShareCompletedFetch {
    /// Constructs a `ShareCompletedFetch`.
    ///
    /// Translates Java's constructor minus the `LogContext` (we use the `log`
    /// crate), the `BufferSupplier` (decompression is handled internally by
    /// [`DefaultRecordBatchRef::decompress_records`]), and the
    /// `ShareFetchMetricsAggregator` (metrics: deferred to KIP-714).
    pub(crate) fn new(
        node_id: i32,
        partition: TopicIdPartition,
        partition_data: PartitionData,
        acquisition_lock_timeout_ms: Option<i32>,
    ) -> Self {
        Self::new_with_version(node_id, partition, partition_data, acquisition_lock_timeout_ms, 0)
    }

    /// As [`Self::new`], but also records the share-fetch request version
    /// (Java's `requestVersion`, consumed by the request manager in a later
    /// phase; unused within this type).
    pub(crate) fn new_with_version(
        node_id: i32,
        partition: TopicIdPartition,
        partition_data: PartitionData,
        acquisition_lock_timeout_ms: Option<i32>,
        request_version: i16,
    ) -> Self {
        let topic_arc: std::sync::Arc<str> = std::sync::Arc::from(partition.topic());
        let acquired_record_list = Self::build_acquired_record_list(&partition, &partition_data.acquired_records);
        Self {
            node_id,
            partition,
            partition_data,
            acquisition_lock_timeout_ms,
            request_version,
            topic_arc,
            acquired_record_list,
            acquired_record_index: 0,
            next_acquired: None,
            cursor: None,
            current_batch_base_offset: 0,
            current_batch_last_offset: 0,
            pending_record_offset: None,
            cached_batch_exception: None,
            cached_record_exception: None,
            last_record_offset: 0,
            records_read: 0,
            bytes_read: 0,
            is_consumed: false,
            initialized: false,
        }
    }

    /// Expands the acquired-record ranges into a per-offset list, dropping
    /// duplicate offsets from overlapping ranges (keeping the first occurrence).
    ///
    /// Mirrors Java's `buildAcquiredRecordList`.
    fn build_acquired_record_list(
        partition: &TopicIdPartition,
        partition_acquired_records: &[AcquiredRecords],
    ) -> Vec<OffsetAndDeliveryCount> {
        if partition_acquired_records.is_empty() {
            return Vec::new();
        }
        // Size hint from the first batch of acquired records; if there is only
        // one batch, no resizing occurs.
        let first = &partition_acquired_records[0];
        let initial_capacity = (first.last_offset - first.first_offset + 1).max(0) as usize;
        let mut acquired_record_list: Vec<OffsetAndDeliveryCount> = Vec::with_capacity(initial_capacity);

        // Set to find duplicates in case of overlapping acquired records.
        let mut offsets: HashSet<i64> = HashSet::new();
        for acquired in partition_acquired_records {
            for offset in acquired.first_offset..=acquired.last_offset {
                if !offsets.insert(offset) {
                    error!(
                        "Duplicate acquired record offset {} found in share fetch response for partition {}. \
                         This indicates a broker processing issue.",
                        offset,
                        partition.topic_partition()
                    );
                } else {
                    acquired_record_list
                        .push(OffsetAndDeliveryCount { offset, delivery_count: acquired.delivery_count });
                }
            }
        }
        acquired_record_list
    }

    /// The topic-partition this fetch belongs to.
    pub(crate) fn partition(&self) -> &TopicIdPartition {
        &self.partition
    }

    /// Returns whether this fetch has been initialized by the collector.
    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Marks this fetch as initialized.
    pub(crate) fn set_initialized(&mut self) {
        self.initialized = true;
    }

    /// Returns whether this fetch has been fully consumed (or drained).
    pub(crate) fn is_consumed(&self) -> bool {
        self.is_consumed
    }

    /// Draining signals that the data has been consumed and the underlying
    /// iteration state is dropped. Idempotent; invoking [`Self::fetch_records`]
    /// afterwards returns an empty batch. Mirrors Java's `void drain()`.
    pub(crate) fn drain(&mut self) {
        if !self.is_consumed {
            self.cursor = None;
            self.cached_record_exception = None;
            self.cached_batch_exception = None;
            self.is_consumed = true;
            // metrics: deferred to KIP-714 (Java records aggregated bytes/records here).
        }
    }

    /// Converts a batch of records to a [`ShareInFlightBatch`] holding the
    /// acquired [`ConsumerRecord`]s and their acknowledgements. Decompression
    /// and deserialization of each record's key and value are performed here.
    ///
    /// Mirrors Java's
    /// `<K, V> ShareInFlightBatch<K, V> fetchRecords(Deserializers<K, V>, int, boolean)`.
    ///
    /// - `max_records`: soft cap on the number of records to return (the
    ///   current batch is finished even if it takes the count past `max_records`).
    /// - `check_crcs`: whether to validate batch CRCs.
    pub(crate) fn fetch_records<K, V>(
        &mut self,
        deserializers: &Deserializers<K, V>,
        max_records: i32,
        check_crcs: bool,
    ) -> ShareInFlightBatch<K, V>
    where
        K: 'static,
        V: 'static,
    {
        let mut in_flight_batch =
            ShareInFlightBatch::new(self.node_id, self.partition.clone(), self.acquisition_lock_timeout_ms);

        if let Some(cached) = self.cached_batch_exception.take() {
            // A CRC check failed earlier: reject the entire record batch because
            // it is corrupt.
            let offsets = self.reject_record_batch(&mut in_flight_batch);
            in_flight_batch.set_exception(ShareInFlightBatchException::new(cached, offsets));
            return in_flight_batch;
        }

        if let Some(cached) = self.cached_record_exception.take() {
            in_flight_batch.add_acknowledgement(self.last_record_offset, AcknowledgeType::Release);
            in_flight_batch.set_exception(ShareInFlightBatchException::new(
                cached,
                [self.last_record_offset].into_iter().collect(),
            ));
            return in_flight_batch;
        }

        if self.is_consumed {
            return in_flight_batch;
        }

        self.ensure_cursor();
        self.initialize_next_acquired();

        match self.collect_records(&mut in_flight_batch, deserializers, max_records, check_crcs) {
            Ok(()) => {},
            Err(CollectLoopError::Serialization { offset, err }) => {
                // Skip the acquired entry for the failed record.
                self.next_acquired = self.next_acquired_record();
                if in_flight_batch.is_empty() {
                    in_flight_batch.add_acknowledgement(offset, AcknowledgeType::Release);
                    in_flight_batch
                        .set_exception(ShareInFlightBatchException::new(err, [offset].into_iter().collect()));
                } else {
                    self.cached_record_exception = Some(err);
                    self.last_record_offset = offset;
                    in_flight_batch.set_has_cached_exception(true);
                }
            },
            Err(CollectLoopError::Corrupt(err)) => {
                if in_flight_batch.is_empty() {
                    // If a CRC check fails, reject the entire record batch
                    // because it is corrupt.
                    let offsets = self.reject_record_batch(&mut in_flight_batch);
                    in_flight_batch.set_exception(ShareInFlightBatchException::new(err, offsets));
                } else {
                    self.cached_batch_exception = Some(err);
                    in_flight_batch.set_has_cached_exception(true);
                }
            },
        }

        in_flight_batch
    }

    /// The record-collection loop. Interleaves acquired offsets with the
    /// batch's records, emitting gaps for acquired offsets with no record and
    /// adding acquired records to `in_flight_batch`.
    ///
    /// Mirrors the body of Java's `fetchRecords` `while` loop.
    fn collect_records<K, V>(
        &mut self,
        in_flight_batch: &mut ShareInFlightBatch<K, V>,
        deserializers: &Deserializers<K, V>,
        max_records: i32,
        check_crcs: bool,
    ) -> Result<(), CollectLoopError>
    where
        K: 'static,
        V: 'static,
    {
        let mut records_in_batch: i32 = 0;
        let mut current_batch_has_more_records = false;

        while records_in_batch < max_records || current_batch_has_more_records {
            let next = self.next_fetched_record(check_crcs).map_err(CollectLoopError::Corrupt)?;
            let (record_offset, has_more) = match next {
                None => {
                    // Any remaining acquired records are gaps.
                    while let Some(acquired) = self.next_acquired {
                        in_flight_batch.add_gap(acquired.offset);
                        self.next_acquired = self.next_acquired_record();
                    }
                    break;
                },
                Some(v) => v,
            };
            current_batch_has_more_records = has_more;

            while let Some(acquired) = self.next_acquired {
                if record_offset == acquired.offset {
                    // Acquired: parse it and add it to the batch.
                    let (record, size_in_bytes) = self
                        .parse_pending_record(deserializers, acquired.delivery_count)
                        .map_err(|err| CollectLoopError::Serialization { offset: record_offset, err })?;
                    in_flight_batch.add_record(record);
                    self.records_read += 1;
                    self.bytes_read += size_in_bytes;
                    records_in_batch += 1;
                    self.next_acquired = self.next_acquired_record();
                    break;
                } else if record_offset < acquired.offset {
                    // Not acquired: skip this record.
                    break;
                } else {
                    // Acquired, but there's no non-control record at this
                    // offset, so it's a gap.
                    in_flight_batch.add_gap(acquired.offset);
                    self.next_acquired = self.next_acquired_record();
                }
            }
        }
        Ok(())
    }

    /// Sets [`Self::next_acquired`] from the iterator if it is not already set.
    /// Mirrors Java's `initializeNextAcquired`.
    fn initialize_next_acquired(&mut self) {
        if self.next_acquired.is_none() {
            self.next_acquired = self.next_acquired_record();
        }
    }

    /// Returns the next acquired record, advancing the iterator. Mirrors Java's
    /// `nextAcquiredRecord`.
    fn next_acquired_record(&mut self) -> Option<OffsetAndDeliveryCount> {
        if self.acquired_record_index < self.acquired_record_list.len() {
            let value = self.acquired_record_list[self.acquired_record_index];
            self.acquired_record_index += 1;
            Some(value)
        } else {
            None
        }
    }

    /// Rejects the whole current batch: rewinds the acquired iterator to the
    /// start and adds a `REJECT` acknowledgement for every acquired offset in
    /// the current batch's `[base, last]` range. Returns the rejected offsets.
    ///
    /// Mirrors Java's `rejectRecordBatch`.
    fn reject_record_batch<K, V>(&mut self, in_flight_batch: &mut ShareInFlightBatch<K, V>) -> HashSet<i64> {
        // Rewind the acquired iterator to the start, so we are in a known state.
        self.acquired_record_index = 0;
        let mut next_acquired = self.next_acquired_record();
        let mut offsets: HashSet<i64> = HashSet::new();
        let mut offset = self.current_batch_base_offset;
        while offset <= self.current_batch_last_offset {
            match next_acquired {
                None => break,
                Some(acquired) if offset == acquired.offset => {
                    in_flight_batch.add_acknowledgement(offset, AcknowledgeType::Reject);
                    offsets.insert(offset);
                },
                Some(acquired) if offset < acquired.offset => {
                    // Not acquired: skip it (do NOT advance the acquired iterator).
                    offset += 1;
                    continue;
                },
                Some(_) => {},
            }
            next_acquired = self.next_acquired_record();
            offset += 1;
        }
        offsets
    }

    /// Lazily initializes the batch cursor, *moving* the partition's records
    /// buffer into it (no copy; §27 "one buffer").
    fn ensure_cursor(&mut self) {
        if self.cursor.is_some() {
            return;
        }
        let records_buffer = self.partition_data.records.take().unwrap_or_default();
        self.cursor = Some(BatchCursor {
            memory_records: MemoryRecords::new(records_buffer),
            next_batch_start: Some(0),
            current_batch: None,
            record_source: RecordSource::None,
            record_byte_offset: 0,
            records_remaining: 0,
        });
    }

    /// Scans for the next record in the available batches, skipping control
    /// batches, consuming it from the cursor and recording its byte location
    /// for re-parsing. Returns `Ok(Some((offset, has_more)))` where `has_more`
    /// indicates whether the current batch still has records, or `Ok(None)`
    /// when iteration is exhausted (in which case the fetch is drained).
    ///
    /// Mirrors Java's `nextFetchedRecord`, except the record is consumed here
    /// (Java's `records.next()`) and re-read on demand via
    /// [`Self::parse_pending_record`] (rather than held as a borrowed
    /// `lastRecord` field) to satisfy the borrow checker while staying
    /// zero-copy.
    fn next_fetched_record(&mut self, check_crcs: bool) -> Result<Option<(i64, bool)>, KafkaError> {
        loop {
            let needs_new_batch = match &self.cursor {
                Some(cursor) => cursor.records_remaining <= 0,
                None => true,
            };
            if needs_new_batch {
                if !self.load_next_batch(check_crcs)? {
                    self.drain();
                    self.pending_record_offset = None;
                    return Ok(None);
                }
                continue;
            }

            // Peek the record at the current byte offset to obtain its offset
            // and serialized size, WITHOUT copying its payload.
            let (record_offset, consumed) = {
                let cursor = self.cursor.as_ref().expect("cursor present");
                let meta = cursor.current_batch.as_ref().expect("batch loaded");
                let records_bytes = match &cursor.record_source {
                    RecordSource::None => return Ok(None),
                    RecordSource::Borrowed(range) => &cursor.memory_records.buffer()[range.clone()],
                    RecordSource::Owned(buf) => buf.as_slice(),
                };
                if cursor.record_byte_offset >= records_bytes.len() {
                    return Ok(None);
                }
                let log_append_time = if meta.timestamp_type == TimestampType::LogAppendTime {
                    Some(meta.max_timestamp)
                } else {
                    None
                };
                let (record, consumed) = DefaultRecord::read_ref_from_buffer(
                    &records_bytes[cursor.record_byte_offset..],
                    meta.base_offset,
                    meta.base_timestamp,
                    meta.base_sequence,
                    log_append_time,
                )
                .map_err(|e| corrupt_record_error(self.partition.topic_partition().to_string(), meta.base_offset, e))?;
                (record.offset(), consumed)
            };

            // Remember where this record started so `parse_pending_record` can
            // re-read it, then consume it from the cursor.
            if let Some(cursor) = &mut self.cursor {
                self.pending_record_offset = Some(cursor.record_byte_offset);
                cursor.record_byte_offset += consumed;
                cursor.records_remaining -= 1;
                let has_more = cursor.records_remaining > 0;
                return Ok(Some((record_offset, has_more)));
            }
            // Unreachable: cursor is Some here (we did not take the new-batch path).
            return Ok(None);
        }
    }

    /// Re-parses the record recorded by the last [`Self::next_fetched_record`]
    /// call, deserializing its key and value. Returns the record and its
    /// serialized size.
    ///
    /// §27: the key/value bytes are borrowed from the cursor's record source
    /// during deserialization; the only owned copies are the user
    /// deserializer's output and the record headers.
    fn parse_pending_record<K, V>(
        &self,
        deserializers: &Deserializers<K, V>,
        delivery_count: i16,
    ) -> Result<(ConsumerRecord<K, V>, i32), KafkaError>
    where
        K: 'static,
        V: 'static,
    {
        let cursor = self.cursor.as_ref().expect("cursor present");
        let meta = cursor.current_batch.as_ref().expect("batch loaded");
        let byte_offset = self.pending_record_offset.expect("pending record set");
        let records_bytes = match &cursor.record_source {
            RecordSource::None => {
                return Err(KafkaError::illegal_state("no record source for pending record"));
            },
            RecordSource::Borrowed(range) => &cursor.memory_records.buffer()[range.clone()],
            RecordSource::Owned(buf) => buf.as_slice(),
        };
        let log_append_time = if meta.timestamp_type == TimestampType::LogAppendTime {
            Some(meta.max_timestamp)
        } else {
            None
        };
        let (record, _consumed) = DefaultRecord::read_ref_from_buffer(
            &records_bytes[byte_offset..],
            meta.base_offset,
            meta.base_timestamp,
            meta.base_sequence,
            log_append_time,
        )
        .map_err(|e| {
            KafkaError::illegal_state(format!(
                "Record batch for partition {} at offset {} is invalid, cause: {}",
                self.partition.topic_partition(),
                meta.base_offset,
                e
            ))
        })?;

        let headers_vec = record.headers().map_err(|e| {
            KafkaError::illegal_state(format!(
                "Record for partition {} at offset {} has invalid headers, cause: {}",
                self.partition.topic_partition(),
                record.offset(),
                e
            ))
        })?;
        let headers = RecordHeaders::from_headers(headers_vec);

        let topic: &str = &self.topic_arc;
        let leader_epoch = maybe_leader_epoch(meta.partition_leader_epoch);
        let key_size = record.key_size();
        let value_size = record.value_size();

        let key = match record.key() {
            None => None,
            Some(key_bytes) => Some(
                deserializers
                    .key_deserializer()
                    .deserialize_with_headers(topic, &headers, key_bytes)
                    .map_err(|e| {
                        new_record_deserialization_error(
                            DeserializationOrigin::Key,
                            self.partition.topic_partition().to_string(),
                            record.offset(),
                            e,
                        )
                    })?,
            ),
        };
        let value = match record.value() {
            None => None,
            Some(value_bytes) => Some(
                deserializers
                    .value_deserializer()
                    .deserialize_with_headers(topic, &headers, value_bytes)
                    .map_err(|e| {
                        new_record_deserialization_error(
                            DeserializationOrigin::Value,
                            self.partition.topic_partition().to_string(),
                            record.offset(),
                            e,
                        )
                    })?,
            ),
        };

        let consumer_record = ConsumerRecord::with_all(
            std::sync::Arc::clone(&self.topic_arc),
            self.partition.partition(),
            record.offset(),
            record.timestamp(),
            meta.timestamp_type,
            key_size,
            value_size,
            key,
            value,
            headers,
            leader_epoch,
            Some(delivery_count),
        );
        Ok((consumer_record, record.size_in_bytes()))
    }

    /// Loads the next batch into the cursor, skipping control batches. Returns
    /// `Ok(true)` if a data batch is now loaded, `Ok(false)` if no more batches
    /// remain. Mirrors Java's batch-advance portion of `nextFetchedRecord` plus
    /// `maybeEnsureValid`.
    fn load_next_batch(&mut self, check_crcs: bool) -> Result<bool, KafkaError> {
        loop {
            // Borrow 1: read the batch header, advance the next-batch pointer.
            struct HeaderInfo {
                batch_start: usize,
                batch_size: usize,
                magic: i8,
                is_control: bool,
                is_compressed: bool,
                records_count: i32,
                meta: ShareBatchMeta,
            }
            let header = {
                let cursor = match &mut self.cursor {
                    Some(c) => c,
                    None => return Ok(false),
                };
                let Some(batch_start) = cursor.next_batch_start else {
                    return Ok(false);
                };
                let buffer = cursor.memory_records.buffer();
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
                cursor.next_batch_start = Some(batch_start + batch_size);
                HeaderInfo {
                    batch_start,
                    batch_size,
                    magic: batch.magic(),
                    is_control: batch.is_control_batch(),
                    is_compressed: batch.is_compressed(),
                    records_count: batch.records_count(),
                    meta: ShareBatchMeta {
                        base_offset: batch.base_offset(),
                        base_timestamp: batch.base_timestamp(),
                        base_sequence: batch.base_sequence(),
                        last_offset: batch.last_offset(),
                        max_timestamp: batch.max_timestamp(),
                        timestamp_type: batch.timestamp_type(),
                        partition_leader_epoch: batch.partition_leader_epoch(),
                    },
                }
            };

            // Retain the batch's offsets for a possible later reject (must be
            // set before the CRC check so the corrupt batch is rejected).
            self.current_batch_base_offset = header.meta.base_offset;
            self.current_batch_last_offset = header.meta.last_offset;

            // Borrow 2: CRC validation + decompression.
            let source = {
                let cursor = self.cursor.as_ref().expect("cursor present");
                let buffer = cursor.memory_records.buffer();
                let batch = DefaultRecordBatchRef::new(&buffer[header.batch_start..]);
                if check_crcs && header.magic >= RecordVersion::V2.value() {
                    batch.ensure_valid().map_err(|e| {
                        corrupt_record_error(self.partition.topic_partition().to_string(), header.meta.base_offset, e)
                    })?;
                }
                if header.is_compressed {
                    let decompressed = batch.decompress_records().map_err(|e| {
                        corrupt_record_error(self.partition.topic_partition().to_string(), header.meta.base_offset, e)
                    })?;
                    RecordSource::Owned(decompressed)
                } else {
                    let records_start = header.batch_start + RecordBatch::RECORD_BATCH_OVERHEAD;
                    let records_end = header.batch_start + header.batch_size;
                    RecordSource::Borrowed(records_start..records_end)
                }
            };

            // Control batches are not returned to the user — skip the whole
            // batch (Java iterates and discards each control record).
            if header.is_control {
                continue;
            }

            if let Some(cursor) = &mut self.cursor {
                cursor.record_source = source;
                cursor.record_byte_offset = 0;
                cursor.records_remaining = header.records_count;
                cursor.current_batch = Some(header.meta);
            }
            return Ok(true);
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

/// Builds the deserialization error message.
///
/// Rust has no dedicated `RecordDeserializationException` type (following the
/// [`CompletedFetch`](super::completed_fetch) precedent); the origin, partition
/// and offset are encoded into a [`KafkaError::serialization`] message that
/// mirrors Java's `newRecordDeserializationException` text ("The record has
/// been released.").
fn new_record_deserialization_error(
    origin: DeserializationOrigin,
    topic_partition: String,
    offset: i64,
    cause: KafkaError,
) -> KafkaError {
    KafkaError::serialization(format!(
        "Error deserializing {} for partition {} at offset {}. The record has been released. Cause: {}",
        origin.as_str(),
        topic_partition,
        offset,
        cause.message(),
    ))
}

/// Builds the error for a failed CRC / batch validation ("corrupt batch").
///
/// Uses [`Errors::CorruptMessage`] — NOT [`KafkaError::illegal_state`]. In Java
/// the equivalent is `CorruptRecordException`, a regular `KafkaException` (via
/// `RetriableException`/`ApiException`). This matters because
/// [`ShareFetchCollector::collect`] treats [`KafkaError::IllegalState`] as
/// *always-propagating* — that arm mirrors ONLY Java's `IllegalStateException`
/// ("unexpected error code"). A corrupt-batch error must instead be *swallowed*
/// when records were already collected, so the good records are returned and
/// the retriable error deferred (Java's
/// `catch (KafkaException e) { if (fetch.isEmpty()) throw e; }`,
/// `ShareFetchCollector.java:121-125`).
///
/// NOTE: the pre-existing non-share `completed_fetch.rs` / `fetch_collector.rs`
/// map CRC failures to `illegal_state` and share the same `is_illegal_state`
/// escape, so the same divergence likely exists there. That is out of
/// Milestone 9 scope and intentionally left untouched; a future cleanup should
/// align both paths with this one.
///
/// [`ShareFetchCollector::collect`]: super::share_fetch_collector::ShareFetchCollector::collect
fn corrupt_record_error(topic_partition: String, base_offset: i64, cause: impl std::fmt::Display) -> KafkaError {
    KafkaError::with_message(
        Errors::CorruptMessage,
        format!("Record batch for partition {topic_partition} at offset {base_offset} is invalid, cause: {cause}"),
    )
}

fn maybe_leader_epoch(leader_epoch: i32) -> Option<i32> {
    if leader_epoch == NO_PARTITION_LEADER_EPOCH {
        None
    } else {
        Some(leader_epoch)
    }
}

impl std::fmt::Debug for ShareCompletedFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareCompletedFetch")
            .field("node_id", &self.node_id)
            .field("partition", &self.partition)
            .field("is_consumed", &self.is_consumed)
            .field("initialized", &self.initialized)
            .field("records_read", &self.records_read)
            .field("bytes_read", &self.bytes_read)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::record::{MemoryRecords, RecordBatch, SimpleRecord};
    use crate::common::serialization::Deserializer;
    use crate::common::{TopicPartition, Uuid};

    const TOPIC_NAME: &str = "test";
    const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30_000);
    const PRODUCER_ID: i64 = 1000;

    fn tip() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new(TOPIC_NAME.to_string(), 0))
    }

    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(e.to_string()))
        }
    }

    /// A deserializer that only accepts a fixed byte length (like Java's
    /// `UUIDDeserializer`, which requires 16 bytes). Used to drive the
    /// corrupted-message path where some records have the wrong key/value size.
    struct FixedLenDeserializer {
        len: usize,
    }
    impl Deserializer<Vec<u8>> for FixedLenDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
            if data.len() == self.len {
                Ok(data.to_vec())
            } else {
                Err(KafkaError::serialization(format!(
                    "expected {} bytes but got {}",
                    self.len,
                    data.len()
                )))
            }
        }
    }

    fn string_deserializers() -> Deserializers<String, String> {
        Deserializers::new(Box::new(StringDeserializer), Box::new(StringDeserializer))
    }

    fn new_share_completed_fetch(partition_data: PartitionData) -> ShareCompletedFetch {
        ShareCompletedFetch::new(0, tip(), partition_data, DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS)
    }

    /// A single acquired-records range `[first_offset, first_offset + count - 1]`
    /// with delivery count 1. Mirrors the Java test's `acquiredRecords(long, int)`.
    fn acquired_records(first_offset: i64, count: i64) -> Vec<AcquiredRecords> {
        let mut ar = AcquiredRecords::new();
        ar.first_offset = first_offset;
        ar.last_offset = first_offset + count - 1;
        ar.delivery_count = 1;
        vec![ar]
    }

    /// Builds `count` records at `base_offset` in a single uncompressed v2 batch.
    fn new_records(base_offset: i64, count: i32) -> Vec<u8> {
        let simple: Vec<SimpleRecord> = (0..count)
            .map(|_| SimpleRecord::new(0, Some(b"key".to_vec()), Some(b"value".to_vec()), vec![]))
            .collect();
        MemoryRecords::with_records_at_offset(2, base_offset, Compression::none(), TimestampType::CreateTime, &simple)
            .buffer()
            .to_vec()
    }

    /// Builds `count` records spread across `batch_count` equal, consecutive
    /// batches. Mirrors the Java test's two-batch `newRecords` overload.
    fn new_records_multi_batch(base_offset: i64, records_per_batch: i32, batch_count: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        for b in 0..batch_count {
            let start = base_offset + (b * records_per_batch) as i64;
            let simple: Vec<SimpleRecord> = (0..records_per_batch)
                .map(|_| SimpleRecord::new(0, Some(b"key".to_vec()), Some(b"value".to_vec()), vec![]))
                .collect();
            let records = MemoryRecords::with_records_at_offset(
                2,
                start,
                Compression::none(),
                TimestampType::CreateTime,
                &simple,
            );
            buf.extend_from_slice(records.buffer());
        }
        buf
    }

    fn partition_data(records: Option<Vec<u8>>, acquired: Vec<AcquiredRecords>) -> PartitionData {
        let mut pd = PartitionData::new();
        pd.partition_index = 0;
        pd.records = records.map(bytes::Bytes::from);
        pd.acquired_records = acquired;
        pd
    }

    /// Builds a single v2 batch with the given transactional / control flags,
    /// holding `count` records at `base_offset`. Mirrors the `batch_full`
    /// helper in `completed_fetch.rs`.
    fn batch_full(base_offset: i64, count: i32, is_transactional: bool, is_control_batch: bool) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_full(
            1024,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            base_offset,
            -1,
            PRODUCER_ID,
            0,
            0,
            is_transactional,
            is_control_batch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
        );
        for i in 0..count {
            let offset = base_offset + i as i64;
            builder.append_with_offset_bytes(offset, 0, Some(b"key"), Some(b"value"));
        }
        builder.build().buffer().to_vec()
    }

    /// Translated from `ShareCompletedFetchTest.testSimple`.
    #[test]
    fn test_simple() {
        let starting_offset = 10;
        let num_records_per_batch = 10;
        let num_records = 20; // Records for 10-29, in 2 equal batches
        let pd = partition_data(
            Some(new_records_multi_batch(starting_offset, num_records_per_batch, 2)),
            acquired_records(starting_offset, num_records),
        );
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        let records = batch.get_in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(10, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.get_acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.get_acquisition_lock_timeout_ms());

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        let records = batch.get_in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(20, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.get_acknowledgements().size());

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
        assert_eq!(0, batch.get_acknowledgements().size());
    }

    /// Translated from `ShareCompletedFetchTest.testSoftMaxPollRecordLimit`.
    #[test]
    fn test_soft_max_poll_record_limit() {
        let starting_offset = 10;
        let num_records = 11; // Records for 10-20, in a single batch
        let pd = partition_data(
            Some(new_records(starting_offset, num_records)),
            acquired_records(starting_offset, num_records as i64),
        );
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        // The whole batch is finished even though maxRecords is 10.
        assert_eq!(11, batch.get_in_flight_records().len());
        assert_eq!(10, batch.get_in_flight_records()[0].offset());
        assert_eq!(0, batch.get_acknowledgements().size());

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
    }

    /// Translated from `ShareCompletedFetchTest.testUnaligned`.
    #[test]
    fn test_unaligned() {
        let starting_offset = 10;
        let num_records = 10;
        let pd = partition_data(
            Some(new_records(starting_offset, num_records + 500)),
            acquired_records(starting_offset + 500, num_records as i64),
        );
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        let records = batch.get_in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(510, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.get_acknowledgements().size());

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
    }

    /// Translated from `ShareCompletedFetchTest.testNegativeFetchCount`.
    #[test]
    fn test_negative_fetch_count() {
        let pd = partition_data(Some(new_records(0, 10)), acquired_records(0, 10));
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);
        let batch = completed_fetch.fetch_records(&deserializers, -10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
        assert_eq!(0, batch.get_acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.get_acquisition_lock_timeout_ms());
    }

    /// Translated from `ShareCompletedFetchTest.testNoRecordsInFetch`.
    #[test]
    fn test_no_records_in_fetch() {
        let pd = partition_data(None, Vec::new());
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);
        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
        assert_eq!(0, batch.get_acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.get_acquisition_lock_timeout_ms());
    }

    /// Translated from `ShareCompletedFetchTest.testAcquiredRecords`
    /// (acquiring records 0-2 and 6-8 out of 0-9).
    #[test]
    fn test_acquired_records() {
        let mut acquired = acquired_records(0, 3);
        acquired.extend(acquired_records(6, 3));
        let pd = partition_data(Some(new_records(0, 10)), acquired);
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        let records = batch.get_in_flight_records();
        assert_eq!(6, records.len());
        assert_eq!(0, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(6, records[3].offset());
        assert_eq!(Some(1), records[3].delivery_count());

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
    }

    /// Translated from `ShareCompletedFetchTest.testAcquireOddRecords`.
    #[test]
    fn test_acquire_odd_records() {
        let mut acquired: Vec<AcquiredRecords> = Vec::new();
        let mut i = 1;
        while i <= 9 {
            acquired.extend(acquired_records(i, 1));
            i += 2;
        }
        let pd = partition_data(Some(new_records(0, 10)), acquired);
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        let records = batch.get_in_flight_records();
        assert_eq!(5, records.len());
        assert_eq!(1, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(3, records[1].offset());
        assert_eq!(Some(1), records[1].delivery_count());

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        assert_eq!(0, batch.get_in_flight_records().len());
    }

    /// Translated from
    /// `ShareCompletedFetchTest.testOverlappingAcquiredRecordsLogsErrorAndRetainsFirstOccurrence`.
    #[test]
    fn test_overlapping_acquired_records_retains_first_occurrence() {
        // Overlapping acquired records: [0-9] dc=1 and [5-14] dc=2.
        let mut acquired: Vec<AcquiredRecords> = Vec::new();
        let mut ar1 = AcquiredRecords::new();
        ar1.first_offset = 0;
        ar1.last_offset = 9;
        ar1.delivery_count = 1;
        acquired.push(ar1);
        let mut ar2 = AcquiredRecords::new();
        ar2.first_offset = 5;
        ar2.last_offset = 14;
        ar2.delivery_count = 2;
        acquired.push(ar2);

        let pd = partition_data(Some(new_records(0, 20)), acquired);
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 20, true);
        let records = batch.get_in_flight_records();
        // 15 unique records: 0-9 dc=1 (first range), 10-14 dc=2 (second range).
        assert_eq!(15, records.len());

        let record5 = records.iter().find(|r| r.offset() == 5).expect("offset 5 present");
        assert_eq!(Some(1), record5.delivery_count());
        let record10 = records.iter().find(|r| r.offset() == 10).expect("offset 10 present");
        assert_eq!(Some(2), record10.delivery_count());

        // All offsets unique.
        let mut seen: HashSet<i64> = HashSet::new();
        for record in &records {
            assert!(seen.insert(record.offset()), "duplicate offset {}", record.offset());
        }
    }

    /// Translated from `ShareCompletedFetchTest.testCorruptedMessage`.
    ///
    /// One good record, then two records that fail to deserialize (bad key, bad
    /// value), then a good record. Rust has no `RecordDeserializationException`
    /// type; the origin/offset are asserted through the error message and the
    /// exception's offset set (following the `CompletedFetch` precedent).
    #[test]
    fn test_corrupted_message() {
        // Fixed-length (16-byte) key+value deserializer stands in for Java's
        // `UUIDDeserializer`.
        let uuid = |n: u8| vec![n; 16];
        let records = {
            let good0 = SimpleRecord::new(0, Some(uuid(0)), Some(uuid(0)), vec![]);
            // Bad key: "key" (3 bytes) != 16.
            let bad_key = SimpleRecord::new(0, Some(b"key".to_vec()), Some(b"value".to_vec()), vec![]);
            // Bad value: valid 16-byte key, "otherValue" value.
            let bad_value = SimpleRecord::new(10, Some(uuid(2)), Some(b"otherValue".to_vec()), vec![]);
            let good3 = SimpleRecord::new(0, Some(uuid(3)), Some(uuid(3)), vec![]);
            let simple = vec![good0, bad_key, bad_value, good3];
            MemoryRecords::with_records_at_offset(2, 0, Compression::none(), TimestampType::CreateTime, &simple)
                .buffer()
                .to_vec()
        };
        let pd = partition_data(Some(records), acquired_records(0, 4));
        let deserializers: Deserializers<Vec<u8>, Vec<u8>> = Deserializers::new(
            Box::new(FixedLenDeserializer { len: 16 }),
            Box::new(FixedLenDeserializer { len: 16 }),
        );
        let mut completed_fetch = new_share_completed_fetch(pd);

        // Record 0 is returned by itself because record 1 fails to deserialize.
        let batch = completed_fetch.fetch_records(&deserializers, 10, false);
        assert!(batch.get_exception().is_none());
        let records = batch.get_in_flight_records();
        assert_eq!(1, records.len());
        assert_eq!(0, records[0].offset());
        assert_eq!(0, batch.get_acknowledgements().size());

        // Record 1 then results in an empty batch (KEY deserialization error at offset 1).
        let batch = completed_fetch.fetch_records(&deserializers, 10, false);
        let exception = batch.get_exception().expect("exception set");
        assert!(
            exception.cause().message().contains("KEY") && exception.cause().message().contains("at offset 1"),
            "unexpected message: {}",
            exception.cause().message()
        );
        assert!(exception.offsets().contains(&1));
        assert_eq!(0, batch.get_in_flight_records().len());
        assert_eq!(1, batch.get_acknowledgements().size());
        assert_eq!(Some(AcknowledgeType::Release), batch.get_acknowledgements().get(1));

        // Record 2 then results in an empty batch (VALUE deserialization error at offset 2).
        let batch = completed_fetch.fetch_records(&deserializers, 10, false);
        let exception = batch.get_exception().expect("exception set");
        assert!(
            exception.cause().message().contains("VALUE") && exception.cause().message().contains("at offset 2"),
            "unexpected message: {}",
            exception.cause().message()
        );
        assert!(exception.offsets().contains(&2));
        assert_eq!(0, batch.get_in_flight_records().len());
        assert_eq!(1, batch.get_acknowledgements().size());
        assert_eq!(Some(AcknowledgeType::Release), batch.get_acknowledgements().get(2));

        // Record 3 is returned in the next batch, because record 2 has now been skipped.
        let batch = completed_fetch.fetch_records(&deserializers, 10, false);
        assert!(batch.get_exception().is_none());
        let records = batch.get_in_flight_records();
        assert_eq!(1, records.len());
        assert_eq!(3, records[0].offset());
        assert_eq!(0, batch.get_acknowledgements().size());
    }

    /// Translated from `ShareCompletedFetchTest.testCommittedTransactionRecordsIncluded`.
    ///
    /// A transactional data batch plus a COMMIT control marker: the committed
    /// records are included and the control batch is skipped.
    #[test]
    fn test_committed_transaction_records_included() {
        let num_records = 10;
        // Transactional (non-control) data batch with 10 records at offsets 0-9.
        let mut buf = batch_full(0, num_records as i32, true, false);
        // A control batch (e.g. a COMMIT end-transaction marker) that must be
        // skipped and not returned to the user. Built with zero records — the
        // builder rejects normal records in a control batch, and the control
        // batch is skipped whole at the header level regardless of its
        // contents, so an empty control batch exercises the skip path.
        let control = batch_full(num_records, 0, true, true);
        buf.extend_from_slice(&control);

        let pd = partition_data(Some(buf), acquired_records(0, num_records));
        let deserializers = string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(pd);

        let batch = completed_fetch.fetch_records(&deserializers, 10, true);
        let records = batch.get_in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(0, batch.get_acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.get_acquisition_lock_timeout_ms());
    }

    // ── §27 per-record allocation-budget regression test ────────────────────
    //
    // Mirrors the `FetchCollector` allocation-budget test (Phase 7b). The share
    // receive path must be zero-copy: per-record allocations are bounded by the
    // user deserializer's key+value output (plus the §27-sanctioned owned
    // headers). Specifically there must be NO topic-name `String` per record
    // (`topic_arc` is cloned), NO `Vec<u8>` clone of fetch-buffer bytes (records
    // borrow via `DefaultRecord::read_ref_from_buffer`), and NO decode of a
    // `Vec<ConsumerRecord>` for the whole batch.

    /// Same as [`StringDeserializer`] but distinctly named so the budget cost is
    /// attributed to this test.
    struct StringDeserializerForBudget;
    impl Deserializer<String> for StringDeserializerForBudget {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(e.to_string()))
        }
    }

    /// §27 per-record allocation-budget regression test on the share receive
    /// path (`ShareCompletedFetch::fetch_records`).
    #[test]
    fn test_fetch_records_per_record_allocation_budget() {
        const RECORD_COUNT: i32 = 100;
        // Baseline is ~key+value String::from_utf8 per record (the user
        // deserializer's `T`, the only allocation §27 permits) plus the owned
        // `RecordHeaders`. Budget at 5 catches a regression re-adding a
        // per-record byte copy (key/value `to_vec`, topic `String::from`, or a
        // `DefaultRecord` clone).
        const ALLOC_BUDGET_PER_RECORD: usize = 5;
        // One-time overhead (ShareInFlightBatch BTreeMap, cursor MemoryRecords
        // move, acquired-record list, etc.).
        const OVERHEAD_BUDGET: usize = 120;

        let pd = partition_data(Some(new_records(0, RECORD_COUNT)), acquired_records(0, RECORD_COUNT as i64));
        let deserializers: Deserializers<String, String> =
            Deserializers::new(Box::new(StringDeserializerForBudget), Box::new(StringDeserializerForBudget));
        let mut completed_fetch = new_share_completed_fetch(pd);

        let alloc_count;
        let record_count;
        {
            let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            let batch = completed_fetch.fetch_records(&deserializers, RECORD_COUNT, true);
            alloc_count = crate::test_alloc_tracker::AllocTrackingGuard::count();
            record_count = batch.get_in_flight_records().len();
        }

        assert_eq!(
            RECORD_COUNT as usize, record_count,
            "did not decode the expected number of records"
        );

        let max_allowed = OVERHEAD_BUDGET + ALLOC_BUDGET_PER_RECORD * (RECORD_COUNT as usize);
        assert!(
            alloc_count <= max_allowed,
            "Per-record allocation regression: {alloc_count} allocs for {RECORD_COUNT} records \
             (budget: {max_allowed} = {OVERHEAD_BUDGET} overhead + {ALLOC_BUDGET_PER_RECORD}/record). \
             Likely cause: a new `String::from_utf8`, `Vec<u8>::clone`, or `DefaultRecord::clone` \
             entered the per-record path (consumer-threading.md §27)."
        );
        assert!(
            alloc_count >= RECORD_COUNT as usize,
            "Expected at least 1 allocation per record (key + value deserializer); \
             got {alloc_count} — tracker likely misconfigured"
        );
        eprintln!(
            "§27 share allocation budget: {alloc_count} allocs for {RECORD_COUNT} records \
             (avg {avg:.2}/record, max allowed {max_allowed})",
            avg = alloc_count as f64 / RECORD_COUNT as f64,
        );
    }
}
