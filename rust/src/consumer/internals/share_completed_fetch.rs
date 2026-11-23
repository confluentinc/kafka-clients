// Copyright 2026 Confluent Inc.
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

//! `ShareCompletedFetch` — the record batches of one partition returned by a
//! `ShareFetch` request, and the state between `fetchRecords` calls.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareCompletedFetch`.
//!
//! **A zero-copy hot path** (`consumer-threading.md` §27, `share-consumer.md`
//! §4). It reads the partition's records through the consumer's decoder
//! ([`BatchCursor`], [`parse_record`]): the records stay in the response
//! buffer, each key and value is handed to the deserializers as a slice of it,
//! and the topic name is one `Arc<str>` per fetch. Per record, the only
//! allocations are the deserializers' output, the record's headers and, for a
//! compressed batch, the buffer the record is decompressed into
//! (`batch_cursor::RecordStream`), as on the consumer path.
//!
//! Java's `CloseableIterator<Record>` over the current batch (ledger gap G15)
//! is the cursor's record source, as in `CompletedFetch`.
//!
//! Failures follow Java's iterators (`ShareCompletedFetch#nextFetchedRecord`,
//! `DefaultRecordBatch.RecordIterator#next`): a record that fails to be read
//! still counts as read, and a batch that fails its checks is the current
//! batch and is moved past, so the next call reads on. A compressed batch
//! whose stream fails surfaces a bare `KafkaError` (Java's `KafkaException`
//! from `DefaultRecordBatch.StreamRecordIterator#readNext` or the codec's
//! `wrapForInput`), which escapes `fetch_records`; the batch's acquired offsets
//! end as gaps once the fetch reads past them. A stream that fails after the
//! batch's first record fails once per record the batch still declares, as
//! Java's iterator does. One that fails before the first record ends the batch
//! at once: an approximation. The Rust codecs read their header with the first
//! record, so the decoder cannot tell a header failure from a failure in the
//! first block. Java's gzip, lz4 and snappy streams read the header as they are
//! opened (`GzipCompression#wrapForInput`, `Lz4Compression#wrapForInput`,
//! `SnappyCompression#wrapForInput`), so a header failure there ends the batch
//! as here; but zstd-jni reads its frame header on the first read
//! (`ZstdCompression#wrapForInput`), and a failure in the first block of any
//! codec comes from `StreamRecordIterator#readNext` too, and Java then fails
//! up to once per declared record where Rust fails once. The acknowledgements
//! end the same (gaps); only the number of failing calls differs. The message is the decoder's
//! (`Failed to decompress record stream: ...`), where Java's codec wrapper may
//! carry the underlying `IOException`'s text instead.
//!
//! Deviations, both from the shared decoder: it refuses message formats v0
//! and v1 by their magic (the consumer's decision D7), so such a batch is
//! rejected as corrupt where Java reads it; and it checks a v2 batch's minimum
//! size whether or not `check.crcs` is set (D2), where Java without
//! `check.crcs` reads past the short header and fails with an
//! `IndexOutOfBoundsException`.

use std::collections::HashSet;
use std::sync::Arc;

use log::error;

use crate::common::Error;
use crate::common::TopicIdPartition;
use crate::common::utils::internals::BufferSupplier;
use crate::consumer::AcknowledgeType;
use crate::consumer::internals::ConsumerShareInFlightBatchError;
use crate::consumer::internals::Deserializers;
use crate::consumer::internals::ShareFetchMetricsAggregator;
use crate::consumer::internals::ShareInFlightBatch;
use crate::consumer::internals::batch_cursor::{BatchCursor, BatchMetadata, parse_record};
use crate::consumer::internals::share_in_flight_batch::{
    RECORD_RELEASED, ShareInFlightBatchStatus, corrupt_batch_error, share_stream_error,
};
use crate::share_fetch_response_data::{AcquiredRecords, PartitionData};

/// The most acquired offsets the list is sized for up front; a bigger first
/// range grows the list as its offsets are added. Java sizes the list from the
/// broker's first range unchecked
/// (`ShareCompletedFetch#buildAcquiredRecordList`); in Rust a failed
/// allocation aborts the process.
const MAX_ACQUIRED_PREALLOCATION: usize = 1 << 16;

/// The record batches of one partition returned by a `ShareFetch` request,
/// with the acquired offsets the broker sent with them.
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.ShareCompletedFetch`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch")]
pub(crate) struct ShareCompletedFetch {
    pub(crate) node_id: i32,
    pub(crate) partition: TopicIdPartition,
    /// The partition's response data. Its `records` are moved into the cursor
    /// at construction (Java builds its `batches` iterator there too); nothing
    /// is copied.
    pub(crate) partition_data: PartitionData,
    pub(crate) acquisition_lock_timeout_ms: Option<i32>,
    #[cfg_attr(not(test), expect(dead_code, reason = "Java sets the field and reads it nowhere"))]
    pub(crate) request_version: i16,

    /// The size of the partition's records, for the collector's log line
    /// (`ShareFetchResponse.recordsSize`), taken before they move.
    records_size: usize,
    /// Topic name, allocated once and cloned per record (§27).
    topic: Arc<str>,
    decompression_buffer_supplier: Arc<BufferSupplier>,
    /// Java's `batches`, `currentBatch` and `records`; `None` once drained.
    cursor: Option<BatchCursor>,
    /// The size of the record the cursor is on and has read: Java's
    /// `records.next()` has consumed it, so the next
    /// [`Self::next_fetched_record`] moves past it.
    consumed_record_size: Option<usize>,
    /// Whether a call was dropped while it was deserializing the record the
    /// cursor is on: the next call parses that record again instead of moving
    /// past it (Rust-only; Java has no cancellation).
    parse_pending: bool,
    /// Whether a record of the current batch has been read. A compressed
    /// batch's stream that fails before its first record is treated as Java's
    /// gzip/lz4/snappy stream failing as it is opened (`streamingIterator`), an
    /// approximation for zstd and first-block failures: see the module doc.
    batch_record_read: bool,
    /// Java's `currentBatch`, as the offset range a corrupt batch rejects:
    /// `(base offset, last offset)`.
    current_batch: Option<(i64, i64)>,
    records_read: i32,
    bytes_read: i32,
    /// Java's `lastRecord`: the offset of the record last read.
    last_record: Option<i64>,
    cached_batch_error: Option<Error>,
    cached_record_error: Option<Error>,
    is_consumed: bool,
    initialized: bool,
    /// Java's `deliveredBatch`.
    delivered_batch: Option<Arc<ShareInFlightBatchStatus>>,
    /// Java's `acquiredRecordList`: (offset, delivery count).
    acquired_record_list: Vec<(i64, i16)>,
    /// Java's `acquiredRecordIterator`: the index of the next entry, `None`
    /// until the iterator is created.
    acquired_record_iterator: Option<usize>,
    next_acquired: Option<(i64, i16)>,
    metrics_aggregator: Arc<ShareFetchMetricsAggregator>,
}

impl ShareCompletedFetch {
    /// A fetch over `partition_data`'s records.
    ///
    /// Java's `LogContext` parameter is dropped (the `log` crate is used).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#ShareCompletedFetch")]
    pub(crate) fn new(
        decompression_buffer_supplier: Arc<BufferSupplier>,
        node_id: i32,
        partition: TopicIdPartition,
        mut partition_data: PartitionData,
        acquisition_lock_timeout_ms: Option<i32>,
        metrics_aggregator: Arc<ShareFetchMetricsAggregator>,
        request_version: i16,
    ) -> Self {
        // Java's `ShareFetchResponse.recordsOrFail(partitionData).batches().iterator()`.
        // §27: the records are a refcounted slice of the response; moving them
        // into the cursor is O(1), no copy.
        let records = partition_data.records.take().unwrap_or_default();
        let records_size = records.len();
        let acquired_record_list = Self::build_acquired_record_list(&partition, &partition_data.acquired_records);
        let topic: Arc<str> = Arc::from(partition.topic());
        Self {
            node_id,
            partition,
            partition_data,
            acquisition_lock_timeout_ms,
            request_version,
            records_size,
            topic,
            decompression_buffer_supplier,
            cursor: Some(BatchCursor::new(records, corrupt_batch_error, share_stream_error)),
            consumed_record_size: None,
            parse_pending: false,
            batch_record_read: false,
            current_batch: None,
            records_read: 0,
            bytes_read: 0,
            last_record: None,
            cached_batch_error: None,
            cached_record_error: None,
            is_consumed: false,
            initialized: false,
            delivered_batch: None,
            acquired_record_list,
            acquired_record_iterator: None,
            next_acquired: None,
            metrics_aggregator,
        }
    }

    /// Flattens the broker's acquired ranges into one (offset, delivery count)
    /// entry per offset, keeping the first occurrence of an offset the ranges
    /// repeat and logging the repeat.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#buildAcquiredRecordList")]
    fn build_acquired_record_list(
        partition: &TopicIdPartition,
        partition_acquired_records: &[AcquiredRecords],
    ) -> Vec<(i64, i16)> {
        // Setting the size of the array to the size of the first batch of acquired records. In case there is only 1 batch acquired, resizing would not happen.
        let Some(first) = partition_acquired_records.first() else {
            return Vec::new();
        };
        let initial_list_size = usize::try_from(first.last_offset.saturating_sub(first.first_offset).saturating_add(1))
            .unwrap_or(0)
            .min(MAX_ACQUIRED_PREALLOCATION);
        let mut acquired_record_list = Vec::with_capacity(initial_list_size);

        // Set to find duplicates in case of overlapping acquired records
        let mut offsets = HashSet::new();
        for acquired_records in partition_acquired_records {
            let mut offset = acquired_records.first_offset;
            while offset <= acquired_records.last_offset {
                if !offsets.insert(offset) {
                    error!(
                        "Duplicate acquired record offset {} found in share fetch response for partition {}. \
                         This indicates a broker processing issue.",
                        offset,
                        partition.topic_partition()
                    );
                } else {
                    acquired_record_list.push((offset, acquired_records.delivery_count));
                }
                let Some(next) = offset.checked_add(1) else {
                    break;
                };
                offset = next;
            }
        }
        acquired_record_list
    }

    /// Whether the collector has initialized this fetch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#isInitialized")]
    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Marks this fetch initialized.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#setInitialized")]
    pub(crate) fn set_initialized(&mut self) {
        self.initialized = true;
    }

    /// Whether every record has been read, or the fetch was drained.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#isConsumed")]
    pub(crate) fn is_consumed(&self) -> bool {
        self.is_consumed
    }

    /// The size of the partition's records.
    pub(crate) fn records_size(&self) -> usize {
        self.records_size
    }

    /// Track the batch that carries records delivered to the application so we can tell when their acknowledgements
    /// are still outstanding. We only advance to a new batch once the previous one has been drained of its
    /// in-flight records and its acknowledgements have been taken by the background thread to be sent.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#trackDeliveredBatch")]
    fn track_delivered_batch(&mut self, in_flight_batch: Arc<ShareInFlightBatchStatus>) {
        let advance = match &self.delivered_batch {
            None => true,
            Some(delivered) => delivered.num_records() == 0 && !delivered.has_renewals(),
        };
        if advance {
            self.delivered_batch = Some(in_flight_batch);
        }
    }

    /// Whether the batch delivered last still has records in flight or being
    /// renewed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#hasPendingAcknowledgements")]
    pub(crate) fn has_pending_acknowledgements(&self) -> bool {
        // Records being renewed are removed and readded to the in-flight set as the renewals are confirmed.
        self.delivered_batch
            .as_ref()
            .is_some_and(|delivered| delivered.num_records() > 0 || delivered.has_renewals())
    }

    /// Draining a `ShareCompletedFetch` will signal that the data has been consumed and the underlying resources
    /// are closed. This is somewhat analogous to closing, though no error will result if a
    /// caller invokes [`Self::fetch_records`]; an empty batch will be returned instead.
    ///
    /// The cursor, and with it this fetch's hold on the response buffer, is
    /// dropped; in-flight batches hold their own handle on it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#drain")]
    pub(crate) fn drain(&mut self) {
        if !self.is_consumed {
            self.maybe_close_record_stream();
            self.cursor = None;
            self.consumed_record_size = None;
            self.parse_pending = false;
            self.cached_record_error = None;
            self.cached_batch_error = None;
            self.is_consumed = true;
            self.record_aggregated_metrics(self.bytes_read, self.records_read);
        }
    }

    /// After each partition is parsed, we update the current metric totals with the total bytes
    /// and number of records parsed. After all partitions have reported, we write the metric.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#recordAggregatedMetrics")]
    pub(crate) fn record_aggregated_metrics(&self, bytes: i32, records: i32) {
        self.metrics_aggregator.record(self.partition.topic_partition(), bytes, records);
    }

    /// Decodes up to `max_records` acquired records into a new
    /// [`ShareInFlightBatch`], with the acknowledgements for the acquired
    /// offsets that have no record (gaps) and for records that failed.
    ///
    /// `max_records` is soft: the rest of the current record batch is read too.
    /// A record whose deserialization fails is released, and a corrupt record
    /// batch's acquired records are rejected; the batch then carries a
    /// [`ConsumerShareInFlightBatchError`]. When records were decoded before the
    /// failure, they are returned and the failure is reported by the next call
    /// ([`ShareInFlightBatch::has_cached_error`]).
    ///
    /// # Errors
    ///
    /// Any other failure, which Java lets escape `fetchRecords`.
    ///
    /// # Cancellation
    ///
    /// Dropping the future while a deserializer is pending leaves the fetch on
    /// the record being decoded, and the next call decodes it again: it is
    /// neither skipped nor acknowledged as a gap. The records decoded before it
    /// in the dropped call are lost with its batch; a caller that must keep
    /// them decodes into a batch it owns ([`Self::fetch_records_into`]).
    /// The batch is tracked for `has_pending_acknowledgements` only once it is
    /// handed out, so a dropped batch never keeps the fetch pending.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#fetchRecords")]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the collector decodes into the batch it keeps (fetch_records_into)"
        )
    )]
    pub(crate) async fn fetch_records<K, V>(
        &mut self,
        deserializers: &Deserializers<K, V>,
        max_records: i32,
        check_crcs: bool,
    ) -> Result<ShareInFlightBatch<K, V>, Error>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        let mut in_flight_batch = self.new_in_flight_batch();
        self.fetch_records_into(deserializers, max_records, check_crcs, &mut in_flight_batch)
            .await?;
        Ok(in_flight_batch)
    }

    /// An empty [`ShareInFlightBatch`] for this fetch's partition and node, the
    /// one [`Self::fetch_records`] decodes into.
    pub(crate) fn new_in_flight_batch<K, V>(&self) -> ShareInFlightBatch<K, V>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        ShareInFlightBatch::new(self.node_id, self.partition.clone(), self.acquisition_lock_timeout_ms)
    }

    /// [`Self::fetch_records`] decoding into `in_flight_batch`, a batch the
    /// caller owns, and counting the records already in it against
    /// `max_records`.
    ///
    /// Rust-only (Java has no cancellation): a call dropped while a
    /// deserializer is pending leaves the records it decoded in
    /// `in_flight_batch`, and a later call given the same batch decodes on into
    /// it from the record that was pending, so none of them is lost. The batch
    /// is a batch of this fetch ([`Self::new_in_flight_batch`]); between the
    /// calls nothing else reads this fetch.
    ///
    /// # Errors
    ///
    /// As [`Self::fetch_records`]; the batch is then incomplete and the caller
    /// drops it, as Java drops the batch of a failed `fetchRecords`.
    pub(crate) async fn fetch_records_into<K, V>(
        &mut self,
        deserializers: &Deserializers<K, V>,
        max_records: i32,
        check_crcs: bool,
        in_flight_batch: &mut ShareInFlightBatch<K, V>,
    ) -> Result<(), Error>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        // Java tracks the batch as the delivered batch here; it is tracked as it
        // is handed out instead (`Self::hand_out`), as nothing reads it in
        // between and a dropped call must not leave a batch nobody holds
        // tracked.
        if let Some(cached_batch_error) = self.cached_batch_error.take() {
            // In the event that a CRC check fails, reject the entire record batch because it is corrupt.
            let offsets = self.reject_record_batch(in_flight_batch);
            in_flight_batch.set_error(ConsumerShareInFlightBatchError::new(cached_batch_error, offsets));
            self.hand_out(in_flight_batch);
            return Ok(());
        }

        if let Some(cached_record_error) = self.cached_record_error.take() {
            if let Some(last_record) = self.last_record {
                in_flight_batch.add_acknowledgement(last_record, AcknowledgeType::Release);
                in_flight_batch.set_error(ConsumerShareInFlightBatchError::new(
                    cached_record_error,
                    HashSet::from([last_record]),
                ));
            }
            self.hand_out(in_flight_batch);
            return Ok(());
        }

        if self.is_consumed {
            self.hand_out(in_flight_batch);
            return Ok(());
        }

        self.initialize_next_acquired();

        // Taken before the loop, which drains the fetch at its end: a refcount,
        // no copy.
        let record_batches = self.cursor.as_ref().map(|cursor| cursor.buffer_bytes().clone());
        match self
            .fetch_records_loop(deserializers, max_records, check_crcs, in_flight_batch)
            .await
        {
            Ok(()) => {},
            Err(se) if se.is_serialization_error() => {
                self.next_acquired = self.next_acquired_record();
                if in_flight_batch.is_empty() {
                    if let Some(last_record) = self.last_record {
                        in_flight_batch.add_acknowledgement(last_record, AcknowledgeType::Release);
                        in_flight_batch
                            .set_error(ConsumerShareInFlightBatchError::new(se, HashSet::from([last_record])));
                    }
                } else {
                    self.cached_record_error = Some(se);
                    in_flight_batch.set_has_cached_error(true);
                }
            },
            Err(e) if e.is_corrupt_record_error() => {
                // The rest of the corrupt batch is not read: its acquired
                // records are rejected below or by the next call.
                self.skip_current_batch();
                if in_flight_batch.is_empty() {
                    // In the event that a CRC check fails, reject the entire record batch because it is corrupt.
                    let offsets = self.reject_record_batch(in_flight_batch);
                    in_flight_batch.set_error(ConsumerShareInFlightBatchError::new(e, offsets));
                } else {
                    self.cached_batch_error = Some(e);
                    in_flight_batch.set_has_cached_error(true);
                }
            },
            Err(e) => return Err(e),
        }

        // The batch can deliver its records again after a RENEW (decision D5).
        if in_flight_batch.num_records() > 0
            && let Some(record_batches) = record_batches
        {
            in_flight_batch.set_record_batches(record_batches);
        }
        self.hand_out(in_flight_batch);
        Ok(())
    }

    /// Tracks `in_flight_batch` as the delivered batch (Java's
    /// `trackDeliveredBatch`, at the start of `fetchRecords`).
    fn hand_out<K, V>(&mut self, in_flight_batch: &ShareInFlightBatch<K, V>)
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        self.track_delivered_batch(in_flight_batch.status());
    }

    /// The body of Java's `try` block in `fetchRecords`.
    async fn fetch_records_loop<K, V>(
        &mut self,
        deserializers: &Deserializers<K, V>,
        max_records: i32,
        check_crcs: bool,
        in_flight_batch: &mut ShareInFlightBatch<K, V>,
    ) -> Result<(), Error>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        // A batch a dropped call decoded into already holds its records.
        let mut records_in_batch = i32::try_from(in_flight_batch.num_records()).unwrap_or(i32::MAX);
        let mut current_batch_has_more_records = false;

        // A call dropped mid-parse left the cursor on its record.
        let mut resume_parse = self.parse_pending;
        while records_in_batch < max_records || current_batch_has_more_records {
            current_batch_has_more_records = if resume_parse {
                resume_parse = false;
                self.cursor.as_ref().is_some_and(|cursor| cursor.records_remaining > 1)
            } else {
                self.next_fetched_record(check_crcs)?
            };
            let Some(last_record) = self.last_record else {
                // Any remaining acquired records are gaps
                while let Some((offset, _)) = self.next_acquired {
                    in_flight_batch.add_gap(offset);
                    self.next_acquired = self.next_acquired_record();
                }
                break;
            };

            while let Some((acquired_offset, delivery_count)) = self.next_acquired {
                if last_record == acquired_offset {
                    // It's acquired, so we parse it and add it to the batch
                    let Some(cursor) = self.cursor.as_ref() else {
                        break;
                    };
                    self.parse_pending = true;
                    let parsed = parse_record(
                        cursor,
                        self.partition.topic_partition(),
                        &self.topic,
                        deserializers.key_deserializer(),
                        deserializers.value_deserializer(),
                        Some(delivery_count),
                        RECORD_RELEASED,
                    )
                    .await;
                    // Java's `records.next()` consumed the record before it was
                    // parsed; here it is consumed once the parse has returned, so
                    // a parse dropped mid-way decodes it again.
                    self.parse_pending = false;
                    self.consume_current_record();
                    let (record, size_in_bytes, _) = parsed?;
                    in_flight_batch.add_record(record);
                    self.records_read += 1;
                    self.bytes_read += size_in_bytes;
                    records_in_batch += 1;

                    self.next_acquired = self.next_acquired_record();
                    break;
                } else if last_record < acquired_offset {
                    // It's not acquired, so we skip it
                    break;
                } else {
                    // It's acquired, but there's no non-control record at this offset, so it's a gap
                    in_flight_batch.add_gap(acquired_offset);
                }

                self.next_acquired = self.next_acquired_record();
            }
            // A record not acquired is skipped.
            self.consume_current_record();
        }
        Ok(())
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#initializeNextAcquired")]
    fn initialize_next_acquired(&mut self) {
        if self.next_acquired.is_none() {
            if self.acquired_record_iterator.is_none() {
                self.acquired_record_iterator = Some(0);
            }
            self.next_acquired = self.next_acquired_record();
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#nextAcquiredRecord")]
    fn next_acquired_record(&mut self) -> Option<(i64, i16)> {
        let index = self.acquired_record_iterator.as_mut()?;
        let next = self.acquired_record_list.get(*index).copied();
        if next.is_some() {
            *index += 1;
        }
        next
    }

    /// Rejects every acquired offset of the current record batch and returns
    /// them.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#rejectRecordBatch")]
    fn reject_record_batch<K, V>(&mut self, in_flight_batch: &mut ShareInFlightBatch<K, V>) -> HashSet<i64>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        // Rewind the acquiredRecordIterator to the start, so we are in a known state
        self.acquired_record_iterator = Some(0);

        let mut acquired = self.next_acquired_record();
        let mut offsets = HashSet::new();
        let Some((base_offset, last_offset)) = self.current_batch else {
            self.next_acquired = acquired;
            return offsets;
        };
        let mut offset = base_offset;
        while offset <= last_offset {
            while acquired.is_some_and(|(acquired_offset, _)| acquired_offset < offset) {
                acquired = self.next_acquired_record();
            }

            match acquired {
                // No more acquired records, so we are done
                None => break,
                Some((acquired_offset, _)) if offset == acquired_offset => {
                    // It's acquired, so we reject it
                    in_flight_batch.add_acknowledgement(offset, AcknowledgeType::Reject);
                    offsets.insert(offset);
                    acquired = self.next_acquired_record();
                },
                // It's not acquired, so we skip it
                Some(_) => {},
            }
            offset += 1;
        }
        self.next_acquired = acquired;
        offsets
    }

    /// Scans for the next record in the available batches, skipping control
    /// records. Returns whether the current batch has more records; leaves
    /// `last_record` on the record found, `None` when there is none.
    ///
    /// # Errors
    ///
    /// A corrupt batch (`CorruptRecordError`, Java's `CorruptRecordException`),
    /// or a malformed record (`InvalidRecordError`; Java's
    /// `org.apache.kafka.common.InvalidRecordException` extends
    /// `InvalidConfigurationException`, not `CorruptRecordException`, so
    /// `fetch_records` lets it escape as Java's `fetchRecords` does).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#nextFetchedRecord")]
    fn next_fetched_record(&mut self, check_crcs: bool) -> Result<bool, Error> {
        loop {
            // Java's `records.next()` consumed the record last read.
            self.consume_current_record();
            let records_remaining = self.cursor.as_ref().map_or(0, |cursor| cursor.records_remaining);
            if records_remaining <= 0 {
                self.maybe_close_record_stream();

                let partition = self.partition.topic_partition();
                let next_batch = match self.cursor.as_mut() {
                    // The checksum is checked below, once the batch is the
                    // current one, so a corrupt batch is the one rejected. A
                    // batch refused here is the current one too, and is moved
                    // past, as Java's `currentBatch = batches.next()` precedes
                    // `maybeEnsureValid`.
                    Some(cursor) => cursor.next_batch_moving_past_refused(partition, false),
                    None => Ok(None),
                };
                let (meta, source, records_count) = match next_batch {
                    Ok(Some(batch)) => batch,
                    Ok(None) => {
                        self.drain();
                        self.last_record = None;
                        break;
                    },
                    Err((error, refused_range)) => {
                        self.current_batch = refused_range;
                        return Err(error);
                    },
                };

                self.current_batch = Some((meta.base_offset, meta.last_offset));
                self.maybe_ensure_valid(&meta, check_crcs)?;

                self.batch_record_read = false;
                if let Some(cursor) = self.cursor.as_mut() {
                    // A failure here leaves the batch uninstalled, so the next
                    // call takes the next batch, as Java's `records` stays null.
                    cursor.install_batch(
                        partition,
                        meta,
                        source,
                        records_count,
                        &self.decompression_buffer_supplier,
                    )?;
                }
            } else {
                let Some(cursor) = self.cursor.as_mut() else {
                    break;
                };
                let partition = self.partition.topic_partition();
                // `RecordIterator#next` counts the record as read before reading
                // it (`readRecords++`), so a record that fails is consumed: the
                // next call reads on. A record whose size is unknown is consumed
                // without moving through the bytes, so each later read fails
                // the same way until the batch's declared count is used up, as
                // Java's does.
                //
                // A compressed batch's stream that fails before its first record
                // ends the batch instead, an approximation: the Rust codecs read
                // their header with the first record. Java's gzip, lz4 and
                // snappy streams read it as they are opened, so their
                // `streamingIterator` throws, `records` stays null, and the next
                // call takes the next batch, as here. A zstd header failure, or
                // a failure in any codec's first block, fails in Java's
                // `readNext` instead, once per declared record; the gaps it
                // ends with are the same (see the module doc).
                if let Err(error) = cursor.read_current_record(partition) {
                    if self.batch_record_read {
                        self.consumed_record_size = Some(0);
                    } else {
                        cursor.close_record_stream();
                        cursor.records_remaining = 0;
                    }
                    return Err(error);
                }
                self.batch_record_read = true;
                let (offset, size, is_control_batch) = match cursor.peek_current_record(partition) {
                    Ok(Some((record, batch))) => {
                        (record.offset(), record.size_in_bytes() as usize, batch.is_control_batch)
                    },
                    Ok(None) => {
                        self.consumed_record_size = Some(0);
                        return Err(crate::consumer::internals::batch_cursor::premature_eof_error(partition));
                    },
                    Err(error) => {
                        self.consumed_record_size = Some(0);
                        return Err(error);
                    },
                };
                // v2 records carry no checksum of their own (`DefaultRecord#ensureValid`).
                self.consumed_record_size = Some(size);
                // `RecordIterator#next` checks that nothing follows a batch's
                // last declared record as it reads it.
                cursor.ensure_none_remaining(partition, size)?;

                // control records are not returned to the user
                if !is_control_batch {
                    self.last_record = Some(offset);
                    break;
                }
            }
        }

        Ok(self.cursor.as_ref().is_some_and(|cursor| cursor.records_remaining > 1))
    }

    /// Java's `maybeEnsureValid(RecordBatch, boolean)`: the batch's checksum,
    /// under `check.crcs`. The batch's size and magic are checked as it is read
    /// (`BatchCursor::next_batch`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#maybeEnsureValid")]
    fn maybe_ensure_valid(&self, batch: &BatchMetadata, check_crcs: bool) -> Result<(), Error> {
        if check_crcs && let Some(cursor) = &self.cursor {
            cursor
                .ensure_valid(batch)
                .map_err(|e| corrupt_batch_error(self.partition.topic_partition(), batch.base_offset, e.message()))?;
        }
        Ok(())
    }

    /// Moves the cursor past the record last read, if it has not moved yet.
    fn consume_current_record(&mut self) {
        if let Some(size) = self.consumed_record_size.take()
            && let Some(cursor) = self.cursor.as_mut()
        {
            cursor.advance_record_cursor(size);
        }
    }

    /// Leaves the rest of the current record batch unread, after it failed.
    fn skip_current_batch(&mut self) {
        self.consumed_record_size = None;
        if let Some(cursor) = self.cursor.as_mut() {
            cursor.close_record_stream();
            cursor.records_remaining = 0;
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareCompletedFetch#maybeCloseRecordStream")]
    fn maybe_close_record_stream(&mut self) {
        if let Some(cursor) = self.cursor.as_mut() {
            cursor.close_record_stream();
        }
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
pub(crate) mod tests {
    //! Translated from `ShareCompletedFetchTest` (22 tests). The helpers are
    //! `pub(crate)` for the `ShareInFlightBatch`, `ShareFetch` and
    //! `ShareFetchCollector` tests.
    //!
    //! Java's `getInFlightRecords()` returns the in-flight records each time it
    //! is called; [`ShareInFlightBatch::in_flight_records`] hands each record
    //! out once (see its doc), so a test that reads them twice in Java binds the
    //! list once here.

    use super::*;
    use crate::common::Uuid;
    use crate::common::compress::Compression;
    use crate::common::errors::DeserializationErrorOrigin;
    use crate::common::header::{Headers, RecordHeader};
    use crate::common::metrics::Metrics;
    use crate::common::protocol::ApiKeys;
    use crate::common::record::TimestampType;
    use crate::common::record::internal::{
        ControlRecordType, EndTransactionMarker, MemoryRecords, MemoryRecordsBuilderOptionsBuilder, RecordBatch,
    };
    use crate::common::serialization::{Deserializer, StringDeserializer};
    use crate::consumer::ConsumerRecord;
    use crate::consumer::internals::{ShareFetchMetricsManager, ShareFetchMetricsRegistry};
    use futures_util::FutureExt;

    pub(crate) const TOPIC_NAME: &str = "test";
    pub(crate) const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30000);
    const PRODUCER_ID: i64 = 1000;
    const PRODUCER_EPOCH: i16 = 0;

    pub(crate) fn tip() -> TopicIdPartition {
        TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 0, TOPIC_NAME)
    }

    /// Java's `newShareCompletedFetch`.
    pub(crate) fn new_share_completed_fetch(
        partition: &TopicIdPartition,
        partition_data: PartitionData,
    ) -> ShareCompletedFetch {
        let metrics = Arc::new(Metrics::new());
        let registry = ShareFetchMetricsRegistry::new();
        let manager = Arc::new(ShareFetchMetricsManager::new(metrics, &registry));
        let aggregator = Arc::new(ShareFetchMetricsAggregator::new(
            manager,
            HashSet::from([partition.topic_partition().clone()]),
        ));
        ShareCompletedFetch::new(
            Arc::new(BufferSupplier::create()),
            0,
            partition.clone(),
            partition_data,
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
            aggregator,
            ApiKeys::SHARE_FETCH.latest_version(),
        )
    }

    pub(crate) fn new_string_deserializers() -> Deserializers<String, String> {
        Deserializers::new(Box::new(StringDeserializer::new()), Box::new(StringDeserializer::new()))
    }

    /// `fetchRecords` driven in one poll, which the always-ready deserializers
    /// of these tests allow.
    pub(crate) fn fetch<K: Send + 'static, V: Send + 'static>(
        completed_fetch: &mut ShareCompletedFetch,
        deserializers: &Deserializers<K, V>,
        max_records: i32,
        check_crcs: bool,
    ) -> ShareInFlightBatch<K, V> {
        completed_fetch
            .fetch_records(deserializers, max_records, check_crcs)
            .now_or_never()
            .expect("the test deserializers are always ready")
            .expect("fetch_records fails only on an unexpected error")
    }

    pub(crate) fn partition_data(records: Option<bytes::Bytes>, acquired: Vec<AcquiredRecords>) -> PartitionData {
        let mut partition_data = PartitionData::new();
        partition_data.set_records(records).set_acquired_records(acquired);
        partition_data
    }

    /// Java's `newRecords(long, int)`: one uncompressed batch.
    pub(crate) fn new_records(base_offset: i64, count: i32) -> bytes::Bytes {
        new_records_compressed(base_offset, count, Compression::none().build())
    }

    pub(crate) fn new_records_compressed(base_offset: i64, count: i32, compression: Compression) -> bytes::Bytes {
        let mut builder =
            MemoryRecords::builder_with_initial_capacity(1024, compression, TimestampType::CreateTime, base_offset);
        for _ in 0..count {
            builder.append_kv(0, Some(b"key"), Some(b"value-"));
        }
        builder.build().into_buffer()
    }

    fn batch_builder(
        base_offset: i64,
        is_transactional: bool,
    ) -> crate::common::record::internal::MemoryRecordsBuilder {
        MemoryRecords::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(1024)
                .set_magic(RecordBatch::CURRENT_MAGIC_VALUE)
                .set_compression(Compression::none().build())
                .set_timestamp_type(TimestampType::CreateTime)
                .set_base_offset(base_offset)
                .set_log_append_time(0)
                .set_producer_id(PRODUCER_ID)
                .set_producer_epoch(PRODUCER_EPOCH)
                .set_base_sequence(0)
                .set_is_transactional(is_transactional)
                .set_is_control_batch(false)
                .set_partition_leader_epoch(RecordBatch::NO_PARTITION_LEADER_EPOCH)
                .set_write_limit(1024)
                .build()
                .expect("every mandatory parameter is set"),
        )
    }

    /// Java's `newRecords(long, int, int)`: `batch_count` transactional batches.
    pub(crate) fn new_records_batches(base_offset: i64, records_per_batch: i32, batch_count: i32) -> bytes::Bytes {
        let mut buffer = Vec::new();
        for b in 0..batch_count {
            let mut builder = batch_builder(base_offset + i64::from(b * records_per_batch), true);
            for _ in 0..records_per_batch {
                builder.append_kv(0, Some(b"key"), Some(b"value"));
            }
            buffer.extend_from_slice(builder.build().buffer());
        }
        bytes::Bytes::from(buffer)
    }

    /// Java's `writeTransactionMarker`: a COMMIT marker at `offset`.
    fn transaction_marker(offset: i64) -> Vec<u8> {
        let marker = EndTransactionMarker::new(ControlRecordType::Commit, 0).expect("COMMIT is a marker type");
        MemoryRecords::write_end_transactional_marker(
            Vec::with_capacity(128),
            offset,
            0,
            0,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            &marker,
        )
        .expect("the marker is written")
        .buffer()
        .to_vec()
    }

    /// Java's `newTransactionalRecords`: `num_records` transactional records,
    /// then a COMMIT marker.
    fn new_transactional_records(num_records: i32) -> bytes::Bytes {
        let mut builder = batch_builder(0, true);
        for _ in 0..num_records {
            builder.append_kv(0, Some(b"key"), Some(b"value"));
        }
        let mut buffer = builder.build().buffer().to_vec();
        buffer.extend_from_slice(&transaction_marker(i64::from(num_records)));
        bytes::Bytes::from(buffer)
    }

    /// Java's `createBatch`: a non-transactional batch, values `value-<offset>`.
    pub(crate) fn create_batch(buffer: &mut Vec<u8>, base_offset: i64, num_records: i32) {
        let mut builder = batch_builder(base_offset, false);
        for i in 0..num_records {
            let value = format!("value-{}", base_offset + i64::from(i));
            builder.append_kv(0, Some(b"key"), Some(value.as_bytes()));
        }
        buffer.extend_from_slice(builder.build().buffer());
    }

    /// Java's `corruptBatchCrc`: the CRC is at offset 17 of a v2 batch.
    fn corrupt_batch_crc(buffer: &mut [u8], batch_start_position: usize) {
        let crc_position = batch_start_position + 17;
        buffer[crc_position..crc_position + 4].copy_from_slice(&0xDEAD_BEEF_u32.to_be_bytes());
    }

    fn new_records_with_corrupted_first_batch() -> bytes::Bytes {
        let mut buffer = Vec::new();
        create_batch(&mut buffer, 0, 10);
        corrupt_batch_crc(&mut buffer, 0);
        create_batch(&mut buffer, 10, 10);
        create_batch(&mut buffer, 20, 10);
        bytes::Bytes::from(buffer)
    }

    fn new_records_with_corrupted_second_batch_small_first() -> bytes::Bytes {
        let mut buffer = Vec::new();
        create_batch(&mut buffer, 0, 5);
        let pos = buffer.len();
        create_batch(&mut buffer, 5, 10);
        corrupt_batch_crc(&mut buffer, pos);
        bytes::Bytes::from(buffer)
    }

    fn new_single_corrupted_batch(base_offset: i64, num_records: i32) -> bytes::Bytes {
        let mut buffer = Vec::new();
        create_batch(&mut buffer, base_offset, num_records);
        corrupt_batch_crc(&mut buffer, 0);
        bytes::Bytes::from(buffer)
    }

    fn new_records_with_multiple_corrupted_batches() -> bytes::Bytes {
        let mut buffer = Vec::new();
        create_batch(&mut buffer, 0, 10);
        corrupt_batch_crc(&mut buffer, 0);
        let pos2 = buffer.len();
        create_batch(&mut buffer, 10, 10);
        corrupt_batch_crc(&mut buffer, pos2);
        create_batch(&mut buffer, 20, 10);
        create_batch(&mut buffer, 30, 10);
        bytes::Bytes::from(buffer)
    }

    /// Java's `acquiredRecords(long, int)`.
    pub(crate) fn acquired_records(first_offset: i64, count: i64) -> Vec<AcquiredRecords> {
        let mut acquired = AcquiredRecords::new();
        acquired
            .set_first_offset(first_offset)
            .set_last_offset(first_offset + count - 1)
            .set_delivery_count(1);
        vec![acquired]
    }

    fn offsets<K, V>(records: &[ConsumerRecord<K, V>]) -> Vec<i64> {
        records.iter().map(ConsumerRecord::offset).collect()
    }

    #[test]
    fn test_simple() {
        let starting_offset = 10;
        let num_records_per_batch = 10;
        let num_records = 20; // Records for 10-29, in 2 equal batches
        let tip = tip();
        let data = partition_data(
            Some(new_records_batches(starting_offset, num_records_per_batch, 2)),
            acquired_records(starting_offset, num_records),
        );
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let records = batch.in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(10, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let records = batch.in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(20, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_has_pending_acknowledgements() {
        let starting_offset = 10;
        let num_records = 5;
        let tip = tip();
        let data = partition_data(
            Some(new_records_batches(starting_offset, num_records, 1)),
            acquired_records(starting_offset, i64::from(num_records)),
        );
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        // No records have been delivered to the application yet.
        assert!(!completed_fetch.has_pending_acknowledgements());

        // Once the records have been delivered, their acknowledgements are outstanding even though the fetch is consumed.
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(num_records as usize, batch.in_flight_records().len());
        assert!(completed_fetch.is_consumed());
        assert!(completed_fetch.has_pending_acknowledgements());

        // Acknowledging the records and taking them to be sent by the background thread, clears the outstanding acknowledgements.
        batch.acknowledge_all(AcknowledgeType::Accept);
        batch.take_acknowledged_records();
        assert!(!completed_fetch.has_pending_acknowledgements());
    }

    #[test]
    fn test_pending_renew_acknowledgements_with_exception() {
        let starting_offset = 10;
        let num_records = 5;
        let tip = tip();
        let data = partition_data(
            Some(new_records_batches(starting_offset, num_records, 1)),
            acquired_records(starting_offset, i64::from(num_records)),
        );
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        // No records have been delivered to the application yet.
        assert!(!completed_fetch.has_pending_acknowledgements());

        // Once the record have been delivered, their acknowledgements are outstanding even though the fetch is consumed.
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(num_records as usize, batch.in_flight_records().len());
        assert!(completed_fetch.is_consumed());
        assert!(completed_fetch.has_pending_acknowledgements());

        // Acknowledging the records and taking them to be sent by the background thread, leaves the acknowledgements outstanding.
        batch.acknowledge_all(AcknowledgeType::Renew);
        let mut renew_acknowledgements = batch.take_acknowledged_records();
        assert!(completed_fetch.has_pending_acknowledgements());

        renew_acknowledgements.complete(None);
        batch
            .renew(&renew_acknowledgements)
            .expect("the acknowledgements are completed");
        assert!(completed_fetch.is_consumed());
        assert!(completed_fetch.has_pending_acknowledgements());

        let released = batch.take_renewals(&deserializers).now_or_never().expect("ready");
        assert!(released.is_none());
        assert!(completed_fetch.is_consumed());
        assert!(completed_fetch.has_pending_acknowledgements());

        batch.acknowledge_all(AcknowledgeType::Renew);
        let mut renew_acknowledgements = batch.take_acknowledged_records();
        assert!(completed_fetch.has_pending_acknowledgements());

        renew_acknowledgements.complete(crate::common::protocol::Errors::NetworkError.error());
        batch
            .renew(&renew_acknowledgements)
            .expect("the acknowledgements are completed");
        assert!(completed_fetch.is_consumed());
        assert!(!completed_fetch.has_pending_acknowledgements());
    }

    #[test]
    fn test_soft_max_poll_record_limit() {
        let starting_offset = 10;
        let num_records = 11; // Records for 10-20, in a single batch
        let tip = tip();
        let data = partition_data(
            Some(new_records(starting_offset, num_records)),
            acquired_records(starting_offset, i64::from(num_records)),
        );
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let records = batch.in_flight_records();
        assert_eq!(11, records.len());
        assert_eq!(10, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_unaligned() {
        let starting_offset = 10;
        let num_records = 10;
        let tip = tip();
        let data = partition_data(
            Some(new_records(starting_offset, num_records + 500)),
            acquired_records(starting_offset + 500, i64::from(num_records)),
        );
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let records = batch.in_flight_records();
        assert_eq!(10, records.len());
        assert_eq!(510, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_committed_transaction_records_included() {
        let num_records = 10;
        let tip = tip();
        let data = partition_data(Some(new_transactional_records(num_records)), acquired_records(0, 10));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(10, batch.in_flight_records().len());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_negative_fetch_count() {
        let tip = tip();
        let data = partition_data(Some(new_records(0, 10)), acquired_records(0, 10));
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let mut batch = fetch(&mut completed_fetch, &deserializers, -10, true);
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_no_records_in_fetch() {
        let tip = tip();
        let mut data = PartitionData::new();
        data.set_partition_index(0);
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    /// Java's `UUIDDeserializer`: the bytes must be a UUID's string form.
    struct UuidDeserializer;
    impl Deserializer<String> for UuidDeserializer {
        async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            let text = std::str::from_utf8(data).map_err(|e| Error::serialization(e.to_string()))?;
            let groups: Vec<&str> = text.split('-').collect();
            let lengths: Vec<usize> = groups.iter().map(|g| g.len()).collect();
            if lengths != [8, 4, 4, 4, 12] || !groups.iter().all(|g| g.chars().all(|c| c.is_ascii_hexdigit())) {
                return Err(Error::serialization(format!("Error parsing data into UUID: {text}")));
            }
            Ok(text.to_string())
        }
    }

    const UUID_1: &[u8] = b"9b2b5e8a-8a0f-4b7e-9d3c-1f2e3d4c5b6a";
    const UUID_2: &[u8] = b"0f1e2d3c-4b5a-4968-8776-655443322110";
    const UUID_3: &[u8] = b"11111111-2222-4333-8444-555555555555";

    #[test]
    fn test_record_deserialization_exception() {
        // Create one good record and then two records which do not deserialize and then another good record.
        let mut builder = MemoryRecords::builder_with_initial_capacity(
            1024,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
        );
        builder.append_kv(RecordBatch::NO_TIMESTAMP, None, Some(UUID_1));
        builder.append_kv(0, Some(b"key"), Some(b"value"));
        let headers = [RecordHeader::new("hkey".to_string(), Some(b"hvalue".to_vec()))];
        builder.append(10, Some(UUID_2), Some(b"otherValue"), &headers);
        builder.append_kv(RecordBatch::NO_TIMESTAMP, None, Some(UUID_3));
        let records = builder.build().into_buffer();

        let tip = tip();
        let mut data = partition_data(Some(records), acquired_records(0, 4));
        data.set_partition_index(0);
        let deserializers: Deserializers<String, String> =
            Deserializers::new(Box::new(UuidDeserializer), Box::new(UuidDeserializer));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        // Record 0 is returned by itself because record 1 fails to deserialize
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, false);
        assert!(batch.error().is_none());
        let fetched_records = batch.in_flight_records();
        assert_eq!(1, fetched_records.len());
        assert_eq!(0, fetched_records[0].offset());
        assert_eq!(0, batch.acknowledgements().size());

        // Record 1 then results in an empty batch
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, false);
        let Some(Error::RecordDeserialization(thrown)) = batch.error().map(ConsumerShareInFlightBatchError::cause)
        else {
            panic!("expected a RecordDeserializationError, got {:?}", batch.error());
        };
        assert_eq!(DeserializationErrorOrigin::Key, thrown.origin());
        assert_eq!(1, thrown.offset());
        assert_eq!(TOPIC_NAME, thrown.topic_partition().topic());
        assert_eq!(0, thrown.topic_partition().partition());
        assert_eq!(0, thrown.timestamp());
        assert_eq!(Some(&b"key"[..]), thrown.key_buffer());
        assert_eq!(Some(&b"value"[..]), thrown.value_buffer());
        assert_eq!(0, thrown.headers().map_or(0, |h| h.to_array().len()));
        assert_eq!(
            "Error deserializing KEY for partition test-0 at offset 1. The record has been released.",
            thrown.message()
        );
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(1, batch.acknowledgements().size());
        assert_eq!(Some(AcknowledgeType::Release), batch.acknowledgements().get(1));
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());

        // Record 2 then results in an empty batch, because record 1 has now been skipped
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, false);
        let Some(Error::RecordDeserialization(thrown)) = batch.error().map(ConsumerShareInFlightBatchError::cause)
        else {
            panic!("expected a RecordDeserializationError, got {:?}", batch.error());
        };
        assert_eq!(DeserializationErrorOrigin::Value, thrown.origin());
        assert_eq!(2, thrown.offset());
        assert_eq!(TOPIC_NAME, thrown.topic_partition().topic());
        assert_eq!(0, thrown.topic_partition().partition());
        assert_eq!(10, thrown.timestamp());
        assert!(thrown.key_buffer().is_some());
        assert_eq!(Some(&b"otherValue"[..]), thrown.value_buffer());
        assert_eq!(0, batch.in_flight_records().len());
        assert_eq!(1, batch.acknowledgements().size());
        assert_eq!(Some(AcknowledgeType::Release), batch.acknowledgements().get(2));
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());

        // Record 3 is returned in the next batch, because record 2 has now been skipped
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, false);
        assert!(batch.error().is_none());
        let fetched_records = batch.in_flight_records();
        assert_eq!(1, fetched_records.len());
        assert_eq!(3, fetched_records[0].offset());
        assert_eq!(0, batch.acknowledgements().size());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, batch.acquisition_lock_timeout_ms());
    }

    #[test]
    fn test_acquired_records() {
        // Acquiring records 0-2 and 6-8
        let mut acquired = acquired_records(0, 3);
        acquired.extend(acquired_records(6, 3));
        let tip = tip();
        let data = partition_data(Some(new_records(0, 10)), acquired);
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let records = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(6, records.len());
        // The first offset should be 0
        assert_eq!(0, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        // The third offset should be 6
        assert_eq!(6, records[3].offset());
        assert_eq!(Some(1), records[3].delivery_count());

        let records = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(0, records.len());
    }

    #[test]
    fn test_gaps_for_control_records_in_acquired_range() {
        let num_records = 10;
        // Acquire all records including the control record (offset 10 is the commit marker)
        let tip = tip();
        let data = partition_data(
            Some(new_transactional_records(num_records)),
            acquired_records(0, i64::from(num_records) + 1),
        );
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 15, true);
        // Should get 10 actual records (control records are filtered out)
        assert_eq!(10, batch.in_flight_records().len());
        // Should have 1 gap for the control record at offset 10
        assert_eq!(1, batch.acknowledgements().size());
        assert_eq!(
            None,
            batch.acknowledgements().get(10),
            "Offset 10 (control record) should be a GAP (null)"
        );
        assert!(batch.acknowledgements().acknowledgements_type_map().contains_key(&10));
    }

    #[test]
    fn test_mixed_records_and_gaps() {
        // Acquire records 0-4 (exist), 10-14 (don't exist = gaps)
        let mut acquired = acquired_records(0, 5);
        acquired.extend(acquired_records(10, 5));
        let tip = tip();
        let data = partition_data(Some(new_records(0, 10)), acquired);
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let mut batch = fetch(&mut completed_fetch, &deserializers, 20, true);
        let records = batch.in_flight_records();
        // Should get 5 actual records (0-4)
        assert_eq!(5, records.len());
        for (i, record) in records.iter().enumerate() {
            assert_eq!(i as i64, record.offset());
        }
        // Should have 5 gaps (10-14) in acknowledgements
        assert_eq!(5, batch.acknowledgements().size());
        // Verify GAP acknowledgements for offsets 10-14
        for offset in 10..=14 {
            assert_eq!(
                None,
                batch.acknowledgements().get(offset),
                "Offset {offset} should be a GAP (null)"
            );
        }
    }

    #[test]
    fn test_acknowledgements_include_only_gaps() {
        // Acquire only non-existent records 15-19 (all should be gaps)
        let tip = tip();
        let data = partition_data(Some(new_records(0, 10)), acquired_records(15, 5));
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let mut batch = fetch(&mut completed_fetch, &deserializers, 20, true);
        // Should get no actual records
        assert_eq!(0, batch.in_flight_records().len());
        // Should have 5 gaps (15-19) in acknowledgements
        assert_eq!(5, batch.acknowledgements().size());
        // Verify all are GAP acknowledgements
        for offset in 15..=19 {
            assert_eq!(
                None,
                batch.acknowledgements().get(offset),
                "Offset {offset} should be a GAP (null)"
            );
        }
    }

    #[test]
    fn test_gaps_with_control_records_at_beginning_and_end() {
        // Create transactional records: control record, data records 1-5, control record at 6
        let mut buffer = transaction_marker(0);
        let mut builder = batch_builder(1, true);
        for _ in 0..5 {
            builder.append_kv(0, Some(b"key"), Some(b"value"));
        }
        buffer.extend_from_slice(builder.build().buffer());
        buffer.extend_from_slice(&transaction_marker(6));

        // Acquire all offsets 0-6 (includes both control records and data records)
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(buffer)), acquired_records(0, 7));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        let fetched_records = batch.in_flight_records();

        // Should get 5 data records (1-5)
        assert_eq!(5, fetched_records.len());
        assert_eq!(1, fetched_records[0].offset());
        assert_eq!(5, fetched_records[4].offset());

        // Should have 2 gaps for the control records (offsets 0 and 6)
        assert_eq!(2, batch.acknowledgements().size());
        assert_eq!(
            None,
            batch.acknowledgements().get(0),
            "Offset 0 (control record) should be a GAP (null)"
        );
        assert_eq!(
            None,
            batch.acknowledgements().get(6),
            "Offset 6 (control record) should be a GAP (null)"
        );
    }

    #[test]
    fn test_acquire_odd_records() {
        // Acquiring all odd Records
        let acquired: Vec<AcquiredRecords> = (1..=9).step_by(2).flat_map(|i| acquired_records(i, 1)).collect();
        let tip = tip();
        let data = partition_data(Some(new_records(0, 10)), acquired);
        let deserializers = new_string_deserializers();
        let mut completed_fetch = new_share_completed_fetch(&tip, data);

        let records = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(5, records.len());
        // The first offset should be 1
        assert_eq!(1, records[0].offset());
        assert_eq!(Some(1), records[0].delivery_count());
        // The second offset should be 3
        assert_eq!(3, records[1].offset());
        assert_eq!(Some(1), records[1].delivery_count());

        let records = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(0, records.len());
    }

    #[test]
    fn test_overlapping_acquired_records_logs_error_and_retains_first_occurrence() {
        // Create overlapping acquired records: [0-9] and [5-14]
        // Offsets 5-9 will be duplicates
        let mut first = AcquiredRecords::new();
        first.set_first_offset(0).set_last_offset(9).set_delivery_count(1);
        let mut second = AcquiredRecords::new();
        second.set_first_offset(5).set_last_offset(14).set_delivery_count(2);
        let tip = tip();
        let data = partition_data(Some(new_records(0, 20)), vec![first, second]);
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // Fetch records and verify that only 15 unique records are returned (0-14)
        let records = fetch(&mut completed_fetch, &deserializers, 20, true).in_flight_records();

        // Should get 15 unique records: 0-9 from first range (with deliveryCount=1)
        // and 10-14 from second range (with deliveryCount=2)
        assert_eq!(15, records.len());

        // Verify first occurrence (offset 5 should have deliveryCount=1 from first range)
        let record5 = records.iter().find(|r| r.offset() == 5).expect("offset 5 is delivered");
        assert_eq!(Some(1), record5.delivery_count());

        // Verify offset 10 has deliveryCount=2 from second range
        let record10 = records.iter().find(|r| r.offset() == 10).expect("offset 10 is delivered");
        assert_eq!(Some(2), record10.delivery_count());

        // Verify all offsets are unique
        let mut offset_set = HashSet::new();
        for record in &records {
            assert!(
                offset_set.insert(record.offset()),
                "Duplicate offset found in results: {}",
                record.offset()
            );
        }
    }

    fn assert_corrupt(batch: &ShareInFlightBatch<String, String>) {
        let cause = batch.error().map(ConsumerShareInFlightBatchError::cause);
        assert!(
            matches!(cause, Some(Error::CorruptRecord(_))),
            "expected a CorruptRecordError, got {cause:?}"
        );
    }

    #[test]
    fn test_crc_failure_on_first_batch_then_subsequent_fetch() {
        // Create 3 batches: corrupted batch (offsets 0-9), good batch (10-19), good batch (20-29)
        let mut acquired = acquired_records(0, 10);
        acquired.extend(acquired_records(10, 10));
        acquired.extend(acquired_records(20, 10));
        let tip = tip();
        let data = partition_data(Some(new_records_with_corrupted_first_batch()), acquired);
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // First fetch should fail with CRC error and reject the entire first batch
        let mut batch1 = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert!(batch1.error().is_some(), "Should have exception for corrupted batch");
        assert_corrupt(&batch1);
        let message = batch1.error().map(|e| e.cause().message()).unwrap_or_default();
        assert!(
            message.starts_with(
                "Record batch for partition test-0 at offset 0 is invalid, cause: Record is corrupt (stored crc = 3735928559, computed crc = "
            ),
            "{message}"
        );

        // Verify all 10 records from first batch are rejected
        let acks1 = batch1.acknowledgements();
        assert_eq!(10, acks1.size(), "All records in corrupted batch should be rejected");
        for offset in 0..10 {
            assert_eq!(
                Some(AcknowledgeType::Reject),
                acks1.get(offset),
                "Record at offset {offset} should be REJECT"
            );
        }

        // No records should be returned
        assert_eq!(0, batch1.in_flight_records().len());

        // Should successfully fetch records from the second batch (offsets 10-19)
        let records2 = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(10, records2.len(), "Should get 10 records from second batch");
        assert_eq!(10, records2[0].offset(), "First record should be offset 10");
        assert_eq!(19, records2[9].offset(), "Last record should be offset 19");

        // Third fetch should process the third batch correctly
        let records3 = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(10, records3.len(), "Should get 10 records from third batch");
        assert_eq!(20, records3[0].offset(), "First record should be offset 20");
        assert_eq!(29, records3[9].offset(), "Last record should be offset 29");
    }

    #[test]
    fn test_crc_failure_with_gaps_in_batch() {
        // Only acquire odd offsets: 1, 3, 5, 7, 9
        let acquired: Vec<AcquiredRecords> = (1..10).step_by(2).flat_map(|o| acquired_records(o, 1)).collect();
        let tip = tip();
        let data = partition_data(Some(new_single_corrupted_batch(0, 10)), acquired);
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // Fetch should fail with CRC error
        let batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert!(batch.error().is_some());
        assert_corrupt(&batch);

        // Only the acquired records (odd offsets) should be rejected
        let acks = batch.acknowledgements();
        assert_eq!(5, acks.size(), "Only 5 acquired records should be rejected");
        for offset in [1, 3, 5, 7, 9] {
            assert_eq!(Some(AcknowledgeType::Reject), acks.get(offset));
        }
    }

    #[test]
    fn test_crc_failure_with_unaligned_acquired_records() {
        // Only acquire records 10-19 (second half of the corrupted batch)
        let tip = tip();
        let data = partition_data(Some(new_single_corrupted_batch(0, 20)), acquired_records(10, 10));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // Fetch should fail with CRC error
        let batch = fetch(&mut completed_fetch, &deserializers, 20, true);
        assert!(batch.error().is_some());

        // Only the acquired records (10-19) should be rejected
        let acks = batch.acknowledgements();
        assert_eq!(10, acks.size());
        for offset in 10..20 {
            assert_eq!(Some(AcknowledgeType::Reject), acks.get(offset));
        }
    }

    #[test]
    fn test_corrupted_records_with_crc_check_disabled() {
        let tip = tip();
        let data = partition_data(Some(new_single_corrupted_batch(0, 10)), acquired_records(0, 10));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // Fetch with CRC check disabled - should process records despite corruption
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, false);

        // Should get records (they're corrupted, but we're not checking)
        assert_eq!(
            10,
            batch.in_flight_records().len(),
            "Should get all records when CRC check is disabled"
        );
    }

    #[test]
    fn test_crc_failure_after_partial_fetch() {
        // Acquire all 15 records
        let tip = tip();
        let data = partition_data(
            Some(new_records_with_corrupted_second_batch_small_first()),
            acquired_records(0, 15),
        );
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // First fetch with maxRecords=10 should return 5 records from first batch,
        // then attempt to load second batch which is corrupted
        let mut batch1 = fetch(&mut completed_fetch, &deserializers, 10, true);
        let records1 = batch1.in_flight_records();
        assert_eq!(
            5,
            records1.len(),
            "Should get 5 records from first batch before hitting CRC error"
        );
        assert_eq!(0, records1[0].offset());
        assert_eq!(4, records1[4].offset());
        assert!(batch1.has_cached_error(), "Should indicate there's a cached exception");

        // Second fetch should return the cached CRC exception for the second batch
        let batch2 = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert!(batch2.error().is_some(), "Should have cached exception");
        assert_corrupt(&batch2);

        // Verify all 10 records from second batch are rejected
        let acks2 = batch2.acknowledgements();
        assert_eq!(10, acks2.size());
        for offset in 5..15 {
            assert_eq!(Some(AcknowledgeType::Reject), acks2.get(offset));
        }

        // No more records
        assert_eq!(
            0,
            fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records().len()
        );
    }

    #[test]
    fn test_multiple_consecutive_crc_failures() {
        // Acquire all records (0-39)
        let tip = tip();
        let data = partition_data(Some(new_records_with_multiple_corrupted_batches()), acquired_records(0, 40));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        // First corrupted batch (0-9)
        let batch1 = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert!(batch1.error().is_some(), "First batch should have CRC exception");
        assert_eq!(
            10,
            batch1.acknowledgements().size(),
            "First corrupted batch should reject 10 records"
        );

        // Second corrupted batch (10-19)
        let batch2 = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert!(batch2.error().is_some(), "Second batch should have CRC exception");
        assert_eq!(
            10,
            batch2.acknowledgements().size(),
            "Second corrupted batch should reject 10 records"
        );

        // Third batch should be good (20-29)
        let records3 = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(10, records3.len(), "Third batch should return 10 good records");
        assert_eq!(20, records3[0].offset());

        // Fourth batch should be good (30-39)
        let records4 = fetch(&mut completed_fetch, &deserializers, 10, true).in_flight_records();
        assert_eq!(10, records4.len(), "Fourth batch should return 10 good records");
        assert_eq!(30, records4[0].offset());
    }

    /// Rust-only: the partition's records are a slice of the fetch response
    /// and each delivered batch keeps one handle on it (decision D5), so a
    /// share fetch reads its records without copying them.
    #[test]
    fn test_delivered_batch_shares_the_response_buffer() {
        let tip = tip();
        let records = new_records(0, 3);
        let data = partition_data(Some(records.clone()), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();
        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(vec![0, 1, 2], offsets(&batch.in_flight_records()));
        assert!(!batch.holds_own_record_batches());
        // The fetch drained itself at the end of the records and let go of them.
        assert!(completed_fetch.is_consumed());
        assert!(completed_fetch.cursor.is_none());
    }

    /// Rust-only: the accessors the collector reads.
    #[test]
    fn test_initialized_and_response_accessors() {
        let tip = tip();
        let records = new_records(0, 3);
        let mut data = partition_data(Some(records.clone()), acquired_records(0, 3));
        data.set_partition_index(7);
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        assert!(!completed_fetch.is_initialized());
        completed_fetch.set_initialized();
        assert!(completed_fetch.is_initialized());
        assert_eq!(records.len(), completed_fetch.records_size());
        assert_eq!(7, completed_fetch.partition_data.partition_index);
        assert_eq!(ApiKeys::SHARE_FETCH.latest_version(), completed_fetch.request_version);
        assert!(
            completed_fetch.partition_data.records.is_none(),
            "the records moved into the cursor"
        );
    }

    /// Deserializes UTF-8 and stays pending, once, on its `pend_on`-th call
    /// (1-based; 0 never): a deserializer a dropped `poll` interrupts.
    #[derive(Clone, Default)]
    pub(crate) struct PendOnceDeserializer {
        pub(crate) calls: Arc<std::sync::atomic::AtomicUsize>,
        pub(crate) pend_on: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Deserializer<String> for PendOnceDeserializer {
        async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            use std::sync::atomic::Ordering;
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.pend_on.load(Ordering::SeqCst) == call {
                self.pend_on.store(0, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            Ok(String::from_utf8_lossy(data).into_owned())
        }
    }

    fn pend_on(deserializer: &PendOnceDeserializer, call: usize) {
        deserializer.pend_on.store(call, std::sync::atomic::Ordering::SeqCst);
    }

    /// Critic 251 Issue 1 (R4 item 8): a call dropped while the deserializer is
    /// pending on a record leaves the fetch on that record, and the next call
    /// decodes it again; it is neither skipped nor acknowledged as a gap.
    #[test]
    fn test_dropped_fetch_records_decodes_the_pending_record_again() {
        let tip = tip();
        let data = partition_data(Some(new_records(0, 3)), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = PendOnceDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));

        pend_on(&value, 1);
        assert!(
            completed_fetch.fetch_records(&deserializers, 10, true).now_or_never().is_none(),
            "dropped on offset 0"
        );

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(vec![0, 1, 2], offsets(&batch.in_flight_records()));
        assert_eq!(
            0,
            batch.acknowledgements().size(),
            "no gap for the record the dropped call was decoding"
        );
        assert!(batch.error().is_none());
    }

    /// Critic 251 Issue 3: a dropped call's batch is never tracked, so once the
    /// records handed out are acknowledged and taken, nothing stays pending.
    /// Through `fetch_records`, which keeps no state, the record the dropped
    /// call had decoded (offset 0) is lost with its batch; the pending one
    /// (offset 1) is decoded again. The consumer reads through
    /// `fetch_records_into` instead, which keeps it (next test).
    #[test]
    fn test_dropped_fetch_records_leaves_no_pending_acknowledgements_behind() {
        let tip = tip();
        let data = partition_data(Some(new_records(0, 3)), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = PendOnceDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));

        pend_on(&value, 2);
        assert!(
            completed_fetch.fetch_records(&deserializers, 10, true).now_or_never().is_none(),
            "dropped on offset 1"
        );
        assert!(
            !completed_fetch.has_pending_acknowledgements(),
            "the dropped batch is not tracked"
        );

        let mut batch = fetch(&mut completed_fetch, &deserializers, 10, true);
        assert_eq!(vec![1, 2], offsets(&batch.in_flight_records()));
        assert_eq!(0, batch.acknowledgements().size(), "offset 1 is not a gap");
        assert!(completed_fetch.has_pending_acknowledgements());
        batch.acknowledge_all(AcknowledgeType::Accept);
        batch.take_acknowledged_records();
        assert!(!completed_fetch.has_pending_acknowledgements());
    }

    /// Rust-only (Milestone 18 P7): a call dropped while the deserializer is
    /// pending leaves the records it decoded in the batch the caller keeps, and
    /// the next call with that batch decodes on from the pending record, so
    /// nothing is lost and `max_records` counts the records already there.
    #[test]
    fn test_dropped_fetch_records_into_keeps_the_records_decoded() {
        let tip = tip();
        let data = partition_data(Some(new_records(0, 3)), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = PendOnceDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));
        let mut batch = completed_fetch.new_in_flight_batch::<String, String>();

        pend_on(&value, 2);
        assert!(
            completed_fetch
                .fetch_records_into(&deserializers, 10, true, &mut batch)
                .now_or_never()
                .is_none(),
            "dropped on offset 1"
        );
        assert_eq!(1, batch.num_records(), "offset 0 stays in the batch");
        assert!(
            !completed_fetch.has_pending_acknowledgements(),
            "the batch is tracked only once it is handed out"
        );

        completed_fetch
            .fetch_records_into(&deserializers, 10, true, &mut batch)
            .now_or_never()
            .expect("ready")
            .expect("no error");
        assert_eq!(vec![0, 1, 2], offsets(&batch.in_flight_records()));
        assert_eq!(0, batch.acknowledgements().size(), "no gap, no release");
        assert!(completed_fetch.has_pending_acknowledgements());
    }

    /// Rust-only: `max_records` counts the records a dropped call left in the
    /// batch.
    #[test]
    fn test_fetch_records_into_counts_the_records_already_in_the_batch() {
        let tip = tip();
        let data = partition_data(Some(new_records_batches(0, 1, 4)), acquired_records(0, 4));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let value = PendOnceDeserializer::default();
        let deserializers = Deserializers::new(Box::new(StringDeserializer::new()), Box::new(value.clone()));
        let mut batch = completed_fetch.new_in_flight_batch::<String, String>();

        pend_on(&value, 2);
        assert!(
            completed_fetch
                .fetch_records_into(&deserializers, 2, true, &mut batch)
                .now_or_never()
                .is_none()
        );
        completed_fetch
            .fetch_records_into(&deserializers, 2, true, &mut batch)
            .now_or_never()
            .expect("ready")
            .expect("no error");
        assert_eq!(vec![0, 1], offsets(&batch.in_flight_records()), "two records in all");
    }

    /// Critic 251 Issue 4 (probe E): a batch declaring fewer records than it
    /// holds fails on its last declared record, which Java's iterator counts
    /// as read (`DefaultRecordBatch.RecordIterator#next`), so the next call
    /// reads on: it drains the fetch and acknowledges the rest as gaps.
    #[test]
    fn test_record_failure_is_moved_past_on_the_next_call() {
        let mut records = new_records(0, 3).to_vec();
        records[RecordBatch::RECORDS_COUNT_OFFSET..RecordBatch::RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&2i32.to_be_bytes());
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        let error = completed_fetch
            .fetch_records(&deserializers, 10, false)
            .now_or_never()
            .expect("ready")
            .expect_err("the batch holds more records than it declares");
        assert!(matches!(error, Error::InvalidRecord(_)), "{error:?}");
        assert!(
            error
                .message()
                .starts_with("Incorrect declared batch size for partition test-0, records still remaining in batch"),
            "{error:?}"
        );

        let batch = fetch(&mut completed_fetch, &deserializers, 10, false);
        assert!(completed_fetch.is_consumed());
        assert!(batch.error().is_none());
        assert_eq!(2, batch.acknowledgements().size());
        assert!(batch.acknowledgements().acknowledgements_type_map().contains_key(&1));
        assert!(batch.acknowledgements().acknowledgements_type_map().contains_key(&2));
        assert_eq!(None, batch.acknowledgements().get(1), "offset 1 is a gap");
        assert_eq!(None, batch.acknowledgements().get(2), "offset 2 is a gap");
    }

    /// A message-format v1 record, as its own legacy batch: Java's
    /// `LegacyRecord` layout (offset, size, crc, magic, attributes, timestamp,
    /// key, value).
    pub(crate) fn legacy_v1_record(offset: i64, value: &[u8]) -> Vec<u8> {
        let mut body = vec![0u8; 4]; // crc, not checked here
        body.push(1); // magic
        body.push(0); // attributes
        body.extend_from_slice(&0i64.to_be_bytes()); // timestamp
        body.extend_from_slice(&(-1i32).to_be_bytes()); // null key
        body.extend_from_slice(&(value.len() as i32).to_be_bytes());
        body.extend_from_slice(value);
        let mut record = offset.to_be_bytes().to_vec();
        record.extend_from_slice(&(body.len() as i32).to_be_bytes());
        record.extend_from_slice(&body);
        record
    }

    /// Critic 251 Issue 4 (probe F): a batch refused by its magic (the
    /// consumer's D7) is the current batch: its own acquired offsets are
    /// rejected, never the batch delivered before it, and the next call moves
    /// on to the next batch.
    #[test]
    fn test_refused_batch_rejects_its_own_offsets_and_is_moved_past() {
        let mut records = new_records(0, 3).to_vec();
        for offset in 3..6 {
            records.extend_from_slice(&legacy_v1_record(offset, b"legacy"));
        }
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, 6));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        let mut batch = fetch(&mut completed_fetch, &deserializers, 3, false);
        assert_eq!(vec![0, 1, 2], offsets(&batch.in_flight_records()));

        for offset in 3..6 {
            let batch = fetch(&mut completed_fetch, &deserializers, 3, false);
            assert_corrupt(&batch);
            assert_eq!(
                Some(format!(
                    "Record batch for partition test-0 at offset {offset} is invalid, cause: Record batch magic v1 is \
                     not supported: this client reads only magic v2 record batches (message formats v0 and v1 were \
                     removed in Kafka 4.0 by KIP-724)"
                )),
                batch.error().map(|e| e.cause().message().to_string())
            );
            assert_eq!(
                Some(&HashSet::from([offset])),
                batch.error().map(ConsumerShareInFlightBatchError::offsets)
            );
            assert_eq!(1, batch.acknowledgements().size(), "only the refused batch's own offset");
            assert_eq!(Some(AcknowledgeType::Reject), batch.acknowledgements().get(offset));
        }

        let mut batch = fetch(&mut completed_fetch, &deserializers, 3, false);
        assert!(batch.in_flight_records().is_empty());
        assert_eq!(0, batch.acknowledgements().size());
        assert!(completed_fetch.is_consumed());
    }

    /// Critic 251 Issue 5: a compressed batch whose stream fails surfaces a
    /// bare `KafkaError` that escapes `fetch_records`, as Java's
    /// `KafkaException` escapes `ShareCompletedFetch#fetchRecords`; nothing is
    /// rejected, and the next call moves on, acknowledging the batch's
    /// acquired offsets as gaps.
    #[test]
    fn test_failing_compressed_stream_surfaces_a_kafka_error_and_ends_as_gaps() {
        let mut records = new_records_compressed(0, 3, Compression::gzip().build()).to_vec();
        // Garble the gzip header at the start of the records section.
        records[RecordBatch::RECORD_BATCH_OVERHEAD] = 0;
        records[RecordBatch::RECORD_BATCH_OVERHEAD + 1] = 0;
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        let error = completed_fetch
            .fetch_records(&deserializers, 10, false)
            .now_or_never()
            .expect("ready")
            .expect_err("the stream fails");
        assert!(matches!(error, Error::KafkaError(_)), "a bare KafkaError: {error:?}");
        assert!(!error.is_corrupt_record_error(), "{error:?}");
        assert!(error.message().starts_with("Failed to decompress record stream: "), "{error:?}");

        let batch = fetch(&mut completed_fetch, &deserializers, 10, false);
        assert!(batch.error().is_none());
        assert!(completed_fetch.is_consumed());
        assert_eq!(3, batch.acknowledgements().size());
        for offset in 0..3 {
            assert!(batch.acknowledgements().acknowledgements_type_map().contains_key(&offset));
            assert_eq!(None, batch.acknowledgements().get(offset), "offset {offset} is a gap");
        }
    }

    /// Calls `fetch_records` until it succeeds, returning the errors it
    /// returned first and the batch it then returned. Gives up after `limit`
    /// calls, which fails the test: a fetch that never moves past a failure.
    fn fetch_past_failures(
        completed_fetch: &mut ShareCompletedFetch,
        deserializers: &Deserializers<String, String>,
        limit: usize,
    ) -> (Vec<Error>, ShareInFlightBatch<String, String>) {
        let mut errors = Vec::new();
        for _ in 0..limit {
            match completed_fetch
                .fetch_records(deserializers, 10, false)
                .now_or_never()
                .expect("ready")
            {
                Ok(batch) => return (errors, batch),
                Err(error) => errors.push(error),
            }
        }
        panic!("still failing after {limit} calls: {:?}", errors.last());
    }

    fn assert_gaps(batch: &ShareInFlightBatch<String, String>, offsets: std::ops::Range<i64>) {
        let map = batch.acknowledgements().acknowledgements_type_map();
        assert_eq!(offsets.clone().count(), map.len(), "{map:?}");
        for offset in offsets {
            assert_eq!(Some(&None), map.get(&offset), "offset {offset} is a gap");
        }
    }

    /// Critic 251 Issue 8, premature EOF: a batch declaring 5 records but
    /// holding 3 fails once per declared record past the end, each counted as
    /// read (`DefaultRecordBatch.RecordIterator#next`), then the fetch moves on
    /// and acknowledges the missing offsets as gaps.
    #[test]
    fn test_premature_eof_fails_once_per_missing_record_then_moves_on() {
        let mut records = new_records(0, 3).to_vec();
        records[RecordBatch::RECORDS_COUNT_OFFSET..RecordBatch::RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&5i32.to_be_bytes());
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, 5));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        let (errors, batch) = fetch_past_failures(&mut completed_fetch, &deserializers, 10);
        assert_eq!(2, errors.len(), "{errors:?}");
        for error in &errors {
            assert!(matches!(error, Error::InvalidRecord(_)), "{error:?}");
            assert_eq!(
                "Incorrect declared batch size for partition test-0, premature EOF reached (declared record count \
                 exceeds the records present in the batch)",
                error.message()
            );
        }
        assert!(completed_fetch.is_consumed());
        assert_gaps(&batch, 3..5);
    }

    /// Critic 251 Issue 8, malformed record body: the second record's
    /// timestamp delta is a varint that never ends. It fails once per record
    /// the batch still declares, then the fetch moves on with gaps.
    #[test]
    fn test_malformed_record_fails_once_per_remaining_record_then_moves_on() {
        let mut records = new_records(0, 3).to_vec();
        let first = RecordBatch::RECORD_BATCH_OVERHEAD;
        let (size, size_length) =
            crate::common::utils::internals::ByteUtils::read_varint(&records[first..]).expect("a size");
        let second = first + size_length + size as usize;
        let (_, second_size_length) =
            crate::common::utils::internals::ByteUtils::read_varint(&records[second..]).expect("a size");
        // After the size and the attributes byte.
        let timestamp_delta = second + second_size_length + 1;
        records[timestamp_delta..timestamp_delta + 10].fill(0xFF);
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, 3));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        let (errors, batch) = fetch_past_failures(&mut completed_fetch, &deserializers, 10);
        assert_eq!(2, errors.len(), "records 1 and 2 each fail once: {errors:?}");
        for error in &errors {
            assert!(matches!(error, Error::InvalidRecord(_)), "{error:?}");
            assert!(
                error
                    .message()
                    .starts_with("Record batch for partition test-0 at offset 0 is invalid, cause: "),
                "{error:?}"
            );
        }
        assert!(completed_fetch.is_consumed());
        assert_gaps(&batch, 1..3);
    }

    /// Critic 251 Issue 8, mid-stream decompression failure: a gzip batch cut
    /// after its first records. The stream fails on the record it is cut in,
    /// and on every record the batch still declares after it, with Java's bare
    /// `KafkaException`; then the fetch moves on and acknowledges the offsets
    /// not delivered as gaps.
    #[test]
    fn test_mid_stream_decompression_failure_fails_once_per_remaining_record_then_moves_on() {
        const RECORD_COUNT: i64 = 40;
        let mut builder = MemoryRecords::builder_with_initial_capacity(
            64 * 1024,
            Compression::gzip().build(),
            TimestampType::CreateTime,
            0,
        );
        for i in 0..RECORD_COUNT {
            let value: String = (0..100).map(|j| format!("{i}-{j},")).collect();
            builder.append_kv(0, None, Some(value.as_bytes()));
        }
        let whole = builder.build().into_buffer();
        let records_length = whole.len() - RecordBatch::RECORD_BATCH_OVERHEAD;
        let mut records = whole[..RecordBatch::RECORD_BATCH_OVERHEAD + records_length / 2].to_vec();
        let length = (records.len() - crate::common::record::internal::AbstractRecords::LOG_OVERHEAD) as i32;
        records[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&length.to_be_bytes());
        let tip = tip();
        let data = partition_data(Some(bytes::Bytes::from(records)), acquired_records(0, RECORD_COUNT));
        let mut completed_fetch = new_share_completed_fetch(&tip, data);
        let deserializers = new_string_deserializers();

        let (errors, batch) = fetch_past_failures(&mut completed_fetch, &deserializers, 100);
        let failed = errors.len() as i64;
        assert!(failed > 1 && failed < RECORD_COUNT, "the cut is mid-stream: {failed} failures");
        for error in &errors {
            assert!(matches!(error, Error::KafkaError(_)), "{error:?}");
            assert_eq!("Failed to decompress record stream: unexpected end of file", error.message());
        }
        assert!(completed_fetch.is_consumed());
        // The records before the cut were decoded into the failing call's batch,
        // which the error dropped, as Java's escaping exception drops it.
        assert_gaps(&batch, RECORD_COUNT - failed..RECORD_COUNT);
    }
}
