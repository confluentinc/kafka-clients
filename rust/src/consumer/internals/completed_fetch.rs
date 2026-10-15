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
//!   buffer (or, for compressed batches, into the chunk the record was
//!   decompressed into). We never copy key/value bytes out of the buffer;
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
//!   are decompressed one record at a time, as `poll()` asks for them, the way
//!   Java's `streamingIterator` reads them ([`RecordStream`]): each record is
//!   read into a chunk shared with the records around it and borrowed from
//!   there. A whole batch is never decompressed into one buffer, since a few
//!   kilobytes of compressed data can expand to gigabytes.
//!
//! # READ_COMMITTED
//!
//! Fully translated as of Milestone 11 Phase 8. `containsAbortMarker`
//! (`CompletedFetch.java:381-391`) is [`CompletedFetch::contains_abort_marker`],
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

#![expect(dead_code)]

use std::collections::BinaryHeap;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use bytes::Buf;
use log::{debug, error};
use rustc_hash::FxHashSet;

use crate::common::Error;
use crate::common::InvalidRecordError;
use crate::common::IsolationLevel;
use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::compress::{Compression, DecompressingReader};
use crate::common::errors::DeserializationErrorOrigin;
use crate::common::errors::RecordDeserializationError;
use crate::common::header::RecordHeaders;
use crate::common::memory::BufferSupplier;
use crate::common::protocol::Errors;
use crate::common::record::TimestampType;
use crate::common::record::internal::{
    ByteBufferLogInputStream, CompressionType, ControlRecordType, DefaultRecord, DefaultRecordBatch,
    DefaultRecordBatchRef, DefaultRecordRef, MemoryRecords, RecordBatch, RecordVersion,
};
use crate::common::serialization::Deserializer;
use crate::common::utils::internals::ByteUtils;
use crate::consumer::internals::FetchConfig;
use crate::consumer::internals::FetchMetricsAggregator;
use crate::consumer::internals::SubscriptionState;
use crate::consumer::{ConsumerRecord, ConsumerRecordOptionsBuilder};
use crate::fetch_response_data::{AbortedTransaction, PartitionData};

/// Sentinel value: a partition leader epoch that is unknown / unset.
const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// Size of the chunks a compressed batch's records are decompressed into
/// ([`RecordStream`]). Large enough that a chunk holds many ordinary records,
/// so they cost no allocation each; small next to what a batch can expand to.
const RECORD_CHUNK_SIZE: usize = 64 * 1024;

/// The most bytes a record's size field takes: a varint holding an `i32`.
const MAX_RECORD_SIZE_LENGTH: usize = 5;

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
#[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch")]
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
    /// Whether `drain` has been called. Java declares it `volatile`
    /// (KAFKA-15529) because the background thread reads it through
    /// `FetchBuffer.bufferedPartitions()` while the application thread works on
    /// the fetch. Here the background task reads it only under the
    /// [`FetchBuffer`] mutex, and the collector puts the fetch back under that
    /// mutex after the position update, so a plain `bool` suffices.
    is_consumed: bool,
    /// Whether iteration reached the end of the records. Set where Java used to
    /// call `drain()` directly; [`FetchCollector`] drains an exhausted fetch
    /// only after it has advanced the subscription position, so `is_consumed`
    /// never reads `true` next to a stale position (KAFKA-15529).
    ///
    /// [`FetchCollector`]: super::FetchCollector
    exhausted: bool,
    /// Whether the cursor has been positioned at the first batch.
    initialized: bool,

    /// Test seam standing in for Java's Mockito `spy(completedFetch)` +
    /// `doAnswer(...).when(completedFetch).drain()`: runs at every
    /// [`Self::drain`] call, before the real body, so a test can observe the
    /// state at the moment of the drain (`FetchCollectorTest
    /// .testPositionUpdatedBeforeDrainOnExhaustedFetch`).
    #[cfg(test)]
    pub(crate) on_drain_for_test: Option<Box<dyn FnMut() + Send>>,

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
    /// the current batch's record section. Unused for a compressed batch,
    /// whose stream drops each record from the front of its chunk instead.
    record_byte_offset: usize,
    /// Number of records left to decode in the current batch.
    records_remaining: i32,
}

/// Where the current batch's record bytes come from. Uncompressed batches
/// borrow their bytes directly from the cursor's `MemoryRecords` buffer (we
/// remember the byte range so we don't re-walk the batch iterator per
/// record). Compressed batches are decompressed one record at a time.
#[derive(Debug)]
enum RecordSource {
    /// No record bytes held: no batch loaded yet, the current batch was
    /// exhausted and released by [`CompletedFetch::maybe_close_record_stream`],
    /// or iteration finished.
    None,
    /// Uncompressed: record bytes are `memory_records.buffer()[range]`.
    Borrowed(std::ops::Range<usize>),
    /// Compressed: records are decompressed as the cursor reaches them, into
    /// refcounted chunks, so per-record key/value slices can be handed out
    /// zero-copy via [`bytes::Bytes::slice_ref`] (§27). Boxed, as the
    /// decompressor is large next to the other variants.
    Stream(Box<RecordStream>),
}

/// Where a batch's record bytes live *before* the batch is installed: the
/// descriptor [`CompletedFetch::load_next_batch`] carries through the
/// READ_COMMITTED skip and the record-count check, and turns into a
/// [`RecordSource`] only once both have passed. So a compressed batch is not
/// inflated for its count to be refused, nor when it is skipped as aborted —
/// Java's cost too: `compressedIterator` wraps the records in a decompression
/// stream but reads nothing through it until `RecordIterator`'s constructor
/// has accepted the count (`DefaultRecordBatch.java:279-297, 579-588`), and
/// a skipped batch never gets an iterator (`CompletedFetch.java:217-226`).
#[derive(Debug)]
enum PendingRecordSource {
    /// Uncompressed: the records section is `memory_records.buffer()[range]`,
    /// borrowed as-is once installed.
    Borrowed(std::ops::Range<usize>),
    /// Compressed: the batch's records section, still to be decompressed, and
    /// its codec. The records share the fetch buffer; nothing is copied.
    Compressed {
        records: bytes::Bytes,
        compression_type: CompressionType,
    },
}

/// A compressed batch's records, decompressed one at a time as the cursor
/// reaches them, the way Java's `streamingIterator` reads them.
///
/// This is the state of Java's `DefaultRecordBatch.StreamRecordIterator` —
/// the decompression stream over the batch's records
/// (`DefaultRecordBatch.java:279-297, 635-670`) — held by the cursor, which
/// does the counting `RecordIterator` does (DoD #7). Java reads each record
/// into a buffer of its own (`DefaultRecord.readFrom(InputStream, ...)`).
/// This reads records into chunks of [`RECORD_CHUNK_SIZE`] bytes shared by the
/// records in them, so an ordinary record costs no allocation, and hands each
/// record out as a slice of its chunk (§27). A record bigger than a chunk gets
/// a chunk of its own. A chunk is freed once the stream has moved past it and
/// no record handed out from it is left.
///
/// So a compressed batch takes the codec's own state, one chunk and one
/// record, whatever its decompressed size. The whole batch is never
/// decompressed at once: a few kilobytes of compressed data can expand to
/// gigabytes, and in Rust a failed allocation aborts the process where Java
/// throws `OutOfMemoryError`.
struct RecordStream {
    /// The decompression stream over the batch's records section.
    reader: DecompressingReader<io::Cursor<bytes::Bytes>>,
    /// Decompressed bytes not yet consumed: the current record first, then
    /// whatever was read after it.
    chunk: bytes::Bytes,
    /// Whether `reader` has reached the end of the stream.
    eof: bool,
    /// The error `reader` failed with, held back until the bytes read before
    /// it have been used up — where Java's stream would report it.
    error: Option<io::Error>,
}

impl RecordStream {
    /// Opens the decompression stream over a batch's `records` section. Reads
    /// nothing yet, except what the codec reads to set itself up (a gzip
    /// header).
    fn open(records: bytes::Bytes, compression_type: CompressionType, magic: i8) -> io::Result<Self> {
        let reader = Compression::of(compression_type)
            .build()
            .wrap_for_input(io::Cursor::new(records), magic)?;
        Ok(Self { reader, chunk: bytes::Bytes::new(), eof: false, error: None })
    }

    /// The decompressed bytes not yet consumed, starting with the current
    /// record once [`Self::next_record`] has read it.
    fn chunk(&self) -> &bytes::Bytes {
        &self.chunk
    }

    /// Reads the next record to the front of the chunk and returns the bytes
    /// there: the whole record; as much of it as the stream holds, for the
    /// record parser to reject; or nothing, once the stream has ended.
    ///
    /// # Errors
    ///
    /// The stream failed before the record was whole. Java's
    /// `StreamRecordIterator.readNext` reports any `IOException` this way
    /// (`DefaultRecordBatch.java:646-654`).
    fn next_record(&mut self) -> Result<&[u8], InvalidRecordError> {
        if !self.fill_record()
            && let Some(e) = &self.error
        {
            return Err(DefaultRecordBatch::decompression_error(e));
        }
        Ok(&self.chunk)
    }

    /// Drops the record just read, its first `consumed` bytes, from the chunk.
    fn consume(&mut self, consumed: usize) {
        self.chunk.advance(consumed.min(self.chunk.len()));
    }

    /// Whether any bytes follow the records read so far: Java's
    /// `ensureNoneRemaining`, which reads one more byte to find out
    /// (`DefaultRecordBatch.java:657-663`). Reaching the end of the stream
    /// also makes the codec verify its trailing checksum, where it has one.
    ///
    /// # Errors
    ///
    /// The stream failed before its end, with Java's text for that.
    fn has_remaining(&mut self) -> Result<bool, InvalidRecordError> {
        if self.fill_to(1) {
            return Ok(true);
        }
        match &self.error {
            Some(e) => Err(InvalidRecordError::new(format!(
                "Error checking for remaining bytes after reading batch: {e}"
            ))),
            None => Ok(false),
        }
    }

    /// Reads until the chunk holds the whole record at its front, and says
    /// whether it does. A record the stream ends or fails inside of is not
    /// whole; a record whose size field is malformed is, as no more bytes
    /// would change what the parser makes of it.
    fn fill_record(&mut self) -> bool {
        // The record's size comes first.
        self.fill_to(MAX_RECORD_SIZE_LENGTH);
        match ByteUtils::read_varint(&self.chunk) {
            Ok((size, size_length)) => match usize::try_from(size) {
                Ok(size) => self.fill_to(size_length.saturating_add(size)),
                Err(_) => true,
            },
            Err(_) => self.chunk.len() >= MAX_RECORD_SIZE_LENGTH,
        }
    }

    /// Reads until the chunk holds at least `needed` bytes, the stream ends
    /// or it fails, and says whether the chunk holds them.
    fn fill_to(&mut self, needed: usize) -> bool {
        if self.chunk.len() < needed && !self.eof && self.error.is_none() {
            self.refill(needed);
        }
        self.chunk.len() >= needed
    }

    /// Moves the bytes not yet consumed into a new chunk and reads after them,
    /// up to [`RECORD_CHUNK_SIZE`] bytes in all, or up to `needed` for a record
    /// bigger than that.
    ///
    /// The chunk grows only as bytes arrive, so a size read from the data is
    /// never allocated before the data is there, as in
    /// [`DefaultRecord::read_from_stream`]. Growing it is fallible
    /// (`read_to_end` reserves with `try_reserve`), so an allocation that cannot
    /// be met fails the stream instead of aborting the process.
    fn refill(&mut self, needed: usize) {
        let mut chunk = Vec::with_capacity(RECORD_CHUNK_SIZE);
        chunk.extend_from_slice(&self.chunk);
        let wanted = needed.max(RECORD_CHUNK_SIZE).saturating_sub(chunk.len());
        // `take` stops the read at `wanted`: a full chunk is not grown to find
        // out whether more follows.
        match (&mut self.reader).take(wanted as u64).read_to_end(&mut chunk) {
            Ok(read) => self.eof = read < wanted,
            // `read_to_end` keeps the bytes read before the failure, and the
            // records in them are still handed out.
            Err(e) => self.error = Some(e),
        }
        // Adopts the allocation; nothing is copied.
        self.chunk = bytes::Bytes::from(chunk);
    }
}

impl std::fmt::Debug for RecordStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The decompressor has no `Debug` of its own.
        f.debug_struct("RecordStream")
            .field("chunk_len", &self.chunk.len())
            .field("eof", &self.eof)
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl CompletedFetch {
    /// Constructs a `CompletedFetch` for the given partition.
    ///
    /// Translates Java's
    /// `CompletedFetch(Logger, SubscriptionState, BufferSupplier,
    ///   TopicPartition, PartitionData, FetchMetricsAggregator, Long)` —
    /// minus the logger (we use the `log` crate). Phase M3 plumbs the
    /// `FetchMetricsAggregator` (dropped by Phase 7a).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#CompletedFetch")]
    pub(crate) fn with_full(
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
            exhausted: false,
            #[cfg(test)]
            on_drain_for_test: None,
            initialized: false,
            // DIAGNOSTIC: stamp construction time only when fetch_diag is on.
            created_at: log::log_enabled!(target: "fetch_diag", log::Level::Info).then(std::time::Instant::now),
        }
    }

    /// Lightweight constructor used by tests / [`FetchBuffer`] when the
    /// subscription state and buffer supplier are not yet wired.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#CompletedFetch")]
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
            exhausted: false,
            #[cfg(test)]
            on_drain_for_test: None,
            initialized: false,
            created_at: None,
        }
    }

    /// Returns the offset the next fetch round should start at.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#nextFetchOffset")]
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#lastEpoch")]
    pub(crate) fn last_epoch(&self) -> Option<i32> {
        self.last_epoch
    }

    /// Returns whether this fetch has been initialized (i.e. the cursor
    /// has been positioned at the first batch).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#isInitialized")]
    pub(crate) fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Marks this fetch as initialized. Called by Phase 7b's
    /// `FetchCollector` after position validation.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#setInitialized")]
    pub(crate) fn set_initialized(&mut self) {
        self.initialized = true;
    }

    /// Returns whether the fetch has been fully consumed (or drained).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#isConsumed")]
    pub(crate) fn is_consumed(&self) -> bool {
        self.is_consumed
    }

    /// Returns whether iteration reached the end of the records (KAFKA-15529).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#isExhausted")]
    pub(crate) fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Drops iteration state and marks the fetch consumed. Idempotent.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#drain")]
    pub(crate) fn drain(&mut self) {
        #[cfg(test)]
        if let Some(hook) = self.on_drain_for_test.as_mut() {
            hook();
        }
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#fetchRecords")]
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
        // (`CompletedFetch.java:271-306`). Three effects per arm: cache the
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
                    Err(Error::KafkaError(KafkaError::with_message_source(
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
    /// (`CompletedFetch.java:271-294`).
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

        // §27 / CLAUDE.md §13 metrics-cost invariant (Milestone-9 Phase M8):
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
                    return Err(premature_eof_error(&self.partition));
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
                // By the cause's message, not its class-prefixed `Display`, as in
                // `peek_current_record`.
                let headers_vec = record.headers().map_err(|e| {
                    Error::InvalidRecord(InvalidRecordError::new(format!(
                        "Record for partition {} at offset {} has invalid headers, cause: {}",
                        self.partition,
                        record.offset(),
                        e.message()
                    )))
                })?;
                headers_owned = RecordHeaders::with_header_iter(headers_vec);
                key_result = match record.key() {
                    None => Ok(None),
                    Some(key_bytes) => key_deserializer
                        .deserialize_from_shared_headers(topic_str, &headers_owned, &source_bytes, key_bytes)
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
                            .deserialize_from_shared_headers(topic_str, &headers_owned, &source_bytes, value_bytes)
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
            let consumer_record = ConsumerRecord::with_options(
                ConsumerRecordOptionsBuilder::new()
                    .set_topic(topic_arc)
                    .set_partition(self.partition.partition())
                    .set_offset(offset)
                    .set_key(key)
                    .set_value(value)
                    .set_timestamp(timestamp)
                    .set_timestamp_type(timestamp_type)
                    .set_serialized_key_size(key_size)
                    .set_serialized_value_size(value_size)
                    .set_headers(headers_owned)
                    .set_leader_epoch(leader_epoch)
                    .build()?,
            );
            self.records_read += 1;
            self.bytes_read += record_size_in_bytes;
            self.next_fetch_offset = offset + 1;
            self.cached_record_error = None;
            out.push(consumer_record);
            // Advance the record cursor — we successfully consumed this
            // record.
            self.advance_record_cursor(record_bytes_consumed);
        }

        Ok(())
    }

    /// Releases the current batch's record bytes once the batch is exhausted:
    /// Java's `maybeCloseRecordStream()` (`CompletedFetch.java:175-180`), which
    /// `nextFetchedRecord` calls before it takes the next batch (`:185`).
    ///
    /// For a compressed batch this drops the batch's [`RecordStream`] — its
    /// decompressor and its chunk — before the next batch's stream is opened,
    /// so a fetch holds one stream at a time, not two. Records a zero-copy
    /// deserializer sliced from a chunk keep their own reference, so for them
    /// the chunk lives on until they are dropped. `drain()`, Java's other
    /// caller, drops the whole cursor instead.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#maybeCloseRecordStream")]
    fn maybe_close_record_stream(&mut self) {
        if let Some(cursor) = self.cursor.as_mut() {
            cursor.record_source = RecordSource::None;
        }
    }

    /// Advances the cursor to the next record that should be returned
    /// to the user, skipping out-of-range, aborted-transaction, and
    /// control batches per READ_COMMITTED semantics.
    ///
    /// Returns `Ok(true)` if a record is positioned at the cursor and
    /// ready to be read via [`Self::peek_current_record`]. Returns
    /// `Ok(false)` when iteration is exhausted; in that case the fetch is
    /// marked exhausted (not drained: see [`Self::is_exhausted`]) and
    /// `next_fetch_offset` is advanced to the end of the last batch.
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
                // records. For an uncompressed batch the check is O(1) — it
                // compares the already-walked byte offset against the
                // (already-known) record section length, with no re-walk or
                // copy; a compressed batch's stream is read one byte further.
                self.ensure_current_batch_fully_consumed()?;
                self.maybe_close_record_stream();
                if !self.load_next_batch(config)? {
                    // No more batches. Advance to the next-after-last-batch
                    // offset (mirrors Java's `nextFetchOffset = currentBatch.nextOffset()`).
                    if let Some(cursor) = &self.cursor
                        && let Some(batch_meta) = &cursor.current_batch
                    {
                        self.next_fetch_offset = batch_meta.next_offset;
                    }
                    // Java (KAFKA-15529, `CompletedFetch.java:200`): mark the
                    // fetch exhausted; `FetchCollector` drains it after the
                    // position update.
                    self.exhausted = true;
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
                self.read_current_record()?;
                let Some((record, batch_meta)) = self.peek_current_record()? else {
                    return Err(premature_eof_error(&self.partition));
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
            match &mut cursor.record_source {
                RecordSource::Stream(stream) => stream.consume(record_bytes_consumed),
                _ => cursor.record_byte_offset += record_bytes_consumed,
            }
            cursor.records_remaining -= 1;
        }
    }

    /// Reads the current record of a compressed batch out of its stream, for
    /// [`Self::peek_current_record`] to parse. An uncompressed batch's records
    /// are in place already, so for one this does nothing.
    ///
    /// # Errors
    ///
    /// The stream failed before the record was whole, reported the way
    /// `maybeEnsureValid` reports a bad batch ([`invalid_batch_error`]).
    fn read_current_record(&mut self) -> Result<(), Error> {
        let Some(cursor) = self.cursor.as_mut() else {
            return Ok(());
        };
        let (RecordSource::Stream(stream), Some(batch)) = (&mut cursor.record_source, &cursor.current_batch) else {
            return Ok(());
        };
        match stream.next_record() {
            Ok(_) => Ok(()),
            Err(e) => Err(invalid_batch_error(&self.partition, batch.base_offset, e.message())),
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
    /// For an uncompressed batch this is O(1): it compares the already-walked
    /// `record_byte_offset` against the (already-known) length of the batch's
    /// record section — no re-walk and no copy. A compressed batch's stream is
    /// read one byte further, as Java's `StreamRecordIterator` reads it, which
    /// also lets the codec verify the stream's trailing checksum. Returns
    /// `Ok(())` when there is no batch loaded yet (nothing to validate).
    ///
    /// # Errors
    ///
    /// Records remain, or the compressed stream failed while looking for them
    /// (Java's `KafkaException("Error checking for remaining bytes after
    /// reading batch", e)`, `DefaultRecordBatch.java:657-663`).
    fn ensure_current_batch_fully_consumed(&mut self) -> Result<(), Error> {
        let Some(cursor) = self.cursor.as_mut() else {
            return Ok(());
        };
        // Only meaningful once a batch has been loaded and its declared count
        // consumed. `current_batch == None` means we have not loaded a batch
        // yet (first iteration).
        let Some(batch) = cursor.current_batch.as_ref() else {
            return Ok(());
        };
        if cursor.records_remaining > 0 {
            return Ok(());
        }
        let records_remain = match &mut cursor.record_source {
            RecordSource::None => false,
            RecordSource::Borrowed(range) => cursor.record_byte_offset < range.len(),
            RecordSource::Stream(stream) => stream
                .has_remaining()
                .map_err(|e| invalid_batch_error(&self.partition, batch.base_offset, e.message()))?,
        };
        if records_remain {
            return Err(records_remaining_error(&self.partition));
        }
        Ok(())
    }

    /// Returns the refcounted buffer that owns the current batch's record
    /// bytes, so per-record key/value slices can be handed to the deserializer
    /// as zero-copy `Bytes` via [`bytes::Bytes::slice_ref`] (§27).
    ///
    /// For an uncompressed batch this is the whole `MemoryRecords` buffer (the
    /// borrowed record slice is a subslice of it); for a compressed batch it is
    /// the chunk the current record was decompressed into. Clones are O(1)
    /// refcount bumps. Returns `None` when no batch's record bytes are held.
    fn current_record_source_bytes(&self) -> Option<bytes::Bytes> {
        let cursor = self.cursor.as_ref()?;
        match &cursor.record_source {
            RecordSource::None => None,
            RecordSource::Borrowed(_) => Some(cursor.memory_records.buffer_bytes().clone()),
            RecordSource::Stream(stream) => Some(stream.chunk().clone()),
        }
    }

    /// Parses the current record (the one at `cursor.record_byte_offset`)
    /// into a borrowing [`DefaultRecordRef`] and returns it together with its
    /// enclosing batch metadata, both borrowing from `self`.
    ///
    /// A compressed batch's current record must have been read out of its
    /// stream first, by [`Self::read_current_record`].
    ///
    /// Returns:
    ///   - `Ok(None)` when no record is positioned at the cursor — either the
    ///     declared record count is exhausted (`records_remaining <= 0`) or the
    ///     batch's record bytes are exhausted (`record_byte_offset` past end,
    ///     a compressed stream ended, or the bytes end inside the next
    ///     record's size).
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
            // The current record is at the front of the chunk, and
            // `record_byte_offset` stays 0.
            RecordSource::Stream(stream) => &stream.chunk()[..],
        };
        if cursor.record_byte_offset >= records_bytes.len()
            || ends_inside_record_size(&records_bytes[cursor.record_byte_offset..])
        {
            return Ok(None);
        }
        let from_stream = matches!(cursor.record_source, RecordSource::Stream(_));
        // A parse failure here is bad input (the record body is malformed),
        // not a logic bug: the receive path no longer re-walks the batch's
        // records on load, so this is the first time the record body is
        // decoded. Surface it as a recoverable error rather than panicking
        // or silently dropping the rest of the batch — matching Java
        // `DefaultRecordBatch.RecordIterator`, which throws
        // `InvalidRecordException` (CRC-independent).
        //
        // The cause is appended by its message, not its `Display`, which
        // prefixes the class name (`InvalidRecordError: `) that Java's
        // `getMessage()` does not carry. Java propagates this exception
        // unwrapped; the wrapper that names the partition and offset is this
        // client's.
        let (record, _consumed) = read_record(batch_meta, &records_bytes[cursor.record_byte_offset..], from_stream)
            .map_err(|e| {
                Error::InvalidRecord(InvalidRecordError::new(format!(
                    "Record batch for partition {} at offset {} is invalid, cause: {}",
                    self.partition,
                    batch_meta.base_offset,
                    e.message()
                )))
            })?;
        Ok(Some((record, batch_meta)))
    }

    /// Whether `batch` is a control batch whose first record is an ABORT marker.
    ///
    /// Translated from `CompletedFetch.containsAbortMarker(RecordBatch)`
    /// (`CompletedFetch.java:381-391`): non-control batches are `false`, an empty
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
    /// `ControlRecordType.parse`, and from the `RecordIterator` that
    /// `batch.iterator()` builds when the header's `records_count` is negative.
    /// For a compressed batch, Java's `iterator()` reads every record before the
    /// first is looked at, so any record's fault, or the stream's, fails it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#containsAbortMarker")]
    fn contains_abort_marker(
        &self,
        batch: &BatchMetadata,
        records_count: i32,
        source: &PendingRecordSource,
    ) -> Result<bool, Error> {
        if !batch.is_control_batch {
            return Ok(false);
        }
        // `batch.iterator()` (`DefaultRecordBatch.java:321-337`) constructs a
        // `RecordIterator`, whose constructor rejects a negative count before any
        // record is read (`:584-587`) — the same check `load_next_batch` applies to
        // a batch it installs (D3).
        if records_count < 0 {
            return Err(Error::InvalidRecord(DefaultRecordBatch::invalid_record_count_error(
                records_count,
                batch.magic,
            )));
        }
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(false);
        };
        // Java's `batch.iterator()` reads the control batch on its own, apart from
        // the `streamingIterator` that `load_next_batch` installs afterwards
        // (`DefaultRecordBatch.java:321-337`, `CompletedFetch.java:226`); so does
        // this, decompressing a compressed control batch here and again at
        // install. The broker writes control batches uncompressed, so in practice
        // neither happens.
        let (records, compression_type) = match source {
            PendingRecordSource::Borrowed(range) => {
                let records_bytes = &cursor.memory_records.buffer()[range.clone()];
                // Java's `if (!batchIterator.hasNext()) return false` — an empty
                // control batch carries no marker.
                if records_bytes.is_empty() {
                    return Ok(false);
                }
                let (record, _consumed) = self.read_control_record(batch, records_bytes, false)?;
                return self.is_abort_marker(batch, &record);
            },
            PendingRecordSource::Compressed { records, compression_type } => (records, *compression_type),
        };

        // `iterator()` reads nothing of an empty batch (`DefaultRecordBatch.java:322-323`).
        if records_count == 0 {
            return Ok(false);
        }
        // Java's `iterator()` collects the records of a compressed batch into a
        // list (`DefaultRecordBatch.java:331-335`). They are read one at a time
        // here, and only what the marker needs — the first record's control
        // type — is kept, so a large control batch takes no more memory than a
        // data batch does (see [`RecordStream`]).
        let mut stream = RecordStream::open(records.clone(), compression_type, batch.magic).map_err(|e| {
            invalid_batch_error(
                &self.partition,
                batch.base_offset,
                DefaultRecordBatch::decompression_error(&e).message(),
            )
        })?;
        let mut first_is_abort_marker = None;
        for _ in 0..records_count {
            let records_bytes = stream
                .next_record()
                .map_err(|e| invalid_batch_error(&self.partition, batch.base_offset, e.message()))?;
            if ends_inside_record_size(records_bytes) {
                return Err(premature_eof_error(&self.partition));
            }
            let (record, consumed) = self.read_control_record(batch, records_bytes, true)?;
            // The first record's key is parsed now but its fault reported only
            // once every record has been read, as Java parses it after
            // `iterator()` has returned.
            if first_is_abort_marker.is_none() {
                first_is_abort_marker = Some(self.is_abort_marker(batch, &record));
            }
            stream.consume(consumed);
        }
        // Java's `ensureNoneRemaining`, after the last declared record.
        if stream
            .has_remaining()
            .map_err(|e| invalid_batch_error(&self.partition, batch.base_offset, e.message()))?
        {
            return Err(records_remaining_error(&self.partition));
        }
        first_is_abort_marker.unwrap_or(Ok(false))
    }

    /// Parses the control record at the front of `records_bytes`, read out of a
    /// compressed stream when `from_stream`, returning it and its length.
    ///
    /// By the cause's message, as in `peek_current_record`: Java's
    /// `batchIterator.next()` throws the `InvalidRecordException` unwrapped.
    fn read_control_record<'a>(
        &self,
        batch: &BatchMetadata,
        records_bytes: &'a [u8],
        from_stream: bool,
    ) -> Result<(DefaultRecordRef<'a>, usize), Error> {
        read_record(batch, records_bytes, from_stream).map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!(
                "Control batch for partition {} at offset {} is invalid, cause: {}",
                self.partition,
                batch.base_offset,
                e.message()
            )))
        })
    }

    /// Whether a control batch's first record is an ABORT marker: Java's
    /// `ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key())`
    /// (`CompletedFetch.java:362-363`).
    fn is_abort_marker(&self, batch: &BatchMetadata, record: &DefaultRecordRef<'_>) -> Result<bool, Error> {
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
            // copy). For compressed batches we take the records section as a
            // shared slice of the fetch buffer and, once the checks below have
            // passed, open a stream that decompresses it one record at a time.
            let (batch_meta, source, records_count) = {
                let cursor = match &mut self.cursor {
                    Some(c) => c,
                    None => return Ok(false),
                };
                let Some(batch_start) = cursor.next_batch_start else {
                    return Ok(false);
                };

                let buffer = cursor.memory_records.buffer();
                // Java walks a fetch's batches with `ByteBufferLogInputStream.nextBatch()`
                // (`ByteBufferLogInputStream.java:41-58`): every header goes through
                // `nextBatchSize()` — sign, minimum size, magic — before the batch is
                // handed out as a slice limited to exactly its declared size. The header
                // is never trusted for where the next batch starts until it has passed.
                // The stream borrows the cursor's buffer, so it is rebuilt over the unread
                // tail per batch rather than stored in the cursor; it is a slice, a
                // position and a limit on the stack, and allocates nothing.
                let unread = buffer.get(batch_start..).unwrap_or_default();
                // A corrupt size or magic propagates as the stream's `CORRUPT_MESSAGE`
                // error, unwrapped, as Java's `batches.hasNext()` throws it
                // (`CompletedFetch.java:192`) — it is not "no batch".
                let batch_size = match ByteBufferLogInputStream::new(unread, i32::MAX).next_batch_size()? {
                    Some(batch_size) if batch_size <= unread.len() => batch_size,
                    // `batchSize == null || remaining < batchSize` → no batch (`:44-46`):
                    // a partial trailing batch, as a broker cutting the response at
                    // `max.partition.fetch.bytes` leaves, ends iteration without an error.
                    _ => {
                        cursor.next_batch_start = None;
                        return Ok(false);
                    },
                };
                let batch_bytes = &unread[..batch_size];

                // D7: message formats v0 and v1 are refused by their
                // magic before anything reads the header as a v2 one. In bounds:
                // `next_batch_size` returns a size only once the magic byte is present.
                let magic = batch_bytes[RecordBatch::MAGIC_OFFSET] as i8;
                if magic < RecordBatch::MAGIC_VALUE_V2 {
                    return Err(invalid_batch_error(
                        &self.partition,
                        raw_base_offset(batch_bytes),
                        ByteBufferLogInputStream::unsupported_magic_error(magic).message(),
                    ));
                }

                // D2: the size half of Java's `ensureValid()`
                // (`DefaultRecordBatch.java:152-154`), reported the way
                // `maybeEnsureValid` reports it (`CompletedFetch.java:153-162`) but run
                // unconditionally, where Java runs it only under `check.crcs`. Without
                // `check.crcs` Java reads the header of a batch shorter than 61 bytes
                // anyway and its absolute reads throw `IndexOutOfBoundsException`; in
                // Rust those reads would be slice panics, which inside the C and Python
                // bindings abort the process. The view is only built over a batch that passes, so no
                // header accessor below can read past it.
                let batch = DefaultRecordBatchRef::new(batch_bytes)
                    .map_err(|e| invalid_batch_error(&self.partition, raw_base_offset(batch_bytes), e.message()))?;

                // The checksum half of Java's `maybeEnsureValid(batch)`, still gated
                // on `check.crcs` as in Java.
                if config.check_crcs
                    && batch.magic() >= RecordVersion::V2.value()
                    && let Err(e) = batch.ensure_valid()
                {
                    return Err(invalid_batch_error(&self.partition, batch.base_offset(), e.message()));
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

                // Describe where this batch's records live; they are materialized
                // below, after the READ_COMMITTED skip and the count check.
                // `try_compression_type`, not `compression_type`: this batch came
                // off the wire, and an unknown codec id must fail as Java's
                // `CompressionType.forId` does rather than be read as
                // uncompressed (which would parse compressed bytes as records).
                //
                // `batch_size` includes AbstractRecords::LOG_OVERHEAD, so the
                // records section is
                // [batch_start + RECORD_BATCH_OVERHEAD, batch_start + size).
                let records_start = batch_start + RecordBatch::RECORD_BATCH_OVERHEAD;
                let records_end = batch_start + batch_size;
                let source = match batch.try_compression_type()? {
                    // Borrow the records section directly from the canonical
                    // buffer.
                    CompressionType::None => PendingRecordSource::Borrowed(records_start..records_end),
                    // Decompressed one record at a time once the batch is
                    // installed. The slice shares the fetch buffer (a refcount
                    // bump), so the stream can own its input.
                    compression_type => PendingRecordSource::Compressed {
                        records: cursor.memory_records.buffer_bytes().slice(records_start..records_end),
                        compression_type,
                    },
                };

                let records_count = batch.records_count();
                // Advance the cursor's next-batch pointer by this batch's validated
                // size (O(1)) so both the skip path and the load path move forward.
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
                if self.contains_abort_marker(&batch_meta, records_count, &source)? {
                    self.aborted_producer_ids.remove(&batch_meta.producer_id);
                } else if batch_meta.is_transactional && self.aborted_producer_ids.contains(&batch_meta.producer_id) {
                    // Java's `isBatchAborted`, which gates on `isTransactional()` —
                    // a non-transactional batch with a producer id is never aborted.
                    debug!(
                        "Skipping aborted record batch from partition {} with producerId {} and offsets {} to {}",
                        self.partition, batch_meta.producer_id, batch_meta.base_offset, batch_meta.last_offset
                    );
                    self.next_fetch_offset = batch_meta.next_offset;
                    // `source` is dropped here still pending — an aborted batch's
                    // records are never decoded, nor inflated.
                    continue;
                }
            }

            // D3: Java's `RecordIterator` constructor rejects a
            // negative record count (`DefaultRecordBatch.java:584-587`) when
            // `currentBatch.streamingIterator(...)` builds it (`CompletedFetch.java:226`),
            // unwrapped and after the READ_COMMITTED skip — so an aborted batch is
            // dropped without its count being looked at. Installing it instead would
            // treat the batch as empty. It also comes before the records are
            // materialized, so a compressed batch with a bad count is refused for
            // the price of reading its header: Java's `compressedIterator` has
            // built the decompression stream by then but read nothing through it.
            if records_count < 0 {
                return Err(Error::InvalidRecord(DefaultRecordBatch::invalid_record_count_error(
                    records_count,
                    batch_meta.magic,
                )));
            }

            // Materialize the records: borrow them in place, or open the
            // stream a compressed batch's records are read through, one at a
            // time, as the cursor reaches them (§27).
            let source = match source {
                PendingRecordSource::Borrowed(range) => RecordSource::Borrowed(range),
                PendingRecordSource::Compressed { records, compression_type } => {
                    let stream = RecordStream::open(records, compression_type, batch_meta.magic).map_err(|e| {
                        invalid_batch_error(
                            &self.partition,
                            batch_meta.base_offset,
                            DefaultRecordBatch::decompression_error(&e).message(),
                        )
                    })?;
                    RecordSource::Stream(Box::new(stream))
                },
            };
            let Some(cursor) = self.cursor.as_mut() else {
                return Ok(false);
            };

            // Install the batch as the current one. We use the batch
            // header's declared record count (not the offset span, which can
            // exceed the record count after log compaction).
            cursor.record_source = source;
            cursor.record_byte_offset = 0;
            cursor.records_remaining = records_count;
            cursor.current_batch = Some(batch_meta);
            return Ok(true);
        }
    }

    /// Drains aborted-transaction entries up to and including `offset`,
    /// recording their producer IDs in `aborted_producer_ids`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#consumeAbortedTransactionsUpTo")]
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
/// Mirrors `newRecordDeserializationException` (`CompletedFetch.java:341-350`),
/// which passes the full record context — origin, partition, offset, timestamp,
/// timestamp type, raw key and value buffers, and headers — alongside the
/// message, with the deserializer's exception as the **cause**. Those eight
/// fields are the ones consumer error handlers read to decide whether to skip
/// the record, so a plain `SerializationException` carrying only the message
/// would drop the information the type exists to convey.
///
/// The message itself carries no "Cause: ..." suffix; the cause is a separate
/// field reachable through [`Error::source`].
#[expect(clippy::too_many_arguments)]
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

/// Java's `maybeEnsureValid(batch)` wrapper (`CompletedFetch.java:158-167`):
/// `new KafkaException("Record batch for partition " + partition + " at offset " +
/// batch.baseOffset() + " is invalid, cause: " + e.getMessage())`, a bare
/// `KafkaException` carrying the cause's message and no cause.
///
/// Every per-batch check in [`CompletedFetch::load_next_batch`] reports through
/// it: the magic (D7), the minimum size (D2) and the checksum. So does a
/// compressed batch's stream, when it fails to decompress or fails while the
/// cursor checks that nothing follows the batch's records ([`RecordStream`]).
fn invalid_batch_error(partition: &TopicPartition, base_offset: i64, cause: &str) -> Error {
    Error::kafka_message(format!(
        "Record batch for partition {partition} at offset {base_offset} is invalid, cause: {cause}"
    ))
}

/// The batch declares more records than it holds: Java's
/// `InvalidRecordException("Incorrect declared batch size, premature EOF
/// reached")` (`DefaultRecordBatch.java:308, 650`), naming the partition.
fn premature_eof_error(partition: &TopicPartition) -> Error {
    Error::InvalidRecord(InvalidRecordError::new(format!(
        "Incorrect declared batch size for partition {partition}, premature EOF reached \
         (declared record count exceeds the records present in the batch)"
    )))
}

/// Whether `records` end before a record's size does, so no record starts at
/// their front. Java reports this as a premature EOF: the buffer reader runs
/// out of bytes (`BufferUnderflowException`, `DefaultRecordBatch.java:308`), and
/// the stream reader takes the end of the stream for a continuation byte
/// (`IllegalArgumentException`, `:650`). A stream that fails rather than ends
/// inside the size is reported by [`RecordStream::next_record`] first.
fn ends_inside_record_size(records: &[u8]) -> bool {
    records.len() < MAX_RECORD_SIZE_LENGTH && records.iter().all(|b| b & 0x80 != 0)
}

/// Parses the record at the front of `records`, the rest of `batch`'s record
/// bytes, returning it and its length.
///
/// A record whose body is cut off fails with the text of the Java reader that
/// reads it: the buffer reader's for an uncompressed batch
/// (`DefaultRecord.java:328-331`), the stream reader's when `from_stream`
/// (`DefaultRecord.java:304-306`). A stream's records end early only where the
/// stream does, as [`RecordStream::next_record`] reads each record whole first.
fn read_record<'a>(
    batch: &BatchMetadata,
    records: &'a [u8],
    from_stream: bool,
) -> Result<(DefaultRecordRef<'a>, usize), InvalidRecordError> {
    let log_append_time = (batch.timestamp_type == TimestampType::LogAppendTime).then_some(batch.last_offset_timestamp);
    DefaultRecord::read_ref_from_buffer(
        records,
        batch.base_offset,
        batch.base_timestamp,
        batch.base_sequence,
        log_append_time,
    )
    .map_err(|e| {
        // Checked only once parsing has failed, so a record read whole costs
        // nothing more.
        match ByteUtils::read_varint(records) {
            Ok((size, size_length))
                if from_stream && usize::try_from(size).is_ok_and(|size| records.len() - size_length < size) =>
            {
                InvalidRecordError::new(format!(
                    "Invalid record size: expected {size} bytes in record payload, but the record payload reached EOF."
                ))
            },
            _ => e,
        }
    })
}

/// The batch holds more records than it declares: Java's
/// `InvalidRecordException("Incorrect declared batch size, records still
/// remaining in file")` (`DefaultRecordBatch.java:617-618`), naming the
/// partition.
fn records_remaining_error(partition: &TopicPartition) -> Error {
    Error::InvalidRecord(InvalidRecordError::new(format!(
        "Incorrect declared batch size for partition {partition}, records still remaining in batch \
         (declared record count is fewer than the records present)"
    )))
}

/// The `BaseOffset` field — a batch's first eight bytes — read before its
/// [`DefaultRecordBatchRef`] exists, so a batch refused by the magic or the size
/// check is still reported at its offset, as Java's `batch.baseOffset()` reports
/// it. The slice always has them: it passed `next_batch_size`, so it is at least
/// `LOG_OVERHEAD + 14` bytes long, and the `-1` fallback would only mark that
/// invariant broken in the message rather than panic.
fn raw_base_offset(batch_bytes: &[u8]) -> i64 {
    batch_bytes
        .first_chunk::<{ RecordBatch::BASE_OFFSET_LENGTH }>()
        .map_or(-1, |base_offset| i64::from_be_bytes(*base_offset))
}

#[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#maybeLeaderEpoch")]
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
            .field("exhausted", &self.exhausted)
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
    use crate::common::record::internal::AbstractRecords;
    use crate::common::record::internal::CompressionType;
    use crate::common::record::internal::{MemoryRecords, MemoryRecordsBuilderOptionsBuilder, SimpleRecord};
    use crate::common::serialization::Deserializer;
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use crate::consumer::internals::FetchMetricsManager;
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
                SimpleRecord::with_timestamp_key_value(0, Some("key".as_bytes().to_vec()), Some(value.into_bytes()))
            })
            .collect();
        let records = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
            2,
            base_offset,
            Compression::none().build(),
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
                SimpleRecord::with_timestamp_key_value(0, Some(key.into_bytes()), Some(value.into_bytes()))
            })
            .collect();
        let records = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
            2,
            base_offset,
            Compression::none().build(),
            TimestampType::CreateTime,
            &simple_records,
        );
        records.buffer().to_vec()
    }

    /// Like [`new_records`] but with the batch records compressed, to
    /// exercise the `RecordSource::Stream` (record-at-a-time) path.
    fn new_compressed_records(base_offset: i64, count: i32, first_message_id: i64) -> Vec<u8> {
        let simple_records: Vec<SimpleRecord> = (0..count)
            .map(|i| {
                let value = format!("value-{}", first_message_id + i as i64);
                SimpleRecord::with_timestamp_key_value(0, Some("key".as_bytes().to_vec()), Some(value.into_bytes()))
            })
            .collect();
        let records = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
            2,
            base_offset,
            Compression::gzip().build(),
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
                    SimpleRecord::with_timestamp_key_value(
                        0,
                        Some(format!("key-{n}").into_bytes()),
                        Some(format!("value-{n}").into_bytes()),
                    )
                })
                .collect();
            let records = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
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
        CompletedFetch::with_full(
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
    /// ZERO per-record cost on the receive hot path (CLAUDE.md §13 / §27).
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
            let _guard = crate::AllocTrackingGuard::new();
            crate::AllocTrackingGuard::reset();
            // The measured call drives ONLY the per-record loop — no drain().
            let recs = cf
                .fetch_records::<usize, usize>(&fetch_config, &key_de, &value_de, RECORD_COUNT)
                .unwrap();
            alloc_count = crate::AllocTrackingGuard::count();
            n_records = recs.len();
        }

        assert_eq!(RECORD_COUNT as usize, n_records, "fetch_records did not return all records");

        let max_allowed = OVERHEAD_BUDGET + ALLOC_BUDGET_PER_RECORD * (RECORD_COUNT as usize);
        assert!(
            alloc_count <= max_allowed,
            "Metrics regression on the per-record path: {alloc_count} allocs for {RECORD_COUNT} \
             records (budget {max_allowed}). The per-record loop must stay pure i32 counter \
             accumulation — an *allocating* metric operation (e.g. `aggregator.record(...)` or a \
             windowed-stat sample rotation) entered the loop (CLAUDE.md §13 / §27)."
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetchTest#testSimple")]
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

    /// Exercises the compressed-batch decode path (`RecordSource::Stream`):
    /// records are decompressed one at a time into a shared chunk, then each
    /// record's key/value is borrowed from it — same observable result as the
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
        for compression in [Compression::none().build(), Compression::gzip().build()] {
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
            // Exhausted, not drained: `FetchCollector` drains after the position
            // update (KAFKA-15529).
            assert!(cf.is_exhausted());
            assert!(!cf.is_consumed());
        }
    }

    /// Builds a single uncompressed v2 batch at `base_offset` holding `count`
    /// `value-{offset}` records, then overwrites its declared record count with
    /// `declared_count`. The CRC is NOT recomputed, so this fixture must be
    /// driven with `check.crcs=false` — exactly the configuration under which
    /// Java's `DefaultRecordBatch.RecordIterator` count validation (which is
    /// CRC-independent) still has to fire.
    fn batch_with_overridden_record_count(base_offset: i64, count: i32, declared_count: i32) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
            512,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none().build(),
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
    fn batch_full(
        base_offset: i64,
        count: i32,
        producer_id: i64,
        is_transactional: bool,
        is_control_batch: bool,
    ) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(512)
                .set_magic(RecordBatch::MAGIC_VALUE_V2)
                .set_compression(Compression::none().build())
                .set_timestamp_type(TimestampType::CreateTime)
                .set_base_offset(base_offset)
                .set_log_append_time(-1)
                .set_producer_id(
                    // log_append_time
                    producer_id,
                )
                .set_producer_epoch(0)
                .set_base_sequence(
                    // producer_epoch
                    0,
                )
                .set_is_transactional(
                    // base_sequence
                    is_transactional,
                )
                .set_is_control_batch(is_control_batch)
                .set_partition_leader_epoch(-1)
                .set_write_limit(
                    // partition_leader_epoch
                    512,
                )
                .build()
                .expect("MemoryRecordsBuilderOptionsBuilder::build: every mandatory parameter is set above"),
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
        // because `records` is non-empty (`CompletedFetch.java:299-305`) and
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
        // record to continue consumption.", e)` (`CompletedFetch.java:262`).
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
            !crate::common::requests::RequestUtils::is_fatal_error(&err),
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
                    // cause (`CompletedFetch.java:299-305`). Java's literal text is
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
                        !crate::common::requests::RequestUtils::is_fatal_error(&e),
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
        let mut cf = CompletedFetch::with_full(
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetchTest#testNegativeFetchCount")]
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetchTest#testNoRecordsInFetch")]
    fn test_no_records_in_fetch() {
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(10);
        partition_data.set_last_stable_offset(20);
        partition_data.set_log_start_offset(0);
        // records is None (Java sets it to null) — but the auto-generated
        // setter also accepts None.
        partition_data.set_records(None);
        let mut cf = CompletedFetch::with_full(
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
        assert_eq!(DeserializationErrorOrigin::Key, rde.origin());
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
            let mut cf = CompletedFetch::with_full(
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
            let mut cf = CompletedFetch::with_full(
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
        let mut cf = CompletedFetch::with_full(
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
        // Exhausted, not drained: `FetchCollector` drains after the position
        // update (KAFKA-15529).
        assert!(cf.is_exhausted());
        assert!(!cf.is_consumed());
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

    // ── Decode hardening: every batch header is validated ───────────────────
    //
    // Java walks a fetch with `ByteBufferLogInputStream.nextBatch()`, which runs
    // `nextBatchSize()` on every batch and hands each out limited to its declared
    // size. The cursor used to check only the first batch (in
    // `FetchCollector::initialize`) and trust every later header, so these tests
    // corrupt the SECOND batch of a payload whose first batch is intact.

    const SEEK_PAST_MESSAGE: &str = "Received an error when fetching the next record from test-0. \
                                     If needed, please seek past the record to continue consumption.";

    /// Two uncompressed batches of two records each — offsets 0-1, then 2-3 —
    /// and the position of the second.
    fn two_batches() -> (Vec<u8>, usize) {
        let buf = new_multi_batch_records(0, 2, 2, Compression::none().build());
        let length = RecordBatch::LENGTH_OFFSET;
        let first_batch_size =
            AbstractRecords::LOG_OVERHEAD + i32::from_be_bytes(buf[length..length + 4].try_into().unwrap()) as usize;
        (buf, first_batch_size)
    }

    fn put_i32(buf: &mut [u8], at: usize, value: i32) {
        buf[at..at + 4].copy_from_slice(&value.to_be_bytes());
    }

    /// Recomputes a batch's CRC over `ATTRIBUTES_OFFSET..` its end, so a
    /// corruption the CRC covers can be driven under `check.crcs=true`.
    fn recompute_crc(batch: &mut [u8]) {
        let crc = crc32c::crc32c(&batch[RecordBatch::ATTRIBUTES_OFFSET..]);
        batch[RecordBatch::CRC_OFFSET..RecordBatch::CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    }

    /// Drives `fetch_records` over a payload whose first batch is intact and
    /// returns the error its second batch raises.
    ///
    /// The first call returns batch 1's records: Java's `catch (KafkaException e)`
    /// caches a fault and returns the records already in hand
    /// (`CompletedFetch.java:299-305`). The second call raises the cached fault
    /// wrapped in the "seek past the record" message (`:256-259`).
    fn fault_after_first_batch(buf: Vec<u8>, check_crcs: bool) -> Error {
        let mut cf = new_completed_fetch(0, buf);
        let config = make_fetch_config(IsolationLevel::ReadUncommitted, check_crcs);
        let first = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect("the intact first batch's records are returned");
        assert_eq!(vec![0, 1], first.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
        let err = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect_err("the second batch's fault must surface");
        assert_eq!(SEEK_PAST_MESSAGE, err.message());
        assert!(err.is_kafka_error(), "{err:?}");
        assert!(
            !crate::common::requests::RequestUtils::is_fatal_error(&err),
            "a corrupt batch is recoverable: {err:?}"
        );
        err
    }

    /// The fault `fetch_records` wrapped.
    fn cause(err: &Error) -> &Error {
        err.source().expect("the fault is the wrapper's cause")
    }

    /// A negative length was read as `i32 as usize` and wrapped; the stream
    /// compares it signed (`ByteBufferLogInputStream.java:72-74`).
    #[test]
    fn test_negative_length_in_a_later_batch_is_corrupt() {
        let (mut buf, second) = two_batches();
        put_i32(&mut buf, second + RecordBatch::LENGTH_OFFSET, -5);
        let err = fault_after_first_batch(buf, false);
        let cause = cause(&err);
        // Java's `CorruptRecordException`, unwrapped by `maybeEnsureValid`: it
        // escapes `batches.hasNext()` before a batch exists.
        assert!(matches!(cause, Error::CorruptRecord(_)), "{cause:?}");
        assert_eq!(Errors::CorruptMessage, cause.error());
        assert_eq!("Record size -5 is less than the minimum record overhead (14)", cause.message());
    }

    /// `ByteBufferLogInputStreamTest.iteratorRaisesOnTooSmallRecords`, through the cursor.
    #[test]
    fn test_too_small_length_in_a_later_batch_is_corrupt() {
        let (mut buf, second) = two_batches();
        put_i32(&mut buf, second + RecordBatch::LENGTH_OFFSET, 9);
        let err = fault_after_first_batch(buf, false);
        let cause = cause(&err);
        assert_eq!(Errors::CorruptMessage, cause.error());
        assert_eq!("Record size 9 is less than the minimum record overhead (14)", cause.message());
    }

    /// `ByteBufferLogInputStreamTest.iteratorRaisesOnInvalidMagic`, through the cursor.
    #[test]
    fn test_invalid_magic_in_a_later_batch_is_corrupt() {
        let (mut buf, second) = two_batches();
        buf[second + RecordBatch::MAGIC_OFFSET] = 37;
        let err = fault_after_first_batch(buf, false);
        let cause = cause(&err);
        assert_eq!(Errors::CorruptMessage, cause.error());
        assert_eq!("Invalid magic found in record: 37", cause.message());
    }

    /// D2: a length that passes `next_batch_size` but makes a batch shorter than
    /// the 61-byte v2 header fails with Java's `ensureValid()` text under both
    /// `check.crcs` settings. Before the fix `check.crcs=false` read the header
    /// past the batch — into the next batch or off the end of the buffer.
    #[test]
    fn test_batch_below_the_header_size_is_invalid_regardless_of_check_crcs() {
        for check_crcs in [false, true] {
            let (mut buf, second) = two_batches();
            // 30 + AbstractRecords::LOG_OVERHEAD = 42 bytes.
            put_i32(&mut buf, second + RecordBatch::LENGTH_OFFSET, 30);
            let err = fault_after_first_batch(buf, check_crcs);
            let cause = cause(&err);
            assert_eq!(
                "Record batch for partition test-0 at offset 2 is invalid, cause: Record batch is corrupt \
                 (the size 42 is smaller than the minimum allowed overhead 61)",
                cause.message(),
                "check.crcs={check_crcs}"
            );
            // Java's `maybeEnsureValid` throws a bare `KafkaException`.
            assert!(matches!(cause, Error::KafkaError(_)), "{cause:?}");
        }
    }

    /// D2 for the only batch: nothing is decoded first, so the first call fails.
    #[test]
    fn test_first_batch_below_the_header_size_is_invalid() {
        let mut batch = vec![0u8; 42];
        batch[..8].copy_from_slice(&7_i64.to_be_bytes());
        put_i32(&mut batch, RecordBatch::LENGTH_OFFSET, 30);
        batch[RecordBatch::MAGIC_OFFSET] = RecordBatch::MAGIC_VALUE_V2 as u8;
        let mut cf = new_completed_fetch(7, batch);
        let config = make_fetch_config(IsolationLevel::ReadUncommitted, false);
        let err = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect_err("a 42-byte batch has no v2 header");
        assert_eq!(SEEK_PAST_MESSAGE, err.message());
        assert_eq!(
            "Record batch for partition test-0 at offset 7 is invalid, cause: Record batch is corrupt \
             (the size 42 is smaller than the minimum allowed overhead 61)",
            cause(&err).message()
        );
    }

    /// D7: a v0 or v1 batch is refused by its magic, under both `check.crcs`
    /// settings, instead of being read as a v2 header.
    #[test]
    fn test_legacy_magic_in_a_later_batch_is_refused() {
        for magic in [RecordBatch::MAGIC_VALUE_V0, RecordBatch::MAGIC_VALUE_V1] {
            for check_crcs in [false, true] {
                let (mut buf, second) = two_batches();
                buf[second + RecordBatch::MAGIC_OFFSET] = magic as u8;
                let err = fault_after_first_batch(buf, check_crcs);
                assert_eq!(
                    format!(
                        "Record batch for partition test-0 at offset 2 is invalid, cause: Record batch magic v{magic} \
                         is not supported: this client reads only magic v2 record batches (message formats v0 and v1 \
                         were removed in Kafka 4.0 by KIP-724)"
                    ),
                    cause(&err).message(),
                    "magic={magic} check.crcs={check_crcs}"
                );
            }
        }
    }

    /// D7 wins over D2: a legacy batch small enough to fail the v2 size check
    /// is reported by its magic, not as a corrupt v2 batch.
    #[test]
    fn test_short_legacy_batch_is_refused_by_its_magic() {
        let (buf, second) = two_batches();
        let mut buf = buf[..second].to_vec();
        // A 34-byte v1 message: AbstractRecords::LOG_OVERHEAD plus a 22-byte record.
        let mut legacy = vec![0u8; 34];
        legacy[..8].copy_from_slice(&2_i64.to_be_bytes());
        put_i32(&mut legacy, RecordBatch::LENGTH_OFFSET, 22);
        legacy[RecordBatch::MAGIC_OFFSET] = RecordBatch::MAGIC_VALUE_V1 as u8;
        buf.extend_from_slice(&legacy);
        let err = fault_after_first_batch(buf, false);
        assert_eq!(
            "Record batch for partition test-0 at offset 2 is invalid, cause: Record batch magic v1 is not \
             supported: this client reads only magic v2 record batches (message formats v0 and v1 were removed in \
             Kafka 4.0 by KIP-724)",
            cause(&err).message()
        );
    }

    /// D3: a negative record count is rejected before the batch is installed,
    /// with Java's `RecordIterator` text (`DefaultRecordBatch.java:584-587`),
    /// unwrapped. It used to be installed as an empty batch.
    #[test]
    fn test_negative_record_count_in_a_later_batch_is_invalid() {
        for check_crcs in [false, true] {
            let (mut buf, second) = two_batches();
            put_i32(&mut buf, second + RecordBatch::RECORDS_COUNT_OFFSET, -1);
            if check_crcs {
                recompute_crc(&mut buf[second..]);
            }
            let err = fault_after_first_batch(buf, check_crcs);
            let cause = cause(&err);
            assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
            assert_eq!("Found invalid record count -1 in magic v2 batch", cause.message());
        }
    }

    /// D3 in Java's order: an aborted batch is skipped under READ_COMMITTED
    /// before its iterator — and so its count check — is ever built
    /// (`CompletedFetch.java:212-221`), so a negative count there is not an error.
    #[test]
    fn test_negative_record_count_in_an_aborted_batch_is_skipped() {
        let aborted_pid = 42;
        let mut buf = batch_full(0, 2, RecordBatch::NO_PRODUCER_ID, false, false);
        let aborted_start = buf.len();
        buf.extend_from_slice(&batch_full(2, 2, aborted_pid, true, false));
        put_i32(&mut buf, aborted_start + RecordBatch::RECORDS_COUNT_OFFSET, -1);
        recompute_crc(&mut buf[aborted_start..]);
        buf.extend_from_slice(&batch_full(4, 2, RecordBatch::NO_PRODUCER_ID, false, false));

        let mut cf = CompletedFetch::with_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data_with_aborted_txn(buf, aborted_pid, 2),
            test_aggregator(),
            0,
        );
        let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
        let records = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect("the aborted batch is skipped, not parsed");
        assert_eq!(vec![0, 1, 4, 5], records.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
    }

    /// D3 on the control-batch path: Java's `containsAbortMarker` builds the
    /// batch's iterator (`CompletedFetch.java:376-386`), whose `RecordIterator`
    /// rejects a negative count (`DefaultRecordBatch.java:584-587`), so the
    /// count fails there under READ_COMMITTED.
    ///
    /// The control batch's producer id is in the aborted set, so that check is
    /// the only one that can raise the error. Were it missing,
    /// `contains_abort_marker` would answer `false` (the fixture's record has no
    /// key) and the next branch would skip the batch as aborted — where Java has
    /// already thrown — ending the fetch with no error. With no aborted
    /// transaction the batch would instead reach the install-point D3 check,
    /// whose identical message would hide a missing control-batch check.
    #[test]
    fn test_negative_record_count_in_a_control_batch_is_invalid() {
        const PRODUCER_ID: i64 = 1000;
        let mut buf = batch_full(0, 2, RecordBatch::NO_PRODUCER_ID, false, false);
        let control_start = buf.len();
        buf.extend_from_slice(&control_batch(2, 1, PRODUCER_ID));
        put_i32(&mut buf, control_start + RecordBatch::RECORDS_COUNT_OFFSET, -1);
        recompute_crc(&mut buf[control_start..]);

        let mut cf = CompletedFetch::with_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data_with_aborted_txn(buf, PRODUCER_ID, 2),
            test_aggregator(),
            0,
        );
        let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
        let first = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect("the first batch's records are returned");
        assert_eq!(vec![0, 1], first.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
        let err = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect_err("the control batch's count is invalid");
        assert_eq!(SEEK_PAST_MESSAGE, err.message());
        let cause = cause(&err);
        assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
        assert_eq!("Found invalid record count -1 in magic v2 batch", cause.message());
    }

    /// Marks a batch's header gzip-compressed and overwrites its records with
    /// bytes no gzip stream starts with, so inflating it must fail. The codec
    /// id sits in the low byte of the two-byte attributes field.
    fn garble_as_compressed(batch: &mut [u8]) {
        batch[RecordBatch::ATTRIBUTES_OFFSET + 1] |= CompressionType::Gzip.id();
        batch[RecordBatch::RECORD_BATCH_OVERHEAD..].fill(0xFF);
    }

    /// The count check comes before the records are inflated: a compressed
    /// batch whose count is negative *and* whose stream is garbage fails on the
    /// count, as in Java, where `RecordIterator`'s constructor runs before
    /// `StreamRecordIterator` reads anything through the decompression stream
    /// (`DefaultRecordBatch.java:279-297, 579-588`). The control case first
    /// shows the garbage alone is refused by the inflation, so the order is
    /// what the second case proves.
    #[test]
    fn test_negative_record_count_in_a_compressed_batch_is_refused_before_inflating() {
        for check_crcs in [false, true] {
            let (mut buf, second) = two_batches();
            garble_as_compressed(&mut buf[second..]);
            if check_crcs {
                recompute_crc(&mut buf[second..]);
            }
            let err = fault_after_first_batch(buf.clone(), check_crcs);
            assert!(
                cause(&err).message().contains("Failed to decompress record stream"),
                "{:?}",
                cause(&err)
            );

            put_i32(&mut buf, second + RecordBatch::RECORDS_COUNT_OFFSET, -1);
            if check_crcs {
                recompute_crc(&mut buf[second..]);
            }
            let err = fault_after_first_batch(buf, check_crcs);
            let cause = cause(&err);
            assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
            assert_eq!("Found invalid record count -1 in magic v2 batch", cause.message());
        }
    }

    /// The READ_COMMITTED skip comes before the records are inflated too: an
    /// aborted compressed batch is dropped without its stream being read, as
    /// Java never builds an iterator for a batch it skips
    /// (`CompletedFetch.java:217-226`).
    #[test]
    fn test_aborted_compressed_batch_is_skipped_without_inflating() {
        let aborted_pid = 42;
        let mut buf = batch_full(0, 2, RecordBatch::NO_PRODUCER_ID, false, false);
        let aborted_start = buf.len();
        buf.extend_from_slice(&batch_full(2, 2, aborted_pid, true, false));
        garble_as_compressed(&mut buf[aborted_start..]);
        recompute_crc(&mut buf[aborted_start..]);
        buf.extend_from_slice(&batch_full(4, 2, RecordBatch::NO_PRODUCER_ID, false, false));

        let mut cf = CompletedFetch::with_full(
            make_subscriptions(),
            Arc::new(BufferSupplier::create()),
            tp("test", 0),
            partition_data_with_aborted_txn(buf, aborted_pid, 2),
            test_aggregator(),
            0,
        );
        let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
        let records = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect("the aborted batch is skipped, not inflated");
        assert_eq!(vec![0, 1, 4, 5], records.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
    }

    /// The cursor lets go of an exhausted batch's record stream before it takes
    /// the next batch, as Java closes a batch's record stream before taking the
    /// next (`maybeCloseRecordStream()`, `CompletedFetch.java:180-185, 190`), so
    /// a fetch never holds two batches' decompressors and chunks at once. The
    /// test keeps its own reference to the first batch's chunk and makes the
    /// second batch fail: once that failure is reported, the test's reference
    /// must be the only one left. The second batch fails either its checksum,
    /// before any stream is opened for it, or its first read. Releasing the
    /// first stream only when the second is installed would fail the checksum
    /// case, since a batch that fails its checks installs nothing.
    #[test]
    fn test_exhausted_inflated_batch_is_released_before_the_next_is_inflated() {
        // (check.crcs, whether the second batch is re-signed after it is garbled,
        // the start of the failure's message).
        let cases = [
            (false, false, "Failed to decompress record stream: invalid gzip header"),
            (true, true, "Failed to decompress record stream: invalid gzip header"),
            (true, false, "Record is corrupt (stored crc = "),
        ];
        for (check_crcs, resign, failure) in cases {
            let mut buf = new_multi_batch_records(0, 2, 2, Compression::gzip().build());
            let length = RecordBatch::LENGTH_OFFSET;
            let second = AbstractRecords::LOG_OVERHEAD
                + i32::from_be_bytes(buf[length..length + 4].try_into().unwrap()) as usize;
            garble_as_compressed(&mut buf[second..]);
            if resign {
                recompute_crc(&mut buf[second..]);
            }
            let mut cf = new_completed_fetch(0, buf);
            let config = make_fetch_config(IsolationLevel::ReadUncommitted, check_crcs);

            // Exactly the first batch's two records, so the cursor stops at the
            // end of that batch without reading the second.
            let first = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 2)
                .expect("the first batch's records are returned");
            assert_eq!(vec![0, 1], first.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
            let first_batch = cf
                .current_record_source_bytes()
                .expect("the exhausted first batch is still the cursor's current one");
            assert!(!first_batch.is_unique(), "the cursor shares the first batch's chunk");

            let err = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
                .expect_err("the second batch fails");
            assert_eq!(SEEK_PAST_MESSAGE, err.message());
            let prefix = "Record batch for partition test-0 at offset 2 is invalid, cause: ";
            assert!(
                cause(&err).message().starts_with(&format!("{prefix}{failure}")),
                "{:?}",
                cause(&err)
            );
            assert!(
                first_batch.is_unique(),
                "the cursor still held the first batch's stream when the second failed (check.crcs={check_crcs})"
            );
        }
    }

    /// The four codecs a compressed batch can use.
    fn codecs() -> [Compression; 4] {
        [
            Compression::gzip().build(),
            Compression::snappy().build(),
            Compression::lz4().build(),
            Compression::zstd().build(),
        ]
    }

    /// Rebuilds the uncompressed `batch` around `records`, a records section of
    /// the test's choosing given in pieces, compressed with `compression`. Each
    /// piece is written to the codec with its own write, so a piece per record
    /// compresses the way a producer appending records one at a time does. The
    /// header is kept, apart from the codec in its attributes, its length and
    /// its CRC.
    fn compressed_batch_with_records(batch: &[u8], records: &[&[u8]], compression: &Compression) -> Vec<u8> {
        use std::io::Write;
        let mut out = batch[..RecordBatch::RECORD_BATCH_OVERHEAD].to_vec();
        let mut writer = compression
            .wrap_for_output(&mut out, RecordBatch::MAGIC_VALUE_V2)
            .expect("the codec opens");
        for piece in records {
            writer.write_all(piece).expect("the records compress");
        }
        writer.finish().expect("the stream closes");
        out[RecordBatch::ATTRIBUTES_OFFSET + 1] |= compression.compression_type().id();
        let length = (out.len() - AbstractRecords::LOG_OVERHEAD) as i32;
        put_i32(&mut out, RecordBatch::LENGTH_OFFSET, length);
        recompute_crc(&mut out);
        out
    }

    /// A compressed batch holding `count` records but declaring
    /// `declared_count`, with `cut` bytes dropped from the end of its records
    /// before they are compressed. Each record has a null key and the value
    /// `value-{offset}`, so its body is 13 bytes.
    fn compressed_batch(count: i32, declared_count: i32, cut: usize, compression: &Compression) -> Vec<u8> {
        let batch = batch_with_overridden_record_count(0, count, declared_count);
        let records = &batch[RecordBatch::RECORD_BATCH_OVERHEAD..batch.len() - cut];
        compressed_batch_with_records(&batch, &[records], compression)
    }

    /// The uncompressed `batch` compressed with `compression` a record at a
    /// time, as a producer compresses it.
    fn compressed_by_record(batch: &[u8], compression: &Compression) -> Vec<u8> {
        let mut records = &batch[RecordBatch::RECORD_BATCH_OVERHEAD..];
        let mut pieces = Vec::new();
        while !records.is_empty() {
            let (size, size_length) = ByteUtils::read_varint(records).expect("a record starts with its size");
            let (record, rest) = records.split_at(size_length + size as usize);
            pieces.push(record);
            records = rest;
        }
        compressed_batch_with_records(batch, &pieces, compression)
    }

    /// Fetches a single-batch payload twice under both `check.crcs` settings:
    /// the first call returns the records before the fault, as Java's
    /// `catch (KafkaException e)` does (`CompletedFetch.java:299-305`), and the
    /// second raises the fault wrapped in the "seek past the record" message.
    /// Returns the offsets the first call returned and the fault.
    fn records_then_fault(buf: &[u8], check_crcs: bool) -> (Vec<i64>, Error) {
        let mut cf = new_completed_fetch(0, buf.to_vec());
        let config = make_fetch_config(IsolationLevel::ReadUncommitted, check_crcs);
        let first = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect("the records before the fault are returned");
        let err = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect_err("the fault surfaces on the next call");
        assert_eq!(SEEK_PAST_MESSAGE, err.message());
        assert!(err.is_kafka_error(), "{err:?}");
        let fault = cause(&err).clone();
        (first.iter().map(ConsumerRecord::offset).collect(), fault)
    }

    /// A compressed batch whose records end before its declared count fails
    /// with the premature-EOF text the uncompressed path uses, after its
    /// records are returned.
    #[test]
    fn test_compressed_batch_ending_before_its_declared_count_is_premature_eof() {
        for compression in codecs() {
            for check_crcs in [false, true] {
                let buf = compressed_batch(2, 3, 0, &compression);
                let (offsets, fault) = records_then_fault(&buf, check_crcs);
                assert_eq!(vec![0, 1], offsets, "{compression:?}");
                assert!(matches!(fault, Error::InvalidRecord(_)), "{fault:?}");
                assert_eq!(
                    "Incorrect declared batch size for partition test-0, premature EOF reached \
                     (declared record count exceeds the records present in the batch)",
                    fault.message(),
                    "{compression:?} check.crcs={check_crcs}"
                );
            }
        }
    }

    /// A compressed batch whose records end inside a record fails at that
    /// record, with the text of Java's stream reader for a record the stream
    /// ends inside of (`DefaultRecord.java:304-306`).
    #[test]
    fn test_compressed_batch_ending_inside_a_record_is_invalid() {
        for compression in codecs() {
            for check_crcs in [false, true] {
                let buf = compressed_batch(2, 2, 3, &compression);
                let (offsets, fault) = records_then_fault(&buf, check_crcs);
                assert_eq!(vec![0], offsets, "{compression:?}");
                assert!(matches!(fault, Error::InvalidRecord(_)), "{fault:?}");
                assert_eq!(
                    "Record batch for partition test-0 at offset 0 is invalid, cause: Invalid record size: \
                     expected 13 bytes in record payload, but the record payload reached EOF.",
                    fault.message(),
                    "{compression:?} check.crcs={check_crcs}"
                );
            }
        }
    }

    /// A batch whose records end inside a record's size fails with the
    /// premature-EOF text, compressed or not, after the records before it are
    /// returned: Java's buffer and stream readers both report it this way
    /// (`DefaultRecordBatch.java:308, 650`).
    #[test]
    fn test_batch_ending_inside_a_record_size_is_premature_eof() {
        // One whole record, then the first byte of a second record's size.
        let batch = batch_with_overridden_record_count(0, 1, 2);
        let records = [&batch[RecordBatch::RECORD_BATCH_OVERHEAD..], &[0x80][..]];
        let mut uncompressed = [&batch[..], &[0x80][..]].concat();
        let length = (uncompressed.len() - AbstractRecords::LOG_OVERHEAD) as i32;
        put_i32(&mut uncompressed, RecordBatch::LENGTH_OFFSET, length);
        recompute_crc(&mut uncompressed);
        let mut cases = vec![("none".to_string(), uncompressed)];
        for compression in codecs() {
            cases.push((
                format!("{compression:?}"),
                compressed_batch_with_records(&batch, &records, &compression),
            ));
        }
        for (compression, buf) in &cases {
            for check_crcs in [false, true] {
                let (offsets, fault) = records_then_fault(buf, check_crcs);
                assert_eq!(vec![0], offsets, "{compression}");
                assert!(matches!(fault, Error::InvalidRecord(_)), "{fault:?}");
                assert_eq!(
                    "Incorrect declared batch size for partition test-0, premature EOF reached \
                     (declared record count exceeds the records present in the batch)",
                    fault.message(),
                    "{compression} check.crcs={check_crcs}"
                );
            }
        }
    }

    /// A compressed batch holding more records than it declares fails with the
    /// records-remaining text the uncompressed path uses, after the declared
    /// records are returned.
    #[test]
    fn test_compressed_batch_holding_more_records_than_declared_is_invalid() {
        for compression in codecs() {
            for check_crcs in [false, true] {
                let buf = compressed_batch(3, 2, 0, &compression);
                let (offsets, fault) = records_then_fault(&buf, check_crcs);
                assert_eq!(vec![0, 1], offsets, "{compression:?}");
                assert!(matches!(fault, Error::InvalidRecord(_)), "{fault:?}");
                assert_eq!(
                    "Incorrect declared batch size for partition test-0, records still remaining in batch \
                     (declared record count is fewer than the records present)",
                    fault.message(),
                    "{compression:?} check.crcs={check_crcs}"
                );
            }
        }
    }

    /// A compressed batch whose stream cannot be decompressed fails with the
    /// codec's own text, before any record is returned.
    #[test]
    fn test_compressed_batch_that_fails_to_decompress_is_invalid() {
        for check_crcs in [false, true] {
            let mut buf = batch_full(0, 2, RecordBatch::NO_PRODUCER_ID, false, false);
            garble_as_compressed(&mut buf);
            recompute_crc(&mut buf);
            let mut cf = new_completed_fetch(0, buf);
            let config = make_fetch_config(IsolationLevel::ReadUncommitted, check_crcs);
            let err = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
                .expect_err("the stream is not gzip");
            assert_eq!(SEEK_PAST_MESSAGE, err.message());
            let cause = cause(&err);
            assert!(matches!(cause, Error::KafkaError(_)), "{cause:?}");
            assert_eq!(
                "Record batch for partition test-0 at offset 0 is invalid, cause: Failed to decompress record \
                 stream: invalid gzip header",
                cause.message(),
                "check.crcs={check_crcs}"
            );
        }
    }

    /// An uncompressed batch of `count` records from offset 0, with null keys
    /// and `value_size`-byte text values (`value-{offset} ` repeated).
    fn batch_of_values(count: i32, value_size: usize) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
            count as usize * (value_size + 16),
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
        );
        for offset in 0..i64::from(count) {
            let text = format!("value-{offset} ");
            let value: Vec<u8> = text.bytes().cycle().take(value_size).collect();
            builder.append_with_offset_bytes(offset, 0, None, Some(&value));
        }
        builder.build().buffer().to_vec()
    }

    /// Fetches until a call returns nothing, `max_records` at a time, and
    /// returns the offset and value length of every record, in order.
    fn fetch_all_lengths(cf: &mut CompletedFetch, max_records: i32) -> Vec<(i64, usize)> {
        let config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let mut out = Vec::new();
        loop {
            let records = cf
                .fetch_records::<usize, usize>(&config, &LenDeserializer, &LenDeserializer, max_records)
                .expect("the records are valid");
            if records.is_empty() {
                return out;
            }
            out.extend(records.iter().map(|r| (r.offset(), *r.value().expect("a value"))));
        }
    }

    /// A compressed batch is read across polls: a poll asking for fewer records
    /// than the batch holds leaves the stream where it stopped, and the next
    /// poll carries on from there, across chunks and into the next batch.
    #[test]
    fn test_compressed_batches_are_read_across_polls() {
        for compression in codecs() {
            let buf = new_multi_batch_records(0, 3, 5, compression.clone());
            let mut cf = new_completed_fetch(0, buf);
            let config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
            let mut read = Vec::new();
            loop {
                let records = cf
                    .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 4)
                    .expect("the batches are valid");
                if records.is_empty() {
                    break;
                }
                assert!(records.len() <= 4, "{compression:?}");
                read.extend(records);
            }
            assert_eq!(
                (0..15).collect::<Vec<i64>>(),
                read.iter().map(ConsumerRecord::offset).collect::<Vec<_>>()
            );
            for record in &read {
                let n = record.offset();
                assert_eq!(Some(&format!("key-{n}")), record.key(), "{compression:?}");
                assert_eq!(Some(&format!("value-{n}")), record.value(), "{compression:?}");
            }

            // Values bigger than a chunk, so records also span chunk boundaries.
            let buf = compressed_by_record(&batch_of_values(300, 1000), &compression);
            let mut cf = new_completed_fetch(0, buf);
            let read = fetch_all_lengths(&mut cf, 7);
            assert_eq!((0..300).map(|n| (n, 1000)).collect::<Vec<_>>(), read, "{compression:?}");
        }
    }

    /// A value deserializer that fails on the `value-{offset}` record at
    /// `offset` while `failing` is set, and decodes every other record.
    struct FailWhileSet {
        offset: i64,
        failing: std::sync::atomic::AtomicBool,
    }
    impl Deserializer<String> for FailWhileSet {
        fn deserialize(&self, topic: &str, data: &[u8]) -> Result<String, Error> {
            if self.failing.load(std::sync::atomic::Ordering::SeqCst)
                && MaybeFailingDeserializer::parse_offset(data, "value-") == Some(self.offset)
            {
                return Err(Error::serialization("simulated failure"));
            }
            StringDeserializer.deserialize(topic, data)
        }
    }

    /// A record of a compressed batch whose deserializer fails stays where it
    /// is across polls: the next poll reads it again from the stream's chunk,
    /// and once the deserializer succeeds the poll carries on from there. Java
    /// re-deserializes the last record "so the user can retry after fixing
    /// whatever state they like" (`CompletedFetch.java:275-278`).
    #[test]
    fn test_failed_record_of_a_compressed_batch_is_read_again_on_the_next_poll() {
        fn offsets(records: &[ConsumerRecord<String, String>]) -> Vec<i64> {
            records.iter().map(ConsumerRecord::offset).collect()
        }
        for compression in codecs() {
            let mut cf = new_completed_fetch(0, new_multi_batch_records(0, 1, 5, compression.clone()));
            let config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
            let value_de = FailWhileSet { offset: 2, failing: std::sync::atomic::AtomicBool::new(true) };

            let first = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &value_de, 10)
                .expect("the records before the failing one are returned");
            assert_eq!(vec![0, 1], offsets(&first), "{compression:?}");
            let err = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &value_de, 10)
                .expect_err("the failing record fails again");
            assert_eq!(
                "Error deserializing VALUE for partition test-0 at offset 2. \
                 If needed, please seek past the record to continue consumption.",
                err.message(),
                "{compression:?}"
            );
            let Error::RecordDeserialization(rde) = &err else {
                panic!("expected Error::RecordDeserialization, got: {err:?}");
            };
            assert_eq!(2, rde.offset(), "{compression:?}");
            assert_eq!(Some(&b"value-2"[..]), rde.value_buffer(), "{compression:?}");

            value_de.failing.store(false, std::sync::atomic::Ordering::SeqCst);
            let rest = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &value_de, 10)
                .expect("the record is read again and decodes");
            assert_eq!(vec![2, 3, 4], offsets(&rest), "{compression:?}");
            assert_eq!(Some(&"value-2".to_string()), rest[0].value(), "{compression:?}");
        }
    }

    /// A record bigger than a chunk is read whole: its chunk grows to the
    /// record as its bytes are decompressed.
    #[test]
    fn test_compressed_record_bigger_than_a_chunk_is_read_whole() {
        const BIG: usize = 3 * RECORD_CHUNK_SIZE + 17;
        for compression in codecs() {
            let mut builder = MemoryRecords::builder_with_initial_capacity_magic(
                BIG + 512,
                RecordBatch::MAGIC_VALUE_V2,
                Compression::none().build(),
                TimestampType::CreateTime,
                0,
            );
            for (offset, size) in [(0, 10), (1, BIG), (2, 10)] {
                let value: Vec<u8> = b"value ".iter().copied().cycle().take(size).collect();
                builder.append_with_offset_bytes(offset, 0, None, Some(&value));
            }
            let buf = compressed_by_record(builder.build().buffer(), &compression);
            let mut cf = new_completed_fetch(0, buf);
            assert_eq!(
                vec![(0, 10), (1, BIG), (2, 10)],
                fetch_all_lengths(&mut cf, 10),
                "{compression:?}"
            );
        }
    }

    /// A compressed batch is decompressed a chunk at a time, never whole: reading
    /// one that decompresses to more than a mebibyte allocates no single block
    /// bigger than 256 KiB, under every codec. Its records are compressed one
    /// write each, as a producer appends them, so lz4 frames them in the 64 KiB
    /// blocks Java's producer writes. (zstd's decoder state is allocated by its C
    /// library, which the tracker does not see; its window is bounded by the
    /// frame.)
    #[test]
    fn test_compressed_batch_is_decompressed_a_chunk_at_a_time() {
        const RECORD_COUNT: i32 = 1100;
        const VALUE_SIZE: usize = 1000;
        const MAX_ALLOCATION: usize = 256 * 1024;
        let batch = batch_of_values(RECORD_COUNT, VALUE_SIZE);
        assert!(batch.len() > 1024 * 1024, "the batch decompresses to more than a mebibyte");
        for compression in codecs() {
            let mut cf = new_completed_fetch(0, compressed_by_record(&batch, &compression));
            let (read, largest) = {
                let _guard = crate::AllocTrackingGuard::new();
                let read = fetch_all_lengths(&mut cf, 100);
                (read, crate::AllocTrackingGuard::max_allocation())
            };
            assert_eq!(
                (0..i64::from(RECORD_COUNT)).map(|n| (n, VALUE_SIZE)).collect::<Vec<_>>(),
                read,
                "{compression:?}"
            );
            assert!(
                largest <= MAX_ALLOCATION,
                "{compression:?}: the largest allocation was {largest} bytes"
            );
            eprintln!("{compression:?} largest allocation: {largest} bytes (max allowed {MAX_ALLOCATION})");
        }
    }

    /// A gzip stream whose trailing checksum is wrong fails once its records
    /// have been read: the codec checks the checksum when the cursor reads past
    /// the last record, as Java's `ensureNoneRemaining` reads past it
    /// (`DefaultRecordBatch.java:657-663`).
    #[test]
    fn test_compressed_batch_with_a_wrong_gzip_checksum_fails_after_its_records() {
        for check_crcs in [false, true] {
            let mut buf = compressed_batch(2, 2, 0, &Compression::gzip().build());
            // A gzip stream ends with its CRC-32 and then its length, four
            // little-endian bytes each.
            let checksum = buf.len() - 8;
            buf[checksum] ^= 0x01;
            recompute_crc(&mut buf);
            let (offsets, fault) = records_then_fault(&buf, check_crcs);
            assert_eq!(vec![0, 1], offsets);
            assert!(matches!(fault, Error::KafkaError(_)), "{fault:?}");
            assert_eq!(
                "Record batch for partition test-0 at offset 0 is invalid, cause: Error checking for remaining \
                 bytes after reading batch: corrupt gzip stream does not have a matching checksum",
                fault.message(),
                "check.crcs={check_crcs}"
            );
        }
    }

    /// A transactional control batch of `count` records whose first is a marker
    /// of `control_type`, as a broker writes one: the type's record key and an
    /// `EndTransactionMarker` v0 value (version 0, coordinator epoch 0). The
    /// others are `value-{offset}` records. The control flag is set after the
    /// batch is built, as [`control_batch`] sets it.
    fn marker_batch(base_offset: i64, count: i32, producer_id: i64, control_type: ControlRecordType) -> Vec<u8> {
        const CONTROL_FLAG_MASK: u8 = 0x20;
        let mut builder = MemoryRecords::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(512)
                .set_magic(RecordBatch::MAGIC_VALUE_V2)
                .set_compression(Compression::none().build())
                .set_timestamp_type(TimestampType::CreateTime)
                .set_base_offset(base_offset)
                .set_log_append_time(-1)
                .set_producer_id(producer_id)
                .set_producer_epoch(0)
                .set_base_sequence(0)
                .set_is_transactional(true)
                .set_is_control_batch(false)
                .set_partition_leader_epoch(-1)
                .set_write_limit(512)
                .build()
                .expect("MemoryRecordsBuilderOptionsBuilder::build: every mandatory parameter is set above"),
        );
        let key = control_type.record_key().expect("the type has a record key");
        builder.append_with_offset_bytes(base_offset, 0, Some(&key), Some(&[0; 6]));
        for i in 1..count {
            let offset = base_offset + i64::from(i);
            let value = format!("value-{offset}");
            builder.append_with_offset_bytes(offset, 0, None, Some(value.as_bytes()));
        }
        let mut buf = builder.build().buffer().to_vec();
        buf[RecordBatch::ATTRIBUTES_OFFSET + 1] |= CONTROL_FLAG_MASK;
        recompute_crc(&mut buf);
        buf
    }

    /// A compressed control batch's marker is read off its stream: an ABORT
    /// marker ends the producer's aborted transaction, so its next transaction
    /// is returned, while a COMMIT marker leaves the transaction aborted. Java
    /// reads the marker through `batch.iterator()` (`CompletedFetch.java:381-391`).
    #[test]
    fn test_marker_in_a_compressed_control_batch_is_read() {
        const PRODUCER_ID: i64 = 42;
        for compression in codecs() {
            for (control_type, expected) in [
                (ControlRecordType::Abort, vec![3, 4]),
                (ControlRecordType::Commit, vec![]),
            ] {
                let mut buf = batch_full(0, 2, PRODUCER_ID, true, false);
                buf.extend_from_slice(&compressed_by_record(
                    &marker_batch(2, 1, PRODUCER_ID, control_type),
                    &compression,
                ));
                buf.extend_from_slice(&batch_full(3, 2, PRODUCER_ID, true, false));
                let mut cf = CompletedFetch::with_full(
                    make_subscriptions(),
                    Arc::new(BufferSupplier::create()),
                    tp("test", 0),
                    partition_data_with_aborted_txn(buf, PRODUCER_ID, 0),
                    test_aggregator(),
                    0,
                );
                let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
                let records = cf
                    .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
                    .expect("the batches are valid");
                assert_eq!(
                    expected,
                    records.iter().map(ConsumerRecord::offset).collect::<Vec<_>>(),
                    "{compression:?} {control_type:?}"
                );
            }
        }
    }

    /// A compressed control batch is read to its end before its marker counts,
    /// as Java's `batch.iterator()` reads every record of a compressed batch
    /// (`DefaultRecordBatch.java:331-335`): a record after the marker that ends
    /// early fails the fetch before any of the batch's records is consumed.
    #[test]
    fn test_compressed_control_batch_with_a_short_later_record_is_invalid() {
        const PRODUCER_ID: i64 = 42;
        for compression in codecs() {
            let marker = marker_batch(0, 2, PRODUCER_ID, ControlRecordType::Abort);
            let records = &marker[RecordBatch::RECORD_BATCH_OVERHEAD..marker.len() - 3];
            let buf = compressed_batch_with_records(&marker, &[records], &compression);
            let mut cf = CompletedFetch::with_full(
                make_subscriptions(),
                Arc::new(BufferSupplier::create()),
                tp("test", 0),
                partition_data_with_aborted_txn(buf, PRODUCER_ID, 0),
                test_aggregator(),
                0,
            );
            let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
            let err = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
                .expect_err("the second control record is short");
            assert_eq!(SEEK_PAST_MESSAGE, err.message());
            let cause = cause(&err);
            assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
            assert_eq!(
                "Control batch for partition test-0 at offset 0 is invalid, cause: Invalid record size: expected \
                 13 bytes in record payload, but the record payload reached EOF.",
                cause.message(),
                "{compression:?}"
            );
            assert_eq!(0, cf.next_fetch_offset, "{compression:?}");
        }
    }

    /// A compressed control batch whose stream ends inside a later record's
    /// size fails with the premature-EOF text, as Java's stream reader reports
    /// it (`DefaultRecordBatch.java:650`), before any of the batch's records is
    /// consumed.
    #[test]
    fn test_compressed_control_batch_ending_inside_a_record_size_is_premature_eof() {
        const PRODUCER_ID: i64 = 42;
        for compression in codecs() {
            // The marker, then the first byte of a second record's size.
            let mut marker = marker_batch(0, 1, PRODUCER_ID, ControlRecordType::Abort);
            put_i32(&mut marker, RecordBatch::RECORDS_COUNT_OFFSET, 2);
            let records = [&marker[RecordBatch::RECORD_BATCH_OVERHEAD..], &[0x80][..]];
            let buf = compressed_batch_with_records(&marker, &records, &compression);
            let mut cf = CompletedFetch::with_full(
                make_subscriptions(),
                Arc::new(BufferSupplier::create()),
                tp("test", 0),
                partition_data_with_aborted_txn(buf, PRODUCER_ID, 0),
                test_aggregator(),
                0,
            );
            let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
            let err = cf
                .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
                .expect_err("the second control record's size is cut off");
            assert_eq!(SEEK_PAST_MESSAGE, err.message());
            let cause = cause(&err);
            assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
            assert_eq!(
                "Incorrect declared batch size for partition test-0, premature EOF reached \
                 (declared record count exceeds the records present in the batch)",
                cause.message(),
                "{compression:?}"
            );
            assert_eq!(0, cf.next_fetch_offset, "{compression:?}");
        }
    }

    /// §27 per-record allocation budget for a compressed batch: records are
    /// read out of the stream's chunks in place, so a compressed batch costs
    /// nothing per record beyond what an uncompressed one does — only the
    /// decompressor, its block buffers and one buffer per chunk. Mirrors
    /// [`test_per_record_loop_is_pure_counter_no_sensor_record`], with a budget
    /// for the whole batch rather than per record: the batch decompresses to
    /// about four chunks, and measured 13 to 26 allocations under the four
    /// codecs, so a single allocation per record (2000) cannot hide in it.
    #[test]
    fn test_compressed_per_record_allocation_budget() {
        const RECORD_COUNT: i32 = 2000;
        const ALLOC_BUDGET: usize = 100;
        let batch = batch_of_values(RECORD_COUNT, 100);
        for compression in codecs() {
            let mut cf = new_completed_fetch(0, compressed_by_record(&batch, &compression));
            let fetch_config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
            cf.ensure_cursor();
            let (alloc_count, n_records) = {
                let _guard = crate::AllocTrackingGuard::new();
                crate::AllocTrackingGuard::reset();
                let recs = cf
                    .fetch_records::<usize, usize>(&fetch_config, &LenDeserializer, &LenDeserializer, RECORD_COUNT)
                    .unwrap();
                (crate::AllocTrackingGuard::count(), recs.len())
            };
            assert_eq!(RECORD_COUNT as usize, n_records, "{compression:?}");
            assert!(
                alloc_count <= ALLOC_BUDGET,
                "{compression:?}: {alloc_count} allocs for {RECORD_COUNT} compressed records (budget \
                 {ALLOC_BUDGET}). A per-record allocation entered the compressed read path (§27)."
            );
            eprintln!(
                "{compression:?} compressed batch: {alloc_count} allocs for {RECORD_COUNT} records \
                 (max allowed {ALLOC_BUDGET})"
            );
        }
    }

    /// Declares a batch's first record 63 bytes long where 13 remain, by
    /// rewriting its one-byte size varint (zigzag 26 → 126), and re-signs the
    /// batch so the fault is the record, not the checksum. The batch must hold
    /// one record whose value is 7 bytes and whose key is null, as `batch_full`
    /// and `control_batch` build it.
    fn overstate_first_record_size(batch: &mut [u8]) {
        assert_eq!(26, batch[RecordBatch::RECORD_BATCH_OVERHEAD], "a 13-byte record body");
        batch[RecordBatch::RECORD_BATCH_OVERHEAD] = 126;
        recompute_crc(batch);
    }

    /// Java's text for the overstated record (`DefaultRecord.java:312-315`).
    const OVERSTATED_RECORD_CAUSE: &str =
        "Invalid record size: expected 63 bytes in record payload, but instead the buffer has only 13 remaining bytes.";

    /// A malformed record is reported with the cause's message after `cause: `,
    /// as Java's `getMessage()` gives it, not with its `Display`, which prefixes
    /// the class name (`InvalidRecordError: `).
    #[test]
    fn test_malformed_record_is_reported_by_the_cause_message() {
        let mut buf = batch_full(0, 1, RecordBatch::NO_PRODUCER_ID, false, false);
        overstate_first_record_size(&mut buf);
        let mut cf = new_completed_fetch(0, buf);
        let config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let err = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect_err("the record declares more bytes than its batch holds");
        assert_eq!(SEEK_PAST_MESSAGE, err.message());
        let cause = cause(&err);
        assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
        assert_eq!(
            format!("Record batch for partition test-0 at offset 0 is invalid, cause: {OVERSTATED_RECORD_CAUSE}"),
            cause.message()
        );
    }

    /// The same for a control batch's first record, read under READ_COMMITTED by
    /// `contains_abort_marker` (Java's `batchIterator.next()`,
    /// `CompletedFetch.java:389`).
    #[test]
    fn test_malformed_control_record_is_reported_by_the_cause_message() {
        let mut buf = control_batch(2, 1, 1000);
        overstate_first_record_size(&mut buf);
        let mut cf = new_completed_fetch(2, buf);
        let config = make_fetch_config(IsolationLevel::ReadCommitted, true);
        let err = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect_err("the control record declares more bytes than its batch holds");
        assert_eq!(SEEK_PAST_MESSAGE, err.message());
        let cause = cause(&err);
        assert!(matches!(cause, Error::InvalidRecord(_)), "{cause:?}");
        assert_eq!(
            format!("Control batch for partition test-0 at offset 2 is invalid, cause: {OVERSTATED_RECORD_CAUSE}"),
            cause.message()
        );
    }

    /// A header declaring more bytes than remain is no batch — Java's
    /// `remaining < batchSize` (`ByteBufferLogInputStream.java:44-46`) — so
    /// iteration ends without an error, at the end of the last complete batch.
    #[test]
    fn test_batch_declaring_more_bytes_than_remain_ends_iteration() {
        let (mut buf, second) = two_batches();
        let declared = (buf.len() - second - AbstractRecords::LOG_OVERHEAD) as i32 + 100;
        put_i32(&mut buf, second + RecordBatch::LENGTH_OFFSET, declared);
        let mut cf = new_completed_fetch(0, buf);
        let config = make_fetch_config(IsolationLevel::ReadUncommitted, true);
        let first = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .expect("a truncated batch is no batch, not an error");
        assert_eq!(vec![0, 1], first.iter().map(ConsumerRecord::offset).collect::<Vec<_>>());
        // Exhausted, not drained: `FetchCollector` drains after the position
        // update (KAFKA-15529).
        assert!(cf.is_exhausted());
        assert!(!cf.is_consumed());
        assert_eq!(2, cf.next_fetch_offset(), "the next fetch starts at the truncated batch");
        let rest = cf
            .fetch_records::<String, String>(&config, &StringDeserializer, &StringDeserializer, 10)
            .unwrap();
        assert!(rest.is_empty());
    }

    /// A trailing fragment of every size from `LOG_OVERHEAD` to one byte short of
    /// a v2 header, whose length field declares exactly the fragment, fails with
    /// the stream's or the D2 error — never a panic — under `check.crcs=false`,
    /// the setting that used to let the header reads run off the end.
    #[test]
    fn test_trailing_fragment_below_the_header_size_errors_without_panicking() {
        for fragment_len in AbstractRecords::LOG_OVERHEAD..RecordBatch::RECORD_BATCH_OVERHEAD {
            let (buf, second) = two_batches();
            let mut buf = buf[..second].to_vec();
            let mut fragment = vec![0u8; fragment_len];
            fragment[..8].copy_from_slice(&2_i64.to_be_bytes());
            let length = (fragment_len - AbstractRecords::LOG_OVERHEAD) as i32;
            put_i32(&mut fragment, RecordBatch::LENGTH_OFFSET, length);
            if fragment_len > RecordBatch::MAGIC_OFFSET {
                fragment[RecordBatch::MAGIC_OFFSET] = RecordBatch::MAGIC_VALUE_V2 as u8;
            }
            buf.extend_from_slice(&fragment);

            let err = fault_after_first_batch(buf, false);
            let expected = if length < 14 {
                format!("Record size {length} is less than the minimum record overhead (14)")
            } else {
                format!(
                    "Record batch for partition test-0 at offset 2 is invalid, cause: Record batch is corrupt \
                     (the size {fragment_len} is smaller than the minimum allowed overhead 61)"
                )
            };
            assert_eq!(expected, cause(&err).message(), "fragment of {fragment_len} bytes");
        }
    }
}
