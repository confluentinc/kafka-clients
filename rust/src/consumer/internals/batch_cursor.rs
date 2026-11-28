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

//! The record-batch decoder shared by [`CompletedFetch`] and
//! [`ShareCompletedFetch`]: a cursor over a fetch's batches and, within the
//! current batch, its records, plus the per-record parse that hands the key and
//! value to the deserializers.
//!
//! **Rust-only (DoD #7).** Java has no such type: both
//! `CompletedFetch#nextFetchedRecord` and `ShareCompletedFetch#nextFetchedRecord`
//! walk `MemoryRecords#batches()` and each batch's
//! `RecordBatch#streamingIterator`, and both `parseRecord`s call the
//! deserializers the same way. The Rust consumer implemented that walk inside
//! `CompletedFetch` with private types (§27 zero-copy, one-record-at-a-time
//! decompression, Java's batch and record checks); the share consumer needs the
//! same walk, so the types live here instead of being duplicated (DoD #6).
//! Everything in this module was moved out of `completed_fetch.rs` unchanged,
//! except that a cursor reports a bad batch through the [`BatchErrorFn`] its
//! owner gives it (Java's `CompletedFetch#maybeEnsureValid` throws a
//! `KafkaException`, `ShareCompletedFetch#maybeEnsureValid` a
//! `CorruptRecordException`), and the deserialization message's last sentence
//! is the caller's (`CompletedFetch#newRecordDeserializationException` and
//! `ShareCompletedFetch#newRecordDeserializationException` differ only there).
//!
//! [`CompletedFetch`]: super::CompletedFetch
//! [`ShareCompletedFetch`]: super::ShareCompletedFetch

use std::io::{self, BufRead, Read};
use std::sync::Arc;

use bytes::Buf;
use log::error;

use crate::common::Error;
use crate::common::InvalidRecordError;
use crate::common::TopicPartition;
use crate::common::compress::{Compression, DecompressingReader};
use crate::common::errors::DeserializationErrorOrigin;
use crate::common::errors::RecordDeserializationError;
use crate::common::header::RecordHeaders;
use crate::common::record::TimestampType;
use crate::common::record::internal::{
    AbstractRecords, ByteBufferLogInputStream, CompressionType, DefaultRecord, DefaultRecordBatch,
    DefaultRecordBatchRef, DefaultRecordRef, MemoryRecords, RecordBatch, RecordVersion,
};
use crate::common::serialization::DynDeserializer;
use crate::common::utils::internals::BufferSupplier;
use crate::common::utils::internals::ByteUtils;
use crate::common::utils::internals::ChunkedBytesStream;
use crate::consumer::{ConsumerRecord, ConsumerRecordOptionsBuilder};

/// A batch [`BatchCursor::next_batch`] read: its metadata, where its records
/// are, and its declared record count.
pub(crate) type NextBatch = (BatchMetadata, PendingRecordSource, i32);

/// A batch [`BatchCursor::next_batch_moving_past_refused`] refused: the error
/// and, when the batch's size was usable, its `(base offset, last offset)`.
pub(crate) type RefusedBatch = (Error, Option<(i64, i64)>);

/// Builds the error a cursor reports a bad batch with, from the partition, the
/// batch's base offset and the cause's message.
pub(crate) type BatchErrorFn = fn(&TopicPartition, i64, &str) -> Error;

/// Sentinel value: a partition leader epoch that is unknown / unset.
pub(crate) const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// The most room a compressed record is given before its bytes have arrived
/// ([`RecordStream::read_record`]). Java allocates a record's declared size at
/// once (`ByteBuffer.allocate(sizeOfBodyInBytes)`, `DefaultRecord#readFrom(InputStream,long,long,int,Long,int)`),
/// trusting the size up to `maxRecordBodySize`; a record declared bigger than
/// this grows as its bytes are decompressed instead, so a forged size costs no
/// more than the bytes behind it, and in Rust a failed allocation aborts the
/// process where Java throws `OutOfMemoryError`.
pub(crate) const MAX_RECORD_PREALLOCATION: usize = 64 * 1024;

/// The most bytes a record's size field takes: a varint holding an `i32`.
const MAX_RECORD_SIZE_LENGTH: usize = 5;

/// Snapshot of the metadata of the current batch the cursor is on. Captures
/// the fields the per-record path needs after the iter_records borrow is
/// dropped.
#[derive(Clone, Debug)]
pub(crate) struct BatchMetadata {
    pub(crate) base_offset: i64,
    pub(crate) base_timestamp: i64,
    pub(crate) base_sequence: i32,
    /// The batch's log-append timestamp (max timestamp), used when
    /// `timestamp_type == LogAppendTime` to override per-record timestamps.
    pub(crate) last_offset_timestamp: i64,
    pub(crate) last_offset: i64,
    pub(crate) next_offset: i64,
    pub(crate) timestamp_type: TimestampType,
    pub(crate) partition_leader_epoch: i32,
    pub(crate) is_control_batch: bool,
    pub(crate) is_transactional: bool,
    pub(crate) has_producer_id: bool,
    pub(crate) producer_id: i64,
    pub(crate) magic: i8,
    /// Where the batch starts in the cursor's buffer, its size field first.
    pub(crate) batch_start: usize,
    /// Where the batch ends in the cursor's buffer (exclusive).
    pub(crate) batch_end: usize,
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
pub(crate) struct BatchCursor {
    /// Records buffer moved out of `partition_data.records` exactly once
    /// per `CompletedFetch` (no clone).
    pub(crate) memory_records: MemoryRecords,
    /// Absolute byte offset of the next batch header in
    /// `memory_records.buffer()`. Advanced incrementally as each batch is
    /// consumed (NOT recomputed from 0), so locating the next batch is O(1).
    /// `None` means iteration has terminated.
    pub(crate) next_batch_start: Option<usize>,
    /// Metadata of the batch we're currently iterating; `None` before the
    /// first batch is loaded.
    pub(crate) current_batch: Option<BatchMetadata>,
    /// Where the current batch's record bytes live, and how to decode the
    /// next record. The key/value/header bytes of each record are borrowed
    /// from this source — never copied.
    pub(crate) record_source: RecordSource,
    /// Byte offset of the next record to decode, relative to the start of
    /// the current batch's record section. Unused for a compressed batch,
    /// whose stream holds the current record in a buffer of its own instead.
    pub(crate) record_byte_offset: usize,
    /// Number of records left to decode in the current batch.
    pub(crate) records_remaining: i32,
    /// Builds the error a bad batch is reported with: the owner's
    /// `maybeEnsureValid` exception type.
    pub(crate) batch_error: BatchErrorFn,
    /// Builds the error a compressed batch's failing stream is reported with:
    /// the consumer's `maybeEnsureValid` text, or, for a share fetch, the bare
    /// `KafkaException` Java's stream throws
    /// (`DefaultRecordBatch.StreamRecordIterator#readNext`).
    pub(crate) stream_error: BatchErrorFn,
}

/// Where the current batch's record bytes come from. Uncompressed batches
/// borrow their bytes directly from the cursor's `MemoryRecords` buffer (we
/// remember the byte range so we don't re-walk the batch iterator per
/// record). Compressed batches are decompressed one record at a time.
#[derive(Debug)]
pub(crate) enum RecordSource {
    /// No record bytes held: no batch loaded yet, the current batch was
    /// exhausted and released by [`CompletedFetch::maybe_close_record_stream`],
    /// or iteration finished.
    None,
    /// Uncompressed: record bytes are `memory_records.buffer()[range]`.
    Borrowed(std::ops::Range<usize>),
    /// Compressed: records are decompressed as the cursor reaches them, each
    /// into a refcounted buffer of its own, so per-record key/value slices can
    /// be handed out zero-copy via [`bytes::Bytes::slice_ref`] (§27). Boxed, as the
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
/// has accepted the count (`DefaultRecordBatch#compressedIterator` and the
/// `DefaultRecordBatch.RecordIterator` constructor), and a skipped batch never
/// gets an iterator (`CompletedFetch#nextFetchedRecord`).
#[derive(Debug)]
pub(crate) enum PendingRecordSource {
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
/// (`DefaultRecordBatch#compressedIterator`) — held by the cursor, which
/// does the counting `RecordIterator` does (DoD #7). Each record is read into
/// a buffer of its own, as Java reads it
/// (`DefaultRecord#readFrom(InputStream,long,long,int,Long,int)`): its size field, then as many bytes as
/// it declares, in one allocation. The record is handed out as a slice of that
/// buffer (§27), which lives for as long as the user holds any of it, so what
/// a batch's records pin once handed out is their own size and nothing more.
/// Single-record batches are common, from producers that send at a low rate
/// per partition and from compacted topics, and a poll can return hundreds of
/// them.
///
/// The stream is read through the codec's [`ChunkedBytesStream`], as Java
/// reads it, with the buffer size Java gives it
/// ([`Compression::decompression_output_size`]): a record's size field is read
/// a byte at a time, and a small record a few bytes at a time, and each such
/// read would otherwise go to the codec. The buffer comes from the fetch's
/// pool and goes back to it when the batch is done, so the batches of a fetch
/// share one buffer, as they do in Java (`CompletedFetch#nextFetchedRecord`).
///
/// So a compressed batch takes the codec's own state, the read buffer and one
/// record, whatever its decompressed size. The whole batch is never
/// decompressed at once: a few kilobytes of compressed data can expand to
/// gigabytes, and in Rust a failed allocation aborts the process where Java
/// throws `OutOfMemoryError`. For the same reason a record is given room for
/// its declared size up front only up to [`MAX_RECORD_PREALLOCATION`]; a
/// bigger one grows as its bytes arrive ([`Self::read_record`]).
///
/// This costs one allocation per record beyond the deserializer's output, and
/// so deviates from `consumer-threading.md` §27, which budgets none for a
/// record's bytes and has a compressed batch decompressed into one buffer.
/// That one buffer is what the paragraph above rules out, and records read
/// into a buffer shared with the records around them would keep all of it
/// alive for as long as any one of them is held. A buffer per record avoids
/// both, at the cost Java pays.
pub(crate) struct RecordStream {
    /// The decompression stream over the batch's records section, read
    /// through a buffer of the codec's size taken from the fetch's pool, which
    /// gets it back when the stream is dropped.
    pub(crate) reader: ChunkedBytesStream<DecompressingReader<io::Cursor<bytes::Bytes>>>,
    /// The record last read, its size field first, less what
    /// [`Self::consume`] dropped: the whole record, or as much of it as the
    /// stream held.
    record: bytes::Bytes,
    /// Whether `reader` has reached the end of the stream.
    eof: bool,
    /// The error `reader` failed with, kept so that every later read reports
    /// it rather than reading on.
    error: Option<io::Error>,
}

impl RecordStream {
    /// Opens the decompression stream over a batch's `records` section, its
    /// read buffer taken from `buffer_supplier`. Reads nothing yet, except
    /// what the codec reads to set itself up (a gzip header).
    pub(crate) fn open(
        records: bytes::Bytes,
        compression_type: CompressionType,
        magic: i8,
        buffer_supplier: Arc<BufferSupplier>,
    ) -> io::Result<Self> {
        let compression = Compression::of(compression_type).build();
        // Defined for every codec; an uncompressed batch's records are borrowed
        // in place and never reach here.
        let buffer_size = compression.decompression_output_size().map_err(io::Error::other)?;
        let reader = compression.wrap_for_input(io::Cursor::new(records), magic)?;
        // Java's `wrapForInput` returns the codec's stream already wrapped
        // (`org.apache.kafka.common.compress.GzipCompression#wrapForInput`,
        // `org.apache.kafka.common.compress.SnappyCompression#wrapForInput`,
        // `org.apache.kafka.common.compress.ZstdCompression#wrapForInput`,
        // `org.apache.kafka.common.compress.Lz4Compression#wrapForInput`), lz4 alone
        // skipping through its own stream. `wrap_for_input` is public and
        // `ChunkedBytesStream` crate-private, so the stream is wrapped here.
        let delegate_skip_to_source_stream = compression_type == CompressionType::Lz4;
        Ok(Self {
            reader: ChunkedBytesStream::new(reader, buffer_supplier, buffer_size, delegate_skip_to_source_stream),
            record: bytes::Bytes::new(),
            eof: false,
            error: None,
        })
    }

    /// The buffer the current record was read into, once [`Self::next_record`]
    /// has read it: the record's bytes and nothing else.
    pub(crate) fn record(&self) -> &bytes::Bytes {
        &self.record
    }

    /// Reads the next record, once the one before it has been consumed, and
    /// returns its bytes: the whole record; as much of it as the stream holds,
    /// for the record parser to reject; or nothing, once the stream has ended.
    /// A record is kept until it is consumed, so reading it again, after a
    /// deserializer failed on it, reads nothing more.
    ///
    /// # Errors
    ///
    /// The stream failed before the record was whole. Java's
    /// `StreamRecordIterator.readNext` reports any `IOException` this way
    /// (`DefaultRecordBatch.StreamRecordIterator#readNext`).
    pub(crate) fn next_record(&mut self) -> Result<&[u8], InvalidRecordError> {
        if self.record.is_empty() && !self.eof && self.error.is_none() {
            self.read_record();
        }
        if let Some(e) = &self.error {
            return Err(DefaultRecordBatch::decompression_error(e));
        }
        Ok(&self.record)
    }

    /// Drops the record just read, its first `consumed` bytes. A buffer of up
    /// to [`MAX_RECORD_PREALLOCATION`] bytes is kept, for
    /// [`Self::read_record`] to read the next record into when nothing else
    /// holds it any more; a bigger one is let go of once all of it is
    /// consumed, so a big record is not held while the next is read. A record
    /// a deserializer sliced keeps the buffer alive through its own reference,
    /// and the next record then gets a buffer of its own. Java keeps the record
    /// in `CompletedFetch#lastRecord` until the next one is read;
    /// nothing a caller sees depends on that.
    pub(crate) fn consume(&mut self, consumed: usize) {
        let size = self.record.len();
        self.record.advance(consumed.min(size));
        if self.record.is_empty() && size > MAX_RECORD_PREALLOCATION {
            self.record = bytes::Bytes::new();
        }
    }

    /// Whether any bytes follow the record last read, consumed or not: Java's
    /// `ensureNoneRemaining`, which reads one more byte to find out
    /// (`DefaultRecordBatch.StreamRecordIterator#ensureNoneRemaining`). Reaching the end of the stream
    /// also makes the codec verify its trailing checksum, where it has one.
    ///
    /// # Errors
    ///
    /// The stream failed before its end, with Java's text for that.
    pub(crate) fn has_remaining(&mut self) -> Result<bool, InvalidRecordError> {
        if !self.eof && self.error.is_none() {
            match self.reader.fill_buf() {
                Ok([]) => self.eof = true,
                Ok(_) => return Ok(true),
                Err(e) => self.error = Some(e),
            }
        }
        match &self.error {
            Some(e) => Err(InvalidRecordError::new(format!(
                "Error checking for remaining bytes after reading batch: {e}"
            ))),
            None => Ok(false),
        }
    }

    /// Reads the next record into a buffer of its own, as Java's
    /// `DefaultRecord.readFrom(InputStream, ...)` does
    /// (`DefaultRecord#readFrom(InputStream,long,long,int,Long,int)`): the size field a byte at a time, then
    /// the body. The size field is kept in front of the body, so the record is
    /// parsed as an uncompressed batch's records are. A size that is
    /// malformed, negative or above Java's maximum is kept without its body,
    /// for the record's reader to refuse with Java's text for each
    /// ([`record_size_is_premature_eof`], [`read_record`]); no more of the
    /// stream would change what it makes of them.
    ///
    /// The buffer is allocated once, at the record's size, up to
    /// [`MAX_RECORD_PREALLOCATION`]; a bigger record's buffer grows only as its
    /// bytes arrive, doubling each time it fills, and the last step stops at
    /// the record's size, so the record ends up in a buffer of exactly its
    /// size. Allocating is fallible (`try_reserve_exact`), so an allocation
    /// that cannot be met fails the stream instead of aborting the process. A
    /// record the stream ends or fails inside of is kept as far as it was
    /// read, for the parser to reject.
    pub(crate) fn read_record(&mut self) {
        let mut size_bytes = [0u8; MAX_RECORD_SIZE_LENGTH];
        let mut size_length = 0;
        while size_length < MAX_RECORD_SIZE_LENGTH {
            let Some(byte) = self.read_byte() else {
                self.record = bytes::Bytes::copy_from_slice(&size_bytes[..size_length]);
                return;
            };
            size_bytes[size_length] = byte;
            size_length += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
        let size_bytes = &size_bytes[..size_length];
        let size = ByteUtils::read_varint(size_bytes)
            .ok()
            .filter(|&(size, _)| {
                DefaultRecord::check_record_body_size(size, AbstractRecords::SOFT_MAX_ARRAY_LENGTH).is_ok()
            })
            .and_then(|(size, _)| usize::try_from(size).ok());
        let Some(size) = size else {
            self.record = bytes::Bytes::copy_from_slice(size_bytes);
            return;
        };
        let total = size_length + size;
        let mut record = self.take_spare_buffer();
        if let Err(e) = record.try_reserve_exact(total.min(MAX_RECORD_PREALLOCATION)) {
            self.error = Some(e.into());
            return;
        }
        record.extend_from_slice(size_bytes);
        while record.len() < total {
            if record.len() == record.capacity() {
                let step = record.len().min(total - record.len());
                if let Err(e) = record.try_reserve_exact(step) {
                    self.error = Some(e.into());
                    break;
                }
            }
            // `take` stops the read at the room there is: `read_to_end` grows a
            // buffer only past what it was given room for, and keeps the bytes
            // read before a failure.
            // A reused buffer may hold more than this record: read no further.
            let wanted = record.capacity().min(total) - record.len();
            match (&mut self.reader).take(wanted as u64).read_to_end(&mut record) {
                Ok(read) if read < wanted => {
                    self.eof = true;
                    break;
                },
                Ok(_) => {},
                Err(e) => {
                    self.error = Some(e);
                    break;
                },
            }
        }
        // Adopts the allocation; nothing is copied.
        self.record = bytes::Bytes::from(record);
    }

    /// The buffer of the record read last, emptied, for the next record to be
    /// read into, when nothing else holds it: the key and value handed to the
    /// deserializers share it (`Bytes::slice_ref`), and once they are dropped
    /// it is this stream's alone again. Reusing it keeps a compressed record
    /// at one allocation, the reference count the first slice of it adds,
    /// rather than two, a buffer and its reference count. A buffer still held
    /// elsewhere (a deserializer kept a slice of it) is left to its holder, and
    /// the next record gets a new one, so no record shares a buffer with the
    /// records around it.
    fn take_spare_buffer(&mut self) -> Vec<u8> {
        match std::mem::take(&mut self.record).try_into_mut() {
            Ok(spare) => {
                let mut spare = Vec::from(spare);
                spare.clear();
                spare
            },
            Err(_) => Vec::new(),
        }
    }

    /// The next byte of the stream, or nothing once it has ended or failed,
    /// the failure kept in `error`.
    fn read_byte(&mut self) -> Option<u8> {
        if self.eof || self.error.is_some() {
            return None;
        }
        match self.reader.read() {
            Ok(Some(byte)) => Some(byte),
            Ok(None) => {
                self.eof = true;
                None
            },
            Err(e) => {
                self.error = Some(e);
                None
            },
        }
    }
}

impl std::fmt::Debug for RecordStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The decompressor has no `Debug` of its own.
        f.debug_struct("RecordStream")
            .field("record_len", &self.record.len())
            .field("eof", &self.eof)
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl BatchCursor {
    /// A cursor over `records`, a fetch's record batches, positioned before
    /// the first batch. `batch_error` builds the error a bad batch is reported
    /// with, `stream_error` the error a compressed batch's failing stream is
    /// with.
    ///
    /// §27 zero-copy: `records` is moved into the cursor's `MemoryRecords`, an
    /// O(1) refcount move; nothing is copied.
    pub(crate) fn new(records: bytes::Bytes, batch_error: BatchErrorFn, stream_error: BatchErrorFn) -> Self {
        Self {
            memory_records: MemoryRecords::new(records),
            next_batch_start: Some(0),
            current_batch: None,
            record_source: RecordSource::None,
            record_byte_offset: 0,
            records_remaining: 0,
            batch_error,
            stream_error,
        }
    }

    /// The buffer the cursor reads: the fetch's record batches.
    pub(crate) fn buffer_bytes(&self) -> &bytes::Bytes {
        self.memory_records.buffer_bytes()
    }

    /// Checks the checksum of `batch`, a batch [`Self::next_batch`] returned:
    /// the half of Java's `RecordBatch#ensureValid` that `next_batch` leaves to
    /// `check.crcs` (its size is always checked there).
    ///
    /// # Errors
    ///
    /// The checksum does not match.
    pub(crate) fn ensure_valid(&self, batch: &BatchMetadata) -> Result<(), InvalidRecordError> {
        let batch_bytes = self
            .memory_records
            .buffer()
            .get(batch.batch_start..batch.batch_end)
            .unwrap_or_default();
        let batch = DefaultRecordBatchRef::new(batch_bytes)?;
        if batch.magic() >= RecordVersion::V2.value() {
            batch.ensure_valid()?;
        }
        Ok(())
    }

    /// Reads the header of the next batch and moves past it, without installing
    /// it: the batch's metadata, where its records are, and its declared record
    /// count. `Ok(None)` once no batch remains.
    ///
    /// Java walks a fetch's batches with
    /// `org.apache.kafka.common.record.internal.ByteBufferLogInputStream#nextBatch`;
    /// the owner then decides whether to skip the batch (READ_COMMITTED) or to
    /// [`Self::install_batch`] it.
    ///
    /// # Errors
    ///
    /// A corrupt size or magic (the stream's `CORRUPT_MESSAGE` error), and the
    /// batch checks reported through the cursor's [`BatchErrorFn`].
    pub(crate) fn next_batch(
        &mut self,
        partition: &TopicPartition,
        check_crcs: bool,
    ) -> Result<Option<(BatchMetadata, PendingRecordSource, i32)>, Error> {
        self.read_next_batch(partition, check_crcs, false).map_err(|(error, _)| error)
    }

    /// [`Self::next_batch`] for a share fetch: a batch refused after its size
    /// was read (its magic, its minimum size, its checksum, its codec) is moved
    /// past, and comes back with the error and its offset range, so the fetch
    /// can reject that batch's own records and read on. Java's
    /// `ShareCompletedFetch#nextFetchedRecord` sets `currentBatch` before
    /// `maybeEnsureValid` and `streamingIterator` can fail, and the next call
    /// takes the next batch. A batch whose size field is unusable comes back
    /// without a range and is not moved past: Java's batch iterator fails on it
    /// for good too.
    ///
    /// # Errors
    ///
    /// As [`Self::next_batch`], with the refused batch's `(base offset, last
    /// offset)` when it has one. For a v0/v1 batch (refused by its magic, D7)
    /// both are the offset field, which is the last offset of a legacy
    /// wrapper; v2 reads its last offset delta when the batch holds it.
    pub(crate) fn next_batch_moving_past_refused(
        &mut self,
        partition: &TopicPartition,
        check_crcs: bool,
    ) -> Result<Option<NextBatch>, RefusedBatch> {
        self.read_next_batch(partition, check_crcs, true)
    }

    /// The body of [`Self::next_batch`]; `move_past_refused` is
    /// [`Self::next_batch_moving_past_refused`]'s.
    fn read_next_batch(
        &mut self,
        partition: &TopicPartition,
        check_crcs: bool,
        move_past_refused: bool,
    ) -> Result<Option<NextBatch>, RefusedBatch> {
        let Some(batch_start) = self.next_batch_start else {
            return Ok(None);
        };

        let buffer = self.memory_records.buffer();
        // Java walks a fetch's batches with
        // `org.apache.kafka.common.record.internal.ByteBufferLogInputStream#nextBatch`:
        // every header goes through `nextBatchSize()` — sign, minimum size, magic — before the batch is
        // handed out as a slice limited to exactly its declared size. The header
        // is never trusted for where the next batch starts until it has passed.
        // The stream borrows the cursor's buffer, so it is rebuilt over the unread
        // tail per batch rather than stored in the cursor; it is a slice, a
        // position and a limit on the stack, and allocates nothing.
        let unread = buffer.get(batch_start..).unwrap_or_default();
        // A corrupt size or magic propagates as the stream's `CORRUPT_MESSAGE`
        // error, unwrapped, as Java's `batches.hasNext()` throws it (in
        // `org.apache.kafka.clients.consumer.internals.CompletedFetch#nextFetchedRecord`)
        // — it is not "no batch".
        let batch_size = match ByteBufferLogInputStream::new(unread, i32::MAX)
            .next_batch_size()
            .map_err(|e| (e, None))?
        {
            Some(batch_size) if batch_size <= unread.len() => batch_size,
            // `batchSize == null || remaining < batchSize` → no batch
            // (`org.apache.kafka.common.record.internal.ByteBufferLogInputStream#nextBatch`):
            // a partial trailing batch, as a broker cutting the response at
            // `max.partition.fetch.bytes` leaves, ends iteration without an error.
            _ => {
                self.next_batch_start = None;
                return Ok(None);
            },
        };
        let batch_bytes = &unread[..batch_size];
        let refused_range = refused_batch_range(batch_bytes);
        let refused = |this: &mut Self, error: Error| {
            if move_past_refused {
                this.next_batch_start = Some(batch_start + batch_size);
            }
            (error, Some(refused_range))
        };

        // D7: message formats v0 and v1 are refused by their
        // magic before anything reads the header as a v2 one. In bounds:
        // `next_batch_size` returns a size only once the magic byte is present.
        let magic = batch_bytes[RecordBatch::MAGIC_OFFSET] as i8;
        if magic < RecordBatch::MAGIC_VALUE_V2 {
            let error = (self.batch_error)(
                partition,
                raw_base_offset(batch_bytes),
                ByteBufferLogInputStream::unsupported_magic_error(magic).message(),
            );
            return Err(refused(self, error));
        }

        // D2: the size half of Java's `ensureValid()`
        // (`org.apache.kafka.common.record.internal.DefaultRecordBatch#ensureValid`),
        // reported the way `CompletedFetch#maybeEnsureValid(FetchConfig,RecordBatch)`
        // reports it but run
        // unconditionally, where Java runs it only under `check.crcs`. Without
        // `check.crcs` Java reads the header of a batch shorter than 61 bytes
        // anyway and its absolute reads throw `IndexOutOfBoundsException`; in
        // Rust those reads would be slice panics, which inside the C and Python
        // bindings abort the process. The view is only built over a batch that passes, so no
        // header accessor below can read past it.
        let batch = match DefaultRecordBatchRef::new(batch_bytes) {
            Ok(batch) => batch,
            Err(e) => {
                let error = (self.batch_error)(partition, raw_base_offset(batch_bytes), e.message());
                return Err(refused(self, error));
            },
        };

        // The checksum half of Java's `maybeEnsureValid(batch)`, still gated
        // on `check.crcs` as in Java.
        if check_crcs
            && batch.magic() >= RecordVersion::V2.value()
            && let Err(e) = batch.ensure_valid()
        {
            let error = (self.batch_error)(partition, batch.base_offset(), e.message());
            return Err(refused(self, error));
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
            batch_start,
            batch_end: batch_start + batch_size,
        };

        // Describe where this batch's records live; they are materialized
        // by `install_batch`, after the owner's READ_COMMITTED skip and the count check.
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
        let compression_type = match batch.try_compression_type() {
            Ok(compression_type) => compression_type,
            Err(e) => return Err(refused(self, e)),
        };
        let source = match compression_type {
            // Borrow the records section directly from the canonical
            // buffer.
            CompressionType::None => PendingRecordSource::Borrowed(records_start..records_end),
            // Decompressed one record at a time once the batch is
            // installed. The slice shares the fetch buffer (a refcount
            // bump), so the stream can own its input.
            compression_type => PendingRecordSource::Compressed {
                records: self.memory_records.buffer_bytes().slice(records_start..records_end),
                compression_type,
            },
        };

        let records_count = batch.records_count();
        // Advance the cursor's next-batch pointer by this batch's validated
        // size (O(1)) so both the skip path and the load path move forward.
        self.next_batch_start = Some(batch_start + batch_size);
        Ok(Some((meta, source, records_count)))
    }

    /// Installs a batch [`Self::next_batch`] returned as the current one:
    /// refuses a negative record count, then borrows the records in place, or
    /// opens the stream a compressed batch's records are read through, one at
    /// a time, as the cursor reaches them (§27).
    ///
    /// D3: Java's `RecordIterator` constructor rejects a negative record count
    /// (`org.apache.kafka.common.record.internal.DefaultRecordBatch.RecordIterator`)
    /// when `currentBatch.streamingIterator(...)` builds it, unwrapped. Installing
    /// it instead would treat the batch as empty. It also comes before the
    /// records are materialized, so a compressed batch with a bad count is
    /// refused for the price of reading its header: Java's `compressedIterator`
    /// has built the decompression stream by then but read nothing through it.
    ///
    /// # Errors
    ///
    /// The negative count, or a compressed stream that cannot be opened,
    /// reported through the cursor's [`BatchErrorFn`].
    pub(crate) fn install_batch(
        &mut self,
        partition: &TopicPartition,
        batch_meta: BatchMetadata,
        source: PendingRecordSource,
        records_count: i32,
        buffer_supplier: &Arc<BufferSupplier>,
    ) -> Result<(), Error> {
        if records_count < 0 {
            return Err(Error::InvalidRecord(DefaultRecordBatch::invalid_record_count_error(
                records_count,
                batch_meta.magic,
            )));
        }
        let source = match source {
            PendingRecordSource::Borrowed(range) => RecordSource::Borrowed(range),
            PendingRecordSource::Compressed { records, compression_type } => {
                let stream =
                    RecordStream::open(records, compression_type, batch_meta.magic, Arc::clone(buffer_supplier))
                        .map_err(|e| {
                            (self.stream_error)(
                                partition,
                                batch_meta.base_offset,
                                DefaultRecordBatch::decompression_error(&e).message(),
                            )
                        })?;
                RecordSource::Stream(Box::new(stream))
            },
        };
        // Install the batch as the current one. We use the batch
        // header's declared record count (not the offset span, which can
        // exceed the record count after log compaction).
        self.record_source = source;
        self.record_byte_offset = 0;
        self.records_remaining = records_count;
        self.current_batch = Some(batch_meta);
        Ok(())
    }

    /// Releases the current batch's record bytes once the batch is exhausted:
    /// for a compressed batch this drops its [`RecordStream`], whose read buffer
    /// goes back to the fetch's pool (Java's `records.close()` in
    /// `CompletedFetch#maybeCloseRecordStream` / `ShareCompletedFetch#maybeCloseRecordStream`).
    pub(crate) fn close_record_stream(&mut self) {
        self.record_source = RecordSource::None;
    }

    /// Advances the within-batch record cursor by `record_bytes_consumed`
    /// bytes and decrements the remaining-records count.
    pub(crate) fn advance_record_cursor(&mut self, record_bytes_consumed: usize) {
        match &mut self.record_source {
            RecordSource::Stream(stream) => stream.consume(record_bytes_consumed),
            _ => self.record_byte_offset += record_bytes_consumed,
        }
        self.records_remaining -= 1;
    }

    /// Reads the current record of a compressed batch out of its stream, for
    /// [`Self::peek_current_record`] to parse. An uncompressed batch's records
    /// are in place already, so for one this does nothing.
    ///
    /// # Errors
    ///
    /// The stream failed before the record was whole, reported the way
    /// `maybeEnsureValid` reports a bad batch (the cursor's [`BatchErrorFn`]).
    pub(crate) fn read_current_record(&mut self, partition: &TopicPartition) -> Result<(), Error> {
        let (RecordSource::Stream(stream), Some(batch)) = (&mut self.record_source, &self.current_batch) else {
            return Ok(());
        };
        match stream.next_record() {
            Ok(_) => Ok(()),
            Err(e) => Err((self.stream_error)(partition, batch.base_offset, e.message())),
        }
    }

    /// Validates, when the current record is the last its batch declares, that
    /// no record bytes follow it. `record_size` is the current record's length.
    ///
    /// This is Java's `ensureNoneRemaining()`, which
    /// `DefaultRecordBatch.RecordIterator.next()` runs as it reads a batch's
    /// last declared record, throwing `InvalidRecordException` ("Incorrect
    /// declared batch size, records still remaining") instead of returning
    /// that record when the declared count is *fewer* than the records
    /// actually present (`DefaultRecordBatch.RecordIterator#next`). So the record is
    /// withheld and the position stays on it: the records before it are
    /// returned and the error comes on the next call, as Java's `fetchRecords`
    /// returns the records it has (`CompletedFetch#fetchRecords`), and seeking
    /// past the record lands on the batch's next offset. Java runs the check
    /// whether the record is then skipped or returned, independently of CRC,
    /// so it must fire even under `check.crcs=false`; and never for a batch
    /// declaring no records, whose iterator reads nothing. Without it the
    /// cursor would silently drop the trailing records.
    ///
    /// For an uncompressed batch this is O(1): it compares where the record
    /// ends against the (already-known) length of the batch's record section —
    /// no re-walk and no copy. A compressed batch's stream is read one byte
    /// further, as Java's `StreamRecordIterator` reads it, which also lets the
    /// codec verify the stream's trailing checksum.
    ///
    /// # Errors
    ///
    /// Records remain, or the compressed stream failed while looking for them
    /// (Java's `KafkaException("Error checking for remaining bytes after
    /// reading batch", e)`, `DefaultRecordBatch.StreamRecordIterator#ensureNoneRemaining`).
    pub(crate) fn ensure_none_remaining(
        &mut self,
        partition: &TopicPartition,
        record_size: usize,
    ) -> Result<(), Error> {
        let Some(batch) = self.current_batch.as_ref() else {
            return Ok(());
        };
        if self.records_remaining != 1 {
            return Ok(());
        }
        let records_remain = match &mut self.record_source {
            RecordSource::None => false,
            RecordSource::Borrowed(range) => self.record_byte_offset + record_size < range.len(),
            RecordSource::Stream(stream) => stream
                .has_remaining()
                .map_err(|e| (self.stream_error)(partition, batch.base_offset, e.message()))?,
        };
        if records_remain {
            return Err(records_remaining_error(partition));
        }
        Ok(())
    }

    /// Returns the refcounted buffer that owns the current batch's record
    /// bytes, so per-record key/value slices can be handed to the deserializer
    /// as zero-copy `Bytes` via [`bytes::Bytes::slice_ref`] (§27).
    ///
    /// For an uncompressed batch this is the whole `MemoryRecords` buffer (the
    /// borrowed record slice is a subslice of it); for a compressed batch it is
    /// the buffer the current record was decompressed into. Returns `None` when
    /// no batch's record bytes are held.
    pub(crate) fn current_record_source_bytes(&self) -> Option<&bytes::Bytes> {
        match &self.record_source {
            RecordSource::None => None,
            RecordSource::Borrowed(_) => Some(self.memory_records.buffer_bytes()),
            RecordSource::Stream(stream) => Some(stream.record()),
        }
    }

    /// Parses the current record (the one at `record_byte_offset`)
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
    pub(crate) fn peek_current_record(
        &self,
        partition: &TopicPartition,
    ) -> Result<Option<(DefaultRecordRef<'_>, &BatchMetadata)>, Error> {
        if self.records_remaining <= 0 {
            return Ok(None);
        }
        let Some(batch_meta) = self.current_batch.as_ref() else {
            return Ok(None);
        };
        let records_bytes = match &self.record_source {
            RecordSource::None => return Ok(None),
            RecordSource::Borrowed(range) => &self.memory_records.buffer()[range.clone()],
            // The current record is in a buffer of its own, and
            // `record_byte_offset` stays 0.
            RecordSource::Stream(stream) => &stream.record()[..],
        };
        let from_stream = matches!(self.record_source, RecordSource::Stream(_));
        if self.record_byte_offset >= records_bytes.len()
            || record_size_is_premature_eof(&records_bytes[self.record_byte_offset..], from_stream)
        {
            return Ok(None);
        }
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
        let (record, _consumed) = read_record(batch_meta, &records_bytes[self.record_byte_offset..], from_stream)
            .map_err(|e| {
                Error::InvalidRecord(InvalidRecordError::new(format!(
                    "Record batch for partition {} at offset {} is invalid, cause: {}",
                    partition,
                    batch_meta.base_offset,
                    e.message()
                )))
            })?;
        Ok(Some((record, batch_meta)))
    }
}

/// Decodes the record the cursor is on, which [`BatchCursor::peek_current_record`]
/// has parsed, into a [`ConsumerRecord`], running the deserializers over its
/// key and value. Returns the record, its size in bytes (for the fetch's byte
/// count) and the bytes the cursor moves past it by. Does not move the cursor:
/// a call dropped while a deserializer is pending leaves the cursor on the
/// record.
///
/// This is the body of Java's `CompletedFetch#parseRecord` and
/// `ShareCompletedFetch#parseRecord`, which differ only in the record's
/// delivery count (`Optional.of(deliveryCount)` for a share fetch) and the
/// last sentence of the deserialization error's message, `deserialization_tail`.
///
/// # Errors
///
/// A record that went missing between positioning and reading (premature
/// EOF), headers that fail to parse, and Java's
/// `RecordDeserializationException` when a deserializer fails. The value
/// deserializer is not run when the key's failed: Java's `parseRecord` is two
/// sequential `try` blocks and the first one's catch throws.
pub(crate) async fn parse_record<K, V>(
    cursor: &BatchCursor,
    partition: &TopicPartition,
    topic: &Arc<str>,
    key_deserializer: &dyn DynDeserializer<K>,
    value_deserializer: &dyn DynDeserializer<V>,
    delivery_count: Option<i16>,
    deserialization_tail: &str,
) -> Result<(ConsumerRecord<K, V>, i32, usize), Error>
where
    K: Send + 'static,
    V: Send + 'static,
{
    // §27: read the record as a borrowing `DefaultRecordRef` — the
    // key/value bytes are slices into the fetch (or decompression)
    // buffer, never copied. They are handed to the deserializers as
    // `Bytes` sharing that buffer (`slice_ref`: a reference count, no
    // copy), so the borrow of the record ends before the deserializers are
    // awaited.
    let key_bytes: Option<bytes::Bytes>;
    let value_bytes: Option<bytes::Bytes>;
    let leader_epoch;
    let timestamp_type;
    let key_size;
    let value_size;
    let offset;
    let timestamp;
    let record_size_in_bytes;
    let record_bytes_consumed;
    let mut headers_owned;
    {
        // The `?` propagates a malformed-record-body error; a `None` here would
        // mean the record went missing between positioning and reading, which
        // is the premature-EOF (declared count > actual) state — surface
        // it as a recoverable error rather than panicking.
        let Some((record, batch_meta)) = cursor.peek_current_record(partition)? else {
            return Err(premature_eof_error(partition));
        };
        // §27: the refcounted buffer that owns this record's key/value
        // bytes, which the key and value are sliced from.
        let Some(source_bytes) = cursor.current_record_source_bytes() else {
            return Err(premature_eof_error(partition));
        };
        // The only owned copy of record payload on the happy path:
        // the §27-sanctioned `RecordHeaders` (Milestone-8 holds
        // owned headers on the emitted `ConsumerRecord`).
        // By the cause's message, not its class-prefixed `Display`, as in
        // `peek_current_record`.
        let headers_vec = record.headers().map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!(
                "Record for partition {} at offset {} has invalid headers, cause: {}",
                partition,
                record.offset(),
                e.message()
            )))
        })?;
        headers_owned = RecordHeaders::with_header_iter(headers_vec);
        key_bytes = record.key().map(|key| source_bytes.slice_ref(key));
        value_bytes = record.value().map(|value| source_bytes.slice_ref(value));
        leader_epoch = maybe_leader_epoch(batch_meta.partition_leader_epoch);
        timestamp_type = batch_meta.timestamp_type;
        key_size = record.key_size();
        value_size = record.value_size();
        offset = record.offset();
        timestamp = record.timestamp();
        record_size_in_bytes = record.size_in_bytes();
        record_bytes_consumed = record.size_in_bytes() as usize;
    }

    // Java's `parseRecord` hands each deserializer the record's own buffer,
    // `record.key()` / `record.value()`; `deserialize_with_headers_bytes` is
    // that call, its default forwarding the borrowed bytes.
    let topic_str: &str = topic;
    let key_result = match &key_bytes {
        None => Ok(None),
        Some(key) => {
            DynDeserializer::deserialize_with_headers_bytes(key_deserializer, topic_str, &mut headers_owned, key)
                .await
                .map(Some)
        },
    };
    // Java's `parseRecord` is two sequential `try` blocks and the first one's
    // catch *throws*, so the value deserializer is never invoked for a record
    // whose key failed. Running it anyway is observable: a user deserializer
    // may count, cache or log, and it would do so for a record Java never hands
    // it. The key error is returned below before this value is read, so
    // `Ok(None)` here is inert.
    let value_result = if key_result.is_err() {
        Ok(None)
    } else {
        match &value_bytes {
            None => Ok(None),
            Some(value) => DynDeserializer::deserialize_with_headers_bytes(
                value_deserializer,
                topic_str,
                &mut headers_owned,
                value,
            )
            .await
            .map(Some),
        }
    };
    // Java's `newRecordDeserializationException` carries the offending
    // record's RAW key and value buffers (`record.key()` /
    // `record.value()`) so a consumer error handler can inspect them,
    // BOTH regardless of which side failed. They are copied out ONLY
    // when a deserializer actually failed, so on the happy path §27's
    // "no per-record payload copy" guarantee holds.
    let (mut error_key_bytes, mut error_value_bytes) = if key_result.is_err() || value_result.is_err() {
        (
            key_bytes.as_deref().map(<[u8]>::to_vec),
            value_bytes.as_deref().map(<[u8]>::to_vec),
        )
    } else {
        (None, None)
    };

    let key = match key_result {
        Ok(k) => k,
        Err(e) => {
            let err = wrap_deserialization_error(
                DeserializationOrigin::Key,
                partition,
                offset,
                timestamp,
                timestamp_type,
                error_key_bytes.take(),
                error_value_bytes.take(),
                Some(headers_owned.clone()),
                e,
                deserialization_tail,
            );
            // Java logs the failing deserializer inside `parseRecord` before
            // throwing.
            error!("Key deserialization failed for {partition} at offset {offset}");
            return Err(err);
        },
    };
    let value = match value_result {
        Ok(v) => v,
        Err(e) => {
            let err = wrap_deserialization_error(
                DeserializationOrigin::Value,
                partition,
                offset,
                timestamp,
                timestamp_type,
                error_key_bytes.take(),
                error_value_bytes.take(),
                Some(headers_owned.clone()),
                e,
                deserialization_tail,
            );
            error!("Value deserialization failed for {partition} at offset {offset}");
            return Err(err);
        },
    };

    // §27: cheap Arc clone — atomic pointer bump, no UTF-8 copy.
    let consumer_record = ConsumerRecord::with_options(
        ConsumerRecordOptionsBuilder::new()
            .set_topic(Arc::clone(topic))
            .set_partition(partition.partition())
            .set_offset(offset)
            .set_key(key)
            .set_value(value)
            .set_timestamp(timestamp)
            .set_timestamp_type(timestamp_type)
            .set_serialized_key_size(key_size)
            .set_serialized_value_size(value_size)
            .set_headers(headers_owned)
            .set_leader_epoch(leader_epoch)
            .set_delivery_count(delivery_count)
            .build()?,
    );
    Ok((consumer_record, record_size_in_bytes, record_bytes_consumed))
}

/// Categorizes which side of the deserialization (key vs value) failed.
/// Mirrors Java's
/// `RecordDeserializationException.DeserializationExceptionOrigin`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DeserializationOrigin {
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
/// Mirrors `CompletedFetch#newRecordDeserializationException`,
/// which passes the full record context — origin, partition, offset, timestamp,
/// timestamp type, raw key and value buffers, and headers — alongside the
/// message, with the deserializer's exception as the **cause**. Those eight
/// fields are the ones consumer error handlers read to decide whether to skip
/// the record, so a plain `SerializationException` carrying only the message
/// would drop the information the type exists to convey.
///
/// `tail` is the message's last sentence, which differs between
/// `CompletedFetch` and `ShareCompletedFetch`.
///
/// The message itself carries no "Cause: ..." suffix; the cause is a separate
/// field reachable through [`Error::source`].
#[expect(clippy::too_many_arguments)]
pub(crate) fn wrap_deserialization_error(
    origin: DeserializationOrigin,
    partition: &TopicPartition,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    key_buffer: Option<Vec<u8>>,
    value_buffer: Option<Vec<u8>>,
    headers: Option<RecordHeaders>,
    source: Error,
    tail: &str,
) -> Error {
    let message = format!(
        "Error deserializing {} for partition {} at offset {}. {}",
        origin.as_str(),
        partition,
        offset,
        tail,
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

/// The batch declares more records than it holds: Java's
/// `InvalidRecordException("Incorrect declared batch size, premature EOF
/// reached")` (`DefaultRecordBatch#uncompressedIterator`, `DefaultRecordBatch.StreamRecordIterator#readNext`),
/// naming the partition.
pub(crate) fn premature_eof_error(partition: &TopicPartition) -> Error {
    Error::InvalidRecord(InvalidRecordError::new(format!(
        "Incorrect declared batch size for partition {partition}, premature EOF reached \
         (declared record count exceeds the records present in the batch)"
    )))
}

/// Whether the size of the record at the front of `records` does not end
/// where Java's reader needs it to, which Java reports as a premature EOF.
/// `records` may end inside it, so the buffer reader runs out of bytes
/// (`BufferUnderflowException`, `DefaultRecordBatch#uncompressedIterator`). Or, for a
/// record read off a compressed stream (`from_stream`), its fifth byte may not
/// be its last: the stream reader takes the end of the stream for a
/// continuation byte, so it cannot tell a size the stream ends inside from
/// one too long, and fails on a fifth continuation byte either way
/// (`IllegalArgumentException`, `DefaultRecordBatch.StreamRecordIterator#readNext`). An uncompressed record's size too
/// long is left to the parser, as Java's buffer reader lets that
/// `IllegalArgumentException` through. A stream that fails rather than ends
/// inside the size is reported by [`RecordStream::next_record`] first.
pub(crate) fn record_size_is_premature_eof(records: &[u8], from_stream: bool) -> bool {
    let size = &records[..records.len().min(MAX_RECORD_SIZE_LENGTH)];
    (from_stream || size.len() < MAX_RECORD_SIZE_LENGTH) && size.iter().all(|b| b & 0x80 != 0)
}

/// Parses the record at the front of `records`, the rest of `batch`'s record
/// bytes, returning it and its length.
///
/// A record whose body is cut off fails with the text of the Java reader that
/// reads it: the buffer reader's for an uncompressed batch
/// (`DefaultRecord#readFrom(ByteBuffer,int,long,long,int,Long)`), the stream reader's when `from_stream`
/// (`DefaultRecord#readFrom(InputStream,long,long,int,Long,int)`). A stream's records end early only where the
/// stream does, as [`RecordStream::next_record`] reads each record whole first.
/// Java's stream reader also refuses a size above
/// [`AbstractRecords::SOFT_MAX_ARRAY_LENGTH`] before reading the body
/// (`DefaultRecord#readFrom(InputStream,long,long,int,Long,int)`). [`RecordStream::next_record`] leaves such a
/// record's body unread, and it fails here with Java's text.
pub(crate) fn read_record<'a>(
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
            Ok((size, size_length)) if from_stream => {
                match DefaultRecord::check_record_body_size(size, AbstractRecords::SOFT_MAX_ARRAY_LENGTH) {
                    Err(too_big) => too_big,
                    Ok(()) if usize::try_from(size).is_ok_and(|size| records.len() - size_length < size) => {
                        InvalidRecordError::new(format!(
                            "Invalid record size: expected {size} bytes in record payload, but the record payload reached EOF."
                        ))
                    },
                    Ok(()) => e,
                }
            },
            _ => e,
        }
    })
}

/// The batch holds more records than it declares: Java's
/// `InvalidRecordException("Incorrect declared batch size, records still
/// remaining in file")` (`DefaultRecordBatch.RecordIterator#next`), naming the
/// partition.
pub(crate) fn records_remaining_error(partition: &TopicPartition) -> Error {
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
pub(crate) fn raw_base_offset(batch_bytes: &[u8]) -> i64 {
    batch_bytes
        .first_chunk::<{ RecordBatch::BASE_OFFSET_LENGTH }>()
        .map_or(-1, |base_offset| i64::from_be_bytes(*base_offset))
}

/// The offsets a batch refused before its view was built covers, for the
/// share fetch to reject: its base offset, and its last offset where the v2
/// header holds the delta, else the base offset again (a v0/v1 batch's offset
/// field, the last offset of a legacy wrapper).
fn refused_batch_range(batch_bytes: &[u8]) -> (i64, i64) {
    let base_offset = raw_base_offset(batch_bytes);
    let magic = batch_bytes.get(RecordBatch::MAGIC_OFFSET).map_or(0, |&m| m as i8);
    let delta_end = RecordBatch::LAST_OFFSET_DELTA_OFFSET + RecordBatch::LAST_OFFSET_DELTA_LENGTH;
    let last_offset = match batch_bytes.get(RecordBatch::LAST_OFFSET_DELTA_OFFSET..delta_end) {
        Some(delta) if magic >= RecordBatch::MAGIC_VALUE_V2 => {
            base_offset.saturating_add(i64::from(i32::from_be_bytes([delta[0], delta[1], delta[2], delta[3]])))
        },
        _ => base_offset,
    };
    (base_offset, last_offset)
}

#[doc(alias = "org.apache.kafka.clients.consumer.internals.CompletedFetch#maybeLeaderEpoch")]
pub(crate) fn maybe_leader_epoch(epoch: i32) -> Option<i32> {
    if epoch == NO_PARTITION_LEADER_EPOCH {
        None
    } else {
        Some(epoch)
    }
}
