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

#![allow(dead_code)]
//! Record batch implementation for magic 2 and above.
//!
//! The schema is:
//!
//! ```text
//! RecordBatch =>
//!   BaseOffset => Int64
//!   Length => Int32
//!   PartitionLeaderEpoch => Int32
//!   Magic => Int8
//!   CRC => Uint32
//!   Attributes => Int16
//!   LastOffsetDelta => Int32
//!   BaseTimestamp => Int64
//!   MaxTimestamp => Int64
//!   ProducerId => Int64
//!   ProducerEpoch => Int16
//!   BaseSequence => Int32
//!   RecordsCount => Int32
//!   Records => [Record]
//! ```
//!
//! Corresponds to Java's `org.apache.kafka.common.record.DefaultRecordBatch`.

use std::io;
use std::io::Read;

use crate::common::InvalidRecordError;
use crate::common::compress;
use crate::common::compress::Compression;
use crate::common::header::internals::RecordHeader;
use crate::common::record::TimestampType;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::DefaultRecord;
use crate::common::record::internal::RecordBatch;
use crate::common::record::internal::SimpleRecord;
use crate::common::record::internal::abstract_records::LOG_OVERHEAD;

/// The most bytes one record batch may decompress to (APPSEC-7665 D4).
///
/// Java has no such bound: it decompresses a batch record by record as the
/// consumer iterates (`DefaultRecordBatch.java:279-297`), so a zip bomb costs it
/// time, not memory. This client decompresses a batch once, into the buffer its
/// records borrow from (`consumer-threading.md` §27), so without a bound a small
/// compressed batch could demand gigabytes. 1 GiB is an order of magnitude above
/// the largest request a broker accepts with its default `socket.request.max.bytes`
/// (`100 * 1024 * 1024`, `SocketServerConfigs.java:96`), so no batch a default
/// broker stored reaches it. A constant rather than a configuration key; promote
/// it to one only if a user hits it.
pub const MAX_DECOMPRESSED_BATCH_BYTES: usize = 1 << 30;

/// The most bytes one `read` asks a decompressor for while a batch is
/// decompressed, and so the furthest the output buffer can grow past
/// [`MAX_DECOMPRESSED_BATCH_BYTES`] (or a lower limit) before the limit is seen.
pub(crate) const DECOMPRESSION_READ_CHUNK_BYTES: usize = 16 * 1024;

// Attribute masks
const COMPRESSION_CODEC_MASK: u8 = 0x07;
const TRANSACTIONAL_FLAG_MASK: u8 = 0x10;
const CONTROL_FLAG_MASK: u8 = 0x20;
const DELETE_HORIZON_FLAG_MASK: u8 = 0x40;
const TIMESTAMP_TYPE_MASK: u8 = 0x08;

/// Record batch implementation for magic 2 and above.
///
/// Wraps a byte buffer containing a complete record batch. The batch header is
/// 61 bytes long, followed by the (possibly compressed) record data.
///
/// Corresponds to Java's `org.apache.kafka.common.record.DefaultRecordBatch`.
#[derive(Clone, Debug)]
pub struct DefaultRecordBatch {
    buffer: Vec<u8>,
}

impl DefaultRecordBatch {
    /// Create a new `DefaultRecordBatch` wrapping the given buffer.
    ///
    /// The buffer must contain exactly one complete v2 record batch starting at
    /// index 0 — at least [`RecordBatch::RECORD_BATCH_OVERHEAD`] bytes, which
    /// [`as_ref`](Self::as_ref)'s header accessors rely on. The only production
    /// source is [`BatchIterator`](crate::common::record::internal::memory_records::BatchIterator),
    /// which copies batches that
    /// [`ByteBufferLogInputStream`](crate::common::record::internal::ByteBufferLogInputStream)
    /// has already validated; the rest are this client's own builders.
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }

    /// Create a `DefaultRecordBatch` from a byte slice (copies the data).
    pub fn from_slice(data: &[u8]) -> Self {
        Self { buffer: data.to_vec() }
    }

    /// Returns a borrowing view over this batch's buffer.
    ///
    /// Lets owned accessors delegate to the shared [`DefaultRecordBatchRef`]
    /// readers (single source of truth for the wire-format field offsets) and
    /// lets the consumer receive path parse a batch header straight out of a
    /// `&[u8]` slice without a per-batch `to_vec` (see `consumer-threading.md`
    /// §27).
    ///
    /// Built directly rather than through the checked
    /// [`DefaultRecordBatchRef::new`]: an owned batch's buffer already satisfies
    /// the view's invariant (see [`new`](Self::new)).
    pub fn as_ref(&self) -> DefaultRecordBatchRef<'_> {
        DefaultRecordBatchRef { buffer: &self.buffer }
    }

    /// Returns the magic byte of this batch.
    pub fn magic(&self) -> i8 {
        self.as_ref().magic()
    }

    /// Validate the record batch, returning an error if corrupt.
    ///
    /// Corresponds to Java's `ensureValid()`.
    pub fn ensure_valid(&self) -> Result<(), InvalidRecordError> {
        self.as_ref().ensure_valid()
    }

    /// Returns the base timestamp of the batch.
    pub fn base_timestamp(&self) -> i64 {
        self.as_ref().base_timestamp()
    }

    /// Returns the max timestamp of the batch.
    pub fn max_timestamp(&self) -> i64 {
        self.as_ref().max_timestamp()
    }

    /// Returns the timestamp type of this batch.
    pub fn timestamp_type(&self) -> TimestampType {
        self.as_ref().timestamp_type()
    }

    /// Returns the base offset of this batch.
    pub fn base_offset(&self) -> i64 {
        self.as_ref().base_offset()
    }

    /// Returns the last offset of this batch.
    pub fn last_offset(&self) -> i64 {
        self.as_ref().last_offset()
    }

    /// Returns the producer ID of this batch.
    pub fn producer_id(&self) -> i64 {
        self.as_ref().producer_id()
    }

    /// Returns the producer epoch of this batch.
    pub fn producer_epoch(&self) -> i16 {
        read_i16(&self.buffer, RecordBatch::PRODUCER_EPOCH_OFFSET)
    }

    /// Returns the base sequence of this batch.
    pub fn base_sequence(&self) -> i32 {
        self.as_ref().base_sequence()
    }

    /// Returns the last offset delta of this batch.
    fn last_offset_delta(&self) -> i32 {
        self.as_ref().last_offset_delta()
    }

    /// Returns the last sequence number of this batch.
    pub fn last_sequence(&self) -> i32 {
        let base_sequence = self.base_sequence();
        if base_sequence == RecordBatch::NO_SEQUENCE {
            RecordBatch::NO_SEQUENCE
        } else {
            increment_sequence(base_sequence, self.last_offset_delta())
        }
    }

    /// Returns the compression type of this batch.
    pub fn compression_type(&self) -> CompressionType {
        self.as_ref().compression_type()
    }

    /// Returns whether this batch uses compression.
    pub fn is_compressed(&self) -> bool {
        self.as_ref().is_compressed()
    }

    /// Whether this batch uses compression, failing for an unknown codec id.
    ///
    /// See [`DefaultRecordBatch::try_compression_type`].
    pub fn try_is_compressed(&self) -> Result<bool, crate::common::Error> {
        self.as_ref().try_is_compressed()
    }

    /// Returns the total size of this batch in bytes (including LOG_OVERHEAD).
    pub fn size_in_bytes(&self) -> usize {
        self.as_ref().size_in_bytes()
    }

    /// Returns the number of records in this batch.
    fn count(&self) -> i32 {
        self.as_ref().records_count()
    }

    /// Returns the record count, or `None` for legacy batches.
    pub fn count_or_null(&self) -> Option<i32> {
        Some(self.count())
    }

    /// Returns whether this batch is transactional.
    pub fn is_transactional(&self) -> bool {
        self.as_ref().is_transactional()
    }

    /// Returns whether the delete horizon flag is set.
    fn has_delete_horizon_ms(&self) -> bool {
        (self.attributes() & DELETE_HORIZON_FLAG_MASK) > 0
    }

    /// Returns the delete horizon timestamp if set, otherwise `None`.
    pub fn delete_horizon_ms(&self) -> Option<i64> {
        if self.has_delete_horizon_ms() {
            Some(read_i64(&self.buffer, RecordBatch::BASE_TIMESTAMP_OFFSET))
        } else {
            None
        }
    }

    /// Returns whether this is a control batch.
    pub fn is_control_batch(&self) -> bool {
        self.as_ref().is_control_batch()
    }

    /// Returns the partition leader epoch.
    pub fn partition_leader_epoch(&self) -> i32 {
        self.as_ref().partition_leader_epoch()
    }

    /// Returns the stored CRC32C checksum.
    pub fn checksum(&self) -> u32 {
        self.as_ref().checksum()
    }

    /// Returns whether the CRC matches the computed value.
    pub fn is_valid(&self) -> bool {
        self.as_ref().is_valid()
    }

    /// Compute the CRC32C over the attributes through the end of the batch.
    fn compute_checksum(&self) -> u32 {
        self.as_ref().compute_checksum()
    }

    /// Returns the attributes byte (lower byte of the 2-byte attributes field).
    fn attributes(&self) -> u8 {
        self.as_ref().attributes()
    }

    /// Write the batch data to a writer.
    pub fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.buffer)
    }

    /// Set the last offset of this batch and recompute the base offset.
    pub fn set_last_offset(&mut self, offset: i64) {
        let last_offset_delta = self.last_offset_delta();
        write_i64(
            &mut self.buffer,
            RecordBatch::BASE_OFFSET_OFFSET,
            offset - last_offset_delta as i64,
        );
    }

    /// Set the max timestamp and timestamp type, then recompute the CRC.
    pub fn set_max_timestamp(&mut self, timestamp_type: TimestampType, max_timestamp: i64) {
        let current_max_timestamp = self.max_timestamp();
        // We don't need to recompute crc if the timestamp is not updated.
        if self.timestamp_type() == timestamp_type && current_max_timestamp == max_timestamp {
            return;
        }

        let attributes = compute_attributes(
            self.compression_type(),
            timestamp_type,
            self.is_transactional(),
            self.is_control_batch(),
            self.has_delete_horizon_ms(),
        );
        write_i16(&mut self.buffer, RecordBatch::ATTRIBUTES_OFFSET, attributes as i16);
        write_i64(&mut self.buffer, RecordBatch::MAX_TIMESTAMP_OFFSET, max_timestamp);
        let crc = self.compute_checksum();
        write_u32(&mut self.buffer, RecordBatch::CRC_OFFSET, crc);
    }

    /// Set the partition leader epoch.
    pub fn set_partition_leader_epoch(&mut self, epoch: i32) {
        write_i32(&mut self.buffer, RecordBatch::PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// The number of records declared in the batch header.
    pub fn records_count(&self) -> i32 {
        self.as_ref().records_count()
    }

    /// The log-append timestamp for the batch, if the batch uses
    /// `LogAppendTime`, else `None`. Used when decoding record timestamps.
    pub fn log_append_time(&self) -> Option<i64> {
        self.as_ref().log_append_time()
    }

    /// The raw, possibly-compressed records section of this batch (the bytes
    /// after the batch header), borrowed from the underlying buffer.
    pub fn records_section(&self) -> &[u8] {
        self.as_ref().records_section()
    }

    /// Decompress this batch's records section into a fresh owned buffer.
    ///
    /// Only valid for compressed batches. The returned `Vec<u8>` is the
    /// decompressed record bytes, suitable for borrowing per-record refs via
    /// [`DefaultRecord::read_ref_from_buffer`]. Per `consumer-threading.md`
    /// §27, this allocation happens once per compressed batch — never per
    /// record.
    ///
    /// Returns an error if the batch is corrupt, decompression fails, or the
    /// records decompress to more than `max_bytes`.
    pub fn decompress_records(&self, max_bytes: usize) -> Result<Vec<u8>, InvalidRecordError> {
        self.as_ref().decompress_records(max_bytes)
    }

    /// Iterate over the records in this batch.
    ///
    /// For uncompressed batches, reads records directly from the buffer.
    /// For compressed batches, decompresses all records into memory first, at
    /// most [`MAX_DECOMPRESSED_BATCH_BYTES`] of them (D4).
    ///
    /// Returns an error if the batch is corrupt.
    pub fn iter_records(&self) -> Result<Vec<DefaultRecord>, InvalidRecordError> {
        self.iter_records_with_limit(MAX_DECOMPRESSED_BATCH_BYTES)
    }

    /// [`iter_records`](Self::iter_records) with the decompression limit as a
    /// parameter, so a test can drive it down to a few bytes.
    fn iter_records_with_limit(&self, max_decompressed_bytes: usize) -> Result<Vec<DefaultRecord>, InvalidRecordError> {
        let num_records = self.count();
        if num_records == 0 {
            return Ok(Vec::new());
        }

        if num_records < 0 {
            return Err(invalid_record_count_error(num_records, self.magic()));
        }

        let log_append_time = if self.timestamp_type() == TimestampType::LogAppendTime {
            Some(self.max_timestamp())
        } else {
            None
        };

        let base_offset = self.base_offset();
        let base_timestamp = self.base_timestamp();
        let base_sequence = self.base_sequence();

        // Java sizes the list by the declared count (`new ArrayList<>(count())`,
        // `DefaultRecordBatch.java:332`), which a header can set to `i32::MAX`.
        // Every record takes at least one byte, so the batch's own record bytes
        // bound the count of an uncompressed batch; for a compressed one they are
        // only a starting capacity, and the list grows past it as records decode.
        let records_data = &self.buffer[RecordBatch::RECORDS_OFFSET..];
        let capacity_hint = (num_records as usize).min(records_data.len());

        if !self.is_compressed() {
            // Read records directly from the buffer
            let mut records = Vec::with_capacity(capacity_hint);
            let mut pos = 0;

            for _ in 0..num_records {
                if pos >= records_data.len() {
                    return Err(InvalidRecordError::new("Incorrect declared batch size, premature EOF reached"));
                }
                let (record, consumed) = DefaultRecord::read_from_buffer(
                    &records_data[pos..],
                    base_offset,
                    base_timestamp,
                    base_sequence,
                    log_append_time,
                )?;
                pos += consumed;
                records.push(record);
            }

            // Validate that we consumed all remaining bytes
            if pos != records_data.len() {
                return Err(InvalidRecordError::new(
                    "Incorrect declared batch size, records still remaining in file",
                ));
            }

            Ok(records)
        } else {
            // Decompress and read records from the stream
            let compression = Compression::of(self.compression_type());
            // D4: the stream may yield `max_decompressed_bytes`; `take` stops it
            // one byte later, so a batch that decompresses to more is visible as
            // an exhausted limit rather than read to its end. `take` enforces the
            // limit for every codec alike, so the limit is not also handed to
            // the reader: a snappy reader that refused a block itself would
            // surface here as a failed record read, indistinguishable from any
            // other. Its blocks stay bounded by what their bytes can encode, and
            // an owned batch is one this client built or already validated.
            let mut reader = compression
                .wrap_for_input(records_data, self.magic())
                .map_err(decompression_error)?
                .take((max_decompressed_bytes as u64).saturating_add(1));
            let limit_exceeded =
                || decompression_error(compress::decompressed_size_limit_error(max_decompressed_bytes));

            let mut records = Vec::with_capacity(capacity_hint);
            for _ in 0..num_records {
                let record = match DefaultRecord::read_from_stream(
                    &mut reader,
                    base_offset,
                    base_timestamp,
                    base_sequence,
                    log_append_time,
                ) {
                    Ok(record) => record,
                    // The stream was cut at the limit: report the limit, not the
                    // record it cut short.
                    Err(_) if reader.limit() == 0 => return Err(limit_exceeded()),
                    Err(e) => return Err(e),
                };
                records.push(record);
            }
            if reader.limit() == 0 {
                return Err(limit_exceeded());
            }

            // Check that no data remains
            let mut check_buf = [0u8; 1];
            match io::Read::read(&mut reader, &mut check_buf) {
                Ok(0) => {}, // EOF, good
                // Java's `ensureNoneRemaining` throws here — `catch (IOException e)
                // { throw new KafkaException("Error checking for remaining bytes
                // after reading batch", e); }` (`DefaultRecordBatch.java:645-652`).
                // Treating the failure as a clean EOF hid a truncated or corrupt
                // decompression stream behind a successful-looking parse. (The class
                // differs: this function's error type is `InvalidRecordError`, also
                // inside the `KafkaException` hierarchy and also non-retriable.)
                Err(e) => {
                    return Err(InvalidRecordError::new(format!(
                        "Error checking for remaining bytes after reading batch: {e}"
                    )));
                },
                Ok(_) => {
                    return Err(InvalidRecordError::new(
                        "Incorrect declared batch size, records still remaining in file",
                    ));
                },
            }

            Ok(records)
        }
    }

    /// Compute the total serialized size of a batch containing the given
    /// simple records.
    ///
    /// Corresponds to Java's `DefaultRecordBatch.sizeInBytes(Iterable<SimpleRecord>)`.
    pub fn size_in_bytes_of_simple_records(records: &[SimpleRecord]) -> usize {
        if records.is_empty() {
            return 0;
        }

        let mut size = RecordBatch::RECORD_BATCH_OVERHEAD;
        let mut base_timestamp: Option<i64> = None;
        for (offset_delta, record) in records.iter().enumerate() {
            if base_timestamp.is_none() {
                base_timestamp = Some(record.timestamp());
            }
            let timestamp_delta = record.timestamp() - base_timestamp.unwrap();
            size += DefaultRecord::size_in_bytes_with_slices(
                offset_delta as i32,
                timestamp_delta,
                record.key(),
                record.value(),
                record.headers(),
            ) as usize;
        }
        size
    }

    /// Get an upper bound estimate on the batch size needed to hold a single
    /// record with the given key, value, and headers.
    ///
    /// This is only an estimate because it does not take into account
    /// overhead from the compression algorithm used.
    pub fn estimate_batch_size_upper_bound(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) -> i32 {
        RecordBatch::RECORD_BATCH_OVERHEAD as i32 + DefaultRecord::record_size_upper_bound(key, value, headers)
    }

    /// Write an empty batch header (no records) to the given buffer.
    ///
    /// Corresponds to Java's `DefaultRecordBatch.writeEmptyHeader`.
    #[allow(clippy::too_many_arguments)]
    pub fn write_empty_header(
        buffer: &mut Vec<u8>,
        magic: i8,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        base_offset: i64,
        last_offset: i64,
        partition_leader_epoch: i32,
        timestamp_type: TimestampType,
        timestamp: i64,
        is_transactional: bool,
        is_control_record: bool,
    ) {
        let offset_delta = (last_offset - base_offset) as i32;
        Self::write_header(
            buffer,
            base_offset,
            offset_delta,
            RecordBatch::RECORD_BATCH_OVERHEAD,
            magic,
            CompressionType::None,
            timestamp_type,
            RecordBatch::NO_TIMESTAMP,
            timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            is_control_record,
            false,
            partition_leader_epoch,
            0,
        );
    }

    /// Write a batch header to the given buffer at its current length.
    ///
    /// The buffer is expected to contain the full batch data (header + records),
    /// and this method writes/overwrites the header portion at `position`.
    ///
    /// Corresponds to Java's `DefaultRecordBatch.writeHeader`.
    #[allow(clippy::too_many_arguments)]
    pub fn write_header(
        buffer: &mut Vec<u8>,
        base_offset: i64,
        last_offset_delta: i32,
        size_in_bytes: usize,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_timestamp: i64,
        max_timestamp: i64,
        producer_id: i64,
        epoch: i16,
        sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        is_delete_horizon_set: bool,
        partition_leader_epoch: i32,
        num_records: i32,
    ) {
        assert!(magic >= RecordBatch::CURRENT_MAGIC_VALUE, "Invalid magic value {}", magic);
        assert!(
            base_timestamp >= 0 || base_timestamp == RecordBatch::NO_TIMESTAMP,
            "Invalid message timestamp {}",
            base_timestamp
        );

        let attributes = compute_attributes(
            compression_type,
            timestamp_type,
            is_transactional,
            is_control_batch,
            is_delete_horizon_set,
        );

        // Ensure buffer has at least RECORD_BATCH_OVERHEAD bytes
        if buffer.len() < RecordBatch::RECORD_BATCH_OVERHEAD {
            buffer.resize(RecordBatch::RECORD_BATCH_OVERHEAD, 0);
        }

        write_i64(buffer, RecordBatch::BASE_OFFSET_OFFSET, base_offset);
        write_i32(buffer, RecordBatch::LENGTH_OFFSET, (size_in_bytes - LOG_OVERHEAD) as i32);
        write_i32(buffer, RecordBatch::PARTITION_LEADER_EPOCH_OFFSET, partition_leader_epoch);
        buffer[RecordBatch::MAGIC_OFFSET] = magic as u8;
        write_i16(buffer, RecordBatch::ATTRIBUTES_OFFSET, attributes as i16);
        write_i64(buffer, RecordBatch::BASE_TIMESTAMP_OFFSET, base_timestamp);
        write_i64(buffer, RecordBatch::MAX_TIMESTAMP_OFFSET, max_timestamp);
        write_i32(buffer, RecordBatch::LAST_OFFSET_DELTA_OFFSET, last_offset_delta);
        write_i64(buffer, RecordBatch::PRODUCER_ID_OFFSET, producer_id);
        write_i16(buffer, RecordBatch::PRODUCER_EPOCH_OFFSET, epoch);
        write_i32(buffer, RecordBatch::BASE_SEQUENCE_OFFSET, sequence);
        write_i32(buffer, RecordBatch::RECORDS_COUNT_OFFSET, num_records);

        // Compute CRC over attributes through end of buffer
        let crc = crc32c::crc32c(&buffer[RecordBatch::ATTRIBUTES_OFFSET..]);
        write_u32(buffer, RecordBatch::CRC_OFFSET, crc);
    }

    /// Write a batch header to the given buffer at a specific position.
    ///
    /// Used by `MemoryRecordsBuilder` to write the header after records have been appended.
    ///
    /// The buffer should already be sized to hold the full batch.
    #[allow(clippy::too_many_arguments)]
    pub fn write_header_at(
        buffer: &mut [u8],
        position: usize,
        base_offset: i64,
        last_offset_delta: i32,
        size_in_bytes: usize,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_timestamp: i64,
        max_timestamp: i64,
        producer_id: i64,
        epoch: i16,
        sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        is_delete_horizon_set: bool,
        partition_leader_epoch: i32,
        num_records: i32,
    ) {
        assert!(magic >= RecordBatch::CURRENT_MAGIC_VALUE, "Invalid magic value {}", magic);
        assert!(
            base_timestamp >= 0 || base_timestamp == RecordBatch::NO_TIMESTAMP,
            "Invalid message timestamp {}",
            base_timestamp
        );

        let attributes = compute_attributes(
            compression_type,
            timestamp_type,
            is_transactional,
            is_control_batch,
            is_delete_horizon_set,
        );

        // Write all header fields at position
        write_i64(&mut buffer[position..], RecordBatch::BASE_OFFSET_OFFSET, base_offset);
        write_i32(
            &mut buffer[position..],
            RecordBatch::LENGTH_OFFSET,
            (size_in_bytes - LOG_OVERHEAD) as i32,
        );
        write_i32(
            &mut buffer[position..],
            RecordBatch::PARTITION_LEADER_EPOCH_OFFSET,
            partition_leader_epoch,
        );
        buffer[position + RecordBatch::MAGIC_OFFSET] = magic as u8;
        write_i16(&mut buffer[position..], RecordBatch::ATTRIBUTES_OFFSET, attributes as i16);
        write_i64(&mut buffer[position..], RecordBatch::BASE_TIMESTAMP_OFFSET, base_timestamp);
        write_i64(&mut buffer[position..], RecordBatch::MAX_TIMESTAMP_OFFSET, max_timestamp);
        write_i32(
            &mut buffer[position..],
            RecordBatch::LAST_OFFSET_DELTA_OFFSET,
            last_offset_delta,
        );
        write_i64(&mut buffer[position..], RecordBatch::PRODUCER_ID_OFFSET, producer_id);
        write_i16(&mut buffer[position..], RecordBatch::PRODUCER_EPOCH_OFFSET, epoch);
        write_i32(&mut buffer[position..], RecordBatch::BASE_SEQUENCE_OFFSET, sequence);
        write_i32(&mut buffer[position..], RecordBatch::RECORDS_COUNT_OFFSET, num_records);

        // Compute CRC over from attributes offset to the end of this batch
        let crc_start = position + RecordBatch::ATTRIBUTES_OFFSET;
        let crc_end = position + size_in_bytes;
        let crc = crc32c::crc32c(&buffer[crc_start..crc_end]);
        write_u32(&mut buffer[position..], RecordBatch::CRC_OFFSET, crc);
    }
}

/// A borrowing view of a single v2+ record batch header, reading its fields
/// directly out of a `&[u8]` slice without owning the batch's bytes.
///
/// This is the zero-copy analog of [`DefaultRecordBatch`]: it exposes the same
/// read-only header accessors (which are pure offset reads into the buffer) but
/// holds only a borrow. The consumer receive path uses it so that locating and
/// parsing the next batch in a fetch response does NOT require copying the
/// batch's bytes into an owned `Vec` per batch (`consumer-threading.md` §27).
/// [`DefaultRecordBatch`]'s own accessors delegate here to avoid duplicating
/// the wire-format offset constants.
///
/// The slice holds exactly one batch: it begins at the batch's `BASE_OFFSET`
/// and ends at the batch's last byte, the shape of Java's
/// `batchSlice.limit(batchSize)` (`ByteBufferLogInputStream.java:51`). The CRC
/// and the records section are bounded by the slice, as Java bounds them by
/// `buffer.limit()` (`DefaultRecordBatch.java:273-277`, `:399-401`), so they
/// never reach into a following batch. The slice is also at least
/// [`RecordBatch::RECORD_BATCH_OVERHEAD`] bytes long, which
/// [`new`](Self::new) checks, so every fixed-offset header accessor is in
/// bounds. [`ByteBufferLogInputStream`](crate::common::record::internal::ByteBufferLogInputStream)
/// is what sizes the slice from a header it has validated.
#[derive(Clone, Copy, Debug)]
pub struct DefaultRecordBatchRef<'a> {
    buffer: &'a [u8],
}

impl<'a> DefaultRecordBatchRef<'a> {
    /// Wraps a slice holding exactly one v2 record batch.
    ///
    /// # Errors
    ///
    /// Returns Java's `ensureValid()` size-check error
    /// (`DefaultRecordBatch.java:152-154`) when the slice is shorter than
    /// [`RecordBatch::RECORD_BATCH_OVERHEAD`]. For a slice holding exactly one
    /// batch its length is the batch's size, so the message is the one Java
    /// prints. Java builds such a batch anyway and fails at the first header
    /// read past its end (an `IndexOutOfBoundsException`); in Rust that read
    /// would be a slice panic, so the view is refused instead.
    pub fn new(buffer: &'a [u8]) -> Result<Self, InvalidRecordError> {
        if buffer.len() < RecordBatch::RECORD_BATCH_OVERHEAD {
            return Err(batch_size_below_overhead_error(buffer.len() as i64));
        }
        Ok(Self { buffer })
    }

    /// The batch's bytes: exactly one batch, from its `BASE_OFFSET` to its last byte.
    pub fn buffer(&self) -> &'a [u8] {
        self.buffer
    }

    /// Returns the magic byte of this batch.
    pub fn magic(&self) -> i8 {
        self.buffer[RecordBatch::MAGIC_OFFSET] as i8
    }

    /// Returns the base timestamp of the batch.
    pub fn base_timestamp(&self) -> i64 {
        read_i64(self.buffer, RecordBatch::BASE_TIMESTAMP_OFFSET)
    }

    /// Returns the max timestamp of the batch.
    pub fn max_timestamp(&self) -> i64 {
        read_i64(self.buffer, RecordBatch::MAX_TIMESTAMP_OFFSET)
    }

    /// Returns the timestamp type of this batch.
    pub fn timestamp_type(&self) -> TimestampType {
        if (self.attributes() & TIMESTAMP_TYPE_MASK) == 0 {
            TimestampType::CreateTime
        } else {
            TimestampType::LogAppendTime
        }
    }

    /// Returns the base offset of this batch.
    pub fn base_offset(&self) -> i64 {
        read_i64(self.buffer, RecordBatch::BASE_OFFSET_OFFSET)
    }

    /// Returns the last offset delta of this batch.
    fn last_offset_delta(&self) -> i32 {
        read_i32(self.buffer, RecordBatch::LAST_OFFSET_DELTA_OFFSET)
    }

    /// Returns the last offset of this batch.
    pub fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }

    /// Returns the producer ID of this batch.
    pub fn producer_id(&self) -> i64 {
        read_i64(self.buffer, RecordBatch::PRODUCER_ID_OFFSET)
    }

    /// Returns the base sequence of this batch.
    pub fn base_sequence(&self) -> i32 {
        read_i32(self.buffer, RecordBatch::BASE_SEQUENCE_OFFSET)
    }

    /// Returns the compression type of this batch, treating a codec id this
    /// client does not know as [`CompressionType::None`].
    ///
    /// Use [`try_compression_type`](Self::try_compression_type) for a batch that
    /// came off the wire — see there for why the difference matters.
    pub fn compression_type(&self) -> CompressionType {
        CompressionType::for_id(self.attributes() & COMPRESSION_CODEC_MASK).unwrap_or(CompressionType::None)
    }

    /// Returns the compression type of this batch, failing for a codec id this
    /// client does not know — Java's behaviour.
    ///
    /// `COMPRESSION_CODEC_MASK` is `0x07`, so ids 5-7 are wire-reachable and
    /// CRC-valid. `CompressionType.forId` throws `IllegalArgumentException` for
    /// them (`CompressionType.java:144-159`) and
    /// `DefaultRecordBatch.compressionType()` lets it propagate
    /// (`DefaultRecordBatch.java:217-219`). Reporting `None` instead would mark
    /// such a batch *uncompressed* and hand its still-compressed bytes to the
    /// record parser, so a consumer would either skip the batch or surface
    /// fabricated records.
    ///
    /// `IllegalArgumentException` is not a `KafkaException`, so in Java it escapes
    /// `FetchCollector`'s swallow guard and reaches the application — which the
    /// `Error::local_illegal_argument` returned by [`CompressionType::for_id`]
    /// reproduces.
    pub fn try_compression_type(&self) -> Result<CompressionType, crate::common::Error> {
        CompressionType::for_id(self.attributes() & COMPRESSION_CODEC_MASK)
    }

    /// Whether this batch uses compression, failing for an unknown codec id.
    ///
    /// See [`try_compression_type`](Self::try_compression_type).
    pub fn try_is_compressed(&self) -> Result<bool, crate::common::Error> {
        Ok(self.try_compression_type()? != CompressionType::None)
    }

    /// Returns whether this batch uses compression.
    pub fn is_compressed(&self) -> bool {
        self.compression_type() != CompressionType::None
    }

    /// Returns the total size of this batch in bytes (including LOG_OVERHEAD).
    ///
    /// Java's `sizeInBytes()` (`DefaultRecordBatch.java:222-225`) reads the
    /// header's length field. A view sized by `ByteBufferLogInputStream` carries
    /// a length of at least 49 (a 61-byte header less `LOG_OVERHEAD`), and an
    /// owned [`DefaultRecordBatch`] holds a buffer this client built or already
    /// validated, so the field is never negative. The conversion is checked
    /// anyway: a corrupt length reads as 0 — which every size check treats as too
    /// small — instead of wrapping to a size near `usize::MAX`.
    pub fn size_in_bytes(&self) -> usize {
        usize::try_from(self.declared_size_in_bytes()).unwrap_or(0)
    }

    /// Java's `sizeInBytes()` arithmetic (`LOG_OVERHEAD + length`) widened to
    /// `i64`, so a corrupt length can neither overflow nor wrap.
    /// [`ensure_valid`](Self::ensure_valid) and [`is_valid`](Self::is_valid)
    /// compare this signed value, as Java compares its `int`.
    fn declared_size_in_bytes(&self) -> i64 {
        LOG_OVERHEAD as i64 + i64::from(read_i32(self.buffer, RecordBatch::LENGTH_OFFSET))
    }

    /// Returns the number of records declared in the batch header.
    pub fn records_count(&self) -> i32 {
        read_i32(self.buffer, RecordBatch::RECORDS_COUNT_OFFSET)
    }

    /// Returns whether this batch is transactional.
    pub fn is_transactional(&self) -> bool {
        (self.attributes() & TRANSACTIONAL_FLAG_MASK) > 0
    }

    /// Returns whether this is a control batch.
    pub fn is_control_batch(&self) -> bool {
        (self.attributes() & CONTROL_FLAG_MASK) > 0
    }

    /// Returns the partition leader epoch.
    pub fn partition_leader_epoch(&self) -> i32 {
        read_i32(self.buffer, RecordBatch::PARTITION_LEADER_EPOCH_OFFSET)
    }

    /// Returns the stored CRC32C checksum.
    pub fn checksum(&self) -> u32 {
        read_u32(self.buffer, RecordBatch::CRC_OFFSET)
    }

    /// Compute the CRC32C over the attributes through the end of the batch.
    ///
    /// Java's `computeChecksum()` covers `ATTRIBUTES_OFFSET` to `buffer.limit()`
    /// (`DefaultRecordBatch.java:399-401`); the slice ends where the batch does.
    fn compute_checksum(&self) -> u32 {
        crc32c::crc32c(&self.buffer[RecordBatch::ATTRIBUTES_OFFSET..])
    }

    /// Returns whether the CRC matches the computed value.
    pub fn is_valid(&self) -> bool {
        self.declared_size_in_bytes() >= RecordBatch::RECORD_BATCH_OVERHEAD as i64
            && self.checksum() == self.compute_checksum()
    }

    /// Validate the record batch, returning an error if corrupt.
    ///
    /// Corresponds to Java's `ensureValid()`.
    pub fn ensure_valid(&self) -> Result<(), InvalidRecordError> {
        let size_in_bytes = self.declared_size_in_bytes();
        if size_in_bytes < RecordBatch::RECORD_BATCH_OVERHEAD as i64 {
            return Err(batch_size_below_overhead_error(size_in_bytes));
        }

        if !self.is_valid() {
            return Err(InvalidRecordError::new(format!(
                "Record is corrupt (stored crc = {}, computed crc = {})",
                self.checksum(),
                self.compute_checksum()
            )));
        }

        Ok(())
    }

    /// Returns the attributes byte (lower byte of the 2-byte attributes field).
    fn attributes(&self) -> u8 {
        read_i16(self.buffer, RecordBatch::ATTRIBUTES_OFFSET) as u8
    }

    /// The log-append timestamp for the batch, if the batch uses
    /// `LogAppendTime`, else `None`. Used when decoding record timestamps.
    pub fn log_append_time(&self) -> Option<i64> {
        if self.timestamp_type() == TimestampType::LogAppendTime {
            Some(self.max_timestamp())
        } else {
            None
        }
    }

    /// The raw, possibly-compressed records section of this batch (the bytes
    /// after the batch header), borrowed from the underlying buffer.
    ///
    /// Java positions a duplicate of the batch buffer at `RECORDS_OFFSET` and
    /// reads to its limit (`DefaultRecordBatch.java:273-277`, `:299-301`); the
    /// slice ends where the batch does.
    pub fn records_section(&self) -> &'a [u8] {
        &self.buffer[RecordBatch::RECORDS_OFFSET..]
    }

    /// Decompress this batch's records section into a fresh owned buffer.
    ///
    /// Only valid for compressed batches. The returned `Vec<u8>` is the
    /// decompressed record bytes, suitable for borrowing per-record refs via
    /// [`DefaultRecord::read_ref_from_buffer`]. Per `consumer-threading.md`
    /// §27, this allocation happens once per compressed batch — never per
    /// record.
    ///
    /// At most `max_bytes` are decompressed (D4): the buffer's capacity never
    /// exceeds `max_bytes + DECOMPRESSION_READ_CHUNK_BYTES`, and the first byte
    /// past `max_bytes` fails the batch — see [`read_decompressed`]. The consumer
    /// passes `FetchConfig::max_decompressed_batch_bytes`, which is
    /// [`MAX_DECOMPRESSED_BATCH_BYTES`] outside tests.
    ///
    /// Returns an error if the batch is corrupt, decompression fails, or the
    /// records decompress to more than `max_bytes`.
    pub fn decompress_records(&self, max_bytes: usize) -> Result<Vec<u8>, InvalidRecordError> {
        let records_data = self.records_section();
        let compression = Compression::of(self.compression_type());
        let mut reader = compression
            .wrap_for_input_with_limit(records_data, self.magic(), max_bytes)
            .map_err(decompression_error)?;
        read_decompressed(&mut reader, records_data.len(), max_bytes).map_err(decompression_error)
    }
}

impl std::fmt::Display for DefaultRecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordBatch(magic={}, offsets=[{}, {}], sequence=[{}, {}], \
             partitionLeaderEpoch={}, isTransactional={}, isControlBatch={}, \
             compression={}, timestampType={}, crc={})",
            self.magic(),
            self.base_offset(),
            self.last_offset(),
            self.base_sequence(),
            self.last_sequence(),
            self.partition_leader_epoch(),
            self.is_transactional(),
            self.is_control_batch(),
            self.compression_type(),
            self.timestamp_type(),
            self.checksum(),
        )
    }
}

impl PartialEq for DefaultRecordBatch {
    fn eq(&self, other: &Self) -> bool {
        self.buffer == other.buffer
    }
}

impl Eq for DefaultRecordBatch {}

impl std::hash::Hash for DefaultRecordBatch {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.buffer.hash(state);
    }
}

/// Compute the attributes byte for a record batch header.
fn compute_attributes(
    compression_type: CompressionType,
    timestamp_type: TimestampType,
    is_transactional: bool,
    is_control: bool,
    is_delete_horizon_set: bool,
) -> u8 {
    assert!(
        timestamp_type != TimestampType::NoTimestampType,
        "Timestamp type must be provided to compute attributes for message format v2 and above"
    );

    let mut attributes: u8 = if is_transactional { TRANSACTIONAL_FLAG_MASK } else { 0 };
    if is_control {
        attributes |= CONTROL_FLAG_MASK;
    }
    let id = compression_type.id();
    if id > 0 {
        attributes |= COMPRESSION_CODEC_MASK & id;
    }
    if timestamp_type == TimestampType::LogAppendTime {
        attributes |= TIMESTAMP_TYPE_MASK;
    }
    if is_delete_horizon_set {
        attributes |= DELETE_HORIZON_FLAG_MASK;
    }
    attributes
}

/// Increment a sequence number, wrapping around at `i32::MAX`.
///
/// Corresponds to Java's `DefaultRecordBatch.incrementSequence(int, int)`.
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Decrement a sequence number, wrapping around at 0.
///
/// Corresponds to Java's `DefaultRecordBatch.decrementSequence(int, int)`.
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    if sequence < decrement {
        i32::MAX - (decrement - sequence) + 1
    } else {
        sequence - decrement
    }
}

/// Reads `reader` to its end into one buffer that never grows past `max_bytes`
/// by more than one read (D4).
///
/// Replaces `read_to_end`, whose doubling has no ceiling. The capacity starts at
/// `size_hint` (the compressed size) and doubles, but is clamped to
/// `max_bytes + DECOMPRESSION_READ_CHUNK_BYTES`, and each `read` is offered at
/// most one chunk, so no capacity above that ceiling is ever requested; the
/// first byte past `max_bytes` is an error. Every reservation is
/// `try_reserve_exact`, so an allocation the process cannot satisfy is an error
/// rather than an abort, and each byte of spare capacity is zeroed once.
fn read_decompressed<R: Read>(reader: &mut R, size_hint: usize, max_bytes: usize) -> io::Result<Vec<u8>> {
    let ceiling = max_bytes.saturating_add(DECOMPRESSION_READ_CHUNK_BYTES);
    let mut decompressed: Vec<u8> = Vec::new();
    // `decompressed[..filled]` is output; `decompressed[filled..]` is zeroed
    // space not yet read into.
    let mut filled = 0;
    loop {
        if filled == decompressed.len() {
            if decompressed.len() == decompressed.capacity() {
                // `filled <= max_bytes < ceiling`, so the target is always past
                // the current length.
                let target = decompressed.capacity().saturating_mul(2).max(size_hint).max(1).min(ceiling);
                decompressed
                    .try_reserve_exact(target - decompressed.len())
                    .map_err(io::Error::other)?;
            }
            let zeroed = decompressed.capacity().min(filled + DECOMPRESSION_READ_CHUNK_BYTES);
            decompressed.resize(zeroed, 0);
        }
        match reader.read(&mut decompressed[filled..]) {
            Ok(0) => break,
            Ok(read) => {
                filled += read;
                if filled > max_bytes {
                    return Err(compress::decompressed_size_limit_error(max_bytes));
                }
            },
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {},
            Err(e) => return Err(e),
        }
    }
    decompressed.truncate(filled);
    Ok(decompressed)
}

/// A decompression failure, in the message the batch reader has always used.
fn decompression_error(e: io::Error) -> InvalidRecordError {
    InvalidRecordError::new(format!("Failed to decompress record stream: {e}"))
}

/// Java's `ensureValid()` size-check error (`DefaultRecordBatch.java:152-154`),
/// for a batch whose size — `LOG_OVERHEAD` plus its length field — is below
/// the v2 header.
fn batch_size_below_overhead_error(size_in_bytes: i64) -> InvalidRecordError {
    InvalidRecordError::new(format!(
        "Record batch is corrupt (the size {size_in_bytes} is smaller than the minimum allowed overhead {})",
        RecordBatch::RECORD_BATCH_OVERHEAD
    ))
}

/// The error Java's `RecordIterator` constructor throws for a negative record
/// count (`DefaultRecordBatch.java:584-587`), before a single record is read.
///
/// Shared by [`DefaultRecordBatch::iter_records`] and the consumer's receive
/// path, which iterates a batch through its own cursor instead of a
/// `RecordIterator` but must reject the count at the same point.
pub(crate) fn invalid_record_count_error(num_records: i32, magic: i8) -> InvalidRecordError {
    InvalidRecordError::new(format!("Found invalid record count {num_records} in magic v{magic} batch"))
}

// -- Big-endian read/write helpers --

fn read_i64(buf: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap())
}

fn read_i32(buf: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn read_i16(buf: &[u8], offset: usize) -> i16 {
    i16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

fn write_i64(buf: &mut [u8], offset: usize, value: i64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn write_i32(buf: &mut [u8], offset: usize, value: i32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn write_i16(buf: &mut [u8], offset: usize, value: i16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::internal::MemoryRecords;
    use crate::common::record::internal::Record;
    use crate::common::record::internal::SimpleRecord;

    #[test]
    fn test_increment_sequence() {
        assert_eq!(increment_sequence(5, 5), 10);
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX - 5, 10), 4);
    }

    #[test]
    fn test_decrement_sequence() {
        assert_eq!(decrement_sequence(5, 5), 0);
        assert_eq!(decrement_sequence(0, 1), i32::MAX);
    }

    #[test]
    fn test_compute_attributes() {
        let attrs = compute_attributes(CompressionType::None, TimestampType::CreateTime, false, false, false);
        assert_eq!(attrs, 0);

        let attrs = compute_attributes(CompressionType::Gzip, TimestampType::CreateTime, false, false, false);
        assert_eq!(attrs & COMPRESSION_CODEC_MASK, 1);

        let attrs = compute_attributes(CompressionType::None, TimestampType::LogAppendTime, false, false, false);
        assert_ne!(attrs & TIMESTAMP_TYPE_MASK, 0);

        let attrs = compute_attributes(CompressionType::None, TimestampType::CreateTime, true, false, false);
        assert_ne!(attrs & TRANSACTIONAL_FLAG_MASK, 0);

        let attrs = compute_attributes(CompressionType::None, TimestampType::CreateTime, false, true, false);
        assert_ne!(attrs & CONTROL_FLAG_MASK, 0);

        let attrs = compute_attributes(CompressionType::None, TimestampType::CreateTime, false, false, true);
        assert_ne!(attrs & DELETE_HORIZON_FLAG_MASK, 0);
    }

    #[test]
    #[should_panic(expected = "Timestamp type must be provided")]
    fn test_compute_attributes_no_timestamp_type() {
        compute_attributes(CompressionType::None, TimestampType::NoTimestampType, false, false, false);
    }

    // -- Tests translated from DefaultRecordBatchTest.java --

    /// Corresponds to Java's `DefaultRecordBatchTest.testWriteEmptyHeader`.
    #[test]
    fn test_write_empty_header() {
        let producer_id = 23423_i64;
        let producer_epoch = 145_i16;
        let base_sequence = 983_i32;
        let base_offset = 15_i64;
        let last_offset = 37_i64;
        let partition_leader_epoch = 15_i32;
        let timestamp = 1_700_000_000_000_i64;

        for timestamp_type in &[TimestampType::CreateTime, TimestampType::LogAppendTime] {
            for &is_transactional in &[true, false] {
                for &is_control_batch in &[true, false] {
                    let mut buffer = Vec::with_capacity(2048);
                    DefaultRecordBatch::write_empty_header(
                        &mut buffer,
                        RecordBatch::CURRENT_MAGIC_VALUE,
                        producer_id,
                        producer_epoch,
                        base_sequence,
                        base_offset,
                        last_offset,
                        partition_leader_epoch,
                        *timestamp_type,
                        timestamp,
                        is_transactional,
                        is_control_batch,
                    );
                    let batch = DefaultRecordBatch::new(buffer);
                    assert_eq!(producer_id, batch.producer_id());
                    assert_eq!(producer_epoch, batch.producer_epoch());
                    assert_eq!(base_sequence, batch.base_sequence());
                    assert_eq!(base_sequence + ((last_offset - base_offset) as i32), batch.last_sequence());
                    assert_eq!(base_offset, batch.base_offset());
                    assert_eq!(last_offset, batch.last_offset());
                    assert_eq!(partition_leader_epoch, batch.partition_leader_epoch());
                    assert_eq!(is_transactional, batch.is_transactional());
                    assert_eq!(*timestamp_type, batch.timestamp_type());
                    assert_eq!(timestamp, batch.max_timestamp());
                    assert_eq!(RecordBatch::NO_TIMESTAMP, batch.base_timestamp());
                    assert_eq!(is_control_batch, batch.is_control_batch());
                }
            }
        }
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.buildDefaultRecordBatch`.
    #[test]
    fn test_build_default_record_batch() {
        let mut builder = MemoryRecords::builder_with_magic(
            2048,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            1234567,
        );
        builder.append_with_offset_bytes(1234567, 1, Some(b"a"), Some(b"v"));
        builder.append_with_offset_bytes(1234568, 2, Some(b"b"), Some(b"v"));

        let records = builder.build();
        for batch in records.batches() {
            assert!(batch.is_valid());
            assert_eq!(1234567, batch.base_offset());
            assert_eq!(1234568, batch.last_offset());
            assert_eq!(2, batch.max_timestamp());
            assert_eq!(RecordBatch::NO_PRODUCER_ID, batch.producer_id());
            assert_eq!(RecordBatch::NO_PRODUCER_EPOCH, batch.producer_epoch());
            assert_eq!(RecordBatch::NO_SEQUENCE, batch.base_sequence());
            assert_eq!(RecordBatch::NO_SEQUENCE, batch.last_sequence());

            for record in batch.iter_records().unwrap() {
                record.ensure_valid().unwrap();
            }
        }
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.buildDefaultRecordBatchWithProducerId`.
    #[test]
    fn test_build_default_record_batch_with_producer_id() {
        let pid = 23423_i64;
        let epoch = 145_i16;
        let base_sequence = 983_i32;

        let mut builder = MemoryRecords::builder_with_producer(
            2048,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            1234567,
            RecordBatch::NO_TIMESTAMP,
            pid,
            epoch,
            base_sequence,
        );
        builder.append_with_offset_bytes(1234567, 1, Some(b"a"), Some(b"v"));
        builder.append_with_offset_bytes(1234568, 2, Some(b"b"), Some(b"v"));

        let records = builder.build();
        for batch in records.batches() {
            assert!(batch.is_valid());
            assert_eq!(1234567, batch.base_offset());
            assert_eq!(1234568, batch.last_offset());
            assert_eq!(2, batch.max_timestamp());
            assert_eq!(pid, batch.producer_id());
            assert_eq!(epoch, batch.producer_epoch());
            assert_eq!(base_sequence, batch.base_sequence());
            assert_eq!(base_sequence + 1, batch.last_sequence());

            for record in batch.iter_records().unwrap() {
                record.ensure_valid().unwrap();
            }
        }
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.buildDefaultRecordBatchWithSequenceWrapAround`.
    #[test]
    fn test_build_default_record_batch_with_sequence_wrap_around() {
        let pid = 23423_i64;
        let epoch = 145_i16;
        let base_sequence = i32::MAX - 1;

        let mut builder = MemoryRecords::builder_with_producer(
            2048,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            1234567,
            RecordBatch::NO_TIMESTAMP,
            pid,
            epoch,
            base_sequence,
        );
        builder.append_with_offset_bytes(1234567, 1, Some(b"a"), Some(b"v"));
        builder.append_with_offset_bytes(1234568, 2, Some(b"b"), Some(b"v"));
        builder.append_with_offset_bytes(1234569, 3, Some(b"c"), Some(b"v"));

        let records = builder.build();
        let batches: Vec<_> = records.batches().collect();
        assert_eq!(1, batches.len());

        let batch = &batches[0];
        assert_eq!(pid, batch.producer_id());
        assert_eq!(epoch, batch.producer_epoch());
        assert_eq!(base_sequence, batch.base_sequence());
        assert_eq!(0, batch.last_sequence());

        let all_records = batch.iter_records().unwrap();
        assert_eq!(3, all_records.len());
        assert_eq!(i32::MAX - 1, all_records[0].sequence());
        assert_eq!(i32::MAX, all_records[1].sequence());
        assert_eq!(0, all_records[2].sequence());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testSizeInBytes`.
    #[test]
    fn test_size_in_bytes() {
        use crate::common::header::internals::RecordHeader;

        let headers = vec![
            RecordHeader::new("foo".to_string(), Some(b"value".to_vec())),
            RecordHeader::new("bar".to_string(), None),
        ];

        let timestamp = 1_700_000_000_000_i64;
        let records = vec![
            SimpleRecord::new_with_key_value(timestamp, Some(b"key".to_vec()), Some(b"value".to_vec())),
            SimpleRecord::new_with_key_value(timestamp + 30000, None, Some(b"value".to_vec())),
            SimpleRecord::new_with_key_value(timestamp + 60000, Some(b"key".to_vec()), None),
            SimpleRecord::new(timestamp + 60000, Some(b"key".to_vec()), Some(b"value".to_vec()), headers),
        ];
        let actual_size = MemoryRecords::with_records(Compression::none(), &records).size_in_bytes();
        assert_eq!(actual_size, DefaultRecordBatch::size_in_bytes_of_simple_records(&records));
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testInvalidRecordSize`.
    #[test]
    fn test_invalid_record_size() {
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &[
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            ],
        );

        let mut buf = records.buffer().to_vec();
        // Corrupt the length field
        buf[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&10_i32.to_be_bytes());

        let batch = DefaultRecordBatch::new(buf);
        assert!(!batch.is_valid());
        assert!(batch.ensure_valid().is_err());
    }

    /// Helper to create records with an invalid record count.
    fn records_with_invalid_record_count(
        timestamp: i64,
        compression_type: CompressionType,
        invalid_count: i32,
    ) -> DefaultRecordBatch {
        let compression = Compression::of(compression_type);
        let mut builder = MemoryRecords::builder_with_magic(
            512,
            RecordBatch::MAGIC_VALUE_V2,
            compression,
            TimestampType::CreateTime,
            0,
        );
        builder.append_with_offset_bytes(0, timestamp, None, Some(b"hello"));
        builder.append_with_offset_bytes(1, timestamp, None, Some(b"there"));
        builder.append_with_offset_bytes(2, timestamp, None, Some(b"beautiful"));
        let records = builder.build();

        let mut buf = records.buffer().to_vec();
        // Overwrite the records count
        buf[RecordBatch::RECORDS_COUNT_OFFSET..RecordBatch::RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&invalid_count.to_be_bytes());
        DefaultRecordBatch::new(buf)
    }

    /// Java's `ensureNoneRemaining` turns a failure of the "is there anything
    /// left?" read into an error — `catch (IOException e) { throw new
    /// KafkaException("Error checking for remaining bytes after reading batch",
    /// e); }` (`DefaultRecordBatch.java:645-652`).
    ///
    /// Truncating the gzip trailer leaves every record decodable but makes the
    /// end-of-stream read fail, which is the only input that reaches that arm.
    /// Before this behaviour was restored the arm was `Ok(0) | Err(_) => {}`, so
    /// this batch parsed clean and handed the caller three records from a
    /// provably corrupt stream.
    #[test]
    fn test_corrupt_compressed_stream_fails_the_remaining_bytes_check() {
        let now = 1_700_000_000_000_i64;
        let mut builder = MemoryRecords::builder_with_magic(
            512,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::of(CompressionType::Gzip),
            TimestampType::CreateTime,
            0,
        );
        builder.append_with_offset_bytes(0, now, None, Some(b"hello"));
        builder.append_with_offset_bytes(1, now, None, Some(b"there"));
        builder.append_with_offset_bytes(2, now, None, Some(b"beautiful"));
        let records = builder.build();

        // The batch is well-formed and parses cleanly as built.
        let intact = DefaultRecordBatch::new(records.buffer().to_vec());
        assert_eq!(intact.iter_records().expect("the intact batch parses").len(), 3);

        // Drop 4 of the 8 gzip trailer bytes: the deflate stream still yields all
        // three records, but the read that checks for leftovers hits EOF inside
        // the trailer and fails.
        let mut buf = records.buffer().to_vec();
        buf.truncate(buf.len() - 4);
        let batch = DefaultRecordBatch::new(buf);

        let err = batch.iter_records().expect_err("a corrupt stream must not parse clean");
        assert_eq!(
            err.message(),
            "Error checking for remaining bytes after reading batch: unexpected end of file"
        );
    }

    /// A codec id this client does not know must fail with Java's
    /// `IllegalArgumentException` message (`CompressionType.java:144-159`,
    /// propagated by `DefaultRecordBatch.compressionType()` at
    /// `DefaultRecordBatch.java:217-219`). `try_compression_type` returns
    /// [`CompressionType::for_id`]'s error unchanged, so the message is the one
    /// place both spellings must agree.
    #[test]
    fn test_try_compression_type_rejects_an_unknown_codec_id() {
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &[SimpleRecord::new_with_key_value(
                1,
                Some(b"a".to_vec()),
                Some(b"1".to_vec()),
            )],
        );
        let mut buf = records.buffer().to_vec();
        // Codec ids 5-7 fit `COMPRESSION_CODEC_MASK` (0x07), so they are
        // wire-reachable; write 5 into the attributes field.
        buf[RecordBatch::ATTRIBUTES_OFFSET..RecordBatch::ATTRIBUTES_OFFSET + 2].copy_from_slice(&5_i16.to_be_bytes());
        let batch = DefaultRecordBatch::new(buf);

        let err = batch.as_ref().try_compression_type().expect_err("codec id 5 is unknown");
        assert_eq!(err.message(), "Unknown compression type id: 5");
        // Java throws `IllegalArgumentException`, which is not a `KafkaException`.
        assert!(matches!(err, crate::common::Error::LocalIllegalArgument(_)));
        assert!(!err.is_kafka_error());

        let err = batch.try_is_compressed().expect_err("codec id 5 is unknown");
        assert_eq!(err.message(), "Unknown compression type id: 5");

        // The lenient accessor still reports `None`, as its own doc says.
        assert_eq!(batch.compression_type(), CompressionType::None);
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testInvalidRecordCountTooManyNonCompressedV2`.
    #[test]
    fn test_invalid_record_count_too_many_non_compressed_v2() {
        let now = 1_700_000_000_000_i64;
        let batch = records_with_invalid_record_count(now, CompressionType::None, 5);
        let result = batch.iter_records();
        assert!(result.is_err());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testInvalidRecordCountTooLittleNonCompressedV2`.
    #[test]
    fn test_invalid_record_count_too_little_non_compressed_v2() {
        let now = 1_700_000_000_000_i64;
        let batch = records_with_invalid_record_count(now, CompressionType::None, 2);
        let result = batch.iter_records();
        assert!(result.is_err());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testInvalidRecordCountTooManyCompressedV2`.
    #[test]
    fn test_invalid_record_count_too_many_compressed_v2() {
        let now = 1_700_000_000_000_i64;
        let batch = records_with_invalid_record_count(now, CompressionType::Gzip, 5);
        let result = batch.iter_records();
        assert!(result.is_err());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testInvalidRecordCountTooLittleCompressedV2`.
    #[test]
    fn test_invalid_record_count_too_little_compressed_v2() {
        let now = 1_700_000_000_000_i64;
        let batch = records_with_invalid_record_count(now, CompressionType::Gzip, 2);
        let result = batch.iter_records();
        assert!(result.is_err());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testInvalidCrc`.
    #[test]
    fn test_invalid_crc() {
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &[
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            ],
        );

        let mut buf = records.buffer().to_vec();
        // Corrupt the last_offset_delta field (will invalidate CRC)
        buf[RecordBatch::LAST_OFFSET_DELTA_OFFSET..RecordBatch::LAST_OFFSET_DELTA_OFFSET + 4]
            .copy_from_slice(&23_i32.to_be_bytes());

        let batch = DefaultRecordBatch::new(buf);
        assert!(!batch.is_valid());
        assert!(batch.ensure_valid().is_err());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testSetLastOffset`.
    #[test]
    fn test_set_last_offset() {
        let simple_records = vec![
            SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &simple_records,
        );

        let last_offset = 500_i64;
        let first_offset = last_offset - simple_records.len() as i64 + 1;

        let buf = records.buffer().to_vec();
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_last_offset(last_offset);
        assert_eq!(last_offset, batch.last_offset());
        assert_eq!(first_offset, batch.base_offset());
        assert!(batch.is_valid());

        // Also verify via MemoryRecords iteration
        let records2 = MemoryRecords::new(batch.buffer().to_vec().into());
        let batches: Vec<_> = records2.batches().collect();
        assert_eq!(1, batches.len());
        assert_eq!(last_offset, batches[0].last_offset());

        for (offset, record) in (first_offset..).zip(records2.records()) {
            assert_eq!(offset, record.offset());
        }
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testSetPartitionLeaderEpoch`.
    #[test]
    fn test_set_partition_leader_epoch() {
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &[
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            ],
        );

        let leader_epoch = 500;

        let buf = records.buffer().to_vec();
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_partition_leader_epoch(leader_epoch);
        assert_eq!(leader_epoch, batch.partition_leader_epoch());
        assert!(batch.is_valid());

        let records2 = MemoryRecords::new(batch.buffer().to_vec().into());
        let batches: Vec<_> = records2.batches().collect();
        assert_eq!(1, batches.len());
        assert_eq!(leader_epoch, batches[0].partition_leader_epoch());
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testSetLogAppendTime`.
    #[test]
    fn test_set_log_append_time() {
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &[
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            ],
        );

        let log_append_time = 15_i64;

        let buf = records.buffer().to_vec();
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_max_timestamp(TimestampType::LogAppendTime, log_append_time);
        assert_eq!(TimestampType::LogAppendTime, batch.timestamp_type());
        assert_eq!(log_append_time, batch.max_timestamp());
        assert!(batch.is_valid());

        let records2 = MemoryRecords::new(batch.buffer().to_vec().into());
        let batches: Vec<_> = records2.batches().collect();
        assert_eq!(1, batches.len());
        assert_eq!(log_append_time, batches[0].max_timestamp());
        assert_eq!(TimestampType::LogAppendTime, batches[0].timestamp_type());

        // When timestamp type is LogAppendTime, all records should have the log append time
        for record in records2.records() {
            assert_eq!(log_append_time, record.timestamp());
        }
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testSetNoTimestampTypeNotAllowed`.
    #[test]
    #[should_panic(expected = "Timestamp type must be provided")]
    fn test_set_no_timestamp_type_not_allowed() {
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none(),
            TimestampType::CreateTime,
            &[
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            ],
        );
        let buf = records.buffer().to_vec();
        let mut batch = DefaultRecordBatch::new(buf);
        batch.set_max_timestamp(TimestampType::NoTimestampType, RecordBatch::NO_TIMESTAMP);
    }

    /// Corresponds to Java's `DefaultRecordBatchTest.testStreamingIteratorConsistency`.
    ///
    /// For each compression type, verifies that iterating records produces consistent results.
    #[test]
    fn test_streaming_iterator_consistency() {
        for compression_type in &[
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let compression = Compression::of(*compression_type);
            let records = MemoryRecords::with_records_at_offset(
                RecordBatch::MAGIC_VALUE_V2,
                0,
                compression,
                TimestampType::CreateTime,
                &[
                    SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                    SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                    SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
                ],
            );
            let batch = DefaultRecordBatch::from_slice(records.buffer());
            let iter_records = batch
                .iter_records()
                .unwrap_or_else(|e| panic!("Failed for {:?}: {}", compression_type, e));
            assert_eq!(3, iter_records.len(), "Failed for {:?}", compression_type);

            // Verify offsets and keys
            assert_eq!(0, iter_records[0].offset());
            assert_eq!(Some(b"a".as_slice()), iter_records[0].key());
            assert_eq!(1, iter_records[1].offset());
            assert_eq!(Some(b"b".as_slice()), iter_records[1].key());
            assert_eq!(2, iter_records[2].offset());
            assert_eq!(Some(b"c".as_slice()), iter_records[2].key());
        }
    }

    // Note: testReadAndWriteControlBatch is skipped because it requires EndTransactionMarker
    // and ControlRecordType which are not yet implemented.

    // Note: testSkipKeyValueIteratorCorrectness, testBufferReuseInSkipKeyValueIterator,
    // and testZstdJniForSkipKeyValueIterator are skipped because they test internal
    // Java-specific iterator types (StreamRecordIterator, RecordIterator) and
    // BufferSupplier, which are not applicable to the Rust implementation.

    // ── APPSEC-7665 D4: bounded decompression, bounded record lists ─────────

    const COMPRESSED_TYPES: [CompressionType; 4] = [
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ];

    /// One batch of 20 small records under `compression_type`.
    fn compressed_batch(compression_type: CompressionType) -> DefaultRecordBatch {
        let records: Vec<SimpleRecord> = (0..20)
            .map(|i| {
                SimpleRecord::new_with_key_value(
                    i,
                    Some(format!("key-{i}").into_bytes()),
                    Some(format!("value-{i}").into_bytes()),
                )
            })
            .collect();
        let records = MemoryRecords::with_records_at_offset(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::of(compression_type),
            TimestampType::CreateTime,
            &records,
        );
        DefaultRecordBatch::from_slice(records.buffer())
    }

    /// A reader that never ends, as a decompression bomb does not.
    struct Endless;
    impl io::Read for Endless {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            buf.fill(0);
            Ok(buf.len())
        }
    }

    /// The growth policy on its own: an endless stream is cut at the limit,
    /// and no allocation larger than the limit plus one read chunk is ever
    /// requested — `read_to_end` would have doubled without a ceiling.
    #[test]
    fn test_read_decompressed_never_grows_past_the_limit_and_a_chunk() {
        for (size_hint, max_bytes) in [(0, 1000), (10, 1000), (100_000, 1000), (64, 200_000)] {
            let (err, max_allocation) = {
                let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
                let err = read_decompressed(&mut Endless, size_hint, max_bytes).expect_err("an endless stream");
                (err, crate::test_alloc_tracker::AllocTrackingGuard::max_allocation())
            };
            assert_eq!(
                format!("decompressed size exceeds the limit of {max_bytes} bytes per record batch"),
                err.to_string()
            );
            assert!(
                max_allocation <= max_bytes + DECOMPRESSION_READ_CHUNK_BYTES,
                "hint {size_hint}, limit {max_bytes}: allocated {max_allocation} bytes"
            );
        }
    }

    /// Every codec: a batch decompresses at exactly its size and fails one byte
    /// under it, with the message naming the limit, and the returned buffer's
    /// capacity stays within the limit plus one read chunk.
    #[test]
    fn test_decompress_records_is_bounded_for_every_codec() {
        for compression_type in COMPRESSED_TYPES {
            let batch = compressed_batch(compression_type);
            let size = batch.decompress_records(usize::MAX).expect("an intact batch").len();

            let decompressed = batch.decompress_records(size).expect("exactly at the limit");
            assert_eq!(size, decompressed.len(), "{compression_type:?}");
            assert!(
                decompressed.capacity() <= size + DECOMPRESSION_READ_CHUNK_BYTES,
                "{compression_type:?}: capacity {} for {size} bytes",
                decompressed.capacity()
            );

            let err = batch.decompress_records(size - 1).expect_err("one byte over the limit");
            assert_eq!(
                format!(
                    "Failed to decompress record stream: decompressed size exceeds the limit of {} bytes per record \
                     batch",
                    size - 1
                ),
                err.message(),
                "{compression_type:?}"
            );
        }
    }

    /// The owned path (`iter_records`) applies the same limit through `take`.
    #[test]
    fn test_iter_records_decompression_is_bounded_for_every_codec() {
        for compression_type in COMPRESSED_TYPES {
            let batch = compressed_batch(compression_type);
            let size = batch.decompress_records(usize::MAX).expect("an intact batch").len();

            assert_eq!(20, batch.iter_records_with_limit(size).expect("exactly at the limit").len());
            let err = batch.iter_records_with_limit(size - 1).expect_err("one byte over the limit");
            assert_eq!(
                format!(
                    "Failed to decompress record stream: decompressed size exceeds the limit of {} bytes per record \
                     batch",
                    size - 1
                ),
                err.message(),
                "{compression_type:?}"
            );
        }
    }

    /// A declared count of `i32::MAX` no longer sizes the record list
    /// (`Vec::with_capacity(count)` would abort on capacity overflow): the batch
    /// fails with Java's premature-EOF text once its real records run out.
    #[test]
    fn test_iter_records_with_i32_max_record_count_errors_instead_of_aborting() {
        let uncompressed = records_with_invalid_record_count(1_700_000_000_000, CompressionType::None, i32::MAX);
        let err = uncompressed.iter_records().expect_err("3 records, not i32::MAX");
        assert_eq!("Incorrect declared batch size, premature EOF reached", err.message());

        let compressed = records_with_invalid_record_count(1_700_000_000_000, CompressionType::Gzip, i32::MAX);
        let err = compressed.iter_records().expect_err("3 records, not i32::MAX");
        assert_eq!("Failed to read record size: failed to fill whole buffer", err.message());
    }
}
