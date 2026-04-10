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

//! RecordBatch implementation for magic v2 and above.
//!
//! Translated from `org.apache.kafka.common.record.DefaultRecordBatch`.
//!
//! RecordBatch schema:
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
//! The CRC covers data from attributes to the end of the batch, using CRC-32C.

use crate::common::record::compression_type::CompressionType;
use crate::common::record::default_record::DefaultRecord;
use crate::common::record::timestamp_type::TimestampType;
use crate::common::record::{
    LOG_OVERHEAD, NO_SEQUENCE, NO_TIMESTAMP, RecordHeader, SimpleRecord, corrupt_record_error, invalid_record_error,
};
use crate::errors::Result;

// ============================================================================
// Field offsets within the batch buffer
// ============================================================================

/// Offset of BaseOffset field.
pub const BASE_OFFSET_OFFSET: usize = 0;
/// Length of BaseOffset field.
pub const BASE_OFFSET_LENGTH: usize = 8;
/// Offset of Length field.
pub const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
/// Length of Length field.
pub const LENGTH_LENGTH: usize = 4;
/// Offset of PartitionLeaderEpoch field.
pub const PARTITION_LEADER_EPOCH_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
/// Length of PartitionLeaderEpoch field.
pub const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;
/// Offset of Magic field.
pub const MAGIC_OFFSET: usize = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
/// Length of Magic field.
pub const MAGIC_LENGTH: usize = 1;
/// Offset of CRC field.
pub const CRC_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
/// Length of CRC field.
pub const CRC_LENGTH: usize = 4;
/// Offset of Attributes field.
pub const ATTRIBUTES_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
/// Length of Attributes field.
pub const ATTRIBUTE_LENGTH: usize = 2;
/// Offset of LastOffsetDelta field.
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
/// Length of LastOffsetDelta field.
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
/// Offset of BaseTimestamp field.
pub const BASE_TIMESTAMP_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
/// Length of BaseTimestamp field.
pub const BASE_TIMESTAMP_LENGTH: usize = 8;
/// Offset of MaxTimestamp field.
pub const MAX_TIMESTAMP_OFFSET: usize = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
/// Length of MaxTimestamp field.
pub const MAX_TIMESTAMP_LENGTH: usize = 8;
/// Offset of ProducerId field.
pub const PRODUCER_ID_OFFSET: usize = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
/// Length of ProducerId field.
pub const PRODUCER_ID_LENGTH: usize = 8;
/// Offset of ProducerEpoch field.
pub const PRODUCER_EPOCH_OFFSET: usize = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
/// Length of ProducerEpoch field.
pub const PRODUCER_EPOCH_LENGTH: usize = 2;
/// Offset of BaseSequence field.
pub const BASE_SEQUENCE_OFFSET: usize = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
/// Length of BaseSequence field.
pub const BASE_SEQUENCE_LENGTH: usize = 4;
/// Offset of RecordsCount field.
pub const RECORDS_COUNT_OFFSET: usize = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
/// Length of RecordsCount field.
pub const RECORDS_COUNT_LENGTH: usize = 4;
/// Offset where records data begins.
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
/// Total overhead of the record batch header (before individual records).
pub const RECORD_BATCH_OVERHEAD: usize = RECORDS_OFFSET;

// Attribute masks
const COMPRESSION_CODEC_MASK: u8 = 0x07;
const TRANSACTIONAL_FLAG_MASK: u8 = 0x10;
const CONTROL_FLAG_MASK: u8 = 0x20;
const DELETE_HORIZON_FLAG_MASK: u8 = 0x40;
const TIMESTAMP_TYPE_MASK: u8 = 0x08;

/// A record batch in magic v2 format, backed by a byte buffer.
///
/// This is a read-only view over a serialized batch. For building batches,
/// use [`super::MemoryRecordsBuilder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultRecordBatch {
    buffer: Vec<u8>,
}

impl DefaultRecordBatch {
    /// Create a `DefaultRecordBatch` from a byte buffer.
    pub fn new(buffer: Vec<u8>) -> Self {
        DefaultRecordBatch { buffer }
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Returns the magic byte.
    pub fn magic(&self) -> i8 {
        self.buffer[MAGIC_OFFSET] as i8
    }

    /// Validate the batch, raising an error if corrupt.
    ///
    /// # Errors
    /// Returns `CorruptRecord` if the CRC doesn't match or the batch is too small.
    pub fn ensure_valid(&self) -> Result<()> {
        if self.size_in_bytes() < RECORD_BATCH_OVERHEAD as i32 {
            return Err(corrupt_record_error(format!(
                "Record batch is corrupt (the size {} is smaller than the minimum allowed overhead {})",
                self.size_in_bytes(),
                RECORD_BATCH_OVERHEAD
            )));
        }
        if !self.is_valid() {
            return Err(corrupt_record_error(format!(
                "Record is corrupt (stored crc = {}, computed crc = {})",
                self.checksum(),
                self.compute_checksum()
            )));
        }
        Ok(())
    }

    /// Returns the base timestamp of the batch.
    pub fn base_timestamp(&self) -> i64 {
        read_i64(&self.buffer, BASE_TIMESTAMP_OFFSET)
    }

    /// Returns the max timestamp of the batch.
    pub fn max_timestamp(&self) -> i64 {
        read_i64(&self.buffer, MAX_TIMESTAMP_OFFSET)
    }

    /// Returns the timestamp type.
    pub fn timestamp_type(&self) -> TimestampType {
        if (self.attributes() & TIMESTAMP_TYPE_MASK) == 0 {
            TimestampType::CreateTime
        } else {
            TimestampType::LogAppendTime
        }
    }

    /// Returns the base offset.
    pub fn base_offset(&self) -> i64 {
        read_i64(&self.buffer, BASE_OFFSET_OFFSET)
    }

    /// Returns the last offset (base_offset + last_offset_delta).
    pub fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }

    /// Returns the producer ID.
    pub fn producer_id(&self) -> i64 {
        read_i64(&self.buffer, PRODUCER_ID_OFFSET)
    }

    /// Returns the producer epoch.
    pub fn producer_epoch(&self) -> i16 {
        read_i16(&self.buffer, PRODUCER_EPOCH_OFFSET)
    }

    /// Returns the base sequence number.
    pub fn base_sequence(&self) -> i32 {
        read_i32(&self.buffer, BASE_SEQUENCE_OFFSET)
    }

    /// Returns the last offset delta.
    fn last_offset_delta(&self) -> i32 {
        read_i32(&self.buffer, LAST_OFFSET_DELTA_OFFSET)
    }

    /// Returns the last sequence number.
    pub fn last_sequence(&self) -> i32 {
        let base_seq = self.base_sequence();
        if base_seq == NO_SEQUENCE {
            NO_SEQUENCE
        } else {
            increment_sequence(base_seq, self.last_offset_delta())
        }
    }

    /// Returns the compression type.
    pub fn compression_type(&self) -> CompressionType {
        CompressionType::for_id((self.attributes() & COMPRESSION_CODEC_MASK) as i32).unwrap_or(CompressionType::None)
    }

    /// Returns the total size in bytes of this batch (LOG_OVERHEAD + length field value).
    pub fn size_in_bytes(&self) -> i32 {
        LOG_OVERHEAD as i32 + read_i32(&self.buffer, LENGTH_OFFSET)
    }

    /// Returns the number of records in the batch.
    pub fn count(&self) -> i32 {
        read_i32(&self.buffer, RECORDS_COUNT_OFFSET)
    }

    /// Returns the record count, always Some for v2.
    pub fn count_or_null(&self) -> Option<i32> {
        Some(self.count())
    }

    /// Returns whether the batch is compressed.
    pub fn is_compressed(&self) -> bool {
        self.compression_type() != CompressionType::None
    }

    /// Returns whether the batch is transactional.
    pub fn is_transactional(&self) -> bool {
        (self.attributes() & TRANSACTIONAL_FLAG_MASK) != 0
    }

    /// Returns whether the delete horizon flag is set.
    fn has_delete_horizon_ms(&self) -> bool {
        (self.attributes() & DELETE_HORIZON_FLAG_MASK) != 0
    }

    /// Returns the delete horizon timestamp if set.
    pub fn delete_horizon_ms(&self) -> Option<i64> {
        if self.has_delete_horizon_ms() {
            Some(read_i64(&self.buffer, BASE_TIMESTAMP_OFFSET))
        } else {
            None
        }
    }

    /// Returns whether this is a control batch.
    pub fn is_control_batch(&self) -> bool {
        (self.attributes() & CONTROL_FLAG_MASK) != 0
    }

    /// Returns the partition leader epoch.
    pub fn partition_leader_epoch(&self) -> i32 {
        read_i32(&self.buffer, PARTITION_LEADER_EPOCH_OFFSET)
    }

    /// Returns the stored CRC-32C checksum as an unsigned 32-bit value.
    pub fn checksum(&self) -> u32 {
        read_u32(&self.buffer, CRC_OFFSET)
    }

    /// Returns whether the checksum matches the computed value.
    pub fn is_valid(&self) -> bool {
        self.size_in_bytes() >= RECORD_BATCH_OVERHEAD as i32 && self.checksum() == self.compute_checksum()
    }

    /// Compute the CRC-32C over the region from attributes to end of batch.
    fn compute_checksum(&self) -> u32 {
        crc32c::crc32c(&self.buffer[ATTRIBUTES_OFFSET..])
    }

    /// Returns the attributes byte (low byte of the 16-bit attributes field).
    ///
    /// Java reads `(byte) buffer.getShort(ATTRIBUTES_OFFSET)` which gets the
    /// big-endian i16 and truncates to the low byte.
    fn attributes(&self) -> u8 {
        read_i16(&self.buffer, ATTRIBUTES_OFFSET) as u8
    }

    /// Write the batch data to a destination buffer.
    pub fn write_to(&self, dest: &mut Vec<u8>) {
        dest.extend_from_slice(&self.buffer);
    }

    /// Set the last offset by adjusting the base offset.
    pub fn set_last_offset(&mut self, offset: i64) {
        let new_base = offset - self.last_offset_delta() as i64;
        write_i64(&mut self.buffer, BASE_OFFSET_OFFSET, new_base);
    }

    /// Set the max timestamp and timestamp type, recomputing CRC.
    pub fn set_max_timestamp(&mut self, timestamp_type: TimestampType, max_timestamp: i64) {
        let current_max = self.max_timestamp();
        let current_type = self.timestamp_type();
        if current_type == timestamp_type && current_max == max_timestamp {
            return;
        }

        let attrs = compute_attributes(
            self.compression_type(),
            timestamp_type,
            self.is_transactional(),
            self.is_control_batch(),
            self.has_delete_horizon_ms(),
        );
        write_i16(&mut self.buffer, ATTRIBUTES_OFFSET, attrs as i16);
        write_i64(&mut self.buffer, MAX_TIMESTAMP_OFFSET, max_timestamp);
        let crc = crc32c::crc32c(&self.buffer[ATTRIBUTES_OFFSET..]);
        write_u32(&mut self.buffer, CRC_OFFSET, crc);
    }

    /// Set the partition leader epoch (does not affect CRC).
    pub fn set_partition_leader_epoch(&mut self, epoch: i32) {
        write_i32(&mut self.buffer, PARTITION_LEADER_EPOCH_OFFSET, epoch);
    }

    /// Iterate over the records in this uncompressed batch.
    ///
    /// # Errors
    /// Returns an error if any record is malformed.
    pub fn iter_records(&self) -> Result<Vec<DefaultRecord>> {
        let count = self.count();
        if count == 0 {
            return Ok(Vec::new());
        }
        if count < 0 {
            return Err(invalid_record_error(format!(
                "Found invalid record count {} in magic v{} batch",
                count,
                self.magic()
            )));
        }

        let log_append_time = if self.timestamp_type() == TimestampType::LogAppendTime {
            Some(self.max_timestamp())
        } else {
            None
        };

        let base_offset = self.base_offset();
        let base_timestamp = self.base_timestamp();
        let base_sequence = self.base_sequence();

        let mut pos = RECORDS_OFFSET;
        let mut records = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let record = DefaultRecord::read_from(
                &self.buffer,
                &mut pos,
                base_offset,
                base_timestamp,
                base_sequence,
                log_append_time,
            )?;
            records.push(record);
        }

        // Verify we consumed all data
        if pos != self.buffer.len() {
            return Err(invalid_record_error(
                "Incorrect declared batch size, records still remaining in file",
            ));
        }

        Ok(records)
    }

    // ========================================================================
    // Static methods
    // ========================================================================

    /// Write an empty batch header (used for producer state preservation after compaction).
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
            RECORD_BATCH_OVERHEAD as i32,
            magic,
            CompressionType::None,
            timestamp_type,
            NO_TIMESTAMP,
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

    /// Write a full batch header.
    #[allow(clippy::too_many_arguments)]
    pub fn write_header(
        buffer: &mut Vec<u8>,
        base_offset: i64,
        last_offset_delta: i32,
        size_in_bytes: i32,
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
        debug_assert!(magic >= super::CURRENT_MAGIC_VALUE, "Invalid magic value {}", magic);

        let attributes = compute_attributes(
            compression_type,
            timestamp_type,
            is_transactional,
            is_control_batch,
            is_delete_horizon_set,
        );

        let position = buffer.len();
        // Reserve space for the entire header
        buffer.resize(position + RECORD_BATCH_OVERHEAD, 0);
        let buf = &mut buffer[position..];

        write_i64(buf, BASE_OFFSET_OFFSET, base_offset);
        write_i32(buf, LENGTH_OFFSET, size_in_bytes - LOG_OVERHEAD as i32);
        write_i32(buf, PARTITION_LEADER_EPOCH_OFFSET, partition_leader_epoch);
        buf[MAGIC_OFFSET] = magic as u8;
        write_i16(buf, ATTRIBUTES_OFFSET, attributes as i16);
        write_i64(buf, BASE_TIMESTAMP_OFFSET, base_timestamp);
        write_i64(buf, MAX_TIMESTAMP_OFFSET, max_timestamp);
        write_i32(buf, LAST_OFFSET_DELTA_OFFSET, last_offset_delta);
        write_i64(buf, PRODUCER_ID_OFFSET, producer_id);
        write_i16(buf, PRODUCER_EPOCH_OFFSET, epoch);
        write_i32(buf, BASE_SEQUENCE_OFFSET, sequence);
        write_i32(buf, RECORDS_COUNT_OFFSET, num_records);

        // CRC covers from attributes to the end. For a header-only write,
        // the CRC covers attributes to end of our header portion.
        // But in the full batch, CRC covers attributes..end of batch.
        // For now, we compute over the buffer from attributes to the end.
        let crc = crc32c::crc32c(&buf[ATTRIBUTES_OFFSET..]);
        write_u32(buf, CRC_OFFSET, crc);
    }
}

/// Write a full batch header into an existing slice (in-place).
///
/// The slice must be large enough to hold at least `RECORD_BATCH_OVERHEAD` bytes.
/// The CRC is computed over the entire slice from `ATTRIBUTES_OFFSET` to the end,
/// so the slice should contain the header followed by the record data.
///
/// This is used by [`super::MemoryRecordsBuilder`] which pre-allocates the header
/// space and writes records after it, then fills in the header in-place.
#[allow(clippy::too_many_arguments)]
pub fn write_header_to_slice(
    buf: &mut [u8],
    base_offset: i64,
    last_offset_delta: i32,
    size_in_bytes: i32,
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
    debug_assert!(magic >= super::CURRENT_MAGIC_VALUE, "Invalid magic value {}", magic);
    debug_assert!(buf.len() >= RECORD_BATCH_OVERHEAD, "Buffer too small for batch header");

    let attributes = compute_attributes(
        compression_type,
        timestamp_type,
        is_transactional,
        is_control_batch,
        is_delete_horizon_set,
    );

    write_i64(buf, BASE_OFFSET_OFFSET, base_offset);
    write_i32(buf, LENGTH_OFFSET, size_in_bytes - LOG_OVERHEAD as i32);
    write_i32(buf, PARTITION_LEADER_EPOCH_OFFSET, partition_leader_epoch);
    buf[MAGIC_OFFSET] = magic as u8;
    write_i16(buf, ATTRIBUTES_OFFSET, attributes as i16);
    write_i64(buf, BASE_TIMESTAMP_OFFSET, base_timestamp);
    write_i64(buf, MAX_TIMESTAMP_OFFSET, max_timestamp);
    write_i32(buf, LAST_OFFSET_DELTA_OFFSET, last_offset_delta);
    write_i64(buf, PRODUCER_ID_OFFSET, producer_id);
    write_i16(buf, PRODUCER_EPOCH_OFFSET, epoch);
    write_i32(buf, BASE_SEQUENCE_OFFSET, sequence);
    write_i32(buf, RECORDS_COUNT_OFFSET, num_records);

    // CRC covers from attributes to the end of the full buffer
    let crc = crc32c::crc32c(&buf[ATTRIBUTES_OFFSET..]);
    write_u32(buf, CRC_OFFSET, crc);
}

impl DefaultRecordBatch {
    /// Calculate the total size in bytes for a batch of simple records.
    pub fn size_in_bytes_for_simple_records(records: &[SimpleRecord]) -> i32 {
        if records.is_empty() {
            return 0;
        }

        let mut size = RECORD_BATCH_OVERHEAD as i32;
        let base_timestamp = records[0].timestamp;
        for (i, record) in records.iter().enumerate() {
            let offset_delta = i as i32;
            let timestamp_delta = record.timestamp - base_timestamp;
            size += DefaultRecord::size_in_bytes_with_slices(
                offset_delta,
                timestamp_delta,
                record.key.as_deref(),
                record.value.as_deref(),
                &record.headers,
            );
        }
        size
    }

    /// Get an upper bound on the size of a batch with a single record.
    pub fn estimate_batch_size_upper_bound(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> usize {
        RECORD_BATCH_OVERHEAD + DefaultRecord::record_size_upper_bound(key, value, headers)
    }

    /// Increment a sequence number with wrap-around at `i32::MAX`.
    pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
        if sequence > i32::MAX - increment {
            increment - (i32::MAX - sequence) - 1
        } else {
            sequence + increment
        }
    }

    /// Decrement a sequence number with wrap-around.
    pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
        if sequence < decrement {
            i32::MAX - (decrement - sequence) + 1
        } else {
            sequence - decrement
        }
    }
}

/// Increment a sequence number with wrap-around at `i32::MAX`.
///
/// Module-level re-export for use by [`super::default_record::DefaultRecord`].
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    DefaultRecordBatch::increment_sequence(sequence, increment)
}

/// Compute the attributes byte for a batch header.
fn compute_attributes(
    compression_type: CompressionType,
    timestamp_type: TimestampType,
    is_transactional: bool,
    is_control: bool,
    is_delete_horizon_set: bool,
) -> u8 {
    assert_ne!(
        timestamp_type,
        TimestampType::NoTimestampType,
        "Timestamp type must be provided to compute attributes for message format v2 and above"
    );

    let mut attributes: u8 = if is_transactional { TRANSACTIONAL_FLAG_MASK } else { 0 };
    if is_control {
        attributes |= CONTROL_FLAG_MASK;
    }
    if compression_type.id() > 0 {
        attributes |= COMPRESSION_CODEC_MASK & compression_type.id();
    }
    if timestamp_type == TimestampType::LogAppendTime {
        attributes |= TIMESTAMP_TYPE_MASK;
    }
    if is_delete_horizon_set {
        attributes |= DELETE_HORIZON_FLAG_MASK;
    }
    attributes
}

// ============================================================================
// Big-endian read/write helpers
// ============================================================================

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
    use crate::common::record::{
        CURRENT_MAGIC_VALUE, MemoryRecords, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, RecordHeader,
        SimpleRecord,
    };

    /// Translated from DefaultRecordBatchTest.testWriteEmptyHeader
    #[test]
    fn test_write_empty_header() {
        let producer_id: i64 = 23423;
        let producer_epoch: i16 = 145;
        let base_sequence: i32 = 983;
        let base_offset: i64 = 15;
        let last_offset: i64 = 37;
        let partition_leader_epoch: i32 = 15;
        let timestamp: i64 = 1000000;

        for timestamp_type in &[TimestampType::CreateTime, TimestampType::LogAppendTime] {
            for is_transactional in &[true, false] {
                for is_control_batch in &[true, false] {
                    let mut buffer = Vec::with_capacity(2048);
                    DefaultRecordBatch::write_empty_header(
                        &mut buffer,
                        CURRENT_MAGIC_VALUE,
                        producer_id,
                        producer_epoch,
                        base_sequence,
                        base_offset,
                        last_offset,
                        partition_leader_epoch,
                        *timestamp_type,
                        timestamp,
                        *is_transactional,
                        *is_control_batch,
                    );

                    let batch = DefaultRecordBatch::new(buffer);
                    assert_eq!(producer_id, batch.producer_id());
                    assert_eq!(producer_epoch, batch.producer_epoch());
                    assert_eq!(base_sequence, batch.base_sequence());
                    assert_eq!(base_sequence + (last_offset - base_offset) as i32, batch.last_sequence());
                    assert_eq!(base_offset, batch.base_offset());
                    assert_eq!(last_offset, batch.last_offset());
                    assert_eq!(partition_leader_epoch, batch.partition_leader_epoch());
                    assert_eq!(*is_transactional, batch.is_transactional());
                    assert_eq!(*timestamp_type, batch.timestamp_type());
                    assert_eq!(timestamp, batch.max_timestamp());
                    assert_eq!(NO_TIMESTAMP, batch.base_timestamp());
                    assert_eq!(*is_control_batch, batch.is_control_batch());
                }
            }
        }
    }

    /// Translated from DefaultRecordBatchTest.buildDefaultRecordBatch
    #[test]
    fn test_build_default_record_batch() {
        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"v".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"v".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            1234567,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        for batch in mem_records.batches() {
            assert!(batch.is_valid());
            assert_eq!(1234567, batch.base_offset());
            assert_eq!(1234568, batch.last_offset());
            assert_eq!(2, batch.max_timestamp());
            assert_eq!(NO_PRODUCER_ID, batch.producer_id());
            assert_eq!(NO_PRODUCER_EPOCH, batch.producer_epoch());
            assert_eq!(NO_SEQUENCE, batch.base_sequence());
            assert_eq!(NO_SEQUENCE, batch.last_sequence());

            for record in batch.iter_records().unwrap() {
                record.ensure_valid();
            }
        }
    }

    /// Translated from DefaultRecordBatchTest.buildDefaultRecordBatchWithProducerId
    #[test]
    fn test_build_default_record_batch_with_producer_id() {
        let pid: i64 = 23423;
        let epoch: i16 = 145;
        let base_sequence: i32 = 983;

        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"v".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"v".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            1234567,
            NO_TIMESTAMP,
            pid,
            epoch,
            base_sequence,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        for batch in mem_records.batches() {
            assert!(batch.is_valid());
            assert_eq!(1234567, batch.base_offset());
            assert_eq!(1234568, batch.last_offset());
            assert_eq!(2, batch.max_timestamp());
            assert_eq!(pid, batch.producer_id());
            assert_eq!(epoch, batch.producer_epoch());
            assert_eq!(base_sequence, batch.base_sequence());
            assert_eq!(base_sequence + 1, batch.last_sequence());

            for record in batch.iter_records().unwrap() {
                record.ensure_valid();
            }
        }
    }

    /// Translated from DefaultRecordBatchTest.buildDefaultRecordBatchWithSequenceWrapAround
    #[test]
    fn test_build_default_record_batch_with_sequence_wrap_around() {
        let pid: i64 = 23423;
        let epoch: i16 = 145;
        let base_sequence: i32 = i32::MAX - 1;

        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"v".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"v".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"v".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            1234567,
            NO_TIMESTAMP,
            pid,
            epoch,
            base_sequence,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        let batches = mem_records.batches();
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

    /// Translated from DefaultRecordBatchTest.testSizeInBytes
    #[test]
    fn test_size_in_bytes() {
        let headers = vec![
            RecordHeader::new("foo", Some(b"value".to_vec())),
            RecordHeader::new("bar", None),
        ];

        let timestamp: i64 = 1000000;
        let records = vec![
            SimpleRecord::with_timestamp(timestamp, Some(b"key".to_vec()), Some(b"value".to_vec())),
            SimpleRecord::with_timestamp(timestamp + 30000, None, Some(b"value".to_vec())),
            SimpleRecord::with_timestamp(timestamp + 60000, Some(b"key".to_vec()), None),
            SimpleRecord::new(timestamp + 60000, Some(b"key".to_vec()), Some(b"value".to_vec()), headers),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        let actual_size = mem_records.size_in_bytes();
        let estimated_size = DefaultRecordBatch::size_in_bytes_for_simple_records(&records);
        assert_eq!(actual_size, estimated_size);
    }

    /// Translated from DefaultRecordBatchTest.testInvalidRecordSize
    #[test]
    fn test_invalid_record_size() {
        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        let mut buffer = mem_records.into_buffer();
        // Corrupt the length field
        write_i32(&mut buffer, LENGTH_OFFSET, 10);

        let batch = DefaultRecordBatch::new(buffer);
        assert!(!batch.is_valid());
        assert!(batch.ensure_valid().is_err());
    }

    /// Translated from DefaultRecordBatchTest.testInvalidRecordCountTooManyNonCompressedV2
    #[test]
    fn test_invalid_record_count_too_many() {
        let batch = records_with_invalid_record_count(5);
        let result = batch.iter_records();
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordBatchTest.testInvalidRecordCountTooLittleNonCompressedV2
    #[test]
    fn test_invalid_record_count_too_little() {
        let batch = records_with_invalid_record_count(2);
        let result = batch.iter_records();
        // With count=2, reading 2 records succeeds but there are remaining bytes
        assert!(result.is_err());
    }

    /// Translated from DefaultRecordBatchTest.testInvalidCrc
    #[test]
    fn test_invalid_crc() {
        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        let mut buffer = mem_records.into_buffer();
        // Corrupt the last offset delta (inside CRC-covered region)
        write_i32(&mut buffer, LAST_OFFSET_DELTA_OFFSET, 23);

        let batch = DefaultRecordBatch::new(buffer);
        assert!(!batch.is_valid());
        assert!(batch.ensure_valid().is_err());
    }

    /// Translated from DefaultRecordBatchTest.testSetLastOffset
    #[test]
    fn test_set_last_offset() {
        let simple_records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &simple_records,
        );

        let last_offset: i64 = 500;
        let first_offset = last_offset - simple_records.len() as i64 + 1;

        let mut batch = DefaultRecordBatch::new(mem_records.into_buffer());
        batch.set_last_offset(last_offset);
        assert_eq!(last_offset, batch.last_offset());
        assert_eq!(first_offset, batch.base_offset());
        assert!(batch.is_valid());

        let records = batch.iter_records().unwrap();
        let mut offset = first_offset;
        for record in &records {
            assert_eq!(offset, record.offset());
            offset += 1;
        }
    }

    /// Translated from DefaultRecordBatchTest.testSetPartitionLeaderEpoch
    #[test]
    fn test_set_partition_leader_epoch() {
        let simple_records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &simple_records,
        );

        let leader_epoch: i32 = 500;
        let mut batch = DefaultRecordBatch::new(mem_records.into_buffer());
        batch.set_partition_leader_epoch(leader_epoch);
        assert_eq!(leader_epoch, batch.partition_leader_epoch());
        assert!(batch.is_valid());
    }

    /// Translated from DefaultRecordBatchTest.testSetLogAppendTime
    #[test]
    fn test_set_log_append_time() {
        let simple_records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &simple_records,
        );

        let log_append_time: i64 = 15;
        let mut batch = DefaultRecordBatch::new(mem_records.into_buffer());
        batch.set_max_timestamp(TimestampType::LogAppendTime, log_append_time);
        assert_eq!(TimestampType::LogAppendTime, batch.timestamp_type());
        assert_eq!(log_append_time, batch.max_timestamp());
        assert!(batch.is_valid());

        // Verify records get the log append time
        let records = batch.iter_records().unwrap();
        for record in &records {
            assert_eq!(log_append_time, record.timestamp());
        }
    }

    /// Translated from DefaultRecordBatchTest.testSetNoTimestampTypeNotAllowed
    #[test]
    #[should_panic(expected = "Timestamp type must be provided")]
    fn test_set_no_timestamp_type_not_allowed() {
        let simple_records = vec![SimpleRecord::with_timestamp(
            1,
            Some(b"a".to_vec()),
            Some(b"1".to_vec()),
        )];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &simple_records,
        );

        let mut batch = DefaultRecordBatch::new(mem_records.into_buffer());
        batch.set_max_timestamp(TimestampType::NoTimestampType, NO_TIMESTAMP);
    }

    /// Translated from DefaultRecordBatchTest.testIncrementSequence
    #[test]
    fn test_increment_sequence() {
        assert_eq!(10, DefaultRecordBatch::increment_sequence(5, 5));
        assert_eq!(0, DefaultRecordBatch::increment_sequence(i32::MAX, 1));
        assert_eq!(4, DefaultRecordBatch::increment_sequence(i32::MAX - 5, 10));
    }

    /// Translated from DefaultRecordBatchTest.testDecrementSequence
    #[test]
    fn test_decrement_sequence() {
        assert_eq!(0, DefaultRecordBatch::decrement_sequence(5, 5));
        assert_eq!(i32::MAX, DefaultRecordBatch::decrement_sequence(0, 1));
    }

    fn records_with_invalid_record_count(invalid_count: i32) -> DefaultRecordBatch {
        let timestamp: i64 = 1000000;
        let records = vec![
            SimpleRecord::with_timestamp(timestamp, None, Some(b"hello".to_vec())),
            SimpleRecord::with_timestamp(timestamp, None, Some(b"there".to_vec())),
            SimpleRecord::with_timestamp(timestamp, None, Some(b"beautiful".to_vec())),
        ];

        let mem_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            &records,
        );

        let mut buffer = mem_records.into_buffer();
        write_i32(&mut buffer, RECORDS_COUNT_OFFSET, invalid_count);
        DefaultRecordBatch::new(buffer)
    }
}
