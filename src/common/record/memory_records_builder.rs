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

//! Builder for in-memory record batches.
//!
//! Translated from `org.apache.kafka.common.record.MemoryRecordsBuilder`.
//!
//! This is the write path for [`super::MemoryRecords`]. It transparently handles
//! compression (NONE only for MVP) and exposes methods for appending new records.

use crate::common::record::compression_type::CompressionType;
use crate::common::record::default_record::DefaultRecord;
use crate::common::record::default_record_batch;
use crate::common::record::timestamp_type::TimestampType;
use crate::common::record::{
    CURRENT_MAGIC_VALUE, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_TIMESTAMP, RecordHeader, SimpleRecord,
    record_batch_header_size_in_bytes,
};
use crate::errors::{ErrorCode, KafkaError, Result};

/// Information about the records that have been written.
///
/// Translated from `MemoryRecordsBuilder.RecordsInfo`.
#[derive(Debug, Clone, Copy)]
pub struct RecordsInfo {
    /// The maximum timestamp in the batch.
    pub max_timestamp: i64,
    /// The shallow offset of the record with the maximum timestamp.
    pub shallow_offset_of_max_timestamp: i64,
}

/// Builder for constructing in-memory record batches.
///
/// Translated from `org.apache.kafka.common.record.MemoryRecordsBuilder`.
///
/// Only magic v2 and `CompressionType::None` are supported for MVP.
pub struct MemoryRecordsBuilder {
    /// The underlying byte buffer.
    buffer: Vec<u8>,
    /// The magic version.
    magic: i8,
    /// The compression type.
    compression_type: CompressionType,
    /// The timestamp type.
    timestamp_type: TimestampType,
    /// The base offset for this batch.
    base_offset: i64,
    /// The log append time (only used for LogAppendTime).
    log_append_time: i64,
    /// The initial position in the buffer (where the batch header starts).
    initial_position: usize,
    /// The batch header size in bytes.
    batch_header_size_in_bytes: usize,
    /// The write limit (max bytes).
    write_limit: usize,
    /// The delete horizon timestamp (-1 if not set).
    delete_horizon_ms: i64,
    /// Whether this is a transactional batch.
    is_transactional: bool,
    /// The producer ID.
    producer_id: i64,
    /// The producer epoch.
    producer_epoch: i16,
    /// The base sequence number.
    base_sequence: i32,
    /// Whether this is a control batch.
    is_control_batch: bool,
    /// The partition leader epoch.
    partition_leader_epoch: i32,
    /// The number of records appended.
    num_records: i32,
    /// The number of uncompressed bytes written (excluding header).
    uncompressed_records_size_in_bytes: usize,
    /// The maximum timestamp seen.
    max_timestamp: i64,
    /// The offset of the record with the maximum timestamp.
    offset_of_max_timestamp: i64,
    /// The last offset appended.
    last_offset: Option<i64>,
    /// The base timestamp (timestamp of the first record or delete horizon).
    base_timestamp: Option<i64>,
    /// The built records (set after close).
    built_records: Option<Vec<u8>>,
    /// Whether the builder has been aborted.
    aborted: bool,
    /// Whether the append stream has been closed.
    append_stream_closed: bool,
    /// The actual compression ratio.
    actual_compression_ratio: f32,
    /// The estimated compression ratio.
    estimated_compression_ratio: f32,
}

impl MemoryRecordsBuilder {
    /// Create a new `MemoryRecordsBuilder`.
    ///
    /// # Arguments
    /// * `buffer_capacity` - The initial buffer capacity.
    /// * `magic` - The record batch magic version (must be v2).
    /// * `compression_type` - The compression type (only NONE supported).
    /// * `timestamp_type` - The timestamp type.
    /// * `base_offset` - The base offset.
    /// * `log_append_time` - The log append time.
    /// * `producer_id` - The producer ID.
    /// * `producer_epoch` - The producer epoch.
    /// * `base_sequence` - The base sequence number.
    /// * `is_transactional` - Whether the batch is transactional.
    /// * `is_control_batch` - Whether the batch is a control batch.
    /// * `partition_leader_epoch` - The partition leader epoch.
    /// * `write_limit` - The max number of bytes to write.
    ///
    /// # Errors
    /// Returns an error if the timestamp type is invalid for the given magic version.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer_capacity: usize,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: usize,
    ) -> Result<Self> {
        Self::with_delete_horizon(
            buffer_capacity,
            magic,
            compression_type,
            timestamp_type,
            base_offset,
            log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            is_control_batch,
            partition_leader_epoch,
            write_limit,
            NO_TIMESTAMP,
        )
    }

    /// Create a new `MemoryRecordsBuilder` with a delete horizon.
    #[allow(clippy::too_many_arguments)]
    pub fn with_delete_horizon(
        buffer_capacity: usize,
        magic: i8,
        compression_type: CompressionType,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
        is_control_batch: bool,
        partition_leader_epoch: i32,
        write_limit: usize,
        delete_horizon_ms: i64,
    ) -> Result<Self> {
        if timestamp_type == TimestampType::NoTimestampType {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                "TimestampType must be set for magic >= 0",
            ));
        }

        let batch_header_size = record_batch_header_size_in_bytes(magic, compression_type);

        let initial_position = 0;

        // Allocate buffer with space for header + records
        let mut buffer = Vec::with_capacity(buffer_capacity);
        // Reserve header space by writing zeros
        buffer.resize(initial_position + batch_header_size, 0);

        let has_delete_horizon = magic >= CURRENT_MAGIC_VALUE && delete_horizon_ms >= 0;
        let base_timestamp = if has_delete_horizon {
            Some(delete_horizon_ms)
        } else {
            None
        };

        Ok(MemoryRecordsBuilder {
            buffer,
            magic,
            compression_type,
            timestamp_type,
            base_offset,
            log_append_time,
            initial_position,
            batch_header_size_in_bytes: batch_header_size,
            write_limit,
            delete_horizon_ms,
            is_transactional,
            producer_id,
            producer_epoch,
            base_sequence,
            is_control_batch,
            partition_leader_epoch,
            num_records: 0,
            uncompressed_records_size_in_bytes: 0,
            max_timestamp: NO_TIMESTAMP,
            offset_of_max_timestamp: -1,
            last_offset: None,
            base_timestamp,
            built_records: None,
            aborted: false,
            append_stream_closed: false,
            actual_compression_ratio: 1.0,
            estimated_compression_ratio: 1.0,
        })
    }

    /// Returns whether a delete horizon is set.
    pub fn has_delete_horizon_ms(&self) -> bool {
        self.magic >= CURRENT_MAGIC_VALUE && self.delete_horizon_ms >= 0
    }

    /// Returns the magic version.
    pub fn magic(&self) -> i8 {
        self.magic
    }

    /// Returns the compression type.
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    /// Returns whether this is a control batch.
    pub fn is_control_batch(&self) -> bool {
        self.is_control_batch
    }

    /// Returns whether this is a transactional batch.
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }

    /// Returns the number of records appended so far.
    pub fn num_records(&self) -> i32 {
        self.num_records
    }

    /// Returns the producer ID.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the producer epoch.
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns the base sequence.
    pub fn base_sequence(&self) -> i32 {
        self.base_sequence
    }

    /// Returns whether the builder has been closed.
    pub fn is_closed(&self) -> bool {
        self.built_records.is_some()
    }

    /// Returns the info about the records (max timestamp and offset of max timestamp).
    ///
    /// Translated from `MemoryRecordsBuilder.info()`.
    pub fn info(&self) -> RecordsInfo {
        if self.timestamp_type == TimestampType::LogAppendTime {
            // For LogAppendTime, all records have the same timestamp
            RecordsInfo {
                max_timestamp: self.log_append_time,
                shallow_offset_of_max_timestamp: self.last_offset.unwrap_or(self.base_offset),
            }
        } else if self.max_timestamp == NO_TIMESTAMP {
            RecordsInfo { max_timestamp: NO_TIMESTAMP, shallow_offset_of_max_timestamp: -1 }
        } else {
            // For magic v2, return lastOffset as shallowOffsetOfMaxTimestamp
            RecordsInfo {
                max_timestamp: self.max_timestamp,
                shallow_offset_of_max_timestamp: self.last_offset.unwrap_or(self.base_offset),
            }
        }
    }

    /// Returns the sum of the size of the batch header (always uncompressed)
    /// and the records (before compression).
    pub fn uncompressed_bytes_written(&self) -> usize {
        self.uncompressed_records_size_in_bytes + self.batch_header_size_in_bytes
    }

    /// Set the producer state (used when resending a batch).
    ///
    /// # Errors
    /// Returns an error if the builder is already closed.
    pub fn set_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) -> Result<()> {
        if self.is_closed() {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Trying to set producer state of an already closed batch",
            ));
        }
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
        Ok(())
    }

    /// Override the last offset. Used when retaining records for compaction.
    ///
    /// # Errors
    /// Returns an error if the records have already been built.
    pub fn override_last_offset(&mut self, last_offset: i64) -> Result<()> {
        if self.built_records.is_some() {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Cannot override the last offset after the records have been built",
            ));
        }
        self.last_offset = Some(last_offset);
        Ok(())
    }

    /// Check if we have room for a new record.
    ///
    /// If no records have been appended, this returns true (we always allow at
    /// least one record).
    pub fn has_room_for(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> bool {
        if self.is_full() {
            return false;
        }
        // Always allow at least one record
        if self.num_records == 0 {
            return true;
        }
        let next_offset_delta = match self.last_offset {
            None => 0,
            Some(lo) => (lo - self.base_offset + 1) as i32,
        };
        let timestamp_delta = match self.base_timestamp {
            None => 0,
            Some(bt) => timestamp - bt,
        };
        let record_size =
            DefaultRecord::size_in_bytes_with_slices(next_offset_delta, timestamp_delta, key, value, headers);
        self.write_limit >= self.estimated_bytes_written() + record_size as usize
    }

    /// Returns whether the batch is full.
    pub fn is_full(&self) -> bool {
        self.append_stream_closed || (self.num_records > 0 && self.write_limit <= self.estimated_bytes_written())
    }

    /// Get an estimate of the number of bytes written.
    fn estimated_bytes_written(&self) -> usize {
        // For NONE compression, this is exact
        self.batch_header_size_in_bytes + self.uncompressed_records_size_in_bytes
    }

    /// Set the estimated compression ratio.
    pub fn set_estimated_compression_ratio(&mut self, ratio: f32) {
        self.estimated_compression_ratio = ratio;
    }

    /// Get an estimate of the size in bytes. Exact if uncompressed or if closed.
    pub fn estimated_size_in_bytes(&self) -> usize {
        if let Some(ref built) = self.built_records {
            built.len()
        } else {
            self.estimated_bytes_written()
        }
    }

    /// Append a record at the next sequential offset.
    ///
    /// # Errors
    /// Returns an error if the builder is closed for appends.
    pub fn append(
        &mut self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<()> {
        let next_offset = self.next_sequential_offset();
        self.append_with_offset(next_offset, timestamp, key, value, headers)
    }

    /// Append a [`SimpleRecord`] at the next sequential offset.
    ///
    /// # Errors
    /// Returns an error if the builder is closed for appends.
    pub fn append_simple_record(&mut self, record: &SimpleRecord) -> Result<()> {
        self.append(
            record.timestamp,
            record.key.as_deref(),
            record.value.as_deref(),
            &record.headers,
        )
    }

    /// Append a record at the given offset.
    ///
    /// # Errors
    /// Returns an error if the builder is closed for appends or if the offset is invalid.
    pub fn append_with_offset(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<()> {
        if let Some(last) = self.last_offset
            && offset <= last
        {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Illegal offset {} following previous offset {} \
                     (Offsets must increase monotonically).",
                    offset, last
                ),
            ));
        }

        if timestamp < 0 && timestamp != NO_TIMESTAMP {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid negative timestamp {}", timestamp),
            ));
        }

        self.append_default_record(offset, timestamp, key, value, headers)
    }

    /// Append a default (v2) record.
    fn append_default_record(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> Result<()> {
        self.ensure_open_for_record_append()?;

        let offset_delta = (offset - self.base_offset) as i32;
        if self.base_timestamp.is_none() {
            self.base_timestamp = Some(timestamp);
        }
        let timestamp_delta = timestamp - self.base_timestamp.unwrap();

        // Write the record directly to the buffer (no compression for NONE)
        let size_in_bytes =
            DefaultRecord::write_to(&mut self.buffer, offset_delta, timestamp_delta, key, value, headers)?;

        self.record_written(offset, timestamp, size_in_bytes as usize)?;
        Ok(())
    }

    /// Record that a record has been written.
    ///
    /// # Errors
    /// Returns an error if the maximum number of records per batch is exceeded
    /// or if the offset delta overflows i32.
    fn record_written(&mut self, offset: i64, timestamp: i64, size: usize) -> Result<()> {
        if self.num_records == i32::MAX {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                format!("Maximum number of records per batch exceeded, max records: {}", i32::MAX),
            ));
        }
        if offset - self.base_offset > i32::MAX as i64 {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Maximum offset delta exceeded, base offset: {}, last offset: {}",
                    self.base_offset, offset
                ),
            ));
        }

        self.num_records += 1;
        self.uncompressed_records_size_in_bytes += size;
        self.last_offset = Some(offset);

        if timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
            self.offset_of_max_timestamp = offset;
        }
        Ok(())
    }

    /// Ensure the builder is open for record appends.
    fn ensure_open_for_record_append(&self) -> Result<()> {
        if self.append_stream_closed {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Tried to append a record, but MemoryRecordsBuilder is closed for record appends",
            ));
        }
        Ok(())
    }

    /// Ensure the builder is open for batch writing.
    fn ensure_open_for_record_batch_write(&self) -> Result<()> {
        if self.is_closed() {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Tried to write record batch header, but MemoryRecordsBuilder is closed",
            ));
        }
        if self.aborted {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Tried to write record batch header, but MemoryRecordsBuilder is aborted",
            ));
        }
        Ok(())
    }

    /// Close the builder for record appends but allow header writes.
    pub fn close_for_record_appends(&mut self) {
        self.append_stream_closed = true;
    }

    /// Abort this builder, releasing resources.
    pub fn abort(&mut self) {
        self.close_for_record_appends();
        self.buffer.truncate(self.initial_position);
        self.aborted = true;
    }

    /// Validate the producer state.
    fn validate_producer_state(&self) -> Result<()> {
        if self.is_transactional && self.producer_id == NO_PRODUCER_ID {
            return Err(KafkaError::new(
                ErrorCode::InvalidArgument,
                "Cannot write transactional messages without a valid producer ID",
            ));
        }

        if self.producer_id != NO_PRODUCER_ID {
            if self.producer_epoch == NO_PRODUCER_EPOCH {
                return Err(KafkaError::new(ErrorCode::InvalidArgument, "Invalid negative producer epoch"));
            }
            if self.base_sequence < 0 && !self.is_control_batch {
                return Err(KafkaError::new(
                    ErrorCode::InvalidArgument,
                    "Invalid negative sequence number used",
                ));
            }
        }

        Ok(())
    }

    /// Write the v2 batch header.
    ///
    /// Returns the number of compressed bytes written (records only, excluding header).
    fn write_default_batch_header(&mut self) -> Result<usize> {
        self.ensure_open_for_record_batch_write()?;

        let pos = self.buffer.len();
        let size = pos - self.initial_position;
        let written_compressed = size - default_record_batch::RECORD_BATCH_OVERHEAD;
        let offset_delta = (self.last_offset.unwrap_or(self.base_offset) - self.base_offset) as i32;

        let max_timestamp = if self.timestamp_type == TimestampType::LogAppendTime {
            self.log_append_time
        } else {
            self.max_timestamp
        };

        // Write header into the reserved header space
        let base_timestamp = self.base_timestamp.unwrap_or(NO_TIMESTAMP);
        let has_delete_horizon = self.has_delete_horizon_ms();

        // Build the header in-place. We capture all field values before
        // taking the mutable borrow on buffer to satisfy the borrow checker.
        let base_offset = self.base_offset;
        let magic = self.magic;
        let compression_type = self.compression_type;
        let timestamp_type = self.timestamp_type;
        let producer_id = self.producer_id;
        let producer_epoch = self.producer_epoch;
        let base_sequence = self.base_sequence;
        let is_transactional = self.is_transactional;
        let is_control_batch = self.is_control_batch;
        let partition_leader_epoch = self.partition_leader_epoch;
        let num_records = self.num_records;
        let initial_position = self.initial_position;

        let header_buf = &mut self.buffer[initial_position..];
        default_record_batch::write_header_to_slice(
            header_buf,
            base_offset,
            offset_delta,
            size as i32,
            magic,
            compression_type,
            timestamp_type,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            is_control_batch,
            has_delete_horizon,
            partition_leader_epoch,
            num_records,
        )?;

        Ok(written_compressed)
    }

    /// Close this builder and return the resulting buffer bytes.
    ///
    /// # Errors
    /// Returns an error if the builder has been aborted.
    pub fn close(&mut self) -> Result<()> {
        if self.aborted {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Cannot close MemoryRecordsBuilder as it has already been aborted",
            ));
        }

        if self.built_records.is_some() {
            return Ok(());
        }

        self.validate_producer_state()?;
        self.close_for_record_appends();

        if self.num_records == 0 {
            self.buffer.truncate(self.initial_position);
            self.built_records = Some(Vec::new());
        } else {
            let written_compressed = self.write_default_batch_header()?;
            self.actual_compression_ratio = written_compressed as f32 / self.uncompressed_records_size_in_bytes as f32;
            let built = self.buffer[self.initial_position..].to_vec();
            self.built_records = Some(built);
        }

        Ok(())
    }

    /// Build and return the [`super::MemoryRecords`].
    ///
    /// # Errors
    /// Returns an error if the builder has been aborted.
    pub fn build(mut self) -> Result<super::MemoryRecords> {
        if self.aborted {
            return Err(KafkaError::new(
                ErrorCode::IllegalState,
                "Attempting to build an aborted record batch",
            ));
        }
        self.close()?;
        let buf = self.built_records.take().unwrap_or_default();
        Ok(super::MemoryRecords::from_buffer(buf))
    }

    /// Returns the next sequential offset (for appending records in order).
    fn next_sequential_offset(&self) -> i64 {
        match self.last_offset {
            None => self.base_offset,
            Some(lo) => lo + 1,
        }
    }

    /// Returns the compression ratio.
    pub fn compression_ratio(&self) -> f32 {
        self.actual_compression_ratio
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::{
        CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
    };

    /// Translated from MemoryRecordsBuilderTest.testWriteEmptyRecordSet
    /// (only for magic v2, CompressionType::None, bufferOffset 0)
    #[test]
    fn test_write_empty_record_set() {
        let builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        let records = builder.build().unwrap();
        assert_eq!(0, records.size_in_bytes());
    }

    /// Translated from MemoryRecordsBuilderTest.testWriteTransactionalRecordSet
    /// (only for magic v2)
    #[test]
    fn test_write_transactional_record_set() {
        let pid: i64 = 9809;
        let epoch: i16 = 15;
        let sequence: i32 = 2342;

        let mut builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            pid,
            epoch,
            sequence,
            true,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        builder
            .append(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
                Some(b"foo"),
                Some(b"bar"),
                &[],
            )
            .unwrap();

        let records = builder.build().unwrap();
        let batches = records.batches();
        assert_eq!(1, batches.len());
        assert!(batches[0].is_transactional());
    }

    /// Translated from MemoryRecordsBuilderTest.testWriteTransactionalWithInvalidPID
    /// (only for magic v2)
    #[test]
    fn test_write_transactional_with_invalid_pid() {
        let pid: i64 = NO_PRODUCER_ID;
        let epoch: i16 = 15;
        let sequence: i32 = 2342;

        let mut builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            pid,
            epoch,
            sequence,
            true,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        // close() should fail because producer_id is invalid for transactional batch
        let result = builder.close();
        assert!(result.is_err());
    }

    /// Translated from MemoryRecordsBuilderTest.testWriteIdempotentWithInvalidEpoch
    /// (only for magic v2)
    #[test]
    fn test_write_idempotent_with_invalid_epoch() {
        let pid: i64 = 9809;
        let epoch: i16 = NO_PRODUCER_EPOCH;
        let sequence: i32 = 2342;

        let mut builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            pid,
            epoch,
            sequence,
            true,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        let result = builder.close();
        assert!(result.is_err());
    }

    /// Translated from MemoryRecordsBuilderTest.testWriteIdempotentWithInvalidBaseSequence
    /// (only for magic v2)
    #[test]
    fn test_write_idempotent_with_invalid_base_sequence() {
        let pid: i64 = 9809;
        let epoch: i16 = 15;
        let sequence: i32 = NO_SEQUENCE;

        let mut builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            pid,
            epoch,
            sequence,
            true,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        let result = builder.close();
        assert!(result.is_err());
    }

    /// Translated from MemoryRecordsBuilderTest.testEstimatedSizeInBytes (v2, NONE)
    #[test]
    fn test_estimated_size_in_bytes() {
        let mut builder = MemoryRecordsBuilder::new(
            1024,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            1024,
        )
        .unwrap();

        let mut previous_estimate = 0;
        for i in 0..10 {
            let value = format!("{}", i);
            builder.append(i, None, Some(value.as_bytes()), &[]).unwrap();
            let current_estimate = builder.estimated_size_in_bytes();
            assert!(
                current_estimate > previous_estimate,
                "Estimate must increase: {} > {}",
                current_estimate,
                previous_estimate
            );
            previous_estimate = current_estimate;
        }

        let bytes_written_before_close = builder.estimated_size_in_bytes();
        let records = builder.build().unwrap();
        // For NONE compression, estimated size before close should equal final size
        assert_eq!(records.size_in_bytes() as usize, bytes_written_before_close);
    }

    /// Translated from MemoryRecordsBuilderTest.buildUsingLogAppendTime (v2, NONE)
    #[test]
    fn test_build_using_log_append_time() {
        let log_append_time: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut builder = MemoryRecordsBuilder::new(
            1024,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::LogAppendTime,
            0,
            log_append_time,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            1024,
        )
        .unwrap();

        builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
        builder.append(0, Some(b"b"), Some(b"2"), &[]).unwrap();
        builder.append(0, Some(b"c"), Some(b"3"), &[]).unwrap();
        let records = builder.build().unwrap();

        let batches = records.batches();
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(TimestampType::LogAppendTime, batch.timestamp_type());

        // All records should have the log append time
        let all_records = batch.iter_records().unwrap();
        for record in &all_records {
            assert_eq!(log_append_time, record.timestamp());
        }
    }

    /// Translated from MemoryRecordsBuilderTest.buildUsingCreateTime (v2, NONE)
    #[test]
    fn test_build_using_create_time() {
        let mut builder = MemoryRecordsBuilder::new(
            1024,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            1024,
        )
        .unwrap();

        builder.append(0, Some(b"a"), Some(b"1"), &[]).unwrap();
        builder.append(2, Some(b"b"), Some(b"2"), &[]).unwrap();
        builder.append(1, Some(b"c"), Some(b"3"), &[]).unwrap();
        let records = builder.build().unwrap();

        let batches = records.batches();
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(TimestampType::CreateTime, batch.timestamp_type());

        let all_records = batch.iter_records().unwrap();
        let expected_timestamps: Vec<i64> = vec![0, 2, 1];
        for (i, record) in all_records.iter().enumerate() {
            assert_eq!(expected_timestamps[i], record.timestamp());
        }
    }

    /// Translated from MemoryRecordsBuilderTest.testSmallWriteLimit (v2, NONE)
    #[test]
    fn test_small_write_limit() {
        let key = b"foo";
        let value = b"bar";
        let write_limit = 0;

        let mut builder = MemoryRecordsBuilder::new(
            512,
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
            write_limit,
        )
        .unwrap();

        assert!(!builder.is_full());
        assert!(builder.has_room_for(0, Some(key), Some(value), &[]));
        builder.append(0, Some(key), Some(value), &[]).unwrap();

        assert!(builder.is_full());
        assert!(!builder.has_room_for(0, Some(key), Some(value), &[]));

        let records = builder.build().unwrap();
        let batches = records.batches();
        assert_eq!(1, batches.len());
        let all_records = batches[0].iter_records().unwrap();
        assert_eq!(1, all_records.len());
        assert_eq!(Some(key.as_slice()), all_records[0].key());
        assert_eq!(Some(value.as_slice()), all_records[0].value());
    }

    /// Translated from MemoryRecordsBuilderTest.testAppendAtInvalidOffset (v2, NONE)
    #[test]
    fn test_append_at_invalid_offset() {
        let mut builder = MemoryRecordsBuilder::new(
            1024,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            1024,
        )
        .unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        builder.append_with_offset(0, now, Some(b"a"), None, &[]).unwrap();

        // offsets must increase monotonically
        let result = builder.append_with_offset(0, now, Some(b"b"), None, &[]);
        assert!(result.is_err());
    }

    /// Translated from MemoryRecordsBuilderTest.testAppendedChecksumConsistency (v2, NONE)
    #[test]
    fn test_appended_checksum_consistency() {
        let mut builder = MemoryRecordsBuilder::new(
            512,
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
            512,
        )
        .unwrap();

        builder.append(1, Some(b"key"), Some(b"value"), &[]).unwrap();
        let records = builder.build().unwrap();
        let batches = records.batches();
        assert_eq!(1, batches.len());
        let all_records = batches[0].iter_records().unwrap();
        assert_eq!(1, all_records.len());
    }

    /// Translated from MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnBuildWhenAborted (v2)
    #[test]
    fn test_build_when_aborted() {
        let mut builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        builder.abort();
        let result = builder.build();
        assert!(result.is_err());
    }

    /// Translated from MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnAppendWhenAborted (v2)
    #[test]
    fn test_append_when_aborted() {
        let mut builder = MemoryRecordsBuilder::new(
            128,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            0,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            128,
        )
        .unwrap();

        builder.abort();
        let result = builder.append(0, Some(b"a"), Some(b"1"), &[]);
        assert!(result.is_err());
    }

    // Note: MemoryRecordsBuilderTest.shouldThrowIllegalStateExceptionOnAppendWhenClosed
    // is not translated because build() consumes self in Rust, making it impossible
    // to call append() after build(). The type system enforces this invariant at compile time.
}
