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

//! Builder for creating memory record batches.
//!
//! This is the write path for [`MemoryRecords`]. It transparently handles
//! compression and exposes methods for appending new records.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.MemoryRecordsBuilder`.

use std::io::{self, Write};

use crate::common::compress::{CompressingWriter, Compression};
use crate::common::header::internals::RecordHeader;
use crate::common::record::abstract_records::record_batch_header_size_in_bytes;
use crate::common::record::compression_type::CompressionType;
use crate::common::record::default_record::DefaultRecord;
use crate::common::record::default_record_batch::DefaultRecordBatch;
use crate::common::record::memory_records::MemoryRecords;
use crate::common::record::record_batch::RecordBatch;
use crate::common::record::simple_record::SimpleRecord;
use crate::common::record::timestamp_type::TimestampType;

/// Estimation factor to account for compression overhead.
const COMPRESSION_RATE_ESTIMATION_FACTOR: f32 = 1.05;

/// State of the append stream.
enum AppendState<W: Write> {
    /// Stream is open for appending records.
    Open(CompressingWriter<W>),
    /// Stream has been closed (records cannot be appended).
    Closed,
}

/// Builder for creating memory record batches.
///
/// Manages a compression stream and provides methods for appending records.
/// On [`close()`](Self::close) or [`build()`](Self::build), flushes compression
/// and writes the batch header (attributes, timestamps, CRC).
///
/// Corresponds to Java's `org.apache.kafka.common.record.MemoryRecordsBuilder`.
pub struct MemoryRecordsBuilder {
    timestamp_type: TimestampType,
    compression: Compression,
    /// The underlying buffer holding the batch data.
    buffer: Vec<u8>,
    magic: i8,
    initial_position: usize,
    base_offset: i64,
    log_append_time: i64,
    is_control_batch: bool,
    partition_leader_epoch: i32,
    write_limit: usize,
    batch_header_size_in_bytes: usize,
    delete_horizon_ms: i64,

    /// Conservative estimate of the compression ratio.
    estimated_compression_ratio: f32,

    /// The compression stream state.
    append_stream: AppendState<Vec<u8>>,
    is_transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    /// Number of bytes (excluding the header) written before compression.
    uncompressed_records_size_in_bytes: usize,
    num_records: i32,
    actual_compression_ratio: f32,
    max_timestamp: i64,
    offset_of_max_timestamp: i64,
    last_offset: Option<i64>,
    base_timestamp: Option<i64>,

    built_records: Option<MemoryRecords>,
    aborted: bool,
}

impl MemoryRecordsBuilder {
    /// Create a new `MemoryRecordsBuilder`.
    ///
    /// The `buffer` is expected to have a position (initial_position) where the
    /// batch should start. The builder will skip `batch_header_size_in_bytes` bytes
    /// at the start for the header, then append records after that.
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder(ByteBufferOutputStream, ...)`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut buffer: Vec<u8>,
        initial_position: usize,
        magic: i8,
        compression: Compression,
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
    ) -> Self {
        // Validate parameters
        if magic > RecordBatch::MAGIC_VALUE_V0 && timestamp_type == TimestampType::NoTimestampType {
            panic!("TimestampType must be set for magic >= 0");
        }
        if magic < RecordBatch::MAGIC_VALUE_V2 {
            if is_transactional {
                panic!("Transactional records are not supported for magic {}", magic);
            }
            if is_control_batch {
                panic!("Control records are not supported for magic {}", magic);
            }
            if compression.compression_type() == CompressionType::Zstd {
                panic!("ZStandard compression is not supported for magic {}", magic);
            }
            if delete_horizon_ms != RecordBatch::NO_TIMESTAMP {
                panic!("Delete horizon timestamp is not supported for magic {}", magic);
            }
        }

        let batch_header_size = record_batch_header_size_in_bytes(magic, compression.compression_type());

        // Ensure the buffer is large enough for the header
        let header_end = initial_position + batch_header_size;
        if buffer.len() < header_end {
            buffer.resize(header_end, 0);
        }

        // Create the append stream (compression wraps a separate Vec<u8>)
        let append_buf = Vec::new();
        let append_writer = compression
            .wrap_for_output(append_buf, magic)
            .expect("Failed to create compression writer");

        let has_delete_horizon = magic >= RecordBatch::MAGIC_VALUE_V2 && delete_horizon_ms >= 0;
        let base_timestamp = if has_delete_horizon {
            Some(delete_horizon_ms)
        } else {
            None
        };

        Self {
            timestamp_type,
            compression,
            buffer,
            magic,
            initial_position,
            base_offset,
            log_append_time,
            is_control_batch,
            partition_leader_epoch,
            write_limit,
            batch_header_size_in_bytes: batch_header_size,
            delete_horizon_ms,
            estimated_compression_ratio: 1.0,
            append_stream: AppendState::Open(append_writer),
            is_transactional,
            producer_id,
            producer_epoch,
            base_sequence,
            uncompressed_records_size_in_bytes: 0,
            num_records: 0,
            actual_compression_ratio: 1.0,
            max_timestamp: RecordBatch::NO_TIMESTAMP,
            offset_of_max_timestamp: -1,
            last_offset: None,
            base_timestamp,
            built_records: None,
            aborted: false,
        }
    }

    /// Create a new builder with default delete_horizon_ms (NO_TIMESTAMP).
    #[allow(clippy::too_many_arguments)]
    pub fn new_default(
        buffer: Vec<u8>,
        initial_position: usize,
        magic: i8,
        compression: Compression,
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
    ) -> Self {
        Self::new(
            buffer,
            initial_position,
            magic,
            compression,
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
            RecordBatch::NO_TIMESTAMP,
        )
    }

    /// Returns the underlying buffer.
    pub fn buffer(&self) -> &Vec<u8> {
        &self.buffer
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Returns the initial capacity of the buffer.
    pub fn initial_capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Returns the actual compression ratio after building.
    pub fn compression_ratio(&self) -> f64 {
        self.actual_compression_ratio as f64
    }

    /// Returns the compression configuration.
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    /// Returns whether this is a control batch.
    pub fn is_control_batch(&self) -> bool {
        self.is_control_batch
    }

    /// Returns whether this batch is transactional.
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }

    /// Returns whether the delete horizon is set.
    pub fn has_delete_horizon_ms(&self) -> bool {
        self.magic >= RecordBatch::MAGIC_VALUE_V2 && self.delete_horizon_ms >= 0
    }

    /// Close this builder and return the resulting `MemoryRecords`.
    ///
    /// Panics if the builder has been aborted.
    pub fn build(&mut self) -> MemoryRecords {
        if self.aborted {
            panic!("Attempting to build an aborted record batch");
        }
        self.close();
        self.built_records.clone().expect("build() called but no records built")
    }

    /// Returns info about the records (max timestamp and shallow offset).
    ///
    /// Corresponds to Java's `MemoryRecordsBuilder.info()`.
    pub fn info(&self) -> RecordsInfo {
        if self.timestamp_type == TimestampType::LogAppendTime {
            if self.compression.compression_type() != CompressionType::None || self.magic >= RecordBatch::MAGIC_VALUE_V2
            {
                RecordsInfo::new(self.log_append_time, self.last_offset.unwrap_or(-1))
            } else {
                RecordsInfo::new(self.log_append_time, self.base_offset)
            }
        } else if self.max_timestamp == RecordBatch::NO_TIMESTAMP {
            RecordsInfo::new(RecordBatch::NO_TIMESTAMP, -1)
        } else if self.compression.compression_type() != CompressionType::None
            || self.magic >= RecordBatch::MAGIC_VALUE_V2
        {
            RecordsInfo::new(self.max_timestamp, self.last_offset.unwrap_or(-1))
        } else {
            RecordsInfo::new(self.max_timestamp, self.offset_of_max_timestamp)
        }
    }

    /// Returns the number of records appended so far.
    pub fn num_records(&self) -> i32 {
        self.num_records
    }

    /// Return the sum of the size of the batch header (always uncompressed)
    /// and the records (before compression).
    pub fn uncompressed_bytes_written(&self) -> usize {
        self.uncompressed_records_size_in_bytes + self.batch_header_size_in_bytes
    }

    /// Set the producer state.
    ///
    /// Panics if the builder is already closed.
    pub fn set_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) {
        if self.is_closed() {
            panic!("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
    }

    /// Override the last offset (for compaction).
    ///
    /// Panics if the records have already been built.
    pub fn override_last_offset(&mut self, last_offset: i64) {
        if self.built_records.is_some() {
            panic!("Cannot override the last offset after the records have been built");
        }
        self.last_offset = Some(last_offset);
    }

    /// Release resources required for record appends.
    ///
    /// After this method is called, it's only possible to update the RecordBatch header.
    pub fn close_for_record_appends(&mut self) {
        if let AppendState::Open(writer) = std::mem::replace(&mut self.append_stream, AppendState::Closed) {
            match writer.finish() {
                Ok(compressed_data) => {
                    // Append the compressed data to the buffer
                    let header_end = self.initial_position + self.batch_header_size_in_bytes;
                    self.buffer.truncate(header_end);
                    self.buffer.extend_from_slice(&compressed_data);
                },
                Err(e) => {
                    panic!("Failed to finish compression: {}", e);
                },
            }
        }
    }

    /// Abort the builder, releasing resources and resetting the buffer position.
    pub fn abort(&mut self) {
        self.close_for_record_appends();
        self.buffer.truncate(self.initial_position);
        self.aborted = true;
    }

    /// Reopen a closed (but not aborted) batch for rewriting producer state.
    pub fn reopen_and_rewrite_producer_state(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        is_transactional: bool,
    ) {
        if self.aborted {
            panic!("Should not reopen a batch which is already aborted.");
        }
        self.built_records = None;
        self.producer_id = producer_id;
        self.producer_epoch = producer_epoch;
        self.base_sequence = base_sequence;
        self.is_transactional = is_transactional;
    }

    /// Close the builder and finalize the batch.
    ///
    /// Panics if the builder has been aborted.
    pub fn close(&mut self) {
        if self.aborted {
            panic!("Cannot close MemoryRecordsBuilder as it has already been aborted");
        }

        if self.built_records.is_some() {
            return;
        }

        self.validate_producer_state();

        self.close_for_record_appends();

        if self.num_records == 0 {
            self.buffer.truncate(self.initial_position);
            self.built_records = Some(MemoryRecords::empty());
        } else if self.magic > RecordBatch::MAGIC_VALUE_V1 {
            let written_compressed = self.write_default_batch_header();
            self.actual_compression_ratio = written_compressed as f32 / self.uncompressed_records_size_in_bytes as f32;

            let batch_data = self.buffer[self.initial_position..].to_vec();
            self.built_records = Some(MemoryRecords::new(batch_data));
        } else {
            // Legacy format not supported
            let batch_data = self.buffer[self.initial_position..].to_vec();
            self.built_records = Some(MemoryRecords::new(batch_data));
        }
    }

    fn validate_producer_state(&self) {
        if self.is_transactional && self.producer_id == RecordBatch::NO_PRODUCER_ID {
            panic!("Cannot write transactional messages without a valid producer ID");
        }

        if self.producer_id != RecordBatch::NO_PRODUCER_ID {
            if self.producer_epoch == RecordBatch::NO_PRODUCER_EPOCH {
                panic!("Invalid negative producer epoch");
            }

            if self.base_sequence < 0 && !self.is_control_batch {
                panic!("Invalid negative sequence number used");
            }

            if self.magic < RecordBatch::MAGIC_VALUE_V2 {
                panic!("Idempotent messages are not supported for magic {}", self.magic);
            }
        }
    }

    /// Write the header to the default batch and return the number of
    /// compressed bytes written (excluding the header).
    fn write_default_batch_header(&mut self) -> usize {
        self.ensure_open_for_record_batch_write();
        let size = self.buffer.len() - self.initial_position;
        let written_compressed = size - RecordBatch::RECORD_BATCH_OVERHEAD;
        let offset_delta = (self.last_offset.unwrap() - self.base_offset) as i32;

        let max_timestamp = if self.timestamp_type == TimestampType::LogAppendTime {
            self.log_append_time
        } else {
            self.max_timestamp
        };

        // Compute has_delete_horizon before borrowing self.buffer mutably
        let has_delete_horizon = self.has_delete_horizon_ms();
        let initial_position = self.initial_position;
        let base_offset = self.base_offset;
        let magic = self.magic;
        let compression_type = self.compression.compression_type();
        let timestamp_type = self.timestamp_type;
        let base_timestamp = self.base_timestamp.unwrap_or(RecordBatch::NO_TIMESTAMP);
        let producer_id = self.producer_id;
        let producer_epoch = self.producer_epoch;
        let base_sequence = self.base_sequence;
        let is_transactional = self.is_transactional;
        let is_control_batch = self.is_control_batch;
        let partition_leader_epoch = self.partition_leader_epoch;
        let num_records = self.num_records;

        DefaultRecordBatch::write_header_at(
            &mut self.buffer,
            initial_position,
            base_offset,
            offset_delta,
            size,
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
        );

        written_compressed
    }

    /// Append a new record at the given offset.
    #[allow(clippy::too_many_arguments)]
    fn append_with_offset_internal(
        &mut self,
        offset: i64,
        is_control_record: bool,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) {
        if is_control_record != self.is_control_batch {
            panic!("Control records can only be appended to control batches");
        }

        if let Some(last) = self.last_offset
            && offset <= last
        {
            panic!(
                "Illegal offset {} following previous offset {} (Offsets must increase monotonically).",
                offset, last
            );
        }

        if timestamp < 0 && timestamp != RecordBatch::NO_TIMESTAMP {
            panic!("Invalid negative timestamp {}", timestamp);
        }

        if self.magic < RecordBatch::MAGIC_VALUE_V2 && !headers.is_empty() {
            panic!("Magic v{} does not support record headers", self.magic);
        }

        if self.base_timestamp.is_none() {
            self.base_timestamp = Some(timestamp);
        }

        if self.magic > RecordBatch::MAGIC_VALUE_V1 {
            self.append_default_record(offset, timestamp, key, value, headers);
        } else {
            // Legacy format not supported in producer path
            panic!("Legacy record format (magic {}) not supported", self.magic);
        }
    }

    /// Append a new record at the given offset.
    ///
    /// # Arguments
    /// * `offset` - The absolute offset of the record in the log buffer
    /// * `timestamp` - The record timestamp
    /// * `key` - The record key
    /// * `value` - The record value
    /// * `headers` - The record headers if there are any
    pub fn append_with_offset(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) {
        self.append_with_offset_internal(offset, false, timestamp, key, value, headers);
    }

    /// Append a new record at the given offset with byte slices.
    pub fn append_with_offset_bytes(&mut self, offset: i64, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) {
        self.append_with_offset(offset, timestamp, key, value, RecordBatch::EMPTY_HEADERS);
    }

    /// Append a new record at the given offset using a `SimpleRecord`.
    pub fn append_with_offset_simple(&mut self, offset: i64, record: &SimpleRecord) {
        self.append_with_offset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /// Append a new record at the next sequential offset.
    pub fn append(&mut self, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>, headers: &[RecordHeader]) {
        let offset = self.next_sequential_offset();
        self.append_with_offset(offset, timestamp, key, value, headers);
    }

    /// Append a new record at the next sequential offset with just key and value.
    pub fn append_kv(&mut self, timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) {
        self.append(timestamp, key, value, RecordBatch::EMPTY_HEADERS);
    }

    /// Append a `SimpleRecord` at the next sequential offset.
    pub fn append_simple(&mut self, record: &SimpleRecord) {
        let offset = self.next_sequential_offset();
        self.append_with_offset_simple(offset, record);
    }

    /// Append a record implementing the Record trait.
    pub fn append_record(&mut self, record: &dyn crate::common::record::record_trait::Record) {
        self.append_with_offset_internal(
            record.offset(),
            self.is_control_batch,
            record.timestamp(),
            record.key(),
            record.value(),
            record.headers(),
        );
    }

    /// Append a record without offset/magic validation (for testing).
    pub fn append_unchecked_with_offset(&mut self, offset: i64, record: &SimpleRecord) -> io::Result<()> {
        if self.magic >= RecordBatch::MAGIC_VALUE_V2 {
            let offset_delta = (offset - self.base_offset) as i32;
            let timestamp = record.timestamp();
            if self.base_timestamp.is_none() {
                self.base_timestamp = Some(timestamp);
            }

            let size_in_bytes = self.write_record(
                offset_delta,
                timestamp - self.base_timestamp.unwrap(),
                record.key(),
                record.value(),
                record.headers(),
            )?;
            self.record_written(offset, timestamp, size_in_bytes);
        }
        Ok(())
    }

    /// Internal method to append a default (v2) record.
    fn append_default_record(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) {
        self.ensure_open_for_record_append();
        let offset_delta = (offset - self.base_offset) as i32;
        let timestamp_delta = timestamp - self.base_timestamp.unwrap();
        let size_in_bytes = self
            .write_record(offset_delta, timestamp_delta, key, value, headers)
            .expect("I/O exception when writing to the append stream, closing");
        self.record_written(offset, timestamp, size_in_bytes);
    }

    /// Write a record to the append stream and return the size in bytes.
    fn write_record(
        &mut self,
        offset_delta: i32,
        timestamp_delta: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> io::Result<usize> {
        match &mut self.append_stream {
            AppendState::Open(writer) => {
                let size = DefaultRecord::write_to(writer, offset_delta, timestamp_delta, key, value, headers)?;
                Ok(size as usize)
            },
            AppendState::Closed => {
                panic!("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
            },
        }
    }

    /// Track that a record was written.
    fn record_written(&mut self, offset: i64, timestamp: i64, size: usize) {
        if self.num_records == i32::MAX {
            panic!("Maximum number of records per batch exceeded, max records: {}", i32::MAX);
        }
        if offset - self.base_offset > i32::MAX as i64 {
            panic!(
                "Maximum offset delta exceeded, base offset: {}, last offset: {}",
                self.base_offset, offset
            );
        }

        self.num_records += 1;
        self.uncompressed_records_size_in_bytes += size;
        self.last_offset = Some(offset);

        if self.magic > RecordBatch::MAGIC_VALUE_V0 && timestamp > self.max_timestamp {
            self.max_timestamp = timestamp;
            self.offset_of_max_timestamp = offset;
        }
    }

    fn ensure_open_for_record_append(&self) {
        if matches!(self.append_stream, AppendState::Closed) {
            panic!("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
        }
    }

    fn ensure_open_for_record_batch_write(&self) {
        if self.is_closed() {
            panic!("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        }
        if self.aborted {
            panic!("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
        }
    }

    /// Get an estimate of the number of bytes written.
    fn estimated_bytes_written(&self) -> usize {
        if self.compression.compression_type() == CompressionType::None {
            self.batch_header_size_in_bytes + self.uncompressed_records_size_in_bytes
        } else {
            self.batch_header_size_in_bytes
                + (self.uncompressed_records_size_in_bytes as f32
                    * self.estimated_compression_ratio
                    * COMPRESSION_RATE_ESTIMATION_FACTOR) as usize
        }
    }

    /// Set the estimated compression ratio.
    pub fn set_estimated_compression_ratio(&mut self, ratio: f32) {
        self.estimated_compression_ratio = ratio;
    }

    /// Check if we have room for a new record containing the given key/value pair.
    ///
    /// If no records have been appended, then this returns true.
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

        // We always allow at least one record to be appended
        if self.num_records == 0 {
            return true;
        }

        let record_size = if self.magic < RecordBatch::MAGIC_VALUE_V2 {
            // Legacy not supported, but provide a reasonable estimate
            0
        } else {
            let next_offset_delta = match self.last_offset {
                None => 0,
                Some(last) => (last - self.base_offset + 1) as i32,
            };
            let timestamp_delta = match self.base_timestamp {
                None => 0i64,
                Some(base) => timestamp - base,
            };
            DefaultRecord::size_in_bytes_with_slices(next_offset_delta, timestamp_delta, key, value, headers) as usize
        };

        // Be conservative and not take compression of the new record into consideration.
        self.write_limit >= self.estimated_bytes_written() + record_size
    }

    /// Check if we have room for a given number of bytes.
    pub fn has_room_for_estimated(&self, estimated_records_size: usize) -> bool {
        if self.is_full() {
            return false;
        }
        self.write_limit >= self.estimated_bytes_written() + estimated_records_size
    }

    /// Returns the maximum number of bytes that can be written for records.
    pub fn max_allowed_bytes(&self) -> usize {
        self.write_limit.saturating_sub(self.batch_header_size_in_bytes)
    }

    /// Returns whether the builder has been closed (records have been built).
    pub fn is_closed(&self) -> bool {
        self.built_records.is_some()
    }

    /// Returns whether the batch is full.
    pub fn is_full(&self) -> bool {
        matches!(self.append_stream, AppendState::Closed)
            || (self.num_records > 0 && self.write_limit <= self.estimated_bytes_written())
    }

    /// Get an estimate of the number of bytes written to the underlying buffer.
    ///
    /// The returned value is exactly correct if the record set is not compressed
    /// or if the builder has been closed.
    pub fn estimated_size_in_bytes(&self) -> usize {
        match &self.built_records {
            Some(records) => records.size_in_bytes(),
            None => self.estimated_bytes_written(),
        }
    }

    /// Returns the magic version.
    pub fn magic(&self) -> i8 {
        self.magic
    }

    fn next_sequential_offset(&self) -> i64 {
        match self.last_offset {
            None => self.base_offset,
            Some(last) => last + 1,
        }
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
}

impl Drop for MemoryRecordsBuilder {
    fn drop(&mut self) {
        // Ensure resources are released
        if !self.aborted && self.built_records.is_none() {
            // Try to close gracefully, but don't panic in Drop
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.close_for_record_appends();
            }));
        }
    }
}

/// Information about records in a batch (max timestamp and shallow offset).
///
/// Corresponds to Java's `MemoryRecordsBuilder.RecordsInfo`.
#[derive(Clone, Debug)]
pub struct RecordsInfo {
    /// The maximum timestamp in the batch.
    pub max_timestamp: i64,
    /// The shallow offset of the record with the maximum timestamp.
    pub shallow_offset_of_max_timestamp: i64,
}

impl RecordsInfo {
    /// Create a new `RecordsInfo`.
    pub fn new(max_timestamp: i64, shallow_offset_of_max_timestamp: i64) -> Self {
        Self { max_timestamp, shallow_offset_of_max_timestamp }
    }
}
