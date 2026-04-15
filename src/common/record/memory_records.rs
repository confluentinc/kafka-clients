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

//! A records implementation backed by a byte buffer.
//!
//! This is used only for reading or modifying in-place an existing buffer of
//! record batches. To create a new buffer see [`MemoryRecordsBuilder`],
//! or one of the [`builder()`](MemoryRecords::builder) variants.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.MemoryRecords`.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::compress::Compression;
use crate::common::record::abstract_records::{self, LOG_OVERHEAD};
use crate::common::record::default_record::DefaultRecord;
use crate::common::record::default_record_batch::DefaultRecordBatch;
use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
use crate::common::record::record_batch::RecordBatch;
use crate::common::record::simple_record::SimpleRecord;
use crate::common::record::timestamp_type::TimestampType;

/// A records implementation backed by a byte buffer.
///
/// Contains one or more complete record batches in serialized form.
///
/// Corresponds to Java's `org.apache.kafka.common.record.MemoryRecords`.
#[derive(Clone, Debug)]
pub struct MemoryRecords {
    buffer: Vec<u8>,
}

impl MemoryRecords {
    /// Create a new `MemoryRecords` wrapping the given buffer.
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }

    /// Create an empty `MemoryRecords`.
    pub fn empty() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Create a `MemoryRecords` from a byte slice (copies the data).
    pub fn readable_records(data: &[u8]) -> Self {
        Self { buffer: data.to_vec() }
    }

    /// Returns the total size of this records set in bytes.
    pub fn size_in_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Returns a mutable reference to the underlying buffer.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Returns an iterator over the batches in this records set.
    ///
    /// Each batch is a `DefaultRecordBatch` containing the full batch header
    /// and record data.
    pub fn batches(&self) -> BatchIterator<'_> {
        BatchIterator { data: &self.buffer, pos: 0 }
    }

    /// Returns an iterator over all individual records across all batches.
    pub fn records(&self) -> impl Iterator<Item = DefaultRecord> + '_ {
        self.batches().flat_map(|batch| batch.iter_records().unwrap_or_default())
    }

    /// The total number of valid bytes (excluding any partial, trailing data).
    pub fn valid_bytes(&self) -> usize {
        let mut bytes = 0;
        for batch in self.batches() {
            bytes += batch.size_in_bytes();
        }
        bytes
    }

    /// Validates the header of the first batch and returns batch size.
    ///
    /// Returns `None` if the buffer does not contain enough bytes for a header.
    pub fn first_batch_size(&self) -> Option<usize> {
        if self.buffer.len() < abstract_records::HEADER_SIZE_UP_TO_MAGIC {
            return None;
        }
        // Read the length field
        let length = i32::from_be_bytes(
            self.buffer[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4]
                .try_into()
                .ok()?,
        );
        Some(LOG_OVERHEAD + length as usize)
    }

    /// Returns a slice of the records data at the given position and size.
    pub fn slice(&self, position: usize, size: usize) -> MemoryRecords {
        assert!(
            position <= self.buffer.len(),
            "Slice from position {} exceeds end position",
            position
        );
        assert!(size <= self.buffer.len(), "Invalid size: {}", size);
        let available_bytes = size.min(self.buffer.len() - position);
        MemoryRecords::new(self.buffer[position..position + available_bytes].to_vec())
    }

    // -- Builder factory methods --

    /// Create a builder with default parameters.
    pub fn builder(
        initial_capacity: usize,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_with_magic(
            initial_capacity,
            RecordBatch::CURRENT_MAGIC_VALUE,
            compression,
            timestamp_type,
            base_offset,
        )
    }

    /// Create a builder with a specific magic value.
    pub fn builder_with_magic(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };
        Self::builder_full(
            initial_capacity,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            initial_capacity,
        )
    }

    /// Create a builder with a max size limit.
    pub fn builder_with_max_size(
        initial_capacity: usize,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        max_size: usize,
    ) -> MemoryRecordsBuilder {
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };
        Self::builder_full(
            initial_capacity,
            RecordBatch::CURRENT_MAGIC_VALUE,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            max_size,
        )
    }

    /// Create a builder with magic, compression, timestamp type, and log append time.
    pub fn builder_with_log_append_time(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
    ) -> MemoryRecordsBuilder {
        Self::builder_full(
            initial_capacity,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            initial_capacity,
        )
    }

    /// Create a builder with producer state.
    #[allow(clippy::too_many_arguments)]
    pub fn builder_with_producer(
        initial_capacity: usize,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
        log_append_time: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> MemoryRecordsBuilder {
        Self::builder_full(
            initial_capacity,
            magic,
            compression,
            timestamp_type,
            base_offset,
            log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            initial_capacity,
        )
    }

    /// Create a builder with full parameters (no delete horizon).
    #[allow(clippy::too_many_arguments)]
    pub fn builder_full(
        initial_capacity: usize,
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
    ) -> MemoryRecordsBuilder {
        let buffer = Vec::with_capacity(initial_capacity);
        MemoryRecordsBuilder::new_default(
            buffer,
            0,
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
        )
    }

    /// Create a builder with full parameters including delete horizon.
    #[allow(clippy::too_many_arguments)]
    pub fn builder_full_with_delete_horizon(
        initial_capacity: usize,
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
    ) -> MemoryRecordsBuilder {
        let buffer = Vec::with_capacity(initial_capacity);
        MemoryRecordsBuilder::new(
            buffer,
            0,
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
            delete_horizon_ms,
        )
    }

    // -- Convenience factory methods for creating records directly --

    /// Create a `MemoryRecords` with the given records using default settings.
    pub fn with_records(compression: Compression, records: &[SimpleRecord]) -> MemoryRecords {
        Self::with_records_magic(RecordBatch::CURRENT_MAGIC_VALUE, compression, records)
    }

    /// Create a `MemoryRecords` with a specific magic value.
    pub fn with_records_magic(magic: i8, compression: Compression, records: &[SimpleRecord]) -> MemoryRecords {
        Self::with_records_at_offset(magic, 0, compression, TimestampType::CreateTime, records)
    }

    /// Create a `MemoryRecords` with records starting at a specific offset.
    pub fn with_records_at_offset(
        magic: i8,
        initial_offset: i64,
        compression: Compression,
        timestamp_type: TimestampType,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            magic,
            initial_offset,
            compression,
            timestamp_type,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            false,
            records,
        )
    }

    /// Create a `MemoryRecords` with records at a specific offset and partition leader epoch.
    pub fn with_records_at_offset_plep(
        initial_offset: i64,
        compression: Compression,
        partition_leader_epoch: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            RecordBatch::CURRENT_MAGIC_VALUE,
            initial_offset,
            compression,
            TimestampType::CreateTime,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            partition_leader_epoch,
            false,
            records,
        )
    }

    /// Create idempotent records.
    pub fn with_idempotent_records(
        compression: Compression,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            RecordBatch::CURRENT_MAGIC_VALUE,
            0,
            compression,
            TimestampType::CreateTime,
            producer_id,
            producer_epoch,
            base_sequence,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            false,
            records,
        )
    }

    /// Create transactional records.
    pub fn with_transactional_records(
        compression: Compression,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        Self::with_records_full(
            RecordBatch::CURRENT_MAGIC_VALUE,
            0,
            compression,
            TimestampType::CreateTime,
            producer_id,
            producer_epoch,
            base_sequence,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            true,
            records,
        )
    }

    /// Create a `MemoryRecords` with full parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn with_records_full(
        magic: i8,
        initial_offset: i64,
        compression: Compression,
        timestamp_type: TimestampType,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        partition_leader_epoch: i32,
        is_transactional: bool,
        records: &[SimpleRecord],
    ) -> MemoryRecords {
        if records.is_empty() {
            return MemoryRecords::empty();
        }

        let size_estimate = abstract_records::estimate_size_in_bytes(magic, compression.compression_type(), records);
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };

        let mut builder = MemoryRecordsBuilder::new_default(
            Vec::with_capacity(size_estimate),
            0,
            magic,
            compression,
            timestamp_type,
            initial_offset,
            log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            false,
            partition_leader_epoch,
            size_estimate,
        );

        for record in records {
            builder.append_simple(record);
        }

        builder.build()
    }
}

impl PartialEq for MemoryRecords {
    fn eq(&self, other: &Self) -> bool {
        self.buffer == other.buffer
    }
}

impl Eq for MemoryRecords {}

impl std::hash::Hash for MemoryRecords {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.buffer.hash(state);
    }
}

impl std::fmt::Display for MemoryRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryRecords(size={})", self.size_in_bytes())
    }
}

/// Iterator over record batches in a `MemoryRecords`.
pub struct BatchIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = DefaultRecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Need at least LOG_OVERHEAD bytes to read base_offset + length
        if self.pos + LOG_OVERHEAD > self.data.len() {
            return None;
        }

        // Read the batch length from the length field
        let length_bytes = &self.data[self.pos + RecordBatch::LENGTH_OFFSET..self.pos + RecordBatch::LENGTH_OFFSET + 4];
        let batch_length = i32::from_be_bytes(length_bytes.try_into().ok()?) as usize;
        let total_batch_size = LOG_OVERHEAD + batch_length;

        if self.pos + total_batch_size > self.data.len() {
            return None;
        }

        let batch_data = self.data[self.pos..self.pos + total_batch_size].to_vec();
        self.pos += total_batch_size;

        Some(DefaultRecordBatch::new(batch_data))
    }
}

/// Get the current time in milliseconds since epoch.
fn current_time_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
