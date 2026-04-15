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
use crate::common::kafka_error::KafkaError;
use crate::common::protocol::Errors;
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
    /// Returns `Ok(None)` if the buffer does not contain enough bytes for a
    /// header. Returns `Err(CorruptMessage)` if the record size is invalid
    /// (too small, too large, or negative) or if the magic byte is invalid.
    ///
    /// Corresponds to Java's `MemoryRecords.firstBatchSize()` which delegates
    /// to `ByteBufferLogInputStream.nextBatchSize()`.
    pub fn first_batch_size(&self) -> Result<Option<usize>, KafkaError> {
        // Minimum overhead for LegacyRecord v0:
        //   CRC(4) + Magic(1) + Attributes(1) + KeySize(4) + ValueSize(4) = 14
        const LEGACY_RECORD_OVERHEAD_V0: i32 = 14;

        if self.buffer.len() < LOG_OVERHEAD {
            return Ok(None);
        }

        // Read the record size (length) field
        let record_size = i32::from_be_bytes(
            self.buffer[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4]
                .try_into()
                .map_err(|_| KafkaError::with_message(Errors::CorruptMessage, "Failed to read record size"))?,
        );

        // Validate minimum record size (V0 has the smallest overhead)
        if record_size < LEGACY_RECORD_OVERHEAD_V0 {
            return Err(KafkaError::with_message(
                Errors::CorruptMessage,
                format!(
                    "Record size {} is less than the minimum record overhead ({})",
                    record_size, LEGACY_RECORD_OVERHEAD_V0
                ),
            ));
        }

        // Validate maximum message size (use i32::MAX like Java's Integer.MAX_VALUE)
        // Java passes Integer.MAX_VALUE as maxMessageSize from firstBatchSize(),
        // so this check only catches negative values that wrapped or truly
        // enormous sizes. Since we already checked >= LEGACY_RECORD_OVERHEAD_V0
        // and record_size is i32, the max check here matches Java behavior.

        if self.buffer.len() < abstract_records::HEADER_SIZE_UP_TO_MAGIC {
            return Ok(None);
        }

        // Validate magic byte
        let magic = self.buffer[RecordBatch::MAGIC_OFFSET] as i8;
        if !(0..=RecordBatch::CURRENT_MAGIC_VALUE).contains(&magic) {
            return Err(KafkaError::with_message(
                Errors::CorruptMessage,
                format!("Invalid magic found in record: {}", magic),
            ));
        }

        Ok(Some(LOG_OVERHEAD + record_size as usize))
    }

    /// Returns a slice of the records data at the given position and size.
    ///
    /// The `size` parameter is clamped to the available bytes from `position`
    /// to the end of the buffer, matching Java's
    /// `MemoryRecords.slice(int, int)` which uses
    /// `Math.min(size, buffer.limit() - position)`.
    pub fn slice(&self, position: usize, size: usize) -> MemoryRecords {
        assert!(
            position <= self.buffer.len(),
            "Slice from position {} exceeds end position",
            position
        );
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

    /// Create a builder using a pre-allocated buffer.
    ///
    /// This is the equivalent of Java's `MemoryRecords.builder(ByteBuffer, ...)` overload
    /// that accepts an existing buffer (e.g., from a buffer pool) instead of allocating a
    /// new one.
    pub fn builder_with_buffer(
        buffer: Vec<u8>,
        magic: i8,
        compression: Compression,
        timestamp_type: TimestampType,
        base_offset: i64,
    ) -> MemoryRecordsBuilder {
        let write_limit = buffer.capacity();
        let log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            current_time_millis()
        } else {
            RecordBatch::NO_TIMESTAMP
        };
        MemoryRecordsBuilder::new_default(
            buffer,
            0,
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
            write_limit,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::internals::RecordHeader as HeaderImpl;
    use crate::common::record::default_record_batch::DefaultRecordBatch;
    use crate::common::record::record_trait::Record;

    /// All compression types to test with.
    fn all_compressions() -> Vec<Compression> {
        vec![
            Compression::none(),
            Compression::gzip(),
            Compression::snappy(),
            Compression::lz4(),
            Compression::zstd(),
        ]
    }

    /// Corresponds to Java's `MemoryRecordsTest.testIterator`.
    #[test]
    fn test_iterator() {
        let log_append_time = current_time_millis();

        for compression in all_compressions() {
            let first_offset = 0_i64;
            let pid = 134234_i64;
            let epoch = 28_i16;
            let first_sequence = 777_i32;
            let partition_leader_epoch = 998;

            let records = vec![
                SimpleRecord::new_with_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::new_with_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::new_with_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
                SimpleRecord::new_with_key_value(4, None, Some(b"4".to_vec())),
                SimpleRecord::new_with_key_value(5, Some(b"d".to_vec()), None),
                SimpleRecord::new_with_key_value(6, None, None),
            ];

            let mut builder = MemoryRecordsBuilder::new_default(
                Vec::with_capacity(1024),
                0,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                first_offset,
                log_append_time,
                pid,
                epoch,
                first_sequence,
                false,
                false,
                partition_leader_epoch,
                1024,
            );
            for record in &records {
                builder.append_simple(record);
            }
            let memory_records = builder.build();

            // Iterate twice to verify idempotency
            for _iteration in 0..2 {
                let mut total = 0;
                for batch in memory_records.batches() {
                    assert!(batch.is_valid());
                    assert_eq!(compression.compression_type(), batch.compression_type());
                    assert_eq!(first_offset + total as i64, batch.base_offset());

                    assert_eq!(pid, batch.producer_id());
                    assert_eq!(epoch, batch.producer_epoch());
                    assert_eq!(first_sequence + total as i32, batch.base_sequence());
                    assert_eq!(partition_leader_epoch, batch.partition_leader_epoch());
                    assert_eq!(Some(records.len() as i32), batch.count_or_null());
                    assert_eq!(TimestampType::CreateTime, batch.timestamp_type());
                    assert_eq!(records[records.len() - 1].timestamp(), batch.max_timestamp());

                    let mut record_count = 0;
                    for record in batch.iter_records().unwrap() {
                        record.ensure_valid().unwrap();
                        assert!(record.has_magic(batch.magic()));
                        assert!(!record.is_compressed());
                        assert_eq!(first_offset + total as i64, record.offset());
                        assert_eq!(records[total].key(), record.key());
                        assert_eq!(records[total].value(), record.value());
                        assert_eq!(first_sequence + total as i32, record.sequence());
                        assert!(!record.has_timestamp_type(TimestampType::LogAppendTime));
                        assert_eq!(records[total].timestamp(), record.timestamp());
                        assert!(!record.has_timestamp_type(TimestampType::NoTimestampType));
                        // For v2, has_timestamp_type(CreateTime) returns false
                        assert!(!record.has_timestamp_type(TimestampType::CreateTime));

                        total += 1;
                        record_count += 1;
                    }

                    assert_eq!(batch.base_offset() + record_count as i64 - 1, batch.last_offset());
                }
            }
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testHasRoomForMethod`.
    #[test]
    fn test_has_room_for_method() {
        for compression in all_compressions() {
            let mut builder = MemoryRecords::builder_with_magic(
                1024,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
            );
            builder.append_kv(0, Some(b"a"), Some(b"1"));
            assert!(builder.has_room_for(1, Some(b"b"), Some(b"2"), RecordBatch::EMPTY_HEADERS));
            builder.close();
            assert!(!builder.has_room_for(1, Some(b"b"), Some(b"2"), RecordBatch::EMPTY_HEADERS));
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testHasRoomForMethodWithHeaders`.
    #[test]
    fn test_has_room_for_method_with_headers() {
        let log_append_time = current_time_millis();

        for compression in all_compressions() {
            let mut builder = MemoryRecords::builder_with_magic(
                120,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::CreateTime,
                0,
            );
            builder.append_kv(log_append_time, Some(b"key"), Some(b"value"));

            let mut headers = Vec::new();
            for _ in 0..10 {
                headers.push(HeaderImpl::new("hello".to_string(), Some(b"world.world".to_vec())));
            }

            // A record without headers should fit
            assert!(builder.has_room_for(log_append_time, Some(b"key"), Some(b"value"), RecordBatch::EMPTY_HEADERS,));
            // A record with many headers should not fit (for v2)
            assert!(!builder.has_room_for(log_append_time, Some(b"key"), Some(b"value"), &headers));
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testChecksum` (v2 only).
    #[test]
    fn test_checksum_v2() {
        // We get reasonable coverage with uncompressed and one compression type
        for (compression, expected_checksum) in &[
            (Compression::none(), 3851219455_u32),
            (Compression::lz4(), 2745969314_u32),
        ] {
            let records = vec![
                SimpleRecord::new_with_key_value(283843, Some(b"key1".to_vec()), Some(b"value1".to_vec())),
                SimpleRecord::new_with_key_value(1234, Some(b"key2".to_vec()), Some(b"value2".to_vec())),
            ];
            let mem_records =
                MemoryRecords::with_records_magic(RecordBatch::MAGIC_VALUE_V2, compression.clone(), &records);
            let batch = mem_records.batches().next().unwrap();
            assert_eq!(
                *expected_checksum,
                batch.checksum(),
                "Unexpected checksum for compression {:?}",
                compression.compression_type()
            );
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testWithRecords`.
    #[test]
    fn test_with_records() {
        for compression in all_compressions() {
            let mem_records = MemoryRecords::with_records_magic(
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                &[SimpleRecord::new_with_key_value(
                    10,
                    Some(b"key1".to_vec()),
                    Some(b"value1".to_vec()),
                )],
            );
            let batch = mem_records.batches().next().unwrap();
            let record = batch.iter_records().unwrap().into_iter().next().unwrap();
            assert_eq!(Some(b"key1".as_slice()), record.key());
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testNextBatchSize` (v2 only).
    #[test]
    fn test_first_batch_size() {
        let log_append_time = current_time_millis();

        for compression in all_compressions() {
            let mut builder = MemoryRecords::builder_with_log_append_time(
                2048,
                RecordBatch::MAGIC_VALUE_V2,
                compression.clone(),
                TimestampType::LogAppendTime,
                0,
                log_append_time,
            );
            builder.append_kv(10, None, Some(b"abc"));
            let records = builder.build();

            let size = records.size_in_bytes();
            assert_eq!(Some(size), records.first_batch_size().unwrap());

            // size not in buffer (only 1 byte)
            let short_records = MemoryRecords::new(records.buffer()[..1].to_vec());
            assert_eq!(None, short_records.first_batch_size().unwrap());

            // magic not in buffer (only LOG_OVERHEAD bytes = 12)
            let short_records = MemoryRecords::new(records.buffer()[..LOG_OVERHEAD].to_vec());
            assert_eq!(None, short_records.first_batch_size().unwrap());

            // payload not in buffer, but header up to magic is present
            let short_records =
                MemoryRecords::new(records.buffer()[..abstract_records::HEADER_SIZE_UP_TO_MAGIC].to_vec());
            assert_eq!(Some(size), short_records.first_batch_size().unwrap());

            // Invalid magic byte (10) should return CorruptMessage error
            let mut corrupt_magic_buf = records.buffer().to_vec();
            corrupt_magic_buf[RecordBatch::MAGIC_OFFSET] = 10;
            let corrupt_records = MemoryRecords::new(corrupt_magic_buf);
            let err = corrupt_records.first_batch_size().unwrap_err();
            assert_eq!(err.error(), Errors::CorruptMessage);

            // Invalid record size (set LSB of size field to 0, making it too small)
            let mut corrupt_size_buf = records.buffer().to_vec();
            corrupt_size_buf[RecordBatch::LENGTH_OFFSET + 3] = 0;
            let corrupt_records = MemoryRecords::new(corrupt_size_buf);
            let err = corrupt_records.first_batch_size().unwrap_err();
            assert_eq!(err.error(), Errors::CorruptMessage);
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSlice` (v2 only).
    #[test]
    fn test_slice() {
        for compression in all_compressions() {
            // Create records with multiple batches
            let mut buf = Vec::new();
            for (offset, count) in &[(0_i64, 3_usize), (6_i64, 8_usize), (15_i64, 4_usize)] {
                let mut builder = MemoryRecords::builder_with_magic(
                    1024,
                    RecordBatch::MAGIC_VALUE_V2,
                    compression.clone(),
                    TimestampType::CreateTime,
                    *offset,
                );
                for i in 0..*count {
                    builder.append_with_offset_bytes(
                        *offset + i as i64,
                        0,
                        Some(format!("key{}", i).as_bytes()),
                        Some(format!("val{}", i).as_bytes()),
                    );
                }
                let batch_records = builder.build();
                buf.extend_from_slice(batch_records.buffer());
            }

            let records = MemoryRecords::new(buf);

            // Test slicing from start
            let sliced = records.slice(0, records.size_in_bytes());
            assert_eq!(records.size_in_bytes(), sliced.size_in_bytes());
            assert_eq!(records.valid_bytes(), sliced.valid_bytes());

            let items: Vec<_> = records.batches().collect();

            // Test slicing past first batch
            let first_size = items[0].size_in_bytes();
            let sliced = records.slice(first_size, records.size_in_bytes() - first_size);
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());

            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read from second message and size is past the end of the file
            // (Java: records.slice(first.sizeInBytes(), records.sizeInBytes()))
            let sliced = records.slice(first_size, records.size_in_bytes());
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read from second message and position + size overflows
            // (Java: records.slice(first.sizeInBytes(), Integer.MAX_VALUE))
            let sliced = records.slice(first_size, usize::MAX);
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read a single batch starting from second batch
            let second_size = items[1].size_in_bytes();
            let sliced = records.slice(first_size, second_size);
            assert_eq!(second_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(1, sliced_batches.len());

            // Read from second message and size is past the end on an already-sliced view
            // (Java: records.slice(1, records.sizeInBytes() - 1)
            //               .slice(first.sizeInBytes() - 1, records.sizeInBytes()))
            let sliced = records
                .slice(1, records.size_in_bytes() - 1)
                .slice(first_size - 1, records.size_in_bytes());
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());

            // Read from second message and position + size overflows on already-sliced view
            // (Java: records.slice(1, records.sizeInBytes() - 1)
            //               .slice(first.sizeInBytes() - 1, Integer.MAX_VALUE))
            let sliced = records.slice(1, records.size_in_bytes() - 1).slice(first_size - 1, usize::MAX);
            assert_eq!(records.size_in_bytes() - first_size, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 1, sliced_batches.len());
            assert!(sliced.valid_bytes() <= sliced.size_in_bytes());
        }
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSliceEmptyRecords`.
    #[test]
    fn test_slice_empty_records() {
        let empty = MemoryRecords::empty();
        let sliced = empty.slice(0, 0);
        assert_eq!(0, sliced.size_in_bytes());
        assert_eq!(0, sliced.batches().count());
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSliceInvalidPosition`.
    #[test]
    #[should_panic(expected = "Slice from position")]
    fn test_slice_invalid_position() {
        let records = MemoryRecords::with_records(
            Compression::none(),
            &[SimpleRecord::new_with_key_value(
                1,
                Some(b"k".to_vec()),
                Some(b"v".to_vec()),
            )],
        );
        records.slice(records.size_in_bytes() + 1, records.size_in_bytes());
    }

    /// Corresponds to Java's `MemoryRecordsTest.testSliceForAlreadySlicedMemoryRecords`.
    #[test]
    fn test_slice_for_already_sliced_memory_records() {
        for compression in all_compressions() {
            // Create records with multiple batches
            let mut buf = Vec::new();
            for (offset, count) in &[
                (0_i64, 5_usize),
                (5_i64, 10_usize),
                (15_i64, 12_usize),
                (27_i64, 4_usize),
            ] {
                let mut builder = MemoryRecords::builder_with_magic(
                    1024,
                    RecordBatch::MAGIC_VALUE_V2,
                    compression.clone(),
                    TimestampType::CreateTime,
                    *offset,
                );
                for i in 0..*count {
                    builder.append_with_offset_bytes(
                        *offset + i as i64,
                        0,
                        Some(format!("key{}", i).as_bytes()),
                        Some(format!("val{}", i).as_bytes()),
                    );
                }
                let batch_records = builder.build();
                buf.extend_from_slice(batch_records.buffer());
            }
            let records = MemoryRecords::new(buf);

            let items: Vec<DefaultRecordBatch> = records.batches().collect();

            // Slice from third batch
            let position: usize = items[0].size_in_bytes() + items[1].size_in_bytes();
            let sliced = records.slice(position, records.size_in_bytes() - position);
            assert_eq!(records.size_in_bytes() - position, sliced.size_in_bytes());
            let sliced_batches: Vec<_> = sliced.batches().collect();
            assert_eq!(items.len() - 2, sliced_batches.len());

            // Further slice from fourth batch
            let position2 = items[2].size_in_bytes();
            let final_sliced = sliced.slice(position2, sliced.size_in_bytes() - position2);
            assert_eq!(sliced.size_in_bytes() - position2, final_sliced.size_in_bytes());
            let final_batches: Vec<_> = final_sliced.batches().collect();
            assert_eq!(items.len() - 3, final_batches.len());
        }
    }

    // Note: filterTo tests (testFilterToPreservesPartitionLeaderEpoch, testFilterToEmptyBatchRetention,
    // testEmptyBatchRetention, testEmptyBatchDeletion, testBaseTimestampToDeleteHorizonConversion,
    // testFilterToBatchDiscard, testFilterToAlreadyCompactedLog, testFilterToPreservesProducerInfo,
    // testFilterToWithUndersizedBuffer, testFilterTo, testFilterToPreservesLogAppendTime) are
    // skipped because filterTo is not implemented in the Rust version. The filterTo method is
    // a server-side operation used for log compaction and not needed for the producer path.

    // Note: testBuildEndTxnMarker and testBuildLeaderChangeMessage are skipped because
    // EndTransactionMarker, ControlRecordType, and LeaderChangeMessage/ControlRecordUtils
    // are not yet implemented.

    // Note: testUnsupportedCompress is skipped because it tests magic v0/v1 which
    // we do not support in the Rust producer path.
}
