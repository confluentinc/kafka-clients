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

//! A `Records` implementation backed by an in-memory byte buffer.
//!
//! Translated from `org.apache.kafka.common.record.MemoryRecords`.
//!
//! This type is used for reading or modifying in-place an existing buffer of
//! record batches. To create a new buffer, use [`super::MemoryRecordsBuilder`]
//! or the convenience methods on this struct (e.g., [`MemoryRecords::build_with_records`]).

use crate::common::record::compression_type::CompressionType;
use crate::common::record::default_record_batch::{self, DefaultRecordBatch};
use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
use crate::common::record::timestamp_type::TimestampType;
use crate::common::record::{
    CURRENT_MAGIC_VALUE, LOG_OVERHEAD, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
    NO_TIMESTAMP, SIZE_OFFSET, SimpleRecord,
};

/// A `Records` implementation backed by a byte buffer.
///
/// Translated from `org.apache.kafka.common.record.MemoryRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRecords {
    /// The underlying buffer holding the serialized batch data.
    buffer: Vec<u8>,
}

impl MemoryRecords {
    /// Create `MemoryRecords` from a raw buffer.
    ///
    /// Corresponds to `MemoryRecords.readableRecords(ByteBuffer)`.
    pub fn from_buffer(buffer: Vec<u8>) -> Self {
        MemoryRecords { buffer }
    }

    /// Returns the total size of the records in bytes.
    pub fn size_in_bytes(&self) -> i32 {
        self.buffer.len() as i32
    }

    /// Returns a reference to the underlying buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Consume this `MemoryRecords` and return the underlying buffer.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }

    /// Parse the batches in this records buffer.
    ///
    /// Each batch consists of a 12-byte header (8-byte offset + 4-byte size) followed
    /// by the batch data.
    ///
    /// Only v2 batches are supported.
    pub fn batches(&self) -> Vec<DefaultRecordBatch> {
        let mut result = Vec::new();
        let buf = &self.buffer;
        let mut pos = 0;

        while pos + LOG_OVERHEAD <= buf.len() {
            // Read the batch size from the 4-byte field at offset 8
            let size_bytes: [u8; 4] = buf[pos + SIZE_OFFSET..pos + SIZE_OFFSET + 4].try_into().unwrap();
            let batch_size = i32::from_be_bytes(size_bytes);

            let total_batch_size = LOG_OVERHEAD + batch_size as usize;
            if pos + total_batch_size > buf.len() {
                break; // Partial batch, stop
            }

            let batch_buf = buf[pos..pos + total_batch_size].to_vec();
            result.push(DefaultRecordBatch::new(batch_buf));

            pos += total_batch_size;
        }

        result
    }

    /// Convenience builder: create `MemoryRecords` with the given simple records.
    ///
    /// This is a simplified version of the Java `MemoryRecords.withRecords()` factory.
    #[allow(clippy::too_many_arguments)]
    pub fn build_with_records(
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
        records: &[SimpleRecord],
    ) -> Self {
        if records.is_empty() {
            return MemoryRecords::from_buffer(Vec::new());
        }

        // Estimate the size
        let size_estimate = default_record_batch::RECORD_BATCH_OVERHEAD
            + records
                .iter()
                .map(|r| {
                    crate::common::record::default_record::DefaultRecord::record_size_upper_bound(
                        r.key.as_deref(),
                        r.value.as_deref(),
                        &r.headers,
                    )
                })
                .sum::<usize>();

        let actual_log_append_time = if timestamp_type == TimestampType::LogAppendTime {
            if log_append_time == NO_TIMESTAMP {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            } else {
                log_append_time
            }
        } else {
            log_append_time
        };

        let mut builder = MemoryRecordsBuilder::new(
            size_estimate,
            magic,
            compression_type,
            timestamp_type,
            base_offset,
            actual_log_append_time,
            producer_id,
            producer_epoch,
            base_sequence,
            is_transactional,
            is_control_batch,
            partition_leader_epoch,
            size_estimate,
        )
        .unwrap();

        for record in records {
            builder.append_simple_record(record).unwrap();
        }

        builder.build().unwrap()
    }

    /// Convenience: create MemoryRecords with NONE compression and simple defaults.
    pub fn with_records(base_offset: i64, records: &[SimpleRecord]) -> Self {
        Self::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            base_offset,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            records,
        )
    }
}

impl std::fmt::Display for MemoryRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryRecords(size={})", self.size_in_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::{
        CURRENT_MAGIC_VALUE, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, RecordHeader,
    };

    /// Translated from MemoryRecordsTest.testIterator (v2, CompressionType::None only)
    #[test]
    fn test_iterator_v2_none() {
        let partition_leader_epoch: i32 = 998;
        let pid: i64 = 134234;
        let epoch: i16 = 28;
        let first_sequence: i32 = 777;
        let base_offset: i64 = 0;

        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            SimpleRecord::with_timestamp(4, None, Some(b"4".to_vec())),
            SimpleRecord::with_timestamp(5, Some(b"d".to_vec()), None),
            SimpleRecord::with_timestamp(6, None, None),
        ];

        let mut builder = MemoryRecordsBuilder::new(
            1024,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            base_offset,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            pid,
            epoch,
            first_sequence,
            false,
            false,
            partition_leader_epoch,
            1024,
        )
        .unwrap();

        for record in &records {
            builder.append_simple_record(record).unwrap();
        }

        let mem_records = builder.build().unwrap();

        // Iterate twice to verify idempotence
        for _ in 0..2 {
            let batches = mem_records.batches();
            for batch in &batches {
                assert!(batch.is_valid());
                assert_eq!(CompressionType::None, batch.compression_type());
                assert_eq!(base_offset, batch.base_offset());
                assert_eq!(pid, batch.producer_id());
                assert_eq!(epoch, batch.producer_epoch());
                assert_eq!(first_sequence, batch.base_sequence());
                assert_eq!(partition_leader_epoch, batch.partition_leader_epoch());
                assert_eq!(Some(records.len() as i32), batch.count_or_null());
                assert_eq!(TimestampType::CreateTime, batch.timestamp_type());
                assert_eq!(records[records.len() - 1].timestamp, batch.max_timestamp());

                let all_records = batch.iter_records().unwrap();
                assert_eq!(records.len(), all_records.len());

                for (i, record) in all_records.iter().enumerate() {
                    record.ensure_valid();
                    assert_eq!(base_offset + i as i64, record.offset());
                    assert_eq!(records[i].timestamp, record.timestamp());
                    assert_eq!(records[i].key.as_deref(), record.key());
                    assert_eq!(records[i].value.as_deref(), record.value());
                }
            }
        }
    }

    /// Translated from MemoryRecordsTest.testIterator (v2, CompressionType::None, firstOffset=57)
    #[test]
    fn test_iterator_v2_none_offset_57() {
        let partition_leader_epoch: i32 = 998;
        let pid: i64 = 134234;
        let epoch: i16 = 28;
        let first_sequence: i32 = 777;
        let base_offset: i64 = 57;

        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
            SimpleRecord::with_timestamp(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
        ];

        let mut builder = MemoryRecordsBuilder::new(
            1024,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            base_offset,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            pid,
            epoch,
            first_sequence,
            false,
            false,
            partition_leader_epoch,
            1024,
        )
        .unwrap();

        for record in &records {
            builder.append_simple_record(record).unwrap();
        }

        let mem_records = builder.build().unwrap();
        let batches = mem_records.batches();
        assert_eq!(1, batches.len());

        let batch = &batches[0];
        assert!(batch.is_valid());
        assert_eq!(base_offset, batch.base_offset());
        assert_eq!(base_offset + 2, batch.last_offset());

        let all_records = batch.iter_records().unwrap();
        for (i, record) in all_records.iter().enumerate() {
            assert_eq!(base_offset + i as i64, record.offset());
            assert_eq!(first_sequence + i as i32, record.sequence());
        }
    }

    /// Translated from MemoryRecordsTest.testHasRoomForMethod (v2 only)
    #[test]
    fn test_has_room_for() {
        let mut builder = MemoryRecordsBuilder::new(
            1024,
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
            1024,
        )
        .unwrap();

        // First record always fits
        assert!(builder.has_room_for(0, Some(b"key"), Some(b"value"), &[]));

        builder.append(0, Some(b"key"), Some(b"value"), &[]).unwrap();

        // Should still have room with a large limit
        assert!(builder.has_room_for(1, Some(b"key"), Some(b"value"), &[]));
    }

    /// Translated from MemoryRecordsTest.testValidBytes (v2 only)
    #[test]
    fn test_valid_bytes() {
        let records = vec![
            SimpleRecord::with_timestamp(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
            SimpleRecord::with_timestamp(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
        ];
        let mem_records = MemoryRecords::with_records(0, &records);
        let batches = mem_records.batches();
        assert_eq!(1, batches.len());

        // valid_bytes should equal size_in_bytes for a single well-formed batch
        let mut valid_bytes = 0;
        for batch in &batches {
            valid_bytes += batch.size_in_bytes();
        }
        assert_eq!(mem_records.size_in_bytes(), valid_bytes);
    }

    /// Translated from MemoryRecordsTest.testEquals
    #[test]
    fn test_equals() {
        let records1 = MemoryRecords::with_records(
            0,
            &[SimpleRecord::with_timestamp(
                1,
                Some(b"a".to_vec()),
                Some(b"1".to_vec()),
            )],
        );
        let records2 = MemoryRecords::with_records(
            0,
            &[SimpleRecord::with_timestamp(
                1,
                Some(b"a".to_vec()),
                Some(b"1".to_vec()),
            )],
        );
        assert_eq!(records1, records2);
    }

    /// Translated from MemoryRecordsTest: verify with headers
    #[test]
    fn test_with_headers() {
        let headers = vec![
            RecordHeader::new("foo", Some(b"value".to_vec())),
            RecordHeader::new("bar", None),
        ];

        let records = vec![SimpleRecord::new(
            1,
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            headers.clone(),
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
            &records,
        );

        let batches = mem_records.batches();
        assert_eq!(1, batches.len());

        let all_records = batches[0].iter_records().unwrap();
        assert_eq!(1, all_records.len());
        assert_eq!(&headers, all_records[0].headers());
    }
}
