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
//! Utility functions for record batch operations.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.AbstractRecords`.

use crate::common::header::RecordHeader;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::DefaultRecord;
use crate::common::record::internal::DefaultRecordBatch;
use crate::common::record::internal::RecordBatch;
use crate::common::record::internal::SimpleRecord;

/// Translates the Java static-utility class `org.apache.kafka.common.record.internal.AbstractRecords`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub(crate) struct AbstractRecords;

impl AbstractRecords {
    /// LOG_OVERHEAD is the number of bytes before the record batch header starts
    /// (base_offset: 8 bytes + length: 4 bytes = 12 bytes).
    pub const LOG_OVERHEAD: usize = RecordBatch::BASE_OFFSET_LENGTH + RecordBatch::LENGTH_LENGTH;

    /// The number of bytes in the header up to and including the magic byte.
    /// Used to validate that a buffer contains at least the minimum header.
    pub const HEADER_SIZE_UP_TO_MAGIC: usize = RecordBatch::BASE_OFFSET_LENGTH
        + RecordBatch::LENGTH_LENGTH
        + RecordBatch::PARTITION_LEADER_EPOCH_LENGTH
        + RecordBatch::MAGIC_LENGTH;

    /// Estimate the size of records in bytes for the given parameters.
    ///
    /// For magic v2+, uses `DefaultRecordBatch::size_in_bytes_of_simple_records`.
    /// For older versions, this is not supported (would require LegacyRecord).
    ///
    /// Corresponds to Java's `AbstractRecords.estimateSizeInBytes(byte, CompressionType, Iterable<SimpleRecord>)`.
    pub fn estimate_size_in_bytes(magic: i8, compression_type: CompressionType, records: &[SimpleRecord]) -> usize {
        let size = if magic <= RecordBatch::MAGIC_VALUE_V1 {
            // Legacy records not supported in this implementation
            // For v2-only producer, this path should not be reached
            0
        } else {
            DefaultRecordBatch::size_in_bytes_of_simple_records(records)
        };
        Self::estimate_compressed_size_in_bytes(size, compression_type)
    }

    /// Get an upper bound estimate on the batch size needed to hold a record with
    /// the given fields. This is only an estimate because it does not take into
    /// account overhead from the compression algorithm.
    ///
    /// Corresponds to Java's `AbstractRecords.estimateSizeInBytesUpperBound`.
    pub fn estimate_size_in_bytes_upper_bound(
        magic: i8,
        _compression_type: CompressionType,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> i32 {
        if magic >= RecordBatch::MAGIC_VALUE_V2 {
            DefaultRecordBatch::estimate_batch_size_upper_bound(key, value, headers)
        } else {
            // Legacy records not supported
            0
        }
    }

    /// Return the size of the record batch header.
    ///
    /// For V2+, this is `DefaultRecordBatch::RECORD_BATCH_OVERHEAD`.
    /// For older versions with no compression, returns 0.
    /// For older versions with compression, would return LOG_OVERHEAD + LegacyRecord overhead.
    ///
    /// Corresponds to Java's `AbstractRecords.recordBatchHeaderSizeInBytes`.
    pub fn record_batch_header_size_in_bytes(magic: i8, compression_type: CompressionType) -> usize {
        if magic > RecordBatch::MAGIC_VALUE_V1 {
            RecordBatch::RECORD_BATCH_OVERHEAD
        } else if compression_type != CompressionType::None {
            // Legacy compressed: LOG_OVERHEAD + LegacyRecord overhead
            // Not fully supported, but provide a reasonable value
            Self::LOG_OVERHEAD
        } else {
            0
        }
    }

    /// Estimate the compressed size from uncompressed size.
    ///
    /// For no compression, returns the size as-is.
    /// For compression, returns `min(max(size / 2, 1024), 1 << 16)`.
    ///
    /// Corresponds to Java's `AbstractRecords.estimateCompressedSizeInBytes`.
    fn estimate_compressed_size_in_bytes(size: usize, compression_type: CompressionType) -> usize {
        if compression_type == CompressionType::None {
            size
        } else {
            (size / 2).clamp(1024, 1 << 16)
        }
    }

    /// Compute the size of a record batch containing the given records.
    ///
    /// This overload takes `DefaultRecord`-implementing records (with offsets).
    ///
    /// Corresponds to Java's `DefaultRecordBatch.sizeInBytes(long, Iterable<Record>)`.
    pub fn size_in_bytes_with_records(
        base_offset: i64,
        records: &[impl crate::common::record::internal::Record],
    ) -> usize {
        if records.is_empty() {
            return 0;
        }

        let mut size = RecordBatch::RECORD_BATCH_OVERHEAD;
        let mut base_timestamp: Option<i64> = None;
        for record in records {
            let offset_delta = (record.offset() - base_offset) as i32;
            if base_timestamp.is_none() {
                base_timestamp = Some(record.timestamp());
            }
            let timestamp_delta = record.timestamp() - base_timestamp.unwrap();
            size += DefaultRecord::size_in_bytes_with_slices(
                offset_delta,
                timestamp_delta,
                record.key(),
                record.value(),
                record.headers(),
            ) as usize;
        }
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_overhead() {
        assert_eq!(AbstractRecords::LOG_OVERHEAD, 12);
    }

    #[test]
    fn test_header_size_up_to_magic() {
        // 8 (base_offset) + 4 (length) + 4 (partition_leader_epoch) + 1 (magic) = 17
        assert_eq!(AbstractRecords::HEADER_SIZE_UP_TO_MAGIC, 17);
    }

    #[test]
    fn test_record_batch_header_size() {
        assert_eq!(
            AbstractRecords::record_batch_header_size_in_bytes(RecordBatch::MAGIC_VALUE_V2, CompressionType::None),
            RecordBatch::RECORD_BATCH_OVERHEAD
        );
        assert_eq!(
            AbstractRecords::record_batch_header_size_in_bytes(RecordBatch::MAGIC_VALUE_V2, CompressionType::Gzip),
            RecordBatch::RECORD_BATCH_OVERHEAD
        );
        assert_eq!(
            AbstractRecords::record_batch_header_size_in_bytes(RecordBatch::MAGIC_VALUE_V0, CompressionType::None),
            0
        );
    }

    #[test]
    fn test_estimate_compressed_size() {
        // No compression: returns size as-is
        assert_eq!(
            AbstractRecords::estimate_compressed_size_in_bytes(1000, CompressionType::None),
            1000
        );

        // With compression: min(max(size/2, 1024), 65536)
        assert_eq!(
            AbstractRecords::estimate_compressed_size_in_bytes(1000, CompressionType::Gzip),
            1024
        );
        assert_eq!(
            AbstractRecords::estimate_compressed_size_in_bytes(4000, CompressionType::Gzip),
            2000
        );
        assert_eq!(
            AbstractRecords::estimate_compressed_size_in_bytes(200000, CompressionType::Gzip),
            65536
        );
    }
}
