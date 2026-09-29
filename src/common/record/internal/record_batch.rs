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

//! Record batch constants.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.RecordBatch` (interface)
//! and `org.apache.kafka.common.record.DefaultRecordBatch` (constants).
//!
//! This module defines the wire-protocol constants for record batch headers.
//! The full `RecordBatch` trait will be implemented in a later phase when
//! `DefaultRecordBatch` is translated.

use crate::common::header::RecordHeader;

/// Constants for record batch magic values and sentinel values.
///
/// Corresponds to Java's `org.apache.kafka.common.record.RecordBatch` interface constants.
pub struct RecordBatch;

impl RecordBatch {
    // -- Magic values ---------------------------------------------------------

    /// Magic value for record format version 0.
    pub const MAGIC_VALUE_V0: i8 = 0;
    /// Magic value for record format version 1.
    pub const MAGIC_VALUE_V1: i8 = 1;
    /// Magic value for record format version 2 (current).
    pub const MAGIC_VALUE_V2: i8 = 2;
    /// The current magic value.
    pub const CURRENT_MAGIC_VALUE: i8 = Self::MAGIC_VALUE_V2;

    // -- Sentinel values ------------------------------------------------------

    /// Timestamp value for records without a timestamp.
    pub const NO_TIMESTAMP: i64 = -1;
    /// Producer ID value for non-idempotent/non-transactional producers.
    pub const NO_PRODUCER_ID: i64 = -1;
    /// Producer epoch value for non-idempotent/non-transactional producers.
    pub const NO_PRODUCER_EPOCH: i16 = -1;
    /// Sequence number value for non-idempotent/non-transactional producers.
    pub const NO_SEQUENCE: i32 = -1;
    /// Unknown partition leader epoch (used when first created by the producer).
    pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;

    // -- Batch header field offsets (from DefaultRecordBatch) ------------------

    /// Offset of the base offset field in the batch header.
    pub const BASE_OFFSET_OFFSET: usize = 0;
    /// Length of the base offset field.
    pub const BASE_OFFSET_LENGTH: usize = 8;

    /// Offset of the batch length field.
    pub const LENGTH_OFFSET: usize = Self::BASE_OFFSET_OFFSET + Self::BASE_OFFSET_LENGTH;
    /// Length of the batch length field.
    pub const LENGTH_LENGTH: usize = 4;

    /// Offset of the partition leader epoch field.
    pub const PARTITION_LEADER_EPOCH_OFFSET: usize = Self::LENGTH_OFFSET + Self::LENGTH_LENGTH;
    /// Length of the partition leader epoch field.
    pub const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;

    /// Offset of the magic byte field.
    pub const MAGIC_OFFSET: usize = Self::PARTITION_LEADER_EPOCH_OFFSET + Self::PARTITION_LEADER_EPOCH_LENGTH;
    /// Length of the magic byte field.
    pub const MAGIC_LENGTH: usize = 1;

    /// Offset of the CRC field.
    pub const CRC_OFFSET: usize = Self::MAGIC_OFFSET + Self::MAGIC_LENGTH;
    /// Length of the CRC field.
    pub const CRC_LENGTH: usize = 4;

    /// Offset of the attributes field.
    pub const ATTRIBUTES_OFFSET: usize = Self::CRC_OFFSET + Self::CRC_LENGTH;
    /// Length of the attributes field.
    pub const ATTRIBUTE_LENGTH: usize = 2;

    /// Offset of the last offset delta field.
    pub const LAST_OFFSET_DELTA_OFFSET: usize = Self::ATTRIBUTES_OFFSET + Self::ATTRIBUTE_LENGTH;
    /// Length of the last offset delta field.
    pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;

    /// Offset of the base timestamp field.
    pub const BASE_TIMESTAMP_OFFSET: usize = Self::LAST_OFFSET_DELTA_OFFSET + Self::LAST_OFFSET_DELTA_LENGTH;
    /// Length of the base timestamp field.
    pub const BASE_TIMESTAMP_LENGTH: usize = 8;

    /// Offset of the max timestamp field.
    pub const MAX_TIMESTAMP_OFFSET: usize = Self::BASE_TIMESTAMP_OFFSET + Self::BASE_TIMESTAMP_LENGTH;
    /// Length of the max timestamp field.
    pub const MAX_TIMESTAMP_LENGTH: usize = 8;

    /// Offset of the producer ID field.
    pub const PRODUCER_ID_OFFSET: usize = Self::MAX_TIMESTAMP_OFFSET + Self::MAX_TIMESTAMP_LENGTH;
    /// Length of the producer ID field.
    pub const PRODUCER_ID_LENGTH: usize = 8;

    /// Offset of the producer epoch field.
    pub const PRODUCER_EPOCH_OFFSET: usize = Self::PRODUCER_ID_OFFSET + Self::PRODUCER_ID_LENGTH;
    /// Length of the producer epoch field.
    pub const PRODUCER_EPOCH_LENGTH: usize = 2;

    /// Offset of the base sequence field.
    pub const BASE_SEQUENCE_OFFSET: usize = Self::PRODUCER_EPOCH_OFFSET + Self::PRODUCER_EPOCH_LENGTH;
    /// Length of the base sequence field.
    pub const BASE_SEQUENCE_LENGTH: usize = 4;

    /// Offset of the records count field.
    pub const RECORDS_COUNT_OFFSET: usize = Self::BASE_SEQUENCE_OFFSET + Self::BASE_SEQUENCE_LENGTH;
    /// Length of the records count field.
    pub const RECORDS_COUNT_LENGTH: usize = 4;

    /// Offset where the actual records begin (immediately after the batch header).
    pub const RECORDS_OFFSET: usize = Self::RECORDS_COUNT_OFFSET + Self::RECORDS_COUNT_LENGTH;

    /// Total overhead of a record batch header (equals `RECORDS_OFFSET`).
    pub const RECORD_BATCH_OVERHEAD: usize = Self::RECORDS_OFFSET;

    /// Empty headers array constant, used when no headers are present.
    pub const EMPTY_HEADERS: &'static [RecordHeader] = &[];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_values() {
        assert_eq!(RecordBatch::MAGIC_VALUE_V0, 0);
        assert_eq!(RecordBatch::MAGIC_VALUE_V1, 1);
        assert_eq!(RecordBatch::MAGIC_VALUE_V2, 2);
        assert_eq!(RecordBatch::CURRENT_MAGIC_VALUE, 2);
    }

    #[test]
    fn test_sentinel_values() {
        assert_eq!(RecordBatch::NO_TIMESTAMP, -1);
        assert_eq!(RecordBatch::NO_PRODUCER_ID, -1);
        assert_eq!(RecordBatch::NO_PRODUCER_EPOCH, -1);
        assert_eq!(RecordBatch::NO_SEQUENCE, -1);
        assert_eq!(RecordBatch::NO_PARTITION_LEADER_EPOCH, -1);
    }

    #[test]
    fn test_record_batch_overhead() {
        // The record batch overhead should be 61 bytes
        // 8 (base_offset) + 4 (length) + 4 (partition_leader_epoch) + 1 (magic)
        // + 4 (crc) + 2 (attributes) + 4 (last_offset_delta) + 8 (base_timestamp)
        // + 8 (max_timestamp) + 8 (producer_id) + 2 (producer_epoch)
        // + 4 (base_sequence) + 4 (records_count) = 61
        assert_eq!(RecordBatch::RECORD_BATCH_OVERHEAD, 61);
    }

    #[test]
    fn test_field_offsets() {
        assert_eq!(RecordBatch::BASE_OFFSET_OFFSET, 0);
        assert_eq!(RecordBatch::LENGTH_OFFSET, 8);
        assert_eq!(RecordBatch::PARTITION_LEADER_EPOCH_OFFSET, 12);
        assert_eq!(RecordBatch::MAGIC_OFFSET, 16);
        assert_eq!(RecordBatch::CRC_OFFSET, 17);
        assert_eq!(RecordBatch::ATTRIBUTES_OFFSET, 21);
        assert_eq!(RecordBatch::LAST_OFFSET_DELTA_OFFSET, 23);
        assert_eq!(RecordBatch::BASE_TIMESTAMP_OFFSET, 27);
        assert_eq!(RecordBatch::MAX_TIMESTAMP_OFFSET, 35);
        assert_eq!(RecordBatch::PRODUCER_ID_OFFSET, 43);
        assert_eq!(RecordBatch::PRODUCER_EPOCH_OFFSET, 51);
        assert_eq!(RecordBatch::BASE_SEQUENCE_OFFSET, 53);
        assert_eq!(RecordBatch::RECORDS_COUNT_OFFSET, 57);
        assert_eq!(RecordBatch::RECORDS_OFFSET, 61);
    }

    #[test]
    fn test_empty_headers() {
        assert_eq!(RecordBatch::EMPTY_HEADERS.len(), 0);
    }
}
