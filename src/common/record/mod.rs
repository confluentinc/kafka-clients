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

//! Record batch serialization for the Kafka wire protocol (magic v2).
//!
//! Translated from `org.apache.kafka.common.record`.
//!
//! This module provides:
//! - [`Record`] trait: a single log record (key, value, headers, offset, timestamp)
//! - [`RecordBatch`] constants and the [`DefaultRecordBatch`] implementation
//! - [`DefaultRecord`]: varint-encoded single record serialization
//! - [`MemoryRecords`] / [`MemoryRecordsBuilder`]: batch containers

pub mod compression_type;
pub mod default_record;
pub mod default_record_batch;
pub mod memory_records;
pub mod memory_records_builder;
pub mod timestamp_type;

pub use compression_type::CompressionType;
pub use default_record::DefaultRecord;
pub use default_record_batch::DefaultRecordBatch;
pub use memory_records::MemoryRecords;
pub use memory_records_builder::MemoryRecordsBuilder;
pub use timestamp_type::TimestampType;

use crate::errors::{ErrorCode, KafkaError};

/// A record header (key-value pair attached to a record).
///
/// Translated from `org.apache.kafka.common.header.Header` /
/// `org.apache.kafka.common.header.internals.RecordHeader`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    /// The header key (never null in valid records).
    pub key: String,
    /// The header value (may be None / null).
    pub value: Option<Vec<u8>>,
}

impl RecordHeader {
    /// Create a new record header.
    pub fn new(key: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        RecordHeader { key: key.into(), value }
    }
}

/// A high-level representation of a kafka record, useful for building record sets
/// without depending on a specific magic version.
///
/// Translated from `org.apache.kafka.common.record.SimpleRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleRecord {
    /// The record timestamp.
    pub timestamp: i64,
    /// The record key (may be None / null).
    pub key: Option<Vec<u8>>,
    /// The record value (may be None / null).
    pub value: Option<Vec<u8>>,
    /// The record headers.
    pub headers: Vec<RecordHeader>,
}

impl SimpleRecord {
    /// Create a new simple record with timestamp, key, value, and headers.
    pub fn new(timestamp: i64, key: Option<Vec<u8>>, value: Option<Vec<u8>>, headers: Vec<RecordHeader>) -> Self {
        SimpleRecord { timestamp, key, value, headers }
    }

    /// Create a simple record with timestamp, key, and value (no headers).
    pub fn with_timestamp(timestamp: i64, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self::new(timestamp, key, value, Vec::new())
    }

    /// Create a simple record with key and value only (no timestamp, no headers).
    pub fn with_key_value(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self::with_timestamp(NO_TIMESTAMP, key, value)
    }

    /// Create a simple record with only a value.
    pub fn with_value(value: Option<Vec<u8>>) -> Self {
        Self::with_key_value(None, value)
    }
}

// ============================================================================
// RecordBatch constants
// Translated from org.apache.kafka.common.record.RecordBatch (interface)
// ============================================================================

/// Magic value for record format version 0.
pub const MAGIC_VALUE_V0: i8 = 0;
/// Magic value for record format version 1.
pub const MAGIC_VALUE_V1: i8 = 1;
/// Magic value for record format version 2 (current).
pub const MAGIC_VALUE_V2: i8 = 2;

/// The current magic value.
pub const CURRENT_MAGIC_VALUE: i8 = MAGIC_VALUE_V2;

/// Timestamp value for records without a timestamp.
pub const NO_TIMESTAMP: i64 = -1;

/// Producer ID for non-idempotent / non-transactional producers.
pub const NO_PRODUCER_ID: i64 = -1;
/// Producer epoch for non-idempotent / non-transactional producers.
pub const NO_PRODUCER_EPOCH: i16 = -1;
/// Sequence number placeholder for non-idempotent producers.
pub const NO_SEQUENCE: i32 = -1;

/// Used to indicate an unknown leader epoch.
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;

// ============================================================================
// Records constants
// Translated from org.apache.kafka.common.record.Records (interface)
// ============================================================================

/// Offset of the offset field in the log entry.
pub const OFFSET_OFFSET: usize = 0;
/// Length of the offset field.
pub const OFFSET_LENGTH: usize = 8;
/// Offset of the size field.
pub const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
/// Length of the size field.
pub const SIZE_LENGTH: usize = 4;
/// Total log overhead (offset + size fields before the batch data).
pub const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;

/// Offset of the magic byte (same for all current message formats).
pub const MAGIC_OFFSET: usize = LOG_OVERHEAD + 4;
/// Length of the magic byte.
pub const MAGIC_LENGTH: usize = 1;
/// Header size up to and including the magic byte.
pub const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + MAGIC_LENGTH;

/// Helper to create a `KafkaError` for corrupt records.
pub(crate) fn corrupt_record_error(msg: impl Into<String>) -> KafkaError {
    KafkaError::new(ErrorCode::CorruptRecord, msg)
}

/// Helper to create a `KafkaError` for invalid records.
pub(crate) fn invalid_record_error(msg: impl Into<String>) -> KafkaError {
    KafkaError::new(ErrorCode::CorruptRecord, msg)
}

/// Return the record batch header size for v2 (only v2 supported).
///
/// Translated from `AbstractRecords.recordBatchHeaderSizeInBytes`.
pub fn record_batch_header_size_in_bytes(_magic: i8, _compression_type: CompressionType) -> usize {
    // Only magic v2 is supported; always returns RECORD_BATCH_OVERHEAD
    default_record_batch::RECORD_BATCH_OVERHEAD
}
