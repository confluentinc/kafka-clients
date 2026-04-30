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

//! Translation of `org.apache.kafka.common.record.RecordBatch`.

use crate::common::errors::KafkaError;
use crate::common::record::{CompressionType, Record, TimestampType};
use crate::common::utils::buffer_supplier::BufferSupplier;

// Magic-byte constants — these mirror Java's `RecordBatch.MAGIC_VALUE_V0` etc.
/// Magic byte for the v0 record format.
pub const MAGIC_VALUE_V0: i8 = 0;
/// Magic byte for the v1 record format.
pub const MAGIC_VALUE_V1: i8 = 1;
/// Magic byte for the v2 record format (current).
pub const MAGIC_VALUE_V2: i8 = 2;
/// Current magic byte.
pub const CURRENT_MAGIC_VALUE: i8 = MAGIC_VALUE_V2;

/// Sentinel timestamp for records without a timestamp.
pub const NO_TIMESTAMP: i64 = -1;

// Sentinel values used in the v2 record format by non-idempotent /
// non-transactional producers, or when up-converting from older formats.
/// Sentinel producer-id meaning "no producer".
pub const NO_PRODUCER_ID: i64 = -1;
/// Sentinel producer epoch meaning "no producer".
pub const NO_PRODUCER_EPOCH: i16 = -1;
/// Sentinel base-sequence meaning "no sequence".
pub const NO_SEQUENCE: i32 = -1;

/// Sentinel value for an unknown partition leader epoch (the case when the
/// record set is first created by the producer).
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// A record batch is a container for records. In old versions of the record
/// format (versions 0 and 1), a batch consisted always of a single record if
/// no compression was enabled, but could contain many records otherwise.
/// Newer versions (magic 2 and above) always contain one or more records,
/// regardless of compression.
///
/// Java's `RecordBatch` extends `Iterable<Record>`. In Rust we expose
/// [`RecordBatch::iter`] returning a boxed iterator yielding boxed `Record`
/// trait objects. The default trait methods [`has_producer_id`],
/// [`next_offset`], [`is_compressed`] and [`offset_of_max_timestamp`] mirror
/// the implementations Java factors out into `AbstractRecordBatch` and the
/// `default` body on the `RecordBatch` interface.
pub trait RecordBatch {
    /// Whether the batch's checksum is correct.
    fn is_valid(&self) -> bool;

    /// Raise an error if the checksum is not valid.
    fn ensure_valid(&self) -> Result<(), KafkaError>;

    /// 4-byte unsigned checksum, returned as `i64` to mirror Java's `long`
    /// signature.
    fn checksum(&self) -> i64;

    /// The maximum timestamp in this batch (or the log-append timestamp).
    fn max_timestamp(&self) -> i64;

    /// The timestamp type. Always [`TimestampType::NoTimestampType`] for magic
    /// 0.
    fn timestamp_type(&self) -> TimestampType;

    /// Base offset contained in this batch.
    fn base_offset(&self) -> i64;

    /// Last offset in this batch (inclusive).
    fn last_offset(&self) -> i64;

    /// The offset following this batch (`last_offset() + 1`). Provided as a
    /// default impl mirroring Java's interface body.
    fn next_offset(&self) -> i64 {
        self.last_offset() + 1
    }

    /// Magic value of this batch.
    fn magic(&self) -> i8;

    /// Producer id, or [`NO_PRODUCER_ID`].
    fn producer_id(&self) -> i64;

    /// Producer epoch, or [`NO_PRODUCER_EPOCH`].
    fn producer_epoch(&self) -> i16;

    /// Whether the batch carries a producer id. Default impl mirrors Java's
    /// `AbstractRecordBatch#hasProducerId`.
    fn has_producer_id(&self) -> bool {
        NO_PRODUCER_ID < self.producer_id()
    }

    /// Base sequence number, or [`NO_SEQUENCE`].
    fn base_sequence(&self) -> i32;

    /// Last sequence number, or [`NO_SEQUENCE`].
    fn last_sequence(&self) -> i32;

    /// Compression type used by this batch.
    fn compression_type(&self) -> CompressionType;

    /// Total size of this batch in bytes.
    fn size_in_bytes(&self) -> i32;

    /// Number of records, where supported (magic 2 and above). Returns `None`
    /// for older magic versions.
    fn count_or_null(&self) -> Option<i32>;

    /// Whether the batch is compressed. Default impl mirrors
    /// `AbstractRecordBatch#isCompressed`.
    fn is_compressed(&self) -> bool {
        self.compression_type() != CompressionType::None
    }

    /// Write this batch into the supplied byte buffer.
    fn write_to(&self, buffer: &mut Vec<u8>);

    /// Whether this batch is part of a transaction.
    fn is_transactional(&self) -> bool;

    /// Delete-horizon timestamp, or `None` if the first timestamp is not the
    /// delete horizon. Mirrors Java's `OptionalLong`.
    fn delete_horizon_ms(&self) -> Option<i64>;

    /// Partition leader epoch, or [`NO_PARTITION_LEADER_EPOCH`].
    fn partition_leader_epoch(&self) -> i32;

    /// Whether this is a control batch.
    fn is_control_batch(&self) -> bool;

    /// Iterate over the records in this batch. Mirrors Java's
    /// `Iterable<Record>` parent on `RecordBatch`.
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a>;

    /// Return a streaming iterator that defers decompression of the record
    /// stream until each next() call. The supplied buffer supplier may be
    /// reused across batches to avoid large per-batch allocations.
    ///
    /// Phase 3a defines this as part of the contract; the concrete iterator
    /// implementations land in Phase 3c/3d.
    fn streaming_iterator<'a>(
        &'a self,
        decompression_buffer_supplier: &'a mut BufferSupplier,
    ) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a>;

    /// Iterate all records to find the offset of the maximum timestamp.
    ///
    /// Notes (mirroring Java's contract):
    /// 1. The earliest offset is returned if multiple records share the max
    ///    timestamp.
    /// 2. Always returns `None` for magic 0 batches.
    fn offset_of_max_timestamp(
        &self,
        decompression_buffer_supplier: &mut BufferSupplier,
    ) -> Result<Option<i64>, KafkaError> {
        if self.magic() == MAGIC_VALUE_V0 {
            return Ok(None);
        }
        let max_timestamp = self.max_timestamp();
        for record in self.streaming_iterator(decompression_buffer_supplier) {
            if max_timestamp == record.timestamp() {
                return Ok(Some(record.offset()));
            }
        }
        Ok(None)
    }
}
