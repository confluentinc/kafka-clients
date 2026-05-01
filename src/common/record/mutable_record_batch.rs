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

//! Translation of `org.apache.kafka.common.record.MutableRecordBatch`.

use crate::common::errors::KafkaError;
use crate::common::record::{Record, RecordBatch, TimestampType};
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;

/// A mutable record batch is one that can be modified in place (without
/// copying). This is used by the broker to override certain fields in the
/// batch before appending it to the log.
///
/// Mirrors Java's `MutableRecordBatch` interface.
pub trait MutableRecordBatch: RecordBatch {
    /// Set the last offset of this batch.
    fn set_last_offset(&mut self, offset: i64);

    /// Set the max timestamp of this batch. Java callers update both the
    /// timestamp value and the timestamp type; the per-record timestamps
    /// are not rewritten because clients ignore them when the type is
    /// `LogAppendTime`. The `baseTimestamp` field is also untouched.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] if `timestamp_type` is
    /// [`TimestampType::NoTimestampType`] (mirrors Java's
    /// `IllegalArgumentException` from
    /// `DefaultRecordBatch.setMaxTimestamp`).
    fn set_max_timestamp(&mut self, timestamp_type: TimestampType, max_timestamp: i64) -> Result<(), KafkaError>;

    /// Set the partition leader epoch.
    fn set_partition_leader_epoch(&mut self, epoch: i32);

    /// Write this batch into the given output stream. Mirrors Java's
    /// `writeTo(ByteBufferOutputStream)` (the parent `RecordBatch` already
    /// declares `write_to(&mut Vec<u8>)`).
    fn write_to_stream(&self, output_stream: &mut ByteBufferOutputStream);

    /// Return an iterator that skips parsing the key, value, and headers from
    /// the record stream. The yielded `Record`s' key and value will be empty.
    /// Used when the consumer does not need the body, saving allocation/GC
    /// overhead.
    ///
    /// Each item is `Result<Box<dyn Record + 'a>, KafkaError>` for the same
    /// reason as [`RecordBatch::iter`].
    fn skip_key_value_iterator<'a>(
        &'a self,
        buffer_supplier: &'a mut BufferSupplier,
    ) -> Box<dyn Iterator<Item = Result<Box<dyn Record + 'a>, KafkaError>> + 'a>;
}
