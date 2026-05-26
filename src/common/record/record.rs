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

//! Translation of `org.apache.kafka.common.record.Record`.

use crate::common::errors::KafkaError;
use crate::common::header::RecordHeader;
use crate::common::record::TimestampType;

/// A log record is a tuple consisting of a unique offset in the log, a sequence
/// number assigned by the producer, a timestamp, a key and a value.
///
/// Mirrors Java's `org.apache.kafka.common.record.Record` interface. Concrete
/// implementations live in Phase 3c (`DefaultRecord`, `MemoryRecords`).
///
/// Per CLAUDE.md rule 12 the byte slices returned by [`Record::key`] and
/// [`Record::value`] are borrowed views into the underlying record buffer —
/// callers MUST NOT copy them, the producer client serializes directly through.
pub trait Record {
    /// The offset of this record in the log.
    fn offset(&self) -> i64;

    /// The producer-assigned sequence number.
    fn sequence(&self) -> i32;

    /// Size in bytes of this record (including overhead).
    fn size_in_bytes(&self) -> i32;

    /// The record's timestamp.
    fn timestamp(&self) -> i64;

    /// Raise a [`KafkaError::CorruptRecord`] if the record does not have a
    /// valid checksum. Mirrors Java's `ensureValid()` which throws
    /// `CorruptRecordException`.
    fn ensure_valid(&self) -> Result<(), KafkaError>;

    /// Size in bytes of the key, or `-1` if there is no key.
    fn key_size(&self) -> i32;

    /// Whether this record carries a key.
    fn has_key(&self) -> bool;

    /// The record's key, or `None` if absent.
    fn key(&self) -> Option<&[u8]>;

    /// Size in bytes of the value, or `-1` if the value is null.
    fn value_size(&self) -> i32;

    /// Whether the value is present (non-null).
    fn has_value(&self) -> bool;

    /// The (nullable) record value.
    fn value(&self) -> Option<&[u8]>;

    /// For magic versions prior to 2 the record carries its own magic, so this
    /// method checks that field. For magic 2 and above, returns `true` if the
    /// passed `magic` is greater than or equal to 2.
    fn has_magic(&self, magic: i8) -> bool;

    /// For magic versions prior to 2, whether the record is compressed
    /// (and therefore has nested record content). For magic 2 and above this
    /// always returns `false`.
    fn is_compressed(&self) -> bool;

    /// For magic versions prior to 2, whether the timestamp-type attribute on
    /// the record matches the supplied [`TimestampType`]. For magic 2 and above
    /// this is always `false` (the timestamp-type lives on the batch, not the
    /// record).
    fn has_timestamp_type(&self, timestamp_type: TimestampType) -> bool;

    /// The headers attached to this record. For magic 1 and below the slice is
    /// always empty.
    fn headers(&self) -> &[RecordHeader];
}
