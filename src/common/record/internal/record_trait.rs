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

//! The Record trait for Kafka log records.
//!
//! A log record is a tuple consisting of a unique offset in the log, a sequence
//! number assigned by the producer, a timestamp, a key, and a value.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.Record` interface.

use crate::common::header::internals::RecordHeader;
use crate::common::record::TimestampType;

/// A log record is a tuple consisting of a unique offset in the log, a sequence
/// number assigned by the producer, a timestamp, a key, and a value.
///
/// Corresponds to Java's `org.apache.kafka.common.record.Record` interface.
pub trait Record {
    /// The offset of this record in the log.
    fn offset(&self) -> i64;

    /// Get the sequence number assigned by the producer.
    fn sequence(&self) -> i32;

    /// Get the size in bytes of this record.
    fn size_in_bytes(&self) -> i32;

    /// Get the record's timestamp.
    fn timestamp(&self) -> i64;

    /// Validate the record, returning an error if the record is corrupt.
    ///
    /// Corresponds to Java's `ensureValid()`. In Java this throws
    /// `CorruptRecordException`; in Rust it returns a `Result`.
    fn ensure_valid(&self) -> Result<(), crate::common::InvalidRecordError>;

    /// Get the size in bytes of the key.
    ///
    /// Returns -1 if there is no key.
    fn key_size(&self) -> i32;

    /// Check whether this record has a key.
    fn has_key(&self) -> bool;

    /// Get the record's key, or `None` if there is no key.
    fn key(&self) -> Option<&[u8]>;

    /// Get the size in bytes of the value.
    ///
    /// Returns -1 if the value is null.
    fn value_size(&self) -> i32;

    /// Check whether a value is present (i.e. if the value is not null).
    fn has_value(&self) -> bool;

    /// Get the record's value, or `None` if the value is null.
    fn value(&self) -> Option<&[u8]>;

    /// Check whether the record has a particular magic version.
    ///
    /// For versions prior to 2, the record contains its own magic,
    /// so this function can be used to check whether it matches a particular value.
    /// For version 2 and above, this method returns true if the passed magic
    /// is greater than or equal to 2.
    fn has_magic(&self, magic: i8) -> bool;

    /// For versions prior to 2, check whether the record is compressed (and therefore
    /// has nested record content). For versions 2 and above, this always returns false.
    fn is_compressed(&self) -> bool;

    /// For versions prior to 2, the record contained a timestamp type attribute.
    /// This method can be used to check whether the value of that attribute matches
    /// a particular timestamp type. For versions 2 and above, this will always be false.
    fn has_timestamp_type(&self, timestamp_type: TimestampType) -> bool;

    /// Get the headers. For magic versions 1 and below, this always returns an empty slice.
    fn headers(&self) -> &[RecordHeader];
}
