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

use crate::common::header::RecordHeader;
use crate::common::record::TimestampType;

/// A log record is a tuple consisting of a unique offset in the log, a sequence
/// number assigned by the producer, a timestamp, a key, and a value.
///
/// Corresponds to Java's `org.apache.kafka.common.record.Record` interface.
#[doc(alias = "org.apache.kafka.common.record.internal.Record")]
pub trait Record {
    /// The offset of this record in the log.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#offset")]
    fn offset(&self) -> i64;

    /// Get the sequence number assigned by the producer.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#sequence")]
    fn sequence(&self) -> i32;

    /// Get the size in bytes of this record.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#sizeInBytes")]
    fn size_in_bytes(&self) -> i32;

    /// Get the record's timestamp.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#timestamp")]
    fn timestamp(&self) -> i64;

    /// Validate the record, returning an error if the record is corrupt.
    ///
    /// Corresponds to Java's `ensureValid()`. In Java this throws
    /// `CorruptRecordException`; in Rust it returns a `Result`.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#ensureValid")]
    fn ensure_valid(&self) -> Result<(), crate::common::InvalidRecordError>;

    /// Get the size in bytes of the key.
    ///
    /// Returns -1 if there is no key.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#keySize")]
    fn key_size(&self) -> i32;

    /// Check whether this record has a key.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#hasKey")]
    fn has_key(&self) -> bool;

    /// Get the record's key, or `None` if there is no key.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#key")]
    fn key(&self) -> Option<&[u8]>;

    /// Get the size in bytes of the value.
    ///
    /// Returns -1 if the value is null.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#valueSize")]
    fn value_size(&self) -> i32;

    /// Check whether a value is present (i.e. if the value is not null).
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#hasValue")]
    fn has_value(&self) -> bool;

    /// Get the record's value, or `None` if the value is null.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#value")]
    fn value(&self) -> Option<&[u8]>;

    /// Check whether the record has a particular magic version.
    ///
    /// For versions prior to 2, the record contains its own magic,
    /// so this function can be used to check whether it matches a particular value.
    /// For version 2 and above, this method returns true if the passed magic
    /// is greater than or equal to 2.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#hasMagic")]
    fn has_magic(&self, magic: i8) -> bool;

    /// For versions prior to 2, check whether the record is compressed (and therefore
    /// has nested record content). For versions 2 and above, this always returns false.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#isCompressed")]
    fn is_compressed(&self) -> bool;

    /// For versions prior to 2, the record contained a timestamp type attribute.
    /// This method can be used to check whether the value of that attribute matches
    /// a particular timestamp type. For versions 2 and above, this will always be false.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#hasTimestampType")]
    fn has_timestamp_type(&self, timestamp_type: TimestampType) -> bool;

    /// Get the headers. For magic versions 1 and below, this always returns an empty slice.
    #[doc(alias = "org.apache.kafka.common.record.internal.Record#headers")]
    fn headers(&self) -> &[RecordHeader];
}
