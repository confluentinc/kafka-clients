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

//! Translated from `org.apache.kafka.common.errors.RecordDeserializationException`.

use std::fmt;

use crate::common::TopicPartition;
use crate::common::header::RecordHeaders;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage};
use crate::common::record::TimestampType;

/// Which side of the record failed to deserialize.
///
/// Corresponds to Java's nested
/// `RecordDeserializationException.DeserializationExceptionOrigin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeserializationErrorOrigin {
    /// The key could not be deserialized.
    Key,
    /// The value could not be deserialized.
    Value,
}

/// A record could not be deserialized, carrying the location and raw bytes of
/// the offending record.
///
/// Corresponds to Java's `RecordDeserializationException`. It has no entry in
/// `Errors`, so it carries no protocol code.
///
/// Java `extends` chain:
///    `RecordDeserializationException` -> `SerializationException` ->
///   `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because it
/// carries the eight fields Java exposes — they identify *which* record failed,
/// and consumer error handlers read them.
#[derive(Clone, Debug)]
pub struct RecordDeserializationError {
    message: String,
    origin: Option<DeserializationErrorOrigin>,
    partition: TopicPartition,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    key_buffer: Option<Vec<u8>>,
    value_buffer: Option<Vec<u8>>,
    headers: Option<RecordHeaders>,
}

impl RecordDeserializationError {
    /// Create a deserialization error with the full record context, mirroring
    /// Java's ten-argument constructor (minus the `cause`, which [`Error`] does
    /// not carry).
    ///
    /// [`Error`]: crate::common::Error
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        origin: DeserializationErrorOrigin,
        partition: TopicPartition,
        offset: i64,
        timestamp: i64,
        timestamp_type: TimestampType,
        key_buffer: Option<Vec<u8>>,
        value_buffer: Option<Vec<u8>>,
        headers: Option<RecordHeaders>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            message: message.into(),
            origin: Some(origin),
            partition,
            offset,
            timestamp,
            timestamp_type,
            key_buffer,
            value_buffer,
            headers,
        }
    }

    /// Which side (key or value) failed; `None` via the deprecated Java
    /// constructor that does not record it.
    pub fn origin(&self) -> Option<DeserializationErrorOrigin> {
        self.origin
    }

    /// The partition of the offending record.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.partition
    }

    /// The offset of the offending record.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The timestamp of the offending record.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The timestamp type of the offending record.
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// The raw key bytes, or `None` if absent.
    pub fn key_buffer(&self) -> Option<&[u8]> {
        self.key_buffer.as_deref()
    }

    /// The raw value bytes, or `None` if absent.
    pub fn value_buffer(&self) -> Option<&[u8]> {
        self.value_buffer.as_deref()
    }

    /// The record headers, or `None` if absent.
    pub fn headers(&self) -> Option<&RecordHeaders> {
        self.headers.as_ref()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RecordDeserializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordDeserializationError: {}", self.message)
    }
}

impl ErrorMessage for RecordDeserializationError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for RecordDeserializationError {}

impl ErrorHierarchy for RecordDeserializationError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_serialization_error(&self) -> bool {
        true
    }
}
