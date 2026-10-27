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
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::header::RecordHeaders;
use crate::common::record::TimestampType;

/// Which side of the record failed to deserialize.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException$DeserializationExceptionOrigin")]
pub enum DeserializationErrorOrigin {
    /// The key could not be deserialized.
    Key,
    /// The value could not be deserialized.
    Value,
}

/// Java's inherited `Enum.toString()`: the constant's name.
impl std::fmt::Display for DeserializationErrorOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Key => "KEY",
            Self::Value => "VALUE",
        })
    }
}

/// This error is raised for any error that occurs while deserializing records received by the
/// consumer using the configured `Deserializer`.
///
/// # Translation notes
///
/// No protocol code.
// Hand-written rather than declared with `kafka_error_type!` because it carries the eight fields
// Java exposes: they identify *which* record failed, and consumer error handlers read them.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException")]
pub struct RecordDeserializationError {
    message: String,
    /// The underlying cause — Java's ten-argument constructor takes one.
    source: Option<Box<crate::common::Error>>,
    origin: DeserializationErrorOrigin,
    partition: TopicPartition,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    key_buffer: Option<Vec<u8>>,
    value_buffer: Option<Vec<u8>>,
    headers: Option<RecordHeaders>,
}

impl RecordDeserializationError {
    /// Creates a deserialization error with the full context of the offending record; the
    /// cause is attached by [`Self::with_source`].
    ///
    /// # Arguments
    ///
    /// - `origin` - which side of the record (key or value) failed to deserialize
    /// - `partition` - the partition of the record
    /// - `offset` - the offset of the record
    /// - `timestamp` - the timestamp of the record
    /// - `timestamp_type` - the timestamp type of the record
    /// - `key_buffer` - the raw key bytes, `None` for a null key
    /// - `value_buffer` - the raw value bytes, `None` for a null value
    /// - `headers` - the record headers
    /// - `message` - the error's message
    #[expect(clippy::too_many_arguments)]
    #[doc(
        alias = "org.apache.kafka.common.errors.RecordDeserializationException#RecordDeserializationException(DeserializationExceptionOrigin,TopicPartition,long,long,TimestampType,ByteBuffer,ByteBuffer,Headers,String,Throwable)"
    )]
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
            source: None,
            origin,
            partition,
            offset,
            timestamp,
            timestamp_type,
            key_buffer,
            value_buffer,
            headers,
        }
    }

    /// Returns which side of the record (key or value) failed to deserialize.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#origin")]
    pub fn origin(&self) -> DeserializationErrorOrigin {
        self.origin
    }

    /// Returns the partition of the offending record.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#topicPartition")]
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.partition
    }

    /// Returns the offset of the offending record.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#offset")]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the timestamp of the offending record.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#timestamp")]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the timestamp type of the offending record.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#timestampType")]
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// Returns the raw key bytes, or `None` if absent.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#keyBuffer")]
    pub fn key_buffer(&self) -> Option<&[u8]> {
        self.key_buffer.as_deref()
    }

    /// Returns the raw value bytes, or `None` if absent.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#valueBuffer")]
    pub fn value_buffer(&self) -> Option<&[u8]> {
        self.value_buffer.as_deref()
    }

    /// Returns the record headers, or `None` if absent.
    #[doc(alias = "org.apache.kafka.common.errors.RecordDeserializationException#headers")]
    pub fn headers(&self) -> Option<&RecordHeaders> {
        self.headers.as_ref()
    }

    /// Attaches the error that caused this deserialization failure.
    ///
    /// # Arguments
    ///
    /// - `source` - the error's cause
    ///
    /// # Returns
    ///
    /// this error, with its cause set
    #[must_use]
    #[doc(
        alias = "org.apache.kafka.common.errors.RecordDeserializationException#RecordDeserializationException(DeserializationExceptionOrigin,TopicPartition,long,long,TimestampType,ByteBuffer,ByteBuffer,Headers,String,Throwable)"
    )]
    pub fn with_source(mut self, source: crate::common::Error) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Returns the error message: Java's `getMessage()`.
    #[doc(alias = "rust-only")]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the underlying cause, if any: Java's `getCause()`.
    #[doc(alias = "rust-only")]
    pub fn source(&self) -> Option<&crate::common::Error> {
        self.source.as_deref()
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
    fn is_record_deserialization_error(&self) -> bool {
        true
    }
}

impl ErrorSource for RecordDeserializationError {
    fn source(&self) -> Option<&crate::common::Error> {
        self.source.as_deref()
    }
}
impl std::error::Error for RecordDeserializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
