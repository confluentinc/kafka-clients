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

//! Translated from `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
//!
//! Named with the `Consumer` prefix because `org.apache.kafka.common.errors`
//! has a *different* class of the same name, already translated as
//! [`OffsetOutOfRangeError`](crate::common::errors::OffsetOutOfRangeError). The
//! two differ in ancestry: that one extends `ApiException`, this one does not.
//! Java tells them apart by package; [`Error`](crate::common::Error) has one
//! flat variant list, so the package moves into the name.

use std::collections::HashMap;
use std::fmt;

use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// A fetch asked for an offset outside the range the broker retains.
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
/// It has no entry in `Errors`, so it carries no protocol code — the wire code
/// `OFFSET_OUT_OF_RANGE` builds the `common.errors` class instead.
///
/// Java `extends` chain:
///    `OffsetOutOfRangeException` -> `InvalidOffsetException` -> `KafkaException`
#[derive(Clone, Debug)]
pub struct ConsumerOffsetOutOfRangeError {
    message: String,
    /// The underlying cause — Java's `OffsetOutOfRangeException(String, Throwable)`.
    source: Option<Box<Error>>,
    /// The out-of-range offset per partition.
    pub offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
}

impl ConsumerOffsetOutOfRangeError {
    /// Create the error, mirroring Java's
    /// `OffsetOutOfRangeException(Map<TopicPartition, Long>)`.
    pub fn new(offset_out_of_range_partitions: HashMap<TopicPartition, i64>) -> Self {
        let mut items: Vec<String> = offset_out_of_range_partitions.iter().map(|(k, v)| format!("{k}={v}")).collect();
        items.sort();
        let message = format!(
            "Offsets out of range with no configured reset policy for partitions: {{{}}}",
            items.join(", ")
        );
        Self { message, offset_out_of_range_partitions, source: None }
    }

    /// Create the error with a caller-supplied message, mirroring Java's
    /// `OffsetOutOfRangeException(String, Map<TopicPartition, Long>)`.
    pub fn with_message(
        message: impl Into<String>,
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
    ) -> Self {
        Self { message: message.into(), offset_out_of_range_partitions, source: None }
    }

    /// The out-of-range offset per partition.
    pub fn offset_out_of_range_partitions(&self) -> &HashMap<TopicPartition, i64> {
        &self.offset_out_of_range_partitions
    }

    /// The partitions this error covers.
    ///
    /// Mirrors Java's `OffsetOutOfRangeException.partitions()`
    /// (`OffsetOutOfRangeException.java:51-54`), which is
    /// `return offsetOutOfRangePartitions.keySet();` — the override of the
    /// single abstract member `InvalidOffsetException` declares
    /// (`InvalidOffsetException.java:36`), so it is available uniformly across
    /// the family that
    /// [`is_consumer_invalid_offset_error`](crate::common::error::ErrorHierarchy::is_consumer_invalid_offset_error)
    /// recognises.
    ///
    /// Returns an iterator rather than a `HashSet`: Java's `keySet()` is a
    /// *view* over the map, so materialising a set here would allocate where
    /// Java does not (CLAUDE.md §11/§12).
    pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition> {
        self.offset_out_of_range_partitions.keys()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerOffsetOutOfRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerOffsetOutOfRangeError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerOffsetOutOfRangeError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerOffsetOutOfRangeError {}

impl ErrorHierarchy for ConsumerOffsetOutOfRangeError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_consumer_invalid_offset_error(&self) -> bool {
        true
    }
    fn is_consumer_offset_out_of_range_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ConsumerOffsetOutOfRangeError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl ConsumerOffsetOutOfRangeError {
    /// The underlying source, if any. Mirrors Java's `getCause()`
    /// (`OffsetOutOfRangeException(String, Throwable)`).
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl std::error::Error for ConsumerOffsetOutOfRangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Recovered from `master:src/consumer/errors.rs`
    /// (`test_offset_out_of_range_default_message_and_partitions`), which was
    /// dropped when that file was split into one file per error class.
    ///
    /// Java: `OffsetOutOfRangeException(Map<TopicPartition, Long>)` composes
    /// `"Offsets out of range with no configured reset policy for partitions: "
    /// + offsetOutOfRangePartitions` (`OffsetOutOfRangeException.java:36-40`),
    /// and Java's `Map.toString()` braces the entries — hence `{t-0=5}`.
    ///
    /// The exact encoding is asserted because the split changed it and no test
    /// noticed.
    #[test]
    fn test_offset_out_of_range_default_message_and_partitions() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = ConsumerOffsetOutOfRangeError::new(HashMap::from([(tp.clone(), 5)]));
        assert_eq!(
            e.message(),
            "Offsets out of range with no configured reset policy for partitions: {t-0=5}"
        );
        assert_eq!(e.offset_out_of_range_partitions().get(&tp), Some(&5));
    }

    /// Java: `OffsetOutOfRangeException.partitions()` is
    /// `offsetOutOfRangePartitions.keySet()`
    /// (`OffsetOutOfRangeException.java:51-54`) — the override of
    /// `InvalidOffsetException`'s single abstract member
    /// (`InvalidOffsetException.java:36`), which Kafka's own javadoc tells
    /// callers to use.
    #[test]
    fn test_offset_out_of_range_partitions_accessor() {
        let tp0 = TopicPartition::new("t".to_string(), 0);
        let tp1 = TopicPartition::new("t".to_string(), 1);
        let e = ConsumerOffsetOutOfRangeError::new(HashMap::from([(tp0.clone(), 5), (tp1.clone(), 7)]));
        let parts: HashSet<&TopicPartition> = e.partitions().collect();
        assert_eq!(parts, HashSet::from([&tp0, &tp1]));
    }

    /// `OffsetOutOfRangeException extends InvalidOffsetException extends
    /// KafkaException` (`OffsetOutOfRangeException.java:29`) — the CONSUMER
    /// package's `InvalidOffsetException`, so it is not an `ApiException`,
    /// unlike the identically-named `common.errors` class.
    #[test]
    fn test_offset_out_of_range_error_hierarchy() {
        let e = Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(HashMap::from([(
            TopicPartition::new("t".to_string(), 0),
            5,
        )])));
        assert!(e.is_kafka_error());
        assert!(e.is_consumer_invalid_offset_error());
        assert!(e.is_consumer_offset_out_of_range_error());
        assert!(!e.is_api_error());
        assert!(!e.is_retriable_error());
    }
}
