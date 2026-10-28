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

/// No reset policy has been defined, and the offsets for these partitions are
/// either larger or smaller than the range of offsets the server has for the
/// given partition.
// Java `extends` chain: `OffsetOutOfRangeException` -> `InvalidOffsetException`
// -> `KafkaException`. It has no entry in `Errors`: the wire code
// `OFFSET_OUT_OF_RANGE` builds the `common.errors` class instead.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.OffsetOutOfRangeException")]
pub struct ConsumerOffsetOutOfRangeError {
    message: String,
    /// The underlying cause — Java's `OffsetOutOfRangeException(String, Throwable)`.
    source: Option<Box<Error>>,
    /// The out-of-range offset per partition.
    pub(crate) offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
}

impl ConsumerOffsetOutOfRangeError {
    /// Constructs a new `ConsumerOffsetOutOfRangeError` with the offsets that
    /// are out of range.
    ///
    /// # Arguments
    ///
    /// - `offset_out_of_range_partitions` - Map of partition to the out-of-range offset
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetOutOfRangeException#OffsetOutOfRangeException")]
    pub fn new(offset_out_of_range_partitions: HashMap<TopicPartition, i64>) -> Self {
        let mut items: Vec<String> = offset_out_of_range_partitions.iter().map(|(k, v)| format!("{k}={v}")).collect();
        items.sort();
        let message = format!(
            "Offsets out of range with no configured reset policy for partitions: {{{}}}",
            items.join(", ")
        );
        Self { message, offset_out_of_range_partitions, source: None }
    }

    /// Constructs a new `ConsumerOffsetOutOfRangeError` with a custom message.
    ///
    /// # Arguments
    ///
    /// - `message` - The detail message
    /// - `offset_out_of_range_partitions` - Map of partition to the out-of-range offset
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetOutOfRangeException#OffsetOutOfRangeException")]
    pub fn with_message(
        message: impl Into<String>,
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
    ) -> Self {
        Self { message: message.into(), offset_out_of_range_partitions, source: None }
    }

    /// Get a map of the topic partitions and the respective out-of-range fetch
    /// offsets.
    ///
    /// # Returns
    ///
    /// The out-of-range fetch offset per partition
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetOutOfRangeException#offsetOutOfRangePartitions")]
    pub fn offset_out_of_range_partitions(&self) -> &HashMap<TopicPartition, i64> {
        &self.offset_out_of_range_partitions
    }

    /// Returns the set of partitions for which the offset was invalid.
    ///
    /// # Returns
    ///
    /// The set of partitions with invalid offsets
    ///
    /// # Translation notes
    ///
    /// Iterates the keys instead of copying them.
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetOutOfRangeException#partitions")]
    pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition> {
        self.offset_out_of_range_partitions.keys()
    }

    /// Returns the error message.
    // Java's Throwable.getMessage, a JDK method
    #[doc(alias = "rust-only")]
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
    /// Returns the cause of this error, if any.
    // Java's Throwable.getCause, a JDK method
    #[doc(alias = "rust-only")]
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
    /// + offsetOutOfRangePartitions` (`org.apache.kafka.clients.consumer.OffsetOutOfRangeException#OffsetOutOfRangeException(Map)`),
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
    /// (`org.apache.kafka.clients.consumer.OffsetOutOfRangeException#partitions`) — the override of
    /// `InvalidOffsetException`'s single abstract member
    /// (`org.apache.kafka.clients.consumer.InvalidOffsetException#partitions`), which Kafka's own javadoc tells
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
    /// KafkaException` (`org.apache.kafka.clients.consumer.OffsetOutOfRangeException`) — the CONSUMER
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
