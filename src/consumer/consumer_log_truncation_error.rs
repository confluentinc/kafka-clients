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

//! Translated from `org.apache.kafka.clients.consumer.LogTruncationException`.

use std::collections::HashMap;
use std::fmt;

use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::consumer::OffsetAndMetadata;

/// Log truncation was detected: the broker's log diverges from the offsets the
/// consumer had fetched.
///
/// Corresponds to Java's `LogTruncationException`. It has no entry in `Errors`,
/// so it carries no protocol code.
///
/// Java `extends` chain:
///    `LogTruncationException` -> `OffsetOutOfRangeException` ->
///   `InvalidOffsetException` -> `KafkaException`
///
/// The `OffsetOutOfRangeException` in that chain is the consumer package's, so
/// this is a
/// [`ConsumerOffsetOutOfRangeError`](super::ConsumerOffsetOutOfRangeError) too —
/// which is why it carries the same `offset_out_of_range_partitions` map.
#[derive(Clone, Debug)]
pub struct ConsumerLogTruncationError {
    message: String,
    /// The out-of-range offset per partition, inherited from the parent class.
    pub offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
    /// The divergent offset per partition.
    pub divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
}

impl ConsumerLogTruncationError {
    /// Mirrors Java's
    /// `LogTruncationException(Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndMetadata>)`,
    /// which composes the message from the divergent offsets:
    /// `"Truncated partitions detected with divergent offsets " + divergentOffsets`.
    pub fn new(
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
        divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self {
        let mut items: Vec<String> = divergent_offsets.iter().map(|(k, v)| format!("{k}={v}")).collect();
        items.sort();
        let message = format!("Truncated partitions detected with divergent offsets {{{}}}", items.join(", "));
        Self { message, offset_out_of_range_partitions, divergent_offsets }
    }

    /// Mirrors Java's three-argument
    /// `LogTruncationException(String, Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndMetadata>)`.
    pub fn with_message(
        message: impl Into<String>,
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
        divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self {
        Self { message: message.into(), offset_out_of_range_partitions, divergent_offsets }
    }

    /// The out-of-range offset per partition.
    pub fn offset_out_of_range_partitions(&self) -> &HashMap<TopicPartition, i64> {
        &self.offset_out_of_range_partitions
    }

    /// The divergent offset per partition.
    pub fn divergent_offsets(&self) -> &HashMap<TopicPartition, OffsetAndMetadata> {
        &self.divergent_offsets
    }

    /// The partitions this error covers.
    ///
    /// `LogTruncationException` does not override
    /// `OffsetOutOfRangeException.partitions()`
    /// (`OffsetOutOfRangeException.java:51-54`), so this is the key set of
    /// `offset_out_of_range_partitions` — NOT of
    /// [`divergent_offsets`](Self::divergent_offsets). The distinction is the
    /// whole point of the accessor: Java's javadoc
    /// (`LogTruncationException.java:48-56`) tells the caller to iterate
    /// `partitions()` and then look each one up in `divergentOffsets()`,
    /// "because there is no guarantee that this offset will be known" for
    /// every truncated partition.
    ///
    /// Returns an iterator rather than a `HashSet` for the same reason as
    /// [`ConsumerOffsetOutOfRangeError::partitions`](super::ConsumerOffsetOutOfRangeError::partitions):
    /// Java's `keySet()` is a view, not a copy.
    pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition> {
        self.offset_out_of_range_partitions.keys()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerLogTruncationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerLogTruncationError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerLogTruncationError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerLogTruncationError {}

impl ErrorHierarchy for ConsumerLogTruncationError {
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

impl ErrorSource for ConsumerLogTruncationError {
    // `LogTruncationException` exposes no `Throwable cause` constructor, so its cause is
    // always null in Java; the trait default (`None`) is that answer.
}

impl ConsumerLogTruncationError {
    /// Always `None`: `LogTruncationException` exposes no `Throwable cause` constructor, so its
    /// cause is null in Java too. Present as an inherent method so it shadows
    /// both `ErrorSource::source` and `std::error::Error::source`, keeping
    /// `x.source()` unambiguous and typed.
    pub fn source(&self) -> Option<&Error> {
        None
    }
}

impl std::error::Error for ConsumerLogTruncationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Option::<&Error>::None.map(|e| e as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Recovered from `master:src/consumer/errors.rs`
    /// (`test_log_truncation_default_message_and_accessors`), which was dropped
    /// when that file was split into one file per error class.
    ///
    /// Java: `LogTruncationException(Map, Map)` composes
    /// `"Truncated partitions detected with divergent offsets " +
    /// divergentOffsets` (`LogTruncationException.java:36-42`); Java's
    /// `Map.toString()` braces the entries.
    #[test]
    fn test_log_truncation_default_message_and_accessors() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = ConsumerLogTruncationError::new(
            HashMap::from([(tp.clone(), 100)]),
            HashMap::from([(tp.clone(), OffsetAndMetadata::new(95).unwrap())]),
        );
        assert!(
            e.message().starts_with("Truncated partitions detected with divergent offsets "),
            "got: {}",
            e.message()
        );
        assert!(e.divergent_offsets().contains_key(&tp));
        assert!(e.offset_out_of_range_partitions().contains_key(&tp));
    }

    /// `LogTruncationException` does NOT override `partitions()`, so it inherits
    /// `OffsetOutOfRangeException`'s `offsetOutOfRangePartitions.keySet()`
    /// (`OffsetOutOfRangeException.java:51-54`). That is what makes the javadoc
    /// at `LogTruncationException.java:48-56` meaningful: iterate
    /// `partitions()`, then look each one up in `divergentOffsets()`, "because
    /// there is no guarantee that this offset will be known". So the two sets
    /// are deliberately allowed to differ, and `partitions()` must follow the
    /// out-of-range map.
    #[test]
    fn test_log_truncation_partitions_is_out_of_range_set_not_divergent_set() {
        let known = TopicPartition::new("t".to_string(), 0);
        let unknown = TopicPartition::new("t".to_string(), 1);
        let e = ConsumerLogTruncationError::new(
            HashMap::from([(known.clone(), 100), (unknown.clone(), 200)]),
            // Only one of the two truncated partitions has a known divergent
            // offset — Java's documented case.
            HashMap::from([(known.clone(), OffsetAndMetadata::new(95).unwrap())]),
        );
        let parts: HashSet<&TopicPartition> = e.partitions().collect();
        assert_eq!(parts, HashSet::from([&known, &unknown]));
        assert!(!e.divergent_offsets().contains_key(&unknown));
    }

    /// `LogTruncationException extends OffsetOutOfRangeException extends
    /// InvalidOffsetException extends KafkaException`
    /// (`LogTruncationException.java:29`), where the middle two are the
    /// CONSUMER package's classes — so it answers to both consumer predicates
    /// and to neither `ApiException` nor `RetriableException`.
    #[test]
    fn test_log_truncation_error_hierarchy() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(
            HashMap::from([(tp.clone(), 100)]),
            HashMap::from([(tp, OffsetAndMetadata::new(95).unwrap())]),
        )));
        assert!(e.is_kafka_error());
        assert!(e.is_consumer_invalid_offset_error());
        assert!(e.is_consumer_offset_out_of_range_error());
        assert!(!e.is_api_error());
        assert!(!e.is_retriable_error());
    }
}
