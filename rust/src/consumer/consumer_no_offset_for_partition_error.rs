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

//! Translated from `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.

use std::collections::HashSet;
use std::fmt;

use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// Indicates that there is no stored offset for a partition and no defined
/// offset reset policy.
// Java `extends` chain: `NoOffsetForPartitionException` ->
// `InvalidOffsetException` -> `KafkaException`, the consumer package's abstract
// class, not `common.errors`' (an `ApiException`); hence
// `is_consumer_invalid_offset_error`, not `is_invalid_offset_error`. It has no
// entry in `Errors`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.NoOffsetForPartitionException")]
pub struct ConsumerNoOffsetForPartitionError {
    message: String,
    /// The partitions with no defined offset.
    pub(crate) partitions: HashSet<TopicPartition>,
}

impl ConsumerNoOffsetForPartitionError {
    /// Constructs a new `ConsumerNoOffsetForPartitionError` for a single
    /// partition.
    ///
    /// # Arguments
    ///
    /// - `partition` - The partition for which no offset is defined
    #[doc(alias = "org.apache.kafka.clients.consumer.NoOffsetForPartitionException#NoOffsetForPartitionException")]
    pub fn new(partition: TopicPartition) -> Self {
        // The singular "partition:", where `with_partitions` writes "partitions:".
        let message = format!("Undefined offset with no reset policy for partition: {partition}");
        Self { message, partitions: HashSet::from([partition]) }
    }

    /// Constructs a new `ConsumerNoOffsetForPartitionError` for multiple
    /// partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions for which no offset is defined
    #[doc(alias = "org.apache.kafka.clients.consumer.NoOffsetForPartitionException#NoOffsetForPartitionException")]
    pub fn with_partitions(partitions: impl IntoIterator<Item = TopicPartition>) -> Self {
        let partitions: HashSet<TopicPartition> = partitions.into_iter().collect();
        let mut items: Vec<String> = partitions.iter().map(ToString::to_string).collect();
        items.sort();
        let message = format!("Undefined offset with no reset policy for partitions: [{}]", items.join(", "));
        Self { message, partitions }
    }

    /// Returns all partitions for which no offsets are defined.
    ///
    /// # Returns
    ///
    /// All partitions without offsets
    #[doc(alias = "org.apache.kafka.clients.consumer.NoOffsetForPartitionException#partitions")]
    pub fn partitions(&self) -> &HashSet<TopicPartition> {
        &self.partitions
    }

    /// Returns the error message.
    // Java's Throwable.getMessage, a JDK method
    #[doc(alias = "rust-only")]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerNoOffsetForPartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerNoOffsetForPartitionError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerNoOffsetForPartitionError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerNoOffsetForPartitionError {}

impl ErrorHierarchy for ConsumerNoOffsetForPartitionError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_consumer_invalid_offset_error(&self) -> bool {
        true
    }
    fn is_consumer_no_offset_for_partition_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ConsumerNoOffsetForPartitionError {
    // `NoOffsetForPartitionException` exposes no `Throwable cause` constructor,
    // so its cause is always null in Java; the trait default is that answer.
}

impl ConsumerNoOffsetForPartitionError {
    /// Returns the cause of this error: always `None`, as no constructor takes
    /// one.
    // An inherent method, so it shadows both `ErrorSource::source` and
    // `std::error::Error::source`, keeping `x.source()` unambiguous and typed.
    // Java's Throwable.getCause, a JDK method
    #[doc(alias = "rust-only")]
    pub fn source(&self) -> Option<&Error> {
        None
    }
}

impl std::error::Error for ConsumerNoOffsetForPartitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Option::<&Error>::None.map(|e| e as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Recovered from `master:src/consumer/errors.rs`
    /// (`test_no_offset_for_partition_singular_message`), which was dropped when
    /// that file was split into one file per error class.
    ///
    /// Java: `NoOffsetForPartitionException(TopicPartition)` renders the
    /// SINGULAR "partition:" (`org.apache.kafka.clients.consumer.NoOffsetForPartitionException#NoOffsetForPartitionException(TopicPartition)`).
    #[test]
    fn test_no_offset_for_partition_singular_message() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = ConsumerNoOffsetForPartitionError::new(tp);
        assert_eq!(e.message(), "Undefined offset with no reset policy for partition: t-0");
    }

    /// Recovered from `master:src/consumer/errors.rs`
    /// (`test_no_offset_for_partitions_plural_message`).
    ///
    /// Java: `NoOffsetForPartitionException(Collection<TopicPartition>)` renders
    /// the PLURAL "partitions:" followed by the collection
    /// (`org.apache.kafka.clients.consumer.NoOffsetForPartitionException#NoOffsetForPartitionException(Collection)`), and Java's
    /// `Collection.toString()` brackets the items — hence `[t-0, t-1]`. The
    /// exact encoding is asserted here because the split changed it (the old
    /// Rust form emitted the items bare) and no test noticed.
    #[test]
    fn test_no_offset_for_partitions_plural_message() {
        let e = ConsumerNoOffsetForPartitionError::with_partitions([
            TopicPartition::new("t".to_string(), 0),
            TopicPartition::new("t".to_string(), 1),
        ]);
        assert_eq!(e.message(), "Undefined offset with no reset policy for partitions: [t-0, t-1]");
    }

    /// Recovered from `master:src/consumer/errors.rs`
    /// (`test_no_offset_for_partition_partitions_accessor`).
    ///
    /// Java: `partitions()` — the one member `InvalidOffsetException` declares
    /// abstract (`org.apache.kafka.clients.consumer.InvalidOffsetException#partitions`).
    #[test]
    fn test_no_offset_for_partition_partitions_accessor() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = ConsumerNoOffsetForPartitionError::new(tp.clone());
        let parts = e.partitions();
        assert_eq!(parts.len(), 1);
        assert!(parts.contains(&tp));
    }

    /// `NoOffsetForPartitionException extends InvalidOffsetException extends
    /// KafkaException` (`org.apache.kafka.clients.consumer.NoOffsetForPartitionException`), where
    /// `InvalidOffsetException` is the CONSUMER package's abstract class — not
    /// the `common.errors` class of the same name, which extends
    /// `ApiException`.
    #[test]
    fn test_no_offset_for_partition_error_hierarchy() {
        let e = Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(TopicPartition::new(
            "t".to_string(),
            0,
        )));
        assert!(e.is_kafka_error());
        assert!(e.is_consumer_invalid_offset_error());
        assert!(!e.is_api_error());
        assert!(!e.is_retriable_error());
    }
}
