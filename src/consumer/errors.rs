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

//! Consumer-specific error hierarchy.
//!
//! Translated from:
//! - `org.apache.kafka.clients.consumer.CommitFailedException`
//! - `org.apache.kafka.clients.consumer.RetriableCommitFailedException`
//! - `org.apache.kafka.clients.consumer.InvalidOffsetException` (abstract)
//! - `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`
//! - `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`
//! - `org.apache.kafka.clients.consumer.LogTruncationException`

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndMetadata;

/// Default message used by Java's `CommitFailedException` no-arg constructor.
///
/// Kept as a constant so we can assert exact equality in tests.
pub const COMMIT_FAILED_DEFAULT_MESSAGE: &str = "Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.";

/// Default message used by Java's `RetriableCommitFailedException(Throwable)`.
pub const RETRIABLE_COMMIT_FAILED_DEFAULT_MESSAGE: &str =
    "Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.";

/// Consumer-specific error hierarchy.
///
/// Convertible into [`Error`] via `From<ConsumerError>`. Variants mirror
/// the Java exception subclasses so test assertions on classification
/// (`is_retriable`, etc.) and message content can be preserved.
#[derive(Clone, Debug)]
pub enum ConsumerError {
    /// Offset commit failed with an unrecoverable error.
    ///
    /// Corresponds to Java's `CommitFailedException`.
    CommitFailed {
        /// Error message.
        message: String,
        /// Optional cause carried from a lower-level error.
        cause: Option<Box<Error>>,
    },
    /// Offset commit failed with a retriable error.
    ///
    /// Corresponds to Java's `RetriableCommitFailedException`.
    RetriableCommitFailed {
        /// Error message.
        message: String,
        /// Optional cause carried from a lower-level error.
        cause: Option<Box<Error>>,
    },
    /// No offset is stored for one or more partitions and no reset policy is
    /// defined.
    ///
    /// Corresponds to Java's `NoOffsetForPartitionException`.
    NoOffsetForPartition {
        /// Partitions for which no offsets are defined.
        partitions: HashSet<TopicPartition>,
    },
    /// One or more fetch offsets are out of range and no reset policy is
    /// defined.
    ///
    /// Corresponds to Java's `OffsetOutOfRangeException`.
    OffsetOutOfRange {
        /// Custom message, or `None` to use the default format.
        message: Option<String>,
        /// Map of out-of-range fetch offsets per partition.
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
    },
    /// Log truncation detected: divergent offsets were observed for one or
    /// more partitions.
    ///
    /// Corresponds to Java's `LogTruncationException` (which extends
    /// `OffsetOutOfRangeException`).
    LogTruncation {
        /// Custom message, or `None` to use the default format.
        message: Option<String>,
        /// Fetch offsets that were out of range (inherited from
        /// `OffsetOutOfRangeException`).
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
        /// First offset known to diverge from what the consumer read, per
        /// partition.
        divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    },
}

impl ConsumerError {
    /// `CommitFailedException` with the canonical default message.
    pub fn commit_failed_default() -> Self {
        Self::CommitFailed { message: COMMIT_FAILED_DEFAULT_MESSAGE.to_string(), cause: None }
    }

    /// `CommitFailedException(String message)`.
    pub fn commit_failed(message: impl Into<String>) -> Self {
        Self::CommitFailed { message: message.into(), cause: None }
    }

    /// `RetriableCommitFailedException(Throwable t)` — uses the default
    /// message and stores the cause.
    pub fn retriable_commit_failed_with_cause(cause: Error) -> Self {
        Self::RetriableCommitFailed {
            message: RETRIABLE_COMMIT_FAILED_DEFAULT_MESSAGE.to_string(),
            cause: Some(Box::new(cause)),
        }
    }

    /// `RetriableCommitFailedException(String message)`.
    pub fn retriable_commit_failed(message: impl Into<String>) -> Self {
        Self::RetriableCommitFailed { message: message.into(), cause: None }
    }

    /// `NoOffsetForPartitionException(TopicPartition)`.
    pub fn no_offset_for_partition(partition: TopicPartition) -> Self {
        let mut partitions = HashSet::with_capacity(1);
        partitions.insert(partition);
        Self::NoOffsetForPartition { partitions }
    }

    /// `NoOffsetForPartitionException(Collection<TopicPartition>)`.
    pub fn no_offset_for_partitions(partitions: impl IntoIterator<Item = TopicPartition>) -> Self {
        Self::NoOffsetForPartition { partitions: partitions.into_iter().collect() }
    }

    /// `OffsetOutOfRangeException(Map<TopicPartition, Long>)`.
    pub fn offset_out_of_range(offset_out_of_range_partitions: HashMap<TopicPartition, i64>) -> Self {
        Self::OffsetOutOfRange { message: None, offset_out_of_range_partitions }
    }

    /// `LogTruncationException(fetchOffsets, divergentOffsets)`.
    pub fn log_truncation(
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
        divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self {
        Self::LogTruncation { message: None, offset_out_of_range_partitions, divergent_offsets }
    }

    /// Returns the set of partitions for which an offset is invalid
    /// (mirrors Java's abstract `InvalidOffsetException.partitions()`).
    ///
    /// Returns `None` for variants that do not have a partition set
    /// (commit-failed variants).
    pub fn partitions(&self) -> Option<HashSet<TopicPartition>> {
        match self {
            Self::NoOffsetForPartition { partitions } => Some(partitions.clone()),
            Self::OffsetOutOfRange { offset_out_of_range_partitions, .. }
            | Self::LogTruncation { offset_out_of_range_partitions, .. } => {
                Some(offset_out_of_range_partitions.keys().cloned().collect())
            },
            Self::CommitFailed { .. } | Self::RetriableCommitFailed { .. } => None,
        }
    }

    /// Returns the map of out-of-range partitions and their fetch offsets
    /// (for `OffsetOutOfRange` and `LogTruncation` variants).
    pub fn offset_out_of_range_partitions(&self) -> Option<&HashMap<TopicPartition, i64>> {
        match self {
            Self::OffsetOutOfRange { offset_out_of_range_partitions, .. }
            | Self::LogTruncation { offset_out_of_range_partitions, .. } => Some(offset_out_of_range_partitions),
            _ => None,
        }
    }

    /// Returns the divergent offsets for the partitions which were truncated.
    pub fn divergent_offsets(&self) -> Option<&HashMap<TopicPartition, OffsetAndMetadata>> {
        match self {
            Self::LogTruncation { divergent_offsets, .. } => Some(divergent_offsets),
            _ => None,
        }
    }

    /// Whether this error is retriable. Mirrors Java's exception hierarchy:
    /// only `RetriableCommitFailedException` is retriable in this set.
    pub fn is_retriable(&self) -> bool {
        matches!(self, Self::RetriableCommitFailed { .. })
    }
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitFailed { message, .. } | Self::RetriableCommitFailed { message, .. } => f.write_str(message),
            Self::NoOffsetForPartition { partitions } => {
                if partitions.len() == 1 {
                    // Java's single-partition constructor uses the singular
                    // form "partition: <tp>".
                    let only = partitions.iter().next().expect("len==1");
                    write!(f, "Undefined offset with no reset policy for partition: {}", only)
                } else {
                    write!(
                        f,
                        "Undefined offset with no reset policy for partitions: {}",
                        format_partition_set(partitions)
                    )
                }
            },
            Self::OffsetOutOfRange { message, offset_out_of_range_partitions } => {
                if let Some(msg) = message {
                    f.write_str(msg)
                } else {
                    write!(
                        f,
                        "Offsets out of range with no configured reset policy for partitions: {}",
                        format_offset_map(offset_out_of_range_partitions)
                    )
                }
            },
            Self::LogTruncation { message, divergent_offsets, .. } => {
                if let Some(msg) = message {
                    f.write_str(msg)
                } else {
                    write!(
                        f,
                        "Truncated partitions detected with divergent offsets {}",
                        format_metadata_map(divergent_offsets)
                    )
                }
            },
        }
    }
}

impl std::error::Error for ConsumerError {}

impl From<ConsumerError> for Error {
    /// Convert to the unified [`Error`] enum.
    ///
    /// `RetriableCommitFailed` is mapped through a generic
    /// `Error::with_message` whose underlying [`crate::common::protocol::Errors`]
    /// is retriable (`RequestTimedOut`), so `Error::is_retriable()` returns
    /// `true` for it. All other variants flow through `IllegalState` (matching
    /// Java's classification of these as non-retriable `KafkaException`s).
    ///
    /// NOTE (Phase 1 design choice, per Critic comment #3): this mapping
    /// flattens `OffsetOutOfRange`, `NoOffsetForPartition`, `LogTruncation`,
    /// and `InvalidOffset` into `Error::IllegalState`. The underlying
    /// classification (e.g. `Errors::OffsetOutOfRange`) is preserved on
    /// the `ConsumerError` itself but lost after the `?`-conversion.
    /// Callers needing structured classification (e.g. matching on
    /// `Errors::OffsetOutOfRange`) should pattern-match on `ConsumerError`
    /// directly before propagating with `?`, or use the dedicated accessors
    /// (`offset_out_of_range_partitions`, `divergent_offsets`, `partitions`).
    /// This is by design for Phase 1; a future phase may revisit and route
    /// these variants through their concrete `Errors::*` codes.
    fn from(e: ConsumerError) -> Self {
        match &e {
            ConsumerError::RetriableCommitFailed { .. } => {
                Error::with_message(crate::common::protocol::Errors::RequestTimedOut, e.to_string())
            },
            _ => Error::illegal_state(e.to_string()),
        }
    }
}

// Java prints collections via `AbstractCollection.toString()` which produces
// `[a, b, c]`. We replicate that loosely for test stability — order is
// platform-dependent in Java for a `HashSet`/`HashMap` too, so tests only
// assert on the partition list contents being present and the prefix.
fn format_partition_set(set: &HashSet<TopicPartition>) -> String {
    let mut items: Vec<String> = set.iter().map(ToString::to_string).collect();
    items.sort();
    format!("[{}]", items.join(", "))
}

fn format_offset_map(map: &HashMap<TopicPartition, i64>) -> String {
    let mut items: Vec<String> = map.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
    items.sort();
    format!("{{{}}}", items.join(", "))
}

fn format_metadata_map(map: &HashMap<TopicPartition, OffsetAndMetadata>) -> String {
    let mut items: Vec<String> = map.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
    items.sort();
    format!("{{{}}}", items.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_failed_default_message() {
        let e = ConsumerError::commit_failed_default();
        assert_eq!(e.to_string(), COMMIT_FAILED_DEFAULT_MESSAGE);
        assert!(!e.is_retriable());
    }

    #[test]
    fn test_commit_failed_custom_message() {
        let e = ConsumerError::commit_failed("custom");
        assert_eq!(e.to_string(), "custom");
    }

    #[test]
    fn test_retriable_commit_failed_default_message() {
        let cause = Error::illegal_state("inner");
        let e = ConsumerError::retriable_commit_failed_with_cause(cause);
        assert_eq!(e.to_string(), RETRIABLE_COMMIT_FAILED_DEFAULT_MESSAGE);
        assert!(e.is_retriable());
    }

    #[test]
    fn test_retriable_commit_failed_into_kafka_error_is_retriable() {
        let ce = ConsumerError::retriable_commit_failed("x");
        let ke: Error = ce.into();
        assert!(ke.is_retriable());
    }

    #[test]
    fn test_commit_failed_into_kafka_error_is_not_retriable() {
        let ce = ConsumerError::commit_failed("x");
        let ke: Error = ce.into();
        assert!(!ke.is_retriable());
    }

    #[test]
    fn test_no_offset_for_partition_singular_message() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = ConsumerError::no_offset_for_partition(tp);
        assert_eq!(e.to_string(), "Undefined offset with no reset policy for partition: t-0");
    }

    #[test]
    fn test_no_offset_for_partitions_plural_message() {
        let e = ConsumerError::no_offset_for_partitions([
            TopicPartition::new("t".to_string(), 0),
            TopicPartition::new("t".to_string(), 1),
        ]);
        let s = e.to_string();
        assert!(
            s.starts_with("Undefined offset with no reset policy for partitions: "),
            "got: {s}"
        );
        assert!(s.contains("t-0"));
        assert!(s.contains("t-1"));
    }

    #[test]
    fn test_no_offset_for_partition_partitions_accessor() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let e = ConsumerError::no_offset_for_partition(tp.clone());
        let parts = e.partitions().expect("has partitions");
        assert_eq!(parts.len(), 1);
        assert!(parts.contains(&tp));
    }

    #[test]
    fn test_offset_out_of_range_default_message_and_partitions() {
        let mut m = HashMap::new();
        let tp = TopicPartition::new("t".to_string(), 0);
        m.insert(tp.clone(), 5);
        let e = ConsumerError::offset_out_of_range(m);
        let s = e.to_string();
        assert!(
            s.starts_with("Offsets out of range with no configured reset policy for partitions: "),
            "got: {s}"
        );
        assert!(s.contains("t-0=5"));
        let parts = e.partitions().expect("has partitions");
        assert!(parts.contains(&tp));
        assert_eq!(e.offset_out_of_range_partitions().expect("has map").get(&tp), Some(&5));
    }

    #[test]
    fn test_log_truncation_default_message_and_accessors() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut fetch = HashMap::new();
        fetch.insert(tp.clone(), 100);
        let mut div = HashMap::new();
        div.insert(tp.clone(), OffsetAndMetadata::new(95).unwrap());
        let e = ConsumerError::log_truncation(fetch, div);
        let s = e.to_string();
        assert!(
            s.starts_with("Truncated partitions detected with divergent offsets "),
            "got: {s}"
        );
        assert!(e.divergent_offsets().expect("has").contains_key(&tp));
        assert!(e.offset_out_of_range_partitions().expect("has").contains_key(&tp));
        assert!(e.partitions().expect("has").contains(&tp));
    }
}
