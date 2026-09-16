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

//! Translated from `org.apache.kafka.common.errors.RecordTooLargeException`.

use std::collections::HashMap;
use std::fmt;

use crate::common::Errors;
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorName, ErrorSource};
use crate::common::{Error, TopicPartition};

/// The request included a message larger than the max message size the
/// server will accept.
///
/// Corresponds to Java's `RecordTooLargeException`, error code
/// [`Errors::MessageTooLarge`].
///
/// Java `extends` chain:
///    `RecordTooLargeException` -> `ApiException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because it
/// carries Java's `recordTooLargePartitions` field and accessor.
#[derive(Clone, Debug)]
pub struct RecordTooLargeError {
    message: String,
    record_too_large_partitions: Option<HashMap<TopicPartition, i64>>,
    /// The underlying cause, translating Java's `Throwable` `cause`.
    source: Option<Box<Error>>,
}

impl RecordTooLargeError {
    /// Create the error with the given message and no partitions — Java's
    /// `RecordTooLargeException(String message)` (`RecordTooLargeException.java:39`).
    ///
    /// The three translated constructors intersect on `{message}`, which is
    /// exactly this one — so it keeps the plain name and the other two are
    /// suffixed with their parameters beyond the intersection (CLAUDE.md §2).
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), record_too_large_partitions: None, source: None }
    }

    /// Create the error with the given message and an underlying cause,
    /// mirroring Java's `RecordTooLargeException(String message, Throwable cause)`
    /// (`:35`). Suffixed per [`new`](Self::new).
    pub fn with_source(message: impl Into<String>, source: Error) -> Self {
        Self {
            message: message.into(),
            record_too_large_partitions: None,
            source: Some(Box::new(source)),
        }
    }

    /// Create the error with the code's default message — used by
    /// [`Errors::error`](crate::common::Errors::error).
    pub fn with_default_message() -> Self {
        Self::new(Errors::MessageTooLarge.message())
    }

    /// Create the error naming the offending partitions, mirroring Java's
    /// `RecordTooLargeException(String message, Map<TopicPartition, Long> recordTooLargePartitions)`
    /// (`:47`). Suffixed per [`new`](Self::new).
    pub fn with_record_too_large_partitions(
        message: impl Into<String>,
        record_too_large_partitions: HashMap<TopicPartition, i64>,
    ) -> Self {
        Self {
            message: message.into(),
            record_too_large_partitions: Some(record_too_large_partitions),
            source: None,
        }
    }

    /// The per-partition record size that exceeded the limit, or `None` if
    /// not recorded. Mirrors Java's `recordTooLargePartitions()`.
    pub fn record_too_large_partitions(&self) -> Option<&HashMap<TopicPartition, i64>> {
        self.record_too_large_partitions.as_ref()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The underlying cause, if any. Mirrors Java's `getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl fmt::Display for RecordTooLargeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordTooLargeError: {}", self.message)
    }
}

impl ErrorMessage for RecordTooLargeError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for RecordTooLargeError {
    fn error(&self) -> Errors {
        Errors::MessageTooLarge
    }
}

impl ErrorName for RecordTooLargeError {
    fn name(&self) -> &'static str {
        "RecordTooLargeError"
    }
}

impl ErrorHierarchy for RecordTooLargeError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
}

impl ErrorSource for RecordTooLargeError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl std::error::Error for RecordTooLargeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_no_partitions() {
        let err = RecordTooLargeError::new("too big");
        assert_eq!(err.message(), "too big");
        assert!(err.record_too_large_partitions().is_none());
    }

    #[test]
    fn new_record_too_large_partitions_records_the_map() {
        let mut partitions = HashMap::new();
        partitions.insert(TopicPartition::new("topic", 0), 42_i64);
        let err = RecordTooLargeError::with_record_too_large_partitions("too big", partitions.clone());
        assert_eq!(err.message(), "too big");
        assert_eq!(err.record_too_large_partitions(), Some(&partitions));
    }
}
