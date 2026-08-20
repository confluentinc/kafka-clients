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

//! Translated from `org.apache.kafka.clients.consumer.CommitFailedException`.

use std::fmt;

use crate::common::Error;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// Default message used by Java's `CommitFailedException` no-arg constructor.
///
/// Kept as a constant so tests can assert exact equality.
pub const CONSUMER_COMMIT_FAILED_DEFAULT_MESSAGE: &str = "Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.";

/// An offset commit could not be completed because the group rebalanced.
///
/// Corresponds to Java's `CommitFailedException`. It has no entry in `Errors`,
/// so it carries no protocol code.
///
/// Java `extends` chain:
///    `CommitFailedException` -> `KafkaException`
///
/// Note it extends `KafkaException` directly, so it is NOT an `ApiException` —
/// unlike its sibling [`ConsumerRetriableCommitFailedError`](super::ConsumerRetriableCommitFailedError),
/// which goes through `RetriableException`.
#[derive(Clone, Debug)]
pub struct ConsumerCommitFailedError {
    message: String,
}

impl ConsumerCommitFailedError {
    /// Create the error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// Create the error with Java's no-arg constructor message.
    pub fn with_default_message() -> Self {
        Self::new(CONSUMER_COMMIT_FAILED_DEFAULT_MESSAGE)
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerCommitFailedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerCommitFailedError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerCommitFailedError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerCommitFailedError {}

impl ErrorHierarchy for ConsumerCommitFailedError {
    fn is_kafka_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ConsumerCommitFailedError {
    // `CommitFailedException` exposes no `Throwable cause` constructor, so its cause is
    // always null in Java; the trait default (`None`) is that answer.
}

impl ConsumerCommitFailedError {
    /// Always `None`: `CommitFailedException` exposes no `Throwable cause` constructor, so its
    /// cause is null in Java too. Present as an inherent method so it shadows
    /// both `ErrorSource::source` and `std::error::Error::source`, keeping
    /// `x.source()` unambiguous and typed.
    pub fn source(&self) -> Option<&Error> {
        None
    }
}

impl std::error::Error for ConsumerCommitFailedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Option::<&Error>::None.map(|e| e as &(dyn std::error::Error + 'static))
    }
}
