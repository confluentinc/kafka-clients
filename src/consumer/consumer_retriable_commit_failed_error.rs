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

//! Translated from `org.apache.kafka.clients.consumer.RetriableCommitFailedException`.

use std::fmt;

use crate::common::Error;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// Default message used by Java's `RetriableCommitFailedException(Throwable)`.
pub const CONSUMER_RETRIABLE_COMMIT_FAILED_DEFAULT_MESSAGE: &str =
    "Offset commit failed with a retriable error. You should retry committing the latest consumed offsets.";

/// An offset commit failed with a retriable error; committing the latest
/// consumed offsets again may succeed.
///
/// Corresponds to Java's `RetriableCommitFailedException`. It has no entry in
/// `Errors`, so it carries no protocol code.
///
/// Java `extends` chain:
///    `RetriableCommitFailedException` -> `RetriableException` ->
///   `ApiException` -> `KafkaException`
///
/// Being its own class is what makes it retriable. The previous mapping routed
/// it through [`Errors::RequestTimedOut`](crate::common::protocol::Errors::RequestTimedOut)
/// solely to borrow that code's retriability, which also made it
/// indistinguishable from a real `TimeoutException`.
///
/// Note the contrast with its sibling
/// [`ConsumerCommitFailedError`](super::ConsumerCommitFailedError), which extends
/// `KafkaException` directly and so is neither retriable nor an `ApiException`.
#[derive(Clone, Debug)]
pub struct ConsumerRetriableCommitFailedError {
    message: String,
    /// The underlying cause — Java's `RetriableCommitFailedException(Throwable)`.
    source: Option<Box<Error>>,
}

impl ConsumerRetriableCommitFailedError {
    /// Create the error with the given message —
    /// `RetriableCommitFailedException(String)`.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), source: None }
    }

    /// Create the error with Java's default message.
    pub fn with_default_message() -> Self {
        Self::new(CONSUMER_RETRIABLE_COMMIT_FAILED_DEFAULT_MESSAGE)
    }

    /// `RetriableCommitFailedException(Throwable t)` — uses the default message.
    ///
    /// The cause is retained and readable through [`Self::cause`] /
    /// [`Error::cause`], matching Java's `getCause()`.
    pub fn with_source(source: Error) -> Self {
        Self {
            message: CONSUMER_RETRIABLE_COMMIT_FAILED_DEFAULT_MESSAGE.to_string(),
            source: Some(Box::new(source)),
        }
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

impl fmt::Display for ConsumerRetriableCommitFailedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerRetriableCommitFailedError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerRetriableCommitFailedError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerRetriableCommitFailedError {}

impl ErrorHierarchy for ConsumerRetriableCommitFailedError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
    fn is_retriable_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ConsumerRetriableCommitFailedError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}
impl std::error::Error for ConsumerRetriableCommitFailedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
