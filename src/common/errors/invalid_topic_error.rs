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

//! Translated from `org.apache.kafka.common.errors.InvalidTopicException`.

use std::collections::HashSet;
use std::fmt;

use ambassador::Delegate;

use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::{Error, KafkaError};
// Ambassador exports its generated helper macros at the crate root; a
// `#[delegate]` outside the trait's own module has to import them.
use crate::common::Errors;
use crate::common::error::{ambassador_impl_ErrorCode, ambassador_impl_ErrorMessage, ambassador_impl_ErrorSource};

use super::format_java_set;

/// Invalid topic error with the set of invalid topics.
///
/// Corresponds to Java's `InvalidTopicException`, error code
/// [`Errors::InvalidTopicError`].
///
/// Java `extends` chain:
///    `InvalidTopicException` -> `InvalidConfigurationException` ->
///   `ApiException` -> `KafkaException`
#[derive(Clone, Debug, Delegate)]
// Two delegations to the same field. Ambassador takes one trait per
// `#[delegate]`, so the repeated `target` key is unavoidable; clippy's
// `duplicated_attributes` reads it as a copy-paste slip.
#[allow(clippy::duplicated_attributes)]
#[delegate(ErrorMessage, target = "kafka_error")]
#[delegate(ErrorSource, target = "kafka_error")]
#[delegate(ErrorCode, target = "kafka_error")]
pub struct InvalidTopicError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The set of invalid topics.
    invalid_topics: HashSet<String>,
}

impl InvalidTopicError {
    /// Create an invalid topic error, formatting the topics into the message.
    ///
    /// Mirrors Java's `InvalidTopicException(Set<String> invalidTopics)`:
    /// `super("Invalid topics: " + invalidTopics)`.
    pub fn new(invalid_topics: HashSet<String>) -> Self {
        let message = format!("Invalid topics: {}", format_java_set(&invalid_topics));
        Self {
            kafka_error: KafkaError::with_message(Errors::InvalidTopicError, message),
            invalid_topics,
        }
    }

    /// Create an invalid topic error with the code's default message and no
    /// topics.
    ///
    /// Mirrors Java's `InvalidTopicException(String message)` reached via the
    /// `Errors.INVALID_TOPIC_EXCEPTION` builder (`exception()`), where the
    /// message is the default constant and the topic set is empty.
    pub fn with_default_message() -> Self {
        Self {
            kafka_error: KafkaError::new(Errors::InvalidTopicError),
            invalid_topics: HashSet::new(),
        }
    }

    /// Create an invalid topic error carrying a custom message.
    ///
    /// Mirrors Java's `InvalidTopicException(String message, Set<String> invalidTopics)`
    /// (`InvalidTopicException.java:60`).
    ///
    /// The two translated constructors intersect on `{invalidTopics}`, which is
    /// exactly `InvalidTopicException(Set<String>)` (`:55`) — so [`new`](Self::new)
    /// keeps the plain name and this one is suffixed with the parameter beyond the
    /// intersection (CLAUDE.md §2).
    pub fn with_message(invalid_topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::with_message(Errors::InvalidTopicError, message),
            invalid_topics,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }

    /// The set of invalid topics. Mirrors Java's
    /// `InvalidTopicException.invalidTopics()`.
    pub fn invalid_topics(&self) -> &HashSet<String> {
        &self.invalid_topics
    }
}

impl fmt::Display for InvalidTopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `<ClassName>: <message>`, matching every other error's `Display`
        // (Java's `Throwable.toString()`); the topics are already in the
        // message for the `new` path.
        write!(f, "InvalidTopicError: {}", self.message())
    }
}

impl ErrorHierarchy for InvalidTopicError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
    fn is_invalid_configuration_error(&self) -> bool {
        true
    }
}

impl InvalidTopicError {
    /// The underlying source, held by the embedded [`KafkaError`] base — Java's
    /// subclass passes its `cause` up to `super(message, cause)`. Mirrors
    /// `Throwable.getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.kafka_error.source()
    }
}

impl std::error::Error for InvalidTopicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kafka_error.source().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
