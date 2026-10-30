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
use crate::common::error::{ambassador_impl_ErrorCode, ambassador_impl_ErrorMessage, ambassador_impl_ErrorSource};
use crate::common::protocol::Errors;

use super::format_java_set;

/// The client has attempted to perform an operation on an invalid topic.
/// For example the topic name is too long, contains invalid characters etc.
/// This error is not retriable because the operation won't suddenly become valid.
///
/// # See also
///
/// - `UnknownTopicOrPartitionError`
///
/// # Translation notes
///
/// Error code `Errors::InvalidTopicError`. Java `extends` chain: `InvalidTopicException` ->
/// `InvalidConfigurationException` -> `ApiException` -> `KafkaException`.
#[derive(Clone, Debug, Delegate)]
// Two delegations to the same field. Ambassador takes one trait per
// `#[delegate]`, so the repeated `target` key is unavoidable; clippy's
// `duplicated_attributes` reads it as a copy-paste slip.
#[expect(clippy::duplicated_attributes)]
#[delegate(ErrorMessage, target = "kafka_error")]
#[delegate(ErrorSource, target = "kafka_error")]
#[delegate(ErrorCode, target = "kafka_error")]
#[doc(alias = "org.apache.kafka.common.errors.InvalidTopicException")]
pub struct InvalidTopicError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The set of invalid topics.
    invalid_topics: HashSet<String>,
}

impl InvalidTopicError {
    /// Creates an invalid topic error with the message `"Invalid topics: [<topics>]"`.
    ///
    /// # Arguments
    ///
    /// - `invalid_topics` - the invalid topics
    #[doc(alias = "org.apache.kafka.common.errors.InvalidTopicException#InvalidTopicException")]
    pub fn new(invalid_topics: HashSet<String>) -> Self {
        let message = format!("Invalid topics: {}", format_java_set(&invalid_topics));
        Self {
            kafka_error: KafkaError::with_message(Errors::InvalidTopicError, message),
            invalid_topics,
        }
    }

    /// Creates an invalid topic error with the default message of `Errors::InvalidTopicError`
    /// and an empty set of topics, as the broker's error code is turned into an error.
    #[doc(alias = "org.apache.kafka.common.errors.InvalidTopicException#InvalidTopicException")]
    pub fn with_default_message() -> Self {
        Self {
            kafka_error: KafkaError::new(Errors::InvalidTopicError),
            invalid_topics: HashSet::new(),
        }
    }

    /// Creates an invalid topic error carrying a custom message.
    ///
    /// # Arguments
    ///
    /// - `invalid_topics` - the invalid topics
    /// - `message` - the error's message
    #[doc(alias = "org.apache.kafka.common.errors.InvalidTopicException#InvalidTopicException")]
    pub fn with_message(invalid_topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::with_message(Errors::InvalidTopicError, message),
            invalid_topics,
        }
    }

    /// Returns the embedded `KafkaError` base, holding the error code, message and source.
    #[doc(alias = "rust-only")]
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }

    /// Returns the set of invalid topics, empty when the error was not built from them.
    #[doc(alias = "org.apache.kafka.common.errors.InvalidTopicException#invalidTopics")]
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
    fn is_invalid_topic_error(&self) -> bool {
        true
    }
}

impl InvalidTopicError {
    /// Returns the underlying cause, if any, held by the embedded [`KafkaError`] base: Java's
    /// `getCause()`.
    #[doc(alias = "rust-only")]
    pub fn source(&self) -> Option<&Error> {
        self.kafka_error.source()
    }
}

impl std::error::Error for InvalidTopicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kafka_error.source().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
