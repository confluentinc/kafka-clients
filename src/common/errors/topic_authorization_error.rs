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

//! Translated from `org.apache.kafka.common.errors.TopicAuthorizationException`.

use std::collections::HashSet;
use std::fmt;

use ambassador::Delegate;

use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::{Error, KafkaError};
// Ambassador exports its generated helper macros at the crate root; a
// `#[delegate]` outside the trait's own module has to import them.
use crate::common::kafka_error::{
    ambassador_impl_ErrorCode, ambassador_impl_ErrorMessage, ambassador_impl_ErrorSource,
};
use crate::common::protocol::Errors;

use super::format_java_set;

/// Topic authorization failure with the set of unauthorized topics.
///
/// Corresponds to Java's `TopicAuthorizationException`, error code
/// [`Errors::TopicAuthorizationFailed`].
///
/// Java `extends` chain:
///    `TopicAuthorizationException` -> `AuthorizationException` ->
///   `InvalidConfigurationException` -> `ApiException` -> `KafkaException`
///
/// Fatal per `RequestUtils.isFatalException`.
#[derive(Clone, Debug, Delegate)]
// Two delegations to the same field. Ambassador takes one trait per
// `#[delegate]`, so the repeated `target` key is unavoidable; clippy's
// `duplicated_attributes` reads it as a copy-paste slip.
#[allow(clippy::duplicated_attributes)]
#[delegate(ErrorMessage, target = "kafka_error")]
#[delegate(ErrorSource, target = "kafka_error")]
#[delegate(ErrorCode, target = "kafka_error")]
pub struct TopicAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The set of unauthorized topics.
    unauthorized_topics: HashSet<String>,
}

impl TopicAuthorizationError {
    /// Create a topic authorization error, formatting the topics into the
    /// message.
    ///
    /// Mirrors Java's `TopicAuthorizationException(Set<String> unauthorizedTopics)`:
    /// `this("Not authorized to access topics: " + unauthorizedTopics, unauthorizedTopics)`.
    pub fn new(unauthorized_topics: HashSet<String>) -> Self {
        let message = format!("Not authorized to access topics: {}", format_java_set(&unauthorized_topics));
        Self {
            kafka_error: KafkaError::new_message(Errors::TopicAuthorizationFailed, message),
            unauthorized_topics,
        }
    }

    /// Create a topic authorization error with the code's default message and no
    /// topics.
    ///
    /// Mirrors Java's `TopicAuthorizationException(String message)` reached via
    /// the `Errors.TOPIC_AUTHORIZATION_FAILED` builder (`exception()`), where the
    /// message is the default constant and the topic set is empty.
    pub fn with_default_message() -> Self {
        Self {
            kafka_error: KafkaError::new(Errors::TopicAuthorizationFailed),
            unauthorized_topics: HashSet::new(),
        }
    }

    /// Create a topic authorization error carrying a custom message.
    ///
    /// Mirrors Java's `TopicAuthorizationException(String message, Set<String>)`
    /// (`TopicAuthorizationException.java:25`), where the message is
    /// caller-supplied rather than the code's default text.
    ///
    /// The two translated constructors intersect on `{unauthorizedTopics}`, which
    /// is exactly `TopicAuthorizationException(Set<String>)` (`:30`) — so
    /// [`new`](Self::new) keeps the plain name and this one is suffixed with the
    /// parameter beyond the intersection (CLAUDE.md §2).
    pub fn new_message(unauthorized_topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::new_message(Errors::TopicAuthorizationFailed, message),
            unauthorized_topics,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }

    /// The set of unauthorized topics. Mirrors Java's
    /// `TopicAuthorizationException.unauthorizedTopics()`.
    pub fn unauthorized_topics(&self) -> &HashSet<String> {
        &self.unauthorized_topics
    }
}

impl fmt::Display for TopicAuthorizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `<ClassName>: <message>`, matching every other error's `Display`
        // (Java's `Throwable.toString()`); the topics are already in the
        // message for the `new` path.
        write!(f, "TopicAuthorizationError: {}", self.message())
    }
}

impl ErrorHierarchy for TopicAuthorizationError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
    fn is_invalid_configuration_error(&self) -> bool {
        true
    }
    fn is_authorization_error(&self) -> bool {
        true
    }
}

impl TopicAuthorizationError {
    /// The underlying source, held by the embedded [`KafkaError`] base — Java's
    /// subclass passes its `cause` up to `super(message, cause)`. Mirrors
    /// `Throwable.getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.kafka_error.source()
    }
}

impl std::error::Error for TopicAuthorizationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kafka_error.source().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
