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

//! Translated from `org.apache.kafka.common.errors.GroupAuthorizationException`.

use std::fmt;

use ambassador::Delegate;

use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::{Error, KafkaError};
// Ambassador exports its generated helper macros at the crate root; a
// `#[delegate]` outside the trait's own module has to import them.
use crate::common::error::{ambassador_impl_ErrorCode, ambassador_impl_ErrorMessage, ambassador_impl_ErrorSource};
use crate::common::protocol::Errors;

/// Group authorization failure with the group ID.
///
/// Error code `Errors::GroupAuthorizationFailed`. Java `extends` chain:
/// `GroupAuthorizationException` -> `AuthorizationException` -> `InvalidConfigurationException` ->
/// `ApiException` -> `KafkaException`. Fatal per `common::requests::request_utils::is_fatal_error`.
#[derive(Clone, Debug, Delegate)]
// Two delegations to the same field. Ambassador takes one trait per
// `#[delegate]`, so the repeated `target` key is unavoidable; clippy's
// `duplicated_attributes` reads it as a copy-paste slip.
#[expect(clippy::duplicated_attributes)]
#[delegate(ErrorMessage, target = "kafka_error")]
#[delegate(ErrorSource, target = "kafka_error")]
#[delegate(ErrorCode, target = "kafka_error")]
#[doc(alias = "org.apache.kafka.common.errors.GroupAuthorizationException")]
pub struct GroupAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The group ID that failed authorization, `None` when it is not known.
    group_id: Option<String>,
}

impl GroupAuthorizationError {
    /// Creates a group authorization error for a group ID, with the message
    /// `"Not authorized to access group: <group_id>"`.
    ///
    /// # Arguments
    ///
    /// - `group_id` - the group ID that failed authorization
    #[doc(alias = "org.apache.kafka.common.errors.GroupAuthorizationException#forGroupId")]
    pub fn for_group_id(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        let message = format!("Not authorized to access group: {group_id}");
        Self {
            kafka_error: KafkaError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id: Some(group_id),
        }
    }

    /// Creates a group authorization error with the default message of
    /// `Errors::GroupAuthorizationFailed` and no group ID, as the broker's error code is turned
    /// into an error.
    #[doc(alias = "org.apache.kafka.common.errors.GroupAuthorizationException#GroupAuthorizationException")]
    pub fn with_default_message() -> Self {
        Self { kafka_error: KafkaError::new(Errors::GroupAuthorizationFailed), group_id: None }
    }

    /// Creates a group authorization error carrying a custom message and no group ID.
    ///
    /// # Arguments
    ///
    /// - `message` - the error's message
    #[doc(alias = "org.apache.kafka.common.errors.GroupAuthorizationException#GroupAuthorizationException(String)")]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id: None,
        }
    }

    /// Creates a group authorization error carrying a custom message.
    ///
    /// # Arguments
    ///
    /// - `group_id` - the group ID that failed authorization
    /// - `message` - the error's message
    #[doc(
        alias = "org.apache.kafka.common.errors.GroupAuthorizationException#GroupAuthorizationException(String,String)"
    )]
    pub fn with_group_id(group_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id: Some(group_id.into()),
        }
    }

    /// Returns the embedded `KafkaError` base, holding the error code, message and source.
    #[doc(alias = "rust-only")]
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }

    /// Return the group ID that failed authorization. May be `None` if it is not known
    /// in the context the error was raised in.
    ///
    /// # Returns
    ///
    /// nullable group_id
    #[doc(alias = "org.apache.kafka.common.errors.GroupAuthorizationException#groupId")]
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }
}

impl fmt::Display for GroupAuthorizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `<ClassName>: <message>`, matching every other error's `Display`
        // (Java's `Throwable.toString()`); the group ID is already in the
        // message for the `for_group_id` path.
        write!(f, "GroupAuthorizationError: {}", self.message())
    }
}

impl ErrorHierarchy for GroupAuthorizationError {
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
    fn is_group_authorization_error(&self) -> bool {
        true
    }
}

impl GroupAuthorizationError {
    /// Returns the underlying cause, if any, held by the embedded [`KafkaError`] base: Java's
    /// `getCause()`.
    #[doc(alias = "rust-only")]
    pub fn source(&self) -> Option<&Error> {
        self.kafka_error.source()
    }
}

impl std::error::Error for GroupAuthorizationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kafka_error.source().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
