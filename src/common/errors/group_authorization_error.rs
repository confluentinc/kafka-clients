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

use crate::common::KafkaError;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage};
// Ambassador exports its generated helper macros at the crate root; a
// `#[delegate]` outside the trait's own module has to import them.
use crate::common::kafka_error::{ambassador_impl_ErrorCode, ambassador_impl_ErrorMessage};
use crate::common::protocol::Errors;

/// Group authorization failure with the group ID.
///
/// Corresponds to Java's `GroupAuthorizationException`, error code
/// [`Errors::GroupAuthorizationFailed`].
///
/// Java `extends` chain:
///    `GroupAuthorizationException` -> `AuthorizationException` ->
///   `InvalidConfigurationException` -> `ApiException` -> `KafkaException`
///
/// Fatal per `RequestUtils.isFatalException`.
#[derive(Clone, Debug, Delegate)]
// Two delegations to the same field. Ambassador takes one trait per
// `#[delegate]`, so the repeated `target` key is unavoidable; clippy's
// `duplicated_attributes` reads it as a copy-paste slip.
#[allow(clippy::duplicated_attributes)]
#[delegate(ErrorMessage, target = "kafka_error")]
#[delegate(ErrorCode, target = "kafka_error")]
pub struct GroupAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The group ID that failed authorization.
    group_id: String,
}

impl GroupAuthorizationError {
    /// Create a group authorization error for a group ID, formatting the group
    /// into the message.
    ///
    /// Mirrors Java's static `GroupAuthorizationException.forGroupId(String)`:
    /// `new GroupAuthorizationException("Not authorized to access group: " + groupId, groupId)`.
    pub fn for_group_id(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        let message = format!("Not authorized to access group: {group_id}");
        Self {
            kafka_error: KafkaError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id,
        }
    }

    /// Create a group authorization error with the code's default message and no
    /// group ID.
    ///
    /// Mirrors Java's `GroupAuthorizationException(String message)` reached via
    /// the `Errors.GROUP_AUTHORIZATION_FAILED` builder (`exception()`), where the
    /// message is the default constant and `groupId` is null.
    pub fn with_default_message() -> Self {
        Self {
            kafka_error: KafkaError::new(Errors::GroupAuthorizationFailed),
            group_id: String::new(),
        }
    }

    /// Create a group authorization error carrying a custom message.
    ///
    /// Mirrors Java's `GroupAuthorizationException(String message, String groupId)`,
    /// where the exception message is caller-supplied rather than the default
    /// error text.
    pub fn with_message(group_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id: group_id.into(),
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }

    /// The group ID that failed authorization. Mirrors Java's
    /// `GroupAuthorizationException.groupId()`.
    pub fn group_id(&self) -> &str {
        &self.group_id
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
}
