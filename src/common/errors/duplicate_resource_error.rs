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

//! Translated from `org.apache.kafka.common.errors.DuplicateResourceException`.

use std::fmt;

use crate::common::Error;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::protocol::Errors;

/// A request illegally referred to the same resource twice.
///
/// Corresponds to Java's `DuplicateResourceException`, error code [`Errors::DuplicateResource`].
///
/// Java `extends` chain:
///    `DuplicateResourceException` -> `ApiException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because it
/// carries Java's `resource` field and `resource()` accessor.
#[derive(Clone, Debug)]
pub struct DuplicateResourceError {
    message: String,
    resource: Option<String>,
    /// The underlying cause — Java's `DuplicateResourceException(String, String, Throwable)`.
    source: Option<Box<Error>>,
}

impl DuplicateResourceError {
    /// Create the error with the given message and no resource — Java's
    /// `DuplicateResourceException(String message)`.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), resource: None, source: None }
    }

    /// Create the error with the code's default message — used by
    /// [`Errors::error`](crate::common::protocol::Errors::error).
    pub fn with_default_message() -> Self {
        Self::new(Errors::DuplicateResource.message())
    }

    /// Create the error naming the offending resource — Java's
    /// `DuplicateResourceException(String resource, String message)`.
    pub fn with_resource(resource: impl Into<String>, message: impl Into<String>) -> Self {
        Self { message: message.into(), resource: Some(resource.into()), source: None }
    }

    /// The offending resource, or `None` if not recorded.
    pub fn resource(&self) -> Option<&str> {
        self.resource.as_deref()
    }

    /// Create the error with a resource, message, and underlying cause,
    /// mirroring Java's `DuplicateResourceException(String resource, String message, Throwable cause)`.
    pub fn with_resource_and_source(resource: impl Into<String>, message: impl Into<String>, source: Error) -> Self {
        Self {
            message: message.into(),
            resource: Some(resource.into()),
            source: Some(Box::new(source)),
        }
    }

    /// The underlying cause, if any. Mirrors Java's `getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DuplicateResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuplicateResourceError: {}", self.message)
    }
}

impl ErrorMessage for DuplicateResourceError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for DuplicateResourceError {
    fn error(&self) -> Errors {
        Errors::DuplicateResource
    }
}

impl ErrorHierarchy for DuplicateResourceError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
}

impl ErrorSource for DuplicateResourceError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}
impl std::error::Error for DuplicateResourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
