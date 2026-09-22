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

//! Translated from `org.apache.kafka.common.errors.ResourceNotFoundException`.

use std::fmt;

use crate::common::Error;
use crate::common::Errors;
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// A request referred to a resource that does not exist.
///
/// Corresponds to Java's `ResourceNotFoundException`, error code [`Errors::ResourceNotFound`].
///
/// Java `extends` chain:
///    `ResourceNotFoundException` -> `ApiException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because it
/// carries Java's `resource` field and `resource()` accessor.
#[derive(Clone, Debug)]
pub struct ResourceNotFoundError {
    message: String,
    resource: Option<String>,
    /// The underlying cause — Java's `ResourceNotFoundException(String, String, Throwable)`.
    source: Option<Box<Error>>,
}

impl ResourceNotFoundError {
    /// Create the error with the given message and no resource — Java's
    /// `ResourceNotFoundException(String message)`.
    ///
    /// The three translated constructors intersect on `{message}`, which is
    /// exactly this one — so it keeps the plain name and the other two are
    /// suffixed with their parameters beyond the intersection (CLAUDE.md §2).
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), resource: None, source: None }
    }

    /// Create the error with the code's default message — used by
    /// [`Errors::error`](crate::common::Errors::error).
    pub fn with_default_message() -> Self {
        Self::new(Errors::ResourceNotFound.message())
    }

    /// Create the error naming the offending resource — Java's
    /// `ResourceNotFoundException(String resource, String message)`.
    /// Suffixed per [`new`](Self::new).
    pub fn with_resource(resource: impl Into<String>, message: impl Into<String>) -> Self {
        Self { message: message.into(), resource: Some(resource.into()), source: None }
    }

    /// The offending resource, or `None` if not recorded.
    pub fn resource(&self) -> Option<&str> {
        self.resource.as_deref()
    }

    /// Create the error with a resource, message, and underlying cause,
    /// mirroring Java's `ResourceNotFoundException(String resource, String message, Throwable cause)`.
    /// Suffixed per [`new`](Self::new).
    pub fn with_resource_source(resource: impl Into<String>, message: impl Into<String>, source: Error) -> Self {
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

impl fmt::Display for ResourceNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResourceNotFoundError: {}", self.message)
    }
}

impl ErrorMessage for ResourceNotFoundError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ResourceNotFoundError {
    fn error(&self) -> Errors {
        Errors::ResourceNotFound
    }
}

impl ErrorHierarchy for ResourceNotFoundError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_api_error(&self) -> bool {
        true
    }
}

impl ErrorSource for ResourceNotFoundError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}
impl std::error::Error for ResourceNotFoundError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}
