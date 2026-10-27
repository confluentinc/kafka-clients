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
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};
use crate::common::protocol::Errors;

/// Error thrown due to a request for a resource that does not exist.
///
/// # Translation notes
///
/// Error code `Errors::ResourceNotFound`.
// Hand-written rather than declared with `kafka_error_type!` because it carries Java's
// `resource` field and `resource()` accessor.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException")]
pub struct ResourceNotFoundError {
    message: String,
    resource: Option<String>,
    /// The underlying cause — Java's `ResourceNotFoundException(String, String, Throwable)`.
    source: Option<Box<Error>>,
}

impl ResourceNotFoundError {
    /// Constructor
    ///
    /// # Arguments
    ///
    /// - `message` - the error's message
    #[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException#ResourceNotFoundException")]
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), resource: None, source: None }
    }

    /// Creates the error with the default message of `Errors::ResourceNotFound`, as the
    /// broker's error code is turned into an error.
    #[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException#ResourceNotFoundException")]
    pub fn with_default_message() -> Self {
        Self::new(Errors::ResourceNotFound.message())
    }

    /// Constructor
    ///
    /// # Arguments
    ///
    /// - `resource` - the (potentially `None`) resource that was not found
    /// - `message` - the error's message
    #[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException#ResourceNotFoundException")]
    pub fn with_resource(resource: impl Into<String>, message: impl Into<String>) -> Self {
        Self { message: message.into(), resource: Some(resource.into()), source: None }
    }

    /// # Returns
    ///
    /// the (potentially `None`) resource that was not found
    #[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException#resource")]
    pub fn resource(&self) -> Option<&str> {
        self.resource.as_deref()
    }

    /// Constructor
    ///
    /// # Arguments
    ///
    /// - `resource` - the (potentially `None`) resource that was not found
    /// - `message` - the error's message
    /// - `source` - the error's cause
    #[doc(alias = "org.apache.kafka.common.errors.ResourceNotFoundException#ResourceNotFoundException")]
    pub fn with_resource_source(resource: impl Into<String>, message: impl Into<String>, source: Error) -> Self {
        Self {
            message: message.into(),
            resource: Some(resource.into()),
            source: Some(Box::new(source)),
        }
    }

    /// Returns the underlying cause, if any: Java's `getCause()`.
    // Java's Throwable.getCause, a JDK method
    #[doc(alias = "rust-only")]
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }

    /// Returns the error message: Java's `getMessage()`.
    // Java's Throwable.getMessage, a JDK method
    #[doc(alias = "rust-only")]
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
    fn is_resource_not_found_error(&self) -> bool {
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
