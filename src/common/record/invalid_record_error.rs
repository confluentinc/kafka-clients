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

//! Invalid record error type.
//!
//! Corresponds to Java's `org.apache.kafka.common.InvalidRecordException`.

use std::fmt;

/// Error indicating that a record is invalid or corrupt.
///
/// This is thrown when a record fails validation during deserialization,
/// for example due to incorrect sizes, malformed varints, or other
/// structural problems.
///
/// Corresponds to Java's `org.apache.kafka.common.InvalidRecordException`.
#[derive(Clone, Debug)]
pub struct InvalidRecordError {
    message: String,
}

impl InvalidRecordError {
    /// Create a new `InvalidRecordError` with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InvalidRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvalidRecordError: {}", self.message)
    }
}

impl std::error::Error for InvalidRecordError {}

impl From<InvalidRecordError> for std::io::Error {
    fn from(e: InvalidRecordError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.message)
    }
}

impl From<std::io::Error> for InvalidRecordError {
    fn from(e: std::io::Error) -> Self {
        Self::new(e.to_string())
    }
}
