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

//! Translated from `org.apache.kafka.common.network.InvalidReceiveException`.

use std::fmt;
use std::io;

use crate::common::Error;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// The size header of a network receive is negative or exceeds the maximum
/// allowed size.
///
/// Corresponds to Java's `InvalidReceiveException`. It has no entry in `Errors`,
/// so it carries no protocol code.
///
/// Java `extends` chain:
///    `InvalidReceiveException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_class!` because the
/// network layer surfaces it across an [`io::Error`] boundary, so it needs the
/// `From<_> for io::Error` conversion the macro does not provide. It still implements
/// [`ErrorHierarchy`] / [`ErrorMessage`] / [`ErrorCode`] and has an
/// [`Error`](crate::common::Error) variant, so it answers `is_kafka_error()`
/// like every other translated `KafkaException` descendant.
#[derive(Clone, Debug)]
pub struct InvalidReceiveError {
    message: String,
}

impl InvalidReceiveError {
    /// Creates a new `InvalidReceiveError` with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InvalidReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvalidReceiveError: {}", self.message)
    }
}

impl std::error::Error for InvalidReceiveError {}

impl ErrorMessage for InvalidReceiveError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for InvalidReceiveError {}

impl ErrorHierarchy for InvalidReceiveError {
    fn is_kafka_error(&self) -> bool {
        true
    }
}

impl From<InvalidReceiveError> for io::Error {
    fn from(e: InvalidReceiveError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}

impl ErrorSource for InvalidReceiveError {
    // `InvalidReceiveException` exposes no `Throwable cause` constructor, so its cause is
    // always null in Java; the trait default (`None`) is that answer.
}

impl InvalidReceiveError {
    /// Always `None`: `InvalidReceiveException` exposes no `Throwable cause`
    /// constructor. Inherent so `x.source()` stays unambiguous against the
    /// existing `std::error::Error` impl (kept for the `io::Error` boundary).
    pub fn source(&self) -> Option<&Error> {
        None
    }
}
