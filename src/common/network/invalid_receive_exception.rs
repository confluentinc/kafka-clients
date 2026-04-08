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

//! Invalid receive exception for Kafka network protocol.
//!
//! Translated from `org.apache.kafka.common.network.InvalidReceiveException`.

use std::fmt;
use std::io;

/// Error returned when an invalid receive is detected.
///
/// This occurs when the size header in a network receive is negative
/// or exceeds the maximum allowed size.
#[derive(Debug)]
pub struct InvalidReceiveException {
    message: String,
}

impl InvalidReceiveException {
    /// Creates a new `InvalidReceiveException` with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InvalidReceiveException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for InvalidReceiveException {}

impl From<InvalidReceiveException> for io::Error {
    fn from(e: InvalidReceiveException) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}
