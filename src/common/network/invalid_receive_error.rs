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

//! Translation of
//! `org.apache.kafka.common.network.InvalidReceiveException`.

use std::fmt;

/// Raised when a [`crate::common::network::NetworkReceive`] reads a length
/// prefix that is negative or larger than the configured maximum.
///
/// In Java this extends `KafkaException`. In Rust we follow the
/// CLAUDE.md `Exception → Error` rule. This error is constructible
/// independently of [`crate::common::errors::KafkaError`] but converts
/// into [`crate::common::errors::KafkaError::Generic`] for callers that
/// want a unified `Result<T, KafkaError>` shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InvalidReceiveError {
    message: String,
}

impl InvalidReceiveError {
    /// Mirrors the single-argument Java constructor.
    pub fn new(message: impl Into<String>) -> Self {
        InvalidReceiveError { message: message.into() }
    }

    /// Mirrors `Throwable.getMessage()`.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InvalidReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InvalidReceiveError {}

impl From<InvalidReceiveError> for crate::common::errors::KafkaError {
    fn from(err: InvalidReceiveError) -> Self {
        crate::common::errors::KafkaError::Generic(err.message)
    }
}

impl From<InvalidReceiveError> for std::io::Error {
    fn from(err: InvalidReceiveError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trip() {
        let err = InvalidReceiveError::new("Invalid receive (size = -1)");
        assert_eq!(err.message(), "Invalid receive (size = -1)");
        assert_eq!(err.to_string(), "Invalid receive (size = -1)");
    }
}
