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
//! `org.apache.kafka.common.requests.CorrelationIdMismatchException`.

use std::fmt;

/// Raised if the correlationId in a response header does not match the
/// expected value from the request header.
///
/// In Java this is a `RuntimeException` (`extends IllegalStateException`).
/// In Rust we follow the CLAUDE.md `Exception → Error` rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelationIdMismatchError {
    message: String,
    request_correlation_id: i32,
    response_correlation_id: i32,
}

impl CorrelationIdMismatchError {
    /// Mirrors the Java constructor.
    pub fn new(message: impl Into<String>, request_correlation_id: i32, response_correlation_id: i32) -> Self {
        CorrelationIdMismatchError { message: message.into(), request_correlation_id, response_correlation_id }
    }

    /// Mirrors `requestCorrelationId()`.
    pub fn request_correlation_id(&self) -> i32 {
        self.request_correlation_id
    }

    /// Mirrors `responseCorrelationId()`.
    pub fn response_correlation_id(&self) -> i32 {
        self.response_correlation_id
    }

    /// Mirrors `Throwable#getMessage`.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for CorrelationIdMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CorrelationIdMismatchError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_id_mismatch_error_accessors() {
        let err = CorrelationIdMismatchError::new("mismatch", 1, 2);
        assert_eq!(err.request_correlation_id(), 1);
        assert_eq!(err.response_correlation_id(), 2);
        assert_eq!(err.message(), "mismatch");
        assert_eq!(err.to_string(), "mismatch");
    }
}
