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

//! Translated from `org.apache.kafka.common.requests.CorrelationIdMismatchException`.

use std::fmt;
use std::io;

use crate::common::Error;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// Raised if the correlation id in a response header does not match the
/// expected value from the request header.
///
/// Corresponds to Java's `CorrelationIdMismatchException`, raised by
/// `AbstractResponse.parseResponse(ByteBuffer, RequestHeader)`
/// (`AbstractResponse.java:105`).
///
/// Java `extends` chain:
///    `CorrelationIdMismatchException` -> `java.lang.IllegalStateException`
///
/// It therefore sits **outside** the `KafkaException` hierarchy, beside it, so
/// every predicate answers `false` — the same shape as
/// [`LocalIllegalStateError`](crate::common::LocalIllegalStateError). In particular
/// [`is_kafka_error`](crate::common::Error::is_kafka_error) is `false`, which is
/// what lets `NetworkClient.parseResponse` distinguish it from the
/// [`SchemaError`](crate::common::protocol::types::SchemaError) it converts
/// *some* of its occurrences into.
///
/// Hand-written rather than declared with `kafka_error_class!` because it
/// carries Java's two `int` fields and their accessors — `responseCorrelationId()`
/// is read by `NetworkClient.parseResponse` to decide whether the response is
/// unrelated to a SASL request.
#[derive(Clone, Debug)]
pub struct CorrelationIdMismatchError {
    message: String,
    request_correlation_id: i32,
    response_correlation_id: i32,
    /// The underlying cause. Java's constructor takes no `Throwable`, so this is
    /// always `None`; the slot exists because `Throwable.getCause()` is declared
    /// on every exception (see [`ErrorSource`]).
    source: Option<Box<Error>>,
}

impl CorrelationIdMismatchError {
    /// Create the error, mirroring Java's
    /// `CorrelationIdMismatchException(String message, int requestCorrelationId, int responseCorrelationId)`.
    pub fn new(message: impl Into<String>, request_correlation_id: i32, response_correlation_id: i32) -> Self {
        Self {
            message: message.into(),
            request_correlation_id,
            response_correlation_id,
            source: None,
        }
    }

    /// The correlation id the request carried. Java's `requestCorrelationId()`.
    pub fn request_correlation_id(&self) -> i32 {
        self.request_correlation_id
    }

    /// The correlation id the response carried. Java's `responseCorrelationId()`.
    pub fn response_correlation_id(&self) -> i32 {
        self.response_correlation_id
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The underlying cause, if any. Mirrors Java's `getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl fmt::Display for CorrelationIdMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CorrelationIdMismatchError: {}", self.message)
    }
}

impl ErrorMessage for CorrelationIdMismatchError {
    fn message(&self) -> &str {
        &self.message
    }
}

// No protocol code: `IllegalStateException` has no entry in `Errors.java` and no
// coded superclass, so Java's `Errors.forException` walk falls through to
// `UNKNOWN_SERVER_ERROR` — the trait default.
impl ErrorCode for CorrelationIdMismatchError {}

// `IllegalStateException` is a `java.lang` runtime exception sitting BESIDE
// `KafkaException`, not below it, so every predicate is false and the empty impl
// is the statement that this class is outside the hierarchy.
impl ErrorHierarchy for CorrelationIdMismatchError {}

impl ErrorSource for CorrelationIdMismatchError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl std::error::Error for CorrelationIdMismatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// Carries a [`CorrelationIdMismatchError`] across an [`io::Result`] boundary,
/// as the payload of an [`io::Error`].
///
/// `AbstractResponse.parseResponse` *throws* the exception
/// (`AbstractResponse.java:105`) and `NetworkClient.parseResponse` catches it by
/// **type** (`NetworkClient.java:829`). This crate's response readers report
/// through [`io::Error`], whose [`io::ErrorKind`] cannot express "the
/// correlation ids disagreed" — so the typed value travels inside the payload
/// and the caller recovers it with [`correlation_id_mismatch`], mirroring
/// Java's `catch`. Same mechanism, and same reason, as
/// [`auth_io_error`](crate::common::network::auth_io_error).
///
/// The kind is [`io::ErrorKind::Other`]: classification downstream is driven by
/// the payload, never by the kind.
pub fn correlation_id_mismatch_io_error(error: CorrelationIdMismatchError) -> io::Error {
    io::Error::other(error)
}

/// Recovers the [`CorrelationIdMismatchError`] `e` carries, if any — the Rust
/// equivalent of Java's `catch (CorrelationIdMismatchException e)` in
/// `NetworkClient.parseResponse` (`NetworkClient.java:829`).
///
/// Returns the whole payload rather than its message because the `catch` clause
/// reads [`response_correlation_id`](CorrelationIdMismatchError::response_correlation_id)
/// to decide whether the response is unrelated to a SASL request.
pub fn correlation_id_mismatch(e: &io::Error) -> Option<&CorrelationIdMismatchError> {
    e.get_ref().and_then(|inner| inner.downcast_ref::<CorrelationIdMismatchError>())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The typed value survives the `io::Result` hop, and both `int` accessors
    /// come back intact — `response_correlation_id` is what the SASL
    /// discrimination in `NetworkClient.parseResponse` reads.
    #[test]
    fn round_trips_through_an_io_error() {
        let io_error = correlation_id_mismatch_io_error(CorrelationIdMismatchError::new("ids disagree", 7, 9));
        let recovered = correlation_id_mismatch(&io_error).expect("payload must be recoverable");
        assert_eq!(recovered.message(), "ids disagree");
        assert_eq!(recovered.request_correlation_id(), 7);
        assert_eq!(recovered.response_correlation_id(), 9);
        // `Display` is Java's `toString()` form, so the class name is the prefix
        // and the bare message must not be read out of it.
        assert_eq!(io_error.to_string(), "CorrelationIdMismatchError: ids disagree");
        assert_eq!(io_error.kind(), io::ErrorKind::Other);
    }

    /// An unrelated read failure must not be mistaken for a correlation-id
    /// mismatch — including one whose kind is also `Other`.
    #[test]
    fn unrelated_io_errors_are_not_mismatches() {
        assert!(correlation_id_mismatch(&io::Error::other("boom")).is_none());
        assert!(
            correlation_id_mismatch(&io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Error reading byte array of 4 byte(s): only 1 byte(s) available"
            ))
            .is_none()
        );
    }

    /// `CorrelationIdMismatchException extends IllegalStateException`, so it is
    /// outside the `KafkaException` hierarchy: every predicate answers `false`,
    /// exactly as for [`crate::common::LocalIllegalStateError`].
    #[test]
    fn is_outside_the_kafka_error_hierarchy() {
        let error = Error::correlation_id_mismatch("ids disagree", 7, 9);
        assert!(!error.is_kafka_error());
        assert!(!error.is_api_error());
        assert!(!error.is_retriable_error());
        assert!(!error.is_authentication_error());
        assert!(!error.is_authorization_error());
        assert!(!crate::common::requests::request_utils::is_fatal_error(&error));
        assert_eq!(error.message(), "ids disagree");
        assert_eq!(error.to_string(), "CorrelationIdMismatchError: ids disagree");
        // No entry in `Errors.java` and no coded superclass, so Java's
        // `Errors.forException` walk falls through to UNKNOWN_SERVER_ERROR.
        assert_eq!(error.error(), crate::common::protocol::Errors::UnknownServerError);
    }
}
