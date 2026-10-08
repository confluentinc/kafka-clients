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
use crate::common::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorSource};

/// The size header of a network receive is negative or exceeds the maximum
/// allowed size.
///
/// Corresponds to Java's `InvalidReceiveException`. It has no entry in `Errors`,
/// so it carries no protocol code.
///
/// Java `extends` chain:
///    `InvalidReceiveException` -> `KafkaException`
///
/// Hand-written rather than declared with `kafka_error_type!` because the
/// network layer surfaces it across an [`io::Error`] boundary, so it needs the
/// `From<_> for io::Error` conversion the macro does not provide. It still implements
/// [`ErrorHierarchy`] / [`ErrorMessage`] / [`ErrorCode`] and has an
/// [`Error`](crate::common::Error) variant, so it answers `is_kafka_error()`
/// like every other translated `KafkaException` descendant.
///
/// Crate-private although `Error::InvalidReceive` is public: Java's class sits in
/// `common.network`, which is "not a supported API". Callers match the variant and
/// use `Display` / `source()`; the payload itself is not reachable by name.
#[expect(unnameable_types)]
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.network.InvalidReceiveException")]
pub struct InvalidReceiveError {
    message: String,
}

impl InvalidReceiveError {
    /// Creates a new `InvalidReceiveError` with the given message.
    #[doc(alias = "org.apache.kafka.common.network.InvalidReceiveException#InvalidReceiveException")]
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

/// Returns `true` if `e` carries an [`InvalidReceiveError`] payload, as the
/// `From<InvalidReceiveError>` conversion above builds it.
///
/// The Rust equivalent of `e instanceof InvalidReceiveException` on the error
/// closing a channel. Java's `Selector.pollSelectionKeys` logs an `IOException`
/// disconnect at DEBUG and every other error at WARN (`Selector.java:600-626`),
/// and `InvalidReceiveException` is a `KafkaException`, so it is one of the
/// others. Like [`is_authentication_error`](super::is_authentication_error) it
/// classifies by the typed payload, never by the [`io::ErrorKind`].
pub fn is_invalid_receive_error(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<InvalidReceiveError>())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::auth_io_error;

    #[test]
    fn test_converted_error_is_recognized() {
        let e = io::Error::from(InvalidReceiveError::new("Invalid receive (size = -1)"));
        assert!(is_invalid_receive_error(&e));
        // `Display` delegates to the payload, Java's `toString()` form.
        assert_eq!("InvalidReceiveError: Invalid receive (size = -1)", e.to_string());
    }

    #[test]
    fn test_other_io_errors_are_not_invalid_receives() {
        // An I/O disconnect, an authentication failure and an unrelated
        // `InvalidData` error: the classification is by payload, not by kind.
        let reset = io::Error::new(io::ErrorKind::ConnectionReset, "Connection reset by peer (os error 104)");
        assert!(!is_invalid_receive_error(&reset));
        assert!(!is_invalid_receive_error(&auth_io_error("bad credentials")));
        let other = io::Error::new(io::ErrorKind::InvalidData, "EOF during payload read");
        assert!(!is_invalid_receive_error(&other));
    }
}
