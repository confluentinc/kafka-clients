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

//! The error a callback implemented in another language reports when it
//! raised one of that language's own errors.
//!
//! No Java counterpart (DoD #7). A Java listener's `Throwable` reaches
//! `ConsumerUtils.maybeWrapAsKafkaException` as itself; an error raised by a
//! Python (or any foreign) callback cannot cross the FFI boundary as a Rust
//! value. This class stands in for it: it carries the foreign error's text,
//! for the client's own logs, and an opaque pointer the binding uses to find
//! the original again when the error comes back out of the client.

use std::ffi::c_void;
use std::fmt;

/// The opaque pointer, never dereferenced, freed or retained by Rust: the
/// binding that created the error keeps whatever it points at alive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Opaque(*mut c_void);

// SAFETY: the pointer is an identifier only — Rust never reads through it, so
// copying it across tasks shares no memory.
unsafe impl Send for Opaque {}
// SAFETY: as for `Send`.
unsafe impl Sync for Opaque {}

/// An error raised by a callback written in another language.
///
/// Like Java's `java.lang` runtime exceptions it sits beside `KafkaException`,
/// not below it, so [`Error::is_kafka_error`](crate::common::Error::is_kafka_error)
/// is `false` and the client wraps it exactly as Java wraps a listener's
/// foreign `Throwable`: the rebalance-listener path builds
/// `KafkaException("User rebalance callback throws an error")` with this error
/// as its [`source`](crate::common::Error::source), Java's `getCause()`.
///
/// The [`opaque`](Self::opaque) pointer lets the binding swap the original
/// error back in: on receiving an error whose cause chain holds a
/// `LocalCallbackError`, it compares the pointer with the error it stored
/// before reporting. Rust never dereferences, frees or retains the pointer;
/// the binding keeps the object alive for as long as it may be compared.
// a binding's foreign callback error, no Java class (DoD #7)
#[doc(alias = "rust-only")]
#[derive(Clone, Debug)]
pub struct LocalCallbackError {
    message: String,
    opaque: Opaque,
}

impl LocalCallbackError {
    /// Create the error with the foreign error's text and the binding's
    /// opaque pointer to it.
    pub fn new(message: impl Into<String>, opaque: *mut c_void) -> Self {
        Self { message: message.into(), opaque: Opaque(opaque) }
    }

    /// The foreign error's text, as the binding reported it.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The binding's opaque pointer to the foreign error, exactly as given.
    pub fn opaque(&self) -> *mut c_void {
        self.opaque.0
    }
}

// No cause: the foreign error's own cause chain stays in its language.
impl crate::common::error::ErrorSource for LocalCallbackError {}

impl std::error::Error for LocalCallbackError {}

// No protocol code: raised only on the client side, never by a broker, so the
// trait default (`Errors::UnknownServerError`) is the right answer.
impl crate::common::error::ErrorCode for LocalCallbackError {}

impl crate::common::error::ErrorMessage for LocalCallbackError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for LocalCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalCallbackError: {}", self.message)
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::common::Error;

    #[test]
    fn carries_message_and_opaque_unchanged() {
        let mut target = 7u8;
        let opaque = (&mut target as *mut u8).cast::<c_void>();
        let err = LocalCallbackError::new("boom", opaque);
        assert_eq!(err.message(), "boom");
        assert_eq!(err.opaque(), opaque);
        assert_eq!(err.clone().opaque(), opaque);
        assert_eq!(err.to_string(), "LocalCallbackError: boom");
        assert!(LocalCallbackError::new("null", ptr::null_mut()).opaque().is_null());
    }

    /// Outside the Kafka hierarchy, so the listener path wraps it with Java's
    /// message and keeps it as the cause.
    #[test]
    fn listener_wrap_keeps_it_as_the_source() {
        let mut target = 0u8;
        let opaque = (&mut target as *mut u8).cast::<c_void>();
        let err = Error::local_callback("ValueError: bad", opaque);
        assert!(err.is_local_callback_error());
        assert!(!err.is_kafka_error());
        assert!(!err.is_api_error());
        assert!(!err.is_retriable_error());
        assert_eq!(err.message(), "ValueError: bad");

        let wrapped = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
            err,
            "User rebalance callback throws an error",
        );
        assert!(wrapped.is_kafka_error());
        assert!(!wrapped.is_local_callback_error());
        assert_eq!(wrapped.message(), "User rebalance callback throws an error");
        let source = wrapped.source().expect("the foreign error is the cause");
        assert!(source.is_local_callback_error());
        match source {
            Error::LocalCallback(e) => assert_eq!(e.opaque(), opaque),
            other => panic!("unexpected cause {other:?}"),
        }
    }
}
