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

//! Java's `KafkaException` base class.
//!
//! [`KafkaError`] carries exactly the state `KafkaException` does — a protocol
//! error code and an optional message — and every specific error struct embeds
//! one, mirroring Java's subclasses inheriting it.
//!
//! It is NOT this crate's base error type. That is [`Error`], the flat enum in
//! [`super::error`], which `KafkaError` is only one payload of: its
//! [`KafkaError`](Error::KafkaError) variant stands for a *bare*
//! `KafkaException`, one with no subclass-specific fields. `Error::Timeout` and
//! the rest are Kafka errors too — see [`Error::is_kafka_error`] for that test.
//!
//! The file keeps its `kafka_error` name because it translates
//! `KafkaException.java`, as does the C FFI type `kafka_common_Error_t`
//! (CLAUDE.md §3).

use std::fmt;

use ambassador::Delegate;

use super::error::{ErrorCode, ErrorHierarchy, ErrorMessage, ErrorName, ErrorSource};
// Ambassador exports its generated helper macros beside the trait; a
// `#[delegate]` outside the trait's own module has to import them.
use super::error::ambassador_impl_ErrorMessage;
use super::{Error, Errors};

/// Base Kafka error with common fields shared by all error types.
///
/// Corresponds to Java's `KafkaException` / `ApiException` base class.
/// Contains the protocol error code and an optional custom message —
/// exactly the state Java's `KafkaException` carries. Fatality is NOT state
/// here: like Java, it is derived from the error's identity, see
/// `request_utils::RequestUtils::is_fatal_error`.
///
/// Specific error types (e.g., [`TopicAuthorizationError`](crate::common::errors::TopicAuthorizationError)) embed this
/// struct and add their own fields, mirroring Java's error subclasses.
///
/// Do not confuse this with [`Error`], the flat enum that wraps it — or with
/// that enum's [`KafkaError`](Error::KafkaError) variant, which holds exactly
/// one of these and nothing else.
///
/// # Examples
///
/// ```
/// use confluent_kafka::common::KafkaError;
/// use confluent_kafka::common::Errors;
///
/// let err = KafkaError::new(Errors::RequestTimedOut);
/// assert_eq!(err.code(), 7);
/// assert_eq!(err.message(), Errors::RequestTimedOut.message());
/// ```
///
/// Classification lives on [`Error`], which wraps this type — Java's
/// `KafkaException` has no `isRetriable()` either:
///
/// ```
/// use confluent_kafka::common::Error;
/// use confluent_kafka::common::Errors;
///
/// assert!(Error::new(Errors::RequestTimedOut).is_retriable_error());
/// ```
#[derive(Clone, Debug, Delegate)]
// `target = "self"`: the trait impl is generated from the inherent `message()`
// below. If that method ever disappeared, this would recurse — and rustc's
// `unconditional_recursion` lint turns that into a compile error under
// `#![deny(warnings)]`.
#[delegate(ErrorMessage, target = "self")]
pub struct KafkaError {
    /// The protocol error code.
    error: Errors,
    /// Custom error message. If `None`, [`Errors::message()`] is used.
    custom_message: Option<String>,
    /// The underlying cause — Java's `KafkaException(String, Throwable)`.
    source: Option<Box<Error>>,
}

impl KafkaError {
    // The four constructors below map one-for-one onto Java's four
    // `KafkaException` constructors — `()` (`KafkaException.java:38`),
    // `(String message)` (`:30`), `(Throwable cause)` (`:34`) and
    // `(String message, Throwable cause)` (`:26`) — with a leading `error: Errors`
    // that has no Java counterpart: it carries what Java's subclass identity
    // carried, since this struct stands in for the whole base class (CLAUDE.md
    // §10.3). `error` is therefore in every signature, so the parameter-name
    // intersection is `{error}` and `new(error)` — Java's no-arg form — keeps the
    // plain name under CLAUDE.md §2; the rest are suffixed with the Rust
    // parameters beyond it, in declaration order.

    /// Create a `KafkaError` from an error code with the default message.
    /// Mirrors Java's no-arg `KafkaException()`.
    pub fn new(error: Errors) -> Self {
        Self { error, custom_message: None, source: None }
    }

    /// Create a `KafkaError` from an error code with a custom message.
    /// Mirrors Java's `KafkaException(String message)`.
    pub fn new_message(error: Errors, message: impl Into<String>) -> Self {
        Self { error, custom_message: Some(message.into()), source: None }
    }

    /// Create a `KafkaError` from an error code, a custom message, and the error
    /// that caused it. Mirrors Java's `KafkaException(String message, Throwable cause)`.
    pub fn new_message_source(error: Errors, message: impl Into<String>, source: Error) -> Self {
        Self { error, custom_message: Some(message.into()), source: Some(Box::new(source)) }
    }

    /// Create a `KafkaError` from an error code and the error that caused it,
    /// keeping the code's default message. Mirrors `KafkaException(Throwable cause)`.
    pub fn new_source(error: Errors, source: Error) -> Self {
        Self { error, custom_message: None, source: Some(Box::new(source)) }
    }

    /// The underlying cause, if any. Mirrors Java's `getCause()`.
    pub fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }

    /// The protocol error code.
    pub fn error(&self) -> Errors {
        self.error
    }

    /// The numeric error code (i16).
    pub fn code(&self) -> i16 {
        self.error.code()
    }

    /// The error message. Returns the custom message if set, otherwise the
    /// default message from the error code.
    pub fn message(&self) -> &str {
        match &self.custom_message {
            Some(msg) => msg,
            None => self.error.message(),
        }
    }
}

impl ErrorSource for KafkaError {
    fn source(&self) -> Option<&Error> {
        self.source.as_deref()
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for KafkaError {
    /// Wired to the stored source. This impl used to be empty, so `source()`
    /// answered `None` even when a cause was present — Java's
    /// `KafkaException(String, Throwable)` keeps it.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl ErrorCode for KafkaError {
    fn error(&self) -> Errors {
        self.error
    }
}

// Hand-written rather than emitted by `error_name_impl!` in `super::error`: that
// macro covers the payloads declared in their own files, and this one is
// declared right here.
impl ErrorName for KafkaError {
    fn name(&self) -> &'static str {
        "KafkaError"
    }
}

/// A *bare* `KafkaException`: `Error::KafkaError` is now only reached for
/// [`Errors::None`], since every other code maps to its own class through
/// [`Errors::error`]. So it answers for `KafkaException` itself and nothing
/// else — in particular NOT [`is_api_error`](ErrorHierarchy::is_api_error),
/// because `KafkaException` is the parent of `ApiException`, not an instance of
/// it. The remaining predicates take the trait's `false`.
impl ErrorHierarchy for KafkaError {
    fn is_kafka_error(&self) -> bool {
        true
    }
}
