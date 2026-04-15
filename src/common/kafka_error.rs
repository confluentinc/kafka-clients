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

//! Kafka error hierarchy.
//!
//! Mirrors Java's `KafkaException` / `ApiException` class hierarchy using
//! Rust structs and composition. Each specific error struct contains a
//! [`KafkaGenericError`] base with common fields (error code, message, fatal flag)
//! plus its own subclass-specific fields.
//!
//! [`KafkaError`] is the unified enum used for polymorphic error handling
//! in return types and storage, replacing the separate `MetadataError` and
//! `UnsupportedApiError` types.

use std::collections::HashSet;
use std::fmt;

use super::protocol::Errors;

// ---------------------------------------------------------------------------
// Base struct — corresponds to Java's KafkaException / ApiException
// ---------------------------------------------------------------------------

/// Base Kafka error with common fields shared by all error types.
///
/// Corresponds to Java's `KafkaException` / `ApiException` base class.
/// Contains the protocol error code, an optional custom message, and a
/// fatal flag.
///
/// Specific error types (e.g., [`TopicAuthorizationError`]) embed this
/// struct and add their own fields, mirroring Java's error subclasses.
///
/// # Examples
///
/// ```
/// use confluent_kafka_rust::common::kafka_error::KafkaGenericError;
/// use confluent_kafka_rust::common::protocol::Errors;
///
/// let err = KafkaGenericError::new(Errors::RequestTimedOut);
/// assert!(err.is_retriable());
/// assert!(!err.is_fatal());
/// assert_eq!(err.code(), 7);
/// ```
#[derive(Clone, Debug)]
pub struct KafkaGenericError {
    /// The protocol error code.
    error: Errors,
    /// Custom error message. If `None`, [`Errors::message()`] is used.
    custom_message: Option<String>,
    /// Whether this error is fatal (unrecoverable at the client level).
    fatal: bool,
}

impl KafkaGenericError {
    /// Create a `KafkaGenericError` from an error code with the default message.
    pub fn new(error: Errors) -> Self {
        Self { error, custom_message: None, fatal: false }
    }

    /// Create a `KafkaGenericError` from an error code with a custom message.
    pub fn with_message(error: Errors, message: impl Into<String>) -> Self {
        Self { error, custom_message: Some(message.into()), fatal: false }
    }

    /// Create a fatal `KafkaGenericError`.
    ///
    /// Fatal errors indicate that the client cannot recover and must be
    /// propagated to the application.
    pub fn fatal(error: Errors, message: impl Into<String>) -> Self {
        Self { error, custom_message: Some(message.into()), fatal: true }
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

    /// Whether this error is retriable.
    ///
    /// Delegates to [`Errors::is_retriable()`].
    pub fn is_retriable(&self) -> bool {
        self.error.is_retriable()
    }

    /// Whether this error is fatal (unrecoverable at the client level).
    pub fn is_fatal(&self) -> bool {
        self.fatal
    }

    /// Whether this error requires the transaction to be aborted.
    pub fn txn_requires_abort(&self) -> bool {
        self.error == Errors::TransactionAbortable
    }
}

impl fmt::Display for KafkaGenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for KafkaGenericError {}

// ---------------------------------------------------------------------------
// Specific error structs — correspond to Java error subclasses
// ---------------------------------------------------------------------------

/// Topic authorization failure with the set of unauthorized topics.
///
/// Corresponds to Java's `TopicAuthorizationException`.
#[derive(Clone, Debug)]
pub struct TopicAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaGenericError,
    /// The set of unauthorized topics.
    pub unauthorized_topics: HashSet<String>,
}

impl TopicAuthorizationError {
    /// Create a new topic authorization error.
    pub fn new(unauthorized_topics: HashSet<String>) -> Self {
        Self {
            kafka_error: KafkaGenericError::new(Errors::TopicAuthorizationFailed),
            unauthorized_topics,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaGenericError {
        &self.kafka_error
    }
}

/// Invalid topic error with the set of invalid topics.
///
/// Corresponds to Java's `InvalidTopicException`.
#[derive(Clone, Debug)]
pub struct InvalidTopicError {
    /// Base error fields.
    kafka_error: KafkaGenericError,
    /// The set of invalid topics.
    pub invalid_topics: HashSet<String>,
}

impl InvalidTopicError {
    /// Create a new invalid topic error.
    pub fn new(invalid_topics: HashSet<String>) -> Self {
        Self {
            kafka_error: KafkaGenericError::new(Errors::InvalidTopicException),
            invalid_topics,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaGenericError {
        &self.kafka_error
    }
}

/// Group authorization failure with the group ID.
///
/// Corresponds to Java's `GroupAuthorizationException`.
#[derive(Clone, Debug)]
pub struct GroupAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaGenericError,
    /// The group ID that failed authorization.
    pub group_id: String,
}

impl GroupAuthorizationError {
    /// Create a new group authorization error.
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaGenericError::new(Errors::GroupAuthorizationFailed),
            group_id: group_id.into(),
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaGenericError {
        &self.kafka_error
    }
}

// ---------------------------------------------------------------------------
// KafkaError — unified enum for polymorphic error handling
// ---------------------------------------------------------------------------

/// Unified Kafka error type for polymorphic error handling.
///
/// This enum wraps the error struct hierarchy so that any Kafka error can
/// be stored and returned through a single type. Each variant holds a
/// specific error struct that contains [`KafkaGenericError`] as its base.
///
/// Common methods (`error()`, `code()`, `message()`, `is_retriable()`,
/// `is_fatal()`, `txn_requires_abort()`) are delegated to the inner
/// [`KafkaGenericError`] base.
///
/// This type is used in `Result` return types and `Option` storage where
/// any kind of Kafka error may occur.
#[derive(Clone, Debug)]
pub enum KafkaError {
    /// Generic Kafka error with no additional context.
    Generic(KafkaGenericError),
    /// Topic authorization failure with unauthorized topic set.
    TopicAuthorization(TopicAuthorizationError),
    /// Invalid topic error with invalid topic set.
    InvalidTopic(InvalidTopicError),
    /// Group authorization failure with group ID.
    GroupAuthorization(GroupAuthorizationError),
    /// Buffer exhausted error — the producer cannot allocate memory for a record
    /// because the buffer pool is full and the max blocking time has elapsed.
    ///
    /// Corresponds to Java's `BufferExhaustedException`.
    BufferExhausted(KafkaGenericError),
    /// Illegal argument error — an invalid argument was provided to a method.
    ///
    /// Corresponds to Java's `IllegalArgumentException`.
    IllegalArgument(String),
    /// Illegal state error — a method was called in an invalid state.
    ///
    /// Corresponds to Java's `IllegalStateException`.
    IllegalState(String),
    /// Timeout error — an operation did not complete within the specified time.
    ///
    /// Corresponds to Java's `TimeoutException`.
    Timeout(String),
}

impl KafkaError {
    // -- Convenience constructors ------------------------------------------

    /// Create a generic error from an error code.
    pub fn new(error: Errors) -> Self {
        Self::Generic(KafkaGenericError::new(error))
    }

    /// Create a generic error with a custom message.
    pub fn with_message(error: Errors, message: impl Into<String>) -> Self {
        Self::Generic(KafkaGenericError::with_message(error, message))
    }

    /// Create a fatal error.
    pub fn fatal(error: Errors, message: impl Into<String>) -> Self {
        Self::Generic(KafkaGenericError::fatal(error, message))
    }

    /// Create a topic authorization error.
    pub fn topic_authorization(topics: HashSet<String>) -> Self {
        Self::TopicAuthorization(TopicAuthorizationError::new(topics))
    }

    /// Create an invalid topic error.
    pub fn invalid_topics(topics: HashSet<String>) -> Self {
        Self::InvalidTopic(InvalidTopicError::new(topics))
    }

    /// Create a group authorization error.
    pub fn group_authorization(group_id: impl Into<String>) -> Self {
        Self::GroupAuthorization(GroupAuthorizationError::new(group_id))
    }

    /// Create a buffer exhausted error.
    ///
    /// Corresponds to Java's `BufferExhaustedException`.
    pub fn buffer_exhausted(message: impl Into<String>) -> Self {
        Self::BufferExhausted(KafkaGenericError::with_message(Errors::UnknownServerError, message))
    }

    /// Create an illegal argument error.
    ///
    /// Corresponds to Java's `IllegalArgumentException`.
    pub fn illegal_argument(message: impl Into<String>) -> Self {
        Self::IllegalArgument(message.into())
    }

    /// Create an illegal state error.
    ///
    /// Corresponds to Java's `IllegalStateException`.
    pub fn illegal_state(message: impl Into<String>) -> Self {
        Self::IllegalState(message.into())
    }

    /// Create a timeout error.
    ///
    /// Corresponds to Java's `TimeoutException`.
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout(message.into())
    }

    /// Create an unsupported version error.
    pub fn unsupported_version(message: impl Into<String>) -> Self {
        Self::Generic(KafkaGenericError::with_message(Errors::UnsupportedVersion, message))
    }

    // -- Base access -------------------------------------------------------

    /// Access the base [`KafkaGenericError`] common to all variants.
    ///
    /// Returns `None` for variants that do not carry a [`KafkaGenericError`]
    /// (e.g. [`IllegalArgument`](Self::IllegalArgument), [`IllegalState`](Self::IllegalState)).
    pub fn kafka_error(&self) -> Option<&KafkaGenericError> {
        match self {
            Self::Generic(e) | Self::BufferExhausted(e) => Some(e),
            Self::TopicAuthorization(e) => Some(&e.kafka_error),
            Self::InvalidTopic(e) => Some(&e.kafka_error),
            Self::GroupAuthorization(e) => Some(&e.kafka_error),
            Self::IllegalArgument(_) | Self::IllegalState(_) | Self::Timeout(_) => None,
        }
    }

    // -- Delegating methods ------------------------------------------------

    /// The protocol error code.
    ///
    /// Returns [`Errors::UnknownServerError`] for variants without a
    /// [`KafkaGenericError`].
    pub fn error(&self) -> Errors {
        match self.kafka_error() {
            Some(e) => e.error(),
            None => Errors::UnknownServerError,
        }
    }

    /// The numeric error code (i16).
    pub fn code(&self) -> i16 {
        self.error().code()
    }

    /// The error message.
    pub fn message(&self) -> &str {
        match self {
            Self::IllegalArgument(msg) | Self::IllegalState(msg) | Self::Timeout(msg) => msg,
            _ => self.kafka_error().map_or("Unknown error", |e| e.message()),
        }
    }

    /// Whether this error is retriable.
    ///
    /// [`IllegalArgument`](Self::IllegalArgument) and
    /// [`IllegalState`](Self::IllegalState) are never retriable.
    pub fn is_retriable(&self) -> bool {
        self.kafka_error().is_some_and(|e| e.is_retriable())
    }

    /// Whether this error is fatal.
    ///
    /// [`IllegalArgument`](Self::IllegalArgument) and
    /// [`IllegalState`](Self::IllegalState) are not marked fatal.
    pub fn is_fatal(&self) -> bool {
        self.kafka_error().is_some_and(|e| e.is_fatal())
    }

    /// Whether this error requires the transaction to be aborted.
    pub fn txn_requires_abort(&self) -> bool {
        self.kafka_error().is_some_and(|e| e.txn_requires_abort())
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Generic(e) | Self::BufferExhausted(e) => write!(f, "{e}"),
            Self::TopicAuthorization(e) => {
                write!(f, "{}: {:?}", e.kafka_error, e.unauthorized_topics)
            },
            Self::InvalidTopic(e) => {
                write!(f, "{}: {:?}", e.kafka_error, e.invalid_topics)
            },
            Self::GroupAuthorization(e) => {
                write!(f, "{}: {}", e.kafka_error, e.group_id)
            },
            Self::IllegalArgument(msg) => write!(f, "IllegalArgumentError: {msg}"),
            Self::IllegalState(msg) => write!(f, "IllegalStateError: {msg}"),
            Self::Timeout(msg) => write!(f, "TimeoutError: {msg}"),
        }
    }
}

impl std::error::Error for KafkaError {}
