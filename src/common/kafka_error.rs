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

use super::Errors;

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
/// use confluent_kafka::common::KafkaGenericError;
/// use confluent_kafka::common::protocol::Errors;
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

    /// Create a group authorization error carrying a custom message.
    ///
    /// Mirrors Java's `GroupAuthorizationException(String message)` /
    /// `forGroupId(...)`, where the exception message is caller-supplied
    /// rather than the default error text. Used when the coordinator manager
    /// surfaces a fatal `GroupAuthorizationException("...")`.
    pub fn with_message(group_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaGenericError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id: group_id.into(),
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaGenericError {
        &self.kafka_error
    }
}

/// Throttling quota exceeded error carrying the throttle time.
///
/// Corresponds to Java's `ThrottlingQuotaExceededException` (a
/// `RetriableException` subclass carrying error code
/// [`Errors::ThrottlingQuotaExceeded`] plus a `throttleTimeMs`).
#[derive(Clone, Debug)]
pub struct ThrottlingQuotaExceededError {
    /// Base error fields.
    kafka_error: KafkaGenericError,
    /// The amount of time to wait before retrying, in milliseconds.
    pub throttle_time_ms: i32,
}

impl ThrottlingQuotaExceededError {
    /// Create a new throttling quota exceeded error.
    pub fn new(throttle_time_ms: i32, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaGenericError::with_message(Errors::ThrottlingQuotaExceeded, message),
            throttle_time_ms,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaGenericError {
        &self.kafka_error
    }

    /// The amount of time to wait before retrying, in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms
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
    /// Throttling quota exceeded error carrying the throttle time.
    ///
    /// Corresponds to Java's `ThrottlingQuotaExceededException`.
    ThrottlingQuotaExceeded(ThrottlingQuotaExceededError),
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
    /// Record too large error — the record is larger than the configured maximum.
    ///
    /// Corresponds to Java's `RecordTooLargeException`.
    RecordTooLarge(String),
    /// Serialization error — the key or value could not be serialized.
    ///
    /// Corresponds to Java's `SerializationException`.
    Serialization(String),
    /// Wakeup error — a blocking operation was preempted by `wakeup()`.
    ///
    /// Corresponds to Java's `WakeupException` (extends `KafkaException`,
    /// carries no error code). Used by `Consumer::wakeup()` to break out
    /// of a `poll()` / `commit_sync()` / etc. call.
    Wakeup(String),
    /// Concurrent modification error — the consumer was accessed from more
    /// than one thread.
    ///
    /// Corresponds to Java's `java.util.ConcurrentModificationException`,
    /// thrown by `KafkaConsumer.acquire()` ("KafkaConsumer is not safe for
    /// multi-threaded access"). Like `IllegalState`, it is a plain
    /// `RuntimeException` — neither an `ApiException` nor a `KafkaException`
    /// — so it is never retriable and never fatal.
    ConcurrentModification(String),
    /// Transaction aborted error — undrained batches are being failed because
    /// the transaction was aborted.
    ///
    /// Corresponds to Java's `TransactionAbortedException`, which extends
    /// `ApiException` but carries **no wire error code** (it is absent from
    /// `Errors.java`), so it needs its own variant rather than a
    /// [`Generic`](Self::Generic) wrapping an [`Errors`]. Java throws it from
    /// exactly one place: `Sender.abortBatches`, as
    /// `accumulator.abortUndrainedBatches(new TransactionAbortedException())`.
    ///
    /// Unlike [`Wakeup`](Self::Wakeup) this IS an `ApiException`, so
    /// [`is_api_exception`](Self::is_api_exception) returns `true` for it.
    TransactionAborted(String),
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

    /// Create a group authorization error carrying a custom message
    /// (Java: `new GroupAuthorizationException(message)`).
    pub fn group_authorization_with_message(group_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self::GroupAuthorization(GroupAuthorizationError::with_message(group_id, message))
    }

    /// Create an invalid group ID error.
    ///
    /// Corresponds to Java's `InvalidGroupIdException` (an `ApiException`
    /// subclass carrying error code [`Errors::InvalidGroupId`]). Thrown
    /// by group-management / offset-commit APIs when the consumer was
    /// constructed without a valid `group.id`.
    pub fn invalid_group_id(message: impl Into<String>) -> Self {
        Self::Generic(KafkaGenericError::with_message(Errors::InvalidGroupId, message))
    }

    /// Create a throttling quota exceeded error.
    ///
    /// Corresponds to Java's `ThrottlingQuotaExceededException(int, String)`.
    pub fn throttling_quota_exceeded(throttle_time_ms: i32, message: impl Into<String>) -> Self {
        Self::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(throttle_time_ms, message))
    }

    /// The throttle time carried by a [`ThrottlingQuotaExceeded`](Self::ThrottlingQuotaExceeded)
    /// error, or `None` for any other error.
    ///
    /// Mirrors Java's `ThrottlingQuotaExceededException.throttleTimeMs()`.
    pub fn throttle_time_ms(&self) -> Option<i32> {
        match self {
            Self::ThrottlingQuotaExceeded(e) => Some(e.throttle_time_ms),
            _ => None,
        }
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

    /// Create a record too large error.
    ///
    /// Corresponds to Java's `RecordTooLargeException`.
    pub fn record_too_large(message: impl Into<String>) -> Self {
        Self::RecordTooLarge(message.into())
    }

    /// Create a serialization error.
    ///
    /// Corresponds to Java's `SerializationException`.
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization(message.into())
    }

    /// Create an unsupported version error.
    pub fn unsupported_version(message: impl Into<String>) -> Self {
        Self::Generic(KafkaGenericError::with_message(Errors::UnsupportedVersion, message))
    }

    /// Create a wakeup error.
    ///
    /// Corresponds to Java's `WakeupException`. Returned from blocking
    /// `Consumer` operations (`poll`, `commit_sync`, `position`, etc.)
    /// when `wakeup()` is invoked from another task.
    pub fn wakeup(message: impl Into<String>) -> Self {
        Self::Wakeup(message.into())
    }

    /// Create a concurrent modification error.
    ///
    /// Corresponds to Java's `ConcurrentModificationException` thrown by
    /// `KafkaConsumer.acquire()` when the consumer is accessed from more
    /// than one thread.
    pub fn concurrent_modification(message: impl Into<String>) -> Self {
        Self::ConcurrentModification(message.into())
    }

    /// Create a transaction aborted error with Java's default message.
    ///
    /// Corresponds to Java's no-arg `TransactionAbortedException()`, whose
    /// message is `"Failing batch since transaction was aborted"`.
    pub fn transaction_aborted() -> Self {
        Self::TransactionAborted("Failing batch since transaction was aborted".to_string())
    }

    /// Create a transaction aborted error with a custom message.
    ///
    /// Corresponds to Java's `TransactionAbortedException(String)`.
    pub fn transaction_aborted_with_message(message: impl Into<String>) -> Self {
        Self::TransactionAborted(message.into())
    }

    /// Create a record batch too large error.
    ///
    /// Corresponds to Java's `RecordBatchTooLargeException`.
    pub fn record_batch_too_large(message: impl Into<String>) -> Self {
        Self::Generic(KafkaGenericError::with_message(Errors::MessageTooLarge, message))
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
            Self::ThrottlingQuotaExceeded(e) => Some(&e.kafka_error),
            Self::IllegalArgument(_)
            | Self::IllegalState(_)
            | Self::Timeout(_)
            | Self::RecordTooLarge(_)
            | Self::Serialization(_)
            | Self::Wakeup(_)
            | Self::ConcurrentModification(_)
            | Self::TransactionAborted(_) => None,
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
            Self::IllegalArgument(msg)
            | Self::IllegalState(msg)
            | Self::Timeout(msg)
            | Self::RecordTooLarge(msg)
            | Self::Serialization(msg)
            | Self::Wakeup(msg)
            | Self::ConcurrentModification(msg)
            | Self::TransactionAborted(msg) => msg,
            _ => self.kafka_error().map_or("Unknown error", |e| e.message()),
        }
    }

    /// Whether this error is retriable.
    ///
    /// [`IllegalArgument`](Self::IllegalArgument) and
    /// [`IllegalState`](Self::IllegalState) are never retriable.
    ///
    /// [`Timeout`](Self::Timeout) is retriable: Java's `TimeoutException`
    /// extends `RetriableException` extends `ApiException`, so timeouts are
    /// transient by definition. Special-cased here because `Timeout` has no
    /// embedded `Errors` code and would otherwise fall through to `false`.
    pub fn is_retriable(&self) -> bool {
        matches!(self, Self::Timeout(_)) || self.kafka_error().is_some_and(|e| e.is_retriable())
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

    /// Whether this error corresponds to a Java `ApiException`.
    ///
    /// In Java, `ApiException` is a subclass of `KafkaException` that
    /// represents errors from the Kafka API. In `KafkaProducer.doSend()`,
    /// `ApiException`s are caught and returned via a failed future (with
    /// callback invocation), while other exceptions propagate directly.
    ///
    /// The following error types correspond to Java `ApiException` subclasses:
    /// - `InvalidTopic` (InvalidTopicException extends ApiException)
    /// - `RecordTooLarge` (RecordTooLargeException extends ApiException)
    /// - `Timeout` (TimeoutException extends RetriableException extends ApiException)
    /// - `Generic` (covers all other Errors-based exceptions)
    /// - `TopicAuthorization` (TopicAuthorizationException extends ApiException)
    /// - `GroupAuthorization` (GroupAuthorizationException extends ApiException)
    /// - `BufferExhausted` (BufferExhaustedException extends ApiException)
    /// - `TransactionAborted` (TransactionAbortedException extends ApiException,
    ///   despite carrying no error code)
    ///
    /// NOT `ApiException`:
    /// - `IllegalArgument` (IllegalArgumentException extends RuntimeException)
    /// - `IllegalState` (IllegalStateException extends RuntimeException)
    /// - `Serialization` (SerializationException extends KafkaException, NOT ApiException)
    pub fn is_api_exception(&self) -> bool {
        !matches!(
            self,
            Self::IllegalArgument(_)
                | Self::IllegalState(_)
                | Self::Serialization(_)
                | Self::Wakeup(_)
                | Self::ConcurrentModification(_)
        )
    }

    /// Whether this error corresponds to a Java `KafkaException` (or a
    /// subclass of it).
    ///
    /// This mirrors Java's `t instanceof KafkaException` test, used by
    /// `ConsumerUtils.maybeWrapAsKafkaException(t, message)`
    /// (`ConsumerUtils.java:256`): a `KafkaException` passes through
    /// unchanged, while a non-`KafkaException` `Throwable` gets wrapped in
    /// a new `KafkaException(message, t)`.
    ///
    /// In Java the only error variants modelled here that are NOT
    /// `KafkaException` are the `RuntimeException` subclasses
    /// `IllegalArgumentException` and `IllegalStateException`. Everything
    /// else — `ApiException` subtypes, `SerializationException`,
    /// `WakeupException`, the bare `KafkaException` — extends
    /// `KafkaException`.
    ///
    /// (`WakeupException` IS a `KafkaException`, hence it differs from
    /// [`is_api_exception`](Self::is_api_exception), which excludes it
    /// because `WakeupException` is not an `ApiException`.)
    pub fn is_kafka_exception(&self) -> bool {
        !matches!(
            self,
            Self::IllegalArgument(_) | Self::IllegalState(_) | Self::ConcurrentModification(_)
        )
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
            Self::ThrottlingQuotaExceeded(e) => write!(f, "{}", e.kafka_error),
            Self::IllegalArgument(msg) => write!(f, "IllegalArgumentError: {msg}"),
            Self::IllegalState(msg) => write!(f, "IllegalStateError: {msg}"),
            Self::Timeout(msg) => write!(f, "TimeoutError: {msg}"),
            Self::RecordTooLarge(msg) => write!(f, "RecordTooLargeError: {msg}"),
            Self::Serialization(msg) => write!(f, "SerializationError: {msg}"),
            Self::Wakeup(msg) => write!(f, "WakeupError: {msg}"),
            Self::ConcurrentModification(msg) => write!(f, "ConcurrentModificationError: {msg}"),
            Self::TransactionAborted(msg) => write!(f, "TransactionAbortedError: {msg}"),
        }
    }
}

impl std::error::Error for KafkaError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ConcurrentModification` mirrors `IllegalState`: a plain Java
    /// `RuntimeException`, so it carries no protocol code, is never
    /// retriable or fatal, and is neither an `ApiException` nor a
    /// `KafkaException`.
    #[test]
    fn concurrent_modification_parity_with_illegal_state() {
        let cme = KafkaError::concurrent_modification("KafkaConsumer is not safe for multi-threaded access.");
        let ise = KafkaError::illegal_state("bad state");

        assert_eq!(cme.message(), "KafkaConsumer is not safe for multi-threaded access.");
        assert_eq!(cme.code(), ise.code());
        assert_eq!(cme.error(), ise.error());
        assert_eq!(cme.is_retriable(), ise.is_retriable());
        assert!(!cme.is_retriable());
        assert_eq!(cme.is_fatal(), ise.is_fatal());
        assert!(!cme.is_fatal());
        assert_eq!(cme.is_api_exception(), ise.is_api_exception());
        assert!(!cme.is_api_exception());
        assert_eq!(cme.is_kafka_exception(), ise.is_kafka_exception());
        assert!(!cme.is_kafka_exception());
        assert!(cme.kafka_error().is_none());
    }

    #[test]
    fn concurrent_modification_display() {
        let cme = KafkaError::concurrent_modification("oops");
        assert_eq!(cme.to_string(), "ConcurrentModificationError: oops");
    }

    #[test]
    fn transaction_aborted_default_message() {
        // Java's no-arg TransactionAbortedException message, verbatim.
        let err = KafkaError::transaction_aborted();
        assert_eq!(err.message(), "Failing batch since transaction was aborted");
    }

    #[test]
    fn transaction_aborted_custom_message() {
        let err = KafkaError::transaction_aborted_with_message("custom reason");
        assert_eq!(err.message(), "custom reason");
    }

    #[test]
    fn transaction_aborted_display() {
        let err = KafkaError::transaction_aborted();
        assert_eq!(
            err.to_string(),
            "TransactionAbortedError: Failing batch since transaction was aborted"
        );
    }

    #[test]
    fn transaction_aborted_has_no_error_code() {
        // TransactionAbortedException is absent from Java's Errors enum, which
        // is the whole reason this is its own variant.
        assert!(KafkaError::transaction_aborted().kafka_error().is_none());
    }

    #[test]
    fn transaction_aborted_is_api_exception() {
        // Java: TransactionAbortedException extends ApiException. This is the
        // difference from Wakeup, which is a KafkaException but NOT an
        // ApiException — the distinction drives whether KafkaProducer.doSend()
        // fails the future or propagates.
        let err = KafkaError::transaction_aborted();
        assert!(err.is_api_exception());
        assert!(err.is_kafka_exception());
        assert!(!KafkaError::wakeup("w").is_api_exception());
    }

    #[test]
    fn transaction_aborted_is_not_retriable_or_fatal() {
        let err = KafkaError::transaction_aborted();
        assert!(!err.is_retriable());
        assert!(!err.is_fatal());
        assert!(!err.txn_requires_abort());
    }
}
