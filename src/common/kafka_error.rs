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
//! [`KafkaError`] base with common fields (error code, message) plus its own
//! subclass-specific fields.
//!
//! Two types share this file, and the distinction matters:
//!
//!  - [`KafkaError`] is the struct translating Java's `KafkaException` base
//!    class — protocol error code and optional message. Every specific error
//!    struct embeds one.
//!  - [`Error`] is the unified enum used for polymorphic error handling in
//!    return types and storage, replacing the separate `MetadataError` and
//!    `UnsupportedApiError` types. It has no Java counterpart: it exists
//!    because Rust cannot express Java's class hierarchy, so it flattens
//!    both `KafkaException`'s subclasses AND the generic `java.lang` /
//!    `java.util` runtime exceptions into one type.
//!
//! `Error::KafkaError(KafkaError)` is therefore the variant for a *bare*
//! `KafkaException` — one with no subclass-specific fields. It is NOT "the
//! variant for Kafka errors"; `Error::Timeout` and the rest are Kafka errors
//! too. See [`Error::is_kafka_error`] for that test.
//!
//! The file keeps its `kafka_error` name because it translates
//! `KafkaException.java`, as does the C FFI type
//! `kafka_common_Error_t` (CLAUDE.md §3).

use std::collections::HashSet;
use std::fmt;

use super::Errors;

// ---------------------------------------------------------------------------
// Base struct — corresponds to Java's KafkaException / ApiException
// ---------------------------------------------------------------------------

/// Base Kafka error with common fields shared by all error types.
///
/// Corresponds to Java's `KafkaException` / `ApiException` base class.
/// Contains the protocol error code and an optional custom message —
/// exactly the state Java's `KafkaException` carries. Fatality is NOT state
/// here: like Java, it is derived from the error's identity, see
/// [`Error::is_fatal`].
///
/// Specific error types (e.g., [`TopicAuthorizationError`]) embed this
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
/// use confluent_kafka::common::protocol::Errors;
///
/// let err = KafkaError::new(Errors::RequestTimedOut);
/// assert!(err.is_retriable());
/// assert_eq!(err.code(), 7);
/// ```
#[derive(Clone, Debug)]
pub struct KafkaError {
    /// The protocol error code.
    error: Errors,
    /// Custom error message. If `None`, [`Errors::message()`] is used.
    custom_message: Option<String>,
}

impl KafkaError {
    /// Create a `KafkaError` from an error code with the default message.
    pub fn new(error: Errors) -> Self {
        Self { error, custom_message: None }
    }

    /// Create a `KafkaError` from an error code with a custom message.
    pub fn with_message(error: Errors, message: impl Into<String>) -> Self {
        Self { error, custom_message: Some(message.into()) }
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
    /// Delegates to [`Errors::is_retriable()`], which is `true` for exactly
    /// the error codes whose Java exception class extends
    /// `RetriableException` — including through `RefreshRetriableException`
    /// and `InvalidMetadataException`. That equivalence is enforced by
    /// `errors.rs`'s `test_retriable_errors_match_java_hierarchy`.
    pub fn is_retriable(&self) -> bool {
        self.error.is_retriable()
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for KafkaError {}

// ---------------------------------------------------------------------------
// Specific error structs — correspond to Java error subclasses
// ---------------------------------------------------------------------------

/// Topic authorization failure with the set of unauthorized topics.
///
/// Corresponds to Java's `TopicAuthorizationException`.
#[derive(Clone, Debug)]
pub struct TopicAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The set of unauthorized topics.
    pub unauthorized_topics: HashSet<String>,
}

impl TopicAuthorizationError {
    /// Create a new topic authorization error.
    pub fn new(unauthorized_topics: HashSet<String>) -> Self {
        Self {
            kafka_error: KafkaError::new(Errors::TopicAuthorizationFailed),
            unauthorized_topics,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }
}

/// Invalid topic error with the set of invalid topics.
///
/// Corresponds to Java's `InvalidTopicException`.
#[derive(Clone, Debug)]
pub struct InvalidTopicError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The set of invalid topics.
    pub invalid_topics: HashSet<String>,
}

impl InvalidTopicError {
    /// Create a new invalid topic error.
    pub fn new(invalid_topics: HashSet<String>) -> Self {
        Self { kafka_error: KafkaError::new(Errors::InvalidTopicException), invalid_topics }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }
}

/// Group authorization failure with the group ID.
///
/// Corresponds to Java's `GroupAuthorizationException`.
#[derive(Clone, Debug)]
pub struct GroupAuthorizationError {
    /// Base error fields.
    kafka_error: KafkaError,
    /// The group ID that failed authorization.
    pub group_id: String,
}

impl GroupAuthorizationError {
    /// Create a new group authorization error.
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::new(Errors::GroupAuthorizationFailed),
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
            kafka_error: KafkaError::with_message(Errors::GroupAuthorizationFailed, message),
            group_id: group_id.into(),
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
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
    kafka_error: KafkaError,
    /// The amount of time to wait before retrying, in milliseconds.
    pub throttle_time_ms: i32,
}

impl ThrottlingQuotaExceededError {
    /// Create a new throttling quota exceeded error.
    pub fn new(throttle_time_ms: i32, message: impl Into<String>) -> Self {
        Self {
            kafka_error: KafkaError::with_message(Errors::ThrottlingQuotaExceeded, message),
            throttle_time_ms,
        }
    }

    /// Access the base error.
    pub fn kafka_error(&self) -> &KafkaError {
        &self.kafka_error
    }

    /// The amount of time to wait before retrying, in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms
    }
}

// ---------------------------------------------------------------------------
// Error — unified enum for polymorphic error handling
// ---------------------------------------------------------------------------

/// Unified error type for polymorphic error handling — this crate's
/// top-level error, returned by every fallible API.
///
/// This enum wraps the error struct hierarchy so that any error can be
/// stored and returned through a single type. Most variants hold a specific
/// error struct that embeds [`KafkaError`] as its base;
/// [`KafkaError`](Self::KafkaError) holds a bare one, standing for Java's
/// `KafkaException` with no subclass.
///
/// It also carries the generic programming errors that Java keeps OUTSIDE
/// the `KafkaException` hierarchy ([`IllegalArgument`](Self::IllegalArgument),
/// [`IllegalState`](Self::IllegalState),
/// [`ConcurrentModification`](Self::ConcurrentModification)); flattening two
/// Java families into one enum is what makes
/// [`is_kafka_error`](Self::is_kafka_error) necessary.
///
/// `error()`, `code()`, `message()` and `is_retriable()` are delegated to
/// the inner [`KafkaError`] base. `is_fatal()` and `txn_requires_abort()`
/// live only here: `KafkaError` mirrors Java's `KafkaException`, which has
/// neither — they are librdkafka-style predicates required by CLAUDE.md
/// §10.3, so they belong on this enum rather than on the Java-shaped base.
///
/// This type is used in `Result` return types and `Option` storage where
/// any kind of Kafka error may occur.
#[derive(Clone, Debug)]
pub enum Error {
    /// A plain Kafka error carrying only the base fields — Java's bare
    /// `KafkaException`, with no subclass-specific context.
    KafkaError(KafkaError),
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
    BufferExhausted(KafkaError),
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
}

impl Error {
    // -- Convenience constructors ------------------------------------------

    /// Create a bare Kafka error from an error code.
    pub fn new(error: Errors) -> Self {
        Self::KafkaError(KafkaError::new(error))
    }

    /// Create a bare Kafka error with a custom message.
    pub fn with_message(error: Errors, message: impl Into<String>) -> Self {
        Self::KafkaError(KafkaError::with_message(error, message))
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
        Self::KafkaError(KafkaError::with_message(Errors::InvalidGroupId, message))
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
        Self::BufferExhausted(KafkaError::with_message(Errors::UnknownServerError, message))
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
        Self::KafkaError(KafkaError::with_message(Errors::UnsupportedVersion, message))
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

    /// Create a record batch too large error.
    ///
    /// Corresponds to Java's `RecordBatchTooLargeException`.
    pub fn record_batch_too_large(message: impl Into<String>) -> Self {
        Self::KafkaError(KafkaError::with_message(Errors::MessageTooLarge, message))
    }

    // -- Base access -------------------------------------------------------

    /// Access the base [`KafkaError`] common to all variants.
    ///
    /// Returns `None` for variants that do not carry a [`KafkaError`]
    /// (e.g. [`IllegalArgument`](Self::IllegalArgument), [`IllegalState`](Self::IllegalState)).
    pub fn kafka_error(&self) -> Option<&KafkaError> {
        match self {
            Self::KafkaError(e) | Self::BufferExhausted(e) => Some(e),
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
            | Self::ConcurrentModification(_) => None,
        }
    }

    // -- Delegating methods ------------------------------------------------

    /// The protocol error code.
    ///
    /// Returns [`Errors::UnknownServerError`] for variants without a
    /// [`KafkaError`].
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
            | Self::ConcurrentModification(msg) => msg,
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

    /// Whether this error is fatal, i.e. whether retrying is pointless
    /// because the condition cannot clear on its own.
    ///
    /// Derived from the error's identity, never from per-instance state —
    /// this is Java's definition. Java has no `KafkaException.isFatal()`; its
    /// one general-purpose test is `RequestUtils.isFatalException(Throwable)`
    /// (`common/requests/RequestUtils.java:88`), which asks whether the
    /// exception's *class* is in the authentication / authorization /
    /// unsupported family. See [`Errors::is_fatal`] for the full class list
    /// and the resulting 13 error codes.
    ///
    /// Consequently the generic variants ([`IllegalArgument`](Self::IllegalArgument),
    /// [`IllegalState`](Self::IllegalState),
    /// [`ConcurrentModification`](Self::ConcurrentModification)) and the
    /// String-carrying Kafka variants ([`Timeout`](Self::Timeout),
    /// [`Serialization`](Self::Serialization), ...) are never fatal: they
    /// resolve to [`Errors::UnknownServerError`], which is not in the family.
    ///
    /// Note this is NOT Java's *other* notion of fatality,
    /// `TransactionManager.hasFatalError()`, which is a state-machine state
    /// (`currentState == FATAL_ERROR`) rather than a property of any error.
    pub fn is_fatal(&self) -> bool {
        self.error().is_fatal()
    }

    /// Whether this error requires the transaction to be aborted.
    pub fn txn_requires_abort(&self) -> bool {
        // Purely a function of the error code, so it belongs on `Errors`;
        // `KafkaError` does not duplicate it.
        self.kafka_error().is_some_and(|e| e.error().txn_requires_abort())
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
    /// - `KafkaError` (covers all other Errors-based exceptions)
    /// - `TopicAuthorization` (TopicAuthorizationException extends ApiException)
    /// - `GroupAuthorization` (GroupAuthorizationException extends ApiException)
    /// - `BufferExhausted` (BufferExhaustedException extends ApiException)
    ///
    /// NOT `ApiException`:
    /// - `IllegalArgument` (IllegalArgumentException extends RuntimeException)
    /// - `IllegalState` (IllegalStateException extends RuntimeException)
    /// - `Serialization` (SerializationException extends KafkaException, NOT ApiException)
    ///
    /// Not to be confused with [`is_kafka_error`](Self::is_kafka_error), which
    /// asks the broader question (is this from Kafka at all, vs. a generic
    /// programming error?). `Serialization` and `Wakeup` separate the two.
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

    /// Whether this is a Kafka error rather than a generic programming
    /// error.
    ///
    /// This enum flattens two families that Java keeps apart by class
    /// hierarchy: Kafka's own `KafkaException` tree, and the generic
    /// `java.lang` / `java.util` runtime exceptions that sit beside it as
    /// siblings rather than below it (`common/KafkaException.java:22`).
    /// The predicate recovers that distinction, mirroring Java's
    /// `t instanceof KafkaException` test.
    ///
    /// Returns `false` for exactly the generic variants — the ones raised
    /// by misuse of the client rather than by Kafka itself:
    /// - [`IllegalArgument`](Self::IllegalArgument) (`IllegalArgumentException`)
    /// - [`IllegalState`](Self::IllegalState) (`IllegalStateException`)
    /// - [`ConcurrentModification`](Self::ConcurrentModification)
    ///   (`ConcurrentModificationException`)
    ///
    /// Everything else returns `true`: the `ApiException` subtypes,
    /// [`Serialization`](Self::Serialization), [`Wakeup`](Self::Wakeup) and
    /// the bare [`KafkaError`](Self::KafkaError) all map to `KafkaException`
    /// subclasses.
    ///
    /// **This is not a test for the [`KafkaError`](Self::KafkaError) variant.**
    /// That variant means "a bare `KafkaException`, no subclass"; this
    /// predicate means "inside the `KafkaException` hierarchy at all", which
    /// is true of `Timeout`, `TopicAuthorization` and most other variants
    /// too. To test the variant, match on it.
    ///
    /// Beware the polarity difference against the sibling
    /// [`is_api_exception`](Self::is_api_exception): both return `true` for
    /// the in-hierarchy case, but they are not the same test — `Serialization`
    /// and `Wakeup` are Kafka errors that are NOT `ApiException`s, so they
    /// return `true` here and `false` there.
    ///
    /// Two call sites depend on this:
    /// - `ConsumerUtils.maybeWrapAsKafkaException(t, message)`
    ///   (`ConsumerUtils.java:256`) — a Kafka error passes through unchanged;
    ///   a generic one gets wrapped in a new `KafkaException(message, t)`.
    /// - `FetchCollector` — Java's `catch (KafkaException e)` cannot catch a
    ///   generic error, so those propagate where Kafka errors are swallowed.
    pub fn is_kafka_error(&self) -> bool {
        !matches!(
            self,
            Self::IllegalArgument(_) | Self::IllegalState(_) | Self::ConcurrentModification(_)
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KafkaError(e) | Self::BufferExhausted(e) => write!(f, "{e}"),
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
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ConcurrentModification` mirrors `IllegalState`: a plain Java
    /// `RuntimeException`, so it carries no protocol code, is never
    /// retriable or fatal, and is neither an `ApiException` nor a
    /// `KafkaException`.
    #[test]
    fn concurrent_modification_parity_with_illegal_state() {
        let cme = Error::concurrent_modification("KafkaConsumer is not safe for multi-threaded access.");
        let ise = Error::illegal_state("bad state");

        assert_eq!(cme.message(), "KafkaConsumer is not safe for multi-threaded access.");
        assert_eq!(cme.code(), ise.code());
        assert_eq!(cme.error(), ise.error());
        assert_eq!(cme.is_retriable(), ise.is_retriable());
        assert!(!cme.is_retriable());
        assert_eq!(cme.is_fatal(), ise.is_fatal());
        assert!(!cme.is_fatal());
        assert_eq!(cme.is_api_exception(), ise.is_api_exception());
        assert!(!cme.is_api_exception());
        assert_eq!(cme.is_kafka_error(), ise.is_kafka_error());
        assert!(!cme.is_kafka_error());
        assert!(cme.kafka_error().is_none());
    }

    #[test]
    fn concurrent_modification_display() {
        let cme = Error::concurrent_modification("oops");
        assert_eq!(cme.to_string(), "ConcurrentModificationError: oops");
    }
}
