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
// Imported unqualified so that `#[delegate(Display)]` on `Error` resolves to
// the std trait; the `#[delegatable_trait_remote]` stub below only registers
// the signature with ambassador, it does not define a trait.
use std::fmt::Display;

use ambassador::{Delegate, delegatable_trait, delegatable_trait_remote};

use super::Errors;
use crate::common::InvalidRecordError;
use crate::common::config::ConfigError;
use crate::common::errors::{
    ApiError, AuthenticationError, AuthorizationError, AuthorizerNotReadyError, BrokerIdNotRegisteredError,
    BrokerNotAvailableError, ClusterAuthorizationError, ConcurrentTransactionsError, ControllerMovedError,
    CoordinatorLoadInProgressError, CoordinatorNotAvailableError, CorruptRecordError,
    DelegationTokenAuthorizationError, DelegationTokenDisabledError, DelegationTokenExpiredError,
    DelegationTokenNotFoundError, DelegationTokenOwnerMismatchError, DisconnectError, DuplicateBrokerRegistrationError,
    DuplicateResourceError, DuplicateSequenceError, DuplicateVoterError, ElectionNotNeededError,
    EligibleLeadersNotAvailableError, FeatureUpdateFailedError, FencedInstanceIdError, FencedLeaderEpochError,
    FencedMemberEpochError, FencedStateEpochError, FetchSessionIdNotFoundError, FetchSessionTopicIdError,
    GroupAuthorizationError, GroupIdNotFoundError, GroupMaxSizeReachedError, GroupNotEmptyError,
    GroupSubscribedToTopicError, IllegalGenerationError, IllegalSaslStateError, InconsistentClusterIdError,
    InconsistentGroupProtocolError, InconsistentTopicIdError, InconsistentVoterSetError, IneligibleReplicaError,
    InterruptError, InvalidCommitOffsetSizeError, InvalidConfigurationError, InvalidFetchSessionEpochError,
    InvalidFetchSizeError, InvalidGroupIdError, InvalidOffsetError, InvalidPartitionsError, InvalidPidMappingError,
    InvalidPrincipalTypeError, InvalidProducerEpochError, InvalidRecordStateError, InvalidRegistrationError,
    InvalidRegularExpressionError, InvalidReplicaAssignmentError, InvalidReplicationFactorError, InvalidRequestError,
    InvalidRequiredAcksError, InvalidSessionTimeoutError, InvalidShareSessionEpochError, InvalidTimestampError,
    InvalidTopicError, InvalidTxnStateError, InvalidTxnTimeoutError, InvalidUpdateVersionError, InvalidVoterKeyError,
    KafkaStorageError, LeaderNotAvailableError, ListenerNotFoundError, LogDirNotFoundError, MemberIdRequiredError,
    MismatchedEndpointTypeError, NetworkError, NewLeaderElectedError, NoReassignmentInProgressError,
    NotControllerError, NotCoordinatorError, NotEnoughReplicasAfterAppendError, NotEnoughReplicasError,
    NotLeaderOrFollowerError, OffsetMetadataTooLargeError, OffsetMovedToTieredStorageError, OffsetNotAvailableError,
    OffsetOutOfRangeError, OperationNotAttemptedError, OutOfOrderSequenceError, PolicyViolationError,
    PositionOutOfRangeError, PreferredLeaderNotAvailableError, PrincipalDeserializationError, ProducerFencedError,
    ReassignmentInProgressError, RebalanceInProgressError, RebootstrapRequiredError, RecordBatchTooLargeError,
    RecordDeserializationError, RecordTooLargeError, ReplicaNotAvailableError, ResourceNotFoundError,
    SaslAuthenticationError, SecurityDisabledError, SerializationError, ShareSessionLimitReachedError,
    ShareSessionNotFoundError, SnapshotNotFoundError, SslAuthenticationError, StaleBrokerEpochError,
    StaleMemberEpochError, StreamsInvalidTopologyEpochError, StreamsInvalidTopologyError, StreamsTopologyFencedError,
    TelemetryTooLargeError, ThrottlingQuotaExceededError, TimeoutError, TopicAuthorizationError,
    TopicDeletionDisabledError, TopicExistsError, TransactionAbortableError, TransactionAbortedError,
    TransactionCoordinatorFencedError, TransactionalIdAuthorizationError, TransactionalIdNotFoundError,
    UnacceptableCredentialError, UnknownControllerIdError, UnknownLeaderEpochError, UnknownMemberIdError,
    UnknownProducerIdError, UnknownServerError, UnknownSubscriptionIdError, UnknownTopicIdError,
    UnknownTopicOrPartitionError, UnreleasedInstanceIdError, UnstableOffsetCommitError, UnsupportedAssignorError,
    UnsupportedByAuthenticationError, UnsupportedCompressionTypeError, UnsupportedEndpointTypeError,
    UnsupportedForMessageFormatError, UnsupportedSaslMechanismError, UnsupportedVersionError, VoterNotFoundError,
    WakeupError,
};
use crate::common::network::InvalidReceiveError;
use crate::consumer::{
    ConsumerCommitFailedError, ConsumerLogTruncationError, ConsumerNoOffsetForPartitionError,
    ConsumerOffsetOutOfRangeError, ConsumerRetriableCommitFailedError,
};
use crate::producer::ProducerBufferExhaustedError;

/// Registers `std::fmt::Display` for delegation.
///
/// Ambassador cannot see a foreign trait's signature, so it is restated here.
/// This declaration defines nothing — the attribute consumes it and emits only
/// the delegation macro, which is why `Display` above still refers to the std
/// trait.
#[delegatable_trait_remote]
trait Display {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error>;
}

// ---------------------------------------------------------------------------
// The exception hierarchy, as a trait
// ---------------------------------------------------------------------------

/// Java's exception hierarchy, recovered as a set of predicates.
///
/// **Mechanism, not API.** This trait is `pub(crate)`: it exists so each error
/// payload can declare its ancestry once, and so ambassador can generate
/// [`Error`]'s forwarding. Callers use the inherent methods on [`Error`], which
/// carry the public documentation for each predicate.
///
/// It has no Java counterpart — it is the Rust encoding of the `extends` chain
/// that [`Error`] flattens away (CLAUDE.md §10.3/§10.4). One method per
/// **intermediate** class in Java's tree; leaf classes need no predicate,
/// because they are a single [`Error`] variant or a single error code.
///
/// Every method defaults to `false`, so each concrete error type declares its
/// ancestry by overriding exactly the predicates that are `true` for it. The
/// override list of an `impl` therefore reads as that type's Java `extends`
/// chain, which is the property a flat enum otherwise destroys:
///
/// ```text
/// TimeoutException extends RetriableException extends ApiException extends KafkaException
///   => is_retriable_error + is_api_error + is_kafka_error are overridden to true
/// ```
///
/// [`Error`] implements this trait through ambassador's `#[derive(Delegate)]`,
/// which generates the `match` forwarding each predicate to the variant's
/// payload. That is why every variant carries a distinct payload type: the
/// payload — not the variant — is what answers the question, and two variants
/// sharing one type could only ever give one answer.
///
/// Predicates are NOT complements of one another. [`SerializationError`] and
/// [`WakeupError`] are `KafkaException`s that are not `ApiException`s, so they
/// answer `true` to [`is_kafka_error`](Self::is_kafka_error) and `false` to
/// [`is_api_error`](Self::is_api_error).
#[delegatable_trait]
pub(crate) trait ErrorHierarchy {
    /// Whether this is a Kafka error rather than a generic programming error —
    /// Java's `t instanceof KafkaException`.
    ///
    /// `false` for exactly the generic `java.lang` / `java.util` runtime
    /// exceptions, which are siblings of `KafkaException` rather than
    /// subclasses (`common/KafkaException.java:22`): [`IllegalArgumentError`],
    /// [`IllegalStateError`], [`ConcurrentModificationError`].
    ///
    /// Two call sites depend on this: `ConsumerUtils.maybeWrapAsKafkaException`
    /// (a Kafka error passes through unchanged, a generic one gets wrapped),
    /// and `FetchCollector`, whose Java `catch (KafkaException e)` cannot catch
    /// a generic error.
    fn is_kafka_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `ApiException`.
    ///
    /// Narrower than [`is_kafka_error`](Self::is_kafka_error):
    /// `SerializationException` and `WakeupException` extend `KafkaException`
    /// directly, bypassing `ApiException`. `KafkaProducer.doSend()` catches
    /// `ApiException` and fails the record's future; anything else propagates.
    fn is_api_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `RetriableException` — i.e.
    /// whether re-sending the failed request can succeed.
    fn is_retriable_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `RefreshRetriableException` —
    /// retriable, and a metadata / coordinator refresh is what clears it.
    fn is_refresh_retriable_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `TimeoutException`.
    ///
    /// Intermediate because `BufferExhaustedException extends TimeoutException`
    /// (`clients/producer/BufferExhaustedException.java`), so Java's
    /// `catch (TimeoutException e)` catches the buffer-exhausted case too.
    fn is_timeout_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `InvalidMetadataException` — the
    /// client's cached metadata may be stale.
    fn is_invalid_metadata_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `InvalidConfigurationException`.
    ///
    /// Parent of BOTH [`is_authentication_error`](Self::is_authentication_error)
    /// and [`is_authorization_error`](Self::is_authorization_error) — in Kafka
    /// 4.2 those two extend `InvalidConfigurationException`, not `ApiException`
    /// directly, so anything answering `true` to either must answer `true` here.
    fn is_invalid_configuration_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `ApplicationRecoverableException`
    /// — the application can recover by re-initialising its producer or
    /// rejoining its group, but the current epoch / session is lost.
    fn is_application_recoverable_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `InvalidOffsetException`.
    fn is_invalid_offset_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends
    /// `org.apache.kafka.clients.consumer.InvalidOffsetException`.
    ///
    /// A **different** class from the one
    /// [`is_invalid_offset_error`](Self::is_invalid_offset_error) tests: the
    /// consumer package has its own abstract `InvalidOffsetException` extending
    /// `KafkaException`, whereas `common.errors`' is concrete and extends
    /// `ApiException`. Java keeps them apart by package; this flat namespace
    /// cannot, so the package is carried in the name.
    fn is_consumer_invalid_offset_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends
    /// `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
    ///
    /// Intermediate because `LogTruncationException` extends it
    /// (`clients/consumer/LogTruncationException.java`). Nested inside
    /// [`is_consumer_invalid_offset_error`](Self::is_consumer_invalid_offset_error),
    /// and distinct from the `common.errors` class of the same name.
    fn is_consumer_offset_out_of_range_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `OutOfOrderSequenceException`.
    fn is_out_of_order_sequence_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `SerializationException`.
    ///
    /// No protocol code maps here — `SerializationException` and its subclass
    /// `RecordDeserializationException` are raised client-side — so only the
    /// [`SerializationError`] payload answers `true`.
    fn is_serialization_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `AuthenticationException`.
    ///
    /// Covers only broker-reported codes. A handshake failure detected locally
    /// is carried as an `AuthenticationError` payload inside an `io::Error`
    /// (`common::network::auth_io_error`) and never reaches [`Error`], so it
    /// answers `false`.
    fn is_authentication_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `AuthorizationException`.
    fn is_authorization_error(&self) -> bool {
        false
    }
}

/// The protocol error code an error corresponds to.
///
/// **Mechanism, not API**, like [`ErrorHierarchy`]. Java's exceptions do not
/// store their code; `Errors` keeps a `CLASS_TO_ERROR` map and
/// `Errors.forException(t)` looks the class up in it
/// (`protocol/Errors.java:521`). This trait is that map inverted into a
/// per-type answer, which is the only form the compiler can check.
///
/// The default is [`Errors::UnknownServerError`], matching what
/// [`Error::error`] has always returned for payloads that carry no protocol
/// code — the generic `java.lang` errors and the client-side-only
/// `KafkaException` subclasses.
#[delegatable_trait]
pub(crate) trait ErrorCode {
    /// The protocol error code for this error's Java class.
    fn error(&self) -> Errors {
        Errors::UnknownServerError
    }
}

/// Declares one Java exception class from `org.apache.kafka.common.errors`.
///
/// Generates the struct (message only), its `Display` (`"<TypeName>: <message>"`,
/// mirroring Java's `Throwable.toString()`), and the [`ErrorMessage`],
/// [`ErrorCode`] and [`ErrorHierarchy`] impls.
///
/// The `extends:` list is the class's Java `extends` chain, written as the
/// predicates it makes true. Everything omitted inherits the trait's `false`, so
/// the list is the whole statement of the type's ancestry and can be diffed
/// against the Java file directly:
///
/// ```text
/// NetworkException extends InvalidMetadataException extends RefreshRetriableException
///     extends RetriableException extends ApiException extends KafkaException
/// => extends: [is_kafka_error, is_api_error, is_retriable_error,
///              is_refresh_retriable_error, is_invalid_metadata_error]
/// ```
macro_rules! kafka_error_class {
    // No `code:` — the class has no entry in `Errors.java`, and walking its
    // superclasses (Java's `Errors.forException`) finds none either, so
    // `ErrorCode` takes its default of `Errors::UnknownServerError`.
    (
        $(#[$meta:meta])*
        $name:ident,
        extends: [$($predicate:ident),* $(,)?] $(,)?
    ) => {
        kafka_error_class! {
            $(#[$meta])*
            $name,
            code: $crate::common::protocol::Errors::UnknownServerError,
            extends: [$($predicate),*],
        }
    };
    (
        $(#[$meta:meta])*
        $name:ident,
        code: $code:expr,
        extends: [$($predicate:ident),* $(,)?] $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        pub struct $name {
            message: String,
            /// The underlying cause, translating Java's `Throwable` `cause`.
            source: Option<Box<$crate::common::Error>>,
        }

        impl $name {
            /// Create the error with the given message and no cause.
            pub fn new(message: impl Into<String>) -> Self {
                Self { message: message.into(), source: None }
            }

            /// Create the error with the given message and an underlying cause,
            /// mirroring Java's `(String message, Throwable cause)` constructor.
            pub fn with_source(message: impl Into<String>, source: $crate::common::Error) -> Self {
                Self { message: message.into(), source: Some(Box::new(source)) }
            }

            /// Create the error with the default message for its error code,
            /// mirroring Java's `Errors.exception()`.
            pub fn with_default_message() -> Self {
                Self::new($code.message())
            }

            /// The error message.
            pub fn message(&self) -> &str {
                &self.message
            }

            /// The underlying cause, if any. Mirrors Java's `getCause()`.
            pub fn source(&self) -> Option<&$crate::common::Error> {
                self.source.as_deref()
            }
        }

        impl $crate::common::kafka_error::ErrorSource for $name {
            fn source(&self) -> Option<&$crate::common::Error> {
                self.source.as_deref()
            }
        }

        // The inherent `source()` above shadows both this and
        // `ErrorSource::source` for method-call syntax, so `e.source()` yields the
        // typed `Option<&Error>` while `StdError::source(&e)` gives the `dyn` view.
        impl ::std::error::Error for $name {
            fn source(&self) -> Option<&(dyn ::std::error::Error + 'static)> {
                self.source
                    .as_deref()
                    .map(|e| e as &(dyn ::std::error::Error + 'static))
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.message)
            }
        }

        impl $crate::common::kafka_error::ErrorMessage for $name {
            fn message(&self) -> &str {
                &self.message
            }
        }

        impl $crate::common::kafka_error::ErrorCode for $name {
            fn error(&self) -> $crate::common::protocol::Errors {
                $code
            }
        }

        impl $crate::common::kafka_error::ErrorHierarchy for $name {
            $(
                fn $predicate(&self) -> bool {
                    true
                }
            )*
        }
    };
}

pub(crate) use kafka_error_class;

/// The message an error carries, translating Java's `Throwable.getMessage()`.
///
/// **Mechanism, not API**, like [`ErrorHierarchy`]: it exists so [`Error`] can
/// delegate [`Error::message`] to the variant's payload instead of matching.
/// Callers use [`Error::message`].
///
/// Distinct from [`fmt::Display`], which translates `Throwable.toString()` —
/// Java renders that as `getClass().getName() + ": " + getLocalizedMessage()`,
/// so `Display` prefixes the class name and may append subclass state (the
/// unauthorized topics, say) while this returns the bare message. Both are
/// pinned by tests; do not collapse them.
///
/// The method is **required**, with no default. There is no `&str` equivalent
/// of the predicates' `false`, and defaulting to `""` would reintroduce exactly
/// what this replaces: a new payload silently getting an empty message instead
/// of a compile error.
#[delegatable_trait]
pub(crate) trait ErrorMessage {
    /// The error message, without the class-name prefix `Display` adds.
    fn message(&self) -> &str;
}

/// The error that caused this one, translating Java's `Throwable.getCause()`.
///
/// **Mechanism, not API**, like [`ErrorMessage`]: it exists so [`Error`] can
/// delegate [`Error::source`] to the variant's payload instead of matching.
/// Callers use [`Error::source`], or [`std::error::Error::source`] for the
/// standard-library view.
///
/// `getCause()` is declared on `java.lang.Throwable`, so **every** exception has
/// a cause slot — not just `KafkaException`. Accordingly every error class in
/// this crate carries one, and every one answers here. Whether a class lets a
/// caller *set* it follows Java: 92 of the 150 `common.errors` classes expose a
/// `Throwable cause` constructor, and the rest inherit an always-null cause.
///
/// The method defaults to `None` — unlike [`ErrorMessage::message`], `None` is a
/// legitimate answer (Java's default cause is null), so a payload that genuinely
/// has no cause slot is not forced to state one.
#[delegatable_trait]
pub(crate) trait ErrorSource {
    /// The underlying cause, if any. Mirrors Java's `Throwable.getCause()`.
    ///
    /// The bare `Error` (not `crate::common::Error`) is deliberate:
    /// `#[delegatable_trait]` copies this signature verbatim into the generated
    /// `ambassador_impl_ErrorSource` macro, and a `crate::` path inside a macro
    /// definition resolves against the *expansion* site
    /// (clippy::crate_in_macro_def). So every module that delegates this trait
    /// imports `Error`, exactly as the modules delegating [`ErrorCode`] import
    /// `Errors`.
    fn source(&self) -> Option<&Error> {
        None
    }
}

// ---------------------------------------------------------------------------
// Boxed payloads
// ---------------------------------------------------------------------------
//
// NOTE: every predicate must be forwarded here. A trait method added without
// a matching line below silently falls back to the default (`false`) for any
// boxed payload — `ConsumerLogTruncation` regressed exactly that way.
//
// A variant may box its payload to keep `Error` small (see
// [`Error::ConsumerLogTruncation`]). Ambassador forwards to the payload by value-typed
// method call, so the box itself has to implement the traits — deref coercion
// does not satisfy a trait bound.

impl<T: ErrorSource + ?Sized> ErrorSource for Box<T> {
    fn source(&self) -> Option<&Error> {
        (**self).source()
    }
}

impl<T: ErrorHierarchy + ?Sized> ErrorHierarchy for Box<T> {
    fn is_kafka_error(&self) -> bool {
        (**self).is_kafka_error()
    }
    fn is_api_error(&self) -> bool {
        (**self).is_api_error()
    }
    fn is_retriable_error(&self) -> bool {
        (**self).is_retriable_error()
    }
    fn is_refresh_retriable_error(&self) -> bool {
        (**self).is_refresh_retriable_error()
    }
    fn is_timeout_error(&self) -> bool {
        (**self).is_timeout_error()
    }
    fn is_invalid_metadata_error(&self) -> bool {
        (**self).is_invalid_metadata_error()
    }
    fn is_invalid_configuration_error(&self) -> bool {
        (**self).is_invalid_configuration_error()
    }
    fn is_application_recoverable_error(&self) -> bool {
        (**self).is_application_recoverable_error()
    }
    fn is_invalid_offset_error(&self) -> bool {
        (**self).is_invalid_offset_error()
    }
    fn is_consumer_invalid_offset_error(&self) -> bool {
        (**self).is_consumer_invalid_offset_error()
    }
    fn is_consumer_offset_out_of_range_error(&self) -> bool {
        (**self).is_consumer_offset_out_of_range_error()
    }
    fn is_out_of_order_sequence_error(&self) -> bool {
        (**self).is_out_of_order_sequence_error()
    }
    fn is_serialization_error(&self) -> bool {
        (**self).is_serialization_error()
    }
    fn is_authentication_error(&self) -> bool {
        (**self).is_authentication_error()
    }
    fn is_authorization_error(&self) -> bool {
        (**self).is_authorization_error()
    }
}

impl<T: ErrorMessage + ?Sized> ErrorMessage for Box<T> {
    fn message(&self) -> &str {
        (**self).message()
    }
}

impl<T: ErrorCode + ?Sized> ErrorCode for Box<T> {
    fn error(&self) -> Errors {
        (**self).error()
    }
}

// ---------------------------------------------------------------------------
// Base struct — corresponds to Java's KafkaException / ApiException
// ---------------------------------------------------------------------------

/// Base Kafka error with common fields shared by all error types.
///
/// Corresponds to Java's `KafkaException` / `ApiException` base class.
/// Contains the protocol error code and an optional custom message —
/// exactly the state Java's `KafkaException` carries. Fatality is NOT state
/// here: like Java, it is derived from the error's identity, see
/// `request_utils::is_fatal_error`.
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
/// assert_eq!(err.code(), 7);
/// assert_eq!(err.message(), Errors::RequestTimedOut.message());
/// ```
///
/// Classification lives on [`Error`], which wraps this type — Java's
/// `KafkaException` has no `isRetriable()` either:
///
/// ```
/// use confluent_kafka::common::Error;
/// use confluent_kafka::common::protocol::Errors;
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
    /// Create a `KafkaError` from an error code with the default message.
    pub fn new(error: Errors) -> Self {
        Self { error, custom_message: None, source: None }
    }

    /// Create a `KafkaError` from an error code with a custom message.
    pub fn with_message(error: Errors, message: impl Into<String>) -> Self {
        Self { error, custom_message: Some(message.into()), source: None }
    }

    /// Create a `KafkaError` from an error code, a custom message, and the error
    /// that caused it. Mirrors Java's `KafkaException(String message, Throwable cause)`.
    pub fn with_message_and_source(error: Errors, message: impl Into<String>, source: Error) -> Self {
        Self { error, custom_message: Some(message.into()), source: Some(Box::new(source)) }
    }

    /// Create a `KafkaError` from an error code and the error that caused it,
    /// keeping the code's default message. Mirrors `KafkaException(Throwable cause)`.
    pub fn with_source(error: Errors, source: Error) -> Self {
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

// ---------------------------------------------------------------------------
// Specific error structs — correspond to Java error subclasses
// ---------------------------------------------------------------------------

/// Declares a message-only error struct — one Java exception class that carries
/// nothing but its message.
///
/// Java gives each of these its own class, which is exactly what
/// `#[enum_dispatch]` requires: the payload type answers the hierarchy
/// predicates, so `Error::Timeout` and `Error::Wakeup` cannot both carry a
/// `String`. The generated `Display` renders as `"<TypeName>: <message>"`,
/// preserving the strings the previous hand-written `Display` produced.
macro_rules! message_only_error {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        pub struct $name {
            message: String,
            /// The underlying cause — `Throwable`'s `cause`, null by default.
            source: Option<Box<Error>>,
        }

        impl $name {
            /// Create the error with the given message and no cause.
            pub fn new(message: impl Into<String>) -> Self {
                Self { message: message.into(), source: None }
            }

            /// Create the error with the given message and an underlying cause,
            /// mirroring Java's `(String message, Throwable cause)` constructor.
            pub fn with_source(message: impl Into<String>, source: Error) -> Self {
                Self { message: message.into(), source: Some(Box::new(source)) }
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

        impl ErrorSource for $name {
            fn source(&self) -> Option<&Error> {
                self.source.as_deref()
            }
        }

        impl std::error::Error for $name {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
            }
        }

        // No protocol code: these are the generic `java.lang` errors and the
        // client-side-only `KafkaException` subclasses, so the trait default
        // (`Errors::UnknownServerError`) is the right answer.
        impl ErrorCode for $name {}

        impl ErrorMessage for $name {
            fn message(&self) -> &str {
                // Field access, not `self.message()`: that would resolve to the
                // inherent method above and is only accidentally equivalent.
                &self.message
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.message)
            }
        }
    };
}

message_only_error! {
    /// Illegal argument error — an invalid argument was provided to a method.
    ///
    /// Corresponds to Java's `java.lang.IllegalArgumentException`, a sibling of
    /// `KafkaException` rather than a subclass, so no predicate holds for it.
    IllegalArgumentError
}

message_only_error! {
    /// Illegal state error — a method was called in an invalid state.
    ///
    /// Corresponds to Java's `java.lang.IllegalStateException`; like
    /// [`IllegalArgumentError`], outside the `KafkaException` hierarchy.
    IllegalStateError
}

message_only_error! {
    /// Concurrent modification error — the consumer was accessed from more than
    /// one task.
    ///
    /// Corresponds to Java's `java.util.ConcurrentModificationException`, thrown
    /// by `KafkaConsumer.acquire()` ("KafkaConsumer is not safe for
    /// multi-threaded access"). A plain `RuntimeException`, so no predicate
    /// holds for it.
    ConcurrentModificationError
}

// ---------------------------------------------------------------------------
// Display — each payload renders itself, as each Java class has its own
// toString(). `Error`'s own Display just forwards (see `Error::as_display`).
// ---------------------------------------------------------------------------

// `ErrorMessage` is not hand-written for any payload: the message-only types get
// it from the `message_only_error!` macro, the five that embed a [`KafkaError`]
// delegate to that field via `#[delegate(ErrorMessage, target = "kafka_error")]`,
// and [`KafkaError`] itself uses `target = "self"` over its inherent method.

// ---------------------------------------------------------------------------
// ErrorHierarchy impls — each type's override list IS its Java `extends` chain
// ---------------------------------------------------------------------------

impl ErrorCode for KafkaError {
    fn error(&self) -> Errors {
        self.error
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

// `IllegalArgumentException`, `IllegalStateException` and
// `ConcurrentModificationException` are `java.lang` / `java.util` runtime
// exceptions sitting BESIDE `KafkaException`, not below it. Every predicate is
// false for them, so each takes the trait's defaults unchanged — the empty impl
// is the statement that they are outside the hierarchy.
impl ErrorHierarchy for IllegalArgumentError {}
impl ErrorHierarchy for IllegalStateError {}
impl ErrorHierarchy for ConcurrentModificationError {}

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
/// `error()`, `code()`, `message()` and `is_retriable_error()` are delegated to
/// the inner [`KafkaError`] base. `request_utils::is_fatal_error` and
/// `is_transaction_abortable_error()`
/// live only here: `KafkaError` mirrors Java's `KafkaException`, which has
/// neither — they are librdkafka-style predicates required by CLAUDE.md
/// §10.3, so they belong on this enum rather than on the Java-shaped base.
///
/// This type is used in `Result` return types and `Option` storage where
/// any kind of Kafka error may occur.
#[derive(Clone, Debug, Delegate)]
#[delegate(ErrorHierarchy)]
#[delegate(ErrorMessage)]
#[delegate(ErrorCode)]
#[delegate(ErrorSource)]
#[delegate(Display)]
pub enum Error {
    // Payloads defined in this file: the bare `KafkaException` and the
    // generic `java.lang` / `java.util` runtime errors, which have no
    // package in the Kafka source to mirror.
    /// A plain Kafka error carrying only the base fields — Java's bare
    /// `KafkaException`, with no subclass-specific context.
    KafkaError(KafkaError),
    /// Illegal argument error — an invalid argument was provided to a method.
    ///
    /// Corresponds to Java's `IllegalArgumentException`.
    IllegalArgument(IllegalArgumentError),
    /// Illegal state error — a method was called in an invalid state.
    ///
    /// Corresponds to Java's `IllegalStateException`.
    IllegalState(IllegalStateError),
    /// Concurrent modification error — the consumer was accessed from more
    /// than one thread.
    ///
    /// Corresponds to Java's `java.util.ConcurrentModificationException`,
    /// thrown by `KafkaConsumer.acquire()` ("KafkaConsumer is not safe for
    /// multi-threaded access"). Like `IllegalState`, it is a plain
    /// `RuntimeException` — neither an `ApiException` nor a `KafkaException`
    /// — so it is never retriable and never fatal.
    ConcurrentModification(ConcurrentModificationError),

    // One variant per Java exception class, each carrying the struct that
    // declares its own `extends` chain. Delegation does the rest.
    /// See [`ApiError`](crate::common::errors::api_error::ApiError).
    Api(ApiError),
    /// See [`AuthenticationError`](crate::common::errors::authentication_error::AuthenticationError).
    Authentication(AuthenticationError),
    /// See [`AuthorizationError`](crate::common::errors::authorization_error::AuthorizationError).
    Authorization(AuthorizationError),
    /// See [`AuthorizerNotReadyError`](crate::common::errors::authorizer_not_ready_error::AuthorizerNotReadyError).
    AuthorizerNotReady(AuthorizerNotReadyError),
    /// See [`BrokerIdNotRegisteredError`](crate::common::errors::broker_id_not_registered_error::BrokerIdNotRegisteredError).
    BrokerIdNotRegistered(BrokerIdNotRegisteredError),
    /// See [`BrokerNotAvailableError`](crate::common::errors::broker_not_available_error::BrokerNotAvailableError).
    BrokerNotAvailable(BrokerNotAvailableError),
    /// See [`ProducerBufferExhaustedError`](crate::producer::producer_buffer_exhausted_error::ProducerBufferExhaustedError).
    ProducerBufferExhausted(ProducerBufferExhaustedError),
    /// See [`ClusterAuthorizationError`](crate::common::errors::cluster_authorization_error::ClusterAuthorizationError).
    ClusterAuthorization(ClusterAuthorizationError),
    /// See [`ConcurrentTransactionsError`](crate::common::errors::concurrent_transactions_error::ConcurrentTransactionsError).
    ConcurrentTransactions(ConcurrentTransactionsError),
    /// See [`ControllerMovedError`](crate::common::errors::controller_moved_error::ControllerMovedError).
    ControllerMoved(ControllerMovedError),
    /// See [`CoordinatorLoadInProgressError`](crate::common::errors::coordinator_load_in_progress_error::CoordinatorLoadInProgressError).
    CoordinatorLoadInProgress(CoordinatorLoadInProgressError),
    /// See [`CoordinatorNotAvailableError`](crate::common::errors::coordinator_not_available_error::CoordinatorNotAvailableError).
    CoordinatorNotAvailable(CoordinatorNotAvailableError),
    /// See [`CorruptRecordError`](crate::common::errors::corrupt_record_error::CorruptRecordError).
    CorruptRecord(CorruptRecordError),
    /// See [`DelegationTokenAuthorizationError`](crate::common::errors::delegation_token_authorization_error::DelegationTokenAuthorizationError).
    DelegationTokenAuthorization(DelegationTokenAuthorizationError),
    /// See [`DelegationTokenDisabledError`](crate::common::errors::delegation_token_disabled_error::DelegationTokenDisabledError).
    DelegationTokenDisabled(DelegationTokenDisabledError),
    /// See [`DelegationTokenExpiredError`](crate::common::errors::delegation_token_expired_error::DelegationTokenExpiredError).
    DelegationTokenExpired(DelegationTokenExpiredError),
    /// See [`DelegationTokenNotFoundError`](crate::common::errors::delegation_token_not_found_error::DelegationTokenNotFoundError).
    DelegationTokenNotFound(DelegationTokenNotFoundError),
    /// See [`DelegationTokenOwnerMismatchError`](crate::common::errors::delegation_token_owner_mismatch_error::DelegationTokenOwnerMismatchError).
    DelegationTokenOwnerMismatch(DelegationTokenOwnerMismatchError),
    /// See [`DisconnectError`](crate::common::errors::disconnect_error::DisconnectError).
    Disconnect(DisconnectError),
    /// See [`DuplicateBrokerRegistrationError`](crate::common::errors::duplicate_broker_registration_error::DuplicateBrokerRegistrationError).
    DuplicateBrokerRegistration(DuplicateBrokerRegistrationError),
    /// See [`DuplicateResourceError`](crate::common::errors::duplicate_resource_error::DuplicateResourceError).
    DuplicateResource(DuplicateResourceError),
    /// See [`DuplicateSequenceError`](crate::common::errors::duplicate_sequence_error::DuplicateSequenceError).
    DuplicateSequence(DuplicateSequenceError),
    /// See [`DuplicateVoterError`](crate::common::errors::duplicate_voter_error::DuplicateVoterError).
    DuplicateVoter(DuplicateVoterError),
    /// See [`ElectionNotNeededError`](crate::common::errors::election_not_needed_error::ElectionNotNeededError).
    ElectionNotNeeded(ElectionNotNeededError),
    /// See [`EligibleLeadersNotAvailableError`](crate::common::errors::eligible_leaders_not_available_error::EligibleLeadersNotAvailableError).
    EligibleLeadersNotAvailable(EligibleLeadersNotAvailableError),
    /// See [`FeatureUpdateFailedError`](crate::common::errors::feature_update_failed_error::FeatureUpdateFailedError).
    FeatureUpdateFailed(FeatureUpdateFailedError),
    /// See [`FencedInstanceIdError`](crate::common::errors::fenced_instance_id_error::FencedInstanceIdError).
    FencedInstanceId(FencedInstanceIdError),
    /// See [`FencedLeaderEpochError`](crate::common::errors::fenced_leader_epoch_error::FencedLeaderEpochError).
    FencedLeaderEpoch(FencedLeaderEpochError),
    /// See [`FencedMemberEpochError`](crate::common::errors::fenced_member_epoch_error::FencedMemberEpochError).
    FencedMemberEpoch(FencedMemberEpochError),
    /// See [`FencedStateEpochError`](crate::common::errors::fenced_state_epoch_error::FencedStateEpochError).
    FencedStateEpoch(FencedStateEpochError),
    /// See [`FetchSessionIdNotFoundError`](crate::common::errors::fetch_session_id_not_found_error::FetchSessionIdNotFoundError).
    FetchSessionIdNotFound(FetchSessionIdNotFoundError),
    /// See [`FetchSessionTopicIdError`](crate::common::errors::fetch_session_topic_id_error::FetchSessionTopicIdError).
    FetchSessionTopicId(FetchSessionTopicIdError),
    /// See [`GroupAuthorizationError`](crate::common::errors::group_authorization_error::GroupAuthorizationError).
    GroupAuthorization(GroupAuthorizationError),
    /// See [`GroupIdNotFoundError`](crate::common::errors::group_id_not_found_error::GroupIdNotFoundError).
    GroupIdNotFound(GroupIdNotFoundError),
    /// See [`GroupMaxSizeReachedError`](crate::common::errors::group_max_size_reached_error::GroupMaxSizeReachedError).
    GroupMaxSizeReached(GroupMaxSizeReachedError),
    /// See [`GroupNotEmptyError`](crate::common::errors::group_not_empty_error::GroupNotEmptyError).
    GroupNotEmpty(GroupNotEmptyError),
    /// See [`GroupSubscribedToTopicError`](crate::common::errors::group_subscribed_to_topic_error::GroupSubscribedToTopicError).
    GroupSubscribedToTopic(GroupSubscribedToTopicError),
    /// See [`IllegalGenerationError`](crate::common::errors::illegal_generation_error::IllegalGenerationError).
    IllegalGeneration(IllegalGenerationError),
    /// See [`IllegalSaslStateError`](crate::common::errors::illegal_sasl_state_error::IllegalSaslStateError).
    IllegalSaslState(IllegalSaslStateError),
    /// See [`InconsistentClusterIdError`](crate::common::errors::inconsistent_cluster_id_error::InconsistentClusterIdError).
    InconsistentClusterId(InconsistentClusterIdError),
    /// See [`InconsistentGroupProtocolError`](crate::common::errors::inconsistent_group_protocol_error::InconsistentGroupProtocolError).
    InconsistentGroupProtocol(InconsistentGroupProtocolError),
    /// See [`InconsistentTopicIdError`](crate::common::errors::inconsistent_topic_id_error::InconsistentTopicIdError).
    InconsistentTopicId(InconsistentTopicIdError),
    /// See [`InconsistentVoterSetError`](crate::common::errors::inconsistent_voter_set_error::InconsistentVoterSetError).
    InconsistentVoterSet(InconsistentVoterSetError),
    /// See [`IneligibleReplicaError`](crate::common::errors::ineligible_replica_error::IneligibleReplicaError).
    IneligibleReplica(IneligibleReplicaError),
    /// See [`InterruptError`](crate::common::errors::interrupt_error::InterruptError).
    Interrupt(InterruptError),
    /// See [`InvalidCommitOffsetSizeError`](crate::common::errors::invalid_commit_offset_size_error::InvalidCommitOffsetSizeError).
    InvalidCommitOffsetSize(InvalidCommitOffsetSizeError),
    /// See [`InvalidConfigurationError`](crate::common::errors::invalid_configuration_error::InvalidConfigurationError).
    InvalidConfiguration(InvalidConfigurationError),
    /// See [`InvalidFetchSessionEpochError`](crate::common::errors::invalid_fetch_session_epoch_error::InvalidFetchSessionEpochError).
    InvalidFetchSessionEpoch(InvalidFetchSessionEpochError),
    /// See [`InvalidFetchSizeError`](crate::common::errors::invalid_fetch_size_error::InvalidFetchSizeError).
    InvalidFetchSize(InvalidFetchSizeError),
    /// See [`InvalidGroupIdError`](crate::common::errors::invalid_group_id_error::InvalidGroupIdError).
    InvalidGroupId(InvalidGroupIdError),
    /// See [`InvalidOffsetError`](crate::common::errors::invalid_offset_error::InvalidOffsetError).
    InvalidOffset(InvalidOffsetError),
    /// See [`InvalidPartitionsError`](crate::common::errors::invalid_partitions_error::InvalidPartitionsError).
    InvalidPartitions(InvalidPartitionsError),
    /// See [`InvalidPidMappingError`](crate::common::errors::invalid_pid_mapping_error::InvalidPidMappingError).
    InvalidPidMapping(InvalidPidMappingError),
    /// See [`InvalidPrincipalTypeError`](crate::common::errors::invalid_principal_type_error::InvalidPrincipalTypeError).
    InvalidPrincipalType(InvalidPrincipalTypeError),
    /// See [`InvalidProducerEpochError`](crate::common::errors::invalid_producer_epoch_error::InvalidProducerEpochError).
    InvalidProducerEpoch(InvalidProducerEpochError),
    /// See [`InvalidRecordError`](crate::common::invalid_record_error::InvalidRecordError).
    InvalidRecord(InvalidRecordError),
    /// See [`InvalidRecordStateError`](crate::common::errors::invalid_record_state_error::InvalidRecordStateError).
    InvalidRecordState(InvalidRecordStateError),
    /// See [`InvalidRegistrationError`](crate::common::errors::invalid_registration_error::InvalidRegistrationError).
    InvalidRegistration(InvalidRegistrationError),
    /// See [`InvalidRegularExpressionError`](crate::common::errors::invalid_regular_expression_error::InvalidRegularExpressionError).
    InvalidRegularExpression(InvalidRegularExpressionError),
    /// See [`InvalidReplicaAssignmentError`](crate::common::errors::invalid_replica_assignment_error::InvalidReplicaAssignmentError).
    InvalidReplicaAssignment(InvalidReplicaAssignmentError),
    /// See [`InvalidReplicationFactorError`](crate::common::errors::invalid_replication_factor_error::InvalidReplicationFactorError).
    InvalidReplicationFactor(InvalidReplicationFactorError),
    /// See [`InvalidRequestError`](crate::common::errors::invalid_request_error::InvalidRequestError).
    InvalidRequest(InvalidRequestError),
    /// See [`InvalidRequiredAcksError`](crate::common::errors::invalid_required_acks_error::InvalidRequiredAcksError).
    InvalidRequiredAcks(InvalidRequiredAcksError),
    /// See [`InvalidSessionTimeoutError`](crate::common::errors::invalid_session_timeout_error::InvalidSessionTimeoutError).
    InvalidSessionTimeout(InvalidSessionTimeoutError),
    /// See [`InvalidShareSessionEpochError`](crate::common::errors::invalid_share_session_epoch_error::InvalidShareSessionEpochError).
    InvalidShareSessionEpoch(InvalidShareSessionEpochError),
    /// See [`InvalidTimestampError`](crate::common::errors::invalid_timestamp_error::InvalidTimestampError).
    InvalidTimestamp(InvalidTimestampError),
    /// See [`InvalidTopicError`](crate::common::errors::invalid_topic_error::InvalidTopicError).
    InvalidTopic(InvalidTopicError),
    /// See [`InvalidTxnStateError`](crate::common::errors::invalid_txn_state_error::InvalidTxnStateError).
    InvalidTxnState(InvalidTxnStateError),
    /// See [`InvalidTxnTimeoutError`](crate::common::errors::invalid_txn_timeout_error::InvalidTxnTimeoutError).
    InvalidTxnTimeout(InvalidTxnTimeoutError),
    /// See [`InvalidUpdateVersionError`](crate::common::errors::invalid_update_version_error::InvalidUpdateVersionError).
    InvalidUpdateVersion(InvalidUpdateVersionError),
    /// See [`InvalidVoterKeyError`](crate::common::errors::invalid_voter_key_error::InvalidVoterKeyError).
    InvalidVoterKey(InvalidVoterKeyError),
    /// See [`KafkaStorageError`](crate::common::errors::kafka_storage_error::KafkaStorageError).
    KafkaStorage(KafkaStorageError),
    /// See [`LeaderNotAvailableError`](crate::common::errors::leader_not_available_error::LeaderNotAvailableError).
    LeaderNotAvailable(LeaderNotAvailableError),
    /// See [`ListenerNotFoundError`](crate::common::errors::listener_not_found_error::ListenerNotFoundError).
    ListenerNotFound(ListenerNotFoundError),
    /// See [`LogDirNotFoundError`](crate::common::errors::log_dir_not_found_error::LogDirNotFoundError).
    LogDirNotFound(LogDirNotFoundError),
    /// See [`MemberIdRequiredError`](crate::common::errors::member_id_required_error::MemberIdRequiredError).
    MemberIdRequired(MemberIdRequiredError),
    /// See [`MismatchedEndpointTypeError`](crate::common::errors::mismatched_endpoint_type_error::MismatchedEndpointTypeError).
    MismatchedEndpointType(MismatchedEndpointTypeError),
    /// See [`NetworkError`](crate::common::errors::network_error::NetworkError).
    Network(NetworkError),
    /// See [`NewLeaderElectedError`](crate::common::errors::new_leader_elected_error::NewLeaderElectedError).
    NewLeaderElected(NewLeaderElectedError),
    /// See [`NoReassignmentInProgressError`](crate::common::errors::no_reassignment_in_progress_error::NoReassignmentInProgressError).
    NoReassignmentInProgress(NoReassignmentInProgressError),
    /// See [`NotControllerError`](crate::common::errors::not_controller_error::NotControllerError).
    NotController(NotControllerError),
    /// See [`NotCoordinatorError`](crate::common::errors::not_coordinator_error::NotCoordinatorError).
    NotCoordinator(NotCoordinatorError),
    /// See [`NotEnoughReplicasError`](crate::common::errors::not_enough_replicas_error::NotEnoughReplicasError).
    NotEnoughReplicas(NotEnoughReplicasError),
    /// See [`NotEnoughReplicasAfterAppendError`](crate::common::errors::not_enough_replicas_after_append_error::NotEnoughReplicasAfterAppendError).
    NotEnoughReplicasAfterAppend(NotEnoughReplicasAfterAppendError),
    /// See [`NotLeaderOrFollowerError`](crate::common::errors::not_leader_or_follower_error::NotLeaderOrFollowerError).
    NotLeaderOrFollower(NotLeaderOrFollowerError),
    /// See [`OffsetMetadataTooLargeError`](crate::common::errors::offset_metadata_too_large_error::OffsetMetadataTooLargeError).
    OffsetMetadataTooLarge(OffsetMetadataTooLargeError),
    /// See [`OffsetMovedToTieredStorageError`](crate::common::errors::offset_moved_to_tiered_storage_error::OffsetMovedToTieredStorageError).
    OffsetMovedToTieredStorage(OffsetMovedToTieredStorageError),
    /// See [`OffsetNotAvailableError`](crate::common::errors::offset_not_available_error::OffsetNotAvailableError).
    OffsetNotAvailable(OffsetNotAvailableError),
    /// See [`OffsetOutOfRangeError`](crate::common::errors::offset_out_of_range_error::OffsetOutOfRangeError).
    OffsetOutOfRange(OffsetOutOfRangeError),
    /// See [`OperationNotAttemptedError`](crate::common::errors::operation_not_attempted_error::OperationNotAttemptedError).
    OperationNotAttempted(OperationNotAttemptedError),
    /// See [`OutOfOrderSequenceError`](crate::common::errors::out_of_order_sequence_error::OutOfOrderSequenceError).
    OutOfOrderSequence(OutOfOrderSequenceError),
    /// See [`PolicyViolationError`](crate::common::errors::policy_violation_error::PolicyViolationError).
    PolicyViolation(PolicyViolationError),
    /// See [`PositionOutOfRangeError`](crate::common::errors::position_out_of_range_error::PositionOutOfRangeError).
    PositionOutOfRange(PositionOutOfRangeError),
    /// See [`PreferredLeaderNotAvailableError`](crate::common::errors::preferred_leader_not_available_error::PreferredLeaderNotAvailableError).
    PreferredLeaderNotAvailable(PreferredLeaderNotAvailableError),
    /// See [`PrincipalDeserializationError`](crate::common::errors::principal_deserialization_error::PrincipalDeserializationError).
    PrincipalDeserialization(PrincipalDeserializationError),
    /// See [`ProducerFencedError`](crate::common::errors::producer_fenced_error::ProducerFencedError).
    ProducerFenced(ProducerFencedError),
    /// See [`ReassignmentInProgressError`](crate::common::errors::reassignment_in_progress_error::ReassignmentInProgressError).
    ReassignmentInProgress(ReassignmentInProgressError),
    /// See [`RebalanceInProgressError`](crate::common::errors::rebalance_in_progress_error::RebalanceInProgressError).
    RebalanceInProgress(RebalanceInProgressError),
    /// See [`RebootstrapRequiredError`](crate::common::errors::rebootstrap_required_error::RebootstrapRequiredError).
    RebootstrapRequired(RebootstrapRequiredError),
    /// See [`RecordBatchTooLargeError`](crate::common::errors::record_batch_too_large_error::RecordBatchTooLargeError).
    RecordBatchTooLarge(RecordBatchTooLargeError),
    /// See [`RecordDeserializationError`](crate::common::errors::record_deserialization_error::RecordDeserializationError).
    ///
    /// Boxed: it carries the full record context (partition, offsets, key/value
    /// buffers, headers), which unboxed pushes `Error` — and every
    /// `Result<_, Error>` — past clippy's 128-byte `result_large_err` threshold.
    RecordDeserialization(Box<RecordDeserializationError>),
    /// See [`RecordTooLargeError`](crate::common::errors::record_too_large_error::RecordTooLargeError).
    RecordTooLarge(RecordTooLargeError),
    /// See [`InvalidReceiveError`](crate::common::network::InvalidReceiveError).
    InvalidReceive(InvalidReceiveError),
    /// See [`ConfigError`](crate::common::config::ConfigError).
    Config(ConfigError),
    /// See [`ConsumerRetriableCommitFailedError`](crate::consumer::ConsumerRetriableCommitFailedError).
    ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError),
    /// See [`ConsumerCommitFailedError`](crate::consumer::ConsumerCommitFailedError).
    ConsumerCommitFailed(ConsumerCommitFailedError),
    /// See [`ConsumerNoOffsetForPartitionError`](crate::consumer::ConsumerNoOffsetForPartitionError).
    ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError),
    /// See [`ConsumerOffsetOutOfRangeError`](crate::consumer::ConsumerOffsetOutOfRangeError).
    ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError),
    /// See [`ConsumerLogTruncationError`](crate::consumer::ConsumerLogTruncationError).
    ///
    /// Boxed: it carries two maps, and unboxed it made `Error` — and therefore
    /// every `Result<_, Error>` in the crate — exceed 128 bytes.
    ConsumerLogTruncation(Box<ConsumerLogTruncationError>),
    /// See [`ReplicaNotAvailableError`](crate::common::errors::replica_not_available_error::ReplicaNotAvailableError).
    ReplicaNotAvailable(ReplicaNotAvailableError),
    /// See [`ResourceNotFoundError`](crate::common::errors::resource_not_found_error::ResourceNotFoundError).
    ResourceNotFound(ResourceNotFoundError),
    /// See [`SaslAuthenticationError`](crate::common::errors::sasl_authentication_error::SaslAuthenticationError).
    SaslAuthentication(SaslAuthenticationError),
    /// See [`SecurityDisabledError`](crate::common::errors::security_disabled_error::SecurityDisabledError).
    SecurityDisabled(SecurityDisabledError),
    /// See [`SerializationError`](crate::common::errors::serialization_error::SerializationError).
    Serialization(SerializationError),
    /// See [`ShareSessionLimitReachedError`](crate::common::errors::share_session_limit_reached_error::ShareSessionLimitReachedError).
    ShareSessionLimitReached(ShareSessionLimitReachedError),
    /// See [`ShareSessionNotFoundError`](crate::common::errors::share_session_not_found_error::ShareSessionNotFoundError).
    ShareSessionNotFound(ShareSessionNotFoundError),
    /// See [`SnapshotNotFoundError`](crate::common::errors::snapshot_not_found_error::SnapshotNotFoundError).
    SnapshotNotFound(SnapshotNotFoundError),
    /// See [`SslAuthenticationError`](crate::common::errors::ssl_authentication_error::SslAuthenticationError).
    SslAuthentication(SslAuthenticationError),
    /// See [`StaleBrokerEpochError`](crate::common::errors::stale_broker_epoch_error::StaleBrokerEpochError).
    StaleBrokerEpoch(StaleBrokerEpochError),
    /// See [`StaleMemberEpochError`](crate::common::errors::stale_member_epoch_error::StaleMemberEpochError).
    StaleMemberEpoch(StaleMemberEpochError),
    /// See [`StreamsInvalidTopologyError`](crate::common::errors::streams_invalid_topology_error::StreamsInvalidTopologyError).
    StreamsInvalidTopology(StreamsInvalidTopologyError),
    /// See [`StreamsInvalidTopologyEpochError`](crate::common::errors::streams_invalid_topology_epoch_error::StreamsInvalidTopologyEpochError).
    StreamsInvalidTopologyEpoch(StreamsInvalidTopologyEpochError),
    /// See [`StreamsTopologyFencedError`](crate::common::errors::streams_topology_fenced_error::StreamsTopologyFencedError).
    StreamsTopologyFenced(StreamsTopologyFencedError),
    /// See [`TelemetryTooLargeError`](crate::common::errors::telemetry_too_large_error::TelemetryTooLargeError).
    TelemetryTooLarge(TelemetryTooLargeError),
    /// See [`ThrottlingQuotaExceededError`](crate::common::errors::throttling_quota_exceeded_error::ThrottlingQuotaExceededError).
    ThrottlingQuotaExceeded(ThrottlingQuotaExceededError),
    /// See [`TimeoutError`](crate::common::errors::timeout_error::TimeoutError).
    Timeout(TimeoutError),
    /// See [`TopicAuthorizationError`](crate::common::errors::topic_authorization_error::TopicAuthorizationError).
    TopicAuthorization(TopicAuthorizationError),
    /// See [`TopicDeletionDisabledError`](crate::common::errors::topic_deletion_disabled_error::TopicDeletionDisabledError).
    TopicDeletionDisabled(TopicDeletionDisabledError),
    /// See [`TopicExistsError`](crate::common::errors::topic_exists_error::TopicExistsError).
    TopicExists(TopicExistsError),
    /// See [`TransactionAbortableError`](crate::common::errors::transaction_abortable_error::TransactionAbortableError).
    TransactionAbortable(TransactionAbortableError),
    /// See [`TransactionAbortedError`](crate::common::errors::transaction_aborted_error::TransactionAbortedError).
    TransactionAborted(TransactionAbortedError),
    /// See [`TransactionCoordinatorFencedError`](crate::common::errors::transaction_coordinator_fenced_error::TransactionCoordinatorFencedError).
    TransactionCoordinatorFenced(TransactionCoordinatorFencedError),
    /// See [`TransactionalIdAuthorizationError`](crate::common::errors::transactional_id_authorization_error::TransactionalIdAuthorizationError).
    TransactionalIdAuthorization(TransactionalIdAuthorizationError),
    /// See [`TransactionalIdNotFoundError`](crate::common::errors::transactional_id_not_found_error::TransactionalIdNotFoundError).
    TransactionalIdNotFound(TransactionalIdNotFoundError),
    /// See [`UnacceptableCredentialError`](crate::common::errors::unacceptable_credential_error::UnacceptableCredentialError).
    UnacceptableCredential(UnacceptableCredentialError),
    /// See [`UnknownControllerIdError`](crate::common::errors::unknown_controller_id_error::UnknownControllerIdError).
    UnknownControllerId(UnknownControllerIdError),
    /// See [`UnknownLeaderEpochError`](crate::common::errors::unknown_leader_epoch_error::UnknownLeaderEpochError).
    UnknownLeaderEpoch(UnknownLeaderEpochError),
    /// See [`UnknownMemberIdError`](crate::common::errors::unknown_member_id_error::UnknownMemberIdError).
    UnknownMemberId(UnknownMemberIdError),
    /// See [`UnknownProducerIdError`](crate::common::errors::unknown_producer_id_error::UnknownProducerIdError).
    UnknownProducerId(UnknownProducerIdError),
    /// See [`UnknownServerError`](crate::common::errors::unknown_server_error::UnknownServerError).
    UnknownServer(UnknownServerError),
    /// See [`UnknownSubscriptionIdError`](crate::common::errors::unknown_subscription_id_error::UnknownSubscriptionIdError).
    UnknownSubscriptionId(UnknownSubscriptionIdError),
    /// See [`UnknownTopicIdError`](crate::common::errors::unknown_topic_id_error::UnknownTopicIdError).
    UnknownTopicId(UnknownTopicIdError),
    /// See [`UnknownTopicOrPartitionError`](crate::common::errors::unknown_topic_or_partition_error::UnknownTopicOrPartitionError).
    UnknownTopicOrPartition(UnknownTopicOrPartitionError),
    /// See [`UnreleasedInstanceIdError`](crate::common::errors::unreleased_instance_id_error::UnreleasedInstanceIdError).
    UnreleasedInstanceId(UnreleasedInstanceIdError),
    /// See [`UnstableOffsetCommitError`](crate::common::errors::unstable_offset_commit_error::UnstableOffsetCommitError).
    UnstableOffsetCommit(UnstableOffsetCommitError),
    /// See [`UnsupportedAssignorError`](crate::common::errors::unsupported_assignor_error::UnsupportedAssignorError).
    UnsupportedAssignor(UnsupportedAssignorError),
    /// See [`UnsupportedByAuthenticationError`](crate::common::errors::unsupported_by_authentication_error::UnsupportedByAuthenticationError).
    UnsupportedByAuthentication(UnsupportedByAuthenticationError),
    /// See [`UnsupportedCompressionTypeError`](crate::common::errors::unsupported_compression_type_error::UnsupportedCompressionTypeError).
    UnsupportedCompressionType(UnsupportedCompressionTypeError),
    /// See [`UnsupportedEndpointTypeError`](crate::common::errors::unsupported_endpoint_type_error::UnsupportedEndpointTypeError).
    UnsupportedEndpointType(UnsupportedEndpointTypeError),
    /// See [`UnsupportedForMessageFormatError`](crate::common::errors::unsupported_for_message_format_error::UnsupportedForMessageFormatError).
    UnsupportedForMessageFormat(UnsupportedForMessageFormatError),
    /// See [`UnsupportedSaslMechanismError`](crate::common::errors::unsupported_sasl_mechanism_error::UnsupportedSaslMechanismError).
    UnsupportedSaslMechanism(UnsupportedSaslMechanismError),
    /// See [`UnsupportedVersionError`](crate::common::errors::unsupported_version_error::UnsupportedVersionError).
    UnsupportedVersion(UnsupportedVersionError),
    /// See [`VoterNotFoundError`](crate::common::errors::voter_not_found_error::VoterNotFoundError).
    VoterNotFound(VoterNotFoundError),
    /// See [`WakeupError`](crate::common::errors::wakeup_error::WakeupError).
    Wakeup(WakeupError),
}

impl Error {
    // -- Convenience constructors ------------------------------------------

    /// Create a bare Kafka error from an error code.
    pub fn new(error: Errors) -> Self {
        // Java's `Errors.exception()`: the code names a class, and that class —
        // not this enum — knows its ancestry. The fallback covers `Errors::None`,
        // which Java maps to a null exception.
        error.error().unwrap_or_else(|| Self::KafkaError(KafkaError::new(error)))
    }

    /// Create a bare Kafka error with a custom message.
    pub fn with_message(error: Errors, message: impl Into<String>) -> Self {
        // Java's `Errors.exception(String)`.
        let message = message.into();
        error
            .error_with_message(&message)
            .unwrap_or_else(|| Self::KafkaError(KafkaError::with_message(error, message)))
    }

    /// Create a topic authorization error.
    pub fn topic_authorization(topics: HashSet<String>) -> Self {
        Self::TopicAuthorization(TopicAuthorizationError::new(topics))
    }

    /// Create a topic authorization error carrying a custom message
    /// (Java: `new TopicAuthorizationException(message, unauthorizedTopics)`).
    pub fn topic_authorization_with_message(topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self::TopicAuthorization(TopicAuthorizationError::with_message(topics, message))
    }

    /// Create an invalid topic error.
    pub fn invalid_topics(topics: HashSet<String>) -> Self {
        Self::InvalidTopic(InvalidTopicError::new(topics))
    }

    /// Create an invalid topic error carrying a custom message
    /// (Java: `new InvalidTopicException(message, invalidTopics)`, and the
    /// `new InvalidTopicException(String message)` form when `topics` is empty).
    pub fn invalid_topics_with_message(topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self::InvalidTopic(InvalidTopicError::with_message(topics, message))
    }

    /// Create a group authorization error for a group ID, formatting the group
    /// into the message (Java: `GroupAuthorizationException.forGroupId(groupId)`).
    pub fn group_authorization(group_id: impl Into<String>) -> Self {
        Self::GroupAuthorization(GroupAuthorizationError::for_group_id(group_id))
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
        Self::ProducerBufferExhausted(ProducerBufferExhaustedError::new(message))
    }

    /// Create an illegal argument error.
    ///
    /// Corresponds to Java's `IllegalArgumentException`.
    pub fn illegal_argument(message: impl Into<String>) -> Self {
        Self::IllegalArgument(IllegalArgumentError::new(message))
    }

    /// Create a configuration error.
    ///
    /// Corresponds to Java's `ConfigException` — an invalid config value. Unlike
    /// [`illegal_argument`](Self::illegal_argument) this is inside the
    /// `KafkaException` hierarchy, so [`is_kafka_error`](Self::is_kafka_error) is
    /// `true` for it.
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(ConfigError::new(message))
    }

    /// Create a configuration error naming the offending value and key, in
    /// Java's `ConfigException(name, value)` format.
    pub fn config_value(name: impl std::fmt::Display, value: impl std::fmt::Display) -> Self {
        Self::Config(ConfigError::with_value(name, value))
    }

    /// Create a configuration error naming the value, key, and a detail message,
    /// in Java's `ConfigException(name, value, message)` format.
    pub fn config_value_message(
        name: impl std::fmt::Display,
        value: impl std::fmt::Display,
        message: impl std::fmt::Display,
    ) -> Self {
        Self::Config(ConfigError::with_value_message(name, value, message))
    }

    /// Create an illegal state error.
    ///
    /// Corresponds to Java's `IllegalStateException`.
    pub fn illegal_state(message: impl Into<String>) -> Self {
        Self::IllegalState(IllegalStateError::new(message))
    }

    /// Create a timeout error.
    ///
    /// Corresponds to Java's `TimeoutException`.
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout(TimeoutError::new(message))
    }

    /// Create a record too large error.
    ///
    /// Corresponds to Java's `RecordTooLargeException`.
    pub fn record_too_large(message: impl Into<String>) -> Self {
        Self::RecordTooLarge(RecordTooLargeError::new(message))
    }

    /// Create a serialization error.
    ///
    /// Corresponds to Java's `SerializationException`.
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization(SerializationError::new(message))
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
        Self::Wakeup(WakeupError::new(message))
    }

    /// Create a concurrent modification error.
    ///
    /// Corresponds to Java's `ConcurrentModificationException` thrown by
    /// `KafkaConsumer.acquire()` when the consumer is accessed from more
    /// than one thread.
    pub fn concurrent_modification(message: impl Into<String>) -> Self {
        Self::ConcurrentModification(ConcurrentModificationError::new(message))
    }

    /// Create a transaction aborted error with Java's default message.
    ///
    /// Corresponds to Java's no-arg `TransactionAbortedException()`, whose
    /// message is `"Failing batch since transaction was aborted"`.
    pub fn transaction_aborted() -> Self {
        Self::TransactionAborted(TransactionAbortedError::new("Failing batch since transaction was aborted"))
    }

    /// Create a transaction aborted error with a custom message.
    ///
    /// Corresponds to Java's `TransactionAbortedException(String)`.
    pub fn transaction_aborted_with_message(message: impl Into<String>) -> Self {
        Self::TransactionAborted(TransactionAbortedError::new(message))
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
            Self::KafkaError(e) => Some(e),
            // The only other payloads that embed one: Java gives these three
            // subclass state on top of the base, so they compose rather than
            // being declared by `kafka_error_class!`.
            Self::TopicAuthorization(e) => Some(e.kafka_error()),
            Self::InvalidTopic(e) => Some(e.kafka_error()),
            Self::GroupAuthorization(e) => Some(e.kafka_error()),
            // Every other variant is a `kafka_error_class!` declaration holding
            // only its message; it answers its code through [`ErrorCode`] and
            // its ancestry through [`ErrorHierarchy`], with no base to expose.
            // A catch-all is safe here precisely because there is nothing
            // variant-specific left to get wrong — unlike `message()`, which is
            // delegated so that a new payload must state its own answer.
            _ => None,
        }
    }

    // -- Delegating methods ------------------------------------------------

    /// The protocol error code.
    ///
    /// Returns [`Errors::UnknownServerError`] for variants without a
    /// [`KafkaError`].
    pub fn error(&self) -> Errors {
        // Delegated to the payload: Java associates a code with an exception
        // *class* (`Errors.forException`), not with an instance.
        ErrorCode::error(self)
    }

    /// The numeric error code (i16).
    pub fn code(&self) -> i16 {
        self.error().code()
    }

    /// The error message, translating Java's `Throwable.getMessage()`.
    ///
    /// Delegated to the variant's payload: a message-only error returns the text
    /// it was constructed with, and a payload wrapping a [`KafkaError`] returns
    /// its custom message if set, otherwise the default text for its error code.
    ///
    /// This is NOT what `to_string()` produces. [`fmt::Display`] translates
    /// `Throwable.toString()`, which Java renders as
    /// `getClass().getName() + ": " + getLocalizedMessage()` — so `Display`
    /// carries a class-name prefix and, for some payloads, extra subclass state.
    /// Use this method for log fields and equality assertions; use `Display`
    /// when rendering the error to a human.
    pub fn message(&self) -> &str {
        // UFCS: `self.message()` would resolve to this inherent method and recurse.
        ErrorMessage::message(self)
    }

    /// The error that caused this one, translating Java's `Throwable.getCause()`.
    ///
    /// `getCause()` lives on `Throwable`, so every error can carry a cause — this
    /// answers for all of them, delegated to the variant's payload. `None` is the
    /// common case, mirroring Java's null default.
    ///
    /// [`std::error::Error::source`] returns the same value, so the standard
    /// library's chain-walking works on any [`Error`].
    pub fn source(&self) -> Option<&Error> {
        // UFCS: `self.source()` would resolve to this inherent method and recurse.
        ErrorSource::source(self)
    }

    /// Whether the transaction must be aborted because of this error.
    ///
    /// Named for the Java class it tests, per CLAUDE.md §10.4's uniform
    /// `is_` + class + `_error` shape, even though `TransactionAbortableException`
    /// is a **leaf**: it has no subclasses, so §10.4 puts no predicate on
    /// [`ErrorHierarchy`] for it and this stays an inherent test on the variant.
    ///
    /// That is what Java does too — `TransactionManager` compares the code
    /// (`error == Errors.TRANSACTION_ABORTABLE`, four sites) or tests the class
    /// (`error.exception() instanceof TransactionAbortableException`,
    /// `TransactionManager.java:1783`). There is no `txnRequiresAbort` in Java;
    /// the previous name had no counterpart there.
    pub fn is_transaction_abortable_error(&self) -> bool {
        matches!(self, Self::TransactionAbortable(_))
    }

    // -- Hierarchy predicates ----------------------------------------------
    //
    // Java's exception hierarchy is flattened into this enum, so each
    // intermediate class becomes a predicate (CLAUDE.md §10.4). The answers
    // come from the variant's payload via `ErrorHierarchy`, which is crate
    // -internal; these forwarders are the public surface.
    //
    // Each MUST call the trait method through UFCS. `self.is_x()` inside
    // `Error::is_x` resolves back to this inherent method — inherent beats
    // trait — and recurses forever at run time while compiling cleanly.

    /// Whether this is a Kafka error rather than a generic programming error.
    ///
    /// This enum flattens two families that Java keeps apart by class
    /// hierarchy: Kafka's own `KafkaException` tree, and the generic
    /// `java.lang` / `java.util` runtime exceptions that sit beside it as
    /// siblings rather than below it (`common/KafkaException.java:22`). This
    /// predicate recovers that distinction, mirroring Java's
    /// `t instanceof KafkaException`.
    ///
    /// Returns `false` for exactly the generic variants — the ones raised by
    /// misuse of the client rather than by Kafka itself:
    /// [`IllegalArgument`](Self::IllegalArgument),
    /// [`IllegalState`](Self::IllegalState) and
    /// [`ConcurrentModification`](Self::ConcurrentModification).
    ///
    /// Everything else returns `true`: the `ApiException` subtypes,
    /// [`Serialization`](Self::Serialization), [`Wakeup`](Self::Wakeup) and the
    /// bare [`KafkaError`](Self::KafkaError) all map to `KafkaException`
    /// subclasses.
    ///
    /// **This is not a test for the [`KafkaError`](Self::KafkaError) variant.**
    /// That variant means "a bare `KafkaException`, no subclass"; this asks
    /// "inside the `KafkaException` hierarchy at all", which is true of
    /// `Timeout`, `TopicAuthorization` and most other variants too. To test the
    /// variant, match on it.
    ///
    /// Beware the polarity difference against [`is_api_error`](Self::is_api_error):
    /// both return `true` for the in-hierarchy case, but they are not the same
    /// test — `Serialization` and `Wakeup` are Kafka errors that are NOT
    /// `ApiException`s, so they return `true` here and `false` there.
    ///
    /// Two call sites depend on this:
    /// `ConsumerUtils.maybeWrapAsKafkaException(t, message)`
    /// (`ConsumerUtils.java:256`) — a Kafka error passes through unchanged, a
    /// generic one gets wrapped in a new `KafkaException(message, t)` — and
    /// `FetchCollector`, whose Java `catch (KafkaException e)` cannot catch a
    /// generic error, so those propagate where Kafka errors are swallowed.
    pub fn is_kafka_error(&self) -> bool {
        ErrorHierarchy::is_kafka_error(self)
    }

    /// Whether this error corresponds to a Java `ApiException`.
    ///
    /// In Java, `ApiException` is a subclass of `KafkaException` representing
    /// errors from the Kafka API. In `KafkaProducer.doSend()`, `ApiException`s
    /// are caught and returned via a failed future (with callback invocation),
    /// while other exceptions propagate directly.
    ///
    /// `true` for [`InvalidTopic`](Self::InvalidTopic),
    /// [`RecordTooLarge`](Self::RecordTooLarge), [`Timeout`](Self::Timeout),
    /// [`KafkaError`](Self::KafkaError) (all other `Errors`-based exceptions),
    /// [`TopicAuthorization`](Self::TopicAuthorization),
    /// [`GroupAuthorization`](Self::GroupAuthorization),
    /// [`ThrottlingQuotaExceeded`](Self::ThrottlingQuotaExceeded) and
    /// [`ProducerBufferExhausted`](Self::ProducerBufferExhausted).
    ///
    /// `false` for [`IllegalArgument`](Self::IllegalArgument) and
    /// [`IllegalState`](Self::IllegalState) (plain `RuntimeException`s),
    /// [`ConcurrentModification`](Self::ConcurrentModification), and for
    /// [`Serialization`](Self::Serialization) / [`Wakeup`](Self::Wakeup), which
    /// extend `KafkaException` directly without passing through `ApiException`.
    ///
    /// Not to be confused with [`is_kafka_error`](Self::is_kafka_error), which
    /// asks the broader question.
    pub fn is_api_error(&self) -> bool {
        ErrorHierarchy::is_api_error(self)
    }

    /// Whether this error's Java class extends `RetriableException` — i.e.
    /// whether re-sending the failed request can succeed.
    ///
    /// [`Timeout`](Self::Timeout) is retriable (Java's `TimeoutException`
    /// extends `RetriableException`), as is
    /// [`ThrottlingQuotaExceeded`](Self::ThrottlingQuotaExceeded). For
    /// [`KafkaError`](Self::KafkaError) the answer comes from the error code
    /// via [`Errors::is_retriable_error`], which is `true` for exactly the codes
    /// whose Java class extends `RetriableException` — including through
    /// `RefreshRetriableException` and `InvalidMetadataException`. That
    /// equivalence is enforced by `errors.rs`'s
    /// `test_retriable_errors_match_java_hierarchy`.
    ///
    /// [`IllegalArgument`](Self::IllegalArgument) and
    /// [`IllegalState`](Self::IllegalState) are never retriable.
    pub fn is_retriable_error(&self) -> bool {
        ErrorHierarchy::is_retriable_error(self)
    }

    /// Whether this error's Java class extends `RefreshRetriableException`
    /// (CLAUDE.md §10.4) — retriable, and a metadata / coordinator refresh is
    /// what clears it.
    ///
    /// Only [`KafkaError`](Self::KafkaError) can answer `true`, since the
    /// property belongs to the error code; see
    /// [`Errors::is_refresh_retriable_error`] for the 15 codes. Variants
    /// carrying no protocol code answer `false`.
    pub fn is_refresh_retriable_error(&self) -> bool {
        ErrorHierarchy::is_refresh_retriable_error(self)
    }

    /// Whether this error's Java class extends `TimeoutException`
    /// (CLAUDE.md §10.4).
    ///
    /// Wider than the [`Timeout`](Self::Timeout) variant:
    /// `BufferExhaustedException extends TimeoutException`, so
    /// [`ProducerBufferExhausted`](Self::ProducerBufferExhausted) answers `true`
    /// here too — matching Java, where `catch (TimeoutException e)` catches a
    /// buffer-pool exhaustion. Match the variant if you mean only the timeout
    /// itself.
    ///
    /// Nested inside [`is_retriable_error`](Self::is_retriable_error).
    pub fn is_timeout_error(&self) -> bool {
        ErrorHierarchy::is_timeout_error(self)
    }

    /// Whether this error's Java class extends `InvalidMetadataException`
    /// (CLAUDE.md §10.4) — the client's cached metadata may be stale.
    ///
    /// Nested inside [`is_refresh_retriable_error`](Self::is_refresh_retriable_error),
    /// which is nested inside [`is_retriable_error`](Self::is_retriable_error).
    /// See [`Errors::is_invalid_metadata_error`] for the 13 codes.
    pub fn is_invalid_metadata_error(&self) -> bool {
        ErrorHierarchy::is_invalid_metadata_error(self)
    }

    /// Whether this error's Java class extends `InvalidConfigurationException`
    /// (CLAUDE.md §10.4).
    ///
    /// Wider than its name suggests: in Kafka 4.2 **both**
    /// `AuthenticationException` and `AuthorizationException` extend
    /// `InvalidConfigurationException` rather than `ApiException` directly, so
    /// every error answering `true` to
    /// [`is_authentication_error`](Self::is_authentication_error) or
    /// [`is_authorization_error`](Self::is_authorization_error) answers `true`
    /// here too. The remaining members are the configuration errors proper —
    /// [`Errors::InvalidConfig`], [`Errors::InvalidReplicationFactor`],
    /// [`Errors::InvalidRequiredAcks`], [`Errors::InvalidTopicError`],
    /// [`Errors::RecordListTooLarge`], [`Errors::UnsupportedForMessageFormat`]
    /// and [`Errors::UnsupportedVersion`].
    pub fn is_invalid_configuration_error(&self) -> bool {
        ErrorHierarchy::is_invalid_configuration_error(self)
    }

    /// Whether this error's Java class extends `ApplicationRecoverableException`
    /// (CLAUDE.md §10.4) — the application can recover, but only by
    /// re-initialising its producer or rejoining its group; the current epoch or
    /// session is gone.
    ///
    /// 6 codes, covering the transaction and group-membership fencing paths:
    /// [`Errors::FencedInstanceId`], [`Errors::IllegalGeneration`],
    /// [`Errors::InvalidProducerEpoch`], [`Errors::InvalidProducerIdMapping`],
    /// [`Errors::ProducerFenced`] and [`Errors::UnknownMemberId`].
    pub fn is_application_recoverable_error(&self) -> bool {
        ErrorHierarchy::is_application_recoverable_error(self)
    }

    /// Whether this error's Java class extends `InvalidOffsetException`
    /// (CLAUDE.md §10.4).
    ///
    /// [`Errors::OffsetOutOfRange`] is the only member carrying a protocol code;
    /// the sibling `NoOffsetForPartitionException` is raised client-side.
    pub fn is_invalid_offset_error(&self) -> bool {
        ErrorHierarchy::is_invalid_offset_error(self)
    }

    /// Whether this error's Java class extends `OutOfOrderSequenceException`
    /// (CLAUDE.md §10.4).
    ///
    /// Two codes: [`Errors::OutOfOrderSequenceNumber`] itself and
    /// [`Errors::UnknownProducerId`], its only subclass.
    pub fn is_out_of_order_sequence_error(&self) -> bool {
        ErrorHierarchy::is_out_of_order_sequence_error(self)
    }

    /// Whether this error's Java class extends
    /// `org.apache.kafka.clients.consumer.InvalidOffsetException`
    /// (CLAUDE.md §10.4) — no offset is usable for the partition.
    ///
    /// Covers [`NoOffsetForPartition`](Self::NoOffsetForPartition),
    /// [`ConsumerOffsetOutOfRange`](Self::ConsumerOffsetOutOfRange) and
    /// [`LogTruncation`](Self::LogTruncation).
    ///
    /// Distinct from [`is_invalid_offset_error`](Self::is_invalid_offset_error),
    /// which tests `common.errors.InvalidOffsetException` — a different Java
    /// class with a different parent (`ApiException` rather than
    /// `KafkaException`). The wire code `OFFSET_OUT_OF_RANGE` produces that one;
    /// `KafkaConsumer::poll` raises these.
    pub fn is_consumer_invalid_offset_error(&self) -> bool {
        ErrorHierarchy::is_consumer_invalid_offset_error(self)
    }

    /// Whether this error's Java class extends
    /// `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`
    /// (CLAUDE.md §10.4).
    ///
    /// Wider than the [`ConsumerOffsetOutOfRange`](Self::ConsumerOffsetOutOfRange)
    /// variant: `LogTruncationException` extends that class, so
    /// [`ConsumerLogTruncation`](Self::ConsumerLogTruncation) answers `true` too
    /// — matching Java, where `catch (OffsetOutOfRangeException e)` catches a
    /// log truncation.
    ///
    /// Nested inside
    /// [`is_consumer_invalid_offset_error`](Self::is_consumer_invalid_offset_error).
    /// Distinct from [`is_invalid_offset_error`](Self::is_invalid_offset_error),
    /// which tests the `common.errors` class of the same name.
    pub fn is_consumer_offset_out_of_range_error(&self) -> bool {
        ErrorHierarchy::is_consumer_offset_out_of_range_error(self)
    }

    /// Whether this error's Java class extends `SerializationException`
    /// (CLAUDE.md §10.4).
    ///
    /// No protocol code maps here — `SerializationException` and its subclass
    /// `RecordDeserializationException` are raised client-side — so only
    /// [`Serialization`](Self::Serialization) answers `true`. Note it is a
    /// `KafkaException` but NOT an `ApiException`, so
    /// [`is_api_error`](Self::is_api_error) is `false` for it.
    pub fn is_serialization_error(&self) -> bool {
        ErrorHierarchy::is_serialization_error(self)
    }

    /// Whether this error's Java class extends `AuthenticationException`
    /// (CLAUDE.md §10.4).
    ///
    /// Covers only the broker-reported codes. A handshake failure detected
    /// locally is carried as an `AuthenticationError` payload inside an
    /// `io::Error` and never reaches this enum, so it answers `false`.
    ///
    /// Nested inside
    /// [`is_invalid_configuration_error`](Self::is_invalid_configuration_error).
    pub fn is_authentication_error(&self) -> bool {
        ErrorHierarchy::is_authentication_error(self)
    }

    /// Whether this error's Java class extends `AuthorizationException`
    /// (CLAUDE.md §10.4).
    ///
    /// `true` for the [`TopicAuthorization`](Self::TopicAuthorization) and
    /// [`GroupAuthorization`](Self::GroupAuthorization) variants, and for a
    /// [`KafkaError`](Self::KafkaError) carrying any of the five
    /// `*AuthorizationFailed` codes.
    pub fn is_authorization_error(&self) -> bool {
        ErrorHierarchy::is_authorization_error(self)
    }
}

// `impl Display for Error` is generated by `#[delegate(Display)]` on the enum:
// each variant forwards to its payload's own `Display`, which renders the way
// that payload's Java class implements `toString()`.

impl std::error::Error for Error {
    /// The standard-library view of Java's `Throwable.getCause()`, so
    /// `{:#}`-style reporters and `anyhow`-style chains walk the cause chain.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // UFCS: `self.source()` would resolve to the inherent method below.
        ErrorSource::source(self).map(|e| e as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java's `getCause()` is declared on `Throwable`, so every error answers it;
    /// the Rust-side name follows the std trait (`source`). `None` is the common
    /// case (Java's null default); a source set through a constructor is readable
    /// back, and `std::error::Error::source()` agrees with the inherent accessor.
    ///
    /// The inherent `source()` shadows both `ErrorSource::source` and
    /// `std::error::Error::source`, so `e.source()` is unambiguous and returns the
    /// **typed** `Option<&Error>`; the `dyn` view is reachable through the trait
    /// and downcasts back to `Error`.
    #[test]
    fn source_is_universal_and_readable() {
        use std::error::Error as StdError;

        // Default: no cause, on both a macro-declared class and the base.
        assert!(Error::new(Errors::RequestTimedOut).source().is_none());
        assert!(Error::illegal_state("misuse").source().is_none());
        assert!(StdError::source(&Error::new(Errors::RequestTimedOut)).is_none());

        // Set through `KafkaError`'s Java-shaped `(String, Throwable)` constructor.
        let root = Error::new(Errors::ClusterAuthorizationFailed);
        let wrapped = Error::KafkaError(KafkaError::with_message_and_source(
            Errors::UnknownServerError,
            "Cannot execute transactional method because we are in an error state",
            root,
        ));
        let source = wrapped.source().expect("the source is retained");
        assert_eq!(source.error(), Errors::ClusterAuthorizationFailed);
        // The std view agrees, so `source()` chains resolve.
        assert!(StdError::source(&wrapped).is_some());
        // The wrapper keeps its own code and message.
        assert_eq!(wrapped.error(), Errors::UnknownServerError);

        // A macro-declared class carries one too (every class has the slot).
        let serialization = Error::Serialization(SerializationError::with_source(
            "bad bytes",
            Error::illegal_argument("not utf-8"),
        ));
        assert_eq!(serialization.source().expect("retained").message(), "not utf-8");

        // The chain is walkable to arbitrary depth.
        let outer = Error::KafkaError(KafkaError::with_source(Errors::UnknownServerError, serialization));
        let mid = outer.source().expect("first link");
        assert_eq!(mid.message(), "bad bytes");
        assert_eq!(mid.source().expect("second link").message(), "not utf-8");

        // The inherent accessor is typed; the std trait's view is `dyn` but
        // downcasts straight back to the enum, so nothing is lost either way.
        let typed: Option<&Error> = outer.source();
        let via_dyn: Option<&Error> = StdError::source(&outer).and_then(|e| e.downcast_ref::<Error>());
        assert_eq!(typed.map(Error::message), via_dyn.map(Error::message));
        assert!(
            matches!(via_dyn, Some(Error::Serialization(_))),
            "the concrete variant survives the round-trip"
        );

        // A `&dyn Error` chain walks the same two levels.
        let mut depth = 0;
        let mut cursor: Option<&(dyn StdError + 'static)> = StdError::source(&outer);
        while let Some(e) = cursor {
            depth += 1;
            cursor = e.source();
        }
        assert_eq!(depth, 2, "walking the dyn chain sees both links");

        // Classes whose Java counterpart has no `Throwable cause` constructor
        // answer `None` — fidelity, not omission.
        assert!(
            Error::ConsumerCommitFailed(crate::consumer::ConsumerCommitFailedError::with_default_message())
                .source()
                .is_none()
        );
    }

    /// `RetriableCommitFailedException(Throwable)` keeps its cause in Java; the
    /// Rust translation used to accept and discard it.
    #[test]
    fn retriable_commit_failed_retains_its_source() {
        let e = Error::ConsumerRetriableCommitFailed(crate::consumer::ConsumerRetriableCommitFailedError::with_source(
            Error::timeout("commit timed out"),
        ));
        assert_eq!(e.source().expect("retained").message(), "commit timed out");
    }

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
        assert_eq!(cme.is_retriable_error(), ise.is_retriable_error());
        assert!(!cme.is_retriable_error());
        assert_eq!(
            crate::common::requests::request_utils::is_fatal_error(&cme),
            crate::common::requests::request_utils::is_fatal_error(&ise)
        );
        assert!(!crate::common::requests::request_utils::is_fatal_error(&cme));
        assert_eq!(cme.is_api_error(), ise.is_api_error());
        assert!(!cme.is_api_error());
        assert_eq!(cme.is_kafka_error(), ise.is_kafka_error());
        assert!(!cme.is_kafka_error());
        assert!(cme.kafka_error().is_none());
    }

    #[test]
    fn concurrent_modification_display() {
        let cme = Error::concurrent_modification("oops");
        assert_eq!(cme.to_string(), "ConcurrentModificationError: oops");
    }

    /// Every [`Error`] variant against its Java `extends` chain, in both
    /// directions.
    ///
    /// The predicates are no longer computed by matching on the enum — each is
    /// delegated to the variant's payload, whose `impl ErrorHierarchy` declares
    /// its ancestry. This table is what stops a payload from silently claiming
    /// (or dropping) a superclass, which a per-variant `assert!` could not.
    ///
    /// Order: kafka, api, retriable, refresh_retriable, invalid_metadata,
    /// authentication, authorization, fatal.
    #[test]
    fn variant_predicates_match_java_hierarchy() {
        use std::collections::HashMap;
        let cases: &[(&str, Error, [bool; 16])] = &[
            // TopicAuthorizationException -> AuthorizationException -> ApiException -> KafkaException
            (
                "TopicAuthorization",
                Error::topic_authorization(HashSet::new()),
                [
                    true, true, false, false, false, false, true, false, false, false, false, false, false, false,
                    true, true,
                ],
            ),
            // GroupAuthorizationException -> AuthorizationException -> ApiException -> KafkaException
            (
                "GroupAuthorization",
                Error::group_authorization("g"),
                [
                    true, true, false, false, false, false, true, false, false, false, false, false, false, false,
                    true, true,
                ],
            ),
            // InvalidTopicException -> ApiException -> KafkaException
            (
                "InvalidTopic",
                Error::invalid_topics(HashSet::new()),
                [
                    true, true, false, false, false, false, true, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // ThrottlingQuotaExceededException -> RetriableException -> ApiException -> KafkaException
            (
                "ThrottlingQuotaExceeded",
                Error::throttling_quota_exceeded(1, "throttled"),
                [
                    true, true, true, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // BufferExhaustedException -> TimeoutException -> RetriableException
            // -> ApiException -> KafkaException. It extends TimeoutException, so
            // it is retriable — and it reports REQUEST_TIMED_OUT, because Java's
            // `Errors.forException` walks up the superclass chain to find a code.
            (
                "BufferExhausted",
                Error::buffer_exhausted("full"),
                [
                    true, true, true, false, true, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // TimeoutException -> RetriableException -> ApiException -> KafkaException
            (
                "Timeout",
                Error::timeout("late"),
                [
                    true, true, true, false, true, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // RecordTooLargeException -> ApiException -> KafkaException
            (
                "RecordTooLarge",
                Error::record_too_large("big"),
                [
                    true, true, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // SerializationException -> KafkaException (NOT an ApiException)
            (
                "Serialization",
                Error::serialization("bad"),
                [
                    true, false, false, false, false, false, false, false, false, false, false, false, true, false,
                    false, false,
                ],
            ),
            // WakeupException -> KafkaException (NOT an ApiException)
            (
                "Wakeup",
                Error::wakeup("woken"),
                [
                    true, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // java.lang / java.util runtime exceptions: outside the hierarchy entirely.
            (
                "IllegalArgument",
                Error::illegal_argument("bad arg"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            (
                "IllegalState",
                Error::illegal_state("bad state"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            (
                "ConcurrentModification",
                Error::concurrent_modification("racy"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // Consumer-package classes: `LogTruncationException extends
            // OffsetOutOfRangeException extends InvalidOffsetException`, so a
            // log truncation answers `true` to both consumer predicates — Java's
            // `catch (OffsetOutOfRangeException e)` catches it.
            (
                "ConsumerOffsetOutOfRange",
                Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(HashMap::new())),
                [
                    true, false, false, false, false, false, false, false, false, true, true, false, false, false,
                    false, false,
                ],
            ),
            (
                "ConsumerLogTruncation",
                Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(HashMap::new(), HashMap::new()))),
                [
                    true, false, false, false, false, false, false, false, false, true, true, false, false, false,
                    false, false,
                ],
            ),
            // `CommitFailedException extends KafkaException` — not an ApiException.
            (
                "ConsumerCommitFailed",
                Error::ConsumerCommitFailed(ConsumerCommitFailedError::new("m")),
                [
                    true, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // A bare KafkaException answers from its error CODE, not its type.
            // NOT_LEADER_OR_FOLLOWER: InvalidMetadataException -> RefreshRetriableException
            // -> RetriableException -> ApiException -> KafkaException.
            (
                "KafkaError(NotLeaderOrFollower)",
                Error::new(Errors::NotLeaderOrFollower),
                [
                    true, true, true, true, false, true, false, false, false, false, false, false, false, false, false,
                    false,
                ],
            ),
            // SASL_AUTHENTICATION_FAILED: AuthenticationException -> ApiException, and fatal.
            (
                "KafkaError(SaslAuthenticationFailed)",
                Error::new(Errors::SaslAuthenticationFailed),
                [
                    true, true, false, false, false, false, true, false, false, false, false, false, false, true,
                    false, true,
                ],
            ),
        ];

        for (name, err, expected) in cases {
            let actual = [
                err.is_kafka_error(),
                err.is_api_error(),
                err.is_retriable_error(),
                err.is_refresh_retriable_error(),
                err.is_timeout_error(),
                err.is_invalid_metadata_error(),
                err.is_invalid_configuration_error(),
                err.is_application_recoverable_error(),
                err.is_invalid_offset_error(),
                err.is_consumer_invalid_offset_error(),
                err.is_consumer_offset_out_of_range_error(),
                err.is_out_of_order_sequence_error(),
                err.is_serialization_error(),
                err.is_authentication_error(),
                err.is_authorization_error(),
                crate::common::requests::request_utils::is_fatal_error(err),
            ];
            assert_eq!(
                &actual, expected,
                "{name}: predicate vector diverges from the Java extends chain \
                 (order: kafka, api, retriable, refresh_retriable, timeout, invalid_metadata, invalid_configuration, application_recoverable, invalid_offset, consumer_invalid_offset, consumer_offset_out_of_range, out_of_order_sequence, serialization, authentication, authorization, fatal)"
            );
        }
    }

    /// `message()` and `Display` are two different Java methods and must stay
    /// that way: `Throwable.getMessage()` returns the bare text, while
    /// `Throwable.toString()` is `getClass().getName() + ": " + message`.
    ///
    /// Both are delegated to the payload now, so this pins each variant's pair
    /// and, for the message-only errors, that `Display` is exactly
    /// `"<TypeName>: <message>"`.
    #[test]
    fn message_is_the_bare_text_and_display_adds_the_class_name() {
        let message_only: &[(&str, Error)] = &[
            ("IllegalArgumentError", Error::illegal_argument("boom")),
            ("IllegalStateError", Error::illegal_state("boom")),
            ("ConcurrentModificationError", Error::concurrent_modification("boom")),
            ("TimeoutError", Error::timeout("boom")),
            ("RecordTooLargeError", Error::record_too_large("boom")),
            ("SerializationError", Error::serialization("boom")),
            ("WakeupError", Error::wakeup("boom")),
        ];
        for (type_name, err) in message_only {
            assert_eq!(err.message(), "boom", "{type_name}: message() must be the bare text");
            assert_eq!(
                err.to_string(),
                format!("{type_name}: boom"),
                "{type_name}: Display must prefix the class name"
            );
        }

        // Payloads wrapping a `KafkaError` report its message — the custom one
        // when set, otherwise the error code's default text. The old `message()`
        // reached these through a `_ =>` arm whose "Unknown error" fallback was
        // unreachable; delegation makes each payload answer for itself.
        assert_eq!(
            Error::new(Errors::RequestTimedOut).message(),
            Errors::RequestTimedOut.message(),
            "bare KafkaError falls back to the code's default text"
        );
        assert_eq!(
            Error::with_message(Errors::RequestTimedOut, "custom").message(),
            "custom",
            "a custom message wins over the code's default"
        );
        assert_eq!(Error::buffer_exhausted("pool full").message(), "pool full");
        assert_eq!(
            Error::group_authorization_with_message("g1", "not authorized").message(),
            "not authorized"
        );
        assert_eq!(Error::throttling_quota_exceeded(42, "throttled").message(), "throttled");

        // The three stateful auth/topic classes format their state into the
        // message, exactly as Java's single-argument constructors /
        // `forGroupId` do — so it appears in BOTH message() and Display.
        // (Java: `TopicAuthorizationException(Set)` ->
        // `"Not authorized to access topics: " + set`.)
        let topic_auth = Error::topic_authorization(HashSet::from(["t1".to_string()]));
        assert_eq!(topic_auth.message(), "Not authorized to access topics: [t1]");
        assert_eq!(
            topic_auth.to_string(),
            "TopicAuthorizationError: Not authorized to access topics: [t1]"
        );

        let group_auth = Error::group_authorization("g1");
        assert_eq!(group_auth.message(), "Not authorized to access group: g1");
        assert_eq!(
            group_auth.to_string(),
            "GroupAuthorizationError: Not authorized to access group: g1"
        );

        let invalid_topics = Error::invalid_topics(HashSet::from(["t1".to_string()]));
        assert_eq!(invalid_topics.message(), "Invalid topics: [t1]");
        assert_eq!(invalid_topics.to_string(), "InvalidTopicError: Invalid topics: [t1]");

        // The `Errors` registry (`exception()` path) still yields the code's
        // default message with empty state, matching Java's `::new` builder.
        assert_eq!(
            Errors::TopicAuthorizationFailed.error().unwrap().message(),
            Errors::TopicAuthorizationFailed.message()
        );
        assert_eq!(
            Errors::GroupAuthorizationFailed.error().unwrap().message(),
            Errors::GroupAuthorizationFailed.message()
        );
        assert_eq!(
            Errors::InvalidTopicError.error().unwrap().message(),
            Errors::InvalidTopicError.message()
        );
    }

    /// The classes translated into `common::errors` answer their own code and
    /// ancestry, without an embedded [`KafkaError`].
    ///
    /// `TimeoutError` moving into that module is a deliberate behaviour change:
    /// it used to carry no code, so `code()` was `-1`. Java associates a code
    /// with an exception *class* (`Errors.forException`, `CLASS_TO_ERROR`), and
    /// `REQUEST_TIMED_OUT` maps to `TimeoutException` — including for
    /// client-side timeouts, which is why the code is unconditional.
    #[test]
    fn translated_classes_answer_their_own_code() {
        use crate::common::errors::{NetworkError, NotCoordinatorError, TimeoutError};

        let timeout = Error::timeout("late");
        assert_eq!(timeout.error(), Errors::RequestTimedOut);
        assert_eq!(timeout.code(), 7, "TimeoutException is Errors.REQUEST_TIMED_OUT");
        assert_eq!(timeout.message(), "late");
        assert!(timeout.is_retriable_error());

        // `with_default_message()` is Java's `Errors.exception()`: the literal
        // stays on the `Errors` constant and is handed to the class.
        let defaulted = TimeoutError::with_default_message();
        assert_eq!(defaulted.message(), Errors::RequestTimedOut.message());

        // NetworkException extends InvalidMetadataException extends
        // RefreshRetriableException extends RetriableException.
        let net = NetworkError::new("disconnected");
        assert_eq!(ErrorCode::error(&net), Errors::NetworkError);
        assert!(ErrorHierarchy::is_invalid_metadata_error(&net));
        assert!(ErrorHierarchy::is_refresh_retriable_error(&net));
        assert!(ErrorHierarchy::is_retriable_error(&net));
        assert_eq!(net.to_string(), "NetworkError: disconnected");

        // NotCoordinatorException is RefreshRetriable but NOT InvalidMetadata.
        let nc = NotCoordinatorError::new("wrong coordinator");
        assert!(ErrorHierarchy::is_refresh_retriable_error(&nc));
        assert!(!ErrorHierarchy::is_invalid_metadata_error(&nc));

        // ThrottlingQuotaExceededError keeps its extra field through the move.
        let throttled = Error::throttling_quota_exceeded(250, "slow down");
        assert_eq!(throttled.throttle_time_ms(), Some(250));
        assert_eq!(throttled.error(), Errors::ThrottlingQuotaExceeded);
        assert!(throttled.is_retriable_error());
    }

    /// `Error::KafkaError` now means a *bare* `KafkaException` — every code maps
    /// to its own class through [`Errors::error`], so the variant is only reached
    /// for [`Errors::None`] or by constructing it directly.
    ///
    /// It is therefore NOT an `ApiException`: `KafkaException` is that class's
    /// parent, not an instance of it. While the variant stood in for every coded
    /// error it had to answer `true`, which is the assumption this pins closed.
    #[test]
    fn bare_kafka_exception_is_not_an_api_exception() {
        let bare = Error::KafkaError(KafkaError::new(Errors::None));
        assert!(bare.is_kafka_error());
        assert!(!bare.is_api_error(), "KafkaException is ApiException's parent, not an instance");
        assert!(!bare.is_retriable_error());
        assert!(!crate::common::requests::request_utils::is_fatal_error(&bare));

        // A code no longer produces this variant at all.
        assert!(matches!(Error::new(Errors::NetworkError), Error::Network(_)));
        assert!(matches!(Error::new(Errors::None), Error::KafkaError(_)));
    }

    /// `is_transaction_abortable_error` tests one leaf class, so it is a variant
    /// match rather than an [`ErrorHierarchy`] predicate.
    ///
    /// Note the confusable neighbour: `TransactionAbortableException` (code 120,
    /// "you may abort and carry on") is a different Java class from
    /// `TransactionAbortedException` (no code, "the transaction was aborted, the
    /// record was not written").
    #[test]
    fn transaction_abortable_is_a_leaf_variant_test() {
        let abortable = Error::new(Errors::TransactionAbortable);
        assert!(abortable.is_transaction_abortable_error());
        assert_eq!(abortable.code(), 120);

        // Different class, despite the near-identical name.
        let aborted = Error::TransactionAborted(TransactionAbortedError::new("aborted"));
        assert!(!aborted.is_transaction_abortable_error());

        for other in [
            Error::timeout("x"),
            Error::new(Errors::NetworkError),
            Error::illegal_state("x"),
        ] {
            assert!(!other.is_transaction_abortable_error(), "{other:?}");
        }
    }

    /// A `KafkaException` subclass is always a `KafkaException`, and anything
    /// retriable through a refresh is retriable. Guards against a payload
    /// overriding a narrow predicate while forgetting its ancestors.
    #[test]
    fn predicate_nesting_holds_for_every_variant() {
        let errors = [
            Error::topic_authorization(HashSet::new()),
            Error::group_authorization("g"),
            Error::invalid_topics(HashSet::new()),
            Error::throttling_quota_exceeded(1, "throttled"),
            Error::buffer_exhausted("full"),
            Error::timeout("late"),
            Error::record_too_large("big"),
            Error::serialization("bad"),
            Error::wakeup("woken"),
            Error::illegal_argument("bad arg"),
            Error::illegal_state("bad state"),
            Error::concurrent_modification("racy"),
            Error::new(Errors::NotLeaderOrFollower),
            Error::new(Errors::SaslAuthenticationFailed),
            Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(std::collections::HashMap::new())),
            Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(
                std::collections::HashMap::new(),
                std::collections::HashMap::new(),
            ))),
            Error::buffer_exhausted("pool full"),
        ];

        for err in &errors {
            if err.is_api_error() {
                assert!(err.is_kafka_error(), "{err}: ApiException must be a KafkaException");
            }
            if err.is_retriable_error() {
                assert!(err.is_api_error(), "{err}: RetriableException must be an ApiException");
            }
            if err.is_refresh_retriable_error() {
                assert!(err.is_retriable_error(), "{err}: RefreshRetriable must be Retriable");
            }
            if err.is_invalid_metadata_error() {
                assert!(
                    err.is_refresh_retriable_error(),
                    "{err}: InvalidMetadata must be RefreshRetriable"
                );
            }
            if err.is_authentication_error() || err.is_authorization_error() {
                // Kafka 4.2: both extend InvalidConfigurationException, NOT
                // ApiException directly.
                assert!(
                    err.is_invalid_configuration_error(),
                    "{err}: Authentication/AuthorizationException extend InvalidConfigurationException"
                );
                assert!(
                    crate::common::requests::request_utils::is_fatal_error(err),
                    "{err}: auth/authz errors are fatal"
                );
                assert!(!err.is_retriable_error(), "{err}: auth/authz errors are not retriable");
            }
            if err.is_invalid_configuration_error()
                || err.is_application_recoverable_error()
                || err.is_invalid_offset_error()
                || err.is_out_of_order_sequence_error()
            {
                assert!(
                    err.is_api_error(),
                    "{err}: InvalidConfiguration / ApplicationRecoverable / InvalidOffset / \
                     OutOfOrderSequence all extend ApiException"
                );
            }
            if err.is_timeout_error() {
                // TimeoutException extends RetriableException.
                assert!(err.is_retriable_error(), "{err}: TimeoutException is a RetriableException");
            }
            if err.is_consumer_offset_out_of_range_error() {
                // consumer OffsetOutOfRangeException extends consumer
                // InvalidOffsetException.
                assert!(
                    err.is_consumer_invalid_offset_error(),
                    "{err}: consumer OffsetOutOfRangeException is an InvalidOffsetException"
                );
            }
            if err.is_consumer_invalid_offset_error() {
                // The consumer's InvalidOffsetException extends KafkaException,
                // NOT ApiException — unlike the common.errors class of the same
                // name.
                assert!(
                    err.is_kafka_error(),
                    "{err}: consumer InvalidOffsetException is a KafkaException"
                );
                assert!(
                    !err.is_api_error(),
                    "{err}: the consumer's InvalidOffsetException is NOT an ApiException"
                );
            }
            if err.is_serialization_error() {
                // SerializationException extends KafkaException directly.
                assert!(err.is_kafka_error(), "{err}: SerializationException is a KafkaException");
                assert!(!err.is_api_error(), "{err}: SerializationException is NOT an ApiException");
            }
        }
    }
}
