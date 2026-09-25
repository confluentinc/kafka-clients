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

//! The crate's flat base error type and the machinery behind it.
//!
//! [`Error`] is the single type every fallible API returns. It has no Java
//! counterpart: Rust cannot express Java's exception hierarchy, so one enum
//! holds both `KafkaException`'s subclasses and the generic `java.lang` /
//! `java.util` runtime exceptions that sit beside it (CLAUDE.md §10.3).
//!
//! Flattening the hierarchy destroys the `extends` chain, so the traits and
//! macros that reconstruct it live here too, next to the enum they serve:
//!
//!  - [`ErrorHierarchy`] recovers each intermediate Java class as a predicate;
//!    [`ErrorCode`], [`ErrorName`], [`ErrorMessage`] and [`ErrorSource`] are the
//!    per-payload answers [`Error`] delegates to.
//!  - [`kafka_error_class`] and [`message_only_error`] declare the two shapes of
//!    error class that carry nothing beyond a message, one per Java class.
//!
//! Java's `KafkaException` base class is a *different* type: the `KafkaError`
//! struct, which every specific error struct embeds and which is re-exported
//! alongside this enum as [`crate::common::KafkaError`].
//! [`Error::KafkaError`](Error::KafkaError) is therefore the variant for a
//! *bare* `KafkaException` — one with no subclass-specific fields. It is NOT
//! "the variant for Kafka errors"; [`Error::Timeout`](Error::Timeout) and the
//! rest are Kafka errors too. See [`Error::is_kafka_error`] for that test.

use std::collections::HashSet;
// Imported unqualified so that `#[delegate(Display)]` on `Error` resolves to
// the std trait; the `#[delegatable_trait_remote]` stub below only registers
// the signature with ambassador, it does not define a trait.
use std::fmt::Display;

use ambassador::{Delegate, delegatable_trait, delegatable_trait_remote};

use super::Errors;
use crate::common::InvalidRecordError;
use crate::common::KafkaError;
use crate::common::LocalConcurrentModificationError;
use crate::common::LocalIllegalArgumentError;
use crate::common::LocalIllegalStateError;
use crate::common::LocalTimeoutError;
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
use crate::common::metrics::QuotaViolationError;
use crate::common::network::InvalidReceiveError;
use crate::common::protocol::types::SchemaError;
use crate::common::requests::CorrelationIdMismatchError;
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
    /// subclasses (`common/KafkaException.java:22`): [`LocalIllegalArgumentError`],
    /// [`LocalIllegalStateError`], [`LocalConcurrentModificationError`].
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
    /// Overridden by the five payloads whose `extends:` list names it:
    /// [`AuthenticationError`] itself — the concrete base class, which has no
    /// `Errors` entry of its own but reports [`Errors::InvalidConfig`] through
    /// `forException`'s superclass walk — plus [`SaslAuthenticationError`],
    /// [`SslAuthenticationError`], [`IllegalSaslStateError`] and
    /// [`UnsupportedSaslMechanismError`]. Only three of the five have an entry
    /// in `Errors`, so this is NOT "the broker-reported codes": a handshake
    /// failure detected locally is carried as an [`AuthenticationError`] payload
    /// inside an `io::Error` (`common::network::auth_io_error`) and the
    /// producer's `Sender` converts it back into [`Error::Authentication`],
    /// which answers `true` here.
    fn is_authentication_error(&self) -> bool {
        false
    }

    /// Whether this error's Java class extends `AuthorizationException`.
    ///
    /// Overridden by the six payloads whose `extends:` list names it:
    /// [`AuthorizationError`] itself — the concrete base class, which reports
    /// [`Errors::InvalidConfig`] by inheritance and carries no
    /// protocol code — plus [`TopicAuthorizationError`],
    /// [`GroupAuthorizationError`], [`ClusterAuthorizationError`],
    /// [`TransactionalIdAuthorizationError`] and
    /// [`DelegationTokenAuthorizationError`].
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

/// The name of the error's own class, translating Java's
/// `getClass().getName()`.
///
/// **Mechanism, not API**, like [`ErrorMessage`]: it exists so
/// [`Errors::error_name`] can answer without matching on 135 codes a second
/// time. Callers use [`Errors::error_name`], which translates
/// `Errors.exceptionName()` (`protocol/Errors.java:470`).
///
/// Java returns the *fully qualified* class name
/// (`"org.apache.kafka.common.errors.UnknownServerException"`). Rust has no Java
/// package to report, so this returns the bare type name
/// (`"UnknownServerError"`) — the same string [`std::fmt::Display`] prefixes, which
/// is the translation of `Throwable.toString()`'s `getClass().getName()` and so
/// keeps the two consistent.
///
/// The method is **required**, with no default, for the same reason
/// [`ErrorMessage::message`] is: a wrong-but-plausible default would let a new
/// payload silently report the wrong class instead of failing to compile.
#[delegatable_trait]
pub(crate) trait ErrorName {
    /// The error's own type name, without a module path.
    fn name(&self) -> &'static str;
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
            code: $crate::common::Errors::UnknownServerError,
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
            /// Create the error with the given message and no cause,
            /// mirroring Java's `(String message)` constructor.
            ///
            /// This keeps the plain name under CLAUDE.md §2: the dominant Java
            /// shape among the classes this macro declares is
            /// `(String message)` + `(String message, Throwable cause)`, whose
            /// parameter-name intersection is `{message}` — matched exactly by
            /// `(String message)`.
            pub fn new(message: impl Into<String>) -> Self {
                Self { message: message.into(), source: None }
            }

            /// Create the error with the given message and an underlying cause,
            /// mirroring Java's `(String message, Throwable cause)` constructor.
            ///
            /// Suffixed with the one parameter beyond the `{message}`
            /// intersection (CLAUDE.md §2) — see [`new`](Self::new).
            pub fn with_source(message: impl Into<String>, source: $crate::common::Error) -> Self {
                Self { message: message.into(), source: Some(Box::new(source)) }
            }

            /// Create the error with the default message for its error code,
            /// mirroring Java's `Errors.exception()`.
            ///
            /// Not a constructor overload, so CLAUDE.md §2's suffixing does not
            /// apply: it translates the `Errors` static factory, which fills in
            /// `Errors.message()` and has no Java constructor counterpart.
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

        impl $crate::common::error::ErrorSource for $name {
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

        impl $crate::common::error::ErrorMessage for $name {
            fn message(&self) -> &str {
                &self.message
            }
        }

        impl $crate::common::error::ErrorName for $name {
            fn name(&self) -> &'static str {
                stringify!($name)
            }
        }

        impl $crate::common::error::ErrorCode for $name {
            fn error(&self) -> $crate::common::Errors {
                $code
            }
        }

        impl $crate::common::error::ErrorHierarchy for $name {
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
/// Distinct from [`std::fmt::Display`], which translates `Throwable.toString()` —
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

impl<T: ErrorName + ?Sized> ErrorName for Box<T> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
}

// ---------------------------------------------------------------------------
// Specific error structs — correspond to Java error subclasses
// ---------------------------------------------------------------------------

/// Declares a message-only error struct — one Java class that carries nothing
/// but its message.
///
/// Java gives each of these its own class, which is exactly what
/// `#[enum_dispatch]` requires: the payload type answers the hierarchy
/// predicates, so `Error::Timeout` and `Error::Wakeup` cannot both carry a
/// `String`. The generated `Display` renders as `"<TypeName>: <message>"`,
/// preserving the strings the previous hand-written `Display` produced.
///
/// Every path in the expansion is `$crate`-qualified, as in
/// [`kafka_error_class`], so an invoking file needs nothing in scope but the
/// macro itself. The four `Local*` classes each live in their own file per
/// CLAUDE.md §2 and invoke this from there.
///
/// # Constructors deliberately not modelled
///
/// The four classes stand in for JDK types (`java.lang.IllegalStateException`,
/// `java.lang.IllegalArgumentException`,
/// `java.util.ConcurrentModificationException`,
/// `java.util.concurrent.TimeoutException`), each of which really declares all
/// four of `()`, `(String)`, `(Throwable)`, `(String, Throwable)`. Under
/// CLAUDE.md §2 the no-arg form would own the plain `new` and the message form
/// would become `with_message`.
///
/// They are not modelled, and `new` keeps the message form, for two reasons:
///
///   - These are not translated Kafka classes but minimal stand-ins for classes
///     outside the repository (CLAUDE.md §1) — hence the macro's name. A
///     null-message constructor and a cause-only constructor would be public API
///     with no caller and no Kafka contract behind them (DoD #7).
///   - [`kafka_error_class`] cannot follow suit: it declares 141 classes from one
///     body, and only ~19 of their Java counterparts have a no-arg constructor.
///     Were `new` to mean the no-arg form here and the message form there,
///     `SomeError::new("msg")` would compile or not depending on which macro
///     declared the class, with nothing at the call site to say which.
macro_rules! message_only_error {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        pub struct $name {
            message: String,
            /// The underlying cause — `Throwable`'s `cause`, null by default.
            source: Option<Box<$crate::common::Error>>,
        }

        impl $name {
            /// Create the error with the given message and no cause,
            /// mirroring the `(String message)` constructor.
            ///
            /// Keeps the plain name under CLAUDE.md §2, for consistency with
            /// `kafka_error_class!` — see the note on this macro about the
            /// constructors these classes deliberately do not model.
            /// (Deliberately not an intra-doc link: this doc comment expands
            /// into each declaring module, where that macro is not in scope.)
            pub fn new(message: impl Into<String>) -> Self {
                Self { message: message.into(), source: None }
            }

            /// Create the error with the given message and an underlying cause,
            /// mirroring the `(String message, Throwable cause)` constructor.
            ///
            /// Suffixed with the one parameter beyond the `{message}`
            /// intersection (CLAUDE.md §2) — see [`new`](Self::new).
            pub fn with_source(message: impl Into<String>, source: $crate::common::Error) -> Self {
                Self { message: message.into(), source: Some(Box::new(source)) }
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

        impl $crate::common::error::ErrorSource for $name {
            fn source(&self) -> Option<&$crate::common::Error> {
                self.source.as_deref()
            }
        }

        impl ::std::error::Error for $name {
            fn source(&self) -> Option<&(dyn ::std::error::Error + 'static)> {
                self.source
                    .as_deref()
                    .map(|e| e as &(dyn ::std::error::Error + 'static))
            }
        }

        impl $crate::common::error::ErrorName for $name {
            fn name(&self) -> &'static str {
                stringify!($name)
            }
        }

        // No protocol code: these are the generic `java.lang` / `java.util`
        // classes, raised only by this client and never reported by a broker, so
        // the trait default (`Errors::UnknownServerError`) is the right answer.
        impl $crate::common::error::ErrorCode for $name {}

        impl $crate::common::error::ErrorMessage for $name {
            fn message(&self) -> &str {
                // Field access, not `self.message()`: that would resolve to the
                // inherent method above and is only accidentally equivalent.
                &self.message
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}: {}", stringify!($name), self.message)
            }
        }
    };
}

pub(crate) use message_only_error;

// ---------------------------------------------------------------------------
// Display — each payload renders itself, as each Java class has its own
// toString(). `Error`'s own Display just forwards (see `Error::as_display`).
// ---------------------------------------------------------------------------

// `ErrorMessage` is not hand-written for any payload: the message-only types get
// it from the `message_only_error!` macro, the five that embed a [`KafkaError`]
// delegate to that field via `#[delegate(ErrorMessage, target = "kafka_error")]`,
// and [`KafkaError`] itself uses `target = "self"` over its inherent method.

// ---------------------------------------------------------------------------
// ErrorName / ErrorHierarchy impls — each type's override list IS its Java
// `extends` chain
// ---------------------------------------------------------------------------

// The fifteen payloads that are neither `kafka_error_class!` nor
// `message_only_error!` declarations: each adds subclass state, so it is
// hand-written in its own file. `ErrorName` is a crate-local trait, so the impls
// live here rather than in fifteen files — keeping the one place a reader can
// check that every payload answers, next to the trait itself. `KafkaError` is
// the sixteenth and answers next to its own struct, in `kafka_error.rs`.
macro_rules! error_name_impl {
    ($($name:ident),+ $(,)?) => {
        $(
            impl ErrorName for $name {
                fn name(&self) -> &'static str {
                    stringify!($name)
                }
            }
        )+
    };
}

error_name_impl! {
    CorrelationIdMismatchError,
    DuplicateResourceError,
    GroupAuthorizationError,
    InvalidTopicError,
    InvalidReceiveError,
    RecordDeserializationError,
    ResourceNotFoundError,
    QuotaViolationError,
    ThrottlingQuotaExceededError,
    TopicAuthorizationError,
    ConsumerCommitFailedError,
    ConsumerLogTruncationError,
    ConsumerNoOffsetForPartitionError,
    ConsumerOffsetOutOfRangeError,
    ConsumerRetriableCommitFailedError,
}

// `IllegalArgumentException`, `IllegalStateException` and
// `ConcurrentModificationException` are `java.lang` / `java.util` runtime
// exceptions sitting BESIDE `KafkaException`, not below it. Every predicate is
// false for them, so each takes the trait's defaults unchanged — the empty impl
// is the statement that they are outside the hierarchy.
impl ErrorHierarchy for LocalIllegalArgumentError {}
impl ErrorHierarchy for LocalIllegalStateError {}
impl ErrorHierarchy for LocalConcurrentModificationError {}
impl ErrorHierarchy for LocalTimeoutError {}

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
/// the `KafkaException` hierarchy ([`LocalIllegalArgument`](Self::LocalIllegalArgument),
/// [`LocalIllegalState`](Self::LocalIllegalState),
/// [`LocalConcurrentModification`](Self::LocalConcurrentModification)); flattening two
/// Java families into one enum is what makes
/// [`is_kafka_error`](Self::is_kafka_error) necessary.
///
/// `error()`, `code()`, `message()` and `is_retriable_error()` are delegated to
/// the inner [`KafkaError`] base. `request_utils::RequestUtils::is_fatal_error` and
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
#[delegate(ErrorName)]
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
    LocalIllegalArgument(LocalIllegalArgumentError),
    /// Illegal state error — a method was called in an invalid state.
    ///
    /// Corresponds to Java's `IllegalStateException`.
    LocalIllegalState(LocalIllegalStateError),
    /// Concurrent modification error — the consumer was accessed from more
    /// than one thread.
    ///
    /// Corresponds to Java's `java.util.ConcurrentModificationException`,
    /// thrown by `KafkaConsumer.acquire()` ("KafkaConsumer is not safe for
    /// multi-threaded access"). Like `LocalIllegalState`, it is a plain
    /// `RuntimeException` — neither an `ApiException` nor a `KafkaException`
    /// — so it is never retriable and never fatal.
    LocalConcurrentModification(LocalConcurrentModificationError),
    /// A wait on a future timed out.
    ///
    /// Corresponds to Java's `java.util.concurrent.TimeoutException` raised by
    /// `Future.get(timeout, unit)`. Distinct from
    /// [`Timeout`](Self::Timeout), which is
    /// `org.apache.kafka.common.errors.TimeoutException` — a
    /// `RetriableException`. This one sits beside `KafkaException`, so it is
    /// never retriable, never an api error and carries no wire code.
    LocalTimeout(LocalTimeoutError),

    // One variant per Java exception class, each carrying the struct that
    // declares its own `extends` chain. Delegation does the rest.
    /// See [`ApiError`](crate::common::errors::ApiError).
    Api(ApiError),
    /// See [`AuthenticationError`](crate::common::errors::AuthenticationError).
    Authentication(AuthenticationError),
    /// See [`AuthorizationError`](crate::common::errors::AuthorizationError).
    Authorization(AuthorizationError),
    /// See [`AuthorizerNotReadyError`](crate::common::errors::AuthorizerNotReadyError).
    AuthorizerNotReady(AuthorizerNotReadyError),
    /// See [`BrokerIdNotRegisteredError`](crate::common::errors::BrokerIdNotRegisteredError).
    BrokerIdNotRegistered(BrokerIdNotRegisteredError),
    /// See [`BrokerNotAvailableError`](crate::common::errors::BrokerNotAvailableError).
    BrokerNotAvailable(BrokerNotAvailableError),
    /// See [`ProducerBufferExhaustedError`](crate::producer::ProducerBufferExhaustedError).
    ProducerBufferExhausted(ProducerBufferExhaustedError),
    /// See [`ClusterAuthorizationError`](crate::common::errors::ClusterAuthorizationError).
    ClusterAuthorization(ClusterAuthorizationError),
    /// See [`ConcurrentTransactionsError`](crate::common::errors::ConcurrentTransactionsError).
    ConcurrentTransactions(ConcurrentTransactionsError),
    /// See [`ControllerMovedError`](crate::common::errors::ControllerMovedError).
    ControllerMoved(ControllerMovedError),
    /// See [`CoordinatorLoadInProgressError`](crate::common::errors::CoordinatorLoadInProgressError).
    CoordinatorLoadInProgress(CoordinatorLoadInProgressError),
    /// See [`CoordinatorNotAvailableError`](crate::common::errors::CoordinatorNotAvailableError).
    CoordinatorNotAvailable(CoordinatorNotAvailableError),
    /// See [`CorrelationIdMismatchError`](crate::common::requests::CorrelationIdMismatchError).
    ///
    /// Lives in `common.requests`, not `common.errors`, and extends
    /// `IllegalStateException` rather than `KafkaException` — so like
    /// [`LocalIllegalState`](Self::LocalIllegalState) it answers `false` to every
    /// predicate.
    CorrelationIdMismatch(CorrelationIdMismatchError),
    /// See [`CorruptRecordError`](crate::common::errors::CorruptRecordError).
    CorruptRecord(CorruptRecordError),
    /// See [`DelegationTokenAuthorizationError`](crate::common::errors::DelegationTokenAuthorizationError).
    DelegationTokenAuthorization(DelegationTokenAuthorizationError),
    /// See [`DelegationTokenDisabledError`](crate::common::errors::DelegationTokenDisabledError).
    DelegationTokenDisabled(DelegationTokenDisabledError),
    /// See [`DelegationTokenExpiredError`](crate::common::errors::DelegationTokenExpiredError).
    DelegationTokenExpired(DelegationTokenExpiredError),
    /// See [`DelegationTokenNotFoundError`](crate::common::errors::DelegationTokenNotFoundError).
    DelegationTokenNotFound(DelegationTokenNotFoundError),
    /// See [`DelegationTokenOwnerMismatchError`](crate::common::errors::DelegationTokenOwnerMismatchError).
    DelegationTokenOwnerMismatch(DelegationTokenOwnerMismatchError),
    /// See [`DisconnectError`](crate::common::errors::DisconnectError).
    Disconnect(DisconnectError),
    /// See [`DuplicateBrokerRegistrationError`](crate::common::errors::DuplicateBrokerRegistrationError).
    DuplicateBrokerRegistration(DuplicateBrokerRegistrationError),
    /// See [`DuplicateResourceError`](crate::common::errors::DuplicateResourceError).
    DuplicateResource(DuplicateResourceError),
    /// See [`DuplicateSequenceError`](crate::common::errors::DuplicateSequenceError).
    DuplicateSequence(DuplicateSequenceError),
    /// See [`DuplicateVoterError`](crate::common::errors::DuplicateVoterError).
    DuplicateVoter(DuplicateVoterError),
    /// See [`ElectionNotNeededError`](crate::common::errors::ElectionNotNeededError).
    ElectionNotNeeded(ElectionNotNeededError),
    /// See [`EligibleLeadersNotAvailableError`](crate::common::errors::EligibleLeadersNotAvailableError).
    EligibleLeadersNotAvailable(EligibleLeadersNotAvailableError),
    /// See [`FeatureUpdateFailedError`](crate::common::errors::FeatureUpdateFailedError).
    FeatureUpdateFailed(FeatureUpdateFailedError),
    /// See [`FencedInstanceIdError`](crate::common::errors::FencedInstanceIdError).
    FencedInstanceId(FencedInstanceIdError),
    /// See [`FencedLeaderEpochError`](crate::common::errors::FencedLeaderEpochError).
    FencedLeaderEpoch(FencedLeaderEpochError),
    /// See [`FencedMemberEpochError`](crate::common::errors::FencedMemberEpochError).
    FencedMemberEpoch(FencedMemberEpochError),
    /// See [`FencedStateEpochError`](crate::common::errors::FencedStateEpochError).
    FencedStateEpoch(FencedStateEpochError),
    /// See [`FetchSessionIdNotFoundError`](crate::common::errors::FetchSessionIdNotFoundError).
    FetchSessionIdNotFound(FetchSessionIdNotFoundError),
    /// See [`FetchSessionTopicIdError`](crate::common::errors::FetchSessionTopicIdError).
    FetchSessionTopicId(FetchSessionTopicIdError),
    /// See [`GroupAuthorizationError`](crate::common::errors::GroupAuthorizationError).
    GroupAuthorization(GroupAuthorizationError),
    /// See [`GroupIdNotFoundError`](crate::common::errors::GroupIdNotFoundError).
    GroupIdNotFound(GroupIdNotFoundError),
    /// See [`GroupMaxSizeReachedError`](crate::common::errors::GroupMaxSizeReachedError).
    GroupMaxSizeReached(GroupMaxSizeReachedError),
    /// See [`GroupNotEmptyError`](crate::common::errors::GroupNotEmptyError).
    GroupNotEmpty(GroupNotEmptyError),
    /// See [`GroupSubscribedToTopicError`](crate::common::errors::GroupSubscribedToTopicError).
    GroupSubscribedToTopic(GroupSubscribedToTopicError),
    /// See [`IllegalGenerationError`](crate::common::errors::IllegalGenerationError).
    IllegalGeneration(IllegalGenerationError),
    /// See [`IllegalSaslStateError`](crate::common::errors::IllegalSaslStateError).
    IllegalSaslState(IllegalSaslStateError),
    /// See [`InconsistentClusterIdError`](crate::common::errors::InconsistentClusterIdError).
    InconsistentClusterId(InconsistentClusterIdError),
    /// See [`InconsistentGroupProtocolError`](crate::common::errors::InconsistentGroupProtocolError).
    InconsistentGroupProtocol(InconsistentGroupProtocolError),
    /// See [`InconsistentTopicIdError`](crate::common::errors::InconsistentTopicIdError).
    InconsistentTopicId(InconsistentTopicIdError),
    /// See [`InconsistentVoterSetError`](crate::common::errors::InconsistentVoterSetError).
    InconsistentVoterSet(InconsistentVoterSetError),
    /// See [`IneligibleReplicaError`](crate::common::errors::IneligibleReplicaError).
    IneligibleReplica(IneligibleReplicaError),
    /// See [`InterruptError`](crate::common::errors::InterruptError).
    Interrupt(InterruptError),
    /// See [`InvalidCommitOffsetSizeError`](crate::common::errors::InvalidCommitOffsetSizeError).
    InvalidCommitOffsetSize(InvalidCommitOffsetSizeError),
    /// See [`InvalidConfigurationError`](crate::common::errors::InvalidConfigurationError).
    InvalidConfiguration(InvalidConfigurationError),
    /// See [`InvalidFetchSessionEpochError`](crate::common::errors::InvalidFetchSessionEpochError).
    InvalidFetchSessionEpoch(InvalidFetchSessionEpochError),
    /// See [`InvalidFetchSizeError`](crate::common::errors::InvalidFetchSizeError).
    InvalidFetchSize(InvalidFetchSizeError),
    /// See [`InvalidGroupIdError`](crate::common::errors::InvalidGroupIdError).
    InvalidGroupId(InvalidGroupIdError),
    /// See [`InvalidOffsetError`](crate::common::errors::InvalidOffsetError).
    InvalidOffset(InvalidOffsetError),
    /// See [`InvalidPartitionsError`](crate::common::errors::InvalidPartitionsError).
    InvalidPartitions(InvalidPartitionsError),
    /// See [`InvalidPidMappingError`](crate::common::errors::InvalidPidMappingError).
    InvalidPidMapping(InvalidPidMappingError),
    /// See [`InvalidPrincipalTypeError`](crate::common::errors::InvalidPrincipalTypeError).
    InvalidPrincipalType(InvalidPrincipalTypeError),
    /// See [`InvalidProducerEpochError`](crate::common::errors::InvalidProducerEpochError).
    InvalidProducerEpoch(InvalidProducerEpochError),
    /// See [`InvalidRecordError`](crate::common::InvalidRecordError).
    InvalidRecord(InvalidRecordError),
    /// See [`InvalidRecordStateError`](crate::common::errors::InvalidRecordStateError).
    InvalidRecordState(InvalidRecordStateError),
    /// See [`InvalidRegistrationError`](crate::common::errors::InvalidRegistrationError).
    InvalidRegistration(InvalidRegistrationError),
    /// See [`InvalidRegularExpressionError`](crate::common::errors::InvalidRegularExpressionError).
    InvalidRegularExpression(InvalidRegularExpressionError),
    /// See [`InvalidReplicaAssignmentError`](crate::common::errors::InvalidReplicaAssignmentError).
    InvalidReplicaAssignment(InvalidReplicaAssignmentError),
    /// See [`InvalidReplicationFactorError`](crate::common::errors::InvalidReplicationFactorError).
    InvalidReplicationFactor(InvalidReplicationFactorError),
    /// See [`InvalidRequestError`](crate::common::errors::InvalidRequestError).
    InvalidRequest(InvalidRequestError),
    /// See [`InvalidRequiredAcksError`](crate::common::errors::InvalidRequiredAcksError).
    InvalidRequiredAcks(InvalidRequiredAcksError),
    /// See [`InvalidSessionTimeoutError`](crate::common::errors::InvalidSessionTimeoutError).
    InvalidSessionTimeout(InvalidSessionTimeoutError),
    /// See [`InvalidShareSessionEpochError`](crate::common::errors::InvalidShareSessionEpochError).
    InvalidShareSessionEpoch(InvalidShareSessionEpochError),
    /// See [`InvalidTimestampError`](crate::common::errors::InvalidTimestampError).
    InvalidTimestamp(InvalidTimestampError),
    /// See [`InvalidTopicError`](crate::common::errors::InvalidTopicError).
    InvalidTopic(InvalidTopicError),
    /// See [`InvalidTxnStateError`](crate::common::errors::InvalidTxnStateError).
    InvalidTxnState(InvalidTxnStateError),
    /// See [`InvalidTxnTimeoutError`](crate::common::errors::InvalidTxnTimeoutError).
    InvalidTxnTimeout(InvalidTxnTimeoutError),
    /// See [`InvalidUpdateVersionError`](crate::common::errors::InvalidUpdateVersionError).
    InvalidUpdateVersion(InvalidUpdateVersionError),
    /// See [`InvalidVoterKeyError`](crate::common::errors::InvalidVoterKeyError).
    InvalidVoterKey(InvalidVoterKeyError),
    /// See [`KafkaStorageError`](crate::common::errors::KafkaStorageError).
    KafkaStorage(KafkaStorageError),
    /// See [`LeaderNotAvailableError`](crate::common::errors::LeaderNotAvailableError).
    LeaderNotAvailable(LeaderNotAvailableError),
    /// See [`ListenerNotFoundError`](crate::common::errors::ListenerNotFoundError).
    ListenerNotFound(ListenerNotFoundError),
    /// See [`LogDirNotFoundError`](crate::common::errors::LogDirNotFoundError).
    LogDirNotFound(LogDirNotFoundError),
    /// See [`MemberIdRequiredError`](crate::common::errors::MemberIdRequiredError).
    MemberIdRequired(MemberIdRequiredError),
    /// See [`MismatchedEndpointTypeError`](crate::common::errors::MismatchedEndpointTypeError).
    MismatchedEndpointType(MismatchedEndpointTypeError),
    /// See [`NetworkError`](crate::common::errors::NetworkError).
    Network(NetworkError),
    /// See [`NewLeaderElectedError`](crate::common::errors::NewLeaderElectedError).
    NewLeaderElected(NewLeaderElectedError),
    /// See [`NoReassignmentInProgressError`](crate::common::errors::NoReassignmentInProgressError).
    NoReassignmentInProgress(NoReassignmentInProgressError),
    /// See [`NotControllerError`](crate::common::errors::NotControllerError).
    NotController(NotControllerError),
    /// See [`NotCoordinatorError`](crate::common::errors::NotCoordinatorError).
    NotCoordinator(NotCoordinatorError),
    /// See [`NotEnoughReplicasError`](crate::common::errors::NotEnoughReplicasError).
    NotEnoughReplicas(NotEnoughReplicasError),
    /// See [`NotEnoughReplicasAfterAppendError`](crate::common::errors::NotEnoughReplicasAfterAppendError).
    NotEnoughReplicasAfterAppend(NotEnoughReplicasAfterAppendError),
    /// See [`NotLeaderOrFollowerError`](crate::common::errors::NotLeaderOrFollowerError).
    NotLeaderOrFollower(NotLeaderOrFollowerError),
    /// See [`OffsetMetadataTooLargeError`](crate::common::errors::OffsetMetadataTooLargeError).
    OffsetMetadataTooLarge(OffsetMetadataTooLargeError),
    /// See [`OffsetMovedToTieredStorageError`](crate::common::errors::OffsetMovedToTieredStorageError).
    OffsetMovedToTieredStorage(OffsetMovedToTieredStorageError),
    /// See [`OffsetNotAvailableError`](crate::common::errors::OffsetNotAvailableError).
    OffsetNotAvailable(OffsetNotAvailableError),
    /// See [`OffsetOutOfRangeError`](crate::common::errors::OffsetOutOfRangeError).
    OffsetOutOfRange(OffsetOutOfRangeError),
    /// See [`OperationNotAttemptedError`](crate::common::errors::OperationNotAttemptedError).
    OperationNotAttempted(OperationNotAttemptedError),
    /// See [`OutOfOrderSequenceError`](crate::common::errors::OutOfOrderSequenceError).
    OutOfOrderSequence(OutOfOrderSequenceError),
    /// See [`PolicyViolationError`](crate::common::errors::PolicyViolationError).
    PolicyViolation(PolicyViolationError),
    /// See [`PositionOutOfRangeError`](crate::common::errors::PositionOutOfRangeError).
    PositionOutOfRange(PositionOutOfRangeError),
    /// See [`PreferredLeaderNotAvailableError`](crate::common::errors::PreferredLeaderNotAvailableError).
    PreferredLeaderNotAvailable(PreferredLeaderNotAvailableError),
    /// See [`PrincipalDeserializationError`](crate::common::errors::PrincipalDeserializationError).
    PrincipalDeserialization(PrincipalDeserializationError),
    /// See [`ProducerFencedError`](crate::common::errors::ProducerFencedError).
    ProducerFenced(ProducerFencedError),
    /// See [`QuotaViolationError`](crate::common::metrics::QuotaViolationError).
    ///
    /// Boxed for the same reason as [`RecordDeserialization`](Self::RecordDeserialization):
    /// it carries a whole [`MetricName`](crate::common::MetricName) (three
    /// `String`s and a `BTreeMap`), which unboxed makes it the largest payload in
    /// the enum at 120 bytes and pushes `Error` — and with it every
    /// `Result<_, Error>` in the crate — past clippy's 128-byte
    /// `result_large_err` threshold.
    QuotaViolation(Box<QuotaViolationError>),
    /// See [`ReassignmentInProgressError`](crate::common::errors::ReassignmentInProgressError).
    ReassignmentInProgress(ReassignmentInProgressError),
    /// See [`RebalanceInProgressError`](crate::common::errors::RebalanceInProgressError).
    RebalanceInProgress(RebalanceInProgressError),
    /// See [`RebootstrapRequiredError`](crate::common::errors::RebootstrapRequiredError).
    RebootstrapRequired(RebootstrapRequiredError),
    /// See [`RecordBatchTooLargeError`](crate::common::errors::RecordBatchTooLargeError).
    RecordBatchTooLarge(RecordBatchTooLargeError),
    /// See [`RecordDeserializationError`](crate::common::errors::RecordDeserializationError).
    ///
    /// Boxed: it carries the full record context (partition, offsets, key/value
    /// buffers, headers), which unboxed pushes `Error` — and every
    /// `Result<_, Error>` — past clippy's 128-byte `result_large_err` threshold.
    RecordDeserialization(Box<RecordDeserializationError>),
    /// See [`RecordTooLargeError`](crate::common::errors::RecordTooLargeError).
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
    /// See [`ReplicaNotAvailableError`](crate::common::errors::ReplicaNotAvailableError).
    ReplicaNotAvailable(ReplicaNotAvailableError),
    /// See [`ResourceNotFoundError`](crate::common::errors::ResourceNotFoundError).
    ResourceNotFound(ResourceNotFoundError),
    /// See [`SaslAuthenticationError`](crate::common::errors::SaslAuthenticationError).
    SaslAuthentication(SaslAuthenticationError),
    /// See [`SchemaError`](crate::common::protocol::types::SchemaError).
    ///
    /// Lives in `common.protocol.types`, not `common.errors`.
    Schema(SchemaError),
    /// See [`SecurityDisabledError`](crate::common::errors::SecurityDisabledError).
    SecurityDisabled(SecurityDisabledError),
    /// See [`SerializationError`](crate::common::errors::SerializationError).
    Serialization(SerializationError),
    /// See [`ShareSessionLimitReachedError`](crate::common::errors::ShareSessionLimitReachedError).
    ShareSessionLimitReached(ShareSessionLimitReachedError),
    /// See [`ShareSessionNotFoundError`](crate::common::errors::ShareSessionNotFoundError).
    ShareSessionNotFound(ShareSessionNotFoundError),
    /// See [`SnapshotNotFoundError`](crate::common::errors::SnapshotNotFoundError).
    SnapshotNotFound(SnapshotNotFoundError),
    /// See [`SslAuthenticationError`](crate::common::errors::SslAuthenticationError).
    SslAuthentication(SslAuthenticationError),
    /// See [`StaleBrokerEpochError`](crate::common::errors::StaleBrokerEpochError).
    StaleBrokerEpoch(StaleBrokerEpochError),
    /// See [`StaleMemberEpochError`](crate::common::errors::StaleMemberEpochError).
    StaleMemberEpoch(StaleMemberEpochError),
    /// See [`StreamsInvalidTopologyError`](crate::common::errors::StreamsInvalidTopologyError).
    StreamsInvalidTopology(StreamsInvalidTopologyError),
    /// See [`StreamsInvalidTopologyEpochError`](crate::common::errors::StreamsInvalidTopologyEpochError).
    StreamsInvalidTopologyEpoch(StreamsInvalidTopologyEpochError),
    /// See [`StreamsTopologyFencedError`](crate::common::errors::StreamsTopologyFencedError).
    StreamsTopologyFenced(StreamsTopologyFencedError),
    /// See [`TelemetryTooLargeError`](crate::common::errors::TelemetryTooLargeError).
    TelemetryTooLarge(TelemetryTooLargeError),
    /// See [`ThrottlingQuotaExceededError`](crate::common::errors::ThrottlingQuotaExceededError).
    ThrottlingQuotaExceeded(ThrottlingQuotaExceededError),
    /// See [`TimeoutError`](crate::common::errors::TimeoutError).
    Timeout(TimeoutError),
    /// See [`TopicAuthorizationError`](crate::common::errors::TopicAuthorizationError).
    TopicAuthorization(TopicAuthorizationError),
    /// See [`TopicDeletionDisabledError`](crate::common::errors::TopicDeletionDisabledError).
    TopicDeletionDisabled(TopicDeletionDisabledError),
    /// See [`TopicExistsError`](crate::common::errors::TopicExistsError).
    TopicExists(TopicExistsError),
    /// See [`TransactionAbortableError`](crate::common::errors::TransactionAbortableError).
    TransactionAbortable(TransactionAbortableError),
    /// See [`TransactionAbortedError`](crate::common::errors::TransactionAbortedError).
    TransactionAborted(TransactionAbortedError),
    /// See [`TransactionCoordinatorFencedError`](crate::common::errors::TransactionCoordinatorFencedError).
    TransactionCoordinatorFenced(TransactionCoordinatorFencedError),
    /// See [`TransactionalIdAuthorizationError`](crate::common::errors::TransactionalIdAuthorizationError).
    TransactionalIdAuthorization(TransactionalIdAuthorizationError),
    /// See [`TransactionalIdNotFoundError`](crate::common::errors::TransactionalIdNotFoundError).
    TransactionalIdNotFound(TransactionalIdNotFoundError),
    /// See [`UnacceptableCredentialError`](crate::common::errors::UnacceptableCredentialError).
    UnacceptableCredential(UnacceptableCredentialError),
    /// See [`UnknownControllerIdError`](crate::common::errors::UnknownControllerIdError).
    UnknownControllerId(UnknownControllerIdError),
    /// See [`UnknownLeaderEpochError`](crate::common::errors::UnknownLeaderEpochError).
    UnknownLeaderEpoch(UnknownLeaderEpochError),
    /// See [`UnknownMemberIdError`](crate::common::errors::UnknownMemberIdError).
    UnknownMemberId(UnknownMemberIdError),
    /// See [`UnknownProducerIdError`](crate::common::errors::UnknownProducerIdError).
    UnknownProducerId(UnknownProducerIdError),
    /// See [`UnknownServerError`](crate::common::errors::UnknownServerError).
    UnknownServer(UnknownServerError),
    /// See [`UnknownSubscriptionIdError`](crate::common::errors::UnknownSubscriptionIdError).
    UnknownSubscriptionId(UnknownSubscriptionIdError),
    /// See [`UnknownTopicIdError`](crate::common::errors::UnknownTopicIdError).
    UnknownTopicId(UnknownTopicIdError),
    /// See [`UnknownTopicOrPartitionError`](crate::common::errors::UnknownTopicOrPartitionError).
    UnknownTopicOrPartition(UnknownTopicOrPartitionError),
    /// See [`UnreleasedInstanceIdError`](crate::common::errors::UnreleasedInstanceIdError).
    UnreleasedInstanceId(UnreleasedInstanceIdError),
    /// See [`UnstableOffsetCommitError`](crate::common::errors::UnstableOffsetCommitError).
    UnstableOffsetCommit(UnstableOffsetCommitError),
    /// See [`UnsupportedAssignorError`](crate::common::errors::UnsupportedAssignorError).
    UnsupportedAssignor(UnsupportedAssignorError),
    /// See [`UnsupportedByAuthenticationError`](crate::common::errors::UnsupportedByAuthenticationError).
    UnsupportedByAuthentication(UnsupportedByAuthenticationError),
    /// See [`UnsupportedCompressionTypeError`](crate::common::errors::UnsupportedCompressionTypeError).
    UnsupportedCompressionType(UnsupportedCompressionTypeError),
    /// See [`UnsupportedEndpointTypeError`](crate::common::errors::UnsupportedEndpointTypeError).
    UnsupportedEndpointType(UnsupportedEndpointTypeError),
    /// See [`UnsupportedForMessageFormatError`](crate::common::errors::UnsupportedForMessageFormatError).
    UnsupportedForMessageFormat(UnsupportedForMessageFormatError),
    /// See [`UnsupportedSaslMechanismError`](crate::common::errors::UnsupportedSaslMechanismError).
    UnsupportedSaslMechanism(UnsupportedSaslMechanismError),
    /// See [`UnsupportedVersionError`](crate::common::errors::UnsupportedVersionError).
    UnsupportedVersion(UnsupportedVersionError),
    /// See [`VoterNotFoundError`](crate::common::errors::VoterNotFoundError).
    VoterNotFound(VoterNotFoundError),
    /// See [`WakeupError`](crate::common::errors::WakeupError).
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

    /// Create a bare Kafka error with no message, translating Java's no-arg
    /// `new KafkaException()` (`KafkaException.java:38`).
    ///
    /// The four `kafka*` constructors below translate `KafkaException`'s four
    /// (`:38`, `:30`, `:34`, `:26`). Their parameter-name intersection is empty
    /// and the no-arg form matches it, so under CLAUDE.md §2 this one keeps the
    /// plain name and the others are suffixed with their parameters.
    ///
    /// This is deliberately NOT [`new`](Self::new) with
    /// [`Errors::UnknownServerError`]: that constructor resolves the code to the
    /// class Java associates with it and yields
    /// [`UnknownServer`](Self::UnknownServer), an `ApiException`. Java's bare
    /// `KafkaException` is a *sibling* of `ApiException`, not a subclass, so it
    /// belongs in the [`KafkaError`](Self::KafkaError) variant. The difference is
    /// observable: [`is_api_error`](Self::is_api_error) answers `false` here and
    /// `true` there, and callers such as `KafkaProducer.doSend` dispatch on
    /// exactly that (`catch (ApiException e)` returns a failed future,
    /// `catch (KafkaException e)` rethrows).
    ///
    /// The wire code stays [`Errors::UnknownServerError`] because a
    /// client-constructed `KafkaException` has no protocol code of its own.
    ///
    /// Java's no-arg form leaves the message null; Rust reports the code's
    /// default message instead, since [`KafkaError`] stores the code rather than
    /// deriving it from a subclass and always has one to fall back on.
    pub fn kafka() -> Self {
        Self::KafkaError(KafkaError::new(Errors::UnknownServerError))
    }

    /// Create a bare Kafka error, translating Java's
    /// `new KafkaException(String message)` (`KafkaException.java:30`).
    ///
    /// See [`kafka`](Self::kafka) for why this does not go through
    /// [`with_message`](Self::with_message).
    pub fn kafka_message(message: impl Into<String>) -> Self {
        Self::KafkaError(KafkaError::with_message(Errors::UnknownServerError, message))
    }

    /// Create a bare Kafka error carrying only the error that caused it,
    /// translating Java's `new KafkaException(Throwable cause)`
    /// (`KafkaException.java:34`).
    ///
    /// See [`kafka`](Self::kafka) for why this does not go through
    /// [`with_message`](Self::with_message).
    pub fn kafka_source(source: Error) -> Self {
        Self::KafkaError(KafkaError::with_source(Errors::UnknownServerError, source))
    }

    /// Create a bare Kafka error carrying the error that caused it, translating
    /// Java's `new KafkaException(String message, Throwable cause)`
    /// (`KafkaException.java:26`).
    ///
    /// See [`kafka`](Self::kafka) for why this does not go through
    /// [`with_message`](Self::with_message).
    pub fn kafka_message_source(message: impl Into<String>, source: Error) -> Self {
        Self::KafkaError(KafkaError::with_message_source(Errors::UnknownServerError, message, source))
    }

    /// Create a topic authorization error
    /// (Java: `new TopicAuthorizationException(unauthorizedTopics)`).
    ///
    /// The pair intersects on `{unauthorizedTopics}`, which is exactly this
    /// overload, so it keeps the plain name (CLAUDE.md §2).
    pub fn topic_authorization(topics: HashSet<String>) -> Self {
        Self::TopicAuthorization(TopicAuthorizationError::new(topics))
    }

    /// Create a topic authorization error carrying a custom message
    /// (Java: `new TopicAuthorizationException(message, unauthorizedTopics)`).
    pub fn topic_authorization_message(topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self::TopicAuthorization(TopicAuthorizationError::with_message(topics, message))
    }

    /// Create an invalid topic error
    /// (Java: `new InvalidTopicException(invalidTopics)`).
    ///
    /// The pair intersects on `{invalidTopics}`, which is exactly this overload,
    /// so it keeps the plain name (CLAUDE.md §2).
    pub fn invalid_topics(topics: HashSet<String>) -> Self {
        Self::InvalidTopic(InvalidTopicError::new(topics))
    }

    /// Create an invalid topic error carrying a custom message
    /// (Java: `new InvalidTopicException(message, invalidTopics)`, and the
    /// `new InvalidTopicException(String message)` form when `topics` is empty).
    pub fn invalid_topics_message(topics: HashSet<String>, message: impl Into<String>) -> Self {
        Self::InvalidTopic(InvalidTopicError::with_message(topics, message))
    }

    // The two `group_authorization*` factories below are NOT an overload group:
    // the first translates Java's `static forGroupId(String)` and the second the
    // `(String message, String groupId)` constructor. Distinct Java members, so
    // CLAUDE.md §2's suffixing does not apply and neither name changes.

    /// Create a group authorization error for a group ID, formatting the group
    /// into the message (Java: `GroupAuthorizationException.forGroupId(groupId)`).
    pub fn group_authorization(group_id: impl Into<String>) -> Self {
        Self::GroupAuthorization(GroupAuthorizationError::for_group_id(group_id))
    }

    /// Create a group authorization error carrying a custom message
    /// (Java: `new GroupAuthorizationException(message, groupId)`).
    pub fn group_authorization_with_message(group_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self::GroupAuthorization(GroupAuthorizationError::new(group_id, message))
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
    pub fn local_illegal_argument(message: impl Into<String>) -> Self {
        Self::LocalIllegalArgument(LocalIllegalArgumentError::new(message))
    }

    // Java's three `ConfigException` constructors have an empty parameter-name
    // intersection and none is no-arg, so no factory below keeps the plain name
    // `config` (CLAUDE.md §2) — each takes its full parameter list as the suffix.
    // [`ConfigError::new`] is the one exception, and only because it is
    // macro-generated; see the note there.

    /// Create a configuration error, in Java's `ConfigException(message)` format.
    ///
    /// Corresponds to Java's `ConfigException` — an invalid config value. Unlike
    /// [`illegal_argument`](Self::local_illegal_argument) this is inside the
    /// `KafkaException` hierarchy, so [`is_kafka_error`](Self::is_kafka_error) is
    /// `true` for it.
    pub fn config_message(message: impl Into<String>) -> Self {
        Self::Config(ConfigError::new(message))
    }

    /// Create a configuration error naming the offending value and key, in
    /// Java's `ConfigException(name, value)` format.
    pub fn config_name_value(name: impl std::fmt::Display, value: impl std::fmt::Display) -> Self {
        Self::Config(ConfigError::with_name_value(name, value))
    }

    /// Create a configuration error naming the value, key, and a detail message,
    /// in Java's `ConfigException(name, value, message)` format.
    pub fn config_name_value_message(
        name: impl std::fmt::Display,
        value: impl std::fmt::Display,
        message: impl std::fmt::Display,
    ) -> Self {
        Self::Config(ConfigError::with_name_value_message(name, value, message))
    }

    /// Create an illegal state error.
    ///
    /// Corresponds to Java's `IllegalStateException`.
    pub fn local_illegal_state(message: impl Into<String>) -> Self {
        Self::LocalIllegalState(LocalIllegalStateError::new(message))
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

    /// Create a correlation-id mismatch error.
    ///
    /// Corresponds to Java's
    /// `CorrelationIdMismatchException(String, int, int)` — an
    /// `IllegalStateException`, so outside the `KafkaException` hierarchy.
    pub fn correlation_id_mismatch(
        message: impl Into<String>,
        request_correlation_id: i32,
        response_correlation_id: i32,
    ) -> Self {
        Self::CorrelationIdMismatch(CorrelationIdMismatchError::new(
            message,
            request_correlation_id,
            response_correlation_id,
        ))
    }

    /// Create a protocol-schema error.
    ///
    /// Corresponds to Java's `SchemaException(String)`.
    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema(SchemaError::new(message))
    }

    /// Create a protocol-schema error carrying the failure that caused it.
    ///
    /// Corresponds to Java's `SchemaException(String, Throwable)`, used by
    /// `NetworkClient.parseResponse` to wrap a buffer underflow.
    ///
    /// The pair intersects on `{message}`, which is exactly
    /// [`schema`](Self::schema), so that one keeps the plain name and this is
    /// suffixed with the parameter beyond it (CLAUDE.md §2).
    pub fn schema_source(message: impl Into<String>, source: Error) -> Self {
        Self::Schema(SchemaError::with_source(message, source))
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
    pub fn local_concurrent_modification(message: impl Into<String>) -> Self {
        Self::LocalConcurrentModification(LocalConcurrentModificationError::new(message))
    }

    /// Create a timed-out-waiting-on-a-future error.
    ///
    /// Corresponds to Java's `java.util.concurrent.TimeoutException` from
    /// `Future.get(timeout, unit)`. Use [`Error::timeout`] instead for
    /// `org.apache.kafka.common.errors.TimeoutException`, the retriable Kafka
    /// class the broker reports.
    pub fn local_timeout(message: impl Into<String>) -> Self {
        Self::LocalTimeout(LocalTimeoutError::new(message))
    }

    /// Create a transaction aborted error with Java's default message.
    ///
    /// Corresponds to Java's no-arg `TransactionAbortedException()`
    /// (`TransactionAbortedException.java:35`), whose message is
    /// `"Failing batch since transaction was aborted"`.
    ///
    /// The pair's parameter-name intersection is empty and this overload matches
    /// it, so it keeps the plain name (CLAUDE.md §2).
    pub fn transaction_aborted() -> Self {
        Self::TransactionAborted(TransactionAbortedError::new("Failing batch since transaction was aborted"))
    }

    /// Create a transaction aborted error with a custom message.
    ///
    /// Corresponds to Java's `TransactionAbortedException(String)` (`:31`).
    pub fn transaction_aborted_message(message: impl Into<String>) -> Self {
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
    /// (e.g. [`LocalIllegalArgument`](Self::LocalIllegalArgument), [`LocalIllegalState`](Self::LocalIllegalState)).
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
    /// This is NOT what `to_string()` produces. [`std::fmt::Display`] translates
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
    /// [`LocalIllegalArgument`](Self::LocalIllegalArgument),
    /// [`LocalIllegalState`](Self::LocalIllegalState) and
    /// [`LocalConcurrentModification`](Self::LocalConcurrentModification).
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
    /// `true` for every payload whose `extends:` list names `is_api_error` —
    /// which is every class carrying a protocol code, since `Errors.java` only
    /// names `ApiException` subclasses, plus the code-less concrete bases
    /// [`Api`](Self::Api), [`Authentication`](Self::Authentication) and
    /// [`Authorization`](Self::Authorization). Examples:
    /// [`InvalidTopic`](Self::InvalidTopic),
    /// [`RecordTooLarge`](Self::RecordTooLarge), [`Timeout`](Self::Timeout),
    /// [`TopicAuthorization`](Self::TopicAuthorization),
    /// [`GroupAuthorization`](Self::GroupAuthorization),
    /// [`ThrottlingQuotaExceeded`](Self::ThrottlingQuotaExceeded) and
    /// [`ProducerBufferExhausted`](Self::ProducerBufferExhausted).
    ///
    /// `false` for the bare [`KafkaError`](Self::KafkaError) — `KafkaException`
    /// is `ApiException`'s *parent*, not an instance of it, and since
    /// [`Error::new`] resolves every code to its own class that variant is now
    /// reached only for [`Errors::None`] and for
    /// [`Error::kafka`](Self::kafka). Also `false` for
    /// [`LocalIllegalArgument`](Self::LocalIllegalArgument) and
    /// [`LocalIllegalState`](Self::LocalIllegalState) (plain `RuntimeException`s),
    /// [`LocalConcurrentModification`](Self::LocalConcurrentModification), and for
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
    /// The answer belongs to the payload, not to the enum: each error class
    /// names `is_retriable_error` in its `extends:` list, so the covered set is
    /// exactly the classes whose Java ancestry passes through
    /// `RetriableException` — directly, or via `RefreshRetriableException`,
    /// `InvalidMetadataException` or `TimeoutException`. [`Timeout`](Self::Timeout)
    /// is retriable (Java's `TimeoutException` extends `RetriableException`), as
    /// are [`ThrottlingQuotaExceeded`](Self::ThrottlingQuotaExceeded) and
    /// [`ProducerBufferExhausted`](Self::ProducerBufferExhausted) (through
    /// `BufferExhaustedException extends TimeoutException`).
    ///
    /// Since [`Error::new`] resolves every code to its own class, the set of
    /// *codes* answering `true` is pinned in both directions — against the Java
    /// `extends` chain, over every code — by `errors.rs`'s
    /// `test_retriable_errors_match_java_hierarchy`.
    ///
    /// The bare [`KafkaError`](Self::KafkaError) answers `false`
    /// (`KafkaException` is not a `RetriableException`), and so do
    /// [`LocalIllegalArgument`](Self::LocalIllegalArgument) and
    /// [`LocalIllegalState`](Self::LocalIllegalState).
    pub fn is_retriable_error(&self) -> bool {
        ErrorHierarchy::is_retriable_error(self)
    }

    /// Whether this error's Java class extends `RefreshRetriableException`
    /// (CLAUDE.md §10.4) — retriable, and a metadata / coordinator refresh is
    /// what clears it.
    ///
    /// Fifteen payloads name it in their `extends:` list: the thirteen that also
    /// answer [`is_invalid_metadata_error`](Self::is_invalid_metadata_error)
    /// (`InvalidMetadataException extends RefreshRetriableException`), plus
    /// [`CoordinatorNotAvailable`](Self::CoordinatorNotAvailable) and
    /// [`NotCoordinator`](Self::NotCoordinator). All fifteen carry a protocol
    /// code, so the set is pinned in both directions over every code by
    /// `errors.rs`'s `test_hierarchy_predicates_match_java`.
    ///
    /// The bare [`KafkaError`](Self::KafkaError) answers `false`, as do the
    /// payloads carrying no protocol code.
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
    /// Thirteen payloads name it, all of them code-carrying; the set is pinned in
    /// both directions over every code by `errors.rs`'s
    /// `test_hierarchy_predicates_match_java`.
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
    /// Covers
    /// [`ConsumerNoOffsetForPartition`](Self::ConsumerNoOffsetForPartition),
    /// [`ConsumerOffsetOutOfRange`](Self::ConsumerOffsetOutOfRange) and
    /// [`ConsumerLogTruncation`](Self::ConsumerLogTruncation) — the `Consumer`
    /// prefix is what CLAUDE.md §2 adds to the `clients.consumer` package's
    /// classes, so it is part of the variant name.
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
    /// Five payloads name it in their `extends:` list:
    /// [`Authentication`](Self::Authentication) — the concrete base class, which
    /// has no entry in `Errors` of its own but inherits
    /// [`Errors::InvalidConfig`] from `InvalidConfigurationException` — plus
    /// [`SaslAuthentication`](Self::SaslAuthentication),
    /// [`SslAuthentication`](Self::SslAuthentication),
    /// [`IllegalSaslState`](Self::IllegalSaslState) and
    /// [`UnsupportedSaslMechanism`](Self::UnsupportedSaslMechanism). Three of
    /// them carry a code (`SASL_AUTHENTICATION_FAILED`, `ILLEGAL_SASL_STATE`,
    /// `UNSUPPORTED_SASL_MECHANISM`) and that code set is pinned in both
    /// directions over every code by `errors.rs`'s
    /// `test_hierarchy_predicates_match_java`.
    ///
    /// A handshake failure detected locally starts out as an
    /// [`AuthenticationError`] payload inside an `io::Error`
    /// (`common::network::auth_io_error`), and it DOES reach this enum: the
    /// producer's `Sender` rebuilds it as [`Authentication`](Self::Authentication)
    /// when failing the transaction manager's pending requests, because that is
    /// the only spelling for which this predicate — and hence
    /// `request_utils::RequestUtils::is_fatal_error` — answers `true`.
    ///
    /// Nested inside
    /// [`is_invalid_configuration_error`](Self::is_invalid_configuration_error).
    pub fn is_authentication_error(&self) -> bool {
        ErrorHierarchy::is_authentication_error(self)
    }

    /// Whether this error's Java class extends `AuthorizationException`
    /// (CLAUDE.md §10.4).
    ///
    /// Six payloads name it in their `extends:` list:
    /// [`Authorization`](Self::Authorization) — the concrete base class, which
    /// has no entry in `Errors` of its own but inherits
    /// [`Errors::InvalidConfig`] from `InvalidConfigurationException` — plus the five
    /// that do carry one,
    /// [`TopicAuthorization`](Self::TopicAuthorization),
    /// [`GroupAuthorization`](Self::GroupAuthorization),
    /// [`ClusterAuthorization`](Self::ClusterAuthorization),
    /// [`TransactionalIdAuthorization`](Self::TransactionalIdAuthorization) and
    /// [`DelegationTokenAuthorization`](Self::DelegationTokenAuthorization).
    /// Those five codes are pinned in both directions over every code by
    /// `errors.rs`'s `test_hierarchy_predicates_match_java`; the bare
    /// [`KafkaError`](Self::KafkaError) answers `false`.
    ///
    /// Nested inside
    /// [`is_invalid_configuration_error`](Self::is_invalid_configuration_error).
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
        assert!(Error::local_illegal_state("misuse").source().is_none());
        assert!(StdError::source(&Error::new(Errors::RequestTimedOut)).is_none());

        // Set through `KafkaError`'s Java-shaped `(String, Throwable)` constructor.
        let root = Error::new(Errors::ClusterAuthorizationFailed);
        let wrapped = Error::KafkaError(KafkaError::with_message_source(
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
            Error::local_illegal_argument("not utf-8"),
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

    /// [`Error::kafka`] must produce a *bare* `KafkaException`, not the
    /// `UnknownServerException` that [`Error::with_message`] resolves
    /// [`Errors::UnknownServerError`] to.
    ///
    /// The difference is observable and load-bearing: `KafkaProducer.doSend`
    /// dispatches on `catch (ApiException e)` (fire the callback, return a failed
    /// future) versus `catch (KafkaException e)` (rethrow), so translating a bare
    /// `new KafkaException(..)` through `with_message` silently moves the error into
    /// the wrong arm.
    #[test]
    fn kafka_builds_a_bare_kafka_error_not_an_api_error() {
        let bare = Error::kafka_message("Producer closed while send in progress");
        assert!(matches!(bare, Error::KafkaError(_)), "got {bare:?}");
        assert!(bare.is_kafka_error());
        // Java: a bare `KafkaException` is not an `ApiException`.
        assert!(!bare.is_api_error(), "a bare Kafka error is not an API error");
        assert_eq!(bare.message(), "Producer closed while send in progress");
        assert_eq!(bare.error(), Errors::UnknownServerError, "no protocol code of its own");

        // The contrast, and why the helper exists.
        let resolved = Error::with_message(Errors::UnknownServerError, "same code, different class");
        assert!(matches!(resolved, Error::UnknownServer(_)), "got {resolved:?}");
        // Java: `UnknownServerException` IS an `ApiException`.
        assert!(resolved.is_api_error(), "an unknown-server error IS an API error");

        // The `(String, Throwable)` form carries the cause.
        let wrapped =
            Error::kafka_message_source("Failed to construct kafka producer", Error::config_message("bad ssl path"));
        assert!(!wrapped.is_api_error());
        assert_eq!(wrapped.message(), "Failed to construct kafka producer");
        assert_eq!(wrapped.source().expect("cause retained").message(), "bad ssl path");
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

    /// `LocalConcurrentModification` mirrors `LocalIllegalState`: a plain Java
    /// `RuntimeException`, so it carries no protocol code, is never
    /// retriable or fatal, and is neither an `ApiException` nor a
    /// `KafkaException`.
    #[test]
    fn concurrent_modification_parity_with_illegal_state() {
        let cme = Error::local_concurrent_modification("KafkaConsumer is not safe for multi-threaded access.");
        let ise = Error::local_illegal_state("bad state");

        assert_eq!(cme.message(), "KafkaConsumer is not safe for multi-threaded access.");
        assert_eq!(cme.code(), ise.code());
        assert_eq!(cme.error(), ise.error());
        assert_eq!(cme.is_retriable_error(), ise.is_retriable_error());
        assert!(!cme.is_retriable_error());
        assert_eq!(
            crate::common::requests::RequestUtils::is_fatal_error(&cme),
            crate::common::requests::RequestUtils::is_fatal_error(&ise)
        );
        assert!(!crate::common::requests::RequestUtils::is_fatal_error(&cme));
        assert_eq!(cme.is_api_error(), ise.is_api_error());
        assert!(!cme.is_api_error());
        assert_eq!(cme.is_kafka_error(), ise.is_kafka_error());
        assert!(!cme.is_kafka_error());
        assert!(cme.kafka_error().is_none());
    }

    #[test]
    fn concurrent_modification_display() {
        let cme = Error::local_concurrent_modification("oops");
        assert_eq!(cme.to_string(), "LocalConcurrentModificationError: oops");
    }

    /// Every [`Error`] variant against its Java `extends` chain, in both
    /// directions.
    ///
    /// The predicates are no longer computed by matching on the enum — each is
    /// delegated to the variant's payload, whose `impl ErrorHierarchy` declares
    /// its ancestry. This table is what stops a payload from silently claiming
    /// (or dropping) a superclass, which a per-variant `assert!` could not.
    ///
    /// Order: kafka, api, retriable, refresh_retriable, timeout,
    /// invalid_metadata, invalid_configuration, application_recoverable,
    /// invalid_offset, consumer_invalid_offset, consumer_offset_out_of_range,
    /// out_of_order_sequence, serialization, authentication, authorization,
    /// fatal — the same order the assertion message below spells out, and the
    /// order the `actual` array is built in.
    #[test]
    fn variant_predicates_match_java_hierarchy() {
        use std::collections::HashMap;
        let cases: &[(&str, Error, [bool; 16])] = &[
            // The three concrete intermediate classes the flattened enum has to
            // carry as variants of their own: Java lets a caller `throw new
            // ApiException(msg)` / `new AuthenticationException(msg)` /
            // `new AuthorizationException(msg)` directly, and none of the three
            // has an entry in `Errors.java`, so no code reaches them. They are
            // the rows `TransactionExceptionHierarchyTest
            // .testInvalidConfigurationExceptionHierarchy` asserts for the two
            // base classes.
            // ApiException -> KafkaException
            (
                "Api",
                Error::Api(ApiError::new("generic")),
                [
                    true, true, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // AuthenticationException -> InvalidConfigurationException -> ApiException
            // -> KafkaException, and fatal per `RequestUtils.isFatalException`.
            (
                "Authentication",
                Error::Authentication(AuthenticationError::new("bad credentials")),
                [
                    true, true, false, false, false, false, true, false, false, false, false, false, false, true,
                    false, true,
                ],
            ),
            // AuthorizationException -> InvalidConfigurationException -> ApiException
            // -> KafkaException, and fatal per `RequestUtils.isFatalException`.
            (
                "Authorization",
                Error::Authorization(AuthorizationError::new("not authorized")),
                [
                    true, true, false, false, false, false, true, false, false, false, false, false, false, false,
                    true, true,
                ],
            ),
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
            // SchemaException -> KafkaException (NOT an ApiException), like
            // `SerializationException`.
            (
                "Schema",
                Error::schema("Buffer underflow"),
                [
                    true, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // CorrelationIdMismatchException -> java.lang.IllegalStateException:
            // outside the hierarchy entirely, so it reads like the `LocalIllegalState`
            // row below rather than like a `KafkaException`.
            (
                "CorrelationIdMismatch",
                Error::correlation_id_mismatch("ids disagree", 7, 9),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
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
                "LocalIllegalArgument",
                Error::local_illegal_argument("bad arg"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            (
                "LocalIllegalState",
                Error::local_illegal_state("bad state"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            (
                "LocalConcurrentModification",
                Error::local_concurrent_modification("racy"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            (
                "LocalTimeout",
                Error::local_timeout("timed out"),
                [
                    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
                    false, false,
                ],
            ),
            // A bare `KafkaException`: inside the hierarchy, but the parent of
            // `ApiException` rather than an instance of it, so it answers `true`
            // to `is_kafka_error` and `false` to every other predicate.
            (
                "KafkaError",
                Error::kafka_message("Producer closed while send in progress"),
                [
                    true, false, false, false, false, false, false, false, false, false, false, false, false, false,
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
            // `Error::new(code)` resolves the code to its own class (Java's
            // `Errors.exception()`), so these two rows check the classes the
            // wire codes name — NOT the bare `KafkaError` variant, which
            // `Error::new` now only yields for `Errors::None`.
            // NOT_LEADER_OR_FOLLOWER: InvalidMetadataException -> RefreshRetriableException
            // -> RetriableException -> ApiException -> KafkaException.
            (
                "NotLeaderOrFollower (from the code)",
                Error::new(Errors::NotLeaderOrFollower),
                [
                    true, true, true, true, false, true, false, false, false, false, false, false, false, false, false,
                    false,
                ],
            ),
            // SASL_AUTHENTICATION_FAILED: AuthenticationException -> ApiException, and fatal.
            (
                "SaslAuthenticationFailed (from the code)",
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
                crate::common::requests::RequestUtils::is_fatal_error(err),
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
            ("LocalIllegalArgumentError", Error::local_illegal_argument("boom")),
            ("LocalIllegalStateError", Error::local_illegal_state("boom")),
            ("LocalConcurrentModificationError", Error::local_concurrent_modification("boom")),
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
        // Java: `TimeoutException` carries `Errors.REQUEST_TIMED_OUT`.
        assert_eq!(timeout.code(), 7, "a timeout error is Errors.REQUEST_TIMED_OUT");
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
    fn bare_kafka_error_is_not_an_api_error() {
        let bare = Error::KafkaError(KafkaError::new(Errors::None));
        assert!(bare.is_kafka_error());
        // Java: `KafkaException` is `ApiException`'s parent, not an instance of it.
        assert!(!bare.is_api_error(), "a bare Kafka error is the parent class, not an API error");
        assert!(!bare.is_retriable_error());
        assert!(!crate::common::requests::RequestUtils::is_fatal_error(&bare));

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
            Error::local_illegal_state("x"),
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
            Error::local_illegal_argument("bad arg"),
            Error::local_illegal_state("bad state"),
            Error::local_concurrent_modification("racy"),
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
                // Java: `ApiException extends KafkaException`.
                assert!(err.is_kafka_error(), "{err}: an API error must be a Kafka error");
            }
            if err.is_retriable_error() {
                // Java: `RetriableException extends ApiException`.
                assert!(err.is_api_error(), "{err}: a retriable error must be an API error");
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
                    "{err}: auth/authz errors must be invalid-configuration errors"
                );
                assert!(
                    crate::common::requests::RequestUtils::is_fatal_error(err),
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
                    "{err}: invalid-configuration / application-recoverable / invalid-offset / \
                     out-of-order-sequence errors are all API errors"
                );
            }
            if err.is_timeout_error() {
                // TimeoutException extends RetriableException.
                assert!(err.is_retriable_error(), "{err}: a timeout error is retriable");
            }
            if err.is_consumer_offset_out_of_range_error() {
                // consumer OffsetOutOfRangeException extends consumer
                // InvalidOffsetException.
                assert!(
                    err.is_consumer_invalid_offset_error(),
                    "{err}: a consumer offset-out-of-range error is a consumer invalid-offset error"
                );
            }
            if err.is_consumer_invalid_offset_error() {
                // The consumer's InvalidOffsetException extends KafkaException,
                // NOT ApiException — unlike the common.errors class of the same
                // name.
                assert!(err.is_kafka_error(), "{err}: a consumer invalid-offset error is a Kafka error");
                assert!(
                    !err.is_api_error(),
                    "{err}: a consumer invalid-offset error is NOT an API error"
                );
            }
            if err.is_serialization_error() {
                // SerializationException extends KafkaException directly.
                assert!(err.is_kafka_error(), "{err}: a serialization error is a Kafka error");
                assert!(!err.is_api_error(), "{err}: a serialization error is NOT an API error");
            }
        }
    }
    // -----------------------------------------------------------------------
    // TransactionExceptionHierarchyTest.java (Apache Kafka 4.2,
    // clients/src/test/java/org/apache/kafka/common/errors/
    // TransactionExceptionHierarchyTest.java)
    //
    // Java asserts on `Class.isAssignableFrom`, which it can do because the
    // hierarchy is the type system. Rust has no subclassing, so the flattened
    // enum answers through the payload's `extends:` list — these four tests are
    // that list checked against Java's, class by class, over exactly the Java
    // `@ValueSource` sets. Each parameterized method becomes a loop, per DoD #3.
    //
    // Java names two classes that this crate carries as `Error` variants with no
    // protocol code — `AuthenticationException` and `AuthorizationException` —
    // and they are the reason this file is translated rather than left to the
    // code-level tables in `protocol/errors.rs`, which can only reach classes a
    // wire code names.
    // -----------------------------------------------------------------------

    /// `testRetriableExceptionHierarchy`: these six extend `RetriableException`
    /// and must NOT extend `RefreshRetriableException`.
    ///
    /// Java's javadoc gives the stake: "Using `RefreshRetriableException`
    /// changes the exception handling behavior, so only exceptions extending
    /// `RetriableException` directly are considered valid here."
    #[test]
    fn transaction_hierarchy_retriable_classes() {
        let classes: &[(&str, Error)] = &[
            ("TimeoutError", Error::Timeout(TimeoutError::new("m"))),
            (
                "NotEnoughReplicasError",
                Error::NotEnoughReplicas(NotEnoughReplicasError::new("m")),
            ),
            (
                "CoordinatorLoadInProgressError",
                Error::CoordinatorLoadInProgress(CoordinatorLoadInProgressError::new("m")),
            ),
            ("CorruptRecordError", Error::CorruptRecord(CorruptRecordError::new("m"))),
            (
                "NotEnoughReplicasAfterAppendError",
                Error::NotEnoughReplicasAfterAppend(NotEnoughReplicasAfterAppendError::new("m")),
            ),
            (
                "ConcurrentTransactionsError",
                Error::ConcurrentTransactions(ConcurrentTransactionsError::new("m")),
            ),
        ];
        for (name, error) in classes {
            assert!(error.is_retriable_error(), "{name} should be a retriable error");
            assert!(
                !error.is_refresh_retriable_error(),
                "{name} should NOT be a refresh-retriable error"
            );
        }
    }

    /// `testRefreshRetriableException`: `RefreshRetriableException extends
    /// RetriableException`.
    ///
    /// Java asserts it on the intermediate class itself. The flattened enum has
    /// no such class, so the equivalent statement is that the nesting holds for
    /// every error: nothing may answer `true` to the child predicate and `false`
    /// to the parent's. Asserted over the same class set the next test uses,
    /// plus every protocol code — the code-level half is also covered by
    /// `errors.rs`'s `test_hierarchy_predicates_nest`.
    #[test]
    fn transaction_hierarchy_refresh_retriable_is_retriable() {
        for code in -1i16..=200 {
            let error = Errors::for_code(code);
            if let Some(error) = error.error() {
                assert!(
                    !error.is_refresh_retriable_error() || error.is_retriable_error(),
                    "{error:?}: a refresh-retriable error should also be a retriable error"
                );
            }
        }
    }

    /// `testRefreshRetriableExceptionHierarchy`: these four extend
    /// `RefreshRetriableException` — and therefore `RetriableException`.
    #[test]
    fn transaction_hierarchy_refresh_retriable_classes() {
        let classes: &[(&str, Error)] = &[
            (
                "UnknownTopicOrPartitionError",
                Error::UnknownTopicOrPartition(UnknownTopicOrPartitionError::new("m")),
            ),
            (
                "NotLeaderOrFollowerError",
                Error::NotLeaderOrFollower(NotLeaderOrFollowerError::new("m")),
            ),
            ("NotCoordinatorError", Error::NotCoordinator(NotCoordinatorError::new("m"))),
            (
                "CoordinatorNotAvailableError",
                Error::CoordinatorNotAvailable(CoordinatorNotAvailableError::new("m")),
            ),
        ];
        for (name, error) in classes {
            assert!(error.is_refresh_retriable_error(), "{name} should be a refresh-retriable error");
            assert!(error.is_retriable_error(), "{name} should be a retriable error");
        }
    }

    /// `testApplicationRecoverableExceptionHierarchy`: these six extend
    /// `ApplicationRecoverableException`.
    #[test]
    fn transaction_hierarchy_application_recoverable_classes() {
        let classes: &[(&str, Error)] = &[
            (
                "FencedInstanceIdError",
                Error::FencedInstanceId(FencedInstanceIdError::new("m")),
            ),
            (
                "IllegalGenerationError",
                Error::IllegalGeneration(IllegalGenerationError::new("m")),
            ),
            (
                "InvalidPidMappingError",
                Error::InvalidPidMapping(InvalidPidMappingError::new("m")),
            ),
            (
                "InvalidProducerEpochError",
                Error::InvalidProducerEpoch(InvalidProducerEpochError::new("m")),
            ),
            ("ProducerFencedError", Error::ProducerFenced(ProducerFencedError::new("m"))),
            ("UnknownMemberIdError", Error::UnknownMemberId(UnknownMemberIdError::new("m"))),
        ];
        for (name, error) in classes {
            assert!(
                error.is_application_recoverable_error(),
                "{name} should be an application-recoverable error"
            );
        }
    }

    /// `testInvalidConfigurationExceptionHierarchy`: these twelve extend
    /// `InvalidConfigurationException`.
    ///
    /// Wider than the name suggests: in Kafka 4.2 both `AuthenticationException`
    /// and `AuthorizationException` extend it, which is why the two authorization
    /// subclasses and the two concrete bases are in Java's `@ValueSource`.
    /// [`Error::Authentication`] and [`Error::Authorization`] appear in no other
    /// predicate test — a wire code cannot reach them.
    #[test]
    fn transaction_hierarchy_invalid_configuration_classes() {
        let classes: &[(&str, Error)] = &[
            ("AuthenticationError", Error::Authentication(AuthenticationError::new("m"))),
            ("AuthorizationError", Error::Authorization(AuthorizationError::new("m"))),
            (
                "ClusterAuthorizationError",
                Error::ClusterAuthorization(ClusterAuthorizationError::new("m")),
            ),
            (
                "TransactionalIdAuthorizationError",
                Error::TransactionalIdAuthorization(TransactionalIdAuthorizationError::new("m")),
            ),
            (
                "UnsupportedVersionError",
                Error::UnsupportedVersion(UnsupportedVersionError::new("m")),
            ),
            (
                "UnsupportedForMessageFormatError",
                Error::UnsupportedForMessageFormat(UnsupportedForMessageFormatError::new("m")),
            ),
            ("InvalidRecordError", Error::InvalidRecord(InvalidRecordError::new("m"))),
            (
                "InvalidRequiredAcksError",
                Error::InvalidRequiredAcks(InvalidRequiredAcksError::new("m")),
            ),
            (
                "RecordBatchTooLargeError",
                Error::RecordBatchTooLarge(RecordBatchTooLargeError::new("m")),
            ),
            ("InvalidTopicError", Error::invalid_topics(HashSet::from(["t".to_string()]))),
            (
                "TopicAuthorizationError",
                Error::topic_authorization(HashSet::from(["t".to_string()])),
            ),
            ("GroupAuthorizationError", Error::group_authorization("g")),
        ];
        for (name, error) in classes {
            assert!(
                error.is_invalid_configuration_error(),
                "{name} should be an invalid-configuration error"
            );
        }
    }
}
