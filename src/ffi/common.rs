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

//! Shared C FFI machinery reused across the producer and consumer FFI layers.
//!
//! This module hosts the pieces that are not specific to either the producer
//! or the consumer:
//!
//! - The opaque [`kafka_common_Error_t`] error handle and its accessor
//!   functions. `kafka_common_*` is shared verbatim between FFI surfaces — a
//!   second definition would make cbindgen emit a duplicate type.
//! - The async completion-queue / dispatcher-thread abstraction
//!   ([`CompletionJob`], [`spawn_dispatcher`], [`enqueue_or_run_inline`]).
//! - The void-returning operation callback machinery ([`OperationCallbackFn`],
//!   [`OperationCompletion`], [`OperationCallbackTarget`]).
//! - The default logger initialization helper ([`init_default_logger`]).
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![allow(non_snake_case, non_camel_case_types)]

use std::ffi::{CString, c_char};

use crate::common::Error;
use crate::common::protocol::Errors;
// The 162 enumerators are spelled out in full (see
// [`kafka_common_ErrorCode_t`]), so this glob keeps the match arms in
// `error_code_of` readable without repeating the type name on each one.
use kafka_common_ErrorCode_t::*;

/// Initialize the default stderr log backend if RUST_LOG is set.
/// Idempotent: succeeds once, silently no-ops on subsequent calls.
/// A custom log backend (e.g. Python logging bridge) can be set before
/// the first producer/consumer is created to override this default.
pub(crate) fn init_default_logger() {
    #[cfg(feature = "ffi")]
    {
        let _ = env_logger::try_init();
    }
}

// ---------------------------------------------------------------------------
// Error handle
// ---------------------------------------------------------------------------

/// Internal wrapper that pairs [`Error`] with a [`CString`] for the
/// error message, so that [`kafka_common_Error_message`] can return a valid
/// `*const c_char` that lives as long as the handle.
pub(crate) struct ErrorInner {
    pub(crate) error: Error,
    /// Cached CString for the error message, created once at construction time.
    pub(crate) message_cstring: CString,
}

/// Opaque error handle returned by functions that can fail.
///
/// Internally wraps a `Box<ErrorInner>` containing the [`Error`]
/// and a cached [`CString`] for the error message.
///
/// A null `kafka_common_Error_t` pointer means success (no error).
#[repr(C)]
pub struct kafka_common_Error_t {
    _private: [u8; 0],
}

/// Wraps a [`Error`] into a heap-allocated opaque error pointer, including
/// a cached [`CString`] for the error message.
pub(crate) fn box_error(error: Error) -> *mut kafka_common_Error_t {
    let message_cstring = CString::new(error.message()).unwrap_or_else(|_| CString::new("").unwrap());
    let inner = ErrorInner { error, message_cstring };
    Box::into_raw(Box::new(inner)) as *mut kafka_common_Error_t
}

/// Casts a `*const kafka_common_Error_t` to a reference to `ErrorInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by [`box_error`].
pub(crate) unsafe fn error_ref(error: *const kafka_common_Error_t) -> &'static ErrorInner {
    unsafe { &*(error as *const ErrorInner) }
}

// ---------------------------------------------------------------------------
// Error code enum
// ---------------------------------------------------------------------------

/// The class of an error, as a named constant a C caller can switch on.
///
/// # Why this exists (DoD #7)
///
/// This type has **no Java counterpart**, and needs none: Java discriminates an
/// error with `instanceof`, and a Rust caller discriminates it by matching the
/// [`Error`] variant. C can do neither — it cannot see enum variants — so its
/// whole budget for classifying an error is the numeric code, a free-text
/// message, and the hierarchy predicates. This is the same reason CLAUDE.md §3
/// gives for exporting the predicates at all, applied to the other half of the
/// problem: the predicates place an error in a *family*, and this enum names
/// the *class*.
///
/// It is needed because Java's `Errors` deliberately carries no code for a
/// class that only the client raises — *"Do not add exceptions that occur only
/// on the client or only on the server here"* (`protocol/Errors.java`) — so all
/// of those classes report `UNKNOWN_SERVER_ERROR` (-1), and to C they are
/// indistinguishable from each other and from a genuine unknown server error.
///
/// # The assignment rule, and the invariant it encodes
///
/// A class takes Java's wire code **iff it owns that code**, that is, iff
/// [`Errors::error`](crate::common::protocol::Errors::error) maps the code back
/// to it. Every other class takes a Rust-local negative.
///
/// The rule states one invariant: **every class a broker can actually report
/// owns its code and keeps it; every class only the client raises gets a
/// negative.** That is exactly the split `Errors.java`'s javadoc describes, so
/// the faithful half of this enum is `Errors` verbatim — values *and* names,
/// which come from
/// [`Errors::enum_name`](crate::common::protocol::Errors::enum_name) and
/// therefore equal Java's constants.
///
/// Four classes resolve to a positive code in Java by **inheritance** without
/// owning it, so under this rule they take negatives:
/// [`Error::ProducerBufferExhausted`] (`BufferExhaustedException extends
/// TimeoutException`, so Java answers 7, owned by [`Error::Timeout`]) and
/// [`Error::Authentication`] / [`Error::Authorization`] /
/// [`Error::SslAuthentication`] (all extend `InvalidConfigurationException`, so
/// Java answers 40, owned by [`Error::InvalidConfiguration`]).
///
/// # Injectivity and ABI
///
/// The 162 values are pairwise distinct — the code alone identifies the class,
/// which is what lets a C caller use it as its sole discriminator. The
/// negatives are ABI once published: a new class **appends at the most-negative
/// end**, because inserting one mid-list would silently renumber every class
/// after it. A table test pins all 27 literally so a renumbering cannot pass.
///
/// cbindgen:prefix-with-name=false
// Implementation notes for Rust readers, kept out of the generated header.
//
// **Why the negatives live here and not in `common::protocol::Errors`.**
// Putting them there would contradict the javadoc quoted above, break the three
// tests translated from Java's `ErrorsTest`, and let `Errors::for_code` resolve
// a garbled `-2` in an `int16` wire field to a client-side class — the
// generated deserializers do no range validation. Keeping the synthetic half in
// the FFI layer leaves `Errors` and every wire path untouched.
//
// **Why `#[repr(C)]` and fully-spelled enumerator names.** cbindgen prefixes
// enumerators with the type's export name and applies `[export.rename]` before
// prefixing, so it would push the `_t` suffix into all 162 enumerators; the
// per-enum `prefix-with-name=false` annotation turns that off locally (leaving
// the global `[enum] prefix_with_name = true` in `cbindgen.toml` intact for
// every other enum) and the names are written out in full instead. As a bonus,
// one `grep kafka_common_ErrorCode_WAKEUP` then finds this definition and every
// C use of it.
//
// `#[repr(C)]` rather than `#[repr(i32)]` so the C typedef names the enum
// itself rather than an integer alias beside it: given an explicit integer repr
// cbindgen must emit an integer typedef to pin the size, and says so at the
// site — "if we need to specify size, then we have no choice but to create a
// typedef, so `config.style` is not respected" (`ir/enumeration.rs`). The ABI
// is identical: a fieldless `#[repr(C)]` enum takes the target C ABI's default
// enum size, which is `int` on every target Rust supports, and a C enum
// spanning -28..=133 is `int` too. Naming the enum in the typedef is what lets
// a C or C++ caller switch on the real type and get the compiler's
// exhaustiveness warning.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_ErrorCode_t {
    // Java's `Errors`, at Java's own values. Names come from
    // `Errors::enum_name()`, so each equals Java's constant.
    kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR = -1,
    kafka_common_ErrorCode_NONE = 0,
    kafka_common_ErrorCode_OFFSET_OUT_OF_RANGE = 1,
    kafka_common_ErrorCode_CORRUPT_MESSAGE = 2,
    kafka_common_ErrorCode_UNKNOWN_TOPIC_OR_PARTITION = 3,
    kafka_common_ErrorCode_INVALID_FETCH_SIZE = 4,
    kafka_common_ErrorCode_LEADER_NOT_AVAILABLE = 5,
    kafka_common_ErrorCode_NOT_LEADER_OR_FOLLOWER = 6,
    kafka_common_ErrorCode_REQUEST_TIMED_OUT = 7,
    kafka_common_ErrorCode_BROKER_NOT_AVAILABLE = 8,
    kafka_common_ErrorCode_REPLICA_NOT_AVAILABLE = 9,
    kafka_common_ErrorCode_MESSAGE_TOO_LARGE = 10,
    kafka_common_ErrorCode_STALE_CONTROLLER_EPOCH = 11,
    kafka_common_ErrorCode_OFFSET_METADATA_TOO_LARGE = 12,
    kafka_common_ErrorCode_NETWORK_ERROR = 13,
    kafka_common_ErrorCode_COORDINATOR_LOAD_IN_PROGRESS = 14,
    kafka_common_ErrorCode_COORDINATOR_NOT_AVAILABLE = 15,
    kafka_common_ErrorCode_NOT_COORDINATOR = 16,
    kafka_common_ErrorCode_INVALID_TOPIC_ERROR = 17,
    kafka_common_ErrorCode_RECORD_LIST_TOO_LARGE = 18,
    kafka_common_ErrorCode_NOT_ENOUGH_REPLICAS = 19,
    kafka_common_ErrorCode_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    kafka_common_ErrorCode_INVALID_REQUIRED_ACKS = 21,
    kafka_common_ErrorCode_ILLEGAL_GENERATION = 22,
    kafka_common_ErrorCode_INCONSISTENT_GROUP_PROTOCOL = 23,
    kafka_common_ErrorCode_INVALID_GROUP_ID = 24,
    kafka_common_ErrorCode_UNKNOWN_MEMBER_ID = 25,
    kafka_common_ErrorCode_INVALID_SESSION_TIMEOUT = 26,
    kafka_common_ErrorCode_REBALANCE_IN_PROGRESS = 27,
    kafka_common_ErrorCode_INVALID_COMMIT_OFFSET_SIZE = 28,
    kafka_common_ErrorCode_TOPIC_AUTHORIZATION_FAILED = 29,
    kafka_common_ErrorCode_GROUP_AUTHORIZATION_FAILED = 30,
    kafka_common_ErrorCode_CLUSTER_AUTHORIZATION_FAILED = 31,
    kafka_common_ErrorCode_INVALID_TIMESTAMP = 32,
    kafka_common_ErrorCode_UNSUPPORTED_SASL_MECHANISM = 33,
    kafka_common_ErrorCode_ILLEGAL_SASL_STATE = 34,
    kafka_common_ErrorCode_UNSUPPORTED_VERSION = 35,
    kafka_common_ErrorCode_TOPIC_ALREADY_EXISTS = 36,
    kafka_common_ErrorCode_INVALID_PARTITIONS = 37,
    kafka_common_ErrorCode_INVALID_REPLICATION_FACTOR = 38,
    kafka_common_ErrorCode_INVALID_REPLICA_ASSIGNMENT = 39,
    kafka_common_ErrorCode_INVALID_CONFIG = 40,
    kafka_common_ErrorCode_NOT_CONTROLLER = 41,
    kafka_common_ErrorCode_INVALID_REQUEST = 42,
    kafka_common_ErrorCode_UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
    kafka_common_ErrorCode_POLICY_VIOLATION = 44,
    kafka_common_ErrorCode_OUT_OF_ORDER_SEQUENCE_NUMBER = 45,
    kafka_common_ErrorCode_DUPLICATE_SEQUENCE_NUMBER = 46,
    kafka_common_ErrorCode_INVALID_PRODUCER_EPOCH = 47,
    kafka_common_ErrorCode_INVALID_TXN_STATE = 48,
    kafka_common_ErrorCode_INVALID_PRODUCER_ID_MAPPING = 49,
    kafka_common_ErrorCode_INVALID_TRANSACTION_TIMEOUT = 50,
    kafka_common_ErrorCode_CONCURRENT_TRANSACTIONS = 51,
    kafka_common_ErrorCode_TRANSACTION_COORDINATOR_FENCED = 52,
    kafka_common_ErrorCode_TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,
    kafka_common_ErrorCode_SECURITY_DISABLED = 54,
    kafka_common_ErrorCode_OPERATION_NOT_ATTEMPTED = 55,
    kafka_common_ErrorCode_KAFKA_STORAGE_ERROR = 56,
    kafka_common_ErrorCode_LOG_DIR_NOT_FOUND = 57,
    kafka_common_ErrorCode_SASL_AUTHENTICATION_FAILED = 58,
    kafka_common_ErrorCode_UNKNOWN_PRODUCER_ID = 59,
    kafka_common_ErrorCode_REASSIGNMENT_IN_PROGRESS = 60,
    kafka_common_ErrorCode_DELEGATION_TOKEN_AUTH_DISABLED = 61,
    kafka_common_ErrorCode_DELEGATION_TOKEN_NOT_FOUND = 62,
    kafka_common_ErrorCode_DELEGATION_TOKEN_OWNER_MISMATCH = 63,
    kafka_common_ErrorCode_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64,
    kafka_common_ErrorCode_DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65,
    kafka_common_ErrorCode_DELEGATION_TOKEN_EXPIRED = 66,
    kafka_common_ErrorCode_INVALID_PRINCIPAL_TYPE = 67,
    kafka_common_ErrorCode_NON_EMPTY_GROUP = 68,
    kafka_common_ErrorCode_GROUP_ID_NOT_FOUND = 69,
    kafka_common_ErrorCode_FETCH_SESSION_ID_NOT_FOUND = 70,
    kafka_common_ErrorCode_INVALID_FETCH_SESSION_EPOCH = 71,
    kafka_common_ErrorCode_LISTENER_NOT_FOUND = 72,
    kafka_common_ErrorCode_TOPIC_DELETION_DISABLED = 73,
    kafka_common_ErrorCode_FENCED_LEADER_EPOCH = 74,
    kafka_common_ErrorCode_UNKNOWN_LEADER_EPOCH = 75,
    kafka_common_ErrorCode_UNSUPPORTED_COMPRESSION_TYPE = 76,
    kafka_common_ErrorCode_STALE_BROKER_EPOCH = 77,
    kafka_common_ErrorCode_OFFSET_NOT_AVAILABLE = 78,
    kafka_common_ErrorCode_MEMBER_ID_REQUIRED = 79,
    kafka_common_ErrorCode_PREFERRED_LEADER_NOT_AVAILABLE = 80,
    kafka_common_ErrorCode_GROUP_MAX_SIZE_REACHED = 81,
    kafka_common_ErrorCode_FENCED_INSTANCE_ID = 82,
    kafka_common_ErrorCode_ELIGIBLE_LEADERS_NOT_AVAILABLE = 83,
    kafka_common_ErrorCode_ELECTION_NOT_NEEDED = 84,
    kafka_common_ErrorCode_NO_REASSIGNMENT_IN_PROGRESS = 85,
    kafka_common_ErrorCode_GROUP_SUBSCRIBED_TO_TOPIC = 86,
    kafka_common_ErrorCode_INVALID_RECORD = 87,
    kafka_common_ErrorCode_UNSTABLE_OFFSET_COMMIT = 88,
    kafka_common_ErrorCode_THROTTLING_QUOTA_EXCEEDED = 89,
    kafka_common_ErrorCode_PRODUCER_FENCED = 90,
    kafka_common_ErrorCode_RESOURCE_NOT_FOUND = 91,
    kafka_common_ErrorCode_DUPLICATE_RESOURCE = 92,
    kafka_common_ErrorCode_UNACCEPTABLE_CREDENTIAL = 93,
    kafka_common_ErrorCode_INCONSISTENT_VOTER_SET = 94,
    kafka_common_ErrorCode_INVALID_UPDATE_VERSION = 95,
    kafka_common_ErrorCode_FEATURE_UPDATE_FAILED = 96,
    kafka_common_ErrorCode_PRINCIPAL_DESERIALIZATION_FAILURE = 97,
    kafka_common_ErrorCode_SNAPSHOT_NOT_FOUND = 98,
    kafka_common_ErrorCode_POSITION_OUT_OF_RANGE = 99,
    kafka_common_ErrorCode_UNKNOWN_TOPIC_ID = 100,
    kafka_common_ErrorCode_DUPLICATE_BROKER_REGISTRATION = 101,
    kafka_common_ErrorCode_BROKER_ID_NOT_REGISTERED = 102,
    kafka_common_ErrorCode_INCONSISTENT_TOPIC_ID = 103,
    kafka_common_ErrorCode_INCONSISTENT_CLUSTER_ID = 104,
    kafka_common_ErrorCode_TRANSACTIONAL_ID_NOT_FOUND = 105,
    kafka_common_ErrorCode_FETCH_SESSION_TOPIC_ID_ERROR = 106,
    kafka_common_ErrorCode_INELIGIBLE_REPLICA = 107,
    kafka_common_ErrorCode_NEW_LEADER_ELECTED = 108,
    kafka_common_ErrorCode_OFFSET_MOVED_TO_TIERED_STORAGE = 109,
    kafka_common_ErrorCode_FENCED_MEMBER_EPOCH = 110,
    kafka_common_ErrorCode_UNRELEASED_INSTANCE_ID = 111,
    kafka_common_ErrorCode_UNSUPPORTED_ASSIGNOR = 112,
    kafka_common_ErrorCode_STALE_MEMBER_EPOCH = 113,
    kafka_common_ErrorCode_MISMATCHED_ENDPOINT_TYPE = 114,
    kafka_common_ErrorCode_UNSUPPORTED_ENDPOINT_TYPE = 115,
    kafka_common_ErrorCode_UNKNOWN_CONTROLLER_ID = 116,
    kafka_common_ErrorCode_UNKNOWN_SUBSCRIPTION_ID = 117,
    kafka_common_ErrorCode_TELEMETRY_TOO_LARGE = 118,
    kafka_common_ErrorCode_INVALID_REGISTRATION = 119,
    kafka_common_ErrorCode_TRANSACTION_ABORTABLE = 120,
    kafka_common_ErrorCode_INVALID_RECORD_STATE = 121,
    kafka_common_ErrorCode_SHARE_SESSION_NOT_FOUND = 122,
    kafka_common_ErrorCode_INVALID_SHARE_SESSION_EPOCH = 123,
    kafka_common_ErrorCode_FENCED_STATE_EPOCH = 124,
    kafka_common_ErrorCode_INVALID_VOTER_KEY = 125,
    kafka_common_ErrorCode_DUPLICATE_VOTER = 126,
    kafka_common_ErrorCode_VOTER_NOT_FOUND = 127,
    kafka_common_ErrorCode_INVALID_REGULAR_EXPRESSION = 128,
    kafka_common_ErrorCode_REBOOTSTRAP_REQUIRED = 129,
    kafka_common_ErrorCode_STREAMS_INVALID_TOPOLOGY = 130,
    kafka_common_ErrorCode_STREAMS_INVALID_TOPOLOGY_EPOCH = 131,
    kafka_common_ErrorCode_STREAMS_TOPOLOGY_FENCED = 132,
    kafka_common_ErrorCode_SHARE_SESSION_LIMIT_REACHED = 133,

    // Classes Java gives no code of their own: Rust-local negatives,
    // grouped by origin. These are ABI — new classes append at the
    // most-negative end, never in the middle.
    // JDK-derived (`Local*`).
    kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION = -2,
    kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT = -3,
    kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE = -4,
    kafka_common_ErrorCode_LOCAL_TIMEOUT = -5,
    // `common` client-side classes.
    kafka_common_ErrorCode_API = -6,
    kafka_common_ErrorCode_AUTHENTICATION = -7,
    kafka_common_ErrorCode_AUTHORIZER_NOT_READY = -8,
    kafka_common_ErrorCode_AUTHORIZATION = -9,
    kafka_common_ErrorCode_CONFIG = -10,
    kafka_common_ErrorCode_DISCONNECT = -11,
    kafka_common_ErrorCode_INTERRUPT = -12,
    kafka_common_ErrorCode_INVALID_OFFSET = -13,
    kafka_common_ErrorCode_SCHEMA = -14,
    kafka_common_ErrorCode_SERIALIZATION = -15,
    kafka_common_ErrorCode_SSL_AUTHENTICATION = -16,
    kafka_common_ErrorCode_TRANSACTION_ABORTED = -17,
    kafka_common_ErrorCode_WAKEUP = -18,
    // Hand-written / consumer classes.
    kafka_common_ErrorCode_CONSUMER_COMMIT_FAILED = -19,
    kafka_common_ErrorCode_CONSUMER_LOG_TRUNCATION = -20,
    kafka_common_ErrorCode_CONSUMER_NO_OFFSET_FOR_PARTITION = -21,
    kafka_common_ErrorCode_CONSUMER_OFFSET_OUT_OF_RANGE = -22,
    kafka_common_ErrorCode_CONSUMER_RETRIABLE_COMMIT_FAILED = -23,
    kafka_common_ErrorCode_CORRELATION_ID_MISMATCH = -24,
    kafka_common_ErrorCode_INVALID_RECEIVE = -25,
    kafka_common_ErrorCode_QUOTA_VIOLATION = -26,
    kafka_common_ErrorCode_RECORD_DESERIALIZATION = -27,
    // Code owned by a superclass (`BufferExhaustedException extends TimeoutException`).
    kafka_common_ErrorCode_PRODUCER_BUFFER_EXHAUSTED = -28,
}

/// The [`kafka_common_ErrorCode_t`] of the class that owns a protocol code.
///
/// This resolves the code through
/// [`Errors::error`] — the very
/// ownership relation [`kafka_common_ErrorCode_t`] is defined by — so the two
/// halves stay consistent by construction rather than by a second hand-written
/// table.
///
/// It exists for [`Error::KafkaError`], the one variant whose code is not a
/// constant: it stores an `Errors` rather than standing for a single class.
fn code_owned_by(error: Errors) -> kafka_common_ErrorCode_t {
    match error.error() {
        // `Errors::None` is the one code no class owns — Java declares it
        // `NONE(0, null, message -> null)`.
        None => kafka_common_ErrorCode_NONE,
        // No code is owned by the bare `KafkaException`, so this arm is
        // unreachable. Spelling it out rather than letting it fall into the
        // recursive arm below is what makes that recursion provably one level
        // deep; without it a future `Errors::error()` regression would hang the
        // ownership test instead of failing it. `UNKNOWN_SERVER_ERROR` is the
        // answer a bare `KafkaException` gives anyway (`Error::kafka` builds it
        // with `Errors::UnknownServerError`).
        Some(Error::KafkaError(_)) => kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR,
        Some(owner) => error_code_of(&owner),
    }
}

/// The [`kafka_common_ErrorCode_t`] for an error, identifying its class.
///
/// This is **not** the same question as
/// [`Error::error`](crate::common::Error::error), and the two deliberately
/// differ. `Error::error` answers Java's *protocol* question — what
/// `Errors.forException` would say — so it walks the superclass chain and can
/// report a code the class does not own, or `UNKNOWN_SERVER_ERROR` for a class
/// Java gives no code at all. This function answers the *class* question for
/// callers that cannot see enum variants, so its value is unique per class.
///
/// They agree for every class that owns its code, and disagree for exactly the
/// four inheritance cases named on [`kafka_common_ErrorCode_t`]:
/// [`Error::ProducerBufferExhausted`] (Java: 7), [`Error::Authentication`],
/// [`Error::Authorization`] and [`Error::SslAuthentication`] (Java: 40).
///
/// The match is exhaustive with **no wildcard arm**, and must stay that way:
/// that is what makes adding an [`Error`] variant fail to compile until this
/// enum learns about it. A `_ =>` fallback is precisely how the two would
/// drift.
pub(crate) fn error_code_of(error: &Error) -> kafka_common_ErrorCode_t {
    match error {
        // The only non-constant arm: `Error::KafkaError` *stores* an `Errors`,
        // so it reports whatever code that value owns.
        Error::KafkaError(k) => code_owned_by(k.error()),
        Error::LocalIllegalArgument(_) => kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT,
        Error::LocalIllegalState(_) => kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE,
        Error::LocalConcurrentModification(_) => kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION,
        Error::LocalTimeout(_) => kafka_common_ErrorCode_LOCAL_TIMEOUT,
        Error::Api(_) => kafka_common_ErrorCode_API,
        Error::Authentication(_) => kafka_common_ErrorCode_AUTHENTICATION,
        Error::Authorization(_) => kafka_common_ErrorCode_AUTHORIZATION,
        Error::AuthorizerNotReady(_) => kafka_common_ErrorCode_AUTHORIZER_NOT_READY,
        Error::BrokerIdNotRegistered(_) => kafka_common_ErrorCode_BROKER_ID_NOT_REGISTERED,
        Error::BrokerNotAvailable(_) => kafka_common_ErrorCode_BROKER_NOT_AVAILABLE,
        Error::ProducerBufferExhausted(_) => kafka_common_ErrorCode_PRODUCER_BUFFER_EXHAUSTED,
        Error::ClusterAuthorization(_) => kafka_common_ErrorCode_CLUSTER_AUTHORIZATION_FAILED,
        Error::ConcurrentTransactions(_) => kafka_common_ErrorCode_CONCURRENT_TRANSACTIONS,
        Error::ControllerMoved(_) => kafka_common_ErrorCode_STALE_CONTROLLER_EPOCH,
        Error::CoordinatorLoadInProgress(_) => kafka_common_ErrorCode_COORDINATOR_LOAD_IN_PROGRESS,
        Error::CoordinatorNotAvailable(_) => kafka_common_ErrorCode_COORDINATOR_NOT_AVAILABLE,
        Error::CorrelationIdMismatch(_) => kafka_common_ErrorCode_CORRELATION_ID_MISMATCH,
        Error::CorruptRecord(_) => kafka_common_ErrorCode_CORRUPT_MESSAGE,
        Error::DelegationTokenAuthorization(_) => kafka_common_ErrorCode_DELEGATION_TOKEN_AUTHORIZATION_FAILED,
        Error::DelegationTokenDisabled(_) => kafka_common_ErrorCode_DELEGATION_TOKEN_AUTH_DISABLED,
        Error::DelegationTokenExpired(_) => kafka_common_ErrorCode_DELEGATION_TOKEN_EXPIRED,
        Error::DelegationTokenNotFound(_) => kafka_common_ErrorCode_DELEGATION_TOKEN_NOT_FOUND,
        Error::DelegationTokenOwnerMismatch(_) => kafka_common_ErrorCode_DELEGATION_TOKEN_OWNER_MISMATCH,
        Error::Disconnect(_) => kafka_common_ErrorCode_DISCONNECT,
        Error::DuplicateBrokerRegistration(_) => kafka_common_ErrorCode_DUPLICATE_BROKER_REGISTRATION,
        Error::DuplicateResource(_) => kafka_common_ErrorCode_DUPLICATE_RESOURCE,
        Error::DuplicateSequence(_) => kafka_common_ErrorCode_DUPLICATE_SEQUENCE_NUMBER,
        Error::DuplicateVoter(_) => kafka_common_ErrorCode_DUPLICATE_VOTER,
        Error::ElectionNotNeeded(_) => kafka_common_ErrorCode_ELECTION_NOT_NEEDED,
        Error::EligibleLeadersNotAvailable(_) => kafka_common_ErrorCode_ELIGIBLE_LEADERS_NOT_AVAILABLE,
        Error::FeatureUpdateFailed(_) => kafka_common_ErrorCode_FEATURE_UPDATE_FAILED,
        Error::FencedInstanceId(_) => kafka_common_ErrorCode_FENCED_INSTANCE_ID,
        Error::FencedLeaderEpoch(_) => kafka_common_ErrorCode_FENCED_LEADER_EPOCH,
        Error::FencedMemberEpoch(_) => kafka_common_ErrorCode_FENCED_MEMBER_EPOCH,
        Error::FencedStateEpoch(_) => kafka_common_ErrorCode_FENCED_STATE_EPOCH,
        Error::FetchSessionIdNotFound(_) => kafka_common_ErrorCode_FETCH_SESSION_ID_NOT_FOUND,
        Error::FetchSessionTopicId(_) => kafka_common_ErrorCode_FETCH_SESSION_TOPIC_ID_ERROR,
        Error::GroupAuthorization(_) => kafka_common_ErrorCode_GROUP_AUTHORIZATION_FAILED,
        Error::GroupIdNotFound(_) => kafka_common_ErrorCode_GROUP_ID_NOT_FOUND,
        Error::GroupMaxSizeReached(_) => kafka_common_ErrorCode_GROUP_MAX_SIZE_REACHED,
        Error::GroupNotEmpty(_) => kafka_common_ErrorCode_NON_EMPTY_GROUP,
        Error::GroupSubscribedToTopic(_) => kafka_common_ErrorCode_GROUP_SUBSCRIBED_TO_TOPIC,
        Error::IllegalGeneration(_) => kafka_common_ErrorCode_ILLEGAL_GENERATION,
        Error::IllegalSaslState(_) => kafka_common_ErrorCode_ILLEGAL_SASL_STATE,
        Error::InconsistentClusterId(_) => kafka_common_ErrorCode_INCONSISTENT_CLUSTER_ID,
        Error::InconsistentGroupProtocol(_) => kafka_common_ErrorCode_INCONSISTENT_GROUP_PROTOCOL,
        Error::InconsistentTopicId(_) => kafka_common_ErrorCode_INCONSISTENT_TOPIC_ID,
        Error::InconsistentVoterSet(_) => kafka_common_ErrorCode_INCONSISTENT_VOTER_SET,
        Error::IneligibleReplica(_) => kafka_common_ErrorCode_INELIGIBLE_REPLICA,
        Error::Interrupt(_) => kafka_common_ErrorCode_INTERRUPT,
        Error::InvalidCommitOffsetSize(_) => kafka_common_ErrorCode_INVALID_COMMIT_OFFSET_SIZE,
        Error::InvalidConfiguration(_) => kafka_common_ErrorCode_INVALID_CONFIG,
        Error::InvalidFetchSessionEpoch(_) => kafka_common_ErrorCode_INVALID_FETCH_SESSION_EPOCH,
        Error::InvalidFetchSize(_) => kafka_common_ErrorCode_INVALID_FETCH_SIZE,
        Error::InvalidGroupId(_) => kafka_common_ErrorCode_INVALID_GROUP_ID,
        Error::InvalidOffset(_) => kafka_common_ErrorCode_INVALID_OFFSET,
        Error::InvalidPartitions(_) => kafka_common_ErrorCode_INVALID_PARTITIONS,
        Error::InvalidPidMapping(_) => kafka_common_ErrorCode_INVALID_PRODUCER_ID_MAPPING,
        Error::InvalidPrincipalType(_) => kafka_common_ErrorCode_INVALID_PRINCIPAL_TYPE,
        Error::InvalidProducerEpoch(_) => kafka_common_ErrorCode_INVALID_PRODUCER_EPOCH,
        Error::InvalidRecord(_) => kafka_common_ErrorCode_INVALID_RECORD,
        Error::InvalidRecordState(_) => kafka_common_ErrorCode_INVALID_RECORD_STATE,
        Error::InvalidRegistration(_) => kafka_common_ErrorCode_INVALID_REGISTRATION,
        Error::InvalidRegularExpression(_) => kafka_common_ErrorCode_INVALID_REGULAR_EXPRESSION,
        Error::InvalidReplicaAssignment(_) => kafka_common_ErrorCode_INVALID_REPLICA_ASSIGNMENT,
        Error::InvalidReplicationFactor(_) => kafka_common_ErrorCode_INVALID_REPLICATION_FACTOR,
        Error::InvalidRequest(_) => kafka_common_ErrorCode_INVALID_REQUEST,
        Error::InvalidRequiredAcks(_) => kafka_common_ErrorCode_INVALID_REQUIRED_ACKS,
        Error::InvalidSessionTimeout(_) => kafka_common_ErrorCode_INVALID_SESSION_TIMEOUT,
        Error::InvalidShareSessionEpoch(_) => kafka_common_ErrorCode_INVALID_SHARE_SESSION_EPOCH,
        Error::InvalidTimestamp(_) => kafka_common_ErrorCode_INVALID_TIMESTAMP,
        Error::InvalidTopic(_) => kafka_common_ErrorCode_INVALID_TOPIC_ERROR,
        Error::InvalidTxnState(_) => kafka_common_ErrorCode_INVALID_TXN_STATE,
        Error::InvalidTxnTimeout(_) => kafka_common_ErrorCode_INVALID_TRANSACTION_TIMEOUT,
        Error::InvalidUpdateVersion(_) => kafka_common_ErrorCode_INVALID_UPDATE_VERSION,
        Error::InvalidVoterKey(_) => kafka_common_ErrorCode_INVALID_VOTER_KEY,
        Error::KafkaStorage(_) => kafka_common_ErrorCode_KAFKA_STORAGE_ERROR,
        Error::LeaderNotAvailable(_) => kafka_common_ErrorCode_LEADER_NOT_AVAILABLE,
        Error::ListenerNotFound(_) => kafka_common_ErrorCode_LISTENER_NOT_FOUND,
        Error::LogDirNotFound(_) => kafka_common_ErrorCode_LOG_DIR_NOT_FOUND,
        Error::MemberIdRequired(_) => kafka_common_ErrorCode_MEMBER_ID_REQUIRED,
        Error::MismatchedEndpointType(_) => kafka_common_ErrorCode_MISMATCHED_ENDPOINT_TYPE,
        Error::Network(_) => kafka_common_ErrorCode_NETWORK_ERROR,
        Error::NewLeaderElected(_) => kafka_common_ErrorCode_NEW_LEADER_ELECTED,
        Error::NoReassignmentInProgress(_) => kafka_common_ErrorCode_NO_REASSIGNMENT_IN_PROGRESS,
        Error::NotController(_) => kafka_common_ErrorCode_NOT_CONTROLLER,
        Error::NotCoordinator(_) => kafka_common_ErrorCode_NOT_COORDINATOR,
        Error::NotEnoughReplicas(_) => kafka_common_ErrorCode_NOT_ENOUGH_REPLICAS,
        Error::NotEnoughReplicasAfterAppend(_) => kafka_common_ErrorCode_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        Error::NotLeaderOrFollower(_) => kafka_common_ErrorCode_NOT_LEADER_OR_FOLLOWER,
        Error::OffsetMetadataTooLarge(_) => kafka_common_ErrorCode_OFFSET_METADATA_TOO_LARGE,
        Error::OffsetMovedToTieredStorage(_) => kafka_common_ErrorCode_OFFSET_MOVED_TO_TIERED_STORAGE,
        Error::OffsetNotAvailable(_) => kafka_common_ErrorCode_OFFSET_NOT_AVAILABLE,
        Error::OffsetOutOfRange(_) => kafka_common_ErrorCode_OFFSET_OUT_OF_RANGE,
        Error::OperationNotAttempted(_) => kafka_common_ErrorCode_OPERATION_NOT_ATTEMPTED,
        Error::OutOfOrderSequence(_) => kafka_common_ErrorCode_OUT_OF_ORDER_SEQUENCE_NUMBER,
        Error::PolicyViolation(_) => kafka_common_ErrorCode_POLICY_VIOLATION,
        Error::PositionOutOfRange(_) => kafka_common_ErrorCode_POSITION_OUT_OF_RANGE,
        Error::PreferredLeaderNotAvailable(_) => kafka_common_ErrorCode_PREFERRED_LEADER_NOT_AVAILABLE,
        Error::PrincipalDeserialization(_) => kafka_common_ErrorCode_PRINCIPAL_DESERIALIZATION_FAILURE,
        Error::ProducerFenced(_) => kafka_common_ErrorCode_PRODUCER_FENCED,
        Error::QuotaViolation(_) => kafka_common_ErrorCode_QUOTA_VIOLATION,
        Error::ReassignmentInProgress(_) => kafka_common_ErrorCode_REASSIGNMENT_IN_PROGRESS,
        Error::RebalanceInProgress(_) => kafka_common_ErrorCode_REBALANCE_IN_PROGRESS,
        Error::RebootstrapRequired(_) => kafka_common_ErrorCode_REBOOTSTRAP_REQUIRED,
        Error::RecordBatchTooLarge(_) => kafka_common_ErrorCode_RECORD_LIST_TOO_LARGE,
        Error::RecordDeserialization(_) => kafka_common_ErrorCode_RECORD_DESERIALIZATION,
        Error::RecordTooLarge(_) => kafka_common_ErrorCode_MESSAGE_TOO_LARGE,
        Error::InvalidReceive(_) => kafka_common_ErrorCode_INVALID_RECEIVE,
        Error::Config(_) => kafka_common_ErrorCode_CONFIG,
        Error::ConsumerRetriableCommitFailed(_) => kafka_common_ErrorCode_CONSUMER_RETRIABLE_COMMIT_FAILED,
        Error::ConsumerCommitFailed(_) => kafka_common_ErrorCode_CONSUMER_COMMIT_FAILED,
        Error::ConsumerNoOffsetForPartition(_) => kafka_common_ErrorCode_CONSUMER_NO_OFFSET_FOR_PARTITION,
        Error::ConsumerOffsetOutOfRange(_) => kafka_common_ErrorCode_CONSUMER_OFFSET_OUT_OF_RANGE,
        Error::ConsumerLogTruncation(_) => kafka_common_ErrorCode_CONSUMER_LOG_TRUNCATION,
        Error::ReplicaNotAvailable(_) => kafka_common_ErrorCode_REPLICA_NOT_AVAILABLE,
        Error::ResourceNotFound(_) => kafka_common_ErrorCode_RESOURCE_NOT_FOUND,
        Error::SaslAuthentication(_) => kafka_common_ErrorCode_SASL_AUTHENTICATION_FAILED,
        Error::Schema(_) => kafka_common_ErrorCode_SCHEMA,
        Error::SecurityDisabled(_) => kafka_common_ErrorCode_SECURITY_DISABLED,
        Error::Serialization(_) => kafka_common_ErrorCode_SERIALIZATION,
        Error::ShareSessionLimitReached(_) => kafka_common_ErrorCode_SHARE_SESSION_LIMIT_REACHED,
        Error::ShareSessionNotFound(_) => kafka_common_ErrorCode_SHARE_SESSION_NOT_FOUND,
        Error::SnapshotNotFound(_) => kafka_common_ErrorCode_SNAPSHOT_NOT_FOUND,
        Error::SslAuthentication(_) => kafka_common_ErrorCode_SSL_AUTHENTICATION,
        Error::StaleBrokerEpoch(_) => kafka_common_ErrorCode_STALE_BROKER_EPOCH,
        Error::StaleMemberEpoch(_) => kafka_common_ErrorCode_STALE_MEMBER_EPOCH,
        Error::StreamsInvalidTopology(_) => kafka_common_ErrorCode_STREAMS_INVALID_TOPOLOGY,
        Error::StreamsInvalidTopologyEpoch(_) => kafka_common_ErrorCode_STREAMS_INVALID_TOPOLOGY_EPOCH,
        Error::StreamsTopologyFenced(_) => kafka_common_ErrorCode_STREAMS_TOPOLOGY_FENCED,
        Error::TelemetryTooLarge(_) => kafka_common_ErrorCode_TELEMETRY_TOO_LARGE,
        Error::ThrottlingQuotaExceeded(_) => kafka_common_ErrorCode_THROTTLING_QUOTA_EXCEEDED,
        Error::Timeout(_) => kafka_common_ErrorCode_REQUEST_TIMED_OUT,
        Error::TopicAuthorization(_) => kafka_common_ErrorCode_TOPIC_AUTHORIZATION_FAILED,
        Error::TopicDeletionDisabled(_) => kafka_common_ErrorCode_TOPIC_DELETION_DISABLED,
        Error::TopicExists(_) => kafka_common_ErrorCode_TOPIC_ALREADY_EXISTS,
        Error::TransactionAbortable(_) => kafka_common_ErrorCode_TRANSACTION_ABORTABLE,
        Error::TransactionAborted(_) => kafka_common_ErrorCode_TRANSACTION_ABORTED,
        Error::TransactionCoordinatorFenced(_) => kafka_common_ErrorCode_TRANSACTION_COORDINATOR_FENCED,
        Error::TransactionalIdAuthorization(_) => kafka_common_ErrorCode_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        Error::TransactionalIdNotFound(_) => kafka_common_ErrorCode_TRANSACTIONAL_ID_NOT_FOUND,
        Error::UnacceptableCredential(_) => kafka_common_ErrorCode_UNACCEPTABLE_CREDENTIAL,
        Error::UnknownControllerId(_) => kafka_common_ErrorCode_UNKNOWN_CONTROLLER_ID,
        Error::UnknownLeaderEpoch(_) => kafka_common_ErrorCode_UNKNOWN_LEADER_EPOCH,
        Error::UnknownMemberId(_) => kafka_common_ErrorCode_UNKNOWN_MEMBER_ID,
        Error::UnknownProducerId(_) => kafka_common_ErrorCode_UNKNOWN_PRODUCER_ID,
        Error::UnknownServer(_) => kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR,
        Error::UnknownSubscriptionId(_) => kafka_common_ErrorCode_UNKNOWN_SUBSCRIPTION_ID,
        Error::UnknownTopicId(_) => kafka_common_ErrorCode_UNKNOWN_TOPIC_ID,
        Error::UnknownTopicOrPartition(_) => kafka_common_ErrorCode_UNKNOWN_TOPIC_OR_PARTITION,
        Error::UnreleasedInstanceId(_) => kafka_common_ErrorCode_UNRELEASED_INSTANCE_ID,
        Error::UnstableOffsetCommit(_) => kafka_common_ErrorCode_UNSTABLE_OFFSET_COMMIT,
        Error::UnsupportedAssignor(_) => kafka_common_ErrorCode_UNSUPPORTED_ASSIGNOR,
        Error::UnsupportedByAuthentication(_) => kafka_common_ErrorCode_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        Error::UnsupportedCompressionType(_) => kafka_common_ErrorCode_UNSUPPORTED_COMPRESSION_TYPE,
        Error::UnsupportedEndpointType(_) => kafka_common_ErrorCode_UNSUPPORTED_ENDPOINT_TYPE,
        Error::UnsupportedForMessageFormat(_) => kafka_common_ErrorCode_UNSUPPORTED_FOR_MESSAGE_FORMAT,
        Error::UnsupportedSaslMechanism(_) => kafka_common_ErrorCode_UNSUPPORTED_SASL_MECHANISM,
        Error::UnsupportedVersion(_) => kafka_common_ErrorCode_UNSUPPORTED_VERSION,
        Error::VoterNotFound(_) => kafka_common_ErrorCode_VOTER_NOT_FOUND,
        Error::Wakeup(_) => kafka_common_ErrorCode_WAKEUP,
    }
}

/// Returns the error code from a [`kafka_common_Error_t`] handle.
///
/// The value identifies the error's **class**, not merely its protocol code:
/// the codes are pairwise distinct, so a `switch` on this value is enough to
/// tell any two errors apart. See [`kafka_common_ErrorCode_t`] for the
/// assignment rule and for the four classes whose value deliberately differs
/// from the code Java's `Errors.forException` would report.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// The error's [`kafka_common_ErrorCode_t`], or
/// `kafka_common_ErrorCode_NONE` (`0`) if the error handle is null. The
/// enumerators have explicit values that fit an `int`, so a C caller that was
/// comparing the previous `int32_t` return against integers keeps compiling
/// and keeps getting the same answers for every broker-reported code.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_code(error: *const kafka_common_Error_t) -> kafka_common_ErrorCode_t {
    if error.is_null() {
        return kafka_common_ErrorCode_NONE;
    }
    error_code_of(&unsafe { error_ref(error) }.error)
}

/// Returns the error message as a null-terminated C string.
///
/// The returned pointer is valid until [`kafka_common_Error_destroy`] is called on
/// the same handle.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// A `*const c_char` pointing to the error message, or null if the error
/// handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
/// The returned pointer must not be used after the error is destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_message(error: *const kafka_common_Error_t) -> *const c_char {
    if error.is_null() {
        return std::ptr::null();
    }
    unsafe { error_ref(error) }.message_cstring.as_ptr()
}

/// Returns whether the error is retriable.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the error is retriable, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_retriable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_retriable_error()
}

// NOTE: fatality (`RequestUtils.isFatalException`) is deliberately NOT exported
// here. `org.apache.kafka.common.requests` carries the package disclaimer "This
// package is not a supported Kafka API; the implementation may change without
// warning between minor or patch releases", and CLAUDE.md §3 forbids C bindings
// for such packages. A C caller that needs the classification composes it from
// the exported predicates (`kafka_common_Error_is_authentication_error`,
// `kafka_common_Error_is_authorization_error`) and the error code.

/// Returns whether this is a Kafka error rather than a generic programming
/// error.
///
/// `true` for errors originating from Kafka — the protocol error codes, plus
/// serialization and wakeup. `false` for errors raised by misuse of the client
/// itself: an invalid argument, an illegal state, or concurrent access from
/// more than one thread. Mirrors Java's `t instanceof KafkaException` test,
/// which separates Kafka's own exception hierarchy from the generic
/// `java.lang` / `java.util` runtime exceptions beside it.
///
/// Note this is NOT a test for a specific error kind: most errors are Kafka
/// errors. Use [`kafka_common_Error_code`] to identify a particular one.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the error came from Kafka, `false` if it is a generic
/// programming error or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_kafka_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_kafka_error()
}

/// Returns whether the error's Java exception extends `ApiException` — an error the broker can report over the protocol, as opposed to a client-side programming or serialization failure.
///
/// Mirrors `Error::is_api_error` — see CLAUDE.md §10.4. Exposed because C cannot see
/// enum variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_api_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_api_error()
}

/// Returns whether the error is retriable AND a metadata/coordinator refresh is what clears it (Java `RefreshRetriableException`).
///
/// Mirrors `Error::is_refresh_retriable_error` — see CLAUDE.md §10.4. Exposed because C cannot see
/// enum variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_refresh_retriable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_refresh_retriable_error()
}

/// Returns whether the error means the client's cached metadata may be stale (Java `InvalidMetadataException`).
///
/// Mirrors `Error::is_invalid_metadata_error` — see CLAUDE.md §10.4. Exposed because C cannot see
/// enum variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_metadata_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_metadata_error()
}

/// Returns whether the error is an authentication failure reported by the broker (Java `AuthenticationException`).
///
/// Mirrors `Error::is_authentication_error` — see CLAUDE.md §10.4. Exposed because C cannot see
/// enum variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authentication_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_authentication_error()
}

/// Returns whether the error is an authorization failure — a missing ACL (Java `AuthorizationException`).
///
/// Mirrors `Error::is_authorization_error` — see CLAUDE.md §10.4. Exposed because C cannot see
/// enum variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authorization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_authorization_error()
}

/// Returns whether the error's Java class extends `InvalidConfigurationException` — the parent of both the authentication and
/// authorization families, so a broad classification a C caller cannot make from
/// the numeric code alone.
///
/// Mirrors `Error::is_invalid_configuration_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_configuration_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_configuration_error()
}

/// Returns whether the error's Java class extends `ApplicationRecoverableException` — recoverable by re-initialising the
/// producer or rejoining the group.
///
/// Mirrors `Error::is_application_recoverable_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_application_recoverable_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_application_recoverable_error()
}

/// Returns whether the error's Java class extends `InvalidOffsetException` (common.errors).
///
/// Mirrors `Error::is_invalid_offset_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_offset_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_invalid_offset_error()
}

/// Returns whether the error's Java class extends `OutOfOrderSequenceException`.
///
/// Mirrors `Error::is_out_of_order_sequence_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_out_of_order_sequence_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_out_of_order_sequence_error()
}

/// Returns whether the error's Java class extends `SerializationException`.
///
/// Mirrors `Error::is_serialization_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_serialization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_serialization_error()
}

/// Returns whether the error's Java class extends `TimeoutException` (also covers `BufferExhaustedException`).
///
/// Mirrors `Error::is_timeout_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_timeout_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_timeout_error()
}

/// Returns whether the error's Java class extends the consumer package's `InvalidOffsetException`.
///
/// Mirrors `Error::is_consumer_invalid_offset_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_invalid_offset_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_invalid_offset_error()
}

/// Returns whether the error's Java class extends the consumer package's `OffsetOutOfRangeException` (also covers
/// `LogTruncationException`).
///
/// Mirrors `Error::is_consumer_offset_out_of_range_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_offset_out_of_range_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_consumer_offset_out_of_range_error()
}

/// Returns whether the error's Java class extends `TransactionAbortableException` — the transaction may be aborted and retried.
///
/// Mirrors `Error::is_transaction_abortable_error` (CLAUDE.md §3/§10.4). Exposed because C cannot see enum
/// variants, so predicates are the only way a C caller classifies an error
/// beyond its numeric code.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the predicate holds, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_abortable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_transaction_abortable_error()
}

/// Destroys an error handle, freeing all associated resources.
///
/// Safe to call with a null pointer (no-op).
///
/// # Safety
///
/// - `error` must be null or a valid handle from a function that returned an error.
/// - After this call, the pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_destroy(error: *mut kafka_common_Error_t) {
    if !error.is_null() {
        unsafe {
            drop(Box::from_raw(error as *mut ErrorInner));
        }
    }
}

// ---------------------------------------------------------------------------
// Per-variant payload accessors (CLAUDE.md §3: "Exceptions having additional
// fields in Java")
//
// Of the ~156 `Error` variants, the 13 below carry state beyond
// `message`/`source` — each gets an opaque `kafka_common_Error_<Variant>_t`,
// retrieved from a `kafka_common_Error_t` via `kafka_common_Error_<Variant>`,
// which returns null if the handle is not that variant. The returned pointer
// is a *borrowed* view into the same `ErrorInner` allocation — valid until
// `kafka_common_Error_destroy` is called on the parent handle, same as
// `kafka_common_Error_message` above.
//
// Collection-valued fields reuse the existing `kafka_consumer_*` collection
// handles verbatim (`StringList`, `TopicPartitionList`, `LongOffsetMap`,
// `OffsetMap`) rather than introducing new ones — `kafka_common_Error_t`
// already crosses into `kafka_consumer_*`/`kafka_producer_*` signatures
// throughout the FFI layer. Those accessors build the collection fresh on
// each call and return an *owned* handle, which the caller destroys with the
// matching `kafka_consumer_*_destroy`, independent of the parent error's
// lifetime — the same contract `subscription()`/`assignment()`/
// `beginning_offsets()` already have.
// ---------------------------------------------------------------------------

use crate::common::header::Header;
use crate::ffi::consumer::{
    box_long_offset_map, box_offset_map, box_string_list, box_topic_partition, box_topic_partition_list,
    kafka_consumer_LongOffsetMap_t, kafka_consumer_OffsetMap_t, kafka_consumer_StringList_t,
    kafka_consumer_TopicPartition_t, kafka_consumer_TopicPartitionList_t,
};

/// `TopicAuthorizationException` -> `kafka_common_TopicAuthorizationError_t`.
#[repr(C)]
pub struct kafka_common_TopicAuthorizationError_t {
    _private: [u8; 0],
}

/// Returns the error's `TopicAuthorizationException` payload, or null if the
/// error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_TopicAuthorization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_TopicAuthorizationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::TopicAuthorization(e) => e as *const _ as *const kafka_common_TopicAuthorizationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the set of unauthorized topics, as an owned handle the caller must
/// destroy with [`kafka_consumer_StringList_destroy`](crate::ffi::consumer::kafka_consumer_StringList_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_TopicAuthorizationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_unauthorized_topics(
    handle: *const kafka_common_TopicAuthorizationError_t,
) -> *mut kafka_consumer_StringList_t {
    let e = unsafe { &*(handle as *const crate::common::errors::TopicAuthorizationError) };
    box_string_list(e.unauthorized_topics().iter().cloned())
}

/// `GroupAuthorizationException` -> `kafka_common_GroupAuthorizationError_t`.
#[repr(C)]
pub struct kafka_common_GroupAuthorizationError_t {
    _private: [u8; 0],
}

/// Returns the error's `GroupAuthorizationException` payload, or null if the
/// error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_GroupAuthorization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_GroupAuthorizationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::GroupAuthorization(e) => e as *const _ as *const kafka_common_GroupAuthorizationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the offending group id as an owned, NUL-terminated C string. The
/// caller must free it with [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_GroupAuthorizationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_group_id(
    handle: *const kafka_common_GroupAuthorizationError_t,
) -> *mut c_char {
    let e = unsafe { &*(handle as *const crate::common::errors::GroupAuthorizationError) };
    CString::new(e.group_id()).unwrap_or_default().into_raw()
}

/// `InvalidTopicException` -> `kafka_common_InvalidTopicError_t`.
#[repr(C)]
pub struct kafka_common_InvalidTopicError_t {
    _private: [u8; 0],
}

/// Returns the error's `InvalidTopicException` payload, or null if the error
/// is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_InvalidTopic(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_InvalidTopicError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::InvalidTopic(e) => e as *const _ as *const kafka_common_InvalidTopicError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the set of invalid topics, as an owned handle the caller must
/// destroy with [`kafka_consumer_StringList_destroy`](crate::ffi::consumer::kafka_consumer_StringList_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_InvalidTopicError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_InvalidTopicError_invalid_topics(
    handle: *const kafka_common_InvalidTopicError_t,
) -> *mut kafka_consumer_StringList_t {
    let e = unsafe { &*(handle as *const crate::common::errors::InvalidTopicError) };
    box_string_list(e.invalid_topics().iter().cloned())
}

/// `DuplicateResourceException` -> `kafka_common_DuplicateResourceError_t`.
#[repr(C)]
pub struct kafka_common_DuplicateResourceError_t {
    _private: [u8; 0],
}

/// Returns the error's `DuplicateResourceException` payload, or null if the
/// error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_DuplicateResource(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_DuplicateResourceError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::DuplicateResource(e) => e as *const _ as *const kafka_common_DuplicateResourceError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the offending resource name as an owned, NUL-terminated C string,
/// or null if not recorded. The caller must free a non-null result with
/// [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_DuplicateResourceError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_resource(
    handle: *const kafka_common_DuplicateResourceError_t,
) -> *mut c_char {
    let e = unsafe { &*(handle as *const crate::common::errors::DuplicateResourceError) };
    match e.resource() {
        Some(r) => CString::new(r).unwrap_or_default().into_raw(),
        None => std::ptr::null_mut(),
    }
}

/// `ResourceNotFoundException` -> `kafka_common_ResourceNotFoundError_t`.
#[repr(C)]
pub struct kafka_common_ResourceNotFoundError_t {
    _private: [u8; 0],
}

/// Returns the error's `ResourceNotFoundException` payload, or null if the
/// error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_ResourceNotFound(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ResourceNotFoundError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::ResourceNotFound(e) => e as *const _ as *const kafka_common_ResourceNotFoundError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the missing resource name as an owned, NUL-terminated C string, or
/// null if not recorded. The caller must free a non-null result with
/// [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ResourceNotFoundError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_resource(
    handle: *const kafka_common_ResourceNotFoundError_t,
) -> *mut c_char {
    let e = unsafe { &*(handle as *const crate::common::errors::ResourceNotFoundError) };
    match e.resource() {
        Some(r) => CString::new(r).unwrap_or_default().into_raw(),
        None => std::ptr::null_mut(),
    }
}

/// `ThrottlingQuotaExceededException` -> `kafka_common_ThrottlingQuotaExceededError_t`.
#[repr(C)]
pub struct kafka_common_ThrottlingQuotaExceededError_t {
    _private: [u8; 0],
}

/// Returns the error's `ThrottlingQuotaExceededException` payload, or null if
/// the error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_ThrottlingQuotaExceeded(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ThrottlingQuotaExceededError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::ThrottlingQuotaExceeded(e) => e as *const _ as *const kafka_common_ThrottlingQuotaExceededError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the throttle time in milliseconds.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ThrottlingQuotaExceededError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(
    handle: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> i32 {
    let e = unsafe { &*(handle as *const crate::common::errors::ThrottlingQuotaExceededError) };
    e.throttle_time_ms()
}

/// `CorrelationIdMismatchException` -> `kafka_common_CorrelationIdMismatchError_t`.
#[repr(C)]
pub struct kafka_common_CorrelationIdMismatchError_t {
    _private: [u8; 0],
}

/// Returns the error's `CorrelationIdMismatchException` payload, or null if
/// the error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_CorrelationIdMismatch(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_CorrelationIdMismatchError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::CorrelationIdMismatch(e) => e as *const _ as *const kafka_common_CorrelationIdMismatchError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the correlation id the request was sent with.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_CorrelationIdMismatchError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_CorrelationIdMismatchError_request_correlation_id(
    handle: *const kafka_common_CorrelationIdMismatchError_t,
) -> i32 {
    let e = unsafe { &*(handle as *const crate::common::requests::CorrelationIdMismatchError) };
    e.request_correlation_id()
}

/// Returns the correlation id found on the response.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_CorrelationIdMismatchError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_CorrelationIdMismatchError_response_correlation_id(
    handle: *const kafka_common_CorrelationIdMismatchError_t,
) -> i32 {
    let e = unsafe { &*(handle as *const crate::common::requests::CorrelationIdMismatchError) };
    e.response_correlation_id()
}

/// `RecordDeserializationException` -> `kafka_common_RecordDeserializationError_t`.
#[repr(C)]
pub struct kafka_common_RecordDeserializationError_t {
    _private: [u8; 0],
}

/// Returns the error's `RecordDeserializationException` payload, or null if
/// the error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_RecordDeserialization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordDeserializationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::RecordDeserialization(e) => e.as_ref() as *const _ as *const kafka_common_RecordDeserializationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns which side of the record failed to deserialize
/// (`0` = key, `1` = value), writing it to `*out_origin` and returning `true`
/// if recorded; returns `false` (leaving `*out_origin` untouched) if absent —
/// Java's deprecated four-argument constructor does not record it.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_origin` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_origin(
    handle: *const kafka_common_RecordDeserializationError_t,
    out_origin: *mut i32,
) -> bool {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    match e.origin() {
        Some(origin) => {
            if !out_origin.is_null() {
                let id = match origin {
                    crate::common::errors::DeserializationErrorOrigin::Key => 0,
                    crate::common::errors::DeserializationErrorOrigin::Value => 1,
                };
                unsafe { *out_origin = id };
            }
            true
        },
        None => false,
    }
}

/// Returns the partition of the offending record, as an owned handle the
/// caller must destroy with [`kafka_consumer_TopicPartition_destroy`](crate::ffi::consumer::kafka_consumer_TopicPartition_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_partition(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> *mut kafka_consumer_TopicPartition_t {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    box_topic_partition(e.topic_partition().clone())
}

/// Returns the offset of the offending record.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_offset(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i64 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    e.offset()
}

/// Returns the timestamp of the offending record.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_timestamp(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i64 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    e.timestamp()
}

/// Returns the timestamp type of the offending record as its numeric id
/// (`-1` = NoTimestampType, `0` = CreateTime, `1` = LogAppendTime), matching
/// [`kafka_consumer_ConsumerRecord_timestamp_type`](crate::ffi::consumer::kafka_consumer_ConsumerRecord_timestamp_type).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_timestamp_type(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i32 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    e.timestamp_type().id()
}

/// Returns the raw key bytes of the offending record as a (ptr, len) pair, or
/// (null, -1) if absent. The pointer is borrowed and valid until the parent
/// error is destroyed.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_key_buffer(
    handle: *const kafka_common_RecordDeserializationError_t,
    out_len: *mut i32,
) -> *const u8 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    match e.key_buffer() {
        Some(k) => {
            if !out_len.is_null() {
                unsafe { *out_len = k.len() as i32 };
            }
            k.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the raw value bytes of the offending record as a (ptr, len) pair,
/// or (null, -1) if absent. The pointer is borrowed and valid until the
/// parent error is destroyed.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_value_buffer(
    handle: *const kafka_common_RecordDeserializationError_t,
    out_len: *mut i32,
) -> *const u8 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    match e.value_buffer() {
        Some(v) => {
            if !out_len.is_null() {
                unsafe { *out_len = v.len() as i32 };
            }
            v.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the number of headers attached to the offending record (`0` if
/// there are none recorded).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_header_count(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i32 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    e.headers().into_iter().flatten().count() as i32
}

/// Returns the key of the header at `index` (insertion order) as a (ptr, len)
/// pair (NOT NUL-terminated), or (null, -1) if out of range.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_header_key(
    handle: *const kafka_common_RecordDeserializationError_t,
    index: i32,
    out_len: *mut i32,
) -> *const c_char {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    let header = if index < 0 {
        None
    } else {
        e.headers().into_iter().flatten().nth(index as usize)
    };
    match header {
        Some(header) => {
            let key = header.key();
            if !out_len.is_null() {
                unsafe { *out_len = key.len() as i32 };
            }
            key.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the value of the header at `index` as a (ptr, len) pair, or
/// (null, -1) if out of range or the header value is null.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_len` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_header_value(
    handle: *const kafka_common_RecordDeserializationError_t,
    index: i32,
    out_len: *mut i32,
) -> *const u8 {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    let header = if index < 0 {
        None
    } else {
        e.headers().into_iter().flatten().nth(index as usize)
    };
    match header.and_then(|h| h.value()) {
        Some(value) => {
            if !out_len.is_null() {
                unsafe { *out_len = value.len() as i32 };
            }
            value.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// `QuotaViolationException` -> `kafka_common_QuotaViolationError_t`.
#[repr(C)]
pub struct kafka_common_QuotaViolationError_t {
    _private: [u8; 0],
}

/// Returns the error's `QuotaViolationException` payload, or null if the
/// error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_QuotaViolation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_QuotaViolationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::QuotaViolation(e) => e.as_ref() as *const _ as *const kafka_common_QuotaViolationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the metric's name as an owned, NUL-terminated C string. The caller
/// must free it with [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// Only `name`/`group` are exposed — Java's own `toString()` uses only
/// `metric.metricName()`, and the client has no caller of `metric()`.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_metric_name(
    handle: *const kafka_common_QuotaViolationError_t,
) -> *mut c_char {
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    CString::new(e.metric_name().name()).unwrap_or_default().into_raw()
}

/// Returns the metric's group as an owned, NUL-terminated C string. The
/// caller must free it with [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_metric_group(
    handle: *const kafka_common_QuotaViolationError_t,
) -> *mut c_char {
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    CString::new(e.metric_name().group()).unwrap_or_default().into_raw()
}

/// Returns the recorded value that violated the quota.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_value(
    handle: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    e.value()
}

/// Returns the configured bound the value violated.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_bound(
    handle: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    e.bound()
}

/// `LogTruncationException` -> `kafka_common_ConsumerLogTruncationError_t`.
#[repr(C)]
pub struct kafka_common_ConsumerLogTruncationError_t {
    _private: [u8; 0],
}

/// Returns the error's `LogTruncationException` payload, or null if the error
/// is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_ConsumerLogTruncation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerLogTruncationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::ConsumerLogTruncation(e) => e.as_ref() as *const _ as *const kafka_common_ConsumerLogTruncationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the out-of-range offset per partition, as an owned handle the
/// caller must destroy with [`kafka_consumer_LongOffsetMap_destroy`](crate::ffi::consumer::kafka_consumer_LongOffsetMap_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ConsumerLogTruncationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions(
    handle: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_consumer_LongOffsetMap_t {
    let e = unsafe { &*(handle as *const crate::consumer::ConsumerLogTruncationError) };
    box_long_offset_map(e.offset_out_of_range_partitions().clone())
}

/// Returns the divergent offset per partition, as an owned handle the caller
/// must destroy with [`kafka_consumer_OffsetMap_destroy`](crate::ffi::consumer::kafka_consumer_OffsetMap_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ConsumerLogTruncationError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_divergent_offsets(
    handle: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_consumer_OffsetMap_t {
    let e = unsafe { &*(handle as *const crate::consumer::ConsumerLogTruncationError) };
    box_offset_map(e.divergent_offsets().clone())
}

/// `NoOffsetForPartitionException` -> `kafka_common_ConsumerNoOffsetForPartitionError_t`.
#[repr(C)]
pub struct kafka_common_ConsumerNoOffsetForPartitionError_t {
    _private: [u8; 0],
}

/// Returns the error's `NoOffsetForPartitionException` payload, or null if
/// the error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_ConsumerNoOffsetForPartition(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerNoOffsetForPartitionError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::ConsumerNoOffsetForPartition(e) => {
            e as *const _ as *const kafka_common_ConsumerNoOffsetForPartitionError_t
        },
        _ => std::ptr::null(),
    }
}

/// Returns the partitions with no defined offset and no reset policy, as an
/// owned handle the caller must destroy with
/// [`kafka_consumer_TopicPartitionList_destroy`](crate::ffi::consumer::kafka_consumer_TopicPartitionList_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ConsumerNoOffsetForPartitionError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_partitions(
    handle: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *mut kafka_consumer_TopicPartitionList_t {
    let e = unsafe { &*(handle as *const crate::consumer::ConsumerNoOffsetForPartitionError) };
    box_topic_partition_list(e.partitions().iter().cloned())
}

/// Consumer-package `OffsetOutOfRangeException` -> `kafka_common_ConsumerOffsetOutOfRangeError_t`.
#[repr(C)]
pub struct kafka_common_ConsumerOffsetOutOfRangeError_t {
    _private: [u8; 0],
}

/// Returns the error's consumer-package `OffsetOutOfRangeException` payload,
/// or null if the error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_ConsumerOffsetOutOfRange(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerOffsetOutOfRangeError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::ConsumerOffsetOutOfRange(e) => e as *const _ as *const kafka_common_ConsumerOffsetOutOfRangeError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the out-of-range offset per partition, as an owned handle the
/// caller must destroy with [`kafka_consumer_LongOffsetMap_destroy`](crate::ffi::consumer::kafka_consumer_LongOffsetMap_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ConsumerOffsetOutOfRangeError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(
    handle: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *mut kafka_consumer_LongOffsetMap_t {
    let e = unsafe { &*(handle as *const crate::consumer::ConsumerOffsetOutOfRangeError) };
    box_long_offset_map(e.offset_out_of_range_partitions().clone())
}

/// `RecordTooLargeException` -> `kafka_common_RecordTooLargeError_t`.
#[repr(C)]
pub struct kafka_common_RecordTooLargeError_t {
    _private: [u8; 0],
}

/// Returns the error's `RecordTooLargeException` payload, or null if the
/// error is not that variant.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_RecordTooLarge(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordTooLargeError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    match &unsafe { error_ref(error) }.error {
        Error::RecordTooLarge(e) => e as *const _ as *const kafka_common_RecordTooLargeError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the per-partition record size that exceeded the limit, as an
/// owned handle the caller must destroy with
/// [`kafka_consumer_LongOffsetMap_destroy`](crate::ffi::consumer::kafka_consumer_LongOffsetMap_destroy),
/// or null if Java's field is `null` (the constructor that does not record
/// per-partition sizes was used).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordTooLargeError_t`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_record_too_large_partitions(
    handle: *const kafka_common_RecordTooLargeError_t,
) -> *mut kafka_consumer_LongOffsetMap_t {
    let e = unsafe { &*(handle as *const crate::common::errors::RecordTooLargeError) };
    match e.record_too_large_partitions() {
        Some(partitions) => box_long_offset_map(partitions.clone()),
        None => std::ptr::null_mut(),
    }
}

// ---------------------------------------------------------------------------
// Async (callback-based) delivery machinery
// ---------------------------------------------------------------------------
//
// The async API mirrors the librdkafka delivery-report model: each operation
// returns immediately and its result is delivered later through a C callback.
// All callbacks are invoked from a single per-handle **dispatcher thread**
// that drains a completion queue, so user callbacks run on one predictable
// thread and never on a tokio worker (a slow callback cannot stall I/O).

/// A unit of work executed by the dispatcher thread. Each async operation
/// captures its own C callback, `user_data`, and owned result handles into the
/// closure and bakes in the correct invocation, so the queue stays uniform
/// (one element type) while every operation delivers exactly the outputs its
/// sync counterpart produces.
pub(crate) type CompletionJob = Box<dyn FnOnce() + Send>;

/// Spawns a dispatcher thread that drains the completion queue, running each
/// queued [`CompletionJob`] in order. The thread exits once all senders are
/// dropped (after draining any queued jobs).
///
/// Returns the sender half of the completion queue and the thread join handle.
/// The caller stores the sender on its handle (cloned into each async op) and
/// keeps the join handle for teardown.
pub(crate) fn spawn_dispatcher(name: &str) -> (std::sync::mpsc::Sender<CompletionJob>, std::thread::JoinHandle<()>) {
    let (completion_tx, completion_rx) = std::sync::mpsc::channel::<CompletionJob>();
    let dispatcher = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            // Run each completion closure; exits once all senders are dropped
            // (after draining any queued jobs).
            while let Ok(job) = completion_rx.recv() {
                job();
            }
        })
        .expect("failed to spawn FFI callback dispatcher thread");
    (completion_tx, dispatcher)
}

/// Enqueues a [`CompletionJob`] on the dispatcher's completion queue. If the
/// dispatcher is gone (post-teardown), runs the job inline to honor the
/// callback obligation rather than leak the owned handles it captured.
pub(crate) fn enqueue_or_run_inline(tx: &std::sync::mpsc::Sender<CompletionJob>, job: CompletionJob) {
    if let Err(returned) = tx.send(job) {
        (returned.0)();
    }
}

/// Canonical operation callback signature (not exported). A null `error` means
/// success. The public per-method typedefs alias this shape.
pub(crate) type OperationCallbackFn = unsafe extern "C" fn(*mut kafka_common_Error_t, *mut std::ffi::c_void);

/// Owned operation completion payload, fired by the dispatcher thread for
/// void-returning operations (`flush` / `close` / consumer void ops).
pub(crate) struct OperationCompletion {
    pub(crate) callback: OperationCallbackFn,
    pub(crate) user_data: *mut std::ffi::c_void,
    pub(crate) error: *mut kafka_common_Error_t,
}
// SAFETY: the raw pointers are owned handles moved to the dispatcher thread;
// the C user is responsible for the thread-safety of `user_data`.
unsafe impl Send for OperationCompletion {}
impl OperationCompletion {
    /// # Safety
    /// Must be called exactly once, on the dispatcher thread.
    pub(crate) unsafe fn fire(self) {
        unsafe { (self.callback)(self.error, self.user_data) };
    }
}

/// A C operation-callback target (function pointer + opaque `user_data`).
/// Wrapped so it can cross the tokio task / dispatcher thread boundary.
#[derive(Clone, Copy)]
pub(crate) struct OperationCallbackTarget {
    pub(crate) callback: OperationCallbackFn,
    pub(crate) user_data: *mut std::ffi::c_void,
}
// SAFETY: the C user owns the thread-safety of `user_data`; the function
// pointer is trivially shareable.
unsafe impl Send for OperationCallbackTarget {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::errors::{
        ApiError, AuthenticationError, AuthorizationError, AuthorizerNotReadyError, DisconnectError,
        DuplicateResourceError, GroupAuthorizationError, InterruptError, InvalidOffsetError, InvalidTopicError,
        RecordTooLargeError, ResourceNotFoundError, SslAuthenticationError, ThrottlingQuotaExceededError,
        TopicAuthorizationError,
    };
    use crate::common::kafka_error::{ErrorName, KafkaError};
    use crate::common::metrics::QuotaViolationError;
    use crate::common::network::InvalidReceiveError;
    use crate::common::requests::CorrelationIdMismatchError;
    use crate::common::{MetricName, TopicPartition};
    use crate::consumer::{
        ConsumerCommitFailedError, ConsumerLogTruncationError, ConsumerNoOffsetForPartitionError,
        ConsumerOffsetOutOfRangeError, ConsumerRetriableCommitFailedError,
    };
    use crate::ffi::consumer::{
        kafka_consumer_LongOffsetMap_count, kafka_consumer_LongOffsetMap_destroy,
        kafka_consumer_LongOffsetMap_get_value, kafka_consumer_OffsetMap_count, kafka_consumer_OffsetMap_destroy,
        kafka_consumer_StringList_count, kafka_consumer_StringList_destroy, kafka_consumer_StringList_get,
        kafka_consumer_TopicPartition_destroy, kafka_consumer_TopicPartition_partition,
        kafka_consumer_TopicPartition_topic, kafka_consumer_TopicPartitionList_count,
        kafka_consumer_TopicPartitionList_destroy, kafka_consumer_TopicPartitionList_get,
        kafka_consumer_string_destroy,
    };
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::ffi::CStr;

    /// Java's lowest and highest `Errors` codes — the full range `Errors::for_code`
    /// resolves to a named constant.
    const FIRST_CODE: i16 = -1;
    const LAST_CODE: i16 = 133;

    /// The 27 classes that own no Java code, each paired with the enumerator it
    /// must map to and that enumerator's literal value.
    ///
    /// Both halves matter. The instance checks that `error_code_of`'s arm points
    /// at the right constant; the literal checks that the constant still has the
    /// value it was published with. See [`kafka_common_ErrorCode_t`] — the
    /// negatives are ABI, so a renumbering must fail here rather than silently
    /// reach a C caller.
    fn client_side_classes() -> Vec<(Error, kafka_common_ErrorCode_t, i32)> {
        vec![
            // -2 ..= -5: JDK-derived (`Local*`).
            (
                Error::local_concurrent_modification("m"),
                kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION,
                -2,
            ),
            (
                Error::local_illegal_argument("m"),
                kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT,
                -3,
            ),
            (Error::local_illegal_state("m"), kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE, -4),
            (Error::local_timeout("m"), kafka_common_ErrorCode_LOCAL_TIMEOUT, -5),
            // -6 ..= -18: `common` client-side classes.
            (Error::Api(ApiError::new("m")), kafka_common_ErrorCode_API, -6),
            (
                Error::Authentication(AuthenticationError::new("m")),
                kafka_common_ErrorCode_AUTHENTICATION,
                -7,
            ),
            (
                Error::AuthorizerNotReady(AuthorizerNotReadyError::new("m")),
                kafka_common_ErrorCode_AUTHORIZER_NOT_READY,
                -8,
            ),
            (
                Error::Authorization(AuthorizationError::new("m")),
                kafka_common_ErrorCode_AUTHORIZATION,
                -9,
            ),
            (Error::config("m"), kafka_common_ErrorCode_CONFIG, -10),
            (
                Error::Disconnect(DisconnectError::new("m")),
                kafka_common_ErrorCode_DISCONNECT,
                -11,
            ),
            (
                Error::Interrupt(InterruptError::new("m")),
                kafka_common_ErrorCode_INTERRUPT,
                -12,
            ),
            (
                Error::InvalidOffset(InvalidOffsetError::new("m")),
                kafka_common_ErrorCode_INVALID_OFFSET,
                -13,
            ),
            (Error::schema("m"), kafka_common_ErrorCode_SCHEMA, -14),
            (Error::serialization("m"), kafka_common_ErrorCode_SERIALIZATION, -15),
            (
                Error::SslAuthentication(SslAuthenticationError::new("m")),
                kafka_common_ErrorCode_SSL_AUTHENTICATION,
                -16,
            ),
            (Error::transaction_aborted(), kafka_common_ErrorCode_TRANSACTION_ABORTED, -17),
            (Error::wakeup("m"), kafka_common_ErrorCode_WAKEUP, -18),
            // -19 ..= -27: hand-written / consumer classes.
            (
                Error::ConsumerCommitFailed(ConsumerCommitFailedError::with_default_message()),
                kafka_common_ErrorCode_CONSUMER_COMMIT_FAILED,
                -19,
            ),
            (
                Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(HashMap::new(), HashMap::new()))),
                kafka_common_ErrorCode_CONSUMER_LOG_TRUNCATION,
                -20,
            ),
            (
                Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(TopicPartition::new(
                    "t", 0,
                ))),
                kafka_common_ErrorCode_CONSUMER_NO_OFFSET_FOR_PARTITION,
                -21,
            ),
            (
                Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(HashMap::new())),
                kafka_common_ErrorCode_CONSUMER_OFFSET_OUT_OF_RANGE,
                -22,
            ),
            (
                Error::ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError::with_default_message()),
                kafka_common_ErrorCode_CONSUMER_RETRIABLE_COMMIT_FAILED,
                -23,
            ),
            (
                Error::correlation_id_mismatch("m", 1, 2),
                kafka_common_ErrorCode_CORRELATION_ID_MISMATCH,
                -24,
            ),
            (
                Error::InvalidReceive(InvalidReceiveError::new("m")),
                kafka_common_ErrorCode_INVALID_RECEIVE,
                -25,
            ),
            (
                Error::QuotaViolation(Box::new(QuotaViolationError::new(
                    MetricName::new("n", "g", "d", BTreeMap::new()),
                    1.0,
                    0.5,
                ))),
                kafka_common_ErrorCode_QUOTA_VIOLATION,
                -26,
            ),
            (
                Error::RecordDeserialization(Box::new(record_deserialization_error())),
                kafka_common_ErrorCode_RECORD_DESERIALIZATION,
                -27,
            ),
            // -28: code owned by a superclass.
            (
                Error::buffer_exhausted("m"),
                kafka_common_ErrorCode_PRODUCER_BUFFER_EXHAUSTED,
                -28,
            ),
        ]
    }

    /// Every code whose `Errors::error()` names a class reports that class's own
    /// value, and the value equals `Errors::code()`.
    ///
    /// This machine-checks the faithful half of [`kafka_common_ErrorCode_t`]
    /// against `Errors` itself, so the two cannot drift: a code renamed, renumbered
    /// or re-pointed at a different class in `Errors` fails here.
    #[test]
    fn ffi_error_code_owned_codes_match_the_java_values() {
        let mut owned = 0;
        for code in FIRST_CODE..=LAST_CODE {
            let error = Errors::for_code(code);
            assert_eq!(error.code(), code, "Errors::for_code({code}) resolved to a different code");

            match error.error() {
                // `Errors::None` is Java's `NONE(0, null, message -> null)` — the
                // one code no class owns.
                None => {
                    assert_eq!(code, 0, "only NONE may map to no class, but {code} does");
                    assert_eq!(kafka_common_ErrorCode_NONE as i32, 0);
                },
                Some(owner) => {
                    owned += 1;
                    assert_eq!(
                        error_code_of(&owner) as i32,
                        i32::from(code),
                        "{} does not report its own code",
                        error.enum_name()
                    );
                },
            }
        }
        assert_eq!(owned, 134, "expected 134 classes to own a Java code");

        // `Error::KafkaError` is the one variant whose code is not a constant: it
        // stores an `Errors`, so it reports whatever that value owns.
        assert_eq!(
            error_code_of(&Error::KafkaError(KafkaError::new(Errors::CorruptMessage))),
            kafka_common_ErrorCode_CORRUPT_MESSAGE
        );
        assert_eq!(
            error_code_of(&Error::KafkaError(KafkaError::new(Errors::None))),
            kafka_common_ErrorCode_NONE
        );
        // A bare `KafkaException` reports -1, which is what Java's
        // `Errors.forException` answers for it.
        assert_eq!(error_code_of(&Error::kafka("m")), kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR);
    }

    /// The 162 values are pairwise distinct, so the code alone identifies the
    /// class. A C caller has no other discriminator, and the gRPC test harness
    /// derives the error type from this value — a collision would silently
    /// misclassify one of the two classes involved.
    #[test]
    fn ffi_error_code_values_are_injective() {
        let mut seen: HashMap<i32, String> = HashMap::new();

        // `NONE` belongs to no class but occupies 0 in the space.
        seen.insert(kafka_common_ErrorCode_NONE as i32, "NONE".to_string());

        for code in FIRST_CODE..=LAST_CODE {
            let error = Errors::for_code(code);
            if let Some(owner) = error.error() {
                let value = error_code_of(&owner) as i32;
                if let Some(previous) = seen.insert(value, error.enum_name().to_string()) {
                    panic!("{value} is reported by both {previous} and {}", error.enum_name());
                }
            }
        }

        for (error, expected, _) in client_side_classes() {
            let value = error_code_of(&error) as i32;
            let name = format!("{expected:?}");
            if let Some(previous) = seen.insert(value, name.clone()) {
                panic!("{value} is reported by both {previous} and {name}");
            }
        }

        assert_eq!(seen.len(), 162, "expected 162 distinct error codes");
    }

    /// The 27 Rust-local negatives keep the values they were published with, and
    /// each client-side class maps to the enumerator named after it.
    ///
    /// These values are ABI (see [`kafka_common_ErrorCode_t`]): a new class
    /// appends at the most-negative end, so inserting one mid-list — which would
    /// renumber everything after it — must fail here.
    #[test]
    fn ffi_error_code_client_side_negatives_are_stable() {
        let classes = client_side_classes();
        assert_eq!(classes.len(), 27, "expected 27 classes with no Java code");

        for (error, expected, value) in classes {
            assert_eq!(expected as i32, value, "{expected:?} moved off its published value");
            assert_eq!(
                error_code_of(&error),
                expected,
                "{} maps to the wrong code",
                ErrorName::name(&error)
            );
        }
    }

    /// A `RecordDeserializationError` with the minimum context its Java
    /// ten-argument constructor requires.
    fn record_deserialization_error() -> crate::common::errors::RecordDeserializationError {
        use crate::common::errors::record_deserialization_error::DeserializationErrorOrigin;
        use crate::common::record::TimestampType;
        crate::common::errors::RecordDeserializationError::new(
            DeserializationErrorOrigin::Value,
            TopicPartition::new("t", 0),
            0,
            0,
            TimestampType::CreateTime,
            None,
            None,
            None,
            "m",
        )
    }

    // -----------------------------------------------------------------------
    // Per-variant `kafka_common_Error_<Variant>` payload accessors
    //
    // Each test covers both halves of the rule: the real class returns a
    // non-null payload whose fields match, and a *different* variant's handle
    // returns null from the extraction function.
    // -----------------------------------------------------------------------

    /// A `RecordDeserializationError` with every optional field populated, for
    /// the header/buffer/origin accessor tests.
    fn record_deserialization_error_full() -> crate::common::errors::RecordDeserializationError {
        use crate::common::errors::record_deserialization_error::DeserializationErrorOrigin;
        use crate::common::header::{RecordHeader, RecordHeaders};
        use crate::common::record::TimestampType;
        crate::common::errors::RecordDeserializationError::new(
            DeserializationErrorOrigin::Key,
            TopicPartition::new("t2", 5),
            42,
            99,
            TimestampType::LogAppendTime,
            Some(vec![1, 2, 3]),
            Some(vec![4, 5]),
            Some(RecordHeaders::from_headers(vec![RecordHeader::new(
                "h1".to_string(),
                Some(vec![9, 9]),
            )])),
            "m",
        )
    }

    /// A benign error of a different variant, used to assert every extraction
    /// function returns null when the handle is not its variant.
    fn other_error() -> Error {
        Error::kafka("other")
    }

    #[test]
    fn topic_authorization_payload() {
        let mut topics = HashSet::new();
        topics.insert("t1".to_string());
        let error = box_error(Error::TopicAuthorization(TopicAuthorizationError::new(topics.clone())));
        unsafe {
            let handle = kafka_common_Error_TopicAuthorization(error);
            assert!(!handle.is_null());
            let list = kafka_common_TopicAuthorizationError_unauthorized_topics(handle);
            assert_eq!(kafka_consumer_StringList_count(list), 1);
            let got = CStr::from_ptr(kafka_consumer_StringList_get(list, 0)).to_str().unwrap();
            assert_eq!(got, "t1");
            kafka_consumer_StringList_destroy(list);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_TopicAuthorization(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn group_authorization_payload() {
        let error = box_error(Error::GroupAuthorization(GroupAuthorizationError::for_group_id("g1")));
        unsafe {
            let handle = kafka_common_Error_GroupAuthorization(error);
            assert!(!handle.is_null());
            let group_id_ptr = kafka_common_GroupAuthorizationError_group_id(handle);
            let group_id = CStr::from_ptr(group_id_ptr).to_str().unwrap();
            assert_eq!(group_id, "g1");
            kafka_consumer_string_destroy(group_id_ptr);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_GroupAuthorization(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn invalid_topic_payload() {
        let mut topics = HashSet::new();
        topics.insert("bad".to_string());
        let error = box_error(Error::InvalidTopic(InvalidTopicError::new(topics)));
        unsafe {
            let handle = kafka_common_Error_InvalidTopic(error);
            assert!(!handle.is_null());
            let list = kafka_common_InvalidTopicError_invalid_topics(handle);
            assert_eq!(kafka_consumer_StringList_count(list), 1);
            let got = CStr::from_ptr(kafka_consumer_StringList_get(list, 0)).to_str().unwrap();
            assert_eq!(got, "bad");
            kafka_consumer_StringList_destroy(list);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_InvalidTopic(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn duplicate_resource_payload() {
        let error = box_error(Error::DuplicateResource(DuplicateResourceError::with_resource("res1", "m")));
        unsafe {
            let handle = kafka_common_Error_DuplicateResource(error);
            assert!(!handle.is_null());
            let resource_ptr = kafka_common_DuplicateResourceError_resource(handle);
            let resource = CStr::from_ptr(resource_ptr).to_str().unwrap();
            assert_eq!(resource, "res1");
            kafka_consumer_string_destroy(resource_ptr);
            kafka_common_Error_destroy(error);

            // No resource recorded -> the accessor returns null.
            let no_resource = box_error(Error::DuplicateResource(DuplicateResourceError::new("m")));
            let no_resource_handle = kafka_common_Error_DuplicateResource(no_resource);
            assert!(kafka_common_DuplicateResourceError_resource(no_resource_handle).is_null());
            kafka_common_Error_destroy(no_resource);

            let other = box_error(other_error());
            assert!(kafka_common_Error_DuplicateResource(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn resource_not_found_payload() {
        let error = box_error(Error::ResourceNotFound(ResourceNotFoundError::with_resource("res2", "m")));
        unsafe {
            let handle = kafka_common_Error_ResourceNotFound(error);
            assert!(!handle.is_null());
            let resource_ptr = kafka_common_ResourceNotFoundError_resource(handle);
            let resource = CStr::from_ptr(resource_ptr).to_str().unwrap();
            assert_eq!(resource, "res2");
            kafka_consumer_string_destroy(resource_ptr);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_ResourceNotFound(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn throttling_quota_exceeded_payload() {
        let error = box_error(Error::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(123, "m")));
        unsafe {
            let handle = kafka_common_Error_ThrottlingQuotaExceeded(error);
            assert!(!handle.is_null());
            assert_eq!(kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(handle), 123);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_ThrottlingQuotaExceeded(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn correlation_id_mismatch_payload() {
        let error = box_error(Error::CorrelationIdMismatch(CorrelationIdMismatchError::new("m", 7, 8)));
        unsafe {
            let handle = kafka_common_Error_CorrelationIdMismatch(error);
            assert!(!handle.is_null());
            assert_eq!(kafka_common_CorrelationIdMismatchError_request_correlation_id(handle), 7);
            assert_eq!(kafka_common_CorrelationIdMismatchError_response_correlation_id(handle), 8);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_CorrelationIdMismatch(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn record_deserialization_payload() {
        let error = box_error(Error::RecordDeserialization(Box::new(record_deserialization_error_full())));
        unsafe {
            let handle = kafka_common_Error_RecordDeserialization(error);
            assert!(!handle.is_null());

            let mut origin = -1;
            assert!(kafka_common_RecordDeserializationError_origin(handle, &mut origin));
            assert_eq!(origin, 0, "Key -> 0");

            let partition = kafka_common_RecordDeserializationError_partition(handle);
            assert!(!partition.is_null());
            let topic = CStr::from_ptr(kafka_consumer_TopicPartition_topic(partition)).to_str().unwrap();
            assert_eq!(topic, "t2");
            assert_eq!(kafka_consumer_TopicPartition_partition(partition), 5);
            kafka_consumer_TopicPartition_destroy(partition);

            assert_eq!(kafka_common_RecordDeserializationError_offset(handle), 42);
            assert_eq!(kafka_common_RecordDeserializationError_timestamp(handle), 99);
            assert_eq!(
                kafka_common_RecordDeserializationError_timestamp_type(handle),
                1,
                "LogAppendTime -> 1"
            );

            let mut key_len = -2;
            let key_ptr = kafka_common_RecordDeserializationError_key_buffer(handle, &mut key_len);
            assert_eq!(key_len, 3);
            assert_eq!(std::slice::from_raw_parts(key_ptr, 3), &[1, 2, 3]);

            let mut value_len = -2;
            let value_ptr = kafka_common_RecordDeserializationError_value_buffer(handle, &mut value_len);
            assert_eq!(value_len, 2);
            assert_eq!(std::slice::from_raw_parts(value_ptr, 2), &[4, 5]);

            assert_eq!(kafka_common_RecordDeserializationError_header_count(handle), 1);
            let mut hkey_len = -2;
            let hkey_ptr = kafka_common_RecordDeserializationError_header_key(handle, 0, &mut hkey_len);
            assert_eq!(hkey_len, 2);
            assert_eq!(
                std::str::from_utf8(std::slice::from_raw_parts(hkey_ptr as *const u8, 2)).unwrap(),
                "h1"
            );
            let mut hvalue_len = -2;
            let hvalue_ptr = kafka_common_RecordDeserializationError_header_value(handle, 0, &mut hvalue_len);
            assert_eq!(hvalue_len, 2);
            assert_eq!(std::slice::from_raw_parts(hvalue_ptr, 2), &[9, 9]);
            // Out of range -> (null, -1).
            let mut oob_len = -2;
            assert!(kafka_common_RecordDeserializationError_header_key(handle, 1, &mut oob_len).is_null());
            assert_eq!(oob_len, -1);

            kafka_common_Error_destroy(error);

            // No key/value/headers -> absent conventions.
            let sparse = box_error(Error::RecordDeserialization(Box::new(record_deserialization_error())));
            let sparse_handle = kafka_common_Error_RecordDeserialization(sparse);
            let mut no_origin = -1;
            // `record_deserialization_error()` uses `DeserializationErrorOrigin::Value`.
            assert!(kafka_common_RecordDeserializationError_origin(sparse_handle, &mut no_origin));
            assert_eq!(no_origin, 1, "Value -> 1");
            let mut none_len = -2;
            assert!(kafka_common_RecordDeserializationError_key_buffer(sparse_handle, &mut none_len).is_null());
            assert_eq!(none_len, -1);
            assert_eq!(kafka_common_RecordDeserializationError_header_count(sparse_handle), 0);
            kafka_common_Error_destroy(sparse);

            let other = box_error(other_error());
            assert!(kafka_common_Error_RecordDeserialization(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn quota_violation_payload() {
        let error = box_error(Error::QuotaViolation(Box::new(QuotaViolationError::new(
            MetricName::new("n1", "g1", "d", BTreeMap::new()),
            1.0,
            0.5,
        ))));
        unsafe {
            let handle = kafka_common_Error_QuotaViolation(error);
            assert!(!handle.is_null());
            let name_ptr = kafka_common_QuotaViolationError_metric_name(handle);
            assert_eq!(CStr::from_ptr(name_ptr).to_str().unwrap(), "n1");
            kafka_consumer_string_destroy(name_ptr);
            let group_ptr = kafka_common_QuotaViolationError_metric_group(handle);
            assert_eq!(CStr::from_ptr(group_ptr).to_str().unwrap(), "g1");
            kafka_consumer_string_destroy(group_ptr);
            assert_eq!(kafka_common_QuotaViolationError_value(handle), 1.0);
            assert_eq!(kafka_common_QuotaViolationError_bound(handle), 0.5);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_QuotaViolation(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn consumer_log_truncation_payload() {
        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("t", 0), 10i64);
        let mut divergent = HashMap::new();
        divergent.insert(TopicPartition::new("t", 0), crate::consumer::OffsetAndMetadata::new(5).unwrap());
        let error = box_error(Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(
            offsets, divergent,
        ))));
        unsafe {
            let handle = kafka_common_Error_ConsumerLogTruncation(error);
            assert!(!handle.is_null());

            let offset_map = kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions(handle);
            assert_eq!(kafka_consumer_LongOffsetMap_count(offset_map), 1);
            assert_eq!(kafka_consumer_LongOffsetMap_get_value(offset_map, 0), 10);
            kafka_consumer_LongOffsetMap_destroy(offset_map);

            let divergent_map = kafka_common_ConsumerLogTruncationError_divergent_offsets(handle);
            assert_eq!(kafka_consumer_OffsetMap_count(divergent_map), 1);
            kafka_consumer_OffsetMap_destroy(divergent_map);

            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_ConsumerLogTruncation(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn consumer_no_offset_for_partition_payload() {
        let error = box_error(Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(
            TopicPartition::new("t", 3),
        )));
        unsafe {
            let handle = kafka_common_Error_ConsumerNoOffsetForPartition(error);
            assert!(!handle.is_null());
            let list = kafka_common_ConsumerNoOffsetForPartitionError_partitions(handle);
            assert_eq!(kafka_consumer_TopicPartitionList_count(list), 1);
            let tp = kafka_consumer_TopicPartitionList_get(list, 0);
            assert_eq!(kafka_consumer_TopicPartition_partition(tp), 3);
            kafka_consumer_TopicPartitionList_destroy(list);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_ConsumerNoOffsetForPartition(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn consumer_offset_out_of_range_payload() {
        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("t", 0), 77i64);
        let error = box_error(Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(offsets)));
        unsafe {
            let handle = kafka_common_Error_ConsumerOffsetOutOfRange(error);
            assert!(!handle.is_null());
            let map = kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(handle);
            assert_eq!(kafka_consumer_LongOffsetMap_count(map), 1);
            assert_eq!(kafka_consumer_LongOffsetMap_get_value(map, 0), 77);
            kafka_consumer_LongOffsetMap_destroy(map);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_ConsumerOffsetOutOfRange(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn record_too_large_payload() {
        let mut partitions = HashMap::new();
        partitions.insert(TopicPartition::new("t", 0), 999i64);
        let error = box_error(Error::RecordTooLarge(RecordTooLargeError::with_partitions("m", partitions)));
        unsafe {
            let handle = kafka_common_Error_RecordTooLarge(error);
            assert!(!handle.is_null());
            let map = kafka_common_RecordTooLargeError_record_too_large_partitions(handle);
            assert!(!map.is_null());
            assert_eq!(kafka_consumer_LongOffsetMap_count(map), 1);
            assert_eq!(kafka_consumer_LongOffsetMap_get_value(map, 0), 999);
            kafka_consumer_LongOffsetMap_destroy(map);
            kafka_common_Error_destroy(error);

            // Java's field defaults to `null` -> the accessor returns null, not
            // an empty map.
            let no_partitions = box_error(Error::RecordTooLarge(RecordTooLargeError::new("m")));
            let no_partitions_handle = kafka_common_Error_RecordTooLarge(no_partitions);
            assert!(kafka_common_RecordTooLargeError_record_too_large_partitions(no_partitions_handle).is_null());
            kafka_common_Error_destroy(no_partitions);

            let other = box_error(other_error());
            assert!(kafka_common_Error_RecordTooLarge(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }
}
