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
#![expect(non_camel_case_types)]

use std::any::Any;
use std::ffi::{CStr, CString, c_char};
use std::panic::AssertUnwindSafe;

use crate::common::Error;
use crate::common::protocol::Errors;
use crate::ffi::ffi_guard;
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
// Panic guard
// ---------------------------------------------------------------------------

/// Runs the body of an `extern "C"` entry point, turning a Rust panic into the
/// entry point's failure value instead of letting it unwind into the C caller.
///
/// This is the runtime half of [`ffi_guard`](crate::ffi::ffi_guard): the
/// attribute rewrites each entry point's body into
/// `ffi_guard_or("<name>", <on-panic closure>, move || <body>)`. A panic that
/// reached the `extern "C"` frame would abort the whole process — the release
/// profile keeps `panic = "unwind"` because std `Mutex` poisoning relies on it,
/// so the abort would come from the shim the compiler inserts at the boundary.
/// Here the panic is caught instead, turned into an [`Error`] by
/// [`panic_error`], logged at `error` level, and handed to `on_panic`, whose
/// value the entry point returns.
///
/// The default panic hook is left alone, so the usual "thread … panicked at …"
/// line still reaches stderr before the error is logged.
///
/// # Unwind safety
///
/// The body runs under [`AssertUnwindSafe`]. A panic can leave the state behind
/// a handle's lock half-updated; std `Mutex` poisoning reports that to every
/// later call on the handle, which then fails with the poison message instead
/// of reading the state. That is intended: such a handle should be destroyed
/// and recreated, as the generated C header says, and the poison is not
/// cleared anywhere.
///
/// `on_panic` must not panic itself: nothing is left to catch it.
///
/// No Java counterpart: Java has no C boundary.
pub(crate) fn ffi_guard_or<R>(fn_name: &'static str, on_panic: impl FnOnce(Error) -> R, body: impl FnOnce() -> R) -> R {
    match std::panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(payload) => {
            let error = panic_error(fn_name, &*payload);
            // A payload's destructor is arbitrary code, and one that panicked
            // would unwind out of this frame after all. Drop it under a second
            // guard, and leak whatever that second panic carries.
            if let Err(nested) = std::panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) {
                std::mem::forget(nested);
            }
            log::error!("{}", error.message());
            on_panic(error)
        },
    }
}

/// The [`Error`] a panic caught by [`ffi_guard_or`] is reported as.
///
/// An [`Error::LocalIllegalState`] whose message is
/// `Rust panic caught at the FFI boundary in <fn_name>: <panic message>`. The
/// panic message is the payload when it is a `&str` or a `String`, which is
/// what `panic!`, `assert!`, `unwrap` and `expect` produce, and
/// `non-string panic payload` otherwise (`std::panic::panic_any`).
pub(crate) fn panic_error(fn_name: &str, payload: &(dyn Any + Send)) -> Error {
    let message = if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    };
    Error::local_illegal_state(format!("Rust panic caught at the FFI boundary in {fn_name}: {message}"))
}

/// Spawns the task a callback-style entry point hands its callback to,
/// aborting the process if the spawn panics.
///
/// `#[ffi_guard(on_panic = ...)]` fires the entry point's callback when the
/// body panics, which is right only while no task can fire it as well. Tokio
/// can panic inside `spawn` after the task is queued: the C caller's thread is
/// not one of the runtime's workers, so the task is queued and a worker is then
/// woken, and a failed wake is `expect("failed to wake I/O driver")`
/// (`runtime/io/driver.rs:260`, tokio 1.52.0). Unwinding from there would fire
/// the callback from `on_panic` while the queued task fires it too, and would
/// skip the producer's `pending_tasks` registration after the spawn, which
/// `destroy` relies on. So the process aborts, as every panic at the boundary
/// did before `#[ffi_guard]`: the OS refused to wake the runtime. See §4 item 6
/// of `design/current/appsec-7665-4521-ffi-panic-guard.md`.
///
/// No Java counterpart: Java has no C boundary.
pub(crate) fn spawn_callback_task<F>(runtime: &tokio::runtime::Handle, task: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_or_abort(|| runtime.spawn(task))
}

/// Runs `spawn` and aborts the process if it panics: the body of
/// [`spawn_callback_task`], taking the spawn as a closure so a test can make it
/// panic.
fn spawn_or_abort<J>(spawn: impl FnOnce() -> J) -> J {
    match std::panic::catch_unwind(AssertUnwindSafe(spawn)) {
        Ok(join) => join,
        // The payload is not dropped: nothing runs after the abort.
        Err(_) => {
            log::error!(
                "aborting: tokio panicked while spawning a task that owns a C callback, which may already be \
                 queued; unwinding would let the FFI guard fire that callback a second time"
            );
            std::process::abort()
        },
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
    // SAFETY: Per this function's `# Safety`, `error` is non-null and was created by
    // `box_error`, whose `Box::into_raw` of an `ErrorInner` is the single allocation behind
    // every `kafka_common_Error_t`, so the cast targets a live, correctly typed
    // `ErrorInner`. The `&'static` is a convenience only: every caller is a
    // `kafka_common_Error_*` accessor in this module that uses the reference for the
    // duration of its own call, during which the C caller keeps the handle alive (not yet
    // passed to `kafka_common_Error_destroy`), and none of them stores or returns it.
    unsafe { &*(error as *const ErrorInner) }
}

/// Creates a new error handle from a protocol error code and a message.
///
/// This is the inverse of the [`kafka_common_Error_code`] /
/// [`kafka_common_Error_message`] accessors: it lets a C (or Python)
/// callback that must *return* an error to the Rust core build the handle to
/// return. The rebalance-listener callbacks are the motivating case — they
/// return `Result<(), Error>` in the core, i.e. a
/// `kafka_common_Error_t*` in C, and a Python listener that raised an
/// exception has to convert it into one.
///
/// `code` is looked up as a Kafka protocol error code; unknown codes (including
/// any value outside the `i16` protocol range) map to
/// `Errors::UnknownServerError`, mirroring Java's `Errors.forCode`.
///
/// # Parameters
///
/// - `code`: Kafka protocol error code (see `kafka_common_Error_code`).
/// - `message`: Null-terminated error message, or null for an empty message.
///
/// # Returns
///
/// A non-null error handle owned by the caller, who must free it with
/// [`kafka_common_Error_destroy`] — unless it is handed to a Rust callback
/// that documents taking ownership of it.
///
/// # Safety
///
/// `message` must be null or a valid, null-terminated C string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_new(code: i32, message: *const c_char) -> *mut kafka_common_Error_t {
    let error = match i16::try_from(code) {
        Ok(code) => Errors::for_code(code),
        Err(_) => Errors::UnknownServerError,
    };
    let message = if message.is_null() {
        String::new()
    } else {
        // SAFETY: `message` is non-null (checked above) and, per this function's `#
        // Safety`, a valid NUL-terminated C string; it is read once here and copied into an
        // owned `String`, so nothing borrows it past this call.
        unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned()
    };
    box_error(Error::with_message(error, message))
}

/// Takes ownership of an error handle **returned by a C callback** and converts
/// it back into a [`Error`], freeing the handle. A null pointer means
/// success and yields `None`.
///
/// This is the inbound counterpart of [`box_error`]: it is how a callback whose
/// Rust signature returns `Result<(), Error>` (the rebalance-listener
/// methods) reports failure across the boundary. The handle is consumed exactly
/// as [`kafka_common_Error_destroy`] would consume it, so the C callback
/// must not free it itself.
///
/// # Safety
///
/// `error` must be null or a handle created by [`box_error`] (i.e. by
/// [`kafka_common_Error_new`] or returned from a fallible FFI function and
/// not yet destroyed). After this call the pointer is invalid.
pub(crate) unsafe fn take_error(error: *mut kafka_common_Error_t) -> Option<Error> {
    if error.is_null() {
        return None;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // handle created by `box_error` (its `Box::into_raw` of an `ErrorInner`) and not yet
    // destroyed. The contract declares the pointer invalid after this call and forbids the
    // C callback from freeing it itself, so this `Box::from_raw` is the single, final use
    // of the allocation.
    let inner = unsafe { Box::from_raw(error as *mut ErrorInner) };
    Some(inner.error)
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
/// message, and the hierarchy predicates. This is the same reason CLAUDE.md §4
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_code(error: *const kafka_common_Error_t) -> kafka_common_ErrorCode_t {
    if error.is_null() {
        return kafka_common_ErrorCode_NONE;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. one created by `box_error`
    // and not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to read `.error` for the
    // duration of this call, during which the C caller keeps the handle alive.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_message(error: *const kafka_common_Error_t) -> *const c_char {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned
    // `message_cstring.as_ptr()` points into that same `ErrorInner` allocation and, as this
    // function's rustdoc states, stays valid until `kafka_common_Error_destroy` is called
    // on the handle.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_retriable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate `is_retriable_error`
    // on `.error` within this call, during which the C caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_retriable_error()
}

// NOTE: fatality (`RequestUtils.isFatalException`) is deliberately NOT exported
// here. `org.apache.kafka.common.requests` carries the package disclaimer "This
// package is not a supported Kafka API; the implementation may change without
// warning between minor or patch releases", and CLAUDE.md §4 forbids C bindings
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_kafka_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate `is_kafka_error` on
    // `.error` within this call, during which the C caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_kafka_error()
}

/// Returns whether the error's Java exception extends `ApiException` — an error the broker can report over the protocol, as opposed to a client-side programming or serialization failure.
///
/// Mirrors `Error::is_api_error` — see CLAUDE.md §12.4. Exposed because C cannot see
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_api_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate `is_api_error` on
    // `.error` within this call, during which the C caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_api_error()
}

/// Returns whether the error is retriable AND a metadata/coordinator refresh is what clears it (Java `RefreshRetriableException`).
///
/// Mirrors `Error::is_refresh_retriable_error` — see CLAUDE.md §12.4. Exposed because C cannot see
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_refresh_retriable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_refresh_retriable_error` on `.error` within this call, during which the C caller
    // keeps the handle alive.
    unsafe { error_ref(error) }.error.is_refresh_retriable_error()
}

/// Returns whether the error means the client's cached metadata may be stale (Java `InvalidMetadataException`).
///
/// Mirrors `Error::is_invalid_metadata_error` — see CLAUDE.md §12.4. Exposed because C cannot see
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_metadata_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_invalid_metadata_error` on `.error` within this call, during which the C caller
    // keeps the handle alive.
    unsafe { error_ref(error) }.error.is_invalid_metadata_error()
}

/// Returns whether the error is an authentication failure reported by the broker (Java `AuthenticationException`).
///
/// Mirrors `Error::is_authentication_error` — see CLAUDE.md §12.4. Exposed because C cannot see
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authentication_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_authentication_error` on `.error` within this call, during which the C caller
    // keeps the handle alive.
    unsafe { error_ref(error) }.error.is_authentication_error()
}

/// Returns whether the error is an authorization failure — a missing ACL (Java `AuthorizationException`).
///
/// Mirrors `Error::is_authorization_error` — see CLAUDE.md §12.4. Exposed because C cannot see
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authorization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_authorization_error` on `.error` within this call, during which the C caller
    // keeps the handle alive.
    unsafe { error_ref(error) }.error.is_authorization_error()
}

/// Returns whether the error's Java class extends `InvalidConfigurationException` — the parent of both the authentication and
/// authorization families, so a broad classification a C caller cannot make from
/// the numeric code alone.
///
/// Mirrors `Error::is_invalid_configuration_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_configuration_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_invalid_configuration_error` on `.error` within this call, during which the C
    // caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_invalid_configuration_error()
}

/// Returns whether the error's Java class extends `ApplicationRecoverableException` — recoverable by re-initialising the
/// producer or rejoining the group.
///
/// Mirrors `Error::is_application_recoverable_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_application_recoverable_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_application_recoverable_error` on `.error` within this call, during which the C
    // caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_application_recoverable_error()
}

/// Returns whether the error's Java class extends `InvalidOffsetException` (common.errors).
///
/// Mirrors `Error::is_invalid_offset_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_offset_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_invalid_offset_error` on `.error` within this call, during which the C caller
    // keeps the handle alive.
    unsafe { error_ref(error) }.error.is_invalid_offset_error()
}

/// Returns whether the error's Java class extends `OutOfOrderSequenceException`.
///
/// Mirrors `Error::is_out_of_order_sequence_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_out_of_order_sequence_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_out_of_order_sequence_error` on `.error` within this call, during which the C
    // caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_out_of_order_sequence_error()
}

/// Returns whether the error's Java class extends `SerializationException`.
///
/// Mirrors `Error::is_serialization_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_serialization_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_serialization_error` on `.error` within this call, during which the C caller
    // keeps the handle alive.
    unsafe { error_ref(error) }.error.is_serialization_error()
}

/// Returns whether the error's Java class extends `TimeoutException` (also covers `BufferExhaustedException`).
///
/// Mirrors `Error::is_timeout_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_timeout_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate `is_timeout_error`
    // on `.error` within this call, during which the C caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_timeout_error()
}

/// Returns whether the error's Java class extends the consumer package's `InvalidOffsetException`.
///
/// Mirrors `Error::is_consumer_invalid_offset_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_invalid_offset_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_consumer_invalid_offset_error` on `.error` within this call, during which the C
    // caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_consumer_invalid_offset_error()
}

/// Returns whether the error's Java class extends the consumer package's `OffsetOutOfRangeException` (also covers
/// `LogTruncationException`).
///
/// Mirrors `Error::is_consumer_offset_out_of_range_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_offset_out_of_range_error(
    error: *const kafka_common_Error_t,
) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_consumer_offset_out_of_range_error` on `.error` within this call, during which
    // the C caller keeps the handle alive.
    unsafe { error_ref(error) }.error.is_consumer_offset_out_of_range_error()
}

/// Returns whether the error's Java class extends `TransactionAbortableException` — the transaction may be aborted and retried.
///
/// Mirrors `Error::is_transaction_abortable_error` (CLAUDE.md §4/§12.4). Exposed because C cannot see enum
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_abortable_error(error: *const kafka_common_Error_t) -> bool {
    if error.is_null() {
        return false;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet passed to `kafka_common_Error_destroy`, which is exactly `error_ref`'s
    // precondition. The `&'static ErrorInner` is used only to evaluate
    // `is_transaction_abortable_error` on `.error` within this call, during which the C
    // caller keeps the handle alive.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_destroy(error: *mut kafka_common_Error_t) {
    if !error.is_null() {
        // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`,
        // a valid handle from a function that returned an error, i.e. the `ErrorInner` that
        // `box_error` leaked with `Box::into_raw` (the one allocation behind every
        // `kafka_common_Error_t`). The contract declares the pointer invalid after this
        // call, and `take_error`'s contract forbids destroying a handle it already
        // consumed, so this `Box::from_raw` is the single, final use of the allocation.
        unsafe {
            drop(Box::from_raw(error as *mut ErrorInner));
        }
    }
}

/// Returns an independent, owned copy of an error handle: same variant, code,
/// message, payload and cause chain.
///
/// Every owned `kafka_common_Error_t` is freed exactly once, so a caller that
/// must hand one error to several consumers — for example a binding that
/// resolves one per-key future per requested key from a single whole-request
/// failure — hands each its own copy. Free the copy with
/// [`kafka_common_Error_destroy`]; the original is left untouched.
///
/// # Safety
///
/// `error` must be a valid, non-null error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_clone(error: *const kafka_common_Error_t) -> *mut kafka_common_Error_t {
    box_error(unsafe { error_ref(error) }.error.clone())
}

/// Returns the error that caused this one, or null when there is none —
/// Java's `Throwable.getCause()`.
///
/// The Rust core keeps a cause wherever Java passes one to the exception's
/// constructor, for example `KafkaException("Failed to create new
/// KafkaAdminClient", exc)`, `KafkaException("Failed to find brokers to send
/// ListGroups", throwable)`, the `removeMembersFromConsumerGroup` remove-all
/// wrap, and a `TimeoutException` that records the last error seen before the
/// deadline. Walking the chain means calling this again on the returned
/// handle, until it returns null.
///
/// # Ownership
///
/// The returned handle is **owned** by the caller, the same as
/// [`kafka_common_Error_clone`]: it is an independent copy of the cause
/// (variant, code, message, payload and its own cause chain), not a view into
/// `error`. Free it with [`kafka_common_Error_destroy`]. It stays valid after
/// `error` is destroyed, and destroying it leaves `error` untouched.
///
/// # Safety
///
/// `error` must be a valid, non-null error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_cause(error: *const kafka_common_Error_t) -> *mut kafka_common_Error_t {
    match unsafe { error_ref(error) }.error.source() {
        Some(cause) => box_error(cause.clone()),
        None => std::ptr::null_mut(),
    }
}

// ---------------------------------------------------------------------------
// Per-variant payload accessors (CLAUDE.md §4: "Exceptions having additional
// fields in Java")
//
// Of the ~156 `Error` variants, the twelve below carry state beyond
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
    kafka_common_TopicPartition_t, kafka_common_TopicPartitionList_t, kafka_consumer_LongOffsetMap_t,
    kafka_consumer_OffsetMap_t, kafka_consumer_StringList_t,
};

/// `TopicAuthorizationException` -> `kafka_common_TopicAuthorizationError_t`.
#[repr(C)]
pub struct kafka_common_TopicAuthorizationError_t {
    _private: [u8; 0],
}

/// Returns the error's `TopicAuthorizationException` payload, or null if the
/// error is not that variant.
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_topic_authorization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_TopicAuthorizationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `TopicAuthorizationError` stored inline in the `Error` enum inside that
    // same `ErrorInner` allocation, so it is valid exactly as long as the parent handle,
    // until `kafka_common_Error_destroy` (see the `Per-variant payload accessors` comment
    // above); the C caller must not destroy the error while using the payload, a condition
    // this function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_unauthorized_topics(
    handle: *const kafka_common_TopicAuthorizationError_t,
) -> *mut kafka_consumer_StringList_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_TopicAuthorizationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_topic_authorization`, which returns the address of the
    // `TopicAuthorizationError` inside a live `ErrorInner`, so the cast back is exact. The
    // reference is used only for the duration of this call, to clone the topics into the
    // fresh owned `StringList` handed to the caller, and the parent error must still be
    // alive (the payload is a borrow that dies with `kafka_common_Error_destroy`, see the
    // `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_group_authorization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_GroupAuthorizationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `GroupAuthorizationError` stored inline in the `Error` enum inside that
    // same `ErrorInner` allocation, so it is valid exactly as long as the parent handle,
    // until `kafka_common_Error_destroy` (see the `Per-variant payload accessors` comment
    // above); the C caller must not destroy the error while using the payload, a condition
    // this function's rustdoc does not yet state (see flags).
    match &unsafe { error_ref(error) }.error {
        Error::GroupAuthorization(e) => e as *const _ as *const kafka_common_GroupAuthorizationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns the offending group id as an owned, NUL-terminated C string, or
/// null where Java's `GroupAuthorizationException.groupId()` is null — the
/// group is not known where the error was raised, e.g. an error built from a
/// broker's `GROUP_AUTHORIZATION_FAILED` code. A non-null result must be freed
/// with [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_GroupAuthorizationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_group_id(
    handle: *const kafka_common_GroupAuthorizationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_GroupAuthorizationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_group_authorization`, which returns the address of the
    // `GroupAuthorizationError` inside a live `ErrorInner`, so the cast back is exact. The
    // reference is used only for the duration of this call, to copy `group_id()` into the
    // fresh owned `CString` handed to the caller, and the parent error must still be alive
    // (the payload is a borrow that dies with `kafka_common_Error_destroy`, see the
    // `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::errors::GroupAuthorizationError) };
    match e.group_id() {
        Some(group_id) => CString::new(group_id).unwrap_or_default().into_raw(),
        None => std::ptr::null_mut(),
    }
}

/// `InvalidTopicException` -> `kafka_common_InvalidTopicError_t`.
#[repr(C)]
pub struct kafka_common_InvalidTopicError_t {
    _private: [u8; 0],
}

/// Returns the error's `InvalidTopicException` payload, or null if the error
/// is not that variant.
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_topic(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_InvalidTopicError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `InvalidTopicError` stored inline in the `Error` enum inside that same
    // `ErrorInner` allocation, so it is valid exactly as long as the parent handle, until
    // `kafka_common_Error_destroy` (see the `Per-variant payload accessors` comment above);
    // the C caller must not destroy the error while using the payload, a condition this
    // function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_InvalidTopicError_invalid_topics(
    handle: *const kafka_common_InvalidTopicError_t,
) -> *mut kafka_consumer_StringList_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_InvalidTopicError_t`; the only producer of such a pointer is
    // `kafka_common_Error_invalid_topic`, which returns the address of the
    // `InvalidTopicError` inside a live `ErrorInner`, so the cast back is exact. The
    // reference is used only for the duration of this call, to clone the topics into the
    // fresh owned `StringList` handed to the caller, and the parent error must still be
    // alive (the payload is a borrow that dies with `kafka_common_Error_destroy`, see the
    // `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_duplicate_resource(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_DuplicateResourceError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `DuplicateResourceError` stored inline in the `Error` enum inside that
    // same `ErrorInner` allocation, so it is valid exactly as long as the parent handle,
    // until `kafka_common_Error_destroy` (see the `Per-variant payload accessors` comment
    // above); the C caller must not destroy the error while using the payload, a condition
    // this function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_resource(
    handle: *const kafka_common_DuplicateResourceError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_DuplicateResourceError_t`; the only producer of such a pointer is
    // `kafka_common_Error_duplicate_resource`, which returns the address of the
    // `DuplicateResourceError` inside a live `ErrorInner`, so the cast back is exact. The
    // reference is used only for the duration of this call, to copy `resource()` into a
    // fresh owned `CString` (or return null when absent), and the parent error must still
    // be alive (the payload is a borrow that dies with `kafka_common_Error_destroy`, see
    // the `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_resource_not_found(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ResourceNotFoundError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `ResourceNotFoundError` stored inline in the `Error` enum inside that
    // same `ErrorInner` allocation, so it is valid exactly as long as the parent handle,
    // until `kafka_common_Error_destroy` (see the `Per-variant payload accessors` comment
    // above); the C caller must not destroy the error while using the payload, a condition
    // this function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_resource(
    handle: *const kafka_common_ResourceNotFoundError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_ResourceNotFoundError_t`; the only producer of such a pointer is
    // `kafka_common_Error_resource_not_found`, which returns the address of the
    // `ResourceNotFoundError` inside a live `ErrorInner`, so the cast back is exact. The
    // reference is used only for the duration of this call, to copy `resource()` into a
    // fresh owned `CString` (or return null when absent), and the parent error must still
    // be alive (the payload is a borrow that dies with `kafka_common_Error_destroy`, see
    // the `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttling_quota_exceeded(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ThrottlingQuotaExceededError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `ThrottlingQuotaExceededError` stored inline in the `Error` enum inside
    // that same `ErrorInner` allocation, so it is valid exactly as long as the parent
    // handle, until `kafka_common_Error_destroy` (see the `Per-variant payload accessors`
    // comment above); the C caller must not destroy the error while using the payload, a
    // condition this function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(
    handle: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_ThrottlingQuotaExceededError_t`; the only producer of such a pointer is
    // `kafka_common_Error_throttling_quota_exceeded`, which returns the address of the
    // `ThrottlingQuotaExceededError` inside a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to copy out the scalar
    // `throttle_time_ms()`, and the parent error must still be alive (the payload is a
    // borrow that dies with `kafka_common_Error_destroy`, see the `Per-variant payload
    // accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::errors::ThrottlingQuotaExceededError) };
    e.throttle_time_ms()
}

/// `RecordDeserializationException` -> `kafka_common_RecordDeserializationError_t`.
#[repr(C)]
pub struct kafka_common_RecordDeserializationError_t {
    _private: [u8; 0],
}

/// Returns the error's `RecordDeserializationException` payload, or null if
/// the error is not that variant.
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_deserialization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordDeserializationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `RecordDeserializationError` in the `Box` owned by the `Error` enum
    // inside that same `ErrorInner` allocation (`e.as_ref()`), so its address is stable and
    // valid exactly as long as the parent handle, until `kafka_common_Error_destroy` (see
    // the `Per-variant payload accessors` comment above); the C caller must not destroy the
    // error while using the payload, a condition this function's rustdoc does not yet state
    // (see flags).
    match &unsafe { error_ref(error) }.error {
        Error::RecordDeserialization(e) => e.as_ref() as *const _ as *const kafka_common_RecordDeserializationError_t,
        _ => std::ptr::null(),
    }
}

/// Returns which side of the record failed to deserialize, as the ordinal of
/// Java's `DeserializationExceptionOrigin` (`0` = key, `1` = value).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_origin(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to read `origin()`, and the parent error
    // must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    match e.origin() {
        crate::common::errors::DeserializationErrorOrigin::Key => 0,
        crate::common::errors::DeserializationErrorOrigin::Value => 1,
    }
}

/// Returns the partition of the offending record, as an owned handle the
/// caller must destroy with [`kafka_common_TopicPartition_destroy`](crate::ffi::consumer::kafka_common_TopicPartition_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_partition(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> *mut kafka_common_TopicPartition_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to clone `topic_partition()` into the
    // fresh owned `kafka_common_TopicPartition_t` handed to the caller, and the parent
    // error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    box_topic_partition(e.topic_partition().clone())
}

/// Returns the offset of the offending record.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_offset(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to copy out the scalar `offset()`, and
    // the parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    e.offset()
}

/// Returns the timestamp of the offending record.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_timestamp(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to copy out the scalar `timestamp()`, and
    // the parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_timestamp_type(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to copy out `timestamp_type().id()`, and
    // the parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_key_buffer(
    handle: *const kafka_common_RecordDeserializationError_t,
    out_len: *mut i32,
) -> *const u8 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // `key_buffer()` returns `Option<&[u8]>` borrowed from the error's own `Vec<u8>`, so
    // the `k.as_ptr()` returned to the caller points into the parent `ErrorInner`
    // allocation and, as the rustdoc states, is valid until the parent error is destroyed;
    // the parent must still be alive for this call (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`).
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    match e.key_buffer() {
        Some(k) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the key length) is
                // written through it.
                unsafe { *out_len = k.len() as i32 };
            }
            k.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the absent marker `-1`)
                // is written through it.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_value_buffer(
    handle: *const kafka_common_RecordDeserializationError_t,
    out_len: *mut i32,
) -> *const u8 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // `value_buffer()` returns `Option<&[u8]>` borrowed from the error's own `Vec<u8>`, so
    // the `v.as_ptr()` returned to the caller points into the parent `ErrorInner`
    // allocation and, as the rustdoc states, is valid until the parent error is destroyed;
    // the parent must still be alive for this call (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`).
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    match e.value_buffer() {
        Some(v) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the value length) is
                // written through it.
                unsafe { *out_len = v.len() as i32 };
            }
            v.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the absent marker `-1`)
                // is written through it.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_header_count(
    handle: *const kafka_common_RecordDeserializationError_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to count `headers()` (an
    // `Option<&RecordHeaders>` borrow), and the parent error must still be alive (the
    // payload is a borrow that dies with `kafka_common_Error_destroy`, see the `Per-variant
    // payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    e.headers().into_iter().flatten().count() as i32
}

/// Returns the key of the header at `index` (insertion order) as a (ptr, len)
/// pair (NOT NUL-terminated), or (null, -1) if out of range.
/// The pointer is borrowed and valid until the parent error is destroyed.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_len` must be a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_header_key(
    handle: *const kafka_common_RecordDeserializationError_t,
    index: i32,
    out_len: *mut i32,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // `headers()` returns `Option<&RecordHeaders>` and `&RecordHeaders` iterates
    // `&RecordHeader`, so `header.key()` borrows the error's own header storage and the
    // `key.as_ptr()` returned to the caller points into the parent `ErrorInner` allocation,
    // valid until the parent error is destroyed (not stated in this function's rustdoc, see
    // flags); the parent must still be alive for this call.
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
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the header key length) is
                // written through it.
                unsafe { *out_len = key.len() as i32 };
            }
            key.as_ptr() as *const c_char
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the out-of-range marker
                // `-1`) is written through it.
                unsafe { *out_len = -1 };
            }
            std::ptr::null()
        },
    }
}

/// Returns the value of the header at `index` as a (ptr, len) pair, or
/// (null, -1) if out of range or the header value is null.
/// The pointer is borrowed and valid until the parent error is destroyed.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_RecordDeserializationError_t`];
/// `out_len` must be a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_header_value(
    handle: *const kafka_common_RecordDeserializationError_t,
    index: i32,
    out_len: *mut i32,
) -> *const u8 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordDeserializationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_deserialization`, which returns the address of the boxed
    // `RecordDeserializationError` owned by a live `ErrorInner`, so the cast back is exact.
    // `headers()` returns `Option<&RecordHeaders>` and `&RecordHeaders` iterates
    // `&RecordHeader`, so `h.value()` (`Option<&[u8]>`) borrows the error's own header
    // storage and the `value.as_ptr()` returned to the caller points into the parent
    // `ErrorInner` allocation, valid until the parent error is destroyed (not stated in
    // this function's rustdoc, see flags); the parent must still be alive for this call.
    let e = unsafe { &*(handle as *const crate::common::errors::RecordDeserializationError) };
    let header = if index < 0 {
        None
    } else {
        e.headers().into_iter().flatten().nth(index as usize)
    };
    match header.and_then(|h| h.value()) {
        Some(value) => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the header value length)
                // is written through it.
                unsafe { *out_len = value.len() as i32 };
            }
            value.as_ptr()
        },
        None => {
            if !out_len.is_null() {
                // SAFETY: `out_len` is non-null (checked above) and, per this function's `#
                // Safety`, a valid pointer, so exactly one `i32` (the out-of-range /
                // null-value marker `-1`) is written through it.
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_quota_violation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_QuotaViolationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `QuotaViolationError` in the `Box` owned by the `Error` enum inside
    // that same `ErrorInner` allocation (`e.as_ref()`), so its address is stable and valid
    // exactly as long as the parent handle, until `kafka_common_Error_destroy` (see the
    // `Per-variant payload accessors` comment above); the C caller must not destroy the
    // error while using the payload, a condition this function's rustdoc does not yet state
    // (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_metric_name(
    handle: *const kafka_common_QuotaViolationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_QuotaViolationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_quota_violation`, which returns the address of the boxed
    // `QuotaViolationError` owned by a live `ErrorInner`, so the cast back is exact. The
    // reference is used only within this call to copy `metric_name().name()` into the fresh
    // owned `CString` handed to the caller, and the parent error must still be alive (the
    // payload is a borrow that dies with `kafka_common_Error_destroy`, see the `Per-variant
    // payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    CString::new(e.metric_name().name()).unwrap_or_default().into_raw()
}

/// Returns the metric's group as an owned, NUL-terminated C string. The
/// caller must free it with [`kafka_consumer_string_destroy`](crate::ffi::consumer::kafka_consumer_string_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_metric_group(
    handle: *const kafka_common_QuotaViolationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_QuotaViolationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_quota_violation`, which returns the address of the boxed
    // `QuotaViolationError` owned by a live `ErrorInner`, so the cast back is exact. The
    // reference is used only within this call to copy `metric_name().group()` into the
    // fresh owned `CString` handed to the caller, and the parent error must still be alive
    // (the payload is a borrow that dies with `kafka_common_Error_destroy`, see the
    // `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    CString::new(e.metric_name().group()).unwrap_or_default().into_raw()
}

/// Returns the recorded value that violated the quota.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_value(
    handle: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_QuotaViolationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_quota_violation`, which returns the address of the boxed
    // `QuotaViolationError` owned by a live `ErrorInner`, so the cast back is exact. The
    // reference is used only within this call to copy out the scalar `value()`, and the
    // parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::common::metrics::QuotaViolationError) };
    e.value()
}

/// Returns the configured bound the value violated.
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_QuotaViolationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_bound(
    handle: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_QuotaViolationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_quota_violation`, which returns the address of the boxed
    // `QuotaViolationError` owned by a live `ErrorInner`, so the cast back is exact. The
    // reference is used only within this call to copy out the scalar `bound()`, and the
    // parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_log_truncation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerLogTruncationError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `ConsumerLogTruncationError` in the `Box` owned by the `Error` enum
    // inside that same `ErrorInner` allocation (`e.as_ref()`), so its address is stable and
    // valid exactly as long as the parent handle, until `kafka_common_Error_destroy` (see
    // the `Per-variant payload accessors` comment above); the C caller must not destroy the
    // error while using the payload, a condition this function's rustdoc does not yet state
    // (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_offset_out_of_range_partitions(
    handle: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_consumer_LongOffsetMap_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_ConsumerLogTruncationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_consumer_log_truncation`, which returns the address of the boxed
    // `ConsumerLogTruncationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to clone
    // `offset_out_of_range_partitions()` into the fresh owned `LongOffsetMap` handed to the
    // caller, and the parent error must still be alive (the payload is a borrow that dies
    // with `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
    let e = unsafe { &*(handle as *const crate::consumer::ConsumerLogTruncationError) };
    box_long_offset_map(e.offset_out_of_range_partitions().clone())
}

/// Returns the divergent offset per partition, as an owned handle the caller
/// must destroy with [`kafka_consumer_OffsetMap_destroy`](crate::ffi::consumer::kafka_consumer_OffsetMap_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ConsumerLogTruncationError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerLogTruncationError_divergent_offsets(
    handle: *const kafka_common_ConsumerLogTruncationError_t,
) -> *mut kafka_consumer_OffsetMap_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_ConsumerLogTruncationError_t`; the only producer of such a pointer is
    // `kafka_common_Error_consumer_log_truncation`, which returns the address of the boxed
    // `ConsumerLogTruncationError` owned by a live `ErrorInner`, so the cast back is exact.
    // The reference is used only within this call to clone `divergent_offsets()` into the
    // fresh owned `OffsetMap` handed to the caller, and the parent error must still be
    // alive (the payload is a borrow that dies with `kafka_common_Error_destroy`, see the
    // `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_no_offset_for_partition(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerNoOffsetForPartitionError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `ConsumerNoOffsetForPartitionError` stored inline in the `Error` enum
    // inside that same `ErrorInner` allocation, so it is valid exactly as long as the
    // parent handle, until `kafka_common_Error_destroy` (see the `Per-variant payload
    // accessors` comment above); the C caller must not destroy the error while using the
    // payload, a condition this function's rustdoc does not yet state (see flags).
    match &unsafe { error_ref(error) }.error {
        Error::ConsumerNoOffsetForPartition(e) => {
            e as *const _ as *const kafka_common_ConsumerNoOffsetForPartitionError_t
        },
        _ => std::ptr::null(),
    }
}

/// Returns the partitions with no defined offset and no reset policy, as an
/// owned handle the caller must destroy with
/// [`kafka_common_TopicPartitionList_destroy`](crate::ffi::consumer::kafka_common_TopicPartitionList_destroy).
///
/// # Safety
///
/// `handle` must be a valid, non-null [`kafka_common_ConsumerNoOffsetForPartitionError_t`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_partitions(
    handle: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *mut kafka_common_TopicPartitionList_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_ConsumerNoOffsetForPartitionError_t`; the only producer of such a
    // pointer is `kafka_common_Error_consumer_no_offset_for_partition`, which returns the
    // address of the `ConsumerNoOffsetForPartitionError` inside a live `ErrorInner`, so the
    // cast back is exact. The reference is used only within this call to clone
    // `partitions()` into the fresh owned `TopicPartitionList` handed to the caller, and
    // the parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_offset_out_of_range(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerOffsetOutOfRangeError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `ConsumerOffsetOutOfRangeError` stored inline in the `Error` enum
    // inside that same `ErrorInner` allocation, so it is valid exactly as long as the
    // parent handle, until `kafka_common_Error_destroy` (see the `Per-variant payload
    // accessors` comment above); the C caller must not destroy the error while using the
    // payload, a condition this function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(
    handle: *const kafka_common_ConsumerOffsetOutOfRangeError_t,
) -> *mut kafka_consumer_LongOffsetMap_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_ConsumerOffsetOutOfRangeError_t`; the only producer of such a pointer
    // is `kafka_common_Error_consumer_offset_out_of_range`, which returns the address of
    // the `ConsumerOffsetOutOfRangeError` inside a live `ErrorInner`, so the cast back is
    // exact. The reference is used only within this call to clone
    // `offset_out_of_range_partitions()` into the fresh owned `LongOffsetMap` handed to the
    // caller, and the parent error must still be alive (the payload is a borrow that dies
    // with `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
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
/// The returned pointer is borrowed from `error` and valid until `error` is
/// destroyed with [`kafka_common_Error_destroy`]; do not free it.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_too_large(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordTooLargeError_t {
    if error.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid handle from a function that returned an error, i.e. created by `box_error` and
    // not yet destroyed, which is `error_ref`'s precondition. The returned pointer is a
    // borrow of the `RecordTooLargeError` stored inline in the `Error` enum inside that
    // same `ErrorInner` allocation, so it is valid exactly as long as the parent handle,
    // until `kafka_common_Error_destroy` (see the `Per-variant payload accessors` comment
    // above); the C caller must not destroy the error while using the payload, a condition
    // this function's rustdoc does not yet state (see flags).
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordTooLargeError_record_too_large_partitions(
    handle: *const kafka_common_RecordTooLargeError_t,
) -> *mut kafka_consumer_LongOffsetMap_t {
    // SAFETY: Per this function's `# Safety`, `handle` is a valid, non-null
    // `kafka_common_RecordTooLargeError_t`; the only producer of such a pointer is
    // `kafka_common_Error_record_too_large`, which returns the address of the
    // `RecordTooLargeError` inside a live `ErrorInner`, so the cast back is exact. The
    // reference is used only within this call to clone `record_too_large_partitions()` into
    // a fresh owned `LongOffsetMap` (or return null when Java's field is null), and the
    // parent error must still be alive (the payload is a borrow that dies with
    // `kafka_common_Error_destroy`, see the `Per-variant payload accessors` comment).
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
// Completions are invoked from a single per-handle **dispatcher thread** that
// drains a completion queue, so user callbacks normally run on one predictable
// thread and never on a tokio worker (a slow callback cannot stall I/O). The
// exceptions fire inline on the calling thread: a request rejected before it is
// queued (null handle, busy access guard, marshaling failure, caught panic) and
// the post-teardown fallback in `enqueue_or_run_inline`.

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

/// Runs `f` on the dispatcher thread and awaits its return value.
///
/// This is how an **async** FFI adapter invokes a C callback whose *completion*
/// the Rust core must observe, rather than firing and forgetting it:
///
/// - the rebalance-listener callbacks return `Result<(), Error>` (a
///   `kafka_common_Error_t*` in C), so the adapter has to wait for the
///   return value;
/// - the commit callback returns nothing, but Java runs `onComplete` on the
///   thread inside `poll()` / `commitSync()`, so the adapter must not let that
///   call return before the C callback has (and must not let the C caller
///   release `user_data` while the job is still queued).
///
/// `f` therefore builds the owned C handles, calls the C function pointer, and
/// maps the result; all of that happens on the dispatcher thread, upholding the
/// invariant that user code never runs on a tokio worker. The awaiting task
/// yields its worker meanwhile, and because the dispatcher is a plain OS thread
/// the user callback may legally `block_on` a nested consumer operation.
///
/// **Head-of-line blocking caveat:** the job shares the single per-handle
/// dispatcher FIFO with every completion callback. A dispatcher callback that
/// block-waits on consumer progress (e.g. a delivery / commit callback that
/// waits for a rebalance to finish) deadlocks, because the job that would make
/// that progress is queued behind it. Dispatcher callbacks must not block on
/// consumer progress.
///
/// **Post-teardown:** once the dispatcher's receiver is gone,
/// [`enqueue_or_run_inline`] runs the job inline on the calling task instead —
/// so the returned value is still produced, but on a tokio worker, where a
/// nested `block_on` inside the user callback would panic.
pub(crate) async fn dispatch_and_wait<T, F>(tx: &std::sync::mpsc::Sender<CompletionJob>, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<T>();
    enqueue_or_run_inline(
        tx,
        Box::new(move || {
            // The receiver is awaited below and never dropped early, so the
            // only way `send` fails is a cancelled awaiting task — in which
            // case nobody observes the value anyway.
            let _ = result_tx.send(f());
        }),
    );
    // Infallible: `enqueue_or_run_inline` either hands the job to the live
    // dispatcher (which runs every queued job before exiting) or runs it inline,
    // so `result_tx` is always used before it is dropped.
    result_rx.await.expect("dispatch_and_wait job dropped without sending a result")
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
        // SAFETY: `callback` and `user_data` were supplied together by the C caller as one
        // `OperationCallbackTarget`, and `error` is null or a fresh `box_error` handle
        // (every constructor site in `admin.rs`, `consumer.rs` and `producer.rs` builds it
        // as `null_mut()` on `Ok` or `box_error(e)` on `Err`) whose ownership transfers to
        // the callee. `fire` takes `self` by value, so the pair fires exactly once per
        // completion, and per this method's `# Safety` the caller invokes it from the
        // `CompletionJob` drained by the dispatcher thread (or inline on the enqueuing task
        // once the dispatcher is gone, the documented `enqueue_or_run_inline` fallback);
        // the C user is responsible for the thread-safety of `user_data`, which the
        // callback contract keeps valid until the callback fires.
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

/// Ownership primitive for **multi-shot** callback registrations.
///
/// [`OperationCallbackTarget`] is `Copy` and fits one-shot operations, where the
/// C caller keeps `user_data` alive across a single completion. A long-lived
/// registration — a rebalance listener passed to `subscribe`, a commit callback
/// passed to `commit_async` — instead hands `user_data` to Rust for the whole
/// lifetime of the registration, so someone has to release it when the
/// registration goes away. `CallbackTarget` is that owner: it holds `user_data`
/// and an optional `destroy` hook that fires **exactly once**, when the adapter
/// holding this target is dropped (i.e. when the consumer drops the listener /
/// callback, on unsubscribe or close).
///
/// The hook is what lets a managed-runtime binding balance its reference count:
/// Python stores the listener `PyObject*` as `user_data`, `Py_INCREF`s it once
/// at registration time, and passes a `destroy` trampoline that takes the GIL
/// and `Py_DECREF`s it.
///
/// `destroy` may fire on **any thread** — whichever one drops the adapter (a
/// tokio worker running the consumer's background task, the dispatcher thread,
/// or the C thread calling `_destroy`). It must therefore be thread-agnostic
/// (Python: `PyGILState_Ensure`) and must not assume the calling thread already
/// holds any of the binding's locks.
pub(crate) struct CallbackTarget {
    /// Opaque pointer owned by this target for the lifetime of the registration.
    pub(crate) user_data: *mut std::ffi::c_void,
    /// Optional release hook, fired once from [`Drop`]. `None` means the C
    /// caller retains ownership of `user_data` (nothing to release).
    pub(crate) destroy: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
}

// SAFETY: `user_data` is an opaque pointer whose thread-safety is the C user's
// responsibility (identical to `OperationCompletion` / `OperationCallbackTarget`);
// the `destroy` function pointer is trivially shareable and, per the type's
// contract, callable from any thread. `CallbackTarget` itself never dereferences
// `user_data`.
unsafe impl Send for CallbackTarget {}
// SAFETY: a `&CallbackTarget` exposes only the opaque `user_data` value and the
// `Copy` function pointer, neither of which the type dereferences, so sharing it
// across threads adds no access beyond what the C contract on `user_data`
// already allows; `Drop`, the one use of both fields, takes `&mut self`.
unsafe impl Sync for CallbackTarget {}

impl Drop for CallbackTarget {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy {
            // SAFETY: `destroy` was supplied by the C caller together with
            // `user_data` and documented as callable from any thread. `Drop`
            // runs exactly once per target, so the hook fires exactly once.
            unsafe { destroy(self.user_data) };
        }
    }
}

/// Metric value kinds, mirroring [`crate::common::MetricValue`]'s variants.
///
/// Returned as a plain `int32_t` by the `..._MetricMap_get_value_kind`
/// accessor on each FFI surface to tell the caller which `get_value_*` accessor
/// is valid:
///
/// - `0` — `Double`: a measurable (or `Double`-valued gauge). Use
///   `get_value_double`.
/// - `1` — `String`: a string-valued gauge. Use `get_value_string`.
/// - `2` — `Long`: a long-valued gauge. Use `get_value_long`.
/// - `3` — `Int`: an integer-valued gauge. Use `get_value_int`.
///
/// These are plain integers rather than a C enum because `cbindgen.toml`
/// restricts `item_types` to functions/structs/typedefs — the generated header
/// contains no enums at all, and adding one type would mean exporting every
/// other enum reachable in the crate. (For the same reason cbindgen does not
/// emit these constants into the header; they are the Rust-side source of truth
/// shared by the consumer and producer surfaces and by their tests.)
pub(crate) const METRIC_VALUE_DOUBLE: i32 = 0;
/// See [`METRIC_VALUE_DOUBLE`].
pub(crate) const METRIC_VALUE_STRING: i32 = 1;
/// See [`METRIC_VALUE_DOUBLE`].
pub(crate) const METRIC_VALUE_LONG: i32 = 2;
/// See [`METRIC_VALUE_DOUBLE`].
pub(crate) const METRIC_VALUE_INT: i32 = 3;

/// One flattened metric entry. `MetricName`'s four fields plus the measured
/// value; tags are parallel key/value vectors so the C side can walk them by
/// index without another opaque type.
pub(crate) struct MetricEntry {
    pub(crate) name_c: CString,
    pub(crate) group_c: CString,
    pub(crate) description_c: CString,
    pub(crate) tag_keys: Vec<CString>,
    pub(crate) tag_values: Vec<CString>,
    pub(crate) kind: i32,
    pub(crate) double_value: f64,
    pub(crate) string_value: CString,
    pub(crate) long_value: i64,
    pub(crate) int_value: i32,
}

/// The heap-owned backing of an opaque `kafka_*_MetricMap_t` handle. Each
/// namespaced opaque type is a `#[repr(C)]` zero-sized placeholder that is cast
/// to `*const MetricMapInner` inside the accessors.
pub(crate) struct MetricMapInner {
    pub(crate) entries: Vec<MetricEntry>,
}

/// Builds the snapshot backing from a `metrics()` map. Each value is measured
/// exactly once here — the resulting handle is a point-in-time snapshot, the
/// only thing that can cross an FFI boundary without an upcall per read.
pub(crate) fn build_metric_map_inner(
    metrics: std::collections::HashMap<crate::common::MetricName, std::sync::Arc<crate::common::metrics::KafkaMetric>>,
) -> Box<MetricMapInner> {
    use crate::common::{Metric, MetricValue};
    let mut entries = Vec::with_capacity(metrics.len());
    for (name, metric) in metrics {
        // `metric_value()` is the one measurement taken for this snapshot.
        let value = metric.metric_value();
        let mut tag_keys = Vec::with_capacity(name.tags().len());
        let mut tag_values = Vec::with_capacity(name.tags().len());
        for (k, v) in name.tags() {
            tag_keys.push(CString::new(k.as_bytes()).unwrap_or_default());
            tag_values.push(CString::new(v.as_bytes()).unwrap_or_default());
        }
        let (kind, double_value, string_value, long_value, int_value) = match value {
            MetricValue::Double(d) => (METRIC_VALUE_DOUBLE, d, CString::default(), 0, 0),
            MetricValue::String(s) => (METRIC_VALUE_STRING, 0.0, CString::new(s.as_bytes()).unwrap_or_default(), 0, 0),
            MetricValue::Long(l) => (METRIC_VALUE_LONG, 0.0, CString::default(), l, 0),
            MetricValue::Int(i) => (METRIC_VALUE_INT, 0.0, CString::default(), 0, i),
        };
        entries.push(MetricEntry {
            name_c: CString::new(name.name().as_bytes()).unwrap_or_default(),
            group_c: CString::new(name.group().as_bytes()).unwrap_or_default(),
            description_c: CString::new(name.description().as_bytes()).unwrap_or_default(),
            tag_keys,
            tag_values,
            kind,
            double_value,
            string_value,
            long_value,
            int_value,
        });
    }
    Box::new(MetricMapInner { entries })
}

/// Resolves the entry at `index`, or `None` if out of range.
///
/// Returns a caller-scoped borrow rather than `&'static` — the entry is only
/// valid as long as the backing [`MetricMapInner`] allocation is, and it must
/// not be held past the matching `*_MetricMap_destroy` call.
///
/// # Safety
/// `inner` must be a valid pointer obtained from [`build_metric_map_inner`].
pub(crate) unsafe fn metric_entry<'a>(inner: *const MetricMapInner, index: i32) -> Option<&'a MetricEntry> {
    if index < 0 {
        return None;
    }
    // SAFETY: Per this function's `# Safety`, `inner` is a valid pointer obtained from
    // `build_metric_map_inner`, i.e. the `Box<MetricMapInner>` that
    // `kafka_producer_Producer_metrics` / the consumer's `box_metric_map` leaked with
    // `Box::into_raw` and that has not yet been passed to `*_MetricMap_destroy`. `index` is
    // rejected when negative and `entries.get` bounds-checks it. The `&'a MetricEntry` is
    // consumed by the `metric_map_get_*` wrappers within the same call, to copy a scalar or
    // take a `CString` pointer that lives in this allocation until that destroy, as the
    // wrappers' rustdoc (`borrowed; valid until the map is destroyed`) promises.
    unsafe { &*inner }.entries.get(index as usize)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_count(inner: *const MetricMapInner) -> i32 {
    // SAFETY: Per this function's `# Safety`, `inner` is a valid metric-map backing
    // pointer: the `Box<MetricMapInner>` that `kafka_producer_Producer_metrics` / the
    // consumer's `box_metric_map` leaked with `Box::into_raw` and that has not yet been
    // passed to `*_MetricMap_destroy`; the reference is used only to read `entries.len()`
    // within this call.
    unsafe { &*inner }.entries.len() as i32
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_name(inner: *const MetricMapInner, index: i32) -> *const c_char {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to take the `name_c` pointer, which lives in the same allocation until
    // `*_MetricMap_destroy`.
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.name_c.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_group(inner: *const MetricMapInner, index: i32) -> *const c_char {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to take the `group_c` pointer, which lives in the same allocation until
    // `*_MetricMap_destroy`.
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.group_c.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_description(inner: *const MetricMapInner, index: i32) -> *const c_char {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to take the `description_c` pointer, which lives in the same allocation
    // until `*_MetricMap_destroy`.
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.description_c.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_tag_count(inner: *const MetricMapInner, index: i32) -> i32 {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to read `tag_keys.len()`.
    unsafe { metric_entry(inner, index) }.map_or(-1, |e| e.tag_keys.len() as i32)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_tag_key(inner: *const MetricMapInner, index: i32, tag_index: i32) -> *const c_char {
    if tag_index < 0 {
        return std::ptr::null();
    }
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index`, `tag_index` is rejected when negative (checked
    // above) and `tag_keys.get` bounds-checks it; the entry borrow is used only within this
    // call to take a tag-key `CString` pointer, which lives in the same allocation until
    // `*_MetricMap_destroy`.
    unsafe { metric_entry(inner, index) }
        .and_then(|e| e.tag_keys.get(tag_index as usize))
        .map_or(std::ptr::null(), |k| k.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_tag_value(
    inner: *const MetricMapInner,
    index: i32,
    tag_index: i32,
) -> *const c_char {
    if tag_index < 0 {
        return std::ptr::null();
    }
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index`, `tag_index` is rejected when negative (checked
    // above) and `tag_values.get` bounds-checks it; the entry borrow is used only within
    // this call to take a tag-value `CString` pointer, which lives in the same allocation
    // until `*_MetricMap_destroy`.
    unsafe { metric_entry(inner, index) }
        .and_then(|e| e.tag_values.get(tag_index as usize))
        .map_or(std::ptr::null(), |v| v.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_kind(inner: *const MetricMapInner, index: i32) -> i32 {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to copy out the scalar `kind`.
    unsafe { metric_entry(inner, index) }.map_or(METRIC_VALUE_DOUBLE, |e| e.kind)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_double(inner: *const MetricMapInner, index: i32) -> f64 {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to copy out the scalar `double_value`.
    unsafe { metric_entry(inner, index) }.map_or(0.0, |e| e.double_value)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_string(inner: *const MetricMapInner, index: i32) -> *const c_char {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to take the `string_value` pointer, which lives in the same allocation
    // until `*_MetricMap_destroy`.
    unsafe { metric_entry(inner, index) }.map_or(std::ptr::null(), |e| e.string_value.as_ptr())
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_long(inner: *const MetricMapInner, index: i32) -> i64 {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to copy out the scalar `long_value`.
    unsafe { metric_entry(inner, index) }.map_or(0, |e| e.long_value)
}

/// # Safety
/// `inner` must be a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_get_value_int(inner: *const MetricMapInner, index: i32) -> i32 {
    // SAFETY: `metric_entry` requires `inner` to be a valid pointer from
    // `build_metric_map_inner`; per this function's `# Safety`, `inner` is a valid
    // metric-map backing pointer, and the only way to obtain one is that builder's `Box`
    // leaked via `Box::into_raw` in the `*_metrics` entry points and not yet destroyed.
    // `metric_entry` range-checks `index` itself, and the entry borrow is used only within
    // this call to copy out the scalar `int_value`.
    unsafe { metric_entry(inner, index) }.map_or(0, |e| e.int_value)
}

/// Reclaims a metric-map backing pointer. Safe with null (no-op).
///
/// # Safety
/// `inner` must be null or a valid metric-map backing pointer.
pub(crate) unsafe fn metric_map_destroy(inner: *mut MetricMapInner) {
    if !inner.is_null() {
        // SAFETY: `inner` is non-null (checked above) and, per this function's `# Safety`,
        // a valid metric-map backing pointer: the `Box<MetricMapInner>` that
        // `kafka_producer_Producer_metrics` / the consumer's `box_metric_map` leaked with
        // `Box::into_raw`. Its only callers are the `*_MetricMap_destroy` entry points,
        // whose contract makes the handle invalid afterwards, so this `Box::from_raw` is
        // the single, final use of the allocation.
        unsafe { drop(Box::from_raw(inner)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::KafkaError;
    use crate::common::error::ErrorName;
    use crate::common::errors::{
        ApiError, AuthenticationError, AuthorizationError, AuthorizerNotReadyError, DisconnectError,
        DuplicateResourceError, GroupAuthorizationError, InterruptError, InvalidOffsetError, InvalidTopicError,
        RecordTooLargeError, ResourceNotFoundError, SslAuthenticationError, ThrottlingQuotaExceededError,
        TopicAuthorizationError,
    };
    use crate::common::metrics::QuotaViolationError;
    use crate::common::network::InvalidReceiveError;
    use crate::common::{MetricName, TopicPartition};
    use crate::consumer::{
        ConsumerCommitFailedError, ConsumerLogTruncationError, ConsumerNoOffsetForPartitionError,
        ConsumerOffsetOutOfRangeError, ConsumerRetriableCommitFailedError,
    };
    use crate::ffi::consumer::{
        kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_partition, kafka_common_TopicPartition_topic,
        kafka_common_TopicPartitionList_count, kafka_common_TopicPartitionList_destroy,
        kafka_common_TopicPartitionList_get, kafka_consumer_LongOffsetMap_count, kafka_consumer_LongOffsetMap_destroy,
        kafka_consumer_LongOffsetMap_get_value, kafka_consumer_OffsetMap_count, kafka_consumer_OffsetMap_destroy,
        kafka_consumer_StringList_count, kafka_consumer_StringList_destroy, kafka_consumer_StringList_get,
        kafka_consumer_string_destroy,
    };
    use std::cell::RefCell;
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
            (Error::config_message("m"), kafka_common_ErrorCode_CONFIG, -10),
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
        assert_eq!(
            error_code_of(&Error::kafka_message("m")),
            kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR
        );
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
        use crate::common::errors::DeserializationErrorOrigin;
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
        use crate::common::errors::DeserializationErrorOrigin;
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
            Some(RecordHeaders::with_header_iter(vec![RecordHeader::new(
                "h1".to_string(),
                Some(vec![9, 9]),
            )])),
            "m",
        )
    }

    /// A benign error of a different variant, used to assert every extraction
    /// function returns null when the handle is not its variant.
    fn other_error() -> Error {
        Error::kafka_message("other")
    }

    #[test]
    fn topic_authorization_payload() {
        let mut topics = HashSet::new();
        topics.insert("t1".to_string());
        let error = box_error(Error::TopicAuthorization(TopicAuthorizationError::new(topics.clone())));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety` (a valid, not yet
        // destroyed handle). `handle` is a borrow into `error` used only before that
        // destroy; `list` is the owned `StringList` the accessor hands over, read while
        // alive (`count` asserted 1 before `get(list, 0)`, and the string copied out via
        // `CStr::from_ptr`) and destroyed exactly once with
        // `kafka_consumer_StringList_destroy`.
        unsafe {
            let handle = kafka_common_Error_topic_authorization(error);
            assert!(!handle.is_null());
            let list = kafka_common_TopicAuthorizationError_unauthorized_topics(handle);
            assert_eq!(kafka_consumer_StringList_count(list), 1);
            let got = CStr::from_ptr(kafka_consumer_StringList_get(list, 0)).to_str().unwrap();
            assert_eq!(got, "t1");
            kafka_consumer_StringList_destroy(list);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_topic_authorization(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn group_authorization_payload() {
        let error = box_error(Error::GroupAuthorization(GroupAuthorizationError::for_group_id("g1")));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `group_id_ptr` is the owned
        // NUL-terminated string the accessor hands over, read via `CStr::from_ptr` and
        // freed exactly once with `kafka_consumer_string_destroy`.
        unsafe {
            let handle = kafka_common_Error_group_authorization(error);
            assert!(!handle.is_null());
            let group_id_ptr = kafka_common_GroupAuthorizationError_group_id(handle);
            let group_id = CStr::from_ptr(group_id_ptr).to_str().unwrap();
            assert_eq!(group_id, "g1");
            kafka_consumer_string_destroy(group_id_ptr);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_group_authorization(other).is_null());
            kafka_common_Error_destroy(other);

            // Java's `groupId()` is null for the `Errors` builder's error, so
            // C gets null, not "".
            for error in [
                Error::GroupAuthorization(GroupAuthorizationError::with_default_message()),
                Errors::GroupAuthorizationFailed
                    .error_with_message("denied")
                    .expect("a real code"),
            ] {
                let error = box_error(error);
                let handle = kafka_common_Error_group_authorization(error);
                assert!(!handle.is_null());
                assert!(kafka_common_GroupAuthorizationError_group_id(handle).is_null());
                kafka_common_Error_destroy(error);
            }
        }
    }

    #[test]
    fn invalid_topic_payload() {
        let mut topics = HashSet::new();
        topics.insert("bad".to_string());
        let error = box_error(Error::InvalidTopic(InvalidTopicError::new(topics)));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `list` is the owned `StringList` the
        // accessor hands over, read while alive (`count` asserted 1 before `get(list, 0)`,
        // and the string copied out via `CStr::from_ptr`) and destroyed exactly once with
        // `kafka_consumer_StringList_destroy`.
        unsafe {
            let handle = kafka_common_Error_invalid_topic(error);
            assert!(!handle.is_null());
            let list = kafka_common_InvalidTopicError_invalid_topics(handle);
            assert_eq!(kafka_consumer_StringList_count(list), 1);
            let got = CStr::from_ptr(kafka_consumer_StringList_get(list, 0)).to_str().unwrap();
            assert_eq!(got, "bad");
            kafka_consumer_StringList_destroy(list);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_invalid_topic(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn duplicate_resource_payload() {
        let error = box_error(Error::DuplicateResource(DuplicateResourceError::with_resource("res1", "m")));
        // SAFETY: Test code: `error`, `no_resource` and `other` are fresh handles from
        // `box_error` created in this test, each passed to `kafka_common_Error_destroy`
        // exactly once after its last use, so every call satisfies the accessors' `#
        // Safety`. `handle` and `no_resource_handle` are borrows into their parent used
        // only before that parent's destroy; `resource_ptr` is the owned NUL-terminated
        // string the accessor hands over, read via `CStr::from_ptr` and freed exactly once
        // with `kafka_consumer_string_destroy`, while the no-resource case only tests the
        // returned pointer for null.
        unsafe {
            let handle = kafka_common_Error_duplicate_resource(error);
            assert!(!handle.is_null());
            let resource_ptr = kafka_common_DuplicateResourceError_resource(handle);
            let resource = CStr::from_ptr(resource_ptr).to_str().unwrap();
            assert_eq!(resource, "res1");
            kafka_consumer_string_destroy(resource_ptr);
            kafka_common_Error_destroy(error);

            // No resource recorded -> the accessor returns null.
            let no_resource = box_error(Error::DuplicateResource(DuplicateResourceError::new("m")));
            let no_resource_handle = kafka_common_Error_duplicate_resource(no_resource);
            assert!(kafka_common_DuplicateResourceError_resource(no_resource_handle).is_null());
            kafka_common_Error_destroy(no_resource);

            let other = box_error(other_error());
            assert!(kafka_common_Error_duplicate_resource(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn resource_not_found_payload() {
        let error = box_error(Error::ResourceNotFound(ResourceNotFoundError::with_resource("res2", "m")));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `resource_ptr` is the owned
        // NUL-terminated string the accessor hands over, read via `CStr::from_ptr` and
        // freed exactly once with `kafka_consumer_string_destroy`.
        unsafe {
            let handle = kafka_common_Error_resource_not_found(error);
            assert!(!handle.is_null());
            let resource_ptr = kafka_common_ResourceNotFoundError_resource(handle);
            let resource = CStr::from_ptr(resource_ptr).to_str().unwrap();
            assert_eq!(resource, "res2");
            kafka_consumer_string_destroy(resource_ptr);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_resource_not_found(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn throttling_quota_exceeded_payload() {
        let error = box_error(Error::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(123, "m")));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy, and the accessor merely copies out a
        // scalar.
        unsafe {
            let handle = kafka_common_Error_throttling_quota_exceeded(error);
            assert!(!handle.is_null());
            assert_eq!(kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(handle), 123);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_throttling_quota_exceeded(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn record_deserialization_payload() {
        let error = box_error(Error::RecordDeserialization(Box::new(record_deserialization_error_full())));
        // SAFETY: Test code: `error`, `sparse` and `other` are fresh handles from
        // `box_error` created in this test, each passed to `kafka_common_Error_destroy`
        // exactly once after its last use, so every call satisfies the accessors' `#
        // Safety`. `handle` and `sparse_handle` are borrows into their parent used only
        // before that parent's destroy; `partition` is the owned `TopicPartition` the
        // accessor hands over, read and destroyed exactly once; `key_ptr`, `value_ptr`,
        // `hkey_ptr` and `hvalue_ptr` borrow buffers inside `error` and are read with
        // `slice::from_raw_parts` using exactly the lengths the accessors wrote, all before
        // `kafka_common_Error_destroy(error)`; the `&mut *_len` out-pointers are live
        // locals that outlive each call.
        unsafe {
            let handle = kafka_common_Error_record_deserialization(error);
            assert!(!handle.is_null());

            assert_eq!(kafka_common_RecordDeserializationError_origin(handle), 0, "Key -> 0");

            let partition = kafka_common_RecordDeserializationError_partition(handle);
            assert!(!partition.is_null());
            let topic = CStr::from_ptr(kafka_common_TopicPartition_topic(partition)).to_str().unwrap();
            assert_eq!(topic, "t2");
            assert_eq!(kafka_common_TopicPartition_partition(partition), 5);
            kafka_common_TopicPartition_destroy(partition);

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
            let sparse_handle = kafka_common_Error_record_deserialization(sparse);
            // `record_deserialization_error()` uses `DeserializationErrorOrigin::Value`.
            assert_eq!(kafka_common_RecordDeserializationError_origin(sparse_handle), 1, "Value -> 1");
            let mut none_len = -2;
            assert!(kafka_common_RecordDeserializationError_key_buffer(sparse_handle, &mut none_len).is_null());
            assert_eq!(none_len, -1);
            assert_eq!(kafka_common_RecordDeserializationError_header_count(sparse_handle), 0);
            kafka_common_Error_destroy(sparse);

            let other = box_error(other_error());
            assert!(kafka_common_Error_record_deserialization(other).is_null());
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
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `name_ptr` and `group_ptr` are owned
        // NUL-terminated strings the accessors hand over, each read via `CStr::from_ptr`
        // and freed exactly once with `kafka_consumer_string_destroy`; the remaining
        // accessors copy out scalars.
        unsafe {
            let handle = kafka_common_Error_quota_violation(error);
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
            assert!(kafka_common_Error_quota_violation(other).is_null());
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
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `offset_map` and `divergent_map` are
        // owned handles the accessors hand over, read while alive (`count` asserted 1
        // before `get_value(offset_map, 0)`) and each destroyed exactly once with its
        // matching `*_destroy`.
        unsafe {
            let handle = kafka_common_Error_consumer_log_truncation(error);
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
            assert!(kafka_common_Error_consumer_log_truncation(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn consumer_no_offset_for_partition_payload() {
        let error = box_error(Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(
            TopicPartition::new("t", 3),
        )));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `list` is the owned
        // `TopicPartitionList` the accessor hands over, `tp` is a borrow into `list` read
        // before `kafka_common_TopicPartitionList_destroy` (`count` asserted 1 before
        // `get(list, 0)`), and `list` is destroyed exactly once.
        unsafe {
            let handle = kafka_common_Error_consumer_no_offset_for_partition(error);
            assert!(!handle.is_null());
            let list = kafka_common_ConsumerNoOffsetForPartitionError_partitions(handle);
            assert_eq!(kafka_common_TopicPartitionList_count(list), 1);
            let tp = kafka_common_TopicPartitionList_get(list, 0);
            assert_eq!(kafka_common_TopicPartition_partition(tp), 3);
            kafka_common_TopicPartitionList_destroy(list);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_consumer_no_offset_for_partition(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn consumer_offset_out_of_range_payload() {
        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("t", 0), 77i64);
        let error = box_error(Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(offsets)));
        // SAFETY: Test code: `error` and `other` are fresh handles from `box_error` created
        // in this test, each passed to `kafka_common_Error_destroy` exactly once after its
        // last use, so every call satisfies the accessors' `# Safety`. `handle` is a borrow
        // into `error` used only before that destroy; `map` is the owned `LongOffsetMap`
        // the accessor hands over, read while alive (`count` asserted 1 before
        // `get_value(map, 0)`) and destroyed exactly once with
        // `kafka_consumer_LongOffsetMap_destroy`.
        unsafe {
            let handle = kafka_common_Error_consumer_offset_out_of_range(error);
            assert!(!handle.is_null());
            let map = kafka_common_ConsumerOffsetOutOfRangeError_offset_out_of_range_partitions(handle);
            assert_eq!(kafka_consumer_LongOffsetMap_count(map), 1);
            assert_eq!(kafka_consumer_LongOffsetMap_get_value(map, 0), 77);
            kafka_consumer_LongOffsetMap_destroy(map);
            kafka_common_Error_destroy(error);

            let other = box_error(other_error());
            assert!(kafka_common_Error_consumer_offset_out_of_range(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn record_too_large_payload() {
        let mut partitions = HashMap::new();
        partitions.insert(TopicPartition::new("t", 0), 999i64);
        let error = box_error(Error::RecordTooLarge(RecordTooLargeError::with_record_too_large_partitions(
            "m", partitions,
        )));
        // SAFETY: Test code: `error`, `no_partitions` and `other` are fresh handles from
        // `box_error` created in this test, each passed to `kafka_common_Error_destroy`
        // exactly once after its last use, so every call satisfies the accessors' `#
        // Safety`. `handle` and `no_partitions_handle` are borrows into their parent used
        // only before that parent's destroy; `map` is the owned `LongOffsetMap` the
        // accessor hands over, read while alive (`count` asserted 1 before `get_value(map,
        // 0)`) and destroyed exactly once, while the null-field case only tests the
        // returned pointer for null.
        unsafe {
            let handle = kafka_common_Error_record_too_large(error);
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
            let no_partitions_handle = kafka_common_Error_record_too_large(no_partitions);
            assert!(kafka_common_RecordTooLargeError_record_too_large_partitions(no_partitions_handle).is_null());
            kafka_common_Error_destroy(no_partitions);

            let other = box_error(other_error());
            assert!(kafka_common_Error_record_too_large(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    // -----------------------------------------------------------------------
    // #[ffi_guard] — decisions D2–D5 of
    // design/current/appsec-7665-4521-ffi-panic-guard.md
    // -----------------------------------------------------------------------
    //
    // Each `guarded_*` function is an `extern "C"` function under
    // `#[ffi_guard]` without `no_mangle`. Its body panics when asked to, and the
    // test checks the value the guard returned in place of the unwind that would
    // otherwise abort the process.

    /// Reads a boxed error's message and code, then frees it.
    ///
    /// # Safety
    ///
    /// `error` must be a live error handle returned by an FFI call (null fails the
    /// assertion); it is freed here and must not be used afterwards.
    unsafe fn take_error(error: *mut kafka_common_Error_t) -> (String, kafka_common_ErrorCode_t) {
        assert!(!error.is_null(), "expected an error handle");
        // SAFETY: Test helper: `error` is non-null (asserted above) and every caller passes
        // a handle freshly produced by `#[ffi_guard]`'s panic path
        // (`box_error(panic_error(..))`, either returned or stored through `out_error`) and
        // not yet destroyed, which satisfies `kafka_common_Error_message`'s `# Safety`. For
        // a non-null handle it returns the NUL-terminated `message_cstring` pointer, valid
        // until destroy, and the text is copied into an owned `String` before the handle is
        // freed below.
        let message = unsafe { CStr::from_ptr(kafka_common_Error_message(error)) }
            .to_str()
            .unwrap()
            .to_owned();
        // SAFETY: Test helper: `error` is non-null (asserted above) and a fresh, not yet
        // destroyed `box_error` handle from `#[ffi_guard]`'s panic path, which satisfies
        // `kafka_common_Error_code`'s `# Safety`; the call only copies out the code.
        let code = unsafe { kafka_common_Error_code(error) };
        // SAFETY: Test helper: `error` is non-null (asserted above) and a fresh `box_error`
        // handle from `#[ffi_guard]`'s panic path that no other code frees (callers pass it
        // here exactly once), so this `kafka_common_Error_destroy` is the single, final
        // use, as its `# Safety` requires.
        unsafe { kafka_common_Error_destroy(error) };
        (message, code)
    }

    /// The message [`panic_error`] builds for a panic in `function`.
    fn panic_message(function: &str, text: &str) -> String {
        format!("Rust panic caught at the FFI boundary in {function}: {text}")
    }

    thread_local! {
        /// Set by `guarded_unit` just before it panics.
        static UNIT_BODY_RAN: RefCell<bool> = const { RefCell::new(false) };
        /// What `guarded_on_panic`'s closure received.
        static ON_PANIC_SAW: RefCell<Option<(String, kafka_common_ErrorCode_t)>> = const { RefCell::new(None) };
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_unit() {
        UNIT_BODY_RAN.with(|ran| *ran.borrow_mut() = true);
        panic!("unit boom");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_bool(panic: bool) -> bool {
        if panic {
            panic!("bool boom");
        }
        true
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i16(panic: bool) -> i16 {
        if panic {
            panic!("i16 boom");
        }
        16
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i32(panic: bool) -> i32 {
        if panic {
            panic!("i32 boom");
        }
        32
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_i64(panic: bool) -> i64 {
        if panic {
            panic!("i64 boom");
        }
        64
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_topics_count(panic: bool) -> i32 {
        if panic {
            panic!("count boom");
        }
        3
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_offsets_count(panic: bool) -> i64 {
        if panic {
            panic!("count boom");
        }
        3
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_f64(panic: bool) -> f64 {
        if panic {
            panic!("f64 boom");
        }
        1.5
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_error_code(panic: bool) -> kafka_common_ErrorCode_t {
        if panic {
            panic!("code boom");
        }
        kafka_common_ErrorCode_NONE
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_error_return(panic: bool) -> *mut kafka_common_Error_t {
        if panic {
            panic!("error boom");
        }
        std::ptr::null_mut()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_borrowed_error(panic: bool) -> *const kafka_common_Error_t {
        if panic {
            panic!("borrowed boom");
        }
        std::ptr::null()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_mut_pointer(panic: bool) -> *mut u8 {
        if panic {
            panic!("pointer boom");
        }
        std::ptr::dangling_mut()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_const_pointer(panic: bool) -> *const c_char {
        if panic {
            panic!("string boom");
        }
        c"value".as_ptr()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; `out_error` must be null or point to a writable slot, because the guard
    /// stores the panic's error handle there.
    #[ffi_guard]
    unsafe extern "C" fn guarded_new(panic: bool, out_error: *mut *mut kafka_common_Error_t) -> *mut u8 {
        if panic {
            panic!("constructor boom");
        }
        std::ptr::dangling_mut()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; `out_error` must be null or point to a writable slot, because the guard
    /// stores the panic's error handle there.
    #[ffi_guard]
    unsafe extern "C" fn guarded_flush(out_error: *mut *mut kafka_common_Error_t) {
        panic!("flush boom");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; `out_error` must be null or point to a writable slot, because the guard
    /// stores the panic's error handle there.
    #[ffi_guard(fallback = 42)]
    unsafe extern "C" fn guarded_fallback(out_error: *mut *mut kafka_common_Error_t) -> i32 {
        panic!("fallback boom");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; `out_error` is read by nothing (the custom `on_panic` path does not store
    /// into it), so the caller has nothing to uphold.
    #[ffi_guard(on_panic = |err| {
        ON_PANIC_SAW.with(|saw| *saw.borrow_mut() = Some((err.message().to_owned(), error_code_of(&err))));
        7
    })]
    unsafe extern "C" fn guarded_on_panic(out_error: *mut *mut kafka_common_Error_t) -> i32 {
        // Named `out_error` on purpose: the test checks nothing is stored there.
        let _ = out_error;
        panic!("custom path");
    }

    /// A callback-style entry point whose spawn panics the way tokio's can after
    /// queueing the task. `on_panic` prints instead of firing a callback, so the
    /// child process of `spawn_panic_aborts_before_the_guard` can show it never ran.
    ///
    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard(on_panic = |err| println!("on_panic fired: {}", err.message()))]
    unsafe extern "C" fn guarded_spawn_panics() {
        println!("spawning");
        spawn_or_abort::<()>(|| panic!("failed to wake I/O driver"));
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_formatted(value: i32) -> i32 {
        panic!("formatted payload {value}");
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_unwrap() -> i32 {
        // `black_box` keeps the `None` opaque, so this is a real runtime panic.
        std::hint::black_box(None::<i32>).unwrap()
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_non_string_payload() -> i32 {
        std::panic::panic_any(42_u32)
    }

    /// A panic payload whose destructor panics as well.
    struct PanicsOnDrop;

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("payload destructor");
        }
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_payload_panics_on_drop() -> i32 {
        std::panic::panic_any(PanicsOnDrop)
    }

    /// # Safety
    ///
    /// `unsafe` only because `#[ffi_guard]` requires the `unsafe extern "C"` entry-point
    /// shape; the function takes no pointer, so the caller has nothing to uphold.
    #[ffi_guard]
    unsafe extern "C" fn guarded_early_return(early: bool) -> i32 {
        if early {
            return 5;
        }
        6
    }

    #[test]
    fn ffi_guard_unit_return_is_logged_only() {
        // SAFETY: Test code: `guarded_unit` is a parameterless `extern "C"` test function
        // under `#[ffi_guard]`; the call is `unsafe` only because the function is declared
        // `unsafe extern "C"`, and it has no pointer arguments or preconditions to uphold.
        // The guard catches its panic, so nothing unwinds into this frame.
        unsafe { guarded_unit() };
        assert!(UNIT_BODY_RAN.with(|ran| *ran.borrow()), "the body ran up to its panic");
    }

    #[test]
    fn ffi_guard_scalar_fallbacks() {
        // SAFETY: Test code: every `guarded_*` function called here is an `extern "C"` test
        // function under `#[ffi_guard]` taking only a `bool`; the calls are `unsafe` solely
        // because the functions are declared `unsafe extern "C"`, and there are no pointer
        // arguments or preconditions to uphold. The guard turns each requested panic into
        // the documented fallback value.
        unsafe {
            assert!(!guarded_bool(true));
            assert_eq!(guarded_i16(true), -1);
            assert_eq!(guarded_i32(true), -1);
            assert_eq!(guarded_i64(true), -1);
            assert!(guarded_f64(true).is_nan());
        }
    }

    #[test]
    fn ffi_guard_count_fallback_is_zero() {
        // SAFETY: Test code: `guarded_topics_count` and `guarded_offsets_count` are `extern
        // "C"` test functions under `#[ffi_guard]` taking only a `bool`; the calls are
        // `unsafe` solely because the functions are declared `unsafe extern "C"`, with no
        // pointer arguments or preconditions to uphold. The guard turns the requested panic
        // into the documented `0` count fallback.
        unsafe {
            assert_eq!(guarded_topics_count(true), 0);
            assert_eq!(guarded_offsets_count(true), 0);
        }
    }

    #[test]
    fn ffi_guard_error_code_fallback_is_unknown_server_error() {
        // SAFETY: Test code: `guarded_error_code` is an `extern "C"` test function under
        // `#[ffi_guard]` taking only a `bool`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard turns the requested panic into the documented error-code
        // fallback.
        let code = unsafe { guarded_error_code(true) };
        // The enumerator `error_code_of` produces for `Errors::UnknownServerError`.
        assert_eq!(code, code_owned_by(Errors::UnknownServerError));
        assert_eq!(code, kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn ffi_guard_error_return_reports_the_panic() {
        // SAFETY: Test code: `guarded_error_return(true)` takes only a `bool` and panics,
        // so the guard returns a fresh `box_error(panic_error(..))` handle (the `*mut
        // kafka_common_Error_t` fallback in `ffi-macros`), which satisfies `take_error`'s
        // expectation of a non-null, not yet destroyed handle; `take_error` asserts
        // non-null and frees it exactly once.
        let (message, code) = unsafe { take_error(guarded_error_return(true)) };
        assert_eq!(message, panic_message("guarded_error_return", "error boom"));
        assert_eq!(code, kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE);
    }

    #[test]
    fn ffi_guard_borrowed_error_return_reports_the_panic() {
        // SAFETY: Test code: `guarded_borrowed_error(true)` takes only a `bool`; on its
        // panic the guard returns a fresh `box_error(..)` handle cast to `*const`, leaked
        // by design for a borrowing accessor (the `*const kafka_common_Error_t` fallback in
        // `ffi-macros`), so nothing else owns or frees it.
        let error = unsafe { guarded_borrowed_error(true) };
        // The handle is leaked by design (the caller of a borrowing accessor
        // never frees); the test knows it is fresh and frees it.
        // SAFETY: Test code: `error` is the fresh, never-freed handle the guard built on
        // the previous line, so casting away `const` and consuming it via `take_error`
        // (which asserts non-null) is the single, final use, as the comment above records.
        let (message, code) = unsafe { take_error(error as *mut kafka_common_Error_t) };
        assert_eq!(message, panic_message("guarded_borrowed_error", "borrowed boom"));
        assert_eq!(code, kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE);
    }

    #[test]
    fn ffi_guard_pointer_fallbacks_are_null() {
        // SAFETY: Test code: `guarded_mut_pointer` and `guarded_const_pointer` are `extern
        // "C"` test functions under `#[ffi_guard]` taking only a `bool`; the calls are
        // `unsafe` solely because the functions are declared `unsafe extern "C"`, with no
        // pointer arguments or preconditions to uphold. The guard turns the requested panic
        // into the documented null fallback, and the results are only tested for null,
        // never dereferenced.
        unsafe {
            assert!(guarded_mut_pointer(true).is_null());
            assert!(guarded_const_pointer(true).is_null());
        }
    }

    #[test]
    fn ffi_guard_stores_the_error_in_out_error() {
        let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: Test code: `out_error` is a live local that outlives the call, so `&mut
        // out_error` is a valid `*mut *mut kafka_common_Error_t`; the guard's generated
        // store writes exactly one fresh `box_error` handle into it when the body panics
        // (`ffi-macros`, D3), and the `bool` argument needs nothing further.
        let result = unsafe { guarded_new(true, &mut out_error) };
        assert!(result.is_null());
        // SAFETY: Test code: `out_error` now holds the fresh handle the guard stored on the
        // panic path, not yet freed by anyone, so `take_error` (which asserts non-null)
        // consumes it exactly once.
        let (message, code) = unsafe { take_error(out_error) };
        assert_eq!(message, panic_message("guarded_new", "constructor boom"));
        assert_eq!(code, kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE);

        // A unit function reports through `out_error` too.
        let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: Test code: `out_error` is a live local that outlives the call, so `&mut
        // out_error` is a valid `*mut *mut kafka_common_Error_t`; the guard's generated
        // store writes exactly one fresh `box_error` handle into it when the body panics
        // (`ffi-macros`, D3).
        unsafe { guarded_flush(&mut out_error) };
        // SAFETY: Test code: `out_error` now holds the fresh handle the guard stored on the
        // panic path, not yet freed by anyone, so `take_error` (which asserts non-null)
        // consumes it exactly once.
        let (message, _) = unsafe { take_error(out_error) };
        assert_eq!(message, panic_message("guarded_flush", "flush boom"));
    }

    #[test]
    fn ffi_guard_tolerates_null_out_error() {
        // SAFETY: Test code: a NULL `out_error` is passed deliberately to exercise the
        // guard's documented null tolerance: the generated store is skipped when the
        // pointer is null, so nothing is written through it, and the `bool` argument needs
        // nothing further.
        unsafe {
            assert!(guarded_new(true, std::ptr::null_mut()).is_null());
            guarded_flush(std::ptr::null_mut());
        }
    }

    #[test]
    fn ffi_guard_fallback_override_keeps_the_out_error_store() {
        let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: Test code: `out_error` is a live local that outlives the call, so `&mut
        // out_error` is a valid `*mut *mut kafka_common_Error_t`; the `fallback = 42`
        // override replaces only the return value and keeps the generated store, which
        // writes exactly one fresh `box_error` handle into it when the body panics
        // (`ffi-macros`, D3).
        assert_eq!(unsafe { guarded_fallback(&mut out_error) }, 42);
        // SAFETY: Test code: `out_error` now holds the fresh handle the guard stored on the
        // panic path, not yet freed by anyone, so `take_error` (which asserts non-null)
        // consumes it exactly once.
        let (message, _) = unsafe { take_error(out_error) };
        assert_eq!(message, panic_message("guarded_fallback", "fallback boom"));
    }

    #[test]
    fn ffi_guard_on_panic_closure_takes_full_control() {
        let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
        // SAFETY: Test code: `out_error` is a live local that outlives the call, so `&mut
        // out_error` is a valid `*mut *mut kafka_common_Error_t`; with an explicit
        // `on_panic` closure no store is generated (asserted null afterwards), so nothing
        // is written through it.
        assert_eq!(unsafe { guarded_on_panic(&mut out_error) }, 7);
        let (message, code) = ON_PANIC_SAW.with(|saw| saw.borrow_mut().take()).expect("the closure ran");
        assert_eq!(message, panic_message("guarded_on_panic", "custom path"));
        assert_eq!(code, kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE);
        // No `out_error` store is generated around an explicit closure.
        assert!(out_error.is_null());
    }

    #[test]
    fn spawn_callback_task_runs_the_task() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        // From a thread that is not one of the runtime's workers, like a C caller's.
        let task = spawn_callback_task(runtime.handle(), async { 7 });
        assert_eq!(runtime.block_on(task).expect("the task ran"), 7);
    }

    /// A panic inside the spawn aborts the process before the guard sees it: the
    /// queued task would fire the callback, so `on_panic` firing it as well would
    /// be a second completion (§4 item 6 of
    /// `design/current/appsec-7665-4521-ffi-panic-guard.md`). The abort would end
    /// this test binary, so the test runs itself again in a child process, which
    /// takes the `CHILD` branch.
    #[test]
    fn spawn_panic_aborts_before_the_guard() {
        const CHILD: &str = "CONFLUENT_KAFKA_SPAWN_PANIC_CHILD";
        if std::env::var_os(CHILD).is_some() {
            // SAFETY: Test code: `guarded_spawn_panics` takes no arguments and has no
            // preconditions; the call is `unsafe` solely because it is declared `unsafe
            // extern "C"`.
            unsafe { guarded_spawn_panics() };
            println!("returned");
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test binary path"))
            .args(["--exact", "ffi::common::tests::spawn_panic_aborts_before_the_guard"])
            .args(["--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("run the child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stdout.contains("spawning"),
            "the child never reached the spawn:\n{stdout}\n{stderr}"
        );
        assert!(stderr.contains("failed to wake I/O driver"), "no panic in the child:\n{stderr}");
        assert!(!stdout.contains("on_panic fired"), "the panic reached the guard:\n{stdout}");
        assert!(!stdout.contains("returned"), "the entry point returned:\n{stdout}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            // SIGABRT (6 on Linux and macOS), which `std::process::abort` raises.
            assert_eq!(
                output.status.signal(),
                Some(6),
                "child exited with {}:\n{stderr}",
                output.status
            );
        }
        #[cfg(not(unix))]
        assert!(!output.status.success(), "child exited with {}", output.status);
    }

    #[test]
    fn ffi_guard_string_and_str_payloads() {
        // `panic!` with arguments carries a `String`.
        let mut seen = None;
        let value = ffi_guard_or(
            "formatted",
            |err| {
                seen = Some(err.message().to_owned());
                -1
            },
            || -> i32 {
                let value = 9;
                panic!("formatted payload {value}")
            },
        );
        assert_eq!(value, -1);
        assert_eq!(seen.unwrap(), panic_message("formatted", "formatted payload 9"));
        // SAFETY: Test code: `guarded_formatted` is an `extern "C"` test function under
        // `#[ffi_guard]` taking only an `i32`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard turns its panic into the documented `-1` fallback.
        assert_eq!(unsafe { guarded_formatted(3) }, -1);

        // A literal `panic!`, and `unwrap` on `None`, carry a `&'static str`.
        let payload: Box<dyn Any + Send> = Box::new("literal payload");
        assert_eq!(panic_error("f", &*payload).message(), panic_message("f", "literal payload"));
        let payload: Box<dyn Any + Send> = Box::new(String::from("owned payload"));
        assert_eq!(panic_error("f", &*payload).message(), panic_message("f", "owned payload"));
        let mut seen = None;
        ffi_guard_or(
            "unwrap",
            |err| seen = Some(err.message().to_owned()),
            || std::hint::black_box(None::<()>).unwrap(),
        );
        assert_eq!(
            seen.unwrap(),
            panic_message("unwrap", "called `Option::unwrap()` on a `None` value")
        );
        // SAFETY: Test code: `guarded_unwrap` is a parameterless `extern "C"` test function
        // under `#[ffi_guard]`; the call is `unsafe` solely because it is declared `unsafe
        // extern "C"`, with no pointer arguments or preconditions to uphold. The guard
        // turns its `unwrap` panic into the documented `-1` fallback.
        assert_eq!(unsafe { guarded_unwrap() }, -1);
    }

    #[test]
    fn ffi_guard_non_string_payload() {
        let payload: Box<dyn Any + Send> = Box::new(42_u32);
        let error = panic_error("f", &*payload);
        assert_eq!(error.message(), panic_message("f", "non-string panic payload"));
        assert_eq!(error_code_of(&error), kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE);
        // SAFETY: Test code: `guarded_non_string_payload` is a parameterless `extern "C"`
        // test function under `#[ffi_guard]`; the call is `unsafe` solely because it is
        // declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard turns its `panic_any` payload into the documented `-1`
        // fallback.
        assert_eq!(unsafe { guarded_non_string_payload() }, -1);
    }

    #[test]
    fn ffi_guard_survives_a_payload_whose_destructor_panics() {
        // SAFETY: Test code: `guarded_payload_panics_on_drop` is a parameterless `extern
        // "C"` test function under `#[ffi_guard]`; the call is `unsafe` solely because it
        // is declared `unsafe extern "C"`, with no pointer arguments or preconditions to
        // uphold. The guard catches both the panic and the payload destructor's nested
        // panic and returns the documented `-1` fallback.
        assert_eq!(unsafe { guarded_payload_panics_on_drop() }, -1);
    }

    #[test]
    fn ffi_guard_passes_through_when_nothing_panics() {
        // SAFETY: Test code: every `guarded_*` call here takes only a `bool`, no argument,
        // or `&mut out_error`, a live local that outlives the call;
        // `guarded_const_pointer(false)` returns a pointer to the `c"value"` literal, which
        // is `'static`, so `CStr::from_ptr` reads a valid NUL-terminated string;
        // `guarded_mut_pointer(false)` returns a dangling pointer that is only compared
        // against null, never dereferenced; and `guarded_new(false, ..)` does not panic, so
        // the guard writes nothing through `out_error` (asserted null afterwards).
        unsafe {
            assert!(guarded_bool(false));
            assert_eq!(guarded_i16(false), 16);
            assert_eq!(guarded_i32(false), 32);
            assert_eq!(guarded_i64(false), 64);
            assert_eq!(guarded_topics_count(false), 3);
            assert_eq!(guarded_offsets_count(false), 3);
            assert_eq!(guarded_f64(false), 1.5);
            assert_eq!(guarded_error_code(false), kafka_common_ErrorCode_NONE);
            assert!(guarded_error_return(false).is_null());
            assert!(guarded_borrowed_error(false).is_null());
            assert!(!guarded_mut_pointer(false).is_null());
            assert_eq!(CStr::from_ptr(guarded_const_pointer(false)), c"value");
            // `return` inside the body still returns the function's value.
            assert_eq!(guarded_early_return(true), 5);
            assert_eq!(guarded_early_return(false), 6);

            let mut out_error: *mut kafka_common_Error_t = std::ptr::null_mut();
            assert!(!guarded_new(false, &mut out_error).is_null());
            assert!(out_error.is_null(), "a call that does not panic leaves out_error alone");
        }
    }

    /// A clone is an independent handle with the same variant, message and
    /// cause chain, freed separately from the original.
    #[test]
    fn error_clone_is_an_independent_equal_handle() {
        let original = box_error(Error::kafka_message_source("outer", Error::group_authorization("g")));
        // SAFETY: Test code: `original` is a live handle from `box_error` just above;
        // `kafka_common_Error_clone` returns a fresh owned handle, every accessor reads a
        // live handle, and each of `original` and `copy` is destroyed exactly once, with
        // `copy` read after `original` is freed to show it is independent.
        unsafe {
            let copy = kafka_common_Error_clone(original);
            assert_ne!(copy, original);
            assert_eq!(kafka_common_Error_code(copy), kafka_common_Error_code(original));
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(copy)).to_str(), Ok("outer"));
            assert_eq!(
                kafka_common_Error_is_kafka_error(copy),
                kafka_common_Error_is_kafka_error(original)
            );
            assert_eq!(kafka_common_Error_is_api_error(copy), kafka_common_Error_is_api_error(original));
            assert!(matches!(error_ref(copy).error.source(), Some(Error::GroupAuthorization(_))));
            kafka_common_Error_destroy(original);
            // The copy outlives the original.
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(copy)).to_str(), Ok("outer"));
            kafka_common_Error_destroy(copy);
        }
    }

    /// Walks a handle's cause chain through `kafka_common_Error_cause`,
    /// returning `(code, message)` per link, freeing every owned cause handle.
    unsafe fn cause_chain(error: *const kafka_common_Error_t) -> Vec<(kafka_common_ErrorCode_t, String)> {
        let mut chain = Vec::new();
        let mut current = unsafe { kafka_common_Error_cause(error) };
        while !current.is_null() {
            unsafe {
                chain.push((
                    kafka_common_Error_code(current),
                    CStr::from_ptr(kafka_common_Error_message(current))
                        .to_str()
                        .unwrap()
                        .to_string(),
                ));
                let next = kafka_common_Error_cause(current);
                kafka_common_Error_destroy(current);
                current = next;
            }
        }
        chain
    }

    /// An error built without a cause — Java's null `getCause()` — returns null.
    #[test]
    fn error_cause_is_null_without_a_cause() {
        let error = box_error(Error::kafka_message("no cause"));
        unsafe {
            assert!(kafka_common_Error_cause(error).is_null());
            kafka_common_Error_destroy(error);
        }
    }

    /// Each wrap the admin client builds with a Java cause — a bare
    /// `KafkaException(message, cause)` for client creation, the ListGroups
    /// broker lookup and the remove-all member error, and a
    /// `TimeoutException(message, cause)` for a call's deadline — hands the
    /// cause back through `kafka_common_Error_cause`, as an owned handle that
    /// outlives its parent.
    #[test]
    fn error_cause_returns_each_admin_wrap_cause() {
        use crate::common::errors::TimeoutError;
        let cases = [
            Error::kafka_message_source(
                "Failed to create new KafkaAdminClient",
                Error::local_illegal_argument("bad bootstrap"),
            ),
            Error::kafka_message_source(
                "Failed to find brokers to send ListGroups",
                Error::new(Errors::BrokerNotAvailable),
            ),
            Error::kafka_message_source(
                "Encounter error when trying to remove: removeAll()",
                Error::new(Errors::UnknownMemberId),
            ),
            Error::Timeout(TimeoutError::with_source(
                "Call(callName=listNodes) timed out at 5 after 1 attempt(s)",
                Error::new(Errors::NetworkError),
            )),
        ];
        for wrapped in cases {
            let expected_cause = wrapped.source().expect("each case has a cause").clone();
            let parent = box_error(wrapped);
            unsafe {
                let cause = kafka_common_Error_cause(parent);
                assert!(!cause.is_null());
                // Owned: still valid after the parent is destroyed.
                kafka_common_Error_destroy(parent);
                assert_eq!(kafka_common_Error_code(cause), error_code_of(&expected_cause));
                assert_eq!(
                    CStr::from_ptr(kafka_common_Error_message(cause)).to_str(),
                    Ok(expected_cause.message())
                );
                kafka_common_Error_destroy(cause);
            }
        }
    }

    /// A two-level chain is walked link by link, ending in null.
    #[test]
    fn error_cause_walks_a_nested_chain() {
        let error = box_error(Error::kafka_message_source(
            "outer",
            Error::kafka_message_source("middle", Error::group_authorization("g")),
        ));
        unsafe {
            let chain = cause_chain(error);
            assert_eq!(chain.len(), 2);
            assert_eq!(chain[0].1, "middle");
            assert_eq!(chain[1].1, "Not authorized to access group: g");
            kafka_common_Error_destroy(error);
        }
    }
}
