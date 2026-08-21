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

/// Returns the error code from a [`kafka_common_Error_t`] handle.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// The numeric error code (i32), or `0` if the error handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_code(error: *const kafka_common_Error_t) -> i32 {
    if error.is_null() {
        return 0;
    }
    i32::from(unsafe { error_ref(error) }.error.code())
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
