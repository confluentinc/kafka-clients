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

//! Shared C FFI machinery reused across the producer, consumer and admin FFI
//! layers.
//!
//! This module hosts the pieces that are not specific to any one client:
//!
//! - The opaque [`kafka_common_Error_t`] error handle (CLAUDE.md §4): its
//!   constructors, one per `Error` static factory
//!   (`kafka_common_Error_kafka_message`, `kafka_common_Error_timeout`, ...),
//!   its accessors (`code`, `message`, `source`, `throttle_time_ms`), the
//!   [`kafka_common_ErrorCode_e`] enum a C caller switches on, and the
//!   per-class payload views (`kafka_common_Error_resource_not_found` ->
//!   `kafka_common_ResourceNotFoundError_t`). The hierarchy predicates are
//!   generated into `error_predicates.rs`. `kafka_common_*` is shared verbatim
//!   between FFI surfaces — a second definition would make cbindgen emit a
//!   duplicate type.
//! - The dispatcher-thread abstraction ([`CompletionJob`],
//!   [`spawn_dispatcher`], [`enqueue_or_run_inline`]) and the void-returning
//!   operation callback machinery ([`OperationCallbackFn`],
//!   [`OperationCompletion`], [`OperationCallbackTarget`]) the clients still
//!   run their `_async` variants on. They are superseded by the per-client
//!   callbacks vector in [`callback_queue`](crate::ffi::callback_queue) and go
//!   away as each client moves to its `_cb` variants.
//! - The default logger initialization helper ([`init_default_logger`]).
//!
//! # Feature Gate
//!
//! This module is only compiled when the `ffi` feature is enabled.

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![expect(non_camel_case_types)]

pub(crate) mod acl;
pub(crate) mod classic_group_state;
pub(crate) mod cluster;
pub(crate) mod cluster_resource;
pub(crate) mod config;
pub(crate) mod election_type;
pub(crate) mod errors;
pub(crate) mod group_state;
pub(crate) mod group_type;
pub(crate) mod header;
pub(crate) mod isolation_level;
pub(crate) mod metric;
pub(crate) mod metric_name;
pub(crate) mod metric_name_template;
pub(crate) mod metrics;
pub(crate) mod node;
pub(crate) mod partition_info;
pub(crate) mod quota;
pub(crate) mod record;
pub(crate) mod resource;
pub(crate) mod security;
pub(crate) mod serialization;
pub(crate) mod topic_collection;
pub(crate) mod topic_id_partition;
pub(crate) mod topic_partition;
pub(crate) mod topic_partition_info;
pub(crate) mod topic_partition_replica;
pub(crate) mod uuid;

use std::any::Any;
use std::collections::HashSet;
use std::ffi::{CString, c_char};
use std::sync::OnceLock;

use crate::common::Error;
use crate::common::protocol::Errors;
use crate::ffi::util::{c_str_to_string, kafka_List_t, list_strings};
// The 162 enumerators are spelled out in full (see
// [`kafka_common_ErrorCode_e`]), so this glob keeps the match arms in
// `error_code_of` readable without repeating the type name on each one.
use kafka_common_ErrorCode_e::*;

/// Initializes the default stderr log backend, honouring `RUST_LOG`, the first
/// time a client is created from C. Idempotent: `try_init` succeeds once and
/// is a silent no-op afterwards, so a logger a binding installed beforehand
/// stays in place.
pub(crate) fn init_default_logger() {
    let _ = env_logger::try_init();
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
    /// The handle [`kafka_common_Error_source`] borrows out, built on first
    /// request from [`Error::source`] and owned by this handle so the chain
    /// of causes lives exactly as long as the outermost error.
    source: OnceLock<Option<Box<ErrorInner>>>,
    /// The payload view a `kafka_common_Error_<class>` accessor borrows out
    /// (an [`errors::Payload`] of the variant's class), built on first request
    /// and owned by this handle so it lives exactly as long as the error. An
    /// error has one class, so one slot suffices.
    payload: OnceLock<Box<dyn Any + Send + Sync>>,
}

impl ErrorInner {
    pub(crate) fn new(error: Error) -> Self {
        let message_cstring = CString::new(error.message()).unwrap_or_default();
        ErrorInner { error, message_cstring, source: OnceLock::new(), payload: OnceLock::new() }
    }

    /// The borrowed payload view of this error's class, built from `value`
    /// (the variant's payload, which the caller has already matched) on the
    /// first call and cached afterwards.
    pub(crate) fn payload_view<T: errors::PayloadClass>(&self, value: &T) -> *const errors::Payload<T> {
        let view = self.payload.get_or_init(|| Box::new(errors::Payload::new(value.clone())));
        view.downcast_ref::<errors::Payload<T>>()
            .map_or(std::ptr::null(), |payload| payload as *const _)
    }

    /// A borrowed handle on the error's cause, or null when it has none.
    fn source_ptr(&self) -> *const kafka_common_Error_t {
        match self
            .source
            .get_or_init(|| self.error.source().cloned().map(|e| Box::new(ErrorInner::new(e))))
        {
            Some(inner) => &**inner as *const ErrorInner as *const kafka_common_Error_t,
            None => std::ptr::null(),
        }
    }
}

/// Opaque error handle returned by functions that can fail.
///
/// Internally wraps a `Box<ErrorInner>` containing the [`Error`]
/// and a cached [`CString`] for the error message.
///
/// A null `kafka_common_Error_t` pointer means success (no error).
#[repr(C)]
// the handle over `common::Error` (CLAUDE.md §4)
#[doc(alias = "rust-only")]
pub struct kafka_common_Error_t {
    _private: [u8; 0],
}

/// Wraps a [`Error`] into a heap-allocated opaque error pointer, including
/// a cached [`CString`] for the error message.
pub(crate) fn box_error(error: Error) -> *mut kafka_common_Error_t {
    Box::into_raw(Box::new(ErrorInner::new(error))) as *mut kafka_common_Error_t
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
// Constructors: the `Error` static factories (CLAUDE.md §4, "Static methods")
// ---------------------------------------------------------------------------
//
// These let a C (or Python) callback that must *return* an error to the Rust
// core build the handle to return. The rebalance-listener callbacks are the
// motivating case — they return `Result<(), Error>` in the core, i.e. a
// `kafka_common_Error_t *` in C, and a Python listener that raised an
// exception has to convert it into one. They mirror `Error`'s own
// constructors one for one, so C builds exactly the classes Rust can: there
// is no constructor from a bare numeric code, because a code does not
// identify a class a C caller may legitimately raise (Java's `Errors.forCode`
// is a wire concern, and the client-side classes have no wire code at all).
//
// Every constructor returns an owned handle the caller frees with
// [`kafka_common_Error_destroy`] — unless it hands it to a Rust callback that
// documents taking ownership of it. A `source` parameter is an owned handle
// the constructor consumes; the caller must not destroy it afterwards.

/// The error behind an owned `source` handle, consumed by the constructor.
///
/// A null `source` violates the constructor's precondition; the bare
/// `KafkaException` stands in so the call stays memory-safe.
///
/// # Safety
///
/// `source` must be null or an owned error handle not yet destroyed.
unsafe fn owned_source(source: *mut kafka_common_Error_t) -> Error {
    unsafe { take_error(source) }.unwrap_or_else(Error::kafka)
}

/// A `kafka_List_t` of `const char *` topic names, read into a set.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
unsafe fn topic_set(topics: *const kafka_List_t) -> HashSet<String> {
    unsafe { list_strings(topics) }.into_iter().collect()
}

/// `new KafkaException()`: the bare Kafka error with the default message.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Error_kafka() -> *mut kafka_common_Error_t {
    box_error(Error::kafka())
}

/// `new KafkaException(String message)`: the bare Kafka error with `message`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_kafka_message(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::kafka_message(unsafe { c_str_to_string(message) }))
}

/// `new KafkaException(Throwable cause)`: the bare Kafka error caused by
/// `source`, which is consumed.
///
/// # Safety
///
/// `source` must be an owned error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_kafka_source(
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_Error_t {
    box_error(Error::kafka_source(unsafe { owned_source(source) }))
}

/// `new KafkaException(String message, Throwable cause)`: the bare Kafka
/// error with `message`, caused by `source`, which is consumed.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string; `source` must
/// be an owned error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_kafka_message_source(
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_Error_t {
    box_error(Error::kafka_message_source(unsafe { c_str_to_string(message) }, unsafe {
        owned_source(source)
    }))
}

/// `new TopicAuthorizationException(Set<String> unauthorizedTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_topic_authorization(
    topics: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    box_error(Error::topic_authorization(unsafe { topic_set(topics) }))
}

/// `new TopicAuthorizationException(String message, Set<String> unauthorizedTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings; `message`
/// must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_topic_authorization_message(
    topics: *const kafka_List_t,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::topic_authorization_message(unsafe { topic_set(topics) }, unsafe {
        c_str_to_string(message)
    }))
}

/// `new InvalidTopicException(Set<String> invalidTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_topics(topics: *const kafka_List_t) -> *mut kafka_common_Error_t {
    box_error(Error::invalid_topics(unsafe { topic_set(topics) }))
}

/// `new InvalidTopicException(String message, Set<String> invalidTopics)`.
///
/// `topics` is a borrowed `kafka_List_t` of `const char *`.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings; `message`
/// must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_topics_message(
    topics: *const kafka_List_t,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::invalid_topics_message(unsafe { topic_set(topics) }, unsafe {
        c_str_to_string(message)
    }))
}

/// `new GroupAuthorizationException(String groupId)`.
///
/// # Safety
///
/// `group_id` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_group_authorization(group_id: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::group_authorization(unsafe { c_str_to_string(group_id) }))
}

/// `new GroupAuthorizationException(String message, String groupId)`.
///
/// # Safety
///
/// `group_id` and `message` must each be null or a valid NUL-terminated C
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_group_authorization_with_message(
    group_id: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::group_authorization_with_message(
        unsafe { c_str_to_string(group_id) },
        unsafe { c_str_to_string(message) },
    ))
}

/// `new InvalidGroupIdException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_group_id(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::invalid_group_id(unsafe { c_str_to_string(message) }))
}

/// `new ThrottlingQuotaExceededException(int throttleTimeMs, String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttling_quota_exceeded(
    throttle_time_ms: i32,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::throttling_quota_exceeded(throttle_time_ms, unsafe {
        c_str_to_string(message)
    }))
}

/// `new BufferExhaustedException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_buffer_exhausted(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::buffer_exhausted(unsafe { c_str_to_string(message) }))
}

/// `new IllegalArgumentException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_illegal_argument(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::local_illegal_argument(unsafe { c_str_to_string(message) }))
}

/// `new ConfigException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_config_message(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::config_message(unsafe { c_str_to_string(message) }))
}

/// `new ConfigException(String name, Object value)`: the value is its text.
///
/// # Safety
///
/// `name` and `value` must each be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_config_name_value(
    name: *const c_char,
    value: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::config_name_value(unsafe { c_str_to_string(name) }, unsafe {
        c_str_to_string(value)
    }))
}

/// `new ConfigException(String name, Object value, String message)`: the
/// value is its text.
///
/// # Safety
///
/// `name`, `value` and `message` must each be null or a valid NUL-terminated
/// C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_config_name_value_message(
    name: *const c_char,
    value: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::config_name_value_message(
        unsafe { c_str_to_string(name) },
        unsafe { c_str_to_string(value) },
        unsafe { c_str_to_string(message) },
    ))
}

/// `new IllegalStateException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_illegal_state(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::local_illegal_state(unsafe { c_str_to_string(message) }))
}

/// `new TimeoutException(String message)` — the retriable
/// `org.apache.kafka.common.errors.TimeoutException`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_timeout(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::timeout(unsafe { c_str_to_string(message) }))
}

/// `new RecordTooLargeException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_too_large(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::record_too_large(unsafe { c_str_to_string(message) }))
}

/// `new CorrelationIdMismatchException(String message, int requestCorrelationId, int responseCorrelationId)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_correlation_id_mismatch(
    message: *const c_char,
    request_correlation_id: i32,
    response_correlation_id: i32,
) -> *mut kafka_common_Error_t {
    box_error(Error::correlation_id_mismatch(
        unsafe { c_str_to_string(message) },
        request_correlation_id,
        response_correlation_id,
    ))
}

/// `new InvalidReceiveException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_invalid_receive(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::invalid_receive(unsafe { c_str_to_string(message) }))
}

/// `new SchemaException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_schema(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::schema(unsafe { c_str_to_string(message) }))
}

/// `new SchemaException(String message, Throwable cause)`: `source` is
/// consumed.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string; `source` must
/// be an owned error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_schema_source(
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_Error_t {
    box_error(Error::schema_source(unsafe { c_str_to_string(message) }, unsafe {
        owned_source(source)
    }))
}

/// `new SerializationException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_serialization(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::serialization(unsafe { c_str_to_string(message) }))
}

/// `new UnsupportedVersionException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_unsupported_version(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::unsupported_version(unsafe { c_str_to_string(message) }))
}

/// `new WakeupException()` carrying `message`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_wakeup(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::wakeup(unsafe { c_str_to_string(message) }))
}

/// `new ConcurrentModificationException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_concurrent_modification(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::local_concurrent_modification(unsafe { c_str_to_string(message) }))
}

/// `new java.util.concurrent.TimeoutException(String message)` — the one
/// `Future.get(timeout, unit)` declares, not the retriable Kafka class.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_timeout(message: *const c_char) -> *mut kafka_common_Error_t {
    box_error(Error::local_timeout(unsafe { c_str_to_string(message) }))
}

/// `new TransactionAbortedException()`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Error_transaction_aborted() -> *mut kafka_common_Error_t {
    box_error(Error::transaction_aborted())
}

/// `new TransactionAbortedException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_transaction_aborted_message(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::transaction_aborted_message(unsafe { c_str_to_string(message) }))
}

/// `new RecordBatchTooLargeException(String message)`.
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_batch_too_large(
    message: *const c_char,
) -> *mut kafka_common_Error_t {
    box_error(Error::record_batch_too_large(unsafe { c_str_to_string(message) }))
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
/// `error` must be null or a handle created by [`box_error`] (i.e. by one of
/// the `kafka_common_Error_<constructor>` functions above or returned from a
/// fallible FFI function and not yet destroyed). After this call the pointer
/// is invalid.
pub(crate) unsafe fn take_error(error: *mut kafka_common_Error_t) -> Option<Error> {
    if error.is_null() {
        return None;
    }
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
// prefixing, so it would push the `_e` suffix into all 162 enumerators; the
// per-enum `prefix-with-name=false` annotation turns that off locally (leaving
// the global `[enum] prefix_with_name = true` in `cbindgen.toml` intact for
// every other enum) and the names are written out in full instead. As a bonus,
// one `grep kafka_common_ErrorCode_e_WAKEUP` then finds this definition and every
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
//
// Every enumerator repeats the enum's name because that is the C name CLAUDE.md
// §4 ("Enums") prescribes, `kafka_common_ErrorCode_e_<VALUE>`, spelled out in
// full here since cbindgen's prefixing is off for this enum (see above); clippy
// reads the repetition as a Rust naming smell.
#[expect(clippy::enum_variant_names)]
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_ErrorCode_e {
    // Java's `Errors`, at Java's own values. Names come from
    // `Errors::enum_name()`, so each equals Java's constant.
    kafka_common_ErrorCode_e_UNKNOWN_SERVER_ERROR = -1,
    kafka_common_ErrorCode_e_NONE = 0,
    kafka_common_ErrorCode_e_OFFSET_OUT_OF_RANGE = 1,
    kafka_common_ErrorCode_e_CORRUPT_MESSAGE = 2,
    kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION = 3,
    kafka_common_ErrorCode_e_INVALID_FETCH_SIZE = 4,
    kafka_common_ErrorCode_e_LEADER_NOT_AVAILABLE = 5,
    kafka_common_ErrorCode_e_NOT_LEADER_OR_FOLLOWER = 6,
    kafka_common_ErrorCode_e_REQUEST_TIMED_OUT = 7,
    kafka_common_ErrorCode_e_BROKER_NOT_AVAILABLE = 8,
    kafka_common_ErrorCode_e_REPLICA_NOT_AVAILABLE = 9,
    kafka_common_ErrorCode_e_MESSAGE_TOO_LARGE = 10,
    kafka_common_ErrorCode_e_STALE_CONTROLLER_EPOCH = 11,
    kafka_common_ErrorCode_e_OFFSET_METADATA_TOO_LARGE = 12,
    kafka_common_ErrorCode_e_NETWORK_ERROR = 13,
    kafka_common_ErrorCode_e_COORDINATOR_LOAD_IN_PROGRESS = 14,
    kafka_common_ErrorCode_e_COORDINATOR_NOT_AVAILABLE = 15,
    kafka_common_ErrorCode_e_NOT_COORDINATOR = 16,
    kafka_common_ErrorCode_e_INVALID_TOPIC_ERROR = 17,
    kafka_common_ErrorCode_e_RECORD_LIST_TOO_LARGE = 18,
    kafka_common_ErrorCode_e_NOT_ENOUGH_REPLICAS = 19,
    kafka_common_ErrorCode_e_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    kafka_common_ErrorCode_e_INVALID_REQUIRED_ACKS = 21,
    kafka_common_ErrorCode_e_ILLEGAL_GENERATION = 22,
    kafka_common_ErrorCode_e_INCONSISTENT_GROUP_PROTOCOL = 23,
    kafka_common_ErrorCode_e_INVALID_GROUP_ID = 24,
    kafka_common_ErrorCode_e_UNKNOWN_MEMBER_ID = 25,
    kafka_common_ErrorCode_e_INVALID_SESSION_TIMEOUT = 26,
    kafka_common_ErrorCode_e_REBALANCE_IN_PROGRESS = 27,
    kafka_common_ErrorCode_e_INVALID_COMMIT_OFFSET_SIZE = 28,
    kafka_common_ErrorCode_e_TOPIC_AUTHORIZATION_FAILED = 29,
    kafka_common_ErrorCode_e_GROUP_AUTHORIZATION_FAILED = 30,
    kafka_common_ErrorCode_e_CLUSTER_AUTHORIZATION_FAILED = 31,
    kafka_common_ErrorCode_e_INVALID_TIMESTAMP = 32,
    kafka_common_ErrorCode_e_UNSUPPORTED_SASL_MECHANISM = 33,
    kafka_common_ErrorCode_e_ILLEGAL_SASL_STATE = 34,
    kafka_common_ErrorCode_e_UNSUPPORTED_VERSION = 35,
    kafka_common_ErrorCode_e_TOPIC_ALREADY_EXISTS = 36,
    kafka_common_ErrorCode_e_INVALID_PARTITIONS = 37,
    kafka_common_ErrorCode_e_INVALID_REPLICATION_FACTOR = 38,
    kafka_common_ErrorCode_e_INVALID_REPLICA_ASSIGNMENT = 39,
    kafka_common_ErrorCode_e_INVALID_CONFIG = 40,
    kafka_common_ErrorCode_e_NOT_CONTROLLER = 41,
    kafka_common_ErrorCode_e_INVALID_REQUEST = 42,
    kafka_common_ErrorCode_e_UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
    kafka_common_ErrorCode_e_POLICY_VIOLATION = 44,
    kafka_common_ErrorCode_e_OUT_OF_ORDER_SEQUENCE_NUMBER = 45,
    kafka_common_ErrorCode_e_DUPLICATE_SEQUENCE_NUMBER = 46,
    kafka_common_ErrorCode_e_INVALID_PRODUCER_EPOCH = 47,
    kafka_common_ErrorCode_e_INVALID_TXN_STATE = 48,
    kafka_common_ErrorCode_e_INVALID_PRODUCER_ID_MAPPING = 49,
    kafka_common_ErrorCode_e_INVALID_TRANSACTION_TIMEOUT = 50,
    kafka_common_ErrorCode_e_CONCURRENT_TRANSACTIONS = 51,
    kafka_common_ErrorCode_e_TRANSACTION_COORDINATOR_FENCED = 52,
    kafka_common_ErrorCode_e_TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,
    kafka_common_ErrorCode_e_SECURITY_DISABLED = 54,
    kafka_common_ErrorCode_e_OPERATION_NOT_ATTEMPTED = 55,
    kafka_common_ErrorCode_e_KAFKA_STORAGE_ERROR = 56,
    kafka_common_ErrorCode_e_LOG_DIR_NOT_FOUND = 57,
    kafka_common_ErrorCode_e_SASL_AUTHENTICATION_FAILED = 58,
    kafka_common_ErrorCode_e_UNKNOWN_PRODUCER_ID = 59,
    kafka_common_ErrorCode_e_REASSIGNMENT_IN_PROGRESS = 60,
    kafka_common_ErrorCode_e_DELEGATION_TOKEN_AUTH_DISABLED = 61,
    kafka_common_ErrorCode_e_DELEGATION_TOKEN_NOT_FOUND = 62,
    kafka_common_ErrorCode_e_DELEGATION_TOKEN_OWNER_MISMATCH = 63,
    kafka_common_ErrorCode_e_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64,
    kafka_common_ErrorCode_e_DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65,
    kafka_common_ErrorCode_e_DELEGATION_TOKEN_EXPIRED = 66,
    kafka_common_ErrorCode_e_INVALID_PRINCIPAL_TYPE = 67,
    kafka_common_ErrorCode_e_NON_EMPTY_GROUP = 68,
    kafka_common_ErrorCode_e_GROUP_ID_NOT_FOUND = 69,
    kafka_common_ErrorCode_e_FETCH_SESSION_ID_NOT_FOUND = 70,
    kafka_common_ErrorCode_e_INVALID_FETCH_SESSION_EPOCH = 71,
    kafka_common_ErrorCode_e_LISTENER_NOT_FOUND = 72,
    kafka_common_ErrorCode_e_TOPIC_DELETION_DISABLED = 73,
    kafka_common_ErrorCode_e_FENCED_LEADER_EPOCH = 74,
    kafka_common_ErrorCode_e_UNKNOWN_LEADER_EPOCH = 75,
    kafka_common_ErrorCode_e_UNSUPPORTED_COMPRESSION_TYPE = 76,
    kafka_common_ErrorCode_e_STALE_BROKER_EPOCH = 77,
    kafka_common_ErrorCode_e_OFFSET_NOT_AVAILABLE = 78,
    kafka_common_ErrorCode_e_MEMBER_ID_REQUIRED = 79,
    kafka_common_ErrorCode_e_PREFERRED_LEADER_NOT_AVAILABLE = 80,
    kafka_common_ErrorCode_e_GROUP_MAX_SIZE_REACHED = 81,
    kafka_common_ErrorCode_e_FENCED_INSTANCE_ID = 82,
    kafka_common_ErrorCode_e_ELIGIBLE_LEADERS_NOT_AVAILABLE = 83,
    kafka_common_ErrorCode_e_ELECTION_NOT_NEEDED = 84,
    kafka_common_ErrorCode_e_NO_REASSIGNMENT_IN_PROGRESS = 85,
    kafka_common_ErrorCode_e_GROUP_SUBSCRIBED_TO_TOPIC = 86,
    kafka_common_ErrorCode_e_INVALID_RECORD = 87,
    kafka_common_ErrorCode_e_UNSTABLE_OFFSET_COMMIT = 88,
    kafka_common_ErrorCode_e_THROTTLING_QUOTA_EXCEEDED = 89,
    kafka_common_ErrorCode_e_PRODUCER_FENCED = 90,
    kafka_common_ErrorCode_e_RESOURCE_NOT_FOUND = 91,
    kafka_common_ErrorCode_e_DUPLICATE_RESOURCE = 92,
    kafka_common_ErrorCode_e_UNACCEPTABLE_CREDENTIAL = 93,
    kafka_common_ErrorCode_e_INCONSISTENT_VOTER_SET = 94,
    kafka_common_ErrorCode_e_INVALID_UPDATE_VERSION = 95,
    kafka_common_ErrorCode_e_FEATURE_UPDATE_FAILED = 96,
    kafka_common_ErrorCode_e_PRINCIPAL_DESERIALIZATION_FAILURE = 97,
    kafka_common_ErrorCode_e_SNAPSHOT_NOT_FOUND = 98,
    kafka_common_ErrorCode_e_POSITION_OUT_OF_RANGE = 99,
    kafka_common_ErrorCode_e_UNKNOWN_TOPIC_ID = 100,
    kafka_common_ErrorCode_e_DUPLICATE_BROKER_REGISTRATION = 101,
    kafka_common_ErrorCode_e_BROKER_ID_NOT_REGISTERED = 102,
    kafka_common_ErrorCode_e_INCONSISTENT_TOPIC_ID = 103,
    kafka_common_ErrorCode_e_INCONSISTENT_CLUSTER_ID = 104,
    kafka_common_ErrorCode_e_TRANSACTIONAL_ID_NOT_FOUND = 105,
    kafka_common_ErrorCode_e_FETCH_SESSION_TOPIC_ID_ERROR = 106,
    kafka_common_ErrorCode_e_INELIGIBLE_REPLICA = 107,
    kafka_common_ErrorCode_e_NEW_LEADER_ELECTED = 108,
    kafka_common_ErrorCode_e_OFFSET_MOVED_TO_TIERED_STORAGE = 109,
    kafka_common_ErrorCode_e_FENCED_MEMBER_EPOCH = 110,
    kafka_common_ErrorCode_e_UNRELEASED_INSTANCE_ID = 111,
    kafka_common_ErrorCode_e_UNSUPPORTED_ASSIGNOR = 112,
    kafka_common_ErrorCode_e_STALE_MEMBER_EPOCH = 113,
    kafka_common_ErrorCode_e_MISMATCHED_ENDPOINT_TYPE = 114,
    kafka_common_ErrorCode_e_UNSUPPORTED_ENDPOINT_TYPE = 115,
    kafka_common_ErrorCode_e_UNKNOWN_CONTROLLER_ID = 116,
    kafka_common_ErrorCode_e_UNKNOWN_SUBSCRIPTION_ID = 117,
    kafka_common_ErrorCode_e_TELEMETRY_TOO_LARGE = 118,
    kafka_common_ErrorCode_e_INVALID_REGISTRATION = 119,
    kafka_common_ErrorCode_e_TRANSACTION_ABORTABLE = 120,
    kafka_common_ErrorCode_e_INVALID_RECORD_STATE = 121,
    kafka_common_ErrorCode_e_SHARE_SESSION_NOT_FOUND = 122,
    kafka_common_ErrorCode_e_INVALID_SHARE_SESSION_EPOCH = 123,
    kafka_common_ErrorCode_e_FENCED_STATE_EPOCH = 124,
    kafka_common_ErrorCode_e_INVALID_VOTER_KEY = 125,
    kafka_common_ErrorCode_e_DUPLICATE_VOTER = 126,
    kafka_common_ErrorCode_e_VOTER_NOT_FOUND = 127,
    kafka_common_ErrorCode_e_INVALID_REGULAR_EXPRESSION = 128,
    kafka_common_ErrorCode_e_REBOOTSTRAP_REQUIRED = 129,
    kafka_common_ErrorCode_e_STREAMS_INVALID_TOPOLOGY = 130,
    kafka_common_ErrorCode_e_STREAMS_INVALID_TOPOLOGY_EPOCH = 131,
    kafka_common_ErrorCode_e_STREAMS_TOPOLOGY_FENCED = 132,
    kafka_common_ErrorCode_e_SHARE_SESSION_LIMIT_REACHED = 133,

    // Classes Java gives no code of their own: Rust-local negatives,
    // grouped by origin. These are ABI — new classes append at the
    // most-negative end, never in the middle.
    // JDK-derived (`Local*`).
    kafka_common_ErrorCode_e_LOCAL_CONCURRENT_MODIFICATION = -2,
    kafka_common_ErrorCode_e_LOCAL_ILLEGAL_ARGUMENT = -3,
    kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE = -4,
    kafka_common_ErrorCode_e_LOCAL_TIMEOUT = -5,
    // `common` client-side classes.
    kafka_common_ErrorCode_e_API = -6,
    kafka_common_ErrorCode_e_AUTHENTICATION = -7,
    kafka_common_ErrorCode_e_AUTHORIZER_NOT_READY = -8,
    kafka_common_ErrorCode_e_AUTHORIZATION = -9,
    kafka_common_ErrorCode_e_CONFIG = -10,
    kafka_common_ErrorCode_e_DISCONNECT = -11,
    kafka_common_ErrorCode_e_INTERRUPT = -12,
    kafka_common_ErrorCode_e_INVALID_OFFSET = -13,
    kafka_common_ErrorCode_e_SCHEMA = -14,
    kafka_common_ErrorCode_e_SERIALIZATION = -15,
    kafka_common_ErrorCode_e_SSL_AUTHENTICATION = -16,
    kafka_common_ErrorCode_e_TRANSACTION_ABORTED = -17,
    kafka_common_ErrorCode_e_WAKEUP = -18,
    // Hand-written / consumer classes.
    kafka_common_ErrorCode_e_CONSUMER_COMMIT_FAILED = -19,
    kafka_common_ErrorCode_e_CONSUMER_LOG_TRUNCATION = -20,
    kafka_common_ErrorCode_e_CONSUMER_NO_OFFSET_FOR_PARTITION = -21,
    kafka_common_ErrorCode_e_CONSUMER_OFFSET_OUT_OF_RANGE = -22,
    kafka_common_ErrorCode_e_CONSUMER_RETRIABLE_COMMIT_FAILED = -23,
    kafka_common_ErrorCode_e_CORRELATION_ID_MISMATCH = -24,
    kafka_common_ErrorCode_e_INVALID_RECEIVE = -25,
    kafka_common_ErrorCode_e_QUOTA_VIOLATION = -26,
    kafka_common_ErrorCode_e_RECORD_DESERIALIZATION = -27,
    // Code owned by a superclass (`BufferExhaustedException extends TimeoutException`).
    kafka_common_ErrorCode_e_PRODUCER_BUFFER_EXHAUSTED = -28,
}

/// The [`kafka_common_ErrorCode_e`] of the class that owns a protocol code.
///
/// This resolves the code through
/// [`Errors::error`] — the very
/// ownership relation [`kafka_common_ErrorCode_e`] is defined by — so the two
/// halves stay consistent by construction rather than by a second hand-written
/// table.
///
/// It exists for [`Error::KafkaError`], the one variant whose code is not a
/// constant: it stores an `Errors` rather than standing for a single class.
fn code_owned_by(error: Errors) -> kafka_common_ErrorCode_e {
    match error.error() {
        // `Errors::None` is the one code no class owns — Java declares it
        // `NONE(0, null, message -> null)`.
        None => kafka_common_ErrorCode_e_NONE,
        // No code is owned by the bare `KafkaException`, so this arm is
        // unreachable. Spelling it out rather than letting it fall into the
        // recursive arm below is what makes that recursion provably one level
        // deep; without it a future `Errors::error()` regression would hang the
        // ownership test instead of failing it. `UNKNOWN_SERVER_ERROR` is the
        // answer a bare `KafkaException` gives anyway (`Error::kafka` builds it
        // with `Errors::UnknownServerError`).
        Some(Error::KafkaError(_)) => kafka_common_ErrorCode_e_UNKNOWN_SERVER_ERROR,
        Some(owner) => error_code_of(&owner),
    }
}

/// The [`kafka_common_ErrorCode_e`] for an error, identifying its class.
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
/// four inheritance cases named on [`kafka_common_ErrorCode_e`]:
/// [`Error::ProducerBufferExhausted`] (Java: 7), [`Error::Authentication`],
/// [`Error::Authorization`] and [`Error::SslAuthentication`] (Java: 40).
///
/// The match is exhaustive with **no wildcard arm**, and must stay that way:
/// that is what makes adding an [`Error`] variant fail to compile until this
/// enum learns about it. A `_ =>` fallback is precisely how the two would
/// drift.
pub(crate) fn error_code_of(error: &Error) -> kafka_common_ErrorCode_e {
    match error {
        // The only non-constant arm: `Error::KafkaError` *stores* an `Errors`,
        // so it reports whatever code that value owns.
        Error::KafkaError(k) => code_owned_by(k.error()),
        Error::LocalIllegalArgument(_) => kafka_common_ErrorCode_e_LOCAL_ILLEGAL_ARGUMENT,
        Error::LocalIllegalState(_) => kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE,
        Error::LocalConcurrentModification(_) => kafka_common_ErrorCode_e_LOCAL_CONCURRENT_MODIFICATION,
        Error::LocalTimeout(_) => kafka_common_ErrorCode_e_LOCAL_TIMEOUT,
        Error::Api(_) => kafka_common_ErrorCode_e_API,
        Error::Authentication(_) => kafka_common_ErrorCode_e_AUTHENTICATION,
        Error::Authorization(_) => kafka_common_ErrorCode_e_AUTHORIZATION,
        Error::AuthorizerNotReady(_) => kafka_common_ErrorCode_e_AUTHORIZER_NOT_READY,
        Error::BrokerIdNotRegistered(_) => kafka_common_ErrorCode_e_BROKER_ID_NOT_REGISTERED,
        Error::BrokerNotAvailable(_) => kafka_common_ErrorCode_e_BROKER_NOT_AVAILABLE,
        Error::ProducerBufferExhausted(_) => kafka_common_ErrorCode_e_PRODUCER_BUFFER_EXHAUSTED,
        Error::ClusterAuthorization(_) => kafka_common_ErrorCode_e_CLUSTER_AUTHORIZATION_FAILED,
        Error::ConcurrentTransactions(_) => kafka_common_ErrorCode_e_CONCURRENT_TRANSACTIONS,
        Error::ControllerMoved(_) => kafka_common_ErrorCode_e_STALE_CONTROLLER_EPOCH,
        Error::CoordinatorLoadInProgress(_) => kafka_common_ErrorCode_e_COORDINATOR_LOAD_IN_PROGRESS,
        Error::CoordinatorNotAvailable(_) => kafka_common_ErrorCode_e_COORDINATOR_NOT_AVAILABLE,
        Error::CorrelationIdMismatch(_) => kafka_common_ErrorCode_e_CORRELATION_ID_MISMATCH,
        Error::CorruptRecord(_) => kafka_common_ErrorCode_e_CORRUPT_MESSAGE,
        Error::DelegationTokenAuthorization(_) => kafka_common_ErrorCode_e_DELEGATION_TOKEN_AUTHORIZATION_FAILED,
        Error::DelegationTokenDisabled(_) => kafka_common_ErrorCode_e_DELEGATION_TOKEN_AUTH_DISABLED,
        Error::DelegationTokenExpired(_) => kafka_common_ErrorCode_e_DELEGATION_TOKEN_EXPIRED,
        Error::DelegationTokenNotFound(_) => kafka_common_ErrorCode_e_DELEGATION_TOKEN_NOT_FOUND,
        Error::DelegationTokenOwnerMismatch(_) => kafka_common_ErrorCode_e_DELEGATION_TOKEN_OWNER_MISMATCH,
        Error::Disconnect(_) => kafka_common_ErrorCode_e_DISCONNECT,
        Error::DuplicateBrokerRegistration(_) => kafka_common_ErrorCode_e_DUPLICATE_BROKER_REGISTRATION,
        Error::DuplicateResource(_) => kafka_common_ErrorCode_e_DUPLICATE_RESOURCE,
        Error::DuplicateSequence(_) => kafka_common_ErrorCode_e_DUPLICATE_SEQUENCE_NUMBER,
        Error::DuplicateVoter(_) => kafka_common_ErrorCode_e_DUPLICATE_VOTER,
        Error::ElectionNotNeeded(_) => kafka_common_ErrorCode_e_ELECTION_NOT_NEEDED,
        Error::EligibleLeadersNotAvailable(_) => kafka_common_ErrorCode_e_ELIGIBLE_LEADERS_NOT_AVAILABLE,
        Error::FeatureUpdateFailed(_) => kafka_common_ErrorCode_e_FEATURE_UPDATE_FAILED,
        Error::FencedInstanceId(_) => kafka_common_ErrorCode_e_FENCED_INSTANCE_ID,
        Error::FencedLeaderEpoch(_) => kafka_common_ErrorCode_e_FENCED_LEADER_EPOCH,
        Error::FencedMemberEpoch(_) => kafka_common_ErrorCode_e_FENCED_MEMBER_EPOCH,
        Error::FencedStateEpoch(_) => kafka_common_ErrorCode_e_FENCED_STATE_EPOCH,
        Error::FetchSessionIdNotFound(_) => kafka_common_ErrorCode_e_FETCH_SESSION_ID_NOT_FOUND,
        Error::FetchSessionTopicId(_) => kafka_common_ErrorCode_e_FETCH_SESSION_TOPIC_ID_ERROR,
        Error::GroupAuthorization(_) => kafka_common_ErrorCode_e_GROUP_AUTHORIZATION_FAILED,
        Error::GroupIdNotFound(_) => kafka_common_ErrorCode_e_GROUP_ID_NOT_FOUND,
        Error::GroupMaxSizeReached(_) => kafka_common_ErrorCode_e_GROUP_MAX_SIZE_REACHED,
        Error::GroupNotEmpty(_) => kafka_common_ErrorCode_e_NON_EMPTY_GROUP,
        Error::GroupSubscribedToTopic(_) => kafka_common_ErrorCode_e_GROUP_SUBSCRIBED_TO_TOPIC,
        Error::IllegalGeneration(_) => kafka_common_ErrorCode_e_ILLEGAL_GENERATION,
        Error::IllegalSaslState(_) => kafka_common_ErrorCode_e_ILLEGAL_SASL_STATE,
        Error::InconsistentClusterId(_) => kafka_common_ErrorCode_e_INCONSISTENT_CLUSTER_ID,
        Error::InconsistentGroupProtocol(_) => kafka_common_ErrorCode_e_INCONSISTENT_GROUP_PROTOCOL,
        Error::InconsistentTopicId(_) => kafka_common_ErrorCode_e_INCONSISTENT_TOPIC_ID,
        Error::InconsistentVoterSet(_) => kafka_common_ErrorCode_e_INCONSISTENT_VOTER_SET,
        Error::IneligibleReplica(_) => kafka_common_ErrorCode_e_INELIGIBLE_REPLICA,
        Error::Interrupt(_) => kafka_common_ErrorCode_e_INTERRUPT,
        Error::InvalidCommitOffsetSize(_) => kafka_common_ErrorCode_e_INVALID_COMMIT_OFFSET_SIZE,
        Error::InvalidConfiguration(_) => kafka_common_ErrorCode_e_INVALID_CONFIG,
        Error::InvalidFetchSessionEpoch(_) => kafka_common_ErrorCode_e_INVALID_FETCH_SESSION_EPOCH,
        Error::InvalidFetchSize(_) => kafka_common_ErrorCode_e_INVALID_FETCH_SIZE,
        Error::InvalidGroupId(_) => kafka_common_ErrorCode_e_INVALID_GROUP_ID,
        Error::InvalidOffset(_) => kafka_common_ErrorCode_e_INVALID_OFFSET,
        Error::InvalidPartitions(_) => kafka_common_ErrorCode_e_INVALID_PARTITIONS,
        Error::InvalidPidMapping(_) => kafka_common_ErrorCode_e_INVALID_PRODUCER_ID_MAPPING,
        Error::InvalidPrincipalType(_) => kafka_common_ErrorCode_e_INVALID_PRINCIPAL_TYPE,
        Error::InvalidProducerEpoch(_) => kafka_common_ErrorCode_e_INVALID_PRODUCER_EPOCH,
        Error::InvalidRecord(_) => kafka_common_ErrorCode_e_INVALID_RECORD,
        Error::InvalidRecordState(_) => kafka_common_ErrorCode_e_INVALID_RECORD_STATE,
        Error::InvalidRegistration(_) => kafka_common_ErrorCode_e_INVALID_REGISTRATION,
        Error::InvalidRegularExpression(_) => kafka_common_ErrorCode_e_INVALID_REGULAR_EXPRESSION,
        Error::InvalidReplicaAssignment(_) => kafka_common_ErrorCode_e_INVALID_REPLICA_ASSIGNMENT,
        Error::InvalidReplicationFactor(_) => kafka_common_ErrorCode_e_INVALID_REPLICATION_FACTOR,
        Error::InvalidRequest(_) => kafka_common_ErrorCode_e_INVALID_REQUEST,
        Error::InvalidRequiredAcks(_) => kafka_common_ErrorCode_e_INVALID_REQUIRED_ACKS,
        Error::InvalidSessionTimeout(_) => kafka_common_ErrorCode_e_INVALID_SESSION_TIMEOUT,
        Error::InvalidShareSessionEpoch(_) => kafka_common_ErrorCode_e_INVALID_SHARE_SESSION_EPOCH,
        Error::InvalidTimestamp(_) => kafka_common_ErrorCode_e_INVALID_TIMESTAMP,
        Error::InvalidTopic(_) => kafka_common_ErrorCode_e_INVALID_TOPIC_ERROR,
        Error::InvalidTxnState(_) => kafka_common_ErrorCode_e_INVALID_TXN_STATE,
        Error::InvalidTxnTimeout(_) => kafka_common_ErrorCode_e_INVALID_TRANSACTION_TIMEOUT,
        Error::InvalidUpdateVersion(_) => kafka_common_ErrorCode_e_INVALID_UPDATE_VERSION,
        Error::InvalidVoterKey(_) => kafka_common_ErrorCode_e_INVALID_VOTER_KEY,
        Error::KafkaStorage(_) => kafka_common_ErrorCode_e_KAFKA_STORAGE_ERROR,
        Error::LeaderNotAvailable(_) => kafka_common_ErrorCode_e_LEADER_NOT_AVAILABLE,
        Error::ListenerNotFound(_) => kafka_common_ErrorCode_e_LISTENER_NOT_FOUND,
        Error::LogDirNotFound(_) => kafka_common_ErrorCode_e_LOG_DIR_NOT_FOUND,
        Error::MemberIdRequired(_) => kafka_common_ErrorCode_e_MEMBER_ID_REQUIRED,
        Error::MismatchedEndpointType(_) => kafka_common_ErrorCode_e_MISMATCHED_ENDPOINT_TYPE,
        Error::Network(_) => kafka_common_ErrorCode_e_NETWORK_ERROR,
        Error::NewLeaderElected(_) => kafka_common_ErrorCode_e_NEW_LEADER_ELECTED,
        Error::NoReassignmentInProgress(_) => kafka_common_ErrorCode_e_NO_REASSIGNMENT_IN_PROGRESS,
        Error::NotController(_) => kafka_common_ErrorCode_e_NOT_CONTROLLER,
        Error::NotCoordinator(_) => kafka_common_ErrorCode_e_NOT_COORDINATOR,
        Error::NotEnoughReplicas(_) => kafka_common_ErrorCode_e_NOT_ENOUGH_REPLICAS,
        Error::NotEnoughReplicasAfterAppend(_) => kafka_common_ErrorCode_e_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        Error::NotLeaderOrFollower(_) => kafka_common_ErrorCode_e_NOT_LEADER_OR_FOLLOWER,
        Error::OffsetMetadataTooLarge(_) => kafka_common_ErrorCode_e_OFFSET_METADATA_TOO_LARGE,
        Error::OffsetMovedToTieredStorage(_) => kafka_common_ErrorCode_e_OFFSET_MOVED_TO_TIERED_STORAGE,
        Error::OffsetNotAvailable(_) => kafka_common_ErrorCode_e_OFFSET_NOT_AVAILABLE,
        Error::OffsetOutOfRange(_) => kafka_common_ErrorCode_e_OFFSET_OUT_OF_RANGE,
        Error::OperationNotAttempted(_) => kafka_common_ErrorCode_e_OPERATION_NOT_ATTEMPTED,
        Error::OutOfOrderSequence(_) => kafka_common_ErrorCode_e_OUT_OF_ORDER_SEQUENCE_NUMBER,
        Error::PolicyViolation(_) => kafka_common_ErrorCode_e_POLICY_VIOLATION,
        Error::PositionOutOfRange(_) => kafka_common_ErrorCode_e_POSITION_OUT_OF_RANGE,
        Error::PreferredLeaderNotAvailable(_) => kafka_common_ErrorCode_e_PREFERRED_LEADER_NOT_AVAILABLE,
        Error::PrincipalDeserialization(_) => kafka_common_ErrorCode_e_PRINCIPAL_DESERIALIZATION_FAILURE,
        Error::ProducerFenced(_) => kafka_common_ErrorCode_e_PRODUCER_FENCED,
        Error::QuotaViolation(_) => kafka_common_ErrorCode_e_QUOTA_VIOLATION,
        Error::ReassignmentInProgress(_) => kafka_common_ErrorCode_e_REASSIGNMENT_IN_PROGRESS,
        Error::RebalanceInProgress(_) => kafka_common_ErrorCode_e_REBALANCE_IN_PROGRESS,
        Error::RebootstrapRequired(_) => kafka_common_ErrorCode_e_REBOOTSTRAP_REQUIRED,
        Error::RecordBatchTooLarge(_) => kafka_common_ErrorCode_e_RECORD_LIST_TOO_LARGE,
        Error::RecordDeserialization(_) => kafka_common_ErrorCode_e_RECORD_DESERIALIZATION,
        Error::RecordTooLarge(_) => kafka_common_ErrorCode_e_MESSAGE_TOO_LARGE,
        Error::InvalidReceive(_) => kafka_common_ErrorCode_e_INVALID_RECEIVE,
        Error::Config(_) => kafka_common_ErrorCode_e_CONFIG,
        Error::ConsumerRetriableCommitFailed(_) => kafka_common_ErrorCode_e_CONSUMER_RETRIABLE_COMMIT_FAILED,
        Error::ConsumerCommitFailed(_) => kafka_common_ErrorCode_e_CONSUMER_COMMIT_FAILED,
        Error::ConsumerNoOffsetForPartition(_) => kafka_common_ErrorCode_e_CONSUMER_NO_OFFSET_FOR_PARTITION,
        Error::ConsumerOffsetOutOfRange(_) => kafka_common_ErrorCode_e_CONSUMER_OFFSET_OUT_OF_RANGE,
        Error::ConsumerLogTruncation(_) => kafka_common_ErrorCode_e_CONSUMER_LOG_TRUNCATION,
        Error::ReplicaNotAvailable(_) => kafka_common_ErrorCode_e_REPLICA_NOT_AVAILABLE,
        Error::ResourceNotFound(_) => kafka_common_ErrorCode_e_RESOURCE_NOT_FOUND,
        Error::SaslAuthentication(_) => kafka_common_ErrorCode_e_SASL_AUTHENTICATION_FAILED,
        Error::Schema(_) => kafka_common_ErrorCode_e_SCHEMA,
        Error::SecurityDisabled(_) => kafka_common_ErrorCode_e_SECURITY_DISABLED,
        Error::Serialization(_) => kafka_common_ErrorCode_e_SERIALIZATION,
        Error::ShareSessionLimitReached(_) => kafka_common_ErrorCode_e_SHARE_SESSION_LIMIT_REACHED,
        Error::ShareSessionNotFound(_) => kafka_common_ErrorCode_e_SHARE_SESSION_NOT_FOUND,
        Error::SnapshotNotFound(_) => kafka_common_ErrorCode_e_SNAPSHOT_NOT_FOUND,
        Error::SslAuthentication(_) => kafka_common_ErrorCode_e_SSL_AUTHENTICATION,
        Error::StaleBrokerEpoch(_) => kafka_common_ErrorCode_e_STALE_BROKER_EPOCH,
        Error::StaleMemberEpoch(_) => kafka_common_ErrorCode_e_STALE_MEMBER_EPOCH,
        Error::StreamsInvalidTopology(_) => kafka_common_ErrorCode_e_STREAMS_INVALID_TOPOLOGY,
        Error::StreamsInvalidTopologyEpoch(_) => kafka_common_ErrorCode_e_STREAMS_INVALID_TOPOLOGY_EPOCH,
        Error::StreamsTopologyFenced(_) => kafka_common_ErrorCode_e_STREAMS_TOPOLOGY_FENCED,
        Error::TelemetryTooLarge(_) => kafka_common_ErrorCode_e_TELEMETRY_TOO_LARGE,
        Error::ThrottlingQuotaExceeded(_) => kafka_common_ErrorCode_e_THROTTLING_QUOTA_EXCEEDED,
        Error::Timeout(_) => kafka_common_ErrorCode_e_REQUEST_TIMED_OUT,
        Error::TopicAuthorization(_) => kafka_common_ErrorCode_e_TOPIC_AUTHORIZATION_FAILED,
        Error::TopicDeletionDisabled(_) => kafka_common_ErrorCode_e_TOPIC_DELETION_DISABLED,
        Error::TopicExists(_) => kafka_common_ErrorCode_e_TOPIC_ALREADY_EXISTS,
        Error::TransactionAbortable(_) => kafka_common_ErrorCode_e_TRANSACTION_ABORTABLE,
        Error::TransactionAborted(_) => kafka_common_ErrorCode_e_TRANSACTION_ABORTED,
        Error::TransactionCoordinatorFenced(_) => kafka_common_ErrorCode_e_TRANSACTION_COORDINATOR_FENCED,
        Error::TransactionalIdAuthorization(_) => kafka_common_ErrorCode_e_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        Error::TransactionalIdNotFound(_) => kafka_common_ErrorCode_e_TRANSACTIONAL_ID_NOT_FOUND,
        Error::UnacceptableCredential(_) => kafka_common_ErrorCode_e_UNACCEPTABLE_CREDENTIAL,
        Error::UnknownControllerId(_) => kafka_common_ErrorCode_e_UNKNOWN_CONTROLLER_ID,
        Error::UnknownLeaderEpoch(_) => kafka_common_ErrorCode_e_UNKNOWN_LEADER_EPOCH,
        Error::UnknownMemberId(_) => kafka_common_ErrorCode_e_UNKNOWN_MEMBER_ID,
        Error::UnknownProducerId(_) => kafka_common_ErrorCode_e_UNKNOWN_PRODUCER_ID,
        Error::UnknownServer(_) => kafka_common_ErrorCode_e_UNKNOWN_SERVER_ERROR,
        Error::UnknownSubscriptionId(_) => kafka_common_ErrorCode_e_UNKNOWN_SUBSCRIPTION_ID,
        Error::UnknownTopicId(_) => kafka_common_ErrorCode_e_UNKNOWN_TOPIC_ID,
        Error::UnknownTopicOrPartition(_) => kafka_common_ErrorCode_e_UNKNOWN_TOPIC_OR_PARTITION,
        Error::UnreleasedInstanceId(_) => kafka_common_ErrorCode_e_UNRELEASED_INSTANCE_ID,
        Error::UnstableOffsetCommit(_) => kafka_common_ErrorCode_e_UNSTABLE_OFFSET_COMMIT,
        Error::UnsupportedAssignor(_) => kafka_common_ErrorCode_e_UNSUPPORTED_ASSIGNOR,
        Error::UnsupportedByAuthentication(_) => kafka_common_ErrorCode_e_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        Error::UnsupportedCompressionType(_) => kafka_common_ErrorCode_e_UNSUPPORTED_COMPRESSION_TYPE,
        Error::UnsupportedEndpointType(_) => kafka_common_ErrorCode_e_UNSUPPORTED_ENDPOINT_TYPE,
        Error::UnsupportedForMessageFormat(_) => kafka_common_ErrorCode_e_UNSUPPORTED_FOR_MESSAGE_FORMAT,
        Error::UnsupportedSaslMechanism(_) => kafka_common_ErrorCode_e_UNSUPPORTED_SASL_MECHANISM,
        Error::UnsupportedVersion(_) => kafka_common_ErrorCode_e_UNSUPPORTED_VERSION,
        Error::VoterNotFound(_) => kafka_common_ErrorCode_e_VOTER_NOT_FOUND,
        Error::Wakeup(_) => kafka_common_ErrorCode_e_WAKEUP,
    }
}

/// Returns the error code from a [`kafka_common_Error_t`] handle.
///
/// The value identifies the error's **class**, not merely its protocol code:
/// the codes are pairwise distinct, so a `switch` on this value is enough to
/// tell any two errors apart. See [`kafka_common_ErrorCode_e`] for the
/// assignment rule and for the four classes whose value deliberately differs
/// from the code Java's `Errors.forException` would report.
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// The error's [`kafka_common_ErrorCode_e`], or
/// `kafka_common_ErrorCode_e_NONE` (`0`) if the error handle is null. The
/// enumerators have explicit values that fit an `int`, so a C caller that was
/// comparing the previous `int32_t` return against integers keeps compiling
/// and keeps getting the same answers for every broker-reported code.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_code(error: *const kafka_common_Error_t) -> kafka_common_ErrorCode_e {
    if error.is_null() {
        return kafka_common_ErrorCode_e_NONE;
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

/// Returns the error's cause (`Throwable.getCause()`), or null when it has
/// none.
///
/// The returned handle is *borrowed*: it is owned by `error` and valid until
/// [`kafka_common_Error_destroy`] is called on `error`. It must not be passed
/// to `kafka_common_Error_destroy` itself. Calling this on the returned handle
/// walks one more step down the chain of causes.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_source(error: *const kafka_common_Error_t) -> *const kafka_common_Error_t {
    if error.is_null() {
        return std::ptr::null();
    }
    unsafe { error_ref(error) }.source_ptr()
}

/// Returns the throttle time the broker reported with this error
/// (`ThrottlingQuotaExceededException.throttleTimeMs()`), or `-1` when the
/// error carries none.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttle_time_ms(error: *const kafka_common_Error_t) -> i32 {
    if error.is_null() {
        return -1;
    }
    unsafe { error_ref(error) }.error.throttle_time_ms().unwrap_or(-1)
}

// NOTE: fatality (`RequestUtils.isFatalException`) is deliberately NOT exported
// here. `org.apache.kafka.common.requests` carries the package disclaimer "This
// package is not a supported Kafka API; the implementation may change without
// warning between minor or patch releases", and CLAUDE.md §4 forbids C bindings
// for such packages. A C caller that needs the classification composes it from
// the exported predicates (`kafka_common_Error_is_authentication_error`,
// `kafka_common_Error_is_authorization_error`) and the error code.

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::KafkaError;
    use crate::common::error::ErrorName;
    use crate::common::errors::{
        ApiError, AuthenticationError, AuthorizationError, AuthorizerNotReadyError, DisconnectError, InterruptError,
        InvalidOffsetError, SslAuthenticationError,
    };
    use crate::common::metrics::QuotaViolationError;
    use crate::common::network::InvalidReceiveError;
    use crate::common::{MetricName, TopicPartition};
    use crate::consumer::{
        ConsumerCommitFailedError, ConsumerLogTruncationError, ConsumerNoOffsetForPartitionError,
        ConsumerOffsetOutOfRangeError, ConsumerRetriableCommitFailedError,
    };
    use std::collections::{BTreeMap, HashMap, HashSet};

    /// Java's lowest and highest `Errors` codes — the full range `Errors::for_code`
    /// resolves to a named constant.
    const FIRST_CODE: i16 = -1;
    const LAST_CODE: i16 = 133;

    /// The 27 classes that own no Java code, each paired with the enumerator it
    /// must map to and that enumerator's literal value.
    ///
    /// Both halves matter. The instance checks that `error_code_of`'s arm points
    /// at the right constant; the literal checks that the constant still has the
    /// value it was published with. See [`kafka_common_ErrorCode_e`] — the
    /// negatives are ABI, so a renumbering must fail here rather than silently
    /// reach a C caller.
    fn client_side_classes() -> Vec<(Error, kafka_common_ErrorCode_e, i32)> {
        vec![
            // -2 ..= -5: JDK-derived (`Local*`).
            (
                Error::local_concurrent_modification("m"),
                kafka_common_ErrorCode_e_LOCAL_CONCURRENT_MODIFICATION,
                -2,
            ),
            (
                Error::local_illegal_argument("m"),
                kafka_common_ErrorCode_e_LOCAL_ILLEGAL_ARGUMENT,
                -3,
            ),
            (
                Error::local_illegal_state("m"),
                kafka_common_ErrorCode_e_LOCAL_ILLEGAL_STATE,
                -4,
            ),
            (Error::local_timeout("m"), kafka_common_ErrorCode_e_LOCAL_TIMEOUT, -5),
            // -6 ..= -18: `common` client-side classes.
            (Error::Api(ApiError::new("m")), kafka_common_ErrorCode_e_API, -6),
            (
                Error::Authentication(AuthenticationError::new("m")),
                kafka_common_ErrorCode_e_AUTHENTICATION,
                -7,
            ),
            (
                Error::AuthorizerNotReady(AuthorizerNotReadyError::new("m")),
                kafka_common_ErrorCode_e_AUTHORIZER_NOT_READY,
                -8,
            ),
            (
                Error::Authorization(AuthorizationError::new("m")),
                kafka_common_ErrorCode_e_AUTHORIZATION,
                -9,
            ),
            (Error::config_message("m"), kafka_common_ErrorCode_e_CONFIG, -10),
            (
                Error::Disconnect(DisconnectError::new("m")),
                kafka_common_ErrorCode_e_DISCONNECT,
                -11,
            ),
            (
                Error::Interrupt(InterruptError::new("m")),
                kafka_common_ErrorCode_e_INTERRUPT,
                -12,
            ),
            (
                Error::InvalidOffset(InvalidOffsetError::new("m")),
                kafka_common_ErrorCode_e_INVALID_OFFSET,
                -13,
            ),
            (Error::schema("m"), kafka_common_ErrorCode_e_SCHEMA, -14),
            (Error::serialization("m"), kafka_common_ErrorCode_e_SERIALIZATION, -15),
            (
                Error::SslAuthentication(SslAuthenticationError::new("m")),
                kafka_common_ErrorCode_e_SSL_AUTHENTICATION,
                -16,
            ),
            (Error::transaction_aborted(), kafka_common_ErrorCode_e_TRANSACTION_ABORTED, -17),
            (Error::wakeup("m"), kafka_common_ErrorCode_e_WAKEUP, -18),
            // -19 ..= -27: hand-written / consumer classes.
            (
                Error::ConsumerCommitFailed(ConsumerCommitFailedError::with_default_message()),
                kafka_common_ErrorCode_e_CONSUMER_COMMIT_FAILED,
                -19,
            ),
            (
                Error::ConsumerLogTruncation(Box::new(ConsumerLogTruncationError::new(HashMap::new(), HashMap::new()))),
                kafka_common_ErrorCode_e_CONSUMER_LOG_TRUNCATION,
                -20,
            ),
            (
                Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(TopicPartition::new(
                    "t", 0,
                ))),
                kafka_common_ErrorCode_e_CONSUMER_NO_OFFSET_FOR_PARTITION,
                -21,
            ),
            (
                Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::new(HashMap::new())),
                kafka_common_ErrorCode_e_CONSUMER_OFFSET_OUT_OF_RANGE,
                -22,
            ),
            (
                Error::ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError::with_default_message()),
                kafka_common_ErrorCode_e_CONSUMER_RETRIABLE_COMMIT_FAILED,
                -23,
            ),
            (
                Error::correlation_id_mismatch("m", 1, 2),
                kafka_common_ErrorCode_e_CORRELATION_ID_MISMATCH,
                -24,
            ),
            (
                Error::InvalidReceive(InvalidReceiveError::new("m")),
                kafka_common_ErrorCode_e_INVALID_RECEIVE,
                -25,
            ),
            (
                Error::QuotaViolation(Box::new(QuotaViolationError::new(
                    MetricName::new("n", "g", "d", BTreeMap::new()),
                    1.0,
                    0.5,
                ))),
                kafka_common_ErrorCode_e_QUOTA_VIOLATION,
                -26,
            ),
            (
                Error::RecordDeserialization(Box::new(record_deserialization_error())),
                kafka_common_ErrorCode_e_RECORD_DESERIALIZATION,
                -27,
            ),
            // -28: code owned by a superclass.
            (
                Error::buffer_exhausted("m"),
                kafka_common_ErrorCode_e_PRODUCER_BUFFER_EXHAUSTED,
                -28,
            ),
        ]
    }

    /// Every code whose `Errors::error()` names a class reports that class's own
    /// value, and the value equals `Errors::code()`.
    ///
    /// This machine-checks the faithful half of [`kafka_common_ErrorCode_e`]
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
                    assert_eq!(kafka_common_ErrorCode_e_NONE as i32, 0);
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
            kafka_common_ErrorCode_e_CORRUPT_MESSAGE
        );
        assert_eq!(
            error_code_of(&Error::KafkaError(KafkaError::new(Errors::None))),
            kafka_common_ErrorCode_e_NONE
        );
        // A bare `KafkaException` reports -1, which is what Java's
        // `Errors.forException` answers for it.
        assert_eq!(
            error_code_of(&Error::kafka_message("m")),
            kafka_common_ErrorCode_e_UNKNOWN_SERVER_ERROR
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
        seen.insert(kafka_common_ErrorCode_e_NONE as i32, "NONE".to_string());

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

    /// One instance of each of the 162 error classes: the owner of every Java code,
    /// the client-side classes, and the bare `KafkaException`, which reports
    /// `UNKNOWN_SERVER_ERROR` rather than a code of its own. The codes are pairwise
    /// distinct ([`ffi_error_code_values_are_injective`]), so no class appears twice.
    fn one_error_per_class() -> Vec<Error> {
        let coded = (FIRST_CODE..=LAST_CODE).filter_map(|code| Errors::for_code(code).error());
        let local = client_side_classes().into_iter().map(|(error, _, _)| error);
        coded.chain(local).chain([Error::kafka_message("m")]).collect()
    }

    /// `is_` + `payload` snake_cased with a trailing `Error` dropped + `_error`, the
    /// class-predicate name `cargo xtask lint-custom` (check-error-predicate)
    /// enforces.
    fn class_predicate(payload: &str) -> String {
        let base = payload.strip_suffix("Error").filter(|b| !b.is_empty()).unwrap_or(payload);
        let mut snake = String::new();
        for (i, c) in base.char_indices() {
            if c.is_ascii_uppercase() && i > 0 {
                snake.push('_');
            }
            snake.push(c.to_ascii_lowercase());
        }
        format!("is_{snake}_error")
    }

    /// The predicates for Java's intermediate (non-leaf) classes. Their exact
    /// coverage is asserted in both directions by the hierarchy tests in
    /// `common::error` (CLAUDE.md §12.4), so here they are only checked to answer
    /// `true` for their own class.
    const INTERMEDIATE_PREDICATES: [&str; 15] = [
        "is_kafka_error",
        "is_api_error",
        "is_retriable_error",
        "is_refresh_retriable_error",
        "is_invalid_metadata_error",
        "is_authentication_error",
        "is_authorization_error",
        "is_invalid_configuration_error",
        "is_application_recoverable_error",
        "is_invalid_offset_error",
        "is_out_of_order_sequence_error",
        "is_serialization_error",
        "is_timeout_error",
        "is_consumer_invalid_offset_error",
        "is_consumer_offset_out_of_range_error",
    ];

    /// The leaf classes that nonetheless have a Java subclass, as
    /// `(class predicate, subclass's own predicate)`. Every other leaf predicate is
    /// `true` for its own class only.
    const LEAF_SUBCLASSES: [(&str, &str); 1] = [
        // `CorrelationIdMismatchException extends IllegalStateException`
        // (`CorrelationIdMismatchException.java:23`).
        ("is_local_illegal_state_error", "is_correlation_id_mismatch_error"),
    ];

    /// Every error class has its own C predicate, `kafka_common_Error_<is_x_error>`,
    /// and each export answers like Java's `instanceof <class>`: `true` for the class
    /// itself and its subclasses, `false` for every other class (CLAUDE.md §3).
    ///
    /// Exercises the generated exports themselves, through the handle a C caller
    /// holds, against one instance of every class.
    #[test]
    fn ffi_every_class_has_its_own_predicate() {
        let predicates: HashMap<&str, _> = super::super::error_predicates::PREDICATES.iter().copied().collect();
        let errors: Vec<(String, *mut kafka_common_Error_t)> = one_error_per_class()
            .into_iter()
            .map(|error| (class_predicate(ErrorName::name(&error)), box_error(error)))
            .collect();
        assert_eq!(errors.len(), 162, "expected one error per class");

        let owners: HashSet<&str> = errors.iter().map(|(own, _)| own.as_str()).collect();
        assert_eq!(owners.len(), errors.len(), "two classes share a predicate name");

        for (own, handle) in &errors {
            let export = predicates
                .get(own.as_str())
                .unwrap_or_else(|| panic!("no C export for `{own}`"));
            assert_eq!(unsafe { export(*handle) }, 1, "`{own}` is false for its own class");
        }

        // Every export is either an intermediate predicate or some class's own.
        for name in predicates.keys() {
            assert!(
                owners.contains(name) || INTERMEDIATE_PREDICATES.contains(name),
                "`{name}` belongs to no error class"
            );
        }

        for (name, export) in &predicates {
            if INTERMEDIATE_PREDICATES.contains(name) {
                continue;
            }
            for (own, handle) in &errors {
                let expected = own == name || LEAF_SUBCLASSES.contains(&(*name, own.as_str()));
                assert_eq!(
                    unsafe { export(*handle) },
                    i8::from(expected),
                    "`{name}` on an instance of the class `{own}` names"
                );
            }
            assert_eq!(
                unsafe { export(std::ptr::null()) },
                0,
                "`{name}` must be false for a null handle"
            );
        }

        for (_, handle) in errors {
            unsafe { kafka_common_Error_destroy(handle) };
        }
    }

    /// The 27 Rust-local negatives keep the values they were published with, and
    /// each client-side class maps to the enumerator named after it.
    ///
    /// These values are ABI (see [`kafka_common_ErrorCode_e`]): a new class
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
}
