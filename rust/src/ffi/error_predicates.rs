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

//! C exports of `Error`'s predicates -- GENERATED, DO NOT EDIT.
//!
//! Generated from the predicates on `impl Error` in `src/common/error.rs` by
//! `cargo xtask generate-error-predicates`, and checked for staleness by
//! `cargo xtask check-generated`.
//!
//! One export per predicate, named after it behind the type prefix
//! (`is_retriable_error` -> `kafka_common_Error_is_retriable_error`,
//! CLAUDE.md §4). C cannot see the error's variants, so these predicates are
//! how a C caller classifies an error beyond its numeric code
//! (`kafka_common_Error__enum`). Each returns `1` when it holds and `0` when it
//! does not or the handle is null: booleans cross the C boundary as `int8_t`
//! (CLAUDE.md §4), never as `bool`.

use super::common::{error_ref, kafka_common_Error_t};
use super::ffi_guard;

/// Returns whether this is a Kafka error rather than a generic programming error.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_kafka_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_kafka_error())
}

/// Returns whether this error's Java class is `ApiException` or extends it.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_api_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_api_error())
}

/// Returns whether this error's Java class extends `RetriableException` — i.e. whether re-sending
/// the failed request can succeed.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_retriable_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_retriable_error())
}

/// Returns whether this error's Java class extends `RefreshRetriableException` — retriable, and a
/// metadata / coordinator refresh is what clears it.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_refresh_retriable_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_refresh_retriable_error())
}

/// Returns whether this error's Java class extends `TimeoutException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_timeout_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_timeout_error())
}

/// Returns whether this error's Java class extends `InvalidMetadataException` — the client's
/// cached metadata may be stale.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_metadata_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_metadata_error())
}

/// Returns whether this error's Java class extends `InvalidConfigurationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_configuration_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_configuration_error())
}

/// Returns whether this error's Java class extends `ApplicationRecoverableException` — the
/// application can recover, but only by re-initialising its producer or rejoining its group; the
/// current epoch or session is gone.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_application_recoverable_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_application_recoverable_error())
}

/// Returns whether this error's Java class extends `InvalidOffsetException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_offset_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_offset_error())
}

/// Returns whether this error's Java class extends `OutOfOrderSequenceException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_out_of_order_sequence_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_out_of_order_sequence_error())
}

/// Returns whether this error's Java class extends
/// `org.apache.kafka.clients.consumer.InvalidOffsetException` — no offset is usable for the
/// partition.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_invalid_offset_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_consumer_invalid_offset_error())
}

/// Returns whether this error's Java class extends
/// `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_offset_out_of_range_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_consumer_offset_out_of_range_error())
}

/// Returns whether this error's Java class extends `SerializationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_serialization_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_serialization_error())
}

/// Returns whether this error's Java class extends `AuthenticationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authentication_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_authentication_error())
}

/// Returns whether this error's Java class extends `AuthorizationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authorization_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_authorization_error())
}

/// Returns whether this error's Java class is, or extends, `java.lang.IllegalArgumentException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_illegal_argument_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_local_illegal_argument_error())
}

/// Returns whether this error's Java class is, or extends, `java.lang.IllegalStateException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_illegal_state_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_local_illegal_state_error())
}

/// Returns whether this error's Java class is, or extends,
/// `java.util.ConcurrentModificationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_concurrent_modification_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_local_concurrent_modification_error())
}

/// Returns whether this error's Java class is, or extends, `java.util.concurrent.TimeoutException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_timeout_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_local_timeout_error())
}

/// Returns whether this error's Java class is, or extends,
/// `java.lang.UnsupportedOperationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_unsupported_operation_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_local_unsupported_operation_error())
}

/// Returns whether this is an error raised by a callback written in another language
/// (`Error::LocalCallback`; no Java class).
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_local_callback_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_local_callback_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.AuthorizerNotReadyException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_authorizer_not_ready_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_authorizer_not_ready_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.BootstrapResolutionException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_bootstrap_resolution_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_bootstrap_resolution_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.BrokerIdNotRegisteredException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_broker_id_not_registered_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_broker_id_not_registered_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.BrokerNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_broker_not_available_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_broker_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.producer.BufferExhaustedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_producer_buffer_exhausted_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_producer_buffer_exhausted_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ClusterAuthorizationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_cluster_authorization_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_cluster_authorization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ConcurrentTransactionsException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_concurrent_transactions_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_concurrent_transactions_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ControllerIdNotRegisteredException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_controller_id_not_registered_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_controller_id_not_registered_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ControllerMovedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_controller_moved_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_controller_moved_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.CoordinatorLoadInProgressException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_coordinator_load_in_progress_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_coordinator_load_in_progress_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.CoordinatorNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_coordinator_not_available_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_coordinator_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.requests.CorrelationIdMismatchException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_correlation_id_mismatch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_correlation_id_mismatch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.CorruptRecordException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_corrupt_record_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_corrupt_record_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenAuthorizationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_authorization_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_delegation_token_authorization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenDisabledException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_disabled_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_delegation_token_disabled_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenExpiredException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_expired_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_delegation_token_expired_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_not_found_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_delegation_token_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_delegation_token_owner_mismatch_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_delegation_token_owner_mismatch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DisconnectException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_disconnect_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_disconnect_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateBrokerRegistrationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_broker_registration_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_duplicate_broker_registration_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateResourceException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_resource_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_duplicate_resource_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateSequenceException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_sequence_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_duplicate_sequence_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.DuplicateVoterException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_duplicate_voter_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_duplicate_voter_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ElectionNotNeededException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_election_not_needed_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_election_not_needed_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.EligibleLeadersNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_eligible_leaders_not_available_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_eligible_leaders_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FeatureUpdateFailedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_feature_update_failed_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_feature_update_failed_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedInstanceIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_instance_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_fenced_instance_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedLeaderEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_leader_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_fenced_leader_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedMemberEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_member_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_fenced_member_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FencedStateEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fenced_state_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_fenced_state_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FetchSessionIdNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fetch_session_id_not_found_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_fetch_session_id_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.FetchSessionTopicIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_fetch_session_topic_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_fetch_session_topic_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupAuthorizationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_authorization_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_group_authorization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupDeletionFailedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_deletion_failed_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_group_deletion_failed_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupIdNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_id_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_group_id_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupMaxSizeReachedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_max_size_reached_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_group_max_size_reached_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupNotEmptyException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_not_empty_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_group_not_empty_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.GroupSubscribedToTopicException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_group_subscribed_to_topic_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_group_subscribed_to_topic_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.IllegalGenerationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_illegal_generation_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_illegal_generation_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.IllegalSaslStateException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_illegal_sasl_state_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_illegal_sasl_state_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentClusterIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_cluster_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_inconsistent_cluster_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentGroupProtocolException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_group_protocol_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_inconsistent_group_protocol_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentTopicIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_topic_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_inconsistent_topic_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InconsistentVoterSetException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_inconsistent_voter_set_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_inconsistent_voter_set_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.IneligibleReplicaException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_ineligible_replica_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_ineligible_replica_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InterruptException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_interrupt_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_interrupt_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidCommitOffsetSizeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_commit_offset_size_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_commit_offset_size_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidFetchSessionEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_fetch_session_epoch_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_fetch_session_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidFetchSizeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_fetch_size_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_fetch_size_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidGroupIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_group_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_group_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidPartitionsException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_partitions_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_partitions_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidPidMappingException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_pid_mapping_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_pid_mapping_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidPrincipalTypeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_principal_type_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_principal_type_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidProducerEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_producer_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_producer_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.InvalidRecordException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_record_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_record_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRecordStateException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_record_state_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_record_state_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRegistrationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_registration_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_registration_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRegularExpression`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_regular_expression_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_regular_expression_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidReplicaAssignmentException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_replica_assignment_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_replica_assignment_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidReplicationFactorException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_replication_factor_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_replication_factor_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRequestException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_request_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_request_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidRequiredAcksException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_required_acks_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_required_acks_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidSessionTimeoutException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_session_timeout_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_session_timeout_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidShareSessionEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_share_session_epoch_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_share_session_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTimestampException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_timestamp_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_timestamp_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTopicException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_topic_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_topic_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTxnStateException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_txn_state_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_txn_state_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidTxnTimeoutException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_txn_timeout_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_txn_timeout_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidUpdateVersionException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_update_version_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_update_version_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.InvalidVoterKeyException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_voter_key_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_voter_key_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.KafkaStorageException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_kafka_storage_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_kafka_storage_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.LeaderNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_leader_not_available_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_leader_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ListenerNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_listener_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_listener_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.LogDirNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_log_dir_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_log_dir_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.MemberIdRequiredException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_member_id_required_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_member_id_required_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.MismatchedEndpointTypeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_mismatched_endpoint_type_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_mismatched_endpoint_type_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NetworkException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_network_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_network_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NewLeaderElectedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_new_leader_elected_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_new_leader_elected_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NoReassignmentInProgressException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_no_reassignment_in_progress_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_no_reassignment_in_progress_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotControllerException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_controller_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_not_controller_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotCoordinatorException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_coordinator_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_not_coordinator_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotEnoughReplicasException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_enough_replicas_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_not_enough_replicas_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_enough_replicas_after_append_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_not_enough_replicas_after_append_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.NotLeaderOrFollowerException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_not_leader_or_follower_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_not_leader_or_follower_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetMetadataTooLarge`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_metadata_too_large_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_offset_metadata_too_large_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetMovedToTieredStorageException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_moved_to_tiered_storage_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_offset_moved_to_tiered_storage_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_not_available_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_offset_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OffsetOutOfRangeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_offset_out_of_range_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_offset_out_of_range_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.OperationNotAttemptedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_operation_not_attempted_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_operation_not_attempted_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PolicyViolationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_policy_violation_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_policy_violation_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PositionOutOfRangeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_position_out_of_range_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_position_out_of_range_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PreferredLeaderNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_preferred_leader_not_available_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_preferred_leader_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.PrincipalDeserializationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_principal_deserialization_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_principal_deserialization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ProducerFencedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_producer_fenced_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_producer_fenced_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.metrics.QuotaViolationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_quota_violation_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_quota_violation_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ReassignmentInProgressException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_reassignment_in_progress_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_reassignment_in_progress_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RebalanceInProgressException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_rebalance_in_progress_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_rebalance_in_progress_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RebootstrapRequiredException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_rebootstrap_required_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_rebootstrap_required_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RecordBatchTooLargeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_record_batch_too_large_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_record_batch_too_large_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RecordDeserializationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_record_deserialization_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_record_deserialization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.RecordTooLargeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_record_too_large_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_record_too_large_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.network.InvalidReceiveException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_invalid_receive_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_invalid_receive_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.config.ConfigException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_config_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_config_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.RetriableCommitFailedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_retriable_commit_failed_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_consumer_retriable_commit_failed_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.CommitFailedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_commit_failed_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_consumer_commit_failed_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_no_offset_for_partition_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_consumer_no_offset_for_partition_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.clients.consumer.LogTruncationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_consumer_log_truncation_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_consumer_log_truncation_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ReplicaNotAvailableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_replica_not_available_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_replica_not_available_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ResourceNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_resource_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_resource_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SaslAuthenticationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_sasl_authentication_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_sasl_authentication_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.protocol.types.SchemaException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_schema_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_schema_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SecurityDisabledException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_security_disabled_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_security_disabled_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ShareSessionLimitReachedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_share_session_limit_reached_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_share_session_limit_reached_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ShareSessionNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_share_session_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_share_session_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SnapshotNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_snapshot_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_snapshot_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.SslAuthenticationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_ssl_authentication_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_ssl_authentication_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StaleBrokerEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_stale_broker_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_stale_broker_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StaleMemberEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_stale_member_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_stale_member_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsInvalidTopologyException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_invalid_topology_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_streams_invalid_topology_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_invalid_topology_epoch_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_streams_invalid_topology_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_topology_description_update_failed_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(
        unsafe { error_ref(error) }
            .error
            .is_streams_topology_description_update_failed_error(),
    )
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.StreamsTopologyFencedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_streams_topology_fenced_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_streams_topology_fenced_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TelemetryTooLargeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_telemetry_too_large_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_telemetry_too_large_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_throttling_quota_exceeded_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_throttling_quota_exceeded_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TopicAuthorizationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_topic_authorization_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_topic_authorization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TopicDeletionDisabledException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_topic_deletion_disabled_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_topic_deletion_disabled_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TopicExistsException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_topic_exists_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_topic_exists_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionAbortableException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_abortable_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_transaction_abortable_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionAbortedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_aborted_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_transaction_aborted_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionCoordinatorFencedException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transaction_coordinator_fenced_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_transaction_coordinator_fenced_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionalIdAuthorizationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transactional_id_authorization_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_transactional_id_authorization_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.TransactionalIdNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_transactional_id_not_found_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_transactional_id_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnacceptableCredentialException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unacceptable_credential_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unacceptable_credential_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownControllerIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_controller_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_controller_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownLeaderEpochException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_leader_epoch_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_leader_epoch_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownMemberIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_member_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_member_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownProducerIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_producer_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_producer_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownServerException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_server_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_server_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownSubscriptionIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_subscription_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_subscription_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownTopicIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_topic_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_topic_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnknownTopicOrPartitionException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unknown_topic_or_partition_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unknown_topic_or_partition_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnreleasedInstanceIdException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unreleased_instance_id_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unreleased_instance_id_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnstableOffsetCommitException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unstable_offset_commit_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unstable_offset_commit_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedAssignorException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_assignor_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_assignor_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedByAuthenticationException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_by_authentication_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_by_authentication_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedCompressionTypeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_compression_type_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_compression_type_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedEndpointTypeException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_endpoint_type_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_endpoint_type_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedForMessageFormatException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_for_message_format_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_for_message_format_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.UnsupportedSaslMechanismException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_sasl_mechanism_error(
    error: *const kafka_common_Error_t,
) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_sasl_mechanism_error())
}

/// Returns whether this error's Java class extends `UnsupportedVersionException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_unsupported_version_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_unsupported_version_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.VoterNotFoundException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_voter_not_found_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_voter_not_found_error())
}

/// Returns whether this error's Java class is, or extends,
/// `org.apache.kafka.common.errors.WakeupException`.
///
/// # Parameters
///
/// - `error`: The error handle, or `NULL`.
///
/// # Returns
///
/// `1` if the predicate holds, `0` if not or if the handle is `NULL`.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_is_wakeup_error(error: *const kafka_common_Error_t) -> i8 {
    if error.is_null() {
        return 0;
    }
    // SAFETY: `error` is non-null (checked above) and, per this function's `# Safety`, a
    // valid error handle: an owned one from `box_error`, not yet destroyed, or a borrowed one
    // whose owner is still alive, so it points at a live `ErrorInner`, as `error_ref` requires.
    // The borrow lasts only for this call, while the caller keeps the handle alive.
    i8::from(unsafe { error_ref(error) }.error.is_wakeup_error())
}

/// Every export above, by the name of the predicate it mirrors, for the tests
/// that check each export against every error class.
#[cfg(test)]
pub(crate) const PREDICATES: &[(&str, unsafe extern "C" fn(*const kafka_common_Error_t) -> i8)] = &[
    ("is_kafka_error", kafka_common_Error_is_kafka_error),
    ("is_api_error", kafka_common_Error_is_api_error),
    ("is_retriable_error", kafka_common_Error_is_retriable_error),
    ("is_refresh_retriable_error", kafka_common_Error_is_refresh_retriable_error),
    ("is_timeout_error", kafka_common_Error_is_timeout_error),
    ("is_invalid_metadata_error", kafka_common_Error_is_invalid_metadata_error),
    (
        "is_invalid_configuration_error",
        kafka_common_Error_is_invalid_configuration_error,
    ),
    (
        "is_application_recoverable_error",
        kafka_common_Error_is_application_recoverable_error,
    ),
    ("is_invalid_offset_error", kafka_common_Error_is_invalid_offset_error),
    (
        "is_out_of_order_sequence_error",
        kafka_common_Error_is_out_of_order_sequence_error,
    ),
    (
        "is_consumer_invalid_offset_error",
        kafka_common_Error_is_consumer_invalid_offset_error,
    ),
    (
        "is_consumer_offset_out_of_range_error",
        kafka_common_Error_is_consumer_offset_out_of_range_error,
    ),
    ("is_serialization_error", kafka_common_Error_is_serialization_error),
    ("is_authentication_error", kafka_common_Error_is_authentication_error),
    ("is_authorization_error", kafka_common_Error_is_authorization_error),
    (
        "is_local_illegal_argument_error",
        kafka_common_Error_is_local_illegal_argument_error,
    ),
    ("is_local_illegal_state_error", kafka_common_Error_is_local_illegal_state_error),
    (
        "is_local_concurrent_modification_error",
        kafka_common_Error_is_local_concurrent_modification_error,
    ),
    ("is_local_timeout_error", kafka_common_Error_is_local_timeout_error),
    (
        "is_local_unsupported_operation_error",
        kafka_common_Error_is_local_unsupported_operation_error,
    ),
    ("is_local_callback_error", kafka_common_Error_is_local_callback_error),
    (
        "is_authorizer_not_ready_error",
        kafka_common_Error_is_authorizer_not_ready_error,
    ),
    (
        "is_bootstrap_resolution_error",
        kafka_common_Error_is_bootstrap_resolution_error,
    ),
    (
        "is_broker_id_not_registered_error",
        kafka_common_Error_is_broker_id_not_registered_error,
    ),
    (
        "is_broker_not_available_error",
        kafka_common_Error_is_broker_not_available_error,
    ),
    (
        "is_producer_buffer_exhausted_error",
        kafka_common_Error_is_producer_buffer_exhausted_error,
    ),
    (
        "is_cluster_authorization_error",
        kafka_common_Error_is_cluster_authorization_error,
    ),
    (
        "is_concurrent_transactions_error",
        kafka_common_Error_is_concurrent_transactions_error,
    ),
    (
        "is_controller_id_not_registered_error",
        kafka_common_Error_is_controller_id_not_registered_error,
    ),
    ("is_controller_moved_error", kafka_common_Error_is_controller_moved_error),
    (
        "is_coordinator_load_in_progress_error",
        kafka_common_Error_is_coordinator_load_in_progress_error,
    ),
    (
        "is_coordinator_not_available_error",
        kafka_common_Error_is_coordinator_not_available_error,
    ),
    (
        "is_correlation_id_mismatch_error",
        kafka_common_Error_is_correlation_id_mismatch_error,
    ),
    ("is_corrupt_record_error", kafka_common_Error_is_corrupt_record_error),
    (
        "is_delegation_token_authorization_error",
        kafka_common_Error_is_delegation_token_authorization_error,
    ),
    (
        "is_delegation_token_disabled_error",
        kafka_common_Error_is_delegation_token_disabled_error,
    ),
    (
        "is_delegation_token_expired_error",
        kafka_common_Error_is_delegation_token_expired_error,
    ),
    (
        "is_delegation_token_not_found_error",
        kafka_common_Error_is_delegation_token_not_found_error,
    ),
    (
        "is_delegation_token_owner_mismatch_error",
        kafka_common_Error_is_delegation_token_owner_mismatch_error,
    ),
    ("is_disconnect_error", kafka_common_Error_is_disconnect_error),
    (
        "is_duplicate_broker_registration_error",
        kafka_common_Error_is_duplicate_broker_registration_error,
    ),
    ("is_duplicate_resource_error", kafka_common_Error_is_duplicate_resource_error),
    ("is_duplicate_sequence_error", kafka_common_Error_is_duplicate_sequence_error),
    ("is_duplicate_voter_error", kafka_common_Error_is_duplicate_voter_error),
    ("is_election_not_needed_error", kafka_common_Error_is_election_not_needed_error),
    (
        "is_eligible_leaders_not_available_error",
        kafka_common_Error_is_eligible_leaders_not_available_error,
    ),
    (
        "is_feature_update_failed_error",
        kafka_common_Error_is_feature_update_failed_error,
    ),
    ("is_fenced_instance_id_error", kafka_common_Error_is_fenced_instance_id_error),
    ("is_fenced_leader_epoch_error", kafka_common_Error_is_fenced_leader_epoch_error),
    ("is_fenced_member_epoch_error", kafka_common_Error_is_fenced_member_epoch_error),
    ("is_fenced_state_epoch_error", kafka_common_Error_is_fenced_state_epoch_error),
    (
        "is_fetch_session_id_not_found_error",
        kafka_common_Error_is_fetch_session_id_not_found_error,
    ),
    (
        "is_fetch_session_topic_id_error",
        kafka_common_Error_is_fetch_session_topic_id_error,
    ),
    ("is_group_authorization_error", kafka_common_Error_is_group_authorization_error),
    (
        "is_group_deletion_failed_error",
        kafka_common_Error_is_group_deletion_failed_error,
    ),
    ("is_group_id_not_found_error", kafka_common_Error_is_group_id_not_found_error),
    (
        "is_group_max_size_reached_error",
        kafka_common_Error_is_group_max_size_reached_error,
    ),
    ("is_group_not_empty_error", kafka_common_Error_is_group_not_empty_error),
    (
        "is_group_subscribed_to_topic_error",
        kafka_common_Error_is_group_subscribed_to_topic_error,
    ),
    ("is_illegal_generation_error", kafka_common_Error_is_illegal_generation_error),
    ("is_illegal_sasl_state_error", kafka_common_Error_is_illegal_sasl_state_error),
    (
        "is_inconsistent_cluster_id_error",
        kafka_common_Error_is_inconsistent_cluster_id_error,
    ),
    (
        "is_inconsistent_group_protocol_error",
        kafka_common_Error_is_inconsistent_group_protocol_error,
    ),
    (
        "is_inconsistent_topic_id_error",
        kafka_common_Error_is_inconsistent_topic_id_error,
    ),
    (
        "is_inconsistent_voter_set_error",
        kafka_common_Error_is_inconsistent_voter_set_error,
    ),
    ("is_ineligible_replica_error", kafka_common_Error_is_ineligible_replica_error),
    ("is_interrupt_error", kafka_common_Error_is_interrupt_error),
    (
        "is_invalid_commit_offset_size_error",
        kafka_common_Error_is_invalid_commit_offset_size_error,
    ),
    (
        "is_invalid_fetch_session_epoch_error",
        kafka_common_Error_is_invalid_fetch_session_epoch_error,
    ),
    ("is_invalid_fetch_size_error", kafka_common_Error_is_invalid_fetch_size_error),
    ("is_invalid_group_id_error", kafka_common_Error_is_invalid_group_id_error),
    ("is_invalid_partitions_error", kafka_common_Error_is_invalid_partitions_error),
    ("is_invalid_pid_mapping_error", kafka_common_Error_is_invalid_pid_mapping_error),
    (
        "is_invalid_principal_type_error",
        kafka_common_Error_is_invalid_principal_type_error,
    ),
    (
        "is_invalid_producer_epoch_error",
        kafka_common_Error_is_invalid_producer_epoch_error,
    ),
    ("is_invalid_record_error", kafka_common_Error_is_invalid_record_error),
    (
        "is_invalid_record_state_error",
        kafka_common_Error_is_invalid_record_state_error,
    ),
    (
        "is_invalid_registration_error",
        kafka_common_Error_is_invalid_registration_error,
    ),
    (
        "is_invalid_regular_expression_error",
        kafka_common_Error_is_invalid_regular_expression_error,
    ),
    (
        "is_invalid_replica_assignment_error",
        kafka_common_Error_is_invalid_replica_assignment_error,
    ),
    (
        "is_invalid_replication_factor_error",
        kafka_common_Error_is_invalid_replication_factor_error,
    ),
    ("is_invalid_request_error", kafka_common_Error_is_invalid_request_error),
    (
        "is_invalid_required_acks_error",
        kafka_common_Error_is_invalid_required_acks_error,
    ),
    (
        "is_invalid_session_timeout_error",
        kafka_common_Error_is_invalid_session_timeout_error,
    ),
    (
        "is_invalid_share_session_epoch_error",
        kafka_common_Error_is_invalid_share_session_epoch_error,
    ),
    ("is_invalid_timestamp_error", kafka_common_Error_is_invalid_timestamp_error),
    ("is_invalid_topic_error", kafka_common_Error_is_invalid_topic_error),
    ("is_invalid_txn_state_error", kafka_common_Error_is_invalid_txn_state_error),
    ("is_invalid_txn_timeout_error", kafka_common_Error_is_invalid_txn_timeout_error),
    (
        "is_invalid_update_version_error",
        kafka_common_Error_is_invalid_update_version_error,
    ),
    ("is_invalid_voter_key_error", kafka_common_Error_is_invalid_voter_key_error),
    ("is_kafka_storage_error", kafka_common_Error_is_kafka_storage_error),
    (
        "is_leader_not_available_error",
        kafka_common_Error_is_leader_not_available_error,
    ),
    ("is_listener_not_found_error", kafka_common_Error_is_listener_not_found_error),
    ("is_log_dir_not_found_error", kafka_common_Error_is_log_dir_not_found_error),
    ("is_member_id_required_error", kafka_common_Error_is_member_id_required_error),
    (
        "is_mismatched_endpoint_type_error",
        kafka_common_Error_is_mismatched_endpoint_type_error,
    ),
    ("is_network_error", kafka_common_Error_is_network_error),
    ("is_new_leader_elected_error", kafka_common_Error_is_new_leader_elected_error),
    (
        "is_no_reassignment_in_progress_error",
        kafka_common_Error_is_no_reassignment_in_progress_error,
    ),
    ("is_not_controller_error", kafka_common_Error_is_not_controller_error),
    ("is_not_coordinator_error", kafka_common_Error_is_not_coordinator_error),
    ("is_not_enough_replicas_error", kafka_common_Error_is_not_enough_replicas_error),
    (
        "is_not_enough_replicas_after_append_error",
        kafka_common_Error_is_not_enough_replicas_after_append_error,
    ),
    (
        "is_not_leader_or_follower_error",
        kafka_common_Error_is_not_leader_or_follower_error,
    ),
    (
        "is_offset_metadata_too_large_error",
        kafka_common_Error_is_offset_metadata_too_large_error,
    ),
    (
        "is_offset_moved_to_tiered_storage_error",
        kafka_common_Error_is_offset_moved_to_tiered_storage_error,
    ),
    (
        "is_offset_not_available_error",
        kafka_common_Error_is_offset_not_available_error,
    ),
    ("is_offset_out_of_range_error", kafka_common_Error_is_offset_out_of_range_error),
    (
        "is_operation_not_attempted_error",
        kafka_common_Error_is_operation_not_attempted_error,
    ),
    ("is_policy_violation_error", kafka_common_Error_is_policy_violation_error),
    (
        "is_position_out_of_range_error",
        kafka_common_Error_is_position_out_of_range_error,
    ),
    (
        "is_preferred_leader_not_available_error",
        kafka_common_Error_is_preferred_leader_not_available_error,
    ),
    (
        "is_principal_deserialization_error",
        kafka_common_Error_is_principal_deserialization_error,
    ),
    ("is_producer_fenced_error", kafka_common_Error_is_producer_fenced_error),
    ("is_quota_violation_error", kafka_common_Error_is_quota_violation_error),
    (
        "is_reassignment_in_progress_error",
        kafka_common_Error_is_reassignment_in_progress_error,
    ),
    (
        "is_rebalance_in_progress_error",
        kafka_common_Error_is_rebalance_in_progress_error,
    ),
    (
        "is_rebootstrap_required_error",
        kafka_common_Error_is_rebootstrap_required_error,
    ),
    (
        "is_record_batch_too_large_error",
        kafka_common_Error_is_record_batch_too_large_error,
    ),
    (
        "is_record_deserialization_error",
        kafka_common_Error_is_record_deserialization_error,
    ),
    ("is_record_too_large_error", kafka_common_Error_is_record_too_large_error),
    ("is_invalid_receive_error", kafka_common_Error_is_invalid_receive_error),
    ("is_config_error", kafka_common_Error_is_config_error),
    (
        "is_consumer_retriable_commit_failed_error",
        kafka_common_Error_is_consumer_retriable_commit_failed_error,
    ),
    (
        "is_consumer_commit_failed_error",
        kafka_common_Error_is_consumer_commit_failed_error,
    ),
    (
        "is_consumer_no_offset_for_partition_error",
        kafka_common_Error_is_consumer_no_offset_for_partition_error,
    ),
    (
        "is_consumer_log_truncation_error",
        kafka_common_Error_is_consumer_log_truncation_error,
    ),
    (
        "is_replica_not_available_error",
        kafka_common_Error_is_replica_not_available_error,
    ),
    ("is_resource_not_found_error", kafka_common_Error_is_resource_not_found_error),
    ("is_sasl_authentication_error", kafka_common_Error_is_sasl_authentication_error),
    ("is_schema_error", kafka_common_Error_is_schema_error),
    ("is_security_disabled_error", kafka_common_Error_is_security_disabled_error),
    (
        "is_share_session_limit_reached_error",
        kafka_common_Error_is_share_session_limit_reached_error,
    ),
    (
        "is_share_session_not_found_error",
        kafka_common_Error_is_share_session_not_found_error,
    ),
    ("is_snapshot_not_found_error", kafka_common_Error_is_snapshot_not_found_error),
    ("is_ssl_authentication_error", kafka_common_Error_is_ssl_authentication_error),
    ("is_stale_broker_epoch_error", kafka_common_Error_is_stale_broker_epoch_error),
    ("is_stale_member_epoch_error", kafka_common_Error_is_stale_member_epoch_error),
    (
        "is_streams_invalid_topology_error",
        kafka_common_Error_is_streams_invalid_topology_error,
    ),
    (
        "is_streams_invalid_topology_epoch_error",
        kafka_common_Error_is_streams_invalid_topology_epoch_error,
    ),
    (
        "is_streams_topology_description_update_failed_error",
        kafka_common_Error_is_streams_topology_description_update_failed_error,
    ),
    (
        "is_streams_topology_fenced_error",
        kafka_common_Error_is_streams_topology_fenced_error,
    ),
    ("is_telemetry_too_large_error", kafka_common_Error_is_telemetry_too_large_error),
    (
        "is_throttling_quota_exceeded_error",
        kafka_common_Error_is_throttling_quota_exceeded_error,
    ),
    ("is_topic_authorization_error", kafka_common_Error_is_topic_authorization_error),
    (
        "is_topic_deletion_disabled_error",
        kafka_common_Error_is_topic_deletion_disabled_error,
    ),
    ("is_topic_exists_error", kafka_common_Error_is_topic_exists_error),
    (
        "is_transaction_abortable_error",
        kafka_common_Error_is_transaction_abortable_error,
    ),
    ("is_transaction_aborted_error", kafka_common_Error_is_transaction_aborted_error),
    (
        "is_transaction_coordinator_fenced_error",
        kafka_common_Error_is_transaction_coordinator_fenced_error,
    ),
    (
        "is_transactional_id_authorization_error",
        kafka_common_Error_is_transactional_id_authorization_error,
    ),
    (
        "is_transactional_id_not_found_error",
        kafka_common_Error_is_transactional_id_not_found_error,
    ),
    (
        "is_unacceptable_credential_error",
        kafka_common_Error_is_unacceptable_credential_error,
    ),
    (
        "is_unknown_controller_id_error",
        kafka_common_Error_is_unknown_controller_id_error,
    ),
    (
        "is_unknown_leader_epoch_error",
        kafka_common_Error_is_unknown_leader_epoch_error,
    ),
    ("is_unknown_member_id_error", kafka_common_Error_is_unknown_member_id_error),
    ("is_unknown_producer_id_error", kafka_common_Error_is_unknown_producer_id_error),
    ("is_unknown_server_error", kafka_common_Error_is_unknown_server_error),
    (
        "is_unknown_subscription_id_error",
        kafka_common_Error_is_unknown_subscription_id_error,
    ),
    ("is_unknown_topic_id_error", kafka_common_Error_is_unknown_topic_id_error),
    (
        "is_unknown_topic_or_partition_error",
        kafka_common_Error_is_unknown_topic_or_partition_error,
    ),
    (
        "is_unreleased_instance_id_error",
        kafka_common_Error_is_unreleased_instance_id_error,
    ),
    (
        "is_unstable_offset_commit_error",
        kafka_common_Error_is_unstable_offset_commit_error,
    ),
    (
        "is_unsupported_assignor_error",
        kafka_common_Error_is_unsupported_assignor_error,
    ),
    (
        "is_unsupported_by_authentication_error",
        kafka_common_Error_is_unsupported_by_authentication_error,
    ),
    (
        "is_unsupported_compression_type_error",
        kafka_common_Error_is_unsupported_compression_type_error,
    ),
    (
        "is_unsupported_endpoint_type_error",
        kafka_common_Error_is_unsupported_endpoint_type_error,
    ),
    (
        "is_unsupported_for_message_format_error",
        kafka_common_Error_is_unsupported_for_message_format_error,
    ),
    (
        "is_unsupported_sasl_mechanism_error",
        kafka_common_Error_is_unsupported_sasl_mechanism_error,
    ),
    ("is_unsupported_version_error", kafka_common_Error_is_unsupported_version_error),
    ("is_voter_not_found_error", kafka_common_Error_is_voter_not_found_error),
    ("is_wakeup_error", kafka_common_Error_is_wakeup_error),
];
