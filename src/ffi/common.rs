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
//! - The opaque [`kafka_common_KafkaError_t`] error handle and its accessor
//!   functions. `kafka_common_*` is shared verbatim between FFI surfaces — a
//!   second definition would make cbindgen emit a duplicate type.
//! - The default logger initialization helper ([`init_default_logger`]).

// FFI function names follow the kafka_<TypeName>_<method> convention with PascalCase
// type names, which intentionally differs from Rust's snake_case convention.
#![allow(non_snake_case, non_camel_case_types)]

use std::ffi::{CString, c_char};

use crate::common::KafkaError;

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

/// Internal wrapper that pairs [`KafkaError`] with a [`CString`] for the
/// error message, so that [`kafka_common_KafkaError_message`] can return a valid
/// `*const c_char` that lives as long as the handle.
pub(crate) struct KafkaErrorInner {
    pub(crate) error: KafkaError,
    /// Cached CString for the error message, created once at construction time.
    pub(crate) message_cstring: CString,
}

/// Opaque error handle returned by functions that can fail.
///
/// Internally wraps a `Box<KafkaErrorInner>` containing the [`KafkaError`]
/// and a cached [`CString`] for the error message.
///
/// A null `kafka_common_KafkaError_t` pointer means success (no error).
#[repr(C)]
pub struct kafka_common_KafkaError_t {
    _private: [u8; 0],
}

/// Wraps a [`KafkaError`] into a heap-allocated opaque error pointer, including
/// a cached [`CString`] for the error message.
pub(crate) fn box_error(error: KafkaError) -> *mut kafka_common_KafkaError_t {
    let message_cstring = CString::new(error.message()).unwrap_or_else(|_| CString::new("").unwrap());
    let inner = KafkaErrorInner { error, message_cstring };
    Box::into_raw(Box::new(inner)) as *mut kafka_common_KafkaError_t
}

/// Casts a `*const kafka_common_KafkaError_t` to a reference to `KafkaErrorInner`.
///
/// # Safety
///
/// The pointer must be non-null and must have been created by [`box_error`].
pub(crate) unsafe fn error_ref(error: *const kafka_common_KafkaError_t) -> &'static KafkaErrorInner {
    unsafe { &*(error as *const KafkaErrorInner) }
}

/// Returns the error code from a [`kafka_common_KafkaError_t`] handle.
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
pub unsafe extern "C" fn kafka_common_KafkaError_code(error: *const kafka_common_KafkaError_t) -> i32 {
    if error.is_null() {
        return 0;
    }
    i32::from(unsafe { error_ref(error) }.error.code())
}

/// Returns the error message as a null-terminated C string.
///
/// The returned pointer is valid until [`kafka_common_KafkaError_destroy`] is called on
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
pub unsafe extern "C" fn kafka_common_KafkaError_message(error: *const kafka_common_KafkaError_t) -> *const c_char {
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
pub unsafe extern "C" fn kafka_common_KafkaError_is_retriable(error: *const kafka_common_KafkaError_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_retriable()
}

/// Returns whether the error is fatal (unrecoverable).
///
/// # Parameters
///
/// - `error`: Non-null error handle.
///
/// # Returns
///
/// `true` if the error is fatal, `false` if not or if the handle is null.
///
/// # Safety
///
/// `error` must be a valid handle from a function that returned an error, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_KafkaError_is_fatal(error: *const kafka_common_KafkaError_t) -> bool {
    if error.is_null() {
        return false;
    }
    unsafe { error_ref(error) }.error.is_fatal()
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
pub unsafe extern "C" fn kafka_common_KafkaError_destroy(error: *mut kafka_common_KafkaError_t) {
    if !error.is_null() {
        unsafe {
            drop(Box::from_raw(error as *mut KafkaErrorInner));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::protocol::Errors;

    /// A boxed error must expose its code, message content, and classification
    /// flags across the ABI, and free cleanly. The message content is asserted
    /// (not just non-null) since it is part of the behavioral contract.
    #[test]
    fn test_error_round_trip_exposes_code_message_and_flags() {
        let error = KafkaError::with_message(Errors::CorruptMessage, "corrupt payload at offset 7");
        let expected_code = i32::from(error.code());
        let expected_retriable = error.is_retriable();
        let expected_fatal = error.is_fatal();

        let handle = box_error(error);
        assert!(!handle.is_null());

        unsafe {
            assert_eq!(kafka_common_KafkaError_code(handle), expected_code);

            let msg_ptr = kafka_common_KafkaError_message(handle);
            assert!(!msg_ptr.is_null());
            let msg = CStr::from_ptr(msg_ptr).to_str().unwrap();
            assert_eq!(msg, "corrupt payload at offset 7");

            assert_eq!(kafka_common_KafkaError_is_retriable(handle), expected_retriable);
            assert_eq!(kafka_common_KafkaError_is_fatal(handle), expected_fatal);

            kafka_common_KafkaError_destroy(handle);
        }
    }

    /// Every accessor must treat a null handle as "no error": code 0, null
    /// message, not retriable, not fatal, and destroy is a no-op.
    #[test]
    fn test_error_null_handle_is_safe() {
        unsafe {
            assert_eq!(kafka_common_KafkaError_code(std::ptr::null()), 0);
            assert!(kafka_common_KafkaError_message(std::ptr::null()).is_null());
            assert!(!kafka_common_KafkaError_is_retriable(std::ptr::null()));
            assert!(!kafka_common_KafkaError_is_fatal(std::ptr::null()));
            kafka_common_KafkaError_destroy(std::ptr::null_mut());
        }
    }
}
