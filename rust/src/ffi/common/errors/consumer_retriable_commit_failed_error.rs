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

//! `kafka_common_ConsumerRetriableCommitFailedError_t`:
//! `org.apache.kafka.clients.consumer.RetriableCommitFailedException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::consumer::ConsumerRetriableCommitFailedError;
use crate::ffi::common::errors::{Payload, PayloadClass, take_source};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`ConsumerRetriableCommitFailedError`].
#[repr(C)]
pub struct kafka_common_ConsumerRetriableCommitFailedError_t {
    _private: [u8; 0],
}

impl PayloadClass for ConsumerRetriableCommitFailedError {
    fn message(&self) -> &str {
        ConsumerRetriableCommitFailedError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ConsumerRetriableCommitFailedError::source(self)
    }
}

/// Returns the payload of a `RetriableCommitFailedException` error, or `NULL` when the error is
/// another class.
///
/// # Parameters
///
/// - `error` - The error handle.
///
/// # Safety
///
/// `error` must be a valid error handle.
/// The returned view is borrowed, valid as long as `error`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_retriable_commit_failed(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerRetriableCommitFailedError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ConsumerRetriableCommitFailedError>` cloned from the variant's payload into the
    // `ErrorInner`'s payload slot, so it lives as long as the error handle: until the error, or the
    // owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerRetriableCommitFailed(e) => {
            inner.payload_view(e) as *const kafka_common_ConsumerRetriableCommitFailedError_t
        },
        _ => ptr::null(),
    }
}

/// `new RetriableCommitFailedException(String message)`.
///
/// # Parameters
///
/// - `message` - The detail message
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string. The returned handle is owned, freed with
/// `kafka_common_ConsumerRetriableCommitFailedError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_new(
    message: *const c_char,
) -> *mut kafka_common_ConsumerRetriableCommitFailedError_t {
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    Payload::boxed(ConsumerRetriableCommitFailedError::new(unsafe { c_str_to_string(message) }))
}

/// `new RetriableCommitFailedException()` with Java's default message.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_ConsumerRetriableCommitFailedError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_with_default_message()
-> *mut kafka_common_ConsumerRetriableCommitFailedError_t {
    Payload::boxed(ConsumerRetriableCommitFailedError::with_default_message())
}

/// `new RetriableCommitFailedException(Throwable cause)`.
///
/// # Parameters
///
/// - `source` - The cause of the error.
///
/// # Safety
///
/// `source` must be an owned error handle not yet destroyed; it is consumed, so the caller must
/// not destroy it afterwards. The returned handle is owned, freed with
/// `kafka_common_ConsumerRetriableCommitFailedError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_with_source(
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_ConsumerRetriableCommitFailedError_t {
    // SAFETY: Per this function's `# Safety`, `source` is null or an owned error handle not yet
    // destroyed, as `take_source` requires; the constructor consumes it, so the caller does not use
    // it again.
    Payload::boxed(ConsumerRetriableCommitFailedError::with_source(unsafe { take_source(source) }))
}

/// Returns the detail message of the error.
///
/// # Safety
///
/// `self_` must be a valid retriable-commit-failed handle.
/// The returned string is borrowed, valid until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_message(
    self_: *const kafka_common_ConsumerRetriableCommitFailedError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid retriable-commit-failed handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `message_ptr` points at the payload's own
    // C string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::from_ptr(self_) }.message_ptr()
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid retriable-commit-failed handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_source(
    self_: *const kafka_common_ConsumerRetriableCommitFailedError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid retriable-commit-failed handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid retriable-commit-failed handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_to_string(
    self_: *const kafka_common_ConsumerRetriableCommitFailedError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid retriable-commit-failed handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). The borrow lasts for this call only:
    // `to_c_string` copies the text into an owned string.
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_destroy(
    self_: *mut kafka_common_ConsumerRetriableCommitFailedError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ConsumerRetriableCommitFailedError_t`
    // handles come only from this class's constructors, which box a
    // `Payload<ConsumerRetriableCommitFailedError>`, and a view is never passed here.
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_message};

    #[test]
    fn source_is_consumed_and_borrowed_back() {
        let error = box_error(Error::ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError::new(
            "m",
        )));
        let message = CString::new("retry").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the errors, the payloads, the string) is destroyed once,
        // `cause` is consumed by `with_source`, the view and the borrowed cause are used only while
        // their owner is alive, and `message` outlives the call that reads it.
        unsafe {
            let view = kafka_common_Error_consumer_retriable_commit_failed(error);
            assert!(!view.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerRetriableCommitFailedError_message(view))
                    .to_str()
                    .unwrap(),
                "m"
            );
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_consumer_retriable_commit_failed(other).is_null());
            kafka_common_Error_destroy(other);

            let cause = box_error(Error::kafka_message("cause"));
            let built = kafka_common_ConsumerRetriableCommitFailedError_with_source(cause);
            let source = kafka_common_ConsumerRetriableCommitFailedError_source(built);
            assert!(!source.is_null());
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(source)).to_str().unwrap(), "cause");
            // Cached: the same borrowed handle comes back.
            assert_eq!(source, kafka_common_ConsumerRetriableCommitFailedError_source(built));
            let s = kafka_common_ConsumerRetriableCommitFailedError_to_string(built);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ConsumerRetriableCommitFailedError::with_source(Error::kafka_message("cause")).to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ConsumerRetriableCommitFailedError_destroy(built);
            kafka_common_ConsumerRetriableCommitFailedError_destroy(
                kafka_common_ConsumerRetriableCommitFailedError_new(message.as_ptr()),
            );
            kafka_common_ConsumerRetriableCommitFailedError_destroy(
                kafka_common_ConsumerRetriableCommitFailedError_with_default_message(),
            );
            kafka_common_ConsumerRetriableCommitFailedError_destroy(ptr::null_mut());
        }
    }
}
