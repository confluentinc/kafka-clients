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

//! `kafka_common_ConsumerCommitFailedError_t`:
//! `org.apache.kafka.clients.consumer.CommitFailedException` (prefixed
//! `Consumer` per CLAUDE.md §2 to avoid clashing with `common` errors).

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::consumer::ConsumerCommitFailedError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`ConsumerCommitFailedError`].
#[repr(C)]
pub struct kafka_common_ConsumerCommitFailedError_t {
    _private: [u8; 0],
}

impl PayloadClass for ConsumerCommitFailedError {
    fn message(&self) -> &str {
        ConsumerCommitFailedError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ConsumerCommitFailedError::source(self)
    }
}

/// The payload of a `CommitFailedException` error, borrowed from the error
/// handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_commit_failed(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerCommitFailedError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ConsumerCommitFailedError>` cloned from the variant's payload into the
    // `ErrorInner`'s payload slot, so it lives as long as the error handle: until the error, or the
    // owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerCommitFailed(e) => inner.payload_view(e) as *const kafka_common_ConsumerCommitFailedError_t,
        _ => ptr::null(),
    }
}

/// `new CommitFailedException(String message)`. Owned, freed with
/// [`kafka_common_ConsumerCommitFailedError_destroy`].
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerCommitFailedError_new(
    message: *const c_char,
) -> *mut kafka_common_ConsumerCommitFailedError_t {
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    Payload::boxed(ConsumerCommitFailedError::new(unsafe { c_str_to_string(message) }))
}

/// `new CommitFailedException()` with Java's default message.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ConsumerCommitFailedError_with_default_message()
-> *mut kafka_common_ConsumerCommitFailedError_t {
    Payload::boxed(ConsumerCommitFailedError::with_default_message())
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid commit-failed handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerCommitFailedError_message(
    self_: *const kafka_common_ConsumerCommitFailedError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid commit-failed handle: a view whose
    // error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires (no call
    // mutates a payload of this class). `message_ptr` points at the payload's own C string, so the
    // string lives as long as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { Payload::<ConsumerCommitFailedError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid commit-failed handle.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerCommitFailedError_source(
    self_: *const kafka_common_ConsumerCommitFailedError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid commit-failed handle: a view whose
    // error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires (no call
    // mutates a payload of this class). `source_ptr` returns the cause handle the payload's
    // `source` slot owns, so it lives as long as the handle; that is the rustdoc's "borrowed from
    // the handle".
    unsafe { Payload::<ConsumerCommitFailedError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid commit-failed handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerCommitFailedError_to_string(
    self_: *const kafka_common_ConsumerCommitFailedError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid commit-failed handle: a view whose
    // error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires (no call
    // mutates a payload of this class). The borrow lasts for this call only: `to_c_string` copies
    // the text into an owned string.
    unsafe { Payload::<ConsumerCommitFailedError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerCommitFailedError_destroy(
    self_: *mut kafka_common_ConsumerCommitFailedError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ConsumerCommitFailedError_t` handles come
    // only from this class's constructors, which box a `Payload<ConsumerCommitFailedError>`, and a
    // view is never passed here.
    unsafe { Payload::<ConsumerCommitFailedError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};

    #[test]
    fn message_is_borrowed_from_the_handle() {
        let error = box_error(Error::ConsumerCommitFailed(ConsumerCommitFailedError::new("m")));
        let message = CString::new("failed").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the errors, the payloads, the string) is destroyed once,
        // the view is used only while its error is alive, and `message` outlives the call that
        // reads it.
        unsafe {
            let view = kafka_common_Error_consumer_commit_failed(error);
            assert!(!view.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerCommitFailedError_message(view))
                    .to_str()
                    .unwrap(),
                "m"
            );
            assert!(kafka_common_ConsumerCommitFailedError_source(view).is_null());
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_consumer_commit_failed(other).is_null());
            kafka_common_Error_destroy(other);

            let built = kafka_common_ConsumerCommitFailedError_new(message.as_ptr());
            let s = kafka_common_ConsumerCommitFailedError_to_string(built);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ConsumerCommitFailedError::new("failed").to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ConsumerCommitFailedError_destroy(built);
            let default = kafka_common_ConsumerCommitFailedError_with_default_message();
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerCommitFailedError_message(default))
                    .to_str()
                    .unwrap(),
                ConsumerCommitFailedError::with_default_message().message()
            );
            kafka_common_ConsumerCommitFailedError_destroy(default);
            kafka_common_ConsumerCommitFailedError_destroy(ptr::null_mut());
        }
    }
}
