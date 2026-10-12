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

//! `kafka_common_ThrottlingQuotaExceededError_t`:
//! `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::ThrottlingQuotaExceededError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`ThrottlingQuotaExceededError`].
#[repr(C)]
pub struct kafka_common_ThrottlingQuotaExceededError_t {
    _private: [u8; 0],
}

impl PayloadClass for ThrottlingQuotaExceededError {
    fn message(&self) -> &str {
        ThrottlingQuotaExceededError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ThrottlingQuotaExceededError::source(self)
    }
}

/// The payload of a `ThrottlingQuotaExceededException` error, borrowed from
/// the error handle, or null when the error is another class. The suffix
/// stays because `kafka_common_Error_throttling_quota_exceeded` is the
/// `Error` factory.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttling_quota_exceeded_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ThrottlingQuotaExceededError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ThrottlingQuotaExceededError>` cloned from the variant's payload into the
    // `ErrorInner`'s payload slot, so it lives as long as the error handle: until the error, or the
    // owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ThrottlingQuotaExceeded(e) => {
            inner.payload_view(e) as *const kafka_common_ThrottlingQuotaExceededError_t
        },
        _ => ptr::null(),
    }
}

/// `new ThrottlingQuotaExceededException(int throttleTimeMs, String
/// message)`. Owned, freed with
/// [`kafka_common_ThrottlingQuotaExceededError_destroy`].
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_new(
    throttle_time_ms: i32,
    message: *const c_char,
) -> *mut kafka_common_ThrottlingQuotaExceededError_t {
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    Payload::boxed(ThrottlingQuotaExceededError::new(throttle_time_ms, unsafe {
        c_str_to_string(message)
    }))
}

/// `new ThrottlingQuotaExceededException(String message)` with Java's
/// default message and the given throttle time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ThrottlingQuotaExceededError_with_default_message(
    throttle_time_ms: i32,
) -> *mut kafka_common_ThrottlingQuotaExceededError_t {
    Payload::boxed(ThrottlingQuotaExceededError::with_default_message(throttle_time_ms))
}

/// `throttleTimeMs()`.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid throttling-quota-exceeded handle:
    // a view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). The borrow lasts for this call only: the
    // scalar is copied out.
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }
        .value()
        .throttle_time_ms()
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_message(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid throttling-quota-exceeded handle:
    // a view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `message_ptr` points at the payload's own
    // C string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_source(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid throttling-quota-exceeded handle:
    // a view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_to_string(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid throttling-quota-exceeded handle:
    // a view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). The borrow lasts for this call only:
    // `to_c_string` copies the text into an owned string.
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_destroy(
    self_: *mut kafka_common_ThrottlingQuotaExceededError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ThrottlingQuotaExceededError_t` handles
    // come only from this class's constructors, which box a
    // `Payload<ThrottlingQuotaExceededError>`, and a view is never passed here.
    unsafe { Payload::<ThrottlingQuotaExceededError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};

    #[test]
    fn throttle_time_crosses_as_is() {
        let error = box_error(Error::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(123, "m")));
        let message = CString::new("slow").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the errors, the payloads, the string) is destroyed once,
        // the view is used only while its error is alive, and `message` outlives the call that
        // reads it.
        unsafe {
            let view = kafka_common_Error_throttling_quota_exceeded_error(error);
            assert!(!view.is_null());
            assert_eq!(kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(view), 123);
            assert_eq!(
                CStr::from_ptr(kafka_common_ThrottlingQuotaExceededError_message(view))
                    .to_str()
                    .unwrap(),
                "m"
            );
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_throttling_quota_exceeded_error(other).is_null());
            kafka_common_Error_destroy(other);

            let built = kafka_common_ThrottlingQuotaExceededError_new(7, message.as_ptr());
            assert_eq!(kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(built), 7);
            assert!(kafka_common_ThrottlingQuotaExceededError_source(built).is_null());
            let s = kafka_common_ThrottlingQuotaExceededError_to_string(built);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ThrottlingQuotaExceededError::new(7, "slow").to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ThrottlingQuotaExceededError_destroy(built);
            let default = kafka_common_ThrottlingQuotaExceededError_with_default_message(9);
            assert_eq!(kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(default), 9);
            kafka_common_ThrottlingQuotaExceededError_destroy(default);
            kafka_common_ThrottlingQuotaExceededError_destroy(ptr::null_mut());
        }
    }
}
