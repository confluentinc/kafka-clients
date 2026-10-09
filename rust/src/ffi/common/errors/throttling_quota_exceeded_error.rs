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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_throttling_quota_exceeded_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ThrottlingQuotaExceededError_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_new(
    throttle_time_ms: i32,
    message: *const c_char,
) -> *mut kafka_common_ThrottlingQuotaExceededError_t {
    Payload::boxed(ThrottlingQuotaExceededError::new(throttle_time_ms, unsafe {
        c_str_to_string(message)
    }))
}

/// `new ThrottlingQuotaExceededException(String message)` with Java's
/// default message and the given throttle time.
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_throttle_time_ms(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> i32 {
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }
        .value()
        .throttle_time_ms()
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_message(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> *const c_char {
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_source(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid throttling-quota-exceeded handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_to_string(
    self_: *const kafka_common_ThrottlingQuotaExceededError_t,
) -> *mut c_char {
    unsafe { Payload::<ThrottlingQuotaExceededError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ThrottlingQuotaExceededError_destroy(
    self_: *mut kafka_common_ThrottlingQuotaExceededError_t,
) {
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
