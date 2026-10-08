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

//! `kafka_common_DuplicateResourceError_t`:
//! `org.apache.kafka.common.errors.DuplicateResourceException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::DuplicateResourceError;
use crate::ffi::common::errors::{Payload, PayloadClass, take_source};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`DuplicateResourceError`].
#[repr(C)]
pub struct kafka_common_DuplicateResourceError_t {
    _private: [u8; 0],
}

impl PayloadClass for DuplicateResourceError {
    fn message(&self) -> &str {
        DuplicateResourceError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        DuplicateResourceError::source(self)
    }

    fn text(&self) -> Option<&str> {
        self.resource()
    }
}

/// The payload of a `DuplicateResourceException` error, borrowed from the
/// error handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_duplicate_resource(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_DuplicateResourceError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::DuplicateResource(e) => inner.payload_view(e) as *const kafka_common_DuplicateResourceError_t,
        _ => ptr::null(),
    }
}

/// `new DuplicateResourceException(String message)`, with no resource.
/// Owned, freed with [`kafka_common_DuplicateResourceError_destroy`].
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_new(
    message: *const c_char,
) -> *mut kafka_common_DuplicateResourceError_t {
    Payload::boxed(DuplicateResourceError::new(unsafe { c_str_to_string(message) }))
}

/// `new DuplicateResourceException()` with Java's default message.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_DuplicateResourceError_with_default_message()
-> *mut kafka_common_DuplicateResourceError_t {
    Payload::boxed(DuplicateResourceError::with_default_message())
}

/// `new DuplicateResourceException(String resource, String message)`.
///
/// # Safety
///
/// `resource` and `message` must be valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_with_resource(
    resource: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_DuplicateResourceError_t {
    Payload::boxed(DuplicateResourceError::with_resource(
        unsafe { c_str_to_string(resource) },
        unsafe { c_str_to_string(message) },
    ))
}

/// `new DuplicateResourceException(String resource, String message,
/// Throwable cause)`; `source` is consumed and must not be destroyed by the
/// caller afterwards.
///
/// # Safety
///
/// `resource` and `message` must be valid NUL-terminated strings and
/// `source` an owned error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_with_resource_source(
    resource: *const c_char,
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_DuplicateResourceError_t {
    Payload::boxed(DuplicateResourceError::with_resource_source(
        unsafe { c_str_to_string(resource) },
        unsafe { c_str_to_string(message) },
        unsafe { take_source(source) },
    ))
}

/// `resource()`: borrowed from the handle, or null when no resource was
/// recorded.
///
/// # Safety
///
/// `self_` must be a valid duplicate-resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_resource(
    self_: *const kafka_common_DuplicateResourceError_t,
) -> *const c_char {
    unsafe { Payload::<DuplicateResourceError>::from_ptr(self_) }.text_ptr()
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid duplicate-resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_message(
    self_: *const kafka_common_DuplicateResourceError_t,
) -> *const c_char {
    unsafe { Payload::<DuplicateResourceError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid duplicate-resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_source(
    self_: *const kafka_common_DuplicateResourceError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<DuplicateResourceError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid duplicate-resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_to_string(
    self_: *const kafka_common_DuplicateResourceError_t,
) -> *mut c_char {
    unsafe { Payload::<DuplicateResourceError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_DuplicateResourceError_destroy(
    self_: *mut kafka_common_DuplicateResourceError_t,
) {
    unsafe { Payload::<DuplicateResourceError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_message};

    #[test]
    fn resource_is_nullable_and_the_source_is_consumed() {
        let error = box_error(Error::DuplicateResource(DuplicateResourceError::with_resource("res1", "m")));
        let res = CString::new("res2").unwrap();
        let message = CString::new("dup").unwrap();
        unsafe {
            let view = kafka_common_Error_duplicate_resource(error);
            assert!(!view.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_DuplicateResourceError_resource(view))
                    .to_str()
                    .unwrap(),
                "res1"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_DuplicateResourceError_message(view))
                    .to_str()
                    .unwrap(),
                "m"
            );
            kafka_common_Error_destroy(error);

            let no_resource = box_error(Error::DuplicateResource(DuplicateResourceError::new("m")));
            assert!(
                kafka_common_DuplicateResourceError_resource(kafka_common_Error_duplicate_resource(no_resource))
                    .is_null()
            );
            kafka_common_Error_destroy(no_resource);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_duplicate_resource(other).is_null());
            kafka_common_Error_destroy(other);

            let cause = box_error(Error::kafka_message("cause"));
            let built = kafka_common_DuplicateResourceError_with_resource_source(res.as_ptr(), message.as_ptr(), cause);
            let source = kafka_common_DuplicateResourceError_source(built);
            assert!(!source.is_null());
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(source)).to_str().unwrap(), "cause");
            assert_eq!(
                CStr::from_ptr(kafka_common_DuplicateResourceError_resource(built))
                    .to_str()
                    .unwrap(),
                "res2"
            );
            let s = kafka_common_DuplicateResourceError_to_string(built);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                DuplicateResourceError::with_resource_source("res2", "dup", Error::kafka_message("cause")).to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_DuplicateResourceError_destroy(built);
            kafka_common_DuplicateResourceError_destroy(kafka_common_DuplicateResourceError_new(message.as_ptr()));
            kafka_common_DuplicateResourceError_destroy(kafka_common_DuplicateResourceError_with_resource(
                res.as_ptr(),
                message.as_ptr(),
            ));
            kafka_common_DuplicateResourceError_destroy(kafka_common_DuplicateResourceError_with_default_message());
            kafka_common_DuplicateResourceError_destroy(ptr::null_mut());
        }
    }
}
