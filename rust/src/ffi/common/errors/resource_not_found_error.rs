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

//! `kafka_common_ResourceNotFoundError_t`:
//! `org.apache.kafka.common.errors.ResourceNotFoundException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::ResourceNotFoundError;
use crate::ffi::common::errors::{Payload, PayloadClass, take_source};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`ResourceNotFoundError`].
#[repr(C)]
pub struct kafka_common_ResourceNotFoundError_t {
    _private: [u8; 0],
}

impl PayloadClass for ResourceNotFoundError {
    fn message(&self) -> &str {
        ResourceNotFoundError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ResourceNotFoundError::source(self)
    }

    fn text(&self) -> Option<&str> {
        self.resource()
    }
}

/// The payload of a `ResourceNotFoundException` error, borrowed from the
/// error handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_resource_not_found(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ResourceNotFoundError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ResourceNotFoundError>` cloned from the variant's payload into the `ErrorInner`'s
    // payload slot, so it lives as long as the error handle: until the error, or the owner it is
    // borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ResourceNotFound(e) => inner.payload_view(e) as *const kafka_common_ResourceNotFoundError_t,
        _ => ptr::null(),
    }
}

/// `new ResourceNotFoundException(String message)`, with no resource.
/// Owned, freed with [`kafka_common_ResourceNotFoundError_destroy`].
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_new(
    message: *const c_char,
) -> *mut kafka_common_ResourceNotFoundError_t {
    // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    Payload::boxed(ResourceNotFoundError::new(unsafe { c_str_to_string(message) }))
}

/// `new ResourceNotFoundException()` with Java's default message.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ResourceNotFoundError_with_default_message() -> *mut kafka_common_ResourceNotFoundError_t
{
    Payload::boxed(ResourceNotFoundError::with_default_message())
}

/// `new ResourceNotFoundException(String resource, String message)`.
///
/// # Safety
///
/// `resource` and `message` must be valid NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_with_resource(
    resource: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_ResourceNotFoundError_t {
    Payload::boxed(ResourceNotFoundError::with_resource(
        // SAFETY: Per this function's `# Safety`, `resource` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(resource) },
        // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(message) },
    ))
}

/// `new ResourceNotFoundException(String resource, String message,
/// Throwable cause)`; `source` is consumed and must not be destroyed by the
/// caller afterwards.
///
/// # Safety
///
/// `resource` and `message` must be valid NUL-terminated strings and
/// `source` an owned error handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_with_resource_source(
    resource: *const c_char,
    message: *const c_char,
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_ResourceNotFoundError_t {
    Payload::boxed(ResourceNotFoundError::with_resource_source(
        // SAFETY: Per this function's `# Safety`, `resource` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(resource) },
        // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(message) },
        // SAFETY: Per this function's `# Safety`, `source` is null or an owned error handle not yet
        // destroyed, as `take_source` requires; the constructor consumes it, so the caller does not
        // use it again.
        unsafe { take_source(source) },
    ))
}

/// `resource()`: borrowed from the handle, or null when no resource was
/// recorded.
///
/// # Safety
///
/// `self_` must be a valid resource-not-found handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_resource(
    self_: *const kafka_common_ResourceNotFoundError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-not-found handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `text_ptr` points at the payload's own C string,
    // so the string lives as long as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { Payload::<ResourceNotFoundError>::from_ptr(self_) }.text_ptr()
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid resource-not-found handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_message(
    self_: *const kafka_common_ResourceNotFoundError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-not-found handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `message_ptr` points at the payload's own C
    // string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<ResourceNotFoundError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid resource-not-found handle.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_source(
    self_: *const kafka_common_ResourceNotFoundError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-not-found handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<ResourceNotFoundError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid resource-not-found handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_to_string(
    self_: *const kafka_common_ResourceNotFoundError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-not-found handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<ResourceNotFoundError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ResourceNotFoundError_destroy(self_: *mut kafka_common_ResourceNotFoundError_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ResourceNotFoundError_t` handles come
    // only from this class's constructors, which box a `Payload<ResourceNotFoundError>`, and a view
    // is never passed here.
    unsafe { Payload::<ResourceNotFoundError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_message};

    #[test]
    fn resource_is_nullable_and_the_source_is_consumed() {
        let error = box_error(Error::ResourceNotFound(ResourceNotFoundError::with_resource("res1", "m")));
        let res = CString::new("res2").unwrap();
        let message = CString::new("missing").unwrap();
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the errors, the payloads, the string) is destroyed once,
        // `cause` is consumed by the constructor it is passed to, each view and the borrowed cause
        // are used only while their owner is alive, and the `CString`s outlive the calls that read
        // them.
        unsafe {
            let view = kafka_common_Error_resource_not_found(error);
            assert!(!view.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_ResourceNotFoundError_resource(view))
                    .to_str()
                    .unwrap(),
                "res1"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_ResourceNotFoundError_message(view))
                    .to_str()
                    .unwrap(),
                "m"
            );
            kafka_common_Error_destroy(error);

            let no_resource = box_error(Error::ResourceNotFound(ResourceNotFoundError::new("m")));
            assert!(
                kafka_common_ResourceNotFoundError_resource(kafka_common_Error_resource_not_found(no_resource))
                    .is_null()
            );
            kafka_common_Error_destroy(no_resource);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_resource_not_found(other).is_null());
            kafka_common_Error_destroy(other);

            let cause = box_error(Error::kafka_message("cause"));
            let built = kafka_common_ResourceNotFoundError_with_resource_source(res.as_ptr(), message.as_ptr(), cause);
            let source = kafka_common_ResourceNotFoundError_source(built);
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(source)).to_str().unwrap(), "cause");
            let s = kafka_common_ResourceNotFoundError_to_string(built);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ResourceNotFoundError::with_resource_source("res2", "missing", Error::kafka_message("cause"))
                    .to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ResourceNotFoundError_destroy(built);
            kafka_common_ResourceNotFoundError_destroy(kafka_common_ResourceNotFoundError_new(message.as_ptr()));
            kafka_common_ResourceNotFoundError_destroy(kafka_common_ResourceNotFoundError_with_resource(
                res.as_ptr(),
                message.as_ptr(),
            ));
            kafka_common_ResourceNotFoundError_destroy(kafka_common_ResourceNotFoundError_with_default_message());
            kafka_common_ResourceNotFoundError_destroy(ptr::null_mut());
        }
    }
}
