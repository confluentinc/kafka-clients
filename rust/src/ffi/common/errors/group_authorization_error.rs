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

//! `kafka_common_GroupAuthorizationError_t`:
//! `org.apache.kafka.common.errors.GroupAuthorizationException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::GroupAuthorizationError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`GroupAuthorizationError`].
#[repr(C)]
pub struct kafka_common_GroupAuthorizationError_t {
    _private: [u8; 0],
}

impl PayloadClass for GroupAuthorizationError {
    fn message(&self) -> &str {
        self.kafka_error().message()
    }

    fn source(&self) -> Option<&Error> {
        GroupAuthorizationError::source(self)
    }

    fn text(&self) -> Option<&str> {
        Some(self.group_id())
    }
}

/// The payload of a `GroupAuthorizationException` error, borrowed from the
/// error handle, or null when the error is another class. The suffix stays
/// because `kafka_common_Error_group_authorization` is the `Error` factory.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_group_authorization_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_GroupAuthorizationError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::GroupAuthorization(e) => inner.payload_view(e) as *const kafka_common_GroupAuthorizationError_t,
        _ => ptr::null(),
    }
}

/// `GroupAuthorizationException.forGroupId(String groupId)`: Java's default
/// message for the group. Owned, freed with
/// [`kafka_common_GroupAuthorizationError_destroy`].
///
/// # Safety
///
/// `group_id` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_for_group_id(
    group_id: *const c_char,
) -> *mut kafka_common_GroupAuthorizationError_t {
    Payload::boxed(GroupAuthorizationError::for_group_id(unsafe { c_str_to_string(group_id) }))
}

/// `new GroupAuthorizationException(String groupId, String message)`.
///
/// # Safety
///
/// `group_id` and `message` must be valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_new(
    group_id: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_GroupAuthorizationError_t {
    Payload::boxed(GroupAuthorizationError::new(unsafe { c_str_to_string(group_id) }, unsafe {
        c_str_to_string(message)
    }))
}

/// `new GroupAuthorizationException(String message)` with Java's default
/// message and no group id.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupAuthorizationError_with_default_message()
-> *mut kafka_common_GroupAuthorizationError_t {
    Payload::boxed(GroupAuthorizationError::with_default_message())
}

/// `groupId()`: borrowed from the handle, valid until it is destroyed.
///
/// # Safety
///
/// `self_` must be a valid group-authorization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_group_id(
    self_: *const kafka_common_GroupAuthorizationError_t,
) -> *const c_char {
    unsafe { Payload::<GroupAuthorizationError>::from_ptr(self_) }.text_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid group-authorization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_source(
    self_: *const kafka_common_GroupAuthorizationError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<GroupAuthorizationError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid group-authorization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_to_string(
    self_: *const kafka_common_GroupAuthorizationError_t,
) -> *mut c_char {
    unsafe { Payload::<GroupAuthorizationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_destroy(
    self_: *mut kafka_common_GroupAuthorizationError_t,
) {
    unsafe { Payload::<GroupAuthorizationError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};

    #[test]
    fn group_id_is_borrowed_from_the_handle() {
        let error = box_error(Error::GroupAuthorization(GroupAuthorizationError::for_group_id("g1")));
        let g2 = CString::new("g2").unwrap();
        let message = CString::new("m").unwrap();
        unsafe {
            let view = kafka_common_Error_group_authorization_error(error);
            assert!(!view.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_GroupAuthorizationError_group_id(view))
                    .to_str()
                    .unwrap(),
                "g1"
            );
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_group_authorization_error(other).is_null());
            kafka_common_Error_destroy(other);

            let built = kafka_common_GroupAuthorizationError_new(g2.as_ptr(), message.as_ptr());
            assert_eq!(
                CStr::from_ptr(kafka_common_GroupAuthorizationError_group_id(built))
                    .to_str()
                    .unwrap(),
                "g2"
            );
            assert!(kafka_common_GroupAuthorizationError_source(built).is_null());
            kafka_common_GroupAuthorizationError_destroy(built);
            let for_group = kafka_common_GroupAuthorizationError_for_group_id(g2.as_ptr());
            let s = kafka_common_GroupAuthorizationError_to_string(for_group);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                GroupAuthorizationError::for_group_id("g2").to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_GroupAuthorizationError_destroy(for_group);
            kafka_common_GroupAuthorizationError_destroy(kafka_common_GroupAuthorizationError_with_default_message());
            kafka_common_GroupAuthorizationError_destroy(ptr::null_mut());
        }
    }
}
