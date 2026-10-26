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
use crate::ffi::ffi_guard;
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

/// Returns the payload of a `GroupAuthorizationException` error, or `NULL` when the error is
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
pub unsafe extern "C" fn kafka_common_Error_group_authorization_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_GroupAuthorizationError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<GroupAuthorizationError>` cloned from the variant's payload into the `ErrorInner`'s
    // payload slot, so it lives as long as the error handle: until the error, or the owner it is
    // borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::GroupAuthorization(e) => inner.payload_view(e) as *const kafka_common_GroupAuthorizationError_t,
        _ => ptr::null(),
    }
}

/// Returns a `GroupAuthorizationException.forGroupId(String groupId)` error: Java's default
/// message for the group.
///
/// # Parameters
///
/// - `group_id` - The group ID that failed authorization.
///
/// # Safety
///
/// `group_id` must be a valid NUL-terminated string. The returned handle is owned, freed with
/// `kafka_common_GroupAuthorizationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_for_group_id(
    group_id: *const c_char,
) -> *mut kafka_common_GroupAuthorizationError_t {
    // SAFETY: Per this function's `# Safety`, `group_id` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    Payload::boxed(GroupAuthorizationError::for_group_id(unsafe { c_str_to_string(group_id) }))
}

/// `new GroupAuthorizationException(String message, String groupId)`.
///
/// # Parameters
///
/// - `group_id` - The group ID that failed authorization.
/// - `message` - The detail message.
///
/// # Safety
///
/// `group_id` and `message` must be valid NUL-terminated strings. The returned handle is owned,
/// freed with `kafka_common_GroupAuthorizationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_new(
    group_id: *const c_char,
    message: *const c_char,
) -> *mut kafka_common_GroupAuthorizationError_t {
    // SAFETY: Per this function's `# Safety`, `group_id` and `message` are each a valid
    // NUL-terminated string, as `c_str_to_string` requires; both are copied into owned `String`s,
    // so nothing borrows them after this call.
    Payload::boxed(GroupAuthorizationError::new(unsafe { c_str_to_string(group_id) }, unsafe {
        c_str_to_string(message)
    }))
}

/// `new GroupAuthorizationException(String message)` with Java's default
/// message and no group id.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_GroupAuthorizationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_GroupAuthorizationError_with_default_message()
-> *mut kafka_common_GroupAuthorizationError_t {
    Payload::boxed(GroupAuthorizationError::with_default_message())
}

/// Return the group ID that failed authorization. May be `NULL` if it is not known
/// in the context the error was raised in.
///
/// # Returns
///
/// nullable groupId
///
/// # Translation notes
///
/// WARNING: returns an empty string, not `NULL`, when unknown.
///
/// # Safety
///
/// `self_` must be a valid group-authorization handle. The returned string is borrowed, valid
/// until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_group_id(
    self_: *const kafka_common_GroupAuthorizationError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid group-authorization handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `text_ptr` points at the payload's own C string,
    // so the string lives as long as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { Payload::<GroupAuthorizationError>::from_ptr(self_) }.text_ptr()
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid group-authorization handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_source(
    self_: *const kafka_common_GroupAuthorizationError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid group-authorization handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<GroupAuthorizationError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid group-authorization handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_to_string(
    self_: *const kafka_common_GroupAuthorizationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid group-authorization handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<GroupAuthorizationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_GroupAuthorizationError_destroy(
    self_: *mut kafka_common_GroupAuthorizationError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_GroupAuthorizationError_t` handles come
    // only from this class's constructors, which box a `Payload<GroupAuthorizationError>`, and a
    // view is never passed here.
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
        // SAFETY: Test code: `box_error` and this module's constructors and getters make every
        // handle used. Each owned handle (the error, the payloads, the string) is destroyed once,
        // the view is used only while its error is alive, and the `CString`s outlive the calls that
        // read them.
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
