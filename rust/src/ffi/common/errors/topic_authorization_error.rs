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

//! `kafka_common_TopicAuthorizationError_t`:
//! `org.apache.kafka.common.errors.TopicAuthorizationException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::TopicAuthorizationError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_List_t, list_string_set, sorted_string_list};

/// Opaque handle to a [`TopicAuthorizationError`].
#[repr(C)]
pub struct kafka_common_TopicAuthorizationError_t {
    _private: [u8; 0],
}

impl PayloadClass for TopicAuthorizationError {
    fn message(&self) -> &str {
        self.kafka_error().message()
    }

    fn source(&self) -> Option<&Error> {
        TopicAuthorizationError::source(self)
    }
}

/// Returns the payload of a `TopicAuthorizationException` error, or `NULL` when the error is
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
pub unsafe extern "C" fn kafka_common_Error_topic_authorization_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_TopicAuthorizationError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<TopicAuthorizationError>` cloned from the variant's payload into the `ErrorInner`'s
    // payload slot, so it lives as long as the error handle: until the error, or the owner it is
    // borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::TopicAuthorization(e) => inner.payload_view(e) as *const kafka_common_TopicAuthorizationError_t,
        _ => ptr::null(),
    }
}

/// `new TopicAuthorizationException(Set<String> unauthorizedTopics)`.
///
/// # Parameters
///
/// - `unauthorized_topics` - The topics not authorized, as `const char *` elements; `NULL` reads
///   as empty.
///
/// # Safety
///
/// `unauthorized_topics` must be `NULL` or a valid list of NUL-terminated
/// strings; it is copied. The returned handle is owned, freed with
/// `kafka_common_TopicAuthorizationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_new(
    unauthorized_topics: *const kafka_List_t,
) -> *mut kafka_common_TopicAuthorizationError_t {
    // SAFETY: Per this function's `# Safety`, `unauthorized_topics` is null or a valid list of
    // NUL-terminated strings, as `list_string_set` requires; the strings are copied into an owned
    // set, so nothing borrows the list after this call.
    Payload::boxed(TopicAuthorizationError::new(unsafe { list_string_set(unauthorized_topics) }))
}

/// `new TopicAuthorizationException(String message)` with Java's default
/// message and no topics.
///
/// # Safety
///
/// The returned handle is owned, freed with `kafka_common_TopicAuthorizationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_TopicAuthorizationError_with_default_message()
-> *mut kafka_common_TopicAuthorizationError_t {
    Payload::boxed(TopicAuthorizationError::with_default_message())
}

/// `new TopicAuthorizationException(String message, Set<String>
/// unauthorizedTopics)`.
///
/// # Parameters
///
/// - `unauthorized_topics` - The topics not authorized, as `const char *` elements; `NULL` reads
///   as empty.
/// - `message` - The detail message.
///
/// # Safety
///
/// `unauthorized_topics` must be `NULL` or a valid list of NUL-terminated
/// strings and `message` a valid NUL-terminated string; both are copied. The returned handle is
/// owned, freed with `kafka_common_TopicAuthorizationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_with_message(
    unauthorized_topics: *const kafka_List_t,
    message: *const c_char,
) -> *mut kafka_common_TopicAuthorizationError_t {
    Payload::boxed(TopicAuthorizationError::with_message(
        // SAFETY: Per this function's `# Safety`, `unauthorized_topics` is null or a valid list of
        // NUL-terminated strings, as `list_string_set` requires; the strings are copied into an
        // owned set, so nothing borrows the list after this call.
        unsafe { list_string_set(unauthorized_topics) },
        // SAFETY: Per this function's `# Safety`, `message` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(message) },
    ))
}

/// `unauthorizedTopics()`, as a sorted list of `char *`.
///
/// # Returns
///
/// possibly empty set of unauthorized topics
///
/// # Safety
///
/// `self_` must be a valid topic-authorization handle. The returned list is owned, freed with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_unauthorized_topics(
    self_: *const kafka_common_TopicAuthorizationError_t,
) -> *mut kafka_List_t {
    sorted_string_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-authorization handle: a
        // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
        // requires (no call mutates a payload of this class). The borrow lasts for this call only:
        // the field is copied into the owned container returned.
        unsafe { Payload::<TopicAuthorizationError>::from_ptr(self_) }
            .value()
            .unauthorized_topics(),
    )
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid topic-authorization handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_source(
    self_: *const kafka_common_TopicAuthorizationError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-authorization handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<TopicAuthorizationError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid topic-authorization handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_to_string(
    self_: *const kafka_common_TopicAuthorizationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-authorization handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<TopicAuthorizationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_destroy(
    self_: *mut kafka_common_TopicAuthorizationError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_TopicAuthorizationError_t` handles come
    // only from this class's constructors, which box a `Payload<TopicAuthorizationError>`, and a
    // view is never passed here.
    unsafe { Payload::<TopicAuthorizationError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ffi::{CStr, CString, c_void};

    use super::*;
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size};

    #[test]
    fn view_is_borrowed_and_constructors_are_owned() {
        let topics: HashSet<String> = ["t2".to_string(), "t1".to_string()].into_iter().collect();
        let error = box_error(Error::TopicAuthorization(TopicAuthorizationError::new(topics)));
        let t = CString::new("t").unwrap();
        let message = CString::new("denied").unwrap();
        // SAFETY: Test code: `box_error`, `kafka_List_new` and this module's constructors and
        // getters make every handle used. Each owned handle (the errors, the payloads, the lists,
        // the string) is destroyed once, the input list borrows `t` and is destroyed after the
        // constructor copies it, the view is used only while its error is alive, and the `CString`s
        // outlive the calls that read them.
        unsafe {
            let view = kafka_common_Error_topic_authorization_error(error);
            assert!(!view.is_null());
            // The view is cached: asking twice returns the same pointer.
            assert_eq!(view, kafka_common_Error_topic_authorization_error(error));
            let list = kafka_common_TopicAuthorizationError_unauthorized_topics(view);
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(CStr::from_ptr(kafka_List_get(list, 0) as *const c_char).to_str().unwrap(), "t1");
            assert_eq!(CStr::from_ptr(kafka_List_get(list, 1) as *const c_char).to_str().unwrap(), "t2");
            kafka_List_destroy(list);
            assert!(kafka_common_TopicAuthorizationError_source(view).is_null());
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_topic_authorization_error(other).is_null());
            kafka_common_Error_destroy(other);

            let input = kafka_List_new();
            kafka_List_add(input, t.as_ptr() as *mut c_void);
            let built = kafka_common_TopicAuthorizationError_with_message(input, message.as_ptr());
            kafka_List_destroy(input);
            let s = kafka_common_TopicAuthorizationError_to_string(built);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                TopicAuthorizationError::with_message(["t".to_string()].into_iter().collect(), "denied").to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_TopicAuthorizationError_destroy(built);
            kafka_common_TopicAuthorizationError_destroy(kafka_common_TopicAuthorizationError_with_default_message());
            kafka_common_TopicAuthorizationError_destroy(kafka_common_TopicAuthorizationError_new(ptr::null()));
            kafka_common_TopicAuthorizationError_destroy(ptr::null_mut());
        }
    }
}
