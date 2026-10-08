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

/// The payload of a `TopicAuthorizationException` error, borrowed from the
/// error handle, or null when the error is another class. The suffix stays
/// because `kafka_common_Error_topic_authorization` is the `Error` factory.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_topic_authorization_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_TopicAuthorizationError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::TopicAuthorization(e) => inner.payload_view(e) as *const kafka_common_TopicAuthorizationError_t,
        _ => ptr::null(),
    }
}

/// `new TopicAuthorizationException(Set<String> unauthorizedTopics)`;
/// `unauthorized_topics` holds `const char *`, copied. Owned, freed with
/// [`kafka_common_TopicAuthorizationError_destroy`].
///
/// # Safety
///
/// `unauthorized_topics` must be null or a valid list of NUL-terminated
/// strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_new(
    unauthorized_topics: *const kafka_List_t,
) -> *mut kafka_common_TopicAuthorizationError_t {
    Payload::boxed(TopicAuthorizationError::new(unsafe { list_string_set(unauthorized_topics) }))
}

/// `new TopicAuthorizationException(String message)` with Java's default
/// message and no topics.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_TopicAuthorizationError_with_default_message()
-> *mut kafka_common_TopicAuthorizationError_t {
    Payload::boxed(TopicAuthorizationError::with_default_message())
}

/// `new TopicAuthorizationException(String message, Set<String>
/// unauthorizedTopics)`.
///
/// # Safety
///
/// `unauthorized_topics` must be null or a valid list of NUL-terminated
/// strings and `message` a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_with_message(
    unauthorized_topics: *const kafka_List_t,
    message: *const c_char,
) -> *mut kafka_common_TopicAuthorizationError_t {
    Payload::boxed(TopicAuthorizationError::with_message(
        unsafe { list_string_set(unauthorized_topics) },
        unsafe { c_str_to_string(message) },
    ))
}

/// `unauthorizedTopics()`: an owned, sorted list of `char *`, freed with
/// `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-authorization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_unauthorized_topics(
    self_: *const kafka_common_TopicAuthorizationError_t,
) -> *mut kafka_List_t {
    sorted_string_list(
        unsafe { Payload::<TopicAuthorizationError>::from_ptr(self_) }
            .value()
            .unauthorized_topics(),
    )
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid topic-authorization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_source(
    self_: *const kafka_common_TopicAuthorizationError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<TopicAuthorizationError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-authorization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_to_string(
    self_: *const kafka_common_TopicAuthorizationError_t,
) -> *mut c_char {
    unsafe { Payload::<TopicAuthorizationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicAuthorizationError_destroy(
    self_: *mut kafka_common_TopicAuthorizationError_t,
) {
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
