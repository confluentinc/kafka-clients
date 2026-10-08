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

/// The payload of a `RetriableCommitFailedException` error, borrowed from
/// the error handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_retriable_commit_failed(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerRetriableCommitFailedError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerRetriableCommitFailed(e) => {
            inner.payload_view(e) as *const kafka_common_ConsumerRetriableCommitFailedError_t
        },
        _ => ptr::null(),
    }
}

/// `new RetriableCommitFailedException(String message)`. Owned, freed with
/// [`kafka_common_ConsumerRetriableCommitFailedError_destroy`].
///
/// # Safety
///
/// `message` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_new(
    message: *const c_char,
) -> *mut kafka_common_ConsumerRetriableCommitFailedError_t {
    Payload::boxed(ConsumerRetriableCommitFailedError::new(unsafe { c_str_to_string(message) }))
}

/// `new RetriableCommitFailedException()` with Java's default message.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_with_default_message()
-> *mut kafka_common_ConsumerRetriableCommitFailedError_t {
    Payload::boxed(ConsumerRetriableCommitFailedError::with_default_message())
}

/// `new RetriableCommitFailedException(Throwable cause)`; `source` is
/// consumed and must not be destroyed by the caller afterwards.
///
/// # Safety
///
/// `source` must be an owned error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_with_source(
    source: *mut kafka_common_Error_t,
) -> *mut kafka_common_ConsumerRetriableCommitFailedError_t {
    Payload::boxed(ConsumerRetriableCommitFailedError::with_source(unsafe { take_source(source) }))
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid retriable-commit-failed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_message(
    self_: *const kafka_common_ConsumerRetriableCommitFailedError_t,
) -> *const c_char {
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid retriable-commit-failed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_source(
    self_: *const kafka_common_ConsumerRetriableCommitFailedError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid retriable-commit-failed handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_to_string(
    self_: *const kafka_common_ConsumerRetriableCommitFailedError_t,
) -> *mut c_char {
    unsafe { Payload::<ConsumerRetriableCommitFailedError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerRetriableCommitFailedError_destroy(
    self_: *mut kafka_common_ConsumerRetriableCommitFailedError_t,
) {
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
