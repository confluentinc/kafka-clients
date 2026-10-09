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

//! `kafka_common_LocalCallbackError_t`: the error a callback written in another
//! language reports when it raised one of that language's errors. No Java
//! class; see [`LocalCallbackError`].
//!
//! A binding reports it from an interface method through
//! `<Client>__set_callback_result`, built by `kafka_common_Error_local_callback`
//! with the foreign error's text and an opaque pointer to it. The client wraps
//! it as Java wraps a listener's foreign `Throwable`, so it comes back as the
//! cause (`kafka_common_Error_source`) of the error the operation fails with;
//! `kafka_common_Error_local_callback_error` then yields this view and
//! `kafka_common_LocalCallbackError_opaque` the pointer, exactly as given.
//! Rust never dereferences, frees or retains the pointer: the binding keeps
//! what it points at alive for as long as it may compare it.

use std::ffi::{c_char, c_void};
use std::ptr;

use crate::common::{Error, LocalCallbackError};
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`LocalCallbackError`].
// a binding's foreign callback error, no Java class (DoD #7)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_LocalCallbackError_t {
    _private: [u8; 0],
}

impl PayloadClass for LocalCallbackError {
    fn message(&self) -> &str {
        LocalCallbackError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        None
    }
}

/// The payload of a foreign callback error, borrowed from the error handle, or
/// null when the error is another class. The suffix stays because
/// `kafka_common_Error_local_callback` is the `Error` factory.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_local_callback_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_LocalCallbackError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::LocalCallback(e) => inner.payload_view(e) as *const kafka_common_LocalCallbackError_t,
        _ => ptr::null(),
    }
}

/// Builds the payload from the foreign error's text and the binding's opaque
/// pointer to it. Owned, freed with [`kafka_common_LocalCallbackError_destroy`].
///
/// # Safety
///
/// `message` must be null or a valid NUL-terminated string. `opaque` is never
/// dereferenced.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_new(
    message: *const c_char,
    opaque: *mut c_void,
) -> *mut kafka_common_LocalCallbackError_t {
    Payload::boxed(LocalCallbackError::new(unsafe { c_str_to_string(message) }, opaque))
}

/// The binding's opaque pointer, exactly as it was given.
///
/// # Safety
///
/// `self_` must be a valid foreign-callback-error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_opaque(
    self_: *const kafka_common_LocalCallbackError_t,
) -> *mut c_void {
    unsafe { Payload::<LocalCallbackError>::from_ptr(self_) }.value().opaque()
}

/// The foreign error's text: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid foreign-callback-error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_message(
    self_: *const kafka_common_LocalCallbackError_t,
) -> *const c_char {
    unsafe { Payload::<LocalCallbackError>::from_ptr(self_) }.message_ptr()
}

/// The display form, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid foreign-callback-error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_to_string(
    self_: *const kafka_common_LocalCallbackError_t,
) -> *mut c_char {
    unsafe { Payload::<LocalCallbackError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here. The opaque pointer is not
/// touched.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_destroy(self_: *mut kafka_common_LocalCallbackError_t) {
    unsafe { Payload::<LocalCallbackError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::{
        box_error, kafka_common_Error_destroy, kafka_common_Error_local_callback, kafka_common_Error_message,
        kafka_common_Error_source,
    };
    use crate::ffi::error_predicates::{kafka_common_Error_is_kafka_error, kafka_common_Error_is_local_callback_error};

    #[test]
    fn view_returns_the_opaque_pointer_unchanged() {
        let mut target = 42u8;
        let opaque = (&mut target as *mut u8).cast::<c_void>();
        let message = CString::new("ValueError: bad").unwrap();
        unsafe {
            let error = kafka_common_Error_local_callback(message.as_ptr(), opaque);
            assert_eq!(kafka_common_Error_is_local_callback_error(error), 1);
            assert_eq!(kafka_common_Error_is_kafka_error(error), 0);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "ValueError: bad"
            );

            let view = kafka_common_Error_local_callback_error(error);
            assert!(!view.is_null());
            assert_eq!(kafka_common_LocalCallbackError_opaque(view), opaque);
            assert_eq!(
                CStr::from_ptr(kafka_common_LocalCallbackError_message(view)).to_str().unwrap(),
                "ValueError: bad"
            );
            let s = kafka_common_LocalCallbackError_to_string(view);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "LocalCallbackError: ValueError: bad");
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_Error_destroy(error);
        }
        assert_eq!(target, 42, "the pointee is never written");
    }

    /// The path a binding takes: the listener's error wrapped as Java wraps a
    /// foreign `Throwable`, read back through the wrapper's cause.
    #[test]
    fn wrapped_error_exposes_the_view_through_its_source() {
        let mut target = 0u8;
        let opaque = (&mut target as *mut u8).cast::<c_void>();
        let wrapped = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
            Error::local_callback("boom", opaque),
            "User rebalance callback throws an error",
        );
        let error = box_error(wrapped);
        unsafe {
            assert!(kafka_common_Error_local_callback_error(error).is_null());
            let source = kafka_common_Error_source(error);
            assert!(!source.is_null());
            let view = kafka_common_Error_local_callback_error(source);
            assert!(!view.is_null());
            assert_eq!(kafka_common_LocalCallbackError_opaque(view), opaque);
            kafka_common_Error_destroy(error);
        }
    }

    #[test]
    fn owned_handle_and_other_classes() {
        let message = CString::new("owned").unwrap();
        unsafe {
            let owned = kafka_common_LocalCallbackError_new(message.as_ptr(), ptr::null_mut());
            assert!(kafka_common_LocalCallbackError_opaque(owned).is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_LocalCallbackError_message(owned)).to_str().unwrap(),
                "owned"
            );
            kafka_common_LocalCallbackError_destroy(owned);
            kafka_common_LocalCallbackError_destroy(ptr::null_mut());

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_local_callback_error(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }
}
