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
//! Rust never dereferences the pointer; it releases it once, through the
//! [`kafka_common_LocalCallbackError_release_fn_t`] the binding gave, if it
//! gave one, when the last copy of the error drops.

use std::ffi::{c_char, c_void};
use std::ptr;

use crate::common::{Error, LocalCallbackError};
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::c_str_to_string;

/// Releases the opaque pointer a foreign callback error owns:
/// `kafka_common_Error_local_callback` takes one, and Rust calls it exactly
/// once, with the pointer, when the last copy of the error is dropped, on
/// whatever thread drops it (a Rust task included). `NULL` where a parameter
/// of this type is accepted means the pointer is not owned.
// a binding's foreign callback error, no Java class (DoD #7)
#[doc(alias = "rust-only")]
pub type kafka_common_LocalCallbackError_release_fn_t = Option<unsafe extern "C" fn(opaque: *mut c_void)>;

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

/// Returns the payload of a foreign callback error, or `NULL` when the error is another class.
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
pub unsafe extern "C" fn kafka_common_Error_local_callback_error(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_LocalCallbackError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a `Payload<LocalCallbackError>`
    // cloned from the variant's payload into the `ErrorInner`'s payload slot, so it lives as long
    // as the error handle: until the error, or the owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::LocalCallback(e) => inner.payload_view(e) as *const kafka_common_LocalCallbackError_t,
        _ => ptr::null(),
    }
}

/// Returns a new payload built from the foreign error's text and the binding's opaque pointer to
/// it, which the payload does not own.
///
/// # Parameters
///
/// - `message` - The foreign error's text, or `NULL` for none.
/// - `opaque` - The binding's pointer to the foreign error, handed back unchanged.
///
/// # Safety
///
/// `message` must be `NULL` or a valid NUL-terminated string. `opaque` is never
/// dereferenced. The returned handle is owned, freed with `kafka_common_LocalCallbackError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_new(
    message: *const c_char,
    opaque: *mut c_void,
) -> *mut kafka_common_LocalCallbackError_t {
    // SAFETY: Per this function's `# Safety`, `message` is null or a valid NUL-terminated string,
    // as `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
    // after this call.
    Payload::boxed(LocalCallbackError::new(unsafe { c_str_to_string(message) }, opaque))
}

/// Returns the binding's opaque pointer, exactly as it was given.
///
/// # Safety
///
/// `self_` must be a valid foreign-callback-error handle.
///
/// A returned pointer the error owns (given to `kafka_common_Error_local_callback` with a release
/// function) is valid while the error handle this view was read from lives: the view borrows from
/// that handle, and the error releases the pointer only once its last copy drops.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_opaque(
    self_: *const kafka_common_LocalCallbackError_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid foreign-callback-error handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). The borrow lasts for this call only: the
    // opaque pointer is copied out unchanged and never dereferenced here.
    unsafe { Payload::<LocalCallbackError>::from_ptr(self_) }.value().opaque()
}

/// Returns the foreign error's text.
///
/// # Safety
///
/// `self_` must be a valid foreign-callback-error handle. The returned string is borrowed, valid
/// until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_message(
    self_: *const kafka_common_LocalCallbackError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid foreign-callback-error handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `message_ptr` points at the payload's own
    // C string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<LocalCallbackError>::from_ptr(self_) }.message_ptr()
}

/// Returns the display form of the error.
///
/// # Safety
///
/// `self_` must be a valid foreign-callback-error handle. The returned string is owned, freed
/// with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_to_string(
    self_: *const kafka_common_LocalCallbackError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid foreign-callback-error handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). The borrow lasts for this call only:
    // `to_c_string` copies the text into an owned string.
    unsafe { Payload::<LocalCallbackError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here. The opaque pointer is not
/// touched.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_LocalCallbackError_destroy(self_: *mut kafka_common_LocalCallbackError_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_LocalCallbackError_t` handles come only
    // from this class's constructors, which box a `Payload<LocalCallbackError>`, and a view is
    // never passed here.
    unsafe { Payload::<LocalCallbackError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        // SAFETY: Test code: the error comes from `kafka_common_Error_local_callback` and is
        // destroyed once, after its last use; the view and the string are used only while it is
        // alive (the string is destroyed once), `message` outlives the calls that read it, and
        // `opaque` is only compared, never dereferenced.
        unsafe {
            let error = kafka_common_Error_local_callback(message.as_ptr(), opaque, None);
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
        // SAFETY: Test code: `box_error` makes the error, destroyed once after its last use; the
        // borrowed cause and the view are used only while it is alive, and `opaque` is only
        // compared, never dereferenced.
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

    /// Counts the calls in the `AtomicUsize` `opaque` points at.
    ///
    /// # Safety
    ///
    /// `opaque` must point at a live `AtomicUsize`.
    unsafe extern "C" fn count_release(opaque: *mut c_void) {
        // SAFETY: Per this function's `# Safety`, `opaque` points at a live `AtomicUsize`, only
        // read through a shared reference.
        unsafe { &*opaque.cast::<AtomicUsize>() }.fetch_add(1, Ordering::SeqCst);
    }

    /// Given a release function, the error owns the pointer: it stays
    /// readable through the error, and through the cause of the error the
    /// client wraps it in, and is released once, when the last of them is
    /// destroyed.
    #[test]
    fn owned_opaque_is_released_once_when_the_last_error_is_destroyed() {
        let releases = AtomicUsize::new(0);
        let opaque = (&releases as *const AtomicUsize).cast_mut().cast::<c_void>();
        let message = CString::new("ValueError: bad").unwrap();
        // SAFETY: Test code: the error comes from `kafka_common_Error_local_callback` and is taken
        // back by `take_error` exactly once; `box_error` makes the two wrapping handles, each
        // destroyed once after its last use; the borrowed cause and the views are used only while
        // their owner is alive; `message` outlives the call that reads it, and `releases` outlives
        // every copy of the error, so `count_release` always finds it.
        unsafe {
            let error = kafka_common_Error_local_callback(message.as_ptr(), opaque, Some(count_release));
            let view = kafka_common_Error_local_callback_error(error);
            assert_eq!(kafka_common_LocalCallbackError_opaque(view), opaque);

            let owned = crate::ffi::common::take_error(error).expect("non-null");
            let wrapped = crate::consumer::internals::ConsumerUtils::maybe_wrap_as_kafka_error_with_msg(
                owned,
                "User rebalance callback throws an error",
            );
            let first = box_error(wrapped.clone());
            let second = box_error(wrapped);
            assert_eq!(releases.load(Ordering::SeqCst), 0);

            kafka_common_Error_destroy(first);
            assert_eq!(releases.load(Ordering::SeqCst), 0);
            let view = kafka_common_Error_local_callback_error(kafka_common_Error_source(second));
            assert_eq!(kafka_common_LocalCallbackError_opaque(view), opaque);
            kafka_common_Error_destroy(second);
        }
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    /// A `NULL` release function leaves the pointer unowned: destroying the
    /// error calls nothing.
    #[test]
    fn null_release_owns_nothing() {
        let releases = AtomicUsize::new(0);
        let opaque = (&releases as *const AtomicUsize).cast_mut().cast::<c_void>();
        let message = CString::new("boom").unwrap();
        // SAFETY: Test code: the error comes from `kafka_common_Error_local_callback` and is
        // destroyed once; `message` outlives the call that reads it, and `opaque` is never
        // dereferenced without a release function.
        unsafe {
            let error = kafka_common_Error_local_callback(message.as_ptr(), opaque, None);
            kafka_common_Error_destroy(error);
        }
        assert_eq!(releases.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn owned_handle_and_other_classes() {
        let message = CString::new("owned").unwrap();
        // SAFETY: Test code: `kafka_common_LocalCallbackError_new` and `box_error` make the owned
        // handles, each destroyed once after its last use; `message` outlives the calls that read
        // it, and the null opaque pointer is never dereferenced.
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
