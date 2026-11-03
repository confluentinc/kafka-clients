// Copyright 2026 Confluent Inc.
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

//! Type preconditions of the C boundary (CLAUDE.md §4): a `NULL` mandatory
//! argument is a panic carrying the error Java throws there.
//!
//! The panic is typed: [`fail`] unwinds with a [`PreconditionPanic`] payload
//! through `std::panic::resume_unwind`, which skips the panic hook, so a
//! binding's ordinary error path prints nothing to stderr. The export's
//! `#[ffi_guard]` catches it and
//! [`panic_error`](crate::ffi::common::panic_error) hands the [`Error`] on
//! unchanged: a fallible function or `_err` twin stores it in `*out_err`, a
//! `_cb` function delivers it through its completion, and a
//! `confluent_kafka.h` function, which has no error channel, aborts the
//! process with it.
//!
//! The message of a bare `NULL` is `<param> must not be null`, the `NullPointerException`
//! a Java dereference throws; where Java checks the argument itself, the
//! export calls [`fail`] with Java's own error and text.
//!
//! This is the one module of `ffi` that reads a raw pointer an export
//! received without checking it first: every other read goes through these
//! helpers (`cargo xtask lint-custom`, `check-ffi-null-checks`).
//!
//! No Java counterpart: Java has no C boundary.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the exports adopt the helpers module by module")
)]

use std::ffi::{CStr, c_char, c_void};

use crate::common::Error;
use crate::ffi::util::{kafka_List_t, kafka_Map_t, list_ref, map_ref};

/// The payload of a type-precondition panic: the error the export reports.
pub(crate) struct PreconditionPanic(pub(crate) Error);

/// Panics with `error` as a type precondition of the export being run.
///
/// The panic skips the panic hook ([`std::panic::resume_unwind`]), so nothing
/// is printed for it; the export's `#[ffi_guard]` reports `error` unchanged.
pub(crate) fn fail(error: Error) -> ! {
    std::panic::resume_unwind(Box::new(PreconditionPanic(error)))
}

/// Panics with Java's `NullPointerException` for the `NULL` parameter `param`:
/// a [`LocalNullPointerError`](crate::common::LocalNullPointerError) whose
/// message is `<param> must not be null`.
pub(crate) fn null(param: &'static str) -> ! {
    fail(Error::local_null_pointer(format!("{param} must not be null")))
}

/// The value behind the handle `ptr`, of the Rust type `T` the C type `C`
/// stands for; panics with [`null`] when `ptr` is `NULL`.
///
/// # Safety
///
/// `ptr` must be `NULL` or point at a valid `T`, alive and not mutated for
/// `'a`.
pub(crate) unsafe fn require_ref<'a, C, T>(ptr: *const C, param: &'static str) -> &'a T {
    if ptr.is_null() {
        null(param)
    }
    // SAFETY: `ptr` is non-null (checked above) and, per this function's `# Safety`, points
    // at a valid `T` that stays alive and unmutated for `'a`.
    unsafe { &*(ptr as *const T) }
}

/// The value behind the handle `ptr`, borrowed mutably; panics with [`null`]
/// when `ptr` is `NULL`.
///
/// # Safety
///
/// `ptr` must be `NULL` or point at a valid `T`, alive and not otherwise
/// accessed for `'a`.
pub(crate) unsafe fn require_mut<'a, C, T>(ptr: *mut C, param: &'static str) -> &'a mut T {
    if ptr.is_null() {
        null(param)
    }
    // SAFETY: `ptr` is non-null (checked above) and, per this function's `# Safety`, points
    // at a valid `T`, alive and not otherwise accessed for `'a`.
    unsafe { &mut *(ptr as *mut T) }
}

/// Reads the mandatory C string `s` into a `String`, invalid UTF-8 replaced
/// (`String::from_utf8_lossy`); panics with [`null`] when `s` is `NULL`.
///
/// # Safety
///
/// `s` must be `NULL` or a valid NUL-terminated string.
pub(crate) unsafe fn require_str(s: *const c_char, param: &'static str) -> String {
    if s.is_null() {
        null(param)
    }
    // SAFETY: `s` is non-null (checked above) and, per this function's `# Safety`, a valid
    // NUL-terminated string. The borrow is copied into an owned `String` before return.
    unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned()
}

/// The elements of the mandatory list `list`; panics with [`null`] when it is
/// `NULL`.
///
/// # Safety
///
/// `list` must be `NULL` or a valid list handle, alive and not mutated for
/// `'a`.
pub(crate) unsafe fn require_list<'a>(list: *const kafka_List_t, param: &'static str) -> &'a [*mut c_void] {
    if list.is_null() {
        null(param)
    }
    // SAFETY: `list` is non-null (checked above) and, per this function's `# Safety`, a valid
    // list handle alive and unmutated for `'a`, as `list_ref` requires.
    unsafe { list_ref(list) }.elements()
}

/// The entries of the mandatory map `map`; panics with [`null`] when it is
/// `NULL`.
///
/// # Safety
///
/// `map` must be `NULL` or a valid map handle, alive and not mutated for `'a`.
pub(crate) unsafe fn require_map<'a>(map: *const kafka_Map_t, param: &'static str) -> &'a [(*mut c_void, *mut c_void)] {
    if map.is_null() {
        null(param)
    }
    // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a valid
    // map handle alive and unmutated for `'a`, as `map_ref` requires.
    unsafe { map_ref(map) }.entries()
}

/// The mandatory function pointer `f`; panics with [`null`] when it is `NULL`.
pub(crate) fn require_fn<F: Copy>(f: Option<F>, param: &'static str) -> F {
    match f {
        Some(f) => f,
        None => null(param),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The error a closure's precondition panic carries.
    fn caught(body: impl FnOnce() + std::panic::UnwindSafe) -> Error {
        let payload = std::panic::catch_unwind(body).expect_err("expected a precondition panic");
        match payload.downcast::<PreconditionPanic>() {
            Ok(panic) => panic.0,
            Err(_) => panic!("not a precondition panic"),
        }
    }

    #[test]
    fn test_null_is_a_null_pointer_error_naming_the_parameter() {
        let error = caught(|| null("self_"));
        assert!(error.is_local_null_pointer_error(), "{error:?}");
        assert_eq!(error.message(), "self_ must not be null");
    }

    #[test]
    fn test_fail_carries_the_error_unchanged() {
        let error = caught(|| fail(Error::local_illegal_argument("Topic cannot be null.")));
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(error.message(), "Topic cannot be null.");
    }

    #[test]
    fn test_require_helpers_panic_on_null_and_read_otherwise() {
        // SAFETY: Test code: every pointer is NULL or points at a live local of the right type.
        unsafe {
            let error = caught(|| {
                let _: &i32 = require_ref::<u8, i32>(std::ptr::null(), "self_");
            });
            assert_eq!(error.message(), "self_ must not be null");
            let value = 7i32;
            assert_eq!(*require_ref::<i32, i32>(&value, "self_"), 7);
            let mut value = 7i32;
            *require_mut::<i32, i32>(&mut value, "self_") = 8;
            assert_eq!(value, 8);
            let error = caught(|| {
                let _: &mut i32 = require_mut::<u8, i32>(std::ptr::null_mut(), "self_");
            });
            assert_eq!(error.message(), "self_ must not be null");
            let error = caught(|| {
                require_str(std::ptr::null(), "topic");
            });
            assert_eq!(error.message(), "topic must not be null");
            assert_eq!(require_str(c"t".as_ptr(), "topic"), "t");
            let error = caught(|| {
                require_list(std::ptr::null(), "partitions");
            });
            assert_eq!(error.message(), "partitions must not be null");
            let error = caught(|| {
                require_map(std::ptr::null(), "offsets");
            });
            assert_eq!(error.message(), "offsets must not be null");
        }
        let error = caught(|| {
            require_fn::<fn()>(None, "cb");
        });
        assert_eq!(error.message(), "cb must not be null");
        assert_eq!(require_fn(Some(7), "cb"), 7);
    }
}
