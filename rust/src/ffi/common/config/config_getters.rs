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

//! The shared bodies of the client configurations' C getters
//! (`kafka_producer_ProducerConfig_get_int`,
//! `kafka_admin_AdminClientConfig_get_list`, ...), the C form of the
//! `AbstractConfig` methods each configuration forwards. No C exports of its
//! own: each export is a plain function in its class's file, calling these.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::sync::{Mutex, PoisonError};

use crate::common::Error;
use crate::ffi::common::{fail_with, kafka_common_Error_t};
use crate::ffi::util::{box_string_list, c_str_to_string, kafka_List_t};

/// The NUL-terminated copies of the strings a configuration handle lent out,
/// one per key, kept so the pointers live as long as the handle, as a Rust
/// `&str` borrowed from the configuration does. An entry is never replaced
/// or removed, and a `CString`'s bytes stay put when the map grows.
#[derive(Default)]
pub(crate) struct LentStrings(Mutex<HashMap<String, CString>>);

impl LentStrings {
    /// A pointer to the copy of `value` lent for `key`, made on the first call.
    fn lend(&self, key: String, value: &str) -> *const c_char {
        // The pointer outlives the guard: the entry stays in the map, unchanged,
        // until the handle is destroyed.
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key)
            .or_insert_with(|| {
                CString::new(value).expect("a value parsed from NUL-terminated C strings, or a constant, has no NUL")
            })
            .as_ptr()
    }
}

/// A `getShort` / `getInt` / `getLong` / `getDouble` / `getBoolean` export:
/// `1` with the value in `*out_value`, `0` for Java's `null`, `*out_value`
/// then left unwritten, or `0` with the error in `*out_err`.
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string, `out_value` valid for a
/// write and `out_err` a valid slot pointing at null.
pub(crate) unsafe fn get_scalar<T>(
    key: *const c_char,
    out_value: *mut T,
    out_err: *mut *mut kafka_common_Error_t,
    get: impl FnOnce(&str) -> Result<Option<T>, Error>,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let key = unsafe { c_str_to_string(key) };
    match get(&key) {
        Ok(Some(value)) => {
            // SAFETY: Per this function's `# Safety`, `out_value` is valid for a write.
            unsafe { out_value.write(value) };
            1
        },
        Ok(None) => 0,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// A `getString` export: the value borrowed from the handle (`strings`),
/// `NULL` for Java's `null`, or `NULL` with the error in `*out_err`.
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string and `out_err` a valid slot
/// pointing at null.
pub(crate) unsafe fn get_string<'a>(
    strings: &LentStrings,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
    get: impl FnOnce(&str) -> Result<Option<&'a str>, Error>,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let key = unsafe { c_str_to_string(key) };
    match get(&key) {
        Ok(Some(value)) => strings.lend(key, value),
        Ok(None) => std::ptr::null(),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// A `getList` export: an owned list of `char *` the caller frees with
/// `kafka_List_destroy`, `NULL` for Java's `null`, or `NULL` with the error
/// in `*out_err`.
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string and `out_err` a valid slot
/// pointing at null.
pub(crate) unsafe fn get_list<'a>(
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
    get: impl FnOnce(&str) -> Result<Option<&'a [String]>, Error>,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let key = unsafe { c_str_to_string(key) };
    match get(&key) {
        Ok(Some(values)) => box_string_list(values),
        Ok(None) => std::ptr::null_mut(),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// A `documentationOf` export: the documentation borrowed from the handle
/// (`docs`), `NULL` for a key the configuration does not define.
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string.
pub(crate) unsafe fn documentation_of<'a>(
    docs: &LentStrings,
    key: *const c_char,
    get: impl FnOnce(&str) -> Option<&'a str>,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let key = unsafe { c_str_to_string(key) };
    match get(&key) {
        Some(doc) => docs.lend(key, doc),
        None => std::ptr::null(),
    }
}

/// An `ignore` export.
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string.
pub(crate) unsafe fn ignore(key: *const c_char, ignore: impl FnOnce(&str)) {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    ignore(&unsafe { c_str_to_string(key) });
}
