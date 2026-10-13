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

//! `kafka_common_Uuid_t`: `org.apache.kafka.common.Uuid` (CLAUDE.md §4).
//!
//! Java's `Uuid` holds two `long`s; the Rust type stores them as `u64` for
//! its bit arithmetic, so the C surface casts at the boundary and the halves
//! cross as `int64_t`, like every Java `long` (§4, "Primitive types").

use std::ffi::{c_char, c_void};
use std::ptr;

use crate::common::Uuid;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, list_elements};

/// Opaque handle to a [`Uuid`].
#[repr(C)]
pub struct kafka_common_Uuid_t {
    _private: [u8; 0],
}

/// Hands `uuid` to C as an owned handle, freed with
/// [`kafka_common_Uuid_destroy`].
pub(crate) fn box_uuid(uuid: Uuid) -> *mut kafka_common_Uuid_t {
    Box::into_raw(Box::new(uuid)) as *mut kafka_common_Uuid_t
}

/// The uuid behind a handle.
///
/// # Safety
///
/// `uuid` must be a valid uuid handle.
pub(crate) unsafe fn uuid_of(uuid: *const kafka_common_Uuid_t) -> Uuid {
    // SAFETY: Per this function's `# Safety`, `uuid` is a valid uuid handle, i.e. the
    // `Box::into_raw` of a `Uuid` in `box_uuid`, the only function that makes one, not yet
    // destroyed, so it points at a live, initialised `Uuid`, which is `Copy` and read out here.
    unsafe { *(uuid as *const Uuid) }
}

/// Hands `uuids` to C as an owned list of `kafka_common_Uuid_t *`.
pub(crate) fn uuid_list(uuids: impl IntoIterator<Item = Uuid>) -> *mut kafka_List_t {
    let elements = uuids.into_iter().map(|uuid| box_uuid(uuid) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<Uuid>))
}

/// Reads a list of `const kafka_common_Uuid_t *`; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are uuid handles.
pub(crate) unsafe fn list_uuids(list: *const kafka_List_t) -> Vec<Uuid> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle, as
    // `list_elements` requires; the slice is used only within this call, while nothing mutates the
    // list.
    unsafe { list_elements(list) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `list` is a uuid handle, as
        // `uuid_of` requires; the `Uuid` is copied out.
        .map(|&element| unsafe { uuid_of(element as *const kafka_common_Uuid_t) })
        .collect()
}

/// `new Uuid(long mostSigBits, long leastSigBits)`, as an owned handle freed
/// with [`kafka_common_Uuid_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Uuid_new(most_sig_bits: i64, least_sig_bits: i64) -> *mut kafka_common_Uuid_t {
    box_uuid(Uuid::new(most_sig_bits as u64, least_sig_bits as u64))
}

/// `Uuid.randomUuid()`: a random uuid outside the reserved range, as an
/// owned handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Uuid_random_uuid() -> *mut kafka_common_Uuid_t {
    box_uuid(Uuid::random_uuid())
}

/// `Uuid.fromString(String)`: parses the base64 form `toString()` produces
/// into `*out_from_string` (an owned handle), or returns the
/// `IllegalArgumentException` Java throws for a malformed string.
///
/// # Safety
///
/// `s` must be a valid NUL-terminated string and `out_from_string` a valid
/// pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Uuid_from_string(
    s: *const c_char,
    out_from_string: *mut *mut kafka_common_Uuid_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `s` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    match Uuid::from_string(&unsafe { c_str_to_string(s) }) {
        Ok(uuid) => {
            // SAFETY: Per this function's `# Safety`, `out_from_string` is a valid pointer, so the
            // write is sound; the caller owns the handle written, as the rustdoc states.
            unsafe { *out_from_string = box_uuid(uuid) };
            ptr::null_mut()
        },
        Err(e) => box_error(e),
    }
}

/// `getMostSignificantBits()`.
///
/// # Safety
///
/// `self_` must be a valid uuid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Uuid_most_significant_bits(self_: *const kafka_common_Uuid_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid uuid handle, as `uuid_of`
    // requires; the `Uuid` is copied out.
    unsafe { uuid_of(self_) }.most_significant_bits() as i64
}

/// `getLeastSignificantBits()`.
///
/// # Safety
///
/// `self_` must be a valid uuid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Uuid_least_significant_bits(self_: *const kafka_common_Uuid_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid uuid handle, as `uuid_of`
    // requires; the `Uuid` is copied out.
    unsafe { uuid_of(self_) }.least_significant_bits() as i64
}

/// `toString()`: the URL-safe base64 form, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid uuid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Uuid_to_string(self_: *const kafka_common_Uuid_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid uuid handle, as `uuid_of`
    // requires; the `Uuid` is copied out. and its text copied into an owned string.
    into_c_string(&unsafe { uuid_of(self_) }.to_string())
}

/// Frees an owned uuid handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned uuid handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Uuid_destroy(self_: *mut kafka_common_Uuid_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // uuid handle not yet destroyed, i.e. the `Box::into_raw` of `box_uuid`, so `Box::from_raw`
        // reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut Uuid) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_size, kafka_string_destroy};

    #[test]
    fn bits_cross_as_signed_longs() {
        // The high bit set: a `u64` above `i64::MAX` must come back as the
        // negative `long` Java would hand out.
        let uuid = kafka_common_Uuid_new(-2, 3);
        // SAFETY: Test code: `uuid` comes from `kafka_common_Uuid_new` and is destroyed once, after
        // the calls reading it.
        unsafe {
            assert_eq!(kafka_common_Uuid_most_significant_bits(uuid), -2);
            assert_eq!(kafka_common_Uuid_least_significant_bits(uuid), 3);
            assert_eq!(uuid_of(uuid), Uuid::new(u64::MAX - 1, 3));
            kafka_common_Uuid_destroy(uuid);
            kafka_common_Uuid_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn from_string_round_trips_to_string_and_reports_malformed_input() {
        let original = kafka_common_Uuid_random_uuid();
        let mut parsed: *mut kafka_common_Uuid_t = ptr::null_mut();
        // SAFETY: Test code: `original` comes from `kafka_common_Uuid_random_uuid` and `parsed`
        // from `kafka_common_Uuid_from_string`, and each is destroyed once, after the calls reading
        // it; `text` comes from `kafka_common_Uuid_to_string` and is destroyed once, after it is
        // read; `bad` outlives the call reading it, `parsed` and `out` are valid out pointers, and
        // the error is destroyed once.
        unsafe {
            let text = kafka_common_Uuid_to_string(original);
            let error = kafka_common_Uuid_from_string(text, &mut parsed);
            assert!(error.is_null());
            assert_eq!(uuid_of(parsed), uuid_of(original));
            assert_eq!(CStr::from_ptr(text).to_bytes().len(), 22);
            kafka_string_destroy(text);
            kafka_common_Uuid_destroy(parsed);
            kafka_common_Uuid_destroy(original);

            let bad = CString::new("not a uuid!").unwrap();
            let mut out: *mut kafka_common_Uuid_t = ptr::null_mut();
            let error = kafka_common_Uuid_from_string(bad.as_ptr(), &mut out);
            assert!(!error.is_null());
            assert!(out.is_null(), "the out slot is untouched on failure");
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
        }
    }

    #[test]
    fn uuid_list_owns_its_elements() {
        let ids = [Uuid::new(1, 2), Uuid::new(3, 4)];
        let list = uuid_list(ids);
        // SAFETY: Test code: `list` comes from `uuid_list`, whose elements are uuid handles, and is
        // destroyed once, after `list_uuids` reads it.
        unsafe {
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(list_uuids(list), ids.to_vec());
            kafka_List_destroy(list);
        }
    }
}
