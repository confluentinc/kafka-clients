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

//! `kafka_admin_SupportedVersionRange_t`:
//! `org.apache.kafka.clients.admin.SupportedVersionRange` (CLAUDE.md §4).

use std::ffi::{c_char, c_void};

use crate::admin::SupportedVersionRange;
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`SupportedVersionRange`].
#[repr(C)]
pub struct kafka_admin_SupportedVersionRange_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_SupportedVersionRange_t`] points at.
pub(crate) struct SupportedVersionRangeInner {
    range: SupportedVersionRange,
}

/// The range behind a handle.
///
/// # Safety
///
/// `range` must be a valid supported-version-range handle, kept alive for
/// `'a`.
pub(crate) unsafe fn supported_version_range_ref<'a>(
    range: *const kafka_admin_SupportedVersionRange_t,
) -> &'a SupportedVersionRange {
    // SAFETY: Per this function's `# Safety`, `range` is a valid supported-version-range handle
    // kept alive for `'a`. Every `kafka_admin_SupportedVersionRange_t` comes from
    // `box_supported_version_range`, which boxes a `SupportedVersionRangeInner`, so the cast yields
    // a valid shared reference. The type has no setters, so nothing mutates it while borrowed.
    &unsafe { &*(range as *const SupportedVersionRangeInner) }.range
}

/// Hands `range` to C as an owned handle, freed with
/// [`kafka_admin_SupportedVersionRange_destroy`].
pub(crate) fn box_supported_version_range(range: SupportedVersionRange) -> *mut kafka_admin_SupportedVersionRange_t {
    Box::into_raw(Box::new(SupportedVersionRangeInner { range })) as *mut kafka_admin_SupportedVersionRange_t
}

/// Frees a `kafka_admin_SupportedVersionRange_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned supported-version-range handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_supported_version_range_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned supported-version-range handle
    // not yet destroyed, that no other call uses at the same time and that is not used afterwards,
    // as `kafka_admin_SupportedVersionRange_destroy` requires.
    unsafe { kafka_admin_SupportedVersionRange_destroy(element as *mut kafka_admin_SupportedVersionRange_t) };
}

/// Raises an error unless the following conditions are met: `0 <= min_version <= max_version`.
///
/// # Parameters
///
/// - `min_version` - The minimum version value.
/// - `max_version` - The maximum version value.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` Raised when the condition described above
///   is not met.
///
/// # Safety
///
/// `out_err` must be a valid slot pointing at `NULL`. The returned handle is owned, freed with
/// [`kafka_admin_SupportedVersionRange_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_new(
    min_version: i16,
    max_version: i16,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_SupportedVersionRange_t {
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe {
        deliver_value(
            SupportedVersionRange::new(min_version, max_version),
            out_err,
            box_supported_version_range,
        )
    }
}

/// Returns the minimum version value.
///
/// # Safety
///
/// `self_` must be a valid supported-version-range handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_min_version(
    self_: *const kafka_admin_SupportedVersionRange_t,
) -> i16 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid supported-version-range handle,
    // kept alive for this call, as `supported_version_range_ref` requires.
    unsafe { supported_version_range_ref(self_) }.min_version()
}

/// Returns the maximum version value.
///
/// # Safety
///
/// `self_` must be a valid supported-version-range handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_max_version(
    self_: *const kafka_admin_SupportedVersionRange_t,
) -> i16 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid supported-version-range handle,
    // kept alive for this call, as `supported_version_range_ref` requires.
    unsafe { supported_version_range_ref(self_) }.max_version()
}

/// Returns the text form of the range.
///
/// # Safety
///
/// `self_` must be a valid supported-version-range handle. The returned string is owned, freed
/// with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_to_string(
    self_: *const kafka_admin_SupportedVersionRange_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid supported-version-range handle,
    // kept alive for this call, as `supported_version_range_ref` requires.
    into_c_string(&unsafe { supported_version_range_ref(self_) }.to_string())
}

/// Frees an owned range handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned range handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_destroy(self_: *mut kafka_admin_SupportedVersionRange_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null here, and per this function's `# Safety` it is an owned range
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every supported-version-range handle comes from
        // `box_supported_version_range`, which boxes a `SupportedVersionRangeInner`, so
        // `Box::from_raw` reclaims that box.
        drop(unsafe { Box::from_raw(self_ as *mut SupportedVersionRangeInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn new_validates_like_java() {
        let mut error = ptr::null_mut();
        // SAFETY: Test code: `error` is a valid slot pointing at null before each call; the
        // failing call returns null and stores the owned error, destroyed once and the slot reset;
        // `range` and `s` are live handles, each destroyed once after its last use, and destroying
        // null is a no-op.
        unsafe {
            let failed = kafka_admin_SupportedVersionRange_new(-1, 2, &mut error);
            assert!(failed.is_null());
            assert!(!error.is_null());
            assert!(error_ref(error).error.is_local_illegal_argument_error());
            kafka_common_Error_destroy(error);
            error = ptr::null_mut();

            let range = kafka_admin_SupportedVersionRange_new(1, 4, &mut error);
            assert!(error.is_null());
            let expected = SupportedVersionRange::new(1, 4).unwrap();
            assert_eq!(*supported_version_range_ref(range), expected);
            assert_eq!(kafka_admin_SupportedVersionRange_min_version(range), 1);
            assert_eq!(kafka_admin_SupportedVersionRange_max_version(range), 4);
            let s = kafka_admin_SupportedVersionRange_to_string(range);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_admin_SupportedVersionRange_destroy(range);
            kafka_admin_SupportedVersionRange_destroy(ptr::null_mut());
        }
    }
}
