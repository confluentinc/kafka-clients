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
use crate::ffi::admin::out_slot;
use crate::ffi::common::kafka_common_Error_t;
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
/// `range` must be a valid supported-version-range handle.
pub(crate) unsafe fn supported_version_range_ref<'a>(
    range: *const kafka_admin_SupportedVersionRange_t,
) -> &'a SupportedVersionRange {
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
/// destroyed.
pub(crate) unsafe fn destroy_supported_version_range_element(element: *mut c_void) {
    unsafe { kafka_admin_SupportedVersionRange_destroy(element as *mut kafka_admin_SupportedVersionRange_t) };
}

/// `new SupportedVersionRange(short minVersion, short maxVersion)`: delivers
/// the owned handle through `out_new` (freed with
/// [`kafka_admin_SupportedVersionRange_destroy`]), or returns the owned
/// `IllegalArgumentException` translation when a version is negative or the
/// maximum is below the minimum.
///
/// # Safety
///
/// `out_new` must be a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_new(
    min_version: i16,
    max_version: i16,
    out_new: *mut *mut kafka_admin_SupportedVersionRange_t,
) -> *mut kafka_common_Error_t {
    unsafe {
        out_slot(
            SupportedVersionRange::new(min_version, max_version),
            out_new,
            box_supported_version_range,
        )
    }
}

/// `minVersion()`.
///
/// # Safety
///
/// `self_` must be a valid supported-version-range handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_min_version(
    self_: *const kafka_admin_SupportedVersionRange_t,
) -> i16 {
    unsafe { supported_version_range_ref(self_) }.min_version()
}

/// `maxVersion()`.
///
/// # Safety
///
/// `self_` must be a valid supported-version-range handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_max_version(
    self_: *const kafka_admin_SupportedVersionRange_t,
) -> i16 {
    unsafe { supported_version_range_ref(self_) }.max_version()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid supported-version-range handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_to_string(
    self_: *const kafka_admin_SupportedVersionRange_t,
) -> *mut c_char {
    into_c_string(&unsafe { supported_version_range_ref(self_) }.to_string())
}

/// Frees an owned range handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned range handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_SupportedVersionRange_destroy(self_: *mut kafka_admin_SupportedVersionRange_t) {
    if !self_.is_null() {
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
        let mut range = ptr::null_mut();
        unsafe {
            let error = kafka_admin_SupportedVersionRange_new(-1, 2, &mut range);
            assert!(!error.is_null());
            assert!(error_ref(error).error.is_local_illegal_argument_error());
            kafka_common_Error_destroy(error);

            assert!(kafka_admin_SupportedVersionRange_new(1, 4, &mut range).is_null());
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
