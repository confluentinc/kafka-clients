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

//! `kafka_admin_FinalizedVersionRange_t`:
//! `org.apache.kafka.clients.admin.FinalizedVersionRange` (CLAUDE.md §4).

use std::ffi::{c_char, c_void};

use crate::admin::FinalizedVersionRange;
use crate::ffi::admin::out_slot;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`FinalizedVersionRange`].
#[repr(C)]
pub struct kafka_admin_FinalizedVersionRange_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_FinalizedVersionRange_t`] points at.
pub(crate) struct FinalizedVersionRangeInner {
    range: FinalizedVersionRange,
}

/// The range behind a handle.
///
/// # Safety
///
/// `range` must be a valid finalized-version-range handle.
pub(crate) unsafe fn finalized_version_range_ref<'a>(
    range: *const kafka_admin_FinalizedVersionRange_t,
) -> &'a FinalizedVersionRange {
    &unsafe { &*(range as *const FinalizedVersionRangeInner) }.range
}

/// Hands `range` to C as an owned handle, freed with
/// [`kafka_admin_FinalizedVersionRange_destroy`].
pub(crate) fn box_finalized_version_range(range: FinalizedVersionRange) -> *mut kafka_admin_FinalizedVersionRange_t {
    Box::into_raw(Box::new(FinalizedVersionRangeInner { range })) as *mut kafka_admin_FinalizedVersionRange_t
}

/// Frees a `kafka_admin_FinalizedVersionRange_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned finalized-version-range handle not yet
/// destroyed.
pub(crate) unsafe fn destroy_finalized_version_range_element(element: *mut c_void) {
    unsafe { kafka_admin_FinalizedVersionRange_destroy(element as *mut kafka_admin_FinalizedVersionRange_t) };
}

/// `new FinalizedVersionRange(short minVersionLevel, short maxVersionLevel)`:
/// delivers the owned handle through `out_new` (freed with
/// [`kafka_admin_FinalizedVersionRange_destroy`]), or returns the owned
/// `IllegalArgumentException` translation when a level is negative or the
/// maximum is below the minimum.
///
/// # Safety
///
/// `out_new` must be a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FinalizedVersionRange_new(
    min_version_level: i16,
    max_version_level: i16,
    out_new: *mut *mut kafka_admin_FinalizedVersionRange_t,
) -> *mut kafka_common_Error_t {
    unsafe {
        out_slot(
            FinalizedVersionRange::new(min_version_level, max_version_level),
            out_new,
            box_finalized_version_range,
        )
    }
}

/// `minVersionLevel()`.
///
/// # Safety
///
/// `self_` must be a valid finalized-version-range handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FinalizedVersionRange_min_version_level(
    self_: *const kafka_admin_FinalizedVersionRange_t,
) -> i16 {
    unsafe { finalized_version_range_ref(self_) }.min_version_level()
}

/// `maxVersionLevel()`.
///
/// # Safety
///
/// `self_` must be a valid finalized-version-range handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FinalizedVersionRange_max_version_level(
    self_: *const kafka_admin_FinalizedVersionRange_t,
) -> i16 {
    unsafe { finalized_version_range_ref(self_) }.max_version_level()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid finalized-version-range handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FinalizedVersionRange_to_string(
    self_: *const kafka_admin_FinalizedVersionRange_t,
) -> *mut c_char {
    into_c_string(&unsafe { finalized_version_range_ref(self_) }.to_string())
}

/// Frees an owned range handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned range handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FinalizedVersionRange_destroy(self_: *mut kafka_admin_FinalizedVersionRange_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut FinalizedVersionRangeInner) });
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
            let error = kafka_admin_FinalizedVersionRange_new(3, 2, &mut range);
            assert!(!error.is_null());
            assert!(error_ref(error).error.is_local_illegal_argument_error());
            kafka_common_Error_destroy(error);

            assert!(kafka_admin_FinalizedVersionRange_new(1, 4, &mut range).is_null());
            let expected = FinalizedVersionRange::new(1, 4).unwrap();
            assert_eq!(*finalized_version_range_ref(range), expected);
            assert_eq!(kafka_admin_FinalizedVersionRange_min_version_level(range), 1);
            assert_eq!(kafka_admin_FinalizedVersionRange_max_version_level(range), 4);
            let s = kafka_admin_FinalizedVersionRange_to_string(range);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_admin_FinalizedVersionRange_destroy(range);
            kafka_admin_FinalizedVersionRange_destroy(ptr::null_mut());
        }
    }
}
