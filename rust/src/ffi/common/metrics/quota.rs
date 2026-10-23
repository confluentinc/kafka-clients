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

//! `kafka_common_metrics_Quota_t`: `org.apache.kafka.common.metrics.Quota`
//! (CLAUDE.md §4).

use std::ffi::c_char;

use crate::common::metrics::Quota;
use crate::ffi::ffi_guard;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`Quota`].
#[repr(C)]
pub struct kafka_common_metrics_Quota_t {
    _private: [u8; 0],
}

/// Hands `quota` to C as an owned handle, freed with
/// [`kafka_common_metrics_Quota_destroy`].
pub(crate) fn box_quota(quota: Quota) -> *mut kafka_common_metrics_Quota_t {
    Box::into_raw(Box::new(quota)) as *mut kafka_common_metrics_Quota_t
}

/// The quota behind a handle, borrowed.
///
/// # Safety
///
/// `quota` must be a valid quota handle, an owned one not destroyed, or a
/// view not invalidated, while the returned reference is used.
pub(crate) unsafe fn quota_of<'a>(quota: *const kafka_common_metrics_Quota_t) -> &'a Quota {
    // SAFETY: Per this function's `# Safety`, `quota` is a valid quota handle, i.e. the
    // `Box::into_raw` of a `Quota` in `box_quota`, or the view of a metric config's `Quota` that
    // `kafka_common_metrics_MetricConfig_quota` hands out, so it points at a live, initialised
    // `Quota`, borrowed for as long as the caller keeps the handle valid.
    unsafe { &*(quota as *const Quota) }
}

/// `new Quota(double bound, boolean upper)`. Owned, freed with
/// [`kafka_common_metrics_Quota_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Quota_new(bound: f64, upper: i8) -> *mut kafka_common_metrics_Quota_t {
    box_quota(Quota::new(bound, upper != 0))
}

/// `Quota.upperBound(double upperBound)`. Owned.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Quota_upper_bound(upper_bound: f64) -> *mut kafka_common_metrics_Quota_t {
    box_quota(Quota::upper_bound(upper_bound))
}

/// `Quota.lowerBound(double lowerBound)`. Owned.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Quota_lower_bound(lower_bound: f64) -> *mut kafka_common_metrics_Quota_t {
    box_quota(Quota::lower_bound(lower_bound))
}

/// `isUpperBound()`.
///
/// # Safety
///
/// `self_` must be a valid quota handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Quota_is_upper_bound(self_: *const kafka_common_metrics_Quota_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota handle, as `quota_of`
    // requires; the quota is read through a borrow that lasts for this call only.
    unsafe { quota_of(self_) }.is_upper_bound() as i8
}

/// `bound()`.
///
/// # Safety
///
/// `self_` must be a valid quota handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Quota_bound(self_: *const kafka_common_metrics_Quota_t) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota handle, as `quota_of`
    // requires; the quota is read through a borrow that lasts for this call only.
    unsafe { quota_of(self_) }.bound()
}

/// `acceptable(double value)`: whether `value` is within the quota.
///
/// # Safety
///
/// `self_` must be a valid quota handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Quota_acceptable(
    self_: *const kafka_common_metrics_Quota_t,
    value: f64,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota handle, as `quota_of`
    // requires; the quota is read through a borrow that lasts for this call only.
    unsafe { quota_of(self_) }.acceptable(value) as i8
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid quota handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Quota_to_string(
    self_: *const kafka_common_metrics_Quota_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota handle, as `quota_of`
    // requires; the quota is read through a borrow that lasts for this call only. The string is
    // copied into an owned C string.
    into_c_string(&unsafe { quota_of(self_) }.to_string())
}

/// Frees an owned quota handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Quota_destroy(self_: *mut kafka_common_metrics_Quota_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `Quota` in `box_quota`, so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut Quota) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructors_getters_and_to_string_follow_java() {
        // SAFETY: Test code: each quota handle comes from a constructor and is destroyed once,
        // after its last use; `s` is an owned string, destroyed once, after it is read.
        unsafe {
            let upper = kafka_common_metrics_Quota_upper_bound(10.0);
            assert_eq!(kafka_common_metrics_Quota_is_upper_bound(upper), 1);
            assert_eq!(kafka_common_metrics_Quota_bound(upper), 10.0);
            assert_eq!(kafka_common_metrics_Quota_acceptable(upper, 10.0), 1);
            assert_eq!(kafka_common_metrics_Quota_acceptable(upper, 10.5), 0);
            let s = kafka_common_metrics_Quota_to_string(upper);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), Quota::upper_bound(10.0).to_string());
            kafka_string_destroy(s);
            kafka_common_metrics_Quota_destroy(upper);

            let lower = kafka_common_metrics_Quota_lower_bound(2.0);
            assert_eq!(kafka_common_metrics_Quota_is_upper_bound(lower), 0);
            assert_eq!(kafka_common_metrics_Quota_acceptable(lower, 1.0), 0);
            assert_eq!(kafka_common_metrics_Quota_acceptable(lower, 2.0), 1);
            kafka_common_metrics_Quota_destroy(lower);

            let built = kafka_common_metrics_Quota_new(3.0, 0);
            assert_eq!(*quota_of(built), Quota::new(3.0, false));
            kafka_common_metrics_Quota_destroy(built);
            kafka_common_metrics_Quota_destroy(ptr::null_mut());
        }
    }
}
