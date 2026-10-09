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

//! `kafka_common_Metric_t`: the `org.apache.kafka.common.Metric` interface
//! (CLAUDE.md §4, "Traits"), and `kafka_common_MetricValue_t`, the Rust-only
//! value `metricValue()` returns in place of Java's `Object`.
//!
//! A `Metric_t` is a borrowed view on a Rust [`Metric`] implementation,
//! reached through the class's `__as_Metric` (`KafkaMetric`); it has no
//! `_new` because nothing in the public API accepts a caller-supplied
//! `Metric`.
//!
//! `MetricValue` is a Rust enum whose four variants all carry data, so it
//! follows the enum rule for data variants (CLAUDE.md §4): one constructor
//! per variant returning an owned handle, plus `kafka_common_MetricValue_e`
//! and `__enum` for a `switch`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char};
use std::sync::OnceLock;

use crate::common::{Metric, MetricValue};
use crate::ffi::common::metric_name::{MetricNameInner, kafka_common_MetricName_t};
use crate::ffi::util::c_str_to_string;

/// Opaque handle to a [`Metric`] implementation.
#[repr(C)]
pub struct kafka_common_Metric_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`MetricValue`], a Rust-only type (Java returns an
/// `Object`).
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_MetricValue_t {
    _private: [u8; 0],
}

/// The variants of [`MetricValue`], for a `switch` over
/// [`kafka_common_MetricValue__enum`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_MetricValue_e {
    /// `MetricValue::Double`: a measurable or double gauge reading.
    kafka_common_MetricValue_DOUBLE,
    /// `MetricValue::String`: a string gauge reading.
    kafka_common_MetricValue_STRING,
    /// `MetricValue::Long`: a long gauge reading.
    kafka_common_MetricValue_LONG,
    /// `MetricValue::Int`: an int gauge reading.
    kafka_common_MetricValue_INT,
}

/// What a [`kafka_common_MetricValue_t`] points at: the value plus the
/// NUL-terminated copy of a string reading that `as_string` hands out.
struct MetricValueInner {
    value: MetricValue,
    string_c: Option<CString>,
}

impl MetricValueInner {
    fn new(value: MetricValue) -> Self {
        let string_c = value.as_string().map(|s| CString::new(s).unwrap_or_default());
        Self { value, string_c }
    }
}

/// Hands `value` to C as an owned handle.
pub(crate) fn box_metric_value(value: MetricValue) -> *mut kafka_common_MetricValue_t {
    Box::into_raw(Box::new(MetricValueInner::new(value))) as *mut kafka_common_MetricValue_t
}

/// The value behind a metric-value handle.
///
/// # Safety
///
/// `value` must be a valid metric-value handle.
unsafe fn metric_value_ref<'a>(value: *const kafka_common_MetricValue_t) -> &'a MetricValueInner {
    unsafe { &*(value as *const MetricValueInner) }
}

/// Takes an owned metric value back from C.
///
/// # Safety
///
/// `value` must be an owned metric-value handle not destroyed afterwards.
pub(crate) unsafe fn take_metric_value(value: *mut kafka_common_MetricValue_t) -> MetricValue {
    unsafe { Box::from_raw(value as *mut MetricValueInner) }.value
}

/// What a [`kafka_common_Metric_t`] points at: the implementation, owned by
/// the class handle that produced this view, plus the metric-name handle the
/// borrowed getter hands out.
pub(crate) struct MetricInner {
    metric: *const dyn Metric,
    name: OnceLock<MetricNameInner>,
}

impl MetricInner {
    /// A view on `metric`, whose owner must outlive the view.
    ///
    /// # Safety
    ///
    /// `metric` must point at a live implementation.
    pub(crate) unsafe fn new(metric: *const dyn Metric) -> Self {
        Self { metric, name: OnceLock::new() }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_Metric_t {
        self as *const Self as *const kafka_common_Metric_t
    }
}

impl MetricInner {
    fn metric(&self) -> &dyn Metric {
        unsafe { &*self.metric }
    }
}

// SAFETY: the pointer targets a metric owned by the same handle tree as this
// view, and a handle tree crosses threads only as a whole, under the C
// caller's synchronisation, like every other handle.
unsafe impl Send for MetricInner {}
unsafe impl Sync for MetricInner {}

/// `metricName()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Metric_metric_name(
    self_: *const kafka_common_Metric_t,
) -> *const kafka_common_MetricName_t {
    let inner = unsafe { &*(self_ as *const MetricInner) };
    inner
        .name
        .get_or_init(|| MetricNameInner::new(inner.metric().metric_name().clone()))
        .as_ptr()
}

/// `metricValue()`: the current reading, owned and freed with
/// [`kafka_common_MetricValue_destroy`].
///
/// # Safety
///
/// `self_` must be a valid metric handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Metric_metric_value(
    self_: *const kafka_common_Metric_t,
) -> *mut kafka_common_MetricValue_t {
    box_metric_value(unsafe { &*(self_ as *const MetricInner) }.metric().metric_value())
}

/// Which variant `self_` holds.
///
/// # Safety
///
/// `self_` must be a valid metric-value handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue__enum(
    self_: *const kafka_common_MetricValue_t,
) -> kafka_common_MetricValue_e {
    match unsafe { metric_value_ref(self_) }.value {
        MetricValue::Double(_) => kafka_common_MetricValue_e::kafka_common_MetricValue_DOUBLE,
        MetricValue::String(_) => kafka_common_MetricValue_e::kafka_common_MetricValue_STRING,
        MetricValue::Long(_) => kafka_common_MetricValue_e::kafka_common_MetricValue_LONG,
        MetricValue::Int(_) => kafka_common_MetricValue_e::kafka_common_MetricValue_INT,
    }
}

/// `MetricValue::Double(value)`. Owned, freed with
/// [`kafka_common_MetricValue_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_MetricValue_double(value: f64) -> *mut kafka_common_MetricValue_t {
    box_metric_value(MetricValue::Double(value))
}

/// `MetricValue::String(value)`: `value` is copied.
///
/// # Safety
///
/// `value` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue_string(value: *const c_char) -> *mut kafka_common_MetricValue_t {
    box_metric_value(MetricValue::String(unsafe { c_str_to_string(value) }))
}

/// `MetricValue::Long(value)`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_MetricValue_long(value: i64) -> *mut kafka_common_MetricValue_t {
    box_metric_value(MetricValue::Long(value))
}

/// `MetricValue::Int(value)`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_MetricValue_int(value: i32) -> *mut kafka_common_MetricValue_t {
    box_metric_value(MetricValue::Int(value))
}

/// `as_double()`: the reading when it is a double, `NaN` otherwise (a
/// string, long or int gauge). `NaN` stands for Rust's `None` because any
/// finite sentinel is a legitimate reading.
///
/// # Safety
///
/// `self_` must be a valid metric-value handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue_as_double(self_: *const kafka_common_MetricValue_t) -> f64 {
    unsafe { metric_value_ref(self_) }.value.as_double().unwrap_or(f64::NAN)
}

/// `as_string()`: the reading when it is a string gauge (borrowed, valid
/// until the handle is destroyed), `NULL` otherwise.
///
/// # Safety
///
/// `self_` must be a valid metric-value handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue_as_string(self_: *const kafka_common_MetricValue_t) -> *const c_char {
    unsafe { metric_value_ref(self_) }
        .string_c
        .as_ref()
        .map_or(std::ptr::null(), |s| s.as_ptr())
}

/// `as_long()`: the reading when it is a long gauge, `-1` otherwise. A long
/// gauge may legitimately read `-1`, so check [`kafka_common_MetricValue__enum`]
/// first.
///
/// # Safety
///
/// `self_` must be a valid metric-value handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue_as_long(self_: *const kafka_common_MetricValue_t) -> i64 {
    unsafe { metric_value_ref(self_) }.value.as_long().unwrap_or(-1)
}

/// `as_int()`: the reading when it is an int gauge, `-1` otherwise. An int
/// gauge may legitimately read `-1`, so check [`kafka_common_MetricValue__enum`]
/// first.
///
/// # Safety
///
/// `self_` must be a valid metric-value handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue_as_int(self_: *const kafka_common_MetricValue_t) -> i32 {
    unsafe { metric_value_ref(self_) }.value.as_int().unwrap_or(-1)
}

/// Frees an owned metric-value handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned metric-value handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricValue_destroy(self_: *mut kafka_common_MetricValue_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut MetricValueInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::MetricName;
    use crate::ffi::common::metric_name::kafka_common_MetricName_name;

    struct TestMetric {
        name: MetricName,
        value: MetricValue,
    }

    impl Metric for TestMetric {
        fn metric_name(&self) -> &MetricName {
            &self.name
        }

        fn metric_value(&self) -> MetricValue {
            self.value.clone()
        }
    }

    #[test]
    fn view_borrows_the_name_and_owns_the_value() {
        let metric = TestMetric {
            name: MetricName::new("n", "g", "d", BTreeMap::new()),
            value: MetricValue::Double(1.5),
        };
        let inner = unsafe { MetricInner::new(&metric as *const dyn Metric) };
        let view = inner.as_ptr();
        unsafe {
            let name = kafka_common_Metric_metric_name(view);
            assert_eq!(name, kafka_common_Metric_metric_name(view), "the name handle is cached");
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_name(name)).to_str().unwrap(), "n");
            let value = kafka_common_Metric_metric_value(view);
            assert_eq!(kafka_common_MetricValue_as_double(value), 1.5);
            kafka_common_MetricValue_destroy(value);
            kafka_common_MetricValue_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn variant_constructors_round_trip_through_enum_and_take() {
        unsafe {
            let cases: [(*mut kafka_common_MetricValue_t, kafka_common_MetricValue_e, MetricValue); 4] = [
                (
                    kafka_common_MetricValue_double(1.5),
                    kafka_common_MetricValue_e::kafka_common_MetricValue_DOUBLE,
                    MetricValue::Double(1.5),
                ),
                (
                    kafka_common_MetricValue_string(c"up".as_ptr()),
                    kafka_common_MetricValue_e::kafka_common_MetricValue_STRING,
                    MetricValue::String("up".to_string()),
                ),
                (
                    kafka_common_MetricValue_long(7),
                    kafka_common_MetricValue_e::kafka_common_MetricValue_LONG,
                    MetricValue::Long(7),
                ),
                (
                    kafka_common_MetricValue_int(-3),
                    kafka_common_MetricValue_e::kafka_common_MetricValue_INT,
                    MetricValue::Int(-3),
                ),
            ];
            for (handle, variant, expected) in cases {
                assert_eq!(kafka_common_MetricValue__enum(handle), variant);
                assert_eq!(take_metric_value(handle), expected);
            }
        }
    }

    #[test]
    fn non_double_readings_are_nan() {
        for value in [
            MetricValue::Long(3),
            MetricValue::Int(4),
            MetricValue::String("s".to_string()),
        ] {
            let metric = TestMetric { name: MetricName::new("n", "g", "d", BTreeMap::new()), value };
            let inner = unsafe { MetricInner::new(&metric as *const dyn Metric) };
            unsafe {
                let value = kafka_common_Metric_metric_value(inner.as_ptr());
                assert!(kafka_common_MetricValue_as_double(value).is_nan());
                kafka_common_MetricValue_destroy(value);
            }
        }
    }

    #[test]
    fn typed_accessors_answer_only_their_own_kind() {
        unsafe {
            let long = kafka_common_MetricValue_long(3);
            let int = kafka_common_MetricValue_int(4);
            let s = std::ffi::CString::new("s").unwrap();
            let string = kafka_common_MetricValue_string(s.as_ptr());
            let double = kafka_common_MetricValue_double(1.5);

            assert_eq!(kafka_common_MetricValue_as_long(long), 3);
            assert_eq!(kafka_common_MetricValue_as_int(int), 4);
            assert_eq!(
                std::ffi::CStr::from_ptr(kafka_common_MetricValue_as_string(string))
                    .to_str()
                    .unwrap(),
                "s"
            );

            // Every other kind reads as the documented absent value.
            for other in [int, string, double] {
                assert_eq!(kafka_common_MetricValue_as_long(other), -1);
            }
            for other in [long, string, double] {
                assert_eq!(kafka_common_MetricValue_as_int(other), -1);
            }
            for other in [long, int, double] {
                assert!(kafka_common_MetricValue_as_string(other).is_null());
            }
            for value in [long, int, string, double] {
                kafka_common_MetricValue_destroy(value);
            }
        }
    }
}
