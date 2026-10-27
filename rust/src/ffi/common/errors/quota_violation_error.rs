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

//! `kafka_common_QuotaViolationError_t`:
//! `org.apache.kafka.common.metrics.QuotaViolationException`. Like every
//! translated `Exception` it lives under the `kafka_common_` prefix whatever
//! its Java package (CLAUDE.md §4).

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::error::ErrorMessage;
use crate::common::metrics::QuotaViolationError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::metric_name::{MetricNameInner, kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;

/// Opaque handle to a [`QuotaViolationError`].
#[repr(C)]
pub struct kafka_common_QuotaViolationError_t {
    _private: [u8; 0],
}

impl PayloadClass for QuotaViolationError {
    fn message(&self) -> &str {
        ErrorMessage::message(self)
    }

    fn source(&self) -> Option<&Error> {
        QuotaViolationError::source(self)
    }
}

/// The metric-name handle the payload borrows out, built once.
fn metric_name_view(payload: &Payload<QuotaViolationError>) -> &MetricNameInner {
    payload.views(|e| MetricNameInner::new(e.metric_name().clone()))
}

/// Returns the payload of a `QuotaViolationException` error, or `NULL` when the error is another
/// class.
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
pub unsafe extern "C" fn kafka_common_Error_quota_violation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_QuotaViolationError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<QuotaViolationError>` cloned from the variant's payload into the `ErrorInner`'s
    // payload slot, so it lives as long as the error handle: until the error, or the owner it is
    // borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::QuotaViolation(e) => inner.payload_view(&**e) as *const kafka_common_QuotaViolationError_t,
        _ => ptr::null(),
    }
}

/// `new QuotaViolationException(KafkaMetric metric, double value, double
/// bound)`, taking the metric's name as the Rust translation does.
///
/// # Parameters
///
/// - `metric_name` - The name of the metric that violated its quota.
/// - `value` - The value that was recorded.
/// - `bound` - The quota bound that was crossed.
///
/// # Safety
///
/// `metric_name` must be a valid metric-name handle; it is copied. The returned handle is owned,
/// freed with `kafka_common_QuotaViolationError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_new(
    metric_name: *const kafka_common_MetricName_t,
    value: f64,
    bound: f64,
) -> *mut kafka_common_QuotaViolationError_t {
    Payload::boxed(QuotaViolationError::new(
        // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
        // `metric_name_ref` requires; it is cloned, so the borrow lasts for this call only.
        unsafe { metric_name_ref(metric_name) }.clone(),
        value,
        bound,
    ))
}

/// Returns the name of the metric that violated its quota (`metric()`'s name).
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle. The returned handle is borrowed, valid until
/// `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_metric_name(
    self_: *const kafka_common_QuotaViolationError_t,
) -> *const kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota-violation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `metric_name_view` builds the metric-name handle
    // once and caches it in the payload's `views` slot, so it lives as long as the handle, as the
    // rustdoc states.
    metric_name_view(unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }).as_ptr()
}

/// Returns the value that was recorded (`value()`).
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_value(
    self_: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota-violation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: the scalar is
    // copied out.
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.value().value()
}

/// Returns the quota bound that was crossed (`bound()`).
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_bound(
    self_: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota-violation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: the scalar is
    // copied out.
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.value().bound()
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_source(
    self_: *const kafka_common_QuotaViolationError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota-violation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_to_string(
    self_: *const kafka_common_QuotaViolationError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid quota-violation handle: a view
    // whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr` requires
    // (no call mutates a payload of this class). The borrow lasts for this call only: `to_c_string`
    // copies the text into an owned string.
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_destroy(self_: *mut kafka_common_QuotaViolationError_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_QuotaViolationError_t` handles come only
    // from this class's constructors, which box a `Payload<QuotaViolationError>`, and a view is
    // never passed here.
    unsafe { Payload::<QuotaViolationError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::CStr;

    use super::*;
    use crate::common::MetricName;
    use crate::ffi::common::metric_name::{
        box_metric_name, kafka_common_MetricName_destroy, kafka_common_MetricName_group, kafka_common_MetricName_name,
    };
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};
    use crate::ffi::util::kafka_string_destroy;

    fn quota_violation() -> QuotaViolationError {
        QuotaViolationError::new(MetricName::new("n1", "g1", "d", BTreeMap::new()), 1.0, 0.5)
    }

    #[test]
    fn view_is_borrowed_and_constructor_is_owned() {
        let error = box_error(Error::QuotaViolation(Box::new(quota_violation())));
        // SAFETY: Test code: `box_error`, `box_metric_name` and this module's constructor and
        // getters make every handle used. Each owned handle (the errors, the metric name, the
        // payload, the string) is destroyed once, and the view and the name handle it borrows out
        // are used only while the error is alive.
        unsafe {
            let view = kafka_common_Error_quota_violation(error);
            assert!(!view.is_null());
            assert_eq!(view, kafka_common_Error_quota_violation(error), "the view is cached");
            let name = kafka_common_QuotaViolationError_metric_name(view);
            assert_eq!(
                name,
                kafka_common_QuotaViolationError_metric_name(view),
                "the name handle is cached"
            );
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_name(name)).to_str().unwrap(), "n1");
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_group(name)).to_str().unwrap(), "g1");
            assert_eq!(kafka_common_QuotaViolationError_value(view), 1.0);
            assert_eq!(kafka_common_QuotaViolationError_bound(view), 0.5);
            assert!(kafka_common_QuotaViolationError_source(view).is_null());
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_quota_violation(other).is_null());
            kafka_common_Error_destroy(other);

            let name = box_metric_name(MetricName::new("n1", "g1", "d", BTreeMap::new()));
            let built = kafka_common_QuotaViolationError_new(name, 1.0, 0.5);
            kafka_common_MetricName_destroy(name);
            let s = kafka_common_QuotaViolationError_to_string(built);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), quota_violation().to_string());
            // Java interpolates `metric.metricName()`, i.e. `MetricName.toString()`.
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                "QuotaViolationError: 'MetricName [name=n1, group=g1, description=d, tags={}]' violated quota. \
                 Actual: 1, Threshold: 0.5"
            );
            kafka_string_destroy(s);
            kafka_common_QuotaViolationError_destroy(built);
            kafka_common_QuotaViolationError_destroy(ptr::null_mut());
        }
    }
}
