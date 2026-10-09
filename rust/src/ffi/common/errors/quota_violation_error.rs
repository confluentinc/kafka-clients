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

/// The payload of a `QuotaViolationException` error, borrowed from the
/// error handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_quota_violation(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_QuotaViolationError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::QuotaViolation(e) => inner.payload_view(&**e) as *const kafka_common_QuotaViolationError_t,
        _ => ptr::null(),
    }
}

/// `new QuotaViolationException(KafkaMetric metric, double value, double
/// bound)`, taking the metric's name as the Rust translation does;
/// `metric_name` is copied. Owned, freed with
/// [`kafka_common_QuotaViolationError_destroy`].
///
/// # Safety
///
/// `metric_name` must be a valid metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_new(
    metric_name: *const kafka_common_MetricName_t,
    value: f64,
    bound: f64,
) -> *mut kafka_common_QuotaViolationError_t {
    Payload::boxed(QuotaViolationError::new(
        unsafe { metric_name_ref(metric_name) }.clone(),
        value,
        bound,
    ))
}

/// `metric()`'s name: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_metric_name(
    self_: *const kafka_common_QuotaViolationError_t,
) -> *const kafka_common_MetricName_t {
    metric_name_view(unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }).as_ptr()
}

/// `value()`: the measured value.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_value(
    self_: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.value().value()
}

/// `bound()`: the quota it violated.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_bound(
    self_: *const kafka_common_QuotaViolationError_t,
) -> f64 {
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.value().bound()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_source(
    self_: *const kafka_common_QuotaViolationError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid quota-violation handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_to_string(
    self_: *const kafka_common_QuotaViolationError_t,
) -> *mut c_char {
    unsafe { Payload::<QuotaViolationError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_QuotaViolationError_destroy(self_: *mut kafka_common_QuotaViolationError_t) {
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
