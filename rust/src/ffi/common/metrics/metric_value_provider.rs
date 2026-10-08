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

//! `kafka_common_metrics_MetricValueProvider_t`:
//! `org.apache.kafka.common.metrics.MetricValueProvider` (CLAUDE.md §4).
//!
//! Java's marker interface has two implementations, `Measurable` and
//! `Gauge`; the Rust enum's two data-carrying variants follow the enum rule
//! for variants with data (§4, "Enums"): each is built from its
//! implementation and returned owned. A provider is passed to the registry
//! by `const` pointer and copied: the copy shares the implementation.

#![expect(non_camel_case_types)]

use std::sync::Arc;

use crate::common::MetricValue;
use crate::common::metrics::{Gauge, Measurable, MetricValueProvider};
use crate::ffi::common::metric::{box_metric_value, kafka_common_MetricValue_t};
use crate::ffi::common::metrics::gauge::{ArcGauge, kafka_common_metrics_Gauge_t, take_gauge};
use crate::ffi::common::metrics::measurable::{ArcMeasurable, kafka_common_metrics_Measurable_t, take_measurable};
use crate::ffi::common::metrics::metric_config::{kafka_common_metrics_MetricConfig_t, metric_config_ref};

/// Opaque handle to a [`MetricValueProvider`].
#[repr(C)]
pub struct kafka_common_metrics_MetricValueProvider_t {
    _private: [u8; 0],
}

/// The variants of [`MetricValueProvider`], for a `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_metrics_MetricValueProvider_e {
    measurable,
    gauge,
}

/// What a [`kafka_common_metrics_MetricValueProvider_t`] points at: the
/// implementation, shared so the provider can be copied into the registry
/// (the Rust enum holds a `Box` and is not `Clone`).
enum MetricValueProviderInner {
    Measurable(Arc<dyn Measurable>),
    Gauge(Arc<dyn Gauge>),
}

impl MetricValueProviderInner {
    /// A fresh provider sharing the implementation.
    fn provider(&self) -> MetricValueProvider {
        match self {
            Self::Measurable(measurable) => {
                MetricValueProvider::Measurable(Box::new(ArcMeasurable(Arc::clone(measurable))))
            },
            Self::Gauge(gauge) => MetricValueProvider::Gauge(Box::new(ArcGauge(Arc::clone(gauge)))),
        }
    }
}

fn boxed(inner: MetricValueProviderInner) -> *mut kafka_common_metrics_MetricValueProvider_t {
    Box::into_raw(Box::new(inner)) as *mut kafka_common_metrics_MetricValueProvider_t
}

unsafe fn inner<'a>(self_: *const kafka_common_metrics_MetricValueProvider_t) -> &'a MetricValueProviderInner {
    unsafe { &*(self_ as *const MetricValueProviderInner) }
}

/// The provider behind a handle, as a fresh value sharing the
/// implementation.
///
/// # Safety
///
/// `provider` must be a valid metric-value-provider handle.
pub(crate) unsafe fn metric_value_provider_of(
    provider: *const kafka_common_metrics_MetricValueProvider_t,
) -> MetricValueProvider {
    unsafe { inner(provider) }.provider()
}

/// `MetricValueProvider::Measurable`: `value` is consumed (an owned handle
/// is freed, the view of a stat class or metric is shared). Owned, freed
/// with [`kafka_common_metrics_MetricValueProvider_destroy`].
///
/// # Safety
///
/// `value` must be a valid measurable handle, not used again by the caller
/// when it was owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_measurable(
    value: *mut kafka_common_metrics_Measurable_t,
) -> *mut kafka_common_metrics_MetricValueProvider_t {
    boxed(MetricValueProviderInner::Measurable(unsafe { take_measurable(value) }))
}

/// `MetricValueProvider::Gauge`: `value` is consumed.
///
/// # Safety
///
/// `value` must be a valid gauge handle, not used again by the caller when
/// it was owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_gauge(
    value: *mut kafka_common_metrics_Gauge_t,
) -> *mut kafka_common_metrics_MetricValueProvider_t {
    boxed(MetricValueProviderInner::Gauge(unsafe { take_gauge(value) }))
}

/// The variant of a provider, for a `switch`.
///
/// # Safety
///
/// `self_` must be a valid metric-value-provider handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider__enum(
    self_: *const kafka_common_metrics_MetricValueProvider_t,
) -> kafka_common_metrics_MetricValueProvider_e {
    match unsafe { inner(self_) } {
        MetricValueProviderInner::Measurable(_) => kafka_common_metrics_MetricValueProvider_e::measurable,
        MetricValueProviderInner::Gauge(_) => kafka_common_metrics_MetricValueProvider_e::gauge,
    }
}

/// `value(MetricConfig config, long now)`: the reading, a double for a
/// measurable, owned and freed with `kafka_common_MetricValue_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-value-provider handle and `config` a
/// valid metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_value(
    self_: *const kafka_common_metrics_MetricValueProvider_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> *mut kafka_common_MetricValue_t {
    let config = unsafe { metric_config_ref(config) };
    let value: MetricValue = match unsafe { inner(self_) } {
        MetricValueProviderInner::Measurable(measurable) => MetricValue::Double(measurable.measure(config, now)),
        MetricValueProviderInner::Gauge(gauge) => gauge.value(config, now),
    };
    box_metric_value(value)
}

/// Frees an owned metric-value-provider handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_destroy(
    self_: *mut kafka_common_metrics_MetricValueProvider_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut MetricValueProviderInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::ptr;

    use super::*;
    use crate::common::metrics::MetricConfig;
    use crate::ffi::common::metric::{
        kafka_common_MetricValue_as_double, kafka_common_MetricValue_destroy, kafka_common_MetricValue_string,
    };
    use crate::ffi::common::metrics::gauge::kafka_common_metrics_Gauge_new;
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_new;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };

    unsafe extern "C" fn c_measure(
        _self: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        now: i64,
    ) -> f64 {
        now as f64 / 2.0
    }
    unsafe extern "C" fn c_value(
        _self: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> *mut kafka_common_MetricValue_t {
        unsafe { kafka_common_MetricValue_string(c"up".as_ptr()) }
    }

    #[test]
    fn variants_are_built_from_their_implementation_and_copied_shared() {
        let config = kafka_common_metrics_MetricConfig_new();
        unsafe {
            let measurable = kafka_common_metrics_MetricValueProvider_measurable(kafka_common_metrics_Measurable_new(
                ptr::null_mut(),
                c_measure,
            ));
            assert_eq!(
                kafka_common_metrics_MetricValueProvider__enum(measurable),
                kafka_common_metrics_MetricValueProvider_e::measurable
            );
            let value = kafka_common_metrics_MetricValueProvider_value(measurable, config, 3);
            assert_eq!(kafka_common_MetricValue_as_double(value), 1.5);
            kafka_common_MetricValue_destroy(value);
            // The copy the registry would take shares the C implementation.
            assert_eq!(
                metric_value_provider_of(measurable).value(&MetricConfig::new(), 8),
                MetricValue::Double(4.0)
            );
            kafka_common_metrics_MetricValueProvider_destroy(measurable);

            let gauge = kafka_common_metrics_MetricValueProvider_gauge(kafka_common_metrics_Gauge_new(
                ptr::null_mut(),
                c_value,
            ));
            assert_eq!(
                kafka_common_metrics_MetricValueProvider__enum(gauge),
                kafka_common_metrics_MetricValueProvider_e::gauge
            );
            let value = kafka_common_metrics_MetricValueProvider_value(gauge, config, 0);
            assert!(kafka_common_MetricValue_as_double(value).is_nan());
            kafka_common_MetricValue_destroy(value);
            assert_eq!(
                metric_value_provider_of(gauge).value(&MetricConfig::new(), 0),
                MetricValue::String("up".to_string())
            );
            kafka_common_metrics_MetricValueProvider_destroy(gauge);
            kafka_common_metrics_MetricValueProvider_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
