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

//! `kafka_common_metrics_KafkaMetric_t`:
//! `org.apache.kafka.common.metrics.KafkaMetric` (CLAUDE.md §4).
//!
//! A metric is only ever created by its registry; the handle a registry
//! method or a reporter callback hands out shares the registered metric.
//! `KafkaMetric` implements `Metric`, reached through
//! [`kafka_common_metrics_KafkaMetric__as_Metric`] (§4, "Traits").

use std::ffi::c_void;
use std::ptr;
use std::sync::{Arc, OnceLock};

use crate::common::Metric;
use crate::common::metrics::{KafkaMetric, Measurable, MetricConfig};
use crate::ffi::common::metric::{MetricInner, kafka_common_Metric_t};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::metric_config::{
    box_metric_config, kafka_common_metrics_MetricConfig_t, metric_config_arc,
};
use crate::ffi::common::{box_error, kafka_common_Error_t};

/// Opaque handle to a [`KafkaMetric`], shared with its registry.
#[repr(C)]
pub struct kafka_common_metrics_KafkaMetric_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_metrics_KafkaMetric_t`] points at: the registry's
/// metric plus the views the borrowed getters hand out.
pub(crate) struct KafkaMetricInner {
    metric: Arc<KafkaMetric>,
    as_metric: OnceLock<MetricInner>,
    measurable: OnceLock<Interface<dyn Measurable>>,
}

impl KafkaMetricInner {
    /// A handle sharing `metric`.
    pub(crate) fn new(metric: Arc<KafkaMetric>) -> Self {
        Self { metric, as_metric: OnceLock::new(), measurable: OnceLock::new() }
    }

    /// The metric.
    pub(crate) fn metric(&self) -> &Arc<KafkaMetric> {
        &self.metric
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_metrics_KafkaMetric_t {
        self as *const Self as *const kafka_common_metrics_KafkaMetric_t
    }
}

/// `KafkaMetric.measurable()` as a shareable [`Measurable`]: the view
/// [`kafka_common_metrics_KafkaMetric_measurable`] hands out, so a `*mut`
/// parameter can share it (`Interface::take`) without owning the metric's
/// provider.
struct MetricMeasurable(Arc<KafkaMetric>);

impl Measurable for MetricMeasurable {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        // Only built for a measurable metric, so the error branch is unreachable.
        self.0
            .measurable()
            .map_or(f64::NAN, |measurable| measurable.measure(config, now))
    }
}

/// Hands `metric` to C as an owned handle sharing it, freed with
/// [`kafka_common_metrics_KafkaMetric_destroy`].
pub(crate) fn box_kafka_metric(metric: Arc<KafkaMetric>) -> *mut kafka_common_metrics_KafkaMetric_t {
    Box::into_raw(Box::new(KafkaMetricInner::new(metric))) as *mut kafka_common_metrics_KafkaMetric_t
}

unsafe fn inner<'a>(self_: *const kafka_common_metrics_KafkaMetric_t) -> &'a KafkaMetricInner {
    unsafe { &*(self_ as *const KafkaMetricInner) }
}

/// The metric behind a handle.
///
/// # Safety
///
/// `metric` must be a valid metric handle.
pub(crate) unsafe fn kafka_metric_ref<'a>(metric: *const kafka_common_metrics_KafkaMetric_t) -> &'a Arc<KafkaMetric> {
    unsafe { inner(metric) }.metric()
}

/// Frees a metric handle held as a `void *` container element.
///
/// # Safety
///
/// `element` must be an owned metric handle not yet destroyed.
pub(crate) unsafe fn destroy_kafka_metric_element(element: *mut c_void) {
    unsafe { kafka_common_metrics_KafkaMetric_destroy(element as *mut kafka_common_metrics_KafkaMetric_t) }
}

/// The metric as the `Metric` interface it implements: a borrowed view,
/// valid as long as the handle, never passed to `kafka_common_Metric_destroy`
/// (there is none).
///
/// # Safety
///
/// `self_` must be a valid metric handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_KafkaMetric__as_Metric(
    self_: *const kafka_common_metrics_KafkaMetric_t,
) -> *const kafka_common_Metric_t {
    let inner = unsafe { inner(self_) };
    inner
        .as_metric
        .get_or_init(|| unsafe { MetricInner::new(&*inner.metric as *const dyn Metric) })
        .as_ptr()
}

/// `config()`: an owned handle sharing the metric's config (setting through
/// it does not change the metric, see `MetricConfig_t`), freed with
/// `kafka_common_metrics_MetricConfig_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_KafkaMetric_config(
    self_: *const kafka_common_metrics_KafkaMetric_t,
) -> *mut kafka_common_metrics_MetricConfig_t {
    box_metric_config(unsafe { kafka_metric_ref(self_) }.config())
}

/// `config(MetricConfig config)`: the metric shares `config` from now on
/// (a borrowed handle is copied).
///
/// # Safety
///
/// `self_` must be a valid metric handle and `config` a valid metric-config
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_KafkaMetric_set_config(
    self_: *const kafka_common_metrics_KafkaMetric_t,
    config: *const kafka_common_metrics_MetricConfig_t,
) {
    unsafe { kafka_metric_ref(self_) }.set_config(unsafe { metric_config_arc(config) });
}

/// `isMeasurable()`.
///
/// # Safety
///
/// `self_` must be a valid metric handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_KafkaMetric_is_measurable(
    self_: *const kafka_common_metrics_KafkaMetric_t,
) -> i8 {
    i8::from(unsafe { kafka_metric_ref(self_) }.is_measurable())
}

/// `measurable()`: stores in `out_measurable` a view borrowed from the
/// handle (valid as long as it is, never passed to
/// `kafka_common_metrics_Measurable_destroy`), or returns the owned
/// `IllegalStateException` translation when the provider is a gauge.
///
/// # Safety
///
/// `self_` must be a valid metric handle and `out_measurable` a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_KafkaMetric_measurable(
    self_: *const kafka_common_metrics_KafkaMetric_t,
    out_measurable: *mut *const kafka_common_metrics_Measurable_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    if let Err(error) = inner.metric.measurable() {
        return box_error(error);
    }
    let view = inner.measurable.get_or_init(|| {
        let measurable: Arc<dyn Measurable> = Arc::new(MetricMeasurable(Arc::clone(&inner.metric)));
        Interface::view(measurable)
    });
    unsafe { *out_measurable = view as *const Interface<dyn Measurable> as *const kafka_common_metrics_Measurable_t };
    ptr::null_mut()
}

/// Frees a metric handle; null is a no-op. The metric stays registered.
///
/// # Safety
///
/// `self_` must be null or an owned metric handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_KafkaMetric_destroy(self_: *mut kafka_common_metrics_KafkaMetric_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut KafkaMetricInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::MetricValue;
    use crate::common::metrics::stats::Avg;
    use crate::common::metrics::{ClosureGauge, MetricValueProvider, Metrics};
    use crate::common::utils::MockTime;
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::common::metric::{
        kafka_common_Metric_metric_name, kafka_common_Metric_metric_value, kafka_common_MetricValue_as_double,
        kafka_common_MetricValue_destroy,
    };
    use crate::ffi::common::metric_name::kafka_common_MetricName_name;
    use crate::ffi::common::metrics::measurable::{kafka_common_metrics_Measurable_measure, take_measurable};
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
        kafka_common_metrics_MetricConfig_samples, kafka_common_metrics_MetricConfig_set_samples,
    };
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_state_error;

    #[test]
    fn measurable_metric_exposes_its_views_and_shares_its_config() {
        // A clock stopped at 0: `metric_value()` measures at the registry's
        // time, and a sample recorded at 0 would be purged at wall-clock time.
        let metrics = Metrics::with_time(Arc::new(
            MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 0, 0),
        ));
        let sensor = metrics.sensor("s").unwrap();
        sensor
            .add_with_metric_name(metrics.metric_name("avg", "g"), Box::new(Avg::new()))
            .unwrap();
        sensor.record_with_value_time_ms(4.0, 0);
        let handle = box_kafka_metric(metrics.metric(&metrics.metric_name("avg", "g")).unwrap());
        unsafe {
            let as_metric = kafka_common_metrics_KafkaMetric__as_Metric(handle);
            assert_eq!(
                as_metric,
                kafka_common_metrics_KafkaMetric__as_Metric(handle),
                "the view is cached"
            );
            let name = kafka_common_Metric_metric_name(as_metric);
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_name(name)).to_str().unwrap(), "avg");
            let value = kafka_common_Metric_metric_value(as_metric);
            assert_eq!(kafka_common_MetricValue_as_double(value), 4.0);
            kafka_common_MetricValue_destroy(value);

            assert_eq!(kafka_common_metrics_KafkaMetric_is_measurable(handle), 1);
            let mut measurable = ptr::null();
            assert!(kafka_common_metrics_KafkaMetric_measurable(handle, &mut measurable).is_null());
            let config = kafka_common_metrics_KafkaMetric_config(handle);
            assert_eq!(kafka_common_metrics_Measurable_measure(measurable, config, 0), 4.0);
            // The view is shared, never consumed: the metric keeps its provider.
            let shared = take_measurable(measurable as *mut _);
            assert_eq!(shared.measure(&MetricConfig::new(), 0), 4.0);
            assert_eq!(kafka_common_metrics_KafkaMetric_is_measurable(handle), 1);

            // Setting through the config handle does not change the metric;
            // `set_config` does.
            kafka_common_metrics_MetricConfig_set_samples(config, 7);
            assert_eq!(kafka_metric_ref(handle).config().samples(), MetricConfig::DEFAULT_NUM_SAMPLES);
            kafka_common_metrics_KafkaMetric_set_config(handle, config);
            assert_eq!(kafka_metric_ref(handle).config().samples(), 7);
            kafka_common_metrics_MetricConfig_destroy(config);
            let config = kafka_common_metrics_KafkaMetric_config(handle);
            assert_eq!(kafka_common_metrics_MetricConfig_samples(config), 7);
            kafka_common_metrics_MetricConfig_destroy(config);

            kafka_common_metrics_KafkaMetric_destroy(handle);
            kafka_common_metrics_KafkaMetric_destroy(ptr::null_mut());
            assert!(
                metrics.metric(&metrics.metric_name("avg", "g")).is_some(),
                "the metric stays registered"
            );
        }
    }

    #[test]
    fn a_gauge_metric_has_no_measurable() {
        let metrics = Metrics::new();
        let metric = metrics.add_metric_if_absent(
            metrics.metric_name("gauge", "g"),
            None,
            MetricValueProvider::Gauge(Box::new(ClosureGauge::new(|_: &MetricConfig, _| MetricValue::Int(1)))),
        );
        let handle = box_kafka_metric(metric);
        unsafe {
            assert_eq!(kafka_common_metrics_KafkaMetric_is_measurable(handle), 0);
            let mut measurable = ptr::null();
            let error = kafka_common_metrics_KafkaMetric_measurable(handle, &mut measurable);
            assert_eq!(kafka_common_Error_is_local_illegal_state_error(error), 1);
            assert!(measurable.is_null());
            kafka_common_Error_destroy(error);
            let config = kafka_common_metrics_MetricConfig_new();
            kafka_common_metrics_MetricConfig_destroy(config);
            kafka_common_metrics_KafkaMetric_destroy(handle);
        }
    }
}
