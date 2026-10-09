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

//! `kafka_common_metrics_MeasurableStat_t`: the
//! `org.apache.kafka.common.metrics.MeasurableStat` interface (CLAUDE.md §4,
//! "Traits"), `Stat` and `Measurable` together, so its `_new` takes both
//! methods.
//!
//! A `MeasurableStat_t` is either a C implementation registered with
//! [`kafka_common_metrics_MeasurableStat_new`] (what `Sensor_add_with_metric_name`
//! takes) or the view a stat class hands out through `__as_MeasurableStat`.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};

/// Opaque handle to a [`MeasurableStat`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_MeasurableStat_t {
    _private: [u8; 0],
}

/// `record(MetricConfig config, double value, long timeMs)` of a C
/// implementation: `config` is borrowed for the call.
pub type kafka_common_metrics_MeasurableStat_record_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
);

/// `measure(MetricConfig config, long now)` of a C implementation: `config`
/// is borrowed for the call.
pub type kafka_common_metrics_MeasurableStat_measure_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, config: *const kafka_common_metrics_MetricConfig_t, now: i64) -> f64;

/// A C implementation of [`MeasurableStat`] registered through
/// [`kafka_common_metrics_MeasurableStat_new`].
struct CMeasurableStat {
    self_: *mut c_void,
    record: kafka_common_metrics_MeasurableStat_record_fn_t,
    measure: kafka_common_metrics_MeasurableStat_measure_fn_t,
}

// SAFETY: `self_` is what the C caller registered, whose thread-safety is
// the caller's responsibility as for every interface implementation
// (CLAUDE.md §4).
unsafe impl Send for CMeasurableStat {}
unsafe impl Sync for CMeasurableStat {}

impl Stat for CMeasurableStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        let config = MetricConfigInner::borrowed(config);
        unsafe { (self.record)(self.self_, config.as_ptr(), value, time_ms) }
    }
}

impl Measurable for CMeasurableStat {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        let config = MetricConfigInner::borrowed(config);
        unsafe { (self.measure)(self.self_, config.as_ptr(), now) }
    }
}

/// A shared [`MeasurableStat`] seen as the boxed one the Rust API takes.
pub(crate) struct ArcMeasurableStat(pub(crate) Arc<dyn MeasurableStat>);

impl Stat for ArcMeasurableStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.0.record(config, value, time_ms);
    }
}

impl Measurable for ArcMeasurableStat {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.0.measure(config, now)
    }
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `stat` must be a valid measurable-stat handle.
pub(crate) unsafe fn measurable_stat_ref<'a>(
    stat: *const kafka_common_metrics_MeasurableStat_t,
) -> &'a dyn MeasurableStat {
    unsafe { Interface::<dyn MeasurableStat>::from_ptr(stat as *const Interface<dyn MeasurableStat>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]), as the boxed stat the Rust API takes.
///
/// # Safety
///
/// `stat` must be a valid measurable-stat handle, not used again by the
/// caller when it was owned.
pub(crate) unsafe fn take_measurable_stat(stat: *mut kafka_common_metrics_MeasurableStat_t) -> Box<dyn MeasurableStat> {
    Box::new(ArcMeasurableStat(unsafe {
        Interface::take(stat as *mut Interface<dyn MeasurableStat>)
    }))
}

/// Registers a C implementation of `MeasurableStat`. The caller owns `self_`
/// and keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_MeasurableStat_destroy`] or, once a `*mut`
/// parameter consumed it, until the sensor it went into is destroyed with
/// its registry.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_MeasurableStat_new(
    self_: *mut c_void,
    record: kafka_common_metrics_MeasurableStat_record_fn_t,
    measure: kafka_common_metrics_MeasurableStat_measure_fn_t,
) -> *mut kafka_common_metrics_MeasurableStat_t {
    let stat: Arc<dyn MeasurableStat> = Arc::new(CMeasurableStat { self_, record, measure });
    Interface::owned(stat) as *mut kafka_common_metrics_MeasurableStat_t
}

/// `record(MetricConfig config, double value, long timeMs)`.
///
/// # Safety
///
/// `self_` must be a valid measurable-stat handle and `config` a valid
/// metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_record(
    self_: *const kafka_common_metrics_MeasurableStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    unsafe { measurable_stat_ref(self_) }.record(unsafe { metric_config_ref(config) }, value, time_ms);
}

/// `measure(MetricConfig config, long now)`.
///
/// # Safety
///
/// `self_` must be a valid measurable-stat handle and `config` a valid
/// metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_measure(
    self_: *const kafka_common_metrics_MeasurableStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64 {
    unsafe { measurable_stat_ref(self_) }.measure(unsafe { metric_config_ref(config) }, now)
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned measurable-stat handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_destroy(
    self_: *mut kafka_common_metrics_MeasurableStat_t,
) {
    unsafe { Interface::<dyn MeasurableStat>::destroy(self_ as *mut Interface<dyn MeasurableStat>) }
}

#[cfg(test)]
mod tests {
    use std::ptr;
    use std::sync::Mutex;

    use super::*;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };

    // A C stat summing what it records.
    unsafe extern "C" fn c_record(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        value: f64,
        _time_ms: i64,
    ) {
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap() += value;
    }
    unsafe extern "C" fn c_measure(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> f64 {
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap()
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_the_invokers() {
        let sum = Mutex::new(0.0_f64);
        let handle =
            kafka_common_metrics_MeasurableStat_new(&sum as *const Mutex<f64> as *mut c_void, c_record, c_measure);
        let config = MetricConfig::new();
        unsafe {
            measurable_stat_ref(handle).record(&config, 1.5, 0);
            let config_handle = kafka_common_metrics_MetricConfig_new();
            kafka_common_metrics_MeasurableStat_record(handle, config_handle, 2.0, 0);
            assert_eq!(kafka_common_metrics_MeasurableStat_measure(handle, config_handle, 0), 3.5);
            kafka_common_metrics_MetricConfig_destroy(config_handle);

            let boxed = take_measurable_stat(handle);
            boxed.record(&config, 0.5, 0);
            assert_eq!(boxed.measure(&config, 0), 4.0);
            kafka_common_metrics_MeasurableStat_destroy(ptr::null_mut());
        }
    }
}
