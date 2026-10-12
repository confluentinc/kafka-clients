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
use crate::ffi::ffi_guard;

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

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_metrics_MeasurableStat_new`: the C user is responsible for the thread-safety of
// `self_`, which that function's rustdoc states, and the function pointers are trivially shareable.
// `CMeasurableStat` itself never dereferences `self_`; it only passes it back to the functions
// registered with it.
unsafe impl Send for CMeasurableStat {}
// SAFETY: `Sync` is required by the `Stat` and `Measurable` traits, the supertraits of
// `MeasurableStat`, whose implementations are shared behind an `Arc` and called through `&self`
// from any thread. A `&CMeasurableStat` exposes only the opaque `self_` and the function pointers,
// which the type never dereferences; `kafka_common_metrics_MeasurableStat_new`'s rustdoc lets them
// be called from any thread, concurrently, and the C user is responsible for the thread-safety of
// `self_` under those calls.
unsafe impl Sync for CMeasurableStat {}

impl Stat for CMeasurableStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.record` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_MeasurableStat_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle is destroyed or, once consumed, until the sensor it went
        // into is destroyed with its registry, which is after this `&self` call, and allows the
        // call to come from any thread, concurrently with others. `config` is the handle
        // `MetricConfigInner::borrowed` made from the `&MetricConfig` this method holds, valid for
        // the call, which is all the function type's rustdoc promises the C side.
        unsafe { (self.record)(self.self_, config.as_ptr(), value, time_ms) }
    }
}

impl Measurable for CMeasurableStat {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.measure` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_MeasurableStat_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle is destroyed or, once consumed, until the sensor it went
        // into is destroyed with its registry, which is after this `&self` call, and allows the
        // call to come from any thread, concurrently with others. `config` is the handle
        // `MetricConfigInner::borrowed` made from the `&MetricConfig` this method holds, valid for
        // the call, which is all the function type's rustdoc promises the C side.
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
/// `stat` must be a valid measurable-stat handle, alive for `'a`.
pub(crate) unsafe fn measurable_stat_ref<'a>(
    stat: *const kafka_common_metrics_MeasurableStat_t,
) -> &'a dyn MeasurableStat {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid measurable-stat handle alive for
    // `'a`, as `Interface::from_ptr` requires: every `kafka_common_metrics_MeasurableStat_t` is an
    // `Interface<dyn MeasurableStat>`, owned or a view; the reference returned lives for the same
    // `'a`.
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
    // SAFETY: Per this function's `# Safety`, `stat` is a valid measurable-stat handle, not used
    // again by the caller when it was owned, as `Interface::take` requires; every such handle is an
    // `Interface` of this trait object.
    Box::new(ArcMeasurableStat(unsafe {
        Interface::take(stat as *mut Interface<dyn MeasurableStat>)
    }))
}

/// Registers a C implementation of `MeasurableStat`. The caller owns `self_`
/// and keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_MeasurableStat_destroy`] or, once a `*mut`
/// parameter consumed it, until the sensor it went into is destroyed with
/// its registry. The functions may be called from any thread, concurrently,
/// so the C caller is responsible for their thread-safety on `self_`.
#[ffi_guard]
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_record(
    self_: *const kafka_common_metrics_MeasurableStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid measurable-stat handle, as
    // `measurable_stat_ref` requires, and `config` a valid metric-config handle, as
    // `metric_config_ref` requires; both references are used only within this call.
    unsafe { measurable_stat_ref(self_) }.record(unsafe { metric_config_ref(config) }, value, time_ms);
}

/// `measure(MetricConfig config, long now)`.
///
/// # Safety
///
/// `self_` must be a valid measurable-stat handle and `config` a valid
/// metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_measure(
    self_: *const kafka_common_metrics_MeasurableStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid measurable-stat handle, as
    // `measurable_stat_ref` requires, and `config` a valid metric-config handle, as
    // `metric_config_ref` requires; both references are used only within this call.
    unsafe { measurable_stat_ref(self_) }.measure(unsafe { metric_config_ref(config) }, now)
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned measurable-stat handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_destroy(
    self_: *mut kafka_common_metrics_MeasurableStat_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned measurable-stat handle
    // not yet destroyed, as `Interface::destroy` requires; a view a class handle caches is never
    // passed here.
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
        // SAFETY: Test code: `self_` is the `sum` mutex the test registered, which outlives every
        // call.
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap() += value;
    }
    unsafe extern "C" fn c_measure(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> f64 {
        // SAFETY: Test code: `self_` is the `sum` mutex the test registered, which outlives every
        // call.
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap()
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_the_invokers() {
        let sum = Mutex::new(0.0_f64);
        let handle =
            kafka_common_metrics_MeasurableStat_new(&sum as *const Mutex<f64> as *mut c_void, c_record, c_measure);
        let config = MetricConfig::new();
        // SAFETY: Test code: `handle` comes from `kafka_common_metrics_MeasurableStat_new` over the
        // `sum` local, which outlives it, and `take_measurable_stat` consumes it, after which it is
        // not used; `config_handle` comes from `kafka_common_metrics_MetricConfig_new` and is
        // destroyed once.
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
