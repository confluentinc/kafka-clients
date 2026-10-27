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

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat};
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::common::metrics::{Interface, c_type_name, type_name_c_str};
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

/// The Rust-only `type_name` of a C implementation, the provider's name in
/// `KafkaMetric_to_string`: a NUL-terminated string that stays valid and
/// unchanged for the rest of the program (a string literal), or `NULL` for
/// none. A `NULL` slot is the default for a C implementation: no type name,
/// as for a closure.
pub type kafka_common_metrics_MeasurableStat_type_name_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> *const c_char>;

/// A C implementation of [`MeasurableStat`] registered through
/// `kafka_common_metrics_MeasurableStat_new`.
struct CMeasurableStat {
    self_: *mut c_void,
    record: kafka_common_metrics_MeasurableStat_record_fn_t,
    measure: kafka_common_metrics_MeasurableStat_measure_fn_t,
    type_name: kafka_common_metrics_MeasurableStat_type_name_fn_t,
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

    fn type_name(&self) -> Option<&'static str> {
        // SAFETY: `self.type_name` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_MeasurableStat_new`, whose rustdoc has the C function
        // return `NULL` or a string valid for the rest of the program and allows the call from any
        // thread, as `c_type_name` requires.
        unsafe { c_type_name(self.type_name, self.self_) }
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

    fn type_name(&self) -> Option<&'static str> {
        self.0.type_name()
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
/// `kafka_common_metrics_MeasurableStat_destroy` or, once a `*mut`
/// parameter consumed it, until the sensor it went into is destroyed with
/// its registry. The functions may be called from any thread, concurrently,
/// so the C caller is responsible for their thread-safety on `self_`.
///
/// # Parameters
///
/// - `self_` - The implementation's state, passed back to every function.
/// - `record` - Its `record(MetricConfig, double, long)`.
/// - `measure` - Its `measure(MetricConfig, long)`.
/// - `type_name` - Its type name, or `NULL` for none (see
///   `kafka_common_metrics_MeasurableStat_type_name_fn_t`).
///
/// # Returns
///
/// A measurable-stat handle.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_metrics_MeasurableStat_destroy` unless a `*mut` parameter consumed it.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_MeasurableStat_new(
    self_: *mut c_void,
    record: kafka_common_metrics_MeasurableStat_record_fn_t,
    measure: kafka_common_metrics_MeasurableStat_measure_fn_t,
    type_name: kafka_common_metrics_MeasurableStat_type_name_fn_t,
) -> *mut kafka_common_metrics_MeasurableStat_t {
    let stat: Arc<dyn MeasurableStat> = Arc::new(CMeasurableStat { self_, record, measure, type_name });
    Interface::owned(stat) as *mut kafka_common_metrics_MeasurableStat_t
}

/// `record(MetricConfig config, double value, long timeMs)`.
///
/// # Parameters
///
/// - `config` - The configuration to use for this metric
/// - `value` - The value to record
/// - `time_ms` - The POSIX time in milliseconds this value occurred
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
/// # Parameters
///
/// - `config` - The configuration for this metric
/// - `now` - The POSIX time in milliseconds the measurement is being taken
///
/// # Returns
///
/// The measured value
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

/// Returns the provider's type name as `kafka_common_metrics_KafkaMetric_to_string` reports it,
/// or `NULL` for none.
///
/// # Safety
///
/// `self_` must be a valid measurable-stat handle. The returned string is borrowed and stays valid for the
/// rest of the program.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MeasurableStat_type_name(
    self_: *const kafka_common_metrics_MeasurableStat_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid measurable-stat handle, as
    // `measurable_stat_ref` requires; the reference is used only within this call.
    type_name_c_str(unsafe { measurable_stat_ref(self_) }.type_name())
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
        let handle = kafka_common_metrics_MeasurableStat_new(
            &sum as *const Mutex<f64> as *mut c_void,
            c_record,
            c_measure,
            None,
        );
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
    // A C `type_name` slot reporting a string literal.
    unsafe extern "C" fn c_type_name_slot(_self: *mut c_void) -> *const c_char {
        c"my.CMeasurableStat".as_ptr()
    }

    #[test]
    fn type_name_is_the_c_slot_or_none_when_the_slot_is_null() {
        let named =
            kafka_common_metrics_MeasurableStat_new(ptr::null_mut(), c_record, c_measure, Some(c_type_name_slot));
        let unnamed = kafka_common_metrics_MeasurableStat_new(ptr::null_mut(), c_record, c_measure, None);
        // SAFETY: Test code: both handles come from `kafka_common_metrics_MeasurableStat_new` over a null
        // `self_` the C functions never read; the strings the invoker returns stay valid for the
        // rest of the program; `measurable_stat_ref` borrows each handle within the block, and each handle is
        // destroyed once, after its last use.
        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(kafka_common_metrics_MeasurableStat_type_name(named)).to_str(),
                Ok("my.CMeasurableStat")
            );
            assert!(kafka_common_metrics_MeasurableStat_type_name(unnamed).is_null());
            assert_eq!(measurable_stat_ref(named).type_name(), Some("my.CMeasurableStat"));
            assert_eq!(measurable_stat_ref(unnamed).type_name(), None);
            // The shared-stat adapter `take_measurable_stat` returns reports the stat it wraps, not its
            // own type.
            assert_eq!(take_measurable_stat(named).type_name(), Some("my.CMeasurableStat"));
            kafka_common_metrics_MeasurableStat_destroy(unnamed);
        }
    }
}
