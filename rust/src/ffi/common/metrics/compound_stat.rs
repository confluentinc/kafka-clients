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

//! `kafka_common_metrics_CompoundStat_t`: the
//! `org.apache.kafka.common.metrics.CompoundStat` interface (CLAUDE.md §4,
//! "Traits"), a `Stat` producing several named measurables, and
//! `kafka_common_metrics_CompoundStat_NamedMeasurable_t`, its nested
//! `NamedMeasurable` class (§4, "Nested types").
//!
//! A `CompoundStat_t` is either a C implementation registered with
//! [`kafka_common_metrics_CompoundStat_new`] (what `Sensor_add` takes) or
//! the view a stat class hands out through `__as_CompoundStat`
//! (`Meter__as_CompoundStat`).

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::metrics::{CompoundStat, MetricConfig, NamedMeasurable, Stat};
use crate::ffi::common::metric_name::{MetricNameInner, kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::measurable::{box_measurable, kafka_common_metrics_Measurable_t, take_measurable};
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::util::{box_list, destroy_boxed, kafka_List_destroy, kafka_List_t, list_elements};

/// Opaque handle to a [`CompoundStat`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_CompoundStat_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`NamedMeasurable`].
#[repr(C)]
pub struct kafka_common_metrics_CompoundStat_NamedMeasurable_t {
    _private: [u8; 0],
}

/// `record(MetricConfig config, double value, long timeMs)` of a C
/// implementation: `config` is borrowed for the call.
pub type kafka_common_metrics_CompoundStat_record_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
);

/// `stats()` of a C implementation: returns a list (`kafka_List_new`) of
/// owned `kafka_common_metrics_CompoundStat_NamedMeasurable_t *` elements
/// (`CompoundStat_NamedMeasurable_new`). Rust takes over the list and its
/// elements, so the implementation never frees them.
pub type kafka_common_metrics_CompoundStat_stats_fn_t = unsafe extern "C" fn(self_: *mut c_void) -> *mut kafka_List_t;

/// A C implementation of [`CompoundStat`] registered through
/// [`kafka_common_metrics_CompoundStat_new`].
struct CCompoundStat {
    self_: *mut c_void,
    record: kafka_common_metrics_CompoundStat_record_fn_t,
    stats: kafka_common_metrics_CompoundStat_stats_fn_t,
}

// SAFETY: `self_` is what the C caller registered, whose thread-safety is
// the caller's responsibility as for every interface implementation
// (CLAUDE.md §4).
unsafe impl Send for CCompoundStat {}
unsafe impl Sync for CCompoundStat {}

impl Stat for CCompoundStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        let config = MetricConfigInner::borrowed(config);
        unsafe { (self.record)(self.self_, config.as_ptr(), value, time_ms) }
    }
}

impl CompoundStat for CCompoundStat {
    fn stats(&self) -> Vec<NamedMeasurable> {
        let list = unsafe { (self.stats)(self.self_) };
        if list.is_null() {
            return Vec::new();
        }
        let stats = unsafe { list_elements(list) }
            .iter()
            .map(|&element| unsafe { Box::from_raw(element as *mut NamedMeasurableInner) }.named)
            .collect();
        unsafe { kafka_List_destroy(list) };
        stats
    }
}

/// A shared [`CompoundStat`] seen as the boxed one the Rust API takes.
pub(crate) struct ArcCompoundStat(pub(crate) Arc<dyn CompoundStat>);

impl Stat for ArcCompoundStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.0.record(config, value, time_ms);
    }
}

impl CompoundStat for ArcCompoundStat {
    fn stats(&self) -> Vec<NamedMeasurable> {
        self.0.stats()
    }
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `stat` must be a valid compound-stat handle.
pub(crate) unsafe fn compound_stat_ref<'a>(stat: *const kafka_common_metrics_CompoundStat_t) -> &'a dyn CompoundStat {
    unsafe { Interface::<dyn CompoundStat>::from_ptr(stat as *const Interface<dyn CompoundStat>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]), as the boxed stat the Rust API takes.
///
/// # Safety
///
/// `stat` must be a valid compound-stat handle, not used again by the
/// caller when it was owned.
pub(crate) unsafe fn take_compound_stat(stat: *mut kafka_common_metrics_CompoundStat_t) -> Box<dyn CompoundStat> {
    Box::new(ArcCompoundStat(unsafe {
        Interface::take(stat as *mut Interface<dyn CompoundStat>)
    }))
}

/// Registers a C implementation of `CompoundStat`. The caller owns `self_`
/// and keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_CompoundStat_destroy`] or, once a `*mut` parameter
/// consumed it, until the sensor it went into is destroyed with its
/// registry.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_CompoundStat_new(
    self_: *mut c_void,
    record: kafka_common_metrics_CompoundStat_record_fn_t,
    stats: kafka_common_metrics_CompoundStat_stats_fn_t,
) -> *mut kafka_common_metrics_CompoundStat_t {
    let stat: Arc<dyn CompoundStat> = Arc::new(CCompoundStat { self_, record, stats });
    Interface::owned(stat) as *mut kafka_common_metrics_CompoundStat_t
}

/// `record(MetricConfig config, double value, long timeMs)`.
///
/// # Safety
///
/// `self_` must be a valid compound-stat handle and `config` a valid
/// metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_record(
    self_: *const kafka_common_metrics_CompoundStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    unsafe { compound_stat_ref(self_) }.record(unsafe { metric_config_ref(config) }, value, time_ms);
}

/// `stats()`: an owned list of owned
/// `kafka_common_metrics_CompoundStat_NamedMeasurable_t *` elements, freed
/// together with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid compound-stat handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_stats(
    self_: *const kafka_common_metrics_CompoundStat_t,
) -> *mut kafka_List_t {
    let stats = unsafe { compound_stat_ref(self_) }
        .stats()
        .into_iter()
        .map(|named| box_named_measurable(named) as *mut c_void)
        .collect();
    box_list(stats, Some(destroy_boxed::<NamedMeasurableInner>))
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned compound-stat handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_destroy(self_: *mut kafka_common_metrics_CompoundStat_t) {
    unsafe { Interface::<dyn CompoundStat>::destroy(self_ as *mut Interface<dyn CompoundStat>) }
}

// ---------------------------------------------------------------------------
// CompoundStat.NamedMeasurable
// ---------------------------------------------------------------------------

/// What a [`kafka_common_metrics_CompoundStat_NamedMeasurable_t`] points at:
/// the pair plus the metric-name handle the borrowed getter hands out.
struct NamedMeasurableInner {
    named: NamedMeasurable,
    name: MetricNameInner,
}

fn box_named_measurable(named: NamedMeasurable) -> *mut kafka_common_metrics_CompoundStat_NamedMeasurable_t {
    let name = MetricNameInner::new(named.name().clone());
    Box::into_raw(Box::new(NamedMeasurableInner { named, name }))
        as *mut kafka_common_metrics_CompoundStat_NamedMeasurable_t
}

unsafe fn named_inner<'a>(
    self_: *const kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) -> &'a NamedMeasurableInner {
    unsafe { &*(self_ as *const NamedMeasurableInner) }
}

/// `new NamedMeasurable(MetricName name, Measurable stat)`: `name` is
/// copied, `stat` is consumed (an owned handle is freed, a view is shared).
/// Owned, freed with [`kafka_common_metrics_CompoundStat_NamedMeasurable_destroy`]
/// or taken over by Rust when returned from a `stats` implementation.
///
/// # Safety
///
/// `name` must be a valid metric-name handle and `stat` a valid measurable
/// handle, not used again by the caller when it was owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_new(
    name: *const kafka_common_MetricName_t,
    stat: *mut kafka_common_metrics_Measurable_t,
) -> *mut kafka_common_metrics_CompoundStat_NamedMeasurable_t {
    let name = unsafe { metric_name_ref(name) }.clone();
    let stat = unsafe { take_measurable(stat) };
    box_named_measurable(NamedMeasurable::new(name, stat))
}

/// `name()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid named-measurable handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_name(
    self_: *const kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) -> *const kafka_common_MetricName_t {
    unsafe { named_inner(self_) }.name.as_ptr()
}

/// `stat()`: an owned handle sharing the measurable, freed with
/// `kafka_common_metrics_Measurable_destroy`.
///
/// # Safety
///
/// `self_` must be a valid named-measurable handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_stat(
    self_: *const kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) -> *mut kafka_common_metrics_Measurable_t {
    box_measurable(unsafe { named_inner(self_) }.named.stat())
}

/// Frees an owned named-measurable handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_destroy(
    self_: *mut kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut NamedMeasurableInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::CStr;
    use std::ptr;
    use std::sync::Mutex;

    use super::*;
    use crate::common::MetricName;
    use crate::common::metrics::ClosureMeasurable;
    use crate::ffi::common::metric_name::{
        box_metric_name, kafka_common_MetricName_destroy, kafka_common_MetricName_name,
    };
    use crate::ffi::common::metrics::measurable::{
        kafka_common_metrics_Measurable_destroy, kafka_common_metrics_Measurable_measure,
        kafka_common_metrics_Measurable_new,
    };
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::util::{kafka_List_add, kafka_List_get, kafka_List_new, kafka_List_size};

    unsafe extern "C" fn c_measure(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> f64 {
        unsafe { *(self_ as *const f64) }
    }

    // A C compound stat recording a sum and exposing it under one name.
    unsafe extern "C" fn c_record(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        value: f64,
        _time_ms: i64,
    ) {
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap() += value;
    }
    unsafe extern "C" fn c_stats(self_: *mut c_void) -> *mut kafka_List_t {
        let name = box_metric_name(MetricName::new("sum", "g", "d", BTreeMap::new()));
        let stat = kafka_common_metrics_Measurable_new(self_, c_measure_sum);
        let list = kafka_List_new();
        unsafe {
            kafka_List_add(
                list,
                kafka_common_metrics_CompoundStat_NamedMeasurable_new(name, stat) as *mut c_void,
            );
            kafka_common_MetricName_destroy(name);
        }
        list
    }
    unsafe extern "C" fn c_measure_sum(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> f64 {
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap()
    }

    #[test]
    fn named_measurable_copies_the_name_and_shares_the_stat() {
        let name = box_metric_name(MetricName::new("n", "g", "d", BTreeMap::new()));
        let mut reading = 2.5_f64;
        let stat = kafka_common_metrics_Measurable_new(&mut reading as *mut f64 as *mut c_void, c_measure);
        let config = kafka_common_metrics_MetricConfig_new();
        unsafe {
            let named = kafka_common_metrics_CompoundStat_NamedMeasurable_new(name, stat);
            kafka_common_MetricName_destroy(name);
            let borrowed = kafka_common_metrics_CompoundStat_NamedMeasurable_name(named);
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_name(borrowed)).to_str().unwrap(), "n");
            let shared = kafka_common_metrics_CompoundStat_NamedMeasurable_stat(named);
            assert_eq!(kafka_common_metrics_Measurable_measure(shared, config, 0), 2.5);
            kafka_common_metrics_Measurable_destroy(shared);
            kafka_common_metrics_CompoundStat_NamedMeasurable_destroy(named);
            kafka_common_metrics_CompoundStat_NamedMeasurable_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_the_invokers() {
        let sum = Mutex::new(0.0_f64);
        let handle = kafka_common_metrics_CompoundStat_new(&sum as *const Mutex<f64> as *mut c_void, c_record, c_stats);
        let config = MetricConfig::new();
        unsafe {
            let config_handle = kafka_common_metrics_MetricConfig_new();
            kafka_common_metrics_CompoundStat_record(handle, config_handle, 4.0, 0);
            // Rust takes over the C-built list and its elements.
            let stats = compound_stat_ref(handle).stats();
            assert_eq!(stats.len(), 1);
            assert_eq!(stats[0].name().name(), "sum");
            assert_eq!(stats[0].stat().measure(&config, 0), 4.0);

            // The invoker hands the pairs back to C as an owned list.
            let list = kafka_common_metrics_CompoundStat_stats(handle);
            assert_eq!(kafka_List_size(list), 1);
            let named = kafka_List_get(list, 0) as *const kafka_common_metrics_CompoundStat_NamedMeasurable_t;
            let stat = kafka_common_metrics_CompoundStat_NamedMeasurable_stat(named);
            assert_eq!(kafka_common_metrics_Measurable_measure(stat, config_handle, 0), 4.0);
            kafka_common_metrics_Measurable_destroy(stat);
            kafka_List_destroy(list);
            kafka_common_metrics_MetricConfig_destroy(config_handle);

            let boxed = take_compound_stat(handle);
            boxed.record(&config, 1.0, 0);
            assert_eq!(boxed.stats()[0].stat().measure(&config, 0), 5.0);
            kafka_common_metrics_CompoundStat_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn rust_stats_cross_as_owned_handles() {
        struct TwoStats;
        impl Stat for TwoStats {
            fn record(&self, _config: &MetricConfig, _value: f64, _time_ms: i64) {}
        }
        impl CompoundStat for TwoStats {
            fn stats(&self) -> Vec<NamedMeasurable> {
                ["a", "b"]
                    .into_iter()
                    .map(|name| {
                        NamedMeasurable::new(
                            MetricName::new(name, "g", "d", BTreeMap::new()),
                            Arc::new(ClosureMeasurable::new(|_: &MetricConfig, _| 1.0)),
                        )
                    })
                    .collect()
            }
        }
        let stat: Arc<dyn CompoundStat> = Arc::new(TwoStats);
        let handle = Interface::owned(stat) as *mut kafka_common_metrics_CompoundStat_t;
        unsafe {
            let list = kafka_common_metrics_CompoundStat_stats(handle);
            assert_eq!(kafka_List_size(list), 2);
            let second = kafka_List_get(list, 1) as *const kafka_common_metrics_CompoundStat_NamedMeasurable_t;
            let name = kafka_common_metrics_CompoundStat_NamedMeasurable_name(second);
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_name(name)).to_str().unwrap(), "b");
            kafka_List_destroy(list);
            kafka_common_metrics_CompoundStat_destroy(handle);
        }
    }
}
