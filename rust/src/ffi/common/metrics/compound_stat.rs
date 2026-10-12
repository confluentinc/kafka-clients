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
use crate::ffi::ffi_guard;
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

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_metrics_CompoundStat_new`: the C user is responsible for the thread-safety of
// `self_`, which that function's rustdoc states, and the function pointers are trivially shareable.
// `CCompoundStat` itself never dereferences `self_`; it only passes it back to the functions
// registered with it.
unsafe impl Send for CCompoundStat {}
// SAFETY: `Sync` is required by the `Stat` trait, the supertrait of `CompoundStat`, whose
// implementations are shared behind an `Arc` and called through `&self` from any thread. A
// `&CCompoundStat` exposes only the opaque `self_` and the function pointers, which the type never
// dereferences; `kafka_common_metrics_CompoundStat_new`'s rustdoc lets them be called from any
// thread, concurrently, and the C user is responsible for the thread-safety of `self_` under those
// calls.
unsafe impl Sync for CCompoundStat {}

impl Stat for CCompoundStat {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.record` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_CompoundStat_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle is destroyed or, once consumed, until the sensor it went
        // into is destroyed with its registry, which is after this `&self` call, and allows the
        // call to come from any thread, concurrently with others. `config` is the handle
        // `MetricConfigInner::borrowed` made from the `&MetricConfig` this method holds, valid for
        // the call, which is all the function type's rustdoc promises the C side.
        unsafe { (self.record)(self.self_, config.as_ptr(), value, time_ms) }
    }
}

impl CompoundStat for CCompoundStat {
    fn stats(&self) -> Vec<NamedMeasurable> {
        // SAFETY: `self.stats` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_CompoundStat_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle is destroyed or, once consumed, until the sensor it went
        // into is destroyed with its registry, which is after this `&self` call, and allows the
        // call to come from any thread, concurrently with others. It takes no other argument; what
        // it returns is handled below.
        let list = unsafe { (self.stats)(self.self_) };
        if list.is_null() {
            return Vec::new();
        }
        // SAFETY: `list` is non-null (checked above) and, per
        // `kafka_common_metrics_CompoundStat_stats_fn_t`'s rustdoc, a list from `kafka_List_new`
        // that Rust now owns, so it stays alive and unmutated until it is destroyed below, after
        // the last use of the slice.
        let stats = unsafe { list_elements(list) }
            .iter()
            // SAFETY: Per `kafka_common_metrics_CompoundStat_stats_fn_t`'s rustdoc, each element is
            // an owned named-measurable handle from
            // `kafka_common_metrics_CompoundStat_NamedMeasurable_new`, the `Box::into_raw` of a
            // `NamedMeasurableInner` in `box_named_measurable`, that Rust takes over, so `from_raw`
            // reclaims that allocation, once; the `kafka_List_new` list holding it has no element
            // destructor and never frees it.
            .map(|&element| unsafe { Box::from_raw(element as *mut NamedMeasurableInner) }.named)
            .collect();
        // SAFETY: `list` is the non-null list from `kafka_List_new` the C function returned, which
        // Rust owns and has not destroyed; it has no element destructor, so this frees the list
        // alone, after its elements were moved out above.
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
/// `stat` must be a valid compound-stat handle, alive for `'a`.
pub(crate) unsafe fn compound_stat_ref<'a>(stat: *const kafka_common_metrics_CompoundStat_t) -> &'a dyn CompoundStat {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid compound-stat handle alive for
    // `'a`, as `Interface::from_ptr` requires: every `kafka_common_metrics_CompoundStat_t` is an
    // `Interface<dyn CompoundStat>`, owned or a view; the reference returned lives for the same
    // `'a`.
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
    // SAFETY: Per this function's `# Safety`, `stat` is a valid compound-stat handle, not used
    // again by the caller when it was owned, as `Interface::take` requires; every such handle is an
    // `Interface` of this trait object.
    Box::new(ArcCompoundStat(unsafe {
        Interface::take(stat as *mut Interface<dyn CompoundStat>)
    }))
}

/// Registers a C implementation of `CompoundStat`. The caller owns `self_`
/// and keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_CompoundStat_destroy`] or, once a `*mut` parameter
/// consumed it, until the sensor it went into is destroyed with its
/// registry. The functions may be called from any thread, concurrently, so
/// the C caller is responsible for their thread-safety on `self_`.
#[ffi_guard]
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_record(
    self_: *const kafka_common_metrics_CompoundStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid compound-stat handle, as
    // `compound_stat_ref` requires, and `config` a valid metric-config handle, as
    // `metric_config_ref` requires; both references are used only within this call.
    unsafe { compound_stat_ref(self_) }.record(unsafe { metric_config_ref(config) }, value, time_ms);
}

/// `stats()`: an owned list of owned
/// `kafka_common_metrics_CompoundStat_NamedMeasurable_t *` elements, freed
/// together with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid compound-stat handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_stats(
    self_: *const kafka_common_metrics_CompoundStat_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid compound-stat handle, as
    // `compound_stat_ref` requires; the reference is used only within this call, and each pair
    // `stats` returns is boxed into an owned handle the returned list owns.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_destroy(self_: *mut kafka_common_metrics_CompoundStat_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned compound-stat handle not
    // yet destroyed, as `Interface::destroy` requires; a view a class handle caches is never passed
    // here.
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

/// What a handle points at.
///
/// # Safety
///
/// `self_` must be a valid named-measurable handle, alive for `'a`.
unsafe fn named_inner<'a>(
    self_: *const kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) -> &'a NamedMeasurableInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid named-measurable handle alive for
    // `'a`, so it points at a live `NamedMeasurableInner`, the `Box::into_raw` of
    // `box_named_measurable`: an owned handle, or an element of the owning list
    // `kafka_common_metrics_CompoundStat_stats` returns, which lives as long as that list. No call
    // mutates a `NamedMeasurableInner`, so the shared reference is sound for `'a`.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_new(
    name: *const kafka_common_MetricName_t,
    stat: *mut kafka_common_metrics_Measurable_t,
) -> *mut kafka_common_metrics_CompoundStat_NamedMeasurable_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid metric-name handle, as
    // `metric_name_ref` requires; the name is cloned, so the borrow lasts for this call only.
    let name = unsafe { metric_name_ref(name) }.clone();
    // SAFETY: Per this function's `# Safety`, `stat` is a valid measurable handle, not used again
    // by the caller when it was owned, as `take_measurable` requires; the `Arc` it returns is
    // owned.
    let stat = unsafe { take_measurable(stat) };
    box_named_measurable(NamedMeasurable::new(name, stat))
}

/// `name()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid named-measurable handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_name(
    self_: *const kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) -> *const kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid named-measurable handle, as
    // `named_inner` requires; the handle returned is a view on the `MetricNameInner` the named
    // measurable embeds, so it lives as long as the named-measurable handle; that is the rustdoc's
    // "borrowed from the handle, valid as long as it is".
    unsafe { named_inner(self_) }.name.as_ptr()
}

/// `stat()`: an owned handle sharing the measurable, freed with
/// `kafka_common_metrics_Measurable_destroy`.
///
/// # Safety
///
/// `self_` must be a valid named-measurable handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_stat(
    self_: *const kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) -> *mut kafka_common_metrics_Measurable_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid named-measurable handle, as
    // `named_inner` requires; `stat` clones the `Arc`, which is boxed into an owned handle, so the
    // borrow lasts for this call only.
    box_measurable(unsafe { named_inner(self_) }.named.stat())
}

/// Frees an owned named-measurable handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_CompoundStat_NamedMeasurable_destroy(
    self_: *mut kafka_common_metrics_CompoundStat_NamedMeasurable_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `NamedMeasurableInner` in
        // `box_named_measurable` (an element of a list Rust returned belongs to that list and is
        // never passed here), so `from_raw` reclaims that allocation, once.
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
        // SAFETY: Test code: `self_` is the `reading` local the test registered, which outlives
        // every call.
        unsafe { *(self_ as *const f64) }
    }

    // A C compound stat recording a sum and exposing it under one name.
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
    unsafe extern "C" fn c_stats(self_: *mut c_void) -> *mut kafka_List_t {
        let name = box_metric_name(MetricName::new("sum", "g", "d", BTreeMap::new()));
        let stat = kafka_common_metrics_Measurable_new(self_, c_measure_sum);
        let list = kafka_List_new();
        // SAFETY: Test code: `list` comes from `kafka_List_new` and takes the owned pair, which
        // takes over `stat` and copies `name`; `name` comes from `box_metric_name` and is destroyed
        // once, after the copy; Rust takes over the list.
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
        // SAFETY: Test code: `self_` is the `sum` mutex the test registered, which outlives every
        // call.
        *unsafe { &*(self_ as *const Mutex<f64>) }.lock().unwrap()
    }

    #[test]
    fn named_measurable_copies_the_name_and_shares_the_stat() {
        let name = box_metric_name(MetricName::new("n", "g", "d", BTreeMap::new()));
        let mut reading = 2.5_f64;
        let stat = kafka_common_metrics_Measurable_new(&mut reading as *mut f64 as *mut c_void, c_measure);
        let config = kafka_common_metrics_MetricConfig_new();
        // SAFETY: Test code: `name` comes from `box_metric_name` and is destroyed once, after the
        // constructor copies it; `stat` comes from `kafka_common_metrics_Measurable_new` over the
        // `reading` local, which outlives it, and the constructor consumes it; `borrowed` is read
        // while `named` lives; `shared`, `named` and `config` are each destroyed once, after their
        // last use.
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
        // SAFETY: Test code: `handle` comes from `kafka_common_metrics_CompoundStat_new` over the
        // `sum` local, which outlives it, and `take_compound_stat` consumes it, after which it is
        // not used; `config_handle` comes from `kafka_common_metrics_MetricConfig_new`; `named` is
        // read while `list`, which owns it, lives; `stat`, `list` and `config_handle` are each
        // destroyed once, after their last use.
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
        // SAFETY: Test code: `handle` comes from `Interface::owned` and is destroyed once, at the
        // end; `second` and its name view are read while `list`, which owns them, lives; `list` is
        // destroyed once.
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
