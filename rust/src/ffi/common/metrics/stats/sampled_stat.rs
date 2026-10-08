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

//! `kafka_common_metrics_stats_SampledStat_t`:
//! `org.apache.kafka.common.metrics.stats.SampledStat` (CLAUDE.md §4), the
//! windowed-sample stat the concrete classes (`Avg`, `Max`, `WindowedSum`,
//! ...) specialise, plus:
//!
//!   - `kafka_common_metrics_stats_SampledStat_Sample_t`, Java's nested
//!     `SampledStat.Sample` (§4, "Nested types");
//!   - `kafka_common_metrics_stats_SampledStatKind_t`, the Rust-only trait
//!     standing for Java's abstract `update` / `combine` methods, which a C
//!     caller implements to define its own sampled stat (§4, "Traits").
//!
//! A `SampledStat_t` built by `_new` consumes the kind handle it is given.
//! It implements `Stat` and `Measurable`, so it hands out the three views
//! described in the module docs of `stats`; `Rate_with_stat` and
//! `Meter_with_rate_stat` share it rather than consume it.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::{Sample, SampledStat, SampledStatKind};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;
use crate::ffi::util::{box_list, kafka_List_destroy, kafka_List_t, list_elements};

// ---------------------------------------------------------------------------
// SampledStat
// ---------------------------------------------------------------------------

/// Opaque handle to a [`SampledStat`].
#[repr(C)]
pub struct kafka_common_metrics_stats_SampledStat_t {
    _private: [u8; 0],
}

unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_SampledStat_t) -> &'a StatHandle<SampledStat> {
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<SampledStat>) }
}

/// The shared stat behind a handle, for the constructors that share it
/// (`Rate`, `Meter`).
///
/// # Safety
///
/// `stat` must be a valid sampled-stat handle.
pub(crate) unsafe fn sampled_stat_ref<'a>(
    stat: *const kafka_common_metrics_stats_SampledStat_t,
) -> &'a Arc<SampledStat> {
    unsafe { handle(stat) }.stat()
}

/// `new SampledStat(double initialValue)` plus the subclass behaviour: the
/// stat takes over `kind` (an owned handle is consumed, a view is shared).
/// Owned, freed with [`kafka_common_metrics_stats_SampledStat_destroy`].
///
/// # Safety
///
/// `kind` must be a valid kind handle not destroyed afterwards when owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_new(
    initial_value: f64,
    kind: *mut kafka_common_metrics_stats_SampledStatKind_t,
) -> *mut kafka_common_metrics_stats_SampledStat_t {
    let kind = unsafe { Interface::take(kind as *mut Interface<dyn SampledStatKind>) };
    StatHandle::boxed(SampledStat::new(initial_value, Box::new(ArcSampledStatKind(kind))))
        as *mut kafka_common_metrics_stats_SampledStat_t
}

/// The stat as the `Stat` interface: a borrowed view valid as long as the
/// handle, never passed to an interface `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_Stat(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_Stat_t {
    unsafe { handle(self_) }.as_stat()
}

/// The stat as the `Measurable` interface: a borrowed view valid as long as
/// the handle.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_Measurable(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_Measurable_t {
    unsafe { handle(self_) }.as_measurable()
}

/// The stat as the `MeasurableStat` interface: a borrowed view valid as
/// long as the handle; a sensor it is added to shares the stat.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_MeasurableStat(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_MeasurableStat_t {
    unsafe { handle(self_) }.as_measurable_stat()
}

/// Frees an owned handle; null is a no-op. A sensor, rate or meter the stat
/// was shared with keeps it alive.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_destroy(
    self_: *mut kafka_common_metrics_stats_SampledStat_t,
) {
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<SampledStat>) }
}

// ---------------------------------------------------------------------------
// SampledStatKind: the C-implementable behaviour of a sampled stat
// ---------------------------------------------------------------------------

/// Opaque handle to a [`SampledStatKind`] implementation: the abstract
/// `update` / `combine` half of Java's `SampledStat`, with no Java class of
/// its own.
// the subclass behaviour of Java's abstract SampledStat (CLAUDE.md §4)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_metrics_stats_SampledStatKind_t {
    _private: [u8; 0],
}

/// `update(Sample sample, MetricConfig config, double value, long timeMs)`:
/// folds `value` into the current sample. `sample` is borrowed for the call
/// and mutated in place.
pub type kafka_common_metrics_stats_SampledStatKind_update_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    sample: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
);

/// `combine(List<Sample> samples, MetricConfig config, long now)`: the
/// measurement over every sample. `samples` holds
/// `const kafka_common_metrics_stats_SampledStat_Sample_t *` elements
/// borrowed for the call.
pub type kafka_common_metrics_stats_SampledStatKind_combine_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    samples: *const kafka_List_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64;

/// `isWindowedCount()`: whether this kind counts events like
/// `WindowedCount`, which `Meter` consults; a Java default (`false`) when
/// the pointer is null, hence the `Option<..>` typedef (the form cbindgen
/// emits as a plain C function pointer).
pub type kafka_common_metrics_stats_SampledStatKind_is_windowed_count_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> i8>;

/// `isWindowedSum()`: whether this kind is a `WindowedSum`, which `Meter`
/// requires of its rate stat; a Java default (`false`) when the pointer is
/// null.
pub type kafka_common_metrics_stats_SampledStatKind_is_windowed_sum_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> i8>;

/// A kind implemented in C.
struct CSampledStatKind {
    self_: *mut c_void,
    update: kafka_common_metrics_stats_SampledStatKind_update_fn_t,
    combine: kafka_common_metrics_stats_SampledStatKind_combine_fn_t,
    is_windowed_count: kafka_common_metrics_stats_SampledStatKind_is_windowed_count_fn_t,
    is_windowed_sum: kafka_common_metrics_stats_SampledStatKind_is_windowed_sum_fn_t,
}

// SAFETY: `self_` is owned by the C caller, who keeps it alive and
// thread-safe for the registration's lifetime (CLAUDE.md §4, "Traits").
unsafe impl Send for CSampledStatKind {}
unsafe impl Sync for CSampledStatKind {}

impl SampledStatKind for CSampledStatKind {
    fn update(&self, sample: &mut Sample, config: &MetricConfig, value: f64, time_ms: i64) {
        let config = MetricConfigInner::borrowed(config);
        unsafe {
            (self.update)(
                self.self_,
                sample as *mut Sample as *mut kafka_common_metrics_stats_SampledStat_Sample_t,
                config.as_ptr(),
                value,
                time_ms,
            )
        }
    }

    fn combine(&self, samples: &[Sample], config: &MetricConfig, now: i64) -> f64 {
        let config = MetricConfigInner::borrowed(config);
        let list = box_list(
            samples.iter().map(|sample| sample as *const Sample as *mut c_void).collect(),
            None,
        );
        let combined = unsafe { (self.combine)(self.self_, list, config.as_ptr(), now) };
        unsafe { kafka_List_destroy(list) };
        combined
    }

    fn is_windowed_count(&self) -> bool {
        match self.is_windowed_count {
            Some(is_windowed_count) => unsafe { is_windowed_count(self.self_) != 0 },
            None => false,
        }
    }

    fn is_windowed_sum(&self) -> bool {
        match self.is_windowed_sum {
            Some(is_windowed_sum) => unsafe { is_windowed_sum(self.self_) != 0 },
            None => false,
        }
    }
}

/// A shared kind as the boxed kind [`SampledStat::new`] takes.
pub(crate) struct ArcSampledStatKind(pub(crate) Arc<dyn SampledStatKind>);

impl SampledStatKind for ArcSampledStatKind {
    fn update(&self, sample: &mut Sample, config: &MetricConfig, value: f64, time_ms: i64) {
        self.0.update(sample, config, value, time_ms)
    }

    fn combine(&self, samples: &[Sample], config: &MetricConfig, now: i64) -> f64 {
        self.0.combine(samples, config, now)
    }

    fn is_windowed_count(&self) -> bool {
        self.0.is_windowed_count()
    }

    fn is_windowed_sum(&self) -> bool {
        self.0.is_windowed_sum()
    }
}

/// The kind behind a handle.
///
/// # Safety
///
/// `kind` must be a valid kind handle.
pub(crate) unsafe fn sampled_stat_kind_ref<'a>(
    kind: *const kafka_common_metrics_stats_SampledStatKind_t,
) -> &'a dyn SampledStatKind {
    unsafe { Interface::from_ptr(kind as *const Interface<dyn SampledStatKind>) }.get()
}

/// Builds a kind from a C implementation: `self_` is owned by the caller and
/// kept alive until the stat built over it is destroyed; the two `is_*`
/// methods may be null for the Java default (`false`). Owned, consumed by
/// [`kafka_common_metrics_stats_SampledStat_new`] or freed with
/// [`kafka_common_metrics_stats_SampledStatKind_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SampledStatKind_new(
    self_: *mut c_void,
    update: kafka_common_metrics_stats_SampledStatKind_update_fn_t,
    combine: kafka_common_metrics_stats_SampledStatKind_combine_fn_t,
    is_windowed_count: kafka_common_metrics_stats_SampledStatKind_is_windowed_count_fn_t,
    is_windowed_sum: kafka_common_metrics_stats_SampledStatKind_is_windowed_sum_fn_t,
) -> *mut kafka_common_metrics_stats_SampledStatKind_t {
    Interface::owned(
        Arc::new(CSampledStatKind { self_, update, combine, is_windowed_count, is_windowed_sum })
            as Arc<dyn SampledStatKind>,
    ) as *mut kafka_common_metrics_stats_SampledStatKind_t
}

/// Invokes `update` on the kind behind `self_`.
///
/// # Safety
///
/// `self_` must be a valid kind handle, `sample` a valid sample handle and
/// `config` a valid metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_update(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
    sample: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    unsafe { sampled_stat_kind_ref(self_) }.update(
        unsafe { &mut *(sample as *mut Sample) },
        unsafe { metric_config_ref(config) },
        value,
        time_ms,
    )
}

/// Invokes `combine` on the kind behind `self_`; `samples` holds
/// `const kafka_common_metrics_stats_SampledStat_Sample_t *` elements, read
/// during the call.
///
/// # Safety
///
/// `self_` must be a valid kind handle, `samples` null or a valid list of
/// sample handles and `config` a valid metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_combine(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
    samples: *const kafka_List_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64 {
    let samples: Vec<Sample> = unsafe { list_elements(samples) }
        .iter()
        .map(|&sample| unsafe { &*(sample as *const Sample) }.clone())
        .collect();
    unsafe { sampled_stat_kind_ref(self_) }.combine(&samples, unsafe { metric_config_ref(config) }, now)
}

/// Invokes `isWindowedCount` on the kind behind `self_`.
///
/// # Safety
///
/// `self_` must be a valid kind handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_is_windowed_count(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
) -> i8 {
    i8::from(unsafe { sampled_stat_kind_ref(self_) }.is_windowed_count())
}

/// Invokes `isWindowedSum` on the kind behind `self_`.
///
/// # Safety
///
/// `self_` must be a valid kind handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_is_windowed_sum(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
) -> i8 {
    i8::from(unsafe { sampled_stat_kind_ref(self_) }.is_windowed_sum())
}

/// Frees an owned kind handle that was not consumed; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet consumed or destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_destroy(
    self_: *mut kafka_common_metrics_stats_SampledStatKind_t,
) {
    unsafe { Interface::destroy(self_ as *mut Interface<dyn SampledStatKind>) }
}

// ---------------------------------------------------------------------------
// SampledStat.Sample
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Sample`]: Java's nested `SampledStat.Sample`.
#[repr(C)]
pub struct kafka_common_metrics_stats_SampledStat_Sample_t {
    _private: [u8; 0],
}

unsafe fn sample_ref<'a>(self_: *const kafka_common_metrics_stats_SampledStat_Sample_t) -> &'a Sample {
    unsafe { &*(self_ as *const Sample) }
}

unsafe fn sample_mut<'a>(self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t) -> &'a mut Sample {
    unsafe { &mut *(self_ as *mut Sample) }
}

/// `new Sample(double initialValue, long now)`. Owned, freed with
/// [`kafka_common_metrics_stats_SampledStat_Sample_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_new(
    initial_value: f64,
    now: i64,
) -> *mut kafka_common_metrics_stats_SampledStat_Sample_t {
    Box::into_raw(Box::new(Sample::new(initial_value, now))) as *mut kafka_common_metrics_stats_SampledStat_Sample_t
}

/// `new Sample(double initialValue, long now, long timeWindowMs)`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_with_time_window_ms(
    initial_value: f64,
    now: i64,
    time_window_ms: i64,
) -> *mut kafka_common_metrics_stats_SampledStat_Sample_t {
    Box::into_raw(Box::new(Sample::with_time_window_ms(initial_value, now, time_window_ms)))
        as *mut kafka_common_metrics_stats_SampledStat_Sample_t
}

/// `reset(long now)`: clears the sample to start recording again at `now`.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_reset(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    now: i64,
) {
    unsafe { sample_mut(self_) }.reset(now)
}

/// `isComplete(long timeMs, MetricConfig config)`: whether the sample's
/// event or time window has elapsed.
///
/// # Safety
///
/// `self_` must be a valid sample handle and `config` a valid metric-config
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_is_complete(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
    time_ms: i64,
    config: *const kafka_common_metrics_MetricConfig_t,
) -> i8 {
    i8::from(unsafe { sample_ref(self_) }.is_complete(time_ms, unsafe { metric_config_ref(config) }))
}

/// The value the sample resets to.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_initial_value(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> f64 {
    unsafe { sample_ref(self_) }.initial_value()
}

/// The number of events recorded into the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_event_count(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    unsafe { sample_ref(self_) }.event_count()
}

/// When the sample started.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_start_time_ms(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    unsafe { sample_ref(self_) }.start_time_ms()
}

/// When the last event was recorded into the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_last_event_ms(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    unsafe { sample_ref(self_) }.last_event_ms()
}

/// The accumulated value of the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_value(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> f64 {
    unsafe { sample_ref(self_) }.value()
}

/// The sample's own time window, `-1` for the config's.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_time_window_ms(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    unsafe { sample_ref(self_) }.time_window_ms()
}

/// Sets the number of events recorded into the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_set_event_count(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    event_count: i64,
) {
    unsafe { sample_mut(self_) }.set_event_count(event_count)
}

/// Sets when the last event was recorded into the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_set_last_event_ms(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    last_event_ms: i64,
) {
    unsafe { sample_mut(self_) }.set_last_event_ms(last_event_ms)
}

/// Sets the accumulated value of the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_set_value(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    value: f64,
) {
    unsafe { sample_mut(self_) }.set_value(value)
}

/// Frees an owned sample handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_destroy(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut Sample) });
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_measure;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_record;
    use crate::ffi::util::{kafka_List_get, kafka_List_size};

    /// A C "max" kind: the sample keeps the largest value, `combine` takes
    /// the largest sample.
    unsafe extern "C" fn max_update(
        _self: *mut c_void,
        sample: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
        _config: *const kafka_common_metrics_MetricConfig_t,
        value: f64,
        _time_ms: i64,
    ) {
        unsafe {
            let current = kafka_common_metrics_stats_SampledStat_Sample_value(sample);
            kafka_common_metrics_stats_SampledStat_Sample_set_value(sample, current.max(value));
        }
    }

    unsafe extern "C" fn max_combine(
        self_: *mut c_void,
        samples: *const kafka_List_t,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> f64 {
        let mut max = f64::NEG_INFINITY;
        unsafe {
            *(self_ as *mut i32) += 1;
            for i in 0..kafka_List_size(samples) {
                let sample = kafka_List_get(samples, i) as *const kafka_common_metrics_stats_SampledStat_Sample_t;
                max = max.max(kafka_common_metrics_stats_SampledStat_Sample_value(sample));
            }
        }
        max
    }

    unsafe extern "C" fn yes(_self: *mut c_void) -> i8 {
        1
    }

    /// A Rust kind summing values, standing for the crate-private
    /// `WindowedSumKind`.
    struct SumKind;

    impl SampledStatKind for SumKind {
        fn update(&self, sample: &mut Sample, _config: &MetricConfig, value: f64, _time_ms: i64) {
            sample.set_value(sample.value() + value);
        }

        fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
            samples.iter().map(Sample::value).sum()
        }

        fn is_windowed_sum(&self) -> bool {
            true
        }
    }

    #[test]
    fn a_c_kind_drives_a_sampled_stat_and_null_predicates_are_the_java_default() {
        let mut combines = 0i32;
        let config = kafka_common_metrics_MetricConfig_new();
        unsafe {
            let kind = kafka_common_metrics_stats_SampledStatKind_new(
                &mut combines as *mut i32 as *mut c_void,
                max_update,
                max_combine,
                None,
                Some(yes),
            );
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_count(kind), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_sum(kind), 1);

            let stat = kafka_common_metrics_stats_SampledStat_new(f64::NEG_INFINITY, kind);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SampledStat__as_Stat(stat), config, 3.0, 0);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SampledStat__as_Stat(stat), config, 9.0, 1);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SampledStat__as_Stat(stat), config, 5.0, 2);
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_SampledStat__as_Measurable(stat),
                    config,
                    2
                ),
                9.0
            );
            assert!(!kafka_common_metrics_stats_SampledStat__as_MeasurableStat(stat).is_null());
            assert_eq!(combines, 1);
            kafka_common_metrics_stats_SampledStat_destroy(stat);
            kafka_common_metrics_stats_SampledStat_destroy(ptr::null_mut());
            kafka_common_metrics_stats_SampledStatKind_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }

    #[test]
    fn invokers_reach_a_rust_kind_through_sample_handles() {
        let config = kafka_common_metrics_MetricConfig_new();
        unsafe {
            let kind = Interface::owned(Arc::new(SumKind) as Arc<dyn SampledStatKind>)
                as *mut kafka_common_metrics_stats_SampledStatKind_t;
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_sum(kind), 1);
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_count(kind), 0);

            let first = kafka_common_metrics_stats_SampledStat_Sample_new(0.0, 0);
            let second = kafka_common_metrics_stats_SampledStat_Sample_with_time_window_ms(0.0, 10, 500);
            kafka_common_metrics_stats_SampledStatKind_update(kind, first, config, 2.0, 1);
            kafka_common_metrics_stats_SampledStatKind_update(kind, first, config, 3.0, 2);
            kafka_common_metrics_stats_SampledStatKind_update(kind, second, config, 4.0, 11);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_value(first), 5.0);
            // The kind folds values only; `SampledStat.record` keeps the
            // event bookkeeping, so a direct `update` leaves it untouched.
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_event_count(first), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_last_event_ms(first), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_start_time_ms(second), 10);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_time_window_ms(second), 500);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_time_window_ms(first), -1);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_initial_value(first), 0.0);

            let samples = box_list(vec![first as *mut c_void, second as *mut c_void], None);
            assert_eq!(
                kafka_common_metrics_stats_SampledStatKind_combine(kind, samples, config, 11),
                9.0
            );
            kafka_List_destroy(samples);

            // The 500 ms window of `second` elapses at 510.
            assert_eq!(
                kafka_common_metrics_stats_SampledStat_Sample_is_complete(second, 509, config),
                0
            );
            assert_eq!(
                kafka_common_metrics_stats_SampledStat_Sample_is_complete(second, 510, config),
                1
            );

            kafka_common_metrics_stats_SampledStat_Sample_set_event_count(first, 7);
            kafka_common_metrics_stats_SampledStat_Sample_set_last_event_ms(first, 70);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_event_count(first), 7);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_last_event_ms(first), 70);
            kafka_common_metrics_stats_SampledStat_Sample_reset(first, 100);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_value(first), 0.0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_event_count(first), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_start_time_ms(first), 100);

            kafka_common_metrics_stats_SampledStat_Sample_destroy(first);
            kafka_common_metrics_stats_SampledStat_Sample_destroy(second);
            kafka_common_metrics_stats_SampledStat_Sample_destroy(ptr::null_mut());
            kafka_common_metrics_stats_SampledStatKind_destroy(kind);
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
