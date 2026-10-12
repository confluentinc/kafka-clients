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

//! `kafka_common_metrics_stats_Rate_t`:
//! `org.apache.kafka.common.metrics.stats.Rate` (CLAUDE.md §4), a sampled
//! stat divided by the elapsed window. See the module docs of `stats` for
//! the interface views.
//!
//! Java's constructors taking a `SampledStat` share it with the rate; so do
//! the C ones, which borrow the `SampledStat_t` handle and leave it valid.

use std::ffi::c_char;
use std::sync::Arc;

use crate::common::metrics::stats::Rate;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::metric_config::{kafka_common_metrics_MetricConfig_t, metric_config_ref};
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;
use crate::ffi::common::metrics::stats::sampled_stat::{kafka_common_metrics_stats_SampledStat_t, sampled_stat_ref};
use crate::ffi::common::metrics::time_unit::{kafka_common_metrics_TimeUnit_t, value_of as time_unit_of};
use crate::ffi::ffi_guard;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`Rate`].
#[repr(C)]
pub struct kafka_common_metrics_stats_Rate_t {
    _private: [u8; 0],
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid handle of this class, alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_Rate_t) -> &'a StatHandle<Rate> {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class alive for
    // `'a`, so it points at the `StatHandle<Rate>` one of this class's constructors boxed, as
    // `StatHandle::from_ptr` requires.
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<Rate>) }
}

fn boxed(rate: Rate) -> *mut kafka_common_metrics_stats_Rate_t {
    StatHandle::boxed(rate) as *mut kafka_common_metrics_stats_Rate_t
}

/// `new Rate()`: per second, over a `WindowedSum`. Owned, freed with
/// [`kafka_common_metrics_stats_Rate_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_Rate_new() -> *mut kafka_common_metrics_stats_Rate_t {
    boxed(Rate::new())
}

/// `new Rate(TimeUnit unit)`.
///
/// # Safety
///
/// `unit` must be a time-unit singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_with_unit(
    unit: *const kafka_common_metrics_TimeUnit_t,
) -> *mut kafka_common_metrics_stats_Rate_t {
    // SAFETY: Per this function's `# Safety`, `unit` is a time-unit singleton, as `time_unit_of`
    // (`time_unit::value_of`) requires; the value is copied out.
    boxed(Rate::with_unit(unsafe { time_unit_of(unit) }))
}

/// `new Rate(SampledStat stat)`: shares `stat`.
///
/// # Safety
///
/// `stat` must be a valid sampled-stat handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_with_stat(
    stat: *const kafka_common_metrics_stats_SampledStat_t,
) -> *mut kafka_common_metrics_stats_Rate_t {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid sampled-stat handle, as
    // `sampled_stat_ref` requires; the rate takes its own `Arc` of the stat, so nothing borrows the
    // handle after this call and the handle stays valid, as the module docs state.
    boxed(Rate::with_stat(Arc::clone(unsafe { sampled_stat_ref(stat) })))
}

/// `new Rate(TimeUnit unit, SampledStat stat)`: shares `stat`.
///
/// # Safety
///
/// `unit` must be a time-unit singleton and `stat` a valid sampled-stat
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_with_unit_stat(
    unit: *const kafka_common_metrics_TimeUnit_t,
    stat: *const kafka_common_metrics_stats_SampledStat_t,
) -> *mut kafka_common_metrics_stats_Rate_t {
    boxed(Rate::with_unit_stat(
        // SAFETY: Per this function's `# Safety`, `unit` is a time-unit singleton, as
        // `time_unit_of` (`time_unit::value_of`) requires; the value is copied out.
        unsafe { time_unit_of(unit) },
        // SAFETY: Per this function's `# Safety`, `stat` is a valid sampled-stat handle, as
        // `sampled_stat_ref` requires; the rate takes its own `Arc` of the stat, so nothing borrows
        // the handle after this call and the handle stays valid, as the module docs state.
        Arc::clone(unsafe { sampled_stat_ref(stat) }),
    ))
}

/// `new Rate(TimeUnit unit, SampledStat stat, long window)`: shares `stat`.
///
/// # Safety
///
/// `unit` must be a time-unit singleton and `stat` a valid sampled-stat
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_with_unit_stat_window(
    unit: *const kafka_common_metrics_TimeUnit_t,
    stat: *const kafka_common_metrics_stats_SampledStat_t,
    window: i64,
) -> *mut kafka_common_metrics_stats_Rate_t {
    boxed(Rate::with_unit_stat_window(
        // SAFETY: Per this function's `# Safety`, `unit` is a time-unit singleton, as
        // `time_unit_of` (`time_unit::value_of`) requires; the value is copied out.
        unsafe { time_unit_of(unit) },
        // SAFETY: Per this function's `# Safety`, `stat` is a valid sampled-stat handle, as
        // `sampled_stat_ref` requires; the rate takes its own `Arc` of the stat, so nothing borrows
        // the handle after this call and the handle stays valid, as the module docs state.
        Arc::clone(unsafe { sampled_stat_ref(stat) }),
        window,
    ))
}

/// `unitName()`: an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_unit_name(
    self_: *const kafka_common_metrics_stats_Rate_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires; the reference is used only within this call, and the unit name is copied into an
    // owned string.
    into_c_string(&unsafe { handle(self_) }.stat().unit_name())
}

/// `windowSize(MetricConfig config, long now)`: the elapsed window in
/// milliseconds.
///
/// A caught Rust panic returns -1, which no window size is.
///
/// # Safety
///
/// `self_` must be a valid handle of this class and `config` a valid
/// metric-config handle.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_window_size(
    self_: *const kafka_common_metrics_stats_Rate_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires; the reference is used only within this call.
    unsafe { handle(self_) }
        .stat()
        // SAFETY: Per this function's `# Safety`, `config` is a valid metric-config handle, as
        // `metric_config_ref` requires; the reference is used only within this call.
        .window_size(unsafe { metric_config_ref(config) }, now)
}

/// The stat as the `Stat` interface: a borrowed view valid as long as the
/// handle, never passed to an interface `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate__as_Stat(
    self_: *const kafka_common_metrics_stats_Rate_t,
) -> *const kafka_common_metrics_Stat_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_stat()
}

/// The stat as the `Measurable` interface: a borrowed view valid as long as
/// the handle.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate__as_Measurable(
    self_: *const kafka_common_metrics_stats_Rate_t,
) -> *const kafka_common_metrics_Measurable_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_measurable()
}

/// The stat as the `MeasurableStat` interface: a borrowed view valid as
/// long as the handle; a sensor it is added to shares the stat.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate__as_MeasurableStat(
    self_: *const kafka_common_metrics_stats_Rate_t,
) -> *const kafka_common_metrics_MeasurableStat_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_measurable_stat()
}

/// Frees an owned handle; null is a no-op. A sensor the stat was added to
/// keeps it alive.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Rate_destroy(self_: *mut kafka_common_metrics_stats_Rate_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // a `StatHandle<Rate>` from this class's constructors, as `StatHandle::destroy` requires; a
    // sensor the stat was added to keeps its own `Arc` of the stat.
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<Rate>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::metrics::stats::WindowedSum;
    use crate::common::metrics::{Measurable, MetricConfig, Stat, TimeUnit};
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_measure;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_record;
    use crate::ffi::common::metrics::stats::sampled_stat::{
        kafka_common_metrics_stats_SampledStat__as_Measurable, kafka_common_metrics_stats_SampledStat_destroy,
    };
    use crate::ffi::common::metrics::time_unit::{
        kafka_common_metrics_TimeUnit_milliseconds, kafka_common_metrics_TimeUnit_minutes,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructors_share_the_sampled_stat_and_follow_the_rust_rate() {
        let config = kafka_common_metrics_MetricConfig_new();
        let rust_config = MetricConfig::new();
        // SAFETY: Test code: every rate comes from this class's constructors and is destroyed once,
        // after the calls through it and its views; `windowed` is a sampled-stat handle boxed as
        // `kafka_common_metrics_stats_SampledStat_new` boxes one and destroyed once, after the
        // rates sharing it; each unit-name string is destroyed once, after it is read; `config`
        // comes from `kafka_common_metrics_MetricConfig_new` and is destroyed once, after them; the
        // time-unit singletons are never freed.
        unsafe {
            let plain = kafka_common_metrics_stats_Rate_new();
            let unit_name = kafka_common_metrics_stats_Rate_unit_name(plain);
            assert_eq!(CStr::from_ptr(unit_name).to_str().unwrap(), Rate::new().unit_name());
            kafka_string_destroy(unit_name);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_Rate__as_Stat(plain), config, 10.0, 0);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_Rate__as_Stat(plain), config, 10.0, 1_000);
            let reference = Rate::new();
            reference.record(&rust_config, 10.0, 0);
            reference.record(&rust_config, 10.0, 1_000);
            assert_eq!(
                kafka_common_metrics_stats_Rate_window_size(plain, config, 2_000),
                reference.window_size(&rust_config, 2_000)
            );
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_Rate__as_Measurable(plain),
                    config,
                    2_000
                ),
                reference.measure(&rust_config, 2_000)
            );
            assert!(!kafka_common_metrics_stats_Rate__as_MeasurableStat(plain).is_null());
            kafka_common_metrics_stats_Rate_destroy(plain);

            let per_minute = kafka_common_metrics_stats_Rate_with_unit(kafka_common_metrics_TimeUnit_minutes());
            let unit_name = kafka_common_metrics_stats_Rate_unit_name(per_minute);
            assert_eq!(
                CStr::from_ptr(unit_name).to_str().unwrap(),
                Rate::with_unit(TimeUnit::Minutes).unit_name()
            );
            kafka_string_destroy(unit_name);
            kafka_common_metrics_stats_Rate_destroy(per_minute);

            // The rate shares the sampled stat: recording through the rate
            // shows through the stat's own handle.
            let windowed = StatHandle::boxed(WindowedSum::new().into_sampled_stat())
                as *mut kafka_common_metrics_stats_SampledStat_t;
            for rate in [
                kafka_common_metrics_stats_Rate_with_stat(windowed),
                kafka_common_metrics_stats_Rate_with_unit_stat(kafka_common_metrics_TimeUnit_milliseconds(), windowed),
                kafka_common_metrics_stats_Rate_with_unit_stat_window(
                    kafka_common_metrics_TimeUnit_milliseconds(),
                    windowed,
                    1_000,
                ),
            ] {
                kafka_common_metrics_Stat_record(kafka_common_metrics_stats_Rate__as_Stat(rate), config, 1.0, 0);
                kafka_common_metrics_stats_Rate_destroy(rate);
            }
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_SampledStat__as_Measurable(windowed),
                    config,
                    0
                ),
                3.0
            );
            kafka_common_metrics_stats_SampledStat_destroy(windowed);
            kafka_common_metrics_stats_Rate_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
