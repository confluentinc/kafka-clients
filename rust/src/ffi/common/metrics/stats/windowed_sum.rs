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

//! `kafka_common_metrics_stats_WindowedSum_t`:
//! `org.apache.kafka.common.metrics.stats.WindowedSum` (CLAUDE.md §4), the sampled sum of the recorded values.
//! See the module docs of `stats` for the interface views.

use crate::common::metrics::stats::WindowedSum;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;
use crate::ffi::ffi_guard;

/// Opaque handle to a [`WindowedSum`].
#[repr(C)]
pub struct kafka_common_metrics_stats_WindowedSum_t {
    _private: [u8; 0],
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid handle of this class, alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_WindowedSum_t) -> &'a StatHandle<WindowedSum> {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class alive for
    // `'a`, so it points at the `StatHandle<WindowedSum>` one of this class's constructors boxed,
    // as `StatHandle::from_ptr` requires.
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<WindowedSum>) }
}

/// `new WindowedSum()`. Owned, freed with [`kafka_common_metrics_stats_WindowedSum_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_WindowedSum_new() -> *mut kafka_common_metrics_stats_WindowedSum_t {
    StatHandle::boxed(WindowedSum::new()) as *mut kafka_common_metrics_stats_WindowedSum_t
}

/// The stat as the `Stat` interface: a borrowed view valid as long as the
/// handle, never passed to an interface `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_WindowedSum__as_Stat(
    self_: *const kafka_common_metrics_stats_WindowedSum_t,
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
pub unsafe extern "C" fn kafka_common_metrics_stats_WindowedSum__as_Measurable(
    self_: *const kafka_common_metrics_stats_WindowedSum_t,
) -> *const kafka_common_metrics_Measurable_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_measurable()
}

/// The stat as the `MeasurableStat` interface (what
/// `kafka_common_metrics_Sensor_add_with_metric_name` takes): a borrowed
/// view valid as long as the handle; the sensor shares the stat and the
/// handle stays valid.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_WindowedSum__as_MeasurableStat(
    self_: *const kafka_common_metrics_stats_WindowedSum_t,
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
pub unsafe extern "C" fn kafka_common_metrics_stats_WindowedSum_destroy(
    self_: *mut kafka_common_metrics_stats_WindowedSum_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // a `StatHandle<WindowedSum>` from this class's constructors, as `StatHandle::destroy`
    // requires; a sensor the stat was added to keeps its own `Arc` of the stat.
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<WindowedSum>) }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_measure;
    use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_measure;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_record;

    #[test]
    fn records_through_the_stat_view_and_measures_through_the_measurable_views() {
        let stat = kafka_common_metrics_stats_WindowedSum_new();
        let config = kafka_common_metrics_MetricConfig_new();
        // SAFETY: Test code: every stat handle comes from this class's constructors and is
        // destroyed once, after the calls through the views it caches; `config` comes from
        // `kafka_common_metrics_MetricConfig_new` and is destroyed once, after them.
        unsafe {
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_WindowedSum__as_Stat(stat), config, 2.0, 0);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_WindowedSum__as_Stat(stat), config, 4.0, 1);
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_WindowedSum__as_Measurable(stat),
                    config,
                    1
                ),
                6.0
            );
            assert_eq!(
                kafka_common_metrics_MeasurableStat_measure(
                    kafka_common_metrics_stats_WindowedSum__as_MeasurableStat(stat),
                    config,
                    1
                ),
                6.0
            );
            kafka_common_metrics_stats_WindowedSum_destroy(stat);
            kafka_common_metrics_stats_WindowedSum_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
