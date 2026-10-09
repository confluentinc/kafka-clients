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

//! `kafka_common_metrics_stats_Max_t`:
//! `org.apache.kafka.common.metrics.stats.Max` (CLAUDE.md §4), the sampled maximum of the recorded values.
//! See the module docs of `stats` for the interface views.

use crate::common::metrics::stats::Max;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;

/// Opaque handle to a [`Max`].
#[repr(C)]
pub struct kafka_common_metrics_stats_Max_t {
    _private: [u8; 0],
}

unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_Max_t) -> &'a StatHandle<Max> {
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<Max>) }
}

/// `new Max()`. Owned, freed with [`kafka_common_metrics_stats_Max_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_Max_new() -> *mut kafka_common_metrics_stats_Max_t {
    StatHandle::boxed(Max::new()) as *mut kafka_common_metrics_stats_Max_t
}

/// The stat as the `Stat` interface: a borrowed view valid as long as the
/// handle, never passed to an interface `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Max__as_Stat(
    self_: *const kafka_common_metrics_stats_Max_t,
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
pub unsafe extern "C" fn kafka_common_metrics_stats_Max__as_Measurable(
    self_: *const kafka_common_metrics_stats_Max_t,
) -> *const kafka_common_metrics_Measurable_t {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Max__as_MeasurableStat(
    self_: *const kafka_common_metrics_stats_Max_t,
) -> *const kafka_common_metrics_MeasurableStat_t {
    unsafe { handle(self_) }.as_measurable_stat()
}

/// Frees an owned handle; null is a no-op. A sensor the stat was added to
/// keeps it alive.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Max_destroy(self_: *mut kafka_common_metrics_stats_Max_t) {
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<Max>) }
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
        let stat = kafka_common_metrics_stats_Max_new();
        let config = kafka_common_metrics_MetricConfig_new();
        unsafe {
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_Max__as_Stat(stat), config, 2.0, 0);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_Max__as_Stat(stat), config, 4.0, 1);
            assert_eq!(
                kafka_common_metrics_Measurable_measure(kafka_common_metrics_stats_Max__as_Measurable(stat), config, 1),
                4.0
            );
            assert_eq!(
                kafka_common_metrics_MeasurableStat_measure(
                    kafka_common_metrics_stats_Max__as_MeasurableStat(stat),
                    config,
                    1
                ),
                4.0
            );
            kafka_common_metrics_stats_Max_destroy(stat);
            kafka_common_metrics_stats_Max_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
