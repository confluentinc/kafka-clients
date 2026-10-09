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

//! `kafka_common_metrics_stats_SimpleRate_t`:
//! `org.apache.kafka.common.metrics.stats.SimpleRate` (CLAUDE.md §4), a
//! rate whose window is the time since the first sample. See the module
//! docs of `stats` for the interface views.

use crate::common::metrics::stats::SimpleRate;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::metric_config::{kafka_common_metrics_MetricConfig_t, metric_config_ref};
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;

/// Opaque handle to a [`SimpleRate`].
#[repr(C)]
pub struct kafka_common_metrics_stats_SimpleRate_t {
    _private: [u8; 0],
}

unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_SimpleRate_t) -> &'a StatHandle<SimpleRate> {
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<SimpleRate>) }
}

/// `new SimpleRate()`. Owned, freed with
/// [`kafka_common_metrics_stats_SimpleRate_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SimpleRate_new() -> *mut kafka_common_metrics_stats_SimpleRate_t {
    StatHandle::boxed(SimpleRate::new()) as *mut kafka_common_metrics_stats_SimpleRate_t
}

/// `windowSize(MetricConfig config, long now)`: the elapsed window in
/// milliseconds.
///
/// # Safety
///
/// `self_` must be a valid handle of this class and `config` a valid
/// metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SimpleRate_window_size(
    self_: *const kafka_common_metrics_stats_SimpleRate_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> i64 {
    unsafe { handle(self_) }
        .stat()
        .window_size(unsafe { metric_config_ref(config) }, now)
}

/// The stat as the `Stat` interface: a borrowed view valid as long as the
/// handle, never passed to an interface `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SimpleRate__as_Stat(
    self_: *const kafka_common_metrics_stats_SimpleRate_t,
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
pub unsafe extern "C" fn kafka_common_metrics_stats_SimpleRate__as_Measurable(
    self_: *const kafka_common_metrics_stats_SimpleRate_t,
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
pub unsafe extern "C" fn kafka_common_metrics_stats_SimpleRate__as_MeasurableStat(
    self_: *const kafka_common_metrics_stats_SimpleRate_t,
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
pub unsafe extern "C" fn kafka_common_metrics_stats_SimpleRate_destroy(
    self_: *mut kafka_common_metrics_stats_SimpleRate_t,
) {
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<SimpleRate>) }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::common::metrics::{Measurable, MetricConfig, Stat};
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_measure;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_record;

    #[test]
    fn window_size_and_views_follow_the_rust_stat() {
        let stat = kafka_common_metrics_stats_SimpleRate_new();
        let reference = SimpleRate::new();
        let config = kafka_common_metrics_MetricConfig_new();
        unsafe {
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SimpleRate__as_Stat(stat), config, 2.0, 0);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SimpleRate__as_Stat(stat), config, 4.0, 1_000);
            reference.record(&MetricConfig::new(), 2.0, 0);
            reference.record(&MetricConfig::new(), 4.0, 1_000);
            assert_eq!(
                kafka_common_metrics_stats_SimpleRate_window_size(stat, config, 2_000),
                reference.window_size(&MetricConfig::new(), 2_000)
            );
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_SimpleRate__as_Measurable(stat),
                    config,
                    2_000
                ),
                reference.measure(&MetricConfig::new(), 2_000)
            );
            assert!(!kafka_common_metrics_stats_SimpleRate__as_MeasurableStat(stat).is_null());
            kafka_common_metrics_stats_SimpleRate_destroy(stat);
            kafka_common_metrics_stats_SimpleRate_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
