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

//! `kafka_common_metrics_stats_Meter_t`:
//! `org.apache.kafka.common.metrics.stats.Meter` (CLAUDE.md §4), a compound
//! stat pairing a rate with a cumulative total. It implements `Stat` and
//! `CompoundStat` (not `Measurable`), so those are its two views; see the
//! module docs of `stats`.
//!
//! The constructors taking a rate stat share the `SampledStat_t` handle, as
//! Java shares the object, and leave the handle valid. The Rust constructor
//! panics when that stat is not a `WindowedSum`/`WindowedCount`, mirroring
//! Java's `IllegalArgumentException`: a programming precondition the C
//! caller must respect.

use crate::common::metrics::stats::Meter;
use crate::ffi::common::metric_name::{kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metrics::compound_stat::kafka_common_metrics_CompoundStat_t;
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;
use crate::ffi::common::metrics::stats::sampled_stat::{kafka_common_metrics_stats_SampledStat_t, sampled_stat_ref};
use crate::ffi::common::metrics::time_unit::{kafka_common_metrics_TimeUnit_t, value_of as time_unit_of};
use std::sync::Arc;

/// Opaque handle to a [`Meter`].
#[repr(C)]
pub struct kafka_common_metrics_stats_Meter_t {
    _private: [u8; 0],
}

unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_Meter_t) -> &'a StatHandle<Meter> {
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<Meter>) }
}

fn boxed(meter: Meter) -> *mut kafka_common_metrics_stats_Meter_t {
    StatHandle::boxed(meter) as *mut kafka_common_metrics_stats_Meter_t
}

/// `new Meter(MetricName rateMetricName, MetricName totalMetricName)`: per
/// second over a `WindowedSum`; the names are copied. Owned, freed with
/// [`kafka_common_metrics_stats_Meter_destroy`].
///
/// # Safety
///
/// Both names must be valid metric-name handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter_new(
    rate_metric_name: *const kafka_common_MetricName_t,
    total_metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_stats_Meter_t {
    boxed(Meter::new(
        unsafe { metric_name_ref(rate_metric_name) }.clone(),
        unsafe { metric_name_ref(total_metric_name) }.clone(),
    ))
}

/// `new Meter(TimeUnit unit, MetricName rateMetricName, MetricName totalMetricName)`.
///
/// # Safety
///
/// `unit` must be a time-unit singleton and both names valid metric-name
/// handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter_with_unit(
    unit: *const kafka_common_metrics_TimeUnit_t,
    rate_metric_name: *const kafka_common_MetricName_t,
    total_metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_stats_Meter_t {
    boxed(Meter::with_unit(
        unsafe { time_unit_of(unit) },
        unsafe { metric_name_ref(rate_metric_name) }.clone(),
        unsafe { metric_name_ref(total_metric_name) }.clone(),
    ))
}

/// `new Meter(SampledStat rateStat, MetricName rateMetricName, MetricName totalMetricName)`:
/// shares `rate_stat`, which must be a `WindowedSum` or `WindowedCount`.
///
/// # Safety
///
/// `rate_stat` must be a valid sampled-stat handle and both names valid
/// metric-name handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter_with_rate_stat(
    rate_stat: *const kafka_common_metrics_stats_SampledStat_t,
    rate_metric_name: *const kafka_common_MetricName_t,
    total_metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_stats_Meter_t {
    boxed(Meter::with_rate_stat(
        Arc::clone(unsafe { sampled_stat_ref(rate_stat) }),
        unsafe { metric_name_ref(rate_metric_name) }.clone(),
        unsafe { metric_name_ref(total_metric_name) }.clone(),
    ))
}

/// `new Meter(TimeUnit unit, SampledStat rateStat, MetricName rateMetricName, MetricName totalMetricName)`:
/// shares `rate_stat`, which must be a `WindowedSum` or `WindowedCount`.
///
/// # Safety
///
/// `unit` must be a time-unit singleton, `rate_stat` a valid sampled-stat
/// handle and both names valid metric-name handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter_with_unit_rate_stat(
    unit: *const kafka_common_metrics_TimeUnit_t,
    rate_stat: *const kafka_common_metrics_stats_SampledStat_t,
    rate_metric_name: *const kafka_common_MetricName_t,
    total_metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_stats_Meter_t {
    boxed(Meter::with_unit_rate_stat(
        unsafe { time_unit_of(unit) },
        Arc::clone(unsafe { sampled_stat_ref(rate_stat) }),
        unsafe { metric_name_ref(rate_metric_name) }.clone(),
        unsafe { metric_name_ref(total_metric_name) }.clone(),
    ))
}

/// The meter as the `Stat` interface: a borrowed view valid as long as the
/// handle, never passed to an interface `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter__as_Stat(
    self_: *const kafka_common_metrics_stats_Meter_t,
) -> *const kafka_common_metrics_Stat_t {
    unsafe { handle(self_) }.as_stat()
}

/// The meter as the `CompoundStat` interface (what
/// `kafka_common_metrics_Sensor_add` takes): a borrowed view valid as long
/// as the handle; the sensor shares the meter and the handle stays valid.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter__as_CompoundStat(
    self_: *const kafka_common_metrics_stats_Meter_t,
) -> *const kafka_common_metrics_CompoundStat_t {
    unsafe { handle(self_) }.as_compound_stat()
}

/// Frees an owned handle; null is a no-op. A sensor the meter was added to
/// keeps it alive.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_Meter_destroy(self_: *mut kafka_common_metrics_stats_Meter_t) {
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<Meter>) }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::MetricName;
    use crate::common::metrics::stats::WindowedCount;
    use crate::ffi::common::metric_name::{
        box_metric_name, kafka_common_MetricName_destroy, kafka_common_MetricName_name,
    };
    use crate::ffi::common::metrics::compound_stat::{
        kafka_common_metrics_CompoundStat_NamedMeasurable_name, kafka_common_metrics_CompoundStat_stats,
    };
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_measure;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_record;
    use crate::ffi::common::metrics::stats::sampled_stat::{
        kafka_common_metrics_stats_SampledStat__as_Measurable, kafka_common_metrics_stats_SampledStat_destroy,
    };
    use crate::ffi::common::metrics::time_unit::kafka_common_metrics_TimeUnit_minutes;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size};

    fn names() -> (*mut kafka_common_MetricName_t, *mut kafka_common_MetricName_t) {
        (
            box_metric_name(MetricName::new("rate", "g", "", BTreeMap::new())),
            box_metric_name(MetricName::new("total", "g", "", BTreeMap::new())),
        )
    }

    #[test]
    fn every_constructor_exposes_rate_and_total_and_shares_the_rate_stat() {
        let config = kafka_common_metrics_MetricConfig_new();
        let (rate_name, total_name) = names();
        unsafe {
            let counting = StatHandle::boxed(WindowedCount::new().into_sampled_stat())
                as *mut kafka_common_metrics_stats_SampledStat_t;
            let meters = [
                kafka_common_metrics_stats_Meter_new(rate_name, total_name),
                kafka_common_metrics_stats_Meter_with_unit(
                    kafka_common_metrics_TimeUnit_minutes(),
                    rate_name,
                    total_name,
                ),
                kafka_common_metrics_stats_Meter_with_rate_stat(counting, rate_name, total_name),
                kafka_common_metrics_stats_Meter_with_unit_rate_stat(
                    kafka_common_metrics_TimeUnit_minutes(),
                    counting,
                    rate_name,
                    total_name,
                ),
            ];
            kafka_common_MetricName_destroy(rate_name);
            kafka_common_MetricName_destroy(total_name);
            for meter in meters {
                kafka_common_metrics_Stat_record(kafka_common_metrics_stats_Meter__as_Stat(meter), config, 7.0, 0);
                let stats =
                    kafka_common_metrics_CompoundStat_stats(kafka_common_metrics_stats_Meter__as_CompoundStat(meter));
                assert_eq!(kafka_List_size(stats), 2);
                let mut seen = Vec::new();
                for i in 0..2 {
                    let named = kafka_List_get(stats, i) as *const _;
                    let name = kafka_common_metrics_CompoundStat_NamedMeasurable_name(named);
                    seen.push(CStr::from_ptr(kafka_common_MetricName_name(name)).to_str().unwrap().to_string());
                }
                // Java's `Meter.stats()` lists the total before the rate.
                assert_eq!(seen, ["total", "rate"]);
                kafka_List_destroy(stats);
                kafka_common_metrics_stats_Meter_destroy(meter);
            }
            // The two meters built over the counting stat each recorded one
            // event into the shared stat.
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_SampledStat__as_Measurable(counting),
                    config,
                    0
                ),
                2.0
            );
            kafka_common_metrics_stats_SampledStat_destroy(counting);
            kafka_common_metrics_stats_Meter_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}
