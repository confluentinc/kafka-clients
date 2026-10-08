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

//! `kafka_common_metrics_Stat_t`: the `org.apache.kafka.common.metrics.Stat`
//! interface (CLAUDE.md §4, "Traits").
//!
//! A `Stat_t` is only ever the borrowed view a stat class hands out through
//! its `__as_Stat` (`Avg__as_Stat`, `Meter__as_Stat`, ...): nothing in the
//! public API accepts a bare `Stat`, so it has no `_new` and no `_destroy`.

use crate::common::metrics::Stat;
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::metric_config::{kafka_common_metrics_MetricConfig_t, metric_config_ref};

/// Opaque handle to a [`Stat`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_Stat_t {
    _private: [u8; 0],
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `stat` must be a valid stat handle.
pub(crate) unsafe fn stat_ref<'a>(stat: *const kafka_common_metrics_Stat_t) -> &'a dyn Stat {
    unsafe { Interface::<dyn Stat>::from_ptr(stat as *const Interface<dyn Stat>) }.get()
}

/// `record(MetricConfig config, double value, long timeMs)`.
///
/// # Safety
///
/// `self_` must be a valid stat handle and `config` a valid metric-config
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Stat_record(
    self_: *const kafka_common_metrics_Stat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    unsafe { stat_ref(self_) }.record(unsafe { metric_config_ref(config) }, value, time_ms);
}
