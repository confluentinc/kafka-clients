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

//! `kafka_common_metrics_stats_*`: `org.apache.kafka.common.metrics.stats`
//! (CLAUDE.md §4).
//!
//! Every stat class is a handle on a shared stat ([`StatHandle`]) and hands
//! out borrowed views of the interfaces it implements through
//! `__as_Stat`, `__as_Measurable`, `__as_MeasurableStat` and, for `Meter`,
//! `__as_CompoundStat` (§4, "Traits"). A view is valid as long as the class
//! handle; passing it where the sensor consumes a `*mut` interface handle
//! shares the stat with the sensor and leaves the class handle valid, so
//! the C caller can keep measuring through it. `MeasurableStat` comes from
//! Rust's blanket `impl<T: Stat + Measurable> MeasurableStat for T`.
//!
//! Each class file writes its exports out in full (the translation lint
//! reads the source without expanding macros).

use std::sync::{Arc, OnceLock};

use crate::common::metrics::{CompoundStat, Measurable, MeasurableStat, Stat};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::compound_stat::kafka_common_metrics_CompoundStat_t;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;

pub(crate) mod avg;
pub(crate) mod cumulative_count;
pub(crate) mod cumulative_sum;
pub(crate) mod max;
pub(crate) mod meter;
pub(crate) mod min;
pub(crate) mod rate;
pub(crate) mod sampled_stat;
pub(crate) mod simple_rate;
pub(crate) mod value;
pub(crate) mod windowed_count;
pub(crate) mod windowed_sum;

/// What a stat class handle points at: the stat, shared with every sensor
/// it was added to, plus the interface views it hands out.
pub(crate) struct StatHandle<T> {
    stat: Arc<T>,
    as_stat: OnceLock<Interface<dyn Stat>>,
    as_measurable: OnceLock<Interface<dyn Measurable>>,
    as_measurable_stat: OnceLock<Interface<dyn MeasurableStat>>,
    as_compound_stat: OnceLock<Interface<dyn CompoundStat>>,
}

impl<T> StatHandle<T> {
    /// Hands `stat` to C as an owned handle.
    pub(crate) fn boxed(stat: T) -> *mut Self {
        Box::into_raw(Box::new(Self {
            stat: Arc::new(stat),
            as_stat: OnceLock::new(),
            as_measurable: OnceLock::new(),
            as_measurable_stat: OnceLock::new(),
            as_compound_stat: OnceLock::new(),
        }))
    }

    /// The handle behind a pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this class.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        unsafe { &*ptr }
    }

    /// The stat.
    pub(crate) fn stat(&self) -> &Arc<T> {
        &self.stat
    }

    /// Frees an owned handle; null is a no-op.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle not yet destroyed.
    pub(crate) unsafe fn destroy(ptr: *mut Self) {
        if !ptr.is_null() {
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

impl<T: Stat + 'static> StatHandle<T> {
    /// The stat as a `Stat`: a view valid as long as the handle.
    pub(crate) fn as_stat(&self) -> *const kafka_common_metrics_Stat_t {
        let view = self
            .as_stat
            .get_or_init(|| Interface::view(Arc::clone(&self.stat) as Arc<dyn Stat>));
        view as *const Interface<dyn Stat> as *const kafka_common_metrics_Stat_t
    }
}

impl<T: Measurable + 'static> StatHandle<T> {
    /// The stat as a `Measurable`: a view valid as long as the handle.
    pub(crate) fn as_measurable(&self) -> *const kafka_common_metrics_Measurable_t {
        let view = self
            .as_measurable
            .get_or_init(|| Interface::view(Arc::clone(&self.stat) as Arc<dyn Measurable>));
        view as *const Interface<dyn Measurable> as *const kafka_common_metrics_Measurable_t
    }
}

impl<T: MeasurableStat + 'static> StatHandle<T> {
    /// The stat as a `MeasurableStat`: a view valid as long as the handle.
    pub(crate) fn as_measurable_stat(&self) -> *const kafka_common_metrics_MeasurableStat_t {
        let view = self
            .as_measurable_stat
            .get_or_init(|| Interface::view(Arc::clone(&self.stat) as Arc<dyn MeasurableStat>));
        view as *const Interface<dyn MeasurableStat> as *const kafka_common_metrics_MeasurableStat_t
    }
}

impl<T: CompoundStat + 'static> StatHandle<T> {
    /// The stat as a `CompoundStat`: a view valid as long as the handle.
    pub(crate) fn as_compound_stat(&self) -> *const kafka_common_metrics_CompoundStat_t {
        let view = self
            .as_compound_stat
            .get_or_init(|| Interface::view(Arc::clone(&self.stat) as Arc<dyn CompoundStat>));
        view as *const Interface<dyn CompoundStat> as *const kafka_common_metrics_CompoundStat_t
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::MetricConfig;
    use crate::common::metrics::stats::Avg;
    use crate::ffi::common::metrics::measurable::measurable_ref;
    use crate::ffi::common::metrics::measurable_stat::take_measurable_stat;
    use crate::ffi::common::metrics::stat::stat_ref;

    #[test]
    fn views_are_cached_and_share_the_stat() {
        let handle = StatHandle::boxed(Avg::new());
        let config = MetricConfig::new();
        unsafe {
            let h = StatHandle::from_ptr(handle);
            assert_eq!(h.as_stat(), h.as_stat());
            assert_eq!(h.as_measurable(), h.as_measurable());
            assert_eq!(h.as_measurable_stat(), h.as_measurable_stat());
            stat_ref(h.as_stat()).record(&config, 3.0, 0);
            assert_eq!(measurable_ref(h.as_measurable()).measure(&config, 0), 3.0);
            // Taking the view shares the stat: the handle keeps working.
            let shared = take_measurable_stat(h.as_measurable_stat() as *mut _);
            shared.record(&config, 5.0, 0);
            assert_eq!(h.stat().measure(&config, 0), 4.0);
            assert_eq!(Arc::strong_count(h.stat()), 5, "handle + three views + the shared stat");
            StatHandle::destroy(handle);
            StatHandle::<Avg>::destroy(std::ptr::null_mut());
        }
    }
}
