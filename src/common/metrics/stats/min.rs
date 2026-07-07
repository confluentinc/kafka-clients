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

//! A sampled minimum statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Min`.

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};

/// Returns the lesser of two values, propagating `NaN` if either is `NaN`.
///
/// `f64::min` instead returns the non-`NaN` operand, which would silently drop
/// a recorded `NaN`.
fn nan_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() { f64::NAN } else { a.min(b) }
}

/// A [`SampledStat`] that reports the minimum over its samples.
#[derive(Debug)]
pub struct Min {
    base: SampledStatBase,
}

impl Min {
    /// Creates a min statistic.
    pub fn new() -> Self {
        Self { base: SampledStatBase::new(f64::MAX) }
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl SampledStat for Min {
    fn sampled_base(&self) -> &SampledStatBase {
        &self.base
    }

    fn sampled_base_mut(&mut self) -> &mut SampledStatBase {
        &mut self.base
    }

    fn update(&mut self, sample_index: usize, _config: &MetricConfig, value: f64, _time_ms: i64) {
        let sample = &mut self.base.samples_mut()[sample_index];
        sample.value = nan_min(sample.value, value);
    }

    fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
        let mut min = f64::MAX;
        let mut count: i64 = 0;
        for sample in self.base.samples() {
            min = nan_min(min, sample.value);
            count += sample.event_count;
        }
        if count == 0 { f64::NAN } else { min }
    }
}

impl_sampled_stat_traits!(Min);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::{Measurable, Stat};

    #[test]
    fn test_min() {
        let config = MetricConfig::new();
        let mut min = Min::new();
        assert!(min.measure(&config, 0).is_nan());
        min.record(&config, 9.0, 0);
        min.record(&config, 2.0, 0);
        min.record(&config, 5.0, 0);
        assert_eq!(min.measure(&config, 0), 2.0);
    }

    #[test]
    fn test_nan_propagates() {
        let config = MetricConfig::new();
        let mut min = Min::new();
        min.record(&config, f64::NAN, 0);
        assert!(min.measure(&config, 0).is_nan());
        // NaN propagates through Math.min, so a later finite value does not
        // clear the poisoned sample.
        min.record(&config, 5.0, 0);
        assert!(min.measure(&config, 0).is_nan());
    }
}
