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

//! A sampled maximum statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Max`.

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};

/// Returns the greater of two values, propagating `NaN` if either is `NaN`.
///
/// `f64::max` instead returns the non-`NaN` operand, which would silently drop
/// a recorded `NaN`.
fn nan_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() { f64::NAN } else { a.max(b) }
}

/// A [`SampledStat`] that reports the maximum over its samples.
#[derive(Debug)]
pub struct Max {
    base: SampledStatBase,
}

impl Max {
    /// Creates a max statistic.
    pub fn new() -> Self {
        Self { base: SampledStatBase::new(f64::NEG_INFINITY) }
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl SampledStat for Max {
    fn sampled_base(&self) -> &SampledStatBase {
        &self.base
    }

    fn sampled_base_mut(&mut self) -> &mut SampledStatBase {
        &mut self.base
    }

    fn update(&mut self, sample_index: usize, _config: &MetricConfig, value: f64, _time_ms: i64) {
        let sample = &mut self.base.samples_mut()[sample_index];
        sample.value = nan_max(sample.value, value);
    }

    fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
        let mut max = f64::NEG_INFINITY;
        let mut count: i64 = 0;
        for sample in self.base.samples() {
            max = nan_max(max, sample.value);
            count += sample.event_count;
        }
        if count == 0 { f64::NAN } else { max }
    }
}

impl_sampled_stat_traits!(Max);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::{Measurable, Stat};

    #[test]
    fn test_max() {
        let config = MetricConfig::new();
        let mut max = Max::new();
        assert!(max.measure(&config, 0).is_nan());
        max.record(&config, 2.0, 0);
        max.record(&config, 9.0, 0);
        max.record(&config, 5.0, 0);
        assert_eq!(max.measure(&config, 0), 9.0);
    }

    #[test]
    fn test_nan_propagates() {
        let config = MetricConfig::new();
        let mut max = Max::new();
        max.record(&config, f64::NAN, 0);
        assert!(max.measure(&config, 0).is_nan());
        // NaN propagates through Math.max, so a later finite value does not
        // clear the poisoned sample.
        max.record(&config, 5.0, 0);
        assert!(max.measure(&config, 0).is_nan());
    }
}
