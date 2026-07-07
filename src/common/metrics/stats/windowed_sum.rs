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

//! A sampled sum statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.WindowedSum`.

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};

/// A [`SampledStat`] that maintains the sum of the values it has seen.
#[derive(Debug)]
pub struct WindowedSum {
    base: SampledStatBase,
}

impl WindowedSum {
    /// Creates a windowed sum statistic.
    pub fn new() -> Self {
        Self { base: SampledStatBase::new(0.0) }
    }
}

impl Default for WindowedSum {
    fn default() -> Self {
        Self::new()
    }
}

impl SampledStat for WindowedSum {
    fn sampled_base(&self) -> &SampledStatBase {
        &self.base
    }

    fn sampled_base_mut(&mut self) -> &mut SampledStatBase {
        &mut self.base
    }

    fn update(&mut self, sample_index: usize, _config: &MetricConfig, value: f64, _time_ms: i64) {
        self.base.samples_mut()[sample_index].value += value;
    }

    fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
        self.base.samples().iter().map(|s| s.value).sum()
    }
}

impl_sampled_stat_traits!(WindowedSum);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::{Measurable, Stat};

    #[test]
    fn test_sum() {
        let config = MetricConfig::new();
        let mut sum = WindowedSum::new();
        sum.record(&config, 2.0, 0);
        sum.record(&config, 3.0, 0);
        assert_eq!(sum.measure(&config, 0), 5.0);
    }
}
