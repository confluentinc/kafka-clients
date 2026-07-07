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

//! A sampled average statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Avg`.

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};

/// A [`SampledStat`] that maintains a simple average over its samples.
#[derive(Debug)]
pub struct Avg {
    base: SampledStatBase,
}

impl Avg {
    /// Creates an average statistic.
    pub fn new() -> Self {
        Self { base: SampledStatBase::new(0.0) }
    }
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl SampledStat for Avg {
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
        let mut total = 0.0;
        let mut count: i64 = 0;
        for sample in self.base.samples() {
            total += sample.value;
            count += sample.event_count;
        }
        if count == 0 { f64::NAN } else { total / count as f64 }
    }
}

impl_sampled_stat_traits!(Avg);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::{Measurable, Stat};

    #[test]
    fn test_average() {
        let config = MetricConfig::new();
        let mut avg = Avg::new();
        assert!(avg.measure(&config, 0).is_nan());
        avg.record(&config, 2.0, 0);
        avg.record(&config, 4.0, 0);
        assert_eq!(avg.measure(&config, 0), 3.0);
    }
}
