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

//! A sampled count statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.WindowedCount`.

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::sampled_stat::{SampledStat, SampledStatBase, impl_sampled_stat_traits};

/// A [`SampledStat`] that counts the number of recordings within each window,
/// incrementing by one regardless of the recorded value.
///
/// In the Java source this extends `WindowedSum`; here it is a standalone
/// statistic, so code that classifies a `WindowedSum` must also account for
/// `WindowedCount`.
#[derive(Debug)]
pub struct WindowedCount {
    base: SampledStatBase,
}

impl WindowedCount {
    /// Creates a windowed count statistic.
    pub fn new() -> Self {
        Self { base: SampledStatBase::new(0.0) }
    }
}

impl Default for WindowedCount {
    fn default() -> Self {
        Self::new()
    }
}

impl SampledStat for WindowedCount {
    fn sampled_base(&self) -> &SampledStatBase {
        &self.base
    }

    fn sampled_base_mut(&mut self) -> &mut SampledStatBase {
        &mut self.base
    }

    fn update(&mut self, sample_index: usize, _config: &MetricConfig, _value: f64, _time_ms: i64) {
        self.base.samples_mut()[sample_index].value += 1.0;
    }

    fn combine(&self, _config: &MetricConfig, _now: i64) -> f64 {
        self.base.samples().iter().map(|s| s.value).sum()
    }
}

impl_sampled_stat_traits!(WindowedCount);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::{Measurable, Stat};

    #[test]
    fn test_count() {
        let config = MetricConfig::new();
        let mut count = WindowedCount::new();
        count.record(&config, 100.0, 0);
        count.record(&config, 0.5, 0);
        assert_eq!(count.measure(&config, 0), 2.0);
    }
}
