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

//! A sampled min (`org.apache.kafka.common.metrics.stats.Min`).

use crate::common::metrics::stats::sampled_stat::{Sample, SampledStat, SampledStatKind};
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A [`SampledStat`] that gives the min over its samples.
pub struct Min {
    inner: SampledStat,
}

struct MinKind;

impl SampledStatKind for MinKind {
    fn update(&self, sample: &mut Sample, _config: &MetricConfig, value: f64, _now: i64) {
        sample.value = sample.value.min(value);
    }

    fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
        // Java seeds with Double.MAX_VALUE (not +inf) and uses Math.min.
        let mut min = f64::MAX;
        let mut count: i64 = 0;
        for sample in samples {
            min = min.min(sample.value);
            count += sample.event_count;
        }
        if count == 0 { f64::NAN } else { min }
    }
}

impl Min {
    /// Create a `Min`.
    pub fn new() -> Self {
        // Java seeds the initial value with Double.MAX_VALUE.
        Self { inner: SampledStat::new(f64::MAX, Box::new(MinKind)) }
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for Min {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner.record(config, value, time_ms);
    }
}

impl Measurable for Min {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.inner.measure(config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_of_records() {
        let config = MetricConfig::new();
        let min = Min::new();
        for i in 0..10 {
            min.record(&config, i as f64, 0);
        }
        assert_eq!(0.0, min.measure(&config, 0));
    }

    #[test]
    fn nan_when_no_events() {
        let config = MetricConfig::new();
        let min = Min::new();
        assert!(min.measure(&config, 0).is_nan());
    }
}
