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

//! A sampled average (`org.apache.kafka.common.metrics.stats.Avg`).

use crate::common::metrics::stats::sampled_stat::{Sample, SampledStat, SampledStatKind};
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A [`SampledStat`] that maintains a simple average over its samples.
pub struct Avg {
    inner: SampledStat,
}

struct AvgKind;

impl SampledStatKind for AvgKind {
    fn update(&self, sample: &mut Sample, _config: &MetricConfig, value: f64, _now: i64) {
        sample.value += value;
    }

    fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
        let mut total = 0.0;
        let mut count: i64 = 0;
        for s in samples {
            total += s.value;
            count += s.event_count;
        }
        if count == 0 { f64::NAN } else { total / count as f64 }
    }
}

impl Avg {
    /// Create an `Avg`.
    pub fn new() -> Self {
        Self { inner: SampledStat::new(0.0, Box::new(AvgKind)) }
    }
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for Avg {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner.record(config, value, time_ms);
    }
}

impl Measurable for Avg {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.inner.measure(config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn average_of_records() {
        let config = MetricConfig::new();
        let avg = Avg::new();
        for i in 0..10 {
            avg.record(&config, i as f64, 0);
        }
        assert_eq!(4.5, avg.measure(&config, 0));
    }

    #[test]
    fn nan_when_no_events() {
        let config = MetricConfig::new();
        let avg = Avg::new();
        assert!(avg.measure(&config, 0).is_nan());
    }
}
