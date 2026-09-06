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

//! A sampled max (`org.apache.kafka.common.metrics.stats.Max`).

use crate::common::metrics::stats::sampled_stat::{Sample, SampledStat, SampledStatKind};
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A [`SampledStat`] that gives the max over its samples.
pub struct Max {
    inner: SampledStat,
}

struct MaxKind;

impl SampledStatKind for MaxKind {
    fn update(&self, sample: &mut Sample, _config: &MetricConfig, value: f64, _now: i64) {
        sample.value = sample.value.max(value);
    }

    fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
        let mut max = f64::NEG_INFINITY;
        let mut count: i64 = 0;
        for sample in samples {
            max = max.max(sample.value);
            count += sample.event_count;
        }
        if count == 0 { f64::NAN } else { max }
    }
}

impl Max {
    /// Create a `Max`.
    pub fn new() -> Self {
        Self { inner: SampledStat::new(f64::NEG_INFINITY, Box::new(MaxKind)) }
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for Max {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner.record(config, value, time_ms);
    }
}

impl Measurable for Max {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.inner.measure(config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Time;
    use crate::common::metrics::internals::metrics_utils::TimeUnit;
    use crate::common::metrics::time::mock::MockTime;

    #[test]
    fn max_of_records() {
        let config = MetricConfig::new();
        let max = Max::new();
        for i in 0..10 {
            max.record(&config, i as f64, 0);
        }
        assert_eq!(9.0, max.measure(&config, 0));
    }

    // MetricsTest.testOldDataHasNoEffect
    #[test]
    fn test_old_data_has_no_effect() {
        let max = Max::new();
        let window_ms = 100i64;
        let samples = 2;
        let config = MetricConfig::new()
            .time_window(window_ms, TimeUnit::Milliseconds)
            .set_samples(samples);
        let time = MockTime::new();
        max.record(&config, 50.0, time.milliseconds());
        time.sleep(samples as i64 * window_ms);
        assert!(max.measure(&config, time.milliseconds()).is_nan());
    }
}
