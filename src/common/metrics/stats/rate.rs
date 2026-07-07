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

//! The rate of a quantity over a set of samples.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Rate`.

use std::any::Any;

use crate::common::metrics::internals::metrics_utils::convert;
use crate::common::metrics::stats::{SampledStat, WindowedSum};
use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat, TimeUnit};

/// The rate of the given quantity: the total observed over a set of samples
/// divided by the elapsed time over the sample windows.
pub struct Rate {
    pub(crate) unit: TimeUnit,
    pub(crate) stat: Box<dyn SampledStat>,
    time_window_ms: i64,
}

impl Rate {
    /// Creates a per-second rate over a windowed sum.
    pub fn new() -> Self {
        Self::with_unit(TimeUnit::Seconds)
    }

    /// Creates a rate in the given unit over a windowed sum.
    pub fn with_unit(unit: TimeUnit) -> Self {
        Self::with_unit_and_stat(unit, Box::new(WindowedSum::new()))
    }

    /// Creates a per-second rate over the given sampled statistic.
    pub fn with_stat(stat: Box<dyn SampledStat>) -> Self {
        Self::with_unit_and_stat(TimeUnit::Seconds, stat)
    }

    /// Creates a rate in the given unit over the given sampled statistic.
    pub fn with_unit_and_stat(unit: TimeUnit, stat: Box<dyn SampledStat>) -> Self {
        Self::with_unit_stat_window(unit, stat, -1)
    }

    /// Creates a rate, optionally constraining the statistic's time window.
    pub fn with_unit_stat_window(unit: TimeUnit, mut stat: Box<dyn SampledStat>, window: i64) -> Self {
        let time_window_ms = if window > 0 {
            stat.with_time_window(window, unit);
            unit.to_millis(window)
        } else {
            -1
        };
        Self { unit, stat, time_window_ms }
    }

    /// The unit name used when naming rate metrics.
    pub fn unit_name(&self) -> String {
        let name = self.unit.name();
        name[..name.len() - 2].to_lowercase()
    }

    /// The size of the rate-computation window in milliseconds.
    pub fn window_size(&mut self, config: &MetricConfig, now: i64) -> i64 {
        // Purge old samples before computing the window size.
        self.stat.purge_obsolete_samples(config, now);

        // The elapsed time since the oldest non-obsolete window is the batch
        // window used for the rate. To avoid an artificially high rate before
        // enough data exists, assume N-1 complete windows plus whatever
        // fraction of the final window is complete.
        let mut total_elapsed_time_ms = now - self.stat.oldest(now).start_time_ms;
        let window_ms = if self.time_window_ms > 0 {
            self.time_window_ms
        } else {
            config.time_window_ms()
        };
        let num_full_windows = (total_elapsed_time_ms / window_ms) as i32;
        let min_full_windows = config.samples() - 1;

        if num_full_windows < min_full_windows {
            total_elapsed_time_ms += (min_full_windows - num_full_windows) as i64 * window_ms;
        }

        // A window computed at the exact start with no samples would be 0, and
        // a rate over a zero window is undefined, so assume at least 1ms.
        total_elapsed_time_ms.max(1)
    }
}

impl Default for Rate {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for Rate {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.stat.record(config, value, time_ms);
    }
}

impl Measurable for Rate {
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        let value = self.stat.measure(config, now);
        value / convert(self.window_size(config, now), self.unit)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl MeasurableStat for Rate {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::internals::metrics_utils::convert;
    use crate::common::metrics::stats::MockTime;

    const EPS: f64 = 0.000001;

    // Recording and measurement happen before the first sample window finishes,
    // with no prior samples retained.
    #[test]
    fn test_rate_with_no_prior_available_samples() {
        for (num_sample, sample_window_size_sec) in [(1i32, 1i64), (1, 11), (11, 1), (11, 11)] {
            let mut rate = Rate::new();
            let time = MockTime::new();

            let config = MetricConfig::new()
                .with_samples(num_sample)
                .unwrap()
                .with_time_window(sample_window_size_sec, TimeUnit::Seconds);
            let sample_value = 50.0;
            // Record at the beginning of the window.
            rate.record(&config, sample_value, time.milliseconds());
            // Advance almost to the end of the window.
            let measurement_time = TimeUnit::Seconds.to_millis(sample_window_size_sec) - 1;
            time.sleep(measurement_time);
            let observed_rate = rate.measure(&config, time.milliseconds());
            assert!(!observed_rate.is_nan());

            // Without enough samples the algorithm assumes N-1 prior samples of
            // value 0, so the window accounts for those dummy samples.
            let dummy_prior_samples = (num_sample - 1) as i64;
            let window_size =
                convert(measurement_time, TimeUnit::Seconds) + (dummy_prior_samples * sample_window_size_sec) as f64;
            let expected_rate_per_sec = sample_value / window_size;
            assert!(
                (expected_rate_per_sec - observed_rate).abs() < EPS,
                "num_sample={num_sample}, window={sample_window_size_sec}: expected {expected_rate_per_sec}, got {observed_rate}"
            );
        }
    }

    // Record an event roughly every 100ms and check the rate is a stable
    // 10-11 events/sec from the second window on, exercising a sample window
    // that partially overlaps the monitored window.
    #[test]
    fn test_rate_is_consistent_after_the_first_window() {
        let mut rate = Rate::new();
        let time = MockTime::new();
        let config = MetricConfig::new()
            .with_time_window(1, TimeUnit::Seconds)
            .with_samples(2)
            .unwrap();
        let steps = [0, 99, 100, 100, 100, 100, 100, 100, 100, 100, 100];

        // Start the first window and record events at 0,99,199,...,999ms.
        for step_ms in steps {
            time.sleep(step_ms);
            rate.record(&config, 1.0, time.milliseconds());
        }

        // Leave a 100ms gap between windows.
        time.sleep(101);

        // Start the second window and record events at 0,99,199,...,999ms.
        for step_ms in steps {
            time.sleep(step_ms);
            rate.record(&config, 1.0, time.milliseconds());
            let observed_rate = rate.measure(&config, time.milliseconds());
            assert!((10.0..=11.0).contains(&observed_rate), "observed_rate={observed_rate}");
            // Measurements must be repeatable with the same timestamp.
            let measured_again = rate.measure(&config, time.milliseconds());
            assert_eq!(observed_rate, measured_again);
        }
    }
}
