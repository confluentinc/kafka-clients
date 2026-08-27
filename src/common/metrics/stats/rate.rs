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

//! The rate of a quantity over the sample windows
//! (`org.apache.kafka.common.metrics.stats.Rate`).

use std::sync::Arc;

use crate::common::metrics::internals::metrics_utils::{TimeUnit, convert};
use crate::common::metrics::stats::SampledStat;
use crate::common::metrics::stats::windowed_sum::WindowedSum;
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// The rate of the given quantity. By default this is the total observed over a
/// set of samples from a sampled statistic divided by the elapsed time over the
/// sample windows. Alternative [`SampledStat`] implementations can be provided,
/// however, to record the rate of occurrences (e.g. the count of values measured
/// over the time interval) or other such values.
pub struct Rate {
    unit: TimeUnit,
    stat: Arc<SampledStat>,
    time_window_ms: i64,
}

impl Rate {
    /// Create a `Rate` over seconds backed by a [`WindowedSum`].
    pub fn new() -> Self {
        Self::with_unit(TimeUnit::Seconds)
    }

    /// Create a `Rate` with the given unit backed by a [`WindowedSum`].
    pub fn with_unit(unit: TimeUnit) -> Self {
        Self::with_unit_stat(unit, Arc::new(WindowedSum::new().into_sampled_stat()))
    }

    /// Create a `Rate` over seconds backed by the given sampled stat.
    pub fn with_stat(stat: Arc<SampledStat>) -> Self {
        Self::with_unit_stat(TimeUnit::Seconds, stat)
    }

    /// Create a `Rate` with the given unit and sampled stat.
    pub fn with_unit_stat(unit: TimeUnit, stat: Arc<SampledStat>) -> Self {
        Self::with_unit_stat_window(unit, stat, -1)
    }

    /// Create a `Rate` with the given unit, sampled stat and explicit window.
    ///
    /// `window` is expressed in `unit`; when positive it configures the stat's
    /// own time window (Java `stat.withTimeWindow(window, unit)`).
    pub fn with_unit_stat_window(unit: TimeUnit, stat: Arc<SampledStat>, window: i64) -> Self {
        let time_window_ms = if window > 0 {
            let ms = unit.to_millis(window);
            stat.with_time_window_ms(ms);
            ms
        } else {
            -1
        };
        Self { unit, stat, time_window_ms }
    }

    /// The lower-cased name of the rate's unit with the trailing "s" stripped
    /// (Java `unitName()`), e.g. `SECONDS` -> `second`.
    pub fn unit_name(&self) -> String {
        let name = self.unit.name();
        name[..name.len() - 2].to_lowercase()
    }

    /// The backing sampled stat (shared with the metric value provider).
    pub(crate) fn stat(&self) -> &Arc<SampledStat> {
        &self.stat
    }

    /// Compute the window size in milliseconds used for the rate calculation.
    ///
    /// Faithful translation of `Rate.windowSize`.
    pub fn window_size(&self, config: &MetricConfig, now: i64) -> i64 {
        // purge old samples before we compute the window size
        self.stat.purge_obsolete_samples(config, now);

        // Here we check the total amount of time elapsed since the oldest
        // non-obsolete window. This gives the total windowSize of the batch which
        // is the time used for Rate computation. However, there is an issue if we
        // do not have sufficient data: e.g. if only 1 second has elapsed in a
        // 30-second window, the measured rate will be very high. Hence we assume
        // that the elapsed time is always N-1 complete windows plus whatever
        // fraction of the final window is complete.
        let mut total_elapsed_time_ms = now - self.stat.oldest_start_time_ms(now);
        // If explicit time window is provided, use that instead of the config value.
        let window_ms = if self.time_window_ms > 0 {
            self.time_window_ms
        } else {
            config.time_window_ms()
        };
        // Check how many full windows of data we have currently retained
        let num_full_windows = (total_elapsed_time_ms / window_ms) as i32;
        let min_full_windows = config.samples() - 1;

        // If the available windows are less than the minimum required, add the
        // difference to the totalElapsedTime
        if num_full_windows < min_full_windows {
            total_elapsed_time_ms += (min_full_windows - num_full_windows) as i64 * window_ms;
        }

        // If window size is being calculated at the exact beginning of the window
        // with no prior samples, the window size will result in a value of 0.
        // Calculation of rate over a window of size 0 is undefined, hence we assume
        // the minimum window size to be at least 1ms.
        total_elapsed_time_ms.max(1)
    }
}

impl Default for Rate {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for Rate {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.stat.record(config, value, time_ms);
    }
}

impl Measurable for Rate {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        let value = self.stat.measure(config, now);
        value / convert(self.window_size(config, now), self.unit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Time;
    use crate::common::metrics::internals::metrics_utils::convert;
    use crate::common::metrics::time::mock::MockTime;

    const EPS: f64 = 0.000001;

    // RateTest.testRateWithNoPriorAvailableSamples (@ParameterizedTest CsvSource).
    #[test]
    fn test_rate_with_no_prior_available_samples() {
        // {numSample, sampleWindowSizeSec}
        for (num_sample, sample_window_size_sec) in [(1, 1), (1, 11), (11, 1), (11, 11)] {
            let config = MetricConfig::new()
                .with_samples(num_sample)
                .with_time_window(sample_window_size_sec, TimeUnit::Seconds);
            let rate = Rate::new();
            let time = MockTime::new();
            let sample_value = 50.0;
            // record at beginning of the window
            rate.record(&config, sample_value, time.milliseconds());
            // forward time till almost the end of window
            let measurement_time = TimeUnit::Seconds.to_millis(sample_window_size_sec) - 1;
            time.sleep(measurement_time);
            // calculate rate at almost the end of window
            let observed_rate = rate.measure(&config, time.milliseconds());
            assert!(!observed_rate.is_nan());

            // The rate calculation assumes N-1 prior samples of value 0.
            let dummy_prior_samples_assumed = (num_sample - 1) as f64;
            let window_size = convert(measurement_time, TimeUnit::Seconds)
                + (dummy_prior_samples_assumed * sample_window_size_sec as f64);
            let expected_rate_per_sec = sample_value / window_size;
            assert!(
                (expected_rate_per_sec - observed_rate).abs() <= EPS,
                "numSample={num_sample}, window={sample_window_size_sec}: expected {expected_rate_per_sec}, got {observed_rate}"
            );
        }
    }

    // RateTest.testRateIsConsistentAfterTheFirstWindow
    #[test]
    fn test_rate_is_consistent_after_the_first_window() {
        let config = MetricConfig::new().with_time_window(1, TimeUnit::Seconds).with_samples(2);
        let rate = Rate::new();
        let time = MockTime::new();
        let steps = [0, 99, 100, 100, 100, 100, 100, 100, 100, 100, 100];

        // start the first window and record events at 0,99,199,...,999 ms
        for step_ms in steps {
            time.sleep(step_ms);
            rate.record(&config, 1.0, time.milliseconds());
        }

        // making a gap of 100 ms between windows
        time.sleep(101);

        // start the second window and record events at 0,99,199,...,999 ms
        for step_ms in steps {
            time.sleep(step_ms);
            rate.record(&config, 1.0, time.milliseconds());
            let observed_rate = rate.measure(&config, time.milliseconds());
            assert!((10.0..=11.0).contains(&observed_rate), "observed_rate={observed_rate}");
            // make sure measurements are repeatable with the same timestamp
            let measured_again = rate.measure(&config, time.milliseconds());
            assert_eq!(observed_rate, measured_again);
        }
    }

    #[test]
    fn unit_name_strips_last_two_chars() {
        // Faithful to Java Rate.unitName(): name().substring(0, length-2).toLowerCase().
        // "SECONDS" (7) -> [0..5] = "SECON" -> "secon".
        assert_eq!("secon", Rate::with_unit(TimeUnit::Seconds).unit_name());
        // "MILLISECONDS" (12) -> [0..10] = "MILLISECON" -> "millisecon".
        assert_eq!("millisecon", Rate::with_unit(TimeUnit::Milliseconds).unit_name());
    }
}
