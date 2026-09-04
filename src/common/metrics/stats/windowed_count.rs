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

//! A sampled count of recordings (`org.apache.kafka.common.metrics.stats.WindowedCount`).

use crate::common::metrics::stats::sampled_stat::{Sample, SampledStat, SampledStatKind};
use crate::common::metrics::stats::windowed_sum::WindowedSumKind;
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A [`SampledStat`] that maintains a simple count of what it has seen. This is
/// a special kind of [`crate::common::metrics::stats::WindowedSum`] that always
/// records a value of `1` instead of the provided value. In other words, it
/// counts the number of `record` invocations, instead of summing the recorded
/// values.
///
/// See also [`crate::common::metrics::stats::CumulativeCount`] for a non-sampled
/// version of this metric.
pub struct WindowedCount {
    inner: SampledStat,
}

struct WindowedCountKind;

impl SampledStatKind for WindowedCountKind {
    fn update(&self, sample: &mut Sample, config: &MetricConfig, _value: f64, now: i64) {
        // super.update(sample, config, 1.0, now)
        WindowedSumKind.update(sample, config, 1.0, now);
    }

    fn combine(&self, samples: &[Sample], config: &MetricConfig, now: i64) -> f64 {
        WindowedSumKind.combine(samples, config, now)
    }

    // WindowedCount extends WindowedSum in Java, so it is both a WindowedSum
    // and a WindowedCount for the `instanceof` checks in Meter.
    fn is_windowed_sum(&self) -> bool {
        true
    }

    fn is_windowed_count(&self) -> bool {
        true
    }
}

impl WindowedCount {
    /// Create a `WindowedCount`.
    pub fn new() -> Self {
        Self { inner: SampledStat::new(0.0, Box::new(WindowedCountKind)) }
    }

    /// Consume this `WindowedCount`, yielding its backing [`SampledStat`].
    /// See [`crate::common::metrics::stats::WindowedSum::into_sampled_stat`].
    ///
    /// Used to build a count-based [`crate::common::metrics::stats::Meter`]
    /// (`Meter::new_rate_stat(WindowedCount::new().into_sampled_stat(), ...)`), as the
    /// consumer fetch/commit "occurrences" meters do. The production callers land
    /// with the FetchMetricsManager wiring in Phase M3; exercised here by the
    /// Meter tests. Kept `pub(crate)` API now to mirror Java's
    /// `new Meter(unit, new WindowedCount(), ...)`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_sampled_stat(self) -> SampledStat {
        self.inner
    }
}

impl Default for WindowedCount {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for WindowedCount {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.inner.record(config, value, time_ms);
    }
}

impl Measurable for WindowedCount {
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
    fn counts_invocations() {
        let config = MetricConfig::new();
        let c = WindowedCount::new();
        c.record(&config, 5.0, 0);
        c.record(&config, 99.0, 0);
        assert_eq!(2.0, c.measure(&config, 0));
    }

    // MetricsTest.testTimeWindowing
    #[test]
    fn test_time_windowing() {
        let count = WindowedCount::new();
        let config = MetricConfig::new().with_time_window(1, TimeUnit::Milliseconds).with_samples(2);
        let time = MockTime::new();
        count.record(&config, 1.0, time.milliseconds());
        time.sleep(1);
        count.record(&config, 1.0, time.milliseconds());
        assert_eq!(2.0, count.measure(&config, time.milliseconds()));
        time.sleep(1);
        count.record(&config, 1.0, time.milliseconds()); // oldest event times out
        assert_eq!(2.0, count.measure(&config, time.milliseconds()));
    }
}
