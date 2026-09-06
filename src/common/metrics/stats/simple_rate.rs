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

//! A simple incremental rate (`org.apache.kafka.common.metrics.stats.SimpleRate`).

use crate::common::metrics::internals::metrics_utils::{TimeUnit, convert};
use crate::common::metrics::stats::Rate;
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A simple rate: the rate is incrementally calculated based on the elapsed time
/// between the earliest reading and now.
///
/// An exception is made for the first window, which is considered of fixed size.
/// This avoids the issue of an artificially high rate when the gap between
/// readings is close to 0.
///
/// In Java `SimpleRate extends Rate` and overrides only `windowSize`. Rust has
/// no inheritance, so we compose a `Rate` and re-implement `measure` using the
/// overridden `window_size` (Java's `measure` is on `Rate`, calling the virtual
/// `windowSize`).
pub struct SimpleRate {
    rate: Rate,
}

impl SimpleRate {
    /// Create a `SimpleRate` over seconds backed by a `WindowedSum`.
    pub fn new() -> Self {
        Self { rate: Rate::new() }
    }

    /// Compute the window size in milliseconds, overriding [`Rate::window_size`]:
    /// `max(elapsed, config.timeWindowMs())`.
    pub fn window_size(&self, config: &MetricConfig, now: i64) -> i64 {
        let stat = self.rate.stat();
        stat.purge_obsolete_samples(config, now);
        let elapsed = now - stat.oldest_start_time_ms(now);
        elapsed.max(config.time_window_ms())
    }
}

impl Default for SimpleRate {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for SimpleRate {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.rate.record(config, value, time_ms);
    }
}

impl Measurable for SimpleRate {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        // Rate.measure: value / convert(windowSize, unit). The unit is SECONDS
        // for the default SimpleRate constructor (matching Java).
        let value = self.rate.stat().measure(config, now);
        value / convert(self.window_size(config, now), TimeUnit::Seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Time;
    use crate::common::metrics::time::mock::MockTime;

    fn record(rate: &SimpleRate, config: &MetricConfig, time: &MockTime, value: f64) {
        rate.record(config, value, time.milliseconds());
    }
    fn measure(rate: &SimpleRate, config: &MetricConfig, time: &MockTime) -> f64 {
        rate.measure(config, time.milliseconds())
    }

    // MetricsTest.testSimpleRate
    #[test]
    fn test_simple_rate() {
        let rate = SimpleRate::new();
        let config = MetricConfig::new().time_window(1, TimeUnit::Seconds).set_samples(10);
        let time = MockTime::new();

        // In the first window the rate is a fraction of the whole (1s) window.
        // So when we record 1000 at t0, the rate should be 1000 until the window
        // completes, or more data is recorded.
        record(&rate, &config, &time, 1000.0);
        assert_eq!(1000.0, measure(&rate, &config, &time)); // 1000B / 0s -> first window fixed
        time.sleep(100);
        assert_eq!(1000.0, measure(&rate, &config, &time)); // 1000B / 0.1s
        time.sleep(100);
        assert_eq!(1000.0, measure(&rate, &config, &time)); // 1000B / 0.2s
        time.sleep(200);
        assert_eq!(1000.0, measure(&rate, &config, &time)); // 1000B / 0.4s

        // In the second (and subsequent) window(s), the rate will be in proportion
        // to the elapsed time.
        time.sleep(600);
        assert_eq!(1000.0, measure(&rate, &config, &time)); // 1000B / 1.0s
        time.sleep(200);
        assert_eq!(1000.0 / 1.2, measure(&rate, &config, &time)); // 1000B / 1.2s
        time.sleep(200);
        assert_eq!(1000.0 / 1.4, measure(&rate, &config, &time)); // 1000B / 1.4s

        // Adding another value, inside the same window should double the rate.
        record(&rate, &config, &time, 1000.0);
        assert_eq!(2000.0 / 1.4, measure(&rate, &config, &time)); // 2000B / 1.4s

        // Going over the next window should not change behaviour.
        time.sleep(1100);
        assert_eq!(2000.0 / 2.5, measure(&rate, &config, &time)); // 2000B / 2.5s
        record(&rate, &config, &time, 1000.0);
        assert_eq!(3000.0 / 2.5, measure(&rate, &config, &time)); // 3000B / 2.5s

        // Sleeping for another 6.5 windows also should be the same (tolerance 1).
        time.sleep(6500);
        assert!((3000.0 / 9.0 - measure(&rate, &config, &time)).abs() <= 1.0); // 3000B / 9s
        record(&rate, &config, &time, 1000.0);
        assert!((4000.0 / 9.0 - measure(&rate, &config, &time)).abs() <= 1.0); // 4000B / 9s

        // Going over the 10 window boundary should purge the first window's
        // values (1000). So the rate is calculated based on the oldest reading,
        // which is inside the second window, at 1.4s.
        time.sleep(1500);
        assert!(((4000.0 - 1000.0) / (10.5 - 1.4) - measure(&rate, &config, &time)).abs() <= 1.0);
        record(&rate, &config, &time, 1000.0);
        assert!(((5000.0 - 1000.0) / (10.5 - 1.4) - measure(&rate, &config, &time)).abs() <= 1.0);
    }
}
