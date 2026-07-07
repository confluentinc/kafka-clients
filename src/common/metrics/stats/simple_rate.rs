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

//! A rate computed incrementally from the earliest reading.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.SimpleRate`.

use std::any::Any;

use crate::common::metrics::internals::metrics_utils::convert;
use crate::common::metrics::stats::Rate;
use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat};

/// A rate calculated incrementally from the elapsed time between the earliest
/// reading and now.
///
/// The first window is treated as fixed size, avoiding an artificially high
/// rate when the gap between readings is near zero.
///
/// In the Java source this extends `Rate`; here it wraps one and overrides the
/// window-size computation.
pub struct SimpleRate {
    rate: Rate,
}

impl SimpleRate {
    /// Creates a per-second simple rate over a windowed sum.
    pub fn new() -> Self {
        Self { rate: Rate::new() }
    }

    /// The size of the rate-computation window in milliseconds.
    pub fn window_size(&mut self, config: &MetricConfig, now: i64) -> i64 {
        self.rate.stat.purge_obsolete_samples(config, now);
        let elapsed = now - self.rate.stat.oldest(now).start_time_ms;
        elapsed.max(config.time_window_ms())
    }
}

impl Default for SimpleRate {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for SimpleRate {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.rate.record(config, value, time_ms);
    }
}

impl Measurable for SimpleRate {
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
        let value = self.rate.stat.measure(config, now);
        value / convert(self.window_size(config, now), self.rate.unit)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl MeasurableStat for SimpleRate {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::TimeUnit;
    use crate::common::metrics::stats::MockTime;

    #[test]
    fn test_first_window_is_fixed_size() {
        let mut rate = SimpleRate::new();
        let time = MockTime::new();
        // 1s window, so a single record read almost immediately uses the fixed
        // window rather than the tiny elapsed time.
        let config = MetricConfig::new()
            .with_time_window(1, TimeUnit::Seconds)
            .with_samples(2)
            .unwrap();
        rate.record(&config, 10.0, time.milliseconds());
        time.sleep(1);
        // window_size is at least the configured 1000ms window.
        assert_eq!(rate.window_size(&config, time.milliseconds()), 1000);
        // rate = 10 events / 1s = 10 per second.
        assert_eq!(rate.measure(&config, time.milliseconds()), 10.0);
    }
}
