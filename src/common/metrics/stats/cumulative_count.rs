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

//! A non-sampled count maintained over all time
//! (`org.apache.kafka.common.metrics.stats.CumulativeCount`).

use crate::common::metrics::stats::CumulativeSum;
use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A non-sampled version of `WindowedCount` maintained over all time.
///
/// This is a special kind of [`CumulativeSum`] that always records `1` instead
/// of the provided value. In other words, it counts the number of `record`
/// invocations, instead of summing the recorded values.
///
/// Java implements this by extending `CumulativeSum` and overriding `record` to
/// pass `1`; Rust composes a `CumulativeSum` and forwards `1.0` on each record.
#[derive(Debug)]
pub struct CumulativeCount {
    inner: CumulativeSum,
}

impl CumulativeCount {
    /// Create a `CumulativeCount` initialized to `0`.
    pub fn new() -> Self {
        Self { inner: CumulativeSum::new() }
    }
}

impl Default for CumulativeCount {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for CumulativeCount {
    fn record(&self, config: &MetricConfig, _value: f64, time_ms: i64) {
        // Always record 1, mirroring Java's super.record(config, 1, timeMs).
        self.inner.record(config, 1.0, time_ms);
    }
}

impl Measurable for CumulativeCount {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.inner.measure(config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cumulative_count() {
        let config = MetricConfig::new();
        let c = CumulativeCount::new();
        assert_eq!(c.measure(&config, 0), 0.0);
        // Value passed is ignored; each record increments by 1.
        c.record(&config, 100.0, 0);
        c.record(&config, 0.0, 1);
        c.record(&config, -50.0, 2);
        assert_eq!(c.measure(&config, 2), 3.0);
    }
}
