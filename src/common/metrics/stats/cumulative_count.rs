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

//! A cumulative count of recordings maintained over all time.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.CumulativeCount`.

use std::any::Any;

use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat};

/// A non-sampled count that increments by one on each recording, regardless of
/// the recorded value.
///
/// In the Java source this extends `CumulativeSum`; here it is a standalone
/// statistic, so code that classifies a `CumulativeSum` must also account for
/// `CumulativeCount`.
#[derive(Debug, Default)]
pub struct CumulativeCount {
    total: f64,
}

impl CumulativeCount {
    /// Creates a cumulative count starting at `0`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Measurable for CumulativeCount {
    fn measure(&mut self, _config: &MetricConfig, _now: i64) -> f64 {
        self.total
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Stat for CumulativeCount {
    fn record(&mut self, _config: &MetricConfig, _value: f64, _time_ms: i64) {
        self.total += 1.0;
    }
}

impl MeasurableStat for CumulativeCount {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counts_recordings() {
        let config = MetricConfig::new();
        let mut count = CumulativeCount::new();
        count.record(&config, 100.0, 0);
        count.record(&config, 0.5, 0);
        count.record(&config, -3.0, 0);
        // Counts invocations, not the recorded values.
        assert_eq!(count.measure(&config, 0), 3.0);
    }
}
