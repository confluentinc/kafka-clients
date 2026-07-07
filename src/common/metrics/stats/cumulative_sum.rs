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

//! A cumulative total maintained over all time.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.CumulativeSum`.

use std::any::Any;
use std::fmt;

use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat};
use crate::common::utils::double_to_string;

/// A non-sampled cumulative total maintained over all time.
#[derive(Debug, Default)]
pub struct CumulativeSum {
    total: f64,
}

impl CumulativeSum {
    /// Creates a cumulative sum starting at `0`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cumulative sum starting at `value`.
    pub fn with_value(value: f64) -> Self {
        Self { total: value }
    }
}

impl Measurable for CumulativeSum {
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

impl Stat for CumulativeSum {
    fn record(&mut self, _config: &MetricConfig, value: f64, _now: i64) {
        self.total += value;
    }
}

impl MeasurableStat for CumulativeSum {}

impl fmt::Display for CumulativeSum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CumulativeSum(total={})", double_to_string(self.total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulates() {
        let config = MetricConfig::new();
        let mut sum = CumulativeSum::new();
        sum.record(&config, 2.0, 0);
        sum.record(&config, 3.0, 0);
        assert_eq!(sum.measure(&config, 0), 5.0);
    }

    #[test]
    fn test_initial_value() {
        let config = MetricConfig::new();
        let mut sum = CumulativeSum::with_value(10.0);
        assert_eq!(sum.measure(&config, 0), 10.0);
        sum.record(&config, 1.0, 0);
        assert_eq!(sum.measure(&config, 0), 11.0);
    }
}
