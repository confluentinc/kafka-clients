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

//! An instantaneous value statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Value`.

use std::any::Any;

use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat};

/// A statistic that reports the most recently recorded value.
#[derive(Debug, Default)]
pub struct Value {
    value: f64,
}

impl Value {
    /// Creates a value statistic initialized to `0`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Measurable for Value {
    fn measure(&mut self, _config: &MetricConfig, _now: i64) -> f64 {
        self.value
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Stat for Value {
    fn record(&mut self, _config: &MetricConfig, value: f64, _time_ms: i64) {
        self.value = value;
    }
}

impl MeasurableStat for Value {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_records_latest_value() {
        let config = MetricConfig::new();
        let mut value = Value::new();
        value.record(&config, 3.0, 0);
        assert_eq!(value.measure(&config, 0), 3.0);
        value.record(&config, 7.5, 1);
        assert_eq!(value.measure(&config, 1), 7.5);
    }
}
