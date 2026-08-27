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

//! An instantaneous value (`org.apache.kafka.common.metrics.stats.Value`).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// An instantaneous value.
///
/// `record` stores the last value (Java `this.value = value`); `measure` returns
/// it. The `f64` is held as its bit pattern in an [`AtomicU64`] so recording is
/// lock-free — the minimal Rust equivalent of Java guarding the field with the
/// sensor's `synchronized`. The stored value is bit-for-bit identical to Java's.
#[derive(Debug)]
pub struct Value {
    value: AtomicU64,
}

impl Value {
    /// Create a `Value` initialized to `0`, matching Java's `private double value = 0`.
    pub fn new() -> Self {
        Self { value: AtomicU64::new(0.0f64.to_bits()) }
    }
}

impl Default for Value {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for Value {
    fn record(&self, _config: &MetricConfig, value: f64, _time_ms: i64) {
        self.value.store(value.to_bits(), Ordering::SeqCst);
    }
}

impl Measurable for Value {
    fn measure(&self, _config: &MetricConfig, _now: i64) -> f64 {
        f64::from_bits(self.value.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_records_last() {
        let config = MetricConfig::new();
        let v = Value::new();
        assert_eq!(v.measure(&config, 0), 0.0);
        v.record(&config, 1.0, 0);
        assert_eq!(v.measure(&config, 0), 1.0);
        v.record(&config, 42.5, 1);
        assert_eq!(v.measure(&config, 1), 42.5);
        // Last value wins.
        v.record(&config, -3.0, 2);
        assert_eq!(v.measure(&config, 2), -3.0);
    }
}
