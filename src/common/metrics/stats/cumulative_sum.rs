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

//! A non-sampled cumulative total maintained over all time
//! (`org.apache.kafka.common.metrics.stats.CumulativeSum`).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::common::metrics::{Measurable, MetricConfig, Stat};

/// A non-sampled cumulative total maintained over all time. This is a
/// non-sampled version of `WindowedSum`.
///
/// See also [`crate::common::metrics::stats::CumulativeCount`] if you just want to
/// increment the value by 1 on each recording.
///
/// The running total `f64` is held as its bit pattern in an [`AtomicU64`] and
/// updated with a compare-exchange add loop. The arithmetic is the identical
/// IEEE-754 `f64` addition Java performs in `total += value`; the CAS only
/// serializes concurrent recorders, exactly as Java's sensor `synchronized` does.
#[derive(Debug)]
pub struct CumulativeSum {
    total: AtomicU64,
}

impl CumulativeSum {
    /// Create a `CumulativeSum` initialized to `0.0`.
    pub fn new() -> Self {
        Self::with_value(0.0)
    }

    /// Create a `CumulativeSum` initialized to `value`.
    pub fn with_value(value: f64) -> Self {
        Self { total: AtomicU64::new(value.to_bits()) }
    }
}

impl Default for CumulativeSum {
    fn default() -> Self {
        Self::new()
    }
}

impl Stat for CumulativeSum {
    fn record(&self, _config: &MetricConfig, value: f64, _now: i64) {
        let mut current = self.total.load(Ordering::Relaxed);
        loop {
            let updated = (f64::from_bits(current) + value).to_bits();
            match self
                .total
                .compare_exchange_weak(current, updated, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

impl Measurable for CumulativeSum {
    fn measure(&self, _config: &MetricConfig, _now: i64) -> f64 {
        f64::from_bits(self.total.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cumulative_sum() {
        let config = MetricConfig::new();
        let s = CumulativeSum::new();
        assert_eq!(s.measure(&config, 0), 0.0);
        s.record(&config, 5.0, 0);
        s.record(&config, 2.5, 1);
        s.record(&config, -1.0, 2);
        assert_eq!(s.measure(&config, 2), 6.5);
    }

    #[test]
    fn test_with_value_seed() {
        let config = MetricConfig::new();
        let s = CumulativeSum::with_value(10.0);
        assert_eq!(s.measure(&config, 0), 10.0);
        s.record(&config, 5.0, 0);
        assert_eq!(s.measure(&config, 0), 15.0);
    }
}
