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

//! A token-bucket rate-limiting statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.TokenBucket`.

use std::any::Any;
use std::fmt;

use crate::common::metrics::internals::metrics_utils::convert;
use crate::common::metrics::{Measurable, MeasurableStat, MetricConfig, Stat, TimeUnit};

/// A [`MeasurableStat`] implementing a token-bucket algorithm.
///
/// The quota bound defines the bucket's refill rate, while the maximum burst
/// (number of credits) is `samples * time_window_ms * quota_bound`. The quota
/// is considered exhausted when the remaining credits drop below zero.
#[derive(Debug)]
pub struct TokenBucket {
    unit: TimeUnit,
    tokens: f64,
    last_update_ms: i64,
}

impl TokenBucket {
    /// Creates a token bucket measured in seconds.
    pub fn new() -> Self {
        Self::with_unit(TimeUnit::Seconds)
    }

    /// Creates a token bucket measured in the given unit.
    pub fn with_unit(unit: TimeUnit) -> Self {
        Self { unit, tokens: 0.0, last_update_ms: 0 }
    }

    fn refill(&mut self, quota: f64, burst: f64, time_ms: i64) {
        self.tokens = burst.min(self.tokens + quota * convert(time_ms - self.last_update_ms, self.unit));
        self.last_update_ms = time_ms;
    }

    fn burst(&self, config: &MetricConfig, quota_bound: f64) -> f64 {
        config.samples() as f64 * convert(config.time_window_ms(), self.unit) * quota_bound
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Measurable for TokenBucket {
    fn measure(&mut self, config: &MetricConfig, time_ms: i64) -> f64 {
        let Some(quota) = config.quota() else {
            return i64::MAX as f64;
        };
        let quota_bound = quota.bound();
        let burst = self.burst(config, quota_bound);
        self.refill(quota_bound, burst, time_ms);
        self.tokens
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Stat for TokenBucket {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        let Some(quota) = config.quota() else {
            return;
        };
        let quota_bound = quota.bound();
        let burst = self.burst(config, quota_bound);
        self.refill(quota_bound, burst, time_ms);
        self.tokens = burst.min(self.tokens - value);
    }
}

impl MeasurableStat for TokenBucket {}

impl fmt::Display for TokenBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TokenBucket(unit={:?}, tokens={}, lastUpdateMs={})",
            self.unit, self.tokens, self.last_update_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Quota;
    use crate::common::metrics::stats::MockTime;

    fn config() -> MetricConfig {
        // Rate = 5 unit/sec, burst = 2s * 10 samples * 5 = ... => 100 credits.
        MetricConfig::new()
            .with_quota(Quota::upper_bound(5.0))
            .with_time_window(2, TimeUnit::Seconds)
            .with_samples(10)
            .unwrap()
    }

    // Start from a large time so the first refill fills the bucket to its burst.
    fn time() -> MockTime {
        MockTime::with_start(1_000_000_000)
    }

    #[test]
    fn test_record() {
        let config = config();
        let time = time();
        let mut tk = TokenBucket::new();

        // Expect 100 credits at T.
        assert!((tk.measure(&config, time.milliseconds()) - 100.0).abs() < 0.1);

        // Record 60 at T, expect 40 credits.
        tk.record(&config, 60.0, time.milliseconds());
        assert!((tk.measure(&config, time.milliseconds()) - 40.0).abs() < 0.1);

        // Advance 2s, record 5, expect 45 credits.
        time.sleep(2000);
        tk.record(&config, 5.0, time.milliseconds());
        assert!((tk.measure(&config, time.milliseconds()) - 45.0).abs() < 0.1);

        // Advance 2s, record 60, expect -5 credits.
        time.sleep(2000);
        tk.record(&config, 60.0, time.milliseconds());
        assert!((tk.measure(&config, time.milliseconds()) - (-5.0)).abs() < 0.1);
    }

    #[test]
    fn test_unrecord() {
        let config = config();
        let time = time();
        let mut tk = TokenBucket::new();

        // Expect 100 credits at T.
        assert!((tk.measure(&config, time.milliseconds()) - 100.0).abs() < 0.1);

        // Record -60 at T, expect 100 credits (capped at burst).
        tk.record(&config, -60.0, time.milliseconds());
        assert!((tk.measure(&config, time.milliseconds()) - 100.0).abs() < 0.1);

        // Advance 2s, record 60, expect 40 credits.
        time.sleep(2000);
        tk.record(&config, 60.0, time.milliseconds());
        assert!((tk.measure(&config, time.milliseconds()) - 40.0).abs() < 0.1);

        // Advance 2s, record -60, expect 100 credits.
        time.sleep(2000);
        tk.record(&config, -60.0, time.milliseconds());
        assert!((tk.measure(&config, time.milliseconds()) - 100.0).abs() < 0.1);
    }
}
