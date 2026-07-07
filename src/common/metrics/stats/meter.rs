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

//! A compound statistic combining a rate and a cumulative total.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Meter`.

use std::sync::{Arc, Mutex};

use crate::common::metrics::stats::{CumulativeSum, Rate, SampledStat, WindowedCount, WindowedSum};
use crate::common::metrics::{CompoundStat, MetricConfig, NamedMeasurable, Stat, TimeUnit};
use crate::common::{KafkaError, MetricName};

/// A compound statistic that reports both a rate metric and a cumulative total
/// metric.
pub struct Meter {
    rate_metric_name: MetricName,
    total_metric_name: MetricName,
    rate: Arc<Mutex<Rate>>,
    total: Arc<Mutex<CumulativeSum>>,
}

impl Meter {
    /// Creates a meter with seconds as the rate unit over a windowed sum.
    pub fn new(rate_metric_name: MetricName, total_metric_name: MetricName) -> Self {
        Self::with_unit_and_stat(
            TimeUnit::Seconds,
            Box::new(WindowedSum::new()),
            rate_metric_name,
            total_metric_name,
        )
        .expect("a windowed sum is a valid meter rate statistic")
    }

    /// Creates a meter with the given rate unit over a windowed sum.
    pub fn with_unit(unit: TimeUnit, rate_metric_name: MetricName, total_metric_name: MetricName) -> Self {
        Self::with_unit_and_stat(unit, Box::new(WindowedSum::new()), rate_metric_name, total_metric_name)
            .expect("a windowed sum is a valid meter rate statistic")
    }

    /// Creates a meter with seconds as the rate unit over the given statistic.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the statistic is not a
    /// windowed sum or count.
    pub fn with_stat(
        rate_stat: Box<dyn SampledStat>,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
    ) -> Result<Self, KafkaError> {
        Self::with_unit_and_stat(TimeUnit::Seconds, rate_stat, rate_metric_name, total_metric_name)
    }

    /// Creates a meter with the given rate unit over the given statistic.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the statistic is not a
    /// windowed sum or count.
    pub fn with_unit_and_stat(
        unit: TimeUnit,
        rate_stat: Box<dyn SampledStat>,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
    ) -> Result<Self, KafkaError> {
        if !(rate_stat.as_any().is::<WindowedSum>() || rate_stat.as_any().is::<WindowedCount>()) {
            return Err(KafkaError::illegal_argument(
                "Meter is supported only for WindowedCount or WindowedSum.",
            ));
        }
        Ok(Self {
            rate_metric_name,
            total_metric_name,
            rate: Arc::new(Mutex::new(Rate::with_unit_and_stat(unit, rate_stat))),
            total: Arc::new(Mutex::new(CumulativeSum::new())),
        })
    }
}

impl Stat for Meter {
    fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
        let total_value = {
            let mut rate = self.rate.lock().expect("meter rate lock poisoned");
            rate.record(config, value, time_ms);
            // A rate over a count records 1.0 into the total (as the count does).
            if rate.stat.as_any().is::<WindowedCount>() {
                1.0
            } else {
                value
            }
        };
        self.total
            .lock()
            .expect("meter total lock poisoned")
            .record(config, total_value, time_ms);
    }
}

impl CompoundStat for Meter {
    fn stats(&self) -> Vec<NamedMeasurable> {
        let total: Arc<Mutex<dyn crate::common::metrics::Measurable>> = self.total.clone();
        let rate: Arc<Mutex<dyn crate::common::metrics::Measurable>> = self.rate.clone();
        vec![
            NamedMeasurable::new(self.total_metric_name.clone(), total),
            NamedMeasurable::new(self.rate_metric_name.clone(), rate),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 0.0000001;

    fn metric_name(name: &str) -> MetricName {
        MetricName::new(name, "test", "", indexmap::IndexMap::new())
    }

    #[test]
    fn test_meter() {
        let rate_metric_name = metric_name("rate");
        let total_metric_name = metric_name("total");
        let mut meter = Meter::new(rate_metric_name.clone(), total_metric_name.clone());
        let stats = meter.stats();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[1].name(), &rate_metric_name);
        assert_eq!(stats[0].name(), &total_metric_name);
        let total_stat = Arc::clone(stats[0].stat());
        let rate_stat = Arc::clone(stats[1].stat());

        let config = MetricConfig::new();
        let mut next_value = 0.0;
        let mut expected_total = 0.0;
        let mut now: i64 = 0;
        let interval_ms: i64 = 100;
        let delta = 5.0;

        // Record values across multiple windows and verify rates are reported
        // over time windows and the total is cumulative.
        for i in 1..=100 {
            while now < i * 1000 {
                expected_total += next_value;
                meter.record(&config, next_value, now);
                now += interval_ms;
                next_value += delta;
            }

            let total_measured = total_stat.lock().unwrap().measure(&config, now);
            assert!((expected_total - total_measured).abs() < EPS);

            let window_size_ms = {
                let mut g = rate_stat.lock().unwrap();
                let rate = g.as_any_mut().downcast_mut::<Rate>().unwrap();
                rate.window_size(&config, now)
            };
            let window_start_ms = (now - window_size_ms).max(0);
            let mut sampled_total = 0.0;
            let mut prev_value = next_value - delta;
            let mut time_ms = now - 100;
            while time_ms >= window_start_ms {
                sampled_total += prev_value;
                time_ms -= interval_ms;
                prev_value -= delta;
            }
            let rate_measured = rate_stat.lock().unwrap().measure(&config, now);
            assert!((sampled_total * 1000.0 / window_size_ms as f64 - rate_measured).abs() < EPS);
        }
    }
}
