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

//! A compound stat combining a rate and a cumulative total
//! (`org.apache.kafka.common.metrics.stats.Meter`).

use std::sync::Arc;

use crate::common::MetricName;
use crate::common::metrics::internals::metrics_utils::TimeUnit;
use crate::common::metrics::stats::windowed_sum::WindowedSum;
use crate::common::metrics::stats::{CumulativeSum, Rate, SampledStat};
use crate::common::metrics::{CompoundStat, Measurable, MetricConfig, NamedMeasurable, Stat};

/// A compound stat that includes a rate metric and a cumulative total metric.
pub struct Meter {
    rate_metric_name: MetricName,
    total_metric_name: MetricName,
    rate: Arc<Rate>,
    total: Arc<CumulativeSum>,
    // Whether the rate's underlying stat is a WindowedCount (Java
    // `rate.stat instanceof WindowedCount`): then the total records 1.0.
    rate_stat_is_windowed_count: bool,
}

impl Meter {
    /// Construct a `Meter` with seconds as time unit, backed by a `WindowedSum`.
    pub fn new(rate_metric_name: MetricName, total_metric_name: MetricName) -> Self {
        Self::new_unit_rate_stat(
            TimeUnit::Seconds,
            Arc::new(WindowedSum::new().into_sampled_stat()),
            rate_metric_name,
            total_metric_name,
        )
    }

    /// Construct a `Meter` with the provided time unit, backed by a `WindowedSum`.
    pub fn new_unit(unit: TimeUnit, rate_metric_name: MetricName, total_metric_name: MetricName) -> Self {
        Self::new_unit_rate_stat(
            unit,
            Arc::new(WindowedSum::new().into_sampled_stat()),
            rate_metric_name,
            total_metric_name,
        )
    }

    /// Construct a `Meter` with seconds as time unit and a provided rate stat.
    pub fn new_rate_stat(
        rate_stat: Arc<SampledStat>,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
    ) -> Self {
        Self::new_unit_rate_stat(TimeUnit::Seconds, rate_stat, rate_metric_name, total_metric_name)
    }

    /// Construct a `Meter` with the provided time unit and rate stat.
    ///
    /// Panics if `rate_stat` is not a `WindowedSum`/`WindowedCount`, mirroring
    /// Java's `IllegalArgumentException` — this is a construction-time
    /// programming error (Meter is only meaningful with a windowed sum/count).
    pub fn new_unit_rate_stat(
        unit: TimeUnit,
        rate_stat: Arc<SampledStat>,
        rate_metric_name: MetricName,
        total_metric_name: MetricName,
    ) -> Self {
        assert!(
            rate_stat.is_windowed_sum(),
            "Meter is supported only for WindowedCount or WindowedSum."
        );
        let rate_stat_is_windowed_count = rate_stat.is_windowed_count();
        Self {
            rate_metric_name,
            total_metric_name,
            rate: Arc::new(Rate::new_unit_stat(unit, rate_stat)),
            total: Arc::new(CumulativeSum::new()),
            rate_stat_is_windowed_count,
        }
    }
}

impl Stat for Meter {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.rate.record(config, value, time_ms);
        // Total metrics with Count stat should record 1.0 (as recorded in the count)
        let total_value = if self.rate_stat_is_windowed_count { 1.0 } else { value };
        self.total.record(config, total_value, time_ms);
    }
}

impl CompoundStat for Meter {
    fn stats(&self) -> Vec<NamedMeasurable> {
        vec![
            NamedMeasurable::new(self.total_metric_name.clone(), Arc::clone(&self.total) as Arc<dyn Measurable>),
            NamedMeasurable::new(self.rate_metric_name.clone(), Arc::clone(&self.rate) as Arc<dyn Measurable>),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::stats::WindowedCount;
    use std::collections::BTreeMap;

    const EPS: f64 = 0.0000001;

    fn name(n: &str) -> MetricName {
        MetricName::new(n, "test", "", BTreeMap::new())
    }

    // MeterTest.testMeter
    #[test]
    fn test_meter() {
        let rate_metric_name = name("rate");
        let total_metric_name = name("total");
        let meter = Meter::new(rate_metric_name.clone(), total_metric_name.clone());
        let stats = meter.stats();
        assert_eq!(2, stats.len());
        let total = &stats[0];
        let rate = &stats[1];
        assert_eq!(&rate_metric_name, rate.name());
        assert_eq!(&total_metric_name, total.name());
        let rate_stat = rate.stat();
        let total_stat = total.stat();

        let config = MetricConfig::new();
        let mut next_value = 0.0;
        let mut expected_total = 0.0;
        let mut now: i64 = 0;
        let interval_ms = 100;
        let delta = 5.0;

        // Record values in multiple windows and verify that rates are reported for
        // time windows and that the total is cumulative.
        for i in 1..=100i64 {
            while now < i * 1000 {
                expected_total += next_value;
                meter.record(&config, next_value, now);
                now += interval_ms;
                next_value += delta;
            }
            assert!((expected_total - total_stat.measure(&config, now)).abs() <= EPS);
            let window_size_ms = meter.rate.window_size(&config, now);
            let window_start_ms = (now - window_size_ms).max(0);
            let mut sampled_total = 0.0;
            let mut prev_value = next_value - delta;
            let mut time_ms = now - 100;
            while time_ms >= window_start_ms {
                sampled_total += prev_value;
                time_ms -= interval_ms;
                prev_value -= delta;
            }
            assert!(
                (sampled_total * 1000.0 / window_size_ms as f64 - rate_stat.measure(&config, now)).abs() <= EPS,
                "i={i}"
            );
        }
    }

    #[test]
    fn record_routes_count_total_as_one() {
        let meter =
            Meter::new_rate_stat(Arc::new(WindowedCount::new().into_sampled_stat()), name("rate"), name("total"));
        let config = MetricConfig::new();
        meter.record(&config, 42.0, 0);
        meter.record(&config, 7.0, 0);
        // total is a count, so it records 1.0 per invocation -> 2.0
        let total = meter.stats()[0].stat();
        assert_eq!(2.0, total.measure(&config, 0));
    }

    #[test]
    #[should_panic(expected = "Meter is supported only for WindowedCount or WindowedSum.")]
    fn rejects_non_windowed_sum_stat() {
        use crate::common::metrics::stats::Max;
        // Build a SampledStat that is not a WindowedSum (Max) and feed it to Meter.
        let _ = Max::new(); // Max wraps a private SampledStat; build directly instead.
        // A SampledStat whose kind is not a windowed sum:
        use crate::common::metrics::stats::sampled_stat::{Sample, SampledStat, SampledStatKind};
        struct NotSum;
        impl SampledStatKind for NotSum {
            fn update(&self, _s: &mut Sample, _c: &MetricConfig, _v: f64, _t: i64) {}
            fn combine(&self, _s: &[Sample], _c: &MetricConfig, _n: i64) -> f64 {
                0.0
            }
        }
        let stat = Arc::new(SampledStat::new(0.0, Box::new(NotSum)));
        let _ = Meter::new_rate_stat(stat, name("rate"), name("total"));
    }
}
