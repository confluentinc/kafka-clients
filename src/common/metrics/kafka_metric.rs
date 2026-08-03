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

//! A `KafkaMetric` holds a metric name, config and value provider
//! (`org.apache.kafka.common.metrics.KafkaMetric`).

use std::sync::{Arc, Mutex};

use crate::common::metrics::{MetricConfig, MetricValue, MetricValueProvider, Time};
use crate::common::{Metric, MetricName};

/// A metric tracked by the registry. Holds a [`MetricName`], a (mutable)
/// [`MetricConfig`] and a [`MetricValueProvider`].
///
/// In Java the `config` field is `volatile` and `metricValue`/`measurableValue`
/// read under a per-metric `lock` to make the stat read consistent with sensor
/// records. Because our simple stats are themselves lock-free (atomic), the
/// value read needs no extra lock; we still guard `config` with a `Mutex` so the
/// `config(new)` setter is observed atomically, matching Java's `synchronized`
/// setter + `volatile` read.
pub struct KafkaMetric {
    metric_name: MetricName,
    config: Mutex<Arc<MetricConfig>>,
    time: Arc<dyn Time>,
    metric_value_provider: MetricValueProvider,
}

impl KafkaMetric {
    /// Create a metric to monitor an object that provides metric values.
    ///
    /// * `metric_name` - The name of the metric
    /// * `value_provider` - The metric value provider associated with this metric
    /// * `config` - The configuration of the metric
    /// * `time` - The time instance to use with the metric
    pub fn new(
        metric_name: MetricName,
        value_provider: MetricValueProvider,
        config: Arc<MetricConfig>,
        time: Arc<dyn Time>,
    ) -> Self {
        Self {
            metric_name,
            config: Mutex::new(config),
            time,
            metric_value_provider: value_provider,
        }
    }

    /// Get the configuration of this metric.
    pub fn config(&self) -> Arc<MetricConfig> {
        Arc::clone(&self.config.lock().expect("metric config mutex poisoned"))
    }

    /// Set the metric config.
    pub fn set_config(&self, config: Arc<MetricConfig>) {
        *self.config.lock().expect("metric config mutex poisoned") = config;
    }

    /// Determine if the metric value provider is of type `Measurable`.
    pub fn is_measurable(&self) -> bool {
        matches!(self.metric_value_provider, MetricValueProvider::Measurable(_))
    }

    /// Take the metric and return the value, where the underlying metric provider
    /// should be a measurable. Returns the measured value if measurable,
    /// otherwise `0`.
    pub fn measurable_value(&self, time_ms: i64) -> f64 {
        let config = self.config();
        match &self.metric_value_provider {
            MetricValueProvider::Measurable(m) => m.measure(&config, time_ms),
            MetricValueProvider::Gauge(_) => 0.0,
        }
    }
}

impl Metric for KafkaMetric {
    fn metric_name(&self) -> &MetricName {
        &self.metric_name
    }

    fn metric_value(&self) -> MetricValue {
        let now = self.time.milliseconds();
        let config = self.config();
        self.metric_value_provider.value(&config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::stats::Value;
    use crate::common::metrics::{ClosureGauge, Stat, SystemTime};
    use std::collections::BTreeMap;

    fn name() -> MetricName {
        MetricName::new("m", "g", "", BTreeMap::new())
    }

    #[test]
    fn measurable_metric_value_and_measurable_value() {
        let value = Value::new();
        value.record(&MetricConfig::new(), 42.0, 0);
        let metric = KafkaMetric::new(
            name(),
            MetricValueProvider::Measurable(Box::new(value)),
            Arc::new(MetricConfig::new()),
            Arc::new(SystemTime),
        );
        assert!(metric.is_measurable());
        assert_eq!(metric.metric_value(), MetricValue::Double(42.0));
        assert_eq!(metric.measurable_value(0), 42.0);
    }

    #[test]
    fn gauge_metric_value_and_zero_measurable_value() {
        let gauge = ClosureGauge::new(|_, _| MetricValue::String("hello".to_string()));
        let metric = KafkaMetric::new(
            name(),
            MetricValueProvider::Gauge(Box::new(gauge)),
            Arc::new(MetricConfig::new()),
            Arc::new(SystemTime),
        );
        assert!(!metric.is_measurable());
        assert_eq!(metric.metric_value(), MetricValue::String("hello".to_string()));
        // Non-measurable returns 0 from measurable_value, matching Java.
        assert_eq!(metric.measurable_value(0), 0.0);
    }
}
