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

//! A concrete metric backed by a value provider.
//!
//! Translated from `org.apache.kafka.common.metrics.KafkaMetric`.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::common::metric::Metric;
use crate::common::metrics::{Measurable, MetricConfig, MetricValue, MetricValueProvider};
use crate::common::{KafkaError, MetricName};

/// A source of the current time in POSIX milliseconds.
///
/// Mirrors the injectable `Time` instance the Java metric holds so tests can
/// supply a mock clock.
pub type TimeSource = Arc<dyn Fn() -> i64 + Send + Sync>;

/// A metric that monitors a [`MetricValueProvider`].
///
/// The provider is shared behind a lock, so the same statistic can be recorded
/// into by a sensor while this metric reads it. In the Java source the lock is
/// a separate object passed to the constructor; here the provider's own mutex
/// serves that role.
#[derive(Clone)]
pub struct KafkaMetric {
    metric_name: MetricName,
    metric_value_provider: MetricValueProvider,
    config: Arc<Mutex<MetricConfig>>,
    time: TimeSource,
}

impl KafkaMetric {
    /// Creates a metric monitoring the given value provider.
    pub fn new(
        metric_name: MetricName,
        metric_value_provider: MetricValueProvider,
        config: MetricConfig,
        time: TimeSource,
    ) -> Self {
        Self { metric_name, metric_value_provider, config: Arc::new(Mutex::new(config)), time }
    }

    /// The configuration of this metric.
    pub fn config(&self) -> MetricConfig {
        self.config.lock().expect("metric config lock poisoned").clone()
    }

    /// Sets the metric configuration.
    ///
    /// Intended for server-side use.
    pub fn set_config(&self, config: MetricConfig) {
        *self.config.lock().expect("metric config lock poisoned") = config;
    }

    /// Whether the value provider is a [`Measurable`].
    pub fn is_measurable(&self) -> bool {
        matches!(self.metric_value_provider, MetricValueProvider::Measurable(_))
    }

    /// The underlying measurable value provider.
    ///
    /// Returns [`KafkaError::IllegalState`] if the provider is not a
    /// [`Measurable`].
    pub fn measurable(&self) -> Result<Arc<Mutex<dyn Measurable>>, KafkaError> {
        match &self.metric_value_provider {
            MetricValueProvider::Measurable(m) => Ok(Arc::clone(m)),
            MetricValueProvider::Gauge(_) => Err(KafkaError::illegal_state("Not a measurable")),
        }
    }

    /// The metric value where the provider is measurable, otherwise `0`.
    // Consumed by the metrics collector, which lands in a later phase.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn measurable_value(&self, time_ms: i64) -> f64 {
        let config = self.config();
        match &self.metric_value_provider {
            MetricValueProvider::Measurable(m) => m.lock().expect("measurable lock poisoned").measure(&config, time_ms),
            MetricValueProvider::Gauge(_) => 0.0,
        }
    }
}

impl fmt::Debug for KafkaMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The value provider and clock are not debug-printable, so summarize
        // the identity fields.
        f.debug_struct("KafkaMetric")
            .field("metric_name", &self.metric_name)
            .field("is_measurable", &self.is_measurable())
            .finish_non_exhaustive()
    }
}

impl Metric for KafkaMetric {
    fn metric_name(&self) -> &MetricName {
        &self.metric_name
    }

    fn metric_value(&self) -> MetricValue {
        let now = (self.time)();
        let config = self.config();
        match &self.metric_value_provider {
            MetricValueProvider::Measurable(m) => {
                MetricValue::Double(m.lock().expect("measurable lock poisoned").measure(&config, now))
            },
            MetricValueProvider::Gauge(g) => g.lock().expect("gauge lock poisoned").value(&config, now),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;
    use crate::common::metrics::Gauge;

    fn metric_name() -> MetricName {
        MetricName::new("name", "group", "description", indexmap::IndexMap::new())
    }

    fn zero_time() -> TimeSource {
        Arc::new(|| 0)
    }

    /// A measurable that always reports a constant value.
    struct ConstMeasurable(f64);

    impl Measurable for ConstMeasurable {
        fn measure(&mut self, _config: &MetricConfig, _now: i64) -> f64 {
            self.0
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    /// A gauge that always reports a constant value.
    struct ConstGauge(MetricValue);

    impl Gauge for ConstGauge {
        fn value(&self, _config: &MetricConfig, _now: i64) -> MetricValue {
            self.0.clone()
        }
    }

    #[test]
    fn test_is_measurable() {
        let metric = KafkaMetric::new(
            metric_name(),
            MetricValueProvider::from_measurable(ConstMeasurable(0.0)),
            MetricConfig::new(),
            zero_time(),
        );
        assert!(metric.is_measurable());
        // Java compares the provider by lambda identity, which cannot be
        // expressed here; instead confirm the measurable is recoverable and
        // reports the expected value.
        let measurable = metric.measurable().unwrap();
        assert_eq!(measurable.lock().unwrap().measure(&MetricConfig::new(), 0), 0.0);
    }

    #[test]
    fn test_is_measurable_with_gauge_provider() {
        let metric = KafkaMetric::new(
            metric_name(),
            MetricValueProvider::from_gauge(ConstGauge(MetricValue::Double(0.0))),
            MetricConfig::new(),
            zero_time(),
        );
        assert!(!metric.is_measurable());
        assert!(metric.measurable().is_err());
    }

    #[test]
    fn test_measurable_value_returns_zero_when_not_measurable() {
        let metric = KafkaMetric::new(
            metric_name(),
            MetricValueProvider::from_gauge(ConstGauge(MetricValue::Int(7))),
            MetricConfig::new(),
            zero_time(),
        );
        assert_eq!(metric.measurable_value(0), 0.0);
    }

    #[test]
    fn test_kafka_metric_accepts_non_measurable_non_gauge_provider() {
        let metric = KafkaMetric::new(
            metric_name(),
            MetricValueProvider::from_gauge(ConstGauge(MetricValue::Str("metric value provider".to_string()))),
            MetricConfig::new(),
            zero_time(),
        );
        assert_eq!(metric.metric_value(), MetricValue::Str("metric value provider".to_string()));
    }
}
