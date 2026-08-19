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

use crate::common::metrics::{Measurable, MetricConfig, MetricValue, MetricValueProvider, Time};
use crate::common::{KafkaError, Metric, MetricName};

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

    /// Get the underlying [`Measurable`] value provider.
    ///
    /// Returns [`KafkaError::IllegalState`] when the provider is a
    /// [`MetricValueProvider::Gauge`] instead — Java's `measurable()` throws
    /// `IllegalStateException("Not a measurable: " + class)` in that case
    /// (`KafkaMetric.java`). Per CLAUDE.md §10.2 an unchecked-but-recoverable
    /// Java exception becomes a `Result` rather than a panic.
    ///
    /// Java returns the provider so callers can compare it by identity (see
    /// `KafkaMetricTest.testIsMeasurable`); Rust returns a borrow of the boxed
    /// trait object, which is the closest equivalent — trait objects have no
    /// meaningful value equality, so callers assert on `is_ok()` / the measured
    /// value instead.
    pub fn measurable(&self) -> Result<&dyn Measurable, KafkaError> {
        match &self.metric_value_provider {
            MetricValueProvider::Measurable(m) => Ok(m.as_ref()),
            MetricValueProvider::Gauge(_) => Err(KafkaError::illegal_state(
                "Not a measurable: the metric value provider is a Gauge",
            )),
        }
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
    use crate::common::metrics::time::mock::MockTime;
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

    // ---------------------------------------------------------------------
    // KafkaMetricTest.java translation.
    //
    // Java's fixture is `METRIC_NAME = new MetricName("name", "group",
    // "description", emptyMap())` and a `MockTime`; `name()` above plus
    // `MockTime` are the equivalents.
    //
    // 4 of Java's 5 methods are translated below. `testConstructorWithNullProvider`
    // is NOT translatable and is intentionally omitted: it asserts
    // `NullPointerException` when the value provider is `null`, but
    // `KafkaMetric::new` takes `MetricValueProvider` by value (not
    // `Option<MetricValueProvider>`), so a null provider is unrepresentable —
    // the Rust type system enforces at compile time what Java asserts at
    // runtime. There is no code path to test.
    // ---------------------------------------------------------------------

    // KafkaMetricTest.testIsMeasurable
    //
    // Java also asserts `assertEquals(metricValueProvider, metric.measurable())`
    // — reference identity on the provider. Rust trait objects have no value
    // equality, so we assert `measurable()` succeeds and that the returned
    // borrow measures the same value the provider was seeded with, which is the
    // behavioral content of the identity check.
    #[test]
    fn test_is_measurable() {
        let value = Value::new();
        value.record(&MetricConfig::new(), 7.0, 0);
        let metric = KafkaMetric::new(
            name(),
            MetricValueProvider::Measurable(Box::new(value)),
            Arc::new(MetricConfig::new()),
            Arc::new(MockTime::new()),
        );
        assert!(metric.is_measurable());
        let measurable = metric.measurable().expect("provider is a Measurable");
        assert_eq!(measurable.measure(&MetricConfig::new(), 0), 7.0);
    }

    // KafkaMetricTest.testIsMeasurableWithGaugeProvider
    //
    // Java: `assertFalse(metric.isMeasurable())` +
    // `assertThrows(IllegalStateException.class, metric::measurable)`. The Rust
    // `measurable()` returns `Err(IllegalState)` instead of throwing
    // (CLAUDE.md §10.2). The message is asserted because error text is part of
    // the behavioral contract (definition-of-done.md #3).
    #[test]
    fn test_is_measurable_with_gauge_provider() {
        let gauge = ClosureGauge::new(|_, _| MetricValue::Double(0.0));
        let metric = KafkaMetric::new(
            name(),
            MetricValueProvider::Gauge(Box::new(gauge)),
            Arc::new(MetricConfig::new()),
            Arc::new(MockTime::new()),
        );
        assert!(!metric.is_measurable());
        // `&dyn Measurable` is not `Debug`, so `expect_err` is unavailable.
        let err = match metric.measurable() {
            Ok(_) => panic!("a Gauge provider must not be measurable"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("Not a measurable"), "unexpected message: {err}");
    }

    // KafkaMetricTest.testMeasurableValueReturnsZeroWhenNotMeasurable
    #[test]
    fn test_measurable_value_returns_zero_when_not_measurable() {
        let time = Arc::new(MockTime::new());
        // Java's gauge is `Gauge<Integer> gauge = (c, now) -> 7` — a non-zero
        // value, so the asserted 0.0 comes from `measurableValue`'s
        // not-a-measurable branch and not from the gauge's own reading.
        let gauge = ClosureGauge::new(|_, _| MetricValue::Int(7));
        let metric = KafkaMetric::new(
            name(),
            MetricValueProvider::Gauge(Box::new(gauge)),
            Arc::new(MetricConfig::new()),
            Arc::clone(&time) as Arc<dyn Time>,
        );
        assert_eq!(metric.measurable_value(time.milliseconds()), 0.0);
    }

    // KafkaMetricTest.testKafkaMetricAcceptsNonMeasurableNonGaugeProvider
    //
    // Java's provider is a bare `MetricValueProvider<String>` — neither
    // `Measurable` nor `Gauge` — returning a `String`. Rust models
    // `MetricValueProvider` as a closed two-variant enum (see its rustdoc), so a
    // third kind of provider is unrepresentable by construction. The
    // behavioral content that survives is "a metric may carry a non-numeric
    // value and `metric_value()` returns it verbatim", which a
    // `MetricValue::String` gauge expresses exactly.
    #[test]
    fn test_kafka_metric_accepts_non_measurable_non_gauge_provider() {
        let gauge = ClosureGauge::new(|_, _| MetricValue::String("metric value provider".to_string()));
        let metric = KafkaMetric::new(
            name(),
            MetricValueProvider::Gauge(Box::new(gauge)),
            Arc::new(MetricConfig::new()),
            Arc::new(MockTime::new()),
        );
        assert_eq!(metric.metric_value(), MetricValue::String("metric value provider".to_string()));
    }
}
