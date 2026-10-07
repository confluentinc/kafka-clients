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

use crate::common::MetricValue;
use crate::common::metrics::{Measurable, MetricConfig, MetricValueProvider};
use crate::common::utils::Time;
use crate::common::{Error, Metric, MetricName};

/// A metric tracked by the registry. Holds a [`MetricName`], a (mutable)
/// [`MetricConfig`] and a [`MetricValueProvider`].
///
/// In Java the `config` field is `volatile` and `metricValue`/`measurableValue`
/// read under a per-metric `lock` to make the stat read consistent with sensor
/// records. Because our simple stats are themselves lock-free (atomic), the
/// value read needs no extra lock; we still guard `config` with a `Mutex` so the
/// `config(new)` setter is observed atomically, matching Java's `synchronized`
/// setter + `volatile` read.
#[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric")]
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
    ///
    /// Crate-private: it takes the non-public `Time`. Java keeps it public only
    /// "for testing"; users obtain metrics from `Metrics`.
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric#KafkaMetric")]
    pub(crate) fn new(
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
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric#config")]
    pub fn config(&self) -> Arc<MetricConfig> {
        Arc::clone(&self.config.lock().expect("metric config mutex poisoned"))
    }

    /// Set the metric config.
    pub fn set_config(&self, config: Arc<MetricConfig>) {
        *self.config.lock().expect("metric config mutex poisoned") = config;
    }

    /// Determine if the metric value provider is of type `Measurable`.
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric#isMeasurable")]
    pub fn is_measurable(&self) -> bool {
        matches!(self.metric_value_provider, MetricValueProvider::Measurable(_))
    }

    /// Get the underlying [`Measurable`] value provider.
    ///
    /// Returns [`Error::LocalIllegalState`] when the provider is a
    /// [`MetricValueProvider::Gauge`] instead — Java's `measurable()` throws
    /// `IllegalStateException("Not a measurable: " + class)` in that case
    /// (`KafkaMetric.java`). Per CLAUDE.md §12.2 an unchecked-but-recoverable
    /// Java exception becomes a `Result` rather than a panic.
    ///
    /// Java returns the provider so callers can compare it by identity (see
    /// `KafkaMetricTest.testIsMeasurable`); Rust returns a borrow of the boxed
    /// trait object, which is the closest equivalent — trait objects have no
    /// meaningful value equality, so callers assert on `is_ok()` / the measured
    /// value instead.
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric#measurable")]
    pub fn measurable(&self) -> Result<&dyn Measurable, Error> {
        match &self.metric_value_provider {
            MetricValueProvider::Measurable(m) => Ok(m.as_ref()),
            MetricValueProvider::Gauge(_) => Err(Error::local_illegal_state(
                "Not a measurable: the metric value provider is a Gauge",
            )),
        }
    }

    /// Take the metric and return the value, where the underlying metric provider
    /// should be a measurable. Returns the measured value if measurable,
    /// otherwise `0`.
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric#measurableValue")]
    pub(crate) fn measurable_value(&self, time_ms: i64) -> f64 {
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

impl std::fmt::Display for KafkaMetric {
    /// Returns a human-readable representation of this metric.
    ///
    /// The metric value provider is represented by its type name rather than its
    /// own state, to avoid dumping internal stat state (e.g. `SampledStat`'s
    /// samples) into logs, which could be verbose and is rarely useful for
    /// identifying which metric changed. A closure provider (Java's lambda or
    /// anonymous class) is omitted.
    ///
    /// Java's `toString()` names the provider's Java class
    /// (`org.apache.kafka.common.metrics.stats.Avg`); the Rust type name is the
    /// Rust path (`confluent_kafka::common::metrics::stats::avg::Avg`).
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetric#toString")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.metric_value_provider.type_name() {
            None => write!(f, "KafkaMetric [metricName={}]", self.metric_name),
            Some(type_name) => {
                write!(
                    f,
                    "KafkaMetric [metricName={}, metricValueProvider={}]",
                    self.metric_name, type_name
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::stats::Avg;
    use crate::common::metrics::stats::Value;
    use crate::common::metrics::{ClosureGauge, ClosureMeasurable, Stat};
    use crate::common::utils::MockTime;
    use crate::common::utils::SystemTime;
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
    // 7 of Java's 8 methods are translated below. `testConstructorWithNullProvider`
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
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testIsMeasurable")]
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
    // (CLAUDE.md §12.2). The message is asserted because error text is part of
    // the behavioral contract (definition-of-done.md #3).
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testIsMeasurableWithGaugeProvider")]
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
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testMeasurableValueReturnsZeroWhenNotMeasurable")]
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
    #[doc(
        alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testKafkaMetricAcceptsNonMeasurableNonGaugeProvider"
    )]
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

    /// Java's `METRIC_NAME_2`.
    fn metric_name_2() -> MetricName {
        MetricName::new(
            "request-latency-avg",
            "consumer-fetch-manager-metrics",
            "The average request latency in ms",
            BTreeMap::from([("client-id".to_string(), "consumer-1".to_string())]),
        )
    }

    /// Java's `testToStringOnLambdaOrAnonymousClass`.
    fn assert_to_string_on_lambda_or_anonymous_class(metric_value_provider: MetricValueProvider) {
        let metric = KafkaMetric::new(
            metric_name_2(),
            metric_value_provider,
            Arc::new(MetricConfig::new()),
            Arc::new(MockTime::new()),
        );
        assert_eq!(
            "KafkaMetric [metricName=MetricName [name=request-latency-avg, \
             group=consumer-fetch-manager-metrics, \
             description=The average request latency in ms, \
             tags={client-id=consumer-1}]]",
            metric.to_string()
        );
    }

    /// Verifies that `Display` produces a human-readable representation suitable for
    /// logging. Note that we skip the metric provider in this case.
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testToStringWithLambdaProvider")]
    fn test_to_string_with_lambda_provider() {
        let metric_value_provider = ClosureMeasurable::new(|_, _| 0.0);
        assert_to_string_on_lambda_or_anonymous_class(MetricValueProvider::Measurable(Box::new(metric_value_provider)));
    }

    /// Java's anonymous `Measurable` subclass. Rust has no anonymous classes: an
    /// inline provider is a closure behind one of the adapters, so this exercises
    /// the other adapter, `ClosureGauge`, which Java's `toString` likewise omits.
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testToStringWithAnonymousClassProvider")]
    fn test_to_string_with_anonymous_class_provider() {
        let metric_value_provider = ClosureGauge::new(|_, _| MetricValue::Double(0.0));
        assert_to_string_on_lambda_or_anonymous_class(MetricValueProvider::Gauge(Box::new(metric_value_provider)));
    }

    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.KafkaMetricTest#testToStringWithStatProvider")]
    fn test_to_string_with_stat_provider() {
        let avg = Avg::new();
        let metric = KafkaMetric::new(
            metric_name_2(),
            MetricValueProvider::Measurable(Box::new(avg)),
            Arc::new(MetricConfig::new()),
            Arc::new(MockTime::new()),
        );
        assert_eq!(
            "KafkaMetric [metricName=MetricName [name=request-latency-avg, \
             group=consumer-fetch-manager-metrics, \
             description=The average request latency in ms, \
             tags={client-id=consumer-1}], \
             metricValueProvider=confluent_kafka::common::metrics::stats::avg::Avg]",
            metric.to_string()
        );
    }

    /// A stat a `Sensor` registers is reported by its own type, not the sensor's
    /// internal sharing wrapper: Java registers the stat itself as the provider.
    #[test]
    fn test_to_string_names_a_sensor_stat_by_its_own_type() {
        let metrics = crate::common::metrics::Metrics::new();
        let sensor = metrics.sensor("s").unwrap();
        sensor
            .add_metric_name(metrics.metric_name("m", "g"), Box::new(Avg::new()))
            .unwrap();
        let metric = metrics.metric(&metrics.metric_name("m", "g")).unwrap();
        assert!(
            metric
                .to_string()
                .ends_with("metricValueProvider=confluent_kafka::common::metrics::stats::avg::Avg]"),
            "{metric}"
        );
    }
}
