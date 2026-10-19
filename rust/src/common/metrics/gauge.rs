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

//! A gauge metric is an instantaneous reading of a particular value
//! (`org.apache.kafka.common.metrics.Gauge`).

use crate::common::MetricValue;
use crate::common::metrics::{MetricConfig, MetricValueProvider};

/// A gauge metric is an instantaneous reading of a particular value.
///
/// In Java `Gauge<T> extends MetricValueProvider<T>` and declares nothing of
/// its own: a gauge is a functional interface whose single method is
/// [`MetricValueProvider::value`]. Because the public metric value is
/// type-erased, the Rust gauge produces a [`MetricValue`].
#[doc(alias = "org.apache.kafka.common.metrics.Gauge")]
pub trait Gauge: MetricValueProvider {}

/// A [`Gauge`] that calls `function`, standing in for a Java lambda
/// implementing `Gauge` (e.g. `(config, now) -> metrics.size()`): `function`
/// is [`MetricValueProvider::value`]. A method Java adds later as an overload
/// of `value` gets its own factory, `with_<parameter>`.
///
/// ```
/// use confluent_kafka::common::MetricValue;
/// use confluent_kafka::common::metrics::{gauge, Metrics};
///
/// let metrics = Metrics::new();
/// let name = metrics.metric_name("answer", "example");
/// metrics
///     .add_metric_with_metric_value_provider(name, Box::new(gauge::new(|_config, _now| MetricValue::Int(42))))
///     .unwrap();
/// ```
// stands in for a Java lambda implementing `Gauge` (CLAUDE.md §3)
#[doc(alias = "rust-only")]
pub fn new<F>(function: F) -> impl Gauge
where
    F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync,
{
    struct FunctionGauge<F>(F);

    impl<F> MetricValueProvider for FunctionGauge<F>
    where
        F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync,
    {
        fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
            (self.0)(config, now)
        }

        /// A function is Rust's lambda, which Java's `KafkaMetric.toString` omits.
        fn type_name(&self) -> Option<&'static str> {
            None
        }
    }

    impl<F> Gauge for FunctionGauge<F> where F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync {}

    FunctionGauge(function)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_calls_the_function_and_reports_no_type_name() {
        let gauge = new(|config, now| MetricValue::Long(now + i64::from(config.samples())));
        let config = MetricConfig::default();
        assert_eq!(gauge.value(&config, 40), MetricValue::Long(40 + i64::from(config.samples())));
        assert_eq!(gauge.type_name(), None);
    }
}
