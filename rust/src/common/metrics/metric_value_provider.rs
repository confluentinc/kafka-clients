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

//! Super-interface for `Measurable` or `Gauge` that provides metric values
//! (`org.apache.kafka.common.metrics.MetricValueProvider`).

use crate::common::MetricValue;
use crate::common::metrics::{Measurable, MetricConfig};

/// Super-interface for [`Measurable`] or [`Gauge`](crate::common::metrics::Gauge)
/// that provides metric values.
///
/// # Translation notes
///
/// It yields a [`MetricValue`].
// Java's `MetricValueProvider<T>` is generic over the produced value type
// (`Double` for `Measurable`, `T` for `Gauge<T>`); the public
// `Metric::metric_value` returns an erased value, so the Rust provider produces
// the type-erased `MetricValue`. Rust cannot default a supertrait's method from
// a subtrait, so every `Measurable` implements `MetricValueProvider` through a
// blanket impl whose `value` is `MetricValue::Double` of `measure`.
#[doc(alias = "org.apache.kafka.common.metrics.MetricValueProvider")]
pub trait MetricValueProvider: Send + Sync {
    /// Returns the current value associated with this metric.
    ///
    /// # Arguments
    ///
    /// - `config` - The configuration for this metric
    /// - `now` - The POSIX time in milliseconds the measurement is being taken
    ///
    /// # Returns
    ///
    /// the current metric value
    #[doc(alias = "org.apache.kafka.common.metrics.MetricValueProvider#value")]
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue;

    /// Returns the provider's type name, as `KafkaMetric`'s `Display` reports
    /// it, or `None` for a provider that is a closure.
    ///
    /// Java's `KafkaMetric.toString` reads the provider's class name
    /// reflectively and omits it for a lambda. Rust has no reflection, so the
    /// provider reports it: the default is the implementing type's
    /// [`std::any::type_name`], and the closure adapters answer `None`.
    // stands in for Java's reflective `getClass().getName()` in `KafkaMetric.toString`
    #[doc(alias = "rust-only")]
    fn type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<Self>())
    }

    /// Returns this provider as a [`Measurable`], or `None` when it is not one.
    ///
    /// It stands in for Java's `metricValueProvider instanceof Measurable` in
    /// `KafkaMetric`, which a trait object cannot answer by itself. The
    /// [`Measurable`] blanket impl answers `Some`; nothing else should override
    /// it.
    // public only because a trait method cannot be narrower than its trait;
    // hidden, it stands in for Java's `instanceof Measurable` in `KafkaMetric`
    #[doc(hidden)]
    #[doc(alias = "rust-only")]
    fn as_measurable(&self) -> Option<&dyn Measurable> {
        None
    }
}

/// Java's `Measurable.value` default: the measured quantity as a double.
impl<M: Measurable> MetricValueProvider for M {
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        MetricValue::Double(self.measure(config, now))
    }

    fn type_name(&self) -> Option<&'static str> {
        Measurable::type_name(self)
    }

    fn as_measurable(&self) -> Option<&dyn Measurable> {
        Some(self)
    }
}

/// A [`MetricValueProvider`] that calls `function`, standing in for a Java
/// lambda implementing `MetricValueProvider`: `function` is
/// [`value`](MetricValueProvider::value). A method Java adds later as an
/// overload of `value` gets its own factory, `with_<parameter>`.
///
/// # Arguments
///
/// - `function` - computes the value from the config and the time in milliseconds
///
/// # Returns
///
/// The provider calling `function`.
///
/// ```
/// use confluent_kafka::common::MetricValue;
/// use confluent_kafka::common::metrics::{metric_value_provider, Metrics};
///
/// let metrics = Metrics::new();
/// let name = metrics.metric_name("uptime-ms", "example");
/// let provider = metric_value_provider::new(|_config, now| MetricValue::Double(now as f64));
/// metrics.add_metric_with_metric_value_provider(name, Box::new(provider)).unwrap();
/// ```
// stands in for a Java lambda implementing `MetricValueProvider`
#[doc(alias = "rust-only")]
pub fn new<F>(function: F) -> impl MetricValueProvider
where
    F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync,
{
    struct FunctionMetricValueProvider<F>(F);

    impl<F> MetricValueProvider for FunctionMetricValueProvider<F>
    where
        F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync,
    {
        fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
            (self.0)(config, now)
        }

        // A function is Rust's lambda, which Java's `KafkaMetric.toString` omits.
        fn type_name(&self) -> Option<&'static str> {
            None
        }
    }

    FunctionMetricValueProvider(function)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_calls_the_function_and_is_neither_named_nor_measurable() {
        let provider = new(|_config, now| MetricValue::Long(now * 2));
        assert_eq!(provider.value(&MetricConfig::default(), 21), MetricValue::Long(42));
        assert_eq!(provider.type_name(), None);
        assert!(provider.as_measurable().is_none());
    }
}
