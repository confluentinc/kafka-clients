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
use crate::common::metrics::MetricConfig;

/// A gauge metric is an instantaneous reading of a particular value.
///
/// In Java `Gauge<T> extends MetricValueProvider<T>` and is a functional
/// interface producing a value of type `T`. Because the public metric value is
/// type-erased, the Rust gauge produces a [`MetricValue`].
#[doc(alias = "org.apache.kafka.common.metrics.Gauge")]
pub trait Gauge: Send + Sync {
    /// Returns the current value associated with this gauge.
    ///
    /// * `config` - The configuration for this metric
    /// * `now` - The POSIX time in milliseconds the measurement is being taken
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue;

    /// The provider's type name, as `KafkaMetric`'s `Display` reports it, or
    /// `None` for a provider that is a closure.
    ///
    /// No Java counterpart (DoD #7): Java's `KafkaMetric.toString` (Kafka 4.4,
    /// 46ad599a6e) reads `metricValueProvider.getClass().getName()` reflectively
    /// and omits it when the class `isSynthetic() || isAnonymousClass()` — a
    /// lambda. Rust has no reflection, so the provider reports it: the default is
    /// the implementing type's [`std::any::type_name`], and the closure adapters
    /// answer `None`, as Java's lambdas do.
    fn type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<Self>())
    }
}

/// A `Gauge` backed by a closure, mirroring Java's functional-interface usage
/// (e.g. `(config, now) -> metrics.size()`).
pub struct ClosureGauge<F>(F)
where
    F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync;

impl<F> ClosureGauge<F>
where
    F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync,
{
    /// Wrap a closure as a gauge.
    pub fn new(f: F) -> Self {
        Self(f)
    }
}

impl<F> Gauge for ClosureGauge<F>
where
    F: Fn(&MetricConfig, i64) -> MetricValue + Send + Sync,
{
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        (self.0)(config, now)
    }

    /// A closure is Rust's lambda, which Java's `KafkaMetric.toString` omits.
    fn type_name(&self) -> Option<&'static str> {
        None
    }
}
