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

//! A measurable quantity that can be registered as a metric
//! (`org.apache.kafka.common.metrics.Measurable`).

use crate::common::metrics::MetricConfig;

/// A measurable quantity that can be registered as a metric.
///
/// # Translation notes
///
/// Blanket-implements [`MetricValueProvider`](crate::common::metrics::MetricValueProvider).
// Java's `Measurable extends MetricValueProvider<Double>` with a default
// `value` delegating to `measure`; the blanket impl makes every `Measurable` a
// `MetricValueProvider` whose `value` is `measure`.
#[doc(alias = "org.apache.kafka.common.metrics.Measurable")]
pub trait Measurable: Send + Sync {
    /// Measure this quantity and return the result as a double.
    ///
    /// # Arguments
    ///
    /// - `config` - The configuration for this metric
    /// - `now` - The POSIX time in milliseconds the measurement is being taken
    ///
    /// # Returns
    ///
    /// The measured value
    #[doc(alias = "org.apache.kafka.common.metrics.Measurable#measure")]
    fn measure(&self, config: &MetricConfig, now: i64) -> f64;

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
}

/// A boxed measurable is a measurable, so `Box<dyn Measurable>` is a
/// [`crate::common::metrics::MetricValueProvider`] too.
impl<M: Measurable + ?Sized> Measurable for Box<M> {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        (**self).measure(config, now)
    }

    fn type_name(&self) -> Option<&'static str> {
        (**self).type_name()
    }
}

/// A [`Measurable`] backed by a closure, mirroring Java's functional-interface
/// usage of `Measurable` (e.g. `(config, now) -> TimeUnit.SECONDS.convert(...)`
/// in `KafkaConsumerMetrics` / `HeartbeatMetricsManager`). The symmetric
/// counterpart of [`crate::common::metrics::gauge::new`].
// a Measurable from a closure, standing in for a Java lambda
#[doc(alias = "rust-only")]
pub struct ClosureMeasurable<F>(F)
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync;

impl<F> ClosureMeasurable<F>
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync,
{
    /// Wraps a closure as a measurable.
    ///
    /// # Arguments
    ///
    /// - `f` - the closure computing the value from the config and the time in milliseconds
    pub fn new(f: F) -> Self {
        Self(f)
    }
}

impl<F> std::fmt::Debug for ClosureMeasurable<F>
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync,
{
    // A closure has nothing to print.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureMeasurable").finish_non_exhaustive()
    }
}

impl<F> Measurable for ClosureMeasurable<F>
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync,
{
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        (self.0)(config, now)
    }

    // A closure is Rust's lambda, which Java's `KafkaMetric.toString` omits.
    fn type_name(&self) -> Option<&'static str> {
        None
    }
}
