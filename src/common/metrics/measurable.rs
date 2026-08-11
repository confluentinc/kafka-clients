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
/// In Java this `extends MetricValueProvider<Double>` whose `value(...)`
/// delegates to `measure(...)`; here the [`crate::common::metrics::MetricValueProvider`]
/// enum performs that delegation, so a `Measurable` only needs to provide
/// `measure`.
pub trait Measurable: Send + Sync {
    /// Measure this quantity and return the result as an `f64`.
    ///
    /// * `config` - The configuration for this metric
    /// * `now` - The POSIX time in milliseconds the measurement is being taken
    fn measure(&self, config: &MetricConfig, now: i64) -> f64;
}

/// A [`Measurable`] backed by a closure, mirroring Java's functional-interface
/// usage of `Measurable` (e.g. `(config, now) -> TimeUnit.SECONDS.convert(...)`
/// in `KafkaConsumerMetrics` / `HeartbeatMetricsManager`). The symmetric
/// counterpart of [`crate::common::metrics::ClosureGauge`].
pub struct ClosureMeasurable<F>(F)
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync;

impl<F> ClosureMeasurable<F>
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync,
{
    /// Wrap a closure as a measurable.
    pub fn new(f: F) -> Self {
        Self(f)
    }
}

impl<F> Measurable for ClosureMeasurable<F>
where
    F: Fn(&MetricConfig, i64) -> f64 + Send + Sync,
{
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        (self.0)(config, now)
    }
}
