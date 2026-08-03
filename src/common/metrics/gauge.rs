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

use crate::common::metrics::{MetricConfig, MetricValue};

/// A gauge metric is an instantaneous reading of a particular value.
///
/// In Java `Gauge<T> extends MetricValueProvider<T>` and is a functional
/// interface producing a value of type `T`. Because the public metric value is
/// type-erased, the Rust gauge produces a [`MetricValue`].
pub trait Gauge: Send + Sync {
    /// Returns the current value associated with this gauge.
    ///
    /// * `config` - The configuration for this metric
    /// * `now` - The POSIX time in milliseconds the measurement is being taken
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue;
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
}
