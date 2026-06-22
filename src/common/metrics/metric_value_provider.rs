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

use crate::common::metrics::{Gauge, Measurable, MetricConfig, MetricValue};

/// Super-interface for [`Measurable`] or [`Gauge`] that provides metric values.
///
/// Java's `MetricValueProvider<T>` is generic over the produced value type
/// (`Double` for `Measurable`, `T` for `Gauge<T>`). Because the public
/// [`crate::common::Metric::metric_value`] returns an erased value, we model the
/// provider as an enum over the two concrete kinds rather than a generic trait;
/// the produced value is the type-erased [`MetricValue`].
pub enum MetricValueProvider {
    /// A measurable quantity (produces an `f64`).
    Measurable(Box<dyn Measurable>),
    /// A gauge (produces an arbitrary instantaneous value).
    Gauge(Box<dyn Gauge>),
}

impl MetricValueProvider {
    /// Returns the current value associated with this metric.
    ///
    /// * `config` - The configuration for this metric
    /// * `now` - The POSIX time in milliseconds the measurement is being taken
    pub fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        match self {
            MetricValueProvider::Measurable(m) => MetricValue::Double(m.measure(config, now)),
            MetricValueProvider::Gauge(g) => g.value(config, now),
        }
    }
}
