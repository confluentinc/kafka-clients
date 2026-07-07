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

//! The value a metric reports and the provider that computes it.
//!
//! Translated from `org.apache.kafka.common.metrics.MetricValueProvider`.

use std::sync::{Arc, Mutex};

use crate::common::metrics::{Gauge, Measurable};

/// A value reported by a metric.
///
/// Java's `Metric.metricValue()` returns `Object`; this enum stands in for that
/// erased value, carrying the concrete numeric or textual types that gauges and
/// measurables produce.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    /// A `double` value (from a measurable, or a gauge over a floating value).
    Double(f64),
    /// A `long` value.
    Long(i64),
    /// An `int` value.
    Int(i32),
    /// A string value.
    Str(String),
}

/// The provider backing a metric, either a [`Measurable`] statistic or a
/// [`Gauge`].
///
/// The provider is held behind `Arc<Mutex<..>>` so the same statistic instance
/// can be shared between a sensor (which records into it) and the metric that
/// reads it, matching the Java framework where a single lock guards shared
/// mutable statistics.
#[derive(Clone)]
pub enum MetricValueProvider {
    /// A measurable statistic producing a `double`.
    Measurable(Arc<Mutex<dyn Measurable>>),
    /// A gauge producing an arbitrary value.
    Gauge(Arc<Mutex<dyn Gauge>>),
}

impl MetricValueProvider {
    /// Wraps a freshly created measurable as a provider.
    pub fn from_measurable(measurable: impl Measurable + 'static) -> Self {
        Self::Measurable(Arc::new(Mutex::new(measurable)))
    }

    /// Wraps a freshly created gauge as a provider.
    pub fn from_gauge(gauge: impl Gauge + 'static) -> Self {
        Self::Gauge(Arc::new(Mutex::new(gauge)))
    }
}
