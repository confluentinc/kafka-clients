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

//! A metric tracked for monitoring purposes (`org.apache.kafka.common.Metric`).

use crate::common::MetricName;

/// The type-erased value of a metric.
///
/// Java's `Metric.metricValue()` returns `Object`, which may be a measurable
/// `Double` or a non-measurable gauge value of any type. This enum captures the
/// value kinds the consumer metrics produce.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    /// A measurable (or `Double`-valued gauge) reading.
    Double(f64),
    /// A string-valued gauge reading.
    String(String),
    /// A long-valued gauge reading.
    Long(i64),
    /// An integer-valued gauge reading.
    Int(i32),
}

impl MetricValue {
    /// Returns the value as an `f64` if it is a [`MetricValue::Double`].
    pub fn as_double(&self) -> Option<f64> {
        match self {
            MetricValue::Double(v) => Some(*v),
            _ => None,
        }
    }
}

/// A metric tracked for monitoring purposes.
pub trait Metric {
    /// A name for this metric.
    fn metric_name(&self) -> &MetricName;

    /// The value of the metric, which may be measurable or a non-measurable gauge.
    fn metric_value(&self) -> MetricValue;
}
