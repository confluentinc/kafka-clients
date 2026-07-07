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

//! An instantaneous reading of an arbitrary value.
//!
//! Translated from `org.apache.kafka.common.metrics.Gauge` (and its
//! super-interface `MetricValueProvider`, which the Java source keeps distinct
//! only for the `Measurable` sub-interface).

use crate::common::metrics::{MetricConfig, MetricValue};

/// A gauge is an instantaneous reading of a particular value.
///
/// This trait also stands in for a bare `MetricValueProvider`: the two Java
/// interfaces are behaviorally identical from a metric's perspective, since the
/// only distinction that matters is whether a provider is also a
/// [`Measurable`](crate::common::metrics::Measurable).
pub trait Gauge: Send {
    /// The current value at the POSIX time `now` (milliseconds).
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue;
}
