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

//! A measurable quantity that can be registered as a metric.
//!
//! Translated from `org.apache.kafka.common.metrics.Measurable`.

use std::any::Any;

use crate::common::metrics::MetricConfig;

/// A quantity that produces a single floating-point measurement.
///
/// `measure` takes `&mut self` because sampled statistics prune expired
/// samples as part of measuring, which mutates their internal state.
///
/// The [`as_any`](Measurable::as_any) / [`as_any_mut`](Measurable::as_any_mut)
/// hooks let callers recover the concrete statistic type (for example, to tell
/// a cumulative sum apart from a windowed count), which the metrics collector
/// relies on to classify sums versus gauges.
pub trait Measurable: Send {
    /// Measures this quantity at the POSIX time `now` (milliseconds).
    fn measure(&mut self, config: &MetricConfig, now: i64) -> f64;

    /// Returns this value as `&dyn Any` for concrete-type recovery.
    fn as_any(&self) -> &dyn Any;

    /// Returns this value as `&mut dyn Any` for concrete-type recovery.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
