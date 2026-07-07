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

//! The definition of a single percentile within a [`Percentiles`] statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Percentile`.
//!
//! [`Percentiles`]: crate::common::metrics::stats::Percentiles

use crate::common::MetricName;

/// A named percentile to be reported by a percentiles compound statistic.
#[derive(Clone, Debug)]
pub struct Percentile {
    name: MetricName,
    percentile: f64,
}

impl Percentile {
    /// Creates a percentile definition.
    pub fn new(name: MetricName, percentile: f64) -> Self {
        Self { name, percentile }
    }

    /// The metric name for this percentile.
    pub fn name(&self) -> &MetricName {
        &self.name
    }

    /// The percentile (0-100) to report.
    pub fn percentile(&self) -> f64 {
        self.percentile
    }
}
