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

//! The definition of a single frequency within a [`Frequencies`] statistic.
//!
//! Translated from `org.apache.kafka.common.metrics.stats.Frequency`.
//!
//! [`Frequencies`]: crate::common::metrics::stats::Frequencies

use std::fmt;

use crate::common::MetricName;
use crate::common::utils::double_to_string;

/// A named frequency metric identifying a [`Frequencies`] bucket by its center
/// value.
///
/// [`Frequencies`]: crate::common::metrics::stats::Frequencies
#[derive(Clone, Debug)]
pub struct Frequency {
    name: MetricName,
    center_value: f64,
}

impl Frequency {
    /// Creates a frequency for the bucket centered on `center_value`.
    pub fn new(name: MetricName, center_value: f64) -> Self {
        Self { name, center_value }
    }

    /// The metric name.
    pub fn name(&self) -> &MetricName {
        &self.name
    }

    /// The center-point value identifying the bucket.
    pub fn center_value(&self) -> f64 {
        self.center_value
    }
}

impl fmt::Display for Frequency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Frequency(name={}, centerValue={})",
            self.name,
            double_to_string(self.center_value)
        )
    }
}
