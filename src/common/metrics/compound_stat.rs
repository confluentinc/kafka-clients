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

//! A compound stat that feeds many metrics from a single measurement
//! (`org.apache.kafka.common.metrics.CompoundStat`).

use std::sync::Arc;

use crate::common::MetricName;
use crate::common::metrics::{Measurable, Stat};

/// A compound stat is a stat where a single measurement and associated data
/// structure feeds many metrics. This is the example for a histogram which has
/// many associated percentiles.
pub trait CompoundStat: Stat {
    /// The named measurable child metrics this compound stat exposes.
    fn stats(&self) -> Vec<NamedMeasurable>;
}

/// A named [`Measurable`] child of a [`CompoundStat`].
///
/// Mirrors `CompoundStat.NamedMeasurable`. The `stat` is shared (`Arc`) because
/// the compound stat records into the same object the child metric measures, so
/// a record must be reflected in the measured value.
#[derive(Clone)]
pub struct NamedMeasurable {
    name: MetricName,
    stat: Arc<dyn Measurable>,
}

impl NamedMeasurable {
    /// Create a `NamedMeasurable`.
    pub fn new(name: MetricName, stat: Arc<dyn Measurable>) -> Self {
        Self { name, stat }
    }

    /// The metric name of this child.
    pub fn name(&self) -> &MetricName {
        &self.name
    }

    /// The measurable backing this child.
    pub fn stat(&self) -> Arc<dyn Measurable> {
        Arc::clone(&self.stat)
    }
}
