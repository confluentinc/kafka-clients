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

//! A statistic whose single measurement feeds several named metrics.
//!
//! Translated from `org.apache.kafka.common.metrics.CompoundStat`.

use std::sync::{Arc, Mutex};

use crate::common::MetricName;
use crate::common::metrics::{Measurable, Stat};

/// A compound statistic where one measurement feeds many metrics, as with a
/// histogram that reports several percentiles.
pub trait CompoundStat: Stat {
    /// The named measurables reported by this compound statistic.
    ///
    /// Each returned measurable shares mutable state with this statistic, so
    /// values recorded here are reflected through the returned handles.
    fn stats(&self) -> Vec<NamedMeasurable>;
}

/// A named measurable produced by a [`CompoundStat`].
///
/// The measurable is shared (`Arc<Mutex<..>>`) so it stays in sync with the
/// compound statistic that produced it.
#[derive(Clone)]
pub struct NamedMeasurable {
    name: MetricName,
    stat: Arc<Mutex<dyn Measurable>>,
}

impl NamedMeasurable {
    /// Creates a named measurable.
    pub fn new(name: MetricName, stat: Arc<Mutex<dyn Measurable>>) -> Self {
        Self { name, stat }
    }

    /// The metric name.
    pub fn name(&self) -> &MetricName {
        &self.name
    }

    /// The shared measurable.
    pub fn stat(&self) -> &Arc<Mutex<dyn Measurable>> {
        &self.stat
    }
}
