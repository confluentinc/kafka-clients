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

//! Boilerplate-reducing builder for fetch sensors
//! (`org.apache.kafka.clients.consumer.internals.SensorBuilder`).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::KafkaError;
use crate::common::MetricNameTemplate;
use crate::common::metrics::stats::{Avg, Max, Meter, Min, SampledStat, Value};
use crate::common::metrics::{Metrics, RecordingLevel, Sensor};

/// `SensorBuilder` takes a bit of the boilerplate out of creating
/// [`Sensor`]s for recording metrics. Mirrors Java's `SensorBuilder`: if a
/// sensor with the given name already exists it is reused untouched
/// (`preexisting == true`), otherwise it is created and the `withXxx` calls add
/// stats to it.
pub(crate) struct SensorBuilder {
    metrics: Arc<Metrics>,
    sensor: Arc<Sensor>,
    preexisting: bool,
    tags: BTreeMap<String, String>,
}

impl SensorBuilder {
    /// Get-or-create a sensor with no tags, at the given recording level.
    ///
    /// Java's `SensorBuilder(Metrics, String)` always uses the default INFO
    /// recording level. We take an explicit level so the
    /// [`crate::consumer::internals::fetch_metrics_manager::FetchMetricsManager`]
    /// can create the partition-level lag/lead sensors at DEBUG (off by default)
    /// per the consumer perf constraint, while client-level sensors stay INFO.
    pub(crate) fn new(metrics: &Arc<Metrics>, name: &str, recording_level: RecordingLevel) -> Result<Self, KafkaError> {
        Self::with_tags(metrics, name, recording_level, BTreeMap::new())
    }

    /// Get-or-create a sensor with the given tags supplier, at the given
    /// recording level. Translates Java's `SensorBuilder(Metrics, String,
    /// Supplier<Map<String,String>>)`; the tags are only materialized when the
    /// sensor is newly created (matching Java, which calls the supplier only on
    /// the create path).
    pub(crate) fn with_tags(
        metrics: &Arc<Metrics>,
        name: &str,
        recording_level: RecordingLevel,
        tags: BTreeMap<String, String>,
    ) -> Result<Self, KafkaError> {
        match metrics.get_sensor(name) {
            Some(sensor) => Ok(Self { metrics: Arc::clone(metrics), sensor, preexisting: true, tags: BTreeMap::new() }),
            None => {
                let sensor = metrics.sensor_with_level(name, recording_level)?;
                Ok(Self { metrics: Arc::clone(metrics), sensor, preexisting: false, tags })
            },
        }
    }

    /// Add an [`Avg`] stat under the given template (if newly created).
    pub(crate) fn with_avg(self, name: &MetricNameTemplate) -> Result<Self, KafkaError> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance_with_tags(name, self.tags.clone())?;
            self.sensor.add(metric_name, Box::new(Avg::new()))?;
        }
        Ok(self)
    }

    /// Add a [`Min`] stat under the given template (if newly created).
    pub(crate) fn with_min(self, name: &MetricNameTemplate) -> Result<Self, KafkaError> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance_with_tags(name, self.tags.clone())?;
            self.sensor.add(metric_name, Box::new(Min::new()))?;
        }
        Ok(self)
    }

    /// Add a [`Max`] stat under the given template (if newly created).
    pub(crate) fn with_max(self, name: &MetricNameTemplate) -> Result<Self, KafkaError> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance_with_tags(name, self.tags.clone())?;
            self.sensor.add(metric_name, Box::new(Max::new()))?;
        }
        Ok(self)
    }

    /// Add a [`Value`] stat under the given template (if newly created).
    pub(crate) fn with_value(self, name: &MetricNameTemplate) -> Result<Self, KafkaError> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance_with_tags(name, self.tags.clone())?;
            self.sensor.add(metric_name, Box::new(Value::new()))?;
        }
        Ok(self)
    }

    /// Add a default (`WindowedSum`-backed) [`Meter`] producing the given rate
    /// and total metrics (if newly created).
    pub(crate) fn with_meter(
        self,
        rate_name: &MetricNameTemplate,
        total_name: &MetricNameTemplate,
    ) -> Result<Self, KafkaError> {
        if !self.preexisting {
            let rate_metric = self.metrics.metric_instance_with_tags(rate_name, self.tags.clone())?;
            let total_metric = self.metrics.metric_instance_with_tags(total_name, self.tags.clone())?;
            self.sensor.add_compound(Box::new(Meter::new(rate_metric, total_metric)))?;
        }
        Ok(self)
    }

    /// Add a [`Meter`] backed by the supplied rate [`SampledStat`] producing the
    /// given rate and total metrics (if newly created). Translates Java's
    /// `withMeter(SampledStat, MetricNameTemplate, MetricNameTemplate)`.
    pub(crate) fn with_meter_stat(
        self,
        sampled_stat: SampledStat,
        rate_name: &MetricNameTemplate,
        total_name: &MetricNameTemplate,
    ) -> Result<Self, KafkaError> {
        if !self.preexisting {
            let rate_metric = self.metrics.metric_instance_with_tags(rate_name, self.tags.clone())?;
            let total_metric = self.metrics.metric_instance_with_tags(total_name, self.tags.clone())?;
            self.sensor
                .add_compound(Box::new(Meter::with_stat(Arc::new(sampled_stat), rate_metric, total_metric)))?;
        }
        Ok(self)
    }

    /// Returns the built sensor.
    pub(crate) fn build(self) -> Arc<Sensor> {
        self.sensor
    }
}
