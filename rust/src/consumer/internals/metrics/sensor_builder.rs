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
//! (`org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder`).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::Error;
use crate::common::MetricNameTemplate;
use crate::common::metrics::Sensor;
use crate::common::metrics::stats::{Avg, Max, Meter, Min, SampledStat, Value};
use crate::consumer::internals::metrics::MetricsLedger;

/// `SensorBuilder` takes a bit of the boilerplate out of creating
/// [`Sensor`]s for recording metrics. Mirrors Java's `SensorBuilder`: if a
/// sensor with the given name already exists it is reused untouched
/// (`preexisting == true`), otherwise it is created and the `withXxx` calls add
/// stats to it.
///
/// Since KAFKA-19542 (9a28bd23ad) the builder works through the manager's
/// [`MetricsLedger`], so every sensor and metric name it creates (and every
/// pre-existing sensor it reuses, through `getSensor`) is removed when the
/// manager closes. Java holds the ledger by reference; the builder borrows it.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder")]
pub(crate) struct SensorBuilder<'a> {
    metrics: &'a MetricsLedger,
    sensor: Arc<Sensor>,
    preexisting: bool,
    tags: BTreeMap<String, String>,
}

impl<'a> SensorBuilder<'a> {
    /// Get-or-create a sensor with no tags. Java's
    /// `SensorBuilder(MetricsLedger, String)`; like Java, the sensor is created
    /// at the default INFO recording level (`metrics.sensor(name)`).
    ///
    /// Every [`crate::consumer::internals::FetchMetricsManager`] sensor,
    /// including the per-partition lag/lead ones, is created at INFO for full
    /// Java parity.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#SensorBuilder")]
    pub(crate) fn new(metrics: &'a MetricsLedger, name: &str) -> Result<Self, Error> {
        Self::with_tags(metrics, name, BTreeMap::new)
    }

    /// Get-or-create a sensor with the given tags supplier. Translates Java's
    /// `SensorBuilder(MetricsLedger, String, Supplier<Map<String,String>>)`.
    ///
    /// `tags` is a closure, not a map, so it is invoked **only when the sensor is
    /// newly created** — exactly what Java's `Supplier` achieves
    /// (`SensorBuilder.java:51-64`). This matters because the per-topic and
    /// per-partition builders in
    /// [`crate::consumer::internals::FetchMetricsManager`]
    /// run on the per-fetch record path: with a by-value `BTreeMap` the caller
    /// had to build the map (plus its `String` keys and values) on *every* call
    /// and it was then discarded whenever the sensor already existed. At 200
    /// partitions and 100 polls/sec that is tens of thousands of wasted
    /// allocations per second that Java does not make.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#SensorBuilder")]
    pub(crate) fn with_tags<F>(metrics: &'a MetricsLedger, name: &str, tags: F) -> Result<Self, Error>
    where
        F: FnOnce() -> BTreeMap<String, String>,
    {
        match metrics.get_sensor(name) {
            Some(sensor) => Ok(Self { metrics, sensor, preexisting: true, tags: BTreeMap::new() }),
            None => {
                let sensor = metrics.sensor(name)?;
                Ok(Self { metrics, sensor, preexisting: false, tags: tags() })
            },
        }
    }

    /// Add an [`Avg`] stat under the given template (if newly created).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#withAvg")]
    pub(crate) fn with_avg(self, name: &MetricNameTemplate) -> Result<Self, Error> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance(name, self.tags.clone())?;
            self.sensor.add_metric_name(metric_name, Box::new(Avg::new()))?;
        }
        Ok(self)
    }

    /// Add a [`Min`] stat under the given template (if newly created).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#withMin")]
    pub(crate) fn with_min(self, name: &MetricNameTemplate) -> Result<Self, Error> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance(name, self.tags.clone())?;
            self.sensor.add_metric_name(metric_name, Box::new(Min::new()))?;
        }
        Ok(self)
    }

    /// Add a [`Max`] stat under the given template (if newly created).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#withMax")]
    pub(crate) fn with_max(self, name: &MetricNameTemplate) -> Result<Self, Error> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance(name, self.tags.clone())?;
            self.sensor.add_metric_name(metric_name, Box::new(Max::new()))?;
        }
        Ok(self)
    }

    /// Add a [`Value`] stat under the given template (if newly created).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#withValue")]
    pub(crate) fn with_value(self, name: &MetricNameTemplate) -> Result<Self, Error> {
        if !self.preexisting {
            let metric_name = self.metrics.metric_instance(name, self.tags.clone())?;
            self.sensor.add_metric_name(metric_name, Box::new(Value::new()))?;
        }
        Ok(self)
    }

    /// Add a default (`WindowedSum`-backed) [`Meter`] producing the given rate
    /// and total metrics (if newly created).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#withMeter")]
    pub(crate) fn with_meter(
        self,
        rate_name: &MetricNameTemplate,
        total_name: &MetricNameTemplate,
    ) -> Result<Self, Error> {
        if !self.preexisting {
            let rate_metric = self.metrics.metric_instance(rate_name, self.tags.clone())?;
            let total_metric = self.metrics.metric_instance(total_name, self.tags.clone())?;
            self.sensor.add(Box::new(Meter::new(rate_metric, total_metric)))?;
        }
        Ok(self)
    }

    /// Add a [`Meter`] backed by the supplied rate [`SampledStat`] producing the
    /// given rate and total metrics (if newly created). Translates Java's
    /// `withMeter(SampledStat, MetricNameTemplate, MetricNameTemplate)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#SensorBuilder")]
    pub(crate) fn with_meter_stat(
        self,
        sampled_stat: SampledStat,
        rate_name: &MetricNameTemplate,
        total_name: &MetricNameTemplate,
    ) -> Result<Self, Error> {
        if !self.preexisting {
            let rate_metric = self.metrics.metric_instance(rate_name, self.tags.clone())?;
            let total_metric = self.metrics.metric_instance(total_name, self.tags.clone())?;
            self.sensor.add(Box::new(Meter::with_rate_stat(
                Arc::new(sampled_stat),
                rate_metric,
                total_metric,
            )))?;
        }
        Ok(self)
    }

    /// Returns the built sensor.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder#build")]
    pub(crate) fn build(self) -> Arc<Sensor> {
        self.sensor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::{MetricConfig, Metrics, RecordingLevel};
    use crate::common::utils::SystemTime;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn metrics() -> MetricsLedger {
        let config = Arc::new(MetricConfig::new().set_record_level(RecordingLevel::Info));
        MetricsLedger::new(Arc::new(Metrics::with_default_config_reporters_time(
            config,
            Vec::new(),
            Arc::new(SystemTime),
        )))
    }

    /// Regression: the tags supplier must run only on the create path.
    ///
    /// `with_tags` used to take a `BTreeMap` by value, so the caller built the map
    /// — and its `String` keys and values — on every call, discarding it whenever
    /// the sensor already existed. Java passes a `Supplier<Map>` and invokes it
    /// only when creating (`SensorBuilder.java:51-64`). These builders sit on the
    /// per-fetch record path, so the difference is tens of thousands of
    /// allocations per second at 200 partitions.
    #[test]
    fn tags_supplier_runs_only_when_the_sensor_is_created() {
        let m = metrics();
        let calls = AtomicUsize::new(0);
        let tags_fn = || {
            calls.fetch_add(1, Ordering::SeqCst);
            let mut t = BTreeMap::new();
            t.insert("topic".to_string(), "t".to_string());
            t
        };

        // First call creates the sensor: the supplier must run exactly once.
        let _ = SensorBuilder::with_tags(&m, "s", tags_fn).expect("create");
        assert_eq!(1, calls.load(Ordering::SeqCst), "supplier should run on the create path");

        // Every subsequent call reuses the sensor and must NOT run the supplier.
        for _ in 0..10 {
            let _ = SensorBuilder::with_tags(&m, "s", tags_fn).expect("reuse");
        }
        assert_eq!(
            1,
            calls.load(Ordering::SeqCst),
            "supplier ran again on the reuse path — the by-value regression is back"
        );
    }
}
