// Copyright 2026 Confluent Inc.
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

//! Records the metrics and sensors a consumer metrics manager creates, so they
//! can all be removed on close
//! (`org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger`, KAFKA-19542).

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::common::metrics::{Measurable, MetricConfig, MetricValueProvider, Metrics, Sensor};
use crate::common::{Error, MetricName, MetricNameTemplate};

/// `MetricsLedger` records the [`MetricName`]s and [`Sensor`]s that are created
/// using the internal [`Metrics`] instance in a ledger. Then, in
/// [`close`](Self::close), the ledger is reviewed and each of the
/// [`MetricName`]s and [`Sensor`]s are removed from the underlying [`Metrics`]
/// instance.
///
/// Because `Metrics` is a `final` class, we cannot extend it in a delegation
/// pattern. Instead, we mimic the subset of APIs that are needed by the callers.
///
/// Rust differences from Java (`MetricsLedger.java:43-111`):
///
/// - The managers record through `&self` from several tasks (the fetch manager
///   is `Arc`-shared with the per-response aggregators), so both ledgers sit
///   behind a `std::sync::Mutex`. Every critical section is a single set
///   operation and never crosses an `.await`.
/// - Java's sensor ledger is a `HashSet<Sensor>`, which compares by identity
///   since `Sensor` does not override `equals`. This one holds sensor *names*.
///   The two are equivalent for what the ledger is used for: `close()` removes
///   each recorded sensor by `name()`, and a name maps to at most one live
///   sensor in `Metrics`. Holding names also keeps the reuse path of
///   [`get_sensor`](Self::get_sensor), which runs on every per-partition fetch
///   metric, free of allocation once the name is recorded.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger")]
pub(crate) struct MetricsLedger {
    metrics: Arc<Metrics>,
    metric_names: Mutex<HashSet<MetricName>>,
    sensors: Mutex<HashSet<String>>,
}

impl MetricsLedger {
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#MetricsLedger")]
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            metric_names: Mutex::new(HashSet::new()),
            sensors: Mutex::new(HashSet::new()),
        }
    }

    /// The underlying registry, for `FetchMetricsManager::metrics_for_test`,
    /// whose callers read metric values back from it. Test-only: Java's tests
    /// keep their own reference to the `Metrics` they pass in.
    #[cfg(test)]
    pub(crate) fn registry(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    fn record_metric_name(&self, metric_name: &MetricName) {
        let mut metric_names = self.metric_names.lock().expect("MetricsLedger metric names mutex poisoned");
        if !metric_names.contains(metric_name) {
            metric_names.insert(metric_name.clone());
        }
    }

    fn record_sensor(&self, name: &str) {
        let mut sensors = self.sensors.lock().expect("MetricsLedger sensors mutex poisoned");
        if !sensors.contains(name) {
            sensors.insert(name.to_string());
        }
    }

    /// Java `metricName(String name, String metricGroupName, String description)`:
    /// creates the name with the registry's default tags and records it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#metricName")]
    pub(crate) fn metric_name(&self, name: &str, metric_group_name: &str, description: &str) -> MetricName {
        let metric_name = self.metrics.metric_name_description(name, metric_group_name, description);
        self.record_metric_name(&metric_name);
        metric_name
    }

    /// Java `metricInstance(MetricNameTemplate template, Map<String, String> tags)`.
    ///
    /// Returns the registry's error when the tags do not match the template
    /// (Java's `IllegalArgumentException`), in which case nothing is recorded,
    /// as in Java where the throw skips the `add`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#metricInstance")]
    pub(crate) fn metric_instance(
        &self,
        template: &MetricNameTemplate,
        tags: BTreeMap<String, String>,
    ) -> Result<MetricName, Error> {
        let metric_name = self.metrics.metric_instance_tags(template, tags)?;
        self.record_metric_name(&metric_name);
        Ok(metric_name)
    }

    /// Java `addMetricIfAbsent(MetricName, MetricConfig, MetricValueProvider<?>)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#addMetricIfAbsent")]
    pub(crate) fn add_metric_if_absent(
        &self,
        metric_name: MetricName,
        config: Option<Arc<MetricConfig>>,
        metric_value_provider: MetricValueProvider,
    ) {
        self.record_metric_name(&metric_name);
        self.metrics.add_metric_if_absent(metric_name, config, metric_value_provider);
    }

    /// Java `addMetric(MetricName metricName, Measurable measurable)`.
    ///
    /// Returns the registry's duplicate-name error (Java's
    /// `IllegalArgumentException`); the name is recorded only on success, as in
    /// Java where the throw skips the `add`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#addMetric")]
    pub(crate) fn add_metric(&self, metric_name: MetricName, measurable: Box<dyn Measurable>) -> Result<(), Error> {
        self.metrics.add_metric_measurable(metric_name.clone(), measurable)?;
        self.record_metric_name(&metric_name);
        Ok(())
    }

    /// Java `removeMetric(MetricName metricName)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#removeMetric")]
    pub(crate) fn remove_metric(&self, metric_name: &MetricName) {
        self.metrics.remove_metric(metric_name);
        self.metric_names
            .lock()
            .expect("MetricsLedger metric names mutex poisoned")
            .remove(metric_name);
    }

    /// Java `sensor(String name)`: gets or creates the sensor (INFO recording
    /// level) and records it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#sensor")]
    pub(crate) fn sensor(&self, name: &str) -> Result<Arc<Sensor>, Error> {
        let sensor = self.metrics.sensor(name)?;
        self.record_sensor(name);
        Ok(sensor)
    }

    /// Java `getSensor(String name)`: records the sensor only if it exists.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#getSensor")]
    pub(crate) fn get_sensor(&self, name: &str) -> Option<Arc<Sensor>> {
        let sensor = self.metrics.get_sensor(name);
        if sensor.is_some() {
            self.record_sensor(name);
        }
        sensor
    }

    /// Java `removeSensor(String name)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#removeSensor")]
    pub(crate) fn remove_sensor(&self, name: &str) {
        // Java first calls `getSensor(name)`, which records the sensor, and then
        // drops it from the ledger: the net effect is that the name is not in
        // the ledger afterwards.
        self.metrics.remove_sensor(name);
        self.sensors.lock().expect("MetricsLedger sensors mutex poisoned").remove(name);
    }

    /// Java `close()` (`final`): removes every recorded sensor, then every
    /// recorded metric name, from the underlying [`Metrics`].
    ///
    /// The ledgers are taken out under their locks and the removals run after
    /// the locks are dropped, so a reporter's `metric_removal` never runs under
    /// them. Java leaves the sets populated; emptying them changes nothing
    /// observable, because removing an absent sensor or metric is a no-op.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger#close")]
    pub(crate) fn close(&self) {
        let sensors = std::mem::take(&mut *self.sensors.lock().expect("MetricsLedger sensors mutex poisoned"));
        for sensor in &sensors {
            self.metrics.remove_sensor(sensor);
        }

        let metric_names =
            std::mem::take(&mut *self.metric_names.lock().expect("MetricsLedger metric names mutex poisoned"));
        for metric_name in &metric_names {
            self.metrics.remove_metric(metric_name);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `MetricsLedgerTest`; the class is covered through
    //! `AbstractConsumerMetricsManagerTest.testCleanup`, translated in each
    //! manager's test module. These tests pin the ledger's own contract.

    use super::*;
    use crate::common::metrics::ClosureMeasurable;
    use crate::common::metrics::stats::Avg;

    fn non_meta_metric_count(metrics: &Metrics) -> usize {
        metrics.metrics().len()
    }

    #[test]
    fn close_removes_every_recorded_sensor_and_metric() {
        let metrics = Arc::new(Metrics::new());
        let initial = non_meta_metric_count(&metrics);
        let ledger = MetricsLedger::new(Arc::clone(&metrics));

        let sensor = ledger.sensor("s").expect("sensor");
        sensor
            .add_metric_name(ledger.metric_name("s-avg", "g", "avg"), Box::new(Avg::new()))
            .expect("add avg");
        ledger
            .add_metric(
                ledger.metric_name("gauge", "g", "gauge"),
                Box::new(ClosureMeasurable::new(|_config, _now| 1.0)),
            )
            .expect("add metric");
        assert_eq!(initial + 2, non_meta_metric_count(&metrics));

        ledger.close();
        assert_eq!(initial, non_meta_metric_count(&metrics));
        assert!(metrics.get_sensor("s").is_none());
    }

    #[test]
    fn get_sensor_records_a_sensor_created_elsewhere() {
        let metrics = Arc::new(Metrics::new());
        let other = metrics.sensor("other").expect("sensor");
        other
            .add_metric_name(metrics.metric_name("other-avg", "g"), Box::new(Avg::new()))
            .expect("add avg");
        let ledger = MetricsLedger::new(Arc::clone(&metrics));

        assert!(ledger.get_sensor("missing").is_none());
        assert!(ledger.get_sensor("other").is_some());

        ledger.close();
        assert!(metrics.get_sensor("other").is_none(), "getSensor records the sensor");
    }

    #[test]
    fn removed_entries_are_dropped_from_the_ledger() {
        let metrics = Arc::new(Metrics::new());
        let ledger = MetricsLedger::new(Arc::clone(&metrics));

        ledger.sensor("s").expect("sensor");
        ledger.remove_sensor("s");
        let gauge = ledger.metric_name("gauge", "g", "gauge");
        ledger
            .add_metric(gauge.clone(), Box::new(ClosureMeasurable::new(|_config, _now| 1.0)))
            .expect("add metric");
        ledger.remove_metric(&gauge);
        assert!(ledger.sensors.lock().unwrap().is_empty());
        assert!(ledger.metric_names.lock().unwrap().is_empty());

        // Re-created outside the ledger after removal: close must not touch it.
        metrics.sensor("s").expect("sensor");
        ledger.close();
        assert!(metrics.get_sensor("s").is_some());
    }

    #[test]
    fn add_metric_error_is_returned_and_not_recorded() {
        let metrics = Arc::new(Metrics::new());
        let name = metrics.metric_name("gauge", "g");
        metrics
            .add_metric_measurable(name.clone(), Box::new(ClosureMeasurable::new(|_config, _now| 1.0)))
            .expect("add metric");
        let ledger = MetricsLedger::new(Arc::clone(&metrics));

        let err = ledger
            .add_metric(name.clone(), Box::new(ClosureMeasurable::new(|_config, _now| 2.0)))
            .expect_err("duplicate");
        assert_eq!(
            format!("A metric named '{name}' already exists, can't register another one."),
            err.message()
        );
        ledger.close();
        assert!(metrics.metric(&name).is_some(), "a failed add is not recorded");
    }
}
