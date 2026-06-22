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

//! A registry of sensors and metrics (`org.apache.kafka.common.metrics.Metrics`).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::common::metrics::internals::metrics_utils;
use crate::common::metrics::{
    ClosureGauge, Gauge, KafkaMetric, Measurable, MetricConfig, MetricValue, MetricValueProvider, MetricsReporter,
    RecordingLevel, Sensor, SystemTime, Time,
};
use crate::common::{KafkaError, Metric, MetricName, MetricNameTemplate};

/// The shared registry state (metrics map + reporters) that is referenced both
/// by [`Metrics`] and by [`Sensor`] (so a sensor can register its metrics).
///
/// Java keeps the metrics map and reporters on `Metrics` itself; the sensor
/// calls back into the registry via its `registry` field. We extract the shared
/// portion into an `Arc<MetricsShared>` so the back-reference does not form an
/// ownership cycle.
pub(crate) struct MetricsShared {
    metrics: Mutex<HashMap<MetricName, Arc<KafkaMetric>>>,
    reporters: Mutex<Vec<Arc<dyn MetricsReporter>>>,
}

impl MetricsShared {
    fn new(reporters: Vec<Arc<dyn MetricsReporter>>) -> Self {
        Self { metrics: Mutex::new(HashMap::new()), reporters: Mutex::new(reporters) }
    }

    /// Register a metric if not present, or return the already-existing metric
    /// with the same name. When a metric is newly registered, returns `None`.
    pub(crate) fn register_metric(&self, metric: Arc<KafkaMetric>) -> Option<Arc<KafkaMetric>> {
        let metric_name = metric.metric_name().clone();
        {
            let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
            if let Some(existing) = metrics.get(&metric_name) {
                return Some(Arc::clone(existing));
            }
            metrics.insert(metric_name, Arc::clone(&metric));
        }
        // Newly added metric: notify reporters (outside the metrics lock,
        // mirroring Java which holds the Metrics monitor but reporters are
        // separate objects).
        let reporters = self.reporters.lock().expect("reporters mutex poisoned");
        for reporter in reporters.iter() {
            reporter.metric_change(&metric);
        }
        None
    }

    /// Remove a metric if it exists and return it; notify reporters on removal.
    fn remove_metric(&self, metric_name: &MetricName) -> Option<Arc<KafkaMetric>> {
        let removed = {
            let mut metrics = self.metrics.lock().expect("metrics mutex poisoned");
            metrics.remove(metric_name)
        };
        if let Some(metric) = &removed {
            let reporters = self.reporters.lock().expect("reporters mutex poisoned");
            for reporter in reporters.iter() {
                reporter.metric_removal(metric);
            }
        }
        removed
    }
}

/// A registry of sensors and metrics.
///
/// A metrics registry is a global repository of metrics and sensors. Sensors
/// apply a sequence of values to associated metrics. This is the consumer-
/// relevant subset of Java's `Metrics`; JMX, the sensor-expiry scheduler thread,
/// and quota machinery are omitted (the expiry mechanism is exposed via
/// [`Metrics::expire_sensors`] so the consumer/tests can drive it explicitly).
pub struct Metrics {
    config: Arc<MetricConfig>,
    shared: Arc<MetricsShared>,
    sensors: Mutex<HashMap<String, Arc<Sensor>>>,
    // childrenSensors keyed by the parent sensor's name (Java keys by Sensor
    // identity; sensor names are unique in the registry so the name is an
    // equivalent stable key).
    children_sensors: Mutex<HashMap<String, Vec<Arc<Sensor>>>>,
    time: Arc<dyn Time>,
}

impl Metrics {
    /// Create a metrics repository with a default config, no reporters, and the
    /// system clock.
    pub fn new() -> Self {
        Self::with_config_reporters_time(Arc::new(MetricConfig::new()), Vec::new(), Arc::new(SystemTime))
    }

    /// Create a metrics repository with the supplied default config.
    pub fn with_config(default_config: Arc<MetricConfig>) -> Self {
        Self::with_config_reporters_time(default_config, Vec::new(), Arc::new(SystemTime))
    }

    /// Create a metrics repository with the supplied clock.
    pub fn with_time(time: Arc<dyn Time>) -> Self {
        Self::with_config_reporters_time(Arc::new(MetricConfig::new()), Vec::new(), time)
    }

    /// Create a metrics repository with a default config, reporters, and clock.
    pub fn with_config_reporters_time(
        default_config: Arc<MetricConfig>,
        reporters: Vec<Arc<dyn MetricsReporter>>,
        time: Arc<dyn Time>,
    ) -> Self {
        for reporter in &reporters {
            reporter.init(&[]);
        }
        let metrics = Metrics {
            config: default_config,
            shared: Arc::new(MetricsShared::new(reporters)),
            sensors: Mutex::new(HashMap::new()),
            children_sensors: Mutex::new(HashMap::new()),
            time,
        };
        // Java registers a "count" gauge in kafka-metrics-count group.
        let shared = Arc::clone(&metrics.shared);
        let count_gauge = ClosureGauge::new(move |_config, _now| {
            let n = shared.metrics.lock().expect("metrics mutex poisoned").len();
            MetricValue::Double(n as f64)
        });
        metrics
            .add_metric_with_provider(
                metrics.metric_name(
                    "count",
                    "kafka-metrics-count",
                    "total number of registered metrics",
                    BTreeMap::new(),
                ),
                None,
                MetricValueProvider::Gauge(Box::new(count_gauge)),
            )
            .expect("count metric registration should not fail on a fresh registry");
        metrics
    }

    /// Create a `MetricName` with the given name, group, description and tags,
    /// plus default tags specified in the metric configuration. Tags in `tags`
    /// take precedence over default tags with the same key.
    pub fn metric_name(
        &self,
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tags: BTreeMap<String, String>,
    ) -> MetricName {
        let mut combined = self.config.tags().clone();
        combined.extend(tags);
        MetricName::new(name, group, description, combined)
    }

    /// Create a `MetricName` with name and group (empty description, default tags).
    pub fn metric_name_group(&self, name: impl Into<String>, group: impl Into<String>) -> MetricName {
        self.metric_name(name, group, "", BTreeMap::new())
    }

    /// Create a `MetricName` from `key, value` tag pairs, plus default tags.
    /// Returns an error if the pairs are not in twos (Java
    /// `IllegalArgumentException`).
    pub fn metric_name_key_value(
        &self,
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        key_value: &[&str],
    ) -> Result<MetricName, KafkaError> {
        Ok(self.metric_name(name, group, description, metrics_utils::get_tags(key_value)?))
    }

    /// The default config of this registry.
    pub fn config(&self) -> &Arc<MetricConfig> {
        &self.config
    }

    /// Get the sensor with the given name if it exists.
    pub fn get_sensor(&self, name: &str) -> Option<Arc<Sensor>> {
        self.sensors.lock().expect("sensors mutex poisoned").get(name).cloned()
    }

    /// Get or create a sensor with the given unique name and no parents at INFO
    /// recording level.
    pub fn sensor(&self, name: &str) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(name, None, i64::MAX, RecordingLevel::Info, &[])
    }

    /// Get or create a sensor with the given name, recording level, and no parents.
    pub fn sensor_with_level(&self, name: &str, recording_level: RecordingLevel) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(name, None, i64::MAX, recording_level, &[])
    }

    /// Get or create a sensor with parents at INFO recording level.
    pub fn sensor_with_parents(&self, name: &str, parents: &[Arc<Sensor>]) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(name, None, i64::MAX, RecordingLevel::Info, parents)
    }

    /// Get or create a sensor with a config, expiration, recording level and parents.
    pub fn sensor_full(
        &self,
        name: &str,
        config: Option<Arc<MetricConfig>>,
        inactive_sensor_expiration_time_seconds: i64,
        recording_level: RecordingLevel,
        parents: &[Arc<Sensor>],
    ) -> Result<Arc<Sensor>, KafkaError> {
        if let Some(existing) = self.get_sensor(name) {
            return Ok(existing);
        }
        let sensor_config = config.unwrap_or_else(|| Arc::clone(&self.config));
        let sensor = Arc::new(Sensor::new(
            Some(Arc::clone(&self.shared)),
            name,
            parents.to_vec(),
            sensor_config,
            Arc::clone(&self.time),
            inactive_sensor_expiration_time_seconds,
            recording_level,
        )?);
        self.sensors
            .lock()
            .expect("sensors mutex poisoned")
            .insert(name.to_string(), Arc::clone(&sensor));
        {
            let mut children = self.children_sensors.lock().expect("children sensors mutex poisoned");
            for parent in parents {
                children.entry(parent.name().to_string()).or_default().push(Arc::clone(&sensor));
            }
        }
        Ok(sensor)
    }

    /// Remove a sensor (if it exists), its associated metrics, and its children.
    pub fn remove_sensor(&self, name: &str) {
        let sensor = self.get_sensor(name);
        let Some(sensor) = sensor else {
            return;
        };
        let mut child_sensors: Option<Vec<Arc<Sensor>>> = None;
        {
            let removed = {
                let mut sensors = self.sensors.lock().expect("sensors mutex poisoned");
                // Remove only if the mapping still points to this sensor.
                if sensors.get(name).map(Arc::as_ptr) == Some(Arc::as_ptr(&sensor)) {
                    sensors.remove(name)
                } else {
                    None
                }
            };
            if removed.is_some() {
                for metric in sensor.metrics() {
                    self.shared.remove_metric(metric.metric_name());
                }
                let mut children = self.children_sensors.lock().expect("children sensors mutex poisoned");
                child_sensors = children.remove(name);
                for parent in sensor.parents() {
                    if let Some(siblings) = children.get_mut(parent.name()) {
                        siblings.retain(|s| !Arc::ptr_eq(s, &sensor));
                    }
                }
            }
        }
        if let Some(children) = child_sensors {
            for child in children {
                self.remove_sensor(child.name());
            }
        }
    }

    /// Add a metric to monitor a measurable. This metric won't be associated
    /// with any sensor.
    pub fn add_metric(&self, metric_name: MetricName, measurable: Box<dyn Measurable>) -> Result<(), KafkaError> {
        self.add_metric_with_provider(metric_name, None, MetricValueProvider::Measurable(measurable))
    }

    /// Add a metric backed by a gauge. This metric won't be associated with any
    /// sensor.
    pub fn add_gauge(&self, metric_name: MetricName, gauge: Box<dyn Gauge>) -> Result<(), KafkaError> {
        self.add_metric_with_provider(metric_name, None, MetricValueProvider::Gauge(gauge))
    }

    /// Add a metric backed by a value provider with an optional config.
    pub fn add_metric_with_provider(
        &self,
        metric_name: MetricName,
        config: Option<Arc<MetricConfig>>,
        provider: MetricValueProvider,
    ) -> Result<(), KafkaError> {
        let metric_config = config.unwrap_or_else(|| Arc::clone(&self.config));
        let metric = Arc::new(KafkaMetric::new(
            metric_name.clone(),
            provider,
            metric_config,
            Arc::clone(&self.time),
        ));
        if self.shared.register_metric(metric).is_some() {
            return Err(KafkaError::illegal_argument(format!(
                "A metric named '{metric_name}' already exists, can't register another one."
            )));
        }
        Ok(())
    }

    /// Remove a metric if it exists and return it. `metric_removal` is invoked
    /// on each reporter when a metric is removed.
    pub fn remove_metric(&self, metric_name: &MetricName) -> Option<Arc<KafkaMetric>> {
        self.shared.remove_metric(metric_name)
    }

    /// Add a metrics reporter, initializing it with all existing metrics.
    pub fn add_reporter(&self, reporter: Arc<dyn MetricsReporter>) {
        let existing: Vec<Arc<KafkaMetric>> = self
            .shared
            .metrics
            .lock()
            .expect("metrics mutex poisoned")
            .values()
            .cloned()
            .collect();
        reporter.init(&existing);
        self.shared.reporters.lock().expect("reporters mutex poisoned").push(reporter);
    }

    /// Get all the metrics currently maintained, indexed by metric name.
    pub fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        self.shared.metrics.lock().expect("metrics mutex poisoned").clone()
    }

    /// Get a single metric by name.
    pub fn metric(&self, metric_name: &MetricName) -> Option<Arc<KafkaMetric>> {
        self.shared
            .metrics
            .lock()
            .expect("metrics mutex poisoned")
            .get(metric_name)
            .cloned()
    }

    /// The children of a parent sensor, keyed by the parent sensor.
    /// For testing use only (mirrors Java's package-private `childrenSensors()`).
    #[cfg(test)]
    pub(crate) fn children_sensors(&self, parent: &Arc<Sensor>) -> Option<Vec<Arc<Sensor>>> {
        self.children_sensors
            .lock()
            .expect("children sensors mutex poisoned")
            .get(parent.name())
            .cloned()
    }

    /// Create a `MetricName` from a template and tag pairs.
    pub fn metric_instance(&self, template: &MetricNameTemplate, key_value: &[&str]) -> Result<MetricName, KafkaError> {
        self.metric_instance_with_tags(template, metrics_utils::get_tags(key_value)?)
    }

    /// Create a `MetricName` from a template and a tags map.
    pub fn metric_instance_with_tags(
        &self,
        template: &MetricNameTemplate,
        tags: BTreeMap<String, String>,
    ) -> Result<MetricName, KafkaError> {
        // Check that the runtime tags + default config tags match the template tags.
        let mut runtime_tag_keys: std::collections::HashSet<String> = tags.keys().cloned().collect();
        runtime_tag_keys.extend(self.config.tags().keys().cloned());
        let template_tag_keys: std::collections::HashSet<String> = template.tags().iter().cloned().collect();
        if runtime_tag_keys != template_tag_keys {
            return Err(KafkaError::illegal_argument(format!(
                "For '{}', runtime-defined metric tags do not match the tags in the template. \
                 Runtime = {runtime_tag_keys:?} Template = {template_tag_keys:?}",
                template.name()
            )));
        }
        Ok(self.metric_name(template.name(), template.group(), template.description(), tags))
    }

    /// Iterate over every sensor and remove the ones that have expired.
    ///
    /// This is the explicit equivalent of Java's `ExpireSensorTask` /
    /// `metricsScheduler`; the consumer (or tests) drives it instead of a
    /// background thread.
    pub fn expire_sensors(&self) {
        let names: Vec<String> = self.sensors.lock().expect("sensors mutex poisoned").keys().cloned().collect();
        for name in names {
            let expired = self.get_sensor(&name).map(|s| s.has_expired()).unwrap_or(false);
            if expired {
                self.remove_sensor(&name);
            }
        }
    }

    /// Close this metrics repository, closing all reporters.
    pub fn close(&self) {
        let reporters = self.shared.reporters.lock().expect("reporters mutex poisoned");
        for reporter in reporters.iter() {
            reporter.close();
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MetricValue;
    use crate::common::metrics::stats::{CumulativeCount, CumulativeSum, Value};
    use crate::common::metrics::time::mock::MockTime;

    fn metrics_with_mock() -> (Metrics, Arc<MockTime>) {
        let time = Arc::new(MockTime::new());
        let metrics = Metrics::with_config_reporters_time(
            Arc::new(MetricConfig::new()),
            Vec::new(),
            Arc::clone(&time) as Arc<dyn Time>,
        );
        (metrics, time)
    }

    fn double_value(metric: &Arc<KafkaMetric>) -> f64 {
        match metric.metric_value() {
            MetricValue::Double(v) => v,
            other => panic!("expected double metric value, got {other:?}"),
        }
    }

    // MetricsTest.testMetricName
    #[test]
    fn test_metric_name() {
        let metrics = Metrics::new();
        let n1 = metrics
            .metric_name_key_value("name", "group", "description", &["key1", "value1", "key2", "value2"])
            .unwrap();
        let mut tags = BTreeMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        tags.insert("key2".to_string(), "value2".to_string());
        let n2 = metrics.metric_name("name", "group", "description", tags);
        assert_eq!(n1, n2, "metric names created in two different ways should be equal");

        // Creating a MetricName with an odd number of keyValue should fail.
        let err = metrics
            .metric_name_key_value("name", "group", "description", &["key1"])
            .unwrap_err();
        assert!(err.to_string().contains("keyValue needs to be specified in pairs"));
    }

    // SensorTest.testIdempotentAdd (Avg/WindowedSum substituted with M1 stats)
    #[test]
    fn test_idempotent_add() {
        let metrics = Metrics::new();
        let sensor = metrics.sensor("sensor").unwrap();

        assert!(
            sensor
                .add(metrics.metric_name_group("test-metric", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        // Adding the same metric to the same sensor is a no-op (returns true).
        assert!(
            sensor
                .add(metrics.metric_name_group("test-metric", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        // Adding the same metric to a DIFFERENT sensor is an error.
        let another = metrics.sensor("another-sensor").unwrap();
        let err = another
            .add(metrics.metric_name_group("test-metric", "test-group"), Box::new(Value::new()))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));

        // Adding a different metric with the same name is also a no-op.
        assert!(
            sensor
                .add(
                    metrics.metric_name_group("test-metric", "test-group"),
                    Box::new(CumulativeSum::new())
                )
                .unwrap()
        );

        // Still just the original metric registered on the sensor.
        assert_eq!(sensor.metrics().len(), 1);
    }

    // MetricsTest.testHierarchicalSensors (WindowedCount substituted with CumulativeCount)
    #[test]
    fn test_hierarchical_sensors() {
        let metrics = Metrics::new();
        let parent1 = metrics.sensor("test.parent1").unwrap();
        parent1
            .add(
                metrics.metric_name_group("test.parent1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let parent2 = metrics.sensor("test.parent2").unwrap();
        parent2
            .add(
                metrics.metric_name_group("test.parent2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child1 = metrics
            .sensor_with_parents("test.child1", &[Arc::clone(&parent1), Arc::clone(&parent2)])
            .unwrap();
        child1
            .add(
                metrics.metric_name_group("test.child1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child2 = metrics.sensor_with_parents("test.child2", &[Arc::clone(&parent1)]).unwrap();
        child2
            .add(
                metrics.metric_name_group("test.child2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let grandchild = metrics.sensor_with_parents("test.grandchild", &[Arc::clone(&child1)]).unwrap();
        grandchild
            .add(
                metrics.metric_name_group("test.grandchild.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();

        // Increment each sensor one time.
        parent1.record_occurrence();
        parent2.record_occurrence();
        child1.record_occurrence();
        child2.record_occurrence();
        grandchild.record_occurrence();

        let p1 = double_value(&parent1.metrics()[0]);
        let p2 = double_value(&parent2.metrics()[0]);
        let c1 = double_value(&child1.metrics()[0]);
        let c2 = double_value(&child2.metrics()[0]);
        let gc = double_value(&grandchild.metrics()[0]);

        // Each metric should have a count equal to one + its children's count.
        assert_eq!(1.0, gc);
        assert_eq!(1.0 + gc, c1);
        assert_eq!(1.0, c2);
        assert_eq!(1.0 + c1, p2);
        assert_eq!(1.0 + c1 + c2, p1);

        let p1_children = metrics.children_sensors(&parent1).unwrap();
        assert_eq!(p1_children.len(), 2);
        assert!(p1_children.iter().any(|s| Arc::ptr_eq(s, &child1)));
        assert!(p1_children.iter().any(|s| Arc::ptr_eq(s, &child2)));
        let p2_children = metrics.children_sensors(&parent2).unwrap();
        assert_eq!(p2_children.len(), 1);
        assert!(Arc::ptr_eq(&p2_children[0], &child1));
        assert!(metrics.children_sensors(&grandchild).is_none());
    }

    // MetricsTest.testBadSensorHierarchy
    #[test]
    fn test_bad_sensor_hierarchy() {
        let metrics = Metrics::new();
        let p = metrics.sensor("parent").unwrap();
        let c1 = metrics.sensor_with_parents("child1", &[Arc::clone(&p)]).unwrap();
        let c2 = metrics.sensor_with_parents("child2", &[Arc::clone(&p)]).unwrap();
        match metrics.sensor_with_parents("gc", &[c1, c2]) {
            Ok(_) => panic!("expected circular dependency error"),
            Err(err) => assert!(err.to_string().contains("Circular dependency")),
        }
    }

    // MetricsTest.testRemoveChildSensor
    #[test]
    fn test_remove_child_sensor() {
        let metrics = Metrics::new();
        let parent = metrics.sensor("parent").unwrap();
        let child = metrics.sensor_with_parents("child", &[Arc::clone(&parent)]).unwrap();

        let children = metrics.children_sensors(&parent).unwrap();
        assert_eq!(children.len(), 1);
        assert!(Arc::ptr_eq(&children[0], &child));

        metrics.remove_sensor("child");

        assert!(metrics.children_sensors(&parent).unwrap().is_empty());
    }

    // MetricsTest.testRemoveSensor (WindowedCount substituted with CumulativeCount)
    #[test]
    fn test_remove_sensor() {
        let metrics = Metrics::new();
        let size = metrics.metrics().len();
        let parent1 = metrics.sensor("test.parent1").unwrap();
        parent1
            .add(
                metrics.metric_name_group("test.parent1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let parent2 = metrics.sensor("test.parent2").unwrap();
        parent2
            .add(
                metrics.metric_name_group("test.parent2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child1 = metrics
            .sensor_with_parents("test.child1", &[Arc::clone(&parent1), Arc::clone(&parent2)])
            .unwrap();
        child1
            .add(
                metrics.metric_name_group("test.child1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child2 = metrics.sensor_with_parents("test.child2", &[Arc::clone(&parent2)]).unwrap();
        child2
            .add(
                metrics.metric_name_group("test.child2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let gchild1 = metrics.sensor_with_parents("test.gchild2", &[Arc::clone(&child2)]).unwrap();
        gchild1
            .add(
                metrics.metric_name_group("test.gchild2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();

        assert!(metrics.get_sensor("test.parent1").is_some());
        metrics.remove_sensor("test.parent1");
        assert!(metrics.get_sensor("test.parent1").is_none());
        assert!(
            metrics
                .metric(&metrics.metric_name_group("test.parent1.count", "grp1"))
                .is_none()
        );
        // child1's only path to removal is via parent1; it is removed too.
        assert!(metrics.get_sensor("test.child1").is_none());
        assert!(metrics.children_sensors(&parent1).is_none());
        assert!(
            metrics
                .metric(&metrics.metric_name_group("test.child1.count", "grp1"))
                .is_none()
        );

        assert!(metrics.get_sensor("test.gchild2").is_some());
        metrics.remove_sensor("test.gchild2");
        assert!(metrics.get_sensor("test.gchild2").is_none());
        assert!(metrics.children_sensors(&gchild1).is_none());
        assert!(
            metrics
                .metric(&metrics.metric_name_group("test.gchild2.count", "grp1"))
                .is_none()
        );

        assert!(metrics.get_sensor("test.child2").is_some());
        metrics.remove_sensor("test.child2");
        assert!(metrics.get_sensor("test.child2").is_none());
        assert!(metrics.children_sensors(&child2).is_none());
        assert!(
            metrics
                .metric(&metrics.metric_name_group("test.child2.count", "grp1"))
                .is_none()
        );

        assert!(metrics.get_sensor("test.parent2").is_some());
        metrics.remove_sensor("test.parent2");
        assert!(metrics.get_sensor("test.parent2").is_none());
        assert!(metrics.children_sensors(&parent2).is_none());
        assert!(
            metrics
                .metric(&metrics.metric_name_group("test.parent2.count", "grp1"))
                .is_none()
        );

        assert_eq!(size, metrics.metrics().len());
    }

    // MetricsTest.testRemoveMetric (WindowedCount substituted with CumulativeCount)
    #[test]
    fn test_remove_metric() {
        let metrics = Metrics::new();
        let size = metrics.metrics().len();
        metrics
            .add_metric(metrics.metric_name_group("test1", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();
        metrics
            .add_metric(metrics.metric_name_group("test2", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();

        assert!(metrics.remove_metric(&metrics.metric_name_group("test1", "grp1")).is_some());
        assert!(metrics.metric(&metrics.metric_name_group("test1", "grp1")).is_none());
        assert!(metrics.metric(&metrics.metric_name_group("test2", "grp1")).is_some());

        assert!(metrics.remove_metric(&metrics.metric_name_group("test2", "grp1")).is_some());
        assert!(metrics.metric(&metrics.metric_name_group("test2", "grp1")).is_none());

        assert_eq!(size, metrics.metrics().len());
    }

    // MetricsTest.testDuplicateMetricName (Avg/CumulativeSum substituted)
    #[test]
    fn test_duplicate_metric_name() {
        let metrics = Metrics::new();
        metrics
            .sensor("test")
            .unwrap()
            .add(metrics.metric_name_group("test", "grp1"), Box::new(Value::new()))
            .unwrap();
        let err = metrics
            .sensor("test2")
            .unwrap()
            .add(metrics.metric_name_group("test", "grp1"), Box::new(CumulativeSum::new()))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // MetricsTest.testRemoveInactiveMetrics (WindowedCount substituted; ExpireSensorTask → expire_sensors)
    #[test]
    fn test_remove_inactive_metrics() {
        let (metrics, time) = metrics_with_mock();

        let s1 = metrics.sensor_full("test.s1", None, 1, RecordingLevel::Info, &[]).unwrap();
        s1.add(
            metrics.metric_name_group("test.s1.count", "grp1"),
            Box::new(CumulativeCount::new()),
        )
        .unwrap();

        let s2 = metrics.sensor_full("test.s2", None, 3, RecordingLevel::Info, &[]).unwrap();
        s2.add(
            metrics.metric_name_group("test.s2.count", "grp1"),
            Box::new(CumulativeCount::new()),
        )
        .unwrap();

        metrics.expire_sensors();
        assert!(metrics.get_sensor("test.s1").is_some(), "Sensor test.s1 must be present");
        assert!(metrics.metric(&metrics.metric_name_group("test.s1.count", "grp1")).is_some());
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name_group("test.s2.count", "grp1")).is_some());

        time.sleep(1001);
        metrics.expire_sensors();
        assert!(
            metrics.get_sensor("test.s1").is_none(),
            "Sensor test.s1 should have been purged"
        );
        assert!(metrics.metric(&metrics.metric_name_group("test.s1.count", "grp1")).is_none());
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name_group("test.s2.count", "grp1")).is_some());

        // Record on s2; resets its clock so it is not purged at the 3s mark.
        s2.record_occurrence();
        time.sleep(2000);
        metrics.expire_sensors();
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name_group("test.s2.count", "grp1")).is_some());

        // After another 1001ms, the metric should be purged.
        time.sleep(1001);
        metrics.expire_sensors();
        assert!(
            metrics.get_sensor("test.s2").is_none(),
            "Sensor test.s2 should have been purged"
        );
        assert!(metrics.metric(&metrics.metric_name_group("test.s2.count", "grp1")).is_none());

        // After purging, it should be possible to recreate a metric.
        let s1 = metrics.sensor_full("test.s1", None, 1, RecordingLevel::Info, &[]).unwrap();
        s1.add(
            metrics.metric_name_group("test.s1.count", "grp1"),
            Box::new(CumulativeCount::new()),
        )
        .unwrap();
        assert!(metrics.get_sensor("test.s1").is_some(), "Sensor test.s1 must be present");
        assert!(metrics.metric(&metrics.metric_name_group("test.s1.count", "grp1")).is_some());
    }

    // The M1-relevant part of MetricsTest.testSimpleStats: the CumulativeSum row.
    #[test]
    fn test_simple_stats_cumulative() {
        let metrics = Metrics::new();
        let s2 = metrics.sensor("test.sensor2").unwrap();
        s2.add(metrics.metric_name_group("s2.total", "grp1"), Box::new(CumulativeSum::new()))
            .unwrap();
        s2.record(5.0);
        assert_eq!(
            5.0,
            double_value(&metrics.metric(&metrics.metric_name_group("s2.total", "grp1")).unwrap()),
            "s2 reflects the constant value"
        );

        // CumulativeCount counts invocations regardless of recorded value.
        let s = metrics.sensor("test.sensor").unwrap();
        s.add(
            metrics.metric_name_group("test.count", "grp1"),
            Box::new(CumulativeCount::new()),
        )
        .unwrap();
        for i in 0..10 {
            s.record(i as f64);
        }
        assert_eq!(
            10.0,
            double_value(&metrics.metric(&metrics.metric_name_group("test.count", "grp1")).unwrap()),
            "Count(0...9) = 10"
        );
    }

    // The kafka-metrics-count gauge is registered on construction.
    #[test]
    fn count_metric_registered_on_construction() {
        let metrics = Metrics::new();
        let count_name = metrics.metric_name(
            "count",
            "kafka-metrics-count",
            "total number of registered metrics",
            BTreeMap::new(),
        );
        let count_metric = metrics.metric(&count_name).expect("count metric must exist");
        // Initially only the count metric itself is registered.
        assert_eq!(double_value(&count_metric), 1.0);
        // Adding a metric increases the count.
        metrics
            .add_metric(metrics.metric_name_group("extra", "grp1"), Box::new(Value::new()))
            .unwrap();
        assert_eq!(double_value(&count_metric), 2.0);
    }

    // metric_instance validates tag-key match against the template.
    #[test]
    fn test_metric_instance() {
        use crate::common::MetricNameTemplate;
        use indexmap::IndexSet;
        let metrics = Metrics::new();
        let mut tag_names = IndexSet::new();
        tag_names.insert("client-id".to_string());
        let template = MetricNameTemplate::new("name", "group", "desc", tag_names);

        let name = metrics.metric_instance(&template, &["client-id", "client-1"]).unwrap();
        assert_eq!(name.name(), "name");
        assert_eq!(name.tags().get("client-id").map(String::as_str), Some("client-1"));

        // Wrong tag keys → error.
        let err = metrics.metric_instance(&template, &["wrong", "v"]).unwrap_err();
        assert!(err.to_string().contains("do not match the tags in the template"));
    }
}
