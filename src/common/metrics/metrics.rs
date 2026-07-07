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

//! A registry of sensors and metrics.
//!
//! Translated from `org.apache.kafka.common.metrics.Metrics`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use indexmap::IndexMap;

use crate::common::metric::Metric;
use crate::common::metrics::internals::metrics_utils::get_tags;
use crate::common::metrics::kafka_metric::TimeSource;
use crate::common::metrics::sensor::{RecordingLevel, Sensor};
use crate::common::metrics::{
    KafkaMetric, KafkaMetricsContext, Measurable, MetricConfig, MetricValueProvider, MetricsContext, MetricsReporter,
};
use crate::common::{KafkaError, MetricName, MetricNameTemplate};

/// How often the (optional) background task purges inactive sensors.
const EXPIRY_PERIOD: Duration = Duration::from_secs(30);

/// The shared state of a metrics registry.
///
/// Held behind an `Arc`; sensors keep a `Weak` reference back so they can
/// register metrics without forming a reference cycle. The registry maps use
/// [`DashMap`] rather than a single guarded map so that the sensor lock can be
/// acquired before a registry shard without any global-lock ordering hazard,
/// and so metric-value reads never contend on a registry-wide lock.
pub(crate) struct MetricsCore {
    self_weak: Weak<MetricsCore>,
    config: MetricConfig,
    metrics: DashMap<MetricName, Arc<KafkaMetric>>,
    sensors: DashMap<String, Arc<Sensor>>,
    children_sensors: DashMap<String, Vec<Arc<Sensor>>>,
    reporters: Mutex<Vec<Arc<dyn MetricsReporter>>>,
    time: TimeSource,
}

impl MetricsCore {
    /// Registers a metric if absent, returning the existing metric with the
    /// same name or `None` when newly registered. Reporter `metric_change`
    /// callbacks fire outside the map lock so a reporter may re-enter the
    /// registry without deadlocking.
    pub(crate) fn register_metric(&self, metric: Arc<KafkaMetric>) -> Option<Arc<KafkaMetric>> {
        let name = metric.metric_name().clone();
        match self.metrics.entry(name) {
            Entry::Occupied(existing) => return Some(existing.get().clone()),
            Entry::Vacant(slot) => {
                slot.insert(metric.clone());
            },
        }
        let reporters = self.reporters.lock().expect("reporters lock poisoned").clone();
        for reporter in &reporters {
            reporter.metric_change(metric.clone());
        }
        None
    }

    /// Removes a metric if present, firing `metric_removal` for each reporter.
    pub(crate) fn remove_metric(&self, metric_name: &MetricName) -> Option<Arc<KafkaMetric>> {
        let removed = self.metrics.remove(metric_name).map(|(_, metric)| metric);
        if let Some(metric) = &removed {
            let reporters = self.reporters.lock().expect("reporters lock poisoned").clone();
            for reporter in &reporters {
                reporter.metric_removal(metric.clone());
            }
        }
        removed
    }

    fn add_metric_provider(
        &self,
        metric_name: MetricName,
        config: Option<MetricConfig>,
        provider: MetricValueProvider,
    ) -> Result<(), KafkaError> {
        let metric = Arc::new(KafkaMetric::new(
            metric_name.clone(),
            provider,
            config.unwrap_or_else(|| self.config.clone()),
            self.time.clone(),
        ));
        if self.register_metric(metric).is_some() {
            return Err(KafkaError::illegal_argument(format!(
                "A metric named '{metric_name}' already exists, can't register another one."
            )));
        }
        Ok(())
    }

    fn get_or_create_sensor(
        &self,
        name: &str,
        config: Option<MetricConfig>,
        inactive_sensor_expiration_time_seconds: i64,
        recording_level: RecordingLevel,
        parents: Vec<Arc<Sensor>>,
    ) -> Result<Arc<Sensor>, KafkaError> {
        match self.sensors.entry(name.to_string()) {
            Entry::Occupied(existing) => Ok(existing.get().clone()),
            Entry::Vacant(slot) => {
                let sensor = Sensor::new(
                    self.self_weak.clone(),
                    name,
                    parents.clone(),
                    config.unwrap_or_else(|| self.config.clone()),
                    self.time.clone(),
                    inactive_sensor_expiration_time_seconds,
                    recording_level,
                )?;
                slot.insert(sensor.clone());
                for parent in &parents {
                    self.children_sensors
                        .entry(parent.name().to_string())
                        .or_default()
                        .push(sensor.clone());
                }
                Ok(sensor)
            },
        }
    }

    fn remove_sensor(&self, name: &str) {
        let Some(sensor) = self.sensors.get(name).map(|entry| entry.clone()) else {
            return;
        };
        // Only remove if the map still holds this exact sensor.
        if self
            .sensors
            .remove_if(name, |_, current| Arc::ptr_eq(current, &sensor))
            .is_none()
        {
            return;
        }
        for metric in sensor.metrics() {
            self.remove_metric(metric.metric_name());
        }
        let child_sensors = self.children_sensors.remove(name).map(|(_, children)| children);
        for parent in sensor.parents() {
            if let Some(mut children) = self.children_sensors.get_mut(parent.name()) {
                children.retain(|child| !Arc::ptr_eq(child, &sensor));
            }
        }
        if let Some(children) = child_sensors {
            for child in children {
                self.remove_sensor(child.name());
            }
        }
    }

    /// Removes every sensor that has been inactive beyond its expiration
    /// window. Mirrors Java's `ExpireSensorTask.run`; callable directly so
    /// tests drive purging deterministically.
    pub(crate) fn expire_sensors(&self) {
        // Snapshot the names first — removing while iterating the map would
        // hold a shard lock across the removal.
        let names: Vec<String> = self.sensors.iter().map(|entry| entry.key().clone()).collect();
        for name in names {
            let expired = self.sensors.get(&name).is_some_and(|entry| entry.has_expired());
            if expired {
                self.remove_sensor(&name);
            }
        }
    }
}

/// A registry of sensors and metrics.
///
/// A metric is a named numerical measurement; a sensor is a handle used to
/// record measurements, with zero or more associated metrics.
pub struct Metrics {
    core: Arc<MetricsCore>,
    expiry_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// The measurable backing the built-in `kafka-metrics-count` metric; reports
/// the current number of registered metrics as a floating-point count.
struct MetricsCountMeasurable {
    core: Weak<MetricsCore>,
}

impl Measurable for MetricsCountMeasurable {
    fn measure(&mut self, _config: &MetricConfig, _now: i64) -> f64 {
        self.core.upgrade().map_or(0.0, |core| core.metrics.len() as f64)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn system_time_source() -> TimeSource {
    Arc::new(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    })
}

impl Metrics {
    /// Creates a metrics repository with no reporters, default config, and
    /// system time. Sensor expiration is disabled.
    pub fn new() -> Self {
        Self::new_with_options(MetricConfig::new(), Vec::new(), system_time_source(), false, &default_context())
    }

    /// Creates a metrics repository with the given default config.
    pub fn with_config(config: MetricConfig) -> Self {
        Self::new_with_options(config, Vec::new(), system_time_source(), false, &default_context())
    }

    /// Creates a metrics repository using the given time source.
    pub fn with_time(time: TimeSource) -> Self {
        Self::new_with_options(MetricConfig::new(), Vec::new(), time, false, &default_context())
    }

    /// Creates a metrics repository with the given default config and time
    /// source.
    pub fn with_config_and_time(config: MetricConfig, time: TimeSource) -> Self {
        Self::new_with_options(config, Vec::new(), time, false, &default_context())
    }

    /// Creates a metrics repository with reporters, optionally enabling the
    /// background purge of inactive sensors.
    pub fn new_with_expiration(
        config: MetricConfig,
        reporters: Vec<Arc<dyn MetricsReporter>>,
        time: TimeSource,
        enable_expiration: bool,
    ) -> Self {
        Self::new_with_options(config, reporters, time, enable_expiration, &default_context())
    }

    /// Creates a metrics repository with full control over config, reporters,
    /// time, expiration, and the initial metrics context.
    ///
    /// The 30-second sensor-expiration task is spawned only when
    /// `enable_expiration` is set *and* a Tokio runtime is available; otherwise
    /// [`expire_sensors`](Metrics::expire_sensors) can be driven manually. Plain
    /// clients construct the registry without expiration and need no runtime.
    pub fn new_with_options(
        config: MetricConfig,
        reporters: Vec<Arc<dyn MetricsReporter>>,
        time: TimeSource,
        enable_expiration: bool,
        metrics_context: &dyn MetricsContext,
    ) -> Self {
        let core = Arc::new_cyclic(|weak| MetricsCore {
            self_weak: weak.clone(),
            config,
            metrics: DashMap::new(),
            sensors: DashMap::new(),
            children_sensors: DashMap::new(),
            reporters: Mutex::new(reporters),
            time,
        });

        for reporter in core.reporters.lock().expect("reporters lock poisoned").iter() {
            reporter.context_change(metrics_context);
            reporter.init(&[]);
        }

        let count_name = build_metric_name(
            &core.config,
            "count",
            "kafka-metrics-count",
            "total number of registered metrics",
            IndexMap::new(),
        );
        let count_measurable = MetricsCountMeasurable { core: Arc::downgrade(&core) };
        core.add_metric_provider(count_name, None, MetricValueProvider::from_measurable(count_measurable))
            .expect("the built-in metrics-count metric is registered exactly once");

        let expiry_task = if enable_expiration {
            tokio::runtime::Handle::try_current().ok().map(|handle| {
                let weak = Arc::downgrade(&core);
                handle.spawn(async move {
                    let mut ticker = tokio::time::interval(EXPIRY_PERIOD);
                    ticker.tick().await; // consume the immediate first tick
                    loop {
                        ticker.tick().await;
                        match weak.upgrade() {
                            Some(core) => core.expire_sensors(),
                            None => break,
                        }
                    }
                })
            })
        } else {
            None
        };

        Metrics { core, expiry_task: Mutex::new(expiry_task) }
    }

    /// The default configuration used for metrics that don't override it.
    pub fn config(&self) -> &MetricConfig {
        &self.core.config
    }

    // ----- Metric names -----------------------------------------------------

    /// Creates a metric name with the default tags from the config.
    pub fn metric_name(&self, name: &str, group: &str) -> MetricName {
        build_metric_name(&self.core.config, name, group, "", IndexMap::new())
    }

    /// Creates a metric name with a description and the default config tags.
    pub fn metric_name_desc(&self, name: &str, group: &str, description: &str) -> MetricName {
        build_metric_name(&self.core.config, name, group, description, IndexMap::new())
    }

    /// Creates a metric name with tags; supplied tags take precedence over the
    /// default config tags.
    pub fn metric_name_tags(
        &self,
        name: &str,
        group: &str,
        description: &str,
        tags: IndexMap<String, String>,
    ) -> MetricName {
        build_metric_name(&self.core.config, name, group, description, tags)
    }

    /// Creates a metric name with tags and an empty description.
    pub fn metric_name_group_tags(&self, name: &str, group: &str, tags: IndexMap<String, String>) -> MetricName {
        build_metric_name(&self.core.config, name, group, "", tags)
    }

    /// Creates a metric name from key/value tag pairs. Fails with
    /// [`KafkaError::IllegalArgument`] on an odd number of arguments.
    pub fn metric_name_key_values(
        &self,
        name: &str,
        group: &str,
        description: &str,
        key_value: &[&str],
    ) -> Result<MetricName, KafkaError> {
        Ok(build_metric_name(
            &self.core.config,
            name,
            group,
            description,
            get_tags(key_value)?,
        ))
    }

    /// Builds a metric name from a template and tag pairs.
    pub fn metric_instance(&self, template: &MetricNameTemplate, key_value: &[&str]) -> Result<MetricName, KafkaError> {
        self.metric_instance_tags(template, get_tags(key_value)?)
    }

    /// Builds a metric name from a template and tags, verifying the runtime tag
    /// keys (plus config defaults) match the template's tag names.
    ///
    /// On a mismatch the error lists the offending key sets as `[a, b]`. The
    /// runtime keys are sorted (they come from an unordered set, so sorting is
    /// what keeps the message deterministic); the template keys are shown in
    /// their declared order.
    pub fn metric_instance_tags(
        &self,
        template: &MetricNameTemplate,
        tags: IndexMap<String, String>,
    ) -> Result<MetricName, KafkaError> {
        let mut runtime_tag_keys: HashSet<&str> = tags.keys().map(String::as_str).collect();
        runtime_tag_keys.extend(self.core.config.tags().keys().map(String::as_str));
        let template_tag_keys: HashSet<&str> = template.tags().iter().map(String::as_str).collect();
        if runtime_tag_keys != template_tag_keys {
            let mut runtime_sorted: Vec<&str> = runtime_tag_keys.iter().copied().collect();
            runtime_sorted.sort_unstable();
            let template_ordered: Vec<&str> = template.tags().iter().map(String::as_str).collect();
            return Err(KafkaError::illegal_argument(format!(
                "For '{}', runtime-defined metric tags do not match the tags in the template. Runtime = [{}] Template = [{}]",
                template.name(),
                runtime_sorted.join(", "),
                template_ordered.join(", "),
            )));
        }
        Ok(build_metric_name(
            &self.core.config,
            template.name(),
            template.group(),
            template.description(),
            tags,
        ))
    }

    // ----- Sensors ----------------------------------------------------------

    /// Returns the sensor with the given name if it exists.
    pub fn get_sensor(&self, name: &str) -> Option<Arc<Sensor>> {
        self.core.sensors.get(name).map(|entry| entry.clone())
    }

    /// Gets or creates a sensor with default recording level and no parents.
    pub fn sensor(&self, name: &str) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(name, None, i64::MAX, RecordingLevel::Info, Vec::new())
    }

    /// Gets or creates a sensor with the given parents (default recording
    /// level).
    pub fn sensor_with_parents(&self, name: &str, parents: Vec<Arc<Sensor>>) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(name, None, i64::MAX, RecordingLevel::Info, parents)
    }

    /// Gets or creates a sensor with the given config (default recording level,
    /// no parents).
    pub fn sensor_with_config(&self, name: &str, config: MetricConfig) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(name, Some(config), i64::MAX, RecordingLevel::Info, Vec::new())
    }

    /// Gets or creates a sensor with a config and inactivity-expiration window.
    pub fn sensor_with_expiration(
        &self,
        name: &str,
        config: Option<MetricConfig>,
        inactive_sensor_expiration_time_seconds: i64,
    ) -> Result<Arc<Sensor>, KafkaError> {
        self.sensor_full(
            name,
            config,
            inactive_sensor_expiration_time_seconds,
            RecordingLevel::Info,
            Vec::new(),
        )
    }

    /// Gets or creates a sensor with full control over its configuration.
    pub fn sensor_full(
        &self,
        name: &str,
        config: Option<MetricConfig>,
        inactive_sensor_expiration_time_seconds: i64,
        recording_level: RecordingLevel,
        parents: Vec<Arc<Sensor>>,
    ) -> Result<Arc<Sensor>, KafkaError> {
        self.core
            .get_or_create_sensor(name, config, inactive_sensor_expiration_time_seconds, recording_level, parents)
    }

    /// Removes a sensor (if it exists) along with its metrics and child
    /// sensors.
    pub fn remove_sensor(&self, name: &str) {
        self.core.remove_sensor(name);
    }

    /// Purges every sensor inactive beyond its expiration window. Callable so
    /// tests can drive expiration deterministically; the periodic task uses the
    /// same logic on [`MetricsCore`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn expire_sensors(&self) {
        self.core.expire_sensors();
    }

    // ----- Metrics ----------------------------------------------------------

    /// Adds a metric backed by a measurable, not associated with any sensor.
    pub fn add_metric_measurable<M: Measurable + 'static>(
        &self,
        metric_name: MetricName,
        measurable: M,
    ) -> Result<(), KafkaError> {
        self.core
            .add_metric_provider(metric_name, None, MetricValueProvider::from_measurable(measurable))
    }

    /// Adds a metric backed by a value provider, not associated with any
    /// sensor.
    pub fn add_metric_provider(
        &self,
        metric_name: MetricName,
        config: Option<MetricConfig>,
        provider: MetricValueProvider,
    ) -> Result<(), KafkaError> {
        self.core.add_metric_provider(metric_name, config, provider)
    }

    /// Creates the metric if absent, or returns the already-registered one.
    pub fn add_metric_if_absent(
        &self,
        metric_name: MetricName,
        config: Option<MetricConfig>,
        provider: MetricValueProvider,
    ) -> Arc<KafkaMetric> {
        let metric = Arc::new(KafkaMetric::new(
            metric_name,
            provider,
            config.unwrap_or_else(|| self.core.config.clone()),
            self.core.time.clone(),
        ));
        match self.core.register_metric(metric.clone()) {
            Some(existing) => existing,
            None => metric,
        }
    }

    /// Removes a metric if it exists, returning it and firing `metric_removal`
    /// for each reporter.
    pub fn remove_metric(&self, metric_name: &MetricName) -> Option<Arc<KafkaMetric>> {
        self.core.remove_metric(metric_name)
    }

    /// Returns the metric with the given name, if registered.
    pub fn metric(&self, metric_name: &MetricName) -> Option<Arc<KafkaMetric>> {
        self.core.metrics.get(metric_name).map(|entry| entry.clone())
    }

    /// A snapshot of all currently registered metrics indexed by name.
    pub fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        self.core
            .metrics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// The number of currently registered metrics.
    pub fn metrics_count(&self) -> usize {
        self.core.metrics.len()
    }

    // ----- Reporters --------------------------------------------------------

    /// Adds a reporter, initializing it with the current metrics.
    pub fn add_reporter(&self, reporter: Arc<dyn MetricsReporter>) {
        let existing: Vec<Arc<KafkaMetric>> = self.core.metrics.iter().map(|entry| entry.value().clone()).collect();
        reporter.init(&existing);
        self.core.reporters.lock().expect("reporters lock poisoned").push(reporter);
    }

    /// Removes a reporter, closing it if it was present.
    pub fn remove_reporter(&self, reporter: &Arc<dyn MetricsReporter>) {
        let removed = {
            let mut reporters = self.core.reporters.lock().expect("reporters lock poisoned");
            reporters
                .iter()
                .position(|r| Arc::ptr_eq(r, reporter))
                .map(|index| reporters.remove(index))
        };
        if let Some(reporter) = removed {
            reporter.close();
        }
    }

    /// The registered reporters.
    pub fn reporters(&self) -> Vec<Arc<dyn MetricsReporter>> {
        self.core.reporters.lock().expect("reporters lock poisoned").clone()
    }

    /// A snapshot of the parent-to-children sensor map, keyed by parent name.
    pub fn children_sensors(&self) -> HashMap<String, Vec<Arc<Sensor>>> {
        self.core
            .children_sensors
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Closes this metrics repository: stops the expiry task and closes all
    /// reporters.
    pub fn close(&self) {
        if let Some(task) = self.expiry_task.lock().expect("expiry task lock poisoned").take() {
            task.abort();
        }
        let reporters = self.core.reporters.lock().expect("reporters lock poisoned").clone();
        for reporter in &reporters {
            reporter.close();
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Metrics {
    fn drop(&mut self) {
        if let Some(task) = self.expiry_task.lock().expect("expiry task lock poisoned").take() {
            task.abort();
        }
    }
}

fn default_context() -> KafkaMetricsContext {
    KafkaMetricsContext::new("")
}

/// Combines the config's default tags with the supplied tags (supplied tags win
/// on key conflicts) to build a metric name.
fn build_metric_name(
    config: &MetricConfig,
    name: &str,
    group: &str,
    description: &str,
    tags: IndexMap<String, String>,
) -> MetricName {
    let mut combined = config.tags().clone();
    for (key, value) in tags {
        combined.insert(key, value);
    }
    MetricName::new(name, group, description, combined)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::common::metric::Metric;
    use crate::common::metrics::internals::metrics_utils::convert;
    use crate::common::metrics::stats::{
        Avg, BucketSizing, CumulativeSum, Max, Meter, Min, Percentile, Percentiles, Rate, SimpleRate, Value,
        WindowedCount, WindowedSum,
    };
    use crate::common::metrics::test_support::{FakeMetricsReporter, MockClock};
    use crate::common::metrics::{MetricValue, Quota, Stat, TimeUnit};

    const EPS: f64 = 0.000001;

    /// Iterations for the concurrency stress tests. Kept modest so the tests run
    /// in bounded time under the unoptimized test profile — a deadlock or data
    /// race in the sensor/registry locking surfaces almost immediately once the
    /// record, read, and report threads run against each other, so a smaller
    /// count still exercises the invariant thoroughly.
    const ITERATIONS: usize = 1000;

    /// Builds the registry used by most tests: default config, a no-op reporter,
    /// the mock clock as its time source, and sensor expiration enabled (no
    /// runtime is present, so the periodic purge task is simply not spawned).
    fn new_metrics(clock: &MockClock) -> Metrics {
        Metrics::new_with_expiration(
            MetricConfig::new(),
            vec![Arc::new(FakeMetricsReporter) as Arc<dyn MetricsReporter>],
            clock.time_source(),
            true,
        )
    }

    /// The `double` value of a registered metric, panicking if it is missing or
    /// not a floating-point value.
    fn metric_double(metrics: &Metrics, name: &MetricName) -> f64 {
        match metrics.metric(name).expect("metric is registered").metric_value() {
            MetricValue::Double(d) => d,
            other => panic!("expected a double metric value, got {other:?}"),
        }
    }

    fn assert_close(expected: f64, actual: f64, eps: f64, msg: &str) {
        assert!((expected - actual).abs() <= eps, "{msg}: expected {expected}, got {actual}");
    }

    /// A measurable reporting a fixed value; mirrors the constant measurable used
    /// to exercise sensor-less metric registration.
    struct ConstantMeasurable {
        value: f64,
    }

    impl Measurable for ConstantMeasurable {
        fn measure(&mut self, _config: &MetricConfig, _now: i64) -> f64 {
            self.value
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn test_metric_name() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let n1 = metrics
            .metric_name_key_values("name", "group", "description", &["key1", "value1", "key2", "value2"])
            .unwrap();
        let mut tags = IndexMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        tags.insert("key2".to_string(), "value2".to_string());
        let n2 = metrics.metric_name_tags("name", "group", "description", tags);
        assert_eq!(n1, n2, "metric names created in two different ways should be equal");

        let err = metrics
            .metric_name_key_values("name", "group", "description", &["key1"])
            .unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(
            err.message().contains("keyValue needs to be specified in pairs"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn test_simple_stats() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let config = MetricConfig::new();

        metrics
            .add_metric_measurable(
                metrics.metric_name_desc(
                    "direct.measurable",
                    "grp1",
                    "The fraction of time an appender waits for space allocation.",
                ),
                ConstantMeasurable { value: 0.0 },
            )
            .unwrap();

        let s = metrics.sensor("test.sensor").unwrap();
        s.add_metric(metrics.metric_name("test.avg", "grp1"), Avg::new()).unwrap();
        s.add_metric(metrics.metric_name("test.max", "grp1"), Max::new()).unwrap();
        s.add_metric(metrics.metric_name("test.min", "grp1"), Min::new()).unwrap();
        s.add_compound(Meter::with_unit(
            TimeUnit::Seconds,
            metrics.metric_name("test.rate", "grp1"),
            metrics.metric_name("test.total", "grp1"),
        ))
        .unwrap();
        s.add_compound(
            Meter::with_unit_and_stat(
                TimeUnit::Seconds,
                Box::new(WindowedCount::new()),
                metrics.metric_name("test.occurrences", "grp1"),
                metrics.metric_name("test.occurrences.total", "grp1"),
            )
            .unwrap(),
        )
        .unwrap();
        s.add_metric(metrics.metric_name("test.count", "grp1"), WindowedCount::new())
            .unwrap();
        s.add_compound(
            Percentiles::new(
                100,
                -100.0,
                100.0,
                BucketSizing::Constant,
                vec![
                    Percentile::new(metrics.metric_name("test.median", "grp1"), 50.0),
                    Percentile::new(metrics.metric_name("test.perc99_9", "grp1"), 99.9),
                ],
            )
            .unwrap(),
        )
        .unwrap();

        let s2 = metrics.sensor("test.sensor2").unwrap();
        s2.add_metric(metrics.metric_name("s2.total", "grp1"), CumulativeSum::new())
            .unwrap();
        s2.record(5.0).unwrap();

        let mut sum = 0.0;
        let count = 10;
        for i in 0..count {
            s.record(i as f64).unwrap();
            sum += i as f64;
        }

        // Prior to any time passing.
        let mut elapsed_secs = (config.time_window_ms() * (config.samples() as i64 - 1)) as f64 / 1000.0;
        assert_close(
            count as f64 / elapsed_secs,
            metric_double(&metrics, &metrics.metric_name("test.occurrences", "grp1")),
            EPS,
            "occurrences before time passes",
        );

        // Pretend 2 seconds passed.
        let sleep_time_ms = 2;
        clock.sleep(sleep_time_ms * 1000);
        elapsed_secs += sleep_time_ms as f64;

        assert_close(
            5.0,
            metric_double(&metrics, &metrics.metric_name("s2.total", "grp1")),
            EPS,
            "s2 reflects the constant value",
        );
        assert_close(
            4.5,
            metric_double(&metrics, &metrics.metric_name("test.avg", "grp1")),
            EPS,
            "Avg(0...9) = 4.5",
        );
        assert_close(
            (count - 1) as f64,
            metric_double(&metrics, &metrics.metric_name("test.max", "grp1")),
            EPS,
            "Max(0...9) = 9",
        );
        assert_close(
            0.0,
            metric_double(&metrics, &metrics.metric_name("test.min", "grp1")),
            EPS,
            "Min(0...9) = 0",
        );
        assert_close(
            sum / elapsed_secs,
            metric_double(&metrics, &metrics.metric_name("test.rate", "grp1")),
            EPS,
            "Rate(0...9) = 1.40625",
        );
        assert_close(
            count as f64 / elapsed_secs,
            metric_double(&metrics, &metrics.metric_name("test.occurrences", "grp1")),
            EPS,
            "occurrences after time passes",
        );
        assert_close(
            count as f64,
            metric_double(&metrics, &metrics.metric_name("test.count", "grp1")),
            EPS,
            "Count(0...9) = 10",
        );
    }

    #[test]
    fn test_hierarchical_sensors() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);

        let parent1 = metrics.sensor("test.parent1").unwrap();
        parent1
            .add_metric(metrics.metric_name("test.parent1.count", "grp1"), WindowedCount::new())
            .unwrap();
        let parent2 = metrics.sensor("test.parent2").unwrap();
        parent2
            .add_metric(metrics.metric_name("test.parent2.count", "grp1"), WindowedCount::new())
            .unwrap();
        let child1 = metrics
            .sensor_with_parents("test.child1", vec![parent1.clone(), parent2.clone()])
            .unwrap();
        child1
            .add_metric(metrics.metric_name("test.child1.count", "grp1"), WindowedCount::new())
            .unwrap();
        let child2 = metrics.sensor_with_parents("test.child2", vec![parent1.clone()]).unwrap();
        child2
            .add_metric(metrics.metric_name("test.child2.count", "grp1"), WindowedCount::new())
            .unwrap();
        let grandchild = metrics.sensor_with_parents("test.grandchild", vec![child1.clone()]).unwrap();
        grandchild
            .add_metric(metrics.metric_name("test.grandchild.count", "grp1"), WindowedCount::new())
            .unwrap();

        // Increment each sensor once.
        parent1.record_occurrence().unwrap();
        parent2.record_occurrence().unwrap();
        child1.record_occurrence().unwrap();
        child2.record_occurrence().unwrap();
        grandchild.record_occurrence().unwrap();

        let value = |sensor: &Sensor| match sensor.metrics()[0].metric_value() {
            MetricValue::Double(d) => d,
            other => panic!("expected double, got {other:?}"),
        };
        let p1 = value(&parent1);
        let p2 = value(&parent2);
        let c1 = value(&child1);
        let c2 = value(&child2);
        let gc = value(&grandchild);

        // Each metric should have a count equal to one plus its children's count.
        assert_close(1.0, gc, EPS, "grandchild");
        assert_close(1.0 + gc, c1, EPS, "child1");
        assert_close(1.0, c2, EPS, "child2");
        assert_close(1.0 + c1, p2, EPS, "parent2");
        assert_close(1.0 + c1 + c2, p1, EPS, "parent1");

        let child_names = |parent: &Sensor| {
            metrics
                .children_sensors()
                .get(parent.name())
                .map(|children| children.iter().map(|s| s.name().to_string()).collect::<Vec<_>>())
        };
        assert_eq!(
            child_names(&parent1),
            Some(vec!["test.child1".to_string(), "test.child2".to_string()])
        );
        assert_eq!(child_names(&parent2), Some(vec!["test.child1".to_string()]));
        assert_eq!(child_names(&grandchild), None);
    }

    #[test]
    fn test_bad_sensor_hierarchy() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let p = metrics.sensor("parent").unwrap();
        let c1 = metrics.sensor_with_parents("child1", vec![p.clone()]).unwrap();
        let c2 = metrics.sensor_with_parents("child2", vec![p.clone()]).unwrap();
        let Err(err) = metrics.sensor_with_parents("gc", vec![c1, c2]) else {
            panic!("expected a circular-dependency error");
        };
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(
            err.message().contains("Circular dependency in sensors"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn test_remove_child_sensor() {
        let metrics = Metrics::new();
        let parent = metrics.sensor("parent").unwrap();
        let child = metrics.sensor_with_parents("child", vec![parent.clone()]).unwrap();

        let before = metrics.children_sensors();
        let before_children = before.get(parent.name()).expect("parent should have a child list");
        assert_eq!(before_children.len(), 1, "parent should have exactly one child before removal");
        assert!(
            Arc::ptr_eq(&before_children[0], &child),
            "the registered child should be the created child sensor"
        );

        metrics.remove_sensor("child");

        assert_eq!(
            metrics.children_sensors().get(parent.name()).map(|c| c.len()),
            Some(0),
            "parent's child list should be empty after removal"
        );
    }

    #[test]
    fn test_remove_sensor() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let size = metrics.metrics().len();
        let parent1 = metrics.sensor("test.parent1").unwrap();
        parent1
            .add_metric(metrics.metric_name("test.parent1.count", "grp1"), WindowedCount::new())
            .unwrap();
        let parent2 = metrics.sensor("test.parent2").unwrap();
        parent2
            .add_metric(metrics.metric_name("test.parent2.count", "grp1"), WindowedCount::new())
            .unwrap();
        let child1 = metrics
            .sensor_with_parents("test.child1", vec![parent1.clone(), parent2.clone()])
            .unwrap();
        child1
            .add_metric(metrics.metric_name("test.child1.count", "grp1"), WindowedCount::new())
            .unwrap();
        let child2 = metrics.sensor_with_parents("test.child2", vec![parent2.clone()]).unwrap();
        child2
            .add_metric(metrics.metric_name("test.child2.count", "grp1"), WindowedCount::new())
            .unwrap();
        let grand_child1 = metrics.sensor_with_parents("test.gchild2", vec![child2.clone()]).unwrap();
        grand_child1
            .add_metric(metrics.metric_name("test.gchild2.count", "grp1"), WindowedCount::new())
            .unwrap();

        let sensor = metrics.get_sensor("test.parent1");
        assert!(sensor.is_some());
        metrics.remove_sensor("test.parent1");
        assert!(metrics.get_sensor("test.parent1").is_none());
        assert!(metrics.metric(&metrics.metric_name("test.parent1.count", "grp1")).is_none());
        assert!(metrics.get_sensor("test.child1").is_none());
        assert!(!metrics.children_sensors().contains_key("test.parent1"));
        assert!(metrics.metric(&metrics.metric_name("test.child1.count", "grp1")).is_none());

        let sensor = metrics.get_sensor("test.gchild2");
        assert!(sensor.is_some());
        metrics.remove_sensor("test.gchild2");
        assert!(metrics.get_sensor("test.gchild2").is_none());
        assert!(!metrics.children_sensors().contains_key("test.gchild2"));
        assert!(metrics.metric(&metrics.metric_name("test.gchild2.count", "grp1")).is_none());

        let sensor = metrics.get_sensor("test.child2");
        assert!(sensor.is_some());
        metrics.remove_sensor("test.child2");
        assert!(metrics.get_sensor("test.child2").is_none());
        assert!(!metrics.children_sensors().contains_key("test.child2"));
        assert!(metrics.metric(&metrics.metric_name("test.child2.count", "grp1")).is_none());

        let sensor = metrics.get_sensor("test.parent2");
        assert!(sensor.is_some());
        metrics.remove_sensor("test.parent2");
        assert!(metrics.get_sensor("test.parent2").is_none());
        assert!(!metrics.children_sensors().contains_key("test.parent2"));
        assert!(metrics.metric(&metrics.metric_name("test.parent2.count", "grp1")).is_none());

        assert_eq!(size, metrics.metrics().len());
    }

    #[test]
    fn test_remove_inactive_metrics() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);

        let s1 = metrics.sensor_with_expiration("test.s1", None, 1).unwrap();
        s1.add_metric(metrics.metric_name("test.s1.count", "grp1"), WindowedCount::new())
            .unwrap();

        let s2 = metrics.sensor_with_expiration("test.s2", None, 3).unwrap();
        s2.add_metric(metrics.metric_name("test.s2.count", "grp1"), WindowedCount::new())
            .unwrap();

        metrics.expire_sensors();
        assert!(metrics.get_sensor("test.s1").is_some(), "Sensor test.s1 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s1.count", "grp1")).is_some());
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_some());

        clock.sleep(1001);
        metrics.expire_sensors();
        assert!(
            metrics.get_sensor("test.s1").is_none(),
            "Sensor test.s1 should have been purged"
        );
        assert!(metrics.metric(&metrics.metric_name("test.s1.count", "grp1")).is_none());
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_some());

        // Recording resets the clock for s2, so it should not be purged at the
        // 3-second mark after creation.
        s2.record_occurrence().unwrap();
        clock.sleep(2000);
        metrics.expire_sensors();
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_some());

        // After another 1001 ms, the metric should be purged.
        clock.sleep(1001);
        metrics.expire_sensors();
        assert!(
            metrics.get_sensor("test.s2").is_none(),
            "Sensor test.s2 should have been purged"
        );
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_none());

        // After purging, it should be possible to recreate a metric.
        let s1 = metrics.sensor_with_expiration("test.s1", None, 1).unwrap();
        s1.add_metric(metrics.metric_name("test.s1.count", "grp1"), WindowedCount::new())
            .unwrap();
        assert!(metrics.get_sensor("test.s1").is_some(), "Sensor test.s1 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s1.count", "grp1")).is_some());
    }

    #[test]
    fn test_remove_metric() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let size = metrics.metrics().len();
        metrics
            .add_metric_measurable(metrics.metric_name("test1", "grp1"), WindowedCount::new())
            .unwrap();
        metrics
            .add_metric_measurable(metrics.metric_name("test2", "grp1"), WindowedCount::new())
            .unwrap();

        assert!(metrics.remove_metric(&metrics.metric_name("test1", "grp1")).is_some());
        assert!(metrics.metric(&metrics.metric_name("test1", "grp1")).is_none());
        assert!(metrics.metric(&metrics.metric_name("test2", "grp1")).is_some());

        assert!(metrics.remove_metric(&metrics.metric_name("test2", "grp1")).is_some());
        assert!(metrics.metric(&metrics.metric_name("test2", "grp1")).is_none());

        assert_eq!(size, metrics.metrics().len());
    }

    #[test]
    fn test_time_windowing() {
        let clock = MockClock::new();
        let mut count = WindowedCount::new();
        let config = MetricConfig::new()
            .with_time_window(1, TimeUnit::Milliseconds)
            .with_samples(2)
            .unwrap();
        count.record(&config, 1.0, clock.milliseconds());
        clock.sleep(1);
        count.record(&config, 1.0, clock.milliseconds());
        assert_close(2.0, count.measure(&config, clock.milliseconds()), EPS, "two events in window");
        clock.sleep(1);
        count.record(&config, 1.0, clock.milliseconds()); // oldest event times out
        assert_close(2.0, count.measure(&config, clock.milliseconds()), EPS, "oldest event expired");
    }

    #[test]
    fn test_old_data_has_no_effect() {
        let clock = MockClock::new();
        let mut max = Max::new();
        let window_ms = 100;
        let samples = 2;
        let config = MetricConfig::new()
            .with_time_window(window_ms, TimeUnit::Milliseconds)
            .with_samples(samples)
            .unwrap();
        max.record(&config, 50.0, clock.milliseconds());
        clock.sleep(samples as i64 * window_ms);
        assert!(max.measure(&config, clock.milliseconds()).is_nan());
    }

    #[test]
    fn test_sampled_stat_returns_nan_when_no_values_exist() {
        let clock = MockClock::new();
        let mut max = Max::new();
        let mut min = Min::new();
        let mut avg = Avg::new();
        let window_ms = 100;
        let samples = 2;
        let config = MetricConfig::new()
            .with_time_window(window_ms, TimeUnit::Milliseconds)
            .with_samples(samples)
            .unwrap();
        max.record(&config, 50.0, clock.milliseconds());
        min.record(&config, 50.0, clock.milliseconds());
        avg.record(&config, 50.0, clock.milliseconds());

        clock.sleep(samples as i64 * window_ms);

        assert!(max.measure(&config, clock.milliseconds()).is_nan());
        assert!(min.measure(&config, clock.milliseconds()).is_nan());
        assert!(avg.measure(&config, clock.milliseconds()).is_nan());
    }

    #[test]
    fn test_sampled_stat_returns_initial_value_when_no_values_exist() {
        let clock = MockClock::new();
        let mut count = WindowedCount::new();
        let mut sampled_total = WindowedSum::new();
        let window_ms = 100;
        let samples = 2;
        let config = MetricConfig::new()
            .with_time_window(window_ms, TimeUnit::Milliseconds)
            .with_samples(samples)
            .unwrap();

        count.record(&config, 50.0, clock.milliseconds());
        sampled_total.record(&config, 50.0, clock.milliseconds());

        clock.sleep(samples as i64 * window_ms);

        assert_close(0.0, count.measure(&config, clock.milliseconds()), EPS, "count resets to 0");
        assert_close(
            0.0,
            sampled_total.measure(&config, clock.milliseconds()),
            EPS,
            "sum resets to 0",
        );
    }

    #[test]
    fn test_duplicate_metric_name() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        metrics
            .sensor("test")
            .unwrap()
            .add_metric(metrics.metric_name("test", "grp1"), Avg::new())
            .unwrap();
        let err = metrics
            .sensor("test2")
            .unwrap()
            .add_metric(metrics.metric_name("test", "grp1"), CumulativeSum::new())
            .unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(
            err.message().contains("already exists"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn test_quotas() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let sensor = metrics.sensor("test").unwrap();
        sensor
            .add_metric_with_config(
                metrics.metric_name("test1.total", "grp1"),
                CumulativeSum::new(),
                Some(MetricConfig::new().with_quota(Quota::upper_bound(5.0))),
            )
            .unwrap();
        sensor
            .add_metric_with_config(
                metrics.metric_name("test2.total", "grp1"),
                CumulativeSum::new(),
                Some(MetricConfig::new().with_quota(Quota::lower_bound(0.0))),
            )
            .unwrap();
        sensor.record(5.0).unwrap();
        assert!(sensor.record(1.0).is_err(), "should have gotten a quota violation");
        assert_close(
            6.0,
            metric_double(&metrics, &metrics.metric_name("test1.total", "grp1")),
            EPS,
            "test1 total",
        );
        sensor.record(-6.0).unwrap();
        assert!(sensor.record(-1.0).is_err(), "should have gotten a quota violation");
    }

    #[test]
    fn test_quotas_equality() {
        let quota1 = Quota::upper_bound(10.5);
        let quota2 = Quota::lower_bound(10.5);
        assert_ne!(quota1, quota2, "quotas with different directions should not be equal");
        let quota3 = Quota::lower_bound(10.5);
        assert_eq!(quota2, quota3, "quotas with the same bound and direction should be equal");
    }

    #[test]
    fn test_percentiles() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let buckets = 100;
        let percs = Percentiles::new(
            4 * buckets,
            0.0,
            100.0,
            BucketSizing::Constant,
            vec![
                Percentile::new(metrics.metric_name("test.p25", "grp1"), 25.0),
                Percentile::new(metrics.metric_name("test.p50", "grp1"), 50.0),
                Percentile::new(metrics.metric_name("test.p75", "grp1"), 75.0),
            ],
        )
        .unwrap();
        let config = MetricConfig::new().with_event_window(50).with_samples(2).unwrap();
        let sensor = metrics.sensor_with_config("test", config).unwrap();
        sensor.add_compound(percs).unwrap();

        for i in 0..buckets {
            sensor.record(i as f64).unwrap();
        }

        assert_eq!(25.0, metric_double(&metrics, &metrics.metric_name("test.p25", "grp1")));
        assert_eq!(50.0, metric_double(&metrics, &metrics.metric_name("test.p50", "grp1")));
        assert_eq!(75.0, metric_double(&metrics, &metrics.metric_name("test.p75", "grp1")));
    }

    #[test]
    fn should_pin_smaller_values_to_min() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let min = 0.0;
        let max = 100.0;
        let percs = Percentiles::new(
            1000,
            min,
            max,
            BucketSizing::Linear,
            vec![Percentile::new(metrics.metric_name("test.p50", "grp1"), 50.0)],
        )
        .unwrap();
        let config = MetricConfig::new().with_event_window(50).with_samples(2).unwrap();
        let sensor = metrics.sensor_with_config("test", config).unwrap();
        sensor.add_compound(percs).unwrap();

        sensor.record(min - 100.0).unwrap();
        sensor.record(min - 100.0).unwrap();
        assert_eq!(min, metric_double(&metrics, &metrics.metric_name("test.p50", "grp1")));
    }

    #[test]
    fn should_pin_larger_values_to_max() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let min = 0.0;
        let max = 100.0;
        let percs = Percentiles::new(
            1000,
            min,
            max,
            BucketSizing::Linear,
            vec![Percentile::new(metrics.metric_name("test.p50", "grp1"), 50.0)],
        )
        .unwrap();
        let config = MetricConfig::new().with_event_window(50).with_samples(2).unwrap();
        let sensor = metrics.sensor_with_config("test", config).unwrap();
        sensor.add_compound(percs).unwrap();

        sensor.record(max + 100.0).unwrap();
        sensor.record(max + 100.0).unwrap();
        assert_eq!(max, metric_double(&metrics, &metrics.metric_name("test.p50", "grp1")));
    }

    #[test]
    fn test_percentiles_with_random_numbers_and_linear_bucketing() {
        // Java seeds from a fresh Random; a fixed seed keeps this deterministic.
        // The generated values are used both to drive the estimator and to
        // compute the exact percentiles it is checked against, so the estimator
        // is validated against the same data regardless of the RNG.
        let seed = 0x5DEECE66Du64;
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let size_in_bytes = 100 * 1000; // 100 kB
        let maximum_value = 1000 * 24 * 60 * 60 * 1000_i64; // if values are ms, max is 1000 days

        let mut prng = StdRng::seed_from_u64(seed);
        let number_of_values = 5000 + prng.random_range(0..10_000); // range [5000, 15000)

        let percs = Percentiles::with_max(
            size_in_bytes,
            maximum_value as f64,
            BucketSizing::Linear,
            vec![
                Percentile::new(metrics.metric_name("test.p90", "grp1"), 90.0),
                Percentile::new(metrics.metric_name("test.p99", "grp1"), 99.0),
            ],
        )
        .unwrap();
        let config = MetricConfig::new().with_event_window(50).with_samples(2).unwrap();
        let sensor = metrics.sensor_with_config("test", config).unwrap();
        sensor.add_compound(percs).unwrap();

        let mut values = Vec::with_capacity(number_of_values as usize);
        for _ in 0..number_of_values {
            let value = (prng.random::<i64>().wrapping_abs() - 1).rem_euclid(maximum_value);
            values.push(value);
            sensor.record(value as f64).unwrap();
        }

        values.sort_unstable();

        let p90_index = ((90 * number_of_values) as f64 / 100.0).ceil() as usize;
        let p99_index = ((99 * number_of_values) as f64 / 100.0).ceil() as usize;

        let expected_p90 = values[p90_index - 1] as f64;
        let expected_p99 = values[p99_index - 1] as f64;

        assert_close(
            expected_p90,
            metric_double(&metrics, &metrics.metric_name("test.p90", "grp1")),
            expected_p90 / 5.0,
            "p90 estimate",
        );
        assert_close(
            expected_p99,
            metric_double(&metrics, &metrics.metric_name("test.p99", "grp1")),
            expected_p99 / 5.0,
            "p99 estimate",
        );
    }

    #[test]
    fn test_rate_windowing() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        // Use the default time window with three samples.
        let cfg = MetricConfig::new().with_samples(3).unwrap();
        let s = metrics.sensor_with_config("test.sensor", cfg.clone()).unwrap();
        let rate_metric_name = metrics.metric_name("test.rate", "grp1");
        let total_metric_name = metrics.metric_name("test.total", "grp1");
        let count_rate_metric_name = metrics.metric_name("test.count.rate", "grp1");
        let count_total_metric_name = metrics.metric_name("test.count.total", "grp1");
        s.add_compound(Meter::with_unit(
            TimeUnit::Seconds,
            rate_metric_name.clone(),
            total_metric_name.clone(),
        ))
        .unwrap();
        s.add_compound(
            Meter::with_unit_and_stat(
                TimeUnit::Seconds,
                Box::new(WindowedCount::new()),
                count_rate_metric_name.clone(),
                count_total_metric_name.clone(),
            )
            .unwrap(),
        )
        .unwrap();
        let total_metric = metrics.metric(&total_metric_name).unwrap();
        let count_total_metric = metrics.metric(&count_total_metric_name).unwrap();

        let mut sum = 0.0;
        let count = cfg.samples() - 1;
        // Advance one window after every record.
        for _ in 0..count {
            s.record(100.0).unwrap();
            sum += 100.0;
            clock.sleep(cfg.time_window_ms());
            assert_close(sum, as_double(&total_metric), EPS, "running total");
        }

        // Sleep for half the window.
        clock.sleep(cfg.time_window_ms() / 2);

        // Prior to any time passing, elapsedSecs = window * (samples - half of final sample).
        let elapsed_secs = convert(cfg.time_window_ms(), TimeUnit::Seconds) * (cfg.samples() as f64 - 0.5);

        let rate_metric = metrics.metric(&rate_metric_name).unwrap();
        let count_rate_metric = metrics.metric(&count_rate_metric_name).unwrap();
        assert_close(sum / elapsed_secs, as_double(&rate_metric), EPS, "Rate(0...2) = 2.666");
        assert_close(
            count as f64 / elapsed_secs,
            as_double(&count_rate_metric),
            EPS,
            "Count rate(0...2) = 0.02666",
        );

        let window_ms = {
            let measurable = rate_metric.measurable().unwrap();
            let mut guard = measurable.lock().unwrap();
            let rate = guard.as_any_mut().downcast_mut::<Rate>().expect("rate metric is a Rate");
            rate.window_size(&cfg, clock.milliseconds())
        };
        assert_close(
            elapsed_secs,
            convert(window_ms, TimeUnit::Seconds),
            EPS,
            "Elapsed Time = 75 seconds",
        );
        assert_close(sum, as_double(&total_metric), EPS, "total");
        assert_close(count as f64, as_double(&count_total_metric), EPS, "count total");

        // Rates expire, but totals are cumulative.
        clock.sleep(cfg.time_window_ms() * cfg.samples() as i64);
        assert_close(0.0, as_double(&rate_metric), EPS, "rate expired");
        assert_close(0.0, as_double(&count_rate_metric), EPS, "count rate expired");
        assert_close(sum, as_double(&total_metric), EPS, "total still cumulative");
        assert_close(
            count as f64,
            as_double(&count_total_metric),
            EPS,
            "count total still cumulative",
        );
    }

    fn as_double(metric: &Arc<KafkaMetric>) -> f64 {
        match metric.metric_value() {
            MetricValue::Double(d) => d,
            other => panic!("expected double, got {other:?}"),
        }
    }

    #[test]
    fn test_simple_rate() {
        let clock = MockClock::new();
        let mut rate = SimpleRate::new();
        let config = MetricConfig::new()
            .with_time_window(1, TimeUnit::Seconds)
            .with_samples(10)
            .unwrap();

        let record =
            |rate: &mut SimpleRate, clock: &MockClock, value: f64| rate.record(&config, value, clock.milliseconds());
        let measure = |rate: &mut SimpleRate, clock: &MockClock| rate.measure(&config, clock.milliseconds());

        // In the first window the rate is a fraction of the whole (1s) window,
        // so recording 1000 at t0 stays at 1000 until the window completes.
        record(&mut rate, &clock, 1000.0);
        assert_eq!(1000.0, measure(&mut rate, &clock));
        clock.sleep(100);
        assert_eq!(1000.0, measure(&mut rate, &clock)); // 1000B / 0.1s
        clock.sleep(100);
        assert_eq!(1000.0, measure(&mut rate, &clock)); // 1000B / 0.2s
        clock.sleep(200);
        assert_eq!(1000.0, measure(&mut rate, &clock)); // 1000B / 0.4s

        // In subsequent windows the rate degrades in proportion to elapsed time.
        clock.sleep(600);
        assert_eq!(1000.0, measure(&mut rate, &clock)); // 1000B / 1.0s
        clock.sleep(200);
        assert_eq!(1000.0 / 1.2, measure(&mut rate, &clock)); // 1000B / 1.2s
        clock.sleep(200);
        assert_eq!(1000.0 / 1.4, measure(&mut rate, &clock)); // 1000B / 1.4s

        // Adding another value inside the same window doubles the rate.
        record(&mut rate, &clock, 1000.0);
        assert_eq!(2000.0 / 1.4, measure(&mut rate, &clock)); // 2000B / 1.4s

        // Crossing the next window boundary should not change behavior.
        clock.sleep(1100);
        assert_eq!(2000.0 / 2.5, measure(&mut rate, &clock)); // 2000B / 2.5s
        record(&mut rate, &clock, 1000.0);
        assert_eq!(3000.0 / 2.5, measure(&mut rate, &clock)); // 3000B / 2.5s

        // Sleeping for another 6.5 windows should be the same.
        clock.sleep(6500);
        assert_close(3000.0 / 9.0, measure(&mut rate, &clock), 1.0, "3000B / 9s");
        record(&mut rate, &clock, 1000.0);
        assert_close(4000.0 / 9.0, measure(&mut rate, &clock), 1.0, "4000B / 9s");

        // Crossing the 10-window boundary purges the first window's values, so
        // the rate is calculated from the oldest remaining reading at 1.4s.
        clock.sleep(1500);
        assert_close(
            (4000.0 - 1000.0) / (10.5 - 1.4),
            measure(&mut rate, &clock),
            1.0,
            "purged first window",
        );
        record(&mut rate, &clock, 1000.0);
        assert_close(
            (5000.0 - 1000.0) / (10.5 - 1.4),
            measure(&mut rate, &clock),
            1.0,
            "purged first window, recorded",
        );
    }

    #[test]
    fn test_metric_instances() {
        let clock = MockClock::new();
        let metrics = new_metrics(&clock);
        let metric1 = MetricNameTemplate::with_tag_names(
            "name",
            "group",
            "The first metric used in testMetricName()",
            &["key1", "key2"],
        );
        let metric2 = MetricNameTemplate::with_tag_names(
            "name",
            "group",
            "The second metric used in testMetricName()",
            &["key1", "key2"],
        );
        let metric_with_inherited_tags = MetricNameTemplate::with_tag_names(
            "inherited.tags",
            "group",
            "inherited.tags in testMetricName",
            &["parent-tag", "child-tag"],
        );

        let n1 = metrics
            .metric_instance(&metric1, &["key1", "value1", "key2", "value2"])
            .unwrap();
        let mut tags = IndexMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        tags.insert("key2".to_string(), "value2".to_string());
        let n2 = metrics.metric_instance_tags(&metric2, tags).unwrap();
        assert_eq!(n1, n2, "metric names created in two different ways should be equal");

        let err = metrics.metric_instance(&metric1, &["key1"]).unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(
            err.message().contains("keyValue needs to be specified in pairs"),
            "unexpected message: {}",
            err.message()
        );

        let mut parent_tags = IndexMap::new();
        parent_tags.insert("parent-tag".to_string(), "parent-tag-value".to_string());

        let mut child_tags = IndexMap::new();
        child_tags.insert("child-tag".to_string(), "child-tag-value".to_string());

        let inherited = Metrics::new_with_expiration(
            MetricConfig::new().with_tags(parent_tags.clone()),
            vec![Arc::new(FakeMetricsReporter) as Arc<dyn MetricsReporter>],
            clock.time_source(),
            true,
        );
        let inherited_metric = inherited.metric_instance_tags(&metric_with_inherited_tags, child_tags).unwrap();
        let filled_out_tags = inherited_metric.tags();
        assert_eq!(filled_out_tags.get("parent-tag"), Some(&"parent-tag-value".to_string()));
        assert_eq!(filled_out_tags.get("child-tag"), Some(&"child-tag-value".to_string()));

        let err = inherited
            .metric_instance_tags(&metric_with_inherited_tags, parent_tags)
            .unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert_eq!(
            err.message(),
            "For 'inherited.tags', runtime-defined metric tags do not match the tags in the template. Runtime = [parent-tag] Template = [parent-tag, child-tag]"
        );

        let mut runtime_tags = IndexMap::new();
        runtime_tags.insert("child-tag".to_string(), "child-tag-value".to_string());
        runtime_tags.insert("tag-not-in-template".to_string(), "unexpected-value".to_string());
        let err = inherited
            .metric_instance_tags(&metric_with_inherited_tags, runtime_tags)
            .unwrap_err();
        assert!(
            matches!(err, KafkaError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert_eq!(
            err.message(),
            "For 'inherited.tags', runtime-defined metric tags do not match the tags in the template. Runtime = [child-tag, parent-tag, tag-not-in-template] Template = [parent-tag, child-tag]"
        );
    }

    /// The kinds of statistic exercised by the concurrency stress tests.
    #[derive(Clone, Copy)]
    enum StatType {
        Avg,
        Total,
        Count,
        Max,
        Min,
        Rate,
        SimpleRate,
        Sum,
        Value,
        Percentiles,
        Meter,
    }

    impl StatType {
        const ALL: [StatType; 11] = [
            StatType::Avg,
            StatType::Total,
            StatType::Count,
            StatType::Max,
            StatType::Min,
            StatType::Rate,
            StatType::SimpleRate,
            StatType::Sum,
            StatType::Value,
            StatType::Percentiles,
            StatType::Meter,
        ];
    }

    fn create_sensor(metrics: &Metrics, stat_type: StatType, index: usize) -> Arc<Sensor> {
        let sensor = metrics.sensor(&format!("kafka.requests.{index}")).unwrap();
        let mut tags = IndexMap::new();
        tags.insert("tag".to_string(), format!("tag{index}"));
        match stat_type {
            StatType::Avg => {
                sensor
                    .add_metric(metrics.metric_name_group_tags("test.metric.avg", "avg", tags), Avg::new())
                    .unwrap();
            },
            StatType::Total => {
                sensor
                    .add_metric(
                        metrics.metric_name_group_tags("test.metric.total", "total", tags),
                        CumulativeSum::new(),
                    )
                    .unwrap();
            },
            StatType::Count => {
                sensor
                    .add_metric(
                        metrics.metric_name_group_tags("test.metric.count", "count", tags),
                        WindowedCount::new(),
                    )
                    .unwrap();
            },
            StatType::Max => {
                sensor
                    .add_metric(metrics.metric_name_group_tags("test.metric.max", "max", tags), Max::new())
                    .unwrap();
            },
            StatType::Min => {
                sensor
                    .add_metric(metrics.metric_name_group_tags("test.metric.min", "min", tags), Min::new())
                    .unwrap();
            },
            StatType::Rate => {
                sensor
                    .add_metric(metrics.metric_name_group_tags("test.metric.rate", "rate", tags), Rate::new())
                    .unwrap();
            },
            StatType::SimpleRate => {
                sensor
                    .add_metric(
                        metrics.metric_name_group_tags("test.metric.simpleRate", "simpleRate", tags),
                        SimpleRate::new(),
                    )
                    .unwrap();
            },
            StatType::Sum => {
                sensor
                    .add_metric(
                        metrics.metric_name_group_tags("test.metric.sum", "sum", tags),
                        WindowedSum::new(),
                    )
                    .unwrap();
            },
            StatType::Value => {
                sensor
                    .add_metric(metrics.metric_name_group_tags("test.metric.value", "value", tags), Value::new())
                    .unwrap();
            },
            StatType::Percentiles => {
                sensor
                    .add_metric(
                        metrics.metric_name_group_tags("test.metric.percentiles", "percentiles", tags),
                        Percentiles::new(
                            100,
                            -100.0,
                            100.0,
                            BucketSizing::Constant,
                            vec![
                                Percentile::new(metrics.metric_name("test.median", "percentiles"), 50.0),
                                Percentile::new(metrics.metric_name("test.perc99_9", "percentiles"), 99.9),
                            ],
                        )
                        .unwrap(),
                    )
                    .unwrap();
            },
            StatType::Meter => {
                sensor
                    .add_compound(Meter::new(
                        metrics.metric_name_group_tags("test.metric.meter.rate", "meter", tags.clone()),
                        metrics.metric_name_group_tags("test.metric.meter.total", "meter", tags),
                    ))
                    .unwrap();
            },
        }
        sensor
    }

    #[test]
    fn test_concurrent_read_update() {
        let metrics = Metrics::with_time(MockClock::with_auto_tick(10).time_source());
        let sensors: Mutex<VecDeque<Arc<Sensor>>> = Mutex::new(VecDeque::new());
        let alive = AtomicBool::new(true);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut rng = rand::rng();
                while alive.load(Ordering::SeqCst) {
                    let snapshot: Vec<Arc<Sensor>> = sensors.lock().unwrap().iter().cloned().collect();
                    for sensor in snapshot {
                        let _ = sensor.record(rng.random_range(0..10000) as f64);
                    }
                }
            });

            let mut rng = rand::rng();
            for i in 0..ITERATIONS {
                {
                    let mut deque = sensors.lock().unwrap();
                    if deque.len() > 5 {
                        let sensor = if rng.random::<bool>() {
                            deque.pop_front()
                        } else {
                            deque.pop_back()
                        };
                        drop(deque);
                        if let Some(sensor) = sensor {
                            metrics.remove_sensor(sensor.name());
                        }
                    }
                }
                let stat_type = StatType::ALL[rng.random_range(0..StatType::ALL.len())];
                let sensor = create_sensor(&metrics, stat_type, i);
                sensors.lock().unwrap().push_back(sensor);
                let snapshot: Vec<Arc<Sensor>> = sensors.lock().unwrap().iter().cloned().collect();
                for sensor in &snapshot {
                    for metric in sensor.metrics() {
                        // A read must always yield a value without panicking or
                        // deadlocking against the concurrent record thread.
                        let _ = metric.metric_value();
                    }
                }
            }
            alive.store(false, Ordering::SeqCst);
        });
    }

    /// A reporter that synchronizes on every callback, used to prove that
    /// reading metric values while holding a reporter lock cannot deadlock with
    /// registration or recording.
    struct LockingReporter {
        active_metrics: Mutex<HashMap<MetricName, Arc<KafkaMetric>>>,
    }

    impl LockingReporter {
        fn new() -> Self {
            Self { active_metrics: Mutex::new(HashMap::new()) }
        }

        fn process_metrics(&self) {
            let guard = self.active_metrics.lock().unwrap();
            for metric in guard.values() {
                let _ = metric.metric_value();
            }
        }
    }

    impl MetricsReporter for LockingReporter {
        fn init(&self, _metrics: &[Arc<KafkaMetric>]) {}

        fn metric_change(&self, metric: Arc<KafkaMetric>) {
            self.active_metrics.lock().unwrap().insert(metric.metric_name().clone(), metric);
        }

        fn metric_removal(&self, metric: Arc<KafkaMetric>) {
            let mut guard = self.active_metrics.lock().unwrap();
            if guard
                .get(metric.metric_name())
                .is_some_and(|existing| Arc::ptr_eq(existing, &metric))
            {
                guard.remove(metric.metric_name());
            }
        }

        fn close(&self) {}
    }

    #[test]
    fn test_concurrent_read_update_report() {
        let reporter = Arc::new(LockingReporter::new());
        let metrics = Metrics::new_with_expiration(
            MetricConfig::new(),
            vec![reporter.clone() as Arc<dyn MetricsReporter>],
            MockClock::with_auto_tick(10).time_source(),
            true,
        );
        let sensors: Mutex<VecDeque<Arc<Sensor>>> = Mutex::new(VecDeque::new());
        let alive = AtomicBool::new(true);

        std::thread::scope(|scope| {
            // Worker liveness (that none of the record/read/report threads has
            // failed) is guaranteed structurally: a panic or poisoned-lock in any
            // spawned worker propagates when `scope` joins, failing the test — an
            // equal-or-stronger check than polling each worker for "not done".
            scope.spawn(|| {
                let mut rng = rand::rng();
                while alive.load(Ordering::SeqCst) {
                    let snapshot: Vec<Arc<Sensor>> = sensors.lock().unwrap().iter().cloned().collect();
                    for sensor in snapshot {
                        let _ = sensor.record(rng.random_range(0..10000) as f64);
                    }
                }
            });
            scope.spawn(|| {
                while alive.load(Ordering::SeqCst) {
                    let snapshot: Vec<Arc<Sensor>> = sensors.lock().unwrap().iter().cloned().collect();
                    for sensor in snapshot {
                        for metric in sensor.metrics() {
                            let _ = metric.metric_value();
                        }
                    }
                }
            });
            scope.spawn(|| {
                while alive.load(Ordering::SeqCst) {
                    reporter.process_metrics();
                }
            });

            let mut rng = rand::rng();
            for i in 0..ITERATIONS {
                {
                    let mut deque = sensors.lock().unwrap();
                    if deque.len() > 10 {
                        let sensor = if rng.random::<bool>() {
                            deque.pop_front()
                        } else {
                            deque.pop_back()
                        };
                        drop(deque);
                        if let Some(sensor) = sensor {
                            metrics.remove_sensor(sensor.name());
                        }
                    }
                }
                let stat_type = StatType::ALL[rng.random_range(0..StatType::ALL.len())];
                let sensor = create_sensor(&metrics, stat_type, i);
                sensors.lock().unwrap().push_back(sensor);
            }
            alive.store(false, Ordering::SeqCst);
        });
    }
}
