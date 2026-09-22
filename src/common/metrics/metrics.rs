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

use crate::common::metrics::internals::MetricsUtils;
use crate::common::metrics::{
    ClosureGauge, Gauge, KafkaMetric, Measurable, MetricConfig, MetricValue, MetricValueProvider, MetricsReporter,
    RecordingLevel, Sensor, SystemTime, Time,
};
use crate::common::{Error, Metric, MetricName, MetricNameTemplate};

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

/// The parameters of [`Metrics::sensor_options`].
///
/// Java's widest `sensor` overload
/// (`sensor(String, MetricConfig, long, Sensor.RecordingLevel, Sensor...)`,
/// `Metrics.java:401`) carries four parameters beyond the overload group's
/// `{name}` intersection, so CLAUDE.md §2 caps the derived name and makes this
/// struct the method's *only* parameter — every Java parameter lives here,
/// `name` included. This struct has no Java counterpart: it exists solely to
/// satisfy that naming rule (DoD #7).
///
/// It is `#[non_exhaustive]`, so build it from [`SensorOptionsBuilder::new`]
/// and set the fields you need. Every other field's initial value is Java's —
/// what its narrower `sensor` overloads pass on the caller's behalf. `name` has
/// no such initial value: Java declares no `sensor` overload that omits it, so
/// there is no Java-derived default to fall back on, and
/// [`SensorOptionsBuilder::build`] returns an error if it was not set. `SensorOptions`
/// itself has deliberately no `Default` for the same reason.
// No `Debug` — `Sensor` is not `Debug`, and adding it there is out of scope for
// a naming change. No `Copy` either: `config` is an `Option<Arc<..>>`.
#[derive(Clone)]
#[non_exhaustive]
pub struct SensorOptions<'a> {
    /// Java's `name`, the sensor's unique registry key.
    pub name: &'a str,
    /// Java's `config`. Starts as `None`, meaning the registry's own config,
    /// as in `Metrics.java:325,336,348,360`.
    pub config: Option<Arc<MetricConfig>>,
    /// Java's `inactiveSensorExpirationTimeSeconds`. Starts as `i64::MAX`, as
    /// in `Metrics.java:325,336,348,360,372,386`.
    pub inactive_sensor_expiration_time_seconds: i64,
    /// Java's `recordingLevel`. Starts as `INFO`, as in
    /// `Metrics.java:325,348,372,427`.
    pub recording_level: RecordingLevel,
    /// Java's `parents` varargs. Starts empty, as in `Metrics.java:325,336`.
    pub parents: &'a [Arc<Sensor>],
}

/// Fluent builder for [`SensorOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`SensorOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct SensorOptionsBuilder<'a> {
    name: Option<&'a str>,
    config: Option<Arc<MetricConfig>>,
    inactive_sensor_expiration_time_seconds: i64,
    recording_level: RecordingLevel,
    parents: &'a [Arc<Sensor>],
}

impl<'a> Default for SensorOptionsBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> SensorOptionsBuilder<'a> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            name: None,
            config: None,
            inactive_sensor_expiration_time_seconds: i64::MAX,
            recording_level: RecordingLevel::Info,
            parents: &[],
        }
    }

    /// Sets [`SensorOptions::name`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }
    /// Sets [`SensorOptions::config`].
    pub fn set_config(mut self, config: Option<Arc<MetricConfig>>) -> Self {
        self.config = config;
        self
    }
    /// Sets [`SensorOptions::inactive_sensor_expiration_time_seconds`].
    pub fn set_inactive_sensor_expiration_time_seconds(mut self, inactive_sensor_expiration_time_seconds: i64) -> Self {
        self.inactive_sensor_expiration_time_seconds = inactive_sensor_expiration_time_seconds;
        self
    }
    /// Sets [`SensorOptions::recording_level`].
    pub fn set_recording_level(mut self, recording_level: RecordingLevel) -> Self {
        self.recording_level = recording_level;
        self
    }
    /// Sets [`SensorOptions::parents`].
    pub fn set_parents(mut self, parents: &'a [Arc<Sensor>]) -> Self {
        self.parents = parents;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `name`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub fn build(self) -> Result<SensorOptions<'a>, Error> {
        Ok(SensorOptions {
            name: self.name.ok_or_else(|| Self::missing("name"))?,
            config: self.config,
            inactive_sensor_expiration_time_seconds: self.inactive_sensor_expiration_time_seconds,
            recording_level: self.recording_level,
            parents: self.parents,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "SensorOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl Metrics {
    // Java's `Metrics` constructors (`Metrics.java:85,93,101,111,122,134,145,158`)
    // have an EMPTY parameter-name intersection, and the no-arg `:85` matches it,
    // so that one keeps the plain name `new` and every sibling is suffixed with
    // its full Rust parameter list (CLAUDE.md §2). The two `MetricsContext`
    // overloads (`:134`, `:158`) are not translated — `MetricsContext` itself is
    // untranslated.
    //
    // `Metrics(defaultConfig, reporters, time, boolean enableExpiration)` (`:145`)
    // is likewise not translated. Its one distinguishing parameter exists solely to
    // decide whether the constructor starts a `ScheduledThreadPoolExecutor` running
    // `ExpireSensorTask` every 30s (`:171-176`); it does not gate whether sensors
    // *can* expire — Java's own `MetricsTest.testRemoveInactiveMetrics` builds a
    // `Metrics` through the `enableExpiration = false` path (`:122` passes `false`)
    // and drives the task by hand. This translation has no scheduler at all: it
    // exposes the task as `expire_sensors()` (below) for the caller to drive, which
    // is exactly that Java test's shape. So a Rust `enable_expiration` parameter
    // would select nothing, and an ignored parameter is a worse contract than an
    // absent overload.

    /// Create a metrics repository with the supplied default config and clock,
    /// no reporters. Mirrors Java's `Metrics(MetricConfig defaultConfig, Time time)`
    /// (`:101`).
    pub fn with_default_config_time(default_config: Arc<MetricConfig>, time: Arc<dyn Time>) -> Self {
        Self::with_default_config_reporters_time(default_config, Vec::new(), time)
    }

    /// Create a metrics repository with a default config, no reporters, and the
    /// system clock. Mirrors Java's `Metrics()` (`Metrics.java:85`).
    pub fn new() -> Self {
        Self::with_default_config_reporters_time(Arc::new(MetricConfig::new()), Vec::new(), Arc::new(SystemTime))
    }

    /// Create a metrics repository with the supplied default config.
    /// Mirrors Java's `Metrics(MetricConfig defaultConfig)` (`:111`).
    pub fn with_default_config(default_config: Arc<MetricConfig>) -> Self {
        Self::with_default_config_reporters_time(default_config, Vec::new(), Arc::new(SystemTime))
    }

    /// Create a metrics repository with the supplied clock.
    /// Mirrors Java's `Metrics(Time time)` (`:93`).
    pub fn with_time(time: Arc<dyn Time>) -> Self {
        Self::with_default_config_reporters_time(Arc::new(MetricConfig::new()), Vec::new(), time)
    }

    /// Create a metrics repository with a default config, reporters, and clock.
    /// Mirrors Java's
    /// `Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time)`
    /// (`:122`).
    pub fn with_default_config_reporters_time(
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
            .add_metric_config_provider(
                metrics.metric_name_description_tags(
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

    // Java's five `metricName` overloads (`Metrics.java:194,208,218,231,243`)
    // intersect on `{name, group}`, and `metricName(String name, String group)`
    // (`:218`) is exactly that pair — so it keeps the plain name and the others
    // are suffixed with their Rust parameters beyond it (CLAUDE.md §2).

    /// Create a `MetricName` with the given name, group, description and tags,
    /// plus default tags specified in the metric configuration. Tags in `tags`
    /// take precedence over default tags with the same key.
    ///
    /// Mirrors Java's
    /// `metricName(String name, String group, String description, Map<String, String> tags)`
    /// (`:194`).
    pub fn metric_name_description_tags(
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

    /// Create a `MetricName` with the given name, group and description, plus the
    /// default tags specified in the metric configuration.
    ///
    /// Mirrors Java's `metricName(String name, String group, String description)`
    /// (`:208`).
    pub fn metric_name_description(
        &self,
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
    ) -> MetricName {
        self.metric_name_description_tags(name, group, description, BTreeMap::new())
    }

    /// Create a `MetricName` with the given name, group and tags, plus the default
    /// tags specified in the metric configuration. Tags in `tags` take precedence
    /// over default tags with the same key.
    ///
    /// Mirrors Java's
    /// `metricName(String name, String group, Map<String, String> tags)` (`:243`).
    pub fn metric_name_tags(
        &self,
        name: impl Into<String>,
        group: impl Into<String>,
        tags: BTreeMap<String, String>,
    ) -> MetricName {
        self.metric_name_description_tags(name, group, "", tags)
    }

    /// Create a `MetricName` with name and group (empty description, default tags).
    ///
    /// Mirrors Java's `metricName(String name, String group)` (`:218`), whose
    /// parameters are the group's intersection — hence the plain name.
    pub fn metric_name(&self, name: impl Into<String>, group: impl Into<String>) -> MetricName {
        self.metric_name_description_tags(name, group, "", BTreeMap::new())
    }

    /// Create a `MetricName` from `key, value` tag pairs, plus default tags.
    /// Returns an error if the pairs are not in twos (Java
    /// `IllegalArgumentException`).
    ///
    /// Mirrors Java's
    /// `metricName(String name, String group, String description, String... keyValue)`
    /// (`:231`).
    pub fn metric_name_description_key_value(
        &self,
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        key_value: &[&str],
    ) -> Result<MetricName, Error> {
        Ok(self.metric_name_description_tags(name, group, description, MetricsUtils::get_tags(key_value)?))
    }

    /// The default config of this registry.
    pub fn config(&self) -> &Arc<MetricConfig> {
        &self.config
    }

    /// Get the sensor with the given name if it exists.
    pub fn get_sensor(&self, name: &str) -> Option<Arc<Sensor>> {
        self.sensors.lock().expect("sensors mutex poisoned").get(name).cloned()
    }

    // Java's eight `sensor` overloads (`Metrics.java:325,336,348,360,372,386,401,427`)
    // intersect on `{name}`, and `sensor(String name)` (`:325`) is exactly that —
    // so it keeps the plain name and the others carry their Rust parameters
    // beyond it (CLAUDE.md §2). Only `:401` would need more than three parameters
    // in its name, so it alone takes the `Options` shape: [`SensorOptions`] is
    // that method's only parameter and carries every Java parameter including
    // `name`, which is why its name is plain `sensor_options` with no parameter
    // names at all. `:427` keeps four parameters spelled out because its name
    // needs only three of them.

    /// Get or create a sensor with the given unique name and no parents at INFO
    /// recording level. Mirrors Java's `sensor(String name)` (`:325`).
    pub fn sensor(&self, name: &str) -> Result<Arc<Sensor>, Error> {
        self.sensor_options(SensorOptionsBuilder::new().set_name(name).build()?)
    }

    /// Get or create a sensor with the given name, recording level, and no parents.
    /// Mirrors Java's `sensor(String name, Sensor.RecordingLevel recordingLevel)` (`:336`).
    pub fn sensor_recording_level(&self, name: &str, recording_level: RecordingLevel) -> Result<Arc<Sensor>, Error> {
        self.sensor_options(
            SensorOptionsBuilder::new()
                .set_name(name)
                .set_recording_level(recording_level)
                .build()?,
        )
    }

    /// Get or create a sensor with parents at INFO recording level.
    /// Mirrors Java's `sensor(String name, Sensor... parents)` (`:348`).
    pub fn sensor_parents(&self, name: &str, parents: &[Arc<Sensor>]) -> Result<Arc<Sensor>, Error> {
        self.sensor_options(SensorOptionsBuilder::new().set_name(name).set_parents(parents).build()?)
    }

    /// Get or create a sensor with the given name, recording level and parents.
    /// Mirrors Java's
    /// `sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents)`
    /// (`:360`).
    pub fn sensor_recording_level_parents(
        &self,
        name: &str,
        recording_level: RecordingLevel,
        parents: &[Arc<Sensor>],
    ) -> Result<Arc<Sensor>, Error> {
        self.sensor_options(
            SensorOptionsBuilder::new()
                .set_name(name)
                .set_recording_level(recording_level)
                .set_parents(parents)
                .build()?,
        )
    }

    /// Get or create a sensor with a config and parents, at INFO recording level.
    /// Mirrors Java's `sensor(String name, MetricConfig config, Sensor... parents)`
    /// (`:372`).
    pub fn sensor_config_parents(
        &self,
        name: &str,
        config: Option<Arc<MetricConfig>>,
        parents: &[Arc<Sensor>],
    ) -> Result<Arc<Sensor>, Error> {
        self.sensor_config_recording_level_parents(name, config, RecordingLevel::Info, parents)
    }

    /// Get or create a sensor with a config, recording level and parents.
    /// Mirrors Java's
    /// `sensor(String name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents)`
    /// (`:386`).
    pub fn sensor_config_recording_level_parents(
        &self,
        name: &str,
        config: Option<Arc<MetricConfig>>,
        recording_level: RecordingLevel,
        parents: &[Arc<Sensor>],
    ) -> Result<Arc<Sensor>, Error> {
        self.sensor_options(
            SensorOptionsBuilder::new()
                .set_name(name)
                .set_config(config)
                .set_recording_level(recording_level)
                .set_parents(parents)
                .build()?,
        )
    }

    /// Get or create a sensor with a config, expiration and parents, at INFO
    /// recording level. Mirrors Java's
    /// `sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor... parents)`
    /// (`:427`).
    pub fn sensor_config_inactive_sensor_expiration_time_seconds_parents(
        &self,
        name: &str,
        config: Option<Arc<MetricConfig>>,
        inactive_sensor_expiration_time_seconds: i64,
        parents: &[Arc<Sensor>],
    ) -> Result<Arc<Sensor>, Error> {
        self.sensor_options(
            SensorOptionsBuilder::new()
                .set_name(name)
                .set_config(config)
                .set_inactive_sensor_expiration_time_seconds(inactive_sensor_expiration_time_seconds)
                .set_parents(parents)
                .build()?,
        )
    }

    /// Get or create a sensor with a config, expiration, recording level and parents.
    ///
    /// Mirrors Java's
    /// `sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel recordingLevel, Sensor... parents)`
    /// (`:401`). Every parameter, `name` included, is carried by
    /// [`SensorOptions`] — see the note above this overload group.
    pub fn sensor_options(&self, options: SensorOptions<'_>) -> Result<Arc<Sensor>, Error> {
        let SensorOptions { name, config, inactive_sensor_expiration_time_seconds, recording_level, parents } = options;
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

    // Java's four `addMetric` overloads (`Metrics.java:470,485,498,518`) intersect
    // on `{metricName}` alone, and no overload takes just a metric name — so
    // under CLAUDE.md §2's strict reading NOBODY keeps the plain `add_metric`,
    // and each translated form carries its Rust parameters beyond the metric
    // name. `add_gauge` below is Rust-only (Java has no `addGauge`) and so is
    // outside the rule.

    /// Add a metric to monitor a measurable. This metric won't be associated
    /// with any sensor. Mirrors Java's
    /// `addMetric(MetricName metricName, Measurable measurable)` (`:470`).
    pub fn add_metric_measurable(&self, metric_name: MetricName, measurable: Box<dyn Measurable>) -> Result<(), Error> {
        self.add_metric_config_provider(metric_name, None, MetricValueProvider::Measurable(measurable))
    }

    /// Add a metric backed by a gauge. This metric won't be associated with any
    /// sensor.
    ///
    /// Rust-only convenience — Java has no `addGauge`; callers there pass a
    /// `Gauge` through `addMetric(MetricName, MetricValueProvider)`.
    pub fn add_gauge(&self, metric_name: MetricName, gauge: Box<dyn Gauge>) -> Result<(), Error> {
        self.add_metric_config_provider(metric_name, None, MetricValueProvider::Gauge(gauge))
    }

    /// Add a metric backed by a value provider with an optional config.
    ///
    /// Mirrors Java's
    /// `addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider)`
    /// (`:498`); `config: None` covers the `(MetricName, MetricValueProvider)`
    /// form (`:518`), which Java implements by passing `null`.
    pub fn add_metric_config_provider(
        &self,
        metric_name: MetricName,
        config: Option<Arc<MetricConfig>>,
        provider: MetricValueProvider,
    ) -> Result<(), Error> {
        let metric_config = config.unwrap_or_else(|| Arc::clone(&self.config));
        let metric = Arc::new(KafkaMetric::new(
            metric_name.clone(),
            provider,
            metric_config,
            Arc::clone(&self.time),
        ));
        if self.shared.register_metric(metric).is_some() {
            return Err(Error::local_illegal_argument(format!(
                "A metric named '{metric_name}' already exists, can't register another one."
            )));
        }
        Ok(())
    }

    /// Register a metric backed by a value provider only if it is not already
    /// present. Returns the existing metric if one is already registered under
    /// the same name, otherwise the newly registered metric. Translates Java's
    /// `Metrics.addMetricIfAbsent(MetricName, MetricConfig, MetricValueProvider)`
    /// — idempotent registration (the consumer's preferred-read-replica gauge
    /// re-registers across assignment updates without error).
    pub fn add_metric_if_absent(
        &self,
        metric_name: MetricName,
        config: Option<Arc<MetricConfig>>,
        provider: MetricValueProvider,
    ) -> Arc<KafkaMetric> {
        let metric_config = config.unwrap_or_else(|| Arc::clone(&self.config));
        let metric = Arc::new(KafkaMetric::new(metric_name, provider, metric_config, Arc::clone(&self.time)));
        match self.shared.register_metric(Arc::clone(&metric)) {
            Some(existing) => existing,
            None => metric,
        }
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

    // Java's two `metricInstance` overloads (`Metrics.java:651,655`) intersect on
    // `{template}`, and neither takes just a template — so nobody keeps the plain
    // `metric_instance` (CLAUDE.md §2, strict reading).

    /// Create a `MetricName` from a template and tag pairs. Mirrors Java's
    /// `metricInstance(MetricNameTemplate template, String... keyValue)` (`:651`).
    pub fn metric_instance_key_value(
        &self,
        template: &MetricNameTemplate,
        key_value: &[&str],
    ) -> Result<MetricName, Error> {
        self.metric_instance_tags(template, MetricsUtils::get_tags(key_value)?)
    }

    /// Create a `MetricName` from a template and a tags map. Mirrors Java's
    /// `metricInstance(MetricNameTemplate template, Map<String, String> tags)` (`:655`).
    pub fn metric_instance_tags(
        &self,
        template: &MetricNameTemplate,
        tags: BTreeMap<String, String>,
    ) -> Result<MetricName, Error> {
        // Check that the runtime tags + default config tags match the template tags.
        let mut runtime_tag_keys: std::collections::HashSet<String> = tags.keys().cloned().collect();
        runtime_tag_keys.extend(self.config.tags().keys().cloned());
        let template_tag_keys: std::collections::HashSet<String> = template.tags().iter().cloned().collect();
        if runtime_tag_keys != template_tag_keys {
            return Err(Error::local_illegal_argument(format!(
                "For '{}', runtime-defined metric tags do not match the tags in the template. \
                 Runtime = {runtime_tag_keys:?} Template = {template_tag_keys:?}",
                template.name()
            )));
        }
        Ok(self.metric_name_description_tags(template.name(), template.group(), template.description(), tags))
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
    use crate::common::metrics::MockTime;
    use crate::common::metrics::stats::{CumulativeCount, CumulativeSum, Value};

    // Matches MetricsTest.EPS.
    const EPS: f64 = 0.000001;

    fn metrics_with_mock() -> (Metrics, Arc<MockTime>) {
        let time = Arc::new(MockTime::new());
        let metrics = Metrics::with_default_config_reporters_time(
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
            .metric_name_description_key_value("name", "group", "description", &["key1", "value1", "key2", "value2"])
            .unwrap();
        let mut tags = BTreeMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        tags.insert("key2".to_string(), "value2".to_string());
        let n2 = metrics.metric_name_description_tags("name", "group", "description", tags);
        assert_eq!(n1, n2, "metric names created in two different ways should be equal");

        // Creating a MetricName with an odd number of keyValue should fail.
        let err = metrics
            .metric_name_description_key_value("name", "group", "description", &["key1"])
            .unwrap_err();
        assert!(err.to_string().contains("keyValue needs to be specified in pairs"));
    }

    // MetricsTest.testMetricInstances
    //
    // Java's fixture templates come from `SampleMetrics`:
    //   METRIC1 / METRIC2 = MetricNameTemplate("name", "group", <desc>, "key1", "key2")
    //   METRIC_WITH_INHERITED_TAGS = template over {parent-tag, child-tag}
    // They are declared inline here rather than in a shared fixture module,
    // since this is the only test that uses them.
    //
    // The `JmxReporter` Java passes to the `inherited` registry is dropped: JMX
    // is out of scope (see the `Metrics` struct docs) and is incidental to the
    // assertions, which are all about template/runtime tag reconciliation.
    #[test]
    fn test_metric_instances() {
        use indexmap::IndexSet;

        let metrics = Metrics::new();

        let mut key_tags = IndexSet::new();
        key_tags.insert("key1".to_string());
        key_tags.insert("key2".to_string());
        let metric1 =
            MetricNameTemplate::new("name", "group", "The first metric used in testMetricName()", key_tags.clone());
        let metric2 = MetricNameTemplate::new("name", "group", "The second metric used in testMetricName()", key_tags);

        // The key/value-pair form and the tags-map form must agree.
        let n1 = metrics
            .metric_instance_key_value(&metric1, &["key1", "value1", "key2", "value2"])
            .expect("metric_instance from key/value pairs");
        let mut tags = BTreeMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        tags.insert("key2".to_string(), "value2".to_string());
        let n2 = metrics
            .metric_instance_tags(&metric2, tags)
            .expect("metric_instance from tags map");
        assert_eq!(n1, n2, "metric names created in two different ways should be equal");

        // An odd number of keyValue entries is rejected.
        let err = metrics
            .metric_instance_key_value(&metric1, &["key1"])
            .expect_err("odd number of keyValue should fail");
        assert!(
            err.to_string().contains("keyValue needs to be specified in pairs"),
            "unexpected message: {err}"
        );

        // A registry whose default config carries a parent tag fills that tag in
        // for templates that declare it, with the child tag supplied at runtime.
        let mut parent_tags = BTreeMap::new();
        parent_tags.insert("parent-tag".to_string(), "parent-tag-value".to_string());
        let mut child_tags = BTreeMap::new();
        child_tags.insert("child-tag".to_string(), "child-tag-value".to_string());

        let inherited = Metrics::with_default_config(Arc::new(MetricConfig::new().set_tags(parent_tags.clone())));
        let mut inherited_tag_names = IndexSet::new();
        inherited_tag_names.insert("parent-tag".to_string());
        inherited_tag_names.insert("child-tag".to_string());
        let metric_with_inherited_tags =
            MetricNameTemplate::new("name", "group", "inherited-tags metric", inherited_tag_names);

        let inherited_metric = inherited
            .metric_instance_tags(&metric_with_inherited_tags, child_tags)
            .expect("metric_instance with inherited parent tag");
        let filled_out_tags = inherited_metric.tags();
        assert_eq!(
            Some(&"parent-tag-value".to_string()),
            filled_out_tags.get("parent-tag"),
            "parent-tag should be set properly"
        );
        assert_eq!(
            Some(&"child-tag-value".to_string()),
            filled_out_tags.get("child-tag"),
            "child-tag should be set properly"
        );

        // Supplying only the parent tag at runtime leaves child-tag undefined.
        let err = inherited
            .metric_instance_tags(&metric_with_inherited_tags, parent_tags)
            .expect_err("child metric tags not defined at runtime should fail");
        assert!(
            err.to_string().contains("do not match the tags in the template"),
            "unexpected message: {err}"
        );

        // A runtime tag absent from the template is also rejected.
        let mut runtime_tags = BTreeMap::new();
        runtime_tags.insert("child-tag".to_string(), "child-tag-value".to_string());
        runtime_tags.insert("tag-not-in-template".to_string(), "unexpected-value".to_string());
        let err = inherited
            .metric_instance_tags(&metric_with_inherited_tags, runtime_tags)
            .expect_err("runtime tag not in template should fail");
        assert!(
            err.to_string().contains("do not match the tags in the template"),
            "unexpected message: {err}"
        );
    }

    // SensorTest.testIdempotentAdd (Avg/WindowedSum substituted with M1 stats)
    #[test]
    fn test_idempotent_add() {
        let metrics = Metrics::new();
        let sensor = metrics.sensor("sensor").unwrap();

        assert!(
            sensor
                .add_metric_name(metrics.metric_name("test-metric", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        // Adding the same metric to the same sensor is a no-op (returns true).
        assert!(
            sensor
                .add_metric_name(metrics.metric_name("test-metric", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        // Adding the same metric to a DIFFERENT sensor is an error.
        let another = metrics.sensor("another-sensor").unwrap();
        let err = another
            .add_metric_name(metrics.metric_name("test-metric", "test-group"), Box::new(Value::new()))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));

        // Adding a different metric with the same name is also a no-op.
        assert!(
            sensor
                .add_metric_name(metrics.metric_name("test-metric", "test-group"), Box::new(CumulativeSum::new()))
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
            .add_metric_name(
                metrics.metric_name("test.parent1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let parent2 = metrics.sensor("test.parent2").unwrap();
        parent2
            .add_metric_name(
                metrics.metric_name("test.parent2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child1 = metrics
            .sensor_parents("test.child1", &[Arc::clone(&parent1), Arc::clone(&parent2)])
            .unwrap();
        child1
            .add_metric_name(
                metrics.metric_name("test.child1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child2 = metrics.sensor_parents("test.child2", &[Arc::clone(&parent1)]).unwrap();
        child2
            .add_metric_name(
                metrics.metric_name("test.child2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let grandchild = metrics.sensor_parents("test.grandchild", &[Arc::clone(&child1)]).unwrap();
        grandchild
            .add_metric_name(
                metrics.metric_name("test.grandchild.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();

        // Increment each sensor one time.
        parent1.record();
        parent2.record();
        child1.record();
        child2.record();
        grandchild.record();

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
        let c1 = metrics.sensor_parents("child1", &[Arc::clone(&p)]).unwrap();
        let c2 = metrics.sensor_parents("child2", &[Arc::clone(&p)]).unwrap();
        match metrics.sensor_parents("gc", &[c1, c2]) {
            Ok(_) => panic!("expected circular dependency error"),
            Err(err) => assert!(err.to_string().contains("Circular dependency")),
        }
    }

    // MetricsTest.testRemoveChildSensor
    #[test]
    fn test_remove_child_sensor() {
        let metrics = Metrics::new();
        let parent = metrics.sensor("parent").unwrap();
        let child = metrics.sensor_parents("child", &[Arc::clone(&parent)]).unwrap();

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
            .add_metric_name(
                metrics.metric_name("test.parent1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let parent2 = metrics.sensor("test.parent2").unwrap();
        parent2
            .add_metric_name(
                metrics.metric_name("test.parent2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child1 = metrics
            .sensor_parents("test.child1", &[Arc::clone(&parent1), Arc::clone(&parent2)])
            .unwrap();
        child1
            .add_metric_name(
                metrics.metric_name("test.child1.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let child2 = metrics.sensor_parents("test.child2", &[Arc::clone(&parent2)]).unwrap();
        child2
            .add_metric_name(
                metrics.metric_name("test.child2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();
        let gchild1 = metrics.sensor_parents("test.gchild2", &[Arc::clone(&child2)]).unwrap();
        gchild1
            .add_metric_name(
                metrics.metric_name("test.gchild2.count", "grp1"),
                Box::new(CumulativeCount::new()),
            )
            .unwrap();

        assert!(metrics.get_sensor("test.parent1").is_some());
        metrics.remove_sensor("test.parent1");
        assert!(metrics.get_sensor("test.parent1").is_none());
        assert!(metrics.metric(&metrics.metric_name("test.parent1.count", "grp1")).is_none());
        // child1's only path to removal is via parent1; it is removed too.
        assert!(metrics.get_sensor("test.child1").is_none());
        assert!(metrics.children_sensors(&parent1).is_none());
        assert!(metrics.metric(&metrics.metric_name("test.child1.count", "grp1")).is_none());

        assert!(metrics.get_sensor("test.gchild2").is_some());
        metrics.remove_sensor("test.gchild2");
        assert!(metrics.get_sensor("test.gchild2").is_none());
        assert!(metrics.children_sensors(&gchild1).is_none());
        assert!(metrics.metric(&metrics.metric_name("test.gchild2.count", "grp1")).is_none());

        assert!(metrics.get_sensor("test.child2").is_some());
        metrics.remove_sensor("test.child2");
        assert!(metrics.get_sensor("test.child2").is_none());
        assert!(metrics.children_sensors(&child2).is_none());
        assert!(metrics.metric(&metrics.metric_name("test.child2.count", "grp1")).is_none());

        assert!(metrics.get_sensor("test.parent2").is_some());
        metrics.remove_sensor("test.parent2");
        assert!(metrics.get_sensor("test.parent2").is_none());
        assert!(metrics.children_sensors(&parent2).is_none());
        assert!(metrics.metric(&metrics.metric_name("test.parent2.count", "grp1")).is_none());

        assert_eq!(size, metrics.metrics().len());
    }

    // MetricsTest.testRemoveMetric (WindowedCount substituted with CumulativeCount)
    #[test]
    fn test_remove_metric() {
        let metrics = Metrics::new();
        let size = metrics.metrics().len();
        metrics
            .add_metric_measurable(metrics.metric_name("test1", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();
        metrics
            .add_metric_measurable(metrics.metric_name("test2", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();

        assert!(metrics.remove_metric(&metrics.metric_name("test1", "grp1")).is_some());
        assert!(metrics.metric(&metrics.metric_name("test1", "grp1")).is_none());
        assert!(metrics.metric(&metrics.metric_name("test2", "grp1")).is_some());

        assert!(metrics.remove_metric(&metrics.metric_name("test2", "grp1")).is_some());
        assert!(metrics.metric(&metrics.metric_name("test2", "grp1")).is_none());

        assert_eq!(size, metrics.metrics().len());
    }

    // MetricsTest.testDuplicateMetricName (Avg/CumulativeSum substituted)
    #[test]
    fn test_duplicate_metric_name() {
        let metrics = Metrics::new();
        metrics
            .sensor("test")
            .unwrap()
            .add_metric_name(metrics.metric_name("test", "grp1"), Box::new(Value::new()))
            .unwrap();
        let err = metrics
            .sensor("test2")
            .unwrap()
            .add_metric_name(metrics.metric_name("test", "grp1"), Box::new(CumulativeSum::new()))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // MetricsTest.testRemoveInactiveMetrics (WindowedCount substituted; ExpireSensorTask → expire_sensors)
    #[test]
    fn test_remove_inactive_metrics() {
        let (metrics, time) = metrics_with_mock();

        let s1 = metrics
            .sensor_options(
                SensorOptionsBuilder::new()
                    .set_name("test.s1")
                    .set_inactive_sensor_expiration_time_seconds(1)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        s1.add_metric_name(metrics.metric_name("test.s1.count", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();

        let s2 = metrics
            .sensor_options(
                SensorOptionsBuilder::new()
                    .set_name("test.s2")
                    .set_inactive_sensor_expiration_time_seconds(3)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        s2.add_metric_name(metrics.metric_name("test.s2.count", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();

        metrics.expire_sensors();
        assert!(metrics.get_sensor("test.s1").is_some(), "Sensor test.s1 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s1.count", "grp1")).is_some());
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_some());

        time.sleep(1001);
        metrics.expire_sensors();
        assert!(
            metrics.get_sensor("test.s1").is_none(),
            "Sensor test.s1 should have been purged"
        );
        assert!(metrics.metric(&metrics.metric_name("test.s1.count", "grp1")).is_none());
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_some());

        // Record on s2; resets its clock so it is not purged at the 3s mark.
        s2.record();
        time.sleep(2000);
        metrics.expire_sensors();
        assert!(metrics.get_sensor("test.s2").is_some(), "Sensor test.s2 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_some());

        // After another 1001ms, the metric should be purged.
        time.sleep(1001);
        metrics.expire_sensors();
        assert!(
            metrics.get_sensor("test.s2").is_none(),
            "Sensor test.s2 should have been purged"
        );
        assert!(metrics.metric(&metrics.metric_name("test.s2.count", "grp1")).is_none());

        // After purging, it should be possible to recreate a metric.
        let s1 = metrics
            .sensor_options(
                SensorOptionsBuilder::new()
                    .set_name("test.s1")
                    .set_inactive_sensor_expiration_time_seconds(1)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        s1.add_metric_name(metrics.metric_name("test.s1.count", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();
        assert!(metrics.get_sensor("test.s1").is_some(), "Sensor test.s1 must be present");
        assert!(metrics.metric(&metrics.metric_name("test.s1.count", "grp1")).is_some());
    }

    // The M1-relevant part of MetricsTest.testSimpleStats: the CumulativeSum row.
    #[test]
    fn test_simple_stats_cumulative() {
        let metrics = Metrics::new();
        let s2 = metrics.sensor("test.sensor2").unwrap();
        s2.add_metric_name(metrics.metric_name("s2.total", "grp1"), Box::new(CumulativeSum::new()))
            .unwrap();
        s2.record_value(5.0);
        assert_eq!(
            5.0,
            double_value(&metrics.metric(&metrics.metric_name("s2.total", "grp1")).unwrap()),
            "s2 reflects the constant value"
        );

        // CumulativeCount counts invocations regardless of recorded value.
        let s = metrics.sensor("test.sensor").unwrap();
        s.add_metric_name(metrics.metric_name("test.count", "grp1"), Box::new(CumulativeCount::new()))
            .unwrap();
        for i in 0..10 {
            s.record_value(i as f64);
        }
        assert_eq!(
            10.0,
            double_value(&metrics.metric(&metrics.metric_name("test.count", "grp1")).unwrap()),
            "Count(0...9) = 10"
        );
    }

    // MetricsTest.testSimpleStats — the Avg/Max/Min/Rate/occurrences/count rows
    // (M2). The CumulativeSum (s2.total) and count rows were already covered by
    // `test_simple_stats_cumulative` (M1); they are re-asserted here for the full
    // method. The Percentiles row is OUT OF SCOPE (consumer doesn't use them; see
    // Phase M2 PLAN Skips) and is therefore omitted.
    #[test]
    fn test_simple_stats() {
        use crate::common::metrics::internals::TimeUnit;
        use crate::common::metrics::stats::{Avg, Max, Meter, Min, WindowedCount};

        let (metrics, time) = metrics_with_mock();
        let config = metrics.config().clone();

        let s = metrics.sensor("test.sensor").unwrap();
        s.add_metric_name(metrics.metric_name("test.avg", "grp1"), Box::new(Avg::new()))
            .unwrap();
        s.add_metric_name(metrics.metric_name("test.max", "grp1"), Box::new(Max::new()))
            .unwrap();
        s.add_metric_name(metrics.metric_name("test.min", "grp1"), Box::new(Min::new()))
            .unwrap();
        s.add(Box::new(Meter::with_unit(
            TimeUnit::Seconds,
            metrics.metric_name("test.rate", "grp1"),
            metrics.metric_name("test.total", "grp1"),
        )))
        .unwrap();
        s.add(Box::new(Meter::with_rate_stat(
            std::sync::Arc::new(WindowedCount::new().into_sampled_stat()),
            metrics.metric_name("test.occurrences", "grp1"),
            metrics.metric_name("test.occurrences.total", "grp1"),
        )))
        .unwrap();
        s.add_metric_name(metrics.metric_name("test.count", "grp1"), Box::new(WindowedCount::new()))
            .unwrap();

        let s2 = metrics.sensor("test.sensor2").unwrap();
        s2.add_metric_name(metrics.metric_name("s2.total", "grp1"), Box::new(CumulativeSum::new()))
            .unwrap();
        s2.record_value(5.0);

        let mut sum = 0i64;
        let count = 10i64;
        for i in 0..count {
            s.record_value(i as f64);
            sum += i;
        }

        // prior to any time passing
        let mut elapsed_secs = (config.time_window_ms() * (config.samples() as i64 - 1)) as f64 / 1000.0;
        assert!(
            (count as f64 / elapsed_secs
                - double_value(&metrics.metric(&metrics.metric_name("test.occurrences", "grp1")).unwrap()))
            .abs()
                <= EPS,
            "Occurrences(0...{count})"
        );

        // pretend 2 seconds passed...
        let sleep_time_ms = 2i64;
        time.sleep(sleep_time_ms * 1000);
        elapsed_secs += sleep_time_ms as f64;

        assert!(
            (5.0 - double_value(&metrics.metric(&metrics.metric_name("s2.total", "grp1")).unwrap())).abs() <= EPS,
            "s2 reflects the constant value"
        );
        assert!(
            (4.5 - double_value(&metrics.metric(&metrics.metric_name("test.avg", "grp1")).unwrap())).abs() <= EPS,
            "Avg(0...9) = 4.5"
        );
        assert!(
            ((count - 1) as f64 - double_value(&metrics.metric(&metrics.metric_name("test.max", "grp1")).unwrap()))
                .abs()
                <= EPS,
            "Max(0...9) = 9"
        );
        assert!(
            (0.0 - double_value(&metrics.metric(&metrics.metric_name("test.min", "grp1")).unwrap())).abs() <= EPS,
            "Min(0...9) = 0"
        );
        assert!(
            (sum as f64 / elapsed_secs
                - double_value(&metrics.metric(&metrics.metric_name("test.rate", "grp1")).unwrap()))
            .abs()
                <= EPS,
            "Rate(0...9)"
        );
        assert!(
            (count as f64 / elapsed_secs
                - double_value(&metrics.metric(&metrics.metric_name("test.occurrences", "grp1")).unwrap()))
            .abs()
                <= EPS,
            "Occurrences(0...{count})"
        );
        assert!(
            (count as f64 - double_value(&metrics.metric(&metrics.metric_name("test.count", "grp1")).unwrap())).abs()
                <= EPS,
            "Count(0...9) = 10"
        );
    }

    // MetricsTest.testRateWindowing
    #[test]
    fn test_rate_windowing() {
        use crate::common::metrics::internals::{MetricsUtils, TimeUnit};
        use crate::common::metrics::stats::{Meter, WindowedCount};

        let time = Arc::new(MockTime::new());
        // Use the default time window. Set 3 samples.
        let cfg = Arc::new(MetricConfig::new().set_samples(3));
        let metrics = Metrics::with_default_config_reporters_time(
            Arc::clone(&cfg),
            Vec::new(),
            Arc::clone(&time) as Arc<dyn Time>,
        );

        let s = metrics
            .sensor_options(
                SensorOptionsBuilder::new()
                    .set_name("test.sensor")
                    .set_config(Some(Arc::clone(&cfg)))
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let rate_metric_name = metrics.metric_name("test.rate", "grp1");
        let total_metric_name = metrics.metric_name("test.total", "grp1");
        let count_rate_metric_name = metrics.metric_name("test.count.rate", "grp1");
        let count_total_metric_name = metrics.metric_name("test.count.total", "grp1");
        s.add(Box::new(Meter::with_unit(
            TimeUnit::Seconds,
            rate_metric_name.clone(),
            total_metric_name.clone(),
        )))
        .unwrap();
        s.add(Box::new(Meter::with_rate_stat(
            Arc::new(WindowedCount::new().into_sampled_stat()),
            count_rate_metric_name.clone(),
            count_total_metric_name.clone(),
        )))
        .unwrap();
        let total_metric = metrics.metrics().get(&total_metric_name).unwrap().clone();
        let count_total_metric = metrics.metrics().get(&count_total_metric_name).unwrap().clone();

        let mut sum = 0i64;
        let count = cfg.samples() as i64 - 1;
        // Advance 1 window after every record.
        for _ in 0..count {
            s.record_value(100.0);
            sum += 100;
            time.sleep(cfg.time_window_ms());
            assert!((sum as f64 - double_value(&total_metric)).abs() <= EPS);
        }

        // Sleep for half the window.
        time.sleep(cfg.time_window_ms() / 2);

        // elapsedSecs = sampleWindowSize * (total samples - half of final sample)
        let elapsed_secs =
            MetricsUtils::convert(cfg.time_window_ms(), TimeUnit::Seconds) * (cfg.samples() as f64 - 0.5);

        let rate_metric = metrics.metrics().get(&rate_metric_name).unwrap().clone();
        let count_rate_metric = metrics.metrics().get(&count_rate_metric_name).unwrap().clone();
        assert!(
            (sum as f64 / elapsed_secs - double_value(&rate_metric)).abs() <= EPS,
            "Rate(0...2)"
        );
        assert!(
            (count as f64 / elapsed_secs - double_value(&count_rate_metric)).abs() <= EPS,
            "Count rate(0...2)"
        );
        // Java additionally casts `rateMetric.measurable()` back to `Rate` and
        // asserts `windowSize == 75s` (== `elapsed_secs` here). Our erased
        // `MetricValueProvider` carries no `Any` downcast seam (adding one would
        // touch every M1 `Measurable` impl), so that single line is not ported.
        // It is fully covered indirectly: the `Rate(0...2)` value assertion above
        // pins the window transitively (rate = sampledValue / windowSize, with
        // sampledValue == `sum`, so a wrong windowSize fails that assertion), and
        // `Rate::window_size` has dedicated bit-for-bit tests in
        // `rate.rs::test_rate_with_no_prior_available_samples`.
        assert!((sum as f64 - double_value(&total_metric)).abs() <= EPS);
        assert!((count as f64 - double_value(&count_total_metric)).abs() <= EPS);

        // Verify that rates are expired, but total is cumulative.
        time.sleep(cfg.time_window_ms() * cfg.samples() as i64);
        assert!((0.0 - double_value(&rate_metric)).abs() <= EPS);
        assert!((0.0 - double_value(&count_rate_metric)).abs() <= EPS);
        assert!((sum as f64 - double_value(&total_metric)).abs() <= EPS);
        assert!((count as f64 - double_value(&count_total_metric)).abs() <= EPS);
    }

    // The kafka-metrics-count gauge is registered on construction.
    #[test]
    fn count_metric_registered_on_construction() {
        let metrics = Metrics::new();
        let count_name = metrics.metric_name_description_tags(
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
            .add_metric_measurable(metrics.metric_name("extra", "grp1"), Box::new(Value::new()))
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

        let name = metrics
            .metric_instance_key_value(&template, &["client-id", "client-1"])
            .unwrap();
        assert_eq!(name.name(), "name");
        assert_eq!(name.tags().get("client-id").map(String::as_str), Some("client-1"));

        // Wrong tag keys → error.
        let err = metrics.metric_instance_key_value(&template, &["wrong", "v"]).unwrap_err();
        assert!(err.to_string().contains("do not match the tags in the template"));
    }

    // ---------------------------------------------------------------------
    // Java `default`-style forwarding overloads added to complete the groups
    // (DoD #2). Each asserts the added form agrees, by value, with the form it
    // forwards to — not merely that it returns.
    // ---------------------------------------------------------------------

    // Metrics(MetricConfig defaultConfig, Time time) -> Metrics.java:101
    #[test]
    fn test_new_default_config_time_forwards() {
        let config = Arc::new(MetricConfig::new().set_samples(7));
        let time = Arc::new(MockTime::new());
        let metrics = Metrics::with_default_config_time(Arc::clone(&config), Arc::clone(&time) as Arc<dyn Time>);

        // The default config is the one we passed, not a fresh one.
        assert!(Arc::ptr_eq(metrics.config(), &config));
        assert_eq!(metrics.config().samples(), 7);

        // ...and the clock is ours, not `SystemTime`: a sensor with a 1s
        // inactivity window expires only if the mock clock drives it.
        let s = metrics
            .sensor_config_inactive_sensor_expiration_time_seconds_parents("s", None, 1, &[])
            .unwrap();
        assert!(!s.has_expired());
        time.sleep(2_000);
        assert!(s.has_expired());
    }

    // metricName(String name, String group, String description) -> Metrics.java:208
    #[test]
    fn test_metric_name_description_forwards() {
        let mut default_tags = BTreeMap::new();
        default_tags.insert("client-id".to_string(), "c1".to_string());
        let metrics = Metrics::with_default_config(Arc::new(MetricConfig::new().set_tags(default_tags)));

        let added = metrics.metric_name_description("n", "g", "the description");
        let forwarded = metrics.metric_name_description_tags("n", "g", "the description", BTreeMap::new());
        assert_eq!(added, forwarded);
        // `MetricName` equality deliberately ignores `description`, so assert it
        // separately or this test would pass with the description dropped.
        assert_eq!(added.description(), "the description");
        assert_eq!(added.name(), "n");
        assert_eq!(added.group(), "g");
        assert_eq!(added.tags().get("client-id").map(String::as_str), Some("c1"));
    }

    // metricName(String name, String group, Map<String, String> tags) -> Metrics.java:243
    #[test]
    fn test_metric_name_tags_forwards() {
        let mut default_tags = BTreeMap::new();
        default_tags.insert("client-id".to_string(), "c1".to_string());
        let metrics = Metrics::with_default_config(Arc::new(MetricConfig::new().set_tags(default_tags)));

        let mut tags = BTreeMap::new();
        tags.insert("node-id".to_string(), "n7".to_string());

        let added = metrics.metric_name_tags("n", "g", tags.clone());
        assert_eq!(added, metrics.metric_name_description_tags("n", "g", "", tags));
        assert_eq!(added.description(), "");
        // Supplied tags are merged on top of the configured default tags.
        assert_eq!(added.tags().get("client-id").map(String::as_str), Some("c1"));
        assert_eq!(added.tags().get("node-id").map(String::as_str), Some("n7"));
    }

    // The four forwarding `sensor` overloads:
    //   :360 (name, recordingLevel, parents...)
    //   :372 (name, config, parents...)
    //   :386 (name, config, recordingLevel, parents...)
    //   :427 (name, config, inactiveSensorExpirationTimeSeconds, parents...)
    // Sensors are cached by name, so each form gets its own name and is compared
    // against the canonical `sensor_options` form by observable state.
    #[test]
    fn test_sensor_forwarding_overloads() {
        let (metrics, time) = metrics_with_mock();
        let config = Arc::new(MetricConfig::new().set_samples(9));
        let parent = metrics.sensor("parent").unwrap();

        // :360 — recording level and parents, default config, no expiry.
        let s = metrics
            .sensor_recording_level_parents("a", RecordingLevel::Debug, std::slice::from_ref(&parent))
            .unwrap();
        assert!(!s.should_record(), "DEBUG sensor must not record under the default INFO config");
        assert_eq!(s.parents().len(), 1);
        assert!(Arc::ptr_eq(&s.parents()[0], &parent));

        // :372 — config and parents, INFO level (Java's stated default).
        let s = metrics
            .sensor_config_parents("b", Some(Arc::clone(&config)), std::slice::from_ref(&parent))
            .unwrap();
        assert!(s.should_record());
        assert!(Arc::ptr_eq(&s.parents()[0], &parent));
        s.add_metric_name(metrics.metric_name("b.count", "grp"), Box::new(CumulativeCount::new()))
            .unwrap();
        assert!(Arc::ptr_eq(
            &metrics.metric(&metrics.metric_name("b.count", "grp")).unwrap().config(),
            &config
        ));

        // :386 — config, recording level and parents.
        let s = metrics
            .sensor_config_recording_level_parents(
                "c",
                Some(Arc::clone(&config)),
                RecordingLevel::Debug,
                std::slice::from_ref(&parent),
            )
            .unwrap();
        assert!(!s.should_record());
        assert!(Arc::ptr_eq(&s.parents()[0], &parent));
        s.add_metric_name(metrics.metric_name("c.count", "grp"), Box::new(CumulativeCount::new()))
            .unwrap();
        assert!(Arc::ptr_eq(
            &metrics.metric(&metrics.metric_name("c.count", "grp")).unwrap().config(),
            &config
        ));

        // :427 — config, expiry and parents, INFO level.
        let s = metrics
            .sensor_config_inactive_sensor_expiration_time_seconds_parents(
                "d",
                Some(Arc::clone(&config)),
                1,
                std::slice::from_ref(&parent),
            )
            .unwrap();
        assert!(s.should_record());
        assert!(Arc::ptr_eq(&s.parents()[0], &parent));
        assert!(!s.has_expired());
        time.sleep(2_000);
        assert!(s.has_expired(), "the 1s inactivity window must reach the underlying sensor");

        // The canonical `sensor_options` form, given the same arguments, agrees.
        let canonical = metrics
            .sensor_options(
                SensorOptionsBuilder::new()
                    .set_name("e")
                    .set_config(Some(Arc::clone(&config)))
                    .set_inactive_sensor_expiration_time_seconds(1)
                    .set_parents(std::slice::from_ref(&parent))
                    .build()
                    .unwrap(),
            )
            .unwrap();
        assert!(canonical.should_record());
        assert!(Arc::ptr_eq(&canonical.parents()[0], &parent));
    }

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`SensorOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn sensor_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = SensorOptionsBuilder::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "SensorOptionsBuilder::build: mandatory parameter `name` was not set"
        );
    }
}
