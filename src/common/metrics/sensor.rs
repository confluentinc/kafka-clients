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

//! A sensor applies a continuous sequence of numerical values to a set of
//! associated metrics (`org.apache.kafka.common.metrics.Sensor`).

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crate::common::metrics::metrics::MetricsShared;
use crate::common::metrics::{KafkaMetric, MeasurableStat, MetricConfig, MetricValueProvider, Stat, Time};
use crate::common::{KafkaError, MetricName};

/// The recording level of a sensor or metric config.
///
/// Mirrors `Sensor.RecordingLevel`. The numeric ids are part of the protocol and
/// must not change: INFO=0, DEBUG=1, TRACE=2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordingLevel {
    /// INFO level (id 0).
    Info,
    /// DEBUG level (id 1).
    Debug,
    /// TRACE level (id 2).
    Trace,
}

impl RecordingLevel {
    /// The permanent and immutable id of this level.
    pub fn id(&self) -> i16 {
        match self {
            RecordingLevel::Info => 0,
            RecordingLevel::Debug => 1,
            RecordingLevel::Trace => 2,
        }
    }

    /// An English description of the level.
    pub fn name(&self) -> &'static str {
        match self {
            RecordingLevel::Info => "INFO",
            RecordingLevel::Debug => "DEBUG",
            RecordingLevel::Trace => "TRACE",
        }
    }

    /// Lookup by id. Returns an error for an unknown id, mirroring Java's
    /// `IllegalArgumentException`.
    pub fn for_id(id: i16) -> Result<RecordingLevel, KafkaError> {
        match id {
            0 => Ok(RecordingLevel::Info),
            1 => Ok(RecordingLevel::Debug),
            2 => Ok(RecordingLevel::Trace),
            _ => Err(KafkaError::illegal_argument(format!(
                "Unexpected RecordLevel id `{id}`, it should be between `0` and `2` (inclusive)"
            ))),
        }
    }

    /// Case-insensitive lookup by name. Returns an error for an unknown name,
    /// mirroring Java's `valueOf` `IllegalArgumentException`.
    pub fn for_name(name: &str) -> Result<RecordingLevel, KafkaError> {
        match name.to_uppercase().as_str() {
            "INFO" => Ok(RecordingLevel::Info),
            "DEBUG" => Ok(RecordingLevel::Debug),
            "TRACE" => Ok(RecordingLevel::Trace),
            other => Err(KafkaError::illegal_argument(format!("No enum constant RecordingLevel.{other}"))),
        }
    }

    /// Whether a sensor at this level should record given the config's level id.
    pub fn should_record(&self, config_id: i16) -> bool {
        if config_id == RecordingLevel::Info.id() {
            self.id() == RecordingLevel::Info.id()
        } else if config_id == RecordingLevel::Debug.id() {
            self.id() == RecordingLevel::Info.id() || self.id() == RecordingLevel::Debug.id()
        } else if config_id == RecordingLevel::Trace.id() {
            true
        } else {
            // Java throws IllegalStateException; this is an internal invariant
            // violation, treated as a programming error.
            panic!("Did not recognize recording level {config_id}")
        }
    }
}

/// A registered stat paired with the config it should be recorded with.
///
/// Java's `StatAndConfig` holds the stat plus a `Supplier<MetricConfig>`. For the
/// `add(MetricName, MeasurableStat, config)` path the supplier is `metric::config`
/// so the stat always reads the backing metric's (possibly updated) config. M1
/// implements only that path; the `add(CompoundStat, config)` path (a constant
/// `statConfig` supplier) arrives with the compound/windowed stats in M2.
struct StatAndConfig {
    stat: Box<dyn Stat>,
    metric: Arc<KafkaMetric>,
}

impl StatAndConfig {
    fn record(&self, value: f64, time_ms: i64) {
        self.stat.record(&self.metric.config(), value, time_ms);
    }
}

/// A sensor applies a continuous sequence of numerical values to a set of
/// associated metrics. For example a sensor on message size would record a
/// sequence of message sizes using the `record` api and would maintain a set of
/// metrics about request sizes such as the average or max.
pub struct Sensor {
    registry: Option<Arc<MetricsShared>>,
    name: String,
    parents: Vec<Arc<Sensor>>,
    inner: Mutex<SensorInner>,
    config: Arc<MetricConfig>,
    time: Arc<dyn Time>,
    last_record_time: AtomicI64,
    inactive_sensor_expiration_time_ms: i64,
    recording_level: RecordingLevel,
}

/// The mutable interior of a sensor, guarded by a single `Mutex` mirroring
/// Java's `synchronized` on the sensor object.
struct SensorInner {
    stats: Vec<StatAndConfig>,
    metrics: indexmap::IndexMap<MetricName, Arc<KafkaMetric>>,
}

impl Sensor {
    /// Create a sensor.
    ///
    /// `registry` is `None` for standalone sensors (mirroring Java's `null`
    /// registry in `SensorTest`), in which case `add` does not register metrics
    /// in a global repository.
    pub(crate) fn new(
        registry: Option<Arc<MetricsShared>>,
        name: impl Into<String>,
        parents: Vec<Arc<Sensor>>,
        config: Arc<MetricConfig>,
        time: Arc<dyn Time>,
        inactive_sensor_expiration_time_seconds: i64,
        recording_level: RecordingLevel,
    ) -> Result<Self, KafkaError> {
        let now = time.milliseconds();
        let sensor = Sensor {
            registry,
            name: name.into(),
            parents,
            inner: Mutex::new(SensorInner { stats: Vec::new(), metrics: indexmap::IndexMap::new() }),
            config,
            time,
            last_record_time: AtomicI64::new(now),
            inactive_sensor_expiration_time_ms: inactive_sensor_expiration_time_seconds.saturating_mul(1000),
            recording_level,
        };
        // Validate that this sensor doesn't reference itself.
        let mut seen: HashSet<*const Sensor> = HashSet::new();
        sensor.check_forest(&mut seen)?;
        Ok(sensor)
    }

    /// Validate that this sensor doesn't end up referencing itself.
    fn check_forest(&self, sensors: &mut HashSet<*const Sensor>) -> Result<(), KafkaError> {
        if !sensors.insert(self as *const Sensor) {
            return Err(KafkaError::illegal_argument(format!(
                "Circular dependency in sensors: {} is its own parent.",
                self.name()
            )));
        }
        for parent in &self.parents {
            parent.check_forest(sensors)?;
        }
        Ok(())
    }

    /// The name this sensor is registered with.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The parents of this sensor.
    pub(crate) fn parents(&self) -> &[Arc<Sensor>] {
        &self.parents
    }

    /// Whether the sensor's record level indicates that the metric will be
    /// recorded.
    pub fn should_record(&self) -> bool {
        self.recording_level.should_record(self.config.record_level().id())
    }

    /// Record an occurrence; short-hand for `record(1.0)`.
    pub fn record_occurrence(&self) {
        if self.should_record() {
            self.record_internal(1.0, self.time.milliseconds());
        }
    }

    /// Record a value with this sensor at the current time.
    pub fn record(&self, value: f64) {
        if self.should_record() {
            self.record_internal(value, self.time.milliseconds());
        }
    }

    /// Record a value at a known time.
    pub fn record_at(&self, value: f64, time_ms: i64) {
        if self.should_record() {
            self.record_internal(value, time_ms);
        }
    }

    fn record_internal(&self, value: f64, time_ms: i64) {
        self.last_record_time.store(time_ms, Ordering::SeqCst);
        {
            let inner = self.inner.lock().expect("sensor mutex poisoned");
            // Increment all the stats.
            for stat_and_config in &inner.stats {
                stat_and_config.record(value, time_ms);
            }
        }
        // Quota enforcement is part of Phase M2 (needs windowed stats); Java's
        // checkQuotas runs here.
        for parent in &self.parents {
            parent.record_at(value, time_ms);
        }
    }

    /// Register a metric with this sensor.
    ///
    /// Returns `Ok(true)` if added, `Ok(false)` if the sensor is expired,
    /// `Err` if the metric name already exists in the registry under a
    /// different sensor (Java's `IllegalArgumentException`).
    pub fn add(&self, metric_name: MetricName, stat: Box<dyn MeasurableStat>) -> Result<bool, KafkaError> {
        self.add_with_config(metric_name, stat, None)
    }

    /// Register a metric with this sensor with an optional per-metric config.
    pub fn add_with_config(
        &self,
        metric_name: MetricName,
        stat: Box<dyn MeasurableStat>,
        config: Option<Arc<MetricConfig>>,
    ) -> Result<bool, KafkaError> {
        if self.has_expired() {
            return Ok(false);
        }
        let mut inner = self.inner.lock().expect("sensor mutex poisoned");
        if inner.metrics.contains_key(&metric_name) {
            return Ok(true);
        }
        let stat_config = config.unwrap_or_else(|| Arc::clone(&self.config));

        // A `MeasurableStat` is both `Stat` and `Measurable`. We need the stat
        // both as a value provider (Measurable) for the KafkaMetric and as a
        // recordable Stat for the sensor; they must be the SAME object so a
        // record is reflected in the measured value. We hold it behind an `Arc`
        // and expose both views.
        let stat: Arc<dyn MeasurableStat> = Arc::from(stat);
        let metric = Arc::new(KafkaMetric::new(
            metric_name.clone(),
            MetricValueProvider::Measurable(Box::new(MeasurableArc(Arc::clone(&stat)))),
            Arc::clone(&stat_config),
            Arc::clone(&self.time),
        ));

        if let Some(registry) = &self.registry {
            let existing = registry.register_metric(Arc::clone(&metric));
            if existing.is_some() {
                return Err(KafkaError::illegal_argument(format!(
                    "A metric named '{metric_name}' already exists, can't register another one."
                )));
            }
        }
        inner.metrics.insert(metric_name, Arc::clone(&metric));
        inner.stats.push(StatAndConfig { stat: Box::new(StatArc(stat)), metric });
        Ok(true)
    }

    /// Return if metrics were registered with this sensor.
    pub fn has_metrics(&self) -> bool {
        !self.inner.lock().expect("sensor mutex poisoned").metrics.is_empty()
    }

    /// Return true if the sensor is eligible for removal due to inactivity.
    pub fn has_expired(&self) -> bool {
        (self.time.milliseconds() - self.last_record_time.load(Ordering::SeqCst))
            > self.inactive_sensor_expiration_time_ms
    }

    /// The metrics registered with this sensor.
    pub fn metrics(&self) -> Vec<Arc<KafkaMetric>> {
        self.inner
            .lock()
            .expect("sensor mutex poisoned")
            .metrics
            .values()
            .cloned()
            .collect()
    }
}

/// A `Stat` view over an `Arc<dyn MeasurableStat>`, so the recordable stat and
/// the metric's value provider share the same underlying object.
struct StatArc(Arc<dyn MeasurableStat>);

impl Stat for StatArc {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.0.record(config, value, time_ms);
    }
}

/// A `Measurable` view over an `Arc<dyn MeasurableStat>`.
struct MeasurableArc(Arc<dyn MeasurableStat>);

impl crate::common::metrics::Measurable for MeasurableArc {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.0.measure(config, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::SystemTime;
    use crate::common::metrics::stats::{CumulativeCount, Value};
    use std::collections::BTreeMap;

    fn level_id(level: RecordingLevel) -> i16 {
        level.id()
    }

    fn info_config() -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().with_record_level(RecordingLevel::Info))
    }
    fn debug_config() -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().with_record_level(RecordingLevel::Debug))
    }
    fn trace_config() -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().with_record_level(RecordingLevel::Trace))
    }

    fn standalone(config: Arc<MetricConfig>, expiration_secs: i64, level: RecordingLevel) -> Sensor {
        Sensor::new(None, "sensor", Vec::new(), config, Arc::new(SystemTime), expiration_secs, level).unwrap()
    }

    fn name(n: &str, g: &str) -> MetricName {
        MetricName::new(n, g, "", BTreeMap::new())
    }

    // SensorTest.testRecordLevelEnum
    #[test]
    fn test_record_level_enum() {
        let config_level = RecordingLevel::Info;
        assert!(RecordingLevel::Info.should_record(level_id(config_level)));
        assert!(!RecordingLevel::Debug.should_record(level_id(config_level)));
        assert!(!RecordingLevel::Trace.should_record(level_id(config_level)));

        let config_level = RecordingLevel::Debug;
        assert!(RecordingLevel::Info.should_record(level_id(config_level)));
        assert!(RecordingLevel::Debug.should_record(level_id(config_level)));
        assert!(!RecordingLevel::Trace.should_record(level_id(config_level)));

        let config_level = RecordingLevel::Trace;
        assert!(RecordingLevel::Info.should_record(level_id(config_level)));
        assert!(RecordingLevel::Debug.should_record(level_id(config_level)));
        assert!(RecordingLevel::Trace.should_record(level_id(config_level)));

        assert_eq!(
            RecordingLevel::Debug,
            RecordingLevel::for_name(RecordingLevel::Debug.name()).unwrap()
        );
        assert_eq!(
            RecordingLevel::Info,
            RecordingLevel::for_name(RecordingLevel::Info.name()).unwrap()
        );
        assert_eq!(
            RecordingLevel::Trace,
            RecordingLevel::for_name(RecordingLevel::Trace.name()).unwrap()
        );
    }

    // SensorTest.testShouldRecordForInfoLevelSensor
    #[test]
    fn test_should_record_for_info_level_sensor() {
        assert!(standalone(info_config(), 0, RecordingLevel::Info).should_record());
        assert!(standalone(debug_config(), 0, RecordingLevel::Info).should_record());
        assert!(standalone(trace_config(), 0, RecordingLevel::Info).should_record());
    }

    // SensorTest.testShouldRecordForDebugLevelSensor
    #[test]
    fn test_should_record_for_debug_level_sensor() {
        assert!(!standalone(info_config(), 0, RecordingLevel::Debug).should_record());
        assert!(standalone(debug_config(), 0, RecordingLevel::Debug).should_record());
        assert!(standalone(trace_config(), 0, RecordingLevel::Debug).should_record());
    }

    // SensorTest.testShouldRecordForTraceLevelSensor
    #[test]
    fn test_should_record_for_trace_level_sensor() {
        assert!(!standalone(info_config(), 0, RecordingLevel::Trace).should_record());
        assert!(!standalone(debug_config(), 0, RecordingLevel::Trace).should_record());
        assert!(standalone(trace_config(), 0, RecordingLevel::Trace).should_record());
    }

    // SensorTest.shouldReturnPresenceOfMetrics (standalone-sensor part)
    #[test]
    fn should_return_presence_of_metrics() {
        let sensor = standalone(Arc::new(MetricConfig::new()), i64::MAX, RecordingLevel::Info);
        assert!(!sensor.has_metrics());
        sensor.add(name("name1", "group1"), Box::new(CumulativeCount::new())).unwrap();
        assert!(sensor.has_metrics());
        sensor.add(name("name2", "group2"), Box::new(Value::new())).unwrap();
        assert!(sensor.has_metrics());
    }
}
