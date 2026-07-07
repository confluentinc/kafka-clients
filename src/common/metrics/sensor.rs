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

//! Sensors and their recording levels.
//!
//! A sensor applies a continuous sequence of numerical values to a set of
//! associated metrics. Its [`RecordingLevel`] controls the verbosity at which
//! measurements are recorded.
//!
//! Translated from `org.apache.kafka.common.metrics.Sensor`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use indexmap::IndexMap;

use crate::common::metrics::kafka_metric::TimeSource;
use crate::common::metrics::metrics::MetricsCore;
use crate::common::metrics::stats::TokenBucket;
use crate::common::metrics::{
    CompoundStat, KafkaMetric, Measurable, MeasurableStat, MetricConfig, MetricValueProvider, QuotaViolationError,
    Stat, TimeUnit,
};
use crate::common::{KafkaError, MetricName};

/// The recording level configured for a sensor, controlling the verbosity at
/// which its measurements are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordingLevel {
    /// Always recorded.
    Info,
    /// Recorded at `DEBUG` verbosity and above.
    Debug,
    /// Recorded only at `TRACE` verbosity.
    Trace,
}

/// The lowest valid recording-level id.
pub const MIN_RECORDING_LEVEL_KEY: i32 = 0;

/// The highest valid recording-level id.
pub const MAX_RECORDING_LEVEL_KEY: i32 = 2;

impl RecordingLevel {
    /// The permanent, immutable id of the recording level.
    pub fn id(&self) -> i16 {
        match self {
            Self::Info => 0,
            Self::Debug => 1,
            Self::Trace => 2,
        }
    }

    /// The upper-case name of the recording level.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }

    /// Returns the recording level for the given id.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the id is outside the valid
    /// range.
    pub fn for_id(id: i32) -> Result<Self, KafkaError> {
        match id {
            0 => Ok(Self::Info),
            1 => Ok(Self::Debug),
            2 => Ok(Self::Trace),
            _ => Err(KafkaError::illegal_argument(format!(
                "Unexpected RecordLevel id `{id}`, it should be between `{MIN_RECORDING_LEVEL_KEY}` and \
                 `{MAX_RECORDING_LEVEL_KEY}` (inclusive)"
            ))),
        }
    }

    /// Case-insensitive lookup by name.
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the name does not match a
    /// known level.
    pub fn for_name(name: &str) -> Result<Self, KafkaError> {
        match name.to_uppercase().as_str() {
            "INFO" => Ok(Self::Info),
            "DEBUG" => Ok(Self::Debug),
            "TRACE" => Ok(Self::Trace),
            other => Err(KafkaError::illegal_argument(format!("No enum constant RecordingLevel.{other}"))),
        }
    }

    /// Whether a sensor at this recording level should record when the metrics
    /// repository is configured at `config_id`.
    ///
    /// Returns [`KafkaError::IllegalState`] if `config_id` is not a recognized
    /// recording level.
    pub fn should_record(&self, config_id: i32) -> Result<bool, KafkaError> {
        let this = self.id() as i32;
        if config_id == Self::Info.id() as i32 {
            Ok(this == Self::Info.id() as i32)
        } else if config_id == Self::Debug.id() as i32 {
            Ok(this == Self::Info.id() as i32 || this == Self::Debug.id() as i32)
        } else if config_id == Self::Trace.id() as i32 {
            Ok(true)
        } else {
            Err(KafkaError::illegal_state(format!(
                "Did not recognize recording level {config_id}"
            )))
        }
    }
}

/// A statistic together with a supplier of the config it should be recorded and
/// measured with.
///
/// The config is a supplier (not a fixed value) because a metric-backed stat
/// reads its owning [`KafkaMetric`]'s current config, so a later `set_config`
/// is reflected on the next recording.
struct StatAndConfig {
    stat: Arc<Mutex<dyn Stat>>,
    config_supplier: Box<dyn Fn() -> MetricConfig + Send + Sync>,
}

impl StatAndConfig {
    fn config(&self) -> MetricConfig {
        (self.config_supplier)()
    }
}

/// The mutable structure of a sensor: its stat list and its own metric view.
///
/// A single lock guards this structure (mirroring Java's `synchronized(sensor)`),
/// which — together with the per-stat `Mutex` inside each shared stat handle —
/// serializes recording, adding, and removing. Reading a metric value never
/// touches this lock (only the stat's own `Mutex`), so a synchronized reporter
/// cannot deadlock with registration.
struct SensorState {
    stats: Vec<StatAndConfig>,
    metrics: IndexMap<MetricName, Arc<KafkaMetric>>,
}

/// A sensor applies a continuous sequence of numerical values to a set of
/// associated metrics (for example, message sizes recorded through
/// [`record`](Sensor::record), with metrics for their average and maximum).
pub struct Sensor {
    registry: Weak<MetricsCore>,
    name: String,
    parents: Vec<Arc<Sensor>>,
    state: Mutex<SensorState>,
    config: MetricConfig,
    time: TimeSource,
    last_record_time: AtomicI64,
    inactive_sensor_expiration_time_ms: i64,
    recording_level: RecordingLevel,
}

impl Sensor {
    /// Creates a sensor. Fails with [`KafkaError::IllegalArgument`] if the
    /// parent hierarchy would make the sensor its own ancestor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        registry: Weak<MetricsCore>,
        name: impl Into<String>,
        parents: Vec<Arc<Sensor>>,
        config: MetricConfig,
        time: TimeSource,
        inactive_sensor_expiration_time_seconds: i64,
        recording_level: RecordingLevel,
    ) -> Result<Arc<Sensor>, KafkaError> {
        let last_record_time = time();
        let sensor = Arc::new(Sensor {
            registry,
            name: name.into(),
            parents,
            state: Mutex::new(SensorState { stats: Vec::new(), metrics: IndexMap::new() }),
            config,
            time,
            last_record_time: AtomicI64::new(last_record_time),
            inactive_sensor_expiration_time_ms: TimeUnit::Seconds.to_millis(inactive_sensor_expiration_time_seconds),
            recording_level,
        });
        sensor.check_forest(&mut HashSet::new())?;
        Ok(sensor)
    }

    /// Validates that this sensor doesn't end up referencing itself. A sensor
    /// reachable twice in the traversal (including via a diamond) is rejected.
    fn check_forest(&self, sensors: &mut HashSet<String>) -> Result<(), KafkaError> {
        if !sensors.insert(self.name.clone()) {
            return Err(KafkaError::illegal_argument(format!(
                "Circular dependency in sensors: {} is its own parent.",
                self.name
            )));
        }
        for parent in &self.parents {
            parent.check_forest(sensors)?;
        }
        Ok(())
    }

    /// The unique name this sensor is registered with.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The parent sensors that also receive values recorded here.
    pub(crate) fn parents(&self) -> &[Arc<Sensor>] {
        &self.parents
    }

    /// Whether this sensor's recording level indicates that its metrics will be
    /// recorded.
    pub fn should_record(&self) -> bool {
        self.recording_level
            .should_record(self.config.record_level().id() as i32)
            .unwrap_or(false)
    }

    /// Records an occurrence — shorthand for `record(1.0)`.
    pub fn record_occurrence(&self) -> Result<(), QuotaViolationError> {
        if self.should_record() {
            self.record_internal(1.0, self.now(), true)?;
        }
        Ok(())
    }

    /// Records a value with this sensor.
    ///
    /// Returns [`QuotaViolationError`] if recording moves a metric beyond its
    /// configured bound. This mirrors Java's `QuotaViolationException`; the
    /// error is intentionally a distinct standalone type rather than a
    /// [`KafkaError`] variant, since no protocol error code corresponds to a
    /// quota violation and no caller here needs the widened enum.
    pub fn record(&self, value: f64) -> Result<(), QuotaViolationError> {
        if self.should_record() {
            self.record_internal(value, self.now(), true)?;
        }
        Ok(())
    }

    /// Records a value at a known time, reusing the supplied timestamp.
    pub fn record_at(&self, value: f64, time_ms: i64) -> Result<(), QuotaViolationError> {
        if self.should_record() {
            self.record_internal(value, time_ms, true)?;
        }
        Ok(())
    }

    /// Records a value at a known time, optionally enforcing quotas.
    pub fn record_with_quotas(&self, value: f64, time_ms: i64, check_quotas: bool) -> Result<(), QuotaViolationError> {
        if self.should_record() {
            self.record_internal(value, time_ms, check_quotas)?;
        }
        Ok(())
    }

    fn record_internal(&self, value: f64, time_ms: i64, check_quotas: bool) -> Result<(), QuotaViolationError> {
        self.last_record_time.store(time_ms, Ordering::SeqCst);
        {
            let state = self.state.lock().expect("sensor state lock poisoned");
            for stat_and_config in &state.stats {
                stat_and_config.stat.lock().expect("stat lock poisoned").record(
                    &stat_and_config.config(),
                    value,
                    time_ms,
                );
            }
            if check_quotas {
                // Quota checks read each metric's value through the metric's own
                // stat lock, never re-locking the sensor state, so this stays
                // inside the (non-reentrant) state guard.
                for metric in state.metrics.values() {
                    if let Some(violation) = check_metric_quota(metric, time_ms) {
                        return Err(violation);
                    }
                }
            }
        }
        for parent in &self.parents {
            parent.record_with_quotas(value, time_ms, check_quotas)?;
        }
        Ok(())
    }

    /// Checks whether any metric with a configured quota has been violated, as
    /// of the current time.
    pub fn check_quotas(&self) -> Result<(), QuotaViolationError> {
        self.check_quotas_at(self.now())
    }

    /// Checks whether any metric with a configured quota has been violated, as
    /// of `time_ms`.
    pub fn check_quotas_at(&self, time_ms: i64) -> Result<(), QuotaViolationError> {
        let state = self.state.lock().expect("sensor state lock poisoned");
        for metric in state.metrics.values() {
            if let Some(violation) = check_metric_quota(metric, time_ms) {
                return Err(violation);
            }
        }
        Ok(())
    }

    /// Registers a compound statistic (which yields multiple measurables) with
    /// no config override.
    ///
    /// Returns `Ok(true)` if added, `Ok(false)` if the sensor has expired, or
    /// [`KafkaError::IllegalArgument`] if one of its metric names is already
    /// registered elsewhere.
    pub fn add_compound<C: CompoundStat + 'static>(&self, stat: C) -> Result<bool, KafkaError> {
        self.add_compound_with_config(stat, None)
    }

    /// Registers a compound statistic with an optional config override.
    pub fn add_compound_with_config<C: CompoundStat + 'static>(
        &self,
        stat: C,
        config: Option<MetricConfig>,
    ) -> Result<bool, KafkaError> {
        if self.has_expired() {
            return Ok(false);
        }
        let registry = self.registry()?;
        let stat_config = config.unwrap_or_else(|| self.config.clone());
        let concrete = Arc::new(Mutex::new(stat));
        let named = concrete.lock().expect("stat lock poisoned").stats();
        let stat_view: Arc<Mutex<dyn Stat>> = concrete;

        let mut state = self.state.lock().expect("sensor state lock poisoned");
        let supplier_config = stat_config.clone();
        state
            .stats
            .push(StatAndConfig { stat: stat_view, config_supplier: Box::new(move || supplier_config.clone()) });
        for measurable in named {
            let metric_name = measurable.name().clone();
            if !state.metrics.contains_key(&metric_name) {
                let provider = MetricValueProvider::Measurable(measurable.stat().clone());
                let metric = Arc::new(KafkaMetric::new(
                    metric_name.clone(),
                    provider,
                    stat_config.clone(),
                    self.time.clone(),
                ));
                if registry.register_metric(metric.clone()).is_some() {
                    return Err(KafkaError::illegal_argument(format!(
                        "A metric named '{metric_name}' already exists, can't register another one."
                    )));
                }
                state.metrics.insert(metric_name, metric);
            }
        }
        Ok(true)
    }

    /// Registers a metric backed by a measurable statistic, with no config
    /// override.
    pub fn add_metric<S: MeasurableStat + 'static>(
        &self,
        metric_name: MetricName,
        stat: S,
    ) -> Result<bool, KafkaError> {
        self.add_metric_with_config(metric_name, stat, None)
    }

    /// Registers a metric backed by a measurable statistic, with an optional
    /// config override.
    ///
    /// Returns `Ok(true)` if added (or already present on this sensor),
    /// `Ok(false)` if the sensor has expired, or [`KafkaError::IllegalArgument`]
    /// if the metric name is already registered elsewhere.
    pub fn add_metric_with_config<S: MeasurableStat + 'static>(
        &self,
        metric_name: MetricName,
        stat: S,
        config: Option<MetricConfig>,
    ) -> Result<bool, KafkaError> {
        if self.has_expired() {
            return Ok(false);
        }
        let mut state = self.state.lock().expect("sensor state lock poisoned");
        if state.metrics.contains_key(&metric_name) {
            return Ok(true);
        }
        let registry = self.registry()?;
        let stat_config = config.unwrap_or_else(|| self.config.clone());
        // The same statistic is recorded into (as a `Stat`) and read from (as a
        // `Measurable`). Coerce the one concrete allocation to both views so
        // they share state.
        let concrete = Arc::new(Mutex::new(stat));
        let measurable_view: Arc<Mutex<dyn Measurable>> = concrete.clone();
        let stat_view: Arc<Mutex<dyn Stat>> = concrete;
        let metric = Arc::new(KafkaMetric::new(
            metric_name.clone(),
            MetricValueProvider::Measurable(measurable_view),
            stat_config,
            self.time.clone(),
        ));
        if registry.register_metric(metric.clone()).is_some() {
            return Err(KafkaError::illegal_argument(format!(
                "A metric named '{metric_name}' already exists, can't register another one."
            )));
        }
        let supplier_metric = metric.clone();
        state.metrics.insert(metric_name, metric);
        state
            .stats
            .push(StatAndConfig { stat: stat_view, config_supplier: Box::new(move || supplier_metric.config()) });
        Ok(true)
    }

    /// Whether any metrics were registered with this sensor.
    pub fn has_metrics(&self) -> bool {
        !self.state.lock().expect("sensor state lock poisoned").metrics.is_empty()
    }

    /// Whether the sensor is eligible for removal due to inactivity.
    pub fn has_expired(&self) -> bool {
        (self.now() - self.last_record_time.load(Ordering::SeqCst)) > self.inactive_sensor_expiration_time_ms
    }

    /// The metrics registered with this sensor.
    pub fn metrics(&self) -> Vec<Arc<KafkaMetric>> {
        self.state
            .lock()
            .expect("sensor state lock poisoned")
            .metrics
            .values()
            .cloned()
            .collect()
    }

    fn now(&self) -> i64 {
        (self.time)()
    }

    fn registry(&self) -> Result<Arc<MetricsCore>, KafkaError> {
        self.registry
            .upgrade()
            .ok_or_else(|| KafkaError::illegal_state("the metrics registry has been dropped"))
    }
}

/// Returns a [`QuotaViolationError`] if the metric has a configured quota that
/// the current value violates. Token-bucket metrics are violated when their
/// remaining credits drop below zero; all others when the value falls outside
/// the quota bound.
fn check_metric_quota(metric: &Arc<KafkaMetric>, time_ms: i64) -> Option<QuotaViolationError> {
    let config = metric.config();
    let quota = config.quota()?;
    let value = metric.measurable_value(time_ms);
    let is_token_bucket = metric
        .measurable()
        .is_ok_and(|m| m.lock().expect("measurable lock poisoned").as_any().is::<TokenBucket>());
    let violated = if is_token_bucket {
        value < 0.0
    } else {
        !quota.acceptable(value)
    };
    if violated {
        Some(QuotaViolationError::new(Arc::clone(metric), value, quota.bound()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Barrier;

    use super::*;
    use crate::common::metrics::Metrics;
    use crate::common::metrics::stats::{Avg, CumulativeCount, Meter, Rate, WindowedSum};
    use crate::common::metrics::test_support::{FakeMetricsReporter, MockClock};
    use crate::common::metrics::{MetricsReporter, Quota};

    fn assert_close(expected: f64, actual: f64, eps: f64, msg: &str) {
        assert!((expected - actual).abs() <= eps, "{msg}: expected {expected}, got {actual}");
    }

    /// The current wall-clock time in POSIX milliseconds, used as a realistic
    /// starting instant for quota tests whose stats fill relative to epoch zero.
    fn wall_clock_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    #[test]
    fn test_id_and_name() {
        assert_eq!(RecordingLevel::Info.id(), 0);
        assert_eq!(RecordingLevel::Debug.id(), 1);
        assert_eq!(RecordingLevel::Trace.id(), 2);
        assert_eq!(RecordingLevel::Info.name(), "INFO");
        assert_eq!(RecordingLevel::Debug.name(), "DEBUG");
        assert_eq!(RecordingLevel::Trace.name(), "TRACE");
    }

    #[test]
    fn test_for_id() {
        assert_eq!(RecordingLevel::for_id(0).unwrap(), RecordingLevel::Info);
        assert_eq!(RecordingLevel::for_id(2).unwrap(), RecordingLevel::Trace);
        let err = RecordingLevel::for_id(3).unwrap_err();
        assert!(
            err.message().contains("Unexpected RecordLevel id `3`"),
            "unexpected message: {}",
            err.message()
        );
        assert!(RecordingLevel::for_id(-1).is_err());
    }

    #[test]
    fn test_for_name_case_insensitive() {
        assert_eq!(RecordingLevel::for_name("info").unwrap(), RecordingLevel::Info);
        assert_eq!(RecordingLevel::for_name("Debug").unwrap(), RecordingLevel::Debug);
        assert_eq!(RecordingLevel::for_name("TRACE").unwrap(), RecordingLevel::Trace);
        assert!(RecordingLevel::for_name("bogus").is_err());
    }

    #[test]
    fn test_should_record() {
        // Configured at INFO: only INFO sensors record.
        assert!(RecordingLevel::Info.should_record(0).unwrap());
        assert!(!RecordingLevel::Debug.should_record(0).unwrap());
        assert!(!RecordingLevel::Trace.should_record(0).unwrap());
        // Configured at DEBUG: INFO and DEBUG sensors record.
        assert!(RecordingLevel::Info.should_record(1).unwrap());
        assert!(RecordingLevel::Debug.should_record(1).unwrap());
        assert!(!RecordingLevel::Trace.should_record(1).unwrap());
        // Configured at TRACE: everything records.
        assert!(RecordingLevel::Info.should_record(2).unwrap());
        assert!(RecordingLevel::Trace.should_record(2).unwrap());

        let err = RecordingLevel::Info.should_record(9).unwrap_err();
        assert!(
            err.message().contains("Did not recognize recording level 9"),
            "unexpected message: {}",
            err.message()
        );
    }

    fn info_config() -> MetricConfig {
        MetricConfig::new().with_record_level(RecordingLevel::Info)
    }

    fn debug_config() -> MetricConfig {
        MetricConfig::new().with_record_level(RecordingLevel::Debug)
    }

    fn trace_config() -> MetricConfig {
        MetricConfig::new().with_record_level(RecordingLevel::Trace)
    }

    /// A sensor with no registry, used to test recording-level behavior in
    /// isolation. The clock and expiration are irrelevant to these tests.
    fn detached_sensor(name: &str, config: MetricConfig, level: RecordingLevel) -> Arc<Sensor> {
        Sensor::new(Weak::new(), name, Vec::new(), config, Arc::new(|| 0_i64), 0, level).unwrap()
    }

    #[test]
    fn test_record_level_enum() {
        let config_level = RecordingLevel::Info;
        assert!(RecordingLevel::Info.should_record(config_level.id() as i32).unwrap());
        assert!(!RecordingLevel::Debug.should_record(config_level.id() as i32).unwrap());
        assert!(!RecordingLevel::Trace.should_record(config_level.id() as i32).unwrap());

        let config_level = RecordingLevel::Debug;
        assert!(RecordingLevel::Info.should_record(config_level.id() as i32).unwrap());
        assert!(RecordingLevel::Debug.should_record(config_level.id() as i32).unwrap());
        assert!(!RecordingLevel::Trace.should_record(config_level.id() as i32).unwrap());

        let config_level = RecordingLevel::Trace;
        assert!(RecordingLevel::Info.should_record(config_level.id() as i32).unwrap());
        assert!(RecordingLevel::Debug.should_record(config_level.id() as i32).unwrap());
        assert!(RecordingLevel::Trace.should_record(config_level.id() as i32).unwrap());

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

    #[test]
    fn test_should_record_for_info_level_sensor() {
        assert!(detached_sensor("infoSensor", info_config(), RecordingLevel::Info).should_record());
        assert!(detached_sensor("infoSensor", debug_config(), RecordingLevel::Info).should_record());
        assert!(detached_sensor("infoSensor", trace_config(), RecordingLevel::Info).should_record());
    }

    #[test]
    fn test_should_record_for_debug_level_sensor() {
        assert!(!detached_sensor("debugSensor", info_config(), RecordingLevel::Debug).should_record());
        assert!(detached_sensor("debugSensor", debug_config(), RecordingLevel::Debug).should_record());
        assert!(detached_sensor("debugSensor", trace_config(), RecordingLevel::Debug).should_record());
    }

    #[test]
    fn test_should_record_for_trace_level_sensor() {
        assert!(!detached_sensor("traceSensor", info_config(), RecordingLevel::Trace).should_record());
        assert!(!detached_sensor("traceSensor", debug_config(), RecordingLevel::Trace).should_record());
        assert!(detached_sensor("traceSensor", trace_config(), RecordingLevel::Trace).should_record());
    }

    #[test]
    fn test_expired_sensor() {
        let clock = MockClock::new();
        let config = MetricConfig::new();
        let metrics = Metrics::new_with_expiration(
            config.clone(),
            vec![Arc::new(FakeMetricsReporter) as Arc<dyn MetricsReporter>],
            clock.time_source(),
            true,
        );
        let inactive_sensor_expiration_time_seconds = 60;
        let sensor = metrics
            .sensor_with_expiration("sensor", Some(config), inactive_sensor_expiration_time_seconds)
            .unwrap();

        assert!(sensor.add_metric(metrics.metric_name("test1", "grp1"), Avg::new()).unwrap());

        let rate_metric_name = MetricName::new("rate", "test", "", IndexMap::new());
        let total_metric_name = MetricName::new("total", "test", "", IndexMap::new());
        assert!(sensor.add_compound(Meter::new(rate_metric_name, total_metric_name)).unwrap());

        clock.sleep(TimeUnit::Seconds.to_millis(inactive_sensor_expiration_time_seconds + 1));
        assert!(!sensor.add_metric(metrics.metric_name("test3", "grp1"), Avg::new()).unwrap());
        // A fresh meter with the same names; the expired sensor rejects it
        // before inspecting the stat, so the names never collide.
        let meter = Meter::new(
            MetricName::new("rate", "test", "", IndexMap::new()),
            MetricName::new("total", "test", "", IndexMap::new()),
        );
        assert!(!sensor.add_compound(meter).unwrap());
    }

    #[test]
    fn test_idempotent_add() {
        let metrics = Metrics::new();
        let sensor = metrics.sensor("sensor").unwrap();

        assert!(
            sensor
                .add_metric(metrics.metric_name("test-metric", "test-group"), Avg::new())
                .unwrap()
        );

        // Adding the same metric to the same sensor is a no-op.
        assert!(
            sensor
                .add_metric(metrics.metric_name("test-metric", "test-group"), Avg::new())
                .unwrap()
        );

        // Adding the same metric to a different sensor is an error.
        let another_sensor = metrics.sensor("another-sensor").unwrap();
        assert!(
            another_sensor
                .add_metric(metrics.metric_name("test-metric", "test-group"), Avg::new())
                .is_err()
        );

        // Adding a different metric with the same name is also a no-op.
        assert!(
            sensor
                .add_metric(metrics.metric_name("test-metric", "test-group"), WindowedSum::new())
                .unwrap()
        );

        // After all this, only the original metric remains registered.
        assert_eq!(sensor.metrics().len(), 1);
        assert!(sensor.metrics()[0].measurable().unwrap().lock().unwrap().as_any().is::<Avg>());
    }

    #[test]
    fn test_check_quotas_in_multi_threads() {
        let base = wall_clock_ms();
        let metrics = Metrics::with_config(
            MetricConfig::new()
                .with_quota(Quota::upper_bound(f64::MAX))
                // A tiny time window makes the sampled stat always record the value,
                // and many samples make it retain more of them.
                .with_time_window(1, TimeUnit::Milliseconds)
                .with_samples(100)
                .unwrap(),
        );
        let sensor = metrics.sensor("sensor").unwrap();
        assert!(
            sensor
                .add_metric(metrics.metric_name("test-metric", "test-group"), Rate::new())
                .unwrap()
        );

        let thread_count = 10;
        let barrier = Barrier::new(thread_count);
        std::thread::scope(|scope| {
            for index in 0..thread_count {
                let sensor = &sensor;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for j in 0..20 {
                        sensor.record_with_quotas((j * index) as f64, base + j as i64, false).unwrap();
                        sensor.check_quotas().unwrap();
                    }
                });
            }
        });
    }

    #[test]
    fn should_return_presence_of_metrics() {
        let metrics = Metrics::new();
        let sensor = metrics.sensor("sensor").unwrap();

        assert!(!sensor.has_metrics());

        sensor
            .add_metric(
                MetricName::new("name1", "group1", "description1", IndexMap::new()),
                WindowedSum::new(),
            )
            .unwrap();
        assert!(sensor.has_metrics());

        sensor
            .add_metric(
                MetricName::new("name2", "group2", "description2", IndexMap::new()),
                CumulativeCount::new(),
            )
            .unwrap();
        assert!(sensor.has_metrics());
    }

    fn strict_record(sensor: &Sensor, value: f64, time_ms: i64) -> Result<(), QuotaViolationError> {
        sensor.check_quotas_at(time_ms)?;
        sensor.record_with_quotas(value, time_ms, false)?;
        Ok(())
    }

    #[test]
    fn test_strict_quota_enforcement_with_rate() {
        let clock = MockClock::with_start(wall_clock_ms());
        let metrics = Metrics::with_time(clock.time_source());
        let sensor = metrics
            .sensor_with_config(
                "sensor",
                MetricConfig::new()
                    .with_quota(Quota::upper_bound(2.0))
                    .with_time_window(1, TimeUnit::Seconds)
                    .with_samples(11)
                    .unwrap(),
            )
            .unwrap();
        let metric_name = metrics.metric_name("rate", "test-group");
        assert!(sensor.add_metric(metric_name.clone(), Rate::new()).unwrap());
        let rate_metric = metrics.metric(&metric_name).unwrap();

        // Record a first value at T+0, bringing the average rate to 3, already
        // above the quota.
        strict_record(&sensor, 30.0, clock.milliseconds()).unwrap();
        assert_close(3.0, rate_metric.measurable_value(clock.milliseconds()), 0.1, "rate at T+0");

        // Waiting 5s is not enough to bring the average rate back under quota.
        clock.sleep(5000);
        assert_close(3.0, rate_metric.measurable_value(clock.milliseconds()), 0.1, "rate after 5s");
        assert!(strict_record(&sensor, 30.0, clock.milliseconds()).is_err());

        metrics.close();
    }

    #[test]
    fn test_strict_quota_enforcement_with_token_bucket() {
        let clock = MockClock::with_start(wall_clock_ms());
        let metrics = Metrics::with_time(clock.time_source());
        let sensor = metrics
            .sensor_with_config(
                "sensor",
                MetricConfig::new()
                    .with_quota(Quota::upper_bound(2.0))
                    .with_time_window(1, TimeUnit::Seconds)
                    .with_samples(10)
                    .unwrap(),
            )
            .unwrap();
        let metric_name = metrics.metric_name("credits", "test-group");
        assert!(sensor.add_metric(metric_name.clone(), TokenBucket::new()).unwrap());
        let tk_metric = metrics.metric(&metric_name).unwrap();

        // Recording a first value at T+0 brings the remaining credits below zero.
        strict_record(&sensor, 30.0, clock.milliseconds()).unwrap();
        assert_close(
            -10.0,
            tk_metric.measurable_value(clock.milliseconds()),
            0.1,
            "credits after first record",
        );

        // After 5s the credits refill back to zero.
        clock.sleep(5000);
        assert_close(0.0, tk_metric.measurable_value(clock.milliseconds()), 0.1, "credits refilled");
        strict_record(&sensor, 30.0, clock.milliseconds()).unwrap();
        assert_close(
            -30.0,
            tk_metric.measurable_value(clock.milliseconds()),
            0.1,
            "credits after second record",
        );

        metrics.close();
    }

    /// Captures the config, value, and timestamp that a stat is recorded and
    /// measured with, so a test can confirm each stat receives its own config.
    #[derive(Clone)]
    struct RecordingMeasurableStat {
        recorded: Arc<Mutex<Vec<(MetricConfig, f64, i64)>>>,
        measured: Arc<Mutex<Vec<(MetricConfig, i64)>>>,
    }

    impl RecordingMeasurableStat {
        fn new() -> Self {
            Self {
                recorded: Arc::new(Mutex::new(Vec::new())),
                measured: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Stat for RecordingMeasurableStat {
        fn record(&mut self, config: &MetricConfig, value: f64, time_ms: i64) {
            self.recorded.lock().unwrap().push((config.clone(), value, time_ms));
        }
    }

    impl Measurable for RecordingMeasurableStat {
        fn measure(&mut self, config: &MetricConfig, now: i64) -> f64 {
            self.measured.lock().unwrap().push((config.clone(), now));
            0.0
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    impl MeasurableStat for RecordingMeasurableStat {}

    fn quota_bound(config: &MetricConfig) -> Option<f64> {
        config.quota().map(|q| q.bound())
    }

    #[test]
    fn test_record_and_check_quota_use_metric_config_of_each_stat() {
        let clock = MockClock::new();
        let metrics = Metrics::with_time(clock.time_source());
        let sensor = metrics.sensor("sensor").unwrap();

        let stat1 = RecordingMeasurableStat::new();
        let stat1_recorded = stat1.recorded.clone();
        let stat1_measured = stat1.measured.clone();
        let stat1_config = MetricConfig::new().with_quota(Quota::upper_bound(5.0));
        sensor
            .add_metric_with_config(metrics.metric_name("stat1", "test-group"), stat1, Some(stat1_config))
            .unwrap();

        let stat2 = RecordingMeasurableStat::new();
        let stat2_recorded = stat2.recorded.clone();
        let stat2_measured = stat2.measured.clone();
        let stat2_config = MetricConfig::new().with_quota(Quota::upper_bound(10.0));
        sensor
            .add_metric_with_config(metrics.metric_name("stat2", "test-group"), stat2, Some(stat2_config))
            .unwrap();

        sensor.record_at(10.0, 1).unwrap();
        assert!(
            stat1_recorded
                .lock()
                .unwrap()
                .iter()
                .any(|(c, v, t)| quota_bound(c) == Some(5.0) && *v == 10.0 && *t == 1)
        );
        assert!(
            stat2_recorded
                .lock()
                .unwrap()
                .iter()
                .any(|(c, v, t)| quota_bound(c) == Some(10.0) && *v == 10.0 && *t == 1)
        );

        sensor.check_quotas_at(2).unwrap();
        assert!(
            stat1_measured
                .lock()
                .unwrap()
                .iter()
                .any(|(c, now)| quota_bound(c) == Some(5.0) && *now == 2)
        );
        assert!(
            stat2_measured
                .lock()
                .unwrap()
                .iter()
                .any(|(c, now)| quota_bound(c) == Some(10.0) && *now == 2)
        );

        metrics.close();
    }

    #[test]
    fn test_updating_metric_config_is_reflected_in_the_sensor() {
        let clock = MockClock::new();
        let metrics = Metrics::with_time(clock.time_source());
        let sensor = metrics.sensor("sensor").unwrap();

        let stat = RecordingMeasurableStat::new();
        let recorded = stat.recorded.clone();
        let measured = stat.measured.clone();
        let stat_name = metrics.metric_name("stat", "test-group");
        let stat_config = MetricConfig::new().with_quota(Quota::upper_bound(5.0));
        sensor
            .add_metric_with_config(stat_name.clone(), stat, Some(stat_config))
            .unwrap();

        sensor.record_at(10.0, 1).unwrap();
        assert!(
            recorded
                .lock()
                .unwrap()
                .iter()
                .any(|(c, v, t)| quota_bound(c) == Some(5.0) && *v == 10.0 && *t == 1)
        );

        sensor.check_quotas_at(2).unwrap();
        assert!(
            measured
                .lock()
                .unwrap()
                .iter()
                .any(|(c, now)| quota_bound(c) == Some(5.0) && *now == 2)
        );

        // Update the config of the KafkaMetric; the sensor should pick it up.
        metrics
            .metric(&stat_name)
            .unwrap()
            .set_config(MetricConfig::new().with_quota(Quota::upper_bound(10.0)));

        sensor.record_at(10.0, 3).unwrap();
        assert!(
            recorded
                .lock()
                .unwrap()
                .iter()
                .any(|(c, v, t)| quota_bound(c) == Some(10.0) && *v == 10.0 && *t == 3)
        );

        sensor.check_quotas_at(4).unwrap();
        assert!(
            measured
                .lock()
                .unwrap()
                .iter()
                .any(|(c, now)| quota_bound(c) == Some(10.0) && *now == 4)
        );

        metrics.close();
    }
}
