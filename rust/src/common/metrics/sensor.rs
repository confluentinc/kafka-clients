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
use std::sync::{Arc, Mutex, Weak};

use crate::common::Metric;
use crate::common::metrics::MetricsShared;
use crate::common::metrics::{
    CompoundStat, KafkaMetric, Measurable, MeasurableStat, MetricConfig, Quota, QuotaViolationError, Stat,
};
use crate::common::utils::Time;
use crate::common::{Error, MetricName};

/// The recording level of a sensor or metric config.
///
/// Mirrors `Sensor.RecordingLevel`. The numeric ids are part of the protocol and
/// must not change: INFO=0, DEBUG=1, TRACE=2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.metrics.Sensor$RecordingLevel")]
pub enum RecordingLevel {
    /// INFO level (id 0).
    Info,
    /// DEBUG level (id 1).
    Debug,
    /// TRACE level (id 2).
    Trace,
}

/// Java's inherited `Enum.toString()`: the constant's name.
impl std::fmt::Display for RecordingLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl RecordingLevel {
    /// Returns the permanent and immutable id of this level.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor$RecordingLevel#id")]
    pub fn id(&self) -> i16 {
        match self {
            RecordingLevel::Info => 0,
            RecordingLevel::Debug => 1,
            RecordingLevel::Trace => 2,
        }
    }

    /// Returns an English description of the level.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor$RecordingLevel#name")]
    pub fn name(&self) -> &'static str {
        match self {
            RecordingLevel::Info => "INFO",
            RecordingLevel::Debug => "DEBUG",
            RecordingLevel::Trace => "TRACE",
        }
    }

    /// Lookup by id.
    ///
    /// # Arguments
    ///
    /// - `id` - the level's id
    ///
    /// # Returns
    ///
    /// The level with that id.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `id` is not between `0` and `2`
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor$RecordingLevel#forId")]
    pub fn for_id(id: i16) -> Result<RecordingLevel, Error> {
        match id {
            0 => Ok(RecordingLevel::Info),
            1 => Ok(RecordingLevel::Debug),
            2 => Ok(RecordingLevel::Trace),
            _ => Err(Error::local_illegal_argument(format!(
                "Unexpected RecordLevel id `{id}`, it should be between `0` and `2` (inclusive)"
            ))),
        }
    }

    /// Case insensitive lookup by protocol name
    ///
    /// # Arguments
    ///
    /// - `name` - the level's name, in any case
    ///
    /// # Returns
    ///
    /// The level with that name.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if no level has that name
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor$RecordingLevel#forName")]
    pub fn for_name(name: &str) -> Result<RecordingLevel, Error> {
        match name.to_uppercase().as_str() {
            "INFO" => Ok(RecordingLevel::Info),
            "DEBUG" => Ok(RecordingLevel::Debug),
            "TRACE" => Ok(RecordingLevel::Trace),
            other => Err(Error::local_illegal_argument(format!(
                "No enum constant RecordingLevel.{other}"
            ))),
        }
    }

    /// Returns whether a sensor at this level should record given the config's
    /// level id.
    ///
    /// # Arguments
    ///
    /// - `config_id` - the id of the config's recording level
    ///
    /// # Panics
    ///
    /// If `config_id` is not the id of a level, with the message of Java's
    /// `IllegalStateException`.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor$RecordingLevel#shouldRecord")]
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
/// so the stat always reads the backing metric's (possibly updated) config; for
/// the `add(CompoundStat, config)` path it is a constant `statConfig` supplier.
/// Both are modelled by [`StatConfigSource`].
#[doc(alias = "org.apache.kafka.common.metrics.Sensor$StatAndConfig")]
struct StatAndConfig {
    stat: Box<dyn Stat>,
    config: StatConfigSource,
}

/// Where a `StatAndConfig` reads its config from, mirroring Java's
/// `Supplier<MetricConfig>`:
///
/// - `FromMetric` — the supplier is `metric::config`, so the stat always reads
///   the backing metric's (possibly updated) config. Used by
///   `add(MetricName, MeasurableStat, config)`.
/// - `Constant` — a fixed `statConfig` supplier (`() -> statConfig`). Used by
///   `add(CompoundStat, config)`, whose single recordable stat is not coupled to
///   any one child metric.
enum StatConfigSource {
    FromMetric(Arc<KafkaMetric>),
    Constant(Arc<MetricConfig>),
}

impl StatAndConfig {
    fn record(&self, value: f64, time_ms: i64) {
        let config = match &self.config {
            StatConfigSource::FromMetric(metric) => metric.config(),
            StatConfigSource::Constant(config) => Arc::clone(config),
        };
        self.stat.record(&config, value, time_ms);
    }
}

/// A sensor applies a continuous sequence of numerical values to a set of
/// associated metrics. For example a sensor on message size would record a
/// sequence of message sizes using the `record` api and would maintain a set of
/// metrics about request sizes such as the average or max.
#[doc(alias = "org.apache.kafka.common.metrics.Sensor")]
pub struct Sensor {
    /// Java's `registry`, held weakly: the registry's metrics map holds the
    /// providers of every metric, and a provider may capture an object that
    /// holds this sensor (`BufferPool`'s gauges capture the pool, which holds
    /// its `bufferpool-wait-time` sensor), so a strong reference would be an
    /// `Arc` cycle keeping the registry and everything its providers capture
    /// allocated forever. Java's garbage collector frees the same graph. The
    /// registry is owned by its [`Metrics`](crate::common::metrics::Metrics);
    /// once that is dropped no one can read the map, and `add` keeps the
    /// metric on the sensor only, as for a standalone sensor.
    registry: Option<Weak<MetricsShared>>,
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

impl std::fmt::Debug for Sensor {
    // The sensor's name, its identifier in the registry.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sensor").field("name", &self.name).finish_non_exhaustive()
    }
}

impl Sensor {
    /// Create a sensor.
    ///
    /// `registry` is `None` for standalone sensors (mirroring Java's `null`
    /// registry in `SensorTest`), in which case `add` does not register metrics
    /// in a global repository.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#Sensor")]
    pub(crate) fn new(
        registry: Option<Weak<MetricsShared>>,
        name: impl Into<String>,
        parents: Vec<Arc<Sensor>>,
        config: Arc<MetricConfig>,
        time: Arc<dyn Time>,
        inactive_sensor_expiration_time_seconds: i64,
        recording_level: RecordingLevel,
    ) -> Result<Self, Error> {
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
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#checkForest")]
    fn check_forest(&self, sensors: &mut HashSet<*const Sensor>) -> Result<(), Error> {
        if !sensors.insert(self as *const Sensor) {
            return Err(Error::local_illegal_argument(format!(
                "Circular dependency in sensors: {} is its own parent.",
                self.name()
            )));
        }
        for parent in &self.parents {
            parent.check_forest(sensors)?;
        }
        Ok(())
    }

    /// Returns the name this sensor is registered with. This name will be unique
    /// among all registered sensors.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#name")]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The parents of this sensor.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#parents")]
    pub(crate) fn parents(&self) -> &[Arc<Sensor>] {
        &self.parents
    }

    /// Returns true if the sensor's record level indicates that the metric will
    /// be recorded, false otherwise.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#shouldRecord")]
    pub fn should_record(&self) -> bool {
        self.recording_level.should_record(self.config.record_level().id())
    }

    // Java's `record` overloads have an EMPTY parameter-name intersection, and
    // the no-arg `record()` matches it, so that one keeps the plain name and the
    // others carry their Rust parameters.

    /// Record an occurrence, this is just short-hand for `record_with_value(1.0)`.
    ///
    /// # Errors
    ///
    /// - `Error::is_quota_violation_error` if recording moves a metric beyond its
    ///   configured maximum or minimum bound
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#record")]
    pub fn record(&self) -> Result<(), Error> {
        if self.should_record() {
            self.record_internal(1.0, self.time.milliseconds(), true)?;
        }
        Ok(())
    }

    /// Record a value with this sensor
    ///
    /// # Arguments
    ///
    /// - `value` - The value to record
    ///
    /// # Errors
    ///
    /// - `Error::is_quota_violation_error` if recording this value moves a metric
    ///   beyond its configured maximum or minimum bound
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#record")]
    pub fn record_with_value(&self, value: f64) -> Result<(), Error> {
        if self.should_record() {
            self.record_internal(value, self.time.milliseconds(), true)?;
        }
        Ok(())
    }

    /// Record a value at a known time. This method is slightly faster than
    /// `record_with_value` since it will reuse the time stamp.
    ///
    /// # Arguments
    ///
    /// - `value` - The value we are recording
    /// - `time_ms` - The current POSIX time in milliseconds
    ///
    /// # Errors
    ///
    /// - `Error::is_quota_violation_error` if recording this value moves a metric
    ///   beyond its configured maximum or minimum bound
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#record")]
    pub fn record_with_value_time_ms(&self, value: f64, time_ms: i64) -> Result<(), Error> {
        if self.should_record() {
            self.record_internal(value, time_ms, true)?;
        }
        Ok(())
    }

    /// Record a value at a known time. This method is slightly faster than
    /// `record_with_value` since it will reuse the time stamp.
    ///
    /// # Arguments
    ///
    /// - `value` - The value we are recording
    /// - `time_ms` - The current POSIX time in milliseconds
    /// - `check_quotas` - Indicate if quota must be enforced or not
    ///
    /// # Errors
    ///
    /// - `Error::is_quota_violation_error` if recording this value moves a metric
    ///   beyond its configured maximum or minimum bound
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#record")]
    pub fn record_with_value_time_ms_check_quotas(
        &self,
        value: f64,
        time_ms: i64,
        check_quotas: bool,
    ) -> Result<(), Error> {
        if self.should_record() {
            self.record_internal(value, time_ms, check_quotas)?;
        }
        Ok(())
    }

    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#recordInternal")]
    fn record_internal(&self, value: f64, time_ms: i64, check_quotas: bool) -> Result<(), Error> {
        self.last_record_time.store(time_ms, Ordering::SeqCst);
        {
            let inner = self.inner.lock().expect("sensor mutex poisoned");
            // Increment all the stats.
            for stat_and_config in &inner.stats {
                stat_and_config.record(value, time_ms);
            }
        }
        if check_quotas {
            self.check_quotas_with_time_ms(time_ms)?;
        }
        for parent in &self.parents {
            parent.record_with_value_time_ms_check_quotas(value, time_ms, check_quotas)?;
        }
        Ok(())
    }

    /// Check if we have violated our quota for any metric that has a configured
    /// quota
    ///
    /// # Errors
    ///
    /// - `Error::is_quota_violation_error` if a metric's value is outside its quota
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#checkQuotas")]
    pub fn check_quotas(&self) -> Result<(), Error> {
        self.check_quotas_with_time_ms(self.time.milliseconds())
    }

    /// Check if we have violated our quota for any metric that has a configured
    /// quota, as of `time_ms`.
    ///
    /// # Arguments
    ///
    /// - `time_ms` - the POSIX time in milliseconds to measure the metrics at
    ///
    /// # Errors
    ///
    /// - `Error::is_quota_violation_error` if a metric's value is outside its quota
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#checkQuotas")]
    // Java guards on `config != null`; a Rust `MetricConfig` is always present,
    // so only the `quota != null` guard survives as `Option<Quota>`. Java also
    // special-cases `metric.measurable() instanceof TokenBucket`, treating any
    // negative value as a violation; `TokenBucket` is not translated, so no
    // metric can take that branch. It must be restored together with it.
    pub fn check_quotas_with_time_ms(&self, time_ms: i64) -> Result<(), Error> {
        // The metrics are cloned out of the guard rather than measured under it:
        // `measurable_value` calls into user-supplied `Measurable` code, and
        // holding the sensor lock across that would let a measurable that
        // re-enters the sensor deadlock. Java has the same call under
        // `synchronized (this)` but its monitor is reentrant.
        // Only the metrics with a quota are collected, so the common case (no
        // quota anywhere, as for every client sensor) collects nothing and does
        // not allocate on the per-record path `record` calls this from.
        let metrics: Vec<(Arc<KafkaMetric>, Quota)> = {
            let inner = self.inner.lock().expect("sensor mutex poisoned");
            inner
                .metrics
                .values()
                .filter_map(|metric| metric.config().quota().map(|quota| (Arc::clone(metric), quota.clone())))
                .collect()
        };
        for (metric, quota) in metrics {
            let value = metric.measurable_value(time_ms);
            if !quota.acceptable(value) {
                return Err(Error::QuotaViolation(Box::new(QuotaViolationError::new(
                    metric.metric_name().clone(),
                    value,
                    quota.bound(),
                ))));
            }
        }
        Ok(())
    }

    // Java's four `add` overloads all name their statistic parameter `stat`, so
    // the parameter-name intersection is `{stat}`, and the overload whose
    // parameters are exactly that is the COMPOUND form `add(CompoundStat stat)`.
    // So the compound form keeps the plain name `add` and the metric-name forms
    // are suffixed with their Rust parameters beyond the intersection. The two families differ
    // in type as well as arity, but the derived names are already distinct, so
    // §2's type tie-break does not fire.

    /// Register a metric with this sensor
    ///
    /// # Arguments
    ///
    /// - `metric_name` - The name of the metric
    /// - `stat` - The statistic to keep
    ///
    /// # Returns
    ///
    /// true if metric is added to sensor, false if sensor is expired
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if a metric of the same name is
    ///   already registered in the registry by another sensor
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#add")]
    pub fn add_with_metric_name(&self, metric_name: MetricName, stat: Box<dyn MeasurableStat>) -> Result<bool, Error> {
        self.add_with_metric_name_config(metric_name, stat, None)
    }

    /// Register a metric with this sensor
    ///
    /// # Arguments
    ///
    /// - `metric_name` - The name of the metric
    /// - `stat` - The statistic to keep
    /// - `config` - A special configuration for this metric. If None use the sensor default
    ///   configuration.
    ///
    /// # Returns
    ///
    /// true if metric is added to sensor, false if sensor is expired
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if a metric of the same name is
    ///   already registered in the registry by another sensor
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#add")]
    pub fn add_with_metric_name_config(
        &self,
        metric_name: MetricName,
        stat: Box<dyn MeasurableStat>,
        config: Option<Arc<MetricConfig>>,
    ) -> Result<bool, Error> {
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
        let measurable_view: Arc<dyn Measurable> = Arc::clone(&stat) as Arc<dyn Measurable>;
        let metric = Arc::new(KafkaMetric::new(
            metric_name.clone(),
            Box::new(MeasurableArc(measurable_view)),
            Arc::clone(&stat_config),
            Arc::clone(&self.time),
        ));

        if let Some(registry) = self.registry.as_ref().and_then(Weak::upgrade) {
            let existing = registry.register_metric(Arc::clone(&metric));
            if existing.is_some() {
                return Err(Error::local_illegal_argument(format!(
                    "A metric named '{metric_name}' already exists, can't register another one."
                )));
            }
        }
        inner.metrics.insert(metric_name, Arc::clone(&metric));
        inner
            .stats
            .push(StatAndConfig { stat: Box::new(StatArc(stat)), config: StatConfigSource::FromMetric(metric) });
        Ok(true)
    }

    /// Register a compound statistic with this sensor with no config override
    ///
    /// # Arguments
    ///
    /// - `stat` - The stat to register
    ///
    /// # Returns
    ///
    /// true if stat is added to sensor, false if sensor is expired
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if a metric of the same name is
    ///   already registered in the registry by another sensor
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#add")]
    pub fn add(&self, stat: Box<dyn CompoundStat>) -> Result<bool, Error> {
        self.add_with_config(stat, None)
    }

    /// Register a compound statistic with this sensor which yields multiple
    /// measurable quantities (like a histogram)
    ///
    /// # Arguments
    ///
    /// - `stat` - The stat to register
    /// - `config` - The configuration for this stat. If None then the stat will use the default
    ///   configuration for this sensor.
    ///
    /// # Returns
    ///
    /// true if stat is added to sensor, false if sensor is expired
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if a metric of the same name is
    ///   already registered in the registry by another sensor
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#add")]
    // The compound stat is recorded once per `record`; each `NamedMeasurable`
    // child gets its own `KafkaMetric` reading the (constant) `statConfig`, and
    // shares state with the compound stat.
    pub fn add_with_config(
        &self,
        stat: Box<dyn CompoundStat>,
        config: Option<Arc<MetricConfig>>,
    ) -> Result<bool, Error> {
        if self.has_expired() {
            return Ok(false);
        }
        let mut inner = self.inner.lock().expect("sensor mutex poisoned");
        let stat_config = config.unwrap_or_else(|| Arc::clone(&self.config));

        // Snapshot the child measurables before moving the compound stat into the
        // recordable stats list.
        let children = stat.stats();

        inner.stats.push(StatAndConfig {
            stat: Box::new(CompoundStatBox(stat)),
            config: StatConfigSource::Constant(Arc::clone(&stat_config)),
        });

        for child in children {
            let metric = Arc::new(KafkaMetric::new(
                child.name().clone(),
                Box::new(MeasurableArc(child.stat())),
                Arc::clone(&stat_config),
                Arc::clone(&self.time),
            ));
            if !inner.metrics.contains_key(child.name()) {
                if let Some(registry) = self.registry.as_ref().and_then(Weak::upgrade) {
                    let existing = registry.register_metric(Arc::clone(&metric));
                    if existing.is_some() {
                        return Err(Error::local_illegal_argument(format!(
                            "A metric named '{}' already exists, can't register another one.",
                            child.name()
                        )));
                    }
                }
                inner.metrics.insert(child.name().clone(), metric);
            }
        }
        Ok(true)
    }

    /// Return if metrics were registered with this sensor.
    ///
    /// # Returns
    ///
    /// true if metrics were registered, false otherwise
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#hasMetrics")]
    pub fn has_metrics(&self) -> bool {
        !self.inner.lock().expect("sensor mutex poisoned").metrics.is_empty()
    }

    /// Returns true if the sensor is eligible for removal due to inactivity.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#hasExpired")]
    pub fn has_expired(&self) -> bool {
        (self.time.milliseconds() - self.last_record_time.load(Ordering::SeqCst))
            > self.inactive_sensor_expiration_time_ms
    }

    /// The metrics registered with this sensor.
    #[doc(alias = "org.apache.kafka.common.metrics.Sensor#metrics")]
    pub(crate) fn metrics(&self) -> Vec<Arc<KafkaMetric>> {
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

/// A `Measurable` view over a shared `Arc<dyn Measurable>` (used both for the
/// `MeasurableStat` `add` path and the `CompoundStat` child measurables).
struct MeasurableArc(Arc<dyn Measurable>);

impl Measurable for MeasurableArc {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.0.measure(config, now)
    }

    /// The shared stat's own type: Java registers the stat itself as the
    /// metric's provider, so its class is what `KafkaMetric.toString` names.
    fn type_name(&self) -> Option<&'static str> {
        self.0.type_name()
    }
}

/// A `Stat` view over a `Box<dyn CompoundStat>` so the compound stat can sit in
/// the sensor's recordable stats list.
struct CompoundStatBox(Box<dyn CompoundStat>);

impl Stat for CompoundStatBox {
    fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
        self.0.record(config, value, time_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Quota;
    use crate::common::metrics::stats::{CumulativeCount, Value};
    use crate::common::protocol::Errors;
    use crate::common::utils::MockTime;
    use crate::common::utils::SystemTime;
    use std::collections::BTreeMap;

    fn level_id(level: RecordingLevel) -> i16 {
        level.id()
    }

    fn info_config() -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().set_record_level(RecordingLevel::Info))
    }
    fn debug_config() -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().set_record_level(RecordingLevel::Debug))
    }
    fn trace_config() -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().set_record_level(RecordingLevel::Trace))
    }

    fn standalone(config: Arc<MetricConfig>, expiration_secs: i64, level: RecordingLevel) -> Sensor {
        Sensor::new(None, "sensor", Vec::new(), config, Arc::new(SystemTime), expiration_secs, level).unwrap()
    }

    fn name(n: &str, g: &str) -> MetricName {
        MetricName::new(n, g, "", BTreeMap::new())
    }

    fn quota_config(quota: Quota) -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().set_quota(quota).set_record_level(RecordingLevel::Info))
    }

    /// The check itself, recording with `check_quotas = false` as Java's
    /// `SensorTest.strictRecord` does and then calling `check_quotas`.
    ///
    /// An upper bound is crossed once the recorded value exceeds it, and the
    /// error carries the metric, the offending value and the bound — Java's
    /// three `QuotaViolationException` fields.
    #[test]
    fn test_check_quotas_reports_an_upper_bound_violation() {
        let sensor = standalone(quota_config(Quota::upper_bound(5.0)), 60, RecordingLevel::Info);
        let metric_name = name("value", "test-group");
        assert!(
            sensor
                .add_with_metric_name(metric_name.clone(), Box::new(Value::new()))
                .unwrap()
        );

        // Under the bound: no violation, in either accessor.
        sensor.record_with_value_time_ms(4.0, 1).unwrap();
        assert!(sensor.check_quotas_with_time_ms(1).is_ok());
        assert!(sensor.check_quotas().is_ok());

        // Over it: Java throws `QuotaViolationException(metric, value, bound)`.
        sensor.record_with_value_time_ms_check_quotas(10.0, 2, false).unwrap();
        let error = sensor
            .check_quotas_with_time_ms(2)
            .expect_err("10.0 exceeds the upper bound of 5.0");
        let Error::QuotaViolation(violation) = &error else {
            panic!("expected a quota violation, got {error:?}");
        };
        assert_eq!(violation.metric_name().name(), "value");
        assert_eq!(violation.value(), 10.0);
        assert_eq!(violation.bound(), 5.0);

        // `QuotaViolationException extends KafkaException` and nothing else, so
        // it is not an `ApiException` and carries no protocol code.
        assert!(error.is_kafka_error());
        assert!(!error.is_api_error());
        assert!(!error.is_retriable_error());
        assert_eq!(error.error(), Errors::UnknownServerError);
        // Java's constructor calls the no-argument `super()`, so `getMessage()`
        // is null; the descriptive text is in the `toString()` override.
        assert_eq!(error.message(), "");
        assert_eq!(
            error.to_string(),
            format!("QuotaViolationError: '{metric_name}' violated quota. Actual: 10, Threshold: 5")
        );
    }

    /// A lower bound is crossed from the other side — Java's
    /// `Quota.acceptable` is the whole test in `checkQuotas`, and a
    /// lower-bound quota must not be read as an upper one.
    #[test]
    fn test_check_quotas_reports_a_lower_bound_violation() {
        let sensor = standalone(quota_config(Quota::lower_bound(5.0)), 60, RecordingLevel::Info);
        assert!(
            sensor
                .add_with_metric_name(name("value", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        sensor.record_with_value_time_ms(10.0, 1).unwrap();
        assert!(sensor.check_quotas_with_time_ms(1).is_ok());

        sensor.record_with_value_time_ms_check_quotas(1.0, 2, false).unwrap();
        let error = sensor
            .check_quotas_with_time_ms(2)
            .expect_err("1.0 is below the lower bound of 5.0");
        let Error::QuotaViolation(violation) = &error else {
            panic!("expected a quota violation, got {error:?}");
        };
        assert_eq!(violation.value(), 1.0);
        assert_eq!(violation.bound(), 5.0);
    }

    /// Java's `record` checks the quotas after recording the stats and throws
    /// on a violation, which stops the parents from recording: a child whose
    /// quota is violated leaves its parent untouched, while
    /// `check_quotas = false` records both.
    #[test]
    fn test_record_violation_does_not_record_the_parents() {
        use crate::common::metrics::Metrics;
        let metrics = Metrics::new();
        let parent = metrics.sensor("parent").unwrap();
        let parent_count = metrics.metric_name("parent.count", "grp1");
        parent
            .add_with_metric_name(parent_count.clone(), Box::new(CumulativeCount::new()))
            .unwrap();
        let child = metrics
            .sensor_with_config_parents("child", Some(quota_config(Quota::upper_bound(5.0))), &[Arc::clone(&parent)])
            .unwrap();
        let child_value = metrics.metric_name("child.value", "grp1");
        child.add_with_metric_name(child_value.clone(), Box::new(Value::new())).unwrap();
        let value_of = |name: &MetricName| metrics.metric(name).unwrap().measurable_value(0);

        child.record_with_value(1.0).unwrap();
        assert_eq!(value_of(&parent_count), 1.0);

        let error = child.record_with_value(10.0).expect_err("10.0 exceeds the upper bound of 5.0");
        assert!(error.is_quota_violation_error());
        assert_eq!(
            error.to_string(),
            format!("QuotaViolationError: '{child_value}' violated quota. Actual: 10, Threshold: 5")
        );
        // The child's stats are recorded before the check, the parent's are not.
        assert_eq!(value_of(&child_value), 10.0);
        assert_eq!(value_of(&parent_count), 1.0);

        child.record_with_value_time_ms_check_quotas(10.0, 0, false).unwrap();
        assert_eq!(value_of(&parent_count), 2.0);
    }

    /// `SensorTest.testStrictQuotaEnforcementWithRate`
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testStrictQuotaEnforcementWithRate")]
    fn test_strict_quota_enforcement_with_rate() {
        use crate::common::metrics::stats::Rate;
        use crate::common::metrics::{Metrics, TimeUnit};
        let wall_clock_ms = SystemTime.milliseconds();
        let time = Arc::new(MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(
            0,
            wall_clock_ms,
            0,
        ));
        let metrics = Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>);
        let config = MetricConfig::new()
            .set_quota(Quota::upper_bound(2.0))
            .set_time_window(1, TimeUnit::Seconds)
            .set_samples(11);
        let sensor = metrics
            .sensor_with_config_parents("sensor", Some(Arc::new(config)), &[])
            .unwrap();
        let metric_name = metrics.metric_name("rate", "test-group");
        assert!(sensor.add_with_metric_name(metric_name.clone(), Box::new(Rate::new())).unwrap());
        let rate_metric = metrics.metric(&metric_name).unwrap();
        let strict_record = |value: f64, time_ms: i64| -> Result<(), Error> {
            sensor.check_quotas_with_time_ms(time_ms)?;
            sensor.record_with_value_time_ms_check_quotas(value, time_ms, false)
        };

        // Recording a first value at T+0 to bring the avg rate to 3 which is already
        // above the quota.
        strict_record(30.0, time.milliseconds()).unwrap();
        assert!((rate_metric.measurable_value(time.milliseconds()) - 3.0).abs() <= 0.1);

        // Theoretically, we should wait 5s to bring back the avg rate to the define quota:
        // ((30 / 10) - 2) / 2 * 10 = 5s
        time.sleep(5000);

        // But, recording a second value is rejected because the avg rate is still equal
        // to 3 after 5s.
        assert!((rate_metric.measurable_value(time.milliseconds()) - 3.0).abs() <= 0.1);
        let error = strict_record(30.0, time.milliseconds()).expect_err("the rate is still above the quota");
        assert!(error.is_quota_violation_error());
    }

    /// A `MeasurableStat` that logs the quota bound of the config each call
    /// receives: Java's `Mockito.verify(stat).record(config, ..)` /
    /// `.measure(config, ..)`.
    struct RecordingStat(Arc<Mutex<Vec<StatCall>>>);

    /// One call a `RecordingStat` received: method, quota bound, value, time.
    type StatCall = (&'static str, f64, f64, i64);

    impl Stat for RecordingStat {
        fn record(&self, config: &MetricConfig, value: f64, time_ms: i64) {
            let bound = config.quota().map_or(f64::NAN, Quota::bound);
            self.0.lock().unwrap().push(("record", bound, value, time_ms));
        }
    }

    impl Measurable for RecordingStat {
        fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
            let bound = config.quota().map_or(f64::NAN, Quota::bound);
            self.0.lock().unwrap().push(("measure", bound, 0.0, now));
            0.0
        }
    }

    /// `SensorTest.testRecordAndCheckQuotaUseMetricConfigOfEachStat`
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testRecordAndCheckQuotaUseMetricConfigOfEachStat")]
    fn test_record_and_check_quota_use_metric_config_of_each_stat() {
        use crate::common::metrics::Metrics;
        let metrics = Metrics::with_time(Arc::new(MockTime::new()));
        let sensor = metrics.sensor("sensor").unwrap();

        let calls1 = Arc::new(Mutex::new(Vec::new()));
        let stat1_config = Arc::new(MetricConfig::new().set_quota(Quota::upper_bound(5.0)));
        sensor
            .add_with_metric_name_config(
                metrics.metric_name("stat1", "test-group"),
                Box::new(RecordingStat(Arc::clone(&calls1))),
                Some(stat1_config),
            )
            .unwrap();
        let calls2 = Arc::new(Mutex::new(Vec::new()));
        let stat2_config = Arc::new(MetricConfig::new().set_quota(Quota::upper_bound(10.0)));
        sensor
            .add_with_metric_name_config(
                metrics.metric_name("stat2", "test-group"),
                Box::new(RecordingStat(Arc::clone(&calls2))),
                Some(stat2_config),
            )
            .unwrap();

        sensor.record_with_value_time_ms(10.0, 1).unwrap();
        assert!(calls1.lock().unwrap().contains(&("record", 5.0, 10.0, 1)));
        assert!(calls2.lock().unwrap().contains(&("record", 10.0, 10.0, 1)));

        sensor.check_quotas_with_time_ms(2).unwrap();
        assert!(calls1.lock().unwrap().contains(&("measure", 5.0, 0.0, 2)));
        assert!(calls2.lock().unwrap().contains(&("measure", 10.0, 0.0, 2)));
    }

    /// `SensorTest.testUpdatingMetricConfigIsReflectedInTheSensor`
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testUpdatingMetricConfigIsReflectedInTheSensor")]
    fn test_updating_metric_config_is_reflected_in_the_sensor() {
        use crate::common::metrics::Metrics;
        let metrics = Metrics::with_time(Arc::new(MockTime::new()));
        let sensor = metrics.sensor("sensor").unwrap();

        let calls = Arc::new(Mutex::new(Vec::new()));
        let stat_name = metrics.metric_name("stat", "test-group");
        let stat_config = Arc::new(MetricConfig::new().set_quota(Quota::upper_bound(5.0)));
        sensor
            .add_with_metric_name_config(
                stat_name.clone(),
                Box::new(RecordingStat(Arc::clone(&calls))),
                Some(stat_config),
            )
            .unwrap();

        sensor.record_with_value_time_ms(10.0, 1).unwrap();
        assert!(calls.lock().unwrap().contains(&("record", 5.0, 10.0, 1)));

        sensor.check_quotas_with_time_ms(2).unwrap();
        assert!(calls.lock().unwrap().contains(&("measure", 5.0, 0.0, 2)));

        // Update the config of the KafkaMetric
        let new_config = Arc::new(MetricConfig::new().set_quota(Quota::upper_bound(10.0)));
        metrics.metric(&stat_name).unwrap().set_config(new_config);

        sensor.record_with_value_time_ms(10.0, 3).unwrap();
        assert!(calls.lock().unwrap().contains(&("record", 10.0, 10.0, 3)));

        sensor.check_quotas_with_time_ms(4).unwrap();
        assert!(calls.lock().unwrap().contains(&("measure", 10.0, 0.0, 4)));
    }

    /// A sensor whose config carries no quota can never violate one, however
    /// much is recorded — Java's `if (quota != null)` guard.
    #[test]
    fn test_check_quotas_is_a_no_op_without_a_quota() {
        let sensor = standalone(info_config(), 60, RecordingLevel::Info);
        assert!(
            sensor
                .add_with_metric_name(name("value", "test-group"), Box::new(Value::new()))
                .unwrap()
        );
        sensor.record_with_value_time_ms(f64::MAX, 1).unwrap();
        assert!(sensor.check_quotas_with_time_ms(1).is_ok());
        assert!(sensor.check_quotas().is_ok());
    }

    /// `SensorTest.testCheckQuotasInMultiThreads`: `check_quotas` is called from
    /// many tasks at once (Java's ReplicaFetcherThreads) and must neither
    /// deadlock nor report a violation for an upper bound of `f64::MAX`.
    #[test]
    fn test_check_quotas_in_multiple_threads() {
        let sensor = Arc::new(standalone(quota_config(Quota::upper_bound(f64::MAX)), 60, RecordingLevel::Info));
        assert!(
            sensor
                .add_with_metric_name(name("test-metric", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        let mut handles = Vec::new();
        for index in 0..10i64 {
            let sensor = Arc::clone(&sensor);
            handles.push(std::thread::spawn(move || {
                for j in 0..20i64 {
                    sensor
                        .record_with_value_time_ms_check_quotas((j * index) as f64, j, false)
                        .unwrap();
                    sensor.check_quotas().expect("an upper bound of f64::MAX is never violated");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("check_quotas must be safe to call from many tasks");
        }
    }

    // SensorTest.testRecordLevelEnum
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testRecordLevelEnum")]
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
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testShouldRecordForInfoLevelSensor")]
    fn test_should_record_for_info_level_sensor() {
        assert!(standalone(info_config(), 0, RecordingLevel::Info).should_record());
        assert!(standalone(debug_config(), 0, RecordingLevel::Info).should_record());
        assert!(standalone(trace_config(), 0, RecordingLevel::Info).should_record());
    }

    // SensorTest.testShouldRecordForDebugLevelSensor
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testShouldRecordForDebugLevelSensor")]
    fn test_should_record_for_debug_level_sensor() {
        assert!(!standalone(info_config(), 0, RecordingLevel::Debug).should_record());
        assert!(standalone(debug_config(), 0, RecordingLevel::Debug).should_record());
        assert!(standalone(trace_config(), 0, RecordingLevel::Debug).should_record());
    }

    // SensorTest.testShouldRecordForTraceLevelSensor
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testShouldRecordForTraceLevelSensor")]
    fn test_should_record_for_trace_level_sensor() {
        assert!(!standalone(info_config(), 0, RecordingLevel::Trace).should_record());
        assert!(!standalone(debug_config(), 0, RecordingLevel::Trace).should_record());
        assert!(standalone(trace_config(), 0, RecordingLevel::Trace).should_record());
    }

    // SensorTest.shouldReturnPresenceOfMetrics (standalone-sensor part)
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#shouldReturnPresenceOfMetrics")]
    fn should_return_presence_of_metrics() {
        let sensor = standalone(Arc::new(MetricConfig::new()), i64::MAX, RecordingLevel::Info);
        assert!(!sensor.has_metrics());
        sensor
            .add_with_metric_name(name("name1", "group1"), Box::new(CumulativeCount::new()))
            .unwrap();
        assert!(sensor.has_metrics());
        sensor
            .add_with_metric_name(name("name2", "group2"), Box::new(Value::new()))
            .unwrap();
        assert!(sensor.has_metrics());
    }

    // SensorTest.testExpiredSensor
    //
    // Java uses `Avg`/`Meter` (M2 stats) as the *thing being added*, but the
    // behavioral assertion is purely on the boolean returned by `add` — which is
    // the same for any `MeasurableStat`. We substitute the M1 stats
    // `CumulativeCount` / `Value` (the same substitution principle already
    // applied to `testIdempotentAdd` / `testSimpleStats`). The Java test uses a
    // `Metrics`-backed sensor; here we use a standalone sensor (registry `None`)
    // driven by `MockTime`, which exercises the identical `has_expired()` /
    // `add`-returns-`false` path without needing the registry.
    #[test]
    #[doc(alias = "org.apache.kafka.common.metrics.SensorTest#testExpiredSensor")]
    fn test_expired_sensor() {
        let time = Arc::new(MockTime::new());
        let inactive_sensor_expiration_time_seconds = 60i64;
        let sensor = Sensor::new(
            None,
            "sensor",
            Vec::new(),
            Arc::new(MetricConfig::new()),
            Arc::clone(&time) as Arc<dyn Time>,
            inactive_sensor_expiration_time_seconds,
            RecordingLevel::Info,
        )
        .unwrap();

        // Before expiry, adds succeed.
        assert!(
            sensor
                .add_with_metric_name(name("test1", "grp1"), Box::new(CumulativeCount::new()))
                .unwrap()
        );
        assert!(
            sensor
                .add_with_metric_name(name("test2", "grp1"), Box::new(Value::new()))
                .unwrap()
        );
        assert_eq!(2, sensor.metrics().len());
        assert!(!sensor.has_expired());

        // Advance past the inactivity window.
        time.sleep((inactive_sensor_expiration_time_seconds + 1) * 1000);
        assert!(sensor.has_expired());

        // Adds to an expired sensor are a no-op returning `false`; the metric is
        // NOT registered (count unchanged).
        assert!(
            !sensor
                .add_with_metric_name(name("test3", "grp1"), Box::new(CumulativeCount::new()))
                .unwrap()
        );
        assert!(
            !sensor
                .add_with_metric_name_config(name("test4", "grp1"), Box::new(Value::new()), None)
                .unwrap()
        );
        assert_eq!(2, sensor.metrics().len());

        // Java's `record` does not gate on expiry; recording resets
        // `last_record_time`, so the sensor is no longer expired afterwards and
        // the metric count remains unchanged.
        sensor.record_with_value(1.0).unwrap();
        assert!(!sensor.has_expired());
        assert_eq!(2, sensor.metrics().len());
    }

    // ---------------------------------------------------------------------
    // Milestone-9 Phase M8 — pure in-process micro-bench (no broker needed).
    //
    // Quantifies the per-call cost of `Sensor::record_with_value_time_ms` on a realistic
    // fetch-shaped sensor (a `Meter` = Rate + CumulativeSum, plus `Avg` +
    // `Max` — the stat shape used by `bytes-fetched` / `fetch-latency` and
    // friends). This is the unit of cost that the consumer pays PER FETCH /
    // PER PARTITION (never per record — see
    // `completed_fetch::tests::test_per_record_loop_is_pure_counter_no_sensor_record`).
    //
    // Marked `#[ignore]` so it does not run in CI (timing is environment
    // dependent), but it BUILDS in CI and is runnable on demand:
    //
    //     cargo test --lib bench_sensor_record_ns -- --ignored --nocapture
    //
    // The printed ns/call, multiplied by the per-fetch/per-partition record
    // frequency in `design/current/consumer-metrics-perf-analysis.md`, gives
    // the metrics-on overhead per poll cycle without needing a broker.
    #[test]
    #[ignore = "micro-bench; run with --ignored --nocapture"]
    fn bench_sensor_record_ns() {
        use crate::common::metrics::stats::{Avg, Max, Meter};
        use std::time::Instant;

        let time = Arc::new(MockTime::new());
        let sensor = Sensor::new(
            None,
            "bench",
            Vec::new(),
            info_config(),
            Arc::clone(&time) as Arc<dyn Time>,
            i64::MAX,
            RecordingLevel::Info,
        )
        .unwrap();
        // Realistic fetch-sensor stat shape: a Meter (rate + total) + Avg + Max.
        sensor.add(Box::new(Meter::new(name("rate", "g"), name("total", "g")))).unwrap();
        sensor.add_with_metric_name(name("avg", "g"), Box::new(Avg::new())).unwrap();
        sensor.add_with_metric_name(name("max", "g"), Box::new(Max::new())).unwrap();

        // Warm up (JIT-free, but warms caches / branch predictors and forces
        // the first sample-buffer allocation outside the timed loop).
        let mut now = time.milliseconds();
        for i in 0..10_000 {
            sensor.record_with_value_time_ms(i as f64, now).unwrap();
        }

        const ITERS: u64 = 2_000_000;
        let start = Instant::now();
        for i in 0..ITERS {
            // Advance the mock clock occasionally so window rollover is
            // exercised the way a real fetch loop would (samples age out).
            if i % 4096 == 0 {
                now += 1;
            }
            sensor.record_with_value_time_ms(i as f64, now).unwrap();
        }
        let elapsed = start.elapsed();
        let ns_per_call = elapsed.as_nanos() as f64 / ITERS as f64;
        eprintln!(
            "M8 micro-bench: Sensor::record_with_value_time_ms over Meter+Avg+Max = {ns_per_call:.1} ns/call \
             ({ITERS} iters in {elapsed:?})"
        );
        // Sanity floor: the work is non-trivial (mutex + 3 compound stats), so
        // a sub-nanosecond reading would mean the call was optimized away.
        assert!(ns_per_call > 0.0);
    }
}
