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

use crate::common::metric::Metric;
use crate::common::metrics::metrics::MetricsShared;
use crate::common::metrics::{
    CompoundStat, KafkaMetric, Measurable, MeasurableStat, MetricConfig, MetricValueProvider, QuotaViolationError,
    Stat, Time,
};
use crate::common::{Error, MetricName};

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

    /// Case-insensitive lookup by name. Returns an error for an unknown name,
    /// mirroring Java's `valueOf` `IllegalArgumentException`.
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
/// so the stat always reads the backing metric's (possibly updated) config; for
/// the `add(CompoundStat, config)` path it is a constant `statConfig` supplier.
/// Both are modelled by [`StatConfigSource`].
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

    // Java's `record` overloads (`Sensor.java:183,195,209`, plus the
    // `checkQuotas` form deferred below) have an EMPTY parameter-name
    // intersection, and the no-arg `record()` (`:183`) matches it — so that one
    // keeps the plain name and the others carry their Rust parameters
    // (CLAUDE.md §2). Note this is the reverse of the pre-rule shape, where the
    // one-argument form held the plain name and the no-arg form was called
    // `record_occurrence`.

    /// Record an occurrence; short-hand for `record_value(1.0)`.
    ///
    /// Mirrors Java's `record()` (`Sensor.java:183`).
    pub fn record(&self) {
        if self.should_record() {
            self.record_internal(1.0, self.time.milliseconds());
        }
    }

    /// Record a value with this sensor at the current time.
    ///
    /// Mirrors Java's `record(double value)` (`Sensor.java:195`).
    pub fn record_value(&self, value: f64) {
        if self.should_record() {
            self.record_internal(value, self.time.milliseconds());
        }
    }

    /// Record a value at a known time.
    ///
    /// Mirrors Java's `record(double value, long timeMs)` (`Sensor.java:209`).
    pub fn record_value_time_ms(&self, value: f64, time_ms: i64) {
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
        // Java runs `if (checkQuotas) checkQuotas(timeMs);` here
        // (`Sensor.java:239-240`) and lets the `QuotaViolationException`
        // propagate out of `record`. The check itself IS translated — see
        // [`check_quotas`](Self::check_quotas) / [`check_quotas_time_ms`] — but it
        // cannot be called from here yet, and the blocker is the signature, not
        // the stats: `record` / `record_value` / `record_value_time_ms` return `()`,
        // so there is nowhere to put the `Result` that CLAUDE.md §9.1/§10.2
        // requires, and Java's `record(value, timeMs, checkQuotas)` overload —
        // the boolean that selects enforcement — has no Rust counterpart for the
        // same reason. Adding the `Result` reaches 45 call sites across
        // `src/consumer/`, so it is its own piece of work; swallowing the
        // violation into a log here instead would both diverge from Java (which
        // stops the caller) and put a per-metric scan on the per-record path
        // (CLAUDE.md §11 / DoD #10).
        //
        // Until then a caller that configures a `Quota` enforces it by calling
        // `check_quotas()` itself, which is what Java's broker-side
        // `ClientQuotaManager` does too.
        for parent in &self.parents {
            parent.record_value_time_ms(value, time_ms);
        }
    }

    /// Check whether any metric with a configured quota has been violated, at
    /// the current time.
    ///
    /// Mirrors Java's `checkQuotas()` (`Sensor.java:247-249`). The group's
    /// parameter-name intersection is empty and this no-arg form matches it, so
    /// it keeps the plain name and its sibling is suffixed (CLAUDE.md §2).
    pub fn check_quotas(&self) -> Result<(), Error> {
        self.check_quotas_time_ms(self.time.milliseconds())
    }

    /// Check whether any metric with a configured quota has been violated, as of
    /// `time_ms`.
    ///
    /// Mirrors Java's `checkQuotas(long timeMs)` (`Sensor.java:251-268`). Two
    /// differences, both forced by what exists on the Rust side:
    ///
    ///  - Java guards on `config != null`; a Rust [`MetricConfig`] is always
    ///    present (each metric holds an `Arc<MetricConfig>`), so only the
    ///    `quota != null` guard survives as `Option<Quota>`.
    ///  - Java special-cases `metric.measurable() instanceof TokenBucket`,
    ///    treating any negative value as a violation regardless of the bound's
    ///    direction. `TokenBucket` is not translated (there is no
    ///    `common::metrics::stats::TokenBucket`), so no metric can take that
    ///    branch and only `Quota::acceptable` is consulted. The branch must be
    ///    restored together with `TokenBucket`.
    pub fn check_quotas_time_ms(&self, time_ms: i64) -> Result<(), Error> {
        // The metrics are cloned out of the guard rather than measured under it:
        // `measurable_value` calls into user-supplied `Measurable` code, and
        // holding the sensor lock across that would let a measurable that
        // re-enters the sensor deadlock. Java has the same call under
        // `synchronized (this)` but its monitor is reentrant.
        let metrics: Vec<Arc<KafkaMetric>> = {
            let inner = self.inner.lock().expect("sensor mutex poisoned");
            inner.metrics.values().map(Arc::clone).collect()
        };
        for metric in metrics {
            let config = metric.config();
            if let Some(quota) = config.quota() {
                let value = metric.measurable_value(time_ms);
                if !quota.acceptable(value) {
                    return Err(Error::QuotaViolation(Box::new(QuotaViolationError::new(
                        metric.metric_name().clone(),
                        value,
                        quota.bound(),
                    ))));
                }
            }
        }
        Ok(())
    }

    // Java's four `add` overloads (`Sensor.java:279,290,316,328`) all name their
    // statistic parameter `stat`, so the parameter-name intersection is `{stat}`
    // — and the overload whose parameters are exactly that is the COMPOUND form
    // `add(CompoundStat stat)` (`:279`). So the compound form keeps the plain
    // name `add` and the metric-name forms are suffixed with their Rust
    // parameters beyond the intersection (CLAUDE.md §2). The two families differ
    // in type as well as arity, but the derived names are already distinct, so
    // §2's type tie-break does not fire.

    /// Register a metric with this sensor.
    ///
    /// Mirrors Java's `add(MetricName metricName, MeasurableStat stat)`
    /// (`Sensor.java:316`). Suffixed per the note above.
    ///
    /// Returns `Ok(true)` if added, `Ok(false)` if the sensor is expired,
    /// `Err` if the metric name already exists in the registry under a
    /// different sensor (Java's `IllegalArgumentException`).
    pub fn add_metric_name(&self, metric_name: MetricName, stat: Box<dyn MeasurableStat>) -> Result<bool, Error> {
        self.add_metric_name_config(metric_name, stat, None)
    }

    /// Register a metric with this sensor with an optional per-metric config.
    ///
    /// Mirrors Java's
    /// `add(MetricName metricName, MeasurableStat stat, MetricConfig config)`
    /// (`Sensor.java:328`).
    pub fn add_metric_name_config(
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
            MetricValueProvider::Measurable(Box::new(MeasurableArc(measurable_view))),
            Arc::clone(&stat_config),
            Arc::clone(&self.time),
        ));

        if let Some(registry) = &self.registry {
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

    /// Register a compound statistic with this sensor with no config override.
    ///
    /// Mirrors Java's `add(CompoundStat stat)` (`Sensor.java:279`), whose
    /// parameter list is exactly the `add` group's intersection `{stat}` — so
    /// this form keeps the plain name (CLAUDE.md §2).
    pub fn add(&self, stat: Box<dyn CompoundStat>) -> Result<bool, Error> {
        self.add_config(stat, None)
    }

    /// Register a compound statistic with this sensor which yields multiple
    /// measurable quantities (like a histogram). Mirrors Java's
    /// `add(CompoundStat stat, MetricConfig config)` (`Sensor.java:290`).
    ///
    /// The compound stat is recorded once per `record`; each `NamedMeasurable`
    /// child gets its own `KafkaMetric` reading the (constant) `statConfig`. The
    /// child measurables share state with the compound stat, so a record is
    /// reflected in every child's measured value.
    pub fn add_config(&self, stat: Box<dyn CompoundStat>, config: Option<Arc<MetricConfig>>) -> Result<bool, Error> {
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
                MetricValueProvider::Measurable(Box::new(MeasurableArc(child.stat()))),
                Arc::clone(&stat_config),
                Arc::clone(&self.time),
            ));
            if !inner.metrics.contains_key(child.name()) {
                if let Some(registry) = &self.registry {
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

/// A `Measurable` view over a shared `Arc<dyn Measurable>` (used both for the
/// `MeasurableStat` `add` path and the `CompoundStat` child measurables).
struct MeasurableArc(Arc<dyn Measurable>);

impl Measurable for MeasurableArc {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.0.measure(config, now)
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
    use crate::common::metrics::stats::{CumulativeCount, Value};
    use crate::common::metrics::time::mock::MockTime;
    use crate::common::metrics::{Quota, SystemTime};
    use crate::common::protocol::Errors;
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

    fn quota_config(quota: Quota) -> Arc<MetricConfig> {
        Arc::new(MetricConfig::new().with_quota(quota).with_record_level(RecordingLevel::Info))
    }

    /// `SensorTest.testStrictQuotaEnforcement`, reduced to the check itself:
    /// Java's `strictRecord` is `sensor.record(value, timeMs, true)`, whose only
    /// effect beyond `record` is the `checkQuotas(timeMs)` this asserts on.
    ///
    /// An upper bound is crossed once the recorded value exceeds it, and the
    /// error carries the metric, the offending value and the bound — Java's
    /// three `QuotaViolationException` fields.
    #[test]
    fn test_check_quotas_reports_an_upper_bound_violation() {
        let sensor = standalone(quota_config(Quota::upper_bound(5.0)), 60, RecordingLevel::Info);
        let metric_name = name("value", "test-group");
        assert!(sensor.add_metric_name(metric_name.clone(), Box::new(Value::new())).unwrap());

        // Under the bound: no violation, in either accessor.
        sensor.record_value_time_ms(4.0, 1);
        assert!(sensor.check_quotas_time_ms(1).is_ok());
        assert!(sensor.check_quotas().is_ok());

        // Over it: Java throws `QuotaViolationException(metric, value, bound)`.
        sensor.record_value_time_ms(10.0, 2);
        let error = sensor.check_quotas_time_ms(2).expect_err("10.0 exceeds the upper bound of 5.0");
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
                .add_metric_name(name("value", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        sensor.record_value_time_ms(10.0, 1);
        assert!(sensor.check_quotas_time_ms(1).is_ok());

        sensor.record_value_time_ms(1.0, 2);
        let error = sensor.check_quotas_time_ms(2).expect_err("1.0 is below the lower bound of 5.0");
        let Error::QuotaViolation(violation) = &error else {
            panic!("expected a quota violation, got {error:?}");
        };
        assert_eq!(violation.value(), 1.0);
        assert_eq!(violation.bound(), 5.0);
    }

    /// A sensor whose config carries no quota can never violate one, however
    /// much is recorded — Java's `if (quota != null)` guard.
    #[test]
    fn test_check_quotas_is_a_no_op_without_a_quota() {
        let sensor = standalone(info_config(), 60, RecordingLevel::Info);
        assert!(
            sensor
                .add_metric_name(name("value", "test-group"), Box::new(Value::new()))
                .unwrap()
        );
        sensor.record_value_time_ms(f64::MAX, 1);
        assert!(sensor.check_quotas_time_ms(1).is_ok());
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
                .add_metric_name(name("test-metric", "test-group"), Box::new(Value::new()))
                .unwrap()
        );

        let mut handles = Vec::new();
        for index in 0..10i64 {
            let sensor = Arc::clone(&sensor);
            handles.push(std::thread::spawn(move || {
                for j in 0..20i64 {
                    sensor.record_value_time_ms((j * index) as f64, j);
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
        sensor
            .add_metric_name(name("name1", "group1"), Box::new(CumulativeCount::new()))
            .unwrap();
        assert!(sensor.has_metrics());
        sensor.add_metric_name(name("name2", "group2"), Box::new(Value::new())).unwrap();
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
                .add_metric_name(name("test1", "grp1"), Box::new(CumulativeCount::new()))
                .unwrap()
        );
        assert!(sensor.add_metric_name(name("test2", "grp1"), Box::new(Value::new())).unwrap());
        assert_eq!(2, sensor.metrics().len());
        assert!(!sensor.has_expired());

        // Advance past the inactivity window.
        time.sleep((inactive_sensor_expiration_time_seconds + 1) * 1000);
        assert!(sensor.has_expired());

        // Adds to an expired sensor are a no-op returning `false`; the metric is
        // NOT registered (count unchanged).
        assert!(
            !sensor
                .add_metric_name(name("test3", "grp1"), Box::new(CumulativeCount::new()))
                .unwrap()
        );
        assert!(
            !sensor
                .add_metric_name_config(name("test4", "grp1"), Box::new(Value::new()), None)
                .unwrap()
        );
        assert_eq!(2, sensor.metrics().len());

        // Java's `record` does not gate on expiry; recording resets
        // `last_record_time`, so the sensor is no longer expired afterwards and
        // the metric count remains unchanged.
        sensor.record_value(1.0);
        assert!(!sensor.has_expired());
        assert_eq!(2, sensor.metrics().len());
    }

    // ---------------------------------------------------------------------
    // Milestone-9 Phase M8 — pure in-process micro-bench (no broker needed).
    //
    // Quantifies the per-call cost of `Sensor::record_value_time_ms` on a realistic
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
        sensor.add_metric_name(name("avg", "g"), Box::new(Avg::new())).unwrap();
        sensor.add_metric_name(name("max", "g"), Box::new(Max::new())).unwrap();

        // Warm up (JIT-free, but warms caches / branch predictors and forces
        // the first sample-buffer allocation outside the timed loop).
        let mut now = time.milliseconds();
        for i in 0..10_000 {
            sensor.record_value_time_ms(i as f64, now);
        }

        const ITERS: u64 = 2_000_000;
        let start = Instant::now();
        for i in 0..ITERS {
            // Advance the mock clock occasionally so window rollover is
            // exercised the way a real fetch loop would (samples age out).
            if i % 4096 == 0 {
                now += 1;
            }
            sensor.record_value_time_ms(i as f64, now);
        }
        let elapsed = start.elapsed();
        let ns_per_call = elapsed.as_nanos() as f64 / ITERS as f64;
        eprintln!(
            "M8 micro-bench: Sensor::record_value_time_ms over Meter+Avg+Max = {ns_per_call:.1} ns/call \
             ({ITERS} iters in {elapsed:?})"
        );
        // Sanity floor: the work is non-trivial (mutex + 3 compound stats), so
        // a sub-nanosecond reading would mean the call was optimized away.
        assert!(ns_per_call > 0.0);
    }
}
