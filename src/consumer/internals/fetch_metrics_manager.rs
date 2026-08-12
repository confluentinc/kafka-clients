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

//! Records lag, lead, latency, and fetch metrics
//! (`org.apache.kafka.clients.consumer.internals.FetchMetricsManager`).

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::common::metrics::stats::WindowedCount;
use crate::common::metrics::{ClosureGauge, MetricValue, MetricValueProvider, Metrics, RecordingLevel, Sensor};
use crate::common::{KafkaError, TopicPartition};
use crate::consumer::internals::fetch_metrics_registry::FetchMetricsRegistry;
use crate::consumer::internals::sensor_builder::SensorBuilder;
use crate::consumer::internals::subscription_state::SubscriptionState;

/// Records lag, lead, latency, and fetch metrics. It keeps an internal ID of
/// the assigned set of partitions which is updated to ensure the set of metrics
/// it records matches up with the topic-partitions in use.
///
/// Owned by the bg-task `AbstractFetch` and shared (`Arc`) with the
/// per-response `FetchMetricsAggregator`. Sensors record through interior
/// mutability (`&self`); the assignment-tracking fields are mutated only from
/// the bg task's `maybe_update_assignment` (`&mut self`).
pub(crate) struct FetchMetricsManager {
    metrics: Arc<Metrics>,
    metrics_registry: FetchMetricsRegistry,
    throttle_time: Arc<Sensor>,
    bytes_fetched: Arc<Sensor>,
    records_fetched: Arc<Sensor>,
    fetch_latency: Arc<Sensor>,
    records_lag: Arc<Sensor>,
    records_lead: Arc<Sensor>,

    /// Assignment-tracking state. Interior-mutable because the manager is
    /// `Arc`-shared (per-response aggregators hold clones), but only ever
    /// mutated by `maybe_update_assignment` on the bg task — the `Mutex` is a
    /// cheap, per-poll (not per-record) guard, never held across an `.await`.
    assignment: Mutex<AssignmentTracking>,
}

struct AssignmentTracking {
    assignment_id: u32,
    assigned_partitions: HashSet<TopicPartition>,
}

impl FetchMetricsManager {
    /// Builds the manager and registers the six client-level sensors.
    ///
    /// All six client-level sensors are registered at INFO, matching Java
    /// (Java's `SensorBuilder` → `metrics.sensor(name)` defaults every sensor
    /// to `RecordingLevel.INFO`). In particular the `records-lag` /
    /// `records-lead` sensors are INFO, so a default (INFO) consumer exposes the
    /// commonly-monitored `records-lag-max` / `records-lead-min` metrics exactly
    /// as Java does.
    ///
    /// The DETAILED per-partition lag/lead sensors (`{tp}.records-lag`,
    /// `-lag-avg`, `-lag-max`, `{tp}.records-lead`, `-lead-min`, `-lead-avg`)
    /// are ALSO registered at INFO — full Java parity. Java's `SensorBuilder`
    /// creates every sensor (including these per-partition detail sensors,
    /// `FetchMetricsManager.java:133,148`) via `metrics.sensor(name)`, which
    /// defaults to `RecordingLevel.INFO`. There is NO DEBUG gating in Java's
    /// fetch metrics, so we record the full per-partition metric set per
    /// partition per poll at the default INFO level — the accepted Java-parity
    /// cost (to be measured in M8). The metric VALUES are Java-identical.
    pub(crate) fn new(metrics: Arc<Metrics>, metrics_registry: FetchMetricsRegistry) -> Self {
        // Each `build_*` closure registers one sensor and returns it (or the
        // registration error). Sensor registration only fails on a duplicate
        // metric name (a construction-time programming error here), so the
        // `.expect`s are unreachable in practice.
        let build_throttle = || -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::new(&metrics, "fetch-throttle-time", RecordingLevel::Info)?
                .with_avg(&metrics_registry.fetch_throttle_time_avg)?
                .with_max(&metrics_registry.fetch_throttle_time_max)?
                .build())
        };
        let build_bytes = || -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::new(&metrics, "bytes-fetched", RecordingLevel::Info)?
                .with_avg(&metrics_registry.fetch_size_avg)?
                .with_max(&metrics_registry.fetch_size_max)?
                .with_meter(&metrics_registry.bytes_consumed_rate, &metrics_registry.bytes_consumed_total)?
                .build())
        };
        let build_records = || -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::new(&metrics, "records-fetched", RecordingLevel::Info)?
                .with_avg(&metrics_registry.records_per_request_avg)?
                .with_meter(
                    &metrics_registry.records_consumed_rate,
                    &metrics_registry.records_consumed_total,
                )?
                .build())
        };
        let build_latency = || -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::new(&metrics, "fetch-latency", RecordingLevel::Info)?
                .with_avg(&metrics_registry.fetch_latency_avg)?
                .with_max(&metrics_registry.fetch_latency_max)?
                .with_meter_stat(
                    WindowedCount::new().into_sampled_stat(),
                    &metrics_registry.fetch_request_rate,
                    &metrics_registry.fetch_request_total,
                )?
                .build())
        };
        // INFO, matching Java: the client-level `records-lag-max` /
        // `records-lead-min` are on by default, as are the per-partition DETAIL
        // sensors (full Java parity, see ctor doc + `record_partition_lag/lead`).
        let build_lag = || -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::new(&metrics, "records-lag", RecordingLevel::Info)?
                .with_max(&metrics_registry.records_lag_max)?
                .build())
        };
        let build_lead = || -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::new(&metrics, "records-lead", RecordingLevel::Info)?
                .with_min(&metrics_registry.records_lead_min)?
                .build())
        };

        let throttle_time = build_throttle().expect("registering fetch-throttle-time sensor");
        let bytes_fetched = build_bytes().expect("registering bytes-fetched sensor");
        let records_fetched = build_records().expect("registering records-fetched sensor");
        let fetch_latency = build_latency().expect("registering fetch-latency sensor");
        let records_lag = build_lag().expect("registering records-lag sensor");
        let records_lead = build_lead().expect("registering records-lead sensor");

        Self {
            metrics,
            metrics_registry,
            throttle_time,
            bytes_fetched,
            records_fetched,
            fetch_latency,
            records_lag,
            records_lead,
            assignment: Mutex::new(AssignmentTracking { assignment_id: 0, assigned_partitions: HashSet::new() }),
        }
    }

    /// Test helper: builds a manager over a fresh default-config `Metrics`
    /// (`metrics.recording.level=INFO`). Mirrors the many Phase 7a fetch-path
    /// tests that previously had no `FetchMetricsManager`; they now construct a
    /// throwaway one so their fetch-path code-under-test still type-checks. At
    /// INFO the partition lag/lead recording is gated off, so these tests pay
    /// nothing for it.
    #[cfg(test)]
    pub(crate) fn for_test() -> Arc<Self> {
        // Empty tag set + default-config `Metrics` (no default tags): the
        // template tag sets match the runtime tags, so the constructor's metric
        // registration succeeds. These tests don't assert metric values.
        let metrics = Arc::new(Metrics::new());
        let registry = FetchMetricsRegistry::new(indexmap::IndexSet::new(), "consumer");
        Arc::new(Self::new(metrics, registry))
    }

    /// Test-only accessor for the registry this manager records into.
    #[cfg(test)]
    pub(crate) fn metrics_for_test(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Test-only accessor for the metric-name templates.
    #[cfg(test)]
    pub(crate) fn registry_for_test(&self) -> &FetchMetricsRegistry {
        &self.metrics_registry
    }

    /// The sensor recording fetch throttle time, exposed for plumbing into the
    /// network client (Java `throttleTimeSensor()`).
    ///
    /// The sensor + its `fetch-throttle-time-avg`/`-max` metrics are registered
    /// in the constructor and are fully recordable. Wiring the broker's
    /// `throttle_time_ms` from the fetch response INTO this sensor lives in
    /// `NetworkClientDelegate` (Java passes `throttleTimeSensor()` into the
    /// network client at construction); that delegate-side recording is a
    /// documented carry-over for the network-client metrics pass. Exercised by
    /// the manager test, which records through this sensor directly.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn throttle_time_sensor(&self) -> Arc<Sensor> {
        Arc::clone(&self.throttle_time)
    }

    /// Records the latency of a fetch request against the client-level sensor
    /// and, if present, the per-node latency sensor.
    pub(crate) fn record_latency(&self, node: &str, request_latency_ms: i64) {
        self.fetch_latency.record(request_latency_ms as f64);
        if !node.is_empty() {
            let node_time_name = format!("node-{node}.latency");
            if let Some(node_request_time) = self.metrics.get_sensor(&node_time_name) {
                node_request_time.record(request_latency_ms as f64);
            }
        }
    }

    /// Records the number of bytes fetched at the client level.
    pub(crate) fn record_bytes_fetched(&self, bytes: i32) {
        self.bytes_fetched.record(bytes as f64);
    }

    /// Records the number of records fetched at the client level.
    pub(crate) fn record_records_fetched(&self, records: i32) {
        self.records_fetched.record(records as f64);
    }

    /// Records the number of bytes fetched for a single topic.
    pub(crate) fn record_bytes_fetched_topic(&self, topic: &str, bytes: i32) {
        let name = topic_bytes_fetched_metric_name(topic);
        self.maybe_record_deprecated_bytes_fetched(&name, topic, bytes);

        let tags = single_tag("topic", topic);
        let bytes_fetched = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(&self.metrics, &name, RecordingLevel::Info, tags)?
                .with_avg(&self.metrics_registry.topic_fetch_size_avg)?
                .with_max(&self.metrics_registry.topic_fetch_size_max)?
                .with_meter(
                    &self.metrics_registry.topic_bytes_consumed_rate,
                    &self.metrics_registry.topic_bytes_consumed_total,
                )?
                .build())
        })();
        let Some(bytes_fetched) = resolve_sensor(bytes_fetched, "topic bytes-fetched sensor") else {
            return;
        };
        bytes_fetched.record(bytes as f64);
    }

    /// Records the number of records fetched for a single topic.
    pub(crate) fn record_records_fetched_topic(&self, topic: &str, records: i32) {
        let name = topic_records_fetched_metric_name(topic);
        self.maybe_record_deprecated_records_fetched(&name, topic, records);

        let tags = single_tag("topic", topic);
        let records_fetched = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(&self.metrics, &name, RecordingLevel::Info, tags)?
                .with_avg(&self.metrics_registry.topic_records_per_request_avg)?
                .with_meter(
                    &self.metrics_registry.topic_records_consumed_rate,
                    &self.metrics_registry.topic_records_consumed_total,
                )?
                .build())
        })();
        let Some(records_fetched) = resolve_sensor(records_fetched, "topic records-fetched sensor") else {
            return;
        };
        records_fetched.record(records as f64);
    }

    /// Records the lag for a single partition.
    ///
    /// Both the client-level `records-lag-max` sensor and the DETAILED
    /// per-partition lag sensors are INFO and recorded unconditionally, exactly
    /// as Java does (`FetchMetricsManager.recordPartitionLag`). There is no
    /// DEBUG gating: a default (INFO) consumer records the full per-partition
    /// metric set per partition per poll — the accepted Java-parity cost.
    pub(crate) fn record_partition_lag(&self, tp: &TopicPartition, lag: i64) {
        self.records_lag.record(lag as f64);

        let name = partition_records_lag_metric_name(tp);
        self.maybe_record_deprecated_partition_lag(&name, tp, lag);

        let tags = topic_partition_tags_raw(tp);
        let records_lag = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(&self.metrics, &name, RecordingLevel::Info, tags)?
                .with_value(&self.metrics_registry.partition_records_lag)?
                .with_max(&self.metrics_registry.partition_records_lag_max)?
                .with_avg(&self.metrics_registry.partition_records_lag_avg)?
                .build())
        })();
        let Some(records_lag) = resolve_sensor(records_lag, "partition records-lag sensor") else {
            return;
        };
        records_lag.record(lag as f64);
    }

    /// Records the lead for a single partition.
    ///
    /// Both the client-level `records-lead-min` sensor and the DETAILED
    /// per-partition lead sensors are INFO and recorded unconditionally, exactly
    /// as Java does (see [`Self::record_partition_lag`]).
    pub(crate) fn record_partition_lead(&self, tp: &TopicPartition, lead: i64) {
        self.records_lead.record(lead as f64);

        let name = partition_records_lead_metric_name(tp);
        self.maybe_record_deprecated_partition_lead(&name, tp, lead as f64);

        let tags = topic_partition_tags_raw(tp);
        let records_lead = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(&self.metrics, &name, RecordingLevel::Info, tags)?
                .with_value(&self.metrics_registry.partition_records_lead)?
                .with_min(&self.metrics_registry.partition_records_lead_min)?
                .with_avg(&self.metrics_registry.partition_records_lead_avg)?
                .build())
        })();
        let Some(records_lead) = resolve_sensor(records_lead, "partition records-lead sensor") else {
            return;
        };
        records_lead.record(lead as f64);
    }

    /// Called before requesting fetches to update the set of per-partition
    /// metrics tracked to match the current assignment.
    ///
    /// `subscription` is the shared [`SubscriptionState`]; we accept it locked
    /// (the caller holds the bg-task lock). The preferred-read-replica gauge
    /// closure captures an `Arc<Mutex<SubscriptionState>>` so it can read the
    /// preferred replica lazily when the metric is measured.
    pub(crate) fn maybe_update_assignment(&self, subscription: &Arc<Mutex<SubscriptionState>>) {
        // Mirror Java's lazy ordering (`maybeUpdateAssignment`): read only the
        // cheap `assignmentId()` first, and acquire the (allocating)
        // `assignedPartitions()` set ONLY when the id has changed. A
        // steady-state poll with an unchanged assignment therefore does ZERO
        // `TopicPartition` clones — restoring the pre-M3 behavior on the
        // bg-task per-poll path (CLAUDE.md §11).
        let new_assignment_id = subscription.lock().expect("SubscriptionState mutex poisoned").assignment_id();

        // Hold the assignment-tracking guard for the whole update. This is
        // bg-task-only and never crosses an `.await`.
        let mut assignment = self.assignment.lock().expect("FetchMetricsManager assignment mutex poisoned");

        if assignment.assignment_id == new_assignment_id {
            return;
        }

        // The assignment changed: now (and only now) clone the assigned set.
        let new_assigned_partitions = subscription
            .lock()
            .expect("SubscriptionState mutex poisoned")
            .assigned_partitions();

        for tp in &assignment.assigned_partitions {
            if !new_assigned_partitions.contains(tp) {
                self.metrics.remove_sensor(&partition_records_lag_metric_name(tp));
                self.metrics.remove_sensor(&partition_records_lead_metric_name(tp));
                if let Some(metric_name) = self.partition_preferred_read_replica_metric_name(tp) {
                    self.metrics.remove_metric(&metric_name);
                }
                // Remove deprecated metrics.
                self.metrics
                    .remove_sensor(&deprecated_metric_name(&partition_records_lag_metric_name(tp)));
                self.metrics
                    .remove_sensor(&deprecated_metric_name(&partition_records_lead_metric_name(tp)));
                if let Some(metric_name) = self.deprecated_partition_preferred_read_replica_metric_name(tp) {
                    self.metrics.remove_metric(&metric_name);
                }
            }
        }

        for tp in &new_assigned_partitions {
            if !assignment.assigned_partitions.contains(tp) {
                self.maybe_record_deprecated_preferred_read_replica(tp, subscription);

                if let Some(metric_name) = self.partition_preferred_read_replica_metric_name(tp) {
                    let subscription = Arc::clone(subscription);
                    let tp_owned = tp.clone();
                    self.metrics.add_metric_if_absent(
                        metric_name,
                        None,
                        MetricValueProvider::Gauge(Box::new(ClosureGauge::new(move |_config, _now| {
                            let value = subscription
                                .lock()
                                .expect("SubscriptionState mutex poisoned")
                                .preferred_read_replica(&tp_owned, 0)
                                .unwrap_or(-1);
                            MetricValue::Int(value)
                        }))),
                    );
                }
            }
        }

        assignment.assigned_partitions = new_assigned_partitions;
        assignment.assignment_id = new_assignment_id;
    }

    // To be removed in Kafka 5.0 release.
    fn maybe_record_deprecated_bytes_fetched(&self, name: &str, topic: &str, bytes: i32) {
        if !should_report_deprecated_metric(topic) {
            return;
        }
        let deprecated = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(
                &self.metrics,
                &deprecated_metric_name(name),
                RecordingLevel::Info,
                topic_tags(topic),
            )?
            .with_avg(&self.metrics_registry.topic_fetch_size_avg)?
            .with_max(&self.metrics_registry.topic_fetch_size_max)?
            .with_meter(
                &self.metrics_registry.topic_bytes_consumed_rate,
                &self.metrics_registry.topic_bytes_consumed_total,
            )?
            .build())
        })();
        let Some(deprecated) = resolve_sensor(deprecated, "deprecated topic bytes-fetched sensor") else {
            return;
        };
        deprecated.record(bytes as f64);
    }

    // To be removed in Kafka 5.0 release.
    fn maybe_record_deprecated_records_fetched(&self, name: &str, topic: &str, records: i32) {
        if !should_report_deprecated_metric(topic) {
            return;
        }
        let deprecated = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(
                &self.metrics,
                &deprecated_metric_name(name),
                RecordingLevel::Info,
                topic_tags(topic),
            )?
            .with_avg(&self.metrics_registry.topic_records_per_request_avg)?
            .with_meter(
                &self.metrics_registry.topic_records_consumed_rate,
                &self.metrics_registry.topic_records_consumed_total,
            )?
            .build())
        })();
        let Some(deprecated) = resolve_sensor(deprecated, "deprecated topic records-fetched sensor") else {
            return;
        };
        deprecated.record(records as f64);
    }

    // To be removed in Kafka 5.0 release.
    fn maybe_record_deprecated_partition_lag(&self, name: &str, tp: &TopicPartition, lag: i64) {
        if !should_report_deprecated_metric(tp.topic()) {
            return;
        }
        let deprecated = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(
                &self.metrics,
                &deprecated_metric_name(name),
                RecordingLevel::Info,
                topic_partition_tags(tp),
            )?
            .with_value(&self.metrics_registry.partition_records_lag)?
            .with_max(&self.metrics_registry.partition_records_lag_max)?
            .with_avg(&self.metrics_registry.partition_records_lag_avg)?
            .build())
        })();
        let Some(deprecated) = resolve_sensor(deprecated, "deprecated partition records-lag sensor") else {
            return;
        };
        deprecated.record(lag as f64);
    }

    // To be removed in Kafka 5.0 release.
    fn maybe_record_deprecated_partition_lead(&self, name: &str, tp: &TopicPartition, lead: f64) {
        if !should_report_deprecated_metric(tp.topic()) {
            return;
        }
        let deprecated = (|| -> Result<Arc<Sensor>, KafkaError> {
            Ok(SensorBuilder::with_tags(
                &self.metrics,
                &deprecated_metric_name(name),
                RecordingLevel::Info,
                topic_partition_tags(tp),
            )?
            .with_value(&self.metrics_registry.partition_records_lead)?
            .with_min(&self.metrics_registry.partition_records_lead_min)?
            .with_avg(&self.metrics_registry.partition_records_lead_avg)?
            .build())
        })();
        let Some(deprecated) = resolve_sensor(deprecated, "deprecated partition records-lead sensor") else {
            return;
        };
        deprecated.record(lead);
    }

    // To be removed in Kafka 5.0 release.
    fn maybe_record_deprecated_preferred_read_replica(
        &self,
        tp: &TopicPartition,
        subscription: &Arc<Mutex<SubscriptionState>>,
    ) {
        if !should_report_deprecated_metric(tp.topic()) {
            return;
        }
        if let Some(metric_name) = self.deprecated_partition_preferred_read_replica_metric_name(tp) {
            let subscription = Arc::clone(subscription);
            let tp_owned = tp.clone();
            self.metrics.add_metric_if_absent(
                metric_name,
                None,
                MetricValueProvider::Gauge(Box::new(ClosureGauge::new(move |_config, _now| {
                    let value = subscription
                        .lock()
                        .expect("SubscriptionState mutex poisoned")
                        .preferred_read_replica(&tp_owned, 0)
                        .unwrap_or(-1);
                    MetricValue::Int(value)
                }))),
            );
        }
    }

    fn partition_preferred_read_replica_metric_name(&self, tp: &TopicPartition) -> Option<crate::common::MetricName> {
        let tags = topic_partition_tags_raw(tp);
        self.metrics
            .metric_instance_with_tags(&self.metrics_registry.partition_preferred_read_replica, tags)
            .ok()
    }

    fn deprecated_partition_preferred_read_replica_metric_name(
        &self,
        tp: &TopicPartition,
    ) -> Option<crate::common::MetricName> {
        let tags = topic_partition_tags(tp);
        self.metrics
            .metric_instance_with_tags(&self.metrics_registry.partition_preferred_read_replica, tags)
            .ok()
    }
}

fn topic_bytes_fetched_metric_name(topic: &str) -> String {
    format!("topic.{topic}.bytes-fetched")
}

fn topic_records_fetched_metric_name(topic: &str) -> String {
    format!("topic.{topic}.records-fetched")
}

fn partition_records_lead_metric_name(tp: &TopicPartition) -> String {
    format!("{tp}.records-lead")
}

fn partition_records_lag_metric_name(tp: &TopicPartition) -> String {
    format!("{tp}.records-lag")
}

fn deprecated_metric_name(name: &str) -> String {
    format!("{name}.deprecated")
}

fn should_report_deprecated_metric(topic: &str) -> bool {
    topic.contains('.')
}

/// Unwraps a lazily-built per-topic / per-partition sensor, logging and skipping
/// the recording instead of panicking when registration fails.
///
/// These builders run on the per-fetch record path, so a `.expect()` here turned
/// a recoverable registration error into a panic that killed the consumer task
/// (CLAUDE.md §10.1 — do not panic where recovery is possible).
///
/// The error is reachable, not theoretical. Two topics differing only by `.` vs
/// `_` — say `my.topic` and `my_topic` — produce *distinct* sensor names, so
/// `SensorBuilder::with_tags` does not find and reuse the first sensor; but the
/// deprecated variants replace periods in the tag value, so both resolve to the
/// same `MetricName` (`fetch-size-avg{client-id, topic=my_topic}`). The second
/// `Sensor::add` then returns `Err`.
///
/// Java throws `IllegalArgumentException` from `Metrics.registerMetric`
/// (`Metrics.java:506`) and lets it propagate out of the fetch path. We
/// deliberately do NOT propagate: these `record_*` methods are a side channel,
/// and failing a user's fetch because two topic names collide after period
/// replacement is worse than the metric being absent. The collision is reported
/// rather than hidden.
///
/// Note the log fires per record while the collision persists. That is intended:
/// it is a genuine misconfiguration, and the volume is the signal.
fn resolve_sensor(built: Result<Arc<Sensor>, KafkaError>, what: &str) -> Option<Arc<Sensor>> {
    match built {
        Ok(sensor) => Some(sensor),
        Err(err) => {
            log::warn!("Skipping {what} recording: sensor registration failed: {err}");
            None
        },
    }
}

fn single_tag(key: &str, value: &str) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert(key.to_string(), value.to_string());
    tags
}

/// `{topic, partition}` tags with the actual topic name (non-deprecated form).
fn topic_partition_tags_raw(tp: &TopicPartition) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert("topic".to_string(), tp.topic().to_string());
    tags.insert("partition".to_string(), tp.partition().to_string());
    tags
}

/// Deprecated topic tag: periods replaced with underscores
/// (Java `topicTags`).
pub(crate) fn topic_tags(topic: &str) -> BTreeMap<String, String> {
    single_tag("topic", &topic.replace('.', "_"))
}

/// Deprecated `{topic, partition}` tags: topic periods replaced with
/// underscores (Java `topicPartitionTags`).
pub(crate) fn topic_partition_tags(tp: &TopicPartition) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert("topic".to_string(), tp.topic().replace('.', "_"));
    tags.insert("partition".to_string(), tp.partition().to_string());
    tags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metric::Metric;
    use crate::common::metrics::MetricConfig;
    use crate::common::metrics::stats::{Avg, Max};
    use crate::common::metrics::time::mock::MockTime;
    use crate::common::{MetricName, MetricNameTemplate};
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use std::collections::HashSet;
    use std::sync::Arc;

    const EPSILON: f64 = 0.0001;
    const TOPIC_NAME: &str = "test";

    /// Mirrors `FetchMetricsManagerTest`'s `setup()`. The Java test uses
    /// `new MockTime(1, 0, 0)` (1ms auto-tick) + `new Metrics(time)`; the Rust
    /// MockTime is fixed (no auto-tick), which is value-equivalent for the
    /// avg/max/total assertions and keeps rate assertions `> 0` (elapsed =
    /// window+1 ms). Like Java, every test runs at the default INFO recording
    /// level — there is no DEBUG gating in the fetch metrics (full Java parity).
    struct Fixture {
        time: Arc<MockTime>,
        metrics: Arc<Metrics>,
        registry: FetchMetricsRegistry,
        manager: FetchMetricsManager,
    }

    fn setup() -> Fixture {
        let time = Arc::new(MockTime::new());
        let config = Arc::new(MetricConfig::new().with_record_level(RecordingLevel::Info));
        let metrics = Arc::new(Metrics::with_config_reporters_time(config, Vec::new(), time.clone() as Arc<_>));
        // Java: `new FetchMetricsRegistry(metrics.config().tags().keySet(), "test")`.
        // Default config has no tags, so the registry tag set is empty.
        let registry = FetchMetricsRegistry::new(indexmap::IndexSet::new(), "test");
        let manager = FetchMetricsManager::new(Arc::clone(&metrics), registry.clone());
        Fixture { time, metrics, registry, manager }
    }

    fn time_window_ms(f: &Fixture) -> i64 {
        f.metrics.config().time_window_ms()
    }

    fn metric_value_template(f: &Fixture, template: &MetricNameTemplate) -> f64 {
        let name = f.metrics.metric_instance(template, &[]).expect("metric instance");
        metric_value(f, &name)
    }

    fn metric_value_tags(f: &Fixture, template: &MetricNameTemplate, tags: &[&str]) -> f64 {
        let name = f.metrics.metric_instance(template, tags).expect("metric instance");
        metric_value(f, &name)
    }

    fn metric_value(f: &Fixture, name: &MetricName) -> f64 {
        let metric = f.metrics.metric(name).expect("metric registered");
        match metric.metric_value() {
            MetricValue::Double(v) => v,
            other => panic!("expected Double metric value, got {other:?}"),
        }
    }

    fn read_replica_metric_value(f: &Fixture, template: &MetricNameTemplate, tags: &[&str]) -> i32 {
        let name = f.metrics.metric_instance(template, tags).expect("metric instance");
        let metric = f.metrics.metric(&name).expect("metric registered");
        match metric.metric_value() {
            MetricValue::Int(v) => v,
            other => panic!("expected Int metric value, got {other:?}"),
        }
    }

    fn register_node_latency_metric(f: &Fixture, connection_id: &str, avg: &MetricName, max: &MetricName) {
        let node_time_name = format!("node-{connection_id}.latency");
        let node_request_time = f.metrics.sensor(&node_time_name).expect("sensor");
        node_request_time.add(avg.clone(), Box::new(Avg::new())).expect("add avg");
        node_request_time.add(max.clone(), Box::new(Max::new())).expect("add max");
    }

    /// `FetchMetricsManagerTest.testLatency`
    #[test]
    fn test_latency() {
        let f = setup();
        f.manager.record_latency("", 123);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_latency("", 456);

        assert!((metric_value_template(&f, &f.registry.fetch_latency_avg) - 289.5).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_latency_max) - 456.0).abs() < EPSILON);
    }

    /// `FetchMetricsManagerTest.testNodeLatency`
    #[test]
    fn test_node_latency() {
        let f = setup();
        let connection_id = "0";
        let node_latency_avg = f.metrics.metric_name_group("request-latency-avg", "group");
        let node_latency_max = f.metrics.metric_name_group("request-latency-max", "group");
        register_node_latency_metric(&f, connection_id, &node_latency_avg, &node_latency_max);

        f.manager.record_latency(connection_id, 123);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_latency(connection_id, 456);

        assert!((metric_value_template(&f, &f.registry.fetch_latency_avg) - 289.5).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_latency_max) - 456.0).abs() < EPSILON);

        assert!((metric_value(&f, &node_latency_avg) - 289.5).abs() < EPSILON);
        assert!((metric_value(&f, &node_latency_max) - 456.0).abs() < EPSILON);

        // Record metric against another node.
        f.manager.record_latency("1", 501);

        assert!((metric_value_template(&f, &f.registry.fetch_latency_avg) - 360.0).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_latency_max) - 501.0).abs() < EPSILON);
        // Node specific metric should not be affected.
        assert!((metric_value(&f, &node_latency_avg) - 289.5).abs() < EPSILON);
        assert!((metric_value(&f, &node_latency_max) - 456.0).abs() < EPSILON);
    }

    /// `FetchMetricsManagerTest.testBytesFetched`
    #[test]
    fn test_bytes_fetched() {
        let f = setup();
        f.manager.record_bytes_fetched(2);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_bytes_fetched(10);

        assert!((metric_value_template(&f, &f.registry.fetch_size_avg) - 6.0).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_size_max) - 10.0).abs() < EPSILON);
    }

    /// `FetchMetricsManagerTest.testBytesFetchedTopic`
    #[test]
    fn test_bytes_fetched_topic() {
        let f = setup();
        let topic_name1 = TOPIC_NAME;
        let topic_name2 = "another.topic";
        let tags1 = ["topic", topic_name1];
        let tags2 = ["topic", topic_name2];
        let deprecated = topic_tags(topic_name2);
        let deprecated_topic = deprecated.get("topic").unwrap().clone();
        let deprecated_tags = ["topic", deprecated_topic.as_str()];
        let initial = f.metrics.metrics().len();

        f.manager.record_bytes_fetched_topic(topic_name1, 2);
        // 4 new metrics shall be registered.
        assert_eq!(4, f.metrics.metrics().len() - initial);
        f.manager.record_bytes_fetched_topic(topic_name2, 1);
        // Another 8 metrics get registered as deprecated metrics should be reported for topicName2.
        assert_eq!(12, f.metrics.metrics().len() - initial);

        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_bytes_fetched_topic(topic_name1, 10);
        f.manager.record_bytes_fetched_topic(topic_name2, 5);

        // Subsequent calls should not register new metrics.
        assert_eq!(12, f.metrics.metrics().len() - initial);
        // Validate metrics for topicName1.
        assert!((metric_value_tags(&f, &f.registry.topic_fetch_size_avg, &tags1) - 6.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.topic_fetch_size_max, &tags1) - 10.0).abs() < EPSILON);
        assert!(metric_value_tags(&f, &f.registry.topic_bytes_consumed_rate, &tags1) > 0.0);
        assert!((metric_value_tags(&f, &f.registry.topic_bytes_consumed_total, &tags1) - 12.0).abs() < EPSILON);
        // Validate metrics for topicName2.
        assert!((metric_value_tags(&f, &f.registry.topic_fetch_size_avg, &tags2) - 3.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.topic_fetch_size_max, &tags2) - 5.0).abs() < EPSILON);
        assert!(metric_value_tags(&f, &f.registry.topic_bytes_consumed_rate, &tags2) > 0.0);
        assert!((metric_value_tags(&f, &f.registry.topic_bytes_consumed_total, &tags2) - 6.0).abs() < EPSILON);
        // Validate metrics for deprecated topic.
        assert!((metric_value_tags(&f, &f.registry.topic_fetch_size_avg, &deprecated_tags) - 3.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.topic_fetch_size_max, &deprecated_tags) - 5.0).abs() < EPSILON);
        assert!(metric_value_tags(&f, &f.registry.topic_bytes_consumed_rate, &deprecated_tags) > 0.0);
        assert!(
            (metric_value_tags(&f, &f.registry.topic_bytes_consumed_total, &deprecated_tags) - 6.0).abs() < EPSILON
        );
    }

    /// `FetchMetricsManagerTest.testRecordsFetched`
    #[test]
    fn test_records_fetched() {
        let f = setup();
        f.manager.record_records_fetched(3);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_records_fetched(15);

        assert!((metric_value_template(&f, &f.registry.records_per_request_avg) - 9.0).abs() < EPSILON);
    }

    /// `FetchMetricsManagerTest.testRecordsFetchedTopic`
    #[test]
    fn test_records_fetched_topic() {
        let f = setup();
        let topic_name1 = TOPIC_NAME;
        let topic_name2 = "another.topic";
        let tags1 = ["topic", topic_name1];
        let tags2 = ["topic", topic_name2];
        let deprecated = topic_tags(topic_name2);
        let deprecated_topic = deprecated.get("topic").unwrap().clone();
        let deprecated_tags = ["topic", deprecated_topic.as_str()];
        let initial = f.metrics.metrics().len();

        f.manager.record_records_fetched_topic(topic_name1, 2);
        // 3 new metrics shall be registered.
        assert_eq!(3, f.metrics.metrics().len() - initial);
        f.manager.record_records_fetched_topic(topic_name2, 1);
        // Another 6 metrics get registered as deprecated metrics should be reported for topicName2.
        assert_eq!(9, f.metrics.metrics().len() - initial);

        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_records_fetched_topic(topic_name1, 10);
        f.manager.record_records_fetched_topic(topic_name2, 5);

        // Subsequent calls should not register new metrics.
        assert_eq!(9, f.metrics.metrics().len() - initial);
        // Validate metrics for topicName1.
        assert!((metric_value_tags(&f, &f.registry.topic_records_per_request_avg, &tags1) - 6.0).abs() < EPSILON);
        assert!(metric_value_tags(&f, &f.registry.topic_records_consumed_rate, &tags1) > 0.0);
        assert!((metric_value_tags(&f, &f.registry.topic_records_consumed_total, &tags1) - 12.0).abs() < EPSILON);
        // Validate metrics for topicName2.
        assert!((metric_value_tags(&f, &f.registry.topic_records_per_request_avg, &tags2) - 3.0).abs() < EPSILON);
        assert!(metric_value_tags(&f, &f.registry.topic_records_consumed_rate, &tags2) > 0.0);
        assert!((metric_value_tags(&f, &f.registry.topic_records_consumed_total, &tags2) - 6.0).abs() < EPSILON);
        // Validate metrics for deprecated topic.
        assert!(
            (metric_value_tags(&f, &f.registry.topic_records_per_request_avg, &deprecated_tags) - 3.0).abs() < EPSILON
        );
        assert!(metric_value_tags(&f, &f.registry.topic_records_consumed_rate, &deprecated_tags) > 0.0);
        assert!(
            (metric_value_tags(&f, &f.registry.topic_records_consumed_total, &deprecated_tags) - 6.0).abs() < EPSILON
        );
    }

    /// `FetchMetricsManagerTest.testPartitionLag`. Runs at the default INFO
    /// level, exactly like the Java test: the per-partition lag sensors are INFO
    /// (full Java parity, no DEBUG gating — see `FetchMetricsManager` ctor doc).
    #[test]
    fn test_partition_lag() {
        let f = setup();
        let tp1 = TopicPartition::new(TOPIC_NAME, 0);
        let tp2 = TopicPartition::new("another.topic", 0);

        let p1 = tp1.partition().to_string();
        let p2 = tp2.partition().to_string();
        let tags1 = ["topic", tp1.topic(), "partition", p1.as_str()];
        let tags2 = ["topic", tp2.topic(), "partition", p2.as_str()];
        let deprecated = topic_partition_tags(&tp2);
        let dep_topic = deprecated.get("topic").unwrap().clone();
        let dep_part = deprecated.get("partition").unwrap().clone();
        let deprecated_tags = ["topic", dep_topic.as_str(), "partition", dep_part.as_str()];
        let initial = f.metrics.metrics().len();

        f.manager.record_partition_lag(&tp1, 14);
        // 3 new metrics shall be registered.
        assert_eq!(3, f.metrics.metrics().len() - initial);

        f.manager.record_partition_lag(&tp1, 8);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_partition_lag(&tp1, 5);

        // Subsequent calls should not register new metrics.
        assert_eq!(3, f.metrics.metrics().len() - initial);
        assert!((metric_value_template(&f, &f.registry.records_lag_max) - 14.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag, &tags1) - 5.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag_max, &tags1) - 14.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag_avg, &tags1) - 9.0).abs() < EPSILON);

        f.manager.record_partition_lag(&tp2, 7);
        // Another 6 metrics get registered as deprecated metrics should be reported for tp2.
        assert_eq!(9, f.metrics.metrics().len() - initial);
        f.manager.record_partition_lag(&tp2, 3);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_partition_lag(&tp2, 2);

        assert_eq!(9, f.metrics.metrics().len() - initial);
        assert!((metric_value_template(&f, &f.registry.records_lag_max) - 7.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag, &tags2) - 2.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag_max, &tags2) - 7.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag_avg, &tags2) - 4.0).abs() < EPSILON);
        // Validate metrics for deprecated topic.
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag, &deprecated_tags) - 2.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag_max, &deprecated_tags) - 7.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag_avg, &deprecated_tags) - 4.0).abs() < EPSILON);
    }

    /// `FetchMetricsManagerTest.testPartitionLead` (default INFO, see `test_partition_lag`).
    #[test]
    fn test_partition_lead() {
        let f = setup();
        let tp1 = TopicPartition::new(TOPIC_NAME, 0);
        let tp2 = TopicPartition::new("another.topic", 0);

        let p1 = tp1.partition().to_string();
        let p2 = tp2.partition().to_string();
        let tags1 = ["topic", tp1.topic(), "partition", p1.as_str()];
        let tags2 = ["topic", tp2.topic(), "partition", p2.as_str()];
        let deprecated = topic_partition_tags(&tp2);
        let dep_topic = deprecated.get("topic").unwrap().clone();
        let dep_part = deprecated.get("partition").unwrap().clone();
        let deprecated_tags = ["topic", dep_topic.as_str(), "partition", dep_part.as_str()];
        let initial = f.metrics.metrics().len();

        f.manager.record_partition_lead(&tp1, 15);
        // 3 new metrics shall be registered.
        assert_eq!(3, f.metrics.metrics().len() - initial);

        f.manager.record_partition_lead(&tp1, 11);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_partition_lead(&tp1, 13);

        assert_eq!(3, f.metrics.metrics().len() - initial);
        assert!((metric_value_template(&f, &f.registry.records_lead_min) - 11.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead, &tags1) - 13.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead_min, &tags1) - 11.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead_avg, &tags1) - 13.0).abs() < EPSILON);

        f.manager.record_partition_lead(&tp2, 18);
        // Another 6 metrics get registered as deprecated metrics should be reported for tp2.
        assert_eq!(9, f.metrics.metrics().len() - initial);

        f.manager.record_partition_lead(&tp2, 12);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_partition_lead(&tp2, 15);

        assert_eq!(9, f.metrics.metrics().len() - initial);
        assert!((metric_value_template(&f, &f.registry.records_lead_min) - 12.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead, &tags2) - 15.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead_min, &tags2) - 12.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead_avg, &tags2) - 15.0).abs() < EPSILON);
        // Validate metrics for deprecated topic.
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead, &deprecated_tags) - 15.0).abs() < EPSILON);
        assert!(
            (metric_value_tags(&f, &f.registry.partition_records_lead_min, &deprecated_tags) - 12.0).abs() < EPSILON
        );
        assert!(
            (metric_value_tags(&f, &f.registry.partition_records_lead_avg, &deprecated_tags) - 15.0).abs() < EPSILON
        );
    }

    /// `FetchMetricsManagerTest.testMaybeUpdateAssignment`
    #[test]
    fn test_maybe_update_assignment() {
        let f = setup();
        let tp1 = TopicPartition::new(TOPIC_NAME, 0);
        let tp2 = TopicPartition::new("another.topic", 0);
        let tp3 = TopicPartition::new("another.topic", 1);
        let initial = f.metrics.metrics().len();

        let subscription = Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::NONE)));
        subscription
            .lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp1.clone()]))
            .unwrap();

        f.manager.maybe_update_assignment(&subscription);
        // 1 new metric shall be registered.
        assert_eq!(1, f.metrics.metrics().len() - initial);

        subscription
            .lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp1.clone(), tp2.clone()]))
            .unwrap();
        subscription.lock().unwrap().update_preferred_read_replica(&tp2, 1, 0).unwrap();
        f.manager.maybe_update_assignment(&subscription);
        // Another 2 metrics get registered as deprecated metrics should be reported for tp2.
        assert_eq!(3, f.metrics.metrics().len() - initial);

        let p1 = tp1.partition().to_string();
        let p2 = tp2.partition().to_string();
        let tags1 = ["topic", tp1.topic(), "partition", p1.as_str()];
        let tags2 = ["topic", tp2.topic(), "partition", p2.as_str()];
        let deprecated = topic_partition_tags(&tp2);
        let dep_topic = deprecated.get("topic").unwrap().clone();
        let dep_part = deprecated.get("partition").unwrap().clone();
        let deprecated_tags = ["topic", dep_topic.as_str(), "partition", dep_part.as_str()];
        // Validate preferred read replica metrics.
        assert_eq!(
            -1,
            read_replica_metric_value(&f, &f.registry.partition_preferred_read_replica, &tags1)
        );
        assert_eq!(
            1,
            read_replica_metric_value(&f, &f.registry.partition_preferred_read_replica, &tags2)
        );
        assert_eq!(
            1,
            read_replica_metric_value(&f, &f.registry.partition_preferred_read_replica, &deprecated_tags)
        );

        // Remove tp2 from subscription set.
        subscription
            .lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp1.clone(), tp3.clone()]))
            .unwrap();
        f.manager.maybe_update_assignment(&subscription);
        // Metrics count shall remain same as tp2 should be removed and tp3 gets added.
        assert_eq!(3, f.metrics.metrics().len() - initial);

        // Remove all partitions.
        subscription.lock().unwrap().assign_from_user(HashSet::new()).unwrap();
        f.manager.maybe_update_assignment(&subscription);
        // Metrics count shall be same as initial count as all new metrics shall be removed.
        assert_eq!(initial, f.metrics.metrics().len());
    }

    /// `FetchMetricsManagerTest.testMaybeUpdateAssignmentWithAdditionalRegisteredMetrics`.
    /// Runs at DEBUG so the per-partition lag/lead sensors register.
    #[test]
    fn test_maybe_update_assignment_with_additional_registered_metrics() {
        let f = setup();
        let tp1 = TopicPartition::new(TOPIC_NAME, 0);
        let tp2 = TopicPartition::new("another.topic", 0);
        let tp3 = TopicPartition::new("another.topic", 1);

        let initial = f.metrics.metrics().len();

        f.manager.record_partition_lag(&tp1, 14);
        f.manager.record_partition_lead(&tp1, 11);
        f.manager.record_partition_lag(&tp2, 5);
        f.manager.record_partition_lead(&tp2, 1);
        f.manager.record_partition_lag(&tp3, 4);
        f.manager.record_partition_lead(&tp3, 2);

        let additional = f.metrics.metrics().len();

        let subscription = Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::NONE)));
        subscription
            .lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp1.clone(), tp2.clone(), tp3.clone()]))
            .unwrap();
        f.manager.maybe_update_assignment(&subscription);

        // 5 new metrics shall be registered.
        assert_eq!(5, f.metrics.metrics().len() - additional);

        // Remove 1 partition which has deprecated metrics as well.
        subscription
            .lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp1.clone(), tp2.clone()]))
            .unwrap();
        f.manager.maybe_update_assignment(&subscription);
        // For tp2, 14 metrics will be unregistered; we should have 9 removed from `additional`.
        assert_eq!(9, additional - f.metrics.metrics().len());

        // Remove all partitions.
        subscription.lock().unwrap().assign_from_user(HashSet::new()).unwrap();
        f.manager.maybe_update_assignment(&subscription);
        assert_eq!(initial, f.metrics.metrics().len());
    }

    /// Full Java parity: at the default INFO level both the client-level
    /// `records-lag-max` / `records-lead-min` AND the DETAILED per-partition
    /// lag/lead sensors are recorded — there is NO DEBUG gating (Java's
    /// `SensorBuilder` defaults every fetch sensor to `RecordingLevel.INFO`,
    /// including the per-partition detail at `FetchMetricsManager.java:133,148`).
    #[test]
    fn test_partition_metrics_recording_level() {
        let f = setup();

        let tp = TopicPartition::new(TOPIC_NAME, 0);
        let initial = f.metrics.metrics().len();
        f.manager.record_partition_lag(&tp, 14);
        f.manager.record_partition_lead(&tp, 11);

        // (a) client-level records-lag-max / records-lead-min ARE recorded at
        // INFO (Java-faithful: a default consumer exposes consumer-lag).
        assert!((metric_value_template(&f, &f.registry.records_lag_max) - 14.0).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.records_lead_min) - 11.0).abs() < EPSILON);

        // (b) the DETAILED per-partition sensors ALSO register at INFO (full
        // Java parity, no DEBUG gating): lag (value/max/avg) + lead
        // (value/min/avg) = 6 new metrics for this non-deprecated topic.
        assert_eq!(6, f.metrics.metrics().len() - initial);

        let p = tp.partition().to_string();
        let tags = ["topic", tp.topic(), "partition", p.as_str()];
        assert!((metric_value_tags(&f, &f.registry.partition_records_lag, &tags) - 14.0).abs() < EPSILON);
        assert!((metric_value_tags(&f, &f.registry.partition_records_lead, &tags) - 11.0).abs() < EPSILON);
    }

    /// Regression: two topic names that differ only by `.` vs `_` must not panic
    /// the record path.
    ///
    /// `my.topic` and `my_topic` produce distinct sensor names, so the second
    /// call does not reuse the first sensor — but the deprecated variants replace
    /// periods in the tag value, so both resolve to the same `MetricName` and the
    /// second `Sensor::add` returns `Err`. That used to hit a `.expect()` on the
    /// per-fetch path and kill the consumer task; it now logs and skips
    /// (see `resolve_sensor`).
    ///
    /// Note the order: the dotted topic must be recorded first, because only it
    /// registers a deprecated sensor (`should_report_deprecated_metric` keys off
    /// the period) and so claims the period-replaced `MetricName`.
    #[test]
    fn test_period_vs_underscore_topic_collision_does_not_panic() {
        let f = setup();

        f.manager.record_bytes_fetched_topic("my.topic", 100);
        f.manager.record_records_fetched_topic("my.topic", 5);

        // Reaching these at all is the assertion: before the fix the collision
        // panicked here rather than returning.
        f.manager.record_bytes_fetched_topic("my_topic", 200);
        f.manager.record_records_fetched_topic("my_topic", 7);

        // The collision is wider than just the deprecated pair: `my_topic`'s
        // *non-deprecated* tags are `single_tag("topic", "my_topic")`, which is
        // exactly what `topic_tags("my.topic")` produces after period
        // replacement. So `my_topic` loses its metric entirely and the first
        // registration — the dotted topic's deprecated sensor — keeps the name.
        //
        // Asserting the retained value documents that cost precisely: 100 from
        // `my.topic`, not 200 from `my_topic`.
        let tags = ["topic", "my_topic"];
        assert!(
            (metric_value_tags(&f, &f.registry.topic_fetch_size_avg, &tags) - 100.0).abs() < EPSILON,
            "the first registration (my.topic's deprecated sensor) should own the collided MetricName"
        );
    }

    /// Exercises the throttle-time sensor (registered + recordable). Java has no
    /// dedicated throttle test in `FetchMetricsManagerTest`; this asserts the
    /// sensor and its avg/max metrics exist and record.
    #[test]
    fn test_throttle_time_sensor_records() {
        let f = setup();
        let sensor = f.manager.throttle_time_sensor();
        sensor.record(100.0);
        f.time.sleep(time_window_ms(&f) + 1);
        sensor.record(200.0);

        assert!((metric_value_template(&f, &f.registry.fetch_throttle_time_avg) - 150.0).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_throttle_time_max) - 200.0).abs() < EPSILON);
    }
}
