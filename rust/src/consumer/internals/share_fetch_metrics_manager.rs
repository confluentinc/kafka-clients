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

//! Records the share consumer's fetch and acknowledgement metrics
//! (`org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager`).

use std::sync::Arc;

use crate::common::Error;
use crate::common::metrics::sensor::record_or_log;
use crate::common::metrics::stats::WindowedCount;
use crate::common::metrics::{Metrics, Sensor};
use crate::consumer::internals::ShareFetchMetricsRegistry;
use crate::consumer::internals::metrics::{AbstractConsumerMetricsManager, MetricsLedger, SensorBuilder};

/// Records the share consumer's fetch latency, throttle time, bytes and
/// records fetched, and acknowledgements sent and failed.
///
/// Java's `ShareFetchMetricsManager extends AbstractConsumerMetricsManager`;
/// Rust composes [`AbstractConsumerMetricsManager`], as
/// [`super::FetchMetricsManager`] does. It is shared (`Arc`) between the share
/// consume request manager (bg task) and the per-response
/// [`super::ShareFetchMetricsAggregator`]; every sensor records through `&self`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager")]
pub(crate) struct ShareFetchMetricsManager {
    inner: AbstractConsumerMetricsManager,
    throttle_time: Arc<Sensor>,
    bytes_fetched: Arc<Sensor>,
    records_fetched: Arc<Sensor>,
    fetch_latency: Arc<Sensor>,
    sent_acknowledgements: Arc<Sensor>,
    failed_acknowledgements: Arc<Sensor>,
}

impl ShareFetchMetricsManager {
    /// Builds the manager and registers its six sensors, all at INFO (Java's
    /// `SensorBuilder` → `metrics.sensor(name)` default).
    ///
    /// Translates Java's public `ShareFetchMetricsManager(Metrics,
    /// ShareFetchMetricsRegistry)`, which delegates to the private
    /// `ShareFetchMetricsManager(MetricsLedger, ShareFetchMetricsRegistry)`,
    /// folded in here: every sensor goes through the ledger, so `close()`
    /// removes them all (KAFKA-19542).
    ///
    /// # Panics
    ///
    /// When a sensor's metric is already registered under another sensor of
    /// the same `Metrics` — a construction-time programming error, which Java
    /// reports with an `IllegalArgumentException` from the constructor too.
    /// The precedent is `FetchMetricsManager::new`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#ShareFetchMetricsManager")]
    pub(crate) fn new(metrics: Arc<Metrics>, metrics_registry: &ShareFetchMetricsRegistry) -> Self {
        let inner = AbstractConsumerMetricsManager::new(MetricsLedger::new(metrics));
        let metrics = inner.metrics();
        let build_bytes = || -> Result<Arc<Sensor>, Error> {
            Ok(SensorBuilder::new(metrics, "bytes-fetched")?
                .with_avg(&metrics_registry.fetch_size_avg)?
                .with_max(&metrics_registry.fetch_size_max)?
                .with_meter(&metrics_registry.bytes_fetched_rate, &metrics_registry.bytes_fetched_total)?
                .build())
        };
        let build_records = || -> Result<Arc<Sensor>, Error> {
            Ok(SensorBuilder::new(metrics, "records-fetched")?
                .with_avg(&metrics_registry.records_per_request_avg)?
                .with_max(&metrics_registry.records_per_request_max)?
                .with_meter(&metrics_registry.records_fetched_rate, &metrics_registry.records_fetched_total)?
                .build())
        };
        let build_sent = || -> Result<Arc<Sensor>, Error> {
            Ok(SensorBuilder::new(metrics, "sent-acknowledgements")?
                .with_meter(
                    &metrics_registry.acknowledgement_send_rate,
                    &metrics_registry.acknowledgement_send_total,
                )?
                .build())
        };
        let build_failed = || -> Result<Arc<Sensor>, Error> {
            Ok(SensorBuilder::new(metrics, "failed-acknowledgements")?
                .with_meter(
                    &metrics_registry.acknowledgement_error_rate,
                    &metrics_registry.acknowledgement_error_total,
                )?
                .build())
        };
        let build_latency = || -> Result<Arc<Sensor>, Error> {
            Ok(SensorBuilder::new(metrics, "fetch-latency")?
                .with_avg(&metrics_registry.fetch_latency_avg)?
                .with_max(&metrics_registry.fetch_latency_max)?
                .with_meter_with_sampled_stat(
                    WindowedCount::new().into_sampled_stat(),
                    &metrics_registry.fetch_request_rate,
                    &metrics_registry.fetch_request_total,
                )?
                .build())
        };
        let build_throttle = || -> Result<Arc<Sensor>, Error> {
            Ok(SensorBuilder::new(metrics, "fetch-throttle-time")?
                .with_avg(&metrics_registry.fetch_throttle_time_avg)?
                .with_max(&metrics_registry.fetch_throttle_time_max)?
                .build())
        };

        // Java's field-initialisation order.
        let bytes_fetched = build_bytes().expect("registering bytes-fetched sensor");
        let records_fetched = build_records().expect("registering records-fetched sensor");
        let sent_acknowledgements = build_sent().expect("registering sent-acknowledgements sensor");
        let failed_acknowledgements = build_failed().expect("registering failed-acknowledgements sensor");
        let fetch_latency = build_latency().expect("registering fetch-latency sensor");
        let throttle_time = build_throttle().expect("registering fetch-throttle-time sensor");

        Self {
            inner,
            throttle_time,
            bytes_fetched,
            records_fetched,
            fetch_latency,
            sent_acknowledgements,
            failed_acknowledgements,
        }
    }

    /// The sensor recording fetch throttle time, for the network client to
    /// record the broker's `throttle_time_ms` into (Java `throttleTimeSensor()`,
    /// which `ShareConsumerImpl` passes to the network client).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#throttleTimeSensor")]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "ShareConsumerImpl uses it from Milestone 18 P7 on")
    )]
    pub(crate) fn throttle_time_sensor(&self) -> Arc<Sensor> {
        Arc::clone(&self.throttle_time)
    }

    /// Records the latency of a share fetch request against the client-level
    /// sensor and, if registered, the `node-<node>.latency` sensor.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#recordLatency")]
    pub(crate) fn record_latency(&self, node: &str, request_latency_ms: i64) {
        record_or_log(self.fetch_latency.record_with_value(request_latency_ms as f64));
        if !node.is_empty() {
            let node_time_name = format!("node-{node}.latency");
            if let Some(node_request_time) = self.inner.metrics().get_sensor(&node_time_name) {
                record_or_log(node_request_time.record_with_value(request_latency_ms as f64));
            }
        }
    }

    /// Records the number of bytes fetched by one share fetch response.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#recordBytesFetched")]
    pub(crate) fn record_bytes_fetched(&self, bytes: i32) {
        record_or_log(self.bytes_fetched.record_with_value(f64::from(bytes)));
    }

    /// Records the number of records fetched by one share fetch response.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#recordRecordsFetched")]
    pub(crate) fn record_records_fetched(&self, records: i32) {
        record_or_log(self.records_fetched.record_with_value(f64::from(records)));
    }

    /// Records the number of record acknowledgements sent.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#recordAcknowledgementSent")]
    pub(crate) fn record_acknowledgement_sent(&self, acknowledgements: i32) {
        record_or_log(self.sent_acknowledgements.record_with_value(f64::from(acknowledgements)));
    }

    /// Records the number of record acknowledgements that failed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManager#recordFailedAcknowledgements")]
    pub(crate) fn record_failed_acknowledgements(&self, acknowledgements: i32) {
        record_or_log(self.failed_acknowledgements.record_with_value(f64::from(acknowledgements)));
    }

    /// Removes every sensor and metric this manager registered. Java inherits
    /// `AbstractConsumerMetricsManager.close()` (KAFKA-19542).
    pub(crate) fn close(&self) {
        self.inner.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::stats::{Avg, Max};
    use crate::common::utils::MockTime;
    use crate::common::{Metric, MetricName, MetricNameTemplate, MetricValue};
    use crate::consumer::internals::ConsumerUtils;

    const EPSILON: f64 = 0.0001;

    /// `ShareFetchMetricsManagerTest`'s fields and `setup()`. Java uses
    /// `new MockTime(1, 0, 0)` (a 1 ms auto-tick on every read); the Rust
    /// `MockTime` does not auto-tick. The tests only read avg / max / total,
    /// which the tick does not change, and the elapsed window is still
    /// `timeWindowMs + 1` (the `FetchMetricsManager` test precedent). Java's
    /// `@AfterEach tearDown` closes `metrics`; dropping the fixture does that.
    struct Fixture {
        time: Arc<MockTime>,
        metrics: Arc<Metrics>,
        registry: ShareFetchMetricsRegistry,
        manager: ShareFetchMetricsManager,
    }

    fn setup() -> Fixture {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(time.clone() as Arc<_>));
        let registry =
            ShareFetchMetricsRegistry::with_metric_grp_prefix(ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX);
        let manager = ShareFetchMetricsManager::new(Arc::clone(&metrics), &registry);
        Fixture { time, metrics, registry, manager }
    }

    fn time_window_ms(f: &Fixture) -> i64 {
        f.metrics.config().time_window_ms()
    }

    /// Java's `metricValue(MetricNameTemplate)` (and `getMetric(..).metricValue()`).
    fn metric_value_template(f: &Fixture, template: &MetricNameTemplate) -> f64 {
        let name = f
            .metrics
            .metric_instance_with_key_value(template, &[])
            .expect("metric instance");
        metric_value(f, &name)
    }

    /// Java's `metricValue(MetricName)`.
    fn metric_value(f: &Fixture, name: &MetricName) -> f64 {
        let metric = f.metrics.metric(name).expect("metric registered");
        match metric.metric_value() {
            MetricValue::Double(v) => v,
            other => panic!("expected Double metric value, got {other:?}"),
        }
    }

    /// Java's `registerNodeLatencyMetric`.
    fn register_node_latency_metric(f: &Fixture, connection_id: &str, avg: &MetricName, max: &MetricName) {
        let node_time_name = format!("node-{connection_id}.latency");
        let node_request_time = f.metrics.sensor(&node_time_name).expect("sensor");
        node_request_time
            .add_with_metric_name(avg.clone(), Box::new(Avg::new()))
            .expect("add avg");
        node_request_time
            .add_with_metric_name(max.clone(), Box::new(Max::new()))
            .expect("add max");
    }

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManagerTest#testLatency")]
    fn test_latency() {
        let f = setup();
        f.manager.record_latency("", 101);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_latency("", 155);

        assert!((metric_value_template(&f, &f.registry.fetch_latency_max) - 155.0).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_latency_avg) - 128.0).abs() < EPSILON);
    }

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManagerTest#testNodeLatency")]
    fn test_node_latency() {
        let f = setup();
        let connection_id = "0";
        let node_latency_avg = f.metrics.metric_name("request-latency-avg", "group");
        let node_latency_max = f.metrics.metric_name("request-latency-max", "group");
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

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManagerTest#testBytesFetched")]
    fn test_bytes_fetched() {
        let f = setup();
        f.manager.record_bytes_fetched(2);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_bytes_fetched(10);

        // Java asserts the max without a tolerance.
        assert_eq!(10.0, metric_value_template(&f, &f.registry.fetch_size_max));
        assert!((metric_value_template(&f, &f.registry.fetch_size_avg) - 6.0).abs() < EPSILON);
    }

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManagerTest#testRecordsFetched")]
    fn test_records_fetched() {
        let f = setup();
        f.manager.record_records_fetched(7);
        f.time.sleep(time_window_ms(&f) + 1);
        f.manager.record_records_fetched(9);

        assert_eq!(9.0, metric_value_template(&f, &f.registry.records_per_request_max));
        assert!((metric_value_template(&f, &f.registry.records_per_request_avg) - 8.0).abs() < EPSILON);
    }

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManagerTest#testAcknowledgements")]
    fn test_acknowledgements() {
        let f = setup();
        f.manager.record_acknowledgement_sent(5);
        f.manager.record_failed_acknowledgements(2);

        assert_eq!(5.0, metric_value_template(&f, &f.registry.acknowledgement_send_total));
        assert_eq!(2.0, metric_value_template(&f, &f.registry.acknowledgement_error_total));
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsManagerTest#testCloseRemovesAllSensors"
    )]
    fn test_close_removes_all_sensors() {
        let f = setup();
        // Define all sensor names that should be created and removed
        let sensor_names = [
            "fetch-throttle-time",
            "bytes-fetched",
            "records-fetched",
            "fetch-latency",
            "sent-acknowledgements",
            "failed-acknowledgements",
        ];

        // Verify that sensors exist before closing
        for sensor_name in sensor_names {
            assert!(
                f.metrics.get_sensor(sensor_name).is_some(),
                "Sensor {sensor_name} should exist before closing"
            );
        }

        // Close the metrics manager
        f.manager.close();

        // Verify that all sensors are removed
        for sensor_name in sensor_names {
            assert!(
                f.metrics.get_sensor(sensor_name).is_none(),
                "Sensor {sensor_name} should be removed after closing"
            );
        }
    }

    /// The throttle-time sensor handed to the network client is the manager's
    /// own, recording into `fetch-throttle-time-avg` / `-max`. Rust-only: Java's
    /// test class does not call `throttleTimeSensor()`.
    #[test]
    fn test_throttle_time_sensor_records() {
        let f = setup();
        let sensor = f.manager.throttle_time_sensor();
        record_or_log(sensor.record_with_value(20.0));
        record_or_log(sensor.record_with_value(40.0));
        assert!((metric_value_template(&f, &f.registry.fetch_throttle_time_avg) - 30.0).abs() < EPSILON);
        assert!((metric_value_template(&f, &f.registry.fetch_throttle_time_max) - 40.0).abs() < EPSILON);
    }
}
