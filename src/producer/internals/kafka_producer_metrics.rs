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

//! Producer-level latency timing metrics
//! (`org.apache.kafka.clients.producer.internals.KafkaProducerMetrics`).

// All 8 latency sensors are registered, exactly as Java's `KafkaProducerMetrics`
// registers them regardless of use. Seven are recorded from `KafkaProducer`:
// `record_flush` and `record_metadata_wait` on the flush / metadata-wait paths,
// and `record_init` / `record_begin_txn` / `record_send_offsets` /
// `record_commit_txn` / `record_abort_txn` from the transaction-control methods.
// Only `record_prepare_txn` has no call site — Java registers `txn-prepare` but
// never calls `recordPrepareTxn` either — so its `dead_code` allowance is narrowed
// to that single method (below) rather than blanketing the file.

use std::sync::Arc;

use crate::common::MetricName;
use crate::common::metrics::stats::CumulativeSum;
use crate::common::metrics::{Metrics, Sensor};

/// The metric group name for producer-level latency metrics.
///
/// Mirrors Java's `KafkaProducerMetrics.GROUP`.
pub(crate) const GROUP: &str = "producer-metrics";

const FLUSH: &str = "flush";
const TXN_INIT: &str = "txn-init";
const TXN_BEGIN: &str = "txn-begin";
const TXN_SEND_OFFSETS: &str = "txn-send-offsets";
const TXN_COMMIT: &str = "txn-commit";
const TXN_ABORT: &str = "txn-abort";
const TXN_PREPARE: &str = "txn-prepare";
const TOTAL_TIME_SUFFIX: &str = "-time-ns-total";
const METADATA_WAIT: &str = "metadata-wait";

/// Records producer latency timing metrics. Mirrors Java's
/// `KafkaProducerMetrics implements AutoCloseable`.
///
/// Owned by the [`KafkaProducer`](crate::producer::KafkaProducer). Each
/// `record_*` method is invoked per top-level producer API call (flush,
/// metadata-wait, and the txn lifecycle methods), never per record. Every one of
/// the 8 latency sensors carries a single [`CumulativeSum`] metric named
/// `<x>-time-ns-total`, exactly as Java registers them.
pub(crate) struct KafkaProducerMetrics {
    metrics: Arc<Metrics>,
    init_time_sensor: Arc<Sensor>,
    begin_txn_time_sensor: Arc<Sensor>,
    flush_time_sensor: Arc<Sensor>,
    send_offsets_sensor: Arc<Sensor>,
    commit_txn_sensor: Arc<Sensor>,
    abort_txn_sensor: Arc<Sensor>,
    prepare_txn_sensor: Arc<Sensor>,
    metadata_wait_sensor: Arc<Sensor>,
}

impl KafkaProducerMetrics {
    /// Registers all 8 latency sensors in the `producer-metrics` group.
    ///
    /// Java: `KafkaProducerMetrics(Metrics metrics)`.
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        let flush_time_sensor =
            Self::new_latency_sensor(&metrics, FLUSH, "Total time producer has spent in flush in nanoseconds.");
        let init_time_sensor = Self::new_latency_sensor(
            &metrics,
            TXN_INIT,
            "Total time producer has spent in initTransactions in nanoseconds.",
        );
        let begin_txn_time_sensor = Self::new_latency_sensor(
            &metrics,
            TXN_BEGIN,
            "Total time producer has spent in beginTransaction in nanoseconds.",
        );
        let send_offsets_sensor = Self::new_latency_sensor(
            &metrics,
            TXN_SEND_OFFSETS,
            "Total time producer has spent in sendOffsetsToTransaction in nanoseconds.",
        );
        let commit_txn_sensor = Self::new_latency_sensor(
            &metrics,
            TXN_COMMIT,
            "Total time producer has spent in commitTransaction in nanoseconds.",
        );
        let abort_txn_sensor = Self::new_latency_sensor(
            &metrics,
            TXN_ABORT,
            "Total time producer has spent in abortTransaction in nanoseconds.",
        );
        let prepare_txn_sensor = Self::new_latency_sensor(
            &metrics,
            TXN_PREPARE,
            "Total time producer has spent in prepareTransaction in nanoseconds.",
        );
        let metadata_wait_sensor = Self::new_latency_sensor(
            &metrics,
            METADATA_WAIT,
            "Total time producer has spent waiting on topic metadata in nanoseconds.",
        );

        Self {
            metrics,
            init_time_sensor,
            begin_txn_time_sensor,
            flush_time_sensor,
            send_offsets_sensor,
            commit_txn_sensor,
            abort_txn_sensor,
            prepare_txn_sensor,
            metadata_wait_sensor,
        }
    }

    /// Java: `recordFlush(long duration)`.
    pub(crate) fn record_flush(&self, duration: i64) {
        self.flush_time_sensor.record(duration as f64);
    }

    /// Java: `recordInit(long duration)`.
    ///
    /// Recorded by [`KafkaProducer::init_transactions`](crate::producer::KafkaProducer::init_transactions).
    pub(crate) fn record_init(&self, duration: i64) {
        self.init_time_sensor.record(duration as f64);
    }

    /// Java: `recordBeginTxn(long duration)`.
    ///
    /// Recorded by [`KafkaProducer::begin_transaction`](crate::producer::KafkaProducer::begin_transaction).
    pub(crate) fn record_begin_txn(&self, duration: i64) {
        self.begin_txn_time_sensor.record(duration as f64);
    }

    /// Java: `recordSendOffsets(long duration)`.
    ///
    /// Recorded by [`KafkaProducer::send_offsets_to_transaction`](crate::producer::KafkaProducer::send_offsets_to_transaction).
    pub(crate) fn record_send_offsets(&self, duration: i64) {
        self.send_offsets_sensor.record(duration as f64);
    }

    /// Java: `recordCommitTxn(long duration)`.
    ///
    /// Recorded by [`KafkaProducer::commit_transaction`](crate::producer::KafkaProducer::commit_transaction).
    pub(crate) fn record_commit_txn(&self, duration: i64) {
        self.commit_txn_sensor.record(duration as f64);
    }

    /// Java: `recordAbortTxn(long duration)`.
    ///
    /// Recorded by [`KafkaProducer::abort_transaction`](crate::producer::KafkaProducer::abort_transaction).
    pub(crate) fn record_abort_txn(&self, duration: i64) {
        self.abort_txn_sensor.record(duration as f64);
    }

    /// Java: `recordPrepareTxn(long duration)`.
    ///
    /// Deliberately never recorded, mirroring Java: `KafkaProducerMetrics`
    /// registers the `txn-prepare` sensor but has no `recordPrepareTxn` call site
    /// of its own, so the sensor is registered for parity and stays at zero. The
    /// `dead_code` allowance is scoped to this one method.
    #[allow(dead_code)]
    pub(crate) fn record_prepare_txn(&self, duration: i64) {
        self.prepare_txn_sensor.record(duration as f64);
    }

    /// Java: `recordMetadataWait(long duration)`.
    pub(crate) fn record_metadata_wait(&self, duration: i64) {
        self.metadata_wait_sensor.record(duration as f64);
    }

    /// Java: `close()` (`AutoCloseable`). Removes all 8 latency sensors.
    pub(crate) fn close(&self) {
        Self::remove_metric(&self.metrics, FLUSH);
        Self::remove_metric(&self.metrics, TXN_INIT);
        Self::remove_metric(&self.metrics, TXN_BEGIN);
        Self::remove_metric(&self.metrics, TXN_SEND_OFFSETS);
        Self::remove_metric(&self.metrics, TXN_COMMIT);
        Self::remove_metric(&self.metrics, TXN_ABORT);
        Self::remove_metric(&self.metrics, TXN_PREPARE);
        Self::remove_metric(&self.metrics, METADATA_WAIT);
    }

    /// Java: `newLatencySensor(String name, String description)`.
    ///
    /// The sensor is named `<name>-time-ns-total` and carries a single
    /// `CumulativeSum` metric of the same name.
    fn new_latency_sensor(metrics: &Arc<Metrics>, name: &str, description: &str) -> Arc<Sensor> {
        let sensor_name = format!("{name}{TOTAL_TIME_SUFFIX}");
        let sensor = metrics
            .sensor(&sensor_name)
            .unwrap_or_else(|_| panic!("creating {sensor_name} sensor"));
        sensor
            .add(Self::metric_name(metrics, name, description), Box::new(CumulativeSum::new()))
            .unwrap_or_else(|_| panic!("adding {sensor_name} metric"));
        sensor
    }

    /// Java: `metricName(String name, String description)`.
    ///
    /// `Metrics::metric_name` already merges the registry's default tags (the
    /// `client-id` tag), so an empty explicit tag map yields the same effective
    /// name as Java passing `metrics.config().tags()`.
    fn metric_name(metrics: &Arc<Metrics>, name: &str, description: &str) -> MetricName {
        metrics.metric_name(
            format!("{name}{TOTAL_TIME_SUFFIX}"),
            GROUP,
            description,
            std::collections::BTreeMap::new(),
        )
    }

    /// Java: `removeMetric(String name)`.
    fn remove_metric(metrics: &Arc<Metrics>, name: &str) {
        metrics.remove_sensor(&format!("{name}{TOTAL_TIME_SUFFIX}"));
    }
}

#[cfg(test)]
mod tests {
    //! `KafkaProducerMetricsTest` (Java) is fully translated here.

    use super::*;
    use crate::common::metric::Metric;

    const METRIC_VALUE: i64 = 123;
    const FLUSH_TIME_TOTAL: &str = "flush-time-ns-total";
    const TXN_INIT_TIME_TOTAL: &str = "txn-init-time-ns-total";
    const TXN_BEGIN_TIME_TOTAL: &str = "txn-begin-time-ns-total";
    const TXN_COMMIT_TIME_TOTAL: &str = "txn-commit-time-ns-total";
    const TXN_ABORT_TIME_TOTAL: &str = "txn-abort-time-ns-total";
    const TXN_SEND_OFFSETS_TIME_TOTAL: &str = "txn-send-offsets-time-ns-total";
    const METADATA_WAIT_TIME_TOTAL: &str = "metadata-wait-time-ns-total";

    struct Fixture {
        metrics: Arc<Metrics>,
        producer_metrics: KafkaProducerMetrics,
    }

    fn setup() -> Fixture {
        let metrics = Arc::new(Metrics::new());
        let producer_metrics = KafkaProducerMetrics::new(Arc::clone(&metrics));
        Fixture { metrics, producer_metrics }
    }

    fn assert_metric_value(metrics: &Metrics, name: &str) {
        let mn = metrics.metric_name_group(name, GROUP);
        let metric = metrics.metric(&mn).expect("metric present");
        assert_eq!(metric.metric_value().as_double(), Some(METRIC_VALUE as f64));
    }

    fn assert_metric_removed(metrics: &Metrics, name: &str) {
        let mn = metrics.metric_name_group(name, GROUP);
        assert!(metrics.metric(&mn).is_none());
    }

    /// Java: `shouldRecordFlushTime`.
    #[test]
    fn should_record_flush_time() {
        let f = setup();
        f.producer_metrics.record_flush(METRIC_VALUE);
        assert_metric_value(&f.metrics, FLUSH_TIME_TOTAL);
    }

    /// Java: `shouldRecordInitTime`.
    #[test]
    fn should_record_init_time() {
        let f = setup();
        f.producer_metrics.record_init(METRIC_VALUE);
        assert_metric_value(&f.metrics, TXN_INIT_TIME_TOTAL);
    }

    /// Java: `shouldRecordTxBeginTime`.
    #[test]
    fn should_record_tx_begin_time() {
        let f = setup();
        f.producer_metrics.record_begin_txn(METRIC_VALUE);
        assert_metric_value(&f.metrics, TXN_BEGIN_TIME_TOTAL);
    }

    /// Java: `shouldRecordTxCommitTime`.
    #[test]
    fn should_record_tx_commit_time() {
        let f = setup();
        f.producer_metrics.record_commit_txn(METRIC_VALUE);
        assert_metric_value(&f.metrics, TXN_COMMIT_TIME_TOTAL);
    }

    /// Java: `shouldRecordTxAbortTime`.
    #[test]
    fn should_record_tx_abort_time() {
        let f = setup();
        f.producer_metrics.record_abort_txn(METRIC_VALUE);
        assert_metric_value(&f.metrics, TXN_ABORT_TIME_TOTAL);
    }

    /// Java: `shouldRecordSendOffsetsTime`.
    #[test]
    fn should_record_send_offsets_time() {
        let f = setup();
        f.producer_metrics.record_send_offsets(METRIC_VALUE);
        assert_metric_value(&f.metrics, TXN_SEND_OFFSETS_TIME_TOTAL);
    }

    /// Java: `shouldRecordMetadataWaitTime`.
    #[test]
    fn should_record_metadata_wait_time() {
        let f = setup();
        f.producer_metrics.record_metadata_wait(METRIC_VALUE);
        assert_metric_value(&f.metrics, METADATA_WAIT_TIME_TOTAL);
    }

    /// Java: `shouldRemoveMetricsOnClose`.
    #[test]
    fn should_remove_metrics_on_close() {
        let f = setup();
        f.producer_metrics.close();
        assert_metric_removed(&f.metrics, FLUSH_TIME_TOTAL);
        assert_metric_removed(&f.metrics, TXN_INIT_TIME_TOTAL);
        assert_metric_removed(&f.metrics, TXN_BEGIN_TIME_TOTAL);
        assert_metric_removed(&f.metrics, TXN_COMMIT_TIME_TOTAL);
        assert_metric_removed(&f.metrics, TXN_ABORT_TIME_TOTAL);
        assert_metric_removed(&f.metrics, TXN_SEND_OFFSETS_TIME_TOTAL);
        assert_metric_removed(&f.metrics, METADATA_WAIT_TIME_TOTAL);
    }
}
