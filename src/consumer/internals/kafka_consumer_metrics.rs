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

//! Consumer-level poll/commit timing metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics`).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::common::MetricName;
use crate::common::metrics::stats::{Avg, CumulativeSum, Max};
use crate::common::metrics::{ClosureMeasurable, Metrics, Sensor};
use crate::consumer::internals::ConsumerUtils;

/// Records consumer poll/commit timing metrics. Mirrors Java's
/// `KafkaConsumerMetrics implements AutoCloseable`.
///
/// Owned by the consumer (app task); `record_poll_*` / `record_commit_*` run
/// per-poll / per-commit (low frequency, never per-record). The shared scalar
/// state (`last_poll_ms`, `poll_start_ms`, `time_since_last_poll_ms`) is held
/// in `AtomicI64`s so the `last-poll-seconds-ago` closure-measurable can read
/// `last_poll_ms` while `record_poll_start` writes it — an idiomatic-Rust,
/// value-neutral swap for Java's plain `long` fields read inside the
/// `synchronized`-free measurable lambda.
pub(crate) struct KafkaConsumerMetrics {
    metrics: Arc<Metrics>,
    last_poll_metric_name: MetricName,
    time_between_poll_sensor: Arc<Sensor>,
    poll_idle_sensor: Arc<Sensor>,
    committed_sensor: Arc<Sensor>,
    commit_sync_sensor: Arc<Sensor>,
    last_poll_ms: Arc<AtomicI64>,
    poll_start_ms: AtomicI64,
    time_since_last_poll_ms: AtomicI64,
}

impl KafkaConsumerMetrics {
    /// Registers the consumer-metrics group sensors/metrics. All sensors are
    /// created via `metrics.sensor(name)` (INFO default) and the last-poll
    /// gauge via `metrics.addMetric` — full Java parity, no DEBUG gating.
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        let metric_group_name = ConsumerUtils::CONSUMER_METRIC_GROUP;

        let last_poll_ms = Arc::new(AtomicI64::new(0));

        // Java: `Measurable lastPoll = (mConfig, now) -> { ... }` — returns -1
        // if no poll has ever happened, else seconds since the last poll.
        let last_poll_ms_for_gauge = Arc::clone(&last_poll_ms);
        let last_poll = ClosureMeasurable::new(move |_config, now| {
            let last_poll_ms = last_poll_ms_for_gauge.load(Ordering::SeqCst);
            if last_poll_ms == 0 {
                // if no poll is ever triggered, just return -1.
                -1.0
            } else {
                // TimeUnit.SECONDS.convert(now - lastPollMs, MILLISECONDS)
                ((now - last_poll_ms) / 1000) as f64
            }
        });
        let last_poll_metric_name = metrics.metric_name_description_tags(
            "last-poll-seconds-ago",
            metric_group_name,
            "The number of seconds since the last poll() invocation.",
            std::collections::BTreeMap::new(),
        );
        metrics
            .add_metric_measurable(last_poll_metric_name.clone(), Box::new(last_poll))
            .expect("registering last-poll-seconds-ago metric");

        let time_between_poll_sensor = metrics.sensor("time-between-poll").expect("creating time-between-poll sensor");
        time_between_poll_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "time-between-poll-avg",
                    metric_group_name,
                    "The average delay between invocations of poll() in milliseconds.",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding time-between-poll-avg");
        time_between_poll_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "time-between-poll-max",
                    metric_group_name,
                    "The max delay between invocations of poll() in milliseconds.",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding time-between-poll-max");

        let poll_idle_sensor = metrics
            .sensor("poll-idle-ratio-avg")
            .expect("creating poll-idle-ratio-avg sensor");
        poll_idle_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "poll-idle-ratio-avg",
                    metric_group_name,
                    "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records.",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding poll-idle-ratio-avg");

        let commit_sync_sensor = metrics
            .sensor("commit-sync-time-ns-total")
            .expect("creating commit-sync-time-ns-total sensor");
        commit_sync_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "commit-sync-time-ns-total",
                    metric_group_name,
                    "The total time the consumer has spent in commitSync in nanoseconds",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(CumulativeSum::new()),
            )
            .expect("adding commit-sync-time-ns-total");

        let committed_sensor = metrics
            .sensor("committed-time-ns-total")
            .expect("creating committed-time-ns-total sensor");
        committed_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "committed-time-ns-total",
                    metric_group_name,
                    "The total time the consumer has spent in committed in nanoseconds",
                    std::collections::BTreeMap::new(),
                ),
                Box::new(CumulativeSum::new()),
            )
            .expect("adding committed-time-ns-total");

        Self {
            metrics,
            last_poll_metric_name,
            time_between_poll_sensor,
            poll_idle_sensor,
            committed_sensor,
            commit_sync_sensor,
            last_poll_ms,
            poll_start_ms: AtomicI64::new(0),
            time_since_last_poll_ms: AtomicI64::new(0),
        }
    }

    /// Java: `recordPollStart(long pollStartMs)`.
    pub(crate) fn record_poll_start(&self, poll_start_ms: i64) {
        self.poll_start_ms.store(poll_start_ms, Ordering::SeqCst);
        let last_poll_ms = self.last_poll_ms.load(Ordering::SeqCst);
        let time_since_last_poll_ms = if last_poll_ms != 0 {
            poll_start_ms - last_poll_ms
        } else {
            0
        };
        self.time_since_last_poll_ms.store(time_since_last_poll_ms, Ordering::SeqCst);
        self.time_between_poll_sensor.record_value(time_since_last_poll_ms as f64);
        self.last_poll_ms.store(poll_start_ms, Ordering::SeqCst);
    }

    /// Java: `recordPollEnd(long pollEndMs)`.
    pub(crate) fn record_poll_end(&self, poll_end_ms: i64) {
        let poll_time_ms = poll_end_ms - self.poll_start_ms.load(Ordering::SeqCst);
        let time_since_last_poll_ms = self.time_since_last_poll_ms.load(Ordering::SeqCst);
        let poll_idle_ratio = poll_time_ms as f64 * 1.0 / (poll_time_ms + time_since_last_poll_ms) as f64;
        self.poll_idle_sensor.record_value(poll_idle_ratio);
    }

    /// Java: `recordCommitSync(long duration)`.
    pub(crate) fn record_commit_sync(&self, duration: i64) {
        self.commit_sync_sensor.record_value(duration as f64);
    }

    /// Java: `recordCommitted(long duration)`.
    pub(crate) fn record_committed(&self, duration: i64) {
        self.committed_sensor.record_value(duration as f64);
    }

    /// Java: `close()` (`AutoCloseable`). Removes the registered metric and the
    /// four sensors.
    pub(crate) fn close(&self) {
        self.metrics.remove_metric(&self.last_poll_metric_name);
        self.metrics.remove_sensor(self.time_between_poll_sensor.name());
        self.metrics.remove_sensor(self.poll_idle_sensor.name());
        self.metrics.remove_sensor(self.commit_sync_sensor.name());
        self.metrics.remove_sensor(self.committed_sensor.name());
    }
}

#[cfg(test)]
mod tests {
    //! `KafkaConsumerMetricsTest` (Java) is fully translated here. The
    //! `KafkaConsumerTest` metrics rows that read `consumer.metrics()` for the
    //! `consumer-metrics` group — `testPollTimeMetrics`, `testPollIdleRatio` —
    //! are translated below at this (the recording) layer (Phase M7).
    //!
    //! The remaining `KafkaConsumerTest` metrics rows are OUT OF SCOPE per the
    //! Milestone-9 plan ("Out of scope") and are NOT translated:
    //!   - testSubscribingCustomMetricsDoesntAffectConsumerMetrics,
    //!     testSubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics,
    //!     testUnsubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics,
    //!     testShouldOnlyCallMetricReporterMetricChangeOnceWithExistingConsumerMetric,
    //!     testShouldNotCallMetricReporterMetricRemovalWithExistingConsumerMetric,
    //!     testUnSubscribingNonExisingMetricsDoesntCauseError — all exercise
    //!     `registerMetricForSubscription` / `unregisterMetricFromSubscription`
    //!     (KIP-714 broker-push telemetry), which is a separate milestone and
    //!     is not present on the `Consumer` trait.
    //!   - testMetricsReporterAutoGeneratedClientId,
    //!     testShouldAttemptToRejoinGroupAfterSyncGroupFailed (reporter count)
    //!     — exercise the `MetricsReporter` plugin list (config-class
    //!     reflection), also out of scope (the `MetricsReporter` trait seam
    //!     exists, but config-driven reporter instantiation does not).
    //!   - testAssignedPartitionsMetrics — the `assigned-partitions` gauge is
    //!     the rebalance-metrics family (Phase M5), covered by
    //!     `ConsumerRebalanceMetricsManagerTest`.

    use super::*;
    use crate::common::Metric;

    const METRIC_VALUE: i64 = 123;
    const CONSUMER_METRIC_GROUP_NAME: &str = "consumer-metrics";
    const COMMIT_SYNC_TIME_TOTAL: &str = "commit-sync-time-ns-total";
    const COMMITTED_TIME_TOTAL: &str = "committed-time-ns-total";

    struct Fixture {
        metrics: Arc<Metrics>,
        consumer_metrics: KafkaConsumerMetrics,
    }

    fn setup() -> Fixture {
        let metrics = Arc::new(Metrics::new());
        let consumer_metrics = KafkaConsumerMetrics::new(Arc::clone(&metrics));
        Fixture { metrics, consumer_metrics }
    }

    fn metric_name(metrics: &Metrics, name: &str) -> MetricName {
        metrics.metric_name(name, CONSUMER_METRIC_GROUP_NAME)
    }

    fn assert_metric_value(metrics: &Metrics, name: &str) {
        let mn = metric_name(metrics, name);
        let metric = metrics.metric(&mn).expect("metric present");
        assert_eq!(metric.metric_value().as_double(), Some(METRIC_VALUE as f64));
    }

    fn assert_metric_removed(metrics: &Metrics, name: &str) {
        let mn = metric_name(metrics, name);
        assert!(metrics.metric(&mn).is_none());
    }

    /// Helper: read a metric's value through the registry as an `f64`.
    fn read_metric(metrics: &Metrics, name: &str) -> f64 {
        let mn = metric_name(metrics, name);
        metrics
            .metric(&mn)
            .unwrap_or_else(|| panic!("metric {name} present"))
            .metric_value()
            .as_double()
            .expect("double-valued metric")
    }

    /// Java `KafkaConsumerTest.testPollTimeMetrics` (line 3226), translated at
    /// the `KafkaConsumerMetrics` level (the recording site). The Java test
    /// drives the values through `consumer.poll(Duration.ZERO)` + a mock
    /// clock; the value math is identical when driven via
    /// `record_poll_start` with the same timestamps and a mock metrics clock
    /// for `last-poll-seconds-ago`. (The end-to-end `consumer.poll()` form
    /// needs a MockClient-backed bg-task fixture absent from these unit tests;
    /// the recorded metric values are what the Java test asserts.)
    #[test]
    fn test_poll_time_metrics() {
        use crate::common::metrics::Metrics;
        use crate::common::metrics::MockTime;
        use crate::common::metrics::Time;

        let time = Arc::new(MockTime::new());
        // Java's `time` starts at a non-zero wall clock; seed a non-zero base
        // so the first poll's `last_poll_ms` is distinguishable from the
        // "no poll yet" sentinel (0).
        time.sleep(1_000_000);
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let consumer_metrics = KafkaConsumerMetrics::new(Arc::clone(&metrics));

        // Default values: -1 / NaN / NaN.
        assert_eq!(read_metric(&metrics, "last-poll-seconds-ago"), -1.0);
        assert!(read_metric(&metrics, "time-between-poll-avg").is_nan());
        assert!(read_metric(&metrics, "time-between-poll-max").is_nan());

        // First poll at the current time.
        consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(read_metric(&metrics, "last-poll-seconds-ago"), 0.0);
        assert_eq!(read_metric(&metrics, "time-between-poll-avg"), 0.0);
        assert_eq!(read_metric(&metrics, "time-between-poll-max"), 0.0);

        // Advance 5,000 ms.
        time.sleep(5 * 1000);
        assert_eq!(read_metric(&metrics, "last-poll-seconds-ago"), 5.0);

        // Second poll.
        consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(read_metric(&metrics, "time-between-poll-avg"), 2.5 * 1000.0);
        assert_eq!(read_metric(&metrics, "time-between-poll-max"), 5.0 * 1000.0);

        // Advance 10,000 ms.
        time.sleep(10 * 1000);
        assert_eq!(read_metric(&metrics, "last-poll-seconds-ago"), 10.0);

        // Third poll.
        consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(read_metric(&metrics, "time-between-poll-avg"), 5.0 * 1000.0);
        assert_eq!(read_metric(&metrics, "time-between-poll-max"), 10.0 * 1000.0);

        // Advance 5,000 ms.
        time.sleep(5 * 1000);
        assert_eq!(read_metric(&metrics, "last-poll-seconds-ago"), 5.0);

        // Fourth poll.
        consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(read_metric(&metrics, "time-between-poll-avg"), 5.0 * 1000.0);
        assert_eq!(read_metric(&metrics, "time-between-poll-max"), 10.0 * 1000.0);
    }

    /// Java `KafkaConsumerTest.testPollIdleRatio` (line 3272), translated at
    /// the `KafkaConsumerMetrics` level. Drives `record_poll_start` /
    /// `record_poll_end` with the same timing the Java test produces via the
    /// mock clock + `consumer.poll()`, then reads `poll-idle-ratio-avg`.
    #[test]
    fn test_poll_idle_ratio() {
        use crate::common::metrics::Metrics;
        use crate::common::metrics::MockTime;
        use crate::common::metrics::Time;

        let time = Arc::new(MockTime::new());
        time.sleep(1_000_000);
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let consumer_metrics = KafkaConsumerMetrics::new(Arc::clone(&metrics));

        // Default value: NaN.
        assert!(read_metric(&metrics, "poll-idle-ratio-avg").is_nan());

        // 1st poll: 50 ms inside poll, none outside → ratio = 1.0.
        consumer_metrics.record_poll_start(time.milliseconds());
        time.sleep(50);
        consumer_metrics.record_poll_end(time.milliseconds());
        assert_eq!(read_metric(&metrics, "poll-idle-ratio-avg"), 1.0);

        // 2nd poll: 50 ms outside, 0 ms inside → ratio = 0.0.
        time.sleep(50);
        consumer_metrics.record_poll_start(time.milliseconds());
        consumer_metrics.record_poll_end(time.milliseconds());
        assert_eq!(read_metric(&metrics, "poll-idle-ratio-avg"), (1.0 + 0.0) / 2.0);

        // 3rd poll: 25 ms outside, 25 ms inside → ratio = 0.5.
        time.sleep(25);
        consumer_metrics.record_poll_start(time.milliseconds());
        time.sleep(25);
        consumer_metrics.record_poll_end(time.milliseconds());
        assert_eq!(read_metric(&metrics, "poll-idle-ratio-avg"), (1.0 + 0.0 + 0.5) / 3.0);
    }

    /// Java: `shouldRecordCommitSyncTime`.
    #[test]
    fn should_record_commit_sync_time() {
        let f = setup();
        f.consumer_metrics.record_commit_sync(METRIC_VALUE);
        assert_metric_value(&f.metrics, COMMIT_SYNC_TIME_TOTAL);
    }

    /// Java: `shouldRecordCommittedTime`.
    #[test]
    fn should_record_committed_time() {
        let f = setup();
        f.consumer_metrics.record_committed(METRIC_VALUE);
        assert_metric_value(&f.metrics, COMMITTED_TIME_TOTAL);
    }

    /// Java: `shouldRemoveMetricsOnClose`.
    #[test]
    fn should_remove_metrics_on_close() {
        let f = setup();
        f.consumer_metrics.close();
        assert_metric_removed(&f.metrics, COMMIT_SYNC_TIME_TOTAL);
        assert_metric_removed(&f.metrics, COMMITTED_TIME_TOTAL);
    }

    /// Java: `checkMetricsAfterCreation`.
    #[test]
    fn check_metrics_after_creation() {
        let f = setup();
        let expected = [
            "last-poll-seconds-ago",
            "time-between-poll-avg",
            "time-between-poll-max",
            "poll-idle-ratio-avg",
            "commit-sync-time-ns-total",
            "committed-time-ns-total",
        ];
        let registered = f.metrics.metrics();
        for name in expected {
            let mn = metric_name(&f.metrics, name);
            assert!(registered.contains_key(&mn), "Missing metric: {name}");
        }
        f.consumer_metrics.close();
        let after_close = f.metrics.metrics();
        for name in expected {
            let mn = metric_name(&f.metrics, name);
            assert!(!after_close.contains_key(&mn), "Metric present after close: {name}");
        }
    }
}
