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

//! Share-consumer poll timing metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics`).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::common::metrics::sensor::record_or_log;
use crate::common::metrics::stats::{Avg, Max};
use crate::common::metrics::{ClosureMeasurable, Metrics, Sensor};
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::metrics::{AbstractConsumerMetricsManager, MetricsLedger};

/// Records the share consumer's poll timing metrics in the
/// `consumer-share-metrics` group: `last-poll-seconds-ago`,
/// `time-between-poll-avg` / `-max` and `poll-idle-ratio-avg`.
///
/// Java's `KafkaShareConsumerMetrics extends AbstractConsumerMetricsManager`;
/// the base is embedded as `inner`, as in `KafkaConsumerMetrics`. The three
/// Java `long` fields are `AtomicI64`s so that the `last-poll-seconds-ago`
/// measurable can read `last_poll_ms` while `record_poll_start` writes it.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics")]
pub(crate) struct KafkaShareConsumerMetrics {
    inner: AbstractConsumerMetricsManager,
    time_between_poll_sensor: Arc<Sensor>,
    poll_idle_sensor: Arc<Sensor>,
    last_poll_ms: Arc<AtomicI64>,
    poll_start_ms: AtomicI64,
    time_since_last_poll_ms: AtomicI64,
}

impl KafkaShareConsumerMetrics {
    /// Registers the share-consumer poll metrics in `metrics`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics#KafkaShareConsumerMetrics"
    )]
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        // Java: `this(new MetricsLedger(metrics))` → `super(metrics)`.
        let inner = AbstractConsumerMetricsManager::new(MetricsLedger::new(metrics));
        let metrics = inner.metrics();
        let metric_group_name = ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP;

        let last_poll_ms = Arc::new(AtomicI64::new(0));
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
        let last_poll_metric_name = metrics.metric_name(
            "last-poll-seconds-ago",
            metric_group_name,
            "The number of seconds since the last poll() invocation.",
        );
        metrics
            .add_metric(last_poll_metric_name, Box::new(last_poll))
            .expect("registering last-poll-seconds-ago metric");

        let time_between_poll_sensor = metrics.sensor("time-between-poll").expect("creating time-between-poll sensor");
        time_between_poll_sensor
            .add_with_metric_name(
                metrics.metric_name(
                    "time-between-poll-avg",
                    metric_group_name,
                    "The average delay between invocations of poll() in milliseconds.",
                ),
                Box::new(Avg::new()),
            )
            .expect("adding time-between-poll-avg");
        time_between_poll_sensor
            .add_with_metric_name(
                metrics.metric_name(
                    "time-between-poll-max",
                    metric_group_name,
                    "The max delay between invocations of poll() in milliseconds.",
                ),
                Box::new(Max::new()),
            )
            .expect("adding time-between-poll-max");

        let poll_idle_sensor = metrics
            .sensor("poll-idle-ratio-avg")
            .expect("creating poll-idle-ratio-avg sensor");
        poll_idle_sensor
            .add_with_metric_name(
                metrics.metric_name(
                    "poll-idle-ratio-avg",
                    metric_group_name,
                    "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records."
                ),
                Box::new(Avg::new()),
            )
            .expect("adding poll-idle-ratio-avg");

        Self {
            inner,
            time_between_poll_sensor,
            poll_idle_sensor,
            last_poll_ms,
            poll_start_ms: AtomicI64::new(0),
            time_since_last_poll_ms: AtomicI64::new(0),
        }
    }

    /// Records the start of a `poll` at `poll_start_ms`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics#recordPollStart")]
    pub(crate) fn record_poll_start(&self, poll_start_ms: i64) {
        self.poll_start_ms.store(poll_start_ms, Ordering::SeqCst);
        let last_poll_ms = self.last_poll_ms.load(Ordering::SeqCst);
        let time_since_last_poll_ms = if last_poll_ms != 0 {
            poll_start_ms - last_poll_ms
        } else {
            0
        };
        self.time_since_last_poll_ms.store(time_since_last_poll_ms, Ordering::SeqCst);
        record_or_log(self.time_between_poll_sensor.record_with_value(time_since_last_poll_ms as f64));
        self.last_poll_ms.store(poll_start_ms, Ordering::SeqCst);
    }

    /// Records the end of a `poll` at `poll_end_ms`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics#recordPollEnd")]
    pub(crate) fn record_poll_end(&self, poll_end_ms: i64) {
        let poll_time_ms = poll_end_ms - self.poll_start_ms.load(Ordering::SeqCst);
        let poll_cycle_time_ms = poll_time_ms + self.time_since_last_poll_ms.load(Ordering::SeqCst);
        let poll_idle_ratio = if poll_cycle_time_ms == 0 {
            0.0
        } else {
            poll_time_ms as f64 * 1.0 / poll_cycle_time_ms as f64
        };
        record_or_log(self.poll_idle_sensor.record_with_value(poll_idle_ratio));
    }

    /// Removes every sensor and metric this manager registered (Java inherits
    /// `AbstractConsumerMetricsManager#close`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManager#close")]
    pub(crate) fn close(&self) {
        self.inner.close();
    }
}

#[cfg(test)]
mod tests {
    //! The rows of `org.apache.kafka.clients.consumer.KafkaShareConsumerMetricsTest`
    //! that assert this class's values, translated at the recording layer as
    //! `KafkaConsumerMetrics`' tests translate the `KafkaConsumerTest` rows:
    //! `testPollTimeMetrics`, `testPollIdleRatio`, `testPollIdleRatioZero`, and
    //! the metrics half of `testClosingConsumerUnregistersConsumerMetrics`.
    //! The same Java tests through `ShareConsumerImpl` (a real `poll`, a real
    //! `close`) are in `share_consumer_impl.rs`. The KIP-714 rows
    //! (`registerMetricForSubscription` / `unregisterMetricFromSubscription`)
    //! belong with those methods (Milestone 18 decision D2, P8).

    use super::*;
    use crate::common::Metric;
    use crate::common::MetricName;
    use crate::common::utils::{MockTime, Time};

    fn metric_name(metrics: &Metrics, name: &str) -> MetricName {
        metrics.metric_name(name, ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP)
    }

    fn read_metric(metrics: &Metrics, name: &str) -> f64 {
        metrics
            .metric(&metric_name(metrics, name))
            .unwrap_or_else(|| panic!("metric {name} present"))
            .metric_value()
            .as_double()
            .expect("double-valued metric")
    }

    fn setup(time: &Arc<MockTime>) -> (Arc<Metrics>, KafkaShareConsumerMetrics) {
        let metrics = Arc::new(Metrics::with_time(Arc::clone(time) as Arc<dyn Time>));
        let share_consumer_metrics = KafkaShareConsumerMetrics::new(Arc::clone(&metrics));
        (metrics, share_consumer_metrics)
    }

    /// Java `KafkaShareConsumerMetricsTest#testPollTimeMetrics`, driving
    /// `record_poll_start` with the timestamps the Java test's `poll`s record.
    #[test]
    fn test_poll_time_metrics() {
        let time = Arc::new(MockTime::new());
        // A non-zero start, so the first poll is not the "no poll yet" 0.
        time.sleep(1_000_000);
        let (metrics, share_consumer_metrics) = setup(&time);

        // Test default values
        assert_eq!(-1.0, read_metric(&metrics, "last-poll-seconds-ago"));
        assert!(read_metric(&metrics, "time-between-poll-avg").is_nan());
        assert!(read_metric(&metrics, "time-between-poll-max").is_nan());
        // Call first poll
        share_consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(0.0, read_metric(&metrics, "last-poll-seconds-ago"));
        assert_eq!(0.0, read_metric(&metrics, "time-between-poll-avg"));
        assert_eq!(0.0, read_metric(&metrics, "time-between-poll-max"));
        // Advance time by 5,000 (total time = 5,000)
        time.sleep(5 * 1000);
        assert_eq!(5.0, read_metric(&metrics, "last-poll-seconds-ago"));
        // Call second poll
        share_consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(2.5 * 1000.0, read_metric(&metrics, "time-between-poll-avg"));
        assert_eq!(5.0 * 1000.0, read_metric(&metrics, "time-between-poll-max"));
        // Advance time by 10,000 (total time = 15,000)
        time.sleep(10 * 1000);
        assert_eq!(10.0, read_metric(&metrics, "last-poll-seconds-ago"));
        // Call third poll
        share_consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(5.0 * 1000.0, read_metric(&metrics, "time-between-poll-avg"));
        assert_eq!(10.0 * 1000.0, read_metric(&metrics, "time-between-poll-max"));
        // Advance time by 5,000 (total time = 20,000)
        time.sleep(5 * 1000);
        assert_eq!(5.0, read_metric(&metrics, "last-poll-seconds-ago"));
        // Call fourth poll
        share_consumer_metrics.record_poll_start(time.milliseconds());
        assert_eq!(5.0 * 1000.0, read_metric(&metrics, "time-between-poll-avg"));
        assert_eq!(10.0 * 1000.0, read_metric(&metrics, "time-between-poll-max"));
    }

    /// Java `KafkaShareConsumerMetricsTest#testPollIdleRatio`.
    #[test]
    fn test_poll_idle_ratio() {
        let time = Arc::new(MockTime::new());
        let (metrics, share_consumer_metrics) = setup(&time);
        // Test default value
        assert!(read_metric(&metrics, "poll-idle-ratio-avg").is_nan());

        // 1st poll
        // Spend 50ms in poll so value = 1.0
        share_consumer_metrics.record_poll_start(time.milliseconds());
        time.sleep(50);
        share_consumer_metrics.record_poll_end(time.milliseconds());
        assert_eq!(1.0, read_metric(&metrics, "poll-idle-ratio-avg"));

        // 2nd poll
        // Spend 50m outside poll and 0ms in poll so value = 0.0
        time.sleep(50);
        share_consumer_metrics.record_poll_start(time.milliseconds());
        share_consumer_metrics.record_poll_end(time.milliseconds());
        // Avg of first two data points
        assert_eq!((1.0 + 0.0) / 2.0, read_metric(&metrics, "poll-idle-ratio-avg"));

        // 3rd poll
        // Spend 25ms outside poll and 25ms in poll so value = 0.5
        time.sleep(25);
        share_consumer_metrics.record_poll_start(time.milliseconds());
        time.sleep(25);
        share_consumer_metrics.record_poll_end(time.milliseconds());
        // Avg of three data points
        assert_eq!((1.0 + 0.0 + 0.5) / 3.0, read_metric(&metrics, "poll-idle-ratio-avg"));
    }

    /// Java `KafkaShareConsumerMetricsTest#testPollIdleRatioZero`.
    #[test]
    fn test_poll_idle_ratio_zero() {
        let time = Arc::new(MockTime::new());
        let (metrics, share_consumer_metrics) = setup(&time);
        // Test default value
        assert!(read_metric(&metrics, "poll-idle-ratio-avg").is_nan());

        // Poll starts and ends within the same millisecond, so the metric should be 0.
        share_consumer_metrics.record_poll_start(time.milliseconds());
        share_consumer_metrics.record_poll_end(time.milliseconds());
        assert_eq!(0.0, read_metric(&metrics, "poll-idle-ratio-avg"));
    }

    /// The metrics half of Java
    /// `KafkaShareConsumerMetricsTest#testClosingConsumerUnregistersConsumerMetrics`:
    /// the metrics are registered on construction and removed on close.
    #[test]
    fn test_closing_unregisters_consumer_metrics() {
        let time = Arc::new(MockTime::new());
        let (metrics, share_consumer_metrics) = setup(&time);
        let names = [
            "last-poll-seconds-ago",
            "time-between-poll-avg",
            "time-between-poll-max",
            "poll-idle-ratio-avg",
        ];
        for name in names {
            assert!(metrics.metric(&metric_name(&metrics, name)).is_some(), "{name}");
        }
        share_consumer_metrics.close();
        for name in names {
            assert!(metrics.metric(&metric_name(&metrics, name)).is_none(), "{name}");
        }
    }
}
