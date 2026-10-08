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

//! Offset-commit request latency/rate metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.OffsetCommitMetricsManager`).

use std::sync::Arc;

#[cfg(test)]
use crate::common::MetricName;
use crate::common::metrics::stats::{Avg, Max, Meter, WindowedCount};
use crate::common::metrics::{Metrics, Sensor};
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::metrics::{AbstractConsumerMetricsManager, MetricsLedger};

/// Records offset-commit request latency and rate. Mirrors Java's
/// `OffsetCommitMetricsManager`.
///
/// Owned by the commit request manager (bg task); `record_request_latency`
/// runs once per commit response (low frequency, never per-record).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.OffsetCommitMetricsManager")]
pub(crate) struct OffsetCommitMetricsManager {
    inner: AbstractConsumerMetricsManager,
    // MetricName fields visible for testing (Java: package-private `final`).
    #[cfg(test)]
    commit_latency_avg: MetricName,
    #[cfg(test)]
    commit_latency_max: MetricName,
    #[cfg(test)]
    commit_rate: MetricName,
    #[cfg(test)]
    commit_total: MetricName,
    commit_sensor: Arc<Sensor>,
}

impl OffsetCommitMetricsManager {
    /// Registers the `commit-latency` sensor (avg/max + a `Meter` over a
    /// `WindowedCount` producing the rate/total). All metrics are INFO (Java's
    /// `metrics.sensor` default) — full Java parity.
    pub(crate) fn new(metrics: &Arc<Metrics>) -> Self {
        // Java: `this(new MetricsLedger(metrics), ..)` → `super(metrics)` (KAFKA-19542).
        let inner = AbstractConsumerMetricsManager::new(MetricsLedger::new(Arc::clone(metrics)));
        let metrics = inner.metrics();
        let metric_group_name = format!(
            "{}{}",
            ConsumerUtils::CONSUMER_METRIC_GROUP_PREFIX,
            ConsumerUtils::COORDINATOR_METRICS_SUFFIX
        );
        let commit_sensor = metrics.sensor("commit-latency").expect("creating commit-latency sensor");

        let commit_latency_avg = metrics.metric_name(
            "commit-latency-avg",
            &metric_group_name,
            "The average time taken for a commit request",
        );
        commit_sensor
            .add_metric_name(commit_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding commit-latency-avg");

        let commit_latency_max = metrics.metric_name(
            "commit-latency-max",
            &metric_group_name,
            "The max time taken for a commit request",
        );
        commit_sensor
            .add_metric_name(commit_latency_max.clone(), Box::new(Max::new()))
            .expect("adding commit-latency-max");

        let commit_rate =
            metrics.metric_name("commit-rate", &metric_group_name, "The number of commit calls per second");
        let commit_total = metrics.metric_name("commit-total", &metric_group_name, "The total number of commit calls");
        // Java: `new Meter(new WindowedCount(), commitRate, commitTotal)`.
        commit_sensor
            .add(Box::new(Meter::with_rate_stat(
                Arc::new(WindowedCount::new().into_sampled_stat()),
                commit_rate.clone(),
                commit_total.clone(),
            )))
            .expect("adding commit rate/total meter");

        Self {
            inner,
            #[cfg(test)]
            commit_latency_avg,
            #[cfg(test)]
            commit_latency_max,
            #[cfg(test)]
            commit_rate,
            #[cfg(test)]
            commit_total,
            commit_sensor,
        }
    }

    /// Java: `recordRequestLatency(long responseLatencyMs)`.
    pub(crate) fn record_request_latency(&self, response_latency_ms: i64) {
        self.commit_sensor.record_value(response_latency_ms as f64);
    }

    /// Removes every sensor and metric this manager registered. Java inherits
    /// `AbstractConsumerMetricsManager.close()` (KAFKA-19542).
    ///
    /// No production caller, as in Java: `AsyncKafkaConsumer.close()` closes
    /// neither this manager nor the request manager that owns it, so these
    /// metrics outlive `close()` when a `group.id` is set (Java's own
    /// `testMetricsRemovedOnClose` builds its consumer without one).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Java never closes this manager: AsyncKafkaConsumer closes only the kafka-consumer, async-consumer, fetch and rebalance-callback managers"
        )
    )]
    pub(crate) fn close(&self) {
        self.inner.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Metric;
    use crate::common::metrics::Metrics;
    use crate::common::utils::MockTime;

    /// Java: `OffsetCommitMetricsManagerTest.testOffsetCommitMetrics`.
    #[test]
    fn test_offset_commit_metrics() {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(time));
        let manager = OffsetCommitMetricsManager::new(&metrics);

        // Assert the existence of metrics.
        assert!(metrics.metric(&manager.commit_latency_avg).is_some());
        assert!(metrics.metric(&manager.commit_latency_max).is_some());
        assert!(metrics.metric(&manager.commit_rate).is_some());
        assert!(metrics.metric(&manager.commit_total).is_some());

        // Record request latency.
        manager.record_request_latency(100);
        manager.record_request_latency(102);
        manager.record_request_latency(98);

        // Assert the recorded latency.
        assert_eq!(
            metrics.metric(&manager.commit_latency_avg).unwrap().metric_value().as_double(),
            Some(100.0)
        );
        assert_eq!(
            metrics.metric(&manager.commit_latency_max).unwrap().metric_value().as_double(),
            Some(102.0)
        );
        assert_eq!(
            metrics.metric(&manager.commit_rate).unwrap().metric_value().as_double(),
            Some(0.1)
        );
        assert_eq!(
            metrics.metric(&manager.commit_total).unwrap().metric_value().as_double(),
            Some(3.0)
        );
    }

    /// `OffsetCommitMetricsManagerTest.testCleanup`, inherited from
    /// `AbstractConsumerMetricsManagerTest` (KAFKA-19542), translated as Java
    /// wrote it: the Java override returns
    /// `new HeartbeatMetricsManager(metrics, groupDescription)`, not a
    /// `OffsetCommitMetricsManager` (a copy of `HeartbeatMetricsManagerTest`'s override).
    /// [`test_cleanup_offset_commit_metrics_manager`] covers this class.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManagerTest#testCleanup")]
    fn test_cleanup() {
        crate::consumer::internals::metrics::abstract_consumer_metrics_manager::tests::test_cleanup(
            |metrics, group_description| {
                let manager = crate::consumer::internals::metrics::HeartbeatMetricsManager::with_prefix(
                    metrics,
                    group_description,
                );
                Box::new(move || manager.close())
            },
        );
    }

    /// Rust-only companion of [`test_cleanup`]: the same check over this
    /// class, which Java's override does not reach.
    #[test]
    fn test_cleanup_offset_commit_metrics_manager() {
        crate::consumer::internals::metrics::abstract_consumer_metrics_manager::tests::test_cleanup(
            |metrics, _group_description| {
                let manager = OffsetCommitMetricsManager::new(metrics);
                Box::new(move || manager.close())
            },
        );
    }
}
