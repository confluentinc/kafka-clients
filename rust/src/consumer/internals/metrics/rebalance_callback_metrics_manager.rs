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

//! Rebalance-listener-callback latency metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager`).

use std::sync::Arc;

#[cfg(test)]
use crate::common::MetricName;
use crate::common::metrics::stats::{Avg, Max};
use crate::common::metrics::{Metrics, Sensor};
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::metrics::{AbstractConsumerMetricsManager, MetricsLedger};

/// Records the latency of the user-supplied `ConsumerRebalanceListener`
/// callbacks (`on_partitions_revoked` / `_assigned` / `_lost`). Mirrors Java's
/// `RebalanceCallbackMetricsManager`.
///
/// Owned by the rebalance-listener invoker; `record_partitions_*_latency` fires
/// once per listener callback (per-rebalance, never per-record). Each metric is
/// an `Avg` + `Max` on its own sensor, all INFO level (Java's default — no
/// explicit recording level).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager")]
pub(crate) struct RebalanceCallbackMetricsManager {
    inner: AbstractConsumerMetricsManager,
    // MetricName fields visible for testing (Java: package-private `final`).
    #[cfg(test)]
    pub(crate) partition_revoke_latency_avg: MetricName,
    #[cfg(test)]
    pub(crate) partition_assign_latency_avg: MetricName,
    #[cfg(test)]
    pub(crate) partition_lost_latency_avg: MetricName,
    #[cfg(test)]
    pub(crate) partition_revoke_latency_max: MetricName,
    #[cfg(test)]
    pub(crate) partition_assign_latency_max: MetricName,
    #[cfg(test)]
    pub(crate) partition_lost_latency_max: MetricName,
    partition_revoke_callback_sensor: Arc<Sensor>,
    partition_assign_callback_sensor: Arc<Sensor>,
    partition_lost_callback_sensor: Arc<Sensor>,
}

impl RebalanceCallbackMetricsManager {
    /// Java: `RebalanceCallbackMetricsManager(Metrics)`. Uses the default
    /// consumer metric group prefix.
    pub(crate) fn new(metrics: &Arc<Metrics>) -> Self {
        Self::with_prefix(metrics, ConsumerUtils::CONSUMER_METRIC_GROUP_PREFIX)
    }

    /// Java: `RebalanceCallbackMetricsManager(Metrics, String grpMetricsPrefix)`.
    /// The metric group is `{prefix}-coordinator-metrics`.
    pub(crate) fn with_prefix(metrics: &Arc<Metrics>, grp_metrics_prefix: &str) -> Self {
        // Java: `this(new MetricsLedger(metrics), ..)` → `super(metrics)` (KAFKA-19542).
        let inner = AbstractConsumerMetricsManager::new(MetricsLedger::new(Arc::clone(metrics)));
        let metrics = inner.metrics();
        let metric_group_name = format!("{grp_metrics_prefix}{}", ConsumerUtils::COORDINATOR_METRICS_SUFFIX);

        let partition_revoke_callback_sensor = metrics
            .sensor("partition-revoked-latency")
            .expect("creating partition-revoked-latency sensor");
        let partition_revoke_latency_avg = metrics.metric_name(
            "partition-revoked-latency-avg",
            &metric_group_name,
            "The average time taken for a partition-revoked rebalance listener callback",
        );
        partition_revoke_callback_sensor
            .add_metric_name(partition_revoke_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding partition-revoked-latency-avg");
        let partition_revoke_latency_max = metrics.metric_name(
            "partition-revoked-latency-max",
            &metric_group_name,
            "The max time taken for a partition-revoked rebalance listener callback",
        );
        partition_revoke_callback_sensor
            .add_metric_name(partition_revoke_latency_max.clone(), Box::new(Max::new()))
            .expect("adding partition-revoked-latency-max");

        let partition_assign_callback_sensor = metrics
            .sensor("partition-assigned-latency")
            .expect("creating partition-assigned-latency sensor");
        let partition_assign_latency_avg = metrics.metric_name(
            "partition-assigned-latency-avg",
            &metric_group_name,
            "The average time taken for a partition-assigned rebalance listener callback",
        );
        partition_assign_callback_sensor
            .add_metric_name(partition_assign_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding partition-assigned-latency-avg");
        let partition_assign_latency_max = metrics.metric_name(
            "partition-assigned-latency-max",
            &metric_group_name,
            "The max time taken for a partition-assigned rebalance listener callback",
        );
        partition_assign_callback_sensor
            .add_metric_name(partition_assign_latency_max.clone(), Box::new(Max::new()))
            .expect("adding partition-assigned-latency-max");

        let partition_lost_callback_sensor = metrics
            .sensor("partition-lost-latency")
            .expect("creating partition-lost-latency sensor");
        let partition_lost_latency_avg = metrics.metric_name(
            "partition-lost-latency-avg",
            &metric_group_name,
            "The average time taken for a partition-lost rebalance listener callback",
        );
        partition_lost_callback_sensor
            .add_metric_name(partition_lost_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding partition-lost-latency-avg");
        let partition_lost_latency_max = metrics.metric_name(
            "partition-lost-latency-max",
            &metric_group_name,
            "The max time taken for a partition-lost rebalance listener callback",
        );
        partition_lost_callback_sensor
            .add_metric_name(partition_lost_latency_max.clone(), Box::new(Max::new()))
            .expect("adding partition-lost-latency-max");

        Self {
            inner,
            #[cfg(test)]
            partition_revoke_latency_avg,
            #[cfg(test)]
            partition_assign_latency_avg,
            #[cfg(test)]
            partition_lost_latency_avg,
            #[cfg(test)]
            partition_revoke_latency_max,
            #[cfg(test)]
            partition_assign_latency_max,
            #[cfg(test)]
            partition_lost_latency_max,
            partition_revoke_callback_sensor,
            partition_assign_callback_sensor,
            partition_lost_callback_sensor,
        }
    }

    /// Java: `recordPartitionsRevokedLatency(long latencyMs)`.
    pub(crate) fn record_partitions_revoked_latency(&self, latency_ms: i64) {
        self.partition_revoke_callback_sensor.record_value(latency_ms as f64);
    }

    /// Java: `recordPartitionsAssignedLatency(long latencyMs)`.
    pub(crate) fn record_partitions_assigned_latency(&self, latency_ms: i64) {
        self.partition_assign_callback_sensor.record_value(latency_ms as f64);
    }

    /// Java: `recordPartitionsLostLatency(long latencyMs)`.
    pub(crate) fn record_partitions_lost_latency(&self, latency_ms: i64) {
        self.partition_lost_callback_sensor.record_value(latency_ms as f64);
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
    use crate::common::Metric;
    use crate::common::metrics::Metrics;
    use crate::common::utils::MockTime;
    use crate::common::utils::Time;

    fn value(metrics: &Metrics, name: &MetricName) -> f64 {
        metrics.metric(name).unwrap().metric_value().as_double().unwrap()
    }

    /// Java: `RebalanceCallbackMetricsManagerTest.testRebalanceCallbackMetrics`.
    #[test]
    fn test_rebalance_callback_metrics() {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let manager = RebalanceCallbackMetricsManager::new(&metrics);

        assert!(metrics.metric(&manager.partition_revoke_latency_avg).is_some());
        assert!(metrics.metric(&manager.partition_revoke_latency_max).is_some());
        assert!(metrics.metric(&manager.partition_assign_latency_avg).is_some());
        assert!(metrics.metric(&manager.partition_assign_latency_max).is_some());
        assert!(metrics.metric(&manager.partition_lost_latency_avg).is_some());
        assert!(metrics.metric(&manager.partition_lost_latency_max).is_some());

        manager.record_partitions_assigned_latency(100);
        manager.record_partitions_revoked_latency(101);
        manager.record_partitions_lost_latency(102);

        assert_eq!(101.0, value(&metrics, &manager.partition_revoke_latency_avg));
        assert_eq!(101.0, value(&metrics, &manager.partition_revoke_latency_max));
        assert_eq!(100.0, value(&metrics, &manager.partition_assign_latency_avg));
        assert_eq!(100.0, value(&metrics, &manager.partition_assign_latency_max));
        assert_eq!(102.0, value(&metrics, &manager.partition_lost_latency_avg));
        assert_eq!(102.0, value(&metrics, &manager.partition_lost_latency_max));
    }

    /// `RebalanceCallbackMetricsManagerTest.testCleanup`, inherited from
    /// `AbstractConsumerMetricsManagerTest` (KAFKA-19542), translated as Java
    /// wrote it: the Java override returns
    /// `new HeartbeatMetricsManager(metrics, groupDescription)`, not a
    /// `RebalanceCallbackMetricsManager` (a copy of `HeartbeatMetricsManagerTest`'s override).
    /// [`test_cleanup_rebalance_callback_metrics_manager`] covers this class.
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
    fn test_cleanup_rebalance_callback_metrics_manager() {
        crate::consumer::internals::metrics::abstract_consumer_metrics_manager::tests::test_cleanup(
            |metrics, group_description| {
                let manager = RebalanceCallbackMetricsManager::with_prefix(metrics, group_description);
                Box::new(move || manager.close())
            },
        );
    }
}
