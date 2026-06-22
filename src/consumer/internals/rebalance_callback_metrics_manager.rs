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

use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
use crate::common::MetricName;
use crate::common::metrics::stats::{Avg, Max};
use crate::common::metrics::{Metrics, Sensor};
use crate::consumer::internals::consumer_utils::{CONSUMER_METRIC_GROUP_PREFIX, COORDINATOR_METRICS_SUFFIX};

/// Records the latency of the user-supplied `ConsumerRebalanceListener`
/// callbacks (`on_partitions_revoked` / `_assigned` / `_lost`). Mirrors Java's
/// `RebalanceCallbackMetricsManager`.
///
/// Owned by the rebalance-listener invoker; `record_partitions_*_latency` fires
/// once per listener callback (per-rebalance, never per-record). Each metric is
/// an `Avg` + `Max` on its own sensor, all INFO level (Java's default — no
/// explicit recording level).
pub(crate) struct RebalanceCallbackMetricsManager {
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
        Self::with_prefix(metrics, CONSUMER_METRIC_GROUP_PREFIX)
    }

    /// Java: `RebalanceCallbackMetricsManager(Metrics, String grpMetricsPrefix)`.
    /// The metric group is `{prefix}-coordinator-metrics`.
    pub(crate) fn with_prefix(metrics: &Arc<Metrics>, grp_metrics_prefix: &str) -> Self {
        let metric_group_name = format!("{grp_metrics_prefix}{COORDINATOR_METRICS_SUFFIX}");

        let partition_revoke_callback_sensor = metrics
            .sensor("partition-revoked-latency")
            .expect("creating partition-revoked-latency sensor");
        let partition_revoke_latency_avg = metrics.metric_name(
            "partition-revoked-latency-avg",
            &metric_group_name,
            "The average time taken for a partition-revoked rebalance listener callback",
            BTreeMap::new(),
        );
        partition_revoke_callback_sensor
            .add(partition_revoke_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding partition-revoked-latency-avg");
        let partition_revoke_latency_max = metrics.metric_name(
            "partition-revoked-latency-max",
            &metric_group_name,
            "The max time taken for a partition-revoked rebalance listener callback",
            BTreeMap::new(),
        );
        partition_revoke_callback_sensor
            .add(partition_revoke_latency_max.clone(), Box::new(Max::new()))
            .expect("adding partition-revoked-latency-max");

        let partition_assign_callback_sensor = metrics
            .sensor("partition-assigned-latency")
            .expect("creating partition-assigned-latency sensor");
        let partition_assign_latency_avg = metrics.metric_name(
            "partition-assigned-latency-avg",
            &metric_group_name,
            "The average time taken for a partition-assigned rebalance listener callback",
            BTreeMap::new(),
        );
        partition_assign_callback_sensor
            .add(partition_assign_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding partition-assigned-latency-avg");
        let partition_assign_latency_max = metrics.metric_name(
            "partition-assigned-latency-max",
            &metric_group_name,
            "The max time taken for a partition-assigned rebalance listener callback",
            BTreeMap::new(),
        );
        partition_assign_callback_sensor
            .add(partition_assign_latency_max.clone(), Box::new(Max::new()))
            .expect("adding partition-assigned-latency-max");

        let partition_lost_callback_sensor = metrics
            .sensor("partition-lost-latency")
            .expect("creating partition-lost-latency sensor");
        let partition_lost_latency_avg = metrics.metric_name(
            "partition-lost-latency-avg",
            &metric_group_name,
            "The average time taken for a partition-lost rebalance listener callback",
            BTreeMap::new(),
        );
        partition_lost_callback_sensor
            .add(partition_lost_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding partition-lost-latency-avg");
        let partition_lost_latency_max = metrics.metric_name(
            "partition-lost-latency-max",
            &metric_group_name,
            "The max time taken for a partition-lost rebalance listener callback",
            BTreeMap::new(),
        );
        partition_lost_callback_sensor
            .add(partition_lost_latency_max.clone(), Box::new(Max::new()))
            .expect("adding partition-lost-latency-max");

        Self {
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
        self.partition_revoke_callback_sensor.record(latency_ms as f64);
    }

    /// Java: `recordPartitionsAssignedLatency(long latencyMs)`.
    pub(crate) fn record_partitions_assigned_latency(&self, latency_ms: i64) {
        self.partition_assign_callback_sensor.record(latency_ms as f64);
    }

    /// Java: `recordPartitionsLostLatency(long latencyMs)`.
    pub(crate) fn record_partitions_lost_latency(&self, latency_ms: i64) {
        self.partition_lost_callback_sensor.record(latency_ms as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metric::Metric;
    use crate::common::metrics::time::mock::MockTime;
    use crate::common::metrics::{Metrics, Time};

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
}
