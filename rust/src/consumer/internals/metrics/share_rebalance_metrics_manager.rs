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

//! Share-group rebalance rate / total metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager`).
//!
//! Java's `ShareRebalanceMetricsManager extends RebalanceMetricsManager`;
//! Rust implements the [`RebalanceMetricsManager`] trait, as
//! [`super::ConsumerRebalanceMetricsManager`] does, and composes
//! [`AbstractConsumerMetricsManager`]. The `metricGroupName` /
//! `createMetric` helpers of the Java base are inlined into
//! [`ShareRebalanceMetricsManager::new`] (see the trait's module docs).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::common::MetricName;
use crate::common::metrics::sensor::record_or_log;
use crate::common::metrics::stats::{CumulativeCount, Rate, WindowedCount};
use crate::common::metrics::{Metrics, Sensor, TimeUnit};
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::metrics::{AbstractConsumerMetricsManager, MetricsLedger, RebalanceMetricsManager};

/// Records share-group rebalance events: the `rebalance-total` count and the
/// hourly `rebalance-rate-per-hour`, both on the `rebalance-latency` sensor in
/// the `consumer-share-coordinator-metrics` group.
///
/// Owned by the share membership state machine (bg task); recorded once per
/// reconcile cycle, never per record. `last_rebalance_end_ms` /
/// `last_rebalance_start_ms` are atomics (init -1) in place of Java's plain
/// `long` fields, so the trait's `&self` methods can update them.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager")]
pub(crate) struct ShareRebalanceMetricsManager {
    inner: AbstractConsumerMetricsManager,
    rebalance_sensor: Arc<Sensor>,
    /// Java: `public final MetricName rebalanceTotal` (read by its tests only).
    pub(crate) rebalance_total: MetricName,
    /// Java: `public final MetricName rebalanceRatePerHour` (read by its tests
    /// only).
    pub(crate) rebalance_rate_per_hour: MetricName,
    /// Java: `private long lastRebalanceEndMs = -1L`.
    last_rebalance_end_ms: AtomicI64,
    /// Java: `private long lastRebalanceStartMs = -1L`.
    last_rebalance_start_ms: AtomicI64,
}

impl ShareRebalanceMetricsManager {
    /// Java: `public ShareRebalanceMetricsManager(Metrics metrics)`, which
    /// delegates to the private `ShareRebalanceMetricsManager(MetricsLedger)`
    /// (folded in here). The metric group is
    /// `CONSUMER_SHARE_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX`; all
    /// metrics are INFO level, Java's default.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager#ShareRebalanceMetricsManager"
    )]
    pub(crate) fn new(metrics: &Arc<Metrics>) -> Self {
        let inner = AbstractConsumerMetricsManager::new(MetricsLedger::new(Arc::clone(metrics)));
        let metrics = inner.metrics();
        let metric_group_name = format!(
            "{}{}",
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX,
            ConsumerUtils::COORDINATOR_METRICS_SUFFIX
        );

        let rebalance_total =
            metrics.metric_name("rebalance-total", &metric_group_name, "The total number of rebalance events");
        let rebalance_rate_per_hour = metrics.metric_name(
            "rebalance-rate-per-hour",
            &metric_group_name,
            "The number of rebalance events per hour",
        );

        let rebalance_sensor = metrics.sensor("rebalance-latency").expect("creating rebalance-latency sensor");
        rebalance_sensor
            .add_with_metric_name(rebalance_total.clone(), Box::new(CumulativeCount::new()))
            .expect("adding rebalance-total");
        // Java: `new Rate(TimeUnit.HOURS, new WindowedCount(), 1)`.
        rebalance_sensor
            .add_with_metric_name(
                rebalance_rate_per_hour.clone(),
                Box::new(Rate::with_unit_stat_window(
                    TimeUnit::Hours,
                    Arc::new(WindowedCount::new().into_sampled_stat()),
                    1,
                )),
            )
            .expect("adding rebalance-rate-per-hour");

        Self {
            inner,
            rebalance_sensor,
            rebalance_total,
            rebalance_rate_per_hour,
            last_rebalance_end_ms: AtomicI64::new(-1),
            last_rebalance_start_ms: AtomicI64::new(-1),
        }
    }

    /// Removes every sensor and metric this manager registered. Java inherits
    /// `AbstractConsumerMetricsManager#close`.
    ///
    /// No production caller, as in Java: `ShareConsumerImpl#close` does not
    /// close the membership manager's metrics manager.
    pub(crate) fn close(&self) {
        self.inner.close();
    }
}

impl RebalanceMetricsManager for ShareRebalanceMetricsManager {
    /// Java: `recordRebalanceStarted(long nowMs)`.
    fn record_rebalance_started(&self, now_ms: i64) {
        self.last_rebalance_start_ms.store(now_ms, Ordering::SeqCst);
    }

    /// Java: `recordRebalanceEnded(long nowMs)`.
    fn record_rebalance_ended(&self, now_ms: i64) {
        self.last_rebalance_end_ms.store(now_ms, Ordering::SeqCst);
        let latency = now_ms - self.last_rebalance_start_ms.load(Ordering::SeqCst);
        record_or_log(self.rebalance_sensor.record_with_value(latency as f64));
    }

    // `maybeRecordRebalanceFailed` is not overridden by the Java share
    // manager: the trait's empty default is the base body.

    /// Java: `rebalanceStarted()`.
    fn rebalance_started(&self) -> bool {
        self.last_rebalance_start_ms.load(Ordering::SeqCst) > self.last_rebalance_end_ms.load(Ordering::SeqCst)
    }
}

/// Translated from `ShareRebalanceMetricsManagerTest`, which extends
/// `AbstractConsumerMetricsManagerTest` (one inherited test, `testCleanup`).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Metric;
    use crate::common::utils::MockTime;
    use crate::common::utils::Time;
    use crate::consumer::internals::metrics::HeartbeatMetricsManager;

    fn value(metrics: &Metrics, name: &MetricName) -> f64 {
        metrics.metric(name).unwrap().metric_value().as_double().unwrap()
    }

    /// Java: `testRebalanceMetrics`.
    #[test]
    fn test_rebalance_metrics() {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let share_rebalance_metrics_manager = ShareRebalanceMetricsManager::new(&metrics);

        assert!(metrics.metric(&share_rebalance_metrics_manager.rebalance_total).is_some());
        assert!(
            metrics
                .metric(&share_rebalance_metrics_manager.rebalance_rate_per_hour)
                .is_some()
        );

        share_rebalance_metrics_manager.record_rebalance_started(10);
        share_rebalance_metrics_manager.record_rebalance_ended(25);
        share_rebalance_metrics_manager.record_rebalance_started(30);
        share_rebalance_metrics_manager.record_rebalance_ended(40);
        share_rebalance_metrics_manager.record_rebalance_started(50);
        share_rebalance_metrics_manager.record_rebalance_ended(100);

        assert_eq!(3.0, value(&metrics, &share_rebalance_metrics_manager.rebalance_total));
        assert_eq!(3.0, value(&metrics, &share_rebalance_metrics_manager.rebalance_rate_per_hour));
    }

    /// `rebalanceStarted()` is true between a start and the next end. Java has
    /// no share test for it; this pins the trait method for this manager.
    #[test]
    fn test_rebalance_started() {
        let metrics = Arc::new(Metrics::new());
        let manager = ShareRebalanceMetricsManager::new(&metrics);
        assert!(!manager.rebalance_started());
        manager.record_rebalance_started(10);
        assert!(manager.rebalance_started());
        manager.record_rebalance_ended(25);
        assert!(!manager.rebalance_started());
    }

    /// Java: `AbstractConsumerMetricsManagerTest#testCleanup`, inherited. As in
    /// Java, `ShareRebalanceMetricsManagerTest#metricsManager` returns a
    /// `HeartbeatMetricsManager`, not a share rebalance manager.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManagerTest#testCleanup")]
    fn test_cleanup() {
        crate::consumer::internals::metrics::abstract_consumer_metrics_manager::tests::test_cleanup(
            |metrics, group_description| {
                let manager = HeartbeatMetricsManager::with_prefix(metrics, group_description);
                Box::new(move || manager.close())
            },
        );
    }

    /// Building the manager registers its metrics and `close` removes exactly
    /// those (the `testCleanup` contract, for the share manager itself).
    #[test]
    fn test_close_removes_its_metrics() {
        let metrics = Arc::new(Metrics::new());
        let metric_count = metrics.metrics().len();
        let manager = ShareRebalanceMetricsManager::new(&metrics);
        assert_eq!(metric_count + 2, metrics.metrics().len());
        manager.close();
        assert_eq!(metric_count, metrics.metrics().len());
    }
}
