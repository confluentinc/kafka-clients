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

//! Consumer-group rebalance latency / rate / failure metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager`).
//!
//! # `RebalanceMetricsManager` abstract base folded in
//!
//! Java's `ConsumerRebalanceMetricsManager extends RebalanceMetricsManager`,
//! an abstract base providing the `metricGroupName` + `createMetric(...)`
//! helper and the abstract `recordRebalanceStarted/Ended` / `rebalanceStarted`
//! / default-no-op `maybeRecordRebalanceFailed` surface. The base exists so
//! `AbstractMembershipManager` can hold a polymorphic `RebalanceMetricsManager`
//! shared by the consumer/streams and share variants. Per
//! `consumer-threading.md` §20 the Share and Streams managers are out of scope,
//! so only this one concrete implementation exists in the Rust client.
//! Translating the base as a single-impl trait would add a trait with no
//! polymorphism (CLAUDE.md DoD §7), so we fold the base directly here. A
//! `RebalanceMetricsManager` trait can be extracted with no API change if/when
//! Share/Streams are translated.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

#[cfg(test)]
use crate::common::MetricName;
use crate::common::metrics::internals::metrics_utils::TimeUnit;
use crate::common::metrics::stats::{Avg, CumulativeCount, CumulativeSum, Max, Rate, WindowedCount};
use crate::common::metrics::{ClosureMeasurable, Metrics, Sensor};
use crate::consumer::internals::consumer_utils::{CONSUMER_METRIC_GROUP_PREFIX, COORDINATOR_METRICS_SUFFIX};
use crate::consumer::internals::subscription_state::SubscriptionState;

/// Records consumer-group rebalance latency, rate, total, and failure metrics,
/// plus the `assigned-partitions` and `last-rebalance-seconds-ago` gauges.
/// Mirrors Java's `ConsumerRebalanceMetricsManager` (with the abstract
/// `RebalanceMetricsManager` base folded in — see module docs).
///
/// Owned by the membership state machine (bg task). `record_rebalance_started`
/// / `record_rebalance_ended` fire once per reconcile cycle and
/// `maybe_record_rebalance_failed` once per non-retriable heartbeat failure —
/// low frequency, never per-record.
///
/// `last_rebalance_end_ms` / `last_rebalance_start_ms` are held in
/// `Arc<AtomicI64>` (init -1) so the `last-rebalance-seconds-ago` gauge closure
/// (driven on the metric-read path) can read `last_rebalance_end_ms` while the
/// record path writes it — an idiomatic-Rust, value-neutral swap for Java's
/// plain `long` fields. All sensors/metrics are INFO level, matching Java's
/// default (`metrics.sensor` / `metricName` without an explicit level).
pub(crate) struct ConsumerRebalanceMetricsManager {
    // MetricName fields visible for testing (Java: `public final`).
    #[cfg(test)]
    pub(crate) rebalance_latency_avg: MetricName,
    #[cfg(test)]
    pub(crate) rebalance_latency_max: MetricName,
    #[cfg(test)]
    pub(crate) rebalance_latency_total: MetricName,
    #[cfg(test)]
    pub(crate) rebalance_total: MetricName,
    #[cfg(test)]
    pub(crate) rebalance_rate_per_hour: MetricName,
    #[cfg(test)]
    pub(crate) last_rebalance_seconds_ago: MetricName,
    #[cfg(test)]
    pub(crate) failed_rebalance_total: MetricName,
    #[cfg(test)]
    pub(crate) failed_rebalance_rate: MetricName,
    #[cfg(test)]
    pub(crate) assigned_partitions_count: MetricName,

    successful_rebalance_sensor: Arc<Sensor>,
    failed_rebalance_sensor: Arc<Sensor>,
    /// Java: `long lastRebalanceEndMs` (init -1). `Arc` so the gauge closure
    /// can read it on the metric-read path.
    last_rebalance_end_ms: Arc<AtomicI64>,
    /// Java: `long lastRebalanceStartMs` (init -1).
    last_rebalance_start_ms: AtomicI64,
}

impl ConsumerRebalanceMetricsManager {
    /// Java: `ConsumerRebalanceMetricsManager(Metrics, SubscriptionState)`.
    ///
    /// The metric group is `consumer-coordinator-metrics` (Java:
    /// `CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX`). Registers
    /// the `rebalance-latency` sensor (avg/max/total + a cumulative-count
    /// `rebalance-total` + an hourly `Rate` over a `WindowedCount`), the
    /// `failed-rebalance` sensor (cumulative-sum total + hourly rate), the
    /// `last-rebalance-seconds-ago` gauge, and the `assigned-partitions` gauge
    /// reading `subscriptions.num_assigned_partitions()`. All INFO level.
    pub(crate) fn new(metrics: &Arc<Metrics>, subscriptions: Arc<Mutex<SubscriptionState>>) -> Self {
        let metric_group_name = format!("{CONSUMER_METRIC_GROUP_PREFIX}{COORDINATOR_METRICS_SUFFIX}");

        let rebalance_latency_avg = metrics.metric_name(
            "rebalance-latency-avg",
            &metric_group_name,
            "The average time in ms taken for a group to complete a rebalance",
            BTreeMap::new(),
        );
        let rebalance_latency_max = metrics.metric_name(
            "rebalance-latency-max",
            &metric_group_name,
            "The max time in ms taken for a group to complete a rebalance",
            BTreeMap::new(),
        );
        let rebalance_latency_total = metrics.metric_name(
            "rebalance-latency-total",
            &metric_group_name,
            "The total number of milliseconds spent in rebalances",
            BTreeMap::new(),
        );
        let rebalance_total = metrics.metric_name(
            "rebalance-total",
            &metric_group_name,
            "The total number of rebalance events",
            BTreeMap::new(),
        );
        let rebalance_rate_per_hour = metrics.metric_name(
            "rebalance-rate-per-hour",
            &metric_group_name,
            "The number of rebalance events per hour",
            BTreeMap::new(),
        );
        let failed_rebalance_total = metrics.metric_name(
            "failed-rebalance-total",
            &metric_group_name,
            "The total number of failed rebalance events",
            BTreeMap::new(),
        );
        let failed_rebalance_rate = metrics.metric_name(
            "failed-rebalance-rate-per-hour",
            &metric_group_name,
            "The number of failed rebalance events per hour",
            BTreeMap::new(),
        );
        let assigned_partitions_count = metrics.metric_name(
            "assigned-partitions",
            &metric_group_name,
            "The number of partitions currently assigned to this consumer",
            BTreeMap::new(),
        );

        // Java: `registerAssignedPartitionCount(subscriptions)` — a Measurable
        // reading `subscriptions.numAssignedPartitions()`. Lock the
        // SubscriptionState briefly on the metric-read path (low frequency,
        // never per-record); drop the guard before returning.
        let subs_for_gauge = Arc::clone(&subscriptions);
        let num_parts = ClosureMeasurable::new(move |_config, _now| {
            let guard = match subs_for_gauge.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.num_assigned_partitions() as f64
        });
        metrics
            .add_metric(assigned_partitions_count.clone(), Box::new(num_parts))
            .expect("registering assigned-partitions metric");

        let successful_rebalance_sensor =
            metrics.sensor("rebalance-latency").expect("creating rebalance-latency sensor");
        successful_rebalance_sensor
            .add(rebalance_latency_avg.clone(), Box::new(Avg::new()))
            .expect("adding rebalance-latency-avg");
        successful_rebalance_sensor
            .add(rebalance_latency_max.clone(), Box::new(Max::new()))
            .expect("adding rebalance-latency-max");
        successful_rebalance_sensor
            .add(rebalance_latency_total.clone(), Box::new(CumulativeSum::new()))
            .expect("adding rebalance-latency-total");
        successful_rebalance_sensor
            .add(rebalance_total.clone(), Box::new(CumulativeCount::new()))
            .expect("adding rebalance-total");
        // Java: `new Rate(TimeUnit.HOURS, new WindowedCount(), 1)`.
        successful_rebalance_sensor
            .add(
                rebalance_rate_per_hour.clone(),
                Box::new(Rate::with_unit_stat_window(
                    TimeUnit::Hours,
                    Arc::new(WindowedCount::new().into_sampled_stat()),
                    1,
                )),
            )
            .expect("adding rebalance-rate-per-hour");

        let failed_rebalance_sensor = metrics.sensor("failed-rebalance").expect("creating failed-rebalance sensor");
        failed_rebalance_sensor
            .add(failed_rebalance_total.clone(), Box::new(CumulativeSum::new()))
            .expect("adding failed-rebalance-total");
        failed_rebalance_sensor
            .add(
                failed_rebalance_rate.clone(),
                Box::new(Rate::with_unit_stat_window(
                    TimeUnit::Hours,
                    Arc::new(WindowedCount::new().into_sampled_stat()),
                    1,
                )),
            )
            .expect("adding failed-rebalance-rate-per-hour");

        let last_rebalance_end_ms = Arc::new(AtomicI64::new(-1));
        // Java: `Measurable lastRebalance = (config, now) -> { ... }`.
        let last_rebalance_end_for_gauge = Arc::clone(&last_rebalance_end_ms);
        let last_rebalance = ClosureMeasurable::new(move |_config, now| {
            let last_end = last_rebalance_end_for_gauge.load(Ordering::SeqCst);
            if last_end == -1 {
                -1.0
            } else {
                // TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, MILLISECONDS)
                ((now - last_end) / 1000) as f64
            }
        });
        let last_rebalance_seconds_ago = metrics.metric_name(
            "last-rebalance-seconds-ago",
            &metric_group_name,
            "The number of seconds since the last rebalance event",
            BTreeMap::new(),
        );
        metrics
            .add_metric(last_rebalance_seconds_ago.clone(), Box::new(last_rebalance))
            .expect("registering last-rebalance-seconds-ago metric");

        Self {
            #[cfg(test)]
            rebalance_latency_avg,
            #[cfg(test)]
            rebalance_latency_max,
            #[cfg(test)]
            rebalance_latency_total,
            #[cfg(test)]
            rebalance_total,
            #[cfg(test)]
            rebalance_rate_per_hour,
            #[cfg(test)]
            last_rebalance_seconds_ago,
            #[cfg(test)]
            failed_rebalance_total,
            #[cfg(test)]
            failed_rebalance_rate,
            #[cfg(test)]
            assigned_partitions_count,
            successful_rebalance_sensor,
            failed_rebalance_sensor,
            last_rebalance_end_ms,
            last_rebalance_start_ms: AtomicI64::new(-1),
        }
    }

    /// Java: `recordRebalanceStarted(long nowMs)`.
    pub(crate) fn record_rebalance_started(&self, now_ms: i64) {
        self.last_rebalance_start_ms.store(now_ms, Ordering::SeqCst);
    }

    /// Java: `recordRebalanceEnded(long nowMs)`.
    pub(crate) fn record_rebalance_ended(&self, now_ms: i64) {
        self.last_rebalance_end_ms.store(now_ms, Ordering::SeqCst);
        let latency = now_ms - self.last_rebalance_start_ms.load(Ordering::SeqCst);
        self.successful_rebalance_sensor.record(latency as f64);
    }

    /// Java: `maybeRecordRebalanceFailed()`. A rebalance failed only if a start
    /// was recorded after the last end (`lastRebalanceStartMs > lastRebalanceEndMs`).
    pub(crate) fn maybe_record_rebalance_failed(&self) {
        let start = self.last_rebalance_start_ms.load(Ordering::SeqCst);
        let end = self.last_rebalance_end_ms.load(Ordering::SeqCst);
        if start <= end {
            return;
        }
        self.failed_rebalance_sensor.record_occurrence();
    }

    /// Java: `rebalanceStarted()`. Part of the manager's public surface but,
    /// as in Java, not called from the membership state machine — only the
    /// `ConsumerRebalanceMetricsManagerTest` cases exercise it. Kept (not
    /// `#[cfg(test)]`) to preserve the Java API.
    #[allow(dead_code)]
    pub(crate) fn rebalance_started(&self) -> bool {
        self.last_rebalance_start_ms.load(Ordering::SeqCst) > self.last_rebalance_end_ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::TopicPartition;
    use crate::common::metric::Metric;
    use crate::common::metrics::time::mock::MockTime;
    use crate::common::metrics::{Metrics, Time};
    use crate::consumer::AutoOffsetResetStrategy;

    /// Build a manager over a `MockTime`-backed `Metrics` plus a fresh
    /// `SubscriptionState`. Mirrors the Java test's `@BeforeEach setUp`
    /// (MetricConfig defaults: 2 samples, 30s window — `Metrics::with_time`
    /// uses the default `MetricConfig` which matches those defaults).
    fn setup() -> (
        Arc<MockTime>,
        Arc<Metrics>,
        Arc<Mutex<SubscriptionState>>,
        ConsumerRebalanceMetricsManager,
    ) {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
        let manager = ConsumerRebalanceMetricsManager::new(&metrics, Arc::clone(&subscriptions));
        (time, metrics, subscriptions, manager)
    }

    fn value(metrics: &Metrics, name: &MetricName) -> f64 {
        metrics.metric(name).unwrap().metric_value().as_double().unwrap()
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Java: `testAssignedPartitionCountMetric`.
    #[test]
    fn test_assigned_partition_count_metric() {
        let (_time, metrics, subscriptions, manager) = setup();
        assert!(
            metrics.metric(&manager.assigned_partitions_count).is_some(),
            "Metric assigned-partitions has not been registered as expected"
        );

        // Manually assigned partitions.
        {
            let mut s = subscriptions.lock().unwrap();
            let mut assigned = std::collections::HashSet::new();
            assigned.insert(tp("topic", 0));
            assigned.insert(tp("topic", 1));
            s.assign_from_user(assigned).expect("assign ok");
        }
        assert_eq!(2.0, value(&metrics, &manager.assigned_partitions_count));
        {
            let mut s = subscriptions.lock().unwrap();
            s.assign_from_user(std::collections::HashSet::new()).expect("assign ok");
        }
        assert_eq!(0.0, value(&metrics, &manager.assigned_partitions_count));

        {
            let mut s = subscriptions.lock().unwrap();
            s.unsubscribe();
        }
        assert_eq!(0.0, value(&metrics, &manager.assigned_partitions_count));

        // Automatically assigned partitions.
        {
            let mut s = subscriptions.lock().unwrap();
            s.subscribe_topics(std::collections::HashSet::from(["topic".to_string()]), None)
                .expect("subscribe ok");
            s.assign_from_subscribed(&[tp("topic", 0)]).expect("assign ok");
        }
        assert_eq!(1.0, value(&metrics, &manager.assigned_partitions_count));
    }

    /// Java: `testRebalanceTimingMetrics`.
    #[test]
    fn test_rebalance_timing_metrics() {
        let (time, metrics, _subs, manager) = setup();

        assert!(metrics.metric(&manager.rebalance_latency_avg).is_some());
        assert!(metrics.metric(&manager.rebalance_latency_max).is_some());
        assert!(metrics.metric(&manager.rebalance_latency_total).is_some());
        assert!(metrics.metric(&manager.rebalance_total).is_some());

        // First rebalance (10ms).
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(10);
        manager.record_rebalance_ended(time.milliseconds());

        assert_eq!(10.0, value(&metrics, &manager.rebalance_latency_avg));
        assert_eq!(10.0, value(&metrics, &manager.rebalance_latency_max));
        assert_eq!(10.0, value(&metrics, &manager.rebalance_latency_total));
        assert_eq!(1.0, value(&metrics, &manager.rebalance_total));

        // Second rebalance (30ms).
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(30);
        manager.record_rebalance_ended(time.milliseconds());

        assert_eq!(20.0, value(&metrics, &manager.rebalance_latency_avg), "avg = (10+30)/2 = 20");
        assert_eq!(30.0, value(&metrics, &manager.rebalance_latency_max), "max = 30");
        assert_eq!(40.0, value(&metrics, &manager.rebalance_latency_total), "total = 40");
        assert_eq!(2.0, value(&metrics, &manager.rebalance_total));

        // Third rebalance (50ms).
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(50);
        manager.record_rebalance_ended(time.milliseconds());

        assert_eq!(30.0, value(&metrics, &manager.rebalance_latency_avg), "avg = (10+30+50)/3 = 30");
        assert_eq!(50.0, value(&metrics, &manager.rebalance_latency_max), "max = 50");
        assert_eq!(90.0, value(&metrics, &manager.rebalance_latency_total), "total = 90");
        assert_eq!(3.0, value(&metrics, &manager.rebalance_total));
    }

    /// Java: `testRebalanceRateMetric`.
    #[test]
    fn test_rebalance_rate_metric() {
        let (time, metrics, _subs, manager) = setup();
        assert!(metrics.metric(&manager.rebalance_rate_per_hour).is_some());

        for _ in 0..3 {
            manager.record_rebalance_started(time.milliseconds());
            time.sleep(10);
            manager.record_rebalance_ended(time.milliseconds());
        }

        let rate = value(&metrics, &manager.rebalance_rate_per_hour);
        assert!((rate - 3.0).abs() < 0.1, "rate {rate} not ≈ 3.0");
    }

    /// Java: `testFailedRebalanceMetrics`.
    #[test]
    fn test_failed_rebalance_metrics() {
        let (time, metrics, _subs, manager) = setup();
        assert!(metrics.metric(&manager.failed_rebalance_total).is_some());
        assert!(metrics.metric(&manager.failed_rebalance_rate).is_some());

        assert_eq!(0.0, value(&metrics, &manager.failed_rebalance_total), "initially no failures");

        // Start a rebalance but don't complete it.
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(10);
        manager.maybe_record_rebalance_failed();
        assert_eq!(1.0, value(&metrics, &manager.failed_rebalance_total), "failure count -> 1");

        // Complete a successful rebalance.
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(10);
        manager.record_rebalance_ended(time.milliseconds());
        manager.maybe_record_rebalance_failed();
        assert_eq!(
            1.0,
            value(&metrics, &manager.failed_rebalance_total),
            "no increment after success"
        );

        // Start another, don't complete it, then record failure.
        time.sleep(10);
        manager.record_rebalance_started(time.milliseconds());
        assert!(manager.rebalance_started(), "rebalance in progress");
        time.sleep(10);
        manager.maybe_record_rebalance_failed();
        assert_eq!(2.0, value(&metrics, &manager.failed_rebalance_total));

        let failed_rate = value(&metrics, &manager.failed_rebalance_rate);
        assert!((failed_rate - 2.0).abs() < 0.1, "failed rate {failed_rate} not ≈ 2.0");
    }

    /// Java: `testLastRebalanceSecondsAgoMetric`.
    #[test]
    fn test_last_rebalance_seconds_ago_metric() {
        let (time, metrics, _subs, manager) = setup();
        assert!(metrics.metric(&manager.last_rebalance_seconds_ago).is_some());

        assert_eq!(-1.0, value(&metrics, &manager.last_rebalance_seconds_ago), "no rebalance -> -1");

        // Complete a rebalance.
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(10);
        manager.record_rebalance_ended(time.milliseconds());
        assert_eq!(0.0, value(&metrics, &manager.last_rebalance_seconds_ago), "0 immediately after");

        time.sleep(5000);
        assert_eq!(5.0, value(&metrics, &manager.last_rebalance_seconds_ago));

        time.sleep(10000);
        assert_eq!(15.0, value(&metrics, &manager.last_rebalance_seconds_ago));

        // Complete another rebalance.
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(20);
        manager.record_rebalance_ended(time.milliseconds());
        assert_eq!(
            0.0,
            value(&metrics, &manager.last_rebalance_seconds_ago),
            "reset to 0 after new rebalance"
        );
    }

    /// Java: `testRebalanceStartedFlag`.
    #[test]
    fn test_rebalance_started_flag() {
        let (time, _metrics, _subs, manager) = setup();

        assert!(!manager.rebalance_started(), "initially no rebalance");

        manager.record_rebalance_started(time.milliseconds());
        assert!(manager.rebalance_started(), "started after recordRebalanceStarted");

        time.sleep(10);
        manager.record_rebalance_ended(time.milliseconds());
        assert!(!manager.rebalance_started(), "not in progress after recordRebalanceEnded");

        time.sleep(100);
        manager.record_rebalance_started(time.milliseconds());
        assert!(manager.rebalance_started(), "new rebalance started");
    }

    /// Java: `testMultipleConsecutiveFailures`.
    #[test]
    fn test_multiple_consecutive_failures() {
        let (time, metrics, _subs, manager) = setup();

        for _ in 0..5 {
            manager.record_rebalance_started(time.milliseconds());
            time.sleep(10);
            manager.maybe_record_rebalance_failed();
        }

        assert_eq!(5.0, value(&metrics, &manager.failed_rebalance_total), "5 consecutive failures");
        assert_eq!(0.0, value(&metrics, &manager.rebalance_total), "success count stays 0");
    }

    /// Java: `testMixedSuccessAndFailureScenarios`.
    #[test]
    fn test_mixed_success_and_failure_scenarios() {
        let (time, metrics, _subs, manager) = setup();

        // First success (20ms).
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(20);
        manager.record_rebalance_ended(time.milliseconds());
        assert_eq!(1.0, value(&metrics, &manager.rebalance_total));

        // First failure.
        time.sleep(10);
        manager.record_rebalance_started(time.milliseconds());
        assert!(manager.rebalance_started(), "first failure rebalance in progress");
        time.sleep(30);
        manager.maybe_record_rebalance_failed();
        assert_eq!(1.0, value(&metrics, &manager.failed_rebalance_total), "one failure after first");

        // Second success (40ms).
        time.sleep(10);
        manager.record_rebalance_started(time.milliseconds());
        time.sleep(40);
        manager.record_rebalance_ended(time.milliseconds());
        assert_eq!(2.0, value(&metrics, &manager.rebalance_total));

        // Second failure.
        time.sleep(10);
        manager.record_rebalance_started(time.milliseconds());
        assert!(manager.rebalance_started(), "second failure rebalance in progress");
        time.sleep(50);
        manager.maybe_record_rebalance_failed();

        assert_eq!(2.0, value(&metrics, &manager.rebalance_total), "2 successes");
        assert_eq!(2.0, value(&metrics, &manager.failed_rebalance_total), "2 failures");

        assert_eq!(
            30.0,
            value(&metrics, &manager.rebalance_latency_avg),
            "avg successes only: (20+40)/2 = 30"
        );
        assert_eq!(40.0, value(&metrics, &manager.rebalance_latency_max), "max successes only = 40");
        assert_eq!(
            60.0,
            value(&metrics, &manager.rebalance_latency_total),
            "total successes only = 60"
        );
    }
}
