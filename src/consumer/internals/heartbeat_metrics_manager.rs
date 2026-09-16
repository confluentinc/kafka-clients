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

//! Coordinator heartbeat latency/rate metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

#[cfg(test)]
use crate::common::MetricName;
use crate::common::metrics::stats::{Max, Meter, WindowedCount};
use crate::common::metrics::{ClosureMeasurable, Metrics, Sensor};
use crate::consumer::internals::ConsumerUtils;

/// Records coordinator-heartbeat latency, rate, and last-heartbeat age.
/// Mirrors Java's `HeartbeatMetricsManager`.
///
/// Owned by the heartbeat request manager (bg task); `record_heartbeat_sent_ms`
/// runs per heartbeat send and `record_request_latency` per heartbeat
/// response (low frequency, never per-record). `last_heartbeat_ms` is held in
/// an `Arc<AtomicI64>` (init -1) so the `last-heartbeat-seconds-ago` closure
/// measurable can read it while `record_heartbeat_sent_ms` writes it — an
/// idiomatic-Rust, value-neutral swap for Java's plain `long lastHeartbeatMs`.
pub(crate) struct HeartbeatMetricsManager {
    // MetricName fields visible for testing (Java: package-private `final`).
    #[cfg(test)]
    heartbeat_response_time_max: MetricName,
    #[cfg(test)]
    heartbeat_rate: MetricName,
    #[cfg(test)]
    heartbeat_total: MetricName,
    #[cfg(test)]
    last_heartbeat_seconds_ago: MetricName,
    heartbeat_sensor: Arc<Sensor>,
    last_heartbeat_ms: Arc<AtomicI64>,
}

impl HeartbeatMetricsManager {
    /// Build with the default consumer metric group prefix. Java:
    /// `HeartbeatMetricsManager(Metrics)`.
    pub(crate) fn new(metrics: &Arc<Metrics>) -> Self {
        Self::with_prefix(metrics, ConsumerUtils::CONSUMER_METRIC_GROUP_PREFIX)
    }

    /// Java: `HeartbeatMetricsManager(Metrics, String metricGroupPrefix)`.
    /// Registers the `heartbeat-latency` sensor (response-time-max + a `Meter`
    /// over a `WindowedCount` producing rate/total) and the
    /// `last-heartbeat-seconds-ago` gauge. All INFO (Java's `metrics.sensor`
    /// default) — full Java parity.
    pub(crate) fn with_prefix(metrics: &Arc<Metrics>, metric_group_prefix: &str) -> Self {
        let metric_group_name = format!("{metric_group_prefix}{}", ConsumerUtils::COORDINATOR_METRICS_SUFFIX);
        let heartbeat_sensor = metrics.sensor("heartbeat-latency").expect("creating heartbeat-latency sensor");

        let heartbeat_response_time_max = metrics.metric_name_description_tags(
            "heartbeat-response-time-max",
            &metric_group_name,
            "The max time taken to receive a response to a heartbeat request",
            BTreeMap::new(),
        );
        heartbeat_sensor
            .add_metric_name(heartbeat_response_time_max.clone(), Box::new(Max::new()))
            .expect("adding heartbeat-response-time-max");

        // windowed meters
        let heartbeat_rate = metrics.metric_name_description_tags(
            "heartbeat-rate",
            &metric_group_name,
            "The number of heartbeats per second",
            BTreeMap::new(),
        );
        let heartbeat_total = metrics.metric_name_description_tags(
            "heartbeat-total",
            &metric_group_name,
            "The total number of heartbeats",
            BTreeMap::new(),
        );
        // Java: `new Meter(new WindowedCount(), heartbeatRate, heartbeatTotal)`.
        heartbeat_sensor
            .add(Box::new(Meter::new_rate_stat(
                Arc::new(WindowedCount::new().into_sampled_stat()),
                heartbeat_rate.clone(),
                heartbeat_total.clone(),
            )))
            .expect("adding heartbeat rate/total meter");

        let last_heartbeat_ms = Arc::new(AtomicI64::new(-1));
        // Java: `Measurable lastHeartbeat = (config, now) -> { ... }`.
        let last_heartbeat_for_gauge = Arc::clone(&last_heartbeat_ms);
        let last_heartbeat = ClosureMeasurable::new(move |_config, now| {
            let last_heartbeat_send = last_heartbeat_for_gauge.load(Ordering::SeqCst);
            if last_heartbeat_send < 0 {
                // if no heartbeat is ever triggered, just return -1.
                -1.0
            } else {
                // TimeUnit.SECONDS.convert(now - lastHeartbeatSend, MILLISECONDS)
                ((now - last_heartbeat_send) / 1000) as f64
            }
        });
        let last_heartbeat_seconds_ago = metrics.metric_name_description_tags(
            "last-heartbeat-seconds-ago",
            &metric_group_name,
            "The number of seconds since the last coordinator heartbeat was sent",
            BTreeMap::new(),
        );
        metrics
            .add_metric_measurable(last_heartbeat_seconds_ago.clone(), Box::new(last_heartbeat))
            .expect("registering last-heartbeat-seconds-ago metric");

        Self {
            #[cfg(test)]
            heartbeat_response_time_max,
            #[cfg(test)]
            heartbeat_rate,
            #[cfg(test)]
            heartbeat_total,
            #[cfg(test)]
            last_heartbeat_seconds_ago,
            heartbeat_sensor,
            last_heartbeat_ms,
        }
    }

    /// Java: `recordHeartbeatSentMs(long timeMs)`.
    pub(crate) fn record_heartbeat_sent_ms(&self, time_ms: i64) {
        self.last_heartbeat_ms.store(time_ms, Ordering::SeqCst);
    }

    /// Java: `recordRequestLatency(long requestLatencyMs)`.
    pub(crate) fn record_request_latency(&self, request_latency_ms: i64) {
        self.heartbeat_sensor.record_value(request_latency_ms as f64);
    }

    /// Test-only: read back the raw `last-heartbeat-sent` timestamp the
    /// `last-heartbeat-seconds-ago` gauge closes over. `-1` means no heartbeat
    /// has been recorded yet. Lets the heartbeat-request-manager tests assert
    /// that a send site wired `record_heartbeat_sent_ms` without reaching into
    /// the private metric registry (the gauge's value derivation is covered by
    /// `test_heartbeat_metrics`).
    #[cfg(test)]
    pub(crate) fn last_heartbeat_ms_for_test(&self) -> i64 {
        self.last_heartbeat_ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Metric;
    use crate::common::metrics::MockTime;
    use crate::common::metrics::{Metrics, Time};

    /// Java: `HeartbeatMetricsManagerTest.testHeartbeatMetrics`.
    ///
    /// Java uses `rand.nextInt(10) + 1` seconds; we loop over each possible
    /// sleep value (1..=10) so the assertion is deterministic and covers the
    /// full range the random pick could land on.
    #[test]
    fn test_heartbeat_metrics() {
        for random_sleep_s in 1..=10i64 {
            let time = Arc::new(MockTime::new());
            let metrics = Arc::new(Metrics::new_time(Arc::clone(&time) as Arc<dyn crate::common::metrics::Time>));
            let manager = HeartbeatMetricsManager::new(&metrics);

            // Assert the existence of metrics.
            assert!(metrics.metric(&manager.heartbeat_response_time_max).is_some());
            assert!(metrics.metric(&manager.heartbeat_rate).is_some());
            assert!(metrics.metric(&manager.heartbeat_total).is_some());

            // Record heartbeat sent time and request latencies.
            let current_time_ms = time.milliseconds();
            manager.record_heartbeat_sent_ms(current_time_ms);
            manager.record_request_latency(100);
            manager.record_request_latency(103);
            manager.record_request_latency(102);

            // Assert recorded metrics values.
            assert_eq!(
                metrics
                    .metric(&manager.heartbeat_response_time_max)
                    .unwrap()
                    .metric_value()
                    .as_double(),
                Some(103.0)
            );
            let rate = metrics
                .metric(&manager.heartbeat_rate)
                .unwrap()
                .metric_value()
                .as_double()
                .unwrap();
            assert!((rate - 0.1).abs() < 0.01, "rate {rate} not ≈ 0.1");
            assert_eq!(
                metrics.metric(&manager.heartbeat_total).unwrap().metric_value().as_double(),
                Some(3.0)
            );

            // Sleep `random_sleep_s` seconds and assert last-heartbeat-seconds-ago.
            time.sleep(random_sleep_s * 1000);
            assert_eq!(
                metrics
                    .metric(&manager.last_heartbeat_seconds_ago)
                    .unwrap()
                    .metric_value()
                    .as_double(),
                Some(random_sleep_s as f64)
            );
        }
    }
}
