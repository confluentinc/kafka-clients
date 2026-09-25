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

//! Async-consumer background-task metrics
//! (`org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics`).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::metrics::stats::{Avg, Max, Value};
use crate::common::metrics::{Metrics, Sensor};

/// Records background-task / event-queue timing and depth metrics. Mirrors
/// Java's `AsyncConsumerMetrics implements AutoCloseable`.
///
/// All ten sensors are created via `metrics.sensor(name)` — the INFO-default
/// overload (`Metrics.java`: `sensor(name)` → `sensor(name, INFO)`). The Java
/// source is the contract: there is NO DEBUG gating on any of these sensors,
/// so the Rust translation keeps them INFO too (they are on by default).
///
/// These record points fire per-bg-poll / per-event-batch / per-unsent-request
/// — never per-record (CLAUDE.md §11) — so the per-fetch / per-record hot path
/// is untouched. `Sensor::record` short-circuits on `should_record()`
/// internally.
pub(crate) struct AsyncConsumerMetrics {
    metrics: Arc<Metrics>,
    time_between_network_thread_poll_sensor: Arc<Sensor>,
    application_event_queue_size_sensor: Arc<Sensor>,
    application_event_queue_time_sensor: Arc<Sensor>,
    application_event_queue_processing_time_sensor: Arc<Sensor>,
    application_event_expired_size_sensor: Arc<Sensor>,
    background_event_queue_size_sensor: Arc<Sensor>,
    background_event_queue_time_sensor: Arc<Sensor>,
    background_event_queue_processing_time_sensor: Arc<Sensor>,
    unsent_requests_queue_size_sensor: Arc<Sensor>,
    unsent_requests_queue_time_sensor: Arc<Sensor>,
}

impl AsyncConsumerMetrics {
    /// Sensor name constants (Java `public static final String *_SENSOR_NAME`).
    pub(crate) const TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME: &str = "time-between-network-thread-poll";

    pub(crate) const APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME: &str = "application-event-queue-size";

    pub(crate) const APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME: &str = "application-event-queue-time";

    pub(crate) const APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME: &str =
        "application-event-queue-processing-time";

    pub(crate) const APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME: &str = "application-events-expired-count";

    pub(crate) const BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME: &str = "background-event-queue-size";

    pub(crate) const BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME: &str = "background-event-queue-time";

    pub(crate) const BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME: &str =
        "background-event-queue-processing-time";

    pub(crate) const UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME: &str = "unsent-requests-queue-size";

    pub(crate) const UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME: &str = "unsent-requests-queue-time";

    /// Java: `AsyncConsumerMetrics(Metrics metrics, String groupName)`.
    pub(crate) fn new(metrics: Arc<Metrics>, group_name: &str) -> Self {
        let time_between_network_thread_poll_sensor = metrics
            .sensor(AsyncConsumerMetrics::TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME)
            .expect("creating time-between-network-thread-poll sensor");
        time_between_network_thread_poll_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "time-between-network-thread-poll-avg",
                    group_name,
                    "The average time taken, in milliseconds, between each poll in the network thread.",
                    BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding time-between-network-thread-poll-avg");
        time_between_network_thread_poll_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "time-between-network-thread-poll-max",
                    group_name,
                    "The maximum time taken, in milliseconds, between each poll in the network thread.",
                    BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding time-between-network-thread-poll-max");

        let application_event_queue_size_sensor = metrics
            .sensor(AsyncConsumerMetrics::APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME)
            .expect("creating application-event-queue-size sensor");
        application_event_queue_size_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    AsyncConsumerMetrics::APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME,
                    group_name,
                    "The current number of events in the queue to send from the application thread to the background thread.",
                    BTreeMap::new(),
                ),
                Box::new(Value::new()),
            )
            .expect("adding application-event-queue-size");

        let application_event_queue_time_sensor = metrics
            .sensor(AsyncConsumerMetrics::APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME)
            .expect("creating application-event-queue-time sensor");
        application_event_queue_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "application-event-queue-time-avg",
                    group_name,
                    "The average time, in milliseconds, that application events are taking to be dequeued.",
                    BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding application-event-queue-time-avg");
        application_event_queue_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "application-event-queue-time-max",
                    group_name,
                    "The maximum time, in milliseconds, that an application event took to be dequeued.",
                    BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding application-event-queue-time-max");

        let application_event_queue_processing_time_sensor = metrics
            .sensor(AsyncConsumerMetrics::APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME)
            .expect("creating application-event-queue-processing-time sensor");
        application_event_queue_processing_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "application-event-queue-processing-time-avg",
                    group_name,
                    "The average time, in milliseconds, that the background thread takes to process all available application events.",
                    BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding application-event-queue-processing-time-avg");
        application_event_queue_processing_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "application-event-queue-processing-time-max",
                    group_name,
                    "The maximum time, in milliseconds, that the background thread took to process all available application events.",
                    BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding application-event-queue-processing-time-max");

        let application_event_expired_size_sensor = metrics
            .sensor(AsyncConsumerMetrics::APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME)
            .expect("creating application-events-expired-count sensor");
        application_event_expired_size_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    AsyncConsumerMetrics::APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME,
                    group_name,
                    "The current number of expired application events.",
                    BTreeMap::new(),
                ),
                Box::new(Value::new()),
            )
            .expect("adding application-events-expired-count");

        let unsent_requests_queue_size_sensor = metrics
            .sensor(AsyncConsumerMetrics::UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME)
            .expect("creating unsent-requests-queue-size sensor");
        unsent_requests_queue_size_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    AsyncConsumerMetrics::UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME,
                    group_name,
                    "The current number of unsent requests in the background thread.",
                    BTreeMap::new(),
                ),
                Box::new(Value::new()),
            )
            .expect("adding unsent-requests-queue-size");

        let unsent_requests_queue_time_sensor = metrics
            .sensor(AsyncConsumerMetrics::UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME)
            .expect("creating unsent-requests-queue-time sensor");
        unsent_requests_queue_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "unsent-requests-queue-time-avg",
                    group_name,
                    "The average time, in milliseconds, that requests are taking to be sent in the background thread.",
                    BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding unsent-requests-queue-time-avg");
        unsent_requests_queue_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "unsent-requests-queue-time-max",
                    group_name,
                    "The maximum time, in milliseconds, that a request remained unsent in the background thread.",
                    BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding unsent-requests-queue-time-max");

        let background_event_queue_size_sensor = metrics
            .sensor(AsyncConsumerMetrics::BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME)
            .expect("creating background-event-queue-size sensor");
        background_event_queue_size_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    AsyncConsumerMetrics::BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME,
                    group_name,
                    "The current number of events in the queue to send from the background thread to the application thread.",
                    BTreeMap::new(),
                ),
                Box::new(Value::new()),
            )
            .expect("adding background-event-queue-size");

        let background_event_queue_time_sensor = metrics
            .sensor(AsyncConsumerMetrics::BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME)
            .expect("creating background-event-queue-time sensor");
        background_event_queue_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "background-event-queue-time-avg",
                    group_name,
                    "The average time, in milliseconds, that background events are taking to be dequeued.",
                    BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding background-event-queue-time-avg");
        background_event_queue_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "background-event-queue-time-max",
                    group_name,
                    "The maximum time, in milliseconds, that background events are taking to be dequeued.",
                    BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding background-event-queue-time-max");

        let background_event_queue_processing_time_sensor = metrics
            .sensor(AsyncConsumerMetrics::BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME)
            .expect("creating background-event-queue-processing-time sensor");
        background_event_queue_processing_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "background-event-queue-processing-time-avg",
                    group_name,
                    "The average time, in milliseconds, that the consumer took to process all available background events.",
                    BTreeMap::new(),
                ),
                Box::new(Avg::new()),
            )
            .expect("adding background-event-queue-processing-time-avg");
        background_event_queue_processing_time_sensor
            .add_metric_name(
                metrics.metric_name_description_tags(
                    "background-event-queue-processing-time-max",
                    group_name,
                    "The maximum time, in milliseconds, that the consumer took to process all available background events.",
                    BTreeMap::new(),
                ),
                Box::new(Max::new()),
            )
            .expect("adding background-event-queue-processing-time-max");

        Self {
            metrics,
            time_between_network_thread_poll_sensor,
            application_event_queue_size_sensor,
            application_event_queue_time_sensor,
            application_event_queue_processing_time_sensor,
            application_event_expired_size_sensor,
            background_event_queue_size_sensor,
            background_event_queue_time_sensor,
            background_event_queue_processing_time_sensor,
            unsent_requests_queue_size_sensor,
            unsent_requests_queue_time_sensor,
        }
    }

    /// Java: `recordTimeBetweenNetworkThreadPoll(long)`.
    pub(crate) fn record_time_between_network_thread_poll(&self, time_between_network_thread_poll: i64) {
        self.time_between_network_thread_poll_sensor
            .record_value(time_between_network_thread_poll as f64);
    }

    /// Java: `recordApplicationEventQueueSize(int)`.
    pub(crate) fn record_application_event_queue_size(&self, size: i32) {
        self.application_event_queue_size_sensor.record_value(size as f64);
    }

    /// Java: `recordApplicationEventQueueTime(long)`.
    pub(crate) fn record_application_event_queue_time(&self, time: i64) {
        self.application_event_queue_time_sensor.record_value(time as f64);
    }

    /// Java: `recordApplicationEventQueueProcessingTime(long)`.
    pub(crate) fn record_application_event_queue_processing_time(&self, processing_time: i64) {
        self.application_event_queue_processing_time_sensor
            .record_value(processing_time as f64);
    }

    /// Java: `recordApplicationEventExpiredSize(long)`.
    pub(crate) fn record_application_event_expired_size(&self, size: i64) {
        self.application_event_expired_size_sensor.record_value(size as f64);
    }

    /// Java: `recordUnsentRequestsQueueSize(int size, long timeMs)`.
    pub(crate) fn record_unsent_requests_queue_size(&self, size: i32, time_ms: i64) {
        self.unsent_requests_queue_size_sensor
            .record_value_time_ms(size as f64, time_ms);
    }

    /// Java: `recordUnsentRequestsQueueTime(long)`.
    pub(crate) fn record_unsent_requests_queue_time(&self, time: i64) {
        self.unsent_requests_queue_time_sensor.record_value(time as f64);
    }

    /// Java: `recordBackgroundEventQueueSize(int)`.
    pub(crate) fn record_background_event_queue_size(&self, size: i32) {
        self.background_event_queue_size_sensor.record_value(size as f64);
    }

    /// Java: `recordBackgroundEventQueueTime(long)`.
    pub(crate) fn record_background_event_queue_time(&self, time: i64) {
        self.background_event_queue_time_sensor.record_value(time as f64);
    }

    /// Java: `recordBackgroundEventQueueProcessingTime(long)`.
    pub(crate) fn record_background_event_queue_processing_time(&self, processing_time: i64) {
        self.background_event_queue_processing_time_sensor
            .record_value(processing_time as f64);
    }

    /// Java: `close()` (`AutoCloseable`). Removes all ten sensors.
    pub(crate) fn close(&self) {
        for name in [
            self.time_between_network_thread_poll_sensor.name(),
            self.application_event_queue_size_sensor.name(),
            self.application_event_queue_time_sensor.name(),
            self.application_event_queue_processing_time_sensor.name(),
            self.application_event_expired_size_sensor.name(),
            self.background_event_queue_size_sensor.name(),
            self.background_event_queue_time_sensor.name(),
            self.background_event_queue_processing_time_sensor.name(),
            self.unsent_requests_queue_size_sensor.name(),
            self.unsent_requests_queue_time_sensor.name(),
        ] {
            self.metrics.remove_sensor(name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Metric;
    use crate::common::MetricName;
    use crate::consumer::internals::ConsumerUtils;

    const METRIC_VALUE: i64 = 123;

    /// Java parameterizes over both groups via `@MethodSource`; we loop.
    fn group_name_provider() -> [&'static str; 2] {
        [
            ConsumerUtils::CONSUMER_METRIC_GROUP,
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP,
        ]
    }

    fn metric_name(metrics: &Metrics, name: &str, group: &str) -> MetricName {
        metrics.metric_name(name, group)
    }

    fn assert_metric_value(metrics: &Metrics, name: &str, group: &str) {
        let mn = metric_name(metrics, name, group);
        let metric = metrics.metric(&mn).expect("metric present");
        assert_eq!(metric.metric_value().as_double(), Some(METRIC_VALUE as f64));
    }

    fn assert_metric_value_eq(metrics: &Metrics, name: &str, group: &str, expected: f64) {
        let mn = metric_name(metrics, name, group);
        let metric = metrics.metric(&mn).expect("metric present");
        assert_eq!(metric.metric_value().as_double(), Some(expected));
    }

    /// Java: `shouldMetricNames`.
    #[test]
    fn should_metric_names() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);

            let expected = [
                "time-between-network-thread-poll-avg",
                "time-between-network-thread-poll-max",
                "application-event-queue-size",
                "application-event-queue-time-avg",
                "application-event-queue-time-max",
                "application-event-queue-processing-time-avg",
                "application-event-queue-processing-time-max",
                "unsent-requests-queue-size",
                "unsent-requests-queue-time-avg",
                "unsent-requests-queue-time-max",
                "background-event-queue-size",
                "background-event-queue-time-avg",
                "background-event-queue-time-max",
                "background-event-queue-processing-time-avg",
                "background-event-queue-processing-time-max",
            ];

            let registered = metrics.metrics();
            for name in expected {
                let mn = metric_name(&metrics, name, group_name);
                assert!(registered.contains_key(&mn), "Missing metric: {name} ({group_name})");
            }

            consumer_metrics.close();
            let after_close = metrics.metrics();
            for name in expected {
                let mn = metric_name(&metrics, name, group_name);
                assert!(
                    !after_close.contains_key(&mn),
                    "Metric present after close: {name} ({group_name})"
                );
            }
        }
    }

    /// Java: `shouldRecordTimeBetweenNetworkThreadPoll`.
    #[test]
    fn should_record_time_between_network_thread_poll() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_time_between_network_thread_poll(METRIC_VALUE);
            assert_metric_value(&metrics, "time-between-network-thread-poll-avg", group_name);
            assert_metric_value(&metrics, "time-between-network-thread-poll-max", group_name);
        }
    }

    /// Java: `shouldRecordApplicationEventQueueSize`.
    #[test]
    fn should_record_application_event_queue_size() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_application_event_queue_size(10);
            assert_metric_value_eq(&metrics, "application-event-queue-size", group_name, 10.0);
        }
    }

    /// Java: `shouldRecordApplicationEventQueueTime`.
    #[test]
    fn should_record_application_event_queue_time() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_application_event_queue_time(METRIC_VALUE);
            assert_metric_value(&metrics, "application-event-queue-time-avg", group_name);
            assert_metric_value(&metrics, "application-event-queue-time-max", group_name);
        }
    }

    /// Java: `shouldRecordApplicationEventQueueProcessingTime`.
    #[test]
    fn should_record_application_event_queue_processing_time() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_application_event_queue_processing_time(METRIC_VALUE);
            assert_metric_value(&metrics, "application-event-queue-processing-time-avg", group_name);
            assert_metric_value(&metrics, "application-event-queue-processing-time-max", group_name);
        }
    }

    /// Java: `shouldRecordUnsentRequestsQueueSize`.
    #[test]
    fn should_record_unsent_requests_queue_size() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_unsent_requests_queue_size(10, 100);
            assert_metric_value_eq(&metrics, "unsent-requests-queue-size", group_name, 10.0);
        }
    }

    /// Java: `shouldRecordUnsentRequestsQueueTime`.
    #[test]
    fn should_record_unsent_requests_queue_time() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_unsent_requests_queue_time(METRIC_VALUE);
            assert_metric_value(&metrics, "unsent-requests-queue-time-avg", group_name);
            assert_metric_value(&metrics, "unsent-requests-queue-time-max", group_name);
        }
    }

    /// Java: `shouldRecordBackgroundEventQueueSize`.
    #[test]
    fn should_record_background_event_queue_size() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_background_event_queue_size(10);
            assert_metric_value_eq(&metrics, "background-event-queue-size", group_name, 10.0);
        }
    }

    /// Java: `shouldRecordBackgroundEventQueueTime`.
    #[test]
    fn should_record_background_event_queue_time() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_background_event_queue_time(METRIC_VALUE);
            assert_metric_value(&metrics, "background-event-queue-time-avg", group_name);
            assert_metric_value(&metrics, "background-event-queue-time-max", group_name);
        }
    }

    /// Java: `shouldRecordBackgroundEventQueueProcessingTime`.
    #[test]
    fn should_record_background_event_queue_processing_time() {
        for group_name in group_name_provider() {
            let metrics = Arc::new(Metrics::new());
            let consumer_metrics = AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name);
            consumer_metrics.record_background_event_queue_processing_time(METRIC_VALUE);
            assert_metric_value(&metrics, "background-event-queue-processing-time-avg", group_name);
            assert_metric_value(&metrics, "background-event-queue-processing-time-max", group_name);
        }
    }
}
