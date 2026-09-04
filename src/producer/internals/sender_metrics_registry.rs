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

//! Metric-name registry for the producer's `Sender`
//! (`org.apache.kafka.clients.producer.internals.SenderMetricsRegistry`).

use std::collections::BTreeMap;
use std::sync::Arc;

use indexmap::IndexSet;

use crate::common::metrics::{Measurable, Metrics, Sensor};
use crate::common::{Error, MetricName, MetricNameTemplate};
use crate::producer::internals::kafka_producer_metrics::GROUP;

/// The metric group name for the per-topic producer metrics.
///
/// Mirrors Java's `SenderMetricsRegistry.TOPIC_METRIC_GROUP_NAME`.
const TOPIC_METRIC_GROUP_NAME: &str = "producer-topic-metrics";

/// Holds every [`MetricName`] / [`MetricNameTemplate`] used by the `Sender`.
///
/// Mirrors Java's `SenderMetricsRegistry`: the client-level metrics are
/// resolved eagerly into concrete [`MetricName`]s (the default tag set is known
/// up front), while the per-topic metrics can only be templates because the
/// `topic` tag value is not known until a topic is first seen. This type also
/// mirrors Java's thin delegation to the shared [`Metrics`] registry
/// (`sensor`/`get_sensor`/`add_metric`), so the `Sender`'s `SenderMetrics` can
/// route every registration through it exactly as Java does.
pub(crate) struct SenderMetricsRegistry {
    metrics: Arc<Metrics>,
    /// Every registered template, for `all_templates()`. Only read by the
    /// metric-template parity tests (Java's `SenderTest.testSenderMetricsTemplates`).
    #[cfg_attr(not(test), allow(dead_code))]
    all_templates: Vec<MetricNameTemplate>,

    /* Client level. */
    pub(crate) batch_size_avg: MetricName,
    pub(crate) batch_size_max: MetricName,
    pub(crate) compression_rate_avg: MetricName,
    pub(crate) record_queue_time_avg: MetricName,
    pub(crate) record_queue_time_max: MetricName,
    pub(crate) request_latency_avg: MetricName,
    pub(crate) request_latency_max: MetricName,
    pub(crate) produce_throttle_time_avg: MetricName,
    pub(crate) produce_throttle_time_max: MetricName,
    pub(crate) record_send_rate: MetricName,
    pub(crate) record_send_total: MetricName,
    pub(crate) records_per_request_avg: MetricName,
    pub(crate) record_retry_rate: MetricName,
    pub(crate) record_retry_total: MetricName,
    pub(crate) record_error_rate: MetricName,
    pub(crate) record_error_total: MetricName,
    pub(crate) record_size_max: MetricName,
    pub(crate) record_size_avg: MetricName,
    pub(crate) requests_in_flight: MetricName,
    pub(crate) metadata_age: MetricName,
    pub(crate) batch_split_rate: MetricName,
    pub(crate) batch_split_total: MetricName,

    /* Topic level. */
    topic_record_send_rate: MetricNameTemplate,
    topic_record_send_total: MetricNameTemplate,
    topic_byte_rate: MetricNameTemplate,
    topic_byte_total: MetricNameTemplate,
    topic_compression_rate: MetricNameTemplate,
    topic_record_retry_rate: MetricNameTemplate,
    topic_record_retry_total: MetricNameTemplate,
    topic_record_error_rate: MetricNameTemplate,
    topic_record_error_total: MetricNameTemplate,
}

impl SenderMetricsRegistry {
    /// Builds the registry over the given [`Metrics`], resolving the client-level
    /// metrics eagerly. Translates Java's `SenderMetricsRegistry(Metrics)`.
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        // Java: `this.tags = this.metrics.config().tags().keySet()` — the set of
        // default tag *keys* (e.g. `client-id`), not the values.
        let tags: IndexSet<String> = metrics.config().tags().keys().cloned().collect();
        let mut all_templates: Vec<MetricNameTemplate> = Vec::new();

        // Resolves a client-level metric: creates the template (adding it to
        // `all_templates`) and resolves it to a concrete `MetricName`, mirroring
        // Java's `createMetricName` → `metrics.metricInstance(createTemplate(...))`.
        let mut create_metric_name = |name: &str, description: &str| -> MetricName {
            let template = MetricNameTemplate::new(name, GROUP, description, tags.clone());
            all_templates.push(template.clone());
            // The empty runtime-tag map merges with the config tags, so the
            // resolved name carries exactly the default (`client-id`) tags. The
            // template tag keys match the config keys by construction, so this
            // never errors.
            metrics
                .metric_instance_key_value(&template, &[])
                .expect("client-level template tags match config tags")
        };

        /* Client level. */
        let batch_size_avg =
            create_metric_name("batch-size-avg", "The average number of bytes sent per partition per-request.");
        let batch_size_max =
            create_metric_name("batch-size-max", "The max number of bytes sent per partition per-request.");
        let compression_rate_avg = create_metric_name(
            "compression-rate-avg",
            "The average compression rate of record batches, defined as the average ratio of the \
             compressed batch size over the uncompressed size.",
        );
        let record_queue_time_avg = create_metric_name(
            "record-queue-time-avg",
            "The average time in ms record batches spent in the send buffer.",
        );
        let record_queue_time_max = create_metric_name(
            "record-queue-time-max",
            "The maximum time in ms record batches spent in the send buffer.",
        );
        let request_latency_avg = create_metric_name("request-latency-avg", "The average request latency in ms");
        let request_latency_max = create_metric_name("request-latency-max", "The maximum request latency in ms");
        let record_send_rate = create_metric_name("record-send-rate", "The average number of records sent per second.");
        let record_send_total = create_metric_name("record-send-total", "The total number of records sent.");
        let records_per_request_avg =
            create_metric_name("records-per-request-avg", "The average number of records per request.");
        let record_retry_rate =
            create_metric_name("record-retry-rate", "The average per-second number of retried record sends");
        let record_retry_total = create_metric_name("record-retry-total", "The total number of retried record sends");
        let record_error_rate = create_metric_name(
            "record-error-rate",
            "The average per-second number of record sends that resulted in errors",
        );
        let record_error_total =
            create_metric_name("record-error-total", "The total number of record sends that resulted in errors");
        let record_size_max = create_metric_name("record-size-max", "The maximum record size");
        let record_size_avg = create_metric_name("record-size-avg", "The average record size");
        let requests_in_flight = create_metric_name(
            "requests-in-flight",
            "The current number of in-flight requests awaiting a response.",
        );
        let metadata_age = create_metric_name(
            "metadata-age",
            "The age in seconds of the current producer metadata being used.",
        );
        let batch_split_rate = create_metric_name("batch-split-rate", "The average number of batch splits per second");
        let batch_split_total = create_metric_name("batch-split-total", "The total number of batch splits");

        let produce_throttle_time_avg = create_metric_name(
            "produce-throttle-time-avg",
            "The average time in ms a request was throttled by a broker",
        );
        let produce_throttle_time_max = create_metric_name(
            "produce-throttle-time-max",
            "The maximum time in ms a request was throttled by a broker",
        );

        /* Topic level. */
        // Java: `this.topicTags = new LinkedHashSet<>(tags); this.topicTags.add("topic");`
        let mut topic_tags: IndexSet<String> = tags.clone();
        topic_tags.insert("topic".to_string());

        // Creates a per-topic template (adding it to `all_templates`), mirroring
        // Java's `createTopicTemplate` → `createTemplate(..., topicTags)`.
        let mut create_topic_template = |name: &str, description: &str| -> MetricNameTemplate {
            let template = MetricNameTemplate::new(name, TOPIC_METRIC_GROUP_NAME, description, topic_tags.clone());
            all_templates.push(template.clone());
            template
        };

        // We can't create the `MetricName` up front for these, because we don't
        // know the topic name yet.
        let topic_record_send_rate =
            create_topic_template("record-send-rate", "The average number of records sent per second for a topic.");
        let topic_record_send_total =
            create_topic_template("record-send-total", "The total number of records sent for a topic.");
        let topic_byte_rate =
            create_topic_template("byte-rate", "The average number of bytes sent per second for a topic.");
        let topic_byte_total = create_topic_template("byte-total", "The total number of bytes sent for a topic.");
        let topic_compression_rate = create_topic_template(
            "compression-rate",
            "The average compression rate of record batches for a topic, defined as the average ratio \
             of the compressed batch size over the uncompressed size.",
        );
        let topic_record_retry_rate = create_topic_template(
            "record-retry-rate",
            "The average per-second number of retried record sends for a topic",
        );
        let topic_record_retry_total =
            create_topic_template("record-retry-total", "The total number of retried record sends for a topic");
        let topic_record_error_rate = create_topic_template(
            "record-error-rate",
            "The average per-second number of record sends that resulted in errors for a topic",
        );
        let topic_record_error_total = create_topic_template(
            "record-error-total",
            "The total number of record sends that resulted in errors for a topic",
        );

        Self {
            metrics,
            all_templates,
            batch_size_avg,
            batch_size_max,
            compression_rate_avg,
            record_queue_time_avg,
            record_queue_time_max,
            request_latency_avg,
            request_latency_max,
            produce_throttle_time_avg,
            produce_throttle_time_max,
            record_send_rate,
            record_send_total,
            records_per_request_avg,
            record_retry_rate,
            record_retry_total,
            record_error_rate,
            record_error_total,
            record_size_max,
            record_size_avg,
            requests_in_flight,
            metadata_age,
            batch_split_rate,
            batch_split_total,
            topic_record_send_rate,
            topic_record_send_total,
            topic_byte_rate,
            topic_byte_total,
            topic_compression_rate,
            topic_record_retry_rate,
            topic_record_retry_total,
            topic_record_error_rate,
            topic_record_error_total,
        }
    }

    /* Topic level metrics. `tags` maps `topic` to the topic name. */

    pub(crate) fn topic_record_send_rate(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_record_send_rate, tags)
    }

    pub(crate) fn topic_record_send_total(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_record_send_total, tags)
    }

    pub(crate) fn topic_byte_rate(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_byte_rate, tags)
    }

    pub(crate) fn topic_byte_total(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_byte_total, tags)
    }

    pub(crate) fn topic_compression_rate(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_compression_rate, tags)
    }

    pub(crate) fn topic_record_retry_rate(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_record_retry_rate, tags)
    }

    pub(crate) fn topic_record_retry_total(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_record_retry_total, tags)
    }

    pub(crate) fn topic_record_error_rate(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_record_error_rate, tags)
    }

    pub(crate) fn topic_record_error_total(&self, tags: BTreeMap<String, String>) -> Result<MetricName, Error> {
        self.metrics.metric_instance_tags(&self.topic_record_error_total, tags)
    }

    /// Returns every registered template, mirroring Java's `allTemplates()`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn all_templates(&self) -> &[MetricNameTemplate] {
        &self.all_templates
    }

    /// Get-or-create a sensor by name (Java `sensor(String)`).
    pub(crate) fn sensor(&self, name: &str) -> Result<Arc<Sensor>, Error> {
        self.metrics.sensor(name)
    }

    /// Registers a measurable metric (Java `addMetric(MetricName, Measurable)`).
    pub(crate) fn add_metric(&self, metric_name: MetricName, measurable: Box<dyn Measurable>) -> Result<(), Error> {
        self.metrics.add_metric_measurable(metric_name, measurable)
    }

    /// Returns the sensor with the given name if it already exists (Java
    /// `getSensor(String)`).
    pub(crate) fn get_sensor(&self, name: &str) -> Option<Arc<Sensor>> {
        self.metrics.get_sensor(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::MetricConfig;

    fn metrics_with_client_id() -> Arc<Metrics> {
        let mut tags = BTreeMap::new();
        tags.insert("client-id".to_string(), "clientA".to_string());
        let config = MetricConfig::new().with_tags(tags);
        Arc::new(Metrics::new_default_config(Arc::new(config)))
    }

    /// `all_templates()` contains all 22 client-level + 9 topic-level = 31
    /// templates, and client-level ones carry the `producer-metrics` group.
    #[test]
    fn test_all_templates_count_and_groups() {
        let registry = SenderMetricsRegistry::new(metrics_with_client_id());
        assert_eq!(31, registry.all_templates().len());
        assert_eq!(GROUP, registry.batch_size_avg.group());
        assert_eq!("producer-metrics", registry.produce_throttle_time_avg.group());
    }

    /// Client-level metric names carry the default `client-id` tag; topic-level
    /// resolved names additionally carry the `topic` tag.
    #[test]
    fn test_tag_propagation() {
        let registry = SenderMetricsRegistry::new(metrics_with_client_id());
        assert!(registry.batch_size_avg.tags().contains_key("client-id"));

        let mut topic_tags = BTreeMap::new();
        topic_tags.insert("topic".to_string(), "my-topic".to_string());
        let name = registry.topic_record_send_rate(topic_tags).unwrap();
        assert_eq!(TOPIC_METRIC_GROUP_NAME, name.group());
        assert_eq!(Some(&"my-topic".to_string()), name.tags().get("topic"));
        assert!(name.tags().contains_key("client-id"));
    }
}
