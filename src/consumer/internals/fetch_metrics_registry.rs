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

//! Templates for the consumer's fetch-manager metrics
//! (`org.apache.kafka.clients.consumer.internals.FetchMetricsRegistry`).

use indexmap::IndexSet;

use crate::common::MetricNameTemplate;

const DEPRECATED_TOPIC_METRICS_MESSAGE: &str = "Note: For topic names with periods (.), an additional \
metric with underscores is emitted. However, the periods replaced metric is deprecated. Please use the \
metric with actual topic name instead.";

/// Holds the [`MetricNameTemplate`]s for every fetch-manager metric. Mirrors
/// Java's `FetchMetricsRegistry`: the template set is split into client-level,
/// topic-level (adds the `topic` tag), and partition-level (adds the
/// `partition` tag) metrics.
#[derive(Clone, Debug)]
pub(crate) struct FetchMetricsRegistry {
    pub(crate) fetch_size_avg: MetricNameTemplate,
    pub(crate) fetch_size_max: MetricNameTemplate,
    pub(crate) bytes_consumed_rate: MetricNameTemplate,
    pub(crate) bytes_consumed_total: MetricNameTemplate,
    pub(crate) records_per_request_avg: MetricNameTemplate,
    pub(crate) records_consumed_rate: MetricNameTemplate,
    pub(crate) records_consumed_total: MetricNameTemplate,
    pub(crate) fetch_latency_avg: MetricNameTemplate,
    pub(crate) fetch_latency_max: MetricNameTemplate,
    pub(crate) fetch_request_rate: MetricNameTemplate,
    pub(crate) fetch_request_total: MetricNameTemplate,
    pub(crate) records_lag_max: MetricNameTemplate,
    pub(crate) records_lead_min: MetricNameTemplate,
    pub(crate) fetch_throttle_time_avg: MetricNameTemplate,
    pub(crate) fetch_throttle_time_max: MetricNameTemplate,
    pub(crate) topic_fetch_size_avg: MetricNameTemplate,
    pub(crate) topic_fetch_size_max: MetricNameTemplate,
    pub(crate) topic_bytes_consumed_rate: MetricNameTemplate,
    pub(crate) topic_bytes_consumed_total: MetricNameTemplate,
    pub(crate) topic_records_per_request_avg: MetricNameTemplate,
    pub(crate) topic_records_consumed_rate: MetricNameTemplate,
    pub(crate) topic_records_consumed_total: MetricNameTemplate,
    pub(crate) partition_records_lag: MetricNameTemplate,
    pub(crate) partition_records_lag_max: MetricNameTemplate,
    pub(crate) partition_records_lag_avg: MetricNameTemplate,
    pub(crate) partition_records_lead: MetricNameTemplate,
    pub(crate) partition_records_lead_min: MetricNameTemplate,
    pub(crate) partition_records_lead_avg: MetricNameTemplate,
    pub(crate) partition_preferred_read_replica: MetricNameTemplate,
}

impl FetchMetricsRegistry {
    /// Creates the registry with the given default tag set and group prefix.
    /// Translates Java's `FetchMetricsRegistry(Set<String> tags, String
    /// metricGrpPrefix)`.
    pub(crate) fn new(tags: IndexSet<String>, metric_grp_prefix: &str) -> Self {
        // Client level.
        let group_name = format!("{metric_grp_prefix}-fetch-manager-metrics");

        let fetch_size_avg = MetricNameTemplate::new(
            "fetch-size-avg",
            &group_name,
            "The average number of bytes fetched per request",
            tags.clone(),
        );
        let fetch_size_max = MetricNameTemplate::new(
            "fetch-size-max",
            &group_name,
            "The maximum number of bytes fetched per request",
            tags.clone(),
        );
        let bytes_consumed_rate = MetricNameTemplate::new(
            "bytes-consumed-rate",
            &group_name,
            "The average number of bytes consumed per second",
            tags.clone(),
        );
        let bytes_consumed_total = MetricNameTemplate::new(
            "bytes-consumed-total",
            &group_name,
            "The total number of bytes consumed",
            tags.clone(),
        );

        let records_per_request_avg = MetricNameTemplate::new(
            "records-per-request-avg",
            &group_name,
            "The average number of records in each request",
            tags.clone(),
        );
        let records_consumed_rate = MetricNameTemplate::new(
            "records-consumed-rate",
            &group_name,
            "The average number of records consumed per second",
            tags.clone(),
        );
        let records_consumed_total = MetricNameTemplate::new(
            "records-consumed-total",
            &group_name,
            "The total number of records consumed",
            tags.clone(),
        );

        let fetch_latency_avg = MetricNameTemplate::new(
            "fetch-latency-avg",
            &group_name,
            "The average time taken for a fetch request.",
            tags.clone(),
        );
        let fetch_latency_max = MetricNameTemplate::new(
            "fetch-latency-max",
            &group_name,
            "The max time taken for any fetch request.",
            tags.clone(),
        );
        let fetch_request_rate = MetricNameTemplate::new(
            "fetch-rate",
            &group_name,
            "The number of fetch requests per second.",
            tags.clone(),
        );
        let fetch_request_total =
            MetricNameTemplate::new("fetch-total", &group_name, "The total number of fetch requests.", tags.clone());

        let records_lag_max = MetricNameTemplate::new(
            "records-lag-max",
            &group_name,
            "The maximum lag in terms of number of records for any partition in this window. NOTE: This is based on current offset and not committed offset",
            tags.clone(),
        );
        let records_lead_min = MetricNameTemplate::new(
            "records-lead-min",
            &group_name,
            "The minimum lead in terms of number of records for any partition in this window",
            tags.clone(),
        );

        let fetch_throttle_time_avg = MetricNameTemplate::new(
            "fetch-throttle-time-avg",
            &group_name,
            "The average throttle time in ms",
            tags.clone(),
        );
        let fetch_throttle_time_max = MetricNameTemplate::new(
            "fetch-throttle-time-max",
            &group_name,
            "The maximum throttle time in ms",
            tags.clone(),
        );

        // Topic level.
        let mut topic_tags: IndexSet<String> = tags.clone();
        topic_tags.insert("topic".to_string());

        let topic_fetch_size_avg = MetricNameTemplate::new(
            "fetch-size-avg",
            &group_name,
            format!("The average number of bytes fetched per request for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            topic_tags.clone(),
        );
        let topic_fetch_size_max = MetricNameTemplate::new(
            "fetch-size-max",
            &group_name,
            format!("The maximum number of bytes fetched per request for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            topic_tags.clone(),
        );
        let topic_bytes_consumed_rate = MetricNameTemplate::new(
            "bytes-consumed-rate",
            &group_name,
            format!("The average number of bytes consumed per second for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            topic_tags.clone(),
        );
        let topic_bytes_consumed_total = MetricNameTemplate::new(
            "bytes-consumed-total",
            &group_name,
            format!("The total number of bytes consumed for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            topic_tags.clone(),
        );

        let topic_records_per_request_avg = MetricNameTemplate::new(
            "records-per-request-avg",
            &group_name,
            format!("The average number of records in each request for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            topic_tags.clone(),
        );
        let topic_records_consumed_rate = MetricNameTemplate::new(
            "records-consumed-rate",
            &group_name,
            format!(
                "The average number of records consumed per second for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"
            ),
            topic_tags.clone(),
        );
        let topic_records_consumed_total = MetricNameTemplate::new(
            "records-consumed-total",
            &group_name,
            format!("The total number of records consumed for a topic. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            topic_tags.clone(),
        );

        // Partition level.
        let mut partition_tags: IndexSet<String> = topic_tags.clone();
        partition_tags.insert("partition".to_string());

        let partition_records_lag = MetricNameTemplate::new(
            "records-lag",
            &group_name,
            format!("The latest lag of the partition. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            partition_tags.clone(),
        );
        let partition_records_lag_max = MetricNameTemplate::new(
            "records-lag-max",
            &group_name,
            format!("The max lag of the partition. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            partition_tags.clone(),
        );
        let partition_records_lag_avg = MetricNameTemplate::new(
            "records-lag-avg",
            &group_name,
            format!("The average lag of the partition. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            partition_tags.clone(),
        );
        let partition_records_lead = MetricNameTemplate::new(
            "records-lead",
            &group_name,
            format!("The latest lead of the partition. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            partition_tags.clone(),
        );
        let partition_records_lead_min = MetricNameTemplate::new(
            "records-lead-min",
            &group_name,
            format!("The min lead of the partition. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            partition_tags.clone(),
        );
        let partition_records_lead_avg = MetricNameTemplate::new(
            "records-lead-avg",
            &group_name,
            format!("The average lead of the partition. {DEPRECATED_TOPIC_METRICS_MESSAGE}"),
            partition_tags.clone(),
        );
        let partition_preferred_read_replica = MetricNameTemplate::new(
            "preferred-read-replica",
            &group_name,
            format!(
                "The current read replica for the partition, or -1 if reading from leader. {DEPRECATED_TOPIC_METRICS_MESSAGE}"
            ),
            partition_tags.clone(),
        );

        Self {
            fetch_size_avg,
            fetch_size_max,
            bytes_consumed_rate,
            bytes_consumed_total,
            records_per_request_avg,
            records_consumed_rate,
            records_consumed_total,
            fetch_latency_avg,
            fetch_latency_max,
            fetch_request_rate,
            fetch_request_total,
            records_lag_max,
            records_lead_min,
            fetch_throttle_time_avg,
            fetch_throttle_time_max,
            topic_fetch_size_avg,
            topic_fetch_size_max,
            topic_bytes_consumed_rate,
            topic_bytes_consumed_total,
            topic_records_per_request_avg,
            topic_records_consumed_rate,
            topic_records_consumed_total,
            partition_records_lag,
            partition_records_lag_max,
            partition_records_lag_avg,
            partition_records_lead,
            partition_records_lead_min,
            partition_records_lead_avg,
            partition_preferred_read_replica,
        }
    }

    /// Returns every template, mirroring Java's `getAllTemplates()`.
    #[cfg(test)]
    pub(crate) fn get_all_templates(&self) -> Vec<&MetricNameTemplate> {
        vec![
            &self.fetch_size_avg,
            &self.fetch_size_max,
            &self.bytes_consumed_rate,
            &self.bytes_consumed_total,
            &self.records_per_request_avg,
            &self.records_consumed_rate,
            &self.records_consumed_total,
            &self.fetch_latency_avg,
            &self.fetch_latency_max,
            &self.fetch_request_rate,
            &self.fetch_request_total,
            &self.records_lag_max,
            &self.records_lead_min,
            &self.fetch_throttle_time_avg,
            &self.fetch_throttle_time_max,
            &self.topic_fetch_size_avg,
            &self.topic_fetch_size_max,
            &self.topic_bytes_consumed_rate,
            &self.topic_bytes_consumed_total,
            &self.topic_records_per_request_avg,
            &self.topic_records_consumed_rate,
            &self.topic_records_consumed_total,
            &self.partition_records_lag,
            &self.partition_records_lag_avg,
            &self.partition_records_lag_max,
            &self.partition_records_lead,
            &self.partition_records_lead_min,
            &self.partition_records_lead_avg,
            &self.partition_preferred_read_replica,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `get_all_templates` returns every registered template (mirrors Java's
    /// `FetchMetricsRegistry.getAllTemplates()`).
    #[test]
    fn test_get_all_templates_returns_all_29() {
        let registry = FetchMetricsRegistry::new(IndexSet::new(), "consumer");
        assert_eq!(29, registry.get_all_templates().len());
        // The client-level group name carries the prefix.
        assert_eq!("consumer-fetch-manager-metrics", registry.fetch_size_avg.group());
        // Topic-level templates carry the `topic` tag.
        assert!(registry.topic_fetch_size_avg.tags().contains("topic"));
        // Partition-level templates carry `topic` and `partition` tags.
        assert!(registry.partition_records_lag.tags().contains("topic"));
        assert!(registry.partition_records_lag.tags().contains("partition"));
    }
}
