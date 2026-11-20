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

//! Templates for the share consumer's fetch-manager metrics
//! (`org.apache.kafka.clients.consumer.internals.ShareFetchMetricsRegistry`).

use indexmap::IndexSet;

use crate::common::MetricNameTemplate;

/// Holds the [`MetricNameTemplate`]s for every share-fetch-manager metric, all
/// client level, in the `<prefix>-fetch-manager-metrics` group. Mirrors Java's
/// `ShareFetchMetricsRegistry`, whose public fields become `pub(crate)` fields
/// here (the type is crate-private).
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsRegistry")]
pub(crate) struct ShareFetchMetricsRegistry {
    /// Java: `fetchSizeAvg`.
    pub(crate) fetch_size_avg: MetricNameTemplate,
    /// Java: `fetchSizeMax`.
    pub(crate) fetch_size_max: MetricNameTemplate,
    /// Java: `bytesFetchedRate`.
    pub(crate) bytes_fetched_rate: MetricNameTemplate,
    /// Java: `bytesFetchedTotal`.
    pub(crate) bytes_fetched_total: MetricNameTemplate,
    /// Java: `recordsPerRequestAvg`.
    pub(crate) records_per_request_avg: MetricNameTemplate,
    /// Java: `recordsPerRequestMax`.
    pub(crate) records_per_request_max: MetricNameTemplate,
    /// Java: `recordsFetchedRate`.
    pub(crate) records_fetched_rate: MetricNameTemplate,
    /// Java: `recordsFetchedTotal`.
    pub(crate) records_fetched_total: MetricNameTemplate,
    /// Java: `acknowledgementSendRate`.
    pub(crate) acknowledgement_send_rate: MetricNameTemplate,
    /// Java: `acknowledgementSendTotal`.
    pub(crate) acknowledgement_send_total: MetricNameTemplate,
    /// Java: `acknowledgementErrorRate`.
    pub(crate) acknowledgement_error_rate: MetricNameTemplate,
    /// Java: `acknowledgementErrorTotal`.
    pub(crate) acknowledgement_error_total: MetricNameTemplate,
    /// Java: `fetchLatencyAvg`.
    pub(crate) fetch_latency_avg: MetricNameTemplate,
    /// Java: `fetchLatencyMax`.
    pub(crate) fetch_latency_max: MetricNameTemplate,
    /// Java: `fetchRequestRate`.
    pub(crate) fetch_request_rate: MetricNameTemplate,
    /// Java: `fetchRequestTotal`.
    pub(crate) fetch_request_total: MetricNameTemplate,
    /// Java: `fetchThrottleTimeAvg`.
    pub(crate) fetch_throttle_time_avg: MetricNameTemplate,
    /// Java: `fetchThrottleTimeMax`.
    pub(crate) fetch_throttle_time_max: MetricNameTemplate,
}

impl ShareFetchMetricsRegistry {
    /// Creates the registry with no tags and an empty group prefix. Translates
    /// Java's `ShareFetchMetricsRegistry()`, which delegates to
    /// `this(new HashSet<>(), "")`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsRegistry#ShareFetchMetricsRegistry")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java uses it only in its tests"))]
    pub(crate) fn new() -> Self {
        Self::with_tags_metric_grp_prefix(IndexSet::new(), "")
    }

    /// Creates the registry with no tags and the given group prefix. Translates
    /// Java's `ShareFetchMetricsRegistry(String metricGrpPrefix)`, which
    /// delegates to `this(new HashSet<>(), metricGrpPrefix)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsRegistry#ShareFetchMetricsRegistry")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java uses it only in its tests"))]
    pub(crate) fn with_metric_grp_prefix(metric_grp_prefix: &str) -> Self {
        Self::with_tags_metric_grp_prefix(IndexSet::new(), metric_grp_prefix)
    }

    /// Creates the registry with the given tag set and group prefix. Translates
    /// Java's `ShareFetchMetricsRegistry(Set<String> tags, String
    /// metricGrpPrefix)`. Java takes a `Set`; `IndexSet` keeps the insertion
    /// order, as `FetchMetricsRegistry::new` does.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsRegistry#ShareFetchMetricsRegistry")]
    pub(crate) fn with_tags_metric_grp_prefix(tags: IndexSet<String>, metric_grp_prefix: &str) -> Self {
        // Client level.
        let group_name = format!("{metric_grp_prefix}-fetch-manager-metrics");
        Self {
            fetch_size_avg: MetricNameTemplate::new(
                "fetch-size-avg",
                &group_name,
                "The average number of bytes fetched per request",
                tags.clone(),
            ),
            fetch_size_max: MetricNameTemplate::new(
                "fetch-size-max",
                &group_name,
                "The maximum number of bytes fetched per request",
                tags.clone(),
            ),
            bytes_fetched_rate: MetricNameTemplate::new(
                "bytes-consumed-rate",
                &group_name,
                "The average number of bytes consumed per second",
                tags.clone(),
            ),
            bytes_fetched_total: MetricNameTemplate::new(
                "bytes-consumed-total",
                &group_name,
                "The total number of bytes consumed",
                tags.clone(),
            ),
            records_per_request_avg: MetricNameTemplate::new(
                "records-per-request-avg",
                &group_name,
                "The average number of records in each request",
                tags.clone(),
            ),
            records_per_request_max: MetricNameTemplate::new(
                "records-per-request-max",
                &group_name,
                "The maximum number of records in a request.",
                tags.clone(),
            ),
            records_fetched_rate: MetricNameTemplate::new(
                "records-consumed-rate",
                &group_name,
                "The average number of records consumed per second",
                tags.clone(),
            ),
            records_fetched_total: MetricNameTemplate::new(
                "records-consumed-total",
                &group_name,
                "The total number of records consumed",
                tags.clone(),
            ),
            acknowledgement_send_rate: MetricNameTemplate::new(
                "acknowledgements-send-rate",
                &group_name,
                "The average number of record acknowledgements sent per second.",
                tags.clone(),
            ),
            acknowledgement_send_total: MetricNameTemplate::new(
                "acknowledgements-send-total",
                &group_name,
                "The total number of record acknowledgements sent.",
                tags.clone(),
            ),
            acknowledgement_error_rate: MetricNameTemplate::new(
                "acknowledgements-error-rate",
                &group_name,
                "The average number of record acknowledgements that resulted in errors per second.",
                tags.clone(),
            ),
            acknowledgement_error_total: MetricNameTemplate::new(
                "acknowledgements-error-total",
                &group_name,
                "The total number of record acknowledgements that resulted in errors.",
                tags.clone(),
            ),
            fetch_latency_avg: MetricNameTemplate::new(
                "fetch-latency-avg",
                &group_name,
                "The average time taken for a fetch request.",
                tags.clone(),
            ),
            fetch_latency_max: MetricNameTemplate::new(
                "fetch-latency-max",
                &group_name,
                "The max time taken for any fetch request.",
                tags.clone(),
            ),
            fetch_request_rate: MetricNameTemplate::new(
                "fetch-rate",
                &group_name,
                "The number of fetch requests per second.",
                tags.clone(),
            ),
            fetch_request_total: MetricNameTemplate::new(
                "fetch-total",
                &group_name,
                "The total number of fetch requests.",
                tags.clone(),
            ),
            fetch_throttle_time_avg: MetricNameTemplate::new(
                "fetch-throttle-time-avg",
                &group_name,
                "The average throttle time in ms",
                tags.clone(),
            ),
            fetch_throttle_time_max: MetricNameTemplate::new(
                "fetch-throttle-time-max",
                &group_name,
                "The maximum throttle time in ms",
                tags.clone(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every template is in the prefixed client-level group and carries the
    /// given tags; names match Java's verbatim.
    #[test]
    fn test_templates_use_the_prefixed_group_and_tags() {
        let mut tags = IndexSet::new();
        tags.insert("client-id".to_string());
        let registry = ShareFetchMetricsRegistry::with_tags_metric_grp_prefix(tags, "consumer-share");
        let all = [
            (&registry.fetch_size_avg, "fetch-size-avg"),
            (&registry.fetch_size_max, "fetch-size-max"),
            (&registry.bytes_fetched_rate, "bytes-consumed-rate"),
            (&registry.bytes_fetched_total, "bytes-consumed-total"),
            (&registry.records_per_request_avg, "records-per-request-avg"),
            (&registry.records_per_request_max, "records-per-request-max"),
            (&registry.records_fetched_rate, "records-consumed-rate"),
            (&registry.records_fetched_total, "records-consumed-total"),
            (&registry.acknowledgement_send_rate, "acknowledgements-send-rate"),
            (&registry.acknowledgement_send_total, "acknowledgements-send-total"),
            (&registry.acknowledgement_error_rate, "acknowledgements-error-rate"),
            (&registry.acknowledgement_error_total, "acknowledgements-error-total"),
            (&registry.fetch_latency_avg, "fetch-latency-avg"),
            (&registry.fetch_latency_max, "fetch-latency-max"),
            (&registry.fetch_request_rate, "fetch-rate"),
            (&registry.fetch_request_total, "fetch-total"),
            (&registry.fetch_throttle_time_avg, "fetch-throttle-time-avg"),
            (&registry.fetch_throttle_time_max, "fetch-throttle-time-max"),
        ];
        for (template, name) in all {
            assert_eq!(name, template.name());
            assert_eq!("consumer-share-fetch-manager-metrics", template.group());
            assert!(template.tags().contains("client-id"));
            assert_eq!(1, template.tags().len());
        }
    }

    /// The no-argument and prefix-only constructors delegate with an empty tag
    /// set (and, for the former, an empty prefix), as Java's do.
    #[test]
    fn test_delegating_constructors() {
        let registry = ShareFetchMetricsRegistry::new();
        assert_eq!("-fetch-manager-metrics", registry.fetch_size_avg.group());
        assert!(registry.fetch_size_avg.tags().is_empty());
        let registry = ShareFetchMetricsRegistry::with_metric_grp_prefix("p");
        assert_eq!("p-fetch-manager-metrics", registry.fetch_throttle_time_max.group());
        assert!(registry.fetch_throttle_time_max.tags().is_empty());
    }
}
