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

//! Incremental fetch-metric aggregation
//! (`org.apache.kafka.clients.consumer.internals.FetchMetricsAggregator`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::common::TopicPartition;
use crate::consumer::internals::fetch_metrics_manager::FetchMetricsManager;

/// Since we parse the message data for each partition from each fetch response
/// lazily, fetch-level metrics need to be aggregated as the messages from each
/// partition are parsed. This class facilitates that incremental aggregation.
///
/// Created once per fetch response and shared (`Arc`) across that response's
/// `CompletedFetch`es. Each `CompletedFetch::drain` calls [`Self::record`]; once
/// every partition has reported, the aggregated totals are written to the
/// [`FetchMetricsManager`] exactly once (Java's contract). The interior state is
/// behind a `Mutex` because the `CompletedFetch`es of one response may be
/// drained on the poll task while the manager is owned by the bg task.
pub(crate) struct FetchMetricsAggregator {
    metrics_manager: Arc<FetchMetricsManager>,
    inner: Mutex<Inner>,
}

struct Inner {
    unrecorded_partitions: HashSet<TopicPartition>,
    fetch_metrics: FetchMetrics,
    per_topic_fetch_metrics: HashMap<String, FetchMetrics>,
}

#[derive(Default)]
struct FetchMetrics {
    bytes: i32,
    records: i32,
}

impl FetchMetrics {
    fn increment(&mut self, bytes: i32, records: i32) {
        self.bytes += bytes;
        self.records += records;
    }
}

impl FetchMetricsAggregator {
    /// Creates an aggregator tracking the given partitions for one fetch
    /// response.
    pub(crate) fn new(metrics_manager: Arc<FetchMetricsManager>, partitions: HashSet<TopicPartition>) -> Self {
        Self {
            metrics_manager,
            inner: Mutex::new(Inner {
                unrecorded_partitions: partitions,
                fetch_metrics: FetchMetrics::default(),
                per_topic_fetch_metrics: HashMap::new(),
            }),
        }
    }

    /// After each partition is parsed, updates the current metric totals with
    /// the total bytes and records parsed. After all partitions have reported,
    /// the metrics are written exactly once.
    pub(crate) fn record(&self, partition: &TopicPartition, bytes: i32, records: i32) {
        // Aggregate the metrics at the fetch level and per-topic, then check
        // whether every partition has now reported.
        let recordings = {
            let mut inner = self.inner.lock().expect("FetchMetricsAggregator mutex poisoned");
            inner.fetch_metrics.increment(bytes, records);
            inner
                .per_topic_fetch_metrics
                .entry(partition.topic().to_string())
                .or_default()
                .increment(bytes, records);

            inner.unrecorded_partitions.remove(partition);
            if !inner.unrecorded_partitions.is_empty() {
                None
            } else {
                // Snapshot the totals so we can record OUTSIDE the lock.
                let fetch_totals = (inner.fetch_metrics.bytes, inner.fetch_metrics.records);
                let per_topic: Vec<(String, i32, i32)> = inner
                    .per_topic_fetch_metrics
                    .iter()
                    .map(|(topic, m)| (topic.clone(), m.bytes, m.records))
                    .collect();
                Some((fetch_totals, per_topic))
            }
        };

        if let Some(((bytes, records), per_topic)) = recordings {
            // Record the metrics aggregated at the fetch level.
            self.metrics_manager.record_bytes_fetched(bytes);
            self.metrics_manager.record_records_fetched(records);

            // Also record the metrics aggregated on a per-topic basis.
            for (topic, topic_bytes, topic_records) in per_topic {
                self.metrics_manager.record_bytes_fetched_topic(&topic, topic_bytes);
                self.metrics_manager.record_records_fetched_topic(&topic, topic_records);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metric::Metric;
    use crate::common::metrics::MetricValue;

    /// The aggregator records the fetch-level bytes/records exactly once, after
    /// every partition of the response has reported via `record`.
    #[test]
    fn test_aggregator_records_once_after_all_partitions() {
        let manager = FetchMetricsManager::for_test();
        let metrics = manager.metrics_for_test();
        let registry = manager.registry_for_test();

        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        let mut partitions = HashSet::new();
        partitions.insert(tp0.clone());
        partitions.insert(tp1.clone());
        let aggregator = FetchMetricsAggregator::new(Arc::clone(&manager), partitions);

        // First partition reported: nothing recorded yet (still one unrecorded).
        aggregator.record(&tp0, 10, 2);
        let total_name = metrics.metric_instance(&registry.bytes_consumed_total, &[]).unwrap();
        assert_eq!(MetricValue::Double(0.0), metrics.metric(&total_name).unwrap().metric_value());

        // Last partition reported: fetch-level totals recorded once (10 + 30 = 40 bytes).
        aggregator.record(&tp1, 30, 4);
        assert_eq!(MetricValue::Double(40.0), metrics.metric(&total_name).unwrap().metric_value());

        let records_total_name = metrics.metric_instance(&registry.records_consumed_total, &[]).unwrap();
        assert_eq!(
            MetricValue::Double(6.0),
            metrics.metric(&records_total_name).unwrap().metric_value()
        );
    }
}
