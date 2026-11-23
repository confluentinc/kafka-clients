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

//! Incremental share-fetch metric aggregation
//! (`org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator`).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::common::TopicPartition;
use crate::consumer::internals::ShareFetchMetricsManager;

/// Since we parse the message data for each partition from each share fetch
/// response lazily, fetch-level metrics need to be aggregated as the messages
/// from each partition are parsed. This class facilitates that incremental
/// aggregation.
///
/// Created once per share fetch response and shared (`Arc`) across that
/// response's `ShareCompletedFetch`es, each of which calls [`Self::record`] when
/// it is drained; once every partition has reported, the totals are written to
/// the [`ShareFetchMetricsManager`] once. Unlike [`super::FetchMetricsAggregator`]
/// there are no per-topic metrics (Java's share aggregator has none).
///
/// Java mutates the aggregator without a lock: the completed fetches of one
/// response are drained by the application thread, and on close by whichever
/// thread closes the buffer. Here the state is behind a `Mutex` because the
/// type must be `Sync` to be shared through `Arc` across the bg task (which
/// creates it) and the app task (which drains); the lock is taken once per
/// partition, never per record, and never held while the manager records.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator")]
pub(crate) struct ShareFetchMetricsAggregator {
    share_fetch_metrics_manager: Arc<ShareFetchMetricsManager>,
    inner: Mutex<Inner>,
}

/// The aggregator's mutable state: Java's `fetchMetrics` and
/// `unrecordedPartitions` fields.
struct Inner {
    fetch_metrics: FetchMetrics,
    unrecorded_partitions: HashSet<TopicPartition>,
}

/// Running totals at the fetch level.
#[derive(Default)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator$FetchMetrics")]
struct FetchMetrics {
    bytes: i32,
    records: i32,
}

impl FetchMetrics {
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator$FetchMetrics#increment")]
    fn increment(&mut self, bytes: i32, records: i32) {
        self.bytes += bytes;
        self.records += records;
    }
}

impl ShareFetchMetricsAggregator {
    /// Creates an aggregator tracking the given partitions of one share fetch
    /// response.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator#ShareFetchMetricsAggregator"
    )]
    pub(crate) fn new(
        share_fetch_metrics_manager: Arc<ShareFetchMetricsManager>,
        partitions: HashSet<TopicPartition>,
    ) -> Self {
        Self {
            share_fetch_metrics_manager,
            inner: Mutex::new(Inner { fetch_metrics: FetchMetrics::default(), unrecorded_partitions: partitions }),
        }
    }

    /// After each partition is parsed, updates the current metric totals with
    /// the total bytes and number of records parsed. After all partitions have
    /// reported, writes the metrics.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator#record")]
    pub(crate) fn record(&self, partition: &TopicPartition, bytes: i32, records: i32) {
        let totals = {
            let mut inner = self.inner.lock().expect("ShareFetchMetricsAggregator mutex poisoned");
            // Aggregate the metrics at the fetch level
            inner.fetch_metrics.increment(bytes, records);
            Self::maybe_record_metrics(&mut inner, partition)
        };
        if let Some((bytes, records)) = totals {
            // Record the metrics aggregated at the fetch level, outside the lock.
            self.share_fetch_metrics_manager.record_records_fetched(records);
            self.share_fetch_metrics_manager.record_bytes_fetched(bytes);
        }
    }

    /// Java's `maybeRecordMetrics(TopicPartition)`: marks the partition as
    /// reported and, once none is left, returns the fetch-level `(bytes,
    /// records)` totals for [`Self::record`] to write after releasing the lock.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchMetricsAggregator#maybeRecordMetrics")]
    fn maybe_record_metrics(inner: &mut Inner, partition: &TopicPartition) -> Option<(i32, i32)> {
        inner.unrecorded_partitions.remove(partition);
        if !inner.unrecorded_partitions.is_empty() {
            return None;
        }
        Some((inner.fetch_metrics.bytes, inner.fetch_metrics.records))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Metrics;
    use crate::common::{Metric, MetricValue};
    use crate::consumer::internals::ShareFetchMetricsRegistry;

    fn total(metrics: &Metrics, template: &crate::common::MetricNameTemplate) -> MetricValue {
        let name = metrics.metric_instance_with_key_value(template, &[]).unwrap();
        metrics.metric(&name).unwrap().metric_value()
    }

    /// Rust-only (Java has no `ShareFetchMetricsAggregatorTest`): the fetch-level
    /// bytes and records are recorded once, after every partition has
    /// reported, and a partition reporting twice does not record early.
    #[test]
    fn test_records_once_after_all_partitions() {
        let metrics = Arc::new(Metrics::new());
        let registry = ShareFetchMetricsRegistry::with_metric_grp_prefix("consumer-share");
        let manager = Arc::new(ShareFetchMetricsManager::new(Arc::clone(&metrics), &registry));

        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        let aggregator = ShareFetchMetricsAggregator::new(manager, HashSet::from([tp0.clone(), tp1.clone()]));

        aggregator.record(&tp0, 10, 2);
        aggregator.record(&tp0, 5, 1);
        assert_eq!(MetricValue::Double(0.0), total(&metrics, &registry.bytes_fetched_total));
        assert_eq!(MetricValue::Double(0.0), total(&metrics, &registry.records_fetched_total));

        aggregator.record(&tp1, 30, 4);
        assert_eq!(MetricValue::Double(45.0), total(&metrics, &registry.bytes_fetched_total));
        assert_eq!(MetricValue::Double(7.0), total(&metrics, &registry.records_fetched_total));
        assert_eq!(MetricValue::Double(7.0), total(&metrics, &registry.records_per_request_max));
    }
}
