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

//! The metric name templates of the share consumer
//! (`org.apache.kafka.clients.consumer.internals.ShareConsumerMetrics`).

use indexmap::IndexSet;

use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::ShareFetchMetricsRegistry;

/// Holds the share consumer's [`ShareFetchMetricsRegistry`]. Java's public
/// field `shareFetchMetrics` is a `pub(crate)` field (the type is
/// crate-private).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetrics")]
pub(crate) struct ShareConsumerMetrics {
    /// Java: `shareFetchMetrics`.
    pub(crate) share_fetch_metrics: ShareFetchMetricsRegistry,
}

impl ShareConsumerMetrics {
    /// The templates with no tags and the `consumer-share` group prefix:
    /// Java's `ShareConsumerMetrics()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetrics#ShareConsumerMetrics")]
    pub(crate) fn new() -> Self {
        Self::with_metrics_tags_metric_grp_prefix(IndexSet::new(), ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX)
    }

    /// The templates with `metrics_tags` and `metric_grp_prefix`. Java takes a
    /// `Set`; the `IndexSet` keeps the insertion order, as
    /// [`ShareFetchMetricsRegistry::with_tags_metric_grp_prefix`] does.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetrics#ShareConsumerMetrics")]
    pub(crate) fn with_metrics_tags_metric_grp_prefix(metrics_tags: IndexSet<String>, metric_grp_prefix: &str) -> Self {
        Self {
            share_fetch_metrics: ShareFetchMetricsRegistry::with_tags_metric_grp_prefix(
                metrics_tags,
                metric_grp_prefix,
            ),
        }
    }
}

impl Default for ShareConsumerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `ShareConsumerMetricsTest`. Rust-only: the no-argument
    //! constructor uses the `consumer-share` prefix and no tags.

    use super::*;

    #[test]
    fn new_uses_the_consumer_share_prefix_and_no_tags() {
        let registry = ShareConsumerMetrics::new().share_fetch_metrics;
        assert_eq!("consumer-share-fetch-manager-metrics", registry.fetch_size_avg.group());
        assert!(registry.fetch_size_avg.tags().is_empty());
    }
}
