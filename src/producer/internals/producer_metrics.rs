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

//! Aggregates the producer's metric-name registries
//! (`org.apache.kafka.clients.producer.internals.ProducerMetrics`).

use std::sync::Arc;

#[cfg(test)]
use crate::common::MetricNameTemplate;
use crate::common::metrics::Metrics;
use crate::producer::internals::sender_metrics_registry::SenderMetricsRegistry;

/// Aggregates the producer's metric-name registries. Currently just wraps the
/// [`SenderMetricsRegistry`], mirroring Java's `ProducerMetrics`.
pub(crate) struct ProducerMetrics {
    pub(crate) sender_metrics: SenderMetricsRegistry,
}

impl ProducerMetrics {
    /// Builds the aggregate registry. Translates Java's `ProducerMetrics(Metrics)`.
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        Self { sender_metrics: SenderMetricsRegistry::new(metrics) }
    }

    /// Returns every metric-name template across all registries. Translates
    /// Java's private `getAllTemplates()` (used by its `main` to emit the docs
    /// table); exposed here for parity/testing.
    #[cfg(test)]
    pub(crate) fn all_templates(&self) -> Vec<MetricNameTemplate> {
        self.sender_metrics.all_templates().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::common::metrics::MetricConfig;

    /// `ProducerMetrics` exposes the sender registry's templates unchanged.
    #[test]
    fn test_all_templates_delegates_to_sender_registry() {
        let mut tags = BTreeMap::new();
        tags.insert("client-id".to_string(), "client-id".to_string());
        let metrics = Arc::new(Metrics::new_default_config(Arc::new(MetricConfig::new().with_tags(tags))));
        let producer_metrics = ProducerMetrics::new(metrics);
        // 22 client-level + 9 topic-level templates.
        assert_eq!(31, producer_metrics.all_templates().len());
    }
}
