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

//! The default [`MetricsContext`] implementation for Kafka clients.
//!
//! Translated from `org.apache.kafka.common.metrics.KafkaMetricsContext`.

use std::collections::HashMap;

use crate::common::metrics::metrics_context::{self, MetricsContext};

/// Holds the required metrics-context properties for Kafka services and
/// clients.
#[derive(Clone, Debug)]
pub struct KafkaMetricsContext {
    context_labels: HashMap<String, Option<String>>,
}

impl KafkaMetricsContext {
    /// Creates a context with the given namespace and no additional labels.
    pub fn new(namespace: impl Into<String>) -> Self {
        Self::with_labels(Some(namespace.into()), HashMap::new())
    }

    /// Creates a context with the given namespace and additional labels.
    ///
    /// A `None` namespace stores an absent value under the [`NAMESPACE`] key,
    /// and absent label values are preserved as-is.
    ///
    /// [`NAMESPACE`]: crate::common::metrics::metrics_context::NAMESPACE
    pub fn with_labels(namespace: Option<String>, context_labels: HashMap<String, Option<String>>) -> Self {
        let mut labels = HashMap::with_capacity(context_labels.len() + 1);
        labels.insert(metrics_context::NAMESPACE.to_string(), namespace);
        for (key, value) in context_labels {
            labels.insert(key, value);
        }
        Self { context_labels: labels }
    }
}

impl MetricsContext for KafkaMetricsContext {
    fn context_labels(&self) -> &HashMap<String, Option<String>> {
        &self.context_labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::metrics_context::NAMESPACE;

    const SAMPLE_NAMESPACE: &str = "sample-ns";
    const LABEL_A_KEY: &str = "label-a";
    const LABEL_A_VALUE: &str = "label-a-value";

    fn labels() -> HashMap<String, Option<String>> {
        let mut m = HashMap::new();
        m.insert(LABEL_A_KEY.to_string(), Some(LABEL_A_VALUE.to_string()));
        m
    }

    #[test]
    fn test_creation_with_valid_namespace_and_no_labels() {
        let context = KafkaMetricsContext::with_labels(Some(SAMPLE_NAMESPACE.to_string()), HashMap::new());
        assert_eq!(context.context_labels().len(), 1);
        assert_eq!(
            context.context_labels().get(NAMESPACE),
            Some(&Some(SAMPLE_NAMESPACE.to_string()))
        );
    }

    #[test]
    fn test_creation_with_valid_namespace_and_labels() {
        let context = KafkaMetricsContext::with_labels(Some(SAMPLE_NAMESPACE.to_string()), labels());
        assert_eq!(context.context_labels().len(), 2);
        assert_eq!(
            context.context_labels().get(NAMESPACE),
            Some(&Some(SAMPLE_NAMESPACE.to_string()))
        );
        assert_eq!(
            context.context_labels().get(LABEL_A_KEY),
            Some(&Some(LABEL_A_VALUE.to_string()))
        );
    }

    #[test]
    fn test_creation_with_valid_namespace_and_null_label_values() {
        let mut input = labels();
        input.insert(LABEL_A_KEY.to_string(), None);
        let context = KafkaMetricsContext::with_labels(Some(SAMPLE_NAMESPACE.to_string()), input);
        assert_eq!(context.context_labels().len(), 2);
        assert_eq!(
            context.context_labels().get(NAMESPACE),
            Some(&Some(SAMPLE_NAMESPACE.to_string()))
        );
        // The key is present with an absent value.
        assert_eq!(context.context_labels().get(LABEL_A_KEY), Some(&None));
    }

    #[test]
    fn test_creation_with_null_namespace_and_labels() {
        let context = KafkaMetricsContext::with_labels(None, labels());
        assert_eq!(context.context_labels().len(), 2);
        assert_eq!(context.context_labels().get(NAMESPACE), Some(&None));
        assert_eq!(
            context.context_labels().get(LABEL_A_KEY),
            Some(&Some(LABEL_A_VALUE.to_string()))
        );
    }

    #[test]
    fn test_new_single_arg_sets_namespace() {
        let context = KafkaMetricsContext::new(SAMPLE_NAMESPACE);
        assert_eq!(context.context_labels().len(), 1);
        assert_eq!(
            context.context_labels().get(NAMESPACE),
            Some(&Some(SAMPLE_NAMESPACE.to_string()))
        );
    }
}
