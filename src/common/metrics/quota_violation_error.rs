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

//! Error raised when a recorded value pushes a metric outside its quota.
//!
//! Translated from `org.apache.kafka.common.metrics.QuotaViolationException`.

use std::fmt;

use crate::common::metric::Metric;
use crate::common::metrics::KafkaMetric;
use crate::common::utils::double_to_string;

/// Raised when a sensor records a value that causes a metric to exceed the
/// bounds configured as its quota.
#[derive(Clone, Debug)]
pub struct QuotaViolationError {
    metric: KafkaMetric,
    value: f64,
    bound: f64,
}

impl QuotaViolationError {
    /// Creates a quota-violation error for the given metric, observed value,
    /// and violated bound.
    pub fn new(metric: KafkaMetric, value: f64, bound: f64) -> Self {
        Self { metric, value, bound }
    }

    /// The metric that violated its quota.
    pub fn metric(&self) -> &KafkaMetric {
        &self.metric
    }

    /// The observed value.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// The violated bound.
    pub fn bound(&self) -> f64 {
        self.bound
    }
}

impl fmt::Display for QuotaViolationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QuotaViolationError: '{}' violated quota. Actual: {}, Threshold: {}",
            self.metric.metric_name(),
            double_to_string(self.value),
            double_to_string(self.bound)
        )
    }
}

impl std::error::Error for QuotaViolationError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::common::MetricName;
    use crate::common::metrics::{MetricConfig, MetricValue, MetricValueProvider};

    struct ConstGauge(MetricValue);

    impl crate::common::metrics::Gauge for ConstGauge {
        fn value(&self, _config: &MetricConfig, _now: i64) -> MetricValue {
            self.0.clone()
        }
    }

    #[test]
    fn test_fields_and_display() {
        let metric = KafkaMetric::new(
            MetricName::new("m", "g", "d", indexmap::IndexMap::new()),
            MetricValueProvider::from_gauge(ConstGauge(MetricValue::Double(1.0))),
            MetricConfig::new(),
            Arc::new(|| 0),
        );
        let err = QuotaViolationError::new(metric, 5.6, 5.0);
        assert_eq!(err.value(), 5.6);
        assert_eq!(err.bound(), 5.0);
        let msg = err.to_string();
        assert!(
            msg.contains("violated quota. Actual: 5.6, Threshold: 5.0"),
            "unexpected message: {msg}"
        );
    }
}
