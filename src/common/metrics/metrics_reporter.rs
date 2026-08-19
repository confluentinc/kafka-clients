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

//! A plugin interface to allow listening as new metrics are created so they can
//! be reported (`org.apache.kafka.common.metrics.MetricsReporter`).

use std::sync::Arc;

use crate::common::metrics::KafkaMetric;

/// A plugin interface to allow things to listen as new metrics are created so
/// they can be reported.
///
/// Java's `MetricsReporter extends Reconfigurable, AutoCloseable` and (via
/// `JmxReporter`) bridges to JMX. JMX is JVM-only and out of scope; this trait
/// keeps the registration seam (`init`/`metric_change`/`metric_removal`/`close`)
/// so non-JMX reporters can be plugged in. All methods have a no-op default,
/// so a do-nothing reporter only needs an empty `impl`.
pub trait MetricsReporter: Send + Sync {
    /// This is called when the reporter is first registered to initially register
    /// all existing metrics.
    ///
    /// * `metrics` - All currently existing metrics
    fn init(&self, metrics: &[Arc<KafkaMetric>]) {
        let _ = metrics;
    }

    /// This is called whenever a metric is updated or added.
    ///
    /// * `metric` - The metric that has been added or changed
    fn metric_change(&self, metric: &Arc<KafkaMetric>) {
        let _ = metric;
    }

    /// This is called whenever a metric is removed.
    ///
    /// * `metric` - The metric that has been removed
    fn metric_removal(&self, metric: &Arc<KafkaMetric>) {
        let _ = metric;
    }

    /// Called when the metrics repository is closed.
    fn close(&self) {}
}
