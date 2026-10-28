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
/// Implement `ClusterResourceListener` to receive cluster metadata once it's
/// available. Please see the class documentation for `ClusterResourceListener`
/// for more information.
///
/// # Translation notes
///
/// WARNING: implementing `ClusterResourceListener` delivers no cluster
/// metadata: no client takes a reporter, as `metric.reporters` is class-valued.
// Java's `MetricsReporter extends Reconfigurable, AutoCloseable` and (via
// `JmxReporter`) bridges to JMX, which is JVM-only and out of scope. Every
// method has a no-op default, so a do-nothing reporter needs an empty `impl`.
#[doc(alias = "org.apache.kafka.common.metrics.MetricsReporter")]
pub trait MetricsReporter: Send + Sync {
    /// This is called when the reporter is first registered to initially register
    /// all existing metrics.
    ///
    /// # Arguments
    ///
    /// - `metrics` - All currently existing metrics
    #[doc(alias = "org.apache.kafka.common.metrics.MetricsReporter#init")]
    fn init(&self, metrics: &[Arc<KafkaMetric>]) {
        let _ = metrics;
    }

    /// This is called whenever a metric is updated or added.
    ///
    /// # Arguments
    ///
    /// - `metric` - The metric that has been added or changed
    #[doc(alias = "org.apache.kafka.common.metrics.MetricsReporter#metricChange")]
    fn metric_change(&self, metric: &Arc<KafkaMetric>) {
        let _ = metric;
    }

    /// This is called whenever a metric is removed.
    ///
    /// # Arguments
    ///
    /// - `metric` - The metric that has been removed
    #[doc(alias = "org.apache.kafka.common.metrics.MetricsReporter#metricRemoval")]
    fn metric_removal(&self, metric: &Arc<KafkaMetric>) {
        let _ = metric;
    }

    /// Called when the metrics repository is closed.
    #[doc(alias = "org.apache.kafka.common.metrics.MetricsReporter#close")]
    fn close(&self) {}
}
