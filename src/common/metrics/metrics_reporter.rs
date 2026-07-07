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

//! A plugin that listens as metrics are created, changed, and removed.
//!
//! Translated from `org.apache.kafka.common.metrics.MetricsReporter`.

use std::sync::Arc;

use crate::common::metrics::{KafkaMetric, MetricsContext};

/// A plugin interface allowing implementations to observe metric lifecycle
/// events so they can report them.
///
/// Reporters are shared and invoked from the registry as metrics change, so
/// the callbacks take `&self` and rely on interior mutability. The reflective
/// `Configurable` / `Reconfigurable` surface of the Java interface is omitted:
/// this client constructs its single reporter directly rather than loading it
/// by class name.
pub trait MetricsReporter: Send + Sync {
    /// Registers all existing metrics when the reporter is first installed.
    fn init(&self, metrics: &[Arc<KafkaMetric>]);

    /// Called whenever a metric is added or changed.
    fn metric_change(&self, metric: Arc<KafkaMetric>);

    /// Called whenever a metric is removed.
    fn metric_removal(&self, metric: Arc<KafkaMetric>);

    /// Called when the metrics repository is closed.
    fn close(&self);

    /// Sets the context labels for the component exposing metrics. Called
    /// before [`init`](MetricsReporter::init) and possibly again afterward.
    fn context_change(&self, _metrics_context: &dyn MetricsContext) {}
}
