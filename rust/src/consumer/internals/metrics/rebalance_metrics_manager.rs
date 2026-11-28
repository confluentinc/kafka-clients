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

//! The rebalance metrics a membership manager records
//! (`org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager`).
//!
//! Java's `RebalanceMetricsManager` is an abstract class extended by
//! `ConsumerRebalanceMetricsManager` and `ShareRebalanceMetricsManager`;
//! `AbstractMembershipManager` holds whichever one the group type supplies.
//! Rust models the abstract methods as this trait, held by the membership
//! manager as `Arc<dyn RebalanceMetricsManager>`.
//!
//! Two members of the Java base are not part of the trait:
//!
//! - `AbstractConsumerMetricsManager` (the Java superclass) is an embeddable
//!   struct in Rust; each implementation composes it, as
//!   [`super::ConsumerRebalanceMetricsManager`] already does.
//! - The `metricGroupName` field and the `createMetric(name, description)`
//!   helper only serve the subclass constructors, which pass the group name to
//!   `MetricsLedger::metric_name` directly.

/// Records the start, end and failure of a group rebalance.
///
/// Java: `abstract class RebalanceMetricsManager extends AbstractConsumerMetricsManager`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager")]
pub(crate) trait RebalanceMetricsManager: Send + Sync + 'static {
    /// Records that a rebalance started at `now_ms`.
    ///
    /// Java: `public abstract void recordRebalanceStarted(long nowMs)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager#recordRebalanceStarted")]
    fn record_rebalance_started(&self, now_ms: i64);

    /// Records that a rebalance ended at `now_ms`.
    ///
    /// Java: `public abstract void recordRebalanceEnded(long nowMs)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager#recordRebalanceEnded")]
    fn record_rebalance_ended(&self, now_ms: i64);

    /// Records a failed rebalance, if one is in progress. The base
    /// implementation does nothing.
    ///
    /// Java: `public void maybeRecordRebalanceFailed()` (an empty body).
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager#maybeRecordRebalanceFailed"
    )]
    fn maybe_record_rebalance_failed(&self) {}

    /// Returns `true` if a rebalance was started and has not ended yet.
    ///
    /// Java: `public abstract boolean rebalanceStarted()`. As in Java, the
    /// membership state machine does not call it; only the metrics tests do.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager#rebalanceStarted")]
    #[cfg_attr(not(test), expect(dead_code))]
    fn rebalance_started(&self) -> bool;
}
