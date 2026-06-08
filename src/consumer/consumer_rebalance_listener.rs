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

//! Callback interface notified when the set of partitions assigned to the
//! consumer changes.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.ConsumerRebalanceListener`.

use async_trait::async_trait;

use crate::common::{KafkaError, TopicPartition};

/// A callback interface that the user can implement to trigger custom actions
/// when the set of partitions assigned to the consumer changes.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.ConsumerRebalanceListener`.
///
/// # Invocation model
///
/// Per `consumer-threading.md` §31, listener methods execute on the **caller's
/// task** during `poll()`, `commit_*()`, `unsubscribe()`, or `close()` — never
/// on the background task. This matches Java's contract that the callback
/// "executes in the user thread as part of the poll() call".
///
/// Users may invoke other `Consumer` methods (notably `commit_sync()`) from
/// inside the callback; rebalance state does not advance until the callback
/// future resolves.
///
/// # Bounds: `Send + Sync + 'static`
///
/// The listener is stored as `Arc<dyn ConsumerRebalanceListener>` inside
/// `SubscriptionState` (which itself lives behind `Arc<Mutex<...>>` per
/// `consumer-threading.md` §16). The invocation pattern in §31 clones the
/// `Arc<dyn>` out of the lock *before* `.await` (per §16, the guard must be
/// dropped before awaiting). Cloning an `Arc<dyn Trait>` across tasks
/// requires `Arc<dyn Trait>: Send`, which requires the trait to be
/// `Send + Sync`. `Box<dyn>` would forbid the clone and the design collapses.
///
/// # Async
///
/// `#[async_trait]` is correct here — listener invocation is per-rebalance,
/// not per-record; the cost of one `Box<Future>` per callback is irrelevant.
/// Per CLAUDE.md §11, `#[async_trait]` is only forbidden on per-record hot
/// paths.
#[async_trait]
pub trait ConsumerRebalanceListener: Send + Sync + 'static {
    /// A callback method the user can implement to provide handling of offset
    /// commits to a customized store. This method will be called during a
    /// rebalance operation when the consumer has to give up some partitions.
    ///
    /// It is recommended that offsets should be committed in this callback to
    /// either Kafka or a custom offset store to prevent duplicate data.
    ///
    /// Corresponds to Java's
    /// `void onPartitionsRevoked(Collection<TopicPartition> partitions)`.
    ///
    /// The Java method is `void` but can throw checked / unchecked
    /// exceptions; per CLAUDE.md §10 we convert this to
    /// `Result<(), KafkaError>`.
    ///
    /// `partitions` is `&[TopicPartition]` rather than
    /// `Vec<TopicPartition>` per CLAUDE.md §12 — accept the most general
    /// borrowed form. Implementors who need ownership can `to_vec()`.
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    /// A callback method the user can implement to provide handling of
    /// customized offsets on completion of a successful partition
    /// re-assignment. This method will be called after the partition
    /// re-assignment completes (even if no new partitions were assigned to
    /// the consumer), and before the consumer starts fetching data.
    ///
    /// Corresponds to Java's
    /// `void onPartitionsAssigned(Collection<TopicPartition> partitions)`.
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    /// A callback method you can implement to provide handling of cleaning
    /// up resources for partitions that have already been reassigned to
    /// other consumers. This method will not be called during normal
    /// execution as the owned partitions would first be revoked by calling
    /// `on_partitions_revoked` before being reassigned to other consumers
    /// during a rebalance event.
    ///
    /// The default implementation delegates to `on_partitions_revoked` —
    /// matching Java's
    /// `default void onPartitionsLost(Collection<TopicPartition> partitions) {
    ///     onPartitionsRevoked(partitions);
    /// }`.
    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.on_partitions_revoked(partitions).await
    }
}
