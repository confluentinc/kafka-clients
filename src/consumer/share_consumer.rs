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

//! The public `ShareConsumer<K, V>` dispatch trait (KIP-932).
//!
//! Translates `org.apache.kafka.clients.consumer.ShareConsumer` (Apache Kafka
//! 4.2). Like [`crate::consumer::Consumer`], it is a single `#[async_trait]`
//! trait (consumer-threading.md §2): methods that block in Java become `async`
//! in Rust, methods that do not block stay synchronous.
//!
//! ## async vs. sync (per §1)
//!
//! | method | Java blocking? | Rust |
//! |--------|----------------|------|
//! | `subscription` | no (state read) | sync |
//! | `subscribe` | yes (`addAndGet(ShareSubscriptionChangeEvent)`) | async |
//! | `unsubscribe` | yes (`addAndGet(ShareUnsubscribeEvent)`) | async |
//! | `poll` | yes | async |
//! | `acknowledge*` | no (records intent in `currentFetch`) | sync |
//! | `commit_sync` | yes | async |
//! | `commit_async` | no network wait, but drains the ack-callback queue (async callback) | async |
//! | `set_acknowledgement_commit_callback` | no | sync |
//! | `client_instance_id` | yes (telemetry fetch) | async |
//! | `acquisition_lock_timeout_ms` | no (state read) | sync |
//! | `close` | yes | async |
//! | `wakeup` | no | sync |
//!
//! ## Metrics (KIP-714 deferral)
//!
//! Java's `ShareConsumer` also declares `metrics()`,
//! `registerMetricForSubscription()`, and `unregisterMetricFromSubscription()`.
//! Those depend on the KIP-714 telemetry/metrics machinery, which is deferred
//! in this client (see CLAUDE.md and the [`crate::consumer::Consumer`] trait,
//! which likewise omits `metrics()`). They are therefore NOT part of this
//! trait. Metric recording is omitted throughout with `// metrics: deferred
//! to KIP-714`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::{KafkaError, TopicIdPartition, Uuid};
use crate::consumer::acknowledge_type::AcknowledgeType;
use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;
use crate::consumer::{ConsumerRecord, ConsumerRecords};

/// A client that consumes records from a Kafka cluster using a share group.
///
/// The Rust equivalent of Java's `ShareConsumer<K, V>` interface. Runtime
/// dispatch is via `Box<dyn ShareConsumer<K, V>>` (see
/// [`crate::consumer::new_share_consumer`]).
///
/// See [`crate::consumer::KafkaShareConsumer`] and
/// [`crate::consumer::MockShareConsumer`].
#[async_trait]
pub trait ShareConsumer<K, V>: Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Get the current subscription.
    ///
    /// Translates Java's `Set<String> subscription()`. Returns a `Result`
    /// because Java throws `IllegalStateException` if the consumer is closed.
    fn subscription(&self) -> Result<HashSet<String>, KafkaError>;

    /// Subscribe to the given list of topics.
    ///
    /// Translates Java's `void subscribe(Collection<String> topics)`.
    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError>;

    /// Unsubscribe from all topics.
    ///
    /// Translates Java's `void unsubscribe()`.
    async fn unsubscribe(&mut self) -> Result<(), KafkaError>;

    /// Deliver records for the subscribed topics.
    ///
    /// Translates Java's `ConsumerRecords<K, V> poll(Duration timeout)`.
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError>;

    /// Acknowledge successful delivery of a record with [`AcknowledgeType::Accept`].
    ///
    /// Translates Java's `void acknowledge(ConsumerRecord<K, V> record)`.
    fn acknowledge(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), KafkaError>;

    /// Acknowledge delivery of a record with the given type.
    ///
    /// Translates Java's `void acknowledge(ConsumerRecord<K, V>, AcknowledgeType)`.
    fn acknowledge_with_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError>;

    /// Acknowledge delivery of a record by its topic/partition/offset.
    ///
    /// Translates Java's `void acknowledge(String, int, long, AcknowledgeType)`.
    fn acknowledge_by_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError>;

    /// Commit the acknowledgements for the records returned, waiting up to
    /// `default.api.timeout.ms`.
    ///
    /// Translates Java's `Map<TopicIdPartition, Optional<KafkaException>> commitSync()`.
    async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError>;

    /// Commit the acknowledgements for the records returned, waiting up to
    /// `timeout`.
    ///
    /// Translates Java's
    /// `Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration)`.
    async fn commit_sync_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError>;

    /// Commit the acknowledgements for the records returned asynchronously.
    ///
    /// Translates Java's `void commitAsync()`.
    async fn commit_async(&mut self) -> Result<(), KafkaError>;

    /// Set the acknowledgement commit callback (or clear it with `None`).
    ///
    /// Translates Java's
    /// `void setAcknowledgementCommitCallback(AcknowledgementCommitCallback)`.
    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>);

    /// Determine the client's unique client instance ID used for telemetry.
    ///
    /// Translates Java's `Uuid clientInstanceId(Duration timeout)`.
    async fn client_instance_id(&mut self, timeout: Duration) -> Result<Uuid, KafkaError>;

    /// Return the acquisition lock timeout for the last set of records fetched.
    ///
    /// Translates Java's `Optional<Integer> acquisitionLockTimeoutMs()`.
    fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError>;

    /// Close the consumer, waiting up to the default timeout.
    ///
    /// Translates Java's `void close()`.
    async fn close(&mut self) -> Result<(), KafkaError>;

    /// Close the consumer, waiting up to `timeout`.
    ///
    /// Translates Java's `void close(Duration timeout)`.
    async fn close_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError>;

    /// Wake up the consumer.
    ///
    /// Translates Java's `void wakeup()`. Sync — callable from any task.
    fn wakeup(&self);
}
