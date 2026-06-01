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

//! Consumer types (org.apache.kafka.clients.consumer).
//!
//! Translated from `org.apache.kafka.clients.consumer`. The `clients` Java
//! package segment is intentionally dropped per CLAUDE.md §2.

pub mod async_kafka_consumer;
pub mod close_options;
pub mod consumer_config;
pub mod consumer_group_metadata;
pub mod consumer_rebalance_listener;
pub mod consumer_rebalance_listener_method_name;
pub mod consumer_record;
pub mod consumer_records;
pub mod errors;
pub mod group_protocol;
pub mod interceptor;
pub mod mock_consumer;
pub mod offset_and_metadata;
pub mod offset_and_timestamp;
pub mod offset_commit_callback;
pub mod offset_reset_strategy;
pub mod subscription_pattern;

pub(crate) mod internals;

pub use close_options::{CloseOptions, GroupMembershipOperation};
pub use consumer_config::ConsumerConfig;
pub use consumer_group_metadata::ConsumerGroupMetadata;
pub use consumer_rebalance_listener::ConsumerRebalanceListener;
pub use consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
pub use consumer_record::{ConsumerRecord, NO_TIMESTAMP, NULL_SIZE};
pub use consumer_records::ConsumerRecords;
pub use errors::ConsumerError;
pub use group_protocol::GroupProtocol;
pub use interceptor::ConsumerInterceptor;
pub use internals::auto_offset_reset_strategy::{AutoOffsetResetStrategy, StrategyType};
pub use mock_consumer::MockConsumer;
pub use offset_and_metadata::OffsetAndMetadata;
pub use offset_and_timestamp::OffsetAndTimestamp;
pub use offset_commit_callback::OffsetCommitCallback;
#[allow(deprecated)]
pub use offset_reset_strategy::OffsetResetStrategy;
pub use subscription_pattern::SubscriptionPattern;

// Re-export [`Deserializer`] at the consumer module root for API ergonomics.
// The canonical location is [`crate::common::serialization::Deserializer`]
// (per CLAUDE.md §2, the trait lives in `common::serialization` because it
// is shared by producer + consumer). This re-export mirrors the convenience
// re-exports of [`ConsumerInterceptor`], [`ConsumerRebalanceListener`], and
// [`OffsetCommitCallback`] so that consumer-side users can `use
// confluent_kafka::consumer::Deserializer;` alongside their other consumer
// imports — matching the PLAN's intent for `src/consumer/deserializer.rs`.
pub use crate::common::serialization::Deserializer;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::{KafkaError, PartitionInfo, TopicPartition};

/// The single dispatch trait that `MockConsumer` (Phase 3) and
/// `AsyncKafkaConsumer` (Phase 11) both implement. Translates Java's
/// `org.apache.kafka.clients.consumer.Consumer<K, V>` interface.
///
/// # Parameter conventions
///
/// The trait uses three argument shapes deliberately:
///
/// - **Owned collections (`Vec<T>`, `HashMap<K, V>`):** the implementation
///   stores or forwards the input long-term (subscription state, request
///   payload). Ownership transfer avoids a per-element clone.
///
/// - **Borrowed slices / maps (`&[T]`, `&HashMap<K, V>`):** the
///   implementation iterates but does not retain the input. Callers can
///   pass `&Vec<T>`, `&[T; N]`, or any slice without conversion.
///
/// - **Borrowed scalars (`&TopicPartition`, `&str`):** read-only access to
///   a single value.
///
/// Methods that take `Vec<T>` are the ones that *consume* the input;
/// methods that take `&[T]` only *iterate* it. This rule is mechanical:
/// if the impl retains, it owns; if the impl reads, it borrows.
///
/// # Bounds: `Send + 'static`, NOT `Send + Sync`
///
/// The trait is `Send + 'static` so it can be stored as
/// `Box<dyn Consumer<K, V>>` and moved between tokio tasks (required for
/// multi-thread runtime support). `Sync` is intentionally NOT required
/// because the API is `&mut self` — only one task can call methods at a
/// time, no shared `&Consumer` reference exists.
///
/// Users who need cross-task sharing wrap in `Arc<Mutex<dyn Consumer>>`,
/// which works without `Sync` on the trait itself. Dropping `Sync` lets
/// users plug in `K`/`V` types that are `Send` but not `Sync` (e.g.
/// types containing `Cell`) without artificial restrictions.
///
/// # Async surface
///
/// Per `consumer-threading.md` §1: methods that block in Java become
/// `async fn`; non-blocking accessors stay `fn`. Per CLAUDE.md §11, the
/// per-call `Box<Future>` cost of `#[async_trait]` is amortized over many
/// records (typical `poll()` granularity is ≤ ~100 calls/sec) and is
/// acceptable on this top-level dispatch trait.
///
/// # Methods NOT translated
///
/// - `metrics()`, `registerMetricForSubscription`,
///   `unregisterMetricFromSubscription`, `clientInstanceId(Duration)`:
///   require a metrics framework that does not exist in this milestone.
///   When the metrics framework lands as its own milestone, the
///   [`crate::producer::Producer`]-style traits and `Consumer` gain these
///   methods together.
/// - `subscribe(Pattern, [ConsumerRebalanceListener])` (Java regex):
///   superseded by the [`SubscriptionPattern`] variants which match
///   server-side regex semantics.
/// - `poll(long timeoutMs)`, `close(Duration timeout)` (both
///   `@Deprecated` in Java): not translated per CLAUDE.md §5.
#[async_trait]
pub trait Consumer<K, V>: Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    // ── State reads (sync — Java: non-blocking accessors) ──

    /// Translates Java's `Set<TopicPartition> assignment()`.
    fn assignment(&self) -> HashSet<TopicPartition>;

    /// Translates Java's `Set<String> subscription()`.
    fn subscription(&self) -> HashSet<String>;

    /// Translates Java's `Set<TopicPartition> paused()`.
    fn paused(&self) -> HashSet<TopicPartition>;

    /// Translates Java's `ConsumerGroupMetadata groupMetadata()`.
    fn group_metadata(&self) -> ConsumerGroupMetadata;

    /// Returns the consumer's `client.id`. Borrowed per CLAUDE.md §12.
    fn client_id(&self) -> &str;

    /// Translates Java's `OptionalLong currentLag(TopicPartition)`.
    ///
    /// Returns `Option<i64>` — the natural Rust analog.
    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64>;

    // ── Subscription / assignment (async per §1 — may interact with bg task) ──

    /// Translates Java's `void subscribe(Collection<String> topics)`.
    ///
    /// Takes `Vec<String>` because the impl moves the elements into
    /// `SubscriptionState`.
    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError>;

    /// Translates Java's
    /// `void subscribe(Collection<String> topics, ConsumerRebalanceListener)`.
    async fn subscribe_with_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError>;

    /// Translates Java's `void subscribe(SubscriptionPattern pattern)`.
    async fn subscribe_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), KafkaError>;

    /// Translates Java's
    /// `void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener)`.
    async fn subscribe_pattern_with_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError>;

    /// Translates Java's `void assign(Collection<TopicPartition>)`.
    ///
    /// Async because Java's `assign` calls
    /// `applicationEventHandler.addAndGet(new AssignmentChangeEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1819`). The Rust translation
    /// `.await`s the event handle.
    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError>;

    /// Translates Java's `void unsubscribe()`.
    async fn unsubscribe(&mut self) -> Result<(), KafkaError>;

    // ── Poll ──

    /// Translates Java's `ConsumerRecords<K, V> poll(Duration timeout)`.
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError>;

    // ── Commit ──

    /// Translates Java's `void commitSync()`.
    async fn commit_sync(&mut self) -> Result<(), KafkaError>;

    /// Translates Java's `void commitSync(Duration timeout)`.
    async fn commit_sync_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError>;

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    async fn commit_sync_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), KafkaError>;

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets,
    ///                  Duration timeout)`.
    async fn commit_sync_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), KafkaError>;

    /// Translates Java's `void commitAsync()`.
    async fn commit_async(&mut self) -> Result<(), KafkaError>;

    /// Translates Java's `void commitAsync(OffsetCommitCallback)`.
    async fn commit_async_with_callback(&mut self, callback: Arc<dyn OffsetCommitCallback>) -> Result<(), KafkaError>;

    /// Translates Java's
    /// `void commitAsync(Map<TopicPartition, OffsetAndMetadata>,
    ///                   OffsetCommitCallback)`.
    async fn commit_async_offsets_with_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), KafkaError>;

    // ── Seek (async — Java: addAndGet on SeekUnvalidatedEvent / ResetOffsetEvent) ──

    /// Translates Java's `void seek(TopicPartition partition, long offset)`.
    ///
    /// Returns `Result` because Java throws `IllegalArgumentException` /
    /// `IllegalStateException` on invalid input. Async because Java's seek
    /// calls `applicationEventHandler.addAndGet(new SeekUnvalidatedEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1068`).
    async fn seek(&mut self, partition: TopicPartition, offset: i64) -> Result<(), KafkaError>;

    /// Translates Java's
    /// `void seek(TopicPartition partition, OffsetAndMetadata)`.
    async fn seek_with_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), KafkaError>;

    /// Translates Java's `void seekToBeginning(Collection<TopicPartition>)`.
    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    /// Translates Java's `void seekToEnd(Collection<TopicPartition>)`.
    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    // ── Position / committed (async — may fetch from broker) ──

    /// Translates Java's `long position(TopicPartition)`.
    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, KafkaError>;

    /// Translates Java's `long position(TopicPartition, Duration)`.
    async fn position_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>)`.
    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>,
    ///                                                    Duration)`.
    async fn committed_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;

    // ── Metadata (async — may fetch from broker) ──

    /// Translates Java's `List<PartitionInfo> partitionsFor(String topic)`.
    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError>;

    /// Translates Java's
    /// `List<PartitionInfo> partitionsFor(String topic, Duration)`.
    async fn partitions_for_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, KafkaError>;

    /// Translates Java's
    /// `Map<String, List<PartitionInfo>> listTopics()`.
    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>;

    /// Translates Java's
    /// `Map<String, List<PartitionInfo>> listTopics(Duration)`.
    async fn list_topics_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
    ///     Map<TopicPartition, Long>)`.
    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
    ///     Map<TopicPartition, Long>, Duration)`.
    async fn offsets_for_times_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>)`.
    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>,
    ///                                              Duration)`.
    async fn beginning_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>)`.
    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>,
    ///                                        Duration)`.
    async fn end_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    // ── Pause / resume (async — Java: addAndGet on PausePartitions / ResumePartitions) ──

    /// Translates Java's `void pause(Collection<TopicPartition>)`.
    ///
    /// Async because Java's pause calls
    /// `applicationEventHandler.addAndGet(new PausePartitionsEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1279`).
    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    /// Translates Java's `void resume(Collection<TopicPartition>)`.
    ///
    /// Async because Java's resume calls
    /// `applicationEventHandler.addAndGet(new ResumePartitionsEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1292`).
    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    // ── Lifecycle ──

    /// Translates Java's `void enforceRebalance()` /
    /// `void enforceRebalance(String reason)` combined; `reason` defaults
    /// to `None`.
    ///
    /// Java's javadoc says this method is classic-protocol-only; under
    /// the KIP-848 protocol it returns an unsupported-version error.
    /// Match Java behavior.
    async fn enforce_rebalance(&mut self, reason: Option<&str>) -> Result<(), KafkaError>;

    /// Translates Java's `void close()`. Closes the consumer with default
    /// timeout.
    async fn close(&mut self) -> Result<(), KafkaError>;

    /// Translates Java's `void close(CloseOptions option)`.
    async fn close_with_options(&mut self, options: CloseOptions) -> Result<(), KafkaError>;

    /// Translates Java's `void wakeup()`. Sync — callable from any task,
    /// including signal handlers.
    fn wakeup(&self);
}

/// Constructs a new [`Consumer`] from a configuration and explicit
/// key/value [`Deserializer`]s.
///
/// For `group.protocol=consumer` (KIP-848), this returns
/// `Box::new(AsyncKafkaConsumer::new(...)?)` — the production consumer
/// built end-to-end with `SubscriptionState`, `ConsumerMetadata`,
/// `NetworkClient`, every `RequestManager`, and a single bg task
/// (`ConsumerNetworkThread`). For `group.protocol=classic`, returns
/// [`KafkaError::unsupported_version`] per `consumer-threading.md` §20
/// (classic protocol deferred to a later milestone).
///
/// Java passes deserializers via `ConsumerConfig` reflection; Rust takes
/// them as explicit `Box<dyn>` parameters (Phase 1 decision not to
/// translate reflection machinery). The consumer wraps them in
/// `Arc<Deserializers<K, V>>` internally for sharing with
/// `Fetcher`/`FetchCollector` (Shape B).
///
/// `MockConsumer` (Phase 3) does NOT come through this factory — it has
/// its own constructor. The factory is for the production consumer only.
pub fn new_consumer<K, V>(
    config: ConsumerConfig,
    key_deserializer: Box<dyn Deserializer<K>>,
    value_deserializer: Box<dyn Deserializer<V>>,
) -> Result<Box<dyn Consumer<K, V>>, KafkaError>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // Phase 12 commit (4/N) wires the `GroupProtocol::Consumer` arm to
    // the production constructor at
    // [`async_kafka_consumer::AsyncKafkaConsumer::new`], which translates
    // the Java primary constructor at `AsyncKafkaConsumer.java:285-518`
    // end-to-end. The ctor builds the full dependency closure
    // (`SubscriptionState`, `ConsumerMetadata`, `NetworkClient` +
    // PLAINTEXT `ChannelBuilder`, every `RequestManager`,
    // `ApplicationEventHandler`, `ConsumerNetworkThread` bg task) and
    // hands off to `AsyncKafkaConsumer::new_with_components` so the
    // Phase-11 test seam is preserved. `Box<dyn Consumer<K, V>>` is
    // returned so the dispatch surface stays object-safe (Consumer
    // trait surface check at `tests/consumer/trait_surface_check.rs`).
    //
    // `GroupProtocol::Classic` remains an `unsupported_version` error
    // per `consumer-threading.md` §20 (classic protocol deferred to a
    // later milestone).
    let protocol = GroupProtocol::of(config.group_protocol())?;
    match protocol {
        GroupProtocol::Consumer => Ok(Box::new(async_kafka_consumer::AsyncKafkaConsumer::<K, V>::new(
            config,
            key_deserializer,
            value_deserializer,
        )?)),
        GroupProtocol::Classic => Err(KafkaError::unsupported_version(
            "Classic group protocol is not yet supported in this client; \
             set group.protocol=consumer (KIP-848).",
        )),
    }
}
