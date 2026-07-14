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

pub mod acknowledge_type;
pub mod acknowledgement_commit_callback;
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
pub mod kafka_share_consumer;
pub mod mock_consumer;
pub mod mock_share_consumer;
pub mod offset_and_metadata;
pub mod offset_and_timestamp;
pub mod offset_commit_callback;
pub mod offset_reset_strategy;
pub mod share_consumer;
pub mod share_consumer_config;
pub mod subscription_pattern;

pub(crate) mod internals;

pub use acknowledge_type::AcknowledgeType;
pub use acknowledgement_commit_callback::AcknowledgementCommitCallback;
pub use async_kafka_consumer::WakeupHandle;
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
pub use kafka_share_consumer::KafkaShareConsumer;
pub use mock_consumer::MockConsumer;
pub use mock_share_consumer::MockShareConsumer;
pub use offset_and_metadata::OffsetAndMetadata;
pub use offset_and_timestamp::OffsetAndTimestamp;
pub use offset_commit_callback::OffsetCommitCallback;
#[allow(deprecated)]
pub use offset_reset_strategy::OffsetResetStrategy;
pub use share_consumer::ShareConsumer;
pub use share_consumer_config::ShareConsumerConfig;
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
    ///
    /// **Contract note (deviation from Java):** Java returns a map whose
    /// value is *nullable*, so an unresolved partition (queried but no
    /// offset at/after the target time) is conveyed as **key present,
    /// value `null`**, and `result.keySet()` always contains every queried
    /// partition. The Rust [`OffsetAndTimestamp`] value is non-nullable, so
    /// an unresolved partition is **omitted entirely** (key absent) rather
    /// than present-with-null. Callers porting Java code that iterates
    /// `result.keySet()` expecting every queried key back must instead treat
    /// an absent key as "no offset". See [`OffsetAndTimestamp`].
    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
    ///     Map<TopicPartition, Long>, Duration)`.
    ///
    /// See [`Self::offsets_for_times`] for the unresolved-partition
    /// contract note (unresolved partitions are omitted, not
    /// present-with-null, unlike Java).
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

    /// Returns a `Send + 'static` [`WakeupHandle`] that can fire
    /// [`Consumer::wakeup`] from a task / thread other than the one that
    /// owns the consumer.
    ///
    /// No Java method counterpart: Java's `Consumer` reference is itself
    /// shareable across threads, so `consumer.wakeup()` can be called from
    /// another thread while the owning thread blocks in `poll()` /
    /// `position()`. Rust borrows the consumer as `&mut self` for the
    /// duration of a blocking call, so a reference cannot cross the task
    /// boundary; obtain a [`WakeupHandle`] beforehand instead. See
    /// [`WakeupHandle`].
    fn wakeup_handle(&self) -> WakeupHandle;
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

/// Constructs a new share [`ShareConsumer`] (KIP-932) from a
/// [`ShareConsumerConfig`] and explicit key/value
/// [`Deserializer`](crate::common::serialization::Deserializer)s.
///
/// The Rust equivalent of Java's `ShareConsumerDelegateCreator.create(...)`.
/// Per `consumer-threading.md` §2 the creator collapses to a direct
/// `Box::new(ShareConsumerImpl)` (single delegate), mirroring how
/// [`new_consumer`] collapses `ConsumerDelegateCreator`.
///
/// Assembles the full production background pipeline — a `NetworkClient`
/// (+ channel builder from `security.protocol`), a `RequestManagers` populated
/// via [`RequestManagers::for_share`] with the share `ShareConsumeRequestManager`
/// / `ShareHeartbeatRequestManager` / `ShareMembershipManager` + coordinator, a
/// single `ConsumerNetworkThread` bg task (dedicated IO thread, as for the
/// KIP-848 consumer), and a production `ProductionShareApplicationEventHandler`
/// — then returns `Box::new(ShareConsumerImpl::from_components(...))`.
///
/// # Rust-specific divergence: `K: Clone, V: Clone`
///
/// Java's `ShareConsumer<K, V>` has no `Clone` bound. The Rust
/// [`ShareConsumerImpl`](internals::share_consumer_impl::ShareConsumerImpl) —
/// the only [`ShareConsumer`] implementation — impls the trait only for
/// `K: Clone, V: Clone`, because the KIP-932 RENEW acknowledgement path must
/// retain a copy of a record whose ownership was already moved to the user
/// (Java shares object references; Rust's §27 zero-copy drain cannot re-deliver
/// an owned record without cloning it). This factory therefore adds the
/// `K: Clone, V: Clone` bound. This is an accepted Rust-specific divergence: it
/// affects only the RENEW re-delivery path, is zero-cost when RENEW is unused,
/// and does not change any method contract. Almost all key/value types
/// (`String`, `Vec<u8>`, integers, user structs) are `Clone`, so the bound is
/// rarely visible in practice.
///
/// # Errors
///
/// Returns a `KafkaError` with the message `"Failed to construct Kafka share
/// consumer"` (Java's `KafkaException` wrapper message) if `group.id` is
/// missing/blank or any pipeline component fails to construct. The underlying
/// cause is logged at error level (Rust's `KafkaError` has no cause chain).
pub fn new_share_consumer<K, V>(
    config: ShareConsumerConfig,
    key_deserializer: Box<dyn Deserializer<K>>,
    value_deserializer: Box<dyn Deserializer<V>>,
) -> Result<Box<dyn ShareConsumer<K, V>>, KafkaError>
where
    K: Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    build_share_consumer(config, key_deserializer, value_deserializer).map_err(|cause| {
        // Java: `throw new KafkaException("Failed to construct Kafka share
        // consumer", t)`. Rust's KafkaError has no cause chain, so log the cause
        // and return the exact top-level message the tests assert.
        log::error!("Failed to construct Kafka share consumer: {cause}");
        KafkaError::with_message(
            crate::common::protocol::Errors::UnknownServerError,
            "Failed to construct Kafka share consumer",
        )
    })
}

/// Inner assembly for [`new_share_consumer`]; every error is wrapped by the
/// caller as `"Failed to construct Kafka share consumer"`.
fn build_share_consumer<K, V>(
    config: ShareConsumerConfig,
    key_deserializer: Box<dyn Deserializer<K>>,
    value_deserializer: Box<dyn Deserializer<V>>,
) -> Result<Box<dyn ShareConsumer<K, V>>, KafkaError>
where
    K: Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};

    use tokio::sync::mpsc;

    use crate::ApiVersions;
    use crate::DefaultHostResolver;
    use crate::client_utils;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::network::Selector;
    use crate::common::network::channel_builders;
    use crate::common::utils::LogContext;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
    use crate::consumer::internals::consumer_network_thread::{
        ConsumerNetworkThread, MAX_POLL_TIMEOUT_MS, SystemThreadTime, ThreadTime,
    };
    use crate::consumer::internals::consumer_utils::CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
    use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;
    use crate::consumer::internals::deserializers::Deserializers;
    use crate::consumer::internals::events::application_event_handler::ApplicationEventHandler;
    use crate::consumer::internals::events::application_event_processor::ApplicationEventProcessor;
    use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
    use crate::consumer::internals::events::completable_event_reaper::CompletableEventReaper;
    use crate::consumer::internals::events::share_acknowledgement_event_handler::ShareAcknowledgementEventHandler;
    use crate::consumer::internals::network_client_delegate::NetworkClientDelegate;
    use crate::consumer::internals::request_managers::RequestManagers;
    use crate::consumer::internals::share_consume_request_manager::{
        ShareConsumeRequestManager, SystemShareConsumeTime,
    };
    use crate::consumer::internals::share_consumer_impl::{
        ProductionShareApplicationEventHandler, ShareApplicationEventHandler, ShareConsumerComponents,
        ShareConsumerImpl, ShareConsumerTime, ShareFetchCollect, SystemShareConsumerTime,
    };
    use crate::consumer::internals::share_consumer_metadata::ShareConsumerMetadata;
    use crate::consumer::internals::share_fetch_buffer::ShareFetchBuffer;
    use crate::consumer::internals::share_fetch_collector::ShareFetchCollector;
    use crate::consumer::internals::share_fetch_config::ShareFetchConfig;
    use crate::consumer::internals::share_heartbeat_request_manager::ShareHeartbeatRequestManager;
    use crate::consumer::internals::share_membership_manager::ShareMembershipManager;
    use crate::consumer::internals::subscription_state::SubscriptionState;
    use crate::consumer::internals::wakeup_trigger::WakeupTrigger;
    use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
    use crate::network_client::NetworkClient;

    log::debug!("Initializing the Kafka share consumer");

    let acknowledgement_mode = config.acknowledgement_mode();
    let config = config.into_consumer_config();

    // group.id is required for a share consumer. Java throws (wrapped as
    // "Failed to construct Kafka share consumer") for null/empty/blank ids.
    let group_id = match config.group_id() {
        Some(g) if !g.trim().is_empty() => g.to_string(),
        _ => {
            return Err(KafkaError::invalid_group_id(
                "You must provide a valid group.id in the share consumer configuration.",
            ));
        },
    };

    let client_id = config.client_id().to_string();
    let request_timeout_ms = config.request_timeout_ms();
    let default_api_timeout_ms = config.default_api_timeout_ms;
    let current_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    // Application/background event channels + the shared selector-wakeup notify.
    let (bg_event_tx, bg_event_rx) =
        mpsc::unbounded_channel::<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>();
    let (app_event_tx, app_event_rx) =
        mpsc::unbounded_channel::<crate::consumer::internals::events::application_event::ApplicationEventEnvelope>();
    let event_notify = Arc::new(tokio::sync::Notify::new());

    // Share groups do not use auto.offset.reset (an unsupported config); the
    // Java tests build SubscriptionState with AutoOffsetResetStrategy.NONE.
    let subscriptions: Arc<Mutex<SubscriptionState>> =
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::NONE)));

    // Single metadata source of truth: `ShareConsumerMetadata` (share-scoped
    // retainTopic / request-builder overrides). The `NetworkClient` updates its
    // shared `Arc<Metadata>`; the membership manager + fetch collector wrap the
    // SAME `Arc<Metadata>` through `ConsumerMetadata::from_shared_metadata`.
    let share_metadata = Arc::new(ShareConsumerMetadata::from_config(
        &config,
        Arc::clone(&subscriptions),
        ClusterResourceListeners::new(),
    ));
    let shared_meta = share_metadata.metadata_arc();
    let consumer_metadata = Arc::new(ConsumerMetadata::from_shared_metadata(
        Arc::clone(&shared_meta),
        Arc::clone(&subscriptions),
        config.allow_auto_create_topics,
    ));

    let addresses = client_utils::parse_and_validate_addresses(config.bootstrap_servers())?;
    shared_meta.bootstrap(addresses);

    let api_versions = Arc::new(ApiVersions::new());
    let background_event_handler = Arc::new(BackgroundEventHandler::new(bg_event_tx));
    let share_fetch_buffer = Arc::new(ShareFetchBuffer::new());
    let share_fetch_config = ShareFetchConfig::from_consumer_config(&config)?;

    // NetworkClient + delegate (mirrors AsyncKafkaConsumer::new).
    let log_context = LogContext::new(format!("[ShareConsumer clientId={}, groupId={}] ", client_id, group_id));
    let channel_builder = channel_builders::client_channel_builder(
        config.security_protocol,
        Some(&config.ssl_config),
        Some(&config.sasl_config),
        None,
        config.client_id(),
        log_context.clone(),
    )
    .map_err(|e| KafkaError::illegal_argument(format!("Failed to create channel builder: {e}")))?;
    let selector =
        Selector::with_defaults_and_log_context(config.connections_max_idle_ms, channel_builder, log_context.clone());
    let network_client = NetworkClient::with_metadata(
        selector,
        Arc::clone(&shared_meta),
        config.client_id(),
        CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION as usize,
        config.reconnect_backoff_ms,
        config.reconnect_backoff_max_ms,
        config.send_buffer_bytes,
        config.receive_buffer_bytes,
        config.request_timeout_ms,
        config.socket_connection_setup_timeout_ms,
        config.socket_connection_setup_timeout_max_ms,
        true,
        Arc::clone(&api_versions),
        DefaultHostResolver::new(),
        config.metadata_max_age_ms,
        MetadataRecoveryStrategy::None,
        log_context.clone(),
    );
    let network_client_delegate = Arc::new(tokio::sync::Mutex::new(NetworkClientDelegate::new(
        &config,
        network_client,
        Arc::clone(&shared_meta),
        Arc::clone(&background_event_handler),
        false,
    )));

    // ── Share request managers (Java RequestManagers share supplier). ──
    let coordinator = Arc::new(CoordinatorRequestManager::new(
        config.retry_backoff_ms(),
        config.retry_backoff_max_ms(),
        group_id.clone(),
    ));
    let share_membership = Arc::new(ShareMembershipManager::new(
        group_id.clone(),
        // rack.id — Java reads GroupRebalanceConfig.rackId (CLIENT_RACK_CONFIG);
        // empty string means "no rack".
        Some(config.client_rack.clone()).filter(|r| !r.is_empty()),
        Arc::clone(&subscriptions),
        Arc::clone(&consumer_metadata),
        Arc::clone(&background_event_handler),
    ));
    let share_heartbeat = ShareHeartbeatRequestManager::new(
        current_time_ms,
        &config,
        Arc::clone(&coordinator),
        Arc::clone(&subscriptions),
        Arc::clone(&share_membership),
        Arc::clone(&background_event_handler),
    );
    // Shared acknowledgement-event queue: the bg-side ShareConsumeRequestManager
    // enqueues completed acknowledgements; the app-side ShareConsumerImpl drains
    // and invokes the AcknowledgementCommitCallback (§31). Clone shares the queue.
    let ack_event_handler = ShareAcknowledgementEventHandler::default();
    let share_consume_time: Arc<dyn crate::consumer::internals::share_consume_request_manager::ShareConsumeTime> =
        Arc::new(SystemShareConsumeTime);
    let mut share_consume = ShareConsumeRequestManager::new(
        Arc::clone(&share_consume_time),
        log_context.clone(),
        group_id.clone(),
        Arc::clone(&share_metadata),
        Arc::clone(&subscriptions),
        share_fetch_config.clone(),
        Arc::clone(&share_fetch_buffer),
        ack_event_handler.clone(),
        config.retry_backoff_ms(),
        config.retry_backoff_max_ms(),
    );
    // Wire the production response-routing notify so ShareFetch/ShareAcknowledge
    // responses are routed back to the manager and the bg task is woken promptly.
    share_consume.set_completion_notify(Arc::clone(&event_notify));

    let request_managers = Arc::new(std::sync::Mutex::new(RequestManagers::for_share(
        Some(Arc::clone(&coordinator)),
        share_consume,
        share_heartbeat,
        Arc::clone(&share_membership),
    )));

    // ── Event processor + handler + bg task. ──
    let time: Arc<dyn ThreadTime> = Arc::new(SystemThreadTime);
    let application_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>> =
        Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
    let app_event_processor = ApplicationEventProcessor::new(
        Arc::clone(&request_managers),
        Arc::clone(&consumer_metadata),
        Arc::clone(&subscriptions),
        Arc::clone(&application_event_reaper),
    );
    let application_event_handler = Arc::new(ApplicationEventHandler::new(app_event_tx, Arc::clone(&event_notify)));

    let wakeup_trigger = WakeupTrigger::new();
    let max_time_to_wait_ms: Arc<AtomicI64> = Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS));

    let mut network_thread = ConsumerNetworkThread::new(
        Arc::clone(&time),
        app_event_rx,
        Arc::clone(&application_event_reaper),
        app_event_processor,
        Arc::clone(&network_client_delegate),
        Arc::clone(&request_managers),
        None, // KIP-848 consumer membership — not used by a share consumer
        wakeup_trigger.clone(),
        Arc::clone(&max_time_to_wait_ms),
        Arc::clone(&event_notify),
    );
    // Drive the share membership state machine from the bg loop (Phase 2.4s/2.5s).
    network_thread.set_share_membership(Some(Arc::clone(&share_membership)));

    let signal_close_running = network_thread.running_handle();
    let signal_close_wakeup = wakeup_trigger.clone();
    let signal_close_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        signal_close_running.store(false, Ordering::Release);
        signal_close_wakeup.wakeup();
    });
    let wakeup_for_fn = wakeup_trigger.clone();
    let wakeup_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
        wakeup_for_fn.wakeup();
    });

    // Dedicated IO thread hosting a current_thread runtime (Phase 21 pattern).
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let thread_handle = std::thread::Builder::new()
        .name("kafka-share-consumer-io".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build share consumer io runtime");
            rt.block_on(async move {
                let mut thread = network_thread;
                while thread.is_running() {
                    thread.run_once().await;
                }
                thread.cleanup().await;
            });
            let _ = done_tx.send(());
        })
        .map_err(|e| KafkaError::illegal_state(format!("Failed to spawn share consumer io thread: {e}")))?;

    let network_thread_close = crate::consumer::async_kafka_consumer::NetworkThreadCloseHandle::new_dedicated(
        signal_close_fn,
        wakeup_fn,
        done_rx,
        thread_handle,
    );

    // Production event handler + fetch collector + components hand-off.
    let production_handler: Arc<dyn ShareApplicationEventHandler> =
        Arc::new(ProductionShareApplicationEventHandler::new(
            Arc::clone(&application_event_handler),
            Arc::clone(&max_time_to_wait_ms),
            Arc::clone(&event_notify),
            network_thread_close,
        ));

    let deserializers: Arc<Deserializers<K, V>> = Arc::new(Deserializers::new(key_deserializer, value_deserializer));
    let fetch_collector: Box<dyn ShareFetchCollect<K, V>> = Box::new(ShareFetchCollector::new(
        Arc::clone(&consumer_metadata),
        Arc::clone(&subscriptions),
        share_fetch_config,
        deserializers,
    ));

    let share_time: Arc<dyn ShareConsumerTime> = Arc::new(SystemShareConsumerTime);
    let components = ShareConsumerComponents {
        application_event_handler: production_handler,
        fetch_collector,
        fetch_buffer: Arc::clone(&share_fetch_buffer),
        subscriptions,
        metadata: consumer_metadata,
        acknowledgement_event_handler: ack_event_handler,
        background_event_rx: bg_event_rx,
        wakeup_trigger: Arc::new(wakeup_trigger),
        time: share_time,
        client_id,
        group_id,
        request_timeout_ms,
        default_api_timeout_ms,
        acknowledgement_mode,
    };

    log::debug!("Kafka share consumer initialized");
    Ok(Box::new(ShareConsumerImpl::from_components(components)))
}

#[cfg(test)]
mod share_pipeline_smoke_tests {
    //! End-to-end smoke test for the production share-consumer pipeline
    //! assembled by [`new_share_consumer`]. Proves the real background pipeline
    //! (dedicated IO thread hosting `ConsumerNetworkThread`, a
    //! `RequestManagers::for_share` container, and a `ShareConsumerImpl`) is
    //! wired: `subscribe` enqueues a `ShareSubscriptionChange` application event
    //! that the bg task processes and completes locally (no broker required),
    //! `subscription()` reflects it, and `close` cleanly signals then joins the
    //! bg task. Full broker round-trips (heartbeat, fetch, acknowledge) are
    //! exercised by `KafkaShareConsumerTest`.

    use std::collections::HashMap;
    use std::time::Duration;

    use crate::common::KafkaError;
    use crate::common::serialization::Deserializer;
    use crate::consumer::ShareConsumerConfig;

    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
            Ok(String::from_utf8_lossy(data).into_owned())
        }
    }

    fn share_config(group_id: &str) -> ShareConsumerConfig {
        let mut props = HashMap::new();
        props.insert("group.id".to_string(), group_id.to_string());
        // Unreachable bootstrap — subscribe does not require a live broker.
        props.insert("bootstrap.servers".to_string(), "localhost:59999".to_string());
        props.insert("request.timeout.ms".to_string(), "1000".to_string());
        props.insert("default.api.timeout.ms".to_string(), "1000".to_string());
        ShareConsumerConfig::from_properties(&props).expect("valid share config")
    }

    #[tokio::test]
    async fn new_share_consumer_builds_working_pipeline_and_subscribes() {
        let config = share_config("smoke-group");
        let mut consumer = super::new_share_consumer::<String, String>(
            config,
            Box::new(StringDeserializer),
            Box::new(StringDeserializer),
        )
        .expect("new_share_consumer must build a working pipeline");

        // subscribe enqueues a ShareSubscriptionChange event; the bg task
        // processes it and completes the event locally (membership + subscription
        // state update). This proves the whole app-event -> bg-task -> processor
        // -> completion round-trip is wired.
        tokio::time::timeout(Duration::from_secs(10), consumer.subscribe(vec!["smoke-topic".to_string()]))
            .await
            .expect("subscribe did not complete within 10s — the bg pipeline is not advancing")
            .expect("subscribe should succeed");

        let subscription = consumer.subscription().expect("subscription() ok");
        assert!(
            subscription.contains("smoke-topic"),
            "subscription must reflect the subscribed topic; got {subscription:?}"
        );

        // close signals + joins the bg task; bounded so it cannot hang without a
        // broker (the leave-group heartbeat times out and is swallowed).
        tokio::time::timeout(Duration::from_secs(10), consumer.close())
            .await
            .expect("close did not complete within 10s")
            .expect("close should succeed");
    }
}
