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

mod async_kafka_consumer;
pub mod close_options;
mod consumer_commit_failed_error;
mod consumer_config;
mod consumer_group_metadata;
mod consumer_log_truncation_error;
mod consumer_no_offset_for_partition_error;
mod consumer_offset_out_of_range_error;
pub mod consumer_partition_assignor;
mod consumer_rebalance_listener;
mod consumer_rebalance_listener_method_name;
mod consumer_record;
mod consumer_records;
mod consumer_retriable_commit_failed_error;
mod group_protocol;
mod interceptor;
mod mock_consumer;
mod offset_and_metadata;
mod offset_and_timestamp;
mod offset_commit_callback;
mod offset_reset_strategy;
mod subscription_pattern;

pub(crate) mod internals;

pub use async_kafka_consumer::{AsyncKafkaConsumer, ConsumerHandle};
pub use close_options::{CloseOptions, GroupMembershipOperation};
pub use consumer_config::ConsumerConfig;
pub use consumer_group_metadata::ConsumerGroupMetadata;
pub use consumer_rebalance_listener::ConsumerRebalanceListener;
pub use consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
pub use consumer_record::{ConsumerRecord, ConsumerRecordOptions, ConsumerRecordOptionsBuilder};
pub use consumer_records::ConsumerRecords;
pub use group_protocol::GroupProtocol;
pub use interceptor::ConsumerInterceptor;
pub use internals::{AutoOffsetResetStrategy, StrategyType};
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

use crate::common::{Error, PartitionInfo, TopicPartition};

// Re-export the metric read types returned by [`Consumer::metrics`]. Java's
// `Consumer.metrics()` returns `Map<MetricName, ? extends Metric>`; these are
// the Rust counterparts. [`MetricName`] is the metric key, [`Metric`] the read
// interface, [`KafkaMetric`] the concrete registry entry (which `impl Metric`),
// and [`MetricValue`] the type-erased reading. Re-exported here so
// consumer-side users can name the `metrics()` return type without reaching
// into `crate::common`. These mirror Java's public metric types being
// accessible from the consumer package.
pub use crate::common::metrics::KafkaMetric;
pub use crate::common::{Metric, MetricName, MetricValue};

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
/// - `registerMetricForSubscription`, `unregisterMetricFromSubscription`,
///   `clientInstanceId(Duration)`: KIP-714 broker-push telemetry, deferred
///   to its own milestone (see Milestone-9 metrics plan, "Out of scope").
///   They are omitted from the trait (no stub); when KIP-714 telemetry
///   lands, the [`crate::producer::Producer`]-style traits and `Consumer`
///   gain these methods together.
/// - `metrics()` IS translated (Phase M7) — it is in Java's `Consumer`
///   interface and snapshots the registry that the metrics managers
///   populate. See [`Consumer::metrics`].
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

    /// Translates Java's `Map<MetricName, ? extends Metric> metrics()`.
    ///
    /// Returns a snapshot of all metrics maintained by the consumer, keyed by
    /// [`MetricName`]. The value type is `Arc<KafkaMetric>` — [`KafkaMetric`]
    /// implements the [`Metric`] read interface, mirroring Java's
    /// `? extends Metric` wildcard. Read each metric's name via
    /// [`Metric::metric_name`] and its current value via
    /// [`Metric::metric_value`].
    ///
    /// Sync — Java's `metrics()` does not block. The returned map is a
    /// point-in-time snapshot taken under the registry lock (a cold,
    /// monitoring-frequency call), not a live view.
    ///
    /// This trait method has **no default**: it is in Java's `Consumer`
    /// interface, so every implementation provides it, and adding it without
    /// a default is acceptable for this pre-1.0 dispatch trait
    /// (consumer-threading.md §2). It MATCHES Java's public surface — it is
    /// not a Rust-only addition.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;

    // ── Subscription / assignment (async per §1 — may interact with bg task) ──

    // Java declares six `subscribe` overloads (`Consumer.java:54-84`). The
    // intersection of their parameters is empty, so under CLAUDE.md §2 no
    // overload keeps the plain name `subscribe`; each is suffixed with the
    // parameter names that distinguish it.
    //
    // Java has two pattern forms and they are NOT sugar for one another:
    // `subscribe(Pattern)` matches a `java.util.regex.Pattern` **client-side**
    // against the consumer's own metadata
    // (`TopicPatternSubscriptionChangeEvent`), while
    // `subscribe(SubscriptionPattern)` sends the pattern to the broker for
    // **server-side** RE2/J evaluation
    // (`TopicRe2JPatternSubscriptionChangeEvent`) —
    // `AsyncKafkaConsumer.java:2107,2131`.
    //
    // **Only the `SubscriptionPattern` form is translated.** The two
    // `subscribe(Pattern ...)` overloads are deliberately NOT implemented in
    // Rust, so there is no `subscribe_pattern` / `subscribe_pattern_listener`
    // on this trait. Callers wanting a regex subscription use
    // [`Self::subscribe_with_pattern`], whose pattern the group
    // coordinator evaluates.

    /// Translates Java's `void subscribe(Collection<String> topics)`.
    ///
    /// Takes `Vec<String>` because the impl moves the elements into
    /// `SubscriptionState`.
    async fn subscribe_with_topics(&mut self, topics: Vec<String>) -> Result<(), Error>;

    /// Translates Java's
    /// `void subscribe(Collection<String> topics, ConsumerRebalanceListener)`.
    async fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error>;

    /// Translates Java's `void subscribe(SubscriptionPattern pattern)`.
    ///
    /// Server-side regex subscription (KIP-848 RE2/J): the pattern is sent to
    /// the group coordinator, which evaluates it. Java's javadoc notes that no
    /// validation of the pattern is performed by the client.
    async fn subscribe_with_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), Error>;

    /// Translates Java's
    /// `void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener)`.
    async fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), Error>;

    /// Translates Java's `void assign(Collection<TopicPartition>)`.
    ///
    /// Async because Java's `assign` calls
    /// `applicationEventHandler.addAndGet(new AssignmentChangeEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1819`). The Rust translation
    /// `.await`s the event handle.
    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), Error>;

    /// Translates Java's `void unsubscribe()`.
    async fn unsubscribe(&mut self) -> Result<(), Error>;

    // ── Poll ──

    /// Translates Java's `ConsumerRecords<K, V> poll(Duration timeout)`.
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error>;

    // ── Commit ──

    /// Translates Java's `void commitSync()`.
    async fn commit_sync(&mut self) -> Result<(), Error>;

    /// Translates Java's `void commitSync(Duration timeout)`.
    async fn commit_sync_with_timeout(&mut self, timeout: Duration) -> Result<(), Error>;

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    async fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), Error>;

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets,
    ///                  Duration timeout)`.
    async fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), Error>;

    /// Translates Java's `void commitAsync()`.
    async fn commit_async(&mut self) -> Result<(), Error>;

    /// Translates Java's `void commitAsync(OffsetCommitCallback)`.
    async fn commit_async_with_callback(&mut self, callback: Arc<dyn OffsetCommitCallback>) -> Result<(), Error>;

    /// Translates Java's
    /// `void commitAsync(Map<TopicPartition, OffsetAndMetadata>,
    ///                   OffsetCommitCallback)`.
    async fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), Error>;

    // ── Seek (async — Java: addAndGet on SeekUnvalidatedEvent / ResetOffsetEvent) ──

    /// Translates Java's `void seek(TopicPartition partition, long offset)`.
    ///
    /// Returns `Result` because Java throws `IllegalArgumentException` /
    /// `IllegalStateException` on invalid input. Async because Java's seek
    /// calls `applicationEventHandler.addAndGet(new SeekUnvalidatedEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1068`).
    async fn seek_with_offset(&mut self, partition: TopicPartition, offset: i64) -> Result<(), Error>;

    /// Translates Java's
    /// `void seek(TopicPartition partition, OffsetAndMetadata)`.
    async fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), Error>;

    /// Translates Java's `void seekToBeginning(Collection<TopicPartition>)`.
    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;

    /// Translates Java's `void seekToEnd(Collection<TopicPartition>)`.
    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;

    // ── Position / committed (async — may fetch from broker) ──

    /// Translates Java's `long position(TopicPartition)`.
    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error>;

    /// Translates Java's `long position(TopicPartition, Duration)`.
    async fn position_with_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>)`.
    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>,
    ///                                                    Duration)`.
    async fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>;

    // ── Metadata (async — may fetch from broker) ──

    /// Translates Java's `List<PartitionInfo> partitionsFor(String topic)`.
    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, Error>;

    /// Translates Java's
    /// `List<PartitionInfo> partitionsFor(String topic, Duration)`.
    async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, Error>;

    /// Translates Java's
    /// `Map<String, List<PartitionInfo>> listTopics()`.
    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<PartitionInfo>>, Error>;

    /// Translates Java's
    /// `Map<String, List<PartitionInfo>> listTopics(Duration)`.
    async fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, Error>;

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
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
    ///     Map<TopicPartition, Long>, Duration)`.
    ///
    /// See [`Self::offsets_for_times`] for the unresolved-partition
    /// contract note (unresolved partitions are omitted, not
    /// present-with-null, unlike Java).
    async fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>)`.
    async fn beginning_offsets(&mut self, partitions: &[TopicPartition])
    -> Result<HashMap<TopicPartition, i64>, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>,
    ///                                              Duration)`.
    async fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>)`.
    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error>;

    /// Translates Java's
    /// `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>,
    ///                                        Duration)`.
    async fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error>;

    // ── Pause / resume (async — Java: addAndGet on PausePartitions / ResumePartitions) ──

    /// Translates Java's `void pause(Collection<TopicPartition>)`.
    ///
    /// Async because Java's pause calls
    /// `applicationEventHandler.addAndGet(new PausePartitionsEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1279`).
    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;

    /// Translates Java's `void resume(Collection<TopicPartition>)`.
    ///
    /// Async because Java's resume calls
    /// `applicationEventHandler.addAndGet(new ResumePartitionsEvent(...))`
    /// which blocks (`AsyncKafkaConsumer.java:1292`).
    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error>;

    // ── Lifecycle ──

    /// Translates Java's `void enforceRebalance()` (`Consumer.java:267`).
    ///
    /// Java's javadoc says this method is classic-protocol-only; under
    /// the KIP-848 protocol it returns an unsupported-version error.
    /// Match Java behavior.
    async fn enforce_rebalance(&mut self) -> Result<(), Error>;

    /// Translates Java's `void enforceRebalance(String reason)`
    /// (`Consumer.java:272`).
    ///
    /// The parameter intersection across Java's two overloads is empty, so
    /// under CLAUDE.md §2 the no-arg form keeps the plain name and this one
    /// carries the `reason` parameter-name suffix.
    async fn enforce_rebalance_with_reason(&mut self, reason: &str) -> Result<(), Error>;

    /// Translates Java's `void close()`. Closes the consumer with default
    /// timeout.
    async fn close(&mut self) -> Result<(), Error>;

    /// Translates Java's `@Deprecated void close(Duration timeout)`
    /// (`Consumer.java:283`).
    #[deprecated(
        note = "mirroring Java's @Deprecated close(Duration); use close_with_options with CloseOptions::timeout"
    )]
    async fn close_with_timeout(&mut self, timeout: Duration) -> Result<(), Error>;

    /// Translates Java's `void close(CloseOptions option)`.
    async fn close_with_options(&mut self, options: CloseOptions) -> Result<(), Error>;

    /// Translates Java's `void wakeup()`. Sync — callable from any task,
    /// including signal handlers.
    fn wakeup(&self);

    /// Returns a `Clone + Send + Sync` [`ConsumerHandle`] exposing
    /// [`Consumer::wakeup`] **and** the reentrant-safe consumer operations,
    /// callable from a task / thread other than the one that owns the
    /// consumer.
    ///
    /// No Java method counterpart — it recovers a Java capability. Java's
    /// `Consumer` reference is itself shareable across threads, so (1)
    /// `consumer.wakeup()` can be called from another thread while the
    /// owning thread blocks in `poll()` / `position()`, and (2) a
    /// `ConsumerRebalanceListener` can call back into the consumer
    /// (`assign`/`seek`/`pause`/`position`/...) from inside a callback by
    /// capturing the `consumer` variable. Rust borrows the consumer as
    /// `&mut self` for the duration of a blocking call and
    /// `Box<dyn Consumer>` is not `Clone`, so neither is expressible with a
    /// bare reference; capture a [`ConsumerHandle`] instead. See
    /// [`ConsumerHandle`].
    fn handle(&self) -> ConsumerHandle;
}

/// Constructs a new [`Consumer`] from a configuration and explicit
/// key/value [`Deserializer`]s.
///
/// For `group.protocol=consumer` (KIP-848), this returns
/// `Box::new(AsyncKafkaConsumer::new(...)?)` — the production consumer
/// built end-to-end with `SubscriptionState`, `ConsumerMetadata`,
/// `NetworkClient`, every `RequestManager`, and a single bg task
/// (`ConsumerNetworkThread`). For `group.protocol=classic`, returns
/// [`Error::unsupported_version`] per `consumer-threading.md` §20
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
) -> Result<Box<dyn Consumer<K, V>>, Error>
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
    // hands off to `AsyncKafkaConsumer::with_components` so the
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
        GroupProtocol::Classic => Err(Error::unsupported_version(
            "Classic group protocol is not yet supported in this client; \
             set group.protocol=consumer (KIP-848).",
        )),
    }
}
pub use consumer_commit_failed_error::ConsumerCommitFailedError;
pub use consumer_log_truncation_error::ConsumerLogTruncationError;
pub use consumer_no_offset_for_partition_error::ConsumerNoOffsetForPartitionError;
pub use consumer_offset_out_of_range_error::ConsumerOffsetOutOfRangeError;
pub use consumer_retriable_commit_failed_error::ConsumerRetriableCommitFailedError;
