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

pub mod close_options;
mod consumer_commit_failed_error;
mod consumer_config;
mod consumer_group_metadata;
mod consumer_log_truncation_error;
mod consumer_no_offset_for_partition_error;
mod consumer_offset_out_of_range_error;
pub mod consumer_partition_assignor;
mod consumer_rebalance_listener;
mod consumer_record;
mod consumer_records;
mod consumer_retriable_commit_failed_error;
mod dyn_consumer;
mod group_protocol;
mod interceptor;
mod kafka_consumer;
mod mock_consumer;
mod offset_and_metadata;
mod offset_and_timestamp;
pub mod offset_commit_callback;
mod subscription_pattern;

pub(crate) mod internals;

pub use close_options::{CloseOptions, GroupMembershipOperation};
pub use consumer_config::ConsumerConfig;
pub use consumer_group_metadata::ConsumerGroupMetadata;
pub(crate) use consumer_group_metadata::ConsumerGroupMetadataImpl;
pub use consumer_rebalance_listener::ConsumerRebalanceListener;
pub use consumer_record::{ConsumerRecord, ConsumerRecordOptions, ConsumerRecordOptionsBuilder};
pub use consumer_records::ConsumerRecords;
pub use dyn_consumer::DynConsumer;
pub use group_protocol::GroupProtocol;
pub use interceptor::ConsumerInterceptor;
pub use internals::ConsumerHandle;
pub use kafka_consumer::KafkaConsumer;
pub use mock_consumer::MockConsumer;
pub use offset_and_metadata::OffsetAndMetadata;
pub use offset_and_timestamp::OffsetAndTimestamp;
pub use offset_commit_callback::OffsetCommitCallback;
pub use subscription_pattern::SubscriptionPattern;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::MetricsView;
use crate::common::{Error, PartitionInfo, TopicPartition};

/// The consumer interface, implemented by [`KafkaConsumer`] and
/// [`MockConsumer`].
///
/// # Translation notes
///
/// Methods that block in Java are asynchronous, returning `impl Future + Send`;
/// hold [`DynConsumer`] to keep different implementations behind one type.
/// Methods taking a `Vec` or `HashMap` retain it; those taking a slice or a
/// `&HashMap` only read it. The KIP-714 telemetry methods and the
/// `subscribe` overloads taking a `java.util.regex.Pattern` are not translated.
//
// # Parameter conventions
//
// The trait uses three argument shapes deliberately:
//
// - **Owned collections (`Vec<T>`, `HashMap<K, V>`):** the implementation
//   stores or forwards the input long-term (subscription state, request
//   payload). Ownership transfer avoids a per-element clone.
//
// - **Borrowed slices / maps (`&[T]`, `&HashMap<K, V>`):** the
//   implementation iterates but does not retain the input. Callers can
//   pass `&Vec<T>`, `&[T; N]`, or any slice without conversion.
//
// - **Borrowed scalars (`&TopicPartition`, `&str`):** read-only access to
//   a single value.
//
// Methods that take `Vec<T>` are the ones that *consume* the input;
// methods that take `&[T]` only *iterate* it. This rule is mechanical:
// if the impl retains, it owns; if the impl reads, it borrows.
//
// # Bounds: `Send + 'static`, NOT `Send + Sync`
//
// The trait is `Send + 'static` so a consumer, and its [`DynConsumer`]
// view `Box<dyn DynConsumer<K, V>>`, can be moved between tokio tasks
// (required for multi-thread runtime support). `Sync` is intentionally NOT
// required because the API is `&mut self` — only one task can call methods
// at a time, no shared `&Consumer` reference exists.
//
// Users who need cross-task sharing wrap in `Arc<Mutex<dyn DynConsumer>>`,
// which works without `Sync` on the trait itself. Dropping `Sync` lets
// users plug in `K`/`V` types that are `Send` but not `Sync` (e.g.
// types containing `Cell`) without artificial restrictions.
//
// # Async surface
//
// Per `consumer-threading.md` §1: methods that block in Java are
// asynchronous; non-blocking accessors stay `fn`. Every asynchronous method
// returns `impl Future + Send` rather than being declared `async fn`, so
// that generic code can move the returned futures across tasks and box them
// as `dyn Future + Send`. That guarantee is what lets [`DynConsumer`] be
// blanket-implemented for every `Consumer`. Implementations may still be
// written with `async fn`.
//
// Returning `impl Future` keeps statically dispatched calls (on a concrete
// [`KafkaConsumer`] or [`MockConsumer`], or through a generic
// `C: Consumer<K, V>`) free of a boxed future, but makes the trait itself
// not dyn-compatible. Hold [`DynConsumer`] to keep different consumer
// implementations behind one type, as Java code holds the `Consumer`
// interface (CLAUDE.md §3).
//
// # Methods NOT translated
//
// - `registerMetricForSubscription`, `unregisterMetricFromSubscription`,
//   `clientInstanceId(Duration)`: KIP-714 broker-push telemetry, deferred
//   to its own milestone (see Milestone-9 metrics plan, "Out of scope").
//   They are omitted from the trait (no stub); when KIP-714 telemetry
//   lands, the [`crate::producer::Producer`]-style traits and `Consumer`
//   gain these methods together.
// - `metrics()` IS translated (Phase M7) — it is in Java's `Consumer`
//   interface and snapshots the registry that the metrics managers
//   populate. See [`Consumer::metrics`].
// - `subscribe(Pattern, [ConsumerRebalanceListener])` (Java regex):
//   superseded by the [`SubscriptionPattern`] variants which match
//   server-side regex semantics.
// - `poll(long timeoutMs)`, `close(Duration timeout)` (both
//   `@Deprecated` in Java): not translated per CLAUDE.md §3.
#[doc(alias = "org.apache.kafka.clients.consumer.Consumer")]
pub trait Consumer<K, V>: Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    // ── State reads (sync — Java: non-blocking accessors) ──

    /// Get the set of partitions currently assigned to this consumer. If
    /// subscription happened by directly assigning partitions using
    /// [`Self::assign`] then this will simply return the same partitions that
    /// were assigned. If topic subscription was used, then this will give the
    /// set of topic partitions currently assigned to the consumer (which may
    /// be none if the assignment hasn't happened yet, or the partitions are in
    /// the process of getting reassigned).
    ///
    /// The returned set is a snapshot of the current assignment at the time of
    /// the call. It will not be updated if the assignment changes afterward.
    ///
    /// # Returns
    ///
    /// An immutable snapshot of the set of partitions currently assigned to this consumer
    ///
    /// # Panics
    ///
    /// On [`KafkaConsumer`], panics with `"This consumer has already been
    /// closed."` when the consumer is closed, where Java's
    /// `acquireAndEnsureOpen()` throws `IllegalStateException`: calling it
    /// after `close()` is a programming error. [`MockConsumer`] follows Java's
    /// `MockConsumer`, which does not check.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#assignment")]
    fn assignment(&mut self) -> HashSet<TopicPartition>;

    /// Get the current subscription. Will return the same topics used in the
    /// most recent call to [`Self::subscribe_with_topics`] or
    /// [`Self::subscribe_with_topics_listener`], or an empty set if no such
    /// call has been made.
    ///
    /// The returned set is a snapshot of the current subscription at the time
    /// of the call. It will not be updated if the subscription changes
    /// afterward.
    ///
    /// # Returns
    ///
    /// An immutable snapshot of the set of topics currently subscribed to
    ///
    /// # Panics
    ///
    /// On [`KafkaConsumer`], panics with `"This consumer has already been
    /// closed."` when the consumer is closed, where Java's
    /// `acquireAndEnsureOpen()` throws `IllegalStateException`: calling it
    /// after `close()` is a programming error. [`MockConsumer`] follows Java's
    /// `MockConsumer`, which does not check.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#subscription")]
    fn subscription(&mut self) -> HashSet<String>;

    /// Get the set of partitions that were previously paused by a call to `Self::pause`.
    ///
    /// # Returns
    ///
    /// The set of paused partitions
    ///
    /// # Panics
    ///
    /// On [`KafkaConsumer`], panics with `"This consumer has already been
    /// closed."` when the consumer is closed, where Java's
    /// `acquireAndEnsureOpen()` throws `IllegalStateException`: calling it
    /// after `close()` is a programming error. [`MockConsumer`] follows Java's
    /// `MockConsumer`, which does not check.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#paused")]
    fn paused(&mut self) -> HashSet<TopicPartition>;

    /// Return the current group metadata associated with this consumer.
    ///
    /// # Returns
    ///
    /// consumer group metadata
    ///
    /// # Panics
    ///
    /// On [`KafkaConsumer`]: with `"This consumer has already been closed."`
    /// when the consumer is closed, and with `InvalidGroupIdError`'s message
    /// when the consumer has no `group.id`.
    ///
    /// # Translation notes
    ///
    /// A misuse Java throws for is a panic. [`MockConsumer`] does not check.
    // `ConsumerGroupMetadata` is a trait, so the consumer returns its own
    // implementation behind an `Arc`, cheap to clone and to hand to
    // `Producer::send_offsets_to_transaction`. MockConsumer follows Java's
    // MockConsumer, which returns a dummy group's metadata.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#groupMetadata")]
    fn group_metadata(&mut self) -> Arc<dyn ConsumerGroupMetadata>;

    /// Get the consumer's current lag on the partition. Returns `None` if the
    /// lag is not known, for example if there is no position yet, or if the
    /// end offset is not known yet.
    ///
    /// This method uses locally cached metadata. If the log end offset is not
    /// known yet, it triggers a request to fetch the log end offset, but
    /// returns immediately.
    ///
    /// # Arguments
    ///
    /// - `topic_partition` - The partition to get the lag for.
    ///
    /// # Returns
    ///
    /// This `Consumer` instance's current lag for the given partition.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the `topicPartition` is not assigned, or the
    ///   consumer is closed
    /// - `Error::is_timeout_error` if the background task does not answer within
    ///   `default.api.timeout.ms`
    ///
    /// # Translation notes
    ///
    /// Asynchronous: Java's `currentLag` waits for the consumer's background
    /// thread too.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#currentLag")]
    fn current_lag(
        &mut self,
        topic_partition: &TopicPartition,
    ) -> impl Future<Output = Result<Option<i64>, Error>> + Send;

    /// Get the metrics kept by the consumer
    ///
    /// The returned map is an unmodifiable live view of the metrics. Changes to
    /// the underlying metrics will be reflected in the returned map.
    ///
    /// # Returns
    ///
    /// An unmodifiable live view of the map of metrics currently maintained by
    /// the consumer
    // The trait method has no default: it is in Java's `Consumer` interface, so
    // every implementation provides it.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#metrics")]
    fn metrics(&self) -> MetricsView;

    // ── Subscription / assignment (async per §1 — may interact with bg task) ──

    // Java declares six `subscribe` overloads (`Consumer#subscribe`). The
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
    // (`TopicRe2JPatternSubscriptionChangeEvent`), in
    // `AsyncKafkaConsumer#subscribe`.
    //
    // **Only the `SubscriptionPattern` form is translated.** The two
    // `subscribe(Pattern ...)` overloads are deliberately NOT implemented in
    // Rust, so there is no `subscribe_pattern` / `subscribe_pattern_listener`
    // on this trait. Callers wanting a regex subscription use
    // [`Self::subscribe_with_pattern`], whose pattern the group
    // coordinator evaluates.

    /// Subscribe to the given list of topics to get dynamically assigned partitions. **Topic
    /// subscriptions are not incremental. This list will replace the current assignment (if
    /// there is one).** It is not possible to combine topic subscription with group management
    /// with manual partition assignment through `Self::assign`. If the given list of topics is
    /// empty, it is treated the same as `Self::unsubscribe`.
    ///
    /// This is a short-hand for [`Self::subscribe_with_topics_listener`], which uses a no-op
    /// listener. If you need the ability to seek to particular offsets, you should prefer
    /// [`Self::subscribe_with_topics_listener`], since group rebalances will cause partition
    /// offsets to be reset. You should also provide your own listener if you are doing your own
    /// offset management since the listener gives you an opportunity to commit offsets before a
    /// rebalance finishes.
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If topics is None or contains None or empty
    ///   elements
    /// - `Error::is_local_illegal_state_error` If `subscribe()` is called previously with pattern,
    ///   or assign is called previously (without a subsequent call to `Self::unsubscribe`), or if
    ///   not configured at-least one partition assignment strategy
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#subscribe")]
    fn subscribe_with_topics(&mut self, topics: Vec<String>) -> impl Future<Output = Result<(), Error>> + Send;

    /// Subscribe to the given list of topics to get dynamically assigned partitions. **Topic
    /// subscriptions are not incremental. This list will replace the current assignment (if
    /// there is one).** Note that it is not possible to combine topic subscription with group
    /// management with manual partition assignment through `Self::assign`. If the given list of
    /// topics is empty, it is treated the same as `Self::unsubscribe`.
    ///
    /// As part of group management, the group coordinator will keep track of the list of
    /// consumers that belong to a particular group and will trigger a rebalance operation if
    /// any one of the following events are triggered:
    ///
    /// - Number of partitions change for any of the subscribed topics
    /// - A subscribed topic is created or deleted
    /// - An existing member of the consumer group is shutdown or fails
    /// - A new member is added to the consumer group
    ///
    /// When any of these events are triggered, the provided listener will be invoked in this
    /// way:
    ///
    /// - [`ConsumerRebalanceListener::on_partitions_revoked`] will be invoked with the
    ///   partitions to revoke, before re-assigning those partitions to another consumer.
    /// - [`ConsumerRebalanceListener::on_partitions_assigned`] will be invoked when the
    ///   rebalance completes (even if no new partitions are assigned to the consumer)
    ///
    /// Note that rebalances will only occur during an active call to `Self::poll`, so callbacks
    /// will also only be invoked during that time. The provided listener will immediately
    /// override any listener set in a previous call to subscribe. It is guaranteed, however,
    /// that the partitions revoked/assigned through this interface are from topics subscribed
    /// in this call. See `ConsumerRebalanceListener` for more details.
    ///
    /// # Arguments
    ///
    /// - `topics` - The list of topics to subscribe to
    /// - `listener` - Non-None listener instance to get notifications on partition
    ///   assignment/revocation for the subscribed topics
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If topics is None or contains None or empty
    ///   elements, or if listener is None
    /// - `Error::is_local_illegal_state_error` If `subscribe()` is called previously with pattern,
    ///   or assign is called previously (without a subsequent call to `Self::unsubscribe`), or if
    ///   not configured at-least one partition assignment strategy
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#subscribe")]
    fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Subscribe to all topics matching the specified pattern, to get dynamically assigned
    /// partitions. The pattern matching will be done periodically against topics. This is only
    /// supported under the CONSUMER group protocol (see `ConsumerConfig::GROUP_PROTOCOL_CONFIG`)
    ///
    /// If the provided pattern is not compatible with Google RE2/J, an
    /// `Error::is_invalid_regular_expression_error` will be eventually returned on a call to
    /// [`Self::poll`] following this call to subscribe.
    ///
    /// This is a short-hand for [`Self::subscribe_with_pattern_listener`], which uses a no-op
    /// listener.
    ///
    /// # Arguments
    ///
    /// - `pattern` - Pattern to subscribe to
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If pattern or listener is None
    /// - `Error::is_local_illegal_state_error` If `subscribe()` is called previously with topics,
    ///   or assign is called previously (without a subsequent call to `Self::unsubscribe`), or if
    ///   not configured at-least one partition assignment strategy
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#subscribe")]
    fn subscribe_with_pattern(
        &mut self,
        pattern: SubscriptionPattern,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Subscribe to all topics matching the specified pattern, to get dynamically assigned
    /// partitions. The pattern matching will be done periodically against all topics. This is
    /// only supported under the CONSUMER group protocol (see
    /// `ConsumerConfig::GROUP_PROTOCOL_CONFIG`).
    ///
    /// If the provided pattern is not compatible with Google RE2/J, an
    /// `Error::is_invalid_regular_expression_error` will be eventually returned on a call to
    /// [`Self::poll`] following this call to subscribe.
    ///
    /// See [`Self::subscribe_with_topics_listener`] for details on the use of the
    /// [`ConsumerRebalanceListener`]. Generally, rebalances are triggered when there is a change
    /// to the topics matching the provided pattern and when consumer group membership changes.
    /// Group rebalances only take place during an active call to [`Self::poll`].
    ///
    /// # Arguments
    ///
    /// - `pattern` - Pattern to subscribe to
    /// - `listener` - Non-None listener instance to get notifications on partition
    ///   assignment/revocation for the subscribed topics
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If pattern or listener is None
    /// - `Error::is_local_illegal_state_error` If `subscribe()` is called previously with topics,
    ///   or assign is called previously (without a subsequent call to `Self::unsubscribe`), or if
    ///   not configured at-least one partition assignment strategy
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#subscribe")]
    fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Manually assign a list of partitions to this consumer. This interface does not allow for
    /// incremental assignment and will replace the previous assignment (if there is one).
    ///
    /// If the given list of topic partitions is empty, it is treated the same as
    /// `Self::unsubscribe`.
    ///
    /// Manual topic assignment through this method does not use the consumer's group management
    /// functionality. As such, there will be no rebalance operation triggered when group
    /// membership or cluster and topic metadata change. Note that it is not possible to use
    /// both manual partition assignment with [`Self::assign`] and group assignment with
    /// [`Self::subscribe_with_topics`].
    ///
    /// If auto-commit is enabled, an async commit (based on the old assignment) will be
    /// triggered before the new assignment replaces the old one.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The list of partitions to assign this consumer
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If partitions is None or contains None or empty
    ///   topics
    /// - `Error::is_local_illegal_state_error` If `subscribe()` is called previously with topics or
    ///   pattern (without a subsequent call to `Self::unsubscribe`)
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#assign")]
    fn assign(&mut self, partitions: Vec<TopicPartition>) -> impl Future<Output = Result<(), Error>> + Send;

    /// Unsubscribe from topics currently subscribed with
    /// [`Self::subscribe_with_topics`] or [`Self::subscribe_with_pattern`].
    /// This also clears any partitions directly assigned through
    /// [`Self::assign`].
    ///
    /// **Note:** Unlike [`Self::close`], this method does not commit the
    /// pending offsets before unsubscribing, even if `enable.auto.commit` is
    /// enabled. To avoid duplicate processing upon re-joining, it is
    /// recommended to explicitly call [`Self::commit_sync`] before invoking this
    /// method.
    ///
    /// # Errors
    ///
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. rebalance callback
    ///   errors)
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#unsubscribe")]
    fn unsubscribe(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    // ── Poll ──

    /// Fetch data for the topics or partitions specified using one of the subscribe/assign
    /// APIs. It is an error to not have subscribed to any topics or partitions before polling
    /// for data.
    ///
    /// On each poll, consumer will try to use the last consumed offset as the starting offset
    /// and fetch sequentially. The last consumed offset can be manually set through
    /// `Self::seek` or automatically set as the last committed offset for the subscribed list
    /// of partitions
    ///
    /// This method returns immediately if there are records available or if the position
    /// advances past control records or aborted transactions when
    /// isolation.level=read_committed. Otherwise, it will await the passed timeout. If the
    /// timeout expires, an empty record set will be returned. Note that this method may block
    /// beyond the timeout in order to execute custom `ConsumerRebalanceListener` callbacks.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time to block (must not be greater than `Long::MAX_VALUE`
    ///   milliseconds)
    ///
    /// # Returns
    ///
    /// map of topic to records since the last fetch for the subscribed list of topics and
    /// partitions
    ///
    /// # Errors
    ///
    /// - `Error::is_consumer_invalid_offset_error` if the offset for a partition or set of
    ///   partitions is undefined or out of range and no offset reset policy has been configured
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if caller lacks Read access to any of the subscribed
    ///   topics or to the configured groupId. See the error for more details
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. invalid groupId or
    ///   session timeout, errors deserializing key/value pairs, your rebalance callback thrown
    ///   errors, or any new error cases in future versions)
    /// - `Error::is_local_illegal_argument_error` if the timeout value is negative
    /// - `Error::is_local_illegal_state_error` if the consumer is not subscribed to any topics or
    ///   manually assigned any partitions to consume from
    /// - `Error::is_invalid_topic_error` if the current subscription contains any invalid topic
    ///   (per `Topic::validate`)
    /// - `Error::is_unsupported_version_error` if the consumer attempts to fetch stable offsets
    ///   when the broker doesn't support this feature. Also, if the consumer attempts to subscribe
    ///   to a SubscriptionPattern via [`Self::subscribe_with_pattern`] and the broker doesn't
    ///   support this feature.
    /// - `Error::is_fenced_instance_id_error` if this consumer instance gets fenced by broker.
    ///
    /// # Translation notes
    ///
    /// The returned future may be dropped before it completes, for example by
    /// `tokio::time::timeout` or a `tokio::select!`, also while a
    /// [`Deserializer`](crate::common::serialization::Deserializer) is
    /// pending. The call then returns no records and leaves the positions
    /// where they were before it, also when a partition's leader changed in
    /// the meantime, so a later commit of the consumed offsets covers none of
    /// the records it decoded, and a later `poll()` returns them, unless the
    /// partition was sought, reset, paused or reassigned in between.
    ///
    /// WARNING: a `timeout` greater than `i64::MAX` milliseconds counts as
    /// `i64::MAX` milliseconds, where Java throws `ArithmeticException`, so
    /// `Duration::MAX` waits without limit.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#poll")]
    fn poll(&mut self, timeout: Duration) -> impl Future<Output = Result<ConsumerRecords<K, V>, Error>> + Send;

    // ── Commit ──

    /// Commit offsets returned on the last poll() for all the subscribed list of topics and
    /// partitions.
    ///
    /// This commits offsets only to Kafka. The offsets committed using this API will be used on
    /// the first fetch after every rebalance and also on startup. As such, if you need to store
    /// offsets in anything other than Kafka, this API should not be used.
    ///
    /// This is a synchronous commit and will block until either the commit succeeds, an
    /// unrecoverable error is encountered (in which case it is returned to the caller), or the
    /// timeout specified by `default.api.timeout.ms` expires (in which case an
    /// `Error::is_timeout_error` is returned to the caller).
    ///
    /// Note that asynchronous offset commits sent previously with the [`Self::commit_async`] (or
    /// similar) are guaranteed to have their callbacks invoked prior to completion of this
    /// method, but only when the consumer is using the consumer group protocol.
    ///
    /// # Errors
    ///
    /// - `Error::is_consumer_commit_failed_error` if the commit failed and cannot be retried. This
    ///   fatal error can only occur if you are using automatic group management with
    ///   `Self::subscribe`, or if there is an active group with the same `group.id` which is using
    ///   group management. In such cases, when you are trying to commit to partitions that are no
    ///   longer assigned to this consumer because the consumer is for example no longer part of the
    ///   group this error would be thrown.
    /// - `Error::is_rebalance_in_progress_error` if the consumer instance is in the middle of a
    ///   rebalance so it is not yet determined which partitions would be assigned to the consumer.
    ///   In such cases you can first complete the rebalance by calling `Self::poll` and commit can
    ///   be reconsidered afterwards. NOTE when you reconsider committing after the rebalance, the
    ///   assigned partitions may have changed, and also for those partitions that are still
    ///   assigned their fetch positions may have changed too if more records are returned from the
    ///   `Self::poll` call.
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. if offset metadata is too
    ///   large or if the topic does not exist).
    /// - `Error::is_timeout_error` if the timeout specified by `default.api.timeout.ms` expires
    ///   before successful completion of the offset commit
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitSync")]
    fn commit_sync(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commit offsets returned on the last poll() for all the subscribed list of topics and
    /// partitions.
    ///
    /// This commits offsets only to Kafka. The offsets committed using this API will be used on
    /// the first fetch after every rebalance and also on startup. As such, if you need to store
    /// offsets in anything other than Kafka, this API should not be used.
    ///
    /// This is a synchronous commit and will block until either the commit succeeds, an
    /// unrecoverable error is encountered (in which case it is returned to the caller), or the
    /// passed timeout expires.
    ///
    /// Note that asynchronous offset commits sent previously with the [`Self::commit_async`] (or
    /// similar) are guaranteed to have their callbacks invoked prior to completion of this
    /// method, but only when the consumer is using the consumer group protocol.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum amount of time to await completion of the offset commit
    ///
    /// # Errors
    ///
    /// - `Error::is_consumer_commit_failed_error` if the commit failed and cannot be retried. This
    ///   can only occur if you are using automatic group management with `Self::subscribe`, or if
    ///   there is an active group with the same `group.id` which is using group management. In such
    ///   cases, when you are trying to commit to partitions that are no longer assigned to this
    ///   consumer because the consumer is for example no longer part of the group this error would
    ///   be thrown.
    /// - `Error::is_rebalance_in_progress_error` if the consumer instance is in the middle of a
    ///   rebalance so it is not yet determined which partitions would be assigned to the consumer.
    ///   In such cases you can first complete the rebalance by calling `Self::poll` and commit can
    ///   be reconsidered afterwards. NOTE when you reconsider committing after the rebalance, the
    ///   assigned partitions may have changed, and also for those partitions that are still
    ///   assigned their fetch positions may have changed too if more records are returned from the
    ///   `Self::poll` call, so when you retry committing you should consider updating the passed in
    ///   `offset` parameter.
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_local_illegal_argument_error` if the committed offset is negative
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. if offset metadata is too
    ///   large or if the topic does not exist).
    /// - `Error::is_timeout_error` if the timeout expires before successful completion of the
    ///   offset commit
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitSync")]
    fn commit_sync_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commit the specified offsets for the specified list of topics and partitions.
    ///
    /// This commits offsets to Kafka. The offsets committed using this API will be used on the
    /// first fetch after every rebalance and also on startup. As such, if you need to store
    /// offsets in anything other than Kafka, this API should not be used. The committed offset
    /// should be the next message your application will consume, i.e.
    /// `nextRecordToBeProcessed.offset()` (or `ConsumerRecords::next_offsets`). You should also
    /// add the leader epoch as commit metadata, which can be obtained from
    /// `ConsumerRecord::leader_epoch` or `ConsumerRecords::next_offsets`. If automatic group
    /// management with `Self::subscribe` is used, then the committed offsets must belong to the
    /// currently auto-assigned partitions.
    ///
    /// This is a synchronous commit and will block until either the commit succeeds or an
    /// unrecoverable error is encountered (in which case it is thrown to the caller), or the
    /// timeout specified by `default.api.timeout.ms` expires (in which case a `TimeoutError` is
    /// thrown to the caller).
    ///
    /// Note that asynchronous offset commits sent previously with the `Self::commit_async` (or
    /// similar) are guaranteed to have their callbacks invoked prior to completion of this
    /// method, but only when the consumer is using the consumer group protocol.
    ///
    /// # Arguments
    ///
    /// - `offsets` - A map of offsets by partition with associated metadata. This map will be
    ///   copied internally, so it is safe to mutate the map after returning.
    ///
    /// # Errors
    ///
    /// - `Error::is_consumer_commit_failed_error` if the commit failed and cannot be retried. This
    ///   can only occur if you are using automatic group management with `Self::subscribe`, or if
    ///   there is an active group with the same `group.id` which is using group management. In such
    ///   cases, when you are trying to commit to partitions that are no longer assigned to this
    ///   consumer because the consumer is for example no longer part of the group this error would
    ///   be thrown.
    /// - `Error::is_rebalance_in_progress_error` if the consumer instance is in the middle of a
    ///   rebalance so it is not yet determined which partitions would be assigned to the consumer.
    ///   In such cases you can first complete the rebalance by calling `Self::poll` and commit can
    ///   be reconsidered afterwards. NOTE when you reconsider committing after the rebalance, the
    ///   assigned partitions may have changed, and also for those partitions that are still
    ///   assigned their fetch positions may have changed too if more records are returned from the
    ///   `Self::poll` call, so when you retry committing you should consider updating the passed in
    ///   `offset` parameter.
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_local_illegal_argument_error` if the committed offset is negative
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. if offset metadata is too
    ///   large or if the topic does not exist).
    /// - `Error::is_timeout_error` if the timeout expires before successful completion of the
    ///   offset commit
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitSync")]
    fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commit the specified offsets for the specified list of topics and partitions.
    ///
    /// This commits offsets to Kafka. The offsets committed using this API will be used on the
    /// first fetch after every rebalance and also on startup. As such, if you need to store
    /// offsets in anything other than Kafka, this API should not be used. The committed offset
    /// should be the next message your application will consume, i.e.
    /// `nextRecordToBeProcessed.offset()` (or `ConsumerRecords::next_offsets`). You should also
    /// add the leader epoch as commit metadata, which can be obtained from
    /// `ConsumerRecord::leader_epoch` or `ConsumerRecords::next_offsets`. If automatic group
    /// management with `Self::subscribe` is used, then the committed offsets must belong to the
    /// currently auto-assigned partitions.
    ///
    /// This is a synchronous commit and will block until either the commit succeeds, an
    /// unrecoverable error is encountered (in which case it is thrown to the caller), or the
    /// timeout expires.
    ///
    /// Note that asynchronous offset commits sent previously with the `Self::commit_async` (or
    /// similar) are guaranteed to have their callbacks invoked prior to completion of this
    /// method, but only when the consumer is using the consumer group protocol.
    ///
    /// # Arguments
    ///
    /// - `offsets` - A map of offsets by partition with associated metadata. This map will be
    ///   copied internally, so it is safe to mutate the map after returning.
    /// - `timeout` - The maximum amount of time to await completion of the offset commit
    ///
    /// # Errors
    ///
    /// - `Error::is_consumer_commit_failed_error` if the commit failed and cannot be retried. This
    ///   can only occur if you are using automatic group management with `Self::subscribe`, or if
    ///   there is an active group with the same `group.id` which is using group management. In such
    ///   cases, when you are trying to commit to partitions that are no longer assigned to this
    ///   consumer because the consumer is for example no longer part of the group this error would
    ///   be thrown.
    /// - `Error::is_rebalance_in_progress_error` if the consumer instance is in the middle of a
    ///   rebalance so it is not yet determined which partitions would be assigned to the consumer.
    ///   In such cases you can first complete the rebalance by calling `Self::poll` and commit can
    ///   be reconsidered afterwards. NOTE when you reconsider committing after the rebalance, the
    ///   assigned partitions may have changed, and also for those partitions that are still
    ///   assigned their fetch positions may have changed too if more records are returned from the
    ///   `Self::poll` call, so when you retry committing you should consider updating the passed in
    ///   `offset` parameter.
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_local_illegal_argument_error` if the committed offset is negative
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. if offset metadata is too
    ///   large or if the topic does not exist).
    /// - `Error::is_timeout_error` if the timeout expires before successful completion of the
    ///   offset commit
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitSync")]
    fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commit offsets returned on the last `Self::poll` for all the subscribed list of topics
    /// and partition. Same as commitAsync(None)
    ///
    /// # Errors
    ///
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitAsync")]
    fn commit_async(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commit offsets returned on the last poll() for the subscribed list of topics and
    /// partitions.
    ///
    /// This commits offsets only to Kafka. The offsets committed using this API will be used on
    /// the first fetch after every rebalance and also on startup. As such, if you need to store
    /// offsets in anything other than Kafka, this API should not be used.
    ///
    /// This is an asynchronous call and will not block. Any errors encountered are either
    /// passed to the callback (if provided) or discarded.
    ///
    /// Offsets committed through multiple calls to this API are guaranteed to be sent in the
    /// same order as the invocations. Corresponding commit callbacks are also invoked in the
    /// same order. Additionally note that offsets committed through this API are guaranteed to
    /// complete before a subsequent call to `Self::commit_sync` (and variants) returns.
    ///
    /// # Arguments
    ///
    /// - `callback` - Callback to invoke when the commit completes
    ///
    /// # Errors
    ///
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitAsync")]
    fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Commit the specified offsets for the specified list of topics and partitions to Kafka.
    ///
    /// This commits offsets to Kafka. The offsets committed using this API will be used on the
    /// first fetch after every rebalance and also on startup. As such, if you need to store
    /// offsets in anything other than Kafka, this API should not be used. The committed offset
    /// should be the next message your application will consume, i.e.
    /// `nextRecordToBeProcessed.offset()` (or `ConsumerRecords::next_offsets`). You should also
    /// add the leader epoch as commit metadata, which can be obtained from
    /// `ConsumerRecord::leader_epoch` or `ConsumerRecords::next_offsets`. If automatic group
    /// management with `Self::subscribe` is used, then the committed offsets must belong to the
    /// currently auto-assigned partitions.
    ///
    /// This is an asynchronous call and will not block. Any errors encountered are either
    /// passed to the callback (if provided) or discarded.
    ///
    /// Offsets committed through multiple calls to this API are guaranteed to be sent in the
    /// same order as the invocations. Corresponding commit callbacks are also invoked in the
    /// same order. Additionally note that offsets committed through this API are guaranteed to
    /// complete before a subsequent call to `Self::commit_sync` (and variants) returns.
    ///
    /// # Arguments
    ///
    /// - `offsets` - A map of offsets by partition with associated metadata. This map will be
    ///   copied internally, so it is safe to mutate the map after returning.
    /// - `callback` - Callback to invoke when the commit completes
    ///
    /// # Errors
    ///
    /// - `Error::is_fenced_instance_id_error` if this consumer is using the classic group protocol
    ///   and this instance gets fenced by broker.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#commitAsync")]
    fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    // ── Seek (async — Java: addAndGet on SeekUnvalidatedEvent / ResetOffsetEvent) ──

    /// Overrides the fetch offsets that the consumer will use on the next poll(timeout). If
    /// this API is invoked for the same partition more than once, the latest offset will be
    /// used on the next poll(). Note that you may lose data if this API is arbitrarily used in
    /// the middle of consumption, to reset the fetch offsets
    ///
    /// The next Consumer Record which will be retrieved when poll() is invoked will have the
    /// offset specified, given that a record with that offset exists (i.e. it is a valid
    /// offset).
    ///
    /// `Self::seek_to_beginning` will go to the first offset in the topic. seek(0) is
    /// equivalent to seekToBeginning for a TopicPartition with beginning offset 0, assuming
    /// that there is a record at offset 0 still available. `Self::seek_to_end` is equivalent to
    /// seeking to the last offset of the partition, but behavior depends on `isolation.level`,
    /// so see `Self::seek_to_end` documentation for more details.
    ///
    /// Seeking to the offset smaller than the log start offset or larger than the log end
    /// offset means an invalid offset is reached. Invalid offset behaviour is controlled by the
    /// `auto.offset.reset` property. If this is set to "earliest", the next poll will return
    /// records from the starting offset. If it is set to "latest", it will seek to the last
    /// offset (similar to seekToEnd()). If it is set to "none", an
    /// `ConsumerOffsetOutOfRangeError` will be thrown.
    ///
    /// Note that, the seek offset won't change to the in-flight fetch request, it will take
    /// effect in next fetch request. So, the consumer might wait for `fetch.max.wait.ms` before
    /// starting to fetch the records from desired offset.
    ///
    /// # Arguments
    ///
    /// - `partition` - the TopicPartition on which the seek will be performed.
    /// - `offset` - the next offset returned by poll().
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if the provided offset is negative
    /// - `Error::is_local_illegal_state_error` if the provided TopicPartition is not assigned to
    ///   this consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#seek")]
    fn seek_with_offset(
        &mut self,
        partition: TopicPartition,
        offset: i64,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Overrides the fetch offsets that the consumer will use on the next
    /// `poll(timeout)`. If this API is invoked for the same partition more than
    /// once, the latest offset will be used on the next `poll()`. Note that you
    /// may lose data if this API is arbitrarily used in the middle of
    /// consumption, to reset the fetch offsets. This method allows for setting
    /// the leaderEpoch along with the desired offset.
    ///
    /// # Arguments
    ///
    /// - `partition` - the TopicPartition on which the seek will be performed.
    /// - `offset_and_metadata` - the next offset returned by poll(), with its
    ///   leader epoch.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if the provided offset is negative
    /// - `Error::is_local_illegal_state_error` if the provided TopicPartition is not assigned to
    ///   this consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#seek")]
    fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Seek to the first offset for each of the given partitions. This function
    /// evaluates lazily, seeking to the first offset in all partitions only when
    /// [`Self::poll`] or [`Self::position`] are called. If no partitions are
    /// provided, seek to the first offset for all of the currently assigned
    /// partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions to seek
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `partitions` is `None`
    /// - `Error::is_local_illegal_state_error` if any of the provided partitions are not currently
    ///   assigned to this consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#seekToBeginning")]
    fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send;

    /// Seek to the last offset for each of the given partitions. This function
    /// evaluates lazily, seeking to the final offset in all partitions only when
    /// [`Self::poll`] or [`Self::position`] are called. If no partitions are
    /// provided, seek to the final offset for all of the currently assigned
    /// partitions.
    ///
    /// If `isolation.level=read_committed`, the end offset will be the Last
    /// Stable Offset, i.e., the offset of the first message with an open
    /// transaction.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions to seek
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `partitions` is `None`
    /// - `Error::is_local_illegal_state_error` if any of the provided partitions are not currently
    ///   assigned to this consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#seekToEnd")]
    fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send;

    // ── Position / committed (async — may fetch from broker) ──

    /// Get the offset of the *next record* that will be fetched (if a record with that offset
    /// exists). This method may issue a remote call to the server if there is no current
    /// position for the given partition.
    ///
    /// This call will block until either the position could be determined or an unrecoverable
    /// error is encountered (in which case it is thrown to the caller), or the timeout
    /// specified by `default.api.timeout.ms` expires (in which case a `TimeoutError` is thrown
    /// to the caller).
    ///
    /// # Arguments
    ///
    /// - `partition` - The partition to get the position for
    ///
    /// # Returns
    ///
    /// The current position of the consumer (that is, the offset of the next record to be fetched)
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the provided TopicPartition is not assigned to
    ///   this consumer
    /// - `Error::is_consumer_invalid_offset_error` if no offset is currently defined for the
    ///   partition
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_unsupported_version_error` if the consumer attempts to fetch stable offsets
    ///   when the broker doesn't support this feature
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    /// - `Error::is_timeout_error` if the position cannot be determined before the timeout
    ///   specified by `default.api.timeout.ms` expires
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#position")]
    fn position(&mut self, partition: &TopicPartition) -> impl Future<Output = Result<i64, Error>> + Send;

    /// Get the offset of the *next record* that will be fetched (if a record with that offset
    /// exists). This method may issue a remote call to the server if there is no current
    /// position for the given partition.
    ///
    /// This call will block until the position can be determined, an unrecoverable error is
    /// encountered (in which case it is thrown to the caller), or the timeout expires.
    ///
    /// # Arguments
    ///
    /// - `partition` - The partition to get the position for
    /// - `timeout` - The maximum amount of time to await determination of the current position
    ///
    /// # Returns
    ///
    /// The current position of the consumer (that is, the offset of the next record to be fetched)
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the provided TopicPartition is not assigned to
    ///   this consumer
    /// - `Error::is_consumer_invalid_offset_error` if no offset is currently defined for the
    ///   partition
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_timeout_error` if the position cannot be determined before the passed timeout
    ///   expires
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#position")]
    fn position_with_timeout(
        &mut self,
        partition: &TopicPartition,
        timeout: Duration,
    ) -> impl Future<Output = Result<i64, Error>> + Send;

    /// Get the last committed offsets for the given partitions (whether the commit happened by
    /// this process or another). The returned offsets will be used as the position for the
    /// consumer in the event of a failure.
    ///
    /// If any of the partitions requested do not exist, the result map will contain `None` as the
    /// value for that partition.
    ///
    /// This call will do a remote call to get the latest committed offsets from the server, and
    /// will block until the committed offsets are gotten successfully, an unrecoverable error
    /// is encountered (in which case it is thrown to the caller), or the timeout specified by
    /// `default.api.timeout.ms` expires (in which case a `TimeoutError` is thrown to the
    /// caller).
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions to check
    ///
    /// # Returns
    ///
    /// The latest committed offsets for the given partitions; `None` will be returned for the
    /// partition if there is no such message.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_unsupported_version_error` if the consumer attempts to fetch stable offsets
    ///   when the broker doesn't support this feature
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    /// - `Error::is_timeout_error` if the committed offset cannot be found before the timeout
    ///   specified by `default.api.timeout.ms` expires.
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#committed")]
    fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> impl Future<Output = Result<HashMap<TopicPartition, Option<OffsetAndMetadata>>, Error>> + Send;

    /// Get the last committed offsets for the given partitions (whether the commit happened by
    /// this process or another). The returned offsets will be used as the position for the
    /// consumer in the event of a failure.
    ///
    /// If any of the partitions requested do not exist, the result map will contain `None` as the
    /// value for that partition.
    ///
    /// This call will block to do a remote call to get the latest committed offsets from the
    /// server.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions to check
    /// - `timeout` - The maximum amount of time to await the latest committed offsets
    ///
    /// # Returns
    ///
    /// The latest committed offsets for the given partitions; `None` will be returned for the
    /// partition if there is no such message.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    /// - `Error::is_timeout_error` if the committed offset cannot be found before expiration of the
    ///   timeout
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#committed")]
    fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, Option<OffsetAndMetadata>>, Error>> + Send;

    // ── Metadata (async — may fetch from broker) ──

    /// Get metadata about the partitions for a given topic. This method will issue a remote
    /// call to the server if it does not already have any metadata about the given topic.
    ///
    /// Returns the list of partitions, which will be empty when the given topic
    /// is not found. Note: when both the broker config
    /// `auto.create.topics.enable` and the consumer config
    /// `allow.auto.create.topics` are `true`, this method may return an empty
    /// list even though the topic is being auto-created in the background.
    /// Callers should not assume the topic does not exist based solely on an
    /// empty result.
    ///
    /// # Arguments
    ///
    /// - `topic` - The topic to get partition metadata for
    ///
    /// # Returns
    ///
    /// The list of partitions, which will be empty when the given topic is not found. Note: when
    /// both the broker config `auto.create.topics.enable` and the consumer config
    /// `allow.auto.create.topics` are `true`, this method may return an empty list even though the
    /// topic is being auto-created in the background. Callers should not assume the topic does not
    /// exist based solely on an empty result.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the specified topic. See the error
    ///   for more details
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before the amount of
    ///   time allocated by `default.api.timeout.ms` expires.
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#partitionsFor")]
    fn partitions_for(&mut self, topic: &str) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send;

    /// Get metadata about the partitions for a given topic. This method will issue a remote
    /// call to the server if it does not already have any metadata about the given topic.
    ///
    /// Returns the list of partitions, which will be empty when the given topic
    /// is not found; see [`Self::partitions_for`] for the auto-topic-creation
    /// caveat on an empty result.
    ///
    /// # Arguments
    ///
    /// - `topic` - The topic to get partition metadata for
    /// - `timeout` - The maximum of time to await topic metadata
    ///
    /// # Returns
    ///
    /// The list of partitions, which will be empty when the given topic is not found. Note: when
    /// both the broker config `auto.create.topics.enable` and the consumer config
    /// `allow.auto.create.topics` are `true`, this method may return an empty list even though the
    /// topic is being auto-created in the background. Callers should not assume the topic does not
    /// exist based solely on an empty result.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the specified topic. See the error
    ///   for more details
    /// - `Error::is_timeout_error` if topic metadata cannot be fetched before expiration of the
    ///   passed timeout
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#partitionsFor")]
    fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send;

    /// Get metadata about partitions for all topics that the user is authorized to view. This
    /// method will issue a remote call to the server.
    ///
    /// # Returns
    ///
    /// The map of topics and its partitions
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before the amount of
    ///   time allocated by `default.api.timeout.ms` expires.
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#listTopics")]
    fn list_topics(&mut self) -> impl Future<Output = Result<HashMap<String, Vec<PartitionInfo>>, Error>> + Send;

    /// Get metadata about partitions for all topics that the user is authorized to view. This
    /// method will issue a remote call to the server.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time this operation will block to fetch topic metadata
    ///
    /// # Returns
    ///
    /// The map of topics and its partitions
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_timeout_error` if the topic metadata could not be fetched before expiration of
    ///   the passed timeout
    /// - `Error::is_kafka_error` for any other unrecoverable errors
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#listTopics")]
    fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<String, Vec<PartitionInfo>>, Error>> + Send;

    /// Look up the offsets for the given partitions by timestamp. The returned offset for each
    /// partition is the earliest offset whose timestamp is greater than or equal to the given
    /// timestamp in the corresponding partition. This is a blocking call. The consumer does not
    /// have to be assigned the partitions. If the message format version in a partition is
    /// before 0.10.0, i.e. the messages do not have timestamps, `None` will be returned for
    /// that partition.
    ///
    /// # Arguments
    ///
    /// - `timestamps_to_search` - the mapping from partition to the timestamp to look up.
    ///
    /// # Returns
    ///
    /// a mapping from partition to the timestamp and offset of the first message with timestamp
    /// greater than or equal to the target timestamp. If the timestamp and offset for a specific
    /// partition cannot be found within the default timeout, and no corresponding message exists,
    /// the entry in the returned map will be `None`
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic(s). See the error for more
    ///   details
    /// - `Error::is_local_illegal_argument_error` if the target timestamp is negative
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before the amount of
    ///   time allocated by `default.api.timeout.ms` expires.
    /// - `Error::is_unsupported_version_error` if the broker does not support looking up the
    ///   offsets by timestamp
    /// - `Error::is_bootstrap_resolution_error` if DNS resolution of the bootstrap servers fails
    ///   within `bootstrap.resolve.timeout.ms`
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#offsetsForTimes")]
    fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, Option<OffsetAndTimestamp>>, Error>> + Send;

    /// Look up the offsets for the given partitions by timestamp. The returned offset for each
    /// partition is the earliest offset whose timestamp is greater than or equal to the given
    /// timestamp in the corresponding partition. This is a blocking call. The consumer does not
    /// have to be assigned the partitions. If the message format version in a partition is
    /// before 0.10.0, i.e. the messages do not have timestamps, `None` will be returned for
    /// that partition.
    ///
    /// # Arguments
    ///
    /// - `timestamps_to_search` - the mapping from partition to the timestamp to look up.
    /// - `timeout` - The maximum amount of time to await retrieval of the offsets
    ///
    /// # Returns
    ///
    /// a mapping from partition to the timestamp and offset of the first message with timestamp
    /// greater than or equal to the target timestamp. If the timestamp and offset for a specific
    /// partition cannot be found within timeout, and no corresponding message exists, the
    /// entry in the returned map will be `None`
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic(s). See the error for more
    ///   details
    /// - `Error::is_local_illegal_argument_error` if the target timestamp is negative
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before expiration of
    ///   the passed timeout
    /// - `Error::is_unsupported_version_error` if the broker does not support looking up the
    ///   offsets by timestamp
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#offsetsForTimes")]
    fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, Option<OffsetAndTimestamp>>, Error>> + Send;

    /// Get the first offset for the given partitions.
    ///
    /// This method does not change the current consumer position of the partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - the partitions to get the earliest offsets.
    ///
    /// # Returns
    ///
    /// The earliest available offsets for the given partitions
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic(s). See the error for more
    ///   details
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before expiration of
    ///   the configured `default.api.timeout.ms`
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#beginningOffsets")]
    fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send;

    /// Get the first offset for the given partitions.
    ///
    /// This method does not change the current consumer position of the partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - the partitions to get the earliest offsets
    /// - `timeout` - The maximum amount of time to await retrieval of the beginning offsets
    ///
    /// # Returns
    ///
    /// The earliest available offsets for the given partitions, and it will return empty map if
    /// zero timeout is provided
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic(s). See the error for more
    ///   details
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before expiration of
    ///   the passed timeout
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#beginningOffsets")]
    fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send;

    /// Get the end offsets for the given partitions. In the default `read_uncommitted`
    /// isolation level, the end offset is the high watermark (that is, the offset of the last
    /// successfully replicated message plus one). For `read_committed` consumers, the end
    /// offset is the last stable offset (LSO), which is the minimum of the high watermark and
    /// the smallest offset of any open transaction. Finally, if the partition has never been
    /// written to, the end offset is 0.
    ///
    /// This method does not change the current consumer position of the partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - the partitions to get the end offsets.
    ///
    /// # Returns
    ///
    /// The end offsets for the given partitions.
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic(s). See the error for more
    ///   details
    /// - `Error::is_timeout_error` if the offset metadata could not be fetched before the amount of
    ///   time allocated by `default.api.timeout.ms` expires
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#endOffsets")]
    fn end_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send;

    /// Get the end offsets for the given partitions. In the default `read_uncommitted`
    /// isolation level, the end offset is the high watermark (that is, the offset of the last
    /// successfully replicated message plus one). For `read_committed` consumers, the end
    /// offset is the last stable offset (LSO), which is the minimum of the high watermark and
    /// the smallest offset of any open transaction. Finally, if the partition has never been
    /// written to, the end offset is 0.
    ///
    /// This method does not change the current consumer position of the partitions.
    ///
    /// # Arguments
    ///
    /// - `partitions` - the partitions to get the end offsets.
    /// - `timeout` - The maximum amount of time to await retrieval of the end offsets
    ///
    /// # Returns
    ///
    /// The end offsets for the given partitions, and it will return empty map if zero timeout is
    /// provided
    ///
    /// # Errors
    ///
    /// - `Error::is_authentication_error` if authentication fails. See the error for more details
    /// - `Error::is_authorization_error` if not authorized to the topic(s). See the error for more
    ///   details
    /// - `Error::is_timeout_error` if the offsets could not be fetched before expiration of the
    ///   passed timeout
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#endOffsets")]
    fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send;

    // ── Pause / resume (async — Java: addAndGet on PausePartitions / ResumePartitions) ──

    /// Suspend fetching from the requested partitions. Future calls to
    /// [`Self::poll`] will not return any records from these partitions until
    /// they have been resumed using [`Self::resume`]. Note that this method
    /// does not affect partition subscription. In particular, it does not cause
    /// a group rebalance when automatic assignment is used.
    ///
    /// The pause state is preserved across a rebalance for partitions that
    /// remain assigned to this consumer, but it is lost for partitions that are
    /// revoked. Which partitions are revoked depends on the group protocol in
    /// use (see [`ConsumerConfig::GROUP_PROTOCOL_CONFIG`]):
    ///
    /// - Classic group protocol: the behavior depends on the assignor
    ///   configured in `partition.assignment.strategy`: eager assignors (e.g.
    ///   `RangeAssignor`, `RoundRobinAssignor`) revoke all partitions on every
    ///   rebalance (pause state is not preserved); cooperative assignors (e.g.
    ///   `CooperativeStickyAssignor`) only revoke the partitions that are
    ///   reassigned to another consumer (pause state preserved for partitions
    ///   that remain assigned). This client does not implement the classic
    ///   protocol yet.
    /// - Consumer group protocol (KIP-848): only revokes partitions that are
    ///   reassigned to another consumer (pause state preserved for partitions
    ///   that remain assigned).
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions which should be paused
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if any of the provided partitions are not currently
    ///   assigned to this consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#pause")]
    fn pause(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send;

    /// Resume specified partitions which have been paused with `Self::pause`. New calls to
    /// `Self::poll` will return records from these partitions if there are any to be fetched.
    /// If the partitions were not previously paused, this method is a no-op.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The partitions which should be resumed
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if any of the provided partitions are not currently
    ///   assigned to this consumer
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#resume")]
    fn resume(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send;

    // ── Lifecycle ──

    /// Alert the consumer to trigger a new rebalance by rejoining the group.
    /// This is a nonblocking call that forces the consumer to trigger a new
    /// rebalance on the next [`Self::poll`] call. Note that this API does not
    /// itself initiate the rebalance, so you must still call [`Self::poll`]. If
    /// a rebalance is already in progress this call will be a no-op. If you wish
    /// to force an additional rebalance you must complete the current one by
    /// calling poll before retrying this API.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the consumer does not use group subscription
    ///
    /// # Translation notes
    ///
    /// With `group.protocol=consumer` the call only logs a warning, as Java's.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#enforceRebalance")]
    fn enforce_rebalance(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Alert the consumer to trigger a new rebalance by rejoining the group.
    /// This is a nonblocking call that forces the consumer to trigger a new
    /// rebalance on the next [`Self::poll`] call. Note that this API does not
    /// itself initiate the rebalance, so you must still call [`Self::poll`]. If
    /// a rebalance is already in progress this call will be a no-op. If you wish
    /// to force an additional rebalance you must complete the current one by
    /// calling poll before retrying this API.
    ///
    /// # Arguments
    ///
    /// - `reason` - The reason why the new rebalance is needed.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the consumer does not use group subscription
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#enforceRebalance")]
    fn enforce_rebalance_with_reason(&mut self, reason: &str) -> impl Future<Output = Result<(), Error>> + Send;

    /// Close the consumer with default leave group behavior, waiting for up to the default
    /// timeout of 30 seconds for any needed cleanup. If auto-commit is enabled, this will
    /// commit the current offsets if possible within the default timeout. See `Self::close` for
    /// details. Note that `Self::wakeup` cannot be used to interrupt close.
    ///
    /// This close operation will attempt all shutdown steps even if one of them fails. It logs
    /// all encountered errors, continues to execute the next steps, and finally throws the
    /// first error found.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_kafka_error` for any other error during close (e.g., errors thrown from
    ///   rebalance callbacks or commit callbacks from previous asynchronous commits)
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#close()")]
    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Close the consumer cleanly. [`CloseOptions`] allows to specify a timeout
    /// and a leave group behavior. If no timeout is specified, the default
    /// timeout of 30 seconds is used. If no leave group behavior is specified,
    /// the default leave group behavior is used.
    ///
    /// This method waits up to the timeout for the consumer to complete pending
    /// commits and maybe leave the group, depending on the specified leave group
    /// behavior. If auto-commit is enabled, this will commit the current offsets
    /// if possible within the timeout. If the consumer is unable to complete
    /// offset commits and to gracefully leave the group (if applicable) before
    /// the timeout expires, the consumer is force closed. Note that
    /// [`Self::wakeup`] cannot be used to interrupt close.
    ///
    /// # Arguments
    ///
    /// - `options` - see [`CloseOptions`]
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If the `options` timeout is negative
    /// - `Error::is_wakeup_error` if `Self::wakeup` is called before or while this function is
    ///   called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_kafka_error` for any other error during close (e.g., errors thrown from
    ///   rebalance callbacks or commit callbacks from previous asynchronous commits)
    // Java's deprecated `close(Duration)` is not translated (CLAUDE.md §3): pass
    // `CloseOptions::new_with_timeout(..)` instead.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#close(CloseOptions)")]
    fn close_with_options(&mut self, options: CloseOptions) -> impl Future<Output = Result<(), Error>> + Send;

    /// Wakeup the consumer. This method is thread-safe and is useful in particular to abort a
    /// long poll. The thread which is blocking in an operation will throw `WakeupError`. If no
    /// thread is blocking in a method which can throw `WakeupError`, the next call to such a
    /// method will raise it instead.
    ///
    /// # Translation notes
    ///
    /// The consumer is not `Sync`: from another task, call
    /// [`ConsumerHandle::wakeup`] on [`Self::handle`]'s handle.
    #[doc(alias = "org.apache.kafka.clients.consumer.Consumer#wakeup")]
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
    /// `&mut self` for the duration of a blocking call and a consumer is not
    /// `Clone`, so neither is expressible with a
    /// bare reference; capture a [`ConsumerHandle`] instead. See
    /// [`ConsumerHandle`].
    ///
    /// The handle carries the other half of the `&mut self` exclusivity: its
    /// async operations are admitted only while this consumer is running a
    /// callback (a `ConsumerRebalanceListener` method or an
    /// `OffsetCommitCallback`), and otherwise return the
    /// `ConcurrentModificationError` Java's `acquire()` throws. `wakeup` and
    /// the sync reads work from any thread at any time. See the `Threads`
    /// section of [`ConsumerHandle`].
    // listener reentrancy handle (consumer-threading.md §31, §41)
    #[doc(alias = "rust-only")]
    fn handle(&self) -> ConsumerHandle;
}

pub use consumer_commit_failed_error::ConsumerCommitFailedError;
pub use consumer_log_truncation_error::ConsumerLogTruncationError;
pub use consumer_no_offset_for_partition_error::ConsumerNoOffsetForPartitionError;
pub use consumer_offset_out_of_range_error::ConsumerOffsetOutOfRangeError;
pub use consumer_retriable_commit_failed_error::ConsumerRetriableCommitFailedError;
