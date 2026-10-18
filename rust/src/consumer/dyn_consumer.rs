// Copyright 2026 Confluent Inc.
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

//! The dyn-compatible view of [`Consumer`].
//!
//! Has no Java counterpart (`definition-of-done.md` §7). A Java interface is
//! always usable as a runtime-polymorphic type, so a `List<Consumer<K, V>>`
//! can hold a `KafkaConsumer` next to a `MockConsumer`. The Rust
//! [`Consumer`] trait returns `impl Future` from its asynchronous methods,
//! which keeps statically dispatched calls free of a boxed future, but makes
//! the trait itself not dyn-compatible. `DynConsumer` restores the Java
//! capability (CLAUDE.md §3, "traits must be dyn-compatible") by boxing each
//! future, and only callers that go through `dyn` pay for it. It is the
//! consumer counterpart of [`DynProducer`](crate::producer::DynProducer).

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::KafkaMetric;
use crate::common::{Error, MetricName, PartitionInfo, TopicPartition};
use crate::consumer::{
    CloseOptions, Consumer, ConsumerGroupMetadata, ConsumerHandle, ConsumerRebalanceListener, ConsumerRecords,
    OffsetAndMetadata, OffsetAndTimestamp, OffsetCommitCallback, SubscriptionPattern,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

mod private {
    // Deliberately unnameable: it seals `DynConsumer` to `Consumer` implementors.
    #[expect(unnameable_types)]
    pub trait Sealed<K, V> {}
}

impl<K, V, C> private::Sealed<K, V> for C
where
    K: Send + 'static,
    V: Send + 'static,
    C: Consumer<K, V>,
{
}

/// A dyn-compatible [`Consumer`], for holding different consumer
/// implementations behind one type, e.g. `Box<dyn DynConsumer<K, V>>`.
///
/// Every [`Consumer`] implements `DynConsumer` automatically, and the trait is
/// sealed, so it cannot be implemented directly: implement [`Consumer`]
/// instead. Each method forwards to the [`Consumer`] method of the same name;
/// asynchronous methods return the future boxed, which costs one allocation per
/// call. `dyn DynConsumer<K, V>` implements [`Consumer`] in turn, so a boxed
/// consumer can be passed as `&mut *boxed` to generic code taking
/// `C: Consumer<K, V> + ?Sized`, and its methods can be called with only
/// [`Consumer`] in scope.
///
/// With both traits in scope a method call on a concrete consumer (a
/// `KafkaConsumer` or `MockConsumer`) is ambiguous, since both apply to it;
/// call it as `Consumer::poll(&mut consumer, timeout)`, or import only one of
/// the two traits. A call on a `dyn DynConsumer` resolves to `DynConsumer`'s
/// own method either way.
///
/// ```
/// use confluent_kafka::consumer::{DynConsumer, MockConsumer};
///
/// let consumers: Vec<Box<dyn DynConsumer<String, String>>> = vec![
///     Box::new(MockConsumer::<String, String>::new("earliest").unwrap()),
///     Box::new(MockConsumer::<String, String>::new("latest").unwrap()),
/// ];
/// assert_eq!(2, consumers.len());
/// ```
// the dyn-compatible companion of the `Consumer` trait (CLAUDE.md §3)
#[doc(alias = "rust-only")]
pub trait DynConsumer<K, V>: private::Sealed<K, V> + Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// See [`Consumer::assignment`].
    fn assignment(&self) -> HashSet<TopicPartition>;

    /// See [`Consumer::subscription`].
    fn subscription(&self) -> HashSet<String>;

    /// See [`Consumer::paused`].
    fn paused(&self) -> HashSet<TopicPartition>;

    /// See [`Consumer::group_metadata`].
    fn group_metadata(&self) -> Arc<dyn ConsumerGroupMetadata>;

    /// See [`Consumer::client_id`].
    fn client_id(&self) -> &str;

    /// See [`Consumer::current_lag`].
    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64>;

    /// See [`Consumer::metrics`].
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;

    /// See [`Consumer::subscribe_with_topics`].
    fn subscribe_with_topics<'a>(&'a mut self, topics: Vec<String>) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::subscribe_with_topics_listener`].
    fn subscribe_with_topics_listener<'a>(
        &'a mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::subscribe_with_pattern`].
    fn subscribe_with_pattern<'a>(&'a mut self, pattern: SubscriptionPattern) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::subscribe_with_pattern_listener`].
    fn subscribe_with_pattern_listener<'a>(
        &'a mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::assign`].
    fn assign<'a>(&'a mut self, partitions: Vec<TopicPartition>) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::unsubscribe`].
    fn unsubscribe<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::poll`].
    fn poll<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<ConsumerRecords<K, V>, Error>>;

    /// See [`Consumer::commit_sync`].
    fn commit_sync<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::commit_sync_with_timeout`].
    fn commit_sync_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::commit_sync_with_offsets`].
    fn commit_sync_with_offsets<'a>(
        &'a mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::commit_sync_with_offsets_timeout`].
    fn commit_sync_with_offsets_timeout<'a>(
        &'a mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::commit_async`].
    fn commit_async<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::commit_async_with_callback`].
    fn commit_async_with_callback<'a>(
        &'a mut self,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::commit_async_with_offsets_callback`].
    fn commit_async_with_offsets_callback<'a>(
        &'a mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::seek_with_offset`].
    fn seek_with_offset<'a>(&'a mut self, partition: TopicPartition, offset: i64) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::seek_with_offset_and_metadata`].
    fn seek_with_offset_and_metadata<'a>(
        &'a mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::seek_to_beginning`].
    fn seek_to_beginning<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::seek_to_end`].
    fn seek_to_end<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::position`].
    fn position<'a>(&'a mut self, partition: &'a TopicPartition) -> BoxFuture<'a, Result<i64, Error>>;

    /// See [`Consumer::position_with_timeout`].
    fn position_with_timeout<'a>(
        &'a mut self,
        partition: &'a TopicPartition,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<i64, Error>>;

    /// See [`Consumer::committed`].
    fn committed<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>>;

    /// See [`Consumer::committed_with_timeout`].
    fn committed_with_timeout<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>>;

    /// See [`Consumer::partitions_for`].
    fn partitions_for<'a>(&'a mut self, topic: &'a str) -> BoxFuture<'a, Result<Vec<PartitionInfo>, Error>>;

    /// See [`Consumer::partitions_for_with_timeout`].
    fn partitions_for_with_timeout<'a>(
        &'a mut self,
        topic: &'a str,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Vec<PartitionInfo>, Error>>;

    /// See [`Consumer::list_topics`].
    fn list_topics<'a>(&'a mut self) -> BoxFuture<'a, Result<HashMap<String, Vec<PartitionInfo>>, Error>>;

    /// See [`Consumer::list_topics_with_timeout`].
    fn list_topics_with_timeout<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<String, Vec<PartitionInfo>>, Error>>;

    /// See [`Consumer::offsets_for_times`].
    fn offsets_for_times<'a>(
        &'a mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>>;

    /// See [`Consumer::offsets_for_times_with_timeout`].
    fn offsets_for_times_with_timeout<'a>(
        &'a mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>>;

    /// See [`Consumer::beginning_offsets`].
    fn beginning_offsets<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>>;

    /// See [`Consumer::beginning_offsets_with_timeout`].
    fn beginning_offsets_with_timeout<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>>;

    /// See [`Consumer::end_offsets`].
    fn end_offsets<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>>;

    /// See [`Consumer::end_offsets_with_timeout`].
    fn end_offsets_with_timeout<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>>;

    /// See [`Consumer::pause`].
    fn pause<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::resume`].
    fn resume<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::enforce_rebalance`].
    fn enforce_rebalance<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::enforce_rebalance_with_reason`].
    fn enforce_rebalance_with_reason<'a>(&'a mut self, reason: &'a str) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::close`].
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::close_with_options`].
    fn close_with_options<'a>(&'a mut self, options: CloseOptions) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`Consumer::wakeup`].
    fn wakeup(&self);

    /// See [`Consumer::handle`].
    fn handle(&self) -> ConsumerHandle;
}

impl<K, V, C> DynConsumer<K, V> for C
where
    K: Send + 'static,
    V: Send + 'static,
    C: Consumer<K, V>,
{
    fn assignment(&self) -> HashSet<TopicPartition> {
        Consumer::assignment(self)
    }

    fn subscription(&self) -> HashSet<String> {
        Consumer::subscription(self)
    }

    fn paused(&self) -> HashSet<TopicPartition> {
        Consumer::paused(self)
    }

    fn group_metadata(&self) -> Arc<dyn ConsumerGroupMetadata> {
        Consumer::group_metadata(self)
    }

    fn client_id(&self) -> &str {
        Consumer::client_id(self)
    }

    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64> {
        Consumer::current_lag(self, topic_partition)
    }

    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        Consumer::metrics(self)
    }

    fn subscribe_with_topics<'a>(&'a mut self, topics: Vec<String>) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::subscribe_with_topics(self, topics))
    }

    fn subscribe_with_topics_listener<'a>(
        &'a mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::subscribe_with_topics_listener(self, topics, listener))
    }

    fn subscribe_with_pattern<'a>(&'a mut self, pattern: SubscriptionPattern) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::subscribe_with_pattern(self, pattern))
    }

    fn subscribe_with_pattern_listener<'a>(
        &'a mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::subscribe_with_pattern_listener(self, pattern, listener))
    }

    fn assign<'a>(&'a mut self, partitions: Vec<TopicPartition>) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::assign(self, partitions))
    }

    fn unsubscribe<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::unsubscribe(self))
    }

    fn poll<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<ConsumerRecords<K, V>, Error>> {
        Box::pin(Consumer::poll(self, timeout))
    }

    fn commit_sync<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_sync(self))
    }

    fn commit_sync_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_sync_with_timeout(self, timeout))
    }

    fn commit_sync_with_offsets<'a>(
        &'a mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_sync_with_offsets(self, offsets))
    }

    fn commit_sync_with_offsets_timeout<'a>(
        &'a mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_sync_with_offsets_timeout(self, offsets, timeout))
    }

    fn commit_async<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_async(self))
    }

    fn commit_async_with_callback<'a>(
        &'a mut self,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_async_with_callback(self, callback))
    }

    fn commit_async_with_offsets_callback<'a>(
        &'a mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::commit_async_with_offsets_callback(self, offsets, callback))
    }

    fn seek_with_offset<'a>(&'a mut self, partition: TopicPartition, offset: i64) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::seek_with_offset(self, partition, offset))
    }

    fn seek_with_offset_and_metadata<'a>(
        &'a mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::seek_with_offset_and_metadata(self, partition, offset_and_metadata))
    }

    fn seek_to_beginning<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::seek_to_beginning(self, partitions))
    }

    fn seek_to_end<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::seek_to_end(self, partitions))
    }

    fn position<'a>(&'a mut self, partition: &'a TopicPartition) -> BoxFuture<'a, Result<i64, Error>> {
        Box::pin(Consumer::position(self, partition))
    }

    fn position_with_timeout<'a>(
        &'a mut self,
        partition: &'a TopicPartition,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<i64, Error>> {
        Box::pin(Consumer::position_with_timeout(self, partition, timeout))
    }

    fn committed<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>> {
        Box::pin(Consumer::committed(self, partitions))
    }

    fn committed_with_timeout<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>> {
        Box::pin(Consumer::committed_with_timeout(self, partitions, timeout))
    }

    fn partitions_for<'a>(&'a mut self, topic: &'a str) -> BoxFuture<'a, Result<Vec<PartitionInfo>, Error>> {
        Box::pin(Consumer::partitions_for(self, topic))
    }

    fn partitions_for_with_timeout<'a>(
        &'a mut self,
        topic: &'a str,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Vec<PartitionInfo>, Error>> {
        Box::pin(Consumer::partitions_for_with_timeout(self, topic, timeout))
    }

    fn list_topics<'a>(&'a mut self) -> BoxFuture<'a, Result<HashMap<String, Vec<PartitionInfo>>, Error>> {
        Box::pin(Consumer::list_topics(self))
    }

    fn list_topics_with_timeout<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<String, Vec<PartitionInfo>>, Error>> {
        Box::pin(Consumer::list_topics_with_timeout(self, timeout))
    }

    fn offsets_for_times<'a>(
        &'a mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>> {
        Box::pin(Consumer::offsets_for_times(self, timestamps_to_search))
    }

    fn offsets_for_times_with_timeout<'a>(
        &'a mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>> {
        Box::pin(Consumer::offsets_for_times_with_timeout(self, timestamps_to_search, timeout))
    }

    fn beginning_offsets<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>> {
        Box::pin(Consumer::beginning_offsets(self, partitions))
    }

    fn beginning_offsets_with_timeout<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>> {
        Box::pin(Consumer::beginning_offsets_with_timeout(self, partitions, timeout))
    }

    fn end_offsets<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>> {
        Box::pin(Consumer::end_offsets(self, partitions))
    }

    fn end_offsets_with_timeout<'a>(
        &'a mut self,
        partitions: &'a [TopicPartition],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<HashMap<TopicPartition, i64>, Error>> {
        Box::pin(Consumer::end_offsets_with_timeout(self, partitions, timeout))
    }

    fn pause<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::pause(self, partitions))
    }

    fn resume<'a>(&'a mut self, partitions: &'a [TopicPartition]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::resume(self, partitions))
    }

    fn enforce_rebalance<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::enforce_rebalance(self))
    }

    fn enforce_rebalance_with_reason<'a>(&'a mut self, reason: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::enforce_rebalance_with_reason(self, reason))
    }

    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::close(self))
    }

    fn close_with_options<'a>(&'a mut self, options: CloseOptions) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(Consumer::close_with_options(self, options))
    }

    fn wakeup(&self) {
        Consumer::wakeup(self)
    }

    fn handle(&self) -> ConsumerHandle {
        Consumer::handle(self)
    }
}

// On the trait object rather than on `Box<dyn DynConsumer>`: a boxed consumer
// that implemented `Consumer` would also pick up the blanket `DynConsumer` impl,
// and a method call on the box would then box every future twice.
//
// Methods taking a borrowed argument besides `self` are `async fn` rather than
// returning the boxed future directly: that future lives for the shorter of the
// two input lifetimes, which the trait's opaque return type cannot name.
impl<K, V> Consumer<K, V> for dyn DynConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn assignment(&self) -> HashSet<TopicPartition> {
        DynConsumer::assignment(self)
    }

    fn subscription(&self) -> HashSet<String> {
        DynConsumer::subscription(self)
    }

    fn paused(&self) -> HashSet<TopicPartition> {
        DynConsumer::paused(self)
    }

    fn group_metadata(&self) -> Arc<dyn ConsumerGroupMetadata> {
        DynConsumer::group_metadata(self)
    }

    fn client_id(&self) -> &str {
        DynConsumer::client_id(self)
    }

    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64> {
        DynConsumer::current_lag(self, topic_partition)
    }

    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        DynConsumer::metrics(self)
    }

    fn subscribe_with_topics(&mut self, topics: Vec<String>) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::subscribe_with_topics(self, topics)
    }

    fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::subscribe_with_topics_listener(self, topics, listener)
    }

    fn subscribe_with_pattern(
        &mut self,
        pattern: SubscriptionPattern,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::subscribe_with_pattern(self, pattern)
    }

    fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::subscribe_with_pattern_listener(self, pattern, listener)
    }

    fn assign(&mut self, partitions: Vec<TopicPartition>) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::assign(self, partitions)
    }

    fn unsubscribe(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::unsubscribe(self)
    }

    fn poll(&mut self, timeout: Duration) -> impl Future<Output = Result<ConsumerRecords<K, V>, Error>> + Send {
        DynConsumer::poll(self, timeout)
    }

    fn commit_sync(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_sync(self)
    }

    fn commit_sync_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_sync_with_timeout(self, timeout)
    }

    fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_sync_with_offsets(self, offsets)
    }

    fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_sync_with_offsets_timeout(self, offsets, timeout)
    }

    fn commit_async(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_async(self)
    }

    fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_async_with_callback(self, callback)
    }

    fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::commit_async_with_offsets_callback(self, offsets, callback)
    }

    fn seek_with_offset(
        &mut self,
        partition: TopicPartition,
        offset: i64,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::seek_with_offset(self, partition, offset)
    }

    fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::seek_with_offset_and_metadata(self, partition, offset_and_metadata)
    }

    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        DynConsumer::seek_to_beginning(self, partitions).await
    }

    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        DynConsumer::seek_to_end(self, partitions).await
    }

    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, Error> {
        DynConsumer::position(self, partition).await
    }

    async fn position_with_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, Error> {
        DynConsumer::position_with_timeout(self, partition, timeout).await
    }

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        DynConsumer::committed(self, partitions).await
    }

    async fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        DynConsumer::committed_with_timeout(self, partitions, timeout).await
    }

    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
        DynConsumer::partitions_for(self, topic).await
    }

    async fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, Error> {
        DynConsumer::partitions_for_with_timeout(self, topic, timeout).await
    }

    fn list_topics(&mut self) -> impl Future<Output = Result<HashMap<String, Vec<PartitionInfo>>, Error>> + Send {
        DynConsumer::list_topics(self)
    }

    fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<String, Vec<PartitionInfo>>, Error>> + Send {
        DynConsumer::list_topics_with_timeout(self, timeout)
    }

    fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>> + Send {
        DynConsumer::offsets_for_times(self, timestamps_to_search)
    }

    fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>> + Send {
        DynConsumer::offsets_for_times_with_timeout(self, timestamps_to_search, timeout)
    }

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        DynConsumer::beginning_offsets(self, partitions).await
    }

    async fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        DynConsumer::beginning_offsets_with_timeout(self, partitions, timeout).await
    }

    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, Error> {
        DynConsumer::end_offsets(self, partitions).await
    }

    async fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, Error> {
        DynConsumer::end_offsets_with_timeout(self, partitions, timeout).await
    }

    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        DynConsumer::pause(self, partitions).await
    }

    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        DynConsumer::resume(self, partitions).await
    }

    fn enforce_rebalance(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::enforce_rebalance(self)
    }

    async fn enforce_rebalance_with_reason(&mut self, reason: &str) -> Result<(), Error> {
        DynConsumer::enforce_rebalance_with_reason(self, reason).await
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::close(self)
    }

    fn close_with_options(&mut self, options: CloseOptions) -> impl Future<Output = Result<(), Error>> + Send {
        DynConsumer::close_with_options(self, options)
    }

    fn wakeup(&self) {
        DynConsumer::wakeup(self)
    }

    fn handle(&self) -> ConsumerHandle {
        DynConsumer::handle(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::MockConsumer;

    fn mock() -> MockConsumer<String, String> {
        MockConsumer::new("earliest").expect("earliest is a valid reset strategy")
    }

    fn tp(partition: i32) -> TopicPartition {
        TopicPartition::new("topic", partition)
    }

    async fn seek_and_position<C: Consumer<String, String> + ?Sized>(
        consumer: &mut C,
        partition: &TopicPartition,
    ) -> i64 {
        Consumer::seek_to_beginning(consumer, std::slice::from_ref(partition))
            .await
            .expect("seek_to_beginning");
        Consumer::position(consumer, partition).await.expect("position")
    }

    #[tokio::test]
    async fn test_different_consumers_in_one_vec() {
        let mut first = mock();
        let mut second = mock();
        {
            let mut consumers: Vec<&mut dyn DynConsumer<String, String>> = vec![&mut first, &mut second];
            for (index, consumer) in consumers.iter_mut().enumerate() {
                let partition = i32::try_from(index).expect("two consumers");
                DynConsumer::assign(&mut **consumer, vec![tp(partition)]).await.expect("assign");
            }
        }

        assert_eq!(HashSet::from([tp(0)]), Consumer::assignment(&first));
        assert_eq!(HashSet::from([tp(1)]), Consumer::assignment(&second));
    }

    #[tokio::test]
    async fn test_boxed_consumers_in_one_vec() {
        let mut consumers: Vec<Box<dyn DynConsumer<String, String>>> = vec![Box::new(mock()), Box::new(mock())];

        for consumer in &mut consumers {
            DynConsumer::assign(&mut **consumer, vec![tp(0)]).await.expect("assign");
            assert_eq!(HashSet::from([tp(0)]), DynConsumer::assignment(&**consumer));
            DynConsumer::close(&mut **consumer).await.expect("close");
        }
    }

    #[tokio::test]
    async fn test_dyn_consumer_is_a_consumer() {
        let mut inner = mock();
        Consumer::assign(&mut inner, vec![tp(0)]).await.expect("assign");
        inner.update_beginning_offsets(HashMap::from([(tp(0), 5)]));
        let mut boxed: Box<dyn DynConsumer<String, String>> = Box::new(inner);

        // Through the `Consumer` impl on `dyn DynConsumer`, including the
        // methods whose borrowed argument makes them `async fn`.
        assert_eq!(5, seek_and_position(&mut *boxed, &tp(0)).await);
        assert_eq!(HashSet::from([tp(0)]), Consumer::assignment(&*boxed));
    }

    #[tokio::test]
    async fn test_errors_pass_through_unchanged() {
        let mut direct = mock();
        let mut through_dyn = mock();

        let expected = Consumer::position(&mut direct, &tp(0))
            .await
            .expect_err("position of an unassigned partition");
        let as_dyn: &mut dyn DynConsumer<String, String> = &mut through_dyn;
        let error = DynConsumer::position(as_dyn, &tp(0))
            .await
            .expect_err("same call through DynConsumer");
        assert_eq!(expected.to_string(), error.to_string());
    }

    #[tokio::test]
    async fn test_sync_methods_reach_the_consumer() {
        let mut inner = mock();
        {
            let as_dyn: &mut dyn DynConsumer<String, String> = &mut inner;
            DynConsumer::wakeup(as_dyn);
            let error = DynConsumer::poll(as_dyn, Duration::ZERO)
                .await
                .expect_err("a woken-up mock fails the next poll");
            assert!(matches!(error, Error::Wakeup(_)), "{error:?}");
            DynConsumer::close(as_dyn).await.expect("close");
        }
        assert!(inner.closed(), "close through DynConsumer closes the mock");
    }

    #[tokio::test]
    async fn test_dyn_consumer_futures_can_be_spawned() {
        let mut inner = mock();
        inner.update_beginning_offsets(HashMap::from([(tp(0), 0)]));
        let mut boxed: Box<dyn DynConsumer<String, String>> = Box::new(inner);
        let task = tokio::spawn(async move {
            DynConsumer::assign(&mut *boxed, vec![tp(0)]).await?;
            DynConsumer::poll(&mut *boxed, Duration::ZERO)
                .await
                .map(|records| records.count())
        });
        assert_eq!(0, task.await.expect("join").expect("poll"));
    }
}
