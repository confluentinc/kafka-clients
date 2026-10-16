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

//! The public consumer type.
//!
//! Translated from `org.apache.kafka.clients.consumer.KafkaConsumer`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::KafkaMetric;
use crate::common::serialization::Deserializer;
use crate::common::{Error, MetricName, PartitionInfo, TopicPartition};
use crate::consumer::internals::AsyncKafkaConsumer;
use crate::consumer::{
    CloseOptions, Consumer, ConsumerConfig, ConsumerGroupMetadata, ConsumerHandle, ConsumerRebalanceListener,
    ConsumerRecords, GroupProtocol, OffsetAndMetadata, OffsetAndTimestamp, OffsetCommitCallback, SubscriptionPattern,
};

/// A client that consumes records from a Kafka cluster.
///
/// Translates `org.apache.kafka.clients.consumer.KafkaConsumer`. Java's class
/// is a thin delegating wrapper: its constructor calls
/// `ConsumerDelegateCreator.create(config, keyDeserializer, valueDeserializer)`
/// (`KafkaConsumer.java:615`), which switches on `group.protocol` and returns
/// either an `AsyncKafkaConsumer` or a `ClassicKafkaConsumer`
/// (`ConsumerDelegateCreator.java:57-66`); its other methods forward verbatim
/// to that delegate. The Rust struct has the same shape: it holds the
/// delegate and every [`Consumer`] method forwards to it unchanged.
///
/// `ConsumerDelegate` and `ConsumerDelegateCreator` are out of scope per
/// `consumer-threading.md` §20: only the KIP-848 delegate exists, so the
/// delegate is held as `AsyncKafkaConsumer` directly, and [`Self::new`] is
/// where the collapsed creator lives. When the classic protocol is added, the
/// field becomes a private enum over the two delegates, with no change to
/// this type's public surface.
///
/// The concrete type lets callers dispatch statically: calling a [`Consumer`]
/// method on a `KafkaConsumer`, or through generic code taking
/// `C: Consumer<K, V>`, involves no boxed future. To hold it next to other
/// consumer implementations behind one type, box it as
/// [`DynConsumer`](crate::consumer::DynConsumer).
#[doc(alias = "org.apache.kafka.clients.consumer.KafkaConsumer")]
pub struct KafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    delegate: AsyncKafkaConsumer<K, V>,
}

impl<K, V> KafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs a new consumer from a configuration and explicit key/value
    /// [`Deserializer`]s.
    ///
    /// For `group.protocol=consumer` (KIP-848), the delegate is
    /// `AsyncKafkaConsumer::new(...)` — the production consumer built
    /// end-to-end with `SubscriptionState`, `ConsumerMetadata`,
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
    /// `MockConsumer` does NOT come through this constructor — it has its own.
    ///
    /// # Errors
    ///
    /// Returns an error if `group.protocol` is invalid or `classic`, or if the
    /// delegate cannot be constructed from `config`.
    #[doc(alias = "org.apache.kafka.clients.consumer.KafkaConsumer#KafkaConsumer(Map,Deserializer,Deserializer)")]
    pub fn new(
        config: ConsumerConfig,
        key_deserializer: Box<dyn Deserializer<K>>,
        value_deserializer: Box<dyn Deserializer<V>>,
    ) -> Result<Self, Error> {
        // The `GroupProtocol::Consumer` arm builds the production delegate with
        // [`AsyncKafkaConsumer::new`], which translates the Java primary
        // constructor at `AsyncKafkaConsumer.java:285-518` end-to-end. The ctor
        // builds the full dependency closure (`SubscriptionState`,
        // `ConsumerMetadata`, `NetworkClient` + PLAINTEXT `ChannelBuilder`,
        // every `RequestManager`, `ApplicationEventHandler`,
        // `ConsumerNetworkThread` bg task) and hands off to
        // `AsyncKafkaConsumer::with_components` so the Phase-11 test seam is
        // preserved.
        //
        // `GroupProtocol::Classic` remains an `unsupported_version` error
        // per `consumer-threading.md` §20 (classic protocol deferred to a
        // later milestone).
        let protocol = GroupProtocol::of(config.group_protocol())?;
        match protocol {
            GroupProtocol::Consumer => Ok(Self {
                delegate: AsyncKafkaConsumer::<K, V>::new(config, key_deserializer, value_deserializer)?,
            }),
            GroupProtocol::Classic => Err(Error::unsupported_version(
                "Classic group protocol is not yet supported in this client; \
                 set group.protocol=consumer (KIP-848).",
            )),
        }
    }
}

// Every method forwards to the delegate's own `Consumer` impl, as Java's
// `KafkaConsumer` forwards to `delegate`. The delegate's futures are returned
// as they are, so no call is boxed.
impl<K, V> Consumer<K, V> for KafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn assignment(&self) -> HashSet<TopicPartition> {
        Consumer::assignment(&self.delegate)
    }

    fn subscription(&self) -> HashSet<String> {
        Consumer::subscription(&self.delegate)
    }

    fn paused(&self) -> HashSet<TopicPartition> {
        Consumer::paused(&self.delegate)
    }

    fn group_metadata(&self) -> Arc<dyn ConsumerGroupMetadata> {
        Consumer::group_metadata(&self.delegate)
    }

    fn client_id(&self) -> &str {
        Consumer::client_id(&self.delegate)
    }

    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64> {
        Consumer::current_lag(&self.delegate, topic_partition)
    }

    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        Consumer::metrics(&self.delegate)
    }

    fn subscribe_with_topics(&mut self, topics: Vec<String>) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::subscribe_with_topics(&mut self.delegate, topics)
    }

    fn subscribe_with_topics_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::subscribe_with_topics_listener(&mut self.delegate, topics, listener)
    }

    fn subscribe_with_pattern(
        &mut self,
        pattern: SubscriptionPattern,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::subscribe_with_pattern(&mut self.delegate, pattern)
    }

    fn subscribe_with_pattern_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::subscribe_with_pattern_listener(&mut self.delegate, pattern, listener)
    }

    fn assign(&mut self, partitions: Vec<TopicPartition>) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::assign(&mut self.delegate, partitions)
    }

    fn unsubscribe(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::unsubscribe(&mut self.delegate)
    }

    fn poll(&mut self, timeout: Duration) -> impl Future<Output = Result<ConsumerRecords<K, V>, Error>> + Send {
        Consumer::poll(&mut self.delegate, timeout)
    }

    fn commit_sync(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_sync(&mut self.delegate)
    }

    fn commit_sync_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_sync_with_timeout(&mut self.delegate, timeout)
    }

    fn commit_sync_with_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_sync_with_offsets(&mut self.delegate, offsets)
    }

    fn commit_sync_with_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_sync_with_offsets_timeout(&mut self.delegate, offsets, timeout)
    }

    fn commit_async(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_async(&mut self.delegate)
    }

    fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_async_with_callback(&mut self.delegate, callback)
    }

    fn commit_async_with_offsets_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::commit_async_with_offsets_callback(&mut self.delegate, offsets, callback)
    }

    fn seek_with_offset(
        &mut self,
        partition: TopicPartition,
        offset: i64,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::seek_with_offset(&mut self.delegate, partition, offset)
    }

    fn seek_with_offset_and_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::seek_with_offset_and_metadata(&mut self.delegate, partition, offset_and_metadata)
    }

    fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::seek_to_beginning(&mut self.delegate, partitions)
    }

    fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::seek_to_end(&mut self.delegate, partitions)
    }

    fn position(&mut self, partition: &TopicPartition) -> impl Future<Output = Result<i64, Error>> + Send {
        Consumer::position(&mut self.delegate, partition)
    }

    fn position_with_timeout(
        &mut self,
        partition: &TopicPartition,
        timeout: Duration,
    ) -> impl Future<Output = Result<i64, Error>> + Send {
        Consumer::position_with_timeout(&mut self.delegate, partition, timeout)
    }

    fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> impl Future<Output = Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>> + Send {
        Consumer::committed(&mut self.delegate, partitions)
    }

    fn committed_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, OffsetAndMetadata>, Error>> + Send {
        Consumer::committed_with_timeout(&mut self.delegate, partitions, timeout)
    }

    fn partitions_for(&mut self, topic: &str) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send {
        Consumer::partitions_for(&mut self.delegate, topic)
    }

    fn partitions_for_with_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<PartitionInfo>, Error>> + Send {
        Consumer::partitions_for_with_timeout(&mut self.delegate, topic, timeout)
    }

    fn list_topics(&mut self) -> impl Future<Output = Result<HashMap<String, Vec<PartitionInfo>>, Error>> + Send {
        Consumer::list_topics(&mut self.delegate)
    }

    fn list_topics_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<String, Vec<PartitionInfo>>, Error>> + Send {
        Consumer::list_topics_with_timeout(&mut self.delegate, timeout)
    }

    fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>> + Send {
        Consumer::offsets_for_times(&mut self.delegate, timestamps_to_search)
    }

    fn offsets_for_times_with_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, OffsetAndTimestamp>, Error>> + Send {
        Consumer::offsets_for_times_with_timeout(&mut self.delegate, timestamps_to_search, timeout)
    }

    fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send {
        Consumer::beginning_offsets(&mut self.delegate, partitions)
    }

    fn beginning_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send {
        Consumer::beginning_offsets_with_timeout(&mut self.delegate, partitions, timeout)
    }

    fn end_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send {
        Consumer::end_offsets(&mut self.delegate, partitions)
    }

    fn end_offsets_with_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> impl Future<Output = Result<HashMap<TopicPartition, i64>, Error>> + Send {
        Consumer::end_offsets_with_timeout(&mut self.delegate, partitions, timeout)
    }

    fn pause(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::pause(&mut self.delegate, partitions)
    }

    fn resume(&mut self, partitions: &[TopicPartition]) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::resume(&mut self.delegate, partitions)
    }

    fn enforce_rebalance(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::enforce_rebalance(&mut self.delegate)
    }

    fn enforce_rebalance_with_reason(&mut self, reason: &str) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::enforce_rebalance_with_reason(&mut self.delegate, reason)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::close(&mut self.delegate)
    }

    fn close_with_options(&mut self, options: CloseOptions) -> impl Future<Output = Result<(), Error>> + Send {
        Consumer::close_with_options(&mut self.delegate, options)
    }

    fn wakeup(&self) {
        Consumer::wakeup(&self.delegate)
    }

    fn handle(&self) -> ConsumerHandle {
        Consumer::handle(&self.delegate)
    }
}
