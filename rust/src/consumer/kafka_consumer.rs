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

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::KafkaMetric;
use crate::common::serialization::DynDeserializer;
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
///
/// # Threads
///
/// The consumer is `Send` but not `Sync`: it can move to another task or
/// thread, but it cannot be shared between them. This is the type-level form
/// of Java's `KafkaConsumer.acquire()` guard, which throws
/// `ConcurrentModificationException` when a second thread calls into a
/// consumer another thread is using: every [`Consumer`] method Java guards
/// takes `&mut self`, and without `Sync` not even the methods taking `&self`
/// can be reached from two threads at once. To reach the consumer from
/// another task or thread — to call `wakeup()` while it waits, or to call back
/// into it from a rebalance listener — take a
/// [`ConsumerHandle`](crate::consumer::ConsumerHandle) with
/// [`Consumer::handle`], which is `Send + Sync`.
///
/// The consumer cannot be shared between threads:
///
/// ```compile_fail,E0277
/// use confluent_kafka::consumer::KafkaConsumer;
///
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<KafkaConsumer<String, String>>();
/// ```
///
/// and its getters cannot be called through a shared reference:
///
/// ```compile_fail,E0596
/// use std::collections::HashSet;
///
/// use confluent_kafka::common::TopicPartition;
/// use confluent_kafka::consumer::{Consumer, KafkaConsumer};
///
/// fn read_assignment(consumer: &KafkaConsumer<String, String>) -> HashSet<TopicPartition> {
///     consumer.assignment()
/// }
/// ```
#[doc(alias = "org.apache.kafka.clients.consumer.KafkaConsumer")]
pub struct KafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    delegate: AsyncKafkaConsumer<K, V>,
    /// Zero-sized: keeps the consumer `Send` and makes it `!Sync`, the
    /// type-level form of Java's `acquire()` guard (see `Threads` above).
    /// Cross-thread access goes through [`ConsumerHandle`].
    not_sync: PhantomData<Cell<()>>,
}

impl<K, V> KafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs a new consumer from a configuration and explicit key/value
    /// deserializers, any [`Deserializer`](crate::common::serialization::Deserializer)
    /// boxed as a [`DynDeserializer`].
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
    /// them as explicit `Box<dyn DynDeserializer>` parameters (Phase 1
    /// decision not to translate reflection machinery). The consumer hands
    /// them to its `FetchCollector`, which calls them for every fetched
    /// record, and closes them when the consumer closes.
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
        key_deserializer: Box<dyn DynDeserializer<K>>,
        value_deserializer: Box<dyn DynDeserializer<V>>,
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
                not_sync: PhantomData,
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
    fn assignment(&mut self) -> HashSet<TopicPartition> {
        Consumer::assignment(&mut self.delegate)
    }

    fn subscription(&mut self) -> HashSet<String> {
        Consumer::subscription(&mut self.delegate)
    }

    fn paused(&mut self) -> HashSet<TopicPartition> {
        Consumer::paused(&mut self.delegate)
    }

    fn group_metadata(&mut self) -> Arc<dyn ConsumerGroupMetadata> {
        Consumer::group_metadata(&mut self.delegate)
    }

    fn current_lag(
        &mut self,
        topic_partition: &TopicPartition,
    ) -> impl Future<Output = Result<Option<i64>, Error>> + Send {
        Consumer::current_lag(&mut self.delegate, topic_partition)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Errors;
    use crate::common::serialization::Deserializer;
    use crate::consumer::CloseOptions;
    use crate::test_fake_broker::{FakeBroker, FetchAnswer, ListOffsetsAnswer};
    use crate::test_thread_safety::{is_send, is_sync};

    const TOPIC: &str = "current-lag-topic";

    /// `ListOffsetsRequest.LATEST_TIMESTAMP`: the request `currentLag` sends
    /// for the log-end offset.
    const LATEST_TIMESTAMP: i64 = -1;

    /// How long a test waits for the background task, as Java's
    /// `TestUtils.waitForCondition` default.
    const WAIT: Duration = Duration::from_secs(15);

    struct StringDeserializer;

    impl Deserializer<String> for StringDeserializer {
        async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {e}")))
        }
    }

    fn tp0() -> TopicPartition {
        TopicPartition::new(TOPIC, 0)
    }

    fn latest_list_offsets(broker: &FakeBroker) -> usize {
        broker
            .list_offsets_timestamps()
            .into_iter()
            .filter(|timestamp| *timestamp == LATEST_TIMESTAMP)
            .count()
    }

    /// A real `KafkaConsumer` against the fake broker. `earliest` makes a
    /// position reset ask for `EARLIEST_TIMESTAMP`, so the `LATEST_TIMESTAMP`
    /// requests counted by the tests are `currentLag`'s only.
    fn new_consumer(broker: &FakeBroker) -> KafkaConsumer<String, String> {
        let props = HashMap::from([
            ("bootstrap.servers".to_string(), broker.bootstrap()),
            ("group.protocol".to_string(), "consumer".to_string()),
            ("auto.offset.reset".to_string(), "earliest".to_string()),
        ]);
        KafkaConsumer::new(
            ConsumerConfig::new(&props).expect("config validates"),
            Box::new(StringDeserializer),
            Box::new(StringDeserializer),
        )
        .expect("constructs against the fake broker")
    }

    /// Java's `KafkaConsumerTest.setUpConsumerForCurrentLag`: the lag of an
    /// unassigned partition is an error, then the partition is assigned and
    /// the consumer learns its leader.
    ///
    /// Java seeds the metadata (`initMetadata`) and polls once. Here the
    /// background task fetches it from the fake broker: until it knows the
    /// leader, the one-shot LIST_OFFSETS of `currentLag` completes without
    /// being sent, so the lag is asked for until the request reaches the
    /// broker, where it is held. Every Java test then makes its first
    /// `currentLag` call against that held request. No poll: one before a
    /// `seek` would reset the position with a LIST_OFFSETS of its own.
    ///
    /// Without the group: Java's helper answers the FIND_COORDINATOR its
    /// `groupId` causes, which plays no part in the lag, and the fake broker
    /// has no coordinator.
    async fn set_up_consumer_for_current_lag(broker: &FakeBroker) -> KafkaConsumer<String, String> {
        let mut consumer = new_consumer(broker);
        // throws for unassigned partition
        let error = consumer.current_lag(&tp0()).await.expect_err("unassigned partition");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!(format!("No current assignment for partition {}", tp0()), error.message());
        consumer.assign(vec![tp0()]).await.expect("assign");

        let deadline = tokio::time::Instant::now() + WAIT;
        while latest_list_offsets(broker) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "No LIST_OFFSETS request sent within allotted timeout"
            );
            // no error for no current position
            assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        consumer
    }

    /// Waits until the fake broker has received `count` `LATEST_TIMESTAMP`
    /// requests, polling the consumer in between when `poll` is set.
    async fn wait_for_latest_list_offsets(
        consumer: &mut KafkaConsumer<String, String>,
        broker: &FakeBroker,
        count: usize,
        poll: bool,
    ) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while latest_list_offsets(broker) < count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "No LIST_OFFSETS request sent within allotted timeout"
            );
            if poll {
                consumer.poll(Duration::ZERO).await.expect("poll");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Closes the consumer, then checks `currentLag` fails as Java's
    /// `acquireAndEnsureOpen()` does.
    async fn close_and_assert_current_lag_fails(mut consumer: KafkaConsumer<String, String>) {
        consumer
            .close_with_options(CloseOptions::new_with_timeout(Duration::ZERO))
            .await
            .expect("close");
        let error = consumer.current_lag(&tp0()).await.expect_err("closed consumer");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!("This consumer has already been closed.", error.message());
    }

    /// Translated from `KafkaConsumerTest.testCurrentLag` (CONSUMER arm),
    /// through the public API and the background task, against a fake broker
    /// in place of Java's `MockClient`. The fake answers the FETCHes sent
    /// before the test's own one empty, with unknown offsets the consumer does
    /// not apply, so they leave the lag as Java's unanswered FETCH does.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_current_lag() {
        let broker = FakeBroker::start("fake-cluster", TOPIC).await;
        let mut consumer = set_up_consumer_for_current_lag(&broker).await;

        // no error for no current position
        assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
        // poll once again, which should send the list-offset request
        consumer.seek_with_offset(tp0(), 50).await.expect("seek");
        // requests: list-offset, fetch
        let deadline = tokio::time::Instant::now() + WAIT;
        while latest_list_offsets(&broker) < 1 || broker.fetch_requests() < 1 {
            assert!(tokio::time::Instant::now() < deadline, "No list-offset & fetch request sent");
            consumer.poll(Duration::ZERO).await.expect("poll");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // no error for no end offset (so unknown lag)
        assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));

        // answer the list-offset request, so a next call returns the lag
        broker.answer_list_offsets(Some(ListOffsetsAnswer { error: Errors::None, offset: 90 }));
        consumer.poll(Duration::ZERO).await.expect("poll");

        // The subscription state is updated in the background, so the result
        // will eventually be updated.
        let deadline = tokio::time::Instant::now() + WAIT;
        while consumer.current_lag(&tp0()).await.expect("lag") != Some(40) {
            assert!(tokio::time::Instant::now() < deadline, "Subscription state is not updated");
            consumer.poll(Duration::ZERO).await.expect("poll");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(1, latest_list_offsets(&broker), "one LIST_OFFSETS for the end offset");

        // one successful fetch should update the log end offset and the position
        broker.answer_fetch(FetchAnswer { log_first_offset: 1, log_last_offset: 99, offset: 50, count: 5 });
        // requests: fetch
        let deadline = tokio::time::Instant::now() + WAIT;
        let records = loop {
            assert!(tokio::time::Instant::now() < deadline, "No fetch request sent");
            let records = consumer.poll(Duration::from_millis(1)).await.expect("poll");
            if !records.is_empty() {
                break records;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(5, records.count());
        assert_eq!(55, consumer.position(&tp0()).await.expect("position"));
        assert_eq!(1, records.next_offsets().len());
        assert_eq!(
            Some(&OffsetAndMetadata::new(55).expect("valid offset")),
            records.next_offsets().get(&tp0())
        );

        // correct lag result
        assert_eq!(Some(45), consumer.current_lag(&tp0()).await.expect("lag"));

        close_and_assert_current_lag_fails(consumer).await;
    }

    /// Translated from `KafkaConsumerTest.testCurrentLagPreventsMultipleInFlightRequests`
    /// (CONSUMER arm, KAFKA-20187).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_current_lag_prevents_multiple_in_flight_requests() {
        let broker = FakeBroker::start("fake-cluster", TOPIC).await;
        let mut consumer = set_up_consumer_for_current_lag(&broker).await;

        assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
        assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
        wait_for_latest_list_offsets(&mut consumer, &broker, 1, false).await;
        // Give the background task time to send a second request, if it did.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let count = latest_list_offsets(&broker);
        assert_eq!(
            1, count,
            "Expected only one in-flight LIST_OFFSETS request for consumerLag(), but consumer submitted {count} requests"
        );
        close_and_assert_current_lag_fails(consumer).await;
    }

    /// The two `testCurrentLagClearsFlagOn*PartitionError` tests. Java reads
    /// the 'end offset requested' flag on its spied `SubscriptionState`; here
    /// it shows through the public API: while it is set, `currentLag` sends no
    /// new LIST_OFFSETS, and once the error response clears it, the next call
    /// sends one.
    async fn assert_current_lag_clears_flag_on_partition_error(error: Errors) {
        let broker = FakeBroker::start("fake-cluster", TOPIC).await;
        let mut consumer = set_up_consumer_for_current_lag(&broker).await;

        assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
        wait_for_latest_list_offsets(&mut consumer, &broker, 1, false).await;

        // Still set, because the previous LIST_OFFSETS call has not received a
        // response: no second request.
        assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(1, latest_list_offsets(&broker));

        // Now respond to the LIST_OFFSETS request with an error in the partition.
        broker.answer_list_offsets(Some(ListOffsetsAnswer { error, offset: -1 }));

        // The flag is cleared, so a later call sends a new request.
        let deadline = tokio::time::Instant::now() + WAIT;
        while latest_list_offsets(&broker) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "endOffsetRequested flag was not cleared within allotted timeout"
            );
            assert_eq!(None, consumer.current_lag(&tp0()).await.expect("lag"));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        close_and_assert_current_lag_fails(consumer).await;
    }

    /// Translated from `KafkaConsumerTest.testCurrentLagClearsFlagOnFatalPartitionError`
    /// (CONSUMER arm, KAFKA-20187).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_current_lag_clears_flag_on_fatal_partition_error() {
        assert_current_lag_clears_flag_on_partition_error(Errors::TopicAuthorizationFailed).await;
    }

    /// Translated from `KafkaConsumerTest.testCurrentLagClearsFlagOnRetriablePartitionError`
    /// (CONSUMER arm, KAFKA-20187).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_current_lag_clears_flag_on_retriable_partition_error() {
        assert_current_lag_clears_flag_on_partition_error(Errors::OffsetNotAvailable).await;
    }

    /// Translated from `KafkaConsumerTest.testListOffsetShouldUpdateSubscriptions`
    /// (CONSUMER arm): `endOffsets` updates the subscription state, so
    /// `currentLag` returns the lag at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_offset_should_update_subscriptions() {
        let broker = FakeBroker::start("fake-cluster", TOPIC).await;
        let mut consumer = new_consumer(&broker);
        consumer.assign(vec![tp0()]).await.expect("assign");
        consumer.seek_with_offset(tp0(), 50).await.expect("seek");

        broker.answer_list_offsets(Some(ListOffsetsAnswer { error: Errors::None, offset: 90 }));
        assert_eq!(
            HashMap::from([(tp0(), 90)]),
            consumer.end_offsets(&[tp0()]).await.expect("end offsets")
        );
        // correct lag result should be returned as well
        assert_eq!(Some(40), consumer.current_lag(&tp0()).await.expect("lag"));
        close_and_assert_current_lag_fails(consumer).await;
    }

    /// An offsets query completes as soon as its response is read, as Java's
    /// does inside `client.poll` (`NetworkClientDelegate.java:454-464` →
    /// `OffsetsRequestManager.java:686`, `:208`). The LIST_OFFSETS forwarder
    /// must wake the background task; without that wake the completion waits
    /// for the network poll's own timeout (`MAX_POLL_TIMEOUT_MS`, 5 s) and a
    /// shorter timeout fails every time.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_offsets_query_completes_without_waiting_for_poll_timeout() {
        let broker = FakeBroker::start("fake-cluster", TOPIC).await;
        let mut consumer = new_consumer(&broker);
        consumer.assign(vec![tp0()]).await.expect("assign");

        broker.answer_list_offsets(Some(ListOffsetsAnswer { error: Errors::None, offset: 90 }));
        assert_eq!(
            HashMap::from([(tp0(), 90)]),
            consumer
                .end_offsets_with_timeout(&[tp0()], Duration::from_secs(1))
                .await
                .expect("end offsets")
        );
        assert_eq!(
            HashMap::from([(tp0(), 90)]),
            consumer
                .beginning_offsets_with_timeout(&[tp0()], Duration::from_secs(1))
                .await
                .expect("beginning offsets")
        );
        consumer.close().await.expect("close");
    }

    /// A closed and dropped consumer frees its state. The offsets manager's
    /// cluster listener, boxed into the consumer's `Metadata`, once held the
    /// manager's shared state, which owns that `Metadata`: an `Arc` cycle that
    /// kept the metadata, the subscription state and the manager allocated for
    /// the life of the process. Java builds the same cycle
    /// (`OffsetsRequestManager.java:148`) and its garbage collector frees it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_closed_consumer_is_freed() {
        for query_offsets in [false, true] {
            let broker = FakeBroker::start("fake-cluster", TOPIC).await;
            let mut consumer = new_consumer(&broker);
            let (metadata, subscriptions) = consumer.delegate.metadata_and_subscriptions_for_test();
            if query_offsets {
                consumer.assign(vec![tp0()]).await.expect("assign");
                broker.answer_list_offsets(Some(ListOffsetsAnswer { error: Errors::None, offset: 90 }));
                consumer.end_offsets(&[tp0()]).await.expect("end offsets");
            }
            consumer.close().await.expect("close");
            drop(consumer);

            // Spawned tasks holding a clone end shortly after the close.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while (metadata.strong_count() > 0 || subscriptions.strong_count() > 0)
                && std::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                (0, 0),
                (metadata.strong_count(), subscriptions.strong_count()),
                "ConsumerMetadata / SubscriptionState still referenced (query_offsets = {query_offsets})"
            );
        }
    }

    /// `KafkaConsumer` can move to another task but not be shared between
    /// tasks, as Java's `KafkaConsumer` is single-threaded and enforces it at
    /// run time with `acquire()` (`consumer-threading.md` §2). The compiler
    /// enforces it here, through the `!Sync` marker.
    #[test]
    fn kafka_consumer_is_send_and_not_sync() {
        assert!(is_send!(KafkaConsumer<String, String>));
        assert!(!is_sync!(KafkaConsumer<String, String>));
    }

    /// The handle is the one consumer value that may be shared between tasks:
    /// it is what a listener or another task uses to call `wakeup()`.
    #[test]
    fn consumer_handle_is_send_and_sync() {
        assert!(is_send!(ConsumerHandle));
        assert!(is_sync!(ConsumerHandle));
    }
}
