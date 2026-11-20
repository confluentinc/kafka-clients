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

//! In-memory `ShareConsumer<K, V>` implementation for use in tests.
//!
//! Translated from `org.apache.kafka.clients.consumer.MockShareConsumer`,
//! field for field. The mock-specific driver method (`add_record`) is an
//! inherent method on the concrete type, not on the
//! [`ShareConsumer`](crate::consumer::ShareConsumer) trait, as for
//! `MockConsumer` (`consumer-threading.md` §2).

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use indexmap::IndexMap;

use crate::common::metrics::MetricsView;
use crate::common::{Error, TopicIdPartition, TopicPartition};
use crate::consumer::internals::{AutoOffsetResetStrategy, ConsumerUtils, SubscriptionState};
use crate::consumer::{
    AcknowledgeOptions, AcknowledgeType, AcknowledgementCommitCallback, ConsumerRecord, ConsumerRecords, ShareConsumer,
    ShareConsumerHandle,
};

/// Java's `IllegalStateException` message for a call on a closed mock.
const CONSUMER_CLOSED_MSG: &str = "This consumer has already been closed.";

/// A mock of the [`ShareConsumer`] interface you can use for testing code that
/// uses Kafka. This class is *not thread-safe*.
///
/// # Translation notes
///
/// `Send` but not `Sync`: wake it through [`ShareConsumer::handle`].
// Java's `synchronized` methods become `&mut self` and the `!Sync` marker, as
// for `MockConsumer`. Java's `clientInstanceId` / `setClientInstanceId` and the
// two metric-subscription no-ops are KIP-714, left out with the trait
// (Milestone 18 decision D2).
#[doc(alias = "org.apache.kafka.clients.consumer.MockShareConsumer")]
pub struct MockShareConsumer<K, V> {
    subscriptions: SubscriptionState,
    /// Java's `AtomicBoolean wakeup`, set by `wakeup` and never read by the
    /// mock itself. An `Arc` so [`ShareConsumer::handle`] can hand it to
    /// another task.
    wakeup: Arc<AtomicBool>,
    /// Java's `HashMap`: the iteration order of the partitions `poll` returns
    /// is unspecified, as in Java.
    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    closed: bool,
    /// Zero-sized: keeps the mock `Send` and makes it `!Sync`, as
    /// `KafkaShareConsumer`.
    not_sync: PhantomData<Cell<()>>,
}

impl<K, V> std::fmt::Debug for MockShareConsumer<K, V> {
    // Whether the mock is closed; the records it holds are not printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockShareConsumer")
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl<K, V> Default for MockShareConsumer<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MockShareConsumer<K, V> {
    /// Constructs a new MockShareConsumer for testing.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockShareConsumer#MockShareConsumer")]
    pub fn new() -> Self {
        Self {
            subscriptions: SubscriptionState::new(AutoOffsetResetStrategy::NONE),
            wakeup: Arc::new(AtomicBool::new(false)),
            records: HashMap::new(),
            closed: false,
            not_sync: PhantomData,
        }
    }

    /// Adds a record to be returned by the next [`ShareConsumer::poll`].
    ///
    /// # Arguments
    ///
    /// - `record` - the record to add
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_state_error` if the mock is closed, or the
    ///   record's topic is not subscribed
    // Java's `addRecord` throws `IllegalStateException`; a `Result`, as
    // `MockConsumer::add_record`.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockShareConsumer#addRecord")]
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) -> Result<(), Error> {
        self.ensure_not_closed()?;
        let tp = TopicPartition::new(record.topic().to_string(), record.partition());
        if !self.subscriptions.subscription().contains(record.topic()) {
            return Err(Error::local_illegal_state(
                "Cannot add records for a topics that is not subscribed by the consumer",
            ));
        }
        self.records.entry(tp).or_default().push(record);
        Ok(())
    }

    /// Whether `wakeup` was called, through the mock or one of its handles.
    /// Java's mock sets the flag and never reads it; the tests read it here.
    #[cfg(test)]
    pub(crate) fn wakeup_requested(&self) -> bool {
        self.wakeup.load(Ordering::SeqCst)
    }

    /// Java's `ensureNotClosed`.
    #[doc(alias = "org.apache.kafka.clients.consumer.MockShareConsumer#ensureNotClosed")]
    fn ensure_not_closed(&self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::local_illegal_state(CONSUMER_CLOSED_MSG));
        }
        Ok(())
    }

    /// The closed check of the methods with no error channel (CLAUDE.md
    /// §12.5), as `KafkaShareConsumer`'s.
    fn panic_if_closed(&self) {
        if self.closed {
            panic!("{CONSUMER_CLOSED_MSG}");
        }
    }
}

impl<K, V> ShareConsumer<K, V> for MockShareConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Returns the topics of the last [`ShareConsumer::subscribe`].
    ///
    /// # Panics
    ///
    /// Panics with `"This consumer has already been closed."` on a closed
    /// mock, where Java's `ensureNotClosed()` throws `IllegalStateException`:
    /// calling it after `close()` is a programming error.
    fn subscription(&mut self) -> HashSet<String> {
        self.panic_if_closed();
        self.subscriptions.subscription()
    }

    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), Error> {
        self.ensure_not_closed()?;
        self.subscriptions.subscribe_with_topics(topics.into_iter().collect(), None)?;
        Ok(())
    }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        self.ensure_not_closed()?;
        self.subscriptions.unsubscribe();
        Ok(())
    }

    async fn poll(&mut self, _timeout: Duration) -> Result<ConsumerRecords<K, V>, Error> {
        self.ensure_not_closed()?;
        let mut results: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>> = IndexMap::new();
        for (partition, records) in self.records.drain() {
            results.entry(partition).or_default().extend(records);
        }
        Ok(ConsumerRecords::with_next_offsets(results, HashMap::new()))
    }

    fn acknowledge_with_record(&mut self, _record: &ConsumerRecord<K, V>) -> Result<(), Error> {
        Ok(())
    }

    fn acknowledge_with_record_type(
        &mut self,
        _record: &ConsumerRecord<K, V>,
        _type: AcknowledgeType,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn acknowledge_with_options(&mut self, _options: AcknowledgeOptions) -> Result<(), Error> {
        Ok(())
    }

    async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<Error>>, Error> {
        Ok(HashMap::new())
    }

    async fn commit_sync_with_timeout(
        &mut self,
        _timeout: Duration,
    ) -> Result<HashMap<TopicIdPartition, Option<Error>>, Error> {
        Ok(HashMap::new())
    }

    async fn commit_async(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Does nothing, as Java's mock; it does not check whether the mock is
    /// closed.
    ///
    /// # Arguments
    ///
    /// - `_callback` - ignored
    fn set_acknowledgement_commit_callback(&mut self, _callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {}

    /// Returns `None`, as Java's mock; it does not check whether the mock is
    /// closed.
    fn acquisition_lock_timeout_ms(&mut self) -> Option<i32> {
        None
    }

    /// Returns an empty view: the mock keeps no metrics.
    ///
    /// # Panics
    ///
    /// Panics with `"This consumer has already been closed."` on a closed
    /// mock, where Java's `ensureNotClosed()` throws `IllegalStateException`:
    /// calling it after `close()` is a programming error.
    fn metrics(&self) -> MetricsView {
        self.panic_if_closed();
        MetricsView::default()
    }

    async fn close(&mut self) -> Result<(), Error> {
        let timeout = Duration::from_millis(u64::try_from(ConsumerUtils::DEFAULT_CLOSE_TIMEOUT_MS).unwrap_or(0));
        self.close_with_timeout(timeout).await
    }

    async fn close_with_timeout(&mut self, _timeout: Duration) -> Result<(), Error> {
        self.closed = true;
        Ok(())
    }

    fn wakeup(&self) {
        self.wakeup.store(true, Ordering::SeqCst);
    }

    fn handle(&self) -> ShareConsumerHandle {
        ShareConsumerHandle::with_mock_flag(Arc::clone(&self.wakeup))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::RecordHeaders;
    use crate::common::record::TimestampType;
    use crate::consumer::ConsumerRecordOptionsBuilder;

    fn record(offset: i64, key: &str, value: &str) -> ConsumerRecord<String, String> {
        ConsumerRecord::with_options(
            ConsumerRecordOptionsBuilder::new()
                .set_topic("test")
                .set_partition(0)
                .set_offset(offset)
                .set_timestamp(0)
                .set_timestamp_type(TimestampType::CreateTime)
                .set_serialized_key_size(0)
                .set_serialized_value_size(0)
                .set_key(Some(key.to_string()))
                .set_value(Some(value.to_string()))
                .set_headers(RecordHeaders::new())
                .set_leader_epoch(None)
                .build()
                .expect("every mandatory parameter is set"),
        )
    }

    fn same_record(expected: &ConsumerRecord<String, String>, actual: &ConsumerRecord<String, String>) {
        assert_eq!(expected.topic(), actual.topic());
        assert_eq!(expected.partition(), actual.partition());
        assert_eq!(expected.offset(), actual.offset());
        assert_eq!(expected.key(), actual.key());
        assert_eq!(expected.value(), actual.value());
    }

    /// Java `MockShareConsumerTest#testSimpleMock`. Java compares the records
    /// by identity (`ConsumerRecord` has no `equals`); the Rust mock returns
    /// the records it was given by value, so their fields are compared.
    #[tokio::test]
    async fn test_simple_mock() {
        let mut consumer = MockShareConsumer::<String, String>::new();
        consumer.subscribe(vec!["test".to_string()]).await.expect("subscribe");
        assert_eq!(0, consumer.poll(Duration::ZERO).await.expect("poll").count());
        let rec1 = record(0, "key1", "value1");
        let rec2 = record(1, "key2", "value2");
        consumer.add_record(rec1.clone()).expect("subscribed");
        consumer.add_record(rec2.clone()).expect("subscribed");
        let recs = consumer.poll(Duration::from_millis(1)).await.expect("poll");
        let mut iter = (&recs).into_iter();
        same_record(&rec1, iter.next().expect("rec1"));
        same_record(&rec2, iter.next().expect("rec2"));
        assert!(iter.next().is_none());
        assert_eq!(0, recs.next_offsets().len());
    }

    /// Java's `addRecord` rejects a record of a topic not subscribed, and a
    /// closed mock rejects it before that.
    #[tokio::test]
    async fn add_record_requires_the_topic_to_be_subscribed() {
        let mut consumer = MockShareConsumer::<String, String>::new();
        let error = consumer.add_record(record(0, "key", "value")).expect_err("not subscribed");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!(
            "Cannot add records for a topics that is not subscribed by the consumer",
            error.message()
        );
        consumer.close().await.expect("close");
        let error = consumer.add_record(record(0, "key", "value")).expect_err("closed");
        assert_eq!(CONSUMER_CLOSED_MSG, error.message());
    }

    /// Java's `ensureNotClosed` guards `subscribe`, `unsubscribe` and `poll`
    /// (an error) and `subscription` and `metrics` (a panic here, CLAUDE.md
    /// §12.5); the acknowledgements and commits do not check.
    #[tokio::test]
    async fn closed_mock_rejects_the_guarded_methods_only() {
        let mut consumer = MockShareConsumer::<String, String>::new();
        consumer.close_with_timeout(Duration::ZERO).await.expect("close");
        // A second close does nothing.
        consumer.close().await.expect("close");
        for error in [
            consumer.subscribe(vec!["test".to_string()]).await.expect_err("closed"),
            consumer.unsubscribe().await.expect_err("closed"),
            consumer.poll(Duration::ZERO).await.expect_err("closed"),
        ] {
            assert!(error.is_local_illegal_state_error(), "{error:?}");
            assert_eq!(CONSUMER_CLOSED_MSG, error.message());
        }
        consumer.acknowledge_with_record(&record(0, "key", "value")).expect("unchecked");
        assert!(consumer.commit_sync().await.expect("unchecked").is_empty());
        consumer.commit_async().await.expect("unchecked");
        assert_eq!(None, consumer.acquisition_lock_timeout_ms());

        let subscription = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consumer.subscription()));
        let panic = subscription.expect_err("subscription on a closed mock panics");
        assert_eq!(Some(&CONSUMER_CLOSED_MSG.to_string()), panic.downcast_ref::<String>());
        let metrics = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consumer.metrics()));
        let panic = metrics.expect_err("metrics on a closed mock panics");
        assert_eq!(Some(&CONSUMER_CLOSED_MSG.to_string()), panic.downcast_ref::<String>());
    }

    /// `unsubscribe` clears the subscription, and `poll` empties the records.
    #[tokio::test]
    async fn unsubscribe_clears_the_subscription_and_poll_drains_the_records() {
        let mut consumer = MockShareConsumer::<String, String>::new();
        consumer.subscribe(vec!["test".to_string()]).await.expect("subscribe");
        assert_eq!(HashSet::from(["test".to_string()]), consumer.subscription());
        consumer.add_record(record(0, "key", "value")).expect("subscribed");
        assert_eq!(1, consumer.poll(Duration::ZERO).await.expect("poll").count());
        assert_eq!(0, consumer.poll(Duration::ZERO).await.expect("poll").count());
        consumer.unsubscribe().await.expect("unsubscribe");
        assert!(consumer.subscription().is_empty());
    }

    /// R8 H7: the handle's `wakeup` sets the mock's flag, as the mock's own
    /// `wakeup` does, also from another thread.
    #[test]
    fn mock_handle_wakeup_sets_the_mock_flag() {
        let consumer = MockShareConsumer::<String, String>::new();
        assert!(!consumer.wakeup_requested());
        let handle = consumer.handle();
        std::thread::spawn(move || handle.wakeup()).join().expect("join");
        assert!(consumer.wakeup_requested());

        let consumer = MockShareConsumer::<String, String>::new();
        ShareConsumer::wakeup(&consumer);
        assert!(consumer.wakeup_requested());
    }
}
