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

//! The dyn-compatible view of [`Producer`].
//!
//! Has no Java counterpart (`definition-of-done.md` §7). A Java interface is
//! always usable as a runtime-polymorphic type, so a `List<Producer<K, V>>`
//! can hold a `KafkaProducer` next to a `MockProducer`. The Rust
//! [`Producer`] trait returns `impl Future` from its asynchronous methods,
//! which keeps statically dispatched calls (one per record on the send path)
//! free of a boxed future, but makes the trait itself not dyn-compatible.
//! `DynProducer` restores the Java capability (CLAUDE.md §3, "traits must be
//! dyn-compatible") by boxing each future, and only callers that go through
//! `dyn` pay for it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::MetricName;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::metrics::KafkaMetric;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;
use crate::producer::Callback;
use crate::producer::Producer;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

mod private {
    // Deliberately unnameable: it seals `DynProducer` to `Producer` implementors.
    #[allow(unnameable_types)]
    pub trait Sealed<K, V> {}
}

impl<K, V, P: super::Producer<K, V>> private::Sealed<K, V> for P {}

/// A dyn-compatible [`Producer`], for holding different producer
/// implementations behind one type, e.g. `Vec<Box<dyn DynProducer<K, V>>>`.
///
/// Every [`Producer`] implements `DynProducer` automatically, and the trait is
/// sealed, so it cannot be implemented directly: implement [`Producer`]
/// instead. Each method forwards to the [`Producer`] method of the same name;
/// asynchronous methods return the future boxed, which costs one allocation per
/// call. `dyn DynProducer<K, V>` implements [`Producer`] in turn, so a boxed
/// producer can be passed as `&*boxed` to generic code taking
/// `P: Producer<K, V> + ?Sized`.
///
/// With both traits in scope a method call on a `dyn DynProducer` is ambiguous,
/// since both define it; call it as `DynProducer::send(&*producer, record)`, or
/// import only one of the two traits.
///
/// ```
/// use confluent_kafka::producer::{DynProducer, MockProducer};
///
/// let producers: Vec<Box<dyn DynProducer<String, String>>> = vec![
///     Box::new(MockProducer::<String, String>::with_auto_complete(true)),
///     Box::new(MockProducer::<String, String>::with_auto_complete(false)),
/// ];
/// assert_eq!(2, producers.len());
/// ```
pub trait DynProducer<K, V>: private::Sealed<K, V> + Send + Sync {
    /// See [`Producer::init_transactions`].
    ///
    /// # Errors
    ///
    /// As [`Producer::init_transactions`].
    fn init_transactions<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::begin_transaction`].
    ///
    /// # Errors
    ///
    /// As [`Producer::begin_transaction`].
    fn begin_transaction(&self) -> Result<(), Error>;

    /// See [`Producer::send_offsets_to_transaction`].
    ///
    /// # Errors
    ///
    /// As [`Producer::send_offsets_to_transaction`].
    fn send_offsets_to_transaction<'a>(
        &'a self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &'a dyn ConsumerGroupMetadata,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::commit_transaction`].
    ///
    /// # Errors
    ///
    /// As [`Producer::commit_transaction`].
    fn commit_transaction<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::abort_transaction`].
    ///
    /// # Errors
    ///
    /// As [`Producer::abort_transaction`].
    fn abort_transaction<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::send`].
    ///
    /// # Errors
    ///
    /// As [`Producer::send`].
    fn send<'a>(&'a self, record: ProducerRecord<K, V>) -> BoxFuture<'a, Result<KafkaFuture<RecordMetadata>, Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::send_with_callback`].
    ///
    /// # Errors
    ///
    /// As [`Producer::send_with_callback`].
    fn send_with_callback<'a>(
        &'a self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> BoxFuture<'a, Result<KafkaFuture<RecordMetadata>, Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::flush`].
    ///
    /// # Errors
    ///
    /// As [`Producer::flush`].
    fn flush<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::partitions_for`].
    ///
    /// # Errors
    ///
    /// As [`Producer::partitions_for`].
    fn partitions_for<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Result<Vec<PartitionInfo>, Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::metrics`].
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;

    /// See [`Producer::close`].
    ///
    /// # Errors
    ///
    /// As [`Producer::close`].
    fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;

    /// See [`Producer::close_with_timeout`].
    ///
    /// # Errors
    ///
    /// As [`Producer::close_with_timeout`].
    fn close_with_timeout<'a>(&'a self, timeout: Duration) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a;
}

impl<K, V, P: Producer<K, V>> DynProducer<K, V> for P {
    fn init_transactions<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::init_transactions(self))
    }

    fn begin_transaction(&self) -> Result<(), Error> {
        Producer::begin_transaction(self)
    }

    fn send_offsets_to_transaction<'a>(
        &'a self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &'a dyn ConsumerGroupMetadata,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::send_offsets_to_transaction(self, offsets, group_metadata))
    }

    fn commit_transaction<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::commit_transaction(self))
    }

    fn abort_transaction<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::abort_transaction(self))
    }

    fn send<'a>(&'a self, record: ProducerRecord<K, V>) -> BoxFuture<'a, Result<KafkaFuture<RecordMetadata>, Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::send(self, record))
    }

    fn send_with_callback<'a>(
        &'a self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> BoxFuture<'a, Result<KafkaFuture<RecordMetadata>, Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::send_with_callback(self, record, callback))
    }

    fn flush<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::flush(self))
    }

    fn partitions_for<'a>(&'a self, topic: &'a str) -> BoxFuture<'a, Result<Vec<PartitionInfo>, Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::partitions_for(self, topic))
    }

    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        Producer::metrics(self)
    }

    fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::close(self))
    }

    fn close_with_timeout<'a>(&'a self, timeout: Duration) -> BoxFuture<'a, Result<(), Error>>
    where
        K: 'a,
        V: 'a,
    {
        Box::pin(Producer::close_with_timeout(self, timeout))
    }
}

// On the trait object rather than on `Box<dyn DynProducer>`: a boxed producer
// that implemented `Producer` would also pick up the blanket `DynProducer` impl,
// and a method call on the box would then box every future twice.
impl<K, V> Producer<K, V> for dyn DynProducer<K, V> + '_ {
    fn init_transactions(&self) -> impl Future<Output = Result<(), Error>> + Send {
        DynProducer::init_transactions(self)
    }

    fn begin_transaction(&self) -> Result<(), Error> {
        DynProducer::begin_transaction(self)
    }

    // An `async fn` rather than returning the boxed future: the box is bounded by a
    // single lifetime, the intersection of `self`'s and `group_metadata`'s, which
    // the trait's opaque return type cannot name.
    async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &dyn ConsumerGroupMetadata,
    ) -> Result<(), Error> {
        DynProducer::send_offsets_to_transaction(self, offsets, group_metadata).await
    }

    fn commit_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send {
        DynProducer::commit_transaction(self)
    }

    fn abort_transaction(&self) -> impl Future<Output = Result<(), Error>> + Send {
        DynProducer::abort_transaction(self)
    }

    fn send(
        &self,
        record: ProducerRecord<K, V>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send {
        DynProducer::send(self, record)
    }

    fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> impl Future<Output = Result<KafkaFuture<RecordMetadata>, Error>> + Send {
        DynProducer::send_with_callback(self, record, callback)
    }

    fn flush(&self) -> impl Future<Output = Result<(), Error>> + Send {
        DynProducer::flush(self)
    }

    // `async fn` rather than returning the boxed future directly: that future
    // lives for the shorter of the two input lifetimes, which an opaque return
    // type cannot name.
    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
        DynProducer::partitions_for(self, topic).await
    }

    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        DynProducer::metrics(self)
    }

    fn close(&self) -> impl Future<Output = Result<(), Error>> + Send {
        DynProducer::close(self)
    }

    fn close_with_timeout(&self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send {
        DynProducer::close_with_timeout(self, timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::MockProducer;

    fn record(value: &str) -> ProducerRecord<String, String> {
        ProducerRecord::with_key("topic".to_string(), Some("key".to_string()), Some(value.to_string()))
    }

    async fn send_through_generic<P: Producer<String, String> + ?Sized>(producer: &P, value: &str) -> i64 {
        let future = Producer::send(producer, record(value)).await.expect("send");
        future.get().await.expect("metadata").offset()
    }

    #[tokio::test]
    async fn test_different_producers_in_one_vec() {
        let auto = MockProducer::<String, String>::with_auto_complete(true);
        let manual = MockProducer::<String, String>::with_auto_complete(false);
        let producers: Vec<&dyn DynProducer<String, String>> = vec![&auto, &manual];

        for (index, producer) in producers.iter().enumerate() {
            DynProducer::send(*producer, record(&format!("value{index}")))
                .await
                .expect("send");
        }

        assert_eq!(vec![record("value0")], auto.history());
        assert_eq!(vec![record("value1")], manual.history());
        assert!(manual.complete_next(), "the manual mock still holds the uncompleted send");
    }

    #[tokio::test]
    async fn test_boxed_producers_in_one_vec() {
        let producers: Vec<Box<dyn DynProducer<String, String>>> = vec![
            Box::new(MockProducer::<String, String>::with_auto_complete(true)),
            Box::new(MockProducer::<String, String>::with_auto_complete(true)),
        ];

        for producer in &producers {
            let future = DynProducer::send(&**producer, record("value0")).await.expect("send");
            assert!(future.is_done(), "an auto-complete mock completes the send immediately");
            DynProducer::flush(&**producer).await.expect("flush");
        }
    }

    #[tokio::test]
    async fn test_dyn_producer_is_a_producer() {
        let boxed: Box<dyn DynProducer<String, String>> =
            Box::new(MockProducer::<String, String>::with_auto_complete(true));

        assert_eq!(0, send_through_generic(&*boxed, "value0").await);
        assert_eq!(1, send_through_generic(&*boxed, "value1").await);
    }

    #[test]
    fn test_errors_pass_through_unchanged() {
        let mock = MockProducer::<String, String>::with_auto_complete(true);
        let as_dyn: &dyn DynProducer<String, String> = &mock;

        let error = DynProducer::begin_transaction(as_dyn).expect_err("begin_transaction before init_transactions");
        let expected = Producer::begin_transaction(&mock).expect_err("same call on the mock itself");
        assert_eq!(expected.to_string(), error.to_string());
    }

    #[tokio::test]
    async fn test_dyn_producer_futures_can_be_spawned() {
        let shared: Arc<dyn DynProducer<String, String>> =
            Arc::new(MockProducer::<String, String>::with_auto_complete(true));
        let task = tokio::spawn({
            let shared = shared.clone();
            async move { DynProducer::send(&*shared, record("value0")).await.map(|_| ()) }
        });
        task.await.expect("join").expect("send");
    }
}
