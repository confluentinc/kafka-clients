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

//! The dyn-compatible view of [`ShareConsumer`].
//!
//! Has no Java counterpart (`definition-of-done.md` §7). A Java interface is
//! always usable as a runtime-polymorphic type, so a
//! `List<ShareConsumer<K, V>>` can hold a `KafkaShareConsumer` next to a
//! `MockShareConsumer`. The Rust [`ShareConsumer`] trait returns
//! `impl Future` from its asynchronous methods, which keeps statically
//! dispatched calls free of a boxed future, but makes the trait itself not
//! dyn-compatible. `DynShareConsumer` restores the Java capability (CLAUDE.md
//! §3, "traits must be dyn-compatible") by boxing each future, and only
//! callers that go through `dyn` pay for it. It is the share counterpart of
//! [`DynConsumer`](crate::consumer::DynConsumer); its test plan is PLAN §4.1
//! (translation-checklist R8).

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::common::metrics::MetricsView;
use crate::common::{Error, TopicIdPartition};
use crate::consumer::{
    AcknowledgeOptions, AcknowledgeType, AcknowledgementCommitCallback, ConsumerRecord, ConsumerRecords, ShareConsumer,
    ShareConsumerHandle,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What [`ShareConsumer::commit_sync`] resolves to.
type CommitSyncResult = Result<HashMap<TopicIdPartition, Option<Error>>, Error>;

mod private {
    // Deliberately unnameable: it seals `DynShareConsumer` to `ShareConsumer`
    // implementors.
    #[expect(unnameable_types)]
    pub trait Sealed<K, V> {}
}

impl<K, V, C> private::Sealed<K, V> for C
where
    K: Send + 'static,
    V: Send + 'static,
    C: ShareConsumer<K, V>,
{
}

/// A dyn-compatible [`ShareConsumer`], for holding different share consumer
/// implementations behind one type, e.g. `Box<dyn DynShareConsumer<K, V>>`.
///
/// Every [`ShareConsumer`] implements `DynShareConsumer` automatically, and the
/// trait is sealed, so it cannot be implemented directly: implement
/// [`ShareConsumer`] instead. Each method forwards to the [`ShareConsumer`]
/// method of the same name; asynchronous methods return the future boxed, which
/// costs one allocation per call. `dyn DynShareConsumer<K, V>` implements
/// [`ShareConsumer`] in turn, so a boxed consumer can be passed as `&mut *boxed`
/// to generic code taking `C: ShareConsumer<K, V> + ?Sized`, and its methods can
/// be called with only [`ShareConsumer`] in scope.
///
/// With both traits in scope a method call on a concrete consumer (a
/// `KafkaShareConsumer` or `MockShareConsumer`) is ambiguous, since both apply
/// to it; call it as `ShareConsumer::poll(&mut consumer, timeout)`, or import
/// only one of the two traits. A call on a `dyn DynShareConsumer` resolves to
/// `DynShareConsumer`'s own method either way.
///
/// ```
/// use confluent_kafka::consumer::{DynShareConsumer, MockShareConsumer};
///
/// let consumers: Vec<Box<dyn DynShareConsumer<String, String>>> = vec![
///     Box::new(MockShareConsumer::<String, String>::new()),
///     Box::new(MockShareConsumer::<String, String>::new()),
/// ];
/// assert_eq!(2, consumers.len());
/// ```
///
/// The trait is sealed:
///
/// ```compile_fail,E0277
/// use confluent_kafka::consumer::DynShareConsumer;
///
/// struct Custom;
/// impl DynShareConsumer<String, String> for Custom {}
/// ```
// the dyn-compatible companion of the `ShareConsumer` trait (CLAUDE.md §3)
#[doc(alias = "rust-only")]
pub trait DynShareConsumer<K, V>: private::Sealed<K, V> + Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// See [`ShareConsumer::subscription`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::subscription`].
    fn subscription(&mut self) -> HashSet<String>;

    /// See [`ShareConsumer::subscribe`].
    ///
    /// # Arguments
    ///
    /// - `topics` - as [`ShareConsumer::subscribe`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::subscribe`].
    fn subscribe<'a>(&'a mut self, topics: Vec<String>) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`ShareConsumer::unsubscribe`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::unsubscribe`].
    fn unsubscribe<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`ShareConsumer::poll`].
    ///
    /// # Arguments
    ///
    /// - `timeout` - as [`ShareConsumer::poll`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::poll`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::poll`].
    fn poll<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<ConsumerRecords<K, V>, Error>>;

    /// See [`ShareConsumer::acknowledge_with_record`].
    ///
    /// # Arguments
    ///
    /// - `record` - as [`ShareConsumer::acknowledge_with_record`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::acknowledge_with_record`].
    fn acknowledge_with_record(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), Error>;

    /// See [`ShareConsumer::acknowledge_with_record_type`].
    ///
    /// # Arguments
    ///
    /// - `record` - as [`ShareConsumer::acknowledge_with_record_type`].
    /// - `r#type` - as [`ShareConsumer::acknowledge_with_record_type`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::acknowledge_with_record_type`].
    fn acknowledge_with_record_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error>;

    /// See [`ShareConsumer::acknowledge_with_options`].
    ///
    /// # Arguments
    ///
    /// - `options` - as [`ShareConsumer::acknowledge_with_options`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::acknowledge_with_options`].
    fn acknowledge_with_options(&mut self, options: AcknowledgeOptions) -> Result<(), Error>;

    /// See [`ShareConsumer::commit_sync`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::commit_sync`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::commit_sync`].
    fn commit_sync<'a>(&'a mut self) -> BoxFuture<'a, CommitSyncResult>;

    /// See [`ShareConsumer::commit_sync_with_timeout`].
    ///
    /// # Arguments
    ///
    /// - `timeout` - as [`ShareConsumer::commit_sync_with_timeout`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::commit_sync_with_timeout`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::commit_sync_with_timeout`].
    fn commit_sync_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, CommitSyncResult>;

    /// See [`ShareConsumer::commit_async`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::commit_async`].
    fn commit_async<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`ShareConsumer::set_acknowledgement_commit_callback`].
    ///
    /// # Arguments
    ///
    /// - `callback` - as [`ShareConsumer::set_acknowledgement_commit_callback`].
    ///
    /// # Panics
    ///
    /// As [`ShareConsumer::set_acknowledgement_commit_callback`].
    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>);

    /// See [`ShareConsumer::acquisition_lock_timeout_ms`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::acquisition_lock_timeout_ms`].
    fn acquisition_lock_timeout_ms(&mut self) -> Option<i32>;

    /// See [`ShareConsumer::metrics`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::metrics`].
    fn metrics(&self) -> MetricsView;

    /// See [`ShareConsumer::close`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::close`].
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`ShareConsumer::close_with_timeout`].
    ///
    /// # Arguments
    ///
    /// - `timeout` - as [`ShareConsumer::close_with_timeout`].
    ///
    /// # Errors
    ///
    /// As [`ShareConsumer::close_with_timeout`].
    fn close_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<(), Error>>;

    /// See [`ShareConsumer::wakeup`].
    fn wakeup(&self);

    /// See [`ShareConsumer::handle`].
    ///
    /// # Returns
    ///
    /// As [`ShareConsumer::handle`].
    fn handle(&self) -> ShareConsumerHandle;
}

impl<K, V, C> DynShareConsumer<K, V> for C
where
    K: Send + 'static,
    V: Send + 'static,
    C: ShareConsumer<K, V>,
{
    fn subscription(&mut self) -> HashSet<String> {
        ShareConsumer::subscription(self)
    }

    fn subscribe<'a>(&'a mut self, topics: Vec<String>) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(ShareConsumer::subscribe(self, topics))
    }

    fn unsubscribe<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(ShareConsumer::unsubscribe(self))
    }

    fn poll<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<ConsumerRecords<K, V>, Error>> {
        Box::pin(ShareConsumer::poll(self, timeout))
    }

    fn acknowledge_with_record(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), Error> {
        ShareConsumer::acknowledge_with_record(self, record)
    }

    fn acknowledge_with_record_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error> {
        ShareConsumer::acknowledge_with_record_type(self, record, r#type)
    }

    fn acknowledge_with_options(&mut self, options: AcknowledgeOptions) -> Result<(), Error> {
        ShareConsumer::acknowledge_with_options(self, options)
    }

    fn commit_sync<'a>(&'a mut self) -> BoxFuture<'a, CommitSyncResult> {
        Box::pin(ShareConsumer::commit_sync(self))
    }

    fn commit_sync_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, CommitSyncResult> {
        Box::pin(ShareConsumer::commit_sync_with_timeout(self, timeout))
    }

    fn commit_async<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(ShareConsumer::commit_async(self))
    }

    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {
        ShareConsumer::set_acknowledgement_commit_callback(self, callback)
    }

    fn acquisition_lock_timeout_ms(&mut self) -> Option<i32> {
        ShareConsumer::acquisition_lock_timeout_ms(self)
    }

    fn metrics(&self) -> MetricsView {
        ShareConsumer::metrics(self)
    }

    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(ShareConsumer::close(self))
    }

    fn close_with_timeout<'a>(&'a mut self, timeout: Duration) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(ShareConsumer::close_with_timeout(self, timeout))
    }

    fn wakeup(&self) {
        ShareConsumer::wakeup(self)
    }

    fn handle(&self) -> ShareConsumerHandle {
        ShareConsumer::handle(self)
    }
}

// On the trait object rather than on `Box<dyn DynShareConsumer>`: a boxed
// consumer that implemented `ShareConsumer` would also pick up the blanket
// `DynShareConsumer` impl, and a method call on the box would then box every
// future twice. No asynchronous method borrows an argument besides `self`, so
// each returns the boxed future directly.
impl<K, V> ShareConsumer<K, V> for dyn DynShareConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn subscription(&mut self) -> HashSet<String> {
        DynShareConsumer::subscription(self)
    }

    fn subscribe(&mut self, topics: Vec<String>) -> impl Future<Output = Result<(), Error>> + Send {
        DynShareConsumer::subscribe(self, topics)
    }

    fn unsubscribe(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynShareConsumer::unsubscribe(self)
    }

    fn poll(&mut self, timeout: Duration) -> impl Future<Output = Result<ConsumerRecords<K, V>, Error>> + Send {
        DynShareConsumer::poll(self, timeout)
    }

    fn acknowledge_with_record(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), Error> {
        DynShareConsumer::acknowledge_with_record(self, record)
    }

    fn acknowledge_with_record_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error> {
        DynShareConsumer::acknowledge_with_record_type(self, record, r#type)
    }

    fn acknowledge_with_options(&mut self, options: AcknowledgeOptions) -> Result<(), Error> {
        DynShareConsumer::acknowledge_with_options(self, options)
    }

    fn commit_sync(&mut self) -> impl Future<Output = CommitSyncResult> + Send {
        DynShareConsumer::commit_sync(self)
    }

    fn commit_sync_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = CommitSyncResult> + Send {
        DynShareConsumer::commit_sync_with_timeout(self, timeout)
    }

    fn commit_async(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynShareConsumer::commit_async(self)
    }

    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {
        DynShareConsumer::set_acknowledgement_commit_callback(self, callback)
    }

    fn acquisition_lock_timeout_ms(&mut self) -> Option<i32> {
        DynShareConsumer::acquisition_lock_timeout_ms(self)
    }

    fn metrics(&self) -> MetricsView {
        DynShareConsumer::metrics(self)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynShareConsumer::close(self)
    }

    fn close_with_timeout(&mut self, timeout: Duration) -> impl Future<Output = Result<(), Error>> + Send {
        DynShareConsumer::close_with_timeout(self, timeout)
    }

    fn wakeup(&self) {
        DynShareConsumer::wakeup(self)
    }

    fn handle(&self) -> ShareConsumerHandle {
        DynShareConsumer::handle(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::{AcknowledgeOptionsBuilder, MockShareConsumer};

    fn mock() -> MockShareConsumer<String, String> {
        MockShareConsumer::new()
    }

    fn topics(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn record(topic: &str, offset: i64) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(topic, 0, offset, Some(format!("key-{offset}")), Some(format!("value-{offset}")))
    }

    /// Generic code over `C: ShareConsumer + ?Sized`, including a method
    /// with a borrowed argument (`acknowledge_with_record`).
    async fn subscribe_and_acknowledge<C: ShareConsumer<String, String> + ?Sized>(
        consumer: &mut C,
        topic: &str,
    ) -> HashSet<String> {
        ShareConsumer::subscribe(consumer, topics(&[topic])).await.expect("subscribe");
        ShareConsumer::acknowledge_with_record(consumer, &record(topic, 0)).expect("the mock accepts any record");
        ShareConsumer::subscription(consumer)
    }

    /// R8 D1: a boxed share consumer keeps the `Send + !Sync` shape of the
    /// consumers it holds (`share-consumer.md` §1).
    #[test]
    fn dyn_share_consumer_is_send_and_not_sync() {
        use crate::test_thread_safety::{is_send, is_sync};
        assert!(is_send!(dyn DynShareConsumer<String, String>));
        assert!(!is_sync!(dyn DynShareConsumer<String, String>));
        assert!(is_send!(Box<dyn DynShareConsumer<String, String>>));
        assert!(!is_sync!(Box<dyn DynShareConsumer<String, String>>));
    }

    /// R8 D2: different consumers behind one type, each call reaching its own.
    #[tokio::test]
    async fn test_different_share_consumers_in_one_vec() {
        let mut first = mock();
        let mut second = mock();
        {
            let mut consumers: Vec<&mut dyn DynShareConsumer<String, String>> = vec![&mut first, &mut second];
            for (index, consumer) in consumers.iter_mut().enumerate() {
                let topic = format!("topic-{index}");
                DynShareConsumer::subscribe(&mut **consumer, vec![topic])
                    .await
                    .expect("subscribe");
            }
        }

        assert_eq!(HashSet::from(["topic-0".to_string()]), ShareConsumer::subscription(&mut first));
        assert_eq!(HashSet::from(["topic-1".to_string()]), ShareConsumer::subscription(&mut second));
    }

    /// R8 D2: boxed consumers in one `Vec`.
    #[tokio::test]
    async fn test_boxed_share_consumers_in_one_vec() {
        let mut consumers: Vec<Box<dyn DynShareConsumer<String, String>>> = vec![Box::new(mock()), Box::new(mock())];

        for consumer in &mut consumers {
            DynShareConsumer::subscribe(&mut **consumer, topics(&["topic"]))
                .await
                .expect("subscribe");
            assert_eq!(
                HashSet::from(["topic".to_string()]),
                DynShareConsumer::subscription(&mut **consumer)
            );
            DynShareConsumer::close(&mut **consumer).await.expect("close");
        }
    }

    /// R8 D3: `dyn DynShareConsumer` is a `ShareConsumer`, including the
    /// method with a borrowed argument.
    #[tokio::test]
    async fn test_dyn_share_consumer_is_a_share_consumer() {
        let mut boxed: Box<dyn DynShareConsumer<String, String>> = Box::new(mock());
        assert_eq!(
            HashSet::from(["topic".to_string()]),
            subscribe_and_acknowledge(&mut *boxed, "topic").await
        );
        let options = AcknowledgeOptionsBuilder::new()
            .set_topic("topic")
            .set_partition(0)
            .set_offset(1)
            .set_type(AcknowledgeType::Release)
            .build()
            .expect("every parameter is set");
        ShareConsumer::acknowledge_with_options(&mut *boxed, options).expect("the mock accepts any offset");
        assert!(ShareConsumer::commit_sync(&mut *boxed).await.expect("commit").is_empty());
        assert!(
            ShareConsumer::commit_sync_with_timeout(&mut *boxed, Duration::ZERO)
                .await
                .expect("commit")
                .is_empty()
        );
        ShareConsumer::commit_async(&mut *boxed).await.expect("commit");
    }

    /// R8 D4: errors pass through unchanged.
    #[tokio::test]
    async fn test_errors_pass_through_unchanged() {
        let mut direct = mock();
        let mut through_dyn = mock();
        ShareConsumer::close(&mut direct).await.expect("close");
        ShareConsumer::close(&mut through_dyn).await.expect("close");

        let expected = ShareConsumer::subscribe(&mut direct, topics(&["topic"]))
            .await
            .expect_err("subscribe on a closed mock");
        let as_dyn: &mut dyn DynShareConsumer<String, String> = &mut through_dyn;
        let error = DynShareConsumer::subscribe(as_dyn, topics(&["topic"]))
            .await
            .expect_err("same call through DynShareConsumer");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!("This consumer has already been closed.", error.message());
        assert_eq!(expected.to_string(), error.to_string());
    }

    /// R8 D5 (mock side): `wakeup`, `handle`, `close` and `metrics` reach the
    /// mock, each observed on its own. The mock's
    /// `acquisition_lock_timeout_ms` and `set_acknowledgement_commit_callback`
    /// are constants, as Java's, so their forwards are pinned on a
    /// `KafkaShareConsumer` (`kafka_share_consumer.rs`,
    /// `dyn_share_consumer_forwards_reach_a_kafka_share_consumer`).
    #[tokio::test]
    async fn test_sync_methods_reach_the_share_consumer() {
        let mut inner = mock();
        {
            let as_dyn: &mut dyn DynShareConsumer<String, String> = &mut inner;
            DynShareConsumer::wakeup(as_dyn);
        }
        assert!(inner.wakeup_requested(), "wakeup through DynShareConsumer reaches the mock");

        let other = mock();
        {
            let as_dyn: &dyn DynShareConsumer<String, String> = &other;
            DynShareConsumer::handle(as_dyn).wakeup();
        }
        assert!(other.wakeup_requested(), "the handle through DynShareConsumer is the mock's");

        let as_dyn: &mut dyn DynShareConsumer<String, String> = &mut inner;
        assert!(DynShareConsumer::metrics(as_dyn).is_empty());
        DynShareConsumer::close(as_dyn).await.expect("close");
        // The mock's `metrics` panics once it is closed, so a panic shows that
        // both `close` and `metrics` reached it.
        let metrics = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| DynShareConsumer::metrics(as_dyn)));
        let panic = metrics.expect_err("metrics on a closed mock panics");
        assert_eq!(
            Some(&"This consumer has already been closed.".to_string()),
            panic.downcast_ref::<String>()
        );
        let error = inner.add_record(record("topic", 0)).expect_err("closed");
        assert_eq!("This consumer has already been closed.", error.message());
    }

    /// R8 D6: the boxed futures are `Send`, so a boxed consumer can be driven
    /// from a spawned task.
    #[tokio::test]
    async fn test_dyn_share_consumer_futures_can_be_spawned() {
        let mut inner = mock();
        ShareConsumer::subscribe(&mut inner, topics(&["topic"]))
            .await
            .expect("subscribe");
        inner.add_record(record("topic", 0)).expect("subscribed");
        let mut boxed: Box<dyn DynShareConsumer<String, String>> = Box::new(inner);
        let task = tokio::spawn(async move {
            let records = DynShareConsumer::poll(&mut *boxed, Duration::ZERO).await?;
            DynShareConsumer::close(&mut *boxed).await?;
            Ok::<_, Error>(records.count())
        });
        assert_eq!(1, task.await.expect("join").expect("poll"));
    }
}
