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

//! `ShareFetchBuffer` — the queue of [`ShareCompletedFetch`]es between the
//! background task that adds them and the application task that reads them.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareFetchBuffer`.
//!
//! Follows the consumer's `FetchBuffer` (`share-consumer.md` §6): the state is
//! behind a `std::sync::Mutex`, never held across an `.await`, and the wait is
//! a `tokio::sync::Notify` with a woken-up flag. Java's `ReentrantLock` /
//! `Condition` become those; its `InterruptException` paths have no Rust
//! counterpart (no thread interruption): a waiting `poll` is ended through
//! the consumer's wakeup token (`share-consumer.md` §8). Java's
//! `IdempotentCloser` is a `closed` flag, as in `FetchBuffer` (ledger G14).

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use log::warn;
use tokio::sync::Notify;

use crate::common::TopicIdPartition;
use crate::consumer::internals::ShareCompletedFetch;

/// Buffers up the results from the broker responses as they are received.
/// It is essentially a wrapper around a queue of [`ShareCompletedFetch`].
/// There is at most one [`ShareCompletedFetch`] per partition in the queue.
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.ShareFetchBuffer`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer")]
#[derive(Debug)]
pub(crate) struct ShareFetchBuffer {
    inner: Mutex<Inner>,
    notify: Notify,
    woken_up: AtomicBool,
}

#[derive(Debug, Default)]
struct Inner {
    completed_fetches: VecDeque<ShareCompletedFetch>,
    next_in_line_fetch: Option<ShareCompletedFetch>,
    /// Fetches which have been consumed but still have acknowledgements outstanding, retained here after they leave
    /// the next-in-line slot so that the share session for their node is not closed prematurely.
    pending_acknowledgement_fetches: Vec<ShareCompletedFetch>,
    /// The partition and node of the next-in-line fetch while
    /// [`ShareFetchBuffer::take_next_in_line_fetch`] has handed it out.
    ///
    /// Rust-only (no Java field). Java's `ShareFetchCollector` works on the
    /// next-in-line fetch in place, so `bufferedPartitions` / `bufferedNodes`
    /// keep reporting it. The Rust collector moves it out to read it across the
    /// deserializers' `.await`s, which the lock cannot be held across; this
    /// marker keeps its partition and node buffered meanwhile, so the
    /// background task does not close the share session under it.
    checked_out: Option<(TopicIdPartition, i32)>,
    closed: bool,
}

impl ShareFetchBuffer {
    /// An empty buffer. Java's `LogContext` is dropped (the `log` crate is
    /// used).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#ShareFetchBuffer")]
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
            woken_up: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("ShareFetchBuffer mutex poisoned")
    }

    /// Returns `true` if there are no completed fetches pending to return to
    /// the user.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#isEmpty")]
    pub(crate) fn is_empty(&self) -> bool {
        self.lock().completed_fetches.is_empty()
    }

    /// Adds completed fetches and wakes a waiting [`Self::await_not_empty`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#add")]
    pub(crate) fn add(&self, fetches: impl IntoIterator<Item = ShareCompletedFetch>) {
        self.lock().completed_fetches.extend(fetches);
        // Java's `notEmptyCondition.signalAll()`; `notify_waiters` is lock-free.
        self.notify.notify_waiters();
    }

    /// Calls `f` with the next-in-line fetch: Java's `nextInLineFetch()`, which
    /// returns the reference. The buffer's lock is held during the call.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#nextInLineFetch")]
    pub(crate) fn next_in_line_fetch<R>(&self, f: impl FnOnce(Option<&ShareCompletedFetch>) -> R) -> R {
        f(self.lock().next_in_line_fetch.as_ref())
    }

    /// Sets the next-in-line fetch. If the outgoing one still has outstanding
    /// acknowledgements, it is retained until they are sent.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#setNextInLineFetch")]
    pub(crate) fn set_next_in_line_fetch(&self, next_in_line_fetch: Option<ShareCompletedFetch>) {
        let mut inner = self.lock();
        // If the outgoing next-in-line fetch still has outstanding acknowledgements,
        // retain it until the acknowledgements are sent.
        if let Some(outgoing) = inner.next_in_line_fetch.take()
            && outgoing.has_pending_acknowledgements()
        {
            inner.pending_acknowledgement_fetches.push(outgoing);
        }
        inner.next_in_line_fetch = next_in_line_fetch;
        inner.checked_out = None;
    }

    /// Takes the next-in-line fetch out of the buffer for the collector to read,
    /// keeping its partition and node buffered until it is set back with
    /// [`Self::set_next_in_line_fetch`] (see `Inner::checked_out`).
    ///
    /// Rust-only: Java's collector reads it in place.
    pub(crate) fn take_next_in_line_fetch(&self) -> Option<ShareCompletedFetch> {
        let mut inner = self.lock();
        let fetch = inner.next_in_line_fetch.take();
        inner.checked_out = fetch.as_ref().map(|cf| (cf.partition.clone(), cf.node_id));
        fetch
    }

    /// Calls `f` with the head of the queue without removing it: Java's
    /// `peek()`. The buffer's lock is held during the call.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#peek")]
    pub(crate) fn peek<R>(&self, f: impl FnOnce(Option<&ShareCompletedFetch>) -> R) -> R {
        f(self.lock().completed_fetches.front())
    }

    /// Removes and returns the head of the queue.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#poll")]
    pub(crate) fn poll(&self) -> Option<ShareCompletedFetch> {
        self.lock().completed_fetches.pop_front()
    }

    /// Removes and returns the head of the queue, as [`Self::poll`] does, but
    /// keeps its partition and node buffered (see `Inner::checked_out`) until
    /// the caller sets it as the next-in-line fetch, puts it back with
    /// [`Self::push_front`], or gives it up with [`Self::clear_checked_out`].
    ///
    /// Rust-only: Java's collector initializes the head of the queue in place
    /// and sets it next-in-line before it polls it, so it never leaves the
    /// buffer (`ShareFetchCollector#collect`).
    pub(crate) fn poll_checked_out(&self) -> Option<ShareCompletedFetch> {
        let mut inner = self.lock();
        let fetch = inner.completed_fetches.pop_front();
        inner.checked_out = fetch.as_ref().map(|cf| (cf.partition.clone(), cf.node_id));
        fetch
    }

    /// Puts a fetch [`Self::poll_checked_out`] handed out back at the head of
    /// the queue. Rust-only: see [`Self::poll_checked_out`].
    pub(crate) fn push_front(&self, completed_fetch: ShareCompletedFetch) {
        let mut inner = self.lock();
        inner.completed_fetches.push_front(completed_fetch);
        inner.checked_out = None;
    }

    /// Forgets the fetch [`Self::poll_checked_out`] handed out, which the
    /// caller discards (Java's `fetchBuffer.poll()` after a failed
    /// `initialize`). Rust-only: see [`Self::poll_checked_out`].
    pub(crate) fn clear_checked_out(&self) {
        self.lock().checked_out = None;
    }

    /// Allows the caller to await presence of data in the buffer. Returns when
    /// the buffer was already non-empty on entry, was populated during the
    /// wait, was woken up ([`Self::wakeup`]), or `timeout` elapsed.
    ///
    /// The waiter registers before it checks the buffer again, so an `add` or
    /// `wakeup` racing the check is not lost (as `FetchBuffer::await_wakeup`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#awaitNotEmpty")]
    pub(crate) async fn await_not_empty(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if !self.is_empty() || self.woken_up.swap(false, Ordering::SeqCst) {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.is_empty() || self.woken_up.swap(false, Ordering::SeqCst) {
                return;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return;
            }
        }
    }

    /// Ends a wait in [`Self::await_not_empty`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#wakeup")]
    pub(crate) fn wakeup(&self) {
        self.woken_up.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Return the set of partitions for which we have data in the buffer or
    /// pending acknowledgements.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#bufferedPartitions")]
    pub(crate) fn buffered_partitions(&self) -> HashSet<TopicIdPartition> {
        let mut inner = self.lock();
        let mut partitions = HashSet::new();
        if let Some(next) = &inner.next_in_line_fetch
            && (!next.is_consumed() || next.has_pending_acknowledgements())
        {
            partitions.insert(next.partition.clone());
        }
        if let Some((partition, _)) = &inner.checked_out {
            partitions.insert(partition.clone());
        }
        partitions.extend(inner.completed_fetches.iter().map(|cf| cf.partition.clone()));
        Self::prune_pending_acknowledgement_fetches(&mut inner);
        partitions.extend(inner.pending_acknowledgement_fetches.iter().map(|cf| cf.partition.clone()));
        partitions
    }

    /// Return the set of node IDs for which we have data in the buffer.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#bufferedNodes")]
    pub(crate) fn buffered_nodes(&self) -> HashSet<i32> {
        let mut inner = self.lock();
        let mut nodes = HashSet::new();
        if let Some(next) = &inner.next_in_line_fetch
            && (!next.is_consumed() || next.has_pending_acknowledgements())
        {
            nodes.insert(next.node_id);
        }
        if let Some((_, node)) = &inner.checked_out {
            nodes.insert(*node);
        }
        nodes.extend(inner.completed_fetches.iter().map(|cf| cf.node_id));
        Self::prune_pending_acknowledgement_fetches(&mut inner);
        nodes.extend(inner.pending_acknowledgement_fetches.iter().map(|cf| cf.node_id));
        nodes
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#prunePendingAcknowledgementFetches")]
    fn prune_pending_acknowledgement_fetches(inner: &mut Inner) {
        inner
            .pending_acknowledgement_fetches
            .retain(ShareCompletedFetch::has_pending_acknowledgements);
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#drainAll")]
    fn drain_all(inner: &mut Inner) {
        for cf in &mut inner.completed_fetches {
            cf.drain();
        }
        inner.completed_fetches.clear();
        if let Some(mut next) = inner.next_in_line_fetch.take() {
            next.drain();
        }
        for cf in &mut inner.pending_acknowledgement_fetches {
            cf.drain();
        }
        inner.pending_acknowledgement_fetches.clear();
        inner.checked_out = None;
    }

    /// Drops all buffered data. A second call only logs.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchBuffer#close")]
    pub(crate) fn close(&self) {
        let mut inner = self.lock();
        if inner.closed {
            warn!("The fetch buffer was already closed");
            return;
        }
        inner.closed = true;
        Self::drain_all(&mut inner);
    }
}

impl Default for ShareFetchBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `ShareFetchBufferTest` (8 tests). Java's tests keep a
    //! reference to a fetch after adding it to the buffer and compare it with
    //! `assertSame`; here the buffer owns it, so the tests read it through
    //! [`ShareFetchBuffer::peek`] / [`ShareFetchBuffer::next_in_line_fetch`]
    //! and identify it by partition and node.

    use std::sync::Arc;

    use futures_util::FutureExt;

    use super::*;
    use crate::common::Uuid;
    use crate::common::metrics::Metrics;
    use crate::common::protocol::ApiKeys;
    use crate::common::utils::internals::BufferSupplier;
    use crate::consumer::AcknowledgeType;
    use crate::consumer::internals::share_completed_fetch::tests::{
        acquired_records, new_string_deserializers, partition_data,
    };
    use crate::consumer::internals::{
        ShareFetchMetricsAggregator, ShareFetchMetricsManager, ShareFetchMetricsRegistry, ShareInFlightBatch,
    };
    use crate::share_fetch_response_data::PartitionData;

    const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30000);

    struct Fixture {
        topic_a_partition0: TopicIdPartition,
        topic_a_partition1: TopicIdPartition,
        topic_a_partition2: TopicIdPartition,
        manager: Arc<ShareFetchMetricsManager>,
    }

    impl Fixture {
        fn new() -> Self {
            let metrics = Arc::new(Metrics::new());
            Self {
                topic_a_partition0: TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 0, "topic-a"),
                topic_a_partition1: TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 1, "topic-a"),
                topic_a_partition2: TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 2, "topic-a"),
                manager: Arc::new(ShareFetchMetricsManager::new(metrics, &ShareFetchMetricsRegistry::new())),
            }
        }

        fn all_partitions(&self) -> HashSet<TopicIdPartition> {
            partitions(&[
                &self.topic_a_partition0,
                &self.topic_a_partition1,
                &self.topic_a_partition2,
            ])
        }

        fn completed_fetch(&self, tp: &TopicIdPartition) -> ShareCompletedFetch {
            self.completed_fetch_with(tp, 0, PartitionData::new())
        }

        fn completed_fetch_with_acquired_records(
            &self,
            tp: &TopicIdPartition,
            node_id: i32,
            num_records: i32,
        ) -> ShareCompletedFetch {
            let mut builder = crate::common::record::internal::MemoryRecords::builder_with_initial_capacity(
                1024,
                crate::common::compress::Compression::none().build(),
                crate::common::record::TimestampType::CreateTime,
                0,
            );
            for _ in 0..num_records {
                builder.append_kv(0, Some(b"key"), Some(b"value"));
            }
            let data = partition_data(Some(builder.build().into_buffer()), acquired_records(0, i64::from(num_records)));
            self.completed_fetch_with(tp, node_id, data)
        }

        fn completed_fetch_with(
            &self,
            tp: &TopicIdPartition,
            node_id: i32,
            data: PartitionData,
        ) -> ShareCompletedFetch {
            let aggregator = Arc::new(ShareFetchMetricsAggregator::new(
                Arc::clone(&self.manager),
                self.all_partitions().iter().map(|p| p.topic_partition().clone()).collect(),
            ));
            ShareCompletedFetch::new(
                Arc::new(BufferSupplier::create()),
                node_id,
                tp.clone(),
                data,
                DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
                aggregator,
                ApiKeys::SHARE_FETCH.latest_version(),
            )
        }
    }

    fn partitions(partitions: &[&TopicIdPartition]) -> HashSet<TopicIdPartition> {
        partitions.iter().map(|&p| p.clone()).collect()
    }

    fn consume_fetch_with_acquired_records(
        completed_fetch: &mut ShareCompletedFetch,
    ) -> ShareInFlightBatch<String, String> {
        let deserializers = new_string_deserializers();
        let mut batch = completed_fetch
            .fetch_records(&deserializers, 100, false)
            .now_or_never()
            .expect("ready")
            .expect("the records decode");
        drop(batch.in_flight_records());
        assert!(completed_fetch.is_consumed());
        assert!(completed_fetch.has_pending_acknowledgements());
        batch
    }

    fn next_in_line_has_pending_acknowledgements(buffer: &ShareFetchBuffer) -> bool {
        buffer.next_in_line_fetch(|cf| cf.expect("a next-in-line fetch").has_pending_acknowledgements())
    }

    /// Verifies the basics: we can add buffered data to the queue, peek to view them, and poll to remove them.
    #[test]
    fn test_basic_peek_and_poll() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        let completed_fetch = f.completed_fetch(&f.topic_a_partition0);
        assert!(fetch_buffer.is_empty());
        fetch_buffer.add([completed_fetch]);
        assert!(!fetch_buffer.is_empty());
        assert_eq!(
            Some(f.topic_a_partition0.clone()),
            fetch_buffer.peek(|cf| cf.map(|cf| cf.partition.clone()))
        );
        assert_eq!(Some(f.topic_a_partition0.clone()), fetch_buffer.poll().map(|cf| cf.partition));
        assert!(fetch_buffer.peek(|cf| cf.is_none()));
        fetch_buffer.close();
    }

    /// Verifies close clears the buffered data for both the queue and the next-in-line buffer.
    #[test]
    fn test_close_clears_data() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        assert!(fetch_buffer.next_in_line_fetch(|cf| cf.is_none()));
        assert!(fetch_buffer.is_empty());

        fetch_buffer.add([f.completed_fetch(&f.topic_a_partition0)]);
        assert!(!fetch_buffer.is_empty());

        fetch_buffer.set_next_in_line_fetch(Some(f.completed_fetch(&f.topic_a_partition0)));
        assert!(fetch_buffer.next_in_line_fetch(|cf| cf.is_some()));
        fetch_buffer.close();

        assert!(fetch_buffer.next_in_line_fetch(|cf| cf.is_none()));
        assert!(fetch_buffer.is_empty());
    }

    /// Tests that the buffer returns partitions for both the queue and the next-in-line buffer.
    #[test]
    fn test_buffered_partitions() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        fetch_buffer.set_next_in_line_fetch(Some(f.completed_fetch(&f.topic_a_partition0)));
        fetch_buffer.add([
            f.completed_fetch(&f.topic_a_partition1),
            f.completed_fetch(&f.topic_a_partition2),
        ]);
        assert_eq!(f.all_partitions(), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());

        fetch_buffer.set_next_in_line_fetch(None);
        assert_eq!(
            partitions(&[&f.topic_a_partition1, &f.topic_a_partition2]),
            fetch_buffer.buffered_partitions()
        );
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());

        fetch_buffer.poll();
        assert_eq!(partitions(&[&f.topic_a_partition2]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());

        fetch_buffer.poll();
        assert_eq!(partitions(&[]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::new(), fetch_buffer.buffered_nodes());
        fetch_buffer.close();
    }

    /// Tests that a fetch which has been consumed but whose acknowledgements are still outstanding continues to be
    /// reported as buffered, thus preventing the share session for its node being closed prematurely.
    #[test]
    fn test_buffered_includes_consumed_fetch_with_pending_acknowledgements() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        let mut completed_fetch = f.completed_fetch_with_acquired_records(&f.topic_a_partition0, 0, 5);
        let mut batch = consume_fetch_with_acquired_records(&mut completed_fetch);
        fetch_buffer.set_next_in_line_fetch(Some(completed_fetch));

        // Even though the fetch has been consumed, its acknowledgements are still outstanding, so it is still buffered.
        assert_eq!(partitions(&[&f.topic_a_partition0]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());
        assert!(next_in_line_has_pending_acknowledgements(&fetch_buffer));

        // Acknowledging and taking the records clears the outstanding acknowledgements, so it is no longer buffered.
        batch.acknowledge_all(AcknowledgeType::Accept);
        batch.take_acknowledged_records();
        assert!(!next_in_line_has_pending_acknowledgements(&fetch_buffer));
        assert_eq!(partitions(&[]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::new(), fetch_buffer.buffered_nodes());
        fetch_buffer.close();
    }

    #[test]
    fn test_retains_evicted_fetch_with_pending_acknowledgements() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        let mut fetch_for_node0 = f.completed_fetch_with_acquired_records(&f.topic_a_partition0, 0, 5);
        let mut batch = consume_fetch_with_acquired_records(&mut fetch_for_node0);
        fetch_buffer.set_next_in_line_fetch(Some(fetch_for_node0));

        // Replacing the next-in-line fetch with one for a different node still retains the node 0 fetch because
        // its acknowledgements are outstanding.
        fetch_buffer.set_next_in_line_fetch(Some(f.completed_fetch_with_acquired_records(&f.topic_a_partition1, 1, 5)));
        assert_eq!(
            partitions(&[&f.topic_a_partition0, &f.topic_a_partition1]),
            fetch_buffer.buffered_partitions()
        );
        assert_eq!(HashSet::from([0, 1]), fetch_buffer.buffered_nodes());

        // Once node 0's acknowledgements are taken, the retained fetch is pruned.
        batch.acknowledge_all(AcknowledgeType::Accept);
        batch.take_acknowledged_records();
        assert_eq!(partitions(&[&f.topic_a_partition1]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::from([1]), fetch_buffer.buffered_nodes());
        fetch_buffer.close();
    }

    #[test]
    fn test_retains_evicted_fetch_with_pending_renew_acknowledgements() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        let mut completed_fetch = f.completed_fetch_with_acquired_records(&f.topic_a_partition0, 0, 5);
        let mut batch = consume_fetch_with_acquired_records(&mut completed_fetch);
        fetch_buffer.set_next_in_line_fetch(Some(completed_fetch));

        // Renewing the records moves them out of the in-flight set, but they are still held and the fetch remains buffered.
        batch.acknowledge_all(AcknowledgeType::Renew);
        let mut renew_acknowledgements = batch.take_acknowledged_records();
        assert!(next_in_line_has_pending_acknowledgements(&fetch_buffer));
        assert_eq!(partitions(&[&f.topic_a_partition0]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());

        // The renewal completed and the records move to the renewed state, and they are still held.
        renew_acknowledgements.complete(None);
        batch.renew(&renew_acknowledgements).unwrap();
        assert!(next_in_line_has_pending_acknowledgements(&fetch_buffer));
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());

        // The renewed records return to the in-flight set, so still buffered.
        let deserializers = new_string_deserializers();
        assert!(batch.take_renewals(&deserializers).now_or_never().expect("ready").is_none());
        assert!(next_in_line_has_pending_acknowledgements(&fetch_buffer));
        assert_eq!(HashSet::from([0]), fetch_buffer.buffered_nodes());

        // Finally the records accepted and the fetch is no longer buffered.
        batch.acknowledge_all(AcknowledgeType::Accept);
        batch.take_acknowledged_records();
        assert!(!next_in_line_has_pending_acknowledgements(&fetch_buffer));
        assert_eq!(partitions(&[]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::new(), fetch_buffer.buffered_nodes());
        fetch_buffer.close();
    }

    #[test]
    fn test_close_clears_pending_acknowledgement_fetches() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        let mut fetch_for_node0 = f.completed_fetch_with_acquired_records(&f.topic_a_partition0, 0, 5);
        let _batch = consume_fetch_with_acquired_records(&mut fetch_for_node0);
        fetch_buffer.set_next_in_line_fetch(Some(fetch_for_node0));

        // Replace the next-in-line fetch with one for a different node
        fetch_buffer.set_next_in_line_fetch(Some(f.completed_fetch_with_acquired_records(&f.topic_a_partition1, 1, 5)));
        assert_eq!(HashSet::from([0, 1]), fetch_buffer.buffered_nodes());
        fetch_buffer.close();

        assert_eq!(partitions(&[]), fetch_buffer.buffered_partitions());
        assert_eq!(HashSet::new(), fetch_buffer.buffered_nodes());
    }

    /// Java waits on another thread and joins it within 30 seconds; here the
    /// wait is polled once (registering it), woken, and must then complete.
    #[tokio::test]
    async fn test_wakeup() {
        let fetch_buffer = ShareFetchBuffer::new();
        let wait = fetch_buffer.await_not_empty(Duration::from_secs(60));
        tokio::pin!(wait);
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        fetch_buffer.wakeup();
        tokio::time::timeout(Duration::from_secs(30), wait)
            .await
            .expect("the wakeup ends the wait");
        fetch_buffer.close();
    }

    /// Rust-only: a second close only logs, as Java's `IdempotentCloser`.
    #[test]
    fn test_close_is_idempotent() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        fetch_buffer.close();
        fetch_buffer.add([f.completed_fetch(&f.topic_a_partition0)]);
        fetch_buffer.close();
        assert!(!fetch_buffer.is_empty(), "the second close does not drain again");
    }

    /// Rust-only: a fetch the collector has taken out stays buffered until it
    /// is set back or given up (`Inner::checked_out`).
    #[test]
    fn test_checked_out_fetch_stays_buffered() {
        let f = Fixture::new();
        let fetch_buffer = ShareFetchBuffer::new();
        fetch_buffer.add([f.completed_fetch(&f.topic_a_partition0)]);
        let cf = fetch_buffer.poll_checked_out().expect("a fetch");
        assert_eq!(partitions(&[&f.topic_a_partition0]), fetch_buffer.buffered_partitions());
        fetch_buffer.push_front(cf);
        assert_eq!(partitions(&[&f.topic_a_partition0]), fetch_buffer.buffered_partitions());
        let _discarded = fetch_buffer.poll_checked_out();
        fetch_buffer.clear_checked_out();
        assert!(fetch_buffer.buffered_partitions().is_empty());

        fetch_buffer.set_next_in_line_fetch(Some(f.completed_fetch(&f.topic_a_partition1)));
        let next = fetch_buffer.take_next_in_line_fetch();
        assert_eq!(partitions(&[&f.topic_a_partition1]), fetch_buffer.buffered_partitions());
        fetch_buffer.set_next_in_line_fetch(next);
        assert_eq!(partitions(&[&f.topic_a_partition1]), fetch_buffer.buffered_partitions());
    }
}
