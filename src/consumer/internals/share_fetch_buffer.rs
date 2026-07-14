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

//! `ShareFetchBuffer` — a thread-safe queue of [`ShareCompletedFetch`] entries
//! plus a wakeup primitive (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareFetchBuffer`.
//!
//! `ShareFetchBuffer` buffers up the results from the broker responses as they
//! are received. It is essentially a wrapper around a queue of
//! [`ShareCompletedFetch`]. There is at most one [`ShareCompletedFetch`] per
//! partition in the queue.
//!
//! *Note*: this type is thread-safe with the intention that the data will be
//! "produced" by the background task and consumed by the application task.
//!
//! # Translation decisions (consumer-threading.md §16 / §11)
//!
//! Mirrors the non-share [`FetchBuffer`](super::fetch_buffer::FetchBuffer):
//! inner state behind `std::sync::Mutex`, `AtomicBool` wakeup flag, and an
//! `async fn await_not_empty` built on `tokio::sync::Notify` + `tokio::time`.
//! Java's `InterruptException` path is dropped — tokio cancellation flows
//! through the consumer's wakeup token (consumer-threading.md §11).
//!
//! Java uses `peek()` (non-destructive) + `poll()` and a mutable
//! `nextInLineFetch()` reference. Because [`ShareCompletedFetch`] iteration is
//! `&mut` and not `Clone`, the Rust `ShareFetchCollector` works on an
//! ownership basis: [`Self::poll`] takes ownership, and [`Self::push_front`]
//! restores an entry to the front when initialization fails but the entry must
//! stay queued (same pattern as the non-share `FetchBuffer`).

#![allow(dead_code)]

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use log::warn;
use tokio::sync::Notify;

use crate::common::TopicIdPartition;
use crate::consumer::internals::share_completed_fetch::ShareCompletedFetch;

/// Thread-safe buffer of [`ShareCompletedFetch`] entries returned by share
/// fetch responses, awaiting consumption by the application.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareFetchBuffer`.
#[derive(Debug)]
pub(crate) struct ShareFetchBuffer {
    inner: Mutex<ShareFetchBufferInner>,
    notify: Notify,
    woken_up: AtomicBool,
}

#[derive(Debug, Default)]
struct ShareFetchBufferInner {
    completed_fetches: VecDeque<ShareCompletedFetch>,
    next_in_line_fetch: Option<ShareCompletedFetch>,
    closed: bool,
}

impl ShareFetchBuffer {
    /// Constructs an empty share fetch buffer.
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(ShareFetchBufferInner::default()),
            notify: Notify::new(),
            woken_up: AtomicBool::new(false),
        }
    }

    /// Returns `true` if there are no completed fetches pending return to the
    /// user. Translates `boolean isEmpty()`.
    pub(crate) fn is_empty(&self) -> bool {
        let guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.completed_fetches.is_empty()
    }

    /// Adds all completed fetches in the iterator to the buffer.
    ///
    /// Translates `void add(List<ShareCompletedFetch>)`.
    pub(crate) fn add<I>(&self, fetches: I)
    where
        I: IntoIterator<Item = ShareCompletedFetch>,
    {
        let mut guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        let mut any = false;
        for cf in fetches {
            guard.completed_fetches.push_back(cf);
            any = true;
        }
        if any {
            // Drop the lock before notifying. Java's `signalAll` runs with the
            // lock held, but tokio's `notify_waiters` is lock-free.
            drop(guard);
            self.woken_up.store(true, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    /// Returns whether the front of the queue is initialized (peek view).
    /// `None` if the queue is empty. Translates the
    /// `fetchBuffer.peek().isInitialized()` access pattern from
    /// `ShareFetchCollector.collect`.
    pub(crate) fn peek_is_initialized(&self) -> Option<bool> {
        let guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.completed_fetches.front().map(ShareCompletedFetch::is_initialized)
    }

    /// Removes and returns the next completed fetch in FIFO order, or `None` if
    /// the queue is empty. Translates `ShareCompletedFetch poll()`.
    pub(crate) fn poll(&self) -> Option<ShareCompletedFetch> {
        let mut guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.completed_fetches.pop_front()
    }

    /// Pushes a completed fetch back to the FRONT of the queue.
    ///
    /// This has no direct Java analog because `ShareFetchCollector.collect`
    /// uses `peek()` (non-destructive) before deciding whether to `poll()`. The
    /// Rust collector instead `poll()`s up front (taking ownership) and uses
    /// `push_front` to restore the entry when initialization fails and the
    /// entry must be left on the queue for the next `collect` call.
    pub(crate) fn push_front(&self, completed_fetch: ShareCompletedFetch) {
        let mut guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.completed_fetches.push_front(completed_fetch);
    }

    /// Returns whether there is a next-in-line fetch installed. Translates the
    /// `nextInLineFetch() != null` check that Java callers run after the getter.
    pub(crate) fn has_next_in_line_fetch(&self) -> bool {
        let guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.next_in_line_fetch.is_some()
    }

    /// Takes the current next-in-line fetch out of the buffer, leaving `None`
    /// in its place.
    pub(crate) fn take_next_in_line_fetch(&self) -> Option<ShareCompletedFetch> {
        let mut guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.next_in_line_fetch.take()
    }

    /// Sets the next-in-line fetch. Pass `None` to clear it.
    ///
    /// Translates `void setNextInLineFetch(ShareCompletedFetch)`. Unlike the
    /// non-share `FetchBuffer`, Java's `ShareFetchBuffer.setNextInLineFetch`
    /// does NOT drain the previous entry (there is no `retainAll` on this
    /// buffer); the outgoing value is simply replaced (and, in Rust, dropped).
    pub(crate) fn set_next_in_line_fetch(&self, fetch: Option<ShareCompletedFetch>) {
        let mut guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        guard.next_in_line_fetch = fetch;
    }

    /// Allows the caller to await presence of data in the buffer. Returns when
    /// the buffer is non-empty, an explicit [`Self::wakeup`] arrives, or
    /// `timeout` elapses.
    ///
    /// Translates `void awaitNotEmpty(Timer timer)`. Java throws
    /// `InterruptException` on thread interruption; the Rust port relies on
    /// tokio task cancellation propagating through the caller's wakeup token
    /// (consumer-threading.md §11) instead. Mirrors the race-free registration
    /// used by [`FetchBuffer::await_wakeup`](super::fetch_buffer::FetchBuffer::await_wakeup).
    pub(crate) async fn await_not_empty(&self, timeout: Duration) {
        // Short-circuit: already non-empty, or a wakeup happened before we got
        // here.
        if !self.is_empty() || self.woken_up.swap(false, Ordering::SeqCst) {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        // Register as a waiter BEFORE the second checks, so any concurrent
        // `notify_waiters` after our first check still finds us registered.
        notified.as_mut().enable();
        if !self.is_empty() || self.woken_up.swap(false, Ordering::SeqCst) {
            return;
        }
        let _ = tokio::time::timeout(timeout, notified).await;
        let _ = self.woken_up.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// Forces any task waiting in [`Self::await_not_empty`] to return.
    ///
    /// Translates `void wakeup()`.
    pub(crate) fn wakeup(&self) {
        self.woken_up.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Returns the set of partitions for which we have data in the buffer,
    /// either in the queue or the next-in-line slot.
    ///
    /// Translates `Set<TopicIdPartition> bufferedPartitions()`.
    pub(crate) fn buffered_partitions(&self) -> HashSet<TopicIdPartition> {
        let guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        let mut partitions: HashSet<TopicIdPartition> = HashSet::new();
        if let Some(next) = guard.next_in_line_fetch.as_ref()
            && !next.is_consumed()
        {
            partitions.insert(next.partition().clone());
        }
        for cf in &guard.completed_fetches {
            partitions.insert(cf.partition().clone());
        }
        partitions
    }

    /// Drops all buffered data and marks the buffer closed. Idempotent.
    ///
    /// Translates `void close()` (Java's `IdempotentCloser` + `drainAll`).
    pub(crate) fn close(&self) {
        let mut guard = self.inner.lock().expect("ShareFetchBuffer mutex poisoned");
        if guard.closed {
            warn!("The share fetch buffer was already closed");
            return;
        }
        guard.closed = true;
        for cf in &mut guard.completed_fetches {
            cf.drain();
        }
        guard.completed_fetches.clear();
        if let Some(next) = guard.next_in_line_fetch.as_mut() {
            next.drain();
            guard.next_in_line_fetch = None;
        }
    }
}

impl Default for ShareFetchBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ShareFetchBuffer {
    fn drop(&mut self) {
        // Mirror Java's try-with-resources cleanup semantics.
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{TopicPartition, Uuid};
    use crate::share_fetch_response_data::PartitionData;
    use std::sync::Arc;
    use std::time::Instant;

    fn tip(topic: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new(topic.to_string(), partition))
    }

    fn completed_fetch(tp: TopicIdPartition) -> ShareCompletedFetch {
        ShareCompletedFetch::new(0, tp, PartitionData::new(), Some(30_000))
    }

    /// Translated from `ShareFetchBufferTest.testBasicPeekAndPoll`. Java's
    /// `assertSame(reference)` is replaced with a partition-equality check on
    /// the polled value (`ShareCompletedFetch` is not `Clone`).
    #[test]
    fn test_basic_peek_and_poll() {
        let buffer = ShareFetchBuffer::new();
        let tp = tip("topic-a", 0);
        assert!(buffer.is_empty());
        buffer.add([completed_fetch(tp.clone())]);
        assert!(!buffer.is_empty());
        // peek: front present.
        assert_eq!(Some(false), buffer.peek_is_initialized());
        let popped = buffer.poll().expect("popped");
        assert_eq!(&tp, popped.partition());
        assert!(buffer.poll().is_none());
        assert_eq!(None, buffer.peek_is_initialized());
    }

    /// Translated from `ShareFetchBufferTest.testCloseClearsData`.
    #[test]
    fn test_close_clears_data() {
        let buffer = ShareFetchBuffer::new();
        assert!(!buffer.has_next_in_line_fetch());
        assert!(buffer.is_empty());

        buffer.add([completed_fetch(tip("topic-a", 0))]);
        assert!(!buffer.is_empty());

        buffer.set_next_in_line_fetch(Some(completed_fetch(tip("topic-a", 0))));
        assert!(buffer.has_next_in_line_fetch());

        buffer.close();
        assert!(!buffer.has_next_in_line_fetch());
        assert!(buffer.is_empty());
    }

    /// Translated from `ShareFetchBufferTest.testBufferedPartitions`.
    #[test]
    fn test_buffered_partitions() {
        let buffer = ShareFetchBuffer::new();
        let tp0 = tip("topic-a", 0);
        let tp1 = tip("topic-a", 1);
        let tp2 = tip("topic-a", 2);
        buffer.set_next_in_line_fetch(Some(completed_fetch(tp0.clone())));
        buffer.add([completed_fetch(tp1.clone()), completed_fetch(tp2.clone())]);
        let all: HashSet<TopicIdPartition> = [tp0.clone(), tp1.clone(), tp2.clone()].into_iter().collect();
        assert_eq!(all, buffer.buffered_partitions());

        buffer.set_next_in_line_fetch(None);
        let expected: HashSet<TopicIdPartition> = [tp1.clone(), tp2.clone()].into_iter().collect();
        assert_eq!(expected, buffer.buffered_partitions());

        buffer.poll();
        let expected: HashSet<TopicIdPartition> = [tp2.clone()].into_iter().collect();
        assert_eq!(expected, buffer.buffered_partitions());

        buffer.poll();
        assert_eq!(HashSet::new(), buffer.buffered_partitions());
    }

    /// Translated from `ShareFetchBufferTest.testWakeup`. A separate tokio task
    /// awaits with a long timeout; the main task wakes it up.
    #[tokio::test(flavor = "current_thread")]
    async fn test_wakeup() {
        let buffer = Arc::new(ShareFetchBuffer::new());
        let buffer_for_task = Arc::clone(&buffer);
        let task = tokio::spawn(async move {
            buffer_for_task.await_not_empty(Duration::from_secs(60)).await;
        });
        // Give the task a beat to enter `await_not_empty`.
        tokio::task::yield_now().await;
        buffer.wakeup();
        let join_result = tokio::time::timeout(Duration::from_secs(10), task).await;
        assert!(join_result.is_ok(), "wakeup did not release the awaiting task within 10s");
    }

    /// `await_not_empty` short-circuits when the buffer is already non-empty.
    #[tokio::test(flavor = "current_thread")]
    async fn test_await_not_empty_returns_when_non_empty() {
        let buffer = ShareFetchBuffer::new();
        buffer.add([completed_fetch(tip("topic-a", 0))]);
        let start = Instant::now();
        buffer.await_not_empty(Duration::from_secs(30)).await;
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "await_not_empty did not short-circuit on non-empty buffer"
        );
    }

    /// `await_not_empty` returns after `timeout` if no data / wakeup arrives.
    #[tokio::test(flavor = "current_thread")]
    async fn test_await_not_empty_times_out() {
        let buffer = ShareFetchBuffer::new();
        let start = Instant::now();
        buffer.await_not_empty(Duration::from_millis(50)).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(40), "returned too quickly: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "await_not_empty hung: {elapsed:?}");
    }
}
