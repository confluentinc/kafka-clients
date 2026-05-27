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

//! `FetchBuffer` — a thread-safe queue of [`CompletedFetch`] entries plus a
//! wakeup primitive.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.FetchBuffer`.
//!
//! # Translation decisions (consumer-threading.md §16 / §11)
//!
//! - Inner state behind `std::sync::Mutex<FetchBufferInner>` — the buffer
//!   is shared between the bg task that adds and the app task that polls,
//!   critical sections are short and never await.
//! - `wokenup: AtomicBool` for the cross-task wakeup flag.
//! - `await_wakeup(timeout)` is `async`: uses `tokio::time::timeout` with
//!   `tokio::sync::Notify::notified()`. Java's `InterruptException` path
//!   is dropped — tokio cancellation flows through the consumer's wakeup
//!   token (consumer-threading.md §11), not through the buffer.
//!
//! `peek` returns nothing in this Rust port — Java's `CompletedFetch` is a
//! mutable reference and the buffer's caller cooperates on lifetime. The
//! Rust port retains the Java contract by exposing
//! [`FetchBuffer::has_completed_fetches`] (Java's predicate-based check)
//! for the few call sites that want a peek-shaped operation.

#![allow(dead_code)]

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use log::debug;
use tokio::sync::Notify;

use crate::common::TopicPartition;
use crate::consumer::internals::completed_fetch::CompletedFetch;

/// Thread-safe buffer of [`CompletedFetch`] entries returned by fetch
/// responses, awaiting consumption by the application.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.FetchBuffer`.
#[derive(Debug)]
pub(crate) struct FetchBuffer {
    inner: Mutex<FetchBufferInner>,
    notify: Notify,
    wokenup: AtomicBool,
}

#[derive(Debug, Default)]
struct FetchBufferInner {
    completed_fetches: VecDeque<CompletedFetch>,
    next_in_line_fetch: Option<CompletedFetch>,
    closed: bool,
}

impl FetchBuffer {
    /// Constructs an empty fetch buffer.
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(FetchBufferInner::default()),
            notify: Notify::new(),
            wokenup: AtomicBool::new(false),
        }
    }

    /// Returns true if there are no completed fetches pending return to
    /// the user.
    ///
    /// Translates `boolean isEmpty()`.
    pub(crate) fn is_empty(&self) -> bool {
        let guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        guard.completed_fetches.is_empty()
    }

    /// Returns true if any completed fetch matches the predicate.
    ///
    /// Translates `boolean hasCompletedFetches(Predicate)`.
    pub(crate) fn has_completed_fetches(&self, predicate: impl FnMut(&CompletedFetch) -> bool) -> bool {
        let guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        guard.completed_fetches.iter().any(predicate)
    }

    /// Adds a single completed fetch to the buffer.
    ///
    /// Translates `void add(CompletedFetch)`.
    pub(crate) fn add(&self, completed_fetch: CompletedFetch) {
        self.add_all_impl([completed_fetch]);
    }

    /// Adds all completed fetches in the iterator to the buffer.
    ///
    /// Translates `void addAll(Collection<CompletedFetch>)`.
    pub(crate) fn add_all<I>(&self, completed_fetches: I)
    where
        I: IntoIterator<Item = CompletedFetch>,
    {
        self.add_all_impl(completed_fetches);
    }

    fn add_all_impl<I>(&self, items: I)
    where
        I: IntoIterator<Item = CompletedFetch>,
    {
        let mut guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        let mut any = false;
        for cf in items {
            guard.completed_fetches.push_back(cf);
            any = true;
        }
        if any {
            // Drop the lock before notifying to avoid contention on the
            // notify path. Java's signalAll runs with the lock held, but
            // tokio's Notify::notify_waiters is lock-free.
            drop(guard);
            self.wokenup.store(true, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    /// Removes and returns the next completed fetch in FIFO order, or
    /// `None` if the buffer is empty.
    ///
    /// Translates `CompletedFetch poll()`.
    pub(crate) fn poll(&self) -> Option<CompletedFetch> {
        let mut guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        guard.completed_fetches.pop_front()
    }

    /// Returns the next-in-line fetch (the one currently being iterated
    /// by `FetchCollector`).
    ///
    /// Java returns the reference directly; Rust returns `None` if absent
    /// (the caller does not need ownership in the way Java's mutable
    /// reference semantics imply — Phase 7b's `FetchCollector` will use
    /// [`FetchBuffer::take_next_in_line_fetch`] / `set_next_in_line_fetch`
    /// to move it in and out).
    ///
    /// Translates `CompletedFetch nextInLineFetch()` — the boolean
    /// "is there a next-in-line, and is it not yet consumed" check that
    /// Java callers run after this getter.
    pub(crate) fn has_next_in_line_fetch(&self) -> bool {
        let guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        guard.next_in_line_fetch.is_some()
    }

    /// Takes the current next-in-line fetch out of the buffer, leaving
    /// `None` in its place.
    pub(crate) fn take_next_in_line_fetch(&self) -> Option<CompletedFetch> {
        let mut guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        guard.next_in_line_fetch.take()
    }

    /// Sets the next-in-line fetch. Pass `None` to clear it.
    ///
    /// If a previous next-in-line was set, it is `drain()`ed before the
    /// new value replaces it (mirrors Java's
    /// `FetchBuffer.close()`/`retainAll(...)` semantics, which drain the
    /// outgoing next-in-line so the `bytes_read > 0` →
    /// `move_partition_to_end` SubscriptionState nudge fires).
    ///
    /// Translates `void setNextInLineFetch(CompletedFetch)`.
    pub(crate) fn set_next_in_line_fetch(&self, fetch: Option<CompletedFetch>) {
        let mut guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        if let Some(prev) = guard.next_in_line_fetch.as_mut() {
            prev.drain();
        }
        guard.next_in_line_fetch = fetch;
    }

    /// Awaits until the buffer is woken up (data added or explicit wakeup)
    /// or `timeout` elapses.
    ///
    /// Translates `void awaitWakeup(Timer timer)`. Java throws
    /// `InterruptException` if the calling thread was interrupted; the
    /// Rust port instead relies on tokio task cancellation propagating
    /// through the caller's wakeup token (consumer-threading.md §11) —
    /// the buffer's only job is to surface the data-arrived signal.
    ///
    /// # Race-free registration
    ///
    /// `tokio::sync::Notify::notified()` only registers a waiter on the
    /// first poll (or via `enable()` on a pinned reference). To avoid
    /// losing a `notify_waiters` that arrives between our pre-check and
    /// the timeout's first poll, we:
    /// 1. Construct the `Notified` future,
    /// 2. Pin and `enable()` it — registers us as a waiter,
    /// 3. Re-check the woken flag (any add/wakeup between step 1 and
    ///    step 3 either set the flag, or fired `notify_waiters` which we
    ///    now hold a permit for),
    /// 4. Race the (already-registered) future against the timeout.
    pub(crate) async fn await_wakeup(&self, timeout: Duration) {
        // Check / clear the woken flag first — short-circuit if a
        // wakeup happened before we got here.
        if self.wokenup.swap(false, Ordering::SeqCst) {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        // Register as a waiter BEFORE the second flag check, so any
        // concurrent `notify_waiters` after our first check still
        // finds us registered.
        notified.as_mut().enable();
        if self.wokenup.swap(false, Ordering::SeqCst) {
            return;
        }
        // Race the (registered) notify against the timeout.
        let _ = tokio::time::timeout(timeout, notified).await;
        // Clear the woken flag (per Java's compareAndSet(true, false)
        // loop). It's fine if the timeout fired without a notification —
        // the flag stays false in that case.
        let _ = self.wokenup.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// Forces any thread waiting in [`Self::await_wakeup`] to return.
    ///
    /// Translates `void wakeup()`.
    pub(crate) fn wakeup(&self) {
        self.wokenup.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Drops every buffered fetch whose partition is not in the retain set.
    ///
    /// Translates `void retainAll(Set<TopicPartition>)`.
    pub(crate) fn retain_all(&self, partitions: &HashSet<TopicPartition>) {
        let mut guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        guard.completed_fetches.retain_mut(|cf| {
            if !partitions.contains(&cf.partition) {
                debug!(
                    "Removing {} from buffered fetch data as it is not in the set of partitions to retain",
                    cf.partition
                );
                cf.drain();
                false
            } else {
                true
            }
        });
        if let Some(next) = guard.next_in_line_fetch.as_mut()
            && !partitions.contains(&next.partition)
        {
            next.drain();
            guard.next_in_line_fetch = None;
        }
    }

    /// Returns the set of partitions for which we have data in the buffer,
    /// either in the queue or the next-in-line slot.
    ///
    /// Translates `Set<TopicPartition> bufferedPartitions()`.
    pub(crate) fn buffered_partitions(&self) -> HashSet<TopicPartition> {
        let guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
        let mut out: HashSet<TopicPartition> = HashSet::new();
        if let Some(next) = guard.next_in_line_fetch.as_ref()
            && !next.is_consumed()
        {
            out.insert(next.partition.clone());
        }
        for cf in &guard.completed_fetches {
            out.insert(cf.partition.clone());
        }
        out
    }

    /// Drops all buffered data and marks the buffer closed. Idempotent.
    ///
    /// Translates `void close()` (Java's `IdempotentCloser`).
    pub(crate) fn close(&self) {
        let already_closed = {
            let mut guard = self.inner.lock().expect("FetchBuffer mutex poisoned");
            if guard.closed {
                true
            } else {
                guard.closed = true;
                false
            }
        };
        if already_closed {
            return;
        }
        // Drain by retaining nothing.
        self.retain_all(&HashSet::new());
    }
}

impl Default for FetchBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FetchBuffer {
    fn drop(&mut self) {
        // Mirror Java's try-with-resources cleanup semantics.
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetch_response_data::PartitionData;
    use std::sync::Arc;
    use std::time::Instant;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    fn cf(topic: &str, partition: i32) -> CompletedFetch {
        CompletedFetch::new(tp(topic, partition), PartitionData::new())
    }

    /// Translated from `FetchBufferTest.testBasicPeekAndPoll`. The Rust
    /// port replaces Java's `peek`/`assertSame(reference)` with
    /// `has_completed_fetches(|_| true)` plus a `poll().unwrap()` that
    /// checks the popped value's partition.
    #[test]
    fn test_basic_peek_and_poll() {
        let buffer = FetchBuffer::new();
        assert!(buffer.is_empty());
        buffer.add(cf("topic-a", 0));
        assert!(buffer.has_completed_fetches(|_| true));
        assert!(!buffer.is_empty());
        let popped = buffer.poll().expect("popped");
        assert_eq!(tp("topic-a", 0), popped.partition);
        assert!(buffer.poll().is_none());
    }

    /// Translated from `FetchBufferTest.testCloseClearsData`.
    #[test]
    fn test_close_clears_data() {
        let buffer = FetchBuffer::new();
        assert!(!buffer.has_next_in_line_fetch());
        assert!(buffer.is_empty());

        buffer.add(cf("topic-a", 0));
        assert!(!buffer.is_empty());

        buffer.set_next_in_line_fetch(Some(cf("topic-a", 0)));
        assert!(buffer.has_next_in_line_fetch());

        buffer.close();
        assert!(!buffer.has_next_in_line_fetch());
        assert!(buffer.is_empty());
    }

    /// Translated from `FetchBufferTest.testBufferedPartitions`.
    #[test]
    fn test_buffered_partitions() {
        let buffer = FetchBuffer::new();
        buffer.set_next_in_line_fetch(Some(cf("topic-a", 0)));
        buffer.add(cf("topic-a", 1));
        buffer.add(cf("topic-a", 2));
        let mut expected: HashSet<TopicPartition> = HashSet::new();
        expected.insert(tp("topic-a", 0));
        expected.insert(tp("topic-a", 1));
        expected.insert(tp("topic-a", 2));
        assert_eq!(expected, buffer.buffered_partitions());

        buffer.set_next_in_line_fetch(None);
        let mut expected = HashSet::new();
        expected.insert(tp("topic-a", 1));
        expected.insert(tp("topic-a", 2));
        assert_eq!(expected, buffer.buffered_partitions());

        buffer.poll();
        let mut expected = HashSet::new();
        expected.insert(tp("topic-a", 2));
        assert_eq!(expected, buffer.buffered_partitions());

        buffer.poll();
        assert_eq!(HashSet::new(), buffer.buffered_partitions());
    }

    /// Translated from `FetchBufferTest.testAddAllAndRetainAll`.
    #[test]
    fn test_add_all_and_retain_all() {
        let buffer = FetchBuffer::new();
        buffer.set_next_in_line_fetch(Some(cf("topic-a", 0)));
        buffer.add_all([cf("topic-a", 1), cf("topic-a", 2)]);
        let mut all = HashSet::new();
        all.insert(tp("topic-a", 0));
        all.insert(tp("topic-a", 1));
        all.insert(tp("topic-a", 2));
        assert_eq!(all, buffer.buffered_partitions());

        let mut retain = HashSet::new();
        retain.insert(tp("topic-a", 1));
        retain.insert(tp("topic-a", 2));
        buffer.retain_all(&retain);
        assert_eq!(retain, buffer.buffered_partitions());

        let mut retain = HashSet::new();
        retain.insert(tp("topic-a", 2));
        buffer.retain_all(&retain);
        assert_eq!(retain, buffer.buffered_partitions());

        buffer.retain_all(&HashSet::new());
        assert_eq!(HashSet::new(), buffer.buffered_partitions());
    }

    /// Translated from `FetchBufferTest.testWakeup`. A separate tokio
    /// task awaits with a long timeout; the main task wakes it up.
    #[tokio::test(flavor = "current_thread")]
    async fn test_wakeup() {
        let buffer = Arc::new(FetchBuffer::new());
        let buffer_for_task = Arc::clone(&buffer);
        let task = tokio::spawn(async move {
            // Long timeout — should not be hit.
            buffer_for_task.await_wakeup(Duration::from_secs(60)).await;
        });
        // Give the task a beat to enter `await_wakeup`.
        tokio::task::yield_now().await;
        buffer.wakeup();
        // The task should complete promptly.
        let start = Instant::now();
        let join_result = tokio::time::timeout(Duration::from_secs(10), task).await;
        assert!(join_result.is_ok(), "wakeup did not release the awaiting task within 10s");
        // The elapsed wall-clock time is dominated by `start_paused` semantics
        // — we just assert the join succeeded.
        let _ = start;
    }

    /// `await_wakeup` returns promptly when the buffer was already woken.
    #[tokio::test(flavor = "current_thread")]
    async fn test_await_wakeup_returns_when_already_woken() {
        let buffer = FetchBuffer::new();
        // Adding data sets the woken flag.
        buffer.add(cf("topic-a", 0));
        let elapsed = {
            let start = Instant::now();
            buffer.await_wakeup(Duration::from_secs(30)).await;
            start.elapsed()
        };
        // Should not have slept the full 30s — the woken flag short-circuits.
        assert!(
            elapsed < Duration::from_secs(1),
            "await_wakeup did not short-circuit: {elapsed:?}"
        );
    }

    /// `await_wakeup` returns after `timeout` if no wakeup arrives.
    #[tokio::test(flavor = "current_thread")]
    async fn test_await_wakeup_times_out() {
        let buffer = FetchBuffer::new();
        let start = Instant::now();
        // Short timeout so the test doesn't drag.
        buffer.await_wakeup(Duration::from_millis(50)).await;
        let elapsed = start.elapsed();
        // Must have taken at least the timeout — slack for runtime
        // scheduling.
        assert!(
            elapsed >= Duration::from_millis(40),
            "await_wakeup returned too quickly: {elapsed:?}"
        );
        // And not absurdly longer.
        assert!(elapsed < Duration::from_secs(5), "await_wakeup hung: {elapsed:?}");
    }

    /// `close` is idempotent — calling twice does not panic.
    #[test]
    fn test_close_is_idempotent() {
        let buffer = FetchBuffer::new();
        buffer.add(cf("topic-a", 0));
        buffer.close();
        buffer.close(); // no panic
        assert!(buffer.is_empty());
    }

    /// `set_next_in_line_fetch(None)` drains the previous next-in-line
    /// (mirrors Java's `retainAll`/`close` semantics).
    #[test]
    fn test_set_next_in_line_fetch_drains_previous() {
        let buffer = FetchBuffer::new();
        let cf_a = cf("topic-a", 0);
        buffer.set_next_in_line_fetch(Some(cf_a));
        assert!(buffer.has_next_in_line_fetch());
        // Replace with None — previous must be drained (no panic + slot empty).
        buffer.set_next_in_line_fetch(None);
        assert!(!buffer.has_next_in_line_fetch());

        // Replace with Some, then Some again — previous drained.
        buffer.set_next_in_line_fetch(Some(cf("topic-a", 1)));
        buffer.set_next_in_line_fetch(Some(cf("topic-b", 0)));
        // The slot now holds topic-b; topic-a was drained.
        let partitions = buffer.buffered_partitions();
        assert!(partitions.contains(&tp("topic-b", 0)));
        assert!(!partitions.contains(&tp("topic-a", 1)));
    }

    /// `await_wakeup` does NOT lose a wakeup that races our flag check.
    /// Hammers the race window with a tokio task that wakes up after
    /// `yield_now`; the awaiter must observe the wakeup promptly.
    #[tokio::test(flavor = "current_thread")]
    async fn test_await_wakeup_does_not_lose_race() {
        let buffer = Arc::new(FetchBuffer::new());
        let buffer_for_task = Arc::clone(&buffer);
        let task = tokio::spawn(async move {
            // Long timeout — must not be hit.
            buffer_for_task.await_wakeup(Duration::from_secs(30)).await;
        });
        // Yield once so the task enters await_wakeup before we wake it.
        tokio::task::yield_now().await;
        buffer.wakeup();
        let join_result = tokio::time::timeout(Duration::from_secs(5), task).await;
        assert!(join_result.is_ok(), "wakeup race lost: awaiter did not return");
    }

    /// `has_completed_fetches` predicate filters correctly.
    #[test]
    fn test_has_completed_fetches_with_predicate() {
        let buffer = FetchBuffer::new();
        buffer.add(cf("topic-a", 0));
        buffer.add(cf("topic-b", 1));
        assert!(buffer.has_completed_fetches(|cf| cf.partition.topic() == "topic-a"));
        assert!(buffer.has_completed_fetches(|cf| cf.partition.topic() == "topic-b"));
        assert!(!buffer.has_completed_fetches(|cf| cf.partition.topic() == "topic-c"));
    }
}
