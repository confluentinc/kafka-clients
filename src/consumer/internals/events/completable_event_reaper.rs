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

//! `CompletableEventReaper`.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper`.
//! Tracks [`super::completable_event::CompletableEventErasedHandle`]s and
//! expires any whose deadline has passed, by failing their inner oneshot
//! sender with [`Error::timeout`].
//!
//! Java uses `List<CompletableEvent<?>>` with explicit iterator-remove;
//! Rust uses `Vec<Arc<dyn CompletableEventErasedHandle>>` with
//! [`Vec::retain`]. Identity comparison goes via
//! [`super::completable_event::CompletableEventErasedHandle::inner_id`]
//! (the stable pointer to the underlying `HandleInner`) to preserve
//! Java's `List.contains(event)` reference-equality semantics across
//! repeated `CompletableEventHandle::erased()` calls.

use std::sync::Arc;

use log::{debug, trace};

use crate::common::Error;

use super::completable_event::CompletableEventErasedHandle;

/// Tracks [`CompletableEventErasedHandle`]s and expires deadline-exceeded
/// events.
///
/// Owned by the consumer background task (Phase 10). Inserted into and
/// queried in single-threaded fashion — no internal locking.
pub(crate) struct CompletableEventReaper {
    tracked: Vec<Arc<dyn CompletableEventErasedHandle>>,
}

impl Default for CompletableEventReaper {
    fn default() -> Self {
        Self::new()
    }
}

impl CompletableEventReaper {
    /// Creates an empty reaper.
    pub(crate) fn new() -> Self {
        Self { tracked: Vec::new() }
    }

    /// Java: `add(CompletableEvent<?> event)`. Starts tracking the
    /// supplied handle. Must be called when the corresponding event is
    /// posted to the bg task so the deadline is enforced even if the
    /// bg-task path does not get to it.
    pub(crate) fn add(&mut self, handle: Arc<dyn CompletableEventErasedHandle>) {
        self.tracked.push(handle);
    }

    /// Java: `reap(long currentTimeMs) -> long`. Two-step process:
    ///
    ///   1. For every tracked event whose deadline has passed, fail its
    ///      sender with [`Error::timeout`].
    ///   2. Remove all events whose sender slot has already been consumed
    ///      (either via the call above, via normal completion on the bg
    ///      task, or via cancellation).
    ///
    /// Returns the number of events that were expired (not the total
    /// removed — events that completed normally are removed but not
    /// counted as expired, matching Java).
    pub(crate) fn reap(&mut self, current_time_ms: i64) -> u64 {
        let mut expired_count: u64 = 0;

        // Mirrors Java's `Iterator.remove()` loop: walk the list, expire
        // deadline-exceeded entries, then drop everything that's done.
        self.tracked.retain(|handle| {
            if handle.is_done() {
                // Completed (normally, exceptionally, or via a prior reap)
                // — stop tracking, do not count as expired.
                return false;
            }

            let deadline = handle.deadline_ms();
            let past_due = current_time_ms.saturating_sub(deadline);

            if past_due < 0 {
                // Still within deadline — keep tracking.
                return true;
            }

            // Past due: count it BEFORE attempting expiration. Java's
            // `reap(currentTimeMs)` (CompletableEventReaper.java:115)
            // increments unconditionally once it has seen the event is
            // past-due — the count means "events that were not done at
            // observation time", regardless of who wins a concurrent
            // race against `completeExceptionally`. Counting on
            // `fail_with_timeout` success would diverge from Java when
            // another task completes the handle between our `is_done`
            // check and the call.
            expired_count += 1;

            let error = Error::timeout(format!(
                "{} was {} ms past its expiration of {}",
                handle.type_name(),
                past_due,
                deadline
            ));

            if handle.fail_with_timeout(error) {
                debug!(
                    "Event {} completed exceptionally since its expiration of {} passed {} ms ago",
                    handle.type_name(),
                    deadline,
                    past_due
                );
            } else {
                trace!(
                    "Event {} not completed exceptionally since it was previously completed",
                    handle.type_name()
                );
            }

            // Drop the entry whether we expired it just now or someone
            // else completed it concurrently.
            false
        });

        expired_count
    }

    /// Java: `reap(Collection<?> events) -> long` — see
    /// `CompletableEventReaper.java:143`.
    ///
    /// Called during consumer close. Expires both the tracked list AND
    /// any handles passed in (typically drained from the application
    /// event queue). Does NOT consider deadlines — closes everything.
    ///
    /// **Side effect**: like Java's `events.clear()` (line 150), this
    /// CLEARS `unprocessed_events` after iteration so the caller does
    /// not need to drain it separately. Returns the total number of
    /// events that were expired (i.e. that were still not done at the
    /// time of the call).
    pub(crate) fn reap_on_close(&mut self, unprocessed_events: &mut Vec<Arc<dyn CompletableEventErasedHandle>>) -> u64 {
        let tracked_expired = complete_events_with_error_on_close(self.tracked.iter());
        self.tracked.clear();

        let extra_expired = complete_events_with_error_on_close(unprocessed_events.iter());
        unprocessed_events.clear();

        tracked_expired + extra_expired
    }

    /// Number of currently-tracked handles.
    pub(crate) fn size(&self) -> usize {
        self.tracked.len()
    }

    /// Java: `contains(CompletableEvent<?> event)` — Java relies on
    /// object identity on the `CompletableEvent` reference.
    ///
    /// Rust uses
    /// [`CompletableEventErasedHandle::inner_id`] for the comparison
    /// because [`CompletableEventHandle::erased`] returns a *fresh*
    /// `Arc<dyn ...>` per call. `inner_id()` returns the stable pointer
    /// to the underlying `HandleInner<T>`, which is invariant across
    /// every `erased()` call (and across `Arc::clone` of the resulting
    /// trait object), so this `contains` works whether the caller saved
    /// the original `Arc` or re-called `handle.erased()` to query.
    pub(crate) fn contains(&self, handle: &Arc<dyn CompletableEventErasedHandle>) -> bool {
        let target = handle.inner_id();
        self.tracked.iter().any(|h| h.inner_id() == target)
    }

    /// Java: `uncompletedEvents()` — returns the subset of tracked
    /// handles whose senders have not yet been consumed.
    pub(crate) fn uncompleted_events(&self) -> Vec<Arc<dyn CompletableEventErasedHandle>> {
        self.tracked.iter().filter(|h| !h.is_done()).cloned().collect()
    }
}

/// Java: `completeEventsExceptionallyOnClose(Collection<?> events)`
/// (see `CompletableEventReaper.java:186-209`).
///
/// For each handle in the iterator, if it isn't already done, increment
/// the count BEFORE attempting expiration. Java counts the event as soon
/// as it observes it not-done, regardless of whether another task wins
/// the race to actually complete the slot.
fn complete_events_with_error_on_close<'a, I>(handles: I) -> u64
where
    I: IntoIterator<Item = &'a Arc<dyn CompletableEventErasedHandle>>,
{
    let mut count: u64 = 0;
    for handle in handles {
        if handle.is_done() {
            continue;
        }

        // Java increments `count` here, *before* the
        // `completeExceptionally` call. Counting on
        // `fail_with_timeout` success would diverge from Java when
        // another task completes the handle between the `is_done`
        // check above and our completion attempt.
        count += 1;

        let error = Error::timeout(format!(
            "{} could not be completed before the consumer closed",
            handle.type_name()
        ));

        if handle.fail_with_timeout(error) {
            debug!(
                "Event {} completed exceptionally since the consumer is closing",
                handle.type_name()
            );
        } else {
            trace!(
                "Event {} not completed exceptionally since it was completed prior to the consumer closing",
                handle.type_name()
            );
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::super::completable_event::make_completable_event;
    use super::*;

    #[test]
    fn empty_reaper_reaps_zero() {
        let mut reaper = CompletableEventReaper::new();
        assert_eq!(reaper.size(), 0);
        assert_eq!(reaper.reap(1_000), 0);
    }

    #[test]
    fn expired_event_is_failed_and_removed() {
        let mut reaper = CompletableEventReaper::new();
        let (_handle, mut rx, erased) = make_completable_event::<()>(100);
        reaper.add(erased);

        // Not yet past deadline.
        assert_eq!(reaper.reap(50), 0);
        assert_eq!(reaper.size(), 1);

        // Past deadline.
        assert_eq!(reaper.reap(200), 1);
        assert_eq!(reaper.size(), 0);

        let received = rx.try_recv().expect("sender used");
        assert!(matches!(received, Err(Error::Timeout(_))), "got: {:?}", received);
    }

    #[test]
    fn completed_event_is_removed_but_not_counted_as_expired() {
        let mut reaper = CompletableEventReaper::new();
        let (handle, mut rx, erased) = make_completable_event::<()>(100);
        reaper.add(erased);

        // Complete it normally.
        assert!(handle.complete(()));

        // Past deadline, but the event is already done.
        assert_eq!(reaper.reap(200), 0);
        assert_eq!(reaper.size(), 0);

        // Java `testCompleted` (CompletableEventReaperTest.java:96) asserts
        // the successful value survives the reap untouched. `Error`
        // does not derive `PartialEq`, so we pattern-match instead of
        // `assert_eq!`-ing against `Ok(())`.
        assert!(matches!(rx.try_recv().expect("sender used"), Ok(())));
    }

    #[test]
    fn reap_on_close_expires_tracked_and_extra() {
        let mut reaper = CompletableEventReaper::new();
        let (_h1, mut rx1, erased1) = make_completable_event::<()>(1_000);
        reaper.add(erased1);

        let (_h2, mut rx2, erased2) = make_completable_event::<()>(1_000);
        // erased2 is NOT added to the reaper — only passed in.
        let mut extras = vec![erased2];

        assert_eq!(reaper.reap_on_close(&mut extras), 2);
        assert_eq!(reaper.size(), 0);
        // Java: `events.clear()` (CompletableEventReaper.java:150).
        assert!(extras.is_empty(), "reap_on_close must clear the supplied collection");

        assert!(matches!(rx1.try_recv().unwrap(), Err(Error::Timeout(_))));
        assert!(matches!(rx2.try_recv().unwrap(), Err(Error::Timeout(_))));
    }

    #[test]
    fn contains_uses_ptr_eq() {
        let mut reaper = CompletableEventReaper::new();
        let (_h, _rx, erased) = make_completable_event::<()>(0);
        reaper.add(Arc::clone(&erased));
        assert!(reaper.contains(&erased));

        let (_h2, _rx2, erased2) = make_completable_event::<()>(0);
        assert!(!reaper.contains(&erased2));
    }

    /// Regression for COMMENTS.1.md #11: `handle.erased()` produces a
    /// *new* `Arc<dyn CompletableEventErasedHandle>` per call, but the
    /// reaper's `contains` MUST still recognise it because the
    /// underlying `HandleInner` is the same. Otherwise Phase-10 code
    /// that registers an erased clone with the reaper and later calls
    /// `handle.erased()` to query would see a spurious `false`.
    #[test]
    fn contains_works_across_erased_recreation() {
        let mut reaper = CompletableEventReaper::new();
        let (handle, _rx) = super::super::completable_event::CompletableEventHandle::<()>::new(0);
        let erased_a = handle.erased();
        reaper.add(erased_a);

        // Recreate a *different* `Arc<dyn ...>` from the same handle —
        // `Arc::ptr_eq` between erased_a (already moved into reaper) and
        // erased_b would be `false`, but inner_id() lines up.
        let erased_b = handle.erased();
        assert!(reaper.contains(&erased_b), "contains must match across erased() recreation");
    }

    /// Java `testCompletedAndExpired` — one event completes normally and
    /// the other expires; both should leave the tracked list, but only
    /// the second is counted as expired.
    #[test]
    fn completed_and_expired() {
        let mut reaper = CompletableEventReaper::new();
        let (h1, _rx1, erased1) = make_completable_event::<()>(100);
        let (_h2, mut rx2, erased2) = make_completable_event::<()>(100);
        reaper.add(erased1);
        reaper.add(erased2);

        // Without any time passing, neither event is done.
        assert_eq!(reaper.reap(50), 0);
        assert_eq!(reaper.size(), 2);

        // Complete event1 normally.
        assert!(h1.complete(()));

        // Past deadline.
        let expired = reaper.reap(200);
        assert_eq!(expired, 1, "only event2 was expired");
        assert_eq!(reaper.size(), 0);

        // Event2's receiver sees a timeout error.
        assert!(matches!(rx2.try_recv().unwrap(), Err(Error::Timeout(_))));
    }

    /// Java `testIncompleteQueue` — events tracked only in the supplied
    /// queue (NOT added to the reaper) are also expired by
    /// `reap_on_close`, while events already completed in the queue are
    /// not re-completed.
    #[test]
    fn reap_on_close_handles_queue_only_events() {
        let mut reaper = CompletableEventReaper::new();
        let (h1, mut rx1, erased1) = make_completable_event::<()>(1_000);
        let (_h2, mut rx2, erased2) = make_completable_event::<()>(1_000);

        // Complete h1 in advance.
        assert!(h1.complete(()));

        // erased1, erased2 simulate the channel-drained queue.
        let mut queue = vec![erased1, erased2];

        assert_eq!(reaper.size(), 0);
        assert_eq!(queue.len(), 2);

        // Only the incomplete (event2) should be counted as expired.
        assert_eq!(reaper.reap_on_close(&mut queue), 1);
        assert!(queue.is_empty(), "reap_on_close must clear the supplied collection");

        // Event1 still carries its `Ok(())` result.
        assert!(matches!(rx1.try_recv().unwrap(), Ok(())));
        // Event2 was timed out.
        assert!(matches!(rx2.try_recv().unwrap(), Err(Error::Timeout(_))));

        assert_eq!(reaper.size(), 0);
    }

    /// Java `testIncompleteTracked` — events tracked exclusively in the
    /// reaper (not the queue) are still closed via `reap_on_close` with
    /// an empty queue argument.
    #[test]
    fn reap_on_close_handles_tracked_only_events() {
        let mut reaper = CompletableEventReaper::new();
        let (h1, mut rx1, erased1) = make_completable_event::<()>(1_000);
        let (_h2, mut rx2, erased2) = make_completable_event::<()>(1_000);
        reaper.add(erased1);
        reaper.add(erased2);

        assert!(h1.complete(()));

        let mut queue: Vec<Arc<dyn CompletableEventErasedHandle>> = Vec::new();
        // event1 is complete; only event2 is expired.
        assert_eq!(reaper.reap_on_close(&mut queue), 1);
        assert_eq!(reaper.size(), 0);
        assert!(queue.is_empty());

        assert!(matches!(rx1.try_recv().unwrap(), Ok(())));
        assert!(matches!(rx2.try_recv().unwrap(), Err(Error::Timeout(_))));
    }

    #[test]
    fn uncompleted_events_filters_done() {
        let mut reaper = CompletableEventReaper::new();
        let (h1, _rx1, erased1) = make_completable_event::<()>(0);
        let (_h2, _rx2, erased2) = make_completable_event::<()>(0);
        reaper.add(erased1);
        reaper.add(erased2);

        assert_eq!(reaper.uncompleted_events().len(), 2);
        h1.complete(());
        assert_eq!(reaper.uncompleted_events().len(), 1);
    }
}
