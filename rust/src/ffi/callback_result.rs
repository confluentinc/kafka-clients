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

//! The pending results behind a client's `__set_callback_result`
//! (CLAUDE.md §4, "Traits").
//!
//! A C interface method whose Rust counterpart is `async fn` returns `void`,
//! takes a trailing `int64_t callback_id` and reports through
//! `<Client>__set_callback_result(client, callback_id, void *result,
//! kafka_common_Error_t *error)`, from inside the method or later, from any
//! thread. Before invoking it, the client registers a [`ResultSink`] in its
//! [`CallbackResults`] and passes the id it returns; the report hands the
//! sink its outcome, the error first: a non-null `error` is the outcome and
//! `result` is ignored, otherwise `result` is the method's return value, which
//! the sink converts, on the reporting thread, to the type the method's
//! `_fn_t` documents.
//!
//! Each client handle holds its own registry, and a client can wait for the
//! reports of the callbacks it handed out before some point
//! ([`CallbackResults::mark`], [`CallbackResults::wait_reported`]).
//!
//! # Layout
//!
//! The registry may sit on a per-record path, so it is a slab: a `Vec` of
//! slots reused through a free list, behind one `std` mutex taken once per
//! [`register`](CallbackResults::register) and once per
//! [`complete`](CallbackResults::complete). Once the slab has grown, neither
//! allocates: the boxed sink is the caller's only allocation. An id packs the
//! slot index (bits 0-31) with the slot's generation (bits 32-61), bumped each
//! time the slot is freed, so a repeated, stale or unknown id is recognised
//! without a lookup table and ignored. Bit 62 is the registry's tag
//! ([`CallbackResults::new_tagged`]), so the ids of a tagged and an untagged
//! registry never collide; ids are always positive and never 0.
//!
//! For the waits, registrations are grouped in epochs, a new one starting at
//! each [`mark`](CallbackResults::mark), with a count of the sinks still
//! pending per epoch, from the oldest epoch that has one to the current one.
//! A registration or a report only changes a count, so the per-epoch counts
//! grow with the marks still awaited, not with the registrations.
//!
//! # Why this exists (DoD #7)
//!
//! `CallbackResults` has no Java counterpart: Java's interface methods return
//! their value, or throw, to the thread that called them, and the C
//! `callback_id` protocol that replaces that has none either. It is the one
//! registry behind every client's `__set_callback_result`, so no client keeps
//! a copy of its own (DoD #6). [`Mark`] is its snapshot token.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;

use crate::common::Error;

/// What a pending interface call does with its outcome: the method's return
/// value as the raw pointer C reported, or the reported error. It runs on
/// the reporting thread, once, and converts the pointer there, taking it
/// over as the method's `_fn_t` documents.
pub(crate) type ResultSink = Box<dyn FnOnce(Result<*mut c_void, Error>) + Send>;

/// The sink of a `void` method: the reported value is ignored, so `deliver`
/// receives `Ok(())` on success and the reported error otherwise.
pub(crate) fn void_sink(deliver: impl FnOnce(Result<(), Error>) + Send + 'static) -> ResultSink {
    Box::new(move |result| deliver(result.map(|_| ())))
}

/// Bits of an id holding the slot index.
const INDEX_BITS: u32 = 32;
/// The generation's mask, once shifted down by [`INDEX_BITS`]: 30 bits, so
/// the generation never reaches the tag bit nor the sign bit.
const GENERATION_MASK: u32 = (1 << 30) - 1;
/// The bit set on every id of a tagged registry.
const TAG_BIT: i64 = 1 << 62;

/// A point in a registry's registrations, from [`CallbackResults::mark`]:
/// the epoch the registrations after it fall in.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Mark(u64);

/// A registered sink, with its registration order and epoch.
struct Pending {
    seq: u64,
    epoch: u64,
    sink: ResultSink,
}

/// One slab entry: the generation an id of its current occupant carries.
struct Slot {
    generation: u32,
    pending: Option<Pending>,
}

/// What the registry's mutex guards.
struct Slab {
    slots: Vec<Slot>,
    /// Indices of the free slots, reused last-freed first.
    free: Vec<u32>,
    /// The registration order of the next registration.
    next_seq: u64,
    /// The epoch `outstanding[0]` counts.
    epoch_base: u64,
    /// The number of pending sinks of each epoch from `epoch_base` to the
    /// current one, the last entry. Never empty, and its first entry is not
    /// 0 unless it is the only one.
    outstanding: VecDeque<usize>,
}

impl Slab {
    /// The epoch new registrations fall in.
    fn current_epoch(&self) -> u64 {
        // `outstanding` is never empty, so the subtraction cannot underflow.
        self.epoch_base + self.outstanding.len() as u64 - 1
    }

    /// Drops the leading epochs with no pending sink, keeping the current one.
    fn trim(&mut self) {
        while self.outstanding.len() > 1 && self.outstanding[0] == 0 {
            self.outstanding.pop_front();
            self.epoch_base += 1;
        }
    }

    /// Takes the sink `id` names out of its slot and frees the slot; `None`
    /// for an id this registry did not issue or whose sink was taken.
    fn take(&mut self, id: i64, tag: i64) -> Option<ResultSink> {
        if id & TAG_BIT != tag {
            return None;
        }
        // Both fit by construction: the index is the low 32 bits and the generation the 30 above.
        let index = (id & i64::from(u32::MAX)) as u32;
        let generation = ((id >> INDEX_BITS) & i64::from(GENERATION_MASK)) as u32;
        let slot = self.slots.get_mut(index as usize)?;
        if slot.generation != generation {
            return None;
        }
        let pending = slot.pending.take()?;
        slot.generation = next_generation(slot.generation);
        self.free.push(index);
        // A pending sink's epoch is never trimmed (its count is not 0), so it is in range.
        self.outstanding[(pending.epoch - self.epoch_base) as usize] -= 1;
        self.trim();
        Some(pending.sink)
    }
}

/// The generation after `generation`, skipping 0 so no id is 0.
fn next_generation(generation: u32) -> u32 {
    if generation == GENERATION_MASK {
        1
    } else {
        generation + 1
    }
}

/// The sinks awaiting a report, by the id C reports with.
pub(crate) struct CallbackResults {
    /// [`TAG_BIT`] for a tagged registry, 0 otherwise.
    tag: i64,
    slab: Mutex<Slab>,
    /// Sinks taken by `complete` and still running, outside the mutex; a
    /// waiter waits for them too, so a report counts once its sink returned.
    in_sink: AtomicUsize,
    /// Tasks inside [`Self::wait_reported`]; `complete` and `drain` notify
    /// only when there is one.
    waiters: AtomicUsize,
    reported: Notify,
}

impl CallbackResults {
    /// An empty registry, whose ids have the tag bit clear.
    pub(crate) fn new() -> Self {
        Self::with_tag(0)
    }

    /// An empty registry whose ids have the tag bit set, so they can be
    /// told apart from those of a [`Self::new`] registry (see
    /// [`Self::is_tagged`]).
    pub(crate) fn new_tagged() -> Self {
        Self::with_tag(TAG_BIT)
    }

    fn with_tag(tag: i64) -> Self {
        let mut outstanding = VecDeque::new();
        outstanding.push_back(0);
        CallbackResults {
            tag,
            slab: Mutex::new(Slab { slots: Vec::new(), free: Vec::new(), next_seq: 0, epoch_base: 0, outstanding }),
            in_sink: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
            reported: Notify::new(),
        }
    }

    /// Whether `id` was issued by a [`Self::new_tagged`] registry.
    pub(crate) fn is_tagged(id: i64) -> bool {
        id & TAG_BIT != 0
    }

    /// The registry's state. No critical section can panic midway short of
    /// an allocation failure, so a poisoned lock is taken as is.
    fn lock(&self) -> MutexGuard<'_, Slab> {
        self.slab.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers `sink` and returns the id C reports with: positive, never
    /// 0, and unique within this registry while the sink is pending.
    pub(crate) fn register(&self, sink: ResultSink) -> i64 {
        let mut slab = self.lock();
        let seq = slab.next_seq;
        slab.next_seq += 1;
        let epoch = slab.current_epoch();
        if let Some(count) = slab.outstanding.back_mut() {
            *count += 1;
        }
        let index = match slab.free.pop() {
            Some(index) => index,
            None => {
                let index = u32::try_from(slab.slots.len()).expect("more than u32::MAX callbacks pending at once");
                slab.slots.push(Slot { generation: 1, pending: None });
                index
            },
        };
        let slot = &mut slab.slots[index as usize];
        slot.pending = Some(Pending { seq, epoch, sink });
        self.tag | (i64::from(slot.generation) << INDEX_BITS) | i64::from(index)
    }

    /// Delivers a report to the sink registered under `id`, running it on
    /// the calling thread outside the registry's lock, so the sink may
    /// register or report again. The error comes first: `Some(error)` is the
    /// outcome and `result` is not taken (it stays the caller's); `None`
    /// hands the sink `result`. Returns `false` for an id this registry did
    /// not issue, or whose report already arrived (or was drained): nothing
    /// is delivered, `error` is dropped and `result` is not taken.
    pub(crate) fn complete(&self, id: i64, result: *mut c_void, error: Option<Error>) -> bool {
        let sink = {
            let mut slab = self.lock();
            let sink = slab.take(id, self.tag);
            if sink.is_some() {
                // Counted inside the critical section that uncounts the pending sink, so a waiter
                // never sees the report as done while its sink still runs.
                self.in_sink.fetch_add(1, Ordering::SeqCst);
            }
            sink
        };
        let Some(sink) = sink else {
            return false;
        };
        let _running = SinkRunning(self);
        sink(error.map_or(Ok(result), Err));
        true
    }

    /// The point a later [`Self::wait_reported`] waits for: the callbacks
    /// registered from now on do not delay it.
    #[cfg_attr(not(test), expect(dead_code, reason = "no client waits for its reports yet"))]
    pub(crate) fn mark(&self) -> Mark {
        let mut slab = self.lock();
        slab.outstanding.push_back(0);
        slab.trim();
        Mark(slab.current_epoch())
    }

    /// Returns once every callback registered before `mark` was reported,
    /// and its sink returned, or was drained. Cancellation-safe: dropping
    /// the future leaves nothing behind.
    ///
    /// It also waits for the sinks of the reports arriving meanwhile,
    /// whenever they were registered, so reports whose sinks keep
    /// overlapping, from several threads, can hold it back past the last
    /// report it waits for; it returns once they stop overlapping.
    #[cfg_attr(not(test), expect(dead_code, reason = "no client waits for its reports yet"))]
    pub(crate) async fn wait_reported(&self, mark: Mark) {
        // Counted before the first check. With `SinkRunning`, which decrements `in_sink` before it
        // reads `waiters` (all `SeqCst`), either a check sees the sink finished or the sink's thread
        // sees this waiter and notifies it.
        let _waiting = Waiting::enter(&self.waiters);
        loop {
            // Enabled before the check, so a notification sent after the check is not lost.
            let notified = self.reported.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.reported_before(mark) {
                return;
            }
            notified.await;
        }
    }

    /// Whether no callback registered before `mark` is awaiting its report
    /// or running its sink.
    fn reported_before(&self, mark: Mark) -> bool {
        let slab = self.lock();
        let earlier = usize::try_from(mark.0.saturating_sub(slab.epoch_base)).unwrap_or(usize::MAX);
        slab.outstanding.iter().take(earlier).all(|&count| count == 0) && self.in_sink.load(Ordering::SeqCst) == 0
    }

    /// Takes every sink still awaiting a report, in registration order, for
    /// a client that completes them itself when it is destroyed; their ids
    /// are unknown afterwards, and the waiters are released.
    #[cfg_attr(not(test), expect(dead_code, reason = "no client drains its registry yet"))]
    pub(crate) fn drain(&self) -> Vec<ResultSink> {
        let mut drained = {
            let mut slab = self.lock();
            let current = slab.current_epoch();
            let Slab { slots, free, outstanding, epoch_base, .. } = &mut *slab;
            let mut drained = Vec::new();
            for (index, slot) in slots.iter_mut().enumerate() {
                if let Some(pending) = slot.pending.take() {
                    slot.generation = next_generation(slot.generation);
                    // Every index fits: `register` hands out `u32` indices only.
                    free.push(index as u32);
                    drained.push(pending);
                }
            }
            outstanding.clear();
            outstanding.push_back(0);
            *epoch_base = current;
            drained
        };
        if self.waiters.load(Ordering::SeqCst) > 0 {
            self.reported.notify_waiters();
        }
        drained.sort_unstable_by_key(|pending| pending.seq);
        drained.into_iter().map(|pending| pending.sink).collect()
    }
}

/// Counts a sink `complete` runs; on drop (also when the sink panics) it
/// wakes the waiters once the last running sink returned.
struct SinkRunning<'a>(&'a CallbackResults);

impl Drop for SinkRunning<'_> {
    fn drop(&mut self) {
        let results = self.0;
        if results.in_sink.fetch_sub(1, Ordering::SeqCst) == 1 && results.waiters.load(Ordering::SeqCst) > 0 {
            results.reported.notify_waiters();
        }
    }
}

/// Counts a task inside [`CallbackResults::wait_reported`] while it lives.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn enter(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::SeqCst);
        Self(waiters)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::{Pin, pin};
    use std::ptr;
    use std::sync::{Arc, mpsc};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use super::*;
    use crate::AllocTrackingGuard;
    use crate::ffi::common::{box_error, take_error};

    /// A sink taking the reported value over as a `Box<i32>`, as a method
    /// returning one would document.
    fn boxed_i32_sink(tx: mpsc::Sender<Result<i32, Error>>) -> ResultSink {
        Box::new(move |result| {
            // SAFETY: Test code: on success the value is the `Box::into_raw` of a `Box<i32>` the
            // test reported, taken over exactly once, here.
            let result = result.map(|value| *unsafe { Box::from_raw(value as *mut i32) });
            tx.send(result).unwrap();
        })
    }

    /// A `void` sink and the receiving end its outcome is sent to.
    fn channel_sink() -> (ResultSink, mpsc::Receiver<Result<(), Error>>) {
        let (tx, rx) = mpsc::channel();
        (void_sink(move |result| tx.send(result).unwrap()), rx)
    }

    /// Polls `future` once, with a waker that does nothing.
    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn value_sink_receives_and_frees_a_boxed_value() {
        let results = CallbackResults::new();
        let (tx, rx) = mpsc::channel();
        let id = results.register(boxed_i32_sink(tx));
        let value = Box::into_raw(Box::new(42_i32));
        assert!(results.complete(id, value as *mut c_void, None));
        assert_eq!(rx.recv().unwrap().unwrap(), 42);
    }

    #[test]
    fn with_both_set_the_error_wins_and_the_value_is_not_taken() {
        let results = CallbackResults::new();
        let (tx, rx) = mpsc::channel();
        let id = results.register(boxed_i32_sink(tx));
        let value = Box::into_raw(Box::new(7_i32));
        assert!(results.complete(id, value as *mut c_void, Some(Error::local_illegal_state("failed"))));
        assert_eq!(rx.recv().unwrap().unwrap_err().message(), "failed");
        // SAFETY: Test code: the sink did not take `value` (the error won), so it is still the live
        // `Box<i32>` this test made, freed exactly once here.
        assert_eq!(*unsafe { Box::from_raw(value) }, 7, "the value stays the caller's");
    }

    #[test]
    fn void_sink_ignores_the_value() {
        let results = CallbackResults::new();
        let (sink, rx) = channel_sink();
        let id = results.register(sink);
        let mut ignored = 0_u8;
        assert!(results.complete(id, ptr::addr_of_mut!(ignored) as *mut c_void, None));
        assert!(rx.recv().unwrap().is_ok());
        let (sink, rx) = channel_sink();
        let id = results.register(sink);
        assert!(results.complete(id, ptr::null_mut(), Some(Error::timeout("late"))));
        assert_eq!(rx.recv().unwrap().unwrap_err().message(), "late");
    }

    #[test]
    fn repeated_id_is_ignored_and_its_error_freed() {
        let results = CallbackResults::new();
        let (sink, rx) = channel_sink();
        let id = results.register(sink);
        assert!(results.complete(id, ptr::null_mut(), None));
        assert!(rx.recv().unwrap().is_ok());
        // The second report carries an owned C error handle, as `__set_callback_result` receives
        // it: `take_error` frees the handle and `complete`, taking the `Error` it yields by value,
        // drops it, so nothing of the report outlives the call.
        let error = box_error(Error::timeout("second"));
        // SAFETY: Test code: `error` is the owned handle `box_error` just made, taken over once.
        let error = unsafe { take_error(error) };
        assert!(!results.complete(id, ptr::null_mut(), error));
        assert!(rx.try_recv().is_err(), "a repeated report delivers nothing");
    }

    #[test]
    fn unknown_id_is_ignored() {
        let results = CallbackResults::new();
        assert!(!results.complete(1, ptr::null_mut(), None), "a slot never handed out");
        assert!(!results.complete(-1, ptr::null_mut(), Some(Error::timeout("unknown"))));
        let (sink, rx) = channel_sink();
        let id = results.register(sink);
        assert!(
            !results.complete(id | TAG_BIT, ptr::null_mut(), None),
            "the tagged twin of a live id"
        );
        let tagged = CallbackResults::new_tagged();
        let (tagged_sink, tagged_rx) = channel_sink();
        let tagged_id = tagged.register(tagged_sink);
        assert!(CallbackResults::is_tagged(tagged_id) && !CallbackResults::is_tagged(id));
        assert!(!results.complete(tagged_id, ptr::null_mut(), None), "another registry's id");
        assert!(!tagged.complete(id, ptr::null_mut(), None), "another registry's id");
        assert!(rx.try_recv().is_err() && tagged_rx.try_recv().is_err());
        assert!(results.complete(id, ptr::null_mut(), None));
        assert!(tagged.complete(tagged_id, ptr::null_mut(), None));
        assert!(rx.recv().unwrap().is_ok() && tagged_rx.recv().unwrap().is_ok());
    }

    #[test]
    fn stale_id_after_slot_reuse_is_ignored() {
        let results = CallbackResults::new();
        let (first, first_rx) = channel_sink();
        let stale = results.register(first);
        assert!(results.complete(stale, ptr::null_mut(), None));
        assert!(first_rx.recv().unwrap().is_ok());
        let (second, second_rx) = channel_sink();
        let reused = results.register(second);
        assert_eq!(reused & i64::from(u32::MAX), stale & i64::from(u32::MAX), "the slot is reused");
        assert_ne!(reused, stale, "under a new generation");
        assert!(!results.complete(stale, ptr::null_mut(), Some(Error::timeout("stale"))));
        assert!(second_rx.try_recv().is_err(), "the stale id does not reach the slot's new sink");
        assert!(results.complete(reused, ptr::null_mut(), None));
        assert!(second_rx.recv().unwrap().is_ok());
    }

    #[test]
    fn ids_are_positive_and_never_zero() {
        let results = CallbackResults::new();
        let tagged = CallbackResults::new_tagged();
        for _ in 0..3 {
            for registry in [&results, &tagged] {
                let id = registry.register(Box::new(|_| {}));
                assert!(id > 0, "{id}");
                assert!(registry.complete(id, ptr::null_mut(), None));
            }
        }
        assert_eq!(next_generation(GENERATION_MASK), 1, "the generation wraps past 0");
    }

    #[test]
    fn register_and_complete_do_not_allocate_once_warm() {
        let results = CallbackResults::new();
        // Grow the slab and its free list past what the loop needs, across a mark.
        let warm: Vec<i64> = (0..4).map(|_| results.register(Box::new(|_| {}))).collect();
        results.mark();
        for id in warm {
            assert!(results.complete(id, ptr::null_mut(), None));
        }
        let count = {
            let _tracking = AllocTrackingGuard::new();
            for _ in 0..1_000 {
                // A closure capturing nothing is zero-sized, so its `Box` allocates nothing: every
                // allocation counted here would be the registry's own.
                let a = results.register(Box::new(|_| {}));
                let b = results.register(Box::new(|_| {}));
                assert!(results.complete(a, ptr::null_mut(), None));
                assert!(results.complete(b, ptr::null_mut(), None));
            }
            AllocTrackingGuard::count()
        };
        assert_eq!(count, 0, "register + complete allocated beyond the sink's box");
    }

    #[test]
    fn a_later_registration_does_not_block_a_waiter() {
        let results = CallbackResults::new();
        let mark = results.mark();
        let (sink, _rx) = channel_sink();
        let later = results.register(sink);
        let mut wait = pin!(results.wait_reported(mark));
        assert!(poll_once(wait.as_mut()).is_ready());
        assert!(results.complete(later, ptr::null_mut(), None));
    }

    #[test]
    fn an_earlier_registration_blocks_a_waiter_until_reported() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let results = Arc::new(CallbackResults::new());
        let (sink, rx) = channel_sink();
        let earlier = results.register(sink);
        let mark = results.mark();
        let (sink, _later_rx) = channel_sink();
        let _later = results.register(sink);
        {
            let mut wait = pin!(results.wait_reported(mark));
            assert!(poll_once(wait.as_mut()).is_pending(), "the earlier callback is pending");
        }
        assert_eq!(
            results.waiters.load(Ordering::SeqCst),
            0,
            "a dropped wait leaves nothing behind"
        );
        let reporter = {
            let results = Arc::clone(&results);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                assert!(results.complete(earlier, ptr::null_mut(), None));
            })
        };
        runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(10), results.wait_reported(mark)).await })
            .expect("the report releases the waiter");
        assert!(rx.try_recv().unwrap().is_ok(), "the sink ran before the waiter returned");
        reporter.join().unwrap();
    }

    #[test]
    fn a_waiter_waits_for_the_sink_to_return() {
        let results = Arc::new(CallbackResults::new());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let id = results.register(Box::new(move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }));
        let mark = results.mark();
        let reporter = {
            let results = Arc::clone(&results);
            std::thread::spawn(move || assert!(results.complete(id, ptr::null_mut(), None)))
        };
        entered_rx.recv().unwrap();
        let mut wait = pin!(results.wait_reported(mark));
        assert!(
            poll_once(wait.as_mut()).is_pending(),
            "the report arrived but its sink still runs"
        );
        release_tx.send(()).unwrap();
        reporter.join().unwrap();
        assert!(poll_once(wait.as_mut()).is_ready());
    }

    #[test]
    fn drain_takes_every_pending_sink_in_order_and_releases_waiters() {
        let results = CallbackResults::new();
        let (tx, rx) = mpsc::channel();
        let register = |n: i32| {
            let tx = tx.clone();
            results.register(Box::new(move |_| tx.send(n).unwrap()))
        };
        let ids: Vec<i64> = (0..3).map(register).collect();
        // Free the middle slot and fill it again, so slot order differs from registration order.
        assert!(results.complete(ids[1], ptr::null_mut(), None));
        assert_eq!(rx.recv().unwrap(), 1);
        register(3);
        let mark = results.mark();
        let mut wait = pin!(results.wait_reported(mark));
        assert!(poll_once(wait.as_mut()).is_pending());
        let sinks = results.drain();
        assert!(poll_once(wait.as_mut()).is_ready(), "drain releases the waiter");
        assert!(!results.complete(ids[0], ptr::null_mut(), None), "drained ids are unknown");
        for sink in sinks {
            sink(Ok(ptr::null_mut()));
        }
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), [0, 2, 3]);
        // The registry stays usable, waits included.
        let id = register(4);
        let mark = results.mark();
        let mut wait = pin!(results.wait_reported(mark));
        assert!(poll_once(wait.as_mut()).is_pending());
        assert!(results.complete(id, ptr::null_mut(), None));
        assert!(poll_once(wait.as_mut()).is_ready());
    }
}
