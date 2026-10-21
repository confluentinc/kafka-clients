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
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

use tokio::sync::Notify;

use crate::common::Error;
use crate::ffi::callback_queue::SendPtr;

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

/// The allocation that holds a registry, which a waiting registration whose
/// future was dropped keeps alive until its report
/// ([`CallbackResults::set_owner`]): a serde handle, the pointer C reports
/// on.
pub(crate) type Owner = Arc<dyn Send + Sync>;

/// What a waiting registration ([`CallbackResults::register_waiting`]) keeps
/// alive for the C implementation until it reports: the arguments it was
/// handed pointers into. Each slot owns one, boxed once, when the slot first
/// waits, and reused by every later call through the slot, so its address is
/// stable while the slab grows and its buffers keep their capacity.
pub(crate) trait KeepAlive: Default + Send + 'static {
    /// Releases what the last call kept alive, keeping the capacity of any
    /// buffer for the next one.
    fn clear(&mut self);
}

impl KeepAlive for () {
    fn clear(&mut self) {}
}

/// Reads a successful report's `result` into the slot's [`KeepAlive`] on
/// the reporting thread, while the pointer is still valid (a serializer's
/// `kafka_Bytes_t *`); the waiting future then takes it from there.
pub(crate) type Convert<K> = fn(&mut K, *mut c_void);

/// The pending calls of the standalone invokers, which invoke a C
/// implementation with no client of their own:
/// `kafka_consumer_ConsumerRebalanceListener_on_partitions_*`,
/// `kafka_consumer_OffsetCommitCallback_on_complete*` and
/// `kafka_producer_Callback_on_completion*`, with their `_cb` forms.
///
/// The one process-wide registry, against the rule that each client holds
/// its own: the C implementation reports through its client's
/// `__set_callback_result` whatever client it was written for, or `NULL`,
/// since it cannot tell an invoker's call from a client's. So this registry
/// is tagged ([`CallbackResults::new_tagged`]) and a report routes its id by
/// the tag, never through the client handle.
pub(crate) static INVOKER_RESULTS: LazyLock<CallbackResults> = LazyLock::new(CallbackResults::new_tagged);

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

/// A waiting registration: the future polling it, the outcome once
/// reported, and whether the future was dropped first.
struct WaitingSlot<K> {
    waker: Option<Waker>,
    outcome: Option<Result<SendPtr, Error>>,
    /// The future was dropped before the report: the report frees the slot.
    abandoned: bool,
    /// The registry's owner, held from the moment the future was dropped
    /// until the report, so the handle C reports on outlives what dropped
    /// the future (a client, or the runtime of a failed constructor).
    kept: Option<Owner>,
    convert: Convert<K>,
}

/// What occupies a slot.
enum Entry<K> {
    /// A [`ResultSink`], run by the report.
    Sink(Pending),
    /// A future waiting for the report.
    Waiting(WaitingSlot<K>),
}

/// One slab entry: the generation an id of its current occupant carries.
struct Slot<K> {
    generation: u32,
    entry: Option<Entry<K>>,
    /// The arguments a waiting registration keeps alive, boxed the first
    /// time this slot waits and reused afterwards.
    keep_alive: Option<Box<K>>,
}

/// What the registry's mutex guards.
struct Slab<K> {
    slots: Vec<Slot<K>>,
    /// Indices of the free slots, reused last-freed first.
    free: Vec<u32>,
    /// The registration order of the next registration.
    next_seq: u64,
    /// The epoch `outstanding[0]` counts.
    epoch_base: u64,
    /// The number of pending sinks of each epoch from `epoch_base` to the
    /// current one, the last entry. Never empty, and its first entry is not
    /// 0 unless it is the only one. Waiting registrations are not counted:
    /// their future waits for them.
    outstanding: VecDeque<usize>,
}

impl<K: KeepAlive> Slab<K> {
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

    /// A free slot's index, growing the slab when none is free.
    fn allocate(&mut self) -> u32 {
        match self.free.pop() {
            Some(index) => index,
            None => {
                let index = u32::try_from(self.slots.len()).expect("more than u32::MAX callbacks pending at once");
                self.slots.push(Slot { generation: 1, entry: None, keep_alive: None });
                index
            },
        }
    }

    /// The id of the occupant of slot `index`.
    fn id(&self, index: u32, tag: i64) -> i64 {
        tag | (i64::from(self.slots[index as usize].generation) << INDEX_BITS) | i64::from(index)
    }

    /// The index of the occupied slot `id` names; `None` for an id this
    /// registry did not issue or whose slot was freed since.
    fn find(&self, id: i64, tag: i64) -> Option<usize> {
        if id & TAG_BIT != tag {
            return None;
        }
        // Both fit by construction: the index is the low 32 bits and the generation the 30 above.
        let index = (id & i64::from(u32::MAX)) as u32 as usize;
        let generation = ((id >> INDEX_BITS) & i64::from(GENERATION_MASK)) as u32;
        let slot = self.slots.get(index)?;
        (slot.generation == generation && slot.entry.is_some()).then_some(index)
    }

    /// Frees slot `index`: its occupant's ids are unknown afterwards, and
    /// what it kept alive is released.
    fn release(&mut self, index: usize) {
        let slot = &mut self.slots[index];
        slot.entry = None;
        slot.generation = next_generation(slot.generation);
        if let Some(keep_alive) = &mut slot.keep_alive {
            keep_alive.clear();
        }
        // Every index fits: `allocate` hands out `u32` indices only.
        self.free.push(index as u32);
    }

    /// Takes the sink `id` names out of its slot and frees the slot; `None`
    /// for an id this registry did not issue, whose sink was taken, or that
    /// names a waiting registration.
    fn take(&mut self, id: i64, tag: i64) -> Option<ResultSink> {
        let index = self.find(id, tag)?;
        if !matches!(self.slots[index].entry, Some(Entry::Sink(_))) {
            return None;
        }
        let Some(Entry::Sink(pending)) = self.slots[index].entry.take() else {
            return None;
        };
        self.release(index);
        // A pending sink's epoch is never trimmed (its count is not 0), so it is in range.
        self.outstanding[(pending.epoch - self.epoch_base) as usize] -= 1;
        self.trim();
        Some(pending.sink)
    }

    /// The waiting registration in slot `index`, if that is what occupies it.
    fn waiting(&mut self, index: usize) -> Option<(&mut WaitingSlot<K>, &mut K)> {
        let slot = &mut self.slots[index];
        match (&mut slot.entry, &mut slot.keep_alive) {
            (Some(Entry::Waiting(waiting)), Some(keep_alive)) => Some((waiting, keep_alive)),
            _ => None,
        }
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

/// The calls awaiting a report, by the id C reports with: boxed
/// [`ResultSink`]s, and waiting registrations whose [`KeepAlive`] `K` holds
/// what the C implementation reads until it reports.
pub(crate) struct CallbackResults<K = ()> {
    /// [`TAG_BIT`] for a tagged registry, 0 otherwise.
    tag: i64,
    slab: Mutex<Slab<K>>,
    /// Sinks taken by `complete` and still running, outside the mutex; a
    /// waiter waits for them too, so a report counts once its sink returned.
    in_sink: AtomicUsize,
    /// Tasks inside [`Self::wait_reported`]; `complete` and `drain` notify
    /// only when there is one.
    waiters: AtomicUsize,
    reported: Notify,
    /// The allocation holding this registry, if it was set
    /// ([`Self::set_owner`]).
    owner: OnceLock<Weak<dyn Send + Sync>>,
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

    /// Whether `id` was issued by a [`Self::new_tagged`] registry.
    pub(crate) fn is_tagged(id: i64) -> bool {
        id & TAG_BIT != 0
    }
}

impl<K: KeepAlive> CallbackResults<K> {
    /// An empty untagged registry whose waiting registrations keep a `K`
    /// alive.
    pub(crate) fn with_keep_alive() -> Self {
        Self::with_tag(0)
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
            owner: OnceLock::new(),
        }
    }

    /// Sets the allocation that holds this registry: a waiting registration
    /// whose future is dropped before its report keeps a strong reference to
    /// it until the report arrives, so the report never reaches a freed
    /// handle, whoever dropped the future. A registration that is never
    /// reported then leaks the owner, as the C implementation still holds an
    /// id for it. Set once; a later call is ignored. Such a registry is
    /// reported through [`Self::complete_owned`].
    pub(crate) fn set_owner(&self, owner: Weak<dyn Send + Sync>) {
        let _ = self.owner.set(owner);
    }

    /// The registry's state. No critical section can panic midway short of
    /// an allocation failure, so a poisoned lock is taken as is.
    fn lock(&self) -> MutexGuard<'_, Slab<K>> {
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
        let index = slab.allocate();
        slab.slots[index as usize].entry = Some(Entry::Sink(Pending { seq, epoch, sink }));
        slab.id(index, self.tag)
    }

    /// Registers a call whose report a [`WaitReport`] future waits for, and
    /// returns its id with what `fill` returns. `fill` stores in the slot's
    /// [`KeepAlive`] what the C implementation will read, typically returning
    /// the pointers into it to pass: they stay valid until the report, even
    /// when the future is dropped first. `convert` reads a successful
    /// report's `result` on the reporting thread.
    ///
    /// Once the slab has grown, this allocates nothing beyond what `fill`
    /// does: the slot and its `K` are reused.
    pub(crate) fn register_waiting<R>(&self, convert: Convert<K>, fill: impl FnOnce(&mut K) -> R) -> (i64, R) {
        let mut slab = self.lock();
        let index = slab.allocate();
        let slot = &mut slab.slots[index as usize];
        let filled = fill(slot.keep_alive.get_or_insert_with(Box::default));
        slot.entry = Some(Entry::Waiting(WaitingSlot {
            waker: None,
            outcome: None,
            abandoned: false,
            kept: None,
            convert,
        }));
        (slab.id(index, self.tag), filled)
    }

    /// Delivers a report to the call registered under `id`. The error comes
    /// first: `Some(error)` is the outcome and `result` is not taken (it
    /// stays the caller's); `None` hands the call `result`.
    ///
    /// A sink runs on the calling thread outside the registry's lock, so it
    /// may register or report again. A waiting registration converts
    /// `result` on the calling thread, stores the outcome and wakes its
    /// future; when the future was dropped first, the report frees the slot
    /// and what it kept alive, and the outcome is dropped.
    ///
    /// Returns `false` for an id this registry did not issue, or whose report
    /// already arrived (or was drained): nothing is delivered, `error` is
    /// dropped and `result` is not taken.
    ///
    /// For a registry with no owner ([`Self::set_owner`]); one with an owner
    /// is reported through [`Self::complete_owned`].
    pub(crate) fn complete(&self, id: i64, result: *mut c_void, error: Option<Error>) -> bool {
        let (delivered, kept) = self.complete_owned(id, result, error);
        // Never set without an owner, which is the only case this is for.
        debug_assert!(kept.is_none(), "a registry with an owner is reported through complete_owned");
        drop(kept);
        delivered
    }

    /// [`Self::complete`] for a registry with an owner: also returns the
    /// owner reference that an abandoned registration kept until this report.
    /// The caller drops it after its last use of the registry, since it may
    /// be the last reference to the allocation holding the registry.
    pub(crate) fn complete_owned(&self, id: i64, result: *mut c_void, error: Option<Error>) -> (bool, Option<Owner>) {
        let mut slab = self.lock();
        let Some(index) = slab.find(id, self.tag) else {
            return (false, None);
        };
        if let Some((waiting, keep_alive)) = slab.waiting(index) {
            if waiting.outcome.is_some() {
                // A second report.
                return (false, None);
            }
            if waiting.abandoned {
                let kept = waiting.kept.take();
                slab.release(index);
                return (true, kept);
            }
            let outcome = match error {
                Some(error) => Err(error),
                None => {
                    (waiting.convert)(keep_alive, result);
                    Ok(SendPtr(result))
                },
            };
            waiting.outcome = Some(outcome);
            let waker = waiting.waker.take();
            drop(slab);
            if let Some(waker) = waker {
                waker.wake();
            }
            return (true, None);
        }
        let sink = slab.take(id, self.tag);
        if sink.is_some() {
            // Counted inside the critical section that uncounts the pending sink, so a waiter
            // never sees the report as done while its sink still runs.
            self.in_sink.fetch_add(1, Ordering::SeqCst);
        }
        drop(slab);
        let Some(sink) = sink else {
            return (false, None);
        };
        let _running = SinkRunning(self);
        sink(error.map_or(Ok(result), Err));
        (true, None)
    }

    /// Polls the waiting registration `id`: once reported, `finish` takes
    /// the outcome and what the slot kept alive (where `convert` left the
    /// result), the slot is freed and its result returned; until then the
    /// waker is stored. An id that names no waiting registration (never the
    /// case for the [`WaitReport`] that registered it) is an error.
    fn poll_waiting<R>(
        &self,
        id: i64,
        cx: &mut Context<'_>,
        finish: impl FnOnce(&mut K, Result<*mut c_void, Error>) -> R,
    ) -> Poll<Result<R, Error>> {
        let mut slab = self.lock();
        let Some((index, (waiting, keep_alive))) =
            slab.find(id, self.tag).and_then(|index| Some(index).zip(slab.waiting(index)))
        else {
            return Poll::Ready(Err(Error::local_illegal_state(
                "The pending serde call is unknown to its registry",
            )));
        };
        match waiting.outcome.take() {
            Some(outcome) => {
                let result = finish(keep_alive, outcome.map(SendPtr::get));
                slab.release(index);
                Poll::Ready(Ok(result))
            },
            None => {
                match &waiting.waker {
                    Some(waker) if waker.will_wake(cx.waker()) => {},
                    _ => waiting.waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            },
        }
    }

    /// The future waiting for registration `id` was dropped: a report that
    /// already arrived frees the slot now, otherwise the report will, and
    /// until then the slot keeps alive what the C implementation reads, and
    /// the registry's owner, if it has one ([`Self::set_owner`]). The owner
    /// is alive here: the future borrowed the registry until now.
    fn abandon(&self, id: i64) {
        let mut slab = self.lock();
        let Some(index) = slab.find(id, self.tag) else {
            return;
        };
        let Some((waiting, _)) = slab.waiting(index) else {
            return;
        };
        if waiting.outcome.is_some() {
            slab.release(index);
        } else {
            waiting.abandoned = true;
            waiting.waker = None;
            waiting.kept = self.owner.get().and_then(Weak::upgrade);
        }
    }

    /// The point a later [`Self::wait_reported`] waits for: the callbacks
    /// registered from now on do not delay it.
    pub(crate) fn mark(&self) -> Mark {
        let mut slab = self.lock();
        slab.outstanding.push_back(0);
        slab.trim();
        Mark(slab.current_epoch())
    }

    /// Returns once every sink registered before `mark` was reported,
    /// and returned, or was drained. Cancellation-safe: dropping
    /// the future leaves nothing behind.
    ///
    /// It also waits for the sinks of the reports arriving meanwhile,
    /// whenever they were registered, so reports whose sinks keep
    /// overlapping, from several threads, can hold it back past the last
    /// report it waits for; it returns once they stop overlapping.
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

    /// Whether no sink registered before `mark` is awaiting its report
    /// or running.
    fn reported_before(&self, mark: Mark) -> bool {
        let slab = self.lock();
        let earlier = usize::try_from(mark.0.saturating_sub(slab.epoch_base)).unwrap_or(usize::MAX);
        slab.outstanding.iter().take(earlier).all(|&count| count == 0) && self.in_sink.load(Ordering::SeqCst) == 0
    }

    /// Takes every sink still awaiting a report, in registration order, for
    /// a client that completes them itself when it is destroyed; their ids
    /// are unknown afterwards, and the waiters are released. Waiting
    /// registrations stay: their futures, or their reports, free them.
    pub(crate) fn drain(&self) -> Vec<ResultSink> {
        let mut drained = {
            let mut slab = self.lock();
            let current = slab.current_epoch();
            let mut drained = Vec::new();
            for index in 0..slab.slots.len() {
                if matches!(slab.slots[index].entry, Some(Entry::Sink(_))) {
                    if let Some(Entry::Sink(pending)) = slab.slots[index].entry.take() {
                        drained.push(pending);
                    }
                    slab.release(index);
                }
            }
            slab.outstanding.clear();
            slab.outstanding.push_back(0);
            slab.epoch_base = current;
            drained
        };
        if self.waiters.load(Ordering::SeqCst) > 0 {
            self.reported.notify_waiters();
        }
        drained.sort_unstable_by_key(|pending| pending.seq);
        drained.into_iter().map(|pending| pending.sink).collect()
    }
}

/// The future of a waiting registration
/// ([`CallbackResults::register_waiting`]): ready with what `finish` makes of
/// the outcome once the report arrives. Dropping it first leaves the slot,
/// and what it keeps alive, to the report.
///
/// A hand-written future rather than an `async` block so that it stays two
/// words and a function pointer, small enough for the serde futures that
/// await it to be stored inline (`InlineFuture`).
pub(crate) struct WaitReport<'a, K: KeepAlive, R> {
    results: &'a CallbackResults<K>,
    id: i64,
    /// Set once the outcome was taken, so `Drop` does not abandon a freed slot.
    done: bool,
    finish: fn(&mut K, Result<*mut c_void, Error>) -> Result<R, Error>,
}

impl<'a, K: KeepAlive, R> WaitReport<'a, K, R> {
    /// Waits for registration `id` of `results`, made by the caller.
    pub(crate) fn new(
        results: &'a CallbackResults<K>,
        id: i64,
        finish: fn(&mut K, Result<*mut c_void, Error>) -> Result<R, Error>,
    ) -> Self {
        WaitReport { results, id, done: false, finish }
    }
}

impl<K: KeepAlive, R> Future for WaitReport<'_, K, R> {
    type Output = Result<R, Error>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `WaitReport` is `Unpin`: it holds no self-reference.
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(Err(Error::local_illegal_state("The serde call was already reported")));
        }
        match this.results.poll_waiting(this.id, cx, this.finish) {
            Poll::Ready(result) => {
                this.done = true;
                Poll::Ready(result.and_then(|r| r))
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<K: KeepAlive, R> Drop for WaitReport<'_, K, R> {
    fn drop(&mut self) {
        if !self.done {
            self.results.abandon(self.id);
        }
    }
}

/// Counts a sink `complete` runs; on drop (also when the sink panics) it
/// wakes the waiters once the last running sink returned.
struct SinkRunning<'a, K: KeepAlive>(&'a CallbackResults<K>);

impl<K: KeepAlive> Drop for SinkRunning<'_, K> {
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

    /// What a test waiting registration keeps alive: a buffer C would read,
    /// and the value a report converted.
    #[derive(Default)]
    struct TestKeepAlive {
        input: Vec<u8>,
        converted: Option<i32>,
        cleared: usize,
    }

    impl KeepAlive for TestKeepAlive {
        fn clear(&mut self) {
            self.input.clear();
            self.converted = None;
            self.cleared += 1;
        }
    }

    /// Reads a reported `*mut i32` into the keep-alive, as a serializer's
    /// convert copies its `kafka_Bytes_t`.
    fn convert_i32(keep_alive: &mut TestKeepAlive, result: *mut c_void) {
        // SAFETY: Test code: every success report passes a pointer to a live `i32`.
        keep_alive.converted = Some(unsafe { *(result as *const i32) });
    }

    fn finish_i32(keep_alive: &mut TestKeepAlive, outcome: Result<*mut c_void, Error>) -> Result<i32, Error> {
        outcome.map(|_| keep_alive.converted.take().expect("converted on report"))
    }

    /// Registers a waiting call keeping `input` alive; returns its id and the
    /// address C would read.
    fn register_input(results: &CallbackResults<TestKeepAlive>, input: &[u8]) -> (i64, *const u8) {
        results.register_waiting(convert_i32, |keep_alive| {
            keep_alive.input.extend_from_slice(input);
            keep_alive.input.as_ptr()
        })
    }

    #[test]
    fn waiting_report_before_the_first_poll_is_ready_at_once() {
        let results = CallbackResults::<TestKeepAlive>::with_keep_alive();
        let (id, _) = register_input(&results, b"abc");
        let mut value = 5_i32;
        assert!(results.complete(id, ptr::addr_of_mut!(value) as *mut c_void, None));
        let mut wait = pin!(WaitReport::new(&results, id, finish_i32));
        assert!(matches!(poll_once(wait.as_mut()), Poll::Ready(Ok(5))));
        assert!(!results.complete(id, ptr::null_mut(), None), "the slot was freed");
    }

    #[test]
    fn waiting_report_after_a_poll_wakes_the_future() {
        let results = Arc::new(CallbackResults::<TestKeepAlive>::with_keep_alive());
        let (id, input) = register_input(&results, b"abc");
        let input = SendPtr(input as *mut c_void);
        let reporter = {
            let results = Arc::clone(&results);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                // SAFETY: Test code: the slot keeps the 3 input bytes alive until this report.
                let read = unsafe { std::slice::from_raw_parts(input.get() as *const u8, 3) }.to_vec();
                let mut value = 9_i32;
                assert!(results.complete(id, ptr::addr_of_mut!(value) as *mut c_void, None));
                read
            })
        };
        let value = crate::ffi::common::serialization::block_on_thread(WaitReport::new(&results, id, finish_i32));
        assert_eq!(value.unwrap(), 9);
        assert_eq!(reporter.join().unwrap(), b"abc");
    }

    #[test]
    fn waiting_error_report_skips_the_conversion() {
        let results = CallbackResults::<TestKeepAlive>::with_keep_alive();
        let (id, _) = register_input(&results, b"x");
        assert!(results.complete(id, ptr::null_mut(), Some(Error::local_illegal_state("bad"))));
        let mut wait = pin!(WaitReport::new(&results, id, finish_i32));
        match poll_once(wait.as_mut()) {
            Poll::Ready(Err(error)) => assert_eq!(error.message(), "bad"),
            _ => panic!("expected the reported error"),
        }
    }

    #[test]
    fn a_second_waiting_report_is_ignored() {
        let results = CallbackResults::<TestKeepAlive>::with_keep_alive();
        let (id, _) = register_input(&results, b"x");
        let mut first = 1_i32;
        let mut second = 2_i32;
        assert!(results.complete(id, ptr::addr_of_mut!(first) as *mut c_void, None));
        assert!(!results.complete(id, ptr::addr_of_mut!(second) as *mut c_void, None));
        let mut wait = pin!(WaitReport::new(&results, id, finish_i32));
        assert!(matches!(poll_once(wait.as_mut()), Poll::Ready(Ok(1))));
    }

    #[test]
    fn an_abandoned_waiting_slot_keeps_its_input_until_the_report() {
        let results = CallbackResults::<TestKeepAlive>::with_keep_alive();
        let (id, input) = register_input(&results, b"keep");
        {
            let mut wait = pin!(WaitReport::new(&results, id, finish_i32));
            assert!(poll_once(wait.as_mut()).is_pending());
        }
        // SAFETY: Test code: the abandoned slot keeps the 4 input bytes alive until the report.
        assert_eq!(unsafe { std::slice::from_raw_parts(input, 4) }, b"keep");
        let mut value = 3_i32;
        assert!(
            results.complete(id, ptr::addr_of_mut!(value) as *mut c_void, None),
            "the report frees it"
        );
        assert!(!results.complete(id, ptr::null_mut(), None));
        // The freed slot, and its keep-alive, are reused, cleared.
        let (again, _) = results.register_waiting(convert_i32, |keep_alive| {
            assert!(keep_alive.input.is_empty());
            assert_eq!(keep_alive.cleared, 1);
        });
        assert_ne!(again, id, "a reused slot gets a new generation");
    }

    /// A registry with an owner: a waiting registration whose future is
    /// dropped before the report keeps the owner alive until the report,
    /// which hands the last reference back to the caller; one reported first
    /// keeps nothing.
    #[test]
    fn an_abandoned_waiting_slot_keeps_the_owner_until_the_report() {
        struct Holder {
            results: CallbackResults<TestKeepAlive>,
        }
        let holder = Arc::new(Holder { results: CallbackResults::with_keep_alive() });
        let owner: Weak<Holder> = Arc::downgrade(&holder);
        holder.results.set_owner(owner.clone());

        // Reported before its future is dropped: nothing is kept.
        let (reported, _) = register_input(&holder.results, b"x");
        let mut value = 4_i32;
        assert_eq!((true, true), {
            let (delivered, kept) =
                holder
                    .results
                    .complete_owned(reported, ptr::addr_of_mut!(value) as *mut c_void, None);
            (delivered, kept.is_none())
        });
        drop(WaitReport::new(&holder.results, reported, finish_i32));

        let (id, _) = register_input(&holder.results, b"keep");
        {
            let mut wait = pin!(WaitReport::new(&holder.results, id, finish_i32));
            assert!(poll_once(wait.as_mut()).is_pending());
        }
        let raw = Arc::as_ptr(&holder);
        drop(holder);
        assert!(owner.upgrade().is_some(), "the abandoned slot keeps its owner");
        // SAFETY: Test code: the abandoned slot holds a strong reference to the holder until this
        // report, so `raw` is live for the call; `kept` is dropped after it.
        let (delivered, kept) = unsafe { &(*raw).results }.complete_owned(id, ptr::null_mut(), None);
        assert!(delivered);
        assert!(kept.is_some(), "the report hands the kept reference back");
        drop(kept);
        assert!(owner.upgrade().is_none(), "the last reference was the slot's");
    }

    #[test]
    fn a_waiting_slot_dropped_after_its_report_is_freed() {
        let results = CallbackResults::<TestKeepAlive>::with_keep_alive();
        let (id, _) = register_input(&results, b"x");
        let mut value = 3_i32;
        assert!(results.complete(id, ptr::addr_of_mut!(value) as *mut c_void, None));
        drop(WaitReport::new(&results, id, finish_i32));
        assert!(!results.complete(id, ptr::null_mut(), None));
        assert_eq!(results.lock().free.len(), 1, "the slot is free again");
    }

    #[test]
    fn waiting_registrations_do_not_allocate_once_warm() {
        let results = CallbackResults::<TestKeepAlive>::with_keep_alive();
        let round = || {
            let (id, _) = register_input(&results, b"abcd");
            let mut value = 1_i32;
            assert!(results.complete(id, ptr::addr_of_mut!(value) as *mut c_void, None));
            let mut wait = pin!(WaitReport::new(&results, id, finish_i32));
            assert!(matches!(poll_once(wait.as_mut()), Poll::Ready(Ok(1))));
        };
        round();
        let _guard = AllocTrackingGuard::new();
        for _ in 0..100 {
            round();
        }
        assert_eq!(AllocTrackingGuard::count(), 0);
    }
}
