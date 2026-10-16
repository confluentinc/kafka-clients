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

//! The per-client callbacks vector behind `__execute_callbacks` /
//! `__set_callbacks_notify` (CLAUDE.md §4, "Async variants of blocking
//! methods").
//!
//! No Rust thread ever runs a user callback. A `_cb` entry point, or a Rust
//! background task with something to deliver, pushes a closure onto the
//! client's [`CallbackQueue`]; the C caller drains it with the client's
//! `__execute_callbacks`, on whichever thread it chooses, and learns that there
//! is something to drain through the notify hook it registered with
//! `__set_callbacks_notify`. The hook fires once each time the vector goes from
//! empty to non-empty and may only *schedule* the drain (`call_soon_threadsafe`,
//! `uv_async_send`, a condition-variable signal), never run callbacks itself:
//! it is called from a Rust task.
//!
//! `CallbackQueue` has no Java counterpart (DoD #7): Java hands completions to
//! the caller's thread through `CompletableFuture` continuations, which C has
//! no equivalent for.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread::{self, ThreadId};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// A queued callback invocation: the C function pointer, its arguments and the
/// ownership transfers it implies, baked into one closure so the queue holds
/// one element type.
pub(crate) type CallbackJob = Box<dyn FnOnce() + Send>;

/// The shape every `kafka_<pkg>_<Client>_callbacks_notify_fn_t` typedef has.
pub(crate) type NotifyFn = unsafe extern "C" fn(opaque: *mut c_void);

/// A raw pointer the C side owns, carried across threads untouched.
///
/// The queue never dereferences it: it is handed back to the C function it
/// was registered with, on the thread that drains the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SendPtr(pub(crate) *mut c_void);

impl SendPtr {
    /// The pointer. Taking it through a method (not the field) makes a
    /// `move` closure capture the whole `Send` wrapper rather than the raw
    /// pointer field (edition 2021 disjoint captures).
    pub(crate) fn get(self) -> *mut c_void {
        self.0
    }
}

// SAFETY: the pointer is opaque to Rust; whoever registered it is responsible
// for the thread-safety of what it points at (the C side documents that
// callbacks may be executed from any thread the caller chooses).
unsafe impl Send for SendPtr {}
// SAFETY: as for `Send`: a shared `&SendPtr` only copies the pointer value out
// (`get`, the public field); Rust never reads or writes through it, so sharing
// the wrapper gives another thread nothing beyond the copy `Send` already allows.
unsafe impl Sync for SendPtr {}

/// The callbacks vector of one client handle.
pub(crate) struct CallbackQueue {
    pending: Mutex<VecDeque<CallbackJob>>,
    notify: Mutex<Option<(NotifyFn, SendPtr)>>,
    /// Signalled on every push, for a client's `_destroy` that must drain
    /// the queue while it awaits the `_cb` tasks still running (one of them
    /// may be waiting for a queued interface invocation to be reported).
    pushed: Notify,
    /// Held for the whole of [`execute`](Self::execute), so two drains of one
    /// queue never run callbacks concurrently.
    running: Mutex<()>,
    /// The thread currently inside [`execute`](Self::execute), so a nested
    /// call from within a callback returns instead of deadlocking.
    owner: Mutex<Option<ThreadId>>,
    /// Set by [`close`](Self::close), read and written under `pending`'s
    /// lock so a push is either queued before the close or run inline after.
    closed: AtomicBool,
    /// Cancelled by [`mark_client_stopping`](Self::mark_client_stopping)
    /// before a client's `_destroy` stops its runtime, whose timers then stop
    /// too: a blocking `KafkaFuture_get` with a timeout drops its timer and
    /// waits on (see `ffi::kafka_future`).
    client_stopping: CancellationToken,
    /// Cancelled by [`mark_client_stopped`](Self::mark_client_stopped) once
    /// the client's background work is gone, so only the queue's last
    /// callbacks can complete a future of it: a blocking `KafkaFuture_get`
    /// waiting on one then answers instead of waiting forever (see
    /// `ffi::kafka_future`).
    client_stopped: CancellationToken,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A C callback never unwinds into Rust (it is an `extern "C"` call), but
    // the Rust half of a job can panic while `execute` holds `running`, which
    // poisons it; the queue's invariants do not depend on the critical section
    // completing, and `Executing` restores the rest.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The snapshot [`CallbackQueue::execute`] is running, as an RAII guard: on
/// every exit, a panicking job's included, it clears the queue's `owner`, so
/// the thread is not left marked as executing (every later `execute` on it
/// would return 0 and run nothing), and it puts the jobs not yet run back at
/// the front of the vector in their order, so their callbacks still fire.
///
/// No Java counterpart: Java has no C callback vector (see the module docs).
struct Executing<'a> {
    queue: &'a CallbackQueue,
    remaining: std::vec::IntoIter<CallbackJob>,
}

impl Drop for Executing<'_> {
    fn drop(&mut self) {
        let unrun: Vec<CallbackJob> = self.remaining.by_ref().collect();
        if !unrun.is_empty() {
            let was_empty = {
                let mut pending = lock(&self.queue.pending);
                let was_empty = pending.is_empty();
                for job in unrun.into_iter().rev() {
                    pending.push_front(job);
                }
                was_empty
            };
            self.queue.pushed.notify_one();
            self.queue.notify_if(was_empty);
        }
        *lock(&self.queue.owner) = None;
    }
}

impl Default for CallbackQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl CallbackQueue {
    pub(crate) fn new() -> Self {
        Self {
            pending: Mutex::new(VecDeque::new()),
            notify: Mutex::new(None),
            pushed: Notify::new(),
            running: Mutex::new(()),
            owner: Mutex::new(None),
            closed: AtomicBool::new(false),
            client_stopping: CancellationToken::new(),
            client_stopped: CancellationToken::new(),
        }
    }

    /// Queues `job` and fires the notify hook when the vector was empty; once
    /// the queue is [closed](Self::close), runs `job` on the calling thread
    /// instead.
    pub(crate) fn push(&self, job: CallbackJob) {
        let was_empty = {
            let mut pending = lock(&self.pending);
            if self.closed.load(Ordering::Relaxed) {
                drop(pending);
                job();
                return;
            }
            let was_empty = pending.is_empty();
            pending.push_back(job);
            was_empty
        };
        self.pushed.notify_one();
        self.notify_if(was_empty);
    }

    /// Closes the queue, for a client's `_destroy` that has stopped the
    /// client's runtime and is about to run the last queued callbacks: no
    /// `__execute_callbacks` can run after it, so a completion pushed later
    /// runs on the pushing thread, as a client-less handle runs every
    /// completion. The only such completion is a `KafkaFuture_get_cb` on a
    /// future that outlived its client: it runs on the thread calling
    /// `get_cb`.
    pub(crate) fn close(&self) {
        let _pending = lock(&self.pending);
        self.closed.store(true, Ordering::Relaxed);
    }

    /// Whether [`close`](Self::close) ran.
    pub(crate) fn is_closed(&self) -> bool {
        let _pending = lock(&self.pending);
        self.closed.load(Ordering::Relaxed)
    }

    /// Records that a client's `_destroy` is about to stop the client's
    /// runtime, and so its timers: a blocking `KafkaFuture_get` with a
    /// timeout drops its timer and waits on, without one, until the client is
    /// [stopped](Self::mark_client_stopped). Called before the runtime
    /// stops, so the waiting thread wakes on this rather than on the timers
    /// shutting down.
    pub(crate) fn mark_client_stopping(&self) {
        self.client_stopping.cancel();
    }

    /// Records that the client's background work is gone, for a client's
    /// `_destroy`: nothing but the queue's last callbacks completes a future
    /// of the client any more, so a blocking `KafkaFuture_get` still waiting
    /// on one answers what the future holds now, and a later one, a callback's
    /// included, does not wait. Called before those last callbacks run, which
    /// may make such a `get` themselves. Implies
    /// [stopping](Self::mark_client_stopping).
    pub(crate) fn mark_client_stopped(&self) {
        self.client_stopping.cancel();
        self.client_stopped.cancel();
    }

    /// Whether [`mark_client_stopping`](Self::mark_client_stopping) (or
    /// [`mark_client_stopped`](Self::mark_client_stopped)) ran.
    pub(crate) fn is_client_stopping(&self) -> bool {
        self.client_stopping.is_cancelled()
    }

    /// Whether [`mark_client_stopped`](Self::mark_client_stopped) ran.
    pub(crate) fn is_client_stopped(&self) -> bool {
        self.client_stopped.is_cancelled()
    }

    /// Resolves once the client is stopping
    /// ([`mark_client_stopping`](Self::mark_client_stopping)); cancellation-safe.
    pub(crate) async fn client_stopping(&self) {
        self.client_stopping.cancelled().await;
    }

    /// Resolves once the client is stopped
    /// ([`mark_client_stopped`](Self::mark_client_stopped)); cancellation-safe.
    pub(crate) async fn client_stopped(&self) {
        self.client_stopped.cancelled().await;
    }

    /// Fires the notify hook when the vector has just gone from empty to
    /// non-empty.
    ///
    /// The hook runs after the registration's lock is released: a hook that
    /// waits (the Python binding's takes the GIL) must not hold up a push by
    /// the thread it waits for, such as a delivery callback the core fires
    /// inline in a non-blocking entry point
    /// (`kafka_producer_Producer_try_send_with_callback`) that the binding
    /// calls with the GIL held. So two calls of the hook can overlap, when a
    /// drain runs between two pushes, and [`set_notify`](Self::set_notify)
    /// does not wait for a call in flight.
    fn notify_if(&self, was_empty: bool) {
        if !was_empty {
            return;
        }
        let registered = *lock(&self.notify);
        if let Some((notify, opaque)) = registered {
            // SAFETY: the hook and its opaque pointer were registered together
            // by the C caller, who keeps the opaque alive until it destroys the
            // client; replacing the hook does not wait for this call.
            unsafe { notify(opaque.0) };
        }
    }

    /// Registers the hook fired on every empty-to-non-empty transition, or
    /// clears it with `None`. A call of the previous hook in flight runs to
    /// its end (see [`notify_if`](Self::notify_if)).
    pub(crate) fn set_notify(&self, notify: Option<NotifyFn>, opaque: *mut c_void) {
        *lock(&self.notify) = notify.map(|f| (f, SendPtr(opaque)));
    }

    /// Runs the callbacks queued so far, serially on the calling thread, and
    /// returns how many ran.
    ///
    /// Takes a snapshot of the vector first: a callback that queues another
    /// callback makes the vector go from empty to non-empty again, so the
    /// notify hook fires and the caller drains again later. Concurrent calls
    /// are serialized; a nested call from inside a running callback (same
    /// thread) returns 0 immediately instead of deadlocking.
    ///
    /// A job that panics unwinds out of this call (the client's
    /// `__execute_callbacks` guard reports it); [`Executing`] still clears the
    /// owner and puts the jobs it left unrun back at the front of the vector,
    /// so the next drain runs each of them once.
    pub(crate) fn execute(&self) -> i32 {
        let me = thread::current().id();
        if *lock(&self.owner) == Some(me) {
            return 0;
        }
        let _running = lock(&self.running);
        *lock(&self.owner) = Some(me);
        let snapshot: Vec<CallbackJob> = lock(&self.pending).drain(..).collect();
        let count = snapshot.len();
        let mut executing = Executing { queue: self, remaining: snapshot.into_iter() };
        for job in executing.remaining.by_ref() {
            job();
        }
        drop(executing);
        i32::try_from(count).unwrap_or(i32::MAX)
    }

    /// Whether the calling thread is inside [`execute`](Self::execute), that
    /// is, running one of the queued callbacks: a call made from a callback
    /// then cannot wait for another queued callback, since the nested
    /// `execute` it would need returns 0.
    pub(crate) fn is_executing_on_current_thread(&self) -> bool {
        *lock(&self.owner) == Some(thread::current().id())
    }

    /// Resolves once a job has been pushed since the last call (a permit is
    /// stored, so a push between a drain and this call is not lost).
    pub(crate) async fn pushed(&self) {
        self.pushed.notified().await;
    }

    /// Whether anything is queued.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.pending).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};

    static NOTIFIES: AtomicI32 = AtomicI32::new(0);

    unsafe extern "C" fn count_notify(opaque: *mut c_void) {
        NOTIFIES.fetch_add(1, Ordering::SeqCst);
        // The opaque is the registered pointer, handed back untouched.
        // SAFETY: Test code: the only registration of this hook passes the boxed
        // `AtomicI32` of `notify_fires_once_per_empty_to_non_empty_transition`, which
        // outlives every push that can fire it (the hook is cleared before the box drops).
        let counter = unsafe { &*(opaque as *const AtomicI32) };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn execute_runs_queued_callbacks_in_order_and_returns_the_count() {
        let queue = CallbackQueue::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        for i in 0..3 {
            let order = order.clone();
            queue.push(Box::new(move || order.lock().unwrap().push(i)));
        }
        assert!(!queue.is_empty());
        assert_eq!(queue.execute(), 3);
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
        assert!(queue.is_empty());
        assert_eq!(queue.execute(), 0);
    }

    /// A job pushed before `close` stays queued for the last drain; one
    /// pushed after runs at once, on the pushing thread.
    #[test]
    fn a_push_after_close_runs_inline() {
        let queue = CallbackQueue::new();
        let ran = Arc::new(Mutex::new(Vec::new()));
        let before = ran.clone();
        queue.push(Box::new(move || before.lock().unwrap().push("before")));
        assert!(!queue.is_closed());
        queue.close();
        assert!(queue.is_closed());
        let after = ran.clone();
        let pusher = thread::current().id();
        queue.push(Box::new(move || {
            assert_eq!(thread::current().id(), pusher);
            after.lock().unwrap().push("after");
        }));
        assert_eq!(*ran.lock().unwrap(), ["after"], "run inline, not queued");
        assert_eq!(queue.execute(), 1);
        assert_eq!(*ran.lock().unwrap(), ["after", "before"]);
        assert_eq!(queue.execute(), 0);
    }

    #[test]
    fn notify_fires_once_per_empty_to_non_empty_transition() {
        let queue = CallbackQueue::new();
        let opaque = Box::new(AtomicI32::new(0));
        let before = NOTIFIES.load(Ordering::SeqCst);
        queue.set_notify(Some(count_notify), &*opaque as *const AtomicI32 as *mut c_void);
        queue.push(Box::new(|| {}));
        queue.push(Box::new(|| {}));
        assert_eq!(opaque.load(Ordering::SeqCst), 1, "the second push finds the vector non-empty");
        assert_eq!(queue.execute(), 2);
        queue.push(Box::new(|| {}));
        assert_eq!(opaque.load(Ordering::SeqCst), 2, "fires again after a drain");
        assert_eq!(NOTIFIES.load(Ordering::SeqCst) - before, 2);
        queue.set_notify(None, std::ptr::null_mut());
        queue.push(Box::new(|| {}));
        assert_eq!(opaque.load(Ordering::SeqCst), 2, "cleared hook no longer fires");
        queue.execute();
    }

    #[test]
    fn a_callback_queuing_another_callback_leaves_it_for_the_next_drain() {
        let queue = Arc::new(CallbackQueue::new());
        let ran = Arc::new(AtomicI32::new(0));
        let (q, r) = (queue.clone(), ran.clone());
        queue.push(Box::new(move || {
            r.fetch_add(1, Ordering::SeqCst);
            let r = r.clone();
            q.push(Box::new(move || {
                r.fetch_add(10, Ordering::SeqCst);
            }));
        }));
        assert_eq!(queue.execute(), 1);
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        assert_eq!(queue.execute(), 1);
        assert_eq!(ran.load(Ordering::SeqCst), 11);
    }

    #[test]
    fn a_nested_execute_from_inside_a_callback_returns_zero() {
        let queue = Arc::new(CallbackQueue::new());
        let nested = Arc::new(AtomicI32::new(-1));
        let (q, n) = (queue.clone(), nested.clone());
        queue.push(Box::new(move || {
            q.push(Box::new(|| {}));
            n.store(q.execute(), Ordering::SeqCst);
        }));
        assert_eq!(queue.execute(), 1);
        assert_eq!(nested.load(Ordering::SeqCst), 0, "the nested call neither runs nor deadlocks");
        assert_eq!(queue.execute(), 1, "the callback queued inside is left for the next drain");
    }

    #[test]
    fn only_the_thread_running_a_callback_is_executing() {
        let queue = Arc::new(CallbackQueue::new());
        assert!(!queue.is_executing_on_current_thread());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (q, s) = (queue.clone(), seen.clone());
        queue.push(Box::new(move || {
            let elsewhere = {
                let q = q.clone();
                thread::spawn(move || q.is_executing_on_current_thread()).join().unwrap()
            };
            s.lock().unwrap().push((q.is_executing_on_current_thread(), elsewhere));
        }));
        assert_eq!(queue.execute(), 1);
        assert_eq!(*seen.lock().unwrap(), [(true, false)]);
        assert!(!queue.is_executing_on_current_thread(), "cleared once the drain returned");
    }

    #[test]
    fn a_panicking_callback_leaves_the_queue_usable_and_the_rest_queued() {
        let queue = CallbackQueue::new();
        let ran = Arc::new(Mutex::new(Vec::new()));
        let r = ran.clone();
        queue.push(Box::new(move || r.lock().unwrap().push("first")));
        queue.push(Box::new(|| panic!("callback boom")));
        let r = ran.clone();
        queue.push(Box::new(move || r.lock().unwrap().push("third")));
        let r = ran.clone();
        queue.push(Box::new(move || r.lock().unwrap().push("fourth")));
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| queue.execute()));
        assert!(unwound.is_err(), "the job's panic unwinds out of execute");
        assert_eq!(*ran.lock().unwrap(), ["first"]);
        // The same thread is not left marked as executing: the next drain runs
        // the jobs the panic left unrun, in order, each once.
        assert_eq!(queue.execute(), 2);
        assert_eq!(*ran.lock().unwrap(), ["first", "third", "fourth"]);
        assert!(queue.is_empty());
        assert_eq!(queue.execute(), 0);
    }

    /// Counts into the registered counter only, leaving `NOTIFIES` to
    /// `notify_fires_once_per_empty_to_non_empty_transition`, which runs in
    /// parallel.
    unsafe extern "C" fn count_into_opaque(opaque: *mut c_void) {
        // SAFETY: Test code: the opaque is the `AtomicI32` the test registered,
        // alive until the hook is cleared.
        let counter = unsafe { &*(opaque as *const AtomicI32) };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn requeued_callbacks_fire_the_notify_hook() {
        let queue = CallbackQueue::new();
        let opaque = Box::new(AtomicI32::new(0));
        queue.set_notify(Some(count_into_opaque), &*opaque as *const AtomicI32 as *mut c_void);
        queue.push(Box::new(|| panic!("callback boom")));
        queue.push(Box::new(|| {}));
        assert_eq!(opaque.load(Ordering::SeqCst), 1);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| queue.execute()));
        assert!(unwound.is_err());
        assert_eq!(
            opaque.load(Ordering::SeqCst),
            2,
            "the vector went from empty to non-empty again"
        );
        queue.set_notify(None, std::ptr::null_mut());
        assert_eq!(queue.execute(), 1);
    }

    #[test]
    fn concurrent_drains_are_serialized() {
        let queue = Arc::new(CallbackQueue::new());
        let inside = Arc::new(AtomicI32::new(0));
        let max_inside = Arc::new(AtomicI32::new(0));
        for _ in 0..50 {
            let (inside, max_inside) = (inside.clone(), max_inside.clone());
            queue.push(Box::new(move || {
                let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
                max_inside.fetch_max(now, Ordering::SeqCst);
                thread::yield_now();
                inside.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let q = queue.clone();
                thread::spawn(move || q.execute())
            })
            .collect();
        let total: i32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 50);
        assert_eq!(max_inside.load(Ordering::SeqCst), 1, "callbacks of one queue never overlap");
    }
}
