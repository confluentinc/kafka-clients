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

//! The per-client callbacks vector behind `_execute_callbacks` /
//! `_set_callbacks_notify` (CLAUDE.md §4, "Async variants of blocking
//! methods").
//!
//! No Rust thread ever runs a user callback. A `_cb` entry point, or a Rust
//! background task with something to deliver, pushes a closure onto the
//! client's [`CallbackQueue`]; the C caller drains it with the client's
//! `_execute_callbacks`, on whichever thread it chooses, and learns that there
//! is something to drain through the notify hook it registered with
//! `_set_callbacks_notify`. The hook fires once each time the vector goes from
//! empty to non-empty and may only *schedule* the drain (`call_soon_threadsafe`,
//! `uv_async_send`, a condition-variable signal), never run callbacks itself:
//! it is called from a Rust task.
//!
//! `CallbackQueue` has no Java counterpart (DoD #7): Java hands completions to
//! the caller's thread through `CompletableFuture` continuations, which C has
//! no equivalent for.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread::{self, ThreadId};

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
unsafe impl Sync for SendPtr {}

/// The callbacks vector of one client handle.
pub(crate) struct CallbackQueue {
    pending: Mutex<VecDeque<CallbackJob>>,
    notify: Mutex<Option<(NotifyFn, SendPtr)>>,
    /// Held for the whole of [`execute`](Self::execute), so two drains of one
    /// queue never run callbacks concurrently.
    running: Mutex<()>,
    /// The thread currently inside [`execute`](Self::execute), so a nested
    /// call from within a callback returns instead of deadlocking.
    owner: Mutex<Option<ThreadId>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A callback never unwinds into Rust (it is an `extern "C"` call), so a
    // poisoned guard can only come from a test; the queue's invariants do not
    // depend on the critical section completing.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
            running: Mutex::new(()),
            owner: Mutex::new(None),
        }
    }

    /// Queues `job` and fires the notify hook when the vector was empty.
    pub(crate) fn push(&self, job: CallbackJob) {
        let was_empty = {
            let mut pending = lock(&self.pending);
            let was_empty = pending.is_empty();
            pending.push_back(job);
            was_empty
        };
        if was_empty && let Some((notify, opaque)) = *lock(&self.notify) {
            // SAFETY: the hook and its opaque pointer were registered together
            // by the C caller, who keeps the opaque alive until it replaces the
            // hook or destroys the client.
            unsafe { notify(opaque.0) };
        }
    }

    /// Registers the hook fired on every empty-to-non-empty transition, or
    /// clears it with `None`.
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
    pub(crate) fn execute(&self) -> i32 {
        let me = thread::current().id();
        if *lock(&self.owner) == Some(me) {
            return 0;
        }
        let _running = lock(&self.running);
        *lock(&self.owner) = Some(me);
        let snapshot: Vec<CallbackJob> = lock(&self.pending).drain(..).collect();
        let count = snapshot.len();
        for job in snapshot {
            job();
        }
        *lock(&self.owner) = None;
        i32::try_from(count).unwrap_or(i32::MAX)
    }

    /// Whether anything is queued.
    // wired by the first client `_destroy`, which drains what is pending (Phase 2)
    #[cfg_attr(not(test), expect(dead_code))]
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
