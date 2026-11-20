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

//! The cross-thread view of a share consumer.
//!
//! Has no Java class (`definition-of-done.md` §7; Milestone 18 decision D6,
//! 2026-11-20). See [`ShareConsumerHandle`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::consumer::internals::WakeupTrigger;

/// A `Clone + Send + Sync` handle on a share consumer that wakes it up from
/// another task or thread.
///
/// Java's `ShareConsumer#wakeup()` is the one method another thread may call
/// on a share consumer while the owning thread is blocked in `poll` or
/// `commitSync` (the `KafkaShareConsumer` class documentation, "Multithreaded
/// Processing"). A Rust share consumer is `Send` but not `Sync`, and `poll`
/// borrows it as `&mut self` for as long as it waits, so no other task can
/// hold a reference to call
/// [`ShareConsumer::wakeup`](crate::consumer::ShareConsumer::wakeup) on it. The handle
/// restores that one capability: take it with
/// [`ShareConsumer::handle`](crate::consumer::ShareConsumer::handle)
/// before handing the consumer to its task, and call
/// [`wakeup`](Self::wakeup) on it from any task or thread.
///
/// ```
/// use confluent_kafka::consumer::{MockShareConsumer, ShareConsumer, ShareConsumerHandle};
///
/// let consumer = MockShareConsumer::<String, String>::new();
/// let handle: ShareConsumerHandle = consumer.handle();
/// std::thread::spawn(move || handle.wakeup()).join().unwrap();
/// ```
///
/// Unlike [`ConsumerHandle`](crate::consumer::ConsumerHandle), the handle
/// exposes nothing but `wakeup`: Java rejects every other share-consumer call
/// from inside the acknowledgement commit callback
/// (`ShareConsumerImpl#acquire`), so there is no callback reentrancy to
/// support. Clones share the consumer, and the handle stays usable after the
/// consumer is closed or dropped, where `wakeup` then does nothing.
// D6: `poll` holds `&mut self`, so `wakeup(&self)` on the consumer is not
// reachable from a second thread during the poll (PLAN §7.1).
#[derive(Clone)]
#[doc(alias = "rust-only")]
pub struct ShareConsumerHandle {
    inner: ShareConsumerHandleInner,
}

#[derive(Clone)]
enum ShareConsumerHandleInner {
    /// `KafkaShareConsumer`: the consumer's own wakeup trigger, which
    /// `ShareConsumerImpl#wakeup` fires.
    Impl(WakeupTrigger),
    /// `MockShareConsumer`: the mock's wakeup flag, which its `wakeup` sets.
    Mock(Arc<AtomicBool>),
}

impl std::fmt::Debug for ShareConsumerHandle {
    // A view of a consumer; it has no identifier of its own.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumerHandle").finish_non_exhaustive()
    }
}

impl ShareConsumerHandle {
    /// The handle of a `KafkaShareConsumer`, firing its wakeup trigger.
    pub(crate) fn with_wakeup_trigger(wakeup_trigger: WakeupTrigger) -> Self {
        Self { inner: ShareConsumerHandleInner::Impl(wakeup_trigger) }
    }

    /// The handle of a `MockShareConsumer`, setting its wakeup flag.
    pub(crate) fn with_mock_flag(flag: Arc<AtomicBool>) -> Self {
        Self { inner: ShareConsumerHandleInner::Mock(flag) }
    }

    /// Wakes up the consumer, as
    /// [`ShareConsumer::wakeup`](crate::consumer::ShareConsumer::wakeup) on the
    /// consumer itself does: the call blocking in `poll` or `commit_sync` returns the
    /// `WakeupError` (predicate `is_wakeup_error`), or, with no such call in
    /// progress, the next one does.
    pub fn wakeup(&self) {
        match &self.inner {
            ShareConsumerHandleInner::Impl(wakeup_trigger) => wakeup_trigger.wakeup(),
            ShareConsumerHandleInner::Mock(flag) => flag.store(true, Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_thread_safety::{is_send, is_sync};

    /// R8 H1: the handle is the share consumer's one `Sync` view.
    #[test]
    fn share_consumer_handle_is_send_sync_and_clone() {
        fn assert_clone<T: Clone>() {}
        assert!(is_send!(ShareConsumerHandle));
        assert!(is_sync!(ShareConsumerHandle));
        assert_clone::<ShareConsumerHandle>();
        let handle = ShareConsumerHandle::with_mock_flag(Arc::new(AtomicBool::new(false)));
        assert_eq!("ShareConsumerHandle { .. }", format!("{handle:?}"));
    }

    /// R8 H5 (mock side): clones share the flag.
    #[test]
    fn handle_clones_share_the_mock_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let handle = ShareConsumerHandle::with_mock_flag(Arc::clone(&flag));
        handle.clone().wakeup();
        assert!(flag.load(Ordering::SeqCst));
    }
}
