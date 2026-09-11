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

//! `OffsetCommitCallbackInvoker` — app-thread executor of pending
//! `OffsetCommitCallback` and `ConsumerInterceptor::on_commit` invocations.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvoker`.
//!
//! # Invocation model (consumer-threading.md §31)
//!
//! The background task enqueues callbacks here; the application task drains
//! them at the top of every `poll() / commit_*() / close()` call via
//! [`Self::invoke_pending_callbacks`]. **Callbacks always run on the
//! caller's task**, never on the background task.
//!
//! This mirrors Java's `BlockingQueue<OffsetCommitCallbackTask>` precisely:
//! enqueue from BG, dequeue + invoke from app side.

// Phase 9 lands the invoker; Phase 11 wires it into AsyncKafkaConsumer.
// Suppress dead-code warnings until then.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::consumer::offset_commit_callback::OffsetCommitCallback;

use super::consumer_interceptors::ConsumerInterceptors;

/// One queued task in the invoker. Java: nested
/// `OffsetCommitCallbackInvoker.OffsetCommitCallbackTask`.
///
/// `kind` is `Interceptor` when the task is to invoke the interceptor
/// chain's `on_commit` (the chain itself is owned by the invoker), and
/// `User { callback }` when it is a user-supplied commit callback.
enum CallbackKind {
    /// Invoke the interceptor chain's `on_commit(offsets)`. The chain is
    /// owned by the invoker (see [`OffsetCommitCallbackInvoker`]).
    Interceptor,
    /// Invoke a user-supplied callback with the offsets / error.
    User { callback: Arc<dyn OffsetCommitCallback> },
}

struct PendingCallback {
    kind: CallbackKind,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    /// Optional error captured at enqueue time. `None` for the interceptor
    /// invocation (Java passes `null`).
    error: Option<Error>,
}

/// Utility that helps the application thread (Rust: caller task) invoke
/// user-registered [`OffsetCommitCallback`] and
/// [`ConsumerInterceptor`](crate::consumer::interceptor::ConsumerInterceptor)
/// instances when a commit response arrives on the BG task.
///
/// Translated from `OffsetCommitCallbackInvoker`.
///
/// # Ownership of the interceptor chain
///
/// Java's `OffsetCommitCallbackInvoker` is constructed with the
/// `ConsumerInterceptors` instance and enqueues a lambda that closes over
/// it. The Rust analog owns the interceptors directly inside the invoker
/// (via a `Mutex` so the sync `on_commit` invocation can be reached from
/// the app task while the BG task enqueues new work). Sharing the invoker
/// between BG and app tasks is done via `Arc<OffsetCommitCallbackInvoker>`,
/// not by sharing the inner chain.
///
/// `Mutex<ConsumerInterceptors>` (rather than `Arc<...>`) because the
/// `ConsumerInterceptor` trait does not require `Sync`, so we cannot share
/// a `&ConsumerInterceptors` across tasks; one task must hold the chain
/// exclusively during `on_commit`. The lock is acquired only inside
/// `invoke_pending_callbacks` and is never held across an `.await`.
pub(crate) struct OffsetCommitCallbackInvoker<K: 'static, V: 'static> {
    interceptors: Mutex<ConsumerInterceptors<K, V>>,
    /// Thread-safe queue of pending callbacks. Java uses
    /// `LinkedBlockingQueue`; here a plain `Mutex<VecDeque<...>>` is
    /// sufficient because the queue is never `take()`ed in a blocking
    /// fashion. The mutex is held only across `push_back` / `pop_front`,
    /// never across an `.await`.
    pending: Mutex<VecDeque<PendingCallback>>,
    /// Tracks whether the chain is empty without locking on the enqueue
    /// path. Mirrors Java's `interceptors.isEmpty()` check that happens
    /// without any extra synchronisation. Set at construction time and not
    /// expected to change for the lifetime of the invoker (matches Java —
    /// the interceptor list is final).
    interceptors_empty: bool,
}

/// Type-erased hook for enqueueing an interceptor `on_commit` invocation on
/// auto-commit success.
///
/// Rust-only shim (no Java analog as a named type): Java's
/// `CommitRequestManager` holds the `OffsetCommitCallbackInvoker` field
/// directly and calls `enqueueInterceptorInvocation(...)` from its
/// `autoCommitCallback` BiConsumer. The Rust `CommitRequestManager` is NOT
/// generic over `<K, V>` (see consumer-threading.md §28 / Phase 9 notes —
/// `commit_async` takes the generic invoker as a *parameter* rather than
/// owning it), so it cannot hold an `Arc<OffsetCommitCallbackInvoker<K, V>>`
/// field. This trait erases the generic parameters so the commit manager can
/// hold an `Option<Arc<dyn AutoCommitInterceptorHook>>` and fire the same
/// interceptor invocation from its auto-commit success path (mirroring Java's
/// `autoCommitCallback`). The only method needed for that path is
/// `enqueue_interceptor_invocation`, which carries no `K`/`V` in its
/// signature.
pub(crate) trait AutoCommitInterceptorHook: Send + Sync + 'static {
    /// Enqueue an interceptor `on_commit` invocation for the committed
    /// offsets. No-op when the interceptor chain is empty.
    fn enqueue_interceptor_invocation(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>);

    /// Whether any interceptor is registered. Lets the caller skip cloning
    /// the committed-offsets map on the auto-commit path when no interceptor
    /// would receive it (mirrors Java capturing the offsets map by reference
    /// in its BiConsumer — no copy when the chain is empty).
    fn has_interceptors(&self) -> bool;
}

impl<K, V> AutoCommitInterceptorHook for OffsetCommitCallbackInvoker<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn enqueue_interceptor_invocation(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) {
        OffsetCommitCallbackInvoker::enqueue_interceptor_invocation(self, offsets);
    }

    fn has_interceptors(&self) -> bool {
        !self.interceptors_empty
    }
}

impl<K, V> OffsetCommitCallbackInvoker<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Constructs a new invoker around the supplied interceptor chain.
    ///
    /// Mirrors Java's package-private constructor
    /// `OffsetCommitCallbackInvoker(ConsumerInterceptors<?, ?>)`.
    pub(crate) fn new(interceptors: ConsumerInterceptors<K, V>) -> Self {
        let interceptors_empty = interceptors.is_empty();
        Self {
            interceptors: Mutex::new(interceptors),
            pending: Mutex::new(VecDeque::new()),
            interceptors_empty,
        }
    }

    /// Enqueues an `on_commit` invocation on the interceptor chain if any
    /// interceptors are registered. No-op when the chain is empty —
    /// mirrors Java's guard `if (!interceptors.isEmpty())`.
    ///
    /// Mirrors Java's `enqueueInterceptorInvocation(Map)`.
    pub(crate) fn enqueue_interceptor_invocation(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) {
        if self.interceptors_empty {
            return;
        }
        let task = PendingCallback { kind: CallbackKind::Interceptor, offsets, error: None };
        let mut guard = match self.pending.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.push_back(task);
    }

    /// Enqueues a user-supplied `OffsetCommitCallback` to be invoked on the
    /// next call to [`Self::invoke_pending_callbacks`].
    ///
    /// Mirrors Java's `enqueueUserCallbackInvocation(OffsetCommitCallback, Map, Exception)`.
    pub(crate) fn enqueue_user_callback_invocation(
        &self,
        callback: Arc<dyn OffsetCommitCallback>,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<Error>,
    ) {
        let task = PendingCallback { kind: CallbackKind::User { callback }, offsets, error };
        let mut guard = match self.pending.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.push_back(task);
    }

    /// Drains the queue and invokes each pending callback in FIFO order on
    /// the caller's task (see consumer-threading.md §31). Must be `async`
    /// because [`OffsetCommitCallback::on_complete`] is `async`.
    ///
    /// Mirrors Java's `executeCallbacks()`. The lock is released between
    /// each `pop_front` and the subsequent `await` so concurrent
    /// `enqueue_*` calls from the BG task are not blocked while a callback
    /// runs.
    pub(crate) async fn invoke_pending_callbacks(&self) {
        loop {
            // Pop one task with the lock held; release before awaiting.
            let task = {
                let mut guard = match self.pending.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.pop_front()
            };
            let Some(task) = task else {
                return;
            };
            match task.kind {
                CallbackKind::Interceptor => {
                    // Acquire the chain lock; drop before any subsequent
                    // task is processed. on_commit is sync (no .await).
                    let guard = match self.interceptors.lock() {
                        Ok(g) => g,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    guard.on_commit(&task.offsets);
                    drop(guard);
                },
                CallbackKind::User { callback } => {
                    callback.on_complete(&task.offsets, task.error.as_ref()).await;
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvokerTest`.
    //!
    //! Java uses Mockito to verify callback / interceptor invocation order.
    //! The Rust translation uses inline `AtomicUsize` counters on the test
    //! interceptor + callback structs — same observable behaviour, no mock
    //! framework needed.

    use super::*;
    use crate::consumer::interceptor::ConsumerInterceptor;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    /// Test interceptor that records `on_commit` invocations in order.
    struct RecordingInterceptor {
        recorded: StdMutex<Vec<HashMap<TopicPartition, OffsetAndMetadata>>>,
    }

    impl RecordingInterceptor {
        fn new() -> Self {
            Self { recorded: StdMutex::new(Vec::new()) }
        }
        fn calls(&self) -> Vec<HashMap<TopicPartition, OffsetAndMetadata>> {
            self.recorded.lock().unwrap().clone()
        }
    }

    impl<K: 'static, V: 'static> ConsumerInterceptor<K, V> for RecordingInterceptor {
        fn on_consume(&self, _records: &mut crate::consumer::ConsumerRecords<K, V>) {}
        fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
            self.recorded.lock().unwrap().push(offsets.clone());
        }
    }

    /// Recorded invocation entry — offsets + optional error.
    type RecordedCall = (HashMap<TopicPartition, OffsetAndMetadata>, Option<Error>);

    /// Test callback that records each `on_complete` invocation.
    struct RecordingCallback {
        recorded: StdMutex<Vec<RecordedCall>>,
        invocations: AtomicUsize,
    }

    impl RecordingCallback {
        fn new() -> Self {
            Self { recorded: StdMutex::new(Vec::new()), invocations: AtomicUsize::new(0) }
        }
        fn invocations(&self) -> usize {
            self.invocations.load(Ordering::SeqCst)
        }
        fn calls(&self) -> Vec<RecordedCall> {
            self.recorded.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OffsetCommitCallback for RecordingCallback {
        async fn on_complete(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            self.recorded.lock().unwrap().push((offsets.clone(), error.cloned()));
        }
    }

    fn singleton_offset(tp: TopicPartition, offset: i64) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let mut map = HashMap::new();
        map.insert(tp, OffsetAndMetadata::new(offset).expect("non-negative offset"));
        map
    }

    /// Translated from
    /// `OffsetCommitCallbackInvokerTest.testMultipleUserCallbacksInvoked`.
    /// Multiple user callbacks are invoked in FIFO order on
    /// `invoke_pending_callbacks` and not re-invoked on subsequent calls.
    #[tokio::test(flavor = "current_thread")]
    async fn test_multiple_user_callbacks_invoked() {
        let tp = TopicPartition::new("t0".to_string(), 2);
        let offsets1 = singleton_offset(tp.clone(), 10);
        let offsets2 = singleton_offset(tp, 20);
        let cb1 = Arc::new(RecordingCallback::new());
        let cb2 = Arc::new(RecordingCallback::new());
        // No interceptors registered.
        let interceptors: ConsumerInterceptors<String, String> = ConsumerInterceptors::new(Vec::new());
        let invoker = OffsetCommitCallbackInvoker::new(interceptors);

        invoker.enqueue_user_callback_invocation(cb1.clone(), offsets1.clone(), None);
        invoker.enqueue_user_callback_invocation(cb2.clone(), offsets2.clone(), None);
        assert_eq!(cb1.invocations(), 0);
        assert_eq!(cb2.invocations(), 0);

        invoker.invoke_pending_callbacks().await;
        assert_eq!(cb1.invocations(), 1);
        assert_eq!(cb2.invocations(), 1);
        assert_eq!(cb1.calls()[0].0, offsets1);
        assert_eq!(cb2.calls()[0].0, offsets2);

        // Re-invoking with an empty queue must not double-fire.
        invoker.invoke_pending_callbacks().await;
        assert_eq!(cb1.invocations(), 1);
        assert_eq!(cb2.invocations(), 1);
    }

    /// Translated from
    /// `OffsetCommitCallbackInvokerTest.testNoOnCommitOnEmptyInterceptors`.
    /// `enqueue_interceptor_invocation` is a no-op when the chain is empty.
    ///
    /// Java: `verify(consumerInterceptors, never()).onCommit(any())`.
    /// Rust: the empty interceptor list has no `on_commit` to verify
    /// against, so we instead enqueue interceptor invocations AND a
    /// user callback, drain, and observe (a) the user callback fires
    /// (proves the queue made progress past the no-op interceptor
    /// entries) and (b) re-draining produces no more invocations
    /// (proves no stale interceptor tasks were retained).
    #[tokio::test(flavor = "current_thread")]
    async fn test_no_on_commit_on_empty_interceptors() {
        let tp = TopicPartition::new("t0".to_string(), 2);
        let offsets1 = singleton_offset(tp.clone(), 10);
        let offsets2 = singleton_offset(tp.clone(), 20);
        let interceptors: ConsumerInterceptors<String, String> = ConsumerInterceptors::new(Vec::new());
        let invoker = OffsetCommitCallbackInvoker::new(interceptors);

        invoker.enqueue_interceptor_invocation(offsets1);
        invoker.enqueue_interceptor_invocation(offsets2);

        // Enqueue a user callback AFTER the no-op interceptor entries.
        // It should fire on drain, proving the queue moves past them.
        let cb = Arc::new(RecordingCallback::new());
        invoker.enqueue_user_callback_invocation(cb.clone(), singleton_offset(tp, 30), None);

        invoker.invoke_pending_callbacks().await;
        // User callback fired exactly once after passing through the
        // no-op interceptor entries.
        assert_eq!(cb.invocations(), 1);

        // Re-draining should produce no additional invocations — the
        // empty-interceptor entries did not leave stale work behind.
        invoker.invoke_pending_callbacks().await;
        assert_eq!(cb.invocations(), 1);
    }

    /// Translated from
    /// `OffsetCommitCallbackInvokerTest.testOnlyInterceptors`. The
    /// interceptor chain's `on_commit` is invoked for each enqueued
    /// invocation in FIFO order.
    #[tokio::test(flavor = "current_thread")]
    async fn test_only_interceptors() {
        let tp = TopicPartition::new("t0".to_string(), 2);
        let offsets1 = singleton_offset(tp.clone(), 10);
        let offsets2 = singleton_offset(tp, 20);
        let recording = Arc::new(RecordingInterceptor::new());
        let interceptors: ConsumerInterceptors<String, String> = ConsumerInterceptors::new(vec![
            // Box the trait-object impl.
            Box::new(RecordingInterceptorWrapper(recording.clone())),
        ]);
        let invoker = OffsetCommitCallbackInvoker::new(interceptors);

        invoker.enqueue_interceptor_invocation(offsets1.clone());
        invoker.enqueue_interceptor_invocation(offsets2.clone());
        assert!(recording.calls().is_empty());

        invoker.invoke_pending_callbacks().await;
        let calls = recording.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], offsets1);
        assert_eq!(calls[1], offsets2);

        // Subsequent calls don't re-invoke.
        invoker.invoke_pending_callbacks().await;
        assert_eq!(recording.calls().len(), 2);
    }

    /// Translated from
    /// `OffsetCommitCallbackInvokerTest.testMixedCallbacksInterceptorsInvoked`.
    /// Mixed enqueue order is preserved: interceptors fire before the
    /// user callback because they were enqueued first.
    #[tokio::test(flavor = "current_thread")]
    async fn test_mixed_callbacks_interceptors_invoked() {
        let tp = TopicPartition::new("t0".to_string(), 2);
        let offsets1 = singleton_offset(tp.clone(), 10);
        let offsets2 = singleton_offset(tp.clone(), 20);
        let recording = Arc::new(RecordingInterceptor::new());
        let interceptors: ConsumerInterceptors<String, String> =
            ConsumerInterceptors::new(vec![Box::new(RecordingInterceptorWrapper(recording.clone()))]);
        let cb1 = Arc::new(RecordingCallback::new());
        let invoker = OffsetCommitCallbackInvoker::new(interceptors);

        invoker.enqueue_interceptor_invocation(offsets1.clone());
        invoker.enqueue_interceptor_invocation(offsets2.clone());
        invoker.enqueue_user_callback_invocation(cb1.clone(), offsets1.clone(), None);
        assert!(recording.calls().is_empty());
        assert_eq!(cb1.invocations(), 0);

        invoker.invoke_pending_callbacks().await;
        let intercept_calls = recording.calls();
        assert_eq!(intercept_calls.len(), 2);
        assert_eq!(intercept_calls[0], offsets1);
        assert_eq!(intercept_calls[1], offsets2);
        assert_eq!(cb1.invocations(), 1);
        assert_eq!(cb1.calls()[0].0, offsets1);

        invoker.invoke_pending_callbacks().await;
        assert_eq!(recording.calls().len(), 2);
        assert_eq!(cb1.invocations(), 1);
    }

    /// Helper wrapper so a single `RecordingInterceptor` instance can be
    /// shared between the chain (which takes ownership via `Box<dyn ...>`)
    /// and the test (which inspects it via `Arc`).
    struct RecordingInterceptorWrapper(Arc<RecordingInterceptor>);

    impl<K: 'static, V: 'static> ConsumerInterceptor<K, V> for RecordingInterceptorWrapper {
        fn on_consume(&self, _records: &mut crate::consumer::ConsumerRecords<K, V>) {}
        fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
            <RecordingInterceptor as ConsumerInterceptor<K, V>>::on_commit(&self.0, offsets);
        }
    }
}
