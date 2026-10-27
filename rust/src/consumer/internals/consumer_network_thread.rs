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

//! `ConsumerNetworkThread` — the consumer background task that drives the
//! request managers, the network client, the event reaper, and the
//! KIP-848 membership state machine.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread`.
//!
//! # Translation overview
//!
//! Java runs a dedicated `Thread`; Rust uses a single `tokio::spawn` per
//! consumer instance (`consumer-threading.md` §10). `runOnce()` is
//! translated phase-for-phase:
//!
//! 1. **Drain application events** — `try_recv` in a `while let` loop
//!    (`consumer-threading.md` §10 "Drain unbounded (Java does)").
//! 2. **Poll each request manager** in `RequestManagers::entries()`
//!    order, stage requests via `add_all_from_poll_result`, fold their
//!    `time_until_next_poll_ms` into a min.
//! 3. **Drive the membership reconciler** —
//!    `ConsumerMembershipManager::reconcile(now)`. Java's
//!    `RequestManagers.entries()` includes membership (whose `poll(...)`
//!    body returns `PollResult.EMPTY` after calling
//!    `maybeReconcile(false)`). Rust's `entries()` intentionally skips
//!    membership because the same `Arc` is shared with the heartbeat
//!    manager (we cannot produce `&mut dyn RequestManager` from an
//!    `Arc`). The Java side-effect is re-supplied here, at the same
//!    phase point Java's loop would visit it.
//! 4. **Poll the network client** — wrapped in `tokio::select!` against
//!    the current wakeup token and the shutdown signal. This is the
//!    only `.await` point in `run_once` that must be cancellation-safe
//!    against the wakeup primitive (`consumer-threading.md` §11).
//! 5. **Refresh `maximumTimeToWait`** — fold each manager's
//!    `maximum_time_to_wait(now)` into a min; store into the cached
//!    `AtomicI64` so the app side can read it without locking the
//!    manager set (CLAUDE.md §13: single numeric across tasks ⇒ atomic).
//! 6. **Reap expired application events** — `CompletableEventReaper::reap`.
//!
//! `cleanup()` is the symmetric path: `pollOnClose` from each manager,
//! `addAll` into the delegate, repeatedly `poll(true)` until the timer
//! expires or the queues drain, then reap-on-close the application event
//! queue and close every manager.
//!
//! # Scope of THIS commit (Phase 10 commit 7/N)
//!
//! Smoke-test surface only: `run_once`, `cleanup`, the wakeup wiring, and
//! the `membership.reconcile()` per-iteration hook. The exhaustive
//! `ConsumerNetworkThreadTest` translation (parametrized poll timeouts,
//! initialize-resources error paths, metrics assertions) lands in commit
//! 8. The `AsyncKafkaConsumer` glue (spawn / app-side wakeup rotation /
//! poll-time computations) lands in Phase 11.
//!
//! # Metrics
//!
//! `AsyncConsumerMetrics` (Phase M6) is wired into the bg loop via the
//! optional [`ConsumerNetworkThread::set_async_consumer_metrics`] setter
//! (M4/M5 precedent). `recordTimeBetweenNetworkThreadPoll` fires per
//! `run_once`, `recordApplicationEventQueueSize`/`...QueueTime`/
//! `...QueueProcessingTime` in `process_application_events`, and
//! `recordApplicationEventExpiredSize` at both reap sites (run_once +
//! cleanup). When the metrics are not wired (tests that don't care), these
//! sites are no-ops. The `AsyncConsumerMetrics` value-parity tests live in
//! `async_consumer_metrics.rs`.
//!
//! # Metadata-error notification on uncompleted events
//!
//! Java's `runOnce` ends with `maybeFailOnMetadataError(uncompletedEvents)`
//! where `uncompletedEvents = applicationEventReaper.uncompletedEvents()`.
//! The Rust reaper stores **erased** handles (`Arc<dyn
//! CompletableEventErasedHandle>`), so it cannot match against
//! `MetadataErrorNotifiableEvent` after the fact.
//!
//! Phase 10 commit 8 adds a parallel list,
//! [`ConsumerNetworkThread::notifiable_handles`], populated during
//! `process_application_events` from
//! [`ApplicationEvent::metadata_error_notifiable_handle`] (the
//! intersection of `is_metadata_error_notifiable()` and
//! `erased_handle().is_some()`). After Phase 6 (reap), Phase 7 runs
//! [`ConsumerNetworkThread::maybe_fail_on_metadata_error_uncompleted`]
//! which mirrors Java's `maybeFailOnMetadataError(uncompletedEvents)`:
//! query the delegate for a pending metadata error and, if present,
//! call `complete_with_error(err)` on every notifiable handle that is not
//! yet done. The per-event arm inside
//! [`ConsumerNetworkThread::process_application_events`] (Java step 1's
//! `maybeFailOnMetadataError(List.of(event))` arm) covers "immediately
//! completed events" — both arms are present and behavior-faithful.

#![expect(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use tokio::sync::{Mutex as AsyncMutex, mpsc};

use crate::KafkaClient;
use crate::common::utils::Time;

use super::ConsumerMembershipManager;
use super::NetworkClientDelegate;
use super::RequestManager;
use super::RequestManagers;
use super::WakeupTrigger;
use super::events::ApplicationEventEnvelope;
use super::events::ApplicationEventProcessor;
use super::events::CompletableEventReaper;
use super::events::EventProcessor;
use super::panic_payload_message;

/// Consumer background task — single `tokio::spawn` per consumer instance
/// per `consumer-threading.md` §10. Owns the request managers, the
/// network client delegate, the application event processor, the event
/// reaper, and the membership manager. The application event channel
/// (receiver half) is also owned here; the sender lives on the app side
/// in [`super::events::ApplicationEventHandler`].
///
/// # Concurrency
///
/// `application_event_processor` and `network_client_delegate` are held
/// behind `tokio::sync::Mutex` to allow brief shared access from app-side
/// methods that need to query `network_client_delegate` state (e.g.
/// `inflight_request_count` for tests), but production accesses go
/// through the owning bg task. The mutex is dropped before any `.await`
/// on the network client to avoid stalling external readers.
///
/// `request_managers` is wrapped in `Arc<std::sync::Mutex<...>>` to
/// match the ownership scheme established in Phase 10 commit 4 (the
/// `ApplicationEventProcessor` also borrows it).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread")]
pub(crate) struct ConsumerNetworkThread<K: KafkaClient + Send + 'static> {
    /// Receiver half of the application event channel. Drained at the
    /// start of every `run_once` via non-blocking `try_recv`.
    application_event_rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
    /// Reaper tracking deadline-exceeded completable events. Same
    /// instance as the one referenced by app-side callers — `Arc` so it
    /// can be probed from tests.
    application_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
    /// Dispatch table for application events.
    application_event_processor: ApplicationEventProcessor,
    /// Network client delegate. Wrapped in `tokio::sync::Mutex` because
    /// it holds an `&mut`-only `poll(...).await` and the bg task is the
    /// sole production holder of the mutex.
    network_client_delegate: Arc<AsyncMutex<NetworkClientDelegate<K>>>,
    /// Shared request-managers container. Production callers (the AEP,
    /// `run_once`) lock briefly with `std::sync::Mutex`. The lock is
    /// never held across an `.await` per `consumer-threading.md` §16.
    request_managers: Arc<std::sync::Mutex<RequestManagers>>,
    /// Wakeup primitive, retained only for [`Self::signal_close`]. The bg
    /// task does NOT observe the wakeup token: interrupting the network poll
    /// goes through the selector's own wakeup handle instead (see Phase 4 of
    /// [`Self::run_once`]), matching Java, where `KafkaConsumer.wakeup()`
    /// completes the app-side future and never touches the network thread.
    wakeup: WakeupTrigger,
    /// Shutdown signal — flipped to `true` by [`Self::signal_close`].
    /// The bg-task loop exits cleanly the next iteration.
    running: Arc<AtomicBool>,
    /// Cached `maximumTimeToWait` value — read by the app side via
    /// [`Self::maximum_time_to_wait`]. Updated by `run_once` after each
    /// pass through the request managers.
    cached_max_time_to_wait_ms: Arc<AtomicI64>,
    /// Close timeout (millis) — Java's `volatile Duration closeTimeout`
    /// (`ConsumerNetworkThread#closeTimeout`). Written by the app side's close
    /// path through the shared [`Self::close_timeout_handle`] (Java's
    /// `ConsumerNetworkThread#closeInternal` assigns `closeTimeout = timeout`)
    /// and read by [`Self::cleanup`].
    close_timeout_ms: Arc<AtomicI64>,
    /// Wall-clock timestamp of the last `run_once` call. Feeds Java's
    /// `recordTimeBetweenNetworkThreadPoll(currentTimeMs - lastPollTimeMs)`
    /// metric (wired in `run_once` when `async_consumer_metrics` is set).
    last_poll_time_ms: i64,
    /// Time source — `SystemTime` in production, `MockTime` in tests.
    time: Arc<dyn Time>,
    /// Membership manager — driven explicitly per iteration per the
    /// Phase-10 Critic round-1 aside. Java drives it via
    /// `entries()` because `AbstractMembershipManager` implements
    /// `RequestManager`; the Rust `entries()` skips it (the same Arc is
    /// shared with the heartbeat manager) so we call `reconcile` here.
    membership: Option<Arc<ConsumerMembershipManager>>,
    /// Erased handles for notifiable+completable events that may still
    /// be in flight. Populated during `process_application_events`,
    /// pruned by `is_done()` checks at the post-poll arm. Mirrors the
    /// subset of `applicationEventReaper.uncompletedEvents()` that Java
    /// passes to `maybeFailOnMetadataError(uncompletedEvents)` — see
    /// the module docstring.
    notifiable_handles: Vec<Arc<dyn super::events::CompletableEventErasedHandle>>,
    /// Scratch buffers reused across `run_once` iterations (Phase 28):
    /// the PollResult before/after batches and the application-event
    /// drain buffer. Java's `entries` is a final List built once in the
    /// constructor and `processApplicationEvents` drains into a
    /// GC-nursery LinkedList — per-iteration heap allocation here was a
    /// translation artifact (~7k iterations/s on the bg hot loop).
    /// Always left empty between iterations; capacity is retained.
    poll_results_before_scratch: Vec<super::PollResult>,
    poll_results_after_scratch: Vec<super::PollResult>,
    app_event_drain_scratch: Vec<ApplicationEventEnvelope>,
    /// Async-consumer metrics (`AsyncConsumerMetrics`). `None` until wired
    /// post-construction by the live consumer (M4/M5 setter precedent);
    /// tests leave it unset and the bg-loop record points are no-ops.
    async_consumer_metrics: Option<Arc<super::metrics::AsyncConsumerMetrics>>,
    /// Shared mirror of the application-event queue depth, written by
    /// [`super::events::ApplicationEventHandler::add`]
    /// and reset to 0 by `process_application_events` (Java's
    /// `recordApplicationEventQueueSize(0)` after `drainTo`). `None` when
    /// metrics are not wired.
    application_event_queue_size: Option<Arc<AtomicI64>>,
}

impl<K: KafkaClient + Send + 'static> ConsumerNetworkThread<K> {
    /// `consumer-threading.md` §11 / Java
    /// `ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS`.
    pub(crate) const MAX_POLL_TIMEOUT_MS: i64 = 5_000;

    /// Default close-timeout in ms. Mirrors Java's
    /// `ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS`.
    pub(crate) const DEFAULT_CLOSE_TIMEOUT_MS: i64 = 30_000;

    /// Java constructor. Drops `LogContext` (Rust uses `log`) and the three
    /// `Supplier<...>` indirections (Rust takes the already-constructed
    /// values directly). The Java `AsyncConsumerMetrics` parameter is wired
    /// post-construction via [`Self::set_async_consumer_metrics`] (Phase M6).
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#ConsumerNetworkThread")]
    pub(crate) fn new(
        time: Arc<dyn Time>,
        application_event_rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
        application_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
        application_event_processor: ApplicationEventProcessor,
        network_client_delegate: Arc<AsyncMutex<NetworkClientDelegate<K>>>,
        request_managers: Arc<std::sync::Mutex<RequestManagers>>,
        membership: Option<Arc<ConsumerMembershipManager>>,
        wakeup: WakeupTrigger,
        cached_max_time_to_wait_ms: Arc<AtomicI64>,
    ) -> Self {
        // Seed the shared slot with `MAX_POLL_TIMEOUT_MS` — Java's
        // `ApplicationEventHandler.maximumTimeToWait()` returns
        // `Long.MAX_VALUE` until the bg thread's first `runOnce` runs, but
        // the bg-task here writes a tighter bound on each iteration, so we
        // seed with `MAX_POLL_TIMEOUT_MS` (the safer "wake at least this
        // often" default).
        cached_max_time_to_wait_ms.store(Self::MAX_POLL_TIMEOUT_MS, Ordering::Release);
        Self {
            application_event_rx,
            application_event_reaper,
            application_event_processor,
            network_client_delegate,
            request_managers,
            wakeup,
            running: Arc::new(AtomicBool::new(true)),
            cached_max_time_to_wait_ms,
            close_timeout_ms: Arc::new(AtomicI64::new(Self::DEFAULT_CLOSE_TIMEOUT_MS)),
            last_poll_time_ms: 0,
            time,
            membership,
            notifiable_handles: Vec::new(),
            poll_results_before_scratch: Vec::new(),
            poll_results_after_scratch: Vec::new(),
            app_event_drain_scratch: Vec::new(),
            async_consumer_metrics: None,
            application_event_queue_size: None,
        }
    }

    /// Wires the `AsyncConsumerMetrics` and the shared application-event
    /// queue-depth counter post-construction (M4/M5 setter precedent —
    /// keeps the no-arg `new` and all existing test call sites untouched).
    /// Java passes `AsyncConsumerMetrics` to the constructor.
    pub(crate) fn set_async_consumer_metrics(
        &mut self,
        metrics: Arc<super::metrics::AsyncConsumerMetrics>,
        application_event_queue_size: Arc<AtomicI64>,
    ) {
        self.async_consumer_metrics = Some(metrics);
        self.application_event_queue_size = Some(application_event_queue_size);
    }

    /// Java: `isRunning()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#isRunning")]
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Returns a clone of the `Arc<AtomicBool>` running flag — used by
    /// the Phase-12 production ctor to build the
    /// `NetworkThreadCloseHandle.signal_close_fn` closure before
    /// `self` is moved into `tokio::spawn`. Setting this to `false`
    /// (in combination with firing the wakeup trigger) is the public
    /// signal to exit the bg-task loop.
    pub(crate) fn running_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.running)
    }

    /// Java: `maximumTimeToWait()` — read-only accessor exposed to the
    /// app side.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#maximumTimeToWait")]
    pub(crate) fn maximum_time_to_wait(&self) -> i64 {
        self.cached_max_time_to_wait_ms.load(Ordering::Acquire)
    }

    /// Mirror of Java's `wakeup()`. Cancels the current token via the
    /// shared [`WakeupTrigger`] and also calls
    /// `network_client_delegate.wakeup()` to unblock the underlying
    /// `KafkaClient::poll`. The trigger is shared with the app side
    /// (`AsyncKafkaConsumer::wakeup()` in Phase 11).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#wakeup")]
    pub(crate) async fn wakeup(&self) {
        self.wakeup.wakeup();
        // Java additionally calls `networkClientDelegate.wakeup()` to
        // break out of the underlying selector — we do the same so the
        // `KafkaClient` is unblocked even if the await is not inside
        // our `select!`.
        let delegate = self.network_client_delegate.lock().await;
        delegate.wakeup();
    }

    /// Signals the bg task to exit at the next iteration. Mirrors the
    /// state change inside Java's `closeInternal(...)` (without the
    /// `join()` — the spawn handle is owned by the caller in Rust).
    ///
    /// Setting `running = false` AND firing the wakeup token in
    /// combination ensure the bg task observes the shutdown without
    /// needing to wait for `MAX_POLL_TIMEOUT_MS` to elapse.
    pub(crate) fn signal_close(&self) {
        self.running.store(false, Ordering::Release);
        self.wakeup.wakeup();
    }

    /// Shared handle to the close timeout read by [`Self::cleanup`], captured
    /// by the production close closure before `self` moves onto the bg task
    /// (same pattern as [`Self::running_handle`]).
    pub(crate) fn close_timeout_handle(&self) -> Arc<AtomicI64> {
        Arc::clone(&self.close_timeout_ms)
    }

    /// Sets the close timeout used by [`Self::cleanup`].
    pub(crate) fn set_close_timeout_ms(&self, timeout_ms: i64) {
        self.close_timeout_ms.store(timeout_ms, Ordering::Release);
    }

    /// One iteration of the bg-task loop. Mirrors Java's
    /// `ConsumerNetworkThread.runOnce()` line-for-line; see the
    /// module-level docstring for the phase-by-phase mapping.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#runOnce")]
    pub(crate) async fn run_once(&mut self) {
        // ──── Phase 1: drain application events ────
        self.process_application_events();

        // ──── Phase 1.5: resume an unsubscribe waiting on its callback ────
        //
        // Java chains `clearAssignmentAndLeaveGroup()` on the leave's release
        // callback with `whenComplete`; it runs on the network thread while
        // `processApplicationEvents()` handles the callback-completed event,
        // i.e. before the request managers poll. Phase 1 does the same when it
        // processes the `ConsumerRebalanceListenerCallbackCompleted` event that
        // carries the ack. This covers an ack dropped with no completed event
        // to carry it (see the per-event check in `process_application_events`)
        // when no application event follows.
        // Either way the member moves PREPARE_LEAVING -> LEAVING before the
        // heartbeat manager's poll in this same iteration, which then builds
        // the leave heartbeat.
        self.maybe_apply_leave_callback_ack();

        // Acquire the network-client-delegate mutex ONCE for the whole
        // iteration. The bg task is the delegate's only locker (verified:
        // the app side signals wakeups via the `WakeupTrigger` token and the
        // selector `Notify`, never through this mutex), so the guard is
        // uncontended and holding it across the iteration's `.await` points
        // can deadlock nothing. Java's `ConsumerNetworkThread` owns
        // `networkClientDelegate` as a plain field with no lock at all —
        // one guard per `runOnce` is the closest Rust emulation, and it
        // removes the 3-4 lock/unlock cycles per iteration that profiling
        // showed as pure futex/atomic overhead on the bg hot loop
        // (Phase 27 Fix #1). Cloning the `Arc` first keeps the guard's
        // borrow off `self`, so `&mut self` helpers stay callable.
        let delegate_arc = Arc::clone(&self.network_client_delegate);
        let mut delegate_guard = delegate_arc.lock().await;

        let current_time_ms = self.time.milliseconds();
        // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#runOnce` — record the time between
        // network-thread polls. Only recorded once a prior poll has happened (Java's `lastPollTimeMs != 0`
        // guard). `current_time_ms` is already computed for the iteration, so
        // there is no extra clock read here.
        if self.last_poll_time_ms != 0
            && let Some(metrics) = &self.async_consumer_metrics
        {
            metrics.record_time_between_network_thread_poll(current_time_ms.saturating_sub(self.last_poll_time_ms));
        }
        self.last_poll_time_ms = current_time_ms;

        // ──── Phase 2: poll each request manager and stage requests ────
        //
        // Java's body:
        //
        //   for (RequestManager rm : requestManagers.entries()) {
        //       PollResult pollResult = rm.poll(currentTimeMs);
        //       long timeoutMs = networkClientDelegate.addAll(pollResult);
        //       pollWaitTimeMs = Math.min(pollWaitTimeMs, timeoutMs);
        //   }
        //
        // Java's `entries()` includes membership in-line (between
        // heartbeat and offsets); its `poll(...)` body is
        // `maybeReconcile(false); return EMPTY;` so the side-effect
        // happens between the heartbeat's `addAll` and the offsets'
        // `poll(...)`. Rust's `entries()` intentionally skips
        // membership (Phase 8b ownership: `Arc` shared with the
        // heartbeat manager), so we split the entries walk in two and
        // call `ConsumerMembershipManager::reconcile(now, false)`
        // between them — preserving Java's invariant that
        // `offsets.poll()` / `topic_metadata.poll()` / `fetch.poll()`
        // observe any post-reconcile subscription-state updates within
        // the SAME `run_once` iteration.
        //
        // Rust additionally drains `PollResult::try_connect` (Phase 10
        // 3d) — Java's request managers call
        // `networkClientDelegate.tryConnect(node)` directly during
        // `poll(...)`, but the Rust manager has no handle on the
        // delegate, so the connect attempt is emitted on the
        // `PollResult` and serviced here.
        //
        // We collect the `(PollResult, try_connect_nodes)` pairs first
        // (sync, inside the rm_guard), then drop the guard, then
        // process `try_connect` and `add_all_from_poll_result` on the
        // delegate. This keeps the `std::sync::Mutex` over
        // `request_managers` from being held across any `.await`
        // (`consumer-threading.md` §16).
        let mut poll_wait_time_ms: i64 = Self::MAX_POLL_TIMEOUT_MS;
        // Phase 28: the two PollResult batches are accumulated in scratch
        // buffers reused across iterations (taken here, restored cleared at
        // the end of `run_once`) — the previous shape allocated three Vecs
        // per iteration (`collect`, `split_off`, `before`) at ~7k
        // iterations/s. Java's `entries` is a final List field built once in
        // the constructor, so per-iteration allocation here was a pure
        // translation artifact.
        let mut collected_before = std::mem::take(&mut self.poll_results_before_scratch);
        let mut collected_after = std::mem::take(&mut self.poll_results_after_scratch);
        debug_assert!(collected_before.is_empty() && collected_after.is_empty());
        {
            // Java's `entries()` walks
            // `coordinator → commit → heartbeat → membership → offsets …`.
            // The Rust container skips the three `Arc`-shared slots
            // (coordinator, commit, membership) so the heartbeat manager
            // and the membership manager can share their dependencies
            // with the slot in `RequestManagers`. The bg-task drives
            // the skipped managers explicitly here:
            //   * `coordinator.poll(now)` — emits FindCoordinator
            //     requests (the only RM whose `poll` does real work
            //     when present); acquired via the `Arc<Mutex<...>>`
            //     handle.
            //   * `commit.poll_with_coordinator(coord, now)` — Java's
            //     `org.apache.kafka.clients.consumer.internals.CommitRequestManager#poll`. Drains
            //     `unsent_offset_commits` and `unsent_offset_fetches`
            //     into `UnsentRequest`s and handles
            //     `closing && coordinator unknown` by failing pending
            //     commits with `CommitFailedException`. It does NOT fire
            //     the auto-commit timer: since KAFKA-18641 that happens
            //     only from the application's poll, through the
            //     `AsyncPoll` / `AssignmentChange` events'
            //     `update_timer_and_maybe_commit`. Without this
            //     call `commit_sync()`/`committed()` hang forever
            //     (Phase-12 Critic Issue 1).
            //   * `membership.reconcile(now, false)` — driven in
            //     Phase 2.5 below.
            //
            // One `request_managers` lock for the whole phase (Phase 28 —
            // was two: one for `entries()`, one for `membership_boundary()`).
            // Poll-call order now matches Java's `entries()` walk:
            // coordinator → commit → heartbeat → offsets → … (the previous
            // shape polled the heartbeat/offsets/fetch managers BEFORE
            // coordinator/commit and only reordered the processing; the
            // managers interact through state set by network *responses*,
            // not by `poll(...)` itself, so this is order-faithfulness, not
            // a behavior change).
            let mut rm_guard = match self.request_managers.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let coord_handle = rm_guard.coordinator_handle();
            let commit_handle = rm_guard.commit_handle();
            if let Some(coord_arc) = coord_handle.as_ref() {
                collected_before.push(coord_arc.poll_shared(current_time_ms));
            }
            // Java: `org.apache.kafka.clients.consumer.internals.CommitRequestManager#poll`. Must run between
            // coordinator and heartbeat so unsent commits/fetches are
            // shipped on every iteration. Both `CoordinatorRequestManager`
            // and `CommitRequestManager` use interior mutability
            // (`Arc<...Inner>`), so `&self` is sufficient on both.
            if let (Some(coord_arc), Some(commit_arc)) = (coord_handle.as_ref(), commit_handle.as_ref()) {
                collected_before.push(commit_arc.poll_with_coordinator(coord_arc.as_ref(), current_time_ms));
            }
            // `entries()` ordering with the three Arc-shared slots and the
            // fetch slot skipped is `heartbeat → offsets → topic_metadata →
            // dyn`; fetch is polled right after (`dyn` is test-only and never
            // populated together with fetch).
            // `membership_boundary()` returns the heartbeat count (0 or 1):
            // the leading `boundary` results join the before-batch (they are
            // processed before `membership.reconcile`); the rest join the
            // after-batch (processed in Phase 2.6, observing post-reconcile
            // subscription-state updates within the SAME iteration — Java's
            // invariant).
            let boundary = rm_guard.membership_boundary();
            for (i, rm) in rm_guard.entries().into_iter().enumerate() {
                let result = rm.poll(current_time_ms);
                if i < boundary {
                    collected_before.push(result);
                } else {
                    collected_after.push(result);
                }
            }
            // Java: `FetchRequestManager.poll(currentTimeMs)`, whose
            // `prepareFetchRequests` asks the delegate `isUnavailable(node)` /
            // `maybeThrowAuthFailure(node)`. The delegate is the one this
            // iteration already holds (`delegate_guard`), so it is lent to the
            // fetch manager rather than locked again. Fetch follows the
            // membership boundary, so it joins the after-batch.
            if let Some(fetch) = rm_guard.fetch.as_mut() {
                collected_after.push(fetch.poll(&*delegate_guard, current_time_ms));
            }
        }
        for mut poll_result in collected_before.drain(..) {
            // Drain the try_connect slot BEFORE add_all_from_poll_result,
            // mirroring Java's tryConnect-then-addAll order inside the
            // manager body. `std::mem::take` swaps in an empty Vec
            // so `add_all_from_poll_result` later drops only an
            // empty `try_connect`.
            let try_connect_nodes = std::mem::take(&mut poll_result.try_connect);
            for node in try_connect_nodes {
                delegate_guard.try_connect(&node, current_time_ms).await;
            }
            let timeout_ms = delegate_guard.add_all_from_poll_result(poll_result, current_time_ms);
            poll_wait_time_ms = poll_wait_time_ms.min(timeout_ms);
        }

        // ──── Phase 2.4: drive a pending §31 assignment release ────
        //
        // The fence / fatal / stale transitions themselves are applied by
        // the heartbeat manager inline in its response handler, where Java
        // applies them (`org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`
        // and `AbstractHeartbeatRequestManager#handleFatalFailure`, synchronously inside the `whenComplete` lambda).
        // What CAN'T be finished there is the §31 `onPartitionsLost` callback each one
        // enqueues: the app task runs the listener and acks, so the release
        // tail resumes here, mirroring Java's
        // `signalPartitionsLost(...).whenComplete(...)` resuming on the
        // network thread.
        if let Some(membership) = self.membership.as_ref() {
            // Phase 41 (Issue 2): an `onPartitionsLost` release callback fired
            // by a fence/fatal/stale transition may still be awaiting its
            // app-side ack. `has_pending_release()` is a single lock-free
            // atomic load; in steady state (no release in flight) it is
            // `false` and this whole block is skipped — Perf Contract item 1
            // adds exactly one atomic load per iteration here.
            if membership.has_pending_release() {
                // Drive it non-blockingly (Java's
                // `callbackResult.whenComplete(...)` resuming on the network
                // thread).
                if let Err(e) = membership.drive_pending_release().await {
                    log::warn!("drive_pending_release failed: {}", e);
                }
            }
        }

        // ──── Phase 2.5: drive membership.reconcile per iteration ────
        //
        // Java's `AbstractMembershipManager.poll(now)` body is
        // `maybeReconcile(false); return EMPTY;`. Pass
        // `can_commit = false`: this is the per-iteration call where
        // we have NOT just run `updateTimerAndMaybeCommit`, so it is
        // not safe to advance reconciliation when auto-commit is
        // enabled (Java's `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeReconcile`).
        //
        // Failures are logged and swallowed, matching Java's
        // surrounding-runOnce `try { ... } catch (Throwable e) { log }`.
        if let Some(membership) = self.membership.as_ref()
            && let Err(e) = membership.reconcile(current_time_ms, false).await
        {
            log::warn!("Membership reconcile failed: {}", e);
        }

        // ──── Phase 2.6: poll after-membership managers (offsets,
        // topic_metadata, dyn, fetch) ────
        for mut poll_result in collected_after.drain(..) {
            let try_connect_nodes = std::mem::take(&mut poll_result.try_connect);
            for node in try_connect_nodes {
                delegate_guard.try_connect(&node, current_time_ms).await;
            }
            let timeout_ms = delegate_guard.add_all_from_poll_result(poll_result, current_time_ms);
            poll_wait_time_ms = poll_wait_time_ms.min(timeout_ms);
        }
        // Restore the (now empty) scratch buffers for the next iteration —
        // they retain their capacity, so steady state allocates nothing.
        self.poll_results_before_scratch = collected_before;
        self.poll_results_after_scratch = collected_after;

        // ──── Phase 4: poll the network client ────
        //
        // The network poll performs connection setup (`initiate_connect`) and
        // I/O. It must NOT be cancelled mid-flight: a `tokio::select!` that
        // races it against the wakeup signals would drop the poll future at an
        // `.await` — e.g. right after `connection_states.connecting()` marked a
        // node `Connecting` but before the socket is created — permanently
        // stranding that node (CLAUDE.md §11.6.1; full analysis in
        // `design/current/consumer-join-stall-rootcause.md`).
        //
        // Instead we run the poll to completion and deliver wakeups the way
        // Java does: everything that wants to interrupt the poll pokes the
        // selector's own wakeup primitive, and the poll returns at a safe
        // boundary. That primitive is `KafkaClient::wakeup_handle()` — an
        // `Arc<Notify>` the app side holds a clone of — so this phase is a
        // plain `.await` with nothing to race:
        //
        //   * `ApplicationEventHandler::add` pokes it after enqueuing
        //     (Java: `add()` -> `wakeupNetworkThread()` ->
        //     `networkClientDelegate.wakeup()` -> `Selector.wakeup()`);
        //   * the §31 rebalance-ack path and `FetchRequestManager`'s completion
        //     signal poke the same handle;
        //   * `wakeup()` pokes it alongside cancelling the user-facing token,
        //     and `signal_close()` pokes it after clearing the running flag.
        //
        // `notify_one()` stores a permit when nobody is parked, so a poke that
        // lands in the gap between the top-of-loop drain and this await still
        // returns the poll immediately — the same semantics as a
        // `Selector.wakeup()` issued just before `select()` (see
        // `selector.rs`, and `CountingClient::poll` in this file's tests,
        // which awaits the handle it hands out).
        //
        // There used to be a `select!` here with two extra arms — one on the
        // wakeup token, one on a separate application-event `Notify` — whose
        // bodies both did nothing but forward the signal to this same handle.
        // They were pure indirection, and being able to poke the wrong one of
        // three primitives is what produced several silent-latency bugs.
        delegate_guard.poll_default(poll_wait_time_ms, current_time_ms).await;

        // ──── Phase 5: refresh cached maximumTimeToWait ────
        //
        // Java takes the minimum over `requestManagers.entries()`, which
        // starts `coordinator → commit → ...`. `RequestManagers::entries()`
        // skips those two `Arc`-shared slots (see Phase 2), so they are
        // consulted here explicitly, as Phase 2 polls them explicitly. The
        // commit manager is the one that matters: its auto-commit timer bounds
        // the application task's wait so `poll()` submits the event that sends
        // the auto-commit on time (and, KAFKA-20970 / KAFKA-21010, it returns
        // the retry backoff rather than an expired timer's 0 while no
        // coordinator is known). The coordinator manager keeps the trait
        // default, `i64::MAX`, as in Java. (Before Milestone 17 Phase 3 the
        // commit manager was never consulted.)
        let mut max_time_to_wait_ms: i64 = i64::MAX;
        {
            let mut rm_guard = match self.request_managers.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if let Some(coordinator) = rm_guard.coordinator_handle() {
                max_time_to_wait_ms = max_time_to_wait_ms
                    .min(RequestManager::maximum_time_to_wait(coordinator.as_ref(), current_time_ms));
            }
            if let Some(commit) = rm_guard.commit_handle() {
                max_time_to_wait_ms =
                    max_time_to_wait_ms.min(RequestManager::maximum_time_to_wait(commit.as_ref(), current_time_ms));
            }
            for rm in rm_guard.entries() {
                let wait_ms = rm.maximum_time_to_wait(current_time_ms);
                max_time_to_wait_ms = max_time_to_wait_ms.min(wait_ms);
            }
            // The fetch manager is outside `entries()` (Phase 2 polls it with
            // the delegate), so it is consulted here explicitly.
            if let Some(fetch) = rm_guard.fetch.as_ref() {
                max_time_to_wait_ms = max_time_to_wait_ms.min(fetch.maximum_time_to_wait(current_time_ms));
            }
        }
        self.cached_max_time_to_wait_ms.store(max_time_to_wait_ms, Ordering::Release);

        // ──── Phase 6: reap expired application events ────
        // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#reapExpiredApplicationEvents` —
        // `recordApplicationEventExpiredSize(reaper.reap(now))`, recorded unconditionally (the Value stat tracks the
        // latest count, which is 0 when nothing expired).
        let expired = {
            let mut reaper = match self.application_event_reaper.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            reaper.reap(current_time_ms)
        };
        if let Some(metrics) = &self.async_consumer_metrics {
            metrics.record_application_event_expired_size(expired as i64);
        }

        // ──── Phase 7: maybeFailOnMetadataError(uncompletedEvents) ────
        //
        // Java: `List<CompletableEvent<?>> uncompletedEvents =
        // applicationEventReaper.uncompletedEvents();
        // maybeFailOnMetadataError(uncompletedEvents);`
        //
        // The Rust reaper holds erased handles only — we cannot match
        // against `MetadataErrorNotifiableEvent` after the fact. Instead
        // we track notifiable+completable events in `notifiable_handles`
        // during `process_application_events` and iterate that list
        // here. Done handles are pruned in-place. See module docstring.
        self.maybe_fail_on_metadata_error_uncompleted(&mut delegate_guard);
    }

    /// Mirrors Java's `maybeFailOnMetadataError(List<?> events)` invoked
    /// with `applicationEventReaper.uncompletedEvents()`. In Rust we
    /// iterate the pre-filtered `notifiable_handles` list (intersection
    /// of "notifiable" and "completable") and prune done entries.
    ///
    /// Behavior parity points with Java's `maybeFailOnMetadataError`:
    ///
    ///   - If the filtered list (here: live, not-done handles) is
    ///     empty, do NOT consume the delegate's metadata error
    ///     (`getAndClearMetadataError`). Java has the same "optimisation"
    ///     guard.
    ///   - If a metadata error IS present, call
    ///     `complete_with_error(err)` on every live handle. Java calls
    ///     `e.onMetadataError(metadataError.get())` which for these
    ///     four variants resolves to `handle.completeExceptionally(...)`
    ///     — exactly what `complete_with_error` does.
    ///
    /// Called by `run_once` after the reap step, passing the iteration's
    /// delegate guard (Phase 27 Fix #1: `run_once` holds a single guard
    /// for the whole iteration instead of re-locking per phase).
    fn maybe_fail_on_metadata_error_uncompleted(&mut self, delegate: &mut NetworkClientDelegate<K>) {
        // Step 1: drop any handle that completed since the last call.
        self.notifiable_handles.retain(|h| !h.is_done());
        if self.notifiable_handles.is_empty() {
            // Java: "Don't get-and-clear the metadata error if there are
            // no events that will be notified."
            // (`org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#maybeFailOnMetadataError`).
            return;
        }

        // Step 2: query the delegate for a pending metadata error.
        let err_opt = delegate.get_and_clear_metadata_error();

        let Some(err) = err_opt else {
            return;
        };

        // Step 3: notify every live handle. Error is Clone so we
        // can fan it out faithfully (Java passes the same exception
        // instance to each `onMetadataError` call).
        for handle in &self.notifiable_handles {
            // Java: `e.onMetadataError(metadataError.get())` resolves
            // to `handle.completeExceptionally(metadataError)` for each
            // of the four notifiable+completable variants. Our erased
            // handle's `complete_with_error(err)` calls
            // `tx.send(Err(err))` on the inner oneshot — identical
            // semantics.
            handle.complete_with_error(err.clone());
        }
        // The handles will be pruned on the next iteration's
        // `retain(!is_done)` pass.
    }

    /// Drain and dispatch every application event currently in the
    /// channel. Matches Java's `processApplicationEvents`:
    ///
    /// 1. `LinkedList<ApplicationEvent> events = new LinkedList<>(); drainTo(events);`
    /// 2. For each event:
    ///    - if `instanceof CompletableEvent`, register with the reaper;
    ///    - if `instanceof MetadataErrorNotifiableEvent`, run
    ///      `maybeFailOnMetadataError(List.of(event))`; if true,
    ///      `continue` (do NOT process);
    ///    - else `applicationEventProcessor.process(event)`.
    ///
    /// We use `try_recv` in a `while let` loop instead of `recv().await`
    /// to mirror Java's `drainTo` (`consumer-threading.md` §10).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#processApplicationEvents")]
    fn process_application_events(&mut self) {
        // Phase 28: drain into a scratch buffer reused across iterations
        // (taken/restored so `&mut self` stays available to the dispatch
        // body). Java drains into a fresh LinkedList the GC nursery
        // absorbs; a heap Vec per iteration was the Rust translation
        // artifact. Buffering before dispatch (rather than dispatching
        // straight out of `try_recv`) is behavior Java relies on: events
        // enqueued DURING dispatch wait for the next iteration.
        let mut envelopes = std::mem::take(&mut self.app_event_drain_scratch);
        debug_assert!(envelopes.is_empty());
        while let Ok(env) = self.application_event_rx.try_recv() {
            envelopes.push(env);
        }
        if envelopes.is_empty() {
            self.app_event_drain_scratch = envelopes;
            return;
        }

        // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#processApplicationEvents` records a
        // literal 0 here, which is exact for Java because `LinkedBlockingQueue.drainTo` calls `fullyLock()` — it holds
        // both the put and take locks, so nothing can arrive during the drain.
        //
        // The `try_recv` loop above (mandated by consumer-threading.md §10) does
        // NOT block senders, so a `store(0)` would be wrong: an `add` racing the
        // loop has its `fetch_add(1)` overwritten even though its event is still
        // queued, and the gauge then under-reports until the next drain.
        //
        // Subtract exactly what was drained instead. The counter is then
        // conserved — `+1` per successful send in `ApplicationEventHandler::add`,
        // `-1` per event actually dequeued here — so it equals the true depth at
        // every observation point regardless of interleaving, and can never go
        // negative. Recording the post-drain value rather than 0 reports events
        // that arrived mid-loop, which is what Java's `0` means when its drain
        // really did empty the queue.
        //
        // Clone the metrics `Arc` up front so `&mut self` stays available to the
        // dispatch body below.
        let metrics = self.async_consumer_metrics.clone();
        if let Some(metrics) = &metrics {
            let remaining = match &self.application_event_queue_size {
                Some(queue_size) => {
                    queue_size.fetch_sub(envelopes.len() as i64, Ordering::SeqCst) - envelopes.len() as i64
                },
                None => 0,
            };
            // Floor at 0: a negative depth is never meaningful, and publishing one
            // would turn a pairing bug into a nonsense gauge. Production cannot go
            // negative -- `ApplicationEventHandler::add` is the only sender -- but a
            // test pushing onto the raw channel can.
            metrics.record_application_event_queue_size(remaining.max(0) as i32);
        }
        // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#processApplicationEvents` — measure
        // the time to process all available events.
        let start_ms = self.time.milliseconds();

        for env in envelopes.drain(..) {
            // A leave's callback ack comes back inside a
            // `ConsumerRebalanceListenerCallbackCompleted` event in this queue,
            // as Java's `ConsumerRebalanceListenerCallbackCompletedEvent` does,
            // and processing that event runs the leave's `whenComplete` ->
            // `clearAssignmentAndLeaveGroup()`. So the leave takes effect after
            // every event enqueued before the ack — including one the listener
            // submitted through its `ConsumerHandle` and stopped waiting for,
            // which can still be queued here — and before every event enqueued
            // after it (e.g. a `subscribe()` right after an `unsubscribe()`
            // that timed out during the listener).
            //
            // This check only fires for an ack dropped with no completed event
            // to carry it: the callback event itself was dropped before the app
            // ran the listener (e.g. the consumer was dropped with it still in
            // its background-event queue), or processing the completed event
            // panicked before completing the future (caught by the boundary
            // below). A caller future dropped mid-listener does NOT get here:
            // `PartitionsRemovedAckGuard` still enqueues the completed event,
            // with an interrupt error, behind the listener's ops. The closed
            // ack is applied before the next event, so the leave cannot hang.
            self.maybe_apply_leave_callback_ack();
            // Java `ConsumerNetworkThread#processApplicationEvents`: record the
            // time this event spent in the queue.
            if let Some(metrics) = &metrics {
                metrics.record_application_event_queue_time(self.time.milliseconds() - env.enqueued_ms);
            }
            // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#processApplicationEvents` wraps
            // each event in `try { ... } catch (Throwable t)`, so one failing event neither skips the rest of the
            // batch nor stops the network thread. Rust's processor is
            // infallible by signature (`fn process(&mut self, event)`): it
            // completes each event's handle itself, so what Java's catch-all
            // guards against, a throw out of the body, is a panic here.
            // KAFKA-18812: log it at ERROR and complete the event's future with
            // the failure, rather than leaving the caller to time out on the
            // reaper. The handle is taken before the boundary because the body
            // consumes the event.
            //
            // `AssertUnwindSafe` because `&mut self` is not `UnwindSafe`. What a
            // caught unwind leaves behind is what Java's thread holds after its
            // own catch (the `Sender` and `NetworkClient::complete_responses`
            // use the same idiom).
            let completable = env.event.erased_handle();
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.process_application_event(env, completable.clone())
            })) {
                let message = panic_payload_message(&*payload);
                log::error!("Error processing event {message}");
                if let Some(erased) = completable {
                    erased.complete_with_error(crate::common::Error::kafka_message(message));
                }
            }
        }
        // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#processApplicationEvents` — record
        // the total processing time for the batch.
        if let Some(metrics) = &metrics {
            metrics.record_application_event_queue_processing_time(self.time.milliseconds() - start_ms);
        }
        // Restore the (drained) scratch buffer; capacity retained.
        self.app_event_drain_scratch = envelopes;
    }

    /// Apply a stored unsubscribe-leave callback ack, if it has arrived
    /// (`ConsumerMembershipManager::drive_pending_leave_callback`). The app
    /// returns that ack through the application-event queue, whose processing
    /// applies it, so here it can only be one dropped with no completed event
    /// to carry it. One atomic load when no leave callback is pending; no
    /// allocation.
    fn maybe_apply_leave_callback_ack(&self) {
        if let Some(membership) = self.membership.as_ref()
            && membership.has_pending_leave_callback()
            && let Err(e) = membership.drive_pending_leave_callback()
        {
            log::warn!("Failed to transition to send the leave group heartbeat: {}", e);
        }
    }

    /// The body of Java's per-event `try` block in `processApplicationEvents`:
    /// register with the reaper, short-circuit on a pending metadata error,
    /// then dispatch. [`Self::process_application_events`] runs it under the
    /// per-event panic boundary, which takes `completable` (the event's
    /// [`ApplicationEvent::erased_handle`]) before the call so it can still
    /// complete the event after a panic.
    fn process_application_event(
        &mut self,
        env: ApplicationEventEnvelope,
        completable: Option<Arc<dyn super::events::CompletableEventErasedHandle>>,
    ) {
        // 1. Register with the reaper if completable. The Java
        // `CompletableEvent` interface check is replaced by the
        // `erased_handle()` accessor on [`ApplicationEvent`].
        if let Some(erased) = completable {
            let mut reaper = match self.application_event_reaper.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            reaper.add(erased);
        }

        // 1b. Track notifiable+completable events in the parallel
        // list so the post-poll `maybeFailOnMetadataError` arm can
        // observe them. Java derives this list from
        // `applicationEventReaper.uncompletedEvents()` filtered for
        // `MetadataErrorNotifiableEvent`; in Rust the reaper holds
        // erased handles only so we keep a side list. See module
        // docstring for the rationale.
        if let Some(notifiable) = env.event.metadata_error_notifiable_handle() {
            self.notifiable_handles.push(notifiable);
        }

        // 2. Metadata-error short-circuit. Java's
        // `maybeFailOnMetadataError` queries the delegate's
        // `getAndClearMetadataError()`; we mirror that here so the
        // error is consumed regardless of whether THIS event was
        // notifiable. Java keeps the same "don't get-and-clear if
        // no notifiable events" optimisation.
        //
        // We hold the delegate guard briefly with `try_lock` because
        // the bg task is the sole holder. No `.await` while held.
        let event_was_notified = if env.event.is_metadata_error_notifiable() {
            let mut delegate_guard = self
                .network_client_delegate
                .try_lock()
                .expect("delegate not contended on bg task");
            if let Some(err) = delegate_guard.get_and_clear_metadata_error() {
                env.event.on_metadata_error(err)
            } else {
                false
            }
        } else {
            false
        };
        if event_was_notified {
            return;
        }

        // 3. Normal dispatch.
        self.application_event_processor.process(env.event);
    }

    /// Test-only helper that pushes an erased handle directly onto
    /// `notifiable_handles`, bypassing `process_application_events`.
    /// Used to test the post-poll metadata-error arm in isolation
    /// from AEP dispatch (which may synchronously complete the handle
    /// in some variants).
    #[cfg(test)]
    pub(crate) fn push_notifiable_handle_for_test(
        &mut self,
        handle: Arc<dyn super::events::CompletableEventErasedHandle>,
    ) {
        self.notifiable_handles.push(handle);
    }

    /// Mirror of Java's `cleanup()`. Runs the close-side request
    /// managers, drains any remaining unsent requests for up to the
    /// close-timeout, reaps any in-flight completable events, and closes
    /// the managers / delegate.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#cleanup")]
    pub(crate) async fn cleanup(&mut self) {
        log::trace!("Closing the consumer network thread");
        let close_timeout_ms = self.close_timeout_ms.load(Ordering::Acquire);
        let close_deadline_ms = self.time.milliseconds().saturating_add(close_timeout_ms);

        // ──── 1. pollOnClose round ────
        //
        // Java's `cleanup` iterates `requestManagers.entries()` and calls
        // `pollOnClose` on each, in the order `coordinator → commit →
        // heartbeat → membership → offsets → topic metadata → fetch`
        // (`RequestManagers` constructor). The Rust `entries()` skips
        // coordinator, commit and fetch; the bg-task drives them explicitly,
        // at their place in that order:
        //   * coordinator has no close-side work (Java's
        //     `CoordinatorRequestManager.pollOnClose` returns EMPTY).
        //   * commit drains pending offset-commit requests via
        //     `drain_pending_offset_commit_requests()`. Java 4.4's
        //     `CommitRequestManager` inherits the EMPTY `pollOnClose`; its
        //     close-time drain is `poll()` while `closing`
        //     (`drainPendingOffsetCommitRequests`), which runs in `runOnce`
        //     before `cleanup`, so a pending `OffsetCommit` is always queued
        //     ahead of everything `runAtClose` builds. This sweep goes first
        //     for the same reason: the commit precedes the leave-group
        //     heartbeat and the fetch-session close requests, also when the
        //     coordinator leads a fetched partition.
        //   * fetch builds the fetch-session close requests last.
        //
        // Java 4.4 `ConsumerNetworkThread#cleanup` wraps `runAtClose(...)` in `try { ... } catch
        // (Exception e) { log.error("Unexpected error during shutdown. Proceed
        // with closing.", e); } finally { ... }`, so a failure here still
        // sends the unsent requests, reaps, and closes the managers and the
        // delegate (steps 2-4). The round is sync, so `catch_unwind` covers
        // it; the unwind drops the delegate guard, and the request-managers
        // lock it may poison is taken through `into_inner` below.
        let current_time_ms = self.time.milliseconds();
        let poll_on_close_round = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let commit_on_close = {
                let rm_guard = match self.request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                rm_guard.commit_handle()
            };
            {
                let mut rm_guard = match self.request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                let mut delegate_guard = self
                    .network_client_delegate
                    .try_lock()
                    .expect("delegate not contended on bg task");
                // Commit's close-side drain (Java `CommitRequestManager.poll` while
                // `closing` → `drainPendingOffsetCommitRequests`), ahead of the
                // `entries()` walk as the commit manager is in Java's.
                if let Some(commit_arc) = commit_on_close.as_ref() {
                    let result = commit_arc.drain_pending_offset_commit_requests();
                    let _ = delegate_guard.add_all_from_poll_result(result, current_time_ms);
                }
                for rm in rm_guard.entries() {
                    let result = rm.poll_on_close(current_time_ms);
                    let _ = delegate_guard.add_all_from_poll_result(result, current_time_ms);
                }
                // Java: `FetchRequestManager.pollOnClose(currentTimeMs)`, which
                // skips close-session requests to unavailable nodes; it reads the
                // delegate held here (`entries()` skips the fetch slot).
                if let Some(fetch) = rm_guard.fetch.as_mut() {
                    let result = fetch.poll_on_close(&*delegate_guard, current_time_ms);
                    let _ = delegate_guard.add_all_from_poll_result(result, current_time_ms);
                }
            }
        }));
        if let Err(payload) = poll_on_close_round {
            log::error!(
                "Unexpected error during shutdown. Proceed with closing. {}",
                panic_payload_message(&*payload)
            );
        }

        // ──── 2. Drain unsent requests until timer expires ────
        //
        // Java's `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#sendUnsentRequests`
        // returns early only when nothing is pending; otherwise it is a
        // do-while that ALWAYS polls once — a non-blocking `poll(0)` when the
        // close timer is already spent — before checking
        // `timer.notExpired() && hasAnyPendingRequests()`. That first pass is
        // what sends the leave-group heartbeat `pollOnClose` builds for a
        // close with zero timeout
        // (`org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#pollOnClose`), so the timer
        // check must not precede it.
        let has_pending = |thread: &Self| {
            thread
                .network_client_delegate
                .try_lock()
                .expect("delegate not contended on bg task")
                .has_any_pending_requests()
        };
        if has_pending(self) {
            loop {
                let now = self.time.milliseconds();
                let remaining = close_deadline_ms.saturating_sub(now).max(0);
                {
                    let mut delegate_guard = self.network_client_delegate.lock().await;
                    delegate_guard.poll_on_close(remaining, now).await;
                }
                // `while (timer.notExpired() && hasAnyPendingRequests())` —
                // short-circuits, so pending is not read once expired.
                if close_deadline_ms.saturating_sub(self.time.milliseconds()) <= 0 || !has_pending(self) {
                    break;
                }
            }
        }
        {
            let delegate_guard = self
                .network_client_delegate
                .try_lock()
                .expect("delegate not contended on bg task");
            if delegate_guard.has_any_pending_requests() {
                log::warn!(
                    "Close timeout of {} ms expired before the consumer network thread was able to \
                     complete pending requests. Inflight request count: {}, Unsent request count: {}",
                    close_timeout_ms,
                    delegate_guard.inflight_request_count(),
                    delegate_guard.unsent_requests().len()
                );
            }
        }

        // ──── 3. Reap any remaining completable events ────
        //
        // Java: `applicationEventReaper.reap(applicationEventQueue)` —
        // drains the queue AND any remaining tracked handles. The Rust
        // reaper signature is `reap_on_close(unprocessed_events: &mut
        // Vec<...>)`; we drain the channel here into an erased vec.
        let mut leftover_envelopes: Vec<ApplicationEventEnvelope> = Vec::new();
        while let Ok(env) = self.application_event_rx.try_recv() {
            leftover_envelopes.push(env);
        }
        let mut leftover_erased: Vec<Arc<dyn super::events::CompletableEventErasedHandle>> = Vec::new();
        for env in leftover_envelopes {
            if let Some(erased) = env.event.erased_handle() {
                leftover_erased.push(erased);
            }
        }
        let expired = {
            let mut reaper = match self.application_event_reaper.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            reaper.reap_on_close(&mut leftover_erased)
        };
        // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#cleanup` — record the expired count
        // during close, same as run_once's reap site.
        if let Some(metrics) = &self.async_consumer_metrics {
            metrics.record_application_event_expired_size(expired as i64);
        }

        // ──── 4. Close managers + delegate ────
        {
            let mut rm_guard = match self.request_managers.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            rm_guard.close();
        }
        // Delegate close — Java: `closeQuietly(networkClientDelegate)`.
        let mut delegate_guard = self.network_client_delegate.lock().await;
        if let Err(e) = delegate_guard.close().await {
            log::warn!("Error closing network client delegate: {}", e);
        }
        log::debug!("Closed the consumer network thread");
    }

    /// Java: `run()`. Wraps `run_once` in a loop guarded by `running`
    /// and the wakeup-token shutdown signal, and calls `cleanup` on
    /// exit. Production runs it through [`Self::spawn_dedicated`], on the
    /// `kafka-consumer-io` OS thread's single-thread runtime.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#run")]
    pub(crate) async fn run(mut self) {
        use futures_util::FutureExt;

        log::debug!("Consumer network thread started");
        while self.is_running() {
            // Re-check the shutdown signal first so `signal_close()`
            // followed by `wakeup.wakeup()` exits immediately.
            //
            // Java `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#run`:
            // `try { runOnce(); } catch (final Throwable e)
            // { log.error(...) }` — swallow and continue, so a failing
            // iteration neither ends the task nor skips `cleanup()`. Without
            // this boundary a panic would unwind out of `block_on` on the
            // dedicated thread and end that OS thread (and the consumer's bg
            // loop with it), not Java's swallow-and-continue.
            if let Err(payload) = std::panic::AssertUnwindSafe(self.run_once()).catch_unwind().await {
                log::error!(
                    "Unexpected error caught in consumer network task: {}",
                    panic_payload_message(&*payload)
                );
                // A Java thread that keeps failing spins on its own thread. A
                // Tokio task that panics before its first real `.await` never
                // returns control to the scheduler, which would starve the
                // worker (and hang a current-thread runtime, including the
                // `close()` waiting on this task). Yield so a repeated failure
                // stays cooperative.
                tokio::task::yield_now().await;
            }
        }
        // Java 4.4 `ConsumerNetworkThread#run`: the `finally { cleanup(); }` sits outside the
        // `catch (Throwable)`, so a failure in `cleanup()` leaves `run()` and
        // reaches `KafkaThread`'s uncaught-exception handler, which logs it at
        // ERROR (`KafkaThread#configureThread`); the thread then ends normally and
        // `ConsumerNetworkThread#closeInternal`'s `join()` returns, so `close()` does
        // not fail. Catch the panic here for the same outcome: log it, return
        // normally, and let `spawn_dedicated` signal `done`. Without the catch
        // the panic unwinds out of `block_on`, skips `done`, and only reaches
        // stderr through the default panic hook.
        if let Err(payload) = std::panic::AssertUnwindSafe(self.cleanup()).catch_unwind().await {
            log::error!("Uncaught error in consumer network task: {}", panic_payload_message(&*payload));
        }
    }

    /// Runs [`Self::run`] on a dedicated `std::thread` named
    /// `kafka-consumer-io` that hosts its own `current_thread` Tokio runtime,
    /// and returns the receiver that fires once `run` (loop plus `cleanup`)
    /// has returned, together with the OS thread handle.
    ///
    /// The Rust analog of Java starting this `KafkaThread` subclass, whose
    /// body is `run()`. Production and tests both go through `run`, so the
    /// per-iteration catch of `ConsumerNetworkThread.run()` applies to the
    /// production loop too.
    pub(crate) fn spawn_dedicated(self) -> (tokio::sync::oneshot::Receiver<()>, std::thread::JoinHandle<()>) {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let thread_handle = std::thread::Builder::new()
            .name("kafka-consumer-io".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build consumer io runtime");
                rt.block_on(self.run());
                // Signal the close path that the bg loop + `cleanup()`
                // have finished. Errors (receiver dropped before close)
                // are benign — `await_join` treats them as a clean exit.
                let _ = done_tx.send(());
            })
            .expect("spawn consumer io thread");
        (done_rx, thread_handle)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, AtomicUsize};
    use std::time::Duration;
    use tokio::sync::Notify;

    use crate::ApiVersions;
    use crate::MockClient;
    use crate::common::Error;
    use crate::common::IsolationLevel;
    use crate::common::Node;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::GroupMembershipOperation;
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use crate::consumer::internals::ConsumerMembershipManager;
    use crate::consumer::internals::ConsumerMetadata;
    use crate::consumer::internals::OffsetsRequestManager;
    use crate::consumer::internals::RequestManager;
    use crate::consumer::internals::RequestManagers;
    use crate::consumer::internals::SubscriptionState;
    use crate::consumer::internals::events::ApplicationEventProcessor;
    use crate::consumer::internals::events::BackgroundEventHandler;
    use crate::consumer::internals::events::CompletableEvent;
    use crate::consumer::internals::events::CompletableEventReaper;
    use crate::consumer::internals::events::{ApplicationEvent, ApplicationEventEnvelope};
    use crate::consumer::internals::{NetworkClientDelegate, PollResult};

    /// A concrete instantiation used purely to name the associated constants.
    /// They do not depend on `K`, but a generic type cannot infer it (E0282).
    type NetThread = ConsumerNetworkThread<CountingClient>;

    use super::*;

    use crate::common::utils::MockTime;

    /// Java's `new MockTime(0, start, 0)`: no auto-tick, the wall clock at
    /// `start_ms`.
    fn mock_time(start_ms: i64) -> MockTime {
        MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, start_ms, 0)
    }

    /// `RequestManager` spy used to replace Mockito's
    /// `mock(RequestManager.class)` in the Java tests. Counts calls and
    /// returns scripted values for [`Self::poll`] and
    /// [`Self::maximum_time_to_wait`].
    ///
    /// Designed so each instance can be passed via
    /// [`RequestManagers::with_dyn_managers`] and inspected after a
    /// `run_once` invocation via the shared `Arc<AtomicUsize>` counters
    /// (cloned out of the spy before move-into-the-vec).
    struct SpyRequestManager {
        poll_calls: Arc<AtomicUsize>,
        max_wait_calls: Arc<AtomicUsize>,
        poll_on_close_calls: Arc<AtomicUsize>,
        /// `time_until_next_poll_ms` returned from each `poll(...)` call.
        poll_return_ms: i64,
        /// `maximum_time_to_wait(...)` return value.
        max_wait_return_ms: i64,
    }

    impl SpyRequestManager {
        fn new(poll_return_ms: i64, max_wait_return_ms: i64) -> Self {
            Self {
                poll_calls: Arc::new(AtomicUsize::new(0)),
                max_wait_calls: Arc::new(AtomicUsize::new(0)),
                poll_on_close_calls: Arc::new(AtomicUsize::new(0)),
                poll_return_ms,
                max_wait_return_ms,
            }
        }

        fn poll_calls(&self) -> Arc<AtomicUsize> {
            self.poll_calls.clone()
        }
        fn max_wait_calls(&self) -> Arc<AtomicUsize> {
            self.max_wait_calls.clone()
        }
        fn poll_on_close_calls(&self) -> Arc<AtomicUsize> {
            self.poll_on_close_calls.clone()
        }
    }

    impl RequestManager for SpyRequestManager {
        fn poll(&mut self, _current_time_ms: i64) -> PollResult {
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            PollResult::with_time_until_next_poll_ms(self.poll_return_ms)
        }

        fn maximum_time_to_wait(&self, _current_time_ms: i64) -> i64 {
            self.max_wait_calls.fetch_add(1, Ordering::SeqCst);
            self.max_wait_return_ms
        }

        fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
            self.poll_on_close_calls.fetch_add(1, Ordering::SeqCst);
            PollResult::empty()
        }
    }

    /// `KafkaClient` wrapper that delegates to an inner [`MockClient`]
    /// and records observable side effects needed by translated Java
    /// tests:
    ///
    /// - `poll_call_count`: how many times `poll(...)` was awaited
    ///   (replaces Mockito's
    ///   `verify(networkClientDelegate, times(N)).poll(...)`).
    /// - `poll_timeouts`: the timeout argument passed to each `poll(...)`
    ///   call. Mirrors Mockito's
    ///   `verify(networkClientDelegate).poll(eq(Math.min(...)), ...)`.
    /// - `has_in_flight_script`: a queue of return values for
    ///   `has_in_flight_requests()` — once exhausted, falls back to the
    ///   inner client's behavior. Mirrors Mockito's
    ///   `when(...).thenReturn(true).thenReturn(true).thenReturn(false)`.
    struct CountingClient {
        inner: MockClient,
        poll_call_count: Arc<AtomicUsize>,
        poll_timeouts: Arc<Mutex<Vec<i64>>>,
        has_in_flight_script: Arc<Mutex<VecDeque<bool>>>,
        /// When `true`, `poll(...)` parks on `poll_release` before
        /// delegating — simulates a real socket poll blocking on I/O
        /// readiness. Used by the application-event-notify regression test
        /// to prove the `run_once` `select!` preempts a blocked poll.
        poll_block: Arc<AtomicBool>,
        poll_release: Arc<Notify>,
        /// When `true`, `close()` panics — a failure in the last step of
        /// `cleanup()`, after the pollOnClose round.
        close_panics: Arc<AtomicBool>,
    }

    impl CountingClient {
        fn new(inner: MockClient) -> Self {
            Self {
                inner,
                poll_call_count: Arc::new(AtomicUsize::new(0)),
                poll_timeouts: Arc::new(Mutex::new(Vec::new())),
                has_in_flight_script: Arc::new(Mutex::new(VecDeque::new())),
                poll_block: Arc::new(AtomicBool::new(false)),
                poll_release: Arc::new(Notify::new()),
                close_panics: Arc::new(AtomicBool::new(false)),
            }
        }

        fn poll_call_count(&self) -> Arc<AtomicUsize> {
            self.poll_call_count.clone()
        }
        fn poll_timeouts(&self) -> Arc<Mutex<Vec<i64>>> {
            self.poll_timeouts.clone()
        }
        fn has_in_flight_script(&self) -> Arc<Mutex<VecDeque<bool>>> {
            self.has_in_flight_script.clone()
        }
        fn poll_block(&self) -> Arc<AtomicBool> {
            self.poll_block.clone()
        }
        fn close_panics(&self) -> Arc<AtomicBool> {
            self.close_panics.clone()
        }
    }

    impl crate::KafkaClient for CountingClient {
        fn is_ready(&self, node: &Node, now: i64) -> bool {
            self.inner.is_ready(node, now)
        }
        async fn ready(&mut self, node: &Node, now: i64) -> bool {
            self.inner.ready(node, now).await
        }
        fn connection_delay(&self, node: &Node, now: i64) -> i64 {
            self.inner.connection_delay(node, now)
        }
        fn poll_delay_ms(&self, node: &Node, now: i64) -> i64 {
            self.inner.poll_delay_ms(node, now)
        }
        fn connection_failed(&self, node: &Node) -> bool {
            self.inner.connection_failed(node)
        }
        fn authentication_error(&self, node: &Node) -> Option<Error> {
            self.inner.authentication_error(node)
        }
        fn send(&mut self, request: crate::ClientRequest, now: i64) {
            self.inner.send(request, now)
        }
        async fn poll(&mut self, timeout: i64, now: i64) -> Vec<crate::ClientResponse> {
            self.poll_call_count.fetch_add(1, Ordering::SeqCst);
            self.poll_timeouts.lock().unwrap().push(timeout);
            if self.poll_block.load(Ordering::SeqCst) {
                // Block until explicitly released — emulates a socket poll
                // waiting on I/O readiness. `wakeup_handle()` hands out this
                // same `Notify`, so poking the handle releases the poll exactly
                // as `Selector.wakeup()` does for the real client. That is what
                // the preempt regression test relies on.
                self.poll_release.notified().await;
            }
            self.inner.poll(timeout, now).await
        }
        async fn disconnect(&mut self, node_id: &str) {
            self.inner.disconnect(node_id).await
        }
        async fn close_connection(&mut self, node_id: &str) {
            self.inner.close_connection(node_id).await
        }
        fn least_loaded_node(&self, now: i64) -> crate::LeastLoadedNode {
            self.inner.least_loaded_node(now)
        }
        fn in_flight_request_count(&self) -> i32 {
            self.inner.in_flight_request_count()
        }
        fn has_in_flight_requests(&self) -> bool {
            // Pop the next scripted value if any, else delegate.
            let mut q = self.has_in_flight_script.lock().unwrap();
            if let Some(v) = q.pop_front() {
                return v;
            }
            self.inner.has_in_flight_requests()
        }
        fn in_flight_request_count_for_node(&self, node_id: &str) -> usize {
            self.inner.in_flight_request_count_for_node(node_id)
        }
        fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool {
            self.inner.has_in_flight_requests_for_node(node_id)
        }
        fn has_ready_nodes(&self, now: i64) -> bool {
            self.inner.has_ready_nodes(now)
        }
        fn wakeup(&self) {
            self.inner.wakeup();
            // Release a blocked poll, mirroring a real selector whose
            // `wakeup()` fires the same `Notify` its `poll()` awaits.
            self.poll_release.notify_one();
        }
        fn wakeup_handle(&self) -> Arc<Notify> {
            // The blocking `poll()` above awaits `poll_release`; hand that out
            // so the bg task's poke wakes it, just like the real selector.
            self.poll_release.clone()
        }
        fn wakeup_notify(&self) -> Arc<Notify> {
            self.poll_release.clone()
        }
        fn new_client_request(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn crate::common::requests::RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> crate::ClientRequest {
            self.inner
                .new_client_request(node_id, request_builder, created_time_ms, expect_response)
        }
        fn new_client_request_with_timeout(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn crate::common::requests::RequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<crate::RequestCompletionHandler>,
        ) -> crate::ClientRequest {
            self.inner.new_client_request_with_timeout(
                node_id,
                request_builder,
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }
        fn initiate_close(&self) {
            self.inner.initiate_close()
        }
        fn active(&self) -> bool {
            self.inner.active()
        }
        async fn close(&mut self) {
            if self.close_panics.load(Ordering::SeqCst) {
                panic!("client close failure");
            }
            self.inner.close().await
        }
    }

    fn make_config() -> ConsumerConfig {
        ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        }
    }

    fn make_metadata(config: &ConsumerConfig, subs: Arc<Mutex<SubscriptionState>>) -> Arc<ConsumerMetadata> {
        Arc::new(ConsumerMetadata::with_config(config, subs, ClusterResourceListeners::new()))
    }

    fn make_delegate(config: &ConsumerConfig, metadata: Arc<ConsumerMetadata>) -> NetworkClientDelegate<MockClient> {
        let client = MockClient::with_static_nodes(Vec::<Node>::new(), Arc::new(mock_time(0)));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let raw_metadata = metadata.metadata_arc();
        NetworkClientDelegate::new(config, client, raw_metadata, beh, false)
    }

    fn make_counting_delegate(
        config: &ConsumerConfig,
        metadata: Arc<ConsumerMetadata>,
    ) -> NetworkClientDelegate<CountingClient> {
        let client = CountingClient::new(MockClient::with_static_nodes(Vec::<Node>::new(), Arc::new(mock_time(0))));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let raw_metadata = metadata.metadata_arc();
        NetworkClientDelegate::new(config, client, raw_metadata, beh, false)
    }

    /// Test-fixture for tests that need to inject spy `RequestManager`s
    /// and observe `delegate.poll(...)` call counts. Mirrors Java's
    /// constructor-time wiring with `mock(NetworkClientDelegate.class)`
    /// and `mock(RequestManager.class)`.
    struct CountingFixture {
        thread: ConsumerNetworkThread<CountingClient>,
        time: Arc<MockTime>,
        delegate: Arc<AsyncMutex<NetworkClientDelegate<CountingClient>>>,
        reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
        tx: mpsc::UnboundedSender<ApplicationEventEnvelope>,
        // Direct handles to the SpyRequestManagers' counters & client
        // counters — populated by the caller via `with_spies(...)`.
    }

    /// Return value of [`make_thread_with_dyn_managers`]: fixture +
    /// handles to the inner `CountingClient`'s `poll` counter, timeout
    /// log, and `has_in_flight` script.
    type DynFixture = (
        CountingFixture,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<i64>>>,
        Arc<Mutex<VecDeque<bool>>>,
    );

    fn make_thread_with_dyn_managers(dyn_managers: Vec<Box<dyn RequestManager>>) -> DynFixture {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let request_managers = Arc::new(Mutex::new(RequestManagers::with_dyn_managers(dyn_managers)));
        let counting_delegate = make_counting_delegate(&config, metadata.clone());
        let poll_call_count = counting_delegate.client_for_test_ref().poll_call_count();
        let poll_timeouts = counting_delegate.client_for_test_ref().poll_timeouts();
        let has_in_flight_script = counting_delegate.client_for_test_ref().has_in_flight_script();
        let delegate = Arc::new(AsyncMutex::new(counting_delegate));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper.clone(),
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );
        (
            CountingFixture { thread, time, delegate, reaper, tx },
            poll_call_count,
            poll_timeouts,
            has_in_flight_script,
        )
    }
    /// An application event enqueued while the bg task is parked in the network
    /// poll must preempt that poll, rather than waiting out
    /// `MAX_POLL_TIMEOUT_MS`. Java: `add()` -> `wakeupNetworkThread()` ->
    /// `networkClientDelegate.wakeup()` -> `Selector.wakeup()`.
    ///
    /// The app side pokes the client's own wakeup handle
    /// (`KafkaClient::wakeup_handle()`), which is exactly what the poll awaits —
    /// so there is no `select!` arm in `run_once` to forward anything, and this
    /// test drives the same handle the production `ApplicationEventHandler`
    /// holds. (It previously poked a separate `event_notify` that a `select!`
    /// arm forwarded here; the arm was pure indirection and is gone.)
    ///
    /// `CountingClient` is put in blocking mode so its `poll(...)` never returns
    /// on its own, emulating a socket wait with no data — and, like the real
    /// selector, it awaits the handle it hands out. Without a working poke this
    /// hangs, which the `timeout` guard turns into a failure.
    ///
    /// The poke lands BEFORE `run_once` reaches the poll, so this also covers
    /// the stored-permit case: `notify_one()` with nobody parked must still
    /// return the next poll immediately (Java NIO's semantics for a
    /// `Selector.wakeup()` issued just before `select()`).
    #[tokio::test]
    async fn application_event_notify_preempts_blocking_network_poll() {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let request_managers = Arc::new(Mutex::new(RequestManagers::with_dyn_managers(Vec::new())));
        let counting_delegate = make_counting_delegate(&config, metadata.clone());
        // Make the network poll block forever to emulate a socket wait.
        counting_delegate
            .client_for_test_ref()
            .poll_block()
            .store(true, Ordering::SeqCst);
        // The one nudge primitive, taken from the delegate exactly as the
        // production ctor does.
        let event_notify = counting_delegate.wakeup_handle();
        let delegate = Arc::new(AsyncMutex::new(counting_delegate));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper.clone(),
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        // Mirror `ApplicationEventHandler::add`, which pokes this handle after
        // enqueuing. The permit is stored even though nothing is parked yet.
        event_notify.notify_one();

        tokio::time::timeout(Duration::from_secs(2), thread.run_once())
            .await
            .expect("run_once must be preempted by the application-event nudge, not block on the poll");
    }

    fn make_offsets_manager(
        config: &ConsumerConfig,
        subs: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
    ) -> OffsetsRequestManager {
        let positions_validator = Arc::new(crate::consumer::internals::PositionsValidator::new(
            subs.clone(),
            metadata.clone(),
        ));
        OffsetsRequestManager::new(
            subs,
            metadata,
            IsolationLevel::ReadUncommitted,
            Arc::new(crate::common::utils::SystemTime),
            100,
            config.request_timeout_ms() as i64,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
            positions_validator,
        )
    }

    /// Build a fully-wired `ConsumerNetworkThread` (without membership)
    /// for tests that do not exercise the rebalance path.
    /// Test-fixture tuple returned by [`make_thread_no_membership`]:
    /// the thread, the app-side event sender, the shared reaper, the
    /// mock time source, and the shared request-managers container.
    /// Aliased to satisfy clippy's `type_complexity` lint.
    type ThreadFixture = (
        ConsumerNetworkThread<MockClient>,
        mpsc::UnboundedSender<ApplicationEventEnvelope>,
        Arc<std::sync::Mutex<CompletableEventReaper>>,
        Arc<MockTime>,
        Arc<std::sync::Mutex<RequestManagers>>,
    );

    fn make_thread_no_membership() -> ThreadFixture {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper.clone(),
            processor,
            delegate,
            request_managers.clone(),
            None,
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );
        (thread, tx, reaper, time, request_managers)
    }

    /// Build a thread with a `ConsumerMembershipManager` wired in so the
    /// `run_once` membership.reconcile path is exercised.
    fn make_thread_with_membership() -> (ConsumerNetworkThread<MockClient>, Arc<ConsumerMembershipManager>) {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let (bg_tx, _bg_rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(bg_tx));

        let membership = Arc::new(ConsumerMembershipManager::new(
            "g".to_string(),
            None,
            None,
            30_000,
            None,
            subs.clone(),
            None,
            metadata.clone(),
            beh,
            false,
            None,
            Arc::new(crate::common::utils::SystemTime),
        ));

        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            Some(membership.clone()),
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let thread = ConsumerNetworkThread::new(
            time as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            Some(membership.clone()),
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );
        (thread, membership)
    }

    // ─── Translated Java tests (Phase 10 commit 8/N) ───

    /// Java: `testEnsureCloseStopsRunningThread`. Verifies `isRunning()`
    /// returns true at construction and `false` after `close()`. The
    /// Rust translation uses `signal_close()` because the spawn handle
    /// is the caller's responsibility; the flag-flip semantics are
    /// identical.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testEnsureCloseStopsRunningThread"
    )]
    async fn test_ensure_close_stops_running_thread() {
        let (thread, _tx, _reaper, _time, _rm) = make_thread_no_membership();
        assert!(thread.is_running(), "ConsumerNetworkThread should start running when created");
        thread.signal_close();
        assert!(
            !thread.is_running(),
            "close() should make consumerNetworkThread.running false by calling closeInternal(Duration timeout)"
        );
    }

    /// Java `testConsumerNetworkThreadPollTimeComputations` —
    /// parameterised over `MAX_POLL_TIMEOUT_MS - 1`. The `@ValueSource`
    /// triple is unrolled into three separate Rust tests so the test
    /// names preserve the parameter labels (DoD §3).
    ///
    /// Coordinator manager returns `PollResult(example_time)` and
    /// `maximumTimeToWait(t) = example_time`; heartbeat returns
    /// `PollResult(example_time + 100)` and `maximumTimeToWait =
    /// example_time + 100`. After `run_once`:
    ///   - `delegate.poll(...)` must be called with `min(example_time,
    ///     MAX_POLL_TIMEOUT_MS)`.
    ///   - `maximum_time_to_wait()` returns `example_time` (the min of
    ///     the two `maximumTimeToWait` returns).
    #[tokio::test]
    async fn test_consumer_network_thread_poll_time_computations_below_max() {
        run_poll_time_computations_case(NetThread::MAX_POLL_TIMEOUT_MS - 1).await;
    }

    #[tokio::test]
    async fn test_consumer_network_thread_poll_time_computations_at_max() {
        run_poll_time_computations_case(NetThread::MAX_POLL_TIMEOUT_MS).await;
    }

    #[tokio::test]
    async fn test_consumer_network_thread_poll_time_computations_above_max() {
        run_poll_time_computations_case(NetThread::MAX_POLL_TIMEOUT_MS + 1).await;
    }

    async fn run_poll_time_computations_case(example_time: i64) {
        let coordinator_spy = SpyRequestManager::new(example_time, example_time);
        let heartbeat_spy = SpyRequestManager::new(example_time + 100, example_time + 100);
        let coord_poll_calls = coordinator_spy.poll_calls();
        let hb_poll_calls = heartbeat_spy.poll_calls();
        let coord_max_wait_calls = coordinator_spy.max_wait_calls();
        let hb_max_wait_calls = heartbeat_spy.max_wait_calls();

        let dyn_managers: Vec<Box<dyn RequestManager>> = vec![Box::new(coordinator_spy), Box::new(heartbeat_spy)];
        let (mut fixture, _poll_call_count, poll_timeouts, _has_in_flight) =
            make_thread_with_dyn_managers(dyn_managers);
        fixture.thread.run_once().await;

        // Verify `delegate.poll(min(exampleTime, MAX_POLL_TIMEOUT_MS), ...)`
        // was called with the expected timeout. We assert the timeout
        // passed to the FIRST poll call (run_once executes exactly one
        // network poll per iteration).
        let expected_timeout = example_time.min(NetThread::MAX_POLL_TIMEOUT_MS);
        let timeouts = poll_timeouts.lock().unwrap();
        assert!(!timeouts.is_empty(), "delegate.poll(...) must be called at least once");
        assert_eq!(
            timeouts[0], expected_timeout,
            "delegate.poll timeout = min(example_time, MAX_POLL_TIMEOUT_MS) = {}, got {}",
            expected_timeout, timeouts[0]
        );

        // Verify maximumTimeToWait() returns example_time (the min of
        // the two managers' returns).
        assert_eq!(
            fixture.thread.maximum_time_to_wait(),
            example_time,
            "cachedMaximumTimeToWait must be example_time after run_once"
        );

        // Each manager's poll and maximumTimeToWait was called exactly
        // once during run_once (verify-then-pass equivalent of
        // Mockito's `verify(rm).poll(...)`).
        assert_eq!(coord_poll_calls.load(Ordering::SeqCst), 1, "coordinator.poll called once");
        assert_eq!(hb_poll_calls.load(Ordering::SeqCst), 1, "heartbeat.poll called once");
        assert_eq!(
            coord_max_wait_calls.load(Ordering::SeqCst),
            1,
            "coordinator.maximumTimeToWait called once"
        );
        assert_eq!(
            hb_max_wait_calls.load(Ordering::SeqCst),
            1,
            "heartbeat.maximumTimeToWait called once"
        );
    }

    /// Java `testRequestsTransferFromManagersToClientOnThreadRun`.
    /// Verifies every manager's `poll(...)` and
    /// `maximumTimeToWait(...)` are called and the delegate's
    /// `addAll(...)` + `poll(...)` are called.
    ///
    /// Mockito equivalent:
    ///   `forEach(rm -> verify(rm).poll(anyLong()));`
    ///   `forEach(rm -> verify(rm).maximumTimeToWait(anyLong()));`
    ///   `verify(networkClientDelegate).addAll(...);`
    ///   `verify(networkClientDelegate).poll(...)`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testRequestsTransferFromManagersToClientOnThreadRun"
    )]
    async fn test_requests_transfer_from_managers_to_client_on_thread_run() {
        let coordinator_spy = SpyRequestManager::new(1_000, 1_000);
        let heartbeat_spy = SpyRequestManager::new(2_000, 2_000);
        let offsets_spy = SpyRequestManager::new(3_000, 3_000);
        let coord_poll = coordinator_spy.poll_calls();
        let coord_max_wait = coordinator_spy.max_wait_calls();
        let hb_poll = heartbeat_spy.poll_calls();
        let hb_max_wait = heartbeat_spy.max_wait_calls();
        let off_poll = offsets_spy.poll_calls();
        let off_max_wait = offsets_spy.max_wait_calls();

        let dyn_managers: Vec<Box<dyn RequestManager>> = vec![
            Box::new(coordinator_spy),
            Box::new(heartbeat_spy),
            Box::new(offsets_spy),
        ];
        let (mut fixture, poll_call_count, _poll_timeouts, _has_in_flight) =
            make_thread_with_dyn_managers(dyn_managers);

        fixture.thread.run_once().await;

        // Every manager observed exactly one `poll` and one
        // `maximumTimeToWait` call — the Mockito `forEach(rm ->
        // verify(rm).poll(anyLong()))` equivalent.
        assert_eq!(coord_poll.load(Ordering::SeqCst), 1, "coordinator.poll(now) called once");
        assert_eq!(hb_poll.load(Ordering::SeqCst), 1, "heartbeat.poll(now) called once");
        assert_eq!(off_poll.load(Ordering::SeqCst), 1, "offsets.poll(now) called once");
        assert_eq!(
            coord_max_wait.load(Ordering::SeqCst),
            1,
            "coordinator.maximumTimeToWait called once"
        );
        assert_eq!(hb_max_wait.load(Ordering::SeqCst), 1, "heartbeat.maximumTimeToWait called once");
        assert_eq!(off_max_wait.load(Ordering::SeqCst), 1, "offsets.maximumTimeToWait called once");

        // delegate.poll(...) was invoked. (Java:
        // `verify(networkClientDelegate).poll(anyLong(), anyLong())`.)
        // `addAll(...)` is verified implicitly: PollResult collection
        // happens for every manager, and `add_all_from_poll_result`
        // returns the timeout that feeds into `delegate.poll` — if it
        // were not called we would not poll with the manager-supplied
        // timeout.
        assert_eq!(
            poll_call_count.load(Ordering::SeqCst),
            1,
            "delegate.poll(timeout, now) must be called exactly once per run_once"
        );
    }

    /// Java `testMaximumTimeToWait`. Verifies:
    ///   1. The initial cached value is `MAX_POLL_TIMEOUT_MS` before
    ///      `runOnce` is called.
    ///   2. After `runOnce`, the value is the min of each registered
    ///      manager's `maximumTimeToWait(now)`. With a single heartbeat
    ///      spy returning 1_000, the cached value must be 1_000.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testMaximumTimeToWait")]
    async fn test_maximum_time_to_wait() {
        const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 1_000;

        let heartbeat_spy = SpyRequestManager::new(NetThread::MAX_POLL_TIMEOUT_MS, DEFAULT_HEARTBEAT_INTERVAL_MS);
        let dyn_managers: Vec<Box<dyn RequestManager>> = vec![Box::new(heartbeat_spy)];
        let (mut fixture, _poll, _to, _hi) = make_thread_with_dyn_managers(dyn_managers);

        // Initial value before runOnce has been called.
        assert_eq!(
            fixture.thread.maximum_time_to_wait(),
            NetThread::MAX_POLL_TIMEOUT_MS,
            "initial cached maximumTimeToWait must equal MAX_POLL_TIMEOUT_MS"
        );

        fixture.thread.run_once().await;

        // After runOnce: the cached value is the heartbeat interval.
        assert_eq!(
            fixture.thread.maximum_time_to_wait(),
            DEFAULT_HEARTBEAT_INTERVAL_MS,
            "after runOnce, maximumTimeToWait reflects the heartbeat-interval min"
        );
    }

    /// Java `testCleanupInvokesReaper`. Verifies `cleanup()` invokes
    /// the reaper. Mockito: `verify(applicationEventReaper).reap(...)`.
    /// Rust observes via a deadline-zero tracked event becoming
    /// `Err(Timeout)` on its receiver.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testCleanupInvokesReaper")]
    async fn test_cleanup_invokes_reaper() {
        let (mut thread, _tx, reaper, _time, _rm) = make_thread_no_membership();
        // Add a deadline-zero completable event so reap-on-close
        // expires it.
        let (_h, mut rx, erased) = CompletableEvent::make_completable_event::<()>(0);
        reaper.lock().unwrap().add(erased);

        thread.cleanup().await;

        // The reaper observed the tracked event and timed it out —
        // proves `reap_on_close(...)` was called inside `cleanup()`.
        assert!(
            matches!(rx.try_recv().expect("sender used"), Err(Error::Timeout(_))),
            "reaper.reap(...) must complete tracked event with Timeout"
        );
    }

    /// Request manager whose `pollOnClose` panics.
    struct PanicOnCloseRequestManager;

    impl RequestManager for PanicOnCloseRequestManager {
        fn poll(&mut self, _current_time_ms: i64) -> PollResult {
            PollResult::empty()
        }

        fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
            panic!("poll on close failure");
        }
    }

    /// Java 4.4 `ConsumerNetworkThread.cleanup()` catches a
    /// failure in `runAtClose(...)`, logs "Unexpected error during shutdown.
    /// Proceed with closing." and still reaps and closes the managers and the
    /// delegate in its `finally`. A panicking `poll_on_close` must therefore
    /// neither escape `cleanup()` nor skip the later steps.
    #[tokio::test]
    async fn cleanup_proceeds_with_closing_when_poll_on_close_panics() {
        let (mut fixture, _poll, _to, _hi) = make_thread_with_dyn_managers(vec![Box::new(PanicOnCloseRequestManager)]);
        let (_h, mut rx, erased) = CompletableEvent::make_completable_event::<()>(0);
        fixture.reaper.lock().unwrap().add(erased);

        fixture.thread.cleanup().await;

        assert!(
            matches!(rx.try_recv().expect("sender used"), Err(Error::Timeout(_))),
            "cleanup must still reap the tracked event after poll_on_close panics"
        );
        assert!(
            !fixture
                .delegate
                .try_lock()
                .expect("delegate not contended")
                .client_for_test_ref()
                .active(),
            "cleanup must still close the network client after poll_on_close panics"
        );
    }

    /// Translated from `ConsumerNetworkThreadTest.testProcessEventFailureCompletesFutureExceptionally`
    /// (KAFKA-18812): when processing an event fails, its future is completed
    /// with the failure instead of being left for the reaper to time out.
    ///
    /// Java stubs `process` to throw a `RuntimeException`; the Rust processor
    /// cannot return an error, so the `fail_process_for_test` seam makes it
    /// panic, and the loop's catch completes the event with a bare Kafka error
    /// carrying the failure's message (Java's `ConsumerUtils.getResult` wraps
    /// the cause in a `KafkaException`).
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testProcessEventFailureCompletesFutureExceptionally"
    )]
    async fn test_process_event_failure_completes_future_exceptionally() {
        let (mut thread, tx, _reaper, time, _rm) = make_thread_no_membership();
        thread.application_event_processor.fail_process_for_test = Some("Simulated processing failure".to_string());

        let now_ms = time.milliseconds();
        let (handle, mut rx, _erased) = CompletableEvent::make_completable_event::<()>(now_ms + 1_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::PausePartitions { handle, partitions: std::collections::HashSet::new() },
            enqueued_ms: now_ms,
        })
        .expect("bg receiver alive");

        thread.run_once().await;

        let result = rx
            .try_recv()
            .expect("Event future should be completed after processing failure");
        let err = result.expect_err("Event future should be completed exceptionally");
        assert!(matches!(err, Error::KafkaError(_)), "a bare KafkaException: {err:?}");
        assert_eq!("Simulated processing failure", err.message());
        // The loop survives the failure: a later event is still processed.
        thread.application_event_processor.fail_process_for_test = None;
        let (handle, mut rx, _erased) = CompletableEvent::make_completable_event::<()>(now_ms + 1_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::PausePartitions { handle, partitions: std::collections::HashSet::new() },
            enqueued_ms: now_ms,
        })
        .expect("bg receiver alive");
        thread.run_once().await;
        rx.try_recv().expect("processed").expect("succeeds");
    }

    /// Java `testRunOnceInvokesReaper`. Verifies `runOnce` invokes
    /// the reaper. Mockito: `verify(applicationEventReaper).reap(any(Long.class))`.
    /// Rust observes via a same-instant-deadline tracked event whose
    /// receiver becomes `Err(Timeout)` after `runOnce`.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testRunOnceInvokesReaper")]
    async fn test_run_once_invokes_reaper() {
        let (mut thread, _tx, reaper, time, _rm) = make_thread_no_membership();
        // Register a deadline-zero event; clock starts at 1_000, so
        // every reap iteration past-due will fire `Timeout` on rx.
        let (_h, mut rx, erased) = CompletableEvent::make_completable_event::<()>(0);
        reaper.lock().unwrap().add(erased);

        // Drive a single iteration. The reap step at Phase 6 must
        // observe the past-due deadline and fail the event.
        let _ = time;
        thread.run_once().await;

        assert!(
            matches!(rx.try_recv().expect("sender used"), Err(Error::Timeout(_))),
            "runOnce must call reaper.reap(now) and expire the past-due event"
        );
    }

    /// Java `testSendUnsentRequests`. Mockito drives
    /// `hasAnyPendingRequests()` to return `true, true, false` so the
    /// cleanup loop polls exactly twice. The Rust translation injects
    /// that scripted return via `CountingClient::has_in_flight_script`.
    /// Verifies `delegate.poll(..., onClose=true)` is called exactly
    /// twice during cleanup.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerNetworkThreadTest#testSendUnsentRequests")]
    async fn test_send_unsent_requests() {
        let (mut fixture, poll_call_count, _poll_timeouts, has_in_flight_script) =
            make_thread_with_dyn_managers(Vec::new());

        // Java: `when(hasAnyPendingRequests).thenReturn(true,true,false)`.
        // Translating directly: the cleanup loop calls
        // `has_any_pending_requests` once before entering the drain
        // loop and once per iteration thereafter. Our delegate calls
        // `client.has_in_flight_requests()` THROUGH the underlying
        // `client` reference. The Rust cleanup body has TWO read
        // sites of `has_any_pending_requests` per iteration:
        // (a) the guard at the top, (b) the post-drain warning check
        // outside the loop. To make poll fire exactly twice we push
        // `[true, true, false, false]` (2 inside the loop guard +
        // 2 outside warning check); the third+fourth `false` is the
        // exit-condition + warning-log read.
        //
        // Java's `testSendUnsentRequests` only counts `poll` calls
        // (twice). The script of three (true, true, false) is the
        // minimum to produce two iterations; we emit four total to
        // service both Rust call sites without underflow.
        has_in_flight_script.lock().unwrap().extend([true, true, false, false]);

        // Bump the close-timeout so the deadline isn't the early
        // exit. Java's `timer.remainingMs()` is bounded by the same
        // close timeout (`closeTimeout` defaults to 30s).
        fixture.thread.set_close_timeout_ms(30_000);
        fixture.thread.cleanup().await;

        assert_eq!(
            poll_call_count.load(Ordering::SeqCst),
            2,
            "delegate.poll(..., onClose=true) must be invoked twice during cleanup"
        );
    }

    /// Java's `ConsumerNetworkThread#sendUnsentRequests` is a do-while: with a request pending it polls
    /// once even when the close timeout is zero, which is how the leave-group
    /// heartbeat `pollOnClose` builds for a zero-timeout close gets sent
    /// (`AbstractHeartbeatRequestManager#pollOnClose`). A zero close timeout
    /// must therefore still perform exactly one non-blocking `poll(0)`.
    #[tokio::test]
    async fn test_send_unsent_requests_polls_once_with_zero_close_timeout() {
        let (mut fixture, poll_call_count, poll_timeouts, has_in_flight_script) =
            make_thread_with_dyn_managers(Vec::new());
        // Initial `hasAnyPendingRequests()` = true -> one poll; the expired
        // timer short-circuits the loop guard; the trailing `false` serves
        // the post-drain warning check.
        has_in_flight_script.lock().unwrap().extend([true, false]);

        fixture.thread.set_close_timeout_ms(0);
        fixture.thread.cleanup().await;

        assert_eq!(
            poll_call_count.load(Ordering::SeqCst),
            1,
            "a zero close timeout must still poll once to send pending requests"
        );
        assert_eq!(
            poll_timeouts.lock().unwrap().as_slice(),
            &[0],
            "the single close poll must be non-blocking (timer.remainingMs() == 0)"
        );
    }

    // ─── Java tests deliberately NOT translated in this commit ───
    //
    // - `testStartupAndTearDown`: exercises `Thread.start()`/
    //   `isAlive()` to verify the background thread is genuinely
    //   running. In Rust the bg task is
    //   `tokio::spawn(thread.run().await)` driven by the consumer
    //   constructor (Phase 11); the spawn/join semantics belong to
    //   `AsyncKafkaConsumer`, not `ConsumerNetworkThread`. The
    //   equivalent assertion (`run().await` returns cleanly after
    //   `signal_close()`) belongs to the Phase-11 consumer-constructor
    //   tests.
    //
    // - `testNetworkClientDelegateInitializeResourcesError`,
    //   `testRequestManagersInitializeResourcesError`,
    //   `testNetworkClientDelegateAndRequestManagersInitializeResourcesError`:
    //   exercise Java's `Supplier<NetworkClientDelegate>` /
    //   `Supplier<RequestManagers>` constructor indirection. The Rust
    //   constructor takes already-constructed values (no `Supplier`),
    //   so the initialize-error path lives at the call site (Phase 11
    //   consumer constructor). Mirrors commit 7's deferral.
    //
    // (`testRunOnceRecordTimeBetweenNetworkThreadPoll` and
    //  `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`
    //  are translated below — see `run_once_records_time_between_network_thread_poll`
    //  and `run_once_records_application_event_queue_size_and_time`.)

    /// Drain-events path: an enqueued completable event is registered
    /// with the reaper during `process_application_events`. We call
    /// the inner method directly so we observe the reaper state at the
    /// `add` site — after a full `run_once` the reap step would
    /// already have removed any handles whose AEP arm synchronously
    /// completed them.
    #[tokio::test]
    async fn process_events_registers_completable_with_reaper() {
        let (mut thread, tx, reaper, _time, _rm) = make_thread_no_membership();
        let (handle, _rx, erased_external) = CompletableEvent::make_completable_event::<()>(60_000);
        let event = ApplicationEvent::AssignmentChange { handle, current_time_ms: 1_000, partitions: HashSet::new() };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: 1_000 })
            .expect("send ok");

        // Verify the reaper is empty before processing.
        assert_eq!(reaper.lock().unwrap().size(), 0);

        // Drain + dispatch. The AEP arm for `AssignmentChange` may
        // complete the handle synchronously, but the reaper's `add`
        // happens BEFORE the dispatch, so post-call `contains` over
        // a same-inner-id erased clone is true iff registration ran.
        thread.process_application_events();

        // The reaper observed the event. `contains` matches across
        // erased recreations via `inner_id()`, so the externally-held
        // `erased_external` is the same identity even though
        // `process_application_events` made its own erased clone.
        let r = reaper.lock().unwrap();
        assert!(
            r.contains(&erased_external),
            "completable event must be registered with the reaper (size={})",
            r.size()
        );
    }

    /// `process_application_events` skips registration for
    /// non-completable variants. Java analog: only `instanceof
    /// CompletableEvent` events are added.
    #[tokio::test]
    async fn process_events_skips_non_completable_variants() {
        let (mut thread, tx, reaper, _time, _rm) = make_thread_no_membership();
        tx.send(ApplicationEventEnvelope { event: ApplicationEvent::CommitOnClose, enqueued_ms: 0 })
            .expect("send ok");
        tx.send(ApplicationEventEnvelope { event: ApplicationEvent::NewTopicsMetadataUpdate, enqueued_ms: 0 })
            .expect("send ok");
        thread.process_application_events();
        assert_eq!(reaper.lock().unwrap().size(), 0, "non-completable events must not be tracked");
    }

    /// Java's `processApplicationEvents` catches `Throwable` per event
    /// (`ConsumerNetworkThread#processApplicationEvents`), so one failing event does not skip the rest of the
    /// batch, and completes the failing event with the failure (KAFKA-18812).
    /// The failure is induced through the production path: with the delegate
    /// lock held elsewhere, the metadata-error arm's `try_lock().expect(..)`
    /// panics on the notifiable event, after the event was registered with
    /// the reaper.
    #[tokio::test]
    async fn process_events_continues_after_an_event_panics() {
        let (fixture, _, _, _) = make_thread_with_dyn_managers(Vec::new());
        let CountingFixture { mut thread, delegate, reaper, tx, .. } = fixture;

        let (failing, mut failing_rx, failing_erased) = CompletableEvent::make_completable_event::<
            std::collections::HashMap<String, Vec<crate::common::PartitionInfo>>,
        >(60_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::AllTopicsMetadata { handle: failing },
            enqueued_ms: 1_000,
        })
        .expect("send ok");
        let (next, mut next_rx, _) = CompletableEvent::make_completable_event::<()>(60_000);
        let event =
            ApplicationEvent::AssignmentChange { handle: next, current_time_ms: 1_000, partitions: HashSet::new() };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: 1_000 })
            .expect("send ok");

        {
            let _held = delegate.lock().await;
            thread.process_application_events();
        }

        assert!(
            matches!(next_rx.try_recv(), Ok(Ok(()))),
            "the event after the failing one must still be dispatched"
        );
        // Like Java's catch, the boundary completes the failing event with the
        // failure (a bare Kafka error carrying the panic message) instead of
        // leaving it for the reaper's deadline.
        let err = failing_rx
            .try_recv()
            .expect("the boundary must complete the failing event")
            .expect_err("completed with the failure");
        assert!(matches!(err, Error::KafkaError(_)), "a bare KafkaException: {err:?}");
        assert!(
            err.message().starts_with("delegate not contended on bg task"),
            "carries the panic message: {err:?}"
        );
        // Registration came before the panic, as `applicationEventReaper.add`
        // precedes the failure in Java's `try`; the reaper drops completed
        // events on its next pass.
        assert!(
            reaper.lock().unwrap().contains(&failing_erased),
            "the failing event was registered with the reaper before it failed"
        );
    }

    /// Request manager whose first `poll` panics; the second clears the
    /// thread's running flag so `run` leaves its loop.
    struct PanickingRequestManager {
        poll_calls: Arc<AtomicUsize>,
        poll_on_close_calls: Arc<AtomicUsize>,
        running: Arc<std::sync::OnceLock<Arc<AtomicBool>>>,
    }

    impl RequestManager for PanickingRequestManager {
        fn poll(&mut self, _current_time_ms: i64) -> PollResult {
            if self.poll_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("request manager failure");
            }
            self.running
                .get()
                .expect("running flag installed")
                .store(false, Ordering::Release);
            PollResult::empty()
        }

        fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
            self.poll_on_close_calls.fetch_add(1, Ordering::SeqCst);
            PollResult::empty()
        }
    }

    /// Java's `run()` wraps each `runOnce()` in `catch (final Throwable e)`
    /// and keeps looping (`ConsumerNetworkThread#run`), and `cleanup()` still runs in its
    /// `finally`. A panicking iteration must therefore neither end the task
    /// nor skip `cleanup()`.
    #[tokio::test]
    async fn run_continues_after_run_once_panics_and_still_cleans_up() {
        let poll_calls = Arc::new(AtomicUsize::new(0));
        let poll_on_close_calls = Arc::new(AtomicUsize::new(0));
        let running = Arc::new(std::sync::OnceLock::new());
        let manager = PanickingRequestManager {
            poll_calls: poll_calls.clone(),
            poll_on_close_calls: poll_on_close_calls.clone(),
            running: running.clone(),
        };
        let (fixture, _, _, _) = make_thread_with_dyn_managers(vec![Box::new(manager)]);
        let thread = fixture.thread;
        running.set(thread.running_handle()).expect("set once");

        tokio::time::timeout(Duration::from_secs(10), thread.run())
            .await
            .expect("run must return once the running flag is cleared");

        assert_eq!(
            poll_calls.load(Ordering::SeqCst),
            2,
            "the loop must run again after the panicking iteration"
        );
        assert_eq!(poll_on_close_calls.load(Ordering::SeqCst), 1, "cleanup must still run");
    }

    /// Request manager that panics on the first `poll` (or on every `poll`
    /// when `always_panic`) and counts calls, without touching the running
    /// flag, so the test decides when the loop stops.
    struct CountingPanickingRequestManager {
        always_panic: bool,
        poll_calls: Arc<AtomicUsize>,
        poll_on_close_calls: Arc<AtomicUsize>,
    }

    impl RequestManager for CountingPanickingRequestManager {
        fn poll(&mut self, _current_time_ms: i64) -> PollResult {
            if self.poll_calls.fetch_add(1, Ordering::SeqCst) == 0 || self.always_panic {
                panic!("request manager failure");
            }
            PollResult::with_time_until_next_poll_ms(10)
        }

        fn maximum_time_to_wait(&self, _current_time_ms: i64) -> i64 {
            10
        }

        fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
            self.poll_on_close_calls.fetch_add(1, Ordering::SeqCst);
            PollResult::empty()
        }
    }

    /// Starts the thread the way the production consumer does
    /// (`spawn_dedicated`, a `kafka-consumer-io` OS thread), waits for at
    /// least `min_polls` manager polls, then stops it like `close()` does
    /// (clear the running flag) and asserts `done` fires, the OS thread exits
    /// and `cleanup()` ran.
    async fn assert_dedicated_thread_survives_panics(always_panic: bool, min_polls: usize) {
        let poll_calls = Arc::new(AtomicUsize::new(0));
        let poll_on_close_calls = Arc::new(AtomicUsize::new(0));
        let manager = CountingPanickingRequestManager {
            always_panic,
            poll_calls: poll_calls.clone(),
            poll_on_close_calls: poll_on_close_calls.clone(),
        };
        let (fixture, _, _, _) = make_thread_with_dyn_managers(vec![Box::new(manager)]);
        let running = fixture.thread.running_handle();
        let (done_rx, thread_handle) = fixture.thread.spawn_dedicated();

        let start = std::time::Instant::now();
        while poll_calls.load(Ordering::SeqCst) < min_polls
            && !thread_handle.is_finished()
            && start.elapsed() < Duration::from_secs(5)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let finished_early = thread_handle.is_finished();
        let polls = poll_calls.load(Ordering::SeqCst);
        running.store(false, Ordering::Release);
        let done = tokio::time::timeout(Duration::from_secs(5), done_rx).await;
        let joined = tokio::task::spawn_blocking(move || thread_handle.join())
            .await
            .expect("join task");

        assert!(
            !finished_early && polls >= min_polls,
            "Java: the network thread logs the error and keeps running; Rust: thread finished early={finished_early}, \
             manager polls={polls}"
        );
        assert!(
            matches!(done, Ok(Ok(()))),
            "the dedicated thread must signal done after the loop exits, got {done:?}"
        );
        assert!(joined.is_ok(), "the dedicated OS thread must exit cleanly");
        assert_eq!(poll_on_close_calls.load(Ordering::SeqCst), 1, "cleanup must still run");
    }

    /// Audit C1-22. The production consumer runs the loop on the dedicated
    /// `kafka-consumer-io` thread; Java's `ConsumerNetworkThread.run()`
    /// catches every `runOnce()` failure, logs "Unexpected error caught in
    /// consumer network thread" and keeps looping, with `cleanup()` in the
    /// `finally`. One panicking iteration must not end that thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_thread_survives_a_panicking_iteration() {
        assert_dedicated_thread_survives_panics(false, 3).await;
    }

    /// Every iteration panics (e.g. a poisoned lock keeps failing). Java just
    /// keeps looping; the Rust loop yields after each caught panic, so the
    /// running-flag check is still reached and stopping the thread still
    /// runs `cleanup()` and fires `done`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_thread_stops_and_cleans_up_when_every_iteration_panics() {
        assert_dedicated_thread_survives_panics(true, 3).await;
    }

    /// Java 4.4 `ConsumerNetworkThread.run()` runs `cleanup()`
    /// in a `finally` outside its `catch (Throwable)`; a failure there is
    /// logged at ERROR by `KafkaThread`'s uncaught-exception handler and the
    /// thread still ends, so `closeInternal`'s `join()` returns normally.
    /// A panic in `cleanup()` (here: the client's `close()`, the last step)
    /// must therefore still let the dedicated thread signal `done` and exit
    /// cleanly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_thread_signals_done_when_cleanup_panics() {
        let poll_calls = Arc::new(AtomicUsize::new(0));
        let poll_on_close_calls = Arc::new(AtomicUsize::new(0));
        let manager = CountingPanickingRequestManager {
            always_panic: false,
            poll_calls: poll_calls.clone(),
            poll_on_close_calls: poll_on_close_calls.clone(),
        };
        let (fixture, _, _, _) = make_thread_with_dyn_managers(vec![Box::new(manager)]);
        let close_panics = fixture
            .delegate
            .try_lock()
            .expect("delegate not contended")
            .client_for_test_ref()
            .close_panics();
        close_panics.store(true, Ordering::SeqCst);
        // Stopped before it starts: `run()` goes straight to `cleanup()`.
        fixture.thread.running_handle().store(false, Ordering::Release);
        let (done_rx, thread_handle) = fixture.thread.spawn_dedicated();

        let done = tokio::time::timeout(Duration::from_secs(5), done_rx).await;
        let joined = tokio::task::spawn_blocking(move || thread_handle.join())
            .await
            .expect("join task");

        assert_eq!(poll_on_close_calls.load(Ordering::SeqCst), 1, "cleanup must have run");
        assert!(
            matches!(done, Ok(Ok(()))),
            "the dedicated thread must signal done after cleanup() panics, got {done:?}"
        );
        assert!(
            joined.is_ok(),
            "the dedicated OS thread must exit cleanly after cleanup() panics"
        );
    }

    /// `wakeup` mid-poll: the bg-task `run_once` should not hang when
    /// the wakeup token is cancelled before/during the network poll.
    /// We assert `run_once` returns within a tight window even though
    /// `MAX_POLL_TIMEOUT_MS = 5_000`.
    #[tokio::test]
    async fn run_once_returns_when_wakeup_fires_during_poll() {
        let (mut thread, _tx, _reaper, _time, _rm) = make_thread_no_membership();
        // Cancel the wakeup token before run_once starts; the select!
        // arm should win immediately.
        thread.wakeup.wakeup();
        let res = tokio::time::timeout(Duration::from_millis(500), thread.run_once()).await;
        assert!(res.is_ok(), "run_once must return promptly after wakeup");
    }

    /// `membership.reconcile()` is called once per `run_once` iteration.
    /// This is the load-bearing assertion from the Critic round-1 aside:
    /// Java's `entries()` includes membership and its `poll(...)` body
    /// calls `maybeReconcile(false)`; Rust's `entries()` skips it, so
    /// the bg task must drive it directly.
    ///
    /// We observe the side-effect by reading the membership state
    /// after `run_once`: `reconcile` is a no-op when state is not
    /// `Reconciling`, but **the call is dispatched** — we assert that
    /// by checking that the membership manager is still in `Unjoined`
    /// state (proving reconcile was invoked, observed the wrong state,
    /// returned `Ok(())`, and did not panic). The deeper assertion that
    /// reconcile actually transitions state lives in
    /// `consumer_membership_manager.rs` tests.
    #[tokio::test]
    async fn run_once_invokes_membership_reconcile() {
        let (mut thread, membership) = make_thread_with_membership();
        // Default state — exact label depends on MemberState init, but
        // it must be a non-Reconciling state so reconcile() short-
        // circuits and returns Ok(()) without panic.
        let state_before = membership.state();
        thread.run_once().await;
        let state_after = membership.state();
        assert_eq!(
            state_before, state_after,
            "reconcile in non-Reconciling state must not transition (state_before={:?})",
            state_before
        );
        // Sanity: a second run_once should also not panic.
        thread.run_once().await;
    }

    /// A thread with a heartbeat manager (coordinator known) and a member in
    /// the group (epoch 5) owning `t-0` with a listener registered, that has
    /// just called `leave_group()`: PREPARE_LEAVING, its `onPartitionsRevoked`
    /// callback enqueued. Returns the thread, the membership manager, the
    /// subscription state, the application-event sender, the background-event
    /// receiver (kept alive), the callback event (whose ack the app side
    /// returns with [`return_leave_callback_ack`]) and the leave future.
    #[expect(clippy::type_complexity)]
    fn make_thread_with_member_preparing_to_leave() -> (
        ConsumerNetworkThread<MockClient>,
        Arc<ConsumerMembershipManager>,
        Arc<Mutex<SubscriptionState>>,
        mpsc::UnboundedSender<ApplicationEventEnvelope>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
        crate::consumer::internals::events::BackgroundEvent,
        crate::common::internals::KafkaFutureImpl<()>,
    ) {
        use crate::consumer::internals::MemberState;
        use crate::consumer::internals::events::BackgroundEvent;

        let (thread, membership, subs, tx, mut bg_rx) = make_thread_with_member_in_group();
        let leave = membership.leave_group(0).unwrap();
        assert_eq!(membership.state(), MemberState::PrepareLeaving);
        let callback = bg_rx.try_recv().expect("revoked callback enqueued").event;
        assert!(
            matches!(
                callback,
                BackgroundEvent::PartitionsRemoved { ack_through_application_events: true, .. }
            ),
            "a leave's callback returns through the application-event queue, got {callback:?}"
        );
        (thread, membership, subs, tx, bg_rx, callback, leave)
    }

    /// What the app side does once the listener for `callback` (a
    /// `PartitionsRemoved` event) has returned `result`, through the same
    /// helper `AsyncKafkaConsumer::process_background_events` uses (DoD §12).
    fn return_leave_callback_ack(
        tx: &mpsc::UnboundedSender<ApplicationEventEnvelope>,
        callback: crate::consumer::internals::events::BackgroundEvent,
        result: Result<(), Error>,
    ) {
        use crate::consumer::internals::events::BackgroundEvent;
        let BackgroundEvent::PartitionsRemoved { method_name, ack, ack_through_application_events, .. } = callback
        else {
            panic!("expected a PartitionsRemoved event, got {callback:?}");
        };
        let handler =
            crate::consumer::internals::events::ApplicationEventHandler::new(tx.clone(), Arc::new(Notify::new()));
        crate::consumer::internals::async_kafka_consumer::notify_background_task_of_rebalance_callback(
            &handler,
            method_name,
            ack,
            ack_through_application_events,
            result,
            1_000,
        );
    }

    /// A thread with a heartbeat manager (coordinator known) and a member in
    /// the group (epoch 5) owning `t-0` with a listener registered. Returns
    /// the thread, the membership manager, the subscription state, the
    /// application-event sender and the background-event receiver (kept
    /// alive).
    #[expect(clippy::type_complexity)]
    fn make_thread_with_member_in_group() -> (
        ConsumerNetworkThread<MockClient>,
        Arc<ConsumerMembershipManager>,
        Arc<Mutex<SubscriptionState>>,
        mpsc::UnboundedSender<ApplicationEventEnvelope>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) {
        use crate::consumer::ConsumerRebalanceListener;
        use crate::consumer::internals::ConsumerHeartbeatRequestManager;
        use crate::consumer::internals::CoordinatorRequestManager;

        struct NoopListener;
        #[async_trait::async_trait]
        impl ConsumerRebalanceListener for NoopListener {
            async fn on_partitions_revoked(&self, _partitions: &[crate::common::TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
            async fn on_partitions_assigned(&self, _partitions: &[crate::common::TopicPartition]) -> Result<(), Error> {
                Ok(())
            }
        }

        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        {
            let mut guard = subs.lock().unwrap();
            guard
                .subscribe_with_topics(HashSet::from(["t".to_string()]), Some(Arc::new(NoopListener)))
                .unwrap();
            guard
                .assign_from_subscribed(&[crate::common::TopicPartition::new("t", 0)])
                .unwrap();
        }
        let metadata = make_metadata(&config, subs.clone());
        let (bg_tx, bg_rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(bg_tx));
        let membership = Arc::new(ConsumerMembershipManager::new(
            "g".to_string(),
            None,
            None,
            30_000,
            None,
            subs.clone(),
            None,
            metadata.clone(),
            Arc::clone(&beh),
            false,
            None,
            Arc::new(crate::common::utils::SystemTime),
        ));
        let coordinator = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g"));
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let heartbeat =
            ConsumerHeartbeatRequestManager::new(0, &config, coordinator, subs.clone(), membership.clone(), beh);
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            Some(heartbeat),
            Some(membership.clone()),
            None,
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let thread = ConsumerNetworkThread::new(
            time as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            Some(membership.clone()),
            WakeupTrigger::new(),
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        membership.transition_to_joining().unwrap();
        membership.abstract_mm.inner.lock().unwrap().update_member_epoch(5);
        (thread, membership, subs, tx, bg_rx)
    }

    /// C2-01 (Critic 1 item 1): an `unsubscribe()` leave waiting in
    /// PREPARE_LEAVING for its `on_partitions_revoked` ack moves to LEAVING
    /// on the bg loop, in `run_once` before the request managers poll — Java
    /// runs `clearAssignmentAndLeaveGroup()` from the callback's
    /// `whenComplete` on the network thread — so the heartbeat manager's poll
    /// in the same iteration builds the leave heartbeat (LEAVING ->
    /// UNSUBSCRIBED). The ack arriving on its own changes nothing.
    #[tokio::test]
    async fn run_once_applies_leave_callback_ack_before_heartbeat_poll() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, _subs, tx, _bg_rx, callback, leave) = make_thread_with_member_preparing_to_leave();
        return_leave_callback_ack(&tx, callback, Ok(()));
        tokio::task::yield_now().await;
        assert_eq!(
            membership.state(),
            MemberState::PrepareLeaving,
            "the ack alone must not move the member: the bg loop applies it"
        );

        thread.run_once().await;
        assert_eq!(
            membership.state(),
            MemberState::Unsubscribed,
            "the leave heartbeat must be built in the same run_once that applied the ack"
        );
        assert_eq!(membership.member_epoch(), -1);
        assert!(!membership.has_pending_leave_callback());
        assert!(!leave.is_done(), "the leave completes only on the leave heartbeat's response");
    }

    /// Audit finding C1-17: a `LeaveGroupOnClose` whose deadline has already
    /// passed (`close(Duration::ZERO)`) is still processed — the member
    /// leaves and the leave heartbeat is built and handed to the network
    /// client — and the reaper then expires the event in the same
    /// `run_once`, so the waiting `close()` proceeds after one iteration.
    /// Java's `runOnce` processes the application events before
    /// `applicationEventReaper.reap(currentTimeMs)`.
    #[tokio::test]
    async fn run_once_processes_then_reaps_an_expired_leave_group_on_close() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, _subs, tx, _bg_rx) = make_thread_with_member_in_group();
        let now_ms = thread.time.milliseconds();
        let (handle, mut rx, _) = CompletableEvent::make_completable_event::<()>(now_ms);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::LeaveGroupOnClose {
                handle,
                membership_operation: GroupMembershipOperation::Default,
            },
            enqueued_ms: now_ms,
        })
        .expect("send ok");

        thread.run_once().await;

        assert_eq!(
            membership.state(),
            MemberState::Unsubscribed,
            "the expired LeaveGroupOnClose must still be processed and its leave heartbeat built"
        );
        assert_eq!(membership.member_epoch(), -1, "the leave heartbeat carries the leave epoch");
        assert!(
            thread.network_client_delegate.lock().await.has_any_pending_requests(),
            "the leave heartbeat must have been handed to the network client"
        );
        match rx.try_recv().expect("the reaper must complete the event in the same run_once") {
            // Java: "{event} was {pastDueMs} ms past its expiration of {deadlineMs}".
            Err(err @ Error::Timeout(_)) => assert!(
                err.to_string().ends_with(&format!(" was 0 ms past its expiration of {now_ms}")),
                "unexpected reaper message: {err}"
            ),
            other => panic!("expected the reaper's timeout, got {other:?}"),
        }
    }

    /// C2-01 (Critic 1 R2-1): the leave's callback ack is applied in queue
    /// order with the application events. Java's
    /// `ConsumerRebalanceListenerCallbackCompletedEvent` is enqueued before
    /// anything the app submits afterwards, so `clearAssignmentAndLeaveGroup()`
    /// runs before a `subscribe()` that follows an `unsubscribe()` which
    /// returned without the ack applied (its deadline passed during the
    /// listener). The new subscription must survive the old leave.
    #[tokio::test]
    async fn leave_callback_ack_is_applied_before_later_application_events() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, subs, tx, _bg_rx, callback, _leave) = make_thread_with_member_preparing_to_leave();
        return_leave_callback_ack(&tx, callback, Ok(()));
        let (handle, sub_rx) = crate::consumer::internals::events::CompletableEventHandle::<()>::new(60_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::TopicSubscriptionChange {
                handle,
                topics: HashSet::from(["t2".to_string()]),
                listener: None,
            },
            enqueued_ms: 1_000,
        })
        .unwrap();

        thread.run_once().await;
        assert!(matches!(sub_rx.await, Ok(Ok(()))), "subscribe() reported success");
        let subscription = subs.lock().unwrap().subscription();
        assert_eq!(
            subscription,
            HashSet::from(["t2".to_string()]),
            "the late leave step must run before the subscribe, not wipe it"
        );
        assert!(!membership.has_pending_leave_callback(), "the old leave's step was applied");
        assert_eq!(
            membership.member_epoch(),
            -1,
            "the old leave reached LEAVING and its leave heartbeat was built"
        );
        assert_ne!(membership.state(), MemberState::PrepareLeaving);
    }

    /// PR #263 review (Copilot): the leave's callback ack must not overtake an
    /// application event the listener submitted before it returned, even one
    /// whose wait the listener abandoned (a `ConsumerHandle` op under
    /// `tokio::time::timeout`, or a dropped future) and that is therefore
    /// still queued. Java's app thread enqueues
    /// `ConsumerRebalanceListenerCallbackCompletedEvent` after the listener
    /// returns (`AsyncKafkaConsumer.invokeRebalanceCallbackAndNotifyBackgroundThread`),
    /// on the same queue, so `ConsumerNetworkThread.processApplicationEvents`
    /// first processes the listener's `SeekUnvalidatedEvent` — the partition
    /// is still assigned, so the seek succeeds — and only then the
    /// completed event, whose `whenComplete` runs
    /// `clearAssignmentAndLeaveGroup()`. Applying the ack first would clear
    /// the assignment under the seek, which then fails with "No current
    /// assignment for partition t-0".
    #[tokio::test]
    async fn leave_callback_ack_is_applied_after_an_abandoned_listener_operation() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, subs, tx, _bg_rx, callback, _leave) = make_thread_with_member_preparing_to_leave();
        let t0 = crate::common::TopicPartition::new("t", 0);

        // The listener submits a seek through its handle and stops waiting
        // for it: the event stays queued, the listener returns, and the app
        // returns the callback's ack.
        let (handle, mut seek_rx) = crate::consumer::internals::events::CompletableEventHandle::<()>::new(60_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::SeekUnvalidated { handle, partition: t0.clone(), offset: 42, offset_epoch: None },
            enqueued_ms: 1_000,
        })
        .unwrap();
        return_leave_callback_ack(&tx, callback, Ok(()));

        thread.run_once().await;
        match seek_rx.try_recv() {
            Ok(Ok(())) => {},
            other => panic!(
                "the seek the listener queued must run before the leave clears the assignment (Java FIFO), got {other:?}"
            ),
        }
        let subs = subs.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty(), "the leave then cleared the assignment");
        assert!(subs.subscription().is_empty(), "and unsubscribed");
        drop(subs);
        assert!(!membership.has_pending_leave_callback(), "the leave's step was applied");
        assert_eq!(
            membership.member_epoch(),
            -1,
            "the leave heartbeat was built in the same run_once"
        );
        assert_ne!(membership.state(), MemberState::PrepareLeaving);
    }

    /// The leave still proceeds when its ack is dropped with no completed
    /// event to carry it (here the callback event itself is dropped before the
    /// app ran the listener): the bg loop applies the closed ack before the
    /// next application event, as it did before the ack moved to the
    /// application-event queue. Java has no equivalent (its callback event
    /// cannot be lost).
    #[tokio::test]
    async fn leave_callback_ack_dropped_by_the_app_is_applied_before_later_application_events() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, subs, tx, _bg_rx, callback, _leave) = make_thread_with_member_preparing_to_leave();
        drop(callback);
        let (handle, sub_rx) = crate::consumer::internals::events::CompletableEventHandle::<()>::new(60_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::TopicSubscriptionChange {
                handle,
                topics: HashSet::from(["t2".to_string()]),
                listener: None,
            },
            enqueued_ms: 1_000,
        })
        .unwrap();

        thread.run_once().await;
        assert!(matches!(sub_rx.await, Ok(Ok(()))), "subscribe() reported success");
        assert_eq!(
            subs.lock().unwrap().subscription(),
            HashSet::from(["t2".to_string()]),
            "the leave step ran before the subscribe"
        );
        assert!(!membership.has_pending_leave_callback());
        assert_eq!(membership.member_epoch(), -1, "the leave heartbeat was built");
        assert_ne!(membership.state(), MemberState::PrepareLeaving);
    }

    /// Critic pr2-c2 finding 2: Phase 1.5 of `run_once` applies a leave ack
    /// dropped with no completed event to carry it when no application event
    /// follows, before the heartbeat poll, so the leave heartbeat is built in
    /// that same iteration rather than waiting for an application event that
    /// may never come.
    #[tokio::test]
    async fn run_once_applies_a_dropped_leave_callback_ack_with_no_application_event() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, _subs, _tx, _bg_rx, callback, _leave) =
            make_thread_with_member_preparing_to_leave();
        drop(callback);

        thread.run_once().await;
        assert!(!membership.has_pending_leave_callback(), "the leave's step was applied");
        assert_eq!(
            membership.member_epoch(),
            -1,
            "the leave heartbeat was built in the same run_once"
        );
        assert_ne!(membership.state(), MemberState::PrepareLeaving);
    }

    /// Critic pr2-c2 finding 1, bg side: a leave ack completed with an error —
    /// the interrupt error the app sends when the caller's future was dropped
    /// mid-listener — still keeps its FIFO position behind the listener's
    /// queued op, and still clears the assignment and leaves, as Java's
    /// `whenComplete` in `AbstractMembershipManager.leaveGroup(boolean)` runs
    /// `clearAssignmentAndLeaveGroup()` whether the callback failed or not.
    #[tokio::test]
    async fn error_completed_leave_callback_ack_is_applied_after_the_listener_operation() {
        use crate::consumer::internals::MemberState;
        let (mut thread, membership, subs, tx, _bg_rx, callback, _leave) = make_thread_with_member_preparing_to_leave();
        let t0 = crate::common::TopicPartition::new("t", 0);
        let (handle, mut seek_rx) = crate::consumer::internals::events::CompletableEventHandle::<()>::new(60_000);
        tx.send(ApplicationEventEnvelope {
            event: ApplicationEvent::SeekUnvalidated { handle, partition: t0, offset: 42, offset_epoch: None },
            enqueued_ms: 1_000,
        })
        .unwrap();
        return_leave_callback_ack(
            &tx,
            callback,
            Err(Error::Interrupt(crate::common::errors::InterruptError::new("interrupted"))),
        );

        thread.run_once().await;
        match seek_rx.try_recv() {
            Ok(Ok(())) => {},
            other => panic!("the listener's seek must run before the leave (Java FIFO), got {other:?}"),
        }
        assert!(
            subs.lock().unwrap().assigned_partitions().is_empty(),
            "the leave cleared the assignment"
        );
        assert!(!membership.has_pending_leave_callback());
        assert_eq!(
            membership.member_epoch(),
            -1,
            "the leave heartbeat was built in the same run_once"
        );
        assert_ne!(membership.state(), MemberState::PrepareLeaving);
    }

    /// Post-poll `maybeFailOnMetadataError(uncompletedEvents)` arm
    /// (added in Phase 10 commit 8 — see module docstring).
    ///
    /// Sequence under test:
    ///   1. Build a thread with a metadata instance we hold a handle
    ///      to. Push a notifiable+completable erased handle directly
    ///      onto `notifiable_handles` (bypassing AEP dispatch, which
    ///      would otherwise synchronously complete the handle in this
    ///      minimal fixture).
    ///   2. Plant a metadata error via `metadata.fatal_error(...)`.
    ///      `delegate.poll(...)` inside `run_once` propagates it into
    ///      `delegate.metadata_error`.
    ///   3. `run_once`'s Phase-7 arm calls
    ///      `maybe_fail_on_metadata_error_uncompleted`, which observes
    ///      the live notifiable handle, consumes the delegate error,
    ///      and `complete_with_error`s the inner oneshot.
    ///   4. The app-side receiver sees the error variant intact (no
    ///      `Timeout` wrap).
    ///
    /// Java analog: `ConsumerNetworkThread.runOnce()` ending with
    /// `maybeFailOnMetadataError(applicationEventReaper.uncompletedEvents())`.
    #[tokio::test]
    async fn maybe_fail_on_metadata_error_post_poll_fans_out_to_notifiable_events() {
        // Build a fixture that exposes `metadata` so we can plant the
        // fatal error on the SAME instance the thread's delegate holds.
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        // 1. Notifiable handle with a large deadline so the reaper
        // doesn't expire it before the metadata-error arm fires.
        let (h, mut event_rx) = crate::consumer::internals::events::CompletableEventHandle::<()>::new(60_000);
        thread.push_notifiable_handle_for_test(h.erased());

        // 2. Plant the metadata error on the cluster.
        metadata
            .metadata_arc()
            .fatal_error(Error::topic_authorization(std::collections::HashSet::from(["t".to_string()])));

        // 3. Drive one runOnce iteration.
        thread.run_once().await;

        // 4. The notifiable handle was completed exceptionally with
        // the metadata error variant intact.
        let received = event_rx.try_recv().expect("sender used");
        let err = received.expect_err("post-poll arm must fail the handle");
        assert!(
            matches!(err, Error::TopicAuthorization(_)),
            "expected TopicAuthorization, got: {err:?}"
        );
    }

    /// When NO notifiable handle is tracked, the post-poll arm must
    /// NOT consume the delegate's metadata error — Java's
    /// "Don't get-and-clear the metadata error if there are no events
    /// that will be notified" optimisation
    /// (`ConsumerNetworkThread#maybeFailOnMetadataError`). Subsequent runOnce
    /// iterations (which DO register a notifiable event later) must
    /// still see the same metadata error.
    #[tokio::test]
    async fn maybe_fail_on_metadata_error_skips_delegate_when_no_notifiable_events() {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        // Plant the metadata error.
        metadata
            .metadata_arc()
            .fatal_error(Error::topic_authorization(std::collections::HashSet::from(["t".to_string()])));

        // No notifiable handles tracked. run_once must NOT consume the
        // delegate's metadata_error.
        thread.run_once().await;

        // Read the delegate's metadata_error: it must still be Some.
        let mut delegate_guard = thread.network_client_delegate.lock().await;
        let leftover = delegate_guard.get_and_clear_metadata_error();
        assert!(
            leftover.is_some(),
            "without notifiable events, the post-poll arm must NOT consume the delegate's metadata_error"
        );
    }

    /// `LeaveGroupOnClose` is a completable variant. Confirms the
    /// completable-arm coverage of [`super::events::ApplicationEvent::erased_handle`]
    /// extends beyond `AssignmentChange`.
    #[tokio::test]
    async fn leave_group_event_registered_with_reaper() {
        let (mut thread, tx, reaper, _time, _rm) = make_thread_no_membership();
        let (handle, _rx, erased_external) = CompletableEvent::make_completable_event::<()>(60_000);
        let event =
            ApplicationEvent::LeaveGroupOnClose { handle, membership_operation: GroupMembershipOperation::Default };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: 1_000 })
            .expect("send ok");
        thread.process_application_events();
        let r = reaper.lock().unwrap();
        assert!(r.contains(&erased_external), "LeaveGroupOnClose must be tracked");
    }

    // ─── Java `ConsumerNetworkThreadTest` metric tests (Phase M6) ───

    use crate::common::Metric;
    use crate::common::metrics::Metrics;
    use crate::consumer::internals::ConsumerUtils;
    use crate::consumer::internals::events::AsyncPollState;
    use crate::consumer::internals::metrics::AsyncConsumerMetrics;

    /// Java parameterizes both metric tests over
    /// `AsyncConsumerMetricsTest#groupNameProvider`; we loop the same two groups.
    fn metric_group_name_provider() -> [&'static str; 2] {
        [
            ConsumerUtils::CONSUMER_METRIC_GROUP,
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP,
        ]
    }

    /// Read a registered metric's value as `f64`.
    fn read_metric(metrics: &Metrics, name: &str, group: &str) -> f64 {
        let mn = metrics.metric_name(name, group);
        metrics
            .metric(&mn)
            .expect("metric present")
            .metric_value()
            .as_double()
            .expect("double-valued metric")
    }

    /// Java `ConsumerNetworkThreadTest#testRunOnceRecordTimeBetweenNetworkThreadPoll`.
    /// Drives two `run_once` iterations 10ms apart on the mock clock and
    /// asserts `time-between-network-thread-poll-{avg,max}` both equal 10.
    /// `@ParameterizedTest` over the two metric groups is unrolled into a
    /// loop (DoD §3). No public `metrics()` accessor is needed: the test
    /// constructs the `Metrics` registry directly and reads via
    /// `metrics.metric(metrics.metric_name(...))`, exactly like Java.
    #[tokio::test]
    async fn run_once_records_time_between_network_thread_poll() {
        for group_name in metric_group_name_provider() {
            let (mut thread, _tx, _reaper, time, _rm) = make_thread_no_membership();
            let metrics = Arc::new(Metrics::new());
            let async_metrics = Arc::new(AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name));
            thread.set_async_consumer_metrics(Arc::clone(&async_metrics), Arc::new(AtomicI64::new(0)));

            // First poll: Java's `lastPollTimeMs == 0` guard skips the
            // record; it only stamps `last_poll_time_ms`.
            thread.run_once().await;
            time.sleep(10);
            // Second poll, 10ms later: records the 10ms gap.
            thread.run_once().await;

            assert_eq!(
                read_metric(&metrics, "time-between-network-thread-poll-avg", group_name),
                10.0,
                "time-between-network-thread-poll-avg must be 10 ({group_name})"
            );
            assert_eq!(
                read_metric(&metrics, "time-between-network-thread-poll-max", group_name),
                10.0,
                "time-between-network-thread-poll-max must be 10 ({group_name})"
            );
        }
    }

    /// Java
    /// `ConsumerNetworkThreadTest#testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`.
    /// Enqueues one application event stamped at the current mock time,
    /// pre-bumps the queue-size gauge to 1, advances the clock 10ms, then
    /// runs one iteration. `run_once` drains the queue (resetting size to 0)
    /// and records the per-event queue time (`now - enqueued_ms == 10`).
    /// `@ParameterizedTest` over the two groups is unrolled into a loop.
    ///
    /// NOTE: this asserts the size gauge reads 0 after the drain, which it does
    /// because the counter is floored at 0 and this test pushes onto the raw
    /// channel without the `fetch_add(1)` that `ApplicationEventHandler::add`
    /// performs. It is therefore NOT a check of the conservation invariant — see
    /// `drain_subtracts_dequeued_events_rather_than_zeroing_the_gauge` for that.
    /// The queue-*time* assertions below are the meaningful part here.
    /// Regression: the drain must SUBTRACT what it dequeued, not overwrite the
    /// depth counter with 0.
    ///
    /// The counter is a shadow of the channel — tokio's `UnboundedSender` has no
    /// `len()`, so `ApplicationEventHandler::add` cannot read the real depth the
    /// way Java's `queue.size()` does. Java can then afford a literal
    /// `recordApplicationEventQueueSize(0)` after `drainTo`, because `drainTo`
    /// calls `fullyLock()` and nothing can arrive mid-drain. The `try_recv` loop
    /// does not block senders, so `store(0)` wiped the `fetch_add(1)` of any `add`
    /// racing the loop whose event was still queued.
    ///
    /// Made deterministic by desynchronising the two by one: the counter says 3
    /// events are enqueued while only 2 have reached the receiver — exactly the
    /// state an `add` that has incremented but whose send the receiver has not yet
    /// observed leaves behind. The old code reported 0, losing the third event;
    /// the conserved counter reports 3 - 2 = 1.
    #[tokio::test]
    async fn drain_subtracts_dequeued_events_rather_than_zeroing_the_gauge() {
        let (mut thread, tx, _reaper, time, _rm) = make_thread_no_membership();
        let metrics = Arc::new(Metrics::new());
        let async_metrics = Arc::new(AsyncConsumerMetrics::new(Arc::clone(&metrics), "consumer-metrics"));
        let queue_size = Arc::new(AtomicI64::new(0));
        thread.set_async_consumer_metrics(Arc::clone(&async_metrics), Arc::clone(&queue_size));

        let enqueued_ms = time.milliseconds();
        for _ in 0..2 {
            let event = ApplicationEvent::AsyncPoll {
                deadline_ms: enqueued_ms + 60_000,
                poll_time_ms: enqueued_ms,
                state: Arc::new(AsyncPollState::new()),
            };
            tx.send(ApplicationEventEnvelope { event, enqueued_ms }).expect("send ok");
        }
        // Three `add`s incremented; only two sends are visible to the receiver.
        queue_size.store(3, Ordering::SeqCst);

        thread.run_once().await;

        assert_eq!(
            queue_size.load(Ordering::SeqCst),
            1,
            "the drain must subtract the 2 it dequeued, leaving the 1 still in flight"
        );
        assert_eq!(
            read_metric(&metrics, "application-event-queue-size", "consumer-metrics"),
            1.0,
            "the gauge must report the event still queued, not 0"
        );
    }

    #[tokio::test]
    async fn run_once_records_application_event_queue_size_and_time() {
        for group_name in metric_group_name_provider() {
            let (mut thread, tx, _reaper, time, _rm) = make_thread_no_membership();
            let metrics = Arc::new(Metrics::new());
            let async_metrics = Arc::new(AsyncConsumerMetrics::new(Arc::clone(&metrics), group_name));
            let queue_size = Arc::new(AtomicI64::new(0));
            thread.set_async_consumer_metrics(Arc::clone(&async_metrics), Arc::clone(&queue_size));

            // Java: `AsyncPollEvent` enqueued with `setEnqueuedMs(time.milliseconds())`.
            let enqueued_ms = time.milliseconds();
            let event = ApplicationEvent::AsyncPoll {
                deadline_ms: enqueued_ms + 60_000,
                poll_time_ms: enqueued_ms,
                state: Arc::new(AsyncPollState::new()),
            };
            tx.send(ApplicationEventEnvelope { event, enqueued_ms }).expect("send ok");
            // Java: `asyncConsumerMetrics.recordApplicationEventQueueSize(1)`.
            async_metrics.record_application_event_queue_size(1);

            // Advance 10ms, then drain the queue in one iteration.
            time.sleep(10);
            thread.run_once().await;

            // Drain resets the size gauge to 0 (Java
            // `org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread#processApplicationEvents`).
            assert_eq!(
                read_metric(&metrics, "application-event-queue-size", group_name),
                0.0,
                "application-event-queue-size must reset to 0 after drain ({group_name})"
            );
            // The event spent 10ms in the queue (`now - enqueued_ms`).
            assert_eq!(
                read_metric(&metrics, "application-event-queue-time-avg", group_name),
                10.0,
                "application-event-queue-time-avg must be 10 ({group_name})"
            );
            assert_eq!(
                read_metric(&metrics, "application-event-queue-time-max", group_name),
                10.0,
                "application-event-queue-time-max must be 10 ({group_name})"
            );
        }
    }

    /// Phase-12 Issue 1 regression: `run_once` drives
    /// `CommitRequestManager::poll_with_coordinator`, draining
    /// `unsent_offset_commits` into the delegate's unsent queue.
    ///
    /// Mirrors Java's `CommitRequestManager.poll(currentTimeMs)` being
    /// called from `runOnce` via `requestManagers.entries()`. Without
    /// this wiring, `commit_sync()` / `committed()` / auto-commit return
    /// `oneshot::Receiver`s that never resolve — the test would hang
    /// (the assertion below would fail with a 0-length unsent queue).
    #[tokio::test]
    async fn run_once_drives_commit_poll_with_coordinator() {
        use std::collections::HashMap;

        use crate::common::Node;
        use crate::common::TopicPartition;
        use crate::consumer::OffsetAndMetadata;
        use crate::consumer::internals::CommitRequestManager;
        use crate::consumer::internals::CoordinatorRequestManager;

        let mut config = make_config();
        config.group_id = Some("g".to_string());
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());

        // Build a coordinator with a known coordinator node so
        // `poll_with_coordinator` proceeds past the `coordinator unknown`
        // early-return.
        let coordinator = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g".to_string()));
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));

        // Build a real commit manager (no auto-commit; this test
        // exercises the explicit `commit_sync` path).
        let commit = Arc::new(CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g".to_string(),
            None,
            Arc::new(crate::common::utils::SystemTime),
            0,
        ));

        // Enqueue a commit request — Java's `commitSync` path that lands
        // on `CommitRequestManager.unsentOffsetCommits`.
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        offsets.insert(tp.clone(), OffsetAndMetadata::new(42).expect("offset is non-negative"));
        let _commit_rx = commit.commit_sync(offsets, i64::MAX, 0);

        // Pre-condition: the delegate's unsent queue is empty.
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            Some(coordinator.clone()),
            None,
            Some(commit.clone()),
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        // Drive one iteration. `run_once` must call
        // `commit.poll_with_coordinator(coord, now)` between coordinator
        // and heartbeat (Java order), draining the unsent commit into
        // the delegate.
        thread.run_once().await;

        // Verify the delegate's unsent queue picked up the commit. Java
        // `runOnce`'s `requestManagers.entries()` walks would have
        // `addAll(pollResult)`-ed the commit-built `UnsentRequest` here.
        let delegate_guard = delegate.lock().await;
        let unsent_count = delegate_guard.unsent_requests().len() as i64;
        let inflight_count = delegate_guard.inflight_request_count() as i64;
        // The request may be inflight already (if the network client's
        // `ready(node)` returned true) or still in `unsent_requests` —
        // either way the count of (unsent + inflight) must be ≥ 1, proving
        // `poll_with_coordinator` was actually called from `run_once`.
        assert!(
            unsent_count + inflight_count >= 1,
            "expected the commit to land on the delegate (unsent={} inflight={})",
            unsent_count,
            inflight_count
        );
    }

    // ─── Milestone 17 Phase 3: no busy loop while bootstrapping (KIP-909) ───

    /// The background loop's two waits, asserted by value, for a group
    /// consumer whose bootstrap resolution has not completed (KIP-909,
    /// `bootstrap.resolve.timeout.ms > 0`): no broker node is known yet, so
    /// the FindCoordinator request cannot be sent and the coordinator stays
    /// unknown. Real coordinator, commit (auto-commit every 100 ms, so its
    /// timer keeps expiring), heartbeat (a JOINING member with Java's initial
    /// zero heartbeat interval), offsets and fetch managers run over a client
    /// with no nodes, which is what the request managers see until resolution
    /// lands (`least_loaded_node` is empty, Phase 2).
    ///
    /// Each iteration must:
    ///   - poll the network with `retry.backoff.ms` (the delegate's clamp for
    ///     the unsent FindCoordinator), not 0 — KAFKA-20253's coordinator
    ///     guard; before it, the in-flight request's elapsed backoff (0)
    ///     became the poll timeout;
    ///   - publish `retry.backoff.ms` as the application task's bound
    ///     (`cachedMaximumTimeToWait`), not 0 — the heartbeat manager
    ///     (KAFKA-21010: a JOINING member wants to heartbeat now and its
    ///     interval is 0), the commit manager (KAFKA-20970: the expired
    ///     auto-commit timer) and the fetch manager (KAFKA-20854: nothing in
    ///     flight) each give exactly that value.
    ///
    /// Before Milestone 17 Phase 3 both values were 0 here, so the background
    /// task and the application task each spun for the whole resolution
    /// window.
    #[tokio::test]
    async fn run_once_does_not_spin_while_bootstrap_resolution_is_pending() {
        let mut config = make_config();
        config.group_id = Some("g".to_string());
        config.enable_auto_commit = true;
        config.auto_commit_interval_ms = 100;
        let retry_backoff_ms = config.retry_backoff_ms();
        assert_eq!(100, retry_backoff_ms);

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let (bg_tx, _bg_rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(bg_tx));

        let coordinator = Arc::new(crate::consumer::internals::CoordinatorRequestManager::new(
            retry_backoff_ms,
            config.retry_backoff_max_ms(),
            "g",
        ));
        let commit = Arc::new(crate::consumer::internals::CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g",
            None,
            time.clone() as Arc<dyn Time>,
            time.milliseconds(),
        ));
        commit.set_coordinator(Arc::clone(&coordinator));
        let membership = Arc::new(ConsumerMembershipManager::new(
            "g".to_string(),
            None,
            None,
            30_000,
            None,
            subs.clone(),
            None,
            metadata.clone(),
            beh.clone(),
            false,
            None,
            time.clone() as Arc<dyn Time>,
        ));
        membership.transition_to_joining().unwrap();
        let heartbeat = crate::consumer::internals::ConsumerHeartbeatRequestManager::new(
            time.milliseconds(),
            &config,
            Arc::clone(&coordinator),
            subs.clone(),
            membership.clone(),
            beh,
        );
        let fetch = crate::consumer::internals::FetchRequestManager::new(
            metadata.clone(),
            subs.clone(),
            crate::consumer::internals::FetchConfig::new(
                1,
                50 * 1024 * 1024,
                500,
                1024 * 1024,
                500,
                true,
                "",
                IsolationLevel::ReadUncommitted,
            ),
            Arc::new(crate::consumer::internals::FetchBuffer::new()),
            Arc::new(crate::common::utils::internals::BufferSupplier::create()),
            Arc::new(ApiVersions::new()),
            crate::consumer::internals::FetchMetricsManager::for_test(),
            retry_backoff_ms,
        );
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            Some(Arc::clone(&coordinator)),
            None,
            Some(Arc::clone(&commit)),
            Some(heartbeat),
            Some(membership.clone()),
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            Some(fetch),
        )));

        let counting = make_counting_delegate(&config, metadata.clone());
        let poll_timeouts = counting.client_for_test_ref().poll_timeouts();
        let delegate = Arc::new(AsyncMutex::new(counting));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let cached_max_time_to_wait_ms = Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS));
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            Some(membership.clone()),
            WakeupTrigger::new(),
            Arc::clone(&cached_max_time_to_wait_ms),
        );

        // 2 s of model time in 50 ms steps: the auto-commit timer expires
        // repeatedly, and the FindCoordinator request stays in flight (its
        // 30 s request timeout is never reached).
        for step in 0..40 {
            thread.run_once().await;
            assert!(coordinator.coordinator().is_none(), "no broker, so no coordinator");
            assert_eq!(crate::consumer::internals::MemberState::Joining, membership.state());
            let last_poll_timeout = *poll_timeouts.lock().unwrap().last().expect("the network was polled");
            assert_eq!(
                retry_backoff_ms, last_poll_timeout,
                "step {step}: the background task must poll the network with retry.backoff.ms, not spin"
            );
            assert_eq!(
                retry_backoff_ms,
                cached_max_time_to_wait_ms.load(Ordering::Acquire),
                "step {step}: the application task's wait bound must be retry.backoff.ms, not 0"
            );
            time.sleep(50);
        }
    }

    /// `run_once` lends the fetch manager the delegate it holds, so a fetch
    /// is not built for a leader inside its reconnect backoff window and is
    /// built again once the window has elapsed.
    ///
    /// Java's `AbstractFetch.prepareFetchRequests` skips a node for which
    /// `NetworkClientDelegate.isUnavailable(node)` (`connectionFailed(node) &&
    /// connectionDelay(node, now) > 0`) holds — the same shape as
    /// `FetchRequestManagerTest.testFetchSkipsBlackedOutNodes`, which drives
    /// `client.backoff(node, 500)` on a `MockClient`. Before the delegate was
    /// lent to the fetch manager, the production predicate was `|_| false`, so
    /// the fetch was built on every iteration and failed at once by the
    /// delegate's disconnect check (a busy loop while the leader was down).
    /// The second half pins that the fix does not stop fetching for good.
    #[tokio::test]
    async fn run_once_skips_fetch_to_node_in_reconnect_backoff_until_it_elapses() {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));

        // One broker (node 0) leading `test-0`, assigned with a validated
        // position so the partition is fetchable.
        let tp = crate::common::TopicPartition::new("test".to_string(), 0);
        metadata.add_transient_topics(HashSet::from(["test".to_string()]));
        let response = crate::common::requests::RequestTestUtils::metadata_update_with_ids(
            "dummy",
            1,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::from([("test".to_string(), 1)]),
            &|_tp| Some(0),
            &std::collections::HashMap::from([("test".to_string(), crate::common::Uuid::random_uuid())]),
        );
        metadata
            .metadata_arc()
            .update_with_current_request_version(&response, false, time.milliseconds());
        let leader = metadata
            .metadata_arc()
            .fetch()
            .leader_for(&tp)
            .cloned()
            .expect("node 0 leads test-0");
        {
            let mut guard = subs.lock().unwrap();
            guard.assign_from_user(HashSet::from([tp.clone()])).unwrap();
            guard
                .seek_validated(
                    &tp,
                    crate::consumer::internals::FetchPosition::with_leader(
                        0,
                        Some(0),
                        crate::metadata::LeaderAndEpoch::new(Some(leader.clone()), Some(0)),
                    ),
                )
                .unwrap();
        }

        let fetch = crate::consumer::internals::FetchRequestManager::new(
            metadata.clone(),
            subs.clone(),
            crate::consumer::internals::FetchConfig::new(
                1,
                50 * 1024 * 1024,
                500,
                1024 * 1024,
                500,
                true,
                "",
                IsolationLevel::ReadUncommitted,
            ),
            Arc::new(crate::consumer::internals::FetchBuffer::new()),
            Arc::new(crate::common::utils::internals::BufferSupplier::create()),
            Arc::new(ApiVersions::new()),
            crate::consumer::internals::FetchMetricsManager::for_test(),
            config.retry_backoff_ms(),
        );
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(fetch),
        )));

        // Java's `client.backoff(node, 500)`: the leader's connection failed
        // and is inside a 500 ms reconnect backoff, on the consumer's clock.
        let mut client = MockClient::with_static_nodes(Vec::<Node>::new(), time.clone() as Arc<dyn Time>);
        client.backoff(&leader, 500);
        let (bg_tx, _bg_rx) = mpsc::unbounded_channel();
        let delegate = Arc::new(AsyncMutex::new(NetworkClientDelegate::new(
            &config,
            client,
            metadata.metadata_arc(),
            Arc::new(BackgroundEventHandler::new(bg_tx)),
            false,
        )));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            Arc::clone(&delegate),
            request_managers.clone(),
            None,
            WakeupTrigger::new(),
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );
        let create_fetch_requests = || {
            let mut guard = request_managers.lock().unwrap();
            guard.fetch.as_mut().expect("fetch manager").create_fetch_requests()
        };
        let fetch_in_flight_to_leader = || {
            let guard = request_managers.lock().unwrap();
            let fetch = guard.fetch.as_ref().expect("fetch manager");
            fetch.abstract_fetch().nodes_with_pending_fetch_requests.contains(&leader.id())
        };

        // Inside the backoff window: no fetch is built, and the request for
        // fetch requests still completes (Java completes the future and
        // returns an empty poll result).
        for step in 0..5 {
            let mut ack = create_fetch_requests();
            thread.run_once().await;
            assert!(
                delegate.lock().await.is_unavailable(&leader, time.milliseconds()),
                "step {step}: the delegate reports node 0 as unavailable"
            );
            assert!(
                !fetch_in_flight_to_leader(),
                "step {step}: Java's AbstractFetch.prepareFetchRequests skips a node that \
                 NetworkClientDelegate.isUnavailable reports in reconnect backoff, but a fetch was built for it"
            );
            assert!(
                matches!(ack.try_recv(), Ok(Ok(()))),
                "step {step}: the createFetchRequests future completes successfully"
            );
            time.sleep(50);
        }

        // The backoff has elapsed: the node is connectable again, so the next
        // poll builds the fetch (Java: `isUnavailable` is false once
        // `connectionDelay` is 0).
        time.sleep(500);
        assert!(!delegate.lock().await.is_unavailable(&leader, time.milliseconds()));
        let mut ack = create_fetch_requests();
        thread.run_once().await;
        assert!(
            fetch_in_flight_to_leader(),
            "once node 0's reconnect backoff has elapsed, the fetch to it must be built again"
        );
        assert!(matches!(ack.try_recv(), Ok(Ok(()))));
    }

    /// COMMENTS.93 Issue 2: `run_once` Phase 5 consults the commit manager
    /// (Java's `entries()` includes it; `RequestManagers::entries()` skips the
    /// `Arc`-shared slot). Here it is the binding minimum: no heartbeat or
    /// fetch manager, and the offsets manager reports `i64::MAX`. With the
    /// coordinator known, the published bound is the auto-commit timer's
    /// remainder; with it unknown, `retry.backoff.ms` (KAFKA-20970 /
    /// KAFKA-21010). Without the consult the bound would be `i64::MAX`.
    #[tokio::test]
    async fn run_once_maximum_time_to_wait_includes_the_commit_manager() {
        let mut config = make_config();
        config.group_id = Some("g".to_string());
        config.enable_auto_commit = true;
        config.auto_commit_interval_ms = 300;
        let retry_backoff_ms = config.retry_backoff_ms();

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let coordinator = Arc::new(crate::consumer::internals::CoordinatorRequestManager::new(
            retry_backoff_ms,
            config.retry_backoff_max_ms(),
            "g",
        ));
        coordinator.set_coordinator_for_test(Node::new(1, "localhost".to_string(), 9092));
        let commit = Arc::new(crate::consumer::internals::CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g",
            None,
            time.clone() as Arc<dyn Time>,
            time.milliseconds(),
        ));
        commit.set_coordinator(Arc::clone(&coordinator));
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            Some(Arc::clone(&coordinator)),
            None,
            Some(Arc::clone(&commit)),
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let cached_max_time_to_wait_ms = Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS));
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            None,
            WakeupTrigger::new(),
            Arc::clone(&cached_max_time_to_wait_ms),
        );

        // Coordinator known: the auto-commit timer's remainder.
        thread.run_once().await;
        assert_eq!(300, cached_max_time_to_wait_ms.load(Ordering::Acquire));
        time.sleep(120);
        thread.run_once().await;
        assert_eq!(180, cached_max_time_to_wait_ms.load(Ordering::Acquire));

        // Coordinator unknown: the retry backoff, even once the timer expires.
        coordinator.mark_coordinator_unknown("test", time.milliseconds());
        time.sleep(500);
        thread.run_once().await;
        assert_eq!(retry_backoff_ms, cached_max_time_to_wait_ms.load(Ordering::Acquire));
    }

    /// Shared setup for the fetch-manager `run_once` / `cleanup` tests:
    /// `num_nodes` brokers with node 0 leading `test-0`, which is assigned
    /// with a validated position so it is fetchable, and a
    /// `FetchRequestManager` over that state. Returns the partition, its
    /// leader, the topic id and the manager.
    fn fetchable_partition_on_node_0(
        config: &ConsumerConfig,
        subs: &Arc<Mutex<SubscriptionState>>,
        metadata: &Arc<ConsumerMetadata>,
        now_ms: i64,
        num_nodes: i32,
    ) -> (
        crate::common::TopicPartition,
        Node,
        crate::common::Uuid,
        crate::consumer::internals::FetchRequestManager,
    ) {
        let tp = crate::common::TopicPartition::new("test".to_string(), 0);
        let topic_id = crate::common::Uuid::random_uuid();
        metadata.add_transient_topics(HashSet::from(["test".to_string()]));
        let response = crate::common::requests::RequestTestUtils::metadata_update_with_ids(
            "dummy",
            num_nodes,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::from([("test".to_string(), 1)]),
            &|_tp| Some(0),
            &std::collections::HashMap::from([("test".to_string(), topic_id)]),
        );
        metadata
            .metadata_arc()
            .update_with_current_request_version(&response, false, now_ms);
        let leader = metadata
            .metadata_arc()
            .fetch()
            .leader_for(&tp)
            .cloned()
            .expect("node 0 leads test-0");
        assert_eq!(0, leader.id());
        {
            let mut guard = subs.lock().unwrap();
            guard.assign_from_user(HashSet::from([tp.clone()])).unwrap();
            guard
                .seek_validated(
                    &tp,
                    crate::consumer::internals::FetchPosition::with_leader(
                        0,
                        Some(0),
                        crate::metadata::LeaderAndEpoch::new(Some(leader.clone()), Some(0)),
                    ),
                )
                .unwrap();
        }
        let fetch = crate::consumer::internals::FetchRequestManager::new(
            metadata.clone(),
            subs.clone(),
            crate::consumer::internals::FetchConfig::new(
                1,
                50 * 1024 * 1024,
                500,
                1024 * 1024,
                500,
                true,
                "",
                IsolationLevel::ReadUncommitted,
            ),
            Arc::new(crate::consumer::internals::FetchBuffer::new()),
            Arc::new(crate::common::utils::internals::BufferSupplier::create()),
            Arc::new(ApiVersions::new()),
            crate::consumer::internals::FetchMetricsManager::for_test(),
            config.retry_backoff_ms(),
        );
        (tp, leader, topic_id, fetch)
    }

    /// A `ConsumerNetworkThread` over a real `NetworkClientDelegate` wrapping
    /// `client`, without membership. Returns the thread, its delegate and the
    /// cached `maximumTimeToWait` it publishes.
    fn mock_client_thread(
        config: &ConsumerConfig,
        subs: &Arc<Mutex<SubscriptionState>>,
        metadata: &Arc<ConsumerMetadata>,
        time: &Arc<MockTime>,
        request_managers: &Arc<Mutex<RequestManagers>>,
        client: MockClient,
    ) -> (
        ConsumerNetworkThread<MockClient>,
        Arc<AsyncMutex<NetworkClientDelegate<MockClient>>>,
        Arc<AtomicI64>,
    ) {
        let (bg_tx, _bg_rx) = mpsc::unbounded_channel();
        let delegate = Arc::new(AsyncMutex::new(NetworkClientDelegate::new(
            config,
            client,
            metadata.metadata_arc(),
            Arc::new(BackgroundEventHandler::new(bg_tx)),
            false,
        )));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let cached_max_time_to_wait_ms = Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS));
        let thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            Arc::clone(&delegate),
            request_managers.clone(),
            None,
            WakeupTrigger::new(),
            Arc::clone(&cached_max_time_to_wait_ms),
        );
        (thread, delegate, cached_max_time_to_wait_ms)
    }

    /// COMMENTS c3-05 Issue 1: `run_once` Phase 5 consults the fetch manager
    /// (Java's `entries()` includes it, `ConsumerNetworkThread#runOnce`;
    /// `RequestManagers::entries()` skips the fetch slot). Here it is the
    /// binding minimum: no coordinator, commit, heartbeat or topic-metadata
    /// manager, and the offsets manager reports `i64::MAX`. With no fetch in
    /// flight, Java's `FetchRequestManager.maximumTimeToWait` returns
    /// `retryBackoffMs` so the application thread re-evaluates subscription
    /// state promptly (KAFKA-20854); with a fetch in flight, `Long.MAX_VALUE`.
    /// Without the consult the published bound would be `i64::MAX` throughout.
    #[tokio::test]
    async fn run_once_maximum_time_to_wait_includes_the_fetch_manager() {
        let config = make_config();
        let retry_backoff_ms = config.retry_backoff_ms();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let (_tp, leader, _topic_id, fetch) =
            fetchable_partition_on_node_0(&config, &subs, &metadata, time.milliseconds(), 1);
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            Some(fetch),
        )));
        let client = MockClient::with_static_nodes(Vec::<Node>::new(), time.clone() as Arc<dyn Time>);
        let (mut thread, _delegate, cached_max_time_to_wait_ms) =
            mock_client_thread(&config, &subs, &metadata, &time, &request_managers, client);

        // No fetch in flight: the fetch manager's retry backoff is the bound.
        thread.run_once().await;
        assert_eq!(
            retry_backoff_ms,
            cached_max_time_to_wait_ms.load(Ordering::Acquire),
            "Java's FetchRequestManager.maximumTimeToWait returns retryBackoffMs with no fetch in flight"
        );

        // A fetch is sent to node 0 and its response does not arrive: the
        // fetch manager no longer bounds the wait.
        let mut ack = request_managers
            .lock()
            .unwrap()
            .fetch
            .as_mut()
            .expect("fetch manager")
            .create_fetch_requests();
        thread.run_once().await;
        assert!(matches!(ack.try_recv(), Ok(Ok(()))));
        assert!(
            request_managers
                .lock()
                .unwrap()
                .fetch
                .as_ref()
                .expect("fetch manager")
                .abstract_fetch()
                .nodes_with_pending_fetch_requests
                .contains(&leader.id()),
            "the fetch to node 0 is in flight"
        );
        assert_eq!(
            i64::MAX,
            cached_max_time_to_wait_ms.load(Ordering::Acquire),
            "Java's FetchRequestManager.maximumTimeToWait returns Long.MAX_VALUE with a fetch in flight"
        );
    }

    /// Java `FetchRequestManagerTest.testFetcherCloseClosesFetchSessionsInBroker`,
    /// driven through `ConsumerNetworkThread::cleanup` (Java's
    /// `ConsumerNetworkThread#runAtClose` over `requestManagers.entries()`,
    /// called from `ConsumerNetworkThread#cleanup`). A normal fetch
    /// establishes a session with node 0; closing then sends node 0 a request
    /// carrying that session id, `FINAL_EPOCH` and no partition data.
    ///
    /// Rust addition: node 1 also holds a session but is in reconnect backoff,
    /// so its close request is skipped (`FetchRequestManager.pollOnClose` →
    /// `AbstractFetch.prepareCloseFetchSessionRequests` skips a node for which
    /// `isUnavailable(node)` holds).
    #[tokio::test]
    async fn test_fetcher_close_closes_fetch_sessions_in_broker() {
        use crate::common::requests::{AbstractRequest, ConcreteResponse, FetchMetadata, FetchResponse};
        use crate::fetch_response_data::{FetchableTopicResponse, PartitionData as RespPartitionData};

        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let (tp, leader, topic_id, mut fetch) =
            fetchable_partition_on_node_0(&config, &subs, &metadata, time.milliseconds(), 2);
        let backed_off = metadata
            .metadata_arc()
            .fetch()
            .node_by_id(1)
            .cloned()
            .expect("node 1 is in the cluster");
        let _ = fetch.abstract_fetch_mut().session_handler_or_create(backed_off.id());
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(fetch),
        )));
        let mut client = MockClient::with_static_nodes(Vec::<Node>::new(), time.clone() as Arc<dyn Time>);
        client.backoff(&backed_off, 500);
        let session_id = 123;
        let mut partition = RespPartitionData::new();
        partition.set_partition_index(tp.partition());
        partition.set_error_code(crate::common::protocol::Errors::None.code());
        partition.set_high_watermark(100);
        partition.set_last_stable_offset(100);
        partition.set_log_start_offset(0);
        partition.set_preferred_read_replica(FetchResponse::INVALID_PREFERRED_REPLICA_ID);
        let mut topic = FetchableTopicResponse::new();
        topic.set_topic(tp.topic().to_string());
        topic.set_topic_id(topic_id);
        topic.set_partitions(vec![partition]);
        let mut data = crate::FetchResponseData::new();
        data.set_error_code(crate::common::protocol::Errors::None.code());
        data.set_throttle_time_ms(0);
        data.set_session_id(session_id);
        data.set_responses(vec![topic]);
        client.prepare_response(ConcreteResponse::Fetch(FetchResponse::new(data)));
        let (mut thread, delegate, _cached) =
            mock_client_thread(&config, &subs, &metadata, &time, &request_managers, client);

        // Normal fetch: sent, answered, and its completion drained by the
        // next `poll`, which establishes the session with node 0.
        let mut ack = request_managers
            .lock()
            .unwrap()
            .fetch
            .as_mut()
            .expect("fetch manager")
            .create_fetch_requests();
        let session_established = || {
            request_managers
                .lock()
                .unwrap()
                .fetch
                .as_ref()
                .expect("fetch manager")
                .abstract_fetch()
                .session_handler(leader.id())
                .is_some_and(|handler| handler.session_id() == session_id)
        };
        for _ in 0..50 {
            thread.run_once().await;
            if session_established() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(matches!(ack.try_recv(), Ok(Ok(()))));
        assert!(
            session_established(),
            "the fetch response establishes session {session_id} with node 0"
        );
        assert!(!delegate.lock().await.has_any_pending_requests());

        // Java's `runAtClose(...)` followed by `networkClientDelegate.poll(...)`:
        // `cleanup` builds the close requests and polls once (zero close
        // timeout), sending them.
        thread.set_close_timeout_ms(0);
        thread.cleanup().await;

        // A request built for a node marks it pending (Java's
        // `createFetchRequest`). The delegate does not hand a request for a
        // node in backoff to the client, so the pending set is where a
        // wrongly built close request for node 1 shows.
        {
            let rm_guard = request_managers.lock().unwrap();
            let pending = &rm_guard
                .fetch
                .as_ref()
                .expect("fetch manager")
                .abstract_fetch()
                .nodes_with_pending_fetch_requests;
            assert!(pending.contains(&leader.id()), "a close-session request is built for node 0");
            assert!(
                !pending.contains(&backed_off.id()),
                "Java's AbstractFetch.prepareCloseFetchSessionRequests skips node 1, which isUnavailable \
                 (reconnect backoff), but a close-session request was built for it"
            );
        }
        let mut guard = delegate.lock().await;
        let fetches: Vec<_> = guard
            .client_for_test_mut()
            .requests_mut()
            .iter_mut()
            .filter(|r| *r.api_key() == crate::common::protocol::ApiKeys::FETCH)
            .collect();
        assert_eq!(
            1,
            fetches.len(),
            "exactly one close-session request is sent, to node 0; node 1 is in reconnect backoff"
        );
        let close = fetches.into_iter().next().unwrap();
        assert_eq!(leader.id_string(), close.destination());
        let AbstractRequest::Fetch(request) = close.request_builder_mut().build().expect("build the fetch request")
        else {
            panic!("a fetch request was sent");
        };
        // session Id is the same
        assert_eq!(session_id, request.metadata().session_id());
        // contains final epoch: it indicates we want to close the session
        assert_eq!(FetchMetadata::FINAL_EPOCH, request.metadata().epoch());
        // partition data should be empty
        assert!(
            request.data().topics().is_empty(),
            "the close request carries no partition data"
        );
    }

    /// Close-time request order, with the group coordinator also leading the
    /// fetched partition (both on node 0). In Java an `OffsetCommit` still
    /// pending at close is queued ahead of every request `runAtClose` builds:
    /// `CommitRequestManager.poll` drains it while `closing`
    /// (`drainPendingOffsetCommitRequests`), and `runAtClose` walks
    /// `requestManagers.entries()`, where the commit manager precedes the
    /// fetch manager (`RequestManagers` constructor,
    /// `ConsumerNetworkThread.runAtClose`). Rust's `cleanup` sweeps the commit
    /// manager's unsent commits itself, so that sweep must come before the
    /// fetch manager's `pollOnClose` and the `OffsetCommit` must reach node 0
    /// before the fetch-session close request.
    #[tokio::test]
    async fn cleanup_queues_the_offset_commit_before_the_fetch_session_close() {
        use std::collections::HashMap;

        use crate::common::protocol::ApiKeys;
        use crate::common::requests::{ConcreteResponse, FetchResponse};
        use crate::consumer::OffsetAndMetadata;
        use crate::consumer::internals::CommitRequestManager;
        use crate::consumer::internals::CoordinatorRequestManager;
        use crate::fetch_response_data::{FetchableTopicResponse, PartitionData as RespPartitionData};

        let mut config = make_config();
        config.group_id = Some("g".to_string());
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let (tp, leader, topic_id, fetch) =
            fetchable_partition_on_node_0(&config, &subs, &metadata, time.milliseconds(), 1);
        // The coordinator is node 0, the leader of `test-0`.
        let coordinator = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g".to_string()));
        coordinator.set_coordinator_for_test(leader.clone());
        let commit = Arc::new(CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g".to_string(),
            None,
            Arc::new(crate::common::utils::SystemTime),
            0,
        ));
        commit.set_coordinator(coordinator.clone());
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            Some(coordinator),
            None,
            Some(commit.clone()),
            None,
            None,
            None,
            Some(fetch),
        )));
        let mut client = MockClient::with_static_nodes(Vec::<Node>::new(), time.clone() as Arc<dyn Time>);
        let session_id = 123;
        let mut partition = RespPartitionData::new();
        partition.set_partition_index(tp.partition());
        partition.set_error_code(crate::common::protocol::Errors::None.code());
        partition.set_high_watermark(100);
        partition.set_last_stable_offset(100);
        partition.set_log_start_offset(0);
        partition.set_preferred_read_replica(FetchResponse::INVALID_PREFERRED_REPLICA_ID);
        let mut topic = FetchableTopicResponse::new();
        topic.set_topic(tp.topic().to_string());
        topic.set_topic_id(topic_id);
        topic.set_partitions(vec![partition]);
        let mut data = crate::FetchResponseData::new();
        data.set_error_code(crate::common::protocol::Errors::None.code());
        data.set_throttle_time_ms(0);
        data.set_session_id(session_id);
        data.set_responses(vec![topic]);
        client.prepare_response(ConcreteResponse::Fetch(FetchResponse::new(data)));
        let (mut thread, delegate, _cached) =
            mock_client_thread(&config, &subs, &metadata, &time, &request_managers, client);

        // A normal fetch establishes a session with node 0, so closing has a
        // fetch session to close there.
        let mut ack = request_managers
            .lock()
            .unwrap()
            .fetch
            .as_mut()
            .expect("fetch manager")
            .create_fetch_requests();
        let session_established = || {
            request_managers
                .lock()
                .unwrap()
                .fetch
                .as_ref()
                .expect("fetch manager")
                .abstract_fetch()
                .session_handler(leader.id())
                .is_some_and(|handler| handler.session_id() == session_id)
        };
        for _ in 0..50 {
            thread.run_once().await;
            if session_established() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(matches!(ack.try_recv(), Ok(Ok(()))));
        assert!(session_established(), "the fetch response establishes a session with node 0");
        assert!(!delegate.lock().await.has_any_pending_requests());
        delegate.lock().await.client_for_test_mut().requests_mut().clear();

        // An offset commit is still unsent when the consumer closes.
        let mut offsets: HashMap<_, OffsetAndMetadata> = HashMap::new();
        offsets.insert(tp.clone(), OffsetAndMetadata::new(42).expect("offset is non-negative"));
        let _commit_rx = commit.commit_sync(offsets, i64::MAX, time.milliseconds());

        thread.set_close_timeout_ms(0);
        thread.cleanup().await;

        let mut guard = delegate.lock().await;
        let sent: Vec<(ApiKeys, String)> = guard
            .client_for_test_mut()
            .requests()
            .iter()
            .map(|r| (*r.api_key(), r.destination().to_string()))
            .collect();
        assert_eq!(
            vec![
                (ApiKeys::OFFSET_COMMIT, leader.id_string().to_string()),
                (ApiKeys::FETCH, leader.id_string().to_string()),
            ],
            sent,
            "Java queues the pending OffsetCommit ahead of the fetch-session close request \
             (the commit manager precedes the fetch manager in RequestManagers.entries())"
        );
    }

    /// Java's `ConsumerNetworkThread.cleanup` ends with
    /// `closeQuietly(requestManagers, ...)`, after the final network poll, and
    /// `RequestManagers.close` closes every `Closeable` entry: the fetch
    /// manager's `AbstractFetch.close` closes the fetch buffer (dropping its
    /// buffered data) and the decompression buffer supplier. The request
    /// managers are shared with the application side, so their `Drop` does not
    /// stand in for this close.
    #[tokio::test]
    async fn cleanup_closes_the_fetch_manager() {
        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let (tp, _leader, _topic_id, fetch) =
            fetchable_partition_on_node_0(&config, &subs, &metadata, time.milliseconds(), 1);
        let fetch_buffer = Arc::clone(&fetch.abstract_fetch().fetch_buffer);
        fetch_buffer.add(crate::consumer::internals::CompletedFetch::new(
            tp,
            crate::fetch_response_data::PartitionData::new(),
        ));
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(fetch),
        )));
        let client = MockClient::with_static_nodes(Vec::<Node>::new(), time.clone() as Arc<dyn Time>);
        let (mut thread, _delegate, _cached) =
            mock_client_thread(&config, &subs, &metadata, &time, &request_managers, client);

        thread.set_close_timeout_ms(0);
        thread.cleanup().await;

        let rm_guard = request_managers.lock().unwrap();
        assert!(rm_guard.is_closed());
        assert!(
            rm_guard.fetch.as_ref().expect("fetch manager").abstract_fetch().closed,
            "Java's cleanup closes the request managers, and with them the fetch manager (AbstractFetch.close)"
        );
        assert!(
            fetch_buffer.is_empty(),
            "Java's AbstractFetch.close closes the fetch buffer, which drops its buffered data"
        );
    }

    /// Audit finding C4-01 / KAFKA-18641, through the real `run_once`: the
    /// background loop never starts the interval auto-commit on its own (Java's
    /// `CommitRequestManager.poll` only drains queued requests), so records
    /// the application is still processing are not committed behind its back.
    /// The fix must not swing the other way either:
    ///   - with the timer expired, the bound published to the application
    ///     task (`cachedMaximumTimeToWait`) is 0, so a long `poll()` with no
    ///     new records wakes and submits an `AsyncPollEvent`;
    ///   - that event (`ApplicationEventProcessor.process(AsyncPollEvent)` →
    ///     `updateTimerAndMaybeCommit`) starts exactly one auto-commit, which
    ///     the following iterations send and do not duplicate.
    #[tokio::test]
    async fn run_once_auto_commits_only_on_the_application_poll_event() {
        use crate::common::TopicPartition;
        use crate::common::protocol::ApiKeys;

        let mut config = make_config();
        config.group_id = Some("g".to_string());
        config.enable_auto_commit = true;
        config.auto_commit_interval_ms = 300;
        let retry_backoff_ms = config.retry_backoff_ms();

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            s.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            s.seek(&tp, 10).expect("seek");
        }
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let coordinator = Arc::new(crate::consumer::internals::CoordinatorRequestManager::new(
            retry_backoff_ms,
            config.retry_backoff_max_ms(),
            "g",
        ));
        coordinator.set_coordinator_for_test(Node::new(1, "localhost".to_string(), 9092));
        let commit = Arc::new(crate::consumer::internals::CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g",
            None,
            time.clone() as Arc<dyn Time>,
            time.milliseconds(),
        ));
        commit.set_coordinator(Arc::clone(&coordinator));
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            Some(Arc::clone(&coordinator)),
            None,
            Some(Arc::clone(&commit)),
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(make_delegate(&config, metadata.clone())));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let cached_max_time_to_wait_ms = Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS));
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            WakeupTrigger::new(),
            Arc::clone(&cached_max_time_to_wait_ms),
        );
        // OffsetCommit requests handed to the network layer so far: still
        // unsent in the delegate, or already sent to the mock client (which
        // never answers them).
        let offset_commits_staged = || async {
            let guard = delegate.lock().await;
            let unsent = guard
                .unsent_requests()
                .iter()
                .filter(|r| r.request_builder().is_some_and(|b| b.api_key() == &ApiKeys::OFFSET_COMMIT))
                .count();
            let sent = guard
                .client_for_test_ref()
                .requests()
                .iter()
                .filter(|r| r.api_key() == &ApiKeys::OFFSET_COMMIT)
                .count();
            unsent + sent
        };

        thread.run_once().await;
        assert_eq!(300, cached_max_time_to_wait_ms.load(Ordering::Acquire));

        // The interval expires while the application is still processing the
        // records of its last poll(): the background loop keeps running, but
        // must not commit.
        time.sleep(400);
        for _ in 0..3 {
            thread.run_once().await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            0,
            offset_commits_staged().await,
            "Java: the background loop never starts an auto-commit; only the application's poll()/assign() does"
        );
        assert_eq!(
            0,
            cached_max_time_to_wait_ms.load(Ordering::Acquire),
            "the expired auto-commit timer must bound the application task's wait, so a long poll() wakes up"
        );

        // The application's poll loop submits an AsyncPollEvent.
        let now_ms = time.milliseconds();
        let event = ApplicationEvent::AsyncPoll {
            deadline_ms: now_ms + 60_000,
            poll_time_ms: now_ms,
            state: Arc::new(AsyncPollState::new()),
        };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: now_ms })
            .expect("send ok");
        // The event's processing runs on a spawned task; the auto-commit it
        // enqueues is sent by a following iteration.
        for _ in 0..16 {
            thread.run_once().await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            1,
            offset_commits_staged().await,
            "the application poll event must start exactly one auto-commit"
        );
        assert_eq!(
            300,
            cached_max_time_to_wait_ms.load(Ordering::Acquire),
            "starting the auto-commit resets the interval timer"
        );
    }

    /// Audit finding C4-01 follow-up: in Java the network thread processes an
    /// `AsyncPollEvent` (which starts the interval auto-commit through
    /// `updateTimerAndMaybeCommit`) and then polls the request managers in the
    /// same `runOnce`, so the OffsetCommit is sent immediately. In Rust the
    /// event's work runs on a spawned task that can enqueue the auto-commit
    /// after the bg loop already polled the commit manager; the commit manager
    /// must then wake the loop, or the commit waits until something else ends
    /// the network poll (up to `MAX_POLL_TIMEOUT_MS`).
    ///
    /// The network poll here never returns on its own (a socket wait with no
    /// data, idle otherwise), and the commit manager is given the client's
    /// wakeup handle exactly as the production constructor does, so only that
    /// wake can get the commit out. The deadline is far below the 5 s max poll
    /// timeout.
    #[tokio::test]
    async fn auto_commit_from_application_poll_event_is_sent_promptly() {
        use crate::common::TopicPartition;
        use crate::common::protocol::ApiKeys;

        let mut config = make_config();
        config.group_id = Some("g".to_string());
        config.enable_auto_commit = true;
        config.auto_commit_interval_ms = 300;
        let retry_backoff_ms = config.retry_backoff_ms();

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            s.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            s.seek(&tp, 10).expect("seek");
        }
        let metadata = make_metadata(&config, subs.clone());
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let coordinator = Arc::new(crate::consumer::internals::CoordinatorRequestManager::new(
            retry_backoff_ms,
            config.retry_backoff_max_ms(),
            "g",
        ));
        coordinator.set_coordinator_for_test(Node::new(1, "localhost".to_string(), 9092));
        let commit = Arc::new(crate::consumer::internals::CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g",
            None,
            time.clone() as Arc<dyn Time>,
            time.milliseconds(),
        ));
        commit.set_coordinator(Arc::clone(&coordinator));
        let counting_delegate = make_counting_delegate(&config, metadata.clone());
        // The network poll blocks until woken, emulating an idle socket wait.
        counting_delegate
            .client_for_test_ref()
            .poll_block()
            .store(true, Ordering::SeqCst);
        // The one wakeup primitive, taken from the delegate and installed in
        // the commit manager exactly as the production constructor does.
        let event_notify = counting_delegate.wakeup_handle();
        commit.set_completion_notify(Arc::clone(&event_notify));
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            Some(Arc::clone(&coordinator)),
            None,
            Some(Arc::clone(&commit)),
            None,
            None,
            Some(make_offsets_manager(&config, subs.clone(), metadata.clone())),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(counting_delegate));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            WakeupTrigger::new(),
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        // The interval has expired; the application calls poll(), whose
        // `ApplicationEventHandler::add` enqueues the AsyncPoll event and pokes
        // the wakeup handle.
        time.sleep(400);
        let now_ms = time.milliseconds();
        let event = ApplicationEvent::AsyncPoll {
            deadline_ms: now_ms + 60_000,
            poll_time_ms: now_ms,
            state: Arc::new(AsyncPollState::new()),
        };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: now_ms })
            .expect("send ok");
        event_notify.notify_one();

        // Drive the bg loop until the commit manager has handed the
        // auto-commit to the network layer: the event reset the timer (so the
        // wait bound is the full interval again) and nothing is left unsent in
        // the commit manager. Then stop the loop between iterations — releasing
        // the blocked poll with the wakeup handle, as `close()` does — rather
        // than cancelling `run_once` mid-poll.
        let stop = AtomicBool::new(false);
        let mut handed_over = false;
        // No yield between iterations: on this current-thread runtime the
        // event's spawned task then runs only once the loop has polled the
        // commit manager and parked in the network poll — the ordering in
        // which the auto-commit is enqueued too late for this iteration.
        let driver = async {
            while !stop.load(Ordering::SeqCst) {
                thread.run_once().await;
            }
        };
        let watcher = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                let now_ms = time.milliseconds();
                if commit.maximum_time_to_wait(now_ms) == 300 && commit.unsent_offset_commits_len_for_test() == 0 {
                    handed_over = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            stop.store(true, Ordering::SeqCst);
            event_notify.notify_one();
        };
        tokio::join!(driver, watcher);

        assert!(
            handed_over,
            "Java sends the auto-commit in the same runOnce that processes the AsyncPollEvent; \
             the bg loop must be woken to send it instead of waiting out the network poll"
        );
        let guard = delegate.lock().await;
        let staged = guard
            .unsent_requests()
            .iter()
            .filter(|r| r.request_builder().is_some_and(|b| b.api_key() == &ApiKeys::OFFSET_COMMIT))
            .count()
            + guard
                .client_for_test_ref()
                .inner
                .requests()
                .iter()
                .filter(|r| r.api_key() == &ApiKeys::OFFSET_COMMIT)
                .count();
        assert_eq!(1, staged, "the application poll event must start exactly one auto-commit");
    }

    /// Audit finding C4-14: a lag monitor calling `end_offsets` on an
    /// otherwise idle consumer. In Java the `ListOffsets` response is handled
    /// in the request future's `whenComplete` on the network thread
    /// (`OffsetsRequestManager.buildListOffsetsRequests`), so the call returns
    /// one round trip later. In Rust a spawned forwarder queues the response
    /// for the offsets manager's next `poll`; unless it wakes the bg loop, the
    /// result waits until something else ends the network poll — up to
    /// `MAX_POLL_TIMEOUT_MS` (5 s), so `end_offsets_with_timeout(tps, 2s)`
    /// always timed out.
    ///
    /// The network poll here never returns on its own (a socket wait with no
    /// data, idle otherwise) and the offsets manager is given the client's
    /// wakeup handle exactly as the production constructor does, so only that
    /// wake can complete the lookup. The deadline is far below the 5 s max
    /// poll timeout.
    #[tokio::test]
    async fn list_offsets_event_completes_on_response_on_idle_background_task() {
        use std::collections::HashMap;

        use crate::ListOffsetsResponseData;
        use crate::common::TopicPartition;
        use crate::common::requests::{ConcreteResponse, ListOffsetsRequest, ListOffsetsResponse, RequestTestUtils};
        use crate::consumer::internals::events::CompletableEventHandle;
        use crate::list_offsets_response_data::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};

        let config = make_config();
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());
        // `t1-0` has a known leader (node 0), so the lookup is sent at once.
        metadata.add_transient_topics(HashSet::from(["t1".to_string()]));
        let metadata_response = RequestTestUtils::metadata_update_with(1, &HashMap::from([("t1".to_string(), 1)]));
        metadata
            .metadata_arc()
            .update_with_current_request_version(&metadata_response, false, 0);
        let tp = TopicPartition::new("t1".to_string(), 0);

        // The broker answers the ListOffsets request with end offset 7.
        let mut partition = ListOffsetsPartitionResponse::new();
        partition.set_partition_index(0);
        partition.set_timestamp(ListOffsetsResponse::UNKNOWN_TIMESTAMP);
        partition.set_offset(7);
        partition.set_leader_epoch(ListOffsetsResponse::UNKNOWN_EPOCH);
        let mut topic = ListOffsetsTopicResponse::new();
        topic.set_name("t1".to_string());
        topic.set_partitions(vec![partition]);
        let mut data = ListOffsetsResponseData::new();
        data.set_topics(vec![topic]);
        let mut mock = MockClient::with_static_nodes(Vec::<Node>::new(), Arc::new(mock_time(0)));
        mock.prepare_response(ConcreteResponse::ListOffsets(ListOffsetsResponse::new(data)));
        let client = CountingClient::new(mock);
        // The network poll blocks until woken, emulating an idle socket wait.
        client.poll_block().store(true, Ordering::SeqCst);
        let (bg_tx, _bg_rx) = mpsc::unbounded_channel();
        let counting_delegate = NetworkClientDelegate::new(
            &config,
            client,
            metadata.metadata_arc(),
            Arc::new(BackgroundEventHandler::new(bg_tx)),
            false,
        );
        // The one wakeup primitive, taken from the delegate and installed in
        // the offsets manager exactly as the production constructor does.
        let event_notify = counting_delegate.wakeup_handle();
        let offsets = make_offsets_manager(&config, subs.clone(), metadata.clone());
        offsets.set_completion_notify(Arc::clone(&event_notify));
        let request_managers = Arc::new(Mutex::new(RequestManagers::new(
            None,
            None,
            None,
            None,
            None,
            Some(offsets),
            None,
        )));
        let delegate = Arc::new(AsyncMutex::new(counting_delegate));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let time: Arc<MockTime> = Arc::new(mock_time(1_000));
        let processor = ApplicationEventProcessor::new(
            request_managers.clone(),
            metadata.clone(),
            subs.clone(),
            reaper.clone(),
            time.clone(),
        );
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let mut thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn Time>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            WakeupTrigger::new(),
            Arc::new(AtomicI64::new(NetThread::MAX_POLL_TIMEOUT_MS)),
        );

        // The application calls `end_offsets([t1-0])`: its
        // `ApplicationEventHandler::add` enqueues the ListOffsets event and
        // pokes the wakeup handle.
        let now_ms = time.milliseconds();
        let (handle, mut result_rx) = CompletableEventHandle::new(now_ms + 60_000);
        let event = ApplicationEvent::ListOffsets {
            handle,
            timestamps_to_search: HashMap::from([(tp.clone(), ListOffsetsRequest::LATEST_TIMESTAMP)]),
            require_timestamps: false,
        };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: now_ms })
            .expect("send ok");
        event_notify.notify_one();

        // Drive the bg loop until the lookup completes or the deadline passes,
        // then stop it between iterations — releasing the blocked poll with
        // the wakeup handle, as `close()` does — rather than cancelling
        // `run_once` mid-poll.
        let stop = AtomicBool::new(false);
        let mut outcome = None;
        let driver = async {
            while !stop.load(Ordering::SeqCst) {
                thread.run_once().await;
            }
        };
        let watcher = async {
            outcome = tokio::time::timeout(Duration::from_secs(1), &mut result_rx).await.ok();
            stop.store(true, Ordering::SeqCst);
            event_notify.notify_one();
        };
        tokio::join!(driver, watcher);

        let offsets = outcome
            .expect(
                "Java completes the ListOffsets lookup when the response arrives; the response must wake \
                 the bg loop instead of waiting out the network poll (up to MAX_POLL_TIMEOUT_MS)",
            )
            .expect("event handle dropped")
            .expect("the lookup must succeed");
        let end_offset = offsets.get(&tp).expect("entry for t1-0").as_ref().expect("an offset");
        assert_eq!(7, end_offset.offset());
    }
}
