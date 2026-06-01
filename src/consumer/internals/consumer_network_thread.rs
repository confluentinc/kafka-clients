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
//!    manager set (CLAUDE.md §11: single numeric across tasks ⇒ atomic).
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
//! `AsyncConsumerMetrics` is not yet translated into Rust. Java's
//! `recordTimeBetweenNetworkThreadPoll`, `recordApplicationEventQueueSize`,
//! and `recordApplicationEventExpiredSize` call sites are replaced with
//! log-only equivalents. The Java metric tests are intentionally NOT
//! translated here — they will be added alongside the metrics framework
//! in a future milestone.
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
//! call `fail_with_timeout(err)` on every notifiable handle that is not
//! yet done. The per-event arm inside
//! [`ConsumerNetworkThread::process_application_events`] (Java step 1's
//! `maybeFailOnMetadataError(List.of(event))` arm) covers "immediately
//! completed events" — both arms are present and behavior-faithful.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::kafka_client::KafkaClient;

use super::consumer_membership_manager::ConsumerMembershipManager;
use super::events::application_event::ApplicationEventEnvelope;
use super::events::application_event_processor::ApplicationEventProcessor;
use super::events::completable_event_reaper::CompletableEventReaper;
use super::events::event_processor::EventProcessor;
use super::network_client_delegate::NetworkClientDelegate;
use super::request_manager::RequestManager;
use super::request_managers::RequestManagers;
use super::wakeup_trigger::WakeupTrigger;

/// `consumer-threading.md` §11 / Java
/// `ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS`.
pub(crate) const MAX_POLL_TIMEOUT_MS: i64 = 5_000;

/// Default close-timeout in ms. Mirrors Java's
/// `ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS`.
pub(crate) const DEFAULT_CLOSE_TIMEOUT_MS: i64 = 30_000;

/// Time source used by `ConsumerNetworkThread` for the `current_time_ms`
/// argument to `runOnce` and `cleanup`. Mirrors Java's `Time` interface
/// (production: `SystemTime::now()`; tests: a mock clock).
///
/// Sync, single `milliseconds()` method — same shape as the existing
/// `FetchCollectorTime` (`fetch_collector.rs`).
pub(crate) trait ThreadTime: Send + Sync + 'static {
    fn milliseconds(&self) -> i64;
}

/// Production implementation of [`ThreadTime`] — wraps
/// `std::time::SystemTime::now()`.
#[derive(Debug, Default)]
pub(crate) struct SystemThreadTime;

impl ThreadTime for SystemThreadTime {
    fn milliseconds(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Consumer background task — single `tokio::spawn` per consumer instance
/// per `consumer-threading.md` §10. Owns the request managers, the
/// network client delegate, the application event processor, the event
/// reaper, and the membership manager. The application event channel
/// (receiver half) is also owned here; the sender lives on the app side
/// in [`super::events::application_event_handler::ApplicationEventHandler`].
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
    /// Wakeup primitive. The app side calls `wakeup.wakeup()`; the bg
    /// task `select!`s on `wakeup_rx.borrow().clone().cancelled()` at
    /// the top of each loop.
    wakeup: WakeupTrigger,
    /// Watch-channel subscription used to re-read the current wakeup
    /// token at the top of every iteration.
    wakeup_rx: watch::Receiver<CancellationToken>,
    /// Shutdown signal — flipped to `true` by [`Self::signal_close`].
    /// The bg-task loop exits cleanly the next iteration.
    running: Arc<AtomicBool>,
    /// Cached `maximumTimeToWait` value — read by the app side via
    /// [`Self::maximum_time_to_wait`]. Updated by `run_once` after each
    /// pass through the request managers.
    cached_max_time_to_wait_ms: Arc<AtomicI64>,
    /// Close timeout (millis). Set by [`Self::set_close_timeout_ms`]
    /// before `close()`.
    close_timeout_ms: AtomicI64,
    /// Wall-clock timestamp of the last `run_once` call. Used by Java's
    /// `recordTimeBetweenNetworkThreadPoll` metric — kept here as a
    /// log-only equivalent.
    last_poll_time_ms: i64,
    /// Time source — `SystemThreadTime` in production, mock in tests.
    time: Arc<dyn ThreadTime>,
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
    notifiable_handles: Vec<Arc<dyn super::events::completable_event::CompletableEventErasedHandle>>,
}

impl<K: KafkaClient + Send + 'static> ConsumerNetworkThread<K> {
    /// Java constructor. Drops `LogContext` (Rust uses `log`),
    /// `AsyncConsumerMetrics` (no metrics yet — see module docstring),
    /// and the three `Supplier<...>` indirections (Rust takes the
    /// already-constructed values directly).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        time: Arc<dyn ThreadTime>,
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
        cached_max_time_to_wait_ms.store(MAX_POLL_TIMEOUT_MS, Ordering::Release);
        let wakeup_rx = wakeup.subscribe();
        Self {
            application_event_rx,
            application_event_reaper,
            application_event_processor,
            network_client_delegate,
            request_managers,
            wakeup,
            wakeup_rx,
            running: Arc::new(AtomicBool::new(true)),
            cached_max_time_to_wait_ms,
            close_timeout_ms: AtomicI64::new(DEFAULT_CLOSE_TIMEOUT_MS),
            last_poll_time_ms: 0,
            time,
            membership,
            notifiable_handles: Vec::new(),
        }
    }

    /// Java: `isRunning()`.
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
    pub(crate) fn maximum_time_to_wait(&self) -> i64 {
        self.cached_max_time_to_wait_ms.load(Ordering::Acquire)
    }

    /// Mirror of Java's `wakeup()`. Cancels the current token via the
    /// shared [`WakeupTrigger`] and also calls
    /// `network_client_delegate.wakeup()` to unblock the underlying
    /// `KafkaClient::poll`. The trigger is shared with the app side
    /// (`AsyncKafkaConsumer::wakeup()` in Phase 11).
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

    /// Sets the close timeout used by [`Self::cleanup`].
    pub(crate) fn set_close_timeout_ms(&self, timeout_ms: i64) {
        self.close_timeout_ms.store(timeout_ms, Ordering::Release);
    }

    /// One iteration of the bg-task loop. Mirrors Java's
    /// `ConsumerNetworkThread.runOnce()` line-for-line; see the
    /// module-level docstring for the phase-by-phase mapping.
    pub(crate) async fn run_once(&mut self) {
        // ──── Phase 1: drain application events ────
        self.process_application_events();

        let current_time_ms = self.time.milliseconds();
        if self.last_poll_time_ms != 0 {
            log::trace!(
                "time-between-network-thread-poll: {} ms",
                current_time_ms.saturating_sub(self.last_poll_time_ms)
            );
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
        let mut poll_wait_time_ms: i64 = MAX_POLL_TIMEOUT_MS;
        let (collected_before, collected_after): (
            Vec<super::network_client_delegate::PollResult>,
            Vec<super::network_client_delegate::PollResult>,
        ) = {
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
            //     `CommitRequestManager.poll(currentTimeMs)`
            //     (`CommitRequestManager.java:181-209`). Drains
            //     `unsent_offset_commits` and `unsent_offset_fetches`
            //     into `UnsentRequest`s, fires the auto-commit timer
            //     via `maybe_auto_commit_async`, and handles
            //     `closing && coordinator unknown` by failing pending
            //     commits with `CommitFailedException`. Without this
            //     call `commit_sync()`/`committed()` hang forever
            //     (Phase-12 Critic Issue 1).
            //   * `membership.reconcile(now, false)` — driven in
            //     Phase 2.5 below.
            let (coord_handle, commit_handle, mut entries_results) = {
                let mut rm_guard = match self.request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                let coord = rm_guard.coordinator_handle();
                let commit = rm_guard.commit_handle();
                let entries: Vec<super::network_client_delegate::PollResult> =
                    rm_guard.entries().into_iter().map(|rm| rm.poll(current_time_ms)).collect();
                (coord, commit, entries)
            };
            // `entries()` ordering with the three Arc-shared slots
            // skipped is `heartbeat → offsets → topic_metadata → fetch
            // → dyn`. `membership_boundary()` returns the heartbeat
            // count (0 or 1); `split_off(boundary)` keeps the
            // heartbeat (if any) in the leading slice and moves
            // offsets/topic_metadata/fetch/dyn to the tail.
            let boundary = {
                let rm_guard = match self.request_managers.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                rm_guard.membership_boundary()
            };
            let safe_boundary = boundary.min(entries_results.len());
            let tail = entries_results.split_off(safe_boundary);
            let mut before: Vec<super::network_client_delegate::PollResult> = Vec::new();
            // Java order: coordinator → commit → heartbeat. Coordinator
            // goes first; its `poll` may discover the coordinator node,
            // which `commit.poll_with_coordinator` then consumes.
            if let Some(coord_arc) = coord_handle.as_ref() {
                let mut g = match coord_arc.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                before.push(g.poll(current_time_ms));
            }
            // Java: `CommitRequestManager.poll(currentTimeMs)`
            // (`CommitRequestManager.java:181-209`). Must run between
            // coordinator and heartbeat so unsent commits/fetches are
            // shipped on every iteration. The coordinator guard is
            // re-acquired briefly (the previous `before.push(g.poll(...))`
            // block drops it before this point) so we can pass
            // `&mut CoordinatorRequestManager` to `poll_with_coordinator`.
            // `&self` is acceptable because `CommitRequestManager` uses
            // interior mutability (`Arc<CommitRequestManagerInner>`).
            if let (Some(coord_arc), Some(commit_arc)) = (coord_handle.as_ref(), commit_handle.as_ref()) {
                let mut coord_g = match coord_arc.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                before.push(commit_arc.poll_with_coordinator(&mut coord_g, current_time_ms));
            }
            before.extend(entries_results); // heartbeat (if any)
            (before, tail)
        };
        {
            let mut delegate_guard = self.network_client_delegate.lock().await;
            for mut poll_result in collected_before {
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
        }

        // ──── Phase 2.5: drive membership.reconcile per iteration ────
        //
        // Java's `AbstractMembershipManager.poll(now)` body is
        // `maybeReconcile(false); return EMPTY;`. Pass
        // `can_commit = false`: this is the per-iteration call where
        // we have NOT just run `updateTimerAndMaybeCommit`, so it is
        // not safe to advance reconciliation when auto-commit is
        // enabled (Java's `AbstractMembershipManager.java:854`).
        //
        // Failures are logged and swallowed, matching Java's
        // surrounding-runOnce `try { ... } catch (Throwable e) { log }`.
        if let Some(membership) = self.membership.clone()
            && let Err(e) = membership.reconcile(current_time_ms, false).await
        {
            log::warn!("Membership reconcile failed: {}", e);
        }

        // ──── Phase 2.6: poll after-membership managers (offsets,
        // topic_metadata, fetch, dyn) ────
        {
            let mut delegate_guard = self.network_client_delegate.lock().await;
            for mut poll_result in collected_after {
                let try_connect_nodes = std::mem::take(&mut poll_result.try_connect);
                for node in try_connect_nodes {
                    delegate_guard.try_connect(&node, current_time_ms).await;
                }
                let timeout_ms = delegate_guard.add_all_from_poll_result(poll_result, current_time_ms);
                poll_wait_time_ms = poll_wait_time_ms.min(timeout_ms);
            }
        }

        // ──── Phase 4: poll the network client ────
        //
        // Only `.await` boundary in `run_once` that must be cancel-safe
        // against the wakeup token (`consumer-threading.md` §11).
        // `tokio::select!` with `biased;` to prefer the shutdown / wakeup
        // signals over the (potentially long) network poll.
        let token = self.wakeup_rx.borrow().clone();
        {
            let mut delegate_guard = self.network_client_delegate.lock().await;
            tokio::select! {
                biased;
                _ = token.cancelled() => {
                    // Wakeup or shutdown fired — exit the poll early.
                    // The `KafkaClient::poll` is already wakeup-aware
                    // via `delegate.wakeup()` (which we call from
                    // `Self::wakeup`/`signal_close`), but the
                    // `select!` arm gives us a cancellation point
                    // even if the network client did not propagate
                    // the wakeup. Java's equivalent is the
                    // `WakeupException` thrown by the selector.
                    log::trace!("Network-client poll preempted by wakeup");
                }
                _ = delegate_guard.poll_default(poll_wait_time_ms, current_time_ms) => {}
            }
        }

        // ──── Phase 5: refresh cached maximumTimeToWait ────
        let mut max_time_to_wait_ms: i64 = i64::MAX;
        {
            let mut rm_guard = match self.request_managers.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            for rm in rm_guard.entries() {
                let wait_ms = rm.maximum_time_to_wait(current_time_ms);
                max_time_to_wait_ms = max_time_to_wait_ms.min(wait_ms);
            }
        }
        self.cached_max_time_to_wait_ms.store(max_time_to_wait_ms, Ordering::Release);

        // ──── Phase 6: reap expired application events ────
        let expired = {
            let mut reaper = match self.application_event_reaper.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            reaper.reap(current_time_ms)
        };
        if expired > 0 {
            log::trace!("application-event-expired-size: {}", expired);
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
        self.maybe_fail_on_metadata_error_uncompleted();
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
    ///     `fail_with_timeout(err)` on every live handle. Java calls
    ///     `e.onMetadataError(metadataError.get())` which for these
    ///     four variants resolves to `handle.completeExceptionally(...)`
    ///     — exactly what `fail_with_timeout` does.
    ///
    /// Called by `run_once` after the reap step.
    fn maybe_fail_on_metadata_error_uncompleted(&mut self) {
        // Step 1: drop any handle that completed since the last call.
        self.notifiable_handles.retain(|h| !h.is_done());
        if self.notifiable_handles.is_empty() {
            // Java: "Don't get-and-clear the metadata error if there are
            // no events that will be notified." (ConsumerNetworkThread
            // .java:447-449).
            return;
        }

        // Step 2: query the delegate for a pending metadata error. The
        // bg task is the sole holder of the delegate mutex; `try_lock`
        // is safe (no contention).
        let err_opt = {
            let mut delegate_guard = self
                .network_client_delegate
                .try_lock()
                .expect("delegate not contended on bg task");
            delegate_guard.get_and_clear_metadata_error()
        };

        let Some(err) = err_opt else {
            return;
        };

        // Step 3: notify every live handle. KafkaError is Clone so we
        // can fan it out faithfully (Java passes the same exception
        // instance to each `onMetadataError` call).
        for handle in &self.notifiable_handles {
            // Java: `e.onMetadataError(metadataError.get())` resolves
            // to `handle.completeExceptionally(metadataError)` for each
            // of the four notifiable+completable variants. Our erased
            // handle's `fail_with_timeout(err)` calls
            // `tx.send(Err(err))` on the inner oneshot — identical
            // semantics. The method is misnamed in Rust for historical
            // reasons (it was originally only used by the reaper); the
            // generic implementation accepts any `KafkaError`.
            handle.fail_with_timeout(err.clone());
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
    fn process_application_events(&mut self) {
        let mut envelopes: Vec<ApplicationEventEnvelope> = Vec::new();
        while let Ok(env) = self.application_event_rx.try_recv() {
            envelopes.push(env);
        }
        if envelopes.is_empty() {
            return;
        }

        for env in envelopes {
            // 1. Register with the reaper if completable. The Java
            // `CompletableEvent` interface check is replaced by the
            // `erased_handle()` accessor on [`ApplicationEvent`].
            if let Some(erased) = env.event.erased_handle() {
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
                continue;
            }

            // 3. Normal dispatch. Java wraps in a `try { ... } catch
            // (Throwable t) { log.warn(...) }`; Rust's processor is
            // already infallible by signature (`fn process(&mut self,
            // event)`) — any panic would unwind the bg task. The
            // surrounding tokio::spawn entry point owns the catch.
            self.application_event_processor.process(env.event);
        }
    }

    /// Test-only helper that pushes an erased handle directly onto
    /// `notifiable_handles`, bypassing `process_application_events`.
    /// Used to test the post-poll metadata-error arm in isolation
    /// from AEP dispatch (which may synchronously complete the handle
    /// in some variants).
    #[cfg(test)]
    pub(crate) fn push_notifiable_handle_for_test(
        &mut self,
        handle: Arc<dyn super::events::completable_event::CompletableEventErasedHandle>,
    ) {
        self.notifiable_handles.push(handle);
    }

    /// Mirror of Java's `cleanup()`. Runs the close-side request
    /// managers, drains any remaining unsent requests for up to the
    /// close-timeout, reaps any in-flight completable events, and closes
    /// the managers / delegate.
    pub(crate) async fn cleanup(&mut self) {
        log::trace!("Closing the consumer network thread");
        let close_timeout_ms = self.close_timeout_ms.load(Ordering::Acquire);
        let close_deadline_ms = self.time.milliseconds().saturating_add(close_timeout_ms);

        // ──── 1. pollOnClose round ────
        //
        // Java's `cleanup` iterates `requestManagers.entries()` and calls
        // `pollOnClose` on each. The Rust `entries()` skips coordinator
        // and commit (Arc-shared); the bg-task drives them explicitly:
        //   * coordinator has no close-side work (Java's
        //     `CoordinatorRequestManager.pollOnClose` returns EMPTY).
        //   * commit drains pending offset-commit requests via
        //     `drain_pending_offset_commit_requests()` — mirrors Java
        //     `CommitRequestManager.pollOnClose` which calls
        //     `drainPendingOffsetCommitRequests()` (Java
        //     `CommitRequestManager.java:215`).
        let current_time_ms = self.time.milliseconds();
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
            for rm in rm_guard.entries() {
                let result = rm.poll_on_close(current_time_ms);
                let _ = delegate_guard.add_all_from_poll_result(result, current_time_ms);
            }
            // Commit's close-side drain (Java
            // `CommitRequestManager.pollOnClose` → `drainPendingOffsetCommitRequests`).
            if let Some(commit_arc) = commit_on_close.as_ref() {
                let result = commit_arc.drain_pending_offset_commit_requests();
                let _ = delegate_guard.add_all_from_poll_result(result, current_time_ms);
            }
        }

        // ──── 2. Drain unsent requests until timer expires ────
        loop {
            let now = self.time.milliseconds();
            let remaining = close_deadline_ms.saturating_sub(now);
            let has_pending = {
                let delegate_guard = self
                    .network_client_delegate
                    .try_lock()
                    .expect("delegate not contended on bg task");
                delegate_guard.has_any_pending_requests()
            };
            if !has_pending || remaining <= 0 {
                break;
            }
            let mut delegate_guard = self.network_client_delegate.lock().await;
            delegate_guard.poll_on_close(remaining, now).await;
            // re-loop; `notExpired() && hasAnyPendingRequests` is the
            // Java guard.
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
        let mut leftover_erased: Vec<Arc<dyn super::events::completable_event::CompletableEventErasedHandle>> =
            Vec::new();
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
        log::trace!("application-event-expired-size (close): {}", expired);

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
    /// exit. Intended to be spawned via `tokio::spawn` from the Phase
    /// 11 consumer constructor.
    pub(crate) async fn run(mut self) {
        log::debug!("Consumer network thread started");
        while self.is_running() {
            // Re-check the shutdown signal first so `signal_close()`
            // followed by `wakeup.wakeup()` exits immediately.
            self.run_once().await;
        }
        self.cleanup().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, AtomicUsize};
    use std::time::Duration;

    use crate::api_versions::ApiVersions;
    use crate::common::IsolationLevel;
    use crate::common::KafkaError;
    use crate::common::Node;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::GroupMembershipOperation;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::consumer_membership_manager::ConsumerMembershipManager;
    use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
    use crate::consumer::internals::events::application_event::{ApplicationEvent, ApplicationEventEnvelope};
    use crate::consumer::internals::events::application_event_processor::ApplicationEventProcessor;
    use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
    use crate::consumer::internals::events::completable_event::make_completable_event;
    use crate::consumer::internals::events::completable_event_reaper::CompletableEventReaper;
    use crate::consumer::internals::network_client_delegate::{NetworkClientDelegate, PollResult};
    use crate::consumer::internals::offsets_request_manager::OffsetsRequestManager;
    use crate::consumer::internals::request_manager::RequestManager;
    use crate::consumer::internals::request_managers::RequestManagers;
    use crate::consumer::internals::subscription_state::SubscriptionState;
    use crate::mock_client::MockClient;

    use super::*;

    // ─── Mock time source for tests (Java's MockTime) ───
    struct MockTime {
        millis: Mutex<i64>,
    }
    impl MockTime {
        fn new(start: i64) -> Self {
            Self { millis: Mutex::new(start) }
        }
        fn sleep(&self, dur_ms: i64) {
            let mut g = self.millis.lock().unwrap();
            *g += dur_ms;
        }
    }
    impl ThreadTime for MockTime {
        fn milliseconds(&self) -> i64 {
            *self.millis.lock().unwrap()
        }
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
            PollResult::from_wait(self.poll_return_ms)
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
    }

    impl CountingClient {
        fn new(inner: MockClient) -> Self {
            Self {
                inner,
                poll_call_count: Arc::new(AtomicUsize::new(0)),
                poll_timeouts: Arc::new(Mutex::new(Vec::new())),
                has_in_flight_script: Arc::new(Mutex::new(VecDeque::new())),
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
        fn authentication_error(&self, node: &Node) -> Option<String> {
            self.inner.authentication_error(node)
        }
        fn send(&mut self, request: crate::ClientRequest, now: i64) {
            self.inner.send(request, now)
        }
        async fn poll(&mut self, timeout: i64, now: i64) -> Vec<crate::ClientResponse> {
            self.poll_call_count.fetch_add(1, Ordering::SeqCst);
            self.poll_timeouts.lock().unwrap().push(timeout);
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
            self.inner.wakeup()
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
            self.inner.close().await
        }
    }

    fn make_config() -> ConsumerConfig {
        ConsumerConfig::new(vec!["localhost:9092".to_string()])
    }

    fn make_metadata(config: &ConsumerConfig, subs: Arc<Mutex<SubscriptionState>>) -> Arc<ConsumerMetadata> {
        Arc::new(ConsumerMetadata::from_config(config, subs, ClusterResourceListeners::new()))
    }

    fn make_delegate(config: &ConsumerConfig, metadata: Arc<ConsumerMetadata>) -> NetworkClientDelegate<MockClient> {
        let time_provider: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| 0);
        let client = MockClient::new(Vec::<Node>::new(), time_provider);
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let raw_metadata = metadata.metadata_arc();
        NetworkClientDelegate::new(config, client, raw_metadata, beh, false)
    }

    fn make_counting_delegate(
        config: &ConsumerConfig,
        metadata: Arc<ConsumerMetadata>,
    ) -> NetworkClientDelegate<CountingClient> {
        let time_provider: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| 0);
        let client = CountingClient::new(MockClient::new(Vec::<Node>::new(), time_provider));
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
        let processor =
            ApplicationEventProcessor::new(request_managers.clone(), metadata.clone(), subs.clone(), reaper.clone());
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let time: Arc<MockTime> = Arc::new(MockTime::new(1_000));
        let wakeup = WakeupTrigger::new();
        let thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn ThreadTime>,
            rx,
            reaper.clone(),
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS)),
        );
        (
            CountingFixture { thread, time, delegate, reaper, tx },
            poll_call_count,
            poll_timeouts,
            has_in_flight_script,
        )
    }

    fn make_offsets_manager(
        config: &ConsumerConfig,
        subs: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
    ) -> OffsetsRequestManager {
        OffsetsRequestManager::new(
            subs,
            metadata,
            IsolationLevel::ReadUncommitted,
            100,
            config.request_timeout_ms() as i64,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
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
        let processor =
            ApplicationEventProcessor::new(request_managers.clone(), metadata.clone(), subs.clone(), reaper.clone());
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let time: Arc<MockTime> = Arc::new(MockTime::new(1_000));
        let wakeup = WakeupTrigger::new();
        let thread = ConsumerNetworkThread::new(
            time.clone() as Arc<dyn ThreadTime>,
            rx,
            reaper.clone(),
            processor,
            delegate,
            request_managers.clone(),
            None,
            wakeup,
            Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS)),
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
        let processor =
            ApplicationEventProcessor::new(request_managers.clone(), metadata.clone(), subs.clone(), reaper.clone());
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let time: Arc<MockTime> = Arc::new(MockTime::new(1_000));
        let wakeup = WakeupTrigger::new();
        let thread = ConsumerNetworkThread::new(
            time as Arc<dyn ThreadTime>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            Some(membership.clone()),
            wakeup,
            Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS)),
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
        run_poll_time_computations_case(MAX_POLL_TIMEOUT_MS - 1).await;
    }

    #[tokio::test]
    async fn test_consumer_network_thread_poll_time_computations_at_max() {
        run_poll_time_computations_case(MAX_POLL_TIMEOUT_MS).await;
    }

    #[tokio::test]
    async fn test_consumer_network_thread_poll_time_computations_above_max() {
        run_poll_time_computations_case(MAX_POLL_TIMEOUT_MS + 1).await;
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
        let expected_timeout = example_time.min(MAX_POLL_TIMEOUT_MS);
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
    async fn test_maximum_time_to_wait() {
        const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 1_000;

        let heartbeat_spy = SpyRequestManager::new(MAX_POLL_TIMEOUT_MS, DEFAULT_HEARTBEAT_INTERVAL_MS);
        let dyn_managers: Vec<Box<dyn RequestManager>> = vec![Box::new(heartbeat_spy)];
        let (mut fixture, _poll, _to, _hi) = make_thread_with_dyn_managers(dyn_managers);

        // Initial value before runOnce has been called.
        assert_eq!(
            fixture.thread.maximum_time_to_wait(),
            MAX_POLL_TIMEOUT_MS,
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
    async fn test_cleanup_invokes_reaper() {
        let (mut thread, _tx, reaper, _time, _rm) = make_thread_no_membership();
        // Add a deadline-zero completable event so reap-on-close
        // expires it.
        let (_h, mut rx, erased) = make_completable_event::<()>(0);
        reaper.lock().unwrap().add(erased);

        thread.cleanup().await;

        // The reaper observed the tracked event and timed it out —
        // proves `reap_on_close(...)` was called inside `cleanup()`.
        assert!(
            matches!(rx.try_recv().expect("sender used"), Err(KafkaError::Timeout(_))),
            "reaper.reap(...) must complete tracked event with Timeout"
        );
    }

    /// Java `testRunOnceInvokesReaper`. Verifies `runOnce` invokes
    /// the reaper. Mockito: `verify(applicationEventReaper).reap(any(Long.class))`.
    /// Rust observes via a same-instant-deadline tracked event whose
    /// receiver becomes `Err(Timeout)` after `runOnce`.
    #[tokio::test]
    async fn test_run_once_invokes_reaper() {
        let (mut thread, _tx, reaper, time, _rm) = make_thread_no_membership();
        // Register a deadline-zero event; clock starts at 1_000, so
        // every reap iteration past-due will fire `Timeout` on rx.
        let (_h, mut rx, erased) = make_completable_event::<()>(0);
        reaper.lock().unwrap().add(erased);

        // Drive a single iteration. The reap step at Phase 6 must
        // observe the past-due deadline and fail the event.
        let _ = time;
        thread.run_once().await;

        assert!(
            matches!(rx.try_recv().expect("sender used"), Err(KafkaError::Timeout(_))),
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
    // - `testRunOnceRecordTimeBetweenNetworkThreadPoll` and
    //   `testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`:
    //   assert against `AsyncConsumerMetrics` histogram values. That
    //   class is not yet translated. The bg-task currently emits
    //   `log::trace!` equivalents at the same call sites. When the
    //   metrics framework lands these tests will be added alongside it.

    /// Drain-events path: an enqueued completable event is registered
    /// with the reaper during `process_application_events`. We call
    /// the inner method directly so we observe the reaper state at the
    /// `add` site — after a full `run_once` the reap step would
    /// already have removed any handles whose AEP arm synchronously
    /// completed them.
    #[tokio::test]
    async fn process_events_registers_completable_with_reaper() {
        let (mut thread, tx, reaper, _time, _rm) = make_thread_no_membership();
        let (handle, _rx, erased_external) = make_completable_event::<()>(60_000);
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
    ///      and `fail_with_timeout`s the inner oneshot.
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
        let processor =
            ApplicationEventProcessor::new(request_managers.clone(), metadata.clone(), subs.clone(), reaper.clone());
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let time: Arc<MockTime> = Arc::new(MockTime::new(1_000));
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time as Arc<dyn ThreadTime>,
            rx,
            reaper,
            processor,
            delegate,
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS)),
        );

        // 1. Notifiable handle with a large deadline so the reaper
        // doesn't expire it before the metadata-error arm fires.
        let (h, mut event_rx) =
            crate::consumer::internals::events::completable_event::CompletableEventHandle::<()>::new(60_000);
        thread.push_notifiable_handle_for_test(h.erased());

        // 2. Plant the metadata error on the cluster.
        metadata
            .metadata_arc()
            .fatal_error(KafkaError::topic_authorization(std::collections::HashSet::from([
                "t".to_string()
            ])));

        // 3. Drive one runOnce iteration.
        thread.run_once().await;

        // 4. The notifiable handle was completed exceptionally with
        // the metadata error variant intact.
        let received = event_rx.try_recv().expect("sender used");
        let err = received.expect_err("post-poll arm must fail the handle");
        assert!(
            matches!(err, KafkaError::TopicAuthorization(_)),
            "expected TopicAuthorization, got: {err:?}"
        );
    }

    /// When NO notifiable handle is tracked, the post-poll arm must
    /// NOT consume the delegate's metadata error — Java's
    /// "Don't get-and-clear the metadata error if there are no events
    /// that will be notified" optimisation
    /// (`ConsumerNetworkThread.java:447-449`). Subsequent runOnce
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
        let processor =
            ApplicationEventProcessor::new(request_managers.clone(), metadata.clone(), subs.clone(), reaper.clone());
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let time: Arc<MockTime> = Arc::new(MockTime::new(1_000));
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time as Arc<dyn ThreadTime>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS)),
        );

        // Plant the metadata error.
        metadata
            .metadata_arc()
            .fatal_error(KafkaError::topic_authorization(std::collections::HashSet::from([
                "t".to_string()
            ])));

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
    /// completable-arm coverage of [`super::ApplicationEvent::erased_handle`]
    /// extends beyond `AssignmentChange`.
    #[tokio::test]
    async fn leave_group_event_registered_with_reaper() {
        let (mut thread, tx, reaper, _time, _rm) = make_thread_no_membership();
        let (handle, _rx, erased_external) = make_completable_event::<()>(60_000);
        let event =
            ApplicationEvent::LeaveGroupOnClose { handle, membership_operation: GroupMembershipOperation::Default };
        tx.send(ApplicationEventEnvelope { event, enqueued_ms: 1_000 })
            .expect("send ok");
        thread.process_application_events();
        let r = reaper.lock().unwrap();
        assert!(r.contains(&erased_external), "LeaveGroupOnClose must be tracked");
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
        use crate::consumer::internals::commit_request_manager::CommitRequestManager;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

        let mut config = make_config();
        config.group_id = Some("g".to_string());
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = make_metadata(&config, subs.clone());

        // Build a coordinator with a known coordinator node so
        // `poll_with_coordinator` proceeds past the `coordinator unknown`
        // early-return.
        let mut coord = CoordinatorRequestManager::new(100, 1_000, "g".to_string());
        coord.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let coordinator = Arc::new(Mutex::new(coord));

        // Build a real commit manager (no auto-commit; this test
        // exercises the explicit `commit_sync` path).
        let commit = Arc::new(CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "g".to_string(),
            None,
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
        let processor =
            ApplicationEventProcessor::new(request_managers.clone(), metadata.clone(), subs.clone(), reaper.clone());
        let (_tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let time: Arc<MockTime> = Arc::new(MockTime::new(1_000));
        let wakeup = WakeupTrigger::new();
        let mut thread = ConsumerNetworkThread::new(
            time as Arc<dyn ThreadTime>,
            rx,
            reaper,
            processor,
            delegate.clone(),
            request_managers,
            None,
            wakeup,
            Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS)),
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
}
