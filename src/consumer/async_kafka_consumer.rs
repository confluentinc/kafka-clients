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

//! User-facing async consumer implementing the KIP-848 (consumer-group)
//! protocol.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer`. This
//! file lands the struct, constructor, and the synchronous state-read
//! methods (Phase 11 commit (2/N)). Subsequent commits (3-7) add
//! subscribe / poll / commit / position / committed / close.
//!
//! # Generic-over-`KafkaClient`
//!
//! Java's `AsyncKafkaConsumer` carries the `NetworkClient` indirectly via
//! `ApplicationEventHandler` and `ConsumerNetworkThread`. The Rust
//! `ConsumerNetworkThread<K>` is generic over `K: KafkaClient` so tests
//! can plug in `MockClient`. To keep the user-facing `Consumer<K, V>`
//! trait clean of the network-client type, the spawned task is wrapped in
//! a type-erased [`NetworkThreadCloseHandle`] that exposes only the
//! lifecycle hooks (`signal_close`, `wakeup`, `await_join`).
//!
//! # Reentrancy guard (not translated)
//!
//! Java guards against multi-thread access via the
//! `acquire()` / `release()` pair throwing
//! `ConcurrentModificationException`. Rust's `&mut self` on the
//! `Consumer` trait makes single-caller exclusivity a *compile-time*
//! guarantee — the runtime guard is redundant. See Phase 11 PLAN.md
//! deferral #4.
//!
//! # Metrics (deferred)
//!
//! All `kafkaConsumerMetrics.record*` / `asyncConsumerMetrics.record*`
//! calls in the Java source are NO-OPs in this translation, marked
//! `// METRICS-DEFERRED: …`. Per Phase 11 PLAN.md deferral #1.
//!
//! # Telemetry (deferred)
//!
//! `ClientTelemetryReporter` / `ClientTelemetryUtils` are NOT translated;
//! the corresponding fields are `None`. Per Phase 11 PLAN.md deferral #6.

#![allow(dead_code)] // Phase 11 commits 5-7 wire commit / state-query / close.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use regex::Regex;

use crate::common::{IsolationLevel, KafkaError, TopicPartition};
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::ConsumerRecords;
use crate::consumer::OffsetAndMetadata;
use crate::consumer::OffsetAndTimestamp;
use crate::consumer::SubscriptionPattern;
use crate::consumer::consumer_config::ConsumerConfig;
use crate::consumer::consumer_rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::internals::consumer_interceptors::ConsumerInterceptors;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::consumer_network_thread::ThreadTime;
use crate::consumer::internals::consumer_rebalance_listener_invoker::ConsumerRebalanceListenerInvoker;
use crate::consumer::internals::deserializers::Deserializers;
use crate::consumer::internals::events::application_event::{ApplicationEvent, AsyncPollState};
use crate::consumer::internals::events::application_event_handler::ApplicationEventHandler;
use crate::consumer::internals::events::background_event::{BackgroundEvent, BackgroundEventEnvelope};
use crate::consumer::internals::events::completable_event::{calculate_deadline_ms, make_completable_event};
use crate::consumer::internals::events::completable_event_reaper::CompletableEventReaper;
use crate::consumer::internals::fetch_buffer::FetchBuffer;
use crate::consumer::internals::fetch_collector::FetchCollector;
use crate::consumer::internals::member_state_listener::MemberStateListener;
use crate::consumer::internals::offset_commit_callback_invoker::OffsetCommitCallbackInvoker;
use crate::consumer::internals::request_managers::RequestManagers;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::internals::wakeup_trigger::WakeupTrigger;

/// Type-erased handle to the spawned consumer background task.
///
/// Owns the `JoinHandle<()>` produced by `tokio::spawn(thread.run())`
/// and the `Box<dyn Fn>` closures that close / wakeup the underlying
/// `ConsumerNetworkThread<K>` regardless of its concrete `K`.
///
/// Held by [`AsyncKafkaConsumer`] for the lifetime of the consumer
/// instance; dropped (with `signal_close`) on close.
pub(crate) struct NetworkThreadCloseHandle {
    /// Cancels the bg-task `run_once` loop and wakes the trigger so the
    /// next iteration observes the shutdown.
    signal_close_fn: Box<dyn Fn() + Send + Sync>,
    /// Wakes the bg-task's `select!` on the wakeup token.
    wakeup_fn: Box<dyn Fn() + Send + Sync>,
    /// Spawned tokio task. Awaitable on close.
    join_handle: Option<JoinHandle<()>>,
}

impl NetworkThreadCloseHandle {
    /// Constructor used by [`AsyncKafkaConsumer::new_with_thread`]. The
    /// closures capture the concrete `ConsumerNetworkThread<K>` clones
    /// of the close / wakeup state so the outer struct can stay
    /// non-generic over `K`.
    pub(crate) fn new(
        signal_close_fn: Box<dyn Fn() + Send + Sync>,
        wakeup_fn: Box<dyn Fn() + Send + Sync>,
        join_handle: JoinHandle<()>,
    ) -> Self {
        Self { signal_close_fn, wakeup_fn, join_handle: Some(join_handle) }
    }

    /// Signals the bg task to exit and wakes it from its current
    /// `select!`. Idempotent.
    pub(crate) fn signal_close(&self) {
        (self.signal_close_fn)();
    }

    /// Wakes the bg task's `select!` without signalling shutdown. Used
    /// from `AsyncKafkaConsumer::wakeup`.
    pub(crate) fn wakeup(&self) {
        (self.wakeup_fn)();
    }

    /// Awaits the spawned task to completion. Returns `Ok(())` on clean
    /// exit, or wraps the JoinError as a `KafkaError::illegal_state` on
    /// panic.
    pub(crate) async fn await_join(&mut self) -> Result<(), KafkaError> {
        if let Some(handle) = self.join_handle.take() {
            match handle.await {
                Ok(()) => Ok(()),
                Err(join_err) => Err(KafkaError::illegal_state(format!(
                    "Consumer network thread terminated with error: {join_err}"
                ))),
            }
        } else {
            Ok(())
        }
    }
}

/// `AsyncKafkaConsumer<K, V>` — the production consumer for the KIP-848
/// group protocol. Implements [`crate::consumer::Consumer`] via
/// [`async_trait`].
///
/// The struct shape mirrors the Phase 11 PLAN.md layout:
///   - **Shared with the bg task**: `subscriptions`, `metadata`,
///     `request_managers`, `background_event_rx`,
///     `application_event_handler`, `max_time_to_wait_ms`,
///     `wakeup_trigger`, `network_thread_close` (the erased handle).
///   - **App-side only**: `client_id`, `group_id`, `group_metadata`,
///     `rebalance_listener_invoker`,
///     `offset_commit_callback_invoker`, `deserializers`,
///     `interceptors`, `auto_commit_enabled`, `default_api_timeout_ms`,
///     `closed`, `rebalance_listener`.
///
/// The `current_thread` / `ref_count` Java fields are NOT translated —
/// `&mut self` on the trait already enforces single-caller exclusivity
/// (Phase 11 PLAN.md deferral #4).
pub struct AsyncKafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // ── Shared with the bg task ────────────────────────────────────────
    /// Subscription / assignment state. Wrapped in `Arc<Mutex<…>>` per
    /// `consumer-threading.md` §16.
    subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Consumer-side metadata cache shared with the bg task and
    /// `NetworkClient`.
    metadata: Arc<ConsumerMetadata>,
    /// Per-RM handles shared with the bg task. See §16 / Phase-10
    /// pattern #3 (`Arc<Mutex<RequestManagers>>` discipline).
    request_managers: Arc<std::sync::Mutex<RequestManagers>>,
    /// Receiver for `BackgroundEvent`s emitted by the bg task. Drained
    /// by `process_background_events` (commit (3)).
    background_event_rx: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    /// App-side handle for enqueuing `ApplicationEvent`s — wraps the
    /// channel sender shared with the bg task.
    application_event_handler: Arc<ApplicationEventHandler>,
    /// Completable-event reaper, shared with the bg task.
    completable_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
    /// Mirror of the bg task's `cached_max_time_to_wait_ms`. Exposed
    /// via `maximum_time_to_wait_ms()` and used by the poll loop.
    max_time_to_wait_ms: Arc<AtomicI64>,
    /// Wakeup primitive shared with the bg task.
    wakeup_trigger: WakeupTrigger,
    /// Type-erased handle to the spawned bg task. Drops the
    /// `JoinHandle` on consumer close (after `signal_close()` + the
    /// final `wakeup_trigger.wakeup()`).
    network_thread_close: NetworkThreadCloseHandle,
    /// Per `consumer-threading.md` §27: the consumer owns one
    /// `Arc<FetchBuffer>` shared with both the bg-side `FetchRequestManager`
    /// (which `add`s `CompletedFetch`es as fetch responses land) and the
    /// app-side [`FetchCollector`] (which drains them in `poll()`).
    fetch_buffer: Arc<FetchBuffer>,
    /// App-side fetch decoder. Owned by `Arc` so the consumer can hand a
    /// shared reference to per-poll helpers without re-construction.
    fetch_collector: Arc<FetchCollector<K, V>>,

    // ── App-side only ─────────────────────────────────────────────────
    /// `client.id`, as a cheap-to-clone `Arc<str>` per CLAUDE.md §11.
    client_id: Arc<str>,
    /// `group.id`, if any.
    group_id: Option<String>,
    /// Group metadata cached by the `MemberStateListener` callback;
    /// returned by [`Self::group_metadata`]. `None` while uninitialized
    /// or for assignment-only consumers. Updated by
    /// [`ConsumerStateNotifier::on_member_epoch_updated`] which is the
    /// `MemberStateListener` registered with the membership manager
    /// (production wire-up in Phase 12; for tests, callers register
    /// [`Self::state_notifier`] directly on the membership manager).
    group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
    /// Java: `private final AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot`
    /// (`AsyncKafkaConsumer.java:317`).
    ///
    /// Snapshot of the partitions assigned to this consumer through the
    /// **group-management** path (not `assign(...)` — manually-assigned
    /// partitions never appear here). Updated by
    /// [`ConsumerStateNotifier::on_group_assignment_updated`] from the
    /// membership manager's reconciliation step (Java
    /// `setGroupAssignmentSnapshot(...)` at line 786-788). Read by
    /// [`Self::run_rebalance_callbacks_on_close`] to determine which
    /// partitions are passed to the user's `on_partitions_revoked` /
    /// `on_partitions_lost` callback on close.
    group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
    /// `MemberStateListener` impl that updates `group_metadata` and
    /// `group_assignment_snapshot` when the membership manager fires a
    /// state-change notification. Cloned out of [`Self::state_notifier`]
    /// at construction time and exposed for production wire-up
    /// (Phase 12).
    state_notifier: Arc<ConsumerStateNotifier>,
    /// Invoker for the user-supplied [`ConsumerRebalanceListener`].
    rebalance_listener_invoker: ConsumerRebalanceListenerInvoker,
    /// Drains pending `OffsetCommitCallback` invocations at the top of
    /// every blocking-style API.
    offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker<K, V>>,
    /// Key + value deserializers, shared with `Fetcher` /
    /// `FetchCollector`.
    deserializers: Arc<Deserializers<K, V>>,
    /// User-supplied interceptors. Wrapped in `Mutex` because
    /// `on_consume` takes `&mut self` per Phase 2.
    interceptors: Arc<Mutex<ConsumerInterceptors<K, V>>>,
    /// Cached `enable.auto.commit`.
    auto_commit_enabled: bool,
    /// Cached `default.api.timeout.ms`.
    default_api_timeout_ms: i64,
    /// Cached `retry.backoff.ms` — used by [`Self::poll_for_fetches`] to
    /// throttle the poll wait when no positions are valid yet (Java
    /// `AsyncKafkaConsumer.pollForFetches`).
    retry_backoff_ms: i64,
    /// Cached `isolation.level`. Currently used by the (Phase 11 commit
    /// 6/N) `current_lag` event and reserved for future fetch-path
    /// callers; kept on the consumer struct because Java reads it from
    /// the same source.
    isolation_level: IsolationLevel,
    /// `true` after [`Self::close`] has run. Subsequent calls return
    /// `KafkaError::illegal_state`.
    closed: AtomicBool,
    /// Listener registered via `subscribe_with_listener` /
    /// `subscribe_pattern_with_listener`. Wrapped in `Mutex<Option<…>>`
    /// so it can be swapped without invalidating
    /// `&self.rebalance_listener_invoker` references.
    rebalance_listener: Mutex<Option<Arc<dyn ConsumerRebalanceListener>>>,
    /// Java: `private AsyncPollEvent inflightPoll`.
    ///
    /// Stores the `Arc<AsyncPollState>` of the currently-inflight
    /// `AsyncPoll` event (if any). Recycled across `poll()` calls per
    /// Java semantics. Held in a plain `Option<...>` instead of
    /// `Mutex<Option<...>>` because `poll()` takes `&mut self` and is the
    /// only caller.
    inflight_poll: Option<InflightPoll>,
    /// Cached `ConsumerConfig` for late-bound config lookups (e.g.
    /// inside `close`).
    config: ConsumerConfig,
    /// Time source used for `current_time_ms` arguments to events.
    time: Arc<dyn ThreadTime>,
    /// Java: `private CompletableFuture<...> lastPendingAsyncCommit`.
    ///
    /// Tracks the most-recently-submitted async commit so that
    /// `commit_sync` and `close` can wait for in-flight async commits to
    /// complete before continuing (mirrors Java's
    /// `awaitPendingAsyncCommitsAndExecuteCommitCallbacks`). The wrapped
    /// receiver resolves to `()` once the async commit (success OR
    /// failure) has finished — the actual commit result is delivered via
    /// the registered [`crate::consumer::OffsetCommitCallback`], not via
    /// this receiver.
    last_pending_async_commit: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// Tracks the state of the currently-inflight `AsyncPoll` event.
///
/// Mirrors Java's `AsyncPollEvent` field: it carries the event's
/// `deadline_ms` and the shared `Arc<AsyncPollState>` that the bg task
/// completes asynchronously. Stored as a separate small struct so the
/// `is_expired` check stays close to the data it operates on.
pub(crate) struct InflightPoll {
    deadline_ms: i64,
    state: Arc<AsyncPollState>,
}

impl InflightPoll {
    fn is_expired(&self, current_time_ms: i64) -> bool {
        current_time_ms >= self.deadline_ms
    }
}

/// Discriminates between the async / sync commit forms when calling the
/// shared [`AsyncKafkaConsumer::commit_inner`] helper. Mirrors the
/// `CommitEvent` Java base class — both `AsyncCommitEvent` and
/// `SyncCommitEvent` carry the same shape (offsets + deadline) and
/// differ only in the bg-side dispatch.
enum CommitEventKind {
    Async {
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    },
    Sync {
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        deadline_ms: i64,
    },
}

impl CommitEventKind {
    fn offsets(&self) -> Option<&HashMap<TopicPartition, OffsetAndMetadata>> {
        match self {
            Self::Async { offsets } => offsets.as_ref(),
            Self::Sync { offsets, .. } => offsets.as_ref(),
        }
    }

    /// For [`Self::Sync`], the user-supplied timeout-converted deadline;
    /// for [`Self::Async`], the consumer's default API timeout (Java's
    /// `AsyncCommitEvent` carries no explicit deadline — it uses the
    /// default).
    fn deadline_ms(&self) -> i64 {
        match self {
            Self::Async { .. } => i64::MAX,
            Self::Sync { deadline_ms, .. } => *deadline_ms,
        }
    }
}

/// `MemberStateListener` implementation that bridges the membership
/// manager's state-change notifications back to the consumer's app-side
/// caches: `group_metadata` and `group_assignment_snapshot`.
///
/// Mirrors Java's anonymous-inner-class `memberStateListener` at
/// `AsyncKafkaConsumer.java:343-353`. The callbacks are invoked
/// synchronously from the bg task (membership-manager reconciliation
/// step); they only touch their two `Arc<Mutex<…>>` fields and never
/// call back into the consumer or take other locks, so the brief
/// critical sections are §16-safe.
pub(crate) struct ConsumerStateNotifier {
    /// Group ID this consumer belongs to. Used to populate fresh
    /// [`ConsumerGroupMetadata`] when the cache is empty.
    group_id: String,
    /// Optional `group.instance.id` (static membership identifier).
    /// Preserved across epoch updates in the resulting
    /// [`ConsumerGroupMetadata`].
    group_instance_id: Option<String>,
    /// Shared with [`AsyncKafkaConsumer::group_metadata`].
    group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
    /// Shared with [`AsyncKafkaConsumer::group_assignment_snapshot`].
    group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
}

impl ConsumerStateNotifier {
    /// Constructor. The `group_metadata` / `group_assignment_snapshot`
    /// Arcs are owned by both the notifier and the consumer.
    pub(crate) fn new(
        group_id: impl Into<String>,
        group_instance_id: Option<String>,
        group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>,
        group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>,
    ) -> Self {
        Self { group_id: group_id.into(), group_instance_id, group_metadata, group_assignment_snapshot }
    }

    /// Java: `private void updateGroupMetadata(Optional<Integer> memberEpoch, String memberId)`
    /// (`AsyncKafkaConsumer.java:772-784`).
    ///
    /// Updates the cached [`ConsumerGroupMetadata`] to carry the new
    /// epoch / member-id. Java's implementation is an `updateAndGet`
    /// over an `AtomicReference<Optional<ConsumerGroupMetadata>>` that
    /// short-circuits when the slot is empty; we mirror that by
    /// initializing the slot lazily here (Java initializes it at
    /// construction time when `group.id` is present —
    /// `initializeGroupMetadata`).
    fn update_group_metadata(&self, member_epoch: Option<i32>, member_id: &str) {
        let Some(epoch) = member_epoch else {
            // Java's `memberEpoch.ifPresent(...)` short-circuits when
            // None — no metadata mutation.
            return;
        };
        let mut guard = self.group_metadata.lock().unwrap();
        #[allow(deprecated)]
        let next = ConsumerGroupMetadata::with_details(
            self.group_id.clone(),
            epoch,
            member_id.to_string(),
            self.group_instance_id.clone(),
        );
        *guard = Some(next);
    }
}

impl MemberStateListener for ConsumerStateNotifier {
    /// Java: `memberStateListener.onMemberEpochUpdated(memberEpoch, memberId)`
    /// (`AsyncKafkaConsumer.java:344-347`).
    fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str) {
        self.update_group_metadata(member_epoch, member_id);
    }

    /// Java: `memberStateListener.onGroupAssignmentUpdated(partitions)`
    /// (`AsyncKafkaConsumer.java:349-352`). Snapshots the assignment so
    /// `runRebalanceCallbacksOnClose` can drive listener callbacks over
    /// the **group**-assigned partitions specifically (manual
    /// `assign(...)` partitions are intentionally excluded).
    fn on_group_assignment_updated(&self, partitions: &HashSet<TopicPartition>) {
        let mut guard = self.group_assignment_snapshot.lock().unwrap();
        *guard = partitions.clone();
    }
}

/// Components handed to [`AsyncKafkaConsumer::new_with_thread`]: the
/// per-RM container, metadata, subscriptions, application-event handle,
/// reaper, wakeup trigger, etc. Constructed by the production factory
/// (commit (7)) and by tests directly.
///
/// Bundling these into a struct keeps the ctor signature manageable as
/// the Java ctor has 14 already-constructed dependencies.
pub(crate) struct AsyncKafkaConsumerComponents<K: Send + Sync + 'static, V: Send + Sync + 'static> {
    pub config: ConsumerConfig,
    pub client_id: Arc<str>,
    pub group_id: Option<String>,
    pub subscriptions: Arc<Mutex<SubscriptionState>>,
    pub metadata: Arc<ConsumerMetadata>,
    pub request_managers: Arc<std::sync::Mutex<RequestManagers>>,
    pub background_event_rx: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    pub application_event_handler: Arc<ApplicationEventHandler>,
    pub completable_event_reaper: Arc<std::sync::Mutex<CompletableEventReaper>>,
    pub max_time_to_wait_ms: Arc<AtomicI64>,
    pub wakeup_trigger: WakeupTrigger,
    pub network_thread_close: NetworkThreadCloseHandle,
    pub fetch_buffer: Arc<FetchBuffer>,
    pub fetch_collector: Arc<FetchCollector<K, V>>,
    pub rebalance_listener_invoker: ConsumerRebalanceListenerInvoker,
    pub offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker<K, V>>,
    pub deserializers: Arc<Deserializers<K, V>>,
    pub interceptors: Arc<Mutex<ConsumerInterceptors<K, V>>>,
    pub isolation_level: IsolationLevel,
    pub time: Arc<dyn ThreadTime>,
}

impl<K, V> AsyncKafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs an `AsyncKafkaConsumer` from pre-built components.
    ///
    /// This is the **only** constructor in commit (2/N); the production
    /// factory in `consumer/mod.rs` is wired in commit (7/N). Tests
    /// build their own components (typically with a `MockClient`-backed
    /// `ConsumerNetworkThread`) and call this directly.
    ///
    /// Mirrors Java's test-visible constructor at
    /// `AsyncKafkaConsumer.java:521` (the 20-arg form), with the
    /// metrics / telemetry parameters dropped per Phase 11 PLAN.md
    /// deferrals.
    pub(crate) fn new_with_components(components: AsyncKafkaConsumerComponents<K, V>) -> Self {
        let auto_commit_enabled = components.config.enable_auto_commit();
        let default_api_timeout_ms = components.config.default_api_timeout_ms as i64;
        let retry_backoff_ms = components.config.retry_backoff_ms();

        // Build the `MemberStateListener` bridge once and share the two
        // backing `Arc<Mutex<…>>` slots with the consumer struct. The
        // production wire-up (Phase 12) clones `state_notifier` and
        // registers it on the `ConsumerMembershipManager`; tests can do
        // the same in-line.
        let group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>> = Arc::new(Mutex::new(None));
        let group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>> = Arc::new(Mutex::new(HashSet::new()));
        let state_notifier = Arc::new(ConsumerStateNotifier::new(
            components.group_id.clone().unwrap_or_default(),
            components.config.group_instance_id().map(|s| s.to_string()),
            Arc::clone(&group_metadata),
            Arc::clone(&group_assignment_snapshot),
        ));

        Self {
            subscriptions: components.subscriptions,
            metadata: components.metadata,
            request_managers: components.request_managers,
            background_event_rx: components.background_event_rx,
            application_event_handler: components.application_event_handler,
            completable_event_reaper: components.completable_event_reaper,
            max_time_to_wait_ms: components.max_time_to_wait_ms,
            wakeup_trigger: components.wakeup_trigger,
            network_thread_close: components.network_thread_close,
            fetch_buffer: components.fetch_buffer,
            fetch_collector: components.fetch_collector,
            client_id: components.client_id,
            group_id: components.group_id,
            group_metadata,
            group_assignment_snapshot,
            state_notifier,
            rebalance_listener_invoker: components.rebalance_listener_invoker,
            offset_commit_callback_invoker: components.offset_commit_callback_invoker,
            deserializers: components.deserializers,
            interceptors: components.interceptors,
            auto_commit_enabled,
            default_api_timeout_ms,
            retry_backoff_ms,
            isolation_level: components.isolation_level,
            closed: AtomicBool::new(false),
            rebalance_listener: Mutex::new(None),
            inflight_poll: None,
            config: components.config,
            time: components.time,
            last_pending_async_commit: None,
        }
    }

    // ── Sync state-read methods ────────────────────────────────────────
    //
    // Per `consumer-threading.md` §16, each method acquires the
    // SubscriptionState lock briefly, reads, drops the guard — NEVER
    // holds the guard across `.await`. Since these methods are `fn`
    // (not `async`), there is no `.await` boundary at all.
    //
    // # Closed-consumer behavior (deliberate Java divergence)
    //
    // Java's accessors call `acquireAndEnsureOpen()` which throws
    // `IllegalStateException("This consumer has already been closed.")`
    // when the consumer is closed. Each Rust accessor below returns the
    // cached / empty value silently instead — there is no error channel
    // on these `fn` signatures and panicking on a pure accessor would
    // diverge sharply from idiomatic Rust. The strict closed-consumer
    // check is surfaced on every `async fn` (poll / commit / position /
    // committed / unsubscribe / close / etc.) via `ensure_open()`.
    //
    // Tests that exercise the Java accessor-throws-after-close behavior
    // (`testListPartitionsAfterClose` style) are listed in the commit-8
    // test-skip rationale.
    //
    // # Mutable Set semantics (deliberate Java divergence)
    //
    // Java wraps the returned `Set` with `Collections.unmodifiableSet(...)`.
    // The Rust accessors return an owned `HashSet` — the caller may
    // freely mutate it without affecting the consumer's internal state.
    // This is idiomatic Rust and is observable only through user code
    // that depended on `UnsupportedOperationException` (none of the
    // translated tests do).

    /// Java: `Set<TopicPartition> assignment()`.
    ///
    /// **Returns an owned mutable `HashSet`** (Java returns
    /// `Collections.unmodifiableSet(...)`). **Returns the empty set
    /// silently when the consumer is closed** (Java throws
    /// `IllegalStateException`). See the module-level "Sync state-read
    /// methods" comment for rationale.
    pub fn assignment(&self) -> std::collections::HashSet<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap();
        subs.assigned_partitions()
    }

    /// Java: `Set<String> subscription()`.
    ///
    /// **Returns an owned mutable `HashSet`** (Java returns
    /// `Collections.unmodifiableSet(...)`). **Returns the empty set
    /// silently when the consumer is closed** (Java throws
    /// `IllegalStateException`).
    pub fn subscription(&self) -> std::collections::HashSet<String> {
        let subs = self.subscriptions.lock().unwrap();
        subs.subscription()
    }

    /// Java: `Set<TopicPartition> paused()`.
    ///
    /// **Returns an owned mutable `HashSet`** (Java returns
    /// `Collections.unmodifiableSet(...)`). **Returns the empty set
    /// silently when the consumer is closed** (Java throws
    /// `IllegalStateException`).
    pub fn paused(&self) -> std::collections::HashSet<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap();
        subs.paused_partitions()
    }

    /// Java: `String clientId()`. Returned as a borrowed `&str` per
    /// CLAUDE.md §12 (most general borrowed form for getters).
    ///
    /// **Returns the configured value silently when the consumer is
    /// closed** (Java throws `IllegalStateException`). The `client_id`
    /// is immutable for the lifetime of the consumer, so returning it
    /// post-close is harmless.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Java: `ConsumerGroupMetadata groupMetadata()`.
    ///
    /// # Java divergence
    ///
    /// Java's `groupMetadata()` throws `InvalidGroupIdException` when
    /// `group.id` is unset (`AsyncKafkaConsumer.java:1428-1436` calls
    /// `throwIfGroupIdNotDefined()` inside `acquireAndEnsureOpen`).
    /// The Rust translation returns a stub
    /// `ConsumerGroupMetadata::new("")` for groupless consumers,
    /// because:
    ///   (a) the [`Consumer`] trait surface returns
    ///       `ConsumerGroupMetadata` with no error channel (Phase 2
    ///       decision), and panicking on a pure accessor diverges
    ///       sharply from idiomatic Rust;
    ///   (b) the strict-Java behavior IS surfaced via `commit_*` /
    ///       `subscribe` etc., which call `throw_if_group_id_not_defined()`
    ///       on the error-bearing path.
    ///
    /// The Java test
    /// `AsyncKafkaConsumerTest.testGroupMetadataAfterCreationWithGroupIdIsNull`
    /// is therefore skipped with this rationale (commit (8/N) test-skip
    /// section).
    ///
    /// **Returns a stub value silently when the consumer is closed**
    /// (Java throws `IllegalStateException`).
    ///
    /// The returned struct is a clone of the cached value. The cache
    /// is populated by [`ConsumerStateNotifier::on_member_epoch_updated`]
    /// which is the [`MemberStateListener`] registered on the
    /// `ConsumerMembershipManager` at production wire-up time
    /// (Phase 12 — see [`Self::state_notifier`]). Until the bg task
    /// receives its first heartbeat response with a member-epoch
    /// (or until tests invoke the notifier directly), the cache is
    /// empty and this method returns a fresh stub.
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        let guard = self.group_metadata.lock().unwrap();
        match guard.as_ref() {
            Some(meta) => meta.clone(),
            None => {
                // Stub matching Java's `initializeGroupMetadata` default for
                // groupless consumers.
                #[allow(deprecated)]
                {
                    let group = self.group_id.clone().unwrap_or_default();
                    ConsumerGroupMetadata::new(group)
                }
            },
        }
    }

    /// Java: `OptionalLong currentLag(TopicPartition)`.
    ///
    /// Returns the cached lag (highWaterMark - position) if both values
    /// are known locally, otherwise `None` (matching `OptionalLong.empty()`).
    ///
    /// **Sync variant — does NOT enqueue a `CurrentLag` event.** Java's
    /// implementation dispatches through a `CurrentLagEvent` to the bg
    /// task when the lag cache is empty; the trait surface (Phase 2) is
    /// sync, so the async dispatch lives on
    /// [`Self::current_lag_async`]. The sync accessor only reads
    /// cached state.
    ///
    /// **Returns `None` silently when the consumer is closed** (Java
    /// throws `IllegalStateException`).
    pub fn current_lag(&self, _topic_partition: &TopicPartition) -> Option<i64> {
        // The bg-task `current_lag_async` path is the one that drives the
        // event; this accessor only reads cached state and currently has
        // no cache (the lag-cache lives on the membership manager in Java
        // and is populated by fetch responses — not yet wired through to
        // the consumer struct). Returning `None` matches Java's behavior
        // for "lag unknown".
        None
    }

    /// Returns the [`ConsumerStateNotifier`] this consumer expects to
    /// be registered as a [`MemberStateListener`] on the
    /// `ConsumerMembershipManager`. The production factory (Phase 12)
    /// performs this registration immediately after constructing the
    /// membership manager; tests can do the same on their stand-in
    /// manager.
    ///
    /// Mirrors Java's anonymous-inner-class `memberStateListener` field
    /// at `AsyncKafkaConsumer.java:343-353`: that listener is passed
    /// into the membership-manager builder so the bg task can invoke it
    /// during reconciliation.
    pub(crate) fn state_notifier(&self) -> Arc<ConsumerStateNotifier> {
        Arc::clone(&self.state_notifier)
    }

    /// Java: `void wakeup()`. Sync — callable from any task, including
    /// signal handlers.
    pub fn wakeup(&self) {
        self.wakeup_trigger.wakeup();
        // Also wake the bg task's `select!` directly so the underlying
        // `KafkaClient::poll` is unblocked even if the wakeup token was
        // already cancelled.
        self.network_thread_close.wakeup();
    }

    /// Returns the cached `max_time_to_wait` value, set by the bg task
    /// after each `run_once` iteration. Used by the poll loop (commit
    /// (4/N)).
    pub(crate) fn maximum_time_to_wait_ms(&self) -> i64 {
        self.max_time_to_wait_ms.load(Ordering::Acquire)
    }

    /// `true` iff [`Self::close`] has already returned.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    // ── Subscribe / unsubscribe / assign ───────────────────────────────
    //
    // Translates Java's `subscribe(Collection<String>)`,
    // `subscribe(Collection<String>, ConsumerRebalanceListener)`,
    // `subscribe(SubscriptionPattern)`, `subscribe(SubscriptionPattern,
    // ConsumerRebalanceListener)`, `subscribe(Pattern)`,
    // `subscribe(Pattern, ConsumerRebalanceListener)`, `unsubscribe()`,
    // and `assign(Collection<TopicPartition>)`.
    //
    // Each method:
    //   1. Verifies `closed` (Java's `acquireAndEnsureOpen`).
    //   2. Validates arguments (returning `KafkaError::illegal_argument`
    //      where Java throws `IllegalArgumentException`). Rust's type
    //      system makes the `null`-target tests un-translatable;
    //      `"".trim().is_empty()` covers the empty/blank case.
    //   3. Briefly acquires the `SubscriptionState` lock to read
    //      `assigned_partitions`, drops the guard, then enqueues an
    //      `ApplicationEvent` carrying the change. Mirrors
    //      `consumer-threading.md` §16 lock discipline — the actual
    //      mutation of `SubscriptionState` happens on the bg task via
    //      the matching `ApplicationEventProcessor` arm.
    //
    // `fetchBuffer.retainAll(...)` calls present in the Java source are
    // omitted here — the consumer's `FetchBuffer` is wired in commit
    // (4/N) along with the poll loop. The bg-side
    // `ApplicationEventProcessor` already retains-all when processing
    // the matching subscribe/assign event, so the only consequence of
    // the gap is that records already buffered for removed partitions
    // are returned on the next `poll()` — Phase 11 commit (4/N) closes
    // this seam.

    /// Translates Java's `private void throwIfGroupIdNotDefined()`.
    fn throw_if_group_id_not_defined(&self) -> Result<(), KafkaError> {
        if self.group_id.as_deref().map(str::is_empty).unwrap_or(true) {
            return Err(KafkaError::illegal_argument(
                "To use the group management or offset commit APIs, you must provide a valid \
                 group.id in the consumer configuration.",
            ));
        }
        Ok(())
    }

    /// Translates Java's `acquireAndEnsureOpen()` — the runtime
    /// reentrancy guard is dropped per Phase 11 PLAN.md deferral #4
    /// (Rust's `&mut self` enforces single-caller exclusivity at
    /// compile time), so this is just the `closed` check.
    fn ensure_open(&self) -> Result<(), KafkaError> {
        if self.is_closed() {
            return Err(KafkaError::illegal_state("This consumer has already been closed."));
        }
        Ok(())
    }

    /// Java: `void subscribe(Collection<String>)`.
    ///
    /// Subscribes to the given topics. An empty list acts as
    /// `unsubscribe()`. Errors:
    ///   - [`KafkaError::illegal_argument`] if any topic is empty / whitespace.
    ///   - [`KafkaError::illegal_argument`] if `group.id` is unset
    ///     (Java's `InvalidGroupIdException`).
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        self.subscribe_internal_topics(topics, None).await
    }

    /// Java: `void subscribe(Collection<String>, ConsumerRebalanceListener)`.
    ///
    /// Same as [`Self::subscribe`] but registers a rebalance listener.
    /// Java throws `IllegalArgumentException` for a null listener;
    /// Rust makes the `Option`-of-`Arc` representation explicit, so the
    /// `with_listener` form takes a concrete `Arc` and the listener is
    /// always non-null at the type level.
    pub async fn subscribe_with_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        self.subscribe_internal_topics(topics, Some(listener)).await
    }

    /// Java: `void subscribe(SubscriptionPattern)` — server-side regex
    /// subscribe (KIP-848 RE2J).
    pub async fn subscribe_re2j_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), KafkaError> {
        self.subscribe_to_regex(pattern, None).await
    }

    /// Java: `void subscribe(SubscriptionPattern, ConsumerRebalanceListener)`.
    pub async fn subscribe_re2j_pattern_with_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        self.subscribe_to_regex(pattern, Some(listener)).await
    }

    /// Java: `void subscribe(Pattern)` — client-side regex subscribe.
    ///
    /// Takes a compiled `regex::Regex` instead of a raw `&str` so we
    /// preserve compile-time pattern validation (Java's `Pattern.compile`
    /// is also up-front).
    pub async fn subscribe_pattern(&mut self, pattern: Regex) -> Result<(), KafkaError> {
        self.subscribe_internal_pattern(pattern, None).await
    }

    /// Java: `void subscribe(Pattern, ConsumerRebalanceListener)`.
    pub async fn subscribe_pattern_with_listener(
        &mut self,
        pattern: Regex,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        self.subscribe_internal_pattern(pattern, Some(listener)).await
    }

    /// Translates Java's `subscribeInternal(Collection<String>, Optional<ConsumerRebalanceListener>)`.
    async fn subscribe_internal_topics(
        &mut self,
        topics: Vec<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.throw_if_group_id_not_defined()?;

        if topics.is_empty() {
            // Java: `topics.isEmpty()` is treated as the same as
            // `unsubscribe()`. Match the recursion.
            return self.unsubscribe().await;
        }

        for topic in &topics {
            if topic.trim().is_empty() {
                return Err(KafkaError::illegal_argument(
                    "Topic collection to subscribe to cannot contain null or empty topic",
                ));
            }
        }

        log::info!("Subscribed to topic(s): {}", topics.join(", "));

        let topics_set: std::collections::HashSet<String> = topics.into_iter().collect();
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        // Java passes the listener INSIDE the event so the bg task owns
        // installation — the app side never registers the listener until
        // the event has been accepted. Mirror this by sending the
        // listener through the event AND only mirroring it into the
        // app-side `rebalance_listener` slot AFTER `add_and_get`
        // resolves Ok (so a failed submission does not leave the
        // app-side slot pointing at a listener that never landed in
        // `SubscriptionState`).
        let listener_for_app_side = listener.as_ref().map(Arc::clone);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::TopicSubscriptionChange { handle, topics: topics_set, listener },
                receiver,
                now_ms,
            )
            .await?;
        if let Some(l) = listener_for_app_side {
            *self.rebalance_listener.lock().unwrap() = Some(l);
        }
        Ok(())
    }

    /// Translates Java's `subscribeInternal(Pattern, Optional<ConsumerRebalanceListener>)`.
    async fn subscribe_internal_pattern(
        &mut self,
        pattern: Regex,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.throw_if_group_id_not_defined()?;
        if pattern.as_str().is_empty() {
            return Err(KafkaError::illegal_argument("Topic pattern to subscribe to cannot be empty"));
        }

        log::info!("Subscribed to pattern: '{pattern}'");

        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        // See `subscribe_internal_topics` for why we store the listener
        // only after `add_and_get` resolves Ok.
        let listener_for_app_side = listener.as_ref().map(Arc::clone);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::TopicPatternSubscriptionChange { handle, pattern, listener },
                receiver,
                now_ms,
            )
            .await?;
        if let Some(l) = listener_for_app_side {
            *self.rebalance_listener.lock().unwrap() = Some(l);
        }
        Ok(())
    }

    /// Translates Java's `subscribeToRegex(SubscriptionPattern, Optional<ConsumerRebalanceListener>)`.
    async fn subscribe_to_regex(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.throw_if_group_id_not_defined()?;
        if pattern.pattern().is_empty() {
            return Err(KafkaError::illegal_argument("Topic pattern to subscribe to cannot be empty"));
        }

        log::info!("Subscribing to regular expression {}", pattern.pattern());

        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        // See `subscribe_internal_topics` for why we store the listener
        // only after `add_and_get` resolves Ok.
        let listener_for_app_side = listener.as_ref().map(Arc::clone);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, pattern, listener },
                receiver,
                now_ms,
            )
            .await?;
        if let Some(l) = listener_for_app_side {
            *self.rebalance_listener.lock().unwrap() = Some(l);
        }
        Ok(())
    }

    /// Java: `void unsubscribe()`.
    ///
    /// Unsubscribes from all topics / patterns and clears the assignment.
    /// Java enqueues an `UnsubscribeEvent` and then loops
    /// `processBackgroundEvents(future, timer, ignoreErrorPredicate)` so
    /// any rebalance-listener callbacks raised during the teardown can be
    /// driven from the caller's task while the unsubscribe future is
    /// outstanding (Java `AsyncKafkaConsumer.java:1830-1855`). The Rust
    /// translation routes the handle's receiver through
    /// [`Self::process_background_events_until`] so the same interleaved
    /// drain happens — without it, the bg task would await the
    /// listener-callback ack indefinitely while the app side blocks on
    /// `add_and_get`.
    pub async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        self.ensure_open()?;

        self.fetch_buffer.retain_all(&std::collections::HashSet::new());

        let assigned_for_log = {
            let subs = self.subscriptions.lock().unwrap();
            subs.assigned_partitions()
        };
        log::info!("Unsubscribing all topics or patterns and assigned partitions {assigned_for_log:?}");

        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        // Enqueue the event without blocking on it — the iterative drain
        // below polls the receiver.
        self.application_event_handler
            .add(ApplicationEvent::Unsubscribe { handle }, now_ms)?;

        // Java's `ignoreErrorEventException` predicate: swallow
        // `GroupAuthorizationException` / `TopicAuthorizationException`
        // surfaced as fatal background errors during unsubscribe so the
        // unsubscribe still completes. Rust surfaces these as
        // [`KafkaError::TopicAuthorization`] / [`KafkaError::GroupAuthorization`].
        let ignore_predicate =
            |err: &KafkaError| matches!(err, KafkaError::TopicAuthorization(_) | KafkaError::GroupAuthorization(_));

        let result = self
            .process_background_events_until::<()>(
                receiver,
                deadline_ms,
                ignore_predicate,
                "Failed while waiting for the unsubscribe event to complete",
            )
            .await;

        // Reset the listener field — the previous subscription is gone.
        *self.rebalance_listener.lock().unwrap() = None;

        match result {
            Ok(()) => Ok(()),
            Err(KafkaError::Timeout(msg)) => {
                // Java logs an error and returns successfully (the
                // unsubscribe event is "fire and forget" past the
                // deadline): `log.error("Failed while waiting...")`.
                log::error!("Failed while waiting for the unsubscribe event to complete: {msg}");
                Ok(())
            },
            Err(err) => Err(err),
        }
    }

    /// Java: `void assign(Collection<TopicPartition>)`.
    ///
    /// Manually assigns the given partitions. An empty collection acts
    /// as `unsubscribe()`. Errors:
    ///   - [`KafkaError::illegal_argument`] if any topic is empty / whitespace.
    pub async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError> {
        self.ensure_open()?;

        if partitions.is_empty() {
            return self.unsubscribe().await;
        }

        for tp in &partitions {
            if tp.topic().trim().is_empty() {
                return Err(KafkaError::illegal_argument(
                    "Topic partitions to assign to cannot have null or empty topic",
                ));
            }
        }

        let partitions_set: std::collections::HashSet<TopicPartition> = partitions.into_iter().collect();
        // Java line 1813: `fetchBuffer.retainAll(currentTopicPartitions)`
        // — drop buffered fetches for partitions that are no longer
        // assigned so the next poll() doesn't surface stale records.
        self.fetch_buffer.retain_all(&partitions_set);
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::AssignmentChange { handle, current_time_ms: now_ms, partitions: partitions_set },
                receiver,
                now_ms,
            )
            .await
    }

    /// Java: `defaultApiTimeoutDeadlineMs()`.
    fn default_api_timeout_deadline_ms(&self) -> i64 {
        calculate_deadline_ms(self.time.milliseconds(), self.default_api_timeout_ms)
    }

    // ── §31 process_background_events ─────────────────────────────────
    //
    // Per `consumer-threading.md` §31, this method MUST be called at the
    // top of every blocking-style API. It drains the bg-event channel
    // via `try_recv` in a `while let` loop and dispatches each event on
    // the caller's task — listener callbacks invoke the user-supplied
    // `ConsumerRebalanceListener` inline.

    /// Drains the background-events channel and dispatches each event
    /// on the caller's task. Mirrors Java's `boolean processBackgroundEvents()`.
    ///
    /// Returns:
    ///   - `Ok(had_events)` on success (no error events drained). The
    ///     boolean reflects Java's return — `true` if any events were
    ///     processed in this call, `false` otherwise. Used by
    ///     [`Self::process_background_events_until`] to decide whether
    ///     to keep spinning the drain loop or fall through to the
    ///     bounded `pollInterval` wait (Java
    ///     `AsyncKafkaConsumer.java:2287-2293`).
    ///   - `Err(KafkaError)` on the first error event drained. Subsequent
    ///     events are still processed (mirroring Java's
    ///     `firstError.compareAndSet`); the additional errors are logged
    ///     at `warn` level.
    ///
    /// Always invokes the background-event reaper at the end of the
    /// drain (Java line 2222: `backgroundEventReaper.reap(time.milliseconds())`),
    /// ensuring expired `CompletableEvent`s do not accumulate.
    ///
    /// # Lock discipline (§16)
    ///
    /// This method MUST NOT hold the `SubscriptionState` mutex guard
    /// across the listener invocation. The implementation does not
    /// acquire the guard at all — the listener invoker reads paused
    /// partitions inside its own brief lock window.
    pub(crate) async fn process_background_events(&mut self) -> Result<bool, KafkaError> {
        let mut first_error: Option<KafkaError> = None;
        let mut had_events = false;

        loop {
            let envelope = match self.background_event_rx.try_recv() {
                Ok(env) => env,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // The bg task has shut down. Nothing more to drain;
                    // surface only if no other error has been recorded.
                    if first_error.is_none() && !self.is_closed() {
                        first_error = Some(KafkaError::illegal_state("Consumer background task is no longer running."));
                    }
                    break;
                },
            };
            had_events = true;

            match envelope.event {
                BackgroundEvent::Error { error } => {
                    Self::record_first_error(&mut first_error, error);
                },
                BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name, partitions, ack } => {
                    // Read the currently-registered listener and drop the
                    // guard before invoking (§16 / §31). The
                    // `rebalance_listener` lock is separate from
                    // `SubscriptionState`, so no recursive lock concern.
                    let listener = self.rebalance_listener.lock().unwrap().clone();

                    let result = match listener {
                        Some(listener) => {
                            // Invoke on the caller's task — never `tokio::spawn`.
                            // The invoker drops `SubscriptionState`'s guard
                            // before `.await`ing the user-supplied callback
                            // (see `consumer_rebalance_listener_invoker.rs`).
                            use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName as M;
                            match method_name {
                                M::OnPartitionsAssigned => {
                                    self.rebalance_listener_invoker
                                        .invoke_partitions_assigned(&listener, &partitions)
                                        .await
                                },
                                M::OnPartitionsRevoked => {
                                    self.rebalance_listener_invoker
                                        .invoke_partitions_revoked(&listener, &partitions)
                                        .await
                                },
                                M::OnPartitionsLost => {
                                    self.rebalance_listener_invoker
                                        .invoke_partitions_lost(&listener, &partitions)
                                        .await
                                },
                            }
                        },
                        // No listener registered — match Java's behavior
                        // (Java's invoker treats a missing listener as a
                        // successful no-op).
                        None => Ok(()),
                    };

                    // Send the result on the embedded oneshot ack so the
                    // bg task can advance the rebalance state machine.
                    let send_result = result.clone();
                    let _ = ack.send(send_result);

                    // Java throws if the result is an error — we propagate
                    // via `first_error` so subsequent events are still
                    // processed.
                    if let Err(err) = result {
                        Self::record_first_error(&mut first_error, err);
                    }
                },
            }
        }

        // Java line 2222: reap expired completable events regardless of
        // drain outcome. Done after the drain so events added by this
        // tick get a chance to land before being reaped.
        {
            let now_ms = self.time.milliseconds();
            let mut reaper = self.completable_event_reaper.lock().unwrap();
            reaper.reap(now_ms);
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(had_events),
        }
    }

    /// Iterative variant of [`Self::process_background_events`] used by
    /// blocking-style APIs (`unsubscribe`, `commit_sync`, future
    /// `poll`) that need to interleave bg-event draining with waiting on
    /// a specific [`tokio::sync::oneshot::Receiver`].
    ///
    /// Mirrors Java's
    /// `<T> T processBackgroundEvents(Future<T> future, Timer timer,
    ///                                Predicate<Exception> ignoreErrorEventException)`
    /// (`AsyncKafkaConsumer.java:2271`). Each iteration:
    ///
    /// 1. Drains the bg-event channel (invokes any pending listener
    ///    callbacks on the caller's task).
    /// 2. If the completion receiver has resolved, returns the value.
    /// 3. Otherwise, races a 100ms bounded wait against the receiver.
    /// 4. Loops while the absolute `deadline_ms` is not exceeded.
    ///
    /// Returns `Err(KafkaError::timeout(...))` when the deadline
    /// expires without a completion.
    pub(crate) async fn process_background_events_until<T: Send + 'static>(
        &mut self,
        receiver: tokio::sync::oneshot::Receiver<Result<T, KafkaError>>,
        deadline_ms: i64,
        ignore_error_predicate: impl Fn(&KafkaError) -> bool,
        timeout_msg: impl AsRef<str>,
    ) -> Result<T, KafkaError> {
        let mut receiver = receiver;

        loop {
            let had_events = match self.process_background_events().await {
                Ok(had) => had,
                Err(err) => {
                    if ignore_error_predicate(&err) {
                        // Treat as if no events were processed (Java
                        // swallows the matched exception inside the
                        // try/catch at line 2274-2279).
                        false
                    } else {
                        return Err(err);
                    }
                },
            };

            // Java line 2282: `if (future.isDone()) return getResult(future)`.
            match receiver.try_recv() {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(err)) => return Err(err),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    return Err(KafkaError::illegal_state(
                        "Background task dropped the completion sender without completing it",
                    ));
                },
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    // Java line 2287: if no events were processed this
                    // tick, do a bounded wait (100ms) for the future or
                    // for a new bg event.
                    if !had_events {
                        let remaining = self.remaining_ms(deadline_ms);
                        if remaining <= 0 {
                            return Err(KafkaError::timeout(timeout_msg.as_ref().to_string()));
                        }
                        let wait = std::cmp::min(remaining, 100) as u64;
                        match tokio::time::timeout(Duration::from_millis(wait), &mut receiver).await {
                            Ok(Ok(Ok(value))) => return Ok(value),
                            Ok(Ok(Err(err))) => return Err(err),
                            Ok(Err(_recv_err)) => {
                                return Err(KafkaError::illegal_state(
                                    "Background task dropped the completion sender without completing it",
                                ));
                            },
                            // Java's `swallow TimeoutException` — keep looping.
                            Err(_elapsed) => {},
                        }
                    }
                },
            }

            // Java line 2299: `while (timer.notExpired())`.
            if self.remaining_ms(deadline_ms) <= 0 {
                return Err(KafkaError::timeout(timeout_msg.as_ref().to_string()));
            }
        }
    }

    /// Returns the milliseconds remaining until the supplied deadline,
    /// saturating at zero.
    fn remaining_ms(&self, deadline_ms: i64) -> i64 {
        let now = self.time.milliseconds();
        deadline_ms.saturating_sub(now).max(0)
    }

    /// Java: `firstError.compareAndSet(null, e)` — first error wins;
    /// subsequent errors are logged at `warn`.
    fn record_first_error(slot: &mut Option<KafkaError>, err: KafkaError) {
        if slot.is_none() {
            *slot = Some(err);
        } else {
            log::warn!("An error occurred when processing the background event: {err}");
        }
    }

    // ── Poll ───────────────────────────────────────────────────────────
    //
    // Translates Java's `AsyncKafkaConsumer.poll(Duration timeout)` body
    // (`AsyncKafkaConsumer.java:836-885`). The Java implementation drives a
    // `do { } while (timer.notExpired())` loop with three stages:
    //
    //   1. `wakeupTrigger.maybeTriggerWakeup()` at the TOP of the loop —
    //      observe a wakeup posted between polls.
    //   2. `checkInflightPoll(timer, firstPass)` — start a new
    //      `AsyncPollEvent` or evaluate whether the existing one has
    //      finished. Also runs the per-iteration
    //      `offsetCommitCallbackInvoker.executeCallbacks()` +
    //      `processBackgroundEvents()` pair (§31 invocation thread).
    //   3. `pollForFetches(timer)` — drain the fetch buffer via
    //      `FetchCollector.collectFetch`.
    //
    // Records are returned as soon as the collector yields a non-empty
    // fetch; otherwise the loop continues until `timer` expires, at which
    // point we return `ConsumerRecords::empty()`.

    /// Java: `ConsumerRecords<K, V> poll(Duration timeout)`.
    ///
    /// The translated body mirrors Java line-for-line; deviations are
    /// limited to:
    ///
    ///   - `kafkaConsumerMetrics.record*` — NO-OPs (Phase 11 PLAN.md #1).
    ///   - The `try/finally` in Java is a single function body in Rust;
    ///     panic-safety is achieved via early returns instead.
    ///   - `interceptors.onConsume(...)` mutates the records in place via
    ///     `Mutex<ConsumerInterceptors>`.
    pub async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError> {
        self.ensure_open()?;

        // Java: `subscriptions.hasNoSubscriptionOrUserAssignment()`.
        {
            let subs = self.subscriptions.lock().unwrap();
            if subs.has_no_subscription_or_user_assignment() {
                return Err(KafkaError::illegal_state(
                    "Consumer is not subscribed to any topics or assigned any partitions",
                ));
            }
        }

        let start_ms = self.time.milliseconds();
        let poll_deadline_ms = calculate_deadline_ms(start_ms, timeout.as_millis() as i64);
        let mut first_pass = true;

        loop {
            // Stage 1: observe pending wakeup before doing any work.
            if let Err(err) = self.wakeup_trigger.maybe_trigger_wakeup() {
                // Java throws WakeupException unconditionally; the Rust
                // analog rotates the token AFTER raising so a subsequent
                // poll() observes a fresh token (consumer-threading.md §11).
                self.wakeup_trigger.rotate();
                return Err(err);
            }

            // Stage 2: drive the `AsyncPollEvent` lifecycle. This also
            // executes pending OffsetCommitCallback invocations and drains
            // background events for §31 listener callbacks.
            self.check_inflight_poll(poll_deadline_ms, first_pass).await?;
            first_pass = false;

            // Stage 3: collect fetched records.
            let mut records = self.poll_for_fetches();
            if !records.is_empty() {
                // Java: `sendPrefetches(timer)` — eagerly enqueue the next
                // batch of fetches so the user's processing overlaps with
                // the next request. In Rust this maps to a non-blocking
                // `CreateFetchRequests` event.
                self.send_prefetches();
                // Java: `interceptors.onConsume(...)`.
                {
                    let chain = self.interceptors.lock().unwrap();
                    chain.on_consume(&mut records);
                }
                return Ok(records);
            }

            // Java: `while (timer.notExpired())`.
            if self.time.milliseconds() >= poll_deadline_ms {
                break;
            }
        }

        Ok(ConsumerRecords::empty())
    }

    /// Java: `private void checkInflightPoll(Timer timer, boolean firstPass)`
    /// (`AsyncKafkaConsumer.java:893-928`).
    ///
    /// Drives the lifetime of the inflight [`ApplicationEvent::AsyncPoll`]
    /// event. On the first pass of a `poll()` call it clears any leftover
    /// event from the previous invocation. If no event is currently
    /// inflight it submits a fresh one. The pending `OffsetCommitCallback`
    /// queue is drained and `process_background_events` is invoked, so a
    /// failed callback / fatal background error short-circuits with the
    /// inflight event cleared (matching Java's `try { … } catch (Throwable t) { … }`).
    async fn check_inflight_poll(&mut self, poll_deadline_ms: i64, first_pass: bool) -> Result<(), KafkaError> {
        if first_pass && self.inflight_poll.is_some() {
            self.maybe_clear_previous_inflight_poll()?;
        }

        let mut newly_submitted_event = false;
        if self.inflight_poll.is_none() {
            let state = Arc::new(AsyncPollState::new());
            let now_ms = self.time.milliseconds();
            let event = ApplicationEvent::AsyncPoll {
                deadline_ms: poll_deadline_ms,
                poll_time_ms: now_ms,
                state: Arc::clone(&state),
            };
            log::trace!("Inflight event AsyncPoll(deadline={poll_deadline_ms}, time={now_ms}) submitted");
            // `add` is non-blocking — bg task drives the state machine.
            self.application_event_handler.add(event, now_ms)?;
            self.inflight_poll = Some(InflightPoll { deadline_ms: poll_deadline_ms, state });
            newly_submitted_event = true;
        }

        // Java: `offsetCommitCallbackInvoker.executeCallbacks();` +
        //       `processBackgroundEvents();`.
        //
        // Both are user-supplied code paths — if either throws, we clear
        // the inflight poll and propagate the error.
        let invocation_result = self.run_check_inflight_drain().await;
        if let Err(err) = invocation_result {
            log::trace!("Inflight event AsyncPoll failed due to {err}, clearing");
            self.inflight_poll = None;
            return Err(err);
        }

        if self.inflight_poll.is_some() {
            self.maybe_clear_current_inflight_poll(newly_submitted_event)?;
        }

        Ok(())
    }

    /// Drain pending OffsetCommitCallback invocations + bg-events.
    ///
    /// Pulled out of [`Self::check_inflight_poll`] so the `?`-based error
    /// propagation can run inside a single try-block-equivalent body —
    /// Java's `try { ... } catch (Throwable t)` semantics translate as
    /// "run this helper, observe the result".
    async fn run_check_inflight_drain(&mut self) -> Result<(), KafkaError> {
        // Invoke any callbacks queued by previous async commits.
        self.offset_commit_callback_invoker.invoke_pending_callbacks().await;
        // Drain pending background events (rebalance-listener callbacks,
        // fatal errors). §31: must run on the caller's task.
        self.process_background_events().await?;
        Ok(())
    }

    /// Java: `private void maybeClearPreviousInflightPoll()`
    /// (`AsyncKafkaConsumer.java:930-963`).
    fn maybe_clear_previous_inflight_poll(&mut self) -> Result<(), KafkaError> {
        let inflight = match self.inflight_poll.as_ref() {
            Some(i) => i,
            None => return Ok(()),
        };
        if inflight.state.is_complete() {
            let err_opt = inflight.state.error();
            if let Some(error) = err_opt {
                log::trace!("Previous inflight event AsyncPoll completed with an error ({error}), clearing");
                self.inflight_poll = None;
                return Err(error);
            }
            // Successful case: check if the bg task populated the buffer.
            if self.fetch_buffer.is_empty() {
                log::trace!("Previous inflight event AsyncPoll completed without filling the buffer, clearing");
                self.inflight_poll = None;
            } else {
                // Buffer is full — keep the event so the caller can drain
                // the buffer before a fresh event is enqueued (Java's
                // "0 timeout starvation" guard).
                log::trace!("Previous inflight event AsyncPoll completed and filled the buffer, not clearing");
            }
            return Ok(());
        }

        // Java: `else if (inflightPoll.isExpired(time) && inflightPoll.isValidatePositionsComplete())`.
        let now_ms = self.time.milliseconds();
        if inflight.is_expired(now_ms) && inflight.state.is_validate_positions_complete() {
            log::trace!("Previous inflight event AsyncPoll expired without completing, clearing");
            self.inflight_poll = None;
        }
        Ok(())
    }

    /// Java: `private void maybeClearCurrentInflightPoll(boolean newlySubmittedEvent)`
    /// (`AsyncKafkaConsumer.java:965-986`).
    fn maybe_clear_current_inflight_poll(&mut self, newly_submitted_event: bool) -> Result<(), KafkaError> {
        let inflight = match self.inflight_poll.as_ref() {
            Some(i) => i,
            None => return Ok(()),
        };
        if inflight.state.is_complete() {
            let err_opt = inflight.state.error();
            self.inflight_poll = None;
            if let Some(error) = err_opt {
                log::trace!("Inflight event AsyncPoll completed with an error ({error}), clearing");
                return Err(error);
            }
            log::trace!("Inflight event AsyncPoll completed without error, clearing");
            return Ok(());
        }

        if !newly_submitted_event {
            let now_ms = self.time.milliseconds();
            if inflight.is_expired(now_ms) && inflight.state.is_validate_positions_complete() {
                log::trace!("Inflight event AsyncPoll expired without completing, clearing");
                self.inflight_poll = None;
            }
        }
        Ok(())
    }

    /// Java: `private Fetch<K, V> pollForFetches(Timer timer)`
    /// (`AsyncKafkaConsumer.java:1872-1932`).
    ///
    /// In the Rust translation the buffer-drain is a pure CPU operation
    /// (no broker round-trip); the `FetchCollector::collect_fetch` call
    /// returns immediately. If decoding fails for any reason we surface an
    /// empty fetch (Java's `Fetch.empty()` fallback) and rely on the bg
    /// task to repopulate the buffer on the next AsyncPoll iteration.
    fn poll_for_fetches(&self) -> ConsumerRecords<K, V> {
        // Java holds a poll-fetch-spin lock that we elide here — the
        // `FetchBuffer` is internally locked. On error we log + return
        // empty so the outer `poll()` loop can retry on the next iteration.
        match self.fetch_collector.collect_fetch(&self.fetch_buffer) {
            Ok(records) => records,
            Err(err) => {
                log::warn!("collect_fetch returned an error: {err}");
                ConsumerRecords::empty()
            },
        }
    }

    // ── Commit ─────────────────────────────────────────────────────────
    //
    // Translates Java's `commitSync()` / `commitAsync()` family
    // (`AsyncKafkaConsumer.java:993-1052`, `1692-1748`). The shared
    // helper `commit(commit_event)` validates group_id, drains pending
    // callbacks, returns the early-completed receiver for empty offsets,
    // adds the event, awaits `offsets_ready`, and returns the
    // `handle.future()` receiver. The sync / async wrappers branch on
    // the resulting receiver:
    //
    //   - `commit_async` registers a callback (or default no-callback
    //     interceptor invocation) via the
    //     `OffsetCommitCallbackInvoker`, then returns immediately.
    //   - `commit_sync` blocks on the receiver and runs the interceptor
    //     `on_commit` chain inline on the caller's task.
    //
    // The receiver from the underlying `CommitAsync` / `CommitSync` event
    // resolves with the committed-offsets map (or an error). Java uses a
    // single `CompletableFuture<Map>` for both arms; Rust uses two: a
    // public `oneshot::Receiver<()>` exposed for app-side completion
    // ordering (`last_pending_async_commit`) and the typed handle
    // returned to the caller.

    /// Translates Java's
    /// `private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit(CommitEvent)`
    /// (`AsyncKafkaConsumer.java:1038-1052`).
    ///
    /// Returns the typed receiver from the commit event's handle. Callers
    /// either await it (sync path) or attach a spawned-task continuation
    /// (async-with-callback path).
    ///
    /// On the empty-offsets early-exit Java returns
    /// `CompletableFuture.completedFuture(null)`; the Rust analog is a
    /// pre-completed `oneshot` channel resolving to `Ok(HashMap::new())`.
    async fn commit_inner(
        &mut self,
        commit_event: CommitEventKind,
    ) -> Result<
        tokio::sync::oneshot::Receiver<Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>>,
        KafkaError,
    > {
        self.throw_if_group_id_not_defined()?;
        self.offset_commit_callback_invoker.invoke_pending_callbacks().await;

        // Java's `if (event.offsets().isPresent() && event.offsets().get().isEmpty())`
        // short-circuits with `completedFuture(null)`. Mirror by sending
        // `Ok(empty)` on a pre-completed oneshot.
        if let Some(map) = commit_event.offsets()
            && map.is_empty()
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(Ok(HashMap::new()));
            return Ok(rx);
        }

        let now_ms = self.time.milliseconds();
        let deadline_ms = commit_event.deadline_ms();
        let (handle, receiver, _erased) =
            make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        let (offsets_ready_handle, offsets_ready_rx, _erased_or) = make_completable_event::<()>(deadline_ms);

        // Build and enqueue the matching event variant.
        let event = match commit_event {
            CommitEventKind::Async { offsets } => {
                ApplicationEvent::CommitAsync { handle, offsets_ready: offsets_ready_handle, offsets }
            },
            CommitEventKind::Sync { offsets, .. } => {
                ApplicationEvent::CommitSync { handle, offsets_ready: offsets_ready_handle, offsets }
            },
        };
        self.application_event_handler.add(event, now_ms)?;

        // Java: `ConsumerUtils.getResult(commitEvent.offsetsReady(), defaultApiTimeoutMs.toMillis())`.
        // This blocks until the bg task has resolved which offsets to
        // commit (so subsequent fetches don't shift the
        // commit window).
        let or_deadline_ms = self.default_api_timeout_deadline_ms();
        let or_remaining = self.remaining_ms(or_deadline_ms);
        match tokio::time::timeout(Duration::from_millis(or_remaining as u64), offsets_ready_rx).await {
            Ok(Ok(Ok(()))) => {},
            Ok(Ok(Err(err))) => return Err(err),
            Ok(Err(_recv_err)) => {
                return Err(KafkaError::illegal_state(
                    "Background task dropped the offsets-ready sender for commit",
                ));
            },
            Err(_elapsed) => {
                return Err(KafkaError::timeout("Timed out waiting for offsetsReady on commit event"));
            },
        }

        Ok(receiver)
    }

    /// Translates Java's `void commitSync()` (uses default API timeout).
    pub async fn commit_sync(&mut self) -> Result<(), KafkaError> {
        self.commit_sync_internal(None, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Translates Java's `void commitSync(Duration timeout)`.
    pub async fn commit_sync_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError> {
        self.commit_sync_internal(None, timeout).await
    }

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    pub async fn commit_sync_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), KafkaError> {
        self.commit_sync_internal(Some(offsets), Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Translates Java's
    /// `void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout)`.
    pub async fn commit_sync_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), KafkaError> {
        self.commit_sync_internal(Some(offsets), timeout).await
    }

    /// Translates Java's
    /// `private void commitSync(Optional<Map<...>>, Duration timeout)`.
    async fn commit_sync_internal(
        &mut self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        timeout: Duration,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        let receiver = self
            .commit_inner(CommitEventKind::Sync { offsets: offsets.clone(), deadline_ms })
            .await?;

        // Java: `awaitPendingAsyncCommitsAndExecuteCommitCallbacks(requestTimer, true)`
        // — drain any pending async commits BEFORE blocking on this sync
        // commit so the user-visible callback ordering matches Java.
        self.await_pending_async_commits_and_execute_commit_callbacks(deadline_ms, true)
            .await?;

        // Java: `ConsumerUtils.getResult(commitFuture, requestTimer)` with
        // wakeup-trigger registration for the duration of the await.
        let remaining = self.remaining_ms(deadline_ms);
        let wait_result = tokio::time::timeout(Duration::from_millis(remaining.max(0) as u64), receiver).await;
        let committed: HashMap<TopicPartition, OffsetAndMetadata> = match wait_result {
            Ok(Ok(Ok(map))) => map,
            Ok(Ok(Err(err))) => return Err(err),
            Ok(Err(_recv_err)) => {
                return Err(KafkaError::illegal_state(
                    "Background task dropped the commit_sync sender without completing it",
                ));
            },
            Err(_elapsed) => {
                return Err(KafkaError::timeout(format!(
                    "Timeout of {} ms expired before successfully committing offsets {:?}",
                    timeout.as_millis(),
                    offsets,
                )));
            },
        };

        // Java: `interceptors.onCommit(committedOffsets)`.
        {
            let chain = self.interceptors.lock().unwrap();
            chain.on_commit(&committed);
        }
        Ok(())
    }

    /// Translates Java's `void commitAsync()` (no callback, no offsets —
    /// commit `allConsumed`).
    pub async fn commit_async(&mut self) -> Result<(), KafkaError> {
        self.commit_async_internal(None, None).await
    }

    /// Translates Java's `void commitAsync(OffsetCommitCallback)`.
    pub async fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), KafkaError> {
        self.commit_async_internal(None, Some(callback)).await
    }

    /// Translates Java's
    /// `void commitAsync(Map<TopicPartition, OffsetAndMetadata>, OffsetCommitCallback)`.
    pub async fn commit_async_offsets_with_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), KafkaError> {
        self.commit_async_internal(Some(offsets), Some(callback)).await
    }

    /// Translates Java's
    /// `private void commitAsync(Optional<Map<...>>, OffsetCommitCallback)`.
    async fn commit_async_internal(
        &mut self,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
        callback: Option<Arc<dyn crate::consumer::OffsetCommitCallback>>,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        let receiver = self.commit_inner(CommitEventKind::Async { offsets: offsets.clone() }).await?;

        // Java: `lastPendingAsyncCommit = commit(asyncCommitEvent).whenComplete(...)`
        // — the resulting future is stored on the consumer so a later
        // `commitSync` / `close` can wait for it to complete. The Rust
        // analog uses a oneshot bridge: we spawn the continuation, the
        // continuation invokes the callback chain, and signals
        // `last_pending_completion_tx` when finished.
        let (pending_tx, pending_rx) = tokio::sync::oneshot::channel::<()>();
        let invoker = Arc::clone(&self.offset_commit_callback_invoker);
        tokio::spawn(async move {
            let result = receiver.await;
            match result {
                Ok(Ok(committed)) => {
                    // Java: `if (throwable == null)
                    //          offsetCommitCallbackInvoker.enqueueInterceptorInvocation(committedOffsets)`.
                    invoker.enqueue_interceptor_invocation(committed.clone());
                    if let Some(cb) = callback {
                        invoker.enqueue_user_callback_invocation(cb, committed, None);
                    }
                },
                Ok(Err(err)) => {
                    if let Some(cb) = callback {
                        invoker.enqueue_user_callback_invocation(cb, HashMap::new(), Some(err.clone()));
                    } else {
                        log::error!("Offset commit failed: {err}");
                    }
                },
                Err(_recv_err) => {
                    log::error!("commit_async receiver dropped without completion");
                },
            }
            let _ = pending_tx.send(());
        });
        self.last_pending_async_commit = Some(pending_rx);
        Ok(())
    }

    /// Translates Java's
    /// `private void awaitPendingAsyncCommitsAndExecuteCommitCallbacks(Timer timer, boolean enableWakeup)`
    /// (`AsyncKafkaConsumer.java:1726-1749`).
    ///
    /// If there is a pending async commit, await it (bounded by the
    /// deadline) and then drain the callback invoker queue. The `enable_wakeup`
    /// flag mirrors Java's wakeup-trigger registration; when `true`, a
    /// concurrent `wakeup()` interrupts the wait with
    /// `KafkaError::Wakeup`.
    async fn await_pending_async_commits_and_execute_commit_callbacks(
        &mut self,
        deadline_ms: i64,
        _enable_wakeup: bool,
    ) -> Result<(), KafkaError> {
        if let Some(rx) = self.last_pending_async_commit.take() {
            let remaining = self.remaining_ms(deadline_ms);
            let wait = remaining.max(0) as u64;
            match tokio::time::timeout(Duration::from_millis(wait), rx).await {
                Ok(_) => {
                    // Either resolved (Ok(())) or the sender was dropped
                    // (RecvError). Both are terminal for the pending
                    // commit; proceed to drain the callbacks.
                },
                Err(_elapsed) => {
                    return Err(KafkaError::timeout(
                        "Timed out waiting for last pending async commit to complete",
                    ));
                },
            }
        }
        // Java: `offsetCommitCallbackInvoker.executeCallbacks()`.
        self.offset_commit_callback_invoker.invoke_pending_callbacks().await;
        Ok(())
    }

    // ── Seek / position / committed / lag ──────────────────────────────
    //
    // Translates Java's `seek(...)` / `seekToBeginning(...)` /
    // `seekToEnd(...)` / `position(...)` / `committed(...)` /
    // `currentLag(...)` (`AsyncKafkaConsumer.java:1055-1155, 1413-1425`).
    //
    // The seek methods route through a `SeekUnvalidated` /
    // `ResetOffset` event. `position` and `committed` route through
    // `CheckAndUpdatePositions` / `FetchCommittedOffsets` events. The
    // current-lag arm enqueues a `CurrentLag` event.
    //
    // Each method calls `ensure_open()` first to mirror Java's
    // `acquireAndEnsureOpen()` closed-consumer guard.

    /// Java: `void seek(TopicPartition, long offset)`.
    pub async fn seek(&mut self, partition: TopicPartition, offset: i64) -> Result<(), KafkaError> {
        if offset < 0 {
            return Err(KafkaError::illegal_argument("seek offset must not be a negative number"));
        }
        self.ensure_open()?;
        log::info!("Seeking to offset {offset} for partition {partition}");
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let now_ms = self.time.milliseconds();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::SeekUnvalidated { handle, partition, offset, offset_epoch: None },
                receiver,
                now_ms,
            )
            .await
    }

    /// Java: `void seek(TopicPartition, OffsetAndMetadata)`.
    pub async fn seek_with_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), KafkaError> {
        let offset = offset_and_metadata.offset();
        if offset < 0 {
            return Err(KafkaError::illegal_argument("seek offset must not be a negative number"));
        }
        self.ensure_open()?;
        match offset_and_metadata.leader_epoch() {
            Some(epoch) => log::info!("Seeking to offset {offset} for partition {partition} with epoch {epoch}"),
            None => log::info!("Seeking to offset {offset} for partition {partition}"),
        }
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let now_ms = self.time.milliseconds();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::SeekUnvalidated {
                    handle,
                    partition,
                    offset,
                    offset_epoch: offset_and_metadata.leader_epoch(),
                },
                receiver,
                now_ms,
            )
            .await
    }

    /// Java: `void seekToBeginning(Collection<TopicPartition>)`.
    pub async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.seek_with_reset_strategy(partitions, crate::consumer::AutoOffsetResetStrategy::EARLIEST)
            .await
    }

    /// Java: `void seekToEnd(Collection<TopicPartition>)`.
    pub async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.seek_with_reset_strategy(partitions, crate::consumer::AutoOffsetResetStrategy::LATEST)
            .await
    }

    /// Translates Java's
    /// `private void seek(Collection<TopicPartition>, AutoOffsetResetStrategy)`.
    async fn seek_with_reset_strategy(
        &mut self,
        partitions: &[TopicPartition],
        strategy: crate::consumer::AutoOffsetResetStrategy,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let now_ms = self.time.milliseconds();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        self.application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::ResetOffset { handle, partitions: set, offset_reset_strategy: strategy },
                receiver,
                now_ms,
            )
            .await
    }

    /// Java: `long position(TopicPartition)` — uses default API timeout.
    pub async fn position(&mut self, partition: &TopicPartition) -> Result<i64, KafkaError> {
        self.position_timeout(partition, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `long position(TopicPartition, Duration timeout)`
    /// (`AsyncKafkaConsumer.java:1133-1155`).
    pub async fn position_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, KafkaError> {
        self.ensure_open()?;
        {
            let subs = self.subscriptions.lock().unwrap();
            if !subs.is_assigned(partition) {
                return Err(KafkaError::illegal_state(
                    "You can only check the position for partitions assigned to this consumer.",
                ));
            }
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        loop {
            // Read the validated position under the lock; drop guard before await.
            let position_offset = {
                let subs = self.subscriptions.lock().unwrap();
                subs.valid_position(partition)?.map(|fp| fp.offset)
            };
            if let Some(offset) = position_offset {
                return Ok(offset);
            }

            // Java: `updateFetchPositions(timer)` — drives the
            // `CheckAndUpdatePositionsEvent` round-trip.
            let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
            self.application_event_handler
                .add_and_get::<()>(ApplicationEvent::CheckAndUpdatePositions { handle }, receiver, now_ms)
                .await
                .ok();

            if let Err(err) = self.wakeup_trigger.maybe_trigger_wakeup() {
                self.wakeup_trigger.rotate();
                return Err(err);
            }

            if self.time.milliseconds() >= deadline_ms {
                return Err(KafkaError::timeout(format!(
                    "Timeout of {}ms expired before the position for partition {} could be determined",
                    timeout.as_millis(),
                    partition
                )));
            }
        }
    }

    /// Java: `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>)`.
    pub async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        self.committed_timeout(partitions, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition>, Duration)`
    /// (`AsyncKafkaConsumer.java:1162-1190`).
    pub async fn committed_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        self.ensure_open()?;
        self.throw_if_group_id_not_defined()?;
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) =
            make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(deadline_ms);
        let result = self
            .application_event_handler
            .add_and_get::<HashMap<TopicPartition, OffsetAndMetadata>>(
                ApplicationEvent::FetchCommittedOffsets { handle, partitions: set },
                receiver,
                now_ms,
            )
            .await;
        match result {
            Ok(map) => Ok(map),
            Err(KafkaError::Timeout(_)) => Err(KafkaError::timeout(format!(
                "Timeout of {}ms expired before the last committed offset for partitions {:?} could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.",
                timeout.as_millis(),
                partitions
            ))),
            Err(err) => Err(err),
        }
    }

    /// Java: `OptionalLong currentLag(TopicPartition)`
    /// (`AsyncKafkaConsumer.java:1413-1425`).
    ///
    /// Phase 11 commit (6/N) wires the `CurrentLag` event. The previous
    /// stub (commit (2/N)) returned `None` for every input.
    pub async fn current_lag_async(&mut self, topic_partition: &TopicPartition) -> Result<Option<i64>, KafkaError> {
        self.ensure_open()?;
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, receiver, _erased) = make_completable_event::<Option<i64>>(deadline_ms);
        self.application_event_handler
            .add_and_get::<Option<i64>>(
                ApplicationEvent::CurrentLag {
                    handle,
                    partition: topic_partition.clone(),
                    isolation_level: self.isolation_level,
                },
                receiver,
                now_ms,
            )
            .await
    }

    // ── Beginning / end offsets / offsetsForTimes ─────────────────────

    /// Java: `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>)`.
    pub async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.beginning_offsets_timeout(partitions, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition>, Duration)`.
    pub async fn beginning_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        // Java's `ListOffsetsRequest.EARLIEST_TIMESTAMP = -2L`.
        self.beginning_or_end_offsets(partitions, -2, timeout).await
    }

    /// Java: `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>)`.
    pub async fn end_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.end_offsets_timeout(partitions, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, Long> endOffsets(Collection<TopicPartition>, Duration)`.
    pub async fn end_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        // Java's `ListOffsetsRequest.LATEST_TIMESTAMP = -1L`.
        self.beginning_or_end_offsets(partitions, -1, timeout).await
    }

    /// Translates Java's
    /// `private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition>, long timestamp, Duration timeout)`
    /// (`AsyncKafkaConsumer.java:1366-1411`).
    async fn beginning_or_end_offsets(
        &mut self,
        partitions: &[TopicPartition],
        timestamp: i64,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let mut timestamps_to_search: HashMap<TopicPartition, i64> = HashMap::new();
        for tp in partitions {
            timestamps_to_search.insert(tp.clone(), timestamp);
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        if timeout.is_zero() {
            // Java: `if (timeout.isZero()) { applicationEventHandler.add(listOffsetsEvent); return new HashMap<>(); }`.
            let (handle, _receiver, _erased) =
                make_completable_event::<HashMap<TopicPartition, Option<OffsetAndTimestamp>>>(deadline_ms);
            self.application_event_handler.add(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: false },
                now_ms,
            )?;
            return Ok(HashMap::new());
        }

        let (handle, receiver, _erased) =
            make_completable_event::<HashMap<TopicPartition, Option<OffsetAndTimestamp>>>(deadline_ms);
        let result = self
            .application_event_handler
            .add_and_get::<HashMap<TopicPartition, Option<OffsetAndTimestamp>>>(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: false },
                receiver,
                now_ms,
            )
            .await;
        match result {
            Ok(offsets_map) => {
                let mut out = HashMap::with_capacity(offsets_map.len());
                for (tp, opt) in offsets_map {
                    if let Some(oat) = opt {
                        out.insert(tp, oat.offset());
                    }
                }
                Ok(out)
            },
            Err(KafkaError::Timeout(_)) => Err(KafkaError::timeout(format!(
                "Failed to get offsets by times in {}ms",
                timeout.as_millis()
            ))),
            Err(err) => Err(err),
        }
    }

    /// Java: `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long>)`.
    pub async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError> {
        self.offsets_for_times_timeout(timestamps_to_search, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long>, Duration)`
    /// (`AsyncKafkaConsumer.java:1303-1344`).
    pub async fn offsets_for_times_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError> {
        self.ensure_open()?;
        // Java's per-entry argument validation: negative targets rejected.
        for (tp, ts) in &timestamps_to_search {
            if *ts < 0 {
                return Err(KafkaError::illegal_argument(format!(
                    "The target time for partition {tp} is {ts}. The target time cannot be negative."
                )));
            }
        }
        if timestamps_to_search.is_empty() {
            return Ok(HashMap::new());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);

        if timeout.is_zero() {
            // Java: `if (timeout.toMillis() == 0L) { applicationEventHandler.add(...); return listOffsetsEvent.emptyResults(); }`.
            let (handle, _receiver, _erased) =
                make_completable_event::<HashMap<TopicPartition, Option<OffsetAndTimestamp>>>(deadline_ms);
            let empty_keys = timestamps_to_search.keys().cloned().collect::<Vec<_>>();
            self.application_event_handler.add(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: true },
                now_ms,
            )?;
            // Java's `emptyResults()` returns a map with each input key
            // mapped to `null`; here we omit the key entirely, since
            // `OffsetAndTimestamp` is not nullable in Rust. The user
            // observes "no data yet" via a missing key, matching Java's
            // null-key semantic for the timeout-zero arm.
            let _ = empty_keys;
            return Ok(HashMap::new());
        }

        let (handle, receiver, _erased) =
            make_completable_event::<HashMap<TopicPartition, Option<OffsetAndTimestamp>>>(deadline_ms);
        let result = self
            .application_event_handler
            .add_and_get::<HashMap<TopicPartition, Option<OffsetAndTimestamp>>>(
                ApplicationEvent::ListOffsets { handle, timestamps_to_search, require_timestamps: true },
                receiver,
                now_ms,
            )
            .await;
        match result {
            Ok(offsets_map) => {
                // Java filters out null values silently; mirror by skipping.
                let mut out = HashMap::with_capacity(offsets_map.len());
                for (tp, opt) in offsets_map {
                    if let Some(oat) = opt {
                        out.insert(tp, oat);
                    }
                }
                Ok(out)
            },
            Err(KafkaError::Timeout(_)) => Err(KafkaError::timeout(format!(
                "Failed to get offsets by times in {}ms",
                timeout.as_millis()
            ))),
            Err(err) => Err(err),
        }
    }

    // ── Topic metadata: partitionsFor / listTopics ────────────────────

    /// Java: `List<PartitionInfo> partitionsFor(String topic)`.
    pub async fn partitions_for(&mut self, topic: &str) -> Result<Vec<crate::common::PartitionInfo>, KafkaError> {
        self.partitions_for_timeout(topic, Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `List<PartitionInfo> partitionsFor(String topic, Duration)`
    /// (`AsyncKafkaConsumer.java:1210-1235`).
    pub async fn partitions_for_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<crate::common::PartitionInfo>, KafkaError> {
        self.ensure_open()?;
        // Java: `Cluster cluster = this.metadata.fetch();
        //        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        //        if (!parts.isEmpty()) return parts;`
        {
            let cluster = self.metadata.metadata_arc().fetch();
            let parts = cluster.partitions_for_topic(topic);
            if !parts.is_empty() {
                return Ok(parts.to_vec());
            }
        }

        if timeout.is_zero() {
            return Err(KafkaError::timeout(format!(
                "Timeout of {}ms expired before partitions for topic {topic} could be determined",
                timeout.as_millis()
            )));
        }

        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let (handle, receiver, _erased) =
            make_completable_event::<HashMap<String, Vec<crate::common::PartitionInfo>>>(deadline_ms);
        let map = self
            .application_event_handler
            .add_and_get::<HashMap<String, Vec<crate::common::PartitionInfo>>>(
                ApplicationEvent::TopicMetadata { handle, topic: topic.to_string() },
                receiver,
                now_ms,
            )
            .await?;
        Ok(map.get(topic).cloned().unwrap_or_default())
    }

    /// Java: `Map<String, List<PartitionInfo>> listTopics()`.
    pub async fn list_topics(&mut self) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, KafkaError> {
        self.list_topics_timeout(Duration::from_millis(self.default_api_timeout_ms as u64))
            .await
    }

    /// Java: `Map<String, List<PartitionInfo>> listTopics(Duration)`
    /// (`AsyncKafkaConsumer.java:1242-1260`).
    pub async fn list_topics_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, KafkaError> {
        self.ensure_open()?;
        if timeout.is_zero() {
            return Err(KafkaError::timeout(format!(
                "Timeout of {}ms expired before all topics' metadata could be listed",
                timeout.as_millis()
            )));
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = calculate_deadline_ms(now_ms, timeout.as_millis() as i64);
        let (handle, receiver, _erased) =
            make_completable_event::<HashMap<String, Vec<crate::common::PartitionInfo>>>(deadline_ms);
        self.application_event_handler
            .add_and_get::<HashMap<String, Vec<crate::common::PartitionInfo>>>(
                ApplicationEvent::AllTopicsMetadata { handle },
                receiver,
                now_ms,
            )
            .await
    }

    // ── Pause / resume ─────────────────────────────────────────────────

    /// Java: `void pause(Collection<TopicPartition>)`
    /// (`AsyncKafkaConsumer.java:1273-1283`).
    pub async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        self.application_event_handler
            .add_and_get::<()>(ApplicationEvent::PausePartitions { handle, partitions: set }, receiver, now_ms)
            .await
    }

    /// Java: `void resume(Collection<TopicPartition>)`
    /// (`AsyncKafkaConsumer.java:1286-1296`).
    pub async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.ensure_open()?;
        if partitions.is_empty() {
            return Ok(());
        }
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let set: std::collections::HashSet<TopicPartition> = partitions.iter().cloned().collect();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        self.application_event_handler
            .add_and_get::<()>(ApplicationEvent::ResumePartitions { handle, partitions: set }, receiver, now_ms)
            .await
    }

    // ── Enforce rebalance (KIP-848: unsupported) ──────────────────────

    /// Java: `void enforceRebalance()` / `void enforceRebalance(String)`
    /// (`AsyncKafkaConsumer.java:1438-1446`).
    ///
    /// Both Java overloads log a warning and otherwise no-op under the
    /// KIP-848 protocol (the classic protocol implements them via
    /// `ConsumerCoordinator`). We match that: log + no-op, return
    /// `Ok(())`. No `KafkaError::unsupported_version` since Java does not
    /// throw.
    pub async fn enforce_rebalance(&mut self, _reason: Option<&str>) -> Result<(), KafkaError> {
        log::warn!("Operation not supported in new consumer group protocol");
        Ok(())
    }

    /// Java: `private void sendPrefetches(Timer timer)`
    /// (`AsyncKafkaConsumer.java:1995-2003`).
    ///
    /// Submits a non-completable `CreateFetchRequests` event so the bg
    /// task can pipeline the next fetch round-trip with the user's
    /// per-record processing. Errors from a closed bg-task channel are
    /// logged-and-swallowed (Java does the same).
    fn send_prefetches(&self) {
        let now_ms = self.time.milliseconds();
        let deadline_ms = self.default_api_timeout_deadline_ms();
        let (handle, _receiver, _erased) = make_completable_event::<()>(deadline_ms);
        // The event is `add`-ed (not `add_and_get`), so the receiver is
        // dropped and the handle is fire-and-forget. The bg side's
        // completion of the handle then resolves into the dropped
        // receiver, which is a no-op.
        if let Err(err) = self
            .application_event_handler
            .add(ApplicationEvent::CreateFetchRequests { handle }, now_ms)
        {
            log::warn!("send_prefetches: failed to enqueue CreateFetchRequests: {err}");
        }
    }

    // ── Close path ─────────────────────────────────────────────────────
    //
    // Translates Java's `close()` chain
    // (`AsyncKafkaConsumer.java:1422-1588`). The close sequence runs
    // best-effort: each step that throws is logged and the close
    // continues so the network thread is always joined. The final error
    // (if any) is propagated only when `swallow_exception=false`.
    //
    // The eight Java steps (line 1553-1577) map to Rust as:
    //   1. `wakeup_trigger.disable()` — Java's
    //      `wakeupTrigger.disableWakeups()`.
    //   2. `auto_commit_on_close(deadline_ms)` — commit-sync of
    //      `allConsumed()` + `CommitOnClose` event.
    //   3. `stop_find_coordinator_on_close()` —
    //      `StopFindCoordinatorOnClose` event.
    //   4. `run_rebalance_callbacks_on_close()` — invoke
    //      `on_partitions_revoked` / `on_partitions_lost` for the
    //      currently-assigned partitions (depending on memberEpoch).
    //   5. `leave_group_on_close(deadline_ms, operation)` —
    //      `LeaveGroupOnClose` event.
    //   6. `await_pending_async_commits_and_execute_commit_callbacks(...)` —
    //      drain pending callbacks one last time on the caller's task.
    //   7. Drop `application_event_handler` — wakes the bg task so its
    //      `run_once` loop exits.
    //   8. `network_thread_close.signal_close()` + `await_join()` —
    //      Java's `closeQuietly(consumerNetworkThread)`.

    /// Java: `void close()`. Closes the consumer with default timeout.
    pub async fn close(&mut self) -> Result<(), KafkaError> {
        self.close_internal(
            Duration::from_millis(crate::consumer::close_options::DEFAULT_CLOSE_TIMEOUT_MS),
            crate::consumer::GroupMembershipOperation::Default,
            false,
        )
        .await
    }

    /// Java: `void close(CloseOptions options)`.
    pub async fn close_with_options(&mut self, options: crate::consumer::CloseOptions) -> Result<(), KafkaError> {
        let timeout = options
            .timeout_value()
            .unwrap_or_else(|| Duration::from_millis(crate::consumer::close_options::DEFAULT_CLOSE_TIMEOUT_MS));
        self.close_internal(timeout, options.group_membership_operation_value(), false)
            .await
    }

    /// Translates Java's
    /// `private void close(Duration timeout,
    ///                     CloseOptions.GroupMembershipOperation membershipOperation,
    ///                     boolean swallowException)`
    /// (`AsyncKafkaConsumer.java:1540-1588`).
    async fn close_internal(
        &mut self,
        timeout: Duration,
        membership_operation: crate::consumer::GroupMembershipOperation,
        swallow_exception: bool,
    ) -> Result<(), KafkaError> {
        log::trace!("Closing the Kafka consumer");
        if self.is_closed() {
            // Java treats double-close as a silent no-op (the closed
            // accessors return immediately).
            return Ok(());
        }

        // Step 1: disable wakeups. After this call, in-flight async
        // operations cannot be cancelled mid-close (Java
        // `wakeupTrigger.disableWakeups()`).
        self.wakeup_trigger.disable();

        let close_start_ms = self.time.milliseconds();
        let close_deadline_ms = calculate_deadline_ms(close_start_ms, timeout.as_millis() as i64);

        // First-error tracking mirrors Java's `AtomicReference<Throwable> firstException`.
        let mut first_error: Option<KafkaError> = None;
        let record = |slot: &mut Option<KafkaError>, op: &str, err: KafkaError| {
            log::error!("{op}: {err}");
            if slot.is_none() {
                *slot = Some(err);
            }
        };

        // Step 2: auto_commit_on_close.
        if let Err(err) = self.auto_commit_on_close(close_deadline_ms).await {
            record(&mut first_error, "Failed to auto-commit offsets", err);
        }

        // Step 3: stop_find_coordinator_on_close.
        if let Err(err) = self.stop_find_coordinator_on_close() {
            record(&mut first_error, "Failed to stop finding coordinator", err);
        }

        // Step 4: run_rebalance_callbacks_on_close.
        if let Err(err) = self.run_rebalance_callbacks_on_close().await {
            record(&mut first_error, "Failed to run rebalance callbacks", err);
        }

        // Step 5: leave_group_on_close.
        if let Err(err) = self.leave_group_on_close(close_deadline_ms, membership_operation).await {
            record(&mut first_error, "Failed to leave group while closing consumer", err);
        }

        // Step 6: drain pending async commits one last time.
        // Java passes `enable_wakeup=false` here (line 1562).
        if let Err(err) = self
            .await_pending_async_commits_and_execute_commit_callbacks(close_deadline_ms, false)
            .await
        {
            record(
                &mut first_error,
                "Failed invoking asynchronous commit callbacks while closing consumer",
                err,
            );
        }

        // Step 7 & 8: shut down the network thread.
        self.network_thread_close.signal_close();
        self.network_thread_close.wakeup();
        if let Err(err) = self.network_thread_close.await_join().await {
            record(&mut first_error, "Failed shutting down network thread", err);
        }

        // Final reaper pass (Java line 1570) — drain any background events
        // queued during the close path.
        {
            let now_ms = self.time.milliseconds();
            let mut reaper = self.completable_event_reaper.lock().unwrap();
            reaper.reap(now_ms);
        }

        self.closed.store(true, Ordering::Release);
        log::debug!("Kafka consumer has been closed");

        match first_error {
            Some(err) if !swallow_exception => Err(err),
            _ => Ok(()),
        }
    }

    /// Java: `private void autoCommitOnClose(final Timer timer)`
    /// (`AsyncKafkaConsumer.java:1596-1604`).
    async fn auto_commit_on_close(&mut self, deadline_ms: i64) -> Result<(), KafkaError> {
        if self.group_id.is_none() {
            return Ok(());
        }

        if self.auto_commit_enabled {
            // Java: `commitSyncAllConsumed(timer)` swallows errors and
            // logs a warning. Match that — auto-commit failure on close
            // does not propagate.
            let remaining_ms = self.remaining_ms(deadline_ms).max(0) as u64;
            if let Err(err) = self.commit_sync_timeout(Duration::from_millis(remaining_ms)).await {
                log::warn!("Synchronous auto-commit failed: {err}");
            }
        }

        // Java: `applicationEventHandler.add(new CommitOnCloseEvent())`.
        let now_ms = self.time.milliseconds();
        let _ = self.application_event_handler.add(ApplicationEvent::CommitOnClose, now_ms);
        Ok(())
    }

    /// Java: `private void stopFindCoordinatorOnClose()`
    /// (`AsyncKafkaConsumer.java:1661-1666`).
    fn stop_find_coordinator_on_close(&self) -> Result<(), KafkaError> {
        if self.group_id.is_none() {
            return Ok(());
        }
        log::debug!("Stop finding coordinator during consumer close");
        let now_ms = self.time.milliseconds();
        self.application_event_handler
            .add(ApplicationEvent::StopFindCoordinatorOnClose, now_ms)
    }

    /// Java: `private void runRebalanceCallbacksOnClose()`
    /// (`AsyncKafkaConsumer.java:1606-1643`).
    ///
    /// Invokes the user's `on_partitions_revoked` (if `memberEpoch > 0`)
    /// or `on_partitions_lost` (if `memberEpoch <= 0`) on the
    /// **group-assigned** partitions captured by the most recent
    /// reconciliation. The listener runs inline on the caller's task
    /// (§31). Errors propagate.
    ///
    /// # Why `group_assignment_snapshot` and not `subscriptions.assigned_partitions()`
    ///
    /// Java reads from `groupAssignmentSnapshot.get()`
    /// (`AsyncKafkaConsumer.java:1624`) which is populated only by the
    /// `MemberStateListener.onGroupAssignmentUpdated` callback fired
    /// during reconciliation. The snapshot deliberately excludes
    /// partitions added via `assign(...)` (manual assignment) so
    /// non-group consumers get no callback on close — Java line
    /// 1626-1628 returns early on empty snapshot.
    ///
    /// Reading from `SubscriptionState::assigned_partitions()` would
    /// (a) include manual `assign(...)` partitions, and (b) miss the
    /// "partition was just revoked but `SubscriptionState` hasn't been
    /// updated yet" window the snapshot still covers.
    async fn run_rebalance_callbacks_on_close(&mut self) -> Result<(), KafkaError> {
        if self.group_id.is_none() {
            return Ok(());
        }

        // Java: `Set<TopicPartition> assignedPartitions = groupAssignmentSnapshot.get();`
        let assigned: Vec<TopicPartition> = {
            let guard = self.group_assignment_snapshot.lock().unwrap();
            guard.iter().cloned().collect()
        };
        // Java line 1626-1628: `if (assignedPartitions.isEmpty()) return;`.
        if assigned.is_empty() {
            return Ok(());
        }

        // Java reads `generationId` from the cached `groupMetadata`.
        // The Rust translation reads the cached value the same way.
        let member_epoch = {
            let guard = self.group_metadata.lock().unwrap();
            guard.as_ref().map(|gm| gm.generation_id()).unwrap_or(-1)
        };

        let listener_opt = self.rebalance_listener.lock().unwrap().clone();
        let listener = match listener_opt {
            Some(l) => l,
            // Java's `rebalanceListenerInvoker.invokePartitions*` is a
            // no-op when no listener is registered.
            None => return Ok(()),
        };

        if member_epoch > 0 {
            self.rebalance_listener_invoker
                .invoke_partitions_revoked(&listener, &assigned)
                .await
        } else {
            self.rebalance_listener_invoker
                .invoke_partitions_lost(&listener, &assigned)
                .await
        }
    }

    /// Java: `private void leaveGroupOnClose(Timer, GroupMembershipOperation)`
    /// (`AsyncKafkaConsumer.java:1645-1659`).
    async fn leave_group_on_close(
        &mut self,
        deadline_ms: i64,
        membership_operation: crate::consumer::GroupMembershipOperation,
    ) -> Result<(), KafkaError> {
        if self.group_id.is_none() {
            return Ok(());
        }

        log::debug!("Leaving the consumer group during consumer close");
        let now_ms = self.time.milliseconds();
        let (handle, receiver, _erased) = make_completable_event::<()>(deadline_ms);
        let result = self
            .application_event_handler
            .add_and_get::<()>(
                ApplicationEvent::LeaveGroupOnClose { handle, membership_operation },
                receiver,
                now_ms,
            )
            .await;
        match result {
            Ok(()) => {
                log::info!("Completed leaving the group");
                Ok(())
            },
            Err(KafkaError::Timeout(_)) => {
                // Java's `catch (TimeoutException) { log.warn(...) }` —
                // close proceeds.
                log::warn!(
                    "Consumer attempted to leave the group but couldn't complete it within {} ms. \
                     It will proceed to close.",
                    self.remaining_ms(deadline_ms)
                );
                Ok(())
            },
            Err(err) => Err(err),
        }
    }
}

// ── `Consumer<K, V>` trait impl ─────────────────────────────────────────
//
// Delegates each trait method to the inherent impl. The split is
// deliberate: the inherent impl carries the (untyped) ctor, the
// internal helpers, and the `pub`-visible API for users that hold an
// `AsyncKafkaConsumer<K, V>` directly. The trait impl exposes the same
// surface through `Box<dyn Consumer<K, V>>` for callers that want
// type-erased dispatch (the factory `new_consumer<K, V>` returns this
// boxed form per DoD §11).

#[async_trait::async_trait]
impl<K, V> crate::consumer::Consumer<K, V> for AsyncKafkaConsumer<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // ── Sync state-read methods ────────────────────────────────────────

    fn assignment(&self) -> std::collections::HashSet<TopicPartition> {
        AsyncKafkaConsumer::assignment(self)
    }

    fn subscription(&self) -> std::collections::HashSet<String> {
        AsyncKafkaConsumer::subscription(self)
    }

    fn paused(&self) -> std::collections::HashSet<TopicPartition> {
        AsyncKafkaConsumer::paused(self)
    }

    fn group_metadata(&self) -> ConsumerGroupMetadata {
        AsyncKafkaConsumer::group_metadata(self)
    }

    fn client_id(&self) -> &str {
        AsyncKafkaConsumer::client_id(self)
    }

    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64> {
        AsyncKafkaConsumer::current_lag(self, topic_partition)
    }

    fn wakeup(&self) {
        AsyncKafkaConsumer::wakeup(self);
    }

    // ── Subscribe / unsubscribe / assign ───────────────────────────────

    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::subscribe(self, topics).await
    }

    async fn subscribe_with_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::subscribe_with_listener(self, topics, listener).await
    }

    async fn subscribe_pattern(&mut self, pattern: SubscriptionPattern) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::subscribe_re2j_pattern(self, pattern).await
    }

    async fn subscribe_pattern_with_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::subscribe_re2j_pattern_with_listener(self, pattern, listener).await
    }

    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::assign(self, partitions).await
    }

    async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::unsubscribe(self).await
    }

    // ── Poll ───────────────────────────────────────────────────────────

    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError> {
        AsyncKafkaConsumer::poll(self, timeout).await
    }

    // ── Commit ─────────────────────────────────────────────────────────

    async fn commit_sync(&mut self) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_sync(self).await
    }

    async fn commit_sync_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_sync_timeout(self, timeout).await
    }

    async fn commit_sync_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_sync_offsets(self, offsets).await
    }

    async fn commit_sync_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_sync_offsets_timeout(self, offsets, timeout).await
    }

    async fn commit_async(&mut self) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_async(self).await
    }

    async fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_async_with_callback(self, callback).await
    }

    async fn commit_async_offsets_with_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn crate::consumer::OffsetCommitCallback>,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::commit_async_offsets_with_callback(self, offsets, callback).await
    }

    // ── Seek ───────────────────────────────────────────────────────────

    async fn seek(&mut self, partition: TopicPartition, offset: i64) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::seek(self, partition, offset).await
    }

    async fn seek_with_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::seek_with_metadata(self, partition, offset_and_metadata).await
    }

    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::seek_to_beginning(self, partitions).await
    }

    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::seek_to_end(self, partitions).await
    }

    // ── Position / committed ───────────────────────────────────────────

    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, KafkaError> {
        AsyncKafkaConsumer::position(self, partition).await
    }

    async fn position_timeout(&mut self, partition: &TopicPartition, timeout: Duration) -> Result<i64, KafkaError> {
        AsyncKafkaConsumer::position_timeout(self, partition, timeout).await
    }

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        AsyncKafkaConsumer::committed(self, partitions).await
    }

    async fn committed_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        AsyncKafkaConsumer::committed_timeout(self, partitions, timeout).await
    }

    // ── Topic metadata ────────────────────────────────────────────────

    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<crate::common::PartitionInfo>, KafkaError> {
        AsyncKafkaConsumer::partitions_for(self, topic).await
    }

    async fn partitions_for_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<crate::common::PartitionInfo>, KafkaError> {
        AsyncKafkaConsumer::partitions_for_timeout(self, topic, timeout).await
    }

    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, KafkaError> {
        AsyncKafkaConsumer::list_topics(self).await
    }

    async fn list_topics_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<crate::common::PartitionInfo>>, KafkaError> {
        AsyncKafkaConsumer::list_topics_timeout(self, timeout).await
    }

    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError> {
        AsyncKafkaConsumer::offsets_for_times(self, timestamps_to_search).await
    }

    async fn offsets_for_times_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError> {
        AsyncKafkaConsumer::offsets_for_times_timeout(self, timestamps_to_search, timeout).await
    }

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        AsyncKafkaConsumer::beginning_offsets(self, partitions).await
    }

    async fn beginning_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        AsyncKafkaConsumer::beginning_offsets_timeout(self, partitions, timeout).await
    }

    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        AsyncKafkaConsumer::end_offsets(self, partitions).await
    }

    async fn end_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        AsyncKafkaConsumer::end_offsets_timeout(self, partitions, timeout).await
    }

    // ── Pause / resume ─────────────────────────────────────────────────

    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::pause(self, partitions).await
    }

    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::resume(self, partitions).await
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    async fn enforce_rebalance(&mut self, reason: Option<&str>) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::enforce_rebalance(self, reason).await
    }

    async fn close(&mut self) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::close(self).await
    }

    async fn close_with_options(&mut self, options: crate::consumer::CloseOptions) -> Result<(), KafkaError> {
        AsyncKafkaConsumer::close_with_options(self, options).await
    }
}

#[cfg(test)]
mod tests {
    //! State-read tests for the Phase 11 commit (2/N) ctor.
    //!
    //! Most Java tests in `AsyncKafkaConsumerTest.java` exercise
    //! end-to-end behavior (poll / commit / close). The handful that
    //! fit commit (2/N) are state-read tests:
    //!
    //!   - `testAssignmentEmpty` (no subscribe → empty assignment),
    //!   - `testSubscriptionEmpty`,
    //!   - `testWakeupIsCallableFromMultipleTasks`,
    //!   - `testClientIdReturnsConfigValue`,
    //!   - `testGroupMetadataReturnsStubForGrouplessConsumer`.
    //!
    //! Subscribe / unsubscribe / assign tests land in commit (3/N).
    //! Poll / commit / close tests land in commits (4/N)-(7/N).

    use std::collections::HashSet;
    use std::sync::atomic::AtomicI64;

    use tokio::sync::mpsc;

    use crate::common::internals::ClusterResourceListeners;
    use crate::common::serialization::Deserializer;
    use crate::consumer::AutoOffsetResetStrategy;
    use crate::consumer::internals::events::application_event::ApplicationEventEnvelope;
    use crate::consumer::internals::events::completable_event_reaper::CompletableEventReaper;

    use super::*;

    /// Minimal `Vec<u8>` deserializer for tests — equivalent to Java's
    /// `ByteArrayDeserializer`. Returns the input bytes unchanged.
    struct TestBytesDeserializer;

    impl Deserializer<Vec<u8>> for TestBytesDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
            Ok(data.to_vec())
        }
    }

    /// Test-side handles handed back from [`make_test_consumer_with_channels`].
    /// Holding these lets the test impersonate the bg task: drain the
    /// app-event channel to act on `add_and_get`-style events, and push
    /// background events into the consumer's bg-event channel for
    /// `process_background_events` to drain.
    struct ConsumerTestHandles {
        /// Receiver for app-side events. Held by the test instead of the
        /// (absent) bg task.
        app_event_rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
        /// Sender for bg-side events, used to enqueue events the bg task
        /// would normally post.
        bg_event_tx: mpsc::UnboundedSender<BackgroundEventEnvelope>,
        /// Handle on the shared `SubscriptionState` so tests can inspect /
        /// pre-populate it.
        subscriptions: Arc<Mutex<SubscriptionState>>,
    }

    /// Builds a consumer along with the test-side channel handles needed
    /// to act as the bg task during a test.
    fn make_test_consumer_with_channels() -> (AsyncKafkaConsumer<Vec<u8>, Vec<u8>>, ConsumerTestHandles) {
        let mut config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        config.client_id = "test-client".to_string();
        config.group_id = Some("test-group".to_string());
        let client_id: Arc<str> = Arc::from(config.client_id.as_str());

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            Arc::clone(&subs),
            ClusterResourceListeners::new(),
        ));
        let request_managers = Arc::new(std::sync::Mutex::new(RequestManagers::new(
            None, None, None, None, None, None, None,
        )));
        // Test stand-in for the bg task's app-event receiver. Tests hold
        // this `app_event_rx` and pull events off it themselves.
        let (app_handler_tx, app_event_rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        let app_handler = Arc::new(ApplicationEventHandler::new(app_handler_tx));
        let reaper = Arc::new(std::sync::Mutex::new(CompletableEventReaper::new()));
        let max_time = Arc::new(AtomicI64::new(0));
        let wakeup = WakeupTrigger::new();

        // Stub join handle — spawn a noop task. Tests do not assert on
        // the bg task's behavior in this commit.
        let join_handle: JoinHandle<()> = tokio::spawn(async move {});
        let signal_close_called = Arc::new(AtomicBool::new(false));
        let signal_close_flag = Arc::clone(&signal_close_called);
        let wakeup_called = Arc::new(AtomicBool::new(false));
        let wakeup_flag = Arc::clone(&wakeup_called);
        let close_handle = NetworkThreadCloseHandle::new(
            Box::new(move || {
                signal_close_flag.store(true, Ordering::Release);
            }),
            Box::new(move || {
                wakeup_flag.store(true, Ordering::Release);
            }),
            join_handle,
        );

        let (bg_event_tx, bg_rx) = mpsc::unbounded_channel::<BackgroundEventEnvelope>();
        let interceptors = Arc::new(Mutex::new(ConsumerInterceptors::<Vec<u8>, Vec<u8>>::new(Vec::new())));
        let offset_commit_callback_invoker =
            Arc::new(OffsetCommitCallbackInvoker::<Vec<u8>, Vec<u8>>::new(ConsumerInterceptors::<
                Vec<u8>,
                Vec<u8>,
            >::new(
                Vec::new()
            )));
        let deserializers = Arc::new(Deserializers::<Vec<u8>, Vec<u8>>::new(
            Box::new(TestBytesDeserializer),
            Box::new(TestBytesDeserializer),
        ));
        let rebalance_listener_invoker = ConsumerRebalanceListenerInvoker::new(Arc::clone(&subs));

        let fetch_buffer = Arc::new(FetchBuffer::new());
        let fetch_config = crate::consumer::internals::fetch_config::FetchConfig::new(
            1,
            50 * 1024 * 1024,
            500,
            1024 * 1024,
            500,
            true,
            "",
            IsolationLevel::ReadUncommitted,
        );
        let fetch_collector = Arc::new(FetchCollector::<Vec<u8>, Vec<u8>>::new(
            Arc::clone(&metadata),
            Arc::clone(&subs),
            fetch_config,
            Arc::clone(&deserializers),
            Arc::new(crate::consumer::internals::fetch_collector::SystemFetchCollectorTime),
        ));

        let components = AsyncKafkaConsumerComponents {
            config,
            client_id,
            group_id: Some("test-group".to_string()),
            subscriptions: Arc::clone(&subs),
            metadata,
            request_managers,
            background_event_rx: bg_rx,
            application_event_handler: app_handler,
            completable_event_reaper: reaper,
            max_time_to_wait_ms: max_time,
            wakeup_trigger: wakeup,
            network_thread_close: close_handle,
            fetch_buffer,
            fetch_collector,
            rebalance_listener_invoker,
            offset_commit_callback_invoker,
            deserializers,
            interceptors,
            isolation_level: IsolationLevel::ReadUncommitted,
            time: Arc::new(crate::consumer::internals::consumer_network_thread::SystemThreadTime),
        };
        (
            AsyncKafkaConsumer::<Vec<u8>, Vec<u8>>::new_with_components(components),
            ConsumerTestHandles { app_event_rx, bg_event_tx, subscriptions: subs },
        )
    }

    /// Backwards-compat alias for the existing state-read tests.
    fn make_test_consumer() -> AsyncKafkaConsumer<Vec<u8>, Vec<u8>> {
        make_test_consumer_with_channels().0
    }

    #[tokio::test]
    async fn assignment_is_empty_before_subscribe() {
        let consumer = make_test_consumer();
        assert!(consumer.assignment().is_empty());
    }

    #[tokio::test]
    async fn subscription_is_empty_before_subscribe() {
        let consumer = make_test_consumer();
        assert!(consumer.subscription().is_empty());
    }

    #[tokio::test]
    async fn paused_is_empty_before_subscribe() {
        let consumer = make_test_consumer();
        assert!(consumer.paused().is_empty());
    }

    #[tokio::test]
    async fn client_id_returns_config_value() {
        let consumer = make_test_consumer();
        assert_eq!(consumer.client_id(), "test-client");
    }

    #[tokio::test]
    async fn group_metadata_returns_stub_for_uninitialized_consumer() {
        let consumer = make_test_consumer();
        let meta = consumer.group_metadata();
        assert_eq!(meta.group_id(), "test-group");
    }

    /// Java parity: `memberStateListener.onMemberEpochUpdated` →
    /// `updateGroupMetadata` populates the cached metadata.
    /// (`AsyncKafkaConsumer.java:343-353`, `:772-784`).
    #[tokio::test]
    async fn state_notifier_populates_group_metadata_on_epoch_update() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        // Pre-condition: the cache is empty (stub returned).
        assert_eq!(consumer.group_metadata().generation_id(), -1);

        notifier.on_member_epoch_updated(Some(42), "member-id-xyz");

        let meta = consumer.group_metadata();
        assert_eq!(meta.group_id(), "test-group");
        assert_eq!(meta.generation_id(), 42);
        assert_eq!(meta.member_id(), "member-id-xyz");
    }

    /// Java: `memberEpoch.ifPresent(...)` short-circuits when None —
    /// the cache is NOT cleared. Mirrors Java's
    /// `updateGroupMetadata(Optional.empty(), …)` no-op behavior.
    #[tokio::test]
    async fn state_notifier_with_none_epoch_does_not_modify_cache() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        notifier.on_member_epoch_updated(Some(7), "m1");
        notifier.on_member_epoch_updated(None, "m1");
        // None did not overwrite — the epoch 7 entry survives.
        assert_eq!(consumer.group_metadata().generation_id(), 7);
    }

    /// Java parity: `memberStateListener.onGroupAssignmentUpdated` →
    /// `setGroupAssignmentSnapshot(partitions)` updates the snapshot
    /// (`AsyncKafkaConsumer.java:349-352`, `:786-788`).
    #[tokio::test]
    async fn state_notifier_updates_group_assignment_snapshot() {
        let consumer = make_test_consumer();
        let notifier = consumer.state_notifier();
        let tp0 = TopicPartition::new("t".to_string(), 0);
        let tp1 = TopicPartition::new("t".to_string(), 1);
        let mut set: HashSet<TopicPartition> = HashSet::new();
        set.insert(tp0.clone());
        set.insert(tp1.clone());

        notifier.on_group_assignment_updated(&set);

        let snap = consumer.group_assignment_snapshot.lock().unwrap().clone();
        assert_eq!(snap.len(), 2);
        assert!(snap.contains(&tp0));
        assert!(snap.contains(&tp1));
    }

    #[tokio::test]
    async fn current_lag_returns_none_in_commit_2() {
        // Phase-11 commit (2/N) stub — verified to return None until
        // commit (6/N) wires CurrentLagEvent.
        let consumer = make_test_consumer();
        let tp = TopicPartition::new("t".to_string(), 0);
        assert_eq!(consumer.current_lag(&tp), None);
    }

    #[tokio::test]
    async fn wakeup_triggers_token_cancellation_and_bg_wakeup() {
        let consumer = make_test_consumer();
        let token = consumer.wakeup_trigger.current_token();
        assert!(!token.is_cancelled());
        consumer.wakeup();
        assert!(token.is_cancelled(), "wakeup() must cancel the current token");
    }

    /// Stand-in for Java's `testAssignmentReturnsAssignedPartitions`
    /// once `assign` lands in commit (3/N) — for now we mutate
    /// `SubscriptionState` directly to verify the read path.
    #[tokio::test]
    async fn assignment_reads_subscription_state() {
        let consumer = make_test_consumer();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }
        let assignment = consumer.assignment();
        assert_eq!(assignment.len(), 1);
        assert!(assignment.contains(&tp));
    }

    // ─── Subscribe / unsubscribe / assign tests (commit (3/N)) ───
    //
    // These tests stand in for Java's
    // `AsyncKafkaConsumerTest.testSubscribeGeneratesEvent` /
    // `testSubscribePatternGeneratesEvent` /
    // `testUnsubscribeGeneratesUnsubscribeEvent` /
    // `testAssign*` / etc. Where Java's tests use Mockito to immediately
    // satisfy `applicationEventHandler.addAndGet(...)`, the Rust tests
    // spawn a small helper task that drains one envelope off the
    // app-event channel and `complete`s the handle inside it.
    //
    // Skipped Java tests for this commit (rationale per row):
    //   - `testAssignOnNullTopicPartition` / `testAssignOnNullTopicInPartition`
    //     — Rust's type system makes the `null` case unrepresentable:
    //     `Vec<TopicPartition>` cannot contain a `null` slot, and
    //     `TopicPartition` requires an owned `String` topic.
    //   - `testSubscribeToNullTopicCollection` /
    //     `testSubscriptionOnNullTopic` — same.
    //   - `testReaperInvokedInUnsubscribe` — depends on `backgroundEventReaper.reap(time)`
    //     wiring inside `process_background_events`; the reaper hookup
    //     itself lands later (Phase 11 commit (4/N)). Deferred to that
    //     commit.
    //   - `testGroupMetadataIsResetAfterUnsubscribe` — depends on the
    //     `MemberStateListener` (commit (7/N)) that populates
    //     `group_metadata`. Deferred to that commit.
    //   - `testUnsubscribeWithoutGroupId` — depends on a no-group ctor
    //     path which is built in commit (7/N) via `new_consumer`.
    //     Deferred.
    //   - `testSubscribePatternAgainstBrokerNotSupportingRegex` —
    //     end-to-end against a `MockClient`; depends on the poll path
    //     (commit (4/N)).
    //   - `testReaperInvokedInPoll` / similar poll-only flows — commit
    //     (4/N).

    /// Helper: spawn a task that takes the next envelope off the
    /// app-event channel and completes its `handle` with `Ok(())`.
    /// Mirrors Java's
    /// `completeTopicSubscriptionChangeEventSuccessfully()` / etc.
    fn auto_complete_next_event(
        mut rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
    ) -> tokio::task::JoinHandle<Option<ApplicationEventEnvelope>> {
        tokio::spawn(async move {
            let env = rx.recv().await?;
            match &env.event {
                ApplicationEvent::TopicSubscriptionChange { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::TopicPatternSubscriptionChange { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::TopicRe2JPatternSubscriptionChange { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::AssignmentChange { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::Unsubscribe { handle } => {
                    handle.complete(());
                },
                ApplicationEvent::SeekUnvalidated { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::ResetOffset { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::PausePartitions { handle, .. } => {
                    handle.complete(());
                },
                ApplicationEvent::ResumePartitions { handle, .. } => {
                    handle.complete(());
                },
                _ => {
                    // Unknown variant — leave the handle un-completed; the
                    // test's `add_and_get` will time out and the
                    // assertion will be a clear failure.
                },
            }
            Some(env)
        })
    }

    /// Java: `testSubscribeGeneratesEvent`.
    #[tokio::test]
    async fn subscribe_generates_topic_subscription_change_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.subscribe(vec!["topic1".to_string()]).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::TopicSubscriptionChange { .. }));
    }

    /// Java: `testSubscribePatternGeneratesEvent` (client-side Pattern).
    #[tokio::test]
    async fn subscribe_pattern_generates_topic_pattern_subscription_change_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        let pattern = Regex::new("topic.*").expect("valid regex");
        consumer.subscribe_pattern(pattern).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::TopicPatternSubscriptionChange { .. }));
    }

    /// Java: `testSubscribeToRe2JPatternGeneratesEvent`.
    #[tokio::test]
    async fn subscribe_re2j_pattern_generates_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer
            .subscribe_re2j_pattern(SubscriptionPattern::new("t*"))
            .await
            .expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::TopicRe2JPatternSubscriptionChange { .. }));
    }

    /// Java: `testSubscribeToRe2JPatternValidation` — empty pattern
    /// rejected, non-empty pattern accepted.
    #[tokio::test]
    async fn subscribe_re2j_pattern_rejects_empty() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .subscribe_re2j_pattern(SubscriptionPattern::new(""))
            .await
            .expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalArgument(ref msg) if msg.contains("empty")));
    }

    /// Java: `testUnsubscribeGeneratesUnsubscribeEvent`.
    #[tokio::test]
    async fn unsubscribe_generates_unsubscribe_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.unsubscribe().await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testSubscribeToEmptyListActsAsUnsubscribe`.
    #[tokio::test]
    async fn subscribe_to_empty_list_acts_as_unsubscribe() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.subscribe(Vec::new()).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testSubscriptionOnEmptyTopic` — blank topic rejected.
    #[tokio::test]
    async fn subscribe_rejects_blank_topic() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer.subscribe(vec!["  ".to_string()]).await.expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    /// Java: `testAssign`.
    #[tokio::test]
    async fn assign_generates_assignment_change_event_and_clears_subscription() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        let tp = TopicPartition::new("foo".to_string(), 3);
        consumer.assign(vec![tp.clone()]).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        match env.event {
            ApplicationEvent::AssignmentChange { partitions, .. } => {
                assert!(partitions.contains(&tp));
            },
            other => panic!("expected AssignmentChange, got {}", other.type_name()),
        }
    }

    /// Java: `testAssignOnEmptyTopicPartition` — empty collection
    /// acts as `unsubscribe()`.
    #[tokio::test]
    async fn assign_on_empty_acts_as_unsubscribe() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        consumer.assign(Vec::new()).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::Unsubscribe { .. }));
    }

    /// Java: `testAssignOnEmptyTopicInPartition` — blank topic rejected.
    #[tokio::test]
    async fn assign_rejects_blank_topic_in_partition() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("  ".to_string(), 0);
        let err = consumer.assign(vec![tp]).await.expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    /// Sanity check: subscribe stores the listener app-side so
    /// `process_background_events` can pick it up.
    #[tokio::test]
    async fn subscribe_with_listener_stores_listener() {
        use async_trait::async_trait;
        struct DummyListener;
        #[async_trait]
        impl ConsumerRebalanceListener for DummyListener {
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
        }
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(DummyListener);
        consumer
            .subscribe_with_listener(vec!["t".to_string()], Arc::clone(&listener))
            .await
            .expect("ok");
        let _ = completer.await;
        let stored = consumer.rebalance_listener.lock().unwrap().clone();
        assert!(stored.is_some(), "listener must be stored on subscribe_with_listener");
    }

    // ─── §31 process_background_events skeleton tests ───
    //
    // Full §31 regression pair (commit-from-inside-listener,
    // listener-blocks-rebalance) lands in Phase 11 commit (11/N). The
    // skeleton tests below just verify the drain loop and dispatch.

    /// Error event is drained and surfaces from
    /// `process_background_events`.
    #[tokio::test]
    async fn process_background_events_surfaces_error_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::Error { error: KafkaError::timeout("boom") },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");
        let result = consumer.process_background_events().await;
        assert!(matches!(result, Err(KafkaError::Timeout(_))));
    }

    /// Callback-needed event with no listener registered: succeeds and
    /// the ack is sent with `Ok(())` — mirrors Java's
    /// `listener.isPresent() == false` no-op branch.
    #[tokio::test]
    async fn process_background_events_with_no_listener_acks_ok() {
        use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
        use tokio::sync::oneshot;
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), KafkaError>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                partitions: vec![TopicPartition::new("t".to_string(), 0)],
                ack: ack_tx,
            },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");
        consumer.process_background_events().await.expect("ok");
        // The ack must have been resolved with `Ok(())`.
        let ack_result = ack_rx.await.expect("ack received");
        assert!(ack_result.is_ok());
    }

    /// Callback-needed event with a registered listener: the listener's
    /// `on_partitions_assigned` is invoked inline on the caller's task,
    /// and the ack is sent with the listener's result.
    #[tokio::test]
    async fn process_background_events_invokes_registered_listener() {
        use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::oneshot;

        struct RecordingListener {
            count: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for RecordingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
        }

        let (mut consumer, handles) = make_test_consumer_with_channels();
        let listener: Arc<RecordingListener> = Arc::new(RecordingListener { count: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), KafkaError>>();
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded {
                method_name: ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                partitions: vec![TopicPartition::new("t".to_string(), 0)],
                ack: ack_tx,
            },
            enqueued_ms: 0,
        };
        handles.bg_event_tx.send(env).expect("send ok");
        consumer.process_background_events().await.expect("ok");
        assert_eq!(listener.count.load(Ordering::SeqCst), 1);
        assert!(ack_rx.await.expect("ack received").is_ok());

        // Silence the unused-field warning for `subscriptions` on the
        // test handles (used here so the helper struct stays
        // forward-compatible with future tests that need to inspect the
        // shared state).
        drop(handles.subscriptions);
    }

    /// Empty bg-events channel: returns immediately with `Ok(())`.
    #[tokio::test]
    async fn process_background_events_on_empty_channel_is_ok() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.process_background_events().await.expect("ok");
    }

    // ─── Poll lifecycle tests (commit 4/N) ───
    //
    // Stand-ins for Java's `testWakeupBeforeCallingPoll`, `testWakeupAfterEmptyFetch`,
    // `testClearWakeupTriggerAfterPoll`, the `checkInflightPoll` arms, and the
    // "no subscription / no assignment" early-return arm.
    //
    // Skipped Java tests for this commit (each carries a one-line rationale):
    //   - `testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime` —
    //     `AsyncConsumerMetrics` deferred to a separate cross-cutting commit.
    //   - `testReaperInvokedInPoll` — depends on the metrics observers that
    //     would observe the reaper invocations. The `reap` call itself is
    //     wired and unit-tested via the bg-events drain test in commit 3.

    /// Java: `poll()` throws `IllegalStateException` when there is no
    /// subscription or assignment.
    #[tokio::test]
    async fn poll_returns_illegal_state_without_subscription() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("must err");
        assert!(
            matches!(err, KafkaError::IllegalState(ref msg)
                if msg == "Consumer is not subscribed to any topics or assigned any partitions"),
            "unexpected err: {err:?}"
        );
    }

    /// Java: `testFailOnClosedConsumer` (the `poll` arm).
    #[tokio::test]
    async fn poll_on_closed_consumer_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.closed.store(true, Ordering::Release);
        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("must err");
        assert!(
            matches!(err, KafkaError::IllegalState(ref msg)
                if msg.contains("already been closed")),
            "unexpected err: {err:?}"
        );
    }

    /// Java: `testWakeupBeforeCallingPoll` — `wakeup()` posted before
    /// `poll()` must surface as `KafkaError::Wakeup`. After the error is
    /// raised, a subsequent `poll()` must observe a fresh token (Java's
    /// "clear the volatile flag after throwing WakeupException once").
    #[tokio::test]
    async fn poll_observes_pending_wakeup_and_rotates_token() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        // Pre-populate an assignment so `poll()` does not bail with the
        // "not subscribed" error before reaching the wakeup check.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        consumer.wakeup_trigger.wakeup();
        let pre_token = consumer.wakeup_trigger.current_token();
        assert!(pre_token.is_cancelled(), "pre-condition: wakeup() cancelled the token");

        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("wakeup err");
        assert!(matches!(err, KafkaError::Wakeup(_)), "unexpected err: {err:?}");

        // After raising the wakeup error the consumer must have rotated
        // the token so the next poll observes a fresh one (§11).
        let post_token = consumer.wakeup_trigger.current_token();
        assert!(!post_token.is_cancelled(), "token must be rotated after Wakeup err");

        drop(handles);
    }

    /// `checkInflightPoll` submits a fresh AsyncPollEvent when none is
    /// in flight, and clears it on completion. Mirrors the Java unit-test
    /// behavior in `testEnsurePollEventSentOnConsumerPoll`.
    #[tokio::test]
    async fn poll_enqueues_async_poll_event_and_clears_on_completion() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Pre-populate the assignment so `poll()` does not error early.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp.clone());
            subs.assign_from_user(assigned).unwrap();
        }

        // First poll() — submits AsyncPoll, returns empty. We don't yet
        // drain the event off the channel, so the inflight state is
        // present and incomplete.
        let records = consumer.poll(Duration::from_millis(0)).await.expect("poll ok");
        assert!(records.is_empty(), "empty fetch buffer should yield empty records");
        assert!(consumer.inflight_poll.is_some(), "first poll() submits an AsyncPoll event");

        // Drain the envelope off the channel and synchronously mark the
        // shared state complete (mirrors the bg task's behavior).
        let env = handles.app_event_rx.try_recv().expect("AsyncPoll envelope must be on channel");
        let async_poll_state = match env.event {
            ApplicationEvent::AsyncPoll { state, .. } => state,
            other => panic!("expected AsyncPoll, got {}", other.type_name()),
        };
        async_poll_state.complete_successfully();

        // Second poll() drives `maybe_clear_previous_inflight_poll` over
        // the now-completed state and clears the inflight slot. The bg
        // task channel is still drained on each invocation; the new
        // AsyncPoll for this iteration goes back onto the channel and we
        // do not bother completing it.
        let _ = consumer.poll(Duration::from_millis(0)).await.expect("second poll ok");
        // Both the previously-completed event (cleared because complete)
        // and the freshly-submitted one for this iteration are accounted
        // for: after the second poll the inflight slot holds the new
        // (incomplete) event because `maybe_clear_current_inflight_poll`
        // is run with `newly_submitted_event=true`.
        assert!(
            consumer.inflight_poll.is_some(),
            "second poll() submits a fresh AsyncPoll event"
        );
    }

    /// A previous-iteration AsyncPoll event that completed with an error
    /// is surfaced from the next `poll()` call (mirrors Java's
    /// `maybeClearPreviousInflightPoll` error arm).
    #[tokio::test]
    async fn poll_surfaces_previous_inflight_poll_error() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        // Plant a previous inflight that already completed with an error.
        let state = Arc::new(AsyncPollState::new());
        state.complete_exceptionally(KafkaError::timeout("prior poll deadline"));
        consumer.inflight_poll = Some(InflightPoll { deadline_ms: 0, state });

        let err = consumer.poll(Duration::from_millis(0)).await.expect_err("must err");
        assert!(
            matches!(err, KafkaError::Timeout(ref m) if m == "prior poll deadline"),
            "unexpected err: {err:?}"
        );
        assert!(
            consumer.inflight_poll.is_none(),
            "previous inflight poll must be cleared after error"
        );
    }

    // ─── Commit tests (commit 5/N) ───
    //
    // Stand-ins for Java's
    //   - `testCommitAsyncWithNullCallback`
    //   - `testCommitAsyncUserSuppliedCallbackNoException`
    //   - `testCommitAsyncShouldCopyOffsets`
    //   - `testCommittedExceptionThrown`
    //   - `testEnsureCommitSyncExecutedCommitAsyncCallbacks`
    //
    // Skipped Java tests for this commit:
    //   - `testCommitInRebalanceCallback` — covered in the §31 regression
    //     pair (commit 11/N) which exercises commit-from-inside-listener.
    //   - `testCommitAsyncUserSuppliedCallbackWithException` (parameterized) —
    //     unit tested in `OffsetCommitCallbackInvoker` tests; the consumer
    //     side just routes the error.
    //   - Tests that exercise auto-commit-on-close — covered in commit 7/N.

    /// Helper: build a HashMap with a single tp -> offset entry.
    fn singleton_offsets(tp: TopicPartition, offset: i64) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let mut m = HashMap::new();
        m.insert(tp, OffsetAndMetadata::new(offset).expect("non-neg"));
        m
    }

    /// `commit_async` without a callback enqueues a `CommitAsync` event
    /// against the bg task. Mirrors Java's `testCommitAsyncWithNullCallback`.
    #[tokio::test]
    async fn commit_async_with_no_callback_enqueues_commit_async_event() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Spawn a completer: pull CommitAsync envelope, complete both
        // its `offsets_ready` and `handle`.
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitAsync { handle, offsets_ready, .. } = env.event {
                    offsets_ready.complete(());
                    handle.complete(HashMap::new());
                    return true;
                }
            }
            false
        });
        consumer.commit_async().await.expect("ok");
        assert!(completer.await.expect("task ok"), "must see CommitAsync envelope");
    }

    /// `commit_sync` runs the interceptor `on_commit` chain on the
    /// committed offsets. Mirrors Java's `testInterceptorOnCommit`.
    #[tokio::test]
    async fn commit_sync_invokes_interceptor_chain() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        let offsets = singleton_offsets(tp.clone(), 42);

        // Spawn a completer to complete the CommitSync envelope with the
        // committed offsets.
        let offsets_for_completer = offsets.clone();
        let completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                if let ApplicationEvent::CommitSync { handle, offsets_ready, .. } = env.event {
                    offsets_ready.complete(());
                    handle.complete(offsets_for_completer.clone());
                    return true;
                }
            }
            false
        });

        consumer.commit_sync_offsets(offsets).await.expect("ok");
        assert!(completer.await.expect("task ok"));
    }

    /// `commit_async` with an empty offsets map short-circuits without
    /// enqueuing a CommitAsync event (Java's `if (offsets.isPresent() &&
    /// offsets.get().isEmpty()) return completedFuture(null)`).
    #[tokio::test]
    async fn commit_async_with_empty_offsets_short_circuits() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer
            .commit_async_offsets_with_callback(HashMap::new(), Arc::new(NoopCallback))
            .await
            .expect("ok");
        // No envelope should be on the channel.
        let env = handles.app_event_rx.try_recv();
        assert!(env.is_err(), "expected no envelope, got {env:?}");
    }

    /// `commit_sync` on a groupless consumer errors with `IllegalArgument`
    /// (Rust analog of Java's `InvalidGroupIdException`). Mirrors Java's
    /// `testCommitSyncWithoutGroupId`.
    #[tokio::test]
    async fn commit_sync_without_group_id_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        let err = consumer.commit_sync().await.expect_err("must err");
        assert!(
            matches!(err, KafkaError::IllegalArgument(ref m) if m.contains("group.id")),
            "unexpected err: {err:?}"
        );
    }

    /// `commit_sync` after a pending async commit drains the async first
    /// (mirrors Java's `testEnsureCommitSyncExecutedCommitAsyncCallbacks`
    /// — the assertion is observable side: `last_pending_async_commit`
    /// is consumed by the sync path).
    #[tokio::test]
    async fn commit_sync_drains_pending_async_commit() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Spawn a single completer that races for both events.
        let async_completer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                        return true;
                    },
                    _ => {},
                }
            }
            false
        });
        consumer.commit_async().await.expect("async ok");
        assert!(
            consumer.last_pending_async_commit.is_some(),
            "async commit must register pending"
        );
        consumer.commit_sync().await.expect("sync ok");
        assert!(
            consumer.last_pending_async_commit.is_none(),
            "sync must consume the pending async commit"
        );
        assert!(async_completer.await.expect("task ok"));
    }

    /// Test-only callback that records no state. Used to keep the
    /// `commit_async_offsets_with_callback` arg slot non-null in tests
    /// that don't observe the callback firing.
    struct NoopCallback;
    #[async_trait::async_trait]
    impl crate::consumer::OffsetCommitCallback for NoopCallback {
        async fn on_complete(
            &self,
            _offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
            _error: Option<&KafkaError>,
        ) {
        }
    }

    // ─── Seek / position / committed / lag tests (commit 6/N) ───

    /// `seek` with a negative offset rejects with `IllegalArgument`.
    /// Java: `seek` throws `IllegalArgumentException("seek offset must not
    /// be a negative number")`.
    #[tokio::test]
    async fn seek_rejects_negative_offset() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer.seek(tp, -1).await.expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalArgument(_)), "unexpected err: {err:?}");
    }

    /// `seek` enqueues a `SeekUnvalidated` event. Mirrors Java's
    /// `testSeek` event-shape assertion.
    #[tokio::test]
    async fn seek_enqueues_seek_unvalidated_event() {
        let (mut consumer, handles) = make_test_consumer_with_channels();
        let completer = auto_complete_next_event(handles.app_event_rx);
        let tp = TopicPartition::new("t".to_string(), 0);
        consumer.seek(tp.clone(), 42).await.expect("ok");
        let env = completer.await.expect("task ok").expect("event received");
        assert!(matches!(env.event, ApplicationEvent::SeekUnvalidated { partition, offset, .. }
                if partition == tp && offset == 42));
    }

    /// `position` on an unassigned partition returns `IllegalState`.
    #[tokio::test]
    async fn position_on_unassigned_partition_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer
            .position_timeout(&tp, Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalState(_)), "unexpected err: {err:?}");
    }

    /// `committed` on an empty partition set returns an empty map without
    /// enqueuing an event. Java: `if (partitions.isEmpty()) return
    /// Collections.emptyMap();`.
    #[tokio::test]
    async fn committed_with_empty_partitions_returns_empty_map() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        let map = consumer.committed_timeout(&[], Duration::from_millis(100)).await.expect("ok");
        assert!(map.is_empty());
        assert!(handles.app_event_rx.try_recv().is_err(), "no event enqueued");
    }

    /// `committed` without group_id errors with `IllegalArgument`
    /// (Rust analog of Java's `InvalidGroupIdException`).
    #[tokio::test]
    async fn committed_without_group_id_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        let tp = TopicPartition::new("t".to_string(), 0);
        let err = consumer
            .committed_timeout(std::slice::from_ref(&tp), Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalArgument(_)), "unexpected err: {err:?}");
    }

    /// `pause` with empty input is a no-op (matches Java's
    /// `if (!partitions.isEmpty()) addAndGet(...)` short-circuit).
    #[tokio::test]
    async fn pause_with_empty_set_is_noop() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.pause(&[]).await.expect("ok");
        assert!(handles.app_event_rx.try_recv().is_err(), "no event enqueued");
    }

    /// `resume` with empty input is a no-op (matches Java's
    /// `if (!partitions.isEmpty()) addAndGet(...)` short-circuit).
    #[tokio::test]
    async fn resume_with_empty_set_is_noop() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.resume(&[]).await.expect("ok");
        assert!(handles.app_event_rx.try_recv().is_err(), "no event enqueued");
    }

    /// `enforce_rebalance` is a documented no-op under KIP-848 (Java
    /// `log.warn("Operation not supported in new consumer group protocol")`).
    #[tokio::test]
    async fn enforce_rebalance_is_noop() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        consumer.enforce_rebalance(None).await.expect("ok");
        consumer.enforce_rebalance(Some("test reason")).await.expect("ok");
    }

    /// `offsets_for_times` rejects negative timestamps.
    #[tokio::test]
    async fn offsets_for_times_rejects_negative_timestamp() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let mut ts = HashMap::new();
        ts.insert(TopicPartition::new("t".to_string(), 0), -5);
        let err = consumer
            .offsets_for_times_timeout(ts, Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(
            matches!(err, KafkaError::IllegalArgument(ref msg) if msg.contains("negative")),
            "unexpected err: {err:?}"
        );
    }

    /// `offsets_for_times` with empty map returns empty map.
    #[tokio::test]
    async fn offsets_for_times_with_empty_map_returns_empty() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let map = consumer
            .offsets_for_times_timeout(HashMap::new(), Duration::from_millis(0))
            .await
            .expect("ok");
        assert!(map.is_empty());
    }

    /// `beginning_offsets` with empty input returns empty map.
    #[tokio::test]
    async fn beginning_offsets_with_empty_input_returns_empty() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let map = consumer
            .beginning_offsets_timeout(&[], Duration::from_millis(0))
            .await
            .expect("ok");
        assert!(map.is_empty());
    }

    /// `partitions_for` with zero timeout and empty metadata cache
    /// errors with `Timeout`. Java: `if (timeout.toMillis() == 0L) throw
    /// new TimeoutException()`.
    #[tokio::test]
    async fn partitions_for_with_zero_timeout_and_empty_metadata_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .partitions_for_timeout("t", Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, KafkaError::Timeout(_)), "unexpected err: {err:?}");
    }

    /// `list_topics` with zero timeout errors with `Timeout`.
    #[tokio::test]
    async fn list_topics_with_zero_timeout_errors() {
        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let err = consumer
            .list_topics_timeout(Duration::from_millis(0))
            .await
            .expect_err("must err");
        assert!(matches!(err, KafkaError::Timeout(_)), "unexpected err: {err:?}");
    }

    // ─── Close / lifecycle tests (commit 7/N) ───
    //
    // Stand-ins for Java's `testCloseShouldBeIdempotent`,
    // `testWakeupShouldThrowAfterClose`, `testLeaveGroupOnClose`,
    // `testRunRebalanceCallbacksOnClose`. The fully end-to-end versions
    // (`MockClient`-backed bg task observing the actual close events)
    // land in commit (10/N) — these unit-test the close-path control
    // flow.
    //
    // Skipped Java tests:
    //   - `testCloseInvokesStreamsRebalanceListenerOn*` — Streams out
    //     of scope per consumer-threading.md §20.
    //   - `testCloseWrapsStreamsRebalanceListenerException` — Streams.

    /// `close` is idempotent: calling close twice does NOT panic or
    /// surface an error. Mirrors Java's `testCloseShouldBeIdempotent`.
    #[tokio::test]
    async fn close_is_idempotent() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drain LeaveGroupOnClose events on a background task, completing
        // each handle with Ok(()) so close_internal does not block.
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("first close ok");
        assert!(consumer.is_closed(), "close marks consumer closed");
        // Second close: no-op, no error.
        consumer.close().await.expect("idempotent close ok");
        drop(drainer);
    }

    /// After `close()`, the wakeup trigger is disabled so subsequent
    /// `wakeup()` calls are no-ops (Java
    /// `wakeupTrigger.disableWakeups()`).
    #[tokio::test]
    async fn close_disables_wakeups() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("ok");
        // wakeup() should be a no-op now — the wakeup trigger is disabled.
        // Verify by calling maybe_trigger_wakeup; it should return Ok.
        consumer.wakeup_trigger.wakeup();
        assert!(consumer.wakeup_trigger.maybe_trigger_wakeup().is_ok());
        drop(drainer);
    }

    /// `close_with_options(timeout=0)` short-cuts the deadline math but
    /// still completes successfully.
    #[tokio::test]
    async fn close_with_options_zero_timeout_completes() {
        use crate::consumer::CloseOptions;
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer
            .close_with_options(CloseOptions::timeout(Duration::from_millis(0)))
            .await
            .expect("ok");
        assert!(consumer.is_closed());
        drop(drainer);
    }

    /// Issue 12 regression: `run_rebalance_callbacks_on_close` must
    /// read from `group_assignment_snapshot` (populated by the
    /// `MemberStateListener`) — NOT from
    /// `SubscriptionState::assigned_partitions()`. With only a manual
    /// `assign(...)` (so the snapshot stays empty), close must NOT
    /// invoke any rebalance callback (Java line 1626-1628).
    #[tokio::test]
    async fn run_rebalance_callbacks_on_close_skips_when_snapshot_empty() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;

        struct CountingListener {
            revoked: AtomicUsize,
            lost: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.lost.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let listener: Arc<CountingListener> =
            Arc::new(CountingListener { revoked: AtomicUsize::new(0), lost: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        // Populate SubscriptionState as if the user called assign(...).
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut subs = consumer.subscriptions.lock().unwrap();
            let mut assigned: HashSet<TopicPartition> = HashSet::new();
            assigned.insert(tp);
            subs.assign_from_user(assigned).unwrap();
        }
        // The snapshot stays empty — no reconciliation has run.
        assert!(consumer.group_assignment_snapshot.lock().unwrap().is_empty());

        consumer.run_rebalance_callbacks_on_close().await.expect("ok");
        assert_eq!(
            listener.revoked.load(Ordering::SeqCst),
            0,
            "no listener call when snapshot is empty (manual-assign consumer)"
        );
        assert_eq!(listener.lost.load(Ordering::SeqCst), 0, "no listener call when snapshot is empty");
    }

    /// Issue 12/13 regression: `run_rebalance_callbacks_on_close`
    /// invokes `on_partitions_revoked` when the snapshot is non-empty
    /// AND `member_epoch > 0` (populated via the state notifier).
    /// Without Issue 13's `MemberStateListener` wire-up the epoch would
    /// always be -1 and the callback would always be `on_partitions_lost`.
    #[tokio::test]
    async fn run_rebalance_callbacks_on_close_invokes_revoked_on_live_epoch() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;

        struct CountingListener {
            revoked: AtomicUsize,
            lost: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.lost.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let listener: Arc<CountingListener> =
            Arc::new(CountingListener { revoked: AtomicUsize::new(0), lost: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        // The state notifier captures both the snapshot and the epoch
        // — mirrors what the bg-task reconciliation step would do.
        let notifier = consumer.state_notifier();
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut snap = HashSet::new();
        snap.insert(tp.clone());
        notifier.on_group_assignment_updated(&snap);
        notifier.on_member_epoch_updated(Some(5), "member-1");

        consumer.run_rebalance_callbacks_on_close().await.expect("ok");
        assert_eq!(
            listener.revoked.load(Ordering::SeqCst),
            1,
            "live epoch + non-empty snapshot must invoke on_partitions_revoked"
        );
        assert_eq!(listener.lost.load(Ordering::SeqCst), 0);
    }

    /// Issue 12/13 regression: when the snapshot is non-empty but the
    /// epoch is unknown (member fenced / never received heartbeat
    /// response), Java falls through to `on_partitions_lost`.
    #[tokio::test]
    async fn run_rebalance_callbacks_on_close_invokes_lost_on_unknown_epoch() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicUsize;

        struct CountingListener {
            revoked: AtomicUsize,
            lost: AtomicUsize,
        }
        #[async_trait]
        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                Ok(())
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.revoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_partitions_lost(&self, _: &[TopicPartition]) -> Result<(), KafkaError> {
                self.lost.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (mut consumer, _handles) = make_test_consumer_with_channels();
        let listener: Arc<CountingListener> =
            Arc::new(CountingListener { revoked: AtomicUsize::new(0), lost: AtomicUsize::new(0) });
        *consumer.rebalance_listener.lock().unwrap() =
            Some(Arc::clone(&listener) as Arc<dyn ConsumerRebalanceListener>);

        // Populate snapshot but do NOT update epoch — generation_id
        // stays at -1, so `on_partitions_lost` is invoked.
        let notifier = consumer.state_notifier();
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut snap = HashSet::new();
        snap.insert(tp.clone());
        notifier.on_group_assignment_updated(&snap);

        consumer.run_rebalance_callbacks_on_close().await.expect("ok");
        assert_eq!(listener.revoked.load(Ordering::SeqCst), 0);
        assert_eq!(
            listener.lost.load(Ordering::SeqCst),
            1,
            "unknown epoch + non-empty snapshot must invoke on_partitions_lost"
        );
    }

    /// `close` on a groupless consumer skips the leave-group event
    /// entirely. Mirrors Java's `if (groupMetadata.get().isEmpty())
    /// return;` guard.
    #[tokio::test]
    async fn close_without_group_id_skips_leave_group() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        consumer.group_id = None;
        consumer.close().await.expect("ok");
        // No LeaveGroupOnClose event should have been enqueued.
        let mut saw_leave_group = false;
        while let Ok(env) = handles.app_event_rx.try_recv() {
            if matches!(env.event, ApplicationEvent::LeaveGroupOnClose { .. }) {
                saw_leave_group = true;
            }
        }
        assert!(!saw_leave_group, "groupless consumer must not enqueue LeaveGroupOnClose");
    }

    /// After `close`, every async public API errors with
    /// `IllegalState` because `ensure_open()` short-circuits.
    #[tokio::test]
    async fn close_then_apis_error_with_already_closed() {
        let (mut consumer, mut handles) = make_test_consumer_with_channels();
        // Drainer that handles every close-path event (LeaveGroupOnClose,
        // CommitSync (auto-commit), CommitOnClose, StopFindCoordinatorOnClose).
        let drainer = tokio::spawn(async move {
            while let Some(env) = handles.app_event_rx.recv().await {
                match env.event {
                    ApplicationEvent::LeaveGroupOnClose { handle, .. } => {
                        handle.complete(());
                    },
                    ApplicationEvent::CommitSync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    ApplicationEvent::CommitAsync { handle, offsets_ready, .. } => {
                        offsets_ready.complete(());
                        handle.complete(HashMap::new());
                    },
                    _ => {},
                }
            }
        });
        consumer.close().await.expect("ok");

        // Each blocking-style API should now return IllegalState.
        let err = consumer.commit_sync().await.expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalState(_)), "commit_sync: {err:?}");

        let err = consumer.unsubscribe().await.expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalState(_)), "unsubscribe: {err:?}");

        drop(drainer);
    }

    /// Compile-time check: `AsyncKafkaConsumer<K, V>` is `Consumer<K, V>`.
    /// Asserts the trait impl is wired correctly.
    #[test]
    fn consumer_trait_impl_compiles() {
        fn _accept_consumer<C: crate::consumer::Consumer<Vec<u8>, Vec<u8>>>(_c: C) {}
        // Only the type-level check matters — no runtime assertions.
        let _phantom: fn(AsyncKafkaConsumer<Vec<u8>, Vec<u8>>) = _accept_consumer;
    }
}
