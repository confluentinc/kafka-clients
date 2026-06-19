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

//! `CommitRequestManager` — handles `OffsetCommit` and `OffsetFetch`
//! request/response cycles, auto-commit timing, sync-commit retry, and
//! membership-state coordination on behalf of the KIP-848 consumer.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.CommitRequestManager`.
//!
//! # Threading model
//!
//! Every method is called either from the background task (which holds the
//! manager and drives `RequestManager::poll`) or from the application task
//! (which holds an `Arc` and dispatches sync / async commit requests via
//! [`Self::commit_sync`] / [`Self::commit_async`] / [`Self::fetch_offsets`]).
//!
//! The shared state — pending requests, auto-commit timer, member info —
//! lives behind a `Mutex` so the two sides can interact without
//! `Arc<Mutex<&mut Self>>`-style ownership pretzels. The mutex is acquired
//! only for short critical sections and **never held across an `.await`**
//! per CLAUDE.md §9.6.
//!
//! # Deferred wiring
//!
//! - `MemberStateListener` impl is supplied (Phase 10, commit 2.5/N).
//!   The membership manager registers the commit manager as a listener;
//!   the registration call site itself lands with the
//!   `AsyncKafkaConsumer` integration in Phase 11.
//! - `maybe_auto_commit_sync_before_rebalance` is supplied (Phase 10,
//!   commit 2.5/N). The reconciliation pipeline's invocation of this
//!   method is deferred to Phase 11 because awaiting the returned
//!   `oneshot::Receiver` requires the consumer poll-path scaffolding.
//! - `init_with_committed_offsets_if_needed` lives on
//!   `OffsetsRequestManager` (Phase 10, commit 3a/N) — that's where Java
//!   places it. It composes a call to [`Self::fetch_offsets`] with
//!   subscription-state updates. The commit manager owns only the
//!   underlying `fetch_offsets` request issuance.

// Phase 9 lands the manager; Phase 10 wires it into the bg task and
// Phase 11 wires the public API. Suppress dead-code warnings until then.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::common::protocol::Errors;
use crate::common::requests::{
    OffsetCommitRequestBuilder, OffsetCommitResponse, OffsetFetchRequestBuilder, RECORD_BATCH_NO_PARTITION_LEADER_EPOCH,
};
use crate::common::{KafkaError, TopicPartition, Uuid};
use crate::consumer::ConsumerConfig;
use crate::consumer::OffsetAndMetadata;
use crate::consumer::errors::ConsumerError;
use crate::consumer::offset_commit_callback::OffsetCommitCallback;
use crate::offset_commit_request_data::{
    OffsetCommitRequestData, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crate::offset_fetch_request_data::{OffsetFetchRequestData, OffsetFetchRequestGroup, OffsetFetchRequestTopics};

use super::consumer_metadata::ConsumerMetadata;
use super::coordinator_request_manager::CoordinatorRequestManager;
use super::member_state_listener::MemberStateListener;
use super::network_client_delegate::{PollResult, UnsentRequest};
use super::offset_commit_callback_invoker::{AutoCommitInterceptorHook, OffsetCommitCallbackInvoker};
use super::request_manager::RequestManager;
use super::subscription_state::SubscriptionState;
use super::timed_request_state::TimedRequestState;

// =========================================================================
//                       MemberInfo + AutoCommitState
// =========================================================================

/// Member identity (id + epoch) carried in every `OffsetCommit` /
/// `OffsetFetch` request issued by this consumer.
///
/// Translated from `CommitRequestManager.MemberInfo`. `member_epoch` is
/// `None` when no epoch is known (e.g. the member has left the group).
#[derive(Clone, Debug, Default)]
pub(crate) struct MemberInfo {
    pub(crate) member_id: String,
    pub(crate) member_epoch: Option<i32>,
}

impl std::fmt::Display for MemberInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.member_epoch {
            Some(e) => write!(f, "memberId={}, memberEpoch={}", self.member_id, e),
            None => write!(f, "memberId={}, memberEpoch=undefined", self.member_id),
        }
    }
}

/// State machine governing periodic auto-commit firings.
///
/// Translated from `CommitRequestManager.AutoCommitState`.
#[derive(Debug)]
struct AutoCommitState {
    auto_commit_interval_ms: i64,
    /// Absolute wall-clock millisecond timestamp at which the timer expires
    /// (i.e. the next auto-commit fires). Mirrors Java's `Timer.expirationMs()`.
    expiration_ms: i64,
    has_inflight_commit: bool,
}

impl AutoCommitState {
    fn new(now_ms: i64, auto_commit_interval_ms: i64) -> Self {
        Self {
            auto_commit_interval_ms,
            expiration_ms: now_ms.saturating_add(auto_commit_interval_ms),
            has_inflight_commit: false,
        }
    }

    /// Java: `shouldAutoCommit()`. Returns `true` if the timer has expired
    /// AND no commit is currently in flight.
    fn should_auto_commit(&self, current_time_ms: i64) -> bool {
        if current_time_ms < self.expiration_ms {
            return false;
        }
        if self.has_inflight_commit {
            log::trace!("Skipping auto-commit on the interval because a previous one is still in-flight.");
            return false;
        }
        true
    }

    /// Java: `resetTimer()`. Reset to the configured auto-commit interval
    /// from `now_ms`.
    fn reset_timer(&mut self, now_ms: i64) {
        self.expiration_ms = now_ms.saturating_add(self.auto_commit_interval_ms);
    }

    /// Java: `resetTimer(long retryBackoffMs)`. Reset to a caller-supplied
    /// backoff from `now_ms` (used when a retriable auto-commit failed).
    fn reset_timer_with_backoff(&mut self, now_ms: i64, retry_backoff_ms: i64) {
        self.expiration_ms = now_ms.saturating_add(retry_backoff_ms);
    }

    /// Java: `remainingMs(currentTimeMs)`. Returns 0 when the timer has
    /// already expired (no negative values).
    fn remaining_ms(&self, current_time_ms: i64) -> i64 {
        (self.expiration_ms - current_time_ms).max(0)
    }

    /// Java: `setInflightCommitStatus(boolean)`.
    fn set_inflight_commit_status(&mut self, inflight: bool) {
        self.has_inflight_commit = inflight;
    }
}

// =========================================================================
//                            Future-channel aliases
// =========================================================================

/// Result yielded by an [`OffsetCommitRequestState`] future.
type CommitResult = Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;
/// Idempotent commit-future sender slot.
type CommitFutureTx = Arc<Mutex<Option<oneshot::Sender<CommitResult>>>>;

/// Result yielded by an [`OffsetFetchRequestState`] future.
type FetchResult = Result<HashMap<TopicPartition, Option<OffsetAndMetadata>>, KafkaError>;
/// Idempotent fetch-future sender slot.
type FetchFutureTx = Arc<Mutex<Option<oneshot::Sender<FetchResult>>>>;

/// Idempotent sender slot for the rebalance-flush future used by
/// [`CommitRequestManager::maybe_auto_commit_sync_before_rebalance`].
/// Mirrors Java's `CompletableFuture<Void>` return type.
type RebalanceFlushTx = Arc<Mutex<Option<oneshot::Sender<Result<(), KafkaError>>>>>;

// =========================================================================
//             OffsetCommitRequestState / OffsetFetchRequestState
// =========================================================================

/// Pending offset-commit request awaiting send / response. Translated from
/// the nested `CommitRequestManager.OffsetCommitRequestState`.
///
/// `future_tx` is the application-side notification sink. Wrapped in
/// `Arc<Mutex<Option<...>>>` so completion is idempotent — only the first
/// call to `complete()` / `complete_err()` wins.
struct OffsetCommitRequestState {
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    member_info: MemberInfo,
    state: TimedRequestState,
    /// Tracks whether `on_send_attempt` has been called at least once.
    /// `RequestState::num_attempts` is private; we track separately so
    /// we can reproduce Java's `maybeExpire` invariant (only expire after
    /// at least one send attempt).
    has_attempted_send: bool,
    /// Number of prior retriable failures for this `commit_sync` flow.
    /// Java tracks this via the inherited `RequestState.numAttempts` field,
    /// which carries across `resetFuture()` calls because Java keeps the
    /// SAME `OffsetCommitRequestState` instance. In Rust the network send
    /// path consumes the state, so each retry creates a fresh state and
    /// this counter carries continuity across retries (seeded into
    /// `state.num_attempts` via [`Self::seed_failed_attempts`] so the
    /// exponential backoff in `RequestState` ramps up correctly).
    ///
    /// Mirrors Java `RequestState.numAttempts`, surfaced here per Phase 10
    /// wire-prereq #9 so the `commit_sync` retry path can decide between
    /// retry and surface-to-caller using both the deadline (`isExpired`)
    /// AND the per-attempt backoff bookkeeping.
    commit_sync_attempts: i32,
    future_tx: CommitFutureTx,
}

impl OffsetCommitRequestState {
    fn new(
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        member_info: MemberInfo,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        deadline_ms: i64,
        now_ms: i64,
    ) -> (Self, oneshot::Receiver<CommitResult>) {
        let (tx, rx) = oneshot::channel();
        let state = TimedRequestState::new(
            "CommitRequestManager",
            retry_backoff_ms,
            retry_backoff_max_ms,
            TimedRequestState::deadline_for(now_ms, deadline_ms),
        );
        (
            Self {
                offsets,
                member_info,
                state,
                has_attempted_send: false,
                commit_sync_attempts: 0,
                future_tx: Arc::new(Mutex::new(Some(tx))),
            },
            rx,
        )
    }

    /// Seed the inner [`RequestState`]'s `num_attempts` counter to `n` by
    /// invoking `on_failed_attempt(now_ms)` `n` times. Used by the
    /// `commit_sync` retry driver to carry exponential-backoff continuity
    /// across retry attempts (each retry creates a fresh state instance
    /// because the original is consumed by the send path).
    fn seed_failed_attempts(&mut self, n: i32, now_ms: i64) {
        for _ in 0..n {
            self.state.on_failed_attempt(now_ms);
        }
        self.commit_sync_attempts = n;
    }

    fn complete_ok(&self, value: HashMap<TopicPartition, OffsetAndMetadata>) {
        let mut guard = self.future_tx.lock().expect("OffsetCommit future_tx mutex poisoned");
        if let Some(tx) = guard.take() {
            let _ = tx.send(Ok(value));
        }
    }

    fn complete_err(&self, err: KafkaError) {
        let mut guard = self.future_tx.lock().expect("OffsetCommit future_tx mutex poisoned");
        if let Some(tx) = guard.take() {
            let _ = tx.send(Err(err));
        }
    }

    fn reset_future(&mut self) -> oneshot::Receiver<CommitResult> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.future_tx.lock().expect("OffsetCommit future_tx mutex poisoned");
        *guard = Some(tx);
        rx
    }
}

/// Pending offset-fetch request awaiting send / response. Translated from
/// the nested `CommitRequestManager.OffsetFetchRequestState`.
struct OffsetFetchRequestState {
    /// Monotonic per-manager identifier. Java uses object identity
    /// (`.remove(fetchRequest)` is reference-equality) to find the matching
    /// entry in `inflightOffsetFetches` on completion; Rust uses an explicit
    /// `u64` because we cannot rely on heap-address identity (the value is
    /// owned by the `Vec` and may move).
    request_id: u64,
    requested_partitions: HashSet<TopicPartition>,
    member_info: MemberInfo,
    state: TimedRequestState,
    /// Topic id → topic name cache, captured at request-build time. Used
    /// to resolve topic names from the response when topic ids are on the
    /// wire (v10+). Mirrors Java's `topicNamesCache`.
    topic_names_cache: HashMap<Uuid, String>,
    future_tx: FetchFutureTx,
}

impl OffsetFetchRequestState {
    fn new(
        request_id: u64,
        requested_partitions: HashSet<TopicPartition>,
        member_info: MemberInfo,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        deadline_ms: i64,
        now_ms: i64,
    ) -> (Self, oneshot::Receiver<FetchResult>) {
        let (tx, rx) = oneshot::channel();
        let state = TimedRequestState::new(
            "CommitRequestManager",
            retry_backoff_ms,
            retry_backoff_max_ms,
            TimedRequestState::deadline_for(now_ms, deadline_ms),
        );
        (
            Self {
                request_id,
                requested_partitions,
                member_info,
                state,
                topic_names_cache: HashMap::new(),
                future_tx: Arc::new(Mutex::new(Some(tx))),
            },
            rx,
        )
    }

    fn same_request(&self, other: &OffsetFetchRequestState) -> bool {
        self.requested_partitions == other.requested_partitions
    }

    fn complete_ok(&self, value: HashMap<TopicPartition, Option<OffsetAndMetadata>>) {
        let mut guard = self.future_tx.lock().expect("OffsetFetch future_tx mutex poisoned");
        if let Some(tx) = guard.take() {
            let _ = tx.send(Ok(value));
        }
    }

    fn complete_err(&self, err: KafkaError) {
        let mut guard = self.future_tx.lock().expect("OffsetFetch future_tx mutex poisoned");
        if let Some(tx) = guard.take() {
            let _ = tx.send(Err(err));
        }
    }

    fn reset_future(&mut self) -> oneshot::Receiver<FetchResult> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.future_tx.lock().expect("OffsetFetch future_tx mutex poisoned");
        *guard = Some(tx);
        rx
    }

    /// Seed the inner [`RequestState`]'s `num_attempts` counter by
    /// invoking `on_failed_attempt(now_ms)` `n` times. Used by the
    /// `fetch_offsets` retry driver to carry exponential-backoff
    /// continuity across retries (each retry creates a fresh state
    /// instance because the original is consumed by the send path).
    /// Mirrors [`OffsetCommitRequestState::seed_failed_attempts`].
    fn seed_failed_attempts(&mut self, n: i32, now_ms: i64) {
        for _ in 0..n {
            self.state.on_failed_attempt(now_ms);
        }
    }
}

// =========================================================================
//                                Manager
// =========================================================================

/// Per-consumer commit / fetch-offsets request manager. Owns the pending
/// request queues, auto-commit timer, and member-info state.
///
/// Translated from `CommitRequestManager`. The "metrics manager" and
/// "Streams" hooks present in the Java source are intentionally dropped
/// per the Phase 9 plan ("Out of scope").
pub(crate) struct CommitRequestManager {
    inner: Arc<CommitRequestManagerInner>,
}

/// Shared state held by [`CommitRequestManager`]. The handle is `Arc`-shared
/// between the BG task (which owns the manager) and any per-request response
/// callback closures registered against the network client. The mutex
/// protects the parts that change at runtime; immutable config lives outside
/// the mutex.
struct CommitRequestManagerInner {
    group_id: String,
    group_instance_id: Option<String>,
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    throw_on_fetch_stable_offset_unsupported: bool,
    metadata: Arc<ConsumerMetadata>,
    /// Java: `SubscriptionState subscriptions`. Used by `maybeAutoCommitAsync`
    /// and `maybeAutoCommitSyncBeforeRebalance` to snapshot
    /// `subscriptions.allConsumed()` at commit time.
    subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Tracks whether `signal_close()` has fired.
    closing: Mutex<bool>,
    /// Monotonic counter handing out per-request identifiers for
    /// `OffsetFetchRequestState` instances so the spawned response handler
    /// can locate the matching entry in `inflightOffsetFetches`. Java uses
    /// object identity; the Rust translation needs an explicit id.
    next_request_id: AtomicU64,
    state: Mutex<CommitRequestManagerState>,
    /// `Arc<CoordinatorRequestManager>` set via [`CommitRequestManager::set_coordinator`]
    /// at consumer construction time. Java holds this as a direct
    /// field on `CommitRequestManager`
    /// (`CommitRequestManager.java:148` — `coordinatorRequestManager`).
    ///
    /// Read-paths (response handlers, retry drivers) call
    /// [`CoordinatorRequestManager::mark_coordinator_unknown`] on
    /// `NotCoordinator`/`CoordinatorNotAvailable` errors so the next
    /// bg-task `poll(now)` re-issues `FindCoordinator`. Mirrors Java's
    /// `OffsetFetchRequestState.onFailure` / `OffsetCommitRequestState.onResponse`
    /// `coordinatorRequestManager.markCoordinatorUnknown(...)` calls
    /// (`CommitRequestManager.java:804,1092`).
    ///
    /// Set asynchronously after construction because both managers
    /// reference each other (Java does so in the same constructor by
    /// passing the coordinator in; in Rust the consumer-construction
    /// flow builds `coordinator` then `commit`, then calls the setter).
    coordinator: Mutex<Option<Arc<CoordinatorRequestManager>>>,
    /// Type-erased hook into the `OffsetCommitCallbackInvoker` so the
    /// auto-commit success path can enqueue an interceptor `on_commit`
    /// invocation. Mirrors Java's `offsetCommitCallbackInvoker` field on
    /// `CommitRequestManager`, used by `autoCommitCallback` (Java
    /// `CommitRequestManager.java:380`). Type-erased because the Rust
    /// commit manager is not generic over `<K, V>` (see
    /// [`AutoCommitInterceptorHook`]). Wired post-construction via
    /// [`CommitRequestManager::set_auto_commit_interceptor_hook`].
    auto_commit_interceptor_hook: Mutex<Option<Arc<dyn AutoCommitInterceptorHook>>>,
}

/// Mutable runtime state. Held behind `Mutex<...>` so the BG-task `poll`
/// path and the app-side `commit_*` / `fetch_offsets` calls can interleave.
struct CommitRequestManagerState {
    pending: PendingRequests,
    auto_commit: Option<AutoCommitState>,
    member_info: MemberInfo,
    /// Last epoch sent in a commit request — diagnostic only. Mirrors
    /// Java's `lastEpochSentOnCommit`.
    last_epoch_sent_on_commit: Option<i32>,
}

/// Holds unsent commits + fetches + inflight fetches. Java:
/// `CommitRequestManager.PendingRequests`.
struct PendingRequests {
    unsent_offset_commits: VecDeque<OffsetCommitRequestState>,
    unsent_offset_fetches: Vec<OffsetFetchRequestState>,
    inflight_offset_fetches: Vec<OffsetFetchRequestState>,
}

impl PendingRequests {
    fn new() -> Self {
        Self {
            unsent_offset_commits: VecDeque::new(),
            unsent_offset_fetches: Vec::new(),
            inflight_offset_fetches: Vec::new(),
        }
    }

    fn has_unsent_requests(&self) -> bool {
        !self.unsent_offset_commits.is_empty() || !self.unsent_offset_fetches.is_empty()
    }
}

impl CommitRequestManager {
    /// Construct a new `CommitRequestManager`.
    ///
    /// Mirrors Java's primary constructor minus the `Metrics`,
    /// `AsyncConsumerMetrics`, and `CoordinatorRequestManager` parameters
    /// (the last is consumed at `poll` time via `Arc<Mutex<...>>` — Java
    /// holds a direct reference; we hold an `Arc<Mutex<...>>` so the BG
    /// task can mutate both managers).
    pub(crate) fn new(
        config: &ConsumerConfig,
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        group_id: impl Into<String>,
        group_instance_id: Option<String>,
        now_ms: i64,
    ) -> Self {
        let auto_commit = if config.enable_auto_commit() {
            Some(AutoCommitState::new(now_ms, config.auto_commit_interval_ms() as i64))
        } else {
            None
        };
        let state = CommitRequestManagerState {
            pending: PendingRequests::new(),
            auto_commit,
            member_info: MemberInfo::default(),
            last_epoch_sent_on_commit: None,
        };
        let inner = Arc::new(CommitRequestManagerInner {
            group_id: group_id.into(),
            group_instance_id,
            retry_backoff_ms: config.retry_backoff_ms(),
            retry_backoff_max_ms: config.retry_backoff_max_ms(),
            throw_on_fetch_stable_offset_unsupported: config.throw_on_fetch_stable_offset_unsupported(),
            metadata,
            subscriptions,
            closing: Mutex::new(false),
            next_request_id: AtomicU64::new(0),
            state: Mutex::new(state),
            coordinator: Mutex::new(None),
            auto_commit_interceptor_hook: Mutex::new(None),
        });
        Self { inner }
    }

    /// Wire up the [`CoordinatorRequestManager`] handle that response
    /// handlers and retry drivers use to call
    /// [`CoordinatorRequestManager::mark_coordinator_unknown`] on
    /// `NotCoordinator`/`CoordinatorNotAvailable` errors.
    ///
    /// Mirrors Java's direct field reference set by the
    /// `CommitRequestManager` constructor — in Rust the two managers
    /// reference each other through `Arc`s, so the consumer wires them
    /// up after both are constructed.
    pub(crate) fn set_coordinator(&self, coordinator: Arc<CoordinatorRequestManager>) {
        let mut guard = self.inner.coordinator.lock().expect("commit manager coordinator slot poisoned");
        *guard = Some(coordinator);
    }

    /// Wire up the [`AutoCommitInterceptorHook`] (the type-erased
    /// `OffsetCommitCallbackInvoker`) so the auto-commit success path can
    /// enqueue an interceptor `on_commit` invocation. Mirrors Java passing
    /// `offsetCommitCallbackInvoker` into the `CommitRequestManager`
    /// constructor; in Rust it is wired post-construction because the
    /// invoker is generic and the commit manager is not.
    pub(crate) fn set_auto_commit_interceptor_hook(&self, hook: Arc<dyn AutoCommitInterceptorHook>) {
        let mut guard = self
            .inner
            .auto_commit_interceptor_hook
            .lock()
            .expect("commit manager auto-commit interceptor hook poisoned");
        *guard = Some(hook);
    }

    /// Returns a new `CommitRequestManager` handle that **shares** the
    /// same `Arc<CommitRequestManagerInner>` state as `self`. Both
    /// handles read and write through the same `Mutex`-guarded
    /// runtime state, so all method calls on either handle observe the
    /// same view (auto-commit timer, in-flight commits, pending fetches,
    /// etc.).
    ///
    /// Mirrors Java's reference-sharing: `AsyncKafkaConsumer.java` builds
    /// one `CommitRequestManager` and passes the same reference to
    /// `RequestManagers` (`Optional<CommitRequestManager>`) and to
    /// `ConsumerMembershipManager` (which holds it as a member field for
    /// `maybeAutoCommitSyncBeforeRebalance`). In Rust the membership
    /// manager wants `Option<Arc<CommitRequestManager>>` and
    /// `RequestManagers` wants `Option<CommitRequestManager>` (owned),
    /// so we hand `share()` to one side and the original handle to the
    /// other.
    pub(crate) fn share(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }

    /// Returns `true` if auto-commit is enabled. Mirrors Java's
    /// `autoCommitEnabled()`.
    pub(crate) fn auto_commit_enabled(&self) -> bool {
        let guard = self.inner.state.lock().expect("commit manager state poisoned");
        guard.auto_commit.is_some()
    }

    /// Reset the auto-commit timer to the auto-commit interval from
    /// `now_ms`. Mirrors Java's `resetAutoCommitTimer()`.
    pub(crate) fn reset_auto_commit_timer(&self, now_ms: i64) {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        if let Some(ac) = guard.auto_commit.as_mut() {
            ac.reset_timer(now_ms);
        }
    }

    /// Reset the auto-commit timer to a caller-supplied backoff. Mirrors
    /// Java's overloaded `resetAutoCommitTimer(long retryBackoffMs)`.
    pub(crate) fn reset_auto_commit_timer_with_backoff(&self, now_ms: i64, retry_backoff_ms: i64) {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        if let Some(ac) = guard.auto_commit.as_mut() {
            ac.reset_timer_with_backoff(now_ms, retry_backoff_ms);
        }
    }

    /// Refreshes the auto-commit interval timer with the current time and
    /// fires an auto-commit if the timer has expired and no commit is
    /// in-flight. Mirrors Java's
    /// [`CommitRequestManager.updateTimerAndMaybeCommit`].
    ///
    /// Java calls `updateAutoCommitTimer(currentTimeMs)` followed by
    /// `maybeAutoCommitAsync()`. In the Rust translation the timer-refresh
    /// step is implicit: every query method ([`Self::maximum_time_to_wait`],
    /// [`AutoCommitState::should_auto_commit`], etc.) takes
    /// `current_time_ms` as an explicit parameter, so there is no stateful
    /// `Timer.update(...)` call to make. This method is therefore a thin
    /// pass-through to the existing `maybe_auto_commit_async` driver.
    ///
    /// Used by `ApplicationEventProcessor` for `AsyncPoll` and
    /// `AssignmentChange` events so the auto-commit interval is honoured at
    /// event-dispatch time rather than only inside the bg-task `poll`.
    /// Takes `&self` (not `&mut self`) because all mutation flows through
    /// the interior `Arc<CommitRequestManagerInner>` `Mutex` slots. The
    /// shape change is required for Phase-12 production wire-up where the
    /// commit manager is held as `Arc<CommitRequestManager>` and shared
    /// with `ConsumerMembershipManager`.
    pub(crate) fn update_timer_and_maybe_commit(&self, current_time_ms: i64) {
        // Java: updateTimerAndMaybeCommit — ensures the auto-commit timer
        // reflects the latest poll/event tick before potentially firing.
        self.maybe_auto_commit_async(current_time_ms);
    }

    /// `true` after [`Self::signal_close`] has been called. Mirrors the
    /// observable side of Java's `closing` flag. Used by
    /// `ApplicationEventProcessor`'s tests to verify the `CommitOnClose`
    /// arm signalled correctly.
    pub(crate) fn is_closing(&self) -> bool {
        *self.inner.closing.lock().expect("commit manager closing flag poisoned")
    }

    /// Inherent `&self` variant of [`RequestManager::signal_close`] —
    /// needed for callers holding an `Arc<CommitRequestManager>` (e.g.
    /// the Phase-12 `ApplicationEventProcessor`, which acquires a shared
    /// handle via `RequestManagers::commit_handle`). Equivalent to the
    /// `RequestManager::signal_close` trait method body but operates
    /// through interior mutability on `self.inner.closing` so an `&Arc`
    /// handle is sufficient.
    pub(crate) fn signal_close_shared(&self) {
        let mut guard = self.inner.closing.lock().expect("commit manager closing flag poisoned");
        *guard = true;
    }

    // ---------------------------------------------------------------------
    //                       MemberStateListener wiring
    // ---------------------------------------------------------------------

    // Java: `public class CommitRequestManager implements RequestManager,
    // MemberStateListener`. The trait impl lives at the bottom of this
    // module ([`impl MemberStateListener for CommitRequestManager`]) and
    // delegates to the inherent `on_member_epoch_updated` method below.

    /// Update the latest member epoch and id. Mirrors Java's
    /// `onMemberEpochUpdated(Optional<Integer>, String)`.
    pub(crate) fn on_member_epoch_updated(&self, new_epoch: Option<i32>, new_member_id: String) {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        let old_epoch = guard.member_info.member_epoch;
        if new_epoch.is_none() && old_epoch.is_some() {
            log::info!(
                "Member {} won't include epoch in following offset commit/fetch requests because it has left the group.",
                guard.member_info.member_id
            );
        } else if let Some(e) = new_epoch {
            log::debug!(
                "Member {} will include new member epoch {} in following offset commit/fetch requests.",
                new_member_id,
                e
            );
        }
        guard.member_info.member_id = new_member_id;
        guard.member_info.member_epoch = new_epoch;
    }

    /// Diagnostic accessor — Java: `lastEpochSentOnCommit()`.
    pub(crate) fn last_epoch_sent_on_commit(&self) -> Option<i32> {
        let guard = self.inner.state.lock().expect("commit manager state poisoned");
        guard.last_epoch_sent_on_commit
    }

    // ---------------------------------------------------------------------
    //                            commit_sync
    // ---------------------------------------------------------------------

    /// Commit the supplied offsets with retry on retriable errors until
    /// `deadline_ms`. Mirrors Java's `commitSync(Map, long)`.
    ///
    /// Returns a `oneshot::Receiver` resolving to the committed offsets on
    /// success or a [`KafkaError`] on failure. Callers `.await` it.
    ///
    /// An empty `offsets` map resolves the future immediately to `Ok({})`.
    pub(crate) fn commit_sync(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        deadline_ms: i64,
        now_ms: i64,
    ) -> oneshot::Receiver<CommitResult> {
        let (tx, rx) = oneshot::channel();
        if offsets.is_empty() {
            let _ = tx.send(Ok(HashMap::new()));
            return rx;
        }
        self.maybe_update_last_seen_epoch_if_newer(&offsets);
        let member_info = {
            let guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.member_info.clone()
        };
        // Clone the offsets so the retry driver can recreate the request
        // state on retriable failures. Java's `commitSyncWithRetries`
        // reuses the same `OffsetCommitRequestState` via `resetFuture()`;
        // in Rust the network send path consumes the state, so we keep
        // a canonical copy here for retries.
        let offsets_for_retry = offsets.clone();
        let (request, request_rx) = OffsetCommitRequestState::new(
            offsets,
            member_info.clone(),
            self.inner.retry_backoff_ms,
            self.inner.retry_backoff_max_ms,
            deadline_ms,
            now_ms,
        );
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.pending.unsent_offset_commits.push_back(request);
        }
        let result_tx = Arc::new(Mutex::new(Some(tx)));
        let inner = Arc::clone(&self.inner);
        // Spawn a task that resolves the public future based on the
        // commit request's response, with sync-style retry on retriable
        // errors. Java models this via `whenComplete` on the
        // `CompletableFuture`; in Rust we drive the same logic with a
        // `tokio::spawn` reading the internal `oneshot::Receiver`.
        tokio::spawn(async move {
            commit_sync_with_retries(
                inner,
                request_rx,
                result_tx,
                offsets_for_retry,
                member_info,
                deadline_ms,
                now_ms,
            )
            .await;
        });
        rx
    }

    // ---------------------------------------------------------------------
    //                maybe_auto_commit_sync_before_rebalance
    // ---------------------------------------------------------------------

    /// Commit `subscriptions.allConsumed()` synchronously if auto-commit is
    /// enabled, retrying on retriable errors until `deadline_ms`. Mirrors
    /// Java's
    /// `CommitRequestManager.maybeAutoCommitSyncBeforeRebalance(deadlineMs)`.
    ///
    /// Used by the membership reconciliation pipeline to flush pending
    /// offsets before partitions are reassigned. Behaviour:
    ///
    /// - If auto-commit is disabled, resolves immediately to `Ok(())`.
    /// - If auto-commit is enabled, captures
    ///   `subscriptions.allConsumed()` and drives the commit with retry.
    /// - Considers [`Errors::StaleMemberEpoch`] retriable (mirrors Java's
    ///   `isStaleEpochErrorAndValidEpochAvailable`).
    /// - Considers [`Errors::UnknownTopicOrPartition`] **fatal** (early
    ///   exit), even though the error otherwise extends `RetriableError`.
    ///   Rationale (Java doc):  if a topic or partition is deleted, the
    ///   rebalance wouldn't finish in time since the auto commit would
    ///   keep retrying.
    /// - On deadline expiry after a retriable error, wraps the final
    ///   error as a [`KafkaError::timeout`] (Java:
    ///   `maybeWrapAsTimeoutException`).
    ///
    /// Returns a `oneshot::Receiver` resolving to `Ok(())` on success or
    /// the surfaced [`KafkaError`] on failure. Callers `.await` it.
    pub(crate) fn maybe_auto_commit_sync_before_rebalance(
        &self,
        deadline_ms: i64,
        now_ms: i64,
    ) -> oneshot::Receiver<Result<(), KafkaError>> {
        let (tx, rx) = oneshot::channel();
        // Java: `if (!autoCommitEnabled()) return CompletableFuture.completedFuture(null);`
        if !self.auto_commit_enabled() {
            let _ = tx.send(Ok(()));
            return rx;
        }
        // Snapshot `subscriptions.allConsumed()` exactly as Java's
        // `createOffsetCommitRequest(subscriptions.allConsumed(), deadlineMs)`.
        let offsets = {
            let guard = self.inner.subscriptions.lock().expect("subscriptions poisoned");
            guard.all_consumed()
        };
        if offsets.is_empty() {
            // Java's `requestAutoCommit` resolves the future immediately
            // when there are no offsets to commit.
            let _ = tx.send(Ok(()));
            return rx;
        }
        self.maybe_update_last_seen_epoch_if_newer(&offsets);
        let member_info = {
            let guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.member_info.clone()
        };
        // Track inflight (Java's `requestAutoCommit` flips the
        // `inflightCommit` flag via the auto-commit state).
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            if let Some(ac) = guard.auto_commit.as_mut() {
                ac.set_inflight_commit_status(true);
            }
        }
        let offsets_for_retry = offsets.clone();
        let (request, request_rx) = OffsetCommitRequestState::new(
            offsets,
            member_info.clone(),
            self.inner.retry_backoff_ms,
            self.inner.retry_backoff_max_ms,
            deadline_ms,
            now_ms,
        );
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.pending.unsent_offset_commits.push_back(request);
        }
        let result_tx = Arc::new(Mutex::new(Some(tx)));
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            auto_commit_sync_before_rebalance_with_retries(
                inner,
                request_rx,
                result_tx,
                offsets_for_retry,
                deadline_ms,
                now_ms,
            )
            .await;
        });
        rx
    }

    // ---------------------------------------------------------------------
    //                            commit_async
    // ---------------------------------------------------------------------

    /// Commit the supplied offsets without retry. Mirrors Java's
    /// `commitAsync(Map)` plus the subsequent
    /// `OffsetCommitCallbackInvoker::enqueueUserCallbackInvocation` wiring
    /// in `AsyncKafkaConsumer.commitAsync` — combined here for the same
    /// reason Java keeps the callback dispatch close to the commit
    /// surface.
    ///
    /// Returns a `oneshot::Receiver` resolving to the offsets that were
    /// just enqueued (matching Java's behaviour of resolving the
    /// `asyncCommitResult` with the input offsets on success).
    ///
    /// If `callback` is `Some(...)`, the callback is enqueued on
    /// `invoker` for invocation on the application task when the commit
    /// completes (or fails). The invoker drains the queue on the next
    /// `poll() / commit_*() / close()` call — see
    /// consumer-threading.md §31.
    pub(crate) fn commit_async<K: Send + 'static, V: Send + 'static>(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Option<Arc<dyn OffsetCommitCallback>>,
        invoker: Arc<OffsetCommitCallbackInvoker<K, V>>,
        now_ms: i64,
    ) -> oneshot::Receiver<CommitResult> {
        // Two-stage chain: the bare `commit_async_no_callback` does the
        // actual commit work (mirrors Java's `commitAsync(Map)` on
        // `CommitRequestManager`). On top of it we layer callback /
        // interceptor enqueueing — Java's `AsyncKafkaConsumer.commitAsync`
        // does the same via `whenComplete` on the future returned by the
        // manager. Keeping the two stages separate lets the bg-task
        // processor invoke the bare commit without owning a generic
        // `OffsetCommitCallbackInvoker<K, V>`.
        let inner_rx = self.commit_async_no_callback(offsets.clone(), now_ms);
        let (tx, rx) = oneshot::channel();
        let result_tx = Arc::new(Mutex::new(Some(tx)));
        let offsets_for_callback = offsets;
        tokio::spawn(async move {
            let outcome = inner_rx.await;
            let (success_value, callback_err) = match outcome {
                Ok(Ok(_committed_offsets)) => (Some(offsets_for_callback.clone()), None),
                Ok(Err(err)) => (None, Some(err)),
                Err(_recv_err) => (None, Some(KafkaError::new(Errors::UnknownServerError))),
            };

            // Mirror Java's AsyncKafkaConsumer.commitAsync (lines
            // 1019-1032): on success, enqueue interceptor invocation
            // FIRST, then the user callback. FIFO queue + same drain
            // order → interceptors fire BEFORE user callback.
            if callback_err.is_none() {
                invoker.enqueue_interceptor_invocation(offsets_for_callback.clone());
            }
            if let Some(cb) = callback {
                invoker.enqueue_user_callback_invocation(cb, offsets_for_callback.clone(), callback_err.clone());
            }

            // Resolve the public future.
            let mut guard = result_tx.lock().expect("commit_async tx poisoned");
            if let Some(tx) = guard.take() {
                let _ = tx.send(match (success_value, callback_err) {
                    (Some(value), None) => Ok(value),
                    (None, Some(err)) => Err(err),
                    _ => unreachable!("commit_async outcome must be ok-or-err"),
                });
            }
        });
        rx
    }

    /// Bare async-commit primitive — mirrors Java's bare
    /// `CommitRequestManager.commitAsync(Map<TopicPartition, OffsetAndMetadata>)`.
    ///
    /// Does NOT enqueue any user callback or interceptor — that wiring
    /// lives in `AsyncKafkaConsumer.commitAsync` on the Java side, and in
    /// the [`Self::commit_async`] wrapper on the Rust side. The
    /// bg-task `ApplicationEventProcessor` calls this method directly
    /// (Java's processor does the same: `manager.commitAsync(offsets)`).
    ///
    /// Returns a `oneshot::Receiver` resolving to the committed offsets on
    /// success or a [`KafkaError`] on failure. Retriable errors are
    /// wrapped with `RetriableCommitFailedException` to match Java's
    /// `commitAsyncExceptionForError`.
    pub(crate) fn commit_async_no_callback(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        now_ms: i64,
    ) -> oneshot::Receiver<CommitResult> {
        let (tx, rx) = oneshot::channel();
        if offsets.is_empty() {
            log::debug!("Skipping commit of empty offsets");
            let _ = tx.send(Ok(HashMap::new()));
            return rx;
        }
        self.maybe_update_last_seen_epoch_if_newer(&offsets);
        let member_info = {
            let guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.member_info.clone()
        };
        let offsets_for_result = offsets.clone();
        let (request, request_rx) = OffsetCommitRequestState::new(
            offsets,
            member_info,
            self.inner.retry_backoff_ms,
            self.inner.retry_backoff_max_ms,
            i64::MAX, // commit_async never expires per Java (no deadline).
            now_ms,
        );
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.pending.unsent_offset_commits.push_back(request);
        }
        let result_tx = Arc::new(Mutex::new(Some(tx)));
        tokio::spawn(async move {
            // Resolve the public future based on the commit request's
            // response. Java wraps retriable errors with
            // `RetriableCommitFailedException` for the async path.
            let outcome = request_rx.await;
            let resolved: CommitResult = match outcome {
                Ok(Ok(_committed_offsets)) => Ok(offsets_for_result),
                Ok(Err(err)) => {
                    let mapped = if err.is_retriable() {
                        KafkaError::from(ConsumerError::retriable_commit_failed_with_cause(err))
                    } else {
                        err
                    };
                    Err(mapped)
                },
                Err(_recv_err) => {
                    // Sender dropped without sending — treat as a generic
                    // failure. This should not happen in steady state.
                    Err(KafkaError::new(Errors::UnknownServerError))
                },
            };

            let mut guard = result_tx.lock().expect("commit_async_no_callback tx poisoned");
            if let Some(tx) = guard.take() {
                let _ = tx.send(resolved);
            }
        });
        rx
    }

    // ---------------------------------------------------------------------
    //                       fetch_offsets / init helper
    // ---------------------------------------------------------------------

    /// Fetch committed offsets for the given partitions, retrying on
    /// retriable errors until `deadline_ms`. Mirrors Java's
    /// `fetchOffsets(Set, long)`.
    ///
    /// Returns a `oneshot::Receiver` resolving to a map keyed by partition,
    /// with `None` values for partitions that had no committed offset.
    pub(crate) fn fetch_offsets(
        &self,
        partitions: HashSet<TopicPartition>,
        deadline_ms: i64,
        now_ms: i64,
    ) -> oneshot::Receiver<FetchResult> {
        let (tx, rx) = oneshot::channel();
        if partitions.is_empty() {
            let _ = tx.send(Ok(HashMap::new()));
            return rx;
        }
        let member_info = {
            let guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.member_info.clone()
        };
        let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        // Preserve the requested partition set so the retry driver can
        // rebuild a fresh `OffsetFetchRequestState` on retriable errors
        // (Java reuses the same state object via `resetFuture()`; the
        // Rust send path consumes it).
        let requested_partitions = partitions.clone();
        let (request, request_rx) = OffsetFetchRequestState::new(
            request_id,
            partitions,
            member_info,
            self.inner.retry_backoff_ms,
            self.inner.retry_backoff_max_ms,
            deadline_ms,
            now_ms,
        );
        // Try to dedupe against an unsent or in-flight identical request
        // — Java does this inside `PendingRequests.addOffsetFetchRequest`.
        // Dedup is best-effort: if a duplicate is found we still enqueue
        // a *fresh* request because the public `oneshot::Receiver` would
        // not naturally chain with the in-flight one (Java chains
        // CompletableFutures; we keep one request per call for simpler
        // semantics — duplicate fetches are wasted bytes, never wrong).
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.pending.unsent_offset_fetches.push(request);
        }
        let result_tx = Arc::new(Mutex::new(Some(tx)));
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            fetch_offsets_with_retries(inner, request_rx, result_tx, requested_partitions, deadline_ms, now_ms).await;
        });
        rx
    }

    /// Test-only accessor exposing the current count of unsent
    /// `OffsetFetch` requests on the pending queue. Used by sibling
    /// modules' tests (e.g.
    /// `OffsetsRequestManager::init_with_committed_offsets_if_needed`)
    /// to verify request issuance without reaching into private state.
    #[cfg(test)]
    pub(crate) fn inner_state_for_test(&self) -> usize {
        let guard = self.inner.state.lock().expect("commit manager state poisoned");
        guard.pending.unsent_offset_fetches.len()
    }

    /// Test-only accessor exposing a clone of the current `MemberInfo`
    /// (member id + member epoch). Used by sibling-module tests
    /// (e.g. `async_kafka_consumer` Issue-7 regression) to verify that
    /// `MemberStateListener::on_member_epoch_updated` writes propagated
    /// into the commit manager's member-info slot — which is read at
    /// `OffsetCommitRequest` build time. Without a working registration
    /// chain the `member_id` stays at the default empty string and the
    /// broker rejects commits with `UNKNOWN_MEMBER_ID`.
    #[cfg(test)]
    pub(crate) fn member_info_for_test(&self) -> MemberInfo {
        let guard = self.inner.state.lock().expect("commit manager state poisoned");
        guard.member_info.clone()
    }

    /// Test-only helper: pop the first unsent `OffsetFetch` request and
    /// resolve its underlying `oneshot::Sender` with the given offset
    /// map. Used by sibling-module tests
    /// (`OffsetsRequestManager::update_fetch_positions`) to drive the
    /// committed-offset response branch without standing up a full
    /// network client.
    ///
    /// Returns `true` if a pending fetch was found and completed,
    /// `false` if the queue was empty.
    #[cfg(test)]
    pub(crate) fn complete_first_unsent_fetch_for_test(
        &self,
        offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>>,
    ) -> bool {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        if guard.pending.unsent_offset_fetches.is_empty() {
            return false;
        }
        let request = guard.pending.unsent_offset_fetches.remove(0);
        drop(guard);
        request.complete_ok(offsets);
        true
    }

    /// Test-only helper: pop the first unsent `OffsetCommit` request and
    /// resolve its underlying `oneshot::Sender` with the given offset
    /// map. Used by sibling-module tests
    /// (e.g. `ApplicationEventProcessorTest` happy-path SyncCommit /
    /// AsyncCommit translations) to drive the commit response without
    /// standing up a full network client — Java's equivalent tests stub
    /// `CommitRequestManager::commitSync` / `::commitAsync` via Mockito.
    ///
    /// Returns `true` if a pending commit was found and completed,
    /// `false` if the queue was empty.
    #[cfg(test)]
    pub(crate) fn complete_first_unsent_commit_for_test(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> bool {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        let Some(request) = guard.pending.unsent_offset_commits.pop_front() else {
            return false;
        };
        drop(guard);
        request.complete_ok(offsets);
        true
    }

    /// Test-only helper: pop the first unsent `OffsetCommit` request and
    /// fail its `oneshot::Sender` with the given error. Sibling to
    /// [`Self::complete_first_unsent_commit_for_test`].
    #[cfg(test)]
    pub(crate) fn fail_first_unsent_commit_for_test(&self, err: KafkaError) -> bool {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        let Some(request) = guard.pending.unsent_offset_commits.pop_front() else {
            return false;
        };
        drop(guard);
        request.complete_err(err);
        true
    }

    // ---------------------------------------------------------------------
    //                              close path
    // ---------------------------------------------------------------------

    /// Drain remaining unsent commit requests for the close path. Java:
    /// `drainPendingOffsetCommitRequests()`.
    pub(crate) fn drain_pending_offset_commit_requests(&self) -> PollResult {
        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        if guard.pending.unsent_offset_commits.is_empty() {
            return PollResult::empty();
        }
        let mut unsent = Vec::with_capacity(guard.pending.unsent_offset_commits.len());
        while let Some(req) = guard.pending.unsent_offset_commits.pop_front() {
            unsent.push(req);
        }
        drop(guard);
        let inner = Arc::clone(&self.inner);
        let requests = unsent
            .into_iter()
            .map(|r| build_offset_commit_unsent_request(&inner, r))
            .collect::<Vec<_>>();
        PollResult::new(i64::MAX, requests)
    }

    // ---------------------------------------------------------------------
    //                          private helpers
    // ---------------------------------------------------------------------

    /// Propagate the leader epoch from each [`OffsetAndMetadata`] into the
    /// `Metadata` cache (Java: `maybeUpdateLastSeenEpochIfNewer`).
    fn maybe_update_last_seen_epoch_if_newer(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
        for (tp, oam) in offsets {
            if let Some(epoch) = oam.leader_epoch() {
                // Java: best-effort, no failure path. The cache treats
                // out-of-order epochs as a no-op via the result.
                let _ = self.inner.metadata.metadata_arc().update_last_seen_epoch_if_newer(tp, epoch);
            }
        }
    }
}

// =========================================================================
//                  Coordinator wiring (helpers parameterised
//                  on the bg-task-owned CoordinatorRequestManager)
// =========================================================================

impl CommitRequestManager {
    /// Drives one `poll` step, computing the next set of unsent requests
    /// and the `PollResult`. The caller passes a mutable `&mut` to the
    /// coordinator request manager so failures can be propagated.
    ///
    /// Takes `&self` despite mutating internal state — all mutation goes
    /// through interior mutability (`Arc<CommitRequestManagerInner>` whose
    /// `state` / `closing` fields are `Mutex`-guarded). This lets the
    /// bg-task call into `poll_with_coordinator` through the shared
    /// `Arc<CommitRequestManager>` handle (`RequestManagers::commit_handle`)
    /// without needing exclusive ownership — matching the Java pattern
    /// where `requestManagers.entries()` iterates immutable references.
    ///
    /// Mirrors Java's `poll(long currentTimeMs)`.
    pub(crate) fn poll_with_coordinator(
        &self,
        coordinator: &CoordinatorRequestManager,
        current_time_ms: i64,
    ) -> PollResult {
        let closing = *self.inner.closing.lock().expect("commit manager closing flag poisoned");

        // Java: if coordinator is unknown, fail unsent commits if closing.
        if coordinator.coordinator().is_none() {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            if let Some(err) = coordinator.fatal_error() {
                Self::fail_all_with_error(&mut guard.pending, err);
            }
            if closing && guard.pending.has_unsent_requests() {
                let commit_failed: KafkaError = ConsumerError::commit_failed(
                    "Failed to commit offsets: Coordinator unknown and consumer is closing",
                )
                .into();
                Self::drain_pending_commits_with_error(&mut guard.pending, commit_failed);
            }
            return PollResult::empty();
        }

        if closing {
            // Java: drainPendingOffsetCommitRequests().
            return self.drain_pending_offset_commit_requests();
        }

        // Auto-commit firing — drain timer.
        self.maybe_auto_commit_async(current_time_ms);

        let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
        if !guard.pending.has_unsent_requests() {
            return PollResult::empty();
        }
        // Drain unsent commits and fetches that can send now.
        let inner = Arc::clone(&self.inner);
        let mut to_send: Vec<UnsentRequest> = Vec::new();
        // Commits.
        let mut commits = std::mem::take(&mut guard.pending.unsent_offset_commits);
        let mut requeue_commits: VecDeque<OffsetCommitRequestState> = VecDeque::new();
        while let Some(mut commit) = commits.pop_front() {
            // Expire any commits whose deadline has passed and at least
            // one send attempt has been made (Java: maybeExpire).
            if commit.has_attempted_send && commit.state.is_expired(current_time_ms) {
                let desc = format!("OffsetCommit request for offsets {:?}", commit.offsets);
                let err = KafkaError::timeout(format!("{desc} could not complete before timeout expired."));
                commit.complete_err(err);
                continue;
            }
            if commit.state.can_send_request(current_time_ms) {
                commit.state.on_send_attempt(current_time_ms);
                commit.has_attempted_send = true;
                // Refresh the request's member id/epoch from the manager's
                // CURRENT member info at SEND time. In Java the request holds a
                // reference to the manager's mutable `MemberInfo`
                // (`CommitRequestManager.java:889`), so an `onMemberEpochUpdated`
                // that fires between enqueue and send is reflected in the
                // built request. The Rust request stored a clone at enqueue, so
                // we re-sync it here before building (and write
                // `lastEpochSentOnCommit` — Java does the same inside
                // `toOffsetCommitRequestData`, line 746).
                commit.member_info = guard.member_info.clone();
                guard.last_epoch_sent_on_commit = commit.member_info.member_epoch;
                to_send.push(build_offset_commit_unsent_request(&inner, commit));
            } else {
                requeue_commits.push_back(commit);
            }
        }
        guard.pending.unsent_offset_commits = requeue_commits;

        // Fetches.
        let mut fetches = std::mem::take(&mut guard.pending.unsent_offset_fetches);
        let mut requeue_fetches: Vec<OffsetFetchRequestState> = Vec::new();
        let mut inflight_to_add: Vec<OffsetFetchRequestState> = Vec::new();
        for mut fetch in fetches.drain(..) {
            if fetch.state.can_send_request(current_time_ms) {
                fetch.state.on_send_attempt(current_time_ms);
                let unsent = build_offset_fetch_unsent_request(&inner, &mut fetch);
                to_send.push(unsent);
                inflight_to_add.push(fetch);
            } else {
                requeue_fetches.push(fetch);
            }
        }
        guard.pending.unsent_offset_fetches = requeue_fetches;
        guard.pending.inflight_offset_fetches.extend(inflight_to_add);

        // Compute next-poll time from the min remaining backoff.
        let mut next_poll = i64::MAX;
        for r in &guard.pending.unsent_offset_commits {
            next_poll = next_poll.min(r.state.remaining_backoff_ms(current_time_ms));
        }
        for r in &guard.pending.unsent_offset_fetches {
            next_poll = next_poll.min(r.state.remaining_backoff_ms(current_time_ms));
        }
        PollResult::new(next_poll, to_send)
    }

    fn maybe_auto_commit_async(&self, current_time_ms: i64) {
        // Java: `maybeAutoCommitAsync()` — only fires when autoCommit enabled
        // AND timer expired AND no in-flight commit. Then snapshots
        // `subscriptions.allConsumed()`, enqueues an `OffsetCommitRequestState`
        // (deadline = `Long.MAX_VALUE`), resets the interval timer, and on
        // a retriable failure resets the timer with `retry_backoff_ms`
        // (Java's `maybeResetTimerWithBackoff`).
        let should_fire = {
            let guard = self.inner.state.lock().expect("commit manager state poisoned");
            match guard.auto_commit.as_ref() {
                Some(ac) => ac.should_auto_commit(current_time_ms),
                None => false,
            }
        };
        if !should_fire {
            return;
        }
        // Snapshot `subscriptions.allConsumed()` (Java:
        // `createOffsetCommitRequest(subscriptions.allConsumed(), Long.MAX_VALUE)`).
        let offsets = {
            let guard = self.inner.subscriptions.lock().expect("subscriptions poisoned");
            guard.all_consumed()
        };
        // Java still resets the timer when no offsets are committed (the
        // `resetAutoCommitTimer()` call in `maybeAutoCommitAsync` runs
        // unconditionally after `requestAutoCommit`). Reset before the
        // empty-short-circuit so the next interval fires on schedule.
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            if let Some(ac) = guard.auto_commit.as_mut() {
                ac.reset_timer(current_time_ms);
            }
        }
        if offsets.is_empty() {
            // Java's `requestAutoCommit` resolves with an empty map; no
            // request is enqueued and the inflight flag is never raised.
            return;
        }
        self.maybe_update_last_seen_epoch_if_newer(&offsets);
        let member_info = {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            if let Some(ac) = guard.auto_commit.as_mut() {
                ac.set_inflight_commit_status(true);
            }
            guard.member_info.clone()
        };
        // Snapshot the offsets so the auto-commit success arm can enqueue
        // the interceptor invocation with the committed offsets (Java's
        // `autoCommitCallback(allConsumedOffsets)`).
        let offsets_for_interceptor = offsets.clone();
        let (request, request_rx) = OffsetCommitRequestState::new(
            offsets,
            member_info,
            self.inner.retry_backoff_ms,
            self.inner.retry_backoff_max_ms,
            i64::MAX,
            current_time_ms,
        );
        {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            guard.pending.unsent_offset_commits.push_back(request);
        }
        // Java: `maybeResetTimerWithBackoff` — on a retriable failure
        // reset the auto-commit timer with `retry_backoff_ms`. Also
        // clears the `inflightCommitStatus` flag regardless of outcome
        // (Java's `autoCommitCallback` BiConsumer in `requestAutoCommit`).
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let outcome = request_rx.await;
            // Clear the inflight flag and (on a retriable failure) reset the
            // auto-commit timer with backoff under a single critical section,
            // then drop the guard before firing the interceptor hook (which
            // takes its own lock inside the invoker).
            {
                let mut guard = inner.state.lock().expect("commit manager state poisoned");
                if let Some(ac) = guard.auto_commit.as_mut() {
                    ac.set_inflight_commit_status(false);
                }
                // Java's `maybeResetTimerWithBackoff`: on a retriable failure
                // reset the auto-commit timer with `retry_backoff_ms`.
                let is_retriable_failure = matches!(&outcome, Ok(Err(err)) if err.is_retriable());
                if let (true, Some(ac)) = (is_retriable_failure, guard.auto_commit.as_mut()) {
                    ac.reset_timer_with_backoff(current_time_ms, inner.retry_backoff_ms);
                }
            }
            match outcome {
                Ok(Ok(_committed)) => {
                    // Java `autoCommitCallback`: on success, enqueue the
                    // interceptor `on_commit` invocation with the committed
                    // offsets (`CommitRequestManager.java:380`).
                    inner.enqueue_interceptor_invocation(offsets_for_interceptor);
                    log::debug!("Completed asynchronous auto-commit of offsets");
                },
                Ok(Err(err)) => {
                    if err.is_retriable() {
                        log::debug!("Asynchronous auto-commit of offsets failed due to retriable error: {err}");
                    } else {
                        log::debug!("Asynchronous auto-commit of offsets failed: {err}");
                    }
                },
                Err(_) => {
                    log::debug!("Asynchronous auto-commit channel closed without a result");
                },
            }
        });
    }

    fn fail_all_with_error(pending: &mut PendingRequests, err: KafkaError) {
        log::warn!("Failing all unsent commit requests and offset fetches because of coordinator fatal error: {err}");
        for r in pending.unsent_offset_commits.iter() {
            r.complete_err(err.clone());
        }
        for r in pending.unsent_offset_fetches.iter() {
            r.complete_err(err.clone());
        }
        pending.unsent_offset_commits.clear();
        pending.unsent_offset_fetches.clear();
    }

    fn drain_pending_commits_with_error(pending: &mut PendingRequests, err: KafkaError) {
        while let Some(r) = pending.unsent_offset_commits.pop_front() {
            r.complete_err(err.clone());
        }
    }
}

// =========================================================================
//                            RequestManager impl
// =========================================================================

impl RequestManager for CommitRequestManager {
    fn poll(&mut self, _current_time_ms: i64) -> PollResult {
        // The trait method doesn't carry the coordinator handle. Phase 10
        // will provide the wiring (the bg task owns both managers and can
        // call `poll_with_coordinator` directly). Until then, `poll`
        // returns empty so the manager is harmless if the bg task picks
        // it up before Phase 10.
        PollResult::empty()
    }

    fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
        // Same constraint as `poll` — Phase 10 wires
        // `drain_pending_offset_commit_requests` directly.
        PollResult::empty()
    }

    fn maximum_time_to_wait(&self, current_time_ms: i64) -> i64 {
        let guard = self.inner.state.lock().expect("commit manager state poisoned");
        guard
            .auto_commit
            .as_ref()
            .map(|ac| ac.remaining_ms(current_time_ms))
            .unwrap_or(i64::MAX)
    }

    fn signal_close(&mut self) {
        let mut guard = self.inner.closing.lock().expect("commit manager closing flag poisoned");
        *guard = true;
    }
}

// =========================================================================
//                       MemberStateListener impl
// =========================================================================

/// Mirrors Java's `class CommitRequestManager implements RequestManager,
/// MemberStateListener`. The listener body is a thin forward to the
/// inherent `on_member_epoch_updated` method (which carries the
/// "log + write to `MemberInfo`" implementation).
///
/// `on_group_assignment_updated` is left to the trait's default no-op
/// because Java does not override it on `CommitRequestManager`
/// (`CommitRequestManager.java:597-606` only implements
/// `onMemberEpochUpdated`).
impl MemberStateListener for CommitRequestManager {
    fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str) {
        // Forward to the inherent method which holds the existing
        // log + state-mutation logic. `String::from` matches the inherent
        // signature; the trait borrows the id, the inherent takes owned.
        CommitRequestManager::on_member_epoch_updated(self, member_epoch, String::from(member_id));
    }
}

// =========================================================================
//                  Request builders (consume the state object)
// =========================================================================

fn build_offset_commit_unsent_request(
    inner: &Arc<CommitRequestManagerInner>,
    request: OffsetCommitRequestState,
) -> UnsentRequest {
    let metadata = inner.metadata.metadata_arc();
    let topic_ids = metadata.topic_ids();
    let mut can_use_topic_ids = !topic_ids.is_empty();
    let mut request_topics: HashMap<String, OffsetCommitRequestTopic> = HashMap::new();

    for (tp, oam) in &request.offsets {
        let topic_id = topic_ids.get(tp.topic()).copied().unwrap_or_else(Uuid::zero);
        if topic_id == Uuid::zero() {
            can_use_topic_ids = false;
        }
        let topic_name = tp.topic().to_string();
        let topic = request_topics.entry(topic_name.clone()).or_insert_with(|| {
            let mut t = OffsetCommitRequestTopic::new();
            t.set_name(topic_name);
            t.set_topic_id(topic_id);
            t
        });
        let mut partition = OffsetCommitRequestPartition::new();
        partition.partition_index = tp.partition();
        partition.committed_offset = oam.offset();
        partition.committed_leader_epoch = oam.leader_epoch().unwrap_or(RECORD_BATCH_NO_PARTITION_LEADER_EPOCH);
        partition.committed_metadata = Some(oam.metadata().to_string());
        topic.partitions.push(partition);
    }

    let mut data = OffsetCommitRequestData::new();
    data.set_group_id(inner.group_id.clone());
    if let Some(g) = inner.group_instance_id.as_ref() {
        data.set_group_instance_id(Some(g.clone()));
    }
    data.set_topics(request_topics.into_values().collect());
    data.set_member_id(request.member_info.member_id.clone());
    if let Some(epoch) = request.member_info.member_epoch {
        data.set_generation_id_or_member_epoch(epoch);
    }

    // `last_epoch_sent_on_commit` writeback is intentionally NOT performed
    // here: this helper runs from inside `poll_with_coordinator` while the
    // bg-task already holds `inner.state.lock()`. Locking it again would
    // deadlock (`std::sync::Mutex` is non-reentrant). The caller writes
    // the epoch back inside its own critical section — see
    // `poll_with_coordinator`.

    let builder = if can_use_topic_ids {
        OffsetCommitRequestBuilder::for_topic_ids_or_names(data)
    } else {
        OffsetCommitRequestBuilder::for_topic_names(data)
    };

    // Build the unsent request, register a completion handler that
    // dispatches the response into the request state's `future_tx`.
    let coordinator_node = inner.coordinator_node();
    let mut unsent = UnsentRequest::new(Box::new(builder), coordinator_node);
    let response_rx = unsent.take_response_receiver().expect("receiver fresh");
    let inner_for_handler = Arc::clone(inner);
    tokio::spawn(async move {
        match response_rx.await {
            Ok(Ok(mut client_response)) => {
                handle_offset_commit_response(&inner_for_handler, request, client_response.take_response_body());
            },
            Ok(Err(err)) => {
                // Transport-level failure (e.g. disconnect). Java's shared
                // RequestState.handleClientResponse error arm calls
                // handleCoordinatorDisconnect before completing exceptionally
                // (CommitRequestManager.java:947).
                inner_for_handler.handle_coordinator_disconnect(&err, current_time_ms_now());
                request.complete_err(err);
            },
            Err(_recv_err) => {
                request.complete_err(KafkaError::new(Errors::NetworkException));
            },
        }
    });
    unsent
}

fn build_offset_fetch_unsent_request(
    inner: &Arc<CommitRequestManagerInner>,
    request: &mut OffsetFetchRequestState,
) -> UnsentRequest {
    let metadata = inner.metadata.metadata_arc();
    let topic_ids = metadata.topic_ids();
    request.topic_names_cache.clear();
    let mut can_use_topic_ids = !topic_ids.is_empty();

    // Group requested partitions by topic name.
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for tp in &request.requested_partitions {
        by_topic.entry(tp.topic().to_string()).or_default().push(tp.partition());
    }
    let mut topics: Vec<OffsetFetchRequestTopics> = Vec::with_capacity(by_topic.len());
    for (topic_name, partitions) in by_topic {
        let topic_id = topic_ids.get(&topic_name).copied().unwrap_or_else(Uuid::zero);
        if topic_id == Uuid::zero() {
            can_use_topic_ids = false;
        } else {
            request.topic_names_cache.insert(topic_id, topic_name.clone());
        }
        let mut topic = OffsetFetchRequestTopics::new();
        topic.set_name(topic_name);
        topic.set_topic_id(topic_id);
        topic.set_partition_indexes(partitions);
        topics.push(topic);
    }

    let mut group = OffsetFetchRequestGroup::new();
    group.set_group_id(inner.group_id.clone());
    group.set_topics(Some(topics));
    if let Some(epoch) = request.member_info.member_epoch {
        group.set_member_id(Some(request.member_info.member_id.clone()));
        group.set_member_epoch(epoch);
    }
    let mut data = OffsetFetchRequestData::new();
    data.set_require_stable(true);
    data.set_groups(vec![group]);

    let builder = if can_use_topic_ids {
        OffsetFetchRequestBuilder::for_topic_ids_or_names(data, inner.throw_on_fetch_stable_offset_unsupported)
    } else {
        OffsetFetchRequestBuilder::for_topic_names(data, inner.throw_on_fetch_stable_offset_unsupported)
    };

    let coordinator_node = inner.coordinator_node();
    let mut unsent = UnsentRequest::new(Box::new(builder), coordinator_node);
    let response_rx = unsent.take_response_receiver().expect("receiver fresh");
    let inner_for_handler = Arc::clone(inner);
    // We need to give the spawned task access to the request's
    // `future_tx` + `topic_names_cache`. Cloning the `Arc<Mutex<...>>` of
    // the future_tx is fine; the topic_names_cache is owned by the
    // request which is moved into the inflight list after this call.
    let future_tx = Arc::clone(&request.future_tx);
    let topic_names_cache = request.topic_names_cache.clone();
    let group_id = inner.group_id.clone();
    let request_id = request.request_id;
    tokio::spawn(async move {
        match response_rx.await {
            Ok(Ok(mut client_response)) => {
                handle_offset_fetch_response(
                    &inner_for_handler,
                    &group_id,
                    &topic_names_cache,
                    future_tx,
                    client_response.take_response_body(),
                );
            },
            Ok(Err(err)) => {
                // Transport-level failure (e.g. disconnect). Java's shared
                // RequestState.handleClientResponse error arm calls
                // handleCoordinatorDisconnect before completing exceptionally
                // (CommitRequestManager.java:947).
                inner_for_handler.handle_coordinator_disconnect(&err, current_time_ms_now());
                if let Some(tx) = future_tx.lock().expect("offset_fetch future_tx poisoned").take() {
                    let _ = tx.send(Err(err));
                }
            },
            Err(_recv_err) => {
                if let Some(tx) = future_tx.lock().expect("offset_fetch future_tx poisoned").take() {
                    let _ = tx.send(Err(KafkaError::new(Errors::NetworkException)));
                }
            },
        }
        // Drain the matching entry from `inflight_offset_fetches`.
        // Mirrors Java's `pendingRequests.inflightOffsetFetches.remove(fetchRequest)`
        // inside `fetchOffsetsWithRetries.whenComplete`. Phase 9 leaked
        // these entries because the completion path did not remove them.
        let mut state_guard = inner_for_handler.state.lock().expect("commit manager state poisoned");
        let inflight = &mut state_guard.pending.inflight_offset_fetches;
        if let Some(pos) = inflight.iter().position(|r| r.request_id == request_id) {
            inflight.swap_remove(pos);
        } else {
            log::warn!(
                "A duplicated, inflight, request was identified, but unable to find it in the outbound buffer: request_id={request_id}"
            );
        }
    });
    unsent
}

// =========================================================================
//                       Response handlers
// =========================================================================

fn handle_offset_commit_response(
    inner: &Arc<CommitRequestManagerInner>,
    request: OffsetCommitRequestState,
    body: Option<crate::common::requests::ConcreteResponse>,
) {
    let response = match body {
        Some(crate::common::requests::ConcreteResponse::OffsetCommit(r)) => r,
        _ => {
            request.complete_err(KafkaError::new(Errors::UnknownServerError));
            return;
        },
    };
    classify_and_complete_commit(inner, request, &response);
}

fn classify_and_complete_commit(
    inner: &Arc<CommitRequestManagerInner>,
    request: OffsetCommitRequestState,
    response: &OffsetCommitResponse,
) {
    let group_id = inner.group_id.as_str();
    let mut unauthorized: HashSet<String> = HashSet::new();
    for topic in response.topics() {
        for partition in &topic.partitions {
            let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
            let error = Errors::for_code(partition.error_code);
            if error == Errors::None {
                continue;
            }
            match error {
                Errors::GroupAuthorizationFailed => {
                    // Match Java: GroupAuthorizationException.forGroupId(groupId)
                    // — embeds the actual group id, not an empty string.
                    request.complete_err(KafkaError::group_authorization(group_id.to_string()));
                    return;
                },
                Errors::CoordinatorNotAvailable | Errors::NotCoordinator | Errors::RequestTimedOut => {
                    // Java line 801-806: mark coordinator unknown before
                    // surfacing the error so the retry driver's next
                    // commit attempt re-discovers the coordinator.
                    inner.mark_coordinator_unknown(error.message(), current_time_ms_now());
                    request.complete_err(KafkaError::new(error));
                    return;
                },
                Errors::OffsetMetadataTooLarge | Errors::InvalidCommitOffsetSize => {
                    request.complete_err(KafkaError::new(error));
                    return;
                },
                Errors::CoordinatorLoadInProgress | Errors::UnknownTopicOrPartition | Errors::UnknownTopicId => {
                    request.complete_err(KafkaError::new(error));
                    return;
                },
                Errors::UnknownMemberId => {
                    let msg = format!("OffsetCommit failed with unknown member ID. {}", error.message());
                    request.complete_err(ConsumerError::commit_failed(msg).into());
                    return;
                },
                Errors::StaleMemberEpoch => {
                    request.complete_err(KafkaError::new(error));
                    return;
                },
                Errors::TopicAuthorizationFailed => {
                    unauthorized.insert(tp.topic().to_string());
                },
                _ => {
                    request.complete_err(KafkaError::with_message(
                        Errors::UnknownServerError,
                        format!("Unexpected error in commit: {}", error.message()),
                    ));
                    return;
                },
            }
        }
    }
    if !unauthorized.is_empty() {
        request.complete_err(KafkaError::topic_authorization(unauthorized));
    } else {
        // Java completes with `null`. Translating: complete with the
        // input offsets (matching commit_sync's contract above).
        request.complete_ok(request.offsets.clone());
    }
}

fn handle_offset_fetch_response(
    inner: &Arc<CommitRequestManagerInner>,
    group_id: &str,
    topic_names_cache: &HashMap<Uuid, String>,
    future_tx: FetchFutureTx,
    body: Option<crate::common::requests::ConcreteResponse>,
) {
    let send = |result: FetchResult| {
        if let Some(tx) = future_tx.lock().expect("offset_fetch future_tx poisoned").take() {
            let _ = tx.send(result);
        }
    };
    let response = match body {
        Some(crate::common::requests::ConcreteResponse::OffsetFetch(r)) => r,
        _ => {
            send(Err(KafkaError::new(Errors::UnknownServerError)));
            return;
        },
    };
    let group_response = match response.group(group_id) {
        Ok(g) => g,
        Err(_e) => {
            send(Err(KafkaError::new(Errors::UnknownServerError)));
            return;
        },
    };
    let group_error = Errors::for_code(group_response.error_code);
    if group_error != Errors::None {
        // Java line 1090-1092: on NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE,
        // refresh the coordinator before completing the future
        // exceptionally so the retry driver's next OffsetFetch goes to
        // a freshly discovered coordinator.
        if matches!(group_error, Errors::NotCoordinator | Errors::CoordinatorNotAvailable) {
            inner.mark_coordinator_unknown(&format!("error response {:?}", group_error), current_time_ms_now());
        }
        send(Err(classify_fetch_group_error(group_error, group_id)));
        return;
    }
    let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
    let mut unauthorized: HashSet<String> = HashSet::new();
    let mut unstable: HashSet<TopicPartition> = HashSet::new();
    for topic in &group_response.topics {
        // If topic_id is set, look up the topic name from the cache.
        let topic_name = if topic.topic_id == Uuid::zero() {
            topic.name.clone()
        } else {
            topic_names_cache.get(&topic.topic_id).cloned().unwrap_or_default()
        };
        for partition in &topic.partitions {
            let tp = TopicPartition::new(topic_name.clone(), partition.partition_index);
            let err = Errors::for_code(partition.error_code);
            if err != Errors::None {
                match err {
                    Errors::UnknownTopicOrPartition | Errors::UnknownTopicId => {
                        send(Err(KafkaError::with_message(
                            Errors::UnknownServerError,
                            "Topic does not exist",
                        )));
                        return;
                    },
                    Errors::TopicAuthorizationFailed => {
                        unauthorized.insert(tp.topic().to_string());
                    },
                    Errors::UnstableOffsetCommit => {
                        unstable.insert(tp);
                    },
                    _ => {
                        send(Err(KafkaError::with_message(
                            Errors::UnknownServerError,
                            format!(
                                "Unexpected error in fetch offset response for partition {tp}: {}",
                                err.message()
                            ),
                        )));
                        return;
                    },
                }
            } else if partition.committed_offset >= 0 {
                let leader_epoch = if partition.committed_leader_epoch >= 0 {
                    Some(partition.committed_leader_epoch)
                } else {
                    None
                };
                match OffsetAndMetadata::with_leader_epoch(
                    partition.committed_offset,
                    leader_epoch,
                    partition.metadata.clone().unwrap_or_default(),
                ) {
                    Ok(oam) => {
                        offsets.insert(tp, Some(oam));
                    },
                    Err(e) => {
                        send(Err(e));
                        return;
                    },
                }
            } else {
                // No committed offset.
                offsets.insert(tp, None);
            }
        }
    }
    if !unauthorized.is_empty() {
        send(Err(KafkaError::topic_authorization(unauthorized)));
    } else if !unstable.is_empty() {
        send(Err(KafkaError::with_message(
            Errors::UnstableOffsetCommit,
            "There are unstable offsets for the requested topic partitions",
        )));
    } else {
        send(Ok(offsets));
    }
}

fn classify_fetch_group_error(error: Errors, group_id: &str) -> KafkaError {
    match error {
        Errors::CoordinatorLoadInProgress
        | Errors::UnknownMemberId
        | Errors::StaleMemberEpoch
        | Errors::NotCoordinator
        | Errors::CoordinatorNotAvailable => KafkaError::new(error),
        Errors::GroupAuthorizationFailed => KafkaError::group_authorization(group_id.to_string()),
        _ if error.is_retriable() => KafkaError::new(error),
        _ => KafkaError::with_message(
            Errors::UnknownServerError,
            format!("Unexpected error in fetch offset response: {}", error.message()),
        ),
    }
}

/// Wall-clock `System.currentTimeMillis()` equivalent used by response
/// handlers and retry drivers that don't carry an injected
/// `current_time_ms` parameter. Mirrors Java's bg-task `time.milliseconds()`
/// inside `OffsetFetchRequestState.onFailure`.
fn current_time_ms_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(i64::MAX)
}

impl CommitRequestManagerInner {
    fn coordinator_node(&self) -> Option<crate::common::Node> {
        // Read the coordinator handle (if wired) and return its currently
        // known coordinator node — the bg task updates the coordinator
        // state on every `FindCoordinator` response. If no handle is
        // wired (Phase 9 unit tests) or no coordinator is known yet, the
        // returned `None` lets `NetworkClientDelegate` pick the
        // least-loaded node as a fallback.
        let guard = self.coordinator.lock().expect("commit manager coordinator slot poisoned");
        guard.as_ref().and_then(|c| c.coordinator())
    }

    /// Call [`CoordinatorRequestManager::mark_coordinator_unknown`] if a
    /// coordinator handle has been wired via [`CommitRequestManager::set_coordinator`].
    /// Mirrors Java's
    /// `coordinatorRequestManager.markCoordinatorUnknown(error.message(), currentTimeMs)`
    /// calls scattered through `OffsetFetchRequestState.onFailure` and
    /// `OffsetCommitRequestState.onResponse`
    /// (`CommitRequestManager.java:804,1092`).
    fn mark_coordinator_unknown(&self, cause: &str, current_time_ms: i64) {
        let coord = {
            let guard = self.coordinator.lock().expect("commit manager coordinator slot poisoned");
            guard.as_ref().map(Arc::clone)
        };
        if let Some(coord) = coord {
            coord.mark_coordinator_unknown(cause, current_time_ms);
        }
    }

    /// Enqueue an interceptor `on_commit` invocation on the wired
    /// [`AutoCommitInterceptorHook`] (the type-erased
    /// `OffsetCommitCallbackInvoker`) if one is set. No-op when the hook
    /// is not wired (Phase 9 unit tests) or the interceptor chain is empty.
    /// Mirrors Java's `autoCommitCallback` success arm
    /// (`CommitRequestManager.java:380`).
    fn enqueue_interceptor_invocation(&self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) {
        let hook = {
            let guard = self
                .auto_commit_interceptor_hook
                .lock()
                .expect("commit manager auto-commit interceptor hook poisoned");
            guard.as_ref().map(Arc::clone)
        };
        if let Some(hook) = hook {
            hook.enqueue_interceptor_invocation(offsets);
        }
    }

    /// Forward a transport-level failure to the coordinator manager so a
    /// disconnect marks the coordinator unknown (triggering re-discovery on
    /// the next `FindCoordinator`). Mirrors Java's
    /// `coordinatorRequestManager.handleCoordinatorDisconnect(error, ...)`
    /// call in the shared `RequestState.handleClientResponse` error arm
    /// (`CommitRequestManager.java:947`), which runs for BOTH commit and
    /// fetch requests on a transport error. No-op when no coordinator handle
    /// is wired (Phase 9 unit tests).
    fn handle_coordinator_disconnect(&self, error: &KafkaError, current_time_ms: i64) {
        let coord = {
            let guard = self.coordinator.lock().expect("commit manager coordinator slot poisoned");
            guard.as_ref().map(Arc::clone)
        };
        if let Some(coord) = coord {
            coord.handle_coordinator_disconnect(error, current_time_ms);
        }
    }
}

// =========================================================================
//                Retry drivers (spawned per outstanding request)
// =========================================================================

async fn commit_sync_with_retries(
    inner: Arc<CommitRequestManagerInner>,
    initial_request_rx: oneshot::Receiver<CommitResult>,
    result_tx: CommitFutureTx,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    member_info: MemberInfo,
    deadline_ms: i64,
    now_ms: i64,
) {
    // Java: `commitSyncWithRetries` recurses on retriable errors using the
    // same `OffsetCommitRequestState` instance (`requestAttempt.resetFuture()`
    // + recurse). In Rust the send path consumes the state, so each retry
    // creates a fresh `OffsetCommitRequestState` and carries the
    // `commit_sync_attempts` counter forward via
    // [`OffsetCommitRequestState::seed_failed_attempts`] so the underlying
    // `RequestState.num_attempts` (used by `ExponentialBackoff`) ramps up
    // correctly across retries.
    //
    // The retry decision gate is the deadline (`is_expired`) — Java does
    // not cap retries by attempt count for `commit_sync`, only by
    // deadline. On deadline expiry after a retriable error the final
    // error is wrapped as a `TimeoutException`
    // (Java: `maybeWrapAsTimeoutException`). Non-retriable errors are
    // surfaced via `commitSyncExceptionForError` (StaleMemberEpoch
    // → `CommitFailedException`; else pass-through).
    let mut request_rx = initial_request_rx;
    let mut commit_sync_attempts: i32 = 0;
    let mut current_time_ms = now_ms;
    let outcome = loop {
        match request_rx.await {
            Ok(Ok(value)) => break Ok(value),
            Ok(Err(err)) => {
                // KIP-848 transient: see the doc-comment in
                // [`fetch_offsets_with_retries`] for the rationale.
                // `OffsetCommit` can also surface
                // `GROUP_ID_NOT_FOUND` from the broker during the
                // initial join window or during fence-rejoin cycles;
                // treat it as retriable in the driver. See Issue 9 in
                // `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`.
                let is_group_creation_in_progress = err.error() == Errors::GroupIdNotFound;
                let retriable = err.is_retriable() || is_group_creation_in_progress;
                if !retriable {
                    // Java's commitSyncExceptionForError wraps
                    // STALE_MEMBER_EPOCH as a CommitFailedException;
                    // everything else passes through.
                    if err.error() == Errors::StaleMemberEpoch {
                        break Err(ConsumerError::commit_failed(format!(
                            "OffsetCommit failed with stale member epoch. {}",
                            Errors::StaleMemberEpoch.message()
                        ))
                        .into());
                    }
                    break Err(err);
                }
                // Retriable error. Advance the local "now" by the
                // configured retry backoff (mirrors Java's bg-task tick
                // which would only re-poll the request once the
                // exponential-backoff window elapsed). Then check the
                // deadline: if expired, surface a TimeoutException
                // wrapping the original error message.
                let backoff = inner.retry_backoff_ms.max(0);
                current_time_ms = current_time_ms.saturating_add(backoff);
                commit_sync_attempts += 1;
                if current_time_ms >= deadline_ms {
                    log::info!("OffsetCommit timeout expired so it won't be retried anymore");
                    break Err(KafkaError::timeout(format!(
                        "Failed to commit offsets within the deadline: {}",
                        err.error().message()
                    )));
                }
                // Re-enqueue a fresh request with continuity in the
                // backoff counter.
                let (mut retry_request, retry_rx) = OffsetCommitRequestState::new(
                    offsets.clone(),
                    member_info.clone(),
                    inner.retry_backoff_ms,
                    inner.retry_backoff_max_ms,
                    deadline_ms,
                    current_time_ms,
                );
                retry_request.seed_failed_attempts(commit_sync_attempts, current_time_ms);
                {
                    let mut guard = inner.state.lock().expect("commit manager state poisoned");
                    guard.pending.unsent_offset_commits.push_back(retry_request);
                }
                request_rx = retry_rx;
            },
            Err(_) => break Err(KafkaError::new(Errors::NetworkException)),
        }
    };
    let mut guard = result_tx.lock().expect("commit_sync tx poisoned");
    if let Some(tx) = guard.take() {
        let _ = tx.send(outcome);
    }
}

/// Drives [`CommitRequestManager::maybe_auto_commit_sync_before_rebalance`].
///
/// Mirrors Java's `autoCommitSyncBeforeRebalanceWithRetries`
/// (`CommitRequestManager.java:342`). On retriable errors:
/// - if deadline expired → surface as [`KafkaError::timeout`] (Java's
///   `maybeWrapAsTimeoutException`);
/// - if [`Errors::UnknownTopicOrPartition`] → fatal (early-exit retries
///   despite the error otherwise being retriable);
/// - otherwise re-snapshot `subscriptions.allConsumed()` and re-enqueue.
///
/// Always clears the auto-commit inflight flag when the driver exits, so a
/// later interval-based auto-commit can fire.
#[allow(clippy::too_many_arguments)]
async fn auto_commit_sync_before_rebalance_with_retries(
    inner: Arc<CommitRequestManagerInner>,
    initial_request_rx: oneshot::Receiver<CommitResult>,
    result_tx: RebalanceFlushTx,
    initial_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    deadline_ms: i64,
    now_ms: i64,
) {
    let mut request_rx = initial_request_rx;
    let mut current_time_ms = now_ms;
    let mut attempts: i32 = 0;
    // Hold onto the most recent offsets so we can refresh from
    // `subscriptions.allConsumed()` on retry — Java does this via
    // `requestAttempt.offsets = subscriptions.allConsumed();` before
    // recursing. Initial value is preserved for the no-mutation path.
    // Also used to enqueue the interceptor `on_commit` invocation on
    // success (Java's `autoCommitCallback`, which `requestAutoCommit`
    // attaches to every attempt, including the rebalance flush).
    let mut last_offsets = initial_offsets;
    // Java re-reads `memberInfo` on every attempt: `isStaleEpochErrorAnd
    // ValidEpochAvailable` checks the CURRENT `memberInfo.memberEpoch`
    // (`CommitRequestManager.java:573-575`) and `createOffsetCommitRequest`
    // builds each retry with the latest id/epoch. The member epoch can change
    // mid-flight (the membership manager calls `onMemberEpochUpdated` after a
    // reconciliation), so a retry on STALE_MEMBER_EPOCH must pick up the new
    // epoch. We therefore re-read `member_info` from `inner.state` at the top
    // of every error iteration rather than capturing it once. The initial
    // The first attempt was already enqueued by the caller using the member
    // info read at that time; this driver refreshes the member id/epoch from
    // `inner.state` on each retry (Java re-reads `memberInfo` per attempt).
    let outcome: Result<(), KafkaError> = loop {
        match request_rx.await {
            Ok(Ok(_committed)) => {
                // Java `autoCommitCallback`: on success, enqueue the
                // interceptor `on_commit` invocation with the offsets that
                // were committed (`CommitRequestManager.java:380`).
                inner.enqueue_interceptor_invocation(last_offsets.clone());
                break Ok(());
            },
            Ok(Err(err)) => {
                // Re-read the latest member id/epoch (Java reads `memberInfo`
                // afresh on each attempt). Used both by the stale-epoch retry
                // gate below and the retry request build.
                let member_info = {
                    let guard = inner.state.lock().expect("commit manager state poisoned");
                    guard.member_info.clone()
                };
                let has_valid_member_epoch = member_info.member_epoch.is_some();
                // Java line 349: enter the retry gate only when the error
                // is a RetriableException OR the stale-epoch case AND a
                // valid member epoch is currently known.
                let is_stale_epoch_with_valid_epoch = err.error() == Errors::StaleMemberEpoch && has_valid_member_epoch;
                // KIP-848 transient: see [`fetch_offsets_with_retries`]
                // for the rationale (Issue 9 in
                // `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`).
                let is_group_creation_in_progress = err.error() == Errors::GroupIdNotFound;
                let is_retriable_for_rebalance =
                    err.is_retriable() || is_stale_epoch_with_valid_epoch || is_group_creation_in_progress;
                if !is_retriable_for_rebalance {
                    log::debug!("Auto-commit sync before rebalance failed with non-retriable error: {err}");
                    break Err(err);
                }
                // Java order (`CommitRequestManager.java:350-368`):
                //   1. `requestAttempt.isExpired()` → wrap as TimeoutException
                //   2. else if UnknownTopicOrPartitionException → fatal,
                //      surface the original error
                //   3. else → retry
                // The previous request-state object is consumed by the
                // network-build path, so we compare `current_time_ms`
                // (advanced once per retriable failure by the configured
                // backoff, mirroring how the bg-task's `runOnce` loop only
                // re-polls a retry after the exponential-backoff window
                // elapses) against `deadline_ms` here.
                let backoff = inner.retry_backoff_ms.max(0);
                current_time_ms = current_time_ms.saturating_add(backoff);
                attempts += 1;
                if current_time_ms >= deadline_ms {
                    log::debug!("Auto-commit sync before rebalance timed out and won't be retried anymore");
                    break Err(KafkaError::timeout(format!(
                        "Failed to commit offsets within the deadline: {}",
                        err.error().message()
                    )));
                }
                // Java treats UNKNOWN_TOPIC_OR_PARTITION as fatal here
                // (`CommitRequestManager.java:353-355`) even though it's
                // otherwise retriable. Checked AFTER expiry per Java's
                // order: when both conditions hold, Java's
                // TimeoutException wins.
                if err.error() == Errors::UnknownTopicOrPartition {
                    log::debug!("Auto-commit sync before rebalance failed because topic or partition were deleted");
                    break Err(err);
                }
                // Re-snapshot `subscriptions.allConsumed()` for the retry
                // (Java: `requestAttempt.offsets = subscriptions.allConsumed();`).
                let refreshed = {
                    let guard = inner.subscriptions.lock().expect("subscriptions poisoned");
                    guard.all_consumed()
                };
                if refreshed.is_empty() {
                    // Nothing left to commit — Java would still enqueue an
                    // empty request and short-circuit on `requestAutoCommit`.
                    // We replicate by resolving Ok here.
                    break Ok(());
                }
                log::debug!(
                    "Member {} will retry auto-commit of latest offsets after receiving retriable error {}",
                    member_info.member_id,
                    err.error().message()
                );
                last_offsets = refreshed.clone();
                let (mut retry_request, retry_rx) = OffsetCommitRequestState::new(
                    refreshed,
                    member_info.clone(),
                    inner.retry_backoff_ms,
                    inner.retry_backoff_max_ms,
                    deadline_ms,
                    current_time_ms,
                );
                retry_request.seed_failed_attempts(attempts, current_time_ms);
                {
                    let mut guard = inner.state.lock().expect("commit manager state poisoned");
                    guard.pending.unsent_offset_commits.push_back(retry_request);
                }
                request_rx = retry_rx;
            },
            Err(_) => break Err(KafkaError::new(Errors::NetworkException)),
        }
    };
    // Clear the inflight flag regardless of outcome (Java:
    // `autoCommitCallback` BiConsumer in `requestAutoCommit`).
    {
        let mut guard = inner.state.lock().expect("commit manager state poisoned");
        if let Some(ac) = guard.auto_commit.as_mut() {
            ac.set_inflight_commit_status(false);
        }
    }
    let mut guard = result_tx.lock().expect("auto-commit-sync tx poisoned");
    if let Some(tx) = guard.take() {
        let _ = tx.send(outcome);
    }
}

/// Drive an `OffsetFetch` retry loop.
///
/// Mirrors Java's `CommitRequestManager.fetchOffsetsWithRetries`
/// (`CommitRequestManager.java:544-571`). The Java implementation uses
/// `CompletableFuture::whenComplete` to recurse on retriable errors:
///
/// ```java
/// currentResult.whenComplete((res, error) -> {
///     pendingRequests.inflightOffsetFetches.remove(fetchRequest);
///     if (error == null) { result.complete(res); }
///     else if (error instanceof RetriableException || isStaleEpochErrorAndValidEpochAvailable(error)) {
///         if (fetchRequest.isExpired()) {
///             result.completeExceptionally(maybeWrapAsTimeoutException(error));
///         } else {
///             fetchRequest.resetFuture();
///             fetchOffsetsWithRetries(fetchRequest, result);
///         }
///     } else { result.completeExceptionally(error); }
/// });
/// ```
///
/// In Rust the per-attempt `OffsetFetchRequestState` is consumed by the
/// send path (its inner state lives only inside `inflight_offset_fetches`
/// until the response handler removes it). Each retry therefore allocates
/// a fresh `OffsetFetchRequestState` and pushes it onto
/// `unsent_offset_fetches`, carrying the failed-attempt counter forward
/// via [`OffsetFetchRequestState::seed_failed_attempts`] so the inner
/// `RequestState.num_attempts` (driving the `ExponentialBackoff`) ramps
/// up across retries.
///
/// Retry-eligibility predicate (Java line 559):
///   * `error.is_retriable()` — any retriable error (NotCoordinator,
///     CoordinatorNotAvailable, CoordinatorLoadInProgress, etc.); OR
///   * `StaleMemberEpoch` AND the consumer has a valid member epoch
///     (Java's `isStaleEpochErrorAndValidEpochAvailable`).
///
/// Deadline expiry (Java's `maybeWrapAsTimeoutException`) surfaces as
/// [`KafkaError::timeout`] wrapping the original error message.
async fn fetch_offsets_with_retries(
    inner: Arc<CommitRequestManagerInner>,
    initial_request_rx: oneshot::Receiver<FetchResult>,
    result_tx: FetchFutureTx,
    requested_partitions: HashSet<TopicPartition>,
    deadline_ms: i64,
    now_ms: i64,
) {
    let mut request_rx = initial_request_rx;
    let mut current_time_ms = now_ms;
    let mut attempts: i32 = 0;
    let outcome: FetchResult = loop {
        match request_rx.await {
            Ok(Ok(value)) => break Ok(value),
            Ok(Err(err)) => {
                // Java line 573-575: `isStaleEpochErrorAndValidEpochAvailable`
                // requires the consumer to currently hold a member epoch.
                let has_valid_member_epoch = {
                    let guard = inner.state.lock().expect("commit manager state poisoned");
                    guard.member_info.member_epoch.is_some()
                };
                let is_stale_epoch_retriable = err.error() == Errors::StaleMemberEpoch && has_valid_member_epoch;
                // KIP-848 transient: a fresh consumer (or one rejoining
                // after a fence) may dispatch `OffsetFetch` before the
                // broker has finished creating the consumer group (the
                // group is created on first heartbeat). Java's
                // production semantics do not retry `GROUP_ID_NOT_FOUND`
                // explicitly (`CommitRequestManager.java:1099-1101`
                // catches it in the final `else` and wraps as
                // non-retriable). However, on KIP-848 brokers we
                // observe it as a transient during the join window and
                // during fence-rejoin cycles, where retrying after the
                // backoff lets the heartbeat manager land its first HB
                // first and the broker registers the group. Treat it as
                // retriable in the driver, without
                // [`CommitRequestManagerInner::mark_coordinator_unknown`]
                // — the coordinator is correct; the group simply does
                // not exist yet. See Issue 9 in
                // `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`.
                let is_group_creation_in_progress = err.error() == Errors::GroupIdNotFound;
                let is_retriable = err.is_retriable() || is_stale_epoch_retriable || is_group_creation_in_progress;
                if !is_retriable {
                    break Err(err);
                }
                // Retriable error. Advance the local "now" by the
                // configured retry backoff (mirrors Java's bg-task tick
                // which re-polls the request only after the
                // exponential-backoff window elapses) and check the
                // deadline. If expired, wrap as TimeoutException.
                let backoff = inner.retry_backoff_ms.max(0);
                current_time_ms = current_time_ms.saturating_add(backoff);
                attempts += 1;
                if current_time_ms >= deadline_ms {
                    log::debug!(
                        "OffsetFetch request for {:?} timed out and won't be retried anymore",
                        requested_partitions
                    );
                    break Err(KafkaError::timeout(format!(
                        "Failed to fetch committed offsets within the deadline: {}",
                        err.error().message()
                    )));
                }
                // Re-enqueue a fresh OffsetFetchRequestState with
                // continuity in the backoff attempt counter. The
                // bg-task `poll_with_coordinator` will pick it up on
                // its next iteration once the coordinator is known.
                let member_info = {
                    let guard = inner.state.lock().expect("commit manager state poisoned");
                    guard.member_info.clone()
                };
                let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
                let (mut retry_request, retry_rx) = OffsetFetchRequestState::new(
                    request_id,
                    requested_partitions.clone(),
                    member_info,
                    inner.retry_backoff_ms,
                    inner.retry_backoff_max_ms,
                    deadline_ms,
                    current_time_ms,
                );
                retry_request.seed_failed_attempts(attempts, current_time_ms);
                {
                    let mut guard = inner.state.lock().expect("commit manager state poisoned");
                    guard.pending.unsent_offset_fetches.push(retry_request);
                }
                request_rx = retry_rx;
            },
            Err(_) => break Err(KafkaError::new(Errors::NetworkException)),
        }
    };
    let mut guard = result_tx.lock().expect("fetch_offsets tx poisoned");
    if let Some(tx) = guard.take() {
        let _ = tx.send(outcome);
    }
}

// =========================================================================
//                                  Tests
// =========================================================================

#[cfg(test)]
mod tests {
    //! Translation of
    //! `org.apache.kafka.clients.consumer.internals.CommitRequestManagerTest`.
    //!
    //! Phase 33 closes the CommitRequestManagerTest gap. The Java file is
    //! 1975 LOC with 50 `@Test`/`@ParameterizedTest` methods. All in-scope
    //! tests (KIP-848, §20) are translated below, expanding each
    //! `@MethodSource` matrix to a loop over every error case (DoD §3). The
    //! only deliberately-skipped tests are the metrics-framework ones:
    //!
    //!   - `testEnsureCommitSensorRecordsMetric` — metrics framework is out
    //!     of scope (CLAUDE.md + Phase 9 plan). No translation.
    //!   - The `commit-rate` / `commit-total` metric assertions inside
    //!     `testPollEnsureAutocommitSent` — same reason. The
    //!     request-emission half of that test IS translated
    //!     (`poll_ensure_autocommit_sent`); only the metric asserts are
    //!     dropped.
    //!
    //! ## Rust-vs-Java test mechanics
    //!
    //! The Rust manager's sync-commit / fetch / rebalance-flush retry drivers
    //! advance a LOCAL `current_time_ms` by `retry_backoff_ms` per retriable
    //! failure and compare it against `deadline_ms` (Java instead uses
    //! `MockTime.sleep` + re-poll + `isExpired()`). Each retry re-enqueues a
    //! FRESH request seeded with `seed_failed_attempts` — there is no stable
    //! `numAttempts` field on a single object across retries. So Java's
    //! `commitRequest.numAttempts` assertions map to peeking the head of the
    //! unsent queue and reading `state.num_attempts()`.
    //!
    //! Responses are driven through the spawned response handler registered
    //! in `build_offset_commit_unsent_request` / `build_offset_fetch_unsent_request`:
    //! `poll_with_coordinator(&coord, now)` ships the request, then
    //! `unsent.handler().on_complete(client_response)` / `on_failure(now, err)`
    //! delivers the response. The handlers spawn, so tests drive the runtime
    //! with `yield_now()` loops (or a `multi_thread` runtime for
    //! deadline-expiry loops that need a real scheduler).

    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::internals::subscription_state::SubscriptionState;

    const GROUP_ID: &str = "group-1";

    fn test_config(enable_auto_commit: bool) -> ConsumerConfig {
        let mut cfg = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        cfg.enable_auto_commit = enable_auto_commit;
        cfg.auto_commit_interval_ms = 1_000;
        cfg
    }

    fn make_manager(now_ms: i64, enable_auto_commit: bool) -> CommitRequestManager {
        let cfg = test_config(enable_auto_commit);
        let subs = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::LATEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &cfg,
            Arc::clone(&subs),
            ClusterResourceListeners::new(),
        ));
        CommitRequestManager::new(&cfg, metadata, subs, GROUP_ID, None, now_ms)
    }

    /// Variant of `make_manager` that returns the manager along with the
    /// `Arc<Mutex<SubscriptionState>>` so the test can seed
    /// `subscriptions.allConsumed()` before invoking
    /// `maybe_auto_commit_sync_before_rebalance`.
    fn make_manager_with_subs(
        now_ms: i64,
        enable_auto_commit: bool,
    ) -> (CommitRequestManager, Arc<Mutex<SubscriptionState>>) {
        let cfg = test_config(enable_auto_commit);
        let subs = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::LATEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &cfg,
            Arc::clone(&subs),
            ClusterResourceListeners::new(),
        ));
        let mgr = CommitRequestManager::new(&cfg, metadata, Arc::clone(&subs), GROUP_ID, None, now_ms);
        (mgr, subs)
    }

    fn singleton_offset(tp: TopicPartition, offset: i64) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let mut map = HashMap::new();
        map.insert(tp, OffsetAndMetadata::new(offset).expect("non-negative"));
        map
    }

    use crate::client_response::ClientResponse;
    use crate::common::Node;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{ConcreteResponse, OffsetFetchResponse, RequestHeader};
    use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

    /// A `CoordinatorRequestManager` with a known coordinator node injected,
    /// mirroring `when(coordinatorRequestManager.coordinator()).thenReturn(...)`.
    fn coordinator_with_node() -> CoordinatorRequestManager {
        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(1, "host1".to_string(), 9092));
        coordinator
    }

    /// Wrap a `ConcreteResponse` in a `ClientResponse` with the appropriate
    /// API key header. Mirrors the Java test's `ClientResponse` construction.
    fn client_response_for(api_key: &ApiKeys, version: i16, response: ConcreteResponse) -> ClientResponse {
        let header = RequestHeader::new(api_key, version, "test-client", 0).expect("request header");
        ClientResponse::new(header, None, "localhost:9092", 0, 1, false, None, None, Some(response))
    }

    /// Build an `OffsetCommit` `ClientResponse` from a per-partition error
    /// map. Mirrors Java's `mockOffsetCommitResponse` /
    /// `buildOffsetCommitClientResponse`.
    fn offset_commit_response(per_partition: HashMap<TopicPartition, Errors>) -> ClientResponse {
        let response = ConcreteResponse::OffsetCommit(OffsetCommitResponse::from_response_data(0, &per_partition));
        client_response_for(&ApiKeys::OFFSET_COMMIT, 1, response)
    }

    /// Build an `OffsetCommit` `ClientResponse` with a single partition at
    /// the given error. Convenience over [`offset_commit_response`].
    fn offset_commit_response_single(tp: &TopicPartition, error: Errors) -> ClientResponse {
        let mut data = HashMap::new();
        data.insert(tp.clone(), error);
        offset_commit_response(data)
    }

    /// A single partition entry for [`offset_fetch_response`]:
    /// `(partition_index, committed_offset, committed_leader_epoch, metadata, error)`.
    type FetchPartition = (i32, i64, i32, &'static str, Errors);

    /// Build an `OffsetFetch` `ClientResponse` (v8+ groups form so the
    /// production `response.group(group_id)` reads from `data.groups`).
    /// `topics` maps `(topic_name, topic_id)` to its partition list.
    /// `group_error` is the group-level error code (`Errors::None` for the
    /// happy path). Mirrors Java's `buildOffsetFetchClientResponse`.
    fn offset_fetch_response(
        group_id: &str,
        topics: Vec<((&str, Uuid), Vec<FetchPartition>)>,
        group_error: Errors,
    ) -> ClientResponse {
        use crate::offset_fetch_response_data::{
            OffsetFetchResponseData, OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        };
        let mut response_topics = Vec::new();
        for ((name, topic_id), partitions) in topics {
            let mut topic = OffsetFetchResponseTopics::new();
            topic.set_name(name.to_string());
            topic.set_topic_id(topic_id);
            let parts = partitions
                .into_iter()
                .map(|(idx, offset, leader_epoch, metadata, error)| {
                    let mut p = OffsetFetchResponsePartitions::new();
                    p.set_partition_index(idx);
                    p.set_committed_offset(offset);
                    p.set_committed_leader_epoch(leader_epoch);
                    p.set_metadata(Some(metadata.to_string()));
                    p.set_error_code(error.code());
                    p
                })
                .collect();
            topic.set_partitions(parts);
            response_topics.push(topic);
        }
        let mut group = OffsetFetchResponseGroup::new();
        group.set_group_id(group_id.to_string());
        group.set_error_code(group_error.code());
        group.set_topics(response_topics);
        let mut data = OffsetFetchResponseData::new();
        data.set_groups(vec![group]);
        let version = ApiKeys::OFFSET_FETCH.latest_version();
        let response = ConcreteResponse::OffsetFetch(OffsetFetchResponse::new(data, version));
        client_response_for(&ApiKeys::OFFSET_FETCH, version, response)
    }

    /// Build a successful single-partition `OffsetFetch` response with
    /// offset=100, metadata="metadata", leaderEpoch=1 (the common fixture
    /// used by Java's `buildOffsetFetchClientResponse(request, partitions,
    /// Errors.NONE)`).
    fn offset_fetch_response_for_partitions(partitions: &HashSet<TopicPartition>) -> ClientResponse {
        let mut by_topic: HashMap<String, Vec<FetchPartition>> = HashMap::new();
        for tp in partitions {
            by_topic.entry(tp.topic().to_string()).or_default().push((
                tp.partition(),
                100,
                1,
                "metadata",
                Errors::None,
            ));
        }
        let topics: Vec<((&str, Uuid), Vec<FetchPartition>)> = by_topic
            .iter()
            .map(|(name, parts)| ((name.as_str(), Uuid::zero()), parts.clone()))
            .collect();
        // Leak-free: the &str borrows live only for the call; build the
        // response inline so the borrow does not escape.
        offset_fetch_response_owned(GROUP_ID, topics, Errors::None)
    }

    /// `offset_fetch_response` variant that owns topic-name strings — avoids
    /// borrow-escape when building from a runtime-constructed map.
    fn offset_fetch_response_owned(
        group_id: &str,
        topics: Vec<((&str, Uuid), Vec<FetchPartition>)>,
        group_error: Errors,
    ) -> ClientResponse {
        offset_fetch_response(group_id, topics, group_error)
    }

    /// Ship exactly one request via `poll_with_coordinator` and return its
    /// `UnsentRequest`. Asserts the poll produced exactly one unsent request.
    fn poll_one_unsent(
        manager: &CommitRequestManager,
        coordinator: &CoordinatorRequestManager,
        now_ms: i64,
    ) -> UnsentRequest {
        let mut poll_result = manager.poll_with_coordinator(coordinator, now_ms);
        assert_eq!(
            poll_result.unsent_requests.len(),
            1,
            "expected exactly one unsent request from poll_with_coordinator"
        );
        poll_result.unsent_requests.remove(0)
    }

    /// Seed `metadata.topic_ids()` with `topic -> topic_id` so the request
    /// builders use the topic-ID wire form (v10+ commit / v10+ fetch).
    /// Mirrors `when(metadata.topicIds()).thenReturn(Map.of(topic, topicId))`.
    fn seed_topic_id(manager: &CommitRequestManager, topic: &str, topic_id: Uuid) {
        manager.inner.metadata.add_transient_topics(HashSet::from([topic.to_string()]));
        let metadata = manager.inner.metadata.metadata_arc();
        let mut counts = HashMap::new();
        counts.insert(topic.to_string(), 1);
        let mut ids = HashMap::new();
        ids.insert(topic.to_string(), topic_id);
        let response = crate::common::requests::request_test_utils::metadata_update_with_ids(
            "cluster",
            1,
            &HashMap::new(),
            &counts,
            &|_tp| None,
            &ids,
        );
        metadata.update_with_current_request_version(&response, false, 0);
    }

    /// Yield the current-thread runtime until `predicate` returns `Some(v)`
    /// or the iteration cap is hit (then panic with `msg`). Used to wait for
    /// a spawned response handler / retry driver to make observable progress.
    async fn yield_until<T>(mut predicate: impl FnMut() -> Option<T>, msg: &str) -> T {
        for _ in 0..64 {
            if let Some(v) = predicate() {
                return v;
            }
            tokio::task::yield_now().await;
        }
        panic!("{msg}");
    }

    /// Phase 9 test (no Java analog at this scope): `auto_commit_enabled`
    /// is `true` when the config enables auto-commit.
    #[test]
    fn auto_commit_enabled_reflects_config() {
        let manager_on = make_manager(0, true);
        assert!(manager_on.auto_commit_enabled());
        let manager_off = make_manager(0, false);
        assert!(!manager_off.auto_commit_enabled());
    }

    /// Translated from
    /// `CommitRequestManagerTest.testPollIntervalMs` — `maximum_time_to_wait`
    /// returns the auto-commit remaining time when auto-commit is enabled,
    /// `i64::MAX` otherwise.
    #[test]
    fn maximum_time_to_wait_reflects_auto_commit_state() {
        let mut manager = make_manager(0, true);
        // With auto-commit interval = 1000ms and `now = 0`, remaining = 1000.
        assert_eq!(manager.maximum_time_to_wait(0), 1_000);
        assert_eq!(manager.maximum_time_to_wait(500), 500);
        assert_eq!(manager.maximum_time_to_wait(1_500), 0);
        // Auto-commit disabled → no deadline.
        let manager_off = make_manager(0, false);
        assert_eq!(manager_off.maximum_time_to_wait(0), i64::MAX);
        // Signal close — does not change `maximum_time_to_wait`.
        manager.signal_close();
        assert_eq!(manager.maximum_time_to_wait(0), 1_000);
    }

    /// `reset_auto_commit_timer` resets the next-firing time relative to
    /// `now_ms`.
    #[test]
    fn reset_auto_commit_timer_resets_expiration() {
        let manager = make_manager(0, true);
        manager.reset_auto_commit_timer(500);
        // After resetting at now=500, remaining at now=500 = 1000.
        assert_eq!(manager.maximum_time_to_wait(500), 1_000);
    }

    /// `commit_sync` on an empty offsets map resolves immediately to
    /// `Ok({})`.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_empty_offsets_resolves_immediately() {
        let manager = make_manager(0, false);
        let rx = manager.commit_sync(HashMap::new(), i64::MAX, 0);
        let result = rx.await.expect("sender alive").expect("ok");
        assert!(result.is_empty());
    }

    /// `commit_async` on an empty offsets map resolves immediately and
    /// does NOT enqueue the user callback.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_async_empty_offsets_resolves_immediately() {
        let manager = make_manager(0, false);
        let interceptors =
            crate::consumer::internals::consumer_interceptors::ConsumerInterceptors::<String, String>::new(Vec::new());
        let invoker = Arc::new(OffsetCommitCallbackInvoker::new(interceptors));
        let rx = manager.commit_async(HashMap::new(), None, invoker, 0);
        let result = rx.await.expect("sender alive").expect("ok");
        assert!(result.is_empty());
    }

    /// `fetch_offsets` on an empty partition set resolves immediately.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_empty_partitions_resolves_immediately() {
        let manager = make_manager(0, false);
        let rx = manager.fetch_offsets(HashSet::new(), i64::MAX, 0);
        let result = rx.await.expect("sender alive").expect("ok");
        assert!(result.is_empty());
    }

    /// `signal_close` flips the closing flag; subsequent operations are
    /// safe to call. Java: `signalClose()`.
    #[test]
    fn signal_close_flips_closing_flag() {
        let mut manager = make_manager(0, true);
        manager.signal_close();
        let closing = manager.inner.closing.lock().unwrap();
        assert!(*closing);
    }

    /// `on_member_epoch_updated` writes the new id + epoch to `MemberInfo`.
    /// Java: `onMemberEpochUpdated(Optional<Integer>, String)`.
    #[test]
    fn on_member_epoch_updated_stores_id_and_epoch() {
        let manager = make_manager(0, false);
        manager.on_member_epoch_updated(Some(7), "member-A".to_string());
        let guard = manager.inner.state.lock().unwrap();
        assert_eq!(guard.member_info.member_id, "member-A");
        assert_eq!(guard.member_info.member_epoch, Some(7));
    }

    /// Setting `member_epoch = None` after a previous epoch logs that the
    /// member has left the group. The exact log isn't testable; verify the
    /// stored state instead.
    #[test]
    fn on_member_epoch_updated_handles_left_group_transition() {
        let manager = make_manager(0, false);
        manager.on_member_epoch_updated(Some(7), "member-A".to_string());
        manager.on_member_epoch_updated(None, "member-A".to_string());
        let guard = manager.inner.state.lock().unwrap();
        assert_eq!(guard.member_info.member_epoch, None);
    }

    /// `commit_sync` with non-empty offsets enqueues a request on the
    /// pending-commits queue and does not resolve immediately.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_enqueues_pending_request() {
        let manager = make_manager(0, false);
        let tp = TopicPartition::new("t".to_string(), 0);
        let offsets = singleton_offset(tp, 100);
        let _rx = manager.commit_sync(offsets, i64::MAX, 0);
        let guard = manager.inner.state.lock().unwrap();
        assert_eq!(guard.pending.unsent_offset_commits.len(), 1);
    }

    /// `fetch_offsets` with non-empty partitions enqueues a request.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_enqueues_pending_request() {
        let manager = make_manager(0, false);
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));
        let _rx = manager.fetch_offsets(partitions, i64::MAX, 0);
        let guard = manager.inner.state.lock().unwrap();
        assert_eq!(guard.pending.unsent_offset_fetches.len(), 1);
    }

    /// `MemberInfo::Display` mirrors Java's `MemberInfo.toString`.
    #[test]
    fn member_info_display_matches_java_format() {
        let info = MemberInfo { member_id: "m".to_string(), member_epoch: Some(3) };
        assert_eq!(info.to_string(), "memberId=m, memberEpoch=3");
        let info_no_epoch = MemberInfo { member_id: "m".to_string(), member_epoch: None };
        assert_eq!(info_no_epoch.to_string(), "memberId=m, memberEpoch=undefined");
    }

    /// `AutoCommitState::should_auto_commit` returns `true` when the timer
    /// has expired AND no inflight commit. Java parity.
    #[test]
    fn auto_commit_state_should_fire_only_when_due_and_idle() {
        let mut ac = AutoCommitState::new(0, 100);
        assert!(!ac.should_auto_commit(50)); // not yet expired
        assert!(ac.should_auto_commit(150)); // expired
        ac.set_inflight_commit_status(true);
        assert!(!ac.should_auto_commit(150)); // expired but in-flight
        ac.set_inflight_commit_status(false);
        assert!(ac.should_auto_commit(150));
    }

    /// `AutoCommitState::reset_timer_with_backoff` resets the next-firing
    /// to a caller-provided backoff.
    #[test]
    fn auto_commit_state_reset_timer_with_backoff_uses_supplied_value() {
        let mut ac = AutoCommitState::new(0, 1_000);
        ac.reset_timer_with_backoff(500, 50);
        assert_eq!(ac.remaining_ms(500), 50);
        assert_eq!(ac.remaining_ms(550), 0);
    }

    // ---------------------------------------------------------------------
    //                       Phase 10 wire-prereqs
    // ---------------------------------------------------------------------

    /// Phase 10 wire-prereq #6:
    /// [`CommitRequestManager::update_timer_and_maybe_commit`] is the
    /// processor-side entry point that ensures the auto-commit timer is
    /// honoured at event-dispatch time. With auto-commit enabled, the
    /// timer past its expiration, AND `subscriptions.allConsumed()`
    /// non-empty, calling the hook must (a) reset the timer to a fresh
    /// interval, (b) enqueue an `OffsetCommitRequestState` on
    /// `pending.unsent_offset_commits`, and (c) raise the auto-commit
    /// `has_inflight_commit` flag (mirrors Java's
    /// `maybeAutoCommitAsync` → `requestAutoCommit` path).
    #[tokio::test(flavor = "current_thread")]
    async fn update_timer_and_maybe_commit_fires_when_timer_expired() {
        let (manager, subs) = make_manager_with_subs(0, true);
        // Seed `subscriptions.allConsumed()` with a single assigned
        // partition at offset 100 so the auto-commit request is built
        // with a non-empty offsets map.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut partitions = HashSet::new();
            partitions.insert(tp.clone());
            s.assign_from_user(partitions).expect("assign_from_user");
            s.seek(&tp, 100).expect("seek");
        }
        // Pre-condition: no unsent commit yet.
        {
            let guard = manager.inner.state.lock().unwrap();
            assert!(guard.pending.unsent_offset_commits.is_empty());
            assert!(
                !guard.auto_commit.as_ref().unwrap().has_inflight_commit,
                "inflight flag must start clear"
            );
        }
        // Advance past the auto-commit interval (1000ms — configured in
        // `test_config`) and call the hook.
        let after_expiry_ms = 2_000;
        manager.update_timer_and_maybe_commit(after_expiry_ms);
        // (a) Timer reset to a fresh interval.
        assert_eq!(manager.maximum_time_to_wait(after_expiry_ms), 1_000);
        // (b) One unsent commit request enqueued; (c) inflight flag is
        // raised because `requestAutoCommit` set it before the request
        // resolves.
        {
            let guard = manager.inner.state.lock().unwrap();
            assert_eq!(
                guard.pending.unsent_offset_commits.len(),
                1,
                "auto-commit must enqueue exactly one request when subscriptions are non-empty"
            );
            assert!(
                guard.auto_commit.as_ref().unwrap().has_inflight_commit,
                "inflight flag must be raised while the auto-commit request is outstanding"
            );
            // Sanity: the enqueued request carries the snapshotted offsets.
            let queued = guard.pending.unsent_offset_commits.front().unwrap();
            assert_eq!(queued.offsets.len(), 1);
            assert!(queued.offsets.contains_key(&tp));
        }

        // Now ship the request and drive a successful response to confirm
        // the inflight flag flips back on completion (mirrors Java's
        // `autoCommitCallback` BiConsumer in `requestAutoCommit`).
        use crate::common::Node;
        use crate::common::requests::ConcreteResponse;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;
        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let poll_result = manager.poll_with_coordinator(&coordinator, after_expiry_ms);
        assert_eq!(poll_result.unsent_requests.len(), 1);
        let mut unsent_requests = poll_result.unsent_requests;
        let unsent = unsent_requests.remove(0);
        let mut response_data: HashMap<TopicPartition, Errors> = HashMap::new();
        response_data.insert(tp.clone(), Errors::None);
        let response = ConcreteResponse::OffsetCommit(
            crate::common::requests::OffsetCommitResponse::from_response_data(0, &response_data),
        );
        let header = crate::common::requests::RequestHeader::new(
            &crate::common::protocol::ApiKeys::OFFSET_COMMIT,
            0,
            "test-client",
            0,
        )
        .expect("request header");
        let client_response = crate::client_response::ClientResponse::new(
            header,
            None,
            "localhost:9092",
            0,
            1,
            false,
            None,
            None,
            Some(response),
        );
        unsent.handler().on_complete(client_response);
        // Yield until the spawned auto-commit task clears the flag.
        for _ in 0..32 {
            tokio::task::yield_now().await;
            let guard = manager.inner.state.lock().unwrap();
            if !guard.auto_commit.as_ref().unwrap().has_inflight_commit {
                return;
            }
        }
        panic!("has_inflight_commit flag was not cleared on auto-commit success");
    }

    /// With auto-commit enabled but `subscriptions.allConsumed()` empty
    /// (no assigned partitions with valid positions), the hook resets the
    /// auto-commit timer but does NOT enqueue a request and does NOT
    /// raise the inflight flag. Mirrors Java's `requestAutoCommit`
    /// short-circuit on empty offsets.
    #[test]
    fn update_timer_and_maybe_commit_resets_timer_when_no_consumed_offsets() {
        let (manager, _subs) = make_manager_with_subs(0, true);
        let after_expiry_ms = 2_000;
        manager.update_timer_and_maybe_commit(after_expiry_ms);
        // Timer reset (Java does this unconditionally in `maybeAutoCommitAsync`).
        assert_eq!(manager.maximum_time_to_wait(after_expiry_ms), 1_000);
        // No request enqueued; inflight flag stays clear.
        let guard = manager.inner.state.lock().unwrap();
        assert!(guard.pending.unsent_offset_commits.is_empty());
        assert!(!guard.auto_commit.as_ref().unwrap().has_inflight_commit);
    }

    /// With auto-commit DISABLED, the hook is a no-op (Java:
    /// `maybeAutoCommitAsync` returns early when `autoCommitEnabled()`
    /// is false).
    #[test]
    fn update_timer_and_maybe_commit_is_noop_without_auto_commit() {
        let manager = make_manager(0, false);
        // Calling the hook should not panic and `maximum_time_to_wait`
        // remains `i64::MAX` since no auto-commit timer exists.
        manager.update_timer_and_maybe_commit(5_000);
        assert_eq!(manager.maximum_time_to_wait(5_000), i64::MAX);
    }

    /// Phase 10 wire-prereq #8:
    /// [`PendingRequests::inflight_offset_fetches`] must drain when an
    /// offset-fetch response (success or failure) is delivered. Phase 9
    /// leaked these entries because the completion path did not remove
    /// them. The test drives a `fetch_offsets` request through
    /// `poll_with_coordinator` to move it into the inflight vec, then
    /// completes the underlying network handler with a synthetic error
    /// and asserts the inflight vec is drained.
    #[tokio::test(flavor = "current_thread")]
    async fn inflight_offset_fetches_drained_on_response() {
        use crate::common::Node;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

        let manager = make_manager(0, false);
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));
        let _public_rx = manager.fetch_offsets(partitions, i64::MAX, 0);
        // Pre-condition: enqueued but not yet inflight.
        {
            let guard = manager.inner.state.lock().unwrap();
            assert_eq!(guard.pending.unsent_offset_fetches.len(), 1);
            assert!(guard.pending.inflight_offset_fetches.is_empty());
        }

        // Drive `poll_with_coordinator` to ship the request.
        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let poll_result = manager.poll_with_coordinator(&coordinator, 1);
        assert_eq!(poll_result.unsent_requests.len(), 1);

        // Inflight has the request now, unsent is empty.
        {
            let guard = manager.inner.state.lock().unwrap();
            assert!(guard.pending.unsent_offset_fetches.is_empty());
            assert_eq!(guard.pending.inflight_offset_fetches.len(), 1);
        }

        // Synthesise a response failure via the request's completion
        // handler. The spawned response handler awaits this and must
        // drain the inflight entry.
        let mut unsent_requests = poll_result.unsent_requests;
        let unsent = unsent_requests.remove(0);
        unsent
            .handler()
            .on_failure(1, KafkaError::new(Errors::CoordinatorLoadInProgress));

        // Yield until the spawned task observes the failure and drains.
        for _ in 0..16 {
            tokio::task::yield_now().await;
            let guard = manager.inner.state.lock().unwrap();
            if guard.pending.inflight_offset_fetches.is_empty() {
                return;
            }
        }
        let guard = manager.inner.state.lock().unwrap();
        panic!(
            "inflight_offset_fetches not drained on response (len={})",
            guard.pending.inflight_offset_fetches.len()
        );
    }

    /// Phase 10 wire-prereq #9:
    /// [`CommitRequestManager::commit_sync`] retries on retriable errors
    /// until the deadline expires, then surfaces a `TimeoutException`
    /// wrapping the last retriable error (Java:
    /// `maybeWrapAsTimeoutException` inside `commitSyncWithRetries`).
    ///
    /// Note: the user prompt suggested `RetriableCommitFailedError` as the
    /// expected surface error; Java actually surfaces `TimeoutException`
    /// from `commit_sync` and reserves `RetriableCommitFailedException`
    /// for the `commit_async` path (`commitAsyncExceptionForError`). We
    /// match Java's behaviour.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_sync_surfaces_timeout_error_after_deadline_expiry() {
        use crate::common::Node;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

        let manager = make_manager(0, false);
        // Short deadline so a small number of retriable failures trips
        // it. The retry driver advances its local `current_time_ms` by
        // `retry_backoff_ms` per retriable error; once that local clock
        // crosses `deadline_ms`, the driver surfaces a TimeoutException.
        let retry_backoff_ms = manager.inner.retry_backoff_ms;
        let deadline_ms = retry_backoff_ms.saturating_mul(2) + 1;
        let tp = TopicPartition::new("t".to_string(), 0);
        let public_rx = manager.commit_sync(singleton_offset(tp.clone(), 100), deadline_ms, 0);

        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));

        // Drive enough send / fail cycles to trip the deadline. The poll
        // time has to advance well beyond the retry-driver's local
        // `current_time_ms` so the re-enqueued request's exponential
        // backoff (seeded via `seed_failed_attempts`) is guaranteed to
        // have elapsed by the time the next `poll_with_coordinator` runs.
        // We use a poll-time step of `retry_backoff_max_ms * 2` to dwarf
        // both the configured backoff and its jitter band.
        let retry_backoff_max_ms = manager.inner.retry_backoff_max_ms;
        let poll_time_step = retry_backoff_max_ms.saturating_mul(2).max(retry_backoff_ms * 4);
        let mut poll_time_ms: i64 = 0;
        let mut public_rx = public_rx;
        let mut iters = 0;
        let outcome = loop {
            iters += 1;
            // Probe the public future without blocking.
            match public_rx.try_recv() {
                Ok(result) => break result,
                Err(oneshot::error::TryRecvError::Closed) => panic!("public sender dropped"),
                Err(oneshot::error::TryRecvError::Empty) => {},
            }
            let poll_result = manager.poll_with_coordinator(&coordinator, poll_time_ms);
            if let Some(unsent) = poll_result.unsent_requests.into_iter().next() {
                unsent
                    .handler()
                    .on_failure(poll_time_ms, KafkaError::new(Errors::CoordinatorLoadInProgress));
            }
            // Yield so the spawned response handler runs and the retry
            // driver enqueues the next attempt — even when the poll
            // didn't ship a request (the retry driver may still be
            // observing the prior completion). Use a short real-time
            // sleep so the multi-thread runtime has a chance to schedule
            // the spawned task on the other worker before the next probe.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            poll_time_ms = poll_time_ms.saturating_add(poll_time_step);
            // Defensive cap to prevent test-loop runaway.
            if iters > 200 {
                panic!("retry driver failed to surface a Timeout within 200 iterations");
            }
        };

        // The outcome must now be a Timeout error
        // (Java: `maybeWrapAsTimeoutException`).
        let err = outcome.expect_err("commit_sync must surface error after deadline expiry");
        assert!(
            matches!(err, KafkaError::Timeout(_)),
            "expected wrapped TimeoutException, got {err:?}",
        );
    }

    /// `OffsetCommitRequestState::seed_failed_attempts` seeds the inner
    /// `RequestState.num_attempts` counter so exponential backoff ramps
    /// across retry attempts.
    #[test]
    fn offset_commit_request_state_seed_failed_attempts_increments_counter() {
        let member_info = MemberInfo::default();
        let (mut state, _rx) = OffsetCommitRequestState::new(HashMap::new(), member_info, 100, 1_000, i64::MAX, 0);
        assert_eq!(state.state.num_attempts(), 0);
        state.seed_failed_attempts(3, 0);
        assert_eq!(state.state.num_attempts(), 3);
        assert_eq!(state.commit_sync_attempts, 3);
    }

    // ---------------------------------------------------------------------
    //   Phase 10 (commit 2.5/N): MemberStateListener + auto-commit-sync-
    //   before-rebalance tests.
    // ---------------------------------------------------------------------

    /// Phase 10 (commit 2.5/N): the trait-object upcast routes
    /// `on_member_epoch_updated` through to the inherent method that writes
    /// `MemberInfo`. Java:
    /// `CommitRequestManager implements MemberStateListener`.
    #[test]
    fn member_state_listener_forwards_epoch_update() {
        let manager = make_manager(0, false);
        // Upcast to the trait object so we exercise the impl, not the
        // inherent method directly.
        let listener: &dyn MemberStateListener = &manager;
        listener.on_member_epoch_updated(Some(13), "member-X");
        let guard = manager.inner.state.lock().unwrap();
        assert_eq!(guard.member_info.member_id, "member-X");
        assert_eq!(guard.member_info.member_epoch, Some(13));
    }

    /// Phase 10 (commit 2.5/N): the trait default for
    /// `on_group_assignment_updated` is a no-op on `CommitRequestManager`
    /// because Java does not override it (`CommitRequestManager.java:597`
    /// implements only `onMemberEpochUpdated`).
    #[test]
    fn member_state_listener_on_group_assignment_updated_is_noop() {
        let manager = make_manager(0, false);
        let listener: &dyn MemberStateListener = &manager;
        let mut tps = HashSet::new();
        tps.insert(TopicPartition::new("t".to_string(), 0));
        listener.on_group_assignment_updated(&tps); // must not panic
        // Member info untouched by this trait method.
        let guard = manager.inner.state.lock().unwrap();
        assert_eq!(guard.member_info.member_id, "");
        assert_eq!(guard.member_info.member_epoch, None);
    }

    /// Phase 10 (commit 2.5/N): with auto-commit disabled,
    /// `maybe_auto_commit_sync_before_rebalance` resolves immediately to
    /// `Ok(())` and enqueues no request. Java:
    /// `if (!autoCommitEnabled()) return CompletableFuture.completedFuture(null);`
    #[tokio::test(flavor = "current_thread")]
    async fn maybe_auto_commit_sync_before_rebalance_noop_when_disabled() {
        let manager = make_manager(0, false);
        let rx = manager.maybe_auto_commit_sync_before_rebalance(i64::MAX, 0);
        let result = rx.await.expect("sender alive");
        assert!(result.is_ok(), "expected immediate Ok(()) when auto-commit disabled");
        let guard = manager.inner.state.lock().unwrap();
        assert!(
            guard.pending.unsent_offset_commits.is_empty(),
            "no commit request should be enqueued when auto-commit is disabled"
        );
    }

    /// Phase 10 (commit 2.5/N): with auto-commit enabled but
    /// `subscriptions.allConsumed()` empty (no assigned partitions with
    /// valid positions), `maybe_auto_commit_sync_before_rebalance` resolves
    /// immediately to `Ok(())` and enqueues no request. Java:
    /// `requestAutoCommit` short-circuits on empty offsets.
    #[tokio::test(flavor = "current_thread")]
    async fn maybe_auto_commit_sync_before_rebalance_noop_when_no_consumed_offsets() {
        let (manager, _subs) = make_manager_with_subs(0, true);
        let rx = manager.maybe_auto_commit_sync_before_rebalance(i64::MAX, 0);
        let result = rx.await.expect("sender alive");
        assert!(result.is_ok(), "expected immediate Ok(()) when no offsets to commit");
        let guard = manager.inner.state.lock().unwrap();
        assert!(guard.pending.unsent_offset_commits.is_empty());
    }

    /// Phase 10 (commit 2.5/N): with auto-commit enabled and
    /// `subscriptions.allConsumed()` non-empty,
    /// `maybe_auto_commit_sync_before_rebalance` enqueues an
    /// `OffsetCommitRequestState` with the snapshotted offsets and resolves
    /// `Ok(())` once a successful response is driven through the test seam.
    #[tokio::test(flavor = "current_thread")]
    async fn maybe_auto_commit_sync_before_rebalance_flushes_offsets() {
        use crate::common::Node;
        use crate::common::requests::ConcreteResponse;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

        let (manager, subs) = make_manager_with_subs(0, true);
        // Seed `subscriptions.allConsumed()`: assign one partition and
        // seek-validate so it has a valid position.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut partitions = HashSet::new();
            partitions.insert(tp.clone());
            s.assign_from_user(partitions).expect("assign_from_user");
            s.seek(&tp, 100).expect("seek");
        }
        // Sanity-check: subscription state now reports an entry for `tp`.
        {
            let s = subs.lock().unwrap();
            let consumed = s.all_consumed();
            assert_eq!(consumed.len(), 1, "expected one all-consumed entry");
            assert!(consumed.contains_key(&tp));
        }

        // Fire the method under test. The driver enqueues an
        // `OffsetCommitRequestState` and spawns a retry loop awaiting its
        // future.
        let mut public_rx = manager.maybe_auto_commit_sync_before_rebalance(i64::MAX, 0);

        // Ship the request via `poll_with_coordinator` to move the unsent
        // entry to the network client, exposing its completion handler.
        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let poll_result = manager.poll_with_coordinator(&coordinator, 1);
        assert_eq!(
            poll_result.unsent_requests.len(),
            1,
            "expected exactly one unsent request enqueued by the rebalance flush"
        );

        // Synthesise a successful (all-zero error-code) OffsetCommitResponse
        // and drive it into the request's completion handler.
        let mut unsent_requests = poll_result.unsent_requests;
        let unsent = unsent_requests.remove(0);
        let mut response_data: HashMap<TopicPartition, Errors> = HashMap::new();
        response_data.insert(tp.clone(), Errors::None);
        let response = ConcreteResponse::OffsetCommit(
            crate::common::requests::OffsetCommitResponse::from_response_data(0, &response_data),
        );
        let header = crate::common::requests::RequestHeader::new(
            &crate::common::protocol::ApiKeys::OFFSET_COMMIT,
            0,
            "test-client",
            0,
        )
        .expect("request header");
        let client_response = crate::client_response::ClientResponse::new(
            header,
            None,
            "localhost:9092",
            0,
            1,
            false,
            None,
            None,
            Some(response),
        );
        unsent.handler().on_complete(client_response);

        // Drive the runtime so the spawned response handler runs and
        // resolves the public future. Cap the loop so the test cannot
        // hang on a regression.
        for _ in 0..32 {
            tokio::task::yield_now().await;
            match public_rx.try_recv() {
                Ok(Ok(())) => {
                    // Inflight commit flag must be cleared so the next
                    // interval-based auto-commit can fire.
                    let guard = manager.inner.state.lock().unwrap();
                    let inflight = guard.auto_commit.as_ref().map(|ac| ac.has_inflight_commit).unwrap_or(true);
                    assert!(!inflight, "has_inflight_commit flag must be cleared on success");
                    return;
                },
                Ok(Err(e)) => panic!("expected Ok(()) flush outcome, got {e:?}"),
                Err(oneshot::error::TryRecvError::Closed) => panic!("public sender dropped"),
                Err(oneshot::error::TryRecvError::Empty) => {},
            }
        }
        panic!("maybe_auto_commit_sync_before_rebalance future did not resolve after 32 yields");
    }

    /// Phase 10 fixup (COMMENTS.1.md #2): Java's
    /// `isStaleEpochErrorAndValidEpochAvailable` predicate requires
    /// `memberInfo.memberEpoch.isPresent()`. When the consumer has no
    /// valid member epoch (e.g. the member has left the group), a
    /// `StaleMemberEpoch` failure must NOT enter the retry gate — Java
    /// surfaces the original error directly. Previously the Rust driver
    /// dropped this guard and would loop until the deadline expired,
    /// then surface a `Timeout` instead of the original error.
    #[tokio::test(flavor = "current_thread")]
    async fn auto_commit_sync_before_rebalance_surfaces_stale_epoch_when_no_valid_epoch() {
        use crate::common::Node;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

        let (manager, subs) = make_manager_with_subs(0, true);
        // Member epoch defaults to None — that's the relevant precondition
        // for this test. Confirm.
        {
            let guard = manager.inner.state.lock().unwrap();
            assert!(
                guard.member_info.member_epoch.is_none(),
                "test relies on the default member_info having no epoch"
            );
        }
        // Seed `subscriptions.allConsumed()` with one partition so the
        // rebalance flush actually enqueues a request.
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut partitions = HashSet::new();
            partitions.insert(tp.clone());
            s.assign_from_user(partitions).expect("assign_from_user");
            s.seek(&tp, 100).expect("seek");
        }

        // Use a generous deadline so a Timeout result would only arise
        // from the buggy retry-loop path (not from genuinely-expired
        // backoff progression).
        let mut public_rx = manager.maybe_auto_commit_sync_before_rebalance(i64::MAX, 0);

        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let poll_result = manager.poll_with_coordinator(&coordinator, 1);
        assert_eq!(poll_result.unsent_requests.len(), 1);
        let mut unsent_requests = poll_result.unsent_requests;
        let unsent = unsent_requests.remove(0);
        // Drive a StaleMemberEpoch failure into the response handler.
        unsent.handler().on_failure(1, KafkaError::new(Errors::StaleMemberEpoch));

        // Yield until the public future resolves; expect the original
        // StaleMemberEpoch error, NOT a Timeout (the buggy code would
        // retry indefinitely because the gate would admit the error,
        // then eventually surface Timeout).
        for _ in 0..32 {
            tokio::task::yield_now().await;
            match public_rx.try_recv() {
                Ok(Ok(())) => panic!("expected StaleMemberEpoch failure, got Ok"),
                Ok(Err(err)) => {
                    assert_eq!(
                        err.error(),
                        Errors::StaleMemberEpoch,
                        "expected StaleMemberEpoch surfaced unchanged when member_epoch is None, got {err:?}"
                    );
                    return;
                },
                Err(oneshot::error::TryRecvError::Closed) => panic!("public sender dropped"),
                Err(oneshot::error::TryRecvError::Empty) => {},
            }
        }
        panic!("auto-commit-sync-before-rebalance future did not resolve after 32 yields");
    }

    /// Phase 10 fixup (COMMENTS.1.md #3): when BOTH the request deadline
    /// is past AND the error is `UnknownTopicOrPartition`, Java's order
    /// (`CommitRequestManager.java:350-368`) checks `isExpired` first and
    /// surfaces a wrapped `TimeoutException` (not the UTOP error). The
    /// Rust driver previously checked UTOP first, surfacing the raw
    /// error.
    #[tokio::test(flavor = "current_thread")]
    async fn auto_commit_sync_before_rebalance_timeout_wins_over_unknown_topic_or_partition() {
        use crate::common::Node;
        use crate::consumer::internals::coordinator_request_manager::CoordinatorRequestManager;

        let (manager, subs) = make_manager_with_subs(0, true);
        let tp = TopicPartition::new("t".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut partitions = HashSet::new();
            partitions.insert(tp.clone());
            s.assign_from_user(partitions).expect("assign_from_user");
            s.seek(&tp, 100).expect("seek");
        }
        // Pick a deadline that is already past at `now_ms = 0`. The
        // driver's local clock starts at `now_ms` and advances by
        // `retry_backoff_ms` on each retriable failure; with `deadline =
        // 1`, a single retry tick crosses it.
        let deadline_ms: i64 = 1;
        let mut public_rx = manager.maybe_auto_commit_sync_before_rebalance(deadline_ms, 0);

        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let poll_result = manager.poll_with_coordinator(&coordinator, 1);
        assert_eq!(poll_result.unsent_requests.len(), 1);
        let mut unsent_requests = poll_result.unsent_requests;
        let unsent = unsent_requests.remove(0);
        // Drive an UnknownTopicOrPartition failure (Errors::is_retriable
        // = true), with the deadline already past.
        unsent.handler().on_failure(1, KafkaError::new(Errors::UnknownTopicOrPartition));

        for _ in 0..32 {
            tokio::task::yield_now().await;
            match public_rx.try_recv() {
                Ok(Ok(())) => panic!("expected failure, got Ok"),
                Ok(Err(err)) => {
                    assert!(
                        matches!(err, KafkaError::Timeout(_)),
                        "expected Timeout (deadline check wins over UTOP), got {err:?}"
                    );
                    return;
                },
                Err(oneshot::error::TryRecvError::Closed) => panic!("public sender dropped"),
                Err(oneshot::error::TryRecvError::Empty) => {},
            }
        }
        panic!("auto-commit-sync-before-rebalance future did not resolve after 32 yields");
    }

    // =====================================================================
    //          Phase 33a — commit-path parity translations
    // =====================================================================

    /// The 13-error matrix of `offsetCommitExceptionSupplier`:
    /// `(error, expected exception class)`. Mirrors the Java `@MethodSource`.
    /// `ExpectedClass` distinguishes the typed exception each error maps to.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum ExpectedClass {
        Timeout,
        GroupAuthorization,
        OffsetMetadataTooLarge,
        InvalidCommitOffsetSize,
        TopicAuthorization,
        CommitFailed,
        KafkaException,
    }

    /// `offsetCommitExceptionSupplier()` — 13 cases.
    fn offset_commit_exception_supplier() -> Vec<(Errors, ExpectedClass)> {
        vec![
            // Retriable → TimeoutException when retry time expires.
            (Errors::NotCoordinator, ExpectedClass::Timeout),
            (Errors::CoordinatorLoadInProgress, ExpectedClass::Timeout),
            (Errors::CoordinatorNotAvailable, ExpectedClass::Timeout),
            (Errors::RequestTimedOut, ExpectedClass::Timeout),
            (Errors::UnknownTopicOrPartition, ExpectedClass::Timeout),
            (Errors::UnknownTopicId, ExpectedClass::Timeout),
            // Non-retriable → specific exceptions.
            (Errors::GroupAuthorizationFailed, ExpectedClass::GroupAuthorization),
            (Errors::OffsetMetadataTooLarge, ExpectedClass::OffsetMetadataTooLarge),
            (Errors::InvalidCommitOffsetSize, ExpectedClass::InvalidCommitOffsetSize),
            (Errors::TopicAuthorizationFailed, ExpectedClass::TopicAuthorization),
            (Errors::UnknownMemberId, ExpectedClass::CommitFailed),
            (Errors::StaleMemberEpoch, ExpectedClass::CommitFailed),
            // Generic → KafkaException.
            (Errors::UnknownServerError, ExpectedClass::KafkaException),
        ]
    }

    /// `offsetFetchExceptionSupplier()` — 14 cases.
    fn offset_fetch_exception_supplier() -> Vec<(Errors, ExpectedClass)> {
        vec![
            // Retriable → TimeoutException when retry time expires.
            (Errors::NotCoordinator, ExpectedClass::Timeout),
            (Errors::CoordinatorLoadInProgress, ExpectedClass::Timeout),
            (Errors::CoordinatorNotAvailable, ExpectedClass::Timeout),
            (Errors::RequestTimedOut, ExpectedClass::Timeout),
            (Errors::UnstableOffsetCommit, ExpectedClass::Timeout),
            (Errors::UnknownTopicOrPartition, ExpectedClass::Timeout),
            (Errors::UnknownTopicId, ExpectedClass::Timeout),
            // Non-retriable → specific exceptions.
            (Errors::GroupAuthorizationFailed, ExpectedClass::GroupAuthorization),
            (Errors::OffsetMetadataTooLarge, ExpectedClass::KafkaException),
            (Errors::InvalidCommitOffsetSize, ExpectedClass::KafkaException),
            (Errors::TopicAuthorizationFailed, ExpectedClass::KafkaException),
            (Errors::UnknownMemberId, ExpectedClass::KafkaException),
            // STALE_MEMBER_EPOCH is non-retriable here (only retried with a new epoch).
            (Errors::StaleMemberEpoch, ExpectedClass::KafkaException),
            // Generic → KafkaException.
            (Errors::UnknownServerError, ExpectedClass::KafkaException),
        ]
    }

    /// Assert that `err` matches the `ExpectedClass`, mirroring Java's
    /// `assertFutureThrows(expectedExceptionClass, future)`. The Rust
    /// `KafkaError` representation determines how each class is checked.
    fn assert_error_class(err: &KafkaError, expected: ExpectedClass) {
        match expected {
            ExpectedClass::Timeout => {
                assert!(matches!(err, KafkaError::Timeout(_)), "expected TimeoutException, got {err:?}");
            },
            ExpectedClass::GroupAuthorization => {
                assert!(
                    matches!(err, KafkaError::GroupAuthorization(_)),
                    "expected GroupAuthorizationException, got {err:?}"
                );
            },
            ExpectedClass::OffsetMetadataTooLarge => {
                assert_eq!(
                    err.error(),
                    Errors::OffsetMetadataTooLarge,
                    "expected OffsetMetadataTooLarge, got {err:?}"
                );
            },
            ExpectedClass::InvalidCommitOffsetSize => {
                assert_eq!(
                    err.error(),
                    Errors::InvalidCommitOffsetSize,
                    "expected InvalidCommitOffsetSizeException, got {err:?}"
                );
            },
            ExpectedClass::TopicAuthorization => {
                assert!(
                    matches!(err, KafkaError::TopicAuthorization(_)),
                    "expected TopicAuthorizationException, got {err:?}"
                );
            },
            ExpectedClass::CommitFailed => {
                // CommitFailedException flows through ConsumerError::commit_failed
                // → KafkaError::IllegalState (consumer/errors.rs). Distinguished
                // from a generic IllegalState by the "failed" message content.
                assert!(
                    matches!(err, KafkaError::IllegalState(msg) if msg.contains("OffsetCommit failed")),
                    "expected CommitFailedException (IllegalState), got {err:?}"
                );
            },
            ExpectedClass::KafkaException => {
                // Generic KafkaException → KafkaError with UnknownServerError
                // and the "Unexpected error in commit" wrapper message.
                assert_eq!(err.error(), Errors::UnknownServerError, "expected KafkaException, got {err:?}");
            },
        }
    }

    fn topic_partition(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Seed `metadata` with a single-partition topic whose leader epoch is
    /// `seed_epoch`, so a later `maybe_update_last_seen_epoch_if_newer` with a
    /// higher epoch records the update (the Rust `Metadata` only replaces an
    /// existing epoch, never inserts on a null one). Returns nothing; assert
    /// the post-commit epoch via `last_seen_leader_epoch`.
    fn seed_partition_leader_epoch(manager: &CommitRequestManager, tp: &TopicPartition, seed_epoch: i32) {
        manager
            .inner
            .metadata
            .add_transient_topics(HashSet::from([tp.topic().to_string()]));
        let metadata = manager.inner.metadata.metadata_arc();
        let mut counts = HashMap::new();
        counts.insert(tp.topic().to_string(), tp.partition() + 1);
        let tp_owned = tp.clone();
        let response = crate::common::requests::request_test_utils::metadata_update_with_ids(
            "cluster",
            1,
            &HashMap::new(),
            &counts,
            &move |q: &TopicPartition| if *q == tp_owned { Some(seed_epoch) } else { None },
            &HashMap::new(),
        );
        metadata.update_with_current_request_version(&response, false, 0);
    }

    /// Pre-seed a lower leader epoch for `tp` then assert that
    /// `maybe_update_last_seen_epoch_if_newer` (invoked by the commit path)
    /// bumps the cached epoch to `expected`. Mirrors Java's
    /// `verify(metadata).updateLastSeenEpochIfNewer(tp, expected)`.
    fn assert_epoch_updated_to(manager: &CommitRequestManager, tp: &TopicPartition, expected: i32) {
        assert_eq!(
            manager.inner.metadata.metadata_arc().last_seen_leader_epoch(tp),
            Some(expected),
            "commit path must call updateLastSeenEpochIfNewer({tp}, {expected})"
        );
    }

    /// Drive the current-thread runtime until the commit-result receiver
    /// resolves, returning the inner `Result`. Panics if it does not resolve
    /// within the iteration cap or if the sender was dropped.
    async fn recv_commit_result(rx: &mut oneshot::Receiver<CommitResult>) -> CommitResult {
        for _ in 0..128 {
            match rx.try_recv() {
                Ok(result) => return result,
                Err(oneshot::error::TryRecvError::Closed) => panic!("commit sender dropped without sending"),
                Err(oneshot::error::TryRecvError::Empty) => tokio::task::yield_now().await,
            }
        }
        panic!("commit future did not resolve within the iteration cap");
    }

    /// Non-blocking probe of a commit-result receiver: `Some(result)` if
    /// resolved, `None` if still pending. Panics if the sender was dropped.
    fn recv_commit_result_nonblocking(rx: &mut oneshot::Receiver<CommitResult>) -> Option<CommitResult> {
        match rx.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Closed) => panic!("commit sender dropped without sending"),
            Err(oneshot::error::TryRecvError::Empty) => None,
        }
    }

    /// Drive the current-thread runtime until the fetch-result receiver
    /// resolves, returning the inner `Result`.
    async fn recv_fetch_result(rx: &mut oneshot::Receiver<FetchResult>) -> FetchResult {
        for _ in 0..128 {
            match rx.try_recv() {
                Ok(result) => return result,
                Err(oneshot::error::TryRecvError::Closed) => panic!("fetch sender dropped without sending"),
                Err(oneshot::error::TryRecvError::Empty) => tokio::task::yield_now().await,
            }
        }
        panic!("fetch future did not resolve within the iteration cap");
    }

    /// Returns `true` if the receiver has not yet resolved (Java's
    /// `assertFalse(future.isDone())`). Yields once first so any
    /// already-scheduled spawned handler has a chance to complete it — this
    /// makes a "still pending" assertion meaningful rather than racy.
    async fn assert_still_pending<T>(rx: &mut oneshot::Receiver<T>) {
        tokio::task::yield_now().await;
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "expected the future to still be pending"
        );
    }

    /// `testCommitSync`: a successful commit completes the future with the
    /// input offsets and the metadata's `updateLastSeenEpochIfNewer` is
    /// invoked for the offset's leader epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_success_completes_with_offsets_and_updates_epoch() {
        let manager = make_manager(0, false);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let mut offsets = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::with_leader_epoch(0, Some(1), String::new()).expect("oam"),
        );
        // Seed a lower epoch so the commit path's update is observable (Java
        // verifies the call on a mock; the Rust Metadata only replaces an
        // existing epoch — see helper docs).
        seed_partition_leader_epoch(&manager, &tp, 0);

        let mut public_rx = manager.commit_sync(offsets.clone(), i64::MAX, 0);
        assert_eq!(manager.inner.state.lock().unwrap().pending.unsent_offset_commits.len(), 1);

        // Java: verify(metadata).updateLastSeenEpochIfNewer(tp, 1).
        assert_epoch_updated_to(&manager, &tp, 1);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent.handler().on_complete(offset_commit_response_single(&tp, Errors::None));

        let committed = recv_commit_result(&mut public_rx).await.expect("commit succeeds");
        assert_eq!(committed, offsets);
    }

    /// `testCommitAsync`: a successful async commit completes the future with
    /// the input offsets and updates the last-seen leader epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_async_success_completes_and_updates_epoch() {
        let manager = make_manager(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let mut offsets = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::with_leader_epoch(0, Some(1), String::new()).expect("oam"),
        );
        seed_partition_leader_epoch(&manager, &tp, 0);

        let mut public_rx = manager.commit_async_no_callback(offsets.clone(), 0);
        assert_eq!(manager.inner.state.lock().unwrap().pending.unsent_offset_commits.len(), 1);
        assert_epoch_updated_to(&manager, &tp, 1);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent.handler().on_complete(offset_commit_response_single(&tp, Errors::None));

        let committed = recv_commit_result(&mut public_rx).await.expect("async commit succeeds");
        assert_eq!(committed, offsets);
    }

    /// `testCommitSyncRetriedAfterExpectedRetriableException` (×13): a sync
    /// commit that fails with a retriable error is NOT yet done (it is
    /// re-queued for retry); a non-retriable error completes it
    /// exceptionally with the expected class.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_retried_after_expected_retriable_exception() {
        for (error, expected) in offset_commit_exception_supplier() {
            let manager = make_manager(0, false);
            let coordinator = coordinator_with_node();
            let tp = topic_partition("topic", 1);
            // Generous deadline so retriable errors stay pending (not timed out).
            let mut public_rx = manager.commit_sync(singleton_offset(tp.clone(), 0), i64::MAX, 0);

            let unsent = poll_one_unsent(&manager, &coordinator, 0);
            unsent.handler().on_complete(offset_commit_response_single(&tp, error));

            let retriable = error.is_retriable();
            if retriable {
                // Java: assertFalse(commitResult.isDone()); request re-queued.
                assert_still_pending(&mut public_rx).await;
                let pending = manager.inner.state.lock().unwrap().pending.unsent_offset_commits.len();
                assert_eq!(pending, 1, "retriable error {error:?} should re-queue the commit");
            } else {
                let err = recv_commit_result(&mut public_rx)
                    .await
                    .expect_err("non-retriable error fails commit");
                assert_error_class(&err, expected);
            }
        }
    }

    /// `testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires`
    /// (×13): a retriable error, once the deadline expires, surfaces a
    /// TimeoutException; a non-retriable error surfaces its specific class.
    #[tokio::test(flavor = "current_thread")]
    async fn offset_commit_sync_failed_with_retriable_throws_timeout_when_retry_time_expires() {
        for (error, expected) in offset_commit_exception_supplier() {
            let manager = make_manager(0, false);
            let coordinator = coordinator_with_node();
            let tp = topic_partition("topic", 1);
            // Deadline = retryBackoffMs * 2 + 1: the driver advances its local
            // clock by retry_backoff_ms per retriable failure, so a couple of
            // failures cross the deadline (Java sleeps to expire the timeout).
            let retry_backoff_ms = manager.inner.retry_backoff_ms;
            let deadline_ms = retry_backoff_ms.saturating_mul(2) + 1;
            let mut public_rx = manager.commit_sync(singleton_offset(tp.clone(), 0), deadline_ms, 0);

            let retriable = error.is_retriable();
            // Drive send/fail cycles to either expire (retriable) or surface
            // the specific error (non-retriable). The poll time advances each
            // iteration past the re-queued request's seeded backoff so each
            // retry is shipped (the driver's own local clock crosses the
            // deadline and surfaces a Timeout).
            let poll_step = manager.inner.retry_backoff_max_ms.saturating_mul(2).max(retry_backoff_ms * 4);
            let mut poll_time = 0;
            let mut iters = 0;
            let err = loop {
                iters += 1;
                if let Some(unsent) = manager
                    .poll_with_coordinator(&coordinator, poll_time)
                    .unsent_requests
                    .into_iter()
                    .next()
                {
                    unsent.handler().on_complete(offset_commit_response_single(&tp, error));
                }
                if let Some(result) = recv_commit_result_nonblocking(&mut public_rx) {
                    break result.expect_err("commit must fail");
                }
                tokio::task::yield_now().await;
                poll_time = poll_time.saturating_add(poll_step);
                assert!(iters < 200, "commit {error:?} did not resolve within 200 iterations");
            };
            if retriable {
                assert!(
                    matches!(err, KafkaError::Timeout(_)),
                    "retriable {error:?} → Timeout, got {err:?}"
                );
            } else {
                assert_error_class(&err, expected);
            }
        }
    }

    /// `testOffsetCommitAsyncFailedWithRetriableThrowsRetriableCommitException`:
    /// an async commit failing with a retriable error is NOT retried and the
    /// future completes with a `RetriableCommitFailedException` (retriable
    /// `KafkaError`), not a Timeout.
    #[tokio::test(flavor = "current_thread")]
    async fn offset_commit_async_failed_with_retriable_throws_retriable_commit_exception() {
        let manager = make_manager(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let mut public_rx = manager.commit_async_no_callback(singleton_offset(tp.clone(), 0), 0);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent
            .handler()
            .on_complete(offset_commit_response_single(&tp, Errors::CoordinatorNotAvailable));

        let err = recv_commit_result(&mut public_rx).await.expect_err("async commit fails");
        // Java: assertFutureThrows(RetriableCommitFailedException.class). Maps
        // through ConsumerError::retriable_commit_failed → retriable KafkaError.
        assert!(
            err.is_retriable(),
            "async retriable error must surface a retriable commit-failed error, got {err:?}"
        );
        assert!(
            !matches!(err, KafkaError::Timeout(_)),
            "must NOT be a Timeout (async is not retried)"
        );
        // The request is not re-queued (no retry).
        assert!(manager.inner.state.lock().unwrap().pending.unsent_offset_commits.is_empty());
    }

    /// `testOffsetCommitRequestErroredRequestsNotRetriedForAsyncCommit` (×13):
    /// async commit is never retried; retriable errors surface as
    /// RetriableCommitFailedException, non-retriable as their specific class.
    #[tokio::test(flavor = "current_thread")]
    async fn offset_commit_request_errored_requests_not_retried_for_async_commit() {
        for (error, expected) in offset_commit_exception_supplier() {
            let manager = make_manager(0, true);
            let coordinator = coordinator_with_node();
            let tp = topic_partition("topic", 1);
            let mut public_rx = manager.commit_async_no_callback(singleton_offset(tp.clone(), 0), 0);

            let unsent = poll_one_unsent(&manager, &coordinator, 0);
            unsent.handler().on_complete(offset_commit_response_single(&tp, error));

            let err = recv_commit_result(&mut public_rx).await.expect_err("async commit fails");
            // Java only asserts RetriableCommitFailedException for retriable
            // errors; for non-retriable it just checks the future completed
            // exceptionally (no specific class). `expected` is unused here but
            // kept for the iteration tuple shape.
            let _ = expected;
            if error.is_retriable() {
                assert!(err.is_retriable(), "retriable {error:?} → RetriableCommitFailed, got {err:?}");
            }
            // Never re-queued: async commit is not retried.
            assert!(
                manager.inner.state.lock().unwrap().pending.unsent_offset_commits.is_empty(),
                "async commit with {error:?} must not be re-queued"
            );
        }
    }

    /// `testCommitSyncFailsWithCommitFailedExceptionIfUnknownMemberId`:
    /// UNKNOWN_MEMBER_ID → CommitFailedException.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_fails_with_commit_failed_exception_if_unknown_member_id() {
        let manager = make_manager(0, false);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let mut public_rx = manager.commit_sync(singleton_offset(tp.clone(), 0), i64::MAX, 0);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent
            .handler()
            .on_complete(offset_commit_response_single(&tp, Errors::UnknownMemberId));

        let err = recv_commit_result(&mut public_rx).await.expect_err("commit fails");
        assert_error_class(&err, ExpectedClass::CommitFailed);
        // No retry queued.
        assert!(manager.inner.state.lock().unwrap().pending.unsent_offset_commits.is_empty());
    }

    /// `testCommitSyncFailsWithCommitFailedExceptionOnStaleMemberEpoch`:
    /// STALE_MEMBER_EPOCH (no valid epoch) → CommitFailedException.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_fails_with_commit_failed_exception_on_stale_member_epoch() {
        let manager = make_manager(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let mut public_rx = manager.commit_sync(singleton_offset(tp.clone(), 0), i64::MAX, 0);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent
            .handler()
            .on_complete(offset_commit_response_single(&tp, Errors::StaleMemberEpoch));

        let err = recv_commit_result(&mut public_rx).await.expect_err("commit fails");
        assert_error_class(&err, ExpectedClass::CommitFailed);
    }

    /// `testCommitSyncShouldSucceedWithTopicId`: a commit succeeds when the
    /// topic id is known (apiVersion ≥ 10 wire form). The success path
    /// completes with the input offsets and updates the leader epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_should_succeed_with_topic_id() {
        let manager = make_manager(0, false);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        seed_topic_id(&manager, "topic", Uuid::new(7, 9));
        let mut offsets = HashMap::new();
        offsets.insert(
            tp.clone(),
            OffsetAndMetadata::with_leader_epoch(0, Some(1), String::new()).expect("oam"),
        );

        let mut public_rx = manager.commit_sync(offsets.clone(), i64::MAX, 0);
        // The builder selects the topic-id wire form because the id is known.
        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        assert!(
            unsent.request_builder().expect("builder present").latest_allowed_version() >= 10,
            "topic-id commit must use apiVersion >= 10"
        );
        unsent.handler().on_complete(offset_commit_response_single(&tp, Errors::None));

        let committed = recv_commit_result(&mut public_rx).await.expect("commit succeeds");
        assert_eq!(committed, offsets);
        // Java also `verify(metadata).updateLastSeenEpochIfNewer(tp, 1)`; the
        // epoch-update call is exercised by
        // `commit_sync_success_completes_with_offsets_and_updates_epoch` (which
        // seeds a prior epoch so the Rust Metadata records the bump). Here the
        // focus is the topic-id wire form, so the epoch-cache assertion is not
        // duplicated (the same code path runs).
    }

    /// `testCommitSyncShouldSucceedWithUnknownOffsetAndMetadata`: a commit of
    /// an offset with no leader epoch succeeds and does NOT update the
    /// last-seen epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_sync_should_succeed_with_unknown_offset_and_metadata() {
        let manager = make_manager(0, false);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("foo", 1);
        let offsets = singleton_offset(tp.clone(), 0); // OffsetAndMetadata::new → no leader epoch

        let mut public_rx = manager.commit_sync(offsets.clone(), i64::MAX, 0);
        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent.handler().on_complete(offset_commit_response_single(&tp, Errors::None));

        let committed = recv_commit_result(&mut public_rx).await.expect("commit succeeds");
        assert_eq!(committed, offsets);
        // No leader epoch on the offset → cache untouched (Java:
        // verify(metadata, never()).updateLastSeenEpochIfNewer(...)).
        assert_eq!(manager.inner.metadata.metadata_arc().last_seen_leader_epoch(&tp), None);
    }

    /// `testOffsetCommitSingleFailedAttemptPerRequestWhenPartitionErrors`
    /// (×13): a multi-partition error response registers exactly ONE failed
    /// attempt on the (single) re-queued retry request, even though the
    /// response carries 3 partition errors. Non-retriable → no re-queue.
    #[tokio::test(flavor = "current_thread")]
    async fn offset_commit_single_failed_attempt_per_request_when_partition_errors() {
        for (error, _expected) in offset_commit_exception_supplier() {
            let manager = make_manager(0, true);
            let coordinator = coordinator_with_node();
            let mut offsets = HashMap::new();
            offsets.insert(topic_partition("t1", 0), OffsetAndMetadata::new(1).unwrap());
            offsets.insert(topic_partition("t1", 1), OffsetAndMetadata::new(2).unwrap());
            offsets.insert(topic_partition("t1", 2), OffsetAndMetadata::new(3).unwrap());
            let _public_rx = manager.commit_sync(offsets.clone(), i64::MAX, 0);

            let unsent = poll_one_unsent(&manager, &coordinator, 0);
            // Build a 3-partition error response.
            let mut per_partition = HashMap::new();
            for p in 0..3 {
                per_partition.insert(topic_partition("t1", p), error);
            }
            unsent.handler().on_complete(offset_commit_response(per_partition.clone()));

            if error.is_retriable() {
                // Wait for the retry driver to re-enqueue the single retry
                // request, then assert exactly one failed attempt.
                let attempts = yield_until(
                    || {
                        let guard = manager.inner.state.lock().unwrap();
                        guard.pending.unsent_offset_commits.front().map(|r| r.state.num_attempts())
                    },
                    "retriable error did not re-queue a commit",
                )
                .await;
                assert_eq!(
                    attempts, 1,
                    "exactly one failed attempt registered for {error:?} despite 3 partition errors"
                );

                // Second failure → numAttempts becomes 2 (still one increment
                // per response, not per partition).
                let poll_step = manager.inner.retry_backoff_max_ms.saturating_mul(2);
                let unsent2 = poll_one_unsent(&manager, &coordinator, poll_step);
                unsent2.handler().on_complete(offset_commit_response(per_partition.clone()));
                let attempts2 = yield_until(
                    || {
                        let guard = manager.inner.state.lock().unwrap();
                        guard.pending.unsent_offset_commits.front().map(|r| r.state.num_attempts())
                    },
                    "second retriable failure did not re-queue a commit",
                )
                .await;
                assert_eq!(attempts2, 2, "exactly one increment per response for {error:?}");
            } else {
                // Non-retriable → no re-queue (Java: assertNull(commitRequest)).
                // Yield once so the spawned handler observes the failure.
                tokio::task::yield_now().await;
                assert!(
                    manager.inner.state.lock().unwrap().pending.unsent_offset_commits.is_empty(),
                    "non-retriable {error:?} must not re-queue"
                );
            }
        }
    }

    /// `testEnsureBackoffRetryOnOffsetCommitRequestTimeout`: an `on_failure`
    /// with a TimeoutException re-queues the commit (one unsent request) for
    /// retry after backoff.
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_backoff_retry_on_offset_commit_request_timeout() {
        let manager = make_manager(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let _public_rx = manager.commit_sync(singleton_offset(tp.clone(), 0), i64::MAX, 0);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        // Java: res.unsentRequests.get(0).handler().onFailure(now, new TimeoutException()).
        unsent.handler().on_failure(0, KafkaError::timeout("request timed out"));

        // Java: assertTrue(hasUnsentRequests()); one re-queued commit.
        let pending = yield_until(
            || {
                let guard = manager.inner.state.lock().unwrap();
                let n = guard.pending.unsent_offset_commits.len();
                if n > 0 { Some(n) } else { None }
            },
            "timeout failure did not re-queue the commit",
        )
        .await;
        assert_eq!(pending, 1, "timeout failure re-queues exactly one commit for retry");
    }

    /// `testCommitAsyncFailsWithRetriableOnCoordinatorDisconnected`: an async
    /// commit whose request disconnects marks the coordinator unknown and the
    /// future fails with a RetriableCommitFailedException.
    #[tokio::test(flavor = "current_thread")]
    async fn commit_async_fails_with_retriable_on_coordinator_disconnected() {
        let manager = make_manager(0, false);
        let coordinator = Arc::new(coordinator_with_node());
        manager.set_coordinator(Arc::clone(&coordinator));
        let tp = topic_partition("topic", 1);
        let mut public_rx = manager.commit_async_no_callback(singleton_offset(tp.clone(), 0), 0);

        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        // Disconnect surfaces as a transport failure (NetworkException).
        unsent.handler().on_failure(0, KafkaError::new(Errors::NetworkException));

        let err = recv_commit_result(&mut public_rx).await.expect_err("async commit fails");
        // Java: assertFutureThrows(RetriableCommitFailedException.class).
        assert!(
            err.is_retriable(),
            "disconnect → RetriableCommitFailedException (retriable), got {err:?}"
        );
        // Java: assertCoordinatorDisconnectHandling() — coordinator marked unknown.
        assert!(
            coordinator.coordinator().is_none(),
            "disconnect must mark the coordinator unknown"
        );
    }

    /// `testLastEpochSentOnCommit`: `last_epoch_sent_on_commit` reflects the
    /// epoch carried by the request that was actually SENT, and only changes
    /// when the next request is sent — not when `on_member_epoch_updated`
    /// fires. Uses `maybe_auto_commit_sync_before_rebalance` with retries on
    /// STALE_MEMBER_EPOCH and increasing epochs.
    #[tokio::test(flavor = "current_thread")]
    async fn last_epoch_sent_on_commit() {
        // Auto-commit enabled, very long interval (avoid interval auto-commits).
        let manager = make_manager(0, true);
        let coordinator = Arc::new(coordinator_with_node());
        manager.set_coordinator(Arc::clone(&coordinator));
        let subs = manager_subscriptions(&manager);
        let tp = topic_partition("topic", 1);
        {
            let mut s = subs.lock().unwrap();
            s.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            s.seek(&tp, 100).expect("seek");
        }

        // Send auto-commit-before-rebalance, retried on STALE_MEMBER_EPOCH
        // with the latest epochs (long deadline so it keeps retrying).
        let _public_rx = manager.maybe_auto_commit_sync_before_rebalance(i64::MAX, 0);

        let initial_epoch = 1;
        let member_id = "member1".to_string();
        manager.on_member_epoch_updated(Some(initial_epoch), member_id.clone());

        // Ship and fail with STALE_MEMBER_EPOCH. The request carries epoch 1.
        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        unsent
            .handler()
            .on_complete(offset_commit_response_single(&tp, Errors::StaleMemberEpoch));
        // After the request was sent, lastEpochSentOnCommit == 1.
        assert_eq!(manager.last_epoch_sent_on_commit(), Some(initial_epoch));

        // Receive new epoch. lastEpochSentOnCommit should NOT change until the
        // next request is sent.
        manager.on_member_epoch_updated(Some(initial_epoch + 1), member_id.clone());
        assert_eq!(manager.last_epoch_sent_on_commit(), Some(initial_epoch));

        // Wait for the retry to be re-queued, then ship it (carries epoch 2).
        let poll_step = manager.inner.retry_backoff_max_ms.saturating_mul(2);
        let unsent = yield_until_unsent(&manager, &coordinator, poll_step).await;
        unsent
            .handler()
            .on_complete(offset_commit_response_single(&tp, Errors::StaleMemberEpoch));
        assert_eq!(manager.last_epoch_sent_on_commit(), Some(initial_epoch + 1));

        // Receive empty epoch. Next sent request carries no epoch.
        manager.on_member_epoch_updated(None, member_id);
        let unsent = yield_until_unsent(&manager, &coordinator, poll_step.saturating_mul(2)).await;
        unsent
            .handler()
            .on_complete(offset_commit_response_single(&tp, Errors::StaleMemberEpoch));
        assert_eq!(manager.last_epoch_sent_on_commit(), None);
    }

    /// `testSignalClose`: after `signal_close`, a pending async commit is
    /// still drained and emitted on the next poll (the topic is preserved).
    #[tokio::test(flavor = "current_thread")]
    async fn signal_close_drains_pending_commit() {
        let mut manager = make_manager(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let _public_rx = manager.commit_async_no_callback(singleton_offset(tp.clone(), 0), 0);
        manager.signal_close();

        // Java: poll returns the pending commit even while closing.
        let unsent = poll_one_unsent(&manager, &coordinator, 0);
        let builder = unsent.request_builder().expect("builder present");
        // Build the request to inspect its topic (Java reads data().topics()).
        assert_eq!(builder.api_key(), &ApiKeys::OFFSET_COMMIT);
    }

    /// `testPollWithFatalErrorShouldFailAllUnsentRequests`: when the
    /// coordinator becomes unknown AND has a fatal error, poll returns EMPTY
    /// and fails all unsent requests (both commits and fetches).
    ///
    /// Replaces the stale "covered by test_fail_all_with_error_via_coordinator
    /// _fatal" doc claim (that fn never existed).
    #[tokio::test(flavor = "current_thread")]
    async fn poll_with_fatal_error_should_fail_all_unsent_requests() {
        let manager = make_manager(0, true);
        let mut partitions = HashSet::new();
        partitions.insert(topic_partition("test", 0));
        let mut public_rx = manager.fetch_offsets(partitions, 200, 0);
        assert_eq!(manager.inner.state.lock().unwrap().pending.unsent_offset_fetches.len(), 1);

        // Coordinator unknown + fatal error.
        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_fatal_error_for_test(KafkaError::group_authorization(GROUP_ID.to_string()));

        let poll_result = manager.poll_with_coordinator(&coordinator, 200);
        assert!(poll_result.unsent_requests.is_empty(), "fatal poll returns no unsent requests");

        // All unsent requests failed and the pending buffers are emptied.
        let err = recv_fetch_result(&mut public_rx).await.expect_err("fatal error fails fetch");
        assert!(
            matches!(err, KafkaError::GroupAuthorization(_)),
            "fatal error surfaced, got {err:?}"
        );
        let guard = manager.inner.state.lock().unwrap();
        assert!(guard.pending.unsent_offset_fetches.is_empty());
        assert!(guard.pending.unsent_offset_commits.is_empty());
        assert!(guard.pending.inflight_offset_fetches.is_empty());
    }

    /// `testPollWithFatalErrorDuringCoordinatorIsEmptyAndClosing`: closing +
    /// coordinator-empty + fatal error → the pending async commit fails with
    /// the fatal `GroupAuthorizationException`, message "Fatal error".
    #[tokio::test(flavor = "current_thread")]
    async fn poll_with_fatal_error_during_coordinator_is_empty_and_closing() {
        let mut manager = make_manager(0, true);
        let tp = topic_partition("topic", 1);
        let mut commit_rx = manager.commit_async_no_callback(singleton_offset(tp.clone(), 0), 0);
        manager.signal_close();

        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        // Java: new GroupAuthorizationException("Fatal error").
        coordinator.set_fatal_error_for_test(KafkaError::group_authorization_with_message(
            GROUP_ID.to_string(),
            "Fatal error",
        ));

        let poll_result = manager.poll_with_coordinator(&coordinator, 0);
        assert!(poll_result.unsent_requests.is_empty());

        let err = recv_commit_result(&mut commit_rx).await.expect_err("fatal error fails commit");
        assert!(
            matches!(err, KafkaError::GroupAuthorization(_)),
            "expected GroupAuthorizationException, got {err:?}"
        );
        // Java: assertFutureThrows(GroupAuthorizationException.class, future, "Fatal error").
        assert!(
            err.message().contains("Fatal error"),
            "fatal error message must contain 'Fatal error', got {err:?}"
        );
    }

    /// `testPollWithClosingAndPendingRequests`: closing + coordinator-empty
    /// (no fatal error) → the pending async commit fails with
    /// CommitFailedException carrying the exact "Failed to commit offsets:
    /// Coordinator unknown and consumer is closing" message.
    #[tokio::test(flavor = "current_thread")]
    async fn poll_with_closing_and_pending_requests() {
        let mut manager = make_manager(0, true);
        let tp = topic_partition("topic", 1);
        let mut commit_rx = manager.commit_async_no_callback(singleton_offset(tp.clone(), 0), 0);
        manager.signal_close();

        // Coordinator empty, NO fatal error.
        let coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);

        let poll_result = manager.poll_with_coordinator(&coordinator, 0);
        assert!(poll_result.unsent_requests.is_empty());

        let err = recv_commit_result(&mut commit_rx).await.expect_err("closing fails commit");
        // Java: assertFutureThrows(CommitFailedException.class, future,
        //   "Failed to commit offsets: Coordinator unknown and consumer is closing").
        assert!(
            matches!(&err, KafkaError::IllegalState(msg)
                if msg == "Failed to commit offsets: Coordinator unknown and consumer is closing"),
            "expected exact CommitFailedException message, got {err:?}"
        );
    }

    /// `testPollEnsureManualCommitSent`: a manual (async) commit is emitted on
    /// the next poll.
    #[tokio::test(flavor = "current_thread")]
    async fn poll_ensure_manual_commit_sent() {
        let manager = make_manager(0, false);
        let coordinator = coordinator_with_node();
        // Empty poll → nothing.
        assert!(manager.poll_with_coordinator(&coordinator, 0).unsent_requests.is_empty());

        let tp = topic_partition("t1", 0);
        let _rx = manager.commit_async_no_callback(singleton_offset(tp, 0), 0);
        let poll_result = manager.poll_with_coordinator(&coordinator, 0);
        assert_eq!(poll_result.unsent_requests.len(), 1, "manual commit emitted on poll");
    }

    /// `testPollEnsureAutocommitSent` (request-emission half; metric asserts
    /// out of scope): an auto-commit is emitted on poll once the interval
    /// expires and there is a consumed offset.
    #[tokio::test(flavor = "current_thread")]
    async fn poll_ensure_autocommit_sent() {
        let (manager, subs) = make_manager_with_subs(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("t1", 1);
        {
            let mut s = subs.lock().unwrap();
            s.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            s.seek(&tp, 100).expect("seek");
        }
        // Empty poll first.
        assert!(manager.poll_with_coordinator(&coordinator, 0).unsent_requests.is_empty());

        // Advance past the interval and fire the auto-commit hook.
        manager.update_timer_and_maybe_commit(0);
        manager.update_timer_and_maybe_commit(2_000);
        let poll_result = manager.poll_with_coordinator(&coordinator, 2_000);
        assert_eq!(poll_result.unsent_requests.len(), 1, "auto-commit emitted on poll");
        // Drive a successful response (Java does the same; metric asserts skipped).
        let unsent = poll_result.unsent_requests.into_iter().next().unwrap();
        unsent.handler().on_complete(offset_commit_response_single(&tp, Errors::None));
    }

    /// `testPollEnsureCorrectInflightRequestBufferSize`: 2 commits + 2 fetches
    /// produce 4 unsent requests in one poll, 2 inflight fetches, both builder
    /// types present; after responses the inflight buffer drains to 0.
    #[tokio::test(flavor = "current_thread")]
    async fn poll_ensure_correct_inflight_request_buffer_size() {
        let manager = make_manager(0, false);
        let coordinator = coordinator_with_node();

        let mut offsets1 = HashMap::new();
        offsets1.insert(topic_partition("test", 0), OffsetAndMetadata::new(10).unwrap());
        offsets1.insert(topic_partition("test", 1), OffsetAndMetadata::new(20).unwrap());
        let mut offsets2 = HashMap::new();
        offsets2.insert(topic_partition("test", 3), OffsetAndMetadata::new(20).unwrap());
        offsets2.insert(topic_partition("test", 4), OffsetAndMetadata::new(20).unwrap());

        let _c1 = manager.commit_sync(offsets1, i64::MAX, 0);
        let _f1 = manager.fetch_offsets(HashSet::from([topic_partition("test", 0)]), i64::MAX, 0);
        let _c2 = manager.commit_sync(offsets2, i64::MAX, 0);
        let _f2 = manager.fetch_offsets(HashSet::from([topic_partition("test", 1)]), i64::MAX, 0);

        let poll_result = manager.poll_with_coordinator(&coordinator, 0);
        assert_eq!(
            poll_result.unsent_requests.len(),
            4,
            "2 commits + 2 fetches → 4 unsent requests"
        );
        // Both builder types present.
        let mut has_commit = false;
        let mut has_fetch = false;
        for req in &poll_result.unsent_requests {
            match req.request_builder().expect("builder present").api_key() {
                k if *k == ApiKeys::OFFSET_COMMIT => has_commit = true,
                k if *k == ApiKeys::OFFSET_FETCH => has_fetch = true,
                _ => {},
            }
        }
        assert!(has_commit, "an OffsetCommit builder must be present");
        assert!(has_fetch, "an OffsetFetch builder must be present");

        {
            let guard = manager.inner.state.lock().unwrap();
            assert!(!guard.pending.has_unsent_requests(), "no unsent requests left after poll");
            assert_eq!(guard.pending.inflight_offset_fetches.len(), 2, "2 inflight fetches");
        }

        // Complete every request; inflight fetches drain to 0.
        for req in poll_result.unsent_requests {
            match req.request_builder().expect("builder present").api_key() {
                k if *k == ApiKeys::OFFSET_FETCH => {
                    req.handler().on_complete(offset_fetch_response(GROUP_ID, vec![], Errors::None));
                },
                _ => {
                    req.handler().on_complete(offset_commit_response(HashMap::new()));
                },
            }
        }
        yield_until(
            || {
                let guard = manager.inner.state.lock().unwrap();
                if guard.pending.inflight_offset_fetches.is_empty() {
                    Some(())
                } else {
                    None
                }
            },
            "inflight offset fetches did not drain to 0",
        )
        .await;
    }

    /// `testPollEnsureEmptyPendingRequestAfterPoll`: after a single async
    /// commit is polled, the unsent-commit queue and all pending buffers are
    /// empty.
    #[tokio::test(flavor = "current_thread")]
    async fn poll_ensure_empty_pending_request_after_poll() {
        let manager = make_manager(0, true);
        let coordinator = coordinator_with_node();
        let tp = topic_partition("topic", 1);
        let _rx = manager.commit_async_no_callback(singleton_offset(tp, 0), 0);
        assert_eq!(manager.inner.state.lock().unwrap().pending.unsent_offset_commits.len(), 1);

        let poll_result = manager.poll_with_coordinator(&coordinator, 0);
        assert_eq!(poll_result.unsent_requests.len(), 1);

        let guard = manager.inner.state.lock().unwrap();
        assert!(
            guard.pending.unsent_offset_commits.is_empty(),
            "unsentOffsetCommitRequests() empty after poll"
        );
        assert!(guard.pending.unsent_offset_fetches.is_empty());
        assert!(guard.pending.inflight_offset_fetches.is_empty());
    }

    /// Helper for `last_epoch_sent_on_commit`: returns the manager's
    /// subscription-state handle. The `make_manager` ctor does not expose it,
    /// so reach through `inner`.
    fn manager_subscriptions(manager: &CommitRequestManager) -> Arc<Mutex<SubscriptionState>> {
        Arc::clone(&manager.inner.subscriptions)
    }

    /// Poll repeatedly (advancing `now_ms` each iteration past the re-queued
    /// request's backoff) until exactly one unsent request is shipped, then
    /// return it. Used by retry tests where the re-enqueue happens on a
    /// spawned task and the request is only sendable after its backoff.
    async fn yield_until_unsent(
        manager: &CommitRequestManager,
        coordinator: &CoordinatorRequestManager,
        now_ms: i64,
    ) -> UnsentRequest {
        for _ in 0..64 {
            if let Some(unsent) = manager
                .poll_with_coordinator(coordinator, now_ms)
                .unsent_requests
                .into_iter()
                .next()
            {
                return unsent;
            }
            tokio::task::yield_now().await;
        }
        panic!("no unsent request shipped within the iteration cap");
    }
}
