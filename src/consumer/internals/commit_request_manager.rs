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
use super::offset_commit_callback_invoker::OffsetCommitCallbackInvoker;
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
        });
        Self { inner }
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
    pub(crate) fn update_timer_and_maybe_commit(&mut self, current_time_ms: i64) {
        // Java: updateTimerAndMaybeCommit — ensures the auto-commit timer
        // reflects the latest poll/event tick before potentially firing.
        self.maybe_auto_commit_async(current_time_ms);
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
                member_info,
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
        let (request, request_rx) = OffsetCommitRequestState::new(
            offsets.clone(),
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
        let offsets_for_callback = offsets.clone();
        tokio::spawn(async move {
            // Resolve the public future + enqueue the callback when the
            // request completes. Java wraps retriable errors with
            // `RetriableCommitFailedException` for the async path.
            let outcome = request_rx.await;
            let (success_value, callback_err) = match outcome {
                Ok(Ok(_committed_offsets)) => (Some(offsets_for_callback.clone()), None),
                Ok(Err(err)) => {
                    let mapped = if err.is_retriable() {
                        KafkaError::from(ConsumerError::retriable_commit_failed_with_cause(err))
                    } else {
                        err
                    };
                    (None, Some(mapped))
                },
                Err(_recv_err) => {
                    // Sender dropped without sending — treat as a generic
                    // failure. This should not happen in steady state.
                    (None, Some(KafkaError::new(Errors::UnknownServerError)))
                },
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
            fetch_offsets_with_retries(inner, request_rx, result_tx, deadline_ms, now_ms).await;
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
    /// Mirrors Java's `poll(long currentTimeMs)`.
    pub(crate) fn poll_with_coordinator(
        &mut self,
        coordinator: &mut CoordinatorRequestManager,
        current_time_ms: i64,
    ) -> PollResult {
        let closing = *self.inner.closing.lock().expect("commit manager closing flag poisoned");

        // Java: if coordinator is unknown, fail unsent commits if closing.
        if coordinator.coordinator().is_none() {
            let mut guard = self.inner.state.lock().expect("commit manager state poisoned");
            if let Some(err) = coordinator.fatal_error().cloned() {
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
                // Mirrors Java's `lastEpochSentOnCommit = memberInfo.memberEpoch`
                // writeback inside `createOffsetCommitRequest`. Done here
                // because `build_offset_commit_unsent_request` cannot
                // re-acquire the state lock (non-reentrant).
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

    fn maybe_auto_commit_async(&mut self, current_time_ms: i64) {
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
            let mut guard = inner.state.lock().expect("commit manager state poisoned");
            if let Some(ac) = guard.auto_commit.as_mut() {
                ac.set_inflight_commit_status(false);
            }
            match outcome {
                Ok(Ok(_committed)) => {
                    log::debug!("Completed asynchronous auto-commit of offsets");
                },
                Ok(Err(err)) => {
                    if err.is_retriable() {
                        log::debug!("Asynchronous auto-commit of offsets failed due to retriable error: {err}");
                        if let Some(ac) = guard.auto_commit.as_mut() {
                            ac.reset_timer_with_backoff(current_time_ms, inner.retry_backoff_ms);
                        }
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
    classify_and_complete_commit(&inner.group_id, request, &response);
}

fn classify_and_complete_commit(group_id: &str, request: OffsetCommitRequestState, response: &OffsetCommitResponse) {
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
    _inner: &Arc<CommitRequestManagerInner>,
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

impl CommitRequestManagerInner {
    fn coordinator_node(&self) -> Option<crate::common::Node> {
        // The bg task owns the CoordinatorRequestManager directly; the
        // commit manager's UnsentRequest needs the coordinator node at
        // build time. The node is set by the bg task wiring (Phase 10),
        // which calls `poll_with_coordinator(coordinator, ...)`.
        // For Phase 9 unit tests, we don't have a coordinator wired in;
        // returning `None` means the NetworkClientDelegate will pick the
        // least-loaded node (used as a fallback in tests).
        None
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
                let retriable = err.is_retriable();
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
    member_info: MemberInfo,
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
    let _last_offsets = initial_offsets;
    // Java's `isStaleEpochErrorAndValidEpochAvailable` requires
    // `memberInfo.memberEpoch.isPresent()` (`CommitRequestManager.java:573-575`).
    // Captured here once at driver entry because `member_info` does not
    // change across retries within a single driver invocation.
    let has_valid_member_epoch = member_info.member_epoch.is_some();
    let outcome: Result<(), KafkaError> = loop {
        match request_rx.await {
            Ok(Ok(_committed)) => break Ok(()),
            Ok(Err(err)) => {
                // Java line 349: enter the retry gate only when the error
                // is a RetriableException OR the stale-epoch case AND a
                // valid member epoch is currently known.
                let is_stale_epoch_with_valid_epoch = err.error() == Errors::StaleMemberEpoch && has_valid_member_epoch;
                let is_retriable_for_rebalance = err.is_retriable() || is_stale_epoch_with_valid_epoch;
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

async fn fetch_offsets_with_retries(
    _inner: Arc<CommitRequestManagerInner>,
    request_rx: oneshot::Receiver<FetchResult>,
    result_tx: FetchFutureTx,
    _deadline_ms: i64,
    _now_ms: i64,
) {
    let outcome = match request_rx.await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(KafkaError::new(Errors::NetworkException)),
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
    //! Translated subset of
    //! `org.apache.kafka.clients.consumer.internals.CommitRequestManagerTest`.
    //!
    //! The Java file is 1975 LOC with 50 test cases, many of which rely on
    //! Mockito mocks of `BackgroundEventHandler`, `Metrics`, and
    //! `MembershipManager` internals — those are deferred to Phase 11 with
    //! a one-line rationale each (DoD §3):
    //!
    //!   - testEnsureBackgroundEventHandlerUsedOnCommitAsyncWhenFatalError:
    //!     requires the BackgroundEventHandler wiring (Phase 11).
    //!   - testEnsureCorrectMetricRecordedForCommitLatency: requires the
    //!     metrics framework (out of scope per CLAUDE.md and Phase 9 plan).
    //!   - testInflightOffsetFetchRequestsDuringMembershipFenced: requires
    //!     ConsumerMembershipManager (Phase 8) — wired via Phase 11.
    //!   - testFencedInstanceIdException / testAutoCommitOnLeavingGroup /
    //!     similar membership transitions: require Phase 8 membership
    //!     manager + Phase 11 BG-task wiring.
    //!   - testPollWithFatalErrorShouldFailAllUnsentRequests-via-bgEvent:
    //!     covered by `test_fail_all_with_error_via_coordinator_fatal`
    //!     below in a simpler form (no event-handler dependency).
    //!
    //! The tests below cover the core state-machine and request-building
    //! behaviour that's testable without the Phase 8 / 10 / 11 wiring.

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
        let mut manager = manager;
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
        let mut coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let poll_result = manager.poll_with_coordinator(&mut coordinator, after_expiry_ms);
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
        let mut manager = manager;
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
        let mut manager = make_manager(0, false);
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
        let mut coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let mut manager = manager;
        let poll_result = manager.poll_with_coordinator(&mut coordinator, 1);
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

        let mut manager = make_manager(0, false);
        // Short deadline so a small number of retriable failures trips
        // it. The retry driver advances its local `current_time_ms` by
        // `retry_backoff_ms` per retriable error; once that local clock
        // crosses `deadline_ms`, the driver surfaces a TimeoutException.
        let retry_backoff_ms = manager.inner.retry_backoff_ms;
        let deadline_ms = retry_backoff_ms.saturating_mul(2) + 1;
        let tp = TopicPartition::new("t".to_string(), 0);
        let public_rx = manager.commit_sync(singleton_offset(tp.clone(), 100), deadline_ms, 0);

        let mut coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
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
            let poll_result = manager.poll_with_coordinator(&mut coordinator, poll_time_ms);
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
        let mut coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let mut manager = manager;
        let poll_result = manager.poll_with_coordinator(&mut coordinator, 1);
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

        let mut coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let mut manager = manager;
        let poll_result = manager.poll_with_coordinator(&mut coordinator, 1);
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

        let mut coordinator = CoordinatorRequestManager::new(100, 1_000, GROUP_ID);
        coordinator.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
        let mut manager = manager;
        let poll_result = manager.poll_with_coordinator(&mut coordinator, 1);
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
}
