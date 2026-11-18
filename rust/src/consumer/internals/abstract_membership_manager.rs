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

//! `AbstractMembershipManager` — core state + state machine + reconcile
//! pipeline for the KIP-848 consumer membership manager.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager`.
//!
//! # Translation notes
//!
//! Java models this as `abstract class AbstractMembershipManager<R extends
//! AbstractResponse>` with concrete subclasses for Consumer / Share /
//! Streams (Streams is out of scope).
//!
//! Composition over inheritance, as for `AbstractFetch`: this struct holds
//! the shared membership-state fields (group ID, member ID / epoch, current /
//! target assignments, listener registrations) and the base-class methods.
//! The subclass ([`super::ConsumerMembershipManager`]) composes it as its
//! `abstract_mm` field and provides its group-type configuration.
//!
//! Java's `abstract` methods, and the `protected` methods a subclass
//! overrides, are the [`MembershipManagerHooks`] trait, which the subclass
//! implements. A base method that calls one takes the subclass as a `hooks`
//! argument (`self.abstract_mm.transition_to_fenced(self, now)`), the Rust
//! stand-in for Java's virtual dispatch on `this`; methods that need no hook
//! take only `&self`. The subclass keeps thin forwarding methods with the Java
//! names, so its callers see the inherited methods as they do in Java.
//!
//! The state is shared via `Arc<Mutex<MembershipInner>>` between the
//! membership manager itself and
//! [`super::ConsumerHeartbeatRequestManager`]
//! — both touch the state from the bg task (Java models the same shape
//! with both managers holding plain references). Per
//! `consumer-threading.md` §16, the lock is `std::sync::Mutex` (short
//! critical sections, never crossing `.await`).
//!
//! # §31 — the critical contract
//!
//! [`reconcile`] enqueues `BackgroundEvent::PartitionsRemoved`
//! events with `oneshot::Sender<Result<(), Error>>` and awaits the
//! matching receiver before advancing the membership state machine.
//! `MutexGuard`s on `MembershipInner` are scoped tightly so they are
//! ALWAYS dropped before any `.await`.

#![expect(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::Metadata;
use crate::common::internals::KafkaFutureImpl;
use crate::common::protocol::Errors;
use crate::common::utils::Time;
use crate::common::{Error, TopicPartition, Uuid};
use crate::consumer::GroupMembershipOperation;
use crate::consumer::internals::events::BackgroundEventHandler;
use crate::consumer::internals::metrics::RebalanceMetricsManager;

use super::MemberState;
use super::MemberStateListener;
use super::SubscriptionState;

/// A member's reconciled (or target) assignment, keyed by topic ID, with
/// a local epoch that bumps every time the value changes.
///
/// Java: `AbstractMembershipManager.LocalAssignment`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager$LocalAssignment")]
pub(crate) struct LocalAssignment {
    pub(crate) local_epoch: i64,
    pub(crate) partitions: HashMap<Uuid, Vec<i32>>,
}

impl LocalAssignment {
    /// Sentinel meaning "no local assignment" (no group / never
    /// reconciled).
    ///
    /// Java: `LocalAssignment.NONE_EPOCH`.
    pub(crate) const NONE_EPOCH: i64 = -1;

    /// Java: `LocalAssignment.NONE`.
    pub(crate) fn none() -> Self {
        Self { local_epoch: Self::NONE_EPOCH, partitions: HashMap::new() }
    }

    /// Construct a new assignment. Panics in Rust would be unsafe for
    /// public API; we return `Result` and let the caller propagate.
    ///
    /// Java: `new LocalAssignment(localEpoch, partitions)`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager$LocalAssignment#LocalAssignment"
    )]
    pub(crate) fn new(local_epoch: i64, partitions: HashMap<Uuid, Vec<i32>>) -> Result<Self, Error> {
        if local_epoch == Self::NONE_EPOCH && !partitions.is_empty() {
            return Err(Error::local_illegal_argument("Local epoch must be set if there are partitions"));
        }
        Ok(Self { local_epoch, partitions })
    }

    /// Returns `true` if this is the NONE sentinel.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager$LocalAssignment#isNone")]
    pub(crate) fn is_none(&self) -> bool {
        self.local_epoch == Self::NONE_EPOCH
    }

    /// Update with a new assignment. Returns `None` if the assignment
    /// is unchanged; otherwise a new `LocalAssignment` with epoch
    /// `local_epoch + 1`.
    ///
    /// Java: `Optional<LocalAssignment> updateWith(Map<...>)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager$LocalAssignment#updateWith")]
    pub(crate) fn update_with(&self, assignment: HashMap<Uuid, Vec<i32>>) -> Option<LocalAssignment> {
        if self.local_epoch != Self::NONE_EPOCH && assignment == self.partitions {
            return None;
        }
        Some(LocalAssignment { local_epoch: self.local_epoch + 1, partitions: assignment })
    }
}

/// The shared mutable membership state. Locked by `std::sync::Mutex` per
/// `consumer-threading.md` §16. Short critical sections only — never
/// hold the guard across `.await`.
pub(crate) struct MembershipInner {
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    pub(crate) member_epoch: i32,
    pub(crate) state: MemberState,
    pub(crate) current_assignment: LocalAssignment,
    pub(crate) current_target_assignment: LocalAssignment,
    /// Local cache of assigned topic IDs → topic names. Populated as
    /// we resolve topic names from metadata.
    pub(crate) assigned_topic_names_cache: HashMap<Uuid, String>,
    /// True while a `reconcile` call is in flight. Java's
    /// `reconciliationInProgress` flag.
    pub(crate) reconciliation_in_progress: bool,
    /// True if the member rejoined while a reconciliation was in
    /// flight. Java's `rejoinedWhileReconciliationInProgress`.
    pub(crate) rejoined_while_reconciliation_in_progress: bool,
    /// `true` if the leave was triggered by an expired poll timer.
    pub(crate) is_poll_timer_expired: bool,
    /// `true` once `on_subscription_updated()` has been called and not
    /// yet consumed by `on_consumer_poll()`. Java models this as an
    /// `AtomicBoolean`; the surrounding `Mutex` provides the same
    /// atomicity here.
    pub(crate) subscription_updated: bool,
    /// `true` while the STALE-member onPartitionsLost assignment release
    /// (`transition_to_stale`) is in flight. Mirrors the lifetime of
    /// Java's `staleMemberAssignmentRelease` `CompletableFuture` between
    /// its creation in `transitionToStale()` and its `whenComplete`
    /// firing (`AbstractMembershipManager#transitionToStale`). While this is
    /// `true`, `maybe_rejoin_stale_member` must NOT transition STALE →
    /// JOINING — it records the intent in
    /// [`Self::stale_rejoin_requested`] and the release-completion path
    /// performs the transition (Java's
    /// `staleMemberAssignmentRelease.whenComplete((__, e) ->
    /// transitionToJoining())`).
    pub(crate) stale_assignment_release_pending: bool,
    /// `true` if `maybe_rejoin_stale_member` was called while the STALE
    /// assignment release was still in flight. The release-completion
    /// path reads this to know it must transition to JOINING once the
    /// callback returns. Mirrors Java chaining `transitionToJoining` onto
    /// the in-flight `staleMemberAssignmentRelease` future.
    pub(crate) stale_rejoin_requested: bool,
    /// If the member is currently leaving the group after a call to
    /// `leave_group` or `leave_group_on_close`, this holds a future that
    /// completes when the ongoing leave operation completes (the response
    /// to the leave heartbeat is received, the leave heartbeat could not be
    /// sent, or the member is fenced / fails while leaving). `None` when no
    /// leave is in progress.
    ///
    /// Java: `Optional<CompletableFuture<Void>> leaveGroupInProgress`
    /// (`AbstractMembershipManager#leaveGroupInProgress`). Completed only through
    /// [`AbstractMembershipManager::maybe_complete_leave_in_progress`] (or
    /// [`MembershipInner::take_leave_in_progress`] where the caller already
    /// holds the guard), always after the guard is released, because the
    /// completion runs the chained continuations (the `Unsubscribe` /
    /// `LeaveGroupOnClose` event handles).
    pub(crate) leave_group_in_progress: Option<KafkaFutureImpl<()>>,
    /// Whether auto-commit is enabled (immutable for the manager's
    /// lifetime; stored here for state-machine queries).
    pub(crate) auto_commit_enabled: bool,
    /// Registered listeners notified on member-epoch / assignment
    /// changes. Phase 8 partial introduced this trait; Phase 8b uses
    /// it.
    pub(crate) state_updates_listeners: Vec<Arc<dyn MemberStateListener>>,
    /// Java: `RebalanceMetricsManager metricsManager`. Records rebalance
    /// start/end/failure. `None` in tests that build the membership manager
    /// without a metrics registry; the live consumer always wires it
    /// (M5). The group type supplies the implementation (Java's abstract
    /// `RebalanceMetricsManager`).
    pub(crate) metrics_manager: Option<Arc<dyn RebalanceMetricsManager>>,
    /// Java: `Time time`. Clock used to stamp rebalance start/end. Defaults to
    /// the metrics `SystemTime`; the consumer overrides it to share the
    /// metrics clock when it wires `metrics_manager` (M5).
    pub(crate) time: Arc<dyn Time>,
}

impl MembershipInner {
    /// Update the member state, setting it to the next state only if
    /// it is a valid transition.
    ///
    /// Java: `transitionTo(MemberState)`.
    pub(crate) fn transition_to(&mut self, next_state: MemberState) -> Result<(), Error> {
        if self.state != next_state && !next_state.previous_valid_states().contains(&self.state) {
            return Err(Error::local_illegal_state(format!(
                "Invalid state transition from {} to {}",
                self.state, next_state
            )));
        }

        // Java: record rebalance end/start when crossing the RECONCILING
        // boundary (`AbstractMembershipManager.transitionTo`).
        if let Some(metrics) = &self.metrics_manager {
            if Self::is_completing_rebalance(self.state, next_state) {
                metrics.record_rebalance_ended(self.time.milliseconds());
            }
            if Self::is_starting_rebalance(self.state, next_state) {
                metrics.record_rebalance_started(self.time.milliseconds());
            }
        }

        log::info!(
            "Member {} with epoch {} transitioned from {} to {}.",
            self.member_id,
            self.member_epoch,
            self.state,
            next_state
        );
        self.state = next_state;
        // AK 4.3.1 (KAFKA-20106): notify state listeners of the transition.
        // Java: `stateUpdatesListeners.forEach(listener -> listener.onMemberStateChange(nextState));`
        for listener in &self.state_updates_listeners {
            listener.on_member_state_change(next_state);
        }
        Ok(())
    }

    /// Java: `isCompletingRebalance(currentState, nextState)`.
    fn is_completing_rebalance(current_state: MemberState, next_state: MemberState) -> bool {
        current_state == MemberState::Reconciling
            && (next_state == MemberState::Stable || next_state == MemberState::Acknowledging)
    }

    /// Java: `isStartingRebalance(currentState, nextState)`.
    fn is_starting_rebalance(current_state: MemberState, next_state: MemberState) -> bool {
        current_state != MemberState::Reconciling && next_state == MemberState::Reconciling
    }

    /// Java: `notifyEpochChange(Optional<Integer> epoch)`.
    pub(crate) fn notify_epoch_change(&self, epoch: Option<i32>) {
        for listener in &self.state_updates_listeners {
            listener.on_member_epoch_updated(epoch, &self.member_id);
        }
    }

    /// Java: `notifyAssignmentChange(Set<TopicPartition>)`.
    pub(crate) fn notify_assignment_change(&self, partitions: &HashSet<TopicPartition>) {
        for listener in &self.state_updates_listeners {
            listener.on_group_assignment_updated(partitions);
        }
    }

    /// Take the in-progress leave future, if any, leaving `None` behind —
    /// the first half of Java's `maybeCompleteLeaveInProgress()` for call sites
    /// that hold the guard. The caller completes the returned future with
    /// [`AbstractMembershipManager::complete_leave`] AFTER dropping the guard.
    pub(crate) fn take_leave_in_progress(&mut self) -> Option<KafkaFutureImpl<()>> {
        self.leave_group_in_progress.take()
    }

    /// Java: `isNotInGroup()`.
    pub(crate) fn is_not_in_group(&self) -> bool {
        matches!(
            self.state,
            MemberState::Unsubscribed | MemberState::Fenced | MemberState::Fatal | MemberState::Stale
        )
    }

    /// Java: `targetAssignmentReconciled()`.
    pub(crate) fn target_assignment_reconciled(&self) -> bool {
        self.current_assignment == self.current_target_assignment
    }

    /// Java: `shouldHeartbeatNow()`.
    pub(crate) fn should_heartbeat_now(&self) -> bool {
        matches!(
            self.state,
            MemberState::Acknowledging | MemberState::Leaving | MemberState::Joining
        )
    }

    /// Java: `shouldSkipHeartbeat()`.
    pub(crate) fn should_skip_heartbeat(&self) -> bool {
        matches!(
            self.state,
            MemberState::Unsubscribed | MemberState::Fatal | MemberState::Stale | MemberState::Fenced
        )
    }

    /// Java: `isLeavingGroup()` (base implementation, before the
    /// Consumer subclass's `groupInstanceId` / `leaveGroupOperation`
    /// override).
    pub(crate) fn is_leaving_group_base(&self) -> bool {
        matches!(self.state, MemberState::PrepareLeaving | MemberState::Leaving)
    }

    /// Java: `updateMemberEpoch(int newEpoch)`.
    pub(crate) fn update_member_epoch(&mut self, new_epoch: i32) {
        let new_epoch_received = self.member_epoch != new_epoch;
        self.member_epoch = new_epoch;
        if new_epoch_received {
            if self.member_epoch > 0 {
                self.notify_epoch_change(Some(self.member_epoch));
            } else {
                self.notify_epoch_change(None);
            }
        }
    }

    /// Java: `clearPendingAssignmentsAndLocalNamesCache()`.
    pub(crate) fn clear_pending_assignments_and_local_names_cache(&mut self) {
        self.current_target_assignment = LocalAssignment::none();
        self.assigned_topic_names_cache.clear();
    }
}

/// The group-type-specific half of Java's `AbstractMembershipManager<R>`:
/// its `abstract` methods and the `protected` methods a subclass overrides.
/// Implemented by each membership manager (the consumer's
/// [`super::ConsumerMembershipManager`], and the share consumer's from P4b).
/// Methods with a default body translate a Java base method that has one;
/// the rest are Java's `abstract` methods.
///
/// The base methods of [`AbstractMembershipManager`] that call a hook take the
/// implementing subclass as a `hooks` argument (see the module docs).
pub(crate) trait MembershipManagerHooks: Send + Sync + 'static {
    /// Java's type parameter `R extends AbstractResponse`: the group type's
    /// heartbeat response.
    type Response;

    /// The heartbeat response's error code.
    ///
    /// Java: `protected abstract short errorCode(R response)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#errorCode")]
    fn error_code(response: &Self::Response) -> i16;

    /// The member epoch the heartbeat response carries. Named after CLAUDE.md
    /// §2's overload rule: Java's `memberEpoch(R)` sits beside the
    /// `memberEpoch()` getter ([`AbstractMembershipManager::member_epoch`]).
    ///
    /// Java: `protected abstract int memberEpoch(R response)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#memberEpoch")]
    fn member_epoch_with_response(response: &Self::Response) -> i32;

    /// The new target assignment the heartbeat response carries, if any, with
    /// each topic's partitions sorted and deduplicated (Java builds a
    /// `TreeSet`).
    ///
    /// Java: `protected abstract Optional<Map<Uuid, SortedSet<Integer>>>
    /// extractAssignment(R response)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#extractAssignment")]
    fn extract_assignment(response: &Self::Response) -> Option<HashMap<Uuid, Vec<i32>>>;

    /// The epoch a member uses to join the group.
    ///
    /// Java: `abstract int joinGroupEpoch()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#joinGroupEpoch")]
    fn join_group_epoch(&self) -> i32;

    /// The epoch a member uses to leave the group.
    ///
    /// Java: `abstract int leaveGroupEpoch()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#leaveGroupEpoch")]
    fn leave_group_epoch(&self) -> i32;

    /// Signals that the assignment has been lost so that group-type actions
    /// can run: called by the fence, fatal and stale transitions with the
    /// partitions to release. Returns the ack of the resulting callback, or
    /// `None` when there is nothing to wait for (Java's completed future).
    ///
    /// Java: `protected CompletableFuture<Void> signalPartitionsLost(Set<TopicPartition>)`,
    /// whose base body returns `CompletableFuture.completedFuture(null)`.
    /// Signals that the member is leaving the group so that group-type actions
    /// can run (the consumer releases its assignment through
    /// `onPartitionsRevoked` / `onPartitionsLost`). Returns the ack the leave
    /// waits on before clearing the assignment, or `None` (Java's completed
    /// future).
    ///
    /// Java: `protected CompletableFuture<Void> signalMemberLeavingGroup()`,
    /// whose base body returns `CompletableFuture.completedFuture(null)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalMemberLeavingGroup")]
    fn signal_member_leaving_group(&self, _current_time_ms: i64) -> Result<Option<SignalAck>, Error> {
        Ok(None)
    }

    /// Signals that reconciliation has started so that group-type actions can
    /// run (the consumer's auto-commit before the rebalance). Returns the ack
    /// the reconciliation waits on before revoking, or `None` (Java's
    /// completed future). `current_time_ms` replaces Java's `time.milliseconds()`.
    ///
    /// Java: `protected CompletableFuture<Void> signalReconciliationStarted()`,
    /// whose base body returns `CompletableFuture.completedFuture(null)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalReconciliationStarted")]
    fn signal_reconciliation_started(&self, _current_time_ms: i64) -> Option<SignalAck> {
        None
    }

    /// Signals that reconciliation is completing so that group-type actions
    /// can run. The base does nothing.
    ///
    /// Java: `protected void signalReconciliationCompleting()` (an empty body).
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalReconciliationCompleting"
    )]
    fn signal_reconciliation_completing(&self, _current_time_ms: i64) {}

    /// Signals that the reconciliation revokes `partitions_revoked` so that
    /// group-type actions can run (the consumer's `onPartitionsRevoked`).
    /// Returns the ack the reconciliation waits on before assigning, or
    /// `None` (Java's completed future). Only called with a non-empty set.
    ///
    /// Java: `public CompletableFuture<Void> signalPartitionsRevoked(Set<TopicPartition>)`,
    /// whose base body returns `CompletableFuture.completedFuture(null)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalPartitionsRevoked")]
    fn signal_partitions_revoked(
        &self,
        _partitions_revoked: Vec<TopicPartition>,
        _current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error> {
        Ok(None)
    }

    /// Signals that the reconciled assignment is `assigned_partitions`, of
    /// which `added_partitions` are new, so that the group type applies it.
    /// Returns the ack the reconciliation waits on before acknowledging, or
    /// `None` when the assignment was applied inline (Java's completed future).
    ///
    /// Java: `protected abstract CompletableFuture<Void>
    /// signalPartitionsAssigned(TopicIdPartitionSet, SortedSet<TopicPartition>)`.
    /// Rust passes the topic-name partitions (`TopicIdPartitionSet#topicPartitions`),
    /// which is all either Java subclass reads from the set.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalPartitionsAssigned")]
    fn signal_partitions_assigned(
        &self,
        assigned_partitions: Vec<TopicPartition>,
        added_partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error>;

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalPartitionsLost")]
    fn signal_partitions_lost(
        &self,
        _partitions_lost: Vec<TopicPartition>,
        _current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error> {
        Ok(None)
    }
}

/// The completion of a group-type signal: the Rust stand-in for the
/// `CompletableFuture<Void>` Java's `signal*` hooks return. The base keeps the
/// receiver across background-loop iterations and `try_recv`s it (§31), so a
/// pending signal never blocks the loop. A hook returns `None` where Java
/// returns an already completed future.
pub(crate) type SignalAck = oneshot::Receiver<Result<(), Error>>;

/// Cross-iteration state for a reconcile callback awaiting its app-side
/// ack (Phase 41b). Mirrors the resume points of Java's
/// `revokeAndAssign(...).whenComplete(...)` chain.
pub(crate) enum PendingReconcile {
    /// The auto-commit-before-rebalance was TRIGGERED (`reconcile` step 8a,
    /// `signalReconciliationStarted()` → `maybeAutoCommitSyncBeforeRebalance`)
    /// and we are awaiting its result [`oneshot::Receiver`]. On completion we
    /// resume at step 8b (abort check → `onPartitionsRevoked` enqueue),
    /// mirroring Java's `commitResult.whenComplete(...)` continuation
    /// (`AbstractMembershipManager#maybeReconcile`).
    ///
    /// Milestone-12 divergence fix (PR #176): Java's `whenComplete` runs the
    /// continuation when the commit resolves while the `ConsumerNetworkThread`
    /// keeps spinning and `markReconciliationCheckCompleted()` has ALREADY
    /// fired (`ApplicationEventProcessor#process(AsyncPollEvent)`). The pre-fix Rust
    /// awaited `commit_rx` inline inside `reconcile()`, so the AsyncPoll task
    /// only marked `reconciliation_check_complete` after the commit resolved,
    /// stalling `poll()` (empty returns) during a slow pre-rebalance commit
    /// even for RETAINED partitions. Storing this leg lets `reconcile()` return
    /// immediately (member stays `RECONCILING`), so the mark fires right away
    /// and retained-partition fetching proceeds; the bg loop `try_recv`s the
    /// commit result on a later iteration (§31 — never awaited).
    AwaitingCommit {
        commit_rx: oneshot::Receiver<Result<(), Error>>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        revoked: HashSet<TopicPartition>,
        current_time_ms: i64,
    },
    /// `onPartitionsRevoked` was enqueued (`reconcile` step 9). On ack we
    /// resume at step 10 (abort check → assign-callback enqueue).
    AfterRevoke {
        ack_rx: oneshot::Receiver<Result<(), Error>>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    },
    /// `onPartitionsAssigned` was enqueued (`reconcile` step 13). On ack we
    /// resume at step 14 (enable-partitions / failure path → ACKNOWLEDGING).
    AfterAssign {
        ack_rx: oneshot::Receiver<Result<(), Error>>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    },
}

/// Cross-iteration state for a release-path `onPartitionsLost` callback
/// awaiting its app-side ack (Phase 41, Issue 2). Mirrors the
/// `callbackResult.whenComplete(...)` tail of Java's `transitionToFenced` /
/// `transitionToFatal` / `transitionToStale`. Each variant carries the ack
/// receiver and identifies the type-specific tail to run once the listener
/// returns. Unlike [`PendingReconcile`], there is no revoke→assign
/// sequencing — `onPartitionsLost` is a single callback.
enum PendingRelease {
    /// `transitionToFenced` release tail: `clearAssignment()` then, if still
    /// `FENCED`, `transitionToJoining()`.
    Fenced {
        ack_rx: oneshot::Receiver<Result<(), Error>>,
    },
    /// `transitionToFatal` release tail: `clearAssignment()`.
    Fatal {
        ack_rx: oneshot::Receiver<Result<(), Error>>,
    },
    /// `transitionToStale` release tail: `clearAssignment()`, clear the
    /// release-pending flag, and (if a timer reset requested a rejoin while
    /// the release was in flight) `transitionToJoining()`.
    Stale {
        ack_rx: oneshot::Receiver<Result<(), Error>>,
    },
}

/// Discriminator for the release-tail to run in
/// [`AbstractMembershipManager::drive_pending_release`], decoupled from the
/// owned ack receiver so the `try_recv` / store-back handling stays uniform.
#[derive(Clone, Copy)]
enum ReleaseKind {
    Fenced,
    Fatal,
    Stale,
}

/// Public surface of the membership manager — the parts of
/// `AbstractMembershipManager` that don't depend on the Consumer-specific
/// hooks. Mirrors Java's `protected` / `package-private` API; we keep
/// everything `pub(crate)` because the module boundary is the consumer
/// subtree.
///
/// `inner` is `pub(crate)` so the composing manager and the heartbeat
/// manager can perform short locked operations directly. Long
/// operations live as methods on this struct so they are documented
/// against the Java source.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager")]
pub(crate) struct AbstractMembershipManager {
    pub(crate) inner: Arc<Mutex<MembershipInner>>,
    pub(crate) subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Java: `private final Metadata metadata` — the base type, so the
    /// consumer passes its `ConsumerMetadata`'s inner `Metadata` and the share
    /// consumer its `ShareConsumerMetadata`'s.
    pub(crate) metadata: Arc<Metadata>,
    pub(crate) background_event_handler: Arc<BackgroundEventHandler>,
    /// Java: `CloseOptions.GroupMembershipOperation leaveGroupOperation`, set
    /// by `leaveGroupOnClose(...)`. Read by the subclass's `leaveGroupEpoch()`
    /// and `isLeavingGroup()`.
    leave_group_operation: Mutex<GroupMembershipOperation>,
    /// Phase 41b — non-blocking reconcile callback state.
    ///
    /// Java's `maybeReconcile` fires the §31 rebalance-listener callback
    /// and returns; `revokeAndAssign(...).whenComplete(...)` resumes the
    /// post-callback steps when the callback future completes, while the
    /// `ConsumerNetworkThread` keeps spinning. Rust collapses Java's
    /// `whenComplete` chain into an explicit cross-iteration state: when a
    /// reconcile callback is in flight, the manager stores the ack
    /// [`tokio::sync::oneshot::Receiver`] plus the data needed to resume
    /// here, and the bg loop's `reconcile` entry `try_recv`s it each
    /// iteration (alloc-free, non-blocking) — so the loop is NOT frozen on
    /// the ack. The membership state stays `RECONCILING`
    /// (`reconciliation_in_progress = true`) until the callback completes.
    ///
    /// `None` in steady state (no rebalance). Guarded by
    /// [`Self::pending_reconcile_flag`] so the steady-state `reconcile`
    /// entry never locks this mutex (Perf Contract item 1).
    pub(crate) pending_reconcile: Mutex<Option<PendingReconcile>>,
    /// Lock-free fast-path mirror of `pending_reconcile.is_some()`. The
    /// steady-state `reconcile` entry does a single `Relaxed` load and, when
    /// `false` (no rebalance in flight), skips the `pending_reconcile` lock
    /// entirely — so the only added per-iteration cost in steady state is
    /// this one atomic load (Perf Contract item 1: no new lock, no alloc, no
    /// Arc clone). Set/cleared in lockstep with `pending_reconcile` under
    /// its mutex.
    pending_reconcile_flag: std::sync::atomic::AtomicBool,
    /// Phase 41 (Issue 2) — non-blocking release-path callback state.
    ///
    /// `transition_to_fenced` / `transition_to_fatal` / `transition_to_stale`
    /// each release the assignment via an `onPartitionsLost` callback. Java
    /// fires that callback and resumes the post-callback steps
    /// (`clearAssignment` + the type-specific tail) from
    /// `callbackResult.whenComplete(...)`, while the `ConsumerNetworkThread`
    /// keeps spinning. Before Phase 41 the Rust translation awaited the ack
    /// inline inside these methods, which — because they run inline in
    /// `run_once` Phase 2.4 — froze the bg loop for the whole callback (any
    /// reentrant handle op submitted from `on_partitions_lost` would deadlock
    /// until its API timeout, see COMMENTS.41 Issue 2).
    ///
    /// The release transitions now enqueue the §31 callback and store the ack
    /// [`oneshot::Receiver`] plus the resume-tail discriminator here, returning
    /// so the loop keeps spinning. The bg loop's Phase 2.4 drives this each
    /// iteration via [`Self::drive_pending_release`] (alloc-free `try_recv`),
    /// resuming the type-specific tail only when the app side has run the
    /// `onPartitionsLost` listener and sent the ack.
    ///
    /// `None` in steady state. Guarded by [`Self::pending_release_flag`] so the
    /// steady-state Phase 2.4 never locks this mutex (Perf Contract item 1).
    pending_release: Mutex<Option<PendingRelease>>,
    /// Lock-free fast-path mirror of `pending_release.is_some()`, analogous to
    /// [`Self::pending_reconcile_flag`]. The steady-state Phase 2.4 release
    /// drive does a single atomic load and skips the `pending_release` mutex
    /// when no release callback is in flight.
    pending_release_flag: std::sync::atomic::AtomicBool,
    /// The ack of the release callback an `unsubscribe()` leave enqueued
    /// ([`MembershipManagerHooks::signal_member_leaving_group`]), while the member waits in
    /// `PREPARE_LEAVING` for it. Java chains `clearAssignmentAndLeaveGroup()`
    /// on that callback's future with `whenComplete`, which runs on the
    /// network thread when the callback-completed event is processed — i.e.
    /// serialized with the heartbeat manager's poll. The app side returns
    /// this ack inside that same event, and the processor drives this slot
    /// when it reaches it (`ConsumerMembershipManager::consumer_rebalance_listener_callback_completed`),
    /// before any request manager polls, so `PREPARE_LEAVING -> LEAVING`
    /// keeps the event's queue position and can never land in the middle of
    /// a heartbeat build.
    pending_leave_callback: Mutex<Option<SignalAck>>,
    /// Lock-free mirror of `pending_leave_callback.is_some()`, so the
    /// steady-state bg loop pays one atomic load.
    pending_leave_callback_flag: std::sync::atomic::AtomicBool,
}

impl AbstractMembershipManager {
    /// Constructor. Java: `AbstractMembershipManager(String, SubscriptionState,
    /// Metadata, Logger, Time, RebalanceMetricsManager, boolean)`.
    ///
    /// Metrics dropped. Time / logger via the `log` crate.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#AbstractMembershipManager")]
    pub(crate) fn new(
        group_id: impl Into<String>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<Metadata>,
        background_event_handler: Arc<BackgroundEventHandler>,
        auto_commit_enabled: bool,
        metrics_manager: Option<Arc<dyn RebalanceMetricsManager>>,
        time: Arc<dyn Time>,
    ) -> Self {
        // Java: `Uuid.randomUuid().toString()`. We use the same Uuid
        // helper (base64 URL encoding) so wire-level traces match Java.
        let member_id = Uuid::random_uuid().to_string();
        let inner = MembershipInner {
            group_id: group_id.into(),
            member_id,
            member_epoch: 0,
            state: MemberState::Unsubscribed,
            current_assignment: LocalAssignment::none(),
            current_target_assignment: LocalAssignment::none(),
            assigned_topic_names_cache: HashMap::new(),
            reconciliation_in_progress: false,
            rejoined_while_reconciliation_in_progress: false,
            is_poll_timer_expired: false,
            stale_assignment_release_pending: false,
            stale_rejoin_requested: false,
            leave_group_in_progress: None,
            subscription_updated: false,
            auto_commit_enabled,
            state_updates_listeners: Vec::new(),
            metrics_manager,
            time,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            subscriptions,
            metadata,
            background_event_handler,
            leave_group_operation: Mutex::new(GroupMembershipOperation::Default),
            pending_reconcile: Mutex::new(None),
            pending_reconcile_flag: std::sync::atomic::AtomicBool::new(false),
            pending_release: Mutex::new(None),
            pending_release_flag: std::sync::atomic::AtomicBool::new(false),
            pending_leave_callback: Mutex::new(None),
            pending_leave_callback_flag: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Java: `groupId()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#groupId")]
    pub(crate) fn group_id(&self) -> String {
        let g = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.group_id.clone()
    }

    /// Java: `memberId()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#memberId")]
    pub(crate) fn member_id(&self) -> String {
        let g = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.member_id.clone()
    }

    /// Java: `memberEpoch()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#memberEpoch")]
    pub(crate) fn member_epoch(&self) -> i32 {
        let g = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.member_epoch
    }

    /// Java: `state()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#state")]
    pub(crate) fn state(&self) -> MemberState {
        let g = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.state
    }

    /// Java: `currentAssignment()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#currentAssignment")]
    pub(crate) fn current_assignment(&self) -> LocalAssignment {
        let g = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.current_assignment.clone()
    }

    /// Java: `leaveGroupOperation()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#leaveGroupOperation")]
    pub(crate) fn leave_group_operation(&self) -> GroupMembershipOperation {
        let g = match self.leave_group_operation.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *g
    }

    /// Set the leave-group operation. Java assigns the field in
    /// `leaveGroupOnClose(...)`.
    pub(crate) fn set_leave_group_operation(&self, op: GroupMembershipOperation) {
        let mut g = match self.leave_group_operation.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *g = op;
    }

    /// Java: `registerStateListener(MemberStateListener listener)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#registerStateListener")]
    pub(crate) fn register_state_listener(&self, listener: Arc<dyn MemberStateListener>) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.state_updates_listeners.push(listener);
    }

    /// Update member info and transition member state based on a successful
    /// heartbeat response. The common response handling lives here
    /// (KAFKA-20681, 6a6b536fbc); group type specifics are provided by the
    /// [`MembershipManagerHooks::error_code`],
    /// [`MembershipManagerHooks::member_epoch_with_response`] and
    /// [`MembershipManagerHooks::extract_assignment`] hooks of `H`.
    ///
    /// Java: `public final void onHeartbeatSuccess(R response)`
    /// (`AbstractMembershipManager#onHeartbeatSuccess`).
    ///
    /// Returns `Err(LocalIllegalArgument)` for an unexpected error in the
    /// response body — Java throws `IllegalArgumentException`.
    ///
    /// An UNSUBSCRIBED member receiving a response with a negative epoch (the
    /// response to its leave heartbeat), and a member receiving an invalid
    /// negative epoch, complete the in-progress leave via Java's
    /// `maybeCompleteLeaveInProgress()` (from `AbstractMembershipManager#onHeartbeatSuccess`),
    /// which is what unblocks the `unsubscribe()` / `close()` caller waiting
    /// on the leave.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onHeartbeatSuccess")]
    pub(crate) fn on_heartbeat_success<H: MembershipManagerHooks>(&self, response: &H::Response) -> Result<(), Error> {
        Self::return_if_unexpected_error::<H>(response)?;

        let response_member_epoch = H::member_epoch_with_response(response);
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let state = guard.state;
        if state == MemberState::Leaving {
            log::debug!(
                "Ignoring heartbeat response received from broker. Member {} with epoch {} is \
                 already leaving the group.",
                guard.member_id,
                guard.member_epoch
            );
            return Ok(());
        }

        if state == MemberState::Unsubscribed
            && response_member_epoch < 0
            && let Some(leave) = guard.take_leave_in_progress()
        {
            log::debug!(
                "Member {} with epoch {} received a successful response to the heartbeat \
                 to leave the group and completed the leave operation. ",
                guard.member_id,
                guard.member_epoch
            );
            drop(guard);
            Self::complete_leave(Some(leave));
            return Ok(());
        }
        if guard.is_not_in_group() {
            log::debug!(
                "Ignoring heartbeat response received from broker. Member {} is in {} state \
                 so it's not a member of the group. ",
                guard.member_id,
                state
            );
            return Ok(());
        }
        if response_member_epoch < 0 {
            log::debug!(
                "Ignoring heartbeat response received from broker. Member {} with epoch {} \
                 is in {} state and the member epoch is invalid: {}. ",
                guard.member_id,
                guard.member_epoch,
                state,
                response_member_epoch
            );
            let leave = guard.take_leave_in_progress();
            drop(guard);
            Self::complete_leave(leave);
            return Ok(());
        }

        guard.update_member_epoch(response_member_epoch);

        if let Some(assignment) = H::extract_assignment(response) {
            if !state.can_handle_new_assignment() {
                // New assignment received but member is in a state where it cannot take new
                // assignments (ex. preparing to leave the group)
                log::debug!(
                    "Ignoring new assignment {:?} received from server because member is in {} state.",
                    assignment,
                    state
                );
                return Ok(());
            }
            // Release the guard: `process_assignment_received` re-acquires it.
            drop(guard);
            self.process_assignment_received(assignment)?;
        }
        Ok(())
    }

    /// Java: `private void throwIfUnexpectedError(R response)`
    /// (`AbstractMembershipManager#throwIfUnexpectedError`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#throwIfUnexpectedError")]
    fn return_if_unexpected_error<H: MembershipManagerHooks>(response: &H::Response) -> Result<(), Error> {
        let error_code = H::error_code(response);
        if error_code != Errors::None.code() {
            return Err(Error::local_illegal_argument(format!(
                "Unexpected error in Heartbeat response. Expected no error, but received: {}",
                Errors::for_code(error_code)
            )));
        }
        Ok(())
    }

    /// Process a newly received target assignment. If the assignment
    /// differs from the current, transitions to `RECONCILING`; if it's
    /// the same as current, transitions to `STABLE` if we were
    /// `RECONCILING` or `JOINING`.
    ///
    /// Java: `processAssignmentReceived(Map<Uuid, SortedSet<Integer>>)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#processAssignmentReceived")]
    pub(crate) fn process_assignment_received(&self, assignment: HashMap<Uuid, Vec<i32>>) -> Result<(), Error> {
        // Compute new target & whether we transition to RECONCILING.
        let (assigned_topic_ids, must_reconcile, state_after) = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if let Some(updated) = guard.current_target_assignment.update_with(assignment) {
                log::debug!(
                    "Member {} updated its target assignment to local_epoch={}",
                    guard.member_id,
                    updated.local_epoch
                );
                guard.current_target_assignment = updated;
            }
            let must_reconcile = !guard.target_assignment_reconciled();
            let state_after = guard.state;
            (
                guard
                    .current_target_assignment
                    .partitions
                    .keys()
                    .copied()
                    .collect::<HashSet<Uuid>>(),
                must_reconcile,
                state_after,
            )
        };

        // Register newly assigned topic IDs on the subscription state.
        {
            let mut subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.set_assigned_topic_ids(assigned_topic_ids);
        }

        // Transition based on reconcile state.
        if must_reconcile {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.transition_to(MemberState::Reconciling)?;
        } else if matches!(state_after, MemberState::Reconciling | MemberState::Joining) {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.transition_to(MemberState::Stable)?;
        }
        Ok(())
    }

    /// Resolve topic names for the current target assignment from the
    /// metadata cache. Builds a [`HashMap`] of topic ID → (topic name,
    /// partitions) and triggers a metadata update for any unresolved
    /// topic IDs.
    ///
    /// Java: `findResolvableAssignmentAndTriggerMetadataUpdate()`.
    /// Returns a `Vec` of `(topic_id, topic_name, partitions)` triples
    /// for the partitions whose topic name could be resolved either
    /// from global metadata or the local cache.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#findResolvableAssignmentAndTriggerMetadataUpdate"
    )]
    pub(crate) fn find_resolvable_assignment_and_trigger_metadata_update(&self) -> Vec<(Uuid, String, Vec<i32>)> {
        let target_partitions = {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.current_target_assignment.partitions.clone()
        };

        let topic_names = self.metadata.topic_names();
        let mut resolved = Vec::with_capacity(target_partitions.len());
        let mut unresolved_count = 0usize;
        let mut to_cache: Vec<(Uuid, String)> = Vec::new();

        for (topic_id, partitions) in &target_partitions {
            // 1. Look in the global metadata cache.
            if let Some(name) = topic_names.get(topic_id) {
                to_cache.push((*topic_id, name.clone()));
                resolved.push((*topic_id, name.clone(), partitions.clone()));
                continue;
            }
            // 2. Fall back to the local cache.
            let cached = {
                let guard = match self.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.assigned_topic_names_cache.get(topic_id).cloned()
            };
            if let Some(name) = cached {
                resolved.push((*topic_id, name, partitions.clone()));
            } else {
                unresolved_count += 1;
            }
        }

        // Update the local cache with metadata-resolved names.
        if !to_cache.is_empty() {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            for (id, name) in to_cache {
                guard.assigned_topic_names_cache.insert(id, name);
            }
        }

        if unresolved_count > 0 {
            log::debug!("Topic IDs in target assignment were not found in metadata; requesting an update.");
            self.metadata.request_update(true);
        }
        resolved
    }

    /// Mark reconciliation in progress. Java: `markReconciliationInProgress()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#markReconciliationInProgress")]
    pub(crate) fn mark_reconciliation_in_progress(&self) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.reconciliation_in_progress = true;
        guard.rejoined_while_reconciliation_in_progress = false;
    }

    /// Mark reconciliation completed. Java: `markReconciliationCompleted()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#markReconciliationCompleted")]
    pub(crate) fn mark_reconciliation_completed(&self) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.reconciliation_in_progress = false;
        guard.rejoined_while_reconciliation_in_progress = false;
    }

    /// Java: `maybeAbortReconciliation()`. Returns `true` if the
    /// reconciliation should be aborted.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeAbortReconciliation")]
    pub(crate) fn maybe_abort_reconciliation(&self) -> bool {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let should_abort = guard.state != MemberState::Reconciling || guard.rejoined_while_reconciliation_in_progress;
        if should_abort {
            let reason = if guard.rejoined_while_reconciliation_in_progress {
                "the member has re-joined the group".to_string()
            } else {
                format!(
                    "the member already transitioned out of the reconciling state into {}",
                    guard.state
                )
            };
            log::info!("Interrupting reconciliation that is not relevant anymore because {}", reason);
            guard.reconciliation_in_progress = false;
            guard.rejoined_while_reconciliation_in_progress = false;
        }
        should_abort
    }

    /// Java: `clearAssignment()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#clearAssignment")]
    pub(crate) fn clear_assignment(&self) {
        // Drop the SubscriptionState guard before mutating inner state.
        let has_auto_assigned = {
            let mut subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let has_auto = subs.has_auto_assigned_partitions();
            if has_auto {
                // Java: subscriptions.assignFromSubscribed(Collections.emptySet())
                // The Rust API takes &[TopicPartition].
                let _ = subs.assign_from_subscribed(&[]);
            }
            has_auto
        };
        if has_auto_assigned {
            let empty = HashSet::new();
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.notify_assignment_change(&empty);
        }
        {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.current_assignment = LocalAssignment::none();
            guard.clear_pending_assignments_and_local_names_cache();
        }
    }

    /// Java: `onSubscriptionUpdated()`. Atomically sets the
    /// `subscriptionUpdated` flag.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onSubscriptionUpdated")]
    pub(crate) fn on_subscription_updated(&self) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if !guard.subscription_updated {
            guard.subscription_updated = true;
        }
    }

    /// Java: `onConsumerPoll()`. If a subscription update is pending
    /// and we're UNSUBSCRIBED, transition to JOINING.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onConsumerPoll")]
    pub(crate) fn on_consumer_poll(&self, join_group_epoch: i32) -> Result<(), Error> {
        let should_join = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            // Java: `subscriptionUpdated.compareAndSet(true, false) && state == UNSUBSCRIBED`.
            // The CAS clears the flag whenever it was set, *regardless of
            // state* (short-circuit `&&` evaluates the CAS first); only
            // then is the state checked to decide whether to join. The
            // earlier Rust form gated the clear on `state == Unsubscribed`,
            // which leaked the flag when polling while in-group (observable
            // via `subscription_updated()` staying true) — diverging from
            // Java (`org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onConsumerPoll`).
            let was_updated = guard.subscription_updated;
            guard.subscription_updated = false;
            was_updated && guard.state == MemberState::Unsubscribed
        };
        if should_join {
            self.transition_to_joining(join_group_epoch)?;
        }
        Ok(())
    }

    /// Java: `transitionToJoining()`. The Consumer subclass supplies
    /// the join epoch via `joinGroupEpoch()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#transitionToJoining")]
    pub(crate) fn transition_to_joining(&self, join_group_epoch: i32) -> Result<(), Error> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if guard.state == MemberState::Fatal {
            log::warn!(
                "No action taken to join the group with the updated subscription because the member is in FATAL state"
            );
            return Ok(());
        }
        if guard.reconciliation_in_progress {
            guard.rejoined_while_reconciliation_in_progress = true;
        }
        guard.update_member_epoch(join_group_epoch);
        guard.transition_to(MemberState::Joining)?;
        log::debug!("Member {} will join the group on the next call to poll.", guard.member_id);
        guard.clear_pending_assignments_and_local_names_cache();
        Ok(())
    }

    /// Java: `transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer)`.
    /// The Consumer subclass supplies the leave epoch via
    /// `leaveGroupEpoch()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#transitionToSendingLeaveGroup"
    )]
    pub(crate) fn transition_to_sending_leave_group(
        &self,
        leave_group_epoch: i32,
        due_to_expired_poll_timer: bool,
    ) -> Result<(), Error> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if guard.state == MemberState::Fatal {
            log::warn!(
                "Member {} with epoch {} won't send leave group request because it is in FATAL state",
                guard.member_id,
                guard.member_epoch
            );
            return Ok(());
        }
        if guard.state == MemberState::Unsubscribed {
            log::warn!(
                "Member {} won't send leave group request because it is already out of the group.",
                guard.member_id
            );
            return Ok(());
        }
        if due_to_expired_poll_timer {
            guard.is_poll_timer_expired = true;
            guard.transition_to(MemberState::PrepareLeaving)?;
        }
        guard.update_member_epoch(leave_group_epoch);
        guard.current_assignment = LocalAssignment::none();
        guard.transition_to(MemberState::Leaving)?;
        Ok(())
    }

    /// Java: `onHeartbeatRequestSkipped()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onHeartbeatRequestSkipped")]
    pub(crate) fn on_heartbeat_request_skipped(&self) -> Result<(), Error> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if guard.state == MemberState::Leaving {
            log::warn!(
                "Heartbeat to leave group cannot be sent (most probably due to coordinator not known/available). Member {} with epoch {} will transition to {}.",
                guard.member_id,
                guard.member_epoch,
                MemberState::Unsubscribed
            );
            guard.transition_to(MemberState::Unsubscribed)?;
            let leave = guard.take_leave_in_progress();
            drop(guard);
            Self::complete_leave(leave);
        }
        Ok(())
    }

    /// Java: `onHeartbeatRequestGenerated()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onHeartbeatRequestGenerated")]
    pub(crate) fn on_heartbeat_request_generated(&self) -> Result<(), Error> {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let state = guard.state;
        match state {
            MemberState::Acknowledging => {
                if guard.target_assignment_reconciled() {
                    guard.transition_to(MemberState::Stable)?;
                } else {
                    guard.transition_to(MemberState::Reconciling)?;
                }
            },
            MemberState::Leaving => {
                if guard.is_poll_timer_expired {
                    // Java transitions to STALE here via
                    // `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#transitionToStale`,
                    // which also schedules the onPartitionsLost assignment
                    // release. The
                    // release is async (it awaits the §31 listener) and cannot
                    // run inside this sync method; the concrete
                    // `ConsumerMembershipManager::transition_to_stale` performs
                    // it (driven from the bg task / directly in tests). We mark
                    // the release pending here so that a `maybe_rejoin_stale_member`
                    // arriving before the release completes defers the
                    // STALE → JOINING transition (mirroring Java chaining
                    // `transitionToJoining` onto the in-flight release future).
                    guard.transition_to(MemberState::Stale)?;
                    guard.stale_assignment_release_pending = true;
                    guard.stale_rejoin_requested = false;
                } else {
                    guard.transition_to(MemberState::Unsubscribed)?;
                }
            },
            _ => {},
        }
        Ok(())
    }

    /// Java: `maybeRejoinStaleMember()`
    /// (`AbstractMembershipManager#maybeRejoinStaleMember`). Resets the
    /// `isPollTimerExpired` flag; if the member is currently STALE,
    /// transitions it to JOINING so the next heartbeat re-joins the
    /// group with `memberEpoch=0`.
    ///
    /// **Translation note**: Java's `transitionToJoining()` happens via
    /// `staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining())`
    /// — i.e. after the onPartitionsLost callback that ran during the
    /// fence flow completes. In Rust the listener is invoked
    /// synchronously on the caller's task via the §31 handshake
    /// (`process_background_events`), so by the time the next
    /// `consumer.poll()` arms `AsyncPoll` and the AEP arm calls into
    /// here, the onPartitionsLost callback has already returned. We
    /// therefore transition inline without a whenComplete dance.
    ///
    /// `join_group_epoch` is supplied by the caller — Phase 10's AEP
    /// `AsyncPoll` arm reads it via
    /// [`MembershipManagerHooks::join_group_epoch`]
    /// just before invoking this method.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeRejoinStaleMember")]
    pub(crate) fn maybe_rejoin_stale_member(&self, join_group_epoch: i32) {
        let should_transition_to_joining = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.is_poll_timer_expired = false;
            if guard.state != MemberState::Stale {
                false
            } else if guard.stale_assignment_release_pending {
                // The onPartitionsLost release triggered by
                // `transition_to_stale` has not completed yet. Java chains
                // `transitionToJoining` onto the in-flight
                // `staleMemberAssignmentRelease` future (in
                // `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeRejoinStaleMember`);
                // we record the
                // intent and let the release-completion path perform the
                // transition once the callback returns. The member stays
                // STALE in the meantime (it must not clear its assignment
                // to rejoin until the callback completes).
                guard.stale_rejoin_requested = true;
                false
            } else {
                true
            }
        };
        if should_transition_to_joining {
            // Re-acquire the lock for the transition — `transition_to_joining`
            // takes its own guard.
            if let Err(e) = self.transition_to_joining(join_group_epoch) {
                log::warn!("maybe_rejoin_stale_member: transition_to_joining failed: {}", e);
            }
        }
    }

    /// Java: `transitionToFatal()`. The flow mirrors Java exactly:
    ///
    /// 1. Transition to `FATAL` on the abstract membership state (this
    ///    runs `notifyEpochChange(Optional.empty())` and logs).
    /// 2. If the previous state was already out of the group
    ///    (`UNSUBSCRIBED`, `PREPARE_LEAVING`, `LEAVING`), skip the
    ///    `onPartitionsLost` callback — Java's `transitionToFatal`
    ///    early-returns in that case because there's nothing to
    ///    release.
    /// 3. Otherwise enqueue `onPartitionsLost` via the §31 handshake to
    ///    release the assignment, **non-blockingly** (Phase 41, Issue 2):
    ///    store a [`PendingRelease::Fatal`] and return so the bg loop keeps
    ///    spinning. The release tail (step 4) runs in
    ///    [`Self::drive_pending_release`] once the listener acks.
    /// 4. Clear the assignment (Java: `clearAssignment()`).
    ///
    /// Phase 41, Issue 1: a release transition that interleaves with an
    /// in-flight reconcile callback **abandons** the stale `pending_reconcile`
    /// (mirroring Java dropping the in-flight reconcile future when the
    /// member leaves `RECONCILING`), so a fresh post-rejoin reconcile can
    /// start immediately instead of being gated on the stale ack draining.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#transitionToFatal")]
    pub(crate) fn transition_to_fatal<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        current_time_ms: i64,
    ) -> Result<(), Error> {
        // Issue 1: abandon any in-flight reconcile — the member is leaving
        // RECONCILING, so the stored reconcile continuation is stale.
        self.clear_pending_reconcile();

        let previous_state = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let prev = guard.state;
            guard.transition_to(MemberState::Fatal)?;
            log::error!(
                "Member {} with epoch {} transitioned to fatal state",
                guard.member_id,
                guard.member_epoch
            );
            guard.notify_epoch_change(None);
            prev
        };

        // If we were UNSUBSCRIBED / LEAVING / PREPARE_LEAVING, Java's
        // semantics are: no onPartitionsLost callback, and the ongoing leave
        // (if any) is completed — Java's `maybeCompleteLeaveInProgress()` in
        // both the "already left" and the "was leaving" branches of
        // `AbstractMembershipManager#transitionToFatal`.
        if matches!(
            previous_state,
            MemberState::Unsubscribed | MemberState::Leaving | MemberState::PrepareLeaving
        ) {
            if previous_state != MemberState::Unsubscribed {
                log::info!(
                    "Member {} with epoch {} was leaving the group with state {} when it got a fatal error \
                     from the broker. It will discard the ongoing leave and remain in fatal state.",
                    self.member_id(),
                    self.member_epoch(),
                    previous_state
                );
            }
            self.maybe_complete_leave_in_progress();
            return Ok(());
        }

        // Enqueue onPartitionsLost via the §31 handshake to release
        // assignment, non-blockingly. When no listener is registered or no
        // partitions are owned, the callback is a completed no-op and we run
        // the release tail inline.
        let partitions = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        let ack = hooks.signal_partitions_lost(partitions, current_time_ms)?;
        match ack {
            Some(ack_rx) => {
                self.store_pending_release(PendingRelease::Fatal { ack_rx });
                Ok(())
            },
            None => {
                self.continue_after_fatal_release();
                Ok(())
            },
        }
    }

    /// Release tail of `transition_to_fatal` (Java's `whenComplete`):
    /// `clearAssignment()`. Synchronous.
    fn continue_after_fatal_release(&self) {
        self.clear_assignment();
    }

    /// Java: `transitionToFenced()`. Same shape as `transition_to_fatal`
    /// but transitions to FENCED and then JOINING after the listener
    /// completes (so the member rejoins).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#transitionToFenced")]
    pub(crate) fn transition_to_fenced<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        current_time_ms: i64,
    ) -> Result<(), Error> {
        // Issue 1: abandon any in-flight reconcile — the member is leaving
        // RECONCILING, so the stored reconcile continuation is stale.
        self.clear_pending_reconcile();

        let pre_state = {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.state
        };

        // Handle Java's "already leaving / unsubscribed" short-circuit.
        match pre_state {
            MemberState::PrepareLeaving => {
                // Java (`AbstractMembershipManager#transitionToFenced`): briefly go
                // through LEAVING (clear epoch, notify listeners), then
                // UNSUBSCRIBED, and complete the ongoing leave operation. An
                // `on_partitions_revoked` callback still in flight for the
                // leave finds the member UNSUBSCRIBED when it completes and
                // sends no leave heartbeat.
                self.transition_to_sending_leave_group(hooks.leave_group_epoch(), false)?;
                {
                    let mut guard = match self.inner.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    guard.transition_to(MemberState::Unsubscribed)?;
                }
                self.maybe_complete_leave_in_progress();
                return Ok(());
            },
            MemberState::Leaving => {
                // Java (`AbstractMembershipManager#transitionToFenced`): no leave
                // request is needed once fenced; complete the ongoing leave
                // operation.
                {
                    let mut guard = match self.inner.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    guard.transition_to(MemberState::Unsubscribed)?;
                }
                self.maybe_complete_leave_in_progress();
                return Ok(());
            },
            MemberState::Unsubscribed => {
                log::debug!("Member got fenced but it already left the group, so it won't attempt to rejoin.");
                return Ok(());
            },
            _ => {},
        }

        // Normal fence flow.
        {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.transition_to(MemberState::Fenced)?;
            // resetEpoch -> updateMemberEpoch(joinGroupEpoch()).
            guard.update_member_epoch(hooks.join_group_epoch());
        }

        // Enqueue onPartitionsLost non-blockingly (Phase 41, Issue 2).
        let partitions = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        let ack = hooks.signal_partitions_lost(partitions, current_time_ms)?;
        match ack {
            Some(ack_rx) => {
                self.store_pending_release(PendingRelease::Fenced { ack_rx });
                Ok(())
            },
            None => self.continue_after_fenced_release(hooks),
        }
    }

    /// Release tail of `transition_to_fenced` (Java's `whenComplete`):
    /// `clearAssignment()` then, if still `FENCED`, `transitionToJoining()`.
    fn continue_after_fenced_release<H: MembershipManagerHooks>(&self, hooks: &H) -> Result<(), Error> {
        self.clear_assignment();

        // Now transition to JOINING if still FENCED.
        let still_fenced = {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.state == MemberState::Fenced
        };
        if still_fenced {
            self.transition_to_joining(hooks.join_group_epoch())?;
        }
        Ok(())
    }

    /// Release the assignment held by a member that has just transitioned
    /// to STALE because of an expired poll timer, then (if a timer reset
    /// already requested it) rejoin.
    ///
    /// Java: the async tail of `AbstractMembershipManager#transitionToStale`:
    ///
    /// ```java
    /// CompletableFuture<Void> callbackResult = signalPartitionsLost(subscriptions.assignedPartitions());
    /// staleMemberAssignmentRelease = callbackResult.whenComplete((result, error) -> {
    ///     ...
    ///     clearAssignment();
    /// });
    /// ```
    ///
    /// The STATE transition to STALE itself happens synchronously inside
    /// `AbstractMembershipManager::on_heartbeat_request_generated` (mirroring
    /// Java's `transitionTo(STALE)` at the top of `transitionToStale`); this
    /// method performs only the release half, which is async because it
    /// awaits the §31 `onPartitionsLost` listener.
    ///
    /// The release-pending flag (set when STALE was entered) is cleared at
    /// the end; if `maybe_rejoin_stale_member` was called while the release
    /// was in flight, the member is transitioned to JOINING now — exactly
    /// Java's `staleMemberAssignmentRelease.whenComplete(__ -> transitionToJoining())`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#transitionToStale")]
    pub(crate) fn transition_to_stale<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        current_time_ms: i64,
    ) -> Result<(), Error> {
        // Issue 1: abandon any in-flight reconcile — the member is leaving
        // RECONCILING, so the stored reconcile continuation is stale.
        self.clear_pending_reconcile();

        // Release assignment via onPartitionsLost (Java's
        // `signalPartitionsLost(subscriptions.assignedPartitions())`),
        // enqueued non-blockingly (Phase 41, Issue 2).
        let partitions = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        let ack = hooks.signal_partitions_lost(partitions, current_time_ms)?;
        match ack {
            Some(ack_rx) => {
                self.store_pending_release(PendingRelease::Stale { ack_rx });
                Ok(())
            },
            None => self.continue_after_stale_release(hooks),
        }
    }

    /// Release tail of `transition_to_stale` (Java's
    /// `staleMemberAssignmentRelease.whenComplete(...)`): `clearAssignment()`,
    /// clear the release-pending flag, and (if a timer reset requested a
    /// rejoin while the release was in flight) `transitionToJoining()`.
    fn continue_after_stale_release<H: MembershipManagerHooks>(&self, hooks: &H) -> Result<(), Error> {
        self.clear_assignment();

        let rejoin = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.stale_assignment_release_pending = false;
            let rejoin = guard.stale_rejoin_requested && guard.state == MemberState::Stale;
            guard.stale_rejoin_requested = false;
            rejoin
        };
        if rejoin {
            self.transition_to_joining(hooks.join_group_epoch())?;
        }
        Ok(())
    }

    /// `true` when a reconcile rebalance callback is awaiting its ack.
    ///
    /// Lock-free: reads the [`Self::pending_reconcile_flag`] atomic only.
    /// This is the steady-state `reconcile` entry check — no mutex lock when
    /// no rebalance is in flight (Perf Contract item 1).
    pub(crate) fn has_pending_reconcile(&self) -> bool {
        self.pending_reconcile_flag.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Stores the cross-iteration pending-callback state (Phase 41b) and
    /// sets the lock-free flag in lockstep.
    fn store_pending(&self, pending: PendingReconcile) {
        let mut guard = match self.pending_reconcile.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *guard = Some(pending);
        self.pending_reconcile_flag.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Phase 41, Issue 1: abandon any in-flight reconcile callback state.
    ///
    /// Called by the release transitions (`transition_to_fenced/fatal/stale`)
    /// when the member leaves `RECONCILING`. Java drops the in-flight
    /// `revokeAndAssign(...)` future implicitly: the next `whenComplete`
    /// callback that fires runs `maybeAbortReconciliation()`, which aborts
    /// because `state != RECONCILING`. Rust collapses Java's `whenComplete`
    /// chain into the explicit stored [`PendingReconcile`], so we must drop it
    /// here — otherwise the stale continuation would (a) keep the `reconcile`
    /// entry short-circuiting to `drive_pending_reconcile` until the stale ack
    /// drains, gating a fresh post-rejoin reconcile, and (b) leave a load-bearing
    /// dependency on the abort-check + un-cleared `reconciliation_in_progress`
    /// for correctness (COMMENTS.41 Issue 1).
    ///
    /// Dropping the [`oneshot::Receiver`] also signals the app side (the ack
    /// `Sender::send` becomes a no-op), matching "the reconcile future is
    /// abandoned". `reconciliation_in_progress` is left to the subsequent
    /// state transition / `maybe_abort_reconciliation`, exactly as Java does
    /// not clear it inside `transitionTo*`.
    fn clear_pending_reconcile(&self) {
        // Always take the lock (no flag fast-path): the release transitions
        // that call this are rare, and a lock-free flag read would race the
        // `drive_pending_reconcile` take/store-back window (where the flag is
        // transiently `false` while the value is owned by the driver) — in
        // the bg loop reconcile and the transitions run on the same task and
        // never overlap, but the component tests drive them from separate
        // tasks, so the unconditional lock keeps the clear deterministic.
        let had_pending = {
            let mut guard = match self.pending_reconcile.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let had = guard.is_some();
            *guard = None;
            self.pending_reconcile_flag.store(false, std::sync::atomic::Ordering::Release);
            had
        };
        // The abandoned reconcile's continuation will never run its
        // `mark_reconciliation_completed` (Java's `revokeAndAssign`
        // `whenComplete` → `maybeAbortReconciliation` clearing
        // `reconciliationInProgress`). Clear it eagerly here so a fresh
        // post-rejoin reconcile is not blocked by `reconciliationInProgress`
        // still being `true`. Only when we actually dropped a pending state,
        // to avoid disturbing a reconcile that is mid-flight but has not yet
        // parked (synchronous window inside a single `reconcile` call).
        if had_pending {
            self.mark_reconciliation_completed();
        }
    }

    /// `true` when a release-path `onPartitionsLost` callback is awaiting its
    /// ack. Lock-free: reads [`Self::pending_release_flag`] only (steady-state
    /// Phase 2.4 entry check — Perf Contract item 1).
    pub(crate) fn has_pending_release(&self) -> bool {
        self.pending_release_flag.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Stores the cross-iteration pending release-callback state and sets the
    /// lock-free flag in lockstep.
    fn store_pending_release(&self, pending: PendingRelease) {
        let mut guard = match self.pending_release.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *guard = Some(pending);
        self.pending_release_flag.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Drives a pending release-path `onPartitionsLost` callback
    /// non-blockingly: `try_recv`s the stored ack receiver and, when it has
    /// resolved, runs the type-specific release tail. While the ack is pending
    /// this is a no-op. Driven from the bg loop's Phase 2.4 once per iteration.
    ///
    /// A listener error is logged and the release tail still runs — Java's
    /// `whenComplete` logs the `onPartitionsLost` error and proceeds with
    /// `clearAssignment()` (and the fence/stale rejoin) regardless.
    pub(crate) async fn drive_pending_release<H: MembershipManagerHooks>(&self, hooks: &H) -> Result<(), Error> {
        // Pop under the lock, then `try_recv` outside it (no guard across the
        // resumed work). If the ack is not ready, put it back.
        let pending = {
            let mut guard = match self.pending_release.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            self.pending_release_flag.store(false, std::sync::atomic::Ordering::Release);
            guard.take()
        };
        let Some(pending) = pending else {
            return Ok(());
        };

        // Destructure by value to own the receiver + a kind discriminator.
        let (mut ack_rx, kind, log_ctx): (oneshot::Receiver<Result<(), Error>>, ReleaseKind, &'static str) =
            match pending {
                PendingRelease::Fenced { ack_rx } => {
                    (ack_rx, ReleaseKind::Fenced, "got fenced. Member will rejoin the group anyways")
                },
                PendingRelease::Fatal { ack_rx } => (ack_rx, ReleaseKind::Fatal, "failed with fatal error"),
                PendingRelease::Stale { ack_rx } => {
                    (ack_rx, ReleaseKind::Stale, "left group due to expired poll timer")
                },
            };

        match ack_rx.try_recv() {
            Ok(result) => {
                if let Err(e) = result {
                    log::error!(
                        "onPartitionsLost callback invocation failed while releasing assignment after member {log_ctx}: {e}"
                    );
                }
                match kind {
                    ReleaseKind::Fenced => self.continue_after_fenced_release(hooks),
                    ReleaseKind::Fatal => {
                        self.continue_after_fatal_release();
                        Ok(())
                    },
                    ReleaseKind::Stale => self.continue_after_stale_release(hooks),
                }
            },
            Err(oneshot::error::TryRecvError::Empty) => {
                // Ack not ready — keep waiting (loop continues).
                let put_back = match kind {
                    ReleaseKind::Fenced => PendingRelease::Fenced { ack_rx },
                    ReleaseKind::Fatal => PendingRelease::Fatal { ack_rx },
                    ReleaseKind::Stale => PendingRelease::Stale { ack_rx },
                };
                self.store_pending_release(put_back);
                Ok(())
            },
            Err(oneshot::error::TryRecvError::Closed) => {
                // App side dropped the receiver before responding. Java logs
                // and proceeds with the release tail anyway; do the same so
                // the member is not stranded holding the released assignment.
                log::error!(
                    "Rebalance listener ack receiver dropped before completion while releasing assignment after member {log_ctx}; proceeding with the release"
                );
                match kind {
                    ReleaseKind::Fenced => self.continue_after_fenced_release(hooks),
                    ReleaseKind::Fatal => {
                        self.continue_after_fatal_release();
                        Ok(())
                    },
                    ReleaseKind::Stale => self.continue_after_stale_release(hooks),
                }
            },
        }
    }

    /// Reconcile the target assignment per §31.
    ///
    /// Synchronous, like Java's `void maybeReconcile(boolean)`: it never
    /// waits. A rebalance-listener callback or the pre-rebalance auto-commit
    /// that is still pending is stored as a [`PendingReconcile`] leg, and a
    /// later call `try_recv`s it and resumes the chain
    /// (`drive_pending_reconcile`), as Java's `whenComplete` continuations
    /// resume on the network thread.
    ///
    /// **Must only be called from the background task.** Java runs
    /// `maybeReconcile` only on the network thread: from
    /// `AbstractMembershipManager.poll` (`canCommit = false`, the bg loop's
    /// Phase 2.5) and from `ApplicationEventProcessor.process(AsyncPollEvent)`
    /// (`canCommit = true`, `process_async_poll`, run during the bg loop's
    /// Phase 1 event drain). That confinement is what keeps the
    /// `reconciliation_in_progress` check (step 1) and
    /// `mark_reconciliation_in_progress()` (step 6) race-free: they are
    /// separate lock scopes, so two concurrent callers could both pass the
    /// check and each start a reconciliation, enqueuing two
    /// `PartitionsAssigned` events (two `onPartitionsAssigned` runs) for one
    /// assignment.
    ///
    /// Pre-condition: state is `RECONCILING`.
    ///
    /// Behavior mirrors Java's `maybeReconcile(boolean canCommit)` +
    /// `revokeAndAssign(...)` chain, with the `CompletableFuture::whenComplete`
    /// continuations translated as explicit resumable steps.
    ///
    /// `can_commit` mirrors Java's parameter: it controls whether
    /// reconciliation can proceed when auto-commit is enabled or there are
    /// partitions to revoke. Auto-commit and partition revocation can only be
    /// triggered on reconciliations initiated within a call to
    /// `consumer.poll()`; if `can_commit` is `false` and either condition
    /// applies, the reconciliation is skipped (KAFKA-20106 / KAFKA-20145
    /// javadoc of `AbstractMembershipManager#maybeReconcile`). Java passes `false` from
    /// `AbstractMembershipManager.poll(now)` (the per-iteration
    /// `entries()` walk) and `true` from
    /// `ApplicationEventProcessor.process(AsyncPollEvent)` (the
    /// poll-time entry point, before any new fetching starts). The poll-time path passes `true` because
    /// any pending offsets can be safely flushed via the commit
    /// manager's auto-commit-before-rebalance path inside this method.
    /// The Rust translation mirrors this through the bg-task call site
    /// (passes `false`) and the `process_async_poll` arm (passes
    /// `true`).
    ///
    /// Java: `AbstractMembershipManager#maybeReconcile`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeReconcile")]
    pub(crate) fn maybe_reconcile<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        current_time_ms: i64,
        can_commit: bool,
    ) -> Result<(), Error> {
        // Phase 41b: if a reconcile rebalance callback is already in
        // flight, drive it non-blockingly instead of starting a new
        // reconciliation. The loop is NOT frozen on the ack: we `try_recv`
        // the stored receiver and resume the post-callback steps only when
        // the app side has run the listener and sent the ack. While the
        // ack is pending the member stays RECONCILING
        // (`reconciliation_in_progress = true`).
        if self.has_pending_reconcile() {
            return self.drive_pending_reconcile(hooks, current_time_ms);
        }

        // 1. State / progress checks.
        {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if guard.state != MemberState::Reconciling {
                return Ok(());
            }
            if guard.target_assignment_reconciled() {
                log::trace!("Ignoring reconciliation attempt. Target assignment is equal to current.");
                return Ok(());
            }
            if guard.reconciliation_in_progress {
                log::trace!("Ignoring reconciliation attempt. Another reconciliation is already in progress.");
                return Ok(());
            }
        }

        // 2. Resolve topic names + drive metadata updates.
        let resolved = self.find_resolvable_assignment_and_trigger_metadata_update();

        // 3. Build the resolved LocalAssignment.
        let mut resolved_partitions: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for (topic_id, _topic_name, partitions) in &resolved {
            resolved_partitions.insert(*topic_id, partitions.clone());
        }
        let target_epoch = {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.current_target_assignment.local_epoch
        };
        let resolved_assignment = LocalAssignment::new(target_epoch, resolved_partitions.clone())?;

        // 4. Short-circuit: if the resolved subset equals the current
        // assignment's partitions, just bump epoch and ACK — once per target
        // epoch. KAFKA-20145 (`org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeReconcile`):
        // when the current assignment already carries the target's local epoch,
        // this partial assignment was acknowledged before, so return without
        // acknowledging it again. Without the check every reconcile attempt
        // (the bg loop's `reconcile(now, false)` runs on every iteration)
        // flipped the member back to ACKNOWLEDGING and sent a redundant
        // heartbeat while a topic id stayed unresolved.
        {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if !guard.current_assignment.is_none()
                && resolved_assignment.partitions == guard.current_assignment.partitions
            {
                if guard.current_assignment.local_epoch == resolved_assignment.local_epoch {
                    return Ok(());
                }
                log::debug!(
                    "There are unresolved partitions, and the resolvable fragment of the target assignment {:?} is \
                     equal to the current assignment. Bumping the local epoch of the assignment and acknowledging \
                     the partially resolved assignment",
                    resolved_assignment.partitions
                );
                guard.current_assignment = resolved_assignment;
                guard.transition_to(MemberState::Acknowledging)?;
                return Ok(());
            }
        }

        // 5. Compute `auto_commit_enabled` for the reconciliation gate
        // below. (AK 4.3.1, KAFKA-20106: the gate itself is applied AFTER
        // computing revoked partitions — see step 5b — because it now also
        // depends on whether the reconciliation would revoke anything.)
        let auto_commit_enabled = {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.auto_commit_enabled
        };

        // 7. Compute added vs revoked partitions.
        let mut assigned_topic_partitions: Vec<TopicPartition> = Vec::new();
        for (_topic_id, topic_name, partitions) in &resolved {
            for p in partitions {
                assigned_topic_partitions.push(TopicPartition::new(topic_name.clone(), *p));
            }
        }
        let assigned_set: HashSet<TopicPartition> = assigned_topic_partitions.iter().cloned().collect();

        let owned_set: HashSet<TopicPartition> = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions()
        };
        let added: HashSet<TopicPartition> = assigned_set.difference(&owned_set).cloned().collect();
        let revoked: HashSet<TopicPartition> = owned_set.difference(&assigned_set).cloned().collect();

        // 5b. AK 4.3.1 (KAFKA-20106) reconciliation gate. Java:
        //   `if (!canCommit && (autoCommitEnabled || !revokedPartitions.isEmpty())) return;`
        //   `markReconciliationInProgress();`
        // (`AbstractMembershipManager.java`, moved to AFTER computing
        // `revokedPartitions`).
        //
        // If `can_commit` is false (called from the background poll,
        // `entries()` walk — NOT from `AsyncPollEvent`), skip reconciliation
        // if it would involve revocation OR auto-commit. Reconciliations
        // revoking partitions cannot be triggered from the background because
        // the app thread could already be returning records for those
        // partitions. Reconciliations that only ADD new partitions are safe
        // to trigger from the background thread since new partitions won't
        // have buffered records.
        if !can_commit && (auto_commit_enabled || !revoked.is_empty()) {
            log::trace!(
                "Skipping reconciliation: can_commit=false and the reconciliation would either \
                 auto-commit (auto_commit_enabled={auto_commit_enabled}) or revoke partitions \
                 (revoked={revoked:?})."
            );
            return Ok(());
        }

        // 6. Mark reconciliation in progress.
        self.mark_reconciliation_in_progress();

        log::info!(
            "Reconciling assignment with local_epoch={}: assigned={:?} added={:?} revoked={:?}",
            resolved_assignment.local_epoch,
            assigned_topic_partitions,
            added,
            revoked
        );

        // 8. Mark partitions pending revocation to stop fetching.
        {
            let mut subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let revoked_vec: Vec<TopicPartition> = revoked.iter().cloned().collect();
            // Java: subscriptions.markPendingRevocation(revokedPartitions).
            if let Err(e) = subs.mark_pending_revocation(&revoked_vec) {
                log::warn!("mark_pending_revocation failed: {}", e);
            }
        }

        // 8a. Java `signalReconciliationStarted()` →
        // `CommitRequestManager::maybeAutoCommitSyncBeforeRebalance(deadlineMs)`
        // (`org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalReconciliationStarted`,
        // `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeReconcile`).
        //
        // Commit `subscriptions.allConsumed()` synchronously if auto-commit is
        // enabled. The deadline mirrors Java: the rebalance timeout (configured
        // on this membership manager). Milestone-12 divergence fix (PR #176):
        // do NOT await the commit inline. Java's `commitResult.whenComplete(...)`
        // runs the continuation (step 8b onward) WHEN the commit resolves, while
        // the `ConsumerNetworkThread` keeps spinning and
        // `markReconciliationCheckCompleted()` has already fired
        // (`org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor#process(AsyncPollEvent)`).
        // Awaiting inline here froze the AsyncPoll task's reconciliation-check mark behind a slow commit, so
        // `poll()` returned empty even for RETAINED partitions until the commit
        // resolved. Instead we store an `AwaitingCommit` leg (member stays
        // `RECONCILING`) and let the bg loop `try_recv` the commit result on a
        // later iteration (§31 — never awaited), so `reconcile()` returns
        // immediately and retained-partition fetching proceeds.
        //
        // Java's `whenComplete` on an ALREADY-completed future runs
        // synchronously (auto-commit disabled or no consumed offsets →
        // `maybeAutoCommitSyncBeforeRebalance` returns a completed future; and
        // the base
        // `org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#signalReconciliationStarted` returns a
        // completed future when there is no commit manager). We reproduce that by `try_recv`ing once:
        // if the result is already available, fall straight through to
        // `continue_after_commit` inline (behavior unchanged from before the
        // fix); only a genuinely in-flight commit stores the `AwaitingCommit`
        // leg and defers.
        if let Some(mut commit_rx) = hooks.signal_reconciliation_started(current_time_ms) {
            match commit_rx.try_recv() {
                Ok(commit_result) => {
                    // Already resolved — continue inline (Java's synchronous
                    // `whenComplete` on a completed future).
                    self.log_commit_result(commit_result);
                },
                Err(oneshot::error::TryRecvError::Empty) => {
                    // Genuinely in-flight — store the leg and return so the bg
                    // loop keeps spinning; `drive_pending_reconcile` resumes at
                    // step 8b when the commit resolves. Member stays RECONCILING.
                    self.store_pending(PendingReconcile::AwaitingCommit {
                        commit_rx,
                        resolved,
                        resolved_assignment,
                        assigned_topic_partitions,
                        added,
                        revoked,
                        current_time_ms,
                    });
                    return Ok(());
                },
                Err(oneshot::error::TryRecvError::Closed) => {
                    // Sender dropped before completion — log and proceed (Java's
                    // `whenComplete` error branch: proceed with revocation).
                    log::error!(
                        "Auto-commit before reconciling new assignment: receiver dropped without completion. \
                         Proceeding with the reconciliation anyway."
                    );
                },
            }
        }

        // 8b onward: abort check + `onPartitionsRevoked` enqueue. When there is
        // no commit manager we fall straight here (Java's base
        // `signalReconciliationStarted()` completed-future path).
        self.continue_after_commit(
            hooks,
            resolved,
            resolved_assignment,
            assigned_topic_partitions,
            added,
            revoked,
            current_time_ms,
        )
    }

    /// Log the auto-commit-before-rebalance result. Mirrors Java's
    /// `commitResult.whenComplete` success / failure logging
    /// (`AbstractMembershipManager#maybeReconcile`). The abort check + revoke
    /// enqueue in [`Self::continue_after_commit`] run on BOTH the success and
    /// failure branches, which is why the logging is separated from them —
    /// exactly as Java's `whenComplete` logs, then unconditionally runs
    /// `if (!maybeAbortReconciliation()) revokeAndAssign(...)`.
    fn log_commit_result(&self, commit_result: Result<(), Error>) {
        match commit_result {
            Ok(()) => {
                log::debug!("Auto-commit before reconciling new assignment completed successfully.");
            },
            Err(err) => {
                // Java: `log.error("Auto-commit request before reconciling new assignment failed. \
                // Will proceed with the reconciliation anyway.", commitReqError)`.
                log::error!(
                    "Auto-commit request before reconciling new assignment failed. \
                     Will proceed with the reconciliation anyway: {err}"
                );
            },
        }
    }

    /// Steps 8b-9 of `reconcile`, run after the auto-commit-before-rebalance
    /// has resolved (or was a no-op / had no commit manager). Mirrors the body
    /// of Java's `commitResult.whenComplete(...)` continuation
    /// (`AbstractMembershipManager#maybeReconcile`):
    ///
    /// ```java
    /// if (!maybeAbortReconciliation()) {
    ///     revokeAndAssign(resolvedAssignment, assignedTopicIdPartitions, revokedPartitions, addedPartitions);
    /// }
    /// ```
    ///
    /// The commit-result logging happens at the call site BEFORE this method
    /// (Java logs in the same `whenComplete`, on both branches, then runs the
    /// abort check + `revokeAndAssign` unconditionally — [`Self::log_commit_result`]).
    #[expect(
        clippy::too_many_arguments,
        reason = "the resume state of the whenComplete chain, plus the subclass hooks"
    )]
    fn continue_after_commit<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        revoked: HashSet<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), Error> {
        // 8b. Abort check, immediately after the commit resolves. Java:
        // `commitResult.whenComplete((__, commitReqError) -> { ...;
        // if (!maybeAbortReconciliation()) { revokeAndAssign(...); } })`
        // (`org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeReconcile`) — the guard runs on
        // BOTH the success and failure paths of the commit.
        //
        // Since the divergence fix (PR #176) the commit receiver IS stored as
        // an `AwaitingCommit` leg while in flight, so a concurrent fence /
        // fatal / stale transition CAN abandon this reconcile via
        // `clear_pending_reconcile()` (Phase 41 Issue 1), exactly as it abandons
        // the `AfterRevoke` / `AfterAssign` legs — the commit receiver is simply
        // dropped and the commit proceeds or times out server-side. In the bg
        // loop, reconcile-drive and the transitions run on the same task and
        // never truly overlap, but this abort check remains the Java-faithful
        // guard for a state change (fence / fatal) observed between the commit
        // resolving and the revoke enqueue.
        if self.maybe_abort_reconciliation() {
            return Ok(());
        }

        // 9. §31: enqueue onPartitionsRevoked. Phase 41b — do NOT await
        // the ack inline (that would freeze the bg loop, blocker b). Java
        // guards on `!partitionsRevoked.isEmpty() && listener.isPresent()`;
        // the Rust translation enqueues when non-empty and the app side is
        // responsible for the listener-present short-circuit (returned as
        // `None` here when no listener is registered).
        let revoke_ack = if revoked.is_empty() {
            None
        } else {
            let revoked_vec: Vec<TopicPartition> = revoked.iter().cloned().collect();
            hooks
                .signal_partitions_revoked(revoked_vec, current_time_ms)
                .map_err(|e| self.fail_reconciliation(e))?
        };

        match revoke_ack {
            Some(ack_rx) => {
                // Store the pending state and return so the bg loop keeps
                // spinning; the next `reconcile` entry will `try_recv` this
                // and resume at step 10. The member stays RECONCILING.
                self.store_pending(PendingReconcile::AfterRevoke {
                    ack_rx,
                    resolved,
                    resolved_assignment,
                    assigned_topic_partitions,
                    added,
                    current_time_ms,
                });
                Ok(())
            },
            // No revoked partitions OR no listener — the revoke callback is
            // a completed no-op, so proceed straight to step 10.
            None => self.continue_after_revoke(
                hooks,
                Ok(()),
                resolved,
                resolved_assignment,
                assigned_topic_partitions,
                added,
                current_time_ms,
            ),
        }
    }

    /// Java's `reconciliationResult.whenComplete` error arm
    /// (`AbstractMembershipManager#revokeAndAssign`):
    ///
    /// ```java
    /// reconciliationResult.whenComplete((__, error) -> {
    ///     if (error != null) {
    ///         // Leaving member in RECONCILING state after callbacks fail. ...
    ///         log.error("Reconciliation failed.", error);
    ///         markReconciliationCompleted();
    ///     }
    /// ```
    ///
    /// Java routes *every* failure in the revocation+assignment chain through
    /// that one arm, so the in-progress flag is always cleared. Rust replaced
    /// the `CompletableFuture` chain with explicit steps and `?`, which returns
    /// past the clearing — leaving `reconciliation_in_progress` set forever, so
    /// every later `reconcile()` short-circuits on "Another reconciliation is
    /// already in progress" and the member can never rebalance again.
    ///
    /// Note what is deliberately NOT done: the member stays in RECONCILING.
    /// Java's comment above is explicit — it does not send the ack, and expects
    /// the broker to kick the member out after the reconciliation commit
    /// timeout, giving a RECONCILING -> FENCED transition. Only the flag is
    /// cleared.
    ///
    /// This is the one place a reconciliation failure is logged at ERROR as
    /// "Reconciliation failed", as in Java's chain; both `reconcile` callers
    /// (`process_async_poll` and the network thread's Phase 2.5) only add a
    /// DEBUG follow-up.
    fn fail_reconciliation(&self, err: Error) -> Error {
        log::error!("Reconciliation failed: {err}");
        self.mark_reconciliation_completed();
        err
    }

    /// Drives a pending reconcile callback non-blockingly: `try_recv`s the
    /// stored ack receiver and, when it has resolved, resumes the matching
    /// post-callback continuation. While the ack is pending this is a
    /// no-op (the member stays RECONCILING) — Perf Contract item 1.
    ///
    /// `current_time_ms` is the live bg-loop iteration time, used for the
    /// resumed steps (assign-callback enqueue, `reset_auto_commit_timer`),
    /// mirroring Java's `whenComplete` running at completion time.
    fn drive_pending_reconcile<H: MembershipManagerHooks>(&self, hooks: &H, current_time_ms: i64) -> Result<(), Error> {
        // Pop the pending state out under the lock (so the lock is not held
        // while the resumed continuation re-locks it via `store_pending`),
        // then `try_recv`. If the ack is
        // not ready, put it back and return.
        let pending = {
            let mut guard = match self.pending_reconcile.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            // Clear the flag in lockstep with taking the state. If the ack
            // is not ready, `store_pending` below re-sets both.
            self.pending_reconcile_flag.store(false, std::sync::atomic::Ordering::Release);
            guard.take()
        };
        let Some(pending) = pending else {
            return Ok(());
        };

        match pending {
            PendingReconcile::AwaitingCommit {
                mut commit_rx,
                resolved,
                resolved_assignment,
                assigned_topic_partitions,
                added,
                revoked,
                current_time_ms: _enqueued_ms,
            } => {
                match commit_rx.try_recv() {
                    Ok(commit_result) => {
                        // Commit resolved — mirror Java's `commitResult.whenComplete`
                        // continuation: log the result, then run the abort check +
                        // `onPartitionsRevoked` enqueue. Use the LIVE bg-loop
                        // `current_time_ms` (Java's `whenComplete` runs at completion
                        // time), matching the `AfterRevoke` / `AfterAssign` arms.
                        self.log_commit_result(commit_result);
                        self.continue_after_commit(
                            hooks,
                            resolved,
                            resolved_assignment,
                            assigned_topic_partitions,
                            added,
                            revoked,
                            current_time_ms,
                        )
                    },
                    Err(oneshot::error::TryRecvError::Empty) => {
                        // Commit still in flight — put the leg back and return
                        // (loop keeps spinning). Member stays RECONCILING.
                        self.store_pending(PendingReconcile::AwaitingCommit {
                            commit_rx,
                            resolved,
                            resolved_assignment,
                            assigned_topic_partitions,
                            added,
                            revoked,
                            current_time_ms: _enqueued_ms,
                        });
                        Ok(())
                    },
                    Err(oneshot::error::TryRecvError::Closed) => {
                        // Sender dropped before completion — log and proceed
                        // (Java's `whenComplete` error branch: proceed with
                        // revocation anyway).
                        log::error!(
                            "Auto-commit before reconciling new assignment: receiver dropped without completion. \
                             Proceeding with the reconciliation anyway."
                        );
                        self.continue_after_commit(
                            hooks,
                            resolved,
                            resolved_assignment,
                            assigned_topic_partitions,
                            added,
                            revoked,
                            current_time_ms,
                        )
                    },
                }
            },
            PendingReconcile::AfterRevoke {
                mut ack_rx,
                resolved,
                resolved_assignment,
                assigned_topic_partitions,
                added,
                current_time_ms: _enqueued_ms,
            } => {
                match ack_rx.try_recv() {
                    Ok(result) => {
                        let result = match result {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                log::error!("onPartitionsRevoked callback failed: {}", e);
                                // Java: member stays RECONCILING after a
                                // callback failure; surface the error so the
                                // bg task can observe it (Java's
                                // `revocationResult.completeExceptionally`),
                                // and log "Reconciliation failed." in the
                                // chain, whichever caller resumed it.
                                return Err(self.fail_reconciliation(e));
                            },
                        };
                        self.continue_after_revoke(
                            hooks,
                            result,
                            resolved,
                            resolved_assignment,
                            assigned_topic_partitions,
                            added,
                            current_time_ms,
                        )
                    },
                    Err(oneshot::error::TryRecvError::Empty) => {
                        // Ack not ready — keep waiting (loop continues).
                        self.store_pending(PendingReconcile::AfterRevoke {
                            ack_rx,
                            resolved,
                            resolved_assignment,
                            assigned_topic_partitions,
                            added,
                            current_time_ms: _enqueued_ms,
                        });
                        Ok(())
                    },
                    Err(oneshot::error::TryRecvError::Closed) => {
                        // App side dropped the receiver before responding.
                        Err(self.fail_reconciliation(Error::local_illegal_state(
                            "Rebalance listener ack receiver dropped before completion",
                        )))
                    },
                }
            },
            PendingReconcile::AfterAssign {
                mut ack_rx,
                resolved,
                resolved_assignment,
                assigned_topic_partitions,
                added,
                current_time_ms: _enqueued_ms,
            } => match ack_rx.try_recv() {
                Ok(result) => self.continue_after_assign(
                    hooks,
                    result,
                    resolved,
                    resolved_assignment,
                    assigned_topic_partitions,
                    added,
                    current_time_ms,
                ),
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.store_pending(PendingReconcile::AfterAssign {
                        ack_rx,
                        resolved,
                        resolved_assignment,
                        assigned_topic_partitions,
                        added,
                        current_time_ms: _enqueued_ms,
                    });
                    Ok(())
                },
                Err(oneshot::error::TryRecvError::Closed) => Err(self.fail_reconciliation(Error::local_illegal_state(
                    "Rebalance listener ack receiver dropped before completion",
                ))),
            },
        }
    }

    /// Steps 10-13 of `reconcile`, run after the `onPartitionsRevoked`
    /// callback has completed (successfully — `revoke_result` is `Ok`).
    ///
    /// AK 4.3.1 (KAFKA-20106) reshape: this method no longer mutates the
    /// subscription state itself. It enqueues a
    /// [`BackgroundEvent::PartitionsAssigned`] (bg → app) — sent EVEN WHEN
    /// NO LISTENER is registered — and stores an `AfterAssign` pending
    /// state. The app thread (inside `poll()`) applies the assignment to
    /// the subscription state via an `ApplyAssignmentEvent` (→
    /// [`Self::apply_assignment`], which does the
    /// `assign_from_subscribed_awaiting_callback` + `notify_assignment_change`),
    /// runs `on_partitions_assigned` if a listener exists, then completes
    /// the ack — resuming at [`Self::continue_after_assign`]. This
    /// guarantees `consumer.assignment()` changes only within `poll()`.
    #[expect(
        clippy::too_many_arguments,
        reason = "the resume state of the whenComplete chain, plus the subclass hooks"
    )]
    fn continue_after_revoke<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        revoke_result: Result<(), Error>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), Error> {
        revoke_result.map_err(|e| self.fail_reconciliation(e))?;

        // 10. Abort check between steps (state may have moved because of a
        // fence / fatal).
        if self.maybe_abort_reconciliation() {
            return Ok(());
        }

        // 13. AK 4.3.1: enqueue `PartitionsAssigned` (bg → app). Java:
        //   `CompletableFuture<Void> result = signalPartitionsAssigned(assignedPartitions, addedPartitions);`
        // ConsumerMembershipManager's `signalPartitionsAssigned` enqueues a
        // `PartitionsAssignedEvent` carrying the FULL assignment and the
        // newly-added set. It is enqueued unconditionally (no listener-present
        // short-circuit) because the app thread must apply the assignment to
        // the subscription state within `poll()`. Phase 41b — do not await
        // inline; store `AfterAssign` and let the loop drive the ack.
        let added_vec: Vec<TopicPartition> = added.iter().cloned().collect();
        let assign_ack = hooks
            .signal_partitions_assigned(assigned_topic_partitions.clone(), added_vec, current_time_ms)
            .map_err(|e| self.fail_reconciliation(e))?;
        match assign_ack {
            Some(ack_rx) => {
                self.store_pending(PendingReconcile::AfterAssign {
                    ack_rx,
                    resolved,
                    resolved_assignment,
                    assigned_topic_partitions,
                    added,
                    current_time_ms,
                });
                Ok(())
            },
            // Java's `whenComplete` on an already completed future (a group
            // type that applies the assignment inline): run step 14 now.
            None => self.continue_after_assign(
                hooks,
                Ok(()),
                resolved,
                resolved_assignment,
                assigned_topic_partitions,
                added,
                current_time_ms,
            ),
        }
    }

    /// Steps 14-16 of `reconcile`, run after the `onPartitionsAssigned`
    /// callback has completed. Synchronous (no `.await`).
    #[expect(
        clippy::too_many_arguments,
        reason = "the resume state of the whenComplete chain, plus the subclass hooks"
    )]
    fn continue_after_assign<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        assigned_callback_result: Result<(), Error>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), Error> {
        // 14. Enable fetching for assigned partitions (only if the callback
        // succeeded — Java's `subscriptions.enablePartitionsAwaitingCallback`).
        match assigned_callback_result {
            Ok(()) => {
                let mut subs = match self.subscriptions.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if let Err(e) = subs.enable_partitions_awaiting_callback(&assigned_topic_partitions) {
                    log::warn!("enable_partitions_awaiting_callback failed: {}", e);
                }
            },
            Err(e) => {
                // Keeping newly added partitions as non-fetchable after the
                // callback failure. They will be retried on the next
                // reconciliation loop, until it succeeds or the broker removes
                // them from the assignment. AK 4.3.1 (KAFKA-20106) adds the
                // `subscriptions.assignedPartitions().containsAll(addedPartitions)`
                // guard to the warn log: the assignment may have already
                // changed (a newer reconciliation applied), in which case the
                // stale added set is no longer meaningful.
                if !added.is_empty() {
                    let still_assigned = {
                        let subs = match self.subscriptions.lock() {
                            Ok(g) => g,
                            Err(p) => p.into_inner(),
                        };
                        let assigned = subs.assigned_partitions();
                        added.iter().all(|tp| assigned.contains(tp))
                    };
                    if still_assigned {
                        log::warn!(
                            "Leaving newly assigned partitions {:?} marked as non-fetchable and not \
                             requiring initializing positions after onPartitionsAssigned callback failed: {}",
                            added,
                            e
                        );
                    }
                }
                // Per COMMENTS.1.md fix #2: surface listener failure so the
                // caller (Phase 10 bg task) can observe it. Java's
                // CompletableFuture chain propagates the error via
                // `reconciliationResult.whenComplete(error, ...)`, which logs
                // "Reconciliation failed." and marks the reconciliation
                // completed.
                return Err(self.fail_reconciliation(e));
            },
        }

        // 15. Update local cache of assigned topic names.
        {
            let mut assigned_topic_names: HashSet<String> = HashSet::new();
            for (_id, name, _partitions) in &resolved {
                assigned_topic_names.insert(name.clone());
            }
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.assigned_topic_names_cache.retain(|_, v| assigned_topic_names.contains(v));
        }

        // 16. Final abort check; advance to ACKNOWLEDGING and call
        // signalReconciliationCompleting.
        let aborted = self.maybe_abort_reconciliation();
        if !aborted {
            // The transition result is captured and handled AFTER the guard is
            // dropped: `fail_reconciliation` locks the same `inner` mutex, and
            // `std::sync::Mutex` is not reentrant (CLAUDE.md §11.6), so calling
            // it while holding the guard would self-deadlock on the error path
            // — a path no test would notice until it hung in production.
            let transition = {
                let mut guard = match self.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.current_assignment = resolved_assignment;
                guard.transition_to(MemberState::Acknowledging)
            };
            transition.map_err(|e| self.fail_reconciliation(e))?;
            // Java: signalReconciliationCompleting() (the consumer resets the
            // auto-commit timer).
            hooks.signal_reconciliation_completing(current_time_ms);
            self.mark_reconciliation_completed();
        }
        Ok(())
    }

    /// Transition to `PREPARE_LEAVING`, then to `LEAVING` once the assignment
    /// is released, and set the membership operation to perform on close.
    /// Invoked by `Consumer::close(...)`.
    ///
    /// Returns the future that completes when the leave operation completes
    /// (see [`Self::leave_group_with_run_callbacks`]). It runs no rebalance
    /// callbacks, so the whole operation up to `LEAVING` is synchronous.
    ///
    /// Java: `AbstractMembershipManager.leaveGroupOnClose(GroupMembershipOperation)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#leaveGroupOnClose")]
    pub(crate) fn leave_group_on_close<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        membership_operation: GroupMembershipOperation,
        current_time_ms: i64,
    ) -> Result<KafkaFutureImpl<()>, Error> {
        self.set_leave_group_operation(membership_operation);
        self.leave_group_with_run_callbacks(hooks, false, current_time_ms)
    }

    /// Transition to `PREPARE_LEAVING` to release the assignment, then to
    /// `LEAVING` to send the heartbeat that leaves the group. Invoked by
    /// `Consumer::unsubscribe()`. Runs the rebalance callbacks (see
    /// [`Self::leave_group_with_run_callbacks`]).
    ///
    /// Java: `AbstractMembershipManager.leaveGroup()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#leaveGroup")]
    pub(crate) fn leave_group<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        current_time_ms: i64,
    ) -> Result<KafkaFutureImpl<()>, Error> {
        self.leave_group_with_run_callbacks(hooks, true, current_time_ms)
    }

    /// Java: `AbstractMembershipManager.leaveGroup(boolean runCallbacks)`.
    /// The shared body of `leave_group` / `leave_group_on_close`, run
    /// synchronously on the background task, as Java runs it inside
    /// `ApplicationEventProcessor.process(...)` on the network thread — so the
    /// heartbeat manager's poll in the same `run_once` already sees the member
    /// leaving and builds the leave heartbeat.
    ///
    /// 1. Not in the group: clear and unsubscribe (no heartbeat needed;
    ///    `FENCED` is reset to `UNSUBSCRIBED` after clearing the assignment)
    ///    and return an already completed future.
    /// 2. Already leaving (`PREPARE_LEAVING` / `LEAVING`): return the existing
    ///    leave-in-progress future, which completes when the ongoing leave
    ///    operation completes.
    /// 3. Otherwise transition to `PREPARE_LEAVING` and store a new
    ///    leave-in-progress future. With `run_callbacks`, enqueue the
    ///    callback releasing the assignment
    ///    ([`MembershipManagerHooks::signal_member_leaving_group`]). If one was enqueued, its
    ///    ack is stored for the bg loop, which runs
    ///    `clear_assignment_and_leave_group` once the ack arrives
    ///    ([`Self::drive_pending_leave_callback`]; Java chains that step with
    ///    `callbackResult.whenComplete(...)`); otherwise the member clears its
    ///    assignment and moves to `LEAVING` right here.
    ///
    /// The returned future completes only through
    /// `maybe_complete_leave_in_progress()`: when the response to the leave
    /// heartbeat is received (success or failure), when the leave heartbeat
    /// could not be sent, or when the member is fenced or fails while
    /// leaving. Rust returns the completable handle so the caller can chain
    /// the event completion with `when_complete`, mirroring Java's
    /// `future.whenComplete(complete(event.future()))`; callers never
    /// complete it themselves.
    fn leave_group_with_run_callbacks<H: MembershipManagerHooks>(
        &self,
        hooks: &H,
        run_callbacks: bool,
        current_time_ms: i64,
    ) -> Result<KafkaFutureImpl<()>, Error> {
        // Step 1: already-out-of-group fast path.
        let pre_state = self.state();
        if matches!(
            pre_state,
            MemberState::Unsubscribed | MemberState::Fenced | MemberState::Fatal | MemberState::Stale
        ) {
            if pre_state == MemberState::Fenced {
                self.clear_assignment();
                let mut guard = match self.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.transition_to(MemberState::Unsubscribed)?;
            }
            {
                let mut subs = match self.subscriptions.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                subs.unsubscribe();
            }
            {
                let guard = match self.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.notify_assignment_change(&std::collections::HashSet::new());
            }
            let completed = KafkaFutureImpl::new();
            completed.complete(());
            return Ok(completed);
        }

        // Steps 2 and 3 under one guard, so the state check, the transition
        // and the future registration are atomic with respect to the
        // completion sites (which take the future under the same guard).
        let leave_result = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if matches!(guard.state, MemberState::PrepareLeaving | MemberState::Leaving) {
                // Member already leaving. No-op and return the existing leave
                // group future that will complete when the ongoing leave
                // operation completes.
                log::debug!("Leave group operation already in progress for member {}", guard.member_id);
                // Deviation: Java's `leaveGroupInProgress.get()` throws
                // `NoSuchElementException` when the member is leaving with no
                // future registered. This is reachable when an invalid-epoch
                // heartbeat response arrives while the member is
                // PREPARE_LEAVING: `onHeartbeatSuccess` takes and completes the
                // future but the state stays PREPARE_LEAVING, so a second
                // `unsubscribe()` or a `close()` lands here. Java 4.4's
                // `ConsumerNetworkThread.processApplicationEvents` catches the
                // throwable and fails the event at once (`unsubscribe()` then
                // throws a `KafkaException`; `close()` logs "Failed to leave
                // group while closing consumer"). Rust deliberately registers a
                // fresh future instead, so the caller waits for the ongoing
                // leave to end and then succeeds.
                let in_progress = guard.leave_group_in_progress.get_or_insert_with(KafkaFutureImpl::new);
                return Ok(in_progress.clone());
            }
            guard.transition_to(MemberState::PrepareLeaving)?;
            let leave_result = KafkaFutureImpl::new();
            guard.leave_group_in_progress = Some(leave_result.clone());
            leave_result
        };

        if run_callbacks {
            match hooks.signal_member_leaving_group(current_time_ms) {
                Ok(Some(ack_rx)) => {
                    self.store_pending_leave_callback(ack_rx);
                    return Ok(leave_result);
                },
                Ok(None) => {
                    log::info!(
                        "Member {} completed callback to release assignment. It will proceed to clear \
                         its assignment and send a leave group heartbeat",
                        self.member_id()
                    );
                },
                Err(e) => {
                    log::error!(
                        "Member {} callback to release assignment failed. It will proceed to clear its \
                         assignment and send a leave group heartbeat: {}",
                        self.member_id(),
                        e
                    );
                },
            }
        } else {
            log::debug!(
                "Member {} attempting to leave has no rebalance callbacks, so it will clear assignments \
                 and transition to send heartbeat to leave group.",
                self.member_id()
            );
        }
        self.clear_assignment_and_leave_group(hooks)?;

        // Return future to indicate that the leave group is done when the
        // callbacks complete, and the transition to send the heartbeat has
        // been made.
        Ok(leave_result)
    }

    /// `true` while an `unsubscribe()` leave waits in `PREPARE_LEAVING` for
    /// its release callback's ack. Lock-free.
    pub(crate) fn has_pending_leave_callback(&self) -> bool {
        self.pending_leave_callback_flag.load(std::sync::atomic::Ordering::Acquire)
    }

    /// A second leave can only store over an older, still-pending ack after
    /// the first leave ended without it (fenced or failed while
    /// PREPARE_LEAVING, then rejoined). Replacing it drops only that stale
    /// clear-and-leave step — the first leave's future was already completed
    /// by the fence / fatal transition. Java would still run the stale step
    /// and move the new leave to LEAVING before its own callback ran.
    fn store_pending_leave_callback(&self, ack_rx: SignalAck) {
        let mut guard = match self.pending_leave_callback.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *guard = Some(ack_rx);
        self.pending_leave_callback_flag
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// The step Java chains on the leave callback with
    /// `callbackResult.whenComplete(...)` in `leaveGroup(boolean)`: once the
    /// ack of the callback [`Self::leave_group`] enqueued has arrived, clear
    /// the assignment and transition to `LEAVING`, whether the callback failed
    /// or succeeded. While the ack is pending this is a no-op (`try_recv`, no
    /// await), so the bg loop keeps spinning (§31).
    ///
    /// The app side returns the ack inside a
    /// `ConsumerRebalanceListenerCallbackCompleted` application event, and
    /// the processor drives this right after completing it
    /// (`ConsumerMembershipManager::consumer_rebalance_listener_callback_completed`) — where
    /// Java runs the `whenComplete`, on the network thread while processing
    /// the callback-completed event. That is in Phase 1 of `run_once`, before
    /// the request managers poll, so the heartbeat manager's poll in the same
    /// iteration builds the leave heartbeat. The bg loop also drives it
    /// around each drained event, which only matters for an ack dropped with
    /// no completed event to carry it (the callback event was dropped before
    /// the app ran the listener, or processing the completed event panicked):
    /// the ack then reads as closed. An error-completed ack — e.g. the
    /// interrupt error sent when the caller's future was dropped mid-listener
    /// — still clears the assignment and leaves, as Java's `whenComplete`
    /// does on error.
    pub(crate) fn drive_pending_leave_callback<H: MembershipManagerHooks>(&self, hooks: &H) -> Result<(), Error> {
        let pending = {
            let mut guard = match self.pending_leave_callback.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            self.pending_leave_callback_flag
                .store(false, std::sync::atomic::Ordering::Release);
            guard.take()
        };
        let Some(mut ack_rx) = pending else {
            return Ok(());
        };
        match ack_rx.try_recv() {
            Ok(Ok(())) => log::info!(
                "Member {} completed callback to release assignment. It will proceed to clear its \
                 assignment and send a leave group heartbeat",
                self.member_id()
            ),
            Ok(Err(e)) => log::error!(
                "Member {} callback to release assignment failed. It will proceed to clear its \
                 assignment and send a leave group heartbeat: {}",
                self.member_id(),
                e
            ),
            Err(oneshot::error::TryRecvError::Closed) => log::error!(
                "Member {} callback to release assignment failed. It will proceed to clear its \
                 assignment and send a leave group heartbeat: rebalance listener ack receiver \
                 dropped before completion",
                self.member_id()
            ),
            Err(oneshot::error::TryRecvError::Empty) => {
                // Ack not ready — keep waiting (the loop continues).
                self.store_pending_leave_callback(ack_rx);
                return Ok(());
            },
        }
        // Clear the assignment, no matter if the callback execution failed or
        // succeeded.
        self.clear_assignment_and_leave_group(hooks)
    }

    /// Java: `AbstractMembershipManager.clearAssignmentAndLeaveGroup()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#clearAssignmentAndLeaveGroup")]
    fn clear_assignment_and_leave_group<H: MembershipManagerHooks>(&self, hooks: &H) -> Result<(), Error> {
        {
            let mut subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.unsubscribe();
        }
        self.clear_assignment();

        // Transition to ensure that a heartbeat request is sent out to
        // effectively leave the group (even in the case where the member had
        // no assignment to release or when the callback execution failed.)
        self.transition_to_sending_leave_group(hooks.leave_group_epoch(), false)
    }

    /// Java: `onHeartbeatFailure(boolean retriable)` shared bookkeeping.
    /// On a non-retriable failure, records a failed rebalance (if one was in
    /// progress). The leave group request is sent out once (not retried), so
    /// an UNSUBSCRIBED member completes the leave in progress once the request
    /// completes, regardless of the response (Java:
    /// `if (state == UNSUBSCRIBED && maybeCompleteLeaveInProgress())`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#onHeartbeatFailure")]
    pub(crate) fn on_heartbeat_failure(&self, retriable: bool) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        // Java: `if (!retriable) metricsManager.maybeRecordRebalanceFailed();`.
        if !retriable && let Some(metrics) = &guard.metrics_manager {
            metrics.maybe_record_rebalance_failed();
        }
        if guard.state != MemberState::Unsubscribed {
            return;
        }
        let leave = guard.take_leave_in_progress();
        drop(guard);
        if Self::complete_leave(leave) {
            log::warn!(
                "Member {} with epoch {} received a failed response to the heartbeat to leave the group \
                 and completed the leave operation. ",
                self.member_id(),
                self.member_epoch()
            );
        }
    }

    /// Complete the leave in progress (if any). This is expected to be used to
    /// complete the leave in progress when a member receives the response to
    /// the leave heartbeat. Returns `true` if a leave was in progress.
    ///
    /// Java: `AbstractMembershipManager#maybeCompleteLeaveInProgress`. The future is taken under
    /// the guard and completed after the guard is released, since completing
    /// it runs the continuations chained on it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#maybeCompleteLeaveInProgress")]
    pub(crate) fn maybe_complete_leave_in_progress(&self) -> bool {
        let leave = {
            let mut guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.take_leave_in_progress()
        };
        Self::complete_leave(leave)
    }

    /// Second half of [`Self::maybe_complete_leave_in_progress`]: complete a
    /// future already taken with [`MembershipInner::take_leave_in_progress`].
    /// MUST be called with no membership guard held. Returns `true` if there
    /// was a future to complete.
    pub(crate) fn complete_leave(leave: Option<KafkaFutureImpl<()>>) -> bool {
        match leave {
            Some(leave) => {
                leave.complete(());
                true
            },
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::ConsumerRebalanceListener;
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use crate::consumer::internals::ConsumerMetadata;
    use async_trait::async_trait;
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    /// Test-only no-op rebalance listener so the §31 short-circuit
    /// (added in COMMENTS.1.md fix #1) lets the handshake proceed.
    struct NoopListener;
    #[async_trait]
    impl ConsumerRebalanceListener for NoopListener {
        async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
            Ok(())
        }
        async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
            Ok(())
        }
    }

    fn setup() -> (
        Arc<Mutex<SubscriptionState>>,
        Arc<Metadata>,
        Arc<BackgroundEventHandler>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        // Register a no-op listener so the §31 short-circuit allows
        // the handshake to enqueue an event in these tests.
        subs.lock()
            .unwrap()
            .subscribe_with_topics(HashSet::new(), Some(Arc::new(NoopListener)))
            .unwrap();
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let metadata =
            ConsumerMetadata::with_config(&config, subs.clone(), ClusterResourceListeners::new()).metadata_arc();
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        (subs, metadata, beh, rx)
    }

    #[test]
    fn new_starts_unsubscribed_with_random_member_id() {
        let (subs, metadata, beh, _rx) = setup();
        let mgr = AbstractMembershipManager::new(
            "g",
            subs,
            metadata,
            beh,
            true,
            None,
            Arc::new(crate::common::utils::SystemTime),
        );
        let inner = mgr.inner.lock().unwrap();
        assert_eq!(inner.state, MemberState::Unsubscribed);
        assert_eq!(inner.member_epoch, 0);
        assert!(!inner.member_id.is_empty());
        assert_eq!(inner.group_id, "g");
    }

    #[test]
    fn invalid_transition_returns_error() {
        let (subs, metadata, beh, _rx) = setup();
        let mgr = AbstractMembershipManager::new(
            "g",
            subs,
            metadata,
            beh,
            true,
            None,
            Arc::new(crate::common::utils::SystemTime),
        );
        let mut inner = mgr.inner.lock().unwrap();
        // UNSUBSCRIBED → STABLE is invalid.
        let err = inner.transition_to(MemberState::Stable).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalState(_)));
    }

    #[test]
    fn valid_transition_succeeds() {
        let (subs, metadata, beh, _rx) = setup();
        let mgr = AbstractMembershipManager::new(
            "g",
            subs,
            metadata,
            beh,
            true,
            None,
            Arc::new(crate::common::utils::SystemTime),
        );
        let mut inner = mgr.inner.lock().unwrap();
        // UNSUBSCRIBED → PREPARE_LEAVING valid.
        inner.transition_to(MemberState::PrepareLeaving).unwrap();
        assert_eq!(inner.state, MemberState::PrepareLeaving);
    }

    #[test]
    fn local_assignment_update_with_same_returns_none() {
        let mut partitions = HashMap::new();
        partitions.insert(Uuid::random_uuid(), vec![0, 1]);
        let assignment = LocalAssignment::new(0, partitions.clone()).unwrap();
        assert!(assignment.update_with(partitions).is_none());
    }

    #[test]
    fn local_assignment_update_with_new_bumps_epoch() {
        let mut partitions = HashMap::new();
        partitions.insert(Uuid::random_uuid(), vec![0, 1]);
        let assignment = LocalAssignment::new(0, partitions.clone()).unwrap();
        let mut new_partitions = HashMap::new();
        new_partitions.insert(Uuid::random_uuid(), vec![2, 3]);
        let updated = assignment.update_with(new_partitions).unwrap();
        assert_eq!(updated.local_epoch, 1);
    }

    #[test]
    fn local_assignment_none_epoch_with_partitions_fails() {
        let mut partitions = HashMap::new();
        partitions.insert(Uuid::random_uuid(), vec![0, 1]);
        let err = LocalAssignment::new(LocalAssignment::NONE_EPOCH, partitions).unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)));
    }
}
