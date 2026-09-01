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

//! `ConsumerMembershipManager` — KIP-848 consumer-group membership
//! manager. Composes [`super::abstract_membership_manager::AbstractMembershipManager`]
//! and supplies the Consumer-specific configuration (group instance
//! ID, server assignor, rack ID), and the §31 reconcile pipeline
//! (which is async because it `.await`s rebalance-listener callbacks).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::common::metrics::Time;
use crate::common::protocol::Errors;
use crate::common::requests::ConsumerGroupHeartbeatResponse;
use crate::common::requests::consumer_group_heartbeat_request::{
    JOIN_GROUP_MEMBER_EPOCH, LEAVE_GROUP_MEMBER_EPOCH, LEAVE_GROUP_STATIC_MEMBER_EPOCH,
};
use crate::common::{KafkaError, TopicPartition, Uuid};
use crate::consumer::close_options::GroupMembershipOperation;
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
#[cfg(test)]
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;

use super::abstract_membership_manager::{AbstractMembershipManager, LocalAssignment};
use super::commit_request_manager::CommitRequestManager;
use super::consumer_metadata::ConsumerMetadata;
use super::consumer_rebalance_metrics_manager::ConsumerRebalanceMetricsManager;
use super::member_state::MemberState;
use super::network_client_delegate::PollResult;
use super::request_manager::RequestManager;
use super::subscription_state::SubscriptionState;

/// KIP-848 consumer-group membership manager.
///
/// Java: `ConsumerMembershipManager extends AbstractMembershipManager<ConsumerGroupHeartbeatResponse>`.
///
/// Composition layout:
///
/// - `abstract_mm: AbstractMembershipManager` — shared state + state
///   machine (via `Arc<Mutex<MembershipInner>>`).
/// - Consumer-specific configuration (group instance ID, server
///   assignor, rack ID, rebalance timeout).
/// - `commit_request_manager: Option<Arc<CommitRequestManager>>` — used
///   by the auto-commit-before-rebalance step. Wrapping in `Option` so
///   constructor can wire it up later (and so tests can pass `None`).
pub(crate) struct ConsumerMembershipManager {
    pub(crate) abstract_mm: AbstractMembershipManager,
    /// Java: `Optional<String> groupInstanceId`. If present this is a
    /// static member; leave-group epoch becomes -2.
    pub(crate) group_instance_id: Option<String>,
    /// Java: `Optional<String> rackId`. Sent on join.
    pub(crate) rack_id: Option<String>,
    /// Java: `int rebalanceTimeoutMs`. Used as the deadline for the
    /// pre-rebalance auto-commit retry budget.
    pub(crate) rebalance_timeout_ms: i32,
    /// Java: `Optional<String> serverAssignor`.
    pub(crate) server_assignor: Option<String>,
    /// Java: `CommitRequestManager commitRequestManager`. Optional
    /// because Phase 8b's tests construct membership managers without
    /// wiring up the full commit pipeline; Phase 10 wires it through
    /// the supplier.
    pub(crate) commit_request_manager: Option<Arc<CommitRequestManager>>,
    /// Java: `CloseOptions.GroupMembershipOperation leaveGroupOperation`.
    /// Stored on the membership manager so `leave_group_epoch()` can
    /// dispatch correctly.
    pub(crate) leave_group_operation: Mutex<GroupMembershipOperation>,
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
    pending_reconcile: Mutex<Option<PendingReconcile>>,
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
}

/// Cross-iteration state for a reconcile callback awaiting its app-side
/// ack (Phase 41b). Mirrors the resume points of Java's
/// `revokeAndAssign(...).whenComplete(...)` chain.
enum PendingReconcile {
    /// `onPartitionsRevoked` was enqueued (`reconcile` step 9). On ack we
    /// resume at step 10 (abort check → assign-callback enqueue).
    AfterRevoke {
        ack_rx: oneshot::Receiver<Result<(), KafkaError>>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    },
    /// `onPartitionsAssigned` was enqueued (`reconcile` step 13). On ack we
    /// resume at step 14 (enable-partitions / failure path → ACKNOWLEDGING).
    AfterAssign {
        ack_rx: oneshot::Receiver<Result<(), KafkaError>>,
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
        ack_rx: oneshot::Receiver<Result<(), KafkaError>>,
    },
    /// `transitionToFatal` release tail: `clearAssignment()`.
    Fatal {
        ack_rx: oneshot::Receiver<Result<(), KafkaError>>,
    },
    /// `transitionToStale` release tail: `clearAssignment()`, clear the
    /// release-pending flag, and (if a timer reset requested a rejoin while
    /// the release was in flight) `transitionToJoining()`.
    Stale {
        ack_rx: oneshot::Receiver<Result<(), KafkaError>>,
    },
}

/// Discriminator for the release-tail to run in
/// [`ConsumerMembershipManager::drive_pending_release`], decoupled from the
/// owned ack receiver so the `try_recv` / store-back handling stays uniform.
#[derive(Clone, Copy)]
enum ReleaseKind {
    Fenced,
    Fatal,
    Stale,
}

impl ConsumerMembershipManager {
    /// Java constructor (the test-visible 13-arg variant). Drops
    /// `LogContext` is dropped (we use `log`). The `Metrics` /
    /// `RebalanceMetricsManager` are wired through as
    /// `Option<Arc<ConsumerRebalanceMetricsManager>>` + a metrics `Time` clock
    /// (M5): `None` in tests that don't exercise rebalance metrics; the live
    /// consumer always supplies them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        group_id: impl Into<String>,
        group_instance_id: Option<String>,
        rack_id: Option<String>,
        rebalance_timeout_ms: i32,
        server_assignor: Option<String>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        commit_request_manager: Option<Arc<CommitRequestManager>>,
        metadata: Arc<ConsumerMetadata>,
        background_event_handler: Arc<BackgroundEventHandler>,
        auto_commit_enabled: bool,
        metrics_manager: Option<Arc<ConsumerRebalanceMetricsManager>>,
        time: Arc<dyn Time>,
    ) -> Self {
        let abstract_mm = AbstractMembershipManager::new(
            group_id,
            subscriptions,
            metadata,
            background_event_handler,
            auto_commit_enabled,
            metrics_manager,
            time,
        );
        Self {
            abstract_mm,
            group_instance_id,
            rack_id,
            rebalance_timeout_ms,
            server_assignor,
            commit_request_manager,
            leave_group_operation: Mutex::new(GroupMembershipOperation::Default),
            pending_reconcile: Mutex::new(None),
            pending_reconcile_flag: std::sync::atomic::AtomicBool::new(false),
            pending_release: Mutex::new(None),
            pending_release_flag: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Java: `groupId()`.
    pub(crate) fn group_id(&self) -> String {
        let g = match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.group_id.clone()
    }

    /// Java: `memberId()`.
    pub(crate) fn member_id(&self) -> String {
        let g = match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.member_id.clone()
    }

    /// Java: `memberEpoch()`.
    pub(crate) fn member_epoch(&self) -> i32 {
        let g = match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.member_epoch
    }

    /// Java: `state()`.
    pub(crate) fn state(&self) -> MemberState {
        let g = match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.state
    }

    /// Java: `groupInstanceId()`.
    pub(crate) fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Java: `rackId()`.
    pub(crate) fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Java: `serverAssignor()`.
    pub(crate) fn server_assignor(&self) -> Option<&str> {
        self.server_assignor.as_deref()
    }

    /// Java: `leaveGroupOperation()`.
    pub(crate) fn leave_group_operation(&self) -> GroupMembershipOperation {
        let g = match self.leave_group_operation.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *g
    }

    /// Set the leave-group operation. Java sets this in
    /// `leaveGroupOnClose(...)`.
    pub(crate) fn set_leave_group_operation(&self, op: GroupMembershipOperation) {
        let mut g = match self.leave_group_operation.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *g = op;
    }

    /// Java: `currentAssignment()`.
    pub(crate) fn current_assignment(&self) -> LocalAssignment {
        let g = match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.current_assignment.clone()
    }

    /// Java: `joinGroupEpoch()` — 0 for the consumer group protocol.
    pub(crate) fn join_group_epoch(&self) -> i32 {
        JOIN_GROUP_MEMBER_EPOCH
    }

    /// Java: `leaveGroupEpoch()`. For static members + `LEAVE_GROUP`
    /// operation: -1 (force fence). Otherwise: -1 for dynamic members,
    /// -2 for static members.
    pub(crate) fn leave_group_epoch(&self) -> i32 {
        let is_static_member = self.group_instance_id.is_some();
        if matches!(self.leave_group_operation(), GroupMembershipOperation::LeaveGroup) {
            return LEAVE_GROUP_MEMBER_EPOCH;
        }
        if is_static_member {
            LEAVE_GROUP_STATIC_MEMBER_EPOCH
        } else {
            LEAVE_GROUP_MEMBER_EPOCH
        }
    }

    /// Java: `transitionToJoining()`.
    pub(crate) fn transition_to_joining(&self) -> Result<(), KafkaError> {
        self.abstract_mm.transition_to_joining(self.join_group_epoch())
    }

    /// Java: `transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer)`.
    pub(crate) fn transition_to_sending_leave_group(&self, due_to_expired_poll_timer: bool) -> Result<(), KafkaError> {
        self.abstract_mm
            .transition_to_sending_leave_group(self.leave_group_epoch(), due_to_expired_poll_timer)
    }

    /// Java: `onHeartbeatSuccess(ConsumerGroupHeartbeatResponse)`.
    /// Updates member info and state from a successful response.
    ///
    /// Returns `Err(KafkaError)` for unexpected errors in the response
    /// body — Java throws `IllegalArgumentException`.
    pub(crate) fn on_heartbeat_success(&self, response: &ConsumerGroupHeartbeatResponse) -> Result<(), KafkaError> {
        let data = response.data();
        if data.error_code != Errors::None.code() {
            return Err(KafkaError::illegal_argument(format!(
                "Unexpected error in Heartbeat response. Expected no error, but received: {:?}",
                Errors::for_code(data.error_code)
            )));
        }
        // Short-circuit decisions based on current state.
        let mut guard = match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let state = guard.state;
        if state == MemberState::Leaving {
            log::debug!(
                "Ignoring heartbeat response received from broker. Member {} with epoch {} is already leaving the group.",
                guard.member_id,
                guard.member_epoch
            );
            return Ok(());
        }
        if state == MemberState::Unsubscribed && data.member_epoch < 0 {
            // Java: maybeCompleteLeaveInProgress (we don't track the
            // leave future here — Phase 10 wires the close handshake).
            log::debug!(
                "Member {} with epoch {} received a successful response to the heartbeat to leave the group.",
                guard.member_id,
                guard.member_epoch
            );
            return Ok(());
        }
        if guard.is_not_in_group() {
            log::debug!(
                "Ignoring heartbeat response received from broker. Member {} is in {} state so it's not a member of the group.",
                guard.member_id,
                state
            );
            return Ok(());
        }
        if data.member_epoch < 0 {
            log::debug!(
                "Ignoring heartbeat response received from broker. Member {} with epoch {} is in {} state and the member epoch is invalid: {}.",
                guard.member_id,
                guard.member_epoch,
                state,
                data.member_epoch
            );
            return Ok(());
        }

        guard.update_member_epoch(data.member_epoch);

        // Assignment field is only populated when there's a new target
        // assignment for the member.
        if data.assignment.is_some() && !state.can_handle_new_assignment() {
            log::debug!(
                "Ignoring new assignment received from server because member is in {} state.",
                state
            );
            return Ok(());
        }

        // Build the new assignment from the response (release the
        // lock first; we'll re-acquire inside process_assignment_received).
        let new_assignment = data.assignment.as_ref().map(|assignment| {
            let mut map: HashMap<Uuid, Vec<i32>> = HashMap::new();
            for tp in &assignment.topic_partitions {
                map.insert(tp.topic_id, tp.partitions.clone());
            }
            map
        });
        drop(guard);

        if let Some(assignment) = new_assignment {
            self.abstract_mm.process_assignment_received(assignment)?;
        }
        Ok(())
    }

    /// Java: `transitionToFatal()` (override that wires Consumer-specific
    /// follow-on actions). The flow mirrors Java exactly:
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
    pub(crate) fn transition_to_fatal(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // Issue 1: abandon any in-flight reconcile — the member is leaving
        // RECONCILING, so the stored reconcile continuation is stale.
        self.clear_pending_reconcile();

        let previous_state = self.abstract_mm.transition_to_fatal()?;

        // If we were UNSUBSCRIBED / LEAVING / PREPARE_LEAVING, Java's
        // semantics are: no onPartitionsLost callback; the leave
        // future (if any) is completed silently.
        if matches!(
            previous_state,
            MemberState::Unsubscribed | MemberState::Leaving | MemberState::PrepareLeaving
        ) {
            return Ok(());
        }

        // Enqueue onPartitionsLost via the §31 handshake to release
        // assignment, non-blockingly. When no listener is registered or no
        // partitions are owned, the callback is a completed no-op and we run
        // the release tail inline.
        let partitions = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        let ack = self.enqueue_release_callback(partitions, current_time_ms)?;
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
        self.abstract_mm.clear_assignment();
    }

    /// Java: `transitionToFenced()`. Same shape as `transition_to_fatal`
    /// but transitions to FENCED and then JOINING after the listener
    /// completes (so the member rejoins).
    pub(crate) fn transition_to_fenced(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // Issue 1: abandon any in-flight reconcile — the member is leaving
        // RECONCILING, so the stored reconcile continuation is stale.
        self.clear_pending_reconcile();

        let pre_state = {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.state
        };

        // Handle Java's "already leaving / unsubscribed" short-circuit.
        match pre_state {
            MemberState::PrepareLeaving => {
                self.transition_to_sending_leave_group(false)?;
                let mut guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.transition_to(MemberState::Unsubscribed)?;
                return Ok(());
            },
            MemberState::Leaving => {
                let mut guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.transition_to(MemberState::Unsubscribed)?;
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
            let mut guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.transition_to(MemberState::Fenced)?;
            // resetEpoch -> updateMemberEpoch(joinGroupEpoch()).
            guard.update_member_epoch(self.join_group_epoch());
        }

        // Enqueue onPartitionsLost non-blockingly (Phase 41, Issue 2).
        let partitions = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        let ack = self.enqueue_release_callback(partitions, current_time_ms)?;
        match ack {
            Some(ack_rx) => {
                self.store_pending_release(PendingRelease::Fenced { ack_rx });
                Ok(())
            },
            None => self.continue_after_fenced_release(),
        }
    }

    /// Release tail of `transition_to_fenced` (Java's `whenComplete`):
    /// `clearAssignment()` then, if still `FENCED`, `transitionToJoining()`.
    fn continue_after_fenced_release(&self) -> Result<(), KafkaError> {
        self.abstract_mm.clear_assignment();

        // Now transition to JOINING if still FENCED.
        let still_fenced = {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.state == MemberState::Fenced
        };
        if still_fenced {
            self.transition_to_joining()?;
        }
        Ok(())
    }

    /// Release the assignment held by a member that has just transitioned
    /// to STALE because of an expired poll timer, then (if a timer reset
    /// already requested it) rejoin.
    ///
    /// Java: the async tail of `AbstractMembershipManager.transitionToStale()`
    /// (`AbstractMembershipManager.java:791-806`):
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
    pub(crate) fn transition_to_stale(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // Issue 1: abandon any in-flight reconcile — the member is leaving
        // RECONCILING, so the stored reconcile continuation is stale.
        self.clear_pending_reconcile();

        // Release assignment via onPartitionsLost (Java's
        // `signalPartitionsLost(subscriptions.assignedPartitions())`),
        // enqueued non-blockingly (Phase 41, Issue 2).
        let partitions = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        let ack = self.enqueue_release_callback(partitions, current_time_ms)?;
        match ack {
            Some(ack_rx) => {
                self.store_pending_release(PendingRelease::Stale { ack_rx });
                Ok(())
            },
            None => self.continue_after_stale_release(),
        }
    }

    /// Release tail of `transition_to_stale` (Java's
    /// `staleMemberAssignmentRelease.whenComplete(...)`): `clearAssignment()`,
    /// clear the release-pending flag, and (if a timer reset requested a
    /// rejoin while the release was in flight) `transitionToJoining()`.
    fn continue_after_stale_release(&self) -> Result<(), KafkaError> {
        self.abstract_mm.clear_assignment();

        let rejoin = {
            let mut guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.stale_assignment_release_pending = false;
            let rejoin = guard.stale_rejoin_requested && guard.state == MemberState::Stale;
            guard.stale_rejoin_requested = false;
            rejoin
        };
        if rejoin {
            self.transition_to_joining()?;
        }
        Ok(())
    }

    /// Reconcile the target assignment per §31. Async because it
    /// `.await`s the rebalance-listener oneshot acks.
    ///
    /// Pre-condition: state is `RECONCILING`.
    ///
    /// Behavior mirrors Java's `maybeReconcile(boolean canCommit)` +
    /// `revokeAndAssign(...)` chain, but linearised because we have
    /// `async/await` instead of `CompletableFuture::whenComplete`.
    ///
    /// `can_commit` mirrors Java's parameter: when auto-commit is
    /// enabled and `can_commit` is `false`, the reconciliation must be
    /// skipped because there is no safe opportunity to flush in-progress
    /// offsets before the assignment changes. Java passes `false` from
    /// `AbstractMembershipManager.poll(now)` (the per-iteration
    /// `entries()` walk) and `true` from
    /// `ApplicationEventProcessor.process(AsyncPollEvent)` (the
    /// poll-time entry point, before any new fetching starts — see
    /// Java line 715-718). The poll-time path passes `true` because
    /// any pending offsets can be safely flushed via the commit
    /// manager's auto-commit-before-rebalance path inside this method.
    /// The Rust translation mirrors this through the bg-task call site
    /// (passes `false`) and the `process_async_poll` arm (passes
    /// `true`).
    ///
    /// Java: `maybeReconcile(boolean canCommit)`
    /// (`AbstractMembershipManager.java:824`).
    pub(crate) async fn reconcile(&self, current_time_ms: i64, can_commit: bool) -> Result<(), KafkaError> {
        // Phase 41b: if a reconcile rebalance callback is already in
        // flight, drive it non-blockingly instead of starting a new
        // reconciliation. The loop is NOT frozen on the ack: we `try_recv`
        // the stored receiver and resume the post-callback steps only when
        // the app side has run the listener and sent the ack. While the
        // ack is pending the member stays RECONCILING
        // (`reconciliation_in_progress = true`).
        if self.has_pending_reconcile() {
            return self.drive_pending_reconcile(current_time_ms).await;
        }

        // 1. State / progress checks.
        {
            let guard = match self.abstract_mm.inner.lock() {
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
        let resolved = self.abstract_mm.find_resolvable_assignment_and_trigger_metadata_update();

        // 3. Build the resolved LocalAssignment.
        let mut resolved_partitions: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for (topic_id, _topic_name, partitions) in &resolved {
            resolved_partitions.insert(*topic_id, partitions.clone());
        }
        let target_epoch = {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.current_target_assignment.local_epoch
        };
        let resolved_assignment = LocalAssignment::new(target_epoch, resolved_partitions.clone())?;

        // 4. Short-circuit: if the resolved subset equals the current
        // assignment's partitions, just bump epoch and ACK.
        let short_circuit = {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            !guard.current_assignment.is_none() && resolved_assignment.partitions == guard.current_assignment.partitions
        };
        if short_circuit {
            let mut guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.current_assignment = resolved_assignment;
            guard.transition_to(MemberState::Acknowledging)?;
            return Ok(());
        }

        // 5. Compute `auto_commit_enabled` for the reconciliation gate
        // below. (AK 4.3.1, KAFKA-20106: the gate itself is applied AFTER
        // computing revoked partitions — see step 5b — because it now also
        // depends on whether the reconciliation would revoke anything.)
        let auto_commit_enabled = if self.commit_request_manager.is_some() {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.auto_commit_enabled
        } else {
            false
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
            let subs = match self.abstract_mm.subscriptions.lock() {
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
        self.abstract_mm.mark_reconciliation_in_progress();

        log::info!(
            "Reconciling assignment with local_epoch={}: assigned={:?} added={:?} revoked={:?}",
            resolved_assignment.local_epoch,
            assigned_topic_partitions,
            added,
            revoked
        );

        // 8. Mark partitions pending revocation to stop fetching.
        {
            let mut subs = match self.abstract_mm.subscriptions.lock() {
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
        // (`ConsumerMembershipManager.java:272-279`,
        //  `AbstractMembershipManager.java:894-919`).
        //
        // Commit `subscriptions.allConsumed()` synchronously if
        // auto-commit is enabled. The deadline mirrors Java: the
        // rebalance timeout (configured on this membership manager).
        // Java's `whenComplete` propagates "failure proceeds with
        // revocation anyway" semantics — log + ignore the commit
        // failure here so the rebalance still advances.
        if let Some(commit_mgr) = self.commit_request_manager.as_ref() {
            let rebalance_timeout_ms = self.rebalance_timeout_ms as i64;
            let deadline_ms = current_time_ms.saturating_add(rebalance_timeout_ms);
            let commit_rx = commit_mgr.maybe_auto_commit_sync_before_rebalance(deadline_ms, current_time_ms);
            match commit_rx.await {
                Ok(Ok(())) => {
                    log::debug!("Auto-commit before reconciling new assignment completed successfully.");
                },
                Ok(Err(err)) => {
                    // Java: `log.error("Auto-commit request before reconciling new assignment failed. \
                    // Will proceed with the reconciliation anyway.", commitReqError)`.
                    log::error!(
                        "Auto-commit request before reconciling new assignment failed. \
                         Will proceed with the reconciliation anyway: {err}"
                    );
                },
                Err(_recv_err) => {
                    // Sender dropped — log and proceed.
                    log::error!(
                        "Auto-commit before reconciling new assignment: receiver dropped without completion. \
                         Proceeding with the reconciliation anyway."
                    );
                },
            }
        }

        // 8b. Abort check, immediately after the commit resolves. Java:
        // `commitResult.whenComplete((__, commitReqError) -> { ...;
        // if (!maybeAbortReconciliation()) { revokeAndAssign(...); } })`
        // (`AbstractMembershipManager.java:911`) — the guard runs on BOTH the
        // success and failure paths of the commit, which is why it sits after
        // the match rather than inside its arms.
        //
        // This is the only point where a reconcile suspends while
        // `pending_reconcile` is still `None`: the auto-commit can take up to
        // the whole rebalance timeout, and the receiver is not stored anywhere
        // a concurrent `clear_pending_reconcile()` could find it. So a
        // fence / fatal / stale transition arriving during the commit sees
        // `had_pending == false` and cannot abandon this reconcile — without
        // this check the stale reconcile would go on to enqueue
        // `on_partitions_revoked` after the release path already enqueued
        // `on_partitions_lost` for the same partitions, and the listener's
        // `commit_sync()` would run for a member that is no longer in the
        // group.
        if self.abstract_mm.maybe_abort_reconciliation() {
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
            self.abstract_mm
                .enqueue_rebalance_callback(
                    ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                    revoked_vec,
                    current_time_ms,
                )
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
            None => {
                self.continue_after_revoke(
                    Ok(()),
                    resolved,
                    resolved_assignment,
                    assigned_topic_partitions,
                    added,
                    current_time_ms,
                )
                .await
            },
        }
    }

    /// Java's `reconciliationResult.whenComplete` error arm
    /// (`AbstractMembershipManager.java:958-965`):
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
    fn fail_reconciliation(&self, err: KafkaError) -> KafkaError {
        log::error!("Reconciliation failed: {err}");
        self.abstract_mm.mark_reconciliation_completed();
        err
    }

    /// `true` when a reconcile rebalance callback is awaiting its ack.
    ///
    /// Lock-free: reads the [`Self::pending_reconcile_flag`] atomic only.
    /// This is the steady-state `reconcile` entry check — no mutex lock when
    /// no rebalance is in flight (Perf Contract item 1).
    fn has_pending_reconcile(&self) -> bool {
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
            self.abstract_mm.mark_reconciliation_completed();
        }
    }

    /// Apply the assignment update to the subscription state (AK 4.3.1,
    /// KAFKA-20106). Called from the background task when processing an
    /// [`crate::consumer::internals::events::ApplicationEvent::ApplyAssignment`]
    /// that was triggered by the application thread during `poll()`. This
    /// ensures the assignment update happens on the background thread but is
    /// coordinated by the application thread, so `consumer.assignment()` only
    /// changes within a call to `consumer.poll()`.
    ///
    /// Java: `ConsumerMembershipManager.applyAssignment(assignedPartitions, addedPartitions)`:
    ///   `subscriptions.assignFromSubscribedAwaitingCallback(assignedPartitions, addedPartitions);`
    ///   `notifyAssignmentChange(assignedPartitions);`
    ///
    /// Synchronous (no `.await`) — only mutates `SubscriptionState` and fires
    /// the `notify_assignment_change` listeners. Any error is returned so the
    /// AEP can complete the `ApplyAssignmentEvent` handle exceptionally
    /// (mirroring Java's try/catch → `event.future().completeExceptionally(e)`).
    pub(crate) fn apply_assignment(
        &self,
        assigned_partitions: &HashSet<TopicPartition>,
        added_partitions: &[TopicPartition],
    ) -> Result<(), KafkaError> {
        let assigned_vec: Vec<TopicPartition> = assigned_partitions.iter().cloned().collect();
        {
            let mut subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assign_from_subscribed_awaiting_callback(&assigned_vec, added_partitions)?;
        }
        {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.notify_assignment_change(assigned_partitions);
        }
        Ok(())
    }

    /// Enqueue the release-path `onPartitionsLost` callback (Phase 41,
    /// Issue 2). This is the Rust analog of Java's
    /// `ConsumerMembershipManager.signalPartitionsLost(partitionsLost)`.
    ///
    /// AK 4.3.1 (KAFKA-20321): mark the lost partitions as pending
    /// revocation to stop fetching from them (no new fetches sent out, and
    /// no in-flight fetch responses processed) BEFORE invoking the
    /// `onPartitionsLost` callback. Java:
    ///   `markPendingRevocationToPauseFetching(partitionsLost);`
    ///   `return invokeOnPartitionsLostCallback(partitionsLost);`
    ///
    /// Returns `Ok(None)` when there is nothing to release (no owned
    /// partitions) or no listener is registered — in which case the caller
    /// runs the release tail inline. Otherwise returns the ack
    /// [`oneshot::Receiver`] for the bg loop to drive. (The pending-revocation
    /// marking still happens when there is a non-empty set to release, even
    /// if no listener is registered — it precedes the listener check, exactly
    /// as in Java's `signalPartitionsLost`.)
    fn enqueue_release_callback(
        &self,
        partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<Option<oneshot::Receiver<Result<(), KafkaError>>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(None);
        }
        // AK 4.3.1 (KAFKA-20321): pause fetching for the lost partitions
        // before running the callback.
        {
            let mut subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if let Err(e) = subs.mark_pending_revocation(&partitions) {
                log::warn!("mark_pending_revocation failed for lost partitions: {}", e);
            }
        }
        self.abstract_mm.enqueue_rebalance_callback(
            ConsumerRebalanceListenerMethodName::OnPartitionsLost,
            partitions,
            current_time_ms,
        )
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
    pub(crate) async fn drive_pending_release(&self) -> Result<(), KafkaError> {
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
        let (mut ack_rx, kind, log_ctx): (oneshot::Receiver<Result<(), KafkaError>>, ReleaseKind, &'static str) =
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
                    ReleaseKind::Fenced => self.continue_after_fenced_release(),
                    ReleaseKind::Fatal => {
                        self.continue_after_fatal_release();
                        Ok(())
                    },
                    ReleaseKind::Stale => self.continue_after_stale_release(),
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
                    ReleaseKind::Fenced => self.continue_after_fenced_release(),
                    ReleaseKind::Fatal => {
                        self.continue_after_fatal_release();
                        Ok(())
                    },
                    ReleaseKind::Stale => self.continue_after_stale_release(),
                }
            },
        }
    }

    /// Drives a pending reconcile callback non-blockingly: `try_recv`s the
    /// stored ack receiver and, when it has resolved, resumes the matching
    /// post-callback continuation. While the ack is pending this is a
    /// no-op (the member stays RECONCILING) — Perf Contract item 1.
    ///
    /// `current_time_ms` is the live bg-loop iteration time, used for the
    /// resumed steps (assign-callback enqueue, `reset_auto_commit_timer`),
    /// mirroring Java's `whenComplete` running at completion time.
    async fn drive_pending_reconcile(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // Pop the pending state out under the lock (so the lock is never
        // held across the resumed `.await`), then `try_recv`. If the ack is
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
                                // `revocationResult.completeExceptionally`).
                                self.abstract_mm.mark_reconciliation_completed();
                                return Err(e);
                            },
                        };
                        self.continue_after_revoke(
                            result,
                            resolved,
                            resolved_assignment,
                            assigned_topic_partitions,
                            added,
                            current_time_ms,
                        )
                        .await
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
                        self.abstract_mm.mark_reconciliation_completed();
                        Err(KafkaError::illegal_state(
                            "Rebalance listener ack receiver dropped before completion",
                        ))
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
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.abstract_mm.mark_reconciliation_completed();
                    Err(KafkaError::illegal_state(
                        "Rebalance listener ack receiver dropped before completion",
                    ))
                },
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
    #[allow(clippy::too_many_arguments)]
    async fn continue_after_revoke(
        &self,
        revoke_result: Result<(), KafkaError>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), KafkaError> {
        revoke_result.map_err(|e| self.fail_reconciliation(e))?;

        // 10. Abort check between steps (state may have moved because of a
        // fence / fatal).
        if self.abstract_mm.maybe_abort_reconciliation() {
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
        let ack_rx = self
            .abstract_mm
            .enqueue_partitions_assigned_event(assigned_topic_partitions.clone(), added_vec, current_time_ms)
            .map_err(|e| self.fail_reconciliation(e))?;
        self.store_pending(PendingReconcile::AfterAssign {
            ack_rx,
            resolved,
            resolved_assignment,
            assigned_topic_partitions,
            added,
            current_time_ms,
        });
        Ok(())
    }

    /// Steps 14-16 of `reconcile`, run after the `onPartitionsAssigned`
    /// callback has completed. Synchronous (no `.await`).
    fn continue_after_assign(
        &self,
        assigned_callback_result: Result<(), KafkaError>,
        resolved: Vec<(Uuid, String, Vec<i32>)>,
        resolved_assignment: LocalAssignment,
        assigned_topic_partitions: Vec<TopicPartition>,
        added: HashSet<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), KafkaError> {
        // 14. Enable fetching for assigned partitions (only if the callback
        // succeeded — Java's `subscriptions.enablePartitionsAwaitingCallback`).
        match assigned_callback_result {
            Ok(()) => {
                let mut subs = match self.abstract_mm.subscriptions.lock() {
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
                        let subs = match self.abstract_mm.subscriptions.lock() {
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
                // `reconciliationResult.whenComplete(error, ...)`.
                self.abstract_mm.mark_reconciliation_completed();
                return Err(e);
            },
        }

        // 15. Update local cache of assigned topic names.
        {
            let mut assigned_topic_names: HashSet<String> = HashSet::new();
            for (_id, name, _partitions) in &resolved {
                assigned_topic_names.insert(name.clone());
            }
            let mut guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.assigned_topic_names_cache.retain(|_, v| assigned_topic_names.contains(v));
        }

        // 16. Final abort check; advance to ACKNOWLEDGING and call
        // signalReconciliationCompleting.
        let aborted = self.abstract_mm.maybe_abort_reconciliation();
        if !aborted {
            // The transition result is captured and handled AFTER the guard is
            // dropped: `fail_reconciliation` locks the same `inner` mutex, and
            // `std::sync::Mutex` is not reentrant (CLAUDE.md §9.6), so calling
            // it while holding the guard would self-deadlock on the error path
            // — a path no test would notice until it hung in production.
            let transition = {
                let mut guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.current_assignment = resolved_assignment;
                guard.transition_to(MemberState::Acknowledging)
            };
            transition.map_err(|e| self.fail_reconciliation(e))?;
            // Java: signalReconciliationCompleting() resets the auto-commit
            // timer.
            if let Some(commit_mgr) = self.commit_request_manager.as_ref() {
                commit_mgr.reset_auto_commit_timer(current_time_ms);
            }
            self.abstract_mm.mark_reconciliation_completed();
        }
        Ok(())
    }

    /// Test-only driver mirroring the bg loop's per-iteration
    /// `reconcile(...)` call (Phase 41b). Calls `reconcile` repeatedly,
    /// yielding between iterations so a concurrent test task that receives
    /// the §31 callback event and sends the ack gets to run, until the
    /// reconcile has either left `RECONCILING` or returned an error.
    ///
    /// The non-blocking reconcile no longer drives to completion in a
    /// single `await` (it stores the pending callback and returns); this
    /// helper reproduces the bg loop's repeated drive so component tests
    /// can assert the post-callback state. Bounded so a stuck test fails
    /// fast instead of looping forever.
    #[cfg(test)]
    pub(crate) async fn reconcile_drive_to_completion(&self, can_commit: bool) -> Result<(), KafkaError> {
        for _ in 0..10_000 {
            self.reconcile(0, can_commit).await?;
            if !self.has_pending_reconcile() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(KafkaError::illegal_state(
            "reconcile_drive_to_completion did not settle within the iteration budget",
        ))
    }

    /// Test-only driver mirroring the bg loop's per-iteration Phase 2.4
    /// `drive_pending_release(...)` call (Phase 41, Issue 2). The
    /// non-blocking release transitions store a [`PendingRelease`] and return;
    /// this helper reproduces the bg loop's repeated drive (yielding between
    /// iterations so a concurrent test task that receives the §31
    /// `onPartitionsLost` event and sends the ack gets to run) until the
    /// release tail has run. Bounded so a stuck test fails fast.
    #[cfg(test)]
    pub(crate) async fn drive_release_to_completion(&self) -> Result<(), KafkaError> {
        for _ in 0..10_000 {
            if !self.has_pending_release() {
                return Ok(());
            }
            self.drive_pending_release().await?;
            if !self.has_pending_release() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(KafkaError::illegal_state(
            "drive_release_to_completion did not settle within the iteration budget",
        ))
    }

    /// Java: `isLeavingGroup()` (override).
    pub(crate) fn is_leaving_group(&self) -> bool {
        let leave_op = self.leave_group_operation();
        if matches!(leave_op, GroupMembershipOperation::RemainInGroup) && self.group_instance_id.is_none() {
            return false;
        }
        let state = self.state();
        let is_leaving_state = matches!(state, MemberState::PrepareLeaving | MemberState::Leaving);
        let has_leave_operation = matches!(
            leave_op,
            GroupMembershipOperation::Default | GroupMembershipOperation::LeaveGroup
        ) || self.group_instance_id.is_some();
        is_leaving_state && has_leave_operation
    }

    /// Java: `ConsumerMembershipManager.invokeOnPartitionsRevokedOrLostToReleaseAssignment()`.
    ///
    /// Choose between `onPartitionsRevoked` and `onPartitionsLost`
    /// based on the current member epoch. From Java:
    ///
    /// > If the member is part of the group (epoch > 0), this will
    /// > invoke onPartitionsRevoked. ... If the member is not part of
    /// > the group anymore (epoch <= 0), this will invoke
    /// > onPartitionsLost.
    ///
    /// Translated as `pub(crate)` because the Java method is
    /// `protected`-on-subclass and the test module reaches into it via
    /// the §31 event channel.
    pub(crate) async fn signal_member_leaving_group(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // Snapshot the dropped partitions + epoch under a single short
        // lock; drop both guards before any .await.
        let (dropped_partitions, member_epoch) = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let partitions = subs.assigned_partitions().into_iter().collect::<Vec<_>>();
            drop(subs);
            let inner = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            (partitions, inner.member_epoch)
        };

        log::info!(
            "Member {} is triggering callbacks to release assignment {:?} and leave group",
            self.member_id(),
            dropped_partitions
        );

        if dropped_partitions.is_empty() {
            return Ok(());
        }

        let method = if member_epoch > 0 {
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked
        } else {
            ConsumerRebalanceListenerMethodName::OnPartitionsLost
        };
        self.abstract_mm
            .invoke_rebalance_callback(method, dropped_partitions, current_time_ms)
            .await
    }

    /// Java: `AbstractMembershipManager.leaveGroupOnClose(GroupMembershipOperation)`.
    /// Invoked by `Consumer::close(...)`. Records the membership
    /// operation and delegates to `leave_group(run_callbacks=false)`.
    pub(crate) async fn leave_group_on_close(
        &self,
        membership_operation: GroupMembershipOperation,
        current_time_ms: i64,
    ) -> Result<(), KafkaError> {
        self.set_leave_group_operation(membership_operation);
        self.leave_group_inner(false, current_time_ms).await
    }

    /// Java: `AbstractMembershipManager.leaveGroup()`. Invoked by
    /// `Consumer::unsubscribe()`. Runs callbacks during the leave.
    pub(crate) async fn leave_group(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        self.leave_group_inner(true, current_time_ms).await
    }

    /// Java: `AbstractMembershipManager.leaveGroup(boolean runCallbacks)`.
    /// The shared body of `leave_group` / `leave_group_on_close`.
    ///
    /// Mirrors Java's flow phase-for-phase:
    ///
    /// 1. If already out of group: clear & unsubscribe (no heartbeat
    ///    needed). `FENCED` is reset to `UNSUBSCRIBED` after clearing
    ///    the assignment.
    /// 2. If already leaving: no-op.
    /// 3. Transition to `PREPARE_LEAVING`. If `run_callbacks=true`,
    ///    invoke the rebalance-listener handshake via
    ///    [`signal_member_leaving_group`]; listener errors are logged
    ///    but the leave proceeds regardless (Java's `whenComplete`
    ///    semantics).
    /// 4. Unsubscribe + clear assignment.
    /// 5. Transition to `LEAVING` so the next heartbeat sends the
    ///    leave-group request.
    async fn leave_group_inner(&self, run_callbacks: bool, current_time_ms: i64) -> Result<(), KafkaError> {
        // Step 1: already-out-of-group fast path.
        let pre_state = {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.state
        };
        if matches!(
            pre_state,
            MemberState::Unsubscribed | MemberState::Fenced | MemberState::Fatal | MemberState::Stale
        ) {
            if pre_state == MemberState::Fenced {
                self.abstract_mm.clear_assignment();
                let mut guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.transition_to(MemberState::Unsubscribed)?;
            }
            {
                let mut subs = match self.abstract_mm.subscriptions.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                subs.unsubscribe();
            }
            {
                let guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.notify_assignment_change(&std::collections::HashSet::new());
            }
            return Ok(());
        }

        // Step 2: already-leaving short-circuit. Java returns the
        // existing in-flight future; our async equivalent is a no-op
        // because the in-flight `leave_group` call will return when
        // the rebalance completes.
        if matches!(pre_state, MemberState::PrepareLeaving | MemberState::Leaving) {
            log::debug!("Leave group operation already in progress for member {}", self.member_id());
            return Ok(());
        }

        // Step 3: PREPARE_LEAVING + optional rebalance-callback step.
        {
            let mut guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.transition_to(MemberState::PrepareLeaving)?;
        }

        if run_callbacks && let Err(e) = self.signal_member_leaving_group(current_time_ms).await {
            log::error!(
                "Member {} callback to release assignment failed. It will proceed to clear its \
                 assignment and send a leave group heartbeat: {}",
                self.member_id(),
                e
            );
        }

        // Step 4 + 5: clearAssignmentAndLeaveGroup().
        {
            let mut subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.unsubscribe();
        }
        self.abstract_mm.clear_assignment();
        self.transition_to_sending_leave_group(false)?;
        Ok(())
    }

    /// Java: `onHeartbeatFailure(boolean retriable)`.
    pub(crate) fn on_heartbeat_failure(&self, retriable: bool) {
        let was_unsubscribed = self.abstract_mm.on_heartbeat_failure(retriable);
        if was_unsubscribed {
            log::warn!(
                "Member with epoch {} received a failed response to the heartbeat to leave the group.",
                self.member_epoch()
            );
        }
    }
}

/// `RequestManager` impl. `poll` performs a best-effort metadata
/// resolution but does NOT drive reconcile from here (reconcile is
/// async; the bg task will drive it via a dedicated path).
///
/// Java: `AbstractMembershipManager.poll(long currentTimeMs)` calls
/// `maybeReconcile(false)` and returns `EMPTY`. Our split: the bg task
/// (Phase 10) is responsible for `.await`ing `reconcile()` — `poll` here
/// is a sync hook that just returns the empty result, mirroring Java's
/// "it never returns requests to send itself" semantics.
impl RequestManager for ConsumerMembershipManager {
    fn poll(&mut self, _current_time_ms: i64) -> PollResult {
        // Sync poll: cannot await reconcile here. Phase 10 wires the
        // bg task to invoke `reconcile().await` when state is
        // RECONCILING.
        PollResult::empty()
    }
}

// Compatibility: produce a friendly debug string for log lines.
impl std::fmt::Debug for ConsumerMembershipManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerMembershipManager")
            .field("group_id", &self.group_id())
            .field("member_id", &self.member_id())
            .field("member_epoch", &self.member_epoch())
            .field("state", &self.state())
            .field("group_instance_id", &self.group_instance_id)
            .field("server_assignor", &self.server_assignor)
            .finish()
    }
}

/// Translation notes on Java test coverage
/// (`ConsumerMembershipManagerTest`, 84 `@Test` cases). The original
/// "26 / 93" count below predates Phases 34 + 35; actual coverage is now
/// ~52 / 84 behaviorally covered. The list below enumerates the Phase-8b
/// core; Phase 34 added the metadata-driven reconcile / delayed-discard /
/// leave-and-fatal-matrix / listener-ordering families, and Phase 35 added
/// the STALE-member family:
///   - `transition_to_leaving_while_{reconciling,joining,stable,acknowledging}_due_to_stale_member`
///     — Java `testTransitionToLeavingWhile*DueToStaleMember`
///   - `stale_member_does_not_send_heartbeat_and_allows_transition_to_joining_to_recover`
///     — Java `testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover`
///   - `stale_member_rejoins_when_timer_resets_no_callbacks`
///     — Java `testStaleMemberRejoinsWhenTimerResetsNoCallbacks`
///   - `stale_member_waits_for_callback_to_rejoin_when_timer_reset`
///     — Java `testStaleMemberWaitsForCallbackToRejoinWhenTimerReset`
///   - `leave_group_when_member_is_stale` — Java `testLeaveGroupWhenMemberIsStale`
///
/// Remaining gaps: the RebalanceMetrics family (OUT_OF_SCOPE — no metrics
/// framework) and a handful of Mockito-spy-only verifications (covered by
/// state-transition assertions in the translated tests).
///
/// Translated (Phase-8b core):
/// - `server_assignor_accessor` — Java: `testMembershipManagerServerAssignor`
/// - `rack_id_accessor` — Java: `testMembershipManagerRackId`
/// - `init_supports_empty_group_instance_id` — Java: `testMembershipManagerInitSupportsEmptyGroupInstanceId`
/// - `transition_to_joining_from_unsubscribed` — Java: `testTransitionToJoiningOnlyIfSubscriptionUpdated` (shape)
/// - `leave_group_epoch_dynamic_member` — Java: part of `testLeaveGroupEpoch`
/// - `leave_group_epoch_static_member` — Java: part of `testLeaveGroupEpoch`
/// - `leave_group_epoch_static_member_force_leave` — Java: part of `testLeaveGroupEpochOnClose`
/// - `is_leaving_group_dynamic_remain_in_group_is_false` — Java: `testIsLeavingGroup` shape
/// - `is_leaving_group_static_remain_in_group_is_true` — Java: `testIsLeavingGroup` shape
/// - `reconcile_emits_assigned_callback_and_acks` — §31 handshake regression
/// - `listeners_notified_only_on_epoch_change` — Java: `testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges`
/// - `on_heartbeat_request_generated_acknowledging_to_stable` — Java: `testReconcilingWhenReceivingAssignmentFoundInMetadata` (post-ack)
/// - `on_heartbeat_success_empty_assignment_transitions_to_reconciling` — Java: `testTransitionToReconcilingIfEmptyAssignmentReceived`
/// - `transition_to_failed_when_trying_to_join` — Java: `testTransitionToFailedWhenTryingToJoin`
/// - `member_id_and_epoch_reset_on_fenced_members` — Java: `testMemberIdAndEpochResetOnFencedMembers`
/// - `fencing_when_state_is_stable` — Java: `testFencingWhenStateIsStable`
/// - `fencing_when_state_is_reconciling` — Java: `testFencingWhenStateIsReconciling`
/// - `fencing_when_state_is_prepare_leaving` — Java: `testFencingWhenStateIsPrepareLeaving`
/// - `fencing_when_state_is_leaving` — Java: `testFencingWhenStateIsLeaving`
/// - `listeners_get_notified_on_transitions_to_fatal` — Java: `testListenersGetNotifiedOnTransitionsToFatal`
/// - `listeners_get_notified_on_transitions_to_leaving_group` — Java: `testListenersGetNotifiedOnTransitionsToLeavingGroup`
/// - `new_assignment_ignored_when_state_is_prepare_leaving` — Java: `testNewAssignmentIgnoredWhenStateIsPrepareLeaving`
/// - `same_assignment_reconciled_again_when_fenced` — Java: `testSameAssignmentReconciledAgainWhenFenced`
/// - `leave_group_epoch_test` — Java: `testLeaveGroupEpoch`
/// - `leave_group_epoch_on_close` — Java: `testLeaveGroupEpochOnClose`
/// - `reconcile_propagates_assigned_listener_error` (new — regression for fix #2)
///
/// Not translated (~32 / 84) — rationale categories:
///
/// 1. **Mockito-spy verification on internal methods** (~28 cases).
///    Java tests use `verify(membershipManager, never()).markReconciliationInProgress()`,
///    `verify(membershipManager).notifyEpochChange(Optional.empty())`,
///    etc. Rust has no equivalent for mocking on a concrete struct
///    (mockall requires the type to be a trait). These tests verify
///    internal bookkeeping side-effects on the membership manager
///    rather than externally-observable state. The same behavior is
///    covered by state-transition assertions in the translated tests
///    where Java asserts `assertEquals(STATE_X, mgr.state())`
///    alongside the `verify(mgr).foo()`. Affected:
///    `testReconcileNewAssignment*`, `testMarkReconciliationInProgress*`,
///    `testNotifyEpochChangeOn*`,
///    `testFencingWhenStateIsPrepareLeavingCompletesTheLeaveOperation`,
///    `testTransitionToFatalWhileReconciling`, the
///    `testReconcileWithMissingMetadataReceivesMetadataUpdate` family,
///    and the `testCommit*BeforeRebalance*` family.
///
/// 2. **Commit-request-manager auto-commit interaction** (~12 cases):
///    `testCommitOffsetsBeforeRebalance*`, `testAutoCommitBeforeRebalance*`,
///    `testCommitErrorDoesNotBlockReconcile*`. The
///    `CommitRequestManager::maybe_auto_commit_sync_before_rebalance`
///    method itself lives on the commit manager (Phase 10, commit 2.5/N),
///    and the call site inside `reconcile` was wired in Phase 11
///    commit (7/N) (step 8a, between `markPendingRevocation` and the
///    `onPartitionsRevoked` callback dispatch). Behavioural translation
///    of these tests requires end-to-end test infrastructure
///    (`MockClient` driving a real bg task with a real commit manager)
///    that lives in `tests/consumer/async_kafka_consumer_test.rs` —
///    deferred to Phase 11 commits (8-10).
///
/// 3. **Streams / Share manager** (~6 cases): `testStreams*`,
///    `testShare*`. Out of scope per `consumer-threading.md` §20.
///
/// 4. **CompletableFuture-chain-shape verification** (~10 cases):
///    Tests that build a `CompletableFuture<Void>` chain via
///    `leaveGroup()` and assert chain shape (completion order,
///    exceptionally-completion). The Rust async path does not expose a
///    chain object; chain ordering is verified by our reconcile +
///    leave_group tests via state assertions instead.
///
/// 5. **Time / metric assertions** (~8 cases): tests using the Java
///    `MockTime` advance + `RebalanceMetricsManager` verification. The
///    metric VALUE behavior is now covered exhaustively by the dedicated
///    `ConsumerRebalanceMetricsManagerTest` translation (8 cases) in
///    `consumer_rebalance_metrics_manager.rs` (Phase M5). The membership
///    *wiring* (that `transition_to` / `on_heartbeat_failure` actually drive
///    those records) is covered here by
///    `transition_to_reconciling_and_back_records_rebalance_metrics` and
///    `non_retriable_heartbeat_failure_records_failed_rebalance`.
///
/// 6. **Reconcile-with-real-metadata** (~3 cases):
///    `testReconcileNewAssignmentReplacesPreviousAssignmentWithEmptyResults`
///    and variants — depend on a populated `ConsumerMetadata` cache
///    that the test would normally seed via Mockito. The Rust
///    equivalent requires building a real `MetadataResponse` and
///    feeding it through `ConsumerMetadata::update`; the surface
///    needed lands in Phase 10 wiring.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::ConsumerRebalanceListener;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use async_trait::async_trait;
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    /// Test-only no-op rebalance listener. Registered by default in
    /// `make()` so the §31 listener-presence short-circuit (introduced
    /// per COMMENTS.1.md fix #1) does not silently drop events.
    struct NoopListener;
    #[async_trait]
    impl ConsumerRebalanceListener for NoopListener {
        async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Ok(())
        }
        async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Ok(())
        }
    }

    fn make(
        group_instance_id: Option<String>,
        server_assignor: Option<String>,
        rack_id: Option<String>,
    ) -> (
        ConsumerMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        make_inner(group_instance_id, server_assignor, rack_id, true)
    }

    /// Variant of [`make`] that omits the rebalance listener — used to
    /// exercise the §31 short-circuit path.
    fn make_without_listener(
        group_instance_id: Option<String>,
        server_assignor: Option<String>,
        rack_id: Option<String>,
    ) -> (
        ConsumerMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        make_inner(group_instance_id, server_assignor, rack_id, false)
    }

    fn make_inner(
        group_instance_id: Option<String>,
        server_assignor: Option<String>,
        rack_id: Option<String>,
        with_listener: bool,
    ) -> (
        ConsumerMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        if with_listener {
            subs.lock()
                .unwrap()
                .subscribe_topics(HashSet::new(), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let mgr = ConsumerMembershipManager::new(
            "test-group",
            group_instance_id,
            rack_id,
            100,
            server_assignor,
            subs,
            None,
            metadata,
            beh,
            true,
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
        );
        (mgr, rx)
    }

    /// Build a membership manager wired with a REAL
    /// `ConsumerRebalanceMetricsManager` over a metrics `MockTime`, returning
    /// the manager, the metrics registry, the metrics manager (to read its
    /// `#[cfg(test)]` MetricName fields), and the clock. Used by the M5 wiring
    /// tests that assert `transition_to` / `on_heartbeat_failure` drive the
    /// rebalance records.
    fn make_with_rebalance_metrics() -> (
        ConsumerMembershipManager,
        Arc<crate::common::metrics::Metrics>,
        Arc<ConsumerRebalanceMetricsManager>,
        Arc<crate::common::metrics::time::mock::MockTime>,
    ) {
        use crate::common::metrics::time::mock::MockTime;
        use crate::common::metrics::{Metrics, Time as MetricsTime};

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn MetricsTime>));
        let metrics_manager = Arc::new(ConsumerRebalanceMetricsManager::new(&metrics, Arc::clone(&subs)));
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let mgr = ConsumerMembershipManager::new(
            "test-group",
            None,
            None,
            100,
            None,
            subs,
            None,
            metadata,
            beh,
            true,
            Some(Arc::clone(&metrics_manager)),
            Arc::clone(&time) as Arc<dyn MetricsTime>,
        );
        (mgr, metrics, metrics_manager, time)
    }

    /// M5 wiring: a RECONCILING-and-back transition drives
    /// `record_rebalance_started` / `record_rebalance_ended` through the
    /// membership state machine (`AbstractMembershipManager.transitionTo`),
    /// so the rebalance-latency/total metrics reflect the elapsed time.
    #[test]
    fn transition_to_reconciling_and_back_records_rebalance_metrics() {
        use crate::common::metric::Metric;
        use crate::common::metrics::Time as MetricsTime;

        let (mgr, metrics, metrics_manager, time) = make_with_rebalance_metrics();
        let value =
            |name: &crate::common::MetricName| metrics.metric(name).unwrap().metric_value().as_double().unwrap();

        // STABLE -> RECONCILING starts the rebalance; +25ms; -> STABLE ends it.
        {
            let mut inner = mgr.abstract_mm.inner.lock().unwrap();
            inner.state = MemberState::Stable;
            inner.transition_to(MemberState::Reconciling).unwrap();
        }
        assert!(metrics_manager.rebalance_started(), "rebalance recorded as started");
        time.sleep(25);
        {
            let mut inner = mgr.abstract_mm.inner.lock().unwrap();
            inner.transition_to(MemberState::Stable).unwrap();
        }
        assert!(!metrics_manager.rebalance_started(), "rebalance ended");

        assert_eq!(25.0, value(&metrics_manager.rebalance_latency_avg));
        assert_eq!(25.0, value(&metrics_manager.rebalance_latency_max));
        assert_eq!(25.0, value(&metrics_manager.rebalance_latency_total));
        assert_eq!(1.0, value(&metrics_manager.rebalance_total));
        // No failures recorded on a clean cycle.
        assert_eq!(0.0, value(&metrics_manager.failed_rebalance_total));
        let _ = time.milliseconds(); // keep the clock import used
    }

    /// M5 wiring: a non-retriable heartbeat failure during an in-progress
    /// rebalance records a failed rebalance
    /// (`AbstractMembershipManager.onHeartbeatFailure`), while a retriable
    /// failure does not.
    #[test]
    fn non_retriable_heartbeat_failure_records_failed_rebalance() {
        use crate::common::metric::Metric;

        let (mgr, metrics, metrics_manager, _time) = make_with_rebalance_metrics();
        let value =
            |name: &crate::common::MetricName| metrics.metric(name).unwrap().metric_value().as_double().unwrap();

        // Start a rebalance (so a failure counts).
        {
            let mut inner = mgr.abstract_mm.inner.lock().unwrap();
            inner.state = MemberState::Stable;
            inner.transition_to(MemberState::Reconciling).unwrap();
        }

        // Retriable failure: NOT recorded.
        mgr.abstract_mm.on_heartbeat_failure(true);
        assert_eq!(0.0, value(&metrics_manager.failed_rebalance_total), "retriable not recorded");

        // Non-retriable failure with a rebalance in progress: recorded.
        mgr.abstract_mm.on_heartbeat_failure(false);
        assert_eq!(1.0, value(&metrics_manager.failed_rebalance_total), "non-retriable recorded");
    }

    /// Helper that constructs a manager carrying a real
    /// `CommitRequestManager` and `auto_commit_enabled=true` — required
    /// to exercise Java's `if (autoCommitEnabled && !canCommit) return;`
    /// gate inside [`ConsumerMembershipManager::reconcile`].
    fn make_with_commit_manager(
        with_listener: bool,
    ) -> (
        ConsumerMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        if with_listener {
            subs.lock()
                .unwrap()
                .subscribe_topics(HashSet::new(), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let commit_mgr = Arc::new(crate::consumer::internals::commit_request_manager::CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "test-group",
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
            0,
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let mgr = ConsumerMembershipManager::new(
            "test-group",
            None,
            None,
            100,
            None,
            subs,
            Some(commit_mgr),
            metadata,
            beh,
            true, // auto_commit_enabled
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
        );
        (mgr, rx)
    }

    /// Translated from `ConsumerMembershipManagerTest#testMembershipManagerServerAssignor`.
    #[test]
    fn server_assignor_accessor() {
        let (mgr, _rx) = make(None, None, None);
        assert_eq!(mgr.server_assignor(), None);

        let (mgr2, _rx2) = make(Some("instance1".to_string()), Some("Uniform".to_string()), None);
        assert_eq!(mgr2.server_assignor(), Some("Uniform"));
    }

    /// Translated from `ConsumerMembershipManagerTest#testMembershipManagerRackId`.
    #[test]
    fn rack_id_accessor() {
        let (mgr, _rx) = make(None, None, None);
        assert_eq!(mgr.rack_id(), None);

        let (mgr2, _rx2) = make(None, None, Some("rack1".to_string()));
        assert_eq!(mgr2.rack_id(), Some("rack1"));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testMembershipManagerInitSupportsEmptyGroupInstanceId`.
    #[test]
    fn init_supports_empty_group_instance_id() {
        let (mgr, _rx) = make(None, None, None);
        assert_eq!(mgr.group_instance_id(), None);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToJoiningOnlyIfSubscriptionUpdated`-shape.
    /// We confirm `transition_to_joining` from `UNSUBSCRIBED` works.
    #[test]
    fn transition_to_joining_from_unsubscribed() {
        let (mgr, _rx) = make(None, None, None);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.transition_to_joining().unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        assert_eq!(mgr.member_epoch(), 0);
    }

    /// `leave_group_epoch` returns -1 for dynamic members.
    #[test]
    fn leave_group_epoch_dynamic_member() {
        let (mgr, _rx) = make(None, None, None);
        assert_eq!(mgr.leave_group_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
    }

    /// `leave_group_epoch` returns -2 for static members (default
    /// operation).
    #[test]
    fn leave_group_epoch_static_member() {
        let (mgr, _rx) = make(Some("static-1".to_string()), None, None);
        assert_eq!(mgr.leave_group_epoch(), LEAVE_GROUP_STATIC_MEMBER_EPOCH);
    }

    /// `leave_group_epoch` returns -1 for static members when
    /// LEAVE_GROUP is explicitly set (force-fence path).
    #[test]
    fn leave_group_epoch_static_member_force_leave() {
        let (mgr, _rx) = make(Some("static-1".to_string()), None, None);
        mgr.set_leave_group_operation(GroupMembershipOperation::LeaveGroup);
        assert_eq!(mgr.leave_group_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
    }

    // Helper: force the manager into PREPARE_LEAVING so the
    // transition to LEAVING (via `transition_to_sending_leave_group`)
    // is valid per `MemberState::previous_valid_states`.
    fn force_into_prepare_leaving(mgr: &ConsumerMembershipManager) {
        mgr.transition_to_joining().unwrap();
        let mut guard = mgr.abstract_mm.inner.lock().unwrap();
        guard.transition_to(MemberState::PrepareLeaving).unwrap();
    }

    /// `is_leaving_group` returns false for a dynamic member with
    /// REMAIN_IN_GROUP operation, regardless of state.
    #[test]
    fn is_leaving_group_dynamic_remain_in_group_is_false() {
        let (mgr, _rx) = make(None, None, None);
        mgr.set_leave_group_operation(GroupMembershipOperation::RemainInGroup);
        force_into_prepare_leaving(&mgr);
        mgr.transition_to_sending_leave_group(false).unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(!mgr.is_leaving_group());
    }

    /// `is_leaving_group` returns true for a static member with
    /// REMAIN_IN_GROUP (static members still send the leave heartbeat).
    #[test]
    fn is_leaving_group_static_remain_in_group_is_true() {
        let (mgr, _rx) = make(Some("static-1".to_string()), None, None);
        mgr.set_leave_group_operation(GroupMembershipOperation::RemainInGroup);
        force_into_prepare_leaving(&mgr);
        mgr.transition_to_sending_leave_group(false).unwrap();
        assert!(mgr.is_leaving_group());
    }

    /// §31 reconcile correctness: enqueueing partitions and acking
    /// drives the state machine to ACKNOWLEDGING.
    #[tokio::test]
    async fn reconcile_emits_assigned_callback_and_acks() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();

        // Build a target assignment with a single resolvable topic via
        // a custom partial-resolve path: we don't have metadata, so we
        // pre-fill the local cache so resolution succeeds without
        // metadata access.
        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }
        let mut new_assignment = HashMap::new();
        new_assignment.insert(topic_id, vec![0, 1]);
        mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr_arc = Arc::new(mgr);
        let mgr_clone = mgr_arc.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        // App-side: expect PartitionsAssigned event (no revoked partitions
        // because we had none). Simulate applying the assignment, then ack.
        let env = rx.recv().await.expect("event");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(added_partitions.len(), 2);
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr_arc.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {:?}", other),
        }

        bg.await.unwrap().unwrap();
        assert_eq!(mgr_arc.state(), MemberState::Acknowledging);
    }

    /// Phase 41b / §31 required test #2 at the membership-state-machine
    /// level: the rebalance does NOT advance out of `RECONCILING` until the
    /// listener ack arrives, AND the bg-loop drive (`reconcile`) is NOT
    /// frozen while the ack is pending — repeated `reconcile` calls return
    /// promptly (each is a non-blocking `try_recv`), leaving the state in
    /// `RECONCILING`, until the ack is delivered.
    #[tokio::test]
    async fn reconcile_does_not_advance_until_ack_and_loop_is_not_frozen() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();

        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }
        let mut new_assignment = HashMap::new();
        new_assignment.insert(topic_id, vec![0, 1]);
        mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // First reconcile enqueues PartitionsAssigned and stores the
        // pending ack — it does NOT block.
        mgr.reconcile(0, true).await.unwrap();
        let env = rx.recv().await.expect("event");
        let (ack, assigned_partitions, added_partitions) = match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                (ack, assigned_partitions, added_partitions)
            },
            other => panic!("unexpected event: {:?}", other),
        };

        // Drive the loop several more times while the ack is still
        // pending: each call returns promptly (NOT frozen) and the state
        // stays RECONCILING (does NOT advance).
        for _ in 0..5 {
            mgr.reconcile(0, true).await.unwrap();
            assert_eq!(
                mgr.state(),
                MemberState::Reconciling,
                "state must NOT advance before the listener ack arrives (§31)"
            );
        }

        // Deliver the ack (after simulating the app applying the
        // assignment); the next reconcile drives the continuation and the
        // state advances to ACKNOWLEDGING.
        let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
        mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
        ack.send(Ok(())).unwrap();
        mgr.reconcile(0, true).await.unwrap();
        assert_eq!(
            mgr.state(),
            MemberState::Acknowledging,
            "state must advance once the listener ack arrives"
        );
    }

    /// Regression for COMMENTS R2-2: `reconcile(now, can_commit=false)`
    /// is a no-op when auto-commit is enabled AND a commit manager is
    /// present, mirroring Java's
    /// `if (autoCommitEnabled && !canCommit) return;` at
    /// `AbstractMembershipManager.java:854`.
    ///
    /// Path: state is `Reconciling` with a real target assignment that
    /// would otherwise emit an `OnPartitionsAssigned` background event;
    /// calling `reconcile(_, false)` MUST NOT emit that event and MUST
    /// NOT transition out of `Reconciling`. Calling
    /// `reconcile(_, true)` from the same setup DOES proceed
    /// (separate test below).
    #[tokio::test]
    async fn reconcile_can_commit_false_is_noop_when_auto_commit_enabled() {
        let (mgr, mut rx) = make_with_commit_manager(true);
        mgr.transition_to_joining().unwrap();

        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }
        let mut new_assignment = HashMap::new();
        new_assignment.insert(topic_id, vec![0]);
        mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // can_commit=false MUST short-circuit (auto-commit gate).
        mgr.reconcile(0, false).await.unwrap();

        // No callback event was emitted, and the state stayed in
        // RECONCILING.
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "no rebalance-listener event should be emitted when can_commit=false gate triggers",
        );
        assert_eq!(
            mgr.state(),
            MemberState::Reconciling,
            "reconciliation must not have advanced when can_commit=false (auto-commit gate active)",
        );
    }

    /// Companion to [`reconcile_can_commit_false_is_noop_when_auto_commit_enabled`].
    /// Same setup with `can_commit=true` proceeds — callback event is
    /// emitted and state advances after ack.
    #[tokio::test]
    async fn reconcile_can_commit_true_proceeds_when_auto_commit_enabled() {
        let (mgr, mut rx) = make_with_commit_manager(true);
        mgr.transition_to_joining().unwrap();

        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }
        let mut new_assignment = HashMap::new();
        new_assignment.insert(topic_id, vec![0]);
        mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr_arc = Arc::new(mgr);
        let mgr_clone = mgr_arc.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        let env = rx.recv().await.expect("event");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(added_partitions.len(), 1);
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr_arc.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {:?}", other),
        }
        bg.await.unwrap().unwrap();
        assert_eq!(mgr_arc.state(), MemberState::Acknowledging);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges`.
    /// Verifies that registered listeners only see epoch-update calls
    /// when the epoch actually changed.
    #[test]
    fn listeners_notified_only_on_epoch_change() {
        use crate::consumer::internals::member_state_listener::MemberStateListener;
        use std::sync::Mutex as StdMutex;

        #[derive(Default)]
        struct Recorder {
            calls: StdMutex<Vec<(Option<i32>, String)>>,
        }
        impl MemberStateListener for Recorder {
            fn on_member_epoch_updated(&self, epoch: Option<i32>, member_id: &str) {
                self.calls.lock().unwrap().push((epoch, member_id.to_string()));
            }
        }

        let (mgr, _rx) = make(None, None, None);
        let listener = Arc::new(Recorder::default());
        mgr.abstract_mm.register_state_listener(listener.clone());
        mgr.transition_to_joining().unwrap();

        // Apply epoch=5 via update_member_epoch — listeners should fire.
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(5);
        }
        let calls = listener.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, Some(5));
        drop(calls);

        // Re-applying the same epoch should NOT fire again.
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(5);
        }
        assert_eq!(listener.calls.lock().unwrap().len(), 1);

        // Changing the epoch fires again.
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(6);
        }
        assert_eq!(listener.calls.lock().unwrap().len(), 2);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcilingWhenReceivingAssignmentFoundInMetadata`
    /// (the post-ack STABLE transition specifically).
    #[tokio::test]
    async fn on_heartbeat_request_generated_acknowledging_to_stable() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();

        // Pre-fill topic cache so reconcile succeeds.
        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }
        let mut new_assignment = HashMap::new();
        new_assignment.insert(topic_id, vec![0]);
        mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();

        let mgr_arc = Arc::new(mgr);
        let mgr_clone = mgr_arc.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        let env = rx.recv().await.expect("event");
        if let BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } = env.event {
            let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
            mgr_arc.apply_assignment(&assigned_set, &added_partitions).unwrap();
            ack.send(Ok(())).unwrap();
        }
        bg.await.unwrap().unwrap();
        assert_eq!(mgr_arc.state(), MemberState::Acknowledging);

        // When the ack heartbeat is sent the member should go back to
        // STABLE (target == current).
        mgr_arc.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr_arc.state(), MemberState::Stable);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToFailedWhenTryingToJoin`.
    /// Transitioning to FATAL while JOINING is valid and parks the
    /// member in FATAL.
    #[tokio::test]
    async fn transition_to_failed_when_trying_to_join() {
        let (mgr, _rx) = make(None, None, None);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.transition_to_joining().unwrap();
        // No assigned partitions, so transition_to_fatal short-circuits
        // the §31 handshake (empty partitions branch).
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testMemberIdAndEpochResetOnFencedMembers`.
    /// Transitioning to FENCED resets the member epoch to 0 (the
    /// join-group epoch).
    #[tokio::test]
    async fn member_id_and_epoch_reset_on_fenced_members() {
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        // Apply an epoch via on_heartbeat_success to mimic STABLE.
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(7);
        }
        let original_member_id = mgr.member_id();
        assert!(!original_member_id.is_empty());
        assert_eq!(mgr.member_epoch(), 7);

        mgr.transition_to_fenced(0).unwrap();
        // After fencing the member should rejoin (state=JOINING) with
        // epoch reset to JOIN_GROUP_MEMBER_EPOCH (0).
        assert_eq!(mgr.member_epoch(), 0);
        // Java: memberId is NOT cleared on fence — only the epoch.
        assert_eq!(mgr.member_id(), original_member_id);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFencingWhenStateIsStable`.
    /// A STABLE member that gets fenced transitions to JOINING after
    /// the §31 release handshake completes.
    #[tokio::test]
    async fn fencing_when_state_is_stable() {
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.transition_to(MemberState::Reconciling).unwrap();
            guard.transition_to(MemberState::Acknowledging).unwrap();
            guard.transition_to(MemberState::Stable).unwrap();
        }
        assert_eq!(mgr.state(), MemberState::Stable);

        // No assigned partitions in subscriptions -> §31 short-circuit.
        mgr.transition_to_fenced(0).unwrap();
        // Fenced -> JOINING (rejoin) when assignment is empty.
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFencingWhenStateIsReconciling`.
    /// A RECONCILING member that gets fenced transitions to JOINING.
    #[tokio::test]
    async fn fencing_when_state_is_reconciling() {
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.transition_to(MemberState::Reconciling).unwrap();
        }
        assert_eq!(mgr.state(), MemberState::Reconciling);
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFencingWhenStateIsPrepareLeaving`.
    /// A PREPARE_LEAVING member that gets fenced goes through LEAVING
    /// to UNSUBSCRIBED (no callback, no rejoin) per
    /// `transition_to_fenced`'s short-circuit.
    #[tokio::test]
    async fn fencing_when_state_is_prepare_leaving() {
        let (mgr, _rx) = make(None, None, None);
        force_into_prepare_leaving(&mgr);
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
        mgr.transition_to_fenced(0).unwrap();
        // The Java contract puts us in UNSUBSCRIBED after the
        // PREPARE_LEAVING -> LEAVING -> UNSUBSCRIBED dance.
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(mgr.abstract_mm.inner.lock().unwrap().should_skip_heartbeat());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFencingWhenStateIsLeaving`.
    /// A LEAVING member that gets fenced transitions to UNSUBSCRIBED
    /// (no callback, no last HB).
    #[tokio::test]
    async fn fencing_when_state_is_leaving() {
        let (mgr, _rx) = make(None, None, None);
        force_into_prepare_leaving(&mgr);
        mgr.transition_to_sending_leave_group(false).unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testListenersGetNotifiedOnTransitionsToFatal`.
    /// `MemberStateListener::on_member_epoch_updated` is invoked with
    /// `None` when the manager transitions to FATAL.
    #[tokio::test]
    async fn listeners_get_notified_on_transitions_to_fatal() {
        use crate::consumer::internals::member_state_listener::MemberStateListener;
        use std::sync::Mutex as StdMutex;
        #[derive(Default)]
        struct Recorder {
            calls: StdMutex<Vec<Option<i32>>>,
        }
        impl MemberStateListener for Recorder {
            fn on_member_epoch_updated(&self, epoch: Option<i32>, _member_id: &str) {
                self.calls.lock().unwrap().push(epoch);
            }
        }

        let (mgr, _rx) = make(None, None, None);
        let listener = Arc::new(Recorder::default());
        mgr.abstract_mm.register_state_listener(listener.clone());
        mgr.transition_to_joining().unwrap();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(5);
        }
        // Clear initial notifications.
        listener.calls.lock().unwrap().clear();

        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
        let calls = listener.calls.lock().unwrap();
        // FATAL transition emits `notify_epoch_change(None)`.
        assert!(calls.contains(&None));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testListenersGetNotifiedOnTransitionsToLeavingGroup`.
    /// `leave_group()` clears the member's epoch and notifies listeners
    /// with `None` via the leave-epoch path.
    #[tokio::test]
    async fn listeners_get_notified_on_transitions_to_leaving_group() {
        use crate::consumer::internals::member_state_listener::MemberStateListener;
        use std::sync::Mutex as StdMutex;
        #[derive(Default)]
        struct Recorder {
            calls: StdMutex<Vec<Option<i32>>>,
        }
        impl MemberStateListener for Recorder {
            fn on_member_epoch_updated(&self, epoch: Option<i32>, _member_id: &str) {
                self.calls.lock().unwrap().push(epoch);
            }
        }

        let (mgr, _rx) = make(None, None, None);
        let listener = Arc::new(Recorder::default());
        mgr.abstract_mm.register_state_listener(listener.clone());
        mgr.transition_to_joining().unwrap();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(5);
        }
        listener.calls.lock().unwrap().clear();

        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        let calls = listener.calls.lock().unwrap();
        // The leave_group_epoch (LEAVE_GROUP_MEMBER_EPOCH=-1) was
        // negative so notify_epoch_change is invoked with None.
        assert!(calls.contains(&None));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testNewAssignmentIgnoredWhenStateIsPrepareLeaving`.
    /// Receiving a new assignment while in PREPARE_LEAVING does not
    /// cause a state transition.
    #[test]
    fn new_assignment_ignored_when_state_is_prepare_leaving() {
        let (mgr, _rx) = make(None, None, None);
        force_into_prepare_leaving(&mgr);
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // Java's onHeartbeatSuccess in PREPARE_LEAVING state is a no-op
        // for new assignments. We invoke it on the response path.
        use crate::consumer_group_heartbeat_response_data::{
            Assignment, ConsumerGroupHeartbeatResponseData, TopicPartitions,
        };
        let topic_id = Uuid::random_uuid();
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 1;
        data.heartbeat_interval_ms = 5000;
        data.assignment = Some(Assignment {
            topic_partitions: vec![TopicPartitions { topic_id, partitions: vec![0], unknown_tagged_fields: vec![] }],
            unknown_tagged_fields: vec![],
        });
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        mgr.on_heartbeat_success(&resp).unwrap();
        // Member stays in PREPARE_LEAVING; the new assignment is
        // ignored because the state can't accept new assignments.
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testSameAssignmentReconciledAgainWhenFenced`.
    /// After a fence, receiving the same target assignment again must
    /// re-trigger reconciliation (state goes to RECONCILING) because
    /// the local epoch was lost.
    #[tokio::test]
    async fn same_assignment_reconciled_again_when_fenced() {
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();

        // Pre-populate the local cache so reconcile can resolve.
        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }

        let mut assignment = HashMap::new();
        assignment.insert(topic_id, vec![0, 1, 2]);
        mgr.abstract_mm.process_assignment_received(assignment.clone()).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Get fenced (no assigned partitions, so §31 short-circuits).
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        // current_assignment was cleared.
        assert!(mgr.current_assignment().is_none());

        // Receive the same assignment again.
        mgr.abstract_mm.process_assignment_received(assignment).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupEpoch`.
    /// `leave_group()` sets the epoch to -2 for static members and -1
    /// for dynamic.
    #[tokio::test]
    async fn leave_group_epoch_test() {
        // Static member -> -2 with default operation.
        let (mgr, _rx) = make(Some("instance1".to_string()), None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_STATIC_MEMBER_EPOCH);

        // Dynamic member -> -1.
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupEpochOnClose`.
    /// `leave_group_on_close()` honors the supplied membership
    /// operation when computing the leave epoch.
    #[tokio::test]
    async fn leave_group_epoch_on_close() {
        // Static member with DEFAULT -> -2.
        let (mgr, _rx) = make(Some("instance1".to_string()), None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group_on_close(GroupMembershipOperation::Default, 0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_STATIC_MEMBER_EPOCH);

        // Static member with LEAVE_GROUP -> -1.
        let (mgr, _rx) = make(Some("instance1".to_string()), None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group_on_close(GroupMembershipOperation::LeaveGroup, 0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);

        // Dynamic member with DEFAULT -> -1.
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group_on_close(GroupMembershipOperation::Default, 0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
    }

    /// Regression: reconcile returns Err when the rebalance listener
    /// returns Err on the onPartitionsAssigned callback (COMMENTS.1.md
    /// fix #2).
    #[tokio::test]
    async fn reconcile_propagates_assigned_listener_error() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.assigned_topic_names_cache.insert(topic_id, "t1".to_string());
        }
        let mut new_assignment = HashMap::new();
        new_assignment.insert(topic_id, vec![0]);
        mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr_arc = Arc::new(mgr);
        let mgr_clone = mgr_arc.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        let env = rx.recv().await.expect("event");
        if let BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } = env.event {
            // applyNewAssignment succeeds; the onPartitionsAssigned callback
            // fails (simulated by acking Err).
            let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
            mgr_arc.apply_assignment(&assigned_set, &added_partitions).unwrap();
            ack.send(Err(KafkaError::timeout("listener error"))).unwrap();
        }
        let result = bg.await.unwrap();
        assert!(result.is_err());
    }

    // ===================================================================
    // Phase 34: metadata-driven reconciliation tests.
    //
    // Unlike the reconcile tests above (which pre-seed
    // `assigned_topic_names_cache` to bypass metadata resolution), these
    // drive reconciliation against REAL `ConsumerMetadata` so that the
    // metadata-resolution / unresolved-assignment / delayed-result-discard
    // logic is actually exercised.
    //
    // Java mocks `SubscriptionState` and `ConsumerMetadata`; we use the
    // real objects. Java's `verify(subscriptionState).assignFromSubscribedAwaitingCallback(...)`
    // becomes an assertion on the resulting real `SubscriptionState`
    // (`assigned_partitions()`, `is_fetchable(tp)`); Java's
    // `when(metadata.topicNames()).thenReturn(...)` becomes a real
    // metadata update via `seed_metadata`; Java's
    // `verify(metadata).requestUpdate(anyBoolean())` becomes
    // `metadata.update_requested()`.
    // ===================================================================

    use crate::common::Node;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::MetadataResponse;
    use crate::metadata_response_data::{
        MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
    };

    /// Build a one-partition `MetadataResponse` per topic, mirroring
    /// `consumer_metadata.rs::tests::build_response`. The membership
    /// reconcile path only needs `topic_names()` (id -> name), which the
    /// metadata snapshot populates from the response's partition topics
    /// regardless of subscription-based retention.
    fn build_metadata_response(topics: &[(&str, Uuid)]) -> MetadataResponse {
        let node = Node::new(1, "localhost".to_string(), 9092);
        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("test-cluster-id".to_string()));
        data.set_controller_id(node.id());

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(node.id());
        broker.set_host(node.host().to_string());
        broker.set_port(node.port());
        data.set_brokers(vec![broker]);

        let response_topics: Vec<MetadataResponseTopic> = topics
            .iter()
            .map(|(name, topic_id)| {
                let mut t = MetadataResponseTopic::new();
                t.set_name(Some((*name).to_string()));
                t.set_topic_id(*topic_id);
                t.set_error_code(Errors::None.code());
                t.set_is_internal(false);
                let mut partition = MetadataResponsePartition::new();
                partition.set_partition_index(0);
                partition.set_error_code(Errors::None.code());
                partition.set_leader_id(node.id());
                partition.set_leader_epoch(5);
                partition.set_replica_nodes(vec![node.id()]);
                partition.set_isr_nodes(vec![node.id()]);
                partition.set_offline_replicas(Vec::new());
                t.set_partitions(vec![partition]);
                t
            })
            .collect();
        data.set_topics(response_topics);
        MetadataResponse::new(data, ApiKeys::METADATA.latest_version())
    }

    /// Feed REAL topic-name metadata to the manager so that
    /// `find_resolvable_assignment_and_trigger_metadata_update` resolves
    /// the given topic ids without pre-seeding the local cache. Mirrors
    /// Java's `when(metadata.topicNames()).thenReturn(map)`.
    fn seed_metadata(mgr: &ConsumerMembershipManager, topics: &[(&str, Uuid)]) {
        let response = build_metadata_response(topics);
        mgr.abstract_mm
            .metadata
            .metadata_arc()
            .update_with_current_request_version(&response, false, 1000);
    }

    /// Set of topic ids in the current target assignment that are NOT yet
    /// resolvable (mirrors Java's `membershipManager.topicsAwaitingReconciliation()`).
    fn topics_awaiting_reconciliation(mgr: &ConsumerMembershipManager) -> HashSet<Uuid> {
        let target_ids: HashSet<Uuid> = {
            let guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.current_target_assignment.partitions.keys().copied().collect()
        };
        let resolved_ids: HashSet<Uuid> = {
            let metadata_names = mgr.abstract_mm.metadata.metadata_arc().topic_names();
            let guard = mgr.abstract_mm.inner.lock().unwrap();
            target_ids
                .iter()
                .filter(|id| metadata_names.contains_key(id) || guard.assigned_topic_names_cache.contains_key(id))
                .copied()
                .collect()
        };
        target_ids.difference(&resolved_ids).copied().collect()
    }

    /// Map of topic id -> partitions in the current target assignment
    /// that has not yet been reconciled into `current_assignment`
    /// (mirrors Java's `topicPartitionsAwaitingReconciliation()`).
    fn topic_partitions_awaiting_reconciliation(mgr: &ConsumerMembershipManager) -> HashMap<Uuid, Vec<i32>> {
        let guard = mgr.abstract_mm.inner.lock().unwrap();
        let mut out = HashMap::new();
        for (id, target_parts) in &guard.current_target_assignment.partitions {
            let current = guard.current_assignment.partitions.get(id);
            let diff: Vec<i32> = match current {
                Some(cur) => target_parts.iter().copied().filter(|p| !cur.contains(p)).collect(),
                None => target_parts.clone(),
            };
            if !diff.is_empty() {
                let mut sorted = diff;
                sorted.sort_unstable();
                out.insert(*id, sorted);
            }
        }
        out
    }

    /// True while a reconcile is in flight (Java's `reconciliationInProgress()`).
    fn reconciliation_in_progress(mgr: &ConsumerMembershipManager) -> bool {
        mgr.abstract_mm.inner.lock().unwrap().reconciliation_in_progress
    }

    /// Test view of the lock-free pending-reconcile flag (Phase 41 Issue 1
    /// observability): `true` while a reconcile callback's cross-iteration
    /// state is stored.
    fn has_pending_reconcile_for_test(mgr: &ConsumerMembershipManager) -> bool {
        mgr.has_pending_reconcile()
    }

    /// Single, non-looping `reconcile` call — reaches the next callback park
    /// point (storing the cross-iteration pending state) and returns. Used by
    /// tests that want to observe the parked reconcile state without a
    /// concurrent driver loop.
    async fn reconcile_once(mgr: &ConsumerMembershipManager, can_commit: bool) -> Result<(), KafkaError> {
        mgr.reconcile(0, can_commit).await
    }

    /// Receive a heartbeat response carrying the given target assignment,
    /// mirroring the Java `receiveAssignment(topicId, partitions, mgr)`
    /// helper.
    fn receive_assignment(mgr: &ConsumerMembershipManager, topic_id: Uuid, partitions: Vec<i32>) {
        use crate::consumer_group_heartbeat_response_data::{
            Assignment, ConsumerGroupHeartbeatResponseData, TopicPartitions,
        };
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 1;
        data.heartbeat_interval_ms = 5000;
        data.assignment = Some(Assignment {
            topic_partitions: vec![TopicPartitions { topic_id, partitions, unknown_tagged_fields: vec![] }],
            unknown_tagged_fields: vec![],
        });
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        mgr.on_heartbeat_success(&resp).unwrap();
    }

    /// Receive a heartbeat with a full multi-topic target assignment.
    fn receive_assignment_map(mgr: &ConsumerMembershipManager, assignment: &[(Uuid, Vec<i32>)]) {
        use crate::consumer_group_heartbeat_response_data::{
            Assignment, ConsumerGroupHeartbeatResponseData, TopicPartitions,
        };
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 1;
        data.heartbeat_interval_ms = 5000;
        data.assignment = Some(Assignment {
            topic_partitions: assignment
                .iter()
                .map(|(topic_id, partitions)| TopicPartitions {
                    topic_id: *topic_id,
                    partitions: partitions.clone(),
                    unknown_tagged_fields: vec![],
                })
                .collect(),
            unknown_tagged_fields: vec![],
        });
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        mgr.on_heartbeat_success(&resp).unwrap();
    }

    /// Receive an empty target assignment (revoke everything).
    fn receive_empty_assignment(mgr: &ConsumerMembershipManager) {
        use crate::consumer_group_heartbeat_response_data::{Assignment, ConsumerGroupHeartbeatResponseData};
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 1;
        data.heartbeat_interval_ms = 5000;
        data.assignment = Some(Assignment { topic_partitions: vec![], unknown_tagged_fields: vec![] });
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        mgr.on_heartbeat_success(&resp).unwrap();
    }

    /// Drive `reconcile(0, can_commit)` on a bg task, expecting exactly
    /// one rebalance-listener callback of `expected_method` over the
    /// given `expected_partitions` (order-insensitive), ack it, and
    /// return once reconcile has completed. Asserts no other event was
    /// emitted. Mirrors Java's `performCallback(..., complete=true)` +
    /// the surrounding `maybeReconcile` flow.
    async fn reconcile_and_complete_callback(
        mgr: Arc<ConsumerMembershipManager>,
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
        can_commit: bool,
        expected_method: ConsumerRebalanceListenerMethodName,
        expected_partitions: &[TopicPartition],
    ) {
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(can_commit).await });
        let env = rx.recv().await.expect("expected a callback-needed event");
        match env.event {
            // Revoke / lost path (AK 4.3.1: `PartitionsRemovedEvent`).
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions } => {
                assert_eq!(method_name, expected_method, "unexpected callback method");
                let got: HashSet<TopicPartition> = partitions.into_iter().collect();
                let want: HashSet<TopicPartition> = expected_partitions.iter().cloned().collect();
                assert_eq!(got, want, "unexpected callback partitions");
                ack.send(Ok(())).unwrap();
            },
            // Assign path (AK 4.3.1: `PartitionsAssignedEvent`). The app
            // thread applies the assignment to the subscription state (via
            // `ApplyAssignmentEvent` → `apply_assignment`) BEFORE running
            // `on_partitions_assigned` and acking. This component-level helper
            // simulates that app-side step so the subscription reflects the
            // new assignment.
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(
                    expected_method,
                    ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                    "PartitionsAssigned received but the expected callback method was not OnPartitionsAssigned"
                );
                let got: HashSet<TopicPartition> = added_partitions.iter().cloned().collect();
                let want: HashSet<TopicPartition> = expected_partitions.iter().cloned().collect();
                assert_eq!(got, want, "unexpected added partitions");
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        bg.await.unwrap().unwrap();
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Put the SubscriptionState into auto-topic mode so
    /// `assign_from_subscribed_awaiting_callback` is permitted, and
    /// pre-own the given partitions (mirrors Java's
    /// `mockOwnedPartition`/`mockOwnedPartitionAndAssignmentReceived`
    /// `when(subscriptionState.assignedPartitions())` setup, but with the
    /// real SubscriptionState).
    fn mock_owned_partitions(mgr: &ConsumerMembershipManager, owned: &[TopicPartition]) {
        let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        subs.assign_from_subscribed(owned).unwrap();
    }

    /// Add topic names to the (real) SubscriptionState subscription so
    /// that assigned partitions of those topics are considered fetchable
    /// (`is_fetchable_and_subscribed` gates on subscription membership).
    /// Java mocks SubscriptionState so it never exercises this gate; in
    /// Rust we must subscribe to the topics we expect to fetch — exactly
    /// what a real consumer does before being assigned them.
    fn subscribe_topics(mgr: &ConsumerMembershipManager, topics: &[&str]) {
        let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        let set: HashSet<String> = topics.iter().map(|s| s.to_string()).collect();
        // Preserve the NoopListener registered by `make()` so the §31
        // handshake still enqueues callback events.
        subs.subscribe_topics(set, Some(Arc::new(NoopListener))).unwrap();
    }

    // ---------------------------------------------------------------
    // Commit 1: reconcile against real metadata.
    // ---------------------------------------------------------------

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcileNewPartitionsAssignedWhenNoPartitionOwned`.
    /// New partitions are assigned starting from an empty owned set; the
    /// member resolves the topic from real metadata, reconciles, and the
    /// SubscriptionState reflects the new assignment.
    #[tokio::test]
    async fn reconcile_new_partitions_assigned_when_no_partition_owned() {
        let (mgr, mut rx) = make(None, None, None);
        // Subscribe to topic1 so its assigned partitions are fetchable
        // (real-SubscriptionState gate; see `subscribe_topics`).
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!reconciliation_in_progress(&mgr));

        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
        )
        .await;

        // verifyReconciliationTriggeredAndCompleted: ACKNOWLEDGING, no
        // longer in progress, and the assignment applied.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));
        {
            let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 0), tp("topic1", 1)]));
        }
        // Both partitions are fetchable after the assigned callback was
        // acked (enable_partitions_awaiting_callback cleared the
        // pending-on-assigned-callback gate). is_fetchable also requires a
        // valid position (real-SubscriptionState gate Java's mock hides),
        // so seek the partitions first.
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 0).unwrap();
            subs.seek(&tp("topic1", 1), 0).unwrap();
        }
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.is_fetchable(&tp("topic1", 0)));
        assert!(subs.is_fetchable(&tp("topic1", 1)));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`.
    /// Owning topic1-0, a new assignment adding 1 and 2 reconciles to the
    /// union with no revocation.
    #[tokio::test]
    async fn reconcile_new_partitions_assigned_when_other_partitions_owned() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        // New assignment adding partitions 1 and 2 to owned partition 0.
        receive_assignment(&mgr, topic_id, vec![0, 1, 2]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        // Only the *added* partitions (1, 2) are passed to onPartitionsAssigned.
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 1), tp("topic1", 2)],
        )
        .await;

        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(
            subs.assigned_partitions(),
            HashSet::from([tp("topic1", 0), tp("topic1", 1), tp("topic1", 2)])
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcileNewPartitionsAssignedAndRevoked`.
    /// Owning topic1-0, a new assignment of {1,2} revokes 0 and assigns
    /// 1,2. With no listener-driven auto-commit, the revoke and assign
    /// callbacks both flow.
    #[tokio::test]
    async fn reconcile_new_partitions_assigned_and_revoked() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        // New assignment revoking 0, assigning 1 and 2.
        receive_assignment(&mgr, topic_id, vec![1, 2]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        // First callback: onPartitionsRevoked for {0}.
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        let env = rx.recv().await.expect("revoked event");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                assert_eq!(partitions.into_iter().collect::<HashSet<_>>(), HashSet::from([tp("topic1", 0)]));
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        // Second event: PartitionsAssigned for {1, 2}. Simulate the app
        // applying the assignment before acking.
        let env = rx.recv().await.expect("assigned event");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(
                    added_partitions.iter().cloned().collect::<HashSet<_>>(),
                    HashSet::from([tp("topic1", 1), tp("topic1", 2)])
                );
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        bg.await.unwrap().unwrap();

        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));
        let mut current = mgr.current_assignment().partitions;
        for v in current.values_mut() {
            v.sort_unstable();
        }
        assert_eq!(current, HashMap::from([(topic_id, vec![1, 2])]));
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 1), tp("topic1", 2)]));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconciliationSkippedWhenSameAssignmentReceived`.
    /// After reconciling+ack'ing {0,1}, receiving the same assignment
    /// again does not re-trigger reconciliation.
    #[tokio::test]
    async fn reconciliation_skipped_when_same_assignment_received() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);

        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);

        // Receive the same assignment again -> no reconciliation.
        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Stable);
        // A reconcile call is a no-op (target == current); no event emitted.
        mgr.reconcile(0, true).await.unwrap();
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "no reconciliation should be triggered for an identical assignment",
        );
        assert_eq!(mgr.state(), MemberState::Stable);
        assert!(!reconciliation_in_progress(&mgr));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcilePartitionsRevokedNoAutoCommitNoCallbacks`.
    /// Owning topic1-0 with no listener and no auto-commit, an empty
    /// assignment revokes everything and completes the revocation.
    #[tokio::test]
    async fn reconcile_partitions_revoked_no_auto_commit_no_callbacks() {
        // make_without_listener: §31 short-circuits (no callback events),
        // mirroring Java's mockRevocationNoCallbacks(false).
        let (mgr, mut rx) = make_without_listener(None, None, None);
        // Auto-topic mode is required for assign_from_subscribed; the
        // no-listener helper skips subscribe_topics, so subscribe here
        // (still without a listener).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!reconciliation_in_progress(&mgr));

        // No listener -> the revoke path emits NO PartitionsRemoved event.
        // But AK 4.3.1 (KAFKA-20106) enqueues a PartitionsAssigned event even
        // with no listener (the app must apply the assignment within poll).
        // The reconcile parks on it; simulate the app applying + acking.
        mgr.reconcile(0, true).await.unwrap();
        let env = rx.recv().await.expect("PartitionsAssigned event (sent even with no listener)");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        // Drive again to consume the ack and advance the reconciliation.
        mgr.reconcile(0, true).await.unwrap();
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "no further events after the assignment ack",
        );

        // testRevocationOfAllPartitionsCompleted: ACKNOWLEDGING, empty
        // current assignment, all revoked.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));
        assert!(mgr.current_assignment().is_none() || mgr.current_assignment().partitions.is_empty());
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks`.
    /// With auto-commit enabled, the member stays in RECONCILING until
    /// the commit completes, then completes the revocation.
    #[tokio::test]
    async fn reconcile_partitions_revoked_with_successful_auto_commit_no_callbacks() {
        let (mgr, mut rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // The commit-manager auto-commit future resolves immediately
        // (no offsets to commit), so the reconcile proceeds and the
        // revocation completes. With no listener registered, the §31
        // revoke handshake short-circuits — but AK 4.3.1 still enqueues a
        // PartitionsAssigned event (empty), which the app applies + acks.
        mgr.reconcile(0, true).await.unwrap();
        let env = rx.recv().await.expect("PartitionsAssigned event (sent even with no listener)");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        mgr.reconcile(0, true).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway`
    /// (`ConsumerMembershipManagerTest.java:1579`).
    /// Even if the auto-commit before rebalance fails EXCEPTIONALLY, the
    /// revocation still completes (Java's "proceed with reconciliation
    /// anyway"). Java arranges a commit future and
    /// `commitResult.completeExceptionally(new KafkaException("...non-retriable
    /// error"))` (test:1596), then asserts the revocation reaches
    /// completion regardless.
    ///
    /// To exercise the failure arm (`Ok(Err(err))` at reconcile step 8a,
    /// lines 638-645) we must drive a REAL commit failure: seed a consumed
    /// offset so the auto-commit actually enqueues a request, spawn the
    /// reconcile (which parks awaiting the commit future), then fail that
    /// commit with a non-retriable error. A mutation that changed the
    /// `Ok(Err(err))` arm to `return Err(err)` (abort revocation on commit
    /// failure) would leave the member in RECONCILING and fail this test.
    #[tokio::test]
    async fn reconcile_partitions_revoked_with_failed_auto_commit_completes_revocation_anyway() {
        // No listener: the §31 revoked-callback handshake short-circuits,
        // so the reconcile parks on the commit future and then (AK 4.3.1) on
        // the PartitionsAssigned event that is enqueued even with no listener.
        let (mgr, mut rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        // Seed a valid position on the owned partition so
        // `subscriptions.allConsumed()` is non-empty and the auto-commit
        // before rebalance actually enqueues a commit request (otherwise it
        // short-circuits to immediate Ok — the previous bug in this test).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        // The reconcile parks at step 8a awaiting the auto-commit. Wait for
        // the commit manager to enqueue the unsent commit request, then
        // fail it with a NON-retriable error — mirroring Java's
        // `commitResult.completeExceptionally(new KafkaException(...))`.
        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        loop {
            if commit_mgr.fail_first_unsent_commit_for_test(KafkaError::new(Errors::OffsetMetadataTooLarge)) {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Reconcile proceeds past the failed commit and enqueues the
        // PartitionsAssigned event (empty, sent even with no listener).
        // Simulate the app applying the assignment + acking so the drive
        // completes.
        let env = rx.recv().await.expect("PartitionsAssigned event");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }

        // Reconcile must proceed with the revocation ANYWAY despite the
        // failed commit, reaching ACKNOWLEDGING with everything revoked.
        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testSameAssignmentReconciledAgainWithMissingTopic`.
    /// One topic resolvable, one permanently missing. The resolvable one
    /// is reconciled+acked; the missing one stays awaiting reconciliation.
    /// A re-sent assignment with the same resolvable partitions is acked
    /// again without re-running a full reconcile.
    #[tokio::test]
    async fn same_assignment_reconciled_again_with_missing_topic() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        // Only topic1 is in metadata.
        seed_metadata(&mgr, &[("topic1", topic1)]);

        // assignment1: topic1-0 (resolvable) + topic2-0 (unresolvable).
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);

        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        // Still RECONCILING because the unresolved topic2 keeps the
        // target unreconciled.
        assert_eq!(mgr.state(), MemberState::Reconciling);
        // topic2 is awaiting reconciliation.
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic2]));

        // Receive original assignment again -> not a full reconcile, but
        // ack again. topic1 stays assigned, topic2 still awaiting.
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        // No callback emitted on the re-ack (same resolvable partitions).
        mgr.reconcile(0, true).await.unwrap();
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "no callback should fire for the same resolvable assignment",
        );
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert_eq!(mgr.current_assignment().partitions, HashMap::from([(topic1, vec![0])]));
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic2]));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable`.
    /// After a topic is reconciled (and cached locally), a subsequent
    /// revocation of one of its partitions completes using the LOCAL
    /// cache even when the topic is no longer present in metadata — no
    /// metadata update is requested.
    #[tokio::test]
    async fn revoke_partitions_uses_topic_names_local_cache_when_metadata_not_available() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        // Reconcile {0, 1}.
        receive_assignment(&mgr, topic_id, vec![0, 1]);
        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        // topic1 is now in the local cache.
        assert!(
            mgr.abstract_mm
                .inner
                .lock()
                .unwrap()
                .assigned_topic_names_cache
                .contains_key(&topic_id),
            "topic name should have been cached during reconciliation",
        );

        // Make metadata no longer contain topic1 (simulate eviction):
        // seed a different topic. The local cache must be used for the
        // revocation. Reset update_requested by reading current state.
        let other = Uuid::random_uuid();
        seed_metadata(&mgr, &[("other", other)]);
        // Clear any pending update flag from prior reconcile so we can
        // assert no NEW update is requested by the revocation path.
        let _ = mgr.abstract_mm.metadata.metadata_arc().update_requested();

        // Revoke partition 0, keep partition 1. This reconcile fires TWO
        // callbacks: onPartitionsRevoked({0}) then onPartitionsAssigned({})
        // (added is empty since {1} is already owned).
        receive_assignment(&mgr, topic_id, vec![1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        expect_callback(
            &mut rx,
            None,
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            &[tp("topic1", 0)],
            Ok(()),
        )
        .await;
        expect_callback(
            &mut rx,
            Some(&*mgr),
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();
        // Revocation completed using the LOCAL topic-name cache (topic1
        // was resolved from cache/retained metadata, no failure).
        // testRevocationCompleted: ACKNOWLEDGING, remaining {1}.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 1)]));
    }

    // ---------------------------------------------------------------
    // Commit 2: metadata resolution / unresolved assignments.
    // ---------------------------------------------------------------

    /// Translated from
    /// `ConsumerMembershipManagerTest#testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived`.
    /// An assignment whose topic is not yet in metadata is kept awaiting
    /// reconciliation; once metadata arrives, the next reconcile resolves
    /// and applies it.
    #[tokio::test]
    async fn unresolved_target_assignment_is_reconciled_when_metadata_received() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        // Bring the member to a STABLE-like baseline by reconciling an
        // empty assignment first is unnecessary; Java starts from a
        // stable member. We just need to be in-group so a new assignment
        // transitions to RECONCILING.
        let topic_id = Uuid::random_uuid();

        // Assignment not in metadata. Member keeps it awaiting metadata.
        receive_assignment(&mgr, topic_id, vec![1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic_id]));

        // The resolution pass cannot resolve the topic, so it requests a
        // metadata update and resolves nothing. (We call the production
        // resolution method directly rather than full `reconcile`: with an
        // unresolved-only target, `reconcile` would still enqueue an empty
        // onPartitionsAssigned and await the ack — Java drives the same via
        // the bg/app split. The behavior under test here is the
        // metadata-update request for the unresolved id, which lives in
        // `find_resolvable_assignment_and_trigger_metadata_update`.)
        let resolved = mgr.abstract_mm.find_resolvable_assignment_and_trigger_metadata_update();
        assert!(
            resolved.is_empty(),
            "nothing should resolve while the topic is missing from metadata"
        );
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(
            mgr.abstract_mm.metadata.metadata_arc().update_requested(),
            "an unresolved topic id must trigger a metadata update request",
        );

        // Metadata update received including the missing topic name.
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 1)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(topics_awaiting_reconciliation(&mgr).is_empty());
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 1)]));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved`.
    /// Of two assigned topics, only one is in metadata. The resolvable
    /// one is reconciled+acked; the unresolved one is kept awaiting and a
    /// metadata update is requested. Re-receiving the same assignment
    /// keeps the unresolved topic awaiting.
    #[tokio::test]
    async fn member_keeps_unresolved_assignment_waiting_for_metadata_until_resolved() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);

        // assignment: topic1-0 (resolvable), topic2-{1,3} (unresolvable).
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![1, 3])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0)],
        )
        .await;
        // Reconciled what was resolvable, kept the unresolved + requested update.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(mgr.abstract_mm.metadata.metadata_arc().update_requested());
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic2]));

        // Ack -> back to RECONCILING (unresolved still pending).
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Receive the same assignment again -> topic2 still unresolved.
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![1, 3])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic2]));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testMetadataUpdatesReconcilesUnresolvedAssignments`.
    /// An unresolved assignment is reconciled as soon as metadata is
    /// discovered, without the broker re-sending the assignment.
    #[tokio::test]
    async fn metadata_updates_reconciles_unresolved_assignments() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();

        // Assignment not in metadata.
        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // First resolution pass: cannot resolve -> request update,
        // nothing resolved (see note in
        // `unresolved_target_assignment_is_reconciled_when_metadata_received`).
        let resolved = mgr.abstract_mm.find_resolvable_assignment_and_trigger_metadata_update();
        assert!(resolved.is_empty());
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic_id]));
        assert!(mgr.abstract_mm.metadata.metadata_arc().update_requested());

        // Metadata discovered -> reconcile completes.
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(topics_awaiting_reconciliation(&mgr).is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testMetadataUpdatesRequestsAnotherUpdateIfNeeded`.
    /// While a topic remains unresolved, every reconcile attempt requests
    /// another metadata update.
    #[tokio::test]
    async fn metadata_updates_requests_another_update_if_needed() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let metadata = mgr.abstract_mm.metadata.metadata_arc();

        // First resolution attempt: unresolved -> request update.
        // Java asserts `verify(metadata).requestUpdate(anyBoolean())` here.
        // The sticky `update_requested()` flag cannot tell a second call
        // from the flag left set by the first; assert on the per-pass call
        // counter instead (Mockito `times(N)` equivalent).
        let resolved = mgr.abstract_mm.find_resolvable_assignment_and_trigger_metadata_update();
        assert!(resolved.is_empty());
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic_id]));
        assert!(metadata.update_requested());
        assert_eq!(
            metadata.request_update_call_count_for_test(),
            1,
            "first pass must request a metadata update exactly once",
        );

        // Second attempt, metadata still missing the topic -> request
        // update AGAIN (still nothing resolved, still awaiting). The
        // production method always re-requests an update for unresolved
        // ids (Java's per-poll `requestUpdate`). Java asserts
        // `verify(metadata, times(2)).requestUpdate(anyBoolean())` — the
        // call count must advance 1 -> 2 across the two passes. A mutation
        // dropping the second re-request would leave the counter at 1 and
        // fail this assertion (the sticky flag would NOT catch it).
        let resolved = mgr.abstract_mm.find_resolvable_assignment_and_trigger_metadata_update();
        assert!(resolved.is_empty());
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic_id]));
        assert!(metadata.update_requested());
        assert_eq!(
            metadata.request_update_call_count_for_test(),
            2,
            "second pass must independently re-request a metadata update (times(2))",
        );
        assert_eq!(mgr.state(), MemberState::Reconciling);
        // unused in this test now that we use the resolution method directly.
        let _ = &mut rx;
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedMetadataUsedToCompleteAssignment`.
    /// Starting from a reconciled topic1-0, a new assignment adds an
    /// unresolved topic2-0. The resolvable subset (no change) is acked,
    /// topic2 stays awaiting + requests metadata; once topic2 metadata
    /// arrives, the next reconcile assigns it.
    #[tokio::test]
    async fn delayed_metadata_used_to_complete_assignment() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);

        // Receive + reconcile topic1-0, reach STABLE.
        receive_assignment(&mgr, topic1, vec![0]);
        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0)],
        )
        .await;
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);

        // New assignment adding topic2-0 (not in metadata).
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // No full reconciliation triggered (topic2 unresolved); the
        // resolvable subset equals the current assignment so the member
        // just acks. topic2 stays awaiting + a metadata update is
        // requested.
        mgr.reconcile(0, true).await.unwrap();
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(mgr.abstract_mm.metadata.metadata_arc().update_requested());
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic2]));

        // Metadata discovered for topic2 -> reconcile assigns it.
        seed_metadata(&mgr, &[("topic1", topic1), ("topic2", topic2)]);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic2", 0)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(topics_awaiting_reconciliation(&mgr).is_empty());
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 0), tp("topic2", 0)]));
    }

    // ---------------------------------------------------------------
    // Commit 3: new-assignment-replaces-waiting-on-metadata.
    // ---------------------------------------------------------------

    /// Receive an assignment of two topics that are NOT in metadata,
    /// leaving the member RECONCILING with both topics awaiting metadata.
    /// Mirrors Java's `mockJoinAndReceiveAssignment(false)` +
    /// `createAssignment(false)` (two random unresolvable topics).
    fn join_and_receive_unresolved_assignment(mgr: &ConsumerMembershipManager) -> (Uuid, Uuid) {
        mgr.transition_to_joining().unwrap();
        let topic_a = Uuid::random_uuid();
        let topic_b = Uuid::random_uuid();
        receive_assignment_map(mgr, &[(topic_a, vec![0, 1, 2]), (topic_b, vec![3, 4, 5])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!topics_awaiting_reconciliation(mgr).is_empty());
        (topic_a, topic_b)
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testNewAssignmentReplacesPreviousOneWaitingOnMetadata`.
    /// A new resolvable assignment that does NOT include the
    /// waiting-on-metadata topics discards them and reconciles the new one.
    #[tokio::test]
    async fn new_assignment_replaces_previous_one_waiting_on_metadata() {
        let (mgr, mut rx) = make(None, None, None);
        join_and_receive_unresolved_assignment(&mgr);

        // Ack -> still RECONCILING, still awaiting metadata.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!topics_awaiting_reconciliation(&mgr).is_empty());

        // New target assignment (resolvable) not including the previous.
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        receive_assignment(&mgr, topic_id, vec![0]);

        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 0)]));
        drop(subs);

        // Ack -> STABLE, nothing awaiting (the unresolved topics were
        // discarded).
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        assert!(topics_awaiting_reconciliation(&mgr).is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata`.
    /// A new EMPTY assignment discards the waiting-on-metadata topics and
    /// goes back to STABLE with nothing to reconcile.
    #[tokio::test]
    async fn new_empty_assignment_replaces_previous_one_waiting_on_metadata() {
        let (mgr, mut rx) = make(None, None, None);
        join_and_receive_unresolved_assignment(&mgr);

        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!topics_awaiting_reconciliation(&mgr).is_empty());

        // Empty assignment received -> the previous unresolved topics are
        // discarded from the target (this happens in
        // process_assignment_received / LocalAssignment::update_with, not
        // in reconcile). With an empty target and an empty current
        // assignment, there is nothing left awaiting reconciliation.
        receive_empty_assignment(&mgr);
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
        assert!(
            topics_awaiting_reconciliation(&mgr).is_empty(),
            "the empty assignment must discard the previously-unresolved topics",
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata`.
    /// A new UNRESOLVABLE assignment replaces the previous unresolved one;
    /// the member keeps only the NEW topic as awaiting metadata.
    #[tokio::test]
    async fn new_assignment_not_in_metadata_replaces_previous_one_waiting_on_metadata() {
        let (mgr, mut rx) = make(None, None, None);
        join_and_receive_unresolved_assignment(&mgr);

        // New unresolvable assignment (metadata empty for it). This
        // replaces the previous unresolved target.
        let topic_id = Uuid::random_uuid();
        receive_assignment(&mgr, topic_id, vec![0]);
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));

        // The resolution pass cannot resolve the new topic -> request
        // update, nothing resolved.
        let resolved = mgr.abstract_mm.find_resolvable_assignment_and_trigger_metadata_update();
        assert!(resolved.is_empty());
        assert_eq!(mgr.state(), MemberState::Reconciling);
        // Only the NEW topic is awaiting reconciliation (the prior two
        // were discarded by the replacing assignment).
        assert_eq!(topics_awaiting_reconciliation(&mgr), HashSet::from([topic_id]));
    }

    // ---------------------------------------------------------------
    // Commit 4: delayed-reconciliation discard (mutation-resistant).
    //
    // A reconcile that is in-flight (stuck on a §31 callback) must be
    // DISCARDED if the member transitions out of RECONCILING (fatal) or
    // rejoins (fence + new assignment) before the callback completes.
    // The discard is enforced by `maybe_abort_reconciliation()` —
    // deleting that guard makes these tests fail (the stale assignment
    // would be applied and the member would reach ACKNOWLEDGING).
    // ---------------------------------------------------------------

    /// A failure while enqueuing a §31 callback must clear
    /// `reconciliation_in_progress` before it propagates.
    ///
    /// Java funnels every failure in the revocation+assignment chain through
    /// one arm that logs and calls `markReconciliationCompleted()`
    /// (`AbstractMembershipManager.java:958-965`). Rust replaced the
    /// `CompletableFuture` chain with explicit steps and `?`, which returns
    /// *past* the clearing — so the flag stayed set and every later
    /// `reconcile()` short-circuited on "Another reconciliation is already in
    /// progress". Permanently: nothing else clears it, so the consumer could
    /// never rebalance again.
    ///
    /// Both enqueue sites are covered: the revoked callback (step 9, reached
    /// with owned partitions being taken away) and the assigned callback
    /// (step 13, reached when nothing is revoked). The channel is closed by
    /// dropping the receiver, which is what `BackgroundEventHandler::add`
    /// reports as an error.
    ///
    /// The member must stay in RECONCILING — Java is explicit that it does not
    /// send the ack and expects the broker to fence it after the reconciliation
    /// commit timeout.
    #[tokio::test]
    async fn callback_enqueue_failure_clears_reconciliation_in_progress() {
        // Case 1 — step 9: a partition is owned and then revoked.
        {
            let (mgr, rx) = make(None, None, None);
            subscribe_topics(&mgr, &["topic1"]);
            mgr.transition_to_joining().unwrap();
            let topic1 = Uuid::random_uuid();
            seed_metadata(&mgr, &[("topic1", topic1)]);
            mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
            receive_empty_assignment(&mgr);
            assert_eq!(mgr.state(), MemberState::Reconciling);

            // Close the background-event channel so the enqueue fails.
            drop(rx);

            let err = reconcile_once(&mgr, true).await.expect_err("enqueue must fail");
            assert!(
                err.message().contains("background-event receiver is closed"),
                "unexpected error: {err}"
            );
            assert!(
                !reconciliation_in_progress(&mgr),
                "step-9 failure must clear reconciliation_in_progress"
            );
            assert_eq!(mgr.state(), MemberState::Reconciling, "the member must stay RECONCILING");
        }

        // Case 2 — step 13: nothing owned, so nothing is revoked and the first
        // enqueue reached is the assigned callback.
        {
            let (mgr, rx) = make(None, None, None);
            subscribe_topics(&mgr, &["topic1"]);
            mgr.transition_to_joining().unwrap();
            let topic1 = Uuid::random_uuid();
            seed_metadata(&mgr, &[("topic1", topic1)]);
            receive_assignment(&mgr, topic1, vec![0]);
            assert_eq!(mgr.state(), MemberState::Reconciling);

            drop(rx);

            let err = reconcile_once(&mgr, true).await.expect_err("enqueue must fail");
            assert!(
                err.message().contains("background-event receiver is closed"),
                "unexpected error: {err}"
            );
            assert!(
                !reconciliation_in_progress(&mgr),
                "step-13 failure must clear reconciliation_in_progress"
            );
            assert_eq!(mgr.state(), MemberState::Reconciling, "the member must stay RECONCILING");
        }
    }

    /// The abort guard must cover the commit park point even **with a
    /// rebalance listener registered** — the case the sibling
    /// `delayed_reconciliation_result_discarded_after_commit_if_member_rejoins`
    /// cannot reach.
    ///
    /// That sibling uses `make_with_commit_manager(false)`, so the §31 revoked
    /// callback short-circuits and control falls through to the step-10 guard
    /// regardless. With a listener present, step 9 *enqueues* the revoked
    /// callback first, so a stale reconcile does user-visible damage before any
    /// later guard can stop it.
    ///
    /// The commit await is also the only point where a reconcile suspends while
    /// `pending_reconcile` is still `None`, so the concurrent
    /// `clear_pending_reconcile()` inside `transition_to_fenced` sees
    /// `had_pending == false` and cannot abandon it. The guard right after the
    /// commit is the only thing that discards it.
    ///
    /// Java: `commitResult.whenComplete((__, commitReqError) -> { ...;
    /// if (!maybeAbortReconciliation()) { revokeAndAssign(...); } })`
    /// (`AbstractMembershipManager.java:911`).
    ///
    /// Failure without the guard: the listener is told its partition was LOST
    /// (by the fence) and then REVOKED (by the stale reconcile), and the
    /// canonical `commit_sync()` inside the revoke callback runs for a member
    /// that is no longer in the group.
    #[tokio::test]
    async fn delayed_reconciliation_discarded_after_commit_when_fenced_with_listener() {
        // Listener present: a revoked callback WOULD be enqueued, and that is
        // exactly what must not happen.
        let (mgr, mut rx) = make_with_commit_manager(true);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_topics(HashSet::from(["topic1".to_string()]), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        // A consumed offset makes `subscriptions.allConsumed()` non-empty, so
        // the auto-commit enqueues a real request and the reconcile parks on it
        // instead of short-circuiting to an immediate Ok.
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        // Empty assignment => topic1-0 is revoked, so step 9 has something to
        // enqueue once the commit resolves.
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        // Wait until the reconcile is parked on the commit.
        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        loop {
            if commit_mgr.unsent_offset_commits_len_for_test() > 0 && reconciliation_in_progress(&mgr) {
                break;
            }
            tokio::task::yield_now().await;
        }

        // A heartbeat fences the member WHILE the commit is in flight. With a
        // listener this enqueues onPartitionsLost and parks the release on its
        // ack, so the member stays FENCED — which is what the abort guard sees.
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fenced);

        let lost = rx.recv().await.expect("lost event");
        match lost.event {
            BackgroundEvent::PartitionsRemoved { method_name, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
            },
            other => panic!("expected the lost callback, got {other:?}"),
        }

        // Now let the commit resolve, releasing the parked reconcile.
        assert!(commit_mgr.complete_first_unsent_commit_for_test(HashMap::new()));
        bg.await.unwrap().unwrap();

        // The stale reconcile must be discarded: NO revoked callback follows the
        // lost one. Without the guard this receives an OnPartitionsRevoked.
        match rx.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {},
            Ok(env) => panic!("stale reconcile must not enqueue another callback, got {:?}", env.event),
            Err(other) => panic!("unexpected channel state: {other:?}"),
        }

        // The abort must also clear the in-progress flag, or every later
        // reconcile short-circuits on "already in progress" forever.
        assert!(!reconciliation_in_progress(&mgr), "abort must clear reconciliation_in_progress");
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultDiscardedIfMemberNotInReconcilingStateAnymore`.
    /// A member stuck reconciling (on the assigned callback) receives a
    /// FATAL error. When the callback finally completes, the
    /// reconciliation must not update the subscription or advance to
    /// ACKNOWLEDGING.
    #[tokio::test]
    async fn delayed_reconciliation_result_discarded_if_member_not_in_reconciling_state_anymore() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        receive_assignment(&mgr, topic_id, vec![0]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Reconcile parks on the PartitionsAssigned event: a single
        // `reconcile` call enqueues the event and stores the AfterAssign
        // pending state (it does NOT block). Capture the ack but do NOT
        // complete it.
        reconcile_once(&mgr, true).await.unwrap();
        let env = rx.recv().await.expect("assigned event");
        let _stuck_ack = match env.event {
            BackgroundEvent::PartitionsAssigned { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        // reconcile is now parked awaiting the ack, with
        // reconciliation_in_progress = true. AK 4.3.1 (KAFKA-20106): the
        // subscription is NOT applied by the parked reconcile anymore — the
        // app thread applies it via `apply_assignment` when processing the
        // PartitionsAssigned event (which this test skips), so the member
        // owns NOTHING while parked.
        assert!(reconciliation_in_progress(&mgr));
        assert!(has_pending_reconcile_for_test(&mgr));

        // Member receives a fatal error while reconciling. Phase 41 Issue 1:
        // transition_to_fatal first ABANDONS the in-flight reconcile
        // (clear_pending_reconcile drops the stored AfterAssign + its ack
        // receiver), so the stuck reconcile is discarded eagerly rather than
        // lazily via the abort-check.
        mgr.transition_to_fatal(0).unwrap();
        // (b) of Issue 1: the stale reconcile state is gone, so a fresh
        // reconcile is no longer gated on the stale ack draining.
        assert!(!has_pending_reconcile_for_test(&mgr));

        // No onPartitionsLost callback fires: because the assignment was never
        // applied to the subscription (app-side apply skipped), the member
        // owns no partitions when fatal fires.
        match rx.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {},
            Ok(env) => panic!("no lost callback expected, got {:?}", env.event),
            Err(other) => panic!("unexpected channel state: {other:?}"),
        }

        // The delayed reconciliation was discarded: state must NOT be
        // ACKNOWLEDGING. (a) of Issue 1's required assertions.
        assert_ne!(mgr.state(), MemberState::Acknowledging);
        assert_eq!(mgr.state(), MemberState::Fatal);
        // The subscription must not have been updated to the stale target.
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultDiscardedAfterPartitionsAssignedCallbackIfMemberRejoins`.
    /// A member stuck on the onPartitionsAssigned callback gets fenced
    /// and rejoins with a new assignment. When the stuck callback
    /// completes, the original reconciliation is discarded and the new
    /// assignment is what remains pending.
    #[tokio::test]
    async fn delayed_reconciliation_result_discarded_after_partitions_assigned_callback_if_member_rejoins() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1", "topic3"]);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);

        receive_assignment(&mgr, topic1, vec![1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Reconcile parks on the onPartitionsAssigned callback (single
        // non-looping call). Capture the ack but do NOT complete it.
        reconcile_once(&mgr, true).await.unwrap();
        let env = rx.recv().await.expect("assigned event");
        let _stuck_ack = match env.event {
            BackgroundEvent::PartitionsAssigned { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        // AK 4.3.1 (KAFKA-20106): the parked reconcile does NOT apply the
        // assignment to the subscription (that is the app thread's job via
        // `apply_assignment`, which this test skips), so the member owns
        // NOTHING while parked.
        assert!(reconciliation_in_progress(&mgr));
        assert!(has_pending_reconcile_for_test(&mgr));

        // Fenced + rejoin while still reconciling. Phase 41 Issue 1: fence
        // ABANDONS the in-flight reconcile (drops the stored AfterAssign and
        // clears reconciliation_in_progress). Because the member owns NO
        // partitions (assignment never applied), NO onPartitionsLost callback
        // fires. Fence transitions FENCED -> JOINING synchronously.
        mgr.transition_to_fenced(0).unwrap();
        // (b) of Issue 1: the stale reconcile is gone immediately.
        assert!(!has_pending_reconcile_for_test(&mgr));
        match rx.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {},
            Ok(env) => panic!("no lost callback expected, got {:?}", env.event),
            Err(other) => panic!("unexpected channel state: {other:?}"),
        }
        assert_eq!(mgr.state(), MemberState::Joining);
        // (a) of Issue 1: no wrong transition to ACKNOWLEDGING with the stale
        // resolved_assignment; reconciliation is no longer in progress.
        assert_ne!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));

        // (b) of Issue 1: the fresh post-rejoin assignment (topic3-5) can be
        // reconciled — it is not gated on the abandoned stale reconcile.
        let topic3 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic3", topic3)]);
        receive_assignment(&mgr, topic3, vec![5]);
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic3, vec![5])])
        );
        // The stale topic1 assignment was not applied.
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(!subs.assigned_partitions().contains(&tp("topic1", 1)));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultDiscardedAfterPartitionsRevokedCallbackIfMemberRejoins`.
    /// A member stuck on the onPartitionsRevoked callback gets fenced and
    /// rejoins. When the stuck callback completes, the reconciliation is
    /// discarded (assignment not applied, ack not sent).
    #[tokio::test]
    async fn delayed_reconciliation_result_discarded_after_partitions_revoked_callback_if_member_rejoins() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1", "topic3"]);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);
        // Own topic1-0 so the new assignment {1,2} revokes 0.
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_assignment(&mgr, topic1, vec![1, 2]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Reconcile parks on the onPartitionsRevoked callback (single
        // non-looping call). First event: onPartitionsRevoked for {0}.
        reconcile_once(&mgr, true).await.unwrap();
        let env = rx.recv().await.expect("revoked event");
        let _stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                assert_eq!(partitions.into_iter().collect::<HashSet<_>>(), HashSet::from([tp("topic1", 0)]));
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(reconciliation_in_progress(&mgr));
        assert!(has_pending_reconcile_for_test(&mgr));

        // Fence + rejoin. Phase 41 Issue 1: fence ABANDONS the in-flight
        // reconcile (drops the stored AfterRevoke). Owned topic1-0 (pre-owned
        // via mock_owned_partitions) means the §31 lost callback fires during
        // fence; drive + ack it.
        mgr.transition_to_fenced(0).unwrap();
        assert!(!has_pending_reconcile_for_test(&mgr));
        let env = rx.recv().await.expect("lost event");
        if let BackgroundEvent::PartitionsRemoved { method_name, ack, .. } = env.event {
            assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
            ack.send(Ok(())).unwrap();
        } else {
            panic!("expected lost callback");
        }
        mgr.drive_release_to_completion().await.unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        // (a) of Issue 1.
        assert_ne!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));

        // (b) of Issue 1: fresh post-rejoin assignment reconciles.
        let topic3 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic3", topic3)]);
        receive_assignment(&mgr, topic3, vec![5]);
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic3, vec![5])])
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultDiscardedAfterCommitIfMemberRejoins`
    /// (`ConsumerMembershipManagerTest.java:566`).
    /// A member is stuck reconciling assignment A, parked on the REVOCATION
    /// COMMIT future (Java's `mockNewAssignmentAndRevocationStuckOnCommit`,
    /// test:576). While parked on the commit it gets fenced and rejoins;
    /// when the commit later completes (`commitResult.complete(null)`,
    /// test:591) the in-flight reconcile must be DISCARDED — no assignment
    /// applied, no ack sent.
    ///
    /// Unlike the sibling
    /// `delayed_reconciliation_result_discarded_after_partitions_revoked_callback_if_member_rejoins`
    /// (which parks on the revoked CALLBACK), this test parks specifically
    /// on the COMMIT future to exercise the rejoin-during-commit timing
    /// Java places at this park point. We therefore use a manager with a
    /// real `CommitRequestManager` (`make_with_commit_manager`) and seed a
    /// consumed offset so the auto-commit before rebalance actually
    /// enqueues a request the test controls.
    ///
    /// No listener: the §31 revoked/lost callbacks short-circuit, so the
    /// reconcile's ONLY park point is the commit future — the rejoin is
    /// guaranteed to land while the member is stalled on the commit.
    ///
    /// Mutation resistance: the discard relies on `maybe_abort_reconciliation`
    /// (step 10, after the commit await + revoked callback). Deleting that
    /// abort guard would let the empty-target reconcile transition to
    /// ACKNOWLEDGING, failing the `assert_ne!(.., Acknowledging)` below.
    #[tokio::test]
    async fn delayed_reconciliation_result_discarded_after_commit_if_member_rejoins() {
        let (mgr, _rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        // Seed a consumed offset so the auto-commit before rebalance
        // enqueues a real commit request (otherwise it short-circuits and
        // there is no commit to park on).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        // Empty assignment revokes owned topic1-0, triggering the
        // revocation commit the reconcile parks on.
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        // Wait until the reconcile has parked on the commit, i.e. the
        // commit manager has enqueued the unsent commit request AND the
        // reconcile has marked reconciliation in progress.
        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        loop {
            if commit_mgr.unsent_offset_commits_len_for_test() > 0 && reconciliation_in_progress(&mgr) {
                break;
            }
            tokio::task::yield_now().await;
        }

        // Rejoin via the fence path while still parked on the commit
        // (FENCED -> JOINING), mirroring Java's
        // `testFencedMemberReleasesAssignmentAndTransitionsToJoining`. With
        // no listener the onPartitionsLost callback short-circuits, so the
        // fence completes synchronously and sets
        // `rejoined_while_reconciliation_in_progress`.
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);

        // New assignment after rejoin (topic3-5).
        let topic3 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic3", topic3)]);
        receive_assignment(&mgr, topic3, vec![5]);

        // The commit completes AFTER the rejoin (Java: commitResult.complete(null)).
        // The in-flight reconcile must now be discarded.
        assert!(
            commit_mgr.complete_first_unsent_commit_for_test(HashMap::new()),
            "expected an unsent commit to complete",
        );
        bg.await.unwrap().unwrap();

        // Discarded: member did NOT advance to ACKNOWLEDGING (no ack sent)
        // and the in-flight reconcile was interrupted. The fence already
        // released the old assignment and transitioned to JOINING; the
        // post-rejoin target (topic3-5) is what is pending to reconcile
        // next — proving the stale empty-target reconcile was NOT applied.
        assert_ne!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic3, vec![5])])
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultAppliedWhenTargetChangedWithNewAssignment`.
    /// Counterpart to the discard tests: if the target changes (new
    /// assignment) WITHOUT a rejoin while a reconcile is in flight, the
    /// in-flight reconcile completes and is APPLIED (member reaches
    /// ACKNOWLEDGING), and the newly-added topic is reconciled in the
    /// next loop.
    #[tokio::test]
    async fn delayed_reconciliation_result_applied_when_target_changed_with_new_assignment() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1", "topic2"]);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic2", topic2)]);

        // Receive topic1-0, stuck on the assigned callback.
        receive_assignment(&mgr, topic1, vec![0]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        let env = rx.recv().await.expect("assigned event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(
                    added_partitions.iter().cloned().collect::<HashSet<_>>(),
                    HashSet::from([tp("topic1", 0)])
                );
                // Simulate the app applying the assignment before acking.
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(reconciliation_in_progress(&mgr));

        // New assignment adding topic2-0 — NO rejoin, so the in-flight
        // reconcile is still valid.
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);

        // Complete the stuck callback -> the first reconciliation APPLIES.
        stuck_ack.send(Ok(())).unwrap();
        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));
        // topic2 is pending for the next reconcile loop.
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic2, vec![0])])
        );

        // Ack -> RECONCILING (target not yet fully reached), reconcile the rest.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic2", 0)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 0), tp("topic2", 0)]));
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultAppliedWhenTargetChangedWithMetadataUpdate`.
    /// Same as the new-assignment variant but the target change comes from
    /// metadata discovering a previously-unresolved topic. The in-flight
    /// reconcile applies, then the newly-discovered topic reconciles next.
    #[tokio::test]
    async fn delayed_reconciliation_result_applied_when_target_changed_with_metadata_update() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1", "topic2"]);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);

        // Receive topic1-0 + topic2-0 (topic2 unresolved). Reconcile the
        // resolvable subset, stuck on the assigned callback for topic1-0.
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        let env = rx.recv().await.expect("assigned event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(
                    added_partitions.iter().cloned().collect::<HashSet<_>>(),
                    HashSet::from([tp("topic1", 0)])
                );
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(reconciliation_in_progress(&mgr));

        // Metadata discovers topic2 while the reconcile is in flight — no
        // rejoin, so the in-flight result is still valid.
        seed_metadata(&mgr, &[("topic1", topic1), ("topic2", topic2)]);

        // Complete the stuck callback -> first reconcile applies.
        stuck_ack.send(Ok(())).unwrap();
        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic2, vec![0])])
        );

        // Ack -> RECONCILING, then reconcile topic2.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Reconciling);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic2", 0)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
    }

    // ---------------------------------------------------------------
    // Commit 5: listener-callback ordering.
    //
    // In Rust the registered `ConsumerRebalanceListener` is invoked on
    // the application task (the test), NOT inside `reconcile` — reconcile
    // only enqueues the §31 callback-needed event and awaits the ack. So
    // Java's `listener.assignedCount()` becomes "number of
    // OnPartitionsAssigned events the test drained", and Java's
    // "listener throws" becomes "the test acks with Err". A listener must
    // still be REGISTERED (NoopListener via `make`/`subscribe_topics`) for
    // events to be enqueued (the §31 listener-presence short-circuit).
    // ---------------------------------------------------------------

    /// Drain exactly one callback-needed event, asserting its method and
    /// partitions, and ack it with the given result. Returns nothing.
    ///
    /// AK 4.3.1: dispatches on the reshaped event types — revoke/lost via
    /// `PartitionsRemoved`, assign via `PartitionsAssigned` (whose app-side
    /// processing is simulated here: the caller must pass `mgr` so the helper
    /// can `apply_assignment` before acking). `mgr` is `None` when the caller
    /// knows only revoke/lost events will be drained.
    async fn expect_callback(
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
        mgr: Option<&ConsumerMembershipManager>,
        expected_method: ConsumerRebalanceListenerMethodName,
        expected_partitions: &[TopicPartition],
        result: Result<(), KafkaError>,
    ) {
        let env = rx.recv().await.expect("expected a callback-needed event");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions } => {
                assert_eq!(method_name, expected_method, "unexpected callback method");
                let got: HashSet<TopicPartition> = partitions.into_iter().collect();
                let want: HashSet<TopicPartition> = expected_partitions.iter().cloned().collect();
                assert_eq!(got, want, "unexpected callback partitions");
                ack.send(result).unwrap();
            },
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(
                    expected_method,
                    ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                    "PartitionsAssigned received but expected method was not OnPartitionsAssigned"
                );
                let got: HashSet<TopicPartition> = added_partitions.iter().cloned().collect();
                let want: HashSet<TopicPartition> = expected_partitions.iter().cloned().collect();
                assert_eq!(got, want, "unexpected added partitions");
                if let Some(mgr) = mgr {
                    let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                    mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                }
                ack.send(result).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testListenerCallbacksBasic`.
    /// Full assign -> ack -> revoke+assign(empty) -> ack cycle, asserting
    /// the callback methods/partitions and resulting state. (Java asserts
    /// listener invocation counts; in Rust the test IS the listener, so
    /// we assert the exact sequence of callback events instead.)
    #[tokio::test]
    async fn listener_callbacks_basic() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        // Assign {0, 1}.
        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        // Step 3: onPartitionsAssigned {0,1}.
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        expect_callback(
            &mut rx,
            Some(&*mgr),
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();
        assert!(!reconciliation_in_progress(&mgr));

        // Step 4: ack -> STABLE.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        let mut current = mgr.current_assignment().partitions;
        for v in current.values_mut() {
            v.sort_unstable();
        }
        assert_eq!(current, HashMap::from([(topic_id, vec![0, 1])]));

        // Step 5: empty assignment -> revoke.
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        // Step 6: onPartitionsRevoked {0,1}.
        expect_callback(
            &mut rx,
            None,
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            &[tp("topic1", 0), tp("topic1", 1)],
            Ok(()),
        )
        .await;
        // Step 7: onPartitionsAssigned {} (still called, even though empty).
        expect_callback(
            &mut rx,
            Some(&*mgr),
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();

        // Step 8: ack -> STABLE.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        assert!(!reconciliation_in_progress(&mgr));
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testListenerCallbacksThrowsErrorOnPartitionsRevoked`.
    /// When the onPartitionsRevoked callback errors, reconcile returns the
    /// error and the rebalance does NOT advance (stays RECONCILING,
    /// onPartitionsAssigned is NOT called). Looped over multiple error
    /// kinds, mirroring Java's three error types.
    #[tokio::test]
    async fn listener_callbacks_throws_error_on_partitions_revoked() {
        // Java loops over WakeupException, InterruptException,
        // IllegalArgumentException. We mirror with three KafkaError kinds.
        let errors: Vec<fn() -> KafkaError> = vec![
            || KafkaError::wakeup("Intentional onPartitionsRevoked() error"),
            || KafkaError::timeout("Intentional onPartitionsRevoked() error"),
            || KafkaError::illegal_argument("Intentional onPartitionsRevoked() error"),
        ];
        for make_err in errors {
            let (mgr, mut rx) = make(None, None, None);
            subscribe_topics(&mgr, &["topic1"]);
            mgr.transition_to_joining().unwrap();
            let topic_id = Uuid::random_uuid();
            seed_metadata(&mgr, &[("topic1", topic_id)]);
            // Own topic1-0 so an empty assignment revokes it.
            mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

            receive_empty_assignment(&mgr);
            assert_eq!(mgr.state(), MemberState::Reconciling);

            let mgr = Arc::new(mgr);
            let mgr_clone = mgr.clone();
            let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
            // onPartitionsRevoked {0} -> ack with error.
            expect_callback(
                &mut rx,
                None,
                ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                &[tp("topic1", 0)],
                Err(make_err()),
            )
            .await;
            let result = bg.await.unwrap();
            // Error propagated, rebalance did not advance.
            assert!(result.is_err(), "revoked-callback error must propagate");
            assert_eq!(
                mgr.state(),
                MemberState::Reconciling,
                "rebalance must not advance after a failed onPartitionsRevoked callback",
            );
            // onPartitionsAssigned must NOT have been called.
            assert!(
                matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
                "onPartitionsAssigned must not be called after onPartitionsRevoked failed",
            );
        }
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testAddedPartitionsTemporarilyDisabledAwaitingOnPartitionsAssignedCallback`.
    /// A newly-added partition is non-fetchable while awaiting the
    /// onPartitionsAssigned callback, and becomes fetchable once the
    /// callback completes (enable_partitions_awaiting_callback).
    #[tokio::test]
    async fn added_partitions_temporarily_disabled_awaiting_on_partitions_assigned_callback() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        // Own topic1-0; assignment adds topic1-1.
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        // The PartitionsAssigned event for the ADDED partition {1} is now
        // pending. Simulate the app applying the assignment (which marks the
        // added partition as awaiting the callback) before the assertion.
        let env = rx.recv().await.expect("assigned event");
        let ack = match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                assert_eq!(
                    added_partitions.iter().cloned().collect::<HashSet<_>>(),
                    HashSet::from([tp("topic1", 1)])
                );
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        // Before the callback completes, the added partition is in the
        // assignment but NOT fetchable. Give it a valid position first so
        // the ONLY remaining gate is pending_on_assigned_callback — this
        // is what makes the assertion mutation-resistant (without the
        // disable, the partition would already be fetchable here).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            assert!(subs.assigned_partitions().contains(&tp("topic1", 1)));
            subs.seek(&tp("topic1", 1), 0).unwrap();
            assert!(
                !subs.is_fetchable(&tp("topic1", 1)),
                "added partition must be disabled awaiting callback even with a valid position",
            );
        }

        // Complete the callback -> partition enabled (pending gate cleared).
        ack.send(Ok(())).unwrap();
        bg.await.unwrap().unwrap();
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(
            subs.is_fetchable(&tp("topic1", 1)),
            "added partition must be fetchable after callback"
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testAddedPartitionsNotEnabledAfterFailedOnPartitionsAssignedCallback`.
    /// If the onPartitionsAssigned callback fails, the added partition is
    /// NOT enabled (stays non-fetchable).
    #[tokio::test]
    async fn added_partitions_not_enabled_after_failed_on_partitions_assigned_callback() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        let env = rx.recv().await.expect("assigned event");
        let ack = match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                // App applies the assignment (marks added awaiting callback)
                // before the callback runs and fails.
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        // Fail the assigned callback.
        ack.send(Err(KafkaError::illegal_state("onPartitionsAssigned failed!")))
            .unwrap();
        let result = bg.await.unwrap();
        assert!(result.is_err());

        // Added partition remains non-fetchable (not enabled). Give it a
        // valid position so the ONLY thing keeping it non-fetchable is the
        // still-set pending_on_assigned_callback gate.
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 1), 0).unwrap();
        }
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(
            !subs.is_fetchable(&tp("topic1", 1)),
            "added partition must NOT be enabled after a failed onPartitionsAssigned callback",
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testOnPartitionsLostNoError`.
    /// Fencing a member that owns a partition fires onPartitionsLost; the
    /// member clears its assignment and rejoins (JOINING).
    #[tokio::test]
    async fn on_partitions_lost_no_error() {
        on_partitions_lost_impl(Ok(())).await;
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testOnPartitionsLostError`.
    /// Even if the onPartitionsLost callback errors, the member still
    /// clears its assignment and rejoins. Looped over multiple error kinds.
    #[tokio::test]
    async fn on_partitions_lost_error() {
        on_partitions_lost_impl(Err(KafkaError::illegal_state("Intentional error for test"))).await;
        on_partitions_lost_impl(Err(KafkaError::wakeup("Intentional error for test"))).await;
        on_partitions_lost_impl(Err(KafkaError::timeout("Intentional error for test"))).await;
    }

    async fn on_partitions_lost_impl(callback_result: Result<(), KafkaError>) {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        // Fence -> onPartitionsLost for owned {0}. Phase 41 Issue 2: the
        // fence enqueues the §31 onPartitionsLost callback non-blockingly and
        // returns; the release tail runs in drive_pending_release once the
        // listener acks.
        mgr.transition_to_fenced(0).unwrap();
        expect_callback(
            &mut rx,
            None,
            ConsumerRebalanceListenerMethodName::OnPartitionsLost,
            &[tp("topic1", 0)],
            callback_result,
        )
        .await;
        mgr.drive_release_to_completion().await.unwrap();

        // Assignment cleared; member rejoined (JOINING) regardless of the
        // callback result.
        assert!(mgr.current_assignment().is_none());
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// AK 4.3.1 (KAFKA-20321): `transition_to_fenced` marks the owned
    /// partitions pending-revocation (pausing fetching) BEFORE enqueuing the
    /// `onPartitionsLost` callback event. Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToFencedMarksPendingRevocationBeforeSignalingPartitionsLost`.
    ///
    /// Java asserts the ordering via a Mockito `InOrder`
    /// (`markPendingRevocation` then `backgroundEventHandler.add`). With the
    /// real `SubscriptionState`, the observable is: after the transition the
    /// owned partition (given a valid position) is no longer fetchable — the
    /// only remaining false-reason is the pending-revocation flag — AND the
    /// `PartitionsRemoved(ON_PARTITIONS_LOST)` event is enqueued.
    #[tokio::test]
    async fn transition_to_fenced_marks_pending_revocation_before_signaling_partitions_lost() {
        assert_marks_pending_revocation_before_lost(ReleaseTransition::Fenced).await;
    }

    /// AK 4.3.1 (KAFKA-20321). Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToFatalMarksPendingRevocationBeforeSignalingPartitionsLost`.
    #[tokio::test]
    async fn transition_to_fatal_marks_pending_revocation_before_signaling_partitions_lost() {
        assert_marks_pending_revocation_before_lost(ReleaseTransition::Fatal).await;
    }

    /// AK 4.3.1 (KAFKA-20321). Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToStaleMarksPendingRevocationBeforeSignalingPartitionsLost`.
    #[tokio::test]
    async fn transition_to_stale_marks_pending_revocation_before_signaling_partitions_lost() {
        assert_marks_pending_revocation_before_lost(ReleaseTransition::Stale).await;
    }

    #[derive(Clone, Copy)]
    enum ReleaseTransition {
        Fenced,
        Fatal,
        Stale,
    }

    async fn assert_marks_pending_revocation_before_lost(kind: ReleaseTransition) {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        // Give the owned partition a valid position so the ONLY thing that
        // could make it non-fetchable after the transition is the
        // pending-revocation flag.
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 0).unwrap();
            assert!(
                subs.is_fetchable(&tp("topic1", 0)),
                "precondition: fetchable before the release transition"
            );
        }

        match kind {
            ReleaseTransition::Fenced => mgr.transition_to_fenced(0).unwrap(),
            ReleaseTransition::Fatal => {
                mgr.transition_to_fatal(0).unwrap();
            },
            ReleaseTransition::Stale => {
                // Java transitions to LEAVING (via the poll timer) before STALE.
                mgr.transition_to_sending_leave_group(true).unwrap();
                mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
                assert_eq!(mgr.state(), MemberState::Stale);
                mgr.transition_to_stale(0).unwrap();
            },
        }

        // markPendingRevocation ran BEFORE the callback: the partition is now
        // non-fetchable even though the lost callback ack has not been sent.
        {
            let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            assert!(
                !subs.is_fetchable(&tp("topic1", 0)),
                "partition must be pending revocation (fetch paused) before the onPartitionsLost callback",
            );
        }

        // ...and the PartitionsRemoved(ON_PARTITIONS_LOST) event was enqueued.
        let env = rx.recv().await.expect("lost event");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
            },
            other => panic!("expected PartitionsRemoved(ON_PARTITIONS_LOST), got {other:?}"),
        }
    }

    /// AK 4.3.1 (KAFKA-20428): when unsubscribe/leaveGroup is called during an
    /// ongoing reconciliation and the pending PartitionsAssigned event is
    /// completed exceptionally (the app skips it), the member can still rejoin
    /// and start a new reconciliation. Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupDuringReconciliationThenRejoin`.
    #[tokio::test]
    async fn leave_group_during_reconciliation_then_rejoin() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        // Owned empty; receive an assignment and start reconciling.
        receive_assignment(&mgr, topic_id, vec![0]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Start reconciliation — parks on the PartitionsAssigned event.
        reconcile_once(&mgr, true).await.unwrap();
        let ack = match rx.recv().await.expect("PartitionsAssigned event").event {
            BackgroundEvent::PartitionsAssigned { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(has_pending_reconcile_for_test(&mgr));

        // Leave group while reconciliation is in progress -> LEAVING.
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);

        // Complete the pending assignment event exceptionally (simulating the
        // app skipping it during unsubscribe, KAFKA-20428).
        ack.send(Err(KafkaError::with_message(
            Errors::UnknownServerError,
            "Assignment event skipped because consumer is unsubscribing",
        )))
        .unwrap();
        // Drive the parked reconcile so the exceptional ack clears the pending
        // state (continue_after_assign observes the callback error and returns).
        let _ = mgr.reconcile(0, true).await;
        assert!(!has_pending_reconcile_for_test(&mgr));

        // Complete the leave and rejoin.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.abstract_mm.on_subscription_updated();
        mgr.abstract_mm.on_consumer_poll(mgr.join_group_epoch()).unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);

        // Receive an assignment again — a NEW reconciliation can start (it is
        // not gated on the skipped one).
        receive_assignment(&mgr, topic_id, vec![0]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        reconcile_once(&mgr, true).await.unwrap();
        let env = rx.recv().await.expect("new PartitionsAssigned after rejoin");
        assert!(
            matches!(env.event, BackgroundEvent::PartitionsAssigned { .. }),
            "the fresh reconciliation must enqueue a PartitionsAssigned event",
        );
    }

    /// Phase 41 Issue 2 regression: a release transition
    /// (`transition_to_fenced`, representative of fatal/stale) that fires
    /// `onPartitionsLost` must NOT block awaiting the listener ack — it
    /// enqueues the §31 callback, stores a `PendingRelease`, and RETURNS
    /// immediately. This is the property that frees the bg loop (Phase 2.4)
    /// to keep spinning and service a reentrant handle op submitted from
    /// inside `on_partitions_lost` (proven end-to-end by
    /// `handle_reentrant_op_completes_through_bg_pipeline` at the consumer
    /// level). Here we assert the manager-level non-blocking contract: with
    /// the callback ack deliberately NOT sent, `transition_to_fenced`
    /// completes promptly and the member is left in FENCED with a pending
    /// release (not parked inside the transition).
    #[tokio::test]
    async fn release_transition_does_not_block_on_callback_ack() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        // Fence: enqueues onPartitionsLost for the owned partition and stores
        // PendingRelease::Fenced. The ack is NEVER sent in this test.
        mgr.transition_to_fenced(0).unwrap();

        // The transition returned WITHOUT the release tail running: the
        // member is still FENCED (not yet JOINING) and a release is pending.
        // This is what keeps the bg loop free to service reentrant handle ops
        // while the listener runs. The owned partition is still in
        // SubscriptionState (the release tail has not cleared it yet).
        assert_eq!(mgr.state(), MemberState::Fenced);
        assert!(mgr.has_pending_release());
        assert!(
            mgr.abstract_mm
                .subscriptions
                .lock()
                .unwrap()
                .assigned_partitions()
                .contains(&tp("topic1", 0)),
            "owned partition must still be assigned until the release tail runs",
        );

        // The §31 onPartitionsLost event was enqueued (the app side would run
        // the listener — and any reentrant handle op it submits is serviced by
        // the still-spinning bg loop). Acking + driving the release advances
        // the member to JOINING.
        let env = rx.recv().await.expect("onPartitionsLost event");
        if let BackgroundEvent::PartitionsRemoved { method_name, ack, .. } = env.event {
            assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
            ack.send(Ok(())).unwrap();
        } else {
            panic!("expected onPartitionsLost callback");
        }
        mgr.drive_release_to_completion().await.unwrap();
        assert!(!mgr.has_pending_release());
        assert_eq!(mgr.state(), MemberState::Joining);
        assert!(mgr.current_assignment().is_none());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testMemberJoiningCallsRebalanceListenerWhenReceivingEmptyAssignment`.
    /// A joining member that receives an empty assignment still fires the
    /// onPartitionsAssigned callback (with an empty set).
    #[tokio::test]
    async fn member_joining_calls_rebalance_listener_when_receiving_empty_assignment() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        // onPartitionsAssigned with an empty set is still emitted.
        expect_callback(
            &mut rx,
            Some(&*mgr),
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
    }

    // ---------------------------------------------------------------
    // Commit 6: leave / fatal matrices + misc.
    //
    // Rust's `leave_group()` is linearized (no separate CompletableFuture
    // returned). Java assertions on `leaveResult.isDone()` map to state
    // transitions: leave_group -> LEAVING, then
    // on_heartbeat_request_generated -> UNSUBSCRIBED, and the leave
    // "completes" when the leave heartbeat response is processed.
    // ---------------------------------------------------------------

    /// Bring a member to STABLE via join -> empty assignment -> reconcile
    /// -> ack, mirroring Java's `createMemberInStableState`. No listener
    /// events fire (empty assignment, no owned partitions, NoopListener
    /// short-circuits since there's nothing to revoke/assign on the real
    /// SubscriptionState).
    async fn create_member_in_stable_state(
        group_instance_id: Option<String>,
    ) -> (
        Arc<ConsumerMembershipManager>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let (mgr, mut rx) = make(group_instance_id, None, None);
        mgr.transition_to_joining().unwrap();
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        let mgr = Arc::new(mgr);
        // Empty assignment with a listener registered still enqueues an
        // onPartitionsAssigned({}) event; drive+ack it.
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });
        expect_callback(
            &mut rx,
            Some(&*mgr),
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        (mgr, rx)
    }

    /// Build a successful (empty assignment) heartbeat response with the
    /// given member epoch.
    fn heartbeat_response(member_id: String, epoch: i32) -> ConsumerGroupHeartbeatResponse {
        use crate::consumer_group_heartbeat_response_data::{Assignment, ConsumerGroupHeartbeatResponseData};
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(member_id);
        data.member_epoch = epoch;
        data.heartbeat_interval_ms = 5000;
        data.assignment = Some(Assignment { topic_partitions: vec![], unknown_tagged_fields: vec![] });
        ConsumerGroupHeartbeatResponse::new(data)
    }

    /// Build a leave-group heartbeat response (epoch == LEAVE_GROUP_MEMBER_EPOCH).
    fn leave_response(member_id: String) -> ConsumerGroupHeartbeatResponse {
        use crate::consumer_group_heartbeat_response_data::ConsumerGroupHeartbeatResponseData;
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(member_id);
        data.member_epoch = LEAVE_GROUP_MEMBER_EPOCH;
        ConsumerGroupHeartbeatResponse::new(data)
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenStateIsStable`.
    #[tokio::test]
    async fn leave_group_when_state_is_stable() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.current_assignment().is_none());

        // Leave heartbeat sent -> UNSUBSCRIBED.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        // Leave response received -> remains UNSUBSCRIBED.
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberOwnsAssignment`.
    /// A member owning a partition fires onPartitionsRevoked during the
    /// leave, then transitions to LEAVING.
    #[tokio::test]
    async fn leave_group_when_member_owns_assignment() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        let mgr = Arc::new(mgr);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
        )
        .await;
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        assert_eq!(mgr.current_assignment().partitions.len(), 1);

        // Leave: owned partitions fire onPartitionsRevoked.
        let mgr_clone = mgr.clone();
        let leave = tokio::spawn(async move { mgr_clone.leave_group(0).await });
        expect_callback(
            &mut rx,
            None,
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            &[tp("topic1", 0), tp("topic1", 1)],
            Ok(()),
        )
        .await;
        leave.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.current_assignment().is_none());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberAlreadyLeaving`.
    /// A second leave while LEAVING is a no-op.
    #[tokio::test]
    async fn leave_group_when_member_already_leaving() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);

        // Second leave while still LEAVING: no-op, state unchanged.
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberAlreadyLeft`.
    /// After fully leaving (UNSUBSCRIBED), a further leave is a no-op
    /// (unsubscribes, no callbacks).
    #[tokio::test]
    async fn leave_group_when_member_already_left() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // Leave again when already left -> no-op, stays UNSUBSCRIBED.
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberFenced`.
    /// Leaving from FENCED clears the assignment and goes to UNSUBSCRIBED.
    #[tokio::test]
    async fn leave_group_when_member_fenced() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        // No owned partitions -> fence short-circuits the §31 lost
        // callback and goes FENCED -> JOINING; to land in FENCED we drive
        // a fence while owning a partition is more complex, so instead we
        // force the FENCED state directly via the inner guard (mirroring
        // Java's `mockFencedMemberStuckOnUserCallback` which parks in
        // FENCED). The leave then unsubscribes from FENCED.
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.transition_to(MemberState::Fenced).unwrap();
        }
        assert_eq!(mgr.state(), MemberState::Fenced);

        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(mgr.current_assignment().is_none());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFatalFailureWhenStateIsStable`.
    #[tokio::test]
    async fn fatal_failure_when_state_is_stable() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        let member_id = mgr.member_id();
        let last_epoch = mgr.member_epoch();
        mgr.on_heartbeat_failure(false);
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
        // Keeps its last member id and epoch.
        assert_eq!(mgr.member_id(), member_id);
        assert_eq!(mgr.member_epoch(), last_epoch);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFatalFailureWhenStateIsPrepareLeaving`.
    /// A member stuck in PREPARE_LEAVING (on the revoked callback) that
    /// gets a fatal error transitions to FATAL; when the callback
    /// completes the member remains FATAL (leave aborted).
    #[tokio::test]
    async fn fatal_failure_when_state_is_prepare_leaving() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        // Member epoch > 0 so the leave releases the assignment via
        // onPartitionsRevoked (not onPartitionsLost), mirroring Java's
        // stable member.
        {
            let mut guard = mgr.abstract_mm.inner.lock().unwrap();
            guard.update_member_epoch(1);
        }
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        let mgr = Arc::new(mgr);
        // leave_group with owned partitions parks PREPARE_LEAVING on the
        // revoked callback.
        let mgr_clone = mgr.clone();
        let leave = tokio::spawn(async move { mgr_clone.leave_group(0).await });
        let env = rx.recv().await.expect("revoked event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // Fatal error while in PREPARE_LEAVING.
        mgr.on_heartbeat_failure(false);
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);

        // Complete the stuck callback -> the leave is aborted; remains FATAL.
        stuck_ack.send(Ok(())).unwrap();
        leave.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFatalFailureWhenStateIsLeaving`.
    /// A member in LEAVING that gets a fatal error transitions to FATAL;
    /// a subsequent heartbeat-generated does not move it out of FATAL.
    #[tokio::test]
    async fn fatal_failure_when_state_is_leaving() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);

        mgr.on_heartbeat_failure(false);
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);

        // The last heartbeat won't be sent because the member already failed.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFatalFailureWhenMemberAlreadyLeft`.
    /// A member that already left (UNSUBSCRIBED) and then gets a fatal
    /// error transitions to FATAL with no callbacks.
    #[tokio::test]
    async fn fatal_failure_when_member_already_left() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        // Last heartbeat sent.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // Fatal failure received after the member already left -> FATAL,
        // no callbacks (no onPartitionsLost).
        mgr.on_heartbeat_failure(false);
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave`.
    /// LEAVING -> (heartbeat generated) UNSUBSCRIBED; a non-leave response
    /// is ignored, the leave response leaves the member UNSUBSCRIBED with
    /// epoch -1 and no assignment.
    #[tokio::test]
    async fn heartbeat_successful_response_when_leaving_group_completes_leave() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);

        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // A non-leave success response is ignored (member already
        // UNSUBSCRIBED with a positive epoch in the response).
        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), 1)).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // The leave response completes the leave.
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.current_assignment().is_none());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave`.
    /// LEAVING -> (heartbeat generated) UNSUBSCRIBED; a failed response
    /// (retriable or not) still completes the leave. Parameterized over
    /// [true, false].
    #[tokio::test]
    async fn heartbeat_failed_response_when_leaving_group_completes_leave() {
        for retriable in [true, false] {
            let (mgr, _rx) = create_member_in_stable_state(None).await;
            mgr.leave_group(0).await.unwrap();
            assert_eq!(mgr.state(), MemberState::Leaving);

            mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
            assert_eq!(mgr.state(), MemberState::Unsubscribed);

            mgr.on_heartbeat_failure(retriable);
            // Member remains UNSUBSCRIBED; the leave is complete.
            assert_eq!(mgr.state(), MemberState::Unsubscribed);
            assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
            assert!(mgr.current_assignment().is_none());
        }
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testIgnoreHeartbeatResponseWhenNotInGroup`.
    /// A heartbeat response received while the member is not in the group
    /// (UNSUBSCRIBED/FENCED/FATAL/STALE) is ignored — the state does not
    /// change. Parameterized over the not-in-group states.
    #[tokio::test]
    async fn ignore_heartbeat_response_when_not_in_group() {
        let not_in_group = [
            MemberState::Unsubscribed,
            MemberState::Fenced,
            MemberState::Fatal,
            MemberState::Stale,
        ];
        for state in not_in_group {
            let (mgr, _rx) = make(None, None, None);
            // Force the manager into the given not-in-group state via the
            // inner guard (bypassing transition validity — these states
            // are reached via different paths in production; the test
            // only needs the member parked there).
            {
                let mut guard = mgr.abstract_mm.inner.lock().unwrap();
                guard.state = state;
            }
            // A response with a positive epoch + an assignment should be
            // ignored; the state must be unchanged.
            mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), 5)).unwrap();
            assert_eq!(mgr.state(), state, "response must be ignored in {state:?}");
        }
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testIgnoreLeaveResponseWhenNotLeavingGroup`.
    /// After the member has sent its leave HB (UNSUBSCRIBED), a stale
    /// non-leave response is ignored; the leave response completes the
    /// leave; a subsequent onSubscriptionUpdated + onConsumerPoll +
    /// leave-response rejoins (JOINING, epoch 0).
    #[tokio::test]
    async fn ignore_leave_response_when_not_leaving_group() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.leave_group(0).await.unwrap();
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // A previous (non-leave) heartbeat response is ignored.
        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), 1)).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // The leave response is processed (still UNSUBSCRIBED).
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // Subscription updated + poll -> rejoin.
        mgr.abstract_mm.on_subscription_updated();
        mgr.abstract_mm.on_consumer_poll(mgr.join_group_epoch()).unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        assert_eq!(mgr.member_epoch(), 0);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFencingWhenStateIsPrepareLeavingCompletesTheLeaveOperation`.
    /// A member stuck in PREPARE_LEAVING gets fenced; it transitions to
    /// UNSUBSCRIBED and the ongoing leave completes.
    #[tokio::test]
    async fn fencing_when_state_is_prepare_leaving_completes_the_leave_operation() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let leave = tokio::spawn(async move { mgr_clone.leave_group(0).await });
        let env = rx.recv().await.expect("revoked event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // Fence while preparing to leave -> UNSUBSCRIBED.
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // Completing the callback finishes the leave; remains UNSUBSCRIBED.
        stuck_ack.send(Ok(())).unwrap();
        leave.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testUpdateStateFailsOnResponsesWithErrors`.
    /// A heartbeat response containing an error code must fail
    /// `on_heartbeat_success` with an illegal-argument error.
    #[test]
    fn update_state_fails_on_responses_with_errors() {
        use crate::consumer_group_heartbeat_response_data::ConsumerGroupHeartbeatResponseData;
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::UnknownMemberId.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 5;
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        let err = mgr.on_heartbeat_success(&resp).unwrap_err();
        // Error message content is part of the contract (DoD §3).
        assert!(
            err.to_string().contains("Unexpected error in Heartbeat response"),
            "unexpected error message: {err}",
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup`.
    /// onSubscriptionUpdated + onConsumerPoll while already in the group
    /// (STABLE) does NOT transition to JOINING; the subscription-updated
    /// flag is consumed.
    #[tokio::test]
    async fn on_subscription_updated_does_not_transition_to_joining_if_in_group() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.abstract_mm.on_subscription_updated();
        assert!(mgr.abstract_mm.inner.lock().unwrap().subscription_updated);
        mgr.abstract_mm.on_consumer_poll(mgr.join_group_epoch()).unwrap();
        // Still STABLE (in-group): no transition to JOINING.
        assert_eq!(mgr.state(), MemberState::Stable);
        assert!(!mgr.abstract_mm.inner.lock().unwrap().subscription_updated);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToReconcilingIfEmptyAssignmentReceived`.
    /// An empty assignment with a new epoch keeps us in RECONCILING.
    #[test]
    fn on_heartbeat_success_empty_assignment_transitions_to_reconciling() {
        use crate::consumer_group_heartbeat_response_data::{Assignment, ConsumerGroupHeartbeatResponseData};

        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);

        // Build a response with an empty assignment (Some<Assignment>
        // with empty topic_partitions).
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 1;
        data.heartbeat_interval_ms = 5000;
        data.assignment = Some(Assignment { topic_partitions: vec![], unknown_tagged_fields: vec![] });
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        mgr.on_heartbeat_success(&resp).unwrap();
        // Empty assignment for a JOINING member: target changes (epoch
        // bumps via update_with), so we transition to RECONCILING.
        assert_eq!(mgr.state(), MemberState::Reconciling);
    }

    // ===============================================================
    // Phase 35 — STALE member path
    // (ConsumerMembershipManagerTest stale-member family).
    // ===============================================================

    /// Drive a member from STABLE into ACKNOWLEDGING by receiving and
    /// reconciling an owned partition. Mirrors Java's
    /// `mockJoinAndReceiveAssignment(true)` tail (leaves the member in
    /// ACKNOWLEDGING after the assigned callback completes).
    async fn create_member_acknowledging(
        mut rx: mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
        mgr: Arc<ConsumerMembershipManager>,
        topic_id: Uuid,
    ) -> mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope> {
        receive_assignment(&mgr, topic_id, vec![0]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        reconcile_and_complete_callback(
            mgr.clone(),
            &mut rx,
            true,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0)],
        )
        .await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        rx
    }

    /// Java helper `assertLeaveGroupDueToExpiredPollAndTransitionToStale`:
    /// `transitionToSendingLeaveGroup(true)` resets epoch to LEAVE, then
    /// `onHeartbeatRequestGenerated()` transitions the member to STALE.
    /// (The async onPartitionsLost release is driven separately via
    /// `transition_to_stale` for owned-partition cases.)
    fn leave_group_due_to_expired_poll_and_transition_to_stale(mgr: &ConsumerMembershipManager) {
        mgr.transition_to_sending_leave_group(true).unwrap();
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stale);
    }

    /// Java helper `assertStaleMemberLeavesGroupAndClearsAssignment` for the
    /// no-owned-partition case (assignment already none after
    /// `transitionToSendingLeaveGroup` sets `current_assignment = NONE`).
    fn assert_stale_member_leaves_group_and_clears_assignment(mgr: &ConsumerMembershipManager) {
        assert_eq!(mgr.state(), MemberState::Stale);
        assert!(mgr.current_assignment().is_none());
        assert!(topics_awaiting_reconciliation(mgr).is_empty());
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToLeavingWhileReconcilingDueToStaleMember`.
    #[tokio::test]
    async fn transition_to_leaving_while_reconciling_due_to_stale_member() {
        // Reach RECONCILING with a fresh (un-reconciled) target assignment.
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        receive_assignment(&mgr, topic_id, vec![0]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        leave_group_due_to_expired_poll_and_transition_to_stale(&mgr);
        assert_stale_member_leaves_group_and_clears_assignment(&mgr);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToLeavingWhileJoiningDueToStaleMember`.
    #[tokio::test]
    async fn transition_to_leaving_while_joining_due_to_stale_member() {
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);

        leave_group_due_to_expired_poll_and_transition_to_stale(&mgr);
        assert_stale_member_leaves_group_and_clears_assignment(&mgr);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToLeavingWhileStableDueToStaleMember`.
    #[tokio::test]
    async fn transition_to_leaving_while_stable_due_to_stale_member() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        assert_eq!(mgr.state(), MemberState::Stable);

        leave_group_due_to_expired_poll_and_transition_to_stale(&mgr);
        assert_stale_member_leaves_group_and_clears_assignment(&mgr);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testTransitionToLeavingWhileAcknowledgingDueToStaleMember`.
    #[tokio::test]
    async fn transition_to_leaving_while_acknowledging_due_to_stale_member() {
        let (mgr, rx) = make(None, None, None);
        subscribe_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        let mgr = Arc::new(mgr);
        let _rx = create_member_acknowledging(rx, mgr.clone(), topic_id).await;
        assert_eq!(mgr.state(), MemberState::Acknowledging);

        leave_group_due_to_expired_poll_and_transition_to_stale(&mgr);
        assert_eq!(mgr.state(), MemberState::Stale);
        // Acknowledging member owned topic1-0; `transitionToSendingLeaveGroup`
        // clears `current_assignment` to NONE, so the assignment is already
        // released from the membership manager's view.
        assert!(mgr.current_assignment().is_none());
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover`.
    #[tokio::test]
    async fn stale_member_does_not_send_heartbeat_and_allows_transition_to_joining_to_recover() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        mgr.transition_to_sending_leave_group(true).unwrap();
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stale);

        // Stale member should not send heartbeats.
        assert!(
            mgr.abstract_mm.inner.lock().unwrap().should_skip_heartbeat(),
            "Stale member should not send heartbeats"
        );

        // Run the STALE assignment release (no owned partitions ⇒ a no-op;
        // mirrors Java's `staleMemberAssignmentRelease` empty-partition future
        // completing immediately, clearing the release-pending flag).
        mgr.transition_to_stale(0).unwrap();

        // Java asserts only that `maybeRejoinStaleMember` does not throw. With
        // the release complete, the member is now allowed to transition to
        // JOINING when the poll timer is reset.
        mgr.abstract_mm.maybe_rejoin_stale_member(mgr.join_group_epoch());
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// Drive a member to STALE with NO owned partitions (mirrors Java
    /// `mockStaleMember`).
    async fn mock_stale_member() -> (
        Arc<ConsumerMembershipManager>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let (mgr, rx) = create_member_in_stable_state(None).await;
        mgr.transition_to_sending_leave_group(true).unwrap();
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        // No owned partitions ⇒ the STALE release is a no-op; the
        // release-pending flag is cleared by transition_to_stale (drive it so
        // the member is not left with a stale pending flag).
        mgr.transition_to_stale(0).unwrap();
        (mgr, rx)
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testStaleMemberRejoinsWhenTimerResetsNoCallbacks`.
    #[tokio::test]
    async fn stale_member_rejoins_when_timer_resets_no_callbacks() {
        let (mgr, _rx) = mock_stale_member().await;
        assert_stale_member_leaves_group_and_clears_assignment(&mgr);

        mgr.abstract_mm.maybe_rejoin_stale_member(mgr.join_group_epoch());
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testStaleMemberWaitsForCallbackToRejoinWhenTimerReset`.
    /// A STALE member that owns a partition fires onPartitionsLost; the timer
    /// reset while the callback is in flight must NOT advance the member out
    /// of STALE — it rejoins (JOINING) only once the callback completes.
    #[tokio::test]
    async fn stale_member_waits_for_callback_to_rejoin_when_timer_reset() {
        let (mgr, mut rx) = create_member_in_stable_state(None).await;
        // Own a partition so onPartitionsLost has something to release.
        let topic_name = "topic1";
        let owned = tp(topic_name, 0);
        mock_owned_partitions(&mgr, std::slice::from_ref(&owned));

        // LEAVING due to expired poll timer, then STALE.
        mgr.transition_to_sending_leave_group(true).unwrap();
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stale);

        // Phase 41 Issue 2: transition_to_stale enqueues an onPartitionsLost
        // callback-needed event and stores PendingRelease::Stale
        // non-blockingly (it does NOT park). The release tail (clear
        // assignment + rejoin) runs in drive_pending_release once the
        // listener acks.
        mgr.transition_to_stale(0).unwrap();
        assert!(mgr.has_pending_release());

        // Capture the callback-needed event WITHOUT acking it yet.
        let env = rx.recv().await.expect("expected onPartitionsLost callback-needed event");
        let ack = match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
                let got: HashSet<TopicPartition> = partitions.into_iter().collect();
                assert_eq!(got, [owned.clone()].into_iter().collect::<HashSet<_>>());
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };

        // Timer reset while the callback has NOT completed: the member must
        // stay STALE (must not clear its assignment to rejoin yet). The
        // release-pending flag is still set (continue_after_stale_release has
        // not run), so maybe_rejoin_stale_member defers.
        mgr.abstract_mm.maybe_rejoin_stale_member(mgr.join_group_epoch());
        assert_eq!(
            mgr.state(),
            MemberState::Stale,
            "member must not leave STALE while the onPartitionsLost callback is in flight"
        );

        // Complete the callback and drive the release tail: it clears the
        // assignment and (because a rejoin was requested) transitions to
        // JOINING.
        ack.send(Ok(())).unwrap();
        mgr.drive_release_to_completion().await.unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        assert!(mgr.current_assignment().is_none());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberIsStale`.
    /// A STALE member's `leave_group()` unsubscribes but the member stays
    /// STALE (it has already left the group due to the expired poll timer).
    #[tokio::test]
    async fn leave_group_when_member_is_stale() {
        let (mgr, _rx) = mock_stale_member().await;
        assert_eq!(mgr.state(), MemberState::Stale);

        mgr.leave_group(0).await.unwrap();
        // SubscriptionState was unsubscribed.
        assert!(mgr.abstract_mm.subscriptions.lock().unwrap().subscription().is_empty());
        assert_eq!(mgr.state(), MemberState::Stale);
    }
}
