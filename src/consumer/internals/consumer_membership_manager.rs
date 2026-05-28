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

use crate::common::{KafkaError, TopicPartition, Uuid};
use crate::common::requests::consumer_group_heartbeat_request::{
    JOIN_GROUP_MEMBER_EPOCH, LEAVE_GROUP_MEMBER_EPOCH, LEAVE_GROUP_STATIC_MEMBER_EPOCH,
};
use crate::common::requests::ConsumerGroupHeartbeatResponse;
use crate::common::protocol::Errors;
use crate::consumer::close_options::GroupMembershipOperation;
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;

use super::abstract_membership_manager::{AbstractMembershipManager, LocalAssignment};
use super::commit_request_manager::CommitRequestManager;
use super::consumer_metadata::ConsumerMetadata;
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
}

impl ConsumerMembershipManager {
    /// Java constructor (the test-visible 13-arg variant). Drops
    /// `Metrics` / `RebalanceMetricsManager` (no Rust metrics framework)
    /// and the `LogContext` (we use `log`).
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
    ) -> Self {
        let abstract_mm = AbstractMembershipManager::new(
            group_id,
            subscriptions,
            metadata,
            background_event_handler,
            auto_commit_enabled,
        );
        Self {
            abstract_mm,
            group_instance_id,
            rack_id,
            rebalance_timeout_ms,
            server_assignor,
            commit_request_manager,
            leave_group_operation: Mutex::new(GroupMembershipOperation::Default),
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
    pub(crate) fn transition_to_sending_leave_group(
        &self,
        due_to_expired_poll_timer: bool,
    ) -> Result<(), KafkaError> {
        self.abstract_mm.transition_to_sending_leave_group(self.leave_group_epoch(), due_to_expired_poll_timer)
    }

    /// Java: `onHeartbeatSuccess(ConsumerGroupHeartbeatResponse)`.
    /// Updates member info and state from a successful response.
    ///
    /// Returns `Err(KafkaError)` for unexpected errors in the response
    /// body — Java throws `IllegalArgumentException`.
    pub(crate) fn on_heartbeat_success(
        &self,
        response: &ConsumerGroupHeartbeatResponse,
    ) -> Result<(), KafkaError> {
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
        if data.assignment.is_some() {
            if !state.can_handle_new_assignment() {
                log::debug!(
                    "Ignoring new assignment received from server because member is in {} state.",
                    state
                );
                return Ok(());
            }
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
    /// follow-on actions). For Phase 8b we do not invoke the
    /// `onPartitionsLost` callback here because the Consumer thread
    /// will drive that on its next `poll()` via the rebalance-listener
    /// event channel. This matches Java's contract: transition first,
    /// then enqueue the lost-callback event; the actual listener runs
    /// on the app thread.
    pub(crate) async fn transition_to_fatal(&self, current_time_ms: i64) -> Result<(), KafkaError> {
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

        // Invoke onPartitionsLost via the §31 handshake to release
        // assignment. Errors from the listener are logged + ignored
        // (Java's `whenComplete` logs and continues).
        let partitions = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        if !partitions.is_empty() {
            if let Err(e) = self
                .abstract_mm
                .invoke_rebalance_callback(
                    ConsumerRebalanceListenerMethodName::OnPartitionsLost,
                    partitions,
                    current_time_ms,
                )
                .await
            {
                log::error!(
                    "onPartitionsLost callback invocation failed while releasing assignment after member failed with fatal error: {}",
                    e
                );
            }
        }
        self.abstract_mm.clear_assignment();
        Ok(())
    }

    /// Java: `transitionToFenced()`. Same shape as `transition_to_fatal`
    /// but transitions to FENCED and then JOINING after the listener
    /// completes (so the member rejoins).
    pub(crate) async fn transition_to_fenced(&self, current_time_ms: i64) -> Result<(), KafkaError> {
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

        // Invoke onPartitionsLost.
        let partitions = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assigned_partitions().into_iter().collect::<Vec<_>>()
        };
        if !partitions.is_empty() {
            if let Err(e) = self
                .abstract_mm
                .invoke_rebalance_callback(
                    ConsumerRebalanceListenerMethodName::OnPartitionsLost,
                    partitions,
                    current_time_ms,
                )
                .await
            {
                log::error!(
                    "onPartitionsLost callback invocation failed while releasing assignment after member got fenced. Member will rejoin the group anyways. {}",
                    e
                );
            }
        }
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

    /// Reconcile the target assignment per §31. Async because it
    /// `.await`s the rebalance-listener oneshot acks.
    ///
    /// Pre-condition: state is `RECONCILING`.
    ///
    /// Behavior mirrors Java's `maybeReconcile(boolean canCommit)` +
    /// `revokeAndAssign(...)` chain, but linearised because we have
    /// `async/await` instead of `CompletableFuture::whenComplete`.
    ///
    /// Java: `maybeReconcile(boolean canCommit)`.
    pub(crate) async fn reconcile(&self, current_time_ms: i64) -> Result<(), KafkaError> {
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
            !guard.current_assignment.is_none()
                && resolved_assignment.partitions == guard.current_assignment.partitions
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

        // 5. Auto-commit-before-reconciliation. Java's
        // `signalReconciliationStarted()` calls
        // `commitRequestManager.maybeAutoCommitSyncBeforeRebalance(...)`.
        // Phase 8b's CommitRequestManager (Phase 9) does not yet
        // expose this hook (deferred to Phase 10's supplier wiring);
        // we log and proceed, mirroring Java's
        // `commitResult.whenComplete` error branch.
        if self.commit_request_manager.is_some() {
            let auto_commit = {
                let guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.auto_commit_enabled
            };
            if auto_commit {
                log::debug!(
                    "Auto-commit-before-rebalance not wired yet (Phase 10); proceeding with reconciliation."
                );
            }
        }

        // 6. Mark reconciliation in progress.
        self.abstract_mm.mark_reconciliation_in_progress();

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

        // 9. §31: enqueue onPartitionsRevoked and AWAIT the ack
        // before advancing the state machine. Java guards on
        // `!partitionsRevoked.isEmpty() && listener.isPresent()`. The
        // Rust translation always enqueues if non-empty; the app side
        // is responsible for the listener-present check.
        if !revoked.is_empty() {
            let revoked_vec: Vec<TopicPartition> = revoked.iter().cloned().collect();
            if let Err(e) = self
                .abstract_mm
                .invoke_rebalance_callback(
                    ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                    revoked_vec,
                    current_time_ms,
                )
                .await
            {
                log::error!("onPartitionsRevoked callback failed: {}", e);
                // Java: leaves the member in RECONCILING state after
                // callbacks fail. We mirror.
                self.abstract_mm.mark_reconciliation_completed();
                return Ok(());
            }
        }

        // 10. Abort check between steps (state may have moved
        // because of a fence / fatal).
        if self.abstract_mm.maybe_abort_reconciliation() {
            return Ok(());
        }

        // 11. Update subscription state with new assignment (marking
        // newly added partitions as pending-on-assigned-callback).
        {
            let mut subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let added_vec: Vec<TopicPartition> = added.iter().cloned().collect();
            if let Err(e) = subs.assign_from_subscribed_awaiting_callback(&assigned_topic_partitions, &added_vec) {
                log::warn!("assign_from_subscribed_awaiting_callback failed: {}", e);
            }
        }

        // 12. Notify state listeners of the assignment change.
        {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.notify_assignment_change(&assigned_set);
        }

        // 13. §31: enqueue onPartitionsAssigned and AWAIT.
        // Java: always enqueue when listener present, even if empty.
        let added_vec: Vec<TopicPartition> = added.iter().cloned().collect();
        let assigned_callback_result = self
            .abstract_mm
            .invoke_rebalance_callback(
                ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                added_vec.clone(),
                current_time_ms,
            )
            .await;

        // 14. Enable fetching for assigned partitions (only if the
        // callback succeeded — Java's `subscriptions.enablePartitionsAwaitingCallback`).
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
                log::warn!(
                    "Leaving newly assigned partitions {:?} marked as non-fetchable after onPartitionsAssigned callback failed: {}",
                    added,
                    e
                );
                self.abstract_mm.mark_reconciliation_completed();
                return Ok(());
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
            {
                let mut guard = match self.abstract_mm.inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                guard.current_assignment = resolved_assignment;
                guard.transition_to(MemberState::Acknowledging)?;
            }
            // Java: signalReconciliationCompleting() resets the
            // auto-commit timer.
            if let Some(commit_mgr) = self.commit_request_manager.as_ref() {
                commit_mgr.reset_auto_commit_timer(current_time_ms);
            }
            self.abstract_mm.mark_reconciliation_completed();
        }
        Ok(())
    }

    /// Java: `isLeavingGroup()` (override).
    pub(crate) fn is_leaving_group(&self) -> bool {
        let leave_op = self.leave_group_operation();
        if matches!(leave_op, GroupMembershipOperation::RemainInGroup) && self.group_instance_id.is_none() {
            return false;
        }
        let state = self.state();
        let is_leaving_state = matches!(state, MemberState::PrepareLeaving | MemberState::Leaving);
        let has_leave_operation = matches!(leave_op, GroupMembershipOperation::Default | GroupMembershipOperation::LeaveGroup)
            || self.group_instance_id.is_some();
        is_leaving_state && has_leave_operation
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

// Send required so RequestManagers::entries can put us in a
// `Vec<&mut dyn RequestManager>` and the bg task can own us.
unsafe impl Send for ConsumerMembershipManager {}

// Suppress unused-emit warnings on the BackgroundEvent re-export from
// the §31 path — used only inside reconcile (typed module path).
#[doc(hidden)]
#[allow(dead_code)]
fn _force_used() {
    let _ = std::mem::size_of::<BackgroundEvent>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use tokio::sync::mpsc;

    fn make(
        group_instance_id: Option<String>,
        server_assignor: Option<String>,
        rack_id: Option<String>,
    ) -> (
        ConsumerMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(&config, subs.clone(), ClusterResourceListeners::new()));
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
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0).await });

        // App-side: expect onPartitionsAssigned event (no revoked
        // partitions because we had none). Ack with Ok(()).
        let env = rx.recv().await.expect("event");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name, ack, partitions } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsAssigned);
                assert_eq!(partitions.len(), 2);
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {:?}", other),
        }

        bg.await.unwrap().unwrap();
        assert_eq!(mgr_arc.state(), MemberState::Acknowledging);
    }
}
