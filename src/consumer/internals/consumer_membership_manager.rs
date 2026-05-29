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
    /// 3. Otherwise invoke `onPartitionsLost` via the §31 handshake to
    ///    release the assignment. The actual listener executes on the
    ///    application task per §31; this `.await` returns once the app
    ///    side has acknowledged completion.
    /// 4. Clear the assignment (Java: `clearAssignment()`).
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
        if !partitions.is_empty()
            && let Err(e) = self
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
        if !partitions.is_empty()
            && let Err(e) = self
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
    /// `can_commit` mirrors Java's parameter: when auto-commit is
    /// enabled and `can_commit` is `false`, the reconciliation must be
    /// skipped because there is no safe opportunity to flush in-progress
    /// offsets before the assignment changes. Java passes `false` from
    /// `AbstractMembershipManager.poll(now)` (the per-iteration
    /// `entries()` walk) and `true` from `ApplicationEventProcessor.process(AsyncPollEvent)`
    /// (which has just run `updateTimerAndMaybeCommit`). The Rust
    /// translation mirrors this through the bg-task call site (passes
    /// `false`) and the `process_async_poll` arm (passes `true`).
    ///
    /// Java: `maybeReconcile(boolean canCommit)`
    /// (`AbstractMembershipManager.java:824`).
    pub(crate) async fn reconcile(&self, current_time_ms: i64, can_commit: bool) -> Result<(), KafkaError> {
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

        // 5. Java's `if (autoCommitEnabled && !canCommit) return;` gate
        // (`AbstractMembershipManager.java:854`). Skip reconciliation when
        // auto-commit is enabled and the caller has not validated that
        // committing is currently safe (i.e. the per-iteration
        // `entries().poll()` path, which passes `can_commit=false`). The
        // AsyncPoll path passes `can_commit=true` because
        // `updateTimerAndMaybeCommit` ran just before, so any pending
        // offsets are already in-flight.
        //
        // Phase 11 will additionally wire the actual flush via
        // `CommitRequestManager::maybe_auto_commit_sync_before_rebalance`
        // (the method exists since commit 2.5/N) once the
        // `AsyncKafkaConsumer` poll-path scaffolding lands. That flush
        // happens inside Java's `revokeAndAssign(...)` chain, NOT here —
        // this `if` is the prior, independent gate.
        let auto_commit_enabled = if self.commit_request_manager.is_some() {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.auto_commit_enabled
        } else {
            false
        };
        if auto_commit_enabled && !can_commit {
            log::trace!(
                "Skipping reconciliation: auto-commit is enabled and the caller cannot \
                 guarantee that offsets are safe to commit (can_commit=false)."
            );
            return Ok(());
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
                // callbacks fail (broker will eventually kick the
                // member out via the reconciliation commit timeout).
                // Per COMMENTS.1.md fix #2 we surface the error to the
                // caller (Phase 10 bg task) so the failure is
                // observable — Java's CompletableFuture chain does the
                // same via `revocationResult.completeExceptionally`.
                self.abstract_mm.mark_reconciliation_completed();
                return Err(e);
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
                // Per COMMENTS.1.md fix #2: surface listener failure so
                // the caller (Phase 10 bg task) can observe it. Java's
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
/// (`ConsumerMembershipManagerTest`, 93 cases):
///
/// Translated (26 / 93):
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
/// Not translated (~67 / 93) — rationale categories:
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
///    method itself lives on the commit manager (Phase 10, commit 2.5/N).
///    The *call site* inside `reconcile` is still a no-op log — Phase 11
///    wires the actual invocation as part of the AsyncKafkaConsumer
///    poll-path scaffolding. Until that wiring lands, these tests have
///    nothing to verify behaviourally on the Rust side. Deferred to
///    Phase 11.
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
///    `MockTime` advance + `RebalanceMetricsManager` verification.
///    Rust has neither MockTime as a first-class fixture in this file
///    (we pass `current_time_ms` directly), nor a metrics framework.
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
        );
        (mgr, rx)
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
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0, true).await });

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
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0, true).await });

        let env = rx.recv().await.expect("event");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name, ack, partitions } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsAssigned);
                assert_eq!(partitions.len(), 1);
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
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0, true).await });

        let env = rx.recv().await.expect("event");
        if let BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { ack, .. } = env.event {
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
        mgr.transition_to_fatal(0).await.unwrap();
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

        mgr.transition_to_fenced(0).await.unwrap();
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
        mgr.transition_to_fenced(0).await.unwrap();
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
        mgr.transition_to_fenced(0).await.unwrap();
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
        mgr.transition_to_fenced(0).await.unwrap();
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
        mgr.transition_to_fenced(0).await.unwrap();
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

        mgr.transition_to_fatal(0).await.unwrap();
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
        mgr.transition_to_fenced(0).await.unwrap();
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
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0, true).await });

        let env = rx.recv().await.expect("event");
        if let BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { ack, .. } = env.event {
            ack.send(Err(KafkaError::timeout("listener error"))).unwrap();
        }
        let result = bg.await.unwrap();
        assert!(result.is_err());
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
}
