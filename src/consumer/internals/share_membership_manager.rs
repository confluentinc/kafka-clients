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

//! `ShareMembershipManager` — KIP-932 share-group membership manager.
//!
//! Composes [`super::abstract_membership_manager::AbstractMembershipManager`]
//! (the shared state machine + §31 reconcile pipeline) and supplies the
//! share-specific configuration and hooks: the rack ID, the
//! `ShareGroupHeartbeatResponse` handler (`on_heartbeat_success`), and the
//! join / leave group epochs.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareMembershipManager`.
//!
//! # Relationship to `ConsumerMembershipManager`
//!
//! `ShareMembershipManager` is a close cousin of
//! [`super::consumer_membership_manager::ConsumerMembershipManager`]. Both
//! extend `AbstractMembershipManager` in Java. The share variant is
//! **simpler**:
//!
//! - No static membership (`groupInstanceId`), no `serverAssignor`, no
//!   configurable `leaveGroupOperation`.
//! - No auto-commit: share groups acknowledge records via the
//!   `ShareConsumeRequestManager` (KIP-932) rather than committing offsets,
//!   so there is no `CommitRequestManager` and `auto_commit_enabled` is
//!   always `false`. The Java super constructor passes `false`. As a
//!   consequence [`Self::reconcile`] omits the auto-commit-before-rebalance
//!   step that [`ConsumerMembershipManager::reconcile`] performs.
//! - `join_group_epoch` / `leave_group_epoch` come from
//!   `ShareGroupHeartbeatRequest` (0 / -1); there is no static-member
//!   `-2` case.
//! - `is_leaving_group()` uses the base `AbstractMembershipManager`
//!   implementation (`PREPARE_LEAVING | LEAVING`) — there is no
//!   remain-in-group / static-member override.
//!
//! **Metrics deferred to KIP-714**: Java's `ShareRebalanceMetricsManager`
//! recording is omitted; each site is marked `// metrics: deferred to
//! KIP-714`. All state-machine / timing logic is preserved.

#![allow(dead_code)] // Phase 4: lands before the bg-loop wiring (Phases 5-6).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use crate::common::protocol::Errors;
use crate::common::requests::ShareGroupHeartbeatResponse;
use crate::common::requests::share_group_heartbeat_request::{JOIN_GROUP_MEMBER_EPOCH, LEAVE_GROUP_MEMBER_EPOCH};
use crate::common::{KafkaError, TopicPartition, Uuid};
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;

use super::abstract_membership_manager::{AbstractMembershipManager, LocalAssignment};
use super::consumer_metadata::ConsumerMetadata;
use super::member_state::MemberState;
use super::network_client_delegate::PollResult;
use super::request_manager::RequestManager;
use super::subscription_state::SubscriptionState;

/// KIP-932 share-group membership manager.
///
/// Java: `ShareMembershipManager extends AbstractMembershipManager<ShareGroupHeartbeatResponse>`.
pub(crate) struct ShareMembershipManager {
    /// Shared state + state machine + §31 reconcile pipeline.
    pub(crate) abstract_mm: AbstractMembershipManager,
    /// Java: `protected final String rackId`. Sent on join. `None` maps to
    /// Java's `null` rack ID.
    pub(crate) rack_id: Option<String>,
}

impl ShareMembershipManager {
    /// Java constructor
    /// `ShareMembershipManager(LogContext, groupId, rackId, subscriptions,
    /// metadata, time, metrics)`. Drops `Metrics` /
    /// `ShareRebalanceMetricsManager` (no Rust metrics framework), the
    /// `LogContext` (we use `log`), and `Time` (deadline-based timing).
    ///
    /// Auto-commit is always disabled for share groups (Java's super
    /// constructor passes `false`).
    pub(crate) fn new(
        group_id: impl Into<String>,
        rack_id: Option<String>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
        background_event_handler: Arc<BackgroundEventHandler>,
    ) -> Self {
        let abstract_mm = AbstractMembershipManager::new(
            group_id,
            subscriptions,
            metadata,
            background_event_handler,
            false, // auto_commit_enabled — share groups do not commit offsets
        );
        Self { abstract_mm, rack_id }
    }

    /// Java: `rackId()`.
    pub(crate) fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Java: `groupId()`.
    pub(crate) fn group_id(&self) -> String {
        let g = self.lock_inner();
        g.group_id.clone()
    }

    /// Java: `memberId()`.
    pub(crate) fn member_id(&self) -> String {
        let g = self.lock_inner();
        g.member_id.clone()
    }

    /// Java: `memberEpoch()`.
    pub(crate) fn member_epoch(&self) -> i32 {
        let g = self.lock_inner();
        g.member_epoch
    }

    /// Java: `state()`.
    pub(crate) fn state(&self) -> MemberState {
        let g = self.lock_inner();
        g.state
    }

    /// Java: `currentAssignment()`.
    pub(crate) fn current_assignment(&self) -> LocalAssignment {
        let g = self.lock_inner();
        g.current_assignment.clone()
    }

    /// Java: `shouldHeartbeatNow()`.
    pub(crate) fn should_heartbeat_now(&self) -> bool {
        self.lock_inner().should_heartbeat_now()
    }

    /// Java: `shouldSkipHeartbeat()`.
    pub(crate) fn should_skip_heartbeat(&self) -> bool {
        self.lock_inner().should_skip_heartbeat()
    }

    /// Java: `reconciliationInProgress()` (visible for testing).
    pub(crate) fn reconciliation_in_progress(&self) -> bool {
        self.lock_inner().reconciliation_in_progress
    }

    /// Java: `subscriptionUpdated()` (visible for testing).
    pub(crate) fn subscription_updated(&self) -> bool {
        self.lock_inner().subscription_updated
    }

    /// Java: `joinGroupEpoch()` — `ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH` (0).
    pub(crate) fn join_group_epoch(&self) -> i32 {
        JOIN_GROUP_MEMBER_EPOCH
    }

    /// Java: `leaveGroupEpoch()` — `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH` (-1).
    pub(crate) fn leave_group_epoch(&self) -> i32 {
        LEAVE_GROUP_MEMBER_EPOCH
    }

    /// Java: `isLeavingGroup()` — base implementation (no static-member /
    /// remain-in-group override for share groups).
    pub(crate) fn is_leaving_group(&self) -> bool {
        self.lock_inner().is_leaving_group_base()
    }

    /// Java: `registerStateListener(MemberStateListener)`.
    pub(crate) fn register_state_listener(&self, listener: Arc<dyn super::member_state_listener::MemberStateListener>) {
        self.abstract_mm.register_state_listener(listener);
    }

    /// Java: `onSubscriptionUpdated()`.
    pub(crate) fn on_subscription_updated(&self) {
        self.abstract_mm.on_subscription_updated();
    }

    /// Java: `onConsumerPoll()`.
    pub(crate) fn on_consumer_poll(&self) -> Result<(), KafkaError> {
        self.abstract_mm.on_consumer_poll(self.join_group_epoch())
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

    /// Java: `onHeartbeatRequestSkipped()`.
    pub(crate) fn on_heartbeat_request_skipped(&self) -> Result<(), KafkaError> {
        self.abstract_mm.on_heartbeat_request_skipped()
    }

    /// Java: `onHeartbeatRequestGenerated()`.
    pub(crate) fn on_heartbeat_request_generated(&self) -> Result<(), KafkaError> {
        self.abstract_mm.on_heartbeat_request_generated()
    }

    /// Java: `maybeRejoinStaleMember()`.
    pub(crate) fn maybe_rejoin_stale_member(&self) {
        self.abstract_mm.maybe_rejoin_stale_member(self.join_group_epoch());
    }

    /// Java: `onHeartbeatFailure(boolean retriable)`.
    pub(crate) fn on_heartbeat_failure(&self, retriable: bool) {
        // metrics: deferred to KIP-714 (Java records the failed-rebalance
        // metric here when a rebalance was in progress).
        let was_unsubscribed = self.abstract_mm.on_heartbeat_failure(retriable);
        if was_unsubscribed {
            log::warn!(
                "Member with epoch {} received a failed response to the heartbeat to leave the group.",
                self.member_epoch()
            );
        }
    }

    /// Java: `onHeartbeatSuccess(ShareGroupHeartbeatResponse)`.
    /// Updates member info and state from a successful response.
    ///
    /// Returns `Err(KafkaError::IllegalArgument)` for unexpected errors in
    /// the response body — Java throws `IllegalArgumentException`.
    pub(crate) fn on_heartbeat_success(&self, response: &ShareGroupHeartbeatResponse) -> Result<(), KafkaError> {
        let data = response.data();
        if data.error_code != Errors::None.code() {
            return Err(KafkaError::illegal_argument(format!(
                "Unexpected error in Heartbeat response. Expected no error, but received: {:?}",
                Errors::for_code(data.error_code)
            )));
        }
        let mut guard = self.lock_inner();
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
            // Java: `maybeCompleteLeaveInProgress()` completes the pending
            // leave future. The Rust close/leave handshake is wired in a
            // later phase; we log and return like Java's early-out.
            log::debug!(
                "Member {} with epoch {} received a successful response to the heartbeat to leave the group and completed the leave operation.",
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
            // Java: `maybeCompleteLeaveInProgress()` (see above).
            return Ok(());
        }

        guard.update_member_epoch(data.member_epoch);

        // Assignment is only populated when there's a new target assignment.
        let new_assignment = match data.assignment.as_ref() {
            Some(assignment) => {
                if !state.can_handle_new_assignment() {
                    // New assignment received but member is in a state where
                    // it cannot take new assignments (e.g. preparing to
                    // leave the group).
                    log::debug!(
                        "Ignoring new assignment received from server because member is in {} state.",
                        state
                    );
                    return Ok(());
                }
                let mut map: HashMap<Uuid, Vec<i32>> = HashMap::new();
                for tp in &assignment.topic_partitions {
                    map.insert(tp.topic_id, tp.partitions.clone());
                }
                Some(map)
            },
            None => None,
        };
        drop(guard);

        if let Some(assignment) = new_assignment {
            self.abstract_mm.process_assignment_received(assignment)?;
        }
        Ok(())
    }

    /// Java: `transitionToFatal()` (override wiring the release of the
    /// assignment via `onPartitionsLost`). Mirrors
    /// [`ConsumerMembershipManager::transition_to_fatal`].
    pub(crate) async fn transition_to_fatal(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // metrics: deferred to KIP-714.
        let previous_state = self.abstract_mm.transition_to_fatal()?;

        if matches!(
            previous_state,
            MemberState::Unsubscribed | MemberState::Leaving | MemberState::PrepareLeaving
        ) {
            return Ok(());
        }

        let partitions = self.assigned_partitions();
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

    /// Java: `transitionToFenced()`. Mirrors
    /// [`ConsumerMembershipManager::transition_to_fenced`].
    pub(crate) async fn transition_to_fenced(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        let pre_state = self.state();

        // Java's "already leaving / unsubscribed" short-circuits.
        match pre_state {
            MemberState::PrepareLeaving => {
                self.transition_to_sending_leave_group(false)?;
                let mut guard = self.lock_inner();
                guard.transition_to(MemberState::Unsubscribed)?;
                return Ok(());
            },
            MemberState::Leaving => {
                let mut guard = self.lock_inner();
                guard.transition_to(MemberState::Unsubscribed)?;
                return Ok(());
            },
            MemberState::Unsubscribed => {
                log::debug!("Member got fenced but it already left the group, so it won't attempt to rejoin.");
                return Ok(());
            },
            _ => {},
        }

        // Normal fence flow: FENCED + reset epoch.
        {
            let mut guard = self.lock_inner();
            guard.transition_to(MemberState::Fenced)?;
            guard.update_member_epoch(self.join_group_epoch());
        }

        let partitions = self.assigned_partitions();
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

        let still_fenced = self.state() == MemberState::Fenced;
        if still_fenced {
            self.transition_to_joining()?;
        }
        Ok(())
    }

    /// Async tail of Java's `AbstractMembershipManager.transitionToStale()`:
    /// release the assignment via `onPartitionsLost` and, if a timer reset
    /// requested it while the release was in flight, rejoin. Mirrors
    /// [`ConsumerMembershipManager::transition_to_stale`]. The STALE state
    /// transition itself happens synchronously inside
    /// [`AbstractMembershipManager::on_heartbeat_request_generated`].
    pub(crate) async fn transition_to_stale(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        let partitions = self.assigned_partitions();
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
                "onPartitionsLost callback invocation failed while releasing assignment after member left group due to expired poll timer: {}",
                e
            );
        }
        self.abstract_mm.clear_assignment();

        let rejoin = {
            let mut guard = self.lock_inner();
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

    /// Reconcile the target assignment per §31. Async because it `.await`s
    /// the rebalance-listener oneshot acks. Pre-condition: state is
    /// `RECONCILING`.
    ///
    /// Mirrors [`ConsumerMembershipManager::reconcile`] but WITHOUT the
    /// auto-commit-before-rebalance step and the `if (autoCommitEnabled &&
    /// !canCommit) return;` gate — share groups do not commit offsets, so
    /// `auto_commit_enabled` is always `false` and those branches are
    /// unreachable (Java's `maybeReconcile(canCommit)` gate never triggers).
    ///
    /// Java: `AbstractMembershipManager.maybeReconcile(boolean canCommit)`.
    pub(crate) async fn reconcile(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // 1. State / progress checks.
        {
            let guard = self.lock_inner();
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
            let guard = self.lock_inner();
            guard.current_target_assignment.local_epoch
        };
        let resolved_assignment = LocalAssignment::new(target_epoch, resolved_partitions.clone())?;

        // 4. Short-circuit: if the resolved subset equals the current
        // assignment's partitions, just bump epoch and ACK.
        let short_circuit = {
            let guard = self.lock_inner();
            !guard.current_assignment.is_none() && resolved_assignment.partitions == guard.current_assignment.partitions
        };
        if short_circuit {
            let mut guard = self.lock_inner();
            guard.current_assignment = resolved_assignment;
            guard.transition_to(MemberState::Acknowledging)?;
            return Ok(());
        }

        // 5. Mark reconciliation in progress.
        self.abstract_mm.mark_reconciliation_in_progress();

        // 6. Compute added vs revoked partitions.
        let mut assigned_topic_partitions: Vec<TopicPartition> = Vec::new();
        for (_topic_id, topic_name, partitions) in &resolved {
            for p in partitions {
                assigned_topic_partitions.push(TopicPartition::new(topic_name.clone(), *p));
            }
        }
        let assigned_set: HashSet<TopicPartition> = assigned_topic_partitions.iter().cloned().collect();

        let owned_set: HashSet<TopicPartition> = {
            let subs = self.lock_subs();
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

        // 7. Mark partitions pending revocation to stop fetching.
        {
            let mut subs = self.lock_subs();
            let revoked_vec: Vec<TopicPartition> = revoked.iter().cloned().collect();
            if let Err(e) = subs.mark_pending_revocation(&revoked_vec) {
                log::warn!("mark_pending_revocation failed: {}", e);
            }
        }

        // 8. §31: enqueue onPartitionsRevoked and AWAIT the ack before
        // advancing the state machine.
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
                self.abstract_mm.mark_reconciliation_completed();
                return Err(e);
            }
        }

        // 9. Abort check between steps (state may have moved).
        if self.abstract_mm.maybe_abort_reconciliation() {
            return Ok(());
        }

        // 10. Update subscription state with new assignment.
        {
            let mut subs = self.lock_subs();
            let added_vec: Vec<TopicPartition> = added.iter().cloned().collect();
            if let Err(e) = subs.assign_from_subscribed_awaiting_callback(&assigned_topic_partitions, &added_vec) {
                log::warn!("assign_from_subscribed_awaiting_callback failed: {}", e);
            }
        }

        // 11. Notify state listeners of the assignment change.
        {
            let guard = self.lock_inner();
            guard.notify_assignment_change(&assigned_set);
        }

        // 12. §31: enqueue onPartitionsAssigned and AWAIT.
        let added_vec: Vec<TopicPartition> = added.iter().cloned().collect();
        let assigned_callback_result = self
            .abstract_mm
            .invoke_rebalance_callback(
                ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                added_vec.clone(),
                current_time_ms,
            )
            .await;

        // 13. Enable fetching for assigned partitions (only if the callback
        // succeeded).
        match assigned_callback_result {
            Ok(()) => {
                let mut subs = self.lock_subs();
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
                return Err(e);
            },
        }

        // 14. Update local cache of assigned topic names.
        {
            let mut assigned_topic_names: HashSet<String> = HashSet::new();
            for (_id, name, _partitions) in &resolved {
                assigned_topic_names.insert(name.clone());
            }
            let mut guard = self.lock_inner();
            guard.assigned_topic_names_cache.retain(|_, v| assigned_topic_names.contains(v));
        }

        // 15. Final abort check; advance to ACKNOWLEDGING.
        let aborted = self.abstract_mm.maybe_abort_reconciliation();
        if !aborted {
            {
                let mut guard = self.lock_inner();
                guard.current_assignment = resolved_assignment;
                guard.transition_to(MemberState::Acknowledging)?;
            }
            // metrics: deferred to KIP-714 (Java records the rebalance
            // latency / total here via signalReconciliationCompleting).
            self.abstract_mm.mark_reconciliation_completed();
        }
        Ok(())
    }

    /// Java: `AbstractMembershipManager.leaveGroup()` (via unsubscribe). Runs
    /// the rebalance-listener callbacks during the leave. Mirrors
    /// [`ConsumerMembershipManager::leave_group`], simplified: share groups
    /// have no static membership or configurable leave operation.
    pub(crate) async fn leave_group(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        // Step 1: already-out-of-group fast path.
        let pre_state = self.state();
        if matches!(
            pre_state,
            MemberState::Unsubscribed | MemberState::Fenced | MemberState::Fatal | MemberState::Stale
        ) {
            if pre_state == MemberState::Fenced {
                self.abstract_mm.clear_assignment();
                let mut guard = self.lock_inner();
                guard.transition_to(MemberState::Unsubscribed)?;
            }
            {
                let mut subs = self.lock_subs();
                subs.unsubscribe();
            }
            {
                let guard = self.lock_inner();
                guard.notify_assignment_change(&HashSet::new());
            }
            return Ok(());
        }

        // Step 2: already-leaving short-circuit.
        if matches!(pre_state, MemberState::PrepareLeaving | MemberState::Leaving) {
            log::debug!("Leave group operation already in progress for member {}", self.member_id());
            return Ok(());
        }

        // Step 3: PREPARE_LEAVING + rebalance-callback step.
        {
            let mut guard = self.lock_inner();
            guard.transition_to(MemberState::PrepareLeaving)?;
        }

        if let Err(e) = self.signal_member_leaving_group(current_time_ms).await {
            log::error!(
                "Member {} callback to release assignment failed. It will proceed to clear its assignment and send a leave group heartbeat: {}",
                self.member_id(),
                e
            );
        }

        // Step 4 + 5: clearAssignmentAndLeaveGroup().
        {
            let mut subs = self.lock_subs();
            subs.unsubscribe();
        }
        self.abstract_mm.clear_assignment();
        self.transition_to_sending_leave_group(false)?;
        Ok(())
    }

    /// Java: `invokeOnPartitionsRevokedOrLostToReleaseAssignment()`. Chooses
    /// between `onPartitionsRevoked` (epoch > 0) and `onPartitionsLost`
    /// (epoch <= 0). Mirrors
    /// [`ConsumerMembershipManager::signal_member_leaving_group`].
    pub(crate) async fn signal_member_leaving_group(&self, current_time_ms: i64) -> Result<(), KafkaError> {
        let (dropped_partitions, member_epoch) = {
            let partitions = {
                let subs = self.lock_subs();
                subs.assigned_partitions().into_iter().collect::<Vec<_>>()
            };
            let inner = self.lock_inner();
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

    /// Snapshot the currently-assigned partitions under a short lock.
    fn assigned_partitions(&self) -> Vec<TopicPartition> {
        let subs = self.lock_subs();
        subs.assigned_partitions().into_iter().collect()
    }

    /// Lock the shared membership state, recovering from a poisoned mutex
    /// (§16: short critical sections, never across `.await`).
    fn lock_inner(&self) -> std::sync::MutexGuard<'_, super::abstract_membership_manager::MembershipInner> {
        match self.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Lock the shared subscription state, recovering from a poisoned mutex.
    fn lock_subs(&self) -> std::sync::MutexGuard<'_, SubscriptionState> {
        match self.abstract_mm.subscriptions.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

/// `RequestManager` impl. `poll` is a sync hook that returns the empty
/// result, mirroring Java's `AbstractMembershipManager.poll(...)` which calls
/// `maybeReconcile(false)` and returns `EMPTY`. Reconcile is async (§31) and
/// driven by the bg task (Phase 5/6), which `.await`s [`Self::reconcile`]
/// when the state is `RECONCILING`.
impl RequestManager for ShareMembershipManager {
    fn poll(&mut self, _current_time_ms: i64) -> PollResult {
        PollResult::empty()
    }
}

impl std::fmt::Debug for ShareMembershipManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareMembershipManager")
            .field("group_id", &self.group_id())
            .field("member_id", &self.member_id())
            .field("member_epoch", &self.member_epoch())
            .field("state", &self.state())
            .field("rack_id", &self.rack_id)
            .finish()
    }
}

/// Translation notes on Java test coverage (`ShareMembershipManagerTest`,
/// 45 `@Test` / `@ParameterizedTest` methods).
///
/// `ShareMembershipManager` is a thin subclass of `AbstractMembershipManager`
/// for the *state machine* and §31 handshake (those genuinely live in
/// `abstract_membership_manager.rs` and are tested there). The **reconcile
/// pipeline**, however, is NOT shared: `AbstractMembershipManager` has no
/// `reconcile`, so the ~170-line pipeline is hand-DUPLICATED into both
/// [`Self::reconcile`] and [`ConsumerMembershipManager::reconcile`]. The share
/// copy is therefore exercised independently by the revocation /
/// added-vs-revoked / same-assignment-short-circuit tests below — a
/// transposed added/revoked diff or a broken short-circuit in the share copy
/// would fail these, not be masked by the consumer tests. The remaining tests
/// cover the share-specific surface (`rackId`, `joinGroupEpoch=0`,
/// `leaveGroupEpoch=-1`, `on_heartbeat_success` with
/// `ShareGroupHeartbeatResponse`), using a REAL `SubscriptionState` +
/// `ConsumerMetadata`.
///
/// Note: the Java tests mock `subscriptionState.rebalanceListener()` to
/// `Optional.empty()`, so `maybeReconcile` never fires a rebalance callback.
/// The Rust tests mirror this by NOT registering a
/// `ConsumerRebalanceListener` — the §31 `invoke_rebalance_callback`
/// short-circuit (`AbstractMembershipManager`) returns immediately, so
/// `reconcile(now).await` completes synchronously without a bg-task ack
/// dance.
///
/// Not translated — rationale categories:
///
/// 1. **Mockito-spy verification on internal methods**
///    (`verify(membershipManager, never()).markReconciliationInProgress()`,
///    `updateAssignment(...)`, `topicsAwaitingReconciliation()`) — Rust has
///    no mocking on a concrete struct; the same behavior is covered by
///    state-transition assertions. The metadata-driven
///    delayed-discard / unresolved-topic families
///    (`testDelayedMetadataUsedToCompleteAssignment`,
///    `testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved`,
///    etc.) exercise the same duplicated reconcile pipeline; the
///    add/revoke/short-circuit branches of the share copy ARE covered by
///    the dedicated tests below, and the deeper metadata-resolution edge
///    cases are covered by the equivalent `ConsumerMembershipManager`
///    reconcile tests (Phase 34) over the identical pipeline shape.
/// 2. **Leave-future completion** (`testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave`,
///    `testIgnoreLeaveResponseWhenNotLeavingGroup`,
///    `testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave`) — depend
///    on the `CompletableFuture` leave-result completion handshake, wired in
///    a later phase (same deferral as `ConsumerMembershipManager`).
/// 3. **RebalanceMetrics family** (`testRebalanceMetricsOn*`,
///    `testMetricsWhenHeartbeatFailed`) — OUT_OF_SCOPE, metrics deferred to
///    KIP-714.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::protocol::{ApiKeys, Errors};
    use crate::common::requests::MetadataResponse;
    use crate::common::{Node, TopicPartition, Uuid};
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::metadata_response_data::{
        MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
    };
    use crate::share_group_heartbeat_response_data::{Assignment, ShareGroupHeartbeatResponseData, TopicPartitions};
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    const GROUP_ID: &str = "test-group";
    /// Member epoch the broker returns in a successful (in-group) response.
    const MEMBER_EPOCH: i32 = 1;

    fn make(
        rack_id: Option<String>,
    ) -> (
        ShareMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let mgr = ShareMembershipManager::new(GROUP_ID, rack_id, subs, metadata, beh);
        (mgr, rx)
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Subscribe the (real) SubscriptionState to the given topics via the
    /// share-group path so assigned partitions are fetchable and
    /// `assign_from_subscribed_awaiting_callback` is permitted. No rebalance
    /// listener is registered — matching Java's mocked
    /// `rebalanceListener() == Optional.empty()`.
    fn subscribe_share(mgr: &ShareMembershipManager, topics: &[&str]) {
        let set: HashSet<String> = topics.iter().map(|s| s.to_string()).collect();
        let mut subs = mgr.lock_subs();
        subs.subscribe_to_share_group(set).unwrap();
    }

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

    /// Feed REAL topic-name metadata so reconcile resolves the given topic
    /// ids (mirrors Java's `when(metadata.topicNames()).thenReturn(map)`).
    fn seed_metadata(mgr: &ShareMembershipManager, topics: &[(&str, Uuid)]) {
        let response = build_metadata_response(topics);
        mgr.abstract_mm
            .metadata
            .update_with_current_request_version(&response, false, 1000);
    }

    /// Build a `ShareGroupHeartbeatResponse` carrying the given assignment
    /// (member epoch = [`MEMBER_EPOCH`]).
    fn heartbeat_response(member_id: String, assignment: Option<Vec<(Uuid, Vec<i32>)>>) -> ShareGroupHeartbeatResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_member_id(Some(member_id));
        data.set_member_epoch(MEMBER_EPOCH);
        data.set_heartbeat_interval_ms(5000);
        if let Some(parts) = assignment {
            let topic_partitions = parts
                .into_iter()
                .map(|(topic_id, partitions)| {
                    let mut tp = TopicPartitions::new();
                    tp.set_topic_id(topic_id).set_partitions(partitions);
                    tp
                })
                .collect();
            let mut a = Assignment::new();
            a.set_topic_partitions(topic_partitions);
            data.set_assignment(Some(a));
        }
        ShareGroupHeartbeatResponse::new(data)
    }

    fn receive_empty_assignment(mgr: &ShareMembershipManager) {
        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), Some(vec![])))
            .unwrap();
    }

    fn receive_assignment(mgr: &ShareMembershipManager, topic_id: Uuid, partitions: Vec<i32>) {
        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), Some(vec![(topic_id, partitions)])))
            .unwrap();
    }

    /// Bring a member to STABLE via join -> empty assignment -> reconcile ->
    /// ack. No listener is registered, so reconcile completes synchronously.
    /// Mirrors Java's `createMemberInStableState`.
    async fn create_member_in_stable_state() -> ShareMembershipManager {
        let (mgr, _rx) = make(None);
        mgr.transition_to_joining().unwrap();
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        mgr.reconcile(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        mgr.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        assert_eq!(mgr.member_epoch(), MEMBER_EPOCH);
        mgr
    }

    #[test]
    fn member_id_is_generated_at_startup() {
        let (mgr, _rx) = make(None);
        assert!(!mgr.member_id().is_empty());
        assert_eq!(mgr.group_id(), GROUP_ID);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    #[test]
    fn rack_id_accessor() {
        let (mgr, _rx) = make(None);
        assert_eq!(mgr.rack_id(), None);
        let (mgr2, _rx2) = make(Some("rack-1".to_string()));
        assert_eq!(mgr2.rack_id(), Some("rack-1"));
    }

    /// Join epoch is 0; leave epoch is -1 (share has no static membership).
    #[test]
    fn join_and_leave_group_epochs() {
        let (mgr, _rx) = make(None);
        assert_eq!(mgr.join_group_epoch(), JOIN_GROUP_MEMBER_EPOCH);
        assert_eq!(mgr.join_group_epoch(), 0);
        assert_eq!(mgr.leave_group_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        assert_eq!(mgr.leave_group_epoch(), -1);
    }

    #[test]
    fn transition_to_joining_from_unsubscribed() {
        let (mgr, _rx) = make(None);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.transition_to_joining().unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        assert_eq!(mgr.member_epoch(), 0);
    }

    /// Translated from `testTransitionToReconcilingIfEmptyAssignmentReceived`.
    #[tokio::test]
    async fn transition_to_reconciling_if_empty_assignment_received() {
        let (mgr, _rx) = make(None);
        mgr.transition_to_joining().unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // A response with an unresolved assignment keeps the member in
        // RECONCILING (topics not in metadata).
        let topic1 = Uuid::random_uuid();
        receive_assignment(&mgr, topic1, vec![0, 1, 2]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
    }

    /// Translated from `testReconcilingWhenReceivingAssignmentFoundInMetadata`
    /// (empty-assignment variant) + `testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment`.
    #[tokio::test]
    async fn empty_assignment_reconciles_and_acks_to_stable() {
        let mgr = create_member_in_stable_state().await;
        assert_eq!(mgr.state(), MemberState::Stable);
    }

    /// Translated from `testReconcileNewPartitionsAssignedWhenNoPartitionOwned`.
    #[tokio::test]
    async fn reconcile_new_partitions_assigned_when_no_partition_owned() {
        let (mgr, _rx) = make(None);
        subscribe_share(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        mgr.reconcile(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let mut expected: HashMap<Uuid, Vec<i32>> = HashMap::new();
        expected.insert(topic_id, vec![0, 1]);
        assert_eq!(mgr.current_assignment().partitions, expected);
        assert!(!mgr.reconciliation_in_progress());

        // Ack sent -> STABLE.
        mgr.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
    }

    // -----------------------------------------------------------------
    // Reconcile revocation / added-vs-revoked / short-circuit coverage.
    //
    // These exercise the DUPLICATED reconcile pipeline in
    // `ShareMembershipManager::reconcile` (which is a hand-copy of the
    // consumer pipeline, NOT a shared method). To observe the exact
    // `onPartitionsRevoked` / `onPartitionsAssigned` partition sets and
    // their ordering, a rebalance listener must be registered so the §31
    // handshake enqueues callback events (otherwise it short-circuits).
    // `SubscriptionState` only exposes listener registration via
    // `subscribe_topics(.., Some(listener))` (AUTO_TOPICS); the reconcile
    // pipeline does not branch on subscription type, so this exercises the
    // identical code path a share (AUTO_TOPICS_SHARE) subscription would.
    // -----------------------------------------------------------------

    /// No-op rebalance listener so the §31 callback handshake enqueues
    /// events (rather than short-circuiting) — the tests below act as the
    /// listener by draining and acking the events.
    struct NoopListener;
    #[async_trait::async_trait]
    impl crate::consumer::ConsumerRebalanceListener for NoopListener {
        async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Ok(())
        }
        async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Ok(())
        }
    }

    /// Subscribe with a registered rebalance listener (see the section
    /// comment above for why this uses `subscribe_topics` not
    /// `subscribe_to_share_group`).
    fn subscribe_with_listener(mgr: &ShareMembershipManager, topics: &[&str]) {
        let set: HashSet<String> = topics.iter().map(|s| s.to_string()).collect();
        let mut subs = mgr.lock_subs();
        subs.subscribe_topics(set, Some(Arc::new(NoopListener))).unwrap();
    }

    /// Pre-own the given partitions on the (real) SubscriptionState so the
    /// reconcile owned-vs-assigned diff has a non-empty owned set. Mirrors
    /// Java's `when(subscriptionState.assignedPartitions()).thenReturn(...)`.
    fn mock_owned_partitions(mgr: &ShareMembershipManager, owned: &[TopicPartition]) {
        let mut subs = mgr.lock_subs();
        subs.assign_from_subscribed(owned).unwrap();
    }

    /// Drain one `ConsumerRebalanceListenerCallbackNeeded` event, assert its
    /// method + partitions (order-insensitive), and ack it.
    async fn expect_callback(
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
        expected_method: ConsumerRebalanceListenerMethodName,
        expected_partitions: &[TopicPartition],
        result: Result<(), KafkaError>,
    ) {
        use crate::consumer::internals::events::background_event::BackgroundEvent;
        let env = rx.recv().await.expect("expected a callback-needed event");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name, ack, partitions } => {
                assert_eq!(method_name, expected_method, "unexpected callback method");
                let got: HashSet<TopicPartition> = partitions.into_iter().collect();
                let want: HashSet<TopicPartition> = expected_partitions.iter().cloned().collect();
                assert_eq!(got, want, "unexpected callback partitions");
                ack.send(result).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
    }

    /// Translated from `testReconcileNewPartitionsAssignedAndRevoked`.
    /// Owning topic1-0, a new assignment of {1,2} revokes 0 and assigns 1,2.
    /// The revoke callback fires BEFORE the assign callback, carrying exactly
    /// {0} and {1,2} respectively.
    #[tokio::test]
    async fn reconcile_new_partitions_assigned_and_revoked() {
        let (mgr, mut rx) = make(None);
        subscribe_with_listener(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_assignment(&mgr, topic_id, vec![1, 2]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0).await });

        // Ordering: revoked({0}) is enqueued and awaited before assigned({1,2}).
        expect_callback(
            &mut rx,
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            &[tp("topic1", 0)],
            Ok(()),
        )
        .await;
        expect_callback(
            &mut rx,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 1), tp("topic1", 2)],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();

        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!mgr.reconciliation_in_progress());
        let mut current = mgr.current_assignment().partitions;
        for v in current.values_mut() {
            v.sort_unstable();
        }
        assert_eq!(current, HashMap::from([(topic_id, vec![1, 2])]));
        let subs = mgr.lock_subs();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 1), tp("topic1", 2)]));
    }

    /// Translated from `testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`.
    /// Owning topic1-0, an assignment of {0,1,2} adds only {1,2} (0 already
    /// owned) — only an `onPartitionsAssigned({1,2})` callback fires, no
    /// revoke.
    #[tokio::test]
    async fn reconcile_new_partitions_assigned_when_other_partitions_owned() {
        let (mgr, mut rx) = make(None);
        subscribe_with_listener(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        receive_assignment(&mgr, topic_id, vec![0, 1, 2]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0).await });
        // Only the *added* partitions (1, 2) are passed to onPartitionsAssigned.
        expect_callback(
            &mut rx,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 1), tp("topic1", 2)],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();

        assert_eq!(mgr.state(), MemberState::Acknowledging);
        let subs = mgr.lock_subs();
        assert_eq!(
            subs.assigned_partitions(),
            HashSet::from([tp("topic1", 0), tp("topic1", 1), tp("topic1", 2)])
        );
    }

    /// Translated from `testReconciliationSkippedWhenSameAssignmentReceived`.
    /// After reconciling + ack'ing {0,1}, receiving the same assignment again
    /// does not re-trigger reconciliation (short-circuit) — no callback event
    /// is enqueued and the member stays STABLE.
    #[tokio::test]
    async fn reconciliation_skipped_when_same_assignment_received() {
        let (mgr, mut rx) = make(None);
        subscribe_with_listener(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);

        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile(0).await });
        expect_callback(
            &mut rx,
            ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
            &[tp("topic1", 0), tp("topic1", 1)],
            Ok(()),
        )
        .await;
        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);

        mgr.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);

        // Receive the same assignment again -> no reconciliation triggered.
        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Stable);
        // A reconcile call is a no-op (target == current); no event emitted.
        mgr.reconcile(0).await.unwrap();
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "no reconciliation should be triggered for an identical assignment"
        );
        assert_eq!(mgr.state(), MemberState::Stable);
        assert!(!mgr.reconciliation_in_progress());
    }

    /// Translated from `testUpdateStateFailsOnResponsesWithErrors`.
    #[test]
    fn update_state_fails_on_responses_with_errors() {
        let (mgr, _rx) = make(None);
        mgr.transition_to_joining().unwrap();
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::UnknownMemberId.code());
        data.set_member_id(Some(mgr.member_id()));
        data.set_member_epoch(5);
        let response = ShareGroupHeartbeatResponse::new(data);
        let err = mgr.on_heartbeat_success(&response).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
        // Error message content is part of the contract.
        assert!(err.to_string().contains("Unexpected error in Heartbeat response"));
    }

    /// Translated from `testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges`.
    #[tokio::test]
    async fn listeners_notified_of_member_epoch_only_if_changed() {
        use crate::consumer::internals::member_state_listener::MemberStateListener;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingListener {
            epoch_updates: AtomicUsize,
        }
        impl MemberStateListener for CountingListener {
            fn on_member_epoch_updated(&self, _member_epoch: Option<i32>, _member_id: &str) {
                self.epoch_updates.fetch_add(1, Ordering::SeqCst);
            }
            fn on_group_assignment_updated(&self, _partitions: &HashSet<TopicPartition>) {}
        }

        let (mgr, _rx) = make(None);
        mgr.transition_to_joining().unwrap();
        let listener = Arc::new(CountingListener { epoch_updates: AtomicUsize::new(0) });
        mgr.register_state_listener(listener.clone());

        // First response with epoch 5 -> one epoch-update notification.
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code());
        data.set_member_id(Some(mgr.member_id()));
        data.set_member_epoch(5);
        mgr.on_heartbeat_success(&ShareGroupHeartbeatResponse::new(data.clone()))
            .unwrap();
        assert_eq!(listener.epoch_updates.load(Ordering::SeqCst), 1);

        // Same epoch again -> no further notification.
        mgr.on_heartbeat_success(&ShareGroupHeartbeatResponse::new(data)).unwrap();
        assert_eq!(listener.epoch_updates.load(Ordering::SeqCst), 1);
    }

    /// Translated from `testMemberIdAndEpochResetOnFencedMembers`.
    #[tokio::test]
    async fn member_id_and_epoch_reset_on_fenced() {
        let mgr = create_member_in_stable_state().await;
        assert_eq!(mgr.member_epoch(), MEMBER_EPOCH);
        let member_id = mgr.member_id();

        mgr.transition_to_fenced(0).await.unwrap();
        // Fenced member resets epoch to 0 and rejoins.
        assert_eq!(mgr.member_epoch(), 0);
        assert_eq!(mgr.member_id(), member_id);
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// Translated from `testFatalFailureWhenStateIsStable` / `testTransitionToFatal`.
    #[tokio::test]
    async fn transition_to_fatal_when_stable() {
        let mgr = create_member_in_stable_state().await;
        let member_id = mgr.member_id();
        let epoch = mgr.member_epoch();

        mgr.transition_to_fatal(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
        // Keeps last member id and epoch.
        assert_eq!(mgr.member_id(), member_id);
        assert_eq!(mgr.member_epoch(), epoch);
    }

    /// Translated from `testTransitionToFailedWhenTryingToJoin`.
    #[tokio::test]
    async fn transition_to_fatal_when_trying_to_join() {
        let (mgr, _rx) = make(None);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.transition_to_joining().unwrap();
        mgr.transition_to_fatal(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from `testFencingWhenStateIsLeaving`. A member fenced while
    /// LEAVING transitions to UNSUBSCRIBED (no rejoin — it does not need to
    /// send a leave request).
    #[tokio::test]
    async fn fencing_when_state_is_leaving() {
        let mgr = create_member_in_stable_state().await;
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);

        mgr.transition_to_fenced(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from `testLeaveGroupEpoch` / `testLeaveGroupWhenStateIsStable`.
    #[tokio::test]
    async fn leave_group_resets_epoch_and_sends_leave() {
        let mgr = create_member_in_stable_state().await;
        mgr.leave_group(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.current_assignment().is_none());

        // Leave heartbeat sent -> UNSUBSCRIBED.
        mgr.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from `testIgnoreHeartbeatResponseWhenNotInGroup`. A response
    /// received while the member is FATAL is ignored (state unchanged).
    #[tokio::test]
    async fn ignore_heartbeat_response_when_not_in_group() {
        let (mgr, _rx) = make(None);
        mgr.transition_to_joining().unwrap();
        mgr.transition_to_fatal(0).await.unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from `testOnSubscriptionUpdatedTransitionsToJoiningOnPollIfNotInGroup`.
    #[test]
    fn on_subscription_updated_transitions_to_joining_on_poll_if_not_in_group() {
        let (mgr, _rx) = make(None);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.on_subscription_updated();
        assert!(mgr.subscription_updated());
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        mgr.on_consumer_poll().unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
    }

    /// Translated from `testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup`.
    #[tokio::test]
    async fn on_subscription_updated_does_not_transition_if_in_group() {
        let mgr = create_member_in_stable_state().await;
        mgr.on_subscription_updated();
        assert!(mgr.subscription_updated());
        mgr.on_consumer_poll().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);
        assert!(!mgr.subscription_updated());
    }

    /// Translated from `testStaleMemberRejoinsWhenTimerResetsNoCallbacks`.
    /// A member whose poll timer expired transitions to STALE, releases its
    /// (empty) assignment, then rejoins when the timer resets.
    #[tokio::test]
    async fn stale_member_rejoins_when_timer_resets() {
        let mgr = create_member_in_stable_state().await;
        // Poll timer expiry path: transition to LEAVING (dueToExpiredPollTimer),
        // then the heartbeat-generated hook moves LEAVING -> STALE.
        mgr.transition_to_sending_leave_group(true).unwrap();
        assert_eq!(mgr.member_epoch(), LEAVE_GROUP_MEMBER_EPOCH);
        mgr.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stale);

        // Release the (empty) assignment; no listener, so synchronous.
        mgr.transition_to_stale(0).await.unwrap();
        assert!(mgr.current_assignment().partitions.is_empty());

        // Timer reset -> rejoin.
        mgr.maybe_rejoin_stale_member();
        assert_eq!(mgr.state(), MemberState::Joining);
    }
}
