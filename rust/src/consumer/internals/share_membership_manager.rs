// Copyright 2026 Confluent Inc.
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
//! Composes [`super::AbstractMembershipManager`], which holds the
//! group-agnostic state machine and the reconcile chain, and supplies the
//! share-group [`super::MembershipManagerHooks`]: the
//! `ShareGroupHeartbeatResponse` accessors, the join / leave epochs, and an
//! assignment applied directly in the background.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareMembershipManager`.
//!
//! The share group has no rebalance listener, no auto-commit and no static
//! membership, so every other hook keeps the base's completed-future body:
//! a share reconciliation never waits on the application thread, and a share
//! leave releases nothing.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::Metadata;
use crate::common::internals::KafkaFutureImpl;
use crate::common::metrics::Metrics;
use crate::common::requests::{ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse};
use crate::common::utils::Time;
use crate::common::{Error, TopicPartition, Uuid};

use super::MemberState;
use super::MemberStateListener;
use super::PollResult;
use super::RequestManager;
use super::SubscriptionState;
use super::abstract_membership_manager::SignalAck;
use super::metrics::{RebalanceMetricsManager, ShareRebalanceMetricsManager};
use super::{AbstractMembershipManager, LocalAssignment, MembershipManagerHooks};

/// Group manager for a single consumer that has a group id defined in the
/// config `group.id` and the share group protocol to get automatically
/// assigned partitions when calling the subscribe API.
///
/// While the subscribe API hasn't been called (or if the consumer called
/// unsubscribe), this manager is only responsible for keeping the member in
/// the [`MemberState::Unsubscribed`] state, without joining the group.
///
/// If the consumer subscribe API is called, this manager uses the group id
/// to join the share group, and based on the share group protocol
/// heartbeats, handles the full lifecycle of the member as it joins the
/// group, reconciles assignments, handles fatal errors, and leaves the group.
///
/// Reconciliation process: the member accepts all assignments received from
/// the broker, resolves topic names from metadata, reconciles the resolved
/// assignments, and keeps the unresolved to be reconciled when discovered
/// with a metadata update. Reconciliations of resolved assignments are
/// executed sequentially and acknowledged to the server as they complete.
/// The member continues to heartbeat while reconciling, to make sure that it
/// stays in the group.
///
/// Reconciliation steps:
///
/// 1. Resolve topic names for all topic IDs received in the target
///    assignment. Topic names found in metadata are then ready to be
///    reconciled. Topic IDs not found are kept as unresolved, and the member
///    requests metadata updates until it resolves them (or the broker removes
///    them from the target assignment).
/// 2. When the above steps complete, the member acknowledges the reconciled
///    assignment, which is the subset of the target that was resolved from
///    metadata and actually reconciled. The ack is performed by sending a
///    heartbeat request back to the broker.
///
/// Java: `public class ShareMembershipManager extends
/// AbstractMembershipManager<ShareGroupHeartbeatResponse>`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager")]
pub(crate) struct ShareMembershipManager {
    pub(crate) abstract_mm: AbstractMembershipManager,
    /// Rack ID of the member, if specified.
    ///
    /// Java: `protected final String rackId` (nullable).
    rack_id: Option<String>,
}

impl ShareMembershipManager {
    /// Java: `ShareMembershipManager(LogContext, String groupId, String rackId,
    /// SubscriptionState, Metadata, Time, Metrics)`, which builds the
    /// [`ShareRebalanceMetricsManager`] over `metrics`. The `LogContext` is
    /// dropped (the `log` crate has no per-client context).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#ShareMembershipManager")]
    pub(crate) fn new(
        group_id: impl Into<String>,
        rack_id: Option<String>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<Metadata>,
        time: Arc<dyn Time>,
        metrics: &Arc<Metrics>,
    ) -> Self {
        Self::with_metrics_manager(
            group_id,
            rack_id,
            subscriptions,
            metadata,
            time,
            Arc::new(ShareRebalanceMetricsManager::new(metrics)),
        )
    }

    /// Java's visible-for-testing `ShareMembershipManager(LogContext, String
    /// groupId, String rackId, SubscriptionState, Metadata, Time,
    /// ShareRebalanceMetricsManager)`. Auto-commit is always off for a share
    /// group (Java passes `false` to the base).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#ShareMembershipManager")]
    pub(crate) fn with_metrics_manager(
        group_id: impl Into<String>,
        rack_id: Option<String>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<Metadata>,
        time: Arc<dyn Time>,
        metrics_manager: Arc<ShareRebalanceMetricsManager>,
    ) -> Self {
        let abstract_mm = AbstractMembershipManager::new(
            group_id,
            subscriptions,
            metadata,
            false,
            Some(metrics_manager as Arc<dyn RebalanceMetricsManager>),
            time,
        );
        Self { abstract_mm, rack_id }
    }

    /// The rack ID of the member, if specified.
    ///
    /// Java: `public String rackId()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#rackId")]
    pub(crate) fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Full reconciliations can always be triggered from the background
    /// thread for a share consumer (it fully updates the assignment), so this
    /// reconciles with `can_commit = true`. Never returns requests.
    ///
    /// A reconciliation error is logged by the base ("Reconciliation failed")
    /// and leaves the member reconciling, as Java's chain does; `poll` itself
    /// returns no error, as Java's does not throw.
    ///
    /// Java: `public PollResult poll(long currentTimeMs)` (override):
    /// `maybeReconcile(true); return PollResult.EMPTY;`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#poll")]
    pub(crate) fn poll(&self, current_time_ms: i64) -> PollResult {
        if let Err(e) = self.abstract_mm.maybe_reconcile(self, current_time_ms, true) {
            log::debug!("Share reconciliation did not complete: {e}");
        }
        PollResult::empty()
    }

    // The inherited `AbstractMembershipManager` methods, as the share
    // consumer's callers see them on the subclass (see the base module docs).

    /// Java: `groupId()` (inherited).
    pub(crate) fn group_id(&self) -> String {
        self.abstract_mm.group_id()
    }

    /// Java: `memberId()` (inherited).
    pub(crate) fn member_id(&self) -> String {
        self.abstract_mm.member_id()
    }

    /// Java: `memberEpoch()` (inherited).
    pub(crate) fn member_epoch(&self) -> i32 {
        self.abstract_mm.member_epoch()
    }

    /// Java: `state()` (inherited).
    pub(crate) fn state(&self) -> MemberState {
        self.abstract_mm.state()
    }

    /// Java: `currentAssignment()` (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn current_assignment(&self) -> LocalAssignment {
        self.abstract_mm.current_assignment()
    }

    /// Java: `onHeartbeatSuccess(ShareGroupHeartbeatResponse)` (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn on_heartbeat_success(&self, response: &ShareGroupHeartbeatResponse) -> Result<(), Error> {
        self.abstract_mm.on_heartbeat_success::<Self>(response)
    }

    /// Java: `onHeartbeatFailure(boolean retriable)` (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn on_heartbeat_failure(&self, retriable: bool) {
        self.abstract_mm.on_heartbeat_failure(retriable);
    }

    /// Java: `onHeartbeatRequestGenerated()` (inherited). Forwards to the base;
    /// as for the consumer, the stale-member release Java runs inside it is
    /// started by the heartbeat manager (`AbstractHeartbeatRequestManager`
    /// calls `transition_to_stale` right after).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn on_heartbeat_request_generated(&self) -> Result<(), Error> {
        self.abstract_mm.on_heartbeat_request_generated()
    }

    /// Java: `transitionToJoining()` (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn transition_to_joining(&self) -> Result<(), Error> {
        self.abstract_mm.transition_to_joining(self.join_group_epoch())
    }

    /// Java: `transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer)`
    /// (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn transition_to_sending_leave_group(&self, due_to_expired_poll_timer: bool) -> Result<(), Error> {
        self.abstract_mm
            .transition_to_sending_leave_group(self.leave_group_epoch(), due_to_expired_poll_timer)
    }

    /// Java: `transitionToFatal()` (inherited).
    pub(crate) fn transition_to_fatal(&self, current_time_ms: i64) -> Result<(), Error> {
        self.abstract_mm.transition_to_fatal(self, current_time_ms)
    }

    /// Java: `transitionToFenced()` (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn transition_to_fenced(&self, current_time_ms: i64) -> Result<(), Error> {
        self.abstract_mm.transition_to_fenced(self, current_time_ms)
    }

    /// Java: `transitionToStale()` (inherited).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production code reaches it through AbstractMembershipManager; only the tests call the subclass forwarder"
        )
    )]
    pub(crate) fn transition_to_stale(&self, current_time_ms: i64) -> Result<(), Error> {
        self.abstract_mm.transition_to_stale(self, current_time_ms)
    }

    /// Java: `maybeRejoinStaleMember()` (inherited).
    pub(crate) fn maybe_rejoin_stale_member(&self) {
        self.abstract_mm.maybe_rejoin_stale_member(self.join_group_epoch());
    }

    /// Java: `onSubscriptionUpdated()` (inherited).
    pub(crate) fn on_subscription_updated(&self) {
        self.abstract_mm.on_subscription_updated();
    }

    /// Java: `onConsumerPoll()` (inherited).
    pub(crate) fn on_consumer_poll(&self) -> Result<(), Error> {
        self.abstract_mm.on_consumer_poll(self.join_group_epoch())
    }

    /// Java: `leaveGroup()` (inherited).
    pub(crate) fn leave_group(&self, current_time_ms: i64) -> Result<KafkaFutureImpl<()>, Error> {
        self.abstract_mm.leave_group(self, current_time_ms)
    }

    /// Java: `maybeReconcile(boolean canCommit)` (inherited).
    pub(crate) fn maybe_reconcile(&self, current_time_ms: i64, can_commit: bool) -> Result<(), Error> {
        self.abstract_mm.maybe_reconcile(self, current_time_ms, can_commit)
    }

    /// Java: `registerStateListener(MemberStateListener)` (inherited).
    pub(crate) fn register_state_listener(&self, listener: Arc<dyn MemberStateListener>) {
        self.abstract_mm.register_state_listener(listener);
    }
}

/// The `ShareMembershipManager` overrides of Java's abstract
/// `AbstractMembershipManager` methods. The other hooks keep the base's
/// completed-future bodies (see the module docs).
impl MembershipManagerHooks for ShareMembershipManager {
    type Response = ShareGroupHeartbeatResponse;

    fn abstract_mm(&self) -> &AbstractMembershipManager {
        &self.abstract_mm
    }

    /// Java: `protected short errorCode(ShareGroupHeartbeatResponse response)`
    /// → `response.data().errorCode()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#errorCode")]
    fn error_code(response: &ShareGroupHeartbeatResponse) -> i16 {
        response.data().error_code
    }

    /// Java: `protected int memberEpoch(ShareGroupHeartbeatResponse response)`
    /// → `response.data().memberEpoch()`. Named per CLAUDE.md §2, beside the
    /// `memberEpoch()` getter ([`Self::member_epoch`]).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#memberEpoch")]
    fn member_epoch_with_response(response: &ShareGroupHeartbeatResponse) -> i32 {
        response.data().member_epoch
    }

    /// The new target assignment the heartbeat response carries, if any.
    /// Java builds each topic's partitions as a `TreeSet`, so the `Vec` here
    /// is sorted and deduplicated to match ([`LocalAssignment::update_with`]
    /// compares the `Vec`s by value).
    ///
    /// Java: `protected Optional<Map<Uuid, SortedSet<Integer>>>
    /// extractAssignment(ShareGroupHeartbeatResponse response)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#extractAssignment")]
    fn extract_assignment(response: &ShareGroupHeartbeatResponse) -> Option<HashMap<Uuid, Vec<i32>>> {
        let assignment = response.data().assignment.as_ref()?;
        let mut new_assignment = HashMap::with_capacity(assignment.topic_partitions.len());
        for topic_partition in &assignment.topic_partitions {
            let mut partitions = topic_partition.partitions.clone();
            partitions.sort_unstable();
            partitions.dedup();
            new_assignment.insert(topic_partition.topic_id, partitions);
        }
        Some(new_assignment)
    }

    /// For the share consumer, assignment changes are applied immediately in
    /// the background thread: the subscription takes the full assignment
    /// (the added partitions awaiting no callback), the state listeners are
    /// notified, and the result is Java's completed future (`None`).
    ///
    /// Java: `protected CompletableFuture<Void>
    /// signalPartitionsAssigned(TopicIdPartitionSet, SortedSet<TopicPartition>)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#signalPartitionsAssigned")]
    fn signal_partitions_assigned(
        &self,
        assigned_partitions: Vec<TopicPartition>,
        added_partitions: Vec<TopicPartition>,
        _current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error> {
        {
            let mut subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.assign_from_subscribed_awaiting_callback(&assigned_partitions, &added_partitions)?;
        }
        let assigned_set: HashSet<TopicPartition> = assigned_partitions.into_iter().collect();
        {
            let guard = match self.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.notify_assignment_change(&assigned_set);
        }
        Ok(None)
    }

    /// Java: `public int joinGroupEpoch()` →
    /// `ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#joinGroupEpoch")]
    fn join_group_epoch(&self) -> i32 {
        ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
    }

    /// Java: `public int leaveGroupEpoch()` →
    /// `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareMembershipManager#leaveGroupEpoch")]
    fn leave_group_epoch(&self) -> i32 {
        ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH
    }
}

/// Java's `poll` override, reached through the request-manager list.
impl RequestManager for ShareMembershipManager {
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        ShareMembershipManager::poll(self, current_time_ms)
    }
}

/// The same `poll`, for the instance `RequestManagers` shares with the share
/// heartbeat manager (Java holds one reference in each). Every method of
/// [`ShareMembershipManager`] takes `&self`, so the shared instance is polled
/// as Java's list entry is.
impl RequestManager for Arc<ShareMembershipManager> {
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        ShareMembershipManager::poll(self, current_time_ms)
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

/// Translated from `ShareMembershipManagerTest` (Kafka 4.4.0).
///
/// Java mocks `SubscriptionState` and `ConsumerMetadata` with Mockito; these
/// tests use the REAL `SubscriptionState` (subscribed to the share group) and a
/// real `Metadata` built by a `ConsumerMetadata` (Java's mocked type), seeded from a
/// `MetadataResponse`, as the `ConsumerMembershipManagerTest` translation does.
/// The Mockito verifications map to observable state:
///
/// - `verify(membershipManager).markReconciliationInProgress()` → the
///   `markPendingRevocationToPauseFetching` call that the base makes right after
///   it, recorded by `SubscriptionState::pending_revocation_marks_for_test`
///   (see [`tests::Fixture::reconciliation_marks`]);
/// - `verify(subscriptionState, times(n)).assignFromSubscribed(..)` → the
///   number of `SubscriptionState::assignment_id` bumps
///   ([`tests::Fixture::assign_from_subscribed_calls`]);
/// - `verify(subscriptionState).assignFromSubscribedAwaitingCallback(assigned, ..)`
///   → `subscriptions.assigned_partitions() == assigned`;
/// - `verify(subscriptionState).unsubscribe()` → the subscription type is back to
///   none;
/// - `verify(metadata, times(n)).requestUpdate(..)` →
///   `Metadata::request_update_call_count_for_test`.
///
/// Every Java test maps to the Rust test of the same name without the `test`
/// prefix, snake_cased:
///
/// - `testMembershipManagerRegistersForClusterMetadataUpdatesOnFirstJoin` → `membership_manager_registers_for_cluster_metadata_updates_on_first_join`
/// - `testReconcilingWhenReceivingAssignmentFoundInMetadata` → `reconciling_when_receiving_assignment_found_in_metadata`
/// - `testTransitionToReconcilingIfEmptyAssignmentReceived` → `transition_to_reconciling_if_empty_assignment_received`
/// - `testMemberIdAndEpochResetOnFencedMembers` → `member_id_and_epoch_reset_on_fenced_members`
/// - `testTransitionToFatal` → `transition_to_fatal`
/// - `testTransitionToFailedWhenTryingToJoin` → `transition_to_failed_when_trying_to_join`
/// - `testFencingWhenStateIsStable` → `fencing_when_state_is_stable`
/// - `testListenersGetNotifiedOnTransitionsToFatal` → `listeners_get_notified_on_transitions_to_fatal`
/// - `testListenersGetNotifiedOnTransitionsToLeavingGroup` → `listeners_get_notified_on_transitions_to_leaving_group`
/// - `testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges` → `listeners_get_notified_of_member_epoch_updates_only_if_it_changes`
/// - `testFencingWhenStateIsReconciling` → `fencing_when_state_is_reconciling`
/// - `testFencingWhenStateIsLeaving` → `fencing_when_state_is_leaving`
/// - `testLeaveGroupEpoch` → `leave_group_epoch`
/// - `testSameAssignmentReconciledAgainWhenFenced` → `same_assignment_reconciled_again_when_fenced`
/// - `testSameAssignmentReconciledAgainWithMissingTopic` → `same_assignment_reconciled_again_with_missing_topic`
/// - `testDelayedMetadataUsedToCompleteAssignment` → `delayed_metadata_used_to_complete_assignment`
/// - `testLeaveGroupWhenStateIsStable` → `leave_group_when_state_is_stable`
/// - `testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave` → `heartbeat_successful_response_when_leaving_group_completes_leave`
/// - `testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave` (`@ValueSource`) → `heartbeat_failed_response_when_leaving_group_completes_leave`
/// - `testIgnoreHeartbeatResponseWhenNotInGroup` (`@MethodSource`) → `ignore_heartbeat_response_when_not_in_group`
/// - `testIgnoreLeaveResponseWhenNotLeavingGroup` → `ignore_leave_response_when_not_leaving_group`
/// - `testLeaveGroupWhenStateIsReconciling` → `leave_group_when_state_is_reconciling`
/// - `testIgnoreHeartbeatWhenLeavingGroup` → `ignore_heartbeat_when_leaving_group`
/// - `testLeaveGroupWhenMemberOwnsAssignment` → `leave_group_when_member_owns_assignment`
/// - `testFencedWhenAssignmentEmpty` → `fenced_when_assignment_empty`
/// - `testLeaveGroupWhenMemberAlreadyLeaving` → `leave_group_when_member_already_leaving`
/// - `testLeaveGroupWhenMemberAlreadyLeft` → `leave_group_when_member_already_left`
/// - `testLeaveGroupWhenMemberIsStale` → `leave_group_when_member_is_stale`
/// - `testFatalFailureWhenStateIsJoining` → `fatal_failure_when_state_is_joining`
/// - `testFatalFailureWhenStateIsStable` → `fatal_failure_when_state_is_stable`
/// - `testFatalFailureWhenStateIsLeaving` → `fatal_failure_when_state_is_leaving`
/// - `testFatalFailureWhenMemberAlreadyLeft` → `fatal_failure_when_member_already_left`
/// - `testUpdateStateFailsOnResponsesWithErrors` → `update_state_fails_on_responses_with_errors`
/// - `testNewAssignmentReplacesPreviousOneWaitingOnMetadata` → `new_assignment_replaces_previous_one_waiting_on_metadata`
/// - `testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata` → `new_empty_assignment_replaces_previous_one_waiting_on_metadata`
/// - `testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata` → `new_assignment_not_in_metadata_replaces_previous_one_waiting_on_metadata`
/// - `testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived` → `unresolved_target_assignment_is_reconciled_when_metadata_received`
/// - `testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved` → `member_keeps_unresolved_assignment_waiting_for_metadata_until_resolved`
/// - `testReconcileNewPartitionsAssignedWhenNoPartitionOwned` → `reconcile_new_partitions_assigned_when_no_partition_owned`
/// - `testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned` → `reconcile_new_partitions_assigned_when_other_partitions_owned`
/// - `testReconciliationSkippedWhenSameAssignmentReceived` → `reconciliation_skipped_when_same_assignment_received`
/// - `testReconcileNewPartitionsAssignedAndRevoked` → `reconcile_new_partitions_assigned_and_revoked`
/// - `testMetadataUpdatesReconcilesUnresolvedAssignments` → `metadata_updates_reconciles_unresolved_assignments`
/// - `testMetadataUpdatesRequestsAnotherUpdateIfNeeded` → `metadata_updates_requests_another_update_if_needed`
/// - `testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable` → `revoke_partitions_uses_topic_names_local_cache_when_metadata_not_available`
/// - `testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup` → `on_subscription_updated_does_not_transition_to_joining_if_in_group`
/// - `testOnSubscriptionUpdatedTransitionsToJoiningOnPollIfNotInGroup` → `on_subscription_updated_transitions_to_joining_on_poll_if_not_in_group`
/// - `testTransitionToLeavingWhileReconcilingDueToStaleMember` → `transition_to_leaving_while_reconciling_due_to_stale_member`
/// - `testTransitionToLeavingWhileJoiningDueToStaleMember` → `transition_to_leaving_while_joining_due_to_stale_member`
/// - `testTransitionToLeavingWhileStableDueToStaleMember` → `transition_to_leaving_while_stable_due_to_stale_member`
/// - `testTransitionToLeavingWhileAcknowledgingDueToStaleMember` → `transition_to_leaving_while_acknowledging_due_to_stale_member`
/// - `testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover` → `stale_member_does_not_send_heartbeat_and_allows_transition_to_joining_to_recover`
/// - `testStaleMemberRejoinsWhenTimerResetsNoCallbacks` → `stale_member_rejoins_when_timer_resets_no_callbacks`
/// - `testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment` → `member_joining_transitions_to_stable_when_receiving_empty_assignment`
/// - `testMetricsWhenHeartbeatFailed` → `metrics_when_heartbeat_failed`
/// - `testRebalanceMetricsOnSuccessfulRebalance` → `rebalance_metrics_on_successful_rebalance`
/// - `testRebalanceMetricsOnFailedRebalance` → `rebalance_metrics_on_failed_rebalance`
///
/// No Java test is skipped.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetadataResponseData;
    use crate::ShareGroupHeartbeatResponseData;
    use crate::common::Metric;
    use crate::common::Node;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::protocol::{ApiKeys, Errors};
    use crate::common::requests::MetadataResponse;
    use crate::common::utils::MockTime;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::{AutoOffsetResetStrategy, ConsumerMetadata, MemberStateListener};
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};
    use crate::share_group_heartbeat_response_data::{Assignment, TopicPartitions};
    use std::cell::Cell;

    const GROUP_ID: &str = "test-group";
    const RACK_ID: Option<String> = None;
    const MEMBER_EPOCH: i32 = 1;

    /// The topics the real `SubscriptionState` is subscribed to: every topic
    /// name the Java tests resolve. Java's mocked `SubscriptionState` needs no
    /// subscription; the real one needs a share subscription for
    /// `hasAutoAssignedPartitions()` (Java stubs it `true`) and for `Metadata` to
    /// retain the seeded topics.
    const SUBSCRIBED_TOPICS: [&str; 3] = ["topic1", "topic2", "topic"];

    /// The Java test's fields (`subscriptionState`, `metadata`, `time`,
    /// `metrics`, `rebalanceMetricsManager`) plus the manager under test, and
    /// the baselines standing in for Mockito's `clearInvocations`.
    struct Fixture {
        mgr: ShareMembershipManager,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        time: Arc<MockTime>,
        metrics: Arc<Metrics>,
        rebalance_metrics_manager: Arc<ShareRebalanceMetricsManager>,
        reconciliation_marks_baseline: Cell<usize>,
        assign_from_subscribed_baseline: Cell<u32>,
        request_update_baseline: Cell<i64>,
        metadata_update_time_ms: Cell<i64>,
    }

    impl Fixture {
        /// Java `setup()` followed by `new ShareMembershipManager(logContext,
        /// GROUP_ID, RACK_ID, subscriptionState, metadata, time,
        /// rebalanceMetricsManager)`.
        fn new() -> Self {
            let time = Arc::new(MockTime::new());
            let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
            let rebalance_metrics_manager = Arc::new(ShareRebalanceMetricsManager::new(&metrics));
            let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
            subscriptions
                .lock()
                .unwrap()
                .subscribe_to_share_group(SUBSCRIBED_TOPICS.iter().map(|t| t.to_string()).collect())
                .unwrap();
            // Java mocks `ConsumerMetadata`; a real one supplies `topicNames()`.
            let config = ConsumerConfig {
                bootstrap_servers: vec!["localhost:9092".to_string()],
                ..ConsumerConfig::defaults()
            };
            let metadata =
                ConsumerMetadata::with_config(&config, Arc::clone(&subscriptions), ClusterResourceListeners::new());
            let mgr = ShareMembershipManager::with_metrics_manager(
                GROUP_ID,
                RACK_ID,
                Arc::clone(&subscriptions),
                metadata.metadata_arc(),
                Arc::clone(&time) as Arc<dyn Time>,
                Arc::clone(&rebalance_metrics_manager),
            );
            let fx = Self {
                mgr,
                subscriptions,
                time,
                metrics,
                rebalance_metrics_manager,
                reconciliation_marks_baseline: Cell::new(0),
                assign_from_subscribed_baseline: Cell::new(0),
                request_update_baseline: Cell::new(0),
                metadata_update_time_ms: Cell::new(1000),
            };
            fx.clear_manager_invocations();
            fx.clear_subscription_invocations();
            fx.clear_metadata_invocations();
            fx
        }

        fn now(&self) -> i64 {
            self.time.milliseconds()
        }

        /// Java `membershipManager.poll(time.milliseconds())`.
        fn poll(&self) {
            let _ = self.mgr.poll(self.now());
        }

        /// Java `membershipManager.onHeartbeatRequestGenerated()`. Java's
        /// LEAVING-with-expired-poll-timer branch calls `transitionToStale()`,
        /// which also releases the assignment; Rust splits that release into
        /// `transition_to_stale`, run right after by the heartbeat manager
        /// (`AbstractHeartbeatRequestManager::poll`), so this helper does the
        /// same.
        fn heartbeat_request_generated(&self) {
            let before = self.mgr.state();
            self.mgr.on_heartbeat_request_generated().unwrap();
            if before == MemberState::Leaving && self.mgr.state() == MemberState::Stale {
                self.mgr.transition_to_stale(self.now()).unwrap();
            }
        }

        fn on_heartbeat_success(&self, response: &ShareGroupHeartbeatResponse) {
            self.mgr.on_heartbeat_success(response).unwrap();
        }

        /// Java `membershipManager.leaveGroup()`.
        fn leave_group(&self) -> KafkaFutureImpl<()> {
            self.mgr.leave_group(self.now()).unwrap()
        }

        /// Java `clearInvocations(membershipManager)`.
        fn clear_manager_invocations(&self) {
            let marks = self.subscriptions.lock().unwrap().pending_revocation_marks_for_test().len();
            self.reconciliation_marks_baseline.set(marks);
        }

        /// Java `clearInvocations(subscriptionState)`.
        fn clear_subscription_invocations(&self) {
            let id = self.subscriptions.lock().unwrap().assignment_id();
            self.assign_from_subscribed_baseline.set(id);
        }

        /// Java `clearInvocations(metadata)`.
        fn clear_metadata_invocations(&self) {
            self.request_update_baseline
                .set(self.mgr.abstract_mm.metadata.request_update_call_count_for_test());
        }

        /// The `markPendingRevocationToPauseFetching` calls since the last
        /// [`Self::clear_manager_invocations`]. The base calls it right after
        /// `markReconciliationInProgress()` on every triggered reconciliation
        /// (and once more from `revokePartitions` when partitions are
        /// revoked), so it stands in for
        /// `verify(membershipManager).markReconciliationInProgress()`.
        fn reconciliation_marks(&self) -> Vec<Vec<TopicPartition>> {
            let subs = self.subscriptions.lock().unwrap();
            subs.pending_revocation_marks_for_test()[self.reconciliation_marks_baseline.get()..].to_vec()
        }

        /// Every `SubscriptionState::markPendingRevocation` call since the
        /// manager was created (Java never clears them in the tests that verify
        /// them).
        fn mark_pending_revocation_calls(&self, partitions: &[TopicPartition]) -> usize {
            let mut expected = partitions.to_vec();
            expected.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
            let subs = self.subscriptions.lock().unwrap();
            subs.pending_revocation_marks_for_test()
                .iter()
                .filter(|m| **m == expected)
                .count()
        }

        /// The `SubscriptionState::assignment_id` bumps since the last
        /// [`Self::clear_subscription_invocations`]: one per
        /// `assignFromSubscribed` / `assignFromSubscribedAwaitingCallback` /
        /// `unsubscribe` call. The stubbing helpers exclude their own bumps.
        fn assign_from_subscribed_calls(&self) -> u32 {
            let id = self.subscriptions.lock().unwrap().assignment_id();
            id.wrapping_sub(self.assign_from_subscribed_baseline.get())
        }

        /// Java `verify(metadata, times(n)).requestUpdate(anyBoolean())`.
        fn request_update_calls(&self) -> i64 {
            self.mgr.abstract_mm.metadata.request_update_call_count_for_test() - self.request_update_baseline.get()
        }

        fn assigned_partitions(&self) -> HashSet<TopicPartition> {
            self.subscriptions.lock().unwrap().assigned_partitions()
        }

        /// `verify(subscriptionState).unsubscribe()`: the real
        /// `unsubscribe()` resets the subscription type.
        fn is_unsubscribed(&self) -> bool {
            self.subscriptions.lock().unwrap().has_no_subscription_or_user_assignment()
        }

        /// Runs a stubbing step on the real `SubscriptionState`, excluding its
        /// `assignment_id` bumps from [`Self::assign_from_subscribed_calls`]
        /// (a Mockito `when(...)` is not an invocation).
        fn stub(&self, f: impl FnOnce(&mut SubscriptionState)) {
            let mut subs = self.subscriptions.lock().unwrap();
            let before = subs.assignment_id();
            f(&mut subs);
            let bumps = subs.assignment_id().wrapping_sub(before);
            self.assign_from_subscribed_baseline
                .set(self.assign_from_subscribed_baseline.get().wrapping_add(bumps));
        }

        /// `when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(value)`.
        fn stub_has_auto_assigned_partitions(&self, value: bool) {
            self.stub(|subs| {
                if value && !subs.has_auto_assigned_partitions() {
                    subs.subscribe_to_share_group(SUBSCRIBED_TOPICS.iter().map(|t| t.to_string()).collect())
                        .unwrap();
                } else if !value && subs.has_auto_assigned_partitions() {
                    subs.unsubscribe();
                }
            });
        }

        /// `when(subscriptionState.assignedPartitions()).thenReturn(partitions)`.
        fn stub_assigned_partitions(&self, partitions: &[TopicPartition]) {
            self.stub(|subs| subs.assign_from_subscribed(partitions).unwrap());
        }

        /// `when(metadata.topicNames()).thenReturn(topics)`: replaces the whole
        /// metadata snapshot with these topics.
        fn seed_metadata(&self, topics: &[(&str, Uuid)]) {
            let now = self.metadata_update_time_ms.get() + 1;
            self.metadata_update_time_ms.set(now);
            self.mgr.abstract_mm.metadata.update_with_current_request_version(
                &build_metadata_response(topics),
                false,
                now,
            );
        }

        /// Java's package-private `updateAssignment(partitions)`:
        /// `currentAssignment = new LocalAssignment(0, partitions)`.
        fn update_assignment(&self, partitions: HashMap<Uuid, Vec<i32>>) {
            let mut guard = self.mgr.abstract_mm.inner.lock().unwrap();
            guard.current_assignment = LocalAssignment::new(0, partitions).unwrap();
        }

        fn reconciliation_in_progress(&self) -> bool {
            self.mgr.abstract_mm.inner.lock().unwrap().reconciliation_in_progress
        }

        fn should_heartbeat_now(&self) -> bool {
            self.mgr.abstract_mm.inner.lock().unwrap().should_heartbeat_now()
        }

        fn should_skip_heartbeat(&self) -> bool {
            self.mgr.abstract_mm.inner.lock().unwrap().should_skip_heartbeat()
        }

        fn subscription_updated(&self) -> bool {
            self.mgr.abstract_mm.inner.lock().unwrap().subscription_updated
        }

        /// Java `currentAssignment().partitions`, with each partition list
        /// sorted (Java holds `SortedSet`s).
        fn current_partitions(&self) -> HashMap<Uuid, Vec<i32>> {
            sorted(self.mgr.current_assignment().partitions)
        }

        /// Java `topicPartitionsAwaitingReconciliation()`.
        fn topic_partitions_awaiting_reconciliation(&self) -> HashMap<Uuid, Vec<i32>> {
            let guard = self.mgr.abstract_mm.inner.lock().unwrap();
            if guard.current_target_assignment.is_none() {
                return HashMap::new();
            }
            if guard.current_assignment.is_none() {
                return sorted(guard.current_target_assignment.partitions.clone());
            }
            let mut out = HashMap::new();
            for (topic_id, target) in sorted(guard.current_target_assignment.partitions.clone()) {
                let reconciled = guard.current_assignment.partitions.get(&topic_id).map(|p| {
                    let mut p = p.clone();
                    p.sort_unstable();
                    p
                });
                if Some(&target) != reconciled.as_ref() {
                    let missing: Vec<i32> = target
                        .iter()
                        .copied()
                        .filter(|p| reconciled.as_ref().is_none_or(|r| !r.contains(p)))
                        .collect();
                    out.insert(topic_id, missing);
                }
            }
            out
        }

        /// Java `topicsAwaitingReconciliation()`.
        fn topics_awaiting_reconciliation(&self) -> HashSet<Uuid> {
            self.topic_partitions_awaiting_reconciliation().into_keys().collect()
        }

        fn metric_value(&self, name: &crate::common::MetricName) -> f64 {
            self.metrics.metric(name).unwrap().metric_value().as_double().unwrap()
        }
    }

    fn sorted(mut partitions: HashMap<Uuid, Vec<i32>>) -> HashMap<Uuid, Vec<i32>> {
        for p in partitions.values_mut() {
            p.sort_unstable();
            p.dedup();
        }
        partitions
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// One partition-0 topic per entry, as `consumer_membership_manager.rs`'s
    /// tests build it: `topicNames()` only needs the id ↔ name pairs.
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
        MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version())
    }

    /// Records `onMemberEpochUpdated` calls (Java `mock(MemberStateListener.class)`).
    #[derive(Default)]
    struct RecordingListener {
        epoch_updates: Mutex<Vec<(Option<i32>, String)>>,
    }

    impl RecordingListener {
        /// `verify(listener, times(n)).onMemberEpochUpdated(epoch, memberId)`.
        fn epoch_updates_with(&self, epoch: Option<i32>, member_id: &str) -> usize {
            let updates = self.epoch_updates.lock().unwrap();
            updates.iter().filter(|(e, id)| *e == epoch && id == member_id).count()
        }

        /// `clearInvocations(listener)`.
        fn clear(&self) {
            self.epoch_updates.lock().unwrap().clear();
        }
    }

    impl MemberStateListener for RecordingListener {
        fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str) {
            self.epoch_updates.lock().unwrap().push((member_epoch, member_id.to_string()));
        }
    }

    fn topic_partitions(topic_id: Uuid, partitions: Vec<i32>) -> TopicPartitions {
        let mut t = TopicPartitions::new();
        t.set_topic_id(topic_id).set_partitions(partitions);
        t
    }

    fn assignment(topic_partitions: Vec<TopicPartitions>) -> Assignment {
        let mut a = Assignment::new();
        a.set_topic_partitions(topic_partitions);
        a
    }

    /// Java `createShareGroupHeartbeatResponse(ShareGroupHeartbeatResponseData)`.
    fn create_share_group_heartbeat_response_with_data(
        data: ShareGroupHeartbeatResponseData,
    ) -> ShareGroupHeartbeatResponse {
        ShareGroupHeartbeatResponse::new(data)
    }

    /// Java `createShareGroupHeartbeatResponse(Assignment, String memberId)`.
    fn create_share_group_heartbeat_response(assignment: Assignment, member_id: String) -> ShareGroupHeartbeatResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_member_id(Some(member_id))
            .set_member_epoch(MEMBER_EPOCH)
            .set_assignment(Some(assignment));
        ShareGroupHeartbeatResponse::new(data)
    }

    /// Java `createShareGroupLeaveResponse(String memberId)`.
    fn create_share_group_leave_response(member_id: String) -> ShareGroupHeartbeatResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_member_id(Some(member_id))
            .set_member_epoch(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
        ShareGroupHeartbeatResponse::new(data)
    }

    /// Java `createShareGroupHeartbeatResponseWithError(String memberId)`.
    fn create_share_group_heartbeat_response_with_error(member_id: String) -> ShareGroupHeartbeatResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::UnknownMemberId.code())
            .set_member_id(Some(member_id))
            .set_member_epoch(5);
        ShareGroupHeartbeatResponse::new(data)
    }

    /// Java `assertMemberIdIsGenerated(String)`.
    fn assert_member_id_is_generated(original_member_id: &str) {
        assert!(!original_member_id.is_empty(), "Member Id should be generated at startup");
    }

    /// Java `createMembershipManager()`.
    fn create_membership_manager() -> Fixture {
        let fx = Fixture::new();
        assert_member_id_is_generated(&fx.mgr.member_id());
        fx
    }

    /// Java `createMembershipManagerJoiningGroup()`.
    fn create_membership_manager_joining_group() -> Fixture {
        let fx = Fixture::new();
        assert_member_id_is_generated(&fx.mgr.member_id());
        fx.mgr.transition_to_joining().unwrap();
        fx
    }

    /// Rust-only: the share reconciliation hands `SubscriptionState` the
    /// assignment in Java's order, topic name then partition
    /// (`TopicIdPartitionSet#topicPartitions()` passed by
    /// `ShareMembershipManager#signalPartitionsAssigned`), whatever the order
    /// of the heartbeat's assignment. Repeated over fresh managers so that a
    /// hash-ordered step would show.
    #[test]
    fn test_reconciliation_assigns_partitions_in_java_order() {
        for _ in 0..20 {
            let fx = create_membership_manager_joining_group();
            let (topic, topic1, topic2) = (Uuid::random_uuid(), Uuid::random_uuid(), Uuid::random_uuid());
            fx.seed_metadata(&[("topic", topic), ("topic1", topic1), ("topic2", topic2)]);
            fx.stub_has_auto_assigned_partitions(true);
            let target = assignment(vec![
                topic_partitions(topic2, vec![5, 3]),
                topic_partitions(topic, vec![1]),
                topic_partitions(topic1, vec![2, 0, 1]),
            ]);
            fx.on_heartbeat_success(&create_share_group_heartbeat_response(target, fx.mgr.member_id()));
            fx.poll();

            let expected = vec![
                tp("topic", 1),
                tp("topic1", 0),
                tp("topic1", 1),
                tp("topic1", 2),
                tp("topic2", 3),
                tp("topic2", 5),
            ];
            assert_eq!(expected, fx.subscriptions.lock().unwrap().assigned_partitions_list());
        }
    }

    /// Java `createAssignment(boolean mockMetadata)`.
    fn create_assignment(fx: &Fixture, mock_metadata: bool) -> Assignment {
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        if mock_metadata {
            fx.seed_metadata(&[("topic1", topic1), ("topic2", topic2)]);
        }
        assignment(vec![
            topic_partitions(topic1, vec![0, 1, 2]),
            topic_partitions(topic2, vec![3, 4, 5]),
        ])
    }

    /// Java `receiveAssignment(Uuid topicId, List<Integer> partitions, ...)`.
    fn receive_assignment(fx: &Fixture, topic_id: Uuid, partitions: Vec<i32>) {
        let response = create_share_group_heartbeat_response(
            assignment(vec![topic_partitions(topic_id, partitions)]),
            fx.mgr.member_id(),
        );
        fx.on_heartbeat_success(&response);
    }

    /// Java `receiveAssignment(Map<Uuid, SortedSet<Integer>>, ...)`.
    fn receive_assignment_map(fx: &Fixture, assignment_map: &[(Uuid, Vec<i32>)]) {
        let target = assignment(
            assignment_map
                .iter()
                .map(|(topic_id, partitions)| topic_partitions(*topic_id, partitions.clone()))
                .collect(),
        );
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(target, fx.mgr.member_id()));
    }

    /// Java `receiveEmptyAssignment(...)`.
    fn receive_empty_assignment(fx: &Fixture) {
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment(vec![]), fx.mgr.member_id()));
    }

    /// Java `mockJoinAndReceiveAssignment(boolean, Assignment)`;
    /// `assignment == None` is the one-argument overload
    /// (`createAssignment(triggerReconciliation)`).
    fn mock_join_and_receive_assignment(trigger_reconciliation: bool, assignment: Option<Assignment>) -> Fixture {
        let fx = create_membership_manager_joining_group();
        let assignment = assignment.unwrap_or_else(|| create_assignment(&fx, trigger_reconciliation));
        let heartbeat_response = create_share_group_heartbeat_response(assignment, fx.mgr.member_id());
        fx.stub_has_auto_assigned_partitions(true);

        fx.on_heartbeat_success(&heartbeat_response);

        if trigger_reconciliation {
            fx.poll();
            // verify(subscriptionState).assignFromSubscribedAwaitingCallback(anyCollection(), anyCollection())
            assert_eq!(1, fx.assign_from_subscribed_calls());
        } else {
            // verify(subscriptionState, never()).assignFromSubscribed(anyCollection())
            assert_eq!(0, fx.assign_from_subscribed_calls());
        }

        fx.clear_manager_invocations();
        fx
    }

    /// Java `createMemberInStableState()`.
    fn create_member_in_stable_state() -> Fixture {
        let fx = create_membership_manager_joining_group();
        let heartbeat_response = create_share_group_heartbeat_response(assignment(vec![]), fx.mgr.member_id());
        fx.stub_has_auto_assigned_partitions(true);
        // when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()):
        // `subscribeToShareGroup` registers no listener.
        fx.on_heartbeat_success(&heartbeat_response);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        fx.poll();
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stable, fx.mgr.state());

        fx.clear_subscription_invocations();
        fx.clear_manager_invocations();
        fx
    }

    /// Java `mockStableMember(membershipManager)`.
    fn mock_stable_member(fx: &Fixture) {
        let heartbeat_response = create_share_group_heartbeat_response(assignment(vec![]), fx.mgr.member_id());
        fx.on_heartbeat_success(&heartbeat_response);
        fx.poll();
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stable, fx.mgr.state());
        assert_eq!(MEMBER_EPOCH, fx.mgr.member_epoch());
    }

    /// Java `mockMemberHasAutoAssignedPartition()`.
    fn mock_member_has_auto_assigned_partition(fx: &Fixture) {
        fx.stub_has_auto_assigned_partitions(true);
        fx.stub_assigned_partitions(&[tp("topic1", 0)]);
    }

    /// Java `mockLeaveGroup()`. `doNothing().when(subscriptionState)
    /// .markPendingRevocation(anySet())` needs no counterpart: the share
    /// manager marks nothing on leave.
    fn mock_leave_group(fx: &Fixture) {
        mock_member_has_auto_assigned_partition(fx);
    }

    /// Java `mockRevocation()`: `doNothing()` on `markPendingRevocation`. The
    /// real `SubscriptionState` marks the (assigned) partitions; nothing to stub.
    fn mock_revocation() {}

    /// Java `mockOwnedPartitionAndAssignmentReceived(...)`.
    fn mock_owned_partition_and_assignment_received(
        fx: &Fixture,
        topic_id: Uuid,
        topic_name: &str,
        previously_owned: &[i32],
    ) {
        fx.stub_has_auto_assigned_partitions(true);
        let owned: Vec<TopicPartition> = previously_owned.iter().map(|p| tp(topic_name, *p)).collect();
        fx.stub_assigned_partitions(&owned);
        fx.update_assignment(HashMap::from([(topic_id, previously_owned.to_vec())]));
        fx.seed_metadata(&[(topic_name, topic_id)]);
    }

    /// Java `mockOwnedPartition(membershipManager, topicId, topic)`.
    fn mock_owned_partition(fx: &Fixture, topic_id: Uuid, topic: &str) {
        let partition = 0;
        fx.update_assignment(HashMap::from([(topic_id, vec![partition])]));
        fx.stub_has_auto_assigned_partitions(true);
        fx.stub_assigned_partitions(&[tp(topic, partition)]);
    }

    /// Java `mockMemberSuccessfullyReceivesAndAcksAssignment(...)`.
    fn mock_member_successfully_receives_and_acks_assignment(
        topic_id: Uuid,
        topic_name: &str,
        partitions: Vec<i32>,
    ) -> Fixture {
        let fx = create_membership_manager_joining_group();
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[]);

        receive_assignment(&fx, topic_id, partitions.clone());

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        let assigned: Vec<(Uuid, &str, i32)> = partitions.iter().map(|p| (topic_id, topic_name, *p)).collect();
        verify_reconciliation_triggered_and_completed(&fx, &assigned);
        fx
    }

    /// Java `verifyReconciliationNotTriggered(...)`: neither
    /// `markReconciliationInProgress()` nor `markReconciliationCompleted()`.
    fn verify_reconciliation_not_triggered(fx: &Fixture) {
        assert!(
            fx.reconciliation_marks().is_empty(),
            "no reconciliation should have been triggered: {:?}",
            fx.reconciliation_marks()
        );
        assert!(!fx.reconciliation_in_progress());
    }

    /// Java `verifyReconciliationTriggeredAndCompleted(...)`.
    fn verify_reconciliation_triggered_and_completed(fx: &Fixture, expected_assignment: &[(Uuid, &str, i32)]) {
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        // verify(membershipManager).markReconciliationInProgress(): exactly one
        // reconciliation, i.e. one pause-fetching mark, plus the identical one
        // `revokePartitions` adds when the reconciliation revokes partitions.
        let marks = fx.reconciliation_marks();
        assert!(
            marks.len() == 1 || (marks.len() == 2 && marks[0] == marks[1] && !marks[0].is_empty()),
            "exactly one reconciliation should have been triggered: {marks:?}"
        );
        // verify(membershipManager).markReconciliationCompleted()
        assert!(!fx.reconciliation_in_progress());

        // Assignment applied
        let expected_topic_partitions: HashSet<TopicPartition> =
            expected_assignment.iter().map(|(_, name, p)| tp(name, *p)).collect();
        assert_eq!(expected_topic_partitions, fx.assigned_partitions());
        assert_eq!(assignment_by_topic_id(expected_assignment), fx.current_partitions());
    }

    /// Java `assignmentByTopicId(List<TopicIdPartition>)`.
    fn assignment_by_topic_id(topic_id_partitions: &[(Uuid, &str, i32)]) -> HashMap<Uuid, Vec<i32>> {
        let mut out: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for (topic_id, _, partition) in topic_id_partitions {
            out.entry(*topic_id).or_default().push(*partition);
        }
        sorted(out)
    }

    /// Java `testRevocationCompleted(...)`.
    fn assert_revocation_completed(fx: &Fixture, expected_current_assignment: &[(Uuid, &str, i32)]) {
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        assert_eq!(assignment_by_topic_id(expected_current_assignment), fx.current_partitions());
        assert!(!fx.reconciliation_in_progress());

        let expected_set: HashSet<TopicPartition> =
            expected_current_assignment.iter().map(|(_, name, p)| tp(name, *p)).collect();
        // verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedSet, Set.of())
        // Deviation: the added set (`Set.of()`) is not observable — the share
        // reconcile clears the awaiting-callback mark in the same call.
        assert_eq!(expected_set, fx.assigned_partitions());
    }

    /// Java `testFencedMemberReleasesAssignmentAndTransitionsToJoining(...)`.
    fn assert_fenced_member_releases_assignment_and_transitions_to_joining(fx: &Fixture) {
        mock_member_has_auto_assigned_partition(fx);

        fx.mgr.transition_to_fenced(fx.now()).unwrap();

        assert_eq!(0, fx.mgr.member_epoch());
        assert_eq!(MemberState::Joining, fx.mgr.state());
    }

    /// Java `testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(...)`.
    fn assert_leave_group_releases_assignment_and_resets_epoch_to_send_leave_group(fx: &Fixture) {
        mock_leave_group(fx);

        let leave_result = fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());

        assert_eq!(MemberState::Leaving, fx.mgr.state());
        assert!(
            !leave_result.is_done(),
            "Leave group result should not complete until the heartbeat request to leave is sent out."
        );

        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(fx, &leave_result);
        assert_eq!(-1, fx.mgr.member_epoch());
        assert!(fx.mgr.current_assignment().is_none());
        // verify(subscriptionState).assignFromSubscribed(Set.of())
        // Deviation: the real `unsubscribe()` that precedes `clearAssignment()`
        // turns `hasAutoAssignedPartitions()` false, so `clearAssignment()` does
        // not call `assignFromSubscribed` (Java's mock keeps it `true`); the
        // assignment it would clear is already empty.
        assert!(fx.assigned_partitions().is_empty());
    }

    /// Java `assertTransitionToUnsubscribeOnHBSentAndWaitForResponseToCompleteLeave(...)`.
    fn assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(
        fx: &Fixture,
        send_leave: &KafkaFutureImpl<()>,
    ) {
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        fx.heartbeat_request_generated();
        assert!(
            !send_leave.is_done(),
            "Send leave operation should not complete until a response is received"
        );
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());

        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment(vec![]), fx.mgr.member_id()));

        assert!(
            !send_leave.is_done(),
            "Send leave operation should not complete until a leave response is received"
        );

        fx.on_heartbeat_success(&create_share_group_leave_response(fx.mgr.member_id()));

        assert_send_leave_completed(fx, send_leave);
    }

    /// Java `assertFalse(future.isCompletedExceptionally())` on a done future.
    fn assert_not_completed_exceptionally(future: &KafkaFutureImpl<()>) {
        use futures_util::FutureExt;
        let result = future.future().get().now_or_never();
        assert!(
            !matches!(result, Some(Err(_))),
            "the future must not complete with an error: {result:?}"
        );
    }

    /// Java `assertSendLeaveCompleted(...)`.
    fn assert_send_leave_completed(fx: &Fixture, send_leave: &KafkaFutureImpl<()>) {
        assert_eq!(
            MemberState::Unsubscribed,
            fx.mgr.state(),
            "Member should remain UNSUBSCRIBED after receiving the response to the HB to leave"
        );
        assert_eq!(-1, fx.mgr.member_epoch());
        assert!(fx.mgr.current_assignment().is_none());
        assert!(
            send_leave.is_done(),
            "Leave group result should complete when the response to the heartbeat request to leave is received."
        );
        assert_not_completed_exceptionally(send_leave);
    }

    /// Java `testFenceIsNoOp(...)`.
    fn assert_fence_is_no_op(fx: &Fixture) {
        assert_ne!(0, fx.mgr.member_epoch());
        // verify(subscriptionState, never()).rebalanceListener()
        // Deviation: the share manager never reads a rebalance listener, so
        // Java's check is vacuous here, and so is this one: the preceding
        // leave already unsubscribed, so a fence that did release would not
        // call `assign_from_subscribed` either. The epoch check above and the
        // caller's state and leave-future checks carry the test.
        assert_eq!(0, fx.assign_from_subscribed_calls());
    }

    /// Java `testStateUpdateOnFatalFailure(...)`.
    fn assert_state_update_on_fatal_failure(fx: &Fixture) {
        let member_id = fx.mgr.member_id();
        let last_epoch = fx.mgr.member_epoch();
        fx.stub_has_auto_assigned_partitions(true);
        fx.mgr.transition_to_fatal(fx.now()).unwrap();
        assert_eq!(MemberState::Fatal, fx.mgr.state());
        // Should keep its last member id and epoch.
        assert_eq!(member_id, fx.mgr.member_id());
        assert_eq!(last_epoch, fx.mgr.member_epoch());
    }

    /// Java `assertLeaveGroupDueToExpiredPollAndTransitionToStale(...)`.
    fn assert_leave_group_due_to_expired_poll_and_transition_to_stale(fx: &Fixture) {
        // assertDoesNotThrow
        fx.mgr.transition_to_sending_leave_group(true).unwrap();
        assert_eq!(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH, fx.mgr.member_epoch());
        fx.heartbeat_request_generated();
        assert_stale_member_leaves_group_and_clears_assignment(fx);
    }

    /// Java `assertStaleMemberLeavesGroupAndClearsAssignment(...)`.
    fn assert_stale_member_leaves_group_and_clears_assignment(fx: &Fixture) {
        assert_eq!(MemberState::Stale, fx.mgr.state());

        // Should reset epoch to leave the group and release the assignment (right away because
        // there is no onPartitionsLost callback defined)
        // verify(subscriptionState).assignFromSubscribed(Set.of())
        assert_eq!(1, fx.assign_from_subscribed_calls());
        assert!(fx.assigned_partitions().is_empty());
        assert!(fx.mgr.current_assignment().partitions.is_empty());
        assert!(fx.topics_awaiting_reconciliation().is_empty());
        assert_eq!(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH, fx.mgr.member_epoch());
    }

    /// Java `mockStaleMember()`.
    fn mock_stale_member() -> Fixture {
        let fx = create_member_in_stable_state();
        // doNothing().when(subscriptionState).assignFromSubscribed(any())
        fx.stub_has_auto_assigned_partitions(true);
        fx.mgr.transition_to_sending_leave_group(true).unwrap();
        fx.heartbeat_request_generated();
        fx
    }

    /// Java `memberJoinWithAssignment()`.
    fn member_join_with_assignment() -> Fixture {
        let topic_id = Uuid::random_uuid();
        let fx = mock_join_and_receive_assignment(true, None);
        fx.heartbeat_request_generated();
        fx.seed_metadata(&[("topic", topic_id)]);
        receive_assignment(&fx, topic_id, vec![0]);
        fx.heartbeat_request_generated();
        assert!(!fx.mgr.current_assignment().is_none());
        fx
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    /// Translated from `ShareMembershipManagerTest#testMembershipManagerRegistersForClusterMetadataUpdatesOnFirstJoin`.
    #[test]
    fn membership_manager_registers_for_cluster_metadata_updates_on_first_join() {
        // First join should register to get metadata updates
        let fx = Fixture::new();
        fx.mgr.transition_to_joining().unwrap();
        fx.clear_metadata_invocations();

        // Following joins should not register again.
        receive_empty_assignment(&fx);
        mock_leave_group(&fx);
        fx.leave_group();
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        fx.mgr.transition_to_joining().unwrap();
    }

    /// Translated from `ShareMembershipManagerTest#testReconcilingWhenReceivingAssignmentFoundInMetadata`.
    #[test]
    fn reconciling_when_receiving_assignment_found_in_metadata() {
        let fx = mock_join_and_receive_assignment(true, None);
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());

        // When the ack is sent the member should go back to STABLE
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stable, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToReconcilingIfEmptyAssignmentReceived`.
    #[test]
    fn transition_to_reconciling_if_empty_assignment_received() {
        let fx = create_membership_manager_joining_group();
        assert_eq!(MemberState::Joining, fx.mgr.state());

        let response_without_assignment = create_share_group_heartbeat_response(Assignment::new(), fx.mgr.member_id());
        fx.on_heartbeat_success(&response_without_assignment);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());

        let response_with_assignment =
            create_share_group_heartbeat_response(create_assignment(&fx, true), fx.mgr.member_id());
        fx.on_heartbeat_success(&response_with_assignment);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testMemberIdAndEpochResetOnFencedMembers`.
    #[test]
    fn member_id_and_epoch_reset_on_fenced_members() {
        let fx = create_member_in_stable_state();
        assert_eq!(MEMBER_EPOCH, fx.mgr.member_epoch());

        mock_member_has_auto_assigned_partition(&fx);

        fx.mgr.transition_to_fenced(fx.now()).unwrap();
        assert_eq!(0, fx.mgr.member_epoch());
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToFatal`.
    #[test]
    fn transition_to_fatal() {
        let fx = create_member_in_stable_state();
        assert_eq!(MEMBER_EPOCH, fx.mgr.member_epoch());

        fx.stub_has_auto_assigned_partitions(true);
        fx.mgr.transition_to_fatal(fx.now()).unwrap();
        assert_eq!(MemberState::Fatal, fx.mgr.state());
        // verify(subscriptionState).assignFromSubscribed(Set.of())
        assert_eq!(1, fx.assign_from_subscribed_calls());
        assert!(fx.assigned_partitions().is_empty());

        fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToFailedWhenTryingToJoin`.
    #[test]
    fn transition_to_failed_when_trying_to_join() {
        let fx = Fixture::new();
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        fx.mgr.transition_to_joining().unwrap();

        fx.stub_has_auto_assigned_partitions(true);
        fx.mgr.transition_to_fatal(fx.now()).unwrap();
        assert_eq!(MemberState::Fatal, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testFencingWhenStateIsStable`.
    #[test]
    fn fencing_when_state_is_stable() {
        let fx = create_member_in_stable_state();
        assert_fenced_member_releases_assignment_and_transitions_to_joining(&fx);
        // verify(subscriptionState).assignFromSubscribed(Set.of())
        assert_eq!(1, fx.assign_from_subscribed_calls());
        assert!(fx.assigned_partitions().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testListenersGetNotifiedOnTransitionsToFatal`.
    #[test]
    fn listeners_get_notified_on_transitions_to_fatal() {
        let fx = create_membership_manager_joining_group();
        let listener = Arc::new(RecordingListener::default());
        fx.mgr
            .abstract_mm
            .register_state_listener(Arc::clone(&listener) as Arc<dyn MemberStateListener>);
        mock_stable_member(&fx);
        assert_eq!(1, listener.epoch_updates_with(Some(MEMBER_EPOCH), &fx.mgr.member_id()));
        listener.clear();

        // Transition to FAILED before getting member ID/epoch
        fx.mgr.transition_to_fatal(fx.now()).unwrap();
        assert_eq!(MemberState::Fatal, fx.mgr.state());
        assert_eq!(1, listener.epoch_updates_with(None, &fx.mgr.member_id()));
    }

    /// Translated from `ShareMembershipManagerTest#testListenersGetNotifiedOnTransitionsToLeavingGroup`.
    #[test]
    fn listeners_get_notified_on_transitions_to_leaving_group() {
        let fx = create_membership_manager_joining_group();
        let listener = Arc::new(RecordingListener::default());
        fx.mgr
            .abstract_mm
            .register_state_listener(Arc::clone(&listener) as Arc<dyn MemberStateListener>);
        mock_stable_member(&fx);
        assert_eq!(1, listener.epoch_updates_with(Some(MEMBER_EPOCH), &fx.mgr.member_id()));
        listener.clear();

        mock_leave_group(&fx);
        fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        assert_eq!(1, listener.epoch_updates_with(None, &fx.mgr.member_id()));
    }

    /// Translated from `ShareMembershipManagerTest#testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges`.
    #[test]
    fn listeners_get_notified_of_member_epoch_updates_only_if_it_changes() {
        let fx = create_membership_manager_joining_group();
        let listener = Arc::new(RecordingListener::default());
        fx.mgr
            .abstract_mm
            .register_state_listener(Arc::clone(&listener) as Arc<dyn MemberStateListener>);
        let epoch = 5;

        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_member_id(Some(fx.mgr.member_id()))
            .set_member_epoch(epoch);
        fx.on_heartbeat_success(&create_share_group_heartbeat_response_with_data(data.clone()));

        assert_eq!(1, listener.epoch_updates_with(Some(epoch), &fx.mgr.member_id()));
        listener.clear();

        fx.on_heartbeat_success(&create_share_group_heartbeat_response_with_data(data));
        // verify(listener, never()).onMemberEpochUpdated(any(), any())
        assert!(listener.epoch_updates.lock().unwrap().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testFencingWhenStateIsReconciling`.
    #[test]
    fn fencing_when_state_is_reconciling() {
        let fx = mock_join_and_receive_assignment(false, None);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());

        assert_fenced_member_releases_assignment_and_transitions_to_joining(&fx);
        // verify(subscriptionState).assignFromSubscribed(Set.of())
        assert_eq!(1, fx.assign_from_subscribed_calls());
        assert!(fx.assigned_partitions().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testFencingWhenStateIsLeaving`.
    #[test]
    fn fencing_when_state_is_leaving() {
        let fx = create_member_in_stable_state();

        // Start leaving group.
        mock_leave_group(&fx);
        let leave_operation = fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
        assert_eq!(MemberState::Leaving, fx.mgr.state());

        // Get fenced while preparing to leave the group. Member should ignore the fence
        // (no rejoin) and should continue leaving the group as it was before getting fenced.
        fx.clear_subscription_invocations();
        fx.mgr.transition_to_fenced(fx.now()).unwrap();
        assert_fence_is_no_op(&fx);
        assert_eq!(
            MemberState::Unsubscribed,
            fx.mgr.state(),
            "Member should transition from LEAVING to UNSUBSCRIBED when getting fenced (it does not need to send \
             leave request if fenced"
        );
        assert!(
            leave_operation.is_done(),
            "Fenced member should complete the ongoing leave operation"
        );
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupEpoch`.
    #[test]
    fn leave_group_epoch() {
        // Member should leave the group with epoch -1.
        let fx = create_member_in_stable_state();
        mock_leave_group(&fx);
        fx.leave_group();
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        assert_eq!(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH, fx.mgr.member_epoch());
    }

    /// Translated from `ShareMembershipManagerTest#testSameAssignmentReconciledAgainWhenFenced`.
    ///
    /// This is the case where a member rejoins (due to fence). If the member
    /// gets the same assignment again, it should still reconcile and ack the
    /// assignment.
    #[test]
    fn same_assignment_reconciled_again_when_fenced() {
        let fx = create_member_in_stable_state();
        let topic1 = Uuid::random_uuid();
        let assignment1 = Assignment::new();
        let assignment2 = assignment(vec![topic_partitions(topic1, vec![0, 1, 2])]);
        fx.seed_metadata(&[("topic1", topic1)]);
        assert_eq!(HashMap::new(), fx.current_partitions());

        // Receive assignment, wait on commit
        let member_id = fx.mgr.member_id();
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment2, member_id.clone()));
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        // Java's `commitResult` future is never chained to anything (a share
        // reconciliation does not commit), so completing it is a no-op; it has
        // no Rust counterpart.
        fx.poll();

        // Get fenced, commit completes
        fx.mgr.transition_to_fenced(fx.now()).unwrap();

        assert_eq!(MemberState::Joining, fx.mgr.state());
        assert!(fx.mgr.current_assignment().is_none());
        assert!(fx.assigned_partitions().is_empty());

        assert_eq!(MemberState::Joining, fx.mgr.state());
        assert!(fx.mgr.current_assignment().is_none());
        assert!(fx.assigned_partitions().is_empty());

        // We have to reconcile & ack the assignment again
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment1, member_id));
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        fx.poll();
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stable, fx.mgr.state());
        assert_eq!(HashMap::new(), fx.current_partitions());
    }

    /// Translated from `ShareMembershipManagerTest#testSameAssignmentReconciledAgainWithMissingTopic`.
    ///
    /// This is the case where we receive a new assignment while reconciling an
    /// existing one. The intermediate assignment is not applied, and a new
    /// assignment containing the same partitions is received and reconciled.
    /// In all assignments, one topic is not resolvable.
    ///
    /// We need to make sure that the last assignment is acked and applied, even
    /// though the set of partitions does not change. In this case, no rebalance
    /// listeners are run.
    #[test]
    fn same_assignment_reconciled_again_with_missing_topic() {
        let fx = create_member_in_stable_state();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        let assignment1 = || assignment(vec![topic_partitions(topic1, vec![0]), topic_partitions(topic2, vec![0])]);
        let assignment2 = assignment(vec![topic_partitions(topic1, vec![0, 1]), topic_partitions(topic2, vec![0])]);
        fx.seed_metadata(&[("topic1", topic1)]);

        // Receive assignment - full reconciliation triggered
        // stay in RECONCILING state, since an unresolved topic is assigned
        let member_id = fx.mgr.member_id();
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment1(), member_id.clone()));
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        fx.poll();
        verify_reconciliation_triggered_and_completed(&fx, &[(topic1, "topic1", 0)]);
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        fx.clear_manager_invocations();

        // Receive extended assignment - assignment received but no reconciliation triggered
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment2, member_id.clone()));
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        verify_reconciliation_not_triggered(&fx);

        // Receive original assignment again - full reconciliation not triggered but assignment is acked again
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment1(), member_id));
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        fx.poll();
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        verify_reconciliation_not_triggered(&fx);
        assert_eq!(HashMap::from([(topic1, vec![0])]), fx.current_partitions());
        assert_eq!(HashSet::from([topic2]), fx.topics_awaiting_reconciliation());
    }

    /// Translated from `ShareMembershipManagerTest#testDelayedMetadataUsedToCompleteAssignment`.
    ///
    /// Tests the case where topic metadata is not available at the time of the
    /// assignment, but is made available later.
    #[test]
    fn delayed_metadata_used_to_complete_assignment() {
        let topic_id1 = Uuid::random_uuid();
        let topic1 = "topic1";
        let topic_id1_partition0 = (topic_id1, topic1, 0);

        let topic_id2 = Uuid::random_uuid();
        let topic2 = "topic2";
        let topic_id2_partition0 = (topic_id2, topic2, 0);

        // Receive assignment with only topic1-0, entering STABLE state.
        let fx = mock_member_successfully_receives_and_acks_assignment(topic_id1, topic1, vec![0]);

        fx.heartbeat_request_generated();

        assert_eq!(MemberState::Stable, fx.mgr.state());
        fx.stub_assigned_partitions(&[tp(topic1, 0)]);
        fx.clear_manager_invocations();
        fx.clear_subscription_invocations();

        // New assignment adding a new topic2-0 (not in metadata).
        // No reconciliation triggered, because new topic in assignment is waiting for metadata.
        receive_assignment_map(&fx, &[(topic_id1, vec![0]), (topic_id2, vec![0])]);
        fx.poll();

        // No full reconciliation triggered, but assignment needs to be acknowledged.
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        assert!(fx.should_heartbeat_now());

        fx.heartbeat_request_generated();

        verify_reconciliation_not_triggered(&fx);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert_eq!(HashSet::from([topic_id2]), fx.topics_awaiting_reconciliation());
        // verify(metadata).requestUpdate(anyBoolean()): `metadata` was never
        // cleared, so this counts from the start of the test.
        assert_eq!(1, fx.request_update_calls());
        fx.clear_manager_invocations();

        // Metadata discovered for topic2. Should trigger reconciliation to complete the assignment,
        // with membership manager entering ACKNOWLEDGING state.
        fx.seed_metadata(&[(topic1, topic_id1), (topic2, topic_id2)]);

        fx.poll();

        verify_reconciliation_triggered_and_completed(&fx, &[topic_id1_partition0, topic_id2_partition0]);
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupWhenStateIsStable`.
    #[test]
    fn leave_group_when_state_is_stable() {
        let fx = create_member_in_stable_state();
        assert_leave_group_releases_assignment_and_resets_epoch_to_send_leave_group(&fx);
        // verify(subscriptionState).assignFromSubscribed(Set.of()): see the
        // deviation in the helper; the assignment is empty.
        assert!(fx.assigned_partitions().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave`.
    #[test]
    fn heartbeat_successful_response_when_leaving_group_completes_leave() {
        let fx = create_member_in_stable_state();
        mock_leave_group(&fx);

        let leave_result = fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        assert!(!leave_result.is_done());

        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        assert!(!leave_result.is_done());

        fx.on_heartbeat_success(&create_share_group_heartbeat_response(
            create_assignment(&fx, true),
            fx.mgr.member_id(),
        ));
        assert!(!leave_result.is_done());

        fx.on_heartbeat_success(&create_share_group_leave_response(fx.mgr.member_id()));
        assert_send_leave_completed(&fx, &leave_result);
    }

    /// Translated from `ShareMembershipManagerTest#testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave`
    /// (`@ValueSource(booleans = {true, false})`).
    #[test]
    fn heartbeat_failed_response_when_leaving_group_completes_leave() {
        for retriable_response_error in [true, false] {
            let fx = create_member_in_stable_state();
            mock_leave_group(&fx);

            let leave_result = fx.leave_group();
            assert_eq!(MemberState::Leaving, fx.mgr.state());
            assert!(!leave_result.is_done());

            fx.heartbeat_request_generated();
            assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
            assert!(!leave_result.is_done());

            fx.mgr.on_heartbeat_failure(retriable_response_error);
            assert_send_leave_completed(&fx, &leave_result);
        }
    }

    /// Translated from `ShareMembershipManagerTest#testIgnoreHeartbeatResponseWhenNotInGroup`
    /// (`@MethodSource("notInGroupStates")`).
    #[test]
    fn ignore_heartbeat_response_when_not_in_group() {
        // notInGroupStates()
        for state in [
            MemberState::Unsubscribed,
            MemberState::Fenced,
            MemberState::Fatal,
            MemberState::Stale,
        ] {
            let fx = create_membership_manager();
            // when(membershipManager.state()).thenReturn(state)
            fx.mgr.abstract_mm.inner.lock().unwrap().state = state;
            // Deviation: Java passes a Mockito mock of the response data and
            // verifies that `memberId()`, `memberEpoch()` (except in
            // UNSUBSCRIBED) and `assignment()` are never read. Accessor reads are
            // not observable in Rust, so the response carries a member id, a
            // positive epoch and an assignment, and the test asserts that none
            // of them reached the member state.
            let mut data = ShareGroupHeartbeatResponseData::new();
            data.set_member_id(Some("other-member-id".to_string()))
                .set_member_epoch(5)
                .set_assignment(Some(assignment(vec![topic_partitions(Uuid::random_uuid(), vec![0])])));
            let member_id = fx.mgr.member_id();

            fx.on_heartbeat_success(&create_share_group_heartbeat_response_with_data(data));

            assert_eq!(state, fx.mgr.state());
            assert_eq!(member_id, fx.mgr.member_id());
            assert_eq!(0, fx.mgr.member_epoch());
            assert!(fx.mgr.abstract_mm.inner.lock().unwrap().current_target_assignment.is_none());
        }
    }

    /// Translated from `ShareMembershipManagerTest#testIgnoreLeaveResponseWhenNotLeavingGroup`.
    #[test]
    fn ignore_leave_response_when_not_leaving_group() {
        let fx = create_member_in_stable_state();

        let leave_result = fx.leave_group();

        // Send leave request, transitioning to UNSUBSCRIBED state
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());

        // Receive a previous heartbeat response, which should be ignored
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_member_id(Some(fx.mgr.member_id()))
            .set_member_epoch(MEMBER_EPOCH);
        fx.on_heartbeat_success(&ShareGroupHeartbeatResponse::new(data));
        assert!(!leave_result.is_done());

        // Receive a leave heartbeat response, which should unblock the consumer
        fx.on_heartbeat_success(&create_share_group_leave_response(fx.mgr.member_id()));
        assert!(leave_result.is_done());

        // Share unblocks and updates subscription
        fx.mgr.on_subscription_updated();
        fx.mgr.on_consumer_poll().unwrap();

        fx.on_heartbeat_success(&create_share_group_leave_response(fx.mgr.member_id()));

        assert_eq!(MemberState::Joining, fx.mgr.state());
        assert_eq!(0, fx.mgr.member_epoch());
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupWhenStateIsReconciling`.
    #[test]
    fn leave_group_when_state_is_reconciling() {
        let fx = mock_join_and_receive_assignment(false, None);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());

        assert_leave_group_releases_assignment_and_resets_epoch_to_send_leave_group(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testIgnoreHeartbeatWhenLeavingGroup`.
    #[test]
    fn ignore_heartbeat_when_leaving_group() {
        let fx = create_member_in_stable_state();
        mock_leave_group(&fx);

        let leave_result = fx.leave_group();

        fx.on_heartbeat_success(&create_share_group_heartbeat_response(
            create_assignment(&fx, true),
            fx.mgr.member_id(),
        ));

        assert_eq!(MemberState::Leaving, fx.mgr.state());
        assert_eq!(-1, fx.mgr.member_epoch());
        assert!(fx.mgr.current_assignment().partitions.is_empty());
        assert!(
            !leave_result.is_done(),
            "Leave group result should not complete until the heartbeat request to leave is sent out."
        );
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupWhenMemberOwnsAssignment`.
    #[test]
    fn leave_group_when_member_owns_assignment() {
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";
        let fx = create_membership_manager_joining_group();
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[]);

        receive_assignment(&fx, topic_id, vec![0, 1]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        let assigned_partitions = [(topic_id, topic_name, 0), (topic_id, topic_name, 1)];
        verify_reconciliation_triggered_and_completed(&fx, &assigned_partitions);

        assert_eq!(1, fx.mgr.current_assignment().partitions.len());

        assert_leave_group_releases_assignment_and_resets_epoch_to_send_leave_group(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testFencedWhenAssignmentEmpty`.
    #[test]
    fn fenced_when_assignment_empty() {
        let fx = create_member_in_stable_state();

        // Clear the assignment
        fx.stub_assigned_partitions(&[]);
        fx.stub_has_auto_assigned_partitions(false);

        fx.mgr.transition_to_fenced(fx.now()).unwrap();

        // Make sure to never call `assignFromSubscribed` again
        assert_eq!(0, fx.assign_from_subscribed_calls());
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupWhenMemberAlreadyLeaving`.
    #[test]
    fn leave_group_when_member_already_leaving() {
        let fx = create_member_in_stable_state();

        // First leave attempt. Should stay LEAVING until the heartbeat is sent out.
        mock_leave_group(&fx);
        let leave_result1 = fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
        assert!(!leave_result1.is_done());
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        // verify(subscriptionState).assignFromSubscribed(Set.of()): see the
        // deviation in `assert_leave_group_releases_assignment_...`; the
        // assignment is empty.
        assert!(fx.assigned_partitions().is_empty());
        fx.clear_subscription_invocations();

        // Second leave attempt while the first one has not completed yet.
        mock_leave_group(&fx);
        let leave_result2 = fx.leave_group();
        // verify(subscriptionState, never()).unsubscribe(): `mock_leave_group`
        // re-subscribed (Java's `hasAutoAssignedPartitions()` stub), and the
        // subscription is still there.
        assert!(!fx.is_unsubscribed());
        assert!(!leave_result2.is_done());

        // Complete first leave group operation. Should also complete the second leave group.
        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(&fx, &leave_result1);
        assert!(leave_result2.is_done());
        assert_not_completed_exceptionally(&leave_result2);

        // Subscription should have been updated only once with the first leave group.
        assert_eq!(0, fx.assign_from_subscribed_calls());
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupWhenMemberAlreadyLeft`.
    #[test]
    fn leave_group_when_member_already_left() {
        let fx = create_member_in_stable_state();

        // Leave group triggered and completed
        mock_leave_group(&fx);
        let leave_result1 = fx.leave_group();
        assert_eq!(MemberState::Leaving, fx.mgr.state());
        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(&fx, &leave_result1);
        // verify(subscriptionState).assignFromSubscribed(Set.of()): see the
        // deviation in `assert_leave_group_releases_assignment_...`.
        assert!(fx.assigned_partitions().is_empty());
        fx.clear_subscription_invocations();

        // Call to leave group again, when member already left. Should be no-op (no assignment updated)
        mock_leave_group(&fx);
        let leave_result2 = fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
        assert!(leave_result2.is_done());
        assert_not_completed_exceptionally(&leave_result2);
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        // verify(subscriptionState, never()).assignFromSubscribed(Set.of()):
        // the single `assignment_id` bump is the `unsubscribe()` verified above.
        assert_eq!(1, fx.assign_from_subscribed_calls());
    }

    /// Translated from `ShareMembershipManagerTest#testLeaveGroupWhenMemberIsStale`.
    #[test]
    fn leave_group_when_member_is_stale() {
        let fx = mock_stale_member();
        assert_eq!(MemberState::Stale, fx.mgr.state());

        mock_leave_group(&fx);
        let leave_result1 = fx.leave_group();
        // verify(subscriptionState).unsubscribe()
        assert!(fx.is_unsubscribed());
        assert!(leave_result1.is_done());
        assert_eq!(MemberState::Stale, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testFatalFailureWhenStateIsJoining`.
    #[test]
    fn fatal_failure_when_state_is_joining() {
        let fx = create_membership_manager_joining_group();
        assert_eq!(MemberState::Joining, fx.mgr.state());

        assert_state_update_on_fatal_failure(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testFatalFailureWhenStateIsStable`.
    #[test]
    fn fatal_failure_when_state_is_stable() {
        let fx = create_member_in_stable_state();

        assert_state_update_on_fatal_failure(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testFatalFailureWhenStateIsLeaving`.
    #[test]
    fn fatal_failure_when_state_is_leaving() {
        let fx = create_member_in_stable_state();

        // Start leaving group.
        mock_leave_group(&fx);
        fx.leave_group();
        assert_eq!(MemberState::Leaving, fx.mgr.state());

        // Get fatal failure while waiting to send the heartbeat to leave. Member should
        // transition to FATAL, so the last heartbeat for leaving won't be sent because the member
        // already failed.
        assert_state_update_on_fatal_failure(&fx);

        assert_eq!(MemberState::Fatal, fx.mgr.state());
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Fatal, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testFatalFailureWhenMemberAlreadyLeft`.
    #[test]
    fn fatal_failure_when_member_already_left() {
        let fx = create_member_in_stable_state();

        // Start leaving group.
        mock_leave_group(&fx);
        fx.leave_group();
        assert_eq!(MemberState::Leaving, fx.mgr.state());

        // Last heartbeat sent.
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());

        // Fatal error received in response for the last heartbeat. Member should remain in FATAL
        // state but the member already left the group.
        fx.mgr.transition_to_fatal(fx.now()).unwrap();

        assert_eq!(MemberState::Fatal, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testUpdateStateFailsOnResponsesWithErrors`.
    #[test]
    fn update_state_fails_on_responses_with_errors() {
        let fx = create_membership_manager_joining_group();
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        let unknown_member_response = create_share_group_heartbeat_response_with_error(fx.mgr.member_id());
        let err = fx.mgr.on_heartbeat_success(&unknown_member_response).unwrap_err();
        assert!(
            matches!(err, Error::LocalIllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        // Java asserts only the type; the message is
        // `AbstractMembershipManager#throwIfUnexpectedError`'s.
        assert_eq!(
            "Unexpected error in Heartbeat response. Expected no error, but received: UNKNOWN_MEMBER_ID",
            err.message()
        );
    }

    /// Translated from `ShareMembershipManagerTest#testNewAssignmentReplacesPreviousOneWaitingOnMetadata`.
    ///
    /// This test should be the case when an assignment is sent to the member,
    /// and it cannot find it in metadata (permanently, ex. topic deleted). The
    /// member will keep the assignment as waiting for metadata, but the
    /// following assignment received from the broker will not contain the
    /// deleted topic. The member will discard the assignment that was pending
    /// and proceed with the reconciliation of the new assignment.
    #[test]
    fn new_assignment_replaces_previous_one_waiting_on_metadata() {
        let fx = mock_join_and_receive_assignment(false, None);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";
        fx.seed_metadata(&[(topic_name, topic_id)]);
        receive_assignment(&fx, topic_id, vec![0]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        let expected_assignment = HashSet::from([tp(topic_name, 0)]);
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        // verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedAssignment, expectedAssignment)
        // Deviation: the added set is not observable (see `assert_revocation_completed`).
        assert_eq!(expected_assignment, fx.assigned_partitions());

        // When ack for the reconciled assignment is sent, member should go back to STABLE
        // because the first assignment that was not resolved should have been discarded
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stable, fx.mgr.state());
        assert!(fx.topics_awaiting_reconciliation().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata`.
    ///
    /// This test ensures that member goes back to STABLE when the broker sends
    /// assignment that removes the unresolved target the client has, without
    /// triggering a reconciliation. In this case the member should discard the
    /// assignment that was unresolved and go back to STABLE with nothing to
    /// reconcile.
    #[test]
    fn new_empty_assignment_replaces_previous_one_waiting_on_metadata() {
        let fx = mock_join_and_receive_assignment(false, None);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";
        fx.seed_metadata(&[(topic_name, topic_id)]);
        receive_empty_assignment(&fx);

        verify_reconciliation_not_triggered(&fx);

        fx.poll();

        verify_reconciliation_triggered_and_completed(&fx, &[]);
        // Java's verify(subscriptionState).assignFromSubscribedAwaitingCallback(emptySet, any())
        // (times(1)): the empty set alone is trivially true here, since nothing
        // was assigned before.
        assert_eq!(1, fx.assign_from_subscribed_calls());

        fx.heartbeat_request_generated();

        assert_eq!(MemberState::Stable, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata`.
    #[test]
    fn new_assignment_not_in_metadata_replaces_previous_one_waiting_on_metadata() {
        let fx = mock_join_and_receive_assignment(false, None);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());

        // New target assignment (not found in metadata) received while there is another one
        // waiting to be resolved and reconciled. This assignment does not include the previous
        // one that is waiting for metadata, so the member will discard the topics that were
        // waiting for metadata, and just keep the new one as unresolved.
        let topic_id = Uuid::random_uuid();
        fx.seed_metadata(&[]);
        receive_assignment(&fx, topic_id, vec![0]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();
        fx.heartbeat_request_generated();

        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());
        assert_eq!(Some(&topic_id), fx.topics_awaiting_reconciliation().iter().next());
    }

    /// Translated from `ShareMembershipManagerTest#testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived`.
    ///
    /// This ensures that the client reconciles target assignments as soon as
    /// they are discovered in metadata, without depending on the broker to
    /// re-send the assignment.
    #[test]
    fn unresolved_target_assignment_is_reconciled_when_metadata_received() {
        let fx = create_member_in_stable_state();
        // Assignment not in metadata. Member cannot reconcile it yet, but keeps it to be
        // reconciled when metadata is discovered.
        let topic_id = Uuid::random_uuid();
        receive_assignment(&fx, topic_id, vec![1]);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert!(!fx.topics_awaiting_reconciliation().is_empty());

        // Metadata update received, including the missing topic name.
        let topic_name = "topic1";
        fx.seed_metadata(&[(topic_name, topic_id)]);
        fx.stub_has_auto_assigned_partitions(true);

        fx.poll();

        // Assignment should have been reconciled.
        let expected_assignment = HashSet::from([tp(topic_name, 1)]);
        // verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedAssignment, expectedAssignment)
        // Deviation: the added set is not observable (see `assert_revocation_completed`).
        assert_eq!(expected_assignment, fx.assigned_partitions());
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        assert!(fx.topics_awaiting_reconciliation().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved`.
    ///
    /// This test should be the case when an assignment is sent to the member,
    /// and it cannot find it in metadata (temporarily). If the broker continues
    /// to send the assignment to the member, this one should keep it waiting
    /// for metadata and continue to request updates.
    #[test]
    fn member_keeps_unresolved_assignment_waiting_for_metadata_until_resolved() {
        // Assignment with 2 topics, only 1 found in metadata
        let topic1 = Uuid::random_uuid();
        let topic1_name = "topic1";
        let topic2 = Uuid::random_uuid();
        let assignment = || assignment(vec![topic_partitions(topic1, vec![0]), topic_partitions(topic2, vec![1, 3])]);

        // Receive assignment partly in metadata - reconcile+ack what's in metadata, keep the
        // unresolved and request metadata update.
        //
        // `mock_join_and_receive_assignment` builds the fixture, so Java's
        // `when(metadata.topicNames())` that precedes it is applied through a
        // fixture built here with the same steps.
        let fx = create_membership_manager_joining_group();
        fx.seed_metadata(&[(topic1_name, topic1)]);
        let heartbeat_response = create_share_group_heartbeat_response(assignment(), fx.mgr.member_id());
        fx.stub_has_auto_assigned_partitions(true);
        fx.on_heartbeat_success(&heartbeat_response);
        fx.poll();
        // verify(subscriptionState).assignFromSubscribedAwaitingCallback(anyCollection(), anyCollection())
        assert_eq!(1, fx.assign_from_subscribed_calls());
        fx.clear_manager_invocations();

        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        // verify(metadata).requestUpdate(anyBoolean())
        assert_eq!(1, fx.request_update_calls());
        assert_eq!(HashSet::from([topic2]), fx.topics_awaiting_reconciliation());

        // When the ack is sent the member should go back to RECONCILING because it still has
        // unresolved assignment to be reconciled.
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Reconciling, fx.mgr.state());

        // Target assignment received again with the same unresolved topic. Client should keep it
        // as unresolved.
        fx.clear_subscription_invocations();
        fx.on_heartbeat_success(&create_share_group_heartbeat_response(assignment(), fx.mgr.member_id()));
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert_eq!(HashSet::from([topic2]), fx.topics_awaiting_reconciliation());
        // verify(subscriptionState, never()).assignFromSubscribed(anyCollection())
        assert_eq!(0, fx.assign_from_subscribed_calls());
    }

    /// Translated from `ShareMembershipManagerTest#testReconcileNewPartitionsAssignedWhenNoPartitionOwned`.
    #[test]
    fn reconcile_new_partitions_assigned_when_no_partition_owned() {
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";
        let fx = create_membership_manager_joining_group();
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[]);

        receive_assignment(&fx, topic_id, vec![0, 1]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        let assigned_partitions = [(topic_id, topic_name, 0), (topic_id, topic_name, 1)];
        verify_reconciliation_triggered_and_completed(&fx, &assigned_partitions);
    }

    /// Translated from `ShareMembershipManagerTest#testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`.
    #[test]
    fn reconcile_new_partitions_assigned_when_other_partitions_owned() {
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";
        let owned_partition = (topic_id, topic_name, 0);
        let fx = create_member_in_stable_state();
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[0]);

        // New assignment received, adding partitions 1 and 2 to the previously owned partition 0.
        receive_assignment(&fx, topic_id, vec![0, 1, 2]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        let assigned_partitions = [owned_partition, (topic_id, topic_name, 1), (topic_id, topic_name, 2)];
        verify_reconciliation_triggered_and_completed(&fx, &assigned_partitions);
    }

    /// Translated from `ShareMembershipManagerTest#testReconciliationSkippedWhenSameAssignmentReceived`.
    #[test]
    fn reconciliation_skipped_when_same_assignment_received() {
        // Member stable, no assignment
        let fx = create_membership_manager_joining_group();
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";

        // Receive assignment different from what the member owns - should reconcile
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[]);
        let expected_assignment_reconciled = [(topic_id, topic_name, 0), (topic_id, topic_name, 1)];
        receive_assignment(&fx, topic_id, vec![0, 1]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        verify_reconciliation_triggered_and_completed(&fx, &expected_assignment_reconciled);
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        fx.clear_subscription_invocations();
        fx.clear_manager_invocations();

        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stable, fx.mgr.state());

        // Receive same assignment again - should not trigger reconciliation
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[0, 1]);
        receive_assignment(&fx, topic_id, vec![0, 1]);
        // Verify new reconciliation was not triggered
        verify_reconciliation_not_triggered(&fx);
        // verify(subscriptionState, never()).assignFromSubscribed(anyCollection())
        assert_eq!(0, fx.assign_from_subscribed_calls());

        assert_eq!(MemberState::Stable, fx.mgr.state());

        assert_eq!(1.0, fx.metric_value(&fx.rebalance_metrics_manager.rebalance_total));
    }

    /// Translated from `ShareMembershipManagerTest#testReconcileNewPartitionsAssignedAndRevoked`.
    #[test]
    fn reconcile_new_partitions_assigned_and_revoked() {
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";
        let fx = create_membership_manager_joining_group();
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[0]);

        mock_revocation();

        // New assignment received, revoking partition 0, and assigning new partitions 1 and 2.
        receive_assignment(&fx, topic_id, vec![1, 2]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();

        let expected_set = HashSet::from([tp(topic_name, 1), tp(topic_name, 2)]);
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        assert_eq!(HashMap::from([(topic_id, vec![1, 2])]), fx.current_partitions());
        assert!(!fx.reconciliation_in_progress());

        // verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedSet, expectedSet)
        // Deviation: the added set is not observable (see `assert_revocation_completed`).
        assert_eq!(expected_set, fx.assigned_partitions());
    }

    /// Translated from `ShareMembershipManagerTest#testMetadataUpdatesReconcilesUnresolvedAssignments`.
    #[test]
    fn metadata_updates_reconciles_unresolved_assignments() {
        let topic_id = Uuid::random_uuid();

        // Assignment not in metadata
        let target_assignment = assignment(vec![topic_partitions(topic_id, vec![0, 1])]);
        let fx = mock_join_and_receive_assignment(true, Some(target_assignment));
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Reconciling, fx.mgr.state());

        // Should not trigger reconciliation, and request a metadata update.
        verify_reconciliation_not_triggered(&fx);
        assert_eq!(HashSet::from([topic_id]), fx.topics_awaiting_reconciliation());
        // verify(metadata).requestUpdate(anyBoolean())
        assert_eq!(1, fx.request_update_calls());

        let topic_name = "topic1";
        // mockTopicNameInMetadataCache(Map.of(topicId, topicName), true)
        fx.seed_metadata(&[(topic_name, topic_id)]);

        // When metadata is updated, the member should re-trigger reconciliation
        fx.poll();
        let expected_assignment_reconciled = [(topic_id, topic_name, 0), (topic_id, topic_name, 1)];
        verify_reconciliation_triggered_and_completed(&fx, &expected_assignment_reconciled);
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        assert!(fx.topics_awaiting_reconciliation().is_empty());
    }

    /// Translated from `ShareMembershipManagerTest#testMetadataUpdatesRequestsAnotherUpdateIfNeeded`.
    #[test]
    fn metadata_updates_requests_another_update_if_needed() {
        let topic_id = Uuid::random_uuid();

        // Assignment not in metadata
        let target_assignment = assignment(vec![topic_partitions(topic_id, vec![0, 1])]);
        let fx = mock_join_and_receive_assignment(true, Some(target_assignment));
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Reconciling, fx.mgr.state());

        // Should not trigger reconciliation, and request a metadata update.
        verify_reconciliation_not_triggered(&fx);
        assert_eq!(HashSet::from([topic_id]), fx.topics_awaiting_reconciliation());
        // verify(metadata).requestUpdate(anyBoolean())
        assert_eq!(1, fx.request_update_calls());

        // Next poll is run, but metadata still without the unresolved topic in it. Should keep
        // the unresolved and request update again.
        fx.seed_metadata(&[]);
        fx.poll();
        verify_reconciliation_not_triggered(&fx);
        assert_eq!(HashSet::from([topic_id]), fx.topics_awaiting_reconciliation());
        // verify(metadata, times(2)).requestUpdate(anyBoolean())
        assert_eq!(2, fx.request_update_calls());
    }

    /// Translated from `ShareMembershipManagerTest#testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable`.
    #[test]
    fn revoke_partitions_uses_topic_names_local_cache_when_metadata_not_available() {
        let topic_id = Uuid::random_uuid();
        let topic_name = "topic1";

        let fx = create_membership_manager_joining_group();
        mock_owned_partition_and_assignment_received(&fx, topic_id, topic_name, &[]);

        // Member received assignment to reconcile;

        receive_assignment(&fx, topic_id, vec![0, 1]);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();
        // verify(subscriptionState).markPendingRevocation(Set.of())
        assert_eq!(1, fx.mark_pending_revocation_calls(&[]));

        // Member should complete reconciliation
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        let assigned_partitions = [tp(topic_name, 0), tp(topic_name, 1)];
        let assigned_topic_id_partitions = HashMap::from([(topic_id, vec![0, 1])]);
        assert_eq!(assigned_topic_id_partitions, fx.current_partitions());
        assert!(!fx.reconciliation_in_progress());

        // mockAckSent(membershipManager)
        fx.heartbeat_request_generated();
        fx.stub_assigned_partitions(&assigned_partitions);

        // Revocation of topic not found in metadata cache
        fx.stub_has_auto_assigned_partitions(true);
        mock_revocation();
        // mockTopicNameInMetadataCache(Map.of(topicId, topicName), false)
        fx.seed_metadata(&[]);

        // Revoke one of the 2 partitions
        receive_assignment(&fx, topic_id, vec![1]);

        fx.poll();
        // verify(subscriptionState, times(2)).markPendingRevocation(Set.of(new TopicPartition(topicName, 0)))
        assert_eq!(2, fx.mark_pending_revocation_calls(&[tp(topic_name, 0)]));

        // Revocation should complete without requesting any metadata update given that the topic
        // received in target assignment should exist in local topic name cache.
        // verify(metadata, never()).requestUpdate(anyBoolean())
        assert_eq!(0, fx.request_update_calls());
        let remaining_assignment = [(topic_id, topic_name, 1)];

        assert_revocation_completed(&fx, &remaining_assignment);
    }

    /// Translated from `ShareMembershipManagerTest#testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup`.
    #[test]
    fn on_subscription_updated_does_not_transition_to_joining_if_in_group() {
        let fx = create_member_in_stable_state();
        fx.mgr.on_subscription_updated();
        assert!(fx.subscription_updated());
        fx.mgr.on_consumer_poll().unwrap();
        // verify(membershipManager, never()).transitionToJoining(): the member
        // is still STABLE.
        assert_eq!(MemberState::Stable, fx.mgr.state());
        assert!(!fx.subscription_updated());
    }

    /// Translated from `ShareMembershipManagerTest#testOnSubscriptionUpdatedTransitionsToJoiningOnPollIfNotInGroup`.
    #[test]
    fn on_subscription_updated_transitions_to_joining_on_poll_if_not_in_group() {
        let fx = create_membership_manager();
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        fx.mgr.on_subscription_updated();
        // verify(membershipManager, never()).transitionToJoining()
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        assert!(fx.subscription_updated());
        assert_eq!(MemberState::Unsubscribed, fx.mgr.state());
        fx.mgr.on_consumer_poll().unwrap();
        // verify(membershipManager).transitionToJoining()
        assert_eq!(MemberState::Joining, fx.mgr.state());
        assert_eq!(ShareGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH, fx.mgr.member_epoch());
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToLeavingWhileReconcilingDueToStaleMember`.
    #[test]
    fn transition_to_leaving_while_reconciling_due_to_stale_member() {
        let fx = member_join_with_assignment();
        fx.clear_subscription_invocations();
        fx.stub_has_auto_assigned_partitions(true);
        assert_eq!(MemberState::Reconciling, fx.mgr.state());
        assert_leave_group_due_to_expired_poll_and_transition_to_stale(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToLeavingWhileJoiningDueToStaleMember`.
    #[test]
    fn transition_to_leaving_while_joining_due_to_stale_member() {
        let fx = create_membership_manager_joining_group();
        // doNothing().when(subscriptionState).assignFromSubscribed(any())
        fx.stub_has_auto_assigned_partitions(true);
        assert_eq!(MemberState::Joining, fx.mgr.state());
        assert_leave_group_due_to_expired_poll_and_transition_to_stale(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToLeavingWhileStableDueToStaleMember`.
    #[test]
    fn transition_to_leaving_while_stable_due_to_stale_member() {
        let fx = create_member_in_stable_state();
        assert_leave_group_due_to_expired_poll_and_transition_to_stale(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testTransitionToLeavingWhileAcknowledgingDueToStaleMember`.
    #[test]
    fn transition_to_leaving_while_acknowledging_due_to_stale_member() {
        let fx = mock_join_and_receive_assignment(true, None);
        // doNothing().when(subscriptionState).assignFromSubscribed(any())
        fx.clear_subscription_invocations();
        fx.stub_has_auto_assigned_partitions(true);
        assert_eq!(MemberState::Acknowledging, fx.mgr.state());
        assert_leave_group_due_to_expired_poll_and_transition_to_stale(&fx);
    }

    /// Translated from `ShareMembershipManagerTest#testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover`.
    #[test]
    fn stale_member_does_not_send_heartbeat_and_allows_transition_to_joining_to_recover() {
        let fx = create_member_in_stable_state();
        // doNothing().when(subscriptionState).assignFromSubscribed(any())
        fx.mgr.transition_to_sending_leave_group(true).unwrap();
        fx.heartbeat_request_generated();
        assert_eq!(MemberState::Stale, fx.mgr.state());
        assert!(fx.should_skip_heartbeat(), "Stale member should not send heartbeats");
        // Check that a transition to joining is allowed, which is what is expected to happen
        // when the poll timer is reset.
        // assertDoesNotThrow(membershipManager::maybeRejoinStaleMember): the
        // Rust method is infallible; the transition it allows is observable.
        fx.mgr.maybe_rejoin_stale_member();
        assert_eq!(MemberState::Joining, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testStaleMemberRejoinsWhenTimerResetsNoCallbacks`.
    #[test]
    fn stale_member_rejoins_when_timer_resets_no_callbacks() {
        let fx = mock_stale_member();
        assert_stale_member_leaves_group_and_clears_assignment(&fx);

        fx.mgr.maybe_rejoin_stale_member();
        assert_eq!(MemberState::Joining, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment`.
    #[test]
    fn member_joining_transitions_to_stable_when_receiving_empty_assignment() {
        let fx = create_membership_manager_joining_group();
        fx.stub_has_auto_assigned_partitions(true);
        assert_eq!(MemberState::Joining, fx.mgr.state());
        receive_empty_assignment(&fx);

        verify_reconciliation_not_triggered(&fx);
        fx.poll();
        fx.heartbeat_request_generated();

        assert_eq!(MemberState::Stable, fx.mgr.state());
    }

    /// Translated from `ShareMembershipManagerTest#testMetricsWhenHeartbeatFailed`.
    #[test]
    fn metrics_when_heartbeat_failed() {
        let fx = create_member_in_stable_state();
        fx.mgr.on_heartbeat_failure(false);

        // Not expecting rebalance failures with only the empty assignment being reconciled.
        assert_eq!(1.0, fx.metric_value(&fx.rebalance_metrics_manager.rebalance_total));
    }

    /// Translated from `ShareMembershipManagerTest#testRebalanceMetricsOnSuccessfulRebalance`.
    #[test]
    fn rebalance_metrics_on_successful_rebalance() {
        let fx = create_membership_manager_joining_group();
        let heartbeat_response = create_share_group_heartbeat_response(Assignment::new(), fx.mgr.member_id());
        fx.on_heartbeat_success(&heartbeat_response);
        mock_owned_partition(&fx, Uuid::random_uuid(), "topic1");

        mock_revocation();

        receive_empty_assignment(&fx);
        let reconciliation_duration_ms = 1234;
        fx.time.sleep(reconciliation_duration_ms);

        fx.poll();
        // Complete commit request to complete the callback invocation: Java's
        // `commitResult` is an already completed future nothing waits on, so
        // completing it is a no-op with no Rust counterpart.

        assert_eq!(1.0, fx.metric_value(&fx.rebalance_metrics_manager.rebalance_total));
        assert_eq!(1.0, fx.metric_value(&fx.rebalance_metrics_manager.rebalance_rate_per_hour));

        let samples = fx.metrics.config().samples();
        let window_length = i64::from(samples) * 3_600_000;
        fx.time.sleep(window_length - reconciliation_duration_ms - 1);
        let rate = fx.metric_value(&fx.rebalance_metrics_manager.rebalance_rate_per_hour);
        let expected = 1.0 / f64::from(samples);
        assert!((expected - rate).abs() <= 0.1, "expected {expected} +/- 0.1, got {rate}");

        // WindowLength have passed, triggering metric reset
        fx.time.sleep(window_length);
        assert_eq!(0.0, fx.metric_value(&fx.rebalance_metrics_manager.rebalance_rate_per_hour));
    }

    /// Translated from `ShareMembershipManagerTest#testRebalanceMetricsOnFailedRebalance`.
    #[test]
    fn rebalance_metrics_on_failed_rebalance() {
        let fx = create_membership_manager_joining_group();
        let heartbeat_response = create_share_group_heartbeat_response(Assignment::new(), fx.mgr.member_id());
        fx.on_heartbeat_success(&heartbeat_response);

        let topic_id = Uuid::random_uuid();

        receive_assignment(&fx, topic_id, vec![0, 1]);

        // sleep for an arbitrary amount
        fx.time.sleep(2300);

        assert!(fx.rebalance_metrics_manager.rebalance_started());
        fx.mgr.on_heartbeat_failure(false);

        assert_eq!(0.0, fx.metric_value(&fx.rebalance_metrics_manager.rebalance_total));
    }

    /// Rust-only (R3): the production constructor builds its own
    /// `ShareRebalanceMetricsManager` over the given registry, so the
    /// `consumer-share-coordinator-metrics` rebalance metrics are registered
    /// and recorded by the state machine. Java's tests only use the
    /// visible-for-testing constructor.
    #[test]
    fn new_registers_share_rebalance_metrics() {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let metadata =
            ConsumerMetadata::with_config(&config, Arc::clone(&subscriptions), ClusterResourceListeners::new());
        let mgr = ShareMembershipManager::new(
            GROUP_ID,
            Some("rack-1".to_string()),
            subscriptions,
            metadata.metadata_arc(),
            Arc::clone(&time) as Arc<dyn Time>,
            &metrics,
        );
        assert_eq!(mgr.rack_id(), Some("rack-1"));
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        let total = crate::consumer::internals::metrics::MetricsLedger::new(Arc::clone(&metrics)).metric_name(
            "rebalance-total",
            "consumer-share-coordinator-metrics",
            "The total number of rebalance events",
        );
        assert_eq!(metrics.metric(&total).unwrap().metric_value().as_double(), Some(0.0));
    }
}
