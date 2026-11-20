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
//! manager. Composes [`super::AbstractMembershipManager`], which holds the
//! group-agnostic state machine and the §31 reconcile chain
//! ([`super::AbstractMembershipManager::maybe_reconcile`]), and supplies the
//! Consumer-specific configuration (group instance ID, server assignor, rack
//! ID) and the [`super::MembershipManagerHooks`] overrides: the
//! rebalance-listener callback enqueues, the auto-commit before the
//! rebalance, and the `PartitionsAssigned` event.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager`.

use crate::common::requests::ConsumerGroupHeartbeatRequest;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::common::internals::KafkaFutureImpl;
use crate::common::requests::ConsumerGroupHeartbeatResponse;
use crate::common::utils::Time;

use crate::common::{Error, TopicPartition, Uuid};
use crate::consumer::GroupMembershipOperation;
use crate::consumer::internals::ConsumerRebalanceListenerMethodName;
use crate::consumer::internals::events::BackgroundEvent;
use crate::consumer::internals::events::BackgroundEventHandler;

use super::CommitRequestManager;
use super::ConsumerMetadata;
use super::MemberState;
use super::PollResult;
use super::RequestManager;
use super::SubscriptionState;
#[cfg(test)]
use super::abstract_membership_manager::PendingReconcile;
use super::abstract_membership_manager::{LeaveCallback, SignalAck};
use super::metrics::ConsumerRebalanceMetricsManager;
use super::metrics::RebalanceMetricsManager;
use super::{AbstractMembershipManager, LocalAssignment, MembershipManagerHooks};

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
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager")]
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
    /// Java: `private final BackgroundEventHandler backgroundEventHandler`:
    /// carries the rebalance-listener callback and `PartitionsAssigned`
    /// events to the application thread. The base class has none (the share
    /// manager applies its assignment in the background).
    pub(crate) background_event_handler: Arc<BackgroundEventHandler>,
}

impl ConsumerMembershipManager {
    /// Java constructor (the test-visible 13-arg variant). Drops
    /// `LogContext` is dropped (we use `log`). The `Metrics` /
    /// `RebalanceMetricsManager` are wired through as
    /// `Option<Arc<ConsumerRebalanceMetricsManager>>` + a metrics `Time` clock
    /// (M5): `None` in tests that don't exercise rebalance metrics; the live
    /// consumer always supplies them.
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#ConsumerMembershipManager")]
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
            metadata.metadata_arc(),
            // The auto-commit-before-rebalance needs the commit manager; the
            // base's reconcile gate reads this flag, so a manager built
            // without one (unit tests) reconciles as if auto-commit were off.
            auto_commit_enabled && commit_request_manager.is_some(),
            metrics_manager.map(|m| m as Arc<dyn RebalanceMetricsManager>),
            time,
        );
        Self {
            abstract_mm,
            group_instance_id,
            rack_id,
            rebalance_timeout_ms,
            server_assignor,
            commit_request_manager,
            background_event_handler,
        }
    }

    /// Java: `groupId()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn group_id(&self) -> String {
        self.abstract_mm.group_id()
    }

    /// Java: `memberId()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn member_id(&self) -> String {
        self.abstract_mm.member_id()
    }

    /// Java: `memberEpoch()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn member_epoch(&self) -> i32 {
        self.abstract_mm.member_epoch()
    }

    /// Java: `state()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn state(&self) -> MemberState {
        self.abstract_mm.state()
    }

    /// Java: `groupInstanceId()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#groupInstanceId")]
    pub(crate) fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Java: `rackId()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#rackId")]
    pub(crate) fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Java: `serverAssignor()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#serverAssignor")]
    pub(crate) fn server_assignor(&self) -> Option<&str> {
        self.server_assignor.as_deref()
    }

    /// Java: `leaveGroupOperation()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn leave_group_operation(&self) -> GroupMembershipOperation {
        self.abstract_mm.leave_group_operation()
    }

    /// Set the leave-group operation. Java sets this in
    /// `leaveGroupOnClose(...)`, which now runs in the base.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production caller lives in AbstractMembershipManager")
    )]
    pub(crate) fn set_leave_group_operation(&self, op: GroupMembershipOperation) {
        self.abstract_mm.set_leave_group_operation(op);
    }

    /// Java: `currentAssignment()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn current_assignment(&self) -> LocalAssignment {
        self.abstract_mm.current_assignment()
    }

    /// Java: `transitionToJoining()` (inherited; "Visible for testing"). The
    /// production paths run inside the base and call
    /// [`AbstractMembershipManager::transition_to_joining`] directly.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production callers live in AbstractMembershipManager")
    )]
    pub(crate) fn transition_to_joining(&self) -> Result<(), Error> {
        self.abstract_mm.transition_to_joining(self.join_group_epoch())
    }

    /// Java: `transitionToSendingLeaveGroup(boolean dueToExpiredPollTimer)`.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production callers go through the generic heartbeat code")
    )]
    pub(crate) fn transition_to_sending_leave_group(&self, due_to_expired_poll_timer: bool) -> Result<(), Error> {
        self.abstract_mm
            .transition_to_sending_leave_group(self.leave_group_epoch(), due_to_expired_poll_timer)
    }

    /// Java: `transitionToFatal()` (inherited from `AbstractMembershipManager`).
    pub(crate) fn transition_to_fatal(&self, current_time_ms: i64) -> Result<(), Error> {
        self.abstract_mm.transition_to_fatal(self, current_time_ms)
    }

    /// Java: `transitionToFenced()` (inherited from `AbstractMembershipManager`).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production callers go through the generic heartbeat code")
    )]
    pub(crate) fn transition_to_fenced(&self, current_time_ms: i64) -> Result<(), Error> {
        self.abstract_mm.transition_to_fenced(self, current_time_ms)
    }

    /// Java: `transitionToStale()` (inherited from `AbstractMembershipManager`):
    /// the release half; see [`AbstractMembershipManager::transition_to_stale`].
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production callers go through the generic heartbeat code")
    )]
    pub(crate) fn transition_to_stale(&self, current_time_ms: i64) -> Result<(), Error> {
        self.abstract_mm.transition_to_stale(self, current_time_ms)
    }

    /// Reconcile the target assignment per §31. Java: `maybeReconcile(boolean)`
    /// (inherited); see [`AbstractMembershipManager::maybe_reconcile`].
    ///
    /// **Must only be called from the background task.**
    pub(crate) fn reconcile(&self, current_time_ms: i64, can_commit: bool) -> Result<(), Error> {
        self.abstract_mm.maybe_reconcile(self, current_time_ms, can_commit)
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#applyAssignment")]
    pub(crate) fn apply_assignment<'a>(
        &self,
        assigned_partitions: impl IntoIterator<Item = &'a TopicPartition>,
        added_partitions: &[TopicPartition],
    ) -> Result<(), Error> {
        // Java's `assignedPartitions` is the reconciliation's `SortedSet`;
        // `SubscriptionState#assignFromSubscribed` keeps the order it is given.
        let assigned_vec: Vec<TopicPartition> = assigned_partitions.into_iter().cloned().collect();
        let assigned_set: HashSet<TopicPartition> = assigned_vec.iter().cloned().collect();
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
            guard.notify_assignment_change(&assigned_set);
        }
        Ok(())
    }

    /// `true` when a reconcile rebalance callback is awaiting its ack. See
    /// [`AbstractMembershipManager::has_pending_reconcile`].
    pub(crate) fn has_pending_reconcile(&self) -> bool {
        self.abstract_mm.has_pending_reconcile()
    }

    /// `true` when a release-path `onPartitionsLost` callback is awaiting its
    /// ack. See [`AbstractMembershipManager::has_pending_release`].
    pub(crate) fn has_pending_release(&self) -> bool {
        self.abstract_mm.has_pending_release()
    }

    /// Drives a pending release-path callback; see
    /// [`AbstractMembershipManager::drive_pending_release`].
    pub(crate) async fn drive_pending_release(&self) -> Result<(), Error> {
        self.abstract_mm.drive_pending_release(self).await
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
    pub(crate) async fn reconcile_drive_to_completion(&self, can_commit: bool) -> Result<(), Error> {
        for _ in 0..10_000 {
            self.reconcile(0, can_commit)?;
            if !self.has_pending_reconcile() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(Error::local_illegal_state(
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
    pub(crate) async fn drive_release_to_completion(&self) -> Result<(), Error> {
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
        Err(Error::local_illegal_state(
            "drive_release_to_completion did not settle within the iteration budget",
        ))
    }

    /// Java: `leaveGroupOnClose(GroupMembershipOperation)` (inherited); see
    /// [`AbstractMembershipManager::leave_group_on_close`].
    pub(crate) fn leave_group_on_close(
        &self,
        membership_operation: GroupMembershipOperation,
        current_time_ms: i64,
    ) -> Result<KafkaFutureImpl<()>, Error> {
        self.abstract_mm
            .leave_group_on_close(self, membership_operation, current_time_ms)
    }

    /// Java: `leaveGroup()` (inherited); see
    /// [`AbstractMembershipManager::leave_group`].
    pub(crate) fn leave_group(&self, current_time_ms: i64) -> Result<KafkaFutureImpl<()>, Error> {
        self.abstract_mm.leave_group(self, current_time_ms)
    }

    /// `true` while an `unsubscribe()` leave waits in `PREPARE_LEAVING` for
    /// its release callback's ack. Lock-free.
    pub(crate) fn has_pending_leave_callback(&self) -> bool {
        self.abstract_mm.has_pending_leave_callback()
    }

    /// The step Java chains on the leave callback; see
    /// [`AbstractMembershipManager::drive_pending_leave_callback`].
    pub(crate) fn drive_pending_leave_callback(&self) -> Result<(), Error> {
        self.abstract_mm.drive_pending_leave_callback(self)
    }

    /// Java: `ConsumerMembershipManager.consumerRebalanceListenerCallbackCompleted(event)`.
    ///
    /// Run by the application-event processor when it reaches the
    /// `ConsumerRebalanceListenerCallbackCompleted` event the app side
    /// enqueued after a leave's release callback returned. Completes the
    /// callback's future (`ack`) and then runs the step Java chains on it
    /// with `whenComplete` ([`Self::drive_pending_leave_callback`]), so the
    /// leave's `clearAssignmentAndLeaveGroup()` takes effect at the event's
    /// position in the queue: after every event the listener submitted
    /// before it returned, even one whose wait it abandoned, and before any
    /// event the app submitted afterwards.
    ///
    /// When `ack` is not the pending leave's (an older leave's callback
    /// whose slot a later leave replaced), the send finds the receiver gone
    /// and the drive leaves the current leave's ack waiting.
    pub(crate) fn consumer_rebalance_listener_callback_completed(
        &self,
        method_name: ConsumerRebalanceListenerMethodName,
        error: Option<Error>,
        ack: oneshot::Sender<Result<(), Error>>,
    ) -> Result<(), Error> {
        let result = match error {
            Some(e) => {
                log::warn!(
                    "The {} method completed with an error ({}); signaling to continue to the next phase of \
                     rebalance",
                    method_name.fully_qualified_method_name(),
                    e
                );
                Err(e)
            },
            None => {
                log::debug!(
                    "The {} method completed successfully; signaling to continue to the next phase of rebalance",
                    method_name.fully_qualified_method_name()
                );
                Ok(())
            },
        };
        let _ = ack.send(result);
        self.drive_pending_leave_callback()
    }

    /// Enqueues the §31 `RebalanceListenerCallbackNeeded` event and
    /// returns the ack [`oneshot::Receiver`] **without awaiting it**
    /// (Phase 41b), so the caller (the bg loop's `reconcile`, a release
    /// transition, or a leave) can store it and return — leaving the loop free to keep spinning while the app side
    /// runs the listener. Returns `Ok(None)` when no listener is
    /// registered (Java's `subscriptions.rebalanceListener().isPresent()`
    /// guard), in which
    /// case the caller proceeds as if the callback completed successfully.
    ///
    /// Java: `enqueueConsumerRebalanceListenerCallback(methodName, partitions)`
    /// behind the listener-presence checks of
    /// `ConsumerMembershipManager#invokeOnPartitions{Revoked,Lost}Callback`.
    ///
    /// `MutexGuard`s are never held across an `.await` (this method does
    /// not await at all).
    fn enqueue_rebalance_callback(
        &self,
        method: ConsumerRebalanceListenerMethodName,
        partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<Option<oneshot::Receiver<Result<(), Error>>>, Error> {
        self.enqueue_rebalance_callback_with_ack_through_application_events(method, partitions, false, current_time_ms)
    }

    /// [`Self::enqueue_rebalance_callback`] for the callback a leave
    /// enqueues, with `ack_through_application_events` set on the event
    /// (see [`BackgroundEvent::PartitionsRemoved`]): the app side returns the
    /// ack through the application-event queue, so it takes effect in FIFO
    /// order with the events the listener submitted.
    fn enqueue_rebalance_callback_with_ack_through_application_events(
        &self,
        method: ConsumerRebalanceListenerMethodName,
        partitions: Vec<TopicPartition>,
        ack_through_application_events: bool,
        current_time_ms: i64,
    ) -> Result<Option<oneshot::Receiver<Result<(), Error>>>, Error> {
        let listener_present = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.rebalance_listener().is_some()
        };
        if !listener_present {
            return Ok(None);
        }

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        let event = BackgroundEvent::PartitionsRemoved {
            method_name: method,
            partitions,
            ack: ack_tx,
            ack_through_application_events,
        };
        self.background_event_handler.add(event, current_time_ms)?;
        Ok(Some(ack_rx))
    }

    /// Enqueue a [`BackgroundEvent::PartitionsAssigned`] to the app thread
    /// and return the ack [`oneshot::Receiver`] **without awaiting it**
    /// (AK 4.3.1, KAFKA-20106).
    ///
    /// Unlike [`Self::enqueue_rebalance_callback`], there is NO
    /// listener-present short-circuit: the event is sent even when no
    /// listener is registered, because the app side must ALWAYS apply the
    /// new assignment to the subscription state within `poll()` (via an
    /// `ApplyAssignmentEvent`) so `consumer.assignment()` changes only there.
    /// The app side completes the ack after applying the assignment and,
    /// if a listener exists, running `on_partitions_assigned`.
    ///
    /// Java: `ConsumerMembershipManager.enqueuePartitionsAssignedEvent(
    /// fullAssignment, addedPartitions)` →
    /// `signalPartitionsAssigned(assignedPartitions, addedPartitions)`.
    ///
    /// `MutexGuard`s are never held across an `.await` (this method does
    /// not await at all).
    fn enqueue_partitions_assigned_event(
        &self,
        assigned_partitions: Vec<TopicPartition>,
        added_partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        let event = BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack: ack_tx };
        self.background_event_handler.add(event, current_time_ms)?;
        log::debug!(
            "The event to update the new assignment and trigger onPartitionsAssigned callback if \
             needed has been enqueued successfully to be sent to the app thread."
        );
        Ok(ack_rx)
    }

    /// Java: `onHeartbeatSuccess(ConsumerGroupHeartbeatResponse)` (inherited
    /// from `AbstractMembershipManager`).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production callers go through the generic heartbeat code")
    )]
    pub(crate) fn on_heartbeat_success(&self, response: &ConsumerGroupHeartbeatResponse) -> Result<(), Error> {
        self.abstract_mm.on_heartbeat_success::<Self>(response)
    }

    /// Java: `onHeartbeatFailure(boolean retriable)` (inherited from
    /// `AbstractMembershipManager`).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production callers go through the generic heartbeat code")
    )]
    pub(crate) fn on_heartbeat_failure(&self, retriable: bool) {
        self.abstract_mm.on_heartbeat_failure(retriable);
    }
}

/// The `ConsumerMembershipManager` overrides of Java's abstract
/// `AbstractMembershipManager` methods.
impl MembershipManagerHooks for ConsumerMembershipManager {
    type Response = ConsumerGroupHeartbeatResponse;

    fn abstract_mm(&self) -> &AbstractMembershipManager {
        &self.abstract_mm
    }

    /// Java: `isLeavingGroup()` (override).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#isLeavingGroup")]
    fn is_leaving_group(&self) -> bool {
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

    /// The heartbeat response's error code, for
    /// [`AbstractMembershipManager::on_heartbeat_success`] (KAFKA-20681).
    ///
    /// Java: `protected short errorCode(ConsumerGroupHeartbeatResponse response)`
    /// (`ConsumerMembershipManager#errorCode`), the override of the
    /// abstract hook.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#errorCode")]
    fn error_code(response: &ConsumerGroupHeartbeatResponse) -> i16 {
        response.data().error_code
    }

    /// The member epoch the heartbeat response carries, for
    /// [`AbstractMembershipManager::on_heartbeat_success`] (KAFKA-20681).
    /// Named after CLAUDE.md §2's overload rule: Java's `memberEpoch(R)` sits
    /// beside the `memberEpoch()` getter ([`Self::member_epoch`]).
    ///
    /// Java: `protected int memberEpoch(ConsumerGroupHeartbeatResponse response)`
    /// (`ConsumerMembershipManager#memberEpoch`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#memberEpoch")]
    fn member_epoch_with_response(response: &ConsumerGroupHeartbeatResponse) -> i32 {
        response.data().member_epoch
    }

    /// The new target assignment the heartbeat response carries, if any, for
    /// [`AbstractMembershipManager::on_heartbeat_success`] (KAFKA-20681). The
    /// field is only populated when there is a new target assignment for the
    /// member.
    ///
    /// Java builds each topic's partitions as a `TreeSet`, so they come out
    /// sorted and without duplicates whatever order the broker sent; the
    /// `Vec` here is sorted and deduplicated to match. That matters because
    /// [`LocalAssignment::update_with`] compares the `Vec`s by value.
    ///
    /// Java: `protected Optional<Map<Uuid, SortedSet<Integer>>>
    /// extractAssignment(ConsumerGroupHeartbeatResponse response)`
    /// (`ConsumerMembershipManager#extractAssignment`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#extractAssignment")]
    fn extract_assignment(response: &ConsumerGroupHeartbeatResponse) -> Option<HashMap<Uuid, Vec<i32>>> {
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

    /// Java: `ConsumerMembershipManager.invokeOnPartitionsRevokedOrLostToReleaseAssignment()`,
    /// reached through `signalMemberLeavingGroup()`.
    ///
    /// Choose between `onPartitionsRevoked` and `onPartitionsLost`
    /// based on the current member epoch. From Java:
    ///
    /// > If the member is part of the group (epoch > 0), this will
    /// > invoke onPartitionsRevoked. ... If the member is not part of
    /// > the group anymore (epoch <= 0), this will invoke
    /// > onPartitionsLost.
    ///
    /// The revoke branch goes through
    /// [`AbstractMembershipManager::revoke_partitions`], as Java's calls
    /// `revokePartitions(droppedPartitions)`: the partitions are marked
    /// pending revocation before the callback.
    ///
    /// Non-blocking, like Java's (which returns the callback's
    /// `CompletableFuture`): the §31 `RebalanceListenerCallbackNeeded` event
    /// is enqueued and its ack receiver returned, never awaited here.
    /// `Ok(None)` is Java's `completedFuture(null)`: no assignment to release,
    /// or no listener registered.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#invokeOnPartitionsRevokedOrLostToReleaseAssignment"
    )]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalMemberLeavingGroup")]
    fn signal_member_leaving_group(&self, current_time_ms: i64) -> Result<Option<LeaveCallback>, Error> {
        // Snapshot the dropped partitions + epoch under short locks, one at
        // a time.
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
            return Ok(None);
        }

        // The ack travels back through the application-event queue (Java's
        // `ConsumerRebalanceListenerCallbackCompletedEvent`), so the step
        // chained on it runs after every event the listener submitted.
        if member_epoch > 0 {
            // Member is part of the group. Invoke onPartitionsRevoked.
            let dropped: HashSet<TopicPartition> = dropped_partitions.into_iter().collect();
            let (revoked_partitions, ack) =
                self.abstract_mm.revoke_partitions(self, &dropped, true, current_time_ms)?;
            Ok(ack.map(|ack_rx| LeaveCallback { ack_rx, revoked_partitions: Some(revoked_partitions) }))
        } else {
            // Member is not part of the group anymore. Invoke onPartitionsLost.
            let ack = self.enqueue_rebalance_callback_with_ack_through_application_events(
                ConsumerRebalanceListenerMethodName::OnPartitionsLost,
                dropped_partitions,
                true,
                current_time_ms,
            )?;
            Ok(ack.map(|ack_rx| LeaveCallback { ack_rx, revoked_partitions: None }))
        }
    }

    /// Java: `signalReconciliationStarted()` →
    /// `commitRequestManager.maybeAutoCommitSyncBeforeRebalance(deadlineMs)`,
    /// with the rebalance timeout as the deadline. `None` without a commit
    /// manager (unit tests): the base's completed future.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalReconciliationStarted")]
    fn signal_reconciliation_started(&self, current_time_ms: i64) -> Option<SignalAck> {
        let commit_mgr = self.commit_request_manager.as_ref()?;
        let rebalance_timeout_ms = self.rebalance_timeout_ms as i64;
        let deadline_ms = current_time_ms.saturating_add(rebalance_timeout_ms);
        Some(commit_mgr.maybe_auto_commit_sync_before_rebalance(deadline_ms, current_time_ms))
    }

    /// Java: `signalReconciliationCompleting()` →
    /// `commitRequestManager.resetAutoCommitTimer()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalReconciliationCompleting"
    )]
    fn signal_reconciliation_completing(&self, current_time_ms: i64) {
        if let Some(commit_mgr) = self.commit_request_manager.as_ref() {
            commit_mgr.reset_auto_commit_timer(current_time_ms);
        }
    }

    /// Java: `signalPartitionsBeingRevoked(partitionsToRevoke)` →
    /// `logPausedPartitionsBeingRevoked`: log the partitions being revoked
    /// that were already paused, since the pause flag will be effectively
    /// lost.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalPartitionsBeingRevoked")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#logPausedPartitionsBeingRevoked"
    )]
    fn signal_partitions_being_revoked(&self, partitions_to_revoke: &[TopicPartition]) {
        let revoke_paused_partitions: Vec<TopicPartition> = {
            let subs = match self.abstract_mm.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let paused = subs.paused_partitions();
            partitions_to_revoke.iter().filter(|tp| paused.contains(*tp)).cloned().collect()
        };
        if !revoke_paused_partitions.is_empty() {
            log::info!("The pause flag in partitions {revoke_paused_partitions:?} will be removed due to revocation.");
        }
    }

    /// Java: `signalPartitionsRevoked(partitionsRevoked)` →
    /// `invokeOnPartitionsRevokedCallback`: enqueue `onPartitionsRevoked`
    /// when the set is non-empty and a listener is registered ("This should
    /// not trigger the callback if partitionsRevoked is empty, to keep the
    /// current behaviour").
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalPartitionsRevoked")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#invokeOnPartitionsRevokedCallback"
    )]
    fn signal_partitions_revoked(
        &self,
        partitions_revoked: Vec<TopicPartition>,
        ack_through_application_events: bool,
        current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error> {
        if partitions_revoked.is_empty() {
            return Ok(None);
        }
        self.enqueue_rebalance_callback_with_ack_through_application_events(
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            partitions_revoked,
            ack_through_application_events,
            current_time_ms,
        )
    }

    /// Java: `signalPartitionsAssigned(assignedPartitions, addedPartitions)` →
    /// `enqueuePartitionsAssignedEvent`: the app thread applies the assignment
    /// within `poll()` and runs `onPartitionsAssigned` (KAFKA-20106), so the
    /// ack is always pending.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalPartitionsAssigned")]
    fn signal_partitions_assigned(
        &self,
        assigned_partitions: Vec<TopicPartition>,
        added_partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error> {
        self.enqueue_partitions_assigned_event(assigned_partitions, added_partitions, current_time_ms)
            .map(Some)
    }

    /// Enqueue the release-path `onPartitionsLost` callback (Phase 41,
    /// Issue 2). Java: `ConsumerMembershipManager.signalPartitionsLost(partitionsLost)`.
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#signalPartitionsLost")]
    fn signal_partitions_lost(
        &self,
        partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<Option<SignalAck>, Error> {
        if partitions.is_empty() {
            return Ok(None);
        }
        // AK 4.3.1 (KAFKA-20321): pause fetching for the lost partitions
        // before running the callback.
        self.abstract_mm.mark_pending_revocation_to_pause_fetching(&partitions);
        self.enqueue_rebalance_callback(
            ConsumerRebalanceListenerMethodName::OnPartitionsLost,
            partitions,
            current_time_ms,
        )
    }

    /// Java: `joinGroupEpoch()` — 0 for the consumer group protocol.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#joinGroupEpoch")]
    fn join_group_epoch(&self) -> i32 {
        ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH
    }

    /// Java: `leaveGroupEpoch()`. For static members + `LEAVE_GROUP`
    /// operation: -1 (force fence). Otherwise: -1 for dynamic members,
    /// -2 for static members.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager#leaveGroupEpoch")]
    fn leave_group_epoch(&self) -> i32 {
        let is_static_member = self.group_instance_id.is_some();
        // The mechanism to make static members permanently leave the group is to
        // send an HB to leave with the -1 epoch (used by dynamic members).
        // This will make the group coordinator fence this member, effectively
        // removing it from the group.
        if matches!(self.leave_group_operation(), GroupMembershipOperation::LeaveGroup) {
            return ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH;
        }
        if is_static_member {
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH
        } else {
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH
        }
    }
}

/// `RequestManager` impl. `poll` does NOT drive reconcile from here: the
/// membership manager is `Arc`-shared and skipped by `entries()`, so the bg
/// task calls `reconcile(now, false)` explicitly (`run_once` Phase 2.5).
///
/// Java: `AbstractMembershipManager.poll(long currentTimeMs)` calls
/// `maybeReconcile(false)` and returns `EMPTY`. Our split: the bg task
/// calls `reconcile()` — `poll` here is a hook that just returns the empty
/// result, mirroring Java's "it never returns requests to send itself"
/// semantics.
impl RequestManager for ConsumerMembershipManager {
    fn poll(&mut self, _current_time_ms: i64) -> PollResult {
        // The bg task's `run_once` Phase 2.5 calls `reconcile()` instead.
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
///    exceptionally-completion). The Rust reconcile path does not expose
///    a chain object; its ordering is verified by the reconcile tests via
///    state assertions instead. (`leave_group()` does return its future,
///    and the leave tests assert `is_done()` exactly where Java asserts
///    `isDone()`.)
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
    use crate::common::protocol::Errors;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::ConsumerRebalanceListener;
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use async_trait::async_trait;
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    /// Test-only no-op rebalance listener. Registered by default in
    /// `make()` so the §31 listener-presence short-circuit (introduced
    /// per COMMENTS.1.md fix #1) does not silently drop events.
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

    fn make(
        group_instance_id: Option<String>,
        server_assignor: Option<String>,
        rack_id: Option<String>,
    ) -> (
        ConsumerMembershipManager,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
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
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
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
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        if with_listener {
            subs.lock()
                .unwrap()
                .subscribe_with_topics(HashSet::new(), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let metadata = Arc::new(ConsumerMetadata::with_config(
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
            Arc::new(crate::common::utils::SystemTime),
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
        Arc<crate::common::utils::MockTime>,
    ) {
        use crate::common::metrics::Metrics;
        use crate::common::utils::MockTime;
        use crate::common::utils::Time as MetricsTime;

        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn MetricsTime>));
        let metrics_manager = Arc::new(ConsumerRebalanceMetricsManager::new(&metrics, Arc::clone(&subs)));
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let metadata = Arc::new(ConsumerMetadata::with_config(
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
        use crate::common::Metric;
        use crate::common::utils::Time as MetricsTime;

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
        use crate::common::Metric;

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
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        if with_listener {
            subs.lock()
                .unwrap()
                .subscribe_with_topics(HashSet::new(), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let metadata = Arc::new(ConsumerMetadata::with_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let commit_mgr = Arc::new(crate::consumer::internals::CommitRequestManager::new(
            &config,
            metadata.clone(),
            subs.clone(),
            "test-group",
            None,
            Arc::new(crate::common::utils::SystemTime),
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
            Arc::new(crate::common::utils::SystemTime),
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
        assert_eq!(mgr.leave_group_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
    }

    /// `leave_group_epoch` returns -2 for static members (default
    /// operation).
    #[test]
    fn leave_group_epoch_static_member() {
        let (mgr, _rx) = make(Some("static-1".to_string()), None, None);
        assert_eq!(
            mgr.leave_group_epoch(),
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH
        );
    }

    /// `leave_group_epoch` returns -1 for static members when
    /// LEAVE_GROUP is explicitly set (force-fence path).
    #[test]
    fn leave_group_epoch_static_member_force_leave() {
        let (mgr, _rx) = make(Some("static-1".to_string()), None, None);
        mgr.set_leave_group_operation(GroupMembershipOperation::LeaveGroup);
        assert_eq!(mgr.leave_group_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
    }

    // Helper: force the manager into PREPARE_LEAVING so the
    // transition to LEAVING (via `transition_to_sending_leave_group`)
    // is valid per `MemberState::previous_valid_states`.
    /// Java `membershipManager.leaveGroup()` for a member with no
    /// assignment to release: the leave runs synchronously to LEAVING and no
    /// callback is left pending.
    fn leave_group_no_callback(mgr: &ConsumerMembershipManager) -> KafkaFutureImpl<()> {
        let leave_result = mgr.leave_group(0).unwrap();
        assert!(
            !mgr.has_pending_leave_callback(),
            "no release callback expected for this member"
        );
        leave_result
    }

    /// Java `membershipManager.leaveGroup()`. Returns the leave future and
    /// whether a release callback was enqueued; when it was, the test acks
    /// it and then calls `drive_pending_leave_callback()` the way the bg loop
    /// does each iteration (Java's `callbackResult.whenComplete(...)`).
    fn leave_group(mgr: &ConsumerMembershipManager) -> (KafkaFutureImpl<()>, bool) {
        let leave_result = mgr.leave_group(0).unwrap();
        (leave_result, mgr.has_pending_leave_callback())
    }

    /// Java `mockPrepareLeavingStuckOnUserCallback`: a member in the group
    /// (epoch > 0) owning `topic1-0` calls `leaveGroup()`, which leaves it in
    /// PREPARE_LEAVING waiting on the `onPartitionsRevoked` callback. Returns
    /// the leave future and the callback's ack sender (Java's
    /// `ConsumerRebalanceListenerCallbackCompletedEvent`).
    fn prepare_leaving_stuck_on_user_callback(
        mgr: &ConsumerMembershipManager,
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) -> (KafkaFutureImpl<()>, oneshot::Sender<Result<(), Error>>) {
        subscribe_with_topics(mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        mgr.abstract_mm.inner.lock().unwrap().update_member_epoch(1);
        mock_owned_partitions(mgr, &[tp("topic1", 0)]);
        let (leave_result, callback_pending) = leave_group(mgr);
        assert!(callback_pending, "the revoked callback was enqueued");
        let ack = match rx.try_recv().expect("revoked callback event").event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                ack
            },
            other => panic!("unexpected event: {other:?}"),
        };
        (leave_result, ack)
    }

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

    /// Rust-only: after a reconciliation the subscription state holds the
    /// assignment in Java's order, topic name then partition, whatever the
    /// order of the heartbeat's assignment. Java gets it from
    /// `TopicIdPartitionSet#topicPartitions()` (a `TreeSet`), carried through
    /// `PartitionsAssignedEvent` and `ApplyAssignmentEvent` into
    /// `SubscriptionState#assignFromSubscribedAwaitingCallback`. Repeated over
    /// fresh managers so that a hash-ordered step would show.
    #[tokio::test]
    async fn reconcile_applies_the_assignment_in_java_order() {
        let tp = |topic: &str, partition: i32| TopicPartition::new(topic.to_string(), partition);
        let expected = vec![
            tp("alpha", 0),
            tp("alpha", 2),
            tp("beta", 0),
            tp("beta", 1),
            tp("beta", 3),
            tp("gamma", 1),
        ];
        for _ in 0..20 {
            let (mgr, mut rx) = make(None, None, None);
            mgr.transition_to_joining().unwrap();

            let mut new_assignment = HashMap::new();
            for (topic, partitions) in [("gamma", vec![1]), ("alpha", vec![2, 0]), ("beta", vec![3, 0, 1])] {
                let topic_id = Uuid::random_uuid();
                mgr.abstract_mm
                    .inner
                    .lock()
                    .unwrap()
                    .assigned_topic_names_cache
                    .insert(topic_id, topic.to_string());
                new_assignment.insert(topic_id, partitions);
            }
            mgr.abstract_mm.process_assignment_received(new_assignment).unwrap();

            mgr.reconcile(0, true).unwrap();
            let env = rx.recv().await.expect("event");
            let BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } = env.event else {
                panic!("expected PartitionsAssigned");
            };
            assert_eq!(expected, assigned_partitions);

            // The app side hands the assignment back in an `ApplyAssignmentEvent`.
            mgr.apply_assignment(&assigned_partitions, &added_partitions).unwrap();
            ack.send(Ok(())).unwrap();
            let subscriptions = mgr.abstract_mm.subscriptions.lock().unwrap();
            assert_eq!(expected, subscriptions.assigned_partitions_list());
        }
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
        mgr.reconcile(0, true).unwrap();
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
            mgr.reconcile(0, true).unwrap();
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
        mgr.reconcile(0, true).unwrap();
        assert_eq!(
            mgr.state(),
            MemberState::Acknowledging,
            "state must advance once the listener ack arrives"
        );
    }

    /// Regression for COMMENTS R2-2: `reconcile(now, can_commit=false)`
    /// is a no-op when auto-commit is enabled AND a commit manager is
    /// present, mirroring the auto-commit half of Java's gate
    /// `if (!canCommit && (autoCommitEnabled || !revokedPartitions.isEmpty())) return;`
    /// (`AbstractMembershipManager#maybeReconcile`; KAFKA-20106 added the
    /// revocation half).
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
        mgr.reconcile(0, false).unwrap();

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
        use crate::consumer::internals::MemberStateListener;
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
    /// `transition_to_fenced`'s short-circuit; when the stuck callback then
    /// completes, the member stays UNSUBSCRIBED (no last HB sent).
    #[tokio::test]
    async fn fencing_when_state_is_prepare_leaving() {
        let (mgr, mut rx) = make(None, None, None);
        let (_leave_result, callback_ack) = prepare_leaving_stuck_on_user_callback(&mgr, &mut rx);
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // Get fenced while preparing to leave the group. Member should ignore
        // the fence (no callbacks or rejoin) and should transition to
        // UNSUBSCRIBED to effectively stop sending heartbeats.
        mgr.transition_to_fenced(0).unwrap();
        assert!(rx.try_recv().is_err(), "the fence must trigger no callback");
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // When PREPARE_LEAVING completes, the member should remain
        // UNSUBSCRIBED (no last HB sent because member is already out of the
        // group in the broker).
        callback_ack.send(Ok(())).unwrap();
        mgr.drive_pending_leave_callback().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.abstract_mm.inner.lock().unwrap().should_skip_heartbeat());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFencingWhenStateIsLeaving`.
    /// A LEAVING member that gets fenced transitions to UNSUBSCRIBED
    /// (no callback, no last HB) and completes the ongoing leave.
    #[tokio::test]
    async fn fencing_when_state_is_leaving() {
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        // Start leaving group.
        let leave_operation = leave_group_no_callback(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(!leave_operation.is_done());

        // Get fenced while leaving. Member should not trigger any callback or
        // try to rejoin.
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(
            mgr.state(),
            MemberState::Unsubscribed,
            "Member should transition from LEAVING to UNSUBSCRIBED when getting fenced (it does not \
             need to send leave request if fenced"
        );
        assert!(
            leave_operation.is_done(),
            "Fenced member should complete the ongoing leave operation"
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testListenersGetNotifiedOnTransitionsToFatal`.
    /// `MemberStateListener::on_member_epoch_updated` is invoked with
    /// `None` when the manager transitions to FATAL.
    #[tokio::test]
    async fn listeners_get_notified_on_transitions_to_fatal() {
        use crate::consumer::internals::MemberStateListener;
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
        use crate::consumer::internals::MemberStateListener;
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

        let _leave = leave_group_no_callback(&mgr);
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
        let (mgr, mut rx) = make(None, None, None);
        let (_leave_result, callback_ack) = prepare_leaving_stuck_on_user_callback(&mgr, &mut rx);
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // Java's onHeartbeatSuccess in PREPARE_LEAVING state is a no-op
        // for new assignments. We invoke it on the response path.
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::{Assignment, TopicPartitions};
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
        assert!(topics_awaiting_reconciliation(&mgr).is_empty());

        // When callback completes member should transition to LEAVING.
        callback_ack.send(Ok(())).unwrap();
        mgr.drive_pending_leave_callback().unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
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
        let _leave = leave_group_no_callback(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(
            mgr.member_epoch(),
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH
        );

        // Dynamic member -> -1.
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let _leave = leave_group_no_callback(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
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
        mgr.leave_group_on_close(GroupMembershipOperation::Default, 0).unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(
            mgr.member_epoch(),
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH
        );

        // Static member with LEAVE_GROUP -> -1.
        let (mgr, _rx) = make(Some("instance1".to_string()), None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group_on_close(GroupMembershipOperation::LeaveGroup, 0).unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);

        // Dynamic member with DEFAULT -> -1.
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        mgr.leave_group_on_close(GroupMembershipOperation::Default, 0).unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
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
            ack.send(Err(Error::timeout("listener error"))).unwrap();
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

    use crate::MetadataResponseData;
    use crate::common::Node;
    use crate::common::errors::InterruptError;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::MetadataResponse;
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};

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
        MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version())
    }

    /// Feed REAL topic-name metadata to the manager so that
    /// `find_resolvable_assignment_and_trigger_metadata_update` resolves
    /// the given topic ids without pre-seeding the local cache. Mirrors
    /// Java's `when(metadata.topicNames()).thenReturn(map)`.
    fn seed_metadata(mgr: &ConsumerMembershipManager, topics: &[(&str, Uuid)]) {
        let response = build_metadata_response(topics);
        mgr.abstract_mm
            .metadata
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
            let metadata_names = mgr.abstract_mm.metadata.topic_names();
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

    /// Test view: `true` when the stored cross-iteration reconcile state is
    /// specifically the `AwaitingCommit` leg (auto-commit-before-rebalance in
    /// flight — Milestone-12 divergence fix, PR #176). Distinguishes it from
    /// the `AfterRevoke` / `AfterAssign` callback legs.
    fn pending_reconcile_is_awaiting_commit(mgr: &ConsumerMembershipManager) -> bool {
        let guard = mgr.abstract_mm.pending_reconcile.lock().unwrap();
        matches!(*guard, Some(PendingReconcile::AwaitingCommit { .. }))
    }

    /// Single, non-looping `reconcile` call — reaches the next callback park
    /// point (storing the cross-iteration pending state) and returns. Used by
    /// tests that want to observe the parked reconcile state without a
    /// concurrent driver loop.
    async fn reconcile_once(mgr: &ConsumerMembershipManager, can_commit: bool) -> Result<(), Error> {
        mgr.reconcile(0, can_commit)
    }

    /// Receive a heartbeat response carrying the given target assignment,
    /// mirroring the Java `receiveAssignment(topicId, partitions, mgr)`
    /// helper.
    fn receive_assignment(mgr: &ConsumerMembershipManager, topic_id: Uuid, partitions: Vec<i32>) {
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::{Assignment, TopicPartitions};
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
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::{Assignment, TopicPartitions};
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
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::Assignment;
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
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
        can_commit: bool,
        expected_method: ConsumerRebalanceListenerMethodName,
        expected_partitions: &[TopicPartition],
    ) {
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(can_commit).await });
        let env = rx.recv().await.expect("expected a callback-needed event");
        match env.event {
            // Revoke / lost path (AK 4.3.1: `PartitionsRemovedEvent`).
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions, .. } => {
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
    fn subscribe_with_topics(mgr: &ConsumerMembershipManager, topics: &[&str]) {
        let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        let set: HashSet<String> = topics.iter().map(|s| s.to_string()).collect();
        // Preserve the NoopListener registered by `make()` so the §31
        // handshake still enqueues callback events.
        subs.subscribe_with_topics(set, Some(Arc::new(NoopListener))).unwrap();
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
        // (real-SubscriptionState gate; see `subscribe_with_topics`).
        subscribe_with_topics(&mgr, &["topic1"]);
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
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions, .. } => {
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
        mgr.reconcile(0, true).unwrap();
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
        // no-listener helper skips subscribe_with_topics, so subscribe here
        // (still without a listener).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
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
        mgr.reconcile(0, true).unwrap();
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
        mgr.reconcile(0, true).unwrap();
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "no further events after the assignment ack",
        );

        // testRevocationOfAllPartitionsCompleted: ACKNOWLEDGING, empty
        // current assignment, all revoked.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!reconciliation_in_progress(&mgr));
        assert!(mgr.current_assignment().is_none() || mgr.current_assignment().partitions.is_empty());
        // verify(subscriptionState, times(2)).markPendingRevocation(Set.of(topic1-0)):
        // once by `maybeReconcile`, once by `revokePartitions`.
        assert_eq!(
            pending_revocation_marks(&mgr),
            vec![vec![tp("topic1", 0)], vec![tp("topic1", 0)]]
        );
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
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        // A consumed position, so the auto-commit before the rebalance sends a
        // request whose completion the test controls (Java's pending
        // `commitResult`).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!reconciliation_in_progress(&mgr), "verifyReconciliationNotTriggered");
        mgr.reconcile(0, true).unwrap();

        // Member stays in RECONCILING while the commit request hasn't completed.
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(pending_reconcile_is_awaiting_commit(&mgr));
        // Partitions should be still owned by the member.
        assert_eq!(
            mgr.abstract_mm.subscriptions.lock().unwrap().assigned_partitions(),
            HashSet::from([tp("topic1", 0)])
        );
        // Java's `InOrder`: the first markPendingRevocation precedes the commit.
        assert_eq!(pending_revocation_marks(&mgr), vec![vec![tp("topic1", 0)]]);

        // Complete commit request.
        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        let offsets = commit_mgr
            .first_unsent_commit_offsets_for_test()
            .expect("auto-commit request sent");
        assert!(commit_mgr.complete_first_unsent_commit_for_test(offsets));
        // The commit result reaches the reconcile through a spawned
        // continuation; drive the reconcile until it has resumed.
        while pending_reconcile_is_awaiting_commit(&mgr) {
            tokio::task::yield_now().await;
            mgr.reconcile(0, true).unwrap();
        }
        process_assignment_event_no_callback(&mgr, &mut rx, &[], &[]).await;
        // ... and the second (`revokePartitions`) follows it.
        assert_eq!(
            pending_revocation_marks(&mgr),
            vec![vec![tp("topic1", 0)], vec![tp("topic1", 0)]]
        );

        // testRevocationOfAllPartitionsCompleted.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(mgr.current_assignment().partitions.is_empty());
        assert!(!reconciliation_in_progress(&mgr));
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway`.
    /// Even if the auto-commit before rebalance fails EXCEPTIONALLY, the
    /// revocation still completes (Java's "proceed with reconciliation
    /// anyway"). Java arranges a commit future and
    /// `commitResult.completeExceptionally(new KafkaException("...non-retriable
    /// error"))`, then asserts the revocation reaches
    /// completion regardless.
    ///
    /// To exercise the failure arm (`log_commit_result`'s `Err` branch, run on
    /// commit failure) we must drive a REAL commit failure: seed a consumed
    /// offset so the auto-commit actually enqueues a request, spawn the
    /// reconcile (which — since the Milestone-12 divergence fix, PR #176 —
    /// parks by storing the `AwaitingCommit` leg rather than awaiting the commit
    /// future inline), then fail that commit with a non-retriable error. The
    /// `reconcile_drive_to_completion` loop `try_recv`s the failed commit result
    /// and proceeds with the revocation anyway. A mutation that turned the
    /// commit-failure branch into `return Err(err)` (abort revocation on commit
    /// failure) would leave the member in RECONCILING and fail this test.
    #[tokio::test]
    async fn reconcile_partitions_revoked_with_failed_auto_commit_completes_revocation_anyway() {
        // No listener: the §31 revoked-callback handshake short-circuits,
        // so the reconcile parks on the commit future and then (AK 4.3.1) on
        // the PartitionsAssigned event that is enqueued even with no listener.
        let (mgr, mut rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
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
            if commit_mgr.fail_first_unsent_commit_for_test(Error::new(Errors::OffsetMetadataTooLarge)) {
                break;
            }
            tokio::task::yield_now().await;
        }
        // The reconcile marked the partition before sending the commit, and
        // has not resumed yet (this task has not yielded since failing it).
        assert_eq!(pending_revocation_marks(&mgr), vec![vec![tp("topic1", 0)]]);

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
        // verify(subscriptionState, times(2)).markPendingRevocation(Set.of(topic1-0)).
        assert_eq!(
            pending_revocation_marks(&mgr),
            vec![vec![tp("topic1", 0)], vec![tp("topic1", 0)]]
        );
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    // ── Milestone-12 divergence fix (PR #176): pre-rebalance auto-commit is
    // deferred via the `AwaitingCommit` leg, so `reconcile()` returns (and
    // `process(AsyncPoll)` marks reconciliation-check-complete, letting `poll()`
    // return RETAINED-partition records) WHILE the commit is still in flight,
    // matching Java
    // `org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor#process(AsyncPollEvent)`.
    // ───────────────

    /// (a) With auto-commit enabled and the commit response withheld, a single
    /// `reconcile()` call parks on the `AwaitingCommit` leg: it returns without
    /// awaiting the commit, the member stays RECONCILING, and NO revoke/assign
    /// callback is enqueued yet. This is the state-machine equivalent of the
    /// app-level "poll() keeps returning retained records during a held-open
    /// commit" behavior (that app-level assertion is not reachable with these
    /// unit fixtures, which have no fetch pipeline, so the state-machine
    /// invariants below stand in for it).
    #[tokio::test]
    async fn reconcile_parks_on_awaiting_commit_while_commit_in_flight() {
        let (mgr, mut rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        // Seed a consumed position so `allConsumed()` is non-empty and the
        // auto-commit-before-rebalance actually enqueues a request that stays
        // in flight (nothing completes it in this test).
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Single reconcile: triggers the commit, parks on `AwaitingCommit`,
        // returns immediately (does NOT await the commit).
        mgr.reconcile(0, true).unwrap();

        assert!(has_pending_reconcile_for_test(&mgr), "reconcile must park on a pending leg");
        assert!(
            pending_reconcile_is_awaiting_commit(&mgr),
            "the parked leg must be AwaitingCommit (not a revoke/assign callback)",
        );
        assert_eq!(
            mgr.state(),
            MemberState::Reconciling,
            "member stays RECONCILING during the commit"
        );
        assert!(
            reconciliation_in_progress(&mgr),
            "reconciliation_in_progress stays true during the commit"
        );
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "no PartitionsRemoved / PartitionsAssigned event before the commit resolves",
        );
        // The commit is genuinely in flight (not a short-circuit).
        let commit_mgr = mgr.commit_request_manager.as_ref().unwrap();
        assert_eq!(
            commit_mgr.unsent_offset_commits_len_for_test(),
            1,
            "a real commit request is enqueued"
        );
    }

    /// (b) Once the withheld commit COMPLETES, the deferred reconcile resumes
    /// the exact commit -> onPartitionsRevoked -> onPartitionsAssigned ->
    /// ACKNOWLEDGING sequence (the gating Java preserves via
    /// `commitResult.whenComplete` -> `revokeAndAssign`). Listener present so
    /// the revoke callback is actually enqueued (`AfterRevoke` leg).
    ///
    /// Uses the spawned bg-driver pattern (like the failed-auto-commit sibling)
    /// so the internal auto-commit retry task — which fires the reconcile's
    /// commit receiver only after the request completes — gets to run.
    #[tokio::test]
    async fn reconcile_after_awaiting_commit_completes_runs_revoke_then_assign() {
        let (mgr, mut rx) = make_with_commit_manager(true);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // First (deterministic, single-task) reconcile parks on the commit:
        // AwaitingCommit stored, and — crucially — NO revoke callback has been
        // enqueued yet (it is gated behind the commit).
        mgr.reconcile(0, true).unwrap();
        assert!(pending_reconcile_is_awaiting_commit(&mgr));
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "revoke callback must NOT precede the commit completion",
        );

        // Now hand off to the bg driver and complete the commit.
        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        let commit_mgr = mgr.commit_request_manager.as_ref().unwrap();
        assert!(commit_mgr.complete_first_unsent_commit_for_test(HashMap::new()));

        // Revoke callback enqueued (topic1-0) once the commit resolves.
        let env = rx.recv().await.expect("onPartitionsRevoked after the commit resolves");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                let got: HashSet<TopicPartition> = partitions.into_iter().collect();
                assert_eq!(got, HashSet::from([tp("topic1", 0)]));
                ack.send(Ok(())).unwrap();
            },
            other => panic!("expected the revoke callback, got {other:?}"),
        }

        // Then the assign callback, sequenced strictly after the revoke ack.
        let env = rx.recv().await.expect("onPartitionsAssigned after the revoke ack");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("expected the assign callback, got {other:?}"),
        }

        bg.await.unwrap().unwrap();
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(!has_pending_reconcile_for_test(&mgr));
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty(), "topic1-0 revoked");
    }

    /// (c) If the auto-commit request is abandoned (dropped without a
    /// response), the reconcile's commit receiver surfaces a failure and the
    /// deferred reconcile logs and PROCEEDS with the revocation anyway —
    /// mirroring Java's `commitResult.whenComplete` error branch ("proceed with
    /// the reconciliation anyway"). No listener: the revoke short-circuits and
    /// the reconcile drives on to ACKNOWLEDGING. This exercises the
    /// `AwaitingCommit` arm's non-`Ok` continuation (the sibling
    /// `reconcile_partitions_revoked_with_failed_auto_commit_completes_revocation_anyway`
    /// covers the explicit broker-error variant of the same "proceed anyway"
    /// contract).
    #[tokio::test]
    async fn reconcile_after_awaiting_commit_abandoned_request_proceeds_anyway() {
        let (mgr, mut rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Park on the commit deterministically.
        mgr.reconcile(0, true).unwrap();
        assert!(pending_reconcile_is_awaiting_commit(&mgr));

        let mgr = Arc::new(mgr);
        let mgr_clone = mgr.clone();
        let bg = tokio::spawn(async move { mgr_clone.reconcile_drive_to_completion(true).await });

        // Abandon the commit request (drop its sender without completing it).
        let commit_mgr = mgr.commit_request_manager.as_ref().unwrap();
        assert!(
            commit_mgr.drop_first_unsent_commit_for_test(),
            "expected an unsent commit to drop"
        );

        // No listener -> revoke short-circuits -> the reconcile enqueues the
        // (empty) PartitionsAssigned event; apply + ack to reach ACKNOWLEDGING.
        let env = rx
            .recv()
            .await
            .expect("PartitionsAssigned after proceeding past the abandoned commit");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        bg.await.unwrap().unwrap();
        assert_eq!(
            mgr.state(),
            MemberState::Acknowledging,
            "revocation proceeds despite the abandoned commit"
        );
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert!(subs.assigned_partitions().is_empty());
    }

    /// (d) A fence WHILE parked on the `AwaitingCommit` leg abandons that leg
    /// via `clear_pending_reconcile()` (Phase 41 Issue 1), exactly as it
    /// abandons the `AfterRevoke` / `AfterAssign` legs — the commit receiver is
    /// simply dropped and NO stale revoke callback is enqueued. Deterministic
    /// single-task version (no concurrent driver, so no take/put-back race) of
    /// `delayed_reconciliation_discarded_after_commit_when_fenced_with_listener`.
    #[tokio::test]
    async fn reconcile_fence_while_awaiting_commit_abandons_leg() {
        let (mgr, mut rx) = make_with_commit_manager(true);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), Some(Arc::new(NoopListener)))
                .unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }

        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Park on the commit (single task, no concurrent driver).
        mgr.reconcile(0, true).unwrap();
        assert!(pending_reconcile_is_awaiting_commit(&mgr));
        assert!(reconciliation_in_progress(&mgr));

        // Fence while parked on the commit: clear_pending_reconcile ABANDONS
        // the AwaitingCommit leg (deterministic — nothing else holds it).
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fenced);
        assert!(
            !has_pending_reconcile_for_test(&mgr),
            "the AwaitingCommit leg is abandoned by the fence"
        );
        assert!(
            !reconciliation_in_progress(&mgr),
            "abandon clears reconciliation_in_progress (fresh post-rejoin reconcile not gated)",
        );

        // The fence enqueues onPartitionsLost for the owned partition; NO
        // onPartitionsRevoked follows from the abandoned stale reconcile.
        let env = rx.recv().await.expect("onPartitionsLost from the fence");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
            },
            other => panic!("expected the lost callback, got {other:?}"),
        }
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "the abandoned stale reconcile must NOT enqueue an onPartitionsRevoked",
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testSameAssignmentReconciledAgainWithMissingTopic`.
    /// One topic resolvable, one permanently missing. The resolvable one
    /// is reconciled+acked; the missing one stays awaiting reconciliation.
    /// An extended assignment is received but not reconciled; the original
    /// assignment re-sent after it is acked again without re-running a full
    /// reconcile.
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

        // Receive extended assignment (assignment2: topic1-0,1 + topic2-0) -
        // assignment received but no reconciliation triggered. Java's step
        // between the two assignment1s: it bumps the target's local epoch, so
        // the re-received assignment1 below is a NEW target epoch. Without it
        // the re-received assignment1 would be the target already acked, which
        // KAFKA-20145 deliberately does not ack again
        // (`partial_ack_not_repeated_on_background_reconcile_when_metadata_missing`).
        receive_assignment_map(&mgr, &[(topic1, vec![0, 1]), (topic2, vec![0])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));

        // Receive original assignment again -> not a full reconcile, but
        // ack again. topic1 stays assigned, topic2 still awaiting.
        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        // No callback emitted on the re-ack (same resolvable partitions).
        // Java reconciles from the background path here (`maybeReconcile(false)`).
        mgr.reconcile(0, false).unwrap();
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
    ///
    /// As in Java no rebalance listener is registered (Java's mocked
    /// `rebalanceListener()` returns `Optional.empty()`), so only the
    /// `PartitionsAssigned` events reach the app side. Java's Mockito
    /// `verify(subscriptionState, times(n)).markPendingRevocation(set)` reads
    /// the real `SubscriptionState`'s test-only call record.
    #[tokio::test]
    async fn revoke_partitions_uses_topic_names_local_cache_when_metadata_not_available() {
        let (mgr, mut rx) = make_without_listener(None, None, None);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        let update_requests_before = mgr.abstract_mm.metadata.request_update_call_count_for_test();

        // Member received assignment to reconcile.
        receive_assignment(&mgr, topic_id, vec![0, 1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        assert!(!reconciliation_in_progress(&mgr), "verifyReconciliationNotTriggered");
        mgr.reconcile(0, true).unwrap();
        process_assignment_event_no_callback(
            &mgr,
            &mut rx,
            &[tp("topic1", 0), tp("topic1", 1)],
            &[tp("topic1", 0), tp("topic1", 1)],
        )
        .await;
        // verify(subscriptionState).markPendingRevocation(Set.of())
        assert_eq!(pending_revocation_marks(&mgr), vec![Vec::<TopicPartition>::new()]);

        // Member should complete reconciliation.
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert_eq!(mgr.current_assignment().partitions, HashMap::from([(topic_id, vec![0, 1])]));
        assert!(!reconciliation_in_progress(&mgr));

        // mockAckSent.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Stable);

        // Revocation of topic not found in metadata cache.
        let other = Uuid::random_uuid();
        seed_metadata(&mgr, &[("other", other)]);

        // Revoke one of the 2 partitions.
        receive_assignment(&mgr, topic_id, vec![1]);
        mgr.reconcile(0, true).unwrap();
        process_assignment_event_no_callback(&mgr, &mut rx, &[tp("topic1", 1)], &[]).await;
        // verify(subscriptionState, times(2)).markPendingRevocation(Set.of(topic1-0)):
        // once by `maybeReconcile`, once by `revokePartitions`.
        assert_eq!(
            pending_revocation_marks(&mgr),
            vec![vec![], vec![tp("topic1", 0)], vec![tp("topic1", 0)]]
        );

        // Revocation should complete without requesting any metadata update
        // given that the topic received in target assignment should exist in
        // local topic name cache.
        assert_eq!(
            mgr.abstract_mm.metadata.request_update_call_count_for_test(),
            update_requests_before
        );

        // testRevocationCompleted(membershipManager, [topic1-1]).
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert_eq!(mgr.current_assignment().partitions, HashMap::from([(topic_id, vec![1])]));
        assert!(!reconciliation_in_progress(&mgr));
        let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        assert_eq!(subs.assigned_partitions(), HashSet::from([tp("topic1", 1)]));
    }

    /// The partitions of every `SubscriptionState::mark_pending_revocation`
    /// call so far (Java's `verify(subscriptionState).markPendingRevocation`).
    fn pending_revocation_marks(mgr: &ConsumerMembershipManager) -> Vec<Vec<TopicPartition>> {
        mgr.abstract_mm
            .subscriptions
            .lock()
            .unwrap()
            .pending_revocation_marks_for_test()
            .to_vec()
    }

    /// Java's `processAssignmentEventNoCallback`: the app side applies the
    /// `PartitionsAssigned` event without a listener and acks it; the next
    /// reconcile consumes the ack. Asserts the event carried
    /// `expected_assigned` / `expected_added` (Java's
    /// `verify(subscriptionState).assignFromSubscribedAwaitingCallback(assigned, added)`).
    async fn process_assignment_event_no_callback(
        mgr: &ConsumerMembershipManager,
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
        expected_assigned: &[TopicPartition],
        expected_added: &[TopicPartition],
    ) {
        let env = rx.recv().await.expect("PartitionsAssigned event (sent even with no listener)");
        match env.event {
            BackgroundEvent::PartitionsAssigned { assigned_partitions, added_partitions, ack } => {
                let assigned_set: HashSet<TopicPartition> = assigned_partitions.iter().cloned().collect();
                let added_set: HashSet<TopicPartition> = added_partitions.iter().cloned().collect();
                assert_eq!(assigned_set, expected_assigned.iter().cloned().collect::<HashSet<_>>());
                assert_eq!(added_set, expected_added.iter().cloned().collect::<HashSet<_>>());
                mgr.apply_assignment(&assigned_set, &added_partitions).unwrap();
                ack.send(Ok(())).unwrap();
            },
            other => panic!("unexpected event: {other:?}"),
        }
        mgr.reconcile(0, true).unwrap();
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
            mgr.abstract_mm.metadata.update_requested(),
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
        assert!(mgr.abstract_mm.metadata.update_requested());
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
        assert!(mgr.abstract_mm.metadata.update_requested());

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

        let metadata = Arc::clone(&mgr.abstract_mm.metadata);

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
        mgr.reconcile(0, true).unwrap();
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
        assert_eq!(mgr.state(), MemberState::Acknowledging);
        assert!(mgr.abstract_mm.metadata.update_requested());
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

    /// Translated from
    /// `ConsumerMembershipManagerTest#testPartialAckNotRepeatedOnBackgroundReconcileWhenMetadataMissing`
    /// (KAFKA-20145). When the resolvable subset of the target equals the
    /// current assignment but some topic ids remain unresolved, the partial
    /// acknowledgement fires exactly once per target epoch, whichever path
    /// reconciles (`can_commit` false = bg loop, true = `AsyncPollEvent`).
    #[tokio::test]
    async fn partial_ack_not_repeated_on_background_reconcile_when_metadata_missing() {
        let (mgr, mut rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        let topic2 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);

        // `mockMemberSuccessfullyReceivesAndAcksAssignment(topicId1, topic1, [0])`.
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
        assert_eq!(MemberState::Stable, mgr.state());

        receive_assignment_map(&mgr, &[(topic1, vec![0]), (topic2, vec![0])]);

        // First reconcile from the background task acks the resolvable fragment, no need to wait for
        // consumer.poll.
        mgr.reconcile(0, false).unwrap();
        assert_eq!(MemberState::Acknowledging, mgr.state());

        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(MemberState::Reconciling, mgr.state());
        assert_eq!(HashSet::from([topic2]), topics_awaiting_reconciliation(&mgr));
        let acked_local_epoch = mgr.abstract_mm.inner.lock().unwrap().current_assignment.local_epoch;

        // Until a new target arrives or new metadata resolves topicId2, further reconciles from either
        // path must stay in RECONCILING - no redundant acks.
        for _ in 0..5 {
            mgr.reconcile(0, false).unwrap();
            assert_eq!(MemberState::Reconciling, mgr.state());
            mgr.reconcile(0, true).unwrap();
            assert_eq!(MemberState::Reconciling, mgr.state());
        }
        // The value the redundant acks used to change: the local epoch is the
        // one acknowledged once, and no rebalance callback was enqueued.
        assert_eq!(
            acked_local_epoch,
            mgr.abstract_mm.inner.lock().unwrap().current_assignment.local_epoch
        );
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
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
    /// (`AbstractMembershipManager#revokeAndAssign`). Rust replaced the
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
            subscribe_with_topics(&mgr, &["topic1"]);
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
            subscribe_with_topics(&mgr, &["topic1"]);
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
    /// Since the Milestone-12 divergence fix (PR #176) the in-flight commit is
    /// no longer awaited inline — it is stored as the `AwaitingCommit`
    /// [`PendingReconcile`] leg. So the stale reconcile is now discarded by
    /// TWO cooperating mechanisms, whichever wins the take race between the
    /// bg-driver loop and the fence: (1) the concurrent
    /// `clear_pending_reconcile()` inside `transition_to_fenced` ABANDONS the
    /// `AwaitingCommit` leg (Phase 41 Issue 1 — the same abandon that already
    /// covers the `AfterRevoke` / `AfterAssign` legs), and/or (2) the
    /// `maybe_abort_reconciliation()` guard in `continue_after_commit` short-
    /// circuits once the commit resolves because the member is no longer
    /// `RECONCILING`. Either way NO revoke callback is enqueued after the fence.
    /// (See `reconcile_fence_while_awaiting_commit_abandons_leg` for the
    /// deterministic single-task version of the abandon.)
    ///
    /// Java: `commitResult.whenComplete((__, commitReqError) -> { ...;
    /// if (!maybeAbortReconciliation()) { revokeAndAssign(...); } })`
    /// (`AbstractMembershipManager#maybeReconcile`).
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
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), Some(Arc::new(NoopListener)))
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        subscribe_with_topics(&mgr, &["topic1", "topic3"]);
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);

        receive_assignment(&mgr, topic1, vec![1]);
        assert_eq!(mgr.state(), MemberState::Reconciling);

        // Reconcile parks on the onPartitionsAssigned callback (single
        // non-looping call). Capture the ack but do NOT complete it.
        reconcile_once(&mgr, true).await.unwrap();
        let env = rx.recv().await.expect("assigned event");
        let stuck_ack = match env.event {
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
        // Java: `clearInvocations(subscriptionState)` after the fence.
        mgr.abstract_mm
            .subscriptions
            .lock()
            .unwrap()
            .clear_pending_revocation_marks_for_test();

        // (b) of Issue 1: the fresh post-rejoin assignment (topic3-5) can be
        // reconciled — it is not gated on the abandoned stale reconcile.
        let topic3 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic3", topic3)]);
        receive_assignment(&mgr, topic3, vec![5]);
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic3, vec![5])])
        );
        // Java's `completeCallback`: the stale ack arrives after the rejoin
        // (its reconcile was abandoned, so nothing receives it).
        let _ = stuck_ack.send(Ok(()));
        // assertInitialReconciliationDiscardedAfterRejoin:
        // `verify(subscriptionState, never()).markPendingRevocation(any())`.
        assert!(pending_revocation_marks(&mgr).is_empty());
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
        subscribe_with_topics(&mgr, &["topic1", "topic3"]);
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
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions, .. } => {
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
        // Java: `clearInvocations(subscriptionState)` after the fence.
        mgr.abstract_mm
            .subscriptions
            .lock()
            .unwrap()
            .clear_pending_revocation_marks_for_test();

        // (b) of Issue 1: fresh post-rejoin assignment reconciles.
        let topic3 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic3", topic3)]);
        receive_assignment(&mgr, topic3, vec![5]);
        assert_eq!(
            topic_partitions_awaiting_reconciliation(&mgr),
            HashMap::from([(topic3, vec![5])])
        );
        // Java's `completeCallback`: the stale onPartitionsRevoked ack
        // arrives after the rejoin; its reconcile was abandoned.
        let _ = stuck_ack.send(Ok(()));
        // assertInitialReconciliationDiscardedAfterRejoin:
        // `verify(subscriptionState, never()).markPendingRevocation(any())`.
        assert!(pending_revocation_marks(&mgr).is_empty());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testDelayedReconciliationResultDiscardedAfterCommitIfMemberRejoins`.
    /// A member is stuck reconciling assignment A, parked on the REVOCATION
    /// COMMIT future (Java's
    /// `ConsumerMembershipManagerTest#mockNewAssignmentAndRevocationStuckOnCommit`). While parked on the commit it
    /// gets fenced and rejoins; when the commit later completes (`commitResult.complete(null)`) the in-flight reconcile
    /// must be DISCARDED — no assignment applied, no ack sent.
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
    /// reconcile's ONLY park point is the commit — since the Milestone-12
    /// divergence fix (PR #176) it parks by storing the `AwaitingCommit`
    /// [`PendingReconcile`] leg rather than awaiting the commit inline, and the
    /// rejoin lands while the member is stalled on that leg.
    ///
    /// Adapted from the pre-fix version (which spawned `reconcile_drive_to_
    /// completion` and had to `complete_first_unsent_commit_for_test` to release
    /// the inline `commit_rx.await`). Now the commit is not awaited inline, so —
    /// like the sibling `..._after_partitions_revoked_callback_if_member_rejoins`
    /// — we use a single `reconcile_once` to park on the leg and step the fence
    /// manually. The fence ABANDONS the `AwaitingCommit` leg via
    /// `clear_pending_reconcile()` (Phase 41 Issue 1), which is the Rust analog
    /// of Java's `commitResult.whenComplete` finding the member no longer
    /// RECONCILING and calling `maybeAbortReconciliation()`. The stale
    /// empty-target reconcile is therefore discarded WITHOUT ever completing the
    /// commit (the abandoned commit receiver is simply dropped; the request
    /// itself proceeds or times out server-side — irrelevant to the discard).
    #[tokio::test]
    async fn delayed_reconciliation_result_discarded_after_commit_if_member_rejoins() {
        let (mgr, _rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
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

        // Single reconcile: parks on the `AwaitingCommit` leg (the commit
        // request is enqueued and in flight).
        reconcile_once(&mgr, true).await.unwrap();
        assert!(pending_reconcile_is_awaiting_commit(&mgr));
        assert!(reconciliation_in_progress(&mgr));
        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        assert_eq!(commit_mgr.unsent_offset_commits_len_for_test(), 1);

        // Rejoin via the fence path while parked on the commit (FENCED ->
        // JOINING). With no listener the onPartitionsLost callback short-circuits
        // so the release completes synchronously. The fence ABANDONS the
        // `AwaitingCommit` leg (Phase 41 Issue 1) and clears
        // `reconciliation_in_progress`.
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Joining);
        assert!(
            !has_pending_reconcile_for_test(&mgr),
            "the AwaitingCommit leg is abandoned by the fence"
        );
        // Java: `clearInvocations(subscriptionState)` after the fence.
        mgr.abstract_mm
            .subscriptions
            .lock()
            .unwrap()
            .clear_pending_revocation_marks_for_test();

        // New assignment after rejoin (topic3-5).
        let topic3 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1), ("topic3", topic3)]);
        receive_assignment(&mgr, topic3, vec![5]);

        // Java: `commitResult.complete(null)` after the rejoin. The abandoned
        // leg no longer listens, so completing the request changes nothing.
        let offsets = commit_mgr.first_unsent_commit_offsets_for_test().expect("commit still unsent");
        assert!(commit_mgr.complete_first_unsent_commit_for_test(offsets));
        tokio::task::yield_now().await;
        // assertInitialReconciliationDiscardedAfterRejoin:
        // `verify(subscriptionState, never()).markPendingRevocation(any())`.
        assert!(pending_revocation_marks(&mgr).is_empty());

        // Discarded: the stale empty-target reconcile never advanced to
        // ACKNOWLEDGING (no ack sent), and the post-rejoin target (topic3-5) is
        // what is pending to reconcile next — proving the stale empty-target
        // reconcile was NOT applied.
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
        subscribe_with_topics(&mgr, &["topic1", "topic2"]);
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
        subscribe_with_topics(&mgr, &["topic1", "topic2"]);
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
    // still be REGISTERED (NoopListener via `make`/`subscribe_with_topics`) for
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
        rx: &mut mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
        mgr: Option<&ConsumerMembershipManager>,
        expected_method: ConsumerRebalanceListenerMethodName,
        expected_partitions: &[TopicPartition],
        result: Result<(), Error>,
    ) {
        let env = rx.recv().await.expect("expected a callback-needed event");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions, .. } => {
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        // Java's three, class for class
        // (`ConsumerMembershipManagerTest#testListenerCallbacksThrowsErrorOnPartitionsRevoked`):
        // `WakeupException`, `InterruptException`, `IllegalArgumentException`.
        // `Interrupt` is not interchangeable with any other class here: together
        // with `Wakeup` it is one of the two that
        // `ConsumerRebalanceListenerInvoker` rethrows verbatim
        // (`Error::Wakeup(_) | Error::Interrupt(_) => Err(err)`) rather than
        // wrapping, so substituting a wrapped class would leave that branch
        // uncovered and duplicate the `LocalIllegalArgument` case.
        let errors: Vec<fn() -> Error> = vec![
            || Error::wakeup("Intentional onPartitionsRevoked() error"),
            || Error::Interrupt(InterruptError::new("Intentional onPartitionsRevoked() error")),
            || Error::local_illegal_argument("Intentional onPartitionsRevoked() error"),
        ];
        for make_err in errors {
            let (mgr, mut rx) = make(None, None, None);
            subscribe_with_topics(&mgr, &["topic1"]);
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        ack.send(Err(Error::local_illegal_state("onPartitionsAssigned failed!")))
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
        on_partitions_lost_impl(Err(Error::local_illegal_state("Intentional error for test"))).await;
        on_partitions_lost_impl(Err(Error::wakeup("Intentional error for test"))).await;
        on_partitions_lost_impl(Err(Error::timeout("Intentional error for test"))).await;
    }

    async fn on_partitions_lost_impl(callback_result: Result<(), Error>) {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_with_topics(&mgr, &["topic1"]);
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        let _leave = leave_group_no_callback(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);

        // Complete the pending assignment event exceptionally (simulating the
        // app skipping it during unsubscribe, KAFKA-20428).
        ack.send(Err(Error::with_message(
            Errors::UnknownServerError,
            "Assignment event skipped because consumer is unsubscribing",
        )))
        .unwrap();
        // Drive the parked reconcile so the exceptional ack clears the pending
        // state (continue_after_assign observes the callback error and returns).
        let _ = mgr.reconcile(0, true);
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
    // `leave_group()` returns the leave-in-progress future like Java's
    // `leaveGroup()`, so Java's `leaveResult.isDone()` assertions translate
    // to `KafkaFutureImpl::is_done`: leave_group -> LEAVING (not done),
    // on_heartbeat_request_generated -> UNSUBSCRIBED (not done), and the
    // leave completes when the leave heartbeat response is processed.
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
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
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
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::Assignment;
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
        use crate::ConsumerGroupHeartbeatResponseData;
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(member_id);
        data.member_epoch = ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH;
        ConsumerGroupHeartbeatResponse::new(data)
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenStateIsStable`.
    #[tokio::test]
    async fn leave_group_when_state_is_stable() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        let (leave_result, continuation) = leave_group(&mgr);
        assert!(!continuation);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(
            !leave_result.is_done(),
            "Leave group result should not complete until the heartbeat request to leave is sent out."
        );
        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(&mgr, &leave_result);
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.current_assignment().is_none());
    }

    /// Java: `assertTransitionToUnsubscribeOnHBSentAndWaitForResponseToCompleteLeave`.
    fn assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(
        mgr: &ConsumerMembershipManager,
        send_leave: &KafkaFutureImpl<()>,
    ) {
        assert_eq!(mgr.state(), MemberState::Leaving);
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert!(
            !send_leave.is_done(),
            "Send leave operation should not complete until a response is received"
        );
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), 1)).unwrap();
        assert!(
            !send_leave.is_done(),
            "Send leave operation should not complete until a leave response is received"
        );

        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_send_leave_completed(mgr, send_leave);
    }

    /// Java: `assertSendLeaveCompleted`.
    fn assert_send_leave_completed(mgr: &ConsumerMembershipManager, send_leave: &KafkaFutureImpl<()>) {
        assert_eq!(
            mgr.state(),
            MemberState::Unsubscribed,
            "Member should remain UNSUBSCRIBED after receiving the response to the HB to leave"
        );
        assert_eq!(mgr.member_epoch(), -1);
        assert!(mgr.current_assignment().is_none());
        assert!(
            send_leave.is_done(),
            "Leave group result should complete when the response to the heartbeat request to leave is received."
        );
        assert_leave_completed_successfully(send_leave);
    }

    /// Java: `assertFalse(future.isCompletedExceptionally())` on a done
    /// leave future.
    fn assert_leave_completed_successfully(leave: &KafkaFutureImpl<()>) {
        use futures_util::FutureExt;
        let result = leave.future().get().now_or_never();
        assert!(
            matches!(result, Some(Ok(()))),
            "the leave must be done and not completed with an error: {result:?}"
        );
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberOwnsAssignment`.
    /// A member owning a partition fires onPartitionsRevoked during the
    /// leave, then transitions to LEAVING.
    #[tokio::test]
    async fn leave_group_when_member_owns_assignment() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_with_topics(&mgr, &["topic1"]);
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

        // Leave: owned partitions fire onPartitionsRevoked. The member waits
        // in PREPARE_LEAVING for the callback.
        let (leave_result, continuation) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
        expect_callback(
            &mut rx,
            None,
            ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            &[tp("topic1", 0), tp("topic1", 1)],
            Ok(()),
        )
        .await;
        assert!(continuation, "a release callback was enqueued");
        mgr.drive_pending_leave_callback().unwrap();
        // Java: testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup.
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(
            !leave_result.is_done(),
            "Leave group result should not complete until the heartbeat request to leave is sent out."
        );
        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(&mgr, &leave_result);
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
        assert!(mgr.current_assignment().is_none());
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberAlreadyLeaving`.
    /// A second leave while LEAVING triggers nothing and returns the SAME
    /// in-progress future (`AbstractMembershipManager#leaveGroup(boolean)`), so
    /// both complete together on the leave heartbeat's response.
    #[tokio::test]
    async fn leave_group_when_member_already_leaving() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;

        // First leave attempt. Stays LEAVING until the heartbeat is sent out.
        let (leave_result1, continuation) = leave_group(&mgr);
        assert!(!continuation);
        assert!(!leave_result1.is_done());
        assert_eq!(mgr.state(), MemberState::Leaving);

        // Second leave attempt while the first one has not completed yet.
        // Should not trigger any callbacks, and return a future that will
        // complete when the ongoing first leave operation completes.
        let (leave_result2, continuation) = leave_group(&mgr);
        assert!(!continuation, "a second leave must not trigger callbacks");
        assert!(!leave_result2.is_done());
        assert_eq!(mgr.state(), MemberState::Leaving);

        // Complete first leave group operation. Should also complete the
        // second leave group.
        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(&mgr, &leave_result1);
        assert!(leave_result2.is_done());
        assert_leave_completed_successfully(&leave_result2);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testLeaveGroupWhenMemberAlreadyLeft`.
    /// After fully leaving (UNSUBSCRIBED), a further leave is a no-op
    /// (unsubscribes, no callbacks).
    #[tokio::test]
    async fn leave_group_when_member_already_left() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        // Leave group triggered and completed.
        let (leave_result1, _) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert_transition_to_unsubscribe_on_hb_sent_and_wait_for_response_to_complete_leave(&mgr, &leave_result1);

        // Leave again when already left -> no-op (no callbacks), already
        // completed future, stays UNSUBSCRIBED.
        let (leave_result2, continuation) = leave_group(&mgr);
        assert!(!continuation);
        assert!(leave_result2.is_done());
        assert_leave_completed_successfully(&leave_result2);
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

        let (leave_operation, continuation) = leave_group(&mgr);
        assert!(!continuation);
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(leave_operation.is_done());
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        let (leave_result, continuation) = leave_group(&mgr);
        assert!(continuation, "a revoked callback was enqueued");
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
        // Beyond Java's test (which does not keep the leave future): the
        // fatal transition completes the ongoing leave
        // (`AbstractMembershipManager#transitionToFatal`).
        assert_leave_completed_successfully(&leave_result);

        // Complete the stuck callback -> the leave is aborted; remains FATAL.
        stuck_ack.send(Ok(())).unwrap();
        mgr.drive_pending_leave_callback().unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testFatalFailureWhenStateIsLeaving`.
    /// A member in LEAVING that gets a fatal error transitions to FATAL;
    /// a subsequent heartbeat-generated does not move it out of FATAL.
    #[tokio::test]
    async fn fatal_failure_when_state_is_leaving() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        // Start leaving group.
        let (leave_operation, _) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(!leave_operation.is_done());

        // Get fatal failure while waiting to send the heartbeat to leave.
        mgr.on_heartbeat_failure(false);
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);
        assert!(
            leave_operation.is_done(),
            "Member should complete the ongoing leave operation when transitioning to FATAL state"
        );

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
        // Start leaving group.
        let (send_leave, _) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        // Last heartbeat sent.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // Fatal failure received after the member already left -> FATAL,
        // no callbacks (no onPartitionsLost).
        mgr.on_heartbeat_failure(false);
        mgr.transition_to_fatal(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Fatal);

        assert!(send_leave.is_done());
        // Send leave should complete when a response to the leave group is
        // received, even if it contains errors.
        assert_leave_completed_successfully(&send_leave);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave`.
    /// `leave_group()` returns the leave-in-progress future, completed only by
    /// `maybe_complete_leave_in_progress()` when the response to the leave
    /// heartbeat arrives (`AbstractMembershipManager#onHeartbeatSuccess`) — not when the
    /// member reaches LEAVING, nor when the leave heartbeat is only generated,
    /// nor on a non-leave response.
    #[tokio::test]
    async fn heartbeat_successful_response_when_leaving_group_completes_leave() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;

        let (leave_result, _) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(
            !leave_result.is_done(),
            "Java: leaveGroup() must not complete while the member is LEAVING (leave heartbeat not sent yet)"
        );

        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(
            !leave_result.is_done(),
            "Java: leaveGroup() must not complete when the leave heartbeat is only generated (no response yet)"
        );

        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), 1)).unwrap();
        assert!(
            !leave_result.is_done(),
            "Java: a non-leave heartbeat response must not complete the leave"
        );

        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_send_leave_completed(&mgr, &leave_result);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave`
    /// (`@ValueSource(booleans = {true, false})`). The leave completes on the
    /// failed response to the leave heartbeat
    /// (`AbstractMembershipManager.onHeartbeatFailure`), not before.
    #[tokio::test]
    async fn heartbeat_failed_response_when_leaving_group_completes_leave() {
        for retriable in [true, false] {
            let (mgr, _rx) = create_member_in_stable_state(None).await;

            let (leave_result, _) = leave_group(&mgr);
            assert_eq!(mgr.state(), MemberState::Leaving);
            assert!(
                !leave_result.is_done(),
                "[retriable={retriable}] Java: leaveGroup() must not complete while the member is LEAVING"
            );

            mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
            assert_eq!(mgr.state(), MemberState::Unsubscribed);
            assert!(
                !leave_result.is_done(),
                "[retriable={retriable}] Java: leaveGroup() must not complete before the leave heartbeat fails"
            );

            mgr.on_heartbeat_failure(retriable);
            assert_send_leave_completed(&mgr, &leave_result);
        }
    }

    /// A heartbeat failure while the member is still LEAVING (the leave
    /// heartbeat has not been generated yet, so the failure is the response
    /// to an earlier heartbeat) must not complete the leave: Java completes
    /// it only for an UNSUBSCRIBED member (`onHeartbeatFailure`).
    #[tokio::test]
    async fn heartbeat_failure_before_leave_heartbeat_does_not_complete_leave() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        let (leave_result, _) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);

        mgr.on_heartbeat_failure(true);
        assert!(
            !leave_result.is_done(),
            "a failed response to a heartbeat sent before the leave heartbeat must not complete the leave"
        );
        assert_eq!(mgr.state(), MemberState::Leaving);
    }

    /// Translated from the `onHeartbeatRequestSkipped` branch of
    /// `AbstractMembershipManager`: when the leave heartbeat
    /// cannot be sent (e.g. no coordinator), the member transitions LEAVING
    /// -> UNSUBSCRIBED and completes the leave, so `unsubscribe()` does not
    /// wait for a response that will never come.
    #[tokio::test]
    async fn heartbeat_request_skipped_while_leaving_completes_leave() {
        let (mgr, _rx) = create_member_in_stable_state(None).await;
        let (leave_result, _) = leave_group(&mgr);
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(!leave_result.is_done());

        mgr.abstract_mm.on_heartbeat_request_skipped().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert_leave_completed_successfully(&leave_result);
    }

    /// Java `AbstractMembershipManager#onHeartbeatSuccess`: a response carrying an invalid
    /// negative epoch for a member still in the group completes any leave in
    /// progress. A member in PREPARE_LEAVING (its leave callback still
    /// running) is such a member: it is neither LEAVING nor out of the group.
    #[tokio::test]
    async fn invalid_epoch_response_while_prepare_leaving_completes_leave() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_with_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        let mgr = Arc::new(mgr);
        let (leave_result, continuation) = leave_group(&mgr);
        assert!(continuation, "a release callback was enqueued");
        let env = rx.recv().await.expect("release callback event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
        assert!(!leave_result.is_done());

        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_leave_completed_successfully(&leave_result);
        assert_eq!(
            mgr.state(),
            MemberState::PrepareLeaving,
            "the invalid-epoch response is otherwise ignored"
        );

        // The callback completing afterwards still moves the member on to
        // send its leave heartbeat.
        stuck_ack.send(Ok(())).unwrap();
        mgr.drive_pending_leave_callback().unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
    }

    /// A second leave after an invalid-epoch response took the leave future
    /// while the member is PREPARE_LEAVING waits for the ongoing leave.
    ///
    /// Deviation from Java 4.4 pinned here: `onHeartbeatSuccess` completes and
    /// clears `leaveGroupInProgress` but leaves the member PREPARE_LEAVING, so
    /// the second `AbstractMembershipManager.leaveGroup(boolean)` takes its
    /// "already leaving" branch and `leaveGroupInProgress.get()` throws
    /// `NoSuchElementException`. `ConsumerNetworkThread.processApplicationEvents`
    /// then fails the event with it and the second `unsubscribe()` throws a
    /// `KafkaException`. Rust instead registers a fresh leave future: the
    /// second leave does not fail and completes when the ongoing leave ends
    /// (here, on the response to the leave heartbeat).
    #[tokio::test]
    async fn second_leave_after_invalid_epoch_response_while_prepare_leaving_waits_for_ongoing_leave() {
        let (mgr, mut rx) = make(None, None, None);
        subscribe_with_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        let mgr = Arc::new(mgr);
        let (first_leave, continuation) = leave_group(&mgr);
        assert!(continuation, "a release callback was enqueued");
        let env = rx.recv().await.expect("release callback event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // The invalid-epoch response takes and completes the leave future;
        // the member stays PREPARE_LEAVING.
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_leave_completed_successfully(&first_leave);
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);

        // Second leave (a second `unsubscribe()`): Java fails it with
        // NoSuchElementException; Rust returns a fresh future.
        let second_leave = mgr
            .leave_group(0)
            .expect("Rust deviation from Java 4.4: the second leave does not fail");
        assert!(rx.try_recv().is_err(), "the second leave enqueues no callback");
        assert!(mgr.has_pending_leave_callback(), "the first leave's callback is still pending");
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
        assert!(
            !second_leave.is_done(),
            "the second leave gets a fresh future that waits for the ongoing leave"
        );

        // The ongoing leave goes on: the callback completes, the leave
        // heartbeat is sent, and its response completes the second leave.
        stuck_ack.send(Ok(())).unwrap();
        mgr.drive_pending_leave_callback().unwrap();
        assert_eq!(mgr.state(), MemberState::Leaving);
        assert!(!second_leave.is_done(), "still waiting for the leave heartbeat");
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(!second_leave.is_done(), "still waiting for the leave heartbeat's response");
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_send_leave_completed(&mgr, &second_leave);
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
        let (leave_result, _) = leave_group(&mgr);

        // Send leave request, transitioning to UNSUBSCRIBED state.
        mgr.abstract_mm.on_heartbeat_request_generated().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);

        // Receive a previous heartbeat response, which should be ignored.
        mgr.on_heartbeat_success(&heartbeat_response(mgr.member_id(), 1)).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(!leave_result.is_done());

        // Receive a leave heartbeat response, which should unblock the
        // consumer.
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert_leave_completed_successfully(&leave_result);

        // Consumer unblocks and updates subscription.
        mgr.abstract_mm.on_subscription_updated();
        mgr.abstract_mm.on_consumer_poll(mgr.join_group_epoch()).unwrap();

        // A late leave response no longer affects the rejoining member.
        mgr.on_heartbeat_success(&leave_response(mgr.member_id())).unwrap();

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
        subscribe_with_topics(&mgr, &["topic1"]);
        mgr.transition_to_joining().unwrap();
        let topic_id = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic_id)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);

        let mgr = Arc::new(mgr);
        let (leave_operation, continuation) = leave_group(&mgr);
        assert!(continuation, "a release callback was enqueued");
        let env = rx.recv().await.expect("revoked event");
        let stuck_ack = match env.event {
            BackgroundEvent::PartitionsRemoved { ack, .. } => ack,
            other => panic!("unexpected event: {other:?}"),
        };
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
        assert!(!leave_operation.is_done());

        // Fence while preparing to leave -> UNSUBSCRIBED, and the leave
        // completes right away, without waiting for the stuck callback.
        mgr.transition_to_fenced(0).unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
        assert!(
            leave_operation.is_done(),
            "Fenced member should complete the ongoing leave operation"
        );

        // Completing the callback afterwards sends no leave heartbeat;
        // remains UNSUBSCRIBED.
        stuck_ack.send(Ok(())).unwrap();
        mgr.drive_pending_leave_callback().unwrap();
        assert_eq!(mgr.state(), MemberState::Unsubscribed);
    }

    /// Translated from
    /// `ConsumerMembershipManagerTest#testUpdateStateFailsOnResponsesWithErrors`.
    /// A heartbeat response containing an error code must fail
    /// `on_heartbeat_success` with an illegal-argument error.
    #[test]
    fn update_state_fails_on_responses_with_errors() {
        use crate::ConsumerGroupHeartbeatResponseData;
        let (mgr, _rx) = make(None, None, None);
        mgr.transition_to_joining().unwrap();
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::UnknownMemberId.code();
        data.member_id = Some(mgr.member_id());
        data.member_epoch = 5;
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        let err = mgr.on_heartbeat_success(&resp).unwrap_err();
        // Error message content is part of the contract (DoD §3). Java formats
        // the `Errors` constant with `%s`, i.e. its enum name
        // (`org.apache.kafka.clients.consumer.internals.AbstractMembershipManager#throwIfUnexpectedError`).
        assert!(
            matches!(err, Error::LocalIllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert_eq!(
            "Unexpected error in Heartbeat response. Expected no error, but received: UNKNOWN_MEMBER_ID",
            err.message()
        );
        assert_eq!(
            MemberState::Joining,
            mgr.state(),
            "a rejected response leaves the state unchanged"
        );
    }

    /// `ConsumerMembershipManager.extractAssignment` (KAFKA-20681) builds each
    /// topic's partitions as a `TreeSet`: sorted and deduplicated whatever
    /// order the broker sent. An absent assignment is `Optional.empty()`.
    #[test]
    fn extract_assignment_sorts_and_deduplicates_partitions_like_java_tree_set() {
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::{Assignment, TopicPartitions};

        let mut data = ConsumerGroupHeartbeatResponseData::new();
        assert_eq!(
            None,
            ConsumerMembershipManager::extract_assignment(&ConsumerGroupHeartbeatResponse::new(data.clone()))
        );

        let topic_id = Uuid::random_uuid();
        let mut topic_partitions = TopicPartitions::new();
        topic_partitions.topic_id = topic_id;
        topic_partitions.partitions = vec![2, 0, 1, 1];
        data.assignment = Some(Assignment { topic_partitions: vec![topic_partitions], unknown_tagged_fields: vec![] });
        let assignment = ConsumerMembershipManager::extract_assignment(&ConsumerGroupHeartbeatResponse::new(data));
        assert_eq!(Some(HashMap::from([(topic_id, vec![0, 1, 2])])), assignment);
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
        use crate::ConsumerGroupHeartbeatResponseData;
        use crate::consumer_group_heartbeat_response_data::Assignment;

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
        mut rx: mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
        mgr: Arc<ConsumerMembershipManager>,
        topic_id: Uuid,
    ) -> mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope> {
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
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
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
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
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
        subscribe_with_topics(&mgr, &["topic1"]);
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
        assert_eq!(mgr.member_epoch(), ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH);
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
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
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
            BackgroundEvent::PartitionsRemoved { method_name, ack, partitions, .. } => {
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

        let (leave_result1, continuation) = leave_group(&mgr);
        assert!(!continuation);
        // SubscriptionState was unsubscribed.
        assert!(mgr.abstract_mm.subscriptions.lock().unwrap().subscription().is_empty());
        assert!(leave_result1.is_done());
        assert_eq!(mgr.state(), MemberState::Stale);
    }

    // ─── Background-loop wakes for the pre-rebalance auto-commit ───
    //
    // Java runs `maybeReconcile(true)` for an `AsyncPollEvent` on the network
    // thread inside `ConsumerNetworkThread.runOnce()`
    // (`ApplicationEventProcessor.process(AsyncPollEvent)`), before that
    // `runOnce` polls the request managers, so the pre-rebalance auto-commit
    // that `signalReconciliationStarted()` queues
    // (`CommitRequestManager.maybeAutoCommitSyncBeforeRebalance`) is sent in
    // the same iteration, and its `whenComplete` continuation runs inside
    // `client.poll` when the response is read. Rust runs the same
    // `reconcile(.., true)` on the bg task during the Phase 1 event drain,
    // before the commit manager's Phase 2 poll, so the queued request is sent
    // in the same `run_once` too; the wake on enqueue is defensive (it only
    // stores a permit). The commit result, by contrast, is resolved by a
    // spawned retry driver while the loop is parked on the `Notify` production
    // wires into `CommitRequestManager::set_completion_notify`, so a resolved
    // commit that does not poke it waits for the poll timeout
    // (`MAX_POLL_TIMEOUT_MS`, 5 s) or an unrelated wake.

    /// A member that owns `topic1-0` with a consumed offset and receives an
    /// empty assignment, with a real commit manager whose completion `Notify`
    /// is the returned one (as production wires `event_notify`).
    fn member_revoking_with_consumed_offset() -> (Arc<ConsumerMembershipManager>, Arc<tokio::sync::Notify>) {
        let (mgr, _rx) = make_with_commit_manager(false);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.subscribe_with_topics(HashSet::from(["topic1".to_string()]), None).unwrap();
        }
        mgr.transition_to_joining().unwrap();
        let topic1 = Uuid::random_uuid();
        seed_metadata(&mgr, &[("topic1", topic1)]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        {
            let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            subs.seek(&tp("topic1", 0), 100).unwrap();
        }
        receive_empty_assignment(&mgr);
        assert_eq!(mgr.state(), MemberState::Reconciling);
        let notify = Arc::new(tokio::sync::Notify::new());
        mgr.commit_request_manager
            .as_ref()
            .expect("commit manager present")
            .set_completion_notify(Arc::clone(&notify));
        (Arc::new(mgr), notify)
    }

    /// The pre-rebalance auto-commit queued by the AsyncPoll reconcile pokes
    /// the background loop. The AsyncPoll reconcile runs on the bg task before
    /// the commit manager's poll, so the request is sent in the same
    /// `run_once` anyway; this pins the defensive wake.
    #[tokio::test(flavor = "current_thread")]
    async fn auto_commit_before_rebalance_queued_from_async_poll_wakes_background_task() {
        let (mgr, notify) = member_revoking_with_consumed_offset();
        // The call `process_async_poll` makes: `reconcile(poll_time_ms, true)`.
        let m = Arc::clone(&mgr);
        tokio::spawn(async move { m.reconcile(0, true) })
            .await
            .expect("reconcile task")
            .expect("reconcile");
        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        assert_eq!(
            commit_mgr.unsent_offset_commits_len_for_test(),
            1,
            "the reconcile must queue the pre-rebalance auto-commit"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), notify.notified())
            .await
            .expect(
                "queuing the pre-rebalance OffsetCommit from the AsyncPoll reconcile did not poke the background \
             loop (the defensive wake on enqueue)",
            );
    }

    /// When the pre-rebalance auto-commit resolves, the background loop must
    /// be woken to resume the parked reconciliation (`drive_pending_reconcile`),
    /// as Java's `commitResult.whenComplete(...)` resumes it inside
    /// `client.poll` (`AbstractMembershipManager#maybeReconcile`).
    #[tokio::test(flavor = "current_thread")]
    async fn auto_commit_before_rebalance_completion_wakes_background_task() {
        let (mgr, notify) = member_revoking_with_consumed_offset();
        let m = Arc::clone(&mgr);
        tokio::spawn(async move { m.reconcile(0, true) })
            .await
            .expect("reconcile task")
            .expect("reconcile");
        assert!(reconciliation_in_progress(&mgr), "the reconcile parks on the commit");
        // Drop a wake left by the enqueue, if any, so only the completion's
        // own wake can satisfy the wait below.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), notify.notified()).await;
        let woken = notify.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();

        let commit_mgr = mgr.commit_request_manager.as_ref().expect("commit manager present");
        assert!(commit_mgr.complete_first_unsent_commit_for_test(HashMap::new()));
        tokio::time::timeout(std::time::Duration::from_secs(1), woken).await.expect(
            "the resolved pre-rebalance auto-commit did not wake the background loop, so the parked \
             reconciliation (onPartitionsRevoked enqueue) waits for the network poll timeout; Java resumes it \
             from the commit future's whenComplete inside client.poll",
        );
    }

    // ---------------------------------------------------------------
    // `AbstractMembershipManager#revokePartitions` (Milestone 18 P4b). Java
    // has no test of its own for the method; these pin each of its steps
    // against the Java body, on the real `SubscriptionState`.
    // ---------------------------------------------------------------

    /// A STABLE member (epoch 1, `NoopListener`) subscribed to `topic1` and
    /// owning `owned`, each at position 0 (so fetchable until marked). The
    /// pending-revocation marks of the join's reconciliation are cleared.
    async fn stable_member_owning(
        owned: &[TopicPartition],
    ) -> (
        Arc<ConsumerMembershipManager>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) {
        let (mgr, rx) = create_member_in_stable_state(None).await;
        subscribe_with_topics(&mgr, &["topic1"]);
        mock_owned_partitions(&mgr, owned);
        let mut subs = mgr.abstract_mm.subscriptions.lock().unwrap();
        for tp in owned {
            subs.seek(tp, 0).unwrap();
            assert!(subs.is_fetchable(tp), "precondition: {tp} fetchable");
        }
        subs.clear_pending_revocation_marks_for_test();
        drop(subs);
        (mgr, rx)
    }

    /// `revokePartitions` keeps only the partitions still assigned
    /// (`revokedPartitions.retainAll(subscriptions.assignedPartitions())`),
    /// marks them pending revocation, and passes them to
    /// `signalPartitionsRevoked` (`onPartitionsRevoked`).
    #[tokio::test]
    async fn revoke_partitions_revokes_only_still_assigned_partitions() {
        let (mgr, mut rx) = stable_member_owning(&[tp("topic1", 0)]).await;
        let to_revoke = HashSet::from([tp("topic1", 0), tp("topic1", 9)]);

        let (revoked, ack) = mgr.abstract_mm.revoke_partitions(&*mgr, &to_revoke, false, 0).unwrap();

        assert_eq!(revoked, vec![tp("topic1", 0)]);
        assert!(ack.is_some(), "a listener is registered: the callback ack is pending");
        assert_eq!(pending_revocation_marks(&mgr), vec![vec![tp("topic1", 0)]]);
        assert!(!mgr.abstract_mm.subscriptions.lock().unwrap().is_fetchable(&tp("topic1", 0)));
        match rx.recv().await.expect("onPartitionsRevoked event").event {
            BackgroundEvent::PartitionsRemoved { method_name, partitions, ack_through_application_events, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                assert_eq!(partitions, vec![tp("topic1", 0)]);
                assert!(!ack_through_application_events);
            },
            other => panic!("expected PartitionsRemoved(ON_PARTITIONS_REVOKED), got {other:?}"),
        }
    }

    /// When none of the partitions is still assigned, `revokePartitions`
    /// signals an empty revocation, which
    /// `ConsumerMembershipManager#invokeOnPartitionsRevokedCallback` turns
    /// into a completed future without a callback.
    #[tokio::test]
    async fn revoke_partitions_with_no_assigned_partition_triggers_no_callback() {
        let (mgr, mut rx) = stable_member_owning(&[tp("topic1", 0)]).await;
        let to_revoke = HashSet::from([tp("topic1", 9)]);

        let (revoked, ack) = mgr.abstract_mm.revoke_partitions(&*mgr, &to_revoke, false, 0).unwrap();

        assert!(revoked.is_empty());
        assert!(ack.is_none());
        assert_eq!(pending_revocation_marks(&mgr), vec![Vec::<TopicPartition>::new()]);
        assert!(mgr.abstract_mm.subscriptions.lock().unwrap().is_fetchable(&tp("topic1", 0)));
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
    }

    /// A member that hit a fatal error aborts the revocation with Java's
    /// `KafkaException` message, after marking the partitions pending
    /// revocation and without triggering the user callback.
    #[tokio::test]
    async fn revoke_partitions_aborts_without_callback_when_member_is_fatal() {
        let (mgr, mut rx) = stable_member_owning(&[tp("topic1", 0)]).await;
        let (member_id, member_epoch) = {
            let mut inner = mgr.abstract_mm.inner.lock().unwrap();
            // Java's test seam is a mock; the state is set directly here.
            inner.state = MemberState::Fatal;
            (inner.member_id.clone(), inner.member_epoch)
        };
        let to_revoke = HashSet::from([tp("topic1", 0)]);

        let err = mgr.abstract_mm.revoke_partitions(&*mgr, &to_revoke, false, 0).unwrap_err();

        assert!(
            matches!(err, Error::KafkaError(_)),
            "expected a bare KafkaException, got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!(
                "Member {member_id} with epoch {member_epoch} received a fatal error while waiting for a \
                 revocation commit to complete. Will abort revocation without triggering user callback."
            )
        );
        assert_eq!(pending_revocation_marks(&mgr), vec![vec![tp("topic1", 0)]]);
        assert!(matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
    }

    /// A leave while still in the group (epoch > 0) releases the assignment
    /// through `revokePartitions`
    /// (`ConsumerMembershipManager#invokeOnPartitionsRevokedOrLostToReleaseAssignment`):
    /// the partitions are marked pending revocation (fetching stops) before
    /// `onPartitionsRevoked` runs, and the callback ack returns through the
    /// application-event queue.
    #[tokio::test]
    async fn leave_group_in_group_marks_pending_revocation_before_on_partitions_revoked() {
        let (mgr, mut rx) = stable_member_owning(&[tp("topic1", 0), tp("topic1", 1)]).await;
        assert!(mgr.member_epoch() > 0);

        let leave_result = mgr.leave_group(0).unwrap();

        assert!(!leave_result.is_done());
        assert!(mgr.has_pending_leave_callback());
        assert_eq!(mgr.state(), MemberState::PrepareLeaving);
        assert_eq!(pending_revocation_marks(&mgr), vec![vec![tp("topic1", 0), tp("topic1", 1)]]);
        {
            let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            assert!(!subs.is_fetchable(&tp("topic1", 0)));
            assert!(!subs.is_fetchable(&tp("topic1", 1)));
            // Positions are kept, so the callback can still commit them.
            assert!(subs.position(&tp("topic1", 0)).unwrap().is_some());
        }
        match rx.recv().await.expect("onPartitionsRevoked event").event {
            BackgroundEvent::PartitionsRemoved { method_name, partitions, ack_through_application_events, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                let got: HashSet<TopicPartition> = partitions.into_iter().collect();
                assert_eq!(got, HashSet::from([tp("topic1", 0), tp("topic1", 1)]));
                assert!(ack_through_application_events);
            },
            other => panic!("expected PartitionsRemoved(ON_PARTITIONS_REVOKED), got {other:?}"),
        }
    }

    /// Java-faithful consequence of the leave marking the released partitions
    /// pending revocation: a partition with no position yet is no longer
    /// initialized while `onPartitionsRevoked` runs
    /// (`SubscriptionState$TopicPartitionState#shouldInitialize` is
    /// `fetchState == INITIALIZING && !pendingRevocation`), so a `position()`
    /// call from the callback finds nothing to fetch and waits until its
    /// timeout (`AsyncKafkaConsumer#position`), as in Java.
    #[tokio::test]
    async fn leave_group_in_group_stops_initializing_released_partitions() {
        let (mgr, mut rx) = create_member_in_stable_state(None).await;
        subscribe_with_topics(&mgr, &["topic1"]);
        mock_owned_partitions(&mgr, &[tp("topic1", 0)]);
        assert!(
            mgr.abstract_mm
                .subscriptions
                .lock()
                .unwrap()
                .initializing_partitions()
                .contains(&tp("topic1", 0)),
            "precondition: the owned partition has no position and needs initializing"
        );

        let _leave_result = mgr.leave_group(0).unwrap();

        assert!(mgr.has_pending_leave_callback(), "onPartitionsRevoked is pending");
        {
            let subs = mgr.abstract_mm.subscriptions.lock().unwrap();
            assert!(!subs.initializing_partitions().contains(&tp("topic1", 0)));
            assert!(subs.position(&tp("topic1", 0)).unwrap().is_none());
        }
        match rx.recv().await.expect("onPartitionsRevoked event").event {
            BackgroundEvent::PartitionsRemoved { method_name, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
            },
            other => panic!("expected PartitionsRemoved(ON_PARTITIONS_REVOKED), got {other:?}"),
        }
    }

    /// A leave after the member lost its epoch (epoch <= 0) releases the
    /// assignment through `onPartitionsLost`, not `revokePartitions`, so the
    /// leave itself marks nothing.
    #[tokio::test]
    async fn leave_group_out_of_group_invokes_on_partitions_lost_without_revocation_mark() {
        let (mgr, mut rx) = stable_member_owning(&[tp("topic1", 0)]).await;
        mgr.abstract_mm.inner.lock().unwrap().member_epoch = 0;

        let _leave_result = mgr.leave_group(0).unwrap();

        assert!(pending_revocation_marks(&mgr).is_empty());
        match rx.recv().await.expect("onPartitionsLost event").event {
            BackgroundEvent::PartitionsRemoved { method_name, ack_through_application_events, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsLost);
                assert!(ack_through_application_events);
            },
            other => panic!("expected PartitionsRemoved(ON_PARTITIONS_LOST), got {other:?}"),
        }
    }
}
