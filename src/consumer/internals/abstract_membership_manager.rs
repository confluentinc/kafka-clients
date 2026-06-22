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
//! Streams. Phase 8b translates only the Consumer subclass scope per
//! `consumer-threading.md` §20 — Share / Streams are out of scope.
//!
//! Composition over inheritance: this struct holds the shared
//! membership-state fields (group ID, member ID / epoch, current /
//! target assignments, listener registrations). The composing
//! [`super::consumer_membership_manager::ConsumerMembershipManager`]
//! wraps an `Arc<Mutex<MembershipInner>>` and provides Consumer-specific
//! configuration (group instance ID, server assignor, rack ID, commit
//! manager, ...).
//!
//! The state is shared via `Arc<Mutex<MembershipInner>>` between the
//! membership manager itself and
//! [`super::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager`]
//! — both touch the state from the bg task (Java models the same shape
//! with both managers holding plain references). Per
//! `consumer-threading.md` §16, the lock is `std::sync::Mutex` (short
//! critical sections, never crossing `.await`).
//!
//! # §31 — the critical contract
//!
//! [`reconcile`] enqueues `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
//! events with `oneshot::Sender<Result<(), KafkaError>>` and awaits the
//! matching receiver before advancing the membership state machine.
//! `MutexGuard`s on `MembershipInner` are scoped tightly so they are
//! ALWAYS dropped before any `.await`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::common::{KafkaError, TopicPartition, Uuid};
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;

use super::consumer_metadata::ConsumerMetadata;
use super::member_state::MemberState;
use super::member_state_listener::MemberStateListener;
use super::subscription_state::SubscriptionState;

/// A member's reconciled (or target) assignment, keyed by topic ID, with
/// a local epoch that bumps every time the value changes.
///
/// Java: `AbstractMembershipManager.LocalAssignment`.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub(crate) fn new(local_epoch: i64, partitions: HashMap<Uuid, Vec<i32>>) -> Result<Self, KafkaError> {
        if local_epoch == Self::NONE_EPOCH && !partitions.is_empty() {
            return Err(KafkaError::illegal_argument("Local epoch must be set if there are partitions"));
        }
        Ok(Self { local_epoch, partitions })
    }

    /// Returns `true` if this is the NONE sentinel.
    pub(crate) fn is_none(&self) -> bool {
        self.local_epoch == Self::NONE_EPOCH
    }

    /// Update with a new assignment. Returns `None` if the assignment
    /// is unchanged; otherwise a new `LocalAssignment` with epoch
    /// `local_epoch + 1`.
    ///
    /// Java: `Optional<LocalAssignment> updateWith(Map<...>)`.
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
    /// firing (`AbstractMembershipManager.java:791-806`). While this is
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
    /// Whether auto-commit is enabled (immutable for the manager's
    /// lifetime; stored here for state-machine queries).
    pub(crate) auto_commit_enabled: bool,
    /// Registered listeners notified on member-epoch / assignment
    /// changes. Phase 8 partial introduced this trait; Phase 8b uses
    /// it.
    pub(crate) state_updates_listeners: Vec<Arc<dyn MemberStateListener>>,
}

impl MembershipInner {
    /// Update the member state, setting it to the next state only if
    /// it is a valid transition.
    ///
    /// Java: `transitionTo(MemberState)`.
    pub(crate) fn transition_to(&mut self, next_state: MemberState) -> Result<(), KafkaError> {
        if self.state != next_state && !next_state.previous_valid_states().contains(&self.state) {
            return Err(KafkaError::illegal_state(format!(
                "Invalid state transition from {} to {}",
                self.state, next_state
            )));
        }
        log::info!(
            "Member {} with epoch {} transitioned from {} to {}.",
            self.member_id,
            self.member_epoch,
            self.state,
            next_state
        );
        self.state = next_state;
        Ok(())
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
pub(crate) struct AbstractMembershipManager {
    pub(crate) inner: Arc<Mutex<MembershipInner>>,
    pub(crate) subscriptions: Arc<Mutex<SubscriptionState>>,
    pub(crate) metadata: Arc<ConsumerMetadata>,
    pub(crate) background_event_handler: Arc<BackgroundEventHandler>,
}

impl AbstractMembershipManager {
    /// Constructor. Java: `AbstractMembershipManager(String, SubscriptionState,
    /// Metadata, Logger, Time, RebalanceMetricsManager, boolean)`.
    ///
    /// Metrics dropped. Time / logger via the `log` crate.
    pub(crate) fn new(
        group_id: impl Into<String>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
        background_event_handler: Arc<BackgroundEventHandler>,
        auto_commit_enabled: bool,
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
            subscription_updated: false,
            auto_commit_enabled,
            state_updates_listeners: Vec::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            subscriptions,
            metadata,
            background_event_handler,
        }
    }

    /// Java: `registerStateListener(MemberStateListener listener)`.
    pub(crate) fn register_state_listener(&self, listener: Arc<dyn MemberStateListener>) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.state_updates_listeners.push(listener);
    }

    /// Process a newly received target assignment. If the assignment
    /// differs from the current, transitions to `RECONCILING`; if it's
    /// the same as current, transitions to `STABLE` if we were
    /// `RECONCILING` or `JOINING`.
    ///
    /// Java: `processAssignmentReceived(Map<Uuid, SortedSet<Integer>>)`.
    pub(crate) fn process_assignment_received(&self, assignment: HashMap<Uuid, Vec<i32>>) -> Result<(), KafkaError> {
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
    pub(crate) fn find_resolvable_assignment_and_trigger_metadata_update(&self) -> Vec<(Uuid, String, Vec<i32>)> {
        let target_partitions = {
            let guard = match self.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.current_target_assignment.partitions.clone()
        };

        let topic_names = self.metadata.metadata_arc().topic_names();
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
            self.metadata.metadata_arc().request_update(true);
        }
        resolved
    }

    /// Mark reconciliation in progress. Java: `markReconciliationInProgress()`.
    pub(crate) fn mark_reconciliation_in_progress(&self) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.reconciliation_in_progress = true;
        guard.rejoined_while_reconciliation_in_progress = false;
    }

    /// Mark reconciliation completed. Java: `markReconciliationCompleted()`.
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
    pub(crate) fn on_consumer_poll(&self, join_group_epoch: i32) -> Result<(), KafkaError> {
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
            // Java. (`AbstractMembershipManager.java:491`).
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
    pub(crate) fn transition_to_joining(&self, join_group_epoch: i32) -> Result<(), KafkaError> {
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
    pub(crate) fn transition_to_sending_leave_group(
        &self,
        leave_group_epoch: i32,
        due_to_expired_poll_timer: bool,
    ) -> Result<(), KafkaError> {
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
    pub(crate) fn on_heartbeat_request_skipped(&self) -> Result<(), KafkaError> {
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
        }
        Ok(())
    }

    /// Java: `onHeartbeatRequestGenerated()`.
    pub(crate) fn on_heartbeat_request_generated(&self) -> Result<(), KafkaError> {
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
                    // Java transitions to STALE here via `transitionToStale()`,
                    // which also schedules the onPartitionsLost assignment
                    // release (`AbstractMembershipManager.java:791-806`). The
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
    /// (`AbstractMembershipManager.java:776-783`). Resets the
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
    /// [`crate::consumer::internals::consumer_membership_manager::ConsumerMembershipManager::join_group_epoch`]
    /// just before invoking this method.
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
                // `staleMemberAssignmentRelease` future
                // (`AbstractMembershipManager.java:781`); we record the
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

    /// Java: `transitionToFatal()`.
    pub(crate) fn transition_to_fatal(&self) -> Result<MemberState, KafkaError> {
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
        Ok(previous_state)
    }

    /// Java: `onHeartbeatFailure(boolean retriable)` shared bookkeeping.
    /// Returns `true` when there was a pending leave operation; caller
    /// (the Consumer subclass) should log a warning.
    pub(crate) fn on_heartbeat_failure(&self, _retriable: bool) -> bool {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.state == MemberState::Unsubscribed
    }

    /// Invokes a rebalance listener callback per §31.
    ///
    /// Mechanism:
    /// 1. Short-circuit if no [`ConsumerRebalanceListener`] is
    ///    registered on the subscription state. Java's
    ///    `invokeOnPartitions{Revoked,Assigned,Lost}Callback` all check
    ///    `subscriptions.rebalanceListener().isPresent()` and return a
    ///    completed future without enqueueing anything. Without this
    ///    guard the bg task would hang forever awaiting an ack that no
    ///    one will send (Phase 10's app-side drain only invokes the
    ///    listener when one exists). See `ConsumerMembershipManager.java:352-383`.
    /// 2. Create a fresh `oneshot::channel`.
    /// 3. Enqueue a [`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`]
    ///    carrying the sender half.
    /// 4. **Await** the receiver. The membership state machine does NOT
    ///    advance until this resolves.
    ///
    /// `MutexGuard`s are NEVER held across this `.await`.
    ///
    /// [`ConsumerRebalanceListener`]: crate::consumer::ConsumerRebalanceListener
    ///
    /// Java: `enqueueConsumerRebalanceListenerCallback(methodName, partitions)`
    /// (defined on `ConsumerMembershipManager`, but the contract is
    /// shared across all subclasses).
    pub(crate) async fn invoke_rebalance_callback(
        &self,
        method: ConsumerRebalanceListenerMethodName,
        partitions: Vec<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), KafkaError> {
        // Step 1: listener-presence short-circuit, matching Java's
        // `subscriptions.rebalanceListener().isPresent()` guard. Drop
        // the guard immediately to satisfy §16.
        let listener_present = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.rebalance_listener().is_some()
        };
        if !listener_present {
            return Ok(());
        }

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), KafkaError>>();
        let event =
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name: method, partitions, ack: ack_tx };
        // Enqueue. If the receiver is gone (consumer shutting down) we
        // surface the error like Java would on a closed queue.
        self.background_event_handler.add(event, current_time_ms)?;

        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                // Non-fatal: Java logs and continues. Java's
                // AbstractMembershipManager calls
                // `revocationResult.completeExceptionally(callbackError)`
                // for revoked / assigned errors — but the rebalance is
                // still treated as effectively complete (the manager
                // logs the failure and the broker will kick the member
                // out if the assignment never gets acked). Our
                // translation surfaces the error to the caller so they
                // can choose to log + continue (state machine still
                // advances).
                log::warn!("Rebalance listener callback returned error: {} (continuing rebalance)", e);
                Err(e)
            },
            Err(_recv_err) => {
                // App side dropped the receiver before responding —
                // treat as fatal listener failure.
                Err(KafkaError::illegal_state(
                    "Rebalance listener ack receiver dropped before completion",
                ))
            },
        }
    }
}

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

    /// Test-only no-op rebalance listener so the §31 short-circuit
    /// (added in COMMENTS.1.md fix #1) lets the handshake proceed.
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

    fn setup() -> (
        Arc<Mutex<SubscriptionState>>,
        Arc<ConsumerMetadata>,
        Arc<BackgroundEventHandler>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        // Register a no-op listener so the §31 short-circuit allows
        // the handshake to enqueue an event in these tests.
        subs.lock()
            .unwrap()
            .subscribe_topics(HashSet::new(), Some(Arc::new(NoopListener)))
            .unwrap();
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        (subs, metadata, beh, rx)
    }

    #[test]
    fn new_starts_unsubscribed_with_random_member_id() {
        let (subs, metadata, beh, _rx) = setup();
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);
        let inner = mgr.inner.lock().unwrap();
        assert_eq!(inner.state, MemberState::Unsubscribed);
        assert_eq!(inner.member_epoch, 0);
        assert!(!inner.member_id.is_empty());
        assert_eq!(inner.group_id, "g");
    }

    #[test]
    fn invalid_transition_returns_error() {
        let (subs, metadata, beh, _rx) = setup();
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);
        let mut inner = mgr.inner.lock().unwrap();
        // UNSUBSCRIBED → STABLE is invalid.
        let err = inner.transition_to(MemberState::Stable).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalState(_)));
    }

    #[test]
    fn valid_transition_succeeds() {
        let (subs, metadata, beh, _rx) = setup();
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);
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
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    /// §31 handshake regression: enqueueing a callback then sending
    /// `Ok(())` on the ack completes the future cleanly.
    #[tokio::test]
    async fn invoke_rebalance_callback_ack_ok() {
        let (subs, metadata, beh, mut rx) = setup();
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);

        // Spawn the bg-side invocation.
        let mgr_for_bg = Arc::new(mgr);
        let mgr_clone = mgr_for_bg.clone();
        let bg = tokio::spawn(async move {
            mgr_clone
                .invoke_rebalance_callback(
                    ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                    vec![TopicPartition::new("t".to_string(), 0)],
                    100,
                )
                .await
        });

        // App side: drain the event and send Ok on the ack.
        let env = rx.recv().await.expect("event must arrive");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name, ack, .. } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
                ack.send(Ok(())).unwrap();
            },
            _ => panic!("unexpected event"),
        }

        let result = bg.await.unwrap();
        assert!(result.is_ok());
    }

    /// §31 handshake: if the app side drops the receiver, the bg call
    /// returns an `IllegalState` error.
    #[tokio::test]
    async fn invoke_rebalance_callback_ack_dropped() {
        let (subs, metadata, beh, mut rx) = setup();
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);

        let mgr_for_bg = Arc::new(mgr);
        let mgr_clone = mgr_for_bg.clone();
        let bg = tokio::spawn(async move {
            mgr_clone
                .invoke_rebalance_callback(ConsumerRebalanceListenerMethodName::OnPartitionsAssigned, vec![], 0)
                .await
        });

        let env = rx.recv().await.expect("event must arrive");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { ack, .. } => {
                // Drop the sender — the bg should see an Err receiver result.
                drop(ack);
            },
            _ => panic!("unexpected event"),
        }

        let result = bg.await.unwrap();
        assert!(matches!(result, Err(KafkaError::IllegalState(_))));
    }

    /// §31 short-circuit (COMMENTS.1.md fix #1): when no rebalance
    /// listener is registered, `invoke_rebalance_callback` returns
    /// `Ok(())` immediately without enqueueing — mirrors Java's
    /// `subscriptions.rebalanceListener().isPresent()` guard.
    #[tokio::test]
    async fn invoke_rebalance_callback_no_listener_short_circuits() {
        // Build a subscription state WITHOUT a registered listener.
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);

        // Without a listener, the handshake must complete immediately
        // and emit no event — otherwise the bg task would hang in
        // Phase 10 when no listener is registered.
        let result = mgr
            .invoke_rebalance_callback(
                ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                vec![TopicPartition::new("t".to_string(), 0)],
                0,
            )
            .await;
        assert!(result.is_ok());
        assert!(rx.try_recv().is_err(), "no event should be enqueued when listener is absent");
    }

    /// §31 handshake: if the app-side reports an error, the bg call
    /// surfaces it (Java: non-fatal — bubble up to the caller who logs
    /// and decides).
    #[tokio::test]
    async fn invoke_rebalance_callback_app_returns_err() {
        let (subs, metadata, beh, mut rx) = setup();
        let mgr = AbstractMembershipManager::new("g", subs, metadata, beh, true);

        let mgr_for_bg = Arc::new(mgr);
        let mgr_clone = mgr_for_bg.clone();
        let bg = tokio::spawn(async move {
            mgr_clone
                .invoke_rebalance_callback(ConsumerRebalanceListenerMethodName::OnPartitionsLost, vec![], 0)
                .await
        });

        let env = rx.recv().await.expect("event must arrive");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { ack, .. } => {
                ack.send(Err(KafkaError::timeout("listener slow"))).unwrap();
            },
            _ => panic!("unexpected event"),
        }

        let result = bg.await.unwrap();
        assert!(matches!(result, Err(KafkaError::Timeout(_))));
    }
}
