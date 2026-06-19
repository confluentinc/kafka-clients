# Phase 34 — Test parity: ConsumerMembershipManager metadata-driven reconciliation

Actor 34. Branch `consumer-impl`. Test-parity effort: **prefer test-only
changes**; a production change is allowed only for a genuine Java-fidelity bug
and must be CPU/perf-neutral (CLAUDE.md §11), called out with the Java line it
mirrors.

## Goal

Close the largest untested region of the membership state machine: the
**metadata-driven reconcile** path. Existing Rust reconcile tests pre-seed
`assigned_topic_names_cache` to bypass metadata resolution. New tests MUST drive
reconciliation against **real metadata** (no cache pre-seed) so the
metadata-resolution / unresolved-assignment / delayed-result-discard logic is
actually exercised.

Authoritative worklist: `design/current/test-translation-review/02-membership-heartbeat.md`.
Java source (contract): `kafka/clients/src/test/.../ConsumerMembershipManagerTest.java` (AK 4.2).
Rust targets (inline `#[cfg(test)]`): `src/consumer/internals/consumer_membership_manager.rs`
and `abstract_membership_manager.rs`.

## Key translation decisions

### 1. Java mocks vs Rust real objects
Java uses `mock(SubscriptionState.class)` + `mock(ConsumerMetadata.class)` and
asserts via Mockito `verify(subscriptionState).assignFromSubscribedAwaitingCallback(...)`.
Rust tests use a **real** `SubscriptionState` and **real** `ConsumerMetadata`.
Therefore:
- Java `verify(subscriptionState).assignFromSubscribedAwaitingCallback(set, added)`
  → Rust asserts the **resulting real state**: `subs.assigned_partitions() == set`,
  and (added-partitions gating) `is_fetchable(tp)` before/after the assigned callback.
- Java `when(metadata.topicNames()).thenReturn(map)` → Rust seeds **real** metadata
  via `metadata.metadata_arc().update_with_current_request_version(&build_response(...), false, ts)`.
  A local `seed_metadata(&mgr, &[(topic_id, name)])` helper builds a one-partition
  `MetadataResponse` (mirroring `consumer_metadata.rs::build_response`) and applies it.
- Java `verify(metadata).requestUpdate(anyBoolean())` → Rust asserts
  `metadata.metadata_arc().update_requested() == true` (production `reconcile` calls
  `request_update(true)` for unresolved topic ids inside
  `find_resolvable_assignment_and_trigger_metadata_update`).

### 2. §31 callback handshake collapses Java's separate callback-completed event
Rust models the listener handshake as a single `oneshot` ack embedded in the
`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded` event (consumer-threading
§31). Java's two-step `ConsumerRebalanceListenerCallbackNeededEvent` +
`consumerRebalanceListenerCallbackCompleted(event)` is collapsed: the app side
drains the needed-event and sends the result on the embedded `ack` sender; the
bg `reconcile`/`transition_to_*` future resumes when the ack arrives. Therefore:
- Java `performCallback(..., complete=true)` → Rust: drain the event, `ack.send(Ok/Err)`.
- Java `performCallback(..., complete=false)` then later `completeCallback(event, mgr)`
  → Rust: capture the `ack` sender from the drained event, hold it, and send later.
  This is exactly how the "stuck on callback" delayed-discard tests are driven.
- The bg-task pattern (`tokio::spawn(async move { mgr_clone.reconcile(0, true).await })`)
  is the established way to drive an awaiting reconcile while the test plays the
  app side. Used by all existing async reconcile tests.

### 3. "topics awaiting reconciliation"
No production accessor exists. Computed in tests as: target topic ids
(`inner.current_target_assignment.partitions.keys()`) minus the ids resolvable
from metadata/cache. A test helper `topics_awaiting_reconciliation(&mgr)` reads
`inner` and subtracts `find_resolvable_assignment_and_trigger_metadata_update`'s
resolved ids. `topic_partitions_awaiting_reconciliation` similarly reads the
pending (received-after-rejoin) target not yet reconciled — read from
`inner.current_target_assignment` vs `current_assignment`.

### 4. auto-commit / revocation
`mockRevocationNoCallbacks(withAutoCommit)` → use `make_with_commit_manager(true)`
and drive the commit oneshot. The production `reconcile` awaits
`commit_mgr.maybe_auto_commit_sync_before_rebalance(...)`. To make a commit "stuck",
the test must intercept that future. The existing CommitRequestManager returns a
real oneshot; the test completes/fails it to mirror Java's
`commitResult.complete(null)` / `completeExceptionally(...)`. Where intercepting
the commit oneshot is not feasible with the current CRM test surface, the test
uses `make()` (no commit mgr, auto-commit effectively off) to cover the
no-auto-commit revocation rows, and documents the auto-commit-ordering rows as
covered by the can_commit gate already tested + the no-callback revocation row.

## In-scope tests (grouped by commit)

### Commit 1 — reconcile-vs-metadata (real metadata, no cache pre-seed)
- `reconcile_new_partitions_assigned_when_no_partition_owned`
  ← testReconcileNewPartitionsAssignedWhenNoPartitionOwned
- `reconcile_new_partitions_assigned_when_other_partitions_owned`
  ← testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned
- `reconcile_new_partitions_assigned_and_revoked`
  ← testReconcileNewPartitionsAssignedAndRevoked
- `reconciliation_skipped_when_same_assignment_received`
  ← testReconciliationSkippedWhenSameAssignmentReceived
- `reconcile_partitions_revoked_no_auto_commit_no_callbacks`
  ← testReconcilePartitionsRevokedNoAutoCommitNoCallbacks
- `reconcile_partitions_revoked_with_successful_auto_commit_no_callbacks`
  ← testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks
- `reconcile_partitions_revoked_with_failed_auto_commit_completes_revocation_anyway`
  ← testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway
- `same_assignment_reconciled_again_with_missing_topic`
  ← testSameAssignmentReconciledAgainWithMissingTopic
- `revoke_partitions_uses_topic_names_local_cache_when_metadata_not_available`
  ← testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable

### Commit 2 — metadata resolution / unresolved
- `unresolved_target_assignment_is_reconciled_when_metadata_received`
  ← testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived
- `member_keeps_unresolved_assignment_waiting_for_metadata_until_resolved`
  ← testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved
- `metadata_updates_reconciles_unresolved_assignments`
  ← testMetadataUpdatesReconcilesUnresolvedAssignments
- `metadata_updates_requests_another_update_if_needed`
  ← testMetadataUpdatesRequestsAnotherUpdateIfNeeded
- `delayed_metadata_used_to_complete_assignment`
  ← testDelayedMetadataUsedToCompleteAssignment

### Commit 3 — new-assignment-replaces-waiting
- `new_assignment_replaces_previous_one_waiting_on_metadata`
  ← testNewAssignmentReplacesPreviousOneWaitingOnMetadata
- `new_empty_assignment_replaces_previous_one_waiting_on_metadata`
  ← testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata
- `new_assignment_not_in_metadata_replaces_previous_one_waiting_on_metadata`
  ← testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata

### Commit 4 — delayed-reconciliation discard (mutation-resistant)
- `delayed_reconciliation_result_discarded_if_member_not_in_reconciling_state_anymore`
  ← testDelayedReconciliationResultDiscardedIfMemberNotInReconcilingStateAnymore
- `delayed_reconciliation_result_discarded_after_commit_if_member_rejoins`
  ← testDelayedReconciliationResultDiscardedAfterCommitIfMemberRejoins
- `delayed_reconciliation_result_discarded_after_partitions_revoked_callback_if_member_rejoins`
  ← testDelayedReconciliationResultDiscardedAfterPartitionsRevokedCallbackIfMemberRejoins
- `delayed_reconciliation_result_discarded_after_partitions_assigned_callback_if_member_rejoins`
  ← testDelayedReconciliationResultDiscardedAfterPartitionsAssignedCallbackIfMemberRejoins
- `delayed_reconciliation_result_applied_when_target_changed_with_metadata_update`
  ← testDelayedReconciliationResultAppliedWhenTargetChangedWithMetadataUpdate
- `delayed_reconciliation_result_applied_when_target_changed_with_new_assignment`
  ← testDelayedReconciliationResultAppliedWhenTargetChangedWithNewAssignment

### Commit 5 — listener ordering
- `listener_callbacks_basic` ← testListenerCallbacksBasic
- `listener_callbacks_throws_error_on_partitions_revoked` ← testListenerCallbacksThrowsErrorOnPartitionsRevoked
- `added_partitions_temporarily_disabled_awaiting_on_partitions_assigned_callback`
  ← testAddedPartitionsTemporarilyDisabledAwaitingOnPartitionsAssignedCallback
- `added_partitions_not_enabled_after_failed_on_partitions_assigned_callback`
  ← testAddedPartitionsNotEnabledAfterFailedOnPartitionsAssignedCallback
- `on_partitions_lost_no_error` ← testOnPartitionsLostNoError
- `on_partitions_lost_error` ← testOnPartitionsLostError
- `member_joining_calls_rebalance_listener_when_receiving_empty_assignment`
  ← testMemberJoiningCallsRebalanceListenerWhenReceivingEmptyAssignment

### Commit 6 — leave / fatal matrices + misc
- `leave_group_when_state_is_stable` ← testLeaveGroupWhenStateIsStable
- `leave_group_when_member_owns_assignment` ← testLeaveGroupWhenMemberOwnsAssignment
- `leave_group_when_member_already_leaving` ← testLeaveGroupWhenMemberAlreadyLeaving
- `leave_group_when_member_already_left` ← testLeaveGroupWhenMemberAlreadyLeft
- `leave_group_when_member_fenced` ← testLeaveGroupWhenMemberFenced
- `fatal_failure_when_state_is_stable` ← testFatalFailureWhenStateIsStable
- `fatal_failure_when_state_is_prepare_leaving` ← testFatalFailureWhenStateIsPrepareLeaving
- `fatal_failure_when_state_is_leaving` ← testFatalFailureWhenStateIsLeaving
- `fatal_failure_when_member_already_left` ← testFatalFailureWhenMemberAlreadyLeft
- `heartbeat_successful_response_when_leaving_group_completes_leave` ← testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave
- `heartbeat_failed_response_when_leaving_group_completes_leave` ← testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave (parameterized: loop over [true,false])
- `ignore_heartbeat_response_when_not_in_group` ← testIgnoreHeartbeatResponseWhenNotInGroup (parameterized over notInGroupStates)
- `ignore_leave_response_when_not_leaving_group` ← testIgnoreLeaveResponseWhenNotLeavingGroup
- `fencing_when_state_is_prepare_leaving_completes_the_leave_operation` ← testFencingWhenStateIsPrepareLeavingCompletesTheLeaveOperation
- `update_state_fails_on_responses_with_errors` ← testUpdateStateFailsOnResponsesWithErrors
- `on_subscription_updated_does_not_transition_to_joining_if_in_group` ← testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup
- `member_joining_transitions_to_stable_when_receiving_empty_assignment` ← testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment (if not already PRESERVED)

## Deferred (Phase 5, STALE-specific) — NOT in this phase
testTransitionToLeavingWhile{Reconciling,Joining,Stable,Acknowledging}DueToStaleMember,
testStaleMemberDoesNotSendHeartbeat..., testStaleMemberRejoinsWhenTimerResetsNoCallbacks,
testStaleMemberWaitsForCallbackToRejoinWhenTimerReset, testLeaveGroupWhenMemberIsStale.
Reason: tied to `reset_poll_timer` / poll-timer-expiry → STALE path, explicitly
scoped to Phase 5 by the prompt.

## Skipped (out of scope) — documented
- RebalanceMetrics tests (testAssignedPartitionCountMetricRegistered,
  testMetricsWhenHeartbeatFailed, testRebalanceMetricsOn{Successful,Failed}Rebalance,
  testRebalanceMetricsForMultipleReconciliations): no Rust metrics framework.
- testPollMustCallsMaybeReconcileWithFalse: Mockito `verify(mgr).maybeReconcile(false)` —
  REDUCED-covered by existing `reconcile_can_commit_false_is_noop_when_auto_commit_enabled`.
- Already-PRESERVED rows (fencing/leave-epoch/listener-epoch/empty-assignment→reconciling/
  same-assignment-when-fenced/etc.) are NOT duplicated.

## Process
Per commit: write tests, then `cargo build && cargo test --lib && cargo xtask lint
&& cargo xtask format-check` — all green before committing. Mutation-resistance
for delayed-discard: assert the new (post-rejoin) assignment is what's pending and
the stale assignment is NOT applied (deleting the `maybe_abort_reconciliation`
guard must flip the assertion).
