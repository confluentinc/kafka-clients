---
name: m13-phase4-rebalance-reshape-notes
description: Milestone-13 Phase 4 (AK 4.3.1) consumer rebalance/poll three-leg handshake reshape — event split, app-side apply, gotchas
metadata:
  type: project
---

Milestone-13 Phase 4 (agent 64) reshaped the KIP-848 rebalance handshake to
AK 4.3.1 (KAFKA-20106/20321/20332/20382/20426/20428). Landed green in 6 commits
on branch milestone-12-ak-4.3.1.

**Why:** AK 4.3.1 changed the single `ConsumerRebalanceListenerCallbackNeeded`
handshake into a three-leg flow so `consumer.assignment()` mutates only within
`poll()`.

**How to apply / key facts for future consumer work:**
- `BackgroundEvent`: `ConsumerRebalanceListenerCallbackNeeded` split into
  `PartitionsRemoved {method_name, partitions, ack}` (revoke/lost) and
  `PartitionsAssigned {assigned_partitions, added_partitions, ack}` (assign,
  **sent even with no listener**, no method_name). New completable app-event
  `ApplicationEvent::ApplyAssignment {handle, assigned_partitions, added_partitions}`.
- The assign path NO LONGER mutates SubscriptionState on the bg side.
  `continue_after_revoke` enqueues `PartitionsAssigned` + stores `AfterAssign`.
  The subscription mutation moved to `ConsumerMembershipManager::apply_assignment`
  (assign_from_subscribed_awaiting_callback + notify_assignment_change), invoked
  by the AEP `process_apply_assignment` when the app sends `ApplyAssignment`.
- App side `process_background_events_inner(skip_rebalance_callback, skip_assignment_events)`:
  PartitionsAssigned → `applyNewAssignment` (add_and_get ApplyAssignment, awaited)
  → on_partitions_assigned → ack. `skip_assignment_events` (unsubscribe/close,
  KAFKA-20428) completes PartitionsAssigned EXCEPTIONALLY ("Assignment event
  skipped because consumer is unsubscribing"), not recorded as first_error.
- `has_pending_reconciliation` = shared `Arc<AtomicBool>` written by the state
  notifier's `on_member_state_change` (RECONCILING→true). `collect_fetch` →
  `poll_for_fetches` gates on `wait_reconciliation_check` (waits on AsyncPollState
  notify racing the wakeup token). `AsyncPollState` gained
  is_reconciliation_check_complete/mark_reconciliation_check_complete + Notify.
- `AbstractMembershipManager::transition_to` fires on_member_state_change.
  Reconcile gate moved after computing revoked: `!can_commit && (auto_commit || !revoked.is_empty())`.
- Lost path `enqueue_release_callback` marks pending revocation BEFORE the
  callback (KAFKA-20321).
- Heartbeat `maximum_time_to_wait` returns i64::MAX when UNSUBSCRIBED (KAFKA-20426).
  GROUP_ID_NOT_FOUND while UNSUBSCRIBED → on_heartbeat_request_skipped (Handled).

**Test-harness gotchas:**
- Component tests of ConsumerMembershipManager MUST simulate the app applying
  the assignment: on receiving `PartitionsAssigned`, call
  `mgr.apply_assignment(&assigned_set, &added)` BEFORE acking (the shared
  `reconcile_and_complete_callback` / `expect_callback(mgr: Option<&...>)`
  helpers now dispatch on event type and do this). Mirrors Java's
  `performCallback` reshape + `processAssignmentEventNoCallback`.
- "No-listener" revoke tests now STILL get a `PartitionsAssigned` event
  (empty) — must receive/apply/ack it, not assert an empty channel.
- Delayed-reconciliation tests parked on assign: the subscription is NOT
  applied by the parked reconcile anymore, so a fence/fatal with empty owned
  fires NO lost callback (the old tests expected one).
- AKC unit tests (make_test_consumer_with_channels, no bg AEP) can't exercise
  the assign path (applyNewAssignment's add_and_get hangs); use the revoke path
  (PartitionsRemoved) for listener/ack/poke assertions, or a fake bg that
  completes the ApplyAssignment handle.
- `wait_reconciliation_check` deadline subtraction can overflow with i64::MIN;
  tests use deadline_ms=0 for the past-deadline case.

**Recorded skips (N/A for Rust):** WakeupTrigger fdece9c358 (rotating-token
model §11), Fetch.forPartition/ConsumerRecords tainted (no Rust Fetch/deprecated
ctor), import-move/javadoc files, GROUP_ID_NOT_FOUND-stable-is-fatal (Issue-9
deviation kept), Share/Streams/classic tests. See PLAN Phase 4 "Recorded skips".
