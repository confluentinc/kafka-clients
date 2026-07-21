---
name: review-m9-phase4-patterns
description: M11 Phase 4 share membership/heartbeat/metadata — duplicated-reconcile test-gap heuristic; "shared pipeline" deferral rationale audit
metadata:
  type: project
---

M11 Phase 4 (`44b8d40`) = ShareMembershipManager, ShareHeartbeatRequestManager,
ShareConsumerMetadata (+ tests). Reviewed substantially clean; ONE
Missing-Requirement finding, no correctness bugs. All 34 new tests pass.

**THE finding — "shared pipeline" deferral rationale is false because Rust
duplicates the pipeline.** The Actor deferred the reconcile/revocation test
families (`testReconcileNewPartitionsAssignedAndRevoked`,
`...WhenOtherPartitionsOwned`, `testReconciliationSkippedWhenSameAssignmentReceived`)
claiming they are "behaviorally identical to ConsumerMembershipManager reconcile
tests since the reconcile pipeline is shared." But **there is NO `reconcile` in
`abstract_membership_manager.rs`** (grep confirms) — the ~170-line reconcile is
hand-DUPLICATED into BOTH `consumer_membership_manager.rs` AND
`share_membership_manager.rs` (Rust has no inheritance + reconcile is async +
uses subclass epochs). So the share copy's revoked/added set-difference,
`mark_pending_revocation`, `onPartitionsRevoked` await, and same-assignment
short-circuit have ZERO share-module coverage — the one reconcile test starts
from an empty owned set so `revoked` is always empty. **Heuristic: when an Actor
defers tests citing "shared/covered elsewhere", grep the abstract layer for the
method. If it's duplicated per-subclass, the cousin's tests do NOT cover this
copy — demand translation or a corrected rationale.** (Recurring M11 pattern:
Phase 1/3 also had "weaker/deferred tests" with shaky justifications.)

**Faithfulness confirmations that saved false positives:**
- **auto-commit-before-rebalance absence CONFIRMED correct**: Java
  `AbstractMembershipManager` ctor's last bool param IS `autoCommitEnabled`;
  ShareMembershipManager passes `false`. Auto-commit is a CONSUMER-only override
  of `signalReconciliationStarted` (`maybeAutoCommitSyncBeforeRebalance`) — share
  does not override it. Rust reconcile correctly omits the `can_commit` gate +
  flush steps. Don't flag the omission.
- **`on_heartbeat_success` drops `maybeCompleteLeaveInProgress` — NOT a share
  regression**: the consumer path drops it too (grep: no
  `maybe_complete_leave_in_progress` anywhere in tree). Leave-future completion
  is a Phase-5/6 close-handshake wiring item for BOTH managers. LEAVING→UNSUBSCRIBED
  is driven by `on_heartbeat_request_generated` (tested), not the response.
- **UNSUPPORTED_VERSION share messages are reachable, not dead code**: abstract
  `classify_response_error` has no UnsupportedVersion arm → falls to
  `_ => DelegateToSpecific` → `handle_specific_exception_in_response` applies
  SHARE_PROTOCOL_NOT_SUPPORTED_MSG (broker) / `handle_specific_failure` applies
  SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG (client). Both tested. Always trace the
  classify→delegate chain before flagging a "custom message never applied".
- **tokio::spawn forwarder is fine**: per-heartbeat (not per-record) — matches the
  accepted `consumer_heartbeat_request_manager.rs` precedent (PendingHeartbeatCompletion
  / PendingMembershipTransition). Not a §11 hot-path violation.
- member epoch is correctly `i32` (Java `memberEpoch` is `int`, not `long`) — the
  CLAUDE i64 rule is about `long` fields; don't misapply it to epochs.
- subscribedTopicNames sent SORTED in Rust vs Java `subscription()` iteration order
  — broker is order-insensitive, diff is order-independent both sides. Non-finding.

**Verification method that worked:** diff the share manager method-by-method
against the consumer cousin (poll, on_response, on_failure, reconcile,
transition_to_fenced/fatal/stale) rather than only vs Java — the cousins already
duplicate the Java abstract, so cousin-parity + one Java spot-check on the
share-specific delta (epochs 0/-1, rackId, SHARE_PROTOCOL msgs, no-auto-commit)
is faster and catches copy drift.
