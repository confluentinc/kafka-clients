# RESOLVED (fixup 9737fdb) — Milestone 9 Phase 4 review

Resolution: the reconcile pipeline is a hand-duplicated copy (not shared —
AbstractMembershipManager has no reconcile). The inaccurate "shared pipeline"
rationale was corrected, and three ShareMembershipManagerTest cases were
translated against the share reconcile copy, exercising the previously-untested
revoked diff, mark_pending_revocation, onPartitionsRevoked-before-assigned
ordering, other-partitions-owned add-only path, and the same-assignment
short-circuit: reconcile_new_partitions_assigned_and_revoked,
reconcile_new_partitions_assigned_when_other_partitions_owned,
reconciliation_skipped_when_same_assignment_received. Lib tests 2165 -> 2168.

# Critic 1 — Milestone 9 Phase 4 review (share membership + heartbeat + metadata)

Commit `44b8d40`. Files: `share_membership_manager.rs`,
`share_heartbeat_request_manager.rs`, `share_consumer_metadata.rs`.
Java refs under `clients/.../consumer/internals/`.

**Phase 4 is substantially clean — one test-coverage finding (Missing
Requirement), no correctness bugs.** The three managers faithfully mirror the
consumer cousins (`ConsumerMembershipManager`, `ConsumerHeartbeatRequestManager`,
`ConsumerMetadata`) and the Java sources. All 34 new tests pass.

## Issue: share reconcile revocation / other-partitions-owned / same-assignment paths are untranslated and the deferral rationale is factually wrong
- **File**: `src/consumer/internals/share_membership_manager.rs:459-632` (reconcile), doc-comment `:800-804`
- **Severity**: Missing Requirement (test coverage)
- **Java Reference**: `ShareMembershipManagerTest.java:1044` (`testReconcileNewPartitionsAssignedAndRevoked`), `:988` (`testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`), `:1009` (`testReconciliationSkippedWhenSameAssignmentReceived`)
- **Description**: The Actor's doc comment justifies skipping the metadata /
  reconcile test families by asserting they are "behaviorally identical to the
  `ConsumerMembershipManager` reconcile tests (Phase 34) **since the reconcile
  pipeline is shared**." That premise is false at the Rust level. There is **no**
  `reconcile` (or `maybe_reconcile` / `revoke_and_assign`) method in
  `abstract_membership_manager.rs` (verified by grep) — the ~170-line reconcile
  pipeline is **duplicated by hand** into both `consumer_membership_manager.rs`
  and `share_membership_manager.rs`. The share copy carries its own
  revoked-vs-added set-difference (`:519-520`), `mark_pending_revocation`
  (`:534`), the `onPartitionsRevoked` await (`:541-556`), and the
  same-assignment short-circuit (`:492-501`). None of those branches is
  exercised by any share test: the only reconcile test
  (`reconcile_new_partitions_assigned_when_no_partition_owned`) starts from an
  empty owned set, so `revoked` is always empty and the short-circuit never
  fires. A transposition of `added`/`revoked`, a wrong argument to
  `mark_pending_revocation`, or a broken short-circuit in the share copy would
  compile and pass the current suite.
- **Expected**: Either translate `testReconcileNewPartitionsAssignedAndRevoked`,
  `testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`, and
  `testReconciliationSkippedWhenSameAssignmentReceived` against the share
  manager (they need an owned assignment first, then a differing target), OR
  correct the doc-comment rationale to state that the reconcile pipeline is a
  *duplicated copy* and explicitly acknowledge these branches are untested in
  the share module. Given the code is copied, translating at least the
  revoked-path test is the safer choice.
- **Actual**: Deferred with a "shared pipeline" rationale that does not hold;
  revoked / owned-partitions / short-circuit reconcile branches have zero
  coverage in the share module.

## Non-findings verified (recorded to save the next reviewer time)
- **State machine parity**: `on_heartbeat_success` matches Java line-for-line —
  LEAVING / UNSUBSCRIBED+epoch<0 / `is_not_in_group` / epoch<0 early-outs, then
  `update_member_epoch` + `can_handle_new_assignment` gate + `process_assignment_received`.
  The captured `state` local (pre-epoch-update) is used for `can_handle_new_assignment`
  exactly as Java. Empty-assignment (`Some(empty_map)`) → RECONCILING is correct.
- **`transition_to_fenced` / `transition_to_fatal` / `transition_to_stale`**
  are faithful copies of the consumer cousins (which duplicate
  `AbstractMembershipManager.transitionTo{Fenced,Fatal,Stale}`). `resetEpoch()`
  → `update_member_epoch(join_group_epoch()=0)`; the empty-partitions guard on
  `onPartitionsLost` is behaviourally equivalent to Java's unconditional
  `signalPartitionsLost(emptySet)`.
- **No auto-commit-before-rebalance step** — CONFIRMED against Java. Java
  `AbstractMembershipManager` ctor's last param is `autoCommitEnabled`; share
  passes `false` (`ShareMembershipManager.java:111`), and share does NOT override
  `signalReconciliationStarted` (that override, with `maybeAutoCommitSyncBeforeRebalance`,
  is consumer-only). The Rust reconcile correctly omits steps 5 (`can_commit` gate)
  and 8a (auto-commit flush).
- **`is_leaving_group()`** correctly uses the base (`PREPARE_LEAVING | LEAVING`),
  no static-member / remain-in-group override — matches Java (share has no
  `groupInstanceId` / `leaveGroupOperation`).
- **Heartbeat build/response**: `build_request_data` field-diff matches Java —
  groupId/memberId/memberEpoch always sent, rackId once (`None` re-set-to-`None`
  is harmless, as Java `setRackId(null)`), subscribedTopicNames on JOINING or
  change. (The Rust stores/sends the *sorted* topic list where Java sends
  `subscription()` iteration order; broker is order-insensitive and the diff is
  order-independent in both — not a bug.) `poll` mirrors
  `AbstractHeartbeatRequestManager.poll` order-for-order (skip → poll-timer-expiry
  → heartbeat-now); `on_response`/`on_failure` match the consumer variant.
- **`UNSUPPORTED_VERSION` → share messages are reachable, not dead code**:
  `classify_response_error` returns `DelegateToSpecific` for `UnsupportedVersion`
  (falls to `_` arm), so `handle_specific_exception_in_response` applies
  `SHARE_PROTOCOL_NOT_SUPPORTED_MSG` (broker-side) and `handle_specific_failure`
  applies `SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG` (client-side). Both tested.
- **`tokio::spawn` forwarder + mpsc channel-back**: identical to
  `consumer_heartbeat_request_manager.rs` (`PendingHeartbeatCompletion` /
  `PendingMembershipTransition`). The per-heartbeat spawn is NOT a per-record hot
  path — it matches the accepted consumer precedent (CLAUDE.md §11 /
  consumer-threading.md §10). No new spawning introduced.
- **§16**: `SubscriptionState` is `Arc<std::sync::Mutex>`; guards are dropped
  before every `.await` (reconcile drops `guard`/`subs` before each
  `invoke_rebalance_callback`; `on_heartbeat_success` drops before
  `process_assignment_received`). No guard held across an await.
- **Rule compliance**: internals `pub(crate)`; member epoch is `i32` (correct —
  Java `memberEpoch` is `int`, not `long`); Apache-2.0 (Confluent Inc) headers;
  one class per file; parent-module re-export imports; `// metrics: deferred to
  KIP-714` at omitted sites.
- **`ShareConsumerMetadata`** faithfully mirrors Java (`newMetadataRequestBuilder`
  scoped to `metadataTopics()`, `retainTopic` = `needsMetadata`,
  `allowAutoTopicCreation`), composing `Metadata` + `MetadataOverrides` like
  `ConsumerMetadata`. Java has no `ShareConsumerMetadataTest`; the 3 hand-written
  tests are appropriate.

## Deferred-test assessment (all defensible EXCEPT the reconcile family above)
- **Mockito-spy-on-internals** (`verify(...never()).markReconciliationInProgress()`,
  etc.): genuinely untranslatable (no mocking of a concrete struct) — accepted
  consumer precedent. BUT note it overlaps the reconcile-coverage gap above:
  spy-based Java tests are the ones that exercised the revoked path.
- **Leave-future completion** (`maybeCompleteLeaveInProgress` on the
  `CompletableFuture` leave result): deferred. VERIFIED not a share regression —
  the consumer path also drops it (grep finds no `maybe_complete_leave_in_progress`
  anywhere), and the LEAVING→UNSUBSCRIBED transition is driven by
  `on_heartbeat_request_generated` (tested), not the response. close()/unsubscribe()
  correctness on the leave *future* is a Phase-5/6 wiring item for BOTH managers,
  not Phase 4.
- **`onComplete`/`onFailure` round-trip**: the classification helpers
  (`on_response`, `on_failure`, `handle_specific_*`) are unit-tested directly; the
  full spawned-forwarder round-trip is deferred to Phase 5/6 bg-loop integration
  (same as consumer Phase-12.5). Does not mask a broken path — the response body
  extraction (`ConcreteResponse::ShareGroupHeartbeat`) and error routing were
  inspected and match the consumer forwarder.
- **KIP-714 metrics**: out of scope.

## Verdict
Phase 4 is clean to proceed **with one Missing-Requirement finding** (share
reconcile revoked/owned/short-circuit branches untested, deferral rationale
factually wrong). No correctness bugs, no rule violations, no false semantics.
The finding is a test-coverage / documentation-accuracy item, not a runtime
defect — the reconcile code itself reads as a correct copy of the reviewed
consumer pipeline.
