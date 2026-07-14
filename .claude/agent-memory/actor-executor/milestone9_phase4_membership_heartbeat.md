---
name: milestone9-phase4-membership-heartbeat
description: KIP-932 Phase 4 — ShareMembershipManager/ShareHeartbeatRequestManager/ShareConsumerMetadata reuse of KIP-848 abstractions
metadata:
  type: project
---

Milestone 9 Phase 4 translated the share-group membership + heartbeat +
metadata managers on branch `milestone9-share-consumer`. Key decisions,
useful for later phases and cousins:

**Reuse without touching the abstractions** — `AbstractMembershipManager`
and `AbstractHeartbeatRequestManager` (in `abstract_*.rs`) are already
response-type-agnostic in their public API, so Share composes them
unchanged, exactly like the Consumer variants. No edits to the shared
abstractions were needed.

**BUT the `reconcile` pipeline is NOT in the abstraction** — despite Java
placing `maybeReconcile` in `AbstractMembershipManager`, the Rust port
hand-DUPLICATES the ~170-line reconcile into BOTH
`consumer_membership_manager.rs` and `share_membership_manager.rs`
(`abstract_membership_manager.rs` has no `reconcile`). So the share copy's
revoked/added diff + short-circuit MUST be tested independently — don't
write "shared pipeline" in a deferral rationale (Critic round-1 caught
this). To assert exact revoked/added callback partitions you need a
registered rebalance listener; `subscribe_to_share_group` takes none, so
tests use `subscribe_topics(.., Some(listener))` (subscription type is
immaterial to reconcile). Covered by
`reconcile_new_partitions_assigned_and_revoked` +
`_when_other_partitions_owned` + `reconciliation_skipped_when_same_assignment_received`. `AbstractMembershipManager.metadata` is
`Arc<ConsumerMetadata>` and the Java `ShareMembershipManagerTest` itself
uses `mock(ConsumerMetadata.class)` — so ShareMembershipManager passes
`Arc<ConsumerMetadata>` (NOT ShareConsumerMetadata). ShareConsumerMetadata
is only for production wiring (Phase 5/6).

**Share is simpler than Consumer** — no groupInstanceId/serverAssignor/
commit_request_manager/leaveGroupOperation. `auto_commit_enabled=false`
(share groups acknowledge, don't commit). join epoch 0 / leave epoch -1
(no static -2). `is_leaving_group()` = base impl (PrepareLeaving|Leaving).
`reconcile(now)` mirrors ConsumerMembershipManager's reconcile MINUS the
auto-commit-before-rebalance step and the `can_commit` gate (both dead for
share since auto_commit is always false).

**§31 short-circuit makes share reconcile synchronous in tests** — the
Java tests mock `subscriptionState.rebalanceListener()` to
`Optional.empty()`. `subscribe_to_share_group(topics)` takes NO listener,
so `invoke_rebalance_callback` short-circuits (returns Ok, no event) and
`reconcile(now).await` completes synchronously — no bg-task spawn+ack dance
needed (unlike ConsumerMembershipManager tests which register a NoopListener
and drive the callback). This is the key test-simplification insight.

**Heartbeat structure copied verbatim from ConsumerHeartbeatRequestManager**
— PendingHeartbeatCompletion (spawned forwarder + mpsc channel-back) +
PendingMembershipTransition (async fence/fatal/stale side-channel). Share
HeartbeatState field-diff carries only groupId/memberId/memberEpoch/rackId
(sent-once)/subscribedTopicNames (diffed). rackId sentinel: `Option<String>`
where None="not yet sent" — mirrors Java's `sentFields.rackId==null`
(so a null rackId re-sets null each build, harmless). Error classification
adds only the two UNSUPPORTED_VERSION arms (SHARE_PROTOCOL_NOT_SUPPORTED_MSG
broker-side / SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG client-side); no
FENCED/UNRELEASED_INSTANCE_ID/GROUP_ID_NOT_FOUND special-casing (they hit
the abstract default→fatal arm, which matches Java's errorProvider fatal set).

**Tests**: 17 membership + 13 heartbeat + 3 metadata = 33 new (all pass;
lib total 2165). Deferred with rationale (per Consumer precedent):
Mockito-spy-on-internals, leave-future-completion (CompletableFuture leave
result — later phase), KIP-714 metrics, and the handler.onComplete/onFailure
response-delivery round-trips (Phase 5/6 bg-loop).

**Deferred to Phase 5/6**: RequestManagers share slots (not needed to
compile/test this phase — managers are self-contained) and the full
bg-loop integration that drives `reconcile`/`transition_to_*`.

No ShareConsumerMetadataTest.java exists (added 3 Rust tests anyway for the
override behavior). ShareGroupHeartbeat wrappers/data from Phase 1 reused
directly (`crate::common::requests::share_group_heartbeat_{request,response}`,
`crate::share_group_heartbeat_{request,response}_data`).
