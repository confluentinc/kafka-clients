# Test-translation review — Membership & Heartbeat (KIP-848)

Scope: ConsumerMembershipManagerTest, ConsumerHeartbeatRequestManagerTest,
HeartbeatRequestStateTest, HeartbeatTest. Read-only fidelity review of
Java→Rust test translation.

## Key findings (most material first)

1. **HeartbeatTest (7 tests) is OUT_OF_SCOPE — correctly skipped.** The
   `Heartbeat` class is used only by `AbstractCoordinator.java` (classic
   protocol), which `consumer-threading.md` §20 explicitly excludes. (Note:
   §20's "in scope" list literally names `Heartbeat`, but the class is a
   classic-coordinator dependency with no KIP-848 callsite — the §20 list is
   slightly over-inclusive here. The skip is behaviorally correct; the
   distinct `HeartbeatRequestState` is what KIP-848 uses, and it is fully
   translated.) No Rust `Heartbeat` struct exists. Not a gap.

2. **HeartbeatRequestStateTest: 5/5 PRESERVED, faithful.** All five tests
   map 1:1 with equivalent timing assertions. The MockTime→explicit
   `current_time_ms` substitution is sound and the per-test docstrings note
   where Java's `Timer.updateAndReset` self-update is mirrored.

3. **ConsumerMembershipManagerTest: ~30 of 84 behaviorally covered; ~54
   not translated, with a documented rationale block.** The −46 raw delta is
   NOT a clean fold — it is a deliberate, documented deferral. The state
   machine *core* (FENCED from every prior state, UNSUBSCRIBED tail, JOINING,
   leave-group epochs static/dynamic/force, fatal transitions, epoch reset on
   fence, listener epoch-change notifications, empty-assignment→RECONCILING,
   same-assignment-after-fence) IS preserved. What is genuinely thin:
   - **Reconciliation against real metadata** (`testReconcileNewPartitions*`,
     `testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived`,
     `testMetadataUpdatesReconciles*`, `testDelayedReconciliationResult*` x4,
     `testNewAssignment*ReplacesPreviousOneWaitingOnMetadata` x3): the Rust
     reconcile tests pre-seed `assigned_topic_names_cache` to bypass metadata
     resolution, so the **metadata-resolution + unresolved-assignment +
     delayed-result-discard** logic is NOT exercised. This is the single
     largest behavioral gap (the membership manager IS a reconciliation state
     machine and the metadata-driven reconcile path is its heart). Deferred to
     "Phase 10 wiring" per the docstring — legitimate but should not stay
     deferred indefinitely.
   - **Stale-member path entirely untranslated** (`testTransitionToLeavingWhile*DueToStaleMember`
     x4, `testStaleMemberDoesNotSendHeartbeat...`, `testStaleMemberRejoins...`,
     `testStaleMemberWaitsForCallback...`): 7 tests covering the STALE state.
     `MemberState::Stale` and its transitions exist in source, but no
     membership test drives the poll-timer-expiry → STALE path. Tied to
     `reset_poll_timer`/`maybe_rejoin_stale_member` deferral (Phase 10).
   - **Listener-callback reconcile ordering** (`testListenerCallbacksBasic`,
     `testListenerCallbacksThrowsErrorOn{Revoked,Assigned}`,
     `testAddedPartitionsTemporarilyDisabled...`,
     `testOnPartitionsLost{NoError,Error}`): partially covered — the §31
     handshake (enqueue + oneshot ack + error-propagation + no-listener
     short-circuit) is well tested in `abstract_membership_manager.rs`, and
     `reconcile_propagates_assigned_listener_error` covers the assigned-error
     case. But revoked-callback-error, partitions-temporarily-disabled, and
     onPartitionsLost specifics are not.
   - **All RebalanceMetrics tests** (`testAssignedPartitionCountMetricRegistered`,
     `testMetricsWhenHeartbeatFailed`, `testRebalanceMetricsOn{Successful,Failed}Rebalance`,
     `testRebalanceMetricsForMultipleReconciliations`): 5 tests skipped —
     no Rust metrics framework. Acceptable (metrics deliberately dropped),
     consistent with other phases.

4. **ConsumerHeartbeatRequestManagerTest: ~17 of 31 behaviorally covered.**
   The in-file deferral docstring claims "Translated 12/31" but that count is
   STALE — Phase-12.5 added 6 more substantive tests (response routing,
   error-response field reset, fenced→transition, fatal→transition,
   unknown-code→fatal-fallback, transport-failure path) that DO cover large
   parts of the docstring's "deferred" list (`testNetworkTimeout`/`testDisconnect`
   failure path, `testFailureOnFatalException`, `testFencedMember...`,
   `testHeartbeatResponseOnErrorHandling` matrix rows,
   `testHeartbeatResponseErrorNotifiedToGroupManager...`). The docstring should
   be updated to reflect actual coverage. Genuine remaining gaps:
   - **`testHeartBeatRequestStateToStringBase` — MISSING and easily fixable.**
     The Java test asserts the EXACT `toStringBase()` string content. The Rust
     `Display` impl (heartbeat_request_state.rs:198) produces the equivalent
     string but is **never tested**. Error/string content is part of the
     behavioral contract (DoD §3). This is a low-effort, in-scope gap worth
     closing now (no Phase-10 dependency).
   - **`testFirstHeartbeatIncludesRequiredInfoToJoinGroup...`,
     `testValidateConsumerGroupHeartbeatRequest{,AssignmentSentWhenLocalEpochChanges}`,
     `testHeartbeatState`, `testRegexInHeartbeatLifecycle`,
     `testRegexInJoiningHeartbeat`, `testRackIdInHeartbeatLifecycle`:** the
     wire-level request-field-diff behavior (`HeartbeatState::build_request_data`)
     is NOT pinned by any test. `issue3_error_response_resets_sent_fields`
     touches the SentFields reset but no test asserts the *positive* "first
     heartbeat includes member-id/rebalance-timeout/server-assignor/rack-id/
     subscribed-topics, subsequent heartbeats omit unchanged fields" contract.
     This is a real fidelity gap for the request-building logic (the field-diff
     omission is wire-incompatible with Java if wrong, and round-trips won't
     catch it). Deferred to Phase 10 per docstring.
   - **`testPollTimerExpiration`, `testPollOnLeaving`,
     `testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving`,
     `testPollOnCloseGeneratesRequestIfNeeded`,
     `testSendingLeaveGroupHeartbeatWhenPreviousOneInFlight`,
     `testisExpiredByUsedForLogging`, `testNoCoordinator` (only the
     coordinator-unknown subset is covered via `poll_returns_empty_when_no_coordinator`):**
     poll-timer / leave-group poll lifecycle largely untranslated.

5. **AbstractMembershipManager / AbstractHeartbeatRequestManager unit tests
   are a Rust-split bonus, not a Java mapping.** Java has no
   `Abstract*ManagerTest`; the Rust split adds focused unit tests
   (transition-validity, LocalAssignment epoch bumps, §31 handshake,
   error-classification table, poll-timer arming). These are good and cover
   the abstract-base behavior that Java tests only implicitly via the concrete
   subclass. No fidelity concern.

6. **§31 contract IS tested — just not in the membership files.** The two
   mandated §31 regression tests (`section_31_commit_sync_from_inside_revoked_callback_succeeds`,
   `section_31_rebalance_does_not_advance_until_listener_resolves`) live in
   `src/consumer/async_kafka_consumer.rs` (out of this review's file scope but
   confirmed present). `tests/consumer/async_kafka_consumer_test.rs` is an
   empty stub (0 tests) — inline tests are used instead.

## ConsumerMembershipManagerTest.java (84 @Test; lines 86-88 are helpers, not tests)

Covered behaviors map across BOTH `consumer_membership_manager.rs` (28 tests)
and `abstract_membership_manager.rs` (10 tests). Status by Java test:

| Java test | Status | Notes |
|---|---|---|
| testMembershipManagerServerAssignor | PRESERVED | `server_assignor_accessor` |
| testMembershipManagerRackId | PRESERVED | `rack_id_accessor` |
| testMembershipManagerInitSupportsEmptyGroupInstanceId | PRESERVED | `init_supports_empty_group_instance_id` |
| testAssignedPartitionCountMetricRegistered | OUT_OF_SCOPE | no Rust metrics framework |
| testReconcilingWhenReceivingAssignmentFoundInMetadata | REDUCED | `on_heartbeat_request_generated_acknowledging_to_stable` covers ACK→STABLE only; metadata-found path uses pre-seeded cache, not metadata lookup |
| testTransitionToReconcilingIfEmptyAssignmentReceived | PRESERVED | `on_heartbeat_success_empty_assignment_transitions_to_reconciling` |
| testMemberIdAndEpochResetOnFencedMembers | PRESERVED | `member_id_and_epoch_reset_on_fenced_members`; asserts member_id NOT cleared |
| testTransitionToFatal | REDUCED | covered by `listeners_get_notified_on_transitions_to_fatal` / `transition_to_failed_when_trying_to_join` |
| testTransitionToFailedWhenTryingToJoin | PRESERVED | `transition_to_failed_when_trying_to_join` |
| testFencingWhenStateIsStable | PRESERVED | `fencing_when_state_is_stable` |
| testListenersGetNotifiedOnTransitionsToFatal | PRESERVED | `listeners_get_notified_on_transitions_to_fatal` |
| testListenersGetNotifiedOnTransitionsToLeavingGroup | PRESERVED | `listeners_get_notified_on_transitions_to_leaving_group` |
| testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges | PRESERVED | `listeners_notified_only_on_epoch_change` |
| testFencingWhenStateIsReconciling | PRESERVED | `fencing_when_state_is_reconciling` |
| testFencingWhenStateIsPrepareLeaving | PRESERVED | `fencing_when_state_is_prepare_leaving` |
| testFencingWhenStateIsPrepareLeavingCompletesTheLeaveOperation | MISSING | Mockito-spy verification; not translated |
| testNewAssignmentIgnoredWhenStateIsPrepareLeaving | PRESERVED | `new_assignment_ignored_when_state_is_prepare_leaving` |
| testFencingWhenStateIsLeaving | PRESERVED | `fencing_when_state_is_leaving` |
| testLeaveGroupEpoch | PRESERVED | `leave_group_epoch_test` + accessor tests |
| testLeaveGroupEpochOnClose | PRESERVED | `leave_group_epoch_on_close` |
| testDelayedReconciliationResultDiscardedIfMemberNotInReconcilingStateAnymore | MISSING | delayed-reconcile/metadata path deferred |
| testDelayedReconciliationResultDiscardedAfterCommitIfMemberRejoins | MISSING | delayed-reconcile path deferred |
| testDelayedReconciliationResultDiscardedAfterPartitionsRevokedCallbackIfMemberRejoins | MISSING | delayed-reconcile path deferred |
| testDelayedReconciliationResultDiscardedAfterPartitionsAssignedCallbackIfMemberRejoins | MISSING | delayed-reconcile path deferred |
| testSameAssignmentReconciledAgainWhenFenced | PRESERVED | `same_assignment_reconciled_again_when_fenced` |
| testSameAssignmentReconciledAgainWithMissingTopic | MISSING | missing-topic/metadata path deferred |
| testDelayedReconciliationResultAppliedWhenTargetChangedWithMetadataUpdate | MISSING | metadata-update path deferred |
| testDelayedReconciliationResultAppliedWhenTargetChangedWithNewAssignment | MISSING | delayed-reconcile path deferred |
| testDelayedMetadataUsedToCompleteAssignment | MISSING | metadata path deferred |
| testLeaveGroupWhenStateIsStable | REDUCED | leave epoch covered; per-state leave matrix not exhaustively translated |
| testHeartbeatSuccessfulResponseWhenLeavingGroupCompletesLeave | MISSING | leave-completion-via-response deferred (Phase 10) |
| testHeartbeatFailedResponseWhenLeavingGroupCompletesLeave | MISSING | leave-completion-via-failure deferred |
| testIgnoreHeartbeatResponseWhenNotInGroup | MISSING | not translated |
| testIgnoreLeaveResponseWhenNotLeavingGroup | MISSING | not translated |
| testLeaveGroupWhenMemberOwnsAssignment | MISSING | owns-assignment leave (callback) deferred |
| testFencedWhenAssignmentEmpty | REDUCED | `fencing_when_state_is_*` exercise empty-assignment short-circuit |
| testLeaveGroupWhenMemberAlreadyLeaving | MISSING | leave-idempotency not translated |
| testLeaveGroupWhenMemberAlreadyLeft | MISSING | leave-idempotency not translated |
| testLeaveGroupWhenMemberFenced | MISSING | not translated |
| testLeaveGroupWhenMemberIsStale | MISSING | stale path not translated |
| testFatalFailureWhenStateIsUnjoined | REDUCED | `transition_to_failed_when_trying_to_join` covers JOINING→FATAL |
| testFatalFailureWhenStateIsStable | MISSING | per-state fatal matrix not exhaustive |
| testFatalFailureWhenStateIsPrepareLeaving | MISSING | per-state fatal matrix not exhaustive |
| testFatalFailureWhenStateIsLeaving | MISSING | per-state fatal matrix not exhaustive |
| testFatalFailureWhenMemberAlreadyLeft | MISSING | not translated |
| testUpdateStateFailsOnResponsesWithErrors | MISSING | not translated (heartbeat-side error covered in HB manager tests) |
| testNewAssignmentReplacesPreviousOneWaitingOnMetadata | MISSING | metadata-waiting path deferred |
| testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata | MISSING | metadata-waiting path deferred |
| testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata | MISSING | metadata-waiting path deferred |
| testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived | MISSING | metadata-resolution path deferred (material) |
| testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved | MISSING | metadata-resolution path deferred (material) |
| testReconcileNewPartitionsAssignedWhenNoPartitionOwned | REDUCED | `reconcile_emits_assigned_callback_and_acks` covers assign+ack but via pre-seeded cache, no metadata resolve, no SubscriptionState assignment verify |
| testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned | MISSING | revoke+assign overlap not exercised |
| testReconciliationSkippedWhenSameAssignmentReceived | REDUCED | partially via `same_assignment_reconciled_again_when_fenced` semantics |
| testReconcilePartitionsRevokedNoAutoCommitNoCallbacks | MISSING | revoke path not exercised |
| testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks | REDUCED | `reconcile_can_commit_true_proceeds_when_auto_commit_enabled` covers can_commit gate, not revoke+autocommit ordering |
| testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway | MISSING | failed-autocommit-still-revokes not translated |
| testReconcileNewPartitionsAssignedAndRevoked | MISSING | combined assign+revoke not translated |
| testMetadataUpdatesReconcilesUnresolvedAssignments | MISSING | metadata path deferred |
| testMetadataUpdatesRequestsAnotherUpdateIfNeeded | MISSING | metadata path deferred |
| testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable | MISSING | local-cache-fallback not directly asserted |
| testOnSubscriptionUpdatedDoesNotTransitionToJoiningIfInGroup | MISSING | onSubscriptionUpdated path not translated |
| testOnSubscriptionUpdatedTransitionsToJoiningOnPollIfNotInGroup | REDUCED | `transition_to_joining_from_unsubscribed` covers UNSUBSCRIBED→JOINING |
| testListenerCallbacksBasic | REDUCED | §31 handshake covered in abstract_membership_manager tests |
| testListenerCallbacksThrowsErrorOnPartitionsRevoked | MISSING | revoked-callback-error not translated (assigned-error IS) |
| testListenerCallbacksThrowsErrorOnPartitionsAssigned | PRESERVED | `reconcile_propagates_assigned_listener_error` |
| testAddedPartitionsTemporarilyDisabledAwaitingOnPartitionsAssignedCallback | MISSING | partition-enable-after-callback not translated |
| testAddedPartitionsNotEnabledAfterFailedOnPartitionsAssignedCallback | MISSING | not translated |
| testOnPartitionsLostNoError | MISSING | onPartitionsLost path not translated |
| testOnPartitionsLostError | MISSING | onPartitionsLost error path not translated |
| testTransitionToLeavingWhileReconcilingDueToStaleMember | MISSING | STALE path deferred |
| testTransitionToLeavingWhileJoiningDueToStaleMember | MISSING | STALE path deferred |
| testTransitionToLeavingWhileStableDueToStaleMember | MISSING | STALE path deferred |
| testTransitionToLeavingWhileAcknowledgingDueToStaleMember | MISSING | STALE path deferred |
| testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover | MISSING | STALE path deferred |
| testStaleMemberRejoinsWhenTimerResetsNoCallbacks | MISSING | STALE path deferred |
| testStaleMemberWaitsForCallbackToRejoinWhenTimerReset | MISSING | STALE path deferred |
| testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment | REDUCED | empty-assignment→RECONCILING covered; STABLE tail via separate test |
| testMemberJoiningCallsRebalanceListenerWhenReceivingEmptyAssignment | MISSING | listener-on-empty-assignment not translated |
| testMetricsWhenHeartbeatFailed | OUT_OF_SCOPE | metrics |
| testRebalanceMetricsOnSuccessfulRebalance | OUT_OF_SCOPE | metrics |
| testRebalanceMetricsForMultipleReconciliations | OUT_OF_SCOPE | metrics |
| testRebalanceMetricsOnFailedRebalance | OUT_OF_SCOPE | metrics |
| testPollMustCallsMaybeReconcileWithFalse | REDUCED | `reconcile_can_commit_false_is_noop_when_auto_commit_enabled` covers the can_commit=false gate |

Rust-only bonus tests (no Java analog, valid behavior pins): transition_to_joining_from_unsubscribed,
leave_group_epoch_{dynamic,static,static_force}, is_leaving_group_{dynamic,static},
reconcile_can_commit_{false_noop,true_proceeds}, plus all 10 abstract_membership_manager.rs unit tests.

## ConsumerHeartbeatRequestManagerTest.java (31 @Test)

Covered across `consumer_heartbeat_request_manager.rs` (18 tests) and
`abstract_heartbeat_request_manager.rs` (9 tests).

| Java test | Status | Notes |
|---|---|---|
| testHeartBeatRequestStateToStringBase | MISSING | Display impl exists but untested; Java asserts exact string. Low-effort, in-scope gap |
| testHeartbeatOnStartup | PRESERVED | `heartbeat_on_startup` |
| testSuccessfulHeartbeatTiming | REDUCED | `successful_response_updates_interval` + `timer_not_due`; full timing matrix not reproduced |
| testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments | MISSING | request-field-diff not pinned (material — wire fidelity) |
| testSkippingHeartbeat | PRESERVED | `poll_returns_empty_when_no_coordinator` + UNSUBSCRIBED skip in `heartbeat_on_startup` |
| testTimerNotDue | PRESERVED | `timer_not_due` |
| testHeartbeatNotSentIfAnotherOneInFlight | REDUCED | `heartbeat_not_sent_if_another_one_in_flight` (omits inflight-completion + retry segment, noted) |
| testHeartbeatOutsideInterval | PRESERVED | `heartbeat_outside_interval` |
| testNetworkTimeout | REDUCED | transport-failure path covered by `test_response_routing_failure_path`; not the exact timeout scenario |
| testDisconnect | REDUCED | covered by failure-path routing test (coordinator disconnect handled in on_failure) |
| testFailureOnFatalException | REDUCED | `issue5_unknown_error_code_falls_through_to_fatal` + `issue4_group_authorization_failed_drives_transition_to_fatal` |
| testHeartbeatResponseErrorNotifiedToGroupManagerAfterErrorPropagated | REDUCED | `issue4_*`/`issue5_*` assert ErrorEvent emission + transition |
| testHeartbeatRequestFailureNotifiedToGroupManagerAfterErrorPropagated | REDUCED | `test_response_routing_failure_path` |
| testNoCoordinator | REDUCED | `poll_returns_empty_when_no_coordinator` (coordinator-unknown subset) |
| testValidateConsumerGroupHeartbeatRequest | MISSING | request-field validation not pinned (material — wire fidelity) |
| testValidateConsumerGroupHeartbeatRequestAssignmentSentWhenLocalEpochChanges | MISSING | assignment-sent-on-epoch-change diff not pinned |
| testHeartbeatResponseOnErrorHandling (parameterized matrix) | REDUCED | abstract `classify_response_error` table tested (NotCoordinator/GroupAuthz/Fenced/UnknownMember/UnsupportedVersion→delegate) + Consumer handle_specific (UnsupportedVersion/FencedInstanceId/UnreleasedInstanceId); not every matrix row driven end-to-end |
| testUnsupportedVersionFromBroker | PRESERVED | `handle_specific_unsupported_version_is_fatal` + `handle_specific_failure_unsupported_version_emits_error_event` |
| testUnsupportedVersionFromClient | REDUCED | client-side unsupported-version partially via handle_specific tests |
| testHeartbeatState | MISSING | request-state field-diff lifecycle not pinned (material) |
| testPollTimerExpiration | MISSING | poll-timer expiry → stale deferred (Phase 10) |
| testPollOnLeaving | REDUCED | `should_not_send_leave_when_not_leaving` covers negative; positive leave-poll matrix not translated |
| testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving | MISSING | STALE/leaving interaction deferred |
| testisExpiredByUsedForLogging | MISSING | not translated |
| testFencedMemberStopHeartbeatUntilItReleasesAssignmentToRejoin | REDUCED | `issue4_fenced_member_epoch_drives_transition_to_fenced` covers fence classification + transition; "stop HB until release" timing not exercised |
| testSendingLeaveGroupHeartbeatWhenPreviousOneInFlight | MISSING | leave-HB-while-inflight not translated |
| testConsumerAcksReconciledAssignmentAfterAckLost | MISSING | ack-lost replay not translated |
| testPollOnCloseGeneratesRequestIfNeeded | REDUCED | poll-on-close path exists in source (poll_on_close), not directly tested |
| testRegexInHeartbeatLifecycle | MISSING | regex subscription path deferred (Phase 9 RE2/J) |
| testRegexInJoiningHeartbeat | MISSING | regex path deferred |
| testRackIdInHeartbeatLifecycle | MISSING | rack-id-in-request diff not pinned |

Rust-only bonus tests: handle_specific_returns_none_for_other_errors,
maximum_time_to_wait_returns_zero_when_poll_timer_expired,
test_response_routing_through_spawned_forwarder, issue3_error_response_resets_sent_fields,
and all 9 abstract_heartbeat_request_manager.rs unit tests (poll-timer arming,
classify_response_error table, on_failure retriable).

## HeartbeatRequestStateTest.java (5 @Test) — heartbeat_request_state.rs

| Java test | Status | Notes |
|---|---|---|
| testCanSendRequestAndTimeToNextHeartbeatMs | PRESERVED | `test_can_send_request_and_time_to_next_heartbeat_ms` |
| testResetTimer | PRESERVED | `test_reset_timer` |
| testUpdateHeartbeatIntervalMs | PRESERVED | `test_update_heartbeat_interval_ms` |
| testUpdateHeartbeatIntervalMsWithSameInterval | PRESERVED | `test_update_heartbeat_interval_ms_with_same_interval` |
| testOnFailedAttempt | PRESERVED | `test_on_failed_attempt` |

## HeartbeatTest.java (7 @Test) — OUT_OF_SCOPE

`Heartbeat` is a classic-coordinator (`AbstractCoordinator`) dependency,
excluded per §20. No Rust translation; correct skip. (All 7: testShouldHeartbeat,
testShouldNotHeartbeat, testTimeToNextHeartbeat, testSessionTimeoutExpired,
testResetSession, testResetTimeouts, testPollTimeout.)

## Suggested actions (priority order)

1. Add `testHeartBeatRequestStateToStringBase` equivalent — assert the exact
   `HeartbeatRequestState` Display string. Trivial, in-scope, no dependency.
2. When Phase-10/11 wiring lands, prioritize the **metadata-driven reconcile**
   and **STALE-member** test families — these are the membership state
   machine's largest untested regions, not mere Mockito-spy ceremony.
3. Pin the heartbeat **request-field diff** behavior
   (`testFirstHeartbeatIncludes...`, `testValidateConsumerGroupHeartbeatRequest`,
   `testHeartbeatState`) — wire-incompatibility here would pass round-trips
   silently (DoD §3 byte-level concern).
4. Update the stale in-file deferral docstrings (HB manager says "12/31",
   membership says "~26/93") to reflect the actual current coverage.
