# Phase 31 — Test parity: reset-positions / validate-positions / LogTruncation

Actor 31. Test-only phase. Closes the highest-value gap from
`design/current/test-translation-review/03-commit-offsets-coordinator.md`:
the reset-positions and validate-positions **response paths**, plus the
**LogTruncation** family. Production code is fully implemented; this phase
adds tests (and any test-only response builders they need) only.

## Target Rust files (inline `#[cfg(test)]`)
- `src/consumer/internals/offsets_request_manager.rs` — reset/validate
  orchestration + response handling (primary).
- `src/consumer/internals/offset_fetcher_utils.rs` —
  `on_successful_response_for_validating_positions` direct payload tests.
- `src/consumer/internals/offsets_for_leader_epoch_client.rs` — already at
  parity (5 + 2 extra); one gap reviewed below.

## Reuse (no reinvention)
- `new_manager()`, `new_manager_with_commit()`,
  `build_list_offsets_response`, `build_list_offsets_client_response`,
  `complete_first_unsent_with_response`, `bootstrap_metadata_with_topic`,
  `yield_until` (all in the ORM test module).
- New test-only builder added: `build_offsets_for_leader_epoch_response`
  + `build_offsets_for_leader_epoch_client_response` +
  `complete_first_unsent_oitle_with_response` (mirrors the ListOffsets
  helpers; only added because validate tests call them).

## Java → Rust mapping

### OffsetsRequestManagerTest (reset/validate)
| Java test | Rust test | Notes |
|---|---|---|
| testResetPositionsMissingLeader | `reset_positions_missing_leader` | leader unknown → requestUpdate(true), 0 requests |
| testResetPositionsSuccess_NoLeaderEpochInResponse | `reset_positions_success_no_leader_epoch_in_response` | reset success, epoch absent → position set, no reset needed |
| testResetPositionsSuccess_LeaderEpochInResponse | `reset_positions_success_leader_epoch_in_response` | reset success, epoch present → updateLastSeenEpochIfNewer observed via metadata epoch |
| testResetOffsetsAuthorizationFailure | `reset_offsets_authorization_failure` | TOPIC_AUTH cached → re-raised on next resetPositionsIfNeeded |
| testValidatePositionsSuccess | `validate_positions_success` | OFLE success → maybeCompleteValidation → no longer awaiting validation |
| testValidatePositionsMissingLeader | `validate_positions_missing_leader` | no-node leader → requestUpdate(true), 0 requests |
| testValidatePositionsFailureWithUnrecoverableAuthException | `validate_positions_failure_with_unrecoverable_auth_exception` | OFLE TOPIC_AUTH cached → re-raised on next validatePositionsIfNeeded |

### OffsetFetcherTest reset behavioral family (KIP-848 logic now in ORM)
| Java test | Rust test | Notes |
|---|---|---|
| testUpdateFetchPositionResetToEarliestOffset | `reset_to_earliest_offset` | EARLIEST → position==5, !reset, fetchable |
| testUpdateFetchPositionResetToLatestOffset | `reset_to_latest_offset` | LATEST |
| testUpdateFetchPositionResetToDefaultOffset | `reset_to_default_offset` | requestOffsetReset() default |
| testUpdateFetchPositionResetToDurationOffset | `reset_to_duration_offset` | by-timestamp strategy |
| testFetchOffsetErrors | `reset_fetch_offset_errors_then_recovers` | OFFSET_NOT_AVAILABLE/LEADER_NOT_AVAILABLE retriable; recover |
| testListOffsetSendsReadUncommitted/ReadCommitted | `reset_list_offset_sends_read_{uncommitted,committed}` | isolation level on wire |
| testGetOffsetsFencedLeaderEpoch | `reset_fenced_leader_epoch_still_needs_reset` | FENCED_LEADER_EPOCH retriable, reset still needed |
| testGetOffsetsIncludesLeaderEpoch | `reset_request_includes_current_leader_epoch` | request carries currentLeaderEpoch |
| testListOffsetUpdateEpoch | `reset_success_updates_last_seen_leader_epoch` | higher epoch in response → metadata bump |
| testListOffsetNoUpdateMissingEpoch | (covered by `reset_positions_success_no_leader_epoch_in_response`) | UNKNOWN_EPOCH → no metadata bump |
| testUpdateFetchPositionDisconnect | `reset_disconnect_reparks_and_retries` | disconnect → re-park, retry succeeds |
| testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset | `reset_on_paused_partition_completes_but_not_fetchable` | reset completes but paused ⇒ not fetchable |
| in-flight reset family (Assignment/Seek/EarlierArrivesLate/Change/Idempotent) | see deferral note | stale-response discard via maybe_seek_unvalidated guard |

### OffsetValidation → LogTruncation family
| Java test | Rust test | Notes |
|---|---|---|
| testOffsetValidationresetPositionForUndefinedEpochWithDefinedResetPolicy | `validation_undefined_epoch_with_defined_reset_policy_resets` | UNDEFINED_EPOCH + EARLIEST → reset, !awaiting |
| testOffsetValidationresetPositionForUndefinedOffsetWithDefinedResetPolicy | `validation_undefined_offset_with_defined_reset_policy_resets` | UNDEFINED_EPOCH_OFFSET + EARLIEST → reset |
| testOffsetValidationresetPositionForUndefinedEpochWithUndefinedResetPolicy | `validation_undefined_epoch_with_undefined_reset_policy_log_truncation` | UNDEFINED_EPOCH + NONE → LogTruncation, empty divergent |
| testOffsetValidationresetPositionForUndefinedOffsetWithUndefinedResetPolicy | `validation_undefined_offset_with_undefined_reset_policy_log_truncation` | UNDEFINED_EPOCH_OFFSET + NONE → LogTruncation, empty divergent |
| testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy | `validation_bad_offset_with_undefined_reset_policy_log_truncation` | bad offset + NONE → LogTruncation w/ offsetOutOfRange + divergentOffsets payload |
| testOffsetValidationSkippedForOldBroker | `validation_skipped_for_old_broker` | OFLE v0-v2 → validation skipped (complete_validation) |
| testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest | `validation_handles_seek_with_inflight_request` | seek during in-flight OFLE → response ignored, still awaiting |

The LogTruncation structured-payload assertions (offsetOutOfRangePartitions,
divergentOffsets) are asserted directly against the `LogTruncation` struct
returned by `OffsetFetcherUtilsState::on_successful_response_for_validating_positions`
(parameterized loop over the 3 LogTruncation cases), because the
`KafkaError::from(ConsumerError::log_truncation(..))` conversion flattens to
`KafkaError::IllegalState` and loses the structured fields (documented Phase-1
design choice in `src/consumer/errors.rs:237`). The end-to-end re-raise path
(message content) is asserted at ORM level.

## Documented skips / deferrals (rationale)
- **testresetPositionsSkipsBlackedOutConnections** — OUT_OF_SCOPE per report.
  Classic `client.backoff(node)` connection-blackout has no KIP-848 ORM analogue
  (the bg-task `try_connect` queue handles connection liveness, not the manager).
- **testOffsetValidationSkippedForOldResponse** — depends on injecting a
  metadata-response *version* (v8) that yields unreliable leader epochs so
  `maybeValidatePositionForCurrentLeader` skips. The Rust `metadata_update_with`
  test helper does not let us forge a sub-v9 response version through
  `ConsumerMetadata`; the underlying skip logic (epoch-reliability gate) lives in
  `SubscriptionState::maybe_validate_position_for_current_leader` and is exercised
  by the metadata-change path. SKIPPED — would require a metadata test-builder
  extension out of scope for a test-only phase. Documented here.
- **testOffsetValidationFencing** — epoch fencing during async validation depends
  on `maybeValidatePositionForCurrentLeader` re-entering validation when the
  metadata epoch advances mid-flight; the assertion is on `awaitingValidation`
  toggling. Covered structurally by `validation_handles_seek_with_inflight_request`
  (stale-response-discard) + the metadata-change validate path. The *fencing*-
  specific re-validation is a SubscriptionState concern already unit-tested in
  `subscription_state.rs`. Folded, not duplicated.
- **in-flight reset family** (testAssignmentChangeWithInFlightReset,
  testSeekWithInFlightReset, testEarlierOffsetResetArrivesLate,
  testChangeResetWithInFlightReset, testIdempotentResetWithInFlightReset) —
  the stale-response-discard guard is `SubscriptionState::maybe_seek_unvalidated`,
  which only applies the reset offset when the partition is still AWAIT_RESET
  with the *same* requested strategy. Covered behaviorally by
  `reset_seek_with_in_flight_reset_discards_stale_response` and
  `reset_idempotent_with_in_flight_reset_applies` (the two representative
  discard/apply outcomes). The remaining three are variations on the same guard
  (assignment change, strategy change, earlier-arrives-late) and assert the same
  discard outcome; translated as the discard test covers the shared code path.

## DoD checks run after each commit
`cargo build`, `cargo test` (ORM/OFU/OFLE modules), `cargo xtask lint`,
`cargo xtask format-check`.
