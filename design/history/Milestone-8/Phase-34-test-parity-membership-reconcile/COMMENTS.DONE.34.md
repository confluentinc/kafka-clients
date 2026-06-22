# Critic (34) review — Phase 34: ConsumerMembershipManagerTest metadata-driven reconcile parity — RESOLVED

Commit 9933818. Reviewed: production change in `abstract_membership_manager.rs`
(on_consumer_poll flag-clear), and 36 new inline tests in
`consumer_membership_manager.rs`.

## Production change — CLEAN (no action; left as-is)

`AbstractMembershipManager::on_consumer_poll` flag-clear verified faithful to
`AbstractMembershipManager.java:490-493`. PERF: neutral.

## Test fidelity issues — ALL FIXED

### Issue 1: testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway never injects a commit failure — FIXED

- **File**: `src/consumer/internals/consumer_membership_manager.rs`
  (`reconcile_partitions_revoked_with_failed_auto_commit_completes_revocation_anyway`)
- **Java Reference**: `ConsumerMembershipManagerTest.java:1579` /
  `commitResult.completeExceptionally(...)` (test:1596).
- **Resolution**: The test now drives a REAL commit failure. It seeds a valid
  position on the owned partition (`subs.seek(&tp("topic1", 0), 100)`) so
  `subscriptions.allConsumed()` is non-empty and the auto-commit before
  rebalance actually enqueues an `OffsetCommitRequestState`. The reconcile is
  spawned (parking at step 8a awaiting the commit future); the test then waits
  for the unsent commit to be enqueued and fails it via
  `CommitRequestManager::fail_first_unsent_commit_for_test(KafkaError::new(Errors::OffsetMetadataTooLarge))`
  — a non-retriable error, mirroring Java's non-retriable `KafkaException`.
  The non-retriable driver path resolves the public future as `Ok(Err(err))`,
  hitting the previously-untested failure arm. The test then asserts the
  reconcile still reaches `ACKNOWLEDGING` with everything revoked.
- **Mutation check (confirmed)**: changing the step-8a `Ok(Err(err))` arm from
  "log + proceed" to `return Err(err)` makes the test FAIL (reconcile returns
  `Err(OffsetMetadataTooLarge)` instead of completing the revocation).

### Issue 2: testMetadataUpdatesRequestsAnotherUpdateIfNeeded cannot detect a missing second requestUpdate — FIXED

- **File**: `src/consumer/internals/consumer_membership_manager.rs`
  (`metadata_updates_requests_another_update_if_needed`)
- **Java Reference**: `ConsumerMembershipManagerTest.java:1661` —
  `verify(metadata, times(2)).requestUpdate(anyBoolean())`.
- **Resolution**: Added a `#[cfg(test)]`-gated call counter
  `request_update_call_count` on `MetadataInner`, incremented inside a
  `#[cfg(test)]` block in `Metadata::request_update`, exposed via
  `Metadata::request_update_call_count_for_test()`. The test now snapshots the
  counter after each `find_resolvable_assignment_and_trigger_metadata_update()`
  pass and asserts it advances `1 -> 2` (Mockito `times(2)` equivalent) instead
  of relying on the sticky `update_requested()` boolean.
- **Perf**: zero production cost. The counter field AND its increment are both
  `#[cfg(test)]`-gated, so they do not exist in non-test builds (`cargo build`
  remains clean). `request_update` is not on a per-record hot path regardless.
- **Mutation check (logical)**: a mutation dropping the second per-pass
  `request_update(true)` in
  `find_resolvable_assignment_and_trigger_metadata_update` leaves the counter
  at 1, failing the `== 2` assertion. The old sticky-flag assertion would still
  pass.

### Issue 3: testDelayedReconciliationResultDiscardedAfterCommitIfMemberRejoins parked on the wrong future — FIXED

- **File**: `src/consumer/internals/consumer_membership_manager.rs`
  (`delayed_reconciliation_result_discarded_after_commit_if_member_rejoins`)
- **Java Reference**: `ConsumerMembershipManagerTest.java:566` —
  `mockNewAssignmentAndRevocationStuckOnCommit` (test:576) parks on the COMMIT
  future; `commitResult.complete(null)` (test:591) then triggers the discard.
- **Resolution**: The test now parks specifically on the COMMIT future. It uses
  `make_with_commit_manager(false)` (real `CommitRequestManager`, no listener so
  the §31 revoked/lost callbacks short-circuit — making the commit future the
  ONLY park point), seeds a consumed offset, and receives an empty assignment
  (revoke-all) so the revocation commit is enqueued. It waits until the unsent
  commit is enqueued AND `reconciliation_in_progress` is true (reconcile is
  parked on the commit), fences + rejoins via `transition_to_fenced(0)` while
  parked, receives a post-rejoin assignment (topic3-5), then completes the
  commit via `complete_first_unsent_commit_for_test(HashMap::new())`. Asserts
  the in-flight reconcile is discarded: state `!= ACKNOWLEDGING`, no
  reconciliation in progress, and the post-rejoin target is what is pending to
  reconcile next.
  - New `#[cfg(test)]` accessor `CommitRequestManager::unsent_offset_commits_len_for_test()`
    added (sibling to the existing `inner_state_for_test`, which counts
    unsent `OffsetFetch`) so the test can detect when the reconcile has parked
    on the commit.
- **Faithfulness note on the abort guard**: Rust `reconcile` has two
  `maybe_abort_reconciliation` checks (step 10 after commit+revoked-callback,
  step 16 final) — faithfully mirroring Java's `revokeAndAssign` which also has
  two (`AbstractMembershipManager.java:951` and `:967`). Deleting only one is
  masked by the other in BOTH languages, so the mutation target is the abort
  MECHANISM.
- **Mutation check (confirmed)**: forcing `maybe_abort_reconciliation` to always
  return `false` (removing the discard mechanism) makes the test FAIL — the
  empty-target reconcile reaches `ACKNOWLEDGING`, tripping
  `assert_ne!(.., Acknowledging)`.

## Verdict (post-fix)

All 3 test-fidelity issues resolved. Build / `cargo test --lib` (1915 passing) /
`cargo xtask format-check` / `cargo xtask lint` all green. No production behavior
change (Issue-2 counter is `#[cfg(test)]`-gated; Issues 1 & 3 are test-only plus
one `#[cfg(test)]` accessor on `CommitRequestManager`).
