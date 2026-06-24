---
name: phase34-critic-round1-patterns
description: Phase 34 Critic round-1 fixes — three reusable test-fidelity patterns for membership reconcile tests
metadata:
  type: feedback
---

Phase 34 Critic round-1 (Actor 34) fixed three membership-reconcile test-fidelity
issues in `consumer_membership_manager.rs`. Reusable patterns:

1. **"proceed-anyway" failure-arm test must inject a REAL failure, not a success.**
   The original test used the no-consumed-offset path (success) so the
   `Ok(Err(err))` proceed-anyway arm was never hit. Fix: seed a valid position
   (`subs.seek(&tp, 100)`) so `all_consumed()` is non-empty → auto-commit
   enqueues a real `OffsetCommitRequestState`; spawn the reconcile (parks on the
   commit future); then `CommitRequestManager::fail_first_unsent_commit_for_test(
   KafkaError::new(Errors::OffsetMetadataTooLarge))` (a NON-retriable error so the
   driver does `break Err`). Mutation: changing the arm to `return Err(err)` fails.
   **How to apply:** any "X completes anyway despite failure Y" test must drive Y;
   `make_with_commit_manager(false)` (no listener) makes the commit the only park.

2. **Mockito `verify(.., times(N))` → `#[cfg(test)]` call counter, NOT a sticky bool.**
   `Metadata::update_requested()` is `need_full_update || need_partial_update` —
   sticky, can't detect a missing SECOND `request_update`. Added
   `#[cfg(test)] request_update_call_count` field on `MetadataInner` +
   `#[cfg(test)]`-block increment in `request_update` + accessor
   `request_update_call_count_for_test()`. Both field and increment cfg(test)-gated
   = zero prod cost (`cargo build` stays clean). Assert counter 1→2 across passes.
   **How to apply:** whenever Java asserts `times(N)` on a method whose only Rust
   observable is a sticky flag, add a cfg(test) call counter sibling to the
   existing Phase-32 `equivalent_response_count_for_test` / `need_full_update_for_test`.

3. **Park on the SAME future Java parks on — and know the two-guard masking is faithful.**
   Java `mockNewAssignmentAndRevocationStuckOnCommit` parks on the COMMIT future;
   the Rust test had collapsed onto the revoked-callback park. Fix: use
   `make_with_commit_manager(false)`, seed consumed offset, `receive_empty_assignment`
   (revoke-all enqueues the commit), busy-wait on
   `unsent_offset_commits_len_for_test() > 0 && reconciliation_in_progress(&mgr)`,
   then `transition_to_fenced(0)` (no listener ⇒ lost callback short-circuits ⇒
   fence completes sync ⇒ sets rejoin flag), receive post-rejoin assignment,
   `complete_first_unsent_commit_for_test(HashMap::new())`, assert discard
   (state != Acknowledging, awaiting = post-rejoin target).
   - Rust `reconcile` has TWO `maybe_abort_reconciliation` checks (step 10 + step 16),
     faithfully mirroring Java `revokeAndAssign` (`AbstractMembershipManager.java:951`
     and `:967`). Deleting ONE is masked by the other in BOTH languages. Mutation
     target is the abort MECHANISM: force `maybe_abort_reconciliation` to return
     `false` → test fails (reaches Acknowledging). Don't expect single-guard deletion
     to fail — that's a faithful structural property, document it.
   - GOTCHA: after fence, the subscription assignment is ALREADY cleared (fence
     releases). Don't assert "revoked partition still owned" — assert on
     `topic_partitions_awaiting_reconciliation == post-rejoin target` instead.

Cross-cutting: `make_with_commit_manager` uses default ConsumerConfig
(enable_auto_commit=true) + shared `subs` Arc; reach the commit manager in tests
via `mgr.commit_request_manager.as_ref().unwrap()` (pub(crate) field).
`mark_pending_revocation` does NOT clear position, so `all_consumed()` still
includes a seeked partition after step-8 pending-revocation marking.
