---
name: review-m8-phase34
description: Phase 34 ConsumerMembershipManager metadata-reconcile test-parity patterns — trivially-passing failure test, sticky-flag vs times(N), park-point collapse
metadata:
  type: project
---

Phase 34 (commit 9933818): translated ConsumerMembershipManagerTest metadata-driven
reconcile region (+36 tests, 38→74) + ONE prod fix (on_consumer_poll clears
subscription_updated unconditionally, mirroring Java CAS-then-state-check). Prod
fix CLEAN + perf-neutral. Three test-fidelity findings (no prod bugs):

**Pattern: "failed X completes anyway" test that never injects the failure.**
Java's testReconcilePartitionsRevokedWithFailedAutoCommit uses
`commitResult.completeExceptionally(...)`. Rust reused the SAME no-offset
commit-manager helper as the SUCCESS test → commit resolves via the
no-op-when-no-consumed-offsets path → the `Ok(Err(err)) => log+proceed` arm is
never hit. Test is a duplicate of the success test, passes trivially. **Critic
heuristic:** when a Java test name says "WithFailed*" / "*Anyway" / "*EvenIf*",
grep the Java for `completeExceptionally` / injected error and confirm the Rust
test actually drives that error arm — not just asserts the happy end-state.

**Pattern: sticky boolean substituted for Mockito `verify(times(N))`.**
metadata.update_requested() (src/metadata.rs:599) = need_full_update ||
need_partial_update — STICKY until an update completes. A test asserting
update_requested() twice cannot detect a missing SECOND requestUpdate (flag
left set by the first). Java's `verify(metadata, times(2)).requestUpdate`
counts per-attempt. **Heuristic:** when Java verifies a call COUNT and Rust
asserts a sticky/idempotent boolean, the second+ assertion is toothless — flag
the mutation gap.

**Pattern: delayed-result discard park-point collapse.** Java places 3 discard
tests at 3 distinct park points (commit / onPartitionsRevoked callback /
onPartitionsAssigned callback). Rust reconcile awaits the commit SYNCHRONOUSLY
before the revoked callback (step 8a) with the only abort check at step 10
(after the callback). So the AfterCommit test parked on the revoked callback
instead → duplicates the AfterPartitionsRevoked test; the rejoin-during-commit
timing is untested. Lower severity (shared abort guard) but a 1:1 divergence.

**What was GOOD (don't re-flag):** direct calls to
find_resolvable_assignment_and_trigger_metadata_update() for the PURE-unresolved
phase are a faithful stand-in for Java's verifyReconciliationNotTriggered
(pre-metadata); the resolvable phase still runs full reconcile. Discard tests
trigger rejoin/fatal BEFORE sending the stuck ack and assert state!=Acknowledging
+ assignment-not-applied (mutation-resistant on maybe_abort_reconciliation).
added_partitions_*_disabled tests seek() a valid position first so is_fetchable
flips solely on the pending_on_assigned_callback gate (mutation-resistant).
update_state_fails asserts error MESSAGE content.
