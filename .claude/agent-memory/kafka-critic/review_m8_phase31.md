---
name: review-m8-phase31
description: Phase 31 reset/validate test-parity review — multi-branch guard collapse SKIP, prod-fix dual-copy drift
metadata:
  type: project
---

Phase 31 (test-parity for reset-positions / validate-positions / LogTruncation,
OffsetsRequestManager + OffsetFetcher reset rows). Critic findings.

**SKIP-collapse anti-pattern (Issues 1 & 2):** When a PLAN justifies skipping
N Java tests as "same guard, same discard outcome as translated test X",
open the guard and count its early-return branches. `maybe_seek_unvalidated`
(subscription_state.rs) has THREE discard triggers: (1) not-assigned, (2)
!awaiting_reset, (3) requested-strategy-mismatch. The translated seek test
exits at branch 2; the idempotent test takes the *apply* path (same strategy).
Branches 1 (testAssignmentChangeWithInFlightReset) and 3
(testChangeResetWithInFlightReset, testEarlierOffsetResetArrivesLate) were
claimed "covered" but are uncovered — a mutation deleting them passes all
translated tests. The in-flight-reset family lives in **OffsetFetcherTest.java**,
NOT OffsetsRequestManagerTest. The strategy-mismatch test also asserts a
DISTINCT post-state (reset still needed + NEW strategy survives), not "same
outcome." **Heuristic: "same guard" ≠ "same branch"; verify by branch, and
check the asserted post-state differs.**

**Production-fix dual-copy drift (recorded, correct):** Java's single
`groupListOffsetRequests` (OffsetsRequestManager.java:892) is called by both
fetchOffsets and resetPositions. Rust split it into `group_list_offset_requests`
(fetch path, line ~357) + an inlined loop in
`send_list_offsets_requests_and_reset_positions` (reset path). The reset copy
had drifted (missing set_current_leader_epoch). Fix correct + guarded by
`reset_request_includes_current_leader_epoch` which builds the actual
ListOffsetsRequest and asserts the epoch on the wire. **When Java has one
helper but Rust has two copies, audit BOTH for drift.**

**Under-assertion vs Java mock-verify (Issue 3, minor):** Java
testResetOffsetsAuthorizationFailure asserts `verify(metadata).requestUpdate(false)`
+ `verify(subscriptionState).requestFailed(...)`. Rust test asserts only the
re-raised variant. Mock-verify side effects map to observable state
(update_requested(), retry-backoff). Note the *reset* auth test asserts
unauthorizedTopics (OffsetFetcherTest) but the *validate* auth test does NOT
(OffsetsRequestManagerTest:616) — match the specific Java test the PLAN maps to.

LogTruncation payload test was strong (concrete divergent offset/epoch +
stays-AWAITING_VALIDATION). OFLE testUnexpectedEmptyResponse faithful; other
5 OFLE tests pre-translated. All 58 ORM tests pass.
