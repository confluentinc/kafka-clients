# COMMENTS.DONE.31 — resolved Critic items for Phase 31

## Issue 1: in-flight reset "strategy change" discard branch is untested — RESOLVED

- **Fix commit**: fixup of `c808f89` (Phase 31: OffsetFetcher reset behavioral family).
- **What changed**: Added two tests to `offsets_request_manager.rs` tests module:
    - `reset_change_strategy_with_in_flight_reset_discards_stale_response`
      (Java parity `OffsetFetcherTest.testChangeResetWithInFlightReset`):
      re-requests reset with EARLIEST while a LATEST reset response is in
      flight; delivers the LATEST response; asserts it is DISCARDED by the
      strategy-mismatch guard (`subscription_state.rs:978-983`) — partition
      still awaiting reset, `reset_strategy == EARLIEST` (the new strategy
      survives), no position applied.
    - `reset_earlier_offset_reset_arrives_late`
      (Java parity `OffsetFetcherTest.testEarlierOffsetResetArrivesLate`):
      two-phase — (1) in-flight EARLIEST response discarded after a LATEST
      reset is requested, reset still needed under LATEST; (2) second reset
      under LATEST succeeds, `position == 10`.
- **Mutation verified**: deleting `subscription_state.rs:978-983` (the
  strategy-mismatch guard) fails BOTH new tests
  ("reset still needed after discarding the stale LATEST response" /
  "stale EARLIEST result ignored; reset still needed").

## Issue 2: in-flight reset "assignment change" discard branch is untested — RESOLVED

- **Fix commit**: fixup of `c808f89`.
- **What changed**: Added
  `reset_assignment_change_with_in_flight_reset_discards_stale_response`
  (Java parity `OffsetFetcherTest.testAssignmentChangeWithInFlightReset`):
  assigns `tp0`, requests reset, polls out the request, reassigns to `tp1`
  (dropping `tp0`), delivers the `tp0` response; asserts it is discarded by
  the first guard (`subscription_state.rs:965-971` — partition no longer
  assigned). Observable assertions: `!is_assigned(tp0)`, `is_assigned(tp1)`,
  and `tp1` has no position applied.

## Issue 3 (minor): `reset_offsets_authorization_failure` under-asserts side effects — RESOLVED

- **Fix commit**: fixup of `b406b1a` (Phase 31: reset-positions response-path).
- **What changed**: Added Java's error-path `verify(...)` side-effect
  assertions to `reset_offsets_authorization_failure` (Java parity
  `OffsetsRequestManagerTest.testResetOffsetsAuthorizationFailure`):
    - `requestFailed(any(), anyLong())`: asserts the partition is still
      awaiting reset but no longer reset-ready at the same instant
      (`partitions_needing_reset(0)` does not contain the partition — the
      retry backoff was advanced).
    - `requestUpdate(false)`: snapshots `metadata.update_requested()` before
      and after; asserts it flips to `true` (a metadata update was requested).
  The existing cached-error re-raise + zero-request assertions are retained
  (not weakened).
