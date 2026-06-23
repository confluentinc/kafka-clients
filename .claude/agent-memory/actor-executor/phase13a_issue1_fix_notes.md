---
name: phase13a_issue1_fix_notes
description: Phase 13a Issue 1 fix patterns — retry driver + mark_coordinator_unknown wiring; one fix closed all 4 production gaps
metadata:
  type: project
---

# Phase 13a (2/N): Issue-1 fix patterns

Single commit `b7e4833` (fixup against `972a053`) resolved all 4
production gaps documented in COMMENTS.1.md. Patterns worth keeping:

## Pattern 1: "Pass-through" stub drivers hide multi-issue surface

`fetch_offsets_with_retries` was named like a retry loop but was a
no-op forwarder of the first response. This single gap masqueraded as
4 distinct issues:
- Issue 1 (`committed()` NotCoordinator) — direct trigger.
- Issue 2 (`commit_sync_offsets` 60s timeout) — indirectly via the
  `mark_coordinator_unknown` gap (same root cause: response handler
  didn't refresh coordinator).
- Issue 3 (`commit_sync` after poll 60s timeout) — same as Issue 2.
- Issue 4 (`poll` surfaces NotCoordinator) — `OffsetsRequestManager
  ::init_with_committed_offsets_if_needed` calls `commit_rm.fetch_offsets(...)`;
  the un-retried error propagated as fatal `poll` error.

**Why:** Before assuming the production gap matches the symptom 1:1,
audit upstream of the symptom. The pilot translator (Phase 13a-1)
classified by user-visible symptom and arrived at 4 issues; the
implementing fixer found one root cause feeding all four.

**How to apply:** When a critic reports N issues that all share a code
path, the fix may collapse them all. Always try Issue 1's fix first;
re-run the full failing-test set before assuming Issues 2..N need
separate work.

## Pattern 2: Late-wired cross-manager handle via setter

`CommitRequestManager` and `CoordinatorRequestManager` reference each
other. Java passes the coordinator into the commit manager's
constructor; Rust hits a chicken-and-egg with `Arc` ownership. Solution:

- Add `Mutex<Option<Arc<Dep>>>` slot to `CommitRequestManagerInner`.
- Add `set_coordinator(arc)` setter on `CommitRequestManager`.
- Consumer ctor builds both, then calls
  `commit_arc.set_coordinator(Arc::clone(coord_arc))`.
- Read-paths (response handlers, retry drivers) clone the `Arc` out of
  the Mutex before using it — keeps the lock short and avoids holding
  it across the API call.

Same pattern as Phase 12.5's MemberStateListener registration.

**Why:** Direct field reference in Java requires constructor injection;
Rust's `Arc` ownership graph doesn't allow that when two managers
reference each other. Late setter is idiomatic and keeps both `Arc`s
ergonomic.

## Pattern 3: `mark_coordinator_unknown` is a contract obligation on the response handler, NOT on the retry driver

Java's `OffsetFetchRequestState.onResponse` calls
`coordinatorRequestManager.markCoordinatorUnknown(...)` **inside the
response handler**, BEFORE completing the future exceptionally. The
retry driver (`fetchOffsetsWithRetries.whenComplete`) does NOT call
`markCoordinatorUnknown` itself — it just checks `isRetriable` and
re-enqueues.

**Why:** Putting the call in the response handler ensures the
coordinator-refresh happens regardless of whether anyone is listening
for a retry. If you only put it in the retry driver, an
unwrapped/dropped future leaves the coordinator stuck.

**How to apply:** When translating retry-loop logic that includes
"mark coordinator unknown on NotCoordinator", put the `mark_coordinator_unknown`
call in the response handler (where Java has it), NOT in the retry
loop's match arm. Use a setter-shared `Arc<CoordinatorRequestManager>`
to reach the coordinator from the response handler.

## Pattern 4: Per-attempt request-state allocation with seeded num_attempts

Java reuses the same `OffsetFetchRequestState` object across retries
via `resetFuture()`. Rust's send path consumes the state (because the
inflight list owns it until response). So each retry allocates a
fresh state, and the `RequestState.num_attempts` counter (driving the
`ExponentialBackoff`) is continued by calling `on_failed_attempt(now)`
N times via a `seed_failed_attempts(n, now)` helper.

This pattern already existed for `OffsetCommitRequestState::seed_failed_attempts`.
Adding it to `OffsetFetchRequestState` was straightforward.

**How to apply:** Whenever you translate a Java retry loop that reuses
the same state object via `resetFuture()`, allocate fresh state per
retry in Rust AND seed the backoff-counter continuity with a
`seed_failed_attempts(n, now)` helper.

## Files touched by this fix

- `src/consumer/internals/commit_request_manager.rs` — ~233 lines diff
  (mostly retry-loop rewrite + a few wiring stubs).
- `src/consumer/async_kafka_consumer.rs` — ~16 lines (the
  `set_coordinator` call site).
- `tests/integration/plaintext_consumer_assign_test.rs` — 7
  `#[ignore]` attributes removed + module-level rustdoc rewritten.

## Tests

Final verification: 8/8 KIP-848 CONSUMER-arm tests pass under
`cargo test --features integration-tests --test integration
plaintext_consumer_assign_test -- --test-threads=1`. Two back-to-back
clean runs.
