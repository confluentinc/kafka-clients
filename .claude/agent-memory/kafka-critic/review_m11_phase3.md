---
name: review-m11-phase3
description: M11 Phase 3 (TransactionManager idempotence core) review — partially-applied reachability corrections, deferred lock-topology rules, Sender.runOnce guard ordering
metadata:
  type: project
---

Critic 43 pass on `ca75e95..7d7d3a7`. Three findings; Actor's four self-reported
claims were all sound on independent re-derivation.

**Why:** the Actor discovered `ABORTABLE_ERROR` is reachable without a
`transactionalId` and amended the PLAN. The correction was right but incomplete,
which is the reusable lesson.

**How to apply — four heuristics:**

1. **A partially-applied reachability correction is the highest-yield thing to
   look for.** When an Actor reports "state X turned out to be reachable", check
   the methods that *leave* X, not only the ones that enter it. Phase 3 added the
   three entry methods (`transitionToAbortableError`, `hasError`,
   `hasAbortableError`) and missed both exit methods
   (`TransactionManager.java:756 transitionToUninitialized`, `:944
   failPendingRequests`), leaving a state the client can enter and never leave.
   The transition table itself is the tell: an arm like
   `UNINITIALIZED ← ABORTABLE_ERROR` exists *because* some method performs it —
   grep for that method.

2. **Guard ordering inside `Sender.runOnce` changes which manager methods are
   reachable.** `Sender.java:318` (`hasFatalError`) and `:325`
   (`hasAbortableError && shouldHandleAuthorizationError`) both `return` before
   `:331` (`bumpIdempotentEpochAndResetIdIfNeeded`). Any claim of the form "then
   the next runOnce calls M and it fails" must be checked against the earlier
   guards — Phase 3's PLAN §9.15 asserted a `FATAL_ERROR` poisoning that `:325`
   makes unreachable. Test harnesses that model "the two calls runOnce makes"
   routinely drop these guards; harmless until a test drives an error state.

3. **"The rule is not yet engaged" is worth probing, but usually resolves to an
   unrecorded deviation rather than a violation.** Rules §2 (lock topology) could
   not literally be applied in Phase 3 (no mutex exists yet, and the Sender has
   no manager until Phase 4), so it was not violated. The real defect: moving
   `onComplete`/`handleResponse` onto the manager made every touch of
   `pending_requests` / `in_flight_request_correlation_id` a
   `&mut TransactionManager` method, so Phase 4 compliance now costs ~10 reshaped
   signatures — and that cost appeared in neither PLAN §10.5 (deviations) nor the
   Phase-4 table. File the unrecorded cost, not a phantom violation. Precedent:
   `COMMENTS.FP.md` accepted exactly this half of Critic 41's finding.

4. **Java's `if (A) return true; else if (B) {…return true;}` flattens safely.**
   Don't report a Rust translation that turns it into sequential `if`s as a
   branch-order change — verified for `canRetry` (`TransactionManager.java:1015`).
   Conversely, DO check that `instanceof` subtyping was preserved: rules §9's
   `UnknownProducerIdException extends OutOfOrderSequenceException` relation must
   make the first arm match *both* wire codes.

**Verified-sound patterns (don't re-flag):** `Caller::Sender` hardcoded at
`on_complete` / `fatal_error` / `abortable_error` is correct even at full scope —
`authenticationFailed`, `failPendingRequests` and `close` are all called only
from `Sender.java:292/339/354`. A queued partition with no in-flight batches must
get an **empty slice**, never be skipped: Java's throw comes from a missing
*entry* (`TxnPartitionMap.java:143`), and `testProducerIdReset`
(`TransactionManagerTest.java:863`) pins the sequence rewind. `VecDeque` for
Java's `PriorityQueue` is safe while only `InitProducerId` can be enqueued.
