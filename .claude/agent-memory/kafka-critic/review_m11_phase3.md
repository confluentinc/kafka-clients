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

## Pass 2 — reviewing the fix (three more heuristics)

The four new methods were all correct; every pass-2 defect was in a *record* the
fix had just written. Where to look next time:

5. **When a fix adds items to an enumerated list, grep for every prose statement
   of the count.** Deviation 7's table grew from ten rows to thirteen and its
   parenthetical said so, but three separate "ten"s survived (PLAN heading, PLAN
   §Phase-4 row, struct doc). Same shape as the M8 "doc half-correction trap"
   note: a count updated in one place is the default failure. Also re-verify
   line citations the fix *copied from the Critic's own comment* — I mis-cited
   `TransactionManager.java:1420` for the `synchronized` block (it is `:1421`,
   as the rules file and PLAN §6.5 both already said) and the Actor propagated
   it into two more documents.

6. **A justification that names a mechanism is a checkable claim.** `close` was
   defended as preventing a hanging future. On the idempotent path nothing ever
   holds the `InitProducerId`'s `TransactionalRequestResult` — it is created
   inside `bumpIdempotentEpochAndResetIdIfNeeded`, never returned, and the only
   method that hands one to a caller is `ensureTransactional`-guarded. Grep for
   an awaiter (`await_result`) before accepting "otherwise a future hangs".
   Scope pulled in on a nonexistent mechanism reads as scope creep next time,
   even when the placement is right.

7. **A test harness that a PLAN row calls "the reference" is production code for
   review purposes.** `run_sender_transaction_phase` models `Sender.runOnce`'s
   guards but omits `:333` (`if (maybeSendAndPollTransactionalRequest()) return;`)
   while claiming only two steps are skipped. That guard matters:
   `maybeSendAndPollTransactionalRequest` has exactly one `return false`
   (`Sender.java:474`, empty queue), so enqueueing an `InitProducerId` at `:331`
   guarantees `runOnce` returns at `:334` and never reaches `sendProducerData` at
   `:343-345`. Check harness completeness claims by counting the Java statements
   in the cited range, and check invented error codes in a harness against the
   crate's stated convention (bare `KafkaException` → `Errors::UnknownServerError`,
   per `maybe_fail_with_error`) — a harness value nothing asserts against Java is
   the one that gets copied.

## Pass 3 — the replacement justification was also false

Only one finding, and it is heuristic 6 recurring: `close`'s hanging-future
reason was retracted correctly and **replaced** with "`FATAL_ERROR` stops
`Sender.runOnce` at `:318` before a new producer id can be requested", which is
refuted by the twenty lines around the call site the same sentence cites.
`transactionManager.close()` (`Sender.java:292`) sits inside `if (forceClose)` at
`:287`, *after* all three `runOnce` loops (`:245`, `:258`, `:267`), followed only
by `client.close()` at `:298` — and the two post-shutdown loops are themselves
`!forceClose`-guarded, so once `forceClose` is set no `runOnce` runs at all.
Java's comment at `:288-289` names the real intent ("wake up the threads waiting
on the futures"), i.e. the very mechanism just retracted for the idempotent path.

8. **When a fix *replaces* a retracted justification rather than deleting it,
   audit the replacement as hard as the original — harder, because the retraction
   now leans on it.** Two rounds, two false mechanisms in the same doc comment.
   The specific trap: a claim of the form "state X prevents later step Y" needs
   the *call ordering* traced, not just X and Y verified to exist. Read the whole
   enclosing method (`Sender.run`, 60 lines) rather than the cited fragment
   (`:287-293`).

   Corollary for scope decisions: when a method's only payoff is in a later
   phase, "it was unscheduled in every phase and costs twelve lines" is a
   complete and honest reason. Pressure to supply a *behavioural* reason for the
   current phase is what manufactured both false mechanisms.

**Verified sound in pass 3 (don't re-check):** the deliberately-kept single
`:1420` in PLAN deviation 7 is an inoculation note, not a survivor; `onComplete`
spans `:1406-1428` and its `synchronized` block is `:1421-1423`, covering
`handleResponse` alone. The `:333-335` predicate
`has_in_flight_request() || (has_pending_requests() && !has_error())` is the
*full* disjunction on Phase-3-reachable states, not an approximation — Java's
`nextRequest` returns null at `:900`, `:904`, `:910`, `:925`, and both omitted
paths are `isEndTxn()`-gated. Return census of
`maybeSendAndPollTransactionalRequest` confirmed: one `return false` (`:474`), six
`return true` (`:463`, `:487`, `:492`, `:497`, `:510`, `:516`).
