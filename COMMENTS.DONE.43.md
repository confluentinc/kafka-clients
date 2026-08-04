# Actor 43 — resolved Critic 43 comments (Milestone 11 Phase 3)

Critic 43's round-1 review of `ca75e95..7d7d3a7` filed three findings. All three
are resolved below. Each was verified against the Java source before being acted
on; all three were real.

---

## Issue 1 (RESOLVED) — the idempotent producer had no way out of `ABORTABLE_ERROR`

**Verdict: correct, and the more serious of the three.** Confirmed against
`Sender.java` directly: `runOnce` tests `hasAbortableError()` at `:325` and calls
`shouldHandleAuthorizationError(lastError)`, which at `:351-360` runs
`failPendingRequests(new AuthenticationException(exception))`,
`maybeAbortBatches(exception)`, `transitionToUninitialized(exception)` and returns
`true`. For an idempotent producer the `instanceof` test at `:352-353` is always
satisfied, because `InitProducerIdHandler`'s authorization arm (`:1524-1528`) is
the only entry to `ABORTABLE_ERROR` and its `lastError` is exactly one of the two
exceptions that test matches. Java therefore always recovers to `UNINITIALIZED`,
which is why the table admits `UNINITIALIZED ← ABORTABLE_ERROR` (`:165`).

As shipped, Phase 3 had translated the three methods that *enter* the state and
none that *leave* it, so an authorization failure produced an inescapable state in
which `maybe_add_partition` rejected every send forever.

**Fixed** in `src/producer/internals/transaction_manager.rs`:

- `transition_to_uninitialized` (Java 756) — the exit. Takes no `error`
  argument; recorded as PLAN §10.5 deviation 8 with the reason.
- `fail_pending_requests` (Java 944) — fails every queued handler with
  `abortableError`, and deliberately does not clear the queue, as Java does not.
- `authentication_failed` (Java 939) — the Critic's third method. It was
  unscheduled in **every** phase. Confirmed reachable idempotently:
  `maybeSendAndPollTransactionalRequest` takes the `coordinatorType == null`
  branch (`Sender.java:479-484`) and still calls `awaitNodeReady` →
  `NetworkClientUtils.awaitReady`, which throws `AuthenticationException`, caught
  at `Sender.java:336`. **Scheduled in Phase 3** and implemented, which is the
  "named somewhere" the finding asked for.
- `close` (Java 949) — **beyond what the finding asked**. Also unscheduled in
  every phase. It is *not* reachable from `ABORTABLE_ERROR` — it is the
  `forceClose` shutdown path at `Sender.java:287-293`, i.e. Phase 6 territory —
  but it is reachable for a purely idempotent producer, and omitting it would
  leave a pending `InitProducerId`'s `TransactionalRequestResult` never completed
  on a force close: a hanging future, which CLAUDE.md §5 forbids. Landed here on
  the same reasoning as the other three, and recorded rather than left implicit.

**Scope decision, made deliberately as the coordinator asked.** All four are
`TransactionManager` methods whose Java call sites live in `Sender` code that
Phases 4 and 6 translate, and none needs Phase-5 state beyond the
`pendingTransition` branch, which is always null for an idempotent producer for
the reason already recorded on `transitionToFatalError`. Pulling them forward
therefore costs nothing in Phase 5 and removes a shipped dead end. PLAN's Phase-5
list no longer claims `transition_to_uninitialized` (`:379`) or
`fail_pending_requests` (`:385`); §Phase-3's added-methods list and §9.15's
"Consequence for the plan" table both record the move.

**Test harness.** `run_sender_transaction_phase` now models the whole
`transactionManager != null` block of `Sender.runOnce` (`Sender.java:311-335`)
guard for guard, including `:318` (`hasFatalError`) and `:325`
(`hasAbortableError`), and returns which arm was taken. The two `Sender`-side
steps with no Phase-3 counterpart (`maybeAbortBatches`, `client.poll`) are skipped
and named in the doc comment. `should_handle_authorization_error` models the
`Sender` private predicate so Phase 4 has a reference.

**New tests.**

- `test_idempotent_producer_recovers_from_abortable_error_to_uninitialized` —
  drives an idempotent producer into `ABORTABLE_ERROR` through both authorization
  error codes, asserts `run_sender_transaction_phase` recovers rather than
  continuing, asserts `UNINITIALIZED` / `last_error() == None` / sends accepted
  again, then runs the next iteration and completes a fresh `InitProducerId` to
  `READY`.
- `test_pending_requests_are_failed_in_bulk` — the non-empty-queue case for all
  three bulk-failure methods (the recovery path reaches
  `fail_pending_requests` with an empty queue, because `on_complete` consumed the
  only handler). Needs a `#[cfg(test)]` door,
  `force_enqueue_init_producer_id_for_test`, because `enqueue_request` is private
  and `bump_idempotent_epoch_and_reset_id_if_needed` is state-guarded; Java reaches
  the same place by driving `Sender.runOnce` against a `MockClient`.
- `test_cluster_authorization_failure_...` additionally asserts that sends are
  rejected *while* in `ABORTABLE_ERROR`, so the entry and exit assertions sit next
  to each other.

**Mutation-checked.** Making `transition_to_uninitialized` a no-op fails the
recovery test. Removing the `Sender.java:325` guard from the harness fails the
same test at `bump_idempotent_epoch_and_reset_id_if_needed` with an
`ABORTABLE_ERROR → INITIALIZING` rejection — which is also the direct evidence for
Issue 2.

---

## Issue 2 (RESOLVED) — PLAN §9.15 recorded behaviour Java does not have

**Verdict: correct.** Confirmed the control flow directly in `Sender.java`:
`:325` returns at `:326` before `:331` is reached, so
`bumpIdempotentEpochAndResetIdIfNeeded` never sees an outstanding abortable error
on this path and never attempts `ABORTABLE_ERROR → INITIALIZING`. The stricken
sentence asserted "poison to `FATAL_ERROR`" where Java recovers to
`UNINITIALIZED` — the opposite outcome, on the one path the section exists to
document.

**Fixed** in PLAN §9.15. The paragraph is replaced with Java's actual behaviour,
quoting `shouldHandleAuthorizationError` and citing the
`UNINITIALIZED ← ABORTABLE_ERROR` table arm (Java 165) as the evidence, plus the
`Sender.java:348-350` comment stating the intent. The old claim is kept as an
explicit, marked **Correction** block rather than deleted silently, so a reader
who saw the earlier text can tell it was retracted and why — and the one narrow
case where a `FATAL_ERROR` outcome *would* arise (an idempotent `ABORTABLE_ERROR`
whose `lastError` is not an authorization exception, i.e. only via
`Errors.TRANSACTION_ABORTABLE`, which brokers return only for transactional
requests) is stated so the retraction is not over-broad.

§9.15's "Lesson" is now in two parts: the original mis-generalisation (entry-point
guards generalised to response handlers), and the half-applied correction that
Issue 1 found. The regression-evidence list carries all three mutations.

---

## Issue 3 (RESOLVED) — the rules §2 split is constrained by deviation 2, and the cost was unrecorded

**Verdict: correct, and the adjudication in the Actor's favour is accepted as
such** — rules §2 is not violated today, and the finding does not claim it is.
The real defect was the unrecorded cost and the wrong stated reason.

**Fixed**, taking resolution (a) of the two the finding offered:

- **PLAN §10.5 deviation 7** (new) records it, with the method table. The table
  lists **thirteen** methods, not the Critic's ten: Issue 1's additions
  (`fail_pending_requests`, `authentication_failed`, `close`) all touch
  `pending_requests` too, so fixing Issue 1 made the Phase-4 cost slightly larger
  and the record says so. It also states what "not complying" would cost —
  `on_complete` holding a lock across work Java deliberately leaves
  unsynchronized (`TransactionManager.java:1410` is outside the `synchronized`
  block that starts at `:1420`).
- **PLAN §Phase-4's table** gains a row, "Split the Sender-owned request-queue
  surface off `TransactionManager`", pointing at deviation 7 for the signatures
  and asking for it as its own commit.
- **The struct doc** now says the rule "cannot be *violated* here — but it is
  already **constrained**, and that is the operative point", names deviation 2 as
  the constraint, and points at deviation 7 and the Phase-4 row. The
  "not yet engaged" phrasing is gone.

Resolution (b) — proposing a rules §2 amendment instead — was not taken, because
the split is worth doing: it is what keeps `on_complete` out of the shared lock,
which is the specific thing Java's unsynchronized `clearInFlightCorrelationId`
call proves is intended. The Critic's alternative §2 wording is nonetheless
preserved below for the human to weigh, since it is a live option if Phase 4
disagrees.

Also fixed, from the same finding's supporting note: PLAN §Phase-4's second table
row now spells out that all four guards in `Sender.java:310-340` matter and that
`:318` / `:325` **return** before `:331`, and names
`run_sender_transaction_phase` as the reference model.

---

## Adjudications with no finding — accepted, not re-litigated

Critic 43 recorded Claims 2, 3 and 4, a method-by-method fidelity sweep, the
three-untranslated-tests check and a clause-by-clause DoD walk as sound, with
"the Actor should not change any of the following". Nothing in those sections was
touched. Two observations from them that were noted-not-filed and are left as-is
on purpose:

  - `transaction_timeout_ms()` and `api_versions()` exist only to give fields a
    reader and are strictly unnecessary under `#![allow(dead_code)]`. Both are
    documented at the site and Phase 5 gives each a real caller; removing and
    re-adding them is churn.
  - §9.15's own memory note inherited Issue 1's incompleteness. Amended — see
    below.

`.claude/agent-memory/actor-executor/phase3_transaction_manager_notes.md` lesson 1
now carries the second half of the reachability rule ("enumerate the exits, not
only the entries"), since the note is the durable artefact and the Critic
specifically asked for it.

---

## Suggested rule updates — preserved for the `agent-roles.md` §2 process

**Not applied.** Per `agent-roles.md`, changes to `CLAUDE.md` and the files under
`.claude/rules/` go through the human review process; they are recorded here for
acceptance or rejection, exactly as Critic 42's CLAUDE.md "Source Reference"
suggestion was recorded in `COMMENTS.DONE.42.md`. Both are Critic 43's wording,
verbatim, with the Actor's position appended.

### 1. `.claude/rules/producer-transactions.md` §2 — phasing guidance

Critic 43 proposes adding a How-to-apply bullet:

> When a phase introduces the manager before the lock exists, the Sender-owned
> fields may live on `TransactionManager` provisionally — but the commit MUST
> record it as a deviation per `definition-of-done.md` §7 and name the methods a
> later phase has to reshape. "The rule is not yet engaged" is not a sufficient
> rationale: hosting `onComplete` on the manager already constrains the split.

with the alternative of narrowing §2 to the four `volatile`-adjacent fields and
dropping `pendingRequests` from the Sender-owned list.

**Actor 43's position:** support the bullet as written; it is exactly the
discipline Issue 3 found missing, and Phase 3 now satisfies it retroactively via
§10.5 deviation 7. Do **not** take the narrowing alternative: `pendingRequests` is
where the confinement evidence is sharpest, since
`clearInFlightCorrelationId` is called from `onComplete` at
`TransactionManager.java:1410`, outside the `synchronized` block that starts at
`:1420`, and `lookupCoordinator(TxnRequestHandler)` (`:969`) mutates the queue
from `Sender.java:522` with no synchronization at all.

### 2. `.claude/rules/definition-of-done.md` — reachability corrections

Critic 43 proposes adding a clause:

> When a phase discovers that a state, method, or code path previously believed
> unreachable is in fact reachable, the correction MUST enumerate **every** method
> reachable from the newly-reachable state — entering it and leaving it — and
> re-schedule each one explicitly. A correction that adds only the entry methods
> leaves the client with a state it can enter and not exit.

**Actor 43's position:** support, unreservedly. This is precisely the mistake
Phase 3 made, it is the second instance of a partially-applied correction in this
milestone (§9.8 being the first), and a DoD clause is the only artefact every
subsequent pass reads. One suggested strengthening for the human to consider: add
"…and the correction MUST be pinned by a test that fails if the exit path is
removed", since the entry-only version of Phase 3 was fully green.

---

## Verification after the fixes

`cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` and
`make verify-sandbox` all exit 0. Exit codes captured without pipes. `make verify`
still cannot complete on this machine (`build-python`'s C extension includes
`<threads.h>`, absent from the Apple SDK; exit 2) — pre-existing and unrelated.

## Nothing rejected

No part of any finding is disputed. Findings 1 and 2 were re-verified against
`Sender.java` before being acted on; finding 3's adjudication (no rules §2
violation today) is accepted, and its narrower defect is fixed.
