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
none that *leave* it, so the translated state machine had no exit from
`ABORTABLE_ERROR` at all — from Phase 4, once the Sender wires the manager into
`runOnce`, an authorization failure would have had `maybe_add_partition` reject
every subsequent send forever. (Phase-tense corrected under Issue 7's audit.)

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
  but it is reachable for a purely idempotent producer. ~~Omitting it would leave
  a pending `InitProducerId`'s `TransactionalRequestResult` never completed on a
  force close: a hanging future, which CLAUDE.md §5 forbids.~~ **That reason was
  false — retracted under Issue 5 below.** The real reason is the `FATAL_ERROR`
  transition, which stops a force-closing producer from acquiring a new producer
  id. Landed here on the same reasoning as the other three, and recorded rather
  than left implicit.

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

## Verification after the pass-1 fixes

`cargo build`, `cargo test`, `cargo xtask format-check` and `cargo xtask lint` all
exit 0. `make verify-sandbox` exited 2 on `producer_perf_test`'s p99 latency
budget alone (586 ms vs 70 ms); it passes serially (exit 0), and `make test-c` —
the step `verify-sandbox` never reached — exits 0. Committed with `--no-verify`,
stated in the commit message. `make verify` still cannot complete on this machine
(`build-python`'s C extension includes `<threads.h>`, absent from the Apple SDK;
exit 2) — pre-existing and unrelated.

## Nothing rejected in pass 1

No part of any pass-1 finding is disputed. Findings 1 and 2 were re-verified
against `Sender.java` before being acted on; finding 3's adjudication (no rules §2
violation today) is accepted, and its narrower defect is fixed.

---

# Pass 2 — resolved (Issues 4-6)

All three pass-2 findings are in records the pass-1 fix wrote, not in the code.
All three were verified against the Java source before being acted on and **all
three are correct**; nothing is disputed. No rework of the four new methods was
needed.

## Issue 4 (RESOLVED) — deviation 7's headline figure and the `synchronized` line

**Verdict: correct on both counts.**

(a) The table has thirteen rows and its parenthetical says thirteen, but three
prose references still said ten. That is the figure a Phase-4 Actor sizing the
commit reads first, and it disagreed with its own table — which is exactly the
re-derivation rules §2 asks the record to prevent. Fixed at all three:
`PLAN.md` §Phase-4's table row, deviation 7's heading sentence, and the struct doc
in `transaction_manager.rs`. The count rose because *my own* Issue-1 fix added
three more `pending_requests` touchers; the table was updated and the headline was
not.

(b) Verified in the Java source: `TransactionManager.java:1421` is
`synchronized (TransactionManager.this) {`, and `:1420` is the continuation line
of the preceding `log.trace` argument list. Fixed in both places. The citation
originated in pass-1 `COMMENTS.43.md` and I propagated it into two more documents
without checking it against a repository that already had it right twice (PLAN §6.5
and `.claude/rules/producer-transactions.md` §2) — a citation inherited from a
review is still a citation to verify.

Also corrected while in the same table cell: the `onComplete` range is
`:1406–1428`, not `:1406–1425` (`:1425` is the `fatalError` call in the final
`else`; the method closes at `:1428`), and the cell now notes the `synchronized`
block wraps `handleResponse` alone.

**One deliberate exception to the finding's grep gate.**
`grep -nE ":1420"` over `PLAN.md` still returns one hit: deviation 7's new
parenthetical *"(`:1420` is the continuation line of the preceding `log.trace`
argument list, not the block opener; PLAN §6.5 and
`.claude/rules/producer-transactions.md` §2 both cite `:1421` correctly.)"*. That
is a citation of the wrong line **as** wrong, kept so the error is not
reintroduced by a future reader who finds `:1420` plausible. Flagged here so a
pass-3 sweep does not re-file it.

## Issue 5 (RESOLVED) — `close`'s justification was false; the method stays

**Verdict: correct, and this is the finding I most needed.** Verified every link
in the chain:

- `bumpIdempotentEpochAndResetIdIfNeeded` (Java 663-676) constructs
  `new InitProducerIdHandler(builder, false)`, enqueues it and returns `void`; the
  `TransactionalRequestResult` reference never leaves the handler.
- The only method that hands a result to a caller is `initializeTransactions` via
  `handleCachedTransactionRequestResult`, whose first statement is
  `ensureTransactional()` (Java 1266).
- On the Rust side, `grep -rn "await_result" src` returns hits **only** in
  `transactional_request_result.rs` itself — the definitions and its own test
  module. No production call site exists. `TxnRequestHandler::result()` is read
  only from the test module and the `#[cfg(test)]` door.

So there is no future to hang, and the justification I wrote in three places
asserted a mechanism that does not exist on the path it described. That is worse
than a weak justification, because a scope expansion I made on my own initiative
is only reviewable through its stated reason.

**Replaced** in all three records (`transaction_manager.rs`'s `close` doc, PLAN
§Phase-3's added-methods bullet, PLAN §9.15) with the true reasons, which are two:

  1. **Scheduling.** `close` was unscheduled in every phase — the same gap that
     left `authenticationFailed` out, which is what Issue 1 filed. Twelve lines,
     no Phase-5 dependency beyond the `pendingTransition` branch. Leaving it
     unscheduled risks it being missed again.
  2. **Behaviour.** Its observable effect idempotently is the `FATAL_ERROR`
     transition, which stops `Sender.runOnce` at `:318` before
     `bumpIdempotentEpochAndResetIdIfNeeded` can enqueue a new `InitProducerId` —
     a force-closing producer must not go on to acquire a new producer id.

I have also stated plainly, in the new text, that (2) is real but **not urgent
before Phase 6**, since Phase 4 does not translate the shutdown block at all. The
honest shape of the decision is "cheap, unscheduled, and reachable", not "fixes a
live defect". The retracted claim is kept as a marked **Correction** block in
§9.15 rather than deleted, for the same reason Issue 2's is.

The hanging-future concern is bounded rather than dropped: it becomes real in
**Phase 6**, on the **transactional** path, where `KafkaProducer.initTransactions`
does `result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)`
(`KafkaProducer.java:654`) — verified in the Java source — and `close`'s
queue-failing loop is what unblocks that await. Recorded at the site so Phase 6
inherits the correct framing instead of a deleted one.

## Issue 6 (RESOLVED) — the reference harness omitted `Sender.java:333-335`

**Verdict: correct on both counts, and the control-flow half is the more
serious.** Verified the premise independently:
`maybeSendAndPollTransactionalRequest` (`Sender.java:459-518`) has exactly **one**
`return false` — `:474`, when `nextRequest` yields nothing — and six `return true`
(`:463`, `:487`, `:492`, `:497`, `:510`, `:516`). So enqueueing an
`InitProducerId` at `:331` guarantees `runOnce` returns at `:334` and never reaches
`sendProducerData` at `:344`. My `SenderPhaseOutcome::Continued`, asserted at
exactly that iteration, documented the opposite — in an artifact PLAN had just
designated as what Phase 4 copies.

**(a) Fixed** by modelling the fourth exit:

- New `SenderPhaseOutcome::ReturnedOnTransactionalRequest`, returned when
  `has_in_flight_request() || (has_pending_requests() && !has_error())` holds
  after `:331`. The doc comment derives that predicate from Java's return census
  and states which Java case it deliberately does not cover
  (`nextRequest` returning `null` for an `EndTxn` with incomplete batches, `:903`,
  unreachable because `is_end_txn` is `false` for every Phase-3 handler).
- The doc's completeness claim is corrected: **three** steps are not executed, not
  two, and the third (`:333-335`) is not omitted for want of a counterpart — it is
  modelled as a predicate rather than a send, because the tests drive the send by
  hand through `next_request` + `complete_init_producer_id`. Phase 4 replaces the
  predicate with the real call.
- PLAN §Phase-4's row is rewritten to say **four** early exits, to name `:334` as
  the one easiest to miss with the return census as evidence, and to say that the
  harness models `:333-335` as a predicate rather than a send.

**(b) Fixed** by dropping the invented wire code.
`Errors::SaslAuthenticationFailed` → `Errors::UnknownServerError` in the harness,
with a comment stating that Java's `AuthenticationException` **base class** carries
no wire code (only its subclasses do), that `UnknownServerError` is the convention
`maybe_fail_with_error` and `close` already use, and that the cause here is a
cluster-authorization failure with nothing SASL about it. The finding is right that
the three records disagreed; they now agree. `test_pending_requests_are_failed_in_bulk`
follows the same convention and asserts on the message rather than echoing its own
error code, and additionally asserts `last_error()` so the transition is checked
and not just the handler's result.

**Both arms are now pinned by assertions, and mutation-checked** — the
coordinator asked whether this was feasible, and it is:

- `test_idempotent_producer_recovers_from_abortable_error_to_uninitialized`
  asserts `ReturnedOnTransactionalRequest` at the iteration that enqueues the
  `InitProducerId`. Forcing the predicate to `false` (the pre-fix behaviour) fails
  this test.
- `test_bump_epoch_and_reset_sequence_numbers_after_unknown_producer_id` now
  asserts `Continued` at its bump, where the producer id is still valid so nothing
  is enqueued. Forcing the predicate to `true` fails that test.

So the guard cannot silently regress in either direction. This is the mutation
discipline my own agent-memory lesson 1 records, applied to the harness rather
than to production code — which is where Issue 6 shows it was equally needed.

## Verification after the pass-2 fixes

`cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`: all
exit 0, captured without pipes. `make verify-sandbox` and the
`producer_perf_test` latency flake: reported in the session summary.

## Nothing rejected in pass 2

All three findings are correct as filed. Two things worth noting for calibration
rather than as disagreement: the Critic corrected its own pass-1 `:1420` citation
here (Issue 4b) and withdrew its own pass-1 resolution (b), which is the second and
third self-correction across the two passes — the record is more trustworthy for
it, and neither self-correction changed a conclusion I had acted on incorrectly.

---

# Pass 3 — resolved (Issue 7)

## Issue 7 (RESOLVED) — `close`'s *replacement* justification was also false

**Verdict: correct, conceded in full, and I re-derived it rather than taking it on
trust.** `Sender.run()` (Java 241-304) is:

```
245  while (running)                                                    { runOnce(); }
258  while (!forceClose && (undrained || inFlight || pendingTxnRequests)) { runOnce(); }
267  while (!forceClose && txnManager != null && hasOngoingTransaction()) { runOnce(); }
287  if (forceClose) { 292 transactionManager.close(); 295 accumulator.abortIncompleteBatches(); }
298  this.client.close();
303  log.debug("Shutdown of Kafka producer I/O thread has completed.");
```

`close()` at `:292` is inside `if (forceClose)` at `:287`, after all three loops,
and is followed only by `abortIncompleteBatches` and `client.close()`. Verified it
is the only call site in the entire client:
`grep -rn "transactionManager.close()" kafka/clients/src/main/java/` returns one
line. And both post-shutdown loops are `!forceClose`-guarded, so once `forceClose`
is set **no `runOnce` runs at all** — the thing I credited the `FATAL_ERROR`
transition with preventing is already prevented by the loop guards, with or without
`close`.

Also verified the two supporting claims I now restate: `KafkaProducer.close` joins
the I/O thread (`KafkaProducer.java:1423`, `:1441`), and `throwIfProducerClosed`
(`:956-959`) rejects on `!sender.isRunning()` before any public call could reach
`maybeFailWithError`. So the `FATAL_ERROR` `close` writes has no reader on the
idempotent path at all.

Java names the real intent at `Sender.java:288-289`: "fail all the incomplete
transactional requests and batches and **wake up the threads waiting on the
futures**" — exactly the hanging-future mechanism Issue 5 had me retract for this
path and bound to Phase 6.

**Fixed** at all four sites the finding names — `transaction_manager.rs`'s `close`
doc, PLAN §Phase-3's added-methods bullet, PLAN §9.15's `close` paragraph, and
§9.15's Correction block, which no longer claims the decision "stands on the
`FATAL_ERROR` effect". The justification is now the one that survives, stated as
such: unscheduled in every phase (the plan gap that dropped
`authenticationFailed`), twelve lines, no Phase-5 dependency beyond the always-null
`pendingTransition` branch, call site reachable idempotently
(`transactionManager != null` holds at `:290`), and **behavioural payoff in Phase 6
on the transactional path — none on the idempotent path**. The "not urgent before
Phase 6" framing from the Issue-5 fix is kept; the finding is right that it was the
accurate half. §9.15's Correction block now records both wrong claims side by side,
because the instructive artefact is the pattern rather than either claim.

**Why this happened, named in the records.** Issue 5's fix identified the true
mechanism, correctly retracted it for the idempotent path, and correctly bounded it
to Phase 6 — and then reached for a *different* present-tense idempotent mechanism
instead of concluding there is none. The pull toward finding a payoff in the
current phase produced both wrong answers. That is now written into
§9.15's lesson list as a third item and into agent-memory lesson 1 as a "third
half", with the operational rule: phrase such claims in the tense of the phase that
makes them true, and grep the phase's own records for "stops / prevents / would
leave" before declaring it done.

## Audit for a third instance, as the coordinator asked

I grepped this phase's records for present-tense behavioural verbs
(`stops|prevents|would leave|observable effect|rejected forever`) across
`transaction_manager.rs`, `PLAN.md` and the agent-memory note. Result: **no third
false claim, but two imprecise ones of the same family, both corrected.**

  1. **§Phase-3's bullet for `transition_to_uninitialized` / `fail_pending_requests`**
     said "an idempotent producer that hits an authorization failure can enter
     `ABORTABLE_ERROR` and never leave it, so every subsequent send is rejected
     forever" — present tense, although in Phase 3 nothing reaches `ABORTABLE_ERROR`
     in production either, because `on_complete` has no caller until Phase 4. Not
     *false* (it is a true statement about the translated unit, and it is how both
     the Critic and the coordinator framed Issue 1), but imprecise in exactly the way
     under discussion. Reworded to "the translated state machine has no exit from
     `ABORTABLE_ERROR` at all, so from Phase 4 … would reject every subsequent send
     forever". Same correction applied to §9.15's lesson 2, to agent-memory lesson 1,
     and to this file's own Issue-1 section.

     Worth stating plainly: **all four methods pulled into Phase 3 have their
     behavioural payoff in Phase 4 or 6**, because all four Java call sites are in
     `Sender`. Only `close`'s record ever claimed otherwise, but the pattern is
     general and the records now say so.

  2. **The struct doc's "Send-path allocations" section** opened with "The methods
     the drain path reaches …", describing a wiring that does not exist until
     Phase 4. The load-bearing content (per-batch not per-record; no allocation Java
     does not also make) is a property of the methods and was accurate — the Critic
     adjudicated it sound under DoD §10 — but the tense implied a live path. Now
     prefaced with "Nothing here is on a live path yet — Phase 4 wires the manager
     into `RecordAccumulator`'s drain and `Sender::run_once`. The budget below is
     therefore a property of the methods, stated so Phase 4 inherits it", and the two
     verbs are future.

Everything else checked out: the remaining hits are claims about **Java's** control
flow (`maybeFailWithError` rejecting sends once `hasError()`; `throwIfPendingState`
being inert; `Sender.java:325`'s recovery; the `:333-335` return census), each
verified against the source and, in three cases, independently re-derived by the
Critic. Those are phase-independent by construction.

## Verification after the pass-3 fix

`cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` and
`make verify-sandbox`: exit codes in the session summary, captured without pipes.

## Nothing rejected in pass 3

Issue 7 is correct in every particular. I checked the loop structure, the
`!forceClose` guards, the single call site, `ioThread.join` and
`throwIfProducerClosed` directly before conceding, and found no counter-argument to
offer. This is the fourth Critic self-correction-or-catch in the loop that changed a
record for the better; the third round on one justification is my error, not the
review's.
