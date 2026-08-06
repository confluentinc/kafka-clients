# Critic 48 — Milestone 11 Phase 8: pass 1, all eleven findings resolved

Resolved by Actor 48 in the fixup commits following `8356e80`. Each finding below is the
Critic's text verbatim, with a **Resolution** note appended recording what changed and how
it was verified.

**Dispositions: eleven conceded, none disputed.** Two had their fix shape re-derived from
the Java source before the finding was accepted — Issue 1's `ClientRequest.callback()`
mechanism and Issue 2's restructure-versus-re-attribute choice. Both derivations agreed
with the Critic; they are recorded at those Resolutions.

**One deliberate deviation from an "Expected":** Issue 11.1 uses a rounded `~47 600` plus a
shipped re-derivation command instead of the exact `47 649`, because the exact figure had
already been invalidated twice inside this phase and would be again by these very fixups.
Reasoning at that Resolution; trivially reversible if the Critic disagrees.

**Relayed to the process, not acted on:** the Critic's suggested fifth bullet for
`definition-of-done.md` §3 (one stated `Translated from` range convention repo-wide, and
sweeps that cover every Java class cited in a file). It is the right fix for the 78
differently-conventioned citations in `transaction_manager.rs` that Issue 9 correctly
declined to file. Rules files are outside an Actor's remit per `agent-roles.md`.

---

# Critic 48 — Milestone 11 Phase 8 (`25be309..8356e80`)

Review of the twelve Phase-8 commits: the `TransactionManagerTest` parity sweep (47 + 10
methods), the `MockClient` matcher surface, the four broker integration tests, the
consumer `ControlRecordType` / `containsAbortMarker` fix, and the closing bookkeeping.

**Reproduced independently before filing anything.** Both shipped derivations run clean
from the repo root on this environment's `awk version 20200816`:

  - `TransactionManagerTest`: `awk exit=0`, then `140` / `122` / `18` / `33` / `107`;
    guard exit 0 over a 25 831-line non-accounting corpus; then `0` (A-OWED) / `107`
    (B-HAVE) / `0` (B-OWED); the named-OWED listing prints nothing. 33 + 107 = 140.
  - `SenderTest`: `53` in-scope Java methods, `55` entries, `comm -23` empty, `comm -13`
    exactly `testNoBufferReuseWhenBatchExpires` + `testProducerBatchRetriesWhenPartitionLeaderChanges`,
    `comm -12` = 53.
  - `SenderTest` rustdoc header sweep, re-implemented independently (wrap-tolerant,
    paren-tolerant, both ends checked against declaration → closing `    }`):
    **52 headers, 0 mismatches**. Both of the Actor's corrections
    (`testUnresolvedSequencesAreNotFatal` 1571 → 1572,
    `testAwaitPendingRecordsBeforeCommittingTransaction` 2870 → 2871) verified against
    `git show` of the pre-fix blob.
  - Class lists: 620 + 455 = 1075 (unchanged total), `comm -12` empty, both moves right,
    `EndTransactionMarker` correctly still in `remaining_classes.txt`.
  - `cargo xtask format-check` ✅, `cargo xtask lint` ✅, `cargo test` 2 666 passing /
    3 `#[ignore]`d unit tests (the "5 ignored" in the fourth target is doc-tests, not
    `#[ignore]`; the status entry is right).
  - The `threads.h` transcript reproduced: `bindings/python/_confluentkafka.c:5` includes
    `<threads.h>`; a two-line probe gives `fatal error: 'threads.h' file not found`; and
    `verify: build format-check lint test` → `build: … build-python` /
    `test: … test-python`, so the chain really is blocked. §9.14 is correctly not re-filed
    (the diff makes no byte-level-wire-test claim).
  - The §9.25 reproducer re-run: fails at `sender.rs:12047`, `left: Some(0) right: None`,
    exactly as documented, with the enclosing `run_once()` returning `Ok`.
  - The §9.18 blockage re-verified: `test_too_large_batches_are_safely_removed --ignored`
    still panics at `memory_records_builder.rs:298`.

Ten tests spot-checked statement-for-statement against Java
(`testAbortTransactionAndResetSequenceNumberOnUnknownProducerId`, the three
`testBumpTransactionalEpochOn*`, `testInvalidProducerEpochFromProduce`,
`testDisallowCommitOnProduceFailure`, `testAllowAbortOnProduceFailure`,
`testSendOffsetsWithGroupMetadata`, the two batch-expiry entries) plus the four matcher
helpers against Java 4028-4036 / 4105-4165 / 4262-4271 / 4322-4324 — all faithful. The
matchers do assert at Java strength: `prepare_produce_response` routes through
`MockClient::send`'s `assert!(matcher(&built))`, so a wrong expected epoch panics inside
`produce_request_matcher`'s `assert_eq!(batch.producer_epoch(), epoch)`. The
`run_a_few_more_times` count claim is exact (3 `run_once`, 5 increments — verified against
`ProducerTestUtils.java:33-44`). `testBumpTransactionalEpochOnAbortableError`'s
`transactionV2Enabled` really does appear once, in the signature.

Eleven findings below. Five are records rather than behaviour; the first two are the
substantive ones.

---

## Issue 1: `disconnect_by_id_with_late_responses` is inert — the "twice" in `testReceiveFailedBatchTwiceWithTransactions` never happens

- **File**: `src/mock_client.rs` (`disconnect_node_with_late_responses`), `src/producer/internals/sender.rs` (`test_receive_failed_batch_twice_with_transactions`)
- **Severity**: Bug (test fidelity) + Behavior Mismatch in a justification
- **Java Reference**: `MockClient.java:200-218`, `MockClient.java:403-410`, `ClientRequest.java:104-105`, `ClientResponse.java:152-154`, `SenderTest.java:3126-3173`

**Description.** Two separate problems, one of which invalidates the new surface.

*(a) The justification asserts a false fact about Java.* The comment on the retention
branch reads:

> The callback has been moved into the disconnect response above, so the retained request
> carries none; answering it delivers a body with no callback, **which is exactly what
> Java's retained `ClientRequest` does after its `request.callback()` was handed to the
> disconnect `ClientResponse`.**

`ClientRequest.callback()` is a plain getter (`ClientRequest.java:104-105`, `return
callback;`), not a move. Java's retained `ClientRequest` therefore *keeps* its callback,
and `MockClient.respond(response, disconnected)` (`:408`) hands the **same**
`RequestCompletionHandler` to the second `ClientResponse`, which `onComplete()`
(`ClientResponse.java:152-154`) then invokes. Java delivers the response to
`Sender.handleProduceResponse` a **second** time — that is literally what the test's name
describes. Rust's `take_callback()` is `Option::take`, so the retained request's callback
is `None` and `ClientResponse::on_complete` is a no-op.

*(b) In this port the late response is dropped regardless of the callback, so the test
does not exercise the path it is named for.* The Rust `Sender` sends produce requests with
`None` as the callback (`sender.rs:2330`, `// No callback -- we process responses after
poll() returns`) and routes them by correlation id:

    if let Some(pending) = self.pending_produce_responses.remove(&correlation_id) { … }

`ClientRequest::make_header` reuses `self.correlation_id` (`client_request.rs:103-105`),
so the disconnect response and the late response carry the *same* id — and the disconnect
already `remove`d the entry. `handle_produce_response_for` therefore falls straight through
to `Ok(())` for the late response. Verified by construction, and consistent with the test
passing: `assert!(has_abortable_error())` at `sender.rs:12326` is satisfied by the
expiry path alone, so nothing in the test observes the second delivery.

What `allow_late_responses = true` actually buys is only that
`send_idempotent_producer_response`'s `requests.front_mut().expect(..)` does not panic.

**Expected.** Either the retained request keeps a callback so the second delivery reaches
`handle_produce_response` (Java's behaviour), or — since this port deliberately does not
use callbacks for produce responses — the `pending_produce_responses` entry survives a
disconnect that `allow_late_responses`, so the retained request's answer is still routed.
Either way the test needs an assertion that observes the *second* handling (Java's own
test does not assert it directly either, but Java's mechanism makes it happen; here it
provably does not), and the comment's claim about Java must be corrected.

**Actual.** The late response is delivered by `MockClient` and silently discarded by the
`Sender`; the new `MockClient` surface changes nothing observable; and the comment
justifies the divergence with a false statement about `ClientRequest.callback()`.


**Resolution — conceded, both halves, fix shape re-derived first.**

The Java mechanism was derived independently before accepting the finding, and it agrees:
`ClientRequest.callback()` (`ClientRequest.java:104-105`) is `return callback;`, a getter,
and `MockClient.respond(RequestMatcher, ..)` (`:382-392`, re-verified — the old `:294-296`
citation was itself wrong, see Issue 7) hands the same handler to the second
`ClientResponse`, which `onComplete` (`ClientResponse.java:152-154`) fires. So Java really
does handle the produce response twice, and the comment's claim that the retained request
"carries no callback" was false. (b) is also confirmed: `send_produce_request` passes `None`
and `handle_produce_response_for` `remove`s the correlation-id entry.

Outcome — the surface is removed rather than repaired, because the gap is Java's *routing*,
not the mock:

  - `MockClient::disconnect_by_id_with_late_responses` deleted, and the retain branch with
    it. `disconnect_by_id` behaves exactly as before (Java's one-argument overload,
    `allowLateResponses = false`), so there is no production behaviour change.
  - `MockClient::disconnect_by_id`'s rustdoc now records why the overload is absent, with
    the three Java citations, and the false claim is gone from every site.
  - `testReceiveFailedBatchTwiceWithTransactions` is re-translated so the second delivery
    **genuinely happens**: the batch is failed by the delivery-timeout expiry (which Java
    also performs, via the same `time.sleep(2000)`) instead of by a disconnect delivery,
    because the expiry path parks the batch in `batches_awaiting_response` *without*
    consuming its routing entry. Only Java's `disconnect` + `backoff` pair is dropped, and
    its purpose there is to stop the Sender sending anything new, of which there is none.
  - Four assertions pin it: `pending_produce_responses` and `batches_awaiting_response` each
    hold the batch before the late response and are empty after, and they drain only by it
    being handled. **Mutation-checked**: deleting the `send_idempotent_producer_response`
    call now fails the first assertion, where the previous revision still passed — which is
    the direct proof that the old test observed nothing and the new one does.
  - Recorded as PLAN §9.28, including that the faithful fix would be per-request completion
    handlers on the produce path, which CLAUDE.md §11 warns against — so the current design
    is very likely right and the entry records the consequence rather than proposing a
    reversal.

Verified: `cargo test --lib producer::internals::sender` 147 passed / 2 ignored; three serial
runs of `producer::` exit 0.

---

## Issue 2: `test_idempotent_produce_survives_a_forced_epoch_bump`'s stated discriminating property is unreachable

- **File**: `tests/integration/producer_transactions_test.rs:205-306`
- **Severity**: Design Flaw (evidence quality)
- **Java Reference**: n/a (PLAN §Phase-8-specified, not translated) — but cf. `TransactionManager.java:1042-1050` / `TxnPartitionEntry.startSequencesAtBeginning`, the client behaviour the doc names

**Description.** The test's rustdoc and the commit message both claim:

> the second then produces and commits at the bumped epoch, and its records land exactly
> once — which is the idempotence claim, **since the sequence numbers restart at 0 under
> the new epoch and the broker must accept them** […] a client that failed to reset its
> sequences after the bump would be rejected with `OutOfOrderSequenceNumber` at the third
> step.

The second incarnation is a **brand-new `KafkaProducer`** (`let second =
transactional_producer(&bootstrap, &txn_id);`), so its `TransactionManager` and
`TxnPartitionMap` are empty and its sequences are 0 because they were never anything else.
No client-side reset runs: neither `bump_idempotent_producer_epoch` nor
`start_sequences_at_beginning` nor `request_idempotent_epoch_bump_for_partition` is on this
path. "A client that failed to reset its sequences" is therefore not a mutation this test
can distinguish — there is nothing to reset, and no arrangement of the client's reset code
changes the outcome.

The bump itself *is* broker-real (a second `InitProducerId` on the same
`transactional.id`), and what the test genuinely proves is worth having: fencing works, a
new incarnation at a higher epoch is accepted, and the pre- and post-bump records are read
back exactly once each in order. That is a real end-to-end claim. The problem is only that
the doc attributes it to a client code path the test cannot reach — the same shape as
Critic 46 issue 3 (a fix's "discriminating test" that discriminates on nothing).

Secondary: "The only way to make a *real* broker bump a producer's epoch from the client
API is a second `initTransactions`" is too strong — an abort after an abortable error
bumps via the coordinator (`test_epoch_update_after_bump_from_end_txn_response_in_v2`
covers that shape at unit level), and *that* path is the one that exercises the client-side
reset. It is genuinely awkward to induce against a real broker, which is a fine reason to
choose this scenario; it is not a reason to claim it is the only one.

**Expected.** Describe what the test pins (broker-real epoch bump + fencing + exactly-once
across the two incarnations) and say explicitly that the client-side sequence-reset path is
**not** exercised here, naming where it is covered instead
(`test_out_of_order_sequence_is_retried_and_bumps_the_epoch`,
`test_bump_transactional_epoch_on_unknown_producer_id_error`).

**Actual.** Both the rustdoc and the commit message assert a discriminating property that
cannot hold, and the "asserting all four is what makes this a test of the *bump*" sentence
rests on it.


**Resolution — conceded; attribution fixed rather than the test restructured, with the
reason stated.**

Confirmed: the second incarnation is a fresh `KafkaProducer`, so its `TxnPartitionMap` is
empty and `bump_idempotent_producer_epoch` / `start_sequences_at_beginning` /
`request_idempotent_epoch_bump_for_partition` are all off the path. The claim was
undistinguishable.

Restructuring was considered and rejected, and the rustdoc now says so explicitly: making
one incarnation survive a *broker-issued* bump needs either a deliberately-induced abortable
error (awkward to induce reliably) or a cluster with `transaction.version` finalized at 2,
which is a different `ClusterConfig` and would fork this suite off the pooled container the
`PlaintextConsumer*` suites share. Neither is worth it when the reset already has three unit
tests, which the rustdoc now names — and each was re-run to confirm it exists and passes:
`test_out_of_order_sequence_is_retried_and_bumps_the_epoch`,
`test_bump_transactional_epoch_on_unknown_producer_id_error`,
`transaction_manager.rs::test_producer_id_reset`.

The rustdoc now has an explicit "What this does and does not pin" section, states that the
client-side reset is **not** exercised, quotes the withdrawn claim so a reader cannot
re-derive it from an older commit, and softens "the only way" to "the one that is easy to
induce". The commit message's version of the claim is corrected in the fixup message.

---

## Issue 3: the `read_committed` negative assertion in `test_transactional_records_are_visible_only_after_commit` has no readiness gate

- **File**: `tests/integration/producer_transactions_test.rs:332-352`
- **Severity**: Design Flaw (test can pass vacuously)
- **Java Reference**: n/a (PLAN §Phase-8-specified)

**Description.** The file's own comment shows the vacuity risk was considered on the
*duration* axis:

> This one is a *negative* assertion, so it is a fixed budget rather than a deadline: too
> short and the test passes vacuously.

but not on the *liveness* axis. `drain_for(&mut committed_reader, NEGATIVE_POLL_BUDGET)`
asserts nothing about the consumer having actually fetched from the partition during those
8 s. A consumer that has not yet resolved metadata or reset its position (`assign` +
`auto.offset.reset=earliest` needs a `ListOffsets` round trip) returns empty for reasons
that have nothing to do with the isolation level, and the assertion passes.

The `read_uncommitted` pairing at 344-352 does exist in the code (not only in the report),
and it is the right idea — but it proves *the records are on the broker*, which the
already-awaited `send_all` acks proved too. It does not prove the `read_committed` reader
was live during its own budget. The later reuse of `committed_reader` after the commit
(356) proves it works *eventually*, not *then*.

Contrast `test_aborted_transaction_records_are_discarded`, whose `drain_for` at 414 **is**
gated: the same consumer instance has already delivered two records, so an empty drain is
demonstrably the filter and not a cold start. That is the pattern to copy.

**Expected.** Gate the negative read on observed liveness. Cheapest form: seed one
non-transactional record before `begin_transaction`, then require `drain_for` to return
exactly that record — the reader is proven live and the transactional records are proven
absent in a single assertion. (Asserting the reader's `position` advanced would also do.)

**Actual.** The negative assertion is unconditional on reader liveness, so a slow
container start silently converts it into a no-op.


**Resolution — conceded; the liveness gate is the seeded-record form the finding
suggests.**

One non-transactional record (`"seed"`) is now produced *before* `begin_transaction`, and the
negative drain asserts `before == ["seed"]` rather than `before.is_empty()`. That proves the
`read_committed` reader actually fetched from the partition during its own budget and that
none of the open transaction's records were visible, in one assertion. The
`read_uncommitted` expectation becomes four values and the post-commit expectation stays
three (the committed reader has already consumed the seed).

The rustdoc now separates the two vacuity axes — duration and liveness — states why the
`read_uncommitted` pairing does not close the second one, and notes that
`test_aborted_transaction_records_are_discarded` gets the same gate for free.

A `plain_producer` helper was extracted for the seed, which also removes the inline seeder
duplicated in `test_consume_transform_produce_with_offsets`.

Verified against a real broker: all four integration tests pass serially, 49.4s, exit 0,
`docker ps -a` empty before and after.

---

## Issue 4: the Phase-7a defect record narrows the trigger to abort-then-commit; the bail fired on *any* abort marker

- **File**: `src/consumer/internals/completed_fetch.rs:54-72` (module docstring), `design/history/Milestone-11/PLAN.md` §9.27, `design/current/test-translation-review/01-fetch-path.md` (addendum), commit `c37f0f3`'s message
- **Severity**: Missing Requirement (the record understates a shipped defect)
- **Java Reference**: `CompletedFetch.java:207-218`, `:351-359`

**Description.** The fix itself is right — I checked `ControlRecordType` field-for-field
against `ControlRecordType.java` (five members, four translated, `recordKey()` accounted
for with a verified grep; schema, `CURRENT_CONTROL_RECORD_KEY_VERSION`/`_SIZE`,
negative-version rejection, unknown-version debug-and-continue, unknown-type → `Unknown`,
both message strings), and `contains_abort_marker` is in Java's order: the ABORT marker
`remove`s the producer id and only the `else if` consults `isBatchAborted`
(`CompletedFetch.java:210-218`). The Actor's core reasoning is also right: a producer id is
allocated once per incarnation and is stable across that producer's transactions, so the
Phase-7a "which is rare" justification is false. Per-batch and control-batch-only, so
consumer-threading §27's per-record budget is untouched (`read_ref_from_buffer` returns a
borrowing `DefaultRecordRef`); the abort integration test does discriminate — reverting the
fix restores the `unsupported_version` bail on the marker batch, which `poll()` surfaces
and `consume_values`'s `.expect("poll should not fail")` turns into a failure.

What is wrong is the **scope** every artifact assigns to the bug. All four say some
variant of:

> a producer that aborts one transaction and commits the next reuses it by construction;
> every `read_committed` consumer of **such a partition** hit the bail

and §9.27: "Every `read_committed` consumer of a partition where any transaction aborted
**and a later one committed** hit the bail".

No reuse and no later commit is needed. The removed guard was

    if batch_meta.is_control_batch && self.aborted_producer_ids.contains(&batch_meta.producer_id)

placed *after* `consume_aborted_transactions_up_to(batch_meta.last_offset)`. The ABORT
marker batch is itself a control batch carrying the aborted transaction's own producer id,
and `consume_aborted_transactions_up_to` has just inserted that id (the fetch response's
`AbortedTransaction.first_offset` is ≤ the marker's `last_offset` by construction). So the
bail fired on the marker of the very transaction that had just been skipped, in the same
fetch — for **any** partition containing **any** aborted transaction read under
`read_committed`, including one with a single aborted transaction and nothing after it, and
including an empty aborted transaction whose marker is its only batch.

That matters for the record: as written, a reader concludes the shipped consumer worked on
`read_committed` partitions unless a specific two-transaction pattern occurred. In fact
`read_committed` was unusable on any partition that had ever had an abort. §9.27's stated
lesson ("the scope of the resulting integration gap is the tell") is strengthened, not
weakened, by saying so.

**Expected.** State the actual trigger: any `read_committed` fetch that reaches an ABORT
marker, i.e. any aborted transaction on the partition. Keep the producer-id-stability
argument as the rebuttal of Phase 7a's *stated* premise, but do not let it stand as the
description of the trigger.

**Actual.** Four artifacts (module docstring, §9.27, the `01-fetch-path.md` addendum, the
commit message) describe a strictly narrower trigger than the code had.


**Resolution — conceded; the trigger is restated in all four artifacts.**

The narrowing was real and the analysis is confirmed: the guard sat after
`consume_aborted_transactions_up_to`, the ABORT marker batch is itself a control batch
carrying the id that call had just inserted, so the bail fired on the marker of the very
transaction just skipped. One aborted transaction with nothing after it was enough; so was
an empty aborted transaction whose marker is its only batch. `read_committed` was unusable
on any partition that had ever had an abort.

Corrected in: `completed_fetch.rs`'s module docstring, PLAN §9.27, the `01-fetch-path.md`
addendum, and — since the original commit message cannot be rewritten — the fixup commit
message, which states the correction and supersedes it.

Producer-id stability is kept, but explicitly demoted to what it is: the rebuttal of Phase
7a's *stated premise*, not the description of the trigger. §9.27 now says the wider scope
strengthens its own lesson rather than weakening it.

---

## Issue 5: §9.25's stated consequence understates the defect — the batch is dropped, not retried

- **File**: `design/history/Milestone-11/PLAN.md` §9.25; `src/producer/internals/sender.rs:2180-2195` (the corrected call-site comment)
- **Severity**: Missing Requirement (a filed-not-fixed defect is mis-sized)
- **Java Reference**: `TransactionManager.java:1015-1060`, `Sender.java` `completeBatch`

**Description.** The defect is real and the premise inversion is real — I confirmed both.
`TransactionManager.canRetry`'s `:1047-1048` arm is the **transactional** one
(`startSequencesAtBeginning`); `:1050`'s `requestIdempotentEpochBumpForPartition` is the
idempotent one, so the old comment had them backwards. `Sender::can_retry` passes `&mut []`.
`TxnPartitionEntry::start_sequences_at_beginning` → `reset_sequence_numbers` errors when a
tracked in-flight batch is absent from the pool, and the failing batch *is* still tracked at
this point. Filing rather than fixing is consistent with the §9.18 precedent it cites (a
produce-response-path signature change plus a DoD §10 audit), the `#[ignore]`d reproducer is
honest, and its failure is exactly the one documented.

But the consequence is described as:

> `last_acked_sequence` is never cleared and the sequence counter is never restarted. The
> error is swallowed by the per-response error handling on the produce path […] so nothing
> surfaces — **the producer simply carries the stale sequence state into the retry.**

There is no retry. `complete_batch` reaches `if self.can_retry(batch, response, now)?`
(`sender.rs:1896`), so the `Err` short-circuits **before** `BatchAction::Reenqueue` can be
produced; it propagates out of `handle_produce_response` and out of
`handle_produce_response_for`'s `?` at `sender.rs:942` — at which point the batch has
already been moved out of `in_flight_batches` into that function's local
`batches: HashMap<TopicPartition, ProducerBatch>`, which is then dropped. There is no
`impl Drop` anywhere in `src/producer/`, so:

  - the batch's `ProduceRequestResult` is never `set` → every record future on it never
    resolves (no timeout owner remains to expire it);
  - its pooled buffer is never deallocated → `BufferPool` accounting leaks, which
    eventually blocks `send` on `max.block.ms`;
  - `handle_client_responses` logs `Uncaught error in request completion` and `run_once`
    returns `Ok` — which is why the reproducer's `.expect("run_once")` passes.

**Expected.** §9.25 (and the `sender.rs` call-site comment, which repeats "so the sequence
rewrite silently does not happen") should say that the response is abandoned: the batch is
dropped un-completed, its futures never resolve and its buffer is not returned. That is
what sizes the fix — it is a hang-and-leak on a reachable transactional path, not a
sequence-hygiene nicety.

**Actual.** Both the PLAN entry and the call-site comment describe a retry that cannot
occur, so the recorded severity is lower than the code's.


**Resolution — conceded; §9.25 and the call-site comment now describe the abandonment.**

Confirmed by tracing: the `?` at the `can_retry` call fires before `BatchAction::Reenqueue`
can be produced, propagates out of `handle_produce_response_for`, and the local `batches`
map — which already owns the batch — is dropped. There is no `impl Drop` in `src/producer/`.

Both artifacts now state the three consequences: the `ProduceRequestResult` is never set so
every record future on the batch never resolves and no timeout owner remains; the pooled
buffer is never returned so `BufferPool` accounting leaks and eventually blocks `send` on
`max.block.ms`; and `run_once` still returns `Ok`, which is why it is silent and why the
reproducer's failure surfaces four assertions later as a stale `last_acked_sequence`. §9.25
now says this sizes the fix: a hang and a leak on a reachable transactional path, not a
sequence-hygiene nicety. The phrase "carries the stale sequence state into the retry" is
quoted and withdrawn in place.

---

## Issue 6: PLAN §9.19 was not updated — it still hands 11 methods to Phase 8 and lists two as blocked

- **File**: `design/history/Milestone-11/PLAN.md` §9.19 (unchanged in this range)
- **Severity**: Missing Requirement (stale tracking section)
- **Java Reference**: n/a

**Description.** `git diff 25be309 8356e80 -- design/history/Milestone-11/PLAN.md` touches
exactly two regions: the §Phase-8 status block and the new §9.25-§9.27. §9.19 — whose
status line still reads "**Status:** open" and which the plan itself calls the tracking
entry for this hand-off — still says:

  - "The other **11 are handed to Phase 8**" (10 landed; 1 remains);
  - "two of those eleven are blocked on named missing surface", naming
    `testTransactionalSplitBatchAndSend` **and**
    `testSenderShouldCloseWhenTransactionManagerInErrorState` — the second of which Phase 8
    translated (`sender.rs:5103`, and its unblocking is correct: Java's
    `mock(TransactionManager.class)` is replaced by the real ABORTABLE_ERROR state, which
    satisfies both stubs, and `force_close` + `close_call_count() == 1` are two independent
    pins on the `catch` arm at `Sender.java:274-278`).

The `sender.rs` accounting block *is* updated and does say "Phase 8 resolved one of those
two […] so **four** are blocked across both groups". §9.19 contradicts it. This is the
milestone's recurring shape (Critic 46 issue 6: when a phase closes a hand-forward, grep the
predecessor's hand-forward section too), and §9.19 is the section a reader lands on from
§Phase-8's own sentence "which §9.19 had assigned here".

Also pre-existing in the same paragraph, worth fixing while it is open: "of which **32**
are translated" against the arithmetic sentence two clauses later that uses `33 + 18 + 3 =
54`.

**Expected.** §9.19 amended for the Phase-8 outcome: 10 of 11 translated,
`testSenderShouldCloseWhenTransactionManagerInErrorState` moved out of the blocked list
with the route that unblocked it, the remaining blocked set stated as four, and the 32/33
typo resolved.

**Actual.** §9.19 is unchanged and now disagrees with both §Phase-8 and the `sender.rs`
accounting block.


**Resolution — conceded; §9.19 amended, and the 32/33 slip resolved.**

§9.19's status line now reads "**four** blocked entries" and records that the count moved
three times (3 → 5 → 4). The Phase-8 outcome is stated as 10 of 11 translated with the two
formerly-blocked entries split:
`testTransactionalSplitBatchAndSend` still blocked on §9.18 (re-verified by running the
reproducer, and the section says that is how it was checked rather than by re-reading the
note), and `testSenderShouldCloseWhenTransactionManagerInErrorState` translated — with the
route that unblocked it, which is the second of the two the old note itself offered.

The `#[ignore]`d §9.25 reproducer is explicitly *not* counted as blocked, with the reason
(its body is complete; the failing assertion is a production assertion), and §9.28's
different-mechanism translation is cross-referenced so a reader of §9.19 finds it.

The "32 are translated / 2 are blocked" clause is corrected to 33 / 3 to match its own
`33 + 18 + 3 = 54` arithmetic, with a note that those Phase-4-era numbers have since moved
and that the `sender.rs` accounting block is authoritative.

---

## Issue 7: five of six `MockClient.java` citations in `mock_client.rs` point at unrelated methods

- **File**: `src/mock_client.rs`
- **Severity**: Missing Requirement (record — citations are the reviewable evidence)
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/MockClient.java`

**Description.** Verified with `grep -n` against the file:

| claim in `mock_client.rs` | cited | actual | what is at the cited lines |
|---|---|---|---|
| `MockClient.RequestMatcher` | `:637-639` | `:623-625` | inside `MetadataUpdate`'s constructor |
| `respond(RequestMatcher, AbstractResponse)` | `:294-296` | `:382-384` | `wakeup()` |
| its `IllegalStateException` | `:301-303` | `:385` / `:389` | `wakeupHook` block in `wakeup()` |
| `prepareResponse(RequestMatcher, AbstractResponse)` | `:246-248` | `:445-447` | the future-response iterator inside `send` |
| `prepareResponse(RequestMatcher, AbstractResponse, boolean)` | `:258-260` | `:472-474` | `build(version)` + matcher check inside `send` |
| "Java builds the request unconditionally here" | `:495` | `:259` | `requests.size() >= minRequests` in `waitForRequests` |
| `disconnect(String, boolean allowLateResponses)` | `:200-218` | `:200-218` | correct |

The offsets are not consistent (+14, −88, −84, −199, −214, +236), so this is not one
mis-indexed read; and four of the five wrong numbers land inside `send`, which is what
makes them survive a spot check — `:259` really *is* `request.requestBuilder().build(version)`,
just cited as the location of `prepareResponse` instead. This is the rotation shape
(Critic 46 issue 2), well past the ±1-3 range slop I have twice declined to file, and here
the citations *are* the evidence that a newly-added harness surface matches Java.

The behaviour these comments describe is correct: Java does default `prepareResponse` to
`ALWAYS_TRUE` (`:432`, `:458`) and therefore builds unconditionally at `:259`, and the
narrower Rust build is a sound deviation given `ProduceRequestBuilder::build`'s move
semantics.

**Expected.** Citations corrected to `:623-625`, `:382-392` (with the throw at `:385`/`:389`),
`:445-447`, `:472-474`, `:259`.

**Actual.** Six citations, five wrong, each landing in a different method than the one
named.


**Resolution — conceded; all six citations corrected after re-deriving each myself.**

Every line was checked against `MockClient.java` rather than taken from the table, and all
five wrong ones reproduce exactly as reported. Corrected to `:623-625` (`RequestMatcher`),
`:382-392` with the throws at `:384-385` / `:388-389` (`respond(RequestMatcher, ..)`),
`:445-447` and `:472-474` (the two `prepareResponse` overloads), and `:259` for the
unconditional build.

The `:259` comment now also names *why* Java builds unconditionally — its matcher-less
overloads default to `ALWAYS_TRUE` (`:49`, used at `:432` / `:458`), so there is always a
matcher to run — which is the fact that makes the narrower Rust build a deviation worth
justifying rather than an omission.

A mechanical re-check now prints every `MockClient.java` citation in the file beside the
line it lands on; all sixteen (including the pre-existing `advanceTimeDuringPoll` ones) land
on the right declaration or statement.

---

## Issue 8: `close_call_count`'s accessor was inserted inside `current_state`'s attribute list, stripping its `#[cfg(test)]` and its doc comment

- **File**: `src/producer/internals/transaction_manager.rs:1960-1972`
- **Severity**: Bug
- **Java Reference**: n/a (test-visibility accessors)

**Description.** The new accessor landed between `current_state`'s doc comment + gate and
its `fn`:

    /// The current state. Visible for testing, as Java's package-private field
    /// access is.
    #[cfg(test)]
    /// How many times [`Self::close`] has been called — Java's
    /// `verify(transactionManager, times(n)).close()`.
    #[cfg(test)]
    pub(crate) fn close_call_count(&self) -> u32 {
        self.close_call_count
    }

    fn current_state(&self) -> State {
        self.current_state
    }

Rust accepts interleaved doc comments and attributes, so this compiles, with three
consequences:

  1. `close_call_count`'s rendered rustdoc is two unrelated paragraphs, opening with "The
     current state. Visible for testing, …".
  2. `current_state` is now undocumented.
  3. `current_state` lost its `#[cfg(test)]` and is compiled into release builds even
     though its only 13 callers are all in `mod tests` (line 4696 onward). It is inside the
     production `impl TransactionManager` at line 1224, and the only reason `cargo xtask
     lint` stays clean is the file-level `#![allow(dead_code)]` at line 19 — so the gate's
     removal is invisible to the gate that would normally catch it.

The counter itself is right: `close_call_count` is the correct translation of
`verify(transactionManager, times(1)).close()` (`SenderTest.java:3413`) and a count rather
than a flag is the right call.

**Expected.** `close_call_count` gets its own doc + single `#[cfg(test)]`, placed before or
after `current_state`, with `current_state`'s doc comment and `#[cfg(test)]` restored to it.

**Actual.** One doc comment and one `#[cfg(test)]` migrated from `current_state` onto the
new method.


**Resolution — conceded; both attributes restored, and swept crate-wide.**

`current_state` has its doc comment and `#[cfg(test)]` back, and `close_call_count` follows
it with its own doc and single gate. Confirmed `current_state` is gated again and that
`cargo build --release` is clean.

The finding asked for a mechanical check rather than a read, so one was written and run over
every `.rs` file in `src/`: flag any doc-comment line followed by one or more attribute
lines followed by another doc-comment line, which is exactly the signature of an item
inserted into a preceding item's attribute list. **0 suspects** crate-wide, so this was the
only instance.

The observation about `#![allow(dead_code)]` masking the lost gate from `cargo xtask lint` is
correct and is why the sweep, not the gate, is the check here.

---

## Issue 9: the header sweep's population excludes the 50 `TransactionManagerTest` headers in the same file; nine deviate, six added by this phase

- **File**: `src/producer/internals/sender.rs` (accounting block ~7620-7660 and the headers themselves)
- **Severity**: Missing Requirement (the audit's denominator excludes the population the phase created)
- **Java Reference**: `TransactionManagerTest.java`

**Description.** The block states the convention and claims the sweep:

> Line numbers are the `public void` declaration line throughout, here and in the
> `Translated from` header of every test above, whose ranges run declaration line to the
> method's closing `    }`. […] Both halves of that convention are now swept mechanically
> rather than asserted. […] **52 headers, 0 mismatches** as of Phase 8 (41 before it).

The sweep's population is `` Translated from `SenderTest.<name>` `` only. `sender.rs` also
carries **50** `` Translated from `TransactionManagerTest.<name>` … (Java A-B) `` headers,
all under the same convention (41 of the 50 conform exactly to declaration → closing
`    }`, which is what establishes that the convention applies to them). Nine do not, and
six of those nine were added by Phase 8:

| header | cited | declaration → closing `    }` | added by |
|---|---|---|---|
| `testMultipleAddPartitionsPerForOneProduce` | 1932-**1976** | 1932-**1970** | Phase 8 |
| `testSenderShutdownWithPendingTransactions` | 228-**247** | 228-**246** | Phase 8 |
| `testFatalErrorWhenProduceResponseWithInvalidPidMapping` | 1435-**1449** | 1435-**1448** | Phase 8 |
| `testSendOffsetWithGroupMetadataFailAsAutoDowngradeTxnCommitNotEnabled` | 2666-**2681** | 2666-**2682** | Phase 8 |
| `testTransitionToFatalErrorWhenRetriedBatchIsExpired` | 2979-**3037** | 2979-**3036** | Phase 8 |
| `testBumpTransactionalEpochOnRecoverableAddOffsetsRequestError` | 3567-**3598** | 3567-**3597** | Phase 8 |
| `testDuplicateSequenceAfterProducerReset` | **748**-810 | **749**-810 | pre-existing |
| `testHealthyPartitionRetriesDuringEpochBump` | **3599**-3692 | **3601**-3692 | pre-existing |
| `testFailedInflightBatchAfterEpochBump` | **3727**-**3810** | **3726**-**3816** | pre-existing |

Five of the six new ones are ±1 and inside the right method, i.e. within the ±1-3 slop bar
I recorded in Phase 5b and have declined to file since. **`testMultipleAddPartitionsPerForOneProduce`
is not**: its range ends at 1976, six lines past its own closing brace at 1970 and inside
`testRetriableErrors`'s `@EnumSource` list (1972-1978), so it spans two methods. The two
pre-existing start-line deviations are the annotation line rather than the declaration
(`@ParameterizedTest` / `@ValueSource`), the same shape as Critic 46 pass 2's finding.

The point is the denominator, not the nine: the phase added 47 headers in this convention
and the sweep it re-ran and re-reported could not see any of them. Phase 6 pass 4's lesson
("before asserting any 'N of M', ask what M would miss") applies to `M = 52` here.

**Checked non-finding, so a later pass does not re-derive it:** `transaction_manager.rs`
carries **78** bare-name range citations (`` `testFoo` (Java A-B) ``) that use a *different*
and unstated convention — annotation line to one line past the closing brace, e.g.
`testFailIfNotReadyForSendNoProducerId` cited `(Java 262-265)` where the declaration is 263.
All 78 differ from `sender.rs`'s convention and are internally consistent with each other,
so they are a convention difference, not 78 defects. Applying the `sender.rs` yardstick to
them would be a false positive; they are also pre-existing and outside Phase 8's diff.

**Expected.** Widen the sweep to `` Translated from `(SenderTest|TransactionManagerTest)\.<name>` ``,
report the two populations' counts, fix `testMultipleAddPartitionsPerForOneProduce`'s
cross-method range, and state whether the ±1 group is being left alone deliberately.

**Actual.** "52 headers, 0 mismatches" is true of its stated population and silent about
the 50-header population the phase created.


**Resolution — conceded; the sweep is widened and all nine ranges fixed.**

The widened sweep (alternation over both Java classes, wrap- and paren-tolerant, both ends
checked) was re-implemented and run: it reported exactly the nine deviations the finding
names, with identical values, and now reports **102 headers (52 `SenderTest` + 50
`TransactionManagerTest`), 0 mismatches**.

All nine are corrected, including the three pre-existing start-line slips whose true
declarations were derived from the Java file (749, 3601, 3726) rather than taken from the
table. `testMultipleAddPartitionsPerForOneProduce`'s cross-method range 1932-1976 → 1932-1970
is the substantive one. Two of the corrections also appear in a shared-body helper's doc,
which is fixed with them.

Rather than state the exclusion, the accounting block now carries the alternation and says
why: the initial sweep reported "52 headers, 0 mismatches" while silent about a 50-header
population *in the same file* that the phase had itself grown from 6 to 50 — Phase 6 pass
4's "ask what M excludes", applied to a sweep's own denominator. The ±1 group is fixed rather
than waived, so the sweep's 0 is meaningful.

The 78 bare-name citations in `transaction_manager.rs` are left alone; the finding's
reasoning that they are a separate internally-consistent convention is correct, and the
Critic's `definition-of-done.md` suggestion is the right home for unifying them — relayed
to the process rather than acted on here, since rules files are outside an Actor's remit.

---

## Issue 10: the header-sweep narrative misattributes one of the two corrections it cites

- **File**: `src/producer/internals/sender.rs:7625-7630`
- **Severity**: Missing Requirement (record — a false supporting fact behind a true claim)
- **Java Reference**: `SenderTest.java:1534-1572`, `:2829-2871`

**Description.** The block says:

> it caught two of Phase 8's own ranges off by one at the *end*
> (`testUnresolvedSequencesAreNotFatal`, `testAwaitPendingRecordsBeforeCommittingTransaction`),
> **both because the method's body is wrapped in a `try (Metrics m = ..)` whose `        }`
> precedes the real `    }`.**

Both corrections are real (verified against `git show 35bffe0:…`: 1534-**1571** → 1534-1572
and 2829-**2870** → 2829-2871). The attribution holds for the second —
`SenderTest.java:2830` is `try (Metrics m = new Metrics()) {` and 2870 is its `        }`.
It is false for the first: `testUnresolvedSequencesAreNotFatal` (1534-1572) contains **no
inner braces at all**; 1571 is `assertTrue(txnManager.hasAbortableError());` and 1572 is
the closing `    }`. It was a plain last-statement-instead-of-brace slip. `grep -n "try
(Metrics"` over `SenderTest.java` returns exactly five sites (548, 2406, 2771, 2829, 3605)
and 1534 is not among them.

The generalisation matters more than the fact: a reader takes away "watch for the
`try (Metrics ..)` wrapper", when the real lesson is the one the block already states
correctly two paragraphs down — check *both* ends against the closing brace, whatever the
body looks like. Relatedly, the wrapper is not the main producer of that shape: **eleven**
of the translated methods have an inner `        }` immediately before their closing `    }`
(508, 2737, 2771, 2829, 2898, 2932, 3051, 3254, 3308, plus the helpers 3871 and 3887), only
three of them from a `try (Metrics ..)`.

**Checked, and it does not hide a third instance:** the one remaining translated
`try (Metrics ..)` method, `testRecordsFlushedImmediatelyOnTransactionCompletion`, was cited
`(Java 2771-2826)` correctly from `35bffe0` onwards; the other two wrapper sites
(`testNodeLatencyStats` 548, `testNoBufferReuseWhenBatchExpires` 3605) and the private
helper `testSplitBatchAndSend` (2406) have no `Translated from` header.

**Expected.** Attribute the `try (Metrics ..)` cause to
`testAwaitPendingRecordsBeforeCommittingTransaction` only, and describe
`testUnresolvedSequencesAreNotFatal`'s as an end-off-by-one with no inner brace involved —
which is the stronger argument for checking both ends unconditionally.

**Actual.** A cause is asserted for two corrections and holds for one.


**Resolution — conceded; the cause is re-derived and split.**

Verified: `SenderTest.java:1534-1572` has no inner braces at all — 1571 is
`assertTrue(txnManager.hasAbortableError());` and 1572 the closing `    }` — so that
correction was a plain last-statement-for-brace slip, while
`testAwaitPendingRecordsBeforeCommittingTransaction`'s really is the `try (Metrics m = ..)`
wrapper at 2830/2870.

The block now attributes the wrapper cause to the second only, describes the first as an
end-off-by-one with no inner brace involved, and states the transferable rule as "check
**both** ends against the closing brace, whatever the body looks like" — noting the
finding's own count that eleven translated methods have that inner-brace shape and only
three come from the wrapper, so the wrapper was never the general case.

---

## Issue 11: three trivial record slips (closeable together)

- **Files**: `design/current/status.md`, `tests/integration/producer_transactions_test.rs:122`, `src/producer/internals/producer_test_utils.rs:29,39`
- **Severity**: Record (trivial)
- **Java Reference**: `ProducerTestUtils.java:33-44`

**Description.** All three verified, none behavioural. Grouped so they cost one cycle, not
three.

1. **`status.md`'s `src/common/` line count was invalidated by this phase's own final
   commit.** The table says `148 | 47 627`, which was exact at `47796aa`; `8356e80`
   (`docs(record): account for ControlRecordType's untranslated recordKey`) added 22 lines
   to `src/common/record/control_record_type.rs`, so the tree now measures **47 649**. I
   re-measured every figure in that table: 263 files / 176 472 total, consumer 64/64 516,
   producer 23/42 099, ffi 4/7 757, root 22/14 242 — all correct; `common` is the only one
   off, and `structure.md`'s "~47 600" still rounds. This is the milestone's signature shape
   (a commit invalidating a number written earlier in the same phase) at its cheapest.
   Everything else about the `design/current/` refresh checks out, including the
   `01-fetch-path.md` review getting a dated addendum rather than an in-place edit.

2. **`assigned_consumer`'s doc names a parameter it does not take.** "A consumer pinned to
   `tp` at its beginning, at the given isolation level" — the signature is
   `(bootstrap, group_id, isolation_level)`; there is no `tp`. The helper is also used with
   `subscribe` in `test_consume_transform_produce_with_offsets`, which the module docstring
   explains but the helper's own name and doc do not.

3. **`producer_test_utils.rs`'s citations of `ProducerTestUtils.java`.** The method spans
   **33-44**; the rustdoc says `(Java 33-43)` twice (lines 29 and 50). And
   "mirroring Java's `assertTrue(condition.get(), ..)` (Java 42)" — the statement is at
   **43**; 42 is the `while` loop's closing `        }`. The range is inside the slop bar I
   have declined to file elsewhere and I would not raise it alone; the single-statement
   citation is the shape I filed in Phase 7 (a line cited as the location of a statement
   quoted verbatim beside it), so it is here for consistency with that bar rather than a new
   one. `MAX_TRIES` at "(Java 24)" and the first overload at "(Java 26-31)" are both
   correct.

**Expected.** `47 649`; drop the `tp` from the helper's doc (or rename it, e.g.
`consumer_for`); `(Java 33-44)` and `(Java 43)`.

**Actual.** As above.

---

# Not findings — checked and cleared

Recorded so a later pass does not spend the cycle.

- **`ControlRecordType` vs `ControlRecordType.java`**: five members, four translated, and
  the `recordKey()` omission is accounted for with a grep whose output I reproduced
  (`MemoryRecordsBuilder.java:614` is its only caller, and `appendControlRecord` /
  `EndTransactionMarker` are out of scope per PLAN §1.1). Java reads the key absolutely
  (`getShort(0)`/`getShort(2)`) but length-checks `remaining()`; a `&[u8]` collapses the two,
  which is fine for every real caller. `parse` returning `Ok(false)` for a keyless control
  record where Java would NPE is a documented, strictly-safer deviation.
- **`contains_abort_marker`'s empty-batch test**: `records_bytes.is_empty()` is equivalent to
  Java's `!batchIterator.hasNext()` here, since a zero-record batch has
  `size_in_bytes == RECORD_BATCH_OVERHEAD` and the borrowed range is therefore empty.
- **Decompression before the aborted-batch skip** (Java creates `streamingIterator` *after*
  the skip decision, so Java never decompresses a skipped aborted batch; `load_next_batch`
  builds `RecordSource::Owned` first and drops it). Pre-existing, unchanged by this diff, and
  not made worse — control marker batches are not compressed.
- **`MockClient` double-`build()` hazard**: `respond_with_matcher` builds via
  `requests.front_mut()` and then `respond_with_disconnect` only re-reads
  `latest_allowed_version()` / `make_header`, so no builder is built twice; the `send`
  matcher path builds once and nothing downstream reads the body. The matcher is *not* used
  for selecting which future response matches — Java does not either (`:249-250` filters on
  node only, then throws), so FIFO semantics are preserved.
- **`send`'s matcher-rejection panic message** omits Java's `" with prepared response " +
  futureResp.responseBody` tail (`:261-262`) and reuses the `respond` wording. Harness-only,
  one message, both sites unambiguous. Disclosed, not filed.
- **§9.26's scope call: correct, and the record is executable.** It names the owner
  (consumer test-parity work), the missing surface (a fixture emitting a real control batch;
  `build_batch_full` does not), the three Java tests with line numbers, the extra owed item
  (`test_consumer_position_updated_when_skipping_aborted_transactions`' literal 2 vs Java's
  3), the reason for deferring, and the interim broker-level cover. Deferring is right even
  though the consumer *production* fix landed here: the fix is one branch in one file, while
  the tests need a new batch builder across two consumer test files, and bundling them would
  make a later regression ambiguous. Its "two further tests there still use plain data
  batches and say so" is accurate under the reading "in those two files" — I enumerated
  them: `test_read_committed_with_aborted_transaction` (fetch_collector) and
  `test_consumer_position_updated_when_skipping_aborted_transactions` (fetch_request_manager);
  `test_read_committed_with_compacted_topic` carries no such note.
- **DoD sweep**: §2 method accounting present for `ControlRecordType`; §3 both derivations
  reproduce; §5 all suites pass; §6 no duplicate test names in `sender.rs` and no
  `TransactionManagerTest` name cited twice across the two files; §7 the only non-Java type
  added is the test-only `FailThirdProduce` enum, documented at its definition (`RequestMatcher`
  is `MockClient.RequestMatcher`, `producer_test_utils` is `ProducerTestUtils`); §8 the diff
  adds no `TODO`/`FIXME`/`todo!`/`unimplemented!`; §10 the producer half adds tests only, and
  the consumer half is per-batch, control-batch-only and borrowing, so consumer-threading §27's
  per-record budget is unchanged.
- **The four integration tests' §9.17 hygiene**: all four go through `TestContext` with
  `ctx.topic(..)` / `ctx.group_id(..)` prefixes and `ctx.cleanup()`, share
  `kip848_3_broker(2)`'s pooled container (the three-broker requirement for
  `transaction.state.log.replication.factor` is correct), and introduce no new container or
  network creation path — so no new leak shape.
- **The ignored-test counts**: `cargo test` reports `3 ignored` in the lib plus `5 ignored` in
  the doc-test target; the five are doc-tests, not `#[ignore]`, so `status.md`'s "3
  `#[ignore]`d" plus the separate note about the integration suite's 10 is right.


**Resolution — two conceded as specified, one with a deliberate deviation.**

2 and 3 as specified. `assigned_consumer`'s doc no longer names a `tp` it does not take; it
now says what it builds and that the caller chooses `assign` or `subscribe`, and the module
docstring cross-references it. `producer_test_utils.rs` is corrected to `(Java 33-44)` in
both places and `(Java 43)` for the `assertTrue`, each verified against the Java file (33 is
the signature, 44 the closing brace, 43 the `assertTrue`, and 42 the `while` loop's brace as
the finding says).

1 **deviates from the Expected, deliberately.** The exact `47 649` is not used. That figure
was invalidated twice inside this phase — once by a 22-line doc comment, and again by this
very fixup series, which moves `common` to 47 649 → and `producer`, `consumer` and root as
well. Writing another exact number resets the trap the finding correctly identifies as "the
milestone's signature shape". Instead the line counts are rounded to the nearest hundred
(matching `structure.md`, which the finding notes still rounds correctly), file counts stay
exact because they are stable, and the table now ships the `find | wc -l` command to
re-derive them. The table says in so many words that the rounding is deliberate and why.

If the Critic prefers the exact figure restored, it is a one-line change — but it will need
re-measuring after whichever commit lands last, which is the argument for rounding.

---

# Suggestion for `.claude/rules/definition-of-done.md` (not a finding)

`COMMENTS.FP.md` records no new false positives from my previous passes and there is no
`COMMENTS.FN.md`, so this is not feedback-driven — it comes out of Issue 9.

`sender.rs` and `transaction_manager.rs` now cite Java test line ranges under **two
different, separately-invented conventions** (declaration → closing `    }`, and annotation
→ one past the closing brace), each stated in at most one of the files, and each with its
own sweep or none. Every phase from 4 onward has spent review cycles on this — Critic 44
issue 10, Critic 45's normalisation script, Critic 46 passes 3 and 4, and Issues 9 and 10
here.

Suggested addition to `definition-of-done.md` §3, as a fifth bullet:

> - A `Translated from` header citing a Java line range uses the **declaration line to the
>   method's closing `    }`** — not the annotation line, not the last statement. State it
>   once per file that carries such headers, and make any completeness sweep cover **every**
>   Java class cited in that file, not one of them.

That turns a per-block convention into a repo-wide one, makes the sweep's population
derivable rather than chosen, and would have caught six of this phase's nine deviations
mechanically.


---

# =========================================================================
# Critic 48 pass 2 — three findings, all resolved
# =========================================================================

Loop: 11 → 3 → (pass 3 pending). **Three conceded, none disputed.** Each was re-derived
from the sources before the finding was accepted; all three derivations agreed with the
Critic.

Also actioned, from the Critic's housekeeping note: `COMMENTS.48.md` is now **truncated to
0 bytes** rather than deleted, matching all seven sibling `COMMENTS.4x.md` placeholders. The
pass-1 close deleted it, which would have errored a Manager loop that reads the path to test
emptiness.

Adjudications recorded in the Critic's favour this pass, for the record: pass-1 item 2's
fork-the-container reasoning was confirmed against `cluster_pool::get_or_create`'s keying,
and item 5's rounded-counts deviation was ruled in the Actor's favour with the general rule
minted — **ask for exactness only where the number cannot be re-derived.**

# Critic 48 — Milestone 11 Phase 8, pass 2 (`8356e80..HEAD`)

Review of the eleven fixes across `a49d060`, `fe253e0`, `1bdd846` (+ four memory commits).
**Nine of eleven fixes are clean.** Three findings, all in the fixes' own records — two of
them created by this round, which is the milestone's signature shape one last time.

Loop: 11 → 3.

*(Housekeeping: `1bdd846` **deleted** `COMMENTS.48.md` rather than truncating it, where all
seven sibling `COMMENTS.4x.md` files exist as 0-byte placeholders. Recreated by this file.
Not filed — but a Manager loop that reads the file to test emptiness would have errored on a
missing path.)*

## Reproduced before filing

  - **The widened header sweep, re-implemented independently:** `{'SenderTest': 52,
    'TransactionManagerTest': 50}`, **total 102, 0 mismatches**. All nine deviations fixed,
    including the cross-method `1932-1976 → 1932-1970`, and the three pre-existing
    start-line slips corrected to the true declarations 749 / 3601 / 3726.
  - **All 16 `MockClient.java` citations** in `mock_client.rs` printed beside the lines they
    land on: all 16 correct (`:623-625` `interface RequestMatcher`, `:196-198` /
    `:200-218` the two `disconnect` overloads, `:382-392` `respond(RequestMatcher, ..)`,
    `:384-385` / `:388-389` its two throws, `:445-447` / `:472-474` the two matcher
    `prepareResponse`s, `:259` the unconditional build, `:49` / `:432` / `:458`
    `ALWAYS_TRUE`, plus the pre-existing `:74` / `:145-147` / `:346-348`).
  - **The attribute-strip sweep** (doc line → one or more `#[..]` lines → doc line) over
    every `.rs` under `src/`: **0 suspects**, matching the claim. `current_state` has its
    doc and `#[cfg(test)]` back, `close_call_count` follows it with one gate each.
  - **`status.md`'s re-derivation command run verbatim:** common 148/47 649, consumer
    64/64 531, producer 23/42 203, ffi 4/7 757, root 22/14 255, total 263/176 604 — every
    rounded figure in the table is the correct nearest hundred, and `structure.md` agrees.
  - **Issue 1's mechanism, from the routing code:** `pending_produce_responses` has exactly
    one insert (`sender.rs:2346`, at send) and exactly one removal (`:906`, keyed on the
    response's correlation id); `fail_expired_batches(expired_inflight_batches, now,
    **false**)` (`:1610`) yields `retain = true` and pushes the batch to
    `batches_awaiting_response` (`:1657`) **without** touching the routing map. So the
    expiry really does leave the entry intact and the late `INVALID_TXN_STATE` really is
    routed into `handle_produce_response` for a done batch. The mutation claim is airtight
    in both halves: with one removal site keyed on a response, deleting the response leaves
    `pending_produce_responses` at 1 (assertion 1 fails), while the previous revision's
    retaining disconnect pushed a `ClientResponse` carrying the *same* correlation id, so
    that assertion would have passed without the late response. `batches_awaiting_response`
    likewise drains only there — its other drain (`:701`) is in `run`'s shutdown tail,
    unreachable from the test's `run_once` calls.
  - **Issue 2's three named unit tests exist and cover what is claimed** — each keeps one
    manager across the bump, which is what makes the reset observable:
    `test_out_of_order_sequence_is_retried_and_bumps_the_epoch` (epoch 1→2,
    `first_in_flight_sequence == 0`, and the re-drained batch's own `producer_epoch() == 2`
    / `base_sequence() == 0`), `test_bump_transactional_epoch_on_unknown_producer_id_error`
    (`sequence_number(&tp0) == 0` after the bump),
    `transaction_manager.rs::test_producer_id_reset` (tp0 3→0, tp1 untouched).
  - **Issue 2's restructuring reasoning holds.** `cluster_pool::get_or_create` keys on
    `config.clone()` — the whole `ClusterConfig` including `server_properties` — so adding a
    `transaction.version` feature level really would mint a second container and fork this
    suite off the `PlaintextConsumer*` pool. **Ruled in the Actor's favour.**
  - Gates: `cargo xtask format-check` ✅, `cargo xtask lint` ✅, `cargo test` 2 666 passing /
    3 `#[ignore]`d.
  - Item 3 verified in all its places: the any-abort scope in the module docstring, §9.27
    and the `01-fetch-path.md` addendum (and `fe253e0`'s message explicitly supersedes the
    immutable `c37f0f3` one — the right handling of an artifact that cannot be edited);
    §9.25's dropped-batch/hanging-futures/leaked-buffer upgrade in both the PLAN and the
    call-site comment; §9.19's amendment, whose "four blocked" header and "3 are blocked
    below" summary are explicitly reconciled ("`testTransactionalSplitBatchAndSend` plus the
    three below") and whose 33 + 18 + 3 = 54, 54 − 2 = 52 now closes.

## Item 5 — adjudicated in the Actor's favour, no finding

Issue 11.1's "Expected" was the exact `47 649`; the Actor rounded every line figure to the
nearest hundred instead, kept file counts exact, and shipped the `find | wc -l` command that
re-derives them.

**That is better than what I asked for, and it is the convention that should win here.** My
finding's substance was that the table asserted something false; the fix makes it true *and*
removes the property that made it go false twice inside one phase. It is the same move I have
endorsed since Phase 4 pass 3 — answer an accounting finding with a derivation rather than a
patched number, and the review of the artifact becomes running its command. The rounding is
marked with `~` and explained at the site, file counts stay exact because they do not churn,
`structure.md` already rounded so the two files now agree, and the one exact figure in the
narrative (`15 894 → ~42 200`) correctly keeps its frozen left-hand side. I re-ran the shipped
command and every rounded value is right.

The general rule I would draw, for the record: **ask for exactness only where the number
cannot be re-derived.** Where a derivation can be shipped, ship it and round the display.

---

## Issue 12: the fix for Issue 1 left `disconnect_by_id` with two stacked doc blocks

- **File**: `src/mock_client.rs:287-292`
- **Severity**: Bug (doc-hygiene, same class as Issue 8 — created by Issue 8's own round)
- **Java Reference**: `MockClient.java:196-198`

**Description.** Removing the inert overload deleted `disconnect_by_id`'s body and moved the
`pub fn` to the end of the rewritten doc block, but left the *old* doc block in place above
it. The item now carries two summaries and two `Translated from` lines:

    /// Disconnect a node by ID string, creating disconnect responses for all
    /// pending requests to that node.
    ///
    /// Translated from `MockClient.disconnect(String)`.
    /// Disconnects `node_id`, failing its in-flight requests.
    ///
    /// Translated from `MockClient.disconnect(String)` (`MockClient.java:196-198`), which
    /// delegates to the two-argument overload with `allowLateResponses = false`.

There is no `///` separator between lines 291 and 292, so rustdoc renders them as one
paragraph: "Translated from `MockClient.disconnect(String)`. Disconnects `node_id`, failing
its in-flight requests."

This is exactly Issue 8's failure mode — an item's doc left behind when the item around it
moved — and it is the second time in this phase that the *fix for* a doc-hygiene finding
introduced one. Worth stating precisely because the round also shipped a crate-wide sweep
for this family: the sweep's shape is *doc line → one or more `#[..]` lines → doc line*,
which reproduces **0 suspects** (I ran it) and structurally cannot see a doc block stacked
directly on another with no attribute between. A sweep for the shape that *did* occur —
contiguous `///` blocks containing two or more `Translated from` openers — returns exactly
one hit across all of `src/`: `src/mock_client.rs:288`.

**Expected.** One doc block: the new summary, one `Translated from` with the citation, then
the `# Why the allowLateResponses overload is not translated` section. And, since the round
shipped a sweep for this family, widen it to catch a doc block appended to a doc block —
that is the shape it missed, and it missed the one instance that existed.

**Actual.** Two summaries and two `Translated from` lines on one item, the second pair
running on from the first.


**Resolution — conceded; blocks merged, and the sweep is now an xtask that catches both
shapes.**

Confirmed exactly as filed: two summaries, two `Translated from` lines, no `///` separator.
Merged into one block — one summary, one `Translated from` with the citation, then the
`# Why the allowLateResponses overload is not translated` section.

On the sweep, the finding's diagnosis is the important half: the shipped shape (doc →
attributes → doc) **structurally cannot** see a doc block stacked directly on another, and
it missed the one instance that existed. So rather than extend an ad-hoc script, both shapes
now live in **`cargo xtask doc-hygiene`**, which `cargo xtask lint` runs first — CLAUDE.md §6
puts repeatable checks in xtask rather than shell scripts, and a check nobody is obliged to
run is a check that finds the next instance one review round late. Which was chosen, as
asked: a sibling shape inside one new durable command, wired into the existing gate.

  - Shape 1 (migrated attribute/doc): `///` line → one or more `#[..]` lines → `///` line.
  - Shape 2 (stacked doc): one contiguous `///` run carrying two or more `Translated from`
    openers.

**Both mutation-checked.** Restoring the stacked block reports
`src/mock_client.rs:288 one doc block carries 2 'Translated from' openers`; re-migrating
`close_call_count` into `current_state`'s attribute list reports
`src/producer/internals/transaction_manager.rs:1961 doc comment separated from its item by an
attribute list`. Exit codes measured without pipes: **1** mutated, **0** restored, and
`cargo xtask lint` **0**.

---

## Issue 13: the corrected header-sweep bullet mis-classifies `testFailedInflightBatchAfterEpochBump` twice

- **File**: `src/producer/internals/sender.rs` (the header-sweep bullet, ~7643-7650)
- **Severity**: Missing Requirement (record — the bullet is the lesson, and its taxonomy is wrong)
- **Java Reference**: `TransactionManagerTest.java:3724-3727`, `:3809-3816`

**Description.** The rewritten bullet classifies the nine fixed deviations:

> Eight of the nine were ±1 or ±2 at one end. `testMultipleAddPartitionsPerForOneProduce`
> was not: cited `1932-1976`, it ran six lines past its own closing brace at 1970 […]
> Three were pre-existing start-line slips citing the `@ParameterizedTest` / `@ValueSource`
> annotation rather than the declaration […]

Both sentences are wrong about the same entry, `testFailedInflightBatchAfterEpochBump`,
which was cited `(Java 3727-3810)` against a true `3726-3816`:

1. **It was not "±1 or ±2 at one end".** It was off at *both* ends, and the end was off by
   **six** — 3810 is `Errors.NONE, 500L, b1AppendTime, 0L);`, mid-body, five lines before
   the method's last statement. So the count is **seven** of the nine in that class, and
   there were **two** large-deviation entries, not one. The other is arguably the worse of
   the pair to have omitted: `testMultipleAddPartitionsPerForOneProduce`'s over-run at least
   landed in a neighbouring method's annotations, whereas this one truncated its own method
   by six lines while also starting one line late.
2. **It did not cite an annotation.** `TransactionManagerTest.java:3724` is
   `@ParameterizedTest`, `:3725` is `@ValueSource(booleans = {true, false})`, `:3726` is the
   declaration and `:3727` is `// Use a custom Sender to allow multiple inflight requests`
   — the first *body* line. So of the three pre-existing start-line slips only **two**
   (`testDuplicateSequenceAfterProducerReset` 748 → 749,
   `testHealthyPartitionRetriesDuringEpochBump` 3599 → 3601) cited an annotation; this one
   started one line *inside* the body.

The fixes themselves are all correct — the sweep reproduces 102/0 and the corrected values
749 / 3601 / 3726 are the true declarations. What is wrong is the taxonomy written beside
them, which is the part a future sweeper reads to know what shapes to expect. Its effect is
to halve the large-deviation class and to make every pre-existing slip look like an
annotation-line habit, when one of them was a body-line slip that only a both-ends check
finds — which is the bullet's own stated rule.

Same shape as pass 1's Issue 10, in the same bullet: the corrections land, the sentence
generalising them does not.

**Expected.** "Seven of the nine were ±1 or ±2 at one end" plus both exceptions named
(`testMultipleAddPartitionsPerForOneProduce` `1932-1976`, six lines into the next method's
`@EnumSource`; `testFailedInflightBatchAfterEpochBump` `3727-3810`, one line into its own
body and six lines short of its closing brace), and "two of the three pre-existing
start-line slips cited the annotation; the third cited the first body line".

**Actual.** Eight/one and three/three.


**Resolution — conceded; the bullet is replaced by pasted derivation output rather than
patched prose.**

Re-derived all nine rows myself before accepting the finding, reading each cited value from
the pre-fix state and each true value from the Java file, and classifying what the *cited*
line actually holds. The result matches the finding exactly:

    testFailedInflightBatchAfterEpochBump  cited 3727-3810  true 3726-3816
        start +1 (body comment); end -6 (body statement)

So it was wrong at both ends, the end by six and mid-body, and 3727 is
`// Use a custom Sender to allow multiple inflight requests` — a body comment, not an
annotation. The class is **seven** of nine at a single end, with **two** large deviations, and
**two** of the three pre-existing start-line slips cited an annotation while the third cited
the first body line.

The bullet now carries the nine-row table as pasted output, with `Δstart`/`Δend` and what the
cited line holds, followed by the two exceptions named individually. That is the fix the
coordinator asked for — re-derive every row rather than patch the named one — and it also
removes the failure mode: this taxonomy was written wrong twice, once per round (pass-1 Issue
10, pass-2 Issue 13), both times as prose beside correct corrections. Derivation output cannot
drift from the thing it describes.

Verified: the widened sweep still reports 102 headers (52 + 50), 0 mismatches; `sender.rs`
tests exit 0.

---

## Issue 14: PLAN §9.28's appeal to CLAUDE.md §11 misapplies the rule it cites

- **File**: `design/history/Milestone-11/PLAN.md` §9.28, final paragraph
- **Severity**: Missing Requirement (record — the justification for a design conclusion a later phase would act on)
- **Java Reference**: `Sender.java` `sendProduceRequest`; CLAUDE.md §11

**Description.** The framing the coordinator asked me to adjudicate — "recording a
consequence rather than proposing to reverse it" — is **accurate and the right call**, and
§9.28 is otherwise the best of the three new sections: the Java mechanism, the false claim it
replaces, the reason the port cannot reproduce it, and the substitute test's four assertions
are all correct as written. One sentence is not:

> the faithful fix is not the overload but the routing: threading a per-request completion
> handler through the produce path the way Java does. That is a send/receive-path change and
> **CLAUDE.md §11 warns against per-message callbacks on the hot path**, so the current
> design is very likely the right one […]

Two problems, and they compound:

1. **§11 states no such rule.** Its four bullets are `Arc<str>` for identifiers cloned per
   message, atomics over `Mutex<i64>`, no `Pin<Box<dyn Future>>` per call on hot paths, and
   no per-message `tokio::spawn` on the send path. None is about completion callbacks.
2. **§11's own scope note excludes the granularity being dismissed.** The rule's **"Hot
   path" definition** reads: "per-record / per-message dispatch (send-path record build,
   batch drain, deserialize/serialize, wire framing). This does **not** include per-RPC or
   per-batch top-level API surfaces […] there, one `Pin<Box<dyn Future>>` per call is
   amortized over many records and is negligible." A `RequestCompletionHandler` on the
   produce path is **one per produce request**, covering every batch in it across every
   partition — per-RPC by construction, which is precisely what §11 carves out. The sentence
   also calls it a "per-message callback", which mis-states the alternative it is rejecting.

So the conclusion ("the current design is very likely the right one") is asserted on a rule
that does not reach it. The conclusion is probably still right — but for a reason this port
already documents, three times, in the file §9.28 is about: **a Rust
`RequestCompletionHandler` cannot capture `&mut self`** (`sender.rs:182`, `:348`, `:374`).
That is a structural obstacle, not a performance budget, and it is why
`pending_produce_responses` exists at all.

This matters because §9.28 is what a later phase reads before deciding whether to touch the
routing. As written it says "a rule forbids it", so the real trade-off — an ownership problem
with known workarounds, against Java fidelity — never gets weighed. Same family as Phase 4
lesson 13 and Phase 6 §5: a justification that proves a different claim than the one it
states, in a section that will be cited.

**Expected.** Drop the §11 appeal, or scope it correctly (§11 explicitly exempts per-RPC
surfaces, so it does not bear on this). State the actual obstacle — a Rust completion handler
cannot capture `&mut self`, which is why produce responses are processed after `poll()`
returns — and note that this makes the current design a consequence of Rust's ownership
model rather than of a performance rule, which is a stronger reason not to reverse it.

**Actual.** The conclusion rests on a rule whose stated scope excludes the case, and
describes a per-RPC handler as a per-message callback.


**Resolution — conceded; the rationale is moved to the ownership ground with the three
citations.**

Both problems verified against the sources before accepting. CLAUDE.md §11's four bullets are
`Arc<str>` for per-message identifiers, atomics over `Mutex<i64>`, no `Pin<Box<dyn Future>>`
per call on hot paths, and no per-message `tokio::spawn` — none about completion callbacks.
And §11's "Hot path" definition reads, verbatim: "This does **not** include per-RPC or
per-batch top-level API surfaces". A `RequestCompletionHandler` is one per produce *request*,
covering every batch across every partition in it — per-RPC, exactly the carve-out. So the
appeal did not reach the case, and "per-message callback" mis-described the alternative.

The three ownership citations were each read and are each on point: `sender.rs:182`
(`PendingProduceRequest`'s doc — "because `handleProduceResponse` needs `&mut self`, we cannot
capture `self` inside the callback … process responses after `client.poll()` returns", citing
CLAUDE.md §9 as the sanctioned translation), `:348` (the same for the transactional handler
Java attaches at `Sender.java:504-505`), `:374` (why `batches_awaiting_response` must be an
explicit field — the field Phase 8's re-translated test now asserts on).

§9.28's closing paragraph now states that the obstacle is structural, quotes and withdraws the
§11 claim in place, gives the three citations, and frames the trade-off a later phase would
actually weigh: an ownership limit with known but invasive workarounds (interior mutability
over the Sender's state, or a channel from the handler back into the loop) against Java
fidelity in one test. The coordinator's point stands — that is a stronger reason than the
mis-cited budget, and it is now the reason on record.

The one other §11 citation in the PLAN (§10.6's per-record `Arc<str>` rebuild) was checked and
is a correct appeal to §11's actual first bullet.

---

# Not findings — checked and cleared this pass

  - **The "twice" framing.** `# The "twice", and how it is reached here` could be read as
    promising the doubling is reproduced; the body says the opposite in its third sentence
    ("That mechanism does not port") and the property under test is stated first. Java's own
    sequence is expiry-fails-the-batch (in `sendProducerData`) then *two* response
    handlings (disconnect, then late); the port has expiry plus *one*. Documented as a
    mechanism divergence in three places (test rustdoc, accounting entry, §9.28) and the
    property Java pins is preserved. Accepted.
  - **"whose purpose there is to stop the Sender sending anything new".** Under-states the
    dropped `disconnect`'s other role — it is also what produces Java's first delivery — but
    the immediately preceding clause says the batch is failed by expiry "instead of by a
    disconnect delivery", so the sentence is about the residual purpose. Marginal; not filed.
  - **`transaction.version` finalized at 2 ⇒ "every `EndTxn` returns a bumped epoch"** —
    correct for KIP-890 part 2, and consistent with
    `test_epoch_update_after_bump_from_end_txn_response_in_v2`. Not load-bearing for the
    decision either way.
  - **§9.19's two counts** ("four blocked entries" in the status line, "3 are blocked below"
    in the summary) are reconciled in the text and the Phase-4-era numbers are flagged as
    superseded by the `sender.rs` block, with the earlier 32/2 slip named. Internally
    consistent.
  - **`plain_producer`** extraction is a genuine de-duplication (it replaced the inline
    seeder in the consume-transform-produce test) and the seeded record's three knock-on
    expectation changes are all updated (`before == ["seed"]`, `uncommitted` 3 → 4 with
    `"seed"` first, and the post-commit read unchanged because the seed was already
    consumed).
  - **`1bdd846`'s grouping disclosure** — issues 7/9/10's diffs landed in `a49d060` whose
    message covers 1/5/6/8, stated rather than left to be noticed, with the reason
    (splitting one file's rewrite would have left neither commit building). Correct call and
    correctly disclosed.

*(The DoD §3 fifth-bullet suggestion from pass 1 is with the coordinator and is deliberately
not re-filed.)*
