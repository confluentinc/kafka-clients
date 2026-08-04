# Critic 43 — Milestone 11 Phase 3: CLOSED on a clean pass

Four Critic passes, **3 → 3 → 1 → 0**. Pass 4 returned zero findings, which is what
closes the phase per `agent-roles.md` §2. Nothing was substituted for it.

| Pass | Findings | Where the defect was |
|---|---|---|
| 1 | 3 | **1 real code defect** (inescapable `ABORTABLE_ERROR`) + 2 records |
| 2 | 3 | all in records written by the pass-1 fix |
| 3 | 1 | third wrong justification for `close` |
| 4 | **0** | — closes the loop |

**All seven findings were real and conceded by the Actor.** The only errors on the
Critic's side were two citation slips it introduced and corrected itself (`:163`→`:165`,
and its own `:1420`→`:1421`), plus one alternative resolution it withdrew.

## The finding that justifies running Actor and Critic as separate agents

Phase 3 was first attempted with the coordinator acting as its own Actor. Those commits
were dropped and the phase redone through the agent model. The reason it mattered shows
up in pass 1, finding 1.

Both the coordinator *and* Actor 43 independently concluded — correctly, and against
PLAN §Phase-3's stated count of four — that `ABORTABLE_ERROR` is reachable on a purely
idempotent producer. Both then added the methods that **enter** that state. **Neither
noticed that nothing leaves it.** Java always recovers via `Sender.java:325` →
`shouldHandleAuthorizationError` → `transitionToUninitialized` (`:354`), which is
precisely why the transition table admits `UNINITIALIZED ← ABORTABLE_ERROR` at all.

Shipped as-is, one authorization failure would have wedged the producer permanently:
`maybe_add_partition` rejecting every subsequent send with no path out.

Two independent agents made the same *half* of a finding. More Critic passes over
either version would not have found the other half — only a reviewer whose task was to
attack the conclusion rather than extend it. That is the argument for the model, and it
is not hypothetical.

## The recurring error, and the rule it produced

`close`'s justification was wrong **three times**:

  1. "prevents a hanging `InitProducerId` future" — refuted: nothing awaits it
     (`bumpIdempotentEpochAndResetIdIfNeeded` returns void; the only path handing a
     result out is `ensureTransactional`-guarded at Java `:1266`).
  2. "the `FATAL_ERROR` transition stops `runOnce` at `:318`" — refuted: `close()` at
     `Sender.java:292` is the Sender's terminal act, and both post-shutdown loops are
     `!forceClose`-guarded, so no `runOnce` executes at all.
  3. Residual: the same claim survived inside the record that had *introduced* it.

Each time the Actor had the true mechanism (Java's own comment at `:288-289`, "wake up
the threads waiting on the futures"), retracted it correctly for the idempotent path,
bounded it correctly to Phase 6 — and then reached for a *different* present-tense
mechanism rather than concluding there is none. **"No payoff until Phase 6, and that is
fine" was always sufficient and true.**

The generalisation, now recorded for all four methods: every method pulled forward into
Phase 3 has its behavioural payoff in Phase 4 or 6, because all four Java call sites are
in `Sender` (`:356`, `:354`, `:339`, `:292`) and none has a Rust production caller yet.

## Lessons worth carrying to Phase 4

  - **An all-green suite proves nothing about an exit path nobody tested.** The
    entry-only version of Phase 3 passed 26 tests and a clean fidelity sweep.
  - **A plan that schedules an exit method into a later phase than its entry is itself
    the smell.** PLAN put `transition_to_uninitialized` and `fail_pending_requests` in
    Phase 5 while Phase 4's table already claimed their call site.
  - **A test harness needs mutation-pinning as much as production code does.** Issue 6's
    wrong control-flow model was fully green, and PLAN had already designated that
    harness as Phase 4's reference. It is now pinned in both directions.
  - **Grep your own records for present-tense behavioural verbs.** The rule caught its
    own first instance one commit after being written.

## Verified clean at closure

`cargo build` 0, `cargo test` 0 (2380 passed, 6 ignored), `format-check` 0, `lint` 0,
`make verify-sandbox` **0** — the full local gate including 91 integration tests and 4 C
suites, which closes DoD §9 on evidence rather than the risk-acceptance pass 1 settled
for. Exit codes captured without pipes throughout, after a lint failure was masked by a
pipe earlier in this milestone.

---

# Pass 1-4 reports

# Critic 43 — Milestone 11 Phase 3 review (`ca75e95..7d7d3a7`)

**All seven findings (pass 1: 1-3, pass 2: 4-6, pass 3: 7) are resolved** and have
been moved to `COMMENTS.DONE.43.md`, together with the two suggested rule updates
(preserved there for the `agent-roles.md` §2 human process, not applied).

What remains below is Critic 43's adjudication record and DoD walk. The Critic
wrote "Recorded so a later pass does not redo them. **The Actor should not change
any of the following.**" — so it is kept here rather than archived, for the next
review pass.

Reviewed: `59cad41`, `e589f9b`, `c4771c8`, `d7a385a`, `7d7d3a7` on
`milestone11-producer-transactions`. Primary artifact
`src/producer/internals/transaction_manager.rs`, Java contract
`kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java`
(Apache Kafka 4.2), phase spec `design/history/Milestone-11/PLAN.md` §Phase 3.

Note on line numbers: the tables below cite `transaction_manager.rs` line numbers
as of `7d7d3a7`. The Issue-1 fix inserted four methods, so lines after ~654 have
shifted; the method names are stable.

---

# Adjudications with no finding

### Claim 2 — scope of the additions: sound, and `TxnRequestHandlerKind` satisfies DoD §7

- `maybe_add_partition` (Java 437) is **forced**, not scope creep.
  `testFailIfNotReadyForSendIdempotentProducer` (Java 269-272) and
  `...FatalError` (Java 276-280) call nothing else; both are inside Phase 3's
  test set (the 15 `initializeTransactionManager(Optional.empty(), ..)` sites),
  so DoD §3 requires them. PLAN listing it under Phase 5 is the plan being
  wrong, not the Actor overreaching; the amendment at PLAN `:281-287` records it.
  Its Java second statement `throwIfPendingState("send")` (439) is correctly
  reasoned as a no-op for an idempotent producer — `pendingTransition` is set
  only by `handleCachedTransactionRequestResult` (1281), which opens with
  `ensureTransactional()`.
- `transition_to_abortable_error` / `has_error` / `has_abortable_error` are
  forced by `InitProducerIdHandler.handleResponse`'s authorization arm, which is
  inside the Phase-3-listed handler.
- `maybe_transition_to_error_state` is the first statement of the Phase-3-listed
  `handleFailedBatch` (Java 789).
- The `TxnRequestHandler` surface (`next_request`, `enqueue_request`, `retry`,
  `maybe_terminate_request_with_error`, `set/clear_in_flight_correlation_id`,
  `has_in_flight_request`, `needs_coordinator`) is what the 12 translated tests
  need to drive an `InitProducerId` round trip in place of Java's
  `Sender.runOnce` + `MockClient`; without it `initializeIdempotentProducerId`
  cannot be reproduced at all.
- `TxnRequestHandlerKind` vs DoD §7: it has no Java counterpart, but it replaces
  Java's six `TxnRequestHandler` subclasses, the need is explained at
  `transaction_manager.rs:213-228` **and** in PLAN §10.5 deviation 2, and the
  crate already uses the identical pattern for `AbstractRequest` /
  `AbstractResponse` (`ConcreteRequest` / `ConcreteResponse`). That is what DoD
  §7 asks for. It also fails loud rather than silently when Phase 5 adds
  variants: `handle_init_producer_id_response` uses an irrefutable
  `let TxnRequestHandlerKind::InitProducerId { .. } = &handler.kind`, and
  `request_builder` / `priority` / `is_end_txn` / `coordinator_type` /
  `coordinator_key` / `Debug` all use single-arm `match`, so a second variant is
  a compile error at every site.
- The `Caller` enum is mandated by rules §1, and `InFlightBatchPool` is a type
  alias (adds no struct) whose per-partition keying is correctly justified —
  `InFlightBatchKey` is `(producer_id, epoch, base_sequence)` and genuinely
  collides across partitions.

### Claim 3 — `Caller` hardcoding: verified correct at all three sites

Checked every Java caller, not just the Phase-3-reachable ones:

- `on_complete` (Java `TxnRequestHandler.onComplete`, 1406) implements
  `RequestCompletionHandler`; the only invoker is `NetworkClient.poll`'s
  `completeResponses`, on the Sender thread. `Caller::Sender` is right and will
  stay right.
- `fatal_error` (Java 1357) / `abortable_error` (Java 1362): callers are
  `onComplete` and the six `handleResponse` implementations (Sender), plus
  `authenticationFailed` (939), `failPendingRequests` (944) and `close` (949) —
  and all three of *those* are called only from `Sender.java:339`, `:354` and
  `:292` respectively, i.e. also the Sender thread. So they are Sender-only even
  at full Phase-5/6 scope, and rules §1's "reachable from both sides" anti-pattern
  does not apply.
- Every other transition-capable method forwards rather than defaults:
  `transition_to`, `transition_to_fatal_error`, `transition_to_abortable_error`,
  `maybe_transition_to_error_state`, `reset_idempotent_producer_id`,
  `bump_idempotent_producer_epoch`,
  `bump_idempotent_epoch_and_reset_id_if_needed`, `handle_failed_batch`.
  `maybe_transition_to_error_state` is the genuinely two-sided one
  (`KafkaProducer.java:1066` app-side, `TransactionManager.java:789` Sender-side)
  and it is parameterised. `maybe_resolve_sequences` correctly takes none — its
  idempotent arm performs no transition (PLAN §10.5 deviation 6).
  `maybe_add_partition` correctly takes none — Java's `maybeAddPartition` throws
  directly and never calls `transitionTo`.

### Claim 4 — both conclusions re-derived independently and confirmed

(a) **Five states, not four.** `InitProducerIdHandler.handleResponse`
`:1524-1528` calls `abortableError(error.exception())` for
`CLUSTER_AUTHORIZATION_FAILED` with no `isTransactional()` test, and no
`ensureTransactional()` exists anywhere on
`abortableError → transitionToAbortableError → transitionTo`. The manager is in
`INITIALIZING` when the response lands (set at
`bumpIdempotentEpochAndResetIdIfNeeded`, `:669`), and `ABORTABLE_ERROR` accepts
`INITIALIZING` as a source (`:180`). `CLUSTER_AUTHORIZATION_FAILED` is what a
non-transactional `InitProducerId` gets when the principal lacks
`IdempotentWrite`. Confirmed reachable. (Issue 1 extended this conclusion; it did
not contradict it.)

(b) **Empty slice, not skip.** `TxnPartitionMap.startSequencesAtBeginning`
(`TxnPartitionMap.java:175-179`) calls `get(topicPartition)`, whose throw at
`:143-147` fires only on a missing **entry**. With an entry present but
`inflightBatchesBySequence` empty, `TxnPartitionEntry.resetSequenceNumbers`
(`TxnPartitionEntry.java:154-161`) iterates zero times and
`startSequencesAtBeginning` (`:116-124`) still lands `nextSequence = 0`,
`lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER` and the new producer
id/epoch. `testProducerIdReset` (`TransactionManagerTest.java:863-878`) pins it:
`tp0` gets an entry via `sequenceNumber(tp0)` and reaches 3 via
`incrementSequenceNumber`, never an in-flight batch, and after the bump
`sequenceNumber(tp0)` must be 0 while the unqueued `tp1` stays 3. Skipping would
fail that assertion. The Rust choice is correct, and
`test_epoch_bump_distinguishes_an_empty_in_flight_set_from_a_missing_entry` pins
both halves of the distinction, including the missing-entry error message.

### Method-by-method fidelity sweep — no divergence found

Checked against the Java line numbers PLAN enumerates. All 38 listed methods are
present. Specifically verified:

- Transition table against Java `:162-188`, arm for arm, including the
  `ABORTABLE_ERROR` self-loop and the absence of `READY → READY`.
  `test_transition_table_matches_java` asserts all 81 cells.
- `transition_to` against Java `:1118-1145`: poison ordering, the
  `IllegalArgumentException` branch for a null error, `lastError = null` on the
  success arm, and both message texts (`"…Invalid transition attempted from state
  X to state Y"`, `"Cannot transition to X with a null exception"`).
- `maybe_fail_with_error` against Java `:1152-1172`: arm order is
  safe — `ProducerFencedException` and `InvalidProducerEpochException` both
  extend `ApplicationRecoverableException`, neither extends the other, and
  `KafkaError::error()` returns `UnknownServerError` for `IllegalState`, so arms
  1-2 cannot shadow arm 3. All four message texts reproduce Java byte-for-byte,
  including `ProducerIdAndEpoch`'s `(producerId=…, epoch=…)` rendering.
- `can_retry` against Java `:1015-1081`. Java's
  `if (A) return true; else if (B) { … return true; }` is behaviourally identical
  to the flattened Rust `if`s because the first arm returns. The
  `OUT_OF_ORDER_SEQUENCE_NUMBER` block, including the nested
  `!hasUnresolvedSequence || isNextSequenceForUnresolvedPartition` epoch-bump
  gate, is arm-for-arm. `NO_LAST_ACKED_SEQUENCE_NUMBER` (not `INVALID_OFFSET`)
  is used as the `lastAckedOffset` default, matching Java `:1063`.
- `handle_failed_batch` against Java `:788-822`, and rules §9 is
  correctly applied: `is_out_of_order_sequence` makes
  `Errors::UnknownProducerId` satisfy the first arm, and the
  `UnknownProducerId`-only arm sits *after* it, so an idempotent
  `UnknownProducerId` takes the epoch-bump path as Java's subclass relation
  requires.
- Rules §8: `TxnPartitionEntry::decrement_sequence` is plain subtraction
  returning `Err`, `increment_sequence` delegates to the wrapping helper.
  `test_sequence_number_overflow` pins the wrap at
  `i32::MAX → 99 → 98`, matching Java `:849-861`.
- Integer widths: `update_last_acked_offset` widens `record_count` to `i64`
  before adding; `epoch + 1` in `bump_idempotent_producer_epoch` is guarded by the
  `i16::MAX` branch above it; `base_sequence` comparison in
  `adjust_sequences_due_to_failed_batch` widens to `i64` as Java's `long`
  parameter does.
- `next_request`: `front()` / `is_end_txn` / `pop_front` /
  `maybe_terminate_request_with_error` order matches Java `:894-932`. The
  `VecDeque`-for-`PriorityQueue` deviation is safe in this phase — I traced every
  `enqueue_request` caller and at most one `InitProducerId` can be pending
  (`bump_idempotent_epoch_and_reset_id_if_needed` is gated on
  `current_state != Initializing && !has_producer_id()`, and `retry` re-enqueues
  the one that was just dequeued), so FIFO and priority order coincide.
- Test harness: `ClientResponse::new` and `PartitionResponse::new` argument
  orders match the Java constructors the tests mirror; `write_idempotent_batch_with_value`
  reproduces `writeIdempotentBatchWithValue` (`TransactionManagerTest.java:811-822`)
  statement for statement, including the trailing `batch.close()`.
- `start_sequences_at_beginning` is reached with the pool in tracked-key order,
  and `TxnPartitionEntry::reset_sequence_numbers` drives membership from its own
  key set (rules §6/§7), so `test_bump_epoch_and_reset_sequence_numbers_after_unknown_producer_id`'s
  `0,1,2,3` assertions are load-bearing rather than an artefact of pool order.

### The three untranslated tests do need Phase-4 surface

Verified each against the Java source:

- `testDuplicateSequenceAfterProducerReset` (Java 748-809) constructs `Metrics`,
  `RecordAccumulator` and `Sender`, appends via `accumulator.append(..)` and
  drives `sender.runOnce()` across request and delivery timeouts. Needs
  `RecordAccumulator.drainBatchesForOneNode` sequence assignment and
  `Sender.failExpiredBatches → markSequenceUnresolved` (`Sender.java:373-374`).
- `testHealthyPartitionRetriesDuringEpochBump` (Java 3600-3672) asserts on
  `accumulator.getDeque(tp1)`, requiring
  `shouldStopDrainBatchesForPartition` (`RecordAccumulator.java:815`).
- `testFailedInflightBatchAfterEpochBump` (Java 3725-3810) additionally needs
  `accumulator.reenqueue(..)`.

All three are correctly named with their missing surface at the end of the Rust
test module, per DoD §3. The enumeration of Phase-3 test scope is also correct:
`grep -c "initializeTransactionManager(Optional.empty()"` returns exactly 15, at
the lines PLAN lists.

### DoD, clause by clause

1. **CLAUDE.md + rules** — §2 naming: every item is `pub(crate)` as required for
   an `internals` package; `is_out_of_order_sequence` is exported only by its
   defining file, not re-exported through `mod.rs`; imports use the parent-module
   re-export form. §5 completeness: no `TODO`/`FIXME`/`todo!()`/`unimplemented!()`
   in the file — every unreached Java path returns
   `KafkaError::unsupported_version` naming Phase 5, which is what §5 asks for.
   §7 licence header present. §9.6.2 not applicable (no `.await` in this file).
   §10.1/§10.2: no `panic!` on any public path; `IllegalStateException` →
   `KafkaError::illegal_state`, `IllegalArgumentException` →
   `KafkaError::illegal_argument`. Rules §1, §6, §7, §8, §9 all satisfied (see
   above). Rules §2 — was Issue 3, resolved.
2. **All methods implemented** — all 38 PLAN-listed items present; the gap was the
   three the plan did not list (was Issue 1, resolved — four landed).
3. **All tests translated** — 12 of 15, 3 named with reasons.
   `@ParameterizedTest @ValueSource(booleans = {true,false})` correctly becomes a
   `for … in [true, false]` loop in all 12, and the note that
   `transaction_v2_enabled` is unobservable in this phase is accurate: the flag
   reaches the manager only through `apiVersions`, read solely by
   `handleCoordinatorReady` (1104) and `maybeUpdateTransactionV2Enabled` (493),
   both Phase 5. Error-message content is asserted, not just `is_err()`, at every
   error site. Byte-level wire tests: PLAN §9.14, not re-reported.
4. **Blockers** — none; `TxnPartitionMap` / `TxnPartitionEntry` /
   `TransactionalRequestResult` / `InitProducerIdRequest` all landed in Phases
   1-2.
5. **Tests passing** — 2378 at review time, per the review brief.
6. **No duplication** — one `TransactionManager`; `mod.rs` declares it once.
7. **Non-Java types** — `Caller` (rules-mandated), `TxnRequestHandlerKind`
   (explained, DoD §7 satisfied), `InFlightBatchPool` (type alias). Two
   accessors with no Java counterpart — `transaction_timeout_ms()` and
   `api_versions()` — exist only to give fields a reader; the file already
   carries `#![allow(dead_code)]`, so they are strictly unnecessary. Noted, not
   filed: they are documented at the site and Phase 5 gives both a real caller.
8. **No TODO/FIXME** — confirmed.
9. **`make verify`** — not independently re-run here; the Actor's memory note
   records that `make verify` cannot complete on this machine
   (`build-python`'s C extension includes `<threads.h>`, absent from the Apple
   SDK) and that `make verify-sandbox` was used instead. Phase 3 adds a
   `pub(crate)` module with no FFI surface and no `cbindgen`-visible symbols, so
   the C/Python suites are not at risk from this change. Accepted.
10. **Hot-path allocation audit** — the struct doc is accurate: the
    drain-path methods (`sequence_number`, `increment_sequence_number`,
    `add_in_flight_batch`, `maybe_update_producer_id_and_epoch`) are per-batch,
    not per-record, and allocate a `TopicPartition` clone only where Java also
    inserts into a map or set. The two `Vec<TopicPartition>` collections are
    per-`runOnce` over error-state partitions only.
11. **Consumer trait surface** — not applicable.

### PLAN and agent-memory amendments — accuracy

- PLAN §2 and §Phase-3: both amendments are accurate, and
  the "these two clauses contradict each other" resolution is right — a
  four-variant enum cannot carry the nine-variant table, and declaring all nine
  is the faithful translation.
- PLAN §Phase-3 (added-methods list, test list): verified item by
  item; every entry is forced by a listed item or a named test.
- PLAN §9.15: the reachability finding is correct; the "Why it matters"
  paragraph was not (Issue 2, resolved) and the "Consequence for the plan"
  paragraph was incomplete (Issue 1, resolved).
- PLAN §10.5: all six deviations are accurately described and each is documented
  at its call site as claimed. The three "not deviations" notes are also
  accurate — I independently confirmed the `VecDeque` argument and the
  `maybe_terminate_request_with_error` omission (`FindCoordinatorHandler` does
  not exist, so Java's escape hatch could only ever be false). The gap was the
  seventh deviation (Issue 3, resolved).
- `.claude/agent-memory/actor-executor/phase3_transaction_manager_notes.md`:
  lessons 1-5 are accurate as written. Lesson 1 inherited Issue 1's
  incompleteness — its own advice ("a reachability claim has to be checked
  against every writer of the state") was applied to the writers that *enter*
  `ABORTABLE_ERROR` but not to the writer that *leaves* it
  (`transitionToUninitialized`, `TransactionManager.java:756`). Amended with the
  Issue-1 fix, since the note is the durable artefact.

---

# Calibration against `COMMENTS.FP.md`

Read before filing. Two points from Critic 41's partially-rejected finding shaped
this pass:

- *"`pub` on a function whose parameters are `pub(crate)` types does not make it
  externally reachable. Check argument constructibility before characterising
  something as a public bypass."* Applied to Issues 1 and 2: every reachability
  claim was traced through a concrete Java call chain with line numbers
  (`InitProducerIdHandler.handleResponse:1524` → `abortableError:1362` →
  `transitionToAbortableError:530` → `transitionTo:1118` with the `:180` table
  arm; then `Sender.runOnce:325` → `shouldHandleAuthorizationError:351` →
  `failPendingRequests:944` / `transitionToUninitialized:756`), not from a
  method's visibility or name.
- The accepted half of that finding was *"the genuine defect was that the
  deviation was not recorded where a reader would find it."* Issue 3 was filed on
  exactly that basis, and deliberately **not** as a rules violation — the Critic
  stated plainly that rules §2 is not violated today.

No `COMMENTS.FN.md` exists in the tree, so there are no recorded false negatives
to calibrate against.

---

# Pass 2 — review of the fixes in `1bc8a8c`

Three findings (Issues 4-6), **all resolved** and moved to
`COMMENTS.DONE.43.md`. All three were in records the pass-1 fix wrote, not in the
code; none required rework of the four new methods. Summaries, for orientation:

  - **Issue 4** — deviation 7's prose said "ten reshaped signatures" against its
    own thirteen-row table, and cited the `synchronized` block at
    `TransactionManager.java:1420` instead of `:1421`.
  - **Issue 5** — `close`'s stated justification (a hanging future on force close)
    described a mechanism that does not exist on the idempotent path. The method
    stays; the reason was replaced with the `FATAL_ERROR` transition and the
    scheduling gap.
  - **Issue 6** — `run_sender_transaction_phase`, which PLAN designates as
    Phase 4's reference, omitted `Sender.java:333-335` and labelled that case
    `Continued`, the opposite of Java; and it modelled Java's
    `AuthenticationException` as `Errors::SaslAuthenticationFailed`.

The pass-2 adjudications below were **not** part of any finding and record what the
Critic checked and found sound. Line numbers are against the tree at `1bc8a8c`;
the Issue 4-6 fixes shifted them again.

---

# Pass-2 adjudications with no finding

**The four new methods are correctly translated.** Each checked against Java line
by line:

- `transition_to_uninitialized` (`:717-724`) vs Java `:756-762`: transition
  first, then `last_error = None`; the `?` short-circuits before the clear exactly
  as Java's throw would. The redundant explicit clear is kept and labelled —
  right call.
- `fail_pending_requests` (`:739-747`) vs Java `:944-947`: per handler,
  `result.fail(e)` then `transitionToAbortableError(e)`, in that order, queue not
  cleared. `ABORTABLE_ERROR → ABORTABLE_ERROR` is the table's self-loop, so the
  repeated transition is valid — and it is the *reachable* case, since
  `Sender.java:325` only enters when `hasAbortableError()`.
- `authentication_failed` (`:766-774`) vs Java `:939-942`: `result.fail(e)` then
  `transitionToFatalError(e)` per handler.
- `close` (`:788-798`) vs Java `:949-956`: `shutdown_error` built once outside the
  loop as Java builds `shutdownException` once; `Errors::UnknownServerError` for
  the bare `KafkaException`, matching the crate's convention; message text exact;
  `pendingTransition` branch deferred with a pointer.
- All three loops index `0..len()` with `len()` snapshotted, which is safe because
  neither `fail` nor either transition touches `pending_requests` — verified.
  Java's `forEach` over the live queue is equivalent for the same reason.
- All three correctly inherit Java's quirk that an **empty** queue means no
  transition at all.
- Iterating by index rather than calling the existing `abortable_error` /
  `fatal_error` helpers is forced by the borrow checker
  (`self.pending_requests[i]` borrows `self` while the transition needs
  `&mut self`), and the inline comments name the Java call the two statements
  stand for. Noted, not filed: these three `pub(crate)` methods take `caller`
  while the private `fatal_error` / `abortable_error` hardcode `Caller::Sender`,
  even though all five are Sender-only. The implicit criterion — a `pub(crate)`
  method's callers live outside this file and must declare their origin — is
  coherent and safer than hardcoding, just unstated.

**Reachability of all four is confirmed.** `transitionToUninitialized` and
`failPendingRequests` via `Sender.java:325` → `:351-360`. `authenticationFailed`
via the `catch` at `Sender.java:336-340`, reachable idempotently because
`maybeSendAndPollTransactionalRequest` takes the `coordinatorType == null` branch
at `Sender.java:479-484` and still calls `awaitNodeReady`
(`NetworkClientUtils.awaitReady`, which throws) — the Actor's argument holds.
`close` via `Sender.java:287-293`, where `transactionManager != null` is true for
an idempotent producer. Pulling all four into Phase 3 is right: each is a
`TransactionManager` method, each is reachable idempotently, and none needs
Phase-5 state beyond the `pendingTransition` branch. `close`'s *reason* is wrong
(Issue 5); its *placement* is not.

**`transition_to_uninitialized` taking no `error` is correct — guard verified
independently.** `grep -n "pendingTransition"` gives eight sites in
`TransactionManager.java`. The only non-null assignment is `:1281`, inside
`handleCachedTransactionRequestResult`, whose first statement is
`ensureTransactional()` at `:1266`; the two other writes (`:1252`, `:1270`) set it
to `null`. So it is unconditionally null for an idempotent producer and the
parameter has no consumer. Deviation 8 records this accurately.

**Issue 2's fix, including the bounding, is correct.** The rewritten §9.15 is
accurate: `Sender.java:325` returns at `:326` before `:331`; the `instanceof` at
`:352-353` is always satisfied idempotently because the authorization arm at
`:1524-1528` is the only entry; and `UNINITIALIZED ← ABORTABLE_ERROR` at Java
`:165` is the table arm that exists for it (the Actor's `:165` citation is right —
my pass-1 note said `:163`, which is the `switch`). The bounding is not a new
false statement: I derived the same exception in pass 1 — a non-authorization
idempotent `ABORTABLE_ERROR` reaches `:331`, attempts
`ABORTABLE_ERROR → INITIALIZING`, is refused, and poisons on the Sender side —
and `Errors.TRANSACTION_ABORTABLE` (Java `:1533`) is indeed the only other
`abortableError` arm a non-transactional `InitProducerId` could take. Keeping the
retracted text as a marked **Correction** block is better than deleting it: the
failure mode was reasoning, and deleting the reasoning would lose the lesson.

**Issue 3's fix: resolution (a) is right and declining (b) is right.** The
deviation-7 table is correct (Issue 4 is about the prose figures around it). On
(b) I withdraw it. My wording proposed narrowing §2 to the four
`volatile`-adjacent fields and dropping `pendingRequests` from the Sender-owned
list, which would not have bought the simplification I claimed —
`on_complete` touches `in_flight_request_correlation_id` too, so it would still
have to be split and the signature reshape would be almost unchanged. The Actor's
counter-argument is also correct on the merits: Java's `onComplete` enters the
monitor only at `:1421`, for `handleResponse` alone, leaving the correlation-id
clear at `:1410`, the disconnect/`reenqueue` path at `:1411-1415` and the
version-mismatch path at `:1416-1417` outside it — so putting all of
`on_complete` under the shared lock would hold a lock across queue mutation that
Java deliberately does not, and would work against rules §4 if `on_complete` ever
gains an `.await`. (a) it is.

**The `#[cfg(test)]` door is acceptable.** It does not enable a false assertion:
every property `test_pending_requests_are_failed_in_bulk` checks — per-handler
failure, the `ABORTABLE_ERROR` self-loop, the queue not being cleared — is a real
Java property of `failPendingRequests` / `authenticationFailed` / `close`. It does
let the test reach a two-element queue, which the Phase-3 production path cannot
(at most one `InitProducerId` is ever pending, as deviation 7's "not a deviation"
note records) — but N>1 is Java-real and becomes reachable in Phase 5, so
exercising the loop now beats shipping an untested loop body into Phase 5. Two
observations, neither filed: the door duplicates the handler construction from
`bump_idempotent_epoch_and_reset_id_if_needed` verbatim, so a Phase-5 change to
the production construction will silently not reach the test; and the door was not
strictly necessary — the test module is a child of this module, so
`Arc::clone(manager.pending_requests[0].result())` after the natural enqueue would
have observed the same result with no new method.

**No regression from the harness signature change.** `run_sender_transaction_phase`
now returns `SenderPhaseOutcome`, and the three pre-existing call sites
(`test_bump_epoch_and_reset_sequence_numbers_after_unknown_producer_id`,
`test_bump_epoch_after_timeout_without_pending_inflight_requests`,
`test_epoch_bump_after_last_in_flight_batch_fails_idempotent_producer`) discard
it. None is in a fatal or abortable state at the call, so the two new guards
cannot fire and their behaviour is unchanged. Test count 2378 → 2380 matches the
two new tests exactly.

**Records other than the three above are accurate.** PLAN §Phase-3's
added-method list, the §9.15 consequence table (four rows, correct old/new
scheduling), the removal of `transition_to_uninitialized` /
`fail_pending_requests` / `transition_to_abortable_error` / `has_abortable_error`
from the Phase-5 lists, deviation 8, the new "not a deviation" note on index
iteration, and the amendment to lesson 1 in
`.claude/agent-memory/actor-executor/phase3_transaction_manager_notes.md` (which
now carries the exits half, plus the two corollaries about mutation-pinning an
exit path and treating an entry/exit phase mismatch as the smell) all check out.
`COMMENTS.DONE.43.md` preserves both suggested rule updates with the Actor's
position on each, and edits neither `CLAUDE.md` nor `.claude/rules/` — correct per
`agent-roles.md` §2.

**Scope of what pass 2 did not re-check.** The pass-1 fidelity sweep, DoD walk and
Claim 2/3/4 adjudications above were not repeated; nothing in `1bc8a8c` touches
the code they covered beyond the struct-doc paragraph and the test harness.

---

# Pass 3 — review of the fixes in `3131600`

One finding. Issues 4 and 6 are fully and correctly resolved. Issue 5 is not:
the retracted mechanism was replaced with a second mechanism that is also false,
in the same three places.

---

## Issue 7 (RESOLVED — moved to `COMMENTS.DONE.43.md`)

`close`'s *replacement* justification was also false: all three records claimed
the `FATAL_ERROR` transition "stops `Sender.runOnce` at `:318`", but `close()` is
the Sender task's terminal act — its sole call site (`Sender.java:292`) sits
inside `if (forceClose)` at `:287`, after all three `run()` loops, and both
post-shutdown loops are `!forceClose`-guarded, so no `runOnce` follows. Java names
the real intent at `:288-289` ("wake up the threads waiting on the futures"),
which is the Phase-6 transactional mechanism Issue 5 had already bounded. The
method stays; the justification is now scheduling plus a Phase-6 payoff, with no
idempotent-path effect claimed.

---

# Pass-3 adjudications with no finding

**The single remaining `:1420` is intentional and correct — not re-filed.**
PLAN `:1900-1904` reads: "begins at `:1421` and wraps `handleResponse` alone.
(`:1420` is the continuation line of the preceding `log.trace` argument list, not
the block opener; PLAN §6.5 and `.claude/rules/producer-transactions.md` §2 both
cite `:1421` correctly.)" That is exactly what it claims to be — an inoculation
against reintroducing the wrong number, not a surviving instance of it. Verified
against the source: `:1419-1420` is the two-line `log.trace(...)` call and `:1421`
is `synchronized (TransactionManager.this) {`.

**The `onComplete` range `:1406-1428` is right.** `:1406` is
`public void onComplete(ClientResponse response) {` and `:1428` is its closing
brace. The previous `:1425` cut the method off mid-`else`. The added clause
"covers `handleResponse` alone" is also correct — the `synchronized` block is
`:1421-1423` and contains only the `handleResponse` call.

**Issue 4 is otherwise clean.** `grep -nE "ten reshaped|ten method signatures|reshaping ten"`
over `PLAN.md` and `transaction_manager.rs` returns nothing; the three former
"ten"s now read "thirteen" at PLAN `:346`, PLAN `:1894` and
`transaction_manager.rs:404`, matching the thirteen-row table.

**Issue 6's predicate is genuinely equivalent to Java, not merely
test-consistent.** Derived independently rather than checked against the tests.
`maybeSendAndPollTransactionalRequest` returns `true` iff
`hasInFlightRequest() || nextRequest(hasIncomplete) != null` — the return census
in the doc is exactly right (one `return false` at `Sender.java:474`; six
`return true` at `:463`, `:487`, `:492`, `:497`, `:510`, `:516`; I counted them in
the source). `nextRequest` (`TransactionManager.java:894-932`) returns null at
four places:

  - `:900` `peek() == null` → modelled by `!has_pending_requests()`;
  - `:904` `isEndTxn() && hasIncompleteBatches` → the excluded case;
  - `:910` `maybeTerminateRequestWithError` → modelled by `has_error()`, and
    exactly so, because Java's `hasAbortableError() && instanceof FindCoordinatorHandler`
    escape at `:1176-1178` cannot fire without that handler;
  - `:925` the `isEndTxn() && !transactionStarted` re-poll → also `isEndTxn`-gated.

  Both null paths the predicate omits are `isEndTxn()`-gated, and
  `TxnRequestHandler::is_end_txn` returns `false` for the only variant this phase
  can build — so `has_in_flight_request() || (has_pending_requests() && !has_error())`
  is not an approximation on Phase-3-reachable states, it is the full disjunction.
  `:895-896`'s `newPartitionsInTransaction` enqueue is transactional-only, as
  already recorded. The predicate is also correctly placed *after* `:331`,
  matching Java, and correctly short-circuits `has_in_flight_request()` first as
  `:460` does.

  The `:903` exclusion is correctly identified and correctly argued. Two notes,
  neither a defect: the `!has_error()` conjunct is unreachable in Phase 3 (getting
  there needs an abortable error whose `lastError` is not an authorization
  exception, i.e. `TRANSACTION_ABORTABLE`, transactional-only) but it is the
  faithful model and the right thing to leave for Phase 4; and the predicate is a
  model of Java's *return value*, not of `nextRequest`'s dequeue-and-fail side
  effect — which the doc says at the site, and which the tests supply by hand
  immediately afterwards.

**Both mutation directions genuinely discriminate — verified by deriving the
state at each call site, not by trusting the claim.**

  - Forcing the predicate false: in
    `test_idempotent_producer_recovers_from_abortable_error_to_uninitialized`,
    the second harness call runs `bump_idempotent_epoch_and_reset_id_if_needed`
    from `Uninitialized` with no producer id, which transitions to `Initializing`
    and enqueues a handler; `on_complete` had already cleared the correlation id,
    so the predicate is `false || (true && true)` = true. Forcing it false yields
    `Continued` and the assertion fails.
  - Forcing it true: in
    `test_bump_epoch_and_reset_sequence_numbers_after_unknown_producer_id`, the
    producer id is still valid at the harness call, so the bump enqueues nothing;
    `initialize_idempotent_producer_id` had dequeued and completed the only
    handler, so the queue is empty and the correlation id cleared — predicate
    `false || (false && ..)` = false. Forcing it true yields
    `ReturnedOnTransactionalRequest` and the assertion fails.

  The two tests therefore pin opposite sides of the same guard, and neither passes
  for an unrelated reason. `ReturnedOnTransactionalRequest`'s own doc ("`:334` …
  `sendProducerData` (`:344`) is **not** reached in this iteration") and
  `Continued`'s ("Fell through past `:335` to `sendProducerData` at `:344`") now
  both describe Java correctly. The revised "three of `runOnce`'s steps have no
  Phase-3 counterpart" list is accurate and complete: `abortUndrainedBatches`
  (`:467`, `:469`) is inside `maybeSendAndPollTransactionalRequest` and so is
  subsumed by the third bullet, not a fourth omission.

**Issue 5's bounding of the hanging-future claim is correct.**
`KafkaProducer.java:654` is `result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);`
inside `initTransactions()`, three lines after
`transactionManager.initializeTransactions(false)` at `:652` — so the concern is
real from Phase 6 on the transactional path, exactly as stated. The Rust-side
claim also checks out: `await_result` and `await_result_timeout` exist
(`transactional_request_result.rs:108`, `:124`) and have no production call site.
Keeping the retraction as a marked Correction block rather than deleting it is the
same move I approved for Issue 2, and it is right here for the stronger reason the
block itself gives: this scope expansion was the Actor's own initiative, so its
reasoning has to stay auditable. (What is now wrong is only the sentence the
Correction points *forward* to — see Issue 7.)

**The `Errors::UnknownServerError` change is consistent and correctly reasoned.**
Java's `AuthenticationException` base class has no `Errors` entry — only
subclasses like `SaslAuthenticationException` (`Errors.SASL_AUTHENTICATION_FAILED`)
do — so the codeless-exception convention applies. All four sites now agree:
`close` (`with_message(UnknownServerError, "The producer closed forcefully")`),
`maybe_fail_with_error`'s fallback (`with_message(UnknownServerError, ..)`), the
harness (`fatal(UnknownServerError, ..)`) and `authentication_failed`'s doc, which
defers the choice to the caller and no longer conflicts with what the caller
supplies. Using `KafkaError::fatal` in the harness rather than `with_message` is a
deliberate and coherent difference — an authentication failure is fatal under
CLAUDE.md §10.3, a forced-shutdown `KafkaException` is not.

**The revised `test_pending_requests_are_failed_in_bulk` assertions are a real
improvement, not a relabelling.** The message assertion on `queued_result` is
still an echo of the test's own input, but it now pins that the handler is failed
with the *same* error object rather than a substitute; and the added
`manager.last_error()` assertions pin that the same error propagated into the
manager's state through the transition — which is Java's `handler.abortableError(e)`
/ `fatalError(e)` contract (fail the result, then transition *with that
exception*), and is not something the test supplies directly. The `close` block is
strongest: both assertions are against Java's literal `"The producer closed
forcefully"`, which the production code produces, not the test.

**No regression.** The three pre-existing harness callers still compile and pass;
one now asserts `Continued` and the other two discard the outcome, which is
harmless — all three are in the same fall-through state. Test count stays 2380
because no test was added this round. `make verify-sandbox` exiting 0 with no
latency flake closes DoD §9, which pass 1 had accepted on risk rather than on
evidence.

---

# Pass 4 — review of the fixes in `d982e13` and `1c29bbc`

## No findings

Issue 7 is resolved, the self-audit it triggered is accurate in all three
instances, and the generalisation it drew holds for all four methods. Phase 3 is
clean from my side.

One item is recorded below as an explicitly **non-blocking follow-up outside
Phase 3** — it is test infrastructure the phase never touched, and it is not a
comment for Actor 43 to fix before the phase closes.

---

# What I checked

**Issue 7's fix says exactly what it should and nothing stronger.**
`transaction_manager.rs:789-828` now opens "**On the idempotent path this method
has no observable effect at all**, and the reason it is here is scheduling, not
behaviour", carries the three scheduling reasons, and locates the behavioural
payoff in Phase 6 and transactional. Every citation verified against the source:

  - `Sender.java:290` is `if (transactionManager != null) {`, so the call site is
    reachable for an idempotent producer as claimed.
  - `Sender.java:292` is the sole `transactionManager.close()` call site in the
    whole client (`grep -rn` over `org/apache/kafka/clients/`).
  - `:287` is `if (forceClose) {`; `:295` `abortIncompleteBatches()` and `:298`
    `client.close()` are all that follow; `:258` and `:267` are both
    `!forceClose`-guarded. So "no `runOnce` executes at all" is right.
  - `Sender.java:288-289` is the quoted comment, verbatim.
  - `KafkaProducer.java:654` is `result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)`
    in `initTransactions()`.
  - Java 663-676 and Java 1266 (the no-hanging-future argument) were already
    verified in pass 3 and are unchanged.

  The additional checks the Actor reports also hold, and they strengthen the
  claim rather than just restating it: `throwIfProducerClosed`
  (`KafkaProducer.java:956-959`) rejects on `sender == null || !sender.isRunning()`,
  and `KafkaProducer` joins the I/O thread at `:1423` and `:1441` — so after
  `close()` writes `FATAL_ERROR` there is genuinely no reader left, on either
  side. Nothing in the comment now asserts a present-tense idempotent effect.

**§9.15's Corrections block is an accurate record of both retractions, not a
third framing.** It is one block titled "Corrections (Critic 43 issues 5 and 7)"
with an *Issue 5* sub-block (hanging future — mechanism absent idempotently,
bounded to Phase 6) and an *Issue 7* sub-block quoting the `:318` claim verbatim
and refuting it with the loop structure and the `!forceClose` guards. I compared
the *Issue 7* text against my own finding line by line: it says the same thing,
adds nothing, and overstates nothing. The closing "The instructive part is the
pattern" paragraph is also accurate — Issue 5's fix did identify the true
mechanism, retract it correctly for the idempotent path, bound it correctly to
Phase 6, and then reach for a different present-tense mechanism instead of
concluding there is none.

  Residual-claim sweep: `grep -rn ":318"` over `src/`, `PLAN.md` and
  `COMMENTS.DONE.43.md` returns eight hits, all legitimate — one unrelated
  (`buffer_pool.rs:660`, `BufferPoolTest.java:318`), two inside retraction text
  (`transaction_manager.rs:827`, `PLAN.md:1738`), one inside the struck-through
  reason (`COMMENTS.DONE.43.md:323`), and four correct statements about the test
  harness modelling `runOnce`'s `:318` guard, which is a different subject.

**Audit instance 1 — the §Phase-3 rewording is right, and Issue 1's framing does
not need changing.** The rewording is correct: none of the four new methods has a
production caller. All five call sites in `transaction_manager.rs` (`:2357`,
`:2360`, `:2669`, `:2681`, `:2690`) are inside `mod tests`, which begins at
`:2055`, and `on_complete` — the only route into `ABORTABLE_ERROR` — likewise has
no production caller. So "the translated state machine has no exit from
`ABORTABLE_ERROR` at all, so from Phase 4 … an idempotent producer … would reject
every subsequent send forever" is the precise statement, and the previous
present-tense version was not.

  On my own framing: Issue 1 said "there is no exit from `ABORTABLE_ERROR` for an
  idempotent producer: `has_error()` stays true, so `maybe_add_partition` rejects
  every subsequent send permanently … this client would wedge." That is a true
  statement about the translated unit and its conditional consequence, so it is
  not false — but it is imprecise in the same direction, and the Actor is right to
  flag that we both wrote it that way. I am not filing a change: the finding is
  resolved and lives in `COMMENTS.DONE.43.md`, while the durable forward-looking
  record (PLAN §Phase-3 and §9.15's lesson 2, both reworded) now carries the
  precise version. The lesson the Actor added to its own memory — "phrase such
  claims in the tense of the phase that makes them true" — is the right
  generalisation and applies to Critic comments as much as to Actor records.

**Audit instance 2 — the DoD §10 budget is unchanged in substance.** All four
substantive claims survive verbatim: the four drain-path methods run once per
**batch** not per record; they allocate no more than Java, a `TopicPartition`
clone only where Java also inserts into a map or set; nothing is per-record, so
§10's per-message budget is unaffected; and the two `Vec<TopicPartition>`
collections are per-`runOnce` over error-state partitions only. Only the tense
changed ("reaches" → "will reach", "run" → "will run") plus the added preface.
The preface's claim is also accurate — PLAN §Phase-4's table does put the wiring
in `RecordAccumulator`'s drain (between `deque.pop_front()` and `batch.close()`)
and `sender.rs:213-216`. My pass-1 adjudication of this section stands.

**Audit instance 3 — `COMMENTS.DONE.43.md`'s Issue-5 resolution is correctly
struck.** Reason 2 is struck through and annotated "**Also false — retracted
under Issue 7 below.** `close()` is the Sender task's terminal act; no `runOnce`
follows it … Only reason (1) survives, plus a Phase-6 payoff." The surviving
framing it keeps ("not urgent before Phase 6", "cheap, unscheduled, and
reachable" rather than "fixes a live defect") is the accurate half, as it says.
The self-aware parenthetical — that the new grep rule found its first residual
instance in the record that introduced the error — is true and is the right thing
to leave in.

**The generalisation holds for all four methods.** Verified both directions:

  | Rust | Java | Java call site | Rust production caller |
  |---|---|---|---|
  | `transition_to_uninitialized` | 756 | `Sender.java:356` | none |
  | `fail_pending_requests` | 944 | `Sender.java:354` | none |
  | `authentication_failed` | 939 | `Sender.java:339` | none |
  | `close` | 949 | `Sender.java:292` | none |

  `grep -rn` over `org/apache/kafka/clients/` confirms those are the *only* call
  sites for all four (the `authenticationFailed` hits in
  `ClusterConnectionStates.java:272` and `NetworkClient.java:888` are an unrelated
  method of the same name). So all four payoffs are in `Sender`, i.e. Phase 4
  (`runOnce`) or Phase 6 (`run`'s shutdown), and none can have a Phase-3 effect.

  The records now say so for all four: PLAN §Phase-3's second bullet locates the
  effect at "from Phase 4 — when the `Sender` wires the manager into `runOnce`",
  and the third says "Neither has any behavioural payoff *in Phase 3* — like the
  two above, their Java call sites are in `Sender.runOnce` / `Sender.run`, which
  Phases 4 and 6 translate." The first bullet
  (`transition_to_abortable_error` / `has_error` / `has_abortable_error`) needs no
  such qualification and correctly has none — it claims a *structural*
  requirement ("required by `InitProducerIdHandler.handleResponse`'s
  authorization arms"), not a behavioural payoff.

**Actor memory amendment is accurate.** The exits half of lesson 1 is reworded to
the Phase-4 tense, and the added "third half" — do not reach for a present-tense
behavioural payoff when a method is pulled in ahead of its call site; grep the
phase's own records for "stops/prevents/would leave" before declaring it done —
is the correct generalisation of a three-round error.

---

# Follow-up outside Phase 3 — does NOT block closing

Recorded so it is not lost, and deliberately not written as a Phase-3 comment:
`tests/common/kafka_cluster.rs` was not touched by this phase and Actor 43 should
not be asked to fix it before Phase 3 closes.

**Leaked broker containers when `KafkaCluster::start` aborts.** The diagnosis is
right and the mechanism is visible in the file. Containers are created inside
`tokio::spawn`ed tasks (`:361-375`) and only become owned by
`KafkaCluster::_containers` (`:285`) after the collection loop at `:379-382` and
the remainder of `start()` complete. There is no `impl Drop for KafkaCluster`, so
cleanup relies entirely on `ContainerAsync`'s own `Drop`. If `start()` aborts
anywhere between the first container starting and `KafkaCluster` being
constructed — `handle.await.expect("Broker start task panicked")` at `:381`
firing while siblings are already up, or the whole `start()` future being dropped
on a harness timeout — the remaining `JoinHandle`s are dropped, detaching tasks
whose `ContainerAsync` output no one ever receives.

Because host ports are **pre-reserved** before any container starts (`:333`), a
survivor collides deterministically on the next run. The cost is misattribution,
not just waste: the collision surfaces inside `with_mapped_port` and presents as
a *test* failure (`sasl_ssl_consumer_test::test_sasl_ssl_consume_records`,
"address already in use") rather than an infrastructure error — i.e. it looks
exactly like a code regression, which is the most expensive way for a leak to
manifest in a review loop. It already cost a manual diagnosis and cleanup this
round.

Worth a small follow-up: give `KafkaCluster` an explicit teardown that is
registered as each container starts rather than after all of them do, so an
abort mid-`start()` still tears down what is already up — plus, if cheap, a
best-effort label-based sweep of stale `apache/kafka:4.2.0` containers at gate
start. Not a Phase-3 defect; suggest filing it against whichever phase next
touches the integration harness.

---

# Closing note

Four passes: 3 findings, 3 findings, 1 finding, 0. Issues 1-7 were all real and
all conceded; nothing was disputed and no false positive was recorded against me
beyond the two citation slips I introduced and corrected myself (`:1420` for the
`synchronized` block in pass 1, `:163` for the table arm in pass 2). The code
itself has been correct since pass 2 — the last two rounds were entirely about
records, which is where a phase that pulls four methods ahead of their call sites
is most exposed. No findings; Phase 3 closes from my side.

---

# Resolved items (moved by Actor 43)

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
§Phase-3's added-methods bullet, PLAN §9.15). At the time this section was written
the replacement was given as two reasons:

  1. **Scheduling.** `close` was unscheduled in every phase — the same gap that
     left `authenticationFailed` out, which is what Issue 1 filed. Twelve lines,
     no Phase-5 dependency beyond the `pendingTransition` branch. Leaving it
     unscheduled risks it being missed again.
  2. ~~**Behaviour.** Its observable effect idempotently is the `FATAL_ERROR`
     transition, which stops `Sender.runOnce` at `:318` before
     `bumpIdempotentEpochAndResetIdIfNeeded` can enqueue a new `InitProducerId`.~~
     **Also false — retracted under Issue 7 below.** `close()` is the Sender task's
     terminal act; no `runOnce` follows it, and the `!forceClose` loop guards
     already end iteration. Only reason (1) survives, plus a Phase-6 payoff.

The framing this section added — that the payoff is **not urgent before Phase 6**,
since Phase 4 does not translate the shutdown block at all, and that the honest
shape of the decision is "cheap, unscheduled, and reachable" rather than "fixes a
live defect" — is the accurate half and stands. The retracted claims are kept as a
marked **Correction** block in §9.15 rather than deleted, for the same reason
Issue 2's is.

*(This paragraph was itself caught by the "grep for present-tense behavioural
verbs" rule that Issue 7's audit produced — the rule found its first residual
instance in the record that introduced the error, one commit after the rule was
written.)*

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
