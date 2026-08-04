# Critic 44 - Milestone 11 Phase 4 - RESOLVED

All five findings of Critic 44 pass 1 were conceded and fixed. Resolutions, with the
commit that carries each:

| Issue | Severity | Resolution | Commit |
|---|---|---|---|
| 1 - `do_send_bytes` allocates the topic name twice per record | Bug | `RecordAppendResult` carries the interned `TopicPartition`; new send-path allocation test, mutation-checked at 2 vs 4 allocations | `c1c6e60` |
| 2 - `maybe_abort_batches` leaks the aborted in-flight batches' buffers | Bug | `Sender::batches_awaiting_response` is the explicit second holder Java gets from its completion callback; the sibling in `fail_expired_batches` is fixed too; `testCancelInFlightRequestAfterFatalError` translated as the pin | `a2ca2f9` |
| 3 - a failing response handler abandons the rest of the poll | Behavior mismatch | per-response catch-and-log, matching `NetworkClient.completeResponses`; rustdoc corrected | `13e6201` |
| 4 - `SenderTest`'s idempotence subset: 33 not translated, landing site does not exist | Missing requirement | 25 translated across six commits; the residue is accounted for per method and owned by new PLAN 9.19 | `7dea6c7`, `f8bf748`, `1f8f820`, `ef4a5ac`, `caeaaf1`, `7df405c` |
| 5 - module doc denies its own contents | Missing requirement | rewritten to state what is actually deferred | `13e6201` |

Non-blocking notes: 1 (the split arm's missing `maybeRemoveAndDeallocateBatch`) and 2
(force close not aborting the Sender's batches) were **fixed** rather than filed, both
in `a2ca2f9`, because Issue 2's mechanism is what they were missing. Note 3 (PLAN
Phase-5 annotation) and note 4 (`test_healthy_partition_retries_during_epoch_bump`'s
tail and doc) are fixed in `13e6201`.

The three rule/plan suggestions are recorded verbatim below and deliberately **not**
acted on: `agent-roles.md` 2 routes changes to `CLAUDE.md` and the rules files through
the process, not through the Actor.

Nothing was disputed.

---

# Critic 44 — Milestone 11 Phase 4 (idempotent send-path integration)

Reviewed `84fa389..144c8a8` (`883ab19` … `144c8a8`) against `Sender.java`,
`RecordAccumulator.java`, `KafkaProducer.java`, `ProducerBatch.java`,
`NetworkClient.java` (Apache Kafka 4.2), `CLAUDE.md`,
`.claude/rules/producer-transactions.md`, `.claude/rules/definition-of-done.md`
and PLAN §Phase-4 / §9.16.

**Five findings.** Verified clean and not re-reported:

- **Rules §3 (lock order).** No site anywhere takes the `TransactionManager` lock
  and then a deque lock. `TransactionManager` holds no accumulator reference, so
  the manager → deque edge does not exist; the deque → manager edge appears at
  `should_stop_drain_batches_for_partition`, `maybe_assign_producer_state`,
  `insert_in_sequence_order` and inside `with_in_flight_batch_pool`'s closure. The
  app task takes either a deque lock (`append`) or the manager lock
  (`maybe_add_partition`, `maybe_transition_to_error_state`) but never one while
  holding the other. `ready()`'s `is_completing()` guard is a statement temporary
  and is dropped before `partition_ready` locks any deque. No re-entrant
  acquisition (checked every `.lock()` in the production half of `sender.rs`,
  `record_accumulator.rs`).
- **Rules §4 (guard across `.await`, poll in `select!`).** No manager guard is
  live across any `.await`: the three `let manager = …lock()` bindings
  (`sender.rs:777`, `:883`, `:936`) are inside await-free blocks, and every other
  acquisition is a statement temporary. `run_transaction_phase` does read
  `last_error` / `has_fatal_error` / `has_abortable_error` in one acquisition
  (`:776-783`), and the guard is released before `maybe_abort_batches`, the poll,
  `await_node_ready` and both `sleep_ms` calls. No `tokio::select!` exists in
  `src/producer/` at all.
- **Rules §1 (`Caller`).** All eight production call sites match the Java call
  chain: `Caller::App` only at `kafka_producer.rs:769` (`doSend`'s
  `catch (ApiException)`), `Caller::Sender` at `sender.rs:527`, `:629`, `:844`,
  `:849`, `:896`, `:908`, `:1818`. `maybe_add_partition`,
  `handle_completed_batch`, `can_retry` and `maybe_resolve_sequences` correctly
  take no `Caller` — none performs a transition on the idempotent path.
- **Rules §7 (pool from both owners).** `with_in_flight_batch_pool` merges the
  accumulator's deques and `Sender::in_flight_batches`, and
  `test_out_of_order_sequence_is_retried_and_bumps_the_epoch`
  (`sender.rs:3712`) has real teeth for the accumulator half: the reenqueued
  batch is tracked but sits in the deque, so dropping that half makes
  `start_sequences_at_beginning` error and `run_once` fail. Every `&mut []`
  argument is justified (guard implies an empty tracked set, or the arm is
  transaction-only).
- **Send-path fidelity.** `run_once`'s four exits, the `AuthenticationException`
  fall-through (which correctly does *not* return),
  `maybe_send_and_poll_transactional_request`'s return census,
  `maybe_find_coordinator_and_retry`, the drain's sequence assignment between
  `pop_front()` and `close()` inside the deque lock,
  `should_stop_drain_batches_for_partition`, `insert_in_sequence_order`'s
  untracked-batch rejection, `can_retry`, `handle_failed_batch`'s rules §9
  arm order, the reenqueue path not untracking while the split path does, and
  `assign_producer_state_to_batches` are all faithful. `run_once` agrees with
  Phase 3's `run_sender_transaction_phase` harness.
- **`0b8c3d0`.** The `Arc::ptr_eq` routing fix is sound: `produce_future` is
  unique per batch, the drain yields at most one batch per partition per
  `send_producer_data`, so `batches.last_mut()` in `send_produce_request` is the
  batch whose bytes went into that request.
  `test_correct_handling_of_duplicate_sequence_error` is a faithful translation of
  Java 1766-1817 and would fail under the old `remove(0)`.

---

## Issue 1: `do_send_bytes` allocates the topic name twice per record when idempotence is on

- **File**: `src/producer/kafka_producer.rs:726`
- **Severity**: Bug (CLAUDE.md §11 / `definition-of-done.md` §10)
- **Java Reference**: `KafkaProducer.java:1040-1046`, `KafkaProducer.java:1606`
  (`AppendCallbacks.setPartition`)
- **Description**: The `maybe_add_partition` wiring added by `0ad7a46` builds the
  topic-partition on the **success** path of every send:

      let tp = TopicPartition::new(topic.to_string(), result.partition);

  `TopicPartition::new` takes `impl Into<Arc<str>>`
  (`src/common/topic_partition.rs:29`), so `topic.to_string()` allocates a
  `String` and copies the name, then `Arc<str>: From<String>` allocates again and
  copies again — two heap allocations and two memcpys of the topic name per
  record. `do_send_bytes` is per-record dispatch, i.e. a hot path under
  CLAUDE.md §11's own definition ("send-path record build"), and
  `enable.idempotence` defaults to `true`, so this is now the default path for
  every produced record. CLAUDE.md §11 names this cost explicitly:
  "Identifiers cloned on every message (topic names, client IDs): prefer
  `Arc<str>` over `String` to make clones cheap." DoD §10 asks for exactly this
  audit ("no identifier `String` clones") on send-path classes.

  It is avoidable with no design change: the accumulator already owns an interned
  `Arc<str>` per topic (`get_or_create_topic_info` returns it, and `append_inner`
  / `append_new_batch` take `topic: &Arc<str>` —
  `record_accumulator.rs:385`, `:534`, `:548`), so `RecordAppendResult` can carry
  the resolved `TopicPartition` (or the `Arc<str>`) instead of a bare `i32`,
  reducing the per-record cost to one atomic increment. Note that on the
  idempotent path `maybe_add_partition` does not even read the argument — its body
  is `maybe_fail_with_error()` plus an `is_transactional()`-gated block
  (`transaction_manager.rs:2059-2066`) — so today the allocation is pure waste.

  The phase's own DoD §10 audit
  (`test_drain_allocations_do_not_scale_with_the_record_count`,
  `record_accumulator.rs:4022`) is sound as far as it goes — the tracker is a real
  `#[global_allocator]` wrapper, the delta design is right, and the `count > 0`
  liveness assertion is genuine — but it measures `drain` only. `KafkaProducer`'s
  send path, which is where the regression landed, has no allocation budget test.
- **Expected**: No per-record heap allocation of the topic name. Carry the
  resolved partition back as a `TopicPartition` built from the accumulator's
  `Arc<str>`, or otherwise clone an `Arc<str>` rather than the string data. Extend
  the DoD §10 audit to cover `do_send_bytes`, since that is the class the clause
  is aimed at.
- **Actual**: `String` + `Arc<str>` allocated and the topic name copied twice on
  every successful `send()` whenever a `TransactionManager` exists (the default).

---

## Issue 2: `maybe_abort_batches` leaks the aborted in-flight batches' pooled buffers

- **File**: `src/producer/internals/sender.rs:1147-1159` (comment at `:1152-1153`)
- **Severity**: Bug
- **Java Reference**: `Sender.java:532-538` (`maybeAbortBatches`),
  `RecordAccumulator.java:1152-1168` (`abortBatches`),
  `Sender.java:172-180` (`maybeRemoveAndDeallocateBatch` /
  `…BatchLater`), `SenderTest.java:2182-2219`
  (`testCancelInFlightRequestAfterFatalError`)
- **Description**: The new `maybe_abort_batches` drains `in_flight_batches` and,
  for a batch still marked in flight, calls `accumulator.complete_batch(batch)`
  (remove from `incomplete`, do **not** deallocate) with this justification:

      // KAFKA-19012: the pooled buffer may still be in use by the
      // network client, so it is deallocated when the response arrives.

  That mechanism does not exist in this port. `drain()` has moved the batches out
  of the map, so after the loop they are dropped; nothing else holds them.
  `pending_produce_responses` stores only `(TopicPartition, Arc<ProduceRequestResult>)`
  identities (`sender.rs:179`), not the batches, and `handle_produce_responses`
  locates the batch by searching `in_flight_batches` (`:697-711`) — which is now
  empty for those partitions. So when the response finally arrives the batch
  cannot be found, `handle_produce_response` takes the `else` arm and logs
  "Can't find batch created for topic id …" (`:1497-1504`), and
  `RecordAccumulator::deallocate` is never reached. The `BufferPool` accounting is
  therefore never restored: `deallocate_with_size` is what adds the size back
  (`buffer_pool.rs:315-322`), and `available_memory` shrinks permanently by
  `initial_capacity` per abandoned batch.

  Java restores it in both of the two paths this method can be reached from,
  because its request-completion callback closes over the batches
  (`recordsByPartition`), so `inFlightBatches.clear()` at `Sender.java:536` only
  drops the map's references. `SenderTest.testCancelInFlightRequestAfterFatalError`
  pins exactly this with a `MatchingBufferPool`:

      sender.runOnce();
      assertFutureFailure(future2, ClusterAuthorizationException.class);
      assertFalse(pool.allMatch(), "Batch should not be deallocated before the response is received");
      // Should be fine if the second response eventually returns
      client.respond(…, produceResponse(tp1, 0, Errors.NONE, 0));
      sender.runOnce();
      assertTrue(pool.allMatch(), "The batch should have been de-allocated");

  That test is one of the 33 named in Issue 5, which is why the branch shipped
  untested: no Rust test drains a batch and *then* triggers a fatal or
  authorization error — `test_run_once_returns_on_a_fatal_transaction_manager_error`
  (`sender.rs:3527`) only has an undrained batch, so the whole
  `is_inflight()` fork of `maybe_abort_batches` is dead in the suite.

  This is not merely a shutdown-time concern. `handle_authorization_error`
  (`sender.rs:829-850`) calls `maybe_abort_batches` and then **recovers** to
  `UNINITIALIZED`, so the producer keeps running; each recurrence of a
  `CLUSTER_AUTHORIZATION_FAILED` / `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` with
  requests in flight shrinks the pool again, and the only symptom is eventual
  blocking in `BufferPool::allocate`.

  Pre-existing sibling worth fixing at the same time (not a Phase-4 defect on its
  own, but the same missing mechanism): `fail_expired_batches` is called with
  `deallocate_buffer = false` for expired in-flight batches
  (`sender.rs:1313`), reaching
  `fail_batch_with_record_exceptions`'s `self.accumulator.complete_batch(batch)`
  branch (`:1830`) — Java's `maybeRemoveAndDeallocateBatchLater`. Those batches are
  dropped at the end of `send_producer_data` and are likewise never deallocated.
- **Expected**: Either make the "deallocate when the response arrives" contract
  real — retain the batch alongside its identity in `pending_produce_responses`
  so the response path can reach it, as Java's callback does — or restore the
  accounting where the batch is dropped. If neither is in scope for a
  transactions phase, record it as a tracked follow-up the way §9.18 is, and
  correct the comment, which currently asserts a mechanism the code does not
  have.
- **Actual**: The batches are dropped un-deallocated and the comment claims a
  later deallocation that cannot happen.

---

## Issue 3: a failing response handler abandons the rest of the poll's responses

- **File**: `src/producer/internals/sender.rs:656-674` (the `?` at `:668` and
  `:670`); doc claim at `:683-687`
- **Severity**: Behavior Mismatch
- **Java Reference**: `NetworkClient.java:666-674` (`completeResponses`)
- **Description**: `handle_client_responses` iterates the responses returned by
  one `poll()` and propagates any failure with `?`, so responses after the failing
  one are never dispatched at all. Their `pending_produce_responses` entries stay
  in the map and their batches stay in `in_flight_batches`, so the records they
  carry are not completed now — they wait for `delivery.timeout.ms` and are then
  failed as expired.

  Java does the opposite, deliberately:

      private void completeResponses(List<ClientResponse> responses) {
          for (ClientResponse response : responses) {
              try {
                  response.onComplete();
              } catch (Exception e) {
                  log.error("Uncaught error in request completion:", e);
              }
          }
      }

  Each completion callback's failure is caught and logged per response, and the
  loop continues. This makes the rustdoc on `handle_produce_responses` wrong at
  its middle step:

      /// Java's `IllegalStateException` from `insertInSequenceOrder` escapes the
      /// completion callback, hence `client.poll` and `runOnce`, to `Sender.run`'s
      /// catch-and-log; [`Self::run_once_logging_errors`] is the same boundary.

  It escapes the callback but is caught before it escapes `client.poll`, so
  `Sender.run`'s catch is *not* the Java boundary and
  `run_once_logging_errors` is not equivalent to it.

  Phase 4 is what introduced this: before `0ad7a46`/`a983e32`,
  `handle_produce_responses` returned `()` and `reenqueue` / `split_and_reenqueue`
  were infallible. The new fallible sites are `accumulator.reenqueue(..)?`
  (`:720`), `accumulator.split_and_reenqueue(..)?` (`:728`) and
  `on_transactional_response(..)?` (`:668`). Reachability is narrow but real —
  e.g. a poll returning a produce response that moves the manager to
  `FATAL_ERROR` (via `handle_failed_batch` → `maybe_transition_to_error_state`)
  followed, in the same batch, by a successful `InitProducerId` response whose
  `transition_to(State::Ready, .., Caller::Sender)`
  (`transaction_manager.rs:1997`) is then an invalid transition; Java logs it and
  still completes everything else in that poll.
- **Expected**: Isolate per-response failures the way `completeResponses` does —
  log the error for the response that raised it and continue dispatching the
  remaining responses — and correct the rustdoc to name
  `NetworkClient.completeResponses` as the Java boundary.
- **Actual**: The first failing response aborts dispatch for the whole poll batch
  and the error is reported one level too high, with a doc comment asserting that
  level is where Java reports it.

---

## Issue 4: `SenderTest`'s idempotence subset — 33 of 37 not translated, and the stated landing site does not exist

- **File**: `src/producer/internals/sender.rs:4391-4453`;
  `design/history/Milestone-11/PLAN.md:365-368`, `:480-490`
- **Severity**: Missing Requirement (`definition-of-done.md` §3)
- **Java Reference**: `SenderTest.java` — the 33 methods listed at
  `sender.rs:4421-4453`
- **Description**: PLAN §Phase-4 assigns the idempotence-only subset of
  `SenderTest.java` to this phase in as many words:

      **Tests:** the idempotence subset of `SenderTest.java` (43 of its 76 tests are
      idempotence/txn-related; the idempotence-only ones land here, the transactional
      ones in Phase 6)

  Four landed (`testInitProducerIdRequest`,
  `testCorrectHandlingOfDuplicateSequenceError`,
  `testUnknownProducerErrorShouldBeRetriedWhenLogStartOffsetIsUnknown`, and
  `testTooLargeBatchesAreSafelyRemoved` — the last `#[ignore]`d). The remaining 33
  are itemised by name and Java line, which is the right way to declare a
  shortfall, and the 17 transactional ones are correctly deferred to Phases 5/6.
  But the declared destination is not real:

      … they are the residual DoD §3 debt of this phase and are carried to
      Phase 8's parity sweep

  PLAN §Phase-8 scopes its sweep to one file: "Close out any
  `TransactionManagerTest` method not landed in Phases 3/5, so the full 140 are
  accounted for" (`PLAN.md:482-484`). It says nothing about `SenderTest`, and the
  PLAN diff in `7055b7d` did not amend it — the only PLAN edits were §9.4, the new
  §9.18 and the new §10.6. So 33 tests the plan assigns to Phase 4 are neither in
  Phase 4 nor scheduled anywhere; the in-code comment is the only record, and it
  points at a sweep that does not cover them.

  This is not a bookkeeping quibble. Issue 2 above is a live defect in code this
  phase wrote, and `testCancelInFlightRequestAfterFatalError` — item 23 on the
  deferred list — is the Java test that asserts precisely the behaviour it breaks.
  Several others cover branches the phase introduced and left with no test at all:
  `testClusterAuthorizationExceptionInProduceRequest` (2159) and
  `testCancelInFlightRequestAfterFatalError` (2182) for the in-flight arm of
  `maybe_abort_batches`; `testSenderShouldRetryWithBackoffOnRetriableError` (3104)
  and `testNodeNotReady`-adjacent paths for `maybe_find_coordinator_and_retry` and
  `sleep_ms`; `testIdempotenceWithMultipleInflights*` (762, 811, 912) for the
  multi-in-flight ordering the `0b8c3d0` fix exists to serve, of which only the
  two-batch case is covered.

  The `RecordAccumulatorTest` half of the same PLAN line *is* handled correctly —
  its single `TransactionManager` test (`testRecordsDrainedWhenTransactionCompleting`,
  Java 976-1019) is named, justified as needing `isCompleting() == true`, and
  assigned to Phase 6 (`record_accumulator.rs:3669-3687`). That is the standard
  the `SenderTest` deferral should meet.
- **Expected**: Either translate the idempotence-only subset in this phase as the
  plan requires, or amend PLAN §Phase-8 (or add a numbered §9 follow-up) so the 33
  named methods have a real owner, and prioritise the ones that cover
  Phase-4-introduced branches with no current coverage. An itemised deferral is
  acceptable under DoD §3 only when the record says which phase closes it *and*
  that phase's scope says so too.
- **Actual**: 33 named tests are deferred to a sweep whose stated scope excludes
  them, and one of them is the test that would have caught Issue 2.

---

## Issue 5: `sender.rs`'s module doc still says the transactional methods are not translated

- **File**: `src/producer/internals/sender.rs:23`
- **Severity**: Missing Requirement (CLAUDE.md §4 — documentation must not
  contradict the code)
- **Java Reference**: `Sender.java:311-345`, `:456-530`, `:532-538`, `:563-574`
- **Description**: The module doc still reads

      //! Transactional methods are not translated in this phase.

  Phase 4 translated `runOnce`'s whole `transactionManager != null` block,
  `maybeSendAndPollTransactionalRequest`, `maybeFindCoordinatorAndRetry`,
  `maybeAbortBatches`, `awaitNodeReady`, `hasPendingTransactionalRequests`,
  `shouldHandleAuthorizationError` and the unsynchronized half of
  `TxnRequestHandler.onComplete`, and added the `pending_requests` /
  `in_flight_request_correlation_id` / `pending_transactional_response` fields.
  The line is now false, and it is the first thing a reader of the phase's primary
  file sees. Phase 3's review loop spent three passes on doc claims of exactly this
  shape; this one costs a single line.
- **Expected**: State what is actually deferred — the transactional state machine
  and the coordinator subsystem (Phases 5/6) — rather than "transactional methods".
- **Actual**: A blanket denial that the file contains any transactional
  translation.

---

## Notes that do **not** block

1. **`PLAN.md` §9.18 is accurate.** I independently confirmed the diagnosis: the
   split arm requires `recordCount > 1 && !batch.isDone() && magic >= v2`
   (`Sender.java:673-675`), `ProducerBatch::records()` is
   `MemoryRecordsBuilder::take_built_records()` and moves the buffer
   (`producer_batch.rs:653-657`, `memory_records_builder.rs:305`), and
   `test_expired_batch_does_not_split_on_message_too_large_error` avoids the path
   by expiring first. It genuinely predates Phase 4 — the same
   `records()`-before-response ordering is in `84fa389` — and the `#[ignore]`
   message and §9.18 tell the same story. One addition for whoever fixes it:
   Java's split arm also calls `maybeRemoveAndDeallocateBatch(batch)` after
   `splitAndReenqueue` (`Sender.java:686-688`), which the Rust
   `BatchAction::SplitAndReenqueue` handler (`sender.rs:722-729`) omits — so the
   big batch is left in `incomplete` and its buffer un-deallocated. Also
   pre-existing, also unreachable while the path panics, but the fix needs it.
2. **Force-close does not abort the Sender's in-flight batches.** `Sender::run`'s
   `force_close` branch calls `self.accumulator.abort_incomplete_batches()`
   (`sender.rs:537`), which walks the deques only; Java's `abortBatches` iterates
   `incomplete.copyAll()`, which returns the batch objects and so covers drained
   ones too (`RecordAccumulator.java:1153`). The record futures of in-flight
   batches are therefore never completed on a force close — the same asymmetry
   §10.6 deviation 8 correctly identified and fixed for `maybe_abort_batches`.
   This one predates Phase 4 (`84fa389`'s `run()` has the same call), so it is not
   a Phase-4 defect, but it is the sibling of a gap this phase reasoned about, and
   it violates CLAUDE.md §5. Suggest a numbered §9 follow-up alongside Issue 2,
   since both need the same "who owns a drained batch" decision.
3. **PLAN §Phase-5 still lists `is_send_to_partition_allowed` (466)**
   without the annotation Phase 3 applied to the items it pulled forward
   (`~~transition_to_uninitialized (756)~~ **landed in Phase 3**`). Phase 4 landed
   its fatal-error and non-transactional arms; only the transactional arm remains.
   The Phase-3 style would be "`is_send_to_partition_allowed` (466, transactional
   arm — the other arms landed in Phase 4)", matching how `maybe_add_partition`
   is already annotated on the line above. Cosmetic, but it is the
   entry-scheduled-before-its-caller smell §9.16 asks Phase 4 to watch for.
4. **`test_healthy_partition_retries_during_epoch_bump` stops short of Java's
   tail** (the final `tp1b3.complete` / `handleCompletedBatch` /
   `maybeUpdateProducerIdAndEpoch(tp1)` and its two assertions,
   `TransactionManagerTest.java:3684-3692`), and the shared-body doc's "identical
   up to the final two assertions" is imprecise — the two Java methods differ by
   one `maybeUpdateProducerIdAndEpoch(tp1)` call and their final two assertions are
   identical. Not filed as a finding because the omitted assertions are covered
   verbatim by `test_failed_inflight_batch_after_epoch_bump` and the extra call is
   inert once tp1's entry already carries the bumped epoch. Worth a one-line
   correction if the file is touched.

---

## Suggested rule / plan changes

1. **`definition-of-done.md` §10** currently says "For any class that sits on the
   producer send path, verify there are no avoidable per-message heap
   allocations". Issue 1 shows the clause can be satisfied *as written* by
   auditing one class while the regression lands in another: the phase measured
   `RecordAccumulator::drain` and missed `KafkaProducer::do_send_bytes`. Suggest
   naming the entry point rather than leaving it to the auditor's choice — e.g.
   "the audit must include the public `send` entry point
   (`KafkaProducer::do_send` / `do_send_bytes`), not only the class the change
   touched, and a delta-measured allocation test must exist for it."
2. **A new `producer-transactions.md` rule (or an addition to §7) on batch
   ownership at abort time.** Issues 2 and note 2 are the same root cause: rules
   §7 establishes that a `ProducerBatch` has exactly one owner, but three Java
   paths (`maybeAbortBatches`, `abortBatches`, `failBatch(deallocateBatch=false)`)
   depend on the *request callback* being a second holder that outlives the map
   entry. Rules §7 should state that consequence explicitly — that any Rust
   translation of a "complete now, deallocate when the response arrives" path must
   name which structure keeps the batch reachable until then, and that
   `pending_produce_responses` holding only an `Arc<ProduceRequestResult>` identity
   is not sufficient.
3. **`agent-roles.md` / `definition-of-done.md` §3 on itemised deferrals.** Issue
   4's deferral was honest about *what* was skipped but named a destination that
   does not exist. Suggest requiring that a deferral record cite the phase that
   will close it **and** that the cited phase's own scope section list the file —
   the `RecordAccumulatorTest` deferral in the same commit meets that bar and is a
   ready-made example. Phase 3's §9.16 already carries the adjacent lesson ("a plan
   scheduling an exit method into a later phase than its entry is itself the
   smell"); this is the test-side form of it.
