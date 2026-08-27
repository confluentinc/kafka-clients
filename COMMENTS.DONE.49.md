# Critic 49 — resolved findings (review of `e147a1e..21343fe`, two passes)

All five findings were real and are fixed — four from pass 1, one (issue 5) filed in
pass 2 against the fixup for issue 4. The production fix in `3740cb3` was confirmed
correct by the Critic and is unchanged; every fix below is in test / example code.

| # | Fix | Fixup of | Verification |
|---|---|---|---|
| 1 | `txn_errors_producer` case 3 now states Java's actual behaviour (a **bare** `KafkaException` from `InitProducerIdHandler`'s fall-through, `TransactionManager.java:1535-1536`) and asserts it: error code `UnknownServerError` **and** the message prefix `Unexpected error in InitProducerIdResponse;`. The `(expect InvalidTransactionTimeout)` claim is gone from both the module doc and the label. | `12bd5d2` | live broker: ✅ with the asserted shape; the three outcomes that used to share one ✅ are now distinguishable |
| 2 | `runs_for` is floored at 1 (`.max(1)`), so a zero-run topic no longer makes `sequence_check`'s expectation empty. Its doc comment's false claim that "check 1 already gates" the empty case is corrected — check 1 counts a different topic. | `12bd5d2` | empirically probed: pointing the flush check at an empty topic now prints `❌ … expected 1000 records, got 0` and exits 1, where it previously printed `✅ 0 records, exactly as expected` |
| 3 | `test_send_after_producer_fenced_fails_the_future` seeds `ABORTABLE_ERROR` (with a `ProducerFenced` `last_error`) instead of `FATAL_ERROR`, asserts `!has_fatal_error()` before the send, and observes the real `ABORTABLE_ERROR -> FATAL_ERROR` transition after. The full re-raised message is asserted too, per DoD §3. | `3740cb3` | verified non-tautological by patching `do_send_bytes` to keep the failed future but skip `maybe_transition_to_error_state` — the test then fails on exactly that assertion |
| 4 | `send_expect_failure` returns a `SendFailure { Synchronous, ViaFuture }` enum. Case 2 requires `expect_synchronous` (Java's `catch (Exception e)` rethrow); the poison case and the lifecycle fencing case require `expect_via_future`. **Correction (issue 5):** the original wording here claimed `expect_via_future` "additionally proves those records reached the broker". It does not — a local `ensure_valid_record_size` rejection is delivered through the future too, in Java as much as here. What proves the end-to-end path is the error *code*, now asserted. | `12bd5d2` | live broker: `txn_errors_producer` six ✅ exit 0, `txn_lifecycle_producer` all ✅ exit 0 |
| 5 | The poison case now asserts `error.error() == Errors::MessageTooLarge`, which only a broker response produces (a local rejection yields `KafkaError::RecordTooLarge`, carrying no wire code, so its `error()` degrades to `UnknownServerError`). Its label says "rejected by the broker" and its comment's false rationale is corrected. `expect_via_future` is kept as a correct statement about the surface Java specifies, no longer as a local-vs-remote discriminator. | `67944a7` | counter-checked by removing the 5 MB `max.request.size` override: the case then prints ❌ on the code assertion and the program exits 1, where it previously printed ✅ |

Note on scope: the Critic's two **rules-change suggestions** (S1 for `CLAUDE.md` §9.6, S2
for `definition-of-done.md` §3) are deliberately NOT applied and remain in
`COMMENTS.49.md`. CLAUDE.md and the rules files are to be avoided by automatic agents;
those are for the human to decide.

---

# Critic 49 — review of `e147a1e..21343fe`

Range: `12bd5d2` (manual `txn_*` suite), `3740cb3` (doSend deadlock fix + 6 regression
tests), `f24ffdf` (README/example wording), `6b1088f` (agent memory), `21343fe` (fixup!
correcting Java line citations).

**The production fix in `3740cb3` is correct and I found nothing wrong with it.** All
four findings below are in test / example code. Verification performed is listed at the
end.

---

## Issue 1: `txn_errors_producer` case 3 states an expectation Java does not meet, and its check cannot notice

- **File**: `examples/txn_errors_producer.rs:29` (module doc), `:200-204` (the check)
- **Severity**: Wrong test / Behavior claim contradicts Java
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java:1535-1536`
  (`InitProducerIdHandler.handleResponse`, the final `else`)
- **Description**:

  The module doc reads "`init_transactions` must fail with `InvalidTransactionTimeout`",
  and the verdict label repeats "(expect InvalidTransactionTimeout)". Java never
  surfaces `InvalidTxnTimeoutException` from `initTransactions`. `INVALID_TRANSACTION_TIMEOUT`
  is not a `RetriableException` and matches none of the earlier arms of
  `InitProducerIdHandler.handleResponse` (`TransactionManager.java:1519-1534`), so it
  reaches

      fatalError(new KafkaException("Unexpected error in InitProducerIdResponse; " + error.message()));

  — a **bare** `KafkaException`, which carries no wire code. This crate reproduces that
  faithfully (`src/producer/internals/transaction_manager.rs:3942`), spelling the bare
  `KafkaException` as `Errors::UnknownServerError` per the convention the fix commit
  itself documents. Live run against the broker on `localhost:9092`:

      ✅ init_transactions with a 20 min transaction timeout (expect InvalidTransactionTimeout) —
         UnknownServerError: Unexpected error in InitProducerIdResponse; The transaction timeout is
         larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).

  The ✅ comes from `expect_error` (`:310-315`), which accepts *any* error, so the same
  output line states an expectation and shows it unmet while passing.

- **Actual**: The case asserts only that `init_transactions` returned some error. The
  expectation printed beside the ✅ describes behaviour that would be a divergence from
  Java if it were ever implemented.
- **Expected**: State what Java produces — a bare `KafkaException`, spelled
  `Errors::UnknownServerError` here, whose message begins
  `Unexpected error in InitProducerIdResponse;` and carries the broker's
  `transaction.max.timeout.ms` text — in both the module doc and the label, and assert
  it (code plus message prefix) rather than mere failure. As written, three different
  outcomes all print the same ✅: today's correct one, a "corrected"
  `InvalidTransactionTimeout` (which would be the real defect), and a plain network
  timeout that never reached the coordinator.

---

## Issue 2: `txn_atomicity_consumer`'s large / flush checks pass vacuously on an empty topic — the exact failure they exist to detect

- **File**: `examples/txn_atomicity_consumer.rs:135-150` (the two checks), `:162-168`
  (`runs_for` and its doc comment)
- **Severity**: Wrong test
- **Description**:

  `sequence_check(label, got, base, runs)` builds `expected = repeated(base, runs)`
  (`examples/txn_common/mod.rs:295-297`, `:308-319`). With `runs == 0`, `repeated`
  yields an empty `Vec`, an empty `got` compares equal to it, and `report(true, ..)`
  prints `✅ <label> — 0 records, exactly as expected`.

  Cases 2 and 3 pass `runs_for(&large, "large-00001")` and
  `runs_for(&flush, "flush-0001")` as `runs`, and `runs_for` returns exactly 0 when the
  topic holds no copy of that value. So an empty `txn-large` or `txn-flush` is reported
  as a pass — and an empty `txn-flush` is precisely the symptom case 3 exists to detect.
  Its producer-side claim is "commit may only succeed after every buffered record is
  acked" (`examples/txn_atomicity_producer.rs:178-182`); a `commit_transaction` that
  returned `Ok` while silently dropping the 1,000 unawaited records leaves that topic
  empty, and the consumer prints ✅. Same shape for case 2's 10,000-record transaction.

  The `runs_for` doc comment claims the opposite: "an unwritten topic fails its sequence
  check loudly instead of passing by accident — except genuinely empty, which check 1
  already gates". Check 1 (`:86-99`) counts `multi-committed-1` on `txn-multi-a` — a
  different topic, written by a different producer in a different case — and its
  `runs == 0` early return therefore says nothing about `txn-large` or `txn-flush`.

  Case 1's own checks are gated correctly (they use the shared `runs`, guarded by the
  early return at `:96-99`), so the hole is specific to the two `runs_for` call sites.

- **Actual**: `sequence_check(.., runs_for(..) == 0)` degenerates to "empty equals
  empty" and prints ✅.
- **Expected**: Gate the two checks the way case 1 is gated — report ❌ (or return
  `Err`) when `runs_for` returns 0 — or floor the expectation at one run. Then correct
  the `runs_for` doc comment, whose stated reasoning is what makes the hole hard to spot
  on re-reading.

---

## Issue 3: `test_send_after_producer_fenced_fails_the_future`'s second assertion cannot fail

- **File**: `src/producer/kafka_producer.rs:3572-3601` (assertion at `:3597-3600`)
- **Severity**: Wrong test
- **Java Reference**: `KafkaProducer.java:1065-1067` (the `maybeTransitionToErrorState`
  tail of `catch (ApiException e)`), `TransactionManager.java:764`
- **Description**:

  The test drives the manager to `FATAL_ERROR` at `:3576-3580` *before* the send, then
  asserts `state_is_fatal` afterwards with the message "the ApiException block runs
  maybeTransitionToErrorState, which keeps a fenced producer fatal". `has_fatal_error()`
  is already `true` at that point and nothing on the send path clears it, so the
  assertion holds whether or not `handle_api_exception` →
  `maybe_transition_to_error_state` ran — it would still pass if the arm were changed to
  route this error to `return Err(error)`. The first assertion (a `ProducerFenced`
  reached *through the future*) does carry weight and does pin the ApiException route;
  only the second is inert while claiming to prove the side effect, and this is the one
  test of the six whose docstring calls it "the direct regression test for the
  deadlock".

- **Actual**: A tautological assertion carrying a causal claim it does not establish.
- **Expected**: Seed a state where the transition is observable. `transition_to_abortable_error(KafkaError::with_message(Errors::ProducerFenced, "fenced"), Caller::App)`
  from `IN_TRANSACTION` reaches the same `maybeFailWithError` arm — `has_error()` is
  true for `AbortableError` (`transaction_manager.rs:1896-1898`, matching Java's
  `hasError()`), and the arm keys on `last_error.error() == Errors::ProducerFenced`
  (`:2502`) — while `maybe_transition_to_error_state` lists `Errors::ProducerFenced`
  among the fatal codes (`:2549-2557`, Java `TransactionManager.java:765-772`). The
  assertion then observes `ABORTABLE_ERROR → FATAL_ERROR`, which the `return Err(error)`
  route would not perform. Asserting the message text as well ("…has been fenced by
  another producer with the same transactionalId") would bring this test in line with
  the other five, which all assert messages — DoD §3.

---

## Issue 4: the manual suite's misuse probe cannot distinguish Java's rethrow from a failed future

- **File**: `examples/txn_common/mod.rs:169-183` (`send_expect_failure`), used at
  `examples/txn_errors_producer.rs:139`
- **Severity**: Wrong test (weaker than its stated claim)
- **Java Reference**: `KafkaProducer.java:1077-1081` (`catch (Exception e)` rethrows)
  versus `:1056-1068` (`catch (ApiException e)` returns `new FutureFailure(e)`)
- **Description**: `send_expect_failure` returns `Ok(error)` for a synchronous `Err`
  from `send()` *and* for an error delivered through the ack future, so case 2's "send
  outside a transaction" verdict is ✅ either way. The comment above the call site
  (`:131-137`) asserts "Java fails it synchronously with IllegalStateException and this
  client does the same" — and that sync-versus-future split is the second half of what
  `3740cb3` established with its `is_api_exception` routing. The unit test
  `test_send_before_init_transactions_returns_illegal_state` does cover it, so this is
  not an uncovered behaviour; the gap is that the file which found the original bug, and
  which `examples/README.md:85-90` now designates the permanent tripwire for this arm,
  would stay green if that half of the fix were undone.
- **Expected**: Have case 2 use a probe that reports *which* of the two paths produced
  the error and require the synchronous one — for example returning an enum, or a
  variant of `send_expect_failure` that fails the case when `send()` returned `Ok`.

---

---

## Issue 5: `expect_via_future` in the poison case does not establish the end-to-end path it is documented to establish

*(new in pass 2 — introduced by `67944a7`, the fixup for issue 4)*

- **File**: `examples/txn_errors_producer.rs:292-308`. The same claim is repeated in
  `67944a7`'s commit message and in `COMMENTS.DONE.49.md`'s issue-4 row ("which
  additionally proves those records reached the broker").
- **Severity**: Wrong test (a guarantee stated that the check cannot falsify — the
  same shape as issue 1)
- **Java Reference**: `KafkaProducer.java:1031` (`ensureValidRecordSize`) →
  `RecordTooLargeException extends ApiException`
  (`kafka/clients/src/main/java/org/apache/kafka/common/errors/RecordTooLargeException.java:26`)
  → `catch (ApiException e)` at `KafkaProducer.java:1056-1068` →
  `return new FutureFailure(e)`.
- **Description**:

  The new comment at `:292-295` reads: "the client cap is raised to 5 MB above
  precisely so this record is *accepted* locally and rejected by the broker. **A
  synchronous failure would mean it never left the client**, so the case would no
  longer be testing the end-to-end path its comment claims." Both halves are false:

  - A *local* rejection is **not** synchronous. `ensure_valid_record_size` failing
    routes through `handle_api_exception` (`src/producer/kafka_producer.rs:1049-1051`),
    which returns `Ok(KafkaFuture::new(FutureRecordMetadata::failed(..)))` (`:1204`) —
    that is `SendFailure::ViaFuture`. Java behaves identically: `RecordTooLargeException`
    is an `ApiException`, so it takes `catch (ApiException e)` and becomes a
    `FutureFailure` rather than a throw.
  - Consequently `expect_via_future` cannot separate "the broker rejected it" from
    "`max.request.size` rejected it locally". Drop the 5 MB override, or change
    `estimate_size_in_bytes_upper_bound`, and the record never leaves the client while
    the case still prints `✅ the 1.5 MB record was rejected`. The rest of case 5 keeps
    passing too: a local `RecordTooLarge` still reaches
    `maybe_transition_to_error_state` → `transition_to_abortable_error`, so
    "commit refuses" and "abort works" both still hold.

  Nothing in the case asserts the error code — `:304` only *prints* it — so the
  end-to-end claim rests entirely on a discriminator that does not discriminate.

- **Actual**: The case requires only that the failure arrived through the ack future,
  which is true of a purely client-side rejection as well.
- **Expected**: Assert the code. `Errors::MessageTooLarge` can only come from a broker
  response; a local rejection produces `KafkaError::RecordTooLarge(String)`, whose
  `error()` is `Errors::UnknownServerError` (`src/common/kafka_error.rs:447-461`,
  `:470-475`), and a broker that never answered produces a `Timeout`. Requiring
  `error.error() == Errors::MessageTooLarge` pins the end-to-end path for real. Keep
  `expect_via_future` — it is a correct, if weaker, statement about which surface
  delivered the error — but correct the rationale in the code comment, and the
  corresponding sentence in `COMMENTS.DONE.49.md`.

  This does **not** apply to the other two call sites. Case 2's `expect_synchronous` is
  exactly right (Java's `IllegalStateException` is not a `KafkaException` and is
  rethrown out of `send()`, `KafkaProducer.java:1077-1081`), and
  `txn_lifecycle_producer`'s `expect_via_future` is sound — A holds no local error and
  no size cap is in play, and that case does assert the code is a fencing one
  (`examples/txn_lifecycle_producer.rs:130`).
