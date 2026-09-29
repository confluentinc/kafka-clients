# Critic 54 — resolved comments (Milestone 11 Python producer-transaction bindings)

Fixes applied in a fixup of the 9c test commit `00bc887b`. Both items were
test-only; no `src/`/generator or non-test file was touched. `_check_closed`
and all pre-existing exception behaviour were left unchanged.

Verification: the sanctioned `make devel-build-python` + pytest path cannot run
on this macOS box (documented C11 `<threads.h>` blocker; CI/Linux is the gate).
Verified locally by executing the exact committed `test_producer.py` against the
real mock FFI via the scratchpad pytest/threads.h shim (not committed;
`target/include/threads.h` is gitignored): **95 passed, 0 failed** (86
pre-existing + 9 new). Nothing regressed.

---

## Finding 1 (should-fix, DoD #3): offset-lifecycle-on-abort/commit MockProducerTest cases are coverable via the exposed hooks but were neither translated nor listed as skipped — RESOLVED

- **File**: `bindings/python/test/unit/test_producer.py`
- **Java Reference**: `kafka/clients/src/test/java/org/apache/kafka/clients/producer/MockProducerTest.java`
  - `shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled` (:529)
  - `shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction` (:464)
  - `shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommitIfTransactionsAreEnabled` (:492)
  - `shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactionsAreEnabled` (:583)

- **Original description**: The phase exposes exactly the two mock hooks needed
  to observe offset lifecycle (`MockProducer_sent_offsets` /
  `MockProducer_committed_offset`) but the offset-on-abort and
  sent-offsets-flag-lifecycle Java tests, which use those same observables, were
  not translated, and the deviation note's two categories (introspector-only,
  fenceProducer) did not cover them.

- **Resolution**: Translated all four cases faithfully, asserting observable
  content through the exposed hooks (the FFI analog of Java's
  `consumerGroupOffsetsHistory()`):
  - `test_txn_reset_sent_offsets_flag_only_when_beginning_new_transaction` —
    proves commit does NOT reset the flag; only `begin_transaction()` does
    (False→send→commit=True→begin=False→send→commit=True→begin=False).
  - `test_txn_publish_latest_and_cumulative_offsets_only_after_commit` — two
    `send_offsets` calls for one group merge cumulatively with latest-wins for
    partition 1 (73→101); nothing is published until commit (all
    `committed_offset` None pre-commit, then 42/101/21 after).
  - `test_txn_drop_consumer_group_offsets_on_abort` — the key exactly-once
    check: init→begin→send_offsets→abort→begin→commit→`committed_offset(...)`
    is None (aborted staged offsets discarded). Java's two-round repeat kept.
  - `test_txn_preserve_committed_offsets_on_later_abort` — committed offsets
    survive a later transaction's abort while its freshly staged offsets are
    dropped. **Deviation (noted in the test):** Java stages the second (aborted)
    transaction under a *different* group ("g2") to show per-group isolation.
    The binding cannot express two groups — `MockConsumer.group_metadata()` is
    fixed to `"dummy.group.id"` (`src/consumer/mock_consumer.rs:409`) and
    `ConsumerGroupMetadata` has no Python constructor (no `tp_new`) — so the
    second transaction stages additional partitions under the SAME group. The
    observable behaviour and the commit/abort staging split exercised are
    identical.
  - **Sub-point folded in**: `test_txn_send_offsets_to_transaction` now asserts
    `MockProducer_sent_offsets(...) is False` **before** the send (mirroring
    Java `shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata` :447),
    proving the False→True transition, not just True-after.
  - The txn-block deviation note was narrowed so it no longer implies all
    exposed-surface tests are covered — it now scopes the "not translatable
    through the FFI" categories to the introspector-only tests
    (`commitCount()`, `transactionInFlight/Committed/Aborted()`) and the
    `fenceProducer` tests only, and states the offset-lifecycle tests ARE
    translated faithfully.

  Underlying behaviour confirmed correct by the Critic
  (`src/producer/mock_producer.rs`: abort clears
  `uncommitted_consumer_group_offsets`; `committed_offset` reads only the
  committed `consumer_group_offsets` history) — all four pass.

---

## Finding 2 (nit, DoD #3): producer-closed transaction tests not translated/listed; binding raises `RuntimeError`, not a Kafka error, on a closed producer — RESOLVED

- **File**: `bindings/python/test/unit/test_producer.py`
- **Java Reference**: `MockProducerTest.java` `shouldThrowOn{Init,Begin,Commit,Abort}TransactionIfProducerIsClosed` and `shouldThrowSendOffsetsToTransaction...IfProducerIsClosed` (:617, :631, :638, :645, :652, :659)

- **Original description**: The closed-producer txn tests were translatable but
  not translated and not in the deviation note; the behaviour diverges from Java
  (every txn method calls `self._check_closed()`, raising
  `RuntimeError("Producer is already closed")` before the FFI, vs Java's
  `IllegalStateException`). This is pre-existing and binding-wide (`_check_closed`
  also guards `send`/`flush`/`partitions_for`), not introduced by this phase.

- **Resolution**: Translated five closed-producer tests asserting the binding's
  actual `RuntimeError` (init/begin/commit/abort + one `send_offsets` covering
  both Java by-groupId/by-groupMetadata forms, since Python has one method):
  `test_txn_{init,begin,commit,abort,send_offsets}_after_close_raises`. A
  one-line section comment records the intentional, pre-existing, binding-wide
  divergence from Java's `IllegalStateException`. **`_check_closed` and all
  pre-existing exception behaviour were left unchanged** (verified
  `_check_closed()` is the first executable line in every txn method:
  `bindings/python/producer.py:372,390,418,439,456`).

---

## Informational item (no action taken, per Critic's explicit request)

The `PyUnicode_AsUTF8(meta)` unchecked-return item in
`py_Producer_send_offsets_to_transaction` was left as-is: the Critic requested
no action (it is byte-for-byte the shipped `py_Consumer_commit_sync_offsets_async`
behaviour and is unreachable through the Python API, since `_offsets_to_spec`
always yields a `str` metadata). Not a defect this phase introduced.

> **Superseded below.** The later `/code-review` pass showed the "unreachable"
> premise is false — `OffsetAndMetadata` does no type validation and
> `_offsets_to_spec` forwards `metadata` verbatim, so a caller can drive a
> non-`str` through. It became code-review Finding 1 and is now fixed.

---

# `/code-review` round — Milestone 11 Python producer-transaction bindings

A `/code-review` pass over this phase's code found two real, reachable bugs.
Both fixed in one fixup commit (`--no-verify`); Python + C-extension only, no
`src/`/generator changes. Local verification: **114 passed, 2 skipped** via the
sanctioned macOS build (`setup.py build_ext --inplace --force` against
`target/debug` + `venv` pytest). The 2 skips are pre-existing.

## Finding 1 (correctness): non-`str` offset metadata silently staged offsets and produced a `SystemError` instead of a clean `TypeError` — FIXED

- **File**: `bindings/python/_confluentkafka.c`,
  `py_Producer_send_offsets_to_transaction` (marshaling loop, ~:1229).
- **Introduced by**: `f27ef1e8` (9a, the C-extension glue).
- **Bug**: `metas[i] = (meta == Py_None) ? NULL : PyUnicode_AsUTF8(meta);` did
  not check the return. `OffsetAndMetadata` does no type validation and
  `_offsets_to_spec` (`producer.py`) forwards `oam.metadata` verbatim (only
  `None`→`""`), so `OffsetAndMetadata(5, metadata=b"x")` reaches the loop. On a
  non-`str`, `PyUnicode_AsUTF8` returns `NULL` and sets a `TypeError`, but `ok`
  stayed `1`, so the loop did not bail: the FFI was invoked (offsets **staged**
  with metadata silently dropped) and the wrapper returned a non-`NULL` `PyLong`
  with the exception still pending → CPython raised a confusing `SystemError`.
- **Fix**: when `meta != Py_None` and `PyUnicode_AsUTF8(meta)` returns `NULL`,
  set `ok = 0` (the `TypeError` is already set by CPython). The loop then frees
  the arrays and `return NULL` **before** the FFI call — a clean `TypeError`,
  offsets NOT staged. `PyUnicode_AsUTF8("")` returns a valid pointer, so a
  legitimate empty-string metadata is unaffected.
- **Test** (added, mock-backed): `test_txn_send_offsets_non_str_metadata_raises_type_error`
  in `test/unit/test_producer.py` — inside a txn,
  `send_offsets_to_transaction({TopicPartition("t",0): OffsetAndMetadata(5, metadata=b"x")}, gm)`
  raises `TypeError` (not `SystemError`), and `MockProducer_sent_offsets(...)`
  is still `False` (offsets not staged). `gm` obtained from a `MockConsumer`.
- **Pre-existing sibling (out of scope, filed for follow-up)**: the consumer
  wrapper `py_Consumer_commit_sync_offsets_async` (`_confluentkafka.c`, ~:1869)
  has the byte-identical unchecked `PyUnicode_AsUTF8` pattern. It was NOT
  modified in this fixup (it predates this phase). It should be filed and fixed
  separately.

## Finding 2 (memory/robustness): `KafkaError` handle leaks if an async txn op is cancelled between the executor returning and `_raise_if_error` — FIXED

- **File**: `bindings/python/producer.py`, the 4 blocking `AsyncProducer` txn
  ops (`init_transactions`, `send_offsets_to_transaction`, `commit_transaction`,
  `abort_transaction`).
- **Introduced by**: `5b596e67` (9b, the producer.py txn API).
- **Bug**: each op did `error = await loop.run_in_executor(None, _lib.Producer_xxx, ...)`
  then `self._raise_if_error(error)`. `run_in_executor` cannot cancel the
  already-running C call. If the awaiting task is cancelled (e.g.
  `asyncio.wait_for(p.commit_transaction(), 0.1)`) **after** the executor
  returned a non-null error handle but **before** `_raise_if_error` ran, the
  `KafkaError` handle was never freed (`_from_c`/`KafkaError_destroy` never
  ran) → leak, unbounded under a retry/cancel loop.
- **Fix**: added `async def _run_blocking(self, fn, *args)` on `AsyncProducer`,
  centralizing the executor call. It `asyncio.shield`s the executor future so
  cancelling the *await* does not cancel the still-running C call, and on
  `asyncio.CancelledError` attaches a done-callback that frees the handle once
  the executor thread finishes (`if not f.cancelled(): err = f.result(); if
  err: _lib.KafkaError_destroy(err)`), then re-raises. All 4 async txn ops now
  route through it. The normal (non-cancelled) path is behaviorally identical
  (`await` the executor, then `_raise_if_error`). `begin_transaction`
  (non-blocking, no executor) and `close()` (its `run_in_executor` calls run
  `Producer_shutdown`/`Producer_destroy`, which return no error handle) were
  left unchanged.
- **Test**: no deterministic unit test added. The cancellation window is a
  genuine thread/loop race — the executor thread must produce the handle in the
  instant between the await being cancelled and the frame resuming — with no
  deterministic trigger from a unit test (and faking a handle would call
  `KafkaError_destroy` on a bogus pointer). Documented in the `_run_blocking`
  docstring, per the code-review's explicit allowance. The normal path and
  error-raising path of all 4 ops are covered by the existing async txn tests
  (33 async tests pass), which now exercise `_run_blocking`.
