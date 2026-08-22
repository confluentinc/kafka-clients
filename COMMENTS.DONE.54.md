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
