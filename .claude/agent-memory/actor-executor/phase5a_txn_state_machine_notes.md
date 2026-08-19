---
name: phase5a-txn-state-machine-notes
description: Milestone-11 Phase 5a (Actor 45) — non-obvious findings from translating TransactionManager's transactional state machine, coordinator subsystem, and the mechanical test/method accounting technique
metadata:
  type: project
---

Phase 5a of Milestone 11 (transactional state machine + FindCoordinator) landed in
five commits on `milestone11-producer-transactions`. Five findings that are not
derivable from the code and would cost real time to rediscover.

**Rules §2's premise is false for one of the four fields it names.**
`coordinatorSupportsBumpingEpoch` is *not* Sender-confined in Java: `KafkaProducer.doSend`'s
`catch (ApiException e)` reads it on the application thread via
`maybeTransitionToErrorState` → `needToTriggerEpochBumpFromClient`. The other three
(`transactionCoordinator`, `consumerGroupCoordinator`, `inFlightRequestCorrelationId`)
really are confined. **Why:** the rule states a blanket claim that only holds for 3 of 4,
and a Critic checking either direction mechanically will get it wrong.
**How to apply:** when a rule asserts "only thread X touches this", grep the *callers of
the readers*, not just the field's `synchronized` status — and if a rule's premise fails,
record the deviation with the call chain rather than following the rule's letter.

**Java 4.2 never calls `setKeepPreparedTxn`.** `initializeTransactions(keepPreparedTxn)`
uses the flag for two log lines only; it is absent from the request data, so
`builder.data.keepPreparedTxn()` — the guard on the 2PC response arm — is always false in
`clients/src`. **Why:** it looks like a translation gap and invites "you dropped the 2PC
path". **How to apply:** before treating an unreachable Rust branch as a deferral, check
whether Java's own path can reach it; `grep -rn setSomeField clients/src` settles it.

**`TransactionManagerTest` has no 2PC test at all in 4.2**: `prepareTransaction` and
`preparedTransactionState` appear in no method body, and `doInitTransactionsWith2PCEnabled`
is declared and never called. 2PC cover must come from `KafkaProducerTest`.

**A mechanical test-scope derivation needs three refinements that are easy to miss.**
Splitting a Java test file into per-method blocks: (1) break at `private` members too, or
helpers sitting between two tests are absorbed into the earlier one; (2) markers reached
only through a helper need the *helper name* in the marker list; (3) a marker like
`maybeAddPartition` must be *conditional* — suppressed on an `assertThrows` line and for an
idempotent fixture — or it blocks every test that only expects it to throw.
**Why:** each of the three silently mis-scoped tests in the direction that looks like less
work, which is exactly the direction a reviewer distrusts.
**How to apply:** cross-check the derived set against a hand list once, then trust the
script; ship the script with the claim so the numbers are re-derivable.

**Method coverage is worth deriving too, not just test coverage.** A regex over Java's
class-level declarations vs `fn <snake>(` in the Rust files found four methods (`isReady`,
`isTransactionV2Enabled`, `isPrepared`, `preparedTransactionState`) that were translatable
in-phase and had simply been missed — none of which any test or reviewer had flagged.
Two systematic false signals to expect: acronym names (`is2PCEnabled` → the crate spells it
`is_2pc_enabled`, naive conversion yields `is2pc_enabled`), and *partially* translated
methods that still define the `fn` and so are not reported.
