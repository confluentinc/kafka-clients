---
name: phase5b-txn-requests-notes
description: Milestone-11 Phase 5b (Actor 45b) — the four txn request handlers, TV2, 2PC; accounting-block derivation traps and the do_init_transactions coordinator-ready fixture bug
metadata:
  type: project
---

Phase 5b closed the `TransactionManager` translation: all 90 Java methods have a
Rust `fn`, all nine `State` variants are enterable, no `unsupported_version` stub
survives in `transaction_manager.rs`.

**Why:** it was the second half of the 5a/5b split the Manager invoked at `9faf0a0`
because Phase 4 had needed four Critic passes at smaller scope.

**How to apply:** the notes below are the non-obvious parts — the mechanical
things a future phase will hit again.

## The fixture bug worth remembering

`do_init_transactions` (the Rust port of Java's `doInitTransactions`) originally
omitted `handleCoordinatorReady`. Java's helper spins `Sender.runOnce`, which
connects to the transaction coordinator and calls it as a *side effect*
(`Sender.java:569`). That method is the only writer of
`coordinatorSupportsBumpingEpoch`, which decides whether `abortableErrorIfPossible`
recovers or goes fatal. Without it every such arm took the fatal branch Java does
not, and only one test
(`testBumpTransactionalEpochOnRecoverableAddPartitionRequestError`) surfaced it.

Generalisation: **when a Java test helper drives a real component, enumerate the
component's side effects on the class under test, not just its return value.** A
manager-level port that reproduces only the visible outcome silently drops them.

The inverse also came up: to get "no bump support" back, do it Java's way — call
`handleCoordinatorReady` with an *empty* coordinator record, which is the
coordinator-disconnect path Java's own test exercises. Do not add a setter.

## Accounting-block derivation traps hit this phase

1. **A self-matching grep can never reach 0.** Three attempts at "no stub
   survives" checks matched their own documentation line
   (`grep -c 'unsupported_version'`, then `'Milestone 11, Phase 5b'`, then the
   parenthesised call form). The form that works skips comment lines:
   `awk '!/^ *\/\// && /PATTERN/' FILE | wc -l`.
2. **Anchor block-title greps at line start.** Every accounting-block title also
   appears inside a doc comment *and* inside the derivation that reads it, so an
   unanchored `grep -n TITLE` returns three line numbers and the arithmetic that
   consumes it fails.
3. **A backtick-quoted-name search must not require the opening backtick.** One
   Rust test writes `` `TransactionManagerTest.testX` ``; matching `` `testX` ``
   reported it as owed. Match `` testX` `` instead.
4. **Read block line ranges from the file, never write them down.** Otherwise
   moving a block silently shrinks the corpus the derivation searches, and a
   shrunk corpus reports everything as owed — which looks like a huge regression
   or, worse, gets "fixed" by trusting the old numbers.
5. **Guard the extraction.** `test -s` plus a line-count floor. An empty corpus is
   the failure mode both agents have been burned by.

## Reclassify, don't preserve, a stale deferral rationale

The `SenderTest` transactional group of 15 was recorded as "blocked on a
Phase-5b/6 entry point it actually calls". After 5b that was false — the
derivation shows not one of them names `commitTransaction` / `abortTransaction`.
They became **owed, not blocked**, with Phase 6 named as owner from its own scope
line. PLAN §9.19 records the general lesson: a completeness claim whose *reason*
expires is the same defect as a wrong count.

## Where the remaining test debt sits

47 of `TransactionManagerTest`'s 107 group-B methods are owed, and the load-bearing
check is a join proving **none of them is manager-only** — every one drives the
accumulator or the `Sender`. Phase 8 owns them by its scope wording, ordered after
Phase 6 builds the end-to-end transactional harness. See
[[phase5a_txn_state_machine_notes]] for the 5a half.
