# M15/P8 — Critic 78 clearance record

**Result: CLEAN — 0 findings.** No `COMMENTS.78.md` was ever created; no fix cycle ran.

Critic 78 ran **exactly once**, after Actor 78 reported all six RPCs complete and green, per the
phase's approved coordination constraints (PLAN §8.1). Range reviewed: `f58bf93d..88aaed4d`.

## Review priorities assigned

1. **D47's hardened test mandate — verify by injection, not by reading.** Mutate a production
   reader; confirm the matching test goes RED. A test that stays green under a real mutation is
   testing the harness (P3-Stage-3 lesson: a green injection needs a control that goes red to be
   interpretable).
2. The two nested `(i,j)` walks (`describeProducers`, `describeTransactions`) — off-by-one,
   `i`/`j` transposition, failed-row behaviour.
3. The three presence discriminants — `false` must yield `null`, never `0`/`-1`; `[MarshalAs(I1)]`
   on every C `bool`.
4. Memory safety on the two no-result-handle RPCs (`abortTransaction`, `forceTerminateTransaction`),
   which bypass `KeyedResultMarshal`: `GCHandle` published before the P/Invoke (both fire the
   completion callback synchronously on the calling thread for bad input), borrowed-vs-owned error
   rule, `DangerousAddRef` spanning the op.
5. **D46** — `TerminateTransactionResult.Result()` (not `All()`), matching
   `TerminateTransactionResult.java:36`.
6. The `TransactionState` wire-name table, both directions — confirming the *corrected* rationale
   holds by measurement, not merely that the earlier false one is gone.
7. `listTransactions`' two-count transposition hazard and its three-view result.

## Explicit do-not-file list supplied (non-findings, verified during planning)

- **No empty-input precondition.** Java has no empty guard on `describeProducers` /
  `describeTransactions` / `fenceProducers` (`KafkaAdminClient.java:4824-4830`, `:4833-4839`,
  `:4889-4895`) and **succeeds** on empty. P7's 77.1 does **not** recur here; adding a Java-less
  throw would be the defect.
- **`ListTransactionsOptions`' two filter arrays conflate null/empty and both mean ALL** — a C#
  empty collection is safe here, the inverse of P5's `ListConsumerGroupOffsetsSpec` landmine.
- **Java's mirrored field-width inconsistencies** (D51) are faithful, not defects.
- Style, naming taste, and xmldoc verbosity — terse comments are this phase's instruction.

## Actor self-corrections, verified independently (filed against the PLAN, not the Actor)

All three were already fixed in the Actor's commits:

1. A **false rationale the Actor had written itself** for the `TransactionState` wire-name table,
   caught and replaced with a measurement — the §9.1 item-10 discipline applied to its own prose.
2. `TransactionState` belongs in the **`Admin`** namespace, not root, per CLAUDE.md's topical-folder
   rule. ⚠ **PLAN §2 said root and was wrong** — plan defect #8 in M15. The Actor overrode the plan
   against the source, which is the behaviour the standing rule asks for.
3. The plan's Java cite for `describeTransactions` read `:4780-4787`; the actual is `:4833-4839`.

**Standing rule applied throughout: the PLAN is never review ground truth over the Java source.**
