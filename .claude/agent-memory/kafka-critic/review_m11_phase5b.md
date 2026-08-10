---
name: review-m11-phase5b
description: M11 Phase 5b (four txn request handlers, TV2, 2PC) — CLOSED 4→1→0, closing Phase 5; Caller both-reachable audit, builder-snapshot vs doc claim, state-reachability proof by writer-Caller grep
metadata:
  type: project
---

Phase 5b (the four transactional request handlers + KIP-890 TV2 + KIP-939 2PC + 41 tests,
Actor 45b). **Pass 1: 4 findings** (1 behavioural, 3 records); **pass 2: 1** — a wrong
reachability claim introduced by the pass-1 fix. The four handler orderings the Actor
self-flagged were all *correct*; the defects were elsewhere.

**Pass-2 lesson (the recurring one): a fix that adds a test also adds a justification for the
test's fixture, and that justification is the new defect surface.** The two new `Caller` tests
picked `READY` as the invalid source and explained it as "the state the shutdown window actually
produces — the application task … leaves `READY` behind". Both `State::Ready` writers
(`reset_transaction_state`, `handle_init_producer_id_response`) are `Caller::Sender`, so the app
task has no path to it; and when the Sender writes it, the loop guard exits before `begin_abort`.
Method to reuse: for any claimed state reachability, grep the writers of that state and check the
`Caller` on each. Note the *method's* own rustdoc got it right ("can move the state", no state
named) — when a doc pair disagrees in specificity, the more specific one is usually the wrong one.

**How to apply:**

## 1. Audit `Caller` by enumerating Java call sites per method, not by reading rustdoc

The one behavioural finding: `begin_abort` hardcoded `Caller::App` although
`grep -rn "beginAbort()" producer/` gives **two** Java callers — `KafkaProducer.java:818`
(app) and **`Sender.java:273`** (the shutdown abort loop). Rules §1's anti-pattern list names
this exactly, and the consequence is real: a Sender-side invalid `→ ABORTING_TRANSACTION`
should poison (`TransactionManager.java:1124-1127` sets `FATAL_ERROR` + `lastError`), and with
`Caller::App` it does not.

The rustdoc *named* the Sender call site while the code contradicted it — so reading docs is
not an audit. Run the grep per entry point. For M11 the answer is: `beginCommit`,
`sendOffsetsToTransaction`, `maybeAddPartition` have exactly one (app) caller each;
`prepareTransaction` has none in `clients/src`; `beginAbort` has two.

## 2. A builder that snapshots at construction defeats "the retry carries only X"

`handle_txn_offset_commit_response`'s doc claimed the re-enqueued request "carries only the
outstanding offsets". False in **both** languages: Java's `TxnOffsetCommitRequest.Builder` ctor
does `setTopics(getTopics(pendingTxnOffsetCommits))` and the Rust does
`set_topics(get_topics(..))` — eager copies. `reenqueue()`/`retry()` re-add the same handler
with the same `data`, so the retry re-sends the full original set. The map governs only
retry-vs-complete and the *next* handler construction.

Generalise: whenever a doc says a retry is narrowed/widened by mutating a collection, check
whether the request data was snapshotted. The danger is not the comment — it is that the
obvious way to make code match it (rebuild before retry) introduces a wire divergence.

## 3. Delegate the accounting blocks to a fork; audit the handler orderings yourself

Phase 5b shipped ~400 lines of accounting comments with four extractable derivations. A fork
running them (with extraction guards: non-empty, balanced braces, even quote count) verified
`90 1 89` → 90/90 zero owed, `140 = 33 + 60 + 47`, both pasted tables byte-identical, and the
load-bearing join (0 `OWED_MGR` out of 107). That freed the whole budget for the four handlers.

Worth knowing about the join: its classifier does **not** scan private helper bodies (the
splitter breaks at `^    private `), so its error direction is false-*MGR* only — and since
every MGR was HAVE, it cannot manufacture the `0`. Establishing the error *direction* is what
makes such a check trustworthy; a marker histogram alone does not.

## 4. Named-but-inert markers are the accounting's soft spot

The prose justifying the join named two driver helpers,
`verifyCommitOrAbortTransactionRetriable` and `verifyProducerFenced`. The second matches
**0/107**: its only call sites (`TransactionManagerTest.java:2077`, `:2101`) sit inside the
*private* helpers `verifyProducerFencedForAddPartitionsToTxn`/`...ForAddOffsetsToTxn`, which
the splitter never scans. Check each named marker's actual hit count before crediting an
enumeration — the conclusion can survive while the stated evidence does not.

## 5. Verify a hot-path audit's load-bearing claim, don't accept it

The `maybe_add_partition` audit argued the per-record V2 `TopicPartition` clones are "an
`Arc<str>` refcount bump plus an `i32`, not a heap allocation". True —
`TopicPartition { partition: i32, topic: Arc<str> }` with derived `Clone`
(`topic_partition.rs:21-25`). Had it been `String`, the audit would have been inverted. One
`grep` settles it; the audit's whole conclusion rests on it.

## 6. Per-handler arm order differs *between* handlers — do not homogenise

`AddOffsetsToTxn` puts `UNKNOWN_PRODUCER_ID` **before** the fenced arm; `EndTxn` puts it
**after**. Each Rust handler matched its own Java handler. The generic
`error.is_retriable()` (standing in for `instanceof RetriableException`) must come *after*
every specific code that is also retriable — `CONCURRENT_TRANSACTIONS`, `NOT_COORDINATOR`,
`COORDINATOR_NOT_AVAILABLE`, `REQUEST_TIMED_OUT` are all in `errors.rs`'s retriable set, so
those orderings are load-bearing in four places.

## 7. Numbers beside code rot; re-derive them

PLAN §10.8 said "the 35 test call sites"; `grep -ro` gave **56**, across three files where two
were named. Third instance this milestone. Corollary: also re-derive the *file list*, not only
the count.

## 8. What I checked and deliberately did not file

`(Java A-B)` ranges in "Translated from" rustdocs carry ±1–3 lines of slop. No citation points
at a different method (which is what made the Phase-4 pass-3 finding substantive), and two
scripted attempts to measure them misfired before I checked by eye. Locators, not claims —
don't spend a fix cycle. Recording the *non*-finding stops the next pass re-deriving it.
