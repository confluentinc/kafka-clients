---
name: review-m11-phase6
description: M11 Phase 6 (public producer txn API + Sender txn loop) — pass 1 = 7 findings, 0 behavioural; name-prefix splitter blind spot, rotated citations, unpinned prod fix, refused-spec record sweep
metadata:
  type: project
---

Phase 6 (`Producer` trait txn methods, `KafkaProducer`'s five public methods, Sender
transactional loop, `PendingRequests` reclassification, N=46). **Pass 1: 7 findings —
zero behavioural defects in production code.** 5 record/accounting, 2 missing tests.
All four adjudications the brief raised resolved **in the Actor's favour**. Same shape as
5a/5b: the code is right, the claims about it are not.

**How to apply:**

## 1. Test-accounting splitters keyed on a name prefix have blind spots — enumerate the exceptions

Two independent missing tests, both invisible to their own completeness check:

- `senderThreadShouldNotGetStuckWhenThrottledAndAddingPartitionsToTxn`
  (`SenderTest.java:508`) — the block's splitter is `/^    (public|private) void test/`.
  This is the **only** annotated test in the file whose name doesn't start with `test`
  (76 checked). Widening to `void [A-Za-z0-9_]+\(` took 52 → 57: 4 private helpers
  correctly excluded, 1 real test. In-scope count was 53, not 52.
- `testPartitionAddedToTransaction` (`KafkaProducerTest.java:2422`) — passes the marker
  filter's *prose* but not its *program*: it drives the txn path through
  `mock(TransactionManager.class)` injected by `KafkaProducerTestContext`, so it names no
  public txn method and no `TRANSACTIONAL_ID_CONFIG`. Denominator was 28, not 27.

Method: always derive ground truth from the **annotation** (`@Test`/`@ParameterizedTest`)
and the enclosing-body content, never from the method name; then `grep -c` the Java name
across `src/ tests/ design/ COMMENTS*.md .claude/` — 0 hits is the finding.

**A `comm -23` "nothing in scope is unplaced" check is vacuous when both sides share the
filter's assumption.** Here the Java list and the Rust list were both `test`-prefix
filtered, so a method missing from the Java side could never surface as unplaced. Ask
what the check's error *direction* is (5b lesson 3) — if a miss on one side is silently
mirrored on the other, the check cannot manufacture its own zero.

Corollary: the mock-injected test also exposed a **real coverage gap**, not just
bookkeeping — `maybe_add_partition` had exactly one reference in `kafka_producer.rs` (the
production call at `:1080`) and no test. Check whether the missing test's subject is
tested *anywhere at the same layer*; "well covered at the manager level" is a different
assertion from "the producer wires it".

## 2. Citations can be right as a set and wrong as a mapping

§10.9's four app-side call sites cited `KafkaProducer.java` 653/741/784/818 — the correct
**set**, with three attached to the wrong method (names rotated one position: 741 is in
`sendOffsetsToTransaction`, 784 in `commitTransaction`, 818 in `abortTransaction`). The
`TransactionManager.java` targets (299/353/361/404) were all correct, which is what made
each arrow false: "commitTransaction (:741) → beginCommit (:353)" cites a line that does
not call `beginCommit`.

Don't just check each number lands in *a* method — check it lands in the *named* one.
`awk -v L=$line 'NR<=L && /^    public [A-Za-z<>, ]*[a-zA-Z]+\(/{m=NR": "$0} NR==L{print m}'`
prints the enclosing method for a line, which settles it in one pass. Distinguish this
from the ±1-3 line slop that Phase 5b decided not to file: slop stays inside the right
method, rotation does not. Escalate when the citation is the **stated evidence for a
rules amendment** — the wrong pairing gets copied into the rules file.

## 3. A prod-fix's cited discriminating test may not discriminate — trace the failure path

`await_sender_handle` passed the `JoinHandle` by value into `tokio::time::timeout`, so
expiry dropped it (the handle was already `.take()`n) and `close(Duration)` returned with
the Sender running. Correct fix (`&mut join_handle` + restore). But both the commit
message and PLAN claimed the three `testCloseIsForcedOn*` tests "are what made it
observable", and as shipped **they pass either way**:

- only discriminating assertion is `elapsed < 5s`; without the join `close` returns
  *sooner*, so it still passes;
- the second assertion is behind `if let Ok(_) = timeout(500ms, init)` and `init` is
  parked on 60 s `max.block.ms` in both variants, so the arm never runs;
- no runtime-shutdown hang either, because `force_close()` already ran and the detached
  Sender exits.

Reusable move: for any bug-fix commit, mentally revert the diff and walk each assertion in
the named test to see which one flips. If none does, the fix is unpinned — file it, and say
which seam a test should observe (here: assert `sender_handle` is `Some` after an expiring
`await_sender_handle`, reachable from the in-module `mod tests`).

## 4. Guard-across-`.await` sweeps: two patterns, and one brace trap

`clippy::await_holding_lock` is **not** enabled in this repo, so this must be checked by
hand. Two complementary sweeps:

1. `let [mut] x = ….lock().unwrap();` bound to a local — walk forward to the binding's
   scope end and look for `.await`. **Trap:** `} else {` is brace-neutral on one line, so
   an end-of-line depth check walks past the `if` block into its sibling `else` and
   reports a false candidate. Check depth **per character** and stop at the first
   position where it goes negative. This produced my only false candidate
   (`sender.rs:1329`) and I nearly filed it.
2. Inline lock temporaries sharing a statement with `.await`. Crude statement splitter is
   fine; expect noise from `tokio::sync::Mutex` (`.lock().await`, legal) and from test
   assertions following `run_once().await`.

Also filter `.await` matches for the substring trap: `awaiting_validation`,
`await_result`, `await_wakeup` are not await points.

Repo-wide result at this commit: 411 guard bindings, **zero** real hits.

## 5. Falsifying rules §2 a second time — and the sentence that overstated the win

The Actor moved `pendingRequests` out of rules §2's Sender-confined group to
`Arc<Mutex<..>>` shared with the producer. **Correct**, verified independently: app side
reaches `enqueueRequest` at `TransactionManager.java` `:325`, `:375`/`:393`, `:433`,
`:1829`, all `synchronized`; Sender side via `nextRequest` (`:894`), `retry` (`:936`), and
the **unsynchronized** `lookupCoordinator(TxnRequestHandler)` (`:969` → `:1191` →
`:1188`).

Second §2 falsification this milestone (after `coordinatorSupportsBumpingEpoch` in 5a).
Treat rules §2's field grouping as a hypothesis per field, not a fact.

But the doc justifying it said Java's unsynchronized writer "is safe for a different
reason (only the Sender calls `lookupCoordinator`)". Single-caller confinement does **not**
make it safe — that `add` races the app thread's synchronized `add`s on a non-thread-safe
`PriorityQueue` with no happens-before edge. It is a genuine (narrow-window) Java race,
and Rust taking the lock there is *strictly safer than Java* — a better argument than
calling the race safe. Watch for justifications that prove a weaker claim than the one
they state, especially when they feed a rule amendment.

Lock-order audit that mattered: order is `deque → pending_requests → manager`; all 20
sites bind the queue guard to a local first (the field doc correctly notes Rust evaluates
a method receiver before its arguments, so `manager.lock().m(&mut q.lock())` would
invert). Inversion is not expressible from the manager side — `transaction_manager.rs` has
no such field and takes `&mut PendingRequests` as a parameter. Confirm no hot path takes
it: only the 4 rare public methods + construction in `kafka_producer.rs`.

## 6. Adjudicating a refused spec line: check every forward-pointer, not just the spec

The Actor refused PLAN §Phase-6's `prepare_transaction` and was **right** — in `kafka/` at
tag `4.2.0`, `prepareTransaction` exists only on `TransactionManager`; `Producer.java` has
0 hits. Stronger evidence than the Actor's own: `KafkaProducerMetrics` creates
`prepareTxnSensor` but has **no `recordPrepareTxn` method**, so the sensor is dead and
implies no producer method (a dangling metric is not evidence of a method — check for its
`record*` caller). `completeTransaction` likewise exists only in a message string.

The spec line *was* struck through with a correction, but PLAN §9.21's "Handed forward"
sentence still promised the surface to Phase 6. **When a phase refuses a hand-forward,
grep the predecessor's hand-forward list too** — and the same sentence carried a stale
"18 rows" (see below). Sections whose status line reads "DONE — closed on clean passes"
are not exempt.

## 7. The 18-vs-15 reconciliation: read whose state a prior Critic finding described

Brief flagged "Phase 5b reclassified 18 → Actor reports 15, where did 3 go?" Answer:
`18 = 3 + 15`, stated in the pre-Phase-6 tree itself, and the header already read
`TRANSACTIONAL (15)` **at `c59c09d`**. The 3 are the ones Critic 45 issue 3 made
translatable (Java 636/689/2991), all present in `sender.rs`. The discrepancy came from
reading Critic 45's description of the **pre-fix** state as the post-fix state.

Generalise: a prior Critic's finding text describes the state it was filed against. Before
treating a count delta as a loss, check whether the fix that closed the finding is what
changed the count — `git show <pre-range-commit>:<file>` and read the block's own arithmetic
sentence.

## 8. Non-findings worth not re-deriving

- `transactional_id().map(str::to_string)` in `send_produce_request` allocates where Java
  hands a reference, but it is per-produce-RPC (not per record) and the generated
  `ProduceRequestData` owns the field — no borrowed form exists to pass. Not a §11 defect.
- The `spawn_blocking` + private current-thread runtime harness is sound, and its
  diagnosis is accurate: in tokio's multi-thread scheduler only one worker holds the time
  driver, and a worker that picks up a never-yielding task never returns to its park loop
  to drive it while the others condvar-park — so **no timer in the runtime fires**. Worth
  remembering as a real tokio hazard with no Java analogue.
- Weakening Java's `assertThrows(KafkaException.class, producer::initTransactions)` to a
  conditional check is faithful here: Java submits it to an `ExecutorService` and discards
  both the `Future` and the latch boolean, so Java does not assert it either. Check what
  Java does with a latch's return value before calling a dropped assertion a regression.
- `#[async_trait]` is correctly absent from the `Producer` trait (plain `async fn` under
  `#[allow(async_fn_in_trait)]`); `begin_transaction` sync is right per Java `:674-681`.
