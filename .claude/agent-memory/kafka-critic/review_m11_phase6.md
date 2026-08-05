---
name: review-m11-phase6
description: M11 Phase 6 (public producer txn API + Sender txn loop) — 7→4→1→1, 0 behavioural ever; splitter/sweep blind spots (mine twice), count cross-references, unpinned prod fix
metadata:
  type: project
---

**Pass 2 (`8c4cace`, all 7 fixes): 4 findings, all records — and the Actor corrected a
false supporting fact of *mine*.** Three of the four were created by the fix commit
itself. Lessons specific to pass 2, before the pass-1 material below:

## P2.1 A fix commit that updates counts must be swept for every prose site carrying them

Fixes 1-2 moved two denominators (27→28, 15→16) and updated the code blocks and the commit
message, but left the pre-fix numbers in **four** prose sites: PLAN §Phase-6's spec line,
its two-sentence "Status: landed" paragraph, §9.21's amendment bullet, and the Actor's own
`phase6_public_txn_api_notes.md`. Sweep: `grep -rn "All 27\|27 transactional\|15
transactional\|22 translated" design/ src/ .claude/`.

Sharpest instance: §9.21's bullet said Phase 6 "changed **disposition only** … with an
**empty membership diff**" — true when written for pass 1, falsified by fix 1 *in the same
commit*, which added a member. This is the milestone's signature defect (a fix's own
justification is the new defect surface) in its purest form: two fixes in one commit, each
correct, mutually inconsistent.

## P2.2 A correction in one section does not correct the section that cites it

§10.9 refuted my false fact explicitly ("That is false: it exists, at
`KafkaProducerMetrics.java:124`") *and* §9.21, amended in the same commit, asserted the
refuted version in the Actor's own voice 740 lines earlier. After any "X was wrong,
here's the corrected form" edit, grep the wrong claim's distinctive token repo-wide —
here `grep -n recordPrepareTxn PLAN.md` returned both the claim and its refutation.

## P2.3 I asserted a false exhaustiveness claim — two compounding errors to avoid

See §6 below for the full account. The two mechanics, worth internalising:
**(a)** Kafka abbreviates inconsistently — `recordPrepareTxn`/`recordBeginTxn` beside
`beginTransaction` — so a grep built from the long form cannot match the accessor. Grep the
**field** (`prepareTxnSensor`) and follow it, or grep both stems. **(b)** I read a bounded
`sed` window, saw the `record*` run end, and treated truncation as exhaustive. Never back a
"no X anywhere in the class" claim with a windowed read.

Corollary: when a correct conclusion has two candidate supporting facts, verify the one you
assert, not the one that sounds stronger. A false premise for a true conclusion is still a
defect — and the Actor was right to refuse to relay it.

## P2.4 Verifying an instrumentation-vs-contract claim: find where the hook runs

The pin's contract test sleeps 300 ms in a Sender exit hook and asserts a flag after
`close`. Whether "with the join the flag is *necessarily* set" is a causal guarantee or a
timing margin turns entirely on **where the hook runs**: it is inside the `spawn_blocking`
closure after `block_on(..)` returns, so the `JoinHandle` cannot resolve until the hook
completes — a guarantee. Had it been a detached task, the same prose would have been a
race. Always locate the hook relative to the joined future before crediting or faulting
such a doc.

## P3.1 A count-removal sweep must include cross-references, not just restatements

Pass 3's single finding: fix 2 deleted every restatement of the moved counts, but
`sender.rs:7817` said "for the same reason **the 15 above** are" — a *cross-reference* to a
group whose header now reads `(16)` two hundred lines up (and whose owed subset was always
11, so 15 matched nothing even before). Grep shape that finds these:
`grep -nE "the (one|two|…|[0-9]+) (above|below)"` over the block, plus `(N) —` group
headers. Restatement sweeps naturally target the headline number; the referring phrase a
few hundred lines away survives.

## P4.1 My own sweep was wrap-blind and start-only — the second self-inflicted classifier gap

Pass 3 I disclosed "3 of 24 rustdoc headers are off" and used the ratio to argue the
deviation was an isolated outlier. Both halves were wrong, and the Actor caught it:

- **Wrap-blind population.** My regex needed ``Translated from `SenderTest.<name>` `` on one
  line. **17 of 41** headers wrap — "Translated from" ends the line, the backticked name
  starts the next — so my denominator was 24 against a true 41.
- **Start-only criterion.** I compared only the range's first number to the declaration,
  never the second to the closing `    }`, so a pre-existing end-off-by-one
  (`testNodeNotReady` 711 vs 712) was structurally invisible.

**Durable fix, for any sweep over doc comments:** join a window of following `///` lines
before matching (never match a doc field on a single line), and check *every* field the
convention constrains, not the first one. And never quote a ratio from a population you have
not separately verified — "3 of 24" was the argument, and the 24 was the defect.

Second time this phase I committed the defect I was reviewing for (see §6 for the first).
Both were confident denominators from classifiers that could not see a shape — the exact
thing pass 1 filed against the Actor. Before asserting any "N of M", ask what M would miss.

## P4.2 A range regex requiring the closing paren silently drops annotated ranges

Pass 4's finding: the block claimed "40 headers, 0 mismatches" over 41. Cause pinned by
running both forms — `\(Java \d+-\d+\)` (closing paren required) matches exactly **40**; the
41st reads `(Java 715-735 — the produce-request variant is at Java 2159-2179)`, a clause
*inside* the parens. Use `\(Java (\d+)\s*[-–]\s*(\d+)` with no trailing `\)`.

Worth remembering because the block's own prose, three sentences below the count, describes
this very escape ("its range is followed by a clause inside the same parentheses rather than
closing them"). When a paragraph documents a sweep-escape shape, test the sweep *against
that shape* — the documented hazard is the likeliest live one, and an off-by-one denominator
is its fingerprint.

## P3.2 Honour a pre-committed non-finding rather than re-litigating at a new threshold

Pass 1 recorded "`(Java A-B)` header ranges carry ±1-3 lines of slop; the bar for filing is
that a citation points at a *different method*" — recorded explicitly so it would not
consume a fix cycle. Pass 3's sweep found headers off by −1, −2, −5, the last starting on a
`}` inside the previous method's body. Tempting to file the −5. I did not: each range still
brackets its own method, and moving the bar from −3 to −5 on the closing pass would be
arbitrary. **Disclose it as a checked non-finding with the reasoning** — that keeps the
record complete without manufacturing, and stops the next pass rediscovering it.

The judgement held up; the *arithmetic* I attached to it did not. I reported it as "3 of 24",
and it was really 5 of 41 (P4.1) — so the "isolated outlier" ratio I leaned on was not
evidence, even though the decision not to file was still right on its own bar. Keep the bar,
drop the ratio: argue such a call from the *kind* of deviation, not from a proportion of a
population you have not verified.

## P3.3 A "must not disagree" invariant is the right shape when the bad form appears in refutations

The Actor's shipped check is `grep -n <token> PLAN.md` "must not return two sections that
disagree" — deliberately *not* a zero-hit rule, because the false form legitimately appears
inside the text refuting it, so a zero-hit rule could only be satisfied by deleting the
refutation. Semantic invariant + exact command is honest and discriminating; judge such a
check by whether it is violated in the case that matters, not by whether it is mechanical.

## P3.4 Two grep traps I hit this pass

- **BWK awk (macOS) does not support `\b`.** `awk '/\b52\b/{...}'` printed **nothing** over
  a file with eight matches. Empty output from a pattern you expect to hit is the tell —
  re-run with `grep -n` before concluding "no mentions". Same family as the pass-5a `delete`
  trap.
- **Single-line patterns miss method chains split across lines.** `grep -n
  "field\.\(insert\|extend\)"` found nothing for a feeder whose `.extend(..)` sat on the
  line *after* the field. Grep the bare field name and enumerate every hit; that is also
  what turns a two-feeder claim into an *exhaustiveness* check, which is what such an
  isolation argument actually needs (removals like `.clear()` / `.retain()` cannot make a
  union non-empty — classify each writer, don't just count them).

## P2.5 Two sibling accounting blocks can disagree about what a line number means

`KafkaProducerTest`'s block cites the **declaration** line (2423, correctly rejecting the
2422 annotation); `SenderTest`'s new entry cites the **annotation** (507, not 508) while
its own convention line says "the `public void` declaration line throughout". Check a new
entry against the block's stated convention *and* against the sibling block — and check
**all** citations to establish whether a deviation is newly introduced (here 1 of 55) or
inherited.

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
0 hits. `completeTransaction` likewise exists only in a message string.

The metrics sensor is dead scaffolding and independently confirms the refusal, but state
the fact correctly: **`recordPrepareTxn` DOES exist, at `KafkaProducerMetrics.java:124`.**
The load-bearing fact is that it has **zero callers** — `grep -rn recordPrepareTxn kafka/`
gives 1 hit tree-wide (its own declaration) against `recordInit`'s 3 in
`kafka/clients/src/` (declaration, `KafkaProducer.java:655`, and
`KafkaProducerMetricsTest.java:52`). A sensor plus a recorder that nothing invokes, not
even a test, is what a forward-looking KIP-939 artifact looks like.

**I got this wrong in pass 1** and the Actor corrected me; it is recorded in §10.9
deviation 2. Two compounding mistakes, both worth avoiding:
  - I grepped `prepareTransaction`, but the method is `recordPrepareTxn` — **Txn, not
    Transaction**. Kafka abbreviates inconsistently (`recordBeginTxn`/`recordCommitTxn`
    vs. `beginTransaction`), so a pattern built from the long form cannot match the
    accessor. Grep both stems, or grep the field (`prepareTxnSensor`) and follow it.
  - I then read `sed -n '55,115p'`, saw the `record*` run end at `recordSendOffsets`, and
    treated a **truncated window as exhaustive**. The method was at 124, nine lines past
    my read.

This is the mirror image of §2's own warning about exhaustiveness claims — "no X anywhere
in the class" is the strongest possible claim and needs the widest possible grep. When a
*correct conclusion* has two candidate supporting facts, verify the one you assert rather
than the one that sounds stronger; a false premise for a true conclusion is still a defect,
and the Actor was right to refuse to propagate it.

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
