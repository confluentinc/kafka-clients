---
name: review-m11-phase7
description: M11 Phase 7 (MockProducer txn surface) — CLOSED 4→0 in two passes, all findings records; a *set* (not a count) can be undercounted differently by each artifact; FFI contract is fallout of a correct core fix
metadata:
  type: project
---

**CLOSED on a clean second pass (`bd19e89`).** 4 findings, all records, all conceded with
nothing disputed — fourth consecutive M11 phase where the production translation was right
on arrival. Fastest loop of the milestone (4 → 0); the fix commit touched no executable
line, only doc comments in `mock_producer.rs` / `ffi/producer.rs` plus PLAN and memory.

**Pass-2 lesson: normalisation-based diffs need every mechanical transform enumerated, and
a rename mapping.** My finding-1 table was right (four tests) but the sentence around it
named only *one* mechanical swap; there were two (`with_auto_complete` → `build_mock_producer`
**and** `make_record("topic","keyN","valueN")` → `recordN()`). Re-running at all three
stages: **19** bodies differ un-normalised, **6** after the first swap, **4** after both.
The sixth is a *renamed* test (`should_be_flushed_after_flush` →
`should_not_be_flushed_after_flush`), which a name-keyed body diff reports as *missing*
rather than changed — so it is invisible without an explicit rename map. Third time this
milestone the Actor corrected a supporting fact of mine while conceding the finding; the
pattern is that my *conclusions* survive and my *supporting sentences* over-generalise from
what I classified by eye.

Phase 7 (`MockProducer`'s transactional surface, N=47). Pass 1: 4 findings, zero
behavioural defects in production code. All 22
production entry points matched `MockProducer.java` statement-for-statement (guard order,
knob position, staging branch, `mem::take` vs `clear()`, six verbatim messages).

**The transferable lesson: when several artifacts each disclose "three X", check whether
they name the *same* three.** The in-file accounting block and PLAN named
`testMetadataOnException` + 2 closed-producer tests; the commit message and the Actor's
memory named `testManualCompletion` + the same 2. Four tests were strengthened, and each
artifact was written beside the commit that made its own subset visible. A count-level
sweep (`grep "three"`) finds nothing wrong — the numbers agree. Only diffing the
*membership* against the diff finds it. Method: strip comments and whitespace from every
pre-phase test body, diff against post, and classify each change; here every non-listed
change was one mechanical fixture swap (`with_auto_complete` → `build_mock_producer`),
which is what made the four unambiguous.

**How to apply:**

## 1. A correct core fix can falsify a contract in a file the phase never touched

`Completion::complete` was fixed to pass a −1-filled `RecordMetadata` on the error path
(Java `MockProducer.java:578`) — right, and matching `kafka_producer.rs:1155`. Fallout:
the FFI shim boxes whichever halves are `Some`, so `kafka_producer_Producer_send_async`'s
documented C contract *"(the other argument is null)"* (`src/ffi/producer.rs:1142`,
shipped in `target/include/confluent_kafka.h`) is now false for the mock path too.

Worth knowing *why* it is insidious: **Java is itself inconsistent across its three
callback sites**, and the Rust mirrors each faithfully —

  - `ProducerBatch.java:315` → `onCompletion(null, exception)` (metadata null)
  - `KafkaProducer.java:1060` → `onCompletion(nullMetadata, e)` (non-null, −1-filled)
  - `MockProducer.java:578` → same non-null −1-filled form

so the C sentence is *true* on the highest-traffic path and false on the other two. When
a fix aligns one path with Java, grep the FFI/C prose for uniformity claims about that
path's shape. `grep -rn "the other argument is null\|whichever is non-null" src/ffi/`.

Also: Java's `ProducerBatch.completeFutureAndFireCallbacks` wraps each callback in
`try/catch` + `log.error` (`:318-320`) while `MockProducer.Completion.complete` does
**not** — the Rust `producer_batch.rs` has no equivalent catch. Out of Phase 7's scope
but a real pre-existing divergence if a later phase needs it.

## 2. Verifying a "panic is swallowed" claim: grep the path, then look at the code beside it

`test_metadata_on_exception`'s rustdoc justified capture-then-assert with "A panic inside
the callback would be swallowed by the mock". False: fully synchronous path, no
`catch_unwind` (the producer hits are all `#[cfg(test)]`), so it unwinds out of
`error_next` (poisoning the mutex) and fails the test. Two cheap disproofs: the closure
*itself* relies on `.expect()` panicking for the no-metadata case; and Java's
`Completion.complete` has no try/catch either. The conclusion was right and independently
justified ("a callback that never fires would pass silently") — a false premise for a
true conclusion, the shape I conceded against myself in Phase 6 §P2.3.

## 3. Single-statement citations are not range headers — the ±1-3 bar does not cover them

I have twice declined to file `(Java A-B)` header slop because a range still brackets its
own method. `ConsumerGroupMetadata.java:41`'s `Objects.requireNonNull(groupId, ..)` is a
different shape: a line cited as the location of a statement quoted verbatim next to it,
and the statement is at `:42` (`:41` is the last parameter line; the ctor actually entered
is the one-arg at `:52`, delegating to `:38`). Filed, with the distinction stated so the
next pass does not think the bar moved. Three sites carried it (file + two PLAN sections).

## 4. Test the guards the Actor ships against the escapes they document — all three held

First phase where every shipped guard behaved exactly as its prose claimed: deleting
`w && /^    @/{next}` keeps `rows=55` and changes exactly one row (`SuppressWarnings`,
Java 688); relaxing `^ *///` to `^ *//` double-scores exactly the two NOT APPLICABLE
markers; and the *self-falsified* claim was honest — the prefix-nesting probe prints `[]`
and dropping the closing backtick changes no count, and the block says so instead of
claiming the guard is load-bearing. Phase 6's lesson (a documented escape shape is
adversarial input for a checker in the same file) was handled before the fact.

Extraction guards that worked for me: `bash -n` for shell and `compile()` for python
beat brace/quote counting — a shell comment containing `Java's` makes the quote count odd,
and a python regex containing `\(` makes the paren count unbalanced. Both would have
aborted a correct extraction.

## 5. "N tests" can mean passed or run — check `ignored` before calling a delta a regression

`298a430` said 2451 lib, the comment-only `fb3da54` said 2449. A comment-only commit
cannot drop two tests, so it looked like a defect. `cargo test --lib` reports
`2449 passed; 2 ignored`: one message counted the run, the other the passes. Nearly filed.

## 5b. Verifying a doc-only fix to a *generated* artifact

Phase 7's issue-3 fix changed a Rust doc comment and nothing else, with the generated C
header left untracked. That is the right call here, and the way to confirm it is three
greps, not an argument: no `.h` is tracked (`git ls-files | grep '\.h$'` minus vendored
unity → empty); `build.rs` generates `target/include/confluent_kafka.h` via cbindgen under
`#[cfg(feature="ffi")]`; and `bindings/c/CMakeLists.txt:11` points the C tests at
`target/include`. The load-bearing detail is easy to miss: `build.rs` emits
`cargo:rerun-if-changed=src/` **inside the ffi branch**, and emitting *any*
`rerun-if-changed` disables cargo's default re-run-on-any-package-change — so without that
`src/` entry the header would go stale after an `src/ffi/` edit. Check for it before
crediting "the doc is the single source of truth".

## 6. Non-findings worth not re-deriving

- **Callback-under-guard reentrancy.** `Completion::complete` fires the user callback with
  the `Mutex` guard held, so a callback calling `producer.flushed()` deadlocks where Java's
  reentrant monitor allows it. Pre-existing (`send_with_callback` under `auto_complete` and
  `error_next` already did this), no Java test covers it, and the obvious "fix" (release
  before the callback) trades Java's atomicity for it. Disclosed rather than filed; §10.10
  deviation 2's stated reason ("no `.await`") answers a different hazard than the
  reentrancy the deviation is about.
- **`cargo doc --no-deps` fails repo-wide** (44 sites). Exactly one is in
  `mock_producer.rs:365` and it is verbatim pre-existing. `cargo doc` is in neither
  CLAUDE.md's workflow list nor `make verify` (`build format-check lint test`), so no DoD
  clause is in play. Disclosed only in the Actor's private `phase1_design_notes.md`,
  unlike its sibling §9.14 which got a numbered PLAN entry.
- **The "10 absent methods block zero tests" summary is the brief's paraphrase, not the
  artifacts' claim** — they say *nine* block zero (verified) and treat `partition`
  separately. Third time in M11 that a brief's compression of a claim looked like a defect
  in the claim (cf. Phase 6 §7).
- **`done()`-after-callback is genuinely load-bearing**, not cosmetic: `ProduceRequestResult::set`
  deliberately does not notify and `is_done()` reads the watch value, so a concurrent waiter
  really could have observed completion early. Check the two-phase primitive before crediting
  or faulting such an ordering fix.
- **Java's `MockProducerTest.testMetadataOnException` has a real hole the Rust closes.**
  `assertTrue(producer.errorNext(e))` proves a `Completion` was *popped*, not that the
  callback *ran*, and the four assertions live inside the callback — so a "callback never
  fires" bug passes the Java test. Capturing outside and asserting after the call returns is
  strictly stronger. Useful precedent whenever a Java test asserts only inside a callback.
- **Pass 2 declined two prose imprecisions**, both recorded in `COMMENTS.47.md`: "six bodies
  differ before that normalisation" (19 before *any*, 6 after one of two swaps) and a
  commit-message "the only `catch_unwind` in `producer/`" (there are two, both test-only).
  Bar applied: the block's machine-checkable contract is its embedded programs plus pasted
  outputs — both reproduced — and neither number is cross-referenced elsewhere. Filing an
  imprecise antecedent would have applied a threshold I have not applied to range-header
  slop or to pass 1's "four collections". Keep that bar.
