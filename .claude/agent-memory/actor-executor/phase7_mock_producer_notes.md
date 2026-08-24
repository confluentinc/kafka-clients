---
name: phase7-mock-producer-notes
description: Milestone-11 Phase 7 (MockProducer txn surface) — reentrant-monitor split, mem::take for Java's add-then-reassign, test-the-guard-before-shipping-the-claim, unrepresentable-input test category
metadata:
  type: project
---

Phase 7 (Actor 47) translated `MockProducer`'s transactional surface into
`src/producer/mock_producer.rs`. 53/55 `MockProducerTest` methods translated,
2 not applicable. Five commits, `make verify-sandbox` exit 0 on each.

**Why:** the phase existed because Phase 6 shipped five loud stubs on the mock;
PLAN called it "the cheapest full-parity phase" and it was — the whole diff is
two files (`mock_producer.rs`, `PLAN.md`).

**How to apply:** four patterns worth reusing.

1. **Java `synchronized` reentrancy → move the body to the inner type.** Java's
   `MockProducer.flush()` is `synchronized` and called from the equally
   `synchronized` `commitTransaction`/`abortTransaction`. A Java monitor is
   reentrant; `std::sync::Mutex` is not, so the public method becomes a
   lock-acquiring shim over an inner `fn` that runs with the guard held. Look for
   this at *every* Java `synchronized` method that calls another one on `this` —
   it is a silent deadlock in Rust, not a compile error.

2. **`mem::take` is the translation of Java's "add the object, then reassign the
   field".** `commitTransaction` appends the uncommitted-offsets map *object* to
   the history and then does `field = new HashMap<>()` (not `.clear()`, which
   would empty the map it just published). `abortTransaction` two lines later
   *does* use `.clear()`, because nothing retains it. When Java mixes the two in
   one class, the asymmetry is deliberate and load-bearing.

3. **Test the accounting guard before shipping the sentence that justifies it.**
   I wrote "without the closing backtick, `shouldBeginTransactions` matches
   `shouldThrowOnBeginTransactionsIfTransactionInflight`", ran it, and it was
   false twice over (the `MockProducerTest.` prefix already stops it; and no
   `@Test` name in the file is a strict prefix of another). Shipped the honest
   version instead — kept as defence against a future nesting name, *with* the
   check that shows it buys nothing today. The guard that *was* load-bearing was
   a different one (three-slash vs two-slash anchor, which stops the NOT
   APPLICABLE markers from double-scoring), and it only surfaced because I tested
   each claim separately.

4. **"Unrepresentable input" is a distinct skip category from "missing
   surface".** `shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction`
   looks like it needs production work; it does not. Its NPE comes from
   `new ConsumerGroupMetadata(null)` *in the lambda* — before the mock is entered
   — and Rust's `impl Into<String>` parameter cannot receive null. Read where the
   Java exception is actually *raised*, not what the test is named after; the
   name pointed at the wrong method entirely.

**Found and fixed while there** (both pre-existing, both in
`Completion::complete`, both against Java 567-581): the error-path callback
received no metadata where Java passes a −1-filled `RecordMetadata`, and
`result.done()` ran *before* the callback instead of after. The first was
visible as a `let _ = topic_partition;` — an unused-binding suppression is a
good smell for "a field was dropped from a translation".

**Also:** a derivation that classifies Java names cannot see a test that is
present but *weakened*. **Four** were — `testManualCompletion` (bare `is_err()`
where Java compares the cause), `testMetadataOnException` (dropped all four
values Java checks), and both closed-producer tests (`contains` instead of exact
message) — and one still is (`test_partitioner`, no `Partitioner` on the Rust
mock). Prose has to name them; the count never will.

**And the lesson that cost a Critic finding: record such a set by membership,
never by count.** I wrote "three" in four artifacts and named *two different
threes* — the in-file block and PLAN dropped `testManualCompletion`, the commit
message and this note dropped `testMetadataOnException` — because each was
written beside the commit that made its own subset visible. Two lists that agree
on `3` look consistent to every count-level check; only a membership diff sees
it. This is the cross-reference cousin of Phase 6's complementary-sweep lesson:
re-derive the set from the diff at fix time, and write the names everywhere.
