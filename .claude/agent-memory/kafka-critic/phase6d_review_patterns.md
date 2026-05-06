---
name: Phase-6d review patterns
description: RecordAccumulator review — skip-list audit framework, Tokio-spin-vs-Java-thread divergence, hot-path Arc<str> allocation, dead-field detection
type: reference
---

Phase 6d (RecordAccumulator) was the largest Phase 6 class (1305 LOC
Java + 1892 LOC tests). It had no Blocking issues but produced 10
Suggestion items. The recurring patterns to carry forward into
Phase 6e (Sender) and Phase 7 (KafkaProducer integration):

## 1. Skip-rationale audit framework (high-yield)

When the actor lists "covered by Phase X test" rationales, audit
each by:
1. Identify the SPECIFIC invariant the Java test asserts.
2. Grep the alleged covering Phase X test for that invariant.
3. If the invariant is "X interacts correctly with Y" (i.e., an
   integration test), Phase 6c-style isolation tests do NOT cover
   it. File Suggestion.

Concrete examples I caught in Phase 6d:
- `testReadyAndDrainWhenABatchIsBeingRetried` — only Java test
  covering "leader change overrides backoff" invariant (Java
  KAFKA-15968). Rust covers cases 1+3 of 4. Cases 2+4 are gaps.
- `testStressfulSituation` — Java does CONCURRENT append+drain.
  Rust replacement only does sequential append-then-drain. The
  "lock-and-future bookkeeping" claim is partially false because
  the actual stressor (interleaving) is missing.
- `testBuiltInPartitionerFractionalBatches` — accumulator+
  partitioner integration. Phase 6c tests partitioner in
  isolation; the integration flow is NOT covered.

## 2. Tokio sync-loop in async context divergence

`abort_incomplete_batches` is `pub fn` (sync) and contains:
```rust
loop {
    self.abort_batches_with_default_reason();
    if !self.appends_in_progress() {
        break;
    }
}
```
Java has the same loop but Java threads run truly concurrently.
On a Tokio single-threaded runtime, if one task is suspended
mid-`append` (between awaits) and another task calls
`abort_incomplete_batches`, the spin-loop can starve the
suspended task. Mitigation in this case: the user is expected to
call `accumulator.close()` first, which closes the BufferPool
and notifies waiters. The Rust BufferPool's `close()` does fire
notifiers correctly. So in practice this works, but the structure
is fragile.

Pattern: SYNC loops that wait on async-task progress in Rust are
a hidden cost. Java's "tight loop" assumes preemptive thread
scheduling; Rust async assumes cooperative scheduling.

## 3. Hot-path Arc::from(&str) allocation per call

Symptom: `let topic_arc: Arc<str> = Arc::from(topic);` at the top
of an `append`-style hot path.

Why it's wrong: Java's `topicInfoMap.computeIfAbsent(topic, ...)`
does NOT allocate when the topic is already in the map — it uses
the input `String` reference. Rust's `Arc::from(&str)` ALWAYS
allocates.

Fix shape: change accessor to take `&str`, return
`(Arc<str>, Arc<TopicInfo>)`. Use `map.get_key_value(topic)` for
the fast path to clone the existing `Arc<str>` key.

This is a Performance Suggestion not Blocking — but it directly
violates CLAUDE.md rule 11's spirit ("`Arc<str>` to make clones
cheap").

## 4. Dead-field detection

Pattern: declared field with documented purpose but never read.
In Phase 6d: `flush_notify: Arc<tokio::sync::Notify>` — declared,
initialized, doc-promised, never read. The actual notification
goes through `ProduceRequestResult::await_all_dependents()`.

How to find it: after reading the entire file, do a final-pass
grep for each non-trivial field. If a field is only at its
declaration and constructor sites, it's dead. The doc-comment
that promises a behavior the impl doesn't deliver is the smell.

## 5. Drop guards used to mirror Java try/finally

Phase 6d adds `AppendInProgressGuard` and `FlushInProgressGuard`.
The shape that's correct:
- `Guard::new(&accumulator)` — increments counter, returns guard.
- `Drop::drop` — decrements counter, refunds any held resource.
- "disarm" success-path API is a no-op marker (the actor's choice
  in Phase 6d) — Drop's Option-take handles success vs failure.

The Drop must:
1. Run on every return path (including panic and future-drop).
2. NOT hold any lock across `.await` (Drop is sync, so this is
   automatically satisfied — but the guard's METHODS must not
   acquire locks that are held during `.await`).
3. Be tested by deliberately dropping the future under test.

Phase 6a established `WaiterGuard` for BufferPool. Phase 6d
should have added a parallel cancellation test for
`AppendInProgressGuard`. Filed as Suggestion 10.

## 6. unreachable!() for plug-in dead branches

The actor used `unreachable!()` for Phase 7-deferred transaction
manager paths. To audit:
1. Grep for `unreachable!()` in production paths.
2. For each, identify what makes the branch unreachable —
   typically an `if let Some(tm) = &self.transaction_manager` where
   `transaction_manager` is always None.
3. Verify by checking if the wrapping field's setter has any
   caller that passes `Some(...)`. In Phase 6d, `TransactionManager`
   is a unit struct with no constructor and `pub(crate)` — so
   construction is grep-checkable.

This is acceptable per CLAUDE.md rule 10.1 ("ArithmeticException
is the named case") — the broader "unrecoverable" reading covers
genuinely impossible code paths.

## 7. Java overflow-detection idiom: `+ > 0`

Java: `if (x + y > 0) { use x + y } else { warn; }` — relies on
two's-complement wrap to negative.

Rust translation hazard: `+` panics in debug builds. The actor
in Phase 6d used `saturating_add` for the candidate value but
left raw `+` for the boundary check. Result: production behavior
correct (release wraps), but debug builds at the boundary panic.

Pattern: when seeing Java's `+ > 0` overflow idiom, audit the
Rust translation for `checked_add().is_some_and(|v| v > 0)` or
equivalent. Raw `+` survives release but breaks debug.

## 8. KAFKA-19012 surrogate-buffer trick

Pattern: Java's `deallocate(ByteBuffer.allocate(initialCapacity()))`
followed by `throw new IllegalStateException`. The trick: the
in-flight batch's real buffer is held by the network code (can't
return it). To keep BufferPool accounting correct, allocate a
SURROGATE same-sized buffer and route THAT through the pool. Then
fail.

Rust's translation in Phase 6d uses `vec![0u8; cap]` (real
heap-zero-fill). Performance-suboptimal but matches Java's
semantics. Filed as Performance Suggestion in Phase 6a's
patterns; not refiled here because the panic-after-deallocate
path is genuinely rare.

## What WORKED in Phase 6d (positive patterns to confirm in 6e)

- 24/24 Java public methods translated, none missing.
- Lock-then-await pattern on `append` correctly drops the deque
  guard before `BufferPool::allocate().await`.
- KAFKA-19012 surrogate-buffer trick faithful translation.
- The `Outcome` enum in `drain_batches_for_one_node` correctly
  separates "decision under lock" from "expensive close() outside
  lock".
- Both `AppendInProgressGuard` and `FlushInProgressGuard` Drop
  impls are visually correct.
- License headers, phase-pointer comments on `#![allow(dead_code)]`,
  module-level rustdoc all in place.

Carry-forward to Phase 6e (Sender):
- Sender will be the consumer of `ready`/`drain`/`abort*`/`close`.
  Verify the Tokio cancellation surface of Sender's
  `tokio::select!` arms — they should not have side effects in
  branches that lose the race.
- Sender's `awaitFlushCompletion` invocation context — make sure
  it doesn't deadlock with `abort_incomplete_batches` on a
  single-threaded runtime.
