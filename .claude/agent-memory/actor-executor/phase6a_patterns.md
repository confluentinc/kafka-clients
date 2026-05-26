---
name: Phase 6a producer-internals patterns
description: Tokio-fair waiter pool, future-result design, and identity-set patterns from Phase 6a translation
type: project
---

Phase 6a translates the producer's buffer-pool + future-plumbing layer.
Key patterns that worked and stay true for later phases:

**BufferPool waiter design (Java ReentrantLock + Condition → Tokio).** Single
`std::sync::Mutex<State>` guards the FIFO `VecDeque<Arc<Notify>>`. Each
waiter constructs its OWN `Arc<Notify>`; `done()` calls `notify_one()`
on the head. `tokio::select! { biased; _ = notify.notified() => false,
_ = sleep(d) => true }` races the wait against the timeout. The mutex
guard is dropped before every `.await` (CLAUDE rule 9.6) — verified by
having the slow-path scope the lock in a block and re-acquire on the
other side. Never collapse the lock+await into one scope, even if it
looks neat.

**Why:** The same shape repeats in `ProduceRequestResult` and will
repeat in `RecordAccumulator::append` (Phase 6d). Once you have one
correct example to copy, the pattern is easy.

**ProduceRequestResult shape.** `AtomicBool` "completed" flag +
`tokio::sync::Notify` for wake-up + `Mutex<ResultData>` for the
volatile fields Java sets together (`base_offset`,
`log_append_time`, `errors_by_index`). The await pattern:

```rust
if self.completed.load(Ordering::Acquire) { return; }
let notified = self.notify.notified();      // register waker first
if self.completed.load(Ordering::Acquire) { return; }  // re-check
notified.await;
```

`done()` uses `notify_waiters()` (not `notify_one()`) because the same
result is shared by all sibling-record futures of a partition.

**FutureRecordMetadata: async fn, NOT impl Future.** CLAUDE rule 11
forbids `Pin<Box<dyn Future>>` per record on the send path. Exposing
`pub async fn get(&self) -> Result<...>` returns a concrete future
type that the compiler can monomorphise. The chain pointer is
`OnceLock<Arc<FutureRecordMetadata>>`, walked recursively in `chain()`
with `Box::pin` only on the chain-tail recursion (a single
allocation per chain hop, not per record).

**Java identity-HashSet → Rust pointer-keyed HashMap.** Java's
`HashSet<ProducerBatch>` uses default identity equals/hashCode.
Mirror with `HashMap<usize, Arc<T>>` keyed by `Arc::as_ptr(&arc) as
usize`. Distinct Arcs to the same T compare distinct, matching Java
exactly. Used by `IncompleteBatches`; will be reused by
`RecordAccumulator` (Phase 6d) for in-flight batch tracking.

**Forward-declaring ProducerBatch in 6a so IncompleteBatches compiles.**
Wrote a 50-LOC placeholder file (`producer_batch.rs`) that exposes
only the constructor and `produce_future()` accessor. Phase 6b refills
the file as a strict superset (extends the constructor signature,
adds the rest of the methods). This avoids forcing IncompleteBatches
to be generic over a placeholder type just to satisfy a CLAUDE rule
about no orphan abstractions.

**Translating Java tests that don't apply to Rust.** Two
FutureRecordMetadataTest cases verify `Future.get(timeout, unit)`
re-passing the *remaining* timeout to the chained future. In Rust you
wrap the whole future tree in `tokio::time::timeout(d, future)`, so
the Java bug (passing original timeout instead of remaining) cannot be
expressed. Documented this rationale in module-level rustdoc and
replaced the tests with a deadline-propagation test that exercises the
same chain path in idiomatic Rust style. DoD line 3 explicitly allows
skipping irrelevant tests if you explain why.

**KafkaError::BufferExhausted variant existed but was missing from
is_retriable().** Java: `BufferExhaustedException extends TimeoutException
extends RetriableException`. The variant existed from earlier phases
but never got included in the retriable matcher; the test
(`is_retriable_like_timeout_subclass`) caught it. Lesson: when a new
constructor shim points at a pre-existing KafkaError variant, audit
`is_retriable`/`is_fatal` membership against the Java hierarchy.

**Mock-based tests via injectable hooks instead of trait objects.**
Java's `Mockito.spy()` overrides `protected` methods. The closest
Rust equivalent that doesn't introduce extra traits: pass an
`Arc<dyn Fn(...) -> Result<...> + Send + Sync>` to a `with_hooks`
constructor. Default hook is the no-op identity; tests inject a
closure that returns Err for fault-injection. Used in BufferPool for
`recordWaitTime`, `allocateByteBuffer`, and the `freeSize` override
(testLargeAvailableMemory).
