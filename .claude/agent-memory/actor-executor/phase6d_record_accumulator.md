---
name: Phase 6d RecordAccumulator translation
description: Patterns settled during Phase 6d — RecordAccumulator structure, lock-then-await for append, AppendCallbacks upcasting, KAFKA-19012 surrogate buffer, Outcome enum for drain
type: project
---

Phase 6d translates `RecordAccumulator.java` (1305 LOC) into 6 commits.
Test count 1010 → 1050 (+40).

**Why:** This is the largest single class in Phase 6. The patterns
chosen here will be re-applied across the Sender (6e) and any future
producer-side translation that crosses the producer ↔ sender boundary.

## Per-partition deque shape

Java holds `ConcurrentMap<Integer, Deque<ProducerBatch>>` where
`synchronized (deque) { ... }` guards the per-partition `Deque`. The
Rust translation:

```rust
pub(crate) type BatchDeque = Arc<Mutex<VecDeque<Arc<ProducerBatch>>>>;

struct TopicInfo {
    batches: Mutex<HashMap<i32, BatchDeque>>,
    built_in_partitioner: BuiltInPartitioner,
}
```

The outer `Mutex<HashMap>` guards insertions; the inner `Mutex<VecDeque>`
guards the per-partition operations. The pattern: take the outer
mutex briefly to clone the `Arc<Mutex<VecDeque>>`, then drop the
outer mutex before locking the inner. This mirrors Java's
`ConcurrentHashMap.computeIfAbsent` + `synchronized(deque)` exactly.

## Lock-then-await pattern (CLAUDE.md rule 9.6)

`append` is `async fn` because `BufferPool::allocate` is async. Java's
`synchronized (deque) { try; if full break; allocate releases monitor;
synchronized again }` flow translates to:

```rust
{
    let mut deque = dq.lock().unwrap();
    // try to append in existing batch
    drop(deque);  // explicit; not strictly needed but documents intent
}
let buffer = self.free.allocate(size, max_time_to_block).await?;
{
    let mut deque = dq.lock().unwrap();
    // append to new batch with the freshly allocated buffer
}
```

Two separate `{ }` lock scopes, with `.await` outside both. The
`AppendInProgressGuard` RAII type refunds the buffer on cancellation.

## AppendCallbacks trait upcasting (Rust 1.86+)

Java has `interface AppendCallbacks extends Callback`. The Rust
translation:

```rust
pub(crate) trait AppendCallbacks: Callback {
    fn set_partition(&self, partition: i32);
}
```

To pass `Arc<dyn AppendCallbacks>` into `ProducerBatch::try_append`
(which takes `Option<Arc<dyn Callback>>`), use trait upcasting:

```rust
fn upcast_callback(cb: Arc<dyn AppendCallbacks>) -> Arc<dyn Callback> {
    cb  // Rust 1.86+ trait upcasting — single fat-pointer copy + Arc bump
}
```

No heap allocation; the `dyn Callback` vtable is reused from the
`dyn AppendCallbacks` vtable's parent slot.

## KAFKA-19012 surrogate buffer

Java's `deallocate(batch)` for an in-flight batch creates a fresh
`ByteBuffer` of `initialCapacity()` to keep pool accounting consistent,
then panics. The Rust translation:

```rust
if batch.is_inflight() {
    let cap = batch.initial_capacity();
    let surrogate = vec![0u8; cap];
    self.free.deallocate(surrogate, cap as i32);
    panic!("Attempting to deallocate a batch that is inflight. Batch is {}", batch);
}
```

The fresh `Vec<u8>` is returned to the pool; the *real* buffer stays
with the in-flight network request and will be deallocated when the
sender's response handler runs (Phase 6e).

## `Outcome` enum for drain

`drainBatchesForOneNode` mixes "decision under lock" with "expensive
post-decision work outside lock". Java uses `synchronized (deque) {
... batch = deque.pollFirst(); ... }` then `batch.close()` outside.
The Rust translation uses an `enum Outcome { Skip, StopDrain,
Drain(Arc<ProducerBatch>) }` block:

```rust
let outcome: Outcome = (|| {
    // ... inspect deque, return Outcome variant ...
})();
match outcome {
    Outcome::Skip => continue,
    Outcome::StopDrain => break,
    Outcome::Drain(batch) => {
        batch.close()?;  // outside the deque lock
        ...
    }
}
```

The IIFE block holds the deque mutex; the `match` runs after the
mutex is dropped. This pattern keeps the "expensive work outside
lock" invariant explicit at the call site.

## TransactionManager plug-in contract

Per Phase 6 NOTES.md: every Java `if (transactionManager != null)`
branch translates to `if let Some(_tm) = &self.transaction_manager
{ unreachable!("...") }` because Phase 7 config validation rejects
the inputs that would set `Some`. Don't write empty bodies — the
`unreachable!()` documents the contract and gives a useful panic
message if the assumption is ever broken.

## Test setup MockTime pinning

`MockTime::default()` initializes from wall-clock to satisfy Java's
"`nanoTime != currentTimeMillis`" parity contract. For
`RecordAccumulator` tests that pass integer timestamps (`now=0, 10,
11, ...`), use `MockTime::with_initial(0, 0, 0)` explicitly so the
internal `time.milliseconds()` reading after the BufferPool allocation
matches the test's frame of reference.

## ExponentialBackoff jitter and tests

`retry_backoff = ExponentialBackoff::new(retryBackoffMs,
RETRY_BACKOFF_EXP_BASE, retryBackoffMaxMs, RETRY_BACKOFF_JITTER)` —
the `RETRY_BACKOFF_JITTER` is 0.2 (±20%). Tests asserting on
backoff-elapsed semantics must use `now_ms` values outside the jitter
band (e.g. `wait <= 80ms` or `wait >= 120ms` for a 100ms baseline).
Don't rely on values exactly at the baseline.

## Topic interning via `Arc<str>`

Append takes `&str topic`. We `Arc::from(topic)` once (in `append`)
and reuse via `Arc::clone` for:
- `topic_info_map` key
- `TopicPartition::topic()` (which itself stores `Arc<str>`)
- partition info indexing

This is one allocation per topic per producer, not per record.
Confirmed by inspection — the only `String::from(topic)` on the
hot path is the unavoidable Arc-from for the first append to a new
topic.

## Compression-ratio estimator is global

`compression_ratio_estimator` uses a static `DashMap<String,
PerTopicRatios>`. Tests that share a topic name with other tests
risk thread-interleaving non-determinism. For new tests that call
`set_estimation` use a unique topic (`"my_test_topic_<distinct>"`)
per test invocation.
