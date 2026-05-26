---
name: Phase 6d Round 1 patterns
description: Lessons applied during Phase 6d Round 1 fixups — interleaved-drain stress test shape, leader-epoch invariant test pattern, cancellation guard test scaffolding, hot-path Arc<str> reuse via get_key_value, checked_add overflow idiom
type: project
---

Patterns from Phase 6d Round 1 worth re-using in 6e and beyond.

**Why:** The Critic flagged 4 weak skip rationales out of 11, plus
several Suggestion items where the impl was correct but coverage
or hot-path tightness was weaker than CLAUDE.md DoD demands.
These fixes establish patterns I should apply pre-emptively in
future phases rather than discover during review.

## Interleaved-drain stress test shape

When translating Java `*StressfulSituation` tests, the producer-task
loop alone is insufficient. Java's `while (read < expected) { drain;
complete }` runs **in parallel** with the producer threads. The
Rust shape:

```rust
let drainer_accum = accum.clone();
let drainer = tokio::spawn(async move {
    let mut seen = 0;
    for _ in 0..MAX_POLLS {
        let r = drainer_accum.ready(&snap, i64::MAX);
        if !r.ready_nodes.is_empty() {
            for (_, batches) in drainer_accum.drain(...) {
                for batch in batches {
                    seen += batch.record_count();
                    batch.complete(0, 0);
                    drainer_accum.complete_and_deallocate_batch(batch);
                }
            }
        }
        if seen >= EXPECTED_TOTAL { break; }
        tokio::task::yield_now().await;
    }
    seen
});
```

Plus an outer `tokio::time::timeout` so deadlock regressions surface
as test failures rather than hangs. **Always** assert exact total
record count (not `> 0`), or the test masks record-loss bugs.

## Leader-epoch invariant test pattern

Tests that drive `has_leader_changed_for_the_ongoing_retry()` need a
helper that builds a `MetadataSnapshot` parameterised by leader_epoch:

```rust
fn build_single_partition_snapshot(cluster: Arc<Cluster>, leader_epoch: i32) -> MetadataSnapshot {
    // Single PartitionMetadata with `leader_epoch=Some(epoch)`,
    // others same as the shared test cluster
}
```

The test then rebuilds the snapshot between retry attempts to
simulate a leader-change observation. Pattern: append → drain (sets
attempt count + epoch) → reenqueue → bump epoch → rebuild snapshot →
drain (now triggers leader-change-overrides-backoff).

## AppendInProgressGuard cancellation test scaffolding

Pattern for testing RAII Drop on Tokio cancellation:

1. Size the buffer pool so the second appender will block on
   `BufferPool::allocate(...).await`.
2. Spawn the blocked task with `tokio::spawn`.
3. Loop with bounded `tokio::time::sleep(2ms)` until the
   appends-in-progress counter shows the in-flight registration.
   (No sync primitive — bounded poll handles the race.)
4. `JoinHandle::abort()` — the aborted future's Drop runs
   synchronously.
5. Assert counter back to 0, queue drained, then exercise the live
   path with a fresh appender + `tokio::time::timeout` to confirm
   no leaked-ghost wakeup.

Required `#[cfg(test)] pub(crate)` inspector for the counter
(`appends_in_progress_count()`); production API stays bool-only.

## Hot-path Arc<str> reuse via HashMap::get_key_value

CLAUDE.md rule 11 says hot-path identifiers should be `Arc<str>` so
clones are cheap. But naively calling `Arc::from(topic)` per
`append` allocates per-call. The cheap-fix pattern:

```rust
fn get_or_create_topic_info(&self, topic: &str) -> (Arc<str>, Arc<TopicInfo>) {
    let mut map = self.topic_info_map.lock().unwrap();
    if let Some((key, info)) = map.get_key_value(topic) {
        return (Arc::clone(key), Arc::clone(info));   // refcount bump only
    }
    let topic_arc: Arc<str> = Arc::from(topic);   // slow path: allocate
    // ... insert, return
}
```

`HashMap::get_key_value` is the key — it returns `(&K, &V)` so the
existing `Arc<str>` key can be cloned without re-allocating. This
matches Java's `computeIfAbsent` reuse semantics. Apply this
pattern to any per-topic / per-partition map keyed by `Arc<str>`.

## checked_add overflow idiom (Java wrap-to-negative parity)

Java's `(int) ((batch.createdMs + deliveryTimeoutMs) > 0)` overflow
check relies on **silent integer wrap to negative**. In Rust, debug
builds **panic on overflow** while release builds wrap. To get
consistent behavior on both:

```rust
match batch.created_ms().checked_add(self.delivery_timeout_ms as i64) {
    Some(candidate) if candidate > 0 => { /* in-range path */ },
    _ => { /* overflow path: log warn */ },
}
```

NOT `saturating_add` — that would clamp to `i64::MAX`, which is
> 0 and would let us into the in-range path where Java's wrap takes
the warn-log branch. Always `checked_add` for Java integer-overflow
idioms.

## Skip-rationale tightening rule

When skipping a Java test, the rationale must name the **specific
Rust test** (function name) **and file:line** where the load-bearing
invariant is covered. Not "Phase X". Not "covered by other tests".
The Critic checks these and demoted 4 of 11 of mine. Future skips:

- Bad: "Covered by Phase 6a tests."
- Good: "(a) covered by `await_flush_completion_returns_immediately_when_no_batches`
  below; (b) covered by `flush_drives_all_batches_to_completion`
  plus the unconditional decrement in `FlushInProgressGuard::Drop`
  (`record_accumulator.rs:1638-1648`)."

Verify by grep before writing the rationale — easy to misremember
test names.
