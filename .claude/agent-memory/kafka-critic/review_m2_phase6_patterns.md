---
name: M2 Phase 6 patterns
description: Callback drop/no-fire, leader epoch tracking dead code, IncompleteBatches unused, buffer pool deallocate fakes
type: project
---

Phase 6 (ProducerBatch, BuiltInPartitioner, RecordAccumulator, ProducerMetadata) review patterns:

1. **Callback fire omission**: Java's `completeFutureAndFireCallbacks` iterates thunks and invokes callbacks; Rust skips this entirely with a comment deferring to async. Plus `RecordAccumulator.try_append` passes `None` as callback to `ProducerBatch.try_append`, silently dropping user callbacks.
**Why:** The Actor assumed Rust's async await pattern replaces Java callbacks, but the Java contract guarantees exactly-once callback invocation per record.
**How to apply:** When translating Java callback/listener patterns, verify callbacks are actually invoked at the equivalent lifecycle point, not just stored.

2. **Leader epoch tracking dead code**: `MetadataSnapshot.leaderEpochFor()` is not implemented. As a result, `batch.maybeUpdateLeaderEpoch()` is never called from `partition_ready` or `drain_batches_for_one_node`, and `should_backoff` always receives `false` for `has_leader_changed`. The entire leader epoch tracking mechanism in ProducerBatch is effectively dead code.
**Why:** Missing method in MetadataSnapshot cascades into missing calls in RecordAccumulator.
**How to apply:** When a Java method calls APIs on dependencies, verify those dependency methods exist in Rust before considering the calling code complete.

3. **IncompleteBatches never populated**: `incomplete.add()` is never called, making `has_incomplete()` always false and flush semantics broken.
**Why:** The batch lifecycle tracking got lost in translation.
**How to apply:** Check that lifecycle management objects (add/remove/query) are all wired up, not just the query side.

4. **Fake buffer deallocation**: `deallocate` allocates a new Vec instead of returning the batch's actual buffer. Java has safety checks for double-dealloc and in-flight status.
**Why:** Ownership model difference -- the batch owns its buffer in Rust, making it harder to extract. But the current approach wastes memory.
**How to apply:** Watch for resource pool return-path implementations that create new resources instead of returning the original.

5. **estimated vs actual size in drain**: Java uses `batch.records().sizeInBytes()` (actual) while Rust uses `estimated_size_in_bytes()`. This affects max-size enforcement during drain.
