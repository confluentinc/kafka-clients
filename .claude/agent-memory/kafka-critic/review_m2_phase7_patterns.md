---
name: M2 Phase 7 Sender/KafkaClient patterns
description: Ownership transfer gaps in retry paths, dead code behind &mut references, callback-to-poll pattern loses batch tracking
type: project
---

Key patterns found in Phase 7 (Sender + KafkaClient async):

1. **Ownership transfer gap in retry path**: Java's `reenqueueBatch` passes the batch object to `accumulator.reenqueue()`. Rust's translation only calls `batch.reenqueued()` (updates timestamp) but never transfers the batch back to the accumulator because the batch is held by `&mut` reference, not owned. The `RecordAccumulator::reenqueue(mut batch: ProducerBatch)` takes ownership. This caused silent data loss for retriable errors.

**Why:** The callback-to-poll conversion (CLAUDE.md rule 9) changed the ownership model. In Java, the callback captures a reference to the batch in a HashMap, and both `handleProduceResponse` and the accumulator share access. In Rust, the batch is extracted from `in_flight_batches` as an owned value, passed to response handling via `&mut`, but cannot be moved into `accumulator.reenqueue()` while still borrowed.

**How to apply:** When reviewing Rust translations of Java code that passes objects to multiple consumers (callbacks, collections), verify that the Rust ownership model allows all the same operations. Look for methods that need ownership (`fn foo(self)` or `fn foo(bar: T)`) being called from contexts that only have `&mut T`.

2. **Missing feature silently drops records**: `split_and_reenqueue` was not implemented, but instead of failing the batch with an error, the code calls `complete_batch` which removes from tracking without resolving record futures. Hanging futures are worse than errors.

**How to apply:** When a Java feature is not yet implemented, the translation should fail the affected records with an appropriate error rather than silently completing. Check that all `ProducerBatch` paths eventually resolve all record futures.

3. **Test coverage predicts bugs**: The `testRetries` test was not translated, and it would have caught the `reenqueue_batch` bug immediately. The pattern from prior phases holds: missing test translations directly correlate with undetected bugs.
