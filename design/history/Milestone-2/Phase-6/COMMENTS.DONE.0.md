# Phase 6 Review: ProducerBatch, BuiltInPartitioner, RecordAccumulator, ProducerMetadata

Reviewed commits d675a08..46805f6

---

## Issue 1: Callbacks are never fired in ProducerBatch.complete_future_and_fire_callbacks

- **File**: `src/clients/producer/internals/producer_batch.rs`
- **Severity**: Bug
- **Java Reference**: `ProducerBatch.java:297-324`
- **Description**: In Java, `completeFutureAndFireCallbacks` iterates through all thunks and invokes `thunk.callback.onCompletion(metadata, exception)` for each one with a non-null callback. The Rust version (lines 412-426) has a comment saying callbacks are "handled through the FutureRecordMetadata.get() async pattern" and skips callback invocation entirely. The callbacks stored in `Thunk.callback` are dropped without being called.

    This means user-provided callbacks (the `Callback` type alias for `Box<dyn FnOnce(...)>`) passed via `try_append` will never execute. The Java client guarantees that `Callback.onCompletion(metadata, exception)` is invoked exactly once per record when a batch completes, fails, or is aborted. The Rust client silently drops them.

- **Expected**: Callbacks should be invoked exactly once when the batch is completed or aborted, matching Java behavior.
- **Actual**: Callbacks stored in `Thunk.callback` are never invoked and are dropped when the `ProducerBatch` is dropped.

---

## Issue 2: RecordAccumulator.try_append silently drops the callback

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Bug
- **Java Reference**: `RecordAccumulator.java:425-441`
- **Description**: The `try_append` method (line 429) accepts a callback parameter named `_callback: Option<&Callback>` (prefixed with underscore, indicating it is unused). On line 448, it calls `last.try_append(timestamp, key, value, headers, None, now_ms)` -- passing `None` instead of the actual callback. This means when a record is appended to an existing batch (the common case), the callback provided by the user is silently dropped.

    In the `append_new_batch` path (line 396), the callback IS correctly passed to `ProducerBatch::try_append`. So the first record in a new batch gets its callback stored, but subsequent records appended to the same batch do not.

    Even though Issue 1 means callbacks are never fired anyway, this should still be fixed so that when Issue 1 is fixed, callbacks are correctly stored.

- **Expected**: The callback should be passed through to `ProducerBatch::try_append` when appending to an existing batch.
- **Actual**: `None` is always passed as the callback when appending to existing batches.

---

## Issue 3: partition_ready does not call maybeUpdateLeaderEpoch and passes false for has_leader_changed

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Bug
- **Java Reference**: `RecordAccumulator.java:687-713`
- **Description**: In Java's `partitionReady`, inside the synchronized block, the code:
    1. Gets the leader epoch via `metadataSnapshot.leaderEpochFor(tp)` (line 687)
    2. Calls `batch.maybeUpdateLeaderEpoch(leaderEpoch)` (line 707)
    3. Passes `batch.hasLeaderChangedForTheOngoingRetry()` to `shouldBackoff` (line 708)

    In Rust's `partition_ready` (lines 608-621):
    1. `leaderEpochFor` is never called (the method does not exist in `MetadataSnapshot`)
    2. `batch.maybe_update_leader_epoch()` is never called
    3. `should_backoff` is called with `false` as the first argument instead of `batch.has_leader_changed_for_the_ongoing_retry()`

    This means leader epoch tracking is completely broken in the ready-check path. Retried batches will never detect that the leader has changed, and will always apply backoff even when a leader change would allow immediate retry.

- **Expected**: Leader epoch should be tracked and `has_leader_changed_for_the_ongoing_retry()` should be used to decide whether to skip backoff.
- **Actual**: Leader epoch is never updated in `partition_ready`; backoff is always applied to retried batches regardless of leader changes.

---

## Issue 4: drain_batches_for_one_node does not call maybeUpdateLeaderEpoch

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Bug
- **Java Reference**: `RecordAccumulator.java:874-886`
- **Description**: In Java's `drainBatchesForOneNode`, the code retrieves the leader epoch via `metadataSnapshot.leaderEpochFor(tp)` (line 874) and calls `first.maybeUpdateLeaderEpoch(leaderEpoch)` (line 885) before checking backoff. The Rust implementation (lines 770-793) skips both of these calls.

    Combined with Issue 3, this means `ProducerBatch.maybe_update_leader_epoch()` is never called from RecordAccumulator at all, making the leader epoch tracking in `ProducerBatch` dead code.

- **Expected**: Leader epoch should be updated during drain to enable leader-change-aware retry decisions.
- **Actual**: Leader epoch is never updated during drain.

---

## Issue 5: drain_batches_for_one_node uses estimated_size_in_bytes instead of records().sizeInBytes()

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `RecordAccumulator.java:931`
- **Description**: After popping a batch from the deque, Java calls `batch.close()` and then `size += batch.records().sizeInBytes()` to accumulate the actual serialized size of the records. Rust (line 804) uses `size += batch.estimated_size_in_bytes() as i32` instead. The estimated size can differ significantly from the actual size, especially with compression. This can cause over-draining (sending more data than `maxSize` allows) or under-draining.

- **Expected**: Use the actual size of the built records after closing the batch.
- **Actual**: Uses estimated size which may differ from actual size.

---

## Issue 6: IncompleteBatches never has batches added to it

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Bug
- **Java Reference**: `RecordAccumulator.java:397-399`
- **Description**: In Java, `incomplete.add(batch)` is called whenever a new batch is created in `appendNewBatch` (line 399) and in `splitAndReenqueue` (line 531). In Rust, `incomplete.add()` is never called anywhere in `RecordAccumulator`.

    This means `has_incomplete()` always returns `false`, which breaks flush semantics. In the Java client, `awaitFlushCompletion()` waits for all batches tracked by `incomplete` to complete. Since no batches are ever added to `incomplete`, the flush mechanism is broken.

    Additionally, the `abort_incomplete_batches()` method's first loop calls `self.abort_batches()`, which iterates through `topic_info_map` directly rather than through `incomplete`, so abort still works. But the design intent of `IncompleteBatches` as a mechanism for tracking in-flight batches across the flush boundary is not implemented.

- **Expected**: New batches should be added to `incomplete` when created, and removed when completed/deallocated.
- **Actual**: `incomplete` is always empty.

---

## Issue 7: split_and_reenqueue missing CompressionRatioEstimator.setEstimation

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `RecordAccumulator.java:515-516`
- **Description**: In Java's `splitAndReenqueue`, the first action is to reset the compression ratio estimation:

    ```java
    CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression.type(),
        Math.max(1.0f, (float) bigBatch.compressionRatio()));
    ```

    This ensures that future batches for this topic/compression combination will use a more conservative compression ratio estimate, reducing the likelihood of needing to split again. This call is missing in the Rust implementation (lines 1013-1036).

- **Expected**: Compression ratio estimation should be reset before splitting to prevent repeated splitting.
- **Actual**: Compression ratio estimation is not reset.

---

## Issue 8: deallocate creates a new buffer instead of returning the batch's buffer

- **File**: `src/clients/producer/internals/record_accumulator.rs`
- **Severity**: Behavior Mismatch
- **Java Reference**: `RecordAccumulator.java:1040-1056`
- **Description**: The Rust `deallocate` method (lines 888-894) creates a brand-new `Vec<u8>` of the batch's initial capacity and passes that to `BufferPool::deallocate`. It does not return the batch's actual buffer. This wastes memory (the original buffer is leaked/dropped, and a new one is allocated just to be "returned").

    The Java version has additional safety checks:
    - Warns and skips if `isBufferDeallocated()` is already true
    - Calls `markBufferDeallocated()` to prevent double deallocation
    - Throws `IllegalStateException` if the batch is in-flight
    - Returns the actual batch buffer to the pool

    All of these checks are missing in Rust.

- **Expected**: The actual batch buffer should be returned to the pool, with safety checks for double-deallocation and in-flight status.
- **Actual**: A new buffer is allocated and immediately "returned" to the pool; the original buffer is dropped.

---

## Issue 9: Multiple significant Java tests not translated

- **File**: Multiple test modules
- **Severity**: Missing Requirement
- **Java Reference**: `ProducerBatchTest.java`, `RecordAccumulatorTest.java`
- **Description**: Several non-trivial Java tests are not translated:

    **ProducerBatch:**
    - `testSplitPreservesMagicAndCompressionType` -- verifies magic version and compression type are preserved across split
    - `testCompleteExceptionallyWithNullRecordErrors` -- verifies NullPointerException behavior

    **RecordAccumulator** (non-transaction-related):
    - `testExponentialRetryBackoff` -- verifies exponential backoff calculation
    - `testExponentialRetryBackoffLeaderChange` -- verifies backoff skipped on leader change
    - `testAbortIncompleteBatches` -- verifies abort of all incomplete batches
    - `testAbortUnsentBatches` -- verifies abort of unsent batches only
    - `testSplitAndReenqueue` -- verifies split and re-enqueue end-to-end
    - `testSplitBatchOffAccumulator` -- verifies batch creation during split
    - `testSplitFrequency` -- verifies split does not recur infinitely
    - `testSplitAndReenqueuePreventInfiniteRecursion` -- critical safety test
    - `testUniformBuiltInPartitioner` -- verifies uniform distribution in accumulator context
    - `testAdaptiveBuiltInPartitioner` -- verifies adaptive partitioning in accumulator context
    - `testBuiltInPartitionerFractionalBatches` -- verifies fractional batch avoidance
    - `testAwaitFlushComplete` -- verifies flush wait semantics
    - `testProduceRequestResultAwaitAllDependents` -- verifies dependent future chaining

- **Expected**: All non-transaction-specific tests should be translated per Definition of Done rule 3.
- **Actual**: 15+ significant tests are missing.
