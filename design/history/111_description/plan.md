# Translation Plan: KAFKA-18475 Flaky PlaintextProducerSendTest.testCloseWithZeroTimeoutFromCallerThread

**AK commit:** `385b70d2f8d17be2e235ed029deb2e5acff57795`
**AK branch:** trunk
**PR:** #111
**Rust branch:** `kafka-translate/385b70d2f8d17be2e235ed029deb2e5acff57795`

---

## Summary of the Apache Kafka Commit

This commit fixes a **flaky integration test** in `BaseProducerSendTest.testCloseWithZeroTimeoutFromCallerThread`. The flakiness was caused by a race condition between `RecordAccumulator#close` and `RecordAccumulator#batchReady`: when the accumulator's closed flag is set, `batchReady` considers the batch sendable, causing it to be sent in the same `Sender#runOnce` call (which doesn't check the `forceClose` flag).

Rather than fixing the race condition (which would require an expensive lock), the commit removes the consumer-side assertion that was unreliable due to this race. The test already verifies that at most one more `Sender#runOnce` executes after force close.

**Changed files:**

| File | Change |
|------|--------|
| `core/src/test/scala/integration/kafka/api/BaseProducerSendTest.scala` | Removed 2 lines: consumer assignment and poll assertion |

**Diff:**
```scala
- consumer.assign(java.util.List.of(new TopicPartition(topic, partition)))
  ...
- assertEquals(0, consumer.poll(Duration.ofMillis(50L)).count, "Fetch response should have no message returned.")
```

The consumer assignment at line 511 and the poll assertion at line 525 are removed because after force close, it's possible that batches still get sent due to the race condition, making the "no messages received" assertion unreliable.

---

## Rust Translation Analysis

### Equivalent Rust code

This commit modifies only a **Scala integration test** (`BaseProducerSendTest`). After searching the Rust codebase, there is no equivalent integration test for `testCloseWithZeroTimeoutFromCallerThread`.

The Rust project does not currently have:
- A `BaseProducerSendTest` equivalent
- An integration test for producer force-close with zero timeout
- A KRaft-based test harness that supports the full producer/consumer lifecycle needed by this test

### Impact assessment

Since this is a **test-only change** that removes flaky assertions from an integration test that does not exist in the Rust codebase, there is **nothing to translate**.

The underlying producer force-close behavior (the race condition between `RecordAccumulator::close` and `RecordAccumulator::batch_ready`) is a design consideration to keep in mind when implementing the Rust producer's force-close path, but no code changes are required for this commit.

---

## Implementation Plan

### No changes required

This commit is a test-only fix for a flaky Scala integration test. The test does not exist in the Rust codebase, and no production code was changed in the original commit.

---

## Files to Modify

None.

---

## Out of Scope

- Translating the full `BaseProducerSendTest` — requires a KRaft-based integration test harness with topic creation, producer, and consumer support.
- Fixing the underlying race condition in `RecordAccumulator` — this was explicitly not fixed in the Java codebase either; it's a known limitation of lock-free force-close.

---

## Definition of Done

- [x] No code changes needed — this is a test-only commit with no Rust equivalent.
- [x] Design document written to record the decision.
