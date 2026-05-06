# PR #63: Adding check for invalid batch from persister in Share Partition

## AK Commit

- **Commit:** `8fe1157ac5f6c2eaf79e21da3d9bfe5e979312a6`
- **Title:** MINOR: Adding check for invalid batch from persister in Share Partition (#20979)
- **Author:** Apoorv Mittal
- **Branch:** trunk

## Summary

The commit adds a defensive validation check inside `SharePartition.maybeInitialize()` during
the persister read-state callback. When iterating over `PersisterStateBatch` entries returned
by the persister, the new check detects batches where `lastOffset < firstOffset` (i.e. an
inverted or corrupt batch) and fails the initialization early with an `IllegalStateException`
rather than silently propagating invalid state to clients.

### Changed Files

| File | Change |
|------|--------|
| `core/src/main/java/kafka/server/share/SharePartition.java` | +9 lines: add `lastOffset < firstOffset` guard with error log |
| `core/src/test/java/kafka/server/share/SharePartitionTest.java` | +20 lines: `testMaybeInitializeWithInvalidOffsetInBatch` test |

### Java Diff (SharePartition.java)

```java
// New guard added at line ~494, inside maybeInitialize() callback loop:
if (stateBatch.lastOffset() < stateBatch.firstOffset()) {
    log.error("Invalid state batch found for the share partition: {}-{}. The first offset: {}"
            + " is less than the last offset of the batch: {}.", groupId, topicIdPartition,
        stateBatch.firstOffset(), stateBatch.lastOffset());
    throwable = new IllegalStateException(String.format(
        "Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
    return;
}
```

The guard is inserted immediately after the existing `firstOffset < startOffset` check and
before the gap-detection logic, so the validation order is:
1. `firstOffset < startOffset` → fail (existing)
2. `lastOffset < firstOffset` → fail (new)
3. Gap detection and state population → proceed

## Scope Analysis

**This commit is out of scope for the Rust client library translation.**

`SharePartition` lives in `core/src/main/java/kafka/server/share/` — the Apache Kafka
broker/server module, not the client library (`clients/`). The Rust codebase being maintained
here is a translation of the Kafka *client* library
(`org.apache.kafka.clients`, `org.apache.kafka.common`). It contains no broker-side
components and does not translate anything from the `kafka.server` package tree.

Affected packages for this commit:
- `kafka.server.share.SharePartition` — broker server class, not translated
- `kafka.server.share.persister.PersisterStateBatch` — broker-side persister type, not translated

Neither class has a counterpart in the current Rust source tree (`src/`), and neither is
referenced in any existing Rust module.

## Decision: No-op

No Rust code changes are required for this PR. The validation logic added by this commit
belongs entirely to broker-side Share Group infrastructure which is outside the scope of the
client library translation.

## If SharePartition Were Ever In Scope

Should broker-side Share Group support be added to the Rust project in a future milestone,
the following would apply:

- **Location:** `src/server/share/share_partition.rs` (mirroring `kafka.server.share`)
- **Persister types:** `src/server/share/persister/persister_state_batch.rs`
- **Validation to port:**
  ```rust
  if state_batch.last_offset() < state_batch.first_offset() {
      error!(
          "Invalid state batch found for share partition: {}-{}. \
           first_offset={} last_offset={}",
          group_id, topic_id_partition,
          state_batch.first_offset(), state_batch.last_offset()
      );
      return Err(KafkaError::IllegalState(format!(
          "Failed to initialize the share partition {}-{}",
          group_id, topic_id_partition
      )));
  }
  ```
- **Test to port:** `testMaybeInitializeWithInvalidOffsetInBatch` — mock persister returns
  a batch with `firstOffset=11, lastOffset=10`; assert `CompletableFuture` completes
  exceptionally with `IllegalStateException` and partition state is `FAILED`.
